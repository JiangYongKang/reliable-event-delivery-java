package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.AggregateGate;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.GateStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.FileEventStore;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 重启恢复：暂停状态、阻塞状态与未投完的排队事件都从本地文件恢复，不重复投递、不丢事件 */
class GateRestartRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(GateRestartRecoveryTests.class);

    @TempDir
    Path dir;

    private FileEventStore eventStore;
    private GateStore gateStore;

    /** 模拟一次进程启动：同一目录重新打开事件存储与闸门存储 */
    private void openStores() throws Exception {
        DeliveryProperties props = new DeliveryProperties();
        props.setStorageDir(dir.toString());
        gateStore = new GateStore(props);
        gateStore.load();
        eventStore = new FileEventStore(props, gateStore);
        Method load = FileEventStore.class.getDeclaredMethod("load");
        load.setAccessible(true);
        load.invoke(eventStore);
    }

    private static DeliveryEvent event(String tenant, String idem, String agg) {
        DeliveryEvent e = new DeliveryEvent();
        e.setTenantId(tenant);
        e.setIdempotencyKey(idem);
        e.setAggregateKey(agg);
        e.setPayload("p-" + idem);
        e.setTargetUrl("http://127.0.0.1/receiver/ok");
        return e;
    }

    @Test
    void pauseBlockAndBacklogSurviveRestart() throws Exception {
        openStores();
        // 暂停的聚合：两件排队
        gateStore.pause("t1", "agg-paused");
        eventStore.submit(event("t1", "p1", "agg-paused"), 100);
        eventStore.submit(event("t1", "p2", "agg-paused"), 100);
        // 阻塞的聚合：e1 进入 RETRY_WAIT（已到期），e2 排在后面
        String blockedHead = eventStore.submit(event("t1", "b1", "agg-blocked"), 100).event().getId();
        eventStore.submit(event("t1", "b2", "agg-blocked"), 100);
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        eventStore.markRetry("t1", blockedHead, "w1", "SERVER_ERROR",
                Instant.now().minusMillis(1), "receiver status 500");
        // 已投递的开放聚合事件：重启后不得重复投递
        String doneId = eventStore.submit(event("t1", "d1", "agg-open"), 100).event().getId();
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        eventStore.markDelivered("t1", doneId, "w1");
        Thread.sleep(20);

        // 模拟进程重启
        openStores();

        AggregateGate paused = gateStore.snapshot("t1", "agg-paused").orElseThrow();
        AggregateGate blocked = gateStore.snapshot("t1", "agg-blocked").orElseThrow();
        log.info("assertion basis: after restart paused={} blocked={} reason={}",
                paused.isPaused(), blocked.isBlocked(), blocked.getBlockReason());
        assertThat(paused.isPaused()).isTrue();
        assertThat(blocked.isBlocked()).isTrue();
        assertThat(blocked.getBlockedEventId()).isEqualTo(blockedHead);
        assertThat(blocked.getBlockReason()).startsWith("RETRY_WAIT:");

        // 积压未丢：暂停聚合 2 件、阻塞聚合卡住事件后面还排 1 件
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-paused", null)).isEqualTo(2);
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-blocked", 1L)).isEqualTo(1);

        // 恢复后的认领：暂停聚合一件都不出；阻塞聚合只出卡住的那件；已投递的不重复出现
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        List<String> ids = claimed.stream().map(DeliveryEvent::getId).toList();
        log.info("assertion basis: claimed after restart={}", ids);
        assertThat(ids).containsExactly(blockedHead);
        assertThat(ids).doesNotContain(doneId);

        // 卡住的事件投递成功 -> 闸门自动放行 -> 后续事件按序可认领
        eventStore.markDelivered("t1", blockedHead, "w2");
        assertThat(gateStore.snapshot("t1", "agg-blocked").orElseThrow().isBlocked()).isFalse();
        List<DeliveryEvent> next = eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        log.info("assertion basis: after unblock claimed={}",
                next.stream().map(DeliveryEvent::getId).toList());
        assertThat(next).hasSize(1);
        assertThat(next.get(0).getAggregateKey()).isEqualTo("agg-blocked");
        assertThat(next.get(0).getSequence()).isEqualTo(2);

        // 暂停的聚合恢复后按原顺序出队
        gateStore.resume("t1", "agg-paused");
        List<DeliveryEvent> resumed = eventStore.claimDeliverable("w4", 10, Duration.ofSeconds(5));
        List<Long> seqs = resumed.stream().map(DeliveryEvent::getSequence).sorted().toList();
        log.info("assertion basis: resumed aggregate sequences={}", seqs);
        assertThat(resumed).hasSize(1); // 队首先行，顺序由队首机制保证
        assertThat(resumed.get(0).getSequence()).isEqualTo(1);
    }
}
