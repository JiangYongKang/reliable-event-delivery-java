package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.GateState;
import com.github.highcumontoa.reliableeventdeliveryjava.store.FileEventStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.GateView;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitOptions;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitResult;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitStatus;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 闸门重启恢复：暂停状态（gates.json）与排队积压（events.json）都从本地恢复；
 * 暂停的聚合恢复后仍不被认领；FAILED 队首继续阻塞后续；重放/恢复后按序认领，已 DELIVERED 不重复。
 */
class GateRestartRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(GateRestartRecoveryTests.class);
    private static final int CAP = 100;

    @TempDir
    Path dir;

    private FileEventStore openStore() throws Exception {
        DeliveryProperties props = new DeliveryProperties();
        props.setStorageDir(dir.toString());
        FileEventStore store = new FileEventStore(props);
        Method load = FileEventStore.class.getDeclaredMethod("load");
        load.setAccessible(true);
        load.invoke(store);
        return store;
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
    void pausedGateAndBacklogSurviveRestartWithoutPrematureDelivery() throws Exception {
        FileEventStore s1 = openStore();
        // agg-paused：暂停后收下 2 件排队
        s1.pauseGate("t1", "agg-paused");
        SubmitResult r1 = s1.submit(event("t1", "p1", "agg-paused"), new SubmitOptions(1000, CAP));
        SubmitResult r2 = s1.submit(event("t1", "p2", "agg-paused"), new SubmitOptions(1000, CAP));
        assertThat(r1.status()).isEqualTo(SubmitStatus.ACCEPTED);
        assertThat(r2.status()).isEqualTo(SubmitStatus.ACCEPTED);

        // agg-blocked：队首 FAILED，后续 1 件排队
        String failedId = s1.submit(event("t1", "b1", "agg-blocked"), new SubmitOptions(1000, CAP)).event().getId();
        s1.claimDeliverable("w1", 10, Duration.ofSeconds(5));
        s1.markFailed("t1", failedId, "w1", "CLIENT_REJECTED", "receiver rejected with status 400");
        String followerId = s1.submit(event("t1", "b2", "agg-blocked"),
                new SubmitOptions(1000, CAP)).event().getId();

        // 一个正常聚合的已投递事件，用于验证重启后不重复认领
        String deliveredId = s1.submit(event("t1", "d1", "agg-done"),
                new SubmitOptions(1000, CAP)).event().getId();
        s1.claimDeliverable("w1", 10, Duration.ofSeconds(5));
        s1.markDelivered("t1", deliveredId, "w1");

        // ---- 模拟进程重启：同一目录重新打开 ----
        FileEventStore s2 = openStore();

        // 暂停状态恢复
        GateView pausedView = s2.findGate("t1", "agg-paused", CAP).orElseThrow();
        assertThat(pausedView.state()).isEqualTo(GateState.PAUSED);
        assertThat(pausedView.queuedCount()).isEqualTo(2);
        assertThat(pausedView.pausedAt()).isNotNull();
        log.info("assertion basis: after restart paused gate state={} queued={} pausedAtPresent={}",
                pausedView.state(), pausedView.queuedCount(), pausedView.pausedAt() != null);

        // 阻塞状态与卡住队首恢复
        GateView blockedView = s2.findGate("t1", "agg-blocked", CAP).orElseThrow();
        assertThat(blockedView.state()).isEqualTo(GateState.BLOCKED);
        assertThat(blockedView.headEventId()).isEqualTo(failedId);
        assertThat(blockedView.queuedCount()).isEqualTo(2);
        log.info("assertion basis: after restart blocked gate head={} queued={}",
                blockedView.headEventId(), blockedView.queuedCount());

        // 恢复后认领：暂停聚合一件都不能被认领；阻塞聚合的 follower 也不能越过 FAILED 队首；
        // 已 DELIVERED 不重复。只剩 follower（FAILED 队首不可认领）——因此应为空。
        List<DeliveryEvent> claimed = s2.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        List<String> claimedIds = claimed.stream().map(DeliveryEvent::getId).toList();
        log.info("assertion basis: claim after restart ids={}", claimedIds);
        assertThat(claimedIds).doesNotContain(r1.event().getId(), r2.event().getId(),
                failedId, followerId, deliveredId);

        // 先重放 FAILED 队首：此时 follower 仍被队首挡住，只应认领队首
        assertThat(s2.replay("t1", failedId)).isTrue();
        List<DeliveryEvent> afterReplay = s2.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        assertThat(afterReplay.stream().map(DeliveryEvent::getId).toList()).containsExactly(failedId);
        s2.markDelivered("t1", failedId, "w3");

        // 队首处理完，follower 才被放行
        List<DeliveryEvent> follower = s2.claimDeliverable("w4", 10, Duration.ofSeconds(5));
        assertThat(follower.stream().map(DeliveryEvent::getId).toList()).containsExactly(followerId);
        log.info("assertion basis: after replay+claim order kept: head then follower");

        // 恢复暂停聚合：每次只认领队首，按 seq1 -> 落 DELIVERED -> seq2 的顺序逐件放行，不重复已投递事件
        assertThat(s2.resumeGate("t1", "agg-paused")).isTrue();
        List<DeliveryEvent> firstHead = s2.claimDeliverable("w5", 10, Duration.ofSeconds(5));
        assertThat(firstHead.stream().map(DeliveryEvent::getId).toList()).containsExactly(r1.event().getId());
        s2.markDelivered("t1", r1.event().getId(), "w5");
        List<DeliveryEvent> nextHead = s2.claimDeliverable("w6", 10, Duration.ofSeconds(5));
        assertThat(nextHead.stream().map(DeliveryEvent::getId).toList()).containsExactly(r2.event().getId());
        log.info("assertion basis: resumed after restart claim order: {} then {}",
                r1.event().getId(), r2.event().getId());

        // 积压容量在恢复后仍生效：暂停聚合排队已满（容量 2 -> 设小容量验证拒绝）
        FileEventStore s3 = openStore();
        s3.pauseGate("t1", "agg-cap");
        s3.submit(event("t1", "c1", "agg-cap"), new SubmitOptions(1000, 1));
        SubmitResult overflow = s3.submit(event("t1", "c2", "agg-cap"), new SubmitOptions(1000, 1));
        assertThat(overflow.status()).isEqualTo(SubmitStatus.AGGREGATE_FULL);
        log.info("assertion basis: backlog cap enforced after restart status={}", overflow.status());

        // 幂等索引也恢复：重复提交仍是 DUPLICATE 而非新事件
        SubmitResult dup = s2.submit(event("t1", "p1", "agg-paused"), new SubmitOptions(1000, CAP));
        assertThat(dup.status()).isEqualTo(SubmitStatus.DUPLICATE);
    }
}
