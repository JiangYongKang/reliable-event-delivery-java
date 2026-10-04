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

/**
 * 落状态过程中被强杀（events.json 已写、gates.json 未写）后的重启恢复：
 * 重启对账必须把闸门阻塞补回/清理到与本地事件状态一致——队首没处理完的聚合仍然阻塞，
 * 卡住的事件、原因、排队数可查，后面的事件一件都不许越过；同一份状态反复重启结果一致。
 */
class GateCrashRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(GateCrashRecoveryTests.class);

    @TempDir
    Path dir;

    private FileEventStore eventStore;
    private GateStore gateStore;

    /** 崩溃模拟：armed 期间 block/unblock 完全不生效，等价于落事件状态后、落闸门状态前被 kill */
    static class CrashyGateStore extends GateStore {
        volatile boolean crashArmed;

        CrashyGateStore(DeliveryProperties properties) {
            super(properties);
        }

        @Override
        public void block(String tenantId, String aggregateKey, String eventId, String reason) {
            if (!crashArmed) {
                super.block(tenantId, aggregateKey, eventId, reason);
            }
        }

        @Override
        public void unblockIfBlockedBy(String tenantId, String aggregateKey, String eventId) {
            if (!crashArmed) {
                super.unblockIfBlockedBy(tenantId, aggregateKey, eventId);
            }
        }
    }

    /** 模拟一次进程启动：同一目录重新打开事件存储与闸门存储 */
    private void openStores() throws Exception {
        openStoresWith(new GateStore(props()));
    }

    private CrashyGateStore openStoresArmed() throws Exception {
        CrashyGateStore crashy = new CrashyGateStore(props());
        openStoresWith(crashy);
        crashy.crashArmed = true;
        return crashy;
    }

    private void openStoresWith(GateStore gates) throws Exception {
        gateStore = gates;
        gateStore.load();
        eventStore = new FileEventStore(props(), gateStore);
        Method load = FileEventStore.class.getDeclaredMethod("load");
        load.setAccessible(true);
        load.invoke(eventStore);
    }

    private DeliveryProperties props() {
        DeliveryProperties p = new DeliveryProperties();
        p.setStorageDir(dir.toString());
        return p;
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
    void retryWaitBlockSurvivesCrashDuringStatePersist() throws Exception {
        openStores();
        String e1 = eventStore.submit(event("t1", "r1", "agg-crash-retry"), 100).event().getId();
        eventStore.submit(event("t1", "r2", "agg-crash-retry"), 100);
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        // 队首进入等重试，落事件状态后、落闸门状态前进程被 kill
        openStoresArmed();
        eventStore.markRetry("t1", e1, "w1", "SERVER_ERROR",
                Instant.now().minusMillis(1), "receiver status 500");

        // 重启：闸门必须仍然判定阻塞，卡住的事件/原因/排队数可查
        openStores();
        AggregateGate gate = gateStore.snapshot("t1", "agg-crash-retry").orElseThrow();
        log.info("assertion basis: after crash-restart blocked={} event={} reason={}",
                gate.isBlocked(), gate.getBlockedEventId(), gate.getBlockReason());
        assertThat(gate.isBlocked()).isTrue();
        assertThat(gate.getBlockedEventId()).isEqualTo(e1);
        assertThat(gate.getBlockReason()).isEqualTo("RETRY_WAIT:SERVER_ERROR");
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-crash-retry", 1L)).isEqualTo(1);

        // 后面的事件不得越过队首：只有卡住的事件可被认领
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed after crash-restart={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).getId()).isEqualTo(e1);

        // 卡住的事件投递成功 -> 放行 -> 后续按序继续
        eventStore.markDelivered("t1", e1, "w2");
        List<DeliveryEvent> next = eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        assertThat(next).hasSize(1);
        assertThat(next.get(0).getSequence()).isEqualTo(2);
    }

    @Test
    void failedHeadBlockSurvivesCrashDuringStatePersist() throws Exception {
        openStores();
        String e1 = eventStore.submit(event("t1", "f1", "agg-crash-fail"), 100).event().getId();
        eventStore.submit(event("t1", "f2", "agg-crash-fail"), 100);
        eventStore.submit(event("t1", "f3", "agg-crash-fail"), 100);
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        // 队首彻底失败，落事件状态后、落闸门状态前进程被 kill
        openStoresArmed();
        eventStore.markFailed("t1", e1, "w1", "CLIENT_REJECTED", "receiver status 400");

        // 重启：聚合仍阻塞，FAILED 队首后面两件一件都不许出来
        openStores();
        AggregateGate gate = gateStore.snapshot("t1", "agg-crash-fail").orElseThrow();
        log.info("assertion basis: after crash-restart blocked={} reason={} queuedBehind={}",
                gate.isBlocked(), gate.getBlockReason(),
                eventStore.countQueuedForAggregate("t1", "agg-crash-fail", 1L));
        assertThat(gate.isBlocked()).isTrue();
        assertThat(gate.getBlockedEventId()).isEqualTo(e1);
        assertThat(gate.getBlockReason()).isEqualTo("FAILED:CLIENT_REJECTED");
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-crash-fail", 1L)).isEqualTo(2);
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed while failed head unresolved={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).as("followers must not pass the failed head").isEmpty();

        // 人工重放队首 -> 放行后严格按 1,2,3 顺序出来，不跳号
        assertThat(eventStore.replay("t1", e1)).isTrue();
        for (long seq = 1; seq <= 3; seq++) {
            List<DeliveryEvent> batch = eventStore.claimDeliverable("w" + (seq + 2), 10,
                    Duration.ofSeconds(5));
            assertThat(batch).hasSize(1);
            assertThat(batch.get(0).getSequence()).isEqualTo(seq);
            eventStore.markDelivered("t1", batch.get(0).getId(), "w" + (seq + 2));
        }
        assertThat(gateStore.snapshot("t1", "agg-crash-fail").orElseThrow().isBlocked()).isFalse();
    }

    @Test
    void staleBlockFromCrashIsClearedOnRestart() throws Exception {
        openStores();
        String e1 = eventStore.submit(event("t1", "d1", "agg-crash-stale"), 100).event().getId();
        eventStore.submit(event("t1", "d2", "agg-crash-stale"), 100);
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        eventStore.markRetry("t1", e1, "w1", "SERVER_ERROR",
                Instant.now().minusMillis(1), "receiver status 500");
        // 队首重试成功，落事件状态后、落闸门放行前进程被 kill
        eventStore.claimDeliverable("w2", 10, Duration.ofMillis(1));
        openStoresArmed();
        eventStore.markDelivered("t1", e1, "w2");

        // 重启：陈旧阻塞被清掉，已投递事件不重复出现，后续事件正常按序认领
        openStores();
        AggregateGate gate = gateStore.snapshot("t1", "agg-crash-stale").orElseThrow();
        log.info("assertion basis: after crash-restart stale block cleared, blocked={}",
                gate.isBlocked());
        assertThat(gate.isBlocked()).isFalse();
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed after stale-clear={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).getId()).isNotEqualTo(e1);
        assertThat(claimed.get(0).getSequence()).isEqualTo(2);
    }

    @Test
    void repeatedRestartsOfSameStateAreDeterministic() throws Exception {
        openStores();
        // 阻塞聚合：队首等重试，后排 1 件
        String b1 = eventStore.submit(event("t1", "b1", "agg-det-blocked"), 100).event().getId();
        eventStore.submit(event("t1", "b2", "agg-det-blocked"), 100);
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        eventStore.markRetry("t1", b1, "w1", "TIMEOUT",
                Instant.now().minusMillis(1), "receiver timeout");
        // 暂停聚合：2 件积压
        gateStore.pause("t1", "agg-det-paused");
        eventStore.submit(event("t1", "p1", "agg-det-paused"), 100);
        eventStore.submit(event("t1", "p2", "agg-det-paused"), 100);
        // 开放聚合：1 件已投递（重启后不得重复出现）
        String done = eventStore.submit(event("t1", "d1", "agg-det-open"), 100).event().getId();
        eventStore.claimDeliverable("w2", 10, Duration.ofMillis(1));
        eventStore.markDelivered("t1", done, "w2");

        // 第一次重启，记录恢复出的闸门与排队数
        openStores();
        AggregateGate blocked1 = gateStore.snapshot("t1", "agg-det-blocked").orElseThrow();
        int queuedBlocked1 = eventStore.countQueuedForAggregate("t1", "agg-det-blocked", null);
        int queuedPaused1 = eventStore.countQueuedForAggregate("t1", "agg-det-paused", null);

        // 第二次重启：同样的本地状态必须恢复出完全一致的闸门与排队数
        openStores();
        AggregateGate blocked2 = gateStore.snapshot("t1", "agg-det-blocked").orElseThrow();
        AggregateGate paused2 = gateStore.snapshot("t1", "agg-det-paused").orElseThrow();
        log.info("assertion basis: restart1 blockedEvent={} reason={} | restart2 blockedEvent={} reason={}",
                blocked1.getBlockedEventId(), blocked1.getBlockReason(),
                blocked2.getBlockedEventId(), blocked2.getBlockReason());
        assertThat(blocked2.isBlocked()).isTrue();
        assertThat(blocked2.getBlockedEventId()).isEqualTo(blocked1.getBlockedEventId());
        assertThat(blocked2.getBlockReason()).isEqualTo(blocked1.getBlockReason());
        assertThat(paused2.isPaused()).isTrue();
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-det-blocked", null))
                .isEqualTo(queuedBlocked1);
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-det-paused", null))
                .isEqualTo(queuedPaused1);

        // 恢复后的认领：只有阻塞聚合的队首可出；已投递事件不重复投递；排队件数不增不减
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed after repeated restarts={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).getId()).isEqualTo(b1);
        assertThat(claimed.stream().map(DeliveryEvent::getId).toList()).doesNotContain(done);

        // 第三次重启：认领未落任何新状态，恢复结果依旧一致
        openStores();
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-det-blocked", null))
                .isEqualTo(queuedBlocked1);
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-det-paused", null))
                .isEqualTo(queuedPaused1);
        assertThat(gateStore.snapshot("t1", "agg-det-blocked").orElseThrow().getBlockedEventId())
                .isEqualTo(blocked1.getBlockedEventId());
    }
}
