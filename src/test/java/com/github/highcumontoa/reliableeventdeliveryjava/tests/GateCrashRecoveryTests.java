package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.AggregateGate;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.GateStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.FileEventStore;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 崩溃恢复：落状态分两步（先 events.json 后 gates.json），进程在两者之间被强杀时，
 * 重启后必须以事件实际状态为准重建闸门阻塞——队首没处理完就仍然阻塞，
 * 能查到卡在哪个事件、因为什么、后面还排几件，且后续事件一件都不能越过。
 * 同一份本地状态反复重启结果一致：不重复投递、不跳号、排队数不增不减。
 */
class GateCrashRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(GateCrashRecoveryTests.class);

    @TempDir
    Path dir;

    private FileEventStore eventStore;
    private GateStore gateStore;

    /** 模拟一次进程启动：同一目录重新打开事件存储与闸门存储（触发启动对账） */
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

    private Path gatesFile() {
        return dir.resolve("gates.json");
    }

    /** 模拟“事件状态已落盘、闸门状态尚未落盘”的强杀：闸门文件回滚到变更前的字节 */
    private void simulateCrashBeforeGatePersist(byte[] gatesFileBytesBefore) throws Exception {
        if (gatesFileBytesBefore == null) {
            Files.deleteIfExists(gatesFile());
        } else {
            Files.write(gatesFile(), gatesFileBytesBefore);
        }
    }

    private static byte[] readOrNull(Path p) throws Exception {
        return Files.exists(p) ? Files.readAllBytes(p) : null;
    }

    @Test
    void retryWaitHeadStaysBlockedAfterCrashBetweenPersists() throws Exception {
        openStores();
        String e1 = eventStore.submit(event("t1", "c1", "agg-crash"), 100).event().getId();
        String e2 = eventStore.submit(event("t1", "c2", "agg-crash"), 100).event().getId();
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        byte[] gatesBefore = readOrNull(gatesFile());
        eventStore.markRetry("t1", e1, "w1", "SERVER_ERROR",
                Instant.now().minusMillis(1), "receiver status 500");
        // 在 gates.json 落盘前被强杀：事件已是 RETRY_WAIT，闸门记录丢失
        simulateCrashBeforeGatePersist(gatesBefore);

        openStores();
        AggregateGate gate = gateStore.snapshot("t1", "agg-crash").orElseThrow();
        log.info("assertion basis: after crash-restart blocked={} event={} reason={}",
                gate.isBlocked(), gate.getBlockedEventId(), gate.getBlockReason());
        assertThat(gate.isBlocked()).isTrue();
        assertThat(gate.getBlockedEventId()).isEqualTo(e1);
        assertThat(gate.getBlockReason()).isEqualTo("RETRY_WAIT:SERVER_ERROR");
        // 卡在 e1 后面还排 1 件
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-crash", 1L)).isEqualTo(1);

        // 后续事件一件都不能越过队首：只有卡住的 e1 可被认领
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed after crash-restart={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).extracting(DeliveryEvent::getId).containsExactly(e1);
        assertThat(claimed).extracting(DeliveryEvent::getId).doesNotContain(e2);

        // 同一份状态再重启一次：结果完全一致（不重复、不增减）
        eventStore.markRetry("t1", e1, "w2", "SERVER_ERROR",
                Instant.now().minusMillis(1), "receiver status 500");
        openStores();
        AggregateGate again = gateStore.snapshot("t1", "agg-crash").orElseThrow();
        log.info("assertion basis: second restart blocked={} event={} reason={}",
                again.isBlocked(), again.getBlockedEventId(), again.getBlockReason());
        assertThat(again.isBlocked()).isTrue();
        assertThat(again.getBlockedEventId()).isEqualTo(e1);
        assertThat(again.getBlockReason()).isEqualTo("RETRY_WAIT:SERVER_ERROR");
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-crash", 1L)).isEqualTo(1);

        // 卡住的事件最终投递成功 -> 自动放行 -> e2 按序补上，不跳号
        eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        eventStore.markDelivered("t1", e1, "w3");
        assertThat(gateStore.snapshot("t1", "agg-crash").orElseThrow().isBlocked()).isFalse();
        List<DeliveryEvent> next = eventStore.claimDeliverable("w4", 10, Duration.ofSeconds(5));
        log.info("assertion basis: after unblock claimed seqs={}",
                next.stream().map(DeliveryEvent::getSequence).toList());
        assertThat(next).extracting(DeliveryEvent::getId).containsExactly(e2);
        assertThat(next.get(0).getSequence()).isEqualTo(2);
    }

    @Test
    void failedHeadStaysBlockedAfterCrashBetweenPersists() throws Exception {
        openStores();
        String e1 = eventStore.submit(event("t1", "f1", "agg-fail"), 100).event().getId();
        String e2 = eventStore.submit(event("t1", "f2", "agg-fail"), 100).event().getId();
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        byte[] gatesBefore = readOrNull(gatesFile());
        eventStore.markFailed("t1", e1, "w1", "CLIENT_REJECTED", "receiver status 400");
        simulateCrashBeforeGatePersist(gatesBefore);

        openStores();
        AggregateGate gate = gateStore.snapshot("t1", "agg-fail").orElseThrow();
        log.info("assertion basis: failed-head after restart blocked={} event={} reason={}",
                gate.isBlocked(), gate.getBlockedEventId(), gate.getBlockReason());
        assertThat(gate.isBlocked()).isTrue();
        assertThat(gate.getBlockedEventId()).isEqualTo(e1);
        assertThat(gate.getBlockReason()).isEqualTo("FAILED:CLIENT_REJECTED");
        assertThat(eventStore.countQueuedForAggregate("t1", "agg-fail", 1L)).isEqualTo(1);

        // FAILED 是终态不可认领，e2 又被闸门挡住：一件都出不来
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed while head failed={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).isEmpty();

        // 人工重放队首：重启后阻塞仍然指向它（重放途中不放行），只有它可被认领
        eventStore.replay("t1", e1);
        openStores();
        AggregateGate afterReplay = gateStore.snapshot("t1", "agg-fail").orElseThrow();
        log.info("assertion basis: after replay+restart blocked={} event={}",
                afterReplay.isBlocked(), afterReplay.getBlockedEventId());
        assertThat(afterReplay.isBlocked()).isTrue();
        assertThat(afterReplay.getBlockedEventId()).isEqualTo(e1);
        List<DeliveryEvent> reclaimed = eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        assertThat(reclaimed).extracting(DeliveryEvent::getId).containsExactly(e1);

        // 重放投递成功 -> 放行 -> e2 按序生效
        eventStore.markDelivered("t1", e1, "w3");
        List<DeliveryEvent> next = eventStore.claimDeliverable("w4", 10, Duration.ofSeconds(5));
        assertThat(next).extracting(DeliveryEvent::getId).containsExactly(e2);
    }

    @Test
    void staleBlockIsHealedWhenHeadWasActuallyDelivered() throws Exception {
        openStores();
        String e1 = eventStore.submit(event("t1", "s1", "agg-stale"), 100).event().getId();
        String e2 = eventStore.submit(event("t1", "s2", "agg-stale"), 100).event().getId();
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        eventStore.markRetry("t1", e1, "w1", "TIMEOUT",
                Instant.now().minusMillis(1), "read timed out");
        // 此刻闸门文件是“阻塞在 e1”的状态；随后 e1 重试成功
        byte[] blockedGates = readOrNull(gatesFile());
        eventStore.claimDeliverable("w2", 10, Duration.ofMillis(1));
        eventStore.markDelivered("t1", e1, "w2");
        // 在闸门放行落盘前被强杀：事件已 DELIVERED，闸门仍是旧的阻塞状态
        simulateCrashBeforeGatePersist(blockedGates);

        openStores();
        AggregateGate gate = gateStore.snapshot("t1", "agg-stale").orElseThrow();
        log.info("assertion basis: stale block healed, blocked={}", gate.isBlocked());
        assertThat(gate.isBlocked()).isFalse();
        // 已投递的 e1 不会重复投递，e2 正常按序认领
        List<DeliveryEvent> claimed = eventStore.claimDeliverable("w3", 10, Duration.ofSeconds(5));
        log.info("assertion basis: claimed after heal={}",
                claimed.stream().map(DeliveryEvent::getId).toList());
        assertThat(claimed).extracting(DeliveryEvent::getId).containsExactly(e2);
    }

    @Test
    void repeatedRestartsAreDeterministicAndDoNotDuplicateDelivered() throws Exception {
        openStores();
        // 已投递事件：重启后绝不能重复投递
        String done = eventStore.submit(event("t1", "d1", "agg-open"), 100).event().getId();
        eventStore.claimDeliverable("w1", 10, Duration.ofMillis(1));
        eventStore.markDelivered("t1", done, "w1");
        // 暂停聚合：两件排队
        gateStore.pause("t1", "agg-paused");
        eventStore.submit(event("t1", "p1", "agg-paused"), 100);
        eventStore.submit(event("t1", "p2", "agg-paused"), 100);
        // 阻塞聚合：队首等重试，闸门落盘前崩溃
        String head = eventStore.submit(event("t1", "b1", "agg-blocked"), 100).event().getId();
        eventStore.submit(event("t1", "b2", "agg-blocked"), 100);
        eventStore.claimDeliverable("w2", 10, Duration.ofMillis(1));
        byte[] gatesBefore = readOrNull(gatesFile());
        eventStore.markRetry("t1", head, "w2", "CONNECTION",
                Instant.now().minusMillis(1), "connection refused");
        simulateCrashBeforeGatePersist(gatesBefore);

        // 连续重启两次，两次恢复的闸门与排队数必须完全一致
        for (int restart = 1; restart <= 2; restart++) {
            openStores();
            AggregateGate blocked = gateStore.snapshot("t1", "agg-blocked").orElseThrow();
            AggregateGate paused = gateStore.snapshot("t1", "agg-paused").orElseThrow();
            int queuedBlocked = eventStore.countQueuedForAggregate("t1", "agg-blocked", 1L);
            int queuedPaused = eventStore.countQueuedForAggregate("t1", "agg-paused", null);
            log.info("assertion basis: restart#{} blocked={} reason={} queuedBlocked={} paused={} queuedPaused={}",
                    restart, blocked.isBlocked(), blocked.getBlockReason(), queuedBlocked,
                    paused.isPaused(), queuedPaused);
            assertThat(blocked.isBlocked()).isTrue();
            assertThat(blocked.getBlockedEventId()).isEqualTo(head);
            assertThat(blocked.getBlockReason()).isEqualTo("RETRY_WAIT:CONNECTION");
            assertThat(paused.isPaused()).isTrue();
            assertThat(queuedBlocked).isEqualTo(1);
            assertThat(queuedPaused).isEqualTo(2);
            // 已投递事件不出现在认领结果里；暂停聚合一件不出；阻塞聚合只出队首
            List<DeliveryEvent> claimed = eventStore.claimDeliverable("w9" + restart, 10,
                    Duration.ofMillis(1));
            log.info("assertion basis: restart#{} claimed={}",
                    restart, claimed.stream().map(DeliveryEvent::getId).toList());
            assertThat(claimed).extracting(DeliveryEvent::getId).doesNotContain(done);
            assertThat(claimed).extracting(DeliveryEvent::getAggregateKey)
                    .doesNotContain("agg-paused");
            // 第一次重启认领了队首（租约 1ms 已过期），第二次重启租约过期后仍只能认领同一队首
            assertThat(claimed).extracting(DeliveryEvent::getId).containsExactly(head);
            // 事件总数不变：没有凭空多出来或丢失
            assertThat(eventStore.listByTenant("t1")).hasSize(5);
        }
    }
}
