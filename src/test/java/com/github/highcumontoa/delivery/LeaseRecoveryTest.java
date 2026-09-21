package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.EventStatus;
import com.github.highcumontoa.delivery.store.EventStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 租约与并发认领：同一事件不得被两个执行单元同时认领；
 * 执行单元异常退出（租约未释放）后，事件在租约到期延迟内被重新认领；
 * 进程重启恢复时残留 IN_FLIGHT 被回收为 PENDING。
 */
class LeaseRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseRecoveryTest.class);
    private static final String DIR = "target/test-data/lease";

    private EventStore store;

    @BeforeEach
    void setUp() {
        new java.io.File(DIR).mkdirs();
        new java.io.File(DIR + "/store.json").delete();
        store = new EventStore(DIR + "/store.json");
        store.recover();
    }

    private DeliveryEvent newEvent(String tenant, String key, String agg) {
        DeliveryEvent e = new DeliveryEvent();
        e.setEventId(key);
        e.setTenantId(tenant);
        e.setIdempotencyKey(key);
        e.setAggregateKey(agg);
        e.setSequence(store.nextSequence(tenant, agg));
        e.setPayload("p");
        e.setContentHash("h-" + key);
        e.setStatus(EventStatus.PENDING);
        e.setNextAttemptAt(Instant.now());
        e.setCreatedAt(Instant.now());
        e.setUpdatedAt(Instant.now());
        return e;
    }

    @Test
    void concurrentClaimNeverDuplicates() throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            store.insertIdempotent(newEvent("t1", "lease-" + i, "agg-" + i));
        }
        int workers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workers);
        ConcurrentLinkedQueue<String> claimed = new ConcurrentLinkedQueue<>();
        for (int w = 0; w < workers; w++) {
            final String workerId = "w" + w;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int round = 0; round < 20; round++) {
                        Instant now = Instant.now();
                        for (DeliveryEvent e : store.claimBatch(workerId, 5, now, now.plusSeconds(30))) {
                            claimed.add(e.getEventId());
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        Set<String> unique = new HashSet<>(claimed);
        log.info("判定依据: 认领总数={} 去重后={} (期望相等且无重复认领)", claimed.size(), unique.size());
        assertEquals(claimed.size(), unique.size(), "同一事件不得被并发重复认领");
        assertEquals(50, unique.size(), "50 个事件应全部被认领一次");
    }

    @Test
    void expiredLeaseIsReclaimedByOtherWorker() {
        store.insertIdempotent(newEvent("t1", "lease-x", "agg-x"));
        Instant now = Instant.now();
        // worker-1 认领，租约 1 秒
        List<DeliveryEvent> first = store.claimBatch("worker-1", 1, now, now.plusSeconds(1));
        assertEquals(1, first.size());

        // 租约未到期：worker-2 不能认领
        List<DeliveryEvent> tooEarly = store.claimBatch("worker-2", 1, now.plusMillis(500), now.plusSeconds(2));
        log.info("判定依据: 租约未到期时其他单元认领数={} (期望=0)", tooEarly.size());
        assertTrue(tooEarly.isEmpty(), "租约未到期不得被重新认领");

        // 租约到期（worker-1 异常退出）：worker-2 在可控延迟内重新认领
        List<DeliveryEvent> reclaimed = store.claimBatch("worker-2", 1, now.plusSeconds(2), now.plusSeconds(3));
        log.info("判定依据: 租约到期后重新认领数={} 持有者={} (期望=1, worker-2)",
                reclaimed.size(), reclaimed.isEmpty() ? "-" : reclaimed.get(0).getLeaseOwner());
        assertEquals(1, reclaimed.size(), "租约到期后事件应被重新认领");
        assertEquals("worker-2", reclaimed.get(0).getLeaseOwner());
    }

    @Test
    void restartReclaimsInFlightWithoutLosingState() {
        store.insertIdempotent(newEvent("t1", "lease-r1", "agg-r"));
        store.insertIdempotent(newEvent("t1", "lease-r2", "agg-r"));
        Instant now = Instant.now();
        List<DeliveryEvent> claimed = store.claimBatch("worker-1", 1, now, now.plusSeconds(60));
        assertEquals(1, claimed.size(), "同聚合只认领队首");

        // 模拟进程重启：新实例加载同一文件
        EventStore restarted = new EventStore(DIR + "/store.json");
        restarted.recover();

        DeliveryEvent recovered = restarted.findById("t1", "lease-r1").orElseThrow();
        log.info("判定依据: 重启后残留 IN_FLIGHT 状态={} (期望=PENDING)", recovered.getStatus());
        assertEquals(EventStatus.PENDING, recovered.getStatus(), "重启后 IN_FLIGHT 应回收为 PENDING");

        // 回收后可被重新认领，且序号不重置（不跳号）
        List<DeliveryEvent> reclaimed = restarted.claimBatch("worker-2", 2, Instant.now(), Instant.now().plusSeconds(5));
        assertEquals(1, reclaimed.size());
        assertEquals(1, reclaimed.get(0).getSequence(), "恢复后仍从队首序号开始");
        long next = restarted.nextSequence("t1", "agg-r");
        log.info("判定依据: 重启后下一序号={} (期望=3, 序号连续不重置)", next);
        assertEquals(3, next, "序号在重启后保持连续");
    }
}
