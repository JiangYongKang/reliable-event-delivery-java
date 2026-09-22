package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.store.FileEventStore;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 租约语义：并发认领互斥、执行单元异常退出后租约到期被回收、失效租约持有者无法落状态 */
class LeaseRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(LeaseRecoveryTests.class);

    @TempDir
    Path dir;

    private FileEventStore newStore() throws Exception {
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
        e.setPayload("p");
        e.setTargetUrl("http://127.0.0.1/receiver/ok");
        return e;
    }

    @Test
    void concurrentClaimsAreMutuallyExclusive() throws Exception {
        FileEventStore store = newStore();
        for (int i = 0; i < 10; i++) {
            store.submit(event("t1", "k" + i, "agg-" + i), 1000);
        }
        int workers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> claimedIds = ConcurrentHashMap.newKeySet();
        Set<String> duplicates = ConcurrentHashMap.newKeySet();
        for (int w = 0; w < workers; w++) {
            String workerId = "w" + w;
            pool.submit(() -> {
                try {
                    start.await();
                    List<DeliveryEvent> claimed = store.claimDeliverable(workerId, 3, Duration.ofSeconds(30));
                    for (DeliveryEvent e : claimed) {
                        if (!claimedIds.add(e.getId())) {
                            duplicates.add(e.getId());
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        log.info("assertion basis: claimed={} duplicates={}", claimedIds.size(), duplicates.size());
        assertThat(duplicates).as("same event must never be claimed by two workers").isEmpty();
        assertThat(claimedIds).hasSize(10);
    }

    @Test
    void expiredLeaseIsReclaimedAndStaleOwnerCannotCommit() throws Exception {
        FileEventStore store = newStore();
        String id = store.submit(event("t1", "k1", "agg-x"), 100).event().getId();

        // worker-A 以极短租约认领，模拟随后异常退出（不落状态）
        List<DeliveryEvent> a = store.claimDeliverable("worker-A", 1, Duration.ofMillis(50));
        assertThat(a).hasSize(1);

        // 租约未过期时 worker-B 认领不到
        assertThat(store.claimDeliverable("worker-B", 1, Duration.ofSeconds(1))).isEmpty();

        Thread.sleep(80);
        // 租约过期后 worker-B 回收该事件
        List<DeliveryEvent> b = store.claimDeliverable("worker-B", 1, Duration.ofSeconds(30));
        assertThat(b).hasSize(1);
        log.info("assertion basis: event={} reclaimed by worker-B after lease expiry", id);

        // 失效的 worker-A 无法落状态
        assertThat(store.markDelivered("t1", id, "worker-A")).isFalse();
        // 当前持有者 worker-B 可以落状态
        assertThat(store.markDelivered("t1", id, "worker-B")).isTrue();
        assertThat(store.findById("t1", id).orElseThrow().getStatus().name()).isEqualTo("DELIVERED");
        log.info("assertion basis: stale owner rejected, current owner committed, status=DELIVERED");
    }
}
