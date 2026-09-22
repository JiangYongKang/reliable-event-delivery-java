package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** 顺序保证：并发提交同一聚合键的事件，接收端必须按序号顺序生效，不倒置、不跳号、不重复 */
class OrderingTests extends BaseIntegrationTest {

    @Test
    void concurrentSubmissionsAreDeliveredInSequence() throws Exception {
        String agg = "agg-ord-" + UUID.randomUUID();
        int n = 20;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < n; i++) {
            int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    submit(TOKEN_A, "idem-" + agg + "-" + idx, agg, "payload-" + idx, loopbackUrl("ok"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(15, TimeUnit.SECONDS)).isTrue();

        // 等待该聚合全部 n 条被接收
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        List<Received> got = List.of();
        while (System.nanoTime() < deadline) {
            got = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).toList();
            if (got.size() == n) {
                break;
            }
            Thread.sleep(50);
        }
        assertThat(got).as("all events of aggregate must be delivered").hasSize(n);

        // 判定依据：接收到的 sequence 必须严格递增 1..n，无重复无跳号
        long[] seqs = got.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: received sequences={}", java.util.Arrays.toString(seqs));
        for (int i = 0; i < n; i++) {
            assertThat(seqs[i]).as("position %d must be sequence %d", i, i + 1).isEqualTo(i + 1);
        }
    }

    @Test
    void retriedHeadEventDoesNotReorderAggregate() {
        // 同一聚合两条事件，目标先 flaky 后恢复：第一条重试期间第二条不得先投递
        receiver.setFlakyFailures(1);
        String agg = "agg-ord2-" + UUID.randomUUID();
        submit(TOKEN_A, "idem-" + agg + "-1", agg, "first", loopbackUrl("flaky"));
        submit(TOKEN_A, "idem-" + agg + "-2", agg, "second", loopbackUrl("flaky"));

        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        List<Received> got = List.of();
        while (System.nanoTime() < deadline) {
            got = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).toList();
            if (got.size() == 2) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(got).hasSize(2);
        log.info("assertion basis: order after retry seq1={} seq2={}", got.get(0).sequence(), got.get(1).sequence());
        assertThat(got.get(0).sequence()).isLessThan(got.get(1).sequence());
        assertThat(got.get(0).payload()).isEqualTo("first");
        assertThat(got.get(1).payload()).isEqualTo("second");
    }
}
