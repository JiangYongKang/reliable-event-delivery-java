package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.highcumontoa.delivery.api.EventController.EventView;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 顺序保证：同一聚合键的事件按提交顺序生效；并发提交、并发投递与重试
 * 不得造成顺序倒置、跳号或重复生效。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18103",
        "delivery.loopback-base-url=http://127.0.0.1:18103",
        "delivery.store-path=target/test-data/ordering/store.json",
        "delivery.max-attempts=5",
        "delivery.backoff-base=50ms",
        "delivery.backoff-max=200ms",
        "delivery.poll-interval=20ms",
        "delivery.workers=4",
        "delivery.tenant-tokens.token-a=tenant-a"
})
class OrderingTest extends IntegrationTestBase {

    private static final int EVENT_COUNT = 20;

    static {
        new java.io.File("target/test-data/ordering").mkdirs();
        new java.io.File("target/test-data/ordering/store.json").delete();
    }

    @BeforeEach
    void resetLoopback() {
        rest.postForEntity("/loopback/reset", null, Map.class);
        setMode("OK");
    }

    private void setMode(String mode) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        rest.postForEntity("/loopback/behavior", new HttpEntity<>("{\"mode\":\"" + mode + "\"}", h), Map.class);
    }

    private void setFlaky(int failTimes) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        rest.postForEntity("/loopback/behavior",
                new HttpEntity<>("{\"mode\":\"FLAKY\",\"failTimes\":\"" + failTimes + "\"}", h), Map.class);
    }

    private void submitConcurrently(String agg, int count) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final int n = i;
            pool.submit(() -> {
                try {
                    start.await();
                    HttpHeaders h = new HttpHeaders();
                    h.setContentType(MediaType.APPLICATION_JSON);
                    h.set("X-Tenant-Token", "token-a");
                    String body = "{\"idempotencyKey\":\"ord-" + agg + "-" + n
                            + "\",\"aggregateKey\":\"" + agg + "\",\"payload\":\"p" + n + "\"}";
                    rest.postForEntity("/tenants/tenant-a/events", new HttpEntity<>(body, h), EventView.class);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await(30, TimeUnit.SECONDS);
        pool.shutdown();
    }

    @SuppressWarnings("unchecked")
    private List<Long> receivedSequences(String agg) {
        List<Map<String, Object>> received = rest.getForObject("/loopback/received", List.class);
        List<Long> seqs = new ArrayList<>();
        for (Map<String, Object> r : received) {
            if (agg.equals(r.get("aggregateKey"))) {
                seqs.add(((Number) r.get("sequence")).longValue());
            }
        }
        return seqs;
    }

    private void assertStrictlyOrdered(String agg) {
        List<Long> seqs = receivedSequences(agg);
        List<Long> expected = new ArrayList<>();
        for (long i = 1; i <= EVENT_COUNT; i++) {
            expected.add(i);
        }
        log.info("判定依据: 聚合={} 接收序列={} (期望严格递增 1..{} 且无重复)", agg, seqs, EVENT_COUNT);
        assertEquals(expected, seqs, "接收序列必须严格按提交顺序生效，无倒置/跳号/重复");
    }

    @Test
    void concurrentSubmissionPreservesOrder() throws InterruptedException {
        String agg = "agg-order-1";
        submitConcurrently(agg, EVENT_COUNT);
        await("all events delivered", () -> receivedSequences(agg).size() == EVENT_COUNT);
        assertStrictlyOrdered(agg);
    }

    @Test
    void retryDoesNotInvertOrder() throws InterruptedException {
        String agg = "agg-order-2";
        // 前 3 次投递失败（500），触发重试与后续事件的竞争
        setFlaky(3);
        submitConcurrently(agg, EVENT_COUNT);
        await("all events delivered despite retries", () -> receivedSequences(agg).size() == EVENT_COUNT);
        assertStrictlyOrdered(agg);
        setMode("OK");
    }
}
