package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 单聚合积压上限（DEFER 策略）：到顶后新提交不立即失败，而是在限时内等待腾出名额；
 * 队列很快消化时提交应被推迟后成功（202），一直不消化时超时返回 429 AGGREGATE_BACKLOG_LIMIT。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.gate-max-backlog=3",
        "delivery.gate-overflow-policy=DEFER",
        "delivery.gate-defer-timeout=1s",
        "delivery.max-pending=10000",
        "delivery.worker-count=2",
        "delivery.poll-interval=50ms",
        "delivery.delivery-timeout=1s"
})
class GateBacklogDeferTests {

    private static final Logger log = LoggerFactory.getLogger(GateBacklogDeferTests.class);
    private static final Path STORAGE_DIR = createTempDir();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("delivery.storage-dir", () -> STORAGE_DIR.toString());
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("event-store-gate-defer");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Test
    void deferredSubmitSucceedsOnceQueueDrainsAfterResume() throws Exception {
        String agg = "agg-defer-" + UUID.randomUUID();
        String okUrl = "http://127.0.0.1:" + port + "/receiver/ok";
        gate(agg, "pause");
        for (int i = 1; i <= 3; i++) {
            assertThat(submit(agg, "k-" + agg + "-" + i, okUrl).getStatusCode().value()).isEqualTo(202);
        }

        // 第 4 件在另一线程提交：满员 -> 进入 DEFER 等待（不立即返回）
        ExecutorService pool = Executors.newSingleThreadExecutor();
        CountDownLatch submitted = new CountDownLatch(1);
        AtomicInteger status = new AtomicInteger();
        long start = System.nanoTime();
        pool.submit(() -> {
            ResponseEntity<SubmitEventResponse> r = submit(agg, "k-" + agg + "-deferred", okUrl);
            status.set(r.getStatusCode().value());
            submitted.countDown();
        });

        // 确认它确实在等待，而不是立刻成功或失败
        Thread.sleep(400);
        assertThat(submitted.getCount()).as("submit should be deferred while queue full").isEqualTo(1L);

        // 恢复闸门：暂停期间积压被快速消化，DEFER 的提交应在超时前拿到名额并 202
        gate(agg, "resume");
        assertThat(submitted.await(10, TimeUnit.SECONDS)).isTrue();
        long waitedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(status.get()).isEqualTo(202);
        log.info("assertion basis: deferred submit waitedMs={} then accepted 202", waitedMs);
        pool.shutdownNow();
    }

    @Test
    void deferredSubmitTimesOutWithDistinctReasonWhenQueueNeverDrains() throws Exception {
        String agg = "agg-defer-timeout-" + UUID.randomUUID();
        String okUrl = "http://127.0.0.1:" + port + "/receiver/ok";
        gate(agg, "pause");
        for (int i = 1; i <= 3; i++) {
            assertThat(submit(agg, "k-" + agg + "-" + i, okUrl).getStatusCode().value()).isEqualTo(202);
        }

        long start = System.nanoTime();
        ResponseEntity<String> r = submitRaw(agg, "k-" + agg + "-never", okUrl);
        long waitedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(r.getStatusCode().value()).isEqualTo(429);
        assertThat(r.getBody()).contains("\"code\":\"AGGREGATE_BACKLOG_LIMIT\"");
        // 确实等待了约一个 defer-timeout 而非立即拒绝
        assertThat(waitedMs).isGreaterThanOrEqualTo(800);
        log.info("assertion basis: queue never drained, deferred submit waitedMs={} -> 429", waitedMs);
    }

    private ResponseEntity<SubmitEventResponse> submit(String agg, String idem, String target) {
        return rest.exchange("/api/events", HttpMethod.POST,
                new HttpEntity<>(body(agg, idem, target), jsonHeaders()), SubmitEventResponse.class);
    }

    private ResponseEntity<String> submitRaw(String agg, String idem, String target) {
        return rest.exchange("/api/events", HttpMethod.POST,
                new HttpEntity<>(body(agg, idem, target), jsonHeaders()), String.class);
    }

    private ResponseEntity<String> gate(String agg, String action) {
        return rest.exchange("/api/gates/" + enc(agg) + "/" + action, HttpMethod.POST,
                new HttpEntity<>("{}", jsonHeaders()), String.class);
    }

    private String body(String agg, String idem, String target) {
        return "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"" + agg + "\","
                + "\"payload\":\"p\",\"targetUrl\":\"" + target + "\"}";
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "token-a-secret");
        headers.set("Content-Type", "application/json");
        return headers;
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
    }
}
