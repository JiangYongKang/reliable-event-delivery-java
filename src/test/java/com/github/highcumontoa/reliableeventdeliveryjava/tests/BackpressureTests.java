package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 积压约束：待投递数量达到上限后，新提交按策略被拒绝（429 BACKPRESSURE_LIMIT），
 * 而不是无界占用内存。用不可达目标让事件停留在待投递/重试状态。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.max-pending=3",
        "delivery.max-attempts=100",
        "delivery.retry-base-backoff=30s",
        "delivery.poll-interval=50ms",
        "delivery.delivery-timeout=500ms"
})
class BackpressureTests {

    private static final Logger log = LoggerFactory.getLogger(BackpressureTests.class);
    private static final Path STORAGE_DIR = createTempDir();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("delivery.storage-dir", () -> STORAGE_DIR.toString());
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("event-store-bp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void submissionsBeyondLimitAreRejected() {
        // 不可达目标：连接失败 -> RETRY_WAIT（长退避），事件持续占用待投递额度
        String deadUrl = "http://127.0.0.1:1/receiver/ok";
        for (int i = 0; i < 3; i++) {
            ResponseEntity<SubmitEventResponse> resp = submit("k" + i + "-" + UUID.randomUUID(), deadUrl);
            assertThat(resp.getStatusCode().value()).isEqualTo(202);
        }
        // 等待投递尝试发生并进入 RETRY_WAIT
        awaitBackoff();

        ResponseEntity<String> rejected = submitRaw("overflow-" + UUID.randomUUID(), deadUrl);
        assertThat(rejected.getStatusCode().value()).isEqualTo(429);
        assertThat(rejected.getBody()).contains("BACKPRESSURE_LIMIT");
        log.info("assertion basis: pending limit=3 reached, 4th submit -> 429 BACKPRESSURE_LIMIT");
    }

    private void awaitBackoff() {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < deadline) {
            try {
                Thread.sleep(100);
                return; // 第一次失败投递在 1s 内必然发生（连接拒绝立即返回）
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private ResponseEntity<SubmitEventResponse> submit(String idem, String url) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "token-a-secret");
        headers.set("Content-Type", "application/json");
        String body = "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"agg-bp\","
                + "\"payload\":\"p\",\"targetUrl\":\"" + url + "\"}";
        return rest.exchange("/api/events", HttpMethod.POST,
                new HttpEntity<>(body, headers), SubmitEventResponse.class);
    }

    private ResponseEntity<String> submitRaw(String idem, String url) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "token-a-secret");
        headers.set("Content-Type", "application/json");
        String body = "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"agg-bp\","
                + "\"payload\":\"p\",\"targetUrl\":\"" + url + "\"}";
        return rest.exchange("/api/events", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
