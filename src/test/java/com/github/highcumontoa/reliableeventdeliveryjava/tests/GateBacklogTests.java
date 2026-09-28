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
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 单聚合积压上限（REJECT 策略）：被暂停聚合的排队事件到顶后，新提交返回 429
 * AGGREGATE_BACKLOG_LIMIT（区别于全局 BACKPRESSURE_LIMIT）；不同聚合与全局额度互不影响。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.gate-max-backlog=3",
        "delivery.gate-overflow-policy=REJECT",
        "delivery.max-pending=10000",
        "delivery.poll-interval=50ms",
        "delivery.delivery-timeout=1s"
})
class GateBacklogTests {

    private static final Logger log = LoggerFactory.getLogger(GateBacklogTests.class);
    private static final Path STORAGE_DIR = createTempDir();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("delivery.storage-dir", () -> STORAGE_DIR.toString());
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("event-store-gate-backlog");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Test
    void pausedAggregateBacklogIsBoundedWithDistinctRejectReason() {
        String agg = "agg-cap-" + UUID.randomUUID();
        String other = "agg-cap-other-" + UUID.randomUUID();
        gate(agg, "pause");

        String okUrl = "http://127.0.0.1:" + port + "/receiver/ok";
        // 暂停聚合可收满 3 件
        for (int i = 1; i <= 3; i++) {
            ResponseEntity<SubmitEventResponse> r = submit(agg, "k-" + agg + "-" + i, okUrl);
            assertThat(r.getStatusCode().value()).isEqualTo(202);
        }
        // 第 4 件：429 且原因为 AGGREGATE_BACKLOG_LIMIT（独立错误码，可与全局 BACKPRESSURE_LIMIT 区分）
        ResponseEntity<String> rejected = submitRaw(agg, "k-" + agg + "-overflow", okUrl);
        assertThat(rejected.getStatusCode().value()).isEqualTo(429);
        assertThat(rejected.getBody()).contains("\"code\":\"AGGREGATE_BACKLOG_LIMIT\"");
        assertThat(rejected.getBody()).doesNotContain("\"code\":\"BACKPRESSURE_LIMIT\"");
        log.info("assertion basis: paused aggregate cap=3, 4th submit -> 429 AGGREGATE_BACKLOG_LIMIT");

        // 别的聚合不受这个上限影响
        ResponseEntity<SubmitEventResponse> otherOk = submit(other, "k-" + other + "-1", okUrl);
        assertThat(otherOk.getStatusCode().value()).isEqualTo(202);
        log.info("assertion basis: other aggregate unaffected by per-aggregate cap");

        // 恢复后积压被投递消化，腾出的名额可再次提交
        gate(agg, "resume");
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(15).toNanos();
        ResponseEntity<SubmitEventResponse> accepted = null;
        while (System.nanoTime() < deadline) {
            accepted = submit(agg, "k-" + agg + "-after", okUrl);
            if (accepted.getStatusCode().value() == 202) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(accepted).isNotNull();
        assertThat(accepted.getStatusCode().value()).isEqualTo(202);
        log.info("assertion basis: after resume queue drained, new submit accepted 202");
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
