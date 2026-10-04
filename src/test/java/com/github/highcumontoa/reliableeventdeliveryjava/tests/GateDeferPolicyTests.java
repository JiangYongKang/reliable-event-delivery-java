package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.gate.GateStore;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
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
 * 闸门容量：DEFER 策略下排队上限同样生效——到顶后新提交不被接收，
 * 返回 429 GATE_CAPACITY_DEFERRED（带 Retry-After，与 REJECT 的 GATE_CAPACITY_EXCEEDED、
 * 全局 BACKPRESSURE_LIMIT 都可区分），排队数不越过上限；闸门打开后客户端重试成功，按原顺序投递。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.gate-max-queued-per-aggregate=2",
        "delivery.gate-overflow-policy=DEFER",
        "delivery.poll-interval=50ms"
})
class GateDeferPolicyTests {

    private static final Logger log = LoggerFactory.getLogger(GateDeferPolicyTests.class);
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

    @Autowired
    GateStore gateStore;

    @Autowired
    LoopbackReceiverController receiver;

    @Test
    void submissionsBeyondGateLimitAreDeferredUntilGateOpens() {
        String agg = "agg-defer-" + UUID.randomUUID();
        gateStore.pause("tenant-a", agg);

        // 上限为 2：前 2 件接收
        for (int i = 0; i < 2; i++) {
            ResponseEntity<String> resp = submit("k" + i + "-" + agg, agg, i);
            assertThat(resp.getStatusCode().value()).isEqualTo(202);
        }
        // 第 3/4 件到顶：DEFER 策略下不接收，原因可区分且带 Retry-After，队列不再增长
        String k2 = "k2-" + agg;
        String k3 = "k3-" + agg;
        ResponseEntity<String> deferred = submit(k2, agg, 2);
        ResponseEntity<String> deferredAgain = submit(k3, agg, 3);
        log.info("assertion basis: defer policy limit=2, 3rd/4th submit -> {} {} retryAfter={}",
                deferred.getStatusCode(), deferredAgain.getStatusCode(),
                deferred.getHeaders().getFirst("Retry-After"));
        assertThat(deferred.getStatusCode().value()).isEqualTo(429);
        assertThat(deferred.getBody()).contains("GATE_CAPACITY_DEFERRED");
        assertThat(deferred.getBody()).doesNotContain("GATE_CAPACITY_EXCEEDED");
        assertThat(deferred.getBody()).doesNotContain("BACKPRESSURE_LIMIT");
        assertThat(deferred.getHeaders().getFirst("Retry-After")).isNotBlank();
        assertThat(deferredAgain.getStatusCode().value()).isEqualTo(429);

        // 被推迟的提交不占幂等键、不入队：闸门打开后客户端原样重试即被接收
        gateStore.resume("tenant-a", agg);
        assertThat(submit(k2, agg, 2).getStatusCode().value()).isEqualTo(202);
        assertThat(submit(k3, agg, 3).getStatusCode().value()).isEqualTo(202);

        // 4 件按原提交顺序全部投递
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        List<Received> got = List.of();
        while (System.nanoTime() < deadline) {
            got = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).toList();
            if (got.size() == 4) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        long[] seqs = got.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: deliveries after defer+resume sequences={}",
                java.util.Arrays.toString(seqs));
        assertThat(seqs).containsExactly(1, 2, 3, 4);
    }

    private ResponseEntity<String> submit(String idem, String agg, int idx) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "token-a-secret");
        headers.set("Content-Type", "application/json");
        String body = "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"" + agg + "\","
                + "\"payload\":\"p" + idx + "\","
                + "\"targetUrl\":\"http://127.0.0.1:" + port + "/receiver/ok\"}";
        return rest.exchange("/api/events", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
