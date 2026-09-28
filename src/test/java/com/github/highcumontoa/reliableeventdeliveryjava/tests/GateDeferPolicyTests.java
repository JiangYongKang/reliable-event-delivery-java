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
 * 闸门容量：DEFER 策略下，排队到顶后新提交照常接收（202）并排队，
 * 等闸门打开后按原顺序投递；内存仍受全局 max-pending 约束。
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
    void submissionsBeyondGateLimitAreDeferredNotRejected() {
        String agg = "agg-defer-" + UUID.randomUUID();
        gateStore.pause("tenant-a", agg);

        // 上限为 2，提交 4 件：DEFER 策略下全部接收
        for (int i = 0; i < 4; i++) {
            ResponseEntity<String> resp = submit("k" + i + "-" + UUID.randomUUID(), agg, i);
            assertThat(resp.getStatusCode().value()).isEqualTo(202);
        }
        log.info("assertion basis: defer policy accepted 4 submissions over limit=2");

        // 恢复后按原提交顺序全部投递
        gateStore.resume("tenant-a", agg);
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
        log.info("assertion basis: deferred deliveries sequences={}", java.util.Arrays.toString(seqs));
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
