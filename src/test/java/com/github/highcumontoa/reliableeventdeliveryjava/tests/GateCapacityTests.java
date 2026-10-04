package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.gate.GateStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.EventStore;
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
 * 闸门容量：被暂停/阻塞的聚合排队到顶后，REJECT 策略下新提交返回 429 GATE_CAPACITY_EXCEEDED
 * （与全局 BACKPRESSURE_LIMIT 可区分），未闸门的聚合不受影响。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.gate-max-queued-per-aggregate=2",
        "delivery.gate-overflow-policy=REJECT",
        "delivery.poll-interval=50ms"
})
class GateCapacityTests {

    private static final Logger log = LoggerFactory.getLogger(GateCapacityTests.class);
    private static final Path STORAGE_DIR = createTempDir();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("delivery.storage-dir", () -> STORAGE_DIR.toString());
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("event-store-gate-cap");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Autowired
    TestRestTemplate rest;

    @Autowired
    GateStore gateStore;

    @Autowired
    EventStore eventStore;

    @Test
    void submissionsBeyondGateLimitAreRejectedWithDistinctReason() {
        String agg = "agg-cap-" + UUID.randomUUID();
        gateStore.pause("tenant-a", agg);

        for (int i = 0; i < 2; i++) {
            ResponseEntity<String> resp = submit("k" + i + "-" + UUID.randomUUID(), agg);
            assertThat(resp.getStatusCode().value()).isEqualTo(202);
        }
        // 第 3 件到顶：拒绝，原因与全局背压可区分
        ResponseEntity<String> rejected = submit("overflow-" + UUID.randomUUID(), agg);
        log.info("assertion basis: gate limit=2 reached, 3rd submit -> {} body={}",
                rejected.getStatusCode(), rejected.getBody());
        assertThat(rejected.getStatusCode().value()).isEqualTo(429);
        assertThat(rejected.getBody()).contains("GATE_CAPACITY_EXCEEDED");
        assertThat(rejected.getBody()).doesNotContain("BACKPRESSURE_LIMIT");

        // 未闸门的聚合不受闸门上限影响
        ResponseEntity<String> free = submit("free-" + UUID.randomUUID(), "agg-free-" + UUID.randomUUID());
        assertThat(free.getStatusCode().value()).isEqualTo(202);
    }

    @Test
    void blockedAggregateAlsoEnforcesLimitAndRejectedSubmissionsAreNotStored() {
        String agg = "agg-cap-blocked-" + UUID.randomUUID();
        // 队首彻底失败造成阻塞（直接落阻塞态，等价于队首 FAILED 后的自动阻塞）
        assertThat(submit("b0-" + UUID.randomUUID(), agg).getStatusCode().value()).isEqualTo(202);
        String headId = eventStore.listByTenant("tenant-a").stream()
                .filter(e -> e.getAggregateKey().equals(agg)).findFirst().orElseThrow().getId();
        gateStore.block("tenant-a", agg, headId, "FAILED:CLIENT_REJECTED");

        // 上限=2：队首已占 1 件，第 2 件接收（queued=1 < 2），第 3 件起到顶拒绝
        assertThat(submit("b1-" + UUID.randomUUID(), agg).getStatusCode().value()).isEqualTo(202);
        ResponseEntity<String> rejected = submit("b2-" + UUID.randomUUID(), agg);
        log.info("assertion basis: blocked agg at limit=2, overflow submit -> {} body={}",
                rejected.getStatusCode(), rejected.getBody());
        assertThat(rejected.getStatusCode().value()).isEqualTo(429);
        assertThat(rejected.getBody()).contains("GATE_CAPACITY_EXCEEDED");

        // 继续提交仍拒绝，且被拒绝的提交没有入库：排队数稳定在上限，不无界增长
        assertThat(submit("b3-" + UUID.randomUUID(), agg).getStatusCode().value()).isEqualTo(429);
        int queued = eventStore.countQueuedForAggregate("tenant-a", agg, null);
        log.info("assertion basis: after rejected submits queued={} (limit=2)", queued);
        assertThat(queued).isEqualTo(2);
    }

    private ResponseEntity<String> submit(String idem, String agg) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "token-a-secret");
        headers.set("Content-Type", "application/json");
        String body = "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"" + agg + "\","
                + "\"payload\":\"p\",\"targetUrl\":\"http://127.0.0.1:1/receiver/ok\"}";
        return rest.exchange("/api/events", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
