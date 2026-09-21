package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.delivery.api.EventController.EventView;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 故障分类与重试：超时/5xx 为暂时性，按退避重试直至耗尽进入 FAILED；
 * 4xx 为永久性，立即进入 FAILED 且不做无意义重试。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18102",
        "delivery.loopback-base-url=http://127.0.0.1:18102",
        "delivery.store-path=target/test-data/failure/store.json",
        "delivery.max-attempts=3",
        "delivery.backoff-base=50ms",
        "delivery.backoff-max=200ms",
        "delivery.poll-interval=50ms",
        "delivery.delivery-timeout=500ms",
        "delivery.tenant-tokens.token-a=tenant-a"
})
class FailureClassificationTest extends IntegrationTestBase {

    static {
        new java.io.File("target/test-data/failure").mkdirs();
        new java.io.File("target/test-data/failure/store.json").delete();
    }

    @BeforeEach
    void resetLoopback() {
        rest.postForEntity("/loopback/reset", null, Map.class);
    }

    private void setMode(String mode) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        rest.postForEntity("/loopback/behavior", new HttpEntity<>("{\"mode\":\"" + mode + "\"}", h), Map.class);
    }

    private String submit(String key, String agg) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-Tenant-Token", "token-a");
        String body = "{\"idempotencyKey\":\"" + key + "\",\"aggregateKey\":\"" + agg + "\",\"payload\":\"p\"}";
        ResponseEntity<EventView> r = rest.postForEntity("/tenants/tenant-a/events", new HttpEntity<>(body, h), EventView.class);
        return r.getBody().eventId();
    }

    private EventView get(String eventId) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-Tenant-Token", "token-a");
        return rest.exchange("/tenants/tenant-a/events/" + eventId, HttpMethod.GET,
                new HttpEntity<>(h), EventView.class).getBody();
    }

    @Test
    void serverErrorIsRetriedThenExhausted() {
        setMode("SERVER_ERROR");
        String eventId = submit("fc-500", "agg-fc1");
        await("retries exhausted -> FAILED", () -> {
            EventView v = get(eventId);
            return v != null && v.status().name().equals("FAILED");
        });
        EventView v = get(eventId);
        log.info("判定依据: 5xx 暂时性失败 kind={} attempts={} (期望=SERVER_ERROR, 3)", v.lastFailureKind(), v.attemptCount());
        assertEquals("SERVER_ERROR", v.lastFailureKind());
        assertEquals(3, v.attemptCount(), "应按 max-attempts 重试后耗尽");
        assertTrue(v.lastError().contains("retries exhausted"));
        setMode("OK");
    }

    @Test
    void timeoutIsRetriedThenExhausted() {
        setMode("TIMEOUT");
        String eventId = submit("fc-timeout", "agg-fc2");
        await("timeout retries exhausted -> FAILED", () -> {
            EventView v = get(eventId);
            return v != null && v.status().name().equals("FAILED");
        });
        EventView v = get(eventId);
        log.info("判定依据: 超时暂时性失败 kind={} attempts={} (期望=TIMEOUT, 3)", v.lastFailureKind(), v.attemptCount());
        assertEquals("TIMEOUT", v.lastFailureKind());
        assertEquals(3, v.attemptCount());
        setMode("OK");
    }

    @Test
    void rejectedIsPermanentAndNotRetried() {
        setMode("REJECT");
        String eventId = submit("fc-400", "agg-fc3");
        await("permanent rejection -> FAILED", () -> {
            EventView v = get(eventId);
            return v != null && v.status().name().equals("FAILED");
        });
        EventView v = get(eventId);
        log.info("判定依据: 4xx 永久性失败 kind={} attempts={} (期望=REJECTED, 1)", v.lastFailureKind(), v.attemptCount());
        assertEquals("REJECTED", v.lastFailureKind());
        assertEquals(1, v.attemptCount(), "永久性失败不得无意义重试");
        setMode("OK");
    }

    @Test
    void failedEventCanBeReplayed() {
        setMode("SERVER_ERROR");
        String eventId = submit("fc-replay", "agg-fc4");
        await("exhausted before replay", () -> {
            EventView v = get(eventId);
            return v != null && v.status().name().equals("FAILED");
        });
        setMode("OK");
        HttpHeaders h = new HttpHeaders();
        h.set("X-Tenant-Token", "token-a");
        ResponseEntity<Map> replay = rest.exchange("/tenants/tenant-a/events/" + eventId + "/replay",
                HttpMethod.POST, new HttpEntity<>(h), Map.class);
        assertEquals(200, replay.getStatusCode().value());
        await("replayed event delivered", () -> {
            EventView v = get(eventId);
            return v != null && v.status().name().equals("DELIVERED");
        });
        log.info("判定依据: 重放后事件状态={} (期望=DELIVERED)", get(eventId).status());
    }
}
