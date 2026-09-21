package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.github.highcumontoa.delivery.api.EventController.EventView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 幂等性：重复提交不产生重复投递；同键不同内容被明确拒绝；终态事件不被再次投递。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18101",
        "delivery.loopback-base-url=http://127.0.0.1:18101",
        "delivery.store-path=target/test-data/idempotency/store.json",
        "delivery.backoff-base=50ms",
        "delivery.backoff-max=500ms",
        "delivery.poll-interval=50ms",
        "delivery.tenant-tokens.token-a=tenant-a",
        "delivery.tenant-tokens.token-b=tenant-b"
})
class IdempotencyTest extends IntegrationTestBase {

    static {
        new java.io.File("target/test-data/idempotency").mkdirs();
        new java.io.File("target/test-data/idempotency/store.json").delete();
    }

    private HttpEntity<String> submit(String token, String key, String agg, String payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Tenant-Token", token);
        String body = "{\"idempotencyKey\":\"" + key + "\",\"aggregateKey\":\"" + agg
                + "\",\"payload\":\"" + payload + "\"}";
        return new HttpEntity<>(body, headers);
    }

    @Test
    void duplicateSubmissionDoesNotRedeliver() {
        ResponseEntity<EventView> first = rest.postForEntity(
                "/tenants/tenant-a/events", submit("token-a", "idem-1", "agg-i1", "p1"), EventView.class);
        assertEquals(HttpStatus.CREATED, first.getStatusCode());
        String eventId = first.getBody().eventId();

        // 等待首次投递完成
        await("first delivery reaches DELIVERED", () -> {
            EventView v = rest.exchange("/tenants/tenant-a/events/" + eventId, HttpMethod.GET,
                    new HttpEntity<>(headers("token-a")), EventView.class).getBody();
            return v != null && v.status().name().equals("DELIVERED");
        });

        // 重复提交：同键同内容 -> 返回同一事件
        ResponseEntity<EventView> dup = rest.postForEntity(
                "/tenants/tenant-a/events", submit("token-a", "idem-1", "agg-i1", "p1"), EventView.class);
        assertEquals(eventId, dup.getBody().eventId(), "重复提交应返回同一事件ID");

        // 终态事件不得被再次投递：接收端记录数保持为 1
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        List<Map<String, Object>> received = rest.getForObject("/loopback/received", List.class);
        long count = received.stream().filter(r -> eventId.equals(r.get("eventId"))).count();
        log.info("判定依据: 重复提交后接收端该事件记录数={} (期望=1)", count);
        assertEquals(1, count, "重复提交/终态事件不得产生重复投递");
    }

    @Test
    void conflictingSubmissionRejected() {
        rest.postForEntity("/tenants/tenant-a/events", submit("token-a", "idem-2", "agg-i2", "p1"), EventView.class);
        ResponseEntity<Map> conflict = rest.postForEntity(
                "/tenants/tenant-a/events", submit("token-a", "idem-2", "agg-i2", "DIFFERENT"), Map.class);
        log.info("判定依据: 冲突提交响应码={} 原因={}", conflict.getStatusCode(), conflict.getBody());
        assertEquals(HttpStatus.CONFLICT, conflict.getStatusCode());
        assertEquals("IDEMPOTENCY_CONFLICT", conflict.getBody().get("error"));
    }

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-Tenant-Token", token);
        return h;
    }
}
