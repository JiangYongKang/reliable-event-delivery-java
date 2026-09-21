package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.highcumontoa.delivery.api.EventController.EventView;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 积压与资源约束：待投递数量超过配置上限时按既定策略拒绝（429 + 可区分原因），
 * 而不是无界占用内存。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18105",
        "delivery.loopback-base-url=http://127.0.0.1:18105",
        "delivery.store-path=target/test-data/backpressure/store.json",
        "delivery.max-pending=2",
        "delivery.poll-interval=50ms",
        "delivery.delivery-timeout=5s",
        "delivery.tenant-tokens.token-a=tenant-a"
})
class BackpressureTest extends IntegrationTestBase {

    static {
        new java.io.File("target/test-data/backpressure").mkdirs();
        new java.io.File("target/test-data/backpressure/store.json").delete();
    }

    @Test
    void submissionsRejectedWhenBacklogFull() {
        // 让接收端挂起，积压不会被消耗
        HttpHeaders json = new HttpHeaders();
        json.setContentType(MediaType.APPLICATION_JSON);
        rest.postForEntity("/loopback/behavior", new HttpEntity<>("{\"mode\":\"TIMEOUT\"}", json), Map.class);

        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-Tenant-Token", "token-a");

        ResponseEntity<EventView> r1 = rest.postForEntity("/tenants/tenant-a/events",
                new HttpEntity<>("{\"idempotencyKey\":\"bp-1\",\"aggregateKey\":\"agg-bp1\",\"payload\":\"p\"}", h), EventView.class);
        ResponseEntity<EventView> r2 = rest.postForEntity("/tenants/tenant-a/events",
                new HttpEntity<>("{\"idempotencyKey\":\"bp-2\",\"aggregateKey\":\"agg-bp2\",\"payload\":\"p\"}", h), EventView.class);
        ResponseEntity<Map> r3 = rest.postForEntity("/tenants/tenant-a/events",
                new HttpEntity<>("{\"idempotencyKey\":\"bp-3\",\"aggregateKey\":\"agg-bp3\",\"payload\":\"p\"}", h), Map.class);

        log.info("判定依据: 第1笔={} 第2笔={} 第3笔={}/{} (期望前两笔接受, 第三笔 429/BACKPRESSURE_LIMIT)",
                r1.getStatusCode(), r2.getStatusCode(), r3.getStatusCode(), r3.getBody());
        assertEquals(HttpStatus.CREATED, r1.getStatusCode());
        assertEquals(HttpStatus.CREATED, r2.getStatusCode());
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, r3.getStatusCode());
        assertEquals("BACKPRESSURE_LIMIT", r3.getBody().get("error"));

        rest.postForEntity("/loopback/behavior", new HttpEntity<>("{\"mode\":\"OK\"}", json), Map.class);
    }
}
