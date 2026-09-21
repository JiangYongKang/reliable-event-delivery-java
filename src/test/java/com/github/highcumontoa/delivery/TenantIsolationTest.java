package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.highcumontoa.delivery.api.EventController.EventView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * 租户隔离与凭据保护：跨租户访问被拒绝且原因可区分；不同租户的查询结果
 * 互相不可见；令牌不出现在错误响应与日志中。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18104",
        "delivery.loopback-base-url=http://127.0.0.1:18104",
        "delivery.store-path=target/test-data/tenant/store.json",
        "delivery.poll-interval=50ms",
        "delivery.tenant-tokens.secret-token-a=tenant-a",
        "delivery.tenant-tokens.secret-token-b=tenant-b"
})
class TenantIsolationTest extends IntegrationTestBase {

    private static final String TOKEN_A = "secret-token-a";
    private static final String TOKEN_B = "secret-token-b";

    static {
        new java.io.File("target/test-data/tenant").mkdirs();
        new java.io.File("target/test-data/tenant/store.json").delete();
    }

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        if (token != null) {
            h.set("X-Tenant-Token", token);
        }
        return h;
    }

    private String submit(String tenant, String token, String key) {
        String body = "{\"idempotencyKey\":\"" + key + "\",\"aggregateKey\":\"agg-t\",\"payload\":\"p\"}";
        ResponseEntity<EventView> r = rest.postForEntity("/tenants/" + tenant + "/events",
                new HttpEntity<>(body, headers(token)), EventView.class);
        return r.getBody().eventId();
    }

    @Test
    void crossTenantAccessDeniedWithDistinctReasons() {
        String eventId = submit("tenant-a", TOKEN_A, "iso-1");

        // 跨租户读取 -> TENANT_MISMATCH
        ResponseEntity<Map> mismatch = rest.exchange("/tenants/tenant-b/events/" + eventId,
                HttpMethod.GET, new HttpEntity<>(headers(TOKEN_A)), Map.class);
        // 无效令牌 -> INVALID_TOKEN
        ResponseEntity<Map> invalid = rest.exchange("/tenants/tenant-a/events/" + eventId,
                HttpMethod.GET, new HttpEntity<>(headers("not-a-token")), Map.class);
        // 缺失令牌 -> MISSING_TOKEN
        ResponseEntity<Map> missing = rest.exchange("/tenants/tenant-a/events/" + eventId,
                HttpMethod.GET, new HttpEntity<>(headers(null)), Map.class);

        log.info("判定依据: mismatch={}/{} invalid={}/{} missing={}/{}",
                mismatch.getStatusCode(), mismatch.getBody(), invalid.getStatusCode(), invalid.getBody(),
                missing.getStatusCode(), missing.getBody());
        assertEquals(HttpStatus.FORBIDDEN, mismatch.getStatusCode());
        assertEquals("TENANT_MISMATCH", mismatch.getBody().get("error"));
        assertEquals(HttpStatus.UNAUTHORIZED, invalid.getStatusCode());
        assertEquals("INVALID_TOKEN", invalid.getBody().get("error"));
        assertEquals(HttpStatus.UNAUTHORIZED, missing.getStatusCode());
        assertEquals("MISSING_TOKEN", missing.getBody().get("error"));

        // 错误响应不得包含令牌内容
        assertFalse(mismatch.getBody().toString().contains(TOKEN_A));
        assertFalse(invalid.getBody().toString().contains("not-a-token"));
    }

    @Test
    void tenantDataIsInvisibleAcrossTenants() {
        submit("tenant-a", TOKEN_A, "iso-2");
        submit("tenant-b", TOKEN_B, "iso-3");

        List<Map<String, Object>> failedA = rest.exchange("/tenants/tenant-a/events?status=FAILED",
                HttpMethod.GET, new HttpEntity<>(headers(TOKEN_A)), List.class).getBody();
        List<Map<String, Object>> failedB = rest.exchange("/tenants/tenant-b/events?status=FAILED",
                HttpMethod.GET, new HttpEntity<>(headers(TOKEN_B)), List.class).getBody();
        log.info("判定依据: tenant-a 查询结果数={} tenant-b 查询结果数={} (互不可见)",
                failedA.size(), failedB.size());
        assertTrue(failedA.stream().allMatch(e -> "tenant-a".equals(e.get("tenantId"))));
        assertTrue(failedB.stream().allMatch(e -> "tenant-b".equals(e.get("tenantId"))));
    }

    @Test
    void tokenNeverAppearsInLogs() {
        Logger authLogger = (Logger) LoggerFactory.getLogger("com.github.highcumontoa.delivery");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        authLogger.addAppender(appender);
        try {
            // 触发各类拒绝路径
            rest.exchange("/tenants/tenant-a/events/x", HttpMethod.GET,
                    new HttpEntity<>(headers("not-a-token")), Map.class);
            rest.exchange("/tenants/tenant-b/events/x", HttpMethod.GET,
                    new HttpEntity<>(headers(TOKEN_A)), Map.class);
            submit("tenant-a", TOKEN_A, "iso-4");

            long leaks = appender.list.stream()
                    .map(ILoggingEvent::getFormattedMessage)
                    .filter(m -> m.contains(TOKEN_A) || m.contains(TOKEN_B) || m.contains("not-a-token"))
                    .peek(m -> log.info("发现泄漏日志: {}", m))
                    .count();
            log.info("判定依据: 日志条数={} 含令牌条数={} (期望=0)", appender.list.size(), leaks);
            assertEquals(0, leaks, "令牌不得出现在日志中");
        } finally {
            authLogger.detachAppender(appender);
        }
    }
}
