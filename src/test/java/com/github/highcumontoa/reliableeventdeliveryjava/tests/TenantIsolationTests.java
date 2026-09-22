package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/** 租户隔离与凭据保护：跨租户不可见、越权可区分拒绝、令牌不出现在错误响应中 */
class TenantIsolationTests extends BaseIntegrationTest {

    @Test
    void missingAndInvalidTokenAreDistinguished() {
        ResponseEntity<String> noToken = rest.exchange("/api/events", HttpMethod.GET,
                HttpEntity.EMPTY, String.class);
        assertThat(noToken.getStatusCode().value()).isEqualTo(401);
        assertThat(noToken.getBody()).contains("TENANT_MISSING");
        log.info("assertion basis: missing token -> 401 TENANT_MISSING");

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "wrong-token-xyz");
        ResponseEntity<String> badToken = rest.exchange("/api/events", HttpMethod.GET,
                new HttpEntity<>(headers), String.class);
        assertThat(badToken.getStatusCode().value()).isEqualTo(401);
        assertThat(badToken.getBody()).contains("TENANT_UNAUTHORIZED");
        assertThat(badToken.getBody()).doesNotContain("wrong-token-xyz");
        log.info("assertion basis: invalid token -> 401 TENANT_UNAUTHORIZED, token not echoed in response");
    }

    @Test
    void crossTenantDataIsInvisible() {
        String idem = "idem-" + UUID.randomUUID();
        String id = submit(TOKEN_A, idem, "agg-iso", "secret-payload", loopbackUrl("ok")).getBody().eventId();

        // tenant-b 列表不包含 tenant-a 的事件
        List<EventView> bEvents = listEvents(TOKEN_B);
        assertThat(bEvents).noneMatch(e -> e.id().equals(id));
        log.info("assertion basis: tenant-b sees {} events, none from tenant-a", bEvents.size());

        // tenant-b 按 id 直接访问 -> 404（不泄露存在性）
        ResponseEntity<String> resp = getEventRaw(TOKEN_B, id);
        assertThat(resp.getStatusCode().value()).isEqualTo(404);
        assertThat(resp.getBody()).contains("EVENT_NOT_FOUND");
        log.info("assertion basis: cross-tenant get -> 404 EVENT_NOT_FOUND");

        // tenant-b 重放 tenant-a 的事件 -> 404
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", TOKEN_B);
        ResponseEntity<String> replay = rest.exchange("/api/events/" + id + "/replay",
                HttpMethod.POST, new HttpEntity<>(headers), String.class);
        assertThat(replay.getStatusCode().value()).isEqualTo(404);
        log.info("assertion basis: cross-tenant replay -> 404");

        // tenant-a 自己可见
        assertThat(getEvent(TOKEN_A, id).getStatusCode().value()).isEqualTo(200);
    }

    @Test
    void idempotencyIsScopedPerTenant() {
        String idem = "idem-" + UUID.randomUUID();
        // 不同租户使用相同幂等键互不冲突
        var a = submit(TOKEN_A, idem, "agg-t", "payload-a", loopbackUrl("ok"));
        var b = submit(TOKEN_B, idem, "agg-t", "payload-b-different", loopbackUrl("ok"));
        assertThat(a.getStatusCode().value()).isEqualTo(202);
        assertThat(b.getStatusCode().value()).isEqualTo(202);
        assertThat(a.getBody().eventId()).isNotEqualTo(b.getBody().eventId());
        log.info("assertion basis: same idem key across tenants accepted independently, ids differ");
    }

    private ResponseEntity<String> getEventRaw(String token, String id) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        return rest.exchange("/api/events/" + id, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }
}
