package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateView;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/** 聚合闸门：暂停期间一件都不投递（其它聚合/租户不受影响），恢复后按原提交顺序继续 */
class GatePauseResumeTests extends BaseIntegrationTest {

    @Test
    void pausedAggregateHoldsEventsAndResumeRestoresOrder() throws Exception {
        String pausedAgg = "agg-pause-" + UUID.randomUUID();
        String freeAgg = "agg-free-" + UUID.randomUUID();

        // 先暂停，再提交：暂停期间新事件照常收下
        ResponseEntity<GateView> paused = pauseGate(TOKEN_A, "tenant-a", pausedAgg);
        assertThat(paused.getStatusCode().value()).isEqualTo(200);
        assertThat(paused.getBody().state()).isEqualTo("PAUSED");

        submit(TOKEN_A, "idem-" + pausedAgg + "-1", pausedAgg, "p1", loopbackUrl("ok"));
        submit(TOKEN_A, "idem-" + pausedAgg + "-2", pausedAgg, "p2", loopbackUrl("ok"));
        submit(TOKEN_A, "idem-" + pausedAgg + "-3", pausedAgg, "p3", loopbackUrl("ok"));
        // 对照组：同租户另一个聚合、另一租户同名聚合，都应照常投递
        submit(TOKEN_A, "idem-" + freeAgg + "-1", freeAgg, "free", loopbackUrl("ok"));
        submit(TOKEN_B, "idem-b-" + pausedAgg + "-1", pausedAgg, "other-tenant", loopbackUrl("ok"));

        Thread.sleep(800);
        List<Received> pausedGot = receiver.recorded().stream()
                .filter(r -> r.aggregateKey().equals(pausedAgg)).toList();
        // tenant-b 的同名聚合不受影响，会出现在 received 里，需要按 payload 区分
        List<Received> pausedTenantA = pausedGot.stream()
                .filter(r -> r.payload().startsWith("p")).toList();
        log.info("assertion basis: while paused, tenant-a deliveries={} tenant-b/free unaffected",
                pausedTenantA.size());
        assertThat(pausedTenantA).as("paused aggregate must not deliver anything").isEmpty();
        assertThat(pausedGot.stream().filter(r -> r.payload().equals("other-tenant")).count())
                .as("other tenant same aggKey unaffected").isEqualTo(1);
        assertThat(receiver.recorded().stream().filter(r -> r.aggregateKey().equals(freeAgg)).count())
                .as("other aggregate unaffected").isEqualTo(1);

        // 闸门视图可查到排队数
        GateView view = getGate(TOKEN_A, "tenant-a", pausedAgg).getBody();
        log.info("assertion basis: paused gate state={} queued={}", view.state(), view.queuedCount());
        assertThat(view.state()).isEqualTo("PAUSED");
        assertThat(view.queuedCount()).isEqualTo(3);

        // 恢复后从暂停位置按原提交顺序继续
        resumeGate(TOKEN_A, "tenant-a", pausedAgg);
        List<Received> got = awaitReceived(pausedAgg, 4, Duration.ofSeconds(15));
        List<Received> tenantA = got.stream().filter(r -> r.payload().startsWith("p")).toList();
        long[] seqs = tenantA.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: after resume sequences={}", java.util.Arrays.toString(seqs));
        assertThat(tenantA).hasSize(3);
        assertThat(seqs).containsExactly(1, 2, 3);
        assertThat(tenantA.stream().map(Received::payload).toList())
                .containsExactly("p1", "p2", "p3");
    }

    @Test
    void crossTenantGateOperationsAreRejected() {
        String agg = "agg-xtenant-" + UUID.randomUUID();
        ResponseEntity<GateView> pause = pauseGate(TOKEN_A, "tenant-b", agg);
        ResponseEntity<GateView> resume = resumeGate(TOKEN_A, "tenant-b", agg);
        ResponseEntity<GateView> get = getGate(TOKEN_A, "tenant-b", agg);
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("X-Api-Key", TOKEN_A);
        ResponseEntity<String> list = rest.exchange("/api/gates/tenant-b",
                org.springframework.http.HttpMethod.GET,
                new org.springframework.http.HttpEntity<>(headers), String.class);
        log.info("assertion basis: cross-tenant pause={} resume={} get={} list={} listBody={}",
                pause.getStatusCode(), resume.getStatusCode(), get.getStatusCode(),
                list.getStatusCode(), list.getBody());
        for (ResponseEntity<?> resp : List.of(pause, resume, get, list)) {
            assertThat(resp.getStatusCode().value()).isEqualTo(403);
        }
        assertThat(list.getBody()).contains("TENANT_FORBIDDEN");
        // 本租户操作不受影响
        assertThat(pauseGate(TOKEN_A, "tenant-a", agg).getStatusCode().value()).isEqualTo(200);
        assertThat(getGate(TOKEN_A, "tenant-a", agg).getBody().state()).isEqualTo("PAUSED");
    }
}
