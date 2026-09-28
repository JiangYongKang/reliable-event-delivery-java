package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateViewResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/**
 * 聚合闸门：暂停期间只收不投、恢复后从暂停位置按原顺序继续；
 * 队首重试/彻底失败自动阻塞后续；人工重放后放行；跨租户操作被可区分拒绝。
 */
class AggregateGateTests extends BaseIntegrationTest {

    @Test
    void pausedAggregateAcceptsButDoesNotDeliverAndResumesInOrder() throws Exception {
        String agg = "agg-pause-" + UUID.randomUUID();
        String other = "agg-pause-other-" + UUID.randomUUID();

        // 先让 seq1 投递完成，确认聚合正常；随后暂停
        String id1 = submit(TOKEN_A, "idem-" + agg + "-1", agg, "first", loopbackUrl("ok")).getBody().eventId();
        awaitStatus(TOKEN_A, id1, "DELIVERED", Duration.ofSeconds(10));

        GateViewResponse paused = pauseGate(TOKEN_A, agg).getBody();
        assertThat(paused.state()).isEqualTo("PAUSED");
        assertThat(paused.paused()).isTrue();
        log.info("assertion basis: gate paused state={} pausedAt={}", paused.state(), paused.pausedAt());

        // 暂停期间 seq2、seq3 照常 202 收下，但一件都不投递
        String id2 = submit(TOKEN_A, "idem-" + agg + "-2", agg, "second", loopbackUrl("ok")).getBody().eventId();
        String id3 = submit(TOKEN_A, "idem-" + agg + "-3", agg, "third", loopbackUrl("ok")).getBody().eventId();
        GateViewResponse whilePaused = getGate(TOKEN_A, agg).getBody();
        assertThat(whilePaused.state()).isEqualTo("PAUSED");
        assertThat(whilePaused.queuedCount()).isEqualTo(2);
        assertThat(whilePaused.headEventId()).isEqualTo(id2);
        assertThat(whilePaused.headSequence()).isEqualTo(2L);
        log.info("assertion basis: while paused queued={} headSeq={} state={}",
                whilePaused.queuedCount(), whilePaused.headSequence(), whilePaused.state());

        // 别的聚合完全不受影响
        String otherId = submit(TOKEN_A, "idem-" + other + "-1", other, "other", loopbackUrl("ok")).getBody().eventId();
        awaitStatus(TOKEN_A, otherId, "DELIVERED", Duration.ofSeconds(10));
        List<Received> otherGot = awaitAggregateReceived(other, 1, Duration.ofSeconds(5));
        assertThat(otherGot).hasSize(1);

        Thread.sleep(400);
        List<Received> stillOnlySeq1 =
                receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).toList();
        assertThat(stillOnlySeq1).hasSize(1);
        assertThat(stillOnlySeq1.get(0).sequence()).isEqualTo(1L);
        log.info("assertion basis: paused aggregate delivered only seq={}", stillOnlySeq1.get(0).sequence());

        // 重复暂停 -> 409 GATE_ALREADY_PAUSED
        ResponseEntity<String> again = pauseRaw(TOKEN_A, agg, null);
        assertThat(again.getStatusCode().value()).isEqualTo(409);
        assertThat(again.getBody()).contains("GATE_ALREADY_PAUSED");

        // 恢复：必须 seq2 先于 seq3，无跳过、无重复、无乱序
        GateViewResponse resumed = resumeGate(TOKEN_A, agg).getBody();
        assertThat(resumed.state()).isEqualTo("ACTIVE");
        awaitStatus(TOKEN_A, id2, "DELIVERED", Duration.ofSeconds(10));
        awaitStatus(TOKEN_A, id3, "DELIVERED", Duration.ofSeconds(10));

        List<Received> got = awaitAggregateReceived(agg, 3, Duration.ofSeconds(10));
        assertThat(got).hasSize(3);
        long[] seqs = got.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: after resume received sequences={}", java.util.Arrays.toString(seqs));
        assertThat(seqs).containsExactly(1L, 2L, 3L);
        assertThat(got.stream().map(Received::payload).toList()).containsExactly("first", "second", "third");
    }

    @Test
    void failedHeadAutomaticallyBlocksFollowersAndReplayReleasesQueueInOrder() throws Exception {
        // 该聚合 flaky 恰好失败 3 次（=maxAttempts）：队首 seq1 耗尽后 FAILED；
        // flaky 计数按聚合隔离，seq2/seq3 不会消耗本聚合的失败预算
        String agg = "agg-block-" + UUID.randomUUID();
        receiver.setFlakyFailures(agg, 3);
        String id1 = submit(TOKEN_A, "idem-" + agg + "-1", agg, "head", loopbackUrl("flaky")).getBody().eventId();
        awaitStatus(TOKEN_A, id1, "FAILED", Duration.ofSeconds(15));

        // 队首彻底失败后再提交 seq2、seq3（ok 目标；被队首挡在后面，绝无机会投递）
        String id2 = submit(TOKEN_A, "idem-" + agg + "-2", agg, "second", loopbackUrl("ok")).getBody().eventId();
        String id3 = submit(TOKEN_A, "idem-" + agg + "-3", agg, "third", loopbackUrl("ok")).getBody().eventId();

        GateViewResponse blocked = awaitGateState(TOKEN_A, agg, "BLOCKED", Duration.ofSeconds(5));
        assertThat(blocked.reason()).isEqualTo("PERMANENT_FAILURE");
        assertThat(blocked.headEventId()).isEqualTo(id1);
        assertThat(blocked.headSequence()).isEqualTo(1L);
        assertThat(blocked.headLastError()).isNotBlank();
        GateViewResponse settled = awaitQueuedCount(TOKEN_A, agg, 3, Duration.ofSeconds(5));
        assertThat(settled.queuedCount()).isEqualTo(3);
        log.info("assertion basis: blocked reason={} headSeq={} queued={} headAttempts={}",
                settled.reason(), settled.headSequence(), settled.queuedCount(), settled.headAttemptCount());

        // 列表接口按状态过滤可查到该阻塞聚合
        List<GateViewResponse> blockedList = listGates(TOKEN_A, "BLOCKED");
        assertThat(blockedList).anyMatch(g -> g.aggregateKey().equals(agg) && g.queuedCount() == 3);

        Thread.sleep(400);
        List<Received> none = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).toList();
        assertThat(none).isEmpty();
        assertThat(getEvent(TOKEN_A, id2).getBody().status()).isEqualTo("PENDING");
        assertThat(getEvent(TOKEN_A, id3).getBody().status()).isEqualTo("PENDING");
        log.info("assertion basis: followers stay PENDING and receive nothing while head FAILED");

        // 接收端恢复（本聚合失败预算清零）后人工重放卡住的队首：聚合解除阻塞，严格按 1,2,3 各生效一次
        receiver.setFlakyFailures(agg, 0);
        replay(TOKEN_A, id1);
        awaitStatus(TOKEN_A, id1, "DELIVERED", Duration.ofSeconds(15));
        awaitStatus(TOKEN_A, id2, "DELIVERED", Duration.ofSeconds(10));
        awaitStatus(TOKEN_A, id3, "DELIVERED", Duration.ofSeconds(10));

        List<Received> got = awaitAggregateReceived(agg, 3, Duration.ofSeconds(10));
        assertThat(got).hasSize(3);
        long[] seqs = got.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: after replay release sequences={}", java.util.Arrays.toString(seqs));
        assertThat(seqs).containsExactly(1L, 2L, 3L);
        // 队列清空后没有 BLOCKED/PAUSED 的聚合残留
        assertThat(listGates(TOKEN_A, "BLOCKED")).noneMatch(g -> g.aggregateKey().equals(agg));
    }

    @Test
    void crossTenantGateOperationsAreRejectedAndDataIsolationHolds() {
        String agg = "agg-x-" + UUID.randomUUID();
        submit(TOKEN_A, "idem-" + agg + "-1", agg, "secret", loopbackUrl("ok"));
        pauseGate(TOKEN_A, agg);

        // tenant-b 查 tenant-a 的聚合 -> 404（不泄露存在性）
        ResponseEntity<String> get = getGateRaw(TOKEN_B, agg);
        assertThat(get.getStatusCode().value()).isEqualTo(404);
        assertThat(get.getBody()).contains("GATE_NOT_FOUND");
        log.info("assertion basis: cross-tenant get gate -> 404 GATE_NOT_FOUND");

        // 请求体显式声明他人租户 -> 403 TENANT_FORBIDDEN（可区分的跨租户拒绝原因）
        ResponseEntity<String> pause = pauseRaw(TOKEN_B, agg, "tenant-a");
        assertThat(pause.getStatusCode().value()).isEqualTo(403);
        assertThat(pause.getBody()).contains("TENANT_FORBIDDEN");
        assertThat(pause.getBody()).doesNotContain(TOKEN_B);
        log.info("assertion basis: declared foreign tenant pause -> 403 TENANT_FORBIDDEN");

        ResponseEntity<String> resume = resumeRaw(TOKEN_B, agg, "tenant-a");
        assertThat(resume.getStatusCode().value()).isEqualTo(403);
        assertThat(resume.getBody()).contains("TENANT_FORBIDDEN");

        // tenant-b 列表里看不到 tenant-a 的聚合
        assertThat(listGates(TOKEN_B, null)).noneMatch(g -> g.aggregateKey().equals(agg));

        // 未暂停的聚合执行恢复 -> 409 GATE_NOT_PAUSED
        String agg2 = "agg-x2-" + UUID.randomUUID();
        submit(TOKEN_A, "idem-" + agg2 + "-1", agg2, "p", loopbackUrl("ok"));
        ResponseEntity<String> resumeNotPaused = resumeRaw(TOKEN_A, agg2, null);
        assertThat(resumeNotPaused.getStatusCode().value()).isEqualTo(409);
        assertThat(resumeNotPaused.getBody()).contains("GATE_NOT_PAUSED");
        log.info("assertion basis: resume of non-paused gate -> 409 GATE_NOT_PAUSED");
    }

    // ---- 小辅助 ----

    protected GateViewResponse awaitGateState(String token, String agg, String expected, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        GateViewResponse last = null;
        while (System.nanoTime() < deadline) {
            last = getGate(token, agg).getBody();
            if (last != null && last.state().equals(expected)) {
                return last;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("gate " + agg + " did not reach " + expected
                + ", last=" + (last == null ? "null" : last.state()));
    }

    protected GateViewResponse awaitQueuedCount(String token, String agg, int expected, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        GateViewResponse last = null;
        while (System.nanoTime() < deadline) {
            last = getGate(token, agg).getBody();
            if (last != null && last.queuedCount() >= expected) {
                return last;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("gate " + agg + " queuedCount not >= " + expected
                + ", last=" + (last == null ? "null" : last.queuedCount()));
    }

    protected void replay(String token, String eventId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        ResponseEntity<String> resp = rest.exchange("/api/events/" + eventId + "/replay", HttpMethod.POST,
                new HttpEntity<>(headers), String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
    }

    protected ResponseEntity<String> pauseRaw(String token, String agg, String declaredTenant) {
        return gateStateRaw(token, agg, "pause", declaredTenant);
    }

    protected ResponseEntity<String> resumeRaw(String token, String agg, String declaredTenant) {
        return gateStateRaw(token, agg, "resume", declaredTenant);
    }

    private ResponseEntity<String> gateStateRaw(String token, String agg, String action, String declaredTenant) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        headers.set("Content-Type", "application/json");
        String body = declaredTenant == null ? "{}" : "{\"tenantId\":\"" + declaredTenant + "\"}";
        return rest.exchange("/api/gates/" + encode(agg) + "/" + action, HttpMethod.POST,
                new HttpEntity<>(body, headers), String.class);
    }

    private static String encode(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
