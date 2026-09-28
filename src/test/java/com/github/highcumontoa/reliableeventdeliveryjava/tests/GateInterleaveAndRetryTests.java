package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateViewResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
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
 * 闸门与重试/重放交织：
 * 1) 队首在长退避 RETRY_WAIT 期间聚合自动 BLOCKED，后续事件被排队，恢复后续按顺序处理；
 * 2) 暂停期间队首彻底失败，人工重放与恢复闸门先后发生，最终仍严格按序号各生效一次。
 * 使用长基础退避，让 RETRY_WAIT 窗口稳定可观察。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.worker-count=2",
        "delivery.poll-interval=50ms",
        "delivery.lease-duration=2s",
        "delivery.delivery-timeout=1s",
        "delivery.retry-base-backoff=2s",
        "delivery.max-attempts=3"
})
class GateInterleaveAndRetryTests {

    private static final Logger log = LoggerFactory.getLogger(GateInterleaveAndRetryTests.class);
    private static final String TOKEN = "token-a-secret";
    private static final Path STORAGE_DIR = createTempDir();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("delivery.storage-dir", () -> STORAGE_DIR.toString());
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("event-store-gate-interleave");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    LoopbackReceiverController receiver;

    @BeforeEach
    void reset() {
        receiver.reset();
        receiver.setFlakyFailures(20);
    }

    private String url(String mode) {
        return "http://127.0.0.1:" + port + "/receiver/" + mode;
    }

    @Test
    void retryingHeadBlocksQueueAndRecoveryKeepsOrder() throws Exception {
        // 该聚合 flaky 持续失败（计数按聚合隔离）：队首首次失败后停留在 RETRY_WAIT，聚合自动阻塞；
        // 恢复（失败预算清零）后下一次重试成功，后续按序放行。
        String agg = "agg-ib-retry-" + UUID.randomUUID();
        receiver.setFlakyFailures(agg, 100);
        String id1 = submit("idem-" + agg + "-1", agg, "first", url("flaky"));
        EventView head = awaitEventStatus(id1, "RETRY_WAIT", Duration.ofSeconds(10));
        assertThat(head.attemptCount()).isEqualTo(1);

        GateViewResponse blocked = awaitGateState(agg, "BLOCKED", Duration.ofSeconds(5));
        assertThat(blocked.reason()).isEqualTo("RETRYING");
        assertThat(blocked.headEventId()).isEqualTo(id1);
        log.info("assertion basis: head RETRY_WAIT -> gate BLOCKED reason={} headSeq={} nextAt={}",
                blocked.reason(), blocked.headSequence(), blocked.headNextAttemptAt());

        // 后续事件在队首重试等待期间一律排队，不会提前生效
        String id2 = submit("idem-" + agg + "-2", agg, "second", url("flaky"));
        String id3 = submit("idem-" + agg + "-3", agg, "third", url("flaky"));
        GateViewResponse withQueue = getGate(agg).getBody();
        assertThat(withQueue.queuedCount()).isEqualTo(3);
        Thread.sleep(300);
        assertThat(receiver.recorded()).isEmpty();
        log.info("assertion basis: followers queued={} while head in RETRY_WAIT", withQueue.queuedCount());

        // 接收端恢复：下一次重试队首成功，随后 1,2,3 严格按序各生效一次
        receiver.setFlakyFailures(agg, 0);
        awaitEventStatus(id1, "DELIVERED", Duration.ofSeconds(15));
        awaitEventStatus(id2, "DELIVERED", Duration.ofSeconds(15));
        awaitEventStatus(id3, "DELIVERED", Duration.ofSeconds(15));

        List<Received> got = awaitAggregateReceived(agg, 3, Duration.ofSeconds(15));
        long[] seqs = got.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: after retry recovery sequences={}", java.util.Arrays.toString(seqs));
        assertThat(seqs).containsExactly(1L, 2L, 3L);
        assertThat(got.stream().map(Received::payload).toList()).containsExactly("first", "second", "third");
        assertThat(listBlocked()).noneMatch(g -> g.aggregateKey().equals(agg));
    }

    @Test
    void replayWhilePausedThenResumeDeliversStrictlyInOrder() throws Exception {
        // 该聚合 flaky 恰好失败 3 次：seq1 耗尽后 FAILED，聚合自动 BLOCKED；随后人工暂停。
        String agg = "agg-ib-mix-" + UUID.randomUUID();
        receiver.setFlakyFailures(agg, 3);

        String id1 = submit("idem-" + agg + "-1", agg, "first", url("flaky"));
        awaitEventStatus(id1, "FAILED", Duration.ofSeconds(15));

        // 在阻塞状态下暂停闸门（人工先管住）
        GateViewResponse paused = pauseGate(agg).getBody();
        assertThat(paused.state()).isEqualTo("PAUSED");

        // seq2、seq3 在暂停+阻塞下继续被收下排队（flaky 计数按聚合隔离，不会受其影响）
        String id2 = submit("idem-" + agg + "-2", agg, "second", url("flaky"));
        String id3 = submit("idem-" + agg + "-3", agg, "third", url("flaky"));
        assertThat(getGate(agg).getBody().queuedCount()).isEqualTo(3);

        // 接收端恢复；先重放失败队首（此时仍暂停，事件只回到 PENDING 排队，不投递）
        receiver.setFlakyFailures(agg, 0);
        replay(id1);
        EventView headAfterReplay = awaitEventStatus(id1, "PENDING", Duration.ofSeconds(5));
        assertThat(headAfterReplay.status()).isEqualTo("PENDING");
        Thread.sleep(300);
        assertThat(receiver.recorded()).isEmpty();
        log.info("assertion basis: replay while paused requeues head seq=1 but nothing delivered");

        // 随后恢复闸门：从暂停位置按原顺序 1,2,3 继续，各恰好一次
        resumeGate(agg);
        awaitEventStatus(id1, "DELIVERED", Duration.ofSeconds(15));
        awaitEventStatus(id2, "DELIVERED", Duration.ofSeconds(15));
        awaitEventStatus(id3, "DELIVERED", Duration.ofSeconds(15));

        List<Received> got = awaitAggregateReceived(agg, 3, Duration.ofSeconds(15));
        long[] seqs = got.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: replay+resume interleaved sequences={}", java.util.Arrays.toString(seqs));
        assertThat(seqs).containsExactly(1L, 2L, 3L);
        assertThat(got.stream().map(Received::payload).toList()).containsExactly("first", "second", "third");
        assertThat(listBlocked()).noneMatch(g -> g.aggregateKey().equals(agg));
    }

    // ---- 工具 ----

    private String submit(String idem, String agg, String payload, String target) {
        ResponseEntity<SubmitEventResponse> resp = rawPost("/api/events",
                "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"" + agg + "\","
                        + "\"payload\":\"" + payload + "\",\"targetUrl\":\"" + target + "\"}",
                SubmitEventResponse.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        return resp.getBody().eventId();
    }

    private EventView awaitEventStatus(String id, String expected, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        EventView last = null;
        while (System.nanoTime() < deadline) {
            last = getEvent(id).getBody();
            if (last != null && last.status().equals(expected)) {
                return last;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("event " + id + " not " + expected + ", last=" + (last == null ? "null" : last.status()));
    }

    private ResponseEntity<EventView> getEvent(String id) {
        return rawGet("/api/events/" + id, EventView.class);
    }

    private void replay(String id) {
        HttpHeaders headers = authedHeaders();
        ResponseEntity<String> resp = rest.exchange("/api/events/" + id + "/replay", HttpMethod.POST,
                new HttpEntity<>(headers), String.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
    }

    private ResponseEntity<GateViewResponse> pauseGate(String agg) {
        return gateAction(agg, "pause");
    }

    private ResponseEntity<GateViewResponse> resumeGate(String agg) {
        return gateAction(agg, "resume");
    }

    private ResponseEntity<GateViewResponse> gateAction(String agg, String action) {
        HttpHeaders headers = authedHeaders();
        headers.set("Content-Type", "application/json");
        return rest.exchange("/api/gates/" + enc(agg) + "/" + action, HttpMethod.POST,
                new HttpEntity<>("{}", headers), GateViewResponse.class);
    }

    private ResponseEntity<GateViewResponse> getGate(String agg) {
        return rawGet("/api/gates/" + enc(agg), GateViewResponse.class);
    }

    private List<GateViewResponse> listBlocked() {
        ResponseEntity<List<GateViewResponse>> typed = rest.exchange("/api/gates?state=BLOCKED", HttpMethod.GET,
                new HttpEntity<>(authedHeaders()),
                new org.springframework.core.ParameterizedTypeReference<>() {
                });
        return typed.getBody();
    }

    private GateViewResponse awaitGateState(String agg, String expected, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        GateViewResponse last = null;
        while (System.nanoTime() < deadline) {
            last = getGate(agg).getBody();
            if (last != null && last.state().equals(expected)) {
                return last;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("gate " + agg + " not " + expected + ", last=" + (last == null ? "null" : last.state()));
    }

    private List<Received> awaitAggregateReceived(String agg, int count, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Received> got = List.of();
        while (System.nanoTime() < deadline) {
            got = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).toList();
            if (got.size() >= count) {
                return got;
            }
            Thread.sleep(50);
        }
        return got;
    }

    private <T> ResponseEntity<T> rawGet(String path, Class<T> type) {
        return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(authedHeaders()), type);
    }

    private <T> ResponseEntity<T> rawPost(String path, String body, Class<T> type) {
        HttpHeaders headers = authedHeaders();
        headers.set("Content-Type", "application/json");
        return rest.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), type);
    }

    private HttpHeaders authedHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", TOKEN);
        return headers;
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
