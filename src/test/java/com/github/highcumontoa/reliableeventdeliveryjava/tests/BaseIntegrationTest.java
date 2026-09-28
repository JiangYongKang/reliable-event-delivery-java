package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateViewResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventRequest;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** 集成测试基类：随机端口 + 独立临时存储目录 + 快捷提交/等待工具。日志只打印判定依据，不打印凭据。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "delivery.worker-count=2",
        "delivery.poll-interval=50ms",
        "delivery.lease-duration=2s",
        "delivery.delivery-timeout=1s",
        "delivery.retry-base-backoff=100ms",
        "delivery.max-attempts=3"
})
public abstract class BaseIntegrationTest {

    protected static final String TOKEN_A = "token-a-secret";
    protected static final String TOKEN_B = "token-b-secret";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static final Path STORAGE_DIR = createTempDir();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("delivery.storage-dir", () -> STORAGE_DIR.toString());
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("event-store-it");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected LoopbackReceiverController receiver;

    @BeforeEach
    void resetReceiver() {
        receiver.reset();
    }

    protected String loopbackUrl(String mode) {
        return "http://127.0.0.1:" + port + "/receiver/" + mode;
    }

    protected ResponseEntity<SubmitEventResponse> submit(String token, String idemKey, String aggKey,
                                                         String payload, String targetUrl) {
        HttpHeaders headers = new HttpHeaders();
        if (token != null) {
            headers.set("X-Api-Key", token);
        }
        return rest.exchange("/api/events", HttpMethod.POST,
                new HttpEntity<>(new SubmitEventRequest(idemKey, aggKey, payload, targetUrl), headers),
                SubmitEventResponse.class);
    }

    protected ResponseEntity<EventView> getEvent(String token, String id) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        return rest.exchange("/api/events/" + id, HttpMethod.GET, new HttpEntity<>(headers), EventView.class);
    }

    protected List<EventView> listEvents(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        ResponseEntity<List<EventView>> resp = rest.exchange("/api/events", HttpMethod.GET,
                new HttpEntity<>(headers), new ParameterizedTypeReference<>() {
                });
        return resp.getBody();
    }

    /** 轮询等待事件到达目标状态，并打印判定依据 */
    protected EventView awaitStatus(String token, String eventId, String expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        EventView last = null;
        while (System.nanoTime() < deadline) {
            last = getEvent(token, eventId).getBody();
            if (last != null && last.status().equals(expected)) {
                log.info("assertion basis: event={} reached status={} attempts={}",
                        eventId, last.status(), last.attemptCount());
                return last;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("event " + eventId + " did not reach " + expected
                + ", last=" + (last == null ? "null" : last.status()));
    }

    protected ResponseEntity<GateViewResponse> pauseGate(String token, String aggKey) {
        return pauseGate(token, aggKey, null);
    }

    protected ResponseEntity<GateViewResponse> pauseGate(String token, String aggKey, String declaredTenant) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        headers.set("Content-Type", "application/json");
        String body = declaredTenant == null ? "{}" : "{\"tenantId\":\"" + declaredTenant + "\"}";
        return rest.exchange("/api/gates/" + encode(aggKey) + "/pause", HttpMethod.POST,
                new HttpEntity<>(body, headers), GateViewResponse.class);
    }

    protected ResponseEntity<GateViewResponse> resumeGate(String token, String aggKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        headers.set("Content-Type", "application/json");
        return rest.exchange("/api/gates/" + encode(aggKey) + "/resume", HttpMethod.POST,
                new HttpEntity<>("{}", headers), GateViewResponse.class);
    }

    protected ResponseEntity<GateViewResponse> getGate(String token, String aggKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        return rest.exchange("/api/gates/" + encode(aggKey), HttpMethod.GET,
                new HttpEntity<>(headers), GateViewResponse.class);
    }

    protected ResponseEntity<String> getGateRaw(String token, String aggKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        return rest.exchange("/api/gates/" + encode(aggKey), HttpMethod.GET,
                new HttpEntity<>(headers), String.class);
    }

    protected List<GateViewResponse> listGates(String token, String state) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", token);
        String url = "/api/gates" + (state == null ? "" : "?state=" + state);
        ResponseEntity<List<GateViewResponse>> resp = rest.exchange(url, HttpMethod.GET,
                new HttpEntity<>(headers), new ParameterizedTypeReference<>() {
                });
        return resp.getBody();
    }

    /** 等待该聚合被接收端记录的投递达到 expectedCount，并返回全部投递 */
    protected List<Received> awaitAggregateReceived(String aggKey, int expectedCount, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Received> got = List.of();
        while (System.nanoTime() < deadline) {
            got = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(aggKey)).toList();
            if (got.size() >= expectedCount) {
                return got;
            }
            Thread.sleep(50);
        }
        return got;
    }

    private static String encode(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
    }
}
