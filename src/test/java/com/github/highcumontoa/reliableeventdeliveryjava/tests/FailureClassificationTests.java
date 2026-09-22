package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** 故障分类：5xx/超时按退避重试直至耗尽进入 FAILED；4xx 永久失败不重试；flaky 恢复后成功 */
class FailureClassificationTests extends BaseIntegrationTest {

    @Test
    void serverErrorIsRetriedUntilExhausted() {
        String idem = "idem-" + UUID.randomUUID();
        String id = submit(TOKEN_A, idem, "agg-se", "p", loopbackUrl("server-error")).getBody().eventId();

        EventView failed = awaitStatus(TOKEN_A, id, "FAILED", Duration.ofSeconds(15));
        assertThat(failed.attemptCount()).isEqualTo(3);
        assertThat(failed.lastError()).contains("500");
        log.info("assertion basis: SERVER_ERROR exhausted attempts={} lastError={}",
                failed.attemptCount(), failed.lastError());
    }

    @Test
    void clientRejectionIsNotRetried() {
        String idem = "idem-" + UUID.randomUUID();
        String id = submit(TOKEN_A, idem, "agg-cr", "p", loopbackUrl("reject")).getBody().eventId();

        EventView failed = awaitStatus(TOKEN_A, id, "FAILED", Duration.ofSeconds(10));
        assertThat(failed.attemptCount())
                .as("permanent failure must not be retried")
                .isEqualTo(1);
        log.info("assertion basis: CLIENT_REJECTED attempts={} (no retry)", failed.attemptCount());
    }

    @Test
    void timeoutIsClassifiedAndRetried() {
        String idem = "idem-" + UUID.randomUUID();
        String id = submit(TOKEN_A, idem, "agg-to", "p", loopbackUrl("timeout")).getBody().eventId();

        EventView failed = awaitStatus(TOKEN_A, id, "FAILED", Duration.ofSeconds(20));
        assertThat(failed.attemptCount()).isEqualTo(3);
        assertThat(failed.lastError()).contains("timeout");
        log.info("assertion basis: TIMEOUT attempts={} lastError={}", failed.attemptCount(), failed.lastError());
    }

    @Test
    void flakyReceiverEventuallySucceeds() {
        receiver.setFlakyFailures(2);
        String idem = "idem-" + UUID.randomUUID();
        String id = submit(TOKEN_A, idem, "agg-fl", "p", loopbackUrl("flaky")).getBody().eventId();

        EventView delivered = awaitStatus(TOKEN_A, id, "DELIVERED", Duration.ofSeconds(10));
        assertThat(delivered.attemptCount()).isEqualTo(2);
        assertThat(receiver.recorded()).hasSize(1);
        log.info("assertion basis: flaky recovered after attempts={} deliveries={}",
                delivered.attemptCount(), receiver.recorded().size());
    }

    @Test
    void failedEventCanBeReplayed() {
        String idem = "idem-" + UUID.randomUUID();
        String id = submit(TOKEN_A, idem, "agg-rp", "p", loopbackUrl("server-error")).getBody().eventId();
        awaitStatus(TOKEN_A, id, "FAILED", Duration.ofSeconds(15));

        // 重放 FAILED 事件：状态机重新进入待投递
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("X-Api-Key", TOKEN_A);
        var resp = rest.exchange("/api/events/" + id + "/replay",
                org.springframework.http.HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(headers), EventView.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody().status()).isIn("PENDING", "RETRY_WAIT", "LEASED", "FAILED");
        log.info("assertion basis: replay accepted, status after replay={}", resp.getBody().status());

        EventView again = awaitStatus(TOKEN_A, id, "FAILED", Duration.ofSeconds(15));
        assertThat(again.attemptCount()).isEqualTo(3);
        log.info("assertion basis: replayed event retried again attempts={}", again.attemptCount());
    }
}
