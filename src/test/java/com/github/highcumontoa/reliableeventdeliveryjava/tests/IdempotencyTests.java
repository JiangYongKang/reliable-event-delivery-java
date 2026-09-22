package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/** 幂等提交：同键同内容去重、同键冲突拒绝、终态不再投递 */
class IdempotencyTests extends BaseIntegrationTest {

    @Test
    void duplicateSubmitWithSameContentIsDeduplicated() {
        String idem = "idem-" + UUID.randomUUID();
        String agg = "agg-1-" + UUID.randomUUID();
        ResponseEntity<SubmitEventResponse> first = submit(TOKEN_A, idem, agg, "p1", loopbackUrl("ok"));
        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(first.getBody().duplicate()).isFalse();

        ResponseEntity<SubmitEventResponse> second = submit(TOKEN_A, idem, agg, "p1", loopbackUrl("ok"));
        assertThat(second.getStatusCode().value()).isEqualTo(202);
        assertThat(second.getBody().duplicate()).isTrue();
        assertThat(second.getBody().eventId()).isEqualTo(first.getBody().eventId());
        log.info("assertion basis: duplicate submit returned same eventId={}", first.getBody().eventId());

        awaitStatus(TOKEN_A, first.getBody().eventId(), "DELIVERED", Duration.ofSeconds(10));
        long deliveries = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count();
        assertThat(deliveries)
                .as("same idempotency key must not produce duplicate delivery")
                .isEqualTo(1);
        log.info("assertion basis: receiver recorded {} delivery for idem key", deliveries);
    }

    @Test
    void conflictingSubmitWithSameKeyIsRejected() {
        String idem = "idem-" + UUID.randomUUID();
        submit(TOKEN_A, idem, "agg-2", "p1", loopbackUrl("ok"));

        ResponseEntity<String> conflict = restPostRaw(TOKEN_A, idem, "agg-2", "DIFFERENT", loopbackUrl("ok"));
        assertThat(conflict.getStatusCode().value()).isEqualTo(409);
        assertThat(conflict.getBody()).contains("IDEMPOTENCY_CONFLICT");
        log.info("assertion basis: conflict rejected with code=IDEMPOTENCY_CONFLICT status=409");
    }

    @Test
    void deliveredEventIsNotRedeliveredOnDuplicateSubmit() throws Exception {
        String idem = "idem-" + UUID.randomUUID();
        String agg = "agg-3-" + UUID.randomUUID();
        ResponseEntity<SubmitEventResponse> first = submit(TOKEN_A, idem, agg, "p1", loopbackUrl("ok"));
        EventView delivered = awaitStatus(TOKEN_A, first.getBody().eventId(), "DELIVERED", Duration.ofSeconds(10));
        assertThat(delivered.status()).isEqualTo("DELIVERED");

        long before = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count();
        submit(TOKEN_A, idem, agg, "p1", loopbackUrl("ok"));
        Thread.sleep(500);
        long after = receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count();
        assertThat(after)
                .as("terminal event must not be redelivered")
                .isEqualTo(before);
        log.info("assertion basis: delivered event redelivery count delta={}", after - before);
    }

    private ResponseEntity<String> restPostRaw(String token, String idem, String agg, String payload, String url) {
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("X-Api-Key", token);
        headers.set("Content-Type", "application/json");
        String body = "{\"idempotencyKey\":\"" + idem + "\",\"aggregateKey\":\"" + agg
                + "\",\"payload\":\"" + payload + "\",\"targetUrl\":\"" + url + "\"}";
        return rest.postForEntity("/api/events", new org.springframework.http.HttpEntity<>(body, headers), String.class);
    }
}
