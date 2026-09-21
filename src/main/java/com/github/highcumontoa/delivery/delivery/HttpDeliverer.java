package com.github.highcumontoa.delivery.delivery;

import com.github.highcumontoa.delivery.config.DeliveryProperties;
import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.DeliveryResult;
import com.github.highcumontoa.delivery.domain.FailureKind;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 单次 HTTP 投递执行器，并对失败分类：
 * 超时 -> TIMEOUT，连接失败 -> CONNECTION，5xx -> SERVER_ERROR（均为暂时性，可重试）；
 * 4xx -> REJECTED（永久性，不重试）。
 */
@Component
public class HttpDeliverer {

    private static final Logger log = LoggerFactory.getLogger(HttpDeliverer.class);

    private final HttpClient client;
    private final Duration deliveryTimeout;

    public HttpDeliverer(DeliveryProperties properties) {
        this.deliveryTimeout = properties.getDeliveryTimeout();
        this.client = HttpClient.newBuilder()
                .connectTimeout(deliveryTimeout)
                .build();
    }

    public DeliveryResult deliver(DeliveryEvent event, String targetUrl) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .timeout(deliveryTimeout)
                .header("Content-Type", "application/json")
                .header("X-Event-Id", event.getEventId())
                .header("X-Aggregate-Key", event.getAggregateKey())
                .header("X-Event-Sequence", String.valueOf(event.getSequence()))
                .POST(HttpRequest.BodyPublishers.ofString(event.getPayload()))
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return DeliveryResult.ok();
            }
            if (status >= 500) {
                return DeliveryResult.failure(FailureKind.SERVER_ERROR, "receiver returned " + status);
            }
            if (status >= 400) {
                return DeliveryResult.failure(FailureKind.REJECTED, "receiver returned " + status);
            }
            return DeliveryResult.failure(FailureKind.UNKNOWN, "unexpected status " + status);
        } catch (HttpTimeoutException e) {
            return DeliveryResult.failure(FailureKind.TIMEOUT, "delivery timed out");
        } catch (ConnectException e) {
            return DeliveryResult.failure(FailureKind.CONNECTION, "connection failed");
        } catch (IOException e) {
            // 超时在部分 JDK 路径上表现为普通 IOException 包装
            if (e.getCause() instanceof HttpTimeoutException) {
                return DeliveryResult.failure(FailureKind.TIMEOUT, "delivery timed out");
            }
            log.info("delivery io failure: eventId={} kind={}", event.getEventId(), FailureKind.CONNECTION);
            return DeliveryResult.failure(FailureKind.CONNECTION, "io failure: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DeliveryResult.failure(FailureKind.UNKNOWN, "interrupted");
        }
    }
}
