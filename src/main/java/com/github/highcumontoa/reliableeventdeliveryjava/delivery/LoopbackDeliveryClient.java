package com.github.highcumontoa.reliableeventdeliveryjava.delivery;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.FailureKind;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 本地回环 HTTP 投递客户端。
 * 失败分类：超时 -> TIMEOUT，连接失败 -> CONNECTION，5xx -> SERVER_ERROR（均暂时性），
 * 4xx -> CLIENT_REJECTED（永久性，不重试）。
 * 请求头只携带事件元数据，不携带任何租户凭据。
 */
@Component
public class LoopbackDeliveryClient implements DeliveryClient {

    private static final Logger log = LoggerFactory.getLogger(LoopbackDeliveryClient.class);

    private final HttpClient httpClient;
    private final DeliveryProperties properties;

    public LoopbackDeliveryClient(DeliveryProperties properties) {
        this.properties = properties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(properties.getDeliveryTimeout())
                .build();
    }

    @Override
    public DeliveryResult deliver(DeliveryEvent event) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(event.getTargetUrl()))
                .timeout(properties.getDeliveryTimeout())
                .header("Content-Type", "application/json")
                .header("X-Event-Id", event.getId())
                .header("X-Aggregate-Key", event.getAggregateKey())
                .header("X-Sequence", String.valueOf(event.getSequence()))
                .POST(HttpRequest.BodyPublishers.ofString(event.getPayload()))
                .build();
        try {
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int code = resp.statusCode();
            if (code >= 200 && code < 300) {
                return DeliveryResult.ok();
            }
            if (code >= 400 && code < 500) {
                return DeliveryResult.permanentFailure("receiver rejected with status " + code);
            }
            if (code >= 500 && code < 600) {
                return DeliveryResult.transientFailure(FailureKind.SERVER_ERROR, "receiver status " + code);
            }
            return DeliveryResult.transientFailure(FailureKind.UNKNOWN, "unexpected status " + code);
        } catch (HttpConnectTimeoutException e) {
            return DeliveryResult.transientFailure(FailureKind.CONNECTION, "connect timeout");
        } catch (HttpTimeoutException e) {
            return DeliveryResult.transientFailure(FailureKind.TIMEOUT, "request timeout");
        } catch (ConnectException e) {
            return DeliveryResult.transientFailure(FailureKind.CONNECTION, "connect refused");
        } catch (IOException e) {
            return DeliveryResult.transientFailure(FailureKind.CONNECTION, "io error: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DeliveryResult.transientFailure(FailureKind.UNKNOWN, "interrupted");
        } catch (Exception e) {
            log.warn("deliver failed event={} reason={}", event.getId(), e.getClass().getSimpleName());
            return DeliveryResult.transientFailure(FailureKind.UNKNOWN, e.getClass().getSimpleName());
        }
    }
}
