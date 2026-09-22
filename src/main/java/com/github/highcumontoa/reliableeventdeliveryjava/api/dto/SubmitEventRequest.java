package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

public record SubmitEventRequest(String idempotencyKey, String aggregateKey, String payload, String targetUrl) {
}
