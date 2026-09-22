package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;

/** 对外视图：不含租约内部字段以外的敏感信息 */
public record EventView(String id, String idempotencyKey, String aggregateKey, long sequence,
                        String status, int attemptCount, String lastError, String createdAt, String updatedAt) {

    public static EventView from(DeliveryEvent e) {
        return new EventView(e.getId(), e.getIdempotencyKey(), e.getAggregateKey(), e.getSequence(),
                e.getStatus().name(), e.getAttemptCount(), e.getLastError(),
                e.getCreatedAt() == null ? null : e.getCreatedAt().toString(),
                e.getUpdatedAt() == null ? null : e.getUpdatedAt().toString());
    }
}
