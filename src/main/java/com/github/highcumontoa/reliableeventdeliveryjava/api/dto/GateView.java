package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

/** 聚合闸门对外视图：状态、卡住的事件、排队数量；不含敏感信息 */
public record GateView(String tenantId, String aggregateKey, String state,
                       String blockedEventId, String blockReason, int queuedCount, String updatedAt) {
}
