package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

import com.github.highcumontoa.reliableeventdeliveryjava.store.GateView;

/** 聚合闸门对外视图：含状态、阻塞原因、卡住事件与排队数量、暂停与容量信息 */
public record GateViewResponse(
        String tenantId,
        String aggregateKey,
        String state,
        String reason,
        String headEventId,
        Long headSequence,
        String headStatus,
        Integer headAttemptCount,
        String headLastError,
        String headNextAttemptAt,
        int queuedCount,
        boolean paused,
        String pausedAt,
        Integer backlogLimit) {

    public static GateViewResponse from(GateView g) {
        return new GateViewResponse(
                g.tenantId(),
                g.aggregateKey(),
                g.state().name(),
                g.reason() == null ? null : g.reason().name(),
                g.headEventId(),
                g.headSequence(),
                g.headStatus(),
                g.headAttemptCount(),
                g.headLastError(),
                g.headNextAttemptAt() == null ? null : g.headNextAttemptAt().toString(),
                g.queuedCount(),
                g.paused(),
                g.pausedAt() == null ? null : g.pausedAt().toString(),
                g.backlogLimit());
    }
}
