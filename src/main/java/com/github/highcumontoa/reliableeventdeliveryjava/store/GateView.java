package com.github.highcumontoa.reliableeventdeliveryjava.store;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.BlockReason;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.GateState;
import java.time.Instant;

/**
 * 聚合闸门读模型：状态、阻塞原因/卡住的事件/排队数量、暂停信息、积压上限。
 * queuedCount 含卡住的队首及其后所有未投递（非 DELIVERED）事件。
 */
public record GateView(
        String tenantId,
        String aggregateKey,
        GateState state,
        BlockReason reason,
        String headEventId,
        Long headSequence,
        String headStatus,
        Integer headAttemptCount,
        String headLastError,
        Instant headNextAttemptAt,
        int queuedCount,
        boolean paused,
        Instant pausedAt,
        Integer backlogLimit) {
}
