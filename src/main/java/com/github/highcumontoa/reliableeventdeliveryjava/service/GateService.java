package com.github.highcumontoa.reliableeventdeliveryjava.service;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateView;
import com.github.highcumontoa.reliableeventdeliveryjava.audit.AuditLog;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.AggregateGate;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.GateStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.EventStore;
import java.util.List;
import org.springframework.stereotype.Service;

/** 聚合闸门业务逻辑：暂停/恢复/查询。所有操作限定在当前认证租户内（控制器已校验）。 */
@Service
public class GateService {

    private final GateStore gateStore;
    private final EventStore eventStore;
    private final AuditLog auditLog;

    public GateService(GateStore gateStore, EventStore eventStore, AuditLog auditLog) {
        this.gateStore = gateStore;
        this.eventStore = eventStore;
        this.auditLog = auditLog;
    }

    public GateView pause(String tenantId, String aggregateKey) {
        AggregateGate g = gateStore.pause(tenantId, aggregateKey);
        auditLog.record(tenantId, aggregateKey, "GATE_PAUSE", "paused");
        return toView(g);
    }

    public GateView resume(String tenantId, String aggregateKey) {
        AggregateGate g = gateStore.resume(tenantId, aggregateKey);
        auditLog.record(tenantId, aggregateKey, "GATE_RESUME",
                g.isBlocked() ? "resumed-still-blocked" : "resumed-open");
        return toView(g);
    }

    public GateView get(String tenantId, String aggregateKey) {
        return gateStore.snapshot(tenantId, aggregateKey)
                .map(this::toView)
                .orElseGet(() -> openView(tenantId, aggregateKey));
    }

    /** 列出租户下所有被暂停或被阻塞的聚合闸门（含卡住原因、卡住的事件、排队数） */
    public List<GateView> listNonOpen(String tenantId) {
        return gateStore.listNonOpen(tenantId).stream().map(this::toView).toList();
    }

    private GateView toView(AggregateGate g) {
        String state = g.isPaused() && g.isBlocked() ? "PAUSED_BLOCKED"
                : g.isPaused() ? "PAUSED"
                : g.isBlocked() ? "BLOCKED" : "OPEN";
        int queued = queuedCount(g);
        return new GateView(g.getTenantId(), g.getAggregateKey(), state,
                g.getBlockedEventId(), g.getBlockReason(), queued,
                g.getUpdatedAt() == null ? null : g.getUpdatedAt().toString());
    }

    /** 排队数：阻塞时只算卡在 blocked 事件后面的；暂停时算整个聚合积压 */
    private int queuedCount(AggregateGate g) {
        Long afterSeq = null;
        if (g.isBlocked()) {
            afterSeq = eventStore.findById(g.getTenantId(), g.getBlockedEventId())
                    .map(DeliveryEvent::getSequence)
                    .orElse(null);
        }
        return eventStore.countQueuedForAggregate(g.getTenantId(), g.getAggregateKey(), afterSeq);
    }

    private static GateView openView(String tenantId, String aggregateKey) {
        return new GateView(tenantId, aggregateKey, "OPEN", null, null, 0, null);
    }
}
