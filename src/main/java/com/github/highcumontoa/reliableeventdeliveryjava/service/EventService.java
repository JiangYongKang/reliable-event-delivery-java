package com.github.highcumontoa.reliableeventdeliveryjava.service;

import com.github.highcumontoa.reliableeventdeliveryjava.api.ApiException;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventRequest;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.audit.AuditLog;
import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.config.GateOverflowPolicy;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;
import com.github.highcumontoa.reliableeventdeliveryjava.store.EventStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitOptions;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitResult;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitStatus;
import java.util.List;
import org.springframework.stereotype.Service;

/** 事件提交、查询、重放业务逻辑。所有操作限定在当前认证租户内。 */
@Service
public class EventService {

    private final EventStore store;
    private final DeliveryProperties properties;
    private final AuditLog auditLog;

    public EventService(EventStore store, DeliveryProperties properties, AuditLog auditLog) {
        this.store = store;
        this.properties = properties;
        this.auditLog = auditLog;
    }

    public SubmitEventResponse submit(String tenantId, SubmitEventRequest request) {
        validate(request);
        DeliveryEvent event = new DeliveryEvent();
        event.setTenantId(tenantId);
        event.setIdempotencyKey(request.idempotencyKey());
        event.setAggregateKey(request.aggregateKey());
        event.setPayload(request.payload());
        event.setTargetUrl(request.targetUrl());

        SubmitOptions options = new SubmitOptions(properties.getMaxPending(), properties.getGateMaxBacklog());
        SubmitResult result = store.submit(event, options);
        // DEFER：聚合闸门排队到顶时在限定时间内等队首被处理而腾出位置，超时仍满则按拒绝处理
        if (result.status() == SubmitStatus.AGGREGATE_FULL
                && properties.getGateOverflowPolicy() == GateOverflowPolicy.DEFER) {
            result = deferSubmit(event, options);
        }
        switch (result.status()) {
            case ACCEPTED -> {
                auditLog.record(tenantId, result.event().getId(), "SUBMIT", "accepted");
                return new SubmitEventResponse(result.event().getId(), result.event().getStatus().name(), false);
            }
            case DUPLICATE -> {
                auditLog.record(tenantId, result.event().getId(), "SUBMIT", "duplicate-ignored");
                return new SubmitEventResponse(result.event().getId(), result.event().getStatus().name(), true);
            }
            case CONFLICT -> throw new ApiException(ErrorCode.IDEMPOTENCY_CONFLICT, 409,
                    "idempotency key already used with different content");
            case OVERLOAD -> throw new ApiException(ErrorCode.BACKPRESSURE_LIMIT, 429,
                    "pending event limit reached");
            case AGGREGATE_FULL -> throw new ApiException(ErrorCode.AGGREGATE_BACKLOG_LIMIT, 429,
                    "aggregate backlog limit reached for paused or blocked aggregate");
            default -> throw new ApiException(ErrorCode.INTERNAL_ERROR, 500, "unexpected submit result");
        }
    }

    /** 轮询等待被拦截聚合腾出排队空间；返回最后一次提交结果（含超时后的 AGGREGATE_FULL） */
    private SubmitResult deferSubmit(DeliveryEvent event, SubmitOptions options) {
        long deadline = System.nanoTime() + properties.getGateDeferTimeout().toNanos();
        SubmitResult result;
        while (true) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ApiException(ErrorCode.AGGREGATE_BACKLOG_LIMIT, 429,
                        "aggregate backlog limit reached; defer interrupted");
            }
            result = store.submit(event, options);
            if (result.status() != SubmitStatus.AGGREGATE_FULL || System.nanoTime() >= deadline) {
                return result;
            }
        }
    }

    public List<EventView> list(String tenantId) {
        return store.listByTenant(tenantId).stream().map(EventView::from).toList();
    }

    public EventView get(String tenantId, String id) {
        return store.findById(tenantId, id)
                .map(EventView::from)
                .orElseThrow(() -> new ApiException(ErrorCode.EVENT_NOT_FOUND, 404, "event not found"));
    }

    public EventView replay(String tenantId, String id) {
        store.findById(tenantId, id)
                .orElseThrow(() -> new ApiException(ErrorCode.EVENT_NOT_FOUND, 404, "event not found"));
        if (!store.replay(tenantId, id)) {
            throw new ApiException(ErrorCode.EVENT_NOT_REPLAYABLE, 409, "only FAILED events can be replayed");
        }
        auditLog.record(tenantId, id, "REPLAY", "requeued");
        return get(tenantId, id);
    }

    private static void validate(SubmitEventRequest request) {
        if (request == null || isBlank(request.idempotencyKey()) || isBlank(request.aggregateKey())
                || request.payload() == null || isBlank(request.targetUrl())) {
            throw new ApiException(ErrorCode.VALIDATION_FAILED, 400,
                    "idempotencyKey, aggregateKey, payload, targetUrl are required");
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
