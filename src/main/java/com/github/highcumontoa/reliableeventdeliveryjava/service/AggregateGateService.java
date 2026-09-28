package com.github.highcumontoa.reliableeventdeliveryjava.service;

import com.github.highcumontoa.reliableeventdeliveryjava.api.ApiException;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateStateRequest;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateViewResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.audit.AuditLog;
import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.GateState;
import com.github.highcumontoa.reliableeventdeliveryjava.store.EventStore;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 按租户 + 聚合键的投递闸门：暂停/恢复、阻塞与排队查询。
 * 租户只来自认证上下文；请求体显式声明的 tenantId 若与认证租户不一致，按跨租户操作拒绝（403）。
 */
@Service
public class AggregateGateService {

    private final EventStore store;
    private final DeliveryProperties properties;
    private final AuditLog auditLog;

    public AggregateGateService(EventStore store, DeliveryProperties properties, AuditLog auditLog) {
        this.store = store;
        this.properties = properties;
        this.auditLog = auditLog;
    }

    /** 暂停聚合：新事件照常收下但不再投递；重复暂停返回 409 GATE_ALREADY_PAUSED */
    public GateViewResponse pause(String tenantId, String aggregateKey, GateStateRequest request) {
        checkTenant(tenantId, request);
        validateKey(aggregateKey);
        if (!store.pauseGate(tenantId, aggregateKey)) {
            throw new ApiException(ErrorCode.GATE_ALREADY_PAUSED, 409, "aggregate gate already paused");
        }
        auditLog.recordGate(tenantId, aggregateKey, "GATE_PAUSE", "paused");
        return get(tenantId, aggregateKey);
    }

    /** 恢复聚合：从暂停位置按原提交顺序继续投递；未暂停返回 409 GATE_NOT_PAUSED */
    public GateViewResponse resume(String tenantId, String aggregateKey, GateStateRequest request) {
        checkTenant(tenantId, request);
        validateKey(aggregateKey);
        if (!store.resumeGate(tenantId, aggregateKey)) {
            throw new ApiException(ErrorCode.GATE_NOT_PAUSED, 409, "aggregate gate is not paused");
        }
        auditLog.recordGate(tenantId, aggregateKey, "GATE_RESUME", "resumed");
        return get(tenantId, aggregateKey);
    }

    /** 查询单个聚合闸门；无事件且未暂停过的聚合返回 404 GATE_NOT_FOUND */
    public GateViewResponse get(String tenantId, String aggregateKey) {
        validateKey(aggregateKey);
        return store.findGate(tenantId, aggregateKey, properties.getGateMaxBacklog())
                .map(GateViewResponse::from)
                .orElseThrow(() -> new ApiException(ErrorCode.GATE_NOT_FOUND, 404, "aggregate gate not found"));
    }

    /** 列出租户内所有有排队事件或被暂停的聚合闸门，可按 state 过滤 */
    public List<GateViewResponse> list(String tenantId, String state) {
        if (state != null && !state.isBlank()) {
            try {
                GateState.valueOf(state);
            } catch (IllegalArgumentException e) {
                throw new ApiException(ErrorCode.VALIDATION_FAILED, 400, "unknown gate state: " + state);
            }
        }
        return store.listGates(tenantId, state, properties.getGateMaxBacklog()).stream()
                .map(GateViewResponse::from)
                .toList();
    }

    /** 请求体显式声明的租户必须与认证租户一致，否则视为跨租户操作 */
    private static void checkTenant(String tenantId, GateStateRequest request) {
        if (request != null && request.tenantId() != null && !request.tenantId().equals(tenantId)) {
            throw new ApiException(ErrorCode.TENANT_FORBIDDEN, 403, "tenant mismatch");
        }
    }

    private static void validateKey(String aggregateKey) {
        if (aggregateKey == null || aggregateKey.isBlank()) {
            throw new ApiException(ErrorCode.VALIDATION_FAILED, 400, "aggregateKey is required");
        }
    }
}
