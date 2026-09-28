package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

/**
 * 暂停/恢复请求体。tenantId 可选，仅用于显式声明并校验归属；
 * 若与认证租户不一致则返回 403 TENANT_FORBIDDEN（跨租户操作可区分拒绝）。
 */
public record GateStateRequest(String tenantId) {
}
