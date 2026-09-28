package com.github.highcumontoa.reliableeventdeliveryjava.domain;

/** 可区分的错误原因码，出现在 API 错误响应中 */
public enum ErrorCode {
    TENANT_MISSING,
    TENANT_UNAUTHORIZED,
    TENANT_FORBIDDEN,
    IDEMPOTENCY_CONFLICT,
    BACKPRESSURE_LIMIT,
    GATE_CAPACITY_EXCEEDED,
    EVENT_NOT_FOUND,
    EVENT_NOT_REPLAYABLE,
    VALIDATION_FAILED,
    INTERNAL_ERROR
}
