package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;

/** 业务异常：携带可区分错误码与 HTTP 状态。message 不得包含凭据。 */
public class ApiException extends RuntimeException {
    private final ErrorCode code;
    private final int httpStatus;
    /** 非空时响应携带 Retry-After 头（秒），用于“推迟”类错误 */
    private final Integer retryAfterSeconds;

    public ApiException(ErrorCode code, int httpStatus, String message) {
        this(code, httpStatus, message, null);
    }

    public ApiException(ErrorCode code, int httpStatus, String message, Integer retryAfterSeconds) {
        super(message);
        this.code = code;
        this.httpStatus = httpStatus;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public ErrorCode getCode() { return code; }
    public int getHttpStatus() { return httpStatus; }
    public Integer getRetryAfterSeconds() { return retryAfterSeconds; }
}
