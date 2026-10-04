package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;

/** 业务异常：携带可区分错误码与 HTTP 状态。message 不得包含凭据。 */
public class ApiException extends RuntimeException {
    private final ErrorCode code;
    private final int httpStatus;
    /** 可选：建议客户端多少秒后重试（映射为 Retry-After 响应头），null 表示不输出 */
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
