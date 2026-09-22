package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;

/** 业务异常：携带可区分错误码与 HTTP 状态。message 不得包含凭据。 */
public class ApiException extends RuntimeException {
    private final ErrorCode code;
    private final int httpStatus;

    public ApiException(ErrorCode code, int httpStatus, String message) {
        super(message);
        this.code = code;
        this.httpStatus = httpStatus;
    }

    public ErrorCode getCode() { return code; }
    public int getHttpStatus() { return httpStatus; }
}
