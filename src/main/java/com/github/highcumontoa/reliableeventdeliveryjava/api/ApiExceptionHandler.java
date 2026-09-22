package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.ErrorResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 统一错误响应：只含错误码与通用描述，不回显请求头或凭据 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApi(ApiException e) {
        return ResponseEntity.status(e.getHttpStatus())
                .body(new ErrorResponse(e.getCode().name(), e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleOther(Exception e) {
        log.warn("unhandled error type={}", e.getClass().getSimpleName());
        return ResponseEntity.status(500)
                .body(new ErrorResponse(ErrorCode.INTERNAL_ERROR.name(), "internal error"));
    }
}
