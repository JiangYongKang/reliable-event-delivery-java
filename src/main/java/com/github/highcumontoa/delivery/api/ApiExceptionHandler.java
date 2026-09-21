package com.github.highcumontoa.delivery.api;

import com.github.highcumontoa.delivery.auth.TenantAccessDeniedException;
import com.github.highcumontoa.delivery.service.SubmissionException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一错误响应：原因可区分（error 字段），且绝不回显令牌等敏感信息。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(TenantAccessDeniedException.class)
    public ResponseEntity<Map<String, String>> handleAccessDenied(TenantAccessDeniedException e) {
        HttpStatus status = switch (e.getReason()) {
            case MISSING_TOKEN, INVALID_TOKEN -> HttpStatus.UNAUTHORIZED;
            case TENANT_MISMATCH -> HttpStatus.FORBIDDEN;
        };
        // 日志只记录原因枚举，不记录令牌
        log.info("access denied: reason={}", e.getReason());
        return ResponseEntity.status(status).body(Map.of("error", e.getReason().name()));
    }

    @ExceptionHandler(SubmissionException.class)
    public ResponseEntity<Map<String, String>> handleSubmission(SubmissionException e) {
        HttpStatus status = switch (e.getReason()) {
            case IDEMPOTENCY_CONFLICT -> HttpStatus.CONFLICT;
            case BACKPRESSURE_LIMIT -> HttpStatus.TOO_MANY_REQUESTS;
        };
        log.info("submission rejected: reason={}", e.getReason());
        return ResponseEntity.status(status).body(Map.of("error", e.getReason().name()));
    }
}
