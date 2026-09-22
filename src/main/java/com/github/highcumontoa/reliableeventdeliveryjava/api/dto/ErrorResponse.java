package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

/** 错误响应：只含错误码与通用描述，不回显请求中的凭据 */
public record ErrorResponse(String code, String message) {
}
