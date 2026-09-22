package com.github.highcumontoa.reliableeventdeliveryjava.security;

import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

/** 租户凭据存储：令牌只用于内存中校验，绝不写入日志、响应或持久化记录 */
@Component
public class TenantCredentialStore {

    private final Map<String, String> tenantTokens = Map.of(
            "tenant-a", "token-a-secret",
            "tenant-b", "token-b-secret");

    /** 校验令牌，返回匹配的租户；不匹配返回 empty */
    public Optional<String> authenticate(String token) {
        if (token == null) {
            return Optional.empty();
        }
        return tenantTokens.entrySet().stream()
                .filter(e -> constantTimeEquals(e.getValue(), token))
                .map(Map.Entry::getKey)
                .findFirst();
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length()) {
            return false;
        }
        int r = 0;
        for (int i = 0; i < a.length(); i++) {
            r |= a.charAt(i) ^ b.charAt(i);
        }
        return r == 0;
    }
}
