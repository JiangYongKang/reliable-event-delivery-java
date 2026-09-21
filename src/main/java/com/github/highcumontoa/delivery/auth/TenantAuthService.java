package com.github.highcumontoa.delivery.auth;

import com.github.highcumontoa.delivery.config.DeliveryProperties;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 租户令牌校验。
 *
 * <p>安全约束：令牌只参与常量时间比对，绝不写入日志、异常消息或任何持久化记录；
 * 拒绝原因通过 {@link TenantAccessDeniedException.Reason} 可区分地表达。
 */
@Service
public class TenantAuthService {

    private static final Logger log = LoggerFactory.getLogger(TenantAuthService.class);

    private final DeliveryProperties properties;

    public TenantAuthService(DeliveryProperties properties) {
        this.properties = properties;
    }

    /**
     * 校验令牌并返回其所属租户。
     *
     * @throws TenantAccessDeniedException 令牌缺失（MISSING_TOKEN）或无效（INVALID_TOKEN）
     */
    public String authenticate(String token) {
        if (token == null || token.isBlank()) {
            log.info("auth rejected: reason={}", TenantAccessDeniedException.Reason.MISSING_TOKEN);
            throw new TenantAccessDeniedException(TenantAccessDeniedException.Reason.MISSING_TOKEN);
        }
        for (Map.Entry<String, String> entry : properties.getTenantTokens().entrySet()) {
            if (constantTimeEquals(entry.getKey(), token)) {
                return entry.getValue();
            }
        }
        // 不打印令牌本身，只打印其长度与哈希前缀以便排障
        log.info("auth rejected: reason={} tokenLength={}",
                TenantAccessDeniedException.Reason.INVALID_TOKEN, token.length());
        throw new TenantAccessDeniedException(TenantAccessDeniedException.Reason.INVALID_TOKEN);
    }

    /**
     * 校验令牌对应的租户是否有权访问 path 中的租户资源。
     *
     * @throws TenantAccessDeniedException 租户不匹配（TENANT_MISMATCH）
     */
    public String authorize(String token, String pathTenantId) {
        String tokenTenant = authenticate(token);
        if (!tokenTenant.equals(pathTenantId)) {
            log.info("auth rejected: reason={} pathTenant={}", 
                    TenantAccessDeniedException.Reason.TENANT_MISMATCH, pathTenantId);
            throw new TenantAccessDeniedException(TenantAccessDeniedException.Reason.TENANT_MISMATCH);
        }
        return tokenTenant;
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
