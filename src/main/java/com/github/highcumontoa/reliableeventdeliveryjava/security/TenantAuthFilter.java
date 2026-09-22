package com.github.highcumontoa.reliableeventdeliveryjava.security;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * 租户认证过滤器：仅保护 /api/**。
 * 令牌只用于内存校验，绝不写入日志或错误响应；失败原因可区分（缺失 vs 无效）。
 */
@Component
public class TenantAuthFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(TenantAuthFilter.class);
    private static final String TOKEN_HEADER = "X-Api-Key";

    private final TenantCredentialStore credentialStore;

    public TenantAuthFilter(TenantCredentialStore credentialStore) {
        this.credentialStore = credentialStore;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String token = request.getHeader(TOKEN_HEADER);
        if (token == null || token.isBlank()) {
            reject(response, ErrorCode.TENANT_MISSING, "missing api key");
            return;
        }
        Optional<String> tenant = credentialStore.authenticate(token);
        if (tenant.isEmpty()) {
            // 不记录令牌内容，只记录来源路径
            log.info("auth failed path={} reason=invalid-token", request.getRequestURI());
            reject(response, ErrorCode.TENANT_UNAUTHORIZED, "invalid api key");
            return;
        }
        try {
            TenantContext.set(tenant.get());
            chain.doFilter(request, response);
        } finally {
            TenantContext.clear();
        }
    }

    private static void reject(HttpServletResponse response, ErrorCode code, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"code\":\"" + code.name() + "\",\"message\":\"" + message + "\"}");
    }
}
