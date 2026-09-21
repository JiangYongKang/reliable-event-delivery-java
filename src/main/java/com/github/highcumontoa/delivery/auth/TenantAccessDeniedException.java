package com.github.highcumontoa.delivery.auth;

/** 越权/未认证访问。reason 可区分，但绝不包含令牌内容。 */
public class TenantAccessDeniedException extends RuntimeException {

    public enum Reason {
        MISSING_TOKEN,
        INVALID_TOKEN,
        TENANT_MISMATCH
    }

    private final Reason reason;

    public TenantAccessDeniedException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
