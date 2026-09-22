package com.github.highcumontoa.reliableeventdeliveryjava.security;

/** 当前请求的租户上下文（ThreadLocal），只保存租户标识，不保存令牌 */
public final class TenantContext {
    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private TenantContext() {
    }

    public static void set(String tenantId) { CURRENT.set(tenantId); }
    public static String get() { return CURRENT.get(); }
    public static void clear() { CURRENT.remove(); }
}
