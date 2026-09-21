package com.github.highcumontoa.delivery.domain;

/** 单次投递尝试的结果。 */
public record DeliveryResult(boolean success, FailureKind failureKind, String detail) {

    public static DeliveryResult ok() {
        return new DeliveryResult(true, null, null);
    }

    public static DeliveryResult failure(FailureKind kind, String detail) {
        return new DeliveryResult(false, kind, detail);
    }

    /** 暂时性失败：按退避重试。 */
    public boolean isRetryable() {
        return !success && failureKind != FailureKind.REJECTED;
    }
}
