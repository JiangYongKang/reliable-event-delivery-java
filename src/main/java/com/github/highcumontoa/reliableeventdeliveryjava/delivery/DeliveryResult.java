package com.github.highcumontoa.reliableeventdeliveryjava.delivery;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.FailureKind;

/** 单次投递尝试结果。detail 不得包含凭据。 */
public record DeliveryResult(boolean success, FailureKind failureKind, String detail) {

    public static DeliveryResult ok() {
        return new DeliveryResult(true, null, null);
    }

    public static DeliveryResult transientFailure(FailureKind kind, String detail) {
        return new DeliveryResult(false, kind, detail);
    }

    public static DeliveryResult permanentFailure(String detail) {
        return new DeliveryResult(false, FailureKind.CLIENT_REJECTED, detail);
    }
}
