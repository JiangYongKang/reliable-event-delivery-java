package com.github.highcumontoa.reliableeventdeliveryjava.store;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;

public record SubmitResult(SubmitStatus status, DeliveryEvent event) {
}
