package com.github.highcumontoa.reliableeventdeliveryjava.api.dto;

public record SubmitEventResponse(String eventId, String status, boolean duplicate) {
}
