package com.github.highcumontoa.reliableeventdeliveryjava.store;

/** 提交容量参数：全局待投递上限与单个被闸门拦截聚合的排队上限。 */
public record SubmitOptions(int maxPending, int maxAggregateBacklog) {
}
