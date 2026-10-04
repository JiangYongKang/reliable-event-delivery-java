package com.github.highcumontoa.reliableeventdeliveryjava.config;

/** 闸门关闭（暂停/阻塞）的聚合积压到顶时，对新提交的处理策略 */
public enum OverflowPolicy {
    /** 拒绝：429 GATE_CAPACITY_EXCEEDED */
    REJECT,
    /** 推迟：429 GATE_CAPACITY_DEFERRED + Retry-After，本次不入队，客户端稍后以同一幂等键重试 */
    DEFER
}
