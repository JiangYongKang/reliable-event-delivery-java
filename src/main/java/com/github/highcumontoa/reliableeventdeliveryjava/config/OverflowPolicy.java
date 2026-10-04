package com.github.highcumontoa.reliableeventdeliveryjava.config;

/** 闸门关闭（暂停/阻塞）的聚合积压到顶时，对新提交的处理策略；两种策略都不允许排队数越过上限 */
public enum OverflowPolicy {
    /** 拒绝：429 GATE_CAPACITY_EXCEEDED */
    REJECT,
    /** 推迟：本次不接收，返回 429 GATE_CAPACITY_DEFERRED + Retry-After，客户端稍后重试 */
    DEFER
}
