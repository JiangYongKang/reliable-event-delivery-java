package com.github.highcumontoa.reliableeventdeliveryjava.domain;

/**
 * 聚合闸门状态（按 租户 + aggregateKey 维度）：
 * ACTIVE 正常放行；PAUSED 人工暂停（只收不投）；BLOCKED 队首事件未决而自动阻塞。
 */
public enum GateState {
    /** 正常：队首可投递时正常投递 */
    ACTIVE,
    /** 人工暂停：新事件照常收下但不投递，恢复后按原顺序继续 */
    PAUSED,
    /** 自动阻塞：队首事件处于重试等待或彻底失败，后续事件一律排队 */
    BLOCKED
}
