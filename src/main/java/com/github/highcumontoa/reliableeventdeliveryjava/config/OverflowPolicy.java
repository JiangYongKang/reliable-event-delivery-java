package com.github.highcumontoa.reliableeventdeliveryjava.config;

/** 闸门关闭（暂停/阻塞）的聚合积压到顶时，对新提交的处理策略 */
public enum OverflowPolicy {
    /** 拒绝：429 GATE_CAPACITY_EXCEEDED */
    REJECT,
    /** 推迟：照常接收并入队，等闸门打开后按序投递（仍受全局 max-pending 约束） */
    DEFER
}
