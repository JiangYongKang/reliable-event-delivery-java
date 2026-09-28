package com.github.highcumontoa.reliableeventdeliveryjava.config;

/** 被暂停/被阻塞聚合的排队积压达到上限后，对新提交的处理策略 */
public enum GateOverflowPolicy {
    /** 立即拒绝（429 AGGREGATE_BACKLOG_LIMIT） */
    REJECT,
    /** 在限定时间内等待积压腾出空间，超时仍满则拒绝 */
    DEFER
}
