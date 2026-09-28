package com.github.highcumontoa.reliableeventdeliveryjava.store;

/** 提交结果分类 */
public enum SubmitStatus {
    /** 新事件被接受 */
    ACCEPTED,
    /** 幂等键相同且内容一致，返回已有事件，不产生重复投递 */
    DUPLICATE,
    /** 幂等键相同但内容冲突，必须拒绝 */
    CONFLICT,
    /** 积压超限，按策略拒绝 */
    OVERLOAD,
    /** 目标聚合被暂停/阻塞且其排队积压已达上限，按策略拒绝或推迟 */
    AGGREGATE_FULL
}
