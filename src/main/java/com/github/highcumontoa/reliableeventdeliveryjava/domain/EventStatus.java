package com.github.highcumontoa.reliableeventdeliveryjava.domain;

/** 事件投递状态机：PENDING -> LEASED -> DELIVERED / RETRY_WAIT / FAILED */
public enum EventStatus {
    /** 已持久化，等待被认领投递 */
    PENDING,
    /** 已被某执行单元租约认领，投递进行中 */
    LEASED,
    /** 暂时性失败，等待下次重试时点 */
    RETRY_WAIT,
    /** 终态：投递成功 */
    DELIVERED,
    /** 终态：重试耗尽或永久性失败，可显式重放 */
    FAILED
}
