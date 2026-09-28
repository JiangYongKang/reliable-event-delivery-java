package com.github.highcumontoa.reliableeventdeliveryjava.domain;

/** 聚合被自动阻塞的原因（仅 BLOCKED 时有意义） */
public enum BlockReason {
    /** 未阻塞 */
    NONE,
    /** 队首事件暂时性失败，仍在等待下次重试 */
    RETRYING,
    /** 队首事件已彻底失败（FAILED），需人工重放后才能继续 */
    PERMANENT_FAILURE
}
