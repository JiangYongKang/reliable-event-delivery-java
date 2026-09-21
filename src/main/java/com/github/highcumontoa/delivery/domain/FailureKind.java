package com.github.highcumontoa.delivery.domain;

/** 投递失败分类：决定重试策略。 */
public enum FailureKind {
    /** 暂时性：连接超时/读取超时，按退避重试。 */
    TIMEOUT,
    /** 暂时性：连接被拒绝/网络不可达，按退避重试。 */
    CONNECTION,
    /** 暂时性：接收端 5xx，按退避重试。 */
    SERVER_ERROR,
    /** 永久性：接收端 4xx 明确拒绝，不重试。 */
    REJECTED,
    /** 未分类异常，按暂时性处理。 */
    UNKNOWN
}
