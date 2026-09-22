package com.github.highcumontoa.reliableeventdeliveryjava.domain;

/** 投递失败分类：暂时性（可重试）与永久性（不重试） */
public enum FailureKind {
    /** 暂时性：连接/读取超时 */
    TIMEOUT,
    /** 暂时性：连接失败（对端不可达、拒绝连接） */
    CONNECTION,
    /** 暂时性：服务端 5xx */
    SERVER_ERROR,
    /** 永久性：接收端明确拒绝（4xx） */
    CLIENT_REJECTED,
    /** 未知异常，按暂时性处理 */
    UNKNOWN
}
