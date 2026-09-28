package com.github.highcumontoa.reliableeventdeliveryjava.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 审计日志：只记录事件标识与状态流转，绝不记录令牌、签名密钥等凭据 */
@Component
public class AuditLog {
    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    public void record(String tenantId, String eventId, String action, String detail) {
        log.info("audit tenant={} event={} action={} detail={}", tenantId, eventId, action, detail);
    }

    /** 聚合闸门动作审计：只记录租户、聚合键与动作，不记录任何凭据 */
    public void recordGate(String tenantId, String aggregateKey, String action, String detail) {
        log.info("audit tenant={} aggregate={} action={} detail={}", tenantId, aggregateKey, action, detail);
    }
}
