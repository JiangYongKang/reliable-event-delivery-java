package com.github.highcumontoa.reliableeventdeliveryjava.gate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;

/**
 * 聚合键投递闸门（按 tenantId + aggregateKey 维度）。持久化到本地文件，重启后恢复。
 * 不含任何凭据字段。
 */
public class AggregateGate {
    private String tenantId;
    private String aggregateKey;
    /** 人工暂停：暂停期间新事件照常接收，但一件都不投递 */
    private boolean paused;
    /** 自动阻塞：卡住该聚合的事件 id（还在等重试或已彻底失败），为 null 表示未阻塞 */
    private String blockedEventId;
    /** 阻塞原因（可区分：重试中 / 重试耗尽 / 永久拒绝） */
    private String blockReason;
    private Instant updatedAt;

    public AggregateGate() {
    }

    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    public String getAggregateKey() { return aggregateKey; }
    public void setAggregateKey(String aggregateKey) { this.aggregateKey = aggregateKey; }
    public boolean isPaused() { return paused; }
    public void setPaused(boolean paused) { this.paused = paused; }
    public String getBlockedEventId() { return blockedEventId; }
    public void setBlockedEventId(String blockedEventId) { this.blockedEventId = blockedEventId; }
    public String getBlockReason() { return blockReason; }
    public void setBlockReason(String blockReason) { this.blockReason = blockReason; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    @JsonIgnore
    public boolean isBlocked() {
        return blockedEventId != null;
    }
}
