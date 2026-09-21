package com.github.highcumontoa.delivery.domain;

import java.time.Instant;

/**
 * 事件持久化实体。注意：绝不包含任何租户凭据字段。
 */
public class DeliveryEvent {

    private String eventId;
    private String tenantId;
    private String idempotencyKey;
    private String aggregateKey;
    /** 同一 (tenantId, aggregateKey) 内单调递增的提交序号。 */
    private long sequence;
    private String payload;
    /** 内容指纹，用于幂等键冲突检测。 */
    private String contentHash;
    private EventStatus status = EventStatus.PENDING;
    private int attemptCount;
    private Instant nextAttemptAt;
    private String leaseOwner;
    private Instant leaseExpiresAt;
    private FailureKind lastFailureKind;
    private String lastError;
    private Instant createdAt;
    private Instant updatedAt;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getAggregateKey() { return aggregateKey; }
    public void setAggregateKey(String aggregateKey) { this.aggregateKey = aggregateKey; }
    public long getSequence() { return sequence; }
    public void setSequence(long sequence) { this.sequence = sequence; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getContentHash() { return contentHash; }
    public void setContentHash(String contentHash) { this.contentHash = contentHash; }
    public EventStatus getStatus() { return status; }
    public void setStatus(EventStatus status) { this.status = status; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public void setNextAttemptAt(Instant nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
    public String getLeaseOwner() { return leaseOwner; }
    public void setLeaseOwner(String leaseOwner) { this.leaseOwner = leaseOwner; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public void setLeaseExpiresAt(Instant leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }
    public FailureKind getLastFailureKind() { return lastFailureKind; }
    public void setLastFailureKind(FailureKind lastFailureKind) { this.lastFailureKind = lastFailureKind; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
