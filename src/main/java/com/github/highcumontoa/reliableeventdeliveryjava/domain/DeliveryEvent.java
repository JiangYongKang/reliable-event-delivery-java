package com.github.highcumontoa.reliableeventdeliveryjava.domain;

import java.time.Instant;

/** 持久化的投递事件。注意：不包含任何凭据字段，toString 不输出敏感信息。 */
public class DeliveryEvent {
    private String id;
    private String tenantId;
    private String idempotencyKey;
    private String aggregateKey;
    private long sequence;
    private String payload;
    private String targetUrl;
    private EventStatus status;
    private int attemptCount;
    private Instant nextAttemptAt;
    private String leaseOwner;
    private Instant leaseExpiresAt;
    private Instant createdAt;
    private Instant updatedAt;
    private String lastError;

    public DeliveryEvent() {
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
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
    public String getTargetUrl() { return targetUrl; }
    public void setTargetUrl(String targetUrl) { this.targetUrl = targetUrl; }
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
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    @Override
    public String toString() {
        return "DeliveryEvent{id=" + id + ", tenant=" + tenantId + ", idemKey=" + idempotencyKey
                + ", agg=" + aggregateKey + ", seq=" + sequence + ", status=" + status
                + ", attempts=" + attemptCount + "}";
    }
}
