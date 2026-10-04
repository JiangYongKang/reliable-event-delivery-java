package com.github.highcumontoa.reliableeventdeliveryjava.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 投递子系统配置项 */
@Component
@ConfigurationProperties(prefix = "delivery")
public class DeliveryProperties {
    /** 本地持久化目录 */
    private String storageDir = "data/events";
    /** 并行投递执行单元数量 */
    private int workerCount = 2;
    /** 单次认领的租约时长 */
    private Duration leaseDuration = Duration.ofSeconds(10);
    /** 单次投递超时 */
    private Duration deliveryTimeout = Duration.ofSeconds(2);
    /** 重试基础退避 */
    private Duration retryBaseBackoff = Duration.ofMillis(200);
    /** 最大投递尝试次数（含首次），耗尽进入 FAILED */
    private int maxAttempts = 5;
    /** 全局待投递（PENDING+RETRY_WAIT+LEASED）上限，超出拒绝提交 */
    private int maxPending = 10000;
    /** 单次扫描认领批量上限 */
    private int claimBatchSize = 32;
    /** 工作线程扫描间隔 */
    private Duration pollInterval = Duration.ofMillis(100);
    /** 被暂停/阻塞的单个聚合允许累积的排队事件上限，到顶按 gate-overflow-policy 处理 */
    private int gateMaxQueuedPerAggregate = 1000;
    /** 闸门聚合排队到顶时的策略：REJECT 拒绝（429 GATE_CAPACITY_EXCEEDED）/ DEFER 推迟接收（429 GATE_CAPACITY_DEFERRED + Retry-After） */
    private OverflowPolicy gateOverflowPolicy = OverflowPolicy.REJECT;

    public String getStorageDir() { return storageDir; }
    public void setStorageDir(String storageDir) { this.storageDir = storageDir; }
    public int getWorkerCount() { return workerCount; }
    public void setWorkerCount(int workerCount) { this.workerCount = workerCount; }
    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
    public Duration getDeliveryTimeout() { return deliveryTimeout; }
    public void setDeliveryTimeout(Duration deliveryTimeout) { this.deliveryTimeout = deliveryTimeout; }
    public Duration getRetryBaseBackoff() { return retryBaseBackoff; }
    public void setRetryBaseBackoff(Duration retryBaseBackoff) { this.retryBaseBackoff = retryBaseBackoff; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public int getMaxPending() { return maxPending; }
    public void setMaxPending(int maxPending) { this.maxPending = maxPending; }
    public int getClaimBatchSize() { return claimBatchSize; }
    public void setClaimBatchSize(int claimBatchSize) { this.claimBatchSize = claimBatchSize; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
    public int getGateMaxQueuedPerAggregate() { return gateMaxQueuedPerAggregate; }
    public void setGateMaxQueuedPerAggregate(int gateMaxQueuedPerAggregate) {
        this.gateMaxQueuedPerAggregate = gateMaxQueuedPerAggregate;
    }
    public OverflowPolicy getGateOverflowPolicy() { return gateOverflowPolicy; }
    public void setGateOverflowPolicy(OverflowPolicy gateOverflowPolicy) {
        this.gateOverflowPolicy = gateOverflowPolicy;
    }
}
