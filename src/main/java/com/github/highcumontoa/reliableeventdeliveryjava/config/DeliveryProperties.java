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
    /** 单个被暂停/被阻塞聚合允许排队的未投递事件上限 */
    private int gateMaxBacklog = 1000;
    /** 积压到顶后的策略：REJECT 立即拒绝，DEFER 限时等待 */
    private GateOverflowPolicy gateOverflowPolicy = GateOverflowPolicy.REJECT;
    /** DEFER 策略下最长等待时间 */
    private Duration gateDeferTimeout = Duration.ofSeconds(5);

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
    public int getGateMaxBacklog() { return gateMaxBacklog; }
    public void setGateMaxBacklog(int gateMaxBacklog) { this.gateMaxBacklog = gateMaxBacklog; }
    public GateOverflowPolicy getGateOverflowPolicy() { return gateOverflowPolicy; }
    public void setGateOverflowPolicy(GateOverflowPolicy gateOverflowPolicy) { this.gateOverflowPolicy = gateOverflowPolicy; }
    public Duration getGateDeferTimeout() { return gateDeferTimeout; }
    public void setGateDeferTimeout(Duration gateDeferTimeout) { this.gateDeferTimeout = gateDeferTimeout; }
}
