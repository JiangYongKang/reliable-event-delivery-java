package com.github.highcumontoa.delivery.config;

import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** 投递子系统配置项。 */
@ConfigurationProperties(prefix = "delivery")
public class DeliveryProperties {

    /** 本地持久化文件路径。 */
    private String storePath = "data/event-store.json";
    /** 最大投递尝试次数（含首次），耗尽后进入 FAILED。 */
    private int maxAttempts = 5;
    /** 重试退避基数。 */
    private Duration backoffBase = Duration.ofMillis(200);
    /** 重试退避上限。 */
    private Duration backoffMax = Duration.ofSeconds(10);
    /** 租约时长：执行单元异常退出后事件被重新认领的延迟上界。 */
    private Duration leaseDuration = Duration.ofSeconds(5);
    /** 调度轮询间隔。 */
    private Duration pollInterval = Duration.ofMillis(100);
    /** 单次投递超时。 */
    private Duration deliveryTimeout = Duration.ofSeconds(2);
    /** 全局待投递积压上限，超出拒绝提交。 */
    private int maxPending = 10_000;
    /** 并行投递执行单元数量（并发上限）。 */
    private int workers = 4;
    /** 本地回环接收端地址。 */
    private String loopbackBaseUrl = "http://127.0.0.1:18080";
    /** 租户令牌表：token -> tenantId。仅用于校验，绝不打印。 */
    private Map<String, String> tenantTokens = Map.of();

    public String getStorePath() { return storePath; }
    public void setStorePath(String storePath) { this.storePath = storePath; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getBackoffBase() { return backoffBase; }
    public void setBackoffBase(Duration backoffBase) { this.backoffBase = backoffBase; }
    public Duration getBackoffMax() { return backoffMax; }
    public void setBackoffMax(Duration backoffMax) { this.backoffMax = backoffMax; }
    public Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
    public Duration getDeliveryTimeout() { return deliveryTimeout; }
    public void setDeliveryTimeout(Duration deliveryTimeout) { this.deliveryTimeout = deliveryTimeout; }
    public int getMaxPending() { return maxPending; }
    public void setMaxPending(int maxPending) { this.maxPending = maxPending; }
    public int getWorkers() { return workers; }
    public void setWorkers(int workers) { this.workers = workers; }
    public String getLoopbackBaseUrl() { return loopbackBaseUrl; }
    public void setLoopbackBaseUrl(String loopbackBaseUrl) { this.loopbackBaseUrl = loopbackBaseUrl; }
    public Map<String, String> getTenantTokens() { return tenantTokens; }
    public void setTenantTokens(Map<String, String> tenantTokens) { this.tenantTokens = tenantTokens; }
}
