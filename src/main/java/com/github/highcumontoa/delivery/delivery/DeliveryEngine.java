package com.github.highcumontoa.delivery.delivery;

import com.github.highcumontoa.delivery.config.DeliveryProperties;
import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.DeliveryResult;
import com.github.highcumontoa.delivery.domain.EventStatus;
import com.github.highcumontoa.delivery.store.EventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * 投递引擎。
 *
 * <p>并发安全：多个执行单元通过 {@link EventStore#claimBatch} 原子认领事件并持有租约，
 * 同一事件不会被并发重复投递；执行单元异常退出后，租约到期的事件会被其他单元重新认领。
 *
 * <p>重试策略：暂时性失败（超时/连接/5xx）按指数退避重试，次数耗尽进入 FAILED；
 * 永久性失败（4xx 拒绝）直接进入 FAILED，不做无意义重试。FAILED 可查询、可显式重放。
 */
@Component
public class DeliveryEngine implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);

    private final EventStore store;
    private final HttpDeliverer deliverer;
    private final DeliveryProperties properties;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public DeliveryEngine(EventStore store, HttpDeliverer deliverer, DeliveryProperties properties) {
        this.store = store;
        this.deliverer = deliverer;
        this.properties = properties;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            java.util.concurrent.atomic.AtomicInteger idx = new java.util.concurrent.atomic.AtomicInteger();
            executor = Executors.newFixedThreadPool(properties.getWorkers(), runnable -> {
                Thread t = new Thread(runnable, "delivery-worker-" + idx.getAndIncrement());
                t.setDaemon(true);
                return t;
            });
            for (int i = 0; i < properties.getWorkers(); i++) {
                String workerId = "worker-" + i;
                executor.submit(() -> workerLoop(workerId));
            }
            log.info("delivery engine started: workers={} lease={}ms maxAttempts={}",
                    properties.getWorkers(), properties.getLeaseDuration().toMillis(), properties.getMaxAttempts());
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("delivery engine stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void workerLoop(String workerId) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Instant now = Instant.now();
                List<DeliveryEvent> claimed = store.claimBatch(
                        workerId, 1, now, now.plus(properties.getLeaseDuration()));
                if (claimed.isEmpty()) {
                    Thread.sleep(properties.getPollInterval().toMillis());
                    continue;
                }
                process(workerId, claimed.get(0));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("worker loop error: worker={} error={}", workerId, e.getMessage());
            }
        }
    }

    private void process(String workerId, DeliveryEvent event) {
        DeliveryResult result = deliverer.deliver(event, properties.getLoopbackBaseUrl() + "/loopback/receive");
        event.setAttemptCount(event.getAttemptCount() + 1);
        event.setLeaseOwner(null);
        event.setLeaseExpiresAt(null);
        if (result.success()) {
            event.setStatus(EventStatus.DELIVERED);
            event.setLastFailureKind(null);
            event.setLastError(null);
            log.info("event delivered: eventId={} tenant={} seq={} attempts={}",
                    event.getEventId(), event.getTenantId(), event.getSequence(), event.getAttemptCount());
        } else if (!result.isRetryable()) {
            // 永久性失败：不重试，直接终态
            event.setStatus(EventStatus.FAILED);
            event.setLastFailureKind(result.failureKind());
            event.setLastError(result.detail());
            log.info("event permanently failed: eventId={} kind={} detail={}",
                    event.getEventId(), result.failureKind(), result.detail());
        } else if (event.getAttemptCount() >= properties.getMaxAttempts()) {
            // 重试耗尽：进入可查询、可重放的 FAILED
            event.setStatus(EventStatus.FAILED);
            event.setLastFailureKind(result.failureKind());
            event.setLastError("retries exhausted: " + result.detail());
            log.info("event retries exhausted: eventId={} kind={} attempts={}",
                    event.getEventId(), result.failureKind(), event.getAttemptCount());
        } else {
            // 暂时性失败：指数退避后重试
            Duration backoff = backoff(event.getAttemptCount());
            event.setStatus(EventStatus.PENDING);
            event.setNextAttemptAt(Instant.now().plus(backoff));
            event.setLastFailureKind(result.failureKind());
            event.setLastError(result.detail());
            log.info("event retry scheduled: eventId={} kind={} attempt={} backoffMs={}",
                    event.getEventId(), result.failureKind(), event.getAttemptCount(), backoff.toMillis());
        }
        boolean applied = store.updateIfLeaseHeld(event, workerId);
        if (!applied) {
            // 租约已丢失（如被回收）：放弃本次结果，事件会被重新认领，保持状态自洽
            log.info("lease lost, result discarded: eventId={} worker={}", event.getEventId(), workerId);
        }
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 backoffMax。 */
    Duration backoff(int attempt) {
        long baseMs = properties.getBackoffBase().toMillis();
        long maxMs = properties.getBackoffMax().toMillis();
        long ms = baseMs;
        for (int i = 1; i < attempt && ms < maxMs; i++) {
            ms = Math.min(maxMs, ms * 2);
        }
        return Duration.ofMillis(Math.min(ms, maxMs));
    }
}
