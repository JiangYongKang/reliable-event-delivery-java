package com.github.highcumontoa.reliableeventdeliveryjava.delivery;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.FailureKind;
import com.github.highcumontoa.reliableeventdeliveryjava.store.EventStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 投递调度器：workerCount 个执行单元并行 认领(租约) -> 投递 -> 落状态。
 * 租约过期未被完成的事件会被其它执行单元重新认领（租约回收），
 * 同一事件同一时刻只会被一个执行单元持有（store 内原子认领保证）。
 */
@Component
public class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final EventStore store;
    private final DeliveryClient client;
    private final DeliveryProperties properties;
    private volatile ExecutorService pool;

    public Dispatcher(EventStore store, DeliveryClient client, DeliveryProperties properties) {
        this.store = store;
        this.client = client;
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        pool = Executors.newFixedThreadPool(properties.getWorkerCount(), r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < properties.getWorkerCount(); i++) {
            String workerId = "worker-" + i + "-" + Long.toHexString(System.nanoTime());
            pool.submit(() -> runLoop(workerId));
        }
        log.info("dispatcher started workers={}", properties.getWorkerCount());
    }

    @PreDestroy
    public void stop() {
        if (pool != null) {
            pool.shutdownNow();
            try {
                pool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runLoop(String workerId) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<DeliveryEvent> claimed = store.claimDeliverable(
                        workerId, properties.getClaimBatchSize(), properties.getLeaseDuration());
                if (claimed.isEmpty()) {
                    Thread.sleep(properties.getPollInterval().toMillis());
                    continue;
                }
                for (DeliveryEvent event : claimed) {
                    deliverOne(workerId, event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("dispatcher loop error worker={} reason={}", workerId, e.getClass().getSimpleName());
            }
        }
    }

    private void deliverOne(String workerId, DeliveryEvent event) {
        DeliveryResult result = client.deliver(event);
        int attempt = event.getAttemptCount() + 1;
        if (result.success()) {
            store.markDelivered(event.getTenantId(), event.getId(), workerId);
            log.info("delivered event={} seq={} attempt={}", event.getId(), event.getSequence(), attempt);
            return;
        }
        if (result.failureKind() == FailureKind.CLIENT_REJECTED) {
            store.markFailed(event.getTenantId(), event.getId(), workerId,
                    result.failureKind().name(), result.detail());
            log.info("permanent failure event={} kind={}", event.getId(), result.failureKind());
            return;
        }
        if (attempt >= properties.getMaxAttempts()) {
            store.markFailed(event.getTenantId(), event.getId(), workerId,
                    result.failureKind().name(), result.detail());
            log.info("retries exhausted event={} attempts={} kind={}", event.getId(), attempt, result.failureKind());
            return;
        }
        Instant next = Instant.now().plus(backoff(attempt));
        store.markRetry(event.getTenantId(), event.getId(), workerId,
                result.failureKind().name(), next, result.detail());
        log.info("transient failure event={} attempt={} kind={} nextAt={}",
                event.getId(), attempt, result.failureKind(), next);
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 30s */
    Duration backoff(int attempt) {
        long baseMs = properties.getRetryBaseBackoff().toMillis();
        long ms = baseMs << Math.min(attempt - 1, 10);
        return Duration.ofMillis(Math.min(ms, MAX_BACKOFF.toMillis()));
    }
}
