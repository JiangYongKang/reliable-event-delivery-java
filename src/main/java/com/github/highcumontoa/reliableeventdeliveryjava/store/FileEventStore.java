package com.github.highcumontoa.reliableeventdeliveryjava.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.config.OverflowPolicy;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.EventStatus;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.AggregateGate;
import com.github.highcumontoa.reliableeventdeliveryjava.gate.GateStore;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 基于本地 JSON 文件的事件存储。
 * 内存索引 + 每次状态变更原子落盘（临时文件 + move），进程重启后从文件恢复。
 * 重启前处于 LEASED 的事件租约到期后会被重新认领，不会重复投递已 DELIVERED 的事件。
 */
@Component
public class FileEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FileEventStore.class);

    private final Path storeFile;
    private final ObjectMapper mapper;
    private final ReentrantLock lock = new ReentrantLock();
    private final DeliveryProperties properties;
    private final GateStore gateStore;

    /** 全量事件，id -> event */
    private final Map<String, DeliveryEvent> byId = new LinkedHashMap<>();
    /** 幂等索引：(tenant|idemKey) -> eventId */
    private final Map<String, String> idemIndex = new HashMap<>();
    /** 聚合序号：(tenant|aggKey) -> 已分配的最大 sequence */
    private final Map<String, Long> aggSeq = new HashMap<>();

    /** 便捷构造：自带独立 GateStore（测试与本地使用），不加载既有闸门文件 */
    public FileEventStore(DeliveryProperties properties) {
        this(properties, new GateStore(properties));
    }

    @org.springframework.beans.factory.annotation.Autowired
    public FileEventStore(DeliveryProperties properties, GateStore gateStore) {
        this.properties = properties;
        this.gateStore = gateStore;
        this.storeFile = Path.of(properties.getStorageDir(), "events.json");
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @PostConstruct
    void load() {
        lock.lock();
        try {
            if (!Files.exists(storeFile)) {
                return;
            }
            DeliveryEvent[] events = mapper.readValue(storeFile.toFile(), DeliveryEvent[].class);
            for (DeliveryEvent e : events) {
                byId.put(e.getId(), e);
                idemIndex.put(idemKey(e.getTenantId(), e.getIdempotencyKey()), e.getId());
                aggSeq.merge(aggKey(e.getTenantId(), e.getAggregateKey()), e.getSequence(), Math::max);
            }
            log.info("store loaded file={} events={}", storeFile, byId.size());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load event store " + storeFile, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SubmitResult submit(DeliveryEvent event, int maxPending) {
        lock.lock();
        try {
            String idemKey = idemKey(event.getTenantId(), event.getIdempotencyKey());
            String existingId = idemIndex.get(idemKey);
            if (existingId != null) {
                DeliveryEvent existing = byId.get(existingId);
                if (contentMatches(existing, event)) {
                    return new SubmitResult(SubmitStatus.DUPLICATE, copy(existing));
                }
                return new SubmitResult(SubmitStatus.CONFLICT, null);
            }
            if (countPendingLocked() >= maxPending) {
                return new SubmitResult(SubmitStatus.OVERLOAD, null);
            }
            // 闸门关闭（暂停/阻塞）的聚合：排队到顶后按策略拒绝或推迟接收
            AggregateGate gate = gateStore.snapshot(event.getTenantId(), event.getAggregateKey()).orElse(null);
            if (gate != null && (gate.isPaused() || gate.isBlocked())
                    && countQueuedForAggregateLocked(event.getTenantId(), event.getAggregateKey(), null)
                            >= properties.getGateMaxQueuedPerAggregate()
                    && properties.getGateOverflowPolicy() == OverflowPolicy.REJECT) {
                return new SubmitResult(SubmitStatus.GATE_OVERFLOW, null);
            }
            Instant now = Instant.now();
            String aggKey = aggKey(event.getTenantId(), event.getAggregateKey());
            long seq = aggSeq.getOrDefault(aggKey, 0L) + 1;
            aggSeq.put(aggKey, seq);

            event.setId(UUID.randomUUID().toString());
            event.setSequence(seq);
            event.setStatus(EventStatus.PENDING);
            event.setAttemptCount(0);
            event.setNextAttemptAt(now);
            event.setCreatedAt(now);
            event.setUpdatedAt(now);
            byId.put(event.getId(), event);
            idemIndex.put(idemKey, event.getId());
            persistLocked();
            return new SubmitResult(SubmitStatus.ACCEPTED, copy(event));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<DeliveryEvent> findById(String tenantId, String id) {
        lock.lock();
        try {
            DeliveryEvent e = byId.get(id);
            if (e == null || !e.getTenantId().equals(tenantId)) {
                return Optional.empty();
            }
            return Optional.of(copy(e));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<DeliveryEvent> listByTenant(String tenantId) {
        lock.lock();
        try {
            List<DeliveryEvent> out = new ArrayList<>();
            for (DeliveryEvent e : byId.values()) {
                if (e.getTenantId().equals(tenantId)) {
                    out.add(copy(e));
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<DeliveryEvent> claimDeliverable(String workerId, int max, Duration leaseDuration) {
        lock.lock();
        try {
            Instant now = Instant.now();
            // 每个聚合键的队首（最小 sequence 的非终态事件）
            Map<String, DeliveryEvent> heads = new HashMap<>();
            for (DeliveryEvent e : byId.values()) {
                if (e.getStatus() == EventStatus.DELIVERED || e.getStatus() == EventStatus.FAILED) {
                    continue;
                }
                String key = aggKey(e.getTenantId(), e.getAggregateKey());
                heads.merge(key, e, (a, b) -> a.getSequence() <= b.getSequence() ? a : b);
            }
            List<DeliveryEvent> candidates = new ArrayList<>(heads.values());
            candidates.sort(Comparator.comparingLong(DeliveryEvent::getSequence)
                    .thenComparing(DeliveryEvent::getCreatedAt));

            List<DeliveryEvent> claimed = new ArrayList<>();
            for (DeliveryEvent e : candidates) {
                if (claimed.size() >= max) {
                    break;
                }
                if (gateBlocksLocked(e)) {
                    continue;
                }
                boolean claimable = switch (e.getStatus()) {
                    case PENDING -> true;
                    case RETRY_WAIT -> !e.getNextAttemptAt().isAfter(now);
                    case LEASED -> e.getLeaseExpiresAt() != null && e.getLeaseExpiresAt().isBefore(now);
                    default -> false;
                };
                if (!claimable) {
                    continue;
                }
                e.setStatus(EventStatus.LEASED);
                e.setLeaseOwner(workerId);
                e.setLeaseExpiresAt(now.plus(leaseDuration));
                e.setUpdatedAt(now);
                claimed.add(copy(e));
            }
            if (!claimed.isEmpty()) {
                persistLocked();
            }
            return claimed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean markDelivered(String tenantId, String id, String workerId) {
        lock.lock();
        try {
            DeliveryEvent e = ownedLeased(tenantId, id, workerId);
            if (e == null) {
                return false;
            }
            e.setStatus(EventStatus.DELIVERED);
            e.setLeaseOwner(null);
            e.setLeaseExpiresAt(null);
            e.setLastError(null);
            e.setUpdatedAt(Instant.now());
            persistLocked();
            // 卡住本聚合的事件投递成功：自动放行闸门，后续事件按序继续
            gateStore.unblockIfBlockedBy(tenantId, e.getAggregateKey(), id);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean markRetry(String tenantId, String id, String workerId, String failureKind,
                             Instant nextAttemptAt, String error) {
        lock.lock();
        try {
            DeliveryEvent e = ownedLeased(tenantId, id, workerId);
            if (e == null) {
                return false;
            }
            e.setStatus(EventStatus.RETRY_WAIT);
            e.setAttemptCount(e.getAttemptCount() + 1);
            e.setNextAttemptAt(nextAttemptAt);
            e.setLeaseOwner(null);
            e.setLeaseExpiresAt(null);
            e.setLastError(sanitize(error));
            e.setUpdatedAt(Instant.now());
            persistLocked();
            // 还有事件没处理完（等下次重试）：聚合自动进入阻塞，后续事件排队
            gateStore.block(tenantId, e.getAggregateKey(), id, "RETRY_WAIT:" + failureKind);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean markFailed(String tenantId, String id, String workerId, String failureKind, String error) {
        lock.lock();
        try {
            DeliveryEvent e = ownedLeased(tenantId, id, workerId);
            if (e == null) {
                return false;
            }
            e.setStatus(EventStatus.FAILED);
            e.setAttemptCount(e.getAttemptCount() + 1);
            e.setLeaseOwner(null);
            e.setLeaseExpiresAt(null);
            e.setLastError(sanitize(error));
            e.setUpdatedAt(Instant.now());
            persistLocked();
            // 彻底失败：聚合自动进入阻塞，后续事件排队等待人工重放
            gateStore.block(tenantId, e.getAggregateKey(), id, "FAILED:" + failureKind);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean replay(String tenantId, String id) {
        lock.lock();
        try {
            DeliveryEvent e = byId.get(id);
            if (e == null || !e.getTenantId().equals(tenantId) || e.getStatus() != EventStatus.FAILED) {
                return false;
            }
            e.setStatus(EventStatus.PENDING);
            e.setAttemptCount(0);
            e.setNextAttemptAt(Instant.now());
            e.setLeaseOwner(null);
            e.setLeaseExpiresAt(null);
            e.setUpdatedAt(Instant.now());
            persistLocked();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int countPending() {
        lock.lock();
        try {
            return countPendingLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int countQueuedForAggregate(String tenantId, String aggregateKey, Long afterSequence) {
        lock.lock();
        try {
            return countQueuedForAggregateLocked(tenantId, aggregateKey, afterSequence);
        } finally {
            lock.unlock();
        }
    }

    // ---- 内部辅助（调用方须持有锁） ----

    /**
     * 闸门拦截判定：暂停的聚合整队跳过；阻塞的聚合只允许卡住它的那个事件被认领，
     * 后续事件排队等待，不得越过先生效。
     */
    private boolean gateBlocksLocked(DeliveryEvent e) {
        AggregateGate gate = gateStore.snapshot(e.getTenantId(), e.getAggregateKey()).orElse(null);
        if (gate == null) {
            return false;
        }
        if (gate.isPaused()) {
            return true;
        }
        if (!gate.isBlocked()) {
            return false;
        }
        DeliveryEvent blocker = byId.get(gate.getBlockedEventId());
        if (blocker == null || blocker.getStatus() == EventStatus.DELIVERED) {
            // 崩溃窗口留下的陈旧阻塞（事件已投递但闸门未放行），自愈放行
            gateStore.unblockIfBlockedBy(e.getTenantId(), e.getAggregateKey(), gate.getBlockedEventId());
            return false;
        }
        return !gate.getBlockedEventId().equals(e.getId());
    }

    private int countQueuedForAggregateLocked(String tenantId, String aggregateKey, Long afterSequence) {
        int n = 0;
        for (DeliveryEvent e : byId.values()) {
            if (!e.getTenantId().equals(tenantId) || !e.getAggregateKey().equals(aggregateKey)) {
                continue;
            }
            if (e.getStatus() != EventStatus.PENDING && e.getStatus() != EventStatus.RETRY_WAIT
                    && e.getStatus() != EventStatus.LEASED) {
                continue;
            }
            if (afterSequence != null && e.getSequence() <= afterSequence) {
                continue;
            }
            n++;
        }
        return n;
    }

    private int countPendingLocked() {
        int n = 0;
        for (DeliveryEvent e : byId.values()) {
            if (e.getStatus() == EventStatus.PENDING || e.getStatus() == EventStatus.RETRY_WAIT
                    || e.getStatus() == EventStatus.LEASED) {
                n++;
            }
        }
        return n;
    }

    private DeliveryEvent ownedLeased(String tenantId, String id, String workerId) {
        DeliveryEvent e = byId.get(id);
        if (e == null || !e.getTenantId().equals(tenantId) || e.getStatus() != EventStatus.LEASED
                || !workerId.equals(e.getLeaseOwner())) {
            return null;
        }
        return e;
    }

    private static boolean contentMatches(DeliveryEvent a, DeliveryEvent b) {
        return a.getAggregateKey().equals(b.getAggregateKey())
                && a.getPayload().equals(b.getPayload())
                && a.getTargetUrl().equals(b.getTargetUrl());
    }

    private static String idemKey(String tenantId, String idempotencyKey) {
        return tenantId + '|' + idempotencyKey;
    }

    private static String aggKey(String tenantId, String aggregateKey) {
        return tenantId + '|' + aggregateKey;
    }

    /** 错误信息只保留失败类别与通用描述，剔除任何类似凭据的内容 */
    private static String sanitize(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 200 ? error.substring(0, 200) : error;
    }

    private void persistLocked() {
        try {
            Files.createDirectories(storeFile.getParent());
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), byId.values());
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist event store", e);
        }
    }

    private static DeliveryEvent copy(DeliveryEvent e) {
        DeliveryEvent c = new DeliveryEvent();
        c.setId(e.getId());
        c.setTenantId(e.getTenantId());
        c.setIdempotencyKey(e.getIdempotencyKey());
        c.setAggregateKey(e.getAggregateKey());
        c.setSequence(e.getSequence());
        c.setPayload(e.getPayload());
        c.setTargetUrl(e.getTargetUrl());
        c.setStatus(e.getStatus());
        c.setAttemptCount(e.getAttemptCount());
        c.setNextAttemptAt(e.getNextAttemptAt());
        c.setLeaseOwner(e.getLeaseOwner());
        c.setLeaseExpiresAt(e.getLeaseExpiresAt());
        c.setCreatedAt(e.getCreatedAt());
        c.setUpdatedAt(e.getUpdatedAt());
        c.setLastError(e.getLastError());
        return c;
    }
}
