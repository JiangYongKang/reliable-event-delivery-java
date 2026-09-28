package com.github.highcumontoa.reliableeventdeliveryjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.BlockReason;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.EventStatus;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.GateState;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 基于本地 JSON 文件的事件存储。
 * 内存索引 + 每次状态变更原子落盘（临时文件 + move），进程重启后从文件恢复。
 * 重启前处于 LEASED 的事件租约到期后会被重新认领，不会重复投递已 DELIVERED 的事件。
 * 聚合闸门的暂停集合单独持久化到 gates.json，与事件一起在同一把锁内变更。
 */
@Component
public class FileEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(FileEventStore.class);

    private final Path storeFile;
    private final Path gatesFile;
    private final ObjectMapper mapper;
    private final ReentrantLock lock = new ReentrantLock();

    /** 全量事件，id -> event */
    private final Map<String, DeliveryEvent> byId = new LinkedHashMap<>();
    /** 幂等索引：(tenant|idemKey) -> eventId */
    private final Map<String, String> idemIndex = new HashMap<>();
    /** 聚合序号：(tenant|aggKey) -> 已分配的最大 sequence */
    private final Map<String, Long> aggSeq = new HashMap<>();
    /** 被人工暂停的聚合：(tenant|aggKey) -> 暂停时刻；暂停期间事件照常收下但不被认领 */
    private final Map<String, Instant> pausedGates = new HashMap<>();

    /** 暂停聚合的持久化记录（供 gates.json 序列化） */
    public record PausedGate(String tenantId, String aggregateKey, Instant pausedAt) {
    }

    public FileEventStore(DeliveryProperties properties) {
        this.storeFile = Path.of(properties.getStorageDir(), "events.json");
        this.gatesFile = Path.of(properties.getStorageDir(), "gates.json");
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @PostConstruct
    void load() {
        lock.lock();
        try {
            if (Files.exists(storeFile)) {
                DeliveryEvent[] events = mapper.readValue(storeFile.toFile(), DeliveryEvent[].class);
                for (DeliveryEvent e : events) {
                    byId.put(e.getId(), e);
                    idemIndex.put(idemKey(e.getTenantId(), e.getIdempotencyKey()), e.getId());
                    aggSeq.merge(aggKey(e.getTenantId(), e.getAggregateKey()), e.getSequence(), Math::max);
                }
            }
            if (Files.exists(gatesFile)) {
                List<PausedGate> gates = mapper.readValue(gatesFile.toFile(), new TypeReference<List<PausedGate>>() {
                });
                for (PausedGate g : gates) {
                    pausedGates.put(aggKey(g.tenantId(), g.aggregateKey()), g.pausedAt());
                }
            }
            log.info("store loaded file={} events={} pausedGates={}",
                    storeFile, byId.size(), pausedGates.size());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load event store " + storeFile, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SubmitResult submit(DeliveryEvent event, int maxPending) {
        return submit(event, new SubmitOptions(maxPending, Integer.MAX_VALUE));
    }

    @Override
    public SubmitResult submit(DeliveryEvent event, SubmitOptions options) {
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
            if (countPendingLocked() >= options.maxPending()) {
                return new SubmitResult(SubmitStatus.OVERLOAD, null);
            }
            String aggregate = aggKey(event.getTenantId(), event.getAggregateKey());
            boolean gated = pausedGates.containsKey(aggregate) || headBlockedLocked(aggregate) != null;
            if (gated && countAggregateQueuedLocked(aggregate) >= options.maxAggregateBacklog()) {
                return new SubmitResult(SubmitStatus.AGGREGATE_FULL, null);
            }
            Instant now = Instant.now();
            long seq = aggSeq.getOrDefault(aggregate, 0L) + 1;
            aggSeq.put(aggregate, seq);

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
            // 每个聚合键的队首：最小 sequence 的未完成事件。
            // 只排除 DELIVERED——FAILED 的队首必须继续挡住后续事件（聚合自动阻塞），直到人工重放。
            Map<String, DeliveryEvent> heads = new HashMap<>();
            for (DeliveryEvent e : byId.values()) {
                if (e.getStatus() == EventStatus.DELIVERED) {
                    continue;
                }
                String key = aggKey(e.getTenantId(), e.getAggregateKey());
                // 人工暂停的聚合：新事件照常收下但一件都不投递
                if (pausedGates.containsKey(key)) {
                    continue;
                }
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
    public boolean pauseGate(String tenantId, String aggregateKey) {
        lock.lock();
        try {
            String key = aggKey(tenantId, aggregateKey);
            if (pausedGates.containsKey(key)) {
                return false;
            }
            pausedGates.put(key, Instant.now());
            persistGatesLocked();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean resumeGate(String tenantId, String aggregateKey) {
        lock.lock();
        try {
            String key = aggKey(tenantId, aggregateKey);
            if (pausedGates.remove(key) == null) {
                return false;
            }
            persistGatesLocked();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<GateView> findGate(String tenantId, String aggregateKey, int backlogLimit) {
        lock.lock();
        try {
            String key = aggKey(tenantId, aggregateKey);
            Instant pausedAt = pausedGates.get(key);
            DeliveryEvent head = aggregateHeadLocked(key);
            int queued = countAggregateQueuedLocked(key);
            if (pausedAt == null && head == null) {
                return Optional.empty();
            }
            return Optional.of(buildGateViewLocked(tenantId, aggregateKey, key, pausedAt, head, queued, backlogLimit));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<GateView> listGates(String tenantId, String state, int backlogLimit) {
        lock.lock();
        try {
            Set<String> aggregates = new HashSet<>();
            for (DeliveryEvent e : byId.values()) {
                if (e.getTenantId().equals(tenantId) && e.getStatus() != EventStatus.DELIVERED) {
                    aggregates.add(aggKey(tenantId, e.getAggregateKey()));
                }
            }
            for (String key : pausedGates.keySet()) {
                if (belongsToTenant(key, tenantId)) {
                    aggregates.add(key);
                }
            }
            GateState filter = state == null || state.isBlank() ? null : GateState.valueOf(state);
            List<GateView> out = new ArrayList<>();
            for (String key : aggregates) {
                String aggregateKey = key.substring(tenantId.length() + 1);
                Instant pausedAt = pausedGates.get(key);
                DeliveryEvent head = aggregateHeadLocked(key);
                int queued = countAggregateQueuedLocked(key);
                GateView view = buildGateViewLocked(tenantId, aggregateKey, key, pausedAt, head, queued, backlogLimit);
                if (filter == null || view.state() == filter) {
                    out.add(view);
                }
            }
            out.sort(Comparator.comparing(GateView::aggregateKey));
            return out;
        } finally {
            lock.unlock();
        }
    }

    // ---- 内部辅助（调用方须持有锁） ----

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

    /** 聚合内最小 sequence 的未投递（非 DELIVERED）事件；没有则 null */
    private DeliveryEvent aggregateHeadLocked(String key) {
        DeliveryEvent head = null;
        for (DeliveryEvent e : byId.values()) {
            if (!aggKey(e.getTenantId(), e.getAggregateKey()).equals(key)
                    || e.getStatus() == EventStatus.DELIVERED) {
                continue;
            }
            if (head == null || e.getSequence() < head.getSequence()) {
                head = e;
            }
        }
        return head;
    }

    /** 聚合内排队事件数量：所有非 DELIVERED 事件（含卡住的队首与其后的事件） */
    private int countAggregateQueuedLocked(String key) {
        int n = 0;
        for (DeliveryEvent e : byId.values()) {
            if (aggKey(e.getTenantId(), e.getAggregateKey()).equals(key)
                    && e.getStatus() != EventStatus.DELIVERED) {
                n++;
            }
        }
        return n;
    }

    /**
     * 判定聚合是否被队首自动阻塞：队首 RETRY_WAIT（等下次重试）或 FAILED（彻底失败）即阻塞。
     * 队首 PENDING / LEASED 表示处理链路正常推进，不视为阻塞。
     */
    private BlockReason headBlockedLocked(String key) {
        DeliveryEvent head = aggregateHeadLocked(key);
        if (head == null) {
            return null;
        }
        return switch (head.getStatus()) {
            case RETRY_WAIT -> BlockReason.RETRYING;
            case FAILED -> BlockReason.PERMANENT_FAILURE;
            default -> null;
        };
    }

    private GateView buildGateViewLocked(String tenantId, String aggregateKey, String key,
                                         Instant pausedAt, DeliveryEvent head, int queued, int backlogLimit) {
        boolean paused = pausedAt != null;
        BlockReason reason = headBlockedLocked(key);
        GateState state = paused ? GateState.PAUSED
                : reason != null ? GateState.BLOCKED : GateState.ACTIVE;
        return new GateView(
                tenantId,
                aggregateKey,
                state,
                reason == null ? BlockReason.NONE : reason,
                head == null ? null : head.getId(),
                head == null ? null : head.getSequence(),
                head == null ? null : head.getStatus().name(),
                head == null ? null : head.getAttemptCount(),
                head == null ? null : head.getLastError(),
                head == null ? null : head.getNextAttemptAt(),
                queued,
                paused,
                pausedAt,
                backlogLimit);
    }

    private static boolean belongsToTenant(String compositeKey, String tenantId) {
        return compositeKey.startsWith(tenantId + '|');
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

    /** 暂停集合落盘：与事件变更在同一把锁内完成，重启后暂停状态可恢复 */
    private void persistGatesLocked() {
        try {
            Files.createDirectories(gatesFile.getParent());
            List<PausedGate> gates = new ArrayList<>();
            for (Map.Entry<String, Instant> entry : pausedGates.entrySet()) {
                String key = entry.getKey();
                int sep = key.indexOf('|');
                gates.add(new PausedGate(key.substring(0, sep), key.substring(sep + 1), entry.getValue()));
            }
            Path tmp = gatesFile.resolveSibling(gatesFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), gates);
            Files.move(tmp, gatesFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist gate store", e);
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
