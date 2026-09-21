package com.github.highcumontoa.delivery.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.EventStatus;
import com.github.highcumontoa.delivery.service.SubmissionException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地持久化事件存储。
 *
 * <p>线程安全：所有公共方法 synchronized。持久化：每次变更将快照原子写入
 * （临时文件 + rename），进程重启后由 {@link #recover()} 恢复；
 * 恢复时残留的 IN_FLIGHT 一律回收为 PENDING（租约随进程死亡失效）。
 */
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final Path storePath;
    private final ObjectMapper mapper;

    /** eventId -> event */
    private final Map<String, DeliveryEvent> events = new LinkedHashMap<>();
    /** tenantId + SEP + idempotencyKey -> eventId */
    private final Map<String, String> idempotencyIndex = new HashMap<>();
    /** tenantId + SEP + aggregateKey -> 已分配的最大序号 */
    private final Map<String, Long> sequences = new HashMap<>();

    public EventStore(String storePath) {
        this.storePath = Path.of(storePath);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /** 启动恢复：加载快照，残留 IN_FLIGHT 回收为 PENDING。 */
    public synchronized void recover() {
        events.clear();
        idempotencyIndex.clear();
        sequences.clear();
        if (Files.exists(storePath)) {
            try {
                Snapshot snapshot = mapper.readValue(storePath.toFile(), Snapshot.class);
                if (snapshot.events != null) {
                    for (DeliveryEvent e : snapshot.events) {
                        events.put(e.getEventId(), e);
                        idempotencyIndex.put(idemKey(e.getTenantId(), e.getIdempotencyKey()), e.getEventId());
                    }
                }
                if (snapshot.sequences != null) {
                    sequences.putAll(snapshot.sequences);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("无法加载事件存储: " + storePath, e);
            }
        }
        int reclaimed = 0;
        for (DeliveryEvent e : events.values()) {
            if (e.getStatus() == EventStatus.IN_FLIGHT) {
                e.setStatus(EventStatus.PENDING);
                e.setLeaseOwner(null);
                e.setLeaseExpiresAt(null);
                e.setNextAttemptAt(Instant.now());
                reclaimed++;
            }
        }
        if (reclaimed > 0) {
            persist();
            log.info("store recovered: {} in-flight events reclaimed to PENDING", reclaimed);
        }
        log.info("store recovered: {} events loaded from {}", events.size(), storePath);
    }

    /**
     * 幂等插入：同租户同幂等键已存在时，内容指纹一致返回已有事件（不产生重复投递），
     * 不一致抛 IDEMPOTENCY_CONFLICT。
     */
    public synchronized DeliveryEvent insertIdempotent(DeliveryEvent event) {
        String existingId = idempotencyIndex.get(idemKey(event.getTenantId(), event.getIdempotencyKey()));
        if (existingId != null) {
            DeliveryEvent existing = events.get(existingId);
            if (existing.getContentHash().equals(event.getContentHash())) {
                return existing;
            }
            throw new SubmissionException(SubmissionException.Reason.IDEMPOTENCY_CONFLICT,
                    "idempotency key already used with different content");
        }
        events.put(event.getEventId(), event);
        idempotencyIndex.put(idemKey(event.getTenantId(), event.getIdempotencyKey()), event.getEventId());
        persist();
        return event;
    }

    public synchronized Optional<DeliveryEvent> findById(String tenantId, String eventId) {
        DeliveryEvent e = events.get(eventId);
        if (e == null || !e.getTenantId().equals(tenantId)) {
            return Optional.empty();
        }
        return Optional.of(copy(e));
    }

    public synchronized List<DeliveryEvent> findByStatus(String tenantId, EventStatus status) {
        List<DeliveryEvent> result = new ArrayList<>();
        for (DeliveryEvent e : events.values()) {
            if (e.getTenantId().equals(tenantId) && e.getStatus() == status) {
                result.add(copy(e));
            }
        }
        result.sort(Comparator.comparing(DeliveryEvent::getCreatedAt));
        return result;
    }

    /**
     * 认领一批可投递事件并授予租约。
     *
     * <p>顺序保证：同一 (tenant, aggregateKey) 只认领其队首事件——
     * 若该聚合存在未过期的 IN_FLIGHT、或存在阻塞的 FAILED 事件，则整组跳过；
     * 租约已过期的 IN_FLIGHT 视为可重新认领（执行单元异常退出的回收路径）。
     */
    public synchronized List<DeliveryEvent> claimBatch(String workerId, int max, Instant now, Instant leaseExpiresAt) {
        // 聚合 -> 队首未完成事件（按序号最小）
        Map<String, DeliveryEvent> heads = new LinkedHashMap<>();
        for (DeliveryEvent e : events.values()) {
            if (e.getStatus() == EventStatus.DELIVERED) {
                continue;
            }
            String agg = aggKey(e.getTenantId(), e.getAggregateKey());
            DeliveryEvent head = heads.get(agg);
            if (head == null || e.getSequence() < head.getSequence()) {
                heads.put(agg, e);
            }
        }
        List<DeliveryEvent> claimed = new ArrayList<>();
        for (DeliveryEvent head : heads.values()) {
            if (claimed.size() >= max) {
                break;
            }
            boolean claimable;
            if (head.getStatus() == EventStatus.PENDING) {
                claimable = head.getNextAttemptAt() == null || !head.getNextAttemptAt().isAfter(now);
            } else if (head.getStatus() == EventStatus.IN_FLIGHT) {
                // 租约回收：仅当租约已过期
                claimable = head.getLeaseExpiresAt() != null && head.getLeaseExpiresAt().isBefore(now);
            } else {
                claimable = false; // FAILED 阻塞该聚合，等待显式重放
            }
            if (claimable) {
                head.setStatus(EventStatus.IN_FLIGHT);
                head.setLeaseOwner(workerId);
                head.setLeaseExpiresAt(leaseExpiresAt);
                head.setUpdatedAt(now);
                claimed.add(copy(head));
            }
        }
        if (!claimed.isEmpty()) {
            persist();
        }
        return claimed;
    }

    /** 仅当租约持有者匹配且事件仍在 IN_FLIGHT 时应用变更并落盘。 */
    public synchronized boolean updateIfLeaseHeld(DeliveryEvent updated, String workerId) {
        DeliveryEvent current = events.get(updated.getEventId());
        if (current == null
                || current.getStatus() != EventStatus.IN_FLIGHT
                || !workerId.equals(current.getLeaseOwner())) {
            return false;
        }
        updated.setUpdatedAt(Instant.now());
        events.put(updated.getEventId(), updated);
        persist();
        return true;
    }

    /** 重放：FAILED -> PENDING，重置尝试计数与失败信息。 */
    public synchronized boolean replay(String tenantId, String eventId) {
        DeliveryEvent e = events.get(eventId);
        if (e == null || !e.getTenantId().equals(tenantId) || e.getStatus() != EventStatus.FAILED) {
            return false;
        }
        e.setStatus(EventStatus.PENDING);
        e.setAttemptCount(0);
        e.setNextAttemptAt(Instant.now());
        e.setLeaseOwner(null);
        e.setLeaseExpiresAt(null);
        e.setLastFailureKind(null);
        e.setLastError(null);
        e.setUpdatedAt(Instant.now());
        persist();
        return true;
    }

    /** 分配同一 (tenant, aggregateKey) 内单调递增的序号。 */
    public synchronized long nextSequence(String tenantId, String aggregateKey) {
        return sequences.merge(aggKey(tenantId, aggregateKey), 1L, Long::sum);
    }

    public synchronized int countPending() {
        int n = 0;
        for (DeliveryEvent e : events.values()) {
            if (e.getStatus() == EventStatus.PENDING || e.getStatus() == EventStatus.IN_FLIGHT) {
                n++;
            }
        }
        return n;
    }

    private void persist() {
        try {
            if (storePath.getParent() != null) {
                Files.createDirectories(storePath.getParent());
            }
            Snapshot snapshot = new Snapshot();
            snapshot.events = new ArrayList<>(events.values());
            snapshot.sequences = new HashMap<>(sequences);
            Path tmp = storePath.resolveSibling(storePath.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, storePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("事件存储落盘失败: " + storePath, e);
        }
    }

    private static String idemKey(String tenantId, String idempotencyKey) {
        return tenantId + ' ' + idempotencyKey;
    }

    private static String aggKey(String tenantId, String aggregateKey) {
        return tenantId + ' ' + aggregateKey;
    }

    private static DeliveryEvent copy(DeliveryEvent e) {
        DeliveryEvent c = new DeliveryEvent();
        c.setEventId(e.getEventId());
        c.setTenantId(e.getTenantId());
        c.setIdempotencyKey(e.getIdempotencyKey());
        c.setAggregateKey(e.getAggregateKey());
        c.setSequence(e.getSequence());
        c.setPayload(e.getPayload());
        c.setContentHash(e.getContentHash());
        c.setStatus(e.getStatus());
        c.setAttemptCount(e.getAttemptCount());
        c.setNextAttemptAt(e.getNextAttemptAt());
        c.setLeaseOwner(e.getLeaseOwner());
        c.setLeaseExpiresAt(e.getLeaseExpiresAt());
        c.setLastFailureKind(e.getLastFailureKind());
        c.setLastError(e.getLastError());
        c.setCreatedAt(e.getCreatedAt());
        c.setUpdatedAt(e.getUpdatedAt());
        return c;
    }

    /** 持久化快照结构。 */
    public static class Snapshot {
        public List<DeliveryEvent> events;
        public Map<String, Long> sequences;
    }
}
