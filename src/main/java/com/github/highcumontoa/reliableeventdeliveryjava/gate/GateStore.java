package com.github.highcumontoa.reliableeventdeliveryjava.gate;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 聚合闸门存储：内存索引 + 状态变更原子落盘（gates.json，临时文件 + move），重启后恢复。
 * 所有方法线程安全。不持有事件存储的引用，调用方负责避免锁环（本类绝不回调事件存储）。
 */
@Component
public class GateStore {

    private static final Logger log = LoggerFactory.getLogger(GateStore.class);

    private final Path storeFile;
    private final ObjectMapper mapper;
    private final ReentrantLock lock = new ReentrantLock();

    /** (tenant|aggKey) -> gate */
    private final Map<String, AggregateGate> gates = new LinkedHashMap<>();

    public GateStore(DeliveryProperties properties) {
        this.storeFile = Path.of(properties.getStorageDir(), "gates.json");
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @PostConstruct
    public void load() {
        lock.lock();
        try {
            if (!Files.exists(storeFile)) {
                return;
            }
            AggregateGate[] loaded = mapper.readValue(storeFile.toFile(), AggregateGate[].class);
            for (AggregateGate g : loaded) {
                gates.put(key(g.getTenantId(), g.getAggregateKey()), g);
            }
            log.info("gate store loaded file={} gates={}", storeFile, gates.size());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load gate store " + storeFile, e);
        } finally {
            lock.unlock();
        }
    }

    /** 人工暂停指定聚合的投递（幂等）。暂停期间新事件照常接收，但一件都不投递。 */
    public AggregateGate pause(String tenantId, String aggregateKey) {
        lock.lock();
        try {
            AggregateGate g = getOrCreateLocked(tenantId, aggregateKey);
            g.setPaused(true);
            g.setUpdatedAt(Instant.now());
            persistLocked();
            return copy(g);
        } finally {
            lock.unlock();
        }
    }

    /** 恢复指定聚合的投递：只解除人工暂停，不影响自动阻塞。 */
    public AggregateGate resume(String tenantId, String aggregateKey) {
        lock.lock();
        try {
            AggregateGate g = getOrCreateLocked(tenantId, aggregateKey);
            g.setPaused(false);
            g.setUpdatedAt(Instant.now());
            persistLocked();
            return copy(g);
        } finally {
            lock.unlock();
        }
    }

    /** 查询闸门快照；无记录表示 OPEN（empty）。 */
    public Optional<AggregateGate> snapshot(String tenantId, String aggregateKey) {
        lock.lock();
        try {
            AggregateGate g = gates.get(key(tenantId, aggregateKey));
            return g == null ? Optional.empty() : Optional.of(copy(g));
        } finally {
            lock.unlock();
        }
    }

    /** 自动阻塞：事件进入重试等待或彻底失败时卡住整个聚合（后续事件排队，不得越过）。 */
    public void block(String tenantId, String aggregateKey, String eventId, String reason) {
        lock.lock();
        try {
            AggregateGate g = getOrCreateLocked(tenantId, aggregateKey);
            g.setBlockedEventId(eventId);
            g.setBlockReason(reason);
            g.setUpdatedAt(Instant.now());
            persistLocked();
        } finally {
            lock.unlock();
        }
    }

    /** 卡住的事件投递成功后自动放行；其它事件不触发放行。 */
    public void unblockIfBlockedBy(String tenantId, String aggregateKey, String eventId) {
        lock.lock();
        try {
            AggregateGate g = gates.get(key(tenantId, aggregateKey));
            if (g == null || !eventId.equals(g.getBlockedEventId())) {
                return;
            }
            g.setBlockedEventId(null);
            g.setBlockReason(null);
            g.setUpdatedAt(Instant.now());
            persistLocked();
        } finally {
            lock.unlock();
        }
    }

    /** 列出租户下所有非 OPEN 的闸门（暂停或阻塞），按聚合键排序保证输出稳定。 */
    public List<AggregateGate> listNonOpen(String tenantId) {
        lock.lock();
        try {
            List<AggregateGate> out = new ArrayList<>();
            for (AggregateGate g : gates.values()) {
                if (g.getTenantId().equals(tenantId) && (g.isPaused() || g.isBlocked())) {
                    out.add(copy(g));
                }
            }
            out.sort((a, b) -> a.getAggregateKey().compareTo(b.getAggregateKey()));
            return out;
        } finally {
            lock.unlock();
        }
    }

    // ---- 内部辅助（调用方须持有锁） ----

    private AggregateGate getOrCreateLocked(String tenantId, String aggregateKey) {
        return gates.computeIfAbsent(key(tenantId, aggregateKey), k -> {
            AggregateGate g = new AggregateGate();
            g.setTenantId(tenantId);
            g.setAggregateKey(aggregateKey);
            return g;
        });
    }

    private void persistLocked() {
        try {
            Files.createDirectories(storeFile.getParent());
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), gates.values());
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist gate store", e);
        }
    }

    private static String key(String tenantId, String aggregateKey) {
        return tenantId + '|' + aggregateKey;
    }

    private static AggregateGate copy(AggregateGate g) {
        AggregateGate c = new AggregateGate();
        c.setTenantId(g.getTenantId());
        c.setAggregateKey(g.getAggregateKey());
        c.setPaused(g.isPaused());
        c.setBlockedEventId(g.getBlockedEventId());
        c.setBlockReason(g.getBlockReason());
        c.setUpdatedAt(g.getUpdatedAt());
        return c;
    }
}
