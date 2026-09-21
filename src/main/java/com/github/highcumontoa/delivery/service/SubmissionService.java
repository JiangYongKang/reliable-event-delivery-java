package com.github.highcumontoa.delivery.service;

import com.github.highcumontoa.delivery.config.DeliveryProperties;
import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.EventStatus;
import com.github.highcumontoa.delivery.store.EventStore;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** 事件提交：幂等去重、冲突拒绝、积压控制、序号分配。 */
@Service
public class SubmissionService {

    private static final Logger log = LoggerFactory.getLogger(SubmissionService.class);

    private final EventStore store;
    private final DeliveryProperties properties;

    public SubmissionService(EventStore store, DeliveryProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * 提交事件。
     *
     * <p>重复提交（同租户同幂等键同内容）返回已有事件，不产生重复投递；
     * 同键不同内容抛 {@link SubmissionException.Reason#IDEMPOTENCY_CONFLICT}；
     * 积压超限抛 {@link SubmissionException.Reason#BACKPRESSURE_LIMIT}。
     */
    public DeliveryEvent submit(String tenantId, String idempotencyKey, String aggregateKey, String payload) {
        DeliveryEvent event = new DeliveryEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setTenantId(tenantId);
        event.setIdempotencyKey(idempotencyKey);
        event.setAggregateKey(aggregateKey);
        event.setPayload(payload);
        event.setContentHash(contentHash(tenantId, aggregateKey, payload));
        event.setStatus(EventStatus.PENDING);
        event.setAttemptCount(0);
        event.setNextAttemptAt(Instant.now());
        event.setCreatedAt(Instant.now());
        event.setUpdatedAt(Instant.now());

        // 序号与插入需原子，避免并发下同聚合序号与落库顺序不一致
        synchronized (this) {
            if (store.countPending() >= properties.getMaxPending()) {
                log.info("submission rejected: backlog limit reached, tenant={} idempotencyKey={}",
                        tenantId, idempotencyKey);
                throw new SubmissionException(SubmissionException.Reason.BACKPRESSURE_LIMIT,
                        "pending backlog limit reached");
            }
            event.setSequence(store.nextSequence(tenantId, aggregateKey));
            DeliveryEvent stored = store.insertIdempotent(event);
            if (stored != event) {
                log.info("duplicate submission ignored: tenant={} idempotencyKey={} existingEventId={}",
                        tenantId, idempotencyKey, stored.getEventId());
            } else {
                log.info("event accepted: tenant={} eventId={} aggregateKey={} seq={}",
                        tenantId, event.getEventId(), aggregateKey, event.getSequence());
            }
            return stored;
        }
    }

    /** 内容指纹：仅用于冲突检测，不含任何凭据。 */
    private static String contentHash(String tenantId, String aggregateKey, String payload) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(tenantId.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0);
            md.update(aggregateKey.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0);
            md.update(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
