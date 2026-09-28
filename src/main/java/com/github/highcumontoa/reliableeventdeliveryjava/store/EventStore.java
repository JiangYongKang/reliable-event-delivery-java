package com.github.highcumontoa.reliableeventdeliveryjava.store;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 事件本地持久化存储。所有方法需线程安全；状态变更需落盘以便重启恢复。 */
public interface EventStore {

    /** 幂等提交（容量按 {@link SubmitOptions} 约束） */
    default SubmitResult submit(DeliveryEvent event, SubmitOptions options) {
        throw new UnsupportedOperationException();
    }

    /** 幂等提交：同租户同幂等键内容一致返回 DUPLICATE，冲突返回 CONFLICT，超限返回 OVERLOAD */
    default SubmitResult submit(DeliveryEvent event, int maxPending) {
        throw new UnsupportedOperationException();
    }

    Optional<DeliveryEvent> findById(String tenantId, String id);

    List<DeliveryEvent> listByTenant(String tenantId);

    /**
     * 原子认领一批可投递事件：PENDING 或到期的 RETRY_WAIT，或租约已过期的 LEASED（租约回收）。
     * 同一聚合键只允许顺序最前的未完成事件被认领；FAILED 队首（除非被重放）与人工暂停的聚合
     * 都不放行，从而保证顺序并实现“阻塞聚合后续事件排队”。
     */
    List<DeliveryEvent> claimDeliverable(String workerId, int max, Duration leaseDuration);

    /** 仅当租约仍属 workerId 时生效 */
    boolean markDelivered(String tenantId, String id, String workerId);

    /** 暂时性失败：进入 RETRY_WAIT 并设置下次重试时点 */
    boolean markRetry(String tenantId, String id, String workerId, String failureKind, Instant nextAttemptAt, String error);

    /** 终态失败：进入 FAILED，可查询、可重放 */
    boolean markFailed(String tenantId, String id, String workerId, String failureKind, String error);

    /** 显式重放 FAILED 事件：重置为 PENDING */
    boolean replay(String tenantId, String id);

    int countPending();

    // ---- 聚合闸门 ----

    /** 暂停某聚合：已在投递的事件继续完成，之后该聚合不再被认领；返回 false 表示已处于暂停 */
    default boolean pauseGate(String tenantId, String aggregateKey) {
        throw new UnsupportedOperationException();
    }

    /** 恢复某聚合：从暂停位置按原顺序继续投递；返回 false 表示此前并未暂停 */
    default boolean resumeGate(String tenantId, String aggregateKey) {
        throw new UnsupportedOperationException();
    }

    /** 查询单个聚合的闸门状态；无事件也未暂停过的聚合返回 empty */
    default Optional<GateView> findGate(String tenantId, String aggregateKey, int backlogLimit) {
        throw new UnsupportedOperationException();
    }

    /** 列出租户内所有存在排队事件或被暂停的聚合闸门，可按状态过滤（state 为 null 表示全部） */
    default List<GateView> listGates(String tenantId, String state, int backlogLimit) {
        throw new UnsupportedOperationException();
    }
}
