package com.github.highcumontoa.reliableeventdeliveryjava.store;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 事件本地持久化存储。所有方法需线程安全；状态变更需落盘以便重启恢复。 */
public interface EventStore {

    /** 幂等提交：同租户同幂等键内容一致返回 DUPLICATE，冲突返回 CONFLICT，超限返回 OVERLOAD */
    SubmitResult submit(DeliveryEvent event, int maxPending);

    Optional<DeliveryEvent> findById(String tenantId, String id);

    List<DeliveryEvent> listByTenant(String tenantId);

    /**
     * 原子认领一批可投递事件：PENDING 或到期的 RETRY_WAIT，或租约已过期的 LEASED（租约回收）。
     * 同一聚合键只允许顺序最前的未投递事件被认领，保证顺序。
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

    /**
     * 统计某聚合内排队中的事件数（PENDING/RETRY_WAIT/LEASED）。
     * afterSequence 非 null 时只统计序号在其之后的事件（即“卡在 blocked 事件后面还排了多少件”）。
     */
    int countQueuedForAggregate(String tenantId, String aggregateKey, Long afterSequence);
}
