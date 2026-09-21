package com.github.highcumontoa.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.EventStatus;
import com.github.highcumontoa.delivery.domain.FailureKind;
import com.github.highcumontoa.delivery.store.EventStore;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 重启恢复：进程重启后，已投递事件不重复投递；待投递/待重试事件从本地
 * 持久化状态恢复并继续调度；FAILED 终态保持可查询。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);
    private static final String DIR = "target/test-data/restart";

    private EventStore store;

    @BeforeEach
    void setUp() {
        new java.io.File(DIR).mkdirs();
        new java.io.File(DIR + "/store.json").delete();
        store = new EventStore(DIR + "/store.json");
        store.recover();
    }

    private DeliveryEvent newEvent(String key, String agg, long seq) {
        DeliveryEvent e = new DeliveryEvent();
        e.setEventId(key);
        e.setTenantId("t1");
        e.setIdempotencyKey(key);
        e.setAggregateKey(agg);
        e.setSequence(seq);
        e.setPayload("p");
        e.setContentHash("h-" + key);
        e.setStatus(EventStatus.PENDING);
        e.setNextAttemptAt(Instant.now());
        e.setCreatedAt(Instant.now());
        e.setUpdatedAt(Instant.now());
        return e;
    }

    @Test
    void deliveredEventsAreNotRedeliveredAfterRestart() {
        // 准备：一个 DELIVERED、一个 PENDING（等待重试，带退避时间）、一个 FAILED
        DeliveryEvent delivered = newEvent("rr-delivered", "agg-a", 1);
        delivered.setStatus(EventStatus.DELIVERED);
        store.insertIdempotent(delivered);

        DeliveryEvent retryPending = newEvent("rr-retry", "agg-b", 1);
        retryPending.setAttemptCount(2);
        retryPending.setLastFailureKind(FailureKind.TIMEOUT);
        retryPending.setNextAttemptAt(Instant.now().minusSeconds(1)); // 已到期
        store.insertIdempotent(retryPending);

        DeliveryEvent failed = newEvent("rr-failed", "agg-c", 1);
        failed.setStatus(EventStatus.FAILED);
        failed.setAttemptCount(5);
        failed.setLastFailureKind(FailureKind.SERVER_ERROR);
        store.insertIdempotent(failed);

        // 模拟重启
        EventStore restarted = new EventStore(DIR + "/store.json");
        restarted.recover();

        DeliveryEvent d = restarted.findById("t1", "rr-delivered").orElseThrow();
        DeliveryEvent r = restarted.findById("t1", "rr-retry").orElseThrow();
        DeliveryEvent f = restarted.findById("t1", "rr-failed").orElseThrow();
        log.info("判定依据: 重启后状态 delivered={} retry={} failed={} (期望 DELIVERED/PENDING/FAILED)",
                d.getStatus(), r.getStatus(), f.getStatus());
        assertEquals(EventStatus.DELIVERED, d.getStatus());
        assertEquals(EventStatus.PENDING, r.getStatus());
        assertEquals(EventStatus.FAILED, f.getStatus());
        assertEquals(2, r.getAttemptCount(), "重试计数应被恢复");

        // 到期重试事件可被认领继续投递；DELIVERED 与 FAILED 不会被认领
        List<DeliveryEvent> claimable = restarted.claimBatch("w", 10, Instant.now(), Instant.now().plusSeconds(5));
        log.info("判定依据: 重启后可认领事件={} (期望仅 rr-retry)",
                claimable.stream().map(DeliveryEvent::getEventId).toList());
        assertEquals(1, claimable.size());
        assertEquals("rr-retry", claimable.get(0).getEventId());

        // 幂等索引同样恢复：重复提交返回已有事件而不是新建
        DeliveryEvent dup = restarted.insertIdempotent(newEvent("rr-delivered", "agg-a", 99));
        assertEquals(EventStatus.DELIVERED, dup.getStatus(), "重启后重复提交仍命中幂等索引，不产生新投递");
        assertTrue(restarted.findByStatus("t1", EventStatus.FAILED).size() == 1, "FAILED 事件重启后可查询");
    }
}
