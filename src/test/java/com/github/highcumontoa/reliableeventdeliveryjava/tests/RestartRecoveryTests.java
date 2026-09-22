package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;
import com.github.highcumontoa.reliableeventdeliveryjava.store.FileEventStore;
import com.github.highcumontoa.reliableeventdeliveryjava.store.SubmitStatus;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 重启恢复：进程重启后未完成/待重试事件从本地持久化恢复，已 DELIVERED 不重复投递，LEASED 经租约到期回收 */
class RestartRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTests.class);

    @TempDir
    Path dir;

    private FileEventStore openStore() throws Exception {
        DeliveryProperties props = new DeliveryProperties();
        props.setStorageDir(dir.toString());
        FileEventStore store = new FileEventStore(props);
        Method load = FileEventStore.class.getDeclaredMethod("load");
        load.setAccessible(true);
        load.invoke(store);
        return store;
    }

    private static DeliveryEvent event(String tenant, String idem, String agg) {
        DeliveryEvent e = new DeliveryEvent();
        e.setTenantId(tenant);
        e.setIdempotencyKey(idem);
        e.setAggregateKey(agg);
        e.setPayload("p-" + idem);
        e.setTargetUrl("http://127.0.0.1/receiver/ok");
        return e;
    }

    @Test
    void stateSurvivesRestartWithoutRedelivery() throws Exception {
        FileEventStore store1 = openStore();
        String deliveredId = store1.submit(event("t1", "done", "agg-a"), 100).event().getId();
        String retryId = store1.submit(event("t1", "retry", "agg-b"), 100).event().getId();
        String leasedId = store1.submit(event("t1", "leased", "agg-c"), 100).event().getId();

        // done -> DELIVERED；retry -> RETRY_WAIT（已到期）；leased -> LEASED（短租约，模拟崩溃时未完成）
        store1.claimDeliverable("w1", 10, Duration.ofMillis(1));
        store1.markDelivered("t1", deliveredId, "w1");
        store1.markRetry("t1", retryId, "w1", "SERVER_ERROR", Instant.now().minusMillis(1), "receiver status 500");
        Thread.sleep(20);

        // 模拟进程重启：同一目录重新打开
        FileEventStore store2 = openStore();

        assertThat(store2.findById("t1", deliveredId).orElseThrow().getStatus().name()).isEqualTo("DELIVERED");
        assertThat(store2.findById("t1", retryId).orElseThrow().getStatus().name()).isEqualTo("RETRY_WAIT");
        log.info("assertion basis: after restart delivered={} retry={}",
                store2.findById("t1", deliveredId).orElseThrow().getStatus(),
                store2.findById("t1", retryId).orElseThrow().getStatus());

        // 恢复后认领：DELIVERED 不得再被认领；RETRY_WAIT 到期与 LEASED 租约过期应被认领
        List<DeliveryEvent> claimed = store2.claimDeliverable("w2", 10, Duration.ofSeconds(5));
        List<String> ids = claimed.stream().map(DeliveryEvent::getId).sorted().toList();
        log.info("assertion basis: reclaimed after restart ids={}", ids);
        assertThat(ids).containsExactlyInAnyOrder(retryId, leasedId);
        assertThat(ids).doesNotContain(deliveredId);

        // 幂等索引同样恢复：同键同内容仍是 DUPLICATE，不重复投递
        var dup = store2.submit(event("t1", "done", "agg-a"), 100);
        assertThat(dup.status()).isEqualTo(SubmitStatus.DUPLICATE);
        var conflict = store2.submit(event("t1", "done", "agg-DIFFERENT"), 100);
        assertThat(conflict.status()).isEqualTo(SubmitStatus.CONFLICT);
        log.info("assertion basis: idempotency index restored, duplicate={} conflict={}",
                dup.status(), conflict.status());
    }
}
