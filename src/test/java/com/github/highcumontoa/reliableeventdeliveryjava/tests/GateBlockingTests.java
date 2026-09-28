package com.github.highcumontoa.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateView;
import com.github.highcumontoa.reliableeventdeliveryjava.receiver.LoopbackReceiverController.Received;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/** 自动阻塞：聚合内有事件等重试或彻底失败时，后续事件排队不得越过；卡住原因/事件/排队数可查 */
class GateBlockingTests extends BaseIntegrationTest {

    @Test
    void failedHeadBlocksFollowersUntilReplayedAndDelivered() {
        String agg = "agg-blk-" + UUID.randomUUID();
        // switchable 默认拒绝：e1 永久性失败 -> FAILED，聚合自动阻塞
        String e1 = submit(TOKEN_A, "idem-" + agg + "-1", agg, "first", loopbackUrl("switchable"))
                .getBody().eventId();
        submit(TOKEN_A, "idem-" + agg + "-2", agg, "second", loopbackUrl("ok"));
        submit(TOKEN_A, "idem-" + agg + "-3", agg, "third", loopbackUrl("ok"));

        awaitStatus(TOKEN_A, e1, "FAILED", Duration.ofSeconds(10));

        // 阻塞期间：e2/e3 一件都不许投递
        List<Received> got = awaitReceived(agg, 1, Duration.ofMillis(600));
        log.info("assertion basis: while blocked deliveries={}", got.size());
        assertThat(got).as("followers must queue behind the failed head").isEmpty();

        // 可查：卡在哪个事件、因为什么、后面排了多少件
        GateView view = getGate(TOKEN_A, "tenant-a", agg).getBody();
        log.info("assertion basis: blocked gate state={} reason={} blockedEvent={} queued={}",
                view.state(), view.blockReason(), view.blockedEventId(), view.queuedCount());
        assertThat(view.state()).isEqualTo("BLOCKED");
        assertThat(view.blockedEventId()).isEqualTo(e1);
        assertThat(view.blockReason()).startsWith("FAILED:");
        assertThat(view.queuedCount()).isEqualTo(2);
        assertThat(listGates(TOKEN_A, "tenant-a").getBody())
                .anySatisfy(g -> assertThat(g.aggregateKey()).isEqualTo(agg));

        // 修复接收端 + 重放卡住的事件：放行后按原顺序继续，不跳号不倒置
        receiver.setSwitchableReject(false);
        replay(TOKEN_A, e1);
        List<Received> all = awaitReceived(agg, 3, Duration.ofSeconds(15));
        long[] seqs = all.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: after replay sequences={}", java.util.Arrays.toString(seqs));
        assertThat(all).hasSize(3);
        assertThat(seqs).containsExactly(1, 2, 3);
        assertThat(all.stream().map(Received::payload).toList())
                .containsExactly("first", "second", "third");
        // 闸门自动放行
        GateView after = getGate(TOKEN_A, "tenant-a", agg).getBody();
        log.info("assertion basis: gate after unblock state={}", after.state());
        assertThat(after.state()).isEqualTo("OPEN");
    }

    @Test
    void retryWaitHeadBlocksFollowersUntilExhausted() {
        String agg = "agg-retry-" + UUID.randomUUID();
        // timeout 模式每次投递都超时：e1 长期处于 RETRY_WAIT，最终 FAILED
        String e1 = submit(TOKEN_A, "idem-" + agg + "-1", agg, "slow", loopbackUrl("timeout"))
                .getBody().eventId();
        submit(TOKEN_A, "idem-" + agg + "-2", agg, "queued", loopbackUrl("ok"));

        // 等到聚合因 RETRY_WAIT 进入阻塞
        GateView view = awaitGateState(agg, "BLOCKED", Duration.ofSeconds(10));
        log.info("assertion basis: retry-wait block reason={} queued={}", view.blockReason(), view.queuedCount());
        assertThat(view.blockReason()).startsWith("RETRY_WAIT:");
        assertThat(view.blockedEventId()).isEqualTo(e1);
        assertThat(view.queuedCount()).isEqualTo(1);
        assertThat(receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count())
                .as("follower must not pass the retrying head").isZero();

        // 重试耗尽后仍是阻塞，原因变为 FAILED，e2 依旧不得越过
        awaitStatus(TOKEN_A, e1, "FAILED", Duration.ofSeconds(20));
        GateView exhausted = getGate(TOKEN_A, "tenant-a", agg).getBody();
        log.info("assertion basis: exhausted block reason={} queued={}",
                exhausted.blockReason(), exhausted.queuedCount());
        assertThat(exhausted.state()).isEqualTo("BLOCKED");
        assertThat(exhausted.blockReason()).startsWith("FAILED:");
        assertThat(receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count())
                .isZero();
    }

    @Test
    void replayWhilePausedThenResumeKeepsOriginalOrder() throws Exception {
        String agg = "agg-mix-" + UUID.randomUUID();
        String e1 = submit(TOKEN_A, "idem-" + agg + "-1", agg, "first", loopbackUrl("switchable"))
                .getBody().eventId();
        awaitStatus(TOKEN_A, e1, "FAILED", Duration.ofSeconds(10));

        // 阻塞之上再人工暂停；暂停期间提交 e2/e3
        pauseGate(TOKEN_A, "tenant-a", agg);
        submit(TOKEN_A, "idem-" + agg + "-2", agg, "second", loopbackUrl("ok"));
        submit(TOKEN_A, "idem-" + agg + "-3", agg, "third", loopbackUrl("ok"));

        // 修复接收端，在暂停状态下重放卡住的事件：一件都不许投递
        receiver.setSwitchableReject(false);
        replay(TOKEN_A, e1);
        Thread.sleep(600);
        log.info("assertion basis: replayed while paused, deliveries={}",
                receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count());
        assertThat(receiver.recorded().stream().filter(r -> r.aggregateKey().equals(agg)).count())
                .isZero();

        GateView view = getGate(TOKEN_A, "tenant-a", agg).getBody();
        log.info("assertion basis: paused+blocked state={} queued={}", view.state(), view.queuedCount());
        assertThat(view.state()).isEqualTo("PAUSED_BLOCKED");

        // 恢复闸门：重放过的事件与其后事件按原提交顺序生效
        resumeGate(TOKEN_A, "tenant-a", agg);
        List<Received> all = awaitReceived(agg, 3, Duration.ofSeconds(15));
        long[] seqs = all.stream().mapToLong(Received::sequence).toArray();
        log.info("assertion basis: replay+resume sequences={}", java.util.Arrays.toString(seqs));
        assertThat(seqs).containsExactly(1, 2, 3);
        assertThat(all.stream().map(Received::payload).toList())
                .containsExactly("first", "second", "third");
    }

    private void replay(String token, String eventId) {
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("X-Api-Key", token);
        rest.exchange("/api/events/" + eventId + "/replay",
                org.springframework.http.HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(headers), String.class);
    }

    private GateView awaitGateState(String agg, String expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        GateView last = null;
        while (System.nanoTime() < deadline) {
            ResponseEntity<GateView> resp = getGate(TOKEN_A, "tenant-a", agg);
            last = resp.getBody();
            if (last != null && last.state().equals(expected)) {
                return last;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("gate " + agg + " did not reach " + expected
                + ", last=" + (last == null ? "null" : last.state()));
    }
}
