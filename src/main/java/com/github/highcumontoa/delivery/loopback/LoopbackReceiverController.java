package com.github.highcumontoa.delivery.loopback;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地回环接收端：模拟真实投递目标。
 *
 * <p>行为可编程：OK 正常接收并记录；SERVER_ERROR 返回 500；REJECT 返回 400；
 * TIMEOUT 挂起超过客户端超时。接收记录（事件ID/聚合键/序号）可用于测试判定
 * 顺序、去重与重放语义。
 */
@RestController
@RequestMapping("/loopback")
public class LoopbackReceiverController {

    private static final Logger log = LoggerFactory.getLogger(LoopbackReceiverController.class);

    public enum Mode {
        OK,
        SERVER_ERROR,
        REJECT,
        TIMEOUT,
        /** 首次请求返回 500，之后正常：用于验证重试不破坏顺序。 */
        FLAKY
    }

    /** 一条接收记录。 */
    public record Received(String eventId, String aggregateKey, long sequence, String payload) {
    }

    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.OK);
    private final java.util.concurrent.atomic.AtomicInteger flakyRemaining = new java.util.concurrent.atomic.AtomicInteger(0);
    private final List<Received> received = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<String> receiveLog = new ConcurrentLinkedQueue<>();

    @PostMapping("/receive")
    public ResponseEntity<String> receive(@RequestHeader("X-Event-Id") String eventId,
                                          @RequestHeader("X-Aggregate-Key") String aggregateKey,
                                          @RequestHeader("X-Event-Sequence") long sequence,
                                          @RequestBody String payload) throws InterruptedException {
        Mode current = mode.get();
        if (current == Mode.FLAKY) {
            if (flakyRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                return ResponseEntity.status(500).body("flaky failure");
            }
            current = Mode.OK;
        }
        switch (current) {
            case TIMEOUT -> {
                // 挂起超过客户端超时，模拟接收端无响应
                Thread.sleep(10_000);
                return ResponseEntity.ok("late");
            }
            case SERVER_ERROR -> {
                return ResponseEntity.status(500).body("simulated server error");
            }
            case REJECT -> {
                return ResponseEntity.status(400).body("simulated rejection");
            }
            default -> {
                received.add(new Received(eventId, aggregateKey, sequence, payload));
                receiveLog.add(aggregateKey + "#" + sequence);
                return ResponseEntity.ok("accepted");
            }
        }
    }

    /** 设置接收端行为模式（测试/本地验证用）。 */
    @PostMapping("/behavior")
    public Map<String, String> setBehavior(@RequestBody Map<String, String> body) {
        Mode newMode = Mode.valueOf(body.get("mode"));
        mode.set(newMode);
        if (newMode == Mode.FLAKY) {
            flakyRemaining.set(Integer.parseInt(body.getOrDefault("failTimes", "1")));
        }
        log.info("loopback behavior set: mode={}", newMode);
        return Map.of("mode", newMode.name());
    }

    /** 已接收事件列表（按接收顺序）。 */
    @GetMapping("/received")
    public List<Received> received() {
        return List.copyOf(received);
    }

    /** 清空接收记录。 */
    @PostMapping("/reset")
    public Map<String, String> reset() {
        received.clear();
        receiveLog.clear();
        return Map.of("status", "reset");
    }
}
