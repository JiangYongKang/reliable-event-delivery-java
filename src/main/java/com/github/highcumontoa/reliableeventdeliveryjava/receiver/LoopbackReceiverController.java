package com.github.highcumontoa.reliableeventdeliveryjava.receiver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地回环接收端，模拟真实投递目标。行为由路径模式决定：
 * ok=正常接收；timeout=长时间不响应；server-error=500；reject=400；flaky=前若干次 500 后恢复。
 * 记录所有收到的投递供测试断言顺序与去重。
 */
@RestController
@RequestMapping("/receiver")
public class LoopbackReceiverController {

    private static final Logger log = LoggerFactory.getLogger(LoopbackReceiverController.class);

    public record Received(String eventId, String aggregateKey, long sequence, String payload) {
    }

    private final List<Received> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger flakyCalls = new AtomicInteger();
    private volatile int flakyFailures = 2;

    @PostMapping("/{mode}")
    public ResponseEntity<String> receive(@PathVariable String mode,
                                          @RequestHeader("X-Event-Id") String eventId,
                                          @RequestHeader("X-Aggregate-Key") String aggregateKey,
                                          @RequestHeader("X-Sequence") long sequence,
                                          @RequestBody String payload) throws InterruptedException {
        switch (mode) {
            case "ok" -> {
                received.add(new Received(eventId, aggregateKey, sequence, payload));
                return ResponseEntity.ok("accepted");
            }
            case "timeout" -> {
                Thread.sleep(10_000);
                return ResponseEntity.ok("too late");
            }
            case "server-error" -> {
                return ResponseEntity.status(500).body("boom");
            }
            case "reject" -> {
                return ResponseEntity.status(400).body("rejected");
            }
            case "flaky" -> {
                if (flakyCalls.incrementAndGet() <= flakyFailures) {
                    return ResponseEntity.status(500).body("flaky failure");
                }
                received.add(new Received(eventId, aggregateKey, sequence, payload));
                return ResponseEntity.ok("accepted");
            }
            default -> {
                return ResponseEntity.status(404).body("unknown mode");
            }
        }
    }

    @GetMapping("/recorded")
    public List<Received> recorded() {
        return List.copyOf(received);
    }

    @PostMapping("/reset")
    public void reset() {
        received.clear();
        flakyCalls.set(0);
    }

    public void setFlakyFailures(int n) {
        this.flakyFailures = n;
    }
}
