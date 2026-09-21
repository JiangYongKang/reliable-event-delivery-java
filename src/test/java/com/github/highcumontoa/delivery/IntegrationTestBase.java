package com.github.highcumontoa.delivery;

import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;

/** 集成测试基类：提供轮询等待与判定依据日志。 */
public abstract class IntegrationTestBase {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    protected TestRestTemplate rest;

    /** 轮询等待条件成立，超时失败。 */
    protected void await(String description, Supplier<Boolean> condition, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) {
                log.info("await satisfied: {}", description);
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assertions.fail("interrupted while awaiting: " + description);
            }
        }
        Assertions.fail("timeout awaiting: " + description);
    }

    protected void await(String description, Supplier<Boolean> condition) {
        await(description, condition, Duration.ofSeconds(15));
    }
}
