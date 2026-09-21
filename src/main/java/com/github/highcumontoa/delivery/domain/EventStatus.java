package com.github.highcumontoa.delivery.domain;

/**
 * 投递状态机：
 * PENDING   -> 待投递（含等待重试）
 * IN_FLIGHT -> 已被某执行单元认领（持有租约），投递中
 * DELIVERED -> 终态：投递成功
 * FAILED    -> 终态：永久性失败或重试耗尽；可查询、可显式重放（重放后回到 PENDING）
 */
public enum EventStatus {
    PENDING,
    IN_FLIGHT,
    DELIVERED,
    FAILED;

    public boolean isTerminal() {
        return this == DELIVERED || this == FAILED;
    }
}
