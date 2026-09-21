# 多租户事件可靠投递与重试子系统

基于 Spring Boot 的事件可靠投递子系统：以本地文件持久化与本地回环接收端模拟真实投递环境，
覆盖幂等提交、失败分类重试、聚合内顺序、租约回收、积压约束、重启恢复与租户隔离。

## 投递状态机

```
                 提交
                  │
                  ▼
              PENDING ──认领(授予租约)──► IN_FLIGHT ──投递成功──► DELIVERED (终态)
                ▲  ▲                        │
                │  │            ┌───────────┼────────────────┐
                │  │            ▼           ▼                ▼
                │  │      暂时性失败    暂时性失败        永久性失败(4xx)
                │  │      未达上限      达到上限          (REJECTED)
                │  │            │           │                │
                │  └──退避到期──┘           ▼                ▼
                │                        FAILED (终态) ◄──────┘
                │                           │
                └──────── 显式重放 ──────────┘
```

- `PENDING`：待投递（含等待退避到期的重试）。
- `IN_FLIGHT`：已被某执行单元认领并持有租约，投递中。
- `DELIVERED`：终态，不再投递；重复提交命中幂等索引直接返回该状态。
- `FAILED`：终态（永久性失败或重试耗尽），可查询、可显式重放，重放后回到 `PENDING` 并重置尝试计数。

## 重试与租约规则

**失败分类**（`FailureKind`）：

| 分类 | 触发 | 策略 |
|---|---|---|
| `TIMEOUT` | 连接/读取超时 | 暂时性，指数退避重试 |
| `CONNECTION` | 连接失败/IO 异常 | 暂时性，指数退避重试 |
| `SERVER_ERROR` | 接收端 5xx | 暂时性，指数退避重试 |
| `REJECTED` | 接收端 4xx | 永久性，立即进入 FAILED，不重试 |

- 退避：`min(backoff-max, backoff-base × 2^(attempt-1))`，尝试次数达到 `max-attempts` 后进入 FAILED。
- **租约**：执行单元通过 `claimBatch` 原子认领事件并持有 `lease-duration` 的租约；
  同一事件不会被并发重复投递。执行单元异常退出后，租约到期的事件会被其他单元在
  可控延迟（≤ lease-duration + poll-interval）内重新认领。投递结果仅在租约仍持有时生效，
  租约丢失则丢弃本次结果，由重新认领保证状态自洽。
- **顺序**：同一 `(tenant, aggregateKey)` 内按提交序号单调递增；任意时刻只允许队首事件
  在飞，FAILED 事件阻塞该聚合直到显式重放，保证不倒置、不跳号、不重复生效。

## 持久化与恢复语义

- 每次状态变更将全量快照原子写入 `delivery.store-path`（临时文件 + rename）。
- 重启恢复（`EventStore.recover()`）：加载快照与序号表；残留 `IN_FLIGHT` 一律回收为
  `PENDING`（租约随进程死亡失效）；`DELIVERED`/`FAILED` 终态保持不变，不会重复投递；
  幂等索引与聚合序号从快照恢复，重复提交仍命中去重，序号连续不重置。

## 租户隔离与凭据保护

- 所有 API 要求 `X-Tenant-Token` 头；令牌与路径租户不一致返回 `403 TENANT_MISMATCH`，
  令牌无效返回 `401 INVALID_TOKEN`，缺失返回 `401 MISSING_TOKEN`（原因可区分）。
- 事件、状态与查询均按租户隔离，跨租户不可见。
- 令牌只参与常量时间比对，绝不出现在日志、错误响应或持久化记录中。

## 配置项（`delivery.*`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `store-path` | `data/event-store.json` | 本地持久化文件 |
| `max-attempts` | 5 | 最大投递尝试次数（含首次） |
| `backoff-base` / `backoff-max` | 200ms / 10s | 指数退避基数与上限 |
| `lease-duration` | 5s | 租约时长（异常退出后的回收延迟上界） |
| `poll-interval` | 100ms | 调度轮询间隔 |
| `delivery-timeout` | 2s | 单次投递超时 |
| `max-pending` | 10000 | 待投递积压上限，超出拒绝提交（429 BACKPRESSURE_LIMIT） |
| `workers` | 4 | 并行投递执行单元数 |
| `loopback-base-url` | `http://127.0.0.1:18080` | 投递目标（本地回环接收端） |
| `tenant-tokens` | — | 令牌 → 租户映射（仅本地验证用） |

## API 摘要

- `POST /tenants/{tenant}/events`：提交事件（幂等键 + 聚合键 + 载荷）。
  重复提交返回已有事件；同键不同内容返回 `409 IDEMPOTENCY_CONFLICT`；积压超限返回 `429 BACKPRESSURE_LIMIT`。
- `GET /tenants/{tenant}/events/{eventId}`：查询事件状态。
- `GET /tenants/{tenant}/events?status=FAILED`：按状态查询（如失败事件）。
- `POST /tenants/{tenant}/events/{eventId}/replay`：显式重放 FAILED 事件。
- 回环接收端（本地验证）：`POST /loopback/behavior`（OK/SERVER_ERROR/REJECT/TIMEOUT/FLAKY）、
  `GET /loopback/received`、`POST /loopback/reset`。

## 本地验证

```bash
# 运行全部测试（幂等/故障分类/顺序竞争/租约回收/重启恢复/越权/背压）
mvn test

# 本地启动并手工验证
mvn spring-boot:run
curl -X POST http://127.0.0.1:18080/tenants/tenant-a/events \
  -H 'X-Tenant-Token: token-tenant-a' -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","aggregateKey":"agg-1","payload":"{}"}'
curl http://127.0.0.1:18080/loopback/received
```

测试日志会打印判定依据（如接收序列、失败分类、认领数），且不打印任何令牌。
