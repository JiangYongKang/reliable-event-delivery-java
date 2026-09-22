# 多租户事件可靠投递与重试子系统

基于 Spring Boot 的事件可靠投递子系统：本地持久化 + 本地回环接收端模拟真实投递环境，
覆盖幂等提交、失败分类重试、聚合内顺序、租约回收、积压背压、重启恢复与租户隔离。

## 投递状态机

```
                 submit
                   │
                   ▼
               PENDING ──claim(租约)──► LEASED ──2xx──► DELIVERED (终态)
                 ▲                        │
                 │ replay                 ├─暂时性失败(TIMEOUT/CONNECTION/SERVER_ERROR)─┐
                 │                        │                                          ▼
                 │                        │                                       RETRY_WAIT
                 │                        │                                          │ 到期后重新被认领
                 │                        ├─永久性失败(CLIENT_REJECTED, 4xx)──────────┤
                 │                        │                                          ▼
                 └────────────────────────┴──────── FAILED (终态，可查询、可显式重放)
```

- `PENDING`：已持久化，等待认领。
- `LEASED`：被某执行单元持有租约，投递进行中。租约过期未完成会被其它执行单元回收。
- `RETRY_WAIT`：暂时性失败，带 `nextAttemptAt`，到期后重新可认领。
- `DELIVERED` / `FAILED`：终态。终态事件不再被投递；`FAILED` 可通过 `POST /api/events/{id}/replay` 显式重放回到 `PENDING`。

## 幂等规则

- 幂等键作用域为**租户内**：`(tenantId, idempotencyKey)` 唯一。
- 同键同内容（aggregateKey + payload + targetUrl 一致）：返回已有事件 `duplicate=true`，不产生重复投递。
- 同键不同内容：`409 IDEMPOTENCY_CONFLICT`，明确拒绝。
- 不同租户使用相同幂等键互不影响。

## 失败分类与重试

| 分类 | 触发 | 策略 |
|---|---|---|
| TIMEOUT | 连接/请求超时 | 暂时性，指数退避重试 |
| CONNECTION | 连接拒绝/IO 错误 | 暂时性，指数退避重试 |
| SERVER_ERROR | 接收端 5xx | 暂时性，指数退避重试 |
| CLIENT_REJECTED | 接收端 4xx | 永久性，立即进入 FAILED，不重试 |

- 退避：`retry-base-backoff * 2^(attempt-1)`，封顶 30s。
- 尝试次数达到 `max-attempts` 后进入 `FAILED`（可查询 `lastError`、可重放），不会被静默丢弃。

## 顺序保证

- 提交时为每个 `(tenantId, aggregateKey)` 分配单调递增 `sequence`（存储层锁内分配）。
- 认领时每个聚合键只允许**队首**（最小 sequence 的非终态事件）被认领，
  因此并发投递、重试、重放都不会造成顺序倒置、跳号或重复生效。

## 并发安全与租约

- `worker-count` 个执行单元并行 `认领 → 投递 → 落状态`。
- 认领在存储层锁内原子完成：同一事件同一时刻只有一个持有者。
- 租约时长 `lease-duration`；执行单元异常退出后，事件在租约过期后被重新认领。
- 落状态（markDelivered/markRetry/markFailed）校验租约持有者，失效持有者无法提交状态。

## 积压与资源约束

- 待投递数量（PENDING+RETRY_WAIT+LEASED）达到 `max-pending` 后，新提交返回 `429 BACKPRESSURE_LIMIT`。
- 单次认领批量上限 `claim-batch-size`，单次投递超时 `delivery-timeout`，避免无界占用。

## 持久化与恢复语义

- 存储：`delivery.storage-dir/events.json`，每次状态变更先写临时文件再原子 move。
- 重启后：全部事件与幂等索引从文件恢复；`DELIVERED` 不会重复投递；
  `PENDING`/到期 `RETRY_WAIT` 立即可认领；崩溃时处于 `LEASED` 的事件在租约过期后被回收重投（接收端需按事件 id 幂等，回环接收端按 sequence 验证不重复生效）。

## 租户隔离与凭据保护

- `/api/**` 需请求头 `X-Api-Key`；缺失返回 `401 TENANT_MISSING`，无效返回 `401 TENANT_UNAUTHORIZED`（可区分）。
- 租户只来自认证上下文，客户端无法指定他人租户；跨租户查询/重放返回 `404 EVENT_NOT_FOUND`。
- 令牌只用于内存常量时间比较，不出现在日志、错误响应或持久化审计记录中；审计日志只含租户、事件 id 与动作。

## 配置项（`delivery.*`）

| 配置 | 默认 | 说明 |
|---|---|---|
| storage-dir | target/event-store | 本地持久化目录 |
| worker-count | 2 | 并行投递执行单元数 |
| lease-duration | 10s | 单次认领养约时长 |
| delivery-timeout | 2s | 单次投递超时 |
| retry-base-backoff | 200ms | 重试基础退避 |
| max-attempts | 5 | 最大尝试次数（含首次） |
| max-pending | 10000 | 待投递数量上限 |
| claim-batch-size | 32 | 单次认领批量上限 |
| poll-interval | 100ms | 工作线程扫描间隔 |

## API

- `POST /api/events` `{idempotencyKey, aggregateKey, payload, targetUrl}` → `202 {eventId, status, duplicate}`
- `GET /api/events` / `GET /api/events/{id}` → 租户内查询
- `POST /api/events/{id}/replay` → 重放 FAILED 事件（非 FAILED 返回 `409 EVENT_NOT_REPLAYABLE`）

## 本地验证

```bash
mvn test
```

测试与覆盖点：

| 测试类 | 覆盖 |
|---|---|
| IdempotencyTests | 重复提交去重、同键冲突 409、终态不重投 |
| FailureClassificationTests | 5xx/超时退避重试至耗尽、4xx 不重试、flaky 恢复、FAILED 重放 |
| OrderingTests | 并发提交同聚合键顺序生效、重试期间不倒置 |
| LeaseRecoveryTests | 并发认领互斥、租约过期回收、失效持有者无法落状态 |
| RestartRecoveryTests | 重启后状态/幂等索引恢复、终态不重投 |
| TenantIsolationTests | 401 可区分、跨租户不可见、令牌不回显 |
| BackpressureTests | 积压超限 429 拒绝 |

回环接收端 `/receiver/{mode}`：`ok` / `timeout` / `server-error` / `reject` / `flaky`，
`GET /receiver/recorded` 查看已接收投递。测试日志打印判定依据（状态、次数、序号），不打印敏感字段。
