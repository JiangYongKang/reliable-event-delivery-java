# 多租户事件可靠投递与重试子系统

基于 Spring Boot 的事件可靠投递子系统：本地持久化 + 本地回环接收端模拟真实投递环境，
覆盖幂等提交、失败分类重试、聚合内顺序、租约回收、积压背压、重启恢复、租户隔离，
以及按聚合键的投递闸门（人工暂停/恢复、失败自动阻塞、排队容量控制）。

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

## 按聚合键的投递闸门

在“按事件/租户”之上再增加一层 `(tenantId, aggregateKey)` 维度的闸门。闸门持久化在
`delivery.storage-dir/gates.json`（临时文件 + 原子 move），暂停状态、阻塞状态与排队事件重启后均可恢复。

### 闸门状态

| 状态 | 含义 |
|---|---|
| `OPEN` | 正常投递（无闸门记录时即为 OPEN） |
| `PAUSED` | 人工暂停：新事件照常接收并持久化，但一件都不投递 |
| `BLOCKED` | 自动阻塞：聚合队首事件还没处理完，后续事件全部排队 |
| `PAUSED_BLOCKED` | 同时被人工暂停且存在卡住事件（两个条件可叠加） |

### 什么时候自动阻塞

认领始终只取每个聚合的队首，队首卡住即阻塞整个聚合：

- 队首暂时性失败进入 `RETRY_WAIT`（等下次重试）→ 立即阻塞，原因形如 `RETRY_WAIT:TIMEOUT`。
- 队首重试耗尽或被接收端永久拒绝（4xx）进入 `FAILED` → 阻塞，原因形如 `FAILED:SERVER_ERROR`。
- 卡住的事件最终投递成功（重试自行恢复，或人工 `replay` 后成功）→ **自动放行**；其它事件成功不触发放行。
- 人工暂停不会自动解除阻塞；`resume` 只解除人工暂停。`BLOCKED` 只能靠卡住事件投递成功解除——
  被阻塞聚合不允许跳过队首事件，杜绝后面的事件先生效。

### 与重试 / 重放 / 顺序保证的配合

- 闸门拦截发生在认领阶段：暂停聚合整队跳过；阻塞聚合只允许“卡住它的那个事件”被认领，
  因此暂停期间新事件、重试等待、重放、恢复任意交织，恢复后都从暂停/卡住位置按原 `sequence` 继续，
  不跳过、不提前生效、不重复生效、不倒置。
- 人工重放与恢复闸门同时发生时：暂停优先（重放的事件也不投递）；恢复后若仍阻塞则继续排队，
  阻塞解除后严格按 1,2,3… 顺序生效。
- 闸门只影响本租户本聚合；同租户其它聚合、其它租户同名聚合完全不受影响。

### 排队容量（不无界占内存）

- 被暂停/阻塞的聚合，其排队事件数达到 `gate-max-queued-per-aggregate` 后，按
  `gate-overflow-policy` 处理新提交：
  - `REJECT`（默认）：`429 GATE_CAPACITY_EXCEEDED`，与全局积压的 `429 BACKPRESSURE_LIMIT` 可区分。
  - `DEFER`：照常接收并入队（仍受全局 `max-pending` 约束），闸门打开后按序投递。
- 排队数可通过闸门查询查看：阻塞时只统计“卡在 blocked 事件之后”的件数。

### 闸门 API

- `PUT /api/gates/{tenantId}/{aggregateKey}/pause` / `…/resume`：暂停 / 恢复（幂等）。
- `GET /api/gates/{tenantId}/{aggregateKey}`：单个闸门状态（state / blockedEventId / blockReason / queuedCount）。
- `GET /api/gates/{tenantId}`：列出该租户所有非 OPEN（暂停或阻塞）的闸门。
- 路径中的 `tenantId` 必须与认证租户一致；跨租户查询/暂停/恢复返回 `403 TENANT_FORBIDDEN`（可区分）。


## 并发安全与租约

- `worker-count` 个执行单元并行 `认领 → 投递 → 落状态`。
- 认领在存储层锁内原子完成：同一事件同一时刻只有一个持有者。
- 租约时长 `lease-duration`；执行单元异常退出后，事件在租约过期后被重新认领。
- 落状态（markDelivered/markRetry/markFailed）校验租约持有者，失效持有者无法提交状态。

## 积压与资源约束

- 待投递数量（PENDING+RETRY_WAIT+LEASED）达到 `max-pending` 后，新提交返回 `429 BACKPRESSURE_LIMIT`。
- 被暂停/阻塞聚合的排队达到 `gate-max-queued-per-aggregate` 后，按 `gate-overflow-policy`（REJECT/DEFER）处理，
  REJECT 返回 `429 GATE_CAPACITY_EXCEEDED`，两个拒绝原因互不混淆。
- 单次认领批量上限 `claim-batch-size`，单次投递超时 `delivery-timeout`，避免无界占用。

## 持久化与恢复语义

- 事件存储：`delivery.storage-dir/events.json`；闸门存储：`delivery.storage-dir/gates.json`，
  每次状态变更先写临时文件再原子 move。
- 重启后：全部事件、幂等索引与闸门（暂停/阻塞状态、卡住的事件、阻塞原因）从文件恢复；`DELIVERED` 不会重复投递；
  `PENDING`/到期 `RETRY_WAIT` 立即可认领；崩溃时处于 `LEASED` 的事件在租约过期后被回收重投（接收端需按事件 id 幂等，回环接收端按 sequence 验证不重复生效）。
- 闸门拦截在认领阶段生效，所以恢复后被暂停的聚合仍然一件不出、被阻塞的聚合仍然只有卡住事件可认领，
  卡住事件成功后自动放行，未投完的排队事件按序接着来。

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
| max-pending | 10000 | 全局待投递数量上限 |
| gate-max-queued-per-aggregate | 1000 | 被暂停/阻塞的单个聚合排队事件上限 |
| gate-overflow-policy | REJECT | 聚合排队到顶策略：REJECT 拒绝（429 GATE_CAPACITY_EXCEEDED）/ DEFER 推迟接收 |
| claim-batch-size | 32 | 单次认领批量上限 |
| poll-interval | 100ms | 工作线程扫描间隔 |

## API

- `POST /api/events` `{idempotencyKey, aggregateKey, payload, targetUrl}` → `202 {eventId, status, duplicate}`
- `GET /api/events` / `GET /api/events/{id}` → 租户内查询
- `POST /api/events/{id}/replay` → 重放 FAILED 事件（非 FAILED 返回 `409 EVENT_NOT_REPLAYABLE`）
- `PUT /api/gates/{tenantId}/{aggregateKey}/pause` / `/resume` → 聚合闸门暂停 / 恢复
- `GET /api/gates/{tenantId}/{aggregateKey}` / `GET /api/gates/{tenantId}` → 闸门与排队查询
- 跨租户闸门操作 → `403 TENANT_FORBIDDEN`；聚合排队到顶（REJECT）→ `429 GATE_CAPACITY_EXCEEDED`

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
| GatePauseResumeTests | 暂停期间不投递且不影响其它租户/聚合、恢复后按序继续、跨租户操作 403 |
| GateBlockingTests | 重试中/彻底失败自动阻塞与排队查询、重放后按序放行、暂停+重放+恢复交织顺序 |
| GateCapacityTests | 排队到顶 REJECT（429 GATE_CAPACITY_EXCEEDED，与全局背压可区分） |
| GateDeferPolicyTests | DEFER 策略到顶仍接收，恢复后按序投递 |
| GateRestartRecoveryTests | 重启后暂停/阻塞/积压恢复，不重复投递、不跳号 |

回环接收端 `/receiver/{mode}`：`ok` / `timeout` / `server-error` / `reject` / `flaky` / `switchable`
（`switchable` 初始拒绝、测试中途可切回正常接收，用于阻塞后重放场景），
`GET /receiver/recorded` 查看已接收投递。测试日志打印判定依据（状态、次数、序号、排队数、阻塞原因），不打印敏感字段。

### 手动验证闸门

```bash
mvn spring-boot:run &
AGG=agg-demo
# 暂停聚合
curl -s -XPUT -H "X-Api-Key: token-a-secret" http://localhost:8080/api/gates/tenant-a/$AGG/pause
# 提交若干事件（202 接收但不投递）
curl -s -H "X-Api-Key: token-a-secret" -H "Content-Type: application/json" \
  -d '{"idempotencyKey":"k1","aggregateKey":"'$AGG'","payload":"p1","targetUrl":"http://localhost:8080/receiver/ok"}' \
  http://localhost:8080/api/events
# 查看闸门：state=PAUSED、queuedCount 随提交增长
curl -s -H "X-Api-Key: token-a-secret" http://localhost:8080/api/gates/tenant-a/$AGG
# 恢复：接收端随后按 1,2,3… 顺序收到事件
curl -s -XPUT -H "X-Api-Key: token-a-secret" http://localhost:8080/api/gates/tenant-a/$AGG/resume
# 跨租户操作被拒绝：403 TENANT_FORBIDDEN
curl -s -XPUT -H "X-Api-Key: token-a-secret" http://localhost:8080/api/gates/tenant-b/$AGG/pause
```
