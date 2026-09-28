# 多租户事件可靠投递与重试子系统

基于 Spring Boot 的事件可靠投递子系统：本地持久化 + 本地回环接收端模拟真实投递环境，
覆盖幂等提交、失败分类重试、聚合内顺序、租约回收、积压背压、重启恢复、租户隔离，
以及按“租户 + 聚合键”的投递闸门（人工暂停/恢复、失败自动阻塞、单聚合积压上限）。

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
- 认领时每个聚合键只允许**队首**（最小 sequence 的未完成事件）被认领，
  因此并发投递、重试、重放都不会造成顺序倒置、跳号或重复生效。

## 按聚合键的投递闸门

在“按事件/租户”的控制之外，再提供一层按 `(tenantId, aggregateKey)` 的运维闸门，
用于先把出问题的聚合“管住、看清楚、再放行”。闸门判断在存储层同一把锁内完成，与认领、提交互斥。

### 闸门状态

| 状态 | 含义 | 进入方式 | 投递行为 |
|---|---|---|---|
| `ACTIVE` | 正常 | 默认；队首可投递 | 队首正常认领投递 |
| `PAUSED` | 人工暂停 | `POST /api/gates/{key}/pause` | 新事件照常收下，但该聚合一件都不投递；已在投递中的事件会继续完成 |
| `BLOCKED` | 自动阻塞 | 队首事件未决（见下） | 卡住队首之后的事件全部排队，不会越过队首先生效 |

状态优先级：暂停优先展示为 `PAUSED`（即使队首同时 FAILED）；未暂停时若队首未决则为 `BLOCKED`；否则 `ACTIVE`。
**暂停与阻塞都不影响事件提交**——暂停/阻塞期间新事件照常 202 收下并入队。

### 什么时候自动阻塞

认领只取聚合内最小 sequence 的未完成事件，且**只排除 DELIVERED**。因此：

- 队首处于 `RETRY_WAIT`（暂时性失败，等下次重试）→ 闸门 `BLOCKED`，原因 `RETRYING`，
  其后事件排队；队首重试成功后聚合自动回到 `ACTIVE` 并继续按序投递。
- 队首处于 `FAILED`（重试耗尽或 4xx 永久失败）→ 闸门 `BLOCKED`，原因 `PERMANENT_FAILURE`；
  后续事件一直排队，直到人工 `replay` 队首并投递成功。
- 队首是 `PENDING`/`LEASED`（链路正在推进）不算阻塞，闸门为 `ACTIVE`。

闸门视图可看到每个被拦聚合：`state`、`reason`、卡住的队首（`headEventId/headSequence/headStatus/
headAttemptCount/headLastError/headNextAttemptAt`）、`queuedCount`（卡住队首及其后全部未投递事件数）。

### 暂停 / 恢复与顺序、重放、重试的配合

- **暂停**：认领循环跳过整个聚合。已被租约认领、正在投递的事件允许完成；之后该聚合不再被认领。
  新事件仍分配连续 sequence 并入队，别的租户、别的聚合完全不受影响。
- **恢复**：不重置任何状态，认领立刻从暂停时的队首继续，按原 `sequence` 顺序逐件投递；
  不会跳过、提前、重复或乱序。
- **重放与恢复同时发生**：`replay` 只把 FAILED 事件重置为 PENDING，是否真正投递仍由“队首 + 闸门”决定。
  暂停期间重放只入队不投递；先重放后恢复、或先恢复后重放，最终都严格按 `1,2,3…` 各生效一次。
- 与重试/租约回收正交：阻塞只是“暂不认领”，事件本身的 `RETRY_WAIT`/`LEASED` 状态与退避、租约语义不变。

### 单聚合积压容量（不能无界占内存）

被暂停或被阻塞的聚合，其排队事件数受 `delivery.gate-max-backlog` 约束（按聚合计数，
含卡住队首；正常 `ACTIVE` 聚合只受全局 `max-pending` 约束）。到顶后新提交按策略处理：

- `delivery.gate-overflow-policy=REJECT`（默认）：立即 `429 AGGREGATE_BACKLOG_LIMIT`，
  与全局超限的 `429 BACKPRESSURE_LIMIT` 是不同错误码，可分辨。
- `=DEFER`：在 `delivery.gate-defer-timeout` 内轮询等待积压腾出名额（如恢复后队列被消化），
  等待期间提交被推迟；超时仍满则同样返回 `429 AGGREGATE_BACKLOG_LIMIT`。
- 幂等判定优先于容量判定：重复提交（同键同内容）即使到顶也返回 `duplicate=true`，不占新名额。

### 跨租户操作

闸门操作的真实租户只来自认证上下文；按 id 查询他人聚合返回 `404 GATE_NOT_FOUND`（不泄露存在性），
列表只含本租户。暂停/恢复请求体可显式带 `tenantId`，若与认证租户不一致则返回
`403 TENANT_FORBIDDEN`（跨租户操作有可区分原因）。

### 持久化与恢复

- 暂停状态持久化在 `delivery.storage-dir/gates.json`（临时文件 + 原子 move），与 `events.json`
  在同一把锁内变更。
- 重启后：暂停集合、各事件状态、幂等索引与排队顺序全部恢复；暂停的聚合仍一件不投，
  FAILED 队首仍阻塞后续，`DELIVERED` 不重复投递；重放/恢复后按原顺序继续。

## 并发安全与租约

- `worker-count` 个执行单元并行 `认领 → 投递 → 落状态`。
- 认领在存储层锁内原子完成：同一事件同一时刻只有一个持有者。
- 租约时长 `lease-duration`；执行单元异常退出后，事件在租约过期后被重新认领。
- 落状态（markDelivered/markRetry/markFailed）校验租约持有者，失效持有者无法提交状态。

## 积压与资源约束

- 待投递数量（PENDING+RETRY_WAIT+LEASED）达到 `max-pending` 后，新提交返回 `429 BACKPRESSURE_LIMIT`。
- 单次认领批量上限 `claim-batch-size`，单次投递超时 `delivery-timeout`，避免无界占用。

## 持久化与恢复语义

- 存储：`delivery.storage-dir/events.json`（事件）与 `gates.json`（暂停的聚合），每次状态变更先写临时文件再原子 move。
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
| gate-max-backlog | 1000 | 单个被暂停/被阻塞聚合的排队事件上限 |
| gate-overflow-policy | REJECT | 到顶策略：`REJECT` 立即拒绝 / `DEFER` 限时等待 |
| gate-defer-timeout | 5s | DEFER 策略最长等待时间 |

## API

- `POST /api/events` `{idempotencyKey, aggregateKey, payload, targetUrl}` → `202 {eventId, status, duplicate}`
- `GET /api/events` / `GET /api/events/{id}` → 租户内查询
- `POST /api/events/{id}/replay` → 重放 FAILED 事件（非 FAILED 返回 `409 EVENT_NOT_REPLAYABLE`）
- `POST /api/gates/{aggregateKey}/pause` / `resume`（请求体可带 `{"tenantId":"..."}` 显式声明归属）
  → 返回闸门视图；重复暂停 `409 GATE_ALREADY_PAUSED`、恢复未暂停 `409 GATE_NOT_PAUSED`
- `GET /api/gates/{aggregateKey}` → 单个聚合闸门状态（无事件且未暂停 → `404 GATE_NOT_FOUND`）
- `GET /api/gates?state=ACTIVE|PAUSED|BLOCKED`（state 可省略）→ 列出本租户有排队或被暂停的聚合

闸门视图字段：`tenantId, aggregateKey, state, reason, headEventId, headSequence, headStatus,
headAttemptCount, headLastError, headNextAttemptAt, queuedCount, paused, pausedAt, backlogLimit`。

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
| BackpressureTests | 全局积压超限 429 拒绝 |
| AggregateGateTests | 暂停只收不投/恢复按序、FAILED 队首自动阻塞与重放放行、跨租户 403/404 |
| GateInterleaveAndRetryTests | RETRY_WAIT 自动阻塞、重放与恢复交织时严格 1,2,3 顺序 |
| GateBacklogTests | 单聚合到顶 429 AGGREGATE_BACKLOG_LIMIT、其它聚合不受影响、恢复后可再投 |
| GateBacklogDeferTests | DEFER 策略：腾位后推迟成功、永不腾位超时 429 |
| GateRestartRecoveryTests | 重启后暂停/阻塞/积压/容量恢复、不重复投递、按序认领 |

回环接收端 `/receiver/{mode}`：`ok` / `timeout` / `server-error` / `reject` / `flaky`，
`GET /receiver/recorded` 查看已接收投递。测试日志打印判定依据（状态、次数、序号），不打印敏感字段。
