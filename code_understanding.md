# Workflow 引擎运行时后台组件全景（代码理解文档）

> 复核基准：仓库根目录（Go package `workflow`，`github.com/luno/workflow`）。所有结论均标注源码文件、函数名与 goroutine 启动点；行号基于当前工作树。本文不涉及任何代码修改。

## 0. 总览与关键事实

- 唯一入口：`(*Workflow).Run`（`workflow.go:112`）。所有后台组件都通过 `track(w, func(){...})`（`workflow.go:215`）以 `go fn()` 启动；`Run` 用 `sync.Once` 保证只启动一次，并通过 `w.launching.Wait()`（`workflow.go:210`）等待每个 goroutine 在 `w.run`（`workflow.go:221`）内完成 `internalState` 登记后才返回。
- 统一进程外壳：每个组件 goroutine 都跑 `w.run` → `runOnce`（`workflow.go:221`、`workflow.go:261`）。`runOnce` 负责：向 `RoleScheduler.Await`（`rolescheduler.go:22`）申请角色（同一 role 全局唯一持有者，用于多实例选主）、在 `internalState` 中登记 `StateIdle/StateRunning/StateShutdown`（`state.go:34`）、错误退避后无限重连。
- 两类消息来源：
  1. **事件流（`EventStreamer`，`eventstreamer.go:10`）**：按 topic 消费，带持久游标 `Ack`（`eventstreamer.go:30`）。
  2. **轮询（`TimeoutStore` / outbox 表）**：定时 `List*`。
- 事件不是 RecordStore 主动推送，而是 **Transactional Outbox**：`RecordStore.Store` 在同一事务里写 record + outbox 行（接口契约见 `store.go:11`；SQL 实现 `adapters/sqlstore/sqlstore.go:48` 的 `Store`，载荷由 `MakeOutboxEventData` 生成，见 `event.go:75`），再由 outbox consumer 投递到 EventStreamer 的具体 topic。
- topic 三类（`topic.go`）：
  - 状态 topic：`Topic(name, status)` = `<wf>-<status>`（`topic.go:13`），每个 status 一条。
  - `DeleteTopic` = `<wf>-delete`（`topic.go:21`）。
  - `RunStateChangeTopic` = `<wf>-run-state-change`（`topic.go:29`）：RunState 变为 `Paused/Cancelled/Completed/DataDeleted` 的事件全部改道到此（`event.go:89-94` 的 `MakeOutboxEventData`），普通 step consumer 因此看不到终态事件。
- RunState 状态机（常量 `runstate.go:13`，转移表 `runstate.go:132`）：
  `Unknown(0) → Initiated(1) → Running(2) ⇄ Paused(3)`；`Running/Paused → Cancelled(4)`；`Running → Completed(5)`；`Completed/Cancelled → RequestedDataDeleted(7) → DataDeleted(6)`（转移表也允许 `DataDeleted → RequestedDataDeleted`）。
  - `Finished()`（`runstate.go:50`）：`Completed/Cancelled/RequestedDataDeleted/DataDeleted`。
  - `Stopped()`（`runstate.go:62`）：`Paused/Cancelled/RequestedDataDeleted/DataDeleted`——各 consumer 见到 Stopped 记录一律跳过。
- 重要事实：仓库中**不存在任何 `recover()`**（全仓 `grep -rn "recover" --include="*.go"` 无命中），panic 不会被引擎隔离（详见第 4 节）。

---

## 1. 组件清单与启动（`Workflow.Run`，`workflow.go:112-211`）

启动顺序即代码顺序（goroutine 实际调度无序，但注册顺序如下）：

| # | 组件 / goroutine 启动点 | 实现函数 | 启动条件 | 监听对象 & 消费方式 | role 构成 |
|---|---|---|---|---|---|
| 1 | outbox consumer，`workflow.go:127` | `outboxConsumer`（`outbox.go:14`）→ `purgeOutbox`（`outbox.go:102`） | 默认开启；`WithoutOutbox()`（`builder.go:305`）置 `outboxConfig.disabled=true` 时不启动（判断在 `workflow.go:125`） | **轮询** RecordStore：`ListOutboxEvents(name, limit)`（默认 limit 1000，`outbox.go:53`）→ 按事件头 `HeaderTopic` 经 EventStreamer `NewSender/Send` 投递到对应 topic → `DeleteOutboxEvent`；空列表睡 `pollingFrequency`（默认 250ms，`builder.go:21`） | `<wf>-outbox-consumer` |
| 2 | step consumers（每个已配置 status 一个或多个），`workflow.go:141` / `workflow.go:147` | `consumeStepEvents`（`step.go:13`）→ `consume`（`consumer.go:29`）+ `stepConsumer`（`step.go:105`） | `builder.AddStep`（`builder.go:50`）注册过的每个 status 必启 | **事件流** `Topic(name,status)`，`NewReceiver` 持久游标；`parallelCount>=2` 时启动 N 个 shard，靠 `shardFilter(shard,total)`（`eventfilter.go:54`，按 `event.ID % total`）分片并行；单实例时 `consumeStepEvents(w,s,c,1,1)` | `<wf>-<status数字>-consumer-<i>-of-<n>`（落库用数字，稳定）；`processName` 用 status 字符串，仅用于指标/状态展示 |
| 3 | timeout auto-inserter consumer，`workflow.go:161` | `timeoutAutoInserterConsumer`（`timeout.go:213`） | 仅当 Build 提供了 `TimeoutStore`（`workflow.go:156`）；配置了 timeout 却不提供 store 会在 `builder.go:254` panic；对 `w.timeouts` 中每个 status 启动一个 | **事件流** status topic，复用 `consume` + `stepConsumer`（无分片、无 lag）。对每条记录调用每个 timeout 配置的 `TimerFunc`，返回非零时间即 `timeoutStore.Create(...)`（`timeout.go:254`）；消费者恒返回 status 0，不推进状态 | `<wf>-<status数字>-timeout-auto-inserter-consumer` |
| 4 | timeout poller，`workflow.go:158` | `timeoutPoller`（`timeout.go:178`）→ `pollTimeouts`（`timeout.go:24`）→ `processTimeout`（`timeout.go:109`） | 同上，每个配置了 timeout 的 status 一个 | **轮询** `TimeoutStore.ListValid(name,status,now)`（`store.go:45`；未完成且 `ExpireAt<=now`，内存实现见 `adapters/memtimeoutstore/memtimeoutstore.go:114`），每轮后睡 `pollingFrequency`（默认 500ms）。不消费事件流、**不分片**（timeout 配 parallel/lag 直接 panic，`builder.go:149`、`builder.go:153`） | `<wf>-<status数字>-timeout-consumer` |
| 5 | connector consumers，`workflow.go:176` / `workflow.go:182` | `connectorConsumer`（`connector.go:35`）→ `consume` + `connectorStreamer`（`connector.go:102`） | 每个 `builder.AddConnector`（`builder.go:163`）配置启动；与 timeoutStore 无关 | **外部流**：`ConnectorConstructor.Make(role)` 产出 `ConnectorConsumer`（`connector.go:14`，调用点 `connector.go:70`），被包成 `EventReceiver`（FNV-1a 哈希 connector 事件字符串 ID 得 `Event.ID` 仅用于分片，`connector.go:144` 的 `connectorEventToEvent`）；`parallelCount>=2` 时按 `shardFilter` 分片。消费到事件只调用户 `ConnectorFunc`（`connector.go:87`）；派生本 workflow 新记录靠用户在其中调用 `api.Trigger`（例：`_examples/connector/connector.go:46`） | `<connector名>-connector-to-<wf>-consumer-<i>-of-<n>` |
| 6 | run-state-change hook consumers，`workflow.go:192` | `runStateChangeHookConsumer`（`hook.go:12`）→ `runHook`（`hook.go:57`） | 仅注册过的 hook：`OnPause/OnCancel/OnComplete`（`builder.go:204/208/212`）分别对应 `RunStatePaused/Cancelled/Completed`，map 每项一个 goroutine | **事件流** `RunStateChangeTopic`，用 `filterByRunState(state)`（`eventfilter.go:86`，比对事件头 `HeaderRunState`）只收本 hook 关心的状态事件（过滤点 `hook.go:52`）；`Lookup(runID)` 取最新记录、反序列化 Object 后调用户 hook | `<wf>-<runstate>-run-state-change-hook-consumer` |
| 7 | delete consumer，`workflow.go:198`（无条件） | `deleteConsumer`（`delete.go:7`）→ `runDelete`（`delete.go:45`） | 恒启动，单实例无分片 | **事件流** `DeleteTopic`。`Lookup` 记录 → 用默认 `{"result":"deleted"}` 或 `WithCustomDelete`（`builder.go:356`）的返回值替换 `Object` → RunState 置 `DataDeleted` 并 `updateRecord`（`delete.go:68` 起） | `<wf>-delete-consumer` |
| 8 | paused-records retry consumer，`workflow.go:204` | `pausedRecordsRetryConsumer`（`pause.go:47`）→ `autoRetryConsumer`（`pause.go:99`） | `pausedRecordsRetry.enabled`：默认开启、`resumeAfter=1h`（`pause.go:137`）；`DisablePauseRetry()` 关闭（`builder.go:389`），`WithPauseRetry(d)` 调整间隔（`builder.go:380`）；判断在 `workflow.go:203` | **事件流** `RunStateChangeTopic` + `filterByRunState(RunStatePaused)`（`pause.go:94`）；消费 lag 即 `resumeAfter`（`pause.go:92` 的 consume 调用传入 `lag=resumeAfter`，事件未成熟不处理），再校验记录仍为 Paused 且 `UpdatedAt` 早于 `now-resumeAfter`，然后 `RunStateController.Resume`（`pause.go:121`） | `<wf>-paused-records-retry-consumer` |

补充：

- `Schedule`（`schedule.go:14`）不是 `Run` 启动的组件，而是用户显式调用的阻塞 API；它内部复用 `w.run`（并自行 `w.launching.Add(1)`，`schedule.go:38`），按 cron 周期调 `Trigger`，遇到 `ErrWorkflowInProgress` 跳过（`schedule.go:90`）。
- `Await`（`await.go:10`）与 `Callback`（`callback.go:15`）也是按需 API，不起常驻 goroutine：`Await` 在目标 status 为图终态时改听 `RunStateChangeTopic`（`await.go:41-43`），按 `foreignID+runID` 过滤（`eventfilter.go:64`、`eventfilter.go:75`）；`Callback` 同步执行用户回调并经同一个 `updater` 推进状态（`callback.go:103`）。
- 每个配置了 timeout 的 status 对应**两个** goroutine（inserter + poller）；每个 step/connector 对应 `max(1, parallelCount)` 个 goroutine。
- 轮询频率 / 退避 / lag / lagAlert / pauseAfterErrCount / parallelCount 均可单组件覆盖（各 `WithOptions`），否则取 `defaultOpts`（默认 500ms 轮询、1s 退避、30min lag 告警，`builder.go:18-26`、`options.go:22`）。

---

## 2. 相互触发关系：一条记录的完整旅程

事件驱动的核心闭环：**Store（事务写 record + outbox）→ outbox consumer 投递到 topic → 对应 topic consumer 处理 → 再次 Store**。

### 2.1 Trigger 入站（`trigger.go:22` 的 `trigger`）

1. 前置校验：`calledRun`（必须先 `Run`，否则 `trigger failed: workflow is not running`，`trigger.go:30`）；starting status 必须是图中合法节点（`statusGraph.IsValid`，`trigger.go:44`）。
2. `recordStore.Latest(name, foreignID)`：同 foreignID 若存在未 `Finished()` 的 run，返回 `ErrWorkflowInProgress`（`trigger.go:72-74`）——同一 foreignID 同时只允许一个活跃 run。
3. 生成 UUIDv4 runID，构造 `Record{RunState: Initiated(1), Status: startingStatus}`（`trigger.go:92`，Meta.Version 零值 0），经 `updateRecord`（`update.go:102`，先 `Version++` → 1）调 `Store`（`trigger.go:100`）。
4. `Store` 同事务写 outbox；`MakeOutboxEventData`（`event.go:75`）按 `RunState=Initiated` 选普通 status topic（`topic := Topic(...)`，`event.go:76`），事件头带 `HeaderRecordVersion=1`、`HeaderRunState=1`、`HeaderTopic` 等（`event.go:97-102`）。
5. outbox consumer 把事件发到 `<wf>-<startingStatus>` topic。

### 2.2 step 接力（`step.go` + `update.go`）

1. 该 status topic 上的 step consumer（以及同 topic 的 timeout auto-inserter）收到事件。`consume`（`consumer.go:29`）依次：检查 ctx → `Recv`（`consumer.go:45`）→ 可选 `ConsumeLag` 延迟等待（`consumer.go:51` 起）→ lag 指标/告警 → filters（分片等，`consumer.go:66-74`）→ 执行 `consumeFn`（`consumer.go:78`）→ 成功才 `ack()`（`consumer.go:83`）。**处理失败不 ack**，事件流游标不前进，形成至少一次投递。
2. `stepConsumer`（`step.go:105`）的幂等闸：
   - `Lookup(runID)`；`ErrRecordNotFound` 直接 ack 跳过（`step.go:121` 起）。
   - 版本校验（`step.go:145-151`）：`record.Meta.Version > 事件头版本` → 事件已处理，跳过（计数指标 "event record version lower..."）；`< 事件头版本` → 读到从库陈旧数据，返回 `stale record lookup` 错误重试；无版本头时回退为比对 record.Status（向后兼容，`step.go:136-142`）。
   - `record.RunState.Stopped()` → debug 日志 + 跳过（`step.go:153-164`）。
3. `buildRun`（`run.go:73`）：反序列化 Object；首次消费时把内存记录 `Initiated → Running`（`run.go:84`，仅内存态，随本次成功 Store 落盘）；从 `sync.Pool` 取 `Run` 并装配 `RunStateController`（`run.go:89-97`），用完 `defer runReleaser` 归还（`step.go:173`）。
4. 执行用户 `ConsumerFunc`（`step.go:177`）：
   - 返回 error：`maybePause`（`pause.go:13`，调用点 `step.go:178`）按 `(processName, runID)` 向 `ErrorCounter` 加一（`internal/errorcounter/errorcounter.go:19`，key 以 `\x00` 分隔，`errorcounter.go:44`）；未达 `PauseAfterErrCount` 则错误上抛，由 `runOnce` 记日志、`metrics.ProcessErrors` 加一、退避 `errBackOff` 后重连（`workflow.go:305-318`），同一条事件因未 ack 而重投。达到阈值则 `run.Pause`（`run.go:22` → `runStateControllerImpl.Pause`，`runstate.go:104`），RunState 改 `Paused` 并 Store，事件被 ack；计数随即 `Clear`（`pause.go:43`）。
   - 返回保留值 0 / `Skip()` / `Pause()` / `Cancel()`（`skipUpdate`，`status.go:13`）→ 不做普通状态更新，直接 ack（`step.go:199-211`；其中 Pause/Cancel 已通过 controller 写过 RunState）。
5. 正常返回 next status：`newUpdater` 返回的 updater（`update.go:21`）——重新 `Lookup` 最新记录（`update.go:43`）；已 `Finished()` 拒绝更新（`update.go:56-58`）；`latest.Meta.Version != workingVersion` 报 `record was modified since it was loaded`（**乐观锁**，`update.go:62-64`）；`SaveAndRepeat`（skipType -2）保持当前 status 存盘（`update.go:67-70`、`run.go:48`），产生同 topic 新事件重投自己；否则 `validateTransition`（`update.go:81`）用 `graph.Transitions` 校验边，非法迁移报错；next 为终态节点（`graph.IsTerminal`，`internal/graph/graph.go:76`）时 RunState 置 `Completed`，否则 `Running`（`update.go:33-35`）；`updateRecord`（`update.go:102`）推 `RunStateChanges` 指标、写 `Meta.StatusDescription`、`Version++`（`update.go:115`）后 `Store`。
6. `Store` 再写 outbox → 事件进入 `<wf>-<nextStatus>` → 下一 status 的 step consumer 接手。逐 status 推进直到 next 为图终态：RunState=`Completed`，事件改道 `RunStateChangeTopic`（`event.go:89-94`）。

### 2.3 timeout 介入（`timeout.go`）

- 记录每次进入某 status，该 status topic 上的 **auto-inserter** 与 step consumer 都收到事件。inserter 的内嵌 consumer（`timeout.go:237-260`）对每个 timeout 配置调 `TimerFunc(run, now)`（`timeout.go:244`）：返回零值时间表示本次不建 timeout；否则 `timeoutStore.Create(workflow, foreignID, runID, status, expireAt)`（`timeout.go:254`，接口 `store.go:41`）。它返回 status 0，所以不推进状态、也不产生额外 outbox 事件。
- **poller** 独立轮询（不经事件流）：`ListValid` 取该 status 所有已过期、未完成 timeout（`timeout.go:41`）。对每条：
  - `recordStore.Latest(workflow, foreignID)`（按 foreignID 取最新 run，`timeout.go:47`）；
  - 记录 status 已变或已 `Finished()` → timeout 失效，调 `timeoutStore.Cancel(id)`（`timeout.go:54`，内存实现是从切片移除，`adapters/memtimeoutstore/memtimeoutstore.go:94`）；
  - `Stopped()`（Paused 等）→ 本轮跳过、不 Cancel，恢复后仍可能再处理（`timeout.go:63-76`）；
  - 否则对该 status 上的每个 timeout 配置执行 `processTimeout`（`timeout.go:109`，调用点 `timeout.go:79`）：跑 `TimeoutFunc`（`timeout.go:129`）；其错误同样走 `maybePause`（`timeout.go:131`），但返回 nil（即 poller 不因单个业务错误整体退出，`timeout.go:139`）；成功且非 skip 时用同一个 `updater` 做乐观锁 + 图校验 + Store（`timeout.go:156`），**然后才** `timeoutStore.Complete(id)`（`timeout.go:162`，置 Completed=true）。
- 因此 timeout 与普通 step 一样通过 Store/outbox 驱动下一 status；若 `TimeoutFunc` 内调 `r.Cancel`，则走取消路径（见第 3 节）。

### 2.4 connector 派生关联记录（`connector.go`）

- connector consumer 消费的是**外部系统的流**（如 `adapters/reflexstreamer/connector.go`、`adapters/kafkastreamer/connector.go`、内存版 `adapters/memstreamer/connector.go`）。`connectorStreamer.Recv`（`connector.go:109`）把 `ConnectorEvent` 序列化进事件头 `HeaderConnectorData`，并把字符串 ID 哈希成 `Event.ID` 供 `shardFilter` 一致性分片（`connectorEventToEvent`，`connector.go:144`）。
- 消费函数（`connector.go:78-88`）反序列化出 `ConnectorEvent`（`streamerEventToConnectorEvent`，`connector.go:129`）后调用用户 `ConnectorFunc(ctx, api, e)`。引擎本身不自动建记录：关联记录由用户在函数内调用 `api.Trigger(ctx, e.ForeignID, ...)` 派生（`_examples/connector/connector.go:46-62`），foreignID 成为两条流之间的关联键（见 `API.Trigger` 注释，`workflow.go:18-25`）。Trigger 之后的链路与 2.1/2.2 完全相同。

### 2.5 hook 通知点（`hook.go`）

- 通知点不在进程内同步调用，而是**状态变更事件**：任何把 RunState 写成 `Paused/Cancelled/Completed/DataDeleted` 的 `Store`，其 outbox 事件都进 `RunStateChangeTopic`（`event.go:89-94`）。
- 每个 hook consumer 独立持有该 topic 的游标，用 `filterByRunState` 在 ack 前过滤掉非目标 RunState（`hook.go:52`）；`runHook`（`hook.go:57`）按事件 ForeignID（= runID）`Lookup` 最新记录，反序列化后把 `*TypedRecord` 交给用户函数（`hook.go:65-79`）。hook 自身的错误同样不 ack、退避重试（经 `consume`/`runOnce`）。

### 2.6 一次带 connector 与 hook 的端到端串联

1. 外部系统产生事件 → connector consumer（sharded）`Recv` → 用户 `ConnectorFunc` 调 `api.Trigger(foreignID)`（`connector.go:87`）。
2. `Trigger` 写 Initiated record + outbox（2.1）→ outbox consumer 投递到起始 status topic。
3. 该 status 的 auto-inserter 建 timeout；step consumer 执行用户逻辑，经乐观锁 updater 写 status=S1/Running + outbox。
4. outbox 投递到 S1 topic；S1 step consumer 推进到终态 status=Done；updater 发现 Done 是终态节点 → RunState=Completed（`update.go:33-35`），事件进 `run-state-change` topic。
5. `OnComplete` hook consumer 收到事件并执行用户 hook。
6. 若第 3 步用户代码持续报错达到阈值 → `maybePause` 写 RunState=Paused（事件进 run-state-change）→ `OnPause` hook 触发；默认 1 小时后 paused-retry consumer `Resume` 回 Running（`pause.go:121`，转移表 `runstate.go:142-144`）。Resume 的 Store 产生 RunState=Running 事件回到**当前 status topic**（Running 不在改道集合，`event.go:89`），step consumer 重新处理。

---

## 3. 到终态的全部路径

### 3.1 成功完成：Running → Completed

- 路径：step（或 timeout / callback）返回一个被 `statusGraph` 标记为 terminal 的 status。终态判定来自图结构：`AddTransition(from, to)` 中没有出边的 `to` 节点自动成为 terminal（`internal/graph/graph.go:33` 的 `AddTransition`，置位在 `graph.go:60`）；`newUpdater` 中 `graph.IsTerminal(next)` 为真则 `runState=RunStateCompleted`（`update.go:33-35`）。
- 涉及组件：step consumer（`step.go`）/ timeout poller（`processTimeout`，`timeout.go:109`）/ 同步 `Callback`（`callback.go:103`）任一写入；outbox consumer 把事件改道 `RunStateChangeTopic`；`OnComplete` hook consumer 消费。
- Completed 之后：step/timeout 都不会再处理（`Finished()`/`Stopped()` 检查；updater 也拒绝更新已 Finished 记录，`update.go:56-58`）。允许的唯一后续转移是 `→ RequestedDataDeleted`（`runstate.go:146-150`）。

### 3.2 失败重试耗尽：Running → Paused（→ 自动恢复或人工处理）

- 引擎**没有“重试耗尽即失败/取消”**的终态；重试耗尽的语义是进入 `RunStatePaused`：
  - step 错误：`step.go:177-197` 调 `maybePause`（`pause.go:13`），同一 `(processName, runID)` 连续错误计数达到 `PauseAfterErrCount` 后 `run.Pause`（`run.go:22` → `runstate.go:104`）。
  - timeout `TimeoutFunc` 错误：`processTimeout` 内同样调 `maybePause`（`timeout.go:131`）。
  - 计数为进程内内存（`internal/errorcounter/errorcounter.go`），Pause 成功后 Clear（`pause.go:43`）；`PauseAfterErrCount=0`（默认）时永不暂停、无限退避重试（`pause.go:22`、`options.go:76-79` 注释）。
- Paused 后：所有事件流消费者见 `Stopped()` 跳过；timeout poller 见 Stopped 跳过且不 Cancel timeout（`timeout.go:63`）。
- 恢复：默认由 paused-records-retry consumer 在 `resumeAfter`（默认 1h）后 `Resume`（`autoRetryConsumer`，`pause.go:99-124`）；也可通过 `RunStateController.Resume`（外部 API / `Await` 拿到的 Run）人工恢复。转移表允许 `Paused → Running / Cancelled`（`runstate.go:142-144`）。

### 3.3 显式取消：Running/Paused → Cancelled

- 用户代码内：`return r.Cancel(ctx, reason)`（`run.go:39`），controller 校验转移后写 RunState=Cancelled 与 `Meta.RunStateReason`（`runstate.go:112`、`runstateControllerImpl.update`，`runstate.go:120`），返回 skipType=-1，外层 ack 且不再更新 status。
- 外部：对 `Await`/API 暴露的 `RunStateController.Cancel`（`runstate.go:112`；`Await` 返回的 Run 带 controller，`await.go:100-106`）。
- 合法前置状态仅 Running / Paused（`runstate.go:137-144`）；Cancelled 不可逆，只能 `→ RequestedDataDeleted`（`runstate.go:149-150`）。
- 事件进 run-state-change topic，`OnCancel` hook 消费；其他 consumer 因 `Stopped()`/`Finished()` 停手；timeout poller 下一轮把该记录残留的已过期 timeout `Cancel` 掉（`timeout.go:54`）。

### 3.4 超时“取消”：timeout 驱动的两条岔路

- timeout 本身不是取消动作。到期后 `TimeoutFunc` 返回一个普通 status → 走正常推进/3.1（`timeout.go:156-162`），并 `timeoutStore.Complete`。
- 若要“超时取消”，需在 `TimeoutFunc` 内调 `r.Cancel`（或持续返回错误达到阈值后走 3.2 的 Paused）。取消后 RunState 事件、hook、timeout 清理行为与 3.3 相同。
- 记录已离开该 status 时 timeout 不再有效：poller 主动 `timeoutStore.Cancel`（`timeout.go:54`）。

### 3.5 删除清理：Completed/Cancelled → RequestedDataDeleted → DataDeleted

1. 入口：仅 `RunStateController.DeleteData(reason)`（`runstate.go:116`），转移表要求当前为 Completed 或 Cancelled（`runstate.go:146-150`）。这是同步 API 调用，把 RunState 写成 `RequestedDataDeleted(7)`（仍属 `Finished()`）。
2. 该次 Store 的 outbox 事件在 `MakeOutboxEventData` 中被**优先改道到 `DeleteTopic`**（`event.go:81-83`，先于 run-state-change 判断）。
3. delete consumer（`delete.go:7`）消费：`runDelete`（`delete.go:45`）`Lookup` 最新记录，执行 `customDelete`（`WithCustomDelete`，`builder.go:356`，用于 PII 脱敏）或用默认 `{"result":"deleted"}` 覆盖 `Object`，写 RunState=`DataDeleted(6)`，previousRunState 记为 `RequestedDataDeleted`（`delete.go:60-70`）。
4. 第二次 Store 的事件这次走 run-state-change topic（DataDeleted 在改道集合内，`event.go:89`），仅供观测（注意没有 OnDelete hook；paused-retry consumer 的 filter 会跳过它）。
5. 转移表允许 `DataDeleted → RequestedDataDeleted`（`runstate.go:155-157`），即可重复请求删除；二者都 `Finished()`，记录此后不再被任何业务 consumer 处理。

### 3.6 终态后的资源回收做什么 / 不做什么

- delete consumer 做的是**业务数据擦写**（Object 覆写 + RunState 标记），不是物理删除 record：record 行仍保留（SQL 实现为按 run_id upsert，`adapters/sqlstore/sqlstore.go:48-87`），以保留审计轨迹。
- 进程侧资源：每次消费结束 `Run` 对象归还 `sync.Pool`（`step.go:173` 的 `defer runReleaser(run)`、`timeout.go:127`）；事件流 receiver 在 `w.run` 闭包退出时 `defer stream.Close()`（如 `step.go:75`、`hook.go:36`、`delete.go:26`）。
- timeout 记录：Complete 置位（ListValid 排除）或 Cancel 物理移除；没有专门的终态 GC goroutine。
- outbox 行：投递成功即 `DeleteOutboxEvent`（`outbox.go:165`；SQL 为 delete，`adapters/sqlstore/sqlstore.go:124`）。

---

## 4. 停机顺序与错误隔离

### 4.1 Stop 流程（`workflow.go:323`）

1. `Stop()` 取 `w.cancel`（`Run` 时保存的、派生自用户 ctx 的 parent context 的 CancelFunc，`workflow.go:115-122`）；未 Run 过直接返回。
2. `cancel()` 一次性取消所有组件的 parent context——**没有差异化关停顺序**：outbox、全部 step shard、两类 timeout goroutine、connector、hook、delete、paused-retry 同时收到取消。
3. 之后轮询 `w.States()`（`state.go:44`），每 10ms 一轮，直到不存在 `StateUnknown/StateShutdown` 之外的进程（`workflow.go:335-352`，sleep 在 `workflow.go:351`）。等待是**无超时、无强制 kill** 的：`Stop` 会一直阻塞到每个 goroutine 的 `w.run` defer 把状态置为 `StateShutdown`（defer 在 `workflow.go:228`）。
4. 各 goroutine 的退出路径：
   - 事件流类：`consume` 在 `Recv`、lag timer、下一轮 ctx 检查处返回 `ctx.Err()`（`consumer.go:31-33` 等）→ process 返回 context.Canceled → `runOnce` 返回 nil（`workflow.go:296-300`，注释说明 role 被 scheduler 取消时也要回去重新抢 role，而 parent ctx 已取消则下一轮在 `runOnce` 开头 `ctx.Err()!=nil` 直接退出，`workflow.go:273-276`）→ `w.run` for 循环结束 → defer `StateShutdown`。
   - 轮询类：`pollTimeouts`/`purgeOutbox` 在循环头或 `wait(ctx, d)`（`step.go:217`）处感知 ctx 取消。
   - receiver 的 `defer stream.Close()` 与 connector 的 `defer consumer.Close()`（`connector.go:74`）在闭包返回时执行；outbox sender 在 `purgeOutbox` 的 defer 中统一 Close（`outbox.go:124-127`）。
5. 在途记录处理：组件只通过 ctx 协作式取消；已经进入用户函数且不响应 ctx 的执行会拖住 `Stop`（无超时兜底）。
6. `Schedule` 走同一 `w.run` 外壳、受同一 parent ctx 控制（`schedule.go:39`），但其生命周期由调用方阻塞持有；`Await`/`Callback` 使用各自传入的 ctx，不在 `Stop` 等待集合里。

### 4.2 错误隔离（`runOnce`，`workflow.go:261`）

- 每个组件 goroutine 独立跑在自己的 `w.run` 循环里，互不持有对方引用，错误只影响自身这一路：
  - `RoleScheduler.Await` 出错：记 error 日志后返回 nil → 下一轮重新抢角色（`workflow.go:285-291`）。
  - process（Recv/业务/Store 等）出错：`logger.Error` + `metrics.ProcessErrors{workflow,processName}` 加一（`workflow.go:305-306`），然后用 `clock.NewTimer(errBackOff)` 退避；退避期间 ctx 取消则退出，否则返回 nil 重连（`workflow.go:307-318`）。
  - 返回 `context.DeadlineExceeded`：干净退出（`workflow.go:281-284`）。
- 业务级错误的第二层吸收：`maybePause` 把“同一 run 反复失败”转化为 Paused 状态而非让错误无限刷屏（3.2 节）；各类“跳过”事件（filtered、record not found、版本已超前、record stopped、skip 指令）只打 debug 日志/`ProcessSkippedEvents` 指标并正常 ack，不上抛（见 `consumer.go:66-74`、`step.go:121-164`、`step.go:199-211`）。
- 因为错误发生后事件未 ack，重连后由事件流重投；updater 的版本检查保证重复投递不会覆盖更新的状态（5.2 节）。
- errorCounter 按 `processName + runID` 隔离内存计数（`internal/errorcounter/errorcounter.go:44`），一个 run 的错误计数不影响其他 run，也不跨进程实例共享。

### 4.3 panic 隔离：实际不存在

- 全仓（含 `internal/` 与 `adapters/`）没有任何 `recover()` 调用：`track` 只是裸 `go fn()`（`workflow.go:215-218`），`w.run`/`runOnce`/`consume` 均无 defer recover。
- 因此用户 `ConsumerFunc`/`TimeoutFunc`/`ConnectorFunc`/hook 或适配器代码中的 panic 会直接崩溃整个进程，**不存在单 consumer panic 被隔离的机制**；panic 发生后 `StateShutdown` 的 defer 也不会执行，`Stop()` 的状态轮询无法反映这种非正常死亡。这是复核时需要特别注意的行为边界。

---

## 5. 存储交互

### 5.1 RecordStore 的读写模式

接口定义见 `store.go:11`，关键约束：`Store` 必须在事务内同时提交 record 与 outbox 行，并且在创建前能拿到 outbox ID（`store.go:12-16` 注释）。

- `Store(ctx, *Record)`：create-or-update（SQL 实现按 `run_id` 查询决定 insert/update，`adapters/sqlstore/sqlstore.go:55-79`），同事务调 `MakeOutboxEventData` + `insertOutboxEvent`（`adapters/sqlstore/sqlstore.go:80-87`），最后 `tx.Commit()`。调用方只有两类：`updateRecord`（`update.go:102`，所有引擎内状态迁移的唯一出口，Version++）与 `trigger`（`trigger.go:100`）。
- `Lookup(runID)`：按 runID 取单行，事件流消费者（step、hook、delete、paused-retry）使用；事件的 `Event.ForeignID` 实际承载 runID（outbox 投递时 `foreignID := outboxRecord.RunId`，`outbox.go:140`）。
- `Latest(workflow, foreignID)`：按 foreignID 取最新 run，`Trigger` 用它拒绝并发 run（`trigger.go:64-74`），timeout poller 用它关联最新记录（`timeout.go:47`），`Callback` 同步路径也用它（`callback.go:56`）。
- `ListOutboxEvents` / `DeleteOutboxEvent`：outbox consumer 专用，批量轮询 + 成功后逐条删除（`outbox.go:113`、`outbox.go:165`）。
- `List(...)`：面向 Web UI / 调试（如 `adapters/webui/internal/api/list.go`），后台组件不使用。

三种访问模式并存：

1. **事件流监听（持久游标 + ack）**：step、timeout-inserter、connector、hook、delete、paused-retry。游标按 receiver name（= role）保存，因此同一 role 名字在多实例间必须由 RoleScheduler 保证唯一持有者（`rolescheduler.go:18-24`；角色名含 workflow 名、status、shard 编号）。
2. **定时轮询**：timeout poller 轮询 TimeoutStore；outbox consumer 轮询 outbox 表；Await/Schedule 是 API 侧的轮询/定时。
3. **同步读写**：Trigger、Callback、RunStateController（Pause/Resume/Cancel/DeleteData）。

### 5.2 乐观锁与“避免重复处理同一条记录”的多层机制

- 事件-记录版本对齐：事件头 `HeaderRecordVersion` 在 Store 时写入当前 Version（`event.go:102`）。消费时三层判断（`step.go:122-151`）：record 版本更高→事件过期直接 ack 跳过；更低→从库陈旧报错重试；相等→继续。这同时抵御了重投与只读副本复制延迟。
- 提交前二次校验（`newUpdater`，`update.go:43-64`）：处理期间重新 `Lookup`，若 `latest.Meta.Version != workingVersion` 则拒绝写入；并拒绝更新 `Finished()` 记录。配合 `updateRecord` 的 `Version++`（`update.go:115`）构成乐观并发控制。
- 图校验：`validateTransition`（`update.go:81`）保证只有 builder 声明过的边能落库。
- RunState 闸：`Stopped()` 记录在 step（`step.go:153`）、timeout poller（`timeout.go:63`）、callback（`callback.go:66`）中被跳过。
- 分片去重：`parallelCount` 分片按 `event.ID % totalShards`（`eventfilter.go:54-60`），一条事件恒定只归一个 shard；connector 用事件字符串 ID 的 FNV 哈希达到同样效果（`connector.go:144-160`）。
- 游标去重：成功处理才 ack（`consumer.go:83`）；失败重投由上述版本/状态闸吸收。timeout 侧用 `Completed` 标记 + Cancel 物理删除防重复执行（`timeout.go:54`、`timeout.go:162`）。
- timeout poller 与 step 之间天然串行化在记录层：两者迁移状态都走同一 updater 的版本检查，先到者成功、后到者版本不符而重试（下一轮即发现 status 已变并 Cancel timeout）。

### 5.3 outbox 的角色

- outbox 是 RecordStore（业务状态）与 EventStreamer（通知/扇出）之间的**事务性桥梁**：状态落库与“产生一条待发事件”原子提交，避免“库已改、消息没发”或反之（接口注释 `store.go:12-16`）。
- topic 路由也在 outbox 载荷构造时决定：`MakeOutboxEventData`（`event.go:75`）依据 RunState 把事件分发到 status topic / DeleteTopic / RunStateChangeTopic，并附全部路由与幂等所需 header。
- outbox consumer 只是搬运工：`ListOutboxEvents → proto 解码（outboxpb.OutboxRecord）→ 按 HeaderTopic 缓存复用 EventSender → Send → DeleteOutboxEvent`（`outbox.go:113-167`）。它**不做业务判断、不分片**（单实例，靠 RoleScheduler 选主）。
- 可关闭：当 RecordStore 自带集中式 outbox 清理时，`WithoutOutbox()`（`builder.go:305`）让本 workflow 不启动 outbox consumer（`workflow.go:125`），改由外部把 outbox 行投递到 EventStreamer。此时事件契约（headers、topic 路由）仍需与 `MakeOutboxEventData` 保持一致。
- 适配器佐证：内存版 RecordStore 同样实现 outbox 并提供 `PurgeOutboxForever` 辅助（`adapters/memrecordstore/outbox.go:19`）；SQL 版 outbox 表的 insert/list/delete 见 `adapters/sqlstore/sqlstore.go:80-130`。

---

## 6. 记录状态迁移 × 组件职责对照总表

| 触发点 / Record 状态变化 | 写入者（组件/API） | 落库函数 | outbox 事件去向（`event.go:75`） | 接手的后台组件 | 关键防护 |
|---|---|---|---|---|---|
| 不存在 → Initiated(1) | `Trigger`（`trigger.go:90-100`，可由 connector func、Schedule、用户调用发起） | `updateRecord` + `Store` | 起始 status topic（`event.go:76`） | step consumer；timeout auto-inserter | 同 foreignID 活跃 run 检查（`trigger.go:72`） |
| Initiated → Running(2) | step consumer 首次消费改内存态后随首次成功迁移一起 Store | `newUpdater`（`update.go:21`） | 当前/下一 status topic | 下一 status 的 step consumer + auto-inserter | 版本校验 + 乐观锁 + 图校验 |
| Running → Running（同 status 重投） | step 返回 `SaveAndRepeat`（`run.go:48`） | `isSaveAndRepeat` 分支（`update.go:67`） | 同 status topic | 同 status step consumer | Version++，旧事件被版本闸丢弃 |
| Pause/Cancel/Skip 返回 | `RunStateController`（`runstate.go:104-128`） | `updateRecord` | 见各自行；Skip(0) 不写库 | — | skipType 不触发普通 updater（`status.go:13`） |
| Running → Paused(3) | step/timeout 错误累计达阈值（`maybePause`，`pause.go:13`）或显式 `r.Pause` | `controller.Pause` | run-state-change topic（`event.go:89`） | OnPause hook；paused-retry consumer | `Stopped()` 后其他 consumer 跳过 |
| Paused → Running(2) | paused-retry consumer（`pause.go:121`，默认 1h）或外部 Resume | `controller.Resume`（`runstate.go:108`） | 当前 status topic（Running 不改道） | step consumer 重新处理 | 仅 Paused 可 Resume（`runstate.go:142`） |
| Running/Paused → Cancelled(4) | 用户 `r.Cancel`（`run.go:39`）/外部 controller；或在 TimeoutFunc 内取消 | `controller.Cancel`（`runstate.go:112`） | run-state-change topic | OnCancel hook；timeout poller 清理残留 timeout | 取消不可逆；`Finished()` 拒绝再更新 |
| Running → Completed(5) | step/timeout/callback 返回图终态 status | `newUpdater`（`update.go:33-35`） | run-state-change topic | OnComplete hook；Await（终态听该 topic，`await.go:41`） | terminal 由图的出边自动推导（`graph.go:60`） |
| Completed/Cancelled → RequestedDataDeleted(7) | 外部 `RunStateController.DeleteData`（`runstate.go:116`） | `controller.DeleteData` | **DeleteTopic**（优先于 run-state-change，`event.go:81`） | delete consumer | 仅终态可请求删除；仍属 Finished |
| RequestedDataDeleted → DataDeleted(6) | delete consumer（`delete.go:45`）覆写 Object | `runDelete` + `updateRecord`（`delete.go:68`） | run-state-change topic | 仅观测；无业务 consumer | customDelete 失败则不 ack、重试 |
| DataDeleted ↔ RequestedDataDeleted | 再次 DeleteData / delete consumer | 同上 | 同上 | delete consumer | 允许重复擦除请求（`runstate.go:155`） |
| （旁支）外部 connector 事件 | connector consumer（`connector.go:35`） | 自身不写 RecordStore；通常由 `api.Trigger` 派生 | 外部流，无 outbox | 派生 run 后进入第一行 | shardFilter on FNV(event.ID)（`connector.go:144`） |
| （旁支）timeout 到期 | timeout poller（`pollTimeouts`，`timeout.go:24`） | `processTimeout` → updater/controller；成功后 `timeoutStore.Complete` | 按迁移结果同普通状态事件 | 对应 status consumer / hooks | 记录已离开 status → `timeoutStore.Cancel`（`timeout.go:54`） |

---

## 7. 复核索引

- 启动与关停：`workflow.go:112`（`Run`）、`workflow.go:215`（`track`）、`workflow.go:221`（`run`）、`workflow.go:261`（`runOnce`）、`workflow.go:323`（`Stop`）。
- 消费循环：`consumer.go:29`（`consume`，lag/filter/ack/指标）。
- 状态机：`runstate.go:13`（常量）、`runstate.go:90`（`NewRunStateController`）、`runstate.go:132`（转移表）；图终态推导：`internal/graph/graph.go:33`。
- 事件/outbox：`event.go:75`（`MakeOutboxEventData` 的 topic 路由）、`outbox.go:102`（`purgeOutbox`）、`topic.go:13`。
- 存储契约：`store.go:11`（RecordStore）、`store.go:40`（TimeoutStore）、`adapters/sqlstore/sqlstore.go:48`（事务 Store）。
- 可选组件开关：`builder.go:254`（timeoutStore 缺失 panic）、`builder.go:305`（WithoutOutbox）、`builder.go:380`/`builder.go:389`（pause retry）、`builder.go:204`（hooks）。
