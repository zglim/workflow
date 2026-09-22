# Workflow 引擎运行时后台组件全景（代码理解文档）

> 本文所有结论均以当前仓库源码为准，引用格式为 `路径:函数名`。核心逻辑在根目录的 `package workflow`，辅助工具位于 `internal/`。

---

## 0. 运行时总体模型

Workflow 是基于「记录（Record）+ 事务发件箱（Transactional Outbox）+ 事件流（EventStreamer）+ 角色调度（RoleScheduler）」的状态机运行时：

- 每次 `Trigger` / Callback / Timeout 推进都会 upsert 一条 `Record`（`record.go:Record`）。`RecordStore.Store` 在**同一事务**内写记录和一条 outbox 事件（`adapters/sqlstore/sqlstore.go:Store` 调 `workflow.MakeOutboxEventData` + `insertOutboxEvent` 后 `Commit`，见 `adapters/sqlstore/util.go`）。
- `outboxConsumer` 轮询 outbox 表，把事件投递到 `EventStreamer` 的具体 topic，投递成功后删除 outbox 行（`outbox.go:purgeOutbox`）。
- 各后台 consumer 以固定 role 名在 `EventStreamer` 上建立 receiver（带持久消费游标），收到事件后回查 `RecordStore`，执行业务逻辑，再用**版本号乐观锁**提交新记录；新记录又产生新事件，驱动下一状态的 consumer。
- `RoleScheduler.Await` 保证同一 role 全局只有一个持有者（`rolescheduler.go:RoleScheduler`；内存实现见 `adapters/memrolescheduler/memrolescheduler.go:Await`，每个 role 一把互斥锁），用于多实例部署选主。

三类 topic（`topic.go`，路由逻辑在 `event.go:MakeOutboxEventData`）：

| Topic | 生成条件 | 消费者 |
| --- | --- | --- |
| `<workflow>-<status>`（`Topic`） | RunState 为 `Initiated/Running` 的普通状态更新 | step consumers、timeout auto-inserter、`Await` |
| `<workflow>-run-state-change`（`RunStateChangeTopic`） | RunState 变为 `Paused/Cancelled/Completed/DataDeleted` | run-state hooks、paused retry consumer |
| `<workflow>-delete`（`DeleteTopic`） | RunState 变为 `RequestedDataDeleted` | delete consumer |

---

## 1. 组件清单与启动（`Workflow.Run`）

启动入口：`workflow.go:(*Workflow).Run`。整体包在 `w.once.Do` 中（重复调用为 noop）；先创建可取消父 context 并置 `calledRun=true`，再按固定顺序用 `track(w, func(){...})` 启动 goroutine；`track`（`workflow.go:track`）先 `w.launching.Add(1)` 再 `go fn()`，`Run` 末尾 `w.launching.Wait()` 阻塞到每个 goroutine 都已在 `w.run` 中登记进 `internalState` 后才返回（注释说明这是为了消除"goroutine 已启动但尚未登记状态"的非确定性窗口）。

每个 goroutine 的统一外壳是 `w.run(role, processName, process, errBackOff)`（`workflow.go:run` → `runOnce`）：进入置 `StateIdle` → `scheduler.Await(ctx, role)` 抢角色 → 抢到置 `StateRunning` 并执行 `process(ctx)`；出错则记日志、打 `metrics.ProcessErrors`、按 `errBackOff` 用 timer 退避后循环重抢角色；父 context 取消时退出并置 `StateShutdown`。`runOnce` 对 `context.Canceled`/`DeadlineExceeded` 做了干净退出处理。

### 1.1 组件总表

| # | 组件 / goroutine 入口 | 职责 | 启动条件 | 监听对象与消费方式 |
| --- | --- | --- | --- | --- |
| 1 | `outbox.go:outboxConsumer` → `purgeOutbox` | 轮询 RecordStore outbox 表，反序列化后按 header 中的 topic 投递事件到 EventStreamer，成功后删除 outbox 行 | **可选**：默认启用；`builder.go:WithoutOutbox()` 置 `outboxConfig.disabled=true` 时不启动（用于存储侧自行清理 outbox 的中心化部署） | `RecordStore.ListOutboxEvents` 主动轮询（默认 250ms，单批 `limit=1000`，见 `defaultOutboxConfig`）；单消费者、不分片，靠 RoleScheduler 选主 |
| 2 | `step.go:consumeStepEvents` | 主状态机消费者：Lookup 记录 → 版本/RunState 校验 → 执行 `ConsumerFunc` → `newUpdater` 提交下一状态（终态置 Completed） | 每个 `builder.go:Builder.AddStep(from,...)` 注册项一个；无 AddStep 则没有 | receiver 订阅 `Topic(name,currentStatus)`；`parallelCount<2` 单实例，否则启动 `shard=1..N` 共 N 个 goroutine，用 `eventfilter.go:shardFilter`（`e.ID % totalShards == shard-1`）一致性分片；可配 polling 频率、lag、lagAlert、errBackOff、pauseAfterErrCount |
| 3a | `timeout.go:timeoutPoller` → `pollTimeouts` | 轮询**已到期且有效**的 timeout，执行 `TimeoutFunc` 推进状态，并把 timeout 标记 Complete/Cancel | **可选**：仅当 `w.timeoutStore != nil` 时启动（`Run` 中条件判断；`Build` 中配了 timeout 却没给 store 会 panic）；每个配置了 `AddTimeout` 的 status 一个 | `TimeoutStore.ListValid(workflow,status,now)` 主动轮询（默认 500ms）；不订阅事件流、不分片；role `...-timeout-consumer` |
| 3b | `timeout.go:timeoutAutoInserterConsumer` | 订阅对应 status 主题，对每条记录跑 `TimerFunc`，把到期时间 `TimeoutStore.Create` 落库；自身永不改状态（返回 0 走 skip） | 同 3a，与 poller 成对启动 | receiver 订阅 `Topic(name,status)`；复用 `step.go:stepConsumer` 全套框架（版本校验、maybePause、Run 对象池）；单实例 |
| 4 | `connector.go:connectorConsumer` | 消费**外部** connector 流（`ConnectorConstructor.Make`），把外部 `ConnectorEvent` 交给用户 `ConnectorFunc`；典型用法是在回调里调 `api.Trigger`，以外部事件的 `ForeignID` 派生/启动本 workflow 记录（`_examples/connector/connector.go`） | **可选**：每个 `builder.go:AddConnector` 一项 | 直接从外部 `ConnectorConsumer.Recv` 拉取，`connectorEventToEvent` 做 FNV 哈希（事件 ID 仅用于分片）并把原始 JSON 放进 `HeaderConnectorData`；`parallelCount>=2` 时 `shardFilter` 分片；支持 lag/lagAlert；**不经过本 workflow 的 outbox** |
| 5 | `hook.go:runStateChangeHookConsumer` → `runHook` | 在记录进入 `Paused/Cancelled/Completed` 时回调用户 hook，入参为反序列化后的 `TypedRecord` | **可选**：仅注册了对应 hook 才启动——`builder.go:OnPause/OnCancel/OnComplete` 各向 `runStateChangeHooks` map 注册一项，每个 RunState 一个 goroutine | receiver 订阅 `RunStateChangeTopic`，用 `eventfilter.go:filterByRunState(runState)` 在公共主题上只取本 hook 关心的 RunState；不分片 |
| 6 | `delete.go:deleteConsumer` → `runDelete` | 处理删除请求：Lookup 记录，用默认 `{"result":"deleted"}` 或 `WithCustomDelete` 提供的函数擦写 Object，置 RunState=`DataDeleted` | **始终启动**（`Run` 中无条件 `track`） | receiver 订阅 `DeleteTopic`；单实例、不分片 |
| 7 | `pause.go:pausedRecordsRetryConsumer` → `autoRetryConsumer` | 自动恢复暂停记录：只处理 RunState=Paused 且暂停已持续 `resumeAfter` 的记录，调 `RunStateController.Resume` 置回 Running | **可选**：默认启用（`defaultPausedRecordsRetry`：enabled、1h）；`builder.go:DisablePauseRetry()` 关闭；`WithPauseRetry(d)` 调时长 | receiver 订阅 `RunStateChangeTopic`，`filterByRunState(Paused)` 过滤，并通过 `consume` 的 `lag=resumeAfter` 参数让事件"老化"到点才处理（`consumer.go:consume` 的 lag 等待）；lagAlert 取 `resumeAfter*3`（不足 1 分钟时取 resumeAfter+5min） |

补充说明：

- `Schedule`（`schedule.go:(*Workflow).Schedule`）虽也用 `w.launching.Add(1)` + `w.run` 起 goroutine，但它是用户显式调用的 API（cron 定时 Trigger），**不在 `Run` 启动清单**；`Await`（`await.go`）、`Callback`（`callback.go`）同理是按需 API，非常驻组件。
- 配置继承：polling 频率、errBackOff、lagAlert、parallelCount、pauseAfterErrCount 先取 `w.defaultOpts`（`WithDefaultOptions`；默认 polling 500ms、backoff 1s、lagAlert 30min，常量见 `builder.go`），再被单组件 `WithOptions` 覆盖（`step.go:consumeStepEvents`、`timeout.go`、`connector.go` 中同构的覆盖逻辑）。
- 命名区分：role 含 workflow 名、**数字** status、shard 编号，是持久化到 EventStreamer offset / 选主的稳定标识；processName 含 status 的**字符串**形式，仅用于日志与 metrics。各组件源码注释均明确 processName 不可持久化。

---

## 2. 一条记录的完整触发链路（含 connector 与 hook）

场景：外部 connector 事件 → Trigger → 若干 step（其中一步配了 timeout）→ 完成 → OnComplete hook。

### 2.1 connector 派生记录

1. `connectorConsumer`（`connector.go`）从外部流 `Recv` 到 `ConnectorEvent`，`connectorEventToEvent` 用 FNV 哈希生成 Event.ID 并把原始 JSON 放进 `HeaderConnectorData`，交给通用循环 `consumer.go:consume`（lag 等待 → 分片过滤 → 业务函数 → ack）。
2. 用户 `ConnectorFunc` 调 `api.Trigger(ctx, e.ForeignID, ...)`（`trigger.go:(*Workflow).Trigger` → 包级函数 `trigger`）。
3. `trigger` 检查 `calledRun`，用 `RecordStore.Latest(workflow, foreignID)` 查上一条记录：若上一 run 的 RunState `Valid() && !Finished()`，返回 `ErrWorkflowInProgress`（Finished = Completed/Cancelled/RequestedDataDeleted/DataDeleted，`runstate.go:Finished`）。
4. 构造新 `Record`：新 UUIDv4 RunID、RunState=`Initiated`、Status=起始 status（图的第一个 starting node，或 `WithStartingPoint` 指定），经 `update.go:updateRecord` 将 `Meta.Version` 置 1 并 `Store`——记录与 outbox 事件在一个事务落库（`adapters/sqlstore/util.go`：create/update + `insertOutboxEvent` + `Commit`）。

### 2.2 outbox 搬运

5. `purgeOutbox`（`outbox.go`）`ListOutboxEvents` 取待发表，proto 反序列化 `outboxpb.OutboxRecord`，按 header `HeaderTopic` 缓存/复用 `EventSender`，`Send(runID, statusType, headers)` 发到 `Topic(name,起始status)`，然后 `DeleteOutboxEvent`。注意事件的 foreignID 字段放的是 **RunID**（`purgeOutbox` 中 `foreignID := outboxRecord.RunId`），后续 consumer 都按 RunID 回查。

### 2.3 第一个 step consumer 接手

6. `consumeStepEvents` 的 receiver 收到事件。`consumer.go:consume` 先做分片过滤（`shardFilter`），再调 `step.go:stepConsumer`：
   - `Lookup(runID)`：记录不存在直接 ack 跳过（metric `record not found`）；
   - 用 header `HeaderRecordVersion` 与 `record.Meta.Version` 对账：事件版本 **小于** 记录版本 → 事件已处理过，跳过；事件版本 **大于** 记录版本 → 读到陈旧副本，返回错误触发重试（复制延迟防护，见 `record.go:Meta.Version` 注释）；
   - `record.RunState.Stopped()`（Paused/Cancelled/RequestedDataDeleted/DataDeleted）→ 跳过；
   - `run.go:buildRun` 反序列化 Object 到从 `sync.Pool` 取出的 `Run`；若记录是 `Initiated` 在此刻翻成 `Running`（defer `releaseRun` 归还对象池）。
7. 执行用户 `ConsumerFunc`。成功且返回值不是 skip 哨兵（`status.go:skipUpdate`：0=`Skip`、-1=run-state 更新）时，调 `update.go:newUpdater`：
   - 重新 `Lookup(runID)`：已 Finished 则拒绝更新；
   - **乐观锁**：`latest.Meta.Version != workingVersion` 则报 "record was modified since it was loaded"，本次提交失败、重试；
   - `validateTransition` 用 `internal/graph` 校验 current→next 是注册过的边；
   - `SaveAndRepeat`（-2）是特例：保持当前 status 落库（版本仍 +1、仍产生同 status 事件），实现"保存后重新排队处理自己"；
   - 终态判定 `graph.IsTerminal(next)`（没有出边的节点，`internal/graph/graph.go:AddTransition` 维护）为 true 时 RunState 置 `Completed`，否则 `Running`；
   - `updateRecord` version+1 后 `Store`，同事务写新 outbox 事件。
8. `consume` 只在业务函数成功后才 `ack()`；任何错误都不 ack，依赖 EventStreamer 重投（at-least-once + 版本对账实现幂等）。

### 2.4 timeout 组件并行介入

9. 同一条 status 事件也被 `timeoutAutoInserterConsumer` 收到（独立 role、独立游标）：对每个 timeout 配置跑 `TimerFunc`，返回零时间则跳过，否则 `TimeoutStore.Create(workflow, foreignID, runID, status, expireAt)`；函数最终返回 0，不产生状态更新。
10. `timeoutPoller` 周期性 `ListValid(workflow, status, now)` 取到期 timeout：每条用 `Latest` 查最新记录——status 已离开或已 Finished → `TimeoutStore.Cancel` 作废；处于 Stopped（Paused 等）→ debug 日志后跳过；否则 `processTimeout`（`timeout.go`）执行 `TimeoutFunc`，经**同一个** `newUpdater` 提交迁移，成功后 `TimeoutStore.Complete(id)`。因此 timeout 与 step 是"谁先满足条件谁推进"的竞争关系：版本乐观锁 + status 校验保证只有一方推进生效，失败方下一轮轮询时把失效 timeout Cancel 掉。

### 2.5 级联到终态并通知 hook

11. step 提交到中间 status → outbox 发 `Topic(name,中间status)` → 该 status 的 step consumer（及 auto-inserter）接手，重复 6–8。
12. 最后一个 step 返回终态 status → updater 写 RunState=`Completed`；`MakeOutboxEventData`（`event.go`）发现是终态 RunState，把 topic 改写为 `RunStateChangeTopic`，header 带 `run_state=5` 与新的 `record_version`。
13. `runStateChangeHookConsumer(Completed)` 经 `filterByRunState(Completed)` 取到事件，`runHook`（`hook.go`）Lookup 记录、反序列化 Object，执行 `OnComplete` hook；同主题上的 paused retry consumer 因 filter 不匹配直接 ack 跳过。
14. 外部方可通过 `Await`（`await.go`）等待指定 status，或之后用 `Latest` 查询结果；新的 `Trigger` 在此时因 `Finished()=true` 而被允许（同一 foreignID 开启下一 run）。

---

## 3. 到终态的全部路径与资源回收

合法 RunState 迁移矩阵硬编码在 `runstate.go:runStateTransitions`：

- `Initiated → Running | Paused`
- `Running → Completed | Paused | Cancelled`
- `Paused → Running | Cancelled`
- `Completed → RequestedDataDeleted`；`Cancelled → RequestedDataDeleted`
- `RequestedDataDeleted → DataDeleted`；`DataDeleted → RequestedDataDeleted`（可重复擦写）

### 3.1 成功完成

- 路径：最后一个 `AddStep`（或 Callback/Timeout）返回图中的终态 status，`update.go:newUpdater` 检测 `graph.IsTerminal(next)` 后写 `RunStateCompleted`。
- 涉及组件：step consumer（或 `callback.go:processCallback`、`timeout.go:processTimeout`，三者共用 `newUpdater`）→ outboxConsumer 把事件改投到 run-state-change 主题 → `OnComplete` hook consumer。
- 完成后记录仍保留在 RecordStore；同 foreignID 可再次 Trigger。

### 3.2 stage 失败、重试耗尽（自动暂停）

- step/timeout 业务函数返回 error 时：`step.go:stepConsumer`（timeout 侧为 `timeout.go:processTimeout`）调 `pause.go:maybePause`。
- 未配置 `PauseAfterErrCount`（默认 0）时：错误上抛 → `consume` 返回 err → 不 ack → `runOnce` 记日志、打 `metrics.ProcessErrors`、退避 `errBackOff` 后**无限重试**。
- 配置了 N 时：`internal/errorcounter` 以 `processName + runID` 为键计数（`internal/errorcounter/errorcounter.go:Counter.Add`，按进程×记录隔离）；达到 N 后调 `run.Pause(ctx, "max error retry threshold hit - automatically paused")`（`run.go:(*Run).Pause` → `RunStateController.Pause`，只允许 Running/Initiated → Paused），随后清错误计数、ack 当前事件。记录进入 `RunStatePaused`，事件被改投 run-state-change 主题，普通 status 主题不再有它的事件；任何 consumer 查到 `Stopped()` 也会跳过。
- 暂停后的出路：①用户显式 `Resume`（controller）；②`pausedRecordsRetryConsumer` 在 `resumeAfter` 到期后自动 `Resume`（`pause.go:autoRetryConsumer`，校验 `record.RunState==Paused` 且 `UpdatedAt` 老于阈值）；③用户显式 `Cancel`。Resume 把 RunState 置回 `Running` 而 Status 不变，`MakeOutboxEventData` 对 Running 不做主题改写，因此事件进入**当前 status 的普通主题**，对应 step consumer 像处理新事件一样重新驱动该记录（重新走版本校验，记录从此继续）。

### 3.3 显式取消

- 两种入口：业务代码内 `run.Cancel(ctx, reason)`（`run.go`，返回 skip 哨兵 -1，仅做 RunState 更新，状态迁移为 Running/Paused → Cancelled）；或外部对某条记录构造 `RunStateController` 调 `Cancel`（`runstate.go:runStateControllerImpl.Cancel`，Paused/Running 可转）。
- 取消后事件进 run-state-change 主题，`OnCancel` hook 被通知；step/timeout consumer 再看到该记录时因 `Stopped()`/`Finished()` 跳过；timeoutPoller 对未消费的 timeout 在下轮 `Cancel` 作废。Cancelled 永久不可 Resume，只能 DeleteData。

### 3.4 超时"取消"

需要区分两种语义：

- **timeout 功能本身不是取消器**：`TimeoutFunc` 与普通 step 一样返回下一状态，由 `newUpdater` 做正常图迁移；如果配置的目标是某个"取消/失败"终态节点，那只是业务定义的 status，RunState 仍按"是否图终态"置 Completed（`timeout.go:processTimeout`）。
- 真正的超时取消需要用户在 `TimeoutFunc` 内 `return run.Cancel(ctx, reason)`（返回 -1，`skipUpdate` 命中，updater 不执行），由 controller 把 RunState 置 `Cancelled`，之后路径同 3.3。timeoutPoller 随后把该 timeout `Complete`。

### 3.5 删除清理（delete consumer 的回收动作）

- 入口：`RunStateController.DeleteData`（`runstate.go`），仅允许 Completed/Cancelled → `RequestedDataDeleted`（业务进程内无 Run 方法封装，属外部 API 操作）。
- 该次 Store 产生的 outbox 事件被 `MakeOutboxEventData` 改投到 `DeleteTopic`。
- `deleteConsumer` → `runDelete`（`delete.go`）：Lookup 记录；用默认 `[]byte(`{"result":"deleted"}`)` 替换 Object，或调用 `builder.go:WithCustomDelete` 注册的擦除函数（先反序列化给用户改写再序列化）；置 RunState=`DataDeleted`，`updateRecord` 以 `RequestedDataDeleted → DataDeleted` 落库（version+1）。
- 这次更新的事件进 run-state-change 主题（DataDeleted 属于 hook 可感知的 RunState，但 `OnPause/OnCancel/OnComplete` 三个 builder API 不会为它注册 consumer）。
- 注意 delete consumer **不删除记录行**：它做的是 PII 擦写（GDPR "被遗忘权"语义），记录骨架（RunID、status、meta）保留；`DataDeleted → RequestedDataDeleted` 合法且 delete consumer 内部对前态没有额外校验，允许重复擦写。

---

## 4. 停机顺序与错误隔离

### 4.1 `Stop` 的关停语义

入口 `workflow.go:(*Workflow).Stop`：

1. 加锁取出父 context 的 `cancel`；未 Run 过（cancel==nil）直接返回。
2. `cancel()` 取消父 context——**所有组件同时收到取消信号，组件之间没有规定的关停先后顺序**（启动顺序是 outbox → step → timeout → connector → hooks → delete → paused retry，但关停是"广播取消、各自退出"）。
3. 轮询等待：循环 `w.States()`（`state.go:States` 快照 `internalState`）统计非 `StateUnknown/StateShutdown` 的进程数，为 0 才返回；否则每 10ms 轮询一次。**没有关停超时**：若某 consumer 阻塞在不响应 ctx 的 I/O 上，`Stop` 会一直等待。

各组件的退出链路：父 ctx 取消 → `runOnce` 中 `awaitRole`/`process`/退避 timer 的 `ctx.Done()` 命中 → `run` defer 置 `StateShutdown`。receiver 侧靠 `EventReceiver.Recv` 与轮询 `wait` 对 ctx 的响应退出；`consumeStepEvents` 等还 defer 了 `stream.Close()`。

`Run` 侧的 `w.launching.Wait()` 与 `Stop` 配合：保证 `Run` 返回时所有进程名已登记，`Stop` 的状态轮询不会漏数。

### 4.2 错误吸收与重试隔离

- 进程级隔离：每个组件跑在独立 goroutine + 独立 `w.run` 循环里。单个组件的错误在 `runOnce`（`workflow.go:runOnce`）内被吸收：`logger.Error` + `metrics.ProcessErrors{workflow,process_name}` + `errBackOff` 定时器退避（退避期间也可被 ctx 取消打断），然后 `return nil` 重新抢角色循环——错误**不会**冒泡到其他 goroutine。
- 记录级熔断：业务错误经 `maybePause`（`pause.go`）进入 `errorCounter`（`internal/errorcounter`，内存 map、按 `processName\x00runID` 复合键）计数；达到 `PauseAfterErrCount` 自动 Pause 该记录并 Clear 计数，避免坏记录无限阻塞 consumer。计数只按进程+记录聚合，不影响其他记录、其他 consumer。
- 角色级隔离：`Await` 返回的子 context 带独立 `cancel`，`runOnce` 中 `defer cancel()`；一次执行结束（或错误退避）后释放角色，下次循环重新竞争。多实例部署下一个实例丢角色只导致本进程回到 Idle 再 Await，不影响其他 role 的组件。
- **panic 不隔离**：全仓库非测试代码没有任何 `recover()`（已全量检索 `grep -rn "recover()"` 确认）。任意 consumer goroutine 内 panic 会沿 goroutine 栈直接崩溃整个进程——错误隔离只覆盖"返回 error"这一通道，用户 `ConsumerFunc/TimeoutFunc/ConnectorFunc/HookFunc` 需自行防 panic。

---

## 5. 存储交互模式

### 5.1 RecordStore 的读写模式

接口定义见 `store.go:RecordStore`；SQL 实现见 `adapters/sqlstore/sqlstore.go`。

- **写（Store）**：必须是"记录 upsert + outbox insert"单事务，并能在创建前确定 ID（接口注释要求）。`sqlstore.Store` 先按 RunID 查存在性决定 create/update，再 `MakeOutboxEventData` 生成 outbox 行，最后 `tx.Commit`——这是整个引擎"状态变更必然产生事件"的根保证。
- **按 RunID 读（Lookup）**：所有事件驱动路径（step、hook、delete、paused-retry、updater 二次确认）都用事件 header 里的 RunID 精确查。
- **按业务键读（Latest）**：`Trigger` 防并发 run、`Callback`、`timeoutPoller` 用 `(workflow, foreignID)` 取最新一条（SQL 为 `order by created_at desc limit 1`）。
- **轮询（List/ListValid）**：`outboxConsumer` 轮 `ListOutboxEvents(limit)`；`timeoutPoller` 轮 `TimeoutStore.ListValid(status, now)`；两者都是"取一批 → 处理 → sleep(pollingFrequency)"（`outbox.go:purgeOutbox` 空转时 sleep，`timeout.go:pollTimeouts` 每轮后 `wait`）。
- **监听**：其余组件全部通过 `EventStreamer.NewReceiver(topic, role, pollFrequency)` 监听；具体适配器（如 `adapters/reflexstreamer`、`adapters/kafkastreamer`）负责按 role 名保存游标，轮询频率由 receiver option 传入。
- **乐观锁提交**：`update.go:newUpdater` 提交前重新 Lookup 并比对 `Meta.Version == workingVersion`；`updateRecord` 每次 version+1（`update.go`）。并发的 step/timeout/callback 只有一方能提交成功，失败方返回错误重放事件。
- TimeoutStore（`store.go:TimeoutStore`）独立维护 timeout 行生命周期：`Create`（auto-inserter）→ `Complete`（成功执行）/`Cancel`（状态已离开或 run 结束）。

### 5.2 outbox 的角色

- 解耦"写记录"与"发消息"：业务侧只面对 RecordStore 一个事务边界，无需分布式事务；消息投递由 outboxConsumer 异步完成，进程崩溃后未投递的 outbox 行仍在，重启继续，保证 at-least-once。
- 路由中枢：topic 不由发送方选择，而由 `MakeOutboxEventData` 根据**落库记录的 RunState/Status** 统一决定（普通 status / run-state-change / delete 三类，见第 0 节表）。
- 可外置：`WithoutOutbox()` 允许由中心化存储层自行 purge outbox，引擎不再启动 outboxConsumer。
- 幂等投递链路：发送成功才 `DeleteOutboxEvent`；失败则下轮重发，可能产生重复事件——下游用"事件 HeaderRecordVersion 与记录版本对账"（`step.go:stepConsumer`）去重。

### 5.3 consumer 之间如何避免重复处理同一条记录

1. **topic 分区**：每条事件按 RunState 只进一个主题，普通 step、生命周期 hook、删除逻辑各听各的主题，天然分流。
2. **主题内过滤**：公共 run-state-change 主题上用 header 过滤（`filterByRunState`）；多实例并行用事件 ID 取模分片（`shardFilter`；connector 用 FNV 哈希后的 ID）。
3. **游标+ack**：`consumer.go:consume` 处理成功才调 `Ack`，游标由 EventStreamer 按 role 持久化；同 role 全局唯一持有者（RoleScheduler），不会两个实例同速并发消费同一分区。
4. **版本对账**：同一事件重投或多消费者竞争时，`record.Meta.Version` 小于事件版本则报错重试、大于则确认已处理直接跳过；状态机内再用 `latest.Status == 期望 status`（trigger 路径、callback、timeoutPoller）与 RunState `Stopped()/Finished()` 做第二道闸。
5. **乐观锁**：提交阶段 version 不匹配直接失败，保证一次状态迁移只落库一次。

---

## 6. 记录状态迁移 × 组件职责对照总表

| 阶段 | RunState / Status 变化 | 触发者（写记录） | 事件 topic | 接手的后台组件 | 关键代码位置 |
| --- | --- | --- | --- | --- | --- |
| 外部事件接入 | 无写库（只产生 Trigger 调用） | connector `ConnectorFunc` 调 `api.Trigger` | 外部流 | connectorConsumer（分片消费外部流） | `connector.go:connectorConsumer` |
| 创建 run | （无）→ `Initiated`，Status=起始 | `Trigger` | `Topic(name,起始status)` | outboxConsumer 投递；step consumer 与 timeout auto-inserter 同时收到 | `trigger.go:trigger`、`event.go:MakeOutboxEventData` |
| 首次消费 | `Initiated → Running`（buildRun 内随首次提交） | step consumer（或 timeout/callback） | 下一个 `Topic(name,next)` | 当前 status 的 step consumer、auto-inserter | `run.go:buildRun`、`step.go:stepConsumer` |
| 正常推进 | `Running → Running`，Status 沿图迁移 | ConsumerFunc + updater（终态校验前） | `Topic(name,next)` | 下一个 status 的 step consumer / auto-inserter | `update.go:newUpdater` |
| timeout 注册 | 不改 RunState/Status | auto-inserter 调 `TimeoutStore.Create` | 无事件 | timeoutPoller 轮询 TimeoutStore | `timeout.go:timeoutAutoInserterConsumer` |
| timeout 到期推进 | `Running → Running/Completed`（或 Cancel） | TimeoutFunc + 同一 updater；timeout Complete | 对应 status 或 run-state-change | 下一 status consumer / hook | `timeout.go:pollTimeouts`、`processTimeout` |
| 业务失败（无阈值） | 不变 | 无提交（错误上抛、不 ack） | 事件重投 | 同一 step consumer 退避重试 | `workflow.go:runOnce`、`consumer.go:consume` |
| 错误达阈值 | `Running/Initiated → Paused` | `maybePause` → `Run.Pause` | `RunStateChangeTopic` | OnPause hook、paused retry consumer（lag 到期自动 Resume） | `pause.go:maybePause`、`run.go:Run.Pause` |
| 暂停中 | `Paused` | — | — | 所有 step/timeout 见 `Stopped()` 跳过 | `step.go:stepConsumer`、`runstate.go:Stopped` |
| 自动/手动恢复 | `Paused → Running`，Status 不变 | `RunStateController.Resume`（paused retry 或用户） | `Topic(name,当前status)`（Running 不改写主题） | 当前 status 的 step consumer 重新接手；auto-inserter 也会重收（TimerFunc 返回零值/已存在则不重复创建） | `pause.go:autoRetryConsumer`、`runstate.go`、`event.go:MakeOutboxEventData` |
| 成功完成 | `Running → Completed`，Status=终态节点 | updater 检测 `IsTerminal` | `RunStateChangeTopic` | OnComplete hook；Trigger 闸门放行下一 run | `update.go:newUpdater`、`hook.go` |
| 显式取消 | `Running/Paused → Cancelled` | `Run.Cancel` / controller | `RunStateChangeTopic` | OnCancel hook；step/timeout 永久跳过；未用 timeout 被 Cancel | `run.go:Run.Cancel`、`runstate.go`、`timeout.go:pollTimeouts` |
| 请求删除数据 | `Completed/Cancelled → RequestedDataDeleted` | controller.DeleteData | `DeleteTopic` | deleteConsumer | `runstate.go`、`topic.go:DeleteTopic` |
| 执行删除 | `RequestedDataDeleted → DataDeleted`，Object 被擦写 | `runDelete`（默认/customDelete） | `RunStateChangeTopic` | 无专门 hook consumer；记录骨架保留 | `delete.go:runDelete` |
| 停机 | 全部组件 → `StateShutdown` | `Stop` 取消父 ctx | — | 全部 goroutine 响应 ctx 退出，Stop 轮询状态归零 | `workflow.go:Stop`、`workflow.go:run` |

---

## 附：关键代码索引

- 启动/关停/重试外壳：`workflow.go:Run`、`track`、`run`、`runOnce`、`Stop`；进程状态：`state.go`
- 通用消费循环：`consumer.go:consume`（lag、过滤、ack、metrics）
- 主 step 链路：`step.go:consumeStepEvents`/`stepConsumer`；提交：`update.go:newUpdater`/`updateRecord`/`validateTransition`
- 触发：`trigger.go`；Run 与状态控制：`run.go`、`runstate.go`；skip 哨兵：`status.go`
- timeout：`timeout.go:timeoutPoller`/`pollTimeouts`/`processTimeout`/`timeoutAutoInserterConsumer`
- connector：`connector.go`；hooks：`hook.go`；删除：`delete.go`；暂停重试：`pause.go`；outbox：`outbox.go`
- 事件/主题/过滤：`event.go`（`MakeOutboxEventData`）、`topic.go`、`eventstreamer.go`、`eventfilter.go`
- 配置入口：`builder.go:NewBuilder/AddStep/AddTimeout/AddConnector/OnPause/OnCancel/OnComplete/Build` 及各 BuildOption/Option；`options.go`
- 存储契约：`store.go:RecordStore/TimeoutStore`；SQL 事务实现：`adapters/sqlstore/sqlstore.go`、`adapters/sqlstore/util.go`
- 选主：`rolescheduler.go`、`adapters/memrolescheduler/memrolescheduler.go`
- 错误计数：`internal/errorcounter/errorcounter.go`；状态图：`internal/graph/graph.go`；metrics：`internal/metrics/metrics.go`
