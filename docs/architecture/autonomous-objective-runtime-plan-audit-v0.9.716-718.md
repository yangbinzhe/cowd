# Autonomous Objective Runtime 统合方案审查审计

> 审计对象：
> `docs/architecture/autonomous-objective-runtime-unified-plan-v0.9.716-718.md`
> 与 `docs/architecture/autonomous-objective-runtime-source-manifest-v0.9.716-718.md`。
>
> 审计状态：**历史审计证据；已由 2026-09-03 根因与提交格式统合方案重新收敛。**
> 当前执行权威为
> [`autonomous-objective-runtime-root-cause-and-execution-plan-2026-09-03.md`](./autonomous-objective-runtime-root-cause-and-execution-plan-2026-09-03.md)。
> 本文只保留此前的方案准入证据，不得用于宣称当前代码已实施或已 E2E 验收。
>
> 本文只证明方案具备可实施的边界、owner、依赖、删除、测试和证据门，不宣称任何版本的
> Runtime、Agent、Surface 或真实业务已经完成。

## 1. 审计结论

方案解决了历史上最危险的四类错位：

```text
活动图被当作业务结果
模型被当作运行时控制器
局部/受控测试被当作全链真实成功
计划/代码/Session/Surface 状态没有唯一事实源
```

审计确认：

- 业务目标、Obligation、Task、Artifact、Evidence、Outcome 已形成完整闭环；
- 模型语义与 Runtime 机械事实有清晰 membrane；
- ObjectiveSupervisor 被定义为确定性协调器，而非第二 scheduler；
- Task Market 复用现有 Graph/ResourceManager，不引入重复容量控制；
- Graph terminal、Program terminal、Objective terminal 的边界已分开；
- 真实模型和浏览器被隔离到 v0.9.718；
- v0.9.716/717 的代码层 gate 足以在付费前发现 capability、permission、projection、lease、
  recovery 和 evaluator 缺陷；
- 每个版本有明确 completion claim，不会再把小范围 PASS 泛化为系统完成。

审计没有发现需要用户在实施开始前重新选择的核心架构分歧。保留的数值（并发、超时、
队列、预算）属于版本化 policy profile，实施时必须从配置快照解析，不能散落成 magic number。

## 2. 计划权威和历史文件审计

以下文件在当前范围内标记为历史证据，由统合方案取代其执行权：

| 历史文件 | 处理 | 原因 |
| --- | --- | --- |
| `docs/architecture/collaboration-program-handoff-2026-08-27.md` | superseded | 交接文件明确不是完成报告，且依赖旧 v0.9.705–707 边界 |
| `docs/architecture/collaboration-program-hardening.md` | historical baseline | 已识别 dual codec、duplicate lifecycle、implicit topology、lossy terminal 等问题；新方案将其纳入 Objective/TaskMarket 终态 |
| `docs/architecture/collaboration-semantic-harness-plan-audit.md` | superseded for execution | 旧三版本只覆盖 semantic/capacity/experience，未覆盖目标级自治恢复和主动任务市场 |
| `docs/architecture/collaboration-semantic-harness-v0.9.705.md` | historical version evidence | 只证明语义编译边界 |
| `docs/architecture/collaboration-semantic-harness-v0.9.706.md` | historical version evidence | 只证明 capacity/approval/live Surface 局部边界 |
| `docs/architecture/collaboration-semantic-harness-v0.9.707.md` | historical version evidence | 只证明 experience/release 局部边界 |
| `docs/architecture/autonomous-collaboration-convergence-v0.9.713.md` | historical incident evidence | 记录多候选失败原因，不再作为当前运行时方案 |
| `docs/architecture/provider-prompt-cache-hardening-v0.9.714.md` | historical incident/design evidence | 缓存仅是成本子系统，不再作为业务闭环的主轴 |

旧文件不删除，以保留审计链；实现时只允许引用其中的事实和回归用例，不得从旧文件恢复
第二执行路径或旧完成声明。

## 3. 全链业务和逆向证据审计

### 3.1 正向业务链

| 箭头 | Canonical owner | 状态载体 | 等待/并发 | 失败与证据 |
| --- | --- | --- | --- | --- |
| User directive -> admission | Gateway transport + Runtime Session/Policy | authenticated input + policy snapshot | short admission CAS | unauthorized/ambiguous/capability gap receipt |
| admission -> Objective | `GoalStore`/ObjectiveSupervisor | goal event stream | no provider await in lock | objective revision and digest |
| Objective -> Frozen intent | semantic decoder/compiler | typed intent revision | deterministic, bounded | field-level correction diagnostic |
| intent -> Program | orchestration compiler + graph commit | immutable Program/Team binding | revision-fenced CAS | stale/duplicate commit rejected |
| Program -> Task Market | Graph/Task owner | offered task + dependency/effect contract | ResourceManager admission | overload/backpressure typed |
| Task -> Agent claim | Task Market + AgentRuntime | claim token/generation/lease | per-key order, cross-key parallel | expiry/release/reclaim receipt |
| Agent -> Tool/model | Agent worker + ToolHost/Provider | effect intent/receipt | wait outside global locks | provider/tool failure, idempotency |
| effect -> Artifact/Evidence | Artifact store + Objective Evidence Ledger | digest, refs, reread receipt | outbox after durable commit | write failure nonzero; no prose fallback |
| Evidence -> verify | ObjectiveSupervisor/Program verifier | obligation status + decision | bounded verify/replan | missing producer/invalid evidence diagnostic |
| verify -> terminal | ObjectiveSupervisor + graph commit | exactly-once outcome fence | short terminal CAS | partial/blocked/failed typed outcome |
| terminal -> Surface | Runtime projection -> Gateway -> Edge/WebUI | schema/revision/cursor | incremental event stream | gap/resync; build identity mismatch |

### 3.2 逆向证据链

```text
WebUI terminal card
 -> projection schema/revision/cursor
 -> CollaborationOutcome + ObjectiveOutcome
 -> obligation/evidence/verification receipts
 -> Task claim/effect/artifact
 -> immutable Agent/Skill/Tool binding
 -> FrozenSemanticIntent
 -> authenticated user directive and policy snapshot
```

审计要求：任何一处只剩模型文本、展示名、内存 map、固定活动数或 Graph Closed，均不得
向上宣称完成。方案已把这些全部列为禁止的第二真相。

## 4. 状态事实审计

| 状态 | Canonical writer | durable | cache/projection | stale fence | 重启策略 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| Objective/Obligation | GoalStore + ObjectiveSupervisor | goal event stream | GoalProjection | goal revision/CAS | event replay | PASS |
| Program lifecycle | graph commit service | execution graph stream | Program projection | graph revision | reconcile on startup | PASS |
| Task claim | Task Market/Task aggregate | task/work events | control view | claim generation | reclaim expired lease | PASS |
| Agent lifecycle | AgentRuntime | agent event stream | Agent projection | instance revision | restore projection | PASS |
| Mission organization | MissionRuntime | production event store | Mission projection | mission revision | load event stream | PASS with v716 migration |
| Session input | Gateway SessionService + Runtime port | session journal | surface session cache | input cursor | Session worker recovery | PASS; no execution truth |
| Capacity | ResourceManager | frozen Program profile + lease | metrics snapshot | profile digest | release/reconcile leases | PASS |
| Approval | ApprovalCoordinator | approval event/outbox | countdown projection | deadline/revision | restart waiter | PASS |
| Evidence/artifact | Evidence Ledger + Artifact store | event/outbox + content digest | evidence projection | receipt/idempotency | replay pending outbox | PASS |
| Terminal outcome | ObjectiveSupervisor | outcome event | Gateway/Edge/UI | terminal fence | no second terminal commit | PASS |

已发现的 `MissionRuntime Mutex<BTreeMap> + Option<EventStore>` 被列为 v716 的迁移 blocker，
不会通过“内存可用”掩盖生产重启丢失。

## 5. Producer/consumer 和任务市场审计

| 队列/事件 | Producer | Consumer | Claim owner | 顺序 | 幂等 | 背压 | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Objective event | GoalStore/Supervisor | reducers, reconciliation | GoalStore | per objective | event fingerprint | bounded stream | PASS |
| Program command | orchestration adapter | graph commit service | graph commit | per Program revision | command id | CAS rejection | PASS |
| Offered Task | Program/Task owner | Agent pull workers | Task Market | fairness key | task revision | queue/bytes limit | PASS |
| Claim/Heartbeat | Agent worker | Task Market | claim generation | same task serial | claim id | lease age | PASS |
| Effect receipt | ToolHost/Provider | AgentRuntime/Supervisor | effect owner | per effect | idempotency ref | provider quota | PASS |
| Evidence outbox | Artifact/Evidence writer | projection/reconciler | outbox | source revision | outbox id | pending bound | PASS |
| Surface projection | Runtime reducer | Gateway/Edge/WebUI | projection cursor | monotonic cursor | cursor/revision | subscriber bound | PASS |

方案明确禁止 Gateway、TeamRuntime、ManagedAgentDispatcher 各自消费并重新解释同一 claim 或
terminal 事件。`Task Market` 不是新的并发调度器，而是现有 Graph/ResourceManager 上的协议。

## 6. 并发、等待和资源审计

### 6.1 等待图

```text
semantic compile（无外部 await）
  -> short graph CAS commit
  -> ApprovalCoordinator Notify（无 graph lock）
  -> ResourceManager fair admission
  -> Agent/Provider/Tool lease
  -> short receipt/evidence/terminal commit
  -> projection/event cursor
```

已通过的设计检查：

- provider、tool、network、storage、subscriber wait 不得持有 global/program/identity lock；
- 不为每个 durable entity 永久 spawn task；
- 同一 Program revision 仅短提交串行，不把整个模型执行串行化；
- 不同 Program、Session、Mission 和无依赖 Team/Role 可并行；
- ResourceManager 是唯一容量队列；
- 队列过载返回 typed backpressure，不通过模型 round 无限重试；
- 观察器没有独立 scheduler，cursor 不前进时不产生新快照。

### 6.2 资源表

| 资源 | owner | admission | reservation | fairness/limit | 指标 |
| --- | --- | --- | --- | --- | --- |
| provider/account/model | ResourceManager | frozen profile | before hydrate/dispatch | provider key, token pool | queue/service p50/p95/p99 |
| Agent/Team | ResourceManager + Task Market | capability + lease | claim time | fairness key | claim age/active |
| Tool/effect | Tool dispatch owner | policy/effect class | before side effect | per effect | retries/uncertain |
| Graph/Program | execution supervisor | graph slot | admission | per Program | active/queued |
| memory/context | context budget owner | profile and bytes | before assembly | context window | hydrate bytes/rebuild |
| projection | event bus/Gateway | subscriber budget | cursor lease | per consumer | bytes/unchanged polls |

## 7. 失败与恢复审计

| 失败 | 必须保留 | 恢复 owner | stale reject | 终态 |
| --- | --- | --- | --- | --- |
| provider stream loss | intent、claim、partial receipts | AgentRuntime/Supervisor | attempt/generation | retry or typed external failure |
| tool partial side effect | effect idempotency/evidence | ToolHost + Supervisor | effect fence | completed/uncertain/manual |
| Agent crash/restart | task lease and event history | Task Market/AgentRuntime | claim generation | reclaim/retry |
| all workers terminal + unresolved | completed artifacts/evidence | ObjectiveSupervisor | objective revision | replan/blocked |
| dependency not physically ready | producer receipt status | Program reconcile | edge revision | wait/replan |
| reviewer lacks read capability | frozen binding | admission/compiler | binding digest | capability_gap before paid call |
| CAS conflict | candidate and base revision | commit service | expected revision | bounded retry/backoff |
| cancellation/deadline | durable cancel request | supervisor | terminal fence | cancelled/partial |
| subscriber lag/drop | source event cursor | projection/Gateway | cursor and schema | resync, no state loss |
| shutdown/delete race | terminal/outbox order | process supervisor | stream revision | exactly-once cleanup |

审计结论：历史 `autonomous_work_orphaned` 由“直接失败”改为 Objective gap；只有不可恢复
或策略耗尽才 fail-closed，且每次修订只覆盖缺口，不重建整个已完成图。

## 8. 能力保留与删除审计

| 能力 | 现有路径 | 目标路径 | 保留证明 | 删除证明 |
| --- | --- | --- | --- | --- |
| semantic Team proposal | facade + generic/narrow tool | one FrozenSemanticIntent decoder | schema/property tests | old codec raw scan |
| Team child execution | TeamRuntime + Graph | same TeamRuntime over Task Market | child graph/restart tests | no second scheduler |
| Agent lifecycle | AgentRuntime + InProcess + Managed | shared receipt protocol, backend adapters | multi-backend parity | private lifecycle scan |
| capacity | ResourceManager + scattered ceilings | frozen ExecutionCapacityProfile | scale/backpressure tests | magic ceiling scan |
| terminal truth | Program + Host + prose | ObjectiveSupervisor/Program projection | reverse chain | host authority scan |
| Mission organization | MissionRuntime map/event | event-store projection | replay test | map-only production scan |
| context/cache | prompt/context/provider cache | stable prefix + dynamic suffix | segment/key tests | full rebuild polling scan |
| Surface live state | Runtime/Gateway/Edge reducers | typed snapshot/delta/resync | generated parity | UI inferred terminal scan |

删除动作均有 replacement owner、caller rewiring 和测试迁移，未授权的“先保留双路径”不在方案内。

## 8.1 终态与自治复审（第二轮，针对“终态而非中台”）

首次审计通过后对实际源码做了反向抽样，发现四处容易在实施时重新引入旧问题的边界：

| 发现 | 原危险 | 复审后的唯一归属/处理 | 结论 |
| --- | --- | --- | --- |
| `TeamResultReducer::DeliveryEnvelope`、`TeamWorkingState::verify_completed_graph` | Team 局部完成可能被当成 Objective 完成 | 仅作 Team-local execution/evidence projection；必须带 scope/revision/evidence，由 ObjectiveSupervisor 归并 | 已补入 716 合同、删除表和隔离测试 |
| `services.rs:project_team_terminal_outcome` | 全图终止直接生成 `OutcomeTerminalClass::Succeeded`，造成业务假成功 | 保留 Team execution observation adapter；不得写 Objective terminal，终态只由 Supervisor + Evidence Ledger 提交 | 已补入 716 owner 和终态单调性门 |
| `in_process_worker.rs` 固定一半 Agent 提案、Runtime default proposal、绝对 leaf prompt | “自治”被评测脚本和 Runtime 代做，合理动态扩展被禁止 | 删除固定比例/代提案/绝对禁止；Agent 通过 `InitiativeProposal` 自主提出，Runtime 只做 typed admission、权限/能力/资源/证据校验 | 已补入 717 删除表、prompt 重写和自由策略测试 |
| `GoalStore + ObjectiveSupervisor`、`Task Market + Graph/Task`、`AgentRuntime + backends` | 名义 owner 并列导致第二写者或第二调度器 | 明确 decider/writer/adapter 三种角色：Supervisor 决策、GoalStore 写入；Task Market 写任务状态；AgentRuntime 写实例事实；其余只能 command/adapter/projection | 已补强单一事实表和 owner scan |

本轮复审同时确认：重叠/重复不是要求所有 Agent 永远互不重叠。允许用户或模型有意安排
交叉验证；Runtime 只记录 focus/novelty 质量信号，不能因观察到重叠而回溯否决已经闭合的
业务义务。重复能力治理的判据是“是否存在第二个状态写者、调度器、容量队列、终态判定者或
无证据的自动动作”，而不是简单删除所有相似功能。

自治复审结论：方案现在同时覆盖了策略自由度、主动任务市场、动态缺口修订和终态安全膜。
硬约束只保留授权、效果、资源、租约、幂等、证据、恢复和终态不变量；没有把固定 Team/Agent
数量、提案比例、模型轮数、角色显示名或预设模板当作业务上限。

## 9. 三版本依赖和边界审计

```text
v0.9.716 Objective/Program truth + capability admission + deterministic recovery
        ↓ stable contracts: obligation, outcome, diagnostic, revision/fence
v0.9.717 Task Market + Agent pull + projection + concurrency/backpressure/cache
        ↓ stable contracts: claim/lease/evidence/projection/cost metrics
v0.9.718 installed build + real Provider + browser + final business scenario
```

| 版本 | 先决条件 | 必须完成 | 不得声称 | E2E |
| --- | --- | --- | --- | --- |
| 716 | 当前两个仓库 clean，基线 hash | Objective/Program truth、single decoder、capability closure、replan | Agent 自主 pull、Surface 完整、真实模型 | 禁止 |
| 717 | 716 clean/tag/evidence | pull/claim/lease、projection、并发/背压、cache/observer、测试迁移 | Provider 真实质量、业务终态 | 禁止 |
| 718 | 716/717 所有 gates pass | 同 SHA 安装、真实 Provider/Browser、递进场景、业务证据闭环 | 无条件吞吐或全局缓存率 | 仅此版本 |

没有跨版本循环依赖；718 不提供新架构 owner。E2E 发现问题必须退回 716/717 对应 owner，
不得在最终评测脚本中打补丁。

## 10. 测试和证据审计

### 10.1 旧测试错误已被纠正

- activity count -> objective/evidence verdict；
- capability metadata -> actual effect/evidence receipt；
- Graph Closed -> Objective Terminal；
- model prose -> typed projection；
- controlled provider adapter -> deterministic fake provider contract；
- stale service/browser -> same-SHA installed E2E；
- fixed topology success -> topology-scoped evidence；
- repeated poll -> monotonic cursor/unchanged budget；
- one broad `passed` -> business/runtime/observability/cost 四类 verdict。

### 10.2 中间版本禁用付费路径

方案明确要求 716/717 的测试套件在没有 provider credentials 时也能完整运行，并加入：

- real provider invocation guard；
- browser/E2E target compile/run guard；
- provider URL/model/fallback forbidden scan；
- fake provider event script covering success/failure/replan；
- test fixture source classification。

如果任何中间测试意外发起真实请求，版本立即 fail-closed，不得继续。

### 10.3 最终证据必须可重放

最终报告必须保存：

```text
core/edge commit + tree + build identity
provider/model/no-fallback + policy/profile digest
objective/obligation/program/task/agent IDs and revisions
event cursor and evidence/artifact digests
all fault injections and recovery receipts
browser screenshot/trace + API projection
fresh/cache/output/retry/observer cost metrics
```

## 11. 剩余风险和处理

| 风险 | 当前状态 | 处理 |
| --- | --- | --- |
| Provider rate/余额/模型格式波动 | 外部不可控 | 作为 typed external failure；不伪造成功、不无限重试 |
| 真实任务的语义不可预知 | 必然存在 | 通过 semantic intent + bounded repair；重大范围变化回到用户 |
| 大规模并发受硬件/Provider 限制 | 不可抽象保证 | 用 named capacity profile、p95/p99 和 backpressure 证明 |
| 缓存 best-effort/TTL | 供应商限制 | cohort SLO，不承诺全局 90% |
| 历史数据 schema | 旧版本存在 | 只读迁移/归档；活动运行时只保留一条新事实路径 |
| 较弱模型执行实施 | 用户明确要求 | 计划采用文件级 allowlist、逐项 gate 和 fail-closed，不依赖模型自由发挥架构判断 |

这些风险不构成方案 blocker，因为它们已被界定为外部条件、版本化 policy 或明确的 typed
failure；没有被隐藏在“稍后处理”中。

## 12. 审计签结

| 门 | 结果 |
| --- | --- |
| 业务目标和用户结果定义 | PASS |
| 终态分类、单调性及 Team-local/Objective 边界 | PASS（复审补强） |
| 历史根因和 Session 证据回溯 | PASS |
| 正向/逆向业务链 | PASS |
| 状态事实和单一 owner | PASS |
| Producer/consumer/claim | PASS |
| 并发、锁、等待和资源 | PASS |
| 失败、恢复、重启和终态 | PASS |
| capability closure | PASS |
| Agent InitiativeProposal 与策略自由度 | PASS（复审补强） |
| 固定提案/Runtime 代做/绝对 prompt 限制已移除 | PASS（实施后需 source scan） |
| 能力保留、删除和 caller rewiring | PASS |
| 三版本依赖和 E2E 隔离 | PASS |
| 测试迁移和反 false-positive | PASS |
| 精确 source manifest | PASS |
| 真实 Provider/Browser 不提前调用 | PASS |

**最终判定：方案可以进入 v0.9.716 代码实施。** 该结论是“实施边界和审计门足够支撑
终态目标”的判断，不是对尚未编写代码的产品完成承诺。实施后必须用本轮新增的终态隔离、
InitiativeProposal 自由策略、无固定提案比例、无 Runtime 代提案和 owner 唯一性测试重新
取得证据；任一项失败都不得进入付费 E2E。

准入后的第一条实施动作仍然是重新采集两仓库 baseline snapshot、生成 version board 和
deletion preflight；这些动作不是代码实现，不得跳过。任何实际代码事实与 manifest 不一致，
先修订本方案和本审计报告，再编辑代码。
