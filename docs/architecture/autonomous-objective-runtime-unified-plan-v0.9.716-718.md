# Autonomous Objective Runtime 统合方案（v0.9.716–v0.9.718）

> 状态：方案已通过首次审计；本次终态/自治复审补强条款已纳入，实施仍必须按版本门禁推进。
> 本文是当前唯一执行权威，取代此前只覆盖 semantic harness、cache、observation 或
> collaboration convergence 的局部计划。旧文档保留为历史证据，不得再作为当前完成状态来源。

## 0. 目标、边界与不可妥协约束

### 0.1 业务目标

用户给出一个开放式、复杂、可能长期运行的目标后，系统必须能够：

```text
理解目标 -> 形成义务 -> 自主拆解 -> Agent 主动领取/协作 -> 执行工具和模型工作
-> 持久化产物和证据 -> 发现缺口并重规划 -> 验证业务结果 -> 在前端交付同一事实
```

“Team/Agent 数量、proposal/bid/review/challenge 数量、模型轮数、图节点完成”只能是
可观测或压力指标，不能单独决定业务成功。固定拓扑测试与业务目标验收必须分开记录。

### 0.2 自治能力定义

| 自治维度 | 目标态 | Runtime 不得替代的模型判断 |
| --- | --- | --- |
| 理解 | 将用户目标转成语义意图、假设和验收义务 | 业务含义、重点、证据相关性 |
| 分解 | 生成任务、输入输出和依赖 | 任务粒度和策略 |
| 领取 | Agent 从持久任务市场主动 pull/claim | 是否竞标、是否协作、是否放弃 |
| 执行 | Agent 自主选择已授权工具和协作者 | 工具顺序、搜索方向、推理路径 |
| 协作 | 发布事实、请求 peer、接受或挑战产物 | 哪些信息有价值、如何综合 |
| 修订 | 提出最小语义修订或缺口任务 | 下一步如何补齐业务缺口 |
| 停止 | 判断完成、部分完成、阻塞或升级 | 结果可信度和是否需要人类判断 |

Runtime 仍然必须硬性控制授权、效果类别、资源、租约、幂等、取消、重试、证据和
终态提交。增强自治不是删除这些内核不变量，而是把模型决策空间从“根节点逐轮喂任务”
扩展为“在明确边界内主动选择有价值的工作”。

### 0.2.1 终态优先与自治包络（本次复审补强）

本项目交付的是可验证的业务终态，不是把若干中台组件拼装完成。任何版本、Session、
Team 或 Agent 的“完成”都必须回答同一个问题：用户要求的 Objective 是否已经由持久化
产物和独立证据闭合。活动图、协同次数、Team/Agent 数量和模型输出只能作为诊断数据。

自治包络分成三层，避免把安全边界误写成业务策略：

| 层 | 模型/Agent 可自由选择 | Runtime 只做的事 | 是否可因业务需要扩展 |
| --- | --- | --- | --- |
| 策略层 | 分解粒度、执行顺序、工具组合、检索方向、协作者、是否复核、是否提出新任务 | 校验目标范围、能力、权限、资源和证据合同 | 是；不以固定角色名、固定轮数或固定提案数限制 |
| 计划层 | 在 Objective 范围内提出 `InitiativeProposal`（新 Task、依赖、协作请求、证据缺口或按资源/权限约束的 Team 扩展） | 验证语义 delta、用户固定约束、能力闭合、幂等、资源/预算和 revision fence | 是；可动态增加工作，但不能静默改变目标或权限 |
| 内核层 | 不得伪造身份、lease、效果、证据或成功 | 授权、效果类别、租约、幂等、背压、取消、恢复、证据持久化和终态提交 | 否；这些是不变量，不是“智能限制” |

`InitiativeProposal` 不是 Runtime 代模型做决定：Agent 可以主动提出并在同一 Task Market
中竞争、邀请 peer、拆解或修订；Runtime 只返回 `accepted/rejected/deferred` 的类型化
结果和原因。禁止用“必须由一半 Agent 提案”“每个 Team 必须产生 N 个协作事件”之类
活动配额冒充自治。重复的无进展提案按 `(objective_id, semantic_delta_digest,
authority_revision)` 去重；只有相同语义反复失败、超出用户固定范围、能力/资源不可闭合或
外部条件不可恢复时才停止。这样既防止空耗，也不把合理的探索次数硬编码成业务上限。

以下行为必须允许在终态实现中发生，并纳入代码/故障测试：Agent 自主提出缺口任务、主动
请求或拒绝协作者、调整无效依赖、重新读取已完成产物、发起受 Objective authority、权限和
实时资源约束的 Team/Role 扩展（不设业务固定数量上限），以及
在发现新证据后提交语义修订。它们都必须通过同一个 typed membrane，不能由 prompt 文本
直接写入 Runtime 身份或生命周期。

### 0.3 三版本约束

- 必须在 `v0.9.716`、`v0.9.717`、`v0.9.718` 三个版本内完成目标态。
- 前两个版本禁止真实 Provider、真实浏览器和真实业务 E2E；只做代码层回归、契约测试、
  故障注入、并发测试、生成物一致性和性能基准。
- 所有高成本模型测试必须等最终版本静态审计、完整代码回归和安装版本校验全部通过后执行。
- 最终版本开始真实 E2E 后禁止再扩展架构，只能修复归属该版本的实现缺陷并重跑门禁。
- 不使用预算作为模型活动数量或智能上限。预算只控制新 admission、并显示剩余预算；
  已经 admitted 的工作必须可完成、取消或按策略降级。
- 所有旧测试必须重新分类、迁移或删除；不得用旧测试证明已经改变的语义。

## 1. 基线、证据和外部研究

### 1.1 当前代码基线

基线采集时间：2026-09-02；采集时其他写入者已停止。

| 仓库 | 分支 | HEAD | 工作区 | 说明 |
| --- | --- | --- | --- | --- |
| `cowd-dev` | `dev` | `6fab114a9235681757332c229e0f53f76a2ac773` | clean，领先 `origin/dev` 27 个提交 | 当前 Runtime/Gateway/Harness 源码 |
| `cowd-edge` | `master` | `0b802324e170d18f4bc78cb998078e3e5ecacc54` | clean，与 `origin/master` 对齐 | Edge adapter、WebUI、生成合同 |

参与实现的两个仓库必须在每个版本开始前重新记录 HEAD、tree、index、worktree、untracked
manifest 和内容 hash。版本提交必须只包含该版本 allowlist，不能用当前分支的累计 diff 充当版本证据。

### 1.2 历史根因登记

| 时间/证据 | 事实 | 根因层 | 必须固化的不变量 | 归属版本 |
| --- | --- | --- | --- | --- |
| 0710 总纲与同日完成矩阵 | 一个文档写 V3–V9 未完成，另一个写全部 completed/tagged/passed | 治理/验收 | implementation、wired、durable、E2E、release 分离且单一状态源 | 716 |
| V505 真实模型审计 | 简单调用通过，复杂单图质量 0/7，Team 2/7 | 业务闭环/模型边界 | 复杂业务结果和证据是终态，不得以工具或节点通过替代 | 716 |
| V506 核心交付 | 仅 1 Team/4 Agent 固定场景通过，却被扩展解释为核心能力完成 | 范围漂移 | 每个版本完成声明必须绑定拓扑、模型、工具、Surface 和业务范围 | 716 |
| V598 审计 | 27 个风险、12 个 P0/P1；4,377 测试未证明组合链 | 测试设计 | 失败传播、重启、守恒、能力闭合、真实组合链为硬门 | 716 |
| v0.9.708 | 自动观察器否决了用户明确的 Team 拓扑 | 模型/Runtime 权责 | 明确用户约束不可被优化器静默改变 | 716 |
| v0.9.709 | CAS 重试不足，健康并发 Team 被判失败并触发重放 | 并发/恢复 | topology-scaled retry/backoff、幂等、presentation recovery | 716 |
| v0.9.710–712 | 输出截断、评测误报、快照重复、root projection 巨大 | Surface/评测/观测 | canonical facts 驱动评测，cursor 增量、无变化不算进度 | 717 |
| v0.9.713 六个候选 | proposal 缺失、lease 过期、reviewer 无权限、余额错误、阻塞不 fail-closed | 任务市场/能力/恢复 | admission 前 capability closure，目标级 revision/replan | 716/717 |
| v0.9.714 | 受控缓存接近 99%，真实 Team 约 51.64%–62.17% | 成本/缓存 | 区分 eligible-warm 与 cold-inclusive；不得外推校准结果 | 717 |
| v0.9.715 最新任务 | 175 轮、1,013 万 tokens、52 分钟；3/4 Team、12/16 Agent；proposal/bid/review/challenge/edge/reread 多项不足，业务 failed | 全链终态 | graph terminal 不等于 objective terminal；必须有目标监督和缺口重规划 | 716 |

历史报告粗统计（不同版本和场景重复，不能当独立产品成功率）如下：

| 场景 | 通过/总数 | 启示 |
| --- | ---: | --- |
| 直连 | 22/27 | 基础链相对稳定 |
| 工具证据 | 24/26 | 工具局部链可用 |
| 单架构复杂任务 | 11/19 | 长链路和业务闭环不稳定 |
| Team projection | 12/23 | Team/Surface 组合不稳定 |
| 群论研究 | 5/9 | 需要证据、研究能力和综合的任务存在明显波动 |
| 隐式协同义务 | 1/4 | 自主义务发现和恢复未形成稳定能力 |
| Qwen 大规模协同 | 1/8 | 固定大拓扑尚未具备可靠闭环 |

### 1.3 外部研究结论及设计取舍

本方案使用 agent-reach 的网页读取路径校验了以下一手资料：

| 资料 | 可复用原则 | 对本方案的影响 |
| --- | --- | --- |
| [Temporal durable execution](https://docs.temporal.io/temporal) 与 [Workflow replay](https://docs.temporal.io/workflow-execution) | Event History、可重放、失败恢复由 Runtime/Worker 负责；Workflow 之间消息通信且可并发 | `ObjectiveSupervisor` 只做确定性协调，不让模型承担 liveness；所有修订有 revision/fence |
| [LangGraph durable execution](https://docs.langchain.com/oss/python/langgraph/durable-execution) | Checkpointer 保存线程状态，Store 保存跨线程事实；子图共享数据需要显式 Store | Session/Task 短期状态与 Objective/Evidence 长期事实分离，跨 Team 不靠隐式快照 |
| [Anthropic Building Effective Agents](https://www.anthropic.com/research/building-effective-agents) | Workflow 和 Agent 区分；先用简单可组合模式；Agent 通过环境真值循环并设置停止条件 | Team 只在任务需要时出现；工具结果和可验证证据驱动进度，活动计数不是成功 |
| [Ray Actors](https://docs.ray.io/en/latest/ray-core/actors.html) | Actor 有局部状态；同一 Actor 串行，不同 Actor 并行；共享状态必须显式 owner | Agent identity 采用每实例状态和 per-key 顺序，禁止全局锁和隐式共享可变状态 |
| [NATS pull consumers](https://docs.nats.io/nats-concepts/jetstream/consumers) | Worker 主动 pull；batch、bytes、ack pending、expiry 明确界限 | Task Market 采用 pull/claim/lease，限制批量、字节、未确认和关闭行为 |
| [DeepSeek Context Caching](https://api-docs.deepseek.com/guides/kv_cache) | 只有已持久化且完整匹配的 prefix unit 才命中；best-effort；输出仍需计算 | 背景/工具 schema 固定前缀，动态身份/状态在后缀；报告命中率和实际金额，不承诺全局 90% |
| [Anthropic Prompt Caching](https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching) | 静态内容置前，breakpoint 放在稳定前缀；并发首个写入完成前不能假定命中 | 取消动态字段污染稳定前缀，cache cohort 按 provider/schema/policy revision 隔离 |

## 2. 终态架构

### 2.1 权责膜

```text
AuthenticatedUserDirective
  -> FrozenSemanticIntent
  -> CompiledCollaborationProgram
  -> ObjectiveSupervisor + TaskMarket + Existing Graph Scheduler
  -> Artifact/Evidence Ledger
  -> CollaborationOutcome
  -> Gateway/Surface projection
```

| 平面 | 可以决定 | 不可以决定 |
| --- | --- | --- |
| 用户指令 | 目标、明确 Team/角色名、固定约束、效果授权、期望结果 | Runtime id、租约、终态、证据事实 |
| 模型语义 | 分解、职责、拓扑、能力谓词、输入输出、证据意义、置信度、有限修订 | 权限授予、实例 id、容量、租约、发布模板、成功声明 |
| Intent Compiler | 规范化、Definition/Skill/Tool 解析、dataflow、权限裁剪、资源估算、不可变绑定 | 修改用户固定字段、猜角色含义、静默 fallback |
| Runtime 内核 | 状态、调度、授权、资源、租约、幂等、效果、恢复、证据、终态 | 从显示名或模型文本猜业务完成 |
| Surface | 展示和交互 | 写执行真相或推导成功 |

### 2.2 四个核心合同

不再把一份 JSON 同时当作 prompt、执行 IR、状态快照和终态回执：

1. `AuthenticatedUserDirective`：保留原始用户目标、固定字段、约束和授权。
2. `FrozenSemanticIntent`：唯一模型语义产物，包含 teams、roles、capability predicates、
   inputs、outputs、acceptance、assumptions、confidence、partial policy 和 semantic-delta
   revision。这里的“有界”指每次修订只能声明目标内的字段差异、权限不提升且可去重，不是
   固定最多 N 次，也不是禁止 Agent 依据新证据继续提出合理工作。
3. `CompiledCollaborationProgram`：Runtime 添加准确 Agent/Skill/Tool revision、权限效果、
   resource snapshot、graph nodes、leases、budget、approval、idempotency 和 recovery fence。
4. `CollaborationOutcome`：只包含持久化的 obligation/team/role 结果、artifact/evidence refs、
   diagnostics、retry disposition 和最终验证结果。

### 2.3 Objective、Mission、Session、Task、Team、Agent 定义

| 概念 | 终态职责 | 当前代码事实 | 目标变化 |
| --- | --- | --- | --- |
| Objective | 用户业务目标和 Obligation 的唯一完成事实 | `GoalStore` 已有事件流、criteria、observations、cost；`GoalContract` 无 Team obligation 级 producer/evidence 状态 | 扩展 Goal 合同和 reducer，加入 obligation、program ref、terminal diagnosis；不新增第二目标存储 |
| Mission | 用户可见的组织/计划容器 | `MissionRuntime` 使用 `Mutex<BTreeMap>`，event store 可选 | 保留 Mission 展示和生命周期；执行完成由 Objective/Program 事实决定，消除 Mission 自己判执行终态 |
| Session | 对话连续性、审批、投影连接和用户输入顺序 | Gateway `SessionService` 与 Runtime Session ports 已存在 | Session 只承载输入和 Surface 连续性，不从 transcript 推导 Program 成功 |
| Task | 带输入输出、依赖、lease、artifact、evidence 的可执行工作 | `TaskAggregateService`/`TaskRuntimePort` 已有 CAS、outbox、binding | 任务市场协议落到现有 Task/Graph scheduler，不新建第二队列 |
| Team | 协作拓扑和策略，不是第二调度器 | `TeamRuntime` 已有 child graph、projection、working state、collaboration view | 保留 TeamRuntime，Program 负责跨 Team aggregation，Agent worker 负责主动 pull |
| Agent Definition | 版本化角色、能力、权限和模型策略 | `AgentCatalog`/Definition registry 已存在 | 只解析已发布 revision；执行实例与定义分离 |
| Agent Instance | 一个受控执行身份 | `AgentRuntime` 事件源化；`ManagedAgentDispatcher` 另有长期调用路径 | 统一 claim/lease/effect/terminal 事实，Managed Agent 只能作为同一 Runtime worker adapter |
| Evidence | 产物和可验证事实 | `context/evidence`、artifact、Mission evidence、Task outbox 多处存在 | Objective Evidence Ledger 作为归并事实；其它模块只能写入或投影 |

### 2.3.1 终态分类、单调性与局部结果边界（本次复审补强）

终态不是一个布尔值，也不是“所有图节点都结束”。Runtime 必须持久化以下互斥且可重放
的 Objective 结果；一旦写入终态，只允许追加诊断/展示投影，不允许改写结果：

| Objective 结果 | 成立条件 | 用户可见含义 |
| --- | --- | --- |
| `Satisfied` | 所有 required Obligation 均有独立 verifier verdict、durable artifact/evidence 和 reread receipt；无未解决依赖、lease 或补偿动作 | 用户目标已完成，可交付 |
| `PartiallySatisfied` | 仅在用户/Objective 明确允许 partial 时，已满足部分和未满足部分均有证据及诊断 | 有限交付，明确缺口和下一步 |
| `Blocked` | 能力、权限、用户决策、资源或外部依赖不可恢复/等待超期，且已保留已完成事实 | 当前不能安全完成，不伪造成功 |
| `Failed` | 执行或验证确定失败，所有有界恢复已耗尽；失败原因和可重试性已类型化 | 任务失败，可按诊断重试或修正 |
| `Cancelled` | 用户或系统取消已持久化并完成安全收尾 | 用户要求停止，已完成部分保留 |

`Running/Waiting/Replanning` 都不是终态。`Graph Completed`、`Team DeliveryStatus::Satisfied`、
`TeamWorkingState::verify_completed_graph` 只能是局部执行/证据投影：它们可以报告“该 Team
交付包已形成”，不能写入或暗示 Objective `Satisfied`。现有 `TeamResultReducer` 和
`project_team_terminal_outcome` 必须保留其局部用途，但输出改名/加上 execution-scope 标识，
由 `ObjectiveSupervisor` 统一把局部事实归并成最终业务结果。

终态提交必须同时满足：`objective_id + authority_revision + terminal_fence` 唯一、CAS 成功、
事件追加与 Evidence Ledger 同事务（或可重放 outbox），重复提交返回同一结果；重启、迟到的
Agent/Provider 回执和旧 projection 只能被拒绝或作为诊断，不得让终态倒退或从失败变成功。

### 2.4 单一状态事实表

| 状态 | 唯一 owner | 持久载体 | 禁止的第二真相 | 修订/恢复 |
| --- | --- | --- | --- | --- |
| Objective/Obligation | `ObjectiveSupervisor`（唯一决策者）→ `GoalStore`（唯一持久写者；无第二调度器） | goal event stream | Mission map、host transcript、Team 结果 | goal revision + CAS；重启 replay |
| Program lifecycle/obligation | graph commit service（唯一生命周期写者）；Coordinator 只能发 command | execution graph stream | host `verified_team_ids`、工具文本、Team reducer | graph revision/fence；reconcile |
| Task offer/claim/lease | Task Market（唯一任务状态写者；Graph/Task aggregate 只提供存储/CAS） | task/work state + event | Agent 本地“已领取”标志、Team control map | claim token、generation、expiry |
| Agent lifecycle | `AgentRuntime`（唯一实例事实写者）；InProcess/Managed 只是 worker adapter | agent event stream | Managed Agent 自有终态表、worker 内存状态 | instance revision、stale result reject |
| Capacity | `ExecutionResourceManager`（唯一容量决策者）+ frozen profile | Program resource ledger | Gateway semaphore、Team semaphore、Agent 私有 ceiling | profile digest、admission lease |
| Approval | `ApprovalCoordinator`/queue | approval event/outbox | orchestration busy poll | deadline/Notify/CAS |
| Artifact/Evidence | Artifact store 写入事实；Objective Evidence Ledger 唯一归并/资格决策者 | event/outbox + artifact store | model prose、重复字符串 carrier、Team working-state 副本 | content digest、reread receipt |
| Projection | Runtime reducer -> Gateway/Edge | snapshot/delta cursor | UI-local inferred status | schema + cursor + resync |
| Terminal outcome | ObjectiveSupervisor durable commit | outcome event | graph Closed、terminal paragraph | exactly-once terminal fence |
| Initiative proposal | Agent/Team 可提出；ObjectiveSupervisor 唯一接纳/拒绝，Task Market 唯一落盘 | proposal/revision event | Runtime 自动代提案、固定提案计数、prompt 文本 | semantic-delta digest、authority revision、幂等去重 |

### 2.5 任务市场和主动 Agent

```text
Task Offered -> eligible capability match -> Agent pull -> claim lease -> heartbeat
-> model/tool effect -> artifact/evidence commit -> accept/challenge -> release/replan
```

不新增独立 scheduler。现有 `ExecutionResourceManager` 继续负责容量；Task Market 只是
Runtime graph/task state 内的有类型协议。Agent 可以主动 pull/claim，并可通过
`InitiativeProposal` 主动提出新 Task、依赖修订、协作请求或受权限/实时资源约束的 Team/Role 扩展；但提案
必须回到同一 Objective authority 和 Task Market，不能由 Agent 直接创建 Runtime identity。
必须满足：

- 一处持久 offered-task 事实和一处 claim owner；
- claim、heartbeat、submit、accept、challenge 全部幂等且带 revision/generation；
- dependency pending 的 Agent 不计 active；
- 无 effect/evidence receipt 不得标记任务完成；
- discussion 必须产出 artifact 或 decision，不能只制造文本；
- Agent 提案不要求固定比例/固定数量；只要能证明与未闭合 Obligation 的语义关联、可执行
  producer、证据路径和资源可行性即可被接纳；无价值或重复提案应得到可解释的 `rejected`
  或 `deduplicated` 回执，而不是 Runtime 替 Agent 伪造成功动作；
- 接纳后的提案沿同一 `offered -> claim -> effect -> evidence -> verify` 链运行，不能绕过
  capability closure、用户固定约束、权限和终态 fence；
- 同一 fairness key 有序，不同 key 可并行；
- 不为每个冷任务永久创建进程；
- 任务市场空闲时 Agent 可等待事件，不得 busy poll。

### 2.6 目标级终态与恢复

```text
workers terminal + unresolved obligations
  -> classify gap
  -> retain completed facts
  -> create minimal missing-task / intent revision
  -> reopen market under same authority/budget fence
  -> verify, partial, or fail-closed with typed diagnostic
```

`autonomous_work_is_orphaned` 不能再是最终处理。它应成为 `ObjectiveRevisionRequired` 的
输入。仅当缺口不可恢复、预算耗尽、用户固定约束冲突或 provider 明确不可用时，才进入
终态 Blocked/Failed。重复同一语义错误必须去重并停止，不得无限请求模型。

### 2.7 Capability closure

Provider dispatch 前必须已经证明每个必需 Obligation 具备：

- 注册的可执行 producer；
- 精确 Agent/Skill/Tool revision；
- 有效权限交集；
- 物理 ready dependency；
- artifact 持久化和 reread 路径；
- 独立验证 predicate；
- policy-compatible resource/budget reservation。

缺少网络研究能力必须在 admission 返回 `capability_gap`；缺少终态读取能力不能等几十轮
后才由 verifier 发现；禁止从 role name、builtin、template default、prompt 或 capability
metadata 推导能力。

### 2.8 并发、背压和锁

- 编译和 registry lookup 在 Program mutation lock 外完成；只在短 CAS commit 内串行。
- 不同 Program/Session/Mission 并行；同一 Program revision commit 串行。
- 无依赖的 Team/Role 并行，join 按显式 `all/any/quorum`；不使用全局 graph lock。
- `ResourceManager` 是唯一容量队列；不新增 Team/Agent/Gateway 第二 semaphore。
- admission 先预留容量，再 hydrate context/provider；provider/tool/subscriber wait 不持有全局锁。
- 队列按 provider/account/model/Agent/Tool 等 key 公平；限制 batch、bytes、lease、retry、memory。
- 记录 p50/p95/p99 的 admission、claim、provider、tool、terminal 延迟和最大队列年龄。

### 2.9 Context 和缓存

上下文分层：

```text
稳定前缀：系统规则、工具 schema、已发布 Definition/Skill 说明、项目长期事实
动态后缀：当前目标、Agent identity、lease、live state、最新工具结果、用户新输入
```

cache key 至少包含 provider/model、schema、tool contract、policy、skill/definition revision。
只报告：eligible-warm、cold-inclusive、cache read/write、output/reasoning、retry 和业务价值。
不以 99% 校准结果承诺真实动态 Team 的全局 90%。观察器使用事件 cursor 和增量 projection，
不得重复拉取完整 root 或固定时间线前缀。

## 3. 当前代码事实、迁移和删除矩阵

| 当前边界/符号 | 当前事实 | 目标 owner | 迁移/删除动作 | 版本 |
| --- | --- | --- | --- | --- |
| `harness-contract/src/goal/mod.rs:GoalContract`, `GoalProgressSnapshot` | 有 criteria、observation、cost、evidence，但无 Team obligation 状态 | Objective contract | 增加 typed Obligation/producer/evidence/terminal diagnosis；保留旧字段只用于一次性读取迁移 | 716 |
| `runtime/src/execution_core/goal/mod.rs:GoalStore`, `GoalProgressReducer` | 事件源化，但只负责 goal observation | ObjectiveSupervisor 协调器 + GoalStore 事实 | 新增 reconcile/terminal APIs；禁止新增内存目标 map | 716 |
| `runtime/src/mission/mission_runtime.rs:MissionRuntime` | `Mutex<BTreeMap>` + 可选 event store，Mission 生命周期与执行投影混合 | Mission projection/organization | event-store 成为生产必需；执行终态改读 Objective outcome；移除 map-only terminal decision | 716 |
| `harness-contract/src/execution_graph/contract.rs:CollaborationProgram*` | 已有 lifecycle、obligations、edges、semantic intent、resource ledger | Program immutable execution contract | 增加 per-obligation outcome、replan reason、terminal fence、reread/evidence refs；禁止新增第二 Program struct | 716 |
| `runtime/src/orchestration/collaboration_coordinator.rs:{prepare_program_admission,reconcile_terminal_program,reconcile_terminal_program_with,reconcile_program_wait_state_with}` | admission、delivery、terminal、experience、startup recovery 集中但职责过宽 | Program admission + ObjectiveSupervisor adapter | 保留 admission/edge reconcile；终态判断下沉到 ObjectiveSupervisor；删除活动计数终态分支 | 716 |
| `runtime/src/execution_core/graph/runner.rs:{autonomous_work_is_orphaned,autonomous_work_blocks_terminal}` | 发现孤立工作后直接失败 | Graph facts -> Objective revision signal | 改为 typed gap event；保留 Graph fail-closed 保护但不代替 Objective replan | 716 |
| `runtime/src/execution_core/graph/commit_service.rs:UpdateCollaborationProgramControl,merge_collaboration_program` | 多处更新 Program control | Graph commit authority | 集中 terminal/replan commit；禁止 coordinator/host 直接写生命周期 | 716 |
| `runtime/src/execution_core/services.rs:project_team_terminal_outcome` | Team 图全节点终止后生成 `OutcomeTerminalClass::Succeeded`，容易被误读为业务成功 | Team execution observation adapter | 保留 Team 局部 execution outcome，但显式标记 scope；不得写 Objective terminal；由 Supervisor 消费并归并 | 716 |
| `runtime/src/team/result_reducer.rs:TeamResultReducer/build_delivery_envelope` | 由 Team 图和 Verify 节点生成 `DeliveryStatus` | Team delivery projection | 保留局部 envelope/evidence bundle；禁止将 `DeliveryStatus::Satisfied` 当作 Objective `Satisfied`；补 scope 和反误判测试 | 716 |
| `runtime/src/team/working_state.rs:{verify_completed_graph,terminal_working_state_event}` | Team working-state 物化与局部挑战/证据检查 | Team evidence projection | 只写 Team-local evidence/diagnostic；不能声明 Objective 终态；补独立 verifier/reread 关联 | 716/717 |
| `runtime/src/orchestration/facade.rs:{submit_runtime_orchestration_request,submit_collaboration_intent_patch,...}` | 多个 ingress 形状 | Semantic ingress adapter | 所有 Team ingress 先进入同一 `FrozenSemanticIntent` decoder；删除第二语义 codec | 716 |
| `runtime/src/conversation/host.rs` 的 `verified_team_*`, `completed_program_*`, `collaboration_program_progress_from_graph`, `root_acceptance_disposition` | transcript、tool receipt、内存状态参与成功判断 | typed Program/Objective projection consumer | 只保留展示和兼容解析；删除 production terminal authority 和 activity heuristic | 716 |
| `runtime/src/conversation/host_backend.rs` 的 control-plane repair budget/terminal prompts | prompt 负责补救缺口，容易循环 | ObjectiveSupervisor diagnostic consumer | prompt 只能展示可修订字段；删除无限/重复修复路径 | 716 |
| `runtime/src/task/store.rs`, `task/runtime_port.rs` | Task backend、CAS、outbox、binding 已存在 | Task Market durable store | 补 TaskOffer/Claim/Lease/Receipt typed events；复用 outbox，不新建 store | 717 |
| `runtime/src/team/team_runtime.rs:{collaboration_control_view,admit,admit_or_resume}` | Team child graph、control view、admission 已有；主动性仍受 root 驱动 | Team adapter over Task Market | 将 control view 改为 event-driven pull view；保留 child graph owner；删除本地推导 active 的业务完成语义 | 717 |
| `runtime/src/agent/runtime.rs`, `agent/in_process_worker.rs`, `agent/managed_agent.rs` | AgentRuntime 事件源；InProcess 和 Managed 两条 backend | Agent worker adapter | 统一 claim/effect/terminal receipts；Managed 只适配同一协议；删除重复 lifecycle truth | 717 |
| `runtime/src/agent/in_process_worker.rs:{team_requires_autonomous_market,designated_autonomous_proposer_nodes,ensure_required_autonomous_proposal,runtime_default_autonomous_proposal_request}` | 当前评测标记触发固定比例提案，模型漏提案时 Runtime 代做；system prompt 还禁止嵌套 Team/Session | InitiativeProposal membrane + Agent worker prompt | 删除固定提案比例、Runtime 代提案和“不得扩展”绝对提示；改为 Agent 可选提案、Runtime 校验身份/权限/能力/资源/范围 | 717 |
| `runtime/src/orchestration/team_authority.rs`, `runtime/src/team/instantiation.rs`, `runtime/src/team/result_reducer.rs` | focus overlap、novelty、terminal/reducer facet 多处生成和消费 | `AutonomyPolicy`/Team-local quality projection | 约束仅作可解释的质量信号或用户声明的 policy；不得因观察到重叠而回溯拒绝有效业务结果；禁止按 role name/活动数量强制行为 | 716/717 |
| `runtime/src/execution_core/graph/resources/manager.rs:ExecutionResourceManager` | 已是公平容量 manager | 唯一 capacity owner | 接受 frozen profile、Task Market demand、backpressure metrics；删除独立 semaphore/魔法 ceiling | 717 |
| `runtime/src/projection/*`, `harness-contract/src/execution_graph/projection.rs` | snapshot/delta、graph/projector 多个投影层 | Runtime canonical projection | 增加 Objective/obligation/claims/evidence/diagnostics；cursor 增量和 resync | 717 |
| `gateway/src/runtime/gateway_tool_executor.rs`, `gateway/src/services/mission_service.rs`, `gateway/src/core/event_bus.rs` | Gateway 既投影又参与 runtime 连接/缓存 | Gateway transport/projection fanout | 删除执行真相推导；只转发 typed projection；建立 build identity 与 cursor gate | 717 |
| `cowd-edge/surfaces/webui/src/stores/projectionRegistry.ts`, `adapters/executionProjection.ts`, `components/runtime/CollaborationProgramSummary.vue`, `types/*`, generated API | 前端已有 projection v3、Team summary、重连逻辑，但不能表达完整 Objective outcome | Surface projection/render | 同步合同生成、显示 Team/Agent/Task/claim/evidence/diagnostic/next action；不在 UI 推导完成 | 717 |
| `crates/harness-eval/src/certification.rs`, `live_scenario_runner.rs`, `report.rs`, `terminal_gate.rs`, `terminal_matrix.rs`, `runner.rs` 及 manifest 列出的测试 | 旧测试混合活动计数、文本结果和真实业务成功 | Harness Eval | 迁移为 objective/evidence verdict；删除 false-positive fixtures；固定拓扑仅保留压力测试标签 | 716/717 |

### 删除前置表

| 删除目标 | 依赖和丢失风险 | 替代者 | 删除证明 |
| --- | --- | --- | --- |
| Host 的 transcript terminal authority | 依赖大量 conversation tests；会丢失旧文本兼容 | typed Program/Objective projection | `rg` production scope 无 `verified_team`/`completed_program` authority call；兼容测试只验证展示 |
| 第二 Team ingress codec | Gateway schema、tool fixture、intent tests | single semantic decoder | `cargo tree` + decoder property tests + old codec raw scan |
| Mission map-only terminal | Mission service、schedule tests | Goal/Objective event stream | restart/replay test + map-only constructor forbidden in production |
| Team/Managed Agent 私有 claim truth | Team/Agent tests、recovery | Task Market/AgentRuntime receipt | claim conservation、stale lease、duplicate submit tests |
| activity-count success gates | Harness eval templates/rubrics | durable obligation/evidence verdict | evaluator mutation tests：删掉活动但保留业务证据仍 PASS，伪造活动无证据 FAIL |
| prompt-only terminal gate | harness prompts、repair tests | ObjectiveSupervisor terminal invariant | negative source scan + orphan/replan fault test |
| full-root polling/固定时间线快照 | Harness Eval observer | cursor/event projection | unchanged poll budget and byte ceiling tests |

## 4. 三版本实施落地方案

### v0.9.716 — Objective/Program Truth 与确定性恢复

**版本边界：** 把“业务目标完成”和“执行图结束”分开，建立唯一 Objective/Program 事实、
单语义 ingress、能力 closure 和目标级修订；不实现新的 Surface 视觉、不跑真实 Provider/E2E。

**允许修改的核心路径：**

```text
crates/harness-contract/src/goal/mod.rs
crates/harness-contract/src/execution_graph/{contract.rs,state.rs,validation.rs}
crates/harness-contract/src/acceptance.rs
crates/runtime/src/execution_core/goal/{mod.rs,policy.rs,supervisor.rs [new]}
crates/runtime/src/execution_core/{supervisor.rs,services.rs}
crates/runtime/src/execution_core/graph/{runner.rs,commit_pipeline.rs,commit_service.rs,events.rs}
crates/runtime/src/orchestration/{facade.rs,compiler.rs,collaboration_coordinator.rs,
  intent_compiler.rs,validator.rs,mod.rs}
crates/runtime/src/mission/mission_runtime.rs
crates/runtime/src/conversation/{host.rs,host_backend.rs}
crates/runtime/src/orchestration/tests/mod.rs
crates/runtime/src/execution_core/tests/{commit.rs,services.rs}
测试和 fixture 的精确清单见
`docs/architecture/autonomous-objective-runtime-source-manifest-v0.9.716-718.md`。
```

**实现顺序：**

1. 在 `goal` 合同中增加 `ObjectiveObligation`、`ProducerContract`、`EvidenceRequirement`、
   `ObjectiveTerminal`、`ObjectiveRevisionReason` 和 `ObjectiveDiagnostic`；所有新字段带 schema
   version、digest、revision、idempotency key。旧历史可读但不能作为新运行时写路径。
2. 在 `GoalStore` 增加原子创建/修订/观察/终态接口；新 `ObjectiveSupervisor` 只协调 GoalStore、
   `RuntimeExecutionSupervisor`、`TaskRuntimePort`，不创建队列、不创建第二 scheduler、不持有
   provider/tool await。
3. 将 `CollaborationProgramControlState` 扩展为每个 Team obligation 的 execution、delivery、
   artifact/evidence、retry disposition、replan requirement；所有状态更新经
   `commit_service` 的 revision-fenced command。
4. 把 `TeamResultReducer::DeliveryEnvelope`、`TeamWorkingState` 和
   `project_team_terminal_outcome` 明确降级为 Team-local execution/evidence projection：它们
   可以记录局部交付包，但不得直接产生或被消费为 Objective terminal `Satisfied`。所有局部
   `Succeeded/Satisfied` 结果必须带 execution scope、source graph revision 和 evidence refs，
   再由 `ObjectiveSupervisor` 归并。
5. 将 `autonomous_work_is_orphaned`、provider/tool failure、lease loss 转换为 typed objective
   gap；Supervisor 保留已完成事实，仅创建最小缺口任务或一条有界 intent revision。
6. 让所有 Team ingress 经同一 semantic decoder；transport repair 仅修形状，禁止 builtin、
   display-name、默认 template、反转依赖或弱化 acceptance。
7. 将 capability closure 前移到 `intent_compiler`/`TeamInstantiationCompiler`，在 provider call
   前拒绝缺 producer、缺 tool、缺 read/reread、缺物理依赖或缺证据的拓扑。
8. 增加 `InitiativeProposal` 的语义合同和 Supervisor admission：Agent 能主动提出缺口 Task、
   依赖修订、协作者请求或有限 Team/Role 扩展；Runtime 只校验并返回类型化结果，不代模型
   生成工作，也不要求固定提案比例或活动数量。
9. 将 Host/HostBackend 的终态判断改为消费 typed Program/Objective terminal；保留自然语言
   narration 作为展示字段；删除生产环境 transcript/活动计数 authority。
10. 将 MissionRuntime 的生产初始化改为 event-store required；Mission 只能组织和投影，不能
    独立声明执行成功。

**必须删除或禁止残留：**

- Host `verified_team_*`、`completed_program_*` 等 production success authority；
- 第二 Team semantic codec；
- 只凭 Graph Closed 或模型终态文本完成 Objective；
- 只凭 Team `DeliveryStatus::Satisfied`、Team `OutcomeTerminalClass::Succeeded`、
  `verify_completed_graph` 或 Team working-state materialization 完成 Objective；
- orphan 后直接终止而无 objective revision signal；
- capability 失败在 provider dispatch 后才发现；
- 用 `designated_autonomous_proposer_nodes`、`ensure_required_autonomous_proposal` 或 Runtime
  default proposal 代替 Agent 自主判断；
- 用叶子 Agent 的绝对提示禁止在授权范围内提出 typed initiative；
- Mission map-only production terminal path。

**只做代码层验证：**

- contract schema round-trip、旧历史 decode、新字段必填和 digest/revision；
- semantic decoder property/fuzz：别名、包装、乱序、缺字段、反向依赖、未知能力；
- Objective supervisor 状态机：正常、缺口、重规划、预算耗尽、不可恢复、取消、重启；
- stale CAS、duplicate command、terminal exactly-once、replan retains completed facts；
- Objective 终态格（Satisfied/Partial/Blocked/Failed/Cancelled）单调性、重复 terminal 提交
  返回同一 outcome、迟到回执不得逆转结果；
- Team-local `DeliveryEnvelope`/execution outcome 与 Objective terminal 隔离：即使 Team 图
  全部 Completed 且局部 delivery 为 Satisfied，Objective 仍须等待所有 Obligation evidence
  和 reread/verifier 事实；
- Agent `InitiativeProposal` 可在同一 Objective 内创建缺口 Task/协作请求/依赖修订，提案
  不受固定数量约束，重复语义被去重，越权提案被拒绝且不丢失已完成证据；
- capability closure negative tests；
- Host 与 Program projection 不一致时以 Program 为准的测试；
- Mission event-store replay；
- 禁止符号和 owner scan；
- changed dependency cone `cargo fmt/check/test`，不调用真实 Provider。

**v0.9.716 完成声明：** 只能声称 Objective/Program 的确定性事实、能力 admission 和目标级
恢复在代码和故障注入层完成；不能声称 Agent 主动领取、Surface 完整或真实模型业务完成。

### v0.9.717 — Event-driven Agent、Projection、并发与成本

**版本边界：** 让 Agent 可以主动 pull/claim，统一 Agent/Team worker 生命周期，补齐前端和
Gateway typed projection，消除并发/观察/缓存的重复成本；继续禁止真实 Provider、浏览器和业务 E2E。

**允许修改的核心路径：**

```text
crates/harness-contract/src/{task.rs,agent/mod.rs,team/mod.rs,
  execution_graph/{contract.rs,projection.rs,state.rs}}
crates/runtime/src/task/store.rs
crates/runtime/src/task/runtime_port.rs
crates/runtime/src/task/lifecycle.rs
crates/runtime/src/agent/runtime.rs
crates/runtime/src/agent/managed_agent.rs
crates/runtime/src/agent/in_process_worker.rs
crates/runtime/src/agent/run_handle.rs
crates/runtime/src/team/team_runtime.rs
crates/runtime/src/team/projection.rs
crates/runtime/src/team/working_state.rs
crates/runtime/src/execution_core/graph/resources/manager.rs
crates/runtime/src/execution_core/execution_live.rs
crates/runtime/src/projection/mod.rs
crates/runtime/src/projection/delta.rs
crates/runtime/src/projection/snapshot.rs
crates/runtime/src/projection/activity.rs
crates/runtime/src/conversation/prompt_assembly.rs
crates/runtime/src/context/tool_exposure.rs
crates/runtime/src/conversation/context_plane.rs
crates/runtime/src/orchestration/collaboration_coordinator.rs
crates/runtime/src/orchestration/collaboration_continuation.rs
crates/runtime/src/execution_core/services.rs
crates/gateway/src/runtime/gateway_tool_executor.rs
crates/gateway/src/core/event_bus.rs
crates/gateway/src/services/mission_service.rs
crates/gateway/src/services/mod.rs
crates/gateway/src/api_routes/mission_routes.rs
crates/gateway/src/api_routes/session_routes.rs
crates/gateway/src/api_routes/capability_contract.rs
crates/gateway/src/api_routes/route_registry.rs
crates/gateway/src/runtime_host/mod.rs
crates/gateway/src/runtime_host/task_set.rs
cowd-edge/crates/edge-contract/src/lib.rs
cowd-edge/crates/edge-contract/src/message.rs
cowd-edge/crates/edge-contract/src/edge_v2_generated.rs
cowd-edge/crates/edge-adapters/src/lib.rs
cowd-edge/crates/edge-adapters/src/mirror.rs
cowd-edge/contracts/edge/v2/schema.json
cowd-edge/surfaces/webui/src/stores/projectionRegistry.ts
cowd-edge/surfaces/webui/src/stores/liveTransport.ts
cowd-edge/surfaces/webui/src/adapters/executionProjection.ts
cowd-edge/surfaces/webui/src/components/runtime/CollaborationProgramSummary.vue
cowd-edge/surfaces/webui/src/components/runtime/ExecutionTruthSummary.vue
cowd-edge/surfaces/webui/src/types/graph.ts
cowd-edge/surfaces/webui/src/types/evidence.ts
cowd-edge/surfaces/webui/src/generated/gateway-api.ts
cowd-edge/surfaces/webui/src/generated/projection-contract-meta.ts
cowd-edge/surfaces/webui/src/generated/live-contract-meta.ts
cowd-edge/surfaces/webui/src/generated/projection-v3-golden.ts
cowd-edge/surfaces/webui/src/i18n/keys.ts
cowd-edge/surfaces/webui/src/i18n/messages/zh-CN.ts
cowd-edge/surfaces/webui/src/i18n/messages/en-US.ts
test files are enumerated in
`docs/architecture/autonomous-objective-runtime-source-manifest-v0.9.716-718.md`;
no unlisted test file may be edited without an allowlist amendment.
```

**实现顺序：**

1. 为 Task 增加 `Offer/Claim/Heartbeat/Submit/Accept/Challenge/Release` typed events；claim
   owner 统一为 Task Market，复用现有 Task outbox 和 Graph state，增加 at-most-once terminal、
   claim conservation、lease generation 和 fair key。
2. `TeamRuntime::collaboration_control_view` 只提供当前 Agent 有权 pull 的任务、能力和事实；
   Agent 不再等待 root 每轮投递。`AgentRuntime`、InProcess、Managed backend 共用 receipt 协议。
3. 保持现有 Graph/ResourceManager 的 scheduler owner，删除 Team-local semaphore、active heuristic、
   重复 claim map；按 key 顺序、跨 key 并发，所有 provider/tool await 在锁外。
4. 删除 `team_requires_autonomous_market`、`designated_autonomous_proposer_nodes`、
   `ensure_required_autonomous_proposal` 和 Runtime default proposal；移除“必须一半 Agent
   提案”以及“叶子 Agent 不得创建嵌套 Team/Session”的绝对提示。替换为稳定的自治说明：
   Agent 可以通过 typed `InitiativeProposal` 提出子 Task、协作请求或有限拓扑扩展，但不能
   直接创建 identity、修改权限/租约或伪造 terminal；提案是否有价值由 Objective/Task
   Market 的语义关联、能力闭合和证据结果判断。
5. 完成 Program -> Team -> Agent -> Task -> Artifact -> Evidence 的 typed projection，增加
   objective/obligation、claims、leases、diagnostic、next_action 和 build identity；Gateway 只
   传输，Edge/WebUI 只 reducer/render，schema/cursor 不匹配必须 resync。
6. 将 `collaboration_continuation.rs` 中只为根提示补救而存在的路径迁移为 typed objective
   revision consumer；删除重复 prompt repair budget 和固定 root projection polling。
7. 重排 prompt/context：稳定背景与 schema 在前，动态身份、状态、结果在后；cache key 含
   provider/model/schema/tool/policy revision；缓存统计拆为 warm/cold、读写、输出、重试和业务价值。
8. 观察器改为事件 cursor 增量；无变化不计进度，单次 root/timeline 字节、poll、backoff 有上限。
9. 更新所有 Rust/WebUI/generated fixtures 和测试断言：业务 verdict 基于 Objective outcome 和
   Evidence receipts，不基于活动计数或文本摘要。

**必须删除或禁止残留：**

- Agent/Team 私有 claim/lifecycle truth；
- 固定比例/固定数量的自治提案、Runtime 代提交提案和仅为满足评测而产生的协作动作；
- 以“禁止嵌套 Team/Session”的 prompt 取代 typed initiative membrane；
- root-only task delivery 作为唯一执行路径；
- Gateway/Edge 自己推导 terminal；
- 全量 root/timeline 重复 polling；
- 把 cache calibration 当作真实业务 SLO；
- 用模型 round/bid/review 数量替代 evidence verdict；
- 旧 projection fixture 继续验证已删除的语义。

**只做代码层验证：**

- 多 Agent pull/claim/lease/heartbeat/submit 的 deterministic fake provider 测试；
- InitiativeProposal 的自由策略测试：0、1、多项提案均可正确收敛；Agent 主动提案、互相
  邀请/拒绝、依赖修订、有限 Team 扩展均走同一 admission；越权、重复和无证据提案被类型化
  拒绝，不触发 Runtime 代做；
- 10/100/1000 个 task 的 claim conservation、重复提交、租约过期和恢复；
- 不同 key 并发、同 key 串行、队列公平、背压、内存/bytes 上限；
- 重叠治理测试：同一输入被多个 Agent 读取时区分有意交叉验证与无效重复；质量信号不能
  回溯否决已闭合义务，且不存在第二 overlap/novelty owner；
- provider/tool/network/storage/subscriber wait 不持有 global/program lock 的 instrumentation；
- Projection snapshot/delta/gap/resync、Gateway/OpenAPI/Edge generated parity；
- WebUI reducer/component 单测、断线重连和旧 revision 拒绝；
- prompt segment/cache key 单测和受控本地 stub 命中统计；
- `cargo fmt/check/test`、TypeScript typecheck/unit/build、forbidden/owner/dependency scans；
- 不执行真实模型、真实浏览器和真实付费场景。

**v0.9.717 完成声明：** 只能声称事件驱动 Task Market、统一 worker/projection、并发/背压和
缓存分层已通过代码层验证；不能声称真实 Provider 或最终业务成功。

### v0.9.718 — 集成候选、真实 Provider/Browser 和业务终态验收

**版本边界：** 只集成已完成的 716/717，不再扩展架构；在同一 commit/tree/build identity 下
完成安装服务、Gateway、Edge、WebUI、真实 DeepSeek、工具、持久化、投影和浏览器的端到端验证。

**执行前硬门：**

1. 两仓库 clean、版本依赖 DAG、allowlist、删除扫描、全 workspace compile/test、Edge generated
   API、安装包 build identity 全部通过。
2. 用 deterministic fake provider 重跑完整目标场景，确认每个 Obligation 都可被证据闭合；
   失败必须归属 716 或 717，不得在 E2E 中新增架构。
3. 独立环境、独立 storage/config、独立端口；不得使用旧 8642 服务、旧 Edge bundle 或旧浏览器缓存。
4. 记录 provider/model/no-fallback、token budget、cache cohort、poll budget、resource profile，
   但不把预算转换成固定 Agent/Team 活动上限。

**真实场景递进：**

```text
1 Team / 2 Agents：目标理解、一个可验证产物和 terminal reread
2 Teams / 4 Agents：独立研究 + 综合，验证 handoff/evidence
3 Teams / 6 Agents：加入 review/challenge、provider/tool failure 和自动 replan
4 Teams / 8 Agents：加入并发、重启、lease loss、前端实时投影
4 Teams / 16 Agents：仅在前述层级通过且业务目标确实受益时执行压力验收
```

最终业务场景采用此前未闭环的“群论在当前 AI 中的应用调研和测试测评”类任务，要求：

- 研究角色有真实网络/资料能力；
- 分析角色能读取研究产物；
- 测评角色能在隔离环境执行测试；
- reviewer 能读取并挑战证据；
- synthesis 能重新读取所有最终产物；
- 输出带来源、方法、实验结果、限制、结论和可重放证据；
- 中途失败可自动补齐缺口；
- 至少一个故障/证据缺口场景由 Agent 自主提出并完成有效 `InitiativeProposal`；简单目标
  不得因没有提案被判失败，复杂目标也不得由 Runtime 预先注入“必需提案”；
- 对同一场景记录 Agent 实际选择的任务、工具、协作者和重规划原因，证明 Runtime 没有
  用固定角色、活动配额或模型轮数替代策略判断；
- 浏览器能看到每个 Team/Agent/Task/Claim/Evidence/Diagnostic/终态；
- 终态由 Objective/Evidence 判定，不由活动数量或模型最后一句话判定。

**最终验收门：**

- 业务 Objective 为 `Satisfied`；所有 required Obligation satisfied；
- Objective 终态属于终态分类之一且不可逆；Team/Graph 局部 `Completed/Satisfied` 不能
  单独提升为 Objective `Satisfied`；`Partial/Blocked/Failed/Cancelled` 必须展示明确缺口、
  原因、可重试性和下一步；
- 每个结果均有 durable artifact、evidence ref、reread receipt 和 verifier decision；
- 无 unresolved task、active lease、orphan work、未补偿失败或物理未就绪依赖；
- root、Program、Graph、Team、Agent、Task、Surface 的 identity/revision/cursor 一致；
- crash/restart、provider stream loss、tool partial effect、cancel/deadline、subscriber lag、
  stale write 至少各执行一次；
- 记录模型轮数、输入/缓存/输出/重试成本、observer 字节和业务价值；
- 浏览器从真实入口发起、看到完整 Team 信息和最终结果；
- 安装服务与源码同 SHA，生成合同无漂移；
- 真实 DeepSeek 失败只能进入 typed external failure，不得伪造成功或无限重试。

**v0.9.718 完成声明：** 只有上述所有硬门同时通过，才可以报告“复杂自主协同业务闭环完成”。

## 5. 测试改革和验收契约

### 5.1 测试层次

| 层级 | 内容 | 费用 | 是否在 716/717 |
| --- | --- | ---: | --- |
| Contract | schema、serde、revision、digest、禁止 fallback | 0 | 是 |
| State/property | Objective/Program/Task/Agent 状态机、守恒、幂等 | 0 | 是 |
| Fault | crash、restart、lease、provider/tool、cancel、stale、shutdown | 0 | 是 |
| Concurrency | key 顺序、cross-key 并行、公平、背压、内存和字节 | 0 | 是 |
| Projection | snapshot/delta/resync、Gateway/Edge generated parity | 0 | 是 |
| Deterministic scenario | fake provider + real Runtime/Tool/Storage/Surface adapter | 0 | 是 |
| Real provider | DeepSeek 指定模型和真实工具 | 有 | 仅 718 |
| Browser E2E | 真实前端、安装服务、浏览器、Session/Team 展示 | 有 | 仅 718 |

### 5.2 测试迁移原则

- 删除“只看到 capability metadata 就算成功”的断言；改为真实 effect/evidence receipt。
- 删除“Graph lifecycle completed 就算业务成功”的断言；改为 Objective obligation verdict。
- 删除固定角色名、固定活动数量驱动的生产判断；固定值只保留在压力 fixture。
- 任何旧测试若仍验证已删除路径，必须重写或删除，不能以兼容 shim 继续通过。
- Harness Eval 报告同时输出业务 verdict、运行时 verdict、可观测性 verdict、成本 verdict，
  四者不再合成一个模糊的 `passed`。

### 5.3 预算和并发门

预算控制器必须：

- 在 admission 前估算并冻结 token/时间/并发预算；
- 达到上限时停止新任务或请求用户决策；
- 不中断已持有租约的安全收尾；
- 不限制模型可以提出的合理任务数量，只限制 Runtime 可同时执行的资源；
- 暴露 provider 余额、429、capacity contraction 和 retry 作为外部失败；
- 对观察器设置独立 byte/poll/unchanged budget，防止监控制造费用。

## 6. 版本审计、提交和回滚

每个版本开始前必须重新生成：

```text
baseline snapshot / tree / index / worktree / untracked hashes
version write allowlist
capability -> owner -> version matrix
current symbol fact map
deletion preflight
test migration matrix
forward/reverse chain tables
```

每个版本结束前必须通过：

```text
cargo fmt --all -- --check
git diff --check
cargo metadata --format-version 1
cargo check --workspace --all-targets
cargo test <changed dependency cone>
forbidden symbol / dependency / owner scan
generated API and contract parity
phase evidence with exact commit/tree/build identity
```

版本间禁止保留一个“新旧路径都能走”的核心半状态。若发现实际代码与本方案事实不一致，
先修订本方案、DAG、allowlist 和审计报告，再编辑代码；不能在实施中临时改变边界。

回滚点：每个版本是单独 commit/tag；任何最终 E2E 失败必须回到拥有该能力的 716/717 版本，
禁止在 718 的测试层添加隐藏兼容路径。历史数据可以归档/清理，但不得让生产运行时同时维护
两套活动事实。

## 7. 实施准入条件

本方案只有在以下条件全部满足后才进入代码实施：

1. 统合方案审计报告为 `PASS`，且没有未命名的核心残留；
2. 每个版本的代码 allowlist、删除目标、替代 owner、调用者和测试迁移已确认；
3. 旧计划全部标记 `superseded`，本文件是唯一执行权威；
4. 两仓库基线 hash 已冻结；
5. 716/717 的真实 Provider/E2E 禁止门已加入脚本或 CI；
6. 最终 718 的模型、配置、独立 storage、浏览器和安装服务运行条件已预留；
7. 实施者可以使用较弱模型，但必须逐项照此文件执行，任何事实偏差先停在计划层修订。

未满足以上条件时，本任务的正确动作是继续审计和修订方案，而不是开始编写业务代码。
