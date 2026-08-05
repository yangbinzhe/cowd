# Runtime 业务执行模型与 Surface 三层投影方案

> 状态：终态方案；已完成第二轮源码、调用链、唯一真相和性能对抗审计  
> 审查日期：2026-08-05  
> 代码基线：`cowd@fbff3ba6`、`cowd-edge@bdb6f4ea` 及第 20 节登记的未提交修改  
> 目标：统一 Mission、Session、Task、Team、Agent、Skill、Tool 的业务语义、执行关系和
> Surface 展示，不新增第二套执行真相，不让前端猜测 Runtime 关系。

## 1. 结论

用户提出的方向正确，但需要固化以下边界：

1. `Session` 是交互和历史边界，`Mission` 是长期目标边界，`Task` 是可验收工作边界。
2. `ExecutionGraph` 是 Runtime 唯一的调度事实，不是新的业务主体。
3. `Team` 组织协作，`Agent` 执行推理，`Tool` 执行原子动作，`Skill` 提供可复用方法和能力。
4. 正文树、执行业务图、右侧活动详情必须消费同一份 canonical activity projection。
5. Mission/Task 图是宏观控制图；Execution 图是单次执行的业务协作图，不能混成一张无限大图。
6. 绑定到 Agent 但未使用的 Skill 只能出现在 Agent 详情中；实际激活或运行的 Skill 才进入执行过程。
7. Team、Agent、Tool、Skill 的状态必须由 Runtime 权威生命周期上卷，前端只负责显示、计数和折叠。
8. 私有思维链不展示；正文只展示 Runtime 明确标记为可公开的思考摘要。
9. `input_refs` 不应继续让模型猜层级和物理引用。模型只声明语义依赖，Runtime 解析实际输入。
10. 每个 Turn 在进入上下文准备前已经拥有唯一 Session ingress Execution；Skill、Provider、
    Team、Agent、Tool 和 Outcome 全部绑定该 Execution lineage，不允许产生无 Execution 的旁路活动。
11. Graph node 是调度身份，TeamRun/AgentRun 是运行身份；两者通过不可变 binding 形成一个
    canonical activity，不能各投影一个叙事节点再由前端去重。
12. 本方案不保留过渡合同、兼容字段、双写真相或前端 fallback；终态合同接通的同一版本必须
    删除被替代路径。

终态数据链：

```text
Canonical stores and Runtime lifecycle
                  |
                  v
ExecutionProjection
├── graph
├── child_executions
├── activities
├── activity_relations
├── teams / agents
├── approvals / outcomes / evidence
├── strategy / context / usage / recovery
└── live
                  |
                  v
Gateway snapshot + delta + live update + activity detail
                  |
        +---------+----------+
        |                    |
        v                    v
   WebUI/TUI             Other Surfaces
        |
        +── 对话正文纵向执行树
        +── Execution 业务关系图
        +── 右侧活动业务/技术模式
        +── Mission/Task 全景图
```

## 2. 七个业务对象和一个运行对象

### 2.1 对象关系

```text
Workspace
│
├── Session：在哪里交流、发生了哪些 Turn
│   ├── Turn 1
│   ├── Turn 2
│   └── Turn N
│
├── Mission：最终要实现什么长期目标
│   ├── Task A
│   ├── Task B
│   └── Task N
│
├── Agent Definitions
├── Team Templates
├── Skill Catalog
└── Tool Catalog

Turn
└── 产生或补充 Task

Task
└── 编译一个或多个 ExecutionGraph
    ├── 初始执行图
    ├── Replan 后的新执行图
    └── Recovery 后恢复的执行图

ExecutionGraph
├── Direct / Inline Model
├── Team Subgraph
├── Agent Task
├── Tool Batch
├── Approval
├── Verify
├── Synthesize
└── Session Dispatch
```

### 2.2 关系基数

```text
Mission  1 -------- N Task
Mission  N -------- N Session association
Session  1 -------- N Turn
Turn     1 -------- 0..N Task
Task     1 -------- 1..N ExecutionGraph

ExecutionGraph 1 -- N child executions
Team Run       1 -- N Agent Instances
Agent Revision 1 -- N Agent Instances
Agent Instance 1 -- N Agent Runs over time

Agent Binding  N -- N Skill references
Agent Binding  N -- N Tool contract references
Skill         0 -- N Tool references
Agent Run      1 -- N Tool invocations
```

### 2.3 所有权

| 对象 | 唯一职责 | 不承担 |
|---|---|---|
| Session | 对话、Turn、消息、输入队列、历史与恢复 | 团队编排、工具执行 |
| Mission | 长期目标、跨 Session/Task 聚合、全局控制 | Provider 循环、Tool 调用 |
| Task | 可验收工作、阶段、阻塞、重规划引用 | Agent 身份、聊天记录 |
| ExecutionGraph | 调度、依赖、并发、取消、恢复、完成合同 | 长期业务身份 |
| Team | 协作模板、角色、共享状态和团队结果 | 直接替 Agent 执行 |
| Agent | 推理、决策、使用能力、产出结果 | Session/Mission 生命周期 |
| Skill | 方法、规约、工作流和可复用能力包 | Runtime 生命周期所有权 |
| Tool | 有权限和副作用边界的原子动作 | 任务规划、Agent 生命周期 |

## 3. 定义、绑定、实例和运行记录

Team、Agent、Skill 和 Tool 不能只用一个名称表示所有生命周期。

```text
Agent Definition
    |
    | 发布 Revision
    v
Agent Definition Revision
    |
    | Runtime 解析模型、权限、Skill、Tool、Memory/Fact/Matrix 边界
    v
Agent Binding Snapshot
    |
    | 同一 Revision 可实例化多次
    +----------------------+----------------------+
    v                      v                      v
Agent Instance 1      Agent Instance 2      Agent Instance N
    |                      |                      |
    v                      v                      v
Agent Run 1           Agent Run 2           Agent Run N
```

```text
Team Template
    |
    | Runtime 根据目标、角色、数量、依赖生成
    v
Team Run
├── Role: researcher
│   ├── Agent Instance researcher-1
│   └── Agent Instance researcher-2
├── Role: reviewer
│   └── Agent Instance reviewer-1
└── Role: synthesizer
    └── Agent Instance synthesizer-1
```

Skill 需要区分四种状态：

```text
bound       Agent Binding 中允许使用，但本轮没有使用
activated   Prompt/Document Skill 已加载到本次 Agent 上下文
executing   Runtime package/MCP/Sidecar/Workflow Skill 正在执行
terminal    completed / failed / blocked / cancelled
```

`bound` 不属于执行节点；`activated/executing/terminal` 才能作为执行活动显示。

## 4. 自动编排的真实链路

```text
User message
    |
    v
Gateway admission
    |
    v
Session input envelope
    |
    v
Turn + context preparation
    |
    v
Model semantic proposal
├── direct
├── agent
├── team
├── review
├── synthesis
└── session_dispatch
    |
    v
Runtime authority binding
├── validate recipe and dependencies
├── resolve Team Template
├── resolve Agent Definition/Binding
├── bind Skill/Tool grants
├── bind Memory/Fact/Matrix lease
├── bind permission and approval
└── bind resource/capacity lease
    |
    v
Compile ExecutionGraph
    |
    v
Execution supervisor
├── start independent nodes concurrently
├── start Team child graph
├── start Agent child execution
├── execute Tool batches
├── collect evidence/artifacts
├── verify completion contract
└── replan/recover when governed
    |
    v
Terminal commit
    |
    v
Projection + Delta + Live update
    |
    v
Surface
```

模型只能提议“做什么、依赖谁、需要什么结果”。以下内容必须由 Runtime 决定：

```text
physical IDs
executor kinds
exact Agent revision/binding
exact Team run identity
leases and permissions
approval policy
idempotency keys
transactions
recovery and terminal commit
```

## 5. Execution 业务图

### 5.1 图要回答的问题

Execution 图只回答：

1. 本次执行要做哪些业务工作；
2. 当前做到哪里；
3. Team、Agent、Skill、Tool 如何协作；
4. 每一步产生了什么；
5. 哪些步骤真实并行，哪些存在依赖。

不在默认业务图展示：

```text
context assembly
provider request internals
token budget calculation
lease transition
cache hit/miss
projection commit
raw event protocol
private chain of thought
```

### 5.2 图形原型

```text
┌─────────────────────────────────────────────────────────────────────┐
│ Goal: 深度调研 WAIC 并生成 HTML 报告                     [运行中]  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ contains
              ┌─────────────────┴──────────────────┐
              │                                    │
              v                                    v
┌─────────────────────────┐          ┌──────────────────────────────┐
│ Team A: 深度调研         │          │ Team B: 网站生成             │
│ 运行中 · 2/3 Agent 完成  │          │ 等待 Team A 产物             │
└────────────┬────────────┘          └──────────────┬───────────────┘
             │ delegates                            │ consumes
      ┌──────┼──────────┐                           │
      │      │          │                           │
      v      v          v                           │
┌─────────┐ ┌─────────┐ ┌─────────────┐            │
│Agent A1 │ │Agent A2 │ │Agent 汇总   │            │
│官方资料 │ │企业案例 │ │等待输入     │            │
└────┬────┘ └────┬────┘ └──────┬──────┘            │
     │           │             │                   │
     │ invokes   │ invokes     │ consumes          │
     v           v             │                   │
┌──────────┐ ┌──────────┐      │                   │
│Tools 4/4 │ │Tools 3/4 │      │                   │
│成功 4    │ │成功 3    │      │                   │
└────┬─────┘ └────┬─────┘      │                   │
     │ produces    │ produces   │                   │
     v             v            v                   │
 [官方证据包]   [案例证据包] -> [调研综合报告] -----+
                                                      
                                      ┌───────────────────────────┐
                                      │Agent B1: 信息架构         │
                                      └────────────┬──────────────┘
                                                   v
                                      ┌───────────────────────────┐
                                      │Agent B2: HTML 实现         │
                                      │Skill: frontend-report      │
                                      │Tools: write/edit/validate  │
                                      └────────────┬──────────────┘
                                                   v
                                             [HTML 网站]
                                                   |
                                                   v
                                      ┌───────────────────────────┐
                                      │Agent B3: 验收审查         │
                                      └────────────┬──────────────┘
                                                   v
                                           [最终验收结果]
```

### 5.3 节点规则

| 节点 | 默认内容 | 点击详情 |
|---|---|---|
| Goal/Execution | 目标、状态、完成比例、耗时 | 完成合同、策略、结果、阻塞 |
| Team | 名称、目标、Agent 进度、状态、产物数 | Template、角色、共享状态、产物 |
| Agent | 名称/角色、目标、状态、耗时 | Definition、Binding、Skill/Tool 授权、结果 |
| Skill | 实际激活或执行状态 | Profile、版本、适配器、来源、入口、证据 |
| ToolGroup | 总数、完成/成功/失败/运行数 | 展开每个 Tool 调用 |
| Tool | 名称、状态、耗时 | Tool contract、输入、输出、审批、证据 |
| Approval | 待批/通过/拒绝 | 风险、作用域、批准模式、回执 |
| Artifact | 名称、类型、生产者 | 摘要、预览/下载、证据 |
| Outcome | 最终结果、验收状态 | 完成合同、未决项、证据 |

所有节点必须使用稳定的中文业务标签；内部 ID 只在详情中展示。

### 5.4 边

```text
contains       Execution/Team 包含子活动
delegated_to   发起者委派给 Team/Agent
invoked        Agent/Skill 调用 Tool
depends_on     后一业务步骤依赖前一步
produced       活动产生 Artifact/Outcome
consumed       活动消费 Artifact/Evidence
contributes_to 多个分支共同贡献给汇总
approved_by    动作由 Approval 放行
replanned_to   原计划重规划到新计划
recovered_from 新执行从失败执行恢复
```

时间重叠只证明“真实并行”，不能推断父子关系。父子、委派、调用、产出必须来自 Runtime
权威关系。

## 6. 对话正文纵向执行树

### 6.1 目标

正文回答“现在正在做什么”，保持可读、实时和紧凑，不承担完整审计职责。

### 6.2 运行中

```text
用户：调研 WAIC，然后让另一个团队生成 HTML 报告
│
├─ 思考摘要：任务包含调研和建站两个阶段，先并行采集证据
│
├─ Team A：深度调研                                      [执行中]
│  │
│  ├─ Agent A1：官方资料研究                             [执行中]
│  │  ├─ Skill  external-research                        [已激活]
│  │  ├─ Tool   web_search                               [完成  1.2s]
│  │  ├─ Tool   web_fetch                                [执行中]
│  │  └─ Tool   evidence_retrieve                        [等待]
│  │
│  ├─ Agent A2：企业与案例研究                           [执行中]
│  │  ├─ Skill  case-research                            [已激活]
│  │  ├─ Tool   web_search                               [完成  0.9s]
│  │  └─ Tool   web_fetch                                [完成  1.7s]
│  │
│  └─ Agent A3：证据综合                                 [等待 A1/A2]
│
└─ Team B：HTML 报告网站                                 [等待调研产物]
```

思考摘要按因果位置插入，而不是统一堆在工具之前或最终回复之后：

```text
思考摘要 -> 发起 Team/Agent -> Tool/Skill 活动 -> 阶段结果
          -> 新思考摘要 -> Replan/新委派 -> 后续活动
```

### 6.3 完成后的折叠

工具结束后先收成一行；Agent 产生产物后，Skill/Tool 子项默认收起；失败和待审批不自动收起。

```text
用户：调研 WAIC，然后让另一个团队生成 HTML 报告
│
├─ Team A：深度调研                               [完成 · 3 Agent]
│  ├─ Agent A1：官方资料研究                      [完成]
│  │  └─ Tools 3/3 · Skills 1 · 产物：官方证据包       [展开]
│  ├─ Agent A2：企业与案例研究                    [完成]
│  │  └─ Tools 2/2 · Skills 1 · 产物：案例证据包       [展开]
│  └─ Agent A3：证据综合                          [完成]
│     └─ Tools 1/1 · 产物：调研综合报告                 [展开]
│
├─ Team B：HTML 报告网站                          [完成 · 3 Agent]
│  ├─ Agent B1：信息架构                          [完成]
│  ├─ Agent B2：网站实现                          [完成]
│  │  └─ Tools 6/6 · Skills 1 · 产物：index.html       [展开]
│  └─ Agent B3：验收审查                          [完成]
│
└─ 最终结果：报告网站已生成并通过验收
```

折叠是 Surface 状态，不删除 Runtime 活动和证据。

## 7. 右侧活动详情

右侧活动是完整回放入口，支持业务模式和技术模式。两种模式共享 activity ID、关系、状态
和详情接口，只改变过滤层级。

### 7.1 业务模式

```text
业务模式
└─ 第 3 轮：调研 WAIC 并生成 HTML 报告                    [运行中]
   ├─ 20:01:02  目标进入执行                              [完成]
   ├─ 20:01:03  选择双团队协作                            [完成]
   ├─ 20:01:04  Team A 深度调研                           [运行中]
   │  ├─ Agent A1 官方资料研究                            [运行中]
   │  ├─ Agent A2 企业案例研究                            [完成]
   │  └─ Agent A3 证据综合                                [等待]
   ├─ 20:01:05  Team B HTML 报告                          [等待]
   └─ --:--:--  最终验收                                  [等待]
```

业务模式显示：

```text
Goal/Execution
public reasoning summary
Team/Agent delegation
actual Skill activation/run
ToolGroup and governed Tool failure
Approval
Artifact/Outcome
business Replan/Recovery
```

### 7.2 技术模式

```text
技术模式
└─ 第 3 轮
   ├─ Admission accepted
   ├─ Session input committed
   ├─ Context packet assembled
   │  ├─ history usage
   │  ├─ memory usage
   │  └─ skill activation
   ├─ Strategy selected: team
   ├─ Model request 1
   ├─ Orchestration proposal validation
   ├─ Graph compiled
   ├─ Resource admission / lease
   ├─ Team child graph started
   ├─ Agent child executions
   ├─ Tool plan / approval / execution receipts
   ├─ Evidence and artifact commits
   ├─ Completion verification
   ├─ Projection commit
   └─ Surface delivery
```

技术模式只显示真实存在且有诊断价值的事件。以下内容不因“完整”而强行制造：

```text
没有明确生命周期的内部函数调用
重复日志行
无消费者的缓存细节
无法解释的原始 JSON
私有思维链
仅通过时间相邻猜出的关系
```

点击活动后使用同一个详情抽屉：

```text
活动详情
├── 基础信息
│   ├── 名称、类型、状态、阶段
│   ├── 开始、结束、耗时
│   └── Team/Agent/Tool/Skill 身份
├── 定义与执行绑定
├── 结构化输入
├── 结构化输出
├── 产物
├── 证据
├── 关系
└── 技术信息
    └── 原始事件（显式展开后才加载）
```

## 8. Mission 与 Task 全景图

Mission 图不能默认嵌入每轮对话并加载全部历史。相关 Session 中显示 Mission breadcrumb 和
“打开全景”入口，用户明确打开后再加载。

```text
┌──────────────────────────────────────────────────────────────┐
│ Mission：完成 WAIC 调研、报告网站和后续发布                  │
│ 状态：Active · Task 2/4 完成 · Session 3 · Team Run 4       │
└──────────────────────────┬───────────────────────────────────┘
                           │
        ┌──────────────────┼───────────────────┐
        │                  │                   │
        v                  v                   v
┌──────────────┐   ┌──────────────┐    ┌──────────────┐
│Task 1 调研   │   │Task 2 建站   │    │Task 3 审查   │
│完成          │   │运行中        │    │等待          │
└──────┬───────┘   └──────┬───────┘    └──────┬───────┘
       │                  │                   │
       v                  v                   v
[Execution A]       [Execution B]        [Execution C]
       │                  │
       ├─ Session 1       ├─ Session 1
       └─ Session 2       └─ Session 3
       │                  │
       v                  v
 [调研证据包]         [HTML 网站]
```

Mission 图默认只展示：

```text
Mission -> Task -> Execution summary -> Team summary -> Artifact/Outcome
Mission -> associated Session
Mission -> Approval/Conflict/Recovery summary
```

Tool 不进入 Mission 默认图。选中 Execution 后下钻到 Execution 业务图。

Task 图位于两者之间：

```text
Task
├── source Session/Turn
├── phases
├── current Execution
├── previous Replan/Recovery Executions
├── acceptance contract
├── artifacts/evidence
└── terminal status
```

## 9. 状态传播

状态传播由 Runtime 完成，不由前端根据子项数量决定。

```text
Tool / Skill lifecycle
          |
          v
Agent Run status + result
          |
          v
Team Run status + shared/terminal result
          |
          v
Execution node / child graph status
          |
          v
Task phase and acceptance status
          |
          v
Mission rollup
```

传播规则：

```text
必需 Tool/Skill 失败且没有恢复
    -> Agent failed/blocked

可选 Tool/Skill 失败
    -> Agent running 或 completed_with_warnings

必需 Agent 失败
    -> Team failed/blocked

可选 Agent 失败且 Team 完成合同满足
    -> Team completed_with_warnings

Team 产生验证结果
    -> 父 Execution node completed

Execution 完成合同满足
    -> Task 当前阶段 completed

Task 子状态
    -> Mission 只做 rollup，不覆盖 Mission 自己的生命周期状态
```

前端可以显示聚合计数，但不得用聚合计数替换 Runtime 状态。

## 10. 当前接口能力

### 10.1 已具备

当前 `ExecutionProjection` 已有：

```text
graph
child_executions
activities
activity_relations
strategy
goals
agents
teams
relations
approvals
admissions
outcomes
interventions
usage
context
evidence
health
recovery
live
available_commands
```

当前增量合同已支持：

```text
UpsertGraphNode
UpsertChildExecution
UpsertActivity
UpsertActivityRelation
UpsertEntity
SetTerminal
AdvanceCursor
```

当前详情接口：

```text
GET /api/runtime/executions/:id/activity?activity_id=...
```

可返回活动、关联关系和相关实体。

### 10.2 缺失或不完整

| 缺口 | 代码事实 | 终态处理 |
|---|---|---|
| Skill 活动类型缺失 | `ExecutionActivityKind` 没有 Skill | 增加 Skill activity；只投影实际激活/执行 |
| Skill 激活未进入执行投影 | 当前主要写 Session journal/context | 以 execution identity 提交有界 Skill activation activity |
| Team 关系不稳定 | 活动投影可能从任意 Team scope event 推断 Team | 从 Team Runtime projection/parent binding 物化 |
| Agent 关系不稳定 | Definition/Instance/Run 容易混用 | Activity 固定使用 Agent Run/Instance，详情引用 Definition |
| Tool 缺定义关联 | activity 只有 tool_call_id | 详情按调用回执关联 Tool catalog contract |
| 点击节点定义不足 | detail 只按 evidence/artifact refs 找 related entities | Team/Agent/Skill/Tool 身份也参与 related entity 查询 |
| 历史与实时存在两套活动来源 | Session `activity_events` 与 canonical activities 合并 | canonical projection 为唯一拓扑；旧事件仅作审计输入 |
| 正文工具可能挂根 | 前端找不到发起者时 fallback root | Runtime writer 必须提供真实 initiator/parent；缺失事件不进入业务投影并产生 health finding |
| Mission 全景已存在但方案描述过时 | `MissionControlRuntime::projection` 已按 selected Mission 过滤 Task/Team/Agent/Session，并已有 `mission_graph` | 只修复经测试证明的成员归属缺口，扩展现有 `MissionControlProjection`，禁止新建第二套 Mission DTO/路由 |
| 业务标签仍可能泛化 | executor kind 被用于名称 | Runtime 提供 display label、role、objective、definition ref |
| Graph node 与 Run 双重活动 | 当前同时生成 `activity:...:node:*` 与 `activity:...:team|agent:*` | 以 graph node activity 为稳定身份，由不可变 binding 合并 Run 生命周期；不再生成第二个叙事 Run 节点 |
| 实际并行与推测并行混用 | `assign_observed_parallel_groups` 按时间重叠改写 `parallel_group_id` | 调度并行组只来自编译器/监督器；时间重叠仅作为观测指标，不能生成拓扑或语义关系 |
| Delta 仍重建全部活动 | 任意相关事件都会调用 `project_execution_activities`，再过滤 changed IDs | 引入按 activity identity 的增量 reducer/index；只有 topology revision 变化才重建拓扑 |
| Activity detail 重建 full snapshot | `activity_detail` 先调用 `snapshot(...Full)` | 建立按 activity identity 的索引化详情读取，点击节点不得重建整个 Execution full projection |
| 右侧业务模式也请求 full | `CompanionPanel.vue` 打开 activity tab 固定 `acquire(..., "full")` | 业务模式只持有 summary；切换技术模式或点击详情才升级到 full/detail |

## 11. 终态合同调整

### 11.1 保留一个事实源

扩展现有 `ExecutionActivityProjection`，不新建第二套 Timeline/Graph DTO。

终态合同：

```text
ExecutionActivityKind += Skill

ExecutionActivityProjection
├── activity_id
├── node_id?                    # graph node identity
├── team_run_id?
├── agent_instance_id?
├── agent_run_id?
├── skill_id?
├── skill_revision?
├── skill_activation_id?
├── tool_contract_id?
├── tool_call_id?
├── definition_refs[]
├── parent/initiator/causal/dependency/parallel
├── status/status_reason/required/timing
├── public_summary/result_summary
└── artifact_refs/evidence_refs/detail_capability

ExecutionActivityDetailProjection.related_entities
├── team_run / team_template
├── agent_run / agent_binding / agent_definition
├── skill_profile / skill_activation
├── tool_invocation / tool_contract
└── artifact / evidence / approval
```

`definition_refs` 只保存稳定引用；定义正文和较大内容点击后懒加载。

现有含义不明确的 `team_id`、`agent_id` 不保留。所有写入者和消费者在同一合同版本中迁移到
`team_run_id`、`agent_instance_id`、`agent_run_id`。项目没有需要维护的稳定外部旧合同，
因此不提供双字段、别名或兼容反序列化。

#### 唯一活动身份

```text
Execution root
  activity:execution:<root_execution_id>

Graph-backed activity
  activity:execution:<root_execution_id>:node:<physical_node_id>

Tool invocation
  activity:execution:<root_execution_id>:tool:<tool_call_id>

Skill activation
  activity:execution:<root_execution_id>:skill:<skill_activation_id>

Artifact / Outcome
  activity:execution:<root_execution_id>:artifact|outcome:<stable_ref_hash>
```

TeamRun/AgentRun 不是第二个叙事节点。它们的生命周期事件必须携带
包含 `execution + node + team_run/agent_run` 的 typed binding，并归并到对应 graph-backed
activity。graph node 在启动前表达 planned/queued；绑定 Run 后，同一 activity 被补齐运行身份和
状态。缺少 node binding 的新 Team/Agent 事件属于写入不变量错误：拒绝提交并记录健康错误，
不能再生成 `activity:...:team:*` 或 `activity:...:agent:*` 让 Surface 去重。

Tool 和 Skill 没有独立 graph node 时使用各自调用/激活身份，但必须有真实
`parent_activity_id`。无法找到父活动的新事件同样拒绝写入；不挂到根、不猜测。

### 11.2 权威父子关系

通用 `RuntimeEventRef.kind: String` 不再承担执行身份和父子关系。当前代码已经同时出现
`node` 与 `execution_node`、`execution` 与 `execution_graph` 等词汇；字符串约定无法作为
终态合同。所有能够进入 activity projection 的事件必须携带 typed binding：

```text
RuntimeActivityBinding
├── root_execution_id
├── activity_id
├── node_id?
├── parent_activity_id?
├── initiator_activity_id?
├── team_run_id?
├── agent_instance_id?
├── agent_run_id?
├── skill_activation_id?
├── tool_call_id?
├── approval_id?
├── parallel_group_id?       # scheduler-owned
├── revision
└── fence
```

`RuntimeEventInput` 和 `DurableRuntimeEvent` 使用该 typed binding。事件库为
`root_execution_id + commit_cursor`、`activity_id + commit_cursor` 建立索引。
`RuntimeEventRef` 继续承载 evidence/artifact/domain reference，但 projector 不再从字符串 ref
反推活动身份。

```text
Team parent:
  owning graph node immutable parent binding

Agent parent:
  ExecutionIdentity.node_id + TeamRun binding

Tool parent:
  execution identity + agent run + model step/tool batch binding

Skill parent:
  SkillActivationIdentity.execution + agent run

Artifact parent:
  producer activity ID
```

禁止：

```text
通过 ID 前缀猜 Team
通过时间相邻猜父子
把任何 Team scope 审计事件当 Team 身份
把所有 Tool fallback 到根
前端拆 Agent ID 猜角色
按时间重叠生成 parallel_group_id
```

`parallel_group_id` 只来自编译器或 Execution supervisor 的调度决定。实际
`started_at_ms/completed_at_ms` 可计算 observed concurrency 指标，但该指标不得改写父子关系、
依赖边或调度并行组。

活动归并顺序固定为 `(commit_cursor, transaction_index, event_id)`。状态只能按 Runtime
生命周期状态机前进；重复事件按 idempotency key 合并，旧 revision/旧 fence 不得覆盖新状态。
`required` 来自 graph completion contract，不能用事件缺省值覆盖，也不能用逻辑 AND 隐式改变。

Skill 激活身份和持久化固定为：

```text
SkillActivationIdentity
├── activation_id
├── execution_identity
│   ├── session_id / turn_id / execution_id
│   └── agent_run_id?          # 主 Agent 使用 Turn primary run identity
├── skill_id / skill_revision
└── adapter_kind

canonical event
├── scope: skill
├── stream: skill-activation:<activation_id>
├── binding: RuntimeActivityBinding
├── refs: skill / skill_revision / profile / evidence
└── idempotency: skill-activation:<execution>:<agent-run>:<skill-revision>
```

Prompt/Document Skill 的 activation 是一次 terminal point fact；有独立执行生命周期的 Skill
使用同一 `activation_id` 追加 started/terminal。Session journal 只保存 canonical event ref，
不再复制候选、查询和 activation payload 形成第二真相。`agent/in_process_worker.rs` 已让 Team
内 Agent 使用 ConversationRuntime；实施时给所有 ConversationRuntime 注入同一
ExecutionIdentity-aware activation writer，主 Agent 和 Team Agent 不得分两套路径。

### 11.3 实时更新

正文、图和活动都通过同一 execution subscription 消费：

```text
initial summary snapshot
        |
        +-- ExecutionLiveUpdate：高频执行状态、Provider 公开输出片段、指标
        |
        +-- ProjectionDelta：Team/Agent/Skill/Tool/Approval/Artifact 活动、关系和实体变化
        |
        +-- explicit resync：游标断裂或授权变化时重新 snapshot
```

`ModelStreamReducer` 已将公开文本和公开 reasoning summary 作为有因果身份的增量发送；
`ExecutionLiveStore` 已按 execution 聚合 `output_parts` 和指标。终态保留这条高频路径：

1. `TextDelta` 和公开 `ReasoningSummaryDelta` 只更新 `ExecutionLiveUpdate`，私有 reasoning
   永不进入公开投影；
2. `ToolStart/Progress/Complete`、`AgentLifecycle`、Team 生命周期、Skill activation 和审批
   生命周期必须同时通过 typed binding 更新 canonical activity reducer；
3. 高频文本不得创建 Activity 节点；Activity 状态变化不得复制一份 Session 业务拓扑；
4. 正文可以按 causal sequence 组合公开思考和输出，但 Team/Agent/Tool 的所有者、状态和关系
   只能来自 canonical activity；
5. terminal snapshot 必须用相同 `activity_id`、revision 和 fence 与实时状态收敛，禁止执行中
   一套结构、执行后换成另一套结构；
6. live subscriber 滞后时按现有 cursor/range 检测触发该订阅者 resync，不让 Gateway 或
   Surface 猜补缺失节点。

不轮询全 Session，不在每个 token 到达时重新布局图。Activity Delta 应按调度和生命周期事件
增量更新；Provider 文本可按帧合并后刷新视图，两者共享 execution identity 和 causal sequence，
但不共享第二套拓扑状态。

## 12. 性能设计

### 12.1 加载边界

```text
进入 Session
├── 先加载最近消息页
├── 加载当前 Turn 的 lightweight execution index/live state
└── 不加载右侧详情、Mission 全景和历史所有图

正文出现执行
├── 订阅当前 execution live update
└── 消费当前 execution activity delta

打开右侧活动
├── 复用内存中的 summary projection
└── 按需加载所选 Turn/Execution

切换技术模式
└── 请求 full detail scope；不预加载所有 raw event

点击节点
└── 请求 activity detail

展开原事件/大证据
└── 再请求对应内容

打开 Mission 全景
└── 才加载 selected Mission projection
```

### 12.2 前端缓存

```text
cache key =
  execution_id
  + revision/cursor
  + detail_scope
  + authorization_revision
  + redaction_revision
```

同一活动详情按 `activity_id + commit_cursor` 缓存。授权或脱敏版本变化时必须失效。

### 12.3 图布局

```text
node/edge set changed -> recompute layout
status/progress changed -> patch node only
output text changed -> no graph layout
right panel closed -> no graph component and no detail subscription
Mission panel closed -> no Mission projection request
```

### 12.4 大规模执行

```text
Tool 默认聚合为 ToolGroup
Agent 完成后折叠子活动
历史 Turn 分页
详情虚拟滚动
大输入/输出只返回摘要和引用
raw/evidence 按需获取
```

## 13. `input_refs` 失败根因

### 13.1 代码事实

当前 `runtime_orchestrate` 手写 JSON Schema 的层级是：

```text
runtime_orchestrate
├── intent
├── operation
├── evidence_refs
├── constraints
└── proposal
    ├── mutation_id
    ├── reason
    ├── nodes[]
    │   ├── node_id
    │   ├── recipe
    │   ├── objective
    │   ├── depends_on
    │   └── input_refs       <-- 仅在这里
    └── completion
```

最近 Session 中模型先把 `input_refs` 放到顶层，随后又错误理解为应该放到 `proposal`
直接子级。两种结构都不合法：

```text
错误 1:
{
  "intent": "...",
  "input_refs": [...]
}

错误 2:
{
  "intent": "...",
  "proposal": {
    "input_refs": [...]
  }
}

当前合法但不推荐继续暴露的结构:
{
  "intent": "...",
  "proposal": {
    "nodes": [
      {
        "node_id": "...",
        "recipe": "agent",
        "objective": "...",
        "input_refs": [...]
      }
    ]
  }
}
```

当前失败有五个根因：

1. 模型可见 Schema 由 `runtime_bootstrap.rs` 手写，与 Rust 类型形成重复合同。
2. `input_refs` 是较深层字段，Tool 描述没有给出最小合法示例。
3. 错误只返回顶层 `allowed_fields`，没有 JSON Pointer 和字段所属路径。
4. `input_refs` 同时承载 context refs 和 `session:<id>` 特殊语义，职责混杂。
5. 模型本应只提议语义依赖，却被要求猜 Runtime 的物理输入引用。

### 13.2 为什么不能放宽校验

不能删除 `deny_unknown_fields` 或忽略未知字段：

```text
忽略 input_refs
    -> 模型认为节点拿到了输入
    -> Runtime 实际丢弃输入
    -> 后续 Team/Agent 在错误上下文执行
    -> 产生比显式失败更危险的假成功
```

严格拒绝是正确的；错误在于合同设计和修复反馈。

### 13.3 终态修复

模型可见合同只保留语义：

```text
ModelGraphSemanticNode
├── node_id
├── recipe
├── objective
├── depends_on
├── focuses
├── template
├── output_artifacts
├── evidence_contract
├── required
├── dependency
├── cancellation_group
└── target_session_id?   # 仅 session_dispatch
```

输入来源：

```text
top-level evidence_refs
        |
        +--> Runtime 分配给允许消费的节点

depends_on
        |
        +--> Runtime 在前驱完成后解析 artifact/result/evidence refs

target_session_id
        |
        +--> Runtime 编译 SessionDispatch
```

不存在过渡字段。现有 `GraphSemanticNode.input_refs` 和
`input_refs` 中的 `session:<id>` 特殊编码在同一版本彻底删除。

Runtime 内部使用不同类型：

```text
ResolvedGraphNode
├── semantic: ModelGraphSemanticNode
├── physical_node_id
├── resolved_input_bindings[]
│   ├── source_kind: request_evidence | predecessor_artifact
│   │                | predecessor_result | predecessor_evidence
│   ├── source_activity_id
│   ├── stable_ref
│   └── authorization_scope
├── target_session_id?       # 仅 SessionDispatch
├── team/agent binding
├── permission/resource lease
└── idempotency/fence
```

解析算法固定为：

1. 先验证 semantic DAG、recipe 字段合法性和 `target_session_id` 适用范围；
2. 将 top-level `evidence_refs` 解析为已授权 evidence identities；
3. 根据 `depends_on` 为每个节点建立 predecessor result/artifact/evidence 输入槽；
4. 在前驱终态提交时解析槽位，不复制大内容，只写稳定 ref；
5. SessionDispatch 只读取显式 `target_session_id`，不从任意字符串识别 Session；
6. 未满足 required evidence/completion contract 时保持 waiting/blocked，不带缺失输入执行；
7. resolved plan 通过一次原子提交进入 ExecutionGraph，模型 JSON 不直接成为可执行图。

### 13.4 单一合同

新增独立的跨 crate 模型输入合同并派生 Schema：

```text
`harness-contract::orchestration::ModelRuntimeOrchestrationInput`
    |
    | serde + schemars, deny_unknown_fields
    v
model-visible JSON Schema
    |
    | Gateway validate; inject authenticated Session/Surface identity
    v
`runtime::orchestration::RuntimeOrchestrationCommand`
    |
    | Runtime injects model lease, selection mode, strategy binding,
    | capability grants, permission/resource lease and idempotency fence
    v
`ResolvedRuntimeOrchestrationPlan`
    |
    v
Runtime compiler + atomic graph mutation
```

`ModelRuntimeOrchestrationInput` 只包含模型允许声明的字段。当前
`RuntimeOrchestrationRequest` 中的 `model_lease`、`selection_mode`、`strategy_binding`、
`capabilities`、`surface`、授权 Session identity 和 permission ceiling 不允许出现在模型
Schema。禁止继续手写一份与 Rust 类型平行演进的 schema；Gateway Tool schema 和 OpenAPI
component 都从该类型生成，并在测试中比较同一 schema hash。

最小合法示例同样由合同 crate 的 typed fixture
`ModelRuntimeOrchestrationInput::minimal_example()` 生成。Tool 描述、typed error 和合同测试
引用同一 fixture，禁止 Gateway、Runtime 和文档分别手写三份示例。schema hash 与 example
fixture 在启动时注册到 capability manifest，模型可通过已有 capability 查询获得，不增加
第二个编排发现接口。

### 13.5 错误修复信息

输入失败必须返回：

```text
class: input_contract
tool: runtime_orchestrate
side_effect_committed: false
json_pointer: /input_refs
reason: unknown top-level field
closest_valid_semantics:
  - use proposal.nodes[].depends_on for predecessor data
  - use top-level evidence_refs for known evidence
  - use target_session_id for session_dispatch
minimal_valid_example: {...}
schema_hash: ...
repair_action: repair_arguments_once
```

Gateway executor 只负责返回 typed input failure，不执行模型循环。Conversation Runtime
拥有参数修复决策：对 `side_effect_committed=false` 的输入合同错误允许一次确定性修复；
Runtime compiler 只验证和编译，不调用模型。第二次仍失败时：

1. 保留原始任务目标和已提交证据；
2. 调用 `runtime_capabilities(detail=orchestration_options)` 获取当前合同；
3. 重新规划，不把工具参数错误误报为业务失败；
4. 只有无法形成合法计划时才停止，并给出准确阻塞项。

## 14. 实施任务包

本方案只允许一个对外版本边界。A-H 是同一版本内的工作流，不是可发布的中间版本。合同、
生产者、投影、Gateway、Surface、旧路径删除和验收必须全部完成后才能提交版本、打 tag 和
宣称完成。

### A. 编排输入合同

修改范围：

```text
crates/harness-contract/src/orchestration/*
crates/gateway/src/runtime/runtime_bootstrap.rs
crates/gateway/src/runtime/gateway_tool_executor.rs
crates/runtime/src/orchestration/*
```

完成：

1. 建立模型专用 orchestration input 类型；
2. 从类型生成 Schema；
3. 从模型合同移除 `input_refs`；
4. 增加 `target_session_id`；
5. Runtime 解析 dependencies/evidence/artifacts；
6. 错误返回 JSON Pointer、语义建议和合法示例；
7. 参数错误一次自动修复，任务目标不丢失。
8. 建立 Runtime-only resolved command/plan/node 类型；
9. 删除 `GraphSemanticNode.input_refs`、`session:<id>` 解析和手写 schema；
10. 保留当前 required evidence、focus、resource scope、multiplicity、completion、
    cancellation 和 control 能力，不得在合同收缩时丢失。

### B. Team/Agent/Skill/Tool 权威活动

修改范围：

```text
crates/harness-contract/src/projection/activity.rs
crates/runtime/src/recovery/runtime_event_store.rs
crates/runtime-postgres/src/lib.rs
crates/runtime/src/projection/activity.rs
crates/runtime/src/team/*
crates/runtime/src/agent/*
crates/runtime/src/conversation/*
crates/runtime/src/tooling/*
```

完成：

1. 增加 Skill activity；
2. 新增 typed `RuntimeActivityBinding` 并迁移所有 graph/Team/Agent/Tool/Skill/Approval writers；
3. 将 graph node activity 与 TeamRun/AgentRun 通过 binding 合并为同一活动；
4. Tool 活动绑定真实 Agent/ToolBatch，缺失父级拒绝写入；
5. Skill 激活使用稳定 activation identity，并绑定 Turn ingress Execution 和 AgentRun；
6. Team/Agent 状态由 Runtime 上卷；
7. 补齐 produces/consumes/delegates/invokes 关系；
8. 调度并行组来自 compiler/supervisor，观测重叠不改写语义；
9. 删除 Team/Agent 双活动身份、root fallback、字符串 ref 和基于时间/ID 的关系猜测；
10. 新事件缺少 required binding 时拒绝提交并暴露 health finding，不产生 unknown 业务节点。

### C. Detail 与定义

完成：

1. Team 节点详情关联 Team Run 和 Template；
2. Agent 节点详情关联 Run、Binding、Definition；
3. Skill 节点详情关联 Profile、版本和 activation；
4. Tool 节点详情关联 invocation、receipt 和 contract；
5. Artifact/Evidence/Approval 使用现有引用；
6. 大内容和原事件懒加载。
7. 详情按 activity identity 和稳定 refs 查询，不调用 full execution snapshot；
8. detail 请求只返回被授权的摘要和引用；原始大内容使用现有 evidence/raw capability 下钻；
9. `activity_id + commit_cursor + authorization_revision + redaction_revision` 为缓存键。
10. `ExecutionProjectionScope::load` 使用 graph-indexed Task/Team/Agent/Approval lookup，不调用
    各聚合的 workspace-wide `list()` 后过滤；
11. `related_event_entities` 使用 root execution binding 查询，删除 `all_events(512)` 截断扫描；
12. strategy 使用 `session + execution + kind` 索引查询，不读取完整 Session stream。

### D. Mission/Task 全景

完成：

1. 复用现有 `MissionControlProjection.mission_graph`，禁止新 DTO、新路由和第二 projector；
2. 对 selected Mission 的 Task/Team/Agent/Session/Approval/Event membership 做源码和场景测试；
3. Execution 的 Mission/Task/Session/Turn scope 在准入时固化；
4. Session 当前 membership 不回填历史执行；
5. Mission 图、Task 图、Execution 图分层下钻；
6. 关联 Session 可进入全景，但默认不预加载；
7. 只有测试证明存在的筛选缺口才修改 `mission_control.rs`，不重做已具备功能；
8. `event_digest_for_mission` 使用 mission binding/cursor 索引，删除 `all_events(limit*20)`；
9. workspace recovery 数量使用 materialized health counter，删除 `all_events(500)` 近似统计。

### E. WebUI 正文树

修改范围：

```text
cowd-edge/surfaces/webui/src/adapters/executionActivity.ts
cowd-edge/surfaces/webui/src/components/chat/ExecutionActivityNode.vue
cowd-edge/surfaces/webui/src/pages/ChatPage.vue
```

完成：

1. 只用 canonical activity 构造拓扑；
2. 纵向多级 Team/Agent/Skill/Tool 树；
3. 公开思考摘要按因果顺序插入；
4. Tool 完成后聚合；
5. Agent 完成并产生产物后自动折叠；
6. 失败、等待审批、阻塞保持展开；
7. 运行中和完成后不切换渲染器。
8. Session `activity_events` 仅用于历史技术审计，不参与正文树；
9. 删除 `nearestConversationOwner`、`fallbackRoot` 和基于 ID 的 owner 推断；
10. live fragment 只补充同一 canonical activity 的公开摘要，不创建临时第二节点；
11. snapshot/delta/live 以 revision/cursor 单调归并，终态 snapshot 只收口同一节点。

### F. Execution 业务图

修改范围：

```text
cowd-edge/surfaces/webui/src/utils/executionLineage.ts
cowd-edge/surfaces/webui/src/components/graph/GraphSurface.vue
cowd-edge/surfaces/webui/src/components/mission/ExecutionGraphCanvas.vue
```

完成：

1. Execution/Goal -> Team -> Agent -> Skill/ToolGroup -> Artifact/Outcome；
2. 跨 Team 产物流；
3. 真实并行时间区间；
4. 业务中文标签；
5. 点击加载结构化详情；
6. 状态更新不重新布局。
7. 删除 frontend-derived owner edge 和 Team/Agent 去重逻辑；
8. graph node/edge set 未变化时只 patch 样式、状态和摘要；
9. 技术 graph node 不进入业务图，但仍保留在技术详情。

### G. 右侧活动

修改范围：

```text
cowd-edge/surfaces/webui/src/components/CompanionPanel.vue
cowd-edge/surfaces/webui/src/components/workbench/TimelineList.vue
```

完成：

1. 按 Turn 和 Execution 分组；
2. 业务/技术模式切换；
3. 业务模式按 Team/Agent lane 展示；
4. 技术模式覆盖完整必要链路；
5. 统一详情抽屉；
6. 右侧关闭时不加载 full projection。
7. 业务模式只申请 summary projection；
8. 切换技术模式才申请 full projection，离开后释放 full consumer；
9. 时间线、Reality、Context 和 raw events 不参与业务拓扑，只在技术模式或详情按需加载。

### H. TUI

TUI 不复制 WebUI 大图，但消费相同语义：

```text
Mission/Task breadcrumb
Execution summary
Team/Agent tree
Tool/Skill counters
status/result/evidence drill-down
```

具体修改：

1. `crates/tui/src/components/runtime_activity_panel.rs` 从 `projection.activities` 和
   `activity_relations` 生成紧凑 Team/Agent 树及 Tool/Skill 计数；
2. `crates/tui/src/app_core/app.rs` 保持现有 projection revision/live revision 单调门禁；
3. `crates/tui/src/app_core/runtime_control_store.rs` 继续消费现有 Mission materialized
   snapshot/delta，不新增 Mission 数据源；
4. 旧的 prose/tool event 计数只保留为无 Execution 的技术诊断，不得覆盖 canonical 计数；
5. 增加 snapshot/delta/live 等价、乱序拒绝、授权裁剪和终态不回退测试。

### 14.1 同一版本内的依赖图

```text
W0 基线冻结与现有修改归类
 |
 v
W1 终态合同
├── orchestration model/runtime split
├── activity identity fields + Skill kind
└── generated schemas/types
 |
 +--------------------+
 |                    |
 v                    v
W2 生命周期生产者      W3 编排解析与错误修复
├── Team/Agent refs    ├── resolved inputs
├── Tool parent refs   ├── target_session_id
└── Skill activation   └── one repair owner
 |                    |
 +----------+---------+
            v
W4 Runtime projection/read path
├── canonical identity reducer
├── summary delta/live
├── indexed activity detail
└── existing Mission Control refinement
            |
      +-----+------+
      |            |
      v            v
W5 WebUI        W6 TUI
      |            |
      +-----+------+
            v
W7 删除旧路径、生成物更新、全链验证和性能门禁
            |
            v
唯一提交 / 版本 / tag / push
```

W2 与 W3 可以并行，W5 与 W6 可以并行；其余依赖不得倒置。任何并行工作都只能修改自己登记的
所有权文件，公共合同由 W1 单独拥有。

### 14.2 必须删除的旧路径

| 旧路径 | 删除条件 | 扫描门禁 |
|---|---|---|
| Gateway 手写 `runtime_orchestrate` schema | 类型派生 schema 接通 | `runtime_bootstrap.rs` 不再出现该手写 properties 树 |
| `GraphSemanticNode.input_refs` | resolved input compiler 通过测试 | Runtime orchestration 源码无字段和 `session:<id>` 解析 |
| Team/Agent event 独立叙事 activity ID | node/run binding reducer 接通 | 不再生成 `:team:<run>`、`:agent:<run>` 业务 activity |
| `assign_observed_parallel_groups` | supervisor 并行组和观测指标分离 | 函数及调用为零 |
| WebUI `mergeActivityViews` 参与业务拓扑 | canonical delta/live 接通 | 正文/业务图调用链无 Session event merge |
| `nearestConversationOwner` / `fallbackRoot` | Runtime parent 完整 | 生产源码引用为零 |
| `derived-owner:*` 关系 | canonical relations 接通 | 业务图不生成 owner 边 |
| Companion 业务模式 full acquire | summary/full 生命周期接通 | business 模式网络记录无 full 请求 |
| Activity detail 的 full snapshot | indexed detail reader 接通 | `activity_detail` 调用链不进入 `snapshot(...Full)` |

不删除 Runtime 原始事件、Session 审计事件、证据、Recovery、Mission Control、技术详情和
授权裁剪能力；它们改变的是消费边界，不是能力本身。

### 14.3 现有未提交修改的吸收规则

当前活动 V2 与 WebUI 修改不是可信基线上的已完成功能，只能逐项吸收：

1. `display_label/phase/status_reason/required/result_summary` 保留并纳入终态合同；
2. execution-scope 有界查询目标保留，但当前 stream prefix + JSON refs containment 实现不作为
   终态；改为 `RuntimeActivityBinding.root_execution_id/activity_id` 显式索引查询；
3. 当前 Team/Agent 双 activity ID、观测重叠改写并行组、前端 derived owner 不保留；
4. 当前 WebUI live transport、GraphSurface 和 ActivityNode 修改按终态消费规则复核；
5. 构建后的 hash assets 只在源代码通过最终验收后统一重建一次，不作为独立修改依据。

## 15. 验收矩阵

| 编号 | 场景 | 必须证明 |
|---|---|---|
| O1 | 顶层误传 input_refs | 返回准确 JSON Pointer 和修复建议，不产生副作用 |
| O2 | 双 Team 提案 | 编译两个 Team 子图，不丢第二阶段 |
| O3 | 前驱产物输入 | Runtime 解析 depends_on 产物，不要求模型猜物理 ref |
| O4 | Session dispatch | 使用 target_session_id，不使用特殊 input_refs 字符串 |
| O5 | Schema 单一来源 | Tool schema、OpenAPI schema 与 Rust model contract hash 一致 |
| O6 | Runtime-owned 字段 | 模型 schema 不出现 lease、binding、grant、permission ceiling、physical ID |
| O7 | 参数修复 | Gateway 只返回 typed error；Conversation Runtime 最多一次修复；compiler 不调用模型 |
| O8 | 能力保持 | multiplicity/focus/resource/evidence/completion/cancellation/control 全部有合同测试 |
| O9 | 示例单一来源 | Tool 描述和 typed error 使用合同 fixture；schema/example 无手写副本 |
| A1 | Team 生命周期 | Team activity 来自 canonical Team Run，不来自 working-state 点事件 |
| A2 | Agent 生命周期 | Definition/Instance/Run 身份不混用 |
| A3 | Tool 归属 | 每个 Tool 位于真实发起 Agent 或主线下 |
| A4 | Skill 激活 | 实际激活可见；仅 bound 的 Skill 不伪造成执行 |
| A5 | 状态上卷 | Tool/Agent/Team 状态符合 required/completion contract |
| A6 | 唯一活动身份 | 一个 physical graph node 最多一个叙事 activity；Run 事件只增强该 activity |
| A7 | 写入完整性 | Team/Agent/Tool/Skill 缺 execution/parent identity 时原子拒绝且有 health finding |
| A8 | 并行语义 | 调度并行组来自 supervisor；实际重叠只作为指标，不改写关系 |
| A9 | 重放等价 | fresh snapshot、delta reduce、durable replay 得到字节等价的 activities/relations |
| A10 | 乱序与重复 | 旧 revision/fence 不回退状态；重复 idempotency key 不产生重复节点 |
| P1 | 正文运行中 | 无刷新实时出现思考、Team、Agent、Skill、Tool |
| P2 | 正文完成 | Tool 收口，Agent/Team 折叠，结果和产物保留 |
| P3 | Execution 图 | 清晰展示协作、依赖、并行、产出 |
| P4 | 业务模式 | 不出现 context/provider/cache/projection 噪声 |
| P5 | 技术模式 | 可回放 admission 到 Surface delivery 的必要链路 |
| P6 | 节点详情 | Team/Agent/Skill/Tool 定义、输入、输出、产物可查 |
| P7 | 实时因果顺序 | 公开思考、Tool 启停和输出按 causal sequence 到达，无刷新、无重复 |
| P8 | 实时终态收敛 | 执行中和完成后使用相同 activity identity/父子关系，不切换渲染模型 |
| M1 | Mission 全景 | 仅 selected Mission 数据，无其他 Mission 污染 |
| M2 | 跨 Session | 关联 Session 可进入同一 Mission，历史执行归属不漂移 |
| M3 | 投影复用 | Mission 页面只消费现有 MissionControl materialized snapshot/delta |
| R1 | 历史 Turn | 每轮绑定自己的 Execution，不显示成一条假直线 |
| R2 | Replan/Recovery | 新旧执行关系明确，状态和证据不丢失 |
| F1 | 初次加载 | 消息先显示，右侧和全景不阻塞 Session 打开 |
| F2 | 高频状态 | 不重排图，不扫描全 Session 历史 |
| F3 | 详情懒加载 | 未点击节点不请求 raw/evidence/definition 大内容 |
| F4 | 业务右栏 | business 模式只请求 summary，不请求 full/timeline/reality/context |
| F5 | 详情查询 | 点击单节点不构建 full Execution snapshot |
| F6 | 增量成本 | 单活动状态事件不重投影全部活动、不 ReplaceActivities |
| F7 | 慢消费者 | 游标滞后触发有界 resync，不无限缓存、不阻塞 Runtime writer |
| F8 | 关闭页面 | 右栏、Mission、详情关闭后 consumer/stream 被释放，无后台请求 |
| F9 | Execution scope | scope 构建不调用 workspace-wide Agent/Team/Task/Approval `list()` |
| F10 | 事件实体 | snapshot/delta/Mission digest 不使用 `all_events(N)` 后过滤 |
| F11 | 双实时通道 | token 增量不重建 Activity；Activity Delta 不复制输出文本或 Session 拓扑 |
| S1 | 权限 | 业务摘要不泄露私有思维链、secret 或未授权原文 |
| S2 | 授权变更 | authorization/redaction revision 变化立即失效缓存并 fail closed |
| C1 | 旧路清理 | 第 14.2 节每一项扫描为零，无 alias、兼容字段和双写 |
| C2 | Surface 一致 | WebUI/TUI 使用同一 activity status/kind/identity，不各自解释协议 |
| C3 | 生成物 | OpenAPI、TS types、WebUI assets 只从最终合同统一重建一次 |

## 16. 对抗性审查

### 16.1 是否又增加了一套执行图

否。`ExecutionGraph` 仍是调度真相，`ExecutionActivityProjection` 是统一读模型。正文、业务图、
活动详情只是同一读模型的不同过滤和布局。

### 16.2 Skill 是否被过度提升

没有。Skill 不是新的执行者：

```text
bound Skill      -> Agent 详情
activated Skill  -> Agent 子活动
runtime Skill    -> 有生命周期的 Agent 子活动
Tool             -> 最终副作用执行
```

### 16.3 Mission 图是否会过重

默认不会加载。Session 只显示 Mission 关联入口；打开全景后才请求 Mission projection。
Mission 默认图不展开 Tool。

### 16.4 技术模式是否会成为日志查看器

不能。技术模式只展示具有状态、因果、诊断或恢复价值的稳定事件。普通日志和内部函数调用
不进入活动合同。

### 16.5 是否应保留前端 fallback 猜测

不应。项目尚未形成稳定外部兼容承诺。新执行必须具备 canonical relationship；历史缺失
引用的事件不进入业务拓扑，只在技术审计中显示 health finding。现有派生投影统一清空并从
终态 writer 之后的 canonical events 重建，不提供旧关系恢复兼容层。

### 16.6 `input_refs` 是否只需补文档

不够。当前问题来自手写双合同、层级过深、错误反馈丢失上下文以及模型承担物理引用解析。
仅补一行描述仍会重复失败。必须收缩模型合同并由 Runtime 解析引用。

### 16.7 状态能否由前端上卷

不能。前端没有 required、quorum、recovery、approval 和 completion contract 的完整权威。
前端上卷会出现假失败和假成功。

### 16.8 方案是否会降低能力

不会删除 Runtime 技术事件、证据、原始输入输出或 Mission 控制能力，只改变默认投影和按需
加载方式。业务可读性提高，技术诊断能力保留在技术模式和详情中。

## 17. 最终裁决点

本方案已经给出明确建议，不保留二选一：

1. 三种 Execution 展示保留，但只消费同一 canonical activity。
2. Mission/Task 使用独立宏观图，下钻 Execution。
3. Skill 仅实际激活/执行时进入业务过程。
4. Runtime 权威生成 Team/Agent/Tool/Skill 关系和状态。
5. 前端停止猜关系和维护第二套拓扑。
6. `input_refs` 从模型可见合同移除，输入由依赖、证据和 Runtime 解析。
7. 右侧业务/技术模式共享数据，只改变过滤和加载深度。
8. 所有大详情按需加载，图只在拓扑变化时重新布局。

裁决通过后，实施必须按第 14 节任务包整体推进，并以第 15 节全部门禁作为完成标准。

## 18. 代码事实索引

本节用于防止实施时重新猜测所有权或建立平行合同。行号以本方案审查时的工作区为准，后续
代码移动时应按符号名重新定位。

### 18.1 Core 合同与 Runtime

| 事实 | 当前代码 |
|---|---|
| Activity kind、关系、scope 和活动字段 | `crates/harness-contract/src/projection/activity.rs` |
| Execution live、Session execution index、Execution projection 和活动详情 | `crates/harness-contract/src/projection/snapshot.rs` |
| Activity/Relation 增量操作 | `crates/harness-contract/src/projection/delta.rs` |
| Activity 从 Runtime graph/event 物化 | `crates/runtime/src/projection/activity.rs` |
| Orchestration 请求、`GraphSemanticNode.input_refs` 和严格反序列化 | `crates/runtime/src/orchestration/request.rs` |
| Orchestration 验证、图变更和完成合同 | `crates/runtime/src/orchestration/validator.rs` |
| Team 权威绑定和编译 | `crates/runtime/src/orchestration/team_authority.rs` |
| Skill 激活、选择和 Session journal 持久化 | `crates/runtime/src/conversation/conversation.rs::activate_skills_for_turn` |
| Agent Definition/Revision/Binding 中的 Skill/Tool references | `crates/harness-contract/src/agent/definition.rs` |
| Team 到 Agent 实例化 | `crates/runtime/src/team/instantiation.rs` |

审查时的关键合同状态：

```text
ExecutionActivityKind
├── Execution / Goal / Team / Agent / Model
├── ToolBatch / Tool / Approval / Verify
├── Artifact / Outcome / Replan / Recovery / Runtime
└── 缺少 Skill

ExecutionActivityProjection
├── parent / initiator / causal / dependency / parallel_group
├── team_id / agent_id / tool_call_id / approval_id
├── status / reason / required / timing
├── public_summary / result_summary
└── artifact_refs / evidence_refs / detail_capability
```

因此第 11 节只需增强现有 activity contract，不应创建第二个业务活动 DTO。

### 18.2 Gateway

| 事实 | 当前代码 |
|---|---|
| `runtime_orchestrate` 模型可见 Tool schema | `crates/gateway/src/runtime/runtime_bootstrap.rs` |
| Tool 输入反序列化、Runtime 调用、typed failure | `crates/gateway/src/runtime/gateway_tool_executor.rs` |
| 顶层 `input_refs` 应被拒绝的现有测试 | `crates/gateway/src/runtime/gateway_tool_executor.rs::runtime_orchestrate_reports_repairable_typed_input_contract_errors` |
| Activity detail 路由 | `crates/gateway/src/api_routes/runtime_routes.rs` |

当前失败链：

```text
手写 Tool schema
      |
      v
模型猜测深层 input_refs
      |
      v
RuntimeOrchestrationRequest 严格反序列化
      |
      v
typed failure 只返回当前层 allowed_fields
      |
      v
模型误判新位置并再次失败
```

严格反序列化不是缺陷；重复合同和低质量修复信息才是根因。

### 18.3 WebUI

| 事实 | 当前代码 |
|---|---|
| canonical activity、Session event 适配和合并 | `cowd-edge/surfaces/webui/src/adapters/executionActivity.ts` |
| 正文树重组、Tool owner fallback、自动折叠 | 同上 `conversationActivityTree` / `activityAutoCollapsed` |
| Execution 图活动筛选和派生边 | `cowd-edge/surfaces/webui/src/utils/executionLineage.ts` |
| 右侧业务/技术活动和执行轮次 | `cowd-edge/surfaces/webui/src/components/CompanionPanel.vue` |
| 正文执行树 | `cowd-edge/surfaces/webui/src/components/chat/ExecutionActivityTree.vue` |
| 单个活动节点 | `cowd-edge/surfaces/webui/src/components/chat/ExecutionActivityNode.vue` |
| 图形表面 | `cowd-edge/surfaces/webui/src/components/graph/GraphSurface.vue` |

当前前端的结构性风险：

```text
canonical activities ------+
                            +--> merge --> frontend-derived topology
Session activity_events ---+
                                      |
                                      +--> parent fallback
                                      +--> inferred Tool owner
                                      +--> derived graph edges
                                      +--> frontend-local status display
```

这能临时补足缺失数据，却无法成为终态，因为前端不知道 required、completion、recovery、
approval 和真实 Agent/Team binding。终态必须是：

```text
Runtime canonical activity topology
             |
             +--> chat tree filter/layout
             +--> business graph filter/layout
             +--> activity business/technical filter
```

Session `activity_events` 只保留为技术审计记录，不参与任何业务拓扑、父子关系、并行关系或
状态裁决；历史和新执行遵守同一规则，不保留过渡分支。

### 18.4 运行证据

本次审查使用的最近失败证据位于：

```text
/home/yi/.cowd/logs/gateway.log
```

对应日志明确记录了两次不同的错误理解：

```text
第一次：把 input_refs 放在 runtime_orchestrate 顶层
第二次：认为 input_refs 应直接放在 proposal 下
```

当前代码测试也明确断言顶层 `input_refs` 必须被拒绝。这证明问题不是“偶发模型笨”，而是
模型合同、Runtime 内部合同和错误修复协议之间缺少稳定边界。

## 19. 审查结论

### 19.1 正确性

第一版审查结论“直接通过”不成立。第二轮源码审计发现了四个会导致终态错位的 P0 问题：

1. graph node 与 TeamRun/AgentRun 会产生双 activity identity；
2. Skill 只有 Session activation record，没有稳定 execution/agent identity；
3. Mission Control 已有宏观图，原方案存在重复建设风险；
4. 审计前版本曾计划“内部暂时保留” `input_refs`，不符合无过渡终态。

本方案已在第 10-14 节修正上述问题。修正后所有权通过：

```text
Session         -> 对话与历史
Mission         -> 长期目标
Task            -> 可验收工作
ExecutionGraph  -> 调度真相
Activity        -> 统一读取投影
Surface         -> 过滤、布局、交互
```

### 19.2 完整性

修订后通过，但完整性不是靠章节数量判断，而是由第 20-25 节的 producer/consumer、状态、
并发、失败恢复、资源性能和删除门禁共同证明。覆盖：

1. Runtime 权威 Team/Agent/Skill/Tool 活动；
2. 正文实时纵向执行树；
3. Execution 业务图；
4. 右侧业务/技术活动；
5. Mission/Task 跨 Session 全景；
6. 节点定义、输入、输出、产物和证据；
7. `input_refs` 合同终态修复；
8. 首屏、实时更新、布局和详情加载性能；
9. 历史 Turn、重规划、恢复、审批和权限边界；
10. WebUI 与 TUI 的同语义消费。

### 19.3 可实施性

修订后通过。唯一实施顺序为：

```text
模型编排合同
    -> Runtime canonical activity/relations/status
    -> Detail/Mission/Task projection
    -> WebUI 三种视图
    -> TUI 紧凑视图
    -> 全场景验收
```

若跳过 Runtime 权威投影而先修改前端，会再次产生 fallback、重复拓扑和状态错判。
W0 登记并归类当前两个仓库的 dirty changes 是实施第一步，不是可跳过的外部前置条件。

### 19.4 性能

方向通过，当前实现不通过。现有 `activity_detail -> full snapshot`、业务右栏固定 full、
delta 任意事件重投影全部 activity、前端多来源 merge 均会产生额外延迟。第 23 节把这些路径
列为必须消除的性能门禁；没有基线、调用计数、payload 和 p95 证据不得宣称性能完成。

### 19.5 能力损失

方案目标为无能力损失，但必须由第 24 节 preservation matrix 证明。业务模式只是过滤默认
展示，不删除技术事件、原始事件、输入输出、证据、审批、恢复或 Mission 控制数据。技术模式
和节点详情仍可完整下钻；任何测试证明能力缺失时，不能用“简化 UI”解释或放行。

### 19.6 最终裁决

架构终态与实施方案均已明确，可以进入实施；当前代码尚未达到终态。只有第 14 节同一版本全部
完成、第 15 节门禁全部通过、第 25 节证据包齐备，才能判定实施完成。

## 20. 第二轮源码事实与基线审计

### 20.1 可复现基线

| 仓库 | HEAD | 审查时 tracked diff SHA-256 | 状态 |
|---|---|---|---|
| `cowd` | `fbff3ba6e0d12542d343fde18caea80c2919b952` | `ce3dbed2522fcacb4265eebfbaf7a584bba6ad37b30b0652295f4ae718d5e68c` | 5 个 tracked 文件修改；本方案文档为 untracked |
| `cowd-edge` | `bdb6f4ea4b84c3cbe1aab21b2a7c6e94032c5e75` | `e7862b7471d2a866b3130a9f88a3f7251ec9f36dea55318898640e68d622de2d` | WebUI source 修改和 35 个新 hash assets |

实施开始前必须生成新的机器可读 manifest，记录：

```text
repo/head/branch
tracked file + blob hash
untracked file + content hash
generated asset classification
plan-owned / existing-partial / unrelated classification
```

不得用 `git reset`、`checkout` 或重新构建覆盖未归类修改。现有活动 V2 与 WebUI 修改按
第 14.3 节吸收，不能默认为“已完成”。

### 20.2 精确事实表

| 领域 | 当前符号/路径 | 当前行为 | 终态决定 |
|---|---|---|---|
| Turn ingress Execution | `runtime::session_execution::session_ingress_graph_id`；`conversation/host.rs` | 每个 Turn 在 Provider 前已有 ingress graph identity | 作为全部活动的 root lineage，保留 |
| Activity contract | `harness-contract/src/projection/activity.rs` | V2 dirty change 已有 label/phase/reason/required/result，缺 Skill 和精确身份字段 | 扩展原合同，不新建 DTO |
| Activity snapshot | `runtime/src/projection/activity.rs::project_execution_activities` | graph nodes、Runtime events、artifact 同时物化 | 改为 identity-indexed reducer |
| Team/Agent activity ID | `event_activity_id` | Team/Agent 按 run ID 生成，graph node 又有 node activity | Run 事件归并 node activity，删除双 ID |
| Parent resolution | `event_parent_activity_id` | 从 refs/payload 推断，缺失可落根 | Writer 提交完整 binding；projector 只消费，不猜 |
| Identity ref vocabulary | `agent/runtime.rs::snapshot_identity_refs` 写 `node`；activity projector 只读 `execution_node`；Team/Execution writers 使用 `execution_node` | 同一身份存在多套字符串名称，导致 Agent/Tool 找不到 node parent | 新增 typed `RuntimeActivityBinding`；通用 refs 不再承担身份 |
| Parallel group | `assign_observed_parallel_groups` | 按时间重叠写入语义字段 | 删除；观测并行只做指标 |
| Activity merge | `merge_activity` | 最新事件覆盖 status；`required` 用 AND | 按 revision/fence 状态机归并；required 只读 completion contract |
| Event query | `execution_events_for_scope` dirty change | 比 Session 全历史扫描更窄，但 PostgreSQL 依赖 `LIKE` 和 JSONB refs containment | 改为 typed binding 的 root execution/activity 索引查询 |
| Delta | `projection/delta.rs::materialize_delta_operations` | 任意事件重建所有 activities 后筛 changed | 按 event identity 增量 reduce |
| Detail | `projection/mod.rs::activity_detail` | 构建 full snapshot 再筛一个节点 | 直接按 activity/ref index 读取 |
| Scope build | `projection/reducer_support.rs::ExecutionProjectionScope::load` | workspace-wide Agent/Team/Task/Approval `list()` 后过滤 | 各 owner 提供 graph/execution scoped lookup |
| Related entities | `projection/snapshot.rs::related_event_entities` | `all_events(512)` 后过滤，事件多时会漏掉关联实体 | root execution binding 索引读取，不设全局近似窗口 |
| Strategy | `projection/snapshot.rs::strategy_entity` | 读取完整 Session stream 后筛 execution | session/execution/kind 索引读取 |
| Skill activation | `ConversationRuntime::activate_skills_for_turn` | 主/嵌套 ConversationRuntime 选择 Skill；只写 Session event，记录 turn_index | 使用 Turn/Execution/Agent identity 写一条 Skill canonical event |
| Agent skill wiring | `agent/in_process_worker.rs` | Agent binding 的 skill refs 被传入 ConversationRuntime | 保留；统一走 canonical activation writer |
| Mission macro graph | `mission/mission_control.rs::mission_graph` | 已生成 Mission/Task/Execution/Team/Agent/Artifact 等图 | 原地增强，不另建 |
| Mission filtering | `MissionControlRuntime::projection` | selected Mission 已过滤 Task/Team/Agent/Session | 用场景测试确认，不按旧假设重写 |
| Mission digest | `event_digest_for_mission` / `workspace_recovery_required_count` | 扫最近全局 1000/500 事件再过滤，既可能漏数据又随全局负载波动 | mission binding 索引 + materialized health counter |
| Orchestration model contract | `runtime/orchestration/request.rs` | Runtime internal 和 model JSON 共用类型 | 拆成 harness-contract model input 与 Runtime resolved command |
| Model schema | `gateway/runtime/runtime_bootstrap.rs` | 手写 JSON schema | 从 model input 类型生成 |
| Session dispatch | `compiler.rs::compile_session_dispatch_node` | 从 `input_refs` 搜索 `session:<id>` | 显式 `target_session_id` |
| Typed error | `gateway_tool_executor.rs` | 严格拒绝，但修复上下文不足 | JSON Pointer + semantic repair；模型循环只在 Conversation Runtime |
| WebUI topology | `executionActivity.ts` | canonical + Session events merge；owner fallback | canonical-only |
| WebUI graph | `executionLineage.ts` | Team/Agent 去重、owner edge 和 tool group 在前端派生 | 只做过滤、ToolGroup 展示聚合和布局 |
| Right panel | `CompanionPanel.vue` | activity tab 固定申请 full | business summary；technical/full 按需 |
| Session first paint | `stores/app.ts::loadMessages` | 消息先加载，右栏关闭时大部分延后 | 保留并加网络门禁 |
| TUI projection gate | `app_core/app.rs::apply_execution_projection` | graph/live 双 revision 单调合并 | 保留；增加 activity 语义消费 |
| TUI Mission | `runtime_control_store.rs` | 已消费 Mission materialized snapshot/delta | 保留，不新增数据源 |

## 21. 全链路审计

### 21.1 正常链

| 阶段 | 权威输入 | 写入者 | 权威状态/输出 | 消费者 | 终态门禁 |
|---|---|---|---|---|---|
| Admission | authenticated Session input | Gateway Session service | Turn + ingress Execution identity | Conversation Host | identity 在上下文准备前存在 |
| Context/Skill | Turn identity + grants | Conversation Runtime | Context envelope + Skill activation event | Provider packer、Activity reducer | Skill 有 execution/agent binding |
| Semantic planning | prompt + capability schema | Provider/model | model orchestration input | Gateway typed tool executor | 只含 model-owned fields |
| Authority binding | model input + Runtime grants | Runtime orchestration | resolved command/plan | compiler | physical refs/leases 不来自模型 |
| Compile | resolved plan | Runtime compiler | immutable graph mutation | supervisor | 原子提交；无半图 |
| Execute | graph nodes | supervisor/Team/Agent/Tool runtimes | lifecycle events/results | activity reducer/recovery | writer refs 完整，幂等 |
| Project | graph + canonical lifecycle | Runtime projection | summary/delta/live/detail | Gateway | snapshot/delta/replay 等价 |
| Deliver | authorized projection | Gateway live routes | cursor/revision stream | WebUI/TUI | 慢消费者有界 resync |
| Render | canonical projection | Surface | tree/graph/activity/detail | user | 不推断业务真相 |

### 21.2 状态真相表

| 状态 | 唯一写入者 | 持久化 | 高频运行态 | 允许派生 | 禁止 |
|---|---|---|---|---|---|
| Turn/Session | Gateway Session service | Session store/journal | Session execution index | UI attention | Runtime/前端另建 Session 状态 |
| Graph node | Execution supervisor | graph state/event | execution live | completion rollup | event标题猜状态 |
| TeamRun | Team Runtime | Team event/snapshot | Team run state | node activity enrichment | arbitrary Team event 创建 Team |
| AgentRun | Agent Runtime | Agent event/snapshot | Agent run state | node activity enrichment | Definition ID 当 Run ID |
| Tool call | Tool host/executor | Tool receipt/event | open call state | ToolGroup count | Surface 自判成功 |
| Skill activation | Skill activation owner | single Skill event | active skill refs | Session reference/activity | Session + Runtime 双写真相 |
| Activity | Runtime reducer | 可重放 materialization | summary/live cache | layouts/counters | Surface 改父子/required/status |
| Mission | Mission Runtime | Mission aggregate | Mission materialized cache | Mission rollup | Execution status覆盖 Mission 生命周期 |

### 21.3 Producer/consumer 接线表

| 合同变化 | 必须更新的 producer | 必须更新的 consumer | 生成物/测试 |
|---|---|---|---|
| Activity 精确身份字段 | graph reducer、Team/Agent/Tool/Skill writers | Runtime detail/delta、Gateway schemas、WebUI、TUI | OpenAPI、TS type、snapshot/delta fixtures |
| RuntimeActivityBinding | 所有会进入 activity 的 Runtime event writer | event store、projection reducer、recovery | writer completeness、DB migration、index/explain tests |
| Skill kind | Skill activation writer | activity reducer、WebUI/TUI labels/filters | schema、i18n、replay tests |
| Model orchestration input | capability contract schema producer | model tool registry、typed executor | schema hash/golden |
| Resolved input binding | authority resolver/compiler | supervisor/node executors/recovery | compile/replay/idempotency tests |
| Mission scope refinement | Mission materialized projector | Mission WebUI/TUI | selected Mission isolation tests |
| Summary/full lifecycle | projection registry/Gateway | Companion business/technical/detail | request-count browser tests |

任何一行 producer 或 consumer 未迁移，版本门禁失败。不得以反序列化默认值、前端 fallback
或旧字段 alias 通过编译。

## 22. 并发、失败与恢复审计

### 22.1 并发与等待

| 场景 | 并发所有者 | 唤醒条件 | 取消/超时 | 慢消费者处理 |
|---|---|---|---|---|
| 独立 graph nodes | Execution supervisor | dependencies/completion satisfied | graph policy/fence | 不受 Surface 影响 |
| Team Agents | Team Runtime + resource manager | role/focus ready | Team completion policy | projection 只观察 |
| Tool batch | Tool host + governed plan | tool permission/resource lease | per-call cancellation | ToolGroup 增量聚合 |
| Provider/model lanes | Runtime capacity manager | provider lease | adaptive timeout/recovery | token stream 有界 |
| Projection subscribers | Gateway projection registry | commit cursor/live revision | disconnect/resync | 有界 buffer + cursor resync |
| Graph layout | Surface | topology hash change | component disposal | 状态更新不重排 |

实现必须证明：

1. Runtime writer 不等待 WebUI/TUI 渲染或数据库详情查询；
2. delta/live channel 有容量、滞后检测、resync 和 consumer release；
3. Team/Agent/Tool 并行关系来自调度记录，UI 不用时间相邻假造；
4. 一个慢 Surface 不阻塞其他 Surface；
5. 取消、Replan、Recovery 后旧 fence 事件不能把终态回退到 running。

### 22.2 失败与恢复

| 失败 | side effect | 所有者 | 恢复 | 必须保留 |
|---|---:|---|---|---|
| Model JSON 合同错误 | 否 | Conversation Runtime | 一次确定性参数修复 | 原始 objective、typed error |
| Resolved input 缺失 | 否 | Runtime authority/compiler | waiting/blocked 或 replan | 缺失 ref 和 predecessor |
| Team/Agent identity 不完整 | 否 | lifecycle writer | 拒绝事件/启动 | health finding |
| Tool 执行失败 | 可能 | Tool host | 按幂等/副作用策略 retry/replan | receipt/evidence |
| Projection cursor gap | 否 | Gateway projection service | summary/full resync | authorization/redaction revision |
| Surface 断连 | 否 | Surface registry | cursor reconnect/resync | canonical durable state |
| Recovery 重放 | 取决于 receipt | Recovery owner | fence/idempotency 判定 | committed evidence |

禁止把 model input failure 交给 compiler 调模型，禁止 Gateway executor 自己循环，禁止投影错误
改变业务执行结果。

## 23. 性能和资源终态

### 23.1 当前已确认的热点

| 热点 | 当前证据 | 影响 | 终态 |
|---|---|---|---|
| 单节点详情构建 full snapshot | `activity_detail` 第 84-86 行 | O(execution) 读取与组装 | identity/ref 索引直读 |
| delta 重投影全部 activity | `materialize_delta_operations` 第 220-249 行 | 高频事件 O(history) | event-local reducer |
| 业务右栏申请 full | `CompanionPanel.vue` 第 721-730 行 | 打开即加载技术数据 | summary/full consumer 分离 |
| 前端三来源归并 | canonical + Session events + timeline | CPU、重复、错位 | topology canonical-only |
| scope 聚合全量 list | `ExecutionProjectionScope::load` | 并发 Execution 形成 O(all workspace entities) | owner-provided graph/execution index |
| related entity 全局窗口 | `related_event_entities::all_events(512)` | 高事件量下静默漏数据 | root execution binding query |
| strategy 全 Session stream | `strategy_entity::list_stream(session)` | 长 Session 线性增长 | execution/kind index |
| Mission event digest | `all_events(limit*20)` / `all_events(500)` | 大负载下既漏事件又延迟 | mission index + materialized counter |
| scope event refs 查询 | PostgreSQL JSONB containment/LIKE | 无专用索引时退化，且字符串 ref 易错 | typed root execution/activity columns + explain/index 门禁 |

### 23.2 数据结构和缓存

各 owner 必须提供下列有界读取，不允许 projection 层再调用全量 `list()`：

```text
TaskAggregateService::list_for_graphs(graph_ids)
AgentRuntime::list_for_graphs(graph_ids)
TeamRuntime::list_for_graphs(graph_ids)
ApprovalQueue::list_for_execution_scope(scope)

RuntimeEventStore::events_for_root_execution_after(execution_id, cursor, limit)
RuntimeEventStore::events_for_activity(activity_id, cursor, limit)
RuntimeEventStore::latest_for_session_execution_kind(session_id, execution_id, kind)
RuntimeEventStore::events_for_mission_after(mission_id, cursor, limit)
```

内存 owner 用 `BTreeMap/HashMap` 二级索引；SQLite/PostgreSQL 使用相同语义的复合索引。Projection
只能通过这些 scoped ports 读取，不允许知道数据库查询细节。

`RuntimeServices::execution_projection_cache` 是唯一 execution projection materialization
cache 所有者；Gateway 只通过 Runtime projection port 读取和订阅，不再维护第二份服务端缓存：

```text
key: execution_id + detail_scope + auth_revision + redaction_revision
value:
  projection header
  activity_by_id
  relation_by_id
  graph_node_to_activity
  team_run_to_activity
  agent_run_to_activity
  tool_call_to_activity
  skill_activation_to_activity
  cursor/revision/topology_hash
```

缓存由 commit/delta 驱动更新；数据库是持久化和恢复来源，不是每次 Surface 点击的运行查询路径。
缓存缺失时从有界 scope snapshot 恢复，恢复后继续消费 cursor。授权版本变化时整项失效，不能
复用旧的未裁剪对象。

资源策略固定为：

1. active Execution summary/index 常驻，直到 terminal；
2. terminal Execution 使用按估算字节数计费的 LRU；
3. full/detail/raw 不进入常驻缓存，只缓存有界摘要和稳定 refs；
4. 缓存预算使用现有 Runtime 内存预算配置，不新增独立、互相竞争的内存配置；
5. 达到预算时先逐出 terminal LRU；active state 不丢弃，必要时降级丢弃可重建的 detail；
6. subscriber queue 有界，滞后只触发该 consumer resync，不扩大全局缓存；
7. 所有逐出均不影响 durable state，可从 scoped ports 重建。

### 23.3 性能验收方法

先在修订前基线采样，再执行绝对和相对双门禁。测试数据至少包含：

```text
1 Execution / 1 Agent / 10 Tool
1 Execution / 20 Agent / 200 Tool
1 Mission / 50 Task / 100 Execution
100 historical Turns + 1 live Turn
2 simultaneous Surfaces + 1 deliberately slow subscriber
```

| 指标 | 终态门禁 |
|---|---|
| Session 最近消息首屏 | 不等待 right panel/Mission/full projection；浏览器 waterfall 证明 |
| 单活动状态 delta | 不调用全量 activity projector；操作数与受影响活动数同阶 |
| Activity detail | 不调用 full snapshot；数据库/缓存查询数有固定上限 |
| Business panel | 无 full、raw event、reality/context 请求 |
| Topology-stable update | Graph layout 调用计数不增加 |
| Slow subscriber | Runtime writer 延迟不随慢客户端线性增长；客户端收到 resync |
| PostgreSQL scope query | `EXPLAIN` 使用 stream/ref 索引，无全表扫描 |
| Payload | summary 不包含 raw input/output/definition 大正文 |
| Regression | p95 不高于修订前基线；若基线已超过交互上限，必须同时下降而非仅“不变” |

具体毫秒阈值必须由同机、同数据、同构建模式的基线产生后写入证据，不允许在方案中随意捏造，
也不允许以没有阈值为由跳过比较。

## 24. 能力保持矩阵

| 现有能力 | 终态保留方式 | 防退化证据 |
|---|---|---|
| Direct/Agent/Team/Review/Synthesis/SessionDispatch | model contract 保留 recipe；Runtime resolved command 执行 | 每种 recipe 场景测试 |
| multiplicity 与多 Agent 并行 | 语义节点扩展 physical nodes | 双 Team、多实例图测试 |
| focus/resource/evidence contracts | 原字段保留，resolved plan 加授权 binding | schema + compile assertions |
| any/quorum/all/cancellation | completion/dependency contract 不变 | 并发完成和取消测试 |
| Replan/Recovery | canonical relation/status/fence | 重放等价和旧 fence 拒绝 |
| Session 补充输入 | ingress Execution/Turn lineage 不变 | live supplement 场景 |
| Tool 权限和审批 | Tool host/Approval owner 不变 | pending/approve/deny/timeout |
| Skill 渐进披露和工具引用 | activation writer 加 identity，不改变 selector/catalog | 主 Agent 与 Team Agent 测试 |
| Memory/Fact/Matrix | 作为授权 context/evidence refs，不进入 Surface 推断 | context/evidence 下钻 |
| Mission Control | 复用现有 materialized projection/delta | selected Mission 和跨 Session |
| 原始事件与证据 | technical/detail 按需读取 | 权限、脱敏、lazy load |
| WebUI/TUI live | 同一 summary/delta/live contract | 双 Surface 同步与断线恢复 |

## 25. 完成证据包和硬门禁

### 25.1 代码证据

```text
evidence/
├── baseline-manifest.json
├── final-manifest.json
├── source-fact-map.md
├── producer-consumer-matrix.md
├── removed-path-scans.txt
├── generated-schema-hashes.json
├── capability-preservation.md
└── diff-scope-audit.md
```

`removed-path-scans.txt` 至少包含第 14.2 节每个旧符号/字段/derived relation 的 `rg` 结果。
扫描结果非零时必须逐条解释为测试夹具、文档反例或真实残留；生产源码残留不允许放行。

### 25.2 测试证据

```text
tests/
├── contract/
├── runtime-identity/
├── orchestration/
├── projection-snapshot-delta-replay/
├── mission-scope/
├── webui-live-and-history/
├── tui-projection/
├── authorization-redaction/
├── concurrency-recovery/
└── performance/
```

每组记录命令、退出码、耗时、失败修复、最终结果。不能只给测试数量。

### 25.3 场景证据

至少保存以下完整链路：

1. direct Turn；
2. 单 Team、多 Agent、并行 Tool；
3. 双 Team 串并混合与跨 Team 产物；
4. Skill 实际激活与 bound-but-unused；
5. required Tool 失败、optional Tool 失败、审批等待；
6. Replan、Recovery、取消和旧事件重放；
7. SessionDispatch；
8. 历史 Turn + 当前 live Turn；
9. WebUI/TUI 同时观察、断线重连和慢消费者；
10. selected Mission 跨 Session 全景。

每个场景同时校验业务结果、activities/relations、Surface 展示、证据下钻和性能记录。

### 25.4 封版门禁

以下任何一项不满足都不得提交版本/tag：

```text
[ ] 所有 W0-W7 工作流完成
[ ] 第 14.2 节旧路径生产引用为零
[ ] 第 15 节验收矩阵全绿
[ ] 第 21-24 节表格都有实际证据
[ ] core/edge schema 和生成类型一致
[ ] snapshot/delta/replay 等价
[ ] WebUI/TUI 均真实接线
[ ] 无 full/detail/mission 意外首屏请求
[ ] 无未分类 dirty/untracked 文件
[ ] 源码审查没有空实现、TODO 占位或兼容分支
[ ] 版本、tag、push 只在以上门禁后执行
```
