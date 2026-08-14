# Cowd

> 企业级、使命导向、图驱动的自主 AI 编排与多团队协同系统

## 1. 定位

Cowd 不是把模型、工具和聊天界面简单拼在一起的 Agent 外壳，而是一套面向真实生产任务的 **AI Harness 与自主执行平台**。它以 Mission 组织长期目标，以 Task 固化责任与结果归属，以 Execution Graph 表达依赖、并行、审批和恢复，再由统一 Runtime 驱动模型、工具、Agent、Team、Memory、Matrix 与外部系统协同工作。

它把高并发 AI 工作负载中最难长期维持一致的部分收敛为一套运行真相：

- **企业级执行治理**：身份、权限、审批、沙箱、预算、审计、事件与恢复贯穿同一条执行链。
- **使命导向**：一次对话可以形成跨 Turn、跨 Session、跨 Agent 的长期 Mission，而不是在聊天记录中丢失目标。
- **图驱动自主编排**：任务被编译为带依赖、资源约束、证据要求和验收门的执行图，安全节点并行，冲突节点串行。
- **多团队协同**：Team Template、AgentTask DAG、WorkingState、冲突仲裁、结果合成和 review gate 共同支撑复杂协作。
- **高并发而不失控**：Session、Agent、工具和 Provider 各有独立准入、背压、取消与资源上限；并发服从依赖、纯度、风险和预算。
- **事实与记忆双内核**：Memory 管理可召回的语义经验，Matrix 管理结构化事实、关系、证据与指标，两者通过 Fact Kernel 对齐语义。
- **一次执行，多种视图**：TUI、WebUI、消息渠道与业务 App 消费相同的事件、证据和状态投影，不各自推导运行事实。

```text
                          一个 Mission
                               │
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
              Session        Session       Schedule
                 │             │             │
              Root Task ── Delegated Task ──┘
                 │
                 ▼
          Execution Graph / AgentTask DAG
          ├── 模型推理与工具节点
          ├── 多 Agent / 多 Team 并行节点
          ├── 审批、证据与验收节点
          └── 检查点、恢复与终态归并
                 │
                 ▼
          可观察 · 可审计 · 可恢复的结果
```

> tips：既然DeepSeek Harness开源了，那么我也贡献给世界吧，让我们一起迎接新时代！（模型尝鲜带来了3天的版本质量塌陷，预计明天补回。该花的钱是真的一点也少不了）

## 2. 核心所有权

| 层 | 负责什么 | 不负责什么 |
|---|---|---|
| **Core** | 会话、任务、Mission、Agent、Team、模型与工具编排、记忆、事实、权限、Gateway、TUI | 具体渠道协议、平台 SDK、垂直业务规则和业务页面 |
| **Edge** | WebUI、消息与数据源 Connector、Managed Sidecar、协议适配和自动发现 | 改写 Runtime 的任务语义、模型循环或事实治理 |
| **App** | 垂直领域的数据模型、工作流、技能、页面、迁移与验证 | 绕过 Core 的安全、审计和能力合同 |
| **Runtime** | 执行图、生命周期、准入、调度、状态投影、证据与恢复 | 承载不可验证的隐式业务逻辑 |

这条边界允许 Core、Edge 与垂直 App 独立发布：Core 保持通用执行语义，Edge 隔离外部协议和重依赖，App 以声明式贡献接入统一治理。

## 3. 统一任务模型

| 概念 | 生命周期与职责 |
|---|---|
| **Session** | 用户与 Runtime 的持续交互容器；持有消息、Turn、分支、执行策略和实时订阅。 |
| **Turn** | 一次用户输入触发的 AI 执行循环；可包含多轮模型调用、工具调用、压缩和证据写入。 |
| **Task** | 跨 Turn 的目标与责任账本；Root Task 持有主目标，Delegated Task 继承边界和证据要求。 |
| **Mission** | 跨 Session、Task、Agent、Team 与 Schedule 的全局工作组织面；聚合贡献、状态和终态。 |
| **Execution Graph** | Runtime 的规范执行结构；节点表达工作、依赖、风险、资源、审批、证据和结果。 |
| **Evidence** | 工具收据、事实引用、事件、评审和恢复记录组成的可追溯依据。 |

```text
Mission（为什么做、最终完成什么）
  └── Task（谁负责、边界与结果归属）
       └── Turn（一次输入触发的执行循环）
            └── Graph Node（模型 / 工具 / Agent / 审批 / 验收）

Session 是交互容器，Mission 是全局组织面；二者通过 Task contribution 关联，互不替代。
```

## 4. 使用方式

### 4.1 安装与初始配置

Linux 与 macOS 可从源码安装。交互式安装会构建工作区，并配置模型端点、密钥、默认模型和工作目录。

```bash
./install.sh --release

# 已有配置或自动化环境
./install.sh --release --no-config
```

默认配置目录为 `~/.cowd`，基础配置模板见 `config-default.yaml`。

### 4.2 启动 Cowd

```bash
# 启动或进入默认 TUI
cowd

# Gateway 服务管理
cowd gateway start
cowd gateway status
cowd gateway open
cowd gateway logs
cowd gateway restart
cowd gateway stop

# 运行诊断
cowd doctor
cowd gateway doctor
```

Gateway 是所有交互面的统一后台。TUI 通过 HTTP/SSE 连接 Gateway；WebUI 由 Gateway 挂载；消息 Connector 通过 SurfaceHost 进入同一任务链。

### 4.3 从源码联调

```bash
# Core 检查与构建
make check
make build

# 带 TUI 的完整二进制
cargo run -p cli --bin cowd --features full

# WebUI（cowd-edge）
cd ../cowd-edge
npm --prefix surfaces/webui install
npm run dev:webui

# Edge 全量构建
npm run build
```

WebUI 构建产物位于 `cowd-edge/surfaces/webui/dist`，通过 `gateway.webui_dir` 挂载。Edge Connector 安装后由 Gateway 根据 `surface.json` 自动发现、校验和托管。

### 4.4 常用控制面

```text
CLI / TUI / WebUI / Message Connector
              │
              ▼
         Cowd Gateway
     ┌────────┼─────────┐
     ▼        ▼         ▼
  Session   Mission   Surface
     │        │         │
     └──── Runtime ─────┘
              │
     Tools · Memory · Matrix · App
```

- TUI：键盘优先的会话、任务、审批和运行控制。
- WebUI：Mission、执行图、Agent、证据、Memory、Matrix、Surface 与审计的浏览器控制台。
- 消息渠道：飞书、邮件、企微和微信 iLink 中的可靠收发与任务接续。
- 数据源：PostgreSQL、MySQL、MariaDB、飞书多维表和 Lark Base 的受治理读取。

## 5. 架构设计

### 5.1 系统全景

```text
                                ┌─────────────────────────────────────┐
                                │              用户入口                │
                                │   CLI · TUI · WebUI · 消息渠道       │
                                └──────────────┬──────────────────────┘
                                               │ HTTP/SSE · UDS/H2
                                               ▼
        ┌──────────────────────────────────────────────────────────────────┐
        │                         Gateway                                  │
        │                                                                  │
        │  RuntimeHost             SurfaceHost             Control Plane   │
        │  会话热加载 / Turn       inbox/outbox/DLQ        Mission/Task    │
        │  token / usage           sidecar 发现与托管       Apps/Auth/Eval │
        └───────────┬───────────────────┬───────────────────────┬──────────┘
                    │                   │                       │
                    ▼                   ▼                       ▼
        ┌────────────────────┐  ┌──────────────────┐  ┌──────────────────┐
        │ AI Harness Runtime │  │    Cowd Edge     │  │     Cowd App     │
        │                    │  │                  │  │                  │
        │ Execution Graph    │  │ WebUI Surface    │  │ 领域合同/工作流   │
        │ Mission/Task       │  │ Message Connector│  │ 技能/模型/页面     │
        │ Agent/Team         │  │ Source Connector │  │ MFG / future     │
        │ Context/Policy     │  └──────────────────┘  └──────────────────┘
        └─────────┬──────────┘
                  │
        ┌─────────┼───────────────────────────────┐
        ▼         ▼                               ▼
   Reality Core  Tool / Skill / Plugin       Model Provider
   Memory/Matrix MCP/LSP/Sandbox             OpenAI/Anthropic/
   Fact Kernel   Mutation Preview            DeepSeek/Qwen
        │
        ▼
   Storage：SQLite / PostgreSQL · Migration · Health · Event Store
```

系统遵循“**单一事实源，多种授权投影**”：Runtime 产生执行事实，Gateway 负责接入与传输，Surface 只呈现经过授权的状态，Edge 和 App 不复制会话或任务状态机。

### 5.2 一次任务

```text
用户 / Connector / Schedule
            │
            ▼
Gateway：认证 → 幂等受理 → 持久 inbox → Session 绑定
            │
            ▼
TaskRouter：复用 Root Task / 创建 Successor / 显式 Focus
            │
            ├── Delegated Task：继承 scope、能力、预算和证据要求
            └── Mission contribution：建立全局目标归属
            │
            ▼
Intent + Policy + Team Template
            │
            ▼
Execution Graph：节点、依赖、资源、风险、审批、验收
            │
      ┌─────┼──────────────┐
      ▼     ▼              ▼
  单 Agent  多 Agent/Team   Steward/Schedule
      │     安全并发         托管推进
      └─────┼──────────────┘
            ▼
WorkingState：evidence / conflict / unresolved / artifact
            │
            ▼
synthesis → verify/review gate → Task/Mission terminal
            │
            ▼
事件、证据、用量与结果一次归并 → TUI / WebUI / Channel / App
```

Task 是结果归属的权威，Execution Graph 是执行方式的权威，Mission 是跨任务组织状态的权威。三者各自持有一种真相，避免把聊天消息误当任务状态。

### 5.3 一次 AI Turn

```text
入站消息
   │
   ▼
Turn Inbox ──► RuntimeHost ──► ContextRuntimeKernel
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
               记忆/事实召回   预算与能力暴露   历史检查点
                    └─────────────┼─────────────┘
                                  ▼
                         Provider Request
                                  │ SSE
                                  ▼
                     ┌──── Conversation Loop ────┐
                     │ 模型输出                   │
                     │   ├── 继续推理             │
                     │   ├── 工具计划/并发执行     │
                     │   ├── Agent/Team 委派       │
                     │   ├── 审批等待与恢复         │
                     │   └── 语义压缩/上下文接续     │
                     └────────────┬───────────────┘
                                  ▼
                    Turn Supervisor / Safety Fuse
                                  │
                                  ▼
                 terminal outbox + usage + evidence + events
                                  │
                                  ▼
                       Surface Egress / 下一 Turn
```

Turn 内部允许多轮模型与工具交替，但对外保持一个可取消、可观察、可恢复的执行身份。循环停滞、预算耗尽、审批等待和工具失败都有明确状态，不以静默重试掩盖失败。

### 5.4 分层与依赖边界

```text
┌─────────────────────────────────────────────────────────────────────┐
│ Entry         cli · gateway · tui · surface                         │
├─────────────────────────────────────────────────────────────────────┤
│ AI Harness    runtime · harness-contract · harness-eval              │
│               model-protocol · provider · approval · session         │
├─────────────────────────────────────────────────────────────────────┤
│ Reality       fact-kernel · memory · matrix                          │
├─────────────────────────────────────────────────────────────────────┤
│ Capability    tools · skill · mcp · plugins · connector              │
├─────────────────────────────────────────────────────────────────────┤
│ Application   app-sdk · app-host · product-apps · auth-broker        │
├─────────────────────────────────────────────────────────────────────┤
│ Persistence   storage · fact/session/runtime/surface postgres adapters│
└─────────────────────────────────────────────────────────────────────┘
```

架构测试强制执行四条关键边界：

- `tui` 不依赖其他 workspace crate，只通过 Gateway HTTP/SSE 工作。
- `runtime` 不依赖 `surface`、`connector`、`tui` 或 `gateway`。
- `tools` 不反向依赖 `runtime` 或 `provider`。
- WebUI 与非 TUI Surface 位于 `cowd-edge`，平台 SDK 不进入 Core Runtime。

## 6. 核心特性矩阵

| 特性域 | 当前能力 | 状态 | 核心组件 |
|---|---|---|---|
| **Mission / Task** | Root/Delegated Task、跨 Turn 绑定、Focus、Mission contribution、计划与终态投影 | ✅ 生产闭环 | `TaskRouter` · `MissionOrganizer` · `mission_runtime` |
| **图驱动执行** | Deliberation、ReWOO、Tool DAG、依赖波次、资源门、Safety Fuse、结果归并 | ✅ 生产闭环 | `execution_core` · `orchestration` · `ExecutionGraphRunner` |
| **多 Agent / Team** | 模板编译、AgentTask DAG、能力选择、并行执行、WorkingState、冲突仲裁、合成与评审 | ✅ 核心闭环 | `agent_runtime` · `team_runtime` · `conflict_arbiter` |
| **高并发调度** | Session/Agent/工具/Provider 分层准入，读并行、写冲突串行、背压、取消与预算上限 | ✅ 生产闭环 | `SessionExecutionPlane` · `tool_orchestrator` · transport pool |
| **多模型路由** | OpenAI、Anthropic、DeepSeek、Qwen 协议适配、路由决策、性能记录与 fallback | ✅ 生产闭环 | `provider` · `model-protocol` · `ModelRouteDecision` |
| **上下文工程** | 动态预算、硬容量预检、证据计划、知识激活、语义检查点压缩 | ✅ 生产闭环 | `context_runtime` · `budget_policy` · `compact` |
| **Memory** | L0–L4 分层、向量/FTS 检索、有界压缩、候选治理与晋升 | ✅ 生产闭环 | `memory` · `fact-kernel` · `GrowthService` |
| **Matrix** | Entity/Relation/Fact/Evidence/Metric/Ontology、质量门、SQLite/PostgreSQL | ✅ 生产闭环 | `matrix-core` · `matrix-repository` · `MatrixDataPlane` |
| **权限 / 审批 / 沙箱** | 风险分类、Grant、节点级审批、自治档位、Linux 隔离、Mutation Preview、完整审计 | ✅ 生产闭环 | `policy_engine` · `ApprovalCoordinator` · `sandbox` |
| **工具 / MCP / Plugin** | 内置工具、异步 Bash、MCP Bridge、LSP、三级插件、Pre/Post Hook | ✅ 生产闭环 | `tools` · `mcp` · `plugins` · `tool_host` |
| **技能系统** | 多 Root 发现、安全扫描、维护评估、生成、路由和按 Surface 投影 | ✅ 生产闭环 | `skill/service` · `SkillRegistry` · `SkillRouter` |
| **Session / Recovery** | 并行会话、暂停/取消、分支、检查点、事件回放、恢复配方和终态幂等 | ✅ 生产闭环 | `session_execution` · `runtime_event_store` · `recovery` |
| **可靠消息 / Surface** | Manifest、Managed Edge、持久 inbox/outbox、DLQ、重试、回放和 operator 修复 | ✅ 生产闭环 | `SurfaceHost` · `message_store` · `ledger` |
| **App Host** | App 注册、配置启停、路由、技能、授权和 UI 同步投影；MFG 为参考 App | ✅ 生产闭环 | `app-sdk` · `app-host` · `product-apps` |
| **Harness Eval** | 场景矩阵、确定性 smoke、能力覆盖、发布证据和 Gateway 服务化 | ✅ 生产闭环 | `harness-eval` · `eval_gate` · `release_gate` |
| **TUI / WebUI** | 键盘优先 TUI、图形化 Mission/Agent/证据/Reality 控制台、统一 SSE 投影 | ✅ 生产闭环 | `tui` · `cowd-edge/surfaces/webui` |
| **Steward 托管执行** | Autonomy Profile、持久调度、决策账本、handoff | 🔶 基本具备 | `steward_agent` · `mission_schedule` |
| **受控进化** | Signal、Case、Diagnosis、Proposal、Candidate、Canary/Stable Review 与发布权限 | 🔶 基本具备 | `evolution` · `GrowthService` · evaluation policy |

`✅ 生产闭环` 表示关键路径已有代码级合同、持久状态与测试门禁；`🔶 基本具备` 表示核心语义已落地，自治深度或生产覆盖仍在增强。

## 7. 核心设计

### 7.1 Mission 与图驱动自主编排

Mission 不直接替代 Runtime 执行 Turn，而是组织 Session、Task、Team、Schedule、Approval 与 Recovery。输入先形成有归属的 Task，再编译为 Execution Graph；图节点持有依赖、服务等级、资源、风险、预算与验收语义。

```text
目标 ──► Mission ──► Root Task ──► Execution Graph
                                      │
                     ┌────────────────┼────────────────┐
                     ▼                ▼                ▼
                  foreground       background        scheduled
                  即时交互          长时工作           持久触发
                     │                │                │
                     └────── Graph State Store ───────┘
                                      │
                     pause / resume / cancel / recover
                                      │
                                      ▼
                       outcome + evidence + lineage
```

图是调度、观察与恢复的共同语言。实时流、历史回放和 WebUI 图形视图共享同一 graph identity 与 lineage，避免执行后再猜测依赖关系。

### 7.2 多 Agent 与多团队协同

```text
Task Intent
    │
    ├── direct ──► 单 Agent Conversation
    │
    └── team ──► Team Template ──► immutable AgentTask DAG
                                      │
                         ┌────────────┼────────────┐
                         ▼            ▼            ▼
                      Agent A      Agent B      Agent C
                      research     implement    verify
                         │            │            │
                         └──── Team WorkingState ──┘
                               │ evidence
                               │ conflict
                               │ unresolved
                               │ artifact
                               ▼
                       synthesis → review gate
```

Agent 不是匿名并发函数。定义注册表、能力上限、模型选择、运行身份、Task 继承、事件顺序和结果验证共同构成生命周期。Team 只并行无依赖且资源不冲突的节点；阶段结果先进入 WorkingState，冲突经过仲裁，最终结果经过合成与验收后才成为 Task 终态。

### 7.3 上下文与自治预算

上下文窗口不是一个可任意填满的字符串，而是受预算管理的稀缺资源。Runtime 根据模型窗口、自治档位和当前任务动态分配 Memory、工具结果、Agent 扇出、系统控制与 review 空间，并在请求前执行硬容量预检。

```text
模型窗口（配置或模型能力）
        │ 默认 70%，比例钳制 1%–95%
        ▼
上下文总预算
  ├── 必需系统语义与工具 Schema
  ├── L0–L4 Memory / Matrix 证据
  ├── 历史与语义检查点
  ├── 工具结果和 Artifact 引用
  └── Agent / Team / Review 预留
        │
        ▼
证据计划 → 知识激活 → 上下文包 → Provider
        ▲                              │
        └──── 结果落账 / 压缩接续 ◄────┘
```

| 自治档位 | 权限姿态 | 审批姿态 | 最大并行 | 最大轮次 | 单 Turn 成本上限示例 |
|---|---|---|---:|---:|---:|
| cautious | read-only | supervised | 1 | 3 | 8k tokens / 25 cents |
| supervised | workspace-write | balanced | 2 | 10 | 32k / 150 cents |
| stewarded | workspace-write | autonomous | 3 | 24 | 64k / 500 cents |
| autonomous | danger-full-access | autonomous | 4 | 30 | 96k / 750 cents |
| yolo | danger-full-access | trust-all | 4 | 40 | 128k / 1000 cents |

档位数字是成本和执行上限，不是模型上下文窗口。实际并发仍受图依赖、资源冲突、Provider 准入和系统容量约束。

### 7.4 Memory、Matrix 与受控进化

```text
运行证据 / 用户事实 / 工具收据 / 团队结论
                    │
                    ▼
              Fact Kernel
          ┌─────────┴─────────┐
          ▼                   ▼
       Memory               Matrix
  L0 Identity          Entity / Relation
  L1 Core              Fact / Evidence
  L2 Project           Metric / Ontology
  L3 Deep              Snapshot / Quality
  L4 Shared            Revision / Attention
          └─────────┬─────────┘
                    ▼
             Evolution Governance
      signal → case → diagnosis → proposal
      → candidate → canary → stable review
                    │
                    ▼
       Memory / Skill / Agent / Team 的受控晋升
```

Memory 保存语义经验和召回线索；Matrix 保存可计算、可追溯的结构化事实。L0 身份只允许 User/System 写入，L4 共享和 Agent/Team 终态必须经过证据、权威性、冲突与审批治理。模型可以提出候选，不能绕过校验直接修改稳定能力。

Matrix 的 SourcePack 以分块 snapshot、checksum 和 watermark 接收外部数据。块数据与 receipt 在同一事务提交，只有最终块成功才推进 watermark；指标使用受治理的计算合同，不把 Matrix 扩张为无边界数据湖。

### 7.5 模型路由与执行策略

`model-protocol` 定义供应商中立的请求、流式事件、工具调用和用量语义；`provider` 负责 OpenAI、Anthropic、DeepSeek 与 Qwen 的适配、连接池、路由和 fallback。Runtime 根据任务、能力、策略与历史性能形成显式 `ModelRouteDecision`，路由结果进入事件与成本账本。

Deliberation、ReWOO、Tool DAG、Reflexion 等策略都落在统一 Execution Graph 和 Outcome 语义上。策略决定“如何执行”，不会创建第二套 Task、Agent 或恢复状态机。

### 7.6 工具、技能、MCP 与 Plugin

- **Tool Host**：统一 lease、权限、效果分类、超时、取消、Artifact、账本与 Memory pulse。
- **异步 Bash**：进程组回收、输出有界、完整输出持久化、环境变量白名单和网络域策略。
- **工具并发**：只读工具可并行；同一路径写入串行；网络、进程、用户级与破坏性操作提升风险等级。
- **MCP**：外部工具通过桥接进入相同的能力、审批和审计链，不成为旁路。
- **Skill**：多 Root 发现、安全扫描、维护评估、路由和 Surface 投影分离“可安装”与“本次可用”。
- **Plugin**：Builtin、Bundled、External 三级来源以及 Pre/Post Hook 共享统一注册表。

### 7.7 权限、审批与沙箱

```text
Autonomy Profile
  └── PermissionMode + SandboxPosture + ApprovalProfile + Budget
                                      │
                                      ▼
工具效果分类 ──► Grant ──► 风险策略 ──► 审批队列
                                      │
                 ┌────────────────────┼────────────────────┐
                 ▼                    ▼                    ▼
             low-risk             reversible            high-risk
             确定性放行             steward/人工          人工决策
                 └────────────────────┼────────────────────┘
                                      ▼
                          Sandbox / Host Execution
                                      │
                                      ▼
                       submitted + decided + receipt
```

Gateway 只消费 Runtime 派生的 `sandbox_posture`，不在入口层重新解释权限。Linux 支持 bwrap 与 Landlock/seccomp 能力检测及受控降级；Windows 与 macOS 的等价内核隔离尚未生产就绪，完整边界见第 10 章。

### 7.8 高并发与资源治理

Cowd 的并发分为四层：Session 并行、Agent/Team 并行、Tool wave 并行和 Provider 连接并行。每一层都有独立 semaphore、资源键、取消传播、背压与预算；同一文件写入、相互依赖节点和高风险副作用不会因追求吞吐而越过顺序约束。

```text
请求洪峰
   │
   ▼
Session Admission
   ├── Session A ──► Graph wave 1 ──► read tools × N
   │                              └─► write(path X) × 1
   ├── Session B ──► Team agents × budget
   └── Session C ──► Provider pool / fallback
   │
   ▼
backpressure · cancellation · deadline · usage reconciliation
```

### 7.9 Session、事件账本与恢复

运行中的 Turn 使用内存执行账本维持低延迟，终态将 Runtime events、terminal outbox、用户输入回执、用量和证据在事务边界归并。崩溃前未形成 durable terminal 的 Turn 可从输入回执与检查点重建；已经提交的终态以幂等键拒绝重复结算。

事件覆盖 Mission、Session、Team、Agent、Tool、Approval、Recovery 与 Surface，实时流和历史回放从同一持久事实投影。

### 7.10 Surface 与可靠消息

```text
Inbound                                  Outbound
received → processing → processed       queued → sending → sent
               │             │                    │
               ▼             ▼                    ▼
       failure_notifying   replying        retry_scheduled
               │             │                    │
               ▼             ▼                    ▼
       failed_notified     replied             dead_letter
```

SurfaceHost 持有 inbox、outbox、DLQ、退避重试、回放和 operator 修复。消息 Connector 只负责平台协议、媒体和动作适配，不创建 Session，也不执行模型循环；回复、失败通知和 typing 清理都有可追踪终态。

### 7.11 App Host 与业务扩展

App 通过 `app-sdk` 声明路由、技能、授权、配置、数据迁移和 UI 贡献，由 `app-host` 与 `AppRegistry` 统一装配。MFG 是首个参考 App，用于验证垂直业务模型可以独立演进，同时复用 Core 的 Mission、Memory、Matrix、审批、事件和身份系统。

### 7.12 Eval 与交互 Surface

Harness Eval 把能力场景、确定性 smoke、覆盖矩阵、性能数据和发布证据纳入 Gateway 控制面。TUI 提供紧凑、键盘优先的即时工作流；WebUI 提供 Mission 图、Agent 拓扑、执行证据、Reality Core、Surface、技能、工具和审计工作台。两者只投影 Runtime 事实，不伪造进度。

## 8. Runtime 模块地图

`crates/runtime/src/module_map.rs` 是代码级归属合同，模块身份、所属域、公开面和生命周期所有权由架构测试校验。

```text
Conversation   Turn、收件箱、会话热运行与事件
Provider       模型传输、注册、策略与连接池
Tooling        工具计划、调度、执行、策略与记忆
Mission        Task、Mission 控制、证据、调度与命令路由
Session        会话执行、输入、生命周期与关系图
Agent          定义、能力、选择、运行与结果验证
Team           实例化、AgentTask、WorkingState、投影与结果归并
Steward        托管 Agent 与持久调度
Approval       审批协调、队列与门控
Context        预算、证据、知识、资源与上下文组装
Recovery       事件存储、回放与恢复配方
Policy         权限、安全、信任、自治与跨面策略
ExecutionCore  执行图、监督、策略、实时投影与结果
RealityBridge  结构化数据、事实提取、决策与召回端口
Evolution      Signal、Case、Candidate、评测与发布治理
Configuration  配置、校验与 Profile
Infrastructure 能力、检查点、质量门、升级、MCP、Sandbox 与 Surface 合同
Skill          技能激活、选择与记忆集成
```

## 9. Core、Edge、App 与外部系统

```text
                    Cowd Core / Gateway
 ┌──────────────────────────────────────────────────────────┐
 │ RuntimeHost        SurfaceHost       AppRegistry/AuthBroker│
 │ AI Turn / Graph    发现/托管/账本     路由/技能/授权/UI      │
 └────────┬─────────────────┬───────────────────┬────────────┘
          │                 │                   │
          ▼                 ▼                   ▼
       Cowd TUI          Cowd Edge           Cowd App
       终端 Surface       ├─ WebUI             ├─ MFG
                         ├─ Message Connector  └─ future apps
                         └─ Source Connector
                              │
            ┌─────────────────┼─────────────────────┐
            ▼                 ▼                     ▼
       飞书/企微/微信/邮件   SQL / Bitable / Base   浏览器用户
```

| 外接对象 | 接入位置 | Core 中的落点 | 边界 |
|---|---|---|---|
| 模型供应商 | Provider Adapter | Model route、stream、usage | 供应商协议不进入 Conversation 语义 |
| MCP Server | MCP Bridge | Governed Tool | 必须经过能力、审批、超时和审计 |
| 消息平台 | Edge Message Connector | Surface inbox/outbox | Connector 不拥有 Session 和 AI Turn |
| 数据源 | Edge Source Connector | SourcePack → Matrix | Connector 不直接写 Memory/Matrix |
| 浏览器界面 | Edge WebUI Surface | Gateway projection | WebUI 不推导任务终态 |
| 垂直业务 | App SDK / Host | 受治理路由、技能、模型和 UI 贡献 | App 不绕过身份、权限和事件合同 |
| PostgreSQL | Backend Adapter | Session/Fact/Runtime/Surface store | 与 SQLite 保持语义一致，禁止隐式双写 |

Cowd Edge 的连接器、模块与 Managed Edge v2 生命周期见 [cowd-edge README](../cowd-edge/README.md)。

## 10. 大演进方向（尚未生产就绪）

以下能力代表 Cowd 的长期演进方向，不构成当前生产承诺：

- **跨节点 Runtime 与全局调度**：Execution Graph 在多机、多集群间迁移，具备全局资源配额、租约、故障转移和一致终态。
- **长期自治 Mission**：Steward 从持久调度与 handoff 扩展为可连续运行数周的目标管理、价值评估、主动补证和人类对齐闭环。
- **多组织、多团队联邦**：跨租户 Agent/Team 协议、能力市场、证据交换、信任域和最小披露协作。
- **受控自进化**：Skill、Agent、Team Template 与策略候选自动生成，在隔离评测、Canary、Stable Review 和人工发布门后逐级晋升。
- **全平台强沙箱**：补齐 Windows 与 macOS 的内核级网络、文件系统、进程和凭据隔离，使安全姿态跨平台等价。
- **Reality Core 深化**：更丰富的指标合同、因果与时态事实、流式 Source、跨域本体和大规模证据质量治理。
- **持续价值评测**：从能力 smoke 扩展到长期任务完成率、证据质量、成本收益、回归归因和自动阻断发布。
- **可验证外部行动**：跨消息、业务 App 与自动化系统的动作统一编译为可预演、可审批、可补偿、可证明的执行图。

演进能力只有在代码合同、真实进程验证、恢复演练、安全证据和发布门同时成立后，才会进入生产特性矩阵。

## 文档入口

- [系统说明书](docs/README.md)
- [架构文档](docs/architecture/README.md)
- [运维与排障](docs/operator/README.md)
