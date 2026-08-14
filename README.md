# Cowd — Rust 原生 AI Harness 内核

> Rust 2021 Edition · MIT

> 文档入口：[docs/README.md](docs/README.md)（架构 / 运维 / 故障处理）。
> **定位：** AI 执行内核与统一控制面，而不是把每一种业务能力塞进单体二进制。

Cowd Core 负责 Agent 编排、模型调用、工具执行、任务流、记忆与状态、Gateway API 与 TUI 终端交互。它用稳定的能力合同连接独立演进的 Edge、Surface、Connector 与 App。

本页集中描述架构全景、模块地图、运行链路和边界说明。具体运行与运维说明请以仓库内文档为准，接口和能力以源码、运行时能力合同与发布 manifest 为准。

## 1. 阅读导航

| 需要了解什么 | 入口 |
|---|---|
| Core、Edge、App 的边界和一次任务如何流转 | 第 2–5 章 |
| 特性矩阵与各模块图示 | 第 6 章（6.3 图示化总览） |
| 模块归属、API、依赖图、消息状态机、多 Agent 流程 | 第 7–11 章 |
| 使用方式、Capability、配置、验证 | 第 12–15 章 |
| 启动、配置、排障、部署 | [系统说明书](docs/README.md) |
| App 的声明、装配与治理约定 | [架构文档](docs/architecture/README.md) |
| API 与能力合同 | [文档索引](docs/README.md) 与运行时能力合同 |
| TUI 使用与交互行为 | [架构文档](docs/architecture/README.md) |

文档结构：第 1–11 章是系统总览、特性矩阵与图示；第 12–15 章只保留最简使用、配置与验证；分域细节统一收敛到 docs/，README 不重复承载。

## 2. 核心所有权

| 层 | 负责什么 | 不负责什么 |
|---|---|---|
| **Core** | 会话、任务、Agent、模型与工具编排、记忆、权限、Gateway、TUI | 具体渠道协议、业务产品规则、业务页面 |
| **Edge** | Connector 协议适配、WebUI/TUI Surface、侧车进程与自动发现 | 改写 Core 的任务语义或存储治理 |
| **App** | 垂直领域的模型、工作流、页面、迁移与验证 | 跨越 Core 的安全、审计、能力合同 |
| **Runtime** | 进程生命周期、发现、健康、授权、状态投影 | 承载不可验证的隐式业务逻辑 |

这条边界让新 App 能作为产品仓独立演进，也让一个 Core 安装能按声明发现、校验、启用或禁用能力，而不演化为不可维护的业务单体。

## 3. 一次任务如何运行

```text
用户 / Connector / WebUI / TUI
            │
            ▼
Gateway：认证、会话、能力合同、任务受理
            │
            ▼
Core Runtime：规划 → 模型调用 → 工具/Agent 并发执行 → 记忆与状态归并
            │
            ├── Edge：外部系统、消息与 Surface 的协议适配
            └── App：MFG 等领域工作流、数据模型和专属页面
            │
            ▼
统一事件、审计、状态与结果投影回各个 Surface
```

关键原则是“**单一事实源，多种视图**”：任务及其状态在 Core 内收敛；WebUI、TUI、Connector 和 App 页面消费同一份经过授权的能力与事件投影，不各自发明任务状态。

---

## 4. 架构全景

```
                                ┌─────────────────────────────────────┐
                                │           用户入口 (Entry)            │
                                │  CLI ──── TUI ──── WebUI ─── Channel │
                                │ (极薄)  (终端面)  (cowd-edge) (飞书/邮件/企微/微信) │
                                └──────────────┬──────────────────────┘
                                               │ HTTP/SSE / UDS·H2
                                               ▼
        ┌──────────────────────────────────────────────────────────────────┐
        │                     Gateway (统一后台入口)                         │
        │  Axum HTTP Server :8642  ·  SSE Stream  ·  CORS  ·  Graceful Shutdown │
        │                                                                   │
        │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
        │  │ RuntimeHost  │  │ SurfaceHost  │  │ Gateway Services  │  │
        │  │ 会话热加载    │  │ Inbox/Outbox │  │ runtime·session·task  │  │
        │  │ turn 执行     │  │ DLQ·重试·回放│  │ memory·matrix·apps   │  │
        │  │ token 管控    │  │ sidecar 托管 │  │ tools·skills·agents  │  │
        │  └──────┬───────┘  └──────┬───────┘  │ surface·approval·...  │  │
        │         │                 │           └───────────────────────┘  │
        └─────────┼─────────────────┼──────────────────────────────────────┘
                  │                 │
    ┌─────────────┼─────────────────┼──────────────────────────────┐
    │             ▼                 ▼                              │
    │   ┌──────────────────────────────────────────────────┐      │
    │   │        AI Harness Runtime (crates/runtime)        │      │
    │   │      显式 module map · 生命周期与治理架构域             │      │
    │   │                                                   │      │
    │   │  ┌──────────────┐ ┌──────────────┐ ┌──────────┐  │      │
    │   │  │ Conversation │ │   Mission    │ │  Session │  │      │
    │   │  │ turn·压缩·SSE │ │ 控制·投影·证据│ │ 执行平面  │  │      │
    │   │  ├──────────────┤ ├──────────────┤ ├──────────┤  │      │
    │   │  │    Agent     │ │    Team      │ │ Steward  │  │      │
    │   │  │ 生命周期·协作  │ │ 执行循环·Cron │ │ 调度·托管 │  │      │
    │   │  ├──────────────┤ ├──────────────┤ ├──────────┤  │      │
    │   │  │   Context    │ │   Approval   │ │ Recovery │  │      │
    │   │  │ 组装·预算·扇出│ │ 全局队列·门控  │ │ 事件·回放 │  │      │
    │   │  ├──────────────┤ ├──────────────┤ ├──────────┤  │      │
    │   │  │   Policy     │ │  Tooling     │ │ Provider │  │      │
    │   │  │ 跨面·信任·自治 │ │ 调度·账本·缓存 │ │ 路由·回退 │  │      │
    │   │  ├──────────────┤ ├──────────────┤ └──────────┘  │      │
    │   │  │ ExecutionCore│ │RealityBridge │               │      │
    │   │  │ 模式·ReWOO·DAG│ │结构化数据合同  │ module map  │      │
    │   │  └──────────────┘ └──────────────┘               │      │
    │   └──────────────────────────────────────────────────┘      │
    │                                                              │
    │   ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐   │
    │   │ Reality Core│  │ Tool System  │  │ Model Provider   │   │
    │   │             │  │              │  │                  │   │
    │   │ fact-kernel │  │ tools        │  │ model-protocol   │   │
    │   │   memory    │  │  · MCP bridge│  │   provider       │   │
    │   │   matrix    │  │  · plugins   │  │   mcp            │   │
    │   │ App bundles │  │  · sandbox   │  │ (OpenAI/Anthro/  │   │
    │   │ MFG / future│  │  · LSP/file  │  │  DeepSeek/Qwen)  │   │
    │   └─────────────┘  └──────────────┘  └──────────────────┘   │
    │                                                              │
    │   底层存储: storage (SQLite·PostgreSQL·Migration·Health)        │
    └──────────────────────────────────────────────────────────────┘
```

### 4.1 分层架构

```
┌─────────────────────────────────────────────────────────────────────┐
│ Entry 层         cli · gateway · tui · surface                      │
│                   极薄入口 · 统一后台 · 终端面 · Edge 协议            │
├─────────────────────────────────────────────────────────────────────┤
│ AI Harness 层    harness-contract · harness-eval · runtime          │
│                   · session · approval · model-protocol · provider  │
│                   · mcp · plugins                                    │
│                   语义合同 · 评测 · 执行核心 · 会话 · 审批 · 模型    │
├─────────────────────────────────────────────────────────────────────┤
│ Fact 层          fact-kernel · memory · matrix                      │
│                   事实语义 · 非结构化记忆 · 结构化事实引擎            │
├─────────────────────────────────────────────────────────────────────┤
│ Tool/Skill 层    tools · skill · connector · surface::channel       │
│                   内置工具 · 技能目录 · 外部连接器 · 渠道合同         │
├─────────────────────────────────────────────────────────────────────┤
│ Application 层   app-sdk · app-host · product-apps · storage        │
│                   受治理业务 App · 通用存储 · 遥测基础                │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Matrix 核心内核

Matrix 是结构化事实与证据引擎，不是通用 OLAP 或数据湖。外部数据进入 Matrix 前先经过 SourcePack/Watermark，再落到实体、关系、事实、证据、指标和质量门。

```text
Edge Source / Connector
          │ 分块 snapshot + checksum + watermark
          ▼
   SourcePack / SourceSnapshot / DataPlaneWatermark
          │
          ▼
┌───────────────────────────────────────────────────────────┐
│ Matrix Core                                               │
│                                                           │
│  Entity ── Relation ── Fact ── Evidence Packet            │
│     │          │          │                               │
│     └──────────┴──────────┘                               │
│                  │                                        │
│      Metric ─ Dependency ─ Snapshot ─ Attention           │
│                  │                                        │
│      Quality Gate ─ Ontology ─ Revision/Migration         │
└───────────────────────────────────────────────────────────┘
          │
          ▼
   Fact Kernel
     ├── Memory：非结构化经验、语义关联、上下文召回
     └── Matrix：结构化事实、证据链、可计算指标
```

关键约束：SQLite 与 PostgreSQL 在同一事务内提交块数据和 receipt，只有最后一块成功才推进 watermark；指标仅支持受治理的 sum/ratio 合同。

### 4.3 App 与 Edge 关系

```text
                    Cowd Core / Gateway
 ┌─────────────────────────────────────────────────────┐
 │ AppRegistry ──> API / Skills / Auth / OpenAPI / UI   │
 │ SurfaceHost  ──> Surface 发现、可靠 inbox/outbox     │
 │ AuthBroker   ──> 身份、授权目录、surface capability   │
 └──────────────┬──────────────────────────────────────┘
                │ surface.json + UDS/H2
    ┌───────────┴───────────────────────────┐
    ▼                                       ▼
┌─────────────────────┐          ┌──────────────────────────────┐
│ Cowd Edge            │          │ Cowd App                     │
│ WebUI / Connectors   │          │ product-apps / app-bundle    │
│  · WebUI Surface     │          │  · MFG contract/core/adapter │
│  · Message Connector │          │  · App SDK contribution      │
│  · Source Connector  │          │  · MFG WebUI/TUI             │
└─────────────────────┘          └──────────────────────────────┘
```

Edge 负责把用户、消息平台和数据源接入 Gateway；App 负责垂直业务域。两者都不拥有 Runtime、不复制会话状态，也不绕过 Core 的安全、审计与能力合同。

---

## 5. 协作全景：一次 AI Turn 的完整链路

```
外部消息到达 (飞书/邮件/企微/微信)
        │
        ▼
┌──────────────────────┐
│  Surface Ingress      │  Managed Edge → Gateway SurfaceHost
│  → 写入持久 inbox     │  status: received
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  SurfaceHost          │  策略匹配: QuickReply / DeepInvestigation
│  → 设置超时+迭代预算   │  status: processing
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  RuntimeService       │  创建/复用 RuntimeHost
│  → Task Router        │  续接/新建/successor/复合 Root Task
│  → 组装 Context       │  ContextRuntimeKernel: 记忆召回·知识激活·预算分配
│  → 启动 Agent Turn    │
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  Conversation Loop    │  SystemPromptBuilder → ProviderClient → SSE Stream
│  ←→ 模型              │  turn_supervisor 监控循环/停滞
│  ←→ 工具执行          │  tool_dispatch → tool_ledger → tool_memory(pulse)
│  ←→ 上下文压缩        │  compact (必需上下文硬容量预检)
│  ←→ 记忆脉冲          │  memory::emit_pulses_from_workgraph
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  Turn 完成             │
│  → 持久 session_events │
│  → 记录 usage/cost     │  ModelPerformanceRegistry 聚合性能数据
│  → 生成 evidence       │  工具证据·memory evidence·recovery evidence
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│  Surface Egress        │  Gateway SurfaceHost → outbox → sidecar
│  → 投递回复            │  status: replied / reply_failed / retry_scheduled
│  → 清理 Typing 状态    │  message.processing_complete action
└──────────────────────┘

多 Agent 协作分流：

用户请求 → intent_planner (意图分类)
  ├─ solo 任务 → Conversation 直接执行
  ├─ team 任务 → Team Template → immutable AgentTask graph
  │               ├─ Agent A/B 并行 → AgentRuntime lifecycle
  │               ├─ Team WorkingState → evidence/conflict/unresolved
  │               └─ dependency → synthesis → verify/review gate
  └─ steward 任务 → StewardScheduler: tick → autonomy_profile → 托管执行
                    └─ steward_agent → decision_ledger → handoff
```

Session、Turn、Task、Mission 的完整所有权和路由不变量见
[架构文档](docs/architecture/README.md)。

---

## 6. 核心特性矩阵

| 特性域 | 能力 | 成熟度 | 关键组件 |
|--------|------|--------|----------|
| **多模型路由** | OpenAI/Anthropic/DeepSeek/Qwen 自动适配 + Provider fallback 链 | ✅ 生产就绪 | `provider` · `model-protocol` · `ModelRouteDecision` |
| **会话管理** | 多 session 并行、切换、后台运行、暂停/关闭、检查点/恢复；持久化连接池并发访问 | ✅ 生产就绪 | `session_execution` · `SessionExecutionPlane` · `UnifiedSessionStore` |
| **Task/Mission 治理** | 普通消息自动路由 Root Task、跨 Turn 绑定、Delegated Task 继承、显式 focus、异步 Mission 组织和 Session contribution 投影 | ✅ 生产就绪 | `runtime::task` · `TaskRouter` · `MissionOrganizer` · `MissionControlProjection` |
| **上下文工程** | 动态预算分配、硬容量预检、语义检查点压缩、记忆召回、知识激活、证据规划 | ✅ 生产就绪 | `context_runtime` · `budget_policy` · `compact` |
| **5 层记忆系统** | L0身份→L1核心→L2项目→L3深度→L4共享 + 有界压缩 + 向量/FTS 检索 | ✅ 生产就绪 | `memory` · `fact-kernel` · `CognitiveContextManager` |
| **进化记忆** | 确定性规则 + 模型候选双层治理，候选校验/提升/审计闭环；L0/L4 与用户输入不进入语义自治 | ✅ 生产就绪 | `evolution` · `memory_maintenance` · `GrowthService` |
| **结构化事实引擎** | 实体/关系/证据/Metrics/Ontology + 后端中立持久化 + 质量门控 | ✅ 生产就绪 | `matrix-core` · `matrix-repository` · `MatrixDataPlane` |
| **多 Agent 协作** | Team 模板编译 → AgentTask DAG → 资源受控并行 → WorkingState → synthesis/verify；Agent 生命周期实时/持久统一投影 | ✅ 核心闭环 | `orchestration` · `ExecutionGraphRunner` · `AgentRuntime` · `team` |
| **Agent 讨论** | 多 agent 讨论引擎、共识方法、联合问题求解管道 | 🔶 基本具备 | `agent_discussion` · `joint_problem_solving` |
| **托管执行(Steward)** | Autonomy profile 驱动、tick调度、决策账本、handoff | 🔶 基本具备 | `steward_runtime` · `steward_scheduler` |
| **可靠消息投递** | Inbox→Outbox→DLQ 完整状态机、重试/backoff、operator 修复入口 | ✅ 生产就绪 | `SurfaceHost` · `message_store` · `ledger` |
| **Surface 协议** | `surface.json` manifest、UDS/H2 managed 与 static/OneShot lifecycle | ✅ 生产就绪 | `surface` · `SurfaceManifest` · `surface.json` |
| **事件账本 & 恢复** | 覆盖 mission/session/team/agent/tool/recovery 的事件存储+回放 | ✅ 基本具备 | `runtime_event_store` · `recovery` · `recovery_recipes` |
| **跨面治理(Policy)** | 跨入口身份绑定、授权、风险审计、信任解析、自治预算 | ✅ 生产就绪 | `cross_plane_policy` · `trust_resolver` · `autonomy_profile` |
| **权限 & 审批** | PermissionMode + Runtime ApprovalCoordinator + 持久化 Request/Grant；低风险策略放行，高风险统一人工决策 | ✅ 生产就绪 | `permissions` · `approval_coordinator` · `approval_queue` · `RuntimeEventStore` |
| **工具系统** | 内置工具 + MCP 桥接 + Plugin 集成 + LSP + Checkpoint + Mutation Preview | ✅ 生产就绪 | `tools` · `tool_orchestrator` · `mcp_tool_bridge` |
| **技能目录** | 多 root 发现、安全扫描、维护评估、生成、路由、projection | ✅ 生产就绪 | `skill/service` · `SkillRegistry` · `SkillRouter` |
| **Harness Eval** | 场景矩阵、确定性 smoke、能力覆盖报告、Gateway 服务化 | ✅ 生产就绪 | `harness-eval` · `/api/harness-eval/*` |
| **TUI 控制面** | Clean/Panorama 双模式、Control Deck、键盘优先、SSE attach | ✅ 生产就绪 | `tui` · `GatewayTuiConfig` |
| **插件系统** | Builtin/Bundled/External 三级插件 + Pre/Post Hook | ✅ 生产就绪 | `plugins` · `PluginRegistry` · `HookRunner` |
| **通用 App 宿主** | 已编译 App 的统一注册、配置启停、路由/技能/授权/界面同步投影；MFG 为首个参考 App | ✅ 生产就绪 | `app-sdk` · `app-host` · `product-apps` · `AppRegistry` · `auth-broker` |
| **沙箱执行** | Linux 容器检测、workspace-only/allow-list 隔离模式 | ✅ 基本具备 | `sandbox` · `sandbox_exec` |
| **执行模式** | Deliberation/ReWOO/Tool DAG/Reflexion 等执行策略 | 🔶 基本具备 | `execution_core` · `orchestration` · `strategy_matcher` |

Session 策略、Agent 子级能力上限、审批范围、Surface writer 和故障分类的完整边界见 [架构文档](docs/architecture/README.md)；配置与日常排查见 [运维文档](docs/operator/README.md)。

### 6.1 自治预算

自治档位同时约束并发、轮次、token 与成本，避免“全自主”变成无限执行。

| 档位 | 权限 | 审批 | 沙箱 | 最大并行 | 最大轮次 | 档位成本上限(示例 token) | 成本上限 |
|---|---|---|---|---|---|---|---|
| cautious | read-only | supervised | read-only sandbox | 1 | 3 | 8k | 25 cents |
| supervised | workspace-write | balanced | workspace-write sandbox | 2 | 10 | 32k | 150 cents |
| stewarded | workspace-write | autonomous | workspace-write sandbox | 3 | 24 | 64k | 500 cents |
| autonomous | danger-full-access | autonomous | host full access | 4 | 30 | 96k | 750 cents |
| yolo | danger-full-access | trust-all | host full access | 4 | 40 | 128k | 1000 cents |

上下文预算不是写死的，而是按当前模型上下文窗口等比缩放：

```text
模型上下文窗口（按当前模型解析 / 配置覆盖）
        │  默认取 70%（比例钳制 1%–95%）
        ▼
上下文预算 subsystem_budget_tokens
        ├── memory 召回预算
        ├── tool 结果预算（总量/单条）
        ├── subagent / team 预算（按 profile 系数）
        └── runtime control / review 预算
        │
        ▼
档位成本上限（上表 8k–128k）约束单 turn 花费
```

示例：1M 窗口模型 → 默认上下文预算约 700k；128k 窗口模型 → 约 89.6k；32k 窗口模型 → 约 22.4k。档位表内数字是成本上限示例，不是上下文预算本身。

### 6.2 记忆分层

```text
L0 Identity  ── 角色、语言、稳定身份
  │
L1 Core      ── 当前任务最关键的事实与约束
  │
L2 Project   ── 项目/工作区级长期经验
  │
L3 Deep      ── 深层主题与可召回知识
  │
L4 Shared    ── 跨会话、跨任务共享知识
```

Memory 负责非结构化记忆、语义关联与上下文召回；Matrix 负责结构化事实、实体关系、证据链和可计算指标。两者通过 `fact-kernel` 共享事实语义，但保持独立治理。

### 6.3 核心模块图示化总览

#### 整体进化框架（Evolution Framework）

```text
运行证据 / 用户输入 / 工具收据 / 团队结论
        │
        ▼
  ┌─────────────────────────────────────────────┐
  │ 记忆治理（确定性规则优先，模型只处理未决候选）│
  │ 来源 / 层级 / 优先级 / scope / ID / 置信度    │
  └──────┬──────────────────────────┬───────────┘
         ▼                          ▼
  ┌─────────────┐            ┌─────────────┐
  │ L0..L4 记忆  │            │ Matrix 事实  │
  │ 身份→共享    │            │ 实体/关系/证据│
  └──────┬──────┘            └──────┬──────┘
         └───────────┬──────────────┘
                     ▼
          ┌────────────────────┐
          │ Evolution Governance │  候选 → 校验 → 提升(promotion) → 审计
          └─────────┬──────────┘
                    ▼
   上下文召回 / 事实证据 / 技能与 Agent 定义演化 / 模板演化
                    │
                    ▼
              下一次运行（再次沉淀证据）
```

#### 记忆与事实分层

```text
L0 Identity（角色/语言/稳定身份）
  │
L1 Core（当前任务关键事实与约束）
  │
L2 Project（项目/工作区长期经验）
  │
L3 Deep（深层主题可召回知识）
  │
L4 Shared（跨会话/跨任务共享）

非结构化记忆 ──► fact-kernel ◄── 结构化事实（Matrix）
       语义关联/召回            实体/关系/证据/指标
```

#### Mission 与 Task 治理

```text
用户输入 ──► TaskRouter ──► Root Task（跨 Turn 绑定）
                │
                ├── Delegated Task（团队/子 Agent 继承）
                ├── 显式 focus / focus partition
                └── MissionOrganizer ──► Mission 全局投影 / contribution
```

#### 上下文工程

```text
模型窗口 ──► 动态预算/容量预检 ──► 证据计划 ──► 记忆召回(L0..L4)
                                                  │
                                                  ▼
知识激活 ──► 语义检查点压缩 ──► 上下文组装（system+证据+历史+工具）
                                                  │
                                                  ▼
                                     Provider 请求（前缀缓存/连接池准入）
```

#### 权限 / 审批 / 沙箱

```text
一个档位 ──► PermissionMode + SandboxPosture + ApprovalProfile + Interruption
                │
                ▼
        工具执行（Gateway 消费 Runtime 派生的 posture，单一权威）
                │
                ├── 普通工具审批：TrustAll 自动批准 + 审计
                ├── 图审批：autonomous/yolo 自动批准 + 审计成对
                └── bash 审计：posture/enabled/network/kernel/fallback
```

#### 存储与事件账本（内存优先终态）

```text
turn 内存账本（图/工具效果/证据/策略）
        │
        ▼
终态一次性批量落库（事件 + terminal outbox + 用户回执 同事务）
        │
        ▼
崩溃恢复只从用户输入回执重建
```

多 Agent 协作的完整流程见第 11 章图示，此处不重复。

---

## 7. 模块归属合同 (Module Map)

`crates/runtime` 通过 `runtime::module_map` 形成代码级归属合同。模块身份、所属域、所有者、公开面与生命周期所有权由 `runtime_module_architecture` 测试校验，避免 README 中的静态计数替代源码事实：

```
runtime 架构域全景

  Conversation  ─── turn、收件箱、会话热运行与事件
  Provider      ─── 模型传输、注册、策略与连接池
  Tooling       ─── 工具计划、调度、执行、策略与记忆
  Mission       ─── 任务、任务控制、证据、调度与命令路由
  Session       ─── 会话执行、输入、生命周期与关系图
  Agent / Team  ─── Agent 生命周期、能力、协作与团队运行
  Steward       ─── 托管 Agent 与调度
  Approval      ─── 审批与门控
  Context       ─── 预算、证据、知识、资源与上下文组装
  Recovery      ─── 事件存储、回放与恢复配方
  Policy        ─── 权限、安全、信任、自治与跨面策略
  ExecutionCore ─── 执行图、监督、实时投影与编排
  RealityBridge ─── 结构化数据、事实提取、决策与回忆端口
  Evolution     ─── 可控演化信号与应用
  Configuration ─── 配置、校验与 Profile
  Infrastructure─── 能力、检查点、质量门、升级、MCP、Sandbox 与 Surface 合同
  Skill         ─── 技能激活、选择与记忆集成
```

---

## 8. 完整 API 表面

Gateway 通过 Axum Router 暴露受能力合同治理的 API；完整路由与能力清单以源码、运行时 `/api/gateway/capability-contract` 及其 OpenAPI 投影为准：

```
Gateway API (HTTP/SSE :8642)

Public
├── GET  /health, /healthz, /readyz         健康检查
├── GET  /api/webui/manifest                 WebUI 资源清单
└── POST /api/auth/*                         认证

Session & Message
├── GET  /api/sessions                       会话列表/创建/详情/删除/分支
├── POST /api/sessions/:id/messages          发送消息
├── GET  /api/sessions/:id/execution         Session 到最新 Runtime execution 的轻量索引
├── GET  /api/runtime/executions/:id         规范执行投影(graph/activity/evidence)
├── POST /api/sessions/:id/compact           触发压缩
└── POST /api/sessions/:id/replay            重放会话

Runtime & Control Plane
├── POST/PATCH /api/runtime/live-subscriptions  管理 Surface 多源实时订阅
├── GET  /api/runtime/live/:id               单物理连接 multiplex SSE
├── GET  /api/runtime/timeline               timeline
├── GET  /api/runtime/control-plane          控制面摘要
├── POST /api/runtime/turns                  提交 turn / 取消
└── GET  /api/runtime/events                 运行时事件

Memory & Reality Core
├── GET  /api/memory/status/search/packet    记忆状态/搜索/上下文包
├── GET  /api/memory/entities/triples        实体/三元组
├── POST /api/memory/facts/check             fact check
├── GET  /api/matrix/entities                实体 CRUD
├── POST /api/matrix/facts/ingest            事实注入
├── GET  /api/matrix/metrics                 指标系统
├── GET  /api/reality/*                      现实引擎状态/流/证据/治理

Tools & Skills
├── GET  /api/tools                          工具注册表
├── POST /api/tools/execute                  执行工具
├── POST /api/tools/mutations/preview        变更预览
├── GET  /api/skills/catalog                 技能全集
├── GET  /api/skills/projection              按 surface 投影

Agents & Mission Control
├── GET  /api/agents                         代理目录/团队配置
├── GET  /api/mission                        任务控制投影
├── POST /api/mission/command                任务控制命令
└── GET  /api/tasks                          任务生命周期

Surface & Cross-Plane
├── GET  /api/surfaces                       已发现 surface 列表
├── GET  /api/surfaces/:id/inbox/outbox      可靠消息账本
├── POST /api/surfaces/:id/inbox/:msg/replay 重放入站消息
├── POST /api/surfaces/:id/outbox/:d/retry   重试投递
├── GET  /api/cross-plane                     跨面治理
└── POST /api/cross-plane/action/execute      跨面行动执行

Apps & Eval & Edge
├── GET  /api/apps                           已启用 App catalog
├── /api/apps/:id/*                          已注册 App 的受治理路由前缀
├── GET  /api/apps/mfg/app                   MFG 参考 App 描述
├── POST /api/apps/mfg/incidents              MFG 创建事件
├── GET  /api/harness-eval/reports            评测报告
├── POST /api/harness-eval/runs               触发评测跑
└── GET  /api/edges                           边缘注册表

Workspace & Profiles
├── GET  /api/workspace                       工作区文件管理
├── POST /api/upload                          文件上传
└── GET  /api/profiles                        配置文件管理
```

---

## 9. 工作区 Crate 依赖图

```
                         ┌──────────────────────┐
                         │         cli           │ (极薄入口)
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
              ┌──────────┐  ┌──────────┐   ┌──────────┐
              │ gateway  │  │   tui    │   │ surface  │
              │ (聚合所有) │  │(零crate依赖)│ │(零crate依赖)│
              └────┬─────┘  └──────────┘   └──────────┘
                   │
    ┌──────────────┼──────────────────────────────────────┐
    │              ▼                                      │
    │       ┌──────────┐                                  │
    │       │ runtime  │ ← harness-contract               │
    │       │ (执行核心)│ ← model-protocol · provider      │
    │       └────┬─────┘ ← memory · storage · approval     │
    │            │        ← plugins                       │
    │   ┌────────┼────────┐                               │
    │   ▼        ▼        ▼                               │
    │ tools   harness  harness-eval                       │
    │  ↑       -eval      ↑                              │
    │  │                   │                              │
    │ mcp · plugins   runtime · memory · tools            │
    │                                                     │
    ├─────────────────────────────────────────────────────┤
    │ Fact 层:                                            │
    │   fact-kernel (零dep)                               │
    │     ├── memory ── storage                           │
    │     └── matrix-core ── matrix-repository ── storage │
    │           └── app-mfg ── storage                    │
    ├─────────────────────────────────────────────────────┤
    │ 零依赖叶子 Crate:                                    │
    │   harness-contract · fact-kernel · model-protocol   │
    │   surface · session · mcp · plugins · storage        │
    └─────────────────────────────────────────────────────┘
```

**关键边界约束** (架构测试强制执行):
- `tui` 不依赖任何 workspace crate → 只通过 Gateway HTTP/SSE
- `runtime` 不依赖 `surface` / `connector` / `tui` / `gateway`
- `tools` 不依赖 `runtime` / `provider`
- 非 TUI surface 全部迁入 `cowd-edge`，不进 core workspace

---

## 10. 可靠消息状态机

外部渠道消息的完整生命周期，由 `SurfaceHost` 持有：

```
Inbound 消息流转                           Outbound 投递流转

received ──→ processing ──→ processed     queued ──→ sending ──→ sent ✅
                │               │            │          │
                │               ▼            │          ▼
                │         replying ──────────┤    retry_scheduled
                │            │               │          │
                │            ▼               │    ┌─────┘
                │         replied ✅         │    ▼
                │                            │  dead_letter ⚠️
                ▼                            │
           failure_notifying ────────────────┘
                │
                ▼
           failed_notified / failed

SurfaceMessageSnapshot = active_inbox + terminal_inbox + active_outbox + dead_letters
```

**飞书 typing 反应**: sidecar 在消息处理中显示 `Typing` reaction；Gateway 在完成/失败/空回复时通过 `message.processing_complete` / `message.processing_failed` 通知清理。

---

## 11. 多 Agent 协作流程

```
用户请求 (自然语言 / Mission Control 命令)
       │
       ▼
┌──────────────────┐
│  intent_planner  │  意图分类 → TaskIntent
│  (运行时策略匹配) │  solo / team / steward / hybrid
└────────┬─────────┘
         │
    ┌────┴────────────────────────┐
    ▼                             ▼
┌──────────────┐          ┌──────────────────┐
│ solo 执行     │          │ team 执行         │
│ (标准 turn)   │          │                   │
│              │          │ TeamTemplate      │
│ conversation │          │   → role specs    │
│   → tools    │          │   → dependency    │
│   → compact  │          │   → budget        │
│   → reply    │          │                   │
└──────────────┘          │ TeamExecution     │
                          │   → AgentTask DAG │
                          │   → resource gate │
                          └────────┬──────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │  Agent A     │ │  Agent B     │ │  Agent C     │
            │ AgentRuntime │ │ AgentRuntime │ │ AgentRuntime │
            │  → turn      │ │  → turn      │ │  → turn      │
            │  → tools     │ │  → tools     │ │  → tools     │
            │  → evidence  │ │  → evidence  │ │  → evidence  │
            └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
                   │               │               │
                   └───────────────┼───────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │  JointProblemSolving        │
                    │  → solution evaluation      │
                    │  → synthesis                │
                    │  → review gate              │
                    └──────────────┬──────────────┘
                                   ▼
                          ┌──────────────┐
                          │  人类 / 审批  │
                          │  review_after │
                          │  _each_phase │
                          └──────────────┘
```

**Agent 事件同步**：`AgentRuntime` 先提交持久生命周期事实，再通过 `CowdEvent::AgentLifecycle + RelatedExecution` 投影到根 Session；Gateway 以相同身份生成实时流和历史回放，WebUI 以 Team/Agent 通道、Tool 依赖波次和语义执行图展示。执行 backend 不拥有第二套生命周期，也不能在持久提交前宣告终态。

---

## 12. 使用方式

```text
CLI / TUI / WebUI / Message Connector
              │
              ▼
         Cowd Gateway（RuntimeHost + SurfaceHost）
              │
      ┌───────┼───────────┐
      ▼       ▼           ▼
  Runtime  Memory/Matrix  App/Edge
```

```bash
# Gateway 服务管理
cowd gateway start
cowd gateway status
cowd gateway restart
cowd gateway stop

# TUI 联调（需要 full feature）
cargo run -p cli --bin cowd --features full

# WebUI 构建（cowd-edge）
cd ../cowd-edge
npm --prefix surfaces/webui run build

# Edge connector 构建
cargo build --release -p edge-adapters --bins
```

WebUI 构建产物通过 `gateway.webui_dir` 指向 `surfaces/webui/dist`；TUI 使用默认二进制会提示未构建。

## 13. Capability 与投影

```text
Capability Registry
  ├── WebUI 投影（最强管理面：表格/过滤/批量/证据/可视化）
  ├── TUI 投影（同能力集，终端密度与键盘优先：Clean/Panorama/Control Deck）
  ├── CLI 投影（轻控制/配置/诊断/启动）
  └── Surface manifest/status（外部渠道只做消息入口与投递）
```

## 14. 配置

```yaml
model: "deepseek-v4-pro" # 示例，实际由本地配置决定
permissions:
  default_mode: "danger-full-access"
approval:
  profile: "autonomous"
  low_risk_timeout: "auto_approve_once"
apps:
  mfg:
    enabled: true
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-edge/surfaces/webui/dist"
```

`apps.<id>.enabled` 是已编译 App 的唯一启动期开关；关闭会同步移除其路由、Skill、授权、工具与界面投影。

## 15. 验证

```bash
cargo fmt --all --check
cargo check
cargo check -p cli --bin cowd --features full
cargo test -p surface
cargo test -p tui
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
```

cowd-edge 验证：

```bash
cd ../cowd-edge
cargo fmt --all --check
cargo check --workspace --bins
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
```

依赖边界：`cargo tree -p cli --edges normal` 不应输出 TUI 渲染依赖；`cargo tree -p gateway --edges normal` 不应输出平台 SDK。Core/Edge/App 的发布与排障细节由各自仓库 README 与 docs/ 维护；最终事实源是当前源码、构建产物与运行时能力合同。

## 16. 系统说明书

- [系统说明书索引](docs/README.md)
- [架构文档](docs/architecture/README.md)
- [运维文档](docs/operator/README.md)
