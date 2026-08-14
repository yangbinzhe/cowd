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
| 分域详细边界、命令、配置与验证 | 第 12–21 章 |
| 启动、配置、排障、部署 | [系统说明书](docs/README.md) |
| App 的声明、装配与治理约定 | [架构文档](docs/architecture/README.md) |
| API 与能力合同 | [文档索引](docs/README.md) 与运行时能力合同 |
| TUI 使用与交互行为 | [架构文档](docs/architecture/README.md) |

文档结构：第 1–11 章是系统总览、图示与快速索引；第 12 章起是分域详细边界。上下两部分是“总览 → 细节”的互补关系，不重复承担同一内容。

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

| 档位 | 权限 | 审批 | 沙箱 | 最大并行 | 最大轮次 | 最大 token | 成本上限 |
|---|---|---|---|---|---|---|---|
| cautious | read-only | supervised | read-only sandbox | 1 | 3 | 8k | 25 cents |
| supervised | workspace-write | balanced | workspace-write sandbox | 2 | 10 | 32k | 150 cents |
| stewarded | workspace-write | autonomous | workspace-write sandbox | 3 | 24 | 64k | 500 cents |
| autonomous | danger-full-access | autonomous | host full access | 4 | 30 | 96k | 750 cents |
| yolo | danger-full-access | trust-all | host full access | 4 | 40 | 128k | 1000 cents |

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

#### 进化记忆（Evolution Memory）

```text
运行证据 / 用户输入 / 知识导入
        │
        ▼
记忆治理：确定性规则优先 ── 未决低风险候选才交给模型
        │  （来源、层级、优先级、scope、ID、动作、置信度硬校验）
        ▼
  ┌───────────────┬────────────────┐
  ▼               ▼                ▼
 L0..L4 分层记忆   Matrix 结构化事实  Evolution Governance
 （身份→共享）     （实体/关系/证据）   候选 → 校验 → 提升 → 审计
        │               │
        └───────┬───────┘
                ▼
          上下文召回 / 事实证据
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
动态预算 / 容量预检
        │
        ▼
证据计划 ─► 记忆召回(L0..L4) ─► 知识激活 ─► 语义检查点压缩
        │
        ▼
  上下文组装（system + 证据 + 历史 + 工具 schema）
        │
        ▼
   Provider 请求（预算/前缀缓存/连接池准入）
```

#### 多 Agent 协作系统

```text
runtime_capabilities / runtime_orchestrate
        │
        ▼
   Team 模板 ─► focus 分区（资源受控）
        │
        ├── researcher ×N（并行执行，作用域隔离）
        ├── team_board（有界结论/冲突交换）
        ├── synthesizer（上游只读综合）
        └── verify（结果契约校验）
        │
        ▼
   terminal_synthesis ─► 最终答复
```

#### 权限 / 审批 / 沙箱

```text
一个档位同时推导四个维度
  PermissionMode + SandboxPosture + ApprovalProfile + InterruptionPolicy
        │
        ▼
工具执行：Gateway 消费 Runtime 派生的 sandbox_posture（单一权威）
        │
        ├── 普通工具审批：TrustAll 自动批准 + 审计
        ├── 图审批：autonomous/yolo 自动批准 + 审计成对
        └── bash 审计：posture/enabled/network/kernel_hardening/fallback
```

#### 存储与事件账本（内存优先终态）

```text
turn 内以内存账本运行（图/工具效果/证据/策略）
        │
        ▼
终态一次性批量落库（事件 + terminal outbox + 用户回执 同事务）
        │
        ▼
崩溃恢复只从用户输入回执重建
```

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

## 12. 总体设计

> 总览与运行链路见第 2–5 章；本章只补充设计原则、App 平台、存储与缓存边界。

### 12.1 第一原则

- Runtime 不持有 channel，也不链接任何平台 SDK。
- Gateway 是唯一后端服务入口，负责 Edge 发现、静态资源转发、callback、health、events 和 managed sidecar 生命周期。
- TUI 和 WebUI 都只通过 Gateway HTTP/SSE API 使用核心能力。
- CLI 不做交互 UI，不承载业务执行器，只负责轻量命令、配置、诊断和 Gateway 启动。
- 默认开发/debug 构建不带 TUI，TUI 与 Gateway 分开开发。
- 只有 TUI 联调、完整产品验证和正式 release 才构建 `--features full`。
- 非 TUI surface 不在 core workspace 编译，全部从 `cowd-edge` 按需独立构建和交付。
- Memory 处理非结构化记忆和经验关联，Matrix 处理结构化事实、实体、关系和证据；两域不做隐式互写，结构化事实只经显式 source/growth 投影进入 Matrix。
- App 是应用层，不是 AI Harness 内核；Cowd 可以容纳多个受治理的业务 App。

### 12.2 通用 App 平台：编译期组成，启动期启用

Cowd 的 App 模型分为两个不可混淆的控制面：构建期决定某个受审核 App 是否进入产品二进制与 WebUI 静态资源；启动期的 `apps.<id>.enabled` 决定已编入 App 是否注册路由、技能、授权、AI tools 和界面入口。

```text
apps/catalog.toml + 审核 App source lock（Git + 固定 commit）
                │
                ▼
Cargo / WebUI 构建 ──> 静态 product-apps ──> apps.<id>.enabled
                                                   │
                                                   ▼
Gateway AppRegistry ──> API / Skill / Auth / OpenAPI / AI tools / TUI / WebUI
```

- MFG 不是唯一应用；未来工程、产品交付或其他领域 App 都遵守同一套 ID、来源锁定、产品组成和统一能力投影约定。
- Cowd 运行期绝不从配置、Git 地址或环境变量拉取、编译或执行未知 App 源码。
- TUI 与 WebUI 只消费 Gateway 的 App catalog/manifest，不各自维护启停状态。
- 当前已实现多 App 的显式 catalog/source lock、可选 Cargo feature 产品矩阵与统一运行时启停；MFG 是第一个真实参考 App。
- App catalog 变更会进入 Auth Broker 的通用授权目录。授权目录按当前已编译 catalog 重算，未知历史档位回落到当前最小权限；迁移后只运行最新授权状态，不保留旧授权执行路径。

完整规范见 [架构文档](docs/architecture/README.md)。
Gateway 的安全启动、二进制替换和运行核验见 [运维文档](docs/operator/README.md)。

### 12.3 全域存储选择与可证明切换

Gateway 在启动时只创建一个 `SelectedStorageTopology`：默认 `auto` 优先 PostgreSQL，
SQLite 是本地回退；选择 PostgreSQL 时，Session、Memory、Knowledge、Runtime Event、Task、Fact/Growth、Matrix、
Approval、Surface Message、Connector Directory 与启用 App 全部消费同一个有界连接池上的
已选择 port。业务 service、Runtime turn 和 App 不得再自行打开业务 SQLite。

默认 `storage.backend=auto`：优先 PostgreSQL，冷启动探测不可用或未配置时自动使用 SQLite
并写入 `~/.cowd/storage/fallback.json`、健康状态标记 `storage.fallback_active`；回退只发生
在进程冷启动，运行中禁止热切换与双写。`postgres` 模式保持失败即退出的强约束，`sqlite`
模式保留纯本地运行能力；回退后需在 Gateway 停止时执行 `cowd storage adopt-postgres`
显式重新接管 PostgreSQL。

PostgreSQL 不做隐式迁移、双写或失败回退。运维人员在 Gateway 停止时依次执行
`cowd storage plan|migrate|verify|cutover`；离线迁移阶段要求逐域 canonical digest、目标身份、
Cowd 版本、工作区、App source lock 与 enabled App 集合全部匹配。active manifest 是首次切换
的历史审计证据，不是后续二进制的启动许可证；后续版本在 Gateway 停止时执行
`cowd storage upgrade`，由当前 PostgreSQL adapter 和已启用 App 幂等升级 schema，不会从已过期
的 SQLite 重新覆盖 PostgreSQL。正常启动不运行 DDL、不检查历史 manifest，而是汇总当前二进制
注册的 migration catalog，并按 namespace 一次性只读校验；缺失或不匹配时拒绝启动并要求离线
upgrade。配置解析失败也会直接拒绝启动，不会因空配置默认值静默退回 SQLite。
健康快照会分别报告 migration transaction、readiness query 和 `search_path` 切换次数，避免把
Session 等领域事务误判为启动迁移。离线 upgrade 始终逐项核对迁移账本并执行缺失的幂等迁移，
不会仅因 catalog 摘要命中而掩盖旧 schema。
凭据只通过配置中的 `secretRef` 在进程边界解析，URL 不进入 projection、health 或证据文件。
本机长期运行推荐 `file:postgres-primary`，从权限不宽于 `0600` 的
`~/.cowd/secrets/postgres-primary` 读取；容器和托管服务可使用
`env:COWD_POSTGRES_URL`。因此 Gateway 重启不依赖临时终端环境，同时配置文件仍不保存 URL。
App 的表、快照和迁移由 App 自己拥有；Cowd 只提供通用 lease、独立 PostgreSQL schema、
migration hook 和全局 evidence envelope。
同步 PostgreSQL 驱动通过运行时安全连接包装进入 Tokio，生产 service 直接从所选拓扑组装，
不会先打开 SQLite baseline 再覆盖；App readiness 统一来自 `AppRegistry`，不把 MFG 或任何
未来 App 硬编码为 core service。

配置、迁移命令、失败边界和 App 存储所有权详见
[架构文档](docs/architecture/README.md)。
使用 PostgreSQL 的本机 Release 统一通过
`scripts/release/deploy-postgres-to-ai.sh` 部署，固定执行停服、原子安装、schema upgrade、
启动和 doctor 门禁，避免版本升级后遗漏 catalog 更新。

### 12.4 运行时性能与缓存边界

活动 Session、执行图、输入队列和运行状态以内存投影作为读取快路径，持久事件账本负责恢复；
关键输入、审批、副作用与终态在成功确认前仍必须持久提交。Provider 使用进程、账户、模型和
token 压力四级准入，PostgreSQL 使用 `critical`、`online_read`、`background` 三个隔离连接池。
Skill 只常驻轻量目录，选中的完整 `SKILL.md` 按需进入有界字节 LRU；工具结果缓存仅覆盖明确的
幂等读取。缓存不会成为第二套业务真相，也不会让写工具或审批绕过真实执行。

记忆治理在分页扫描后先执行确定性规则；nightly 或手动全盘治理才把仍未决的低风险派生候选
交给当前配置模型。模型不获得工具、Session 或 Memory 写权限，返回结果还必须经过来源、层级、
优先级、scope、ID、动作和置信度硬校验。用户明确输入、导入知识、关键记忆与 L0/L4 不进入
语义自治；模型失败时候选保持开放，治理报告保留模型、token、理由和 lifecycle 证据。

配置、生命周期、容量与验证边界详见
[架构文档](docs/architecture/README.md)。

---

## 13. 仓库与 Workspace 布局

> 分层与运行链路见第 2、4 章；`crates/runtime` 内部能力域以第 7 章模块归属合同为唯一权威，本章只给仓库级落位，不重复模块树。

| 层 | 仓库/crate | 职责 |
|---|---|---|
| Entry | `crates/cli` | 极薄命令入口；默认构建不带 TUI，不依赖 runtime/memory。 |
| Entry | `crates/gateway` | 后台服务入口：HTTP/SSE API、RuntimeHost、SurfaceHost、服务编排。 |
| Entry | `crates/tui` | core 仓内唯一终端 surface，仅 `--features full` 构建。 |
| Entry | `crates/surface` | Surface manifest、managed/OneShot 传输、静态资源、callback、health 合同。 |
| AI Harness | `crates/harness-contract` · `harness-eval` | 核心语义与评测边界。 |
| AI Harness | `crates/runtime` | 会话、上下文、任务/Mission、工具/MCP/provider 调度、执行图与恢复。 |
| AI Harness | `crates/session` · `model-protocol` · `provider` · `mcp` · `plugins` | 会话合同、模型协议、provider 适配、MCP、插件。 |
| Fact | `crates/fact-kernel` · `memory` · `matrix/core` · `matrix/repository` | 事实语义、非结构化记忆、结构化事实与持久化。 |
| Tool/Skill/Connector | `crates/tools` · `skill/service` · `connector` · `surface::channel` | 工具、技能、外部资源与平台通道合同（不含 SDK 实现）。 |
| Application | `app-sdk` · `app-host` · `product-apps` · 外部 `cowd-app-<id>-bundle` | 受治理 App 的注册、投影与产品组合。 |
| Storage | `crates/storage` | SQLite/PostgreSQL 后端执行器、连接池与迁移合同。 |

cowd-edge 独立仓库承载 WebUI 与外部渠道 sidecar：

```text
cowd-edge
  surfaces/webui                  WebUI 静态 surface
  connectors/message/*            飞书/邮件/企微/微信 iLink 消息 connector
  connectors/source/*             Bitable/Lark 数据源 connector
  crates/edge-contract             Edge 协议镜像
  crates/edge-adapters             平台适配实现和 sidecar 二进制
```

WebUI 与全部 connector 不进入 core workspace。Gateway 通过 `surface.json` 发现它们；
managed Edge 使用私有 UDS 上的 authenticated HTTP/2，stdio JSONL 只保留给 OneShot 单元。
Memory/Matrix 分工与 App 所有权边界见第 6.2 节与第 12.2 节，不在此重复。

---

## 14. Gateway 与 Surface

> 外部消息的可靠状态机见第 10 章；本章展开 Gateway 职责、Surface 协议和 WebUI/TUI 边界。

### 14.1 Gateway 职责

Gateway 是所有 UI 和外部 surface 使用 core 能力的后端服务入口。

它负责：

- 启动 RuntimeHost。
- 组装 GatewayServices。
- 暴露 HTTP/SSE API。
- 发现 surface manifest。
- 托管 WebUI 静态资源。
- 转发 surface callback/webhook/OAuth redirect。
- 管理 managed Edge 生命周期、认证传输和 OneShot 单元。
- 收集 surface health/events。
- 将外部渠道的 ingress/egress 接入 Gateway 服务边界。

Gateway 不负责：

- 渲染 TUI/WebUI。
- 链接飞书、邮件、企微、微信等平台 SDK。
- 直接执行 AI turn 的内部细节。
- 作为第二套 runtime 或第二套会话状态。

### 14.2 Surface 协议

Surface 通过 `surface.json` 描述自己：

```json
{
  "schema": "cowd.surface.v1",
  "id": "feishu",
  "name": "Feishu Message Connector",
  "kind": "message-connector",
  "runtime": {
    "kind": "managed",
    "artifact": "cowd-edge-open-platform-message",
    "driver_profile": "feishu-message",
    "transport": "uds-http2"
  },
  "capabilities": ["message.ingress", "message.egress", "message.callback", "health"],
  "routes": [
    { "kind": "callback", "path": "/events", "method": "POST", "public": true }
  ],
  "resources": [],
  "health": { "mode": "http2", "interval_ms": 30000 },
  "default_enabled": false
}
```

Gateway 按 manifest 提供：

| API | 用途 |
|---|---|
| `GET /api/surfaces` | 已发现 surface 列表 |
| `GET /api/surfaces/:id/health` | 单个 surface health |
| `GET /api/surfaces/:id/events` | managed sidecar event buffer |
| `GET /api/surfaces/:id/routes` | surface route 摘要 |
| `GET /api/surfaces/:id/resources` | surface static resource 摘要 |
| `GET /api/surfaces/:id/inbox` | 持久 inbound 消息账本 |
| `GET /api/surfaces/:id/outbox` | 持久 outbound 投递账本 |
| `GET /api/surfaces/:id/deliveries` | surface delivery event 账本 |
| `POST /api/surfaces/:id/inbox/:message_id/replay` | 重放 inbound 消息，复用 ingress/runtime 链路 |
| `POST /api/surfaces/:id/outbox/:delivery_id/retry` | 重试失败或待重试 outbound delivery |
| `POST /api/surfaces/:id/outbox/:delivery_id/dead-letter` | 将 outbound delivery 移入 DLQ |
| `GET /s/:surface/*path` | surface 静态资源转发 |
| `GET|POST /surface-callback/:surface/*path` | callback/webhook 转发 |

Surface 可靠消息层由 Gateway `SurfaceHost` 持有：inbound 先写持久 inbox 再进入 runtime，outbound 先写 outbox 再投递 sidecar，失败进入 `retry_scheduled` 或 `dead_letter`，重试带 `max_attempts` 与 backoff。完整状态机与快照语义见第 10 章，不在此重复；Runtime 不持有 surface/channel SDK。

飞书 surface 使用 WebSocket 接收消息。收到用户消息后 sidecar 会在原消息上设置 `Typing` reaction 表示处理中；Gateway 在 runtime 完成、回复成功、空回复或失败时都会通过 `message.processing_complete` / `message.processing_failed` action 通知 sidecar 清理或替换该 reaction。Feishu reply 发送路径也会在成功/失败时兜底清理，避免已经回复的消息仍残留"工作中"状态。

外部 surface 的 runtime turn 不再只有一个硬超时。Gateway 会根据消息内容选择 `SurfaceQuickReply` 或 `DeepInvestigation` 策略，并给每个策略同时设置总耗时和最大模型/工具迭代轮次。README、文档核查、代码检查、调研、测试、重构等消息会进入深度策略；普通短消息走快速策略。若 runtime 超时、超过迭代预算或执行失败，Gateway 不会只把 inbox 标成 `failed` 后沉默，而会通过同一套可靠 outbox 投递一条用户可见的失败通知，并把 inbox 推进到 `failed_notified`。这样 Feishu、未来邮件/企微/微信等 surface 都能避免"消息已处理失败但用户端没有任何回复"的黑洞。

### 14.3 WebUI

WebUI 不在 core 仓库。它位于：

```text
cowd-edge/surfaces/webui
```

Gateway 通过配置读取 WebUI 构建产物：

```yaml
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-edge/surfaces/webui/dist"
```

如果未配置 `gateway.webui_dir`，或者目录没有 `index.html`，Gateway 仍应健康启动，并在根路由返回 health/status，而不是失败退出。

WebUI 作为静态 surface 也可以通过 `surface.json` 被 Gateway 发现。构建产物必须包含 `dist/index.html`，这样根路由和 `/s/webui/*` 的 SPA fallback 都能工作；`dist/index.dev.html` 只是开发入口，不应作为 Gateway fallback 的唯一入口。

WebUI 是否展示业务 App 不由前端本地配置决定。它在挂载前读取 Gateway 的 `/api/webui/manifest`，并据其中的已启用 App 清单注册对应页面、导航和 capability 请求；后端禁用 App 时，即使静态资源仍包含其代码，也不会暴露入口。

### 14.4 TUI

TUI 是 core 仓内唯一 UI surface，但默认 debug 不编译。这样日常开发可以让 Gateway 和 TUI 分开演进，避免所有开发者都为终端渲染依赖付出编译成本。

```bash
# 默认开发，不带 TUI
cargo check
cargo build -p cli --bin cowd

# TUI 联调 / full 构建
cargo check -p cli --bin cowd --features full
cargo build -p cli --bin cowd --features full
```

TUI 的定位不是 WebUI 的终端复刻版，而是终端环境中的 `Terminal Control Surface`：

- 默认以键盘优先、正文优先、低噪声方式操控后端服务。
- 通过 Gateway HTTP/SSE attach session、订阅事件、发送消息、执行 cancel 和刷新投影。
- 支持 `Clean / Panorama` 两种显示语义：Clean 只保留正文和关键计数，Panorama 展开运行线索和证据。
- 顶部状态条展示 Gateway/session、turn 状态、display mode、context/memory/reality evidence 摘要。
- `Control Deck` 聚合 Gateway、Runtime readiness、session、lease、task、approval、surface、Reality Core、Fact Flow 和 degraded signal。
- TUI 不直接读取 runtime/channel/provider/store，不越过 Gateway 查内部表。
- Surface 面板提供轻量可靠消息操作：`i/o/v` 查看 inbox/outbox/delivery events，`p/d/D` 执行 replay/retry/DLQ。
- TUI 建立 Gateway 会话前读取 `/api/apps`，只挂载 Gateway 实际注册的已编译 App contribution；catalog 读取失败时按 fail-closed 隐藏 App 面板。

### 14.5 Harness Eval 服务化

`crates/harness-eval` 不再只是离线 CLI 报告。报告 DTO、store 和 runner 已成为 library API，Gateway 通过 `/api/harness-eval/*` 暴露评测报告、场景矩阵和 smoke run：

| API | 用途 |
|---|---|
| `GET /api/harness-eval/reports` | 历史评测报告列表 |
| `GET /api/harness-eval/reports/latest` | 最新评测健康摘要 |
| `GET /api/harness-eval/reports/:id` | 单份评测报告详情 |
| `GET /api/harness-eval/scenarios` | stable AI 场景矩阵 |
| `GET /api/harness-eval/runs` | 评测 run 历史 |
| `POST /api/harness-eval/runs` | 触发 quick/full deterministic smoke run |

默认 Gateway/WebUI/TUI 只触发无真实 provider token 消耗的 deterministic smoke。deep/real model 路径必须显式授权，防止评测面板误耗 token。

常用终端快捷键：

| 快捷键 | 用途 |
|---|---|
| `Alt+V` | 切换 Clean / Panorama |
| `Alt+E` | 打开 runtime/evidence panorama 面板 |
| `Alt+G` | 打开 Gateway Control Deck |
| `Ctrl+P` | 打开命令面板 |
| `Esc Esc` 或 `Ctrl+C Ctrl+C` | 当前 turn 中取消，空闲时退出 |

---

## 15. 使用方式

```text
CLI / TUI / WebUI / Message Connector
              │
              ▼
         Cowd Gateway
        ┌─────────────┐
        │ RuntimeHost │
        │ SurfaceHost │
        └──────┬──────┘
               │
   ┌───────────┼────────────────┐
   ▼           ▼                ▼
Runtime    Memory/Matrix    App/Edge
```

### 15.1 默认开发

默认开发路径只验证 core、不编译 TUI；完整命令见第 18 章“验证”，此处不重复。开发常用入口：

当前 App catalog 与受锁定来源可在完整产品构建前校验：

```bash
cargo run -p xtask -- apps verify --locked
```

`gateway`、`tui` 与 `cli` 通过相同的 `app-<id>` feature 选择 `cowd-product-apps` 中的静态 bundle。正常产品默认包含 MFG；`cargo build -p cli --no-default-features` 生成不含 MFG 的 core-only 产品，`cargo build -p cli --features full` 显式构建 TUI 与当前审核 App。YAML 配置仅控制已编译 App 的启停，不能替代 catalog/source lock 审核。

### 15.2 Gateway

Gateway 是后台服务入口：

```bash
# Gateway 服务管理
cowd gateway start
cowd gateway status
cowd gateway restart
cowd gateway stop
```

Gateway 启动后，TUI、WebUI 和外部 surface 都通过 Gateway API 使用核心能力。`restart` 只回收同一可执行文件启动的 `gateway run` 进程；二进制覆盖安装后也会通过启动路径识别旧进程，等待其退出后再拉起新实例，避免两个 Gateway 同时占用同一会话库。

### 15.3 TUI 联调

TUI 联调需要 full feature：

```bash
cargo run -p cli --bin cowd --features full
cargo run -p cli --bin cowd --features full -- tui
```

如果使用不带 TUI 的默认二进制请求 TUI，CLI 会明确提示该二进制未构建 TUI surface。

### 15.4 WebUI

WebUI 在 `cowd-edge` 构建：

```bash
cd ../cowd-edge
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
```

然后通过 `gateway.webui_dir` 指向 `surfaces/webui/dist`。

### 15.5 Cowd Edge

外部 surface 与 connector 在 Cowd Edge 仓库构建：

```bash
cd ../cowd-edge
cargo check --workspace --bins
cargo build --release -p edge-adapters --bins
```

每个 UI surface、message connector 与 source connector 都通过 `surface.json` 暴露能力，不进入 core 依赖图。

---

## 16. Capability 与投影

Cowd 通过 capability registry 描述核心能力，并按 surface 投影。

```text
Capability Registry
  -> WebUI projection
  -> TUI projection
  -> CLI projection
  -> Surface manifest/status
```

设计要求：

- WebUI 是最强管理面，适合复杂表格、过滤、批量操作、治理证据和可视化。
- TUI 保持同一核心能力集，但以终端密度和键盘操作为优先；它用 Clean/Panorama、证据摘要和 Control Deck 实现对后端服务的快速掌控。
- CLI 只保留轻控制、配置、状态、诊断和启动，不做复杂业务管理。
- 外部渠道 surface 只负责消息入口、消息投递、callback、长连接和静态资源，不成为 Runtime 的一部分。

---

## 17. 配置

常见配置片段：

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

模型/API 密钥属于配置和 secrets，不应成为顶层 auth 模块。Gateway 的 WebUI 静态资源配置是可选项，缺失时服务仍应可用。

`apps.<id>.enabled` 是已编译、已审核 App 的唯一启动期开关：修改后重启 Gateway。关闭某个 App 会从 App catalog、路由、Skill、Auth capability、OpenAPI、AI tools、TUI 与 WebUI 投影中同步移除它；该配置不会在运行期拉取源码，也不会改变二进制中已编入的代码。新增通用 App 的来源锁定、开发/发行模式和后续 Cargo feature 规约见 [架构文档](docs/architecture/README.md)。服务更新、认证状态迁移和单实例核验按 [运维文档](docs/operator/README.md) 执行；不要手工编辑 `credential-state.json` 或复制认证凭据。

---

## 18. 验证

core 发布前验证：

```bash
cargo fmt --all --check
cargo check
cargo check -p cli --bin cowd --features full
cargo test -p surface
cargo test -p tui
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
scripts/scenarios/tui-daemon-attach.sh
```

surface 仓库验证：

```bash
cd ../cowd-edge
cargo fmt --all --check
cargo check --workspace --bins
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
```

依赖边界验证重点：

```bash
cargo tree -p cli --edges normal | rg 'tui|ratatui|crossterm|syntect|tui-textarea'
cargo tree -p gateway --edges normal | rg 'edge-adapters|lettre|imap|mail-parser'
```

默认情况下第一条不应输出 TUI 渲染依赖；第二条不应输出平台 SDK。

---

### 18.1 验证边界

- Core、Edge 与 App 的具体发布内容、构建方式、部署与排障步骤由各自仓库的 README 和 `docs/` 维护；能力、路由和接口的最终事实源始终是当前源码、构建产物与运行时能力合同，而不是文档中的静态数量。
- 文档聚焦“Core/Edge/App 分层、能力合同、统一状态投影”的当前终态，不描述历史版本，也不改变运行时代码、配置或对外行为。

## 19. 系统说明书

更完整的运行、配置、构建、部署、API 与故障排查内容见：

- [系统说明书索引](docs/README.md)
- [架构文档](docs/architecture/README.md)
- [运维文档](docs/operator/README.md)
