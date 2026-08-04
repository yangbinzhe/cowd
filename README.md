# Cowd — Rust 原生 AI Harness 内核

> 核心版本：`v0.9.636` | Rust 2021 Edition | MIT

📊 **[历史 v0.9.438 能力全景 Dashboard →](docs/capability-dashboard.html)** — 作为阶段快照保留；当前能力以本文与 `docs/` 活跃文档为准

---

## 🏗️ 架构全景

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
        │  │ RuntimeHost  │  │ SurfaceHost  │  │ GatewayServices (24)  │  │
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
    │   │         68 个公共模块 · 18 个架构域                  │      │
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
    │   │  │ 模式·ReWOO·DAG│ │结构化数据合同  │  68 modules │      │
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

### 分层架构

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

---

## 🔄 协作全景：一次 AI Turn 的完整链路

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

---

## ⚡ 核心特性矩阵

| 特性域 | 能力 | 成熟度 | 关键组件 |
|--------|------|--------|----------|
| **多模型路由** | OpenAI/Anthropic/DeepSeek/Qwen 自动适配 + Provider fallback 链 | ✅ 生产就绪 | `provider` · `model-protocol` · `ModelRouteDecision` |
| **会话管理** | 多 session 并行、切换、后台运行、暂停/关闭、检查点/恢复；持久化连接池并发访问 | ✅ 生产就绪（V564 已消除全局存储锁） | `session_execution` · `SessionExecutionPlane` · `UnifiedSessionStore` |
| **上下文工程** | 动态预算分配、硬容量预检、语义检查点压缩、记忆召回、知识激活、证据规划 | ✅ 生产就绪 | `context_runtime` · `budget_policy` · `compact` |
| **5 层记忆系统** | L0身份→L1核心→L2项目→L3深度→L4共享 + 有界压缩 + 向量/FTS 检索 | ✅ 生产就绪 | `memory` · `fact-kernel` · `CognitiveContextManager` |
| **结构化事实引擎** | 实体/关系/证据/Metrics/Ontology + 后端中立持久化 + 质量门控 | ✅ 生产就绪 | `matrix-core` · `matrix-repository` · `MatrixDataPlane` |
| **多 Agent 协作** | Team 模板编译 → AgentTask DAG → 资源受控并行 → WorkingState → synthesis/verify；Agent 生命周期实时/持久统一投影 | ✅ 核心闭环 | `orchestration` · `ExecutionGraphRunner` · `AgentRuntime` · `team` |
| **Agent 讨论** | 多 agent 讨论引擎、共识方法、联合问题求解管道 | 🔶 基础完成 | `agent_discussion` · `joint_problem_solving` |
| **托管执行(Steward)** | Autonomy profile 驱动、tick调度、决策账本、handoff | 🔶 基础完成 | `steward_runtime` · `steward_scheduler` |
| **可靠消息投递** | Inbox→Outbox→DLQ 完整状态机、重试/backoff、operator 修复入口 | ✅ 生产就绪 | `SurfaceHost` · `message_store` · `ledger` |
| **Surface 协议** | `surface.json` manifest、UDS/H2 managed 与 static/OneShot lifecycle | ✅ 生产就绪 | `surface` · `SurfaceManifest` · `surface.json` |
| **事件账本 & 恢复** | 覆盖 mission/session/team/agent/tool/recovery 的事件存储+回放 | ✅ 基础完成 | `runtime_event_store` · `recovery` · `recovery_recipes` |
| **跨面治理(Policy)** | 跨入口身份绑定、授权、风险审计、信任解析、自治预算 | ✅ 生产就绪 | `cross_plane_policy` · `trust_resolver` · `autonomy_profile` |
| **权限 & 审批** | PermissionMode + Runtime ApprovalCoordinator + 持久化 Request/Grant；低风险策略放行，高风险统一人工决策 | ✅ 生产就绪 | `permissions` · `approval_coordinator` · `approval_queue` · `RuntimeEventStore` |
| **工具系统** | 内置工具 + MCP 桥接 + Plugin 集成 + LSP + Checkpoint + Mutation Preview | ✅ 生产就绪 | `tools` · `tool_orchestrator` · `mcp_tool_bridge` |
| **技能目录** | 多 root 发现、安全扫描、维护评估、生成、路由、projection | ✅ 生产就绪 | `skill/service` · `SkillRegistry` · `SkillRouter` |
| **Harness Eval** | 场景矩阵、确定性 smoke、能力覆盖报告、Gateway 服务化 | ✅ 生产就绪 | `harness-eval` · `/api/harness-eval/*` |
| **TUI 控制面** | Clean/Panorama 双模式、Control Deck、键盘优先、SSE attach | ✅ 生产就绪 | `tui` · `GatewayTuiConfig` |
| **插件系统** | Builtin/Bundled/External 三级插件 + Pre/Post Hook | ✅ 生产就绪 | `plugins` · `PluginRegistry` · `HookRunner` |
| **通用 App 宿主** | 已编译 App 的统一注册、配置启停、路由/技能/授权/界面同步投影；MFG 为首个参考 App | ✅ V563 建立 catalog/source lock 与统一启停；V564 补齐授权目录状态迁移和默认服务收敛 | `app-sdk` · `app-host` · `product-apps` · `AppRegistry` · `auth-broker` |
| **沙箱执行** | Linux 容器检测、workspace-only/allow-list 隔离模式 | ✅ 基础完成 | `sandbox` · `sandbox_exec` |
| **执行模式** | Deliberation/ReWOO/Tool DAG/Reflexion 等执行策略 | 🔶 基础完成 | `execution_core` · `orchestration` · `strategy_matcher` |

---

## 模块归属合同 (Module Map)

`crates/runtime` 通过 `runtime::module_map` 形成代码级归属合同，68 个公共模块分属 18 个架构域，由 `runtime_module_architecture` 测试强制校验：

```
runtime 架构域全景

  Conversation  ─── 核心对话循环、提示组装、压缩、SSE、turn 监督       (8 模块)
  Provider      ─── LLM 客户端、注册表、连接池、用量/成本追踪           (4 模块)
  Tooling       ─── 文件操作、调度、缓存、执行计划、账本、记忆           (8 模块)
  Mission       ─── 任务运行时、全局控制、证据总线、AI 内核              (7 模块)
  Session       ─── 会话核心、执行平面、生命周期、关系图、检查点        (6 模块)
  Agent         ─── 生命周期、邮箱、事件总线、协作、讨论、工作图        (15 模块)
  Team          ─── 团队运行时、执行循环、发现、Cron 注册表             (4 模块)
  Steward       ─── 托管运行时、调度器、决策账本                         (3 模块)
  Approval      ─── 全局审批队列、门控                                   (2 模块)
  Context       ─── 上下文组装、分析器、扇出、预算、证据、知识          (8 模块)
  Recovery      ─── 事件存储、回放、执行器、配方、自我审计              (5 模块)
  Policy        ─── 权限、门控、策略引擎、信任、自治、跨面、绿色合约    (8 模块)
  ExecutionCore ─── 模式目录、ReWOO、工具 DAG、反思、编排、审议         (2+9 模块)
  RealityBridge ─── 结构化数据合约(实体/事实源映射)                      (1 模块)
  Skill         ─── 运行时技能激活、选择、记忆集成                       (1+3 模块)
  Infrastructure─── 配置、MCP、钩子、沙盒、波浪、远程、事件、插件       (41 模块)
  Persistence   ─── 异步存储端口(SQLite/PostgreSQL/Cache)                (1+2 模块)
  StructuredData─── 实体/事实源映射与对账                                 (1 模块)
```

---

## 完整 API 表面

Gateway 通过 Axum Router 暴露 **27 个路由组**，覆盖 200+ API 端点：

```
Gateway API (HTTP/SSE :8642)

Public
├── GET  /health, /healthz, /readyz         健康检查
├── GET  /api/webui/manifest                 WebUI 资源清单
└── POST /api/auth/*                         认证

Session & Message
├── GET  /api/sessions                       会话列表/创建/详情/删除/分支
├── POST /api/sessions/:id/messages          发送消息
├── GET  /api/sessions/:id/projection        运行投影(run graph/时间线/telemetry)
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

## 工作区 Crate 依赖图

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

## 可靠消息状态机

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

## 多 Agent 协作流程

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

Cowd 是 Rust 原生的 AI Harness 核心仓库。当前核心版本：`0.9.636`。

本仓库的目标不是实现一个单一聊天 CLI，而是建设一个可长期演进的 AI Harness 内核：统一承载模型调用、会话、上下文、记忆、事实、工具、技能、审批、任务推进、运行时治理和 surface 投影。CLI、TUI、WebUI、外部渠道都只是这个内核能力的不同入口和呈现方式。

非 TUI surface 已从 core 仓库迁出，统一进入独立仓库 `cowd-edge`。core 仓库只保留协议、Gateway 装载能力、AI Harness 核心能力，以及可选的 TUI surface。

---

## 1. 总体设计

### 1.1 核心定位

Cowd core 负责 AI Harness 的稳定内核，不负责把所有 UI 和平台 SDK 打进一个巨大二进制。

```text
用户入口
  CLI       极薄命令入口，负责配置、诊断、Gateway 启动等轻控制
  TUI       core 仓内唯一 UI surface，仅 full/release 联调时构建
  WebUI     cowd-edge 中的浏览器 surface
  Channel   cowd-edge 中的外部渠道 sidecar

Gateway
  HTTP/SSE API
  RuntimeHost
  SurfaceHost
  Surface static/callback/health/events

AI Harness core
  Conversation Runtime
  Mission Runtime
  Mission Control Runtime
  Session Execution Plane
  Team Execution Loop
  Agent Lifecycle / Mailbox / Event Bus
  Steward Runtime / Steward Scheduler
  Runtime Event Store / Recovery
  Context / Approval / Tools / Skills / MCP / Provider
  Memory / Matrix / Task / Eval / Telemetry

Fact/application layer
  fact-kernel
  memory
  matrix
  app bundles (MFG / future apps)
```

### 1.2 第一原则

- Runtime 不持有 channel，也不链接任何平台 SDK。
- Gateway 是唯一后端服务入口，负责 Edge 发现、静态资源转发、callback、health、events 和 managed sidecar 生命周期。
- TUI 和 WebUI 都只通过 Gateway HTTP/SSE API 使用核心能力。
- CLI 不做交互 UI，不承载业务执行器，只负责轻量命令、配置、诊断和 Gateway 启动。
- 默认开发/debug 构建不带 TUI，TUI 与 Gateway 分开开发。
- 只有 TUI 联调、完整产品验证和正式 release 才构建 `--features full`。
- 非 TUI surface 不在 core workspace 编译，全部从 `cowd-edge` 按需独立构建和交付。
- Memory 处理非结构化记忆和经验关联，Matrix 处理结构化事实、实体、关系和证据。
- App 是应用层，不是 AI Harness 内核；MFG 只是第一个参考 App，Cowd 可以容纳多个受治理的业务 App。

### 1.3 通用 App 平台：编译期组成，启动期启用

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
- App catalog 变更会进入 Auth Broker 的通用授权目录。V564 对历史 v2 状态提供一次性、凭据验证后的迁移：能力始终按当前已编译 catalog 重算，未知历史档位回落到当前最小权限；迁移后只运行 v3 状态，不保留历史授权执行路径。

完整规范见 [通用 App 开发与产品组合规范](docs/architecture/application-development-and-product-composition.md) 与 [当前 App 启停和构建说明](docs/architecture/app-activation-and-build.md)。
Gateway 的安全启动、二进制替换和运行核验见 [Gateway 生命周期运行手册](docs/operator/gateway-lifecycle.md)。

### 1.4 全域存储选择与可证明切换

Gateway 在启动时只创建一个 `SelectedStorageTopology`：SQLite 是默认本地后端；选择
PostgreSQL 时，Session、Memory、Knowledge、Runtime Event、Task、Fact/Growth、Matrix、
Approval、Surface Message、Connector Directory 与启用 App 全部消费同一个有界连接池上的
已选择 port。业务 service、Runtime turn 和 App 不得再自行打开业务 SQLite。

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
App 的表、快照和迁移由 App 自己拥有；Cowd 只提供通用 lease、独立 PostgreSQL schema、
migration hook 和全局 evidence envelope。
同步 PostgreSQL 驱动通过运行时安全连接包装进入 Tokio，生产 service 直接从所选拓扑组装，
不会先打开 SQLite baseline 再覆盖；App readiness 统一来自 `AppRegistry`，不把 MFG 或任何
未来 App 硬编码为 core service。

配置、迁移命令、失败边界和 App 存储所有权详见
[存储治理与 PostgreSQL cutover](docs/architecture/storage-governance.md)。

### 1.5 运行时性能与缓存边界

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
[Runtime 性能与缓存架构](docs/architecture/runtime-performance-and-cache.md)。

---

## 2. 仓库边界

### 2.1 core 仓库

```text
crates/cli        极薄 CLI 入口，默认 debug 不编译 TUI
crates/gateway    HTTP/SSE 服务入口，负责 RuntimeHost 与 SurfaceHost
crates/runtime    AI Harness 运行时核心，不依赖 channel/surface SDK
crates/surface    Edge 生命周期、传输与 manifest 合同（底层协议名仍为 cowd.surface.v1）
crates/tui        core 仓内唯一 UI surface，full 构建才进入 cowd
```

### 2.2 edge 仓库

```text
cowd-edge
  surfaces/webui                 WebUI 静态 surface
  connectors/message/feishu      飞书消息 connector
  connectors/message/email       邮件消息 connector
  connectors/message/wecom       企微消息 connector
  connectors/message/wechat-ilink 微信 iLink 消息 connector
  connectors/source/feishu-bitable 飞书多维表格数据源 connector
  connectors/source/lark-bitable   Lark Bitable 数据源 connector
  crates/edge-contract           Edge 协议镜像
  crates/edge-adapters           平台适配实现和 sidecar 二进制
```

WebUI、飞书、邮件、企微、微信 iLink 与数据源 connector 不再进入 core workspace。Gateway 通过
`surface.json` 发现它们；managed Edge 使用私有 UDS 上的 authenticated HTTP/2，stdio JSONL
只保留给一次一请求的 OneShot 单元。

---

## 3. Workspace 能力分层

### 3.1 Entry 层

| crate | 职责 |
|---|---|
| `crates/cli` | 极薄命令入口。默认构建不带 TUI，不依赖 runtime/memory。 |
| `crates/gateway` | 后台服务入口。承载 HTTP/SSE API、RuntimeHost、SurfaceHost 和服务编排。 |
| `crates/tui` | 终端 surface。只在 `--features full` 或显式选择时构建。 |
| `crates/surface` | Surface manifest、managed/OneShot 传输、静态资源、callback、health 合同。 |

### 3.2 AI Harness 层

| crate | 职责 |
|---|---|
| `crates/harness-contract` | AI Harness 语义入口，承载策略、目标、工作图等核心语义。 |
| `crates/harness-eval` | 评测和能力验证边界。 |
| `crates/runtime` | 会话运行、上下文组装、任务生命周期、工具/MCP/provider 调度、运行时控制。 |
| `crates/session` | session 合同和生命周期存储。 |
| `crates/model-protocol` | 模型协议、prompt cache、usage 合同。 |
| `crates/provider` | OpenAI/Anthropic/DeepSeek/Qwen 等模型 provider 适配。 |
| `crates/mcp` | MCP stdio / lifecycle 合同。 |
| `crates/plugins` | plugin manifest、registry 和生命周期。 |

#### Runtime 内部能力

`crates/runtime` 是当前 AI Harness 的真正执行核心。它不是 UI 层、不是 Gateway 层，也不是 channel 适配层。它现在承载的核心子域如下：

```text
runtime
  conversation                 单次 turn、模型调用、工具回调、上下文压缩
  provider_runtime_client      provider fallback、模型链、请求执行
  mission_runtime              mission session、命令队列、proxy、steward 入口
  mission_control              Mission Control 全局投影和控制命令
  session_execution            session 状态、跨 session 消息、后台/切换/关闭
  approval                     审批协调、作用域授权、恢复与运行事件
  orchestration                意图理解、策略选择、Team/Agent 图编译与校验
  execution_core               DAG、资源准入、并行 runner、恢复和证据提交
  team                         Team 定义、实例化、WorkingState、result reducer
  agent                        Agent 定义/Binding、AgentRuntime、backend 与评测
  cowd_event                   Session 根事件流、嵌套执行谱系和实时生命周期投影
  steward_runtime              autonomy profile 驱动的托管执行
  steward_scheduler            steward tick、ledger、schedule evidence
  runtime_event_store          mission/session/team/agent/tool/recovery 事件账本
  runtime_event_replay         事件回放和恢复前分析
  recovery                     failed/stale/recovery required 状态恢复执行器
  global_approval_queue        全局审批队列和投影
  cross_plane_policy           跨入口身份、授权、风险、审计和 dispatch receipt
  tool_*                       工具调度、工具账本、工具记忆、工具执行计划
  collaboration_template       多 agent 协作模板和匹配
  context_* / memory bridge    context packet、memory recall、skill memory
  module_map                   模块归属、生命周期 owner 与架构验收合同
```

当前实现已经把"多 session 管理、mission control、team 执行、agent 生命周期、托管 steward、审批、事件证据、恢复"这几条主链路放回 runtime，而不是散落在 tools、TUI 或 Gateway 中。`runtime::module_map` 进一步把 conversation、provider、tooling、mission、session、agent、team、steward、approval、context、recovery、policy、reality bridge 等核心域纳入代码级归属合同。

### 3.3 Fact 层

| crate | 职责 |
|---|---|
| `crates/fact-kernel` | 事实语义核心，连接 Memory 和 Matrix 的事实表达。 |
| `crates/memory` | 非结构化记忆、多层召回、上下文包、经验沉淀。 |
| `crates/matrix/core` | 结构化事实、实体、关系、证据、水位和图语义。 |
| `crates/matrix/repository` | Matrix 持久化仓储。 |

Memory 更偏知识、经验、语义关联和上下文召回。Matrix 更偏结构化事实、实体关系、证据链、可计算推理和应用数据基础。二者通过 `fact-kernel` 建立可互相促进但不混淆的边界。

### 3.4 Tool / Skill / Connector 层

| crate | 职责 |
|---|---|
| `crates/tools` | 内置工具、工具 schema、工具执行和工具治理。 |
| `crates/skill/service` | skill catalog、projection、run、governance 和 API 服务。 |
| `crates/connector` | 外部资源、账号、服务和资源状态。 |
| `crates/surface::channel` | Gateway 层使用的平台/channel 合同，不包含 SDK 实现。 |

渠道自身的聊天、收发消息、长连接、静态资源等属于 surface/sidecar；渠道附带的文档操作、平台高级能力未来应作为 skill/tool 安装，而不是塞回 Runtime 或 Gateway。

### 3.5 Application 层

| crate | 职责 |
|---|---|
| `crates/app-sdk` | App descriptor、受限宿主上下文与 App contribution 合同。 |
| `crates/app-host` | `AppRegistry` 与统一 HTTP/Skill/catalog 投影宿主。 |
| `crates/product-apps` | 由 `apps/catalog.toml` 生成的唯一 Cowd 产品组合入口；只聚合外部 App 的静态贡献。 |
| 外部 `cowd-app-<id>-bundle` | 由对应 App 仓库拥有的组成适配层；MFG 是首个真实 bundle。 |
| 后续 `app-<id>` | 遵守同一来源锁定、所有权、feature、配置启停与界面投影规约的领域 App。 |
| `crates/storage` | SQLite/PostgreSQL 后端执行器、连接池与迁移合同。 |
| `crates/model-protocol::telemetry` | provider/runtime 共享的事件和遥测基础类型。 |

App 的业务代码可在独立仓库中开发；Cowd 通过受审核的 catalog/source lock、外部 `app-<id>-bundle` 与 `AppRegistry` 将其纳入产品。MFG 不是 App 框架的例外，也不是其他业务 App 的模板外特权。

---

## 4. Gateway 与 Surface

### 4.1 Gateway 职责

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

### 4.2 Surface 协议

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

Surface 可靠消息层由 Gateway `SurfaceHost` 持有。inbound 先写持久 inbox 再进入 runtime，outbound 先写 outbox 再投递 sidecar；失败会进入 `retry_scheduled` 或 `dead_letter`，重试有 `max_attempts` 与 backoff，不依赖 sidecar 内部重试作为唯一可靠性来源。Runtime 仍不持有 surface/channel SDK。

可靠消息状态不是简单的"是否收到"。当前语义如下：

| 对象 | 状态 | 含义 |
|---|---|---|
| inbox | `received` | Gateway 已持久化 inbound，还未交给 runtime。 |
| inbox | `processing` | runtime turn 正在处理该消息。 |
| inbox | `processed` | runtime 已完成，但还没有进入外部回复终态；通常只作为极短暂中间态或无回复终态。 |
| inbox | `replying` | 已生成回复，正在投递 outbox。 |
| inbox | `replied` | outbound 已经成功投递，消息处理闭环完成。 |
| inbox | `failure_notifying` | runtime 已失败，Gateway 正在通过 outbox 投递可见失败通知。 |
| inbox | `failed_notified` | runtime 失败已通过 surface 通知用户，失败原因保留在 `last_error`。 |
| inbox | `reply_retry_scheduled` | 回复投递失败但仍有重试计划。 |
| inbox | `reply_failed` | 回复进入失败终态或 DLQ。 |
| inbox | `failed` | runtime 处理失败。 |
| outbox | `queued` / `sending` / `retry_scheduled` | 投递中或等待重试。 |
| outbox | `sent` | 已投递成功。 |
| outbox | `dead_letter` | 已进入死信队列，需要人工处理或重放。 |

`SurfaceMessageSnapshot` 会同时返回 `active_inbox`、`terminal_inbox`、`active_outbox` 和 `dead_letters`。WebUI/TUI 不应再用全部 inbox/outbox 数量代表"工作中"，而应读取 active 集合或按上述状态白名单降级计算。

飞书 surface 使用 WebSocket 接收消息。收到用户消息后 sidecar 会在原消息上设置 `Typing` reaction 表示处理中；Gateway 在 runtime 完成、回复成功、空回复或失败时都会通过 `message.processing_complete` / `message.processing_failed` action 通知 sidecar 清理或替换该 reaction。Feishu reply 发送路径也会在成功/失败时兜底清理，避免已经回复的消息仍残留"工作中"状态。

外部 surface 的 runtime turn 不再只有一个硬超时。Gateway 会根据消息内容选择 `SurfaceQuickReply` 或 `DeepInvestigation` 策略，并给每个策略同时设置总耗时和最大模型/工具迭代轮次。README、文档核查、代码检查、调研、测试、重构等消息会进入深度策略；普通短消息走快速策略。若 runtime 超时、超过迭代预算或执行失败，Gateway 不会只把 inbox 标成 `failed` 后沉默，而会通过同一套可靠 outbox 投递一条用户可见的失败通知，并把 inbox 推进到 `failed_notified`。这样 Feishu、未来邮件/企微/微信等 surface 都能避免"消息已处理失败但用户端没有任何回复"的黑洞。

### 4.3 WebUI

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

### 4.4 TUI

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

### 4.5 Harness Eval 服务化

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

## 5. 主要 API 能力

### 5.1 健康与状态

| API | 用途 |
|---|---|
| `GET /health` | 简单健康 |
| `GET /healthz` | Gateway health |
| `GET /readyz` | ready 状态，包含 WebUI 静态资源状态 |
| `GET /api/webui/manifest` | WebUI 静态资源 manifest |

### 5.2 Session / Runtime

| API | 用途 |
|---|---|
| `GET /api/sessions` | session 列表 |
| `POST /api/sessions` | 创建 session |
| `GET /api/sessions/:id/events` | session event |
| `GET /api/sessions/:id/runs` | run 列表 |
| `GET /api/sessions/:id/history-index` | 无正文历史索引：恢复状态、checkpoint、索引覆盖、近期元数据和导航卡片 |
| `POST /api/sessions/:id/messages` | 发送消息 |
| `POST/PATCH /api/runtime/live-subscriptions` | 创建或原子更新 Surface 多源实时订阅 |
| `GET /api/runtime/live/:id` | Session、Execution、Mission 共用的 multiplex SSE |
| `GET /api/runtime/timeline` | runtime timeline |
| `GET /api/runtime/control-plane` | 控制面摘要 |
| `GET/POST /api/mission/schedules` | 查询或创建由 Runtime 持有的 Mission 定时任务 |
| `POST /api/mission/schedules/:id/run` | 通过正式 Mission dispatch 路径立即运行一次 |
| `POST /api/mission/schedules/:id/pause` | 暂停后续自动触发 |
| `POST /api/mission/schedules/:id/resume` | 恢复后续自动触发 |
| `DELETE /api/mission/schedules/:id` | 删除未来调度，保留既有 fire 证据 |

### 5.3 Context / Memory

| API | 用途 |
|---|---|
| `GET /api/sessions/:id/projection` | Session 运行投影：run graph、工具时间线、token/model telemetry、memory/context、team/session、risk/approval |
| `GET /api/context/current` | 当前上下文 |
| `GET /api/evidence/resolve` | evidence ref 解析 |
| `GET /api/memory/status` | memory 状态 |
| `GET /api/memory/search` | memory 搜索 |
| `GET /api/memory/packet` | context packet |
| `GET /api/memory/entities` | entity |
| `GET /api/memory/triples` | triples |
| `POST /api/memory/facts/check` | fact check |

### 5.4 Skills

Skills API 分三层：Catalog、Projection、Governance。通用 Skill API 只负责发现、投影、文件查看和治理评估；具体领域的 Skill 执行由已注册 App 在 `/api/apps/<id>/**` 下承接。MFG 仅是这个通用规则的第一个实例。

| API | 用途 |
|---|---|
| `GET /api/skills/catalog` | 技能全集 |
| `GET /api/skills/:id` | 技能详情 |
| `GET /api/skills/projection?surface=webui` | WebUI 投影 |
| `GET /api/skills/projection?surface=tui` | TUI 投影 |
| `GET /api/skills/projection?surface=cli` | CLI 投影 |
| `GET /api/skills/:id/files` | 技能文件列表 |
| `GET /api/skills/:id/files/raw` | 技能文件内容 |
| `POST /api/skills/install` | 上传 `.tar` 技能包，经安全检查后安装 |
| `POST /api/skills/maintenance/evaluate` | 技能维护与演进建议 |
| `GET /api/apps` | 当前 Gateway 已注册、可投影的 App catalog |
| `POST /api/apps/mfg/incidents/:id/skills/plan` | MFG 应用层技能规划 |
| `POST /api/apps/mfg/incidents/:id/skills/:skill_id/run` | MFG 应用层技能执行 |
| `GET /api/apps/mfg/incidents/:id/skills` | MFG 事件技能运行记录 |

### 5.5 Tools

| API | 用途 |
|---|---|
| `GET /api/tools` | 工具 registry |
| `POST /api/tools/execute` | 执行允许的工具 |
| `GET /api/tools/cache` | 工具缓存状态 |
| `POST /api/tools/batch-readonly` | 只读工具批处理 |
| `POST /api/tools/mutations/preview` | 变更预览 |
| `POST /api/tools/mutations/apply` | 变更应用 |
| `GET|POST /api/tools/checkpoints` | checkpoint 列表和创建 |
| `POST /api/tools/intent-plan` | intent plan |
| `POST /api/tools/context-fanout/plan` | context fanout plan |

### 5.6 Matrix / App 示例（MFG）

Matrix 是结构化事实引擎。业务 App 可以基于 Matrix/Memory 构建领域能力；MFG 是制造领域的参考 App，不构成 Cowd 对应用类型的限制。

| API | 用途 |
|---|---|
| `GET /api/matrix/health` | Matrix store 健康 |
| `GET /api/matrix/entities` | entity 列表 |
| `POST /api/matrix/entities/upsert` | entity upsert |
| `POST /api/matrix/relations/upsert` | relation upsert |
| `POST /api/matrix/facts/ingest` | fact ingest |
| `GET /api/matrix/metrics` | metric 列表 |
| `GET /api/apps/mfg/app` | MFG app descriptor |
| `GET /api/apps/mfg/incidents` | incident 列表 |
| `POST /api/apps/mfg/incidents` | 创建 incident |
| `POST /api/apps/mfg/incidents/:id/analyze` | operational analysis |

---

## 6. 使用方式

### 6.1 默认开发

默认开发路径只验证 core，不编译 TUI：

```bash
cargo fmt --all --check
cargo check
cargo check --workspace --exclude tui --no-default-features
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
cargo build -p cli --bin cowd
```

当前 App catalog 与受锁定来源可在完整产品构建前校验：

```bash
cargo run -p xtask -- apps verify --locked
```

`gateway`、`tui` 与 `cli` 通过相同的 `app-<id>` feature 选择 `cowd-product-apps` 中的静态 bundle。正常产品默认包含 MFG；`cargo build -p cli --no-default-features` 生成不含 MFG 的 core-only 产品，`cargo build -p cli --features full` 显式构建 TUI 与当前审核 App。YAML 配置仅控制已编译 App 的启停，不能替代 catalog/source lock 审核。

### 6.2 Gateway

Gateway 是后台服务入口：

```bash
# Gateway 服务管理
cowd gateway start
cowd gateway status
cowd gateway restart
cowd gateway stop
```

Gateway 启动后，TUI、WebUI 和外部 surface 都通过 Gateway API 使用核心能力。`restart` 只回收同一可执行文件启动的 `gateway run` 进程；二进制覆盖安装后也会通过启动路径识别旧进程，等待其退出后再拉起新实例，避免两个 Gateway 同时占用同一会话库。

### 6.3 TUI 联调

TUI 联调需要 full feature：

```bash
cargo run -p cli --bin cowd --features full
cargo run -p cli --bin cowd --features full -- tui
```

如果使用不带 TUI 的默认二进制请求 TUI，CLI 会明确提示该二进制未构建 TUI surface。

### 6.4 WebUI

WebUI 在 `cowd-edge` 构建：

```bash
cd ../cowd-edge
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
```

然后通过 `gateway.webui_dir` 指向 `surfaces/webui/dist`。

### 6.5 Cowd Edge

外部 surface 与 connector 在 Cowd Edge 仓库构建：

```bash
cd ../cowd-edge
cargo check --workspace --bins
cargo build --release -p edge-adapters --bins
```

每个 UI surface、message connector 与 source connector 都通过 `surface.json` 暴露能力，不进入 core 依赖图。

---

## 7. Capability 与投影

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

## 8. 当前依赖关系

### 8.1 Workspace crate 依赖图

以下是当前 workspace 内部依赖的主链路，省略 `serde/tokio/chrono` 等通用第三方包：

```text
cli
  -> gateway
  -> tui optional, only with `tui-surface/full`

tui
  -> no core crate dependency
  -> Gateway HTTP/SSE only

gateway
  -> runtime
  -> tools
  -> provider / model-protocol / mcp
  -> memory / fact-kernel / matrix-core / matrix-repository
  -> approval / session / harness-contract
  -> skill / plugins
  -> connector / surface
  -> app-mfg

runtime
  -> harness-contract
  -> provider / model-protocol
  -> memory / storage / approval
  -> plugins
  -> runtime::task / runtime::eval_gate / runtime::runtime_harness
  -> matrix-core only in dev-dependencies

tools
  -> harness-contract
  -> mcp / plugins / skill
  -> no runtime/provider dependency

fact-kernel
  -> no workspace domain dependency

memory
  -> fact-kernel
  -> storage

matrix-core
  -> fact-kernel

matrix-repository
  -> matrix-core
  -> storage

app-mfg
  -> matrix-core / matrix-repository
  -> storage

harness-eval
  -> harness-contract
  -> runtime

provider
  -> model-protocol

surface
  -> surface::channel contracts

model-protocol
  -> provider config / telemetry / usage contracts
```

### 8.2 依赖边界判断

已经符合目标的部分：

- TUI 不直接依赖 Gateway 内部 crate、runtime、provider、memory、channel，只通过 Gateway HTTP/SSE 使用能力。
- Runtime 不依赖 `channel` 和 `surface`，也不链接飞书、邮件、企微、WebUI 等平台 SDK。
- 非 TUI surface 不再进入 core workspace；Gateway 通过 `surface.json` 发现外部 surface，并以
  UDS/H2 托管 managed Edge，stdio JSONL 仅用于 OneShot。
- Matrix 和 Memory 没有互相直接吞并，二者通过 `fact-kernel` 保持事实语义边界。
- Gateway 作为后台服务聚合边界，集中承接 Runtime、Reality Core、Skill、Tool、Surface 与已注册 App 的 API 暴露；MFG 只是当前参考 App。
- Tools 已经从 `runtime` 和 `provider` 中解耦，只保留工具 schema、权限需求、纯执行支撑和工具局部治理能力。
- Gateway 的生产路径不再保留旧 `LiveCli`、`run_prompt`、REPL prompt loop、`AnthropicRuntimeClient` 和 `CliToolExecutor` 执行壳；Runtime 装载由 `runtime_factory` 创建，热 runtime 生命周期由 `GatewayRuntimeEntry` 与 `RuntimeService` 承接。
- API routes 和 services 不直接持有热 runtime lock，不直接调用 `run_turn_async`；运行时操作收敛到 `RuntimeService` 边界。

仍需继续收束的部分：

- `runtime` 不再依赖 connector/channel；`CrossPlaneRisk`、`DataClassification` 已进入 `harness-contract::policy`，connector 继续负责外部资源目录与能力描述。
- `gateway` 作为聚合 crate 依赖面很宽，这是服务入口的正常代价，但需要继续保持"route/service 薄编排，业务状态归 runtime/domain"的纪律，避免 Gateway 变成第二套 runtime。
- `runtime` 内部模块数量已经很大，Mission、Agent、Team、Steward、Recovery 已接入，但后续需要更清晰的子目录或 crate 内分层，减少 `lib.rs` 直接暴露过宽的问题。
- `gateway` 仍在测试夹具中保留少量 provider 错误格式化和响应转换辅助，用于覆盖历史输出兼容测试；生产路径由架构测试明确禁止直接 provider client。

---

## 9. 配置

常见配置片段：

```yaml
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
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

`apps.<id>.enabled` 是已编译、已审核 App 的唯一启动期开关：修改后重启 Gateway。关闭某个 App 会从 App catalog、路由、Skill、Auth capability、OpenAPI、AI tools、TUI 与 WebUI 投影中同步移除它；该配置不会在运行期拉取源码，也不会改变二进制中已编入的代码。新增通用 App 的来源锁定、开发/发行模式和后续 Cargo feature 规约见 [App 规范](docs/architecture/application-development-and-product-composition.md)。服务更新、认证状态迁移和单实例核验按 [Gateway 生命周期运行手册](docs/operator/gateway-lifecycle.md) 执行；不要手工编辑 `credential-state.json` 或复制认证凭据。

---

## 10. 验证

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

## 11. 当前实现状态

### 11.1 已经落成的能力

- core workspace 已删除旧平台适配 crate。
- `crates/cli` 默认不编译 `crates/tui`。
- `crates/gateway` 通过 `surface::channel` 使用 channel 合同和 `surface` 协议，但不依赖平台 SDK。
- `crates/runtime` 不依赖 channel/surface adapter。
- `cowd-edge` 承载 WebUI 和非 TUI sidecar。
- TUI 已形成 Clean/Panorama、Control Deck 和 Gateway attach 场景验证闭环。
- WebUI/TUI 的核心入口都走 Gateway，符合 Gateway 作为唯一后台服务入口的原则。
- Mission Control Runtime 已提供全局 projection 和 command receipt。
- Mission Runtime 已支持 session、proxy、command queue、steward request、projection。
- Session Execution Plane 已支持 session 切换、后台运行、跨 session message、pause/close 等控制语义。
- Team Template 已能编译为不可变 Agent Binding 与 AgentTask DAG；`ExecutionGraphRunner` 通过统一资源管理器并发执行就绪节点，角色依赖、WorkingState、综合与验证仍由同一图约束。
- `AgentRuntime` 是 Agent 生命周期唯一所有者。所有 backend 的状态在持久事件提交后统一投影到根 Session；WebUI 实时活动、历史回放和语义执行图使用同一 Agent/run/team/graph/node 身份。
- Team Agent 的 Skill 上限来自 exact approved Agent Definition，写入 Binding 后只能缩减；纯上游综合角色不重新获取工具证据。
- Steward Scheduler 已具备 tick、ledger、profile、approval action、evidence 记录等托管推进基础。
- Runtime Event Store 已覆盖 mission、session command、team、agent、approval、relation、steward、task、worker、schedule、tool、recovery 等 scope。
- Recovery Executor 已能基于事件账本执行恢复扫描并写入 recovery evidence。
- Runtime Module Map 已把 conversation、provider、tooling、mission、session、agent、team、steward、approval、context、recovery、policy、reality bridge 等核心域纳入代码级归属合同。
- Harness Eval 已服务化，Gateway/WebUI/TUI 可查询 latest/report/scenario/run，并通过 deterministic smoke 验证 runtime capability domains 覆盖情况。
- Gateway 已提供 `session.run_projection`，从持久 `session_events` 聚合 run graph、工具时间线、token/model telemetry、memory/context 证据、team/session 状态和 risk/approval 事件；TUI 启动时会拉取该投影并在 Runtime Activity 面板展示紧凑摘要，WebUI/报告可消费同一事实源。
- Gateway 已提供 body-free `SessionHistoryIndexProjection`。WebUI/TUI 先读取恢复代际、checkpoint、索引覆盖和近期消息元数据，再按需分页读取正文；长 Session 首屏不再依赖全部历史正文。Runtime live projection 同时区分 Harness 与 Provider wall time，便于定位慢在上下文、调度还是模型。
- WebUI 全景面板和 TUI 状态栏使用相同 Session/Input/Execution 投影，展示连续输入的归属决策、历史恢复状态、上下文覆盖与 Harness/Provider 延迟；Surface 不自行推断第二套执行状态。
- Runtime 已在 provider usage 层接入 `ModelPerformanceRegistry`，能从 `RunModelTelemetry` 聚合首 token 延迟、输出速度、真实/估算 usage、失败率和质量评分，并按 quick/standard/deep/recovery 意图生成 `ModelRouteDecision`；`runtime_capabilities` 已暴露 `model_router`，模型能看到该能力并据此选择快答、深度或恢复策略。
- SurfaceHost 已具备持久 inbox/outbox/delivery event、重试、DLQ 和 operator replay/retry 修复入口。
- SurfaceHost 已能把 inbound runtime 处理和 outbound reply 投递关联成完整状态机，`replied` / `reply_failed` / `reply_retry_scheduled` 进入 inbox 终态或修复态，WebUI/TUI 使用 active snapshot 避免已回复消息继续显示为 working。
- Feishu managed sidecar 已通过 WebSocket 接收真实消息，并支持 `message.processing_complete` / `message.processing_failed` action 清理 Typing reaction；回复发送路径也会兜底清理原消息处理状态。
- WebUI 静态 surface 构建产物已要求同时生成 `dist/index.html`，Gateway 根路由和 `/s/webui/*` fallback 均以该文件为静态入口。
- 当前阶段版本标签：`v0.9.636`。

### 11.2 是否达到当前阶段目标

结论：当前代码已经达到"核心链路接线、可被 Gateway/TUI/WebUI 投影、可用 harness-eval 验证"的阶段目标，但还没有达到"完全自主、多 agent 深度协作、自我成长闭环完全成熟"的终局状态。

更具体地说：

- 对"Runtime 是 AI Harness 核心"的目标：阶段性达成。Mission、session、team、agent、steward、approval、event、recovery 都已回到 runtime，并由 `runtime::module_map` 形成可测试的模块归属和生命周期 owner 合同。
- 对"Gateway 干净，只做后台入口和编排"的目标：阶段性达成。旧 LiveCli/run_prompt/REPL prompt loop 已删除，热 runtime 承载体已迁到 `GatewayRuntimeEntry`，routes/services 的热 runtime 操作已收敛到 `RuntimeService`。
- 对"surface 与 runtime 解耦"的目标：已达成核心边界。TUI/WebUI/channel 都不应直接进入 runtime，当前 runtime 没有依赖 channel/surface。
- 对"tools 只是 AI 的手脚"的目标：当前阶段已达成核心边界。`tools` 不再依赖 runtime/provider，后续重点是继续提高工具合同、审计、checkpoint、mutation preview 的能力质量，而不是再承担 harness 生命周期。
- 对"多 agent 高阶协同"的目标：完成基础底座，但还不是完整智能团队运行时。当前 team execution 更像任务分派、事件、证据和 agent input 投递闭环，最终综合、复杂依赖调度、失败恢复、跨 agent 互看输出和人类实时介入仍需继续增强。
- 对"长对话控制多 session / Mission Control"的目标：完成主要控制模型和 API 底座，但高级自然语言跨 session 指挥、session 间代理互拉、全局托管 agent 汇报仍需要更深的 runtime 策略层。
- 对"自我成长和事实内核"的目标：Memory、Matrix、Fact Kernel 已有边界；Memory 已形成确定性治理、低风险语义治理、人工复核和证据保留闭环。跨版本能力晋升、自身代码变更验证等更高风险进化仍必须通过 Eval 与人工发布门禁，不能由记忆治理越权替代。

### 11.3 当前主要缺口

必须继续处理的架构缺口：

- Cross-plane 风险和数据分类合同已经上移到 `harness-contract::policy`，后续仍需把更多跨入口治理合同继续从 connector 中剥离，避免 connector 变成治理语义大桶。
- Runtime 内部已经具备代码级模块归属表和架构测试，后续如继续做物理目录迁移，必须保持 `runtime_module_architecture` 测试通过，避免再出现未归属公开模块。
- Gateway 聚合依赖还包括 provider crate，这是当前服务测试、模型配置和 runtime factory 装载链路的现实结果；生产代码必须继续维持"不直接执行 provider turn"的架构门禁。
- Recovery 目前更像状态恢复和事件补偿，不是完整的 provider turn 续跑系统。真实 kill/restart、进程中断、provider stream 中断、agent 半完成任务恢复还需要场景化强化。
- Steward 目前具备 tick 和 ledger，但长期托管执行还需要后台循环、预算、策略退避、审批超时、失败降级和汇报生成的完整服务化。
- Team/Agent 的单机 in-process 主路径已具备资源受控并行、角色依赖、WorkingState、synthesis/verify、审批介入及实时/历史观测；后续风险集中在外部 JSONL worker、长时运行和进程故障注入下的恢复质量，不应再建设第二套协同调度器。
- Mission Control 的自然语言控制还没有完全成为一等能力。现在 API/命令底座存在，但"用户在一个高级视窗里用自然语言管理全部 session/agent/team/steward"的体验还需要 WebUI/TUI 继续上层实现。
- Harness Eval 与 Surface 可靠消息层已能证明核心链路健康和投递可恢复，但测试矩阵还缺少长时间压测、并发 session、大量真实 sidecar、故障注入、权限审批超时、跨 surface 多入口投递等场景。

### 11.4 下一步演进原则

- 先补边界，再补体验。`tools -> runtime` 反向依赖已经清零；cross-plane 合同已从 connector 中抽离，后续继续清理 connector 中的治理语义残留。
- Runtime 继续承载 AI Harness 内核，但 runtime 内部要按业务子域收束，避免变成无边界大桶。
- Gateway 保持统一后台入口，继续承接 surface、WebUI、TUI、channel sidecar、callback、静态资源和服务 API，但不保存第二套执行状态。
- WebUI 做最完整的 Mission Control / Reality Core / Tool / Skill / Surface 管理面；TUI 做低噪声、高效率、键盘优先的终端控制面。
- Memory 和 Matrix 继续作为 Reality Core 的两个事实引擎；MFG 与未来业务 App 都作为消费 Reality Core 的应用，不再混入内核概念。
