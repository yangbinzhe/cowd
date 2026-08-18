# Cowd

> Rust 2021 Edition · MIT · 文档入口：[docs/README.md](docs/README.md)

Cowd 是面向复杂现实任务的自主智能执行基础设施：它以 Mission 作为跨会话的长期目标权威，以 Task 固化责任、边界与验收，以 Execution Graph 将模型推理、工具调用、多 Agent / Team 协作、审批、证据与恢复编译为同一条可持久化执行链。它不是聊天壳，也不把能力押注在某个模型上；它把企业级身份与授权、高并发资源治理、事实/记忆双内核、可验证外部行动和多 Surface 协作收敛为一个能够持续组织工作、并行推进、失败恢复并对结果负责的自主智能运行时。

从一次 AI Turn 到跨 Session、跨团队、跨系统的长期 Mission，Cowd 始终保留一份可观察、可审计、可恢复的执行真相。

- **企业级执行治理**：身份、权限、审批、沙箱、预算、审计、事件与恢复贯穿同一条执行链。
- **使命导向**：一次对话可以形成跨 Turn、跨 Session、跨 Agent 的长期 Mission，而不是在聊天记录中丢失目标。
- **图驱动自主编排**：任务被编译为带依赖、资源约束、证据要求和验收门的执行图，安全节点并行，冲突节点串行。
- **多团队协同**：Team Template、AgentTask DAG、WorkingState、冲突仲裁、结果合成和 review gate 共同支撑复杂协作。
- **高并发而不失控**：Session、Agent、工具和 Provider 各有独立准入、背压、取消与资源上限；并发服从依赖、纯度、风险和预算。
- **事实与记忆双内核**：Memory 管理可召回的语义经验，Matrix 管理结构化事实、关系、证据与指标，两者通过 Fact Kernel 对齐语义。
- **一次执行，多种视图**：TUI、WebUI、消息渠道与 Edge 消费相同的事件、证据和状态投影，不各自推导运行事实。

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

> 既然 DeepSeek Harness 开源了，Cowd 也开放给世界——欢迎一起构建自主智能基础设施。

## 1. 阅读导航

| 需要了解什么 | 入口 |
|---|---|
| 核心所有权、任务模型和一次任务如何流转 | 第 2–4 章 |
| 架构全景、分层与 Matrix 内核 | 第 5–6 章 |
| 一次 AI Turn 的完整协作链路 | 第 7 章 |
| 特性矩阵与各模块图示 | 第 8 章（8.3 图示化总览） |
| 模块归属、API、依赖图、消息状态机、多 Agent 流程 | 第 9–13 章 |
| 使用方式、配置、验证 | 第 14–17 章 |
| 启动、配置、排障、部署 | [系统说明书](docs/README.md) |
| 架构细节 | [架构文档](docs/architecture/README.md) |

README 只承载系统总览、特性矩阵与图示；分域细节统一收敛到 docs/，避免重复维护。

## 2. 核心所有权

| 层 | 唯一所有权 | 不拥有 |
|---|---|---|
| **Core / Runtime** | Session、Task、Mission、Turn、Graph、Agent/Team、模型、工具、权限、审批、Memory、Matrix、事件、恢复 | 平台 SDK、垂直业务状态、外部产品规则 |
| **Gateway** | 服务入口、认证上下文、Surface、可靠传输、受治理能力路由 | 业务 JSON Schema 与业务数据库 |
| **Edge** | WebUI、消息/数据源 Connector、Managed Sidecar、外部协议与驱动 | AI Turn、Task 语义和事实终态 |
| **业务 App** | 垂直领域合同、业务状态、私库、Worker 与 presentation | Core 身份/权限/执行状态和第二套生命周期权威 |

| 概念 | 权威职责 |
|---|---|
| **Session** | 用户与 Runtime 的持续交互容器，持有消息、Turn、分支、执行策略和实时订阅。 |
| **Turn** | 一次输入触发的执行循环，可包含多轮模型、工具、压缩、审批和证据写入。 |
| **Task** | 跨 Turn 的目标、责任、边界与验收账本；Delegated Task 继承明确 scope。 |
| **Mission** | 跨 Session、Task、Agent、Team 与 Schedule 的长期目标组织面。 |
| **Execution Graph** | 工作、依赖、资源、风险、审批、证据和终态的规范执行结构。 |
| **Evidence** | 工具收据、事实引用、事件、评审和恢复记录组成的反向可追溯依据。 |

```text
Mission（为什么做、最终完成什么）
  └── Task（谁负责、边界与结果归属）
       └── Turn（一次输入触发的执行循环）
            └── Graph Node（模型 / 工具 / Agent / 审批 / 验收）

Session 是交互容器，Mission 是全局组织面；二者通过 Task contribution 关联，互不替代。
```

## 3. 一次任务如何运行

```text
用户 / Connector / WebUI / TUI / Schedule
            │
            ▼
Gateway：认证、会话、能力合同、任务受理（幂等 inbox）
            │
            ▼
TaskRouter：复用 Root Task / Delegated Task / 显式 Focus
            ├── Mission contribution：建立全局目标归属
            └── Intent + Policy + Team Template
            │
            ▼
Core Runtime：规划 → 模型调用 → 工具/Agent 并发执行 → 记忆与状态归并
            │
            ├── Edge：外部系统、消息与 Surface 的协议适配
            └── 业务 App：垂直领域工作流与数据模型（受治理接入）
            │
            ▼
WorkingState：evidence / conflict / unresolved / artifact
            │
            ▼
synthesis → verify/review gate → Task/Mission terminal
            │
            ▼
统一事件、审计、状态与结果投影回各个 Surface
```

关键原则是“单一事实源，多种视图”：任务及其状态在 Core 内收敛；WebUI、TUI、Connector 和业务页面消费同一份经过授权的能力与事件投影，不各自发明任务状态。

## 4. 一次 AI Turn

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
                     │ 继续推理 / 工具计划        │
                     │ Agent/Team 委派            │
                     │ 审批等待 / 语义压缩 / 接续  │
                     └────────────┬───────────────┘
                                  ▼
                     execution_supervisor / Safety Fuse
                                  │
                                  ▼
                  terminal outbox + usage + evidence + events
                                  │
                                  ▼
                       Surface Egress / 下一 Turn
```

Turn 对外保持一个可取消、可观察、可恢复的执行身份。停滞、预算耗尽、审批等待和工具失败都有 typed 状态，不以静默重试或模型自报终态掩盖失败。

## 5. 架构全景

```text
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
        │  │ RuntimeHost  │  │ SurfaceHost  │  │ Gateway Services      │  │
        │  │ 会话热加载    │  │ Inbox/Outbox │  │ runtime·session·task   │  │
        │  │ turn 执行     │  │ DLQ·重试·回放│  │ memory·matrix·approval │  │
        │  │ token 管控    │  │ sidecar 托管 │  │ tools·skills·agents    │  │
        │  └──────┬───────┘  └──────┬───────┘  │ surface·policy·...     │  │
        └─────────┼─────────────────┼──────────┴──────────────────────────┘
                  │                 │
    ┌─────────────┼─────────────────┼──────────────────────────────┐
    │             ▼                 ▼                              │
    │   ┌──────────────────────────────────────────────────┐      │
    │   │        AI Harness Runtime (crates/runtime)        │      │
    │   │      显式 module map · 生命周期与治理架构域             │      │
    │   │  Conversation · Provider · Tooling · Mission      │      │
    │   │  Session · Agent · Team · Approval · Context      │      │
    │   │  Policy · ExecutionCore · Recovery · Evolution    │      │
    │   │  Configuration · Infrastructure · Skill          │      │
    │   └──────────────────────────────────────────────────┘      │
    │                                                              │
    │   ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐   │
    │   │ Reality Core│  │ Tool System  │  │ Model Provider   │   │
    │   │ fact-kernel │  │ tools        │  │ model-protocol   │   │
    │   │  memory     │  │  · MCP bridge│  │   provider       │   │
    │   │  matrix     │  │  · plugins   │  │   mcp            │   │
    │   │             │  │  · sandbox   │  │ (OpenAI/Anthro/   │   │
    │   │             │  │  · LSP/file  │  │  DeepSeek/Qwen)   │   │
    │   └─────────────┘  └──────────────┘  └──────────────────┘   │
    │                                                              │
    │   底层存储: storage (SQLite·PostgreSQL·Migration·Health)        │
    └──────────────────────────────────────────────────────────────┘
```

### 5.1 分层架构

```text
┌─────────────────────────────────────────────────────────────────────┐
│ Entry 层         cli · gateway · tui · edge/surface                 │
│                   极薄入口 · 统一后台 · 终端面 · WebUI/消息渠道       │
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
│ Application 层   app-host · product-apps · storage                  │
│                   受治理业务 App · 通用存储 · 遥测基础                │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Matrix 核心内核

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

### 5.3 Edge 与业务 App

```text
                    Cowd Core / Gateway
 ┌─────────────────────────────────────────────────────┐
 │ RuntimeHost / SurfaceHost / Auth / API / Skills     │
 └──────────────┬──────────────────────────────────────┘
                │ surface.json + UDS/H2 / HTTP/SSE
    ┌───────────┴───────────────────────────┐
    ▼                                       ▼
┌─────────────────────┐          ┌──────────────────────────────┐
│ Cowd Edge            │          │ 业务 App（独立产品仓）         │
│ WebUI / Connectors   │          │  · 垂直领域合同与业务状态      │
│  · WebUI Surface     │          │  · app-protocol / 签名 Bundle │
│  · Message Connector │          │  · Worker 与专属 UI          │
│  · Source Connector  │          └──────────────────────────────┘
└─────────────────────┘
```

Edge 负责把用户、消息平台和数据源接入 Gateway；业务 App 以独立产品仓按声明式合同接入垂直领域。两者都不拥有 Runtime、不复制会话状态，也不绕过 Core 的安全、审计与能力合同。

## 6. 协作全景：一次 AI Turn 的完整链路

```text
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
│  ←→ 工具执行          │  governed tool DAG → tool_ledger → evidence
│  ←→ 上下文压缩        │  compact（语义检查点）
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
```

多 Agent 协作分流：

```text
用户请求 → intent_planner (意图分类)
  ├─ solo 任务 → Conversation 直接执行
  ├─ team 任务 → Team Template → immutable AgentTask graph
  │               ├─ Agent A/B 并行（受并发阶梯约束）
  │               ├─ Team WorkingState / team_board → evidence/conflict/unresolved
  │               └─ dependency → convergence/arbiter → synthesis → verify gate
  └─ steward 任务 → 持久调度: tick → autonomy_profile → 托管执行
                    └─ steward_agent → decision_ledger → handoff
```

## 7. 核心特性矩阵

| 特性域 | 能力 | 成熟度 | 关键组件 |
|--------|------|--------|----------|
| **多模型路由** | OpenAI/Anthropic/DeepSeek/Qwen 自动适配 + Provider fallback 链 | ✅ 生产就绪 | `provider` · `model-protocol` · Provider routing |
| **会话管理** | 多 session 并行、切换、后台运行、暂停/关闭、检查点/恢复；持久化连接池并发访问 | ✅ 生产就绪 | `session_execution` · `SessionExecutionPlane` |
| **Task/Mission 治理** | Root/Delegated Task、跨 Turn 绑定、显式 focus、Mission 组织与 contribution 投影 | ✅ 生产就绪 | `runtime::task` · `TaskRouter` · `MissionOrganizer` |
| **图驱动执行** | Deliberation、ReWOO、Tool DAG、资源门、Safety Fuse（无进展才熔断）、结果归并 | ✅ 生产就绪 | `execution_core` · `orchestration` |
| **上下文工程** | 动态预算、硬容量预检、语义检查点压缩、记忆召回、知识激活、证据规划 | ✅ 生产就绪 | `context_runtime` · `budget_policy` · `compact` |
| **自治预算** | 按模型窗口等比缩放、80% 软阈值、共享上下文占用测算、缓存复用与命中率记录 | ✅ 生产就绪 | `budget_policy` · provider cache |
| **5 层记忆系统** | L0身份→L1核心→L2项目→L3深度→L4共享 + 有界压缩 + 向量/FTS 检索 | ✅ 生产就绪 | `memory` · `fact-kernel` |
| **进化记忆** | 确定性规则 + 模型候选双层治理，候选校验/提升/审计闭环 | ✅ 生产就绪 | `evolution` · `GrowthService` |
| **结构化事实引擎** | 实体/关系/证据/Metrics/Ontology + 后端中立持久化 + 质量门控 | ✅ 生产就绪 | `matrix-core` · `matrix-repository` · `MatrixDataPlane` |
| **多 Agent 协作** | Team 模板 → AgentTask DAG → 并发阶梯（8/16/64/256）→ WorkingState/team_board → 仲裁收敛 → synthesis/verify | ✅ 核心闭环 | `orchestration` · `ExecutionGraphRunner` · `team_runtime` |
| **团队收敛仲裁** | 多角色 evidence/conflict 汇总、仲裁理由、终态 JSON/产物合同 | ✅ 生产就绪 | `convergence` · `result_reducer` · Team Template |
| **权限与审批** | 五档审批矩阵（cautious/supervised/stewarded/autonomous/yolo）× 五域统一 ApprovalRouter；YOLO 全信任直连宿主机 | ✅ 生产就绪 | `permissions` · `approval::router` · `approval_queue` |
| **Linux 沙箱** | Landlock/seccomp/rlimit/cgroup 强隔离、Mutation Preview、bash 审计；完全信任档位直连宿主机 | ✅ 生产就绪 | `sandbox` · `sandbox_exec` · `policy_engine` |
| **可靠消息投递** | Inbox→Outbox→DLQ 完整状态机、重试/backoff、operator 修复入口 | ✅ 生产就绪 | `SurfaceHost` · `message_store` |
| **Surface 协议** | `surface.json` manifest、UDS/H2 managed 与 static/OneShot lifecycle | ✅ 生产就绪 | `surface` · `SurfaceManifest` |
| **事件账本 & 恢复** | 覆盖 mission/session/team/agent/tool/recovery 的事件存储+回放 | ✅ 生产就绪 | `runtime_event_store` · `recovery` |
| **工具系统** | 内置工具 + MCP 桥接 + Plugin 集成 + LSP + Checkpoint + Mutation Preview；工具 DAG 并行（默认 42） | ✅ 生产就绪 | `tools` · `tool_orchestrator` · `mcp_tool_bridge` |
| **技能目录** | 多 root 发现、安全扫描、维护评估、生成、路由、projection | ✅ 生产就绪 | `skill/service` · `SkillRegistry` · `SkillRouter` |
| **长期任务策略** | 健康推进不设死期限；仅死循环/崩溃/严重故障终止；30 分钟用户警告 + finalize 收尾 | ✅ 生产就绪 | `execution_live` · `safety_fuse` · `/api/sessions/:id/finalize` |
| **Harness Eval** | 场景矩阵、确定性 smoke、能力覆盖报告、Gateway 服务化 | ✅ 生产就绪 | `harness-eval` · `/api/harness-eval/*` |
| **TUI / WebUI 控制面** | TUI Clean/Panorama 双模式；WebUI 会话/执行图/团队/审批全景 | ✅ 生产就绪 | `tui` · cowd-edge `surfaces/webui` |

“已实现/已接线”表示代码合同、生产调用链与相应回归存在，不代表所有外部平台凭据组合都完成生产认证；“增强中”不作为生产完成承诺。

### 7.1 自治预算

自治档位同时约束并发、轮次、token 与成本；预算按当前模型上下文窗口等比缩放，**默认取 80% 软阈值**（1M 窗口 → 约 800k），并记录每次执行的缓存命中率与缓存节省量。

```text
模型上下文窗口（按当前模型解析 / 配置覆盖）
        │  默认取 80%（比例钳制 1%–95%）
        ▼
上下文预算 subsystem_budget_tokens
        ├── memory 召回预算
        ├── tool 结果预算（总量/单条）
        ├── subagent / team 预算（共享上下文占用测算）
        └── runtime control / review 预算
        │
        ▼
档位成本上限约束单 turn 花费；软阈值只记录与展示，不硬性中断推进
```

团队预算按“共享上下文占用”测算：只有需要共享上下文的协同点（团队广播、WorkingState、收敛合成）计入占用，普通角色的独立请求只要不超标即可并行，不做全团队一刀切限制。缓存复用优先：前缀缓存、provider 缓存解析与命中率计入预算展示。

### 7.2 记忆分层

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

### 7.3 核心模块图示化总览

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
模型窗口 ──► 动态预算(80%)/容量预检 ──► 证据计划 ──► 记忆召回(L0..L4)
                                                  │
                                                  ▼
知识激活 ──► 语义检查点压缩 ──► 上下文组装（system+证据+历史+工具）
                                                  │
                                                  ▼
                                     Provider 请求（前缀缓存/连接池准入）
```

#### 权限 / 审批 / 沙箱

```text
一个档位 ──► PermissionMode + SandboxPosture + ApprovalProfile + Budget
                │
                ▼
        工具效果分类 ──► Grant ──► 风险策略 ──► 统一 ApprovalRouter（五域）
                │                              │
                ▼                              ├── 低风险：策略自动放行 + 审计
        Sandbox / Host Execution              ├── 高风险：审批队列（按档位）
                │                              └── yolo：全信任自动通过
                ▼
        submitted + decided + receipt + evidence
```

审批按“五档 × 五域”统一路由：cautious（只读+人工）、supervised（工作区写+平衡审批）、stewarded（自主+低风险自动）、autonomous（高自主+中风险自动）、yolo（完全信任，所有动作放行并审计）。沙箱与宿主机选择随档位：完全信任档位直接使用宿主机，不再强制沙箱。

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

## 8. 模块归属合同 (Module Map)

`crates/runtime` 通过 `runtime::module_map` 形成代码级归属合同。模块身份、所属域、所有者、公开面与生命周期所有权由架构测试校验，避免 README 中的静态计数替代源码事实：

```text
runtime 架构域全景

  Conversation  ─── turn、收件箱、会话热运行与事件
  Provider      ─── 模型传输、注册、策略与连接池
  Tooling       ─── 工具计划、调度、执行、策略与记忆
  Mission       ─── 任务、任务控制、证据、调度与命令路由
  Session       ─── 会话执行、输入、生命周期与关系图
  Agent         ─── Agent 定义、能力、运行与结果验证
  Team          ─── 团队实例化、AgentTask、WorkingState、投影与归并
  Steward       ─── 托管 Agent 与调度
  Approval      ─── 审批协调、统一路由、队列与门控
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

`runtime` 不依赖 Gateway、TUI、Surface 或 Connector；TUI 只使用 Gateway HTTP/SSE；工具和 Provider 不反向拥有 Runtime 生命周期。

## 9. 完整 API 表面

Gateway 通过 Axum Router 暴露受能力合同治理的 API；完整路由与能力清单以源码、运行时 `/api/gateway/capability-contract` 及其 OpenAPI 投影为准：

```text
Gateway API (HTTP/SSE :8642)

Public
├── GET  /health, /healthz, /readyz         健康检查
├── GET  /api/webui/manifest                 WebUI 资源清单
└── POST /api/auth/*                         认证

Session & Message
├── GET  /api/sessions                       会话列表/创建/详情/删除/分支
├── POST /api/sessions/:id/messages          发送消息
├── POST /api/sessions/:id/cancel            取消当前 Turn
├── POST /api/sessions/:id/finalize          收尾：回收中间成果并产出交付
├── GET  /api/sessions/:id/execution         Session 执行索引
├── GET  /api/runtime/executions/:id         规范执行投影(graph/activity/evidence)
├── POST /api/sessions/:id/compact           触发压缩
└── POST /api/sessions/:id/replay            重放会话

Runtime & Control Plane
├── GET  /api/runtime/live/:id               单物理连接 multiplex SSE
├── GET  /api/runtime/timeline               timeline
├── GET  /api/runtime/control-plane          控制面摘要
├── POST /api/runtime/turns                  提交 turn / 取消
└── GET  /api/runtime/events                 运行时事件

Memory & Reality Core
├── GET  /api/memory/*                       记忆状态/搜索/上下文包
├── GET  /api/matrix/entities                实体 CRUD
├── POST /api/matrix/facts/ingest            事实注入
├── GET  /api/matrix/metrics                 指标系统
└── GET  /api/reality/*                      现实引擎状态/流/证据/治理

Tools & Skills
├── GET  /api/tools                          工具注册表
├── POST /api/tools/mutations/preview        变更预览
├── GET  /api/skills/catalog                 技能全集
└── GET  /api/skills/projection              按 surface 投影

Agents & Mission Control
├── GET  /api/agents                         代理目录/团队配置
├── GET  /api/team-templates                 团队模板目录
├── GET  /api/mission/control                任务控制投影
├── POST /api/mission/control                任务控制命令
├── GET  /api/mission/approvals              审批请求
└── POST /api/mission/approvals/:id/decision 审批决策

Surface & Cross-Plane
├── GET  /api/surfaces                       已发现 surface 列表
├── GET  /api/surfaces/:id/inbox/outbox      可靠消息账本
├── POST /api/surfaces/:id/inbox/:msg/replay 重放入站消息
├── POST /api/surfaces/:id/outbox/:d/retry   重试投递
└── POST /api/cross-plane/action/execute     跨面行动执行

Eval & Edge
├── GET  /api/harness-eval/reports           评测报告
├── POST /api/harness-eval/runs              触发评测跑
├── GET  /api/edges                          边缘注册表
└── GET  /api/edges/health                   边缘健康

Workspace & Profiles
├── GET  /api/workspace                       工作区文件管理
├── POST /api/upload                          文件上传
└── GET  /api/profiles                        配置文件管理
```

## 10. 工作区 Crate 依赖图

```text
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
    ├─────────────────────────────────────────────────────┤
    │ Fact 层:                                            │
    │   fact-kernel (零dep)                               │
    │     ├── memory ── storage                           │
    │     └── matrix-core ── matrix-repository ── storage │
    ├─────────────────────────────────────────────────────┤
    │ 零依赖叶子 Crate:                                    │
    │   harness-contract · fact-kernel · model-protocol   │
    │   surface · session · mcp · plugins · storage        │
    └─────────────────────────────────────────────────────┘
```

**关键边界约束**（架构测试强制执行）：
- `tui` 不依赖任何 workspace crate → 只通过 Gateway HTTP/SSE。
- `runtime` 不依赖 `surface` / `connector` / `tui` / `gateway`。
- `tools` 不依赖 `runtime` / `provider`。
- 非 TUI surface 全部迁入 `cowd-edge`，不进 core workspace。

## 11. 可靠消息状态机

外部渠道消息的完整生命周期，由 `SurfaceHost` 持有：

```text
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
```

## 12. 多 Agent 协作流程

```text
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
│ (标准 turn)   │          │ TeamTemplate      │
│  conversation│          │   → role specs    │
│   → tools    │          │   → dependency    │
│   → compact  │          │   → budget        │
│   → reply    │          │   → 并发阶梯       │
└──────────────┘          └────────┬──────────┘
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
                    │  WorkingState / team_board  │
                    │  evidence / conflict /      │
                    │  unresolved / artifact      │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │  convergence / arbiter      │  仲裁理由 → key_decisions
                    │  → synthesis                │
                    │  → review gate              │
                    └──────────────┬──────────────┘
                                   ▼
                          ┌──────────────┐
                          │  人类 / 审批  │
                          │  review_after │
                          │  _each_phase  │
                          └──────────────┘
```

**Agent 事件同步**：`AgentRuntime` 先提交持久生命周期事实，再通过 `CowdEvent::AgentLifecycle + RelatedExecution` 投影到根 Session；Gateway 以相同身份生成实时流和历史回放，WebUI 以 Team/Agent 通道、Tool 依赖波次和语义执行图展示。执行 backend 不拥有第二套生命周期，也不能在持久提交前宣告终态。

## 13. 使用方式

```bash
# 安装（构建并部署到 ~/.cowd/bin）
./install.sh --release

# 已有配置或自动化环境
./install.sh --release --no-config

# TUI
cowd

# Gateway 服务管理
cowd gateway start
cowd gateway status
cowd gateway restart
cowd gateway stop
cowd gateway open
cowd gateway logs

# 诊断
cowd gateway diagnostics
```

WebUI 与 Edge 在 `cowd-edge` 仓库构建：

```bash
cd ../cowd-edge
npm --prefix surfaces/webui run build
```

## 14. Capability 与投影

```text
Capability Registry
  ├── WebUI 投影（最强管理面：表格/过滤/批量/证据/可视化）
  ├── TUI 投影（同能力集，终端密度与键盘优先：Clean/Panorama/Control Deck）
  ├── CLI 投影（轻控制/配置/诊断/启动）
  └── Surface manifest/status（外部渠道只做消息入口与投递）
```

## 15. 配置

```yaml
model: "deepseek-v4-pro"       # 示例，实际由本地配置决定
permissions:
  default_mode: "danger-full-access"
approval:
  profile: "yolo"              # cautious / supervised / stewarded / autonomous / yolo
  low_risk_timeout: "auto_approve_once"
runtime:
  budget:
    subsystem_budget_ratio_bp: 8000   # 上下文预算软阈值 80%
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-edge/surfaces/webui/dist"
```

执行策略（权限、审批档位、沙箱姿态、中断策略）可在会话发送消息前设置；`execution_policy_preset` 对应五档自治配置。

## 16. 验证

```bash
cargo fmt --all --check
cargo check
cargo check -p cli --bin cowd --features full
cargo test -p runtime --lib
cargo test -p gateway --lib
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

## 17. 大演进方向（尚未生产就绪）

以下能力不构成生产承诺：

- 跨节点 Runtime、全局配额、租约迁移和多集群故障转移。
- 可连续运行数周的长期自治 Mission 与主动价值评估。
- 跨租户 Agent/Team 联邦、能力市场、证据交换和最小披露协作。
- Skill、Agent、Team Template 与策略候选的全自动生成和晋升。
- Windows/macOS 与 Linux Landlock/seccomp/cgroup 等价的强内核隔离。
- 更大规模的时态/因果事实、流式 Source 和跨域本体治理。
- 长期任务完成率、证据质量、成本收益与自动回归归因。
- 跨外部系统全局 exactly-once 行动；终态方向是预演、审批、补偿和证据化。

## 18. 文档入口

- [系统说明书](docs/README.md)
- [架构文档](docs/architecture/README.md)
- [运维与排障](docs/operator/README.md)
- [Cowd Edge](../cowd-edge/README.md)
