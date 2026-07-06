# Cowd

Cowd 是 Rust 原生的 AI Harness 核心仓库。当前核心版本：`0.9.455`。

本仓库的目标不是实现一个单一聊天 CLI，而是建设一个可长期演进的 AI Harness 内核：统一承载模型调用、会话、上下文、记忆、事实、工具、技能、审批、任务推进、运行时治理和 surface 投影。CLI、TUI、WebUI、外部渠道都只是这个内核能力的不同入口和呈现方式。

非 TUI surface 已从 core 仓库迁出，统一进入独立仓库 `cowd-edge`。core 仓库只保留协议、Gateway 装载能力、AI Harness 核心能力，以及可选的 TUI surface。

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
  app-mfg
```

### 1.2 第一原则

- Runtime 不持有 channel，也不链接任何平台 SDK。
- Gateway 是唯一后端服务入口，负责 surface 发现、静态资源转发、callback 转发、health、events 和 JSONL sidecar 调度。
- TUI 和 WebUI 都只通过 Gateway HTTP/SSE API 使用核心能力。
- CLI 不做交互 UI，不承载业务执行器，只负责轻量命令、配置、诊断和 Gateway 启动。
- 默认开发/debug 构建不带 TUI，TUI 与 Gateway 分开开发。
- 只有 TUI 联调、完整产品验证和正式 release 才构建 `--features full`。
- 非 TUI surface 不在 core workspace 编译，全部从 `cowd-edge` 按需独立构建和交付。
- Memory 处理非结构化记忆和经验关联，Matrix 处理结构化事实、实体、关系和证据。
- MFG 是应用层，不是 AI Harness 内核。

### 1.3 当前核心闭环图

当前代码已经把“模型可感知能力、Runtime 编排、Mission 控制、证据沉淀、前端投影、评测门禁”接成同一条主链路：

```text
Model / Agent
  |
  | system prompt + tool schema
  |   - runtime_capabilities(detail=...)
  |   - runtime_orchestrate(action=...)
  v
Gateway RuntimeHost
  |
  | GatewayToolExecutor 自动绑定 active session_id
  | routes/service 只做薄编排
  v
Runtime AI Harness Core
  |
  +-- Execution Core
  |     - execution mode catalog
  |     - strategy matcher / ReWOO / Tool DAG / Reflexion
  |     - model-visible action guidance
  |
  +-- Mission Runtime v2
  |     - mission sessions / proxy / command queue
  |     - WorkGraph projection
  |     - conflict projection
  |     - evidence projection
  |     - steward/capability/health projection
  |
  +-- Team / Agent Runtime
  |     - team templates
  |     - role task dispatch
  |     - agent capability binding
  |     - mailbox / event bus / lifecycle
  |
  +-- Session Execution Plane
  |     - cross-session command
  |     - background/running/claimed lifecycle
  |     - session relation graph
  |
  +-- Reality Core
  |     - Memory: semantic recall, runtime context, experience
  |     - Matrix: structured fact/entity/relation/evidence
  |     - Fact Kernel: fact extraction and bridge contract
  |
  +-- Governance
        - approval queue
        - conflict arbiter
        - event store / replay / recovery
        - tool ledger / tool memory / budget policy
```

对外投影关系：

```text
Runtime Projection
  |
  +-- Gateway API
  |     /api/runtime/*
  |     /api/mission/*
  |     /api/sessions/*
  |     /api/harness-eval/*
  |
  +-- WebUI (cowd-edge)
  |     Mission Control
  |     Reality Core
  |     Surface / Connector / Tool / Skill consoles
  |
  +-- TUI (core optional surface)
  |     Gateway panel
  |     Runtime Control Deck
  |     Clean / Panorama terminal control
  |
  +-- Edge sidecars
        message ingress / outbox / callback / health
```

### 1.4 模型主动使用 Runtime 能力的机制

Cowd 不把多 Agent、团队、跨 session、审批和工具批处理硬编码成固定流程，而是把它们作为模型可感知的 Runtime 能力暴露出来：

```text
Prompt Builder
  -> 注入 Runtime execution decision
  -> 注入 runtime_capabilities / runtime_orchestrate 的使用说明
  -> 注入“批量证据、并行工具、团队协同、低新颖度重规划”的策略提示

Model
  -> 普通问题：直接回答
  -> 复杂问题：查询 runtime_capabilities
  -> 需要编排：调用 runtime_orchestrate(action=...)

Runtime
  -> 校验 action contract
  -> 选择 TeamTemplate / WorkGraph / session command / conflict gate
  -> 记录 Mission Evidence 和 RuntimeEvent
  -> 通过 Gateway 投影给 WebUI/TUI
```

关键点：

- 模型看到的是“可用能力、适用场景、动作合同、降级策略”，不是一堆 UI 规则。
- `runtime_capabilities` 是只读能力发现工具。
- `runtime_orchestrate` 是受控编排入口，Gateway/API session 会自动绑定当前 `session_id`。
- Runtime 最终负责校验、执行、降级和记录证据，模型不能绕过权限/审批/冲突治理。

## 2. 仓库边界

### 2.1 core 仓库

```text
crates/cli        极薄 CLI 入口，默认 debug 不编译 TUI
crates/gateway    HTTP/SSE 服务入口，负责 RuntimeHost 与 SurfaceHost
crates/runtime    AI Harness 运行时核心，不依赖 channel/surface SDK
crates/surface    Edge JSONL 协议与 manifest 合同（底层协议名仍为 cowd.surface.v1）
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

WebUI、飞书、邮件、企微、微信 iLink 与数据源 connector 不再进入 core workspace。它们通过 `surface.json` 和 JSONL sidecar 协议被 Gateway 发现和调用。

## 3. Workspace 能力分层

### 3.1 Entry 层

| crate | 职责 |
|---|---|
| `crates/cli` | 极薄命令入口。默认构建不带 TUI，不依赖 runtime/memory。 |
| `crates/gateway` | 后台服务入口。承载 HTTP/SSE API、RuntimeHost、SurfaceHost 和服务编排。 |
| `crates/tui` | 终端 surface。只在 `--features full` 或显式选择时构建。 |
| `crates/surface` | Surface manifest、JSONL frame、静态资源、callback、health 合同。 |

### 3.2 AI Harness 层

| crate | 职责 |
|---|---|
| `crates/harness-contract` | AI Harness 语义入口，承载策略、目标、工作图等核心语义。 |
| `crates/harness-eval` | 评测和能力验证边界。 |
| `crates/runtime` | 会话运行、上下文组装、任务生命周期、工具/MCP/provider 调度、运行时控制。 |
| `crates/session` | session 合同和生命周期存储。 |
| `crates/approval` | 用户介入、审批记录、权限与审计。 |
| `crates/model-protocol` | 模型协议、prompt cache、usage 合同。 |
| `crates/provider` | OpenAI/Anthropic/DeepSeek/Qwen 等模型 provider 适配。 |
| `crates/mcp` | MCP stdio / lifecycle 合同。 |

Provider 协议由 `model-protocol` 统一定义，当前只支持三类终态枚举：
`anthropic`、`completions`、`responses`。配置中显式写 `protocol` 时直接使用该协议，不再探测；不配置时由 provider 名、`base_url` 和模型名做本地确定性探测，不做网络探测，也不会消耗 token。Responses 协议会走 `/responses`，Completions 协议会走 `/chat/completions`，Anthropic 协议走 Messages API。

#### Runtime 内部能力

`crates/runtime` 是当前 AI Harness 的真正执行核心。它不是 UI 层、不是 Gateway 层，也不是 channel 适配层。它现在承载的核心子域如下：

```text
runtime
  conversation                 单次 turn、模型调用、工具回调、上下文压缩
  provider_runtime_client      provider fallback、模型链、请求执行
  mission_runtime              mission session、命令队列、proxy、steward 入口
  mission_control              Mission Control 全局投影和控制命令
  session_execution            session 状态、跨 session 消息、后台/切换/关闭
  team_runtime                 team 模板、角色、agent 组队
  team_execution               role task 生成、agent task 投递、evidence 记录
  agent_lifecycle              agent 进程/任务生命周期、状态和控制命令
  agent_mailbox                agent task mailbox
  agent_event_bus              agent progress event
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

当前实现已经把“多 session 管理、mission control、team 执行、agent 生命周期、托管 steward、审批、事件证据、恢复”这几条主链路放回 runtime，而不是散落在 tools、TUI 或 Gateway 中。`runtime::module_map` 进一步把 conversation、provider、tooling、mission、session、agent、team、steward、approval、context、recovery、policy、reality bridge 等核心域纳入代码级归属合同。

#### Mission Runtime v2

Mission Runtime v2 是当前多 Agent / 多 Session / 全局控制的核心投影，不再只是 session 列表：

```text
MissionProjection(schema_version=2)
  |
  +-- mission
  |     sessions
  |     proxies
  |     session_commands
  |
  +-- workgraph_projection
  |     selected template
  |     role tasks
  |     dependencies
  |     ready/blocked/completed nodes
  |
  +-- conflict_projection
  |     conflict receipts
  |     severity
  |     decision
  |     affected scope
  |
  +-- evidence_projection
  |     MissionEvidenceRef
  |     runtime_orchestration evidence
  |     team/session/conflict evidence
  |
  +-- steward_projection
  |     steward state
  |     scheduler ticks
  |     approval action receipts
  |
  +-- capability_projection
  |     RuntimeCapabilityCatalog
  |     RuntimeActionContract
  |     model-visible operation groups
  |
  +-- health_projection
        readiness
        degraded signals
        projection freshness
```

WebUI 的 Mission Control 页面、TUI Gateway panel 和 harness-eval 都消费同一套 v2 投影，避免“能力已实现但前端看不到、测试也无法证明”的断裂。

#### Agent / Team / Session 协同链路

```text
User turn / surface message
  -> Gateway RuntimeService
  -> ConversationRuntime
  -> Model sees runtime_capabilities/runtime_orchestrate
  -> RuntimeOrchestrationRequest
  -> TeamRuntime selects template
  -> TeamExecutionLoop creates role tasks
  -> AgentCapabilityResolver binds tools/permissions/evidence duties
  -> AgentMailbox / AgentLifecycle / AgentEventBus
  -> WorkGraph + MissionEvidence + RuntimeEventStore
  -> MissionProjection v2
  -> WebUI/TUI control surfaces
```

当前阶段已经具备协同运行底座、任务投递、证据记录、冲突仲裁和投影闭环。更高阶的“长时间真实并行 Agent 执行、Agent 间互阅输出、自然语言全局调度、托管 Agent 汇报”仍属于后续增强方向，但不再缺核心承载位置。

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
| `crates/plugins` | plugin manifest、registry 和生命周期。 |

渠道自身的聊天、收发消息、长连接、静态资源等属于 surface/sidecar；渠道附带的文档操作、平台高级能力未来应作为 skill/tool 安装，而不是塞回 Runtime 或 Gateway。

### 3.5 Application 层

| crate | 职责 |
|---|---|
| `crates/app-mfg` | MFG 制造应用层。基于 Matrix/Memory，不属于内核。 |
| `crates/storage` | 通用 SQLite/存储基础。 |
| `crates/model-protocol::telemetry` | provider/runtime 共享的事件和遥测基础类型。 |

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
- 管理 JSONL sidecar 生命周期。
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
  "entry": "./cowd-edge-feishu-message",
  "transport": "stdio-jsonl",
  "lifecycle": "managed",
  "capabilities": ["message.ingress", "message.egress", "message.callback", "health"],
  "routes": [
    { "kind": "callback", "path": "/events", "method": "POST", "public": true }
  ],
  "resources": [],
  "health": { "mode": "jsonl", "interval_ms": 30000 },
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

可靠消息状态不是简单的“是否收到”。当前语义如下：

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

`SurfaceMessageSnapshot` 会同时返回 `active_inbox`、`terminal_inbox`、`active_outbox` 和 `dead_letters`。WebUI/TUI 不应再用全部 inbox/outbox 数量代表“工作中”，而应读取 active 集合或按上述状态白名单降级计算。

飞书 surface 使用 WebSocket 接收消息。收到用户消息后 sidecar 会在原消息上设置 `Typing` reaction 表示处理中；Gateway 在 runtime 完成、回复成功、空回复或失败时都会通过 `message.processing_complete` / `message.processing_failed` action 通知 sidecar 清理或替换该 reaction。Feishu reply 发送路径也会在成功/失败时兜底清理，避免已经回复的消息仍残留“工作中”状态。

外部 surface 的 runtime turn 不再只有一个硬超时。Gateway 会根据消息内容选择 `SurfaceQuickReply` 或 `DeepInvestigation` 策略，并给每个策略同时设置总耗时和最大模型/工具迭代轮次。README、文档核查、代码检查、调研、测试、重构等消息会进入深度策略；普通短消息走快速策略。若 runtime 超时、超过迭代预算或执行失败，Gateway 不会只把 inbox 标成 `failed` 后沉默，而会通过同一套可靠 outbox 投递一条用户可见的失败通知，并把 inbox 推进到 `failed_notified`。这样 Feishu、未来邮件/企微/微信等 surface 都能避免“消息已处理失败但用户端没有任何回复”的黑洞。

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

### 4.5 Harness Eval 服务化

`crates/harness-eval` 不再只是离线 CLI 报告。报告 DTO、store 和 runner 已成为 library API，Gateway 通过 `/api/harness-eval/*` 暴露评测报告、场景矩阵和 smoke run：

| API | 用途 |
|---|---|
| `GET /api/harness-eval/reports` | 历史评测报告列表 |
| `GET /api/harness-eval/reports/latest` | 最新评测健康摘要 |
| `GET /api/harness-eval/reports/:id` | 单份评测报告详情 |
| `GET /api/harness-eval/scenarios` | stable AI 场景矩阵与 next-gen harness closure 场景 |
| `GET /api/harness-eval/runs` | 评测 run 历史 |
| `POST /api/harness-eval/runs` | 触发 quick/full deterministic smoke run |

默认 Gateway/WebUI/TUI 只触发无真实 provider token 消耗的 deterministic smoke。deep/real model 路径必须显式授权，防止评测面板误耗 token。

评测报告包会写入 `report.json`、`execution-trace.json`、`analysis-context.json`、`full-analysis-report-template.md`、`full-analysis-report-prompt.md`、`evidence/evidence-manifest.json` 和 `evidence/next-gen-harness-closure.json`。`report_gate` 会检查报告声称与证据是否一致：声称真实模型必须有 provider rounds，声称工具验证必须有工具调用，声称记忆/上下文治理必须有 Reality Context 证据，声称恢复/回放必须有 replay 或 recovery 证据。

常用终端快捷键：

| 快捷键 | 用途 |
|---|---|
| `Alt+V` | 切换 Clean / Panorama |
| `Alt+E` | 打开 runtime/evidence panorama 面板 |
| `Alt+G` | 打开 Gateway Control Deck |
| `Ctrl+P` | 打开命令面板 |
| `Esc Esc` 或 `Ctrl+C Ctrl+C` | 当前 turn 中取消，空闲时退出 |

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
| `POST /api/sessions/:id/messages` | 发送消息 |
| `GET /api/sessions/:id/stream` | SSE stream |
| `GET /api/runtime/timeline` | runtime timeline |
| `GET /api/runtime/control-plane` | 控制面摘要 |

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

Skills API 分三层：Catalog、Projection、Governance。通用 Skill API 只负责发现、投影、文件查看和治理评估；具体领域的 Skill 执行由上层应用路由承接，例如 MFG 的运行能力在 `/api/apps/mfg/**` 下。

| API | 用途 |
|---|---|
| `GET /api/skills/catalog` | 技能全集 |
| `GET /api/skills/:id` | 技能详情 |
| `GET /api/skills/projection?surface=webui` | WebUI 投影 |
| `GET /api/skills/projection?surface=tui` | TUI 投影 |
| `GET /api/skills/projection?surface=cli` | CLI 投影 |
| `GET /api/skills/:id/files` | 技能文件列表 |
| `GET /api/skills/:id/files/raw` | 技能文件内容 |
| `POST /api/skills/maintenance/evaluate` | 技能维护与演进建议 |
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

### 5.6 Matrix / MFG

Matrix 是结构化事实引擎，MFG 是基于 Matrix/Memory 的制造应用层。

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

### 6.2 Gateway

Gateway 是后台服务入口：

```bash
cargo run -p cli --bin cowd -- gateway
```

Gateway 启动后，TUI、WebUI 和外部 surface 都通过 Gateway API 使用核心能力。

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
- 非 TUI surface 不再进入 core workspace，外部 surface 通过 `surface.json` 和 JSONL sidecar 与 Gateway 连接。
- Matrix 和 Memory 没有互相直接吞并，二者通过 `fact-kernel` 保持事实语义边界。
- Gateway 作为后台服务聚合边界，集中承接 Runtime、Reality Core、Skill、Tool、Surface、MFG 的 API 暴露。
- Tools 已经从 `runtime` 和 `provider` 中解耦，只保留工具 schema、权限需求、纯执行支撑和工具局部治理能力。
- Gateway 的生产路径不再保留旧 `LiveCli`、`run_prompt`、REPL prompt loop、`AnthropicRuntimeClient` 和 `CliToolExecutor` 执行壳；Runtime 装载由 `runtime_factory` 创建，热 runtime 生命周期由 `GatewayRuntimeEntry` 与 `RuntimeService` 承接。
- API routes 和 services 不直接持有热 runtime lock，不直接调用 `run_turn_async`；运行时操作收敛到 `RuntimeService` 边界。

仍需继续收束的部分：

- `runtime` 不再依赖 connector/channel；`CrossPlaneRisk`、`DataClassification` 已进入 `harness-contract::policy`，connector 继续负责外部资源目录与能力描述。
- `gateway` 作为聚合 crate 依赖面很宽，这是服务入口的正常代价，但需要继续保持“route/service 薄编排，业务状态归 runtime/domain”的纪律，避免 Gateway 变成第二套 runtime。
- `runtime` 内部模块数量已经很大，Mission、Agent、Team、Steward、Recovery 已接入，但后续需要更清晰的子目录或 crate 内分层，减少 `lib.rs` 直接暴露过宽的问题。
- `gateway` 仍在测试夹具中保留少量 provider 错误格式化和响应转换辅助，用于覆盖历史输出兼容测试；生产路径由架构测试明确禁止直接 provider client。

## 9. 配置

常见配置片段：

```yaml
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-edge/surfaces/webui/dist"
```

模型/API 密钥属于配置和 secrets，不应成为顶层 auth 模块。Gateway 的 WebUI 静态资源配置是可选项，缺失时服务仍应可用。

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

### 10.1 当前测试矩阵状态

当前测试体系按“默认快速门禁、架构门禁、场景化门禁、真实 provider 门禁、人工/视觉门禁”分层：

```text
Default deterministic
  cargo fmt / diff check
  cargo check --workspace --all-targets
  cargo test --workspace --all-targets
  pnpm --dir ../cowd-edge/surfaces/webui test -- --run

Architecture gates
  gateway_runtimehost_architecture
  runtime_module_architecture
  module_map / dependency boundary checks

Harness scenario gates
  runtime ai_harness_deep_scenarios
  runtime ai_harness_e2e
  runtime mission_harness_e2e_eval
  harness-eval quick/full/deep deterministic reports

Surface / interactive gates
  scripts/scenarios/*
  tests/interactive/*
  manual WebUI/TUI visual checks

Live provider gates
  COWD_AI_HARNESS_LIVE=1 scripts/ci/ai-harness-live-provider.sh
  COWD_EVAL_REAL_MODEL=1 cargo test -p harness-eval real_ai_deep_scenarios -- --nocapture
```

本阶段最新全量回归证据：

- `cargo fmt --all -- --check`：通过。
- `git diff --check`：通过。
- `cargo check --workspace --all-targets`：通过。
- `cargo test --workspace --all-targets`：通过。
- `pnpm --dir ../cowd-edge/surfaces/webui test -- --run`：通过。
- 证据文件：`../plan/0706-MissionRuntime多Agent多Session智能协同闭环/evidence/全范围测试评价-证据.md`。

测试治理判断：

- 单元/合同测试已经覆盖核心 crate、架构边界、provider 协议、runtime 模块归属、TUI 状态和 WebUI 能力矩阵。
- 场景化测试覆盖 Gateway/session/runtime/memory/tool permission/skill/matrix/surface 的主要黄金路径。
- Live provider 和真实大模型评测仍是显式 opt-in，避免默认消耗 token。
- 当前重复风险主要集中在 `scripts/scenarios/*` 与 `tests/interactive/*` 的历史手工场景；默认门禁应继续优先收敛成少量黄金路径，而不是追加相似长链路脚本。
- 需要长期强化的测试方向是：真实多 Agent 长任务并行、跨 session 自然语言调度、surface 大量并发消息、sidecar 故障注入、provider stream 中断恢复、长期记忆污染/召回准确率。

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
- Team Execution Loop 已能根据 team/template 生成 role task，投递到 agent mailbox/lifecycle，记录 agent event 和 mission evidence。
- Agent Mailbox 和 Agent Event Bus 已进入 runtime，主 session 能看到 team/agent 进展的基础事件来源。
- Steward Scheduler 已具备 tick、ledger、profile、approval action、evidence 记录等托管推进基础。
- Runtime Event Store 已覆盖 mission、session command、team、agent、approval、relation、steward、task、worker、schedule、tool、recovery 等 scope。
- Recovery Executor 已能基于事件账本执行恢复扫描并写入 recovery evidence。
- Runtime Module Map 已把 conversation、provider、tooling、mission、session、agent、team、steward、approval、context、recovery、policy、reality bridge 等核心域纳入代码级归属合同。
- Runtime Capability Catalog 已把 execution modes、team templates、agent catalog、orchestration options、budget controls、policy gates 和 action contract 暴露为模型可感知能力。
- `runtime_capabilities` 与 `runtime_orchestrate` 已进入 Gateway tool executor，支持无 MCP 状态执行，并能在 Gateway session 中自动绑定 `session_id`。
- MissionProjection 已升级为 schema v2，包含 workgraph、conflict、evidence、steward、capability 和 health 投影。
- Conflict Arbiter 已成为 Runtime 内核能力，team/session/relation 冲突可记录 conflict receipt、Mission Evidence 和 RuntimeEvent。
- Agent Capability Resolver 已能按 role capability 绑定工具白名单、权限策略和 evidence duties。
- Harness Eval 已服务化，Gateway/WebUI/TUI 可查询 latest/report/scenario/run，并通过 deterministic smoke 验证 runtime capability domains 覆盖情况。
- Harness Eval 已新增 `mission_runtime_collaboration_closure`，用于证明能力合同、团队模板、WorkGraph、Agent 能力绑定、跨 session 命令、冲突仲裁和 MissionProjection v2 的闭环。
- Harness Eval 已新增 `next_gen_harness_closure`，把简单快答、复杂策略选择、批量工具证据、多 Agent 团队执行、跨 session 派发、记忆/现实上下文治理、冲突恢复纳入同一评测门禁，并写入 evidence manifest 防止报告只给表层结论。
- Gateway 已提供 `session.run_projection`，从持久 `session_events` 聚合 run graph、工具时间线、token/model telemetry、memory/context 证据、team/session 状态和 risk/approval 事件；TUI 启动时会拉取该投影并在 Runtime Activity 面板展示紧凑摘要，WebUI/报告可消费同一事实源。
- Runtime 已在 provider usage 层接入 `ModelPerformanceRegistry`，能从 `RunModelTelemetry` 聚合首 token 延迟、输出速度、真实/估算 usage、失败率和质量评分，并按 quick/standard/deep/recovery 意图生成 `ModelRouteDecision`；`runtime_capabilities` 已暴露 `model_router`，模型能看到该能力并据此选择快答、深度或恢复策略。
- SurfaceHost 已具备持久 inbox/outbox/delivery event、重试、DLQ 和 operator replay/retry 修复入口。
- SurfaceHost 已能把 inbound runtime 处理和 outbound reply 投递关联成完整状态机，`replied` / `reply_failed` / `reply_retry_scheduled` 进入 inbox 终态或修复态，WebUI/TUI 使用 active snapshot 避免已回复消息继续显示为 working。
- Feishu managed sidecar 已通过 WebSocket 接收真实消息，并支持 `message.processing_complete` / `message.processing_failed` action 清理 Typing reaction；回复发送路径也会兜底清理原消息处理状态。
- WebUI 静态 surface 构建产物已要求同时生成 `dist/index.html`，Gateway 根路由和 `/s/webui/*` fallback 均以该文件为静态入口。
- 版本标签：`v0.9.455`。

### 11.2 是否达到当前阶段目标

结论：当前代码已经达到“核心链路接线、可被 Gateway/TUI/WebUI 投影、可用 harness-eval 验证”的阶段目标。它已经能证明当前规划中的 Mission Runtime v2、多 Agent/多 Session 协同底座、模型可感知 Runtime 编排、冲突仲裁、证据记录和前端投影闭环。

它还没有达到“完全自主、多 Agent 长时间深度协作、自我成长闭环完全成熟”的终局状态。这个判断不是当前阶段失败，而是终局能力本身需要继续发展到更强的真实并行执行、长期自治、故障恢复和自我演进。

更具体地说：

- 对“Runtime 是 AI Harness 核心”的目标：阶段性达成。Mission、session、team、agent、steward、approval、event、recovery 都已回到 runtime，并由 `runtime::module_map` 形成可测试的模块归属和生命周期 owner 合同。
- 对“Gateway 干净，只做后台入口和编排”的目标：阶段性达成。旧 LiveCli/run_prompt/REPL prompt loop 已删除，热 runtime 承载体已迁到 `GatewayRuntimeEntry`，routes/services 的热 runtime 操作已收敛到 `RuntimeService`。
- 对“surface 与 runtime 解耦”的目标：已达成核心边界。TUI/WebUI/channel 都不应直接进入 runtime，当前 runtime 没有依赖 channel/surface。
- 对“tools 只是 AI 的手脚”的目标：当前阶段已达成核心边界。`tools` 不再依赖 runtime/provider，后续重点是继续提高工具合同、审计、checkpoint、mutation preview 的能力质量，而不是再承担 harness 生命周期。
- 对“多 agent 高阶协同”的目标：完成基础底座，但还不是完整智能团队运行时。当前 team execution 更像任务分派、事件、证据和 agent input 投递闭环，最终综合、复杂依赖调度、失败恢复、跨 agent 互看输出和人类实时介入仍需继续增强。
- 对“长对话控制多 session / Mission Control”的目标：完成主要控制模型和 API 底座，但高级自然语言跨 session 指挥、session 间代理互拉、全局托管 agent 汇报仍需要更深的 runtime 策略层。
- 对“自我成长和事实内核”的目标：Memory、Matrix、Fact Kernel 已有边界，但成长闭环还更多是可记录、可召回、可验证的基础能力，没有完全形成长期自动提炼、冲突治理、衰减、质量评分和自我修正的成熟闭环。

### 11.3 当前主要缺口

必须继续处理的架构缺口：

- Cross-plane 风险和数据分类合同已经上移到 `harness-contract::policy`，后续仍需把更多跨入口治理合同继续从 connector 中剥离，避免 connector 变成治理语义大桶。
- Runtime 内部已经具备代码级模块归属表和架构测试，后续如继续做物理目录迁移，必须保持 `runtime_module_architecture` 测试通过，避免再出现未归属公开模块。
- Gateway 聚合依赖还包括 provider crate，这是当前服务测试、模型配置和 runtime factory 装载链路的现实结果；生产代码必须继续维持“不直接执行 provider turn”的架构门禁。
- Recovery 目前更像状态恢复和事件补偿，不是完整的 provider turn 续跑系统。真实 kill/restart、进程中断、provider stream 中断、agent 半完成任务恢复还需要场景化强化。
- Steward 目前具备 tick 和 ledger，但长期托管执行还需要后台循环、预算、策略退避、审批超时、失败降级和汇报生成的完整服务化。
- Team Execution 目前能派发任务和记录证据，但还需要真实并行 agent 执行、角色依赖阻塞、最终 synthesis、review gate、人类插手、agent 间互阅输出的强闭环。
- Mission Control 的自然语言控制还没有完全成为一等能力。现在 API/命令底座存在，但“用户在一个高级视窗里用自然语言管理全部 session/agent/team/steward”的体验还需要 WebUI/TUI 继续上层实现。
- Harness Eval 与 Surface 可靠消息层已能证明核心链路健康和投递可恢复，但测试矩阵还缺少长时间压测、并发 session、大量真实 sidecar、故障注入、权限审批超时、跨 surface 多入口投递等场景。

### 11.4 下一步演进原则

- 先补边界，再补体验。`tools -> runtime` 反向依赖已经清零；cross-plane 合同已从 connector 中抽离，后续继续清理 connector 中的治理语义残留。
- Runtime 继续承载 AI Harness 内核，但 runtime 内部要按业务子域收束，避免变成无边界大桶。
- Gateway 保持统一后台入口，继续承接 surface、WebUI、TUI、channel sidecar、callback、静态资源和服务 API，但不保存第二套执行状态。
- WebUI 做最完整的 Mission Control / Reality Core / Tool / Skill / Surface 管理面；TUI 做低噪声、高效率、键盘优先的终端控制面。
- Memory 和 Matrix 继续作为 Reality Core 的两个事实引擎，MFG 作为消费 Reality Core 的应用，不再混入内核概念。
