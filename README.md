# Cowd

Cowd 是面向复杂现实任务的自主智能执行基础设施：它以 Mission 作为跨会话的长期目标权威，以 Task 固化责任、边界与验收，以 Execution Graph 将模型推理、工具调用、多 Agent / Team 协作、审批、证据与恢复编译为同一条可持久化执行链。它不是聊天壳，也不把能力押注在某个模型上；它把企业级身份与授权、高并发资源治理、事实/记忆双内核、可验证外部行动和多 Surface / App 协作收敛为一个能够持续组织工作、并行推进、失败恢复并对结果负责的自主智能运行时。

从一次 AI Turn 到跨 Session、跨团队、跨系统的长期 Mission，Cowd 始终保留一份可观察、可审计、可恢复的执行真相。

- **企业级执行治理**：身份、权限、审批、沙箱、预算、审计、事件与恢复贯穿同一条执行链。
- **使命导向**：一次对话可以形成跨 Turn、跨 Session、跨 Agent 的长期 Mission，而不是在聊天记录中丢失目标。
- **图驱动自主编排**：任务被编译为带依赖、资源约束、证据要求和验收门的执行图，安全节点并行，冲突节点串行。
- **多团队协同**：Team Template、AgentTask DAG、WorkingState、冲突仲裁、结果合成和 review gate 共同支撑复杂协作。
- **高并发而不失控**：Session、Agent、工具和 Provider 各有独立准入、背压、取消与资源上限；并发服从依赖、纯度、风险和预算。
- **事实与记忆双内核**：Memory 管理可召回的语义经验，Matrix 管理结构化事实、关系、证据与指标，两者通过 Fact Kernel 对齐语义。
- **一次执行，多种视图**：TUI、WebUI、消息渠道与业务 APP 消费相同的事件、证据和状态投影，不各自推导运行事实。

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

## 使用方式

### 安装与启动

```bash
./install.sh --release

# 已有配置或自动化环境
./install.sh --release --no-config

# TUI
cowd

# Gateway
cowd gateway start
cowd gateway status
cowd gateway open
cowd gateway logs
cowd gateway restart
cowd gateway stop

# 诊断
cowd doctor
cowd gateway doctor
```

Gateway 是 Runtime、TUI、WebUI、Connector 和 APP 的统一服务入口。零 APP 时 Core 仍可独立启动；可选 APP 损坏或不可用只隔离自身，只有显式 `required: true` 的 APP 才影响 readiness。

源码检查与构建：

```bash
make check
make build
cargo run -p cli --bin cowd --features full

cd ../cowd-edge
npm ci --prefix surfaces/webui
npm run dev:webui
```

### 动态 APP Bundle

Cowd 不编译或下载 APP 源码。APP 以 sealed signed Bundle 放入配置目录，Gateway 只在启动时发现、验签和建立 immutable Catalog；运行中没有目录监听或热替换。

```yaml
apps:
  directories:
    - /srv/cowd/apps
  trust_store: /etc/cowd/app-trust.json
  launcher:
    path: /usr/libexec/cowd/managed-worker-launcher
    sha256: sha256:<launcher-digest>
  runtime_root: /run/cowd/apps
  data_root: /var/lib/cowd/apps
  core_bridge_socket: /run/cowd/core-bridge.sock
  postgres_socket_dirs:
    - /run/postgresql
  cgroup_root: /sys/fs/cgroup/cowd
  resources:
    nofile: 256
    nproc: 4096
    address_space_bytes: 536870912
    cgroup_memory_bytes: 536870912
    cgroup_pids: 64
  supervisor:
    max_active_workers: 16
    max_starting_workers: 4
    activation_timeout_ms: 10000
    handshake_timeout_ms: 3000
    graceful_shutdown_ms: 5000
    idle_ttl_seconds: 300
    max_waiters_per_app: 256
    restart_window_seconds: 60
    max_restarts_per_window: 5
  entries:
    mfg:
      enabled: true
      required: false
      activation: lazy
      config_file: /etc/cowd/apps/mfg.json
```

未写入 `entries` 的合法 Bundle 默认 `enabled=true`、`required=false`、`activation=lazy`。`lazy` 表示按需 singleflight 激活并可在空闲后回收，不等于热更新；常驻必须显式设为 `resident`。

```bash
cowd apps list
cowd apps status mfg
cowd apps doctor mfg
cowd apps logs mfg
cowd apps restart mfg
```

## 架构设计

### 核心所有权与统一任务模型

| 层 | 唯一所有权 | 不拥有 |
|---|---|---|
| **Core / Runtime** | Session、Task、Mission、Turn、Graph、Agent/Team、模型、工具、权限、审批、Memory、Matrix、事件、恢复 | 平台 SDK、垂直业务状态、APP 私库 |
| **Gateway** | 服务入口、认证上下文、Surface、APP Catalog/Supervisor/CoreBridge、可靠传输 | APP 业务 JSON Schema 字节和业务数据库 |
| **Edge** | WebUI、消息/数据源 Connector、Managed Sidecar、外部协议与驱动 | AI Turn、Task 语义和事实终态 |
| **APP** | 垂直领域合同、业务状态、私库、Worker、WebUI/TUI presentation | Core 身份/权限/执行状态和第二套生命周期权威 |

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

### 系统全景

```text
                                用户入口
                    CLI · TUI · WebUI · 消息渠道
                                   │
                                   ▼
┌──────────────────────────────── Cowd Gateway ───────────────────────────────┐
│ Auth / API / RuntimeHost / SurfaceHost / AppPlatform / Catalog             │
│                                      │                                      │
│                         AppRuntimeSupervisor                               │
│               discover · admit · activate · health · quota                │
│                    restart · idle reap · drain · recover                   │
└───────────────┬──────────────────────┬──────────────────────┬───────────────┘
                │                      │ UDS/H2              │ HTTP/SSE
                ▼                      ▼                      ▼
      AI Harness Runtime        managed APP Worker       Cowd Edge / TUI
      Mission/Task/Graph        own data + own UI        WebUI/Connector
      Agent/Team/Context               │
                │                      │ signed typed CoreBridge
                └───────────┬──────────┘
                            ▼
        Reality Core · Tool/Skill/MCP/Plugin · Provider · Storage
```

APP 静态列表和静态资产不会激活 Worker；detail、invoke、stream 或声明式 TUI 请求可以激活 lazy APP。Gateway 校验已接纳 descriptor/digest、身份、tenant/workspace/session/turn/task、能力、deadline、字节上限、幂等键与调用链；业务 payload 的 typed/schema 校验在签名 digest 约束下由 Worker 执行。

### 一次任务

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
事件、证据、用量与结果一次归并 → TUI / WebUI / Channel / APP
```

Task 是结果归属的权威，Execution Graph 是执行方式的权威，Mission 是跨任务组织状态的权威。

### 一次 AI Turn

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

### 签名 APP 生命周期

```text
APP source ──independent build──► signed sealed Bundle
                                         │
Gateway startup ── scan / verify / admit ─┤
                                         ▼
                              immutable App Catalog
                                         │
                         ┌───────────────┴───────────────┐
                         ▼                               ▼
                       lazy                           resident
                  first request singleflight      startup readiness
                         └───────────────┬───────────────┘
                                         ▼
                                  managed Worker
                      credential → handshake → UDS/H2 multiplex
                      health → circuit/backoff → drain/recovery
```

Linux Worker 使用一次性凭据、完整 app_id/generation 身份、同连接 peer credential、no-new-privs、Landlock、seccomp、rlimit 和 delegated cgroup。强内核隔离只对 Linux 作此精确声明；macOS/Windows 等价隔离仍属于路线图。

## 核心特性矩阵

| 特性域 | 能力 | 状态 | 主要组件 |
|---|---|---|---|
| Mission / Task | Root/Delegated Task、Focus、贡献、Schedule、终态投影 | 已实现 | `TaskRouter` · `MissionOrganizer` · `mission_runtime` |
| 图驱动执行 | Deliberation、ReWOO、Tool DAG、资源门、Safety Fuse、结果归并 | 已实现 | `execution_core` · `orchestration` |
| Agent / Team | AgentTask DAG、能力选择、WorkingState、冲突仲裁、合成与评审 | 已实现 | `agent_runtime` · `team_runtime` |
| 高并发治理 | 分层准入、资源键、背压、取消、deadline、预算与 Provider pool | 已实现 | `session_execution` · `tool_orchestrator` |
| Provider | OpenAI/Anthropic/DeepSeek/Qwen 协议、路由策略、usage 与 fallback | 已接线 | `provider` · `model-protocol` |
| 上下文 | 动态预算、容量预检、证据计划、召回、检查点压缩 | 已实现 | `context_runtime` · `budget_policy` |
| Memory / Matrix | L0–L4、Entity/Relation/Fact/Evidence/Metric/Ontology、SQLite/PG | 已实现 | `memory` · `fact-kernel` · `matrix-*` |
| 权限 / 审批 / 沙箱 | Grant、风险、审批、Mutation Preview、Linux 强隔离与审计 | 已接线 | `policy_engine` · `approval` · `sandbox` |
| Tool / Skill / MCP / Plugin | 受治理调用、异步 Bash、LSP、Hook、发现与投影 | 已实现 | `tools` · `skill` · `mcp` · `plugins` |
| Session / Recovery | 并行会话、暂停/取消、分支、事件回放、检查点与幂等终态 | 已实现 | `session_execution` · `runtime_event_store` |
| Surface | inbox/outbox/DLQ、ACK、重试、回放和 operator 修复 | 已实现 | `SurfaceHost` · `message_store` |
| 动态 AppPlatform | 签名 Catalog、Supervisor、CoreBridge、通用代理、TUI/WebUI | 已接线 | `app-host` · `app-protocol` · `auth-broker` |
| Eval | 场景、smoke、能力覆盖、发布证据与服务化入口 | 增强中 | `harness-eval` · release gates |
| Steward / 受控进化 | 持久调度、handoff、candidate/canary/stable review | 增强中 | `steward_agent` · `evolution` |

“已实现/已接线”表示代码合同、生产调用链与相应回归存在，不代表所有外部平台凭据组合都完成生产认证；“增强中”不作为生产完成承诺。

## 核心设计

### Mission、Graph 与 Agent/Team

模型可以提出任务关系、策略、团队拓扑、工具计划和 replan；内核必须执行权限、审批、顺序、幂等、事务、lease/generation、资源配额、取消、重试、终态提交和审计。统一路径是 `proposal → policy validation → governed execution → evidence feedback`。

Team 只并行无依赖且资源不冲突的 AgentTask。每个 Agent 持有定义、能力上限、模型选择、Task 继承和运行身份；阶段结果先进入 WorkingState，冲突经过仲裁，最终结果经过合成与验收后才成为 Task 终态。

### 上下文、Memory 与 Matrix

上下文窗口是受预算管理的资源。Runtime 在系统语义、Memory/Matrix 证据、历史检查点、工具结果、Agent 扇出和 review 空间之间分配容量，请求前执行硬预检，并在终态后生成可接续的语义检查点。

Memory 保存可召回的经验和语义线索；Matrix 保存结构化、可计算、带 revision/evidence 的事实。Fact Kernel 对齐两者语义。SourcePack 使用分块 snapshot、checksum、receipt 与 watermark；只有最终事务成功才推进水位线。

### Provider、工具与能力扩展

`model-protocol` 定义供应商中立请求、流事件、工具调用和 usage；Provider routing policy 根据任务、授权、能力和历史性能选择模型。Tool Host 统一 lease、效果分类、超时、取消、Artifact、账本和 Memory pulse；MCP、Skill、Plugin 都进入同一权限/审批/审计链，不形成旁路。

### 权限、审批与 Linux 沙箱

```text
Autonomy Profile
  └── PermissionMode + SandboxPosture + ApprovalProfile + Budget
                                      │
工具效果分类 ──► Grant ──► 风险策略 ──► 审批队列
                                      │
                                      ▼
                          Sandbox / Host Execution
                                      │
                                      ▼
                       submitted + decided + receipt
```

Gateway 从认证上下文生成 principal、tenant、workspace、Session/Turn/Task 和授权 profile；这些字段不能由 APP payload 伪造。高风险副作用必须有审批或明确 policy，执行后以 typed receipt 与 evidence 关闭 obligation。

### 并发、事件与恢复

Session、Agent/Team、Tool wave、Provider 和 APP Worker 各有独立容量门。正确顺序由依赖与资源键决定，而不是全局串行或永久 per-entity task。持久输入、检查点、事件、receipt、terminal outbox 和 usage 构成恢复依据；已提交终态通过 idempotency key 拒绝重复结算。

### Surface、动态 APP 与 Eval

SurfaceHost 持有可靠消息生命周期，Connector 只适配平台协议。动态 APP 通过产品中立 `app-protocol` 和受管 Worker 接入，同一个签名 presentation 投影到 Edge WebUI 和 Cowd TUI。Core 正式合同包含 14 类通用 typed effect operation；MFG 验证的 Matrix 子集为 39，二者不是全局 Core operation 总数。

Harness Eval 将确定性 scenario、真实进程、恢复/并发/安全证据与发布门关联，但不会用 HTTP 200、mock 或模型自报结果替代 durable receipt。

## Runtime 模块地图

`crates/runtime/src/module_map.rs` 是代码级归属合同：

```text
Conversation   Turn、收件箱、会话执行与事件
Provider       模型传输、注册、策略与连接池
Tooling        工具计划、调度、执行、策略与记忆
Mission        Task、Mission、证据、调度与命令路由
Session        会话执行、输入、生命周期与关系图
Agent          定义、能力、选择、运行与结果验证
Team           实例化、AgentTask、WorkingState、投影与归并
Steward        托管 Agent 与持久调度
Approval       审批协调、队列与门控
Context        预算、证据、知识、资源与上下文组装
Recovery       事件存储、回放与恢复配方
Policy         权限、安全、信任、自治与跨面策略
ExecutionCore  执行图、监督、策略、实时投影与结果
RealityBridge  结构化数据、事实提取、决策与召回端口
Evolution      Signal、Case、Candidate、评测与发布治理
Configuration  配置、校验与 Profile
Infrastructure 检查点、质量门、升级、MCP、Sandbox 与 Surface 合同
Skill          技能激活、选择与记忆集成
```

`runtime` 不依赖 Gateway、TUI、Surface 或 Connector；TUI 只使用 Gateway HTTP/SSE；工具和 Provider 不反向拥有 Runtime 生命周期。

## Core、Edge、APP 与外部系统

```text
External World
  ├── Model Providers ─────────► Provider adapters
  ├── MCP Servers ─────────────► governed tools
  ├── Browser ─────────────────► Edge WebUI
  ├── Message Platforms ───────► Edge Message Connectors
  ├── Databases / Tables ──────► Edge Source Connectors
  └── Signed APP Bundles ──────► AppPlatform / Catalog / Supervisor
                                      │
                                      ▼
                    Runtime / Mission / Memory / Matrix
```

| 外接对象 | 接入点 | 边界 |
|---|---|---|
| 模型供应商 | Provider adapter | 供应商 wire 不进入 Conversation 语义 |
| MCP Server | MCP Bridge | 必须经过能力、审批、deadline 和审计 |
| 消息平台 | Edge Message Connector | Connector 不拥有 Session 或 AI Turn |
| 数据源 | Edge Source Connector | SourcePack 经 Core 校验后进入 Matrix |
| 浏览器 | Edge WebUI | 只呈现 Gateway 授权投影与传输态 |
| APP | Signed Bundle / managed Worker | 私库与 Core 隔离，能力经 typed transport/CoreBridge |
| SQLite / PostgreSQL | owner-specific adapters | Core 与 APP 不共享连接池、凭据或跨 schema SQL |

## 大演进方向（尚未生产就绪）

以下能力不构成生产承诺：

- 跨节点 Runtime、全局配额、租约迁移和多集群故障转移。
- 可连续运行数周的长期自治 Mission 与主动价值评估。
- 跨租户 Agent/Team 联邦、能力市场、证据交换和最小披露协作。
- Skill、Agent、Team Template 与策略候选的全自动生成和晋升。
- Windows/macOS 与 Linux Landlock/seccomp/cgroup 等价的强内核隔离。
- 更大规模的时态/因果事实、流式 Source 和跨域本体治理。
- 长期任务完成率、证据质量、成本收益与自动回归归因。
- 跨外部系统全局 exactly-once 行动；终态方向是预演、审批、补偿和证据化。

这些方向只有在代码合同、真实进程、故障恢复、安全和发布证据同时成立后，才会进入生产矩阵。

## 文档入口

- [系统说明书](docs/README.md)
- [架构文档](docs/architecture/README.md)
- [运维与排障](docs/operator/README.md)
- [Cowd Edge](../cowd-edge/README.md)
