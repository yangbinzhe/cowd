# Cowd — AI 编程智能体框架

> **Rust 实现的高性能 AI 编程智能体框架**，支持 CLI / TUI / WebUI 三种交互模式。
> 提供模型适配、工具执行、五层记忆系统、MCP 协议、插件系统和多平台网关等完整能力。

---

## 目录

- [核心能力](#核心能力)
- [架构总览](#架构总览)
- [Crate 架构详解](#crate-架构详解)
- [记忆系统](#记忆系统)
- [工具系统](#工具系统)
- [插件与技能系统](#插件与技能系统)
- [WebUI 前端](#webui-前端)
- [配置体系](#配置体系)
- [平台网关](#平台网关)
- [快速开始](#快速开始)
- [开发指南](#开发指南)
- [测试状态](#测试状态)
- [许可证](#许可证)

---

## 核心能力

### 模型与 Provider 适配

Cowd 内置多 Provider 路由层，支持自动根据模型名匹配对应的 API 端点：

| Provider | 支持情况 | 说明 |
|---|---|---|
| Anthropic (Claude) | 原生支持 | OAuth + API Key，完整支持 Prompt Caching |
| OpenAI 兼容 | 通用适配 | GPT / OpenRouter / Ollama / StepFun 等 |
| DeepSeek | 内置路由 | 含 `reasoning_content` 回传支持 |
| Qwen / DashScope | 前缀路由 | `qwen/*` 前缀自动路由 |
| Grok / xAI | 别名支持 | 内置模型别名表 |
| Kimi / Moonshot | 前缀路由 | `kimi/*` 前缀路由 |
| 自定义 Provider | `config.yaml` 配置 | 任意 OpenAI 兼容接口 |

**Provider Chain**：支持配置多 Provider 故障转移链和负载均衡策略，当主 Provider 返回 429/500 等可重试错误时自动切换到备用 Provider。

### 交互模式

| 模式 | 启动方式 | 特性 |
|---|---|---|
| **CLI** (交互式 REPL) | `cowd` | 类 Claw Code 命令行对话，支持管道输入、历史记录、斜杠命令 |
| **CLI** (单次 Prompt) | `cowd prompt "..."` | 非交互式单次问答，支持 `--output-format json` |
| **TUI** (全屏终端) | `cowd --tui` | 基于 ratatui 的终端 UI，多面板布局，支持 Markdown 渲染 |
| **WebUI** (Web 服务) | `cowd serve` | 启动时读取 gateway 配置中的 host/port，CLI `--host`/`--port` 可覆盖 |
| **管道模式** | `echo "..." \| cowd` | 自动检测 stdin 非 TTY 时以一问一答模式运行 |

### 上下文管理

- **预检自动压缩**：在每次模型调用前检测上下文窗口使用率（默认 80% 阈值），超限时自动压缩
- **Prompt Caching**：兼容 Anthropic Prompt Caching 协议，大幅降低 API 成本
- **Token 计数与成本估算**：实时累计输入/输出 token，美元计价成本追踪
- **System Prompt 缓存**：感知配置文件和身份文件变更，自动重建缓存

### Agent 编排

- 50+ 内置工具（文件、Bash、搜索、LSP、TODO、Web 等），按安全等级分类并发执行
- 子 Agent 委派（`task` / `team` / `cron`）支持并行执行，含 token 预算控制
- 智能审批流（`SmartApprovalGate` / `ToolSafetyCategory`）—— 危险操作分类拦截 + 模态确认
- YOLO 模式 —— 跳过所有审批，用于全自动场景
- Worker 生命周期管理 —— 远端 worker boot、trust gate、prompt 交付保障

### Wave 并行编排

基于依赖关系的 Wave 并行执行引擎（`runtime::wave`），将任务按依赖关系自动分组为 Wave：
- **DependencyGraph** — 任务依赖图构建与循环检测
- **WaveOrchestrator** — Wave 构建、调度、执行，同 Wave 内任务并行
- **ErrorPolicy** — 可配置的错误处理策略（停止/跳过/忽略）
- **WaveExecutor** — 可扩展的任务执行器 trait，支持自定义任务实现

适用于：多文件重构、并行代码审查、批量测试执行等场景。

### Gates 质量控制

四层 Gate 机制（`runtime::gates`），在代码变更的关键环节进行质量把关：
- **PreFlightGate** — 执行前检查（lint、类型检查、测试）
- **RevisionGate** — 变更后审查（diff 质量、breaking changes 检测）
- **EscalationGate** — 升级门（超出范围、高风险变更时阻止）
- **AbortGate** — 中止门（检测到严重问题时强制中止）

每个 Gate 生成 `GateResult`（passed / warnings / suggestions / duration_ms），支持自定义 Gate 实现。

---

## 架构总览

```
cowd/
├── crates/
│   ├── cowd-cli/               # 主程序入口：CLI / TUI / Server
│   ├── runtime/               # 运行时核心（64 模块）
│   ├── api/                   # 模型 Provider 适配层
│   ├── tools/                 # 内置工具系统（50+ 工具）
│   ├── commands/              # 斜杠命令 + 技能系统
│   ├── memory/                # 5 层记忆系统（36 模块）
│   ├── config/                # 统一配置管理
│   ├── plugins/               # 插件注册与生命周期
│   ├── telemetry/             # 遥测事件追踪
│   ├── compat-harness/        # 兼容性测试套件
│   └── mock-anthropic-service/ # 模拟 Anthropic 服务（测试用）
├── webui/                     # 浏览器前端（Vanilla JS）
├── scripts/                   # 测试与基准脚本
├── docs/                      # 文档
├── config-default.yaml        # 默认配置文件（带完整注释）
├── Cargo.toml                 # 工作空间配置
└── install.sh                 # 安装脚本
```

### 核心数据流

```
用户输入
    │
    ▼
┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│  CLI/TUI/WebUI  │────▶│ ConversationLoop  │────▶│  Provider API  │
│  接口层         │     │  会话循环 + 工具   │     │  Anthropic/     │
│                 │     │  执行 + 记忆提取   │     │  OpenAI/...    │
└─────────────────┘     └──────────────────┘     └────────────────┘
         │                       │                        │
         │                       ▼                        │
         │               ┌──────────────┐                 │
         │               │   ToolExecutor │◀───────────────┘
         │               │  20+ 内置工具  │   （模型调用工具后）
         │               │  MCP 工具桥接  │
         │               │  插件工具代理  │
         │               └──────┬───────┘
         │                      │
         ▼                      ▼
   ┌──────────┐        ┌──────────────┐        ┌──────────────┐
   │  Session  │        │  Memory System │        │  Platform    │
   │  持久化    │        │  L0-L4 自动提取 │        │  网关        │
   │  SQLite   │        │  AAAK 压缩索引 │        │  飞书/企微/邮件│
   └──────────┘        └──────────────┘        └──────────────┘
```

---

## Crate 架构详解

### `cowd-cli` — 主程序入口

唯一的二进制目标 `cowd`。职责：
- 解析 CLI 参数（支持 `--model` / `--permission-mode` / `--yolo` 等 30+ 标志）
- 管理三种交互模式：REPL、TUI、Server
- 组装运行时组件：ConfigLoader → ConversationRuntime → ToolExecutor → ProviderClient
- 处理会话生命周期：创建、继续、fork、compact、export

关键模块：
| 模块 | 职责 |
|---|---|
| `bootstrap` | 首次引导配置生成 |
| `engine` | 对话引擎核心循环 |
| `render` | Markdown 流式渲染 + 语法高亮 |
| `server` | Axum HTTP 服务（REST API + WebUI + SSE） |
| `tui` | ratatui 终端 UI |
| `init` | 仓库初始化 |

### `runtime` — 运行时核心（62+ 模块）

最大的 crate，涵盖所有运行时基础设施：

| 模块类别 | 模块 | 职责 |
|---|---|---|
| **会话管理** | `session`, `session_control`, `conversation`, `cached_prompt` | 对话循环、消息管理、会话持久化、System Prompt 缓存 |
| **配置** | `config`, `config_validate`, `cowd_dirs` | 5 级优先级配置加载与合并、目录管理 |
| **权限** | `permissions`, `approval_gate`, `policy_engine`, `permission_enforcer` | 三级权限模型 + 审批流 |
| **MCP** | `mcp_server`, `mcp_client`, `mcp_stdio`, `mcp_tool_bridge`, `mcp_lifecycle_hardened`, `mcp` | MCP 协议全栈实现（含生命周期管理、故障降级） |
| **文件操作** | `file_ops`, `doc_ingestion` | 读写/编辑/搜索文件，文档注入与分类 |
| **Git 集成** | `git_context`, `stale_base`, `stale_branch`, `branch_lock` | Git 上下文、base commit 检查、分支锁检测 |
| **工具编排** | `tool_orchestrator`, `subagent_executor`, `subagent`, `task_graph` | 工具调用调度、子 Agent 委派、依赖图 |
| **平台网关** | `platform/` | 飞书/企微/邮件/API Server 适配器 |
| **钩子系统** | `hooks`, `lifecycle_hooks`, `plugin_lifecycle` | Pre/Post 工具钩子 |
| **任务系统** | `task_registry`, `team_cron_registry`, `task_packet` | 后台任务、团队、定时任务 |
| **Wave 编排** | `wave` | 基于依赖关系的 Wave 并行任务执行引擎 |
| **Gates 机制** | `gates` | 提交质量控制门（PreFlight / Revision / Escalation / Abort） |
| **上下文分析** | `context_profiler`, `lane_events` | 会话事件追踪、上下文水位监控 |
| **Agent 系统** | `agent`, `worker_boot`, `subagent_executor` | Agent 生命周期管理、Worker 启动与信任门 |
| **恢复与韧性** | `recovery_recipes`, `green_contract`, `summary_compression` | 故障恢复策略、绿色契约验证、会话摘要压缩 |
| **SSE 与框架** | `sse`, `json`, `bus`, `effect`, `mirror`, `pairing`, `profile` | SSE 解析、JSON 工具、事件总线、Effect 系统、运行时 Profile 等 |
| **安全与沙箱** | `sandbox`, `bash_validation`, `oauth`, `remote`, `trust_resolver` | 沙箱执行、Bash 命令校验、OAuth、远程代理、信任解析 |
| **其他** | `bootstrap`, `compact`, `provider_pool`, `memory_provider`, `prompt`, `usage` | 引导配置、会话压缩、Provider 池、记忆桥接、System Prompt 构建 |

#### Gateway 配置 (`config.rs`)

配置加载遵循严格优先级：
1. **CLI 参数**（最高优先级，如 `--model` `--yolo`）
2. **环境变量**（`COWD_*` 前缀，如 `COWD_MODEL`）
3. **Local 配置**（`.cowd/config.local.yaml`，git-ignored）
4. **Project 配置**（`.cowd/config.yaml`）
5. **User 配置**（`~/.cowd/config.yaml`）

特征配置（`RuntimeFeatureConfig`）包含：hooks、plugins、MCP、OAuth、模型别名、权限模式、审批配置、沙箱、Provider 故障转移、提供者配置、受信根路径、记忆配置、压缩配置、网关配置。

### `api` — 模型 Provider 适配

统一接口 `ProviderClient` + `MessageStream`，多种 Provider 实现：

| 适配器 | 文件 |
|---|---|
| Anthropic | `providers/anthropic.rs` — 原生消息 API + Prompt Caching + SSE |
| OpenAI 兼容 | `providers/openai_compat.rs` — GPT / DeepSeek / Qwen / Grok 等 |
| Provider Chain | `provider_chain.rs` — 故障转移 + 负载均衡 |
| Prompt Cache | `prompt_cache.rs` — Anthropic 兼容的缓存管理 |
| SSE 解析 | `sse.rs` — Server-Sent Events 流式响应解析 |

核心类型：`MessageRequest`、`MessageResponse`、`StreamEvent`、`ToolDefinition`、`ToolResultContentBlock`。

### `tools` — 内置工具系统

定义 50+ 工具规范（`mvp_tool_specs()`），每个工具包含：
- `name` / `description` / `input_schema`
- `required_permission`（ReadOnly / WorkspaceWrite / DangerFullAccess）

**核心工具列表**：

| 类别 | 工具 |
|---|---|
| 文件读写 | `read_file`, `write_file`, `edit_file` |
| 搜索 | `glob_search`, `grep_search`, `ToolSearch` |
| 执行 | `bash`, `PowerShell`, `REPL`, `execute_code` |
| 网络 | `WebFetch`, `WebSearch` |
| 代码智能 | `LSP`（7 种操作：symbols/references/diagnostics/definition/hover） |
| 任务 | `Agent`, `TaskCreate/Get/List/Stop/Update/Output`, `RunTaskPacket` |
| 团队 | `TeamCreate/Delete`, `CronCreate/Delete/List` |
| Worker | `WorkerCreate/Get/Observe/ResolveTrust/AwaitReady/SendPrompt/Restart/Terminate/ObserveCompletion` |
| MCP | `MCP`, `ListMcpResources`, `ReadMcpResource`, `McpAuth` |
| 其他 | `TodoWrite`, `Question`, `AskUserQuestion`, `Skill`, `Config`, `NotebookEdit`, `Sleep`, `SendUserMessage`, `EnterPlanMode`, `ExitPlanMode`, `StructuredOutput`, `RemoteTrigger`, `vision_analyze`, `TestingPermission` |

`GlobalToolRegistry` 统一管理内置工具 + 运行时工具 + 插件工具的去重注册和权限查询。

### `commands` — 斜杠命令与技能系统

**斜杠命令**：定义了 100+ 个斜杠命令（`SlashCommandSpec`），涵盖：

| 类别 | 命令 |
|---|---|
| 会话管理 | `/help`, `/status`, `/model`, `/clear`, `/resume`, `/session`, `/compact`, `/export` |
| 文件与 Git | `/diff`, `/commit`, `/pr`, `/issue`, `/blame`, `/log`, `/stash`, `/branch`, `/files` |
| 开发工具 | `/test`, `/lint`, `/build`, `/run`, `/fix`, `/refactor`, `/explain`, `/docs` |
| 系统管理 | `/config`, `/doctor`, `/upgrade`, `/mcp`, `/agents`, `/skills`, `/plugins`, `/tasks` |
| LSP | `/symbols`, `/references`, `/definition`, `/hover`, `/diagnostics`, `/autofix` |
| 协作 | `/review`, `/share`, `/feedback`, `/copy`, `/paste` |
| 配置 | `/theme`, `/color`, `/effort`, `/fast`, `/output-style`, `/language` |
| Agent | `/agent`, `/subagent`, `/team`, `/cron`, `/plan`, `/parallel` |
| 其他 | `/undo`, `/retry`, `/stop`, `/approve`, `/deny`, `/vim`, `/voice`, `/screenshot` |

所有命令通过 `validate_slash_command_input()` 严格校验参数。解析错误的命令会触发模糊匹配（Levenshtein 距离 + 前缀奖励）推荐最接近的已知命令。

**技能系统**：`SkillManager` 支持技能的安装、卸载、查看、编辑、生成。技能安全扫描（`skill_security`）检测代码注入风险。通过 `skill_manifest` 解析技能清单，支持平台条件检查和前置依赖验证。

### `plugins` — 插件系统

三种插件类型：

| 类型 | 说明 |
|---|---|
| Builtin | 编译内置插件（当前仅 example-builtin） |
| Bundled | 随仓库打包的插件，自动同步到安装目录 |
| External | 从远程（Git URL）或本地路径安装的插件 |

**插件能力**：
- **工具注册**：插件可以声明自己的工具，通过 stdin/stdout 与环境交互
- **生命周期钩子**：`Init` / `Shutdown` — 插件初始化和清理
- **工具钩子**：`PreToolUse` / `PostToolUse` / `PostToolUseFailure`
- **安全沙箱**：插件运行在独立进程中，通过环境变量传递参数
- **锁机制**：插件工具名不能与内置工具冲突

插件清单（`plugin.json`）受严格校验，检测 Claude Code 兼容性缺口（如 `skills`、`mcpServers`、`agents` 等不支持的字段）。

### `memory` — 记忆系统（36 模块）

Cowd 最复杂最精密的子系统。详见下方 [记忆系统](#记忆系统) 章节。

### `config` — 统一配置管理（独立 crate）

与 `runtime` 中的配置层分离的独立配置 crate，提供 `UnifiedConfig` 类型安全的配置加载。支持：
- YAML 配置文件自动发现（已移除 JSON 支持，仅 YAML）
- 深度合并（deep merge），User → Project → Local → 环境变量 → CLI 参数严格优先级
- 环境变量覆盖（`COWD_*` / `CC_*` 前缀）
- Provider 解析（按模型名匹配）
- 规划目录管理（`.cowd/planning/`）：PlanState 的 YAML 序列化
- 完整的错误类型体系（`ConfigError`）
- 与 `runtime::ConfigLoader` 通过 `UnifiedConfigLoader` 兼容桥接

### `telemetry` — 遥测

事件追踪基础设施，支持 `JsonlTelemetrySink` 和 `MemoryTelemetrySink` 两种 sink。

---

## 记忆系统

Cowd 的记忆系统是最具特色的核心能力，采用**多层架构 + 自动提取 + 符号化压缩**的设计。

### 五层记忆架构

```
L0 ─ Identity（身份层）
  ├── 持久化用户身份信息
  ├── 跨会话不变
  └── 初始加载后不再更新

L1 ─ Essential（核心层）
  ├── 高频高优先级记忆
  ├── token 预算控制（默认 800 tokens / 15 条）
  └── 自动提取 + 频率衰减

L2 ─ Project（项目层）
  ├── 项目级上下文和知识
  └── 跨会话共享

L3 ─ Deep Recall（深度召回层）
  ├── 全文搜索历史记忆
  ├── FTS5 全文索引
  └── BM25 + 混合搜索

L4 ─ Shared（共享层）
  ├── 团队/组织共享知识
  ├── 跨 Agent 可见
  └── 可选的，通过 shared_scope 隔离
```

### 自动提取引擎（Background Extractor）

异步运行，零 token 消耗。从对话流中自动提取：
- **命名实体** —— 人物、技术、项目等实体的识别与追踪
- **关系三元组** —— `(subject, predicate, object)` 形式的知识图谱
- **事实陈述** —— 可验证的事实提取
- **核心记忆** —— 根据频率和优先级自动提升到 L1

### AAAK 符号化压缩 (AAAK Index)

专有压缩技术，将记忆注入量减少 70–85%：
- **Abbreviation** — 实体名称缩写（如 "Cowd" 代表 "Cowd AI Coding Framework"）
- **EntityType** — 实体类型分类
- **PriorityItem** — 优先级标记
- **GsdContext/GsdState** — 上下文与状态快照的增量编码

### 认知上下文管理

- **CognitiveContextManager** — 智能构建注入到 system prompt 的记忆上下文块
- **FreshContextManager** — 新鲜度感知的上下文预算分配
- **ContextFence** — 基于规则的记忆过滤，防止注入无关记忆
- **Coherence** — 记忆一致性检查器
- **Drift** — 记忆偏移检测

### 记忆增强模块

- **WriteGuard（写保护）** — `write_guard.rs` 记忆写保护机制，防止腐败注入。审计日志追踪所有写操作来源（`WriteSource`），完整性检查器检测异常（`Anomaly`）并生成报告
- **Closet（指针索引层）** — `closet.rs` 精简指针行索引，实现快速主题路由。为常见查询维护指向相关"抽屉"（记忆条目）的短指针，并应用排名提升（`RANK_BOOSTS`）
- **Miner（多模式挖掘）** — `miner.rs` 三种挖掘模式：Project（项目代码/文档）、Conversations（会话文件）、General（自由文本），支持 .gitignore 感知和自动分类
- **StateRebuilder（状态重建）** — `state_rebuilder.rs` 从持久化的记忆状态重建会话上下文，支持 GSD（GsdState）增量重建
- **HotReload（配置热重载）** — `hot_reload.rs` 监听配置文件变更事件（`ConfigChangeEvent`），自动刷新记忆系统配置
- **EntityRegistry（实体消歧）** — `entity_registry.rs` 对提取出的命名实体进行消歧与合并，避免同义实体（如 "GPT" 和 "ChatGPT"）重复存储
- **TemporalGraph（时序知识图谱）** — `temporal_graph.rs` 为关系三元组添加时间戳维度（`TemporalMarker`），支持时序推理和知识演化追踪，提供时间范围查询（`TemporalQuery`）
- **ContextSync（跨会话同步）** — `context_sync.rs` 在多个并发会话之间同步共享上下文，确保 Project 层（L2）记忆的一致性
- **ContextFence（记忆围栏）** — `context_fence.rs` 基于规则的记忆过滤，防止注入无关记忆。支持 `FenceConfig` 和 `FenceRegistry`，提供 `build_memory_context_block()` 构建注入块
- **FactChecker（事实核查）** — `fact_checker.rs` 对提取的事实进行一致性验证，防止矛盾信息进入记忆存储
- **HandoffManager（交接管理）** — `handoff.rs` 管理跨会话的认知上下文传递，支持任务中断恢复
- **FreshContextManager（新鲜度管理）** — `fresh_context.rs` 新鲜度感知的上下文预算分配，每个会话独立 `SessionTokenBudget`
- **Drift（偏移检测）** — `drift.rs` 检测记忆条目的陈旧度和矛盾，支持自动剪枝
- **SummaryCompression（摘要压缩）** — `runtime::summary_compression` 使用 LLM 对长会话进行语义摘要压缩，降低上下文占用

### Closet 快速路由

Closet（源自 MemPalace 的 closet 概念）是一个精简的指针行索引层，用于快速主题路由。对于常见查询，Closet 维护指向相关"抽屉"的短指针，应用排名提升（`RANK_BOOSTS: [0.40, 0.25, 0.15, 0.08, 0.04]`），无需读取完整数据即可实现高效检索。

### 向量索引

支持通过远程 Embedding API 构建向量索引，提供语义搜索能力。向量维度、模型、API 端点均可配置（`VectorConfig`）。

### 工具沙箱

`ToolOutputSandbox` — 将历史工具执行输出压缩为可检索的快照，不占用上下文窗口。

---

## 工具系统

Cowd 的工具系统分为三层：

### 1. 核心工具（MVP Tool Specs — 始终可用）

```rust
mvp_tool_specs() -> Vec<ToolSpec>
```

包含文件操作、Bash 执行、搜索、网络、Agent 委派、任务管理等最常用工具。始终注入模型上下文窗口。

### 2. 运行时工具（Runtime Tools — 按需注册）

通过 `GlobalToolRegistry::with_runtime_tools()` 注册。适用于动态条件（如项目检测到特定文件结构时注入的 LSP 工具）。

### 3. 插件工具（Plugin Tools — 扩展机制）

插件通过 `plugin.json` 声明自己的工具，由 `PluginManager` 加载并注入注册表。插件工具运行在独立进程中，通过 stdin/stdout + 环境变量交互，提供沙箱隔离。

### 工具搜索

`ToolSearch` 工具支持对延迟工具（deferred tools）进行关键词搜索，包含模糊匹配和排名。当有 MCP 服务器待连接时，搜索结果中会包含待发现工具的状态提示。

### 安全分类与并发控制

`ToolSafetyCategory` 将每个内置工具分为四类，同时控制并发执行：

| 分类 | 示例工具 | 并发策略 |
|---|---|---|
| `ReadOnly` | read, grep, glob, git_status | 无限制并发 |
| `WriteLocal` | write, edit, bash, memory_create | 按文件路径序列化 |
| `Network` | web_search, web_fetch, mcp_call | 限制最大并发数（默认 3） |
| `Destructive` | rm, kill, sudo | 需要显式确认 |

### 权限控制

每个工具有对应的 `PermissionMode`：
- `ReadOnly` — 无害的读取操作
- `WorkspaceWrite` — 工作区写入操作
- `DangerFullAccess` — 危险操作（Bash 执行、网络访问等）

`PermissionEnforcer` + `ApprovalGate`（双层审批门）+ `ToolSafetyCategory` 构成三层防护。YOLO 模式可完全跳过审批。

工具调用结果会自动裁剪（`ToolResultBudget`），支持 HeadOnly / TailOnly / HeadAndTail 三种截断策略。

### 工具执行器（Tool Executor）

工具执行由 `tools` crate 的 `executor.rs` 统一调度，包含：
- **Web 工具**（`web_tools.rs`）：`WebFetch` 和 `WebSearch` 的实现，支持自定义 User-Agent、超时控制
- **PDF 提取**（`pdf_extract.rs`）：PDF 文档文本提取与结构化解析
- **沙箱执行**（`sandbox_exec.rs`）：代码沙箱执行隔离环境
- **Lane 完成管理**（`lane_completion.rs`）：工具调用 lane 的生命周期追踪

---

## 插件与技能系统

### 技能（Skills）

技能是预定义的 prompt 模板 + 条件执行规则，通过斜杠命令或 `Skill` 工具调用。

**技能发现路径**（按优先级）：
- `.cowd/skills/`
- `.agents/skills/`
- `~/.cowd/skills/`
- `~/.cowd/skills/omc-learned/`

**安全扫描**：`scan_skill_content()` 检测代码注入、shell 注入、路径遍历、敏感信息泄露等风险。

**技能清单**（`SkillManifest`）包含：名称、描述、版本、作者、标签、平台条件、前置依赖、环境变量、关联技能。

### 插件（Plugins）

插件是自包含的可扩展模块，可以提供：
- **工具**：通过外部进程执行的自定义工具
- **钩子**：在工具调用前后插入自定义逻辑
- **生命周期**：插件初始化和清理回调

插件市场（`marketplace`）支持发现和安装社区插件。

---

## WebUI 前端

Cowd 的 WebUI 是完全独立的浏览器前端，位于 `webui/` 目录。

### 技术栈

- **零框架依赖**：纯 Vanilla JavaScript，无 React/Vue/Angular
- **响应式状态管理**：`window.State` — 路径键控的订阅-通知模式
- **API 客户端**：`window.Api` — 统一 REST API 封装
- **测试**：Vitest + jsdom（11 个测试）

### 架构模块

| 模块 | 职责 |
|---|---|
| `api.js` | REST API 客户端（会话、记忆、技能、Cron、工作区、审批等） |
| `state.js` | 响应式状态管理（路径监听、变更通知） |
| `boot.js` | 应用启动与初始化 |
| `ui.js` | 主 UI 渲染 |
| `panels.js` | 面板系统（激活/切换/布局） |
| `messages.js` | 消息流渲染 + SSE 流式响应 |
| `sessions.js` | 会话列表管理 |
| `commands.js` | 斜杠命令面板 |
| `workspace.js` | 工作区文件浏览器 |
| `sw.js` | Service Worker（离线缓存/PWA） |
| `style.css` | 完整样式系统 |
| `index.html` | 应用入口 |
| `manifest.json` | PWA 清单 |

### WebUI 面板

- **Chat** — 消息对话流，支持 SSE 实时流式响应
- **Sessions** — 会话列表管理
- **Memory** — 记忆查看/搜索/编辑（L0/L1/L3 层可视化）
- **Skills** — 技能市场浏览和安装管理
- **Crons** — 定时任务管理
- **Workspace** — 文件浏览器
- **Approval** — 审批队列管理
- **Commands** — 斜杠命令补全与执行
- **Platforms** — 平台网关状态

### REST API 端点

WebUI 通过 REST API 与后端通信，主要端点：

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/sessions` | GET/POST | 会话 CRUD |
| `/api/sessions/:id/messages` | GET/POST | 消息管理 |
| `/api/sessions/:id/messages/stream` | GET | SSE 流式响应 |
| `/api/memory/*` | GET/POST/PATCH/DELETE | 记忆系统全部操作 |
| `/api/config` | GET/PUT | 配置读写 |
| `/v1/skills/*` | GET/POST/DELETE | 技能管理 |
| `/api/crons/*` | GET/POST/DELETE | 定时任务 |
| `/api/workspace/*` | GET/POST | 工作区文件操作 |
| `/api/approval/*` | GET/POST | 审批管理 |
| `/api/platforms` | GET | 平台网关状态 |

---

## 配置体系

> 配置格式仅支持 **YAML**（已移除 JSON 支持）。

### 配置文件优先级

1. **CLI 参数** — `--model` `--permission-mode` `--yolo` `--reasoning-effort` 等
2. **环境变量** — `COWD_MODEL` `ANTHROPIC_API_KEY` `COWD_PERMISSION_MODE` `COWD_AUTO_COMPACT_INPUT_TOKENS` 等
3. **Local 配置** — `.cowd/config.local.yaml`（git-ignored）
4. **Project 配置** — `.cowd/config.yaml`
5. **User 配置** — `~/.cowd/config.yaml`

### 主要配置项

```yaml
# 默认模型
model: "claude-sonnet-4-6"

# 模型别名
aliases:
  main: "claude-sonnet-4-6"
  fast: "claude-haiku-4-5-20251213"

# 自定义 Provider
providers:
  deepseek:
    base_url: "https://api.deepseek.com"
    api_key: "sk-..."
    models: ["deepseek-v4-pro", "deepseek-v4-flash"]

# 上下文压缩（百分比阈值，默认 80%）
compression:
  threshold_percent: 80
  buffer_tokens: 8000

# 权限模式
permissions:
  defaultMode: "acceptEdits"  # plan | acceptEdits | dontAsk

# 记忆系统
memory:
  enabled: true

# 网关配置
gateway:
  enabled: true
  platforms:
    - type: api_server
      enabled: true
      host: "127.0.0.1"
      port: 8642
```

详见 `config-default.yaml`（128 行，包含所有可配置项的完整注释）。

---

## 平台网关

Cowd 的 Gateway 子系统允许通过多种平台与 AI Agent 交互：

| 平台 | 状态 | 说明 |
|---|---|---|
| **API Server** | ✅ 完整实现 | REST API + WebUI + SSE |
| **飞书机器人** | ✅ 完整实现 | 消息/文档/评论处理 + 规则引擎 |
| **企业微信** | ✅ 实现 | WeCom 消息适配 |
| **邮箱** | ✅ 实现 | SMTP 发送 + IMAP 接收 |

平台适配器位于 `crates/runtime/src/platform/`，遵循统一的 `PlatformAdapter` trait。

---

## 快速开始

```bash
# 编译
cargo build --release

# CLI 交互模式
cowd

# TUI 全屏终端模式
cowd --tui

# 启动 Web 服务（浏览器访问 http://localhost:8642）
cowd serve --port 8080

# 非交互式单次提问
cowd prompt "解释这个项目"

# 管道输入
echo "列出当前目录文件" | cowd

# 指定模型
cowd --model deepseek-v4-pro "写一个排序函数"

# YOLO 模式（跳过所有审批）
cowd --yolo prompt "自动修复所有 lint 错误"

# 继续已保存会话并执行斜杠命令
cowd --resume latest /status

# 指定 reasoning effort（仅支持模型）
cowd --reasoning-effort high prompt "分析这个架构"

# JSON 输出格式
cowd prompt "当前 Git 状态" --output-format json

# 导出会话
cowd export --session latest --output session.md
```

### 安装

```bash
# 从源码编译
cargo install --path .

# 或使用安装脚本
./install.sh

# 首次启动会自动引导生成配置
cowd
```

---

## 开发指南

### 开发环境要求

- Rust 1.80+
- Node.js 18+（仅 WebUI 测试需要）
- SQLite 3（bundled，无需系统安装）

### 本地开发

```bash
# 运行测试（推荐逐 crate 运行，避免内存溢出）
cargo test -p api -p commands -p tools -p runtime -p plugins

# Clippy 检查
cargo clippy --workspace --all-targets

# 格式化
cargo fmt --all -- --check

# WebUI 测试
cd webui && npm test

# 单 crate 详细测试输出
cargo test -p memory -- --nocapture

# 构建文档
cargo doc --no-deps --open
```

### 构建配置

项目使用 `workspace.lints` 统一 lint 配置：
- `unsafe_code = "forbid"` — 禁止 unsafe 代码
- clippy `all` 为 warn，`pedantic` 为 allow

编译并行度限制为 4 任务（`.cargo/config.toml`），防止内存溢出。

### CI/CD

兼容性测试套件（`compat-harness`）提供与上游（Claude Code）的 manifest 提取和路径兼容性验证。

---

## 测试状态

| Crate | 测试数 | 覆盖内容 |
|---|---|---|
| runtime | 569 | 核心运行时（会话、压缩、权限、MCP、配置、子 Agent 编排） |
| memory | 252 | 记忆系统各层、AAAK 压缩、实体提取、时序图谱、上下文同步 |
| cowd-cli | 193 | CLI 参数解析、输出格式、会话恢复、Mock 一致性 |
| api | 140 | Provider 适配、SSE 解析、类型序列化、Provider Chain |
| tools | 109 | 工具执行、安全分类、权限校验、MCP 桥接 |
| commands | 51 | 斜杠命令解析、技能管理、安全扫描 |
| plugins | 39 | 插件安装/卸载/启用/禁用/更新/市场 |
| config（独立 crate） | 34 | 配置加载、合并、环境变量覆盖、规划目录 |
| compat-harness | 3 | Manifest 提取与路径兼容性 |
| telemetry | 3 | 事件追踪 serialization |
| WebUI | 11 | 状态管理、API 客户端（Vitest） |
| **总计** | **~1400+** | |

---

## 许可与致谢

### 许可证

MIT License — 详见项目根目录 LICENSE 文件。

### 设计渊源

Cowd 的设计借鉴了 AI 编程助手领域的最佳实践，结合 Rust 的性能优势进行重新实现。核心设计理念包括：

- **零模板化**：代码生成优先于配置
- **上下文效率**：AAAK 符号化压缩、预检自动压缩、Prompt Caching 三重优化
- **实体感知记忆**：命名实体消歧、时序知识图谱、跨会话上下文同步
- **可扩展性**：插件系统 + MCP 协议 + Provider Chain + 技能市场
- **安全优先**：四级安全分类（ToolSafetyCategory）+ 三级权限模型 + 审批流 + 沙箱执行 + 安全扫描

---

> **Cowd** — 让 AI 编程助手更快、更智能、更可控。
