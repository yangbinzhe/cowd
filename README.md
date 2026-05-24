# COWD — AI 编程智能体框架

> **Rust 原生高性能 AI 编程智能体** | 273 源文件 · 173K 行 · 524 测试
> CLI / TUI / WebUI · 内存系统 · 代码图谱 · MCP · 插件系统

---

## 项目规模

| Crate | 文件 | 行数 | 职责 |
|-------|------|------|------|
| `cowd-cli` | 68 | 50,749 | 主程序：CLI / TUI / Server 入口 |
| `runtime` | 81 | 45,131 | 运行时核心：会话、工具、权限、MCP、Gates |
| `memory` | 61 | 32,411 | 36模块内存系统 + 代码图谱 + 知识图谱 |
| `tools` | 8 | 10,633 | 50+ 内置工具规范 |
| `commands` | 7 | 8,926 | 100+ 斜杠命令 + 技能系统 |
| `api` | 13 | 7,939 | 多 Provider 模型适配层 |
| `plugins` | 3 | 4,288 | 插件注册与生命周期 |
| `config` | 2 | 2,162 | 统一配置管理 |
| 其他 | 5 | 1,814 | 遥测、兼容测试、Mock 服务 |
| **总计** | **273** | **173,053** | |

---

## TUI 终端界面

### 组件系统 (19 组件, ~31K 行)

```
┌─────────────────────────────┬──────────────────┐
│  ChatView (70%)             │  侧边栏 (30%)     │
│  · 消息时间线               │  Context | Changes │
│  · 流式渲染 + 虚拟滚动      │  Todo | Diff       │
│  · 搜索高亮                 │  Files | Sessions  │
│                             │  ── 6 个标签页     │
├─────────────────────────────┴──────────────────┤
│  Prompt (自动补全 + frecency + @file 引用)       │
├────────────────────────────────────────────────┤
│  状态栏 (模型 / Token / MCP / LSP / 权限)       │
└────────────────────────────────────────────────┘
+ Toast · Dialog · CommandPalette · WhichKey
+ ThinkingPanel · AgentsOverlay · QuestionForm
```

| 能力 | 实现 | 能力 | 实现 |
|------|------|------|------|
| Component trait 抽象 | `base.rs` | Diff Viewer (unified/split) | `diff_viewer.rs` |
| LayoutTree 布局引擎 | `layout/` | Command Palette (Ctrl+P) | `command_palette.rs` |
| Keybind + Which-Key | `keybind/` | Session Sidebar | `session_sidebar.rs` |
| Dialog (Alert/Confirm/Select/Prompt) | `dialog.rs` | File Tree Browser | `file_tree.rs` |
| Toast 通知 | `toast.rs` | Export Dialog | `export_dialog.rs` |
| Theme Engine (热加载) | `theme/` | Session Fork | `session_sidebar.rs` |
| Prompt 自动补全 (frecency) | `prompt.rs` | Multi-Stage Permission | `dialog.rs` |
| Question 多问题表单 | `question_form.rs` | Sub-Agent 导航 | `chat_view.rs` |
| Startup Loading | `state.rs` | Clipboard (OSC52 + 图像) | `clipboard.rs` |
| Infinite Scroll | `app.rs` | ANSI 颜色降级 | `ansi_fallback.rs` |
| Animation 引擎 | `animation.rs` | Error Recovery | `error_recovery.rs` |
| Accessibility (WCAG AA) | `accessibility.rs` | Perf Profiler | `profiler.rs` |

---

## 内存系统 (36 模块 · 32K 行 · 524 测试)

内存子系统是 cowd 的知识核心——一套多智能体、分层感知的知识管理系统，实现知识从流入、存储、索引、检索、演化、衰减到重建的完整闭环。

### 核心架构：三维模型

| 维度 | 枚举值 | 说明 |
|------|--------|------|
| **范围(Scope)** | Global / Project / Session / Agent | 知识所属的隔离域，每项目独立 SQLite 存储 |
| **层级(Layer)** | L0 Identity / L1 Essential / L2 Project / L3 Deep / L4 Shared | 记忆的分层存储与检索策略 |
| **状态(State)** | Stable / Transient / Rotting / Archived | 记忆的生命周期状态及老化策略 |

每维正交组合形成一个记忆定位点，系统据此决定写入位置、检索策略、压缩优先级和衰减速率。

### 五层存储

```
L0 Identity   — 用户身份信息，跨所有会话不变
L1 Essential  — 热记忆(15条) + 热符号(5个代码槽位)，容量饱和时驱逐低频项
L2 Project    — 项目级上下文(~3000 token) + 项目知识图谱 + 逐字存储
L3 Deep       — SQLite FTS5 全文检索 + 时序知识图谱 + 符号↔对话关联
L4 Shared     — 多智能体共享知识 + 冲突检测 + 共识裁决
```

### 知识生命周期

```
  流入                        感知                        演化
  ┌─────────┐    ┌─────────────────────┐    ┌───────────────────┐
  │ Extractor│───▶│ prepare_context()  │───▶│ KnowledgeGraph    │
  │ Miner    │    │ · Peer Perception  │    │ · Entity dedup    │
  │ Verbatim │    │ · Session Resume   │    │ · Temporal edges  │
  │ Tool     │    │ · Fresh Context    │    │ · Coherence check │
  │ Sandbox  │    │ · Hot Topics       │    │ · Drift detection │
  └─────────┘    │ · Conflict Detect  │    └───────────────────┘
                 └─────────────────────┘           │
                                                   ▼
  检索                        衰减                重建
  ┌─────────┐    ┌─────────────────────┐    ┌───────────────────┐
  │ FTS5    │    │ ContextRot          │    │ StateRebuilder    │
  │ BM25    │◀───│ · Token window      │───▶│ · Recover L0-L3   │
  │ Hybrid  │    │ Drift               │    │ Handoff protocol  │
  │ Embed   │    │ · Stale KG cleanup  │    └───────────────────┘
  └─────────┘    └─────────────────────┘
```

### 36 个模块详解

#### 调度中枢 (3)

| 模块 | 文件 | 职责 |
|------|------|------|
| **CognitiveContextManager** | `cognitive.rs` | 统一入口门面，编排 `prepare_context()`(13步，含 L0+L2 预加载→BM25 恢复→同伴感知→L4 查询→L3 混合→种子压缩→热主题→代码注入) 和 `on_turn_end()`(14步，含提取→工具沙箱→微观→会话→漂移→种子→tick→KG 陈旧→跨存储校验→壁橱→向量→KG 持久化→腐烂→种子保存→批量嵌入) 完整生命周期 |
| **MemoryOrchestrator** | `orchestrator.rs` | 顶层协调器：`remember()` 带记忆分层写入和 OnceLock 缓存正则(~10,000×提速)，`recall_peer_context()` 跨会话同伴召回，`detect_conflict()` 冲突检测触发 |
| **SessionManager** | `session_manager.rs` | 统一会话管理，支持会话创建、切换、fork、搜索、续接 |

#### 范围隔离 (1)

| 模块 | 文件 | 职责 |
|------|------|------|
| **ProjectScopeManager** | `project_scope.rs` | 每项目独立 SQLite 存储(`memory_{hash}.db`)，范围感知(Global/Project/Session/Agent)查询过滤，`unified_scan()` 单次目录遍历双输出：正则知识图谱实体(14+语言+文档+配置+前端) + tree-sitter 代码符号 |

#### 代码智能 (3)

| 模块 | 文件 | 职责 |
|------|------|------|
| **CodeIndexer** | `code_indexer.rs` | tree-sitter 嵌入式 AST 引擎：5 语言解析(Rust/Python/TypeScript/Go/Java)，提取函数/类/方法/接口/结构体/枚举，生成调用/导入/继承/实现边，增量索引+文件指纹变更检测 |
| **ProjectKG** | `entity.rs` | 知识图谱：Entity 节点(函数/类/模块等，带 `source_type` 标记运行时/空间/MCP/代码库)，Triple 边(调用/导入/继承/定义/引用，带时间戳+Agent归属) |
| **HotSymbols** | 内置 | L1 5 个热符号槽位，文件访问后自动提升为热符号，LLM 调用前自动注入 |

#### 检索系统 (4)

| 模块 | 文件 | 职责 |
|------|------|------|
| **FTS5 Search** | `store/sqlite.rs` | 全量 SQLite FTS5 全文搜索，支持范围限定查询 |
| **BM25 Session Resume** | `session_resume.rs` | BM25 算法重排序会话历史，`prepare_context()` Step 2a 注入最相关的 L3 条目 |
| **FreshContextManager** | `fresh_context.rs` | 新鲜上下文管理：80% 预算给当前轮次核心意图，20% 给辅助上下文 |
| **RelevanceScorer** | `relevance.rs` | 多信号相关性评分：语义相似度(BM25 向量混合)+时间衰减+来源信任度，动态加载最相关记忆 |

#### 知识提取 (2)

| 模块 | 文件 | 职责 |
|------|------|------|
| **Extractor** | `extractor.rs` | 后台实体+关系自动提取，`on_turn_end()` Step 0 将对话内容解析为结构化知识写入 KG |
| **Miner** | `miner.rs` | 多模式知识挖掘：项目扫描(文件结构+依赖+配置)、对话挖掘(决策+模式+约束)、通用文本提取 |

#### L4 共享层 (1)

| 模块 | 文件 | 职责 |
|------|------|------|
| **SharedMemoryManager** | `shared.rs` | 跨会话/跨 Agent 同步：`recall_peers()` 5 分钟窗口同伴召回，`recall_peers_realtime()` 无窗口实时感知，团队/项目/全局多级查询，热主题聚合 |

#### 版本控制与审计 (7)

| 模块 | 文件 | 职责 |
|------|------|------|
| **VerbatimSink** | `cognitive.rs`/`project_scope.rs` | 逐字存储——原始观察内容永久保存，永不覆盖或摘要化，支持精确恢复 |
| **WriteGuard** | `write_guard.rs` | 写保护：控制来源(L0 用户/L1 系统/Agent/外部)可写哪些层，所有写入审计日志 |
| **Drift** | `drift.rs` | 陈旧度检测：KG 三元组时间戳衰减评分，超过阈值标记为腐烂触发重建 |
| **ContextRot** | `context_rot.rs` | 上下文窗口健康监控：跟踪 Token 水位线，向 Agent 反馈窗口压力告警和驱逐建议 |
| **StateRebuilder** | `state_rebuilder.rs` | 状态重建：在上下文丢失或会话中断后，从持久化存储恢复 L0→L3 层级状态 |
| **Handoff** | `handoff.rs` | 跨会话交接协议：序列化当前上下文状态包，支持→new session 无缝迁移 |
| **Seeds** | `seeds.rs` | 种子决策线程：标记关键决策点，支持回溯、展开和分支探索 |

#### 压缩与路由 (3)

| 模块 | 文件 | 职责 |
|------|------|------|
| **AAAK** | `aaak_compression.rs` | 自适应缩写知识压缩：符号化压缩 70-85%，保留语义完整性 |
| **AAAK Index** | `aaak_index.rs` | 符号指针索引：记忆条目的快速符号路由层 |
| **Closet** | `closet.rs` | 紧凑指针行索引：快速主题路由，无需全文扫描即可定位相关记忆簇 |

#### 一致性保障 (4)

| 模块 | 文件 | 职责 |
|------|------|------|
| **FactChecker** | `fact_checker.rs` | 事实一致性：`detect_conflict()` 三信号仲裁(置信度×0.4 + 新近度×0.3 + Agent 权重×0.3)，`detect_consensus()` 3+ Agent 一致则提权至 0.95 |
| **Coherence** | `coherence.rs` | 记忆连贯性评分：Jaccard 相似度评估记忆-上下文相关性，标识不一致簇 |
| **Entity Registry** | `entity_registry.rs` | 实体消歧合并：同名实体指纹匹配，跨存储实体一致性维护 |
| **ContextFence** | `context_fence.rs` | 上下文围栏：基于规则的记忆隔离，确保跨会话引用不会引入无关上下文 |

#### 运行时辅助 (5)

| 模块 | 文件 | 职责 |
|------|------|------|
| **ToolSandbox** | `tool_sandbox.rs` | 工具输出沙箱：内存 FTS5 索引 + 摘要替换，防止工具原始输出污染上下文窗口 |
| **TemporalGraph** | `temporal_graph.rs` | 时序知识图谱：实体-关系三元组按时间轴组织，支持时间窗口查询和趋势分析 |
| **Splitter** | `splitter.rs` | 语义分割器：在语义边界(主题切换、段落结束、代码/文本切换)分割文本 |
| **ContextSync** | `context_sync.rs` | 跨会话上下文同步：在 Agent 团队间共享活跃上下文片段 |
| **Embedding** | `embedding.rs` | 远程嵌入客户端：对接外部嵌入服务实现向量化检索 |

### 13 步上下文准备 (prepare_context)

| 步骤 | 动作 | 数据源 |
|------|------|--------|
| 1 | L0 身份 + L2 项目预加载 | ProjectScope SQLite |
| 2a | BM25 会话恢复(重排序) | SQLite FTS5 |
| 2b | P1 项目知识图谱注入 | KnowledgeGraph |
| 2c | 同伴感知(5 分钟窗口) | L4 Shared |
| 2c2 | 实时同伴感知(无窗口) | L4 Shared |
| 2d | 双 L4 查询(团队+项目) | L4 Shared |
| 3 | L3 深度回忆 + 混合排序 | BM25 + Embedding |
| 4 | 种子决策线程注入 | Seeds |
| 5 | 上下文压力评估 | ContextRot |
| 5b | 新鲜度预算切换 | FreshContextManager |
| 6 | AAAK 压缩 + Closet 路由 | AAAK + Closet |
| 6b | 热主题聚合 | L4 Shared |
| 7 | tree-sitter 代码注入 | CodeIndexer |

### 14 步轮次结束 (on_turn_end)

| 步骤 | 动作 |
|------|------|
| 0 | Extractor/Miner 知识提取 |
| 0b | ToolSandbox 工具输出索引 |
| 1 | L1 微观记忆写入 |
| 2 | L3 会话持久化 |
| 3 | Drift 陈旧度更新 |
| 4 | Seeds 决策点标记 |
| 5 | Tick 计数器递增 |
| 5a | KG 陈旧实体清理 |
| 5a2 | 周期性 KG 重建 |
| 5a3 | 跨存储一致性校验(含 Coherence) |
| 5b | Closet 更新 |
| 6 | Embedding 异步生成 |
| 7 | KG 边持久化 |
| 8 | ContextRot 窗口维护 |
| 9 | Closet 持久化 |
| 10 | Seeds 持久化 |
| 11 | Batch 嵌入写入 |

### 关键设计决策

- **Per-Project SQLite**: 每项目独立 `memory_{hash}.db`，杜绝跨项目记忆污染，Global 范围保持单实例共享
- **OnceLock 正则缓存**: `remember()` 热路径正则从 30 次编译降为 3 次(OneLock)，实测 10,000× 加速
- **统一单遍扫描**: `unified_scan()` 一次 walkdir 同时产出 KG 实体(正则 14+ 语言)和代码符号(tree-sitter 5 语言)，避免两次 IO 遍历
- **三元信号仲裁**: 冲突检测使用置信度(0.4)+新近度(0.3)+Agent 权重(0.3)加权裁决，3+ Agent 一致自动共识提权
- **Agent 角色预算**: Planner 40%/Executor 25%/Reviewer 15% L4 写入预算，防止单一 Agent 主宰共享知识
- **逐字永存(Verbatim)**: 原始内容永久保存，永不覆盖或摘要化，确保 LLM 调用前能从原始记录精确恢复

---

## 运行时核心 (81 文件 · 45K 行)

| 类别 | 模块 |
|------|------|
| **会话** | session, session_control, conversation, cached_prompt, summary_compression |
| **权限** | permissions, approval_gate, permission_enforcer, policy_engine |
| **MCP** | mcp_server, mcp_client, mcp_stdio, mcp_tool_bridge, mcp_lifecycle_hardened |
| **工具** | tool_orchestrator, tool_dispatch, subagent_executor, task_graph |
| **Gates** | gates (PreFlight/Revision/Escalation/Abort + Impact Analysis) |
| **Wave** | wave (依赖图+并行编排引擎) |
| **平台** | platform (API Server/飞书/企微/邮件) |
| **Agent** | agent, worker_boot, task_registry, team_cron_registry |
| **其他** | sandbox, effect, bus, mirror, pairing, profile, recovery_recipes |

---

## Provider 支持

| Provider | 方式 |
|----------|------|
| Anthropic (Claude) | 原生 · OAuth + API Key · Prompt Caching |
| OpenAI 兼容 | GPT / OpenRouter / Ollama / StepFun |
| DeepSeek | reasoning_content 回传 |
| Qwen / DashScope | 前缀路由 |
| Grok / xAI | 模型别名 |
| Kimi / Moonshot | 前缀路由 |
| 自定义 | config.yaml 任意 OpenAI 兼容接口 |

---

## 快速开始

```bash
# 编译
cargo build --release

# TUI 终端模式（默认）
cowd

# 单次问答
cowd prompt "解释这个项目"

# 指定模型
cowd --model deepseek-v4-pro "写一个排序函数"

# 启动 Web 服务
cowd serve --port 8080

# 继续会话
cowd --resume latest
```

---

## 开发

```bash
# 测试
cargo test -p cowd-cli     # 812 tests
cargo test -p cowd-memory  # 524 tests
cargo test --workspace     # 1000+ tests

# 编译
cargo build --release      # → target/release/cowd (~28MB)
```

---

## 许可证

MIT License
