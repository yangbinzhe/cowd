# COWD — AI Agent Runtime

> **Rust 原生 AI 运行时框架 + 结构化运营智能系统**
> v0.9.103 · IACC · Unified Daemon Runtime · SQLite Session Source-of-Truth
> TUI/WebUI Control · Connector Runtime · Cross-plane Governance · MCP/Feishu/Local Service

---

## 系统架构总览

COWD 是一个七层异构系统：下层承载 AI 智能体的基础设施（会话、记忆、编排、安全），上层嫁接结构化运营智能（IACC）用于企业运营决策。

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         VII.  产品交互层  (Product Layer)                      │
│  Command Center ▏ Personal Cockpit ▏ Incident Room ▏ Report ▏ Case UI        │
├──────────────────────────────────────────────────────────────────────────────┤
│                          VI.  接入层  (Access Layer)                          │
│     CLI · TUI(9/9 panels) · WebUI · Gateway(HTTP:8642) · Feishu · WeCom      │
├──────────────────────────────────────────────────────────────────────────────┤
│                      V.  Cowd 统一运行时层  (Cowd Runtime)                     │
│ ┌────────────┐ ┌──────────┐ ┌─────────────────┐ ┌──────────────────────┐    │
│ │ Session    │ │ Agent    │ │ Context         │ │ Channel/Permission   │    │
│ │ Kernel     │ │ WorkGraph│ │ Runtime         │ │ PolicyEngine         │    │
│ │ Lifecycle  │ │ Wave     │ │ StableHeader    │ │ CrossPlanePolicy     │    │
│ │ LeaseMgr   │ │ SubAgent │ │ Snapshot/Diff   │ │ Identity/Grant/Audit │    │
│ └─────┬──────┘ └────┬─────┘ └───────┬─────────┘ └───────────┬──────────┘    │
│       └──────────────┴──────────────┼────────────────────────┘               │
│                                     ▼                                        │
│ ┌──────────────────────────────────────────────────────────────────────┐    │
│ │                      Memory System (3D: Scope × Layer × State)        │    │
│ │  10 Groups · 36 Modules · CodeIndexer(tree-sitter) · Universal Scan   │    │
│ └──────────────────────────────────────────────────────────────────────┘    │
├──────────────────────────────────────────────────────────────────────────────┤
│               IV.  Connector 运行时  (Connector Runtime)                      │
│  ConnectorRegistry · CapabilityManifest · ResourceDirectory · Feishu Plane   │
├──────────────────────────────────────────────────────────────────────────────┤
│ ┌──────────────────────────────────────────────────────────────────────┐    │
│ │                III.  IACC 结构化运营智能  (Operating Intelligence)     │    │
│ │                                                                        │    │
│ │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐  │    │
│ │  │ Attention │  │ Evidence  │  │ Quality   │  │ Analysis          │  │    │
│ │  │ Item      │─▶│ Packet    │─▶│ Gate      │─▶│ Attribution       │  │    │
│ │  │ Priority  │  │ Budget    │  │ 6-dim     │  │ ImpactPropagation │  │    │
│ │  └───────────┘  └───────────┘  └───────────┘  └─────────┬─────────┘  │    │
│ │                                                         ▼              │    │
│ │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐  │    │
│ │  │ Incident  │  │ Execution │  │ Cockpit   │  │ Recovery          │  │    │
│ │  │ Lifecycle │──│ dry_run/  │──│ Profile/  │──│ Monitor           │  │    │
│ │  │ TaskBridge│  │ commit    │  │ Report    │  │ MemoryCase/Play   │  │    │
│ │  └───────────┘  └───────────┘  └───────────┘  └───────────────────┘  │    │
│ └──────────────────────────────────────────────────────────────────────┘    │
├──────────────────────────────────────────────────────────────────────────────┤
│ ┌──────────────────────────────────────────────────────────────────────┐    │
│ │               II.  IACC 结构化认知层  (Structured Cognition)          │    │
│ │                                                                        │    │
│ │  ┌───────┐  ┌───────┐  ┌───────┐  ┌───────────┐  ┌───────────────┐  │    │
│ │  │ Entity│  │Relation│  │ Metric│  │MetricGraph│  │Compute        │  │    │
│ │  │Resolve│──│Graph  │  │Define │──│Dependency │──│Job/Plan       │  │    │
│ │  │Alias  │  │Impact │  │State  │  │Lineage    │  │Incremental    │  │    │
│ │  └───────┘  └───────┘  └───────┘  └───────────┘  └───────────────┘  │    │
│ └──────────────────────────────────────────────────────────────────────┘    │
├──────────────────────────────────────────────────────────────────────────────┤
│              I.  IACC 数据处理层  (Data Processing)                           │
│  SourceSnapshot · Fact(Ingest+Dedup) · Change(Deduplicate) · DomainSeed      │
├──────────────────────────────────────────────────────────────────────────────┤
│                        底层企业系统  (Enterprise Systems)                     │
│    ERP · MES · PLM · WMS · SRM · QMS · Excel · DB · API · RPA               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 项目规模

| Crate | 文件数 | 行数 | 职责 |
|-------|--------|------|------|
| `cowd-cli` | 70+ | ~47K | 主程序入口：CLI/TUI/Server/Gateway/daemon |
| `runtime` | 79+ | ~45K | 运行时核心：会话、编排、安全、MCP、Connector、IACC |
| `memory` | 61 | ~32K | 36 模块记忆系统 + 代码图谱 + 通用知识扫描 |
| `tools` | 8 | ~10K | 50+ 内置工具规范 |
| `commands` | 7 | ~9K | 100+ 斜杠命令 + 技能系统 |
| `api` | 13 | ~8K | 多 Provider 模型适配层（Anthropic/OpenAI/DeepSeek/Qwen） |
| `plugins` | 3 | ~4K | 插件注册与生命周期管理 |
| `config` | 2 | ~2K | 统一配置管理（YAML-only） |
| 其他 | 5 | ~2K | 遥测、兼容测试、Mock 服务 |
| **总计** | **~250+** | **~170K** | |

---

# 第一部分：Cowd 运行时基础设施

## 1. 统一 Daemon Runtime

v0.9.67-v0.9.75 重构后，Cowd 收束为**统一 daemon runtime** 架构：

```
                 ┌─────────────────────────────┐
                 │       Unified Daemon         │
                 │   ┌─────────────────────┐    │
                 │   │    RuntimeHandle     │    │
                 │   │  (Unified Entry)    │    │
                 │   └─────────┬───────────┘    │
                 │             │                │
                 │   ┌─────────▼───────────┐    │
                 │   │  ┌───────┐ ┌───────┐│    │
                 │   │  │Socket │ │ HTTP  ││    │
                 │   │  │Control│ │Proj.  ││    │
                 │   │  │Plane  │ │Plane  ││    │
                 │   │  └───┬───┘ └───┬───┘│    │
                 │   └──────┼─────────┼────┘    │
                 └──────────┼─────────┼─────────┘
                            │         │
              ┌─────────────▼──┐  ┌──▼──────────────┐
              │  TUI Control   │  │  WebUI / HTTP    │
              │  (Unix Socket) │  │  (Projection)    │
              │  P95 <20ms     │  │  P95 <150ms      │
              │  + SSE stream  │  │  + REST + SSE    │
              └────────────────┘  └──────────────────┘
```

### Socket Control Plane — 核心指令协议

本地控制面使用 Unix domain socket，支持 12 类指令：

| 指令 | 作用 |
|------|------|
| `runtime.status` | 查询 daemon 健康状态和模块就绪度 |
| `session.ensure` | 确保 session 存在（创建或复用） |
| `session.attach` | 客户端 attach 到已有 session |
| `session.detach` | 客户端 detach |
| `session.lease.acquire` | 获取 session 写入租约 |
| `session.lease.release` | 释放租约 |
| `session.chat` | 发送对话 turn |
| `task.start` | 启动异步任务 |
| `agent.dispatch` | 派发 Agent 协作消息 |
| `memory.query` | 查询记忆系统 |
| `context.snapshot` | 获取上下文快照 |
| `channel.execute` | 执行渠道特定操作 |

每条指令携带：`protocol_version`, `request_id`, `session_id`, `actor`, `timeout_ms`, `idempotency_key`。
每条响应返回：`ok`, `request_id`, `event_sequence`, `payload`, `error.kind`, `error.retryable`。

### HTTP Projection Plane — REST API + Event Stream

WebUI/远程访问走 HTTP 投影面，Gateway 提供：
- 静态 WebUI 资源服务
- REST 投影 API（`/api/runtime/*`, `/api/memory/*`, `/api/context/*` 等）
- Server-Sent Events 实时流
- Health/Readiness 探针

---

## 2. 会话生命周期

Session 是 Cowd 运行时的**根对象**，所有 agent/memory/context/task/channel 操作都挂靠到 session。

```
  ┌──────────────────────────────────────────────────────┐
  │                  Session Lifecycle                    │
  │                                                      │
  │  ┌──────────┐   attach   ┌──────────────────────┐   │
  │  │ CLI/TUI  │───────────▶│                       │   │
  │  └──────────┘            │   ActiveSessions      │   │
  │  ┌──────────┐   attach   │   (Hot Runtime Cache) │   │
  │  │ WebUI    │───────────▶│                       │   │
  │  └──────────┘            └───────────┬───────────┘   │
  │                                      │ fan-out        │
  │  ┌──────────┐   attach              ▼                │
  │  │ Feishu   │───────────▶ ┌──────────────────────┐   │
  │  └──────────┘             │ SessionEventBus       │   │
  │                           │ (Multi-Frontend Sync) │   │
  │                           └───────────┬───────────┘   │
  │                                       │               │
  │  ┌────────────────────────────────────▼───────────┐   │
  │  │              SessionKernel                      │   │
  │  │  ┌─────────┐  ┌──────────┐  ┌──────────────┐  │   │
  │  │  │ Lease   │  │ Event    │  │ State        │  │   │
  │  │  │ Manager │  │ Replay   │  │ Machine      │  │   │
  │  │  └────┬────┘  └────┬─────┘  └──────┬───────┘  │   │
  │  └───────┼────────────┼───────────────┼──────────┘   │
  │          ▼            ▼               ▼              │
  │  ┌──────────────────────────────────────────────┐    │
  │  │          UnifiedSessionStore (SQLite)         │    │
  │  │   session/message/event/snapshot/memory      │    │
  │  └──────────────────────────────────────────────┘    │
  └──────────────────────────────────────────────────────┘
```

**关键规则**：
- SQLite/DB 是 session 运行态**唯一事实源**（不再依赖 JSONL）
- 同一 session 可被多个前端同时 attach，通过 Lease 控制写入权
- Event Replay 保证新 attach 的客户端能追上历史事件

---

## 3. 多智能体编排

### AgentWorkGraph

```
  Task Reception
       │
       ▼
  Task Decomposition  ←── Agent Roles (researcher/reviewer/merger/executor)
       │
       ▼
  ┌───────────────────────────────────┐
  │         Wave Engine               │
  │                                   │
  │  Wave 1: [Task A] [Task B]       │  并行执行
  │       │        │                  │
  │       └───┬────┘                  │
  │           ▼                       │
  │  Wave 2: [Task C] [Task D]       │  依赖完成后并行
  │           │        │              │
  │           └───┬────┘              │
  │               ▼                   │
  │  Wave 3:      [Task E]           │  最终汇总
  └───────────────────────────────────┘
```

### SubAgent 约束模型

| 约束维度 | 限制 |
|----------|------|
| 工具列表 | 仅分配必要工具（如只读文件、写特定目录） |
| WriteGuard | 限制写入 L3/L4，不可写 L0/L1 |
| Token 预算 | 默认 20K 上限 |
| 超时控制 | `timeout_secs` 可配置 |
| 结果汇总 | 执行结果回流到父 Agent |

---

# 第二部分：认知基础设施 — Memory System

## 1. 3D 记忆架构（Scope × Layer × State）

```
          Scope轴 (知识属于谁)
     Global ─── Project ─── Session ─── Agent
           \        |         |       /
            ═══◎ 记忆定位点 ◎═══
           /        |         |       \
     L0 ── L1 ──── L2 ───── L3 ──── L4
          Layer轴 (知识如何存储)
               ↑
            State轴 (知识处于什么阶段)
        Stable ─── Transient ─── Rotting ─── Archived
```

每个知识条目被三维坐标定位，系统据此决定：

| 操作 | 决策逻辑 |
|------|----------|
| **写** | 确定目标 SQLite 存储、Layer、Write Strategy |
| **读** | 确定 FTS5/BM25/Embedding 检索范围、重排序策略 |
| **衰减** | Stable 永不压缩，Transient 逐代降级，Rotting 标记重建，Archived 冻结 |

**五层含义**：
- **L0 (Identity)**：Agent 是谁、角色定义、权限边界
- **L1 (Essential)**：基础设施知识、关键架构决策、不可变项目上下文
- **L2 (Project)**：项目代码结构、API 文档、业务逻辑
- **L3 (Deep)**：会话历史、工具调用结果、推理链
- **L4 (Shared)**：跨 Agent/跨会话共享共识（peer perception + hot topics）

## 2. 36 模块 10 组分类

| 组 | 模块 | 职责 | 关键连接 |
|----|------|------|----------|
| **调度中枢** | Cognitive, Orchestrator, SessionManager | 所有生命周期入口，编排记忆读/写 | ConversationRuntime → Cognitive → Orchestrator |
| **范围隔离** | ProjectScopeManager | 每项目独立 SQLite，决定知识可见边界 | 所有 Scope=Project 的读写都经此路由 |
| **代码智能** | CodeIndexer, ProjectKG, HotSymbols | tree-sitter AST → KG → L1 热槽 | prepare_context() 时注入当前文件结构 |
| **检索引擎** | FTS5, BM25, FreshContext, Relevance | 4 维度召回，混合排序 | Extractor/Miner 写入 → 检索 → prepare_context |
| **知识提取** | Extractor, Miner, ToolSandbox | 对话/工具输出/文件 → 结构化知识 | on_turn_end → Extractor → Miner → KG |
| **共享层** | SharedMemoryManager | L4 跨 Agent/跨会话中介 | peer perception + hot_topics → prepare_context |
| **审计控制** | VerbatimSink, WriteGuard, Drift, ContextRot | 写控制、衰减、清理 | WriteGuard 检查所有写操作 |
| **重建恢复** | StateRebuilder, Handoff, Seeds | 会话中断恢复、决策回溯、交接 | Session 恢复时重建状态 |
| **压缩路由** | AAAK, AAAK Index, Closet | 70-85% 压缩率 + 主题指针索引 | ContextRot → AAAK → Closet 索引 |
| **一致性** | FactChecker, Coherence, EntityRegistry, ContextFence | 多 Agent 知识不冲突/不重复 | Writer → FactChecker → Coherence 校验 |

## 3. 检索管道

```
  用户提问 / 工具调用
       │
       ▼
  ┌──────────────────────────────────────────────┐
  │          prepare_context()                   │
  │                                              │
  │  ┌──────┐  ┌──────┐  ┌──────┐  ┌─────────┐ │
  │  │FTS5  │  │BM25  │  │Embed │  │HotSymbol│ │  四路并行召回
  │  │全文  │  │关键词 │  │语义  │  │CodeL1   │ │
  │  └──┬───┘  └──┬───┘  └──┬───┘  └────┬────┘ │
  │     │          │          │           │      │
  │     └──────────┴──────────┴───────────┘      │
  │                    │                         │
  │                    ▼                         │
  │           ┌──────────────┐                   │
  │           │ HybridRank   │  混合排序去重      │
  │           └──────┬───────┘                   │
  │                  ▼                           │
  │           ┌──────────────┐                   │
  │           │ Context      │  Token budget 约束 │
  │           │ Assembly     │  + 13 步注入       │
  │           └──────────────┘                   │
  └──────────────────────────────────────────────┘
```

## 4. CodeIndexer — 代码结构理解

```
  文件变更 / 初次索引
       │
       ▼
  tree-sitter AST 解析 (Rust / Python / JS / TS / Go)
       │
       ▼
  ┌──────────────────────────────┐
  │     ProjectKG (代码图谱)     │
  │  ┌────────┐  ┌────────────┐  │
  │  │Function│──│CALLS       │  │
  │  │Class   │──│EXTENDS     │  │
  │  │Method  │──│IMPLEMENTS  │  │
  │  │Import  │──│IMPORTS     │  │
  │  └────────┘  └────────────┘  │
  └──────────────┬───────────────┘
                 ▼
  ┌──────────────────────────────┐
  │      HotSymbols (L1 热槽)    │
  │  当前打开文件 + 热点调用链    │
  └──────────────┬───────────────┘
                 ▼
         prepare_context()
```

---

# 第三部分：安全与管控

## 1. 四层防护模型

```
  ┌──────────────┐
  │ 1. 权限判定   │  PermissionMode: read-only / workspace-write / danger-full
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ 2. Gate 流水线│  PreFlight → Approval → Revision → Escalation → Abort
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ 3. WriteGuard │  控制 L0/L1/Agent/External 写入权限，全审计日志
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ 4. Sandbox   │  Linux Sandbox: 容器隔离 + 文件系统隔离 + 网络限制
  └──────────────┘
```

## 2. Gate 流水线详解

| Gate | 触发时机 | 行为 |
|------|----------|------|
| **PreFlightGate** | 执行前 | 检查影响范围，触发 impact analysis |
| **ApprovalGate** | PreFlight 后 | 根据 PolicyEngine 规则自动批准或转人工 |
| **RevisionGate** | 执行中 | 检测修改是否超出预期，触发修正建议 |
| **EscalationGate** | 风险上升 | 重新评估权限，升级审批 |
| **AbortGate** | 硬终止 | 回滚到上一个安全状态 |

## 3. Cross-plane PolicyEngine

```
  Action Request
       │
       ▼
  PolicyEngine 检查:
  ┌─────────────────────────────────┐
  │ 1. 高风险动作 → 必须审批        │
  │ 2. 外部系统写 → 必须 idempotent │
  │ 3. 无 Evidence 动作 → 禁止      │
  │ 4. 无目标实体绑定 → 禁止        │
  │ 5. 高影响无回滚计划 → 禁止      │
  │ 6. 跨组织动作 → 记录责任链      │
  │ 7. 低置信度归因 → 仅生成人工任务 │
  └─────────────────────────────────┘
       │
       ▼
  Decision: allow / allow_with_approval / dry_run_only / deny / needs_more_evidence
```

---

# 第四部分：Connector 运行时

## 1. Connector 注册与发现

```
  ┌─────────────────────────────────┐
  │     ConnectorRegistry           │
  │  ┌───────────┐ ┌─────────────┐  │
  │  │ Accounts  │ │ Capabilities│  │
  │  │ (Feishu/  │ │ (read/write │  │
  │  │  WeCom/   │ │  /probe/    │  │
  │  │  Email)   │ │  execute)   │  │
  │  └─────┬─────┘ └──────┬──────┘  │
  │        └───────────────┘         │
  │                ▼                 │
  │      ┌──────────────────┐        │
  │      │ ResourceDirectory │        │
  │      │ (File/Doc/Data)   │        │
  │      └──────────────────┘        │
  └─────────────────────────────────┘
```

## 2. 飞书只读证据平面

```
  Feishu API
      │
      ▼
  Connector Registry → Capability: readonly
      │
      ▼
  Resource Promotion → Memory (L2/L3)
      │
      ▼
  Evidence Bridge → Context Runtime
```

## 3. MCP Operator 控制台

```
  MCP Server (stdio/SSE/remote)
      │
      ▼
  mcp_client.rs → tool registry
      │
      ▼
  mcp_tool_bridge.rs → Cowd tool dispatch
      │
      ▼
  WebUI MCP console (operator management)
```

---

# 第五部分：IACC 结构化运营智能

IACC（智能体认知架构）是 Cowd 的可选结构化认知子系统，处理企业运营数据的摄入、计算、推理、行动和呈现。

## 1. 完整认知链路（21 步全链）

```
  SourceSnapshot
       │
       ▼
  EntityResolution → Entity + SourceKey registry
       │
       ▼
  FactIngest (SHA-256 dedup) → IaccAttentionItem 自动生成
       │
       ▼
  MetricRecompute (affected scope 增量) ← MetricGraph (dependency/lineage)
       │
       ▼
  ChangeEvent (delta severity) → Anomaly (多信号)
       │
       ▼
  Attribution (entity-relation graph reasoning)
       │
       ▼
  ImpactPropagation (BFS entity graph traversal)
       │
       ▼
  Attention (priority_score × severity × urgency)
       │
       ▼
  EvidencePacket (bounded context, token_budget)
       │
       ▼
  QualityGate (6-dim scoring: pass >=0.75 / review >=0.45 / fail)
       │
       ▼
  Incident (open → AgentGraph bridge)
       │
       ▼
  OperationalAnalysis (attribution + impact + recommended actions)
       │
       ▼
  CrossPlaneAction (dry_run → preflight → approval → commit → receipt)
       │
       ▼
  ExecutionReceipt (bridge receipt + audit_record)
       │
       ▼
  Feedback (outcome → auto-close incident 或 re-attribute)
       │
       ▼
  RecoveryMonitor (主指标+保护指标 double-check, observation window)
       │
       ▼
  MemoryCase / Playbook (case promotion 6 rules)
       │
       ▼
  CockpitUpdate (Profile → Projection → Report → Delivery)
```

## 2. 17 模块五层架构

### 数据层

| 模块 | 文件 | 核心类型 | 作用 |
|------|------|----------|------|
| **source** | `source.rs` (56行) | `IaccSourceSnapshot`, `IaccSourceKind` | 6 种数据源（API/DB/File/RPA/Manual/Connector）快照 |
| **entity** | `entity.rs` (88行) | `IaccEntity`, `IaccSourceKey` | 跨系统实体统一，规范化 key 匹配 |
| **fact** | `fact.rs` (103行) | `IaccFact`, `IaccFactInput` | 运营事实记录，SHA-256 内容 hash 去重 |
| **domain** | `domain.rs` (735行) | `IaccDomainPack`, `IaccDomainSeedPlan` | 服务器制造领域包（22实体/14关系/8指标/3场景） |

### 指标层

| 模块 | 文件 | 核心类型 | 作用 |
|------|------|----------|------|
| **metric** | `metric.rs` (95行) | `IaccMetricDefinition`, `IaccMetricState` | 指标定义（domain/grain/formula/threshold）+ 状态快照 |
| **metric_graph** | `metric_graph.rs` (77行) | `IaccMetricDependency`, `IaccMetricLineage` | 指标依赖图（上下游 typed transformation） |
| **change** | `change.rs` (35行) | `IaccChangeEvent` | 指标变动事件（from/to/delta/severity） |
| **compute** | `compute.rs` (72行) | `IaccComputeJob`, `IaccComputePlan` | 增量计算引擎（trigger_fact_type → affected_metric_ids） |

### 认知层

| 模块 | 文件 | 核心类型 | 作用 |
|------|------|----------|------|
| **attention** | `attention.rs` (76行) | `IaccAttentionItem`, `IaccSeverity` | 注意力管理（priority × severity × urgency） |
| **relation** | `relation.rs` (72行) | `IaccRelation`, `IaccImpactTrace` | 实体关系图 + BFS 影响传播 |
| **evidence** | `evidence.rs` (97行) | `IaccEvidencePacket`, `IaccEvidenceSourceRef` | 有界证据包（token_budget）→ ContextItem 桥接 |
| **quality** | `quality.rs` (98行) | `IaccQualityGateDecision` | 证据质量门（6维评分 pass/review/fail） |

### 行动层

| 模块 | 文件 | 核心类型 | 作用 |
|------|------|----------|------|
| **incident** | `incident.rs` (33行) | `IaccIncident` | 运营事件生命周期（open→analysis→execution→closed） |
| **analysis** | `analysis.rs` (314行) | `IaccOperationalAnalysis`, `IaccRecommendedAction` | 归因分析 + 影响路径 + 推荐处置 |
| **execution** | `execution.rs` (220行) | `IaccActionExecution`, `IaccCrossPlaneBridgeReceipt` | 治理执行（dry_run/commit + feedback + cross-plane receipt） |

### 呈现层

| 模块 | 文件 | 核心类型 | 作用 |
|------|------|----------|------|
| **cockpit** | `cockpit.rs` (560行) | `IaccCockpitProfile/Projection/Report/Delivery` | 驾驶舱投影、报告快照、多渠道投递 |

### 持久化

| 模块 | 文件 | 核心类型 | 作用 |
|------|------|----------|------|
| **store** | `store.rs` (3294行) | `IaccStore`, `IaccHealth`, `IaccMetricRecomputeResult` | 15 张 SQLite 表，完整 CRUD + 10 集成测试 |

## 3. IaccStore 表结构（15 张表）

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  source_snapshots│     │  facts           │     │  entities        │
│  (source_system, │     │  (fact_type,     │     │  (entity_type,   │
│   business_period)│    │   measures JSON)  │     │   source_keys)   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  attention_items │     │  metric_states   │     │  relations       │
│  (priority_score,│     │  (value, delta,  │     │  (relation_type, │
│   severity)      │     │   status)        │     │   entity_a/b)    │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  evidence_packets│     │  metric_defs    │     │  metric_deps     │
│  (bound context, │     │  (domain, grain │     │  (upstream_id,   │
│   confidence)    │     │   formula)      │     │   downstream_id) │
└────────┬────────┘     └────────┬────────┘     └──────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  incidents       │     │  compute_jobs    │
│  (status,        │     │  (status,        │
│   task_id)       │     │   metric_ids)    │
└────────┬────────┘     └──────────────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  analyses        │     │  executions      │     │  quality_gates   │
│  (attributions,  │     │  (mode, status,  │     │  (score,         │
│   impact_paths)  │     │   xplane_receipts)│     │   decision)      │
└────────┬────────┘     └────────┬────────┘     └──────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  cockpit_profiles│     │  cockpit_reports │
│  (focus_refs,    │     │  (projection,    │
│   cadence)       │     │   deliveries)    │
└─────────────────┘     └─────────────────┘
```

## 4. 核心数据流示例 — 短缺风险处理

```
  数据源输入: ERP 库存 < 安全库存
      │
      ▼
  SourceSnapshot(ERP, Api, weekly) → schema_version=1, row_count=5000
      │
      ▼
  EntityResolution:
    "MAT-0042" → IaccEntity(entity_type="component", canonical_key="gpu-h100-80gb")
      │
      ▼
  FactIngest: fact_type="inventory_level", measures={"on_hand": 120, "safety": 200}
      │ SHA-256 hash: 3a2f...
      ▼
  MetricRecompute: trigger → affected_metric_ids=["inventory_coverage", "supply_risk"]
      │
      ▼
  MetricGraph traversal:
    inventory_coverage ← BOM_req ─→ supply_risk ─→ prod_plan_feasibility
      │
      ▼
  ChangeEvent: entity=MAT-0042, metric=inventory_coverage, delta=-40%, severity=critical
      │
      ▼
  Attention: priority_score=0.92, severity=Critical, reason_codes=["safety_stock_breach"]
      │
      ▼
  EvidencePacket: metric_evidence + change_evidence + source_refs, confidence=0.85
      │
      ▼
  QualityGate: score=0.88 → decision=pass (>=0.75)
      │
      ▼
  Incident(title="GPU H100 库存跌破安全线", status=open)
      │
      ▼
  AgentRunGraph bridge: researcher(分析归因) → reviewer(审查方案) → merger(汇总)
      │
      ▼
  OperationalAnalysis:
    attributions = [IaccAttributionCandidate(cause_type="supplier_delay", confidence=0.78)]
    impact_paths = [BOM→server_config→customer_order]
    recommended_actions = [IaccRecommendedAction(type="expedite_order", priority=P0)]
      │
      ▼
  CrossPlaneAction(action=expedite_order, mode=dry_run → preflight → commit)
      │
      ▼
  ExecutionReceipt: IaccCrossPlaneBridgeReceipt(bridge_id="SRM-001", status=committed)
      │
      ▼
  Feedback: outcome="order_expedited", metric_delta={"inventory_coverage": "recovering"}
      │
      ▼
  Incident close: status=closed

  全程 Cockpit 实时更新 → Report 投递飞书
```

---

# 第六部分：接入层

## 入口模式

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │                          COWD Entry Points                           │
  │                                                                      │
  │  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │
  │  │ CLI     │  │ TUI      │  │ Gateway  │  │ WebUI (bundled)      │ │
  │  │ cowd    │  │ --solo   │  │ run      │  │ http://127.0.0.1:8642 │ │
  │  │ prompt  │  │ 9/9 panel│  │ HTTP:8642│  │                      │ │
  │  │ --resume│  │          │  │ +Socket  │  │                      │ │
  │  └────┬────┘  └────┬─────┘  └────┬─────┘  └──────────┬───────────┘ │
  │       └─────────────┴────────────┼────────────────────┘             │
  │                                  │                                   │
  │                                  ▼                                   │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │                    Feishu / WeCom / Email                     │   │
  │  │                  (双向消息通道 → EventBus)                    │   │
  │  └──────────────────────────────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────────────────┘
```

## TUI 9/9 面板

```
  ┌──────────────┬──────────────────────────────────────────────┐
  │              │                                              │
  │  Sessions    │           ChatView (主对话区)                 │
  │  (会话列表)   │                                              │
  │              │    [User] 提问...                            │
  ├──────────────┤    [Agent] 回答...                           │
  │              │    [Tool] 工具输出...                         │
  │  File Tree   │                                              │
  │  (文件树)    ├──────────────────────────────────────────────┤
  │              │                                              │
  ├──────────────┤  ┌─────────┐ ┌──────────┐ ┌──────────────┐  │
  │              │  │ Context │ │ Approval │ │ DiffView     │  │
  │  Memory      │  │ (上下文) │ │ (审批)    │ │ (差异视图)   │  │
  │  (记忆面板)  │  └─────────┘ └──────────┘ └──────────────┘  │
  │              │  ┌─────────┐ ┌──────────┐ ┌──────────────┐  │
  ├──────────────┤  │ Agent   │ │ Connector│ │ Task         │  │
  │  StatusBar   │  │ Team    │ │ Console  │ │ Workbench    │  │
  │  (状态栏)    │  └─────────┘ └──────────┘ └──────────────┘  │
  └──────────────┴──────────────────────────────────────────────┘
```

---

# 第七部分：模块交互矩阵

## Crate 间依赖关系

```
                ┌─────────┐
                │ cowd-cli │  ← Gateway, TUI, daemon, api_routes
                └────┬─────┘
                     │ 依赖
         ┌───────────┼───────────┐
         ▼           ▼           ▼
    ┌────────┐ ┌────────┐ ┌──────────┐
    │runtime │ │memory  │ │commands  │
    │(核心)  │ │(记忆)  │ │(100+cmd) │
    └───┬────┘ └───┬────┘ └────┬─────┘
        │          │           │
        ▼          ▼           ▼
    ┌────────┐ ┌────────┐ ┌──────────┐
    │ tools  │ │ api    │ │ plugins  │
    │ (50+)  │ │ (LLM)  │ │ (动态)   │
    └───┬────┘ └───┬────┘ └────┬─────┘
        │          │           │
        └──────────┼───────────┘
                   ▼
            ┌────────────┐
            │  config    │
            │  (YAML)    │
            └────────────┘
```

## 核心模块间的数据流关系

| 源模块 | 目标模块 | 交互方式 | 关键函数/类型 |
|--------|----------|----------|---------------|
| ConversationRuntime | Memory::Cognitive | 调用 | `prepare_context()` 注入上下文 |
| Memory::Orchestrator | ConversationRuntime | 回调 | `on_turn_end()` 写入本轮记忆 |
| ToolDispatch | Gate Pipeline | 拦截 | PreFlight → Approval 检查 |
| Gate Pipeline | PolicyEngine | 查询 | `evaluate(action, context)` → Decision |
| ToolDispatch | Sandbox | 委派 | 沙箱隔离执行 |
| AgentWorkGraph | Wave Engine | 编排 | TaskGraph 依赖解析 + 并行执行 |
| Wave Engine | SubAgent | 派发 | 受限子任务执行 |
| EventBus | ConversationRuntime | 注入 | 外部事件（飞书/API/CLI）驱动 |
| IaccStore | Cowd Memory | 桥接 | Evidence → ContextItem 转化 |
| IaccEvidencePacket | Context Runtime | 注入 | `to_context_item()` → 上下文 |
| ConnectorRegistry | Memory::Shared | 提升 | Resource 推入记忆 L2/L3 |
| SessionKernel | UnifiedSessionStore | 读写 | Session CRUD + Lease |
| SessionKernel | SessionEventBus | 广播 | 多前端 fan-out |

---

# 第八部分：启动与开发

## 启动方式

```bash
# 编译
cargo build --release          # → target/release/cowd (~28MB)

# TUI 终端模式
cowd                           # 新建会话，全功能 TUI
cowd --solo                    # 显式别名
cowd --resume latest           # 续接最近会话
cowd --resume <id>             # 续接指定会话
cowd --tui                     # 显式 TUI 启动

# API 网关服务
cowd gateway run               # 前台 (HTTP:8642 + Unix Socket + 飞书)
cowd gateway start             # systemd 后台
cowd gateway stop              # 停止
cowd gateway status            # 状态

# 安装部署
cowd install --systemd         # → ~/.cowd/bin/cowd + systemd

# 信息
cowd version                   # 版本
cowd help                      # 帮助
```

## IACC 专用操作

```bash
# 播种服务器制造领域数据
cowd iacc seed-manufacturing

# 查看 Cockpit 投影
curl http://127.0.0.1:8642/api/iacc/cockpit/projection?profile_id=default

# 生成驾驶舱报告
curl -X POST http://127.0.0.1:8642/api/iacc/cockpit/reports \
  -H "Content-Type: application/json" \
  -d '{"profile_id":"default","cadence":"daily"}'

# 列出活跃事件
curl http://127.0.0.1:8642/api/iacc/incidents?status=open

# Connector 管理
curl http://127.0.0.1:8642/api/connectors/summary
curl http://127.0.0.1:8642/api/connectors/accounts
curl http://127.0.0.1:8642/api/connectors/capabilities
curl http://127.0.0.1:8642/api/connectors/resources
```

## 开发

```bash
cargo test -p cowd-memory        # Memory: 456+ tests
cargo test --workspace           # 全量: 1000+ tests

# 验证脚本
scripts/validate.sh fast         # 快速验证
scripts/validate.sh core         # 核心验证
scripts/validate.sh full         # 全量验证
scripts/validate.sh live         # 实时场景验证
scripts/validate.sh release      # 发版验证
```

## 日志

```bash
RUST_LOG=debug cowd --solo
tail -f ~/.cowd/logs/cowd.$(date +%Y-%m-%d)
```

---

# 当前状态评估

## 已完成核心能力

| 领域 | 完成度 | 关键指标 |
|------|--------|----------|
| 内存系统 36 模块 | 100% | 456 测试，3D 架构（Scope×Layer×State），14+ 语言扫描 |
| 统一 Daemon Runtime | 95% | Socket control plane + HTTP projection plane |
| Session Lifecycle | 95% | SessionKernel + Lease + EventReplay，多端 attach |
| Gate 流水线 | 90% | 5 种 Gate + PolicyEngine + 7 条强制策略 |
| Multi-Agent | 85% | AgentWorkGraph + Wave + SubAgent |
| Connector Runtime | 85% | Feishu/MCP/本地服务，Capability & Resource 注册 |
| TUI | 95% | 9/9 侧边栏全功能，socket 控制传输 |
| IACC 数据层 | 100% | Entity/Relation/Fact/Source/Domain 完整链路 |
| IACC 指标层 | 70% | MetricGraph + Compute(增量) 存在，缺 CDC/partition |
| IACC 认知层 | 90% | Attention/Evidence/Quality/Analysis 完整闭环 |
| IACC 行动层 | 40% | Execution receipt 存在，缺完整 Cross-plane bridge |
| IACC 呈现层 | 60% | Cockpit Profile/Report/Delivery 完整，缺 CommandCenter/IncidentRoom |
| IACC 智能化层 | 0% | Recovery Monitor/MemoryCase/Playbook/Agent Skills 全部缺失 |

## 存在薄弱环节

1. **IACC 行动闭环缺少完整治理**：Cross-plane receipt 存在，但 ActionPlan → PolicyDecision → multi-connector dispatch 链路不足。
2. **IACC 智能化层空白**：Recovery Monitor、MemoryCase/Playbook、Agent Skill System 未经实现。
3. **内存-运行时尚未 push 模式**：CognitiveContextManager 是"调用-返回"，非"订阅-推送"。
4. **TUI 未全面 daemon 驱动**：保留直连逻辑，socket protocol 未固化。
5. **性能基线无 CI 护栏**：1K/10K/20K 手动运行，无自动化回归。

---

# 下一阶段演进

## IACC 主线：补全生产试运行条件

```
P0: Cross-plane Bridge 完整化 → Recovery Monitor → Command Center API
P1: MemoryCase/Playbook → Anomaly Detection → Incident Room → Metric Engine 强化
P2: Agent Skill System → Personal Cockpit → IACC WebUI
P3: Scale benchmark → Multi-tenant → DAO connector → Production package
```

## Cowd 主线：从被动到主动

1. **记忆 2.0**：后台知识流持续摄入，预测性预取，语义压缩
2. **蜂群 1.0**：多 Agent 通过 L4 实时协商，加权投票决策
3. **Gates 2.0**：自动修正 Gate，影响预测反馈，事务式回滚执行
4. **运维 1.0**：CI 性能门禁，内存剖析仪表盘，自适应存储策略
5. **Platform 2.0**：双向事件驱动（Webhook → 记忆注入，异常 → 主动推送）

---

## 许可证

MIT License
