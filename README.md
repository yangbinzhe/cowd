# COWD — AI Agent Runtime

> **Rust 原生 AI 运行时框架** | 当前版本 v0.9.96
> 统一 daemon · SQLite session source of truth · Runtime Control Plane
> TUI/WebUI 管控 · Connector Runtime · Cross-plane 权限治理 · MCP/飞书/本地服务接入

---

## 当前主线：v0.9 AI Runtime Release

v0.9 的目标是把 runtime control-plane、session lease、connector account/capability/resource、cross-plane governance、TUI/WebUI 控制台和统一场景门禁收束为可交付版本。

这一阶段不再只做局部 API 或 UI，而是让系统能回答四个生产问题：

- 当前 daemon 是否具备运行条件，哪些模块 blocked/degraded？
- 当前有哪些外部账号、服务能力、MCP 能力和资源引用可被 AI 使用？
- 跨渠道、跨服务动作是否有 identity、grant、policy、audit 和 receipt？
- TUI/WebUI/API 是否展示同一事实源，且可被脚本自动验收？

```
RuntimeCommand -> RuntimeEventLog -> RuntimeProjection
              -> AgentWorkGraph
              -> MemoryPulseConsumer
              -> ContextPolicyEngine
              -> ConnectorRegistrySnapshot
              -> TUI/WebUI Runtime Cockpit
```

这条主线要求 Session、Agent、Task、Tool、Memory、Context、Policy、UI 全部围绕同一事实源协作。SQLite/DB 是运行态事实源；JSONL 只保留为显式导入、导出和 debug bundle 格式。

### 关键入口

```bash
cowd gateway run
curl http://127.0.0.1:8642/api/runtime/control-plane
curl http://127.0.0.1:8642/api/connectors/summary
curl http://127.0.0.1:8642/api/connectors/accounts
curl http://127.0.0.1:8642/api/connectors/capabilities
curl http://127.0.0.1:8642/api/connectors/resources
scripts/validate.sh scenario
scripts/validate.sh release
```

Operator 文档见 [docs/operator/v0.9.42-connector-runtime-operator-checklist.md](docs/operator/v0.9.42-connector-runtime-operator-checklist.md)。

## 项目规模

| Crate | 文件 | 行数 | 职责 |
|-------|------|------|------|
| `cowd-cli` | 70 | ~47K | 主程序：CLI / TUI / Server / Gateway 入口（新增 gateway.rs 101行, api_routes.rs 230行） |
| `runtime` | 81 | 45,131 | 运行时核心：会话、工具、权限、MCP、Gates |
| `memory` | 61 | 32,411 | 36模块内存系统 + 代码图谱 + 知识图谱 |
| `tools` | 8 | 10,633 | 50+ 内置工具规范 |
| `commands` | 7 | 8,926 | 100+ 斜杠命令 + 技能系统 |
| `api` | 13 | 7,939 | 多 Provider 模型适配层 |
| `plugins` | 3 | 4,288 | 插件注册与生命周期 |
| `config` | 2 | 2,162 | 统一配置管理 |
| 其他 | 5 | 1,814 | 遥测、兼容测试、Mock 服务 |
| **总计** | **274** | **~172K** | |

---

## 设计哲学

cowd 解决一个核心问题：**AI 编程智能体如何拥有可靠、持久、可进化的认知能力？**

不是提示词工程，也不是工具集合。cowd 是一套**智能体运行时系统**——它不回答"这一轮该说什么"，而是回答"这个智能体如何存在、如何感知、如何安全行动、如何记住、如何学习"。

核心信念：

1. **智能体需要记忆架构，而不只是上下文窗口**。窗口是瞬态的，记忆是持久的。没有记忆的智能体每轮对话都是第一次见面，永远无法理解项目的深层脉络。
2. **知识必须分层，不能平铺**。身份(L0)不应与项目细节(L2)混在一起，对话历史(L3)不应与共享共识(L4)共同竞争上下文预算。分层是认知的基础设施。
3. **安全不是策略，是架构**。权限不是事后检查，而是写死在 Gate 流水线和 WriteGuard 中的不可绕过控制。
4. **智能体应能理解代码结构，而不仅仅是文本**。tree-sitter AST 代码索引让 cowd 理解函数边界、调用关系、继承层次——不依赖 LLM 的幻觉推断。
5. **多智能体需要共享知识，但必须防冲突**。L4 共享层 + 三元信号仲裁让多个 Agent 协作时不会互相覆盖，而是形成加权共识。

---

## 五大子系统 · 有机连接

### 1. 对话运行时 (Conversation Runtime) — 循环中枢

这是驱动每一轮智能体交互的核心循环。它不只是一个 LLM 调用封装，而是**上下文准备 → LLM 调用 → 工具执行 → 记忆写入**的完整闭环。

核心流程：

```
  用户输入
      │
      ▼
  prepare_context()     ← 内存系统注入：身份/项目/代码/同伴/历史/种子
      │
      ▼
  LLM 流式推理          ← Provider 适配层 + Prompt Caching
      │
      ▼
  流式回传用户          ← TUI 渲染 / Server SSE 推送
      │
      ▼
  工具调用 (tool_call)  ← MCP 桥接 → 权限 Gate → 沙箱执行
      │
      ▼
  on_turn_end()         ← 记忆写入/提取/漂移/嵌入/压缩/持久化
```

**关键连接**：
- 对话运行时依赖 **内存系统** 的 `CognitiveContextManager.prepare_context()` 构建上下文，依赖 `on_turn_end()` 写入本轮收获
- 工具调用经过 **权限系统** 的 Gate 流水线（PreFlight → Approval → Revision）才能执行
- 复杂工具链由 **Wave 编排引擎** 拆解为并行任务图，每个子任务可委派给 **SubAgent**
- 运行时可被 Platform 事件(飞书消息/邮件/Webhook)触发

### 2. 内存系统 (Memory System) — 智能体的大脑

cowd 的核心原创。36 个模块组成的认知架构，不是数据库，不是缓存，而是一套**完整的知识生命周期管理系统**。

三维架构：

```
         Scope轴 (知识属于谁)
    Global ─── Project ─── Session ─── Agent
          \        |         |       /
           ───◎ 记忆定位点 ◎───
          /        |         |       \
    L0 ── L1 ──── L2 ───── L3 ──── L4
         Layer轴 (知识如何存储)
              ↑
           State轴 (知识处于什么阶段)
       Stable ─── Transient ─── Rotting ─── Archived
```

每个知识条目都被这三个轴定位，系统据此决定：
- **写**：写入哪个 SQLite 存储？进入哪一层？用什么写策略？
- **读**：什么范围可用 FTS5？什么范围需要 BM25 重排序？什么范围直接注入？
- **衰减**：Stable 永不压缩，Transient 逐代降级，Rotting 标记重建，Archived 冻结

**内存不是配角，而是主架构**。整个智能体的感知、决策、行动都围绕它展开：

```
  ┌─────────────────────────────────────────────────────────┐
  │                    记忆生命周期                          │
  │                                                         │
  │  流入层        感知层         演化层         检索层       │
  │  Extractor──▶ prepare──▶ KnowledgeGraph──▶ FTS5/BM25   │
  │  Miner       context()     TemporalGraph   Embeddings   │
  │  ToolOutput  13步注入       Coherence       HybridRank   │
  │  Verbatim    Dual L4       ConflictDetect               │
  │              代码注入       EntityDedup                  │
  │                            DriftUpdate                  │
  │                                  │                      │
  │                                  ▼                      │
  │                       衰减层           重建层             │
  │                       ContextRot──▶ StateRebuilder      │
  │                       KG Stale       HandoffProtocol    │
  │                       CrossStoreVerify                  │
  └─────────────────────────────────────────────────────────┘
```

22 个核心模块按角色划分为 10 组，但真正的架构不是模块列表，而是**它们之间的数据流**：

| 组 | 模块 | 连接关系 |
|----|------|----------|
| 调度中枢 | Cognitive, Orchestrator, SessionManager | 所有生命周期的入口和编排者 |
| 范围隔离 | ProjectScopeManager | 每项目独立 SQLite，决定知识的可见边界 |
| 代码智能 | CodeIndexer, ProjectKG, HotSymbols | tree-sitter AST → KnowledgeGraph → L1 热槽注入 → prepare_context |
| 检索引擎 | FTS5, BM25, FreshContext, Relevance | 从 4 个不同维度召回知识，混合排序后注入上下文 |
| 知识提取 | Extractor, Miner, ToolSandbox | 对话/工具输出/文件 → 结构化知识，on_turn_end 入口 |
| 共享层 | SharedMemoryManager | 跨 Agent/跨会话的 L4 中介，peer perception + hot topics |
| 审计控制 | VerbatimSink, WriteGuard, Drift, ContextRot | 写什么、谁能写、何时腐烂、何时清理 |
| 重建恢复 | StateRebuilder, Handoff, Seeds | 会话中断恢复 + 决策点回溯 + 跨会话交接 |
| 压缩路由 | AAAK, AAAK Index, Closet | 70-85% 压缩率 + 主题指针索引，保持可检索性 |
| 一致性 | FactChecker, Coherence, EntityRegistry, ContextFence | 多个 Agent 写入的知识不冲突、不重复、不污染 |

### 3. 安全与管控 (Safety & Control) — 不可绕过的防护

安全不是附加功能，而是架构中**不可绕过的一层**。智能体可以访问文件系统、执行命令、调用 API——这些能力如果没有管控就是灾难。

四层防护：

```
  权限判定     → PermissionMode: read-only / workspace-write / danger-full-access
  Gate 流水线  → PreFlight(预检) → Approval(审批) → Revision(修正) → Escalation(升级) → Abort(终止)
  WriteGuard   → 控制谁(L0/L1/Agent/External)可以写哪些内存层，全部审计日志
  沙箱隔离     → Linux Sandbox: 容器隔离 + 文件系统隔离 + 网络限制
```

Gate 系统特别值得一提。它不是简单的"放行/拒绝"二元判断——它是一个完整的事件流水线，包含：

- **PreFlightGate**：执行前检查影响范围，触发 impact analysis
- **ApprovalGate**：根据策略自动批准或转人工审批
- **RevisionGate**：检测修改是否超出预期范围，触发修正建议
- **EscalationGate**：风险升级，重新评估权限
- **AbortGate**：硬终止，回滚到上一个安全状态

Gate 与 **PolicyEngine** 协同——PolicyEngine 定义规则，Gate 执行检查，两个组件都在运行时内存中，不需要外部服务。

### 4. 编排引擎 (Orchestration) — 复杂任务分解

单个 LLM 调用能做的事情有限。真正的编程工作需要：并行修改多个文件 → 运行测试 → 分析错误 → 修复 → 重建。cowd 通过两个组件解决：

**Wave**：依赖图 + 并行执行引擎
```
  Task A ──→ Task C ──→ Task E
      \        ↑            ↑
       → Task B ──→ Task D ─┘
      
  Wave 1: A, B (并行)
  Wave 2: C, D (A,B 完成后并行)
  Wave 3: E (C,D 完成后)
```

**SubAgent**：受限执行的子智能体
- 每个 SubAgent 获得有限的工具列表（比如只有文件读写）
- WriteGuard 限制只能写入 L3/L4，不能写 L0/L1
- Token 预算上限（默认 20K）
- 超时控制
- 执行结果汇总到父 Agent

Wave + SubAgent 的组合让 cowd 可以处理复杂的多步骤任务，同时每个子任务的权限范围都被精确控制。

### 5. 平台与集成 (Platform & Integration) — 全渠道接入

cowd 不只是一个 TUI 工具。它可以嵌入到：

- **API Server**：REST API，支持认证和 CORS，WebUI 捆绑，ActiveSessions 多会话并发，API 完全对等（记忆/工具/配置）
- **飞书机器人**：接收飞书消息，返回智能体回答，支持富文本和交互卡片
- **企微机器人**：企业微信适配
- **邮件**：邮件收发，自动推理和回复
- **CLI**：`cowd prompt "xxx"` 单次问答，`cowd --resume` 续接会话

所有平台共享同一个运行时核心，通过 `EventBus` 注入外部事件：

```
  飞书消息 ──→ EventBus ──→ ConversationRuntime ──→ LLM → 回复飞书
  API请求  ──→ EventBus ──→ ConversationRuntime ──→ LLM → 回传API
   CLI输入  ──→ EventBus ──→ ConversationRuntime ──→ LLM → TUI渲染
```

### 6. 入口模式 (Entry Points) — 独立进程，共享后端

TUI 模式和 Server 模式是两个独立进程入口，共享同一个 `runtime` crate 后端代码，但各自独立运行：

```
  cowd --solo (TUI 模式)              cowd serve (Server 模式)
  ┌──────────────────────┐            ┌──────────────────────────┐
  │  TUI 控制台           │            │  HTTP API + 飞书 WebSocket │
  │  9/9 面板           │            │  + WebUI 捆绑             │
  │                      │            │                          │
  │  ┌────────────────┐  │            │  ┌──────────────────────┐ │
  │  │ Conversation   │  │            │  │ Conversation         │ │
  │  │ Runtime        │  │            │  │ Runtime              │ │
  │  │ (独立实例)      │  │            │  │ (独立实例)            │ │
  │  └────────────────┘  │            │  └──────────────────────┘ │
  │        │              │            │        │                  │
  │  ┌─────▼──────────┐  │            │  ┌─────▼──────────────┐  │
  │  │ runtime crate  │  │            │  │ runtime crate      │  │
  │  │ memory·tools·  │  │            │  │ memory·tools·      │  │
  │  │ permissions·   │  │            │  │ permissions·       │  │
  │  │ config         │  │            │  │ config             │  │
  │  └────────────────┘  │            │  └────────────────────┘  │
  └──────────────────────┘            └──────────────────────────┘
```

关键特性：
- **共享代码，独立进程**：TUI 和 Server 使用相同的 `runtime` crate（内存系统、工具注册、权限管控、配置管理），但运行在独立进程中，各有自己的 ConversationRuntime 实例
- **Session SQLite 共享**：TUI 和 API 可以访问同一个 `~/.cowd/sessions.db`，但同一时间只有一个进程写入
- **API 完全对等**：Server 模式的 `/api/memory` `/api/tools` `/api/config` 全部连接实际运行时后端
- **多 Session 并发**：Server 模式下 ActiveSessions 管理多个独立 ConversationRuntime
- **安装部署**：`cowd install` → `~/.cowd/bin/cowd` + systemd 服务注册

---

## 当前状态评估

### 已完成的核心能力

| 领域 | 完成度 | 关键指标 |
|------|--------|----------|
| 内存系统 36 模块 | 100% | 456 测试，全生命周期闭环 |
| 3D 记忆架构 | 100% | Scope×Layer×State，14+语言扫描 |
| tree-sitter 代码索引 | 100% | 5 语言 AST，增量索引，关系图谱 |
| 会话运行时 | 95% | ConversationRuntime，自动压缩，续接 |
| Gate 流水线 | 90% | 5 种 Gate，PolicyEngine 协同 |
| Wave 编排 | 85% | TaskGraph 依赖图引擎，并行执行（TaskGraph 已存在，尚未完全接入运行时） |
| SubAgent | 85% | trait 已定义，受限执行/WriteGuard/Token 预算，生产级实现仍为 stub |
| MCP 工具协议 | 90% | Stdio/SSE/Remote，生命周期管理 |
| 权限系统 | 90% | 3 种模式，Gate 集成 |
| 平台适配 | 85% | API 完全对等 (记忆/工具/配置)，Gateway 守护进程 |
| TUI | 95% | 9/9 侧边栏面板全功能，ChatView 对话渲染，快捷键提示 |
| 性能 | 80% | OnceLock 加速，1K/10K/20K 基准测试 |

### 存在的薄弱环节

1. **内存-运行时深度集成尚未完成**。CognitiveContextManager 和 ConversationRuntime 目前的集成方式是"调用-返回"模式，还不是"订阅-推送"模式。内存应该在后台持续学习变化，而不是每次轮次被动调用。

2. **多智能体团队尚未实战**。L4 共享层、冲突检测、角色预算这些能力已经构建，但缺少一个完整的"多 Agent 协同解决复杂问题"的端到端工作流。现在能并行执行子任务，但子任务之间无法通过 L4 实时协商。

3. **Gate 执行流水线缺少自动修复**。PreFlight 可以检测到问题，但修正策略需要手动指定。下一步是 Gate + 自动修正：检测到 lint 错误 → 自动 fix → 重新验证。

4. **平台适配器是单向的**。飞书/企微可以接收消息并回复，但不能将外部事件（如代码仓库 webhook）触发到 cowd 的主动推理循环。

5. **性能基线没有 CI 护栏**。1K 测试是手动运行的，没有自动化的性能回归门禁。10K/20K 是优化参考，但缺少持续监控。

6. **Gateway 守护进程需进一步增强**。当前 `cowd gateway start` 使用子进程方式启动，非真正的 daemonize。API 端点虽然连接了实际后端，但内存搜索、工具执行等高级功能尚未通过 API 完整暴露。

---

## 下一阶段演进方向

### 方向一：记忆驱动的认知架构（内存 2.0）

当前的内存系统是"被动人库"——调用时检索，轮次结束时写入。进化方向是**主动认知**：

- **后台知识流**：Extractor 在对话之间持续运行，将 Git 提交、文件变更、文档更新自动摄入 KG，不需要等待用户提问
- **预测性预取**：根据当前会话的主题趋势和 Agent 行为模式，提前从 L3/L4 加载可能需要的上下文，降低 LLM 调用延迟
- **语义记忆压缩**：AAAK 现在是符号化压缩，下一步是语义压缩——识别重复知识模式，合并相似实体，主动遗忘冗余
- **跨会话叙事链**：不仅保留单次会话，而是建立跨会话的"叙事弧"——知识图谱中的实体随时间演化，形成项目的深层脉络

```
  当前                          进化
  ────                          ────
  用户提问 → 检索 → 回答        用户提问 → 预测预取 → 检索 → 增强 → 回答
                                          ↕
  轮次结束 → 写入                持续知识流 ← Git/文件/文档变更
```

### 方向二：真正的多智能体团队协作（蜂群 1.0）

现在：一个 Agent 自主行动，SubAgent 作为受限执行器。

进化：**多个对等 Agent 组成虚拟团队，通过 L4 共享记忆协商决策**。

- **职责分配代理**：接收到复杂任务后，主 Agent 分解子任务并通过 L4 发布招标，其他 Agent 根据专业领域竞标
- **实时观点交换**：不只共享知识（L4 key-value），而是共享推理链——Agent A 说"我建议重构 X 因为 Y"，Agent B 通过 L4 读到并回应
- **加权投票决策**：FactChecker 的三元仲裁从冲突检测扩展到决策表决——多个 Agent 对技术方案投票，按角色权重加权
- **记忆驱动的角色固化**：某个 Agent 如果持续在 Rust 性能优化领域输出高质量结果，它在 L4 的领域权重提升，未来相关任务自动偏向它

### 方向三：安全自动化和可审计执行（Gates 2.0）

当前 Gate 系统是"检查-通过/拒绝"。进化方向是**防御性执行**：

- **自动修正 Gate**：PreFlight 检测到 lint/style/import 问题 → 自动修改 → 重新验证 → 通过。无需人类介入
- **影响预测反馈**：在执行前，显示"此修改将影响 3 个模块、2 个执行流、1 个公共 API"，并在执行后验证预测的准确性
- **可回滚事务式执行**：文件编辑、工具调用、记忆写入作为一个事务——如果某一步失败，前面的所有修改自动回滚
- **策略自学习**：PolicyEngine 从过去的人工审批决策中学习——如果人类总是批准"修改测试文件"，Gate 自动放行这类操作

### 方向四：性能持续优化和可观测性（运维 1.0）

当前有基准但没有护栏。进化方向：

- **CI 性能门禁**：每次 PR 自动运行 1K 压力测试，比较与基线的偏差，超过 5% 则阻止合并
- **内存剖析仪表盘**：通过 ContextRot 和 Profiler 暴露实时指标——检索延迟、压缩率、分层使用率、冲突频率
- **自适应存储策略**：L3 数据量增长时自动触发归档，高频访问的条目自动提升到 L1/L2，冷数据降级到 SQLite WAL 压缩

### 方向五：平台深度集成（Platform 2.0）

当前平台适配器是"通道"。进化方向是**双向事件驱动**：

- **代码仓库 Webhook → 记忆注入**：Git push / PR merge 事件自动触发 ExtractionPipeline，将代码变更录入 KG
- **飞书/企微 → 主动推送**：异常检测（如 ContextRot 告警）主动推送通知到即时通讯
- **API Server → 服务化**：提供 WebSocket 实时流、Admin API（查看记忆统计、Gate 日志、Agent 状态），支持外部系统编排

---

## 架构总览

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                       接入层 (Platform)                          │
  │   CLI    TUI(控制台,9/9面板)    API Server    Gateway    飞书/企微/邮件   WebUI  │
  └──────────┬──────────┬──────────────────────┬────────────────────┘
             │          │                      │
             ▼          ▼                      ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                       运行时核心 (Runtime)                       │
  │                                                                  │
  │  ┌──────────────┐  ┌──────────┐  ┌─────────────────────────┐   │
  │  │ Conversation │  │   Wave   │  │    Gate Pipeline        │   │
  │  │   Runtime    │──│  Engine  │──│ PreFlight→Approval→Abort│   │
  │  │              │  │          │  │                         │   │
  │  │ prepare_ctx  │  │ TaskGraph│  │ PolicyEngine            │   │
  │  │ LLM call     │  │ SubAgent │  │ WriteGuard + Audit      │   │
  │  │ tool_exec    │  │ Parallel │  │ Sandbox Isolation       │   │
  │  │ on_turn_end  │  │          │  │                         │   │
  │  └──────┬───────┘  └──────────┘  └─────────────────────────┘   │
  │         │                                                       │
  │         ▼                                                       │
  │  ┌──────────────────────────────────────────────────────────┐   │
  │  │                 内存系统 (Memory)                         │   │
  │  │                                                          │   │
  │  │  L0 Identity  │  L1 Essential  │  L2 Project            │   │
  │  │  L3 Deep      │  L4 Shared     │  (Scope×Layer×State)   │   │
  │  │                                                          │   │
  │  │  CodeIndexer(tree-sitter) → ProjectKG → HotSymbols      │   │
  │  │  Extractor → Miner → KG    │  FTS5 → BM25 → Embeddings │   │
  │  │  FactChecker → Coherence   │  Drift → ContextRot       │   │
  │  │  AAAK → Closet → FreshCtx  │  StateRebuilder → Handoff │   │
  │  └──────────────────────────────────────────────────────────┘   │
  │                                                                  │
  │  ┌──────────────────────────────────────────────────────────┐   │
  │  │                   Provider 适配层                        │   │
  │  │   Anthropic(原生) │ OpenAI兼容 │ DeepSeek │ Qwen │ Grok  │   │
  │  └──────────────────────────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────────────┘
```

---

## 启动方式

```bash
# 编译
cargo build --release

# TUI 终端模式（默认，自动启动 daemon）
cowd                     # 新建会话，全功能 TUI 控制台
cowd --solo              # 等同无参数运行（显式别名）
cowd --resume latest     # 续接最近会话
cowd --resume <id>       # 续接指定会话

# API 网关服务
cowd gateway run         # 前台运行 daemon (HTTP:8642 + Unix Socket + 飞书)
cowd gateway start       # systemd 后台启动
cowd gateway stop        # 停止
cowd gateway status      # 查看状态

# 安装部署
cowd install --systemd   # 安装到 ~/.cowd/bin/ + systemd 注册

# 信息
cowd version             # 版本
cowd help                # 帮助
```

---

## 开发

```bash
cargo clean && cargo build --release  # clean rebuild (~1m)
cargo test -p cowd-memory  # 456 tests
cargo test --workspace     # 1000+ tests
cargo build --release      # → target/release/cowd (~28MB)
```

## 日志

```bash
# 调试模式（完整日志到终端 + 文件）
RUST_LOG=debug cowd --solo

# 日志文件位置
tail -f ~/.cowd/logs/cowd.$(date +%Y-%m-%d)
```

---

## 许可证

MIT License
