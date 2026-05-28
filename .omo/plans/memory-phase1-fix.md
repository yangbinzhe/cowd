# Phase 1 — 记忆框架立即修复：止血与激活

## TL;DR

> **Quick Summary**: 修复5个关键集成缺陷，将记忆框架发挥价值从 2.7/5 提升至 3.5/5。启用L4共享层、修复ToolSandbox管道、解除Deep Compression硬编码、合并FactChecker双实例、统一SqliteStore连接。
> 
> **Deliverables**:
> - 可用的多Agent L4共享层（peer context, hot topics, delegation）
> - 可检索的工具输出记忆（ToolSandbox消费路径）
> - 工作状态的 Deep Compression（Stage 3 压缩）
> - 单例 FactChecker（消除状态分裂）
> - 单连接 SqliteStore（消除双连接不一致风险）
> 
> **Estimated Effort**: Short (1-2天)
> **Parallel Execution**: YES — 3 waves
> **Critical Path**: F4 → F5 (共享FactChecker → 消除双连接) → F1+F2+F3 (并行)

---

## Context

### Original Request
基于 `plan/0527-分析/01-记忆框架/` 的6份深度分析报告，针对Phase 1的5个关键缺陷生成TDD模式的详细执行计划。

### Interview Summary
- **Oracle Precision Review 已完成**：全部5项 VERDICT: GO
  - F1: 需在 conversation.rs 两个构造分支中都注入 `set_active_agent`
  - F2: `ToolOutputSandbox::search()` 已存在（tool_sandbox.rs:132-164），只需在 prepare_context 中调用
  - F3: config-default.yaml 行号修正（deep.enabled 在 line 105/206，非107）
  - F4: 需删除两个struct的 fact_checker 字段，引入 OnceLock 全局单例
  - F5: 最复杂，需在 MemoryStore trait 新增 kv_put/kv_get 方法

### Oracle Review Corrections Incorporated
- F1: 双分支注入（line 346 `Ok(mgr)` 和 line 363 `Ok(mgr)` 两处）
- F2: 不新增方法，直接调用已有的 `ToolOutputSandbox::search("shared", query, limit)`
- F3: 精确指向 `feature_config.compression().deep.enabled`（line 1929 修改）
- F4: 删除 orchestrator.rs:69 和 cognitive.rs:163 两个字段
- F5: trait 新增2个默认方法 + SqliteStore 实现 + cognitive.rs 迁移

---

## Work Objectives

### Core Objective
消除记忆框架与运行时之间5个关键集成断裂点，恢复设计意图中的能力。

### Concrete Deliverables
- 修改 `conversation.rs`：`set_active_agent()` 双分支注入
- 修改 `cognitive.rs`：ToolSandbox查询步骤 + 移除第二连接
- 修改 `orchestrator.rs`：移除 FactChecker 字段
- 修改 `store/mod.rs`：新增 `kv_put`/`kv_get` trait 方法
- 修改 `store/sqlite.rs`：实现 kv 方法 + 建表
- 新增测试：L4 peer context 测试、sandbox 消费测试、单连接测试

### Definition of Done
- [ ] `cargo build -p cowd-memory -p runtime` 编译通过
- [ ] `cargo test -p cowd-memory` 全部456+测试通过
- [ ] `cargo test -p cowd-memory -- swarm_e2e swarm_concurrent` 通过
- [ ] L4 peer context 在日志中可见（`tracing::debug!("peer context: recalled X entries")`）
- [ ] ToolSandbox 内容在 prepare_context 输出中可见（`[SANDBOX: tool_name]` 格式）
- [ ] Deep Compression 可通过配置启用/禁用

### Must Have
- `set_active_agent("primary")` 在 ConversationRuntime 构造时调用
- `prepare_context()` 在 Step 10 后查询 ToolSandbox
- `enable_deep_compression` 从运行时配置读取（非硬编码false）
- 仅一个 FactChecker 实例（全局 OnceLock）
- 仅一个 SqliteStore 连接

### Must NOT Have (Guardrails)
- **NOT** 修改 CognitiveContextManager 的公开 API 签名
- **NOT** 修改 `remember()` 的调用方接口
- **NOT** 改变现有的 Token 预算计算逻辑（留到 Phase 2 F7）
- **NOT** 新增对 `notify` 或任何新 crate 的依赖
- **NOT** 向后不兼容的配置格式变更
- **NOT** 在 prepare_context 热路径中增加超过1个额外查询

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** - ALL verification is agent-executed.

### Test Decision
- **Infrastructure exists**: YES (456 existing tests)
- **Automated tests**: Tests-after (write implementation then add tests)
- **Framework**: `cargo test -p cowd-memory`
- **QA Policy**: Every task includes curl/bash/REPL-based verification

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately — 2 independent foundation tasks):
├── Task 1: F4 - FactChecker OnceLock 全局单例化 [deep]
└── Task 2: F5 - MemoryStore trait kv_put/kv_get + SqliteStore 实现 [deep]

Wave 2 (After Wave 1 — 3 independent integration tasks):
├── Task 3: F1 - L4 set_active_agent 双分支注入 [quick]
├── Task 4: F2 - ToolSandbox prepare_context 消费路径 [quick]
└── Task 5: F3 - Deep Compression 配置读取修复 [quick]

Wave FINAL (After ALL tasks — 2 parallel reviews):
├── Task F1: Regression test + code audit [deep]
└── Task F2: Plan compliance verification [oracle]

Critical Path: Task 1 (FactChecker) → Task 3-5 (depend on consistent FactChecker state)
Parallel Speedup: ~60% faster than sequential
Max Concurrent: 2 (Wave 1) + 3 (Wave 2)
```

---

## TODOs

- [x] 1. F4 — FactChecker OnceLock 全局单例化

  **What to do**:
  1. 在 `crates/memory/src/orchestrator.rs` 顶部新增全局单例：
     ```rust
     use std::sync::OnceLock;
     static GLOBAL_FACT_CHECKER: OnceLock<parking_lot::Mutex<FactChecker>> = OnceLock::new();
     pub fn get_fact_checker() -> &'static parking_lot::Mutex<FactChecker> {
         GLOBAL_FACT_CHECKER.get_or_init(|| parking_lot::Mutex::new(FactChecker::new()))
     }
     ```
  2. 删除 `orchestrator.rs:69` 的 `fact_checker: Mutex<Option<FactChecker>>` 字段
  3. 删除构造函数 `orchestrator.rs:149` 的 `fact_checker: Mutex::new(Some(FactChecker::new()))` 初始化
  4. 删除 `with_fact_checker()` 和 `with_fact_checker_mut()` 方法（约807-818行）
  5. 修改 `remember()` 中 FactChecker 使用代码（约465-524行）：将 `self.fact_checker.lock()` 替换为 `get_fact_checker().lock()`
  6. 删除 `cognitive.rs:163` 的 `fact_checker: Mutex<FactChecker>` 字段
  7. 删除 `cognitive.rs:330` 的 `fact_checker: Mutex::new(FactChecker::new())` 初始化
  8. 修改 `cognitive.rs:1103` 的 `self.fact_checker.lock()` 替换为 `crate::orchestrator::get_fact_checker().lock()`

  **Must NOT do**:
  - 不要修改 FactChecker 的 `check_triple()` 或 `auto_correct()` 行为
  - 不要改变 `remember()` 中对 `entry.confidence` 的修改逻辑
  - 不要引入新的 crate 依赖（OnceLock 是 std 自带）

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 涉及跨两个struct的状态迁移，需要精确的字段删除和引用替换，避免编译错误
  - **Skills**: []
  - **Skills Evaluated but Omitted**: 无 — 纯 Rust 重构，无需特定技能

  **Parallelization**:
  - **Can Run In Parallel**: YES (与 Task 2 并行)
  - **Parallel Group**: Wave 1
  - **Blocks**: Task 3, Task 4, Task 5 (后续任务需要一致的 FactChecker 状态)
  - **Blocked By**: None

  **References**:
  - `crates/memory/src/orchestrator.rs:69` — 需要删除的 fact_checker 字段
  - `crates/memory/src/orchestrator.rs:149` — 需要删除的初始化行
  - `crates/memory/src/orchestrator.rs:465-524` — remember() 中需要替换的 `self.fact_checker.lock()` 调用
  - `crates/memory/src/orchestrator.rs:807-818` — 需要删除的 with_fact_checker 方法
  - `crates/memory/src/cognitive.rs:163` — 需要删除的第二个 fact_checker 字段
  - `crates/memory/src/cognitive.rs:330` — 需要删除的第二个初始化
  - `crates/memory/src/cognitive.rs:1103` — on_turn_end 中需要替换的引用
  - `crates/memory/src/fact_checker.rs` — FactChecker struct 定义（确认 Send + Sync）

  **Acceptance Criteria**:
  - [ ] `cargo build -p cowd-memory` 编译通过
  - [ ] `cargo test -p cowd-memory -- fact_check` 全部通过
  - [ ] `cargo test -p cowd-memory -- conflict` 全部通过
  - [ ] `rg "self\.fact_checker" crates/memory/src/` 无匹配（证明字段已删除）

  **QA Scenarios**:

  ```
  Scenario: OnceLock singleton initializes correctly
    Tool: Bash (cargo test)
    Preconditions: 项目根目录
    Steps:
      1. cargo test -p cowd-memory -- fact_check_test -- --nocapture
      2. 在测试输出中搜索 "fact check: contradiction detected"
    Expected Result: 测试通过，矛盾检测功能正常运作
    Failure Indicators: 测试失败或矛盾检测无输出
    Evidence: .omo/evidence/task-1-factcheck-test-output.txt

  Scenario: remember() still downgrades confidence on contradiction
    Tool: Bash (cargo test)
    Preconditions: 构建成功
    Steps:
      1. cargo test -p cowd-memory orchestrator::tests -- --nocapture
      2. 确认 remember() 相关测试通过
    Expected Result: 所有 orchestrator 测试通过
    Evidence: .omo/evidence/task-1-orchestrator-tests.txt
  ```

  **Commit**: YES
  - Message: `refactor(memory): use OnceLock global FactChecker singleton`
  - Files: `crates/memory/src/orchestrator.rs`, `crates/memory/src/cognitive.rs`

- [x] 2. F5 — MemoryStore trait kv_put/kv_get + SqliteStore 统一连接

  **What to do**:
  1. 在 `crates/memory/src/store/mod.rs` 的 `MemoryStore` trait 中新增两个方法（放在 trait 末尾，约第198行之前）：
     ```rust
     async fn kv_put(&self, key: &str, value: &str) -> Result<()> {
         Err(MemoryError::Store("kv store not supported by this backend".into()))
     }
     async fn kv_get(&self, key: &str) -> Result<Option<String>> {
         Err(MemoryError::Store("kv store not supported by this backend".into()))
     }
     ```
  2. 在 `crates/memory/src/store/sqlite.rs` 中实现这两个方法：
     - `ensure_kv_table()` 创建 `CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL)`
     - `kv_put()` 使用 `INSERT OR REPLACE INTO kv_store`
     - `kv_get()` 使用 `SELECT value FROM kv_store WHERE key = ?`
  3. 修改 `crates/memory/src/cognitive.rs` 构造函数：
     - 删除第256行的 `let sqlite_store = SqliteStore::open(&config.store)?;`
     - 将 Closet 加载（第259-269行）改为 `orchestrator.store().kv_get("closet").await`
     - 将 Seeds 加载（第282-286行）改为 `orchestrator.store().kv_get("seeds").await`
  4. 修改 `cognitive.rs` `on_turn_end` 中的 Closet 保存（第1311-1331行）：
     将 `self.sqlite_store.save_closet(&json)` 改为 `self.orchestrator.store().kv_put("closet", &json).await`
  5. 修改 `cognitive.rs` `on_turn_end` 中的 Seeds 保存（第1334-1349行）：
     将 `self.sqlite_store.save_seeds(&json)` 改为 `self.orchestrator.store().kv_put("seeds", &json).await`
  6. 删除 `cognitive.rs:125` 的 `sqlite_store: SqliteStore` 字段
  7. 删除 `cognitive.rs` 中 `save_closet()` 和 `save_seeds()` 方法在 `SqliteStore` 上的依赖

  **Must NOT do**:
  - 不要删除 `SqliteStore::save_closet()` / `save_seeds()` 方法（可能其他地方使用）
  - 不要改变 Closet/Seeds 的 JSON 序列化格式
  - 不要在 trait 方法中使用泛型约束（保持 dyn-compatible）

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: 涉及 trait 扩展 + 两处实现 + 构造函数重构，需要谨慎处理依赖关系
  - **Skills**: []
  - **Skills Evaluated but Omitted**: 无

  **Parallelization**:
  - **Can Run In Parallel**: YES (与 Task 1 并行)
  - **Parallel Group**: Wave 1
  - **Blocks**: None (独立于其他Task)
  - **Blocked By**: None

  **References**:
  - `crates/memory/src/store/mod.rs:50-199` — MemoryStore trait 定义（在末尾新增方法）
  - `crates/memory/src/store/sqlite.rs` — SqliteStore 实现（搜索 `fn open` 找到初始化点）
  - `crates/memory/src/cognitive.rs:125` — 需要删除的 `sqlite_store` 字段
  - `crates/memory/src/cognitive.rs:256` — 需要删除的第二个 `SqliteStore::open()` 调用
  - `crates/memory/src/cognitive.rs:259-269` — Closet 加载逻辑（改为 kv_get）
  - `crates/memory/src/cognitive.rs:282-286` — Seeds 加载逻辑（改为 kv_get）
  - `crates/memory/src/cognitive.rs:1311-1331` — Closet 保存逻辑（改为 kv_put）
  - `crates/memory/src/cognitive.rs:1334-1349` — Seeds 保存逻辑（改为 kv_put）

  **Acceptance Criteria**:
  - [ ] `cargo build -p cowd-memory` 编译通过
  - [ ] `cargo test -p cowd-memory` 全部测试通过
  - [ ] Closet 加载/保存功能正常（通过 `closet` 相关测试验证）
  - [ ] Seeds 加载/保存功能正常
  - [ ] `rg "sqlite_store" crates/memory/src/cognitive.rs` 无匹配（证明字段已删除）
  - [ ] `rg "SqliteStore::open" crates/memory/src/cognitive.rs` 仅1处匹配（在 Orchestrator 初始化路径中）

  **QA Scenarios**:

  ```
  Scenario: kv_put and kv_get round-trip
    Tool: Bash (cargo test)
    Preconditions: SqliteStore 编译通过
    Steps:
      1. 编写内联测试：kv_put("test_key", "test_value") → kv_get("test_key") == Some("test_value")
      2. cargo test -p cowd-memory store::sqlite::tests
    Expected Result: kv round-trip 测试通过
    Evidence: .omo/evidence/task-2-kv-test.txt

  Scenario: Closet survives save + reload cycle
    Tool: Bash (cargo test)
    Preconditions: 构建成功
    Steps:
      1. cargo test -p cowd-memory -- closet -- --nocapture
    Expected Result: Closet 加载/保存测试通过
    Evidence: .omo/evidence/task-2-closet-test.txt
  ```

  **Commit**: YES
  - Message: `feat(memory): add kv_put/kv_get to MemoryStore trait, unify SqliteStore connections`
  - Files: `crates/memory/src/store/mod.rs`, `crates/memory/src/store/sqlite.rs`, `crates/memory/src/cognitive.rs`

- [x] 3. F1 — L4 set_active_agent 双分支注入

  **What to do**:
  1. 打开 `crates/runtime/src/conversation.rs`
  2. 找到 ConversationRuntime 构造函数中 CognitiveContextManager 初始化成功的**两个**位置：
     - 第一个分支（tokio runtime 内）：约第346-348行 `Ok(mgr) => { (Some(Arc::new(mgr)), None) }`
     - 第二个分支（无 tokio runtime）：约第363-365行 `Ok(mgr) => { (Some(Arc::new(mgr)), None) }`
  3. 在**两个分支**的 `Arc::new(mgr)` 之前，都插入：
     ```rust
     mgr.set_active_agent("primary".to_string());
     ```
  4. 完整修改示例（第一个分支）：
     ```rust
     Ok(mgr) => {
         mgr.set_active_agent("primary".to_string());
         tracing::debug!("memory: CognitiveContextManager initialised, active_agent=primary");
         (Some(Arc::new(mgr)), None)
     }
     ```
  5. 在 `prepare_memory_context()` 中（约1594行），确认 `mgr.set_active_session(session_id.clone())` 已存在 — 不需要修改
  6. 在 `run_memory_post_turn()` 中（约1763行），在 `mgr.on_turn_end()` 调用前新增 delegation observation：
     ```rust
     // 如果有子Agent执行结果，观察并写入L4
     if let Some(subagent_results) = &turn_output.subagent_results {
         for result in subagent_results {
             mgr.observe_delegation(&result.agent_role, &result.task, &result.result, Some(&session_id));
         }
     }
     ```

  **Must NOT do**:
  - 不要修改 `set_active_agent()` 的方法签名
  - 不要在 constructor 的失败分支中注入（None 分支）
  - 不要改变 `prepare_context()` 中 L4 recall 的调用方式

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 纯注入型修改，逻辑简单，但需要精确找到两个注入点
  - **Skills**: []
  - **Skills Evaluated but Omitted**: 无

  **Parallelization**:
  - **Can Run In Parallel**: YES (与 Task 4, Task 5 并行)
  - **Parallel Group**: Wave 2
  - **Blocks**: None
  - **Blocked By**: Task 1 (FactChecker 单例化完成后才能验证 remember)

  **References**:
  - `crates/runtime/src/conversation.rs:346-348` — 第一个注入点（tokio runtime 内）
  - `crates/runtime/src/conversation.rs:363-365` — 第二个注入点（无 tokio runtime）
  - `crates/runtime/src/conversation.rs:1594` — set_active_session 调用（确认已存在）
  - `crates/runtime/src/conversation.rs:1763` — on_turn_end 调用（delegation observation 插在前面）
  - `crates/memory/src/cognitive.rs:389-392` — set_active_agent 定义（验证方法签名）
  - `crates/memory/src/cognitive.rs:644-689` — prepare_context L4 recall 部分（验证 current_agent 检查逻辑）

  **Acceptance Criteria**:
  - [ ] `cargo build -p runtime` 编译通过
  - [ ] 启动 cowd 后日志显示 `active_agent=primary`
  - [ ] `prepare_context` 中 L4 peer context 不再返回空（当存在L4数据时）
  - [ ] `cargo test -p cowd-memory -- swarm_e2e` 通过

  **QA Scenarios**:

  ```
  Scenario: ConversationRuntime sets active_agent on construction
    Tool: Bash (grep + cargo build)
    Preconditions: 代码已修改
    Steps:
      1. rg "set_active_agent" crates/runtime/src/conversation.rs
      2. 确认在 Ok(mgr) 的两个分支中各有一处调用
      3. cargo build -p runtime
    Expected Result: 两处调用均存在，编译通过
    Failure Indicators: 只有一处调用，或编译失败
    Evidence: .omo/evidence/task-3-agent-injection.txt

  Scenario: L4 peer context injection not empty after agent set
    Tool: Bash (cargo test)
    Preconditions: swarm_e2e_test 可用
    Steps:
      1. cargo test -p cowd-memory -- swarm_e2e -- --nocapture
      2. 检查输出是否有 "peer context" 相关的 debug 日志
    Expected Result: 测试通过，peer context 不再返回空
    Evidence: .omo/evidence/task-3-swarm-test.txt
  ```

  **Commit**: YES
  - Message: `feat(memory): inject set_active_agent into ConversationRuntime constructor`
  - Files: `crates/runtime/src/conversation.rs`

- [x] 4. F2 — ToolSandbox prepare_context 消费路径

  **What to do**:
  1. 打开 `crates/memory/src/cognitive.rs`
  2. 找到 `prepare_context()` 中 Step 7（code symbol injection，约第931-944行）
  3. 在 code_context 构建完成之后、Step 7 assemble PreparedContext（约946行）之前，插入 ToolSandbox 查询步骤：
     ```rust
     // ── Step 7b: Tool output sandbox injection ──
     let sandbox = self.tool_sandbox.lock();
     let count = sandbox.entry_count();
     if count > 0 {
         let results = sandbox.search("shared", query, 3).unwrap_or_default();
         for summary in results {
             entries.push(MemoryEntry {
                 id: uuid::Uuid::new_v4(),
                 layer: MemoryLayer::L3,
                 category: MemoryCategory::Reference,
                 priority: Priority::Normal,
                 source: MemorySource::AutoExtracted,
                 title: format!("[SANDBOX: {}] call={}", summary.tool_name, summary.call_id),
                 content: format!("Tool output from {} ({} lines):\n{}",
                     summary.tool_name, summary.total_lines, summary.summary),
                 embedding: None,
                 tags: vec!["sandbox".into(), "tool_output".into()],
                 relations: vec![],
                 confidence: 0.7, access_count: 0, staleness: 0.0,
                 created_at: Utc::now(), updated_at: Utc::now(),
                 last_accessed_at: None,
                 scope: MemoryScope::default(),
                 session_id: None, source_agent: None,
                 visibility: crate::types::AgentVisibility::default(),
             });
         }
     }
     ```
  4. 确认 `ToolOutputSandbox::search("shared", query, limit)` 方法存在且可用（`tool_sandbox.rs:132-164`）
  5. 确认 `ToolOutputSandbox::entry_count()` 方法存在，若不存在则新增简单的 `fn entry_count(&self) -> usize { self.store.xxx }`

  **Must NOT do**:
  - 不要新增 `ToolOutputSandbox::search()` 方法（已存在）
  - 不要在 sandbox 查询超过3个结果（避免 token 浪费）
  - 不要阻塞主路径：sandbox 查询失败时静默跳过

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 单文件、单函数插入，逻辑复用已有API
  - **Skills**: []
  - **Skills Evaluated but Omitted**: 无

  **Parallelization**:
  - **Can Run In Parallel**: YES (与 Task 3, Task 5 并行)
  - **Parallel Group**: Wave 2
  - **Blocks**: None
  - **Blocked By**: None

  **References**:
  - `crates/memory/src/tool_sandbox.rs:132-164` — 已有的 `search()` 方法签名和实现
  - `crates/memory/src/tool_sandbox.rs` — 检查 `entry_count()` 是否存在，搜索 `fn entry_count`
  - `crates/memory/src/cognitive.rs:931-962` — 插入位置（Step 7 code symbol 和 assemble PreparedContext 之间）
  - `crates/memory/src/cognitive.rs:1079-1094` — on_turn_end 中 sandbox 索引位置（确认写入路径）

  **Acceptance Criteria**:
  - [ ] `cargo build -p cowd-memory` 编译通过
  - [ ] `prepare_context()` 输出中包含 `[SANDBOX: tool_name]` 格式的条目（当有索引数据时）
  - [ ] sandbox 查询失败不影响 prepare_context 返回（静默回退）
  - [ ] sandbox 条目不超过3个（limit=3）

  **QA Scenarios**:

  ```
  Scenario: Sandbox entries appear in prepare_context output
    Tool: Bash (cargo test)
    Preconditions: 有大型工具输出被索引
    Steps:
      1. 编写测试: on_turn_end index → prepare_context query → verify sandbox entries present
      2. cargo test -p cowd-memory -- tool_sandbox -- --nocapture
    Expected Result: 测试验证 prepare_context 包含 sandbox 来源的条目
    Evidence: .omo/evidence/task-4-sandbox-consumption.txt

  Scenario: Sandbox query failure is non-fatal
    Tool: Bash (cargo test)
    Steps:
      1. 模拟 sandbox 为空，调用 prepare_context
      2. 验证返回正常的 PreparedContext（不含sandbox条目）
    Expected Result: 无错误，上下文正常返回
    Evidence: .omo/evidence/task-4-sandbox-fallback.txt
  ```

  **Commit**: YES
  - Message: `fix(memory): add ToolSandbox query step to prepare_context pipeline`
  - Files: `crates/memory/src/cognitive.rs`

- [x] 5. F3 — Deep Compression 配置读取修复

  **What to do**:
  1. 打开 `crates/runtime/src/conversation.rs`
  2. 找到 `build_cc_memory_config()` 函数（约第1893行）
  3. 定位第1929行的硬编码：
     ```rust
     enable_deep_compression: false,
     ```
  4. 替换为从运行时配置读取：
     ```rust
     enable_deep_compression: feature_config.compression().deep.enabled,
     ```
  5. 确认 `RuntimeFeatureConfig::compression()` 方法存在且返回类型含 `deep.enabled: bool` 字段
  6. 若配置路径不存在，检查 `feature_config.memory().compression.deep.enabled` 替代路径

  **Must NOT do**:
  - 不要改变 `CompressionConfig::default()` — 仅修改 `build_cc_memory_config` 中的值
  - 不要修改 `config-default.yaml` 中的 deep compression 配置 — 它已经是 true

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: 单行修改，纯配置传递
  - **Skills**: []
  - **Skills Evaluated but Omitted**: 无

  **Parallelization**:
  - **Can Run In Parallel**: YES (与 Task 3, Task 4 并行)
  - **Parallel Group**: Wave 2
  - **Blocks**: None
  - **Blocked By**: None

  **References**:
  - `crates/runtime/src/conversation.rs:1929` — 需要修改的行
  - `crates/runtime/src/config.rs` — 搜索 `deep` 字段确认 RuntimeFeatureConfig 中的配置路径
  - `config-default.yaml:105` — memory.compression.deep.enabled: true（验证默认值）
  - `config-default.yaml:206` — compression.deep.enabled: true（第二处）
  - `crates/memory/src/config.rs:370-394` — CompressionConfig 中的 enable_deep_compression 字段

  **Acceptance Criteria**:
  - [ ] `cargo build -p runtime` 编译通过
  - [ ] `enable_deep_compression` 的值来自配置而非硬编码
  - [ ] 默认配置下 `enable_deep_compression == true`
  - [ ] 手动设置 `compression.deep.enabled: false` 后可禁用

  **QA Scenarios**:

  ```
  Scenario: Deep compression enabled by default
    Tool: Bash (grep + cargo build)
    Steps:
      1. rg "enable_deep_compression" crates/runtime/src/conversation.rs
      2. 确认该行不包含字面量 "false"，而包含 feature_config/compression 引用
      3. cargo build -p runtime
    Expected Result: 编译通过，无硬编码 false
    Evidence: .omo/evidence/task-5-deep-comp-config.txt

  Scenario: Deep compression can be disabled via config
    Tool: Bash (cargo test)
    Steps:
      1. 编写测试: 设置 compression.deep.enabled=false → build_cc_memory_config → verify enable_deep_compression==false
      2. cargo test -p runtime
    Expected Result: 测试通过
    Evidence: .omo/evidence/task-5-deep-comp-test.txt
  ```

  **Commit**: YES
  - Message: `fix(memory): read enable_deep_compression from runtime config instead of hardcoding false`
  - Files: `crates/runtime/src/conversation.rs`

---


- [x] F1. **Regression Test + Code Audit** — `deep`
  Run full test suite: `cargo test -p cowd-memory && cargo test -p runtime`. Run swarm tests. Verify no new clippy warnings. Search for residual `std::sync::Mutex` in memory crate (should be none after F4). Verify single `SqliteStore::open` call in cognitive.rs constructor (should be 1 after F5).
  Output: `Tests [N pass/N fail] | Clippy [CLEAN/N issues] | Store calls [1/N] | VERDICT: PASS/FAIL`

- [x] F2. **Plan Compliance Verification** — `oracle`
  Read the plan. For each "Must Have": verify implementation exists. For each "Must NOT Have": search codebase for forbidden patterns. Check evidence files exist.
  Output: `Must Have [5/5] | Must NOT Have [5/5] | Tasks [5/5] | VERDICT: APPROVE/REJECT`

---

## Commit Strategy

- **F4**: `refactor(memory): use OnceLock global FactChecker singleton` — orchestrator.rs, cognitive.rs
- **F5**: `feat(memory): add kv_put/kv_get to MemoryStore trait, unify SqliteStore connections` — store/mod.rs, store/sqlite.rs, cognitive.rs
- **F1**: `feat(memory): inject set_active_agent into ConversationRuntime constructor` — conversation.rs
- **F2**: `fix(memory): add ToolSandbox query to prepare_context pipeline` — cognitive.rs
- **F3**: `fix(memory): read enable_deep_compression from runtime config` — conversation.rs

---

## Success Criteria

### Verification Commands
```bash
cargo build -p cowd-memory -p runtime
cargo test -p cowd-memory
cargo test -p cowd-memory -- swarm_e2e swarm_concurrent
cargo clippy -p cowd-memory -- -D warnings
```

### Final Checklist
- [ ] All "Must Have" present
- [ ] All "Must NOT Have" absent
- [ ] All tests pass
- [ ] Oracle F2 verdict: APPROVE
