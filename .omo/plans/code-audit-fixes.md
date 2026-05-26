# COWD 框架审计修复计划 (TDD模式)

## TL;DR

> **Quick Summary**: 修复审计发现的 5 个崩溃级 Bug、2 个锁中毒路径、8 个设计/死代码问题，将所有子系统统一到标准化版本管理。
> 
> **Deliverables**:
> - 5 个崩溃 Bug 修复 (server WebSocket, server_execute_turn, provider_pool, permissions, cognitive)
> - 2 个锁中毒修复 (permissions.rs, cognitive.rs)
> - 3 个 deprecated allow 移除 (conversation.rs, session.rs, session_store.rs)
> - 4 个死代码清理 (runtime session_manager, ProviderChain, trust_resolver, SubAgentExecutor)
> - 17 个构建警告清零
> - Workspace 依赖标准化 + thiserror v1→v2 统一
> - 版本提升: 0.6.2 → 0.6.7
> 
> **Estimated Effort**: Medium
> **Parallel Execution**: YES — 4 Waves + Final Verification
> **Critical Path**: Wave 1 (foundation) → Wave 2 (crash fixes) → Wave 3 (cleanup) → F1-F4

---

## Context

### Original Request
用户要求对 cowd 框架进行全面代码审查，找出所有缺陷、设计问题、重大 Bug、未启用功能和未充分使用的能力。

### Interview Summary
**关键发现**:
- 5 个崩溃级 Bug (server WebSocket/API 嵌套 enter_runtime, provider_pool 空Vec索引, 两个锁中毒)
- 3 个 `#![allow(deprecated)]` 文件绕过 crate 级 deny
- runtime session_manager (123行) 从未使用
- SubAgentExecutor 命名冲突 (trait与struct同名无关)
- ProviderChain 398行故障转移逻辑死代码
- trust_resolver 299行被 `#[cfg(test)]` 隐藏

### Metis Review
**已识别差距**:
- 版本不一致：workspace 0.6.2 但实际已完成 0.6.6 级别工作
- thiserror v1/v2 同时编译造成二进制膨胀
- workspace 依赖未标准化造成版本漂移

---

## Work Objectives

### Core Objective
修复所有审计发现的崩溃Bug和安全问题，清理死代码，统一版本管理和依赖标准。

### Concrete Deliverables
- `crates/cowd-cli/src/server/mod.rs` — 修复 2 处 Handle::current().block_on()
- `crates/runtime/src/provider_pool.rs` — 空Vec守卫
- `crates/runtime/src/permissions.rs` — lock().expect() → into_inner()
- `crates/memory/src/cognitive.rs` — lock().unwrap() → into_inner()
- `crates/memory/src/orchestrator.rs` — MutexGuard 不跨 .await
- `crates/runtime/src/conversation.rs` — 移除 #![allow(deprecated)]
- `crates/memory/src/store/session.rs` — 移除 #![allow(deprecated)]
- `crates/memory/src/session_store.rs` — 移除 #![allow(deprecated)]
- 删除: `crates/runtime/src/session_manager.rs`
- 删除: `crates/api/src/provider_chain.rs`
- 删除: `crates/runtime/src/task_graph.rs`
- `crates/runtime/src/trust_resolver.rs` — 移除 #[cfg(test)] 门控
- `Cargo.toml` — 版本 0.6.2→0.6.7 + workspace 依赖标准化
- `crates/config/Cargo.toml` + `crates/memory/Cargo.toml` — thiserror v1→v2

### Definition of Done
- [x] `cargo build --release` 零错误 + 零警告
- [x] `cargo test` 全部通过 (456 + 相关测试)
- [x] 所有 `Handle::current().block_on()` 在生产路径中已消除或安全
- [x] 所有 `.lock().unwrap()` 在生产代码中已转换为 `into_inner()` 模式
- [x] 版本号统一到 0.6.7

### Must Have
- 所有崩溃 Bug 修复
- 锁中毒模式统一
- 版本提升

### Must NOT Have (Guardrails)
- 不新增功能（纯修复）
- 不删除仍在使用的任何代码
- 不改变公共 API 签名
- 不引入新依赖

---

## Verification Strategy (MANDATORY)

### Test Decision
- **Infrastructure exists**: YES
- **Automated tests**: YES (TDD)
- **Framework**: cargo test
- **TDD 策略**: 每个修复任务包含 RED（编写失败测试验证Bug存在）→ GREEN（最小修复）→ REFACTOR

### QA Policy
每个任务包含 agent-executed QA 场景，验证修复后行为正确。

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately - foundation + 独立修复, MAX PARALLEL 6):
├── Task 1: 版本提升 0.6.2 → 0.6.7 + cargo test [quick]
├── Task 2: permissions.rs:116 lock().expect() → into_inner() [quick]
├── Task 3: cognitive.rs:1479 lock().unwrap() → into_inner() [quick]
├── Task 4: provider_pool.rs:46 空Vec守卫 [quick]
├── Task 5: thiserror v1→v2 统一 + workspace 依赖标准化 [quick]
└── Task 6: server WebSocket handler fix (mod.rs:2301) [deep]

Wave 2 (After Wave 1 — 依赖 Wave 1 的修复):
├── Task 7: server_execute_turn fix (mod.rs:3322) [deep]
├── Task 8: orchestrator.rs:315 MutexGuard across .await [deep]
└── Task 9: 移除 deprecated allow — conversation.rs [quick]

Wave 3 (After Wave 2 — 清理):
├── Task 10: 移除 deprecated allow — session.rs + session_store.rs [quick]
├── Task 11: 删除 runtime session_manager.rs (死代码) [quick]
├── Task 12: 删除/处理 ProviderChain 死代码 [quick]
├── Task 13: 处理 trust_resolver #[cfg(test)] 门控 [quick]
├── Task 14: 处理 SubAgentExecutor 命名冲突 [quick]
├── Task 15: 清理 17 构建警告 + dead_code 审计 [quick]
└── Task 16: 构建验证 + 全量测试 [quick]

Wave FINAL (After ALL tasks — 并行审查):
├── F1: Plan compliance audit (oracle)
├── F2: Code quality review (unspecified-high)
├── F3: Real manual QA (unspecified-high)
└── F4: Scope fidelity check (deep)
```

Critical Path: Task 1 → Task 5 → Task 6 → Task 7 → Task 16 → F1-F4

---

## TODOs

- [x] 1. **版本提升 0.6.2 → 0.6.7**

  **What to do**:
  - 修改 `/media/yi/Datas/workspace/cowd/Cargo.toml`: `version = "0.6.7"`
  - `cargo build --release` 验证
  - `cargo test -p cowd-memory --lib` 验证 456 通过
  - `grep -rn "0\.6\.[0-9]" --include="*.toml" | grep version` 确认唯一版本

  **Must NOT do**:
  - 不跨越到 0.7.x 大版本
  - 不修改其他 Cargo.toml（workspace 继承自动更新）

  **QA Scenarios** (Agent-Executed):
  ```
  Scenario: 版本验证
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
      3. grep "^version" Cargo.toml
    Expected Result: 零错误，456 passed，version = "0.6.7"
    Evidence: .omo/evidence/task-1-version-bump.txt
  ```

  **Commit**: YES
  - Message: `chore: bump version 0.6.2 → 0.6.7`

- [x] 2. **permissions.rs:116 — lock().expect() → into_inner()**

  **What to do**:
  - 读取 `crates/runtime/src/permissions.rs:115-120`
  - 替换 `self.inner.lock().expect("SharedPrompter lock poisoned")` 为:
    ```rust
    self.inner.lock().unwrap_or_else(|e| e.into_inner())
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变返回类型或函数签名
  - 不影响其他 lock() 调用

  **QA Scenarios**:
  ```
  Scenario: 锁中毒恢复
    Tool: Bash
    Steps:
      1. cargo test --lib -p runtime 2>&1 | grep "test result"
      2. grep -n "lock().expect" crates/runtime/src/permissions.rs
    Expected Result: 零错误，零 expect 残留
    Evidence: .omo/evidence/task-2-permissions-fix.txt
  ```

  **Commit**: Groups with T3, T4

- [x] 3. **cognitive.rs:1479 — lock().unwrap() → into_inner()**

  **What to do**:
  - 读取 `crates/runtime/src/provider_pool.rs:43-48`
  - 在 `let idx = ...` 前添加空检查:
    ```rust
    if self.clients.is_empty() {
        return Box::pin(futures::stream::once(async {
            Err(RuntimeError::new("ProviderPool: no clients configured"))
        }));
    }
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变 ProviderPool 的外部接口
  - 不影响 idx 计算逻辑

  **QA Scenarios**:
  ```
  Scenario: 空 ProviderPool 安全返回错误
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. cargo test --lib -p runtime 2>&1 | grep "test result"
    Expected Result: 零编译错误，现有测试全部通过
    Evidence: .omo/evidence/task-4-providerpool-guard.txt
  ```

  **Commit**: Groups with T2, T3

  **What to do**:
  - 读取 `crates/runtime/src/permissions.rs:115-120`
  - 替换 `self.inner.lock().expect("SharedPrompter lock poisoned")` 为:
    ```rust
    self.inner.lock().unwrap_or_else(|e| e.into_inner())
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变返回类型或函数签名
  - 不影响其他 lock() 调用

  **QA Scenarios**:
  ```
  Scenario: 锁中毒恢复
    Tool: Bash
    Steps:
      1. cargo test --lib -p runtime 2>&1 | grep "test result"
      2. grep -n "lock().expect" crates/runtime/src/permissions.rs
    Expected Result: 零错误，零 expect 残留
    Evidence: .omo/evidence/task-2-permissions-fix.txt
  ```

  **Commit**: Groups with T3, T4

- [x] 4. **provider_pool.rs:46 — 空Vec索引守卫**

  **What to do**:
  - 读取 `crates/runtime/src/provider_pool.rs:43-48`
  - 在 `let idx = ...` 前添加空检查:
    ```rust
    if self.clients.is_empty() {
        return Box::pin(futures::stream::once(async {
            Err(RuntimeError::new("ProviderPool: no clients configured"))
        }));
    }
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变 ProviderPool 的外部接口
  - 不影响 idx 计算逻辑

  **QA Scenarios**:
  ```
  Scenario: 空 ProviderPool 安全返回错误
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. cargo test --lib -p runtime 2>&1 | grep "test result"
    Expected Result: 零编译错误，现有测试全部通过
    Evidence: .omo/evidence/task-4-providerpool-guard.txt
  ```

  **Commit**: Groups with T2, T3

- [x] 5. **thiserror v1→v2 统一 + workspace 依赖标准化**

  **What to do**:
  - 修改 `crates/config/Cargo.toml`: `thiserror = "1"` → `thiserror = "2"`
  - 修改 `crates/memory/Cargo.toml`: `thiserror = "1"` → `thiserror = "2"`
  - 修改 `/Cargo.toml` `[workspace.dependencies]`，添加:
    ```toml
    serde = { version = "1", features = ["derive"] }
    tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros"] }
    chrono = { version = "0.4", features = ["serde"] }
    tracing = "0.1"
    reqwest = { version = "0.12", features = ["json", "stream"] }
    uuid = { version = "1", features = ["v4"] }
    futures = "0.3"
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0
  - `cargo build --release 2>&1 | grep "thiserror" | wc -l` 应显示仅 v2 编译

  **Must NOT do**:
  - 不移除任何 crate 中现有的独立依赖声明（先标准化，后续逐步替换）

  **QA Scenarios**:
  ```
  Scenario: thiserror 不再双重编译
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep "Compiling thiserror" | wc -l
      2. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
    Expected Result: 只编译一个 thiserror 版本，456 passed
    Evidence: .omo/evidence/task-5-thiserror-unify.txt
  ```

  **Commit**: YES
  - Message: `chore: unify thiserror to v2 + expand workspace dependencies`

- [x] 6. **server/mod.rs:2301 — WebSocket 处理器嵌套 enter_runtime 修复**

  **What to do**:
  - 读取 `crates/cowd-cli/src/server/mod.rs:2298-2310`（WebSocket handler 中的 `Handle::current().block_on(run_turn_async)` 块）
  - 替换为 `std::thread::spawn` 模式（与 main.rs 修复一致）:
    ```rust
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().expect("ws turn rt");
        let result = rt.block_on(runtime.run_turn_async(&user_input, &runtime::permissions::SharedPrompter::none()));
        let _ = tx.send(result);
    });
    let content_result = rx.recv().map_err(|_| ...)?;
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变响应格式
  - 不影响其他 WebSocket 逻辑

  **QA Scenarios**:
  ```
  Scenario: WebSocket turn 不崩溃
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. grep -n "Handle::current()" crates/cowd-cli/src/server/mod.rs | grep -v "try_current"
    Expected Result: 零编译错误，零 Handle::current() 残留
    Evidence: .omo/evidence/task-6-ws-fix.txt
  ```

  **Commit**: Groups with T7

- [x] 7. **server/mod.rs:3322 — server_execute_turn 嵌套 enter_runtime 修复**

  **What to do**:
  - 读取 `crates/cowd-cli/src/server/mod.rs:3318-3328`
  - 替换 `tokio::runtime::Handle::current().block_on(runtime.run_turn_async(input, &prompter))` 为:
    ```rust
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().expect("server turn rt");
        let result = rt.block_on(runtime.run_turn_async(input, &prompter));
        let _ = tx.send(result);
    });
    let summary = rx.recv().map_err(|_| ...)?;
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变返回类型
  - 不影响其他 server 函数

  **QA Scenarios**:
  ```
  Scenario: server_execute_turn 不崩溃
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. grep -n "Handle::current()" crates/cowd-cli/src/server/mod.rs | grep -v "try_current"
    Expected Result: 零编译错误，零 Handle::current() 残留
    Evidence: .omo/evidence/task-7-server-fix.txt
  ```

  **Commit**: Groups with T6

- [x] 8. **orchestrator.rs:315 — MutexGuard 跨越 .await 修复**

  **What to do**:
  - 读取 `crates/memory/src/orchestrator.rs:310-320`
  - 重构 `rebuild_closet`:
    ```rust
    pub async fn rebuild_closet(&self) -> Result<()> {
        let rebuilt = ClosetManager::build_from_orchestrator(self).await?;
        *self.closet.lock() = rebuilt;
        Ok(())
    }
    ```
  - 关键: 在 `.await` 完成后才获取锁
  - `cargo test -p cowd-memory --lib 2>&1 | grep "test result"` 确认 456 通过

  **Must NOT do**:
  - 不改变函数公共签名
  - 不改变 ClosetManager::build_from_orchestrator 行为

  **QA Scenarios**:
  ```
  Scenario: rebuild_closet 不持有锁跨越 await
    Tool: Bash
    Steps:
      1. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
      2. sed -n '310,320p' crates/memory/src/orchestrator.rs
    Expected Result: 456 passed，.await 前锁已释放
    Evidence: .omo/evidence/task-8-orchestrator-fix.txt
  ```

  **Commit**: YES
  - Message: `fix: orchestrator rebuild_closet — drop MutexGuard before .await`

- [x] 9. **移除 #![allow(deprecated)] — conversation.rs**

  **What to do**:
  - 读取 `crates/runtime/src/conversation.rs:1` — 确认 `#![allow(deprecated)]` 存在
  - 删除该行
  - `cargo build --release 2>&1 | grep -c "^error"` 检查是否有废弃 API 使用暴露
  - 如有废弃 API 使用 → 逐一修复
  - 最终: `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不删除其他 `#![allow(...)]` 属性
  - 不改变文件其余部分

  **QA Scenarios**:
  ```
  Scenario: conversation.rs 不再绕过 deprecated deny
    Tool: Bash
    Steps:
      1. grep "^#\!\[allow(deprecated)" crates/runtime/src/conversation.rs
      2. cargo build --release 2>&1 | grep -c "^error"
    Expected Result: 未找到 allow(deprecated)，零编译错误
    Evidence: .omo/evidence/task-9-conversation-deny.txt
  ```

  **Commit**: Groups with T10, T11

- [x] 10. **移除 #![allow(deprecated)] — store/session.rs + session_store.rs**

  **What to do**:
  - 读取 `crates/memory/src/store/session.rs:1` — 删除 `#![allow(deprecated)]`
  - 读取 `crates/memory/src/session_store.rs:1` — 删除 `#![allow(deprecated)]`
  - `cargo test -p cowd-memory --lib 2>&1 | grep "test result"` 确认 456 通过
  - 如有废弃 API 使用暴露 → 逐一修复

  **Must NOT do**:
  - 不删除其他文件属性

  **QA Scenarios**:
  ```
  Scenario: memory crate 全部 deny deprecated
    Tool: Bash
    Steps:
      1. grep -rn "#\!\[allow(deprecated)" crates/memory/src/
      2. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
    Expected Result: 未找到 allow(deprecated)，456 passed
    Evidence: .omo/evidence/task-10-memory-deny.txt
  ```

  **Commit**: Groups with T9, T11

- [x] 11. **删除 runtime session_manager.rs 死代码**

  **What to do**:
  - 确认零外部引用: `grep -rn "InMemorySessionManager\|SessionManager" crates/cowd-cli/src/ crates/runtime/src/ --include="*.rs" | grep -v "session_manager.rs" | grep -v "test"`
  - 删除 `crates/runtime/src/session_manager.rs`
  - 修改 `crates/runtime/src/lib.rs:61` — 移除 `pub mod session_manager;`
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不删除 `crates/memory/src/session_manager.rs`（内存 crate 的独立 session manager）

  **QA Scenarios**:
  ```
  Scenario: 运行时 session_manager 已删除
    Tool: Bash
    Steps:
      1. ls crates/runtime/src/session_manager.rs 2>&1
      2. cargo build --release 2>&1 | grep -c "^error"
    Expected Result: 文件不存在，零编译错误
    Evidence: .omo/evidence/task-11-delete-session-mgr.txt
  ```

  **Commit**: Groups with T9, T10

- [x] 12. **处理 ProviderChain 死代码**

  **What to do**:
  - 确认零外部调用: `grep -rn "ProviderChain" crates/ --include="*.rs" | grep -v "provider_chain.rs" | grep -v "lib.rs"`
  - 删除 `crates/api/src/provider_chain.rs`
  - 修改 `crates/api/src/lib.rs` — 移除 ProviderChain 相关的 `pub use` / `pub mod`
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不删除 `ProviderClient` enum（在 client.rs 中，正在使用）
  - 不删除 `ProviderKind` 枚举

  **QA Scenarios**:
  ```
  Scenario: ProviderChain 已移除
    Tool: Bash
    Steps:
      1. ls crates/api/src/provider_chain.rs 2>&1
      2. cargo build --release 2>&1 | grep -c "^error"
    Expected Result: 文件不存在，零编译错误
    Evidence: .omo/evidence/task-12-delete-providerchain.txt
  ```

  **Commit**: YES
  - Message: `cleanup: remove dead ProviderChain code (398 lines)`

- [x] 13. **处理 trust_resolver #[cfg(test)] 门控**

  **What to do**:
  - 读取 `crates/runtime/src/lib.rs:70-71,210-211`
  - 移除 `#[cfg(test)]` 修饰符从 `mod trust_resolver` 和 `pub use trust_resolver::{...}`
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0
  - 验证 `crates/runtime/src/trust_resolver.rs` 无需额外修改即编译通过

  **Must NOT do**:
  - 不修改 trust_resolver.rs 内部逻辑
  - 如果编译失败（生产环境缺少依赖），则评估是否需要保留 #[cfg(test)]

  **QA Scenarios**:
  ```
  Scenario: trust_resolver 生产可用
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. grep -n "cfg(test).*trust_resolver" crates/runtime/src/lib.rs
    Expected Result: 零编译错误，零 cfg(test) trust_resolver 残留
    Evidence: .omo/evidence/task-13-trust-resolver.txt
  ```

  **Commit**: Groups with T14, T15

- [x] 14. **处理 SubAgentExecutor 命名冲突**

  **What to do**:
  - 确认零外部调用: `grep -rn "subagent_executor::SubAgentExecutor" crates/ --include="*.rs"`
  - 如无引用 → 删除 `crates/runtime/src/subagent_executor.rs` 中的 struct SubAgentExecutor<C,T>
  - 从 `lib.rs` 移除不需要的导出
  - 保留 `crates/runtime/src/agent.rs` 中的 `trait SubAgentExecutor` + `StubExecutor`
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不删除 agent.rs 中的 trait SubAgentExecutor
  - 不删除 StubExecutor 测试存根

  **QA Scenarios**:
  ```
  Scenario: SubAgentExecutor 命名冲突已解决
    Tool: Bash
    Steps:
      1. grep -rn "struct SubAgentExecutor" crates/runtime/src/
      2. cargo build --release 2>&1 | grep -c "^error"
    Expected Result: 仅 trait 残留（无 struct），零编译错误
    Evidence: .omo/evidence/task-14-subagent-cleanup.txt
  ```

  **Commit**: Groups with T13, T15

- [x] 15. **清理 17 构建警告 + dead_code 审计**

  **What to do**:
  - `cargo build 2>&1 | grep "warning:" | grep -v "imap-proto"` → 获取所有警告列表
  - 运行 `cargo fix --bin cowd -p cowd-cli --allow-dirty` 自动修复未使用导入
  - 手动处理自动化无法修复的警告:
    - `conversation.rs`: `record_assistant_iteration`, `record_tool_finished`, `build_assistant_message`, `flush_text_block`, `flush_thinking_block`, `format_hook_message`, `merge_hook_feedback` → 删除或添加 forced `#[allow(dead_code)]`
    - `server/mod.rs`: `KeepAlive`, `chrono::Utc`, `ReceiverStream`, `TurnOutcome`, `RecvTimeoutError`, `Instant`, `format_usd`, `pricing_for_model`, `EventStream`, `Read` → 移除未使用导入
    - `main.rs`: `compact` 变量 → 删除
  - `cargo build 2>&1 | grep -c "warning:"` 仅剩 imap-proto 外部警告

  **Must NOT do**:
  - 不修改 imap-proto 依赖（外部 crate）

  **QA Scenarios**:
  ```
  Scenario: 零警告（imap-proto 除外）
    Tool: Bash
    Steps:
      1. cargo build 2>&1 | grep "warning:" | grep -v "imap-proto" | wc -l
      2. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
    Expected Result: 0 内部警告，456 passed
    Evidence: .omo/evidence/task-15-zero-warnings.txt
  ```

  **Commit**: Groups with T13, T14

- [x] 16. **全量构建验证 + 测试 + 崩溃日志验证**

  **What to do**:
  - `cargo build --release` → 零错误零警告（imap-proto 除外）
  - `cargo test -p cowd-memory --lib` → 456 passed
  - `cargo test --lib -p runtime` → 全部通过
  - 构建 TUI: `cp target/release/cowd /home/yi/AI/cowd`
  - 启动 TUI + 发送测试消息 → 验证零 crash.log 新条目
  - `git diff --stat HEAD` → 确认仅预期的文件被修改

  **Must NOT do**:
  - 不跳过任何测试

  **QA Scenarios**:
  ```
  Scenario: 全量验证
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
      3. B=$(wc -l < /home/yi/.cowd/crash.log)
      4. 启动 TUI 发送消息
      5. A=$(wc -l < /home/yi/.cowd/crash.log); echo "$((A-B)) new"
    Expected Result: 零错误，456 passed，0 new crashes
    Evidence: .omo/evidence/task-16-full-verify.txt
  ```

  **Commit**: YES
  - Message: `verify: full build + test + crash regression after audit fixes`

  **What to do**:
  - 确认零外部调用: `grep -rn "ProviderChain" crates/ --include="*.rs" | grep -v "provider_chain.rs" | grep -v "lib.rs"`
  - 删除 `crates/api/src/provider_chain.rs`
  - 修改 `crates/api/src/lib.rs` — 移除 ProviderChain 相关的 `pub use` / `pub mod`
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不删除 `ProviderClient` enum（在 client.rs 中，正在使用）
  - 不删除 `ProviderKind` 枚举

  **QA Scenarios**:
  ```
  Scenario: ProviderChain 已移除
    Tool: Bash
    Steps:
      1. ls crates/api/src/provider_chain.rs 2>&1
      2. cargo build --release 2>&1 | grep -c "^error"
    Expected Result: 文件不存在，零编译错误
    Evidence: .omo/evidence/task-12-delete-providerchain.txt
  ```

  **Commit**: YES
  - Message: `cleanup: remove dead ProviderChain code (398 lines)`

  **What to do**:
  - 读取 `crates/memory/src/orchestrator.rs:310-320`
  - 重构 `rebuild_closet`:
    ```rust
    pub async fn rebuild_closet(&self) -> Result<()> {
        let rebuilt = ClosetManager::build_from_orchestrator(self).await?;
        *self.closet.lock() = rebuilt;
        Ok(())
    }
    ```
  - 关键: 在 `.await` 完成后才获取锁
  - `cargo test -p cowd-memory --lib 2>&1 | grep "test result"` 确认 456 通过

  **Must NOT do**:
  - 不改变函数公共签名
  - 不改变 ClosetManager::build_from_orchestrator 行为

  **QA Scenarios**:
  ```
  Scenario: rebuild_closet 不持有锁跨越 await
    Tool: Bash
    Steps:
      1. cargo test -p cowd-memory --lib 2>&1 | grep "test result"
      2. sed -n '310,320p' crates/memory/src/orchestrator.rs
    Expected Result: 456 passed，.await 前锁已释放
    Evidence: .omo/evidence/task-8-orchestrator-fix.txt
  ```

  **Commit**: YES
  - Message: `fix: orchestrator rebuild_closet — drop MutexGuard before .await`

  **What to do**:
  - 读取 `crates/runtime/src/provider_pool.rs:43-48`
  - 在 `let idx = ...` 前添加空检查:
    ```rust
    if self.clients.is_empty() {
        return Box::pin(futures::stream::once(async {
            Err(RuntimeError::new("ProviderPool: no clients configured"))
        }));
    }
    ```
  - `cargo build --release 2>&1 | grep -c "^error"` 必须为 0

  **Must NOT do**:
  - 不改变 ProviderPool 的外部接口
  - 不影响 idx 计算逻辑

  **QA Scenarios**:
  ```
  Scenario: 空 ProviderPool 安全返回错误
    Tool: Bash
    Steps:
      1. cargo build --release 2>&1 | grep -c "^error"
      2. cargo test --lib -p runtime 2>&1 | grep "test result"
    Expected Result: 零编译错误，现有测试全部通过
    Evidence: .omo/evidence/task-4-providerpool-guard.txt
  ```

  **Commit**: Groups with T2, T3

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

- [x] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists. For each "Must NOT Have": search codebase. Check evidence files exist in .omo/evidence/.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [x] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo build --release` + `cargo test`. Review all changed files for: `as any`/`@ts-ignore`, empty catches, console.log in prod, commented-out code, unused imports. Check AI slop.
  Output: `Build [PASS/FAIL] | Tests [N pass/N fail] | Warnings [N] | VERDICT`

- [x] F3. **Real Manual QA** — `unspecified-high`
  Start from clean state. Run TUI with `--solo`. Send test message. Verify no crash.log entries. Run HTTP API if applicable.
  Output: `TUI [PASS/FAIL] | Crash log [0 new] | VERDICT`

- [x] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual diff. Verify 1:1. Check "Must NOT do" compliance.
  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | VERDICT`

---

## Commit Strategy

- **Wave 1**: `fix(audit): version bump + lock poison fixes + provider_pool guard + thiserror unification`
- **Wave 2**: `fix(audit): server block_on fixes + orchestrator MutexGuard`
- **Wave 3**: `cleanup(audit): remove deprecated allows + dead code + build warnings`

---

## Success Criteria

### Verification Commands
```bash
cargo build --release 2>&1 | grep -E "^error|^warning" | wc -l  # Expected: 0
cargo test --lib 2>&1 | grep "test result"  # Expected: all passed
grep -rn "Handle::current()" crates/cowd-cli/src/server/mod.rs  # Expected: 0 (all fixed)
grep -rn "\.lock()\.unwrap()" crates/memory/src/cognitive.rs  # Expected: 0 (not counting tests)
```

### Final Checklist
- [x] All "Must Have" present
- [x] All "Must NOT Have" absent
- [x] All tests pass
- [x] Zero new crash.log entries
