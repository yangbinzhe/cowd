# Phase 1 Session 修补 — 并发安全 + 事件总线完善

## TL;DR

> **Quick Summary**: 修补 Session Phase 1 的3项遗留问题：conversation.rs 的 `std::sync::RwLock` → `tokio::sync::RwLock` 迁移（消除死锁风险）、bus.rs 补充缺失 SSE 事件类型、session_store.rs `std::sync::Mutex` → `tokio::sync::Mutex`。
> 
> **Deliverables**:
> - conversation.rs 30+ 处 RwLock 调用全部异步化
> - bus.rs 新增 ThinkingDelta/ToolStart/ToolProgress/ToolComplete/SignatureDelta 事件
> - session_store.rs 异步锁迁移
> 
> **Estimated Effort**: Short (4-6h)
> **Parallel Execution**: YES — 3 tasks can run in parallel
> **Critical Path**: R1 (RwLock) is the most invasive; R2+R3 are independent

---

## Context

### Original Request
基于 `plan/0527-分析/02-Session/` 的 Phase 1 计划回溯审计，发现3项遗留未完成。

### Audit Results
- ✅ 1.1 合并冲突: 已解决（0 conflict markers）
- ❌ 1.2 RwLock迁移: `conversation.rs:5` 仍 `use std::sync::{Arc, RwLock}` — 30+ 处 `.read().unwrap()` 跨 await 点
- ❌ 1.2 Mutex迁移: `session_store.rs:52` 仍 `Arc<Mutex<SqliteSessionStore>>`
- ✅ 1.3 TextDelta去重: 已完成（仅1个变体）
- ❌ 1.3 SSE事件: `bus.rs` 缺少 ThinkingDelta/ToolStart/ToolProgress/ToolComplete/SignatureDelta

---

## Work Objectives

### Core Objective
消除 conversation.rs 中 std::sync::RwLock 跨 .await 点的死锁风险，补全 EventBus SSE 事件类型。

### Definition of Done
- [ ] `cargo build --workspace` 零错误
- [ ] 所有 `self.session.read().unwrap()` → `self.session.read().await`
- [ ] `bus.rs` 包含全部8种事件类型
- [ ] `session_store.rs` 无 std::sync::Mutex

### Must NOT Have
- **NOT** 新增 crate 依赖
- **NOT** 修改 Session 数据结构本身
- **NOT** 改变 API 路由的公开签名
- **NOT** 引入 `block_in_place` 阻塞（全部用 async）

---

## Verification Strategy

### Test Decision
- **Infrastructure exists**: YES
- **Automated tests**: Tests-after
- **Framework**: `cargo test -p runtime -p cowd-memory`

---

## Execution Strategy

```
Wave 1 (3 parallel tasks):
├── R1: conversation.rs std::sync::RwLock → tokio::sync::RwLock [deep] (most invasive)
├── R2: bus.rs SSE events + conversation.rs wiring [quick]
└── R3: session_store.rs std::sync::Mutex → tokio::sync::Mutex [quick]

Wave FINAL (2 parallel):
├── F1: Regression test + build verification
└── F2: Plan compliance audit
```

---

## TODOs

- [ ] 1. R1 — conversation.rs `std::sync::RwLock` → `tokio::sync::RwLock`

  **What to do**:
  This is the most invasive change. There are 30+ call sites using `self.session.read().unwrap()` and `self.session.write().unwrap()` that must all become async.

  **Step 1**: Change import at line 5
  ```rust
  // DELETE: use std::sync::{Arc, RwLock};
  // ADD: use std::sync::Arc;
  // ADD: use tokio::sync::RwLock;
  ```

  **Step 2**: Field type — line 243 (unchanged, just import changes semantics)
  ```rust
  session: Arc<RwLock<Session>>,  // now tokio::sync::RwLock
  ```

  **Step 3**: Constructor — line 386
  ```rust
  // RwLock::new() works for both std and tokio, no syntax change needed
  let session = Arc::new(RwLock::new(session));
  ```

  **Step 4**: Replace ALL `.read().unwrap_or_else(|e| e.into_inner())` with `.read().await`
  Key locations (search pattern `self.session.read()`):
  - line 754: `self.session.read().await.messages.is_empty()`
  - line 777: `self.session.read().await.compaction.is_some()`
  - line 807: `&*self.session.read().await`
  - line 811: `&*self.session.read().await`
  - line 816: `&*self.session.read().await`
  - line 824: `self.session.read().await.messages.clone()`
  - line 1345: `&*self.session.read().await`
  - line 1350: `&*self.session.read().await`
  - line 1360: `self.session.read().await.clone()`
  - line 1378: `arc.read().await.clone()`
  - line 1385: `&*self.session.read().await`
  - line 1392: `&self.session.read().await`

  **Step 5**: Replace ALL `.write().unwrap_or_else(|e| e.into_inner())` with `.write().await`
  - line 786: `self.session.write().await`
  - line 988: `self.session.write().await.push_message(...)`
  - line 1042: `self.session.write().await`
  - line 1129: `self.session.write().await.push_user_text(...)`
  - line 1205: `self.session.write().await.push_message(...)`
  - line 1247: `self.session.write().await.push_message(...)`
  - line 1309: `self.session.write().await.push_message(...)`
  - line 1317: `self.session.write().await.push_message(...)`

  **Step 6**: Update `into_session()` at line 1378
  ```rust
  // OLD: Arc::try_unwrap(self.session).map(|lock| lock.into_inner().unwrap_or_else(|e| e.into_inner().clone())).unwrap_or_else(|arc| arc.read().unwrap_or_else(|e| e.into_inner()).clone())
  // NEW:
  let session = Arc::try_unwrap(self.session)
      .map(|lock| lock.into_inner())
      .unwrap_or_else(|arc| arc.blocking_read().clone());
  ```

  **Step 7**: `session()` method (line 1360) — keep sync but use blocking_read
  ```rust
  pub fn session(&self) -> Session {
      self.session.blocking_read().clone()
  }
  ```

  **Step 8**: `session_mut()` method (line 1367) — keep sync but use blocking_write
  ```rust
  pub fn session_mut(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
      self.session.blocking_write()
  }
  ```

  **Must NOT do**:
  - Do NOT make `session()` async (many callers are in sync context)
  - Do NOT use `block_in_place` (blocking_read/blocking_write is sufficient)
  - Do NOT change Session struct itself

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**: Can run in parallel with R2, R3
  **Blocked By**: None

  **References**:
  - `crates/runtime/src/conversation.rs:5` — import change
  - `crates/runtime/src/conversation.rs:754,777,786,807,811,816,824,988,1042,1129,1205,1247,1309,1317,1345,1350,1360,1367,1378,1385,1392` — all call sites

  **Acceptance Criteria**:
  - [ ] `cargo build -p runtime` passes
  - [ ] `grep -c "std::sync::RwLock" crates/runtime/src/conversation.rs` = 0
  - [ ] `grep -c "\.read()\.unwrap\|\.write()\.unwrap" crates/runtime/src/conversation.rs` = 0

  **Commit**: YES — `fix(session): replace std::sync::RwLock with tokio::sync::RwLock in ConversationRuntime`

- [ ] 2. R2 — bus.rs SSE事件类型补充 + conversation.rs wiring

  **What to do**:
  **Step 1**: In `crates/runtime/src/bus.rs`, extend the Event enum:
  ```rust
  #[derive(Debug, Clone)]
  pub enum Event {
      SessionCreated { id: String },
      SessionDeleted { id: String },
      TextDelta { content: String },
      ThinkingDelta { content: String },                          // NEW
      ToolStart { id: String, name: String, preview: String },    // NEW
      ToolProgress { id: String, name: String, progress: String },// NEW
      ToolComplete { id: String, name: String, summary: String, exit_code: Option<i32> }, // NEW
      SignatureDelta { signature: String },                       // NEW
      TurnCompleted { tokens: u32, model: String },
      ToolExecuted { name: String, duration_ms: u64 },
      MemoryExtracted { count: usize },
      ApprovalRequested { tool: String },
  }
  ```

  **Step 2**: In conversation.rs, wire new events in the stream handler. Search for `Ok(AssistantEvent::ThinkingDelta` and add bus.emit:
  ```rust
  Ok(AssistantEvent::ThinkingDelta(thinking)) => {
      // ... existing code ...
      if let Some(ref bus) = self.bus {
          bus.emit(crate::bus::Event::ThinkingDelta { content: thinking.clone() });
      }
  }
  ```
  Same pattern for ToolStart, ToolProgress, ToolComplete, SignatureDelta.

  **Must NOT do**:
  - Do NOT remove existing event types
  - Do NOT change TextDelta emission (already correct)

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**: Can run in parallel with R1, R3
  **Blocked By**: None

  **References**:
  - `crates/runtime/src/bus.rs` — entire file (66 lines)
  - `crates/runtime/src/conversation.rs` — search for `AssistantEvent::ThinkingDelta`, `ToolStart`, `ToolProgress`, `ToolComplete`, `SignatureDelta`

  **Acceptance Criteria**:
  - [ ] `cargo build -p runtime` passes
  - [ ] `grep -c "ThinkingDelta\|ToolStart\|ToolProgress\|ToolComplete\|SignatureDelta" crates/runtime/src/bus.rs` ≥ 5

  **Commit**: YES — `feat(bus): add SSE event types (ThinkingDelta, ToolStart, ToolProgress, ToolComplete, SignatureDelta)`

- [ ] 3. R3 — session_store.rs `std::sync::Mutex` → `tokio::sync::Mutex`

  **What to do**:
  **Step 1**: Change import at line 26
  ```rust
  // DELETE: use std::sync::{Arc, Mutex};
  // ADD: use std::sync::Arc;
  // ADD: use tokio::sync::Mutex;
  ```

  **Step 2**: Field type — line 52 (unchanged, semantics change via import)
  **Step 3**: Constructor — lines 67, 75 (unchanged, `Mutex::new()` works for both)

  **Step 4**: Replace ALL `.lock().unwrap()` with `.lock().await`
  Search pattern: `self.inner.lock()` in session_store.rs
  All methods in the impl block that access `self.inner.lock()` must become async:
  - `create_session()` → `async fn`
  - `get_session()` → `async fn`
  - `update_session()` → `async fn`
  - `delete_session()` → `async fn`
  - etc.

  **Step 5**: Update all callers of UnifiedSessionStore methods to use `.await`
  - `crates/cowd-cli/src/main.rs`
  - `crates/cowd-cli/src/daemon.rs`
  - `crates/cowd-cli/src/api_routes.rs`
  - Any test files

  **Must NOT do**:
  - Do NOT change the SQLite connection pattern (already handled internally)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**: Can run in parallel with R1, R2
  **Blocked By**: None

  **References**:
  - `crates/memory/src/session_store.rs` — entire file
  - Callers: `grep -rn "UnifiedSessionStore\|session_store" crates/cowd-cli/src/`

  **Acceptance Criteria**:
  - [ ] `cargo build -p cowd-memory -p cowd-cli` passes
  - [ ] `grep -c "std::sync::Mutex" crates/memory/src/session_store.rs` = 0

  **Commit**: YES — `fix(session): replace std::sync::Mutex with tokio::sync::Mutex in UnifiedSessionStore`

---


- [ ] F1. **Build + Test Verification** — `deep`
  `cargo build --workspace && cargo test -p runtime && cargo test -p cowd-memory`
  Output: `Build [PASS/FAIL] | Tests [N pass/N fail] | VERDICT`

- [ ] F2. **Plan Compliance** — `oracle`
  Verify: 0 std::sync::RwLock in conversation.rs, bus.rs has 5 new events, 0 std::sync::Mutex in session_store.rs

---

## Commit Strategy

- **R1**: `fix(session): replace std::sync::RwLock with tokio::sync::RwLock in ConversationRuntime` — conversation.rs
- **R2**: `feat(bus): add SSE event types (ThinkingDelta, ToolStart, ToolProgress, ToolComplete, SignatureDelta)` — bus.rs, conversation.rs
- **R3**: `fix(session): replace std::sync::Mutex with tokio::sync::Mutex in UnifiedSessionStore` — session_store.rs

---

## Success Criteria

```bash
cargo build --workspace
cargo test -p runtime
grep -c "std::sync::RwLock" crates/runtime/src/conversation.rs  # 应输出 0
grep -c "ThinkingDelta\|ToolStart\|ToolProgress\|ToolComplete" crates/runtime/src/bus.rs  # 应输出 5+
```
