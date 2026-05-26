# Draft: COWD 框架审计修复方案

## 审计日期
2026-05-26

## 发现的问题总计
- 🔴 重大崩溃级 Bug: 5 个
- 🟠 锁中毒/panic 路径: 2 个
- 🟡 设计问题: 3 个
- 🔵 死代码/未启用功能: 5 个
- ⚪ 未充分利用: 3 个
- ⚠️ 构建警告: 17 个

---

## 问题清单（含文件位置、代码级别、修复方案）

### 1. server/mod.rs:2301 — WebSocket handler 内 Handle::current().block_on()
- **严重程度**: 崩溃 (CRITICAL)
- **位置**: crates/cowd-cli/src/server/mod.rs:2301
- **触发条件**: 任何 WebSocket API 连接执行 turn 时
- **根因**: Axum 异步 handler 内调用 `Handle::current().block_on(run_turn_async)`
- **修复方案**: 使用 `std::thread::spawn` + 独立 Runtime::block_on，与 main.rs TUI 修复一致
- **TDD 策略**: 先编写 WebSocket 集成测试验证 panic，修复后测试通过

### 2. server/mod.rs:3322 — server_execute_turn 内 Handle::current().block_on()
- **严重程度**: 崩溃 (CRITICAL)
- **位置**: crates/cowd-cli/src/server/mod.rs:3322
- **触发条件**: 任何 HTTP API execute_turn 调用
- **根因**: 同步函数内使用 `Handle::current()` (无 runtime 时 panic) + `block_on()` (有 runtime 时 panic)
- **修复方案**: 使用 `std::thread::spawn` + 独立 Runtime::block_on
- **TDD 策略**: 同上，编写 API 集成测试

### 3. provider_pool.rs:46 — 空 Vec 直接索引 panic
- **严重程度**: 崩溃 (CRITICAL)
- **位置**: crates/runtime/src/provider_pool.rs:46
- **触发条件**: ProviderPool 在 clients 为空时被调用
- **根因**: `self.clients.len().max(1)` 导致 idx=0，`clients[0]` panic
- **修复方案**: 添加 `if self.clients.is_empty() { return Err(...) }` 守卫
- **TDD 策略**: 编写空 ProviderPool 测试

### 4. permissions.rs:116 — lock().expect() 锁中毒 panic
- **严重程度**: 高危 (HIGH)
- **位置**: crates/runtime/src/permissions.rs:116
- **触发条件**: 任何线程持有锁时 panic 导致锁中毒
- **根因**: 项目其他所有 lock() 都已用 `into_inner()` 模式，此处遗漏
- **修复方案**: 改为 `.lock().unwrap_or_else(|e| e.into_inner())`
- **TDD 策略**: 验证代码审查 + 现有测试

### 5. cognitive.rs:1479 — lock().unwrap() 锁中毒 panic
- **严重程度**: 高危 (HIGH)
- **位置**: crates/memory/src/cognitive.rs:1479
- **触发条件**: persist_vector_index() 调用时锁中毒
- **根因**: 同样的 `lock().unwrap()` 遗漏修复
- **修复方案**: 改为 `.lock().unwrap_or_else(|e| e.into_inner())`
- **TDD 策略**: 代码审查 + 现有测试

### 6. orchestrator.rs:315 — MutexGuard 跨越 .await
- **严重程度**: 高危 (HIGH)
- **位置**: crates/memory/src/orchestrator.rs:315
- **触发条件**: rebuild_closet() 异步函数调用
- **根因**: parking_lot::MutexGuard 跨越 .await 持有，可能死锁
- **修复方案**: 提取值后释放锁，再执行 async 操作
- **TDD 策略**: 编写 rebuild_closet 测试

### 7. conversation.rs 覆盖 deprecated deny
- **严重程度**: 中 (MEDIUM)
- **位置**: crates/runtime/src/conversation.rs:1
- **根因**: 文件级 `#![allow(deprecated)]` 覆盖 crate 级 `#![deny(deprecated)]`
- **修复方案**: 移除 `#![allow(deprecated)]`，修复出现的废弃 API 错误
- **TDD 策略**: 移除后 cargo build 验证

### 8. store/session.rs + session_store.rs 覆盖 deprecated deny
- **严重程度**: 中 (MEDIUM)
- **位置**: crates/memory/src/store/session.rs:1 + crates/memory/src/session_store.rs:1
- **修复方案**: 同样移除 allow(deprecated)
- **TDD 策略**: 移除后 cargo build 验证

### 9. main.rs + 17 TUI 文件全局 dead_code allow
- **严重程度**: 中 (MEDIUM)
- **位置**: crates/cowd-cli/src/main.rs:1-6 + 17+ TUI 文件
- **修复方案**: 移除全局 `#![allow(dead_code)]`，对个别项添加 target 级 `#[allow(dead_code)]`
- **TDD 策略**: 移除后 cargo build --message-format=json 收集所有死代码

### 10. 17 构建警告 — 未使用导入/变量/函数
- **严重程度**: 低 (LOW)
- **位置**: main.rs, conversation.rs, runtime 多个文件
- **修复方案**: 运行 cargo fix + 手动清理
- **TDD 策略**: cargo build 零警告

### 11. runtime/src/session_manager.rs — 123 行死代码
- **严重程度**: 中 (MEDIUM)
- **位置**: crates/runtime/src/session_manager.rs (全部)
- **根因**: 创建后从未接入，内存 crate 有自己的 session manager
- **修复方案**: 删除整个文件，从 lib.rs 移除 mod 声明
- **TDD 策略**: 确认零外部引用后删除

### 12. SubAgentExecutor 命名冲突
- **严重程度**: 中 (MEDIUM)
- **位置**: crates/runtime/src/agent.rs:134 (trait) + subagent_executor.rs:5 (struct)
- **根因**: trait 和 struct 同名但不相关，struct 不实现 trait
- **修复方案**: 删除 struct SubAgentExecutor（未被使用）或重命名 struct
- **TDD 策略**: 确认零外部引用后处理

### 13. ProviderChain 398 行死代码
- **严重程度**: 中 (MEDIUM)
- **位置**: crates/api/src/provider_chain.rs
- **根因**: 故障转移逻辑已构建但零调用
- **修复方案**: 删除 + 从 lib.rs 移除导出，或接入 AnthropicRuntimeClient
- **TDD 策略**: 确认零外部引用后删除

### 14. trust_resolver #[cfg(test)] 门控
- **严重程度**: 低 (LOW)
- **位置**: crates/runtime/src/trust_resolver.rs + lib.rs:70-71,210-211
- **根因**: 299 行生产质量代码仅测试编译
- **修复方案**: 移除 #[cfg(test)] 使其成为生产代码（如果确实需要），或移到测试模块
- **TDD 策略**: 检查是否有生产使用价值

### 15. thiserror v1/v2 统一
- **严重程度**: 中 (MEDIUM)
- **位置**: config/Cargo.toml + memory/Cargo.toml (v1) vs 其余 (v2)
- **修复方案**: 升级 config 和 memory 到 thiserror v2
- **TDD 策略**: cargo build 零错误

### 16. Workspace 依赖标准化
- **严重程度**: 低 (LOW)
- **位置**: /Cargo.toml [workspace.dependencies]
- **修复方案**: 添加 serde, tokio, chrono, thiserror, tracing, reqwest, uuid, futures 到 workspace 依赖
- **TDD 策略**: cargo build 零错误

### 17. 版本提升 0.6.2 → 0.6.7
- **严重程度**: 低 (LOW)
- **位置**: /Cargo.toml
- **修复方案**: bump workspace version
- **TDD 策略**: 检查构建

---

## 测试策略
- **自动化测试**: YES (TDD)
- **框架**: cargo test
- **Agent QA**: 构建后运行完整测试套件

## 执行策略
- 按 Wave 并行执行独立任务
- 每个 Wave 完成后：cargo build + cargo test 验证
- 最后统一提交
