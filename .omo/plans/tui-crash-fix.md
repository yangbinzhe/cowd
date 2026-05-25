# TUI 崩溃修复方案 —— 基于 crash.log 的完整分析

## 崩溃日志摘要

```
=== CRASH [1779637226] 2026-05-24 15:40 ===
prompt.rs:843  → index out of bounds: len is 2 but index is 3

=== CRASH [1779640575] 2026-05-24 16:36 ===
main.rs:2839  → there is no reactor running, must be called from Tokio context

=== CRASH [1779676844/64] 2026-05-25 02:40-41 ===
tokio multi_thread/mod.rs:91 → Cannot start a runtime from within a runtime
```

## 根因分析

### 崩溃 1: prompt.rs UTF-8 越界
**根因**：`current_word_from_text()` 中 `let col = last_line.len()` 获取的是**字节长度**，但 `chars: Vec<char>` 是**字符数**。CJK 文本如"你好a"(bytes=7, chars=3) → `chars[6]` 越界。

**状态**：✅ 已在 Wave 1 Task 2 中修复（改用 `chars.len()`）。但需确认 `textarea.lines()` 是否存在并发安全问题。

**残留风险**：`textarea.lines()` 返回内部缓冲区的引用，如果渲染和事件处理在同一线程交替不当，可能拿到脏数据。tui_textarea 0.7 本身是单线程安全的，但需验证。

### 崩溃 2: main.rs:2839 — 无 Tokio Reactor
**根因**：TUI 在 `std::thread::spawn` 中创建运行时。旧代码用 `run_turn`（内部有 `try_current().unwrap_or_else(Runtime::new())`），迁移到 `run_turn_async` 后，`rt.block_on()` 设置了运行上下文。但如果 `run_turn_async` 内部调用链中有代码尝试在 `block_in_place` 中执行 `Handle::current()`，而当前线程运行时(current_thread) 不支持 `block_in_place` 的线程切换语义，会导致 panic。

**具体点**：`conversation.rs:970-971` 在 `run_turn_async` 中：
```rust
tokio::task::block_in_place(|| {
    handle.block_on(self.prepare_memory_context(&user_input))
});
```
`block_in_place` 在 current_thread 运行时中可能不正常工作。

### 崩溃 3: 嵌套 Runtime
**根因**：`Runtime::new()` 在已有运行时上下文的线程中被调用。所有 `conversation.rs` 中的 `Runtime::new()` 都有 `try_current()` 守卫，但**`main.rs:3110` 处的 `RuntimeMcpState::new()` 没有守卫**：

```rust
// main.rs:3110 — UNGUARDED!
let runtime = tokio::runtime::Runtime::new()?;
```

如果 `RuntimeMcpState::new()` 从已有运行时上下文的线程被调用（如通过 `ConversationRuntime` 回调间接触发），会直接崩溃。

## 修复方案（5 个改动）

### Fix 1: RuntimeMcpState — 添加 try_current 守卫
```rust
// main.rs:3110
let runtime = match tokio::runtime::Handle::try_current() {
    Ok(handle) => handle,  // 复用现有运行时
    Err(_) => tokio::runtime::Runtime::new()?.handle().clone(),
};
```

### Fix 2: run_turn_async — 移除 block_in_place
`block_in_place` 在 current_thread 运行时中不安全。改为直接 `block_on`：
```rust
// conversation.rs:968-971
let handle = tokio::runtime::Handle::try_current()
    .unwrap_or_else(|_| tokio::runtime::Runtime::new().expect("tokio runtime fallback").handle().clone());
handle.block_on(self.prepare_memory_context(&user_input))
```

### Fix 3: ApiClient::stream_collect — 同样移除 block_in_place
```rust
// conversation.rs:94-98  — 已经是 handle.block_on(async { ... })，没有 block_in_place，OK
```
检查确认即可，不需要改动。

### Fix 4: 检查 run_turn 是否仍有外部调用
已确认只有 `LiveCli::run_turn()`（包装`run_turn_async`）和测试代码使用。生产代码无直接调用。

### Fix 5: TextArea lines() 并发保护
在 `prompt.rs` 的 `current_word_from_text()` 中，增加基于字符位置的边界检查：
```rust
let chars: Vec<char> = last_line.chars().collect();
let max_len = chars.len();
if max_len == 0 { return String::new(); }
let mut start = max_len;
while start > 0 && start - 1 < max_len {
    let c = chars[start - 1];
    if c.is_whitespace() { break; }
    start = start.saturating_sub(1);
}
let end = max_len.min(start.saturating_add(max_len));
chars[start..end].iter().collect()
```
（使用 `saturating_sub` 和边界检查防止负值/越界）

### 验证标准
- `cargo build --release` 零警告
- `cargo test -p cowd-memory --lib` 447/447 PASS
- 模拟 CJK 输入 + 快速输入切换 30 秒无崩溃
- 连发 10 次 `cowd prompt "test"` 无运行时崩溃
