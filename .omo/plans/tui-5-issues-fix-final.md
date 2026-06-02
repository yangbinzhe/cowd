# TUI 5问题 — 最终执行计划 (Oracle 4修正已应用)

> Oracle审定: 4项修正 | v0.8.7 | TDD | 预估: 5h

---

## Q1-F1: 删除j/k空输入时的光标导航拦截

**文件**: `crates/cowd-cli/src/tui/state.rs:1148-1158`
**操作**: 删除整个if块 (11行)
**理由**: 删除后 j/k 流入keybind引擎 → `Action::Scroll(±1)` → scroll正常工作

---

## Q1-F2: TextDelta条件性auto_scroll (Oracle修正)

**文件**: `crates/cowd-cli/src/tui/app.rs:776`
**修改**:
```rust
// 修改前:
self.auto_scroll = true;

// 修改后:
// 仅在用户未手动滚走时自动贴底
if self.auto_scroll {
    // 已经是true — 保持
} else {
    // 用户手动滚走了 — 不强制拉回
    // (TurnStarted/TurnComplete会恢复)
}
```
**效果**: 用户手动滚动后，后续TextDelta不再拉回底部

---

## Q1-F3: sync_to_app调用 (Oracle修正)

**文件**: `crates/cowd-cli/src/tui/state.rs`
**修改**: 在 `chat_view.render()` 之后**新增** `sync_to_app()` 调用（不删除现有625-626行）

```rust
// 第521行: chat_view.render(&mut main_ctx, chat_area);
// 新增 (在625行之前):
self.chat_view.sync_to_app(&mut self.app);

// 保留原有625-627行不变 (app→chat_view方向正确)
```

---

## Q2-F1/F2/F3: 自动贴底恢复

**文件**: `crates/cowd-cli/src/tui/app.rs`

```rust
// TurnStarted (line 892): 新增 self.auto_scroll = true;
// TurnComplete (line 904): 新增 self.auto_scroll = true;
// Note: ToolProgress (line 844) — 不添加(工具进度不改变内容位置)
```

---

## Q3-F1: 去重检查

**文件**: `crates/cowd-cli/src/tui/app.rs:921`

```rust
// 在 push "✓ Done" 之前:
if !self.timeline_has_recent_done() {
    self.timeline_push(TimelineEntry::Message {
        role: "assistant".into(),
        content: "✓ Done".into(), ...
    });
}
```

## Q3-F2: timeline_last()替代实现 (Oracle修正)

**文件**: `crates/cowd-cli/src/tui/app.rs`

```rust
fn timeline_has_recent_done(&self) -> bool {
    let len = self.timeline_len();
    if len == 0 { return false; }
    self.timeline_get(len - 1).map_or(false, |e| {
        matches!(e, TimelineEntry::Message { content, .. } if content == "✓ Done")
    })
}
```

---

## Q4-F1: WalkDir添加过滤

**文件**: `crates/runtime/src/file_ops.rs:452` (`collect_search_files`)

```rust
let skip_dirs = ["target", "node_modules", ".git", ".cargo", ".gitnexus"];
WalkDir::new(base_path)
    .max_depth(20)
    .into_iter()
    .filter_entry(|e| !skip_dirs.iter().any(|d| e.file_name().to_str() == Some(d)))
```

## Q4-F2: ReadOnly超时30s→120s

**文件**: `crates/runtime/src/tool_orchestrator.rs:77`

```rust
// 修改: Duration::from_secs(30) → Duration::from_secs(120)
```

## Q4-F3: 文件大小检查

**文件**: `crates/runtime/src/file_ops.rs:379` (read_to_string之前)

```rust
let max_size = 10 * 1024 * 1024;
if std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) > max_size {
    continue;
}
```

## Q4-F4: 空白正则检查 (Oracle修正)

**文件**: `crates/runtime/src/file_ops.rs:335` (NOT executor.rs)

```rust
// 修改前: Some(p) if !p.is_empty() => p.as_str(),
// 修改后: Some(p) if !p.trim().is_empty() => p.as_str(),
```

---

## Q5-F1/F2/F3: 启动优化

**Q5-F1**: `main.rs:3431` — `discover_tools_best_effort()` 调用改延迟(注释掉,仅记录配置)
**Q5-F2**: `main.rs:6977` — 添加 `OnceLock<Arc<PluginState>>` 缓存
**Q5-F3**: `main.rs` — 添加 `Instant::now()` 计时日志

---

## TDD 测试

```rust
// Q1: j/k scrolls when input empty
#[test] fn test_j_scrolls_not_cursor_nav() { /* scroll_offset+1 */ }

// Q3: done dedup
#[test] fn test_done_not_duplicated() { /* 2x TurnComplete → 1 "✓ Done" */ }

// Q4: grep timeout
#[test] fn test_read_only_timeout_is_120s() { /* assert_eq */ }
```

---

## 验证清单

- [ ] `j/k` 空输入时滚动 (scroll_offset变化)
- [ ] 流式输出中手动滚走不被拉回
- [ ] 新轮次自动贴底
- [ ] "✓ Done" 仅一次
- [ ] grep不再30s超时
- [ ] 启动日志显示耗时
- [ ] `cargo build --workspace` 通过
- [ ] 现有TUI面板无异常
