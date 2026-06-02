# TUI 5问题 — 详细执行计划 (Oracle审定版)

> 基于 Oracle 根因审计 v2 | v0.8.7 | TDD模式 | 预估: 5h

---

## Q1: 键盘滚动失效

### 根因 (Oracle确认)
1. `j/k` 被 `state.rs:1148` 拦截为光标导航(仅空输入时)
2. `TextDelta` 无条件设置 `auto_scroll=true` (`app.rs:776`)
3. `sync_to_app()` 从未调用 (`chat_view.rs:156`)

### Oracle否定项
❌ Up/Down/PgUp/PgDn/Home/End **不在** is_textarea_key中 — 早已到达keybind引擎。无需修改。

### 修复

**Q1-F1: 删除j/k空输入时的光标导航拦截** (`state.rs:1148-1158`)
```rust
// 删除这段代码:
if self.app.input.is_empty()
    && key.modifiers.is_empty()
    && matches!(key.code, KeyCode::Char('j' | 'k'))
{
    if key.code == KeyCode::Char('j') { self.app.cursor_down(); }
    else { self.app.cursor_up(); }
    return ProcessedKey::Nothing;
}
// 删除后 j/k 将流入 keybind 引擎 → Action::Scroll(±1)
```

**Q1-F2: TextDelta 不覆盖用户手动滚动** (`app.rs:774-776`)
```rust
// 修改前:
self.auto_scroll = true;

// 修改后: 仅在没被用户手动滚走时保持
// (auto_scroll 在用户按j/k/Up/Down时已被 dispatch_action 设为false)
// 不修改 — 保持原样，让用户按 End 键重新启用 auto_scroll
// 真正的修复: 添加"新内容提示"指示器
```

**Q1-F3: 调用 sync_to_app() 替代反向同步** (`state.rs:625-627`)
```rust
// 修改前: self.chat_view.scroll_state.offset = self.app.scroll_offset;
// 修改后: self.chat_view.sync_to_app(&mut self.app);
```

---

## Q2: 自动贴底恢复缺失

### 修复

**Q2-F1: TurnStarted 恢复 auto_scroll** (`app.rs:892`)
```rust
CowdEvent::TurnStarted { ... } => {
    self.auto_scroll = true;  // 新增
    // ... existing code
}
```

**Q2-F2: TurnComplete 恢复 auto_scroll** (`app.rs:904`)
```rust
CowdEvent::TurnComplete { ... } => {
    self.auto_scroll = true;  // 新增
    // ... existing code
}
```

**Q2-F3: ToolProgress 恢复 auto_scroll** (`app.rs:844`)
```rust
CowdEvent::ToolProgress { ... } => {
    self.auto_scroll = true;  // 新增
    // ... existing code
}
```

---

## Q3: 多次"done"消息

### 修复

**Q3-F1: 去重检查** (`app.rs:921-925`)
```rust
// 在 push "✓ Done" 之前添加检查:
if !self.timeline_has_recent_done() {
    self.timeline_push(TimelineEntry::Message {
        role: "assistant".into(),
        content: "✓ Done".into(),
        ...
    });
}
```

**Q3-F2: 新增去重辅助方法**
```rust
fn timeline_has_recent_done(&self) -> bool {
    self.timeline.last().map_or(false, |e| {
        matches!(e, TimelineEntry::Message { content, .. } if content == "✓ Done")
    })
}
```

---

## Q4: grep_search 频繁失败

### 修复

**Q4-F1: WalkDir添加目录过滤** (`file_ops.rs:452-465`)
```rust
fn collect_search_files(base_path: &Path) -> io::Result<Vec<PathBuf>> {
    if base_path.is_file() { return Ok(vec![base_path.to_path_buf()]); }
    let skip_dirs = ["target", "node_modules", ".git", ".cargo", ".gitnexus"];
    let mut files = Vec::new();
    for entry in WalkDir::new(base_path)
        .max_depth(20)
        .into_iter()
        .filter_entry(|e| !skip_dirs.iter().any(|d| e.file_name().to_str() == Some(d)))
    { ... }
}
```

**Q4-F2: ReadOnly超时30s→120s** (`tool_orchestrator.rs:77`)
```rust
ToolSafetyCategory::ReadOnly => Duration::from_secs(120),  // was 30
```

**Q4-F3: 文件大小检查** (`file_ops.rs:379`)
```rust
let max_size = 10 * 1024 * 1024; // 10MB
if std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) > max_size {
    continue;
}
```

**Q4-F4: 空白正则检查** (`executor.rs:853`)
```rust
if input.pattern.trim().is_empty() {
    return Err("grep_search: pattern must not be empty or whitespace-only".into());
}
```

---

## Q5: TUI启动缓慢

### 修复

**Q5-F1: 延迟MCP初始化** (`main.rs:3431`)
```rust
// RuntimeMcpState::new() — discover_tools_best_effort() 改为延迟
// 仅存储服务器配置，首次工具调用时才连接
```

**Q5-F2: 缓存PluginState** (`main.rs:6977`)
```rust
static CACHED_PLUGIN_STATE: OnceLock<PluginState> = OnceLock::new();
// 首次构建后缓存，后续TUI启动直接复用
```

**Q5-F3: 启动计时日志**
```rust
let t0 = std::time::Instant::now();
// ... build_runtime()
tracing::info!(elapsed_ms = t0.elapsed().as_millis(), "TUI runtime built");
```

---

## TDD测试规格

```rust
// Q1: j/k scrolls when input empty
#[test]
fn test_j_key_scrolls_when_input_empty() {
    state.input = String::new();
    state.app.scroll_offset = 5;
    let event = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE);
    state.process_raw_key(event);
    assert_eq!(state.app.scroll_offset, 6); // scrolled down
}
// Q3: done dedup
#[test]
fn test_done_not_duplicated() {
    app.apply_event(TurnComplete { ... });
    app.apply_event(TurnComplete { ... }); // second time
    let done_count = app.timeline.iter()
        .filter(|e| matches!(e, Message { content, .. } if content == "✓ Done")).count();
    assert_eq!(done_count, 1);
}
```

---

## 验证清单

- [ ] `j/k` 空输入时滚动而非光标导航
- [ ] 用户手动滚走后流式输出不拉回底部
- [ ] 新轮次开始时自动贴底
- [ ] "✓ Done" 仅出现一次
- [ ] grep 在大型仓库中不超时
- [ ] TUI启动日志显示耗时
