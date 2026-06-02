# TUI 5问题完备修复方案 v2 (Oracle校正版)

> 基于 Oracle 根因审计 | v0.8.7 | 工期: 1-2天

---

## Q1: 键盘滚动失效

### 根因 (Oracle校正)

1. **`j/k` 被 `process_raw_key:1148` 拦截为光标导航** — 输入为空时跳过了滚动
2. **`Up/Down/PgUp/PgDn/Home/End` 被 `is_textarea_key` 拦截** — 送至textarea(keybind引擎收不到)
3. **`TextDelta` 无条件设置 `auto_scroll=true`** (`app.rs:776`) — 流式输出时覆盖用户手动滚动
4. **`sync_to_app()` 从未调用** — ChatView滚动变更丢失

### 修复

**1a. 键盘滚动恢复** (`state.rs:1148-1158`)
```rust
// 修改前: j/k → cursor_up/down
// 修改后: j/k → 通过keybind引擎分发为Scroll(1)/Scroll(-1)
// 删除空输入时j/k的特殊拦截
```

**1b. is_textarea_key放开导航键** (`state.rs:1243`)
```rust
// 从is_textarea_key中移除: Up, Down, PageUp, PageDown, Home, End
// 这些键不再传给textarea，而是落入keybind引擎 → Scroll/ScrollPage动作
```

**1c. TextDelta不覆盖手动滚动** (`app.rs:774-776`)
```rust
// 修改前: self.auto_scroll = true;  // 无条件
// 修改后: 仅在auto_scroll已是true时才保持; 用户手动滚走后不再强制拉回
if self.auto_scroll {
    // 已经是true → 继续保持（用户未手动滚走）
} else {
    // 用户手动滚走了 → 不强制拉回，但添加"新内容提示"
}
```

**1d. 调用sync_to_app** (`state.rs:625-627`)
```rust
// 修改前: self.chat_view.scroll_state.offset = self.app.scroll_offset; // 方向反了
// 修改后: self.chat_view.sync_to_app(&mut self.app); // 正确方向
```

---

## Q2: 自动贴底恢复缺失

### 根因

`TurnStarted`、`TurnComplete`、`ToolProgress` 事件处理中缺失 `self.auto_scroll = true`

### 修复

```rust
// app.rs:892 TurnStarted → 添加 self.auto_scroll = true;
// app.rs:904 TurnComplete → 添加 self.auto_scroll = true;  
// app.rs:844 ToolProgress → 添加 self.auto_scroll = true;
```

---

## Q3: 多次"done"消息

### 根因 (Oracle校正)

`TurnComplete` 事件可能被重复发射。`app.rs:921` 每次 `TurnComplete` 都追加一条 "✓ Done"。

### 修复

```rust
// app.rs:921-925 — 在push "✓ Done" 之前检查去重
if !self.timeline_has_recent_done() {
    self.timeline_push(TimelineEntry::Message {
        role: "assistant".into(),
        content: "✓ Done".into(),
        ...
    });
}
// 新增方法: 检查timeline最后一条是否已是 "✓ Done"
fn timeline_has_recent_done(&self) -> bool {
    self.timeline.last().map_or(false, |e| {
        matches!(e, TimelineEntry::Message { content, .. } if content == "✓ Done")
    })
}
```

---

## Q4: grep_search 频繁失败

### 根因

1. `WalkDir::new()` 无过滤 — 遍历所有文件(包括.git/node_modules/target)
2. ReadOnly超时仅30秒
3. LLM生成无效正则导致0-1ms秒失败
4. 无重试、无文件大小检查

### 修复

**4a. 添加目录过滤** (`file_ops.rs:452-465`)
```rust
fn collect_search_files(base_path: &Path) -> io::Result<Vec<PathBuf>> {
    let skip_dirs = ["target", "node_modules", ".git", ".cargo", ".gitnexus"];
    for entry in WalkDir::new(base_path)
        .max_depth(20)
        .into_iter()
        .filter_entry(|e| !skip_dirs.iter().any(|d| e.file_name().to_str() == Some(d)))
    { ... }
}
```

**4b. 增加超时** (`tool_orchestrator.rs:77`)
```rust
// ReadOnly超时: 30s → 120s (与WriteLocal/Network一致)
```

**4c. LLM输入校验** (`executor.rs:853`)
```rust
// 在from_value之前检查pattern是否为空或无效
if input.pattern.trim().is_empty() {
    return Err("grep_search: pattern must not be empty".into());
}
```

**4d. 添加文件大小检查** (`file_ops.rs:379`)
```rust
let max_size = 10 * 1024 * 1024; // 10MB
if std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) > max_size {
    continue; // skip large files
}
```

---

## Q5: TUI启动缓慢

### 根因 (Oracle校正)

daemon复用**已正确实现**。真正缓慢根源：`LiveCli::new()` → `build_runtime()` 每次完整初始化插件+MCP+工具注册+ConversationRuntime。

### 修复

**5a. 延迟MCP初始化** (`main.rs:3431`)
```rust
// RuntimeMcpState::new() 中的 discover_tools_best_effort() — 改为延迟加载
// 仅在首次工具调用时才连接MCP服务器，而非启动时
// 此时仅记录服务器配置，不连接
```

**5b. 缓存PluginState** 
```rust
// 将build_runtime_plugin_state()的结果缓存在OnceLock中
// 后续TUI启动时直接复用
static CACHED_PLUGIN_STATE: OnceLock<PluginState> = OnceLock::new();
```

**5c. 添加启动计时日志**
```rust
let t0 = std::time::Instant::now();
// build_runtime() ...
tracing::info!(elapsed_ms = t0.elapsed().as_millis(), "TUI runtime built");
```

---

## 执行计划

| 问题 | 文件 | 行数 | 工期 |
|------|------|------|------|
| Q1 滚动 | state.rs, app.rs, chat_view.rs | ~40 | 2h |
| Q2 自动贴底 | app.rs | ~10 | 30min |
| Q3 done消息 | app.rs | ~15 | 30min |
| Q4 grep修复 | file_ops.rs, tool_orchestrator.rs, executor.rs | ~30 | 1h |
| Q5 启动优化 | main.rs | ~30 | 2h |
| **总计** | 6文件 | ~125 | 6h |

*待Oracle审核*
