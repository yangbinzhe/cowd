# TUI 5问题根因分析与完备修复方案

> 基于 v0.8.7 代码库 + 日志分析 | Oracle审核前交付

---

## 问题1: 内容无法滚动

### 根因: 双ScrollState冲突 + 事件路由缺失

```
ChatView.scroll_state (ScrollState) ← 用户鼠标/键盘事件更新这里
          ↕ sync_from_app() 每帧覆盖
App.scroll_offset + App.auto_scroll (u16 + bool) ← 唯一真相源

冲突: 用户滚动 → ScrollState.offset 变化
     → 下一帧 sync_from_app() → App.scroll_offset 覆盖 ScrollState
     → 用户的滚动被撤销！
```

### 证据

`chat_view.rs:141-143`:
```rust
self.scroll_state.offset = app.scroll_offset;  // 每帧从App复制，覆盖用户操作
self.scroll_state.auto_scroll = app.auto_scroll;
self.scroll_state.viewport_height = app.viewport_height;
```

`app.rs:599,613,707,776,830` — App 有自己的 `auto_scroll` 逻辑，与 ScrollState 完全独立。

### 修复

**方案**: 删除 ChatView 中的独立 ScrollState，统一使用 App 的 scroll_offset/auto_scroll。鼠标/键盘事件直接更新 App 字段。

---

## 问题2: 自动滚到底部不工作

### 根因: auto_scroll 在手动滚动后未恢复

`scroll_state.rs:42-51` — `scroll_up/scroll_down` 设置 `auto_scroll = false`。此后新输出到达，`clamp()` 不再自动贴底。

但 ChatView 的 `sync_from_app()` 在第142行会从 App 恢复 `auto_scroll` — 理论上应该工作。问题在于：流式输出的过程中，`set_content_size()` 调用 `clamp()` 时 auto_scroll 已是 false（被手动滚动禁用）。

### 修复

在流式输出追加到 timeline 时，检测 `turn_active` 状态：如果正在接收流式输出且用户未手动滚动，保持 auto_scroll=true。若用户手动滚动过（用 End 键），应显式恢复 auto_scroll。

---

## 问题3: 多次输出 "done"

### 根因: ToolCall.done 字段在每个工具调用完成时被设置为 true

`app.rs:865-873`:
```rust
if let Some((output, done, expanded, ec)) = found {
    *done = true;  // 每个 tool call 完成都触发
}
```

`app.rs:923`: `content: "✓ Done"` — 单独的 "done" 条目。

当多个工具并行调用时，每个工具完成都会触发一次 "done" 状态变化，导致多条 "done" 提示。

### 修复

仅在最终 `TurnCompleted` 时追加一条 "✓ Done" 条目，而非在每个工具调用完成时。

---

## 问题4: grep_search 频繁失败

### 根因: 日志分析

```
~/.cowd/logs/cowd.2026-06-02:
  WARN: post_turn: drift failed e=storage error: Conversion error type Text at index: 0
  DEBUG: tool execution completed tool="bash" duration_ms=99 is_error=false
  DEBUG: tool execution completed tool="read_file" duration_ms=0 is_error=true ← 0ms 失败
```

`read_file` 0ms 失败表明文件路径不存在或无权限。grep_search 可能传递了不存在的路径。

需要进一步检查：`crates/tools/src/executor.rs` 中 grep_search 的实现，以及权限 Gate 是否正确允许 grep 在工作区外搜索。

### 修复

1. 确保 grep_search 接收的路径在 workspace_root 内
2. 检查 tool timeout 配置 (当前默认 120s in writeguard)
3. 失败时增加更详细的错误日志

---

## 问题5: TUI 每次进入缓慢

### 根因: 重复启动 gateway daemon

`main.rs` TUI 入口点未检查已有 daemon。每次 `cowd` 启动都重新初始化完整的 CognitiveContextManager + CompressionPipeline + SqliteStore，这需要初始化内存层、加载知识图谱、构建索引。

### 修复

1. 检查 `~/.cowd/gateway.pid` 是否存在且进程存活
2. 若 daemon 已运行，TUI 直连 unix socket
3. 避免重复初始化内存系统 (CognitiveContextManager 构造函数需要 2-3 秒)

---

## 执行方案

| 问题 | 文件 | 修复行数 | 工期 | 风险 |
|------|------|---------|------|------|
| #1 滚动 | app.rs, chat_view.rs, state.rs | ~30 | 1h | 中 |
| #2 自动贴底 | app.rs, state.rs | ~15 | 30min | 低 |
| #3 done消息 | app.rs (~L865, ~L923) | ~10 | 30min | 低 |
| #4 grep失败 | executor.rs, 权限Gate | ~15 | 1h | 低 |
| #5 启动优化 | main.rs | ~25 | 2h | 中 |

*待Oracle审核后执行*
