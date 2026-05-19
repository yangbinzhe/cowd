# Cowd 计划2 — Phase 3: TUI 事件驱动重构

> 优先级：P0 | 前置：Phase 0 + Phase 1 | 工时：12h
> GitNexus 数据：8 个 stdout 泄漏点，run_tui_repl 调用 19 个模块

## 核心目标

消除前两次分析识别的 **P0-2**：8 个 stdout 泄漏点全部迁移到 EventBus

## 问题溯源（GitNexus 验证）

main.rs 中 stdout 泄漏点：

| # | 行号 | 函数 | 写入方式 | 内容 |
|---|------|------|---------|------|
| 1 | 3266 | run_tui_repl | spinner.tick() | "🦀 Thinking..." |
| 2 | 3277 | run_tui_repl | spinner.finish() | "✔ ✨ Done" |
| 3 | 3282 | run_tui_repl | println!() | 空行 |
| 4 | 3284 | run_tui_repl | println!() | auto-compaction 通知 |
| 5 | 3294 | run_tui_repl | spinner.fail() | "✘ ❌ Request failed" |
| 6 | 6541 | run_tui_repl | write!(out) | 流式 Markdown 渲染 |
| 7 | 6663-6834 | consume_stream | write!(stdout) | 文本流 |
| 8 | 7208-7287 | format_tool_* | println!() | 工具调用/结果 |

## 执行步骤

### Step 3.1: TUI Logger 适配 (2h)

**目标**：所有文本输出改为 EventBus::publish(RuntimeEvent::TextDelta)

**修改文件**：`crates/cowd-cli/src/tui/turn.rs`

**关键改造**（consume_stream 函数）：
```rust
// 旧代码 (行 6663-6834)
fn consume_stream(&mut self, stream: MessageStream) {
    while let Some(event) = stream.next().await {
        match event {
            StreamEvent::TextDelta(text) => {
                write!(stdout, "{}", text)?;  // ❌ stdout 泄漏
            }
            // ...
        }
    }
}

// 新代码
fn consume_stream(&mut self, stream: MessageStream, bus: &EventBus) {
    while let Some(event) = stream.next().await {
        match event {
            StreamEvent::TextDelta(text) => {
                bus.publish(RuntimeEvent::TextDelta {  // ✅ EventBus
                    text: text.clone(),
                    turn_id: self.turn_id.clone(),
                })?;
            }
            StreamEvent::ToolStart { id, name } => {
                bus.publish(RuntimeEvent::ToolStart { id, name })?;
            }
            // ...
        }
    }
}
```

**验证**：`cargo test -p cowd-cli -- turn::no_stdout`

---

### Step 3.2: Spinner 移除 (1h)

**目标**：spinner 视觉效果改为 TUI thinking_panel widget

**修改点**：
- 删除 `main.rs` 中所有 `spinner.tick()` / `spinner.finish()` / `spinner.fail()`
- 替换为 `EventBus::publish(ThinkingDelta)` 事件
- TUI 渲染循环中 `thinking_panel` widget 根据事件更新

**验证**：TUI 模式下 spinner 不再写 stdout

---

### Step 3.3: Tool Card 事件驱动 (2h)

**目标**：工具状态完整生命周期通过 EventBus 呈现

**修改文件**：`crates/cowd-cli/src/tui/turn.rs` + `widgets/tools.rs`

```rust
// 工具生命周期事件流
ToolStart → tool widget 出现 (⏳)
  ToolProgress → tool widget 更新进度
  ToolProgress → tool widget 更新进度
  ToolComplete(summary, exit_code) → tool widget 显示结果 (✅/❌)
```

**验证**：`cargo test -p cowd-cli -- tui::tool_card`

---

### Step 3.4: TUI 渲染循环整合 (3h)

**目标**：事件收集 → 状态更新 → 渲染 三个步骤解耦

**修改文件**：`crates/cowd-cli/src/tui/mod.rs`

```rust
fn run_tui_event_loop(args: CliArgs) -> Result<()> {
    let (tx, rx) = mpsc::channel(256);
    let bus = EventBus::new(256);
    let mut app = App::new(args);
    let mut terminal = ratatui::init()?;

    loop {
        // 1. 收集事件
        while let Ok(event) = rx.try_recv() {
            app.handle_event(event);
        }

        // 2. 处理输入
        if let Some(text) = input_handler.poll() {
            spawn_turn(text, tx.clone(), bus.clone());
        }

        // 3. 渲染
        terminal.draw(|f| {
            app.render(f);
        })?;

        // 4. 帧率控制
        if app.should_quit { break; }
    }

    ratatui::restore()?;
    Ok(())
}
```

**验证**：`cargo test -p cowd-cli -- tui::render_loop`

---

### Step 3.5: E2E 验证 (2h)

**清除验证**：
```bash
# TUI 模式 stdout 泄漏检查
cowd --tui 2>/tmp/tui_stderr.log &
# 发送 3 个 turn 后退出
# 检查 /tmp/tui_stderr.log 仅含日志, 无 println!/write! 输出
```

**性能验证**：
```bash
# 帧率检查
cowd --tui --benchmark  # 渲染帧率 > 60fps
```

**回归测试**：
```bash
cargo test -p cowd-cli -- mock_parity
cargo test -p runtime
cargo test -p commands
cargo test --workspace
```

---

## 并行执行方案

```
Step 3.1 (consume_stream 改造) ── 需先完成
        ↓
Step 3.2 (spinner 移除) ──┐
        │                   ├── 并行（独立 widget）
Step 3.3 (tool card)  ────┘
        ↓
Step 3.4 (渲染循环) ── 依赖 3.1~3.3
        ↓
Step 3.5 (E2E 验证)
```

## 验收标准（AC）

| AC | 描述 | 验证方法 |
|----|------|---------|
| AC-1 | 零 stdout 泄漏 | `cowd --tui 2>/tmp/log` → log 为空 |
| AC-2 | 流式渲染 | 文本逐字追加，< 16ms 延迟 |
| AC-3 | 工具卡片生命周期 | ToolStart→Progress→Complete 三阶段正确 |
| AC-4 | Thinking 面板 | 使用 thinking model 时显示 thinking 内容 |
| AC-5 | 错误恢复 | 模型错误后 TUI 不冻结 |
| AC-6 | 帧率 | 空闲 < 10fps，流式 > 30fps |
