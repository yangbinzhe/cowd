# v0.6.0: Async TUI Event Loop — 彻底消除崩溃

## 原理

当前崩溃根因：`std::thread::spawn` + `Runtime::new()` 触发了 tokio `enter_runtime` 的跨线程检测。

解决：将 TUI 事件循环从同步 `crossterm::event::poll()` 改为异步 `crossterm::event::EventStream`。主线程运行在 SHARED_RT 内。Turn 作为 `SHARED_RT.spawn()` 异步任务执行。不再有跨线程 Runtime 创建。

## TDD 分步

### T1: crossterm EventStream 引入

**目标**: TUI 主循环能通过异步 EventStream 接收键盘事件。

**实现**:
- `main.rs` TUI loop 中替换 `event::poll()` + `event::read()` 为 `EventStream::new()`
- 使用 `tokio::select!` 同时接收 TUI 事件和 turn 结果
- 渲染保持不变

**验证**: `cargo build` 通过，TUI 启动后键盘响应正常

### T2: Turn 改为 SHARED_RT.spawn

**目标**: Turn 直接在 SHARED_RT 上执行，不创建新 Runtime。

**实现**:
- 移除 `std::thread::spawn` + `Runtime::new()`
- 改为 `SHARED_RT.spawn(async { run_turn_async().await })`
- Turn 事件仍通过 `tui_tx/tui_rx` channel 传递
- TUI select! 中接收 TurnComplete/TurnError

**验证**: Turn 执行成功，状态栏更新

### T3: 回归验证

**目标**: crash.log 零增长。

**测试**:
- 连续 3 次 TUI turn，crash.log 行数不变
- `cargo build --release` 零警告
- `cargo test -p cowd-memory --lib` 456/456

---

## 关键风险

- `run_turn_async` future 非 Send（MutexGuard）。需用 `tokio::task::LocalSet` 在当前线程执行。
- TUI 渲染必须在主线程。`select!` 的 rendering 分支在每次循环后执行。
