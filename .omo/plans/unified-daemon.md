# COWD 统一守护进程设计

## 当前架构 vs 设计目标

```
当前（问题）:
  TUI 模式: std::thread::spawn → Builder::new_current_thread().build() ← 每次 turn 创建新 Runtime
  Server 模式: Runtime::new() → axum ← 独立的 Runtime
  问题: 多 Runtime 冲突 → tokio "Cannot start runtime from within runtime"

目标:
  单一 Runtime 守护进程，同时提供:
  - HTTP API（axum，对远程客户端）
  - TUI （local crossterm，对本地用户）
  - 共享 ConversationRuntime + Memory System
```

## 统一进程架构

```
┌──────────────────────────────────────────────────┐
│                Main Process                       │
│                                                   │
│  ┌────────────────────────────────────────────┐  │
│  │       Tokio Runtime (multi_thread)          │  │
│  │       worker_threads: 4                     │  │
│  │                                             │  │
│  │  ┌──────────────┐   ┌───────────────────┐  │  │
│  │  │  Axum Server │   │   TUI Thread       │  │  │
│  │  │  (async)     │   │   (std::thread)    │  │  │
│  │  │              │   │                    │  │  │
│  │  │  POST /chat  │   │  键盘→事件循环     │  │  │
│  │  │  GET /status │   │  Handle::block_on  │  │  │
│  │  └──────┬───────┘   └────────┬──────────┘  │  │
│  │         │                    │              │  │
│  │         ▼                    ▼              │  │
│  │  ┌─────────────────────────────────────┐   │  │
│  │  │    ConversationRuntime (Arc)        │   │  │
│  │  │    ┌─────────────────────────────┐ │   │  │
│  │  │    │ Memory System               │ │   │  │
│  │  │    │ Tool Executor               │ │   │  │
│  │  │    │ UsageTracker                │ │   │  │
│  │  │    └─────────────────────────────┘ │   │  │
│  │  └─────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## 关键设计决策

### 1. 单一 Tokio Runtime

```rust
// main.rs: 启动时创建，全局持有
let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(4)
    .enable_all()
    .build()?;

// 全局 handle，TUI 和 Server 共用
let handle = rt.handle().clone();
```

### 2. TUI 使用共享 Runtime 的 Handle

不再每次 turn 创建新 Runtime，也不使用 std::thread::spawn。替代：

```rust
// TUI turn 通过 Handle::spawn_blocking 在 Runtime 的 blocking pool 中执行
let handle = rt.handle().clone();
let result = handle.spawn_blocking(move || {
    // 在 Runtime 的 blocking pool 中执行，可以用 Handle::current()
    let runtime = ConversationRuntime::new_with_features(...);
    runtime.run_turn_async(input, &prompter)
}).await?;
```

不——`spawn_blocking` 运行同步代码。我们需要运行异步函数。更好的方式：

```rust
// 方案A: 直接在当前 tokio 上下文执行
let handle = rt.handle().clone();
handle.block_on(async {
    runtime.run_turn_async(input, &prompter).await
})
```

但这会阻塞 TUI 主事件循环。替代方案：

```rust
// 方案B: TUI 使用 channel 提交任务，在 Runtime 内部执行
let (tx, rx) = oneshot::channel();
rt.spawn(async move {
    let result = runtime.run_turn_async(input, &prompter).await;
    let _ = tx.send(result);
});
// TUI 线程收到结果前不阻塞，继续处理输入事件
tokio::select! {
    result = rx => { /* 处理完成 */ }
    _ = tui_event_stream => { /* 处理键盘事件 */ }
}
```

### 3. TUI 主循环异步化

当前 TUI 用 `crossterm` 的同步事件循环。改为 `tokio-crossterm` 的异步事件：

```rust
let mut reader = crossterm::event::EventStream::new();
loop {
    tokio::select! {
        // TUI 键盘事件
        Some(Ok(event)) = reader.next() => {
            state.handle_event(event);
        }
        // Turn 完成
        result = turn_rx => {
            state.handle_turn_complete(result);
        }
        // tick (动画、定时器)
        _ = tokio::time::sleep(Duration::from_millis(50)) => {
            state.tick();
        }
    }
    state.render()?;
}
```

### 4. API 和 TUI 共享状态

```rust
struct SharedAppState {
    // HTTP Server 和 TUI 共享
    runtime: tokio::runtime::Handle,
    config: RuntimeConfig,
    model: Arc<RwLock<String>>,
    // Session 管理：HTTP 和 TUI 创建不同的 session
    session_store: Arc<UnifiedSessionStore>,
    memory_manager: Arc<CognitiveContextManager>,
    tool_registry: Arc<GlobalToolRegistry>,
}

// Axum
let app_state = Arc::new(SharedAppState { ... });
let app = Router::new()
    .route("/chat", post(chat_handler))
    .route("/status", get(status_handler))
    .with_state(app_state.clone());

// TUI
let tui = TuiState::new(app_state.clone());
```

## 实现路线

### Phase 1: 统一 Runtime（1-2天）
- 在 main() 中创建单一 multi_thread Runtime
- 将所有 Runtime::new() 替换为 handle.clone()
- 验证 TUI 不再崩溃

### Phase 2: TUI 异步事件循环（2-3天）
- 将 crossterm 事件循环改为 tokio::select!
- 用 channel 替代 std::thread::spawn
- Turn 执行在 Runtime 内部，TUI 通过 channel 接收结果

### Phase 3: Server + TUI 共存（2-3天）
- axum server 和 TUI 共享 Runtime
- 统一状态管理
- `cowd` 启动后同时开启 API 和 TUI

### 收益
| 维度 | 当前 | 设计后 |
|------|------|--------|
| Runtime 数量 | 每 turn 1个 | 全局 1 个 |
| 崩溃风险 | 高（嵌套 Runtime） | 消失 |
| 资源开销 | 每 turn 创建/销毁 Runtime | 共享，低开销 |
| 代码复杂度 | TUI + Server 两套路径 | 统一路径 |
| 功能 | TUI 或 Server 二选一 | 同时可用 |
