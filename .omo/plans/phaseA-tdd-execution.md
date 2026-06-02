# Phase A TDD执行方案 v2 (Oracle修正)

> Oracle: 并行假象已修正, TDD真实可执行, 事件流已修复

## 前置: daemon.rs子模块拆分 (1h)

```
crates/cowd-cli/src/daemon/
  mod.rs (run_daemon, 保持)
  singletons.rs (OnceLock全局单例) ← A1
  commands.rs (handle_unix_client) ← A2+A3
  prompter.rs (SocketPrompter)     ← A4
```

## 执行顺序 (修正并行)

```
Step 0: daemon子模块拆分 (1h)

Step 1: A1 (singletons.rs) + A2 (commands.rs) — 并行(不同文件)

Step 2: A3 (conversation.rs) + A4 (prompter.rs) — 并行(不同文件)
```

---

## A1: 全局单例 (3h) — singletons.rs

### TDD: RED (可执行)

```rust
#[test]
fn test_global_plugin_is_singleton() {
    // 手动set后验证get返回同一实例
    let state = build_test_plugin_state();
    GLOBAL_PLUGIN.set(Arc::new(state)).unwrap(); // 仅可set一次
    let a = GLOBAL_PLUGIN.get().unwrap();
    assert_eq!(Arc::strong_count(a), 1);
}
```

### 实现
`daemon/singletons.rs`: 3个 OnceLock + init函数

---

## A2: create_session注入全能力 (3h) — commands.rs

### TDD: RED (可执行)

```rust
#[tokio::test]
async fn test_create_session_response_includes_session_id() {
    let sessions = Arc::new(ActiveSessions::default());
    // 模拟socket收发
    let (response, _) = handle_create_session(&sessions, &json!({"cmd":"create_session","model":"claude"})).await;
    assert!(response["ok"].as_bool().unwrap());
    assert!(!response["session_id"].as_str().unwrap().is_empty());
}
```

### 实现
`daemon/commands.rs`: create_session命令 + memory_manager注入

---

## A3: chat_stream (6h) — conversation.rs + commands.rs

### TDD: RED (可执行 — 使用CowdEventBus, 非AssistantEvent)

```rust
#[tokio::test]
async fn test_chat_stream_emits_text_delta() {
    // 创建runtime → 订阅CowdEventBus → 发送chat → 验证TextDelta到达
    let mut rx = runtime.cowd_bus().unwrap().subscribe();
    // spawn chat_stream...
    let event = rx.recv().await.unwrap();
    assert!(matches!(event, CowdEvent::TextDelta { .. }));
}
```

### 8个事件 (非9个 — ApprovalRequested不在CowdEvent流中)

| TextDelta | ToolStart | ToolProgress | ToolComplete |
| ThinkingDelta | TurnComplete | TurnError | TokenUsage |

### 实现
`conversation.rs`: 新增TurnError/TokenUsage发射
`daemon/commands.rs`: chat_stream命令(spawn_blocking)

---

## A4: 工具审批 (4h) — prompter.rs

### TDD: RED (可执行)

```rust
#[test]
fn test_socket_prompter_decide_allow() {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut prompter = SocketPrompter::new(tx);
    let request = PermissionRequest {
        tool_name: "bash".into(),
        input: "ls".into(),
        current_mode: PermissionMode::WorkspaceWrite,
        required_mode: PermissionMode::WorkspaceWrite,
        reason: None,
    };
    // 模拟: TUI发送tool_approve → prompter.decide返回Allow
    // 在测试中手动设置pending oneshot
}
```

### 实现
`daemon/prompter.rs`: SocketPrompter + tool_approve/deny处理

---

## 验证

```bash
cargo build -p cowd-cli
cargo test daemon::singletons     # A1
cargo test daemon::commands       # A2+A3
cargo test daemon::prompter       # A4
cargo test runtime::conversation  # A3事件
```
