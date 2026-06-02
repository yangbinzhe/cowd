# Phase B: TUI改造 — TDD执行方案 v2 (Oracle重构)

> Oracle修正: 18处分三类, B2a(回合处理)+B2b(会话管理)拆分
> 工期: B2a 3h + B2b 8h = 11h

## 前置: TDD测试先行 (1h)

```rust
// crates/cowd-cli/tests/daemon_client_tests.rs

#[test]
fn test_create_session_json_format() {
    let cmd = json!({"cmd":"create_session","model":"claude-sonnet-4-6"});
    assert_eq!(cmd["cmd"], "create_session");
}

#[test]  
fn test_chat_stream_json_format() {
    let cmd = json!({"cmd":"chat_stream","session_id":"abc","content":"hello"});
    assert_eq!(cmd["cmd"], "chat_stream");
}

#[test]
fn test_parse_cowd_event_from_json() {
    let json = r#"{"type":"TextDelta","content":"hello"}"#;
    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    assert_eq!(v["type"], "TextDelta");
}
```

---

## B1: DaemonClient + 非阻塞协议 (3h)

### 新文件: `crates/cowd-cli/src/daemon_client.rs`

```rust
pub struct DaemonClient {
    stream: BufStream<UnixStream>,
    pub session_id: String,
    buf: Vec<u8>,
}

impl DaemonClient {
    /// 连接daemon + 创建session
    pub async fn connect(model: &str) -> Result<Self> { ... }
    
    /// 发送chat_stream命令
    pub async fn send_chat(&mut self, content: &str) -> Result<()> { ... }
    
    /// 阻塞接收一个CowdEvent (用于回合内等待)
    pub async fn recv_event(&mut self) -> Result<CowdEvent> { ... }
    
    /// 非阻塞接收所有待处理事件 (用于事件循环每16ms drain)
    pub fn try_recv_events(&mut self) -> Vec<CowdEvent> { ... }
}
```

### 非阻塞协议: daemon追加poll命令

`daemon/commands.rs`:
```rust
Some("poll_events") => {
    let session_id = cmd.get("session_id")...;
    // 返回所有待处理事件(不阻塞)
    let _ = writer.write_all(b"{\"events\":[]}\n").await;
}
```

---

## B2a: 回合处理路径替换 (3h) — 仅2处

| 行号 | 原代码 | 替换 |
|------|--------|------|
| 3942 | `prepare_turn_runtime()` → `build_runtime()` | `daemon.send_chat()` + `try_recv_events()` |
| 6778 | `run_prompt` → `build_runtime()` | `daemon.send_chat()` + `recv_event()` |

### 替换后的事件循环 (main.rs ~3060)
```rust
// 旧: 事件来自 sync_channel
// 新: 事件来自 daemon.try_recv_events()
let events = daemon.try_recv_events();
for event in events {
    state.apply_event(event);
}
```

---

## B2b: 会话管理命令 (8h, 后续Phase)

需要daemon侧新增命令（非本次范围）:
- `switch_session` / `resume_session` / `compact_session`
- `reload_features`
- `fork_session`

---

## 验证

```bash
cargo build -p cowd-cli
cargo test daemon_client
# 手动: cowd --solo → 验证回合处理通过daemon
```
