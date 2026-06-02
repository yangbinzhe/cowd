# TUI+Server 统一后端方案 v1

> 目标：TUI直连daemon的Unix Socket，共享运行时，无需每次重建Runtime
> Oracle: 待审核

---

## 当前架构 vs 目标架构

```
当前 (冗余):
  TUI启动 → build_runtime() → LiveCli → ConversationRuntime (独立副本)
  daemon → ActiveSessions → ConversationRuntime (闲置, TUI不用)

目标 (共享):
  TUI启动 → UnixStream::connect("/tmp/cowd.sock") → 直连daemon
  daemon → ActiveSessions → ConversationRuntime (TUI和API共用)
```

## 现有能力分析

daemon已实现完整的远程Session API（`daemon.rs:379-550`）:

| 命令 | 功能 | 状态 |
|------|------|------|
| `create_session` | 创建Session+Runtime | ✅ 已实现 |
| `chat` | 发送消息+获取回复 | ⚠️ 仅返回最终文本，无流式 |
| `list_sessions` | 列出所有会话 | ✅ 已实现 |
| `ToolUse/ToolResult` | 工具调用 | ❌ 未暴露 |

## 缺失能力

1. **流式输出**: `chat`命令等待turn完成才返回，TUI需要实时TextDelta
2. **事件通道**: daemon需要发送ToolStart/Progress/Complete/TextDelta/TurnComplete事件流
3. **TUI socket客户端**: TUI启动时连接socket代替build_runtime

## 方案

### Phase 1: daemon流式chat命令 (2h)

新增 `chat_stream` 命令，返回 SSE-style JSON事件流:

```rust
// daemon.rs: 新增
Some("chat_stream") => {
    // 创建通道用于发送事件
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    
    // 在runtime上设置SSE回调
    runtime.set_sse_callback(Arc::new(move |data| {
        let _ = event_tx.send(data);
    }));
    
    // 启动turn (异步)
    tokio::spawn(async move {
        runtime.run_turn_async(content, prompter).await;
    });
    
    // 流式返回事件给TUI
    while let Some(event) = event_rx.recv().await {
        writer.write_all(event.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
}
```

### Phase 2: TUI socket客户端 (3h)

TUI启动时检测daemon存活，若存活则连接socket:

```rust
// main.rs: run_repl() — 替代 build_runtime()
if let Ok(stream) = UnixStream::connect("/tmp/cowd.sock").await {
    // 直连daemon模式
    let (reader, writer) = stream.into_split();
    
    // 发送 create_session
    send_cmd(&mut writer, json!({"cmd":"create_session","model":"claude-sonnet-4-6"}));
    let resp = read_response(&mut reader).await;
    let session_id = resp["session_id"].as_str();
    
    // 使用socket客户端替代in-process runtime
    let daemon_client = DaemonClient::new(reader, writer, session_id);
    run_tui_with_client(daemon_client).await;
}
```

### Phase 3: TUI事件适配 (2h)

将daemon事件流映射到TUI的CowdEvent:

```rust
impl DaemonClient {
    async fn recv_event(&mut self) -> CowdEvent {
        let line = self.reader.read_line().await;
        match serde_json::from_str::<Value>(&line) {
            Ok(v) if v["type"] == "TextDelta" => 
                CowdEvent::TextDelta { text: v["content"].as_str() },
            Ok(v) if v["type"] == "ToolUse" =>
                CowdEvent::ToolStart { id, name, preview },
            // ... 其他事件类型
        }
    }
}
```

---

## 验证清单

- [ ] TUI直连daemon，无build_runtime开销
- [ ] TUI流式输出(TextDelta实时渲染)
- [ ] 工具调用正常(ToolStart/Progress/Complete)
- [ ] TUI退出后会话保持(daemon存活)
- [ ] `cowd --resume` 可恢复已有会话
- [ ] 回退: daemon不可用时TUI仍可独立运行(自建runtime)
