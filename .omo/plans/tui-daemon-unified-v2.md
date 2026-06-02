# Runtime与Daemon统一方案 v2 (完整版)

## 一、分离原因分析

### 两者实际共用同一代码路径

`daemon.rs:414` 调用 `build_runtime()` → 内部调用 `build_runtime_with_plugin_state()` — **与TUI完全相同的基础设施**。

### 能力差异（仅3项缺失）

| 能力 | TUI | Daemon | 差距 |
|------|-----|--------|------|
| Plugin+MCP初始化 | ✅ `build_runtime_plugin_state()` | ✅ 同路径 | 无 |
| ConversationRuntime | ✅ 完整 | ✅ 完整 | 无 |
| **CowdEventBus** (流式事件) | ✅ `with_cowd_event_bus()` | ❌ `stream_callback: None` | TUI有实时事件流 |
| **ToolCallback** (工具可视化) | ✅ `with_tool_callback()` | ❌ `tool_callback: None` | TUI有工具进度 |
| **HookProgressReporter** | ✅ `with_hook_progress_reporter()` | ❌ 未设置 | TUI有hook状态 |
| Collaboration/JPS/MemorySync | ✅ 完整 | ✅ 完整 | 无 |

**结论：差异仅3项，均为事件通知/回调机制。核心计算能力完全一致。**

---

## 二、统一方案

### 核心理念

daemon作为**唯一Runtime宿主**，TUI作为**纯视图层**。二者通过Unix Socket传输事件流。

### 架构

```
daemon (cowd serve):
  ┌─────────────────────────────────────────┐
  │ ActiveSessions → ConversationRuntime    │
  │   ├── with_cowd_event_bus() → 事件流    │ ← 新增
  │   ├── with_tool_callback() → 工具回调   │ ← 新增
  │   └── run_turn_async() → 完整执行       │
  │                                         │
  │ Unix Socket (/tmp/cowd.sock):           │
  │   ├── create_session → 新建Session      │
  │   ├── resume_session → 恢复Session      │ ← 新增
  │   ├── chat_stream → 流式对话            │ ← 新增
  │   ├── tool_approve / tool_deny → 审批   │ ← 新增
  │   └── list/delete sessions              │
  └─────────────────────────────────────────┘
           ↕ Unix Socket (JSON Lines)
  TUI (cowd):
  ┌─────────────────────────────────────────┐
  │ DaemonClient → 直连socket              │ ← 替代 build_runtime
  │   ├── 接收 CowdEvent 流 → apply_event  │
  │   ├── 发送 chat → 收到流式输出          │
  │   └── 心跳检测 + 回退自建runtime        │
  └─────────────────────────────────────────┘
```

### Phase 1: daemon补全3项缺失能力 (2h)

**F1: daemon的chat_stream命令** (`daemon.rs`)

```rust
Some("chat_stream") => {
    let session_id = cmd["session_id"].as_str();
    let content = cmd["content"].as_str();
    
    let Some(entry) = sessions.get(session_id) else {
        send_json(&mut writer, json!({"error":"session not found"}));
        continue;
    };
    
    let mut guard = entry.lock().await;
    
    // 创建事件通道
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let tx_clone = event_tx.clone();
    
    // 设置SSE回调 → 流式事件
    guard.set_sse_callback(Arc::new(move |data| {
        let _ = tx_clone.send(data);
    }));
    
    // 设置ToolCallback → 工具进度
    guard.set_tool_callback(Arc::new(DaemonToolCallback::new(event_tx.clone())));
    
    // 订阅CowdEventBus → 生命周期事件  
    let mut cowd_rx = guard.cowd_bus().subscribe();
    tokio::spawn(async move {
        while let Ok(event) = cowd_rx.recv().await {
            let json = match event {
                CowdEvent::TurnStarted { .. } => json!({"type":"TurnStarted"}),
                CowdEvent::TokenUsage { input, output, .. } => json!({"type":"TokenUsage","input":input,"output":output}),
                _ => continue,
            };
            let _ = event_tx.send(json.to_string());
        }
    });
    
    // 异步执行turn
    tokio::spawn(async move {
        match guard.run_turn_async(content, &SharedPrompter::none()).await {
            Ok(summary) => {
                let _ = event_tx.send(json!({"type":"TurnComplete"}).to_string());
            }
            Err(e) => {
                let _ = event_tx.send(json!({"type":"TurnError","error":e.to_string()}).to_string());
            }
        }
    });
    
    // 流式输出到socket
    while let Some(event) = event_rx.recv().await {
        writer.write_all(event.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
}
```

**F2: resume_session命令**

```rust
Some("resume_session") => {
    let session_id = cmd["session_id"].as_str();
    if let Some(store) = unified_store.as_ref() {
        if let Ok(Some(record)) = store.get_session(session_id) {
            // 从持久化恢复session → 创建runtime
            let session = restore_session_from_record(&record)?;
            let runtime = build_runtime(session, session_id, ...)?;
            sessions.register(session_id, runtime);
            send_json(&mut writer, json!({"ok":true,"session_id":session_id}));
        }
    }
}
```

**F3: tool_approve/deny命令**

```rust
Some("tool_approve") | Some("tool_deny") => {
    // 通过共享状态或通道通知等待中的工具审批
    // 使用 tokio::sync::oneshot 通道
}
```

### Phase 2: TUI DaemonClient (3h)

```rust
// 新增: crates/cowd-cli/src/daemon_client.rs
struct DaemonClient {
    stream: UnixStream,
    session_id: String,
    event_rx: mpsc::Receiver<String>,
}

impl DaemonClient {
    async fn connect() -> Result<Self> {
        let stream = UnixStream::connect("/tmp/cowd.sock").await?;
        // 发送 create_session → 获取 session_id
        // 返回 DaemonClient
    }
    
    async fn send_chat(&mut self, content: &str) {
        write_cmd(&mut self.stream, json!({"cmd":"chat_stream","session_id":self.session_id,"content":content}));
    }
    
    async fn recv_event(&mut self) -> CowdEvent {
        let line = self.event_rx.recv().await;
        parse_cowd_event(&line)
    }
}
```

### Phase 3: TUI接入daemon (2h)

```rust
// main.rs: run_repl() 中
if let Ok(client) = DaemonClient::connect().await {
    // 直连daemon模式
    run_tui_with_daemon(client).await;
} else {
    // 回退: 自建runtime (保留现有路径)
    run_tui_standalone(cli).await;
}
```

### Phase 4: 工具审批协议 (2h)

```
TUI ←→ daemon:
  daemon: {"type":"ApprovalRequired","tool":"bash","params":"rm -rf /"}
  TUI:    {"cmd":"tool_deny","tool_id":"xxx"}  // 或 tool_approve
  daemon: 继续或终止工具执行
```

---

## 三、功能零损失验证清单

| 能力 | TUI独立 | 统一后 | 实现 |
|------|---------|--------|------|
| 内存系统(L0-L4) | ✅ | ✅ | daemon内置CognitiveContextManager |
| 多Agent协作 | ✅ | ✅ | daemon内置collaboration+jps |
| 流式输出 | ✅ | ✅ | chat_stream → CowdEvent流 |
| 工具调用+进度 | ✅ | ✅ | ToolCallback → socket事件 |
| 工具审批 | ✅ | ✅ | tool_approve/deny协议 |
| Hook执行 | ✅ | ✅ | daemon内置HookRunner |
| MCP工具 | ✅ | ✅ | daemon启动时发现 |
| 会话恢复 | ✅ | ✅ | resume_session命令 |
| MemorySync | ✅ | ✅ | daemon内置 |
| 回退自建runtime | — | ✅ | daemon不可用时自动回退 |
| 性能 | 每次慢 | 首次后快 | 会话复用Runtime |

---

## 四、工期

| 阶段 | 内容 | 工期 |
|------|------|------|
| Phase 1 | daemon补全能力(F1-F3) | 6h |
| Phase 2 | DaemonClient | 3h |
| Phase 3 | TUI接入 | 2h |
| Phase 4 | 工具审批 | 2h |
| Phase 5 | 回退+测试 | 3h |
| **总计** | | **16h** |

*待Oracle审核*
