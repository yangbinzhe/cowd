# 后端完全统一方案 v4 Final (Oracle修正全部应用)

> 4项阻塞已修正 | 18调用点全覆盖 | 33h工期
> Oracle: 待最终审核

---

## 一、架构（不变）

```
Daemon = 唯一Runtime宿主
TUI = 纯视图层 (Unix Socket JSON Lines)
零回退, 零重复, 零能力损失
```

---

## 二、Daemon 改造 (Phase A, 16h)

### A1: 全局单例 (3h)

```rust
// daemon.rs
static GLOBAL_PLUGIN: OnceLock<Arc<RuntimePluginState>> = OnceLock::new();
static GLOBAL_MEMORY: OnceLock<Arc<CognitiveContextManager>> = OnceLock::new();
static GLOBAL_STORE: OnceLock<Arc<UnifiedSessionStore>> = OnceLock::new();
```

### A2: create_session 注入全能力 (3h)

所有builder在daemon侧构建，TUI不再创建runtime:

```rust
runtime = runtime
    .with_memory_manager(GLOBAL_MEMORY.get().unwrap().clone())
    .with_tool_callback(Arc::new(SocketToolCallback(tx.clone())))
    .with_hook_progress_reporter(Box::new(SocketHookReporter(tx.clone())))
    .with_collaboration(new_boxed(executor.clone()))
    .with_jps_pipeline(new_boxed(executor));
```

### A3: chat_stream — 完整事件流 (6h) — ORACLE修正全部应用

**修正6**: 新增3个缺失的CowdEvent发射
**修正11**: 使用 `spawn_blocking` 替代 `tokio::spawn`（避免`!Send`问题）

```rust
Some("chat_stream") => {
    let entry = sessions.get(&session_id).unwrap();
    
    // 提取CowdEventBus订阅（在guard内完成）
    let cowd_rx = {
        let guard = entry.lock().await;
        guard.cowd_bus().map(|b| b.subscribe())
    }; // guard dropped — 解决!Send问题
    
    // 使用spawn_blocking + block_on（匹配现有daemon模式 daemon.rs:306-329）
    let entry_clone = entry.clone();
    let content_owned = content.to_string();
    let session_clone = session_id.to_string();
    
    tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async move {
            let mut guard = entry_clone.lock().await;
            match guard.run_turn_async(&content_owned, &prompter).await {
                Ok(summary) => { /* TurnComplete处理 */ }
                Err(e) => { /* TurnError处理 — 修正6 */ }
            }
        });
    });
    
    // 事件转发循环（修正6: 完整9事件）
    if let Some(mut rx) = cowd_rx {
        while let Ok(event) = rx.recv().await {
            let json = match event {
                CowdEvent::TextDelta { text } => json!({"type":"TextDelta","content":text}),
                CowdEvent::ThinkingDelta { thinking } => json!({"type":"ThinkingDelta","content":thinking}),
                CowdEvent::ToolStart { id, name, preview } => json!({"type":"ToolStart","id":id,"name":name,"preview":preview}),
                CowdEvent::ToolProgress { id, name, progress } => json!({"type":"ToolProgress","id":id,"name":name,"progress":progress}),
                CowdEvent::ToolComplete { id, name, summary, exit_code } => json!({"type":"ToolComplete","id":id,"name":name,"summary":summary,"exit_code":exit_code}),
                CowdEvent::ApprovalRequested { tool } => json!({"type":"ApprovalRequested","tool":tool}),
                CowdEvent::TurnComplete { assistant_text, iterations } => json!({"type":"TurnComplete","text":assistant_text,"iterations":iterations}),
                CowdEvent::TurnError { error } => json!({"type":"TurnError","error":error}),
                CowdEvent::TokenUsage { input, output, .. } => json!({"type":"TokenUsage","input":input,"output":output}),
                _ => continue,
            };
            writer.write_all(json.to_string().as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }
    }
}
```

**修正6所需的新增发射** (在`conversation.rs`的`run_turn_async`中):
```rust
// 错误路径 (约line 1630):
Err(e) => {
    if let Some(bus) = &self.cowd_bus {
        let _ = bus.send(CowdEvent::TurnError { error: e.to_string() });
    }
    return Err(e);
}
// Token统计路径 (约line 1640):
if let Some(bus) = &self.cowd_bus {
    let _ = bus.send(CowdEvent::TokenUsage { input: usage.input, output: usage.output });
}
```

### A4: 工具审批协议 (4h) — ORACLE修正全部应用

**修正7**: 使用正确的trait方法 `decide(&mut self, &PermissionRequest) -> PermissionPromptDecision`
**修正8**: 使用 `tokio::sync::oneshot` 替代 `std blocking_recv`

```rust
// 正确的PermissionPrompter实现
struct SocketPrompter {
    tx: tokio::sync::mpsc::UnboundedSender<String>,
    pending: std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<PermissionPromptDecision>>>,
}

impl PermissionPrompter for SocketPrompter {
    fn decide(&mut self, request: &PermissionRequest) -> PermissionPromptDecision {  // ← 修正7
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();  // ← 修正8
        
        self.pending.lock().unwrap().insert(id.clone(), tx);
        
        // 发送审批请求给TUI
        let _ = self.tx.send(json!({
            "type":"ApprovalRequested",
            "tool": request.tool_name,
            "params": request.input,
            "required_mode": request.required_mode,
            "id": id
        }).to_string());
        
        // 阻塞等待（在spawn_blocking上下文中安全 — 见A3）
        rx.blocking_recv().unwrap_or(PermissionPromptDecision::Deny)
    }
}
```

---

## 三、TUI 改造 (Phase B, 11h)

### B1: 删除18个build_runtime调用点 (5h) — ORACLE修正9

| # | 位置 | 行号 | 用途 | 替换为 |
|---|------|------|------|--------|
| 1 | LiveCli::new | 3813 | 主TUI runtime | DaemonClient::connect |
| 2 | prepare_turn_runtime | 3893 | 每轮runtime | DaemonClient::send_chat |
| 3 | registry_runtime | 2890 | 工具注册 | 不再需要(daemon已有) |
| 4 | run_prompt single | 4402 | 单次prompt | HTTP API调用 |
| 5 | run_prompt batch | 4433 | 批量prompt | HTTP API调用 |
| 6 | compact sub | 4475 | 子agent | DaemonClient子会话 |
| 7 | agent create | 4634 | 多agent | DaemonClient子会话 |
| 8 | agent create | 4648 | 多agent注册 | 不再需要 |
| 9 | agent create | 4684 | 多agent | DaemonClient子会话 |
| 10 | agent create | 4698 | 多agent注册 | 不再需要 |
| 11 | agent create | 4736 | 多agent | DaemonClient子会话 |
| 12 | daemon create | daemon.rs:414 | daemon会话 | 统一create_session |
| 13 | api_routes create | api_routes.rs:253 | HTTP创会话 | 统一create_session |
| 14 | gateway test | gateway.rs:141 | 测试 | 保留(test helper) |
| 15 | offline runtime | 4833 | 离线场景 | DaemonClient |
| 16 | offline runtime | 4854 | 离线场景 | DaemonClient |
| 17 | offline runtime | 4880 | 离线场景 | DaemonClient |
| 18 | prompt runtime | 6729 | prompt场景 | HTTP API调用 |

**定义保留**: `build_runtime()` 函数定义(7089行)保留 — daemon内部仍使用它创建会话runtime。

### B2: DaemonClient (3h)
### B3: TUI事件循环接入 (3h)

---

## 四、测试 (Phase C, 6h)

---

## 五、能力零损失 — 最终版

| 能力 | 统一后 | 实现 |
|------|--------|------|
| 内存系统 L0-L4 | ✅ | Daemon全局CognitiveContextManager |
| 流式输出 | ✅ | CowdEventBus → socket |
| 工具调用+进度 | ✅ | SocketToolCallback |
| 工具审批 | ✅ | SocketPrompter (decide+tokio::oneshot) |
| Hook状态 | ✅ | SocketHookReporter |
| 多Agent协作 | ✅ | Daemon内置 |
| MCP工具 | ✅ | OnceLock全局 |
| 会话恢复 | ✅ | UnifiedSessionStore |
| 平台适配 | ✅ | Daemon保留 |
| HTTP API | ✅ | Daemon保留 |
| WebUI | ✅ | Daemon保留 |
| MemorySync | ✅ | Daemon内置 |

---

## 六、工期

| 阶段 | 内容 | 工期 |
|------|------|------|
| A1 | 全局单例 | 3h |
| A2 | create_session注入 | 3h |
| A3 | chat_stream (含3事件新增+spawn_blocking) | 6h |
| A4 | 工具审批 (decide+tokio::oneshot) | 4h |
| B1 | 删除19处build_runtime | 5h |
| B2 | DaemonClient | 3h |
| B3 | TUI事件循环 | 3h |
| C | 回归测试 | 6h |
| **总计** | | **33h** |
