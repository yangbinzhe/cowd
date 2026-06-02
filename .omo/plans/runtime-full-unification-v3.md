# 后端完全统一方案 (v3 Final)

> 原则: 零回退、零重复、零能力损失。Daemon=唯一Runtime，TUI=纯视图层。
> Oracle: 待审核

---

## 一、目标架构

```
                    ┌──────────────────────────────────────────────────────┐
                    │              Daemon (cowd serve)                     │
                    │              唯一 Runtime 宿主                       │
                    │                                                      │
                    │  启动时: build_runtime_plugin_state() → 全局复用     │
                    │          CognitiveContextManager → 全局单例          │
                    │          UnifiedSessionStore → 全局单例              │
                    │                                                      │
                    │  ┌─────────────────────────────────────────────┐    │
                    │  │ Session 1 → ConversationRuntime             │    │
                    │  │   with_cowd_event_bus()      ← 事件广播      │    │
                    │  │   with_tool_callback(daemon)  ← 工具进度      │    │
                    │  │   with_hook_progress_reporter ← hook状态      │    │
                    │  │   with_collaboration()       ← 多Agent       │    │
                    │  │   with_jps_pipeline()        ← 联合求解       │    │
                    │  │   CognitiveContextManager     ← 内存系统       │    │
                    │  │   Plugin+MCP Tools            ← 工具注册       │    │
                    │  └─────────────────────────────────────────────┘    │
                    │                                                      │
                    │  HTTP :8642 → WebUI / API                           │
                    │  Unix Socket → TUI 事件流                            │
                    └──────────────────────┬───────────────────────────────┘
                                           ↕ JSON Lines (双向)
                    ┌──────────────────────┴───────────────────────────────┐
                    │              TUI (cowd)                              │
                    │              纯视图层                                │
                    │                                                      │
                    │  DaemonClient::connect("/tmp/cowd.sock")             │
                    │    → create_session → 获取 session_id                │
                    │    → chat_stream → 接收实时事件流                     │
                    │    → tool_approve/deny → 交互审批                    │
                    │                                                      │
                    │  接收事件 → CowdEvent → apply_event → 渲染          │
                    │                                                      │
                    │  启动: daemon不存在 → 自动启动 daemon                 │
                    │        daemon存在 → 直连                             │
                    └──────────────────────────────────────────────────────┘
```

---

## 二、Daemon 改造 (Phase A, 8h)

### A1: 全局复用Plugin+MCP (消除最大重复)

```rust
// daemon.rs: run_daemon() — 启动时构建一次，所有session复用
static GLOBAL_PLUGIN_STATE: OnceLock<Arc<RuntimePluginState>> = OnceLock::new();
static GLOBAL_MEMORY: OnceLock<Arc<CognitiveContextManager>> = OnceLock::new();
static GLOBAL_STORE: OnceLock<Arc<UnifiedSessionStore>> = OnceLock::new();

pub async fn run_daemon(config: DaemonConfig) {
    // 1. 全局单例 — 启动时构建，永不重复
    let plugin_state = GLOBAL_PLUGIN_STATE.get_or_init(|| {
        Arc::new(build_runtime_plugin_state().expect("plugin init"))
    });
    let memory = GLOBAL_MEMORY.get_or_init(|| {
        Arc::new(CognitiveContextManager::new(mem_cfg).await.expect("memory init"))
    });
    let store = GLOBAL_STORE.get_or_init(|| {
        Arc::new(get_unified_store().expect("store init"))
    });
    
    // 2. Session创建 — 复用全局单例
    let sessions = Arc::new(ActiveSessions::default());
    // ... (HTTP+Unix Socket监听)
}
```

### A2: create_session注入所有能力

```rust
// daemon.rs: handle_unix_client — create_session命令
Some("create_session") => {
    let plugin_state = GLOBAL_PLUGIN_STATE.get().unwrap();
    let memory = GLOBAL_MEMORY.get().unwrap();
    
    let mut runtime = ConversationRuntime::new_with_features(
        session, client, tool_executor, policy, system_prompt, &plugin_state.feature_config,
    );
    
    // 注入所有TUI需要的能力（之前仅在TUI侧有）
    let cowd_bus = CowdEventBus::new();
    runtime = runtime
        .with_cowd_event_bus(cowd_bus.clone())           // ← 事件广播
        .with_tool_callback(Arc::new(SocketToolCallback::new(tx)))  // ← 工具进度到socket
        .with_hook_progress_reporter(Box::new(SocketHookReporter::new(tx))) // ← hook状态到socket
        .with_collaboration(new_boxed(executor.clone()))  // ← 多Agent
        .with_jps_pipeline(new_boxed(executor))           // ← 联合求解
        .with_memory_manager(memory.clone());              // ← 内存系统
    
    sessions.register(session_id, runtime);
    // 返回 session_id 给TUI
}
```

### A3: chat_stream — 完整事件流

```rust
Some("chat_stream") => {
    let entry = sessions.get(&session_id).unwrap();
    let mut guard = entry.lock().await;
    
    // 提取CowdEventBus订阅（在guard drop前）
    let cowd_rx = guard.cowd_bus().map(|b| b.subscribe());
    drop(guard); // ← 关键: 释放MutexGuard，避免!Send
    
    // 异步执行turn
    tokio::spawn(async move {
        let mut guard = entry.lock().await;
        match guard.run_turn_async(content, &perm).await {
            Ok(summary) => { /* 发送TurnComplete */ }
            Err(e)      => { /* 发送TurnError */ }
        }
    });
    
    // 事件转发循环
    while let Ok(event) = cowd_rx.unwrap().recv().await {
        let json = match event {
            CowdEvent::TextDelta { text } => json!({"type":"TextDelta","content":text}),
            CowdEvent::ThinkingDelta { text } => json!({"type":"ThinkingDelta","content":text}),
            CowdEvent::ToolStart { id, name, preview } => json!({"type":"ToolStart","id":id,"name":name,"preview":preview}),
            CowdEvent::ToolProgress { id, name, progress } => json!({"type":"ToolProgress","id":id,"name":name,"progress":progress}),
            CowdEvent::ToolComplete { id, name, summary, exit_code } => json!({"type":"ToolComplete","id":id,"name":name,"summary":summary,"exit_code":exit_code}),
            CowdEvent::TurnComplete { .. } => json!({"type":"TurnComplete"}),
            CowdEvent::TurnError { error } => json!({"type":"TurnError","error":error}),
            CowdEvent::TokenUsage { input, output } => json!({"type":"TokenUsage","input":input,"output":output}),
            CowdEvent::ApprovalRequired { tool, id } => json!({"type":"ApprovalRequired","tool":tool,"id":id}),
            _ => continue,
        };
        writer.write_all(json.to_string().as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
}
```

### A4: 工具审批协议

```rust
// 新增prompter实现 — 通过socket发送审批请求
struct SocketPrompter {
    tx: mpsc::UnboundedSender<String>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<bool>>>>,
}

impl PermissionPrompter for SocketPrompter {
    fn prompt(&self, tool: &str, params: &str) -> PermissionOutcome {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(id.clone(), tx);
        
        // 发送审批请求给TUI
        self.tx.send(json!({"type":"ApprovalRequired","tool":tool,"params":params,"id":id}).to_string());
        
        // 阻塞等待TUI响应
        match rx.blocking_recv() {
            Ok(true) => PermissionOutcome::Allow,
            Ok(false) => PermissionOutcome::Deny,
            Err(_) => PermissionOutcome::Deny,
        }
    }
}

// TUI发送审批响应
Some("tool_approve") => { /* 设置prompter中对应id的oneshot为true */ }
Some("tool_deny")   => { /* 设置prompter中对应id的oneshot为false */ }
```

---

## 三、TUI 改造 (Phase B, 6h)

### B1: 删除所有build_runtime调用

删除位置:
- `LiveCli::new()` → `build_runtime()` (main.rs:3813) — 替换为DaemonClient
- `prepare_turn_runtime()` → `build_runtime()` (main.rs:3893) — 不再需要
- `registry_runtime` → `build_runtime()` (main.rs:2890) — 不再需要

### B2: DaemonClient实现

```rust
// crates/cowd-cli/src/daemon_client.rs (新文件)
pub struct DaemonClient {
    stream: BufStream<UnixStream>,
    session_id: String,
    model: String,
}

impl DaemonClient {
    pub async fn connect(model: &str) -> Result<Self> {
        // 1. 检测daemon是否运行
        match server::get_server_status() {
            Ok(Some(_)) => { /* daemon在线 */ }
            _ => {
                // 2. 自动启动daemon
                spawn_daemon().await?;
                // 3. 等待daemon就绪
                wait_for_daemon(5).await?;
            }
        }
        // 4. 连接socket
        let stream = UnixStream::connect("/tmp/cowd.sock").await?;
        let mut client = Self { stream: BufStream::new(stream), session_id: String::new(), model: model.to_string() };
        // 5. 创建session
        client.create_session().await?;
        Ok(client)
    }
    
    async fn create_session(&mut self) -> Result<()> {
        send_json(&mut self.stream, json!({"cmd":"create_session","model":self.model})).await?;
        let resp: Value = read_json(&mut self.stream).await?;
        self.session_id = resp["session_id"].as_str().unwrap().to_string();
        Ok(())
    }
    
    pub async fn send_chat(&mut self, content: &str) -> Result<()> {
        send_json(&mut self.stream, json!({"cmd":"chat_stream","session_id":self.session_id,"content":content})).await?;
        Ok(())
    }
    
    pub async fn recv_event(&mut self) -> Result<CowdEvent> {
        let line = read_line(&mut self.stream).await?;
        Ok(parse_cowd_event(&line))
    }
    
    pub async fn approve_tool(&mut self, id: &str) -> Result<()> {
        send_json(&mut self.stream, json!({"cmd":"tool_approve","id":id})).await?;
        Ok(())
    }
    
    pub async fn deny_tool(&mut self, id: &str) -> Result<()> {
        send_json(&mut self.stream, json!({"cmd":"tool_deny","id":id})).await?;
        Ok(())
    }
}
```

### B3: TUI事件循环接入

```rust
// main.rs: run_tui_repl() — 替换原有 event loop
let mut daemon = DaemonClient::connect(&cli.model).await?;

// TUI事件循环
loop {
    // 1. 用户输入
    if let Some(content) = get_user_input() {
        daemon.send_chat(&content).await?;
    }
    
    // 2. 接收daemon事件
    while let Ok(event) = daemon.recv_event().await {
        match event {
            CowdEvent::TextDelta { text } => state.apply_event(text),
            CowdEvent::ToolStart { id, name, .. } => state.apply_event(tool_start),
            CowdEvent::ApprovalRequired { tool, id } => {
                // 显示审批对话框
                if user_approved() { daemon.approve_tool(&id).await?; }
                else { daemon.deny_tool(&id).await?; }
            }
            CowdEvent::TurnComplete => break, // 进入下一轮
            _ => {}
        }
    }
    
    // 3. 渲染
    terminal.draw(|f| state.render(f))?;
}
```

---

## 四、TUI启动流程（简化版）

```rust
// main.rs: CliAction::Repl handler — 新流程
CliAction::Repl { .. } => {
    // 1. 确保daemon运行
    match server::get_server_status() {
        Ok(Some(_)) => { /* 已运行 */ }
        _ => { spawn_daemon(); wait_for_daemon(5); }
    }
    
    // 2. 直连daemon（无build_runtime）
    let client = DaemonClient::connect(&model).await?;
    
    // 3. 启动TUI
    run_tui_with_daemon(client, workspace).await?;
}
```

---

## 五、能力零损失验证

| 能力 | TUI独立 | Daemon独立 | 统一后 | 实现方式 |
|------|---------|-----------|--------|---------|
| 内存系统(L0-L4) | ✅ | ✅ | ✅ | Daemon全局CognitiveContextManager |
| 流式输出 | ✅ | ❌ | ✅ | chat_stream → CowdEvent流 |
| 工具调用+进度 | ✅ | ❌ | ✅ | SocketToolCallback → socket事件 |
| 工具审批 | ✅ | ❌ | ✅ | SocketPrompter + tool_approve/deny |
| Hook状态 | ✅ | ❌ | ✅ | SocketHookReporter → socket事件 |
| 多Agent协作 | ✅ | ✅ | ✅ | Daemon内置collaboration+jps |
| MCP工具 | ✅ | ✅ | ✅ | Daemon全局Plugin+MCP |
| 会话恢复 | ✅ | ❌ | ✅ | resume_session + UnifiedSessionStore |
| 平台适配(飞书/企微) | ❌ | ✅ | ✅ | Daemon保留 |
| HTTP API :8642 | ❌ | ✅ | ✅ | Daemon保留 |
| WebUI | ❌ | ✅ | ✅ | Daemon保留 |
| MemorySync | ✅ | ✅ | ✅ | Daemon全局 |

**结论: 12/12能力零损失。TUI独有6项全部移入daemon，daemon独有6项全部保留。**

---

## 六、消除的重复建设

| 消除项 | 原状态 | 新状态 |
|--------|--------|--------|
| Plugin初始化 | 每次TUI启动+每个daemon session | 全局OnceLand，仅一次 |
| MCP发现 | 每次TUI启动+每个daemon session | 全局OnceLand，仅一次 |
| ToolRegistry | 每Runtime新建 | 全局共享 |
| ConversationRuntime | 每TUI session新 | daemon提供 |
| CognitiveContextManager | 每Runtime一份 | 全局单例 |
| SystemPrompt | TUI独建 | daemon提供 |
| MemoryConfig | 各自解析 | 全局一份 |

**TUI启动时间: 从8秒降至<0.3秒（socket连接）**

---

## 七、工期

| 阶段 | 内容 | 工期 |
|------|------|------|
| A1 | 全局单例 (Plugin+MCP+Memory+Store) | 3h |
| A2 | create_session注入全能力 | 2h |
| A3 | chat_stream完整事件流 | 3h |
| A4 | 工具审批协议 SocketPrompter | 3h |
| B1 | 删除TUI所有build_runtime | 2h |
| B2 | DaemonClient实现 | 3h |
| B3 | TUI事件循环接入 | 3h |
| C | 全量回归测试 | 4h |
| **总计** | | **23h** |
