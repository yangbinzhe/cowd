# 统一守护进程 — 极简方案 v2

## 你的质疑完全正确

### 1. thin client CLI 没有存在的必要

| 场景 | 不需要 CLI 的原因 |
|------|------------------|
| 脚本自动化 | `curl POST /api/sessions/{id}/messages -d '{"content":"hello"}'` |
| CI/CD | 同上，curl 是标准工具 |
| 管道操作 | `curl ... | jq '.response'` |
| 快速查询 | TUI 启动即用，daemon 已在后台 |

**结论: 删除独立的 CLI prompt 模式。** TUI 是全功能入口，HTTP API 是脚本接口。不需要在中间加一个 thin client。

### 2. `cowd serve` 没有存在价值

`serve` 和 `gateway run` 功能重叠。gateway 应该就是 daemon，默认启动 HTTP。

### 3. `cowd migrate-sessions` 不需要

默认环境是干净的，不需要迁移代码。

### 4. 简化后的架构

```
cowd gateway run          ← 唯一守护进程入口 (HTTP + Unix Socket + 飞书)
cowd gateway start/stop   ← systemd 管理
cowd --solo               ← TUI (如 daemon 未运行则自动启动)
cowd version/help/install ← 纯信息/部署命令
curl                      ← 脚本通过 HTTP API 访问
```

**就是这些。** 不需要 serve, prompt, migrate, compact, thin client。

## 精确实施方案

### 需要删除的代码

| 删除 | 文件 | 行号 | 原因 |
|------|------|------|------|
| `CliAction::Serve` 分支 | `main.rs:451-468` | 独立 HTTP server | gateway run 替代 |
| `start_http_server()` | `server/mod.rs:237` | 独立启动 | daemon 统一管理 |
| `run_repl` 中 compact 路径 | `main.rs:2660-2683` | thin client | 删除, 不需要 |
| `run_gateway_action` 子进程 | `main.rs:480-540` | 子进程 start | 改为 daemon 直接运行 |
| `migrate-sessions` 命令 | `main.rs: ~466-471` | 数据迁移 | 不需要 |

### 需要修改的核心代码

#### 文件 A: `crates/cowd-cli/src/main.rs`

**parse_args 清理** — 删除 `--compact`、`prompt` 子命令的 compact 逻辑:

```rust
// 删除: let mut compact = false; (line 764)
// 删除: "--compact" => { compact = true; } (line 850-851)
// 保留: CliAction::Repl { ... } 的 dispatch
// 修改: CliAction::Repl → 不再需要 compact 和 prompt 字段

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliAction {
    // 删除: Serve { config: HttpConfig }
    Help { output_format: CliOutputFormat },
    Version { output_format: CliOutputFormat },
    Install { systemd: bool, path: Option<PathBuf> },
    Gateway { action: GatewayAction, output_format: CliOutputFormat },
    Repl { model, allowed_tools, permission_mode, base_commit, reasoning_effort, allow_broad_cwd },
    // 删除: MigrateSessions
}
```

**dispatch 简化**:

```rust
// 删除: CliAction::Serve { config } => start_http_server(config)
// 删除: CliAction::MigrateSessions { output_format } => run_migrate_sessions(...)
// 修改:
CliAction::Repl { model, allowed_tools, permission_mode, base_commit, reasoning_effort, allow_broad_cwd } => {
    // 检查 daemon 是否运行
    let sock = Path::new("/tmp/cowd.sock");
    if !sock.exists() {
        // 自动启动 daemon (后台)
        let exe = std::env::current_exe()?;
        std::process::Command::new(&exe)
            .arg("gateway").arg("run")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        // 等待 daemon 就绪 (poll socket)
        for _ in 0..50 {  // 最多 5 秒
            if sock.exists() { break; }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    run_tui_repl(cli, workspace)
}

CliAction::Gateway { action, output_format } => match action {
    GatewayAction::Run => {
        // 直接在当前进程运行 daemon (不再 fork 子进程)
        let daemon_config = build_daemon_config()?;
        SHARED_RT.block_on(GatewayDaemon::run(daemon_config))?;
    }
    GatewayAction::Start => { /* systemd 不变 */ }
    GatewayAction::Stop => { /* systemd 不变 */ }
    GatewayAction::Status => { /* systemd 不变 */ }
}
```

#### 文件 B: `crates/cowd-cli/src/server/mod.rs`

**删除整个 `start_http_server` 函数** (第 237-495 行)。HTTP server 由 daemon 统一管理。

**保留**: `HttpConfig` 结构体定义, `ServerStatus`, `get_server_status`, `stop_server` (systemd 管理需要)。

#### 文件 C: `crates/cowd-cli/src/daemon.rs` (新建)

```rust
// GatewayDaemon — 唯一守护进程

use tokio::net::TcpListener;
use tokio::net::UnixListener;

pub struct DaemonConfig {
    pub http_addr: String,          // "0.0.0.0:8642"
    pub unix_sock_path: String,     // "/tmp/cowd.sock"
    pub memory_enabled: bool,
    pub platform_configs: Vec<runtime::platform::PlatformConfig>,
}

pub async fn run_daemon(config: DaemonConfig) -> Result<()> {
    // 1. 初始化共享状态
    let sessions = Arc::new(ActiveSessions::new());
    let tools = Arc::new(GlobalToolRegistry::builtin());
    let cognitive = if config.memory_enabled {
        CognitiveContextManager::default().await.ok().map(Arc::new)
    } else { None };
    let event_bus = SessionEventBus::new();
    
    let app_state = Arc::new(api_routes::AppState {
        sessions: sessions.clone(),
        memory_manager: cognitive.clone(),
        tool_registry: tools.clone(),
        config: Some(load_runtime_config().await),
        event_bus: event_bus.clone(),
    });
    
    // 2. 构建 HTTP router (复用 api_routes + 加 SSE)
    let app = api_routes::api_router(app_state)
        .route("/api/sessions/:id/stream", get(sse_stream_handler));
    
    // 3. 启动 HTTP
    let listener = TcpListener::bind(&config.http_addr).await?;
    tracing::info!("HTTP listening on {}", config.http_addr);
    
    // 4. 启动 Unix Socket
    let _ = std::fs::remove_file(&config.unix_sock_path);
    let unix_listener = UnixListener::bind(&config.unix_sock_path)?;
    tracing::info!("Unix socket on {}", config.unix_sock_path);
    
    // 5. 启动平台适配器
    start_platform_adapters(&config.platform_configs).await;
    
    // 6. 启动 Unix Socket accept 循环
    let daemon_state = Arc::new(DaemonState {
        sessions: sessions.clone(),
        cognitive: cognitive.clone(),
        event_bus: event_bus.clone(),
    });
    
    let unix_handle = tokio::spawn(async move {
        loop {
            let (stream, _) = unix_listener.accept().await.unwrap();
            let state = daemon_state.clone();
            tokio::spawn(handle_unix_client(stream, state));
        }
    });
    
    // 7. HTTP server (阻塞主循环)
    axum::serve(listener, app).await?;
    
    Ok(())
}
```

#### 文件 D: `crates/cowd-cli/src/api_routes.rs` (修改)

**第 23-28 行** — AppState 新增 `event_bus`:
```rust
pub struct AppState {
    pub sessions: Arc<ActiveSessions>,
    pub memory_manager: Option<Arc<CognitiveContextManager>>,
    pub tool_registry: Arc<GlobalToolRegistry>,
    pub config: Option<serde_json::Value>,
    pub event_bus: Arc<SessionEventBus>,  // ← 新增
}
```

**第 32-43 行** — router 新增 SSE:
```rust
pub fn api_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route("/api/sessions/:id", get(get_session).delete(delete_session))
        .route("/api/sessions/:id/messages", post(send_message))
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
        .route("/api/sessions/:id/stream", get(sse_stream_handler))  // ← 新增
        .with_state(state)
}
```

**第 263 行之后** — 新增 SSE handler:
```rust
async fn sse_stream_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::unbounded_channel();
    state.event_bus.subscribe(&session_id, tx).await;
    Sse::new(ReceiverStream::new(rx).map(|s| Ok(Event::default().data(s))))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("keep-alive"))
}
```

**send_message handler 修改** — 发送 token 到 event_bus:

```rust
async fn send_message(...) -> ... {
    // ... 获取 runtime, 调用 run_turn_async ...
    // ← 新增: 流式事件广播
    // 在 run_turn_async 的流式循环中:
    //   每次 AssistantEvent::TextDelta → state.event_bus.broadcast(&session_id, &json)
    //   每次 AssistantEvent::ToolStart → state.event_bus.broadcast(...)
    //   每次 AssistantEvent::ToolComplete → state.event_bus.broadcast(...)
}
```

### 最终简化后的命令

```bash
# 守护进程
cowd gateway run              # 前台运行 daemon (HTTP:8642 + Unix Socket + 飞书)
cowd gateway start            # systemd 后台启动
cowd gateway stop             # systemd 停止
cowd gateway status           # 查看状态

# TUI
cowd --solo                   # TUI (自动启动 daemon 如未运行)

# 部署
cowd install --systemd        # 安装 + 注册 systemd

# 信息
cowd version                  # 版本
cowd help                     # 帮助

# 脚本 (通过 HTTP API)
curl -X POST http://localhost:8642/api/sessions/{id}/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"hello"}'
```

### 删除的冗余代码统计

| 删除内容 | 行数 | 原因 |
|---------|------|------|
| `run_repl` compact 路径 | ~30 | thin client, 不需要 |
| `start_http_server()` | ~260 | daemon 统一管理 |
| `run_gateway_action` 子进程 | ~60 | daemon 直接运行 |
| `migrate-sessions` 相关 | ~50 | 不需要数据迁移 |
| `--compact` 标志处理 | ~10 | 不需要 compact 模式 |
| `CliAction::Serve` | ~5 | 不需要独立 serve |
| **总计** | **~415** | 净减少代码 |

### 新增代码

| 新增内容 | 行数 | 说明 |
|---------|------|------|
| `daemon.rs` | ~200 | 统一守护进程 |
| `event_bus.rs` | ~80 | 多前端事件同步 |
| `api_routes.rs` 新增 | +60 | event_bus + SSE handler |
| **总计** | **~340** | 净增加 |
