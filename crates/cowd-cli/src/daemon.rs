// ── Unified Gateway Daemon ────────────────────────────────────
// `cowd gateway run` = single daemon process providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Unix socket (/tmp/cowd.sock) for TUI connection
//   - Platform adapters (feishu, wechat_ilink, email)
// Shared state: ActiveSessions, CognitiveContextManager, GlobalToolRegistry, SessionEventBus

use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use crate::api_routes;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use memory::cognitive::CognitiveContextManager;
use memory::MemoryConfig;
use memory::UnifiedSessionStore;
use runtime::mirror::MessageMirror;
use runtime::platform::config::PlatformRuntimeConfig;
use runtime::platform::{PlatformConfig, PlatformRuntime};
use tools::GlobalToolRegistry;

use runtime::session_lifecycle::{
    EvictionPolicy, SessionLifecycleConfig, SessionLifecycleManager, SessionStatus,
};

// ── Background session cleanup task ────────────────────────────

/// Spawns a periodic task that checks all active sessions and closes
/// any that are idle or expired.
///
/// Uses `spawn_blocking` + `block_on` internally because the session
/// entry's `MutexGuard` is `!Send` across `.await` points.
fn spawn_session_cleanup_task(
    active_sessions: Arc<ActiveSessions>,
    lifecycle: Arc<SessionLifecycleManager>,
    unified_store: Option<Arc<UnifiedSessionStore>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            // Run lifecycle's internal TTL/idle/eviction checks first
            lifecycle.run_cleanup().await;
            let ids = active_sessions.list();
            for id in &ids {
                if let Some(status) = lifecycle.check_session(id).await {
                    if matches!(
                        status,
                        SessionStatus::Expired | SessionStatus::Idle | SessionStatus::Evicted
                    ) {
                        tracing::info!(session_id=%id, ?status, "cleanup: closing session");
                        if let Some(entry) = active_sessions.get(id) {
                            let entry = entry.clone();
                            let store = unified_store.clone();
                            let id = id.clone();
                            tokio::task::spawn_blocking(move || {
                                let handle = tokio::runtime::Handle::current();
                                handle.block_on(async {
                                    let mut runtime = entry.lock().await;
                                    // Shutdown MCP and plugins before dropping
                                    let _ = runtime.shutdown_mcp();
                                    let _ = runtime.shutdown_plugins();
                                    drop(runtime);
                                    if let Some(ref store) = store {
                                        let _ = store.delete_session(&id);
                                    }
                                });
                            })
                            .await
                            .ok();
                        }
                        active_sessions.remove(id);
                        lifecycle.unregister(id).await;
                    }
                } else {
                    // Session tracked in active_sessions but not in lifecycle
                    active_sessions.remove(id);
                }
            }
        }
    })
}

// ── Config ─────────────────────────────────────────────────────

pub struct DaemonConfig {
    pub http_addr: String,
    pub unix_sock_path: String,
    pub memory_config: Option<MemoryConfig>,
    pub platform_configs: Vec<PlatformConfig>,
    pub runtime_config: Option<serde_json::Value>,
    pub cors_origins: Vec<String>,
    pub auth_token: Option<String>,
    pub message_mirror: Option<Arc<MessageMirror>>,
}

// ── PID file guard ──────────────────────────────────────────────

struct PidFileGuard;

impl PidFileGuard {
    fn new() -> Result<Self, String> {
        let pid_path = crate::server::pid_file();
        let pid = std::process::id();
        std::fs::write(&pid_path, pid.to_string())
            .map_err(|e| format!("failed to write PID file {:?}: {e}", pid_path))?;
        tracing::info!(pid, path = %pid_path.display(), "PID file written");
        Ok(Self)
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let pid_path = crate::server::pid_file();
        if pid_path.exists() {
            std::fs::remove_file(&pid_path).ok();
            let _ = std::fs::remove_file(crate::server::addr_file());
            tracing::info!(path = %pid_path.display(), "PID file removed");
        }
    }
}

// ── Daemon entry point ─────────────────────────────────────────

pub async fn run_daemon(config: DaemonConfig) -> Result<(), String> {
    // 0. Write PID file (removed on drop via guard)
    let _pid_guard = PidFileGuard::new()?;

    // 1. Initialise shared state
    let sessions = Arc::new(ActiveSessions::default());
    let tools = Arc::new(GlobalToolRegistry::builtin());

    let cognitive: Option<Arc<CognitiveContextManager>> = match config.memory_config {
        Some(mem_cfg) => {
            tracing::info!("initialising memory manager...");
            CognitiveContextManager::new(mem_cfg)
                .await
                .ok()
                .map(Arc::new)
        }
        None => None,
    };

    let event_bus = SessionEventBus::new();

    let unified_store = crate::get_unified_store().ok().map(|s| Arc::new(s.clone()));
    let approval_dir = std::env::var_os("COWD_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".cowd"));
    let approval_gate = Arc::new(runtime::approval_gate::SmartApprovalGate::new(
        Arc::new(
            runtime::permission_enforcer::DestructivePatternDetector::new(approval_dir.clone()),
        ),
        runtime::ApprovalConfig::default(),
        Some(approval_dir.join("approval_history.json")),
    ));
    let profile_manager = Arc::new(runtime::ProfileManager::from_config_home(&approval_dir));
    if let Err(e) = profile_manager.initialize() {
        tracing::warn!("failed to initialize profile manager: {e}");
    }
    if let Ok(requested_profile) = std::env::var("COWD_PROFILE") {
        if profile_manager.get_profile(&requested_profile).is_none() {
            if let Err(e) = profile_manager.create_profile(&requested_profile) {
                tracing::warn!("failed to create requested profile {requested_profile}: {e}");
            }
        }
        if let Err(e) = profile_manager.switch_profile(&requested_profile) {
            tracing::warn!("failed to activate requested profile {requested_profile}: {e}");
        }
    }
    let profile_id = profile_manager.active_id();

    // Spawn background session cleanup (idle/expired session reaper)
    let lifecycle_config = SessionLifecycleConfig {
        idle_timeout: Some(Duration::from_secs(300)),
        max_ttl: Some(Duration::from_secs(86400)),
        max_active_sessions: 100,
        eviction_policy: EvictionPolicy::Lru,
        cleanup_interval: Duration::from_secs(300),
    };
    let lifecycle = Arc::new(SessionLifecycleManager::new(lifecycle_config));
    let _cleanup_handle = spawn_session_cleanup_task(
        sessions.clone(),
        lifecycle,
        unified_store.clone(),
        Duration::from_secs(300),
    );

    let app_state = Arc::new(api_routes::AppState {
        sessions: sessions.clone(),
        memory_manager: cognitive.clone(),
        unified_store: unified_store.clone(),
        tool_registry: tools.clone(),
        config: config.runtime_config.clone(),
        event_bus: event_bus.clone(),
        approval_gate: Some(approval_gate),
        auth_token: config.auth_token.clone(),
        workspace_root: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
        config_home: approval_dir.clone(),
        profile_id,
        profile_manager,
    });

    // 2. Build HTTP router (reuse api_routes + SSE)
    let app = {
        let default_origins = [
            "http://localhost:8642",
            "http://127.0.0.1:8642",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
        ];
        let mut cors_origin_values: Vec<HeaderValue> = default_origins
            .iter()
            .filter_map(|origin| origin.parse::<HeaderValue>().ok())
            .collect();
        for origin in &config.cors_origins {
            if let Ok(hv) = origin.parse::<HeaderValue>() {
                cors_origin_values.push(hv);
            }
        }
        let cors = CorsLayer::new()
            .allow_origin(cors_origin_values)
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::PATCH,
                axum::http::Method::DELETE,
            ])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

        let webui_dir = std::env::current_dir().unwrap().join("webui");
        api_routes::api_router(app_state.clone())
            .fallback_service(ServeDir::new(webui_dir))
            .layer(cors)
    };

    // 3. HTTP listener
    let listener = TcpListener::bind(&config.http_addr)
        .await
        .map_err(|e| format!("failed to bind HTTP {}: {}", config.http_addr, e))?;
    tracing::info!("HTTP + SSE on {}", config.http_addr);

    if let Err(e) = std::fs::write(
        crate::server::addr_file(),
        format!("http://{}", config.http_addr),
    ) {
        tracing::warn!("failed to write addr file: {e}");
    }

    // 4. Unix socket
    let _ = std::fs::remove_file(&config.unix_sock_path);
    let unix_listener = UnixListener::bind(&config.unix_sock_path).map_err(|e| {
        format!(
            "failed to bind unix socket {}: {}",
            config.unix_sock_path, e
        )
    })?;
    tracing::info!("Unix socket on {}", config.unix_sock_path);

    // 5. Platform adapters via PlatformRuntime
    let platform_runtime = Arc::new(PlatformRuntime::new(PlatformRuntimeConfig::default()));

    // Initialise the message mirror with default rules from daemon config
    if let Some(mirror) = config.message_mirror {
        platform_runtime.set_mirror(mirror).await;
    } else {
        let default_mirror = Arc::new(MessageMirror::new());
        platform_runtime.set_mirror(default_mirror).await;
    }

    for pc in &config.platform_configs {
        if !pc.enabled {
            continue;
        }
        let settings_json = serde_json::to_value(&pc.settings).unwrap_or_default();
        match pc.platform_type.as_str() {
            "feishu" | "lark" => {
                match runtime::platform::feishu::create_feishu_adapter(&settings_json) {
                    Ok(adapter) => {
                        let app_id = pc
                            .settings
                            .get("app_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        tracing::info!("feishu adapter created for app_id={app_id}");
                        if let Err(e) = platform_runtime.register_adapter(Box::new(adapter)).await {
                            tracing::error!("failed to register feishu adapter: {e}");
                        }
                    }
                    Err(e) => tracing::error!("failed to create feishu adapter: {e}"),
                }
            }
            "wechat_ilink" | "wechat" => {
                match runtime::platform::wechat_ilink::create_wechat_ilink_adapter(&settings_json) {
                    Ok(adapter) => {
                        tracing::info!("wechat_ilink adapter created");
                        if let Err(e) = platform_runtime.register_adapter(Box::new(adapter)).await {
                            tracing::error!("failed to register wechat_ilink adapter: {e}");
                        }
                    }
                    Err(e) => tracing::error!("failed to create wechat_ilink adapter: {e}"),
                }
            }
            "email" | "mail" => {
                match runtime::platform::email::create_email_adapter(&settings_json) {
                    Ok(adapter) => {
                        tracing::info!("email adapter created");
                        if let Err(e) = platform_runtime.register_adapter(Box::new(adapter)).await {
                            tracing::error!("failed to register email adapter: {e}");
                        }
                    }
                    Err(e) => tracing::error!("failed to create email adapter: {e}"),
                }
            }
            "wecom" => match runtime::platform::wecom::create_wecom_adapter(&settings_json) {
                Ok(adapter) => {
                    if let Err(e) = platform_runtime.register_adapter(Box::new(adapter)).await {
                        tracing::error!("failed to register wecom adapter: {e}");
                    } else {
                        tracing::info!("wecom adapter created and registered");
                    }
                }
                Err(e) => {
                    tracing::error!("wecom adapter init failed: {e}");
                }
            },
            other => {
                tracing::warn!("unknown platform type: {other}");
            }
        }
    }

    // Start the platform runtime (connects all registered adapters and spawns loops)
    if let Err(e) = platform_runtime.start().await {
        tracing::error!("failed to start platform runtime: {e}");
    }

    // 6. Unix socket accept loop (background)
    // Use spawn_blocking — handle_unix_client internally calls run_turn_async
    // which holds std::sync::MutexGuard across .await, making its future !Send.
    {
        let sessions = sessions.clone();
        let event_bus = event_bus.clone();
        let unified_store = unified_store.clone();
        tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move {
                loop {
                    match unix_listener.accept().await {
                        Ok((stream, _addr)) => {
                            let sessions = sessions.clone();
                            let event_bus = event_bus.clone();
                            let unified_store = unified_store.clone();
                            handle_unix_client(stream, sessions, event_bus, unified_store).await;
                        }
                        Err(e) => {
                            tracing::warn!("unix socket accept error: {e}");
                        }
                    }
                }
            })
        });
    }

    // 7. HTTP server with graceful shutdown on SIGINT/SIGTERM
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("failed to install SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
                _ = sigint.recv() => tracing::info!("SIGINT received, shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install ctrl_c handler");
            tracing::info!("shutdown signal received");
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .map_err(|e| format!("HTTP server error: {e}"))?;

    // ── Cleanup after shutdown ──
    tracing::info!("cleaning up daemon resources...");

    // Remove unix socket
    let _ = std::fs::remove_file(&config.unix_sock_path);
    tracing::info!("unix socket removed");

    // Shutdown platform adapters
    let _ = platform_runtime.shutdown().await;
    tracing::info!("platform runtime shut down");

    // PID file is cleaned up by PidFileGuard drop
    tracing::info!("daemon shutdown complete");
    Ok(())
}

// ── Unix client handler ─────────────────────────────────────────

/// Handle a single Unix socket client connection.
/// Reads newline-delimited JSON commands and writes JSON responses.
/// Supported commands:
///   {"cmd":"create_session","model":"..."}
///   {"cmd":"chat","session_id":"...","content":"..."}
///   {"cmd":"list_sessions"}
async fn handle_unix_client(
    stream: UnixStream,
    sessions: Arc<ActiveSessions>,
    event_bus: Arc<SessionEventBus>,
    unified_store: Option<Arc<UnifiedSessionStore>>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF
                break;
            }
            Ok(_n) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let response = match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(cmd) => match cmd.get("cmd").and_then(|c| c.as_str()) {
                        Some("create_session") => {
                            let model = cmd
                                .get("model")
                                .and_then(|m| m.as_str())
                                .unwrap_or("claude-sonnet-4-6");
                            let session_id = uuid::Uuid::new_v4().to_string();
                            let session = runtime::Session::new();
                            let runtime_result = if let Some(ref store) = unified_store {
                                crate::build_runtime_with_session_store(
                                    store.clone(),
                                    session,
                                    &session_id,
                                    model.to_string(),
                                    vec![],
                                    true,
                                    true,
                                    None,
                                    runtime::PermissionMode::WorkspaceWrite,
                                    None,
                                    None,
                                )
                            } else {
                                crate::build_runtime(
                                    session,
                                    &session_id,
                                    model.to_string(),
                                    vec![],
                                    true,
                                    true,
                                    None,
                                    runtime::PermissionMode::WorkspaceWrite,
                                    None,
                                    None,
                                )
                            };
                            match runtime_result {
                                Ok(runtime) => {
                                    if let Some(ref store) = unified_store {
                                        let record = crate::api_routes::new_api_session_record(
                                            &session_id,
                                            Some(model.to_string()),
                                        );
                                        if let Err(e) = store.upsert_session(&record).await {
                                            tracing::warn!(session_id = %session_id, error = %e, "failed to persist daemon session");
                                        }
                                    }
                                    let _ = sessions.register(session_id.clone(), runtime);
                                    serde_json::json!({
                                        "ok": true,
                                        "session_id": session_id,
                                    })
                                }
                                Err(e) => {
                                    serde_json::json!({
                                        "ok": false,
                                        "error": format!("failed to build runtime: {e}"),
                                    })
                                }
                            }
                        }
                        Some("chat") => {
                            let session_id = cmd
                                .get("session_id")
                                .and_then(|s| s.as_str())
                                .unwrap_or_default();
                            let content = cmd
                                .get("content")
                                .and_then(|c| c.as_str())
                                .unwrap_or_default();

                            if session_id.is_empty() || content.is_empty() {
                                serde_json::json!({
                                    "ok": false,
                                    "error": "session_id and content are required",
                                })
                            } else {
                                match sessions.get(session_id) {
                                    Some(entry) => {
                                        let mut guard = entry.lock().await;
                                        match guard
                                            .run_turn_async(
                                                content,
                                                &runtime::permissions::SharedPrompter::none(),
                                            )
                                            .await
                                        {
                                            Ok(summary) => {
                                                if let Some(ref store) = unified_store {
                                                    let session_snapshot = guard.session().clone();
                                                    if let Err(e) = crate::api_routes::sync_runtime_session_metadata_to_store(
                                                            store,
                                                            session_id,
                                                            &session_snapshot,
                                                        ).await {
                                                            tracing::warn!(session_id = %session_id, error = %e, "failed to sync daemon session metadata");
                                                        }
                                                }
                                                let final_text = summary
                                                    .assistant_messages
                                                    .last()
                                                    .map(|msg| {
                                                        msg.blocks
                                                            .iter()
                                                            .filter_map(|block| match block {
                                                                runtime::ContentBlock::Text {
                                                                    text,
                                                                } => Some(text.as_str()),
                                                                _ => None,
                                                            })
                                                            .collect::<Vec<_>>()
                                                            .join("")
                                                    })
                                                    .unwrap_or_default();

                                                let sse_data = serde_json::json!({
                                                    "type": "TurnComplete",
                                                    "session_id": session_id,
                                                    "response": final_text,
                                                    "iterations": summary.iterations,
                                                });
                                                event_bus
                                                    .broadcast(session_id, &sse_data.to_string())
                                                    .await;

                                                serde_json::json!({
                                                    "ok": true,
                                                    "response": final_text,
                                                    "iterations": summary.iterations,
                                                })
                                            }
                                            Err(e) => {
                                                let err_msg = e.to_string();
                                                let sse_data = serde_json::json!({
                                                    "type": "TurnError",
                                                    "session_id": session_id,
                                                    "error": err_msg,
                                                });
                                                event_bus
                                                    .broadcast(session_id, &sse_data.to_string())
                                                    .await;
                                                serde_json::json!({
                                                    "ok": false,
                                                    "error": err_msg,
                                                })
                                            }
                                        }
                                    }
                                    None => {
                                        serde_json::json!({
                                            "ok": false,
                                            "error": format!("session {session_id} not found"),
                                        })
                                    }
                                }
                            }
                        }
                        Some("list_sessions") => {
                            let ids = sessions.list();
                            serde_json::json!({ "ok": true, "sessions": ids })
                        }
                        Some(other) => {
                            serde_json::json!({
                                "ok": false,
                                "error": format!("unknown command: {other}"),
                            })
                        }
                        None => {
                            serde_json::json!({
                                "ok": false,
                                "error": "missing 'cmd' field",
                            })
                        }
                    },
                    Err(e) => {
                        serde_json::json!({
                            "ok": false,
                            "error": format!("invalid JSON: {e}"),
                        })
                    }
                };

                let mut resp_bytes = serde_json::to_vec(&response).unwrap_or_default();
                resp_bytes.push(b'\n');
                if let Err(e) = writer.write_all(&resp_bytes).await {
                    tracing::warn!("unix socket write error: {e}");
                    break;
                }
            }
            Err(e) => {
                tracing::warn!("unix socket read error: {e}");
                break;
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use memory::MemoryConfig;

    #[test]
    fn daemon_config_defaults() {
        let config = DaemonConfig {
            http_addr: "0.0.0.0:8642".into(),
            unix_sock_path: "/tmp/cowd.sock".into(),
            memory_config: None,
            platform_configs: vec![],
            runtime_config: None,
            cors_origins: vec![],
            auth_token: None,
            message_mirror: None,
        };
        assert_eq!(config.http_addr, "0.0.0.0:8642");
        assert_eq!(config.unix_sock_path, "/tmp/cowd.sock");
        assert!(config.memory_config.is_none());
        assert!(config.platform_configs.is_empty());
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn daemon_config_with_auth() {
        let config = DaemonConfig {
            http_addr: "127.0.0.1:9000".into(),
            unix_sock_path: "/tmp/test.sock".into(),
            memory_config: None,
            platform_configs: vec![],
            runtime_config: None,
            cors_origins: vec!["http://localhost:3000".into()],
            auth_token: Some("secret-token".into()),
            message_mirror: None,
        };
        assert_eq!(config.http_addr, "127.0.0.1:9000");
        assert_eq!(config.auth_token.as_deref(), Some("secret-token"));
        assert_eq!(config.cors_origins, vec!["http://localhost:3000"]);
    }

    #[test]
    fn daemon_config_with_memory() {
        let mem_cfg = MemoryConfig::default();
        let config = DaemonConfig {
            http_addr: "0.0.0.0:8642".into(),
            unix_sock_path: "/tmp/cowd.sock".into(),
            memory_config: Some(mem_cfg),
            platform_configs: vec![],
            runtime_config: None,
            cors_origins: vec![],
            auth_token: None,
            message_mirror: None,
        };
        assert!(config.memory_config.is_some());
    }
}
