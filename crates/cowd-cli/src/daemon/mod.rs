// ── Unified Gateway Daemon ────────────────────────────────────
// `cowd gateway run` = single daemon process providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Unix socket (/tmp/cowd.sock) for TUI connection
//   - Platform adapters (feishu, wechat_ilink, email)
// Shared state: ActiveSessions, CognitiveContextManager, GlobalToolRegistry, SessionEventBus

pub mod commands;
pub mod prompter;
pub mod singletons;

use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue};
use tokio::net::{TcpListener, UnixListener};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use crate::api_routes;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use memory::cognitive::CognitiveContextManager;
use memory::MemoryConfig;
use memory::UnifiedSessionStore;
use runtime::platform::{PlatformConfig, PlatformRuntime};
use runtime::platform::config::PlatformRuntimeConfig;
use runtime::mirror::MessageMirror;
use tools::GlobalToolRegistry;

use runtime::session_lifecycle::{EvictionPolicy, SessionLifecycleConfig, SessionLifecycleManager, SessionStatus};

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
                    if matches!(status, SessionStatus::Expired | SessionStatus::Idle | SessionStatus::Evicted) {
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
            tracing::info!(path = %pid_path.display(), "PID file removed");
        }
        let _ = std::fs::remove_file(crate::server::addr_file());
    }
}

// ── Daemon entry point ─────────────────────────────────────────

pub async fn run_daemon(
    config: DaemonConfig,
) -> Result<(), String> {
    // 0. Write PID file (removed on drop via guard)
    let _pid_guard = PidFileGuard::new()?;

    // 0.5. Initialize global singletons once (before any session state)
    singletons::GLOBAL_PLUGIN.get_or_init(|| {
        Arc::new(crate::build_runtime_plugin_state().expect("global plugin init"))
    });
    singletons::GLOBAL_MEMORY.get_or_init(|| {
        let mem_cfg = config.memory_config.clone().unwrap_or_default();
        // Use block_in_place to yield the current runtime thread before
        // performing a nested block_on. This prevents tokio v1.52+ from
        // rejecting same-runtime nested block_on.
        let cognitive = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(CognitiveContextManager::new(mem_cfg))
        });
        Arc::new(cognitive.expect("global memory init"))
    });
    singletons::GLOBAL_STORE.get_or_init(|| {
        Arc::new(crate::get_unified_store().expect("global store init").clone())
    });

    // 1. Initialise shared state
    let sessions = Arc::new(ActiveSessions::default());
    let tools = Arc::new(GlobalToolRegistry::builtin());

    let cognitive: Option<Arc<CognitiveContextManager>> = match &config.memory_config {
        Some(mem_cfg) => {
            tracing::info!("initialising memory manager...");
            CognitiveContextManager::new(mem_cfg.clone())
                .await
                .ok()
                .map(Arc::new)
        }
        None => None,
    };

    let event_bus = SessionEventBus::new();

    let unified_store = crate::get_unified_store()
        .ok()
        .map(|s| Arc::new(s.clone()));

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
        unified_store,
        tool_registry: tools.clone(),
        config: config.runtime_config.clone(),
        event_bus: event_bus.clone(),
        auth_token: config.auth_token.clone(),
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
    let unix_listener = UnixListener::bind(&config.unix_sock_path)
        .map_err(|e| format!("failed to bind unix socket {}: {}", config.unix_sock_path, e))?;
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
                match runtime::platform::wechat_ilink::create_wechat_ilink_adapter(
                    &settings_json,
                ) {
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
            "wecom" => {
                match runtime::platform::wecom::create_wecom_adapter(&settings_json) {
                    Ok(adapter) => {
                        if let Err(e) = platform_runtime.register_adapter(Box::new(adapter)).await {
                            tracing::error!("failed to register wecom adapter: {e}");
                        } else {
                            tracing::info!("wecom adapter created and registered");
                        }
                    }
                    Err(e) => { tracing::error!("wecom adapter init failed: {e}"); }
                }
            }
            other => {
                tracing::warn!("unknown platform type: {other}");
            }
        }
    }

    // Start the platform runtime (connects all registered adapters and spawns loops)
    if let Err(e) = platform_runtime.start().await {
        tracing::error!("failed to start platform runtime: {e}");
    }

    // 6. SocketPrompter (bridges tool approval between daemon and TUI)
    let (prompter_tx, mut prompter_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let prompter = Arc::new(prompter::SocketPrompter::new(prompter_tx));

    // Drain prompter channel (approval requests forwarded to TUI in later phases)
    tokio::spawn(async move {
        while let Some(msg) = prompter_rx.recv().await {
            tracing::debug!(%msg, "SocketPrompter approval request (no TUI attached yet)");
            // Phase B: forward to active TUI connection
        }
    });

    // 7. Unix socket accept loop (background)
    // Use spawn_blocking — handle_unix_client internally calls run_turn_async
    // which holds std::sync::MutexGuard across .await, making its future !Send.
    {
        let sessions = sessions.clone();
        let event_bus = event_bus.clone();
        let prompter = prompter.clone();
        tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move {
                loop {
                    match unix_listener.accept().await {
                        Ok((stream, _addr)) => {
                            let sessions = sessions.clone();
                            let event_bus = event_bus.clone();
                            let prompter = prompter.clone();
                            commands::handle_unix_client(stream, sessions, event_bus, Some(prompter)).await;
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
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate()
            ).expect("failed to install SIGTERM handler");
            let mut sigint = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::interrupt()
            ).expect("failed to install SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
                _ = sigint.recv() => tracing::info!("SIGINT received, shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.expect("failed to install ctrl_c handler");
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

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use memory::MemoryConfig;

    #[test]
    fn daemon_config_defaults() {
        let config = DaemonConfig {
            http_addr: "0.0.0.0:8642".into(),
            unix_sock_path: crate::server::socket_file().to_string_lossy().to_string(),
            memory_config: None,
            platform_configs: vec![],
            runtime_config: None,
            cors_origins: vec![],
            auth_token: None,
            message_mirror: None,
        };
        assert_eq!(config.http_addr, "0.0.0.0:8642");
        assert_eq!(config.unix_sock_path, crate::server::socket_file().to_string_lossy());
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
            unix_sock_path: crate::server::socket_file().to_string_lossy().to_string(),
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
