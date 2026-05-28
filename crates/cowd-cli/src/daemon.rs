// ── Unified Gateway Daemon ────────────────────────────────────
// `cowd gateway run` = single daemon process providing:
//   - HTTP API (0.0.0.0:8642) + SSE streaming
//   - Unix socket (/tmp/cowd.sock) for TUI connection
//   - Platform adapters (feishu, wechat_ilink, email)
// Shared state: ActiveSessions, CognitiveContextManager, GlobalToolRegistry, SessionEventBus

use std::sync::Arc;

use axum::http::{header, HeaderValue};
use tokio::net::{TcpListener, UnixListener};
use tower_http::cors::CorsLayer;

use crate::api_routes;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use memory::cognitive::CognitiveContextManager;
use memory::MemoryConfig;
use runtime::platform::{PlatformAdapter, PlatformConfig};
use tools::GlobalToolRegistry;

// ── Config ─────────────────────────────────────────────────────

pub struct DaemonConfig {
    pub http_addr: String,
    pub unix_sock_path: String,
    pub memory_config: Option<MemoryConfig>,
    pub platform_configs: Vec<PlatformConfig>,
    pub runtime_config: Option<serde_json::Value>,
    pub cors_origins: Vec<String>,
    pub auth_token: Option<String>,
}

// ── Daemon entry point ─────────────────────────────────────────

pub async fn run_daemon(
    config: DaemonConfig,
) -> Result<(), String> {
    // 1. Initialise shared state
    let sessions = Arc::new(ActiveSessions::new());
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

    let app_state = Arc::new(api_routes::AppState {
        sessions: sessions.clone(),
        memory_manager: cognitive.clone(),
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

        api_routes::api_router(app_state).layer(cors)
    };

    // 3. HTTP listener
    let listener = TcpListener::bind(&config.http_addr)
        .await
        .map_err(|e| format!("failed to bind HTTP {}: {}", config.http_addr, e))?;
    tracing::info!("HTTP + SSE on {}", config.http_addr);

    // 4. Unix socket
    let _ = std::fs::remove_file(&config.unix_sock_path);
    let unix_listener = UnixListener::bind(&config.unix_sock_path)
        .map_err(|e| format!("failed to bind unix socket {}: {}", config.unix_sock_path, e))?;
    tracing::info!("Unix socket on {}", config.unix_sock_path);

    // 5. Platform adapters
    for pc in &config.platform_configs {
        if !pc.enabled {
            continue;
        }
        let settings_json = serde_json::to_value(&pc.settings).unwrap_or_default();
        match pc.platform_type.as_str() {
            "feishu" | "lark" => {
                match runtime::platform::feishu::create_feishu_adapter(&settings_json) {
                    Ok(mut adapter) => {
                        let app_id = pc
                            .settings
                            .get("app_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        tracing::info!("feishu adapter created for app_id={app_id}");
                        match adapter.connect().await {
                            Ok(()) => tracing::info!("feishu adapter connected"),
                            Err(e) => tracing::error!("feishu adapter connect failed: {e}"),
                        }
                    }
                    Err(e) => tracing::error!("failed to create feishu adapter: {e}"),
                }
            }
            "wechat_ilink" | "wechat" => {
                match runtime::platform::wechat_ilink::create_wechat_ilink_adapter(
                    &settings_json,
                ) {
                    Ok(mut adapter) => {
                        tracing::info!("wechat_ilink adapter created");
                        match adapter.connect().await {
                            Ok(()) => tracing::info!("wechat_ilink adapter connected"),
                            Err(e) => tracing::error!("wechat_ilink adapter connect failed: {e}"),
                        }
                    }
                    Err(e) => tracing::error!("failed to create wechat_ilink adapter: {e}"),
                }
            }
            "email" | "mail" => {
                match runtime::platform::email::create_email_adapter(&settings_json) {
                    Ok(mut adapter) => {
                        tracing::info!("email adapter created");
                        match adapter.connect().await {
                            Ok(()) => tracing::info!("email adapter connected"),
                            Err(e) => tracing::error!("email adapter connect failed: {e}"),
                        }
                    }
                    Err(e) => tracing::error!("failed to create email adapter: {e}"),
                }
            }
            other => {
                tracing::warn!("unknown platform type: {other}");
            }
        }
    }

    // 6. Unix socket accept loop (background)
    tokio::spawn(async move {
        loop {
            match unix_listener.accept().await {
                Ok((stream, _)) => {
                    drop(stream); // TODO: handle TUI client connection over Unix socket
                }
                Err(e) => {
                    tracing::warn!("unix socket accept error: {e}");
                }
            }
        }
    });

    // 7. HTTP server (blocking main loop)
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("HTTP server error: {e}"))
}
