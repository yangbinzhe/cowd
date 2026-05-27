#![allow(dead_code)]
//! Gateway server — service management (pid, status, start/stop) and HTTP entry-point.

use std::{fmt, fs, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    http::{header, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::Serialize;
use tokio::net::TcpListener as TokioTcpListener;
use tower_http::cors::CorsLayer;

use memory::{cognitive::CognitiveContextManager, MemoryConfig};
use runtime::platform::PlatformAdapter;
use runtime::platform::PlatformConfig;
use runtime::{ApprovalConfig, ConfigLoader, SessionResetPolicy};
use tools::GlobalToolRegistry;

use crate::api_routes;
use crate::gateway::ActiveSessions;

// ── Error type ───────────────────────────────────────────────────

#[derive(Debug)]
pub struct ServerError(String);

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError(e.to_string())
    }
}

impl From<std::num::ParseIntError> for ServerError {
    fn from(e: std::num::ParseIntError) -> Self {
        ServerError(e.to_string())
    }
}

impl From<axum::Error> for ServerError {
    fn from(e: axum::Error) -> Self {
        ServerError(e.to_string())
    }
}

// ── Helpers ──────────────────────────────────────────────────────

fn not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("Not Found"))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .expect("NOT_FOUND response should build")
        })
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()
        .map(PathBuf::from)
}

fn find_webui_dir() -> PathBuf {
    // Priority 1: next to the running binary
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            let dir = exe_dir.join("webui");
            if dir.join("index.html").exists() {
                return dir;
            }
        }
    }

    // Priority 2: ~/.cowd/webui/
    if let Some(home) = home_dir() {
        let dir = home.join(".cowd").join("webui");
        if dir.join("index.html").exists() {
            return dir;
        }
    }

    // Priority 3: cwd/webui/ (legacy behavior)
    if let Ok(cwd) = std::env::current_dir() {
        let dir = cwd.join("webui");
        if dir.join("index.html").exists() {
            return dir;
        }
    }

    // Fallback: return binary-dir-based path
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            return exe_dir.join("webui");
        }
    }

    PathBuf::from("webui")
}

// ── Service management ───────────────────────────────────────────

pub fn pid_file() -> PathBuf {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| format!("/tmp/cowd-{}", std::env::var("USER").unwrap_or_else(|_| "unknown".to_string())));
    let dir = PathBuf::from(runtime_dir);
    let _ = std::fs::create_dir_all(&dir);
    dir.join("cowd-serve.pid")
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub pid: u32,
    pub address: String,
}

pub fn get_server_status() -> Result<Option<ServerInfo>, ServerError> {
    let pid_path = pid_file();
    if !pid_path.exists() {
        return Ok(None);
    }

    let pid: u32 = fs::read_to_string(&pid_path)?
        .trim()
        .parse()?;

    if pid == 0 || !process_exists(pid) {
        fs::remove_file(&pid_path).ok();
        return Ok(None);
    }

    Ok(Some(ServerInfo {
        pid,
        address: "http://127.0.0.1:8642".to_string(),
    }))
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn process_exists(_pid: u32) -> bool {
    true
}

pub fn stop_server() -> Result<(), ServerError> {
    if let Some(info) = get_server_status()? {
        #[cfg(unix)]
        {
            std::process::Command::new("kill")
                .arg("-TERM")
                .arg(info.pid.to_string())
                .output()?;
        }
        fs::remove_file(pid_file())?;
    }
    Ok(())
}

// ── HTTP config ──────────────────────────────────────────────────

#[derive(Clone)]
pub struct HttpConfig {
    pub host: String,
    pub port: u16,
    pub auth_enabled: bool,
    pub auth_token: String,
    pub with_webui: bool,
    pub memory_config: Option<MemoryConfig>,
    pub session_store_path: Option<PathBuf>,
    pub platform_configs: Vec<PlatformConfig>,
    pub cors_origins: Vec<String>,
    pub approval_config: Option<ApprovalConfig>,
    pub session_reset: SessionResetPolicy,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8642,
            auth_enabled: true,
            auth_token: String::new(),
            with_webui: true,
            memory_config: None,
            session_store_path: None,
            platform_configs: Vec::new(),
            cors_origins: Vec::new(),
            approval_config: None,
            session_reset: SessionResetPolicy::None,
        }
    }
}

// ── HTTP server ──────────────────────────────────────────────────

async fn index_handler() -> Response {
    let html_path = find_webui_dir().join("index.html");

    let fallback_html = include_str!("../../../../webui/index.html");
    let html = if html_path.exists() {
        fs::read_to_string(&html_path).unwrap_or_else(|_| fallback_html.to_string())
    } else {
        fallback_html.to_string()
    };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
    .into_response()
}

pub async fn start_http_server(config: HttpConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TokioTcpListener::bind(&addr)
        .await
        .map_err(|e| ServerError(e.to_string()))?;

    // Write PID file
    let pid = std::process::id();
    fs::write(pid_file(), pid.to_string())?;

    // ── Build router from api_routes ──
    let active_sessions = Arc::new(ActiveSessions::new());
    let tool_registry = Arc::new(GlobalToolRegistry::builtin());

    let cwd = std::env::current_dir().unwrap_or_default();
    let runtime_config = ConfigLoader::default_for(&cwd)
        .load()
        .ok()
        .map(|c| json_value_to_serde_json(&c.as_json()));

    // Init memory manager if configured
    let memory_manager = match &config.memory_config {
        Some(mem_cfg) => CognitiveContextManager::new(mem_cfg.clone())
            .await
            .ok()
            .map(Arc::new),
        None => None,
    };

    let state = Arc::new(api_routes::AppState {
        sessions: active_sessions,
        memory_manager,
        tool_registry,
        config: runtime_config,
    });

    let api_router = api_routes::api_router(state.clone());

    // ── Public routes (no auth) ──
    let public_routes = Router::new()
        .route("/", get(index_handler));

    // ── CORS ──
    let default_origins = [
        "http://localhost:8642",
        "http://127.0.0.1:8642",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ];
    let mut cors_origin_values: Vec<HeaderValue> = default_origins
        .iter()
        .filter_map(|origin| match origin.parse::<HeaderValue>() {
            Ok(hv) => Some(hv),
            Err(e) => {
                tracing::warn!("Invalid default CORS origin '{}': {}", origin, e);
                None
            }
        })
        .collect();
    for origin in &config.cors_origins {
        match origin.parse::<HeaderValue>() {
            Ok(hv) => cors_origin_values.push(hv),
            Err(e) => tracing::warn!("Invalid CORS origin '{}': {}", origin, e),
        }
    }
    let cors = CorsLayer::new()
        .allow_origin(cors_origin_values)
        .allow_methods([axum::http::Method::GET, axum::http::Method::POST, axum::http::Method::PUT, axum::http::Method::PATCH, axum::http::Method::DELETE])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    // ── Merge routes ──
    let mut router = Router::new()
        .merge(public_routes)
        .merge(api_router)
        .layer(cors);

    // ── WebUI serving ──
    if config.with_webui {
        let webui_dir = find_webui_dir();
        let assets_dir = webui_dir.join("assets");

        let webui_dir_fallback = webui_dir.clone();
        let assets_dir_fallback = assets_dir.clone();

        router = router.fallback(move |req: Request<Body>| {
            let webui_dir = webui_dir_fallback.clone();
            let assets_dir = assets_dir_fallback.clone();
            async move {
                let path = req.uri().path().to_string();

                // Handle root path
                if path == "/" || path.is_empty() {
                    let html_path = webui_dir.join("index.html");
                    if html_path.exists() {
                        if let Ok(html) = fs::read_to_string(&html_path) {
                            return Response::builder()
                                .status(StatusCode::OK)
                                .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                                .body(Body::from(html))
                                .unwrap_or_else(|_| not_found());
                        }
                    }
                    return not_found();
                }

                // Handle /assets/ paths
                if path.starts_with("/assets/") || path.starts_with("/static/") {
                    let asset_file = path.trim_start_matches("/assets/").trim_start_matches("/static/");
                    let asset_path = assets_dir.join(asset_file);
                    if asset_path.exists() && asset_path.is_file() {
                        let content_type = if asset_file.ends_with(".css") {
                            "text/css"
                        } else if asset_file.ends_with(".js") {
                            "application/javascript"
                        } else if asset_file.ends_with(".svg") {
                            "image/svg+xml"
                        } else if asset_file.ends_with(".png") {
                            "image/png"
                        } else {
                            "application/octet-stream"
                        };
                        if let Ok(content) = fs::read(&asset_path) {
                            return Response::builder()
                                .status(StatusCode::OK)
                                .header(header::CONTENT_TYPE, content_type)
                                .body(Body::from(content))
                                .unwrap_or_else(|_| not_found());
                        }
                    }
                    return not_found();
                }

                // Handle other static files
                let file_path = webui_dir.join(path.trim_start_matches("/"));
                if file_path.exists() && file_path.is_file() {
                    let content_type = if path.ends_with(".css") {
                        "text/css"
                    } else if path.ends_with(".js") {
                        "application/javascript"
                    } else if path.ends_with(".html") {
                        "text/html; charset=utf-8"
                    } else if path.ends_with(".svg") {
                        "image/svg+xml"
                    } else if path.ends_with(".png") {
                        "image/png"
                    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
                        "image/jpeg"
                    } else {
                        "application/octet-stream"
                    };

                    if let Ok(content) = fs::read(&file_path) {
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, content_type)
                            .body(Body::from(content))
                            .unwrap_or_else(|_| not_found());
                    }
                }

                not_found()
            }
        });
    }

    let app = router;

    tracing::info!(port = config.port, host = %config.host, "server started");
    println!("Cowd gateway HTTP listening on {} (PID: {})", addr, pid);

    // Create and connect platform adapters
    for pc in &config.platform_configs {
        if !pc.enabled {
            continue;
        }
        let settings_json = serde_json::to_value(&pc.settings).unwrap_or_default();

        match pc.platform_type.as_str() {
            "feishu" | "lark" => {
                match runtime::platform::feishu::create_feishu_adapter(&settings_json) {
                    Ok(mut adapter) => {
                        tracing::info!(
                            "feishu adapter created for app_id={}",
                            pc.settings
                                .get("app_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("?")
                        );
                        match adapter.connect().await {
                            Ok(()) => tracing::info!("feishu adapter connected"),
                            Err(e) => tracing::error!("feishu adapter connect failed: {e}"),
                        }
                    }
                    Err(e) => {
                        tracing::error!("failed to create feishu adapter: {e}");
                    }
                }
            }
            "wechat_ilink" | "wechat" => {
                match runtime::platform::wechat_ilink::create_wechat_ilink_adapter(&settings_json) {
                    Ok(mut adapter) => {
                        tracing::info!("wechat_ilink adapter created");
                        match adapter.connect().await {
                            Ok(()) => tracing::info!("wechat_ilink adapter connected"),
                            Err(e) => tracing::error!("wechat_ilink adapter connect failed: {e}"),
                        }
                    }
                    Err(e) => {
                        tracing::error!("failed to create wechat_ilink adapter: {e}");
                    }
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
                    Err(e) => {
                        tracing::error!("failed to create email adapter: {e}");
                    }
                }
            }
            other => {
                tracing::warn!("unknown platform type: {other}");
            }
        }
    }

    axum::serve(listener, app)
        .await
        .map_err(|e| ServerError(e.to_string()))?;

    // Clean up PID file
    tracing::info!("server shutting down");
    fs::remove_file(pid_file()).ok();

    Ok(())
}

fn json_value_to_serde_json(v: &runtime::JsonValue) -> serde_json::Value {
    match v {
        runtime::JsonValue::Null => serde_json::Value::Null,
        runtime::JsonValue::Bool(b) => serde_json::Value::Bool(*b),
        runtime::JsonValue::Number(n) => serde_json::json!(*n),
        runtime::JsonValue::String(s) => serde_json::Value::String(s.clone()),
        runtime::JsonValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(json_value_to_serde_json).collect())
        }
        runtime::JsonValue::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), json_value_to_serde_json(v)))
                .collect(),
        ),
    }
}
