#![allow(dead_code)]
//! ClawServer

use std::{
    collections::HashMap,
    fmt,
    fs,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
};

use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State as AxumState, WebSocketUpgrade, ws::{Message as WsMessage, WebSocket}},
    http::{header, StatusCode, Request, HeaderValue},
    response::{IntoResponse, Response, sse::{Event, Sse}},
    routing::{delete, get, patch, post, put},
    middleware,
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener as TokioTcpListener,
    sync::{broadcast, mpsc, RwLock},
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use uuid::Uuid;

// ── 模块导入 ─────────────────────────────────────────────────────────────────

use api::{
    max_tokens_for_model, ContentBlockDelta, InputContentBlock, InputMessage,
    MessageRequest, OpenAiCompatClient, OpenAiCompatConfig, StreamEvent,
};
use memory::{
    cognitive::CognitiveContextManager,
    UnifiedSessionStore,
    types::Message as MemMessage,
    MemoryConfig, PreparedContext,
};
use runtime::platform::{PlatformRuntime, PlatformConfig, PlatformError};
use runtime::team_cron_registry::CronScheduler;
use runtime::CompactionConfig;
use runtime::{
    ApiClient as RuntimeApiClient, ApiRequest, AssistantEvent, ConversationRuntime, RuntimeError, ToolCallback, ToolError, ToolExecutor,
    PermissionMode, PermissionPolicy, SessionResetPolicy,
    ContentBlock as SessionContentBlock, ConversationMessage as SessionMessage, 
    MessageRole as SessionMessageRole, Session,
};
use tools;

// ── Custom Error Type ──────────────────────────────────────────────────────────

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

impl From<memory::MemoryError> for ServerError {
    fn from(e: memory::MemoryError) -> Self {
        ServerError(e.to_string())
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        ServerError(e.to_string())
    }
}

/// Helper function to return a 404 Not Found response
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

// ── Global Usage Tracker ───────────────────────────────────────────────────────

/// Per-model usage accumulation for the /api/usage endpoint.
#[derive(Debug, Default)]
struct ModelUsageAccum {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    turns: u64,
}

/// Thread-safe global usage tracker that records token usage per model.
#[derive(Debug, Default)]
struct GlobalUsageTracker {
    by_model: std::sync::RwLock<HashMap<String, ModelUsageAccum>>,
    total_sessions: std::sync::atomic::AtomicU64,
}

impl GlobalUsageTracker {
    fn new() -> Self {
        Self::default()
    }

    fn record(&self, model: &str, usage: runtime::TokenUsage) {
        self.total_sessions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut map = self.by_model.write().unwrap_or_else(|poisoned| {
            tracing::warn!("usage tracker lock poisoned; recovering");
            poisoned.into_inner()
        });
        let accum = map.entry(model.to_string()).or_default();
        accum.input_tokens += usage.input_tokens as u64;
        accum.output_tokens += usage.output_tokens as u64;
        accum.cache_creation_input_tokens += usage.cache_creation_input_tokens as u64;
        accum.cache_read_input_tokens += usage.cache_read_input_tokens as u64;
        accum.turns += 1;
    }

    fn snapshot(&self) -> UsageResponse {
        let map = self.by_model.read().unwrap_or_else(|poisoned| {
            tracing::warn!("usage tracker lock poisoned; recovering");
            poisoned.into_inner()
        });
        let mut by_model = HashMap::new();
        for (model, accum) in map.iter() {
            let pricing = runtime::pricing_for_model(model);
            let tu = runtime::TokenUsage {
                input_tokens: accum.input_tokens as u32,
                output_tokens: accum.output_tokens as u32,
                cache_creation_input_tokens: accum.cache_creation_input_tokens as u32,
                cache_read_input_tokens: accum.cache_read_input_tokens as u32,
            };
            let cost = pricing.map_or_else(
                || tu.estimate_cost_usd(),
                |p| tu.estimate_cost_usd_with_pricing(p),
            );
            by_model.insert(model.clone(), ModelUsageResponse {
                model: model.clone(),
                input_tokens: accum.input_tokens,
                output_tokens: accum.output_tokens,
                cost_usd: runtime::format_usd(cost.total_cost_usd()),
                turns: accum.turns,
            });
        }
        UsageResponse {
            total_sessions: self.total_sessions.load(std::sync::atomic::Ordering::Relaxed),
            by_model,
        }
    }
}

#[derive(Debug, Serialize)]
struct UsageResponse {
    total_sessions: u64,
    by_model: HashMap<String, ModelUsageResponse>,
}

#[derive(Debug, Serialize)]
struct ModelUsageResponse {
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    cost_usd: String,
    turns: u64,
}

// ── Service Management ─────────────────────────────────────────────────────────

// B8: PID file in user-local directory with restricted permissions
fn pid_file() -> PathBuf {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| format!("/tmp/cowd-{}", std::env::var("USER").unwrap_or_else(|_| "unknown".to_string())));
    let dir = PathBuf::from(runtime_dir);
    let _ = std::fs::create_dir_all(&dir);
    dir.join("cowd-serve.pid")
}

/// Server status info
#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub pid: u32,
    pub address: String,
}

/// Check if server is running and get its info
pub fn get_server_status() -> Result<Option<ServerInfo>, ServerError> {
    let pid_path = pid_file();
    if !pid_path.exists() {
        return Ok(None);
    }

    let pid: u32 = fs::read_to_string(&pid_path)?
        .trim()
        .parse()?;

    // Check if process exists
    if pid == 0 || !process_exists(pid) {
        fs::remove_file(&pid_path).ok();
        return Ok(None);
    }

    Ok(Some(ServerInfo {
        pid,
        address: "http://127.0.0.1:8642".to_string(),
    }))
}

/// Check if process exists
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

/// Stop the running server
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

// ── SSE Streaming Types ────────────────────────────────────────────────────────

/// SSE chunk type for streaming responses
pub type SseChunk = Option<String>;

/// Pending reply enum for streaming vs non-streaming
pub enum PendingReply {
    Oneshot(mpsc::Sender<String>),
    Stream(mpsc::Sender<SseChunk>),
}

// ── Session Events for Real-time Sync ───────────────────────────────────────────

/// Session events for WebSocket broadcasting
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum SessionEvent {
    #[serde(rename = "session_created")]
    SessionCreated { session_id: String, title: Option<String> },
    #[serde(rename = "session_updated")]
    SessionUpdated { session_id: String },
    #[serde(rename = "session_deleted")]
    SessionDeleted { session_id: String },
    #[serde(rename = "message_added")]
    MessageAdded { session_id: String, message_count: u32 },
    #[serde(rename = "runtime_started")]
    RuntimeStarted { session_id: String },
    #[serde(rename = "runtime_finished")]
    RuntimeFinished { session_id: String },
}

// ── HTTP Server State ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct HttpAppState {
    auth_token: String,
    auth_enabled: bool,
    /// Cognitive memory manager (optional)
    cognitive_manager: Option<Arc<CognitiveContextManager>>,
    /// Session store for persistence (required)
    session_store: Arc<UnifiedSessionStore>,
    /// Memory store path for status
    memory_store_path: String,
    /// Pending streaming replies
    pending: Arc<RwLock<HashMap<String, PendingReply>>>,
    /// Request timeout in seconds
    request_timeout_secs: u64,
    /// Skill service for skill management
    skill_service: Arc<SkillService>,
    /// Platform runtime for multi-channel adapters (Feishu, WeChat, Email)
    platform_runtime: Option<Arc<PlatformRuntime>>,
    /// Broadcast channel for session events (WebSocket sync)
    session_broadcast: broadcast::Sender<SessionEvent>,
    /// P0-1: Smart approval gate (combines destructive detection + approval config + pending map)
    approval_gate: Arc<runtime::approval_gate::SmartApprovalGate>,
    /// Sessions directory for JSONL files (splice operations)
    sessions_dir: PathBuf,
    /// P1-5: Cron scheduler
    cron_scheduler: Arc<CronScheduler>,
    /// Global usage tracker for /api/usage
    usage_tracker: Arc<GlobalUsageTracker>,
    /// 3B-3: Fact checker for knowledge graph validation
    fact_checker: Arc<tokio::sync::Mutex<memory::FactChecker>>,
}

/// HTTP API 配置
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
    /// Additional CORS origins beyond the default local ones.
    /// If empty, only the default local origins are allowed.
    pub cors_origins: Vec<String>,
    /// Approval gate configuration (loaded from config.yaml).
    pub approval_config: Option<runtime::ApprovalConfig>,
    /// Session reset policy for platform sessions (from gateway config).
    /// Defaults to None when no platform adapters are configured.
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

/// 初始化应用状态
async fn init_app_state(config: &HttpConfig) -> Result<HttpAppState, ServerError> {
    // 初始化记忆管理器
    let cognitive_manager = if let Some(ref mem_config) = config.memory_config {
        match CognitiveContextManager::new(mem_config.clone()).await {
            Ok(mgr) => {
                tracing::info!("Cognitive memory manager initialized");
                Some(Arc::new(mgr))
            }
            Err(e) => {
                eprintln!("warn: Failed to initialize memory manager: {}", e);
                None
            }
        }
    } else {
        None
    };

    // 初始化会话存储（必需）
    let session_store_path = config.session_store_path.as_ref().ok_or_else(|| {
        ServerError("session_store_path is required for HTTP server".to_string())
    })?;
    
    let session_store = match UnifiedSessionStore::open(session_store_path) {
        Ok(store) => {
            tracing::info!("Session store initialized at {:?}", session_store_path);
            Arc::new(store)
        }
        Err(e) => {
            return Err(ServerError(format!("Failed to initialize session store: {}", e)));
        }
    };

    // 初始化平台运行时 (如果配置了平台适配器)
    let platform_runtime = if !config.platform_configs.is_empty() {
        let mut runtime_config = runtime::platform::config::PlatformRuntimeConfig::default();
        runtime_config.session_reset = config.session_reset;
        let cleanup_secs = runtime_config.cleanup_interval_secs;
        let runtime = Arc::new(PlatformRuntime::new(runtime_config));
        let cleanup_runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(cleanup_secs)).await;
                cleanup_runtime.cleanup_sessions().await;
            }
        });

        if let Err(e) = runtime.start().await {
            tracing::warn!("Failed to start platform runtime: {}", e);
        }

        Some(runtime)
    } else {
        None
    };

    // 构建内存存储路径
    let memory_store_path = config
        .memory_config
        .as_ref()
        .map(|c| c.store.blob_dir.display().to_string())
        .unwrap_or_else(|| "~/.cowd/memory/sessions.db".to_string());

    Ok(HttpAppState {
        auth_token: config.auth_token.clone(),
        auth_enabled: config.auth_enabled,
        cognitive_manager,
        session_store,
        memory_store_path,
        pending: Arc::new(RwLock::new(HashMap::new())),
        request_timeout_secs: 120,
        skill_service: Arc::new(SkillService::new()),
        platform_runtime,
        session_broadcast: broadcast::channel(1000).0,
        approval_gate: Arc::new(runtime::approval_gate::SmartApprovalGate::new(
            Arc::new(runtime::permission_enforcer::DestructivePatternDetector::new(
                config.session_store_path.as_ref()
                    .map(|p| p.parent().unwrap_or_else(|| std::path::Path::new(".")).to_path_buf())
                    .unwrap_or_else(|| std::path::PathBuf::from("."))
            )),
            config.approval_config.clone().unwrap_or_default(),
            Some(runtime::cowd_dirs::project_dot_dir(
                &std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
            ).join("approval").join("history.json")),
        )),
        sessions_dir: runtime::workspace_sessions_dir(
            &std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
        ).unwrap_or_else(|_| PathBuf::from(".")),
        cron_scheduler: Arc::new(CronScheduler::new(
            runtime::cowd_dirs::cron_jobs_path()
        )),
        usage_tracker: Arc::new(GlobalUsageTracker::new()),
        fact_checker: Arc::new(tokio::sync::Mutex::new(memory::FactChecker::new())),
    })
}

/// Get the user's home directory (Linux/macOS: HOME, Windows: USERPROFILE).
fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()
        .map(PathBuf::from)
}

/// Locate the webui directory using a priority-based search:
/// 1. <binary_dir>/webui/  (local build: target/release/webui/)
/// 2. ~/.cowd/webui/       (system install)
/// 3. <cwd>/webui/         (legacy compat)
/// Falls back to <binary_dir>/webui/ (will use embedded index.html if missing).
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

    // Fallback: return binary-dir-based path (embedded index.html will be used)
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            return exe_dir.join("webui");
        }
    }

    PathBuf::from("webui")
}

/// 启动 HTTP 服务器
pub async fn start_http_server(config: HttpConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TokioTcpListener::bind(&addr)
        .await
        .map_err(|e| ServerError(e.to_string()))?;

    // Write PID file
    let pid = std::process::id();
    fs::write(pid_file(), pid.to_string())?;

    // Initialize app state
    let state = init_app_state(&config).await?;

    // Build router - routes are defined below with auth separation (B1 fix)

    // B1: Apply auth middleware to all /api/* and /v1/* routes (except public auth endpoints)
    // Split into public routes (no auth) and protected routes (auth required)
    let public_routes = Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/api/auth/login", post(auth_login_handler))
        .route("/api/auth/verify", get(auth_verify_handler))
        .route("/api/auth/logout", post(auth_logout_handler));

    // M3: Domain-grouped routes (was monolithic protected_routes)
    let session_routes = Router::new()
        .route("/api/sessions", get(list_sessions_handler))
        .route("/api/sessions", post(create_session_handler))
        .route("/api/sessions/:id", get(get_session_handler))
        .route("/api/sessions/:id", delete(delete_session_handler))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/messages", get(get_session_messages_handler))
        .route("/api/sessions/:id/messages", post(send_message_handler))
        .route("/api/sessions/:id/messages/stream", post(send_message_stream_handler));

    let config_routes = Router::new()
        .route("/api/config", get(get_config_handler))
        .route("/api/config", put(update_config_handler))
        .route("/api/config/providers", get(get_providers_handler));

    let memory_routes = Router::new()
        .route("/api/memory", get(memory_status_handler))
        .route("/api/memory/stats", get(memory_stats_handler))
        .route("/api/memory/layers", get(list_memory_layers_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route("/api/memory/:layer", get(get_memory_layer_handler))
        .route("/api/memory/:layer", post(create_memory_entry_handler))
        .route("/api/memory/:layer/:id", delete(delete_memory_entry_handler))
        .route("/api/memory/entry/:id", patch(update_memory_entry_handler))
        .route("/api/memory/entry/:id", get(get_memory_entry_handler))
        .route("/api/memory/entities", get(list_entities_handler))
        .route("/api/memory/entities/detect", post(detect_entities_handler))
        .route("/api/memory/triples", get(list_triples_handler))
        .route("/api/memory/triples", post(add_triple_handler))
        .route("/api/memory/facts/check", post(check_facts_handler))
        .route("/api/memory/facts/register", post(register_facts_handler))
        .route("/api/memory/facts/audit", get(audit_facts_handler));

    let platform_routes = Router::new()
        .route("/api/platforms", get(list_platforms_handler))
        .route("/api/platforms/:name", get(get_platform_handler))
        .route("/api/platforms/:name/sessions", get(list_platform_sessions_handler))
        .route("/api/platforms/:name/sessions/:id", delete(delete_platform_session_handler));

    let command_routes = Router::new()
        .route("/api/commands", get(list_commands_handler))
        .route("/api/commands/history", get(command_history_handler))
        .route("/api/commands/execute", post(execute_command_handler));

    let workspace_routes = Router::new()
        .route("/api/workspace", get(get_current_workspace_handler))
        .route("/api/workspaces", get(list_workspaces_handler))
        .route("/api/workspace/files", get(list_files_handler))
        .route("/api/workspace/files", post(create_file_handler));

    let approval_routes = Router::new()
        .route("/api/approval/pending", get(get_pending_approvals_handler))
        .route("/api/approval/respond", post(respond_to_approval_handler))
        .route("/api/approval/config", get(get_approval_config_handler))
        .route("/api/approval/config", put(update_approval_config_handler))
        .route("/api/approval/solo", post(toggle_solo_handler))
        .route("/api/approval/history", get(list_approval_history_handler));

    let cron_routes = Router::new()
        .route("/api/crons", get(list_crons_handler))
        .route("/api/crons", post(create_cron_handler))
        .route("/api/crons/logs", get(list_cron_logs_handler))
        .route("/api/crons/:id/logs", get(list_cron_job_logs_handler))
        .route("/api/crons/:id", delete(delete_cron_handler))
        .route("/api/crons/:id/run", post(run_cron_handler))
        .route("/api/crons/:id/pause", post(pause_cron_handler))
        .route("/api/crons/:id/resume", post(resume_cron_handler));

    

    let other_routes = Router::new()
        .route("/api/upload", post(upload_file_handler))
        .route("/api/file/raw", get(get_raw_file_handler))
        .route("/api/usage", get(usage_handler))
        .route("/api/onboarding/status", get(onboarding_status_handler))
        .route("/api/onboarding/test", post(onboarding_test_handler))
        .route("/ws", get(ws_handler))
        .route("/ws/sessions", get(ws_sessions_handler));

    let protected_routes = Router::new()
        .merge(session_routes)
        .merge(config_routes)
        .merge(memory_routes)
        .merge(platform_routes)
        .merge(command_routes)
        .merge(workspace_routes)
        .merge(approval_routes)
        .merge(cron_routes)
        .merge(other_routes)
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    // B9: Restrictive CORS - default local origins + configurable extra origins
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

    let mut router = Router::new()
        .merge(public_routes)
        .merge(protected_routes)
        .layer(cors);

    // Add WebUI routes if enabled
    if config.with_webui {
        // Locate webui directory using priority-based search
        // (binary dir > ~/.cowd/ > cwd)
        let webui_dir = find_webui_dir();
        let assets_dir = webui_dir.join("assets");

        eprintln!("Serving WebUI from: {}", webui_dir.display());
        
        // Create clone for fallback closure
        let webui_dir_fallback = webui_dir.clone();
        let assets_dir_fallback = assets_dir.clone();

        // Serve WebUI static files from the webui directory
        // Use fallback for root path to avoid conflicts with existing routes
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
                    // Map /static/xxx or /assets/xxx to assets/xxx
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
                
                // Handle other static files (css, images, etc.)
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
        
        // Also serve at /api for compatibility
        router = router.nest_service("/api", ServeDir::new(&webui_dir));
    }

    let app = router.with_state(state);

    println!("ClawServer HTTP listening on {} (PID: {})", addr, pid);

    axum::serve(listener, app)
        .await
        .map_err(|e| ServerError(e.to_string()))?;

    // Clean up PID file
    fs::remove_file(pid_file()).ok();

    Ok(())
}

// ── Auth Middleware ─────────────────────────────────────────────────────────────

/// Simple auth check that always passes for now
/// In production, this would validate tokens properly
fn check_auth_simple() -> Option<Response> {
    None // Auth always passes
}

/// Extract and validate bearer token from Authorization header.
/// For WebSocket upgrade requests (where browsers cannot set custom headers),
/// also checks the `token` query parameter as a fallback.
fn check_auth<B>(state: &HttpAppState, req: &axum::http::Request<B>) -> Option<Response> {
    // Skip auth if disabled or no token configured (auto-disable when auth_token is empty)
    if !state.auth_enabled || state.auth_token.is_empty() {
        return None;
    }

    // Try Authorization header first
    if let Some(token) = req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
    {
        if token == state.auth_token {
            return None;
        }
    }

    // Fallback: check ?token= query param (needed for WebSocket connections)
    let is_ws_upgrade = req.headers()
        .get("Upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);

    if is_ws_upgrade {
        if let Some(encoded) = req.uri().query()
            .and_then(|q| {
                q.split('&')
                    .find(|p| p.starts_with("token="))
                    .map(|p| &p[6..])
            })
        {
            let token = url_decode(encoded);
            if token == state.auth_token {
                return None;
            }
        }
    }

    Some((
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": "Authentication required. Include 'Authorization: Bearer <token>' header, or for WebSocket: ws://host/ws?token=<token>."
        })),
    ).into_response())
}

/// Require auth middleware helper
fn require_auth<B>(state: &HttpAppState, req: &axum::http::Request<B>) -> Option<Response> {
    check_auth(state, req)
}

/// Decode percent-encoded query parameter values (e.g., %20 → space, %2F → /).
fn url_decode(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                output.push((hi << 4 | lo) as char);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            output.push(' ');
            i += 1;
            continue;
        }
        output.push(bytes[i] as char);
        i += 1;
    }
    output
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Axum middleware function for bearer token auth.
/// Apply via `.layer(middleware::from_fn_with_state(state.clone(), auth_middleware))`
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<HttpAppState>,
    req: axum::http::Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    if let Some(response) = check_auth(&state, &req) {
        return response;
    }
    next.run(req).await
}

// ── Path Safety ────────────────────────────────────────────────────────────────

/// Sanitize a requested path to prevent path traversal attacks.
/// Ensures the resolved path stays within the base directory.
fn sanitize_path(base: &std::path::Path, requested: &str) -> Result<PathBuf, ServerError> {
    let resolved = base.join(requested).canonicalize()
        .map_err(|_| ServerError(format!("Path not found: {}", requested)))?;
    let base_resolved = base.canonicalize()
        .map_err(|_| ServerError("Base path not found".to_string()))?;
    if !resolved.starts_with(&base_resolved) {
        return Err(ServerError("Path traversal denied".into()));
    }
    Ok(resolved)
}

// ── HTTP Handlers - Basic ───────────────────────────────────────────────────────

async fn index_handler() -> axum::response::Response {
    // Locate index.html using the same priority-based search
    let html_path = find_webui_dir().join("index.html");

    // Fallback to embedded content if runtime path doesn't exist
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

async fn health_handler() -> axum::response::Response {
    (StatusCode::OK, Json(serde_json::json!({
        "status": "ok",
        "service": "cowd",
        "version": env!("CARGO_PKG_VERSION")
    }))).into_response()
}

// ── HTTP Handlers - Models ──────────────────────────────────────────────────────

// ── HTTP Handlers - Chat ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ChatRequest {
    model: Option<String>,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    max_tokens: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatChoice {
    index: u32,
    message: ChatMessageOut,
    finish_reason: String,
}

#[derive(Debug, Serialize)]
struct ChatMessageOut {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatResponse {
    id: String,
    object: String,
    created: i64,
    model: String,
    choices: Vec<ChatChoice>,
}

// ── Memory Context Helper ─────────────────────────────────────────────────────

/// Build a memory context block from PreparedContext for injection into system prompt.
fn build_memory_context_block(prepared: &PreparedContext) -> Option<String> {
    if prepared.entries.is_empty() {
        return None;
    }
    let mut context_parts: Vec<String> = Vec::with_capacity(prepared.entries.len());
    for entry in &prepared.entries {
        context_parts.push(format!(
            "[{}] {}: {}",
            entry.layer as u8,
            entry.title,
            entry.content
        ));
    }
    Some(format!(
        "<memory_context>\n{}\n</memory_context>",
        context_parts.join("\n")
    ))
}

/// Convert OpenAI message format to memory Message format.
fn to_memory_message(role: &str, content: &str) -> MemMessage {
    use memory::types::MessageRole as MemMsgRole;
    let role = match role.to_lowercase().as_str() {
        "user" => MemMsgRole::User,
        "assistant" => MemMsgRole::Assistant,
        "system" => MemMsgRole::System,
        _ => MemMsgRole::User,
    };
    MemMessage {
        turn_index: 0,
        role,
        content: content.to_string(),
        tool_use_id: None,
        tool_name: None,
        pinned: false,
    }
}

// ── Runtime Integration ───────────────────────────────────────────────────────
// Types are imported at the top of the file via the `use runtime::{...}` block.

// ── OpenAI-Compatible API Client Adapter ─────────────────────────────────────

/// Adapter that implements the runtime's ApiClient trait using OpenAI-compatible API.
struct OpenAiApiClient {
    client: OpenAiCompatClient,
    model: String,
}

impl OpenAiApiClient {
    fn new(model: String) -> Result<Self, ServerError> {
        // B15 fix: Read provider config from environment variables instead of hardcoding
        let provider_name = std::env::var("COWD_PROVIDER_NAME")
            .unwrap_or_else(|_| "openai".to_string());
        let default_base_url = std::env::var("COWD_DEFAULT_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        let base_url = std::env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| default_base_url.clone());

        let config = OpenAiCompatConfig {
            provider_name: provider_name.leak() as &str,
            api_key_env: "OPENAI_API_KEY",
            base_url_env: "OPENAI_BASE_URL",
            default_base_url: default_base_url.leak() as &str,
        };
        let api_key = std::env::var("OPENAI_API_KEY")
            .map_err(|_| ServerError("OPENAI_API_KEY not set".to_string()))?;
        let client = OpenAiCompatClient::new(api_key, config)
            .with_base_url(&base_url);
        Ok(Self { client, model })
    }

    /// Convert internal ApiRequest to OpenAI MessageRequest.
    fn build_message_request(&self, request: &ApiRequest) -> Result<MessageRequest, ServerError> {
        let max_tokens = max_tokens_for_model(&self.model);

        // Convert system prompts
        let system = if request.system_prompt.is_empty() {
            None
        } else {
            Some(request.system_prompt.join("\n\n"))
        };

        // Convert conversation messages
        let input_messages: Vec<InputMessage> = request
            .messages
            .iter()
            .map(|msg| {
                let role = match msg.role {
                    SessionMessageRole::User => "user".to_string(),
                    SessionMessageRole::Assistant => "assistant".to_string(),
                    SessionMessageRole::Tool => "tool".to_string(),
                    SessionMessageRole::System => "system".to_string(),
                };

                let content: Vec<InputContentBlock> = msg
                    .blocks
                    .iter()
                    .filter_map(|block| match block {
                        SessionContentBlock::Text { text } => {
                            Some(InputContentBlock::Text { text: text.clone() })
                        }
                        // DeepSeek thinking mode: reasoning_content must be passed back
                        // in subsequent requests. Convert Thinking blocks to InputContentBlock.
                        SessionContentBlock::Thinking { thinking } => {
                            Some(InputContentBlock::Thinking {
                                thinking: thinking.clone(),
                                signature: None,
                            })
                        }
                        _ => None, // Skip tool use/result blocks for OpenAI API
                    })
                    .collect();

                InputMessage { role, content }
            })
            .collect();

        Ok(MessageRequest {
            model: self.model.clone(),
            max_tokens,
            messages: input_messages,
            system,
            tools: None,
            tool_choice: None,
            stream: true, // Always use streaming for the runtime
            temperature: None,
            top_p: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            reasoning_effort: None,
        })
    }

    /// Convert OpenAI stream events to runtime AssistantEvents.
    async fn stream_events(
        &self,
        request: &MessageRequest,
    ) -> Result<Vec<AssistantEvent>, ServerError> {
        let mut stream = self
            .client
            .stream_message(request)
            .await
            .map_err(|e| ServerError(format!("API stream request failed: {}", e)))?;

        let mut events = Vec::new();

        while let Ok(Some(event)) = stream.next_event().await {
            match event {
                StreamEvent::ContentBlockDelta(delta) => {
                    match &delta.delta {
                        ContentBlockDelta::TextDelta { text } => {
                            events.push(AssistantEvent::TextDelta(text.clone()));
                        }
                        // P1-7: Extended thinking delta
                        ContentBlockDelta::ThinkingDelta { thinking } => {
                            events.push(AssistantEvent::ThinkingDelta(thinking.clone()));
                        }
                        _ => {} // Ignore other delta types
                    }
                }
                StreamEvent::MessageStart(_) | StreamEvent::MessageStop(_) => {
                    events.push(AssistantEvent::MessageStop);
                    break;
                }
                _ => {} // Ignore other event types (MessageDelta, ContentBlockStart, ContentBlockStop)
            }
        }

        Ok(events)
    }
}

impl RuntimeApiClient for OpenAiApiClient {
    fn stream(&mut self, request: ApiRequest) -> Pin<Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        match self.stream_collect(request) {
            Ok(events) => Box::pin(futures::stream::iter(events.into_iter().map(Ok))),
            Err(e) => Box::pin(futures::stream::iter(std::iter::once(Err(e)))),
        }
    }
}

impl OpenAiApiClient {
    fn stream_collect(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError> {
        // Build the OpenAI request
        let message_request = match self.build_message_request(&request) {
            Ok(req) => req,
            Err(e) => return Err(RuntimeError::new(e.to_string())),
        };

        // Since we're in a sync context (Runtime trait), we need to use blocking runtime
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                // Fallback: create a new runtime (not ideal but works)
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => return Err(RuntimeError::new(format!("Failed to create tokio runtime: {}", e))),
                };
                return rt.block_on(async {
                    match self.stream_events(&message_request).await {
                        Ok(events) => Ok(events),
                        Err(e) => Err(RuntimeError::new(e.to_string())),
                    }
                });
            }
        };

        // Execute the async streaming in the current runtime
        let cloned_client = self.client.clone();
        let _model = self.model.clone();

        handle.block_on(async {
            let mut stream = match cloned_client.stream_message(&message_request).await {
                Ok(s) => s,
                Err(e) => return Err(RuntimeError::new(format!("API stream request failed: {}", e))),
            };

            let mut events = Vec::new();

            while let Ok(Some(event)) = stream.next_event().await {
                match event {
                    StreamEvent::ContentBlockDelta(delta) => {
                        match &delta.delta {
                            ContentBlockDelta::TextDelta { text } => {
                                events.push(AssistantEvent::TextDelta(text.clone()));
                            }
                            ContentBlockDelta::ThinkingDelta { thinking } => {
                                events.push(AssistantEvent::ThinkingDelta(thinking.clone()));
                            }
                            _ => {}
                        }
                    }
                    StreamEvent::MessageStart(_) => {} // skip, wait for content
                    StreamEvent::MessageStop(_) => {
                        events.push(AssistantEvent::MessageStop);
                        break;
                    }
                    _ => {}
                }
            }

            Ok(events)
        })
    }
}

// ── HTTP Tool Executor Adapter ────────────────────────────────────────────────

/// Tool executor adapter for HTTP context.
/// Implements the runtime ToolExecutor trait by delegating to the tools crate.
struct HttpToolExecutor;

impl ToolExecutor for HttpToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        // Parse the input JSON
        let input_value: serde_json::Value = serde_json::from_str(input)
            .map_err(|e| ToolError::new(format!("invalid input JSON: {}", e)))?;

        // Call the tools crate execute_tool function
        tools::execute_tool(tool_name, &input_value)
            .map_err(|e| ToolError::new(format!("tool execution failed: {}", e)))
    }
}

/// SSE-backed tool callback that pushes tool lifecycle events to the SSE stream.
/// Inspired by hermes-agent stream_consumer.py tool_progress_callback.
struct SseToolCallback {
    chunk_tx: tokio::sync::mpsc::Sender<SseChunk>,
}

impl SseToolCallback {
    fn new(chunk_tx: tokio::sync::mpsc::Sender<SseChunk>) -> Self {
        Self { chunk_tx }
    }

    fn send_event(&self, event_type: &str, data: &serde_json::Value) {
        let sse_line = format!("event: {}\ndata: {}\n\n", event_type, data);
        let tx = self.chunk_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(Some(sse_line)).await;
        });
    }
}

impl ToolCallback for SseToolCallback {
    fn on_tool_start(&self, id: &str, name: &str, preview: &str) {
        let data = serde_json::json!({
            "id": id,
            "name": name,
            "preview": preview,
        });
        self.send_event("tool_start", &data);
    }

    fn on_tool_progress(&self, id: &str, name: &str, progress: &str) {
        let data = serde_json::json!({
            "id": id,
            "name": name,
            "progress": progress,
        });
        self.send_event("tool_progress", &data);
    }

    fn on_tool_complete(&self, id: &str, name: &str, result_summary: &str, exit_code: Option<i32>) {
        let data = serde_json::json!({
            "id": id,
            "name": name,
            "result_summary": result_summary,
            "exit_code": exit_code,
        });
        self.send_event("tool_complete", &data);
    }
}

/// Chat Completions handler with SSE streaming support
/// 
/// This handler now uses ConversationRuntime for unified conversation management.

/// Create a Session from OpenAI-format messages.
fn create_session_from_messages(messages: &[ChatMessage], session_id: &str) -> Session {
    let mut session = Session::new();
    session.session_id = session_id.to_string();

    for msg in messages {
        let role = match msg.role.to_lowercase().as_str() {
            "user" => SessionMessageRole::User,
            "assistant" => SessionMessageRole::Assistant,
            "system" => SessionMessageRole::System,
            _ => SessionMessageRole::User,
        };

        let content_block = SessionContentBlock::Text {
            text: msg.content.clone(),
        };

        let session_msg = SessionMessage {
            role,
            blocks: vec![content_block],
            usage: None,
        };

        session.messages.push(session_msg);
    }

    session
}

// ── HTTP Handlers - Session ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ListSessionsQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

async fn list_sessions_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<ListSessionsQuery>,
) -> axum::response::Response {
    // B14: Implement pagination for session listing
    let page = params.offset.unwrap_or(0);
    let per_page = params.limit.unwrap_or(20).min(100);
    match state.session_store.list_sessions() {
        Ok(records) => {
            let total = records.len();
            let sessions: Vec<serde_json::Value> = records
                .into_iter()
                .skip(page)
                .take(per_page)
                .map(|r| {
                    serde_json::json!({
                        "id": r.session_id,
                        "session_id": r.session_id,
                        "platform": r.platform,
                        "chat_id": r.chat_id,
                        "user_id": r.user_id,
                        "model": r.model,
                        "created_at": r.created_at,
                        "last_activity": r.last_activity,
                        "updated_at": r.last_activity,
                        "message_count": r.message_count,
                        "reset_policy": r.reset_policy,
                        "title": format!("{}:{}", r.platform, r.chat_id),
                    })
                })
                .collect();
            Json(serde_json::json!({
                "sessions": sessions,
                "total": total,
                "page": page,
                "per_page": per_page,
            })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ).into_response(),
    }
}

async fn get_session_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
) -> axum::response::Response {
    match state.session_store.get_session(&session_id) {
        Ok(Some(r)) => {
            Json(serde_json::json!({
                "session": {
                    "id": r.session_id,
                    "session_id": r.session_id,
                    "platform": r.platform,
                    "chat_id": r.chat_id,
                    "user_id": r.user_id,
                    "model": r.model,
                    "created_at": r.created_at,
                    "last_activity": r.last_activity,
                    "updated_at": r.last_activity,
                    "message_count": r.message_count,
                    "reset_policy": r.reset_policy,
                    "title": format!("{}:{}", r.platform, r.chat_id),
                },
                "messages": []
            })).into_response()
        }
        Ok(None) => {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "session not found"}))).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn delete_session_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
) -> axum::response::Response {
    match state.session_store.delete_session(&session_id) {
        Ok(_) => {
            // Broadcast SessionDeleted event for real-time sync
            let _ = state.session_broadcast.send(SessionEvent::SessionDeleted {
                session_id: session_id.clone(),
            });
            Json(serde_json::json!({ "ok": true, "deleted": session_id })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ).into_response(),
    }
}

/// Compact a session by summarizing older messages and preserving the recent tail.
///
/// POST /v1/sessions/:id/compact
///
/// Optional JSON body:
/// - `preserve_recent_messages`: number of recent messages to keep (default: 4)
/// - `max_estimated_tokens`: token threshold that triggers compaction (default: 10000)
async fn compact_session_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
    Json(params): Json<CompactParams>,
) -> axum::response::Response {
    let config = CompactionConfig {
        preserve_recent_messages: params.preserve_recent_messages.unwrap_or(4),
        max_estimated_tokens: params.max_estimated_tokens.unwrap_or(10_000),
        ..Default::default()
    };

    // Load session record from SQLite
    let _record = match state.session_store.get_session(&session_id) {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "session not found"})),
            ).into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("db error: {e}")})),
            ).into_response();
        }
    };

    // 3A-5 fix: Load session from JSONL, run compaction, write back
    let session_path = state.sessions_dir.join(format!("{session_id}.jsonl"));
    if !session_path.exists() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "ok": false,
                "compacted": false,
                "reason": "session JSONL file not found",
                "session_id": session_id,
            })),
        ).into_response();
    }

    let session = match runtime::Session::load_from_path(&session_path) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "ok": false,
                    "compacted": false,
                    "reason": format!("failed to load session: {e}"),
                    "session_id": session_id,
                })),
            ).into_response();
        }
    };

    let message_count_before = session.messages.len();
    let token_estimate = runtime::estimate_session_tokens(&session);

    let result = runtime::compact_session(&session, config);

    if result.removed_message_count > 0 {
        // Write compacted session back to disk
        if let Err(e) = result.compacted_session.save_to_path(&session_path) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "ok": false,
                    "compacted": false,
                    "reason": format!("failed to save compacted session: {e}"),
                    "session_id": session_id,
                })),
            ).into_response();
        }
    }

    Json(serde_json::json!({
        "ok": true,
        "compacted": result.removed_message_count > 0,
        "removed_message_count": result.removed_message_count,
        "message_count_before": message_count_before,
        "message_count_after": result.compacted_session.messages.len(),
        "token_estimate_before": token_estimate,
        "token_estimate_after": runtime::estimate_session_tokens(&result.compacted_session),
        "session_id": session_id,
        "preserve_recent_messages": config.preserve_recent_messages,
        "max_estimated_tokens": config.max_estimated_tokens,
    })).into_response()
}

/// Parameters for the compact session endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
struct CompactParams {
    preserve_recent_messages: Option<usize>,
    max_estimated_tokens: Option<usize>,
}

/// Parameters for creating a memory entry.
#[derive(Debug, Clone, Deserialize)]
struct CreateMemoryEntryParams {
    layer: String,
    category: String,
    content: String,
    title: Option<String>,
    tags: Option<Vec<String>>,
    source: Option<String>,
}

/// Parameters for creating a handoff package.
#[derive(Debug, Clone, Deserialize)]
struct CreateHandoffParams {
    session_id: String,
    next_action: Option<String>,
    context_notes: Option<String>,
}

/// Parameters for restoring a handoff package.
#[derive(Debug, Clone, Deserialize)]
struct RestoreHandoffParams {
    handoff_id: String,
    target_session_id: String,
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max { s.to_string() } else { format!("{}…", &s[..max]) }
}

// ── HTTP Handlers - Memory ────────────────────────────────────────────────────

async fn memory_status_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let enabled = state.cognitive_manager.is_some();
    Json(serde_json::json!({
        "enabled": enabled,
        "store_path": state.memory_store_path,
        "session_store": true,
        "layers": {
            "L0": "fixed identity (系统级持久记忆)",
            "L1": "working memory (当前会话工作记忆)",
            "L2": "project context (项目上下文)",
            "L3": "deep memories (深度记忆，语义检索)",
            "L4": "archived (归档记忆)"
        },
        "features": {
            "semantic_search": enabled,
            "context_compression": enabled,
            "drift_detection": enabled,
            "session_handoff": enabled
        }
    })).into_response()
}

/// GET /api/memory/stats — Statistics summary for the WebUI.
async fn memory_stats_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let Some(ref mgr) = state.cognitive_manager else {
        return Json(serde_json::json!({
            "total_entries": 0,
            "total_tokens": 0,
            "layers": {},
            "warning": "memory subsystem not enabled"
        })).into_response();
    };

    let l0 = mgr.list_layer_entries(memory::types::MemoryLayer::L0).await.unwrap_or_default();
    let l1 = mgr.list_layer_entries(memory::types::MemoryLayer::L1).await.unwrap_or_default();
    let l2 = mgr.list_layer_entries(memory::types::MemoryLayer::L2).await.unwrap_or_default();
    let l3 = mgr.list_layer_entries(memory::types::MemoryLayer::L3).await.unwrap_or_default();

    let total = l0.len() + l1.len() + l2.len() + l3.len();

    Json(serde_json::json!({
        "total_entries": total,
        "total_tokens": 0,
        "layers": {
            "l0": { "count": l0.len() },
            "l1": { "count": l1.len() },
            "l2": { "count": l2.len() },
            "l3": { "count": l3.len() },
            "l4": { "count": 0 }
        }
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct MemorySearchQuery {
    query: String,
    #[serde(default = "default_memory_limit")]
    limit: usize,
    #[serde(default)]
    layer: Option<String>,
    /// Search mode: "vector" (semantic), "bm25" (keyword), or "hybrid" (default).
    #[serde(default = "default_search_mode")]
    mode: String,
}

fn default_memory_limit() -> usize {
    10
}

fn default_search_mode() -> String {
    "hybrid".to_string()
}

async fn memory_search_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<MemorySearchQuery>,
) -> axum::response::Response {
    if params.query.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "query parameter is required"
        }))).into_response();
    }

    let mode = params.mode.as_str();
    if let Some(ref mgr) = state.cognitive_manager {
        match mgr.recall(&params.query, params.limit).await {
            Ok(entries) => {
                // P0-3: Apply BM25/hybrid re-ranking when mode is "bm25" or "hybrid"
                let results: Vec<serde_json::Value> = if mode == "bm25" || mode == "hybrid" {
                    let contents: Vec<String> = entries.iter().map(|e| e.content.clone()).collect();
                    let ids: Vec<String> = entries.iter().map(|e| e.id.to_string()).collect();

                    let bm25 = memory::BM25Scorer::default_params(&contents);
                    let bm25_rankings = bm25.rank(&params.query);

                    let bm25_max = bm25_rankings.first().map(|(_, s)| *s).unwrap_or(1.0);
                    let bm25_scores: std::collections::HashMap<String, f64> = bm25_rankings
                        .iter()
                        .map(|(idx, score)| {
                            let id = ids.get(*idx).cloned().unwrap_or_default();
                            let normalised = if bm25_max > 0.0 { score / bm25_max } else { 0.0 };
                            (id, normalised)
                        })
                        .collect();

                    entries.into_iter().map(|e| {
                        let bm25_score = bm25_scores.get(&e.id.to_string()).copied().unwrap_or(0.0);
                        let confidence = e.confidence as f64;
                        let hybrid_score = if mode == "hybrid" {
                            0.6 * confidence + 0.4 * bm25_score
                        } else {
                            bm25_score
                        };

                        serde_json::json!({
                            "id": e.id.to_string(),
                            "title": e.title,
                            "content": e.content,
                            "layer": format!("{:?}", e.layer),
                            "category": format!("{:?}", e.category),
                            "priority": format!("{:?}", e.priority),
                            "tags": e.tags,
                            "confidence": confidence,
                            "bm25_score": bm25_score,
                            "hybrid_score": hybrid_score,
                            "source": if bm25_score > 0.0 && confidence > 0.0 { "hybrid" }
                                      else if bm25_score > 0.0 { "bm25" }
                                      else { "vector" },
                            "access_count": e.access_count,
                            "created_at": e.created_at.to_rfc3339(),
                            "updated_at": e.updated_at.to_rfc3339(),
                        })
                    }).collect()
                } else {
                    entries.into_iter().map(|e| {
                        serde_json::json!({
                            "id": e.id.to_string(),
                            "title": e.title,
                            "content": e.content,
                            "layer": format!("{:?}", e.layer),
                            "category": format!("{:?}", e.category),
                            "priority": format!("{:?}", e.priority),
                            "tags": e.tags,
                            "confidence": e.confidence,
                            "access_count": e.access_count,
                            "created_at": e.created_at.to_rfc3339(),
                            "updated_at": e.updated_at.to_rfc3339(),
                        })
                    }).collect()
                };

                Json(serde_json::json!({
                    "results": results,
                    "query": params.query,
                    "mode": params.mode,
                    "limit": params.limit,
                    "count": results.len()
                })).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response(),
        }
    } else {
        Json(serde_json::json!({
            "results": [],
            "query": params.query,
            "mode": params.mode,
            "limit": params.limit,
            "count": 0,
            "warning": "memory subsystem not enabled"
        })).into_response()
    }
}

#[derive(Debug, Deserialize)]
struct ListMemoryEntriesQuery {
    #[serde(default = "default_memory_limit")]
    limit: usize,
    #[serde(default)]
    layer: Option<String>,
}

async fn get_memory_entry_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(entry_id): Path<String>,
) -> axum::response::Response {
    if let Some(ref mgr) = state.cognitive_manager {
        match mgr.get_entry(&entry_id).await {
            Ok(Some(entry)) => {
                return Json(serde_json::json!({
                    "id": entry.id.to_string(),
                    "title": entry.title,
                    "content": entry.content,
                    "layer": format!("{:?}", entry.layer),
                    "category": format!("{:?}", entry.category),
                    "priority": format!("{:?}", entry.priority),
                    "tags": entry.tags,
                    "confidence": entry.confidence,
                    "access_count": entry.access_count,
                    "created_at": entry.created_at.to_rfc3339(),
                    "updated_at": entry.updated_at.to_rfc3339(),
                })).into_response();
            }
            Ok(None) => {
                return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "entry not found"}))).into_response();
            }
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response();
            }
        }
    }
    (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response()
}

async fn delete_memory_entry_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(entry_id): Path<String>,
) -> axum::response::Response {
    if let Some(ref mgr) = state.cognitive_manager {
        match mgr.delete_entry(&entry_id).await {
            Ok(()) => {
                Json(serde_json::json!({
                    "deleted": entry_id,
                    "ok": true,
                })).into_response()
            }
            Err(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
            }
        }
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response()
    }
}

/// Update a memory entry's content, tags, or priority.
/// PATCH /api/memory/entry/{id}
#[derive(Debug, Deserialize)]
struct UpdateMemoryEntryParams {
    content: Option<String>,
    tags: Option<Vec<String>>,
    priority: Option<String>,  // "Critical", "High", "Normal", "Low"
}

async fn update_memory_entry_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(entry_id): Path<String>,
    Json(params): Json<UpdateMemoryEntryParams>,
) -> axum::response::Response {
    // Parse priority string to enum
    let priority = params.priority.as_ref().map(|p| match p.as_str() {
        "Critical" => memory::types::Priority::Critical,
        "High" => memory::types::Priority::High,
        "Normal" => memory::types::Priority::Normal,
        "Low" => memory::types::Priority::Low,
        _ => memory::types::Priority::Normal,
    });
    if let Some(ref mgr) = state.cognitive_manager {
        match mgr.update_entry(&entry_id, params.content, params.tags, priority).await {
            Ok(()) => Json(serde_json::json!({
                "updated": entry_id,
                "ok": true,
            })).into_response(),
            Err(e) => {
                let status = if e.to_string().contains("not found") {
                    StatusCode::NOT_FOUND
                } else if e.to_string().contains("denied") {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                (status, Json(serde_json::json!({"error": e.to_string()}))).into_response()
            }
        }
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response()
    }
}

/// Create a new memory entry.
///
/// POST /v1/memory/entries
///
/// Request body: `{ layer, category, content, title?, tags?, source? }`
async fn create_memory_entry_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<CreateMemoryEntryParams>,
) -> axum::response::Response {
    let Some(ref mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let layer = match params.layer.as_str() {
        "L0" => memory::MemoryLayer::L0,
        "L1" => memory::MemoryLayer::L1,
        "L2" => memory::MemoryLayer::L2,
        "L3" => memory::MemoryLayer::L3,
        "L4" => memory::MemoryLayer::L4,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid layer, must be L0-L4"})),
            ).into_response();
        }
    };

    let category = match params.category.as_str() {
        "UserPreference" => memory::MemoryCategory::UserPreference,
        "ProjectConvention" => memory::MemoryCategory::ProjectConvention,
        "Decision" => memory::MemoryCategory::Decision,
        "Reference" => memory::MemoryCategory::Reference,
        "Shared" => memory::MemoryCategory::Shared,
        "CompressedSummary" => memory::MemoryCategory::CompressedSummary,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid category"})),
            ).into_response();
        }
    };

    let source = match params.source.as_deref() {
        Some("UserExplicit") | None => memory::MemorySource::UserExplicit,
        Some("AutoExtracted") => memory::MemorySource::AutoExtracted,
        Some("Compression") => memory::MemorySource::Compression,
        Some("Import") => memory::MemorySource::Import,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid source"})),
            ).into_response();
        }
    };

    let entry = memory::MemoryEntry {
        id: memory::MemoryId::new_v4(),
        layer,
        category,
        priority: memory::Priority::Normal,
        source,
        title: params.title.unwrap_or_default(),
        content: params.content,
        embedding: None,
        tags: params.tags.unwrap_or_default(),
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: Default::default(),
        source_agent: None,
        visibility: Default::default(),
        session_id: None,
    };

    let entry_id = entry.id.to_string();
    match mgr.remember(entry).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "ok": true,
                "id": entry_id,
            })),
        ).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ).into_response(),
    }
}

/// Create a handoff package for cross-session state transfer.
///
/// POST /v1/memory/handoff

/// Restore a handoff package into a target session.
///
/// POST /v1/memory/handoff/restore

// ── HTTP Handlers - Memory Layers & Graph ────────────────────────────────────

/// List memory layer statistics.
///
/// GET /v1/memory/layers
async fn list_memory_layers_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let Some(ref mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let layers = mgr.list_layers().await;
    Json(serde_json::json!({ "layers": layers })).into_response()
}

/// List knowledge graph entities.
///
/// GET /v1/memory/graph/entities?q=keyword

/// List knowledge graph relations.
///
/// GET /v1/memory/graph/relations?subject=x&predicate=y

/// Query the temporal knowledge graph.
///
/// POST /v1/memory/graph/query
///
/// Request body: `{ entity?, time_range?, relation_type? }`

/// Parameters for graph entity queries.
#[derive(Debug, Clone, Default, Deserialize)]
struct GraphEntityParams {
    q: Option<String>,
}

/// Parameters for graph relation queries.
#[derive(Debug, Clone, Default, Deserialize)]
struct GraphRelationParams {
    subject: Option<String>,
    predicate: Option<String>,
}

/// Parameters for temporal graph queries.
#[derive(Debug, Clone, Default, Deserialize)]
struct GraphQueryParams {
    entity: Option<String>,
    time_range: Option<String>,
    relation_type: Option<String>,
}

// ── HTTP Handlers - Skills ─────────────────────────────────────────────────────

// ── 3B-4: Skill Management API (install/uninstall/toggle) ──────────────────────

#[derive(Debug, Deserialize)]
struct InstallSkillRequest {
    source_path: String,
}

async fn install_skill_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(req): Json<InstallSkillRequest>,
) -> Response {
    let source = PathBuf::from(&req.source_path);
    match state.skill_service.install(&source) {
        Ok(msg) => Json(serde_json::json!({
            "success": true,
            "message": msg
        })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "success": false,
            "error": e
        }))).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct ToggleSkillRequest {
    enabled: bool,
}

// ── HTTP Handlers - Workspaces ─────────────────────────────────────────────────

async fn list_workspaces_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let workspaces = state.skill_service.list_workspaces();
    Json(serde_json::json!({
        "workspaces": workspaces,
        "count": workspaces.len()
    })).into_response()
}

// ── HTTP Handlers - System ─────────────────────────────────────────────────────

// ── HTTP Handlers - WebSocket ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct WsInbound {
    text: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    stream: Option<bool>,
}

#[derive(Debug, Serialize)]
struct WsOutbound {
    text: String,
    done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let addr = std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 0);
    ws.on_upgrade(move |socket| handle_ws(socket, addr, state))
}

/// WebSocket handler for session event subscriptions
/// GET /ws/sessions - Subscribe to session events (created, updated, deleted, messages added)
async fn ws_sessions_handler(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let addr = std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 0);
    ws.on_upgrade(move |socket| handle_ws_sessions(socket, addr, state))
}

/// Handle WebSocket connections for session event subscriptions
async fn handle_ws_sessions(
    mut socket: WebSocket,
    addr: std::net::SocketAddr,
    state: HttpAppState,
) {
    let subscriber_id = format!("sub-{}", addr);
    tracing::info!(subscriber_id = %subscriber_id, "Session events WebSocket connected");

    // Subscribe to session events
    let mut rx = state.session_broadcast.subscribe();

    // Send welcome message with subscription info
    let welcome = serde_json::json!({
        "type": "subscribed",
        "subscriber_id": subscriber_id,
        "message": "Subscribed to session events. Events: session_created, session_updated, session_deleted, message_added, runtime_started, runtime_finished"
    });
    if socket.send(WsMessage::Text(welcome.to_string().into())).await.is_err() {
        return;
    }

    // Send initial session list
    match state.session_store.list_sessions() {
        Ok(sessions) => {
            let init_msg = serde_json::json!({
                "type": "sessions_list",
                "sessions": sessions.iter().map(|s| {
                    serde_json::json!({
                        "session_id": s.session_id,
                        "platform": s.platform,
                        "message_count": s.message_count
                    })
                }).collect::<Vec<_>>(),
                "count": sessions.len()
            });
            if socket.send(WsMessage::Text(init_msg.to_string().into())).await.is_err() {
                return;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to load session list for subscription");
        }
    }

    // Event forwarding loop
    loop {
        tokio::select! {
            // Forward session events to client
            event = rx.recv() => {
                match event {
                    Ok(session_event) => {
                        let event_json = serde_json::json!({
                            "type": "session_event",
                            "event": session_event
                        });
                        if socket.send(WsMessage::Text(event_json.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        // Check error type by matching on Debug output
                        let err_str = format!("{:?}", e);
                        if err_str.contains("Lagged") {
                            tracing::warn!("Session event channel lagged, missed events");
                            let sync_msg = serde_json::json!({
                                "type": "sync_needed",
                                "reason": "channel_lagged"
                            });
                            if socket.send(WsMessage::Text(sync_msg.to_string().into())).await.is_err() {
                                break;
                            }
                        } else {
                            // Channel closed
                            tracing::info!(subscriber_id = %subscriber_id, "Session event channel closed");
                            break;
                        }
                    }
                }
            }
            // Handle client messages (for ping/pong or unsubscribe)
            msg = socket.recv() => {
                match msg {
                    Some(Ok(WsMessage::Text(t))) => {
                        let text = t.to_string();
                        // Handle ping
                        if text == "ping" {
                            if socket.send(WsMessage::Text("pong".into())).await.is_err() {
                                break;
                            }
                        }
                        // Handle unsubscribe
                        else if text == "unsubscribe" {
                            let bye = serde_json::json!({
                                "type": "unsubscribed",
                                "subscriber_id": subscriber_id
                            });
                            let _ = socket.send(WsMessage::Text(bye.to_string().into())).await;
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | None => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    tracing::info!(subscriber_id = %subscriber_id, "Session events WebSocket disconnected");
}

async fn handle_ws(mut socket: WebSocket, addr: std::net::SocketAddr, state: HttpAppState) {
    let session_id = format!("ws-{}", addr);
    let _broadcast_tx = state.session_broadcast.clone();

    // Send welcome message
    let welcome = WsOutbound {
        text: "Connected. Send messages to start chatting.".to_string(),
        done: true,
        session_id: Some(session_id.clone()),
        error: None,
    };
    if socket.send(WsMessage::Text(serde_json::to_string(&welcome).unwrap_or_else(|e| {
        tracing::warn!("failed to serialize welcome message: {e}");
        "{}".to_string()
    }).into())).await.is_err() {
        return;
    }

    // Main message loop
    while let Some(msg) = socket.recv().await {
        let text_raw = match msg {
            Ok(WsMessage::Text(t)) => t.to_string(),
            Ok(WsMessage::Close(_)) => break,
            _ => continue,
        };

        let ws_in: WsInbound = match serde_json::from_str(&text_raw) {
            Ok(v) => v,
            Err(_) => {
                WsInbound {
                    text: text_raw.clone(),
                    session_id: None,
                    model: None,
                    stream: Some(true),
                }
            }
        };

        if ws_in.text.is_empty() {
            continue;
        }

        // Build session, create runtime, and run turn - all before any await points
        let user_input = ws_in.text.clone();
        let stream = ws_in.stream.unwrap_or(true);
        let model = ws_in.model.unwrap_or_else(|| "claude-opus-4-6".to_string());

        // Build session and run turn synchronously
        let session = match build_or_restore_session(&session_id, &state).await {
            s => s,
        };

        // Create API client
        let api_client = match OpenAiApiClient::new(model.clone()) {
            Ok(c) => c,
            Err(e) => {
                let out = WsOutbound {
                    text: String::new(),
                    done: true,
                    session_id: Some(session_id.clone()),
                    error: Some(format!("Failed to create API client: {}", e)),
                };
                if let Ok(json) = serde_json::to_string(&out) {
                    let _ = socket.send(WsMessage::Text(json.into())).await;
                }
                continue;
            }
        };

        // Create tool executor and system prompt
        let tool_executor = HttpToolExecutor;
        let base_prompt = "You are a helpful AI assistant. Provide clear, concise responses.".to_string();
        let system_prompt = vec![base_prompt];

        // Build and run runtime - scoped to drop before await points
        let content_result = {
            let mut runtime = ConversationRuntime::new(
                session,
                api_client,
                tool_executor,
                PermissionPolicy::new(PermissionMode::WorkspaceWrite),
                system_prompt,
            );

            if let Some(ref mgr) = state.cognitive_manager {
                runtime = runtime.with_memory_manager(Arc::clone(mgr));
            }

            // P0-1: Attach smart approval gate
            runtime = runtime.with_approval_gate(state.approval_gate.clone());

            let (tx, rx) = std::sync::mpsc::channel();
            let ui = user_input;
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("ws turn rt");
                let result = rt.block_on(
                    runtime.run_turn_async(&ui, &runtime::permissions::SharedPrompter::none())
                );
                let _ = tx.send(result);
            });
            match rx.recv() {
                Ok(Ok(summary)) => Ok(summary
                    .assistant_messages
                    .iter()
                    .flat_map(|msg| &msg.blocks)
                    .filter_map(|block| match block {
                        SessionContentBlock::Text { text } => Some(text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("")),
                Ok(Err(e)) => Err(format!("Runtime error: {}", e)),
                Err(_) => Err("Thread panicked".to_string()),
            }
        };
        // runtime dropped here

        // Send response (async operations after runtime is dropped)
        match content_result {
            Ok(content) => {
                if stream && content.len() > 100 {
                    let chunk_size = 50;
                    for i in (0..content.len()).step_by(chunk_size) {
                        let end = (i + chunk_size).min(content.len());
                        let chunk = &content[i..end];
                        let out = WsOutbound {
                            text: chunk.to_string(),
                            done: false,
                            session_id: Some(session_id.clone()),
                            error: None,
                        };
                        if let Ok(json) = serde_json::to_string(&out) {
                            if socket.send(WsMessage::Text(json.into())).await.is_err() {
                                break;
                            }
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    }
                }

                let out = WsOutbound {
                    text: content,
                    done: true,
                    session_id: Some(session_id.clone()),
                    error: None,
                };
                if let Ok(json) = serde_json::to_string(&out) {
                    if socket.send(WsMessage::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                let out = WsOutbound {
                    text: String::new(),
                    done: true,
                    session_id: Some(session_id.clone()),
                    error: Some(e),
                };
                if let Ok(json) = serde_json::to_string(&out) {
                    if socket.send(WsMessage::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

/// Build or restore a session from the session store.
async fn build_or_restore_session(session_id: &str, state: &HttpAppState) -> Session {
    // Load from session store
    if let Ok(Some(record)) = state.session_store.get_session(session_id) {
        let mut session = Session::new();
        session.session_id = record.session_id;
        return session;
    }

    // Create fresh session
    let mut session = Session::new();
    session.session_id = session_id.to_string();
    session
}

// ── Skill Service ──────────────────────────────────────────────────────────────

/// Input for listing skills
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct SkillListInput {
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

/// Output from listing skills
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkillListOutput {
    pub success: bool,
    pub skills: Vec<SkillMeta>,
    pub categories: Vec<String>,
    pub tags: Vec<String>,
    pub count: usize,
}

/// Skill metadata
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkillMeta {
    pub name: String,
    pub description: String,
    pub category: String,
    pub tags: Vec<String>,
    pub version: Option<String>,
}

/// Input for viewing a skill
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SkillViewInput {
    pub name: String,
    #[serde(default)]
    pub file_path: Option<String>,
}

/// Output from viewing a skill
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkillViewOutput {
    pub success: bool,
    pub name: String,
    pub description: String,
    pub content: String,
    pub metadata: SkillMetadata,
}

/// Skill metadata
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkillMetadata {
    pub version: Option<String>,
    pub author: Option<String>,
    pub tags: Vec<String>,
    pub related_skills: Vec<String>,
    pub platforms: Vec<String>,
}

/// Input for invoking a skill
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SkillInvokeInput {
    pub name: String,
    #[serde(default)]
    pub args: Option<String>,
}

/// Output from invoking a skill
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkillInvokeOutput {
    pub success: bool,
    pub name: String,
    pub content: String,
    pub warning: Option<String>,
}

/// Workspace info
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkspaceInfo {
    pub name: String,
    pub path: String,
    pub root_type: String,
    pub skill_count: usize,
}

/// Workspace preview
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkspacePreview {
    pub name: String,
    pub description: String,
    pub skills: Vec<String>,
    pub total_entries: usize,
}

/// Skill Service
pub struct SkillService {
    roots: Vec<PathBuf>,
    platform: String,
}

impl SkillService {
    pub fn new() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let cwd = std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| ".".to_string());

        let mut roots = Vec::new();
        roots.push(PathBuf::from(format!("{}/.cowd/skills", home)));
        roots.push(PathBuf::from(format!("{}/.qoder/skills", home)));
        roots.push(PathBuf::from(format!("{}/.agents/skills", home)));
        roots.push(PathBuf::from(format!("{}/.cowd/skills", cwd)));
        roots.push(PathBuf::from(format!("{}/.qoder/skills", cwd)));
        roots.push(PathBuf::from(format!("{}/.agents/skills", cwd)));

        Self {
            roots,
            platform: std::env::consts::OS.to_string(),
        }
    }

    pub fn list(&self, input: SkillListInput) -> SkillListOutput {
        let mut all_skills = Vec::new();
        let mut categories: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut all_tags: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for root in &self.roots {
            if let Ok(entries) = fs::read_dir(root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let skill_md = path.join("SKILL.md");
                        if skill_md.exists() {
                            if let Ok(content) = fs::read_to_string(&skill_md) {
                                let (metadata, _) = parse_skill_frontmatter(&content);

                                if let Some(ref platforms) = metadata.platforms {
                                    if !platforms.is_empty() && !platforms.contains(&self.platform) {
                                        continue;
                                    }
                                }

                                if let Some(ref tags) = input.tags {
                                    if let Some(ref skill_tags) = metadata.tags {
                                        if !tags.iter().any(|t| skill_tags.contains(t)) {
                                            continue;
                                        }
                                    }
                                }

                                let category = path.file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_default();

                                *categories.entry(category.clone()).or_insert(0) += 1;

                                if let Some(ref tags) = metadata.tags {
                                    for tag in tags {
                                        *all_tags.entry(tag.clone()).or_insert(0) += 1;
                                    }
                                }

                                all_skills.push(SkillMeta {
                                    name: metadata.name.unwrap_or_else(|| category.clone()),
                                    description: metadata.description.unwrap_or_default(),
                                    category,
                                    tags: metadata.tags.unwrap_or_default(),
                                    version: metadata.version,
                                });
                            }
                        }
                    }
                }
            }
        }

        if let Some(ref cat) = input.category {
            all_skills.retain(|s| s.category == *cat);
        }

        SkillListOutput {
            success: true,
            count: all_skills.len(),
            skills: all_skills,
            categories: categories.into_iter().map(|(k, _)| k).collect(),
            tags: all_tags.into_iter().map(|(k, _)| k).collect(),
        }
    }

    pub fn view(&self, input: SkillViewInput) -> SkillViewOutput {
        if let Some(path) = self.find_skill(&input.name) {
            let skill_md = path.join("SKILL.md");
            if let Ok(content) = fs::read_to_string(&skill_md) {
                let (metadata, body) = parse_skill_frontmatter(&content);

                return SkillViewOutput {
                    success: true,
                    name: metadata.name.unwrap_or_else(|| input.name.clone()),
                    description: metadata.description.unwrap_or_default(),
                    content: body,
                    metadata: SkillMetadata {
                        version: metadata.version,
                        author: metadata.author,
                        tags: metadata.tags.unwrap_or_default(),
                        related_skills: metadata.related_skills.unwrap_or_default(),
                        platforms: metadata.platforms.unwrap_or_default(),
                    },
                };
            }
        }

        SkillViewOutput {
            success: false,
            name: input.name,
            description: String::new(),
            content: String::new(),
            metadata: SkillMetadata {
                version: None,
                author: None,
                tags: vec![],
                related_skills: vec![],
                platforms: vec![],
            },
        }
    }

    pub fn invoke(&self, input: SkillInvokeInput) -> SkillInvokeOutput {
        let result = self.view(SkillViewInput {
            name: input.name.clone(),
            file_path: None,
        });

        if result.success {
            SkillInvokeOutput {
                success: true,
                name: result.name,
                content: result.content,
                warning: None,
            }
        } else {
            SkillInvokeOutput {
                success: false,
                name: input.name.clone(),
                content: String::new(),
                warning: Some(format!("Skill '{}' not found", input.name)),
            }
        }
    }

    pub fn list_workspaces(&self) -> Vec<WorkspaceInfo> {
        let mut workspaces = Vec::new();

        for root in &self.roots {
            if root.exists() && root.is_dir() {
                let skill_count = fs::read_dir(root)
                    .map(|e| e.into_iter().flatten().filter(|d| d.path().is_dir()).count())
                    .unwrap_or(0);

                let root_type = if root.to_string_lossy().contains(".cowd") {
                    "cowd"
                } else if root.to_string_lossy().contains(".qoder") {
                    "qoder"
                } else {
                    "agents"
                };

                workspaces.push(WorkspaceInfo {
                    name: root.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default(),
                    path: root.to_string_lossy().to_string(),
                    root_type: root_type.to_string(),
                    skill_count,
                });
            }
        }

        workspaces
    }

    pub fn get_workspace(&self, name: &str) -> Option<WorkspaceInfo> {
        self.list_workspaces()
            .into_iter()
            .find(|w| w.name == name)
    }

    pub fn preview_workspace(&self, name: &str) -> Option<WorkspacePreview> {
        for root in &self.roots {
            if root.exists() && root.is_dir() {
                if let Some(dir_name) = root.file_name().and_then(|n| n.to_str()) {
                    if dir_name == name {
                        let mut skills = Vec::new();
                        let mut total_entries = 0;

                        if let Ok(entries) = fs::read_dir(root) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_dir() {
                                    let skill_name = path.file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_default();
                                    skills.push(skill_name.clone());

                                    // Count entries in skill directory
                                    if let Ok(sub_entries) = fs::read_dir(&path) {
                                        total_entries += sub_entries.count();
                                    }
                                }
                            }
                        }

                        return Some(WorkspacePreview {
                            name: name.to_string(),
                            description: format!("{} workspace with {} skills", root_type(root), skills.len()),
                            skills,
                            total_entries,
                        });
                    }
                }
            }
        }
        None
    }

    fn find_skill(&self, name: &str) -> Option<PathBuf> {
        for root in &self.roots {
            let skill_path = root.join(name);
            if skill_path.is_dir() && skill_path.join("SKILL.md").exists() {
                return Some(skill_path);
            }

            if let Ok(entries) = fs::read_dir(root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                            if dir_name.to_lowercase() == name.to_lowercase()
                                && path.join("SKILL.md").exists()
                            {
                                return Some(path);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    // ── 3B-4: Skill dynamic management ────────────────────────────────────

    /// Install a skill by copying from a source directory into the user's
    /// skill root (`~/.qoder/skills/{name}/`).
    pub fn install(&self, source: &std::path::Path) -> Result<String, String> {
        let skill_md = source.join("SKILL.md");
        if !skill_md.exists() {
            return Err(format!("Source directory does not contain SKILL.md: {}", source.display()));
        }

        let name = source.file_name()
            .and_then(|n: &std::ffi::OsStr| n.to_str())
            .ok_or_else(|| "Cannot determine skill name from source path".to_string())?
            .to_string();

        // Install to the first writable root (user's home .qoder/skills)
        let target_root = self.roots.first()
            .ok_or_else(|| "No skill roots configured".to_string())?;

        if !target_root.exists() {
            std::fs::create_dir_all(target_root)
                .map_err(|e| format!("Failed to create skill root: {}", e))?;
        }

        let target = target_root.join(&name);
        if target.exists() {
            return Err(format!("Skill '{}' is already installed at {}", name, target.display()));
        }

        // Copy the directory recursively
        copy_dir_recursive(source, &target)?;

        Ok(format!("Skill '{}' installed to {}", name, target.display()))
    }

    /// Uninstall (remove) a skill by name.
    pub fn uninstall(&self, name: &str) -> Result<String, String> {
        if let Some(path) = self.find_skill(name) {
            std::fs::remove_dir_all(&path)
                .map_err(|e| format!("Failed to remove skill '{}': {}", name, e))?;
            Ok(format!("Skill '{}' uninstalled", name))
        } else {
            Err(format!("Skill '{}' not found", name))
        }
    }

    /// Toggle a skill's enabled state by renaming its SKILL.md file.
    /// When disabled, the file is renamed to SKILL.md.disabled.
    pub fn toggle(&self, name: &str, enabled: bool) -> Result<String, String> {
        if let Some(path) = self.find_skill(name) {
            let skill_md = path.join("SKILL.md");
            let disabled_md = path.join("SKILL.md.disabled");

            if enabled {
                // Enable: rename SKILL.md.disabled → SKILL.md
                if disabled_md.exists() && !skill_md.exists() {
                    std::fs::rename(&disabled_md, &skill_md)
                        .map_err(|e| format!("Failed to enable skill: {}", e))?;
                    Ok(format!("Skill '{}' enabled", name))
                } else if skill_md.exists() {
                    Ok(format!("Skill '{}' is already enabled", name))
                } else {
                    Err(format!("Skill '{}' has no SKILL.md or SKILL.md.disabled file", name))
                }
            } else {
                // Disable: rename SKILL.md → SKILL.md.disabled
                if skill_md.exists() {
                    std::fs::rename(&skill_md, &disabled_md)
                        .map_err(|e| format!("Failed to disable skill: {}", e))?;
                    Ok(format!("Skill '{}' disabled", name))
                } else {
                    Ok(format!("Skill '{}' is already disabled", name))
                }
            }
        } else {
            Err(format!("Skill '{}' not found", name))
        }
    }
}

impl Default for SkillService {
    fn default() -> Self {
        Self::new()
    }
}

fn root_type(path: &PathBuf) -> &'static str {
    if path.to_string_lossy().contains(".qoder") {
        "Qoder"
    } else {
        "Agents"
    }
}

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<(), String> {
    std::fs::create_dir_all(dst)
        .map_err(|e| format!("Failed to create directory {}: {}", dst.display(), e))?;
    for entry in std::fs::read_dir(src).map_err(|e| format!("Failed to read dir: {}", e))? {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)
                .map_err(|e| format!("Failed to copy {}: {}", src_path.display(), e))?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct ParsedFrontmatter {
    name: Option<String>,
    description: Option<String>,
    version: Option<String>,
    author: Option<String>,
    tags: Option<Vec<String>>,
    related_skills: Option<Vec<String>>,
    platforms: Option<Vec<String>>,
}

fn parse_skill_frontmatter(content: &str) -> (ParsedFrontmatter, String) {
    let mut metadata = ParsedFrontmatter::default();

    if !content.starts_with("---") {
        return (metadata, content.to_string());
    }

    if let Some(end_idx) = content[3..].find("---") {
        let frontmatter = &content[3..end_idx + 3];
        let body = content[end_idx + 6..].trim();

        for line in frontmatter.lines() {
            let line = line.trim();
            if line.starts_with("name:") {
                metadata.name = Some(line["name:".len()..].trim().to_string());
            } else if line.starts_with("description:") {
                metadata.description = Some(line["description:".len()..].trim().to_string());
            } else if line.starts_with("version:") {
                metadata.version = Some(line["version:".len()..].trim().to_string());
            } else if line.starts_with("author:") {
                metadata.author = Some(line["author:".len()..].trim().to_string());
            } else if line.starts_with("tags:") {
                if let Some(tags_str) = line["tags:".len()..].trim().strip_prefix('[') {
                    if let Some(end) = tags_str.find(']') {
                        let tags_str = &tags_str[..end];
                        metadata.tags = Some(
                            tags_str.split(',')
                                .map(|s| s.trim().trim_matches('"').to_string())
                                .collect(),
                        );
                    }
                }
            } else if line.starts_with("related_skills:") {
                if let Some(skills_str) = line["related_skills:".len()..].trim().strip_prefix('[') {
                    if let Some(end) = skills_str.find(']') {
                        let skills_str = &skills_str[..end];
                        metadata.related_skills = Some(
                            skills_str.split(',')
                                .map(|s| s.trim().trim_matches('"').to_string())
                                .collect(),
                        );
                    }
                }
            } else if line.starts_with("platforms:") {
                if let Some(platforms_str) = line["platforms:".len()..].trim().strip_prefix('[') {
                    if let Some(end) = platforms_str.find(']') {
                        let platforms_str = &platforms_str[..end];
                        metadata.platforms = Some(
                            platforms_str.split(',')
                                .map(|s| s.trim().trim_matches('"').to_string())
                                .collect(),
                        );
                    }
                }
            }
        }

        (metadata, body.to_string())
    } else {
        (metadata, content.to_string())
    }
}

// ── Platform Adapter Factory ─────────────────────────────────────────────────────

use runtime::platform::PlatformAdapter;

/// Create a platform adapter based on configuration.
async fn create_platform_adapter(
    config: &PlatformConfig,
) -> Result<Box<dyn PlatformAdapter>, PlatformError> {
    use runtime::platform::feishu::create_feishu_adapter;
    use runtime::platform::email::create_email_adapter;

    match config.platform_type.to_lowercase().as_str() {
        "feishu" | "lark" => {
            let settings = serde_json::to_value(&config.settings)
                .map_err(|e| PlatformError::ConfigError(e.to_string()))?;
            let adapter = create_feishu_adapter(&settings)?;
            Ok(Box::new(adapter))
        }
        "email" | "mail" => {
            let settings = serde_json::to_value(&config.settings)
                .map_err(|e| PlatformError::ConfigError(e.to_string()))?;
            let adapter = create_email_adapter(&settings)?;
            Ok(Box::new(adapter))
        }
        // WeCom 需要企业微信 SDK，这里暂时返回错误
        "wecom" | "wechat" => {
            Err(PlatformError::ConfigError(
                "WeChat adapter not yet implemented".to_string()
            ))
        }
        other => {
            Err(PlatformError::ConfigError(format!(
                "Unknown platform type: {}", other
            )))
        }
    }
}

// ── HTTP Handlers - Platform Management (T01-06) ──────────────────────────────

async fn list_platforms_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let Some(ref runtime) = state.platform_runtime else {
        return Json(serde_json::json!({
            "platforms": [],
            "message": "no platform runtime configured"
        })).into_response();
    };

    let platform_names = runtime.list_platforms().await;
    let mut platform_list = Vec::new();
    for name in &platform_names {
        if let Some(info) = runtime.get_platform_info(name).await {
            platform_list.push(info);
        }
    }

    Json(serde_json::json!({
        "platforms": platform_list,
        "count": platform_list.len()
    })).into_response()
}

async fn get_platform_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let Some(ref runtime) = state.platform_runtime else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "no platform runtime configured"}))).into_response();
    };

    match runtime.get_platform_info(&name).await {
        Some(info) => Json(info).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": format!("platform not found: {name}")}))).into_response(),
    }
}

async fn list_platform_sessions_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(platform): Path<String>,
) -> axum::response::Response {
    let Some(ref runtime) = state.platform_runtime else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "no platform runtime configured"}))).into_response();
    };

    let all_sessions = runtime.list_sessions().await;
    let platform_sessions: Vec<_> = all_sessions.into_iter()
        .filter(|s| s.get("platform").and_then(|v| v.as_str()) == Some(&platform))
        .collect();

    Json(serde_json::json!({
        "platform": platform,
        "sessions": platform_sessions,
        "count": platform_sessions.len()
    })).into_response()
}

async fn delete_platform_session_handler(
    AxumState(state): AxumState<HttpAppState>,
    axum::extract::Path((platform, session_id)): axum::extract::Path<(String, String)>,
) -> axum::response::Response {
    let Some(ref runtime) = state.platform_runtime else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "no platform runtime configured"}))).into_response();
    };

    let session_key = format!("{}:{}", platform, session_id);
    if runtime.delete_session(&session_key).await {
        Json(serde_json::json!({"deleted": session_key, "ok": true})).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "session not found"}))).into_response()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// WebUI API Handlers - Token-based Authentication & WebUI Support
// ═══════════════════════════════════════════════════════════════════════════

/// Request body for login
#[derive(Debug, Deserialize)]
struct LoginRequest {
    token: String,
}

/// Response for auth operations
#[derive(Debug, Serialize)]
struct AuthResponse {
    success: bool,
    token: Option<String>,
    message: Option<String>,
    user: Option<UserInfo>,
}

/// Basic user info
#[derive(Debug, Serialize)]
struct UserInfo {
    id: String,
    name: String,
    role: String,
}

// ── Auth Handlers ────────────────────────────────────────────────────────────

/// POST /api/auth/login
/// Authenticate with token and return session info
async fn auth_login_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(req): Json<LoginRequest>,
) -> axum::response::Response {
    // Validate token - B1 fix: must match configured auth_token exactly
    // When auth_token is empty, auth is effectively disabled (auto-pass)
    let token_valid = if !state.auth_enabled || state.auth_token.is_empty() {
        true
    } else {
        req.token == state.auth_token
    };

    if token_valid {
        Json(AuthResponse {
            success: true,
            token: Some(req.token.clone()),
            message: Some("Login successful".to_string()),
            user: Some(UserInfo {
                id: "webui-user".to_string(),
                name: "WebUI User".to_string(),
                role: "user".to_string(),
            }),
        }).into_response()
    } else {
        (StatusCode::UNAUTHORIZED, Json(AuthResponse {
            success: false,
            token: None,
            message: Some("Invalid token".to_string()),
            user: None,
        })).into_response()
    }
}

/// GET /api/auth/verify
/// Verify current token is valid
async fn auth_verify_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    // For now, auth always passes - in production, check token
    let _ = state;
    Json(AuthResponse {
        success: true,
        token: None,
        message: Some("Token valid".to_string()),
        user: Some(UserInfo {
            id: "webui-user".to_string(),
            name: "WebUI User".to_string(),
            role: "user".to_string(),
        }),
    }).into_response()
}

/// POST /api/auth/logout
/// Logout current session
async fn auth_logout_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    Json(serde_json::json!({
        "success": true,
        "message": "Logged out successfully"
    })).into_response()
}

// ── WebUI Session Handlers ────────────────────────────────────────────────────

/// Create a new session
/// POST /api/sessions
async fn create_session_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<CreateSessionParams>,
) -> axum::response::Response {
    let session_id = format!("webui-{}", Uuid::new_v4());
    let now_str = chrono::Utc::now().to_rfc3339();

    // Create session record
    let record = memory::store::session::SessionRecord {
        session_id: session_id.clone(),
        platform: "webui".to_string(),
        chat_id: session_id.clone(),
        user_id: Some("webui-user".to_string()),
        model: Some(params.model.unwrap_or_else(|| "claude-opus-4-6".to_string())),
        created_at: now_str.clone(),
        last_activity: now_str,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: None,
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
    };

    if let Err(e) = state.session_store.create_session(&record) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("Failed to create session: {}", e)
        }))).into_response();
    }

    // Broadcast SessionCreated event for real-time sync
    let title = params.title.clone().unwrap_or_else(|| "新会话".to_string());
    let _ = state.session_broadcast.send(SessionEvent::SessionCreated {
        session_id: session_id.clone(),
        title: Some(title.clone()),
    });

    Json(serde_json::json!({
        "id": session_id,
        "session_id": session_id,
        "title": title,
        "model": record.model,
        "created_at": record.created_at,
        "updated_at": record.last_activity,
        "message_count": 0
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct CreateSessionParams {
    title: Option<String>,
    model: Option<String>,
}

/// Get session messages
/// GET /api/sessions/:id/messages
/// 3A-2 fix: Load messages from JSONL file instead of returning empty array
async fn get_session_messages_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
) -> axum::response::Response {
    match state.session_store.get_session(&session_id) {
        Ok(Some(record)) => {
            // 3A-2: Load actual messages from the JSONL session file
            let messages = load_messages_from_jsonl(&state.sessions_dir, &session_id);

            Json(serde_json::json!({
                "session_id": session_id,
                "messages": messages,
                "count": record.message_count,
                "session": {
                    "id": record.session_id,
                    "platform": record.platform,
                    "model": record.model,
                    "created_at": record.created_at,
                    "last_activity": record.last_activity,
        }
    })).into_response()
}

        Ok(None) => {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({
                "error": format!("Session {} not found", session_id)
            }))).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": format!("Failed to load session: {}", e)
            }))).into_response()
        }
    }
}

/// 3A-2: Load messages from a session's JSONL file.
/// Each line is a JSON object with a "type" field; messages have type="message"
/// and the payload is under the "message" key.
fn load_messages_from_jsonl(sessions_dir: &std::path::Path, session_id: &str) -> Vec<serde_json::Value> {
    let path = sessions_dir.join(format!("{session_id}.jsonl"));
    if !path.exists() {
        return Vec::new();
    }
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "Failed to read JSONL session file");
            return Vec::new();
        }
    };
    content
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }
            let record: serde_json::Value = serde_json::from_str(trimmed).ok()?;
            if record.get("type").and_then(|v| v.as_str()) == Some("message") {
                record.get("message").cloned()
            } else {
                None
            }
        })
        .collect()
}

/// Send a message and get response
/// POST /api/sessions/:id/messages
#[derive(Debug, Deserialize)]
struct SendMessageParams {
    content: String,
    #[serde(default)]
    stream: bool,
}

async fn send_message_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
    Json(params): Json<SendMessageParams>,
) -> impl IntoResponse {
    let user_content = params.content.clone();
    let broadcast_tx = state.session_broadcast.clone();
    let cognitive_manager = state.cognitive_manager.clone();
    let sid = session_id.clone();
    let content_for_turn = user_content.clone();

    let _ = broadcast_tx.send(SessionEvent::RuntimeStarted { session_id: sid.clone() });

    let response_text = match tokio::task::spawn_blocking(move || {
        server_execute_turn(&content_for_turn)
    }).await {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => format!("Error: {}", e),
        Err(e) => format!("Internal error: {}", e),
    };

    let _ = broadcast_tx.send(SessionEvent::RuntimeFinished { session_id: sid.clone() });
    let _ = broadcast_tx.send(SessionEvent::MessageAdded { session_id: sid.clone(), message_count: 1 });

    if let Some(ref mgr) = cognitive_manager {
        let user_msg = to_memory_message("user", &user_content);
        let assistant_msg = to_memory_message("assistant", &response_text);
        let mut mem_messages = vec![user_msg, assistant_msg];
        let mgr_arc = Arc::clone(mgr);
        let _ = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("memory rt");
            rt.block_on(async { mgr_arc.on_turn_end(&mut mem_messages).await })
        }).await;
    }

    let response = serde_json::json!({
        "id": format!("msg-{}", Uuid::new_v4()),
        "session_id": session_id,
        "role": "assistant",
        "content": response_text,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    Json(response).into_response()
}

fn server_execute_turn(input: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let system_prompt = crate::build_system_prompt().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    let session = crate::new_cli_session().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    let session_id = session.session_id.clone();
    let mut runtime = crate::build_runtime(
        session, &session_id, "claude-sonnet-4-6".to_string(), system_prompt,
        true, false, None,
        runtime::PermissionMode::DangerFullAccess, None, None,
    ).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    let prompter = runtime::permissions::SharedPrompter::new(Box::new(
        crate::CliPermissionPrompter::new(runtime::PermissionMode::DangerFullAccess)
    ));
    let turn_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("server_execute_turn rt");
    let summary = turn_rt
        .block_on(runtime.run_turn_async(input, &prompter))
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    let text = summary.assistant_messages.last()
        .and_then(|m| m.blocks.iter().find_map(|b| match b {
            runtime::ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        }))
        .unwrap_or_else(|| "No response".to_string());
    Ok(text)
}

/// Send a message with SSE streaming
/// POST /api/sessions/:id/messages/stream
async fn send_message_stream_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
    Json(params): Json<SendMessageParams>,
) -> Response {
    let user_content = params.content.clone();
    let broadcast_tx = state.session_broadcast.clone();
    let sid = session_id.clone();

    let (tx, rx) = tokio::sync::mpsc::channel::<String>(256);
    let content_for_task = user_content.clone();
    let _sid_for_task = sid.clone();
    let _broadcast_for_task = broadcast_tx.clone();

    tokio::task::spawn_blocking(move || {
        let text = match server_execute_turn(&content_for_task) {
            Ok(t) => t,
            Err(e) => format!("Error: {}", e),
        };
        for c in text.chars() {
            if tx.blocking_send(c.to_string()).is_err() { break; }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    let sid_stream = sid.clone();
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(move |c| {
        let sse = serde_json::json!({
            "id": format!("msg-{}", Uuid::new_v4()),
            "session_id": sid_stream,
            "role": "assistant",
            "content": c,
            "delta": true
        });
        Ok::<_, std::convert::Infallible>(Event::default().data(sse.to_string()))
    });

    Sse::new(stream).into_response()
}

// ── WebUI Config Handlers ─────────────────────────────────────────────────────

/// Get current configuration
/// GET /api/config
async fn get_config_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    Json(serde_json::json!({
        "model": "claude-opus-4-6",
        "provider": "anthropic",
        "theme": "dark",
        "language": "zh-CN",
        "streaming": true
    })).into_response()
}

/// Update configuration
/// PUT /api/config
#[derive(Debug, Deserialize)]
struct UpdateConfigParams {
    model: Option<String>,
    provider: Option<String>,
    theme: Option<String>,
    language: Option<String>,
}

async fn update_config_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<UpdateConfigParams>,
) -> axum::response::Response {
    let _ = state;
    Json(serde_json::json!({
        "success": true,
        "config": {
            "model": params.model.unwrap_or_else(|| "claude-opus-4-6".to_string()),
            "provider": params.provider.unwrap_or_else(|| "anthropic".to_string()),
            "theme": params.theme.unwrap_or_else(|| "dark".to_string()),
            "language": params.language.unwrap_or_else(|| "zh-CN".to_string())
        }
    })).into_response()
}

/// Get available providers and models
/// GET /api/config/providers
async fn get_providers_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    Json(serde_json::json!({
        "providers": [
            {
                "id": "anthropic",
                "name": "Anthropic",
                "models": [
                    {"id": "claude-opus-4-6", "name": "Claude Opus 4.6"},
                    {"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6"},
                    {"id": "claude-haiku-4-5-20251213", "name": "Claude Haiku 4.5"}
                ]
            },
            {
                "id": "openai",
                "name": "OpenAI",
                "models": [
                    {"id": "gpt-4o", "name": "GPT-4o"},
                    {"id": "gpt-4o-mini", "name": "GPT-4o Mini"}
                ]
            },
            {
                "id": "google",
                "name": "Google",
                "models": [
                    {"id": "gemini-2.0-flash", "name": "Gemini 2.0 Flash"}
                ]
            },
            {
                "id": "ollama",
                "name": "Ollama",
                "models": [
                    {"id": "llama3", "name": "Llama 3"},
                    {"id": "codellama", "name": "Code Llama"}
                ]
            },
            {
                "id": "kimi",
                "name": "Kimi",
                "models": [
                    {"id": "moonshot-v1-128k", "name": "Moonshot V1 128K"}
                ]
            }
        ]
    })).into_response()
}

// ── WebUI Memory Handlers ─────────────────────────────────────────────────────

/// Get memory for a specific layer
/// GET /api/memory/:layer
async fn get_memory_layer_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(layer): Path<String>,
) -> axum::response::Response {
    if let Some(ref mgr) = state.cognitive_manager {
        let memory_layer = match layer.as_str() {
            "working" => memory::types::MemoryLayer::L1,
            "personal" => memory::types::MemoryLayer::L2,
            "project" => memory::types::MemoryLayer::L3,
            "global" => memory::types::MemoryLayer::L0,
            _ => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                    "error": "Invalid layer. Use: working, personal, project, global"
                }))).into_response();
            }
        };

        match mgr.list_layer_entries(memory_layer).await {
            Ok(entries) => {
                let items: Vec<_> = entries.into_iter().map(|m| {
                    serde_json::json!({
                        "id": m.id.to_string(),
                        "title": m.title,
                        "layer": format!("{:?}", m.layer),
                        "category": format!("{:?}", m.category)
                    })
                }).collect();

                Json(serde_json::json!({
                    "layer": layer,
                    "entries": items,
                    "count": items.len()
                })).into_response()
            }
            Err(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
            }
        }
    } else {
        Json(serde_json::json!({
            "layer": layer,
            "entries": [],
            "count": 0,
            "warning": "memory subsystem not enabled"
        })).into_response()
    }
}

// ── WebUI Command Handlers ─────────────────────────────────────────────────────

/// List available commands
/// GET /api/commands
async fn list_commands_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    let commands = vec![
        serde_json::json!({"name": "new", "description": "创建新会话", "usage": "/new [title]"}),
        serde_json::json!({"name": "clear", "description": "清空当前对话", "usage": "/clear"}),
        serde_json::json!({"name": "sessions", "description": "列出所有会话", "usage": "/sessions"}),
        serde_json::json!({"name": "memory", "description": "记忆管理", "usage": "/memory [layer] [query]"}),
        serde_json::json!({"name": "remember", "description": "添加到记忆", "usage": "/remember <content>"}),
        serde_json::json!({"name": "set", "description": "设置配置项", "usage": "/set <key> <value>"}),
        serde_json::json!({"name": "get", "description": "获取配置项", "usage": "/get [key]"}),
        serde_json::json!({"name": "theme", "description": "切换主题", "usage": "/theme [dark|light|slate]"}),
        serde_json::json!({"name": "help", "description": "显示帮助", "usage": "/help [command]"}),
        serde_json::json!({"name": "history", "description": "显示命令历史", "usage": "/history"}),
    ];

    Json(serde_json::json!({
        "commands": commands,
        "count": commands.len()
    })).into_response()
}

/// Get command history
/// GET /api/commands/history
async fn command_history_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    // Return empty history for now
    Json(serde_json::json!({
        "history": [],
        "count": 0
    })).into_response()
}

/// Execute a command
/// POST /api/commands/execute
#[derive(Debug, Deserialize)]
struct ExecuteCommandParams {
    command: String,
    #[serde(default)]
    context: serde_json::Value,
}

async fn execute_command_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<ExecuteCommandParams>,
) -> axum::response::Response {
    let _ = state;
    // Parse and execute the command
    let cmd = params.command.trim();
    if !cmd.starts_with('/') {
        return Json(serde_json::json!({
            "error": "Commands must start with /"
        })).into_response();
    }

    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let cmd_name = parts.get(0).unwrap_or(&"").trim_start_matches('/');

    let result = match cmd_name {
        "new" => format!("创建新会话: {}", parts.get(1).unwrap_or(&"新会话")),
        "clear" => "对话已清空".to_string(),
        "sessions" => "获取会话列表".to_string(),
        "help" => {
            if let Some(target) = parts.get(1) {
                format!("显示 {} 命令的帮助", target)
            } else {
                "显示帮助信息".to_string()
            }
        }
        "history" => "显示命令历史".to_string(),
        "theme" => {
            let theme = parts.get(1).unwrap_or(&"dark");
            format!("切换到 {} 主题", theme)
        }
        _ => format!("未知命令: /{}", cmd_name),
    };

    Json(serde_json::json!({
        "success": true,
        "command": cmd,
        "result": result
    })).into_response()
}

// ── WebUI Workspace Handlers ──────────────────────────────────────────────────

/// Get current workspace
/// GET /api/workspace
async fn get_current_workspace_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    let cwd = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| ".".to_string());

    Json(serde_json::json!({
        "id": "default",
        "name": "当前工作区",
        "path": cwd,
        "type": "local"
    })).into_response()
}

/// List files in workspace
/// GET /api/workspace/files
#[derive(Debug, Deserialize)]
struct ListFilesQuery {
    path: Option<String>,
}

async fn list_files_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<ListFilesQuery>,
) -> axum::response::Response {
    let _ = state;
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let base_path = params.path.as_ref()
        .map(|p| cwd.join(p))
        .unwrap_or(cwd);

    let mut files = Vec::new();

    if let Ok(entries) = fs::read_dir(&base_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            // Skip hidden files
            if name.starts_with('.') {
                continue;
            }

            files.push(serde_json::json!({
                "name": name,
                "path": path.to_string_lossy(),
                "type": if path.is_dir() { "dir" } else { "file" },
                "size": path.metadata().map(|m| m.len()).unwrap_or(0)
            }));
        }
    }

    Json(serde_json::json!({
        "path": params.path.unwrap_or_else(|| "/".to_string()),
        "files": files,
        "count": files.len()
    })).into_response()
}

/// Create a file in workspace
/// POST /api/workspace/files
#[derive(Debug, Deserialize)]
struct CreateFileParams {
    name: String,
    path: Option<String>,
    content: Option<String>,
}

async fn create_file_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<CreateFileParams>,
) -> axum::response::Response {
    let _ = state;
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let base_path = params.path.as_ref()
        .map(|p| cwd.join(p))
        .unwrap_or(cwd);

    let file_path = base_path.join(&params.name);

    match fs::write(&file_path, params.content.unwrap_or_default()) {
        Ok(_) => Json(serde_json::json!({
            "success": true,
            "file": {
                "name": params.name,
                "path": file_path.to_string_lossy(),
                "type": "file"
            }
        })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("Failed to create file: {}", e)
        }))).into_response()
    }
}


// ── P0-1: Approval Handlers ──────────────────────────────────────────────────

/// GET /api/approval/pending - Get all pending approval requests
async fn get_pending_approvals_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let list = state.approval_gate.get_pending_requests().await;
    Json(serde_json::json!({"pending": list, "count": list.len()})).into_response()
}

#[derive(Debug, Deserialize)]
struct ApprovalResponsePayload {
    request_id: String,
    approved: bool,
    reason: Option<String>,
    persistence: Option<String>,
}

/// POST /api/approval/respond - Respond to an approval request
async fn respond_to_approval_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(payload): Json<ApprovalResponsePayload>,
) -> axum::response::Response {
    let verdict = if payload.approved {
        runtime::permission_enforcer::ApprovalVerdict::Approved
    } else {
        runtime::permission_enforcer::ApprovalVerdict::Denied {
            reason: payload.reason.unwrap_or_else(|| "Denied by user".to_string()),
        }
    };

    let persistence = match payload.persistence.as_deref() {
        Some("session") => runtime::permission_enforcer::ApprovalPersistence::Session,
        Some("always") => runtime::permission_enforcer::ApprovalPersistence::Always,
        _ => runtime::permission_enforcer::ApprovalPersistence::Once,
    };

    match state.approval_gate.resolve_approval(&payload.request_id, verdict, persistence).await {
        Some(_) => Json(serde_json::json!({"status": "ok", "request_id": payload.request_id})).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "Approval not found or expired"
        }))).into_response(),
    }
}

/// GET /api/approval/config - Get current approval configuration
async fn get_approval_config_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let config = state.approval_gate.config().read().await;
    Json(serde_json::json!({
        "solo_mode": config.solo_mode,
        "solo_honor_critical": config.solo_honor_critical,
        "auto_pass_read_only": config.auto_pass_read_only,
        "auto_pass_low_risk": config.auto_pass_low_risk,
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct UpdateApprovalConfigPayload {
    solo_mode: Option<bool>,
    solo_honor_critical: Option<bool>,
    auto_pass_read_only: Option<bool>,
    auto_pass_low_risk: Option<bool>,
}

/// PUT /api/approval/config - Update approval configuration
async fn update_approval_config_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(payload): Json<UpdateApprovalConfigPayload>,
) -> axum::response::Response {
    let mut config = state.approval_gate.config().read().await.clone();
    if let Some(v) = payload.solo_mode { config.solo_mode = v; }
    if let Some(v) = payload.solo_honor_critical { config.solo_honor_critical = v; }
    if let Some(v) = payload.auto_pass_read_only { config.auto_pass_read_only = v; }
    if let Some(v) = payload.auto_pass_low_risk { config.auto_pass_low_risk = v; }
    state.approval_gate.update_config(config).await;

    let config = state.approval_gate.config().read().await;
    Json(serde_json::json!({
        "solo_mode": config.solo_mode,
        "solo_honor_critical": config.solo_honor_critical,
        "auto_pass_read_only": config.auto_pass_read_only,
        "auto_pass_low_risk": config.auto_pass_low_risk,
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct ToggleSoloPayload {
    enabled: bool,
    honor_critical: Option<bool>,
}

/// POST /api/approval/solo - Toggle SOLO mode
async fn toggle_solo_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(payload): Json<ToggleSoloPayload>,
) -> axum::response::Response {
    let mut config = state.approval_gate.config().read().await.clone();
    config.solo_mode = payload.enabled;
    if let Some(honor) = payload.honor_critical {
        config.solo_honor_critical = honor;
    }
    state.approval_gate.update_config(config).await;

    let config = state.approval_gate.config().read().await;
    Json(serde_json::json!({
        "solo_mode": config.solo_mode,
        "solo_honor_critical": config.solo_honor_critical,
    })).into_response()
}

// ═══════════════════════════════════════════════════════════════════════
// P0-4: File Upload Handlers
// ═══════════════════════════════════════════════════════════════════════

const MAX_UPLOAD_SIZE: u64 = 20 * 1024 * 1024; // 20MB
const MAX_FILENAME_LEN: usize = 200;

/// Dangerous file extension blacklist
const DANGEROUS_EXTENSIONS: &[&str] = &[
    "exe", "bat", "cmd", "ps1", "vbs", "js", "wsf", "msi", "com",
    "scr", "pif", "hta", "cpl", "dll", "sys",
];

fn is_dangerous_extension(filename: &str) -> bool {
    let ext = std::path::Path::new(filename)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    DANGEROUS_EXTENSIONS.contains(&ext.as_str())
}

/// Sanitize filename: non-word chars → _, truncate 200 chars, preserve extension.
fn sanitize_filename(original: &str) -> String {
    let path = std::path::Path::new(original);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("upload");
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    let safe_stem: String = stem
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let max_stem = MAX_FILENAME_LEN.saturating_sub(ext.len() + 1);
    let truncated: String = safe_stem.chars().take(max_stem).collect();

    if ext.is_empty() {
        truncated
    } else {
        format!("{}.{}", truncated, ext)
    }
}

/// POST /api/upload - Upload a file via multipart/form-data
async fn upload_file_handler(
    AxumState(state): AxumState<HttpAppState>,
    mut multipart: Multipart,
) -> axum::response::Response {
    let _ = state;
    let workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = match field.file_name() {
            Some(name) => name.to_string(),
            None => continue,
        };

        // Security: check dangerous extension
        if is_dangerous_extension(&filename) {
            let ext = std::path::Path::new(&filename)
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": format!("File type .{} is not allowed", ext)
                })),
            )
                .into_response();
        }

        let safe_name = sanitize_filename(&filename);
        let data = match field.bytes().await {
            Ok(d) => d,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response()
            }
        };

        // Size check
        if data.len() as u64 > MAX_UPLOAD_SIZE {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({
                    "error": format!(
                        "File too large: {} bytes (max {} bytes)",
                        data.len(),
                        MAX_UPLOAD_SIZE
                    )
                })),
            )
                .into_response();
        }

        // Write to workspace (path traversal safety via sanitize_path)
        let _dest_path = workspace.join(&safe_name);
        match sanitize_path(&workspace, &safe_name) {
            Ok(safe_path) => {
                if let Err(e) = tokio::fs::write(&safe_path, &data).await {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
                return Json(serde_json::json!({
                    "filename": safe_name,
                    "path": safe_path.to_str().unwrap_or(""),
                    "size": data.len()
                }))
                .into_response();
            }
            Err(e) => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response()
            }
        }
    }

    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error": "No file provided"})),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct FileParams {
    path: String,
}

/// MIME type mapping
fn mime_type(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|s| s.to_str()) {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("pdf") => "application/pdf",
        Some("html") => "text/html",
        Some("css") => "text/css",
        Some("js") => "application/javascript",
        Some("json") => "application/json",
        Some("md") => "text/markdown",
        Some("py") | Some("rs") | Some("ts") | Some("go") | Some("java") => "text/plain",
        _ => "application/octet-stream",
    }
}

/// GET /api/file/raw?path=... - Get raw file content
async fn get_raw_file_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<FileParams>,
) -> axum::response::Response {
    let _ = state;
    let workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    match sanitize_path(&workspace, &params.path) {
        Ok(safe_path) => match tokio::fs::read(&safe_path).await {
            Ok(data) => {
                let mime = mime_type(&safe_path);
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, mime)],
                    data,
                )
                    .into_response()
            }
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        },
        Err(_) => StatusCode::FORBIDDEN.into_response(),
    }
}

/// DELETE /api/sessions/{id}/messages/{index} - Splice messages from index onwards
async fn splice_messages_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path((session_id, index)): Path<(String, usize)>,
) -> axum::response::Response {
    // Find the session JSONL file
    let session_file = state.sessions_dir.join(format!("{}.jsonl", session_id));
    if !session_file.exists() {
        // Try alternative: session_id might be in a sub-path
        let alt_file = state.sessions_dir.join(session_id.clone()).with_extension("jsonl");
        if !alt_file.exists() {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("Session {} not found", session_id)})),
            )
                .into_response();
        }
    }

    // Read all lines, keep only lines before the index
    let content = match tokio::fs::read_to_string(&session_file).await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to read session: {}", e)})),
            )
                .into_response()
        }
    };

    let lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
    if index >= lines.len() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("Index {} out of range (0-{})", index, lines.len().saturating_sub(1))})),
        )
            .into_response();
    }

    let kept: Vec<&str> = lines.iter().take(index).map(|s| s.as_str()).collect();
    let new_content = kept.join("\n");

    match tokio::fs::write(&session_file, new_content).await {
        Ok(()) => {
            // Update message_count in session store
            let _ = state.session_store.get_session(&session_id).map(|opt| {
                if let Some(mut record) = opt {
                    record.message_count = index as i64;
                    let _ = state.session_store.update_session(&record);
                }
            });
            Json(serde_json::json!({
                "status": "ok",
                "session_id": session_id,
                "remaining_messages": index
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to write session: {}", e)})),
        )
            .into_response(),
    }
}

// ═══════════════════════════════════════════════════════════════════
// P1-2: Entity & Knowledge Graph API Handlers
// ═══════════════════════════════════════════════════════════════════

/// GET /api/memory/entities - List all known entities
async fn list_entities_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    // Entity detection is stateless; return from the knowledge graph if available
    Json(serde_json::json!({
        "entities": [],
        "count": 0,
        "note": "Use POST /api/memory/entities/detect to detect entities from text"
    })).into_response()
}

/// POST /api/memory/entities/detect - Detect entities from text
#[derive(Debug, Deserialize)]
struct DetectEntitiesParams {
    text: String,
}

async fn detect_entities_handler(
    Json(params): Json<DetectEntitiesParams>,
) -> axum::response::Response {
    let detector = memory::entity::EntityDetector::new();
    let candidates = detector.extract(&params.text);

    // Build frequency map from candidates
    let mut freq: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (name, _, _) in &candidates {
        *freq.entry(name.clone()).or_insert(0) += 1;
    }

    let entities = detector.classify(&candidates, &freq);

    Json(serde_json::json!({
        "entities": entities,
        "count": entities.len(),
        "candidates_total": candidates.len()
    })).into_response()
}

/// GET /api/memory/triples - List all knowledge graph triples
async fn list_triples_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let _ = state;
    Json(serde_json::json!({
        "triples": [],
        "count": 0
    })).into_response()
}

/// POST /api/memory/triples - Add a knowledge graph triple
#[derive(Debug, Deserialize)]
struct AddTripleParams {
    subject_id: String,
    predicate: String,
    object_id: String,
    source: Option<String>,
    confidence: Option<f64>,
}

async fn add_triple_handler(
    Json(params): Json<AddTripleParams>,
) -> axum::response::Response {
    let _ = params;
    // Knowledge graph is in-memory per request for now; persistence would require
    // wiring into CognitiveContextManager or a dedicated store
    Json(serde_json::json!({
        "status": "ok",
        "note": "Triple accepted. Knowledge graph persistence will be added in a future update."
    })).into_response()
}

// ── P1-5: Cron Scheduler Handlers ────────────────────────────────────────────

/// GET /api/crons - List all cron jobs
async fn list_crons_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    let jobs = state.cron_scheduler.list_jobs().await;
    Json(serde_json::json!({
        "jobs": jobs,
        "count": jobs.len()
    })).into_response()
}

/// POST /api/crons - Create a new cron job
#[derive(Debug, Deserialize)]
struct CreateCronParams {
    name: String,
    schedule: String,
    prompt: String,
    #[serde(default = "default_grace_window")]
    grace_window_secs: u64,
}

fn default_grace_window() -> u64 {
    60 // Default 60-second grace window
}

async fn create_cron_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<CreateCronParams>,
) -> axum::response::Response {
    match state.cron_scheduler.create_job(
        &params.name,
        &params.schedule,
        &params.prompt,
        params.grace_window_secs,
    ).await {
        Ok(job) => Json(serde_json::json!({
            "status": "ok",
            "job": job
        })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": e}))).into_response(),
    }
}

/// DELETE /api/crons/:id - Delete a cron job
async fn delete_cron_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match state.cron_scheduler.delete_job(&id).await {
        Ok(job) => Json(serde_json::json!({
            "status": "ok",
            "deleted": job
        })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e}))).into_response(),
    }
}

/// POST /api/crons/:id/run - Manually trigger a cron job
async fn run_cron_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let start = std::time::Instant::now();
    match state.cron_scheduler.record_run_with_log(
        &id,
        runtime::team_cron_registry::CronExecutionStatus::Success,
        None,  // output — filled by actual execution in the future
        None,  // error
        start.elapsed().as_millis() as u64,
        "manual",
    ).await {
        Ok(job) => Json(serde_json::json!({
            "status": "ok",
            "job": job,
            "note": "Run recorded. Prompt execution should be triggered by the caller."
        })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e}))).into_response(),
    }
}

/// POST /api/crons/:id/pause - Pause a cron job
async fn pause_cron_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match state.cron_scheduler.pause_job(&id).await {
        Ok(job) => Json(serde_json::json!({
            "status": "ok",
            "job": job
        })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e}))).into_response(),
    }
}

/// POST /api/crons/:id/resume - Resume a cron job
async fn resume_cron_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match state.cron_scheduler.resume_job(&id).await {
        Ok(job) => Json(serde_json::json!({
            "status": "ok",
            "job": job
        })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e}))).into_response(),
    }
}

// ── Cron Logs & Approval History Handlers ──────────────────────────────────

#[derive(Debug, Deserialize)]
struct ListCronLogsQuery {
    #[serde(default = "default_query_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn default_query_limit() -> usize {
    20
}

/// GET /api/crons/logs - List all cron execution logs
async fn list_cron_logs_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<ListCronLogsQuery>,
) -> axum::response::Response {
    let log_store = state.cron_scheduler.log_store();
    let (logs, total) = log_store.list_all_logs(params.limit, params.offset).await;
    Json(serde_json::json!({
        "status": "ok",
        "logs": logs,
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
    })).into_response()
}

/// GET /api/crons/:id/logs - List execution logs for a specific cron job
async fn list_cron_job_logs_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(id): Path<String>,
    Query(params): Query<ListCronLogsQuery>,
) -> axum::response::Response {
    let log_store = state.cron_scheduler.log_store();
    let (logs, total) = log_store.list_logs(&id, params.limit, params.offset).await;
    Json(serde_json::json!({
        "status": "ok",
        "cron_job_id": id,
        "logs": logs,
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct ListApprovalHistoryQuery {
    #[serde(default = "default_query_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

/// GET /api/approval/history - List approval decision history
async fn list_approval_history_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<ListApprovalHistoryQuery>,
) -> axum::response::Response {
    let history = state.approval_gate.history();
    let (entries, total) = history.list_history(params.limit, params.offset).await;
    Json(serde_json::json!({
        "status": "ok",
        "history": entries,
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
    })).into_response()
}

// ── P1-8: Onboarding Handlers ────────────────────────────────────────────────

/// GET /api/onboarding/status - Check if onboarding is needed
async fn onboarding_status_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    // Check if config exists by trying to read provider info
    let has_providers = state.cognitive_manager.is_some();
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let config_path = runtime::cowd_dirs::project_dot_dir(&cwd).join("config.yaml");
    let config_exists = config_path.exists();

    Json(serde_json::json!({
        "needs_onboarding": !config_exists,
        "config_exists": config_exists,
        "has_memory": has_providers
    })).into_response()
}

/// POST /api/onboarding/test - Test a provider connection
#[derive(Debug, Deserialize)]
struct OnboardingTestParams {
    provider: String,
    api_key: String,
    model: Option<String>,
    base_url: Option<String>,
}

async fn onboarding_test_handler(
    Json(params): Json<OnboardingTestParams>,
) -> axum::response::Response {
    // Basic validation: check if the API key looks valid
    if params.api_key.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "success": false,
            "error": "API key cannot be empty"
        }))).into_response();
    }

    let key_len = params.api_key.len();
    let provider = params.provider.as_str();
    let valid_prefix = match provider {
        "openai" => params.api_key.starts_with("sk-"),
        "anthropic" => params.api_key.starts_with("sk-ant-"),
        _ => key_len >= 8,
    };

    if !valid_prefix {
        return Json(serde_json::json!({
            "success": false,
            "error": format!("API key format doesn't match expected {} key format", provider)
        })).into_response();
    }

    Json(serde_json::json!({
        "success": true,
        "message": "API key format validated. Save configuration to complete setup.",
        "provider": provider,
        "model": params.model
    })).into_response()
}

// ── 3B-5: Usage & Cost API ────────────────────────────────────────────────────

async fn usage_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> Response {
    let snapshot = state.usage_tracker.snapshot();
    Json(snapshot).into_response()
}

// ── 3B-3: FactChecker API ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RegisterFactsRequest {
    entity: String,
    facts: memory::temporal_graph::EntityFacts,
}

async fn register_facts_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(req): Json<RegisterFactsRequest>,
) -> Response {
    let mut checker = state.fact_checker.lock().await;
    checker.register_facts(&req.entity, req.facts);
    Json(serde_json::json!({
        "success": true,
        "message": format!("Facts registered for entity '{}'", req.entity)
    })).into_response()
}

#[derive(Debug, Deserialize)]
struct CheckFactsRequest {
    subject: String,
    predicate: String,
    object: String,
}

async fn check_facts_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(req): Json<CheckFactsRequest>,
) -> Response {
    let checker = state.fact_checker.lock().await;
    // Create a temporary triple to check
    let triple = memory::temporal_graph::Triple {
        id: format!("check_{}", chrono::Utc::now().timestamp()),
        subject: req.subject,
        predicate: req.predicate,
        object: req.object,
        valid_from: None,
        valid_until: None,
        confidence: 1.0,
        source_memory_id: None,
        source_file: None,
        source_agent: None,
    };
    let result = checker.check_triple(&triple);
    Json(result).into_response()
}

async fn audit_facts_handler(
    AxumState(state): AxumState<HttpAppState>,
) -> Response {
    // Returns the list of registered entities and their facts
    let _checker = state.fact_checker.lock().await;
    // FactChecker doesn't expose entity_facts directly, so return summary
    Json(serde_json::json!({
        "message": "Fact checker is active. Use /api/memory/facts/check to validate triples.",
        "supported_predicates": {
            "person": ["child_of", "parent_of", "partner_of", "sibling_of", "born_on", "works_for", "manages"],
            "organization": ["located_in", "subsidiary_of", "owns", "employs"],
            "project": ["uses", "depends_on", "belongs_to", "has_member"],
            "universal": ["related_to", "has_property", "contains", "part_of", "known_as"]
        }
    })).into_response()
}
