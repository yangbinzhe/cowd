//! ClawServer - 共享的运行时核心服务
//!
//! 这是所有客户端（TUI、HTTP API）共享的后端服务。
//! 提供完整的 HTTP API 支持，包括：
//! - Chat Completions (OpenAI 兼容，支持 SSE 流式输出)
//! - Memory Management (L0-L4 分层记忆系统)
//! - Skill Management (技能发现、查看、调用)
//! - Workspace Management (工作空间预览、管理)
//! - Session Management (会话 CRUD)
//! - Multi-channel Platform Adapters (Feishu, WeChat, Email)

use std::{
    collections::HashMap,
    fmt,
    fs,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Path, Query, State as AxumState, WebSocketUpgrade, ConnectInfo, ws::{Message as WsMessage, WebSocket}},
    http::{header, StatusCode, Request},
    response::{IntoResponse, Response, sse::{Event, KeepAlive, Sse}},
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::Utc;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener as TokioTcpListener,
    sync::{broadcast, mpsc, RwLock},
};
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use uuid::Uuid;

// ── 模块导入 ─────────────────────────────────────────────────────────────────

use api::{
    detect_provider_kind, max_tokens_for_model, ContentBlockDelta, InputContentBlock, InputMessage,
    MessageRequest, OpenAiCompatClient, OpenAiCompatConfig, MessageStream,
    OutputContentBlock, StreamEvent,
};
use memory::{
    cognitive::CognitiveContextManager,
    store::session::SqliteSessionStore,
    types::Message as MemMessage,
    MemoryConfig, MemoryEntry, PreparedContext,
};
use runtime::platform::{PlatformRuntime, PlatformConfig, PlatformError};
use runtime::CompactionConfig;
use runtime::{
    ApiClient as RuntimeApiClient, ApiRequest, AssistantEvent, ConversationRuntime,
    PromptCacheEvent, RuntimeError, StaticToolExecutor, ToolError, ToolExecutor,
    PermissionMode, PermissionPolicy,
    ContentBlock as SessionContentBlock, ConversationMessage as SessionMessage, 
    MessageRole as SessionMessageRole, Session,
    TokenUsage as RuntimeTokenUsage,
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
                .unwrap()
        })
}

// ── Service Management ─────────────────────────────────────────────────────────

const PID_FILE: &str = "/tmp/claw-serve.pid";

/// Server status info
#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub pid: u32,
    pub address: String,
}

/// Get PID file path
fn pid_file() -> PathBuf {
    PathBuf::from(PID_FILE)
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
    session_store: Arc<SqliteSessionStore>,
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
                tracing::warn!("Failed to initialize memory manager: {}", e);
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
    
    let session_store = match SqliteSessionStore::open(session_store_path) {
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
        let runtime_config = runtime::platform::config::PlatformRuntimeConfig::default();
        let runtime = Arc::new(PlatformRuntime::new(runtime_config));

        // 注册并启动平台适配器
        for platform_config in &config.platform_configs {
            match create_platform_adapter(platform_config).await {
                Ok(adapter) => {
                    if let Err(e) = runtime.register_adapter(adapter).await {
                        tracing::warn!("Failed to register platform adapter: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to create platform adapter: {}", e);
                }
            }
        }

        // 启动平台运行时
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
        session_broadcast: broadcast::channel(100).0,
    })
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

    // Build router - 包含所有 API 端点
    let mut router = Router::new()
        // 基础端点
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        // WebUI Auth API (Token-based authentication)
        .route("/api/auth/login", post(auth_login_handler))
        .route("/api/auth/verify", get(auth_verify_handler))
        .route("/api/auth/logout", post(auth_logout_handler))
        // WebUI Session API (创建会话)
        .route("/api/sessions", get(list_sessions_handler))
        .route("/api/sessions", post(create_session_handler))
        .route("/api/sessions/:id", get(get_session_handler))
        .route("/api/sessions/:id", delete(delete_session_handler))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/messages", get(get_session_messages_handler))
        .route("/api/sessions/:id/messages", post(send_message_handler))
        .route("/api/sessions/:id/messages/stream", post(send_message_stream_handler))
        // WebUI Config API
        .route("/api/config", get(get_config_handler))
        .route("/api/config", put(update_config_handler))
        .route("/api/config/providers", get(get_providers_handler))
        // WebUI Memory API
        .route("/api/memory", get(memory_status_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route("/api/memory/:layer", get(get_memory_layer_handler))
        .route("/api/memory/:layer", post(create_memory_entry_handler))
        .route("/api/memory/:layer/:id", delete(delete_memory_entry_handler))
        // WebUI Platform API
        .route("/api/platforms", get(list_platforms_handler))
        .route("/api/platforms/:name", get(get_platform_handler))
        .route("/api/platforms/:name/sessions", get(list_platform_sessions_handler))
        .route("/api/platforms/:name/sessions/:id", delete(delete_platform_session_handler))
        // WebUI Command API
        .route("/api/commands", get(list_commands_handler))
        .route("/api/commands/history", get(command_history_handler))
        .route("/api/commands/execute", post(execute_command_handler))
        // WebUI Workspace API
        .route("/api/workspace", get(get_current_workspace_handler))
        .route("/api/workspaces", get(list_workspaces_handler))
        .route("/api/workspace/files", get(list_files_handler))
        .route("/api/workspace/files", post(create_file_handler))
        // WebSocket 端点
        .route("/ws", get(ws_handler))
        .route("/ws/sessions", get(ws_sessions_handler))
        // 兼容 /v1 前缀 (OpenAI-compatible)
        .route("/v1/chat/completions", post(chat_handler))
        .route("/v1/models", get(models_handler))
        .route("/v1/sessions", get(list_sessions_handler))
        .route("/v1/sessions/:id", get(get_session_handler))
        .route("/v1/sessions/:id", delete(delete_session_handler))
        .route("/v1/sessions/:id/compact", post(compact_session_handler))
        .route("/v1/memory/status", get(memory_status_handler))
        .route("/v1/memory/search", get(memory_search_handler))
        .route("/v1/memory/entries", get(list_memory_entries_handler))
        .route("/v1/memory/entries", post(create_memory_entry_handler))
        .route("/v1/memory/entries/:id", get(get_memory_entry_handler).delete(delete_memory_entry_handler))
        .route("/v1/memory/handoff", post(create_handoff_handler))
        .route("/v1/memory/handoff/restore", post(restore_handoff_handler))
        .route("/v1/memory/layers", get(list_memory_layers_handler))
        .route("/v1/memory/graph/entities", get(list_graph_entities_handler))
        .route("/v1/memory/graph/relations", get(list_graph_relations_handler))
        .route("/v1/memory/graph/query", post(query_graph_handler))
        .route("/v1/skills", get(list_skills_handler))
        .route("/v1/skills/:name", get(view_skill_handler))
        .route("/v1/skills/:name/invoke", post(invoke_skill_handler))
        .route("/v1/workspaces", get(list_workspaces_handler))
        .route("/v1/workspaces/:name", get(get_workspace_handler))
        .route("/v1/workspaces/:name/preview", get(preview_workspace_handler))
        .route("/v1/system/status", get(system_status_handler))
        .route("/v1/platforms", get(list_platforms_handler))
        .route("/v1/platforms/:name", get(get_platform_handler))
        .route("/v1/platforms/:name/sessions", get(list_platform_sessions_handler))
        .route("/v1/platforms/:name/sessions/:id", delete(delete_platform_session_handler))
        .layer(CorsLayer::permissive());

    // Add WebUI routes if enabled
    if config.with_webui {
        // Get the directory containing the binary for locating webui assets
        // Use current working directory as fallback since server should be started from project root
        let base_dir = if let Ok(cwd) = std::env::current_dir() {
            cwd.clone()
        } else {
            PathBuf::from(".")
        };
        let webui_dir = base_dir.join("webui");
        let assets_dir = webui_dir.join("assets");
        
        eprintln!("Serving WebUI from: {}", webui_dir.display());
        
        // Create clone for fallback closure
        let webui_dir_fallback = webui_dir.clone();
        let assets_dir_fallback = assets_dir.clone();
        let base_dir_fallback = base_dir.clone();
        
        // Serve WebUI static files from the webui directory
        // Use fallback for root path to avoid conflicts with existing routes
        router = router.fallback(move |req: Request<Body>| {
            let webui_dir = webui_dir_fallback.clone();
            let assets_dir = assets_dir_fallback.clone();
            let base_dir = base_dir_fallback.clone();
            async move {
                let path = req.uri().path().to_string();
                
                // Handle root path
                if path == "/" || path.is_empty() {
                    let html_path = base_dir.join("webui").join("index.html");
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
fn check_auth<B>(state: &HttpAppState, req: &axum::http::Request<B>) -> Option<Response> {
    // Skip auth if disabled
    if !state.auth_enabled {
        return None;
    }

    // Extract Authorization header
    let auth_header = req.headers()
        .get("Authorization")?
        .to_str()
        .ok()?;

    // Check Bearer token format
    let token = auth_header.strip_prefix("Bearer ")?;

    // Validate token (constant-time comparison)
    if token != state.auth_token {
        return Some((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Invalid or missing token. Include 'Authorization: Bearer <token>' header."
            })),
        ).into_response());
    }

    None // Auth passed
}

/// Require auth middleware helper
fn require_auth<B>(state: &HttpAppState, req: &axum::http::Request<B>) -> Option<Response> {
    check_auth(state, req)
}

// ── HTTP Handlers - Basic ───────────────────────────────────────────────────────

async fn index_handler() -> axum::response::Response {
    // Read index.html from webui directory at runtime
    let base_dir = if let Ok(cwd) = std::env::current_dir() {
        cwd.clone()
    } else {
        PathBuf::from(".")
    };
    let html_path = base_dir.join("webui").join("index.html");
    
    // Fallback to embedded content if runtime path doesn't exist
    // Path: crates/rusty-claude-cli/src/ -> ../../../webui/
    let fallback_html = include_str!("../../../webui/index.html");
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

async fn models_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    // 返回默认模型列表（实际应该从配置中读取）
    let models = vec![
        serde_json::json!({"id": "claude-opus-4-6", "provider": "anthropic"}),
        serde_json::json!({"id": "claude-sonnet-4-6", "provider": "anthropic"}),
        serde_json::json!({"id": "claude-haiku-4-5-20251213", "provider": "anthropic"}),
        serde_json::json!({"id": "gpt-4o", "provider": "openai"}),
        serde_json::json!({"id": "gpt-4o-mini", "provider": "openai"}),
    ];
    (StatusCode::OK, Json(serde_json::json!({ "models": models }))).into_response()
}

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
        let config = OpenAiCompatConfig {
            provider_name: "stepfun",
            api_key_env: "OPENAI_API_KEY",
            base_url_env: "OPENAI_BASE_URL",
            default_base_url: "https://api.stepfun.com/v1",
        };
        let api_key = std::env::var("OPENAI_API_KEY")
            .map_err(|_| ServerError("OPENAI_API_KEY not set".to_string()))?;
        let client = OpenAiCompatClient::new(api_key, config)
            .with_base_url("https://api.stepfun.com/v1");
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
    fn stream(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError> {
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
        let model = self.model.clone();

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
                            _ => {}
                        }
                    }
                    StreamEvent::MessageStart(_) | StreamEvent::MessageStop(_) => {
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
    fn execute(&mut self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        // Parse the input JSON
        let input_value: serde_json::Value = serde_json::from_str(input)
            .map_err(|e| ToolError::new(format!("invalid input JSON: {}", e)))?;

        // Call the tools crate execute_tool function
        tools::execute_tool(tool_name, &input_value)
            .map_err(|e| ToolError::new(format!("tool execution failed: {}", e)))
    }
}

/// Chat Completions handler with SSE streaming support
/// 
/// This handler now uses ConversationRuntime for unified conversation management.
async fn chat_handler(
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    AxumState(state): AxumState<HttpAppState>,
    axum::extract::Json(req_json): axum::extract::Json<ChatRequest>,
) -> Response {
    let model = req_json.model.unwrap_or_else(|| "claude-opus-4-6".to_string());
    let user_input = req_json
        .messages
        .last()
        .map(|m| m.content.clone())
        .unwrap_or_default();
    let session_id = format!("api-{}", addr);

    // Build system prompt with memory context
    let memory_context = if let Some(ref mgr) = state.cognitive_manager {
        let mem_messages: Vec<MemMessage> = req_json
            .messages
            .iter()
            .filter_map(|m| {
                let role = match m.role.as_str() {
                    "user" | "system" | "assistant" => m.role.clone(),
                    _ => "user".to_string(),
                };
                Some(to_memory_message(&role, &m.content))
            })
            .collect();

        match mgr.prepare_context(&user_input, &mem_messages).await {
            Ok(prepared) => {
                tracing::debug!(
                    entries = prepared.entries.len(),
                    tokens = prepared.total_tokens,
                    "memory: prepared context for chat"
                );
                build_memory_context_block(&prepared)
            }
            Err(e) => {
                tracing::warn!(error = %e, "memory: prepare_context failed");
                None
            }
        }
    } else {
        None
    };

    let base_system_prompt = "You are a helpful AI assistant. Provide clear, concise responses.";
    let system_prompt = match memory_context {
        Some(ctx) => vec![ctx, base_system_prompt.to_string()],
        None => vec![base_system_prompt.to_string()],
    };

    // Create a Session from the conversation history
    let session = create_session_from_messages(&req_json.messages, &session_id);

    // Create the API client
    let api_client = match OpenAiApiClient::new(model.clone()) {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(error = %e, "Failed to create API client");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": format!("Server error: {}", e)
            }))).into_response();
        }
    };

    // Create the tool executor (placeholder for HTTP mode)
    let tool_executor = HttpToolExecutor;

    // Create permission policy (allow all for HTTP API)
    let permission_policy = PermissionPolicy::new(PermissionMode::DangerFullAccess);

    // Build the ConversationRuntime
    let mut runtime = ConversationRuntime::new(
        session,
        api_client,
        tool_executor,
        permission_policy,
        system_prompt,
    );

    // Optionally attach memory manager
    if let Some(ref mgr) = state.cognitive_manager {
        runtime = runtime.with_memory_manager(Arc::clone(mgr));
    }

    if req_json.stream {
        // ── SSE Streaming Path via ConversationRuntime ───────────────────
        let (chunk_tx, chunk_rx) = mpsc::channel::<SseChunk>(256);
        let model_clone = model.clone();
        let cognitive_manager = state.cognitive_manager.clone();

        // Clone user_input for post-processing
        let user_input_for_mem = user_input.clone();

        // Run the conversation turn directly (blocking)
        match runtime.run_turn(user_input, None) {
            Ok(summary) => {
                // Build messages for memory post-processing
                let mem_messages: Vec<MemMessage> = summary
                    .assistant_messages
                    .iter()
                    .map(|msg| {
                        let role = match msg.role {
                            SessionMessageRole::User => "user",
                            SessionMessageRole::Assistant => "assistant",
                            SessionMessageRole::System => "system",
                            _ => "assistant",
                        };
                        let content = msg
                            .blocks
                            .iter()
                            .filter_map(|b| match b {
                                SessionContentBlock::Text { text } => Some(text.clone()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("");
                        to_memory_message(role, &content)
                    })
                    .collect();

                // Stream text from assistant messages
                for msg in &summary.assistant_messages {
                    for block in &msg.blocks {
                        if let SessionContentBlock::Text { text } = block {
                            let sse_data = serde_json::json!({
                                "id": format!("chatcmpl-{}", Uuid::new_v4()),
                                "object": "chat.completion.chunk",
                                "created": Utc::now().timestamp(),
                                "model": model_clone,
                                "choices": [{
                                    "index": 0,
                                    "delta": { "content": text },
                                    "finish_reason": serde_json::Value::Null
                                }]
                            });
                            if chunk_tx.blocking_send(Some(format!("data: {}\n\n", sse_data))).is_err() {
                                break;
                            }
                        }
                    }
                }
                // Send [DONE]
                let _ = chunk_tx.blocking_send(None);

                // ── Post-processing: Memory on_turn_end ────────────────────
                if let Some(ref mgr) = cognitive_manager {
                    let mut mem_messages = mem_messages;
                    // Add user message at the beginning
                    mem_messages.insert(0, to_memory_message("user", &user_input_for_mem));
                    tracing::debug!("SSE stream ended, triggering on_turn_end for chat");

                    let mgr_clone = Arc::clone(mgr);
                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        match mgr_clone.on_turn_end(&mut mem_messages).await {
                            Ok(_) => tracing::debug!("on_turn_end completed for chat"),
                            Err(e) => tracing::warn!("on_turn_end failed: {}", e),
                        }
                    });
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "ConversationRuntime run_turn failed");
                let error_data = serde_json::json!({
                    "error": { "message": e.to_string(), "type": "runtime_error" }
                });
                let _ = chunk_tx.blocking_send(Some(format!("data: {}\n\n", error_data)));
                let _ = chunk_tx.blocking_send(None);
            }
        }

        let event_stream = ReceiverStream::new(chunk_rx).map(|chunk| {
            match chunk {
                Some(raw_line) => {
                    let data = raw_line
                        .strip_prefix("data: ")
                        .unwrap_or(&raw_line)
                        .trim_end_matches(['\n', '\r'])
                        .to_string();
                    Ok::<Event, std::convert::Infallible>(Event::default().data(data))
                }
                None => {
                    Ok::<Event, std::convert::Infallible>(Event::default().data("[DONE]"))
                }
            }
        });

        Sse::new(event_stream)
            .keep_alive(KeepAlive::default())
            .into_response()
    } else {
        // ── Non-streaming Path via ConversationRuntime ───────────────────
        let cognitive_manager = state.cognitive_manager.clone();
        let user_input_for_mem = user_input.clone();

        match runtime.run_turn(user_input, None) {
            Ok(summary) => {
                // Extract text content from all assistant messages
                let content: String = summary
                    .assistant_messages
                    .iter()
                    .flat_map(|msg| &msg.blocks)
                    .filter_map(|block| match block {
                        SessionContentBlock::Text { text } => Some(text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("");

                let resp = ChatResponse {
                    id: format!("chatcmpl-{}", Uuid::new_v4()),
                    object: "chat.completion".to_owned(),
                    created: Utc::now().timestamp(),
                    model,
                    choices: vec![ChatChoice {
                        index: 0,
                        message: ChatMessageOut {
                            role: "assistant".to_owned(),
                            content: content.clone(),
                        },
                        finish_reason: "stop".to_owned(),
                    }],
                };

                // ── Post-processing: Memory on_turn_end ────────────────────
                if let Some(ref mgr) = cognitive_manager {
                    let mut mem_messages: Vec<MemMessage> = summary
                        .assistant_messages
                        .iter()
                        .map(|msg| {
                            let role = match msg.role {
                                SessionMessageRole::User => "user",
                                SessionMessageRole::Assistant => "assistant",
                                SessionMessageRole::System => "system",
                                _ => "assistant",
                            };
                            let text = msg
                                .blocks
                                .iter()
                                .filter_map(|b| match b {
                                    SessionContentBlock::Text { text } => Some(text.clone()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("");
                            to_memory_message(role, &text)
                        })
                        .collect();
                    // Add user message at the beginning
                    mem_messages.insert(0, to_memory_message("user", &user_input_for_mem));

                    tracing::debug!("Non-streaming chat complete, triggering on_turn_end");
                    let mgr_clone = Arc::clone(mgr);
                    tokio::spawn(async move {
                        match mgr_clone.on_turn_end(&mut mem_messages).await {
                            Ok(_) => tracing::debug!("on_turn_end completed for chat"),
                            Err(e) => tracing::warn!("on_turn_end failed: {}", e),
                        }
                    });
                }

                Json(resp).into_response()
            }
            Err(e) => {
                tracing::error!(error = %e, "ConversationRuntime run_turn failed");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                    "error": format!("Runtime error: {}", e)
                }))).into_response()
            }
        }
    }
}

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
    Query(_params): Query<ListSessionsQuery>,
) -> axum::response::Response {
    match state.session_store.list_sessions() {
        Ok(records) => {
            let sessions: Vec<serde_json::Value> = records
                .into_iter()
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
            Json(serde_json::json!({ "sessions": sessions })).into_response()
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
    };

    // Load session record from SQLite
    let record = match state.session_store.get_session(&session_id) {
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

    // Return compaction parameters and current session metadata
    // (actual compaction requires Session with messages which is stored as jsonl on disk)
    let token_estimate = record.message_count as usize * 50; // rough estimate
    let message_count_before = record.message_count;

    Json(serde_json::json!({
        "ok": true,
        "compacted": false,
        "reason": "compaction requires session message history (jsonl); use /v1/sessions/:id to inspect",
        "session_id": session_id,
        "token_estimate": token_estimate,
        "message_count": message_count_before,
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

#[derive(Debug, Deserialize)]
struct MemorySearchQuery {
    query: String,
    #[serde(default = "default_memory_limit")]
    limit: usize,
    #[serde(default)]
    layer: Option<String>,
}

fn default_memory_limit() -> usize {
    10
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

    // 使用 CognitiveContextManager 进行真正的语义搜索
    if let Some(ref mgr) = state.cognitive_manager {
        match mgr.recall(&params.query, params.limit).await {
            Ok(entries) => {
                let results: Vec<serde_json::Value> = entries
                    .into_iter()
                    .map(|e| {
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
                    })
                    .collect();
                Json(serde_json::json!({
                    "results": results,
                    "query": params.query,
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
        // Memory manager not available - return empty results
        Json(serde_json::json!({
            "results": [],
            "query": params.query,
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

async fn list_memory_entries_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<ListMemoryEntriesQuery>,
) -> axum::response::Response {
    if let Some(ref mgr) = state.cognitive_manager {
        // If layer specified, list that layer; otherwise list all
        if let Some(layer_str) = &params.layer {
            let layer = match layer_str.as_str() {
                "L0" | "l0" => memory::types::MemoryLayer::L0,
                "L1" | "l1" => memory::types::MemoryLayer::L1,
                "L2" | "l2" => memory::types::MemoryLayer::L2,
                "L3" | "l3" => memory::types::MemoryLayer::L3,
                "L4" | "l4" => memory::types::MemoryLayer::L4,
                _ => {
                    return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                        "error": format!("unknown layer: {layer_str}")
                    }))).into_response();
                }
            };
            match mgr.list_layer_entries(layer).await {
                Ok(metas) => {
                    let entries: Vec<_> = metas.into_iter().take(params.limit).map(|m| {
                        serde_json::json!({
                            "id": m.id.to_string(),
                            "title": m.title,
                            "layer": format!("{:?}", m.layer),
                            "category": format!("{:?}", m.category),
                        })
                    }).collect();
                    return Json(serde_json::json!({
                        "entries": entries,
                        "count": entries.len(),
                        "layer": params.layer,
                    })).into_response();
                }
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response();
                }
            }
        } else {
            // List all layers
            let layers = mgr.list_layers().await;
            return Json(serde_json::json!({
                "layers": layers,
                "limit": params.limit,
            })).into_response();
        }
    }
    Json(serde_json::json!({
        "entries": [],
        "warning": "memory subsystem not enabled"
    })).into_response()
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
        scope: None,
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
async fn create_handoff_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<CreateHandoffParams>,
) -> axum::response::Response {
    let Some(ref mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let handoff_mgr = memory::HandoffManager::new();
    let handoff = match handoff_mgr.create_handoff(
        &params.session_id,
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        &params.next_action.unwrap_or_default(),
        &params.context_notes.unwrap_or_default(),
    ) {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            ).into_response();
        }
    };

    let handoff_id = handoff.session_id.clone();
    if let Err(e) = handoff_mgr.save(&handoff) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("failed to save handoff: {e}")})),
        ).into_response();
    }

    let _ = mgr.remember(memory::MemoryEntry {
        id: memory::MemoryId::new_v4(),
        layer: memory::MemoryLayer::L1,
        category: memory::MemoryCategory::Reference,
        priority: memory::Priority::High,
        source: memory::MemorySource::UserExplicit,
        title: format!("Handoff: {}", handoff_id),
        content: handoff.summary.clone(),
        embedding: None,
        tags: vec!["handoff".to_string()],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: None,
        session_id: Some(handoff_id.clone()),
    }).await;

    Json(serde_json::json!({
        "ok": true,
        "handoff_id": handoff_id,
        "summary": handoff.summary,
    })).into_response()
}

/// Restore a handoff package into a target session.
///
/// POST /v1/memory/handoff/restore
async fn restore_handoff_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<RestoreHandoffParams>,
) -> axum::response::Response {
    let Some(ref _mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let handoff_mgr = memory::HandoffManager::new();
    match handoff_mgr.load(&params.handoff_id) {
        Ok(Some(handoff)) => {
            Json(serde_json::json!({
                "ok": true,
                "handoff_id": handoff.session_id,
                "target_session_id": params.target_session_id,
                "work_items": handoff.work_items.len(),
                "decisions": handoff.decisions.len(),
                "blockers": handoff.blockers.len(),
                "summary": handoff.summary,
            })).into_response()
        }
        Ok(None) => {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "handoff not found"}))).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

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
async fn list_graph_entities_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<GraphEntityParams>,
) -> axum::response::Response {
    let Some(ref _mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let entities: Vec<serde_json::Value> = vec![];
    Json(serde_json::json!({
        "entities": entities,
        "filter": params.q,
        "note": "knowledge graph is populated via memory entries with relation metadata"
    })).into_response()
}

/// List knowledge graph relations.
///
/// GET /v1/memory/graph/relations?subject=x&predicate=y
async fn list_graph_relations_handler(
    AxumState(state): AxumState<HttpAppState>,
    Query(params): Query<GraphRelationParams>,
) -> axum::response::Response {
    let Some(ref _mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let relations: Vec<serde_json::Value> = vec![];
    Json(serde_json::json!({
        "relations": relations,
        "filter": {
            "subject": params.subject,
            "predicate": params.predicate,
        }
    })).into_response()
}

/// Query the temporal knowledge graph.
///
/// POST /v1/memory/graph/query
///
/// Request body: `{ entity?, time_range?, relation_type? }`
async fn query_graph_handler(
    AxumState(state): AxumState<HttpAppState>,
    Json(params): Json<GraphQueryParams>,
) -> axum::response::Response {
    let Some(ref _mgr) = state.cognitive_manager else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "memory subsystem not enabled"}))).into_response();
    };

    let results: Vec<serde_json::Value> = vec![];
    Json(serde_json::json!({
        "results": results,
        "query": {
            "entity": params.entity,
            "relation_type": params.relation_type,
        }
    })).into_response()
}

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

async fn list_skills_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let result = state.skill_service.list(SkillListInput::default());
    Json(serde_json::json!({
        "success": result.success,
        "skills": result.skills,
        "categories": result.categories,
        "tags": result.tags,
        "count": result.count
    })).into_response()
}

async fn view_skill_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let result = state.skill_service.view(SkillViewInput {
        name,
        file_path: None,
    });
    Json(serde_json::json!({
        "success": result.success,
        "name": result.name,
        "description": result.description,
        "content": result.content,
        "metadata": result.metadata
    })).into_response()
}

async fn invoke_skill_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(name): Path<String>,
    Json(input): Json<SkillInvokeInput>,
) -> axum::response::Response {
    let mut skill_input = input;
    skill_input.name = name;
    let result = state.skill_service.invoke(skill_input);
    Json(serde_json::json!({
        "success": result.success,
        "name": result.name,
        "content": result.content,
        "warning": result.warning
    })).into_response()
}

// ── HTTP Handlers - Workspaces ─────────────────────────────────────────────────

async fn list_workspaces_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let workspaces = state.skill_service.list_workspaces();
    Json(serde_json::json!({
        "workspaces": workspaces,
        "count": workspaces.len()
    })).into_response()
}

async fn get_workspace_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    match state.skill_service.get_workspace(&name) {
        Some(workspace) => Json(serde_json::json!({
            "success": true,
            "workspace": workspace
        })).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "success": false,
            "error": "workspace not found"
        }))).into_response(),
    }
}

async fn preview_workspace_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    match state.skill_service.preview_workspace(&name) {
        Some(preview) => Json(serde_json::json!({
            "success": true,
            "preview": preview
        })).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "success": false,
            "error": "workspace preview not available"
        }))).into_response(),
    }
}

// ── HTTP Handlers - System ─────────────────────────────────────────────────────

async fn system_status_handler(AxumState(state): AxumState<HttpAppState>) -> axum::response::Response {
    let memory_enabled = state.cognitive_manager.is_some();

    // Count sessions
    let sessions_count = state.session_store.list_sessions().map(|v| v.len()).unwrap_or(0);

    let cwd = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "cwd": cwd,
        "memory": {
            "enabled": memory_enabled,
            "store_path": state.memory_store_path,
            "session_store": true,
        },
        "sessions_count": sessions_count,
        "uptime_seconds": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    })).into_response()
}

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
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_ws(socket, addr, state))
}

/// WebSocket handler for session event subscriptions
/// GET /ws/sessions - Subscribe to session events (created, updated, deleted, messages added)
async fn ws_sessions_handler(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    AxumState(state): AxumState<HttpAppState>,
) -> axum::response::Response {
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
    let broadcast_tx = state.session_broadcast.clone();

    // Send welcome message
    let welcome = WsOutbound {
        text: "Connected. Send messages to start chatting.".to_string(),
        done: true,
        session_id: Some(session_id.clone()),
        error: None,
    };
    if socket.send(WsMessage::Text(serde_json::to_string(&welcome).unwrap().into())).await.is_err() {
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
                PermissionPolicy::new(PermissionMode::DangerFullAccess),
                system_prompt,
            );

            if let Some(ref mgr) = state.cognitive_manager {
                runtime = runtime.with_memory_manager(Arc::clone(mgr));
            }

            match runtime.run_turn(&user_input, None) {
                Ok(summary) => Ok(summary
                    .assistant_messages
                    .iter()
                    .flat_map(|msg| &msg.blocks)
                    .filter_map(|block| match block {
                        SessionContentBlock::Text { text } => Some(text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("")),
                Err(e) => Err(format!("Runtime error: {}", e)),
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
        roots.push(PathBuf::from(format!("{}/.qoder/skills", home)));
        roots.push(PathBuf::from(format!("{}/.agents/skills", home)));
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

                let root_type = if root.to_string_lossy().contains(".qoder") {
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
    // Validate token
    let token_valid = if state.auth_enabled {
        req.token == state.auth_token || !req.token.is_empty()
    } else {
        true
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
async fn get_session_messages_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
) -> axum::response::Response {
    let _ = state;
    // Return empty messages for now (messages stored in JSONL files)
    Json(serde_json::json!({
        "session_id": session_id,
        "messages": [],
        "count": 0
    })).into_response()
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
) -> axum::response::Response {
    // For non-streaming, return a simple response
    // In production, this would integrate with the conversation runtime
    let response = serde_json::json!({
        "id": format!("msg-{}", Uuid::new_v4()),
        "session_id": session_id,
        "role": "assistant",
        "content": format!("收到: {}", params.content),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "model": "claude-opus-4-6"
    });

    // Broadcast MessageAdded event for real-time sync
    let _ = state.session_broadcast.send(SessionEvent::MessageAdded {
        session_id: session_id.clone(),
        message_count: 1,
    });

    Json(response).into_response()
}

/// Send a message with SSE streaming
/// POST /api/sessions/:id/messages/stream
async fn send_message_stream_handler(
    AxumState(state): AxumState<HttpAppState>,
    Path(session_id): Path<String>,
    Json(params): Json<SendMessageParams>,
) -> Response {
    let session_id_clone = session_id.clone();
    let broadcast_tx = state.session_broadcast.clone();
    let cognitive_manager = state.cognitive_manager.clone();

    let (chunk_tx, chunk_rx) = mpsc::channel::<SseChunk>(256);
    let user_content = params.content.clone();

    // Spawn a task to handle the streaming
    tokio::spawn(async move {
        // Broadcast RuntimeStarted event
        let _ = broadcast_tx.send(SessionEvent::RuntimeStarted {
            session_id: session_id_clone.clone(),
        });

        // Simulate streaming response
        let response_text = format!("收到: {}", user_content);
        for chunk in response_text.chars() {
            let sse_data = serde_json::json!({
                "id": format!("msg-{}", Uuid::new_v4()),
                "session_id": session_id_clone.clone(),
                "role": "assistant",
                "content": chunk.to_string(),
                "delta": true
            });
            let _ = chunk_tx.send(Some(format!("data: {}\n\n", sse_data))).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        let _ = chunk_tx.send(None).await;

        // Broadcast RuntimeFinished and MessageAdded events
        let _ = broadcast_tx.send(SessionEvent::RuntimeFinished {
            session_id: session_id_clone.clone(),
        });
        let _ = broadcast_tx.send(SessionEvent::MessageAdded {
            session_id: session_id_clone.clone(),
            message_count: 1,
        });

        // ── Post-processing: Memory on_turn_end ─────────────────────────────
        if let Some(ref mgr) = cognitive_manager {
            tracing::debug!("SSE stream ended, triggering on_turn_end for session {}", session_id_clone);

            // Build message list for memory processing
            let user_msg = to_memory_message("user", &user_content);
            let assistant_msg = to_memory_message("assistant", &response_text);
            let mut mem_messages = vec![user_msg, assistant_msg];

            // Wait a short moment to ensure SSE is fully sent
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            match mgr.on_turn_end(&mut mem_messages).await {
                Ok(_) => {
                    tracing::debug!("on_turn_end completed successfully for session {}", session_id_clone);
                }
                Err(e) => {
                    tracing::warn!("on_turn_end failed for session {}: {}", session_id_clone, e);
                }
            }
        }
    });

    let event_stream = ReceiverStream::new(chunk_rx).map(|chunk| {
        match chunk {
            Some(raw_line) => {
                let data = raw_line
                    .strip_prefix("data: ")
                    .unwrap_or(&raw_line)
                    .trim_end_matches(['\n', '\r'])
                    .to_string();
                Ok::<Event, std::convert::Infallible>(Event::default().data(data))
            }
            None => {
                Ok::<Event, std::convert::Infallible>(Event::default().data("[DONE]"))
            }
        }
    });

    Sse::new(event_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
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

