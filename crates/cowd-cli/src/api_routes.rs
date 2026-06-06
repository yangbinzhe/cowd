// ── API Routes (T7) — Minimal API using ActiveSessions ────────
// Core session management routes shared between TUI and HTTP API.
// DO NOT delete old server/mod.rs yet (T16 will do that).

use std::{
    collections::HashMap,
    convert::Infallible,
    fs,
    path::{Component, Path as FsPath, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State as AxumState},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Json,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::StreamExt;
use futures::stream::Stream;
use runtime::approval_gate::SmartApprovalGate;
use runtime::permission_enforcer::{ApprovalPersistence, ApprovalVerdict};
use runtime::{
    ApprovalConfig, ContextAuthority, ContextEnvelopeRequest, ContextIdentity, ContextItem,
    ContextMode, ContextOmission, ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind,
    ContextVisibility, ResumeContextPacket, ResumeContextSource,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_stream::wrappers::ReceiverStream;

use runtime::ProfileManager;
use tools::GlobalToolRegistry;

use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use crate::session_kernel::SessionKernel;
use crate::task_kernel::{TaskKernel, TaskRecord, TaskStatus};
use memory::RotAlert;
use memory::cognitive::CognitiveContextManager;
use memory::session_store::UnifiedSessionStore;
use memory::store::session::{SessionEvent, SessionListOptions, SessionRecord};
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::{
    MaintenanceCandidateFilter, MaintenanceCandidateKind, MaintenanceCandidateStatus,
    MaintenanceScanConfig, MemoryKernel, MemoryScope, MemoryTurnContext, SearchMemoriesRequest,
};

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub session_kernel: Arc<SessionKernel>,
    /// Compatibility fields retained while API/TUI callers migrate to
    /// `session_kernel`.
    pub sessions: Arc<ActiveSessions>,
    pub memory_manager: Option<Arc<CognitiveContextManager>>,
    pub unified_store: Option<Arc<UnifiedSessionStore>>,
    pub tool_registry: Arc<GlobalToolRegistry>,
    pub config: Option<serde_json::Value>,
    pub event_bus: Arc<SessionEventBus>,
    pub approval_gate: Option<Arc<SmartApprovalGate>>,
    pub auth_token: Option<String>,
    pub workspace_root: PathBuf,
    pub config_home: PathBuf,
    pub profile_id: String,
    pub profile_manager: Arc<ProfileManager>,
    pub task_kernel: Arc<TaskKernel>,
}

type RuntimeEntry = Arc<tokio::sync::Mutex<crate::BuiltRuntime>>;

impl AppState {
    fn unified_store(&self) -> Option<Arc<UnifiedSessionStore>> {
        self.session_kernel.unified_store()
    }

    fn has_unified_store(&self) -> bool {
        self.session_kernel.has_unified_store()
    }

    fn event_bus(&self) -> Arc<SessionEventBus> {
        self.session_kernel.event_bus()
    }

    fn list_active_session_ids(&self) -> Vec<String> {
        self.session_kernel.list_active_session_ids()
    }

    fn active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.session_kernel.active_runtime(session_id)
    }

    fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::BuiltRuntime,
    ) -> Result<Option<RuntimeEntry>, String> {
        self.session_kernel.register_runtime(session_id, runtime)
    }

    fn remove_active_runtime(&self, session_id: &str) -> Option<RuntimeEntry> {
        self.session_kernel.remove_active_runtime(session_id)
    }
}

// ── Auth middleware ────────────────────────────────────────────

/// If `auth_token` is set on `AppState`, require `Authorization: Bearer <token>`.
/// When `auth_token` is `None`, all requests are allowed (no auth configured).
async fn auth_middleware(
    AxumState(state): AxumState<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if let Some(token) = &state.auth_token {
        let auth_header = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v: &axum::http::HeaderValue| v.to_str().ok());

        match auth_header {
            Some(h) if h == format!("Bearer {token}") => Ok(next.run(request).await),
            _ => Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )),
        }
    } else {
        Ok(next.run(request).await)
    }
}

// ── Router ─────────────────────────────────────────────────────

pub fn api_router(state: Arc<AppState>) -> Router {
    let public_routes = Router::new()
        .route("/health", get(health_handler))
        .route("/api/auth/login", post(login_handler))
        .route("/api/auth/verify", get(verify_handler));

    let protected_routes = Router::new()
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route("/api/sessions/search", get(search_messages_handler))
        .route(
            "/api/sessions/:id",
            get(get_session)
                .patch(update_session_handler)
                .delete(delete_session),
        )
        .route(
            "/api/sessions/:id/messages",
            get(get_session_messages).post(send_message),
        )
        .route("/api/sessions/:id/events", get(get_session_events))
        .route("/api/sessions/:id/runs", get(get_session_runs))
        .route(
            "/api/sessions/:id/context",
            get(get_session_context_history),
        )
        .route(
            "/api/sessions/:id/context/recommendations",
            get(get_context_recommendation_stats).post(record_context_recommendation_action),
        )
        .route("/api/sessions/:id/stream", get(sse_stream_handler))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/stats", get(get_session_stats_handler))
        .route("/api/context/current", get(context_current_handler))
        .route(
            "/api/context/:envelope_id",
            get(get_context_envelope_handler),
        )
        .route("/api/evidence/resolve", get(resolve_evidence_ref_handler))
        .route("/api/tasks", get(tasks_status_handler))
        .route("/api/tasks/start", post(start_task_handler))
        .route("/api/tasks/:id/phases", post(start_task_phase_handler))
        .route(
            "/api/tasks/:id/phases/:phase_id/artifacts",
            post(record_task_phase_artifact_handler),
        )
        .route(
            "/api/tasks/:id/phases/:phase_id/review",
            post(review_task_phase_handler),
        )
        .route("/api/tasks/:id/cancel", post(cancel_task_handler))
        .route("/api/tasks/:id/complete", post(complete_task_handler))
        .route("/api/tasks/:id/failure", post(record_task_failure_handler))
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/status", get(memory_status_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route(
            "/api/memory/recall/explain",
            get(memory_recall_explain_handler),
        )
        .route("/api/memory/packet", get(memory_packet_handler))
        .route("/api/memory/links", get(memory_links_handler))
        .route("/api/memory/stats", get(memory_stats_handler))
        .route("/api/memory/layers", get(memory_layers_handler))
        .route(
            "/api/memory/maintenance",
            get(memory_maintenance_handler).post(scan_memory_maintenance_handler),
        )
        .route(
            "/api/memory/maintenance/:id",
            axum::routing::patch(update_memory_maintenance_handler),
        )
        .route("/api/memory/entities", get(memory_entities_handler))
        .route("/api/memory/triples", get(memory_triples_handler))
        .route(
            "/api/memory/symbol-links",
            get(memory_symbol_links_handler).post(create_memory_symbol_link_handler),
        )
        .route("/api/memory/performance", get(performance_handler))
        .route(
            "/api/memory/:layer",
            get(memory_layer_handler).post(create_memory_entry_handler),
        )
        .route(
            "/api/memory/:layer/:id",
            axum::routing::delete(delete_memory_entry_handler),
        )
        .route(
            "/api/memory/entry/:id",
            axum::routing::patch(update_memory_entry_handler),
        )
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
        .route(
            "/api/profiles",
            get(profiles_handler).post(create_profile_handler),
        )
        .route("/api/profiles/switch", post(switch_profile_handler))
        .route(
            "/api/profiles/:id",
            axum::routing::delete(delete_profile_handler),
        )
        .route("/api/workspace", get(workspace_handler))
        .route("/api/workspaces", get(workspaces_handler))
        .route(
            "/api/workspace/files",
            get(workspace_files_handler).post(create_workspace_file_handler),
        )
        .route("/api/file/raw", get(raw_workspace_file_handler))
        .route("/api/approval/pending", get(approval_pending_handler))
        .route("/api/approval/respond", post(approval_respond_handler))
        .route(
            "/api/approval/config",
            get(approval_config_handler).put(update_approval_config_handler),
        )
        .route("/api/approval/solo", post(toggle_solo_handler))
        .route("/api/approval/history", get(approval_history_handler))
        .route("/api/audit/export", get(audit_export_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    public_routes.merge(protected_routes).with_state(state)
}

// ── Response types ─────────────────────────────────────────────

#[derive(Serialize)]
struct SessionInfo {
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens: Option<i64>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
struct CreateSessionRequest {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct SendMessageRequest {
    content: String,
}

#[derive(Deserialize)]
struct ContextRecommendationActionRequest {
    envelope_id: String,
    recommendation: String,
    #[serde(default = "default_context_recommendation_action")]
    action: String,
    #[serde(default)]
    note: Option<String>,
}

fn default_context_recommendation_action() -> String {
    "acknowledged".to_string()
}

#[derive(Deserialize)]
struct LoginRequest {
    token: String,
}

#[derive(Deserialize)]
struct CreateMemoryEntryRequest {
    content: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct UpdateMemoryEntryRequest {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    priority: Option<String>,
}

#[derive(Deserialize)]
struct MemoryMaintenanceQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct MemoryMaintenanceScanRequest {
    #[serde(default)]
    stale_threshold: Option<f32>,
    #[serde(default)]
    low_confidence_threshold: Option<f32>,
    #[serde(default)]
    authority_confidence_threshold: Option<f32>,
    #[serde(default)]
    max_candidates: Option<usize>,
}

#[derive(Deserialize)]
struct UpdateMemoryMaintenanceRequest {
    status: String,
}

#[derive(Deserialize)]
struct CreateSymbolLinkRequest {
    symbol_id: String,
    memory_id: String,
    #[serde(default)]
    turn_index: Option<i32>,
    #[serde(default)]
    reference_type: Option<String>,
}

// ── Query param types ────────────────────────────────────────────

#[derive(Deserialize)]
struct ListSessionsParams {
    #[serde(default = "default_sort")]
    sort: String,
    #[serde(default = "default_order")]
    order: String,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

fn default_sort() -> String {
    "updated_at".to_string()
}
fn default_order() -> String {
    "desc".to_string()
}

#[derive(Deserialize)]
struct GetMessagesParams {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct GetEventsParams {
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct GetRecommendationStatsParams {
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SearchMessagesParams {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

fn default_search_limit() -> usize {
    20
}

#[derive(Deserialize)]
struct WorkspaceFilesParams {
    #[serde(default)]
    dir: Option<String>,
}

#[derive(Deserialize)]
struct RawFileParams {
    path: String,
}

#[derive(Deserialize)]
struct CreateWorkspaceFileRequest {
    path: String,
    #[serde(default)]
    content: String,
}

#[derive(Deserialize)]
struct CreateProfileRequest {
    name: String,
}

#[derive(Deserialize)]
struct SwitchProfileRequest {
    profile: String,
}

#[derive(Serialize)]
struct WorkspaceFileItem {
    name: String,
    path: String,
    is_dir: bool,
    #[serde(rename = "type")]
    kind: String,
    size: u64,
    modified_ms: Option<u128>,
}

fn default_config_home() -> PathBuf {
    std::env::var_os("COWD_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| PathBuf::from(".cowd"))
}

fn path_has_safe_relative_components(path: &FsPath) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn workspace_root_canonical(workspace_root: &FsPath) -> Result<PathBuf, String> {
    workspace_root
        .canonicalize()
        .map_err(|e| format!("workspace root is unavailable: {e}"))
}

fn resolve_existing_workspace_path(
    workspace_root: &FsPath,
    relative: Option<&str>,
) -> Result<PathBuf, String> {
    let root = workspace_root_canonical(workspace_root)?;
    let rel = relative.map(str::trim).unwrap_or("");
    let rel_path = FsPath::new(rel);
    if !rel.is_empty() && !path_has_safe_relative_components(rel_path) {
        return Err("path must stay inside the workspace".to_string());
    }
    let candidate = if rel.is_empty() {
        root.clone()
    } else {
        root.join(rel_path)
    };
    let resolved = candidate
        .canonicalize()
        .map_err(|e| format!("path not found: {e}"))?;
    if !resolved.starts_with(&root) {
        return Err("path must stay inside the workspace".to_string());
    }
    Ok(resolved)
}

fn resolve_new_workspace_file_path(
    workspace_root: &FsPath,
    relative: &str,
) -> Result<PathBuf, String> {
    let root = workspace_root_canonical(workspace_root)?;
    let rel = relative.trim();
    if rel.is_empty() {
        return Err("path is required".to_string());
    }
    let rel_path = FsPath::new(rel);
    if !path_has_safe_relative_components(rel_path) {
        return Err("path must stay inside the workspace".to_string());
    }
    let target = root.join(rel_path);
    let parent = target
        .parent()
        .ok_or_else(|| "file parent is unavailable".to_string())?;
    fs::create_dir_all(parent).map_err(|e| format!("failed to create parent directory: {e}"))?;
    let parent_resolved = parent
        .canonicalize()
        .map_err(|e| format!("file parent is unavailable: {e}"))?;
    if !parent_resolved.starts_with(&root) {
        return Err("path must stay inside the workspace".to_string());
    }
    Ok(target)
}

fn workspace_relative_path(root: &FsPath, path: &FsPath) -> String {
    path.strip_prefix(root)
        .ok()
        .and_then(|p| p.to_str())
        .unwrap_or("")
        .replace('\\', "/")
}

fn workspace_file_item(root: &FsPath, path: PathBuf) -> Option<WorkspaceFileItem> {
    let metadata = fs::metadata(&path).ok()?;
    let name = path.file_name()?.to_string_lossy().to_string();
    let is_dir = metadata.is_dir();
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis());
    Some(WorkspaceFileItem {
        name,
        path: workspace_relative_path(root, &path),
        is_dir,
        kind: if is_dir { "dir" } else { "file" }.to_string(),
        size: if is_dir { 0 } else { metadata.len() },
        modified_ms,
    })
}

fn session_title_from_metadata(metadata_json: Option<&str>) -> Option<String> {
    metadata_json
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| {
            v.get("title")
                .and_then(|t| t.as_str())
                .map(ToString::to_string)
        })
}

fn session_info_from_record(record: SessionRecord) -> SessionInfo {
    SessionInfo {
        id: record.session_id,
        status: record.status,
        title: session_title_from_metadata(record.metadata_json.as_deref()),
        model: record.model,
        created_at: Some(record.created_at),
        updated_at: Some(record.last_activity),
        input_tokens: Some(record.input_tokens),
        output_tokens: Some(record.output_tokens),
    }
}

fn active_session_info(id: String) -> SessionInfo {
    SessionInfo {
        id,
        status: "active".to_string(),
        title: None,
        model: None,
        created_at: None,
        updated_at: None,
        input_tokens: None,
        output_tokens: None,
    }
}

pub(crate) fn new_api_session_record(session_id: &str, model: Option<String>) -> SessionRecord {
    let now = chrono::Utc::now().to_rfc3339();
    let title = format!("Session {}", session_id.chars().take(8).collect::<String>());
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "api_server".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(serde_json::json!({ "title": title }).to_string()),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

pub(crate) async fn sync_runtime_session_metadata_to_store(
    store: &UnifiedSessionStore,
    session_id: &str,
    session: &runtime::Session,
) -> Result<(), String> {
    let now = chrono::Utc::now().to_rfc3339();
    let existing_record = store
        .get_session(session_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut record = existing_record
        .clone()
        .unwrap_or_else(|| new_api_session_record(session_id, session.model.clone()));

    record.model = session.model.clone().or(record.model);
    record.last_activity = now;
    record.message_count = session.messages.len() as i64;
    record.input_tokens = session
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| i64::from(u.input_tokens))
        .sum();
    record.output_tokens = session
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| i64::from(u.output_tokens))
        .sum();

    if existing_record.is_some() {
        store
            .update_session(&record)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        store
            .create_session(&record)
            .await
            .map_err(|e| e.to_string())?;
    }

    store
        .delete_messages_from(session_id, 0)
        .await
        .map_err(|e| e.to_string())?;
    store
        .delete_events_by_type_from(session_id, "message_appended", 0)
        .await
        .map_err(|e| e.to_string())?;

    for (sequence, message) in session.messages.iter().enumerate() {
        let message_record = message.to_session_message(session_id, sequence);
        store
            .insert_message(&message_record)
            .await
            .map_err(|e| e.to_string())?;

        let message_json = serde_json::from_str::<serde_json::Value>(&message.to_json().render())
            .unwrap_or(serde_json::Value::Null);
        let event = memory::SessionEvent {
            session_id: session_id.to_string(),
            event_type: "message_appended".to_string(),
            event_json: serde_json::json!({
                "type": "message_appended",
                "sequence": sequence,
                "role": message.role.role_str(),
                "message": message_json,
            })
            .to_string(),
            sequence,
            created_at_ms: message_record.created_at_ms,
        };
        store
            .append_event(&event)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// ── Search response types ────────────────────────────────────────

#[derive(Serialize)]
struct SearchMessagesItem {
    session_id: String,
    sequence: usize,
    role: String,
    blocks: Vec<serde_json::Value>,
    content_preview: String,
    tool_use_id: Option<String>,
    tool_name: Option<String>,
    created_at_ms: u64,
}

#[derive(Serialize)]
struct SearchMessagesResponse {
    query: String,
    results: Vec<SearchMessagesItem>,
    total: usize,
}

#[derive(Deserialize)]
struct UpdateSessionRequest {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ApprovalRespondRequest {
    id: String,
    approved: bool,
    #[serde(default)]
    persistence: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct StartTaskRequest {
    objective: String,
    #[serde(default)]
    yolo_mode: bool,
}

#[derive(Deserialize)]
struct TaskFailureRequest {
    reason: String,
}

#[derive(Deserialize)]
struct StartTaskPhaseRequest {
    name: String,
    objective: String,
    #[serde(default)]
    plan: Vec<String>,
    #[serde(default)]
    acceptance: Vec<String>,
    #[serde(default)]
    test_commands: Vec<String>,
}

#[derive(Deserialize)]
struct TaskPhaseArtifactRequest {
    #[serde(default = "default_task_artifact_kind")]
    kind: String,
    label: String,
    value: String,
}

#[derive(Deserialize)]
struct TaskPhaseReviewRequest {
    result: String,
    #[serde(default)]
    completed: bool,
}

fn default_task_artifact_kind() -> String {
    "note".to_string()
}

// ── Handlers ───────────────────────────────────────────────────

async fn health_handler() -> &'static str {
    "OK"
}

async fn tasks_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "tasks": state.task_kernel.list(),
        "current": state.task_kernel.current(),
    }))
}

async fn context_current_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next())
        .unwrap_or_else(|| "api-context".to_string());
    let query = params.get("q").cloned().unwrap_or_default();
    let profile = params
        .get("profile")
        .and_then(|value| parse_context_profile(value))
        .unwrap_or(ContextProfile::MainTurn);

    if let Some(runtime_entry) = state.active_runtime(&session_id) {
        let runtime = runtime_entry.lock().await;
        if let Some(envelope) = runtime.last_context_envelope() {
            let lean_probe = ContextRuntimeKernel::lean_probe(&envelope);
            let policy_decision = ContextRuntimeKernel::policy_decision(&lean_probe);
            return Json(serde_json::json!({
                "enabled": true,
                "source": "runtime",
                "envelope": envelope,
                "lean_probe": lean_probe,
                "policy_decision": policy_decision,
            }));
        }
    }

    let mut identity = ContextIdentity::main(session_id.clone());
    identity.mode = context_mode_for_profile(profile);
    let mut dynamic_items = Vec::new();
    let mut omitted_items = Vec::new();
    let mut degraded = Vec::new();

    if let Some(ref mgr) = state.memory_manager {
        let mgr = Arc::clone(mgr);
        let session_for_packet = session_id.clone();
        let query_for_packet = query.clone();
        let packet_result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| err.to_string())?;
            rt.block_on(async move {
                let kernel = MemoryKernel::new(mgr);
                let memory_ctx = MemoryTurnContext::new(session_for_packet, "api");
                kernel
                    .context_packet(&memory_ctx, &query_for_packet, &[], 12, 2_000)
                    .await
                    .map_err(|err| err.to_string())
            })
        })
        .await
        .map_err(|err| err.to_string())
        .and_then(|result| result);

        match packet_result {
            Ok(packet) => {
                for item in packet.selected {
                    let mut context_item = ContextItem::new(
                        item.atom.id.to_string(),
                        ContextSourceKind::Memory,
                        match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        },
                        format!(
                            "{}\nreason: {}\nevidence: {}",
                            item.atom.title,
                            item.reason,
                            item.atom.evidence_pointer.as_deref().unwrap_or("")
                        ),
                    );
                    context_item.authority = ContextAuthority::Session;
                    context_item.visibility = ContextVisibility::Private;
                    context_item.score = item.atom.confidence;
                    dynamic_items.push(context_item);
                }
                for omitted in packet.omitted {
                    omitted_items.push(ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    });
                }
            }
            Err(_) => degraded.push(ContextSourceKind::Memory),
        }
    } else {
        degraded.push(ContextSourceKind::Memory);
    }

    let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
        profile,
        identity,
        intent: query,
        stable_head: vec!["cowd-context-runtime:v0.8.13".to_string()],
        runtime_header: vec![format!("session:{session_id} agent:api profile:{profile:?}")],
        dynamic_items,
        omitted: omitted_items,
        total_budget_tokens: 8_000,
    });
    envelope.diagnostics.degraded_sources = degraded;
    let lean_probe = ContextRuntimeKernel::lean_probe(&envelope);
    let policy_decision = ContextRuntimeKernel::policy_decision(&lean_probe);

    Json(serde_json::json!({
        "enabled": true,
        "source": "synthetic",
        "lean_probe": lean_probe,
        "policy_decision": policy_decision,
        "envelope": envelope,
    }))
}

fn parse_context_profile(value: &str) -> Option<ContextProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mainturn" | "main" => Some(ContextProfile::MainTurn),
        "sologoal" | "solo" => Some(ContextProfile::SoloGoal),
        "yologoal" | "yolo" => Some(ContextProfile::YoloGoal),
        "subagent" | "sub_agent" => Some(ContextProfile::SubAgent),
        "collaboration" => Some(ContextProfile::Collaboration),
        "review" => Some(ContextProfile::Review),
        "resume" => Some(ContextProfile::Resume),
        "cron" => Some(ContextProfile::Cron),
        _ => None,
    }
}

fn context_mode_for_profile(profile: ContextProfile) -> ContextMode {
    match profile {
        ContextProfile::MainTurn => ContextMode::MainTurn,
        ContextProfile::SoloGoal => ContextMode::SoloGoal,
        ContextProfile::YoloGoal => ContextMode::YoloGoal,
        ContextProfile::SubAgent => ContextMode::SubAgent,
        ContextProfile::Collaboration => ContextMode::Collaboration,
        ContextProfile::Review => ContextMode::Review,
        ContextProfile::Resume => ContextMode::Resume,
        ContextProfile::Cron => ContextMode::Cron,
    }
}

async fn start_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<StartTaskRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .start_goal(body.objective, body.yolo_mode)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn start_task_phase_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<StartTaskPhaseRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .start_phase(
            &id,
            body.name,
            body.objective,
            body.plan,
            body.acceptance,
            body.test_commands,
        )
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn record_task_phase_artifact_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, phase_id)): Path<(String, String)>,
    Json(body): Json<TaskPhaseArtifactRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .record_phase_artifact(&id, &phase_id, body.kind, body.label, body.value)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(task))
}

async fn review_task_phase_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, phase_id)): Path<(String, String)>,
    Json(body): Json<TaskPhaseReviewRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .review_phase(&id, &phase_id, body.result, body.completed)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(task))
}

async fn cancel_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .transition(&id, TaskStatus::Cancelled, None, "cancelled by user")
        .map_err(|e| api_error(StatusCode::NOT_FOUND, e))?;
    Ok(Json(task))
}

async fn complete_task_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .transition(&id, TaskStatus::Completed, None, "accepted")
        .map_err(|e| api_error(StatusCode::NOT_FOUND, e))?;
    Ok(Json(task))
}

async fn record_task_failure_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TaskFailureRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_kernel
        .record_failure(&id, body.reason)
        .map_err(|e| api_error(StatusCode::NOT_FOUND, e))?;
    Ok(Json(task))
}

async fn list_sessions(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<ListSessionsParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20).min(200);
    let offset = params.offset.unwrap_or(0);

    // Try unified store first for DB-backed listing
    if let Ok(Some(page)) = state
        .session_kernel
        .list_stored_sessions_page(&SessionListOptions {
            query: params.q.as_deref(),
            model: params.model.as_deref(),
            status: params.status.as_deref(),
            sort: &params.sort,
            order: &params.order,
            limit,
            offset,
        })
        .await
    {
        let total = page.total;
        let sessions: Vec<SessionInfo> = page
            .records
            .into_iter()
            .map(session_info_from_record)
            .collect();
        return Json(serde_json::json!({
            "sessions": sessions,
            "total": total,
            "offset": offset,
            "limit": limit,
            "sort": params.sort,
            "order": params.order,
        }));
    }

    // Fallback: in-memory active sessions
    let mut sessions: Vec<SessionInfo> = state
        .list_active_session_ids()
        .into_iter()
        .map(active_session_info)
        .collect();
    if let Some(status) = params.status.as_ref().filter(|s| !s.is_empty()) {
        sessions.retain(|s| s.status.eq_ignore_ascii_case(status));
    }
    if let Some(model) = params.model.as_ref().filter(|s| !s.is_empty()) {
        sessions.retain(|s| {
            s.model
                .as_deref()
                .is_some_and(|m| m.eq_ignore_ascii_case(model))
        });
    }
    if let Some(q) = params
        .q
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
    {
        sessions.retain(|s| {
            s.id.to_lowercase().contains(&q)
                || s.title.as_deref().unwrap_or("").to_lowercase().contains(&q)
        });
    }
    if params.sort == "created_at" {
        sessions.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    } else {
        sessions.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
    }
    if !params.order.eq_ignore_ascii_case("asc") {
        sessions.reverse();
    }
    let total = sessions.len();
    let sessions: Vec<SessionInfo> = sessions.into_iter().skip(offset).take(limit).collect();
    Json(serde_json::json!({
        "sessions": sessions,
        "total": total,
        "offset": offset,
        "limit": limit,
        "sort": params.sort,
        "order": params.order,
    }))
}

async fn create_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested");

    let session = runtime::Session::new();
    let model = body
        .model
        .unwrap_or_else(|| "claude-sonnet-4-6".to_string());
    let runtime = if let Some(store) = state.unified_store() {
        crate::build_runtime_with_session_store(
            store.clone(),
            session,
            &session_id,
            model.clone(),
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
            model.clone(),
            vec![],
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    }
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("failed to build runtime: {e}"),
            }),
        )
    })?;

    if let Err(e) = state.register_runtime(session_id.clone(), runtime) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("failed to register session: {e}"),
            }),
        ));
    }

    let mut info = active_session_info(session_id.clone());
    if state.has_unified_store() {
        let record = new_api_session_record(&session_id, Some(model));
        state
            .session_kernel
            .upsert_stored_session(&record)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to persist session: {e}"),
                    }),
                )
            })?;
        info = session_info_from_record(record);
    }

    Ok((StatusCode::CREATED, Json(info)))
}

async fn get_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.has_unified_store() {
        match state.session_kernel.stored_session(&id).await {
            Ok(Some(record)) => return Ok(Json(session_info_from_record(record))),
            Ok(None) => {}
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load session: {e}"),
                    }),
                ));
            }
        }
    }

    if state.active_runtime(&id).is_some() {
        Ok(Json(active_session_info(id)))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

async fn delete_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let removed_active = state.remove_active_runtime(&id).is_some();
    let removed_stored = state
        .session_kernel
        .delete_stored_session(&id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to delete session: {e}"),
                }),
            )
        })?;

    if removed_active || removed_stored {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

async fn append_session_timeline_event(
    store: &UnifiedSessionStore,
    session_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) {
    let sequence = match store.next_event_sequence(session_id).await {
        Ok(sequence) => sequence,
        Err(error) => {
            tracing::warn!(%session_id, %event_type, error = %error, "failed to allocate session event sequence");
            return;
        }
    };
    let created_at_ms = current_time_ms();
    let event = memory::SessionEvent {
        session_id: session_id.to_string(),
        event_type: event_type.to_string(),
        event_json: payload.to_string(),
        sequence,
        created_at_ms,
    };
    if let Err(error) = store.append_event(&event).await {
        tracing::warn!(%session_id, %event_type, error = %error, "failed to append session event");
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

async fn append_session_timeline_event_to_kernel(
    kernel: &SessionKernel,
    session_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) {
    if let Err(error) = kernel
        .append_timeline_event(session_id, event_type, payload)
        .await
    {
        tracing::warn!(%session_id, %event_type, error = %error, "failed to append session event");
    }
}

fn task_resume_context_packet(session_id: &str, task: &TaskRecord) -> ResumeContextPacket {
    let current_phase = task.current_phase.as_ref().and_then(|phase_ref| {
        task.phases
            .iter()
            .find(|phase| &phase.id == phase_ref || &phase.name == phase_ref)
    });
    let phase_summary = current_phase.map(|phase| {
        format!(
            "phase={} status={} objective={} acceptance=[{}]",
            phase.name,
            phase.status.as_str(),
            phase.objective,
            phase.acceptance.join("; ")
        )
    });
    let active_task = Some(format!(
        "id={} status={} yolo={} objective={}{}",
        task.id,
        task.status.as_str(),
        task.yolo_mode,
        task.objective,
        phase_summary
            .as_ref()
            .map(|summary| format!(" current_{summary}"))
            .unwrap_or_default()
    ));
    let recent_decisions = task
        .audit
        .iter()
        .rev()
        .take(5)
        .map(|event| format!("{}: {}", event.event_type, event.message))
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    if let Some(reason) = task
        .blocker_reason
        .as_ref()
        .filter(|reason| !reason.is_empty())
    {
        blockers.push(reason.clone());
    }
    if task.failure_count > 0 {
        blockers.push(format!("failure_count={}", task.failure_count));
    }

    ResumeContextPacket {
        session_id: session_id.to_string(),
        handoff_summary: None,
        active_task,
        recent_decisions,
        blockers,
        source: ResumeContextSource::TaskRegistry,
    }
}

fn runtime_run_started_payload(
    session_id: &str,
    run_id: &str,
    profile: ContextProfile,
    intent: &str,
    started_at_ms: u64,
) -> serde_json::Value {
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "started",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
        "profile": profile,
        "status": "running",
        "summary": intent.chars().take(120).collect::<String>(),
        "intent_preview": intent.chars().take(240).collect::<String>(),
        "started_at_ms": started_at_ms,
        "refs": [],
    })
}

fn runtime_run_completed_payload(
    session_id: &str,
    run_id: &str,
    profile: ContextProfile,
    status: &str,
    iterations: Option<usize>,
    context_envelope_id: Option<String>,
    error: Option<String>,
    started_at_ms: u64,
    completed_at_ms: u64,
) -> serde_json::Value {
    let refs = context_envelope_id
        .as_ref()
        .map(|id| vec![serde_json::json!({"type": "context_envelope", "id": id})])
        .unwrap_or_default();
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "completed",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
        "profile": profile,
        "status": status,
        "summary": error
            .as_ref()
            .map(|value| value.chars().take(160).collect::<String>())
            .unwrap_or_else(|| format!("turn {status}")),
        "iterations": iterations,
        "context_envelope_id": context_envelope_id,
        "error": error,
        "started_at_ms": started_at_ms,
        "completed_at_ms": completed_at_ms,
        "duration_ms": completed_at_ms.saturating_sub(started_at_ms),
        "refs": refs,
    })
}

async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");

    let session_id = id.clone();
    let event_bus = state.event_bus();
    let run_id = uuid::Uuid::new_v4().to_string();
    let run_started_at_ms = current_time_ms();
    let active_task = state.task_kernel.current();
    let run_profile = if active_task
        .as_ref()
        .is_some_and(|task| task.yolo_mode)
    {
        ContextProfile::YoloGoal
    } else {
        ContextProfile::MainTurn
    };
    append_session_timeline_event_to_kernel(
        &state.session_kernel,
        &session_id,
        "RuntimeRun",
        runtime_run_started_payload(
            &session_id,
            &run_id,
            run_profile,
            &body.content,
            run_started_at_ms,
        ),
    )
    .await;

    // Phase 1b: Subscribe CowdEventBus → forward text/thinking/tool events to SessionEventBus
    {
        let runtime_guard = runtime_entry.lock().await;
        if let Some(cowd_bus) = runtime_guard.cowd_bus() {
            let mut rx = cowd_bus.subscribe();
            let eb = event_bus.clone();
            let sid = session_id.clone();
            let kernel = state.session_kernel.clone();
            let active_run_id = run_id.clone();
            tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    match event {
                        runtime::CowdEvent::TextDelta { text } => {
                            eb.text_delta(&sid, &text).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "TextDelta",
                                serde_json::json!({"type":"TextDelta","content":text}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ThinkingDelta { thinking } => {
                            eb.thinking_delta(&sid, &thinking).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "ThinkingDelta",
                                serde_json::json!({"type":"ThinkingDelta","content":thinking}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ToolStart { id, name, preview } => {
                            eb.tool_start(&sid, &id, &name).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "ToolStart",
                                serde_json::json!({"type":"ToolStart","id":id,"name":name,"preview":preview}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ToolProgress { id, name, progress } => {
                            eb.tool_progress(&sid, &id, &name, &progress).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "ToolProgress",
                                serde_json::json!({"type":"ToolProgress","id":id,"name":name,"progress":progress}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ToolComplete {
                            id,
                            name,
                            summary,
                            exit_code,
                        } => {
                            eb.tool_complete(&sid, &id, &name, &summary, exit_code)
                                .await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "ToolComplete",
                                serde_json::json!({"type":"ToolComplete","id":id,"name":name,"summary":summary,"exit_code":exit_code}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::TurnComplete {
                            assistant_text,
                            iterations,
                        } => {
                            let json = serde_json::json!({"type":"TurnComplete","text":assistant_text,"iterations":iterations});
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "TurnComplete",
                                json,
                            )
                            .await;
                        }
                        runtime::CowdEvent::TurnStarted => {
                            let json = serde_json::json!({"type":"TurnStarted"});
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "TurnStarted",
                                json,
                            )
                            .await;
                        }
                        runtime::CowdEvent::TurnError { error } => {
                            let json = serde_json::json!({"type":"TurnError","error":error});
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "TurnError",
                                json,
                            )
                            .await;
                        }
                        runtime::CowdEvent::ContextEnvelope { envelope } => {
                            let json = serde_json::json!({
                                "type": "ContextEnvelope",
                                "envelope_id": envelope.id.clone(),
                                "run_id": active_run_id.clone(),
                                "session_id": envelope.identity.session_id.clone(),
                                "agent_id": envelope.identity.agent_id.clone(),
                                "profile": envelope.profile,
                                "diagnostics": envelope.diagnostics.clone(),
                                "budget": envelope.budget.clone(),
                                "hashes": {
                                    "stable_head": envelope.diagnostics.stable_head_hash,
                                    "runtime_header": envelope.diagnostics.runtime_header_hash,
                                    "dynamic_tail": envelope.diagnostics.dynamic_tail_hash,
                                },
                                "envelope": envelope,
                            });
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event_to_kernel(
                                &kernel,
                                &sid,
                                "ContextEnvelope",
                                json,
                            )
                            .await;
                        }
                        runtime::CowdEvent::TokenUsage { .. }
                        | runtime::CowdEvent::Warning { .. }
                        | runtime::CowdEvent::CompactionNotice { .. }
                        | _ => {}
                    }
                }
            });
        }
    }

    if let Some(task) = active_task {
        let packet = task_resume_context_packet(&session_id, &task);
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(run_profile);
        runtime_guard.inject_resume_context(packet);
    } else {
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(run_profile);
    }

    const TURN_TIMEOUT: Duration = Duration::from_secs(300);

    // Phase 2: Run turn in spawn_blocking — ConversationRuntime::run_turn_async
    // internally holds std::sync::MutexGuard across .await, making its future !Send.
    // spawn_blocking runs this on a dedicated thread so the axum handler future stays Send.
    let content = body.content;
    let rt_entry = runtime_entry.clone();
    let turn_result = tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async move {
            let mut runtime_guard = rt_entry.lock().await;
            timeout(
                TURN_TIMEOUT,
                runtime_guard
                    .run_turn_async(&content, &runtime::permissions::SharedPrompter::none()),
            )
            .await
        })
    })
    .await;

    // Phase 3: Process result — all work here is Send (tokio::sync channels, serde, Json)
    match turn_result {
        Ok(Ok(Ok(summary))) => {
            let final_text = summary
                .assistant_messages
                .last()
                .map(|msg| {
                    msg.blocks
                        .iter()
                        .filter_map(|block| match block {
                            runtime::ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("")
                })
                .unwrap_or_default();

            let session_snapshot = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard.session().clone()
            };
            let context_envelope_id = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard
                    .last_context_envelope()
                    .map(|envelope| envelope.id)
            };
            if let Err(e) = state
                .session_kernel
                .sync_runtime_session_snapshot(&session_id, &session_snapshot)
                .await
            {
                tracing::warn!(%session_id, error = %e, "failed to sync API session to SQLite");
            }

            let response = serde_json::json!({
                "session_id": &session_id,
                "status": "complete",
                "response": final_text,
                "iterations": summary.iterations,
            });

            let sse_data = serde_json::json!({
                "type": "TurnComplete",
                "session_id": &session_id,
                "response": final_text,
                "iterations": summary.iterations,
            });
            event_bus
                .broadcast(&session_id, &sse_data.to_string())
                .await;
            append_session_timeline_event_to_kernel(
                &state.session_kernel,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "completed",
                    Some(summary.iterations),
                    context_envelope_id,
                    None,
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;

            Ok(Json(response))
        }
        Ok(Ok(Err(e))) => {
            let error_msg = e.to_string();
            let context_envelope_id = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard
                    .last_context_envelope()
                    .map(|envelope| envelope.id)
            };

            let sse_data = serde_json::json!({
                "type": "TurnError",
                "session_id": &session_id,
                "error": error_msg,
            });
            event_bus
                .broadcast(&session_id, &sse_data.to_string())
                .await;
            append_session_timeline_event_to_kernel(
                &state.session_kernel,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "failed",
                    None,
                    context_envelope_id,
                    Some(error_msg.clone()),
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;

            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
        Ok(Err(_elapsed)) => {
            let error_msg = format!("turn timed out after {}s", TURN_TIMEOUT.as_secs());
            let context_envelope_id = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard
                    .last_context_envelope()
                    .map(|envelope| envelope.id)
            };

            let sse_data = serde_json::json!({
                "type": "TurnError",
                "session_id": &session_id,
                "error": error_msg,
            });
            event_bus
                .broadcast(&session_id, &sse_data.to_string())
                .await;
            append_session_timeline_event_to_kernel(
                &state.session_kernel,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "timeout",
                    None,
                    context_envelope_id,
                    Some(error_msg.clone()),
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;

            Err((
                StatusCode::REQUEST_TIMEOUT,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
        Err(join_err) => {
            let error_msg = format!("task join error: {join_err}");
            append_session_timeline_event_to_kernel(
                &state.session_kernel,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "failed",
                    None,
                    None,
                    Some(error_msg.clone()),
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
    }
}

// ── Memory / Tools / Config handlers ───────────────────────────

fn context_health_json(alert: RotAlert) -> serde_json::Value {
    match alert {
        RotAlert::None => serde_json::json!({
            "level": "healthy",
            "message": null,
        }),
        RotAlert::Warning(message) => serde_json::json!({
            "level": "warning",
            "message": message,
        }),
        RotAlert::Critical(message) => serde_json::json!({
            "level": "critical",
            "message": message,
        }),
    }
}

fn memory_kernel_health_json(health: memory::MemoryHealth) -> serde_json::Value {
    let degraded_reasons: Vec<String> = health
        .degraded
        .iter()
        .map(|reason| format!("{reason:?}"))
        .collect();
    serde_json::json!({
        "degraded": health.is_degraded(),
        "degraded_reasons": degraded_reasons,
        "orientation_pressure": health.orientation_pressure,
        "conflict_pressure": health.conflict_pressure,
        "stale_pressure": health.stale_pressure,
        "evidence_coverage": health.evidence_coverage,
        "link_coverage": health.link_coverage,
        "background_lag_ms": health.background_lag_ms,
    })
}

async fn memory_status_value(state: &AppState) -> serde_json::Value {
    if let Some(ref mgr) = state.memory_manager {
        let layers = mgr.list_layers().await;
        let kernel = MemoryKernel::new(Arc::clone(mgr));
        let kernel_ctx = MemoryTurnContext::new("api-memory-status", "api");
        let kernel_health = kernel
            .health(&kernel_ctx)
            .await
            .map(memory_kernel_health_json)
            .unwrap_or_else(|error| {
                serde_json::json!({
                    "degraded": true,
                    "degraded_reasons": [format!("health failed: {error}")],
                    "orientation_pressure": 0.0,
                    "conflict_pressure": 0.0,
                    "stale_pressure": 0.0,
                    "evidence_coverage": 0.0,
                    "link_coverage": 0.0,
                    "background_lag_ms": null,
                })
            });
        let vector_count = mgr.vector_index_count();
        let total_entries: usize = layers
            .iter()
            .filter_map(|layer| layer.get("entry_count").and_then(|v| v.as_u64()))
            .map(|count| count as usize)
            .sum();
        serde_json::json!({
            "enabled": true,
            "status": "ready",
            "degraded": false,
            "degraded_reason": null,
            "layers": layers,
            "total_entries": total_entries,
            "vector_count": vector_count,
            "session_store": true,
            "context_health": context_health_json(mgr.ctx_health()),
            "kernel_health": kernel_health,
            "performance": mgr.performance_report(),
        })
    } else {
        serde_json::json!({
            "enabled": false,
            "status": "disabled",
            "degraded": false,
            "degraded_reason": "memory not configured",
            "layers": empty_memory_layers(),
            "total_entries": 0,
            "vector_count": 0,
            "session_store": false,
            "context_health": {
                "level": "unavailable",
                "message": "memory not configured",
            },
            "kernel_health": {
                "degraded": true,
                "degraded_reasons": ["memory not configured"],
                "orientation_pressure": 0.0,
                "conflict_pressure": 0.0,
                "stale_pressure": 0.0,
                "evidence_coverage": 0.0,
                "link_coverage": 0.0,
                "background_lag_ms": null,
            },
            "message": "memory not configured"
        })
    }
}

async fn memory_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(memory_status_value(&state).await)
}

async fn memory_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(memory_status_value(&state).await)
}

async fn memory_stats_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        let layers = mgr.list_layers().await;
        let total_entries: usize = layers
            .iter()
            .filter_map(|layer| layer.get("entry_count").and_then(|v| v.as_u64()))
            .map(|count| count as usize)
            .sum();
        let entity_count = mgr
            .list_entities()
            .await
            .map(|v| v.len())
            .unwrap_or_default();
        let triple_count = mgr
            .list_triples()
            .await
            .map(|v| v.len())
            .unwrap_or_default();
        Json(serde_json::json!({
            "enabled": true,
            "total_entries": total_entries,
            "layers": layers,
            "entity_count": entity_count,
            "triple_count": triple_count,
            "vector_count": mgr.vector_index_count(),
            "performance": mgr.performance_report(),
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "total_entries": 0,
            "layers": empty_memory_layers(),
            "entity_count": 0,
            "triple_count": 0,
            "vector_count": 0,
        }))
    }
}

async fn memory_layers_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        Json(serde_json::json!({
            "enabled": true,
            "layers": mgr.list_layers().await,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "layers": empty_memory_layers(),
        }))
    }
}

async fn memory_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MemoryMaintenanceQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Ok(Json(serde_json::json!({
            "enabled": false,
            "candidates": [],
            "degraded_reason": "memory not configured",
        })));
    };
    let status = match query.status.as_deref() {
        Some(value) => Some(
            parse_maintenance_status(value)
                .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "invalid maintenance status"))?,
        ),
        None => None,
    };
    let kind = match query.kind.as_deref() {
        Some(value) => Some(
            parse_maintenance_kind(value)
                .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "invalid maintenance kind"))?,
        ),
        None => None,
    };
    let candidates = mgr
        .list_memory_maintenance(MaintenanceCandidateFilter {
            status,
            kind,
            source: query.source.filter(|source| !source.trim().is_empty()),
            limit: query.limit.map(|limit| limit.min(500)),
        })
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "enabled": true,
        "candidates": candidates,
    })))
}

async fn scan_memory_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<MemoryMaintenanceScanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Ok(Json(serde_json::json!({
            "enabled": false,
            "candidates": [],
            "degraded_reason": "memory not configured",
        })));
    };
    let defaults = MaintenanceScanConfig::default();
    let config = MaintenanceScanConfig {
        stale_threshold: body.stale_threshold.unwrap_or(defaults.stale_threshold),
        low_confidence_threshold: body
            .low_confidence_threshold
            .unwrap_or(defaults.low_confidence_threshold),
        authority_confidence_threshold: body
            .authority_confidence_threshold
            .unwrap_or(defaults.authority_confidence_threshold),
        max_candidates: body
            .max_candidates
            .unwrap_or(defaults.max_candidates)
            .min(500),
    };
    let candidates = mgr
        .scan_memory_maintenance(config)
        .await
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "enabled": true,
        "candidates": candidates,
    })))
}

async fn update_memory_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateMemoryMaintenanceRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let status = parse_maintenance_status(&body.status)
        .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "invalid maintenance status"))?;
    match mgr.transition_memory_maintenance(&id, status) {
        Ok(Some(candidate)) => Ok(Json(serde_json::json!({
            "enabled": true,
            "candidate": candidate,
        }))),
        Ok(None) => Err(api_error(
            StatusCode::NOT_FOUND,
            "maintenance candidate not found",
        )),
        Err(e) => Err(api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn memory_layer_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(layer): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(layer) = parse_memory_layer(&layer) else {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid memory layer"));
    };

    if let Some(ref mgr) = state.memory_manager {
        match mgr.list_layer_full_entries(layer).await {
            Ok(entries) => Ok(Json(serde_json::json!({
                "enabled": true,
                "layer": format!("{layer:?}"),
                "entries": entries,
            }))),
            Err(e) => Err(api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        }
    } else {
        Ok(Json(serde_json::json!({
            "enabled": false,
            "layer": format!("{layer:?}"),
            "entries": [],
        })))
    }
}

async fn create_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(layer): Path<String>,
    Json(body): Json<CreateMemoryEntryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(layer) = parse_memory_layer(&layer) else {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid memory layer"));
    };
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let content = body.content.trim();
    if content.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "memory content is required",
        ));
    }
    let category = body
        .category
        .as_deref()
        .and_then(parse_memory_category)
        .unwrap_or(MemoryCategory::Reference);
    let priority = body
        .priority
        .as_deref()
        .and_then(parse_memory_priority)
        .unwrap_or(Priority::Normal);
    let title = body
        .title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(String::from)
        .unwrap_or_else(|| content.chars().take(64).collect());
    let scope = body
        .scope
        .as_deref()
        .and_then(|scope| scope.parse::<MemoryScope>().ok())
        .unwrap_or_else(|| {
            if layer == MemoryLayer::L4 {
                MemoryScope::Global
            } else {
                MemoryScope::default()
            }
        });

    let id = MemoryId::new_v4();
    let entry = MemoryEntry {
        id,
        layer,
        category,
        priority,
        source: MemorySource::UserExplicit,
        title: title.clone(),
        content: content.to_string(),
        embedding: None,
        tags: body.tags,
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope,
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::Shared,
    };
    let kernel = MemoryKernel::new(Arc::clone(mgr));
    let memory_ctx = MemoryTurnContext::new("api-memory-create", "api");

    match kernel.remember(&memory_ctx, entry).await {
        Ok(()) => Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": id,
                "layer": format!("{layer:?}"),
                "title": title,
            })),
        )),
        Err(e) => Err(api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn delete_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((_layer, id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let memory_id = MemoryId::try_parse(&id)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid memory id"))?;
    let kernel = MemoryKernel::new(Arc::clone(mgr));
    let memory_ctx = MemoryTurnContext::new("api-memory-delete", "api");
    kernel
        .archive(&memory_ctx, memory_id, "archived by API delete request")
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn update_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateMemoryEntryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };

    let content = body
        .content
        .map(|content| content.trim().to_string())
        .filter(|content| !content.is_empty());
    let priority = body.priority.as_deref().and_then(parse_memory_priority);

    if content.is_none() && body.tags.is_none() && priority.is_none() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "content, tags, or priority is required",
        ));
    }

    mgr.update_entry(&id, content, body.tags, priority)
        .await
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "id": id,
        "updated": true,
    })))
}

async fn memory_entities_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        let entities = mgr.list_entities().await.unwrap_or_default();
        Json(serde_json::json!({
            "enabled": true,
            "entities": entities,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "entities": [],
        }))
    }
}

async fn memory_triples_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        let triples = mgr.list_triples().await.unwrap_or_default();
        Json(serde_json::json!({
            "enabled": true,
            "triples": triples,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "triples": [],
        }))
    }
}

async fn create_memory_symbol_link_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSymbolLinkRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let symbol_id = body.symbol_id.trim();
    if symbol_id.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "symbol_id is required"));
    }
    let memory_id = body
        .memory_id
        .parse::<uuid::Uuid>()
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "memory_id must be a valid UUID"))?;
    let reference_type = body
        .reference_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("reference");

    mgr.link_symbol_to_memory(symbol_id, memory_id, body.turn_index, reference_type)
        .await
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "symbol_id": symbol_id,
            "memory_id": memory_id,
            "reference_type": reference_type,
        })),
    ))
}

async fn memory_symbol_links_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let symbol = params
        .get("symbol")
        .or_else(|| params.get("q"))
        .map(String::as_str)
        .unwrap_or("")
        .trim();
    if symbol.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "symbol query is required",
        ));
    }

    let entries = mgr
        .find_memories_by_symbol(symbol)
        .await
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let total = entries.len();
    Ok(Json(serde_json::json!({
        "enabled": true,
        "symbol": symbol,
        "entries": entries,
        "total": total,
    })))
}

async fn memory_search_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();
    if let Some(ref mgr) = state.memory_manager {
        match mgr.search(&query).await {
            Ok(results) => Json(serde_json::json!({ "results": results })),
            Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
        }
    } else {
        Json(serde_json::json!({ "results": [] }))
    }
}

async fn memory_recall_explain_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .clamp(1, 50);

    let Some(ref mgr) = state.memory_manager else {
        return Json(serde_json::json!({
            "enabled": false,
            "query": query,
            "mode": "disabled",
            "degraded": true,
            "degraded_reason": "memory not configured",
            "total": 0,
            "results": [],
            "keywords": [],
            "categories": [],
        }));
    };

    let request = SearchMemoriesRequest {
        query: query.clone(),
        limit,
        with_snippets: true,
        with_keywords: true,
        ..Default::default()
    };

    match mgr.search_memories(request).await {
        Ok(result) => {
            let mode = result.search_mode.clone();
            let results: Vec<_> = result
                .entries
                .into_iter()
                .enumerate()
                .map(|(index, entry)| {
                    let snippet = result
                        .snippets
                        .get(index)
                        .and_then(|snippet| snippet.as_ref())
                        .map(|snippet| snippet.text.clone());
                    serde_json::json!({
                        "id": entry.id,
                        "title": entry.title,
                        "content": entry.content,
                        "source_layer": format!("{:?}", entry.layer),
                        "category": format!("{:?}", entry.category),
                        "priority": format!("{:?}", entry.priority),
                        "scope": entry.scope.to_string(),
                        "score": entry.confidence,
                        "mode": mode,
                        "snippet": snippet,
                        "tags": entry.tags,
                    })
                })
                .collect();
            Json(serde_json::json!({
                "enabled": true,
                "query": result.query,
                "mode": mode,
                "degraded": false,
                "degraded_reason": null,
                "total": result.total_matches,
                "results": results,
                "keywords": result.keywords,
                "categories": result.categories_found,
            }))
        }
        Err(e) => Json(serde_json::json!({
            "enabled": true,
            "query": query,
            "mode": mgr.search_mode_label(),
            "degraded": true,
            "degraded_reason": e.to_string(),
            "total": 0,
            "results": [],
            "keywords": [],
            "categories": [],
        })),
    }
}

async fn memory_packet_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let query = params.get("q").cloned().unwrap_or_default();
    let max_items = params
        .get("max_items")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(12)
        .clamp(1, 64);
    let max_tokens = params
        .get("max_tokens")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2_000)
        .clamp(64, 32_000);

    let Some(ref mgr) = state.memory_manager else {
        return Json(serde_json::json!({
            "enabled": false,
            "query": query,
            "packet": null,
            "degraded": true,
            "degraded_reason": "memory not configured",
        }));
    };

    let mgr = Arc::clone(mgr);
    let query_for_packet = query.clone();
    let packet_result = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())?;
        rt.block_on(async move {
            let kernel = MemoryKernel::new(mgr);
            let ctx = MemoryTurnContext::new("api-memory-packet", "api");
            kernel
                .context_packet(&ctx, &query_for_packet, &[], max_items, max_tokens)
                .await
                .map_err(|err| err.to_string())
        })
    })
    .await
    .map_err(|err| err.to_string())
    .and_then(|result| result);

    match packet_result {
        Ok(packet) => Json(serde_json::json!({
            "enabled": true,
            "query": query,
            "packet": packet,
            "degraded": false,
            "degraded_reason": null,
        })),
        Err(error) => Json(serde_json::json!({
            "enabled": true,
            "query": query,
            "packet": null,
            "degraded": true,
            "degraded_reason": error,
        })),
    }
}

async fn memory_links_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let Some(ref mgr) = state.memory_manager else {
        return Json(serde_json::json!({
            "enabled": false,
            "links": [],
            "degraded": true,
            "degraded_reason": "memory not configured",
        }));
    };
    let kernel = MemoryKernel::new(Arc::clone(mgr));
    match kernel.links().await {
        Ok(links) => Json(serde_json::json!({
            "enabled": true,
            "links": links,
            "total": links.len(),
            "degraded": false,
            "degraded_reason": null,
        })),
        Err(error) => Json(serde_json::json!({
            "enabled": true,
            "links": [],
            "total": 0,
            "degraded": true,
            "degraded_reason": error.to_string(),
        })),
    }
}

async fn performance_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        let report = mgr.performance_report();
        Json(serde_json::json!(report))
    } else {
        Json(serde_json::json!({
            "error": "memory not configured",
        }))
    }
}

fn empty_memory_layers() -> Vec<serde_json::Value> {
    ["L0", "L1", "L2", "L3", "L4"]
        .into_iter()
        .map(|layer| serde_json::json!({ "layer": layer, "entry_count": 0 }))
        .collect()
}

fn parse_memory_layer(layer: &str) -> Option<MemoryLayer> {
    match layer.to_ascii_uppercase().as_str() {
        "L0" => Some(MemoryLayer::L0),
        "L1" => Some(MemoryLayer::L1),
        "L2" => Some(MemoryLayer::L2),
        "L3" => Some(MemoryLayer::L3),
        "L4" => Some(MemoryLayer::L4),
        _ => None,
    }
}

fn parse_memory_category(category: &str) -> Option<MemoryCategory> {
    match category.to_ascii_lowercase().as_str() {
        "userpreference" | "user_preference" => Some(MemoryCategory::UserPreference),
        "projectconvention" | "project_convention" => Some(MemoryCategory::ProjectConvention),
        "decision" => Some(MemoryCategory::Decision),
        "reference" => Some(MemoryCategory::Reference),
        "shared" => Some(MemoryCategory::Shared),
        "compressedsummary" | "compressed_summary" => Some(MemoryCategory::CompressedSummary),
        "projectknowledge" | "project_knowledge" => Some(MemoryCategory::ProjectKnowledge),
        _ => None,
    }
}

fn parse_memory_priority(priority: &str) -> Option<Priority> {
    match priority.to_ascii_lowercase().as_str() {
        "critical" => Some(Priority::Critical),
        "high" => Some(Priority::High),
        "normal" => Some(Priority::Normal),
        "low" => Some(Priority::Low),
        _ => None,
    }
}

fn parse_maintenance_kind(kind: &str) -> Option<MaintenanceCandidateKind> {
    match kind.to_ascii_lowercase().as_str() {
        "conflict" => Some(MaintenanceCandidateKind::Conflict),
        "stale" => Some(MaintenanceCandidateKind::Stale),
        "duplicate" => Some(MaintenanceCandidateKind::Duplicate),
        "authoritypromotion" | "authority_promotion" => {
            Some(MaintenanceCandidateKind::AuthorityPromotion)
        }
        "relationshiprefresh" | "relationship_refresh" => {
            Some(MaintenanceCandidateKind::RelationshipRefresh)
        }
        _ => None,
    }
}

fn parse_maintenance_status(status: &str) -> Option<MaintenanceCandidateStatus> {
    match status.to_ascii_lowercase().as_str() {
        "open" => Some(MaintenanceCandidateStatus::Open),
        "acknowledged" | "ack" => Some(MaintenanceCandidateStatus::Acknowledged),
        "applied" => Some(MaintenanceCandidateStatus::Applied),
        "dismissed" | "dismiss" => Some(MaintenanceCandidateStatus::Dismissed),
        _ => None,
    }
}

fn api_error(status: StatusCode, error: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
}

async fn tools_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let tools: Vec<serde_json::Value> = state
        .tool_registry
        .definitions(None)
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "description": t.description,
                "enabled": true,
            })
        })
        .collect();
    Json(serde_json::json!({ "tools": tools, "count": tools.len() }))
}

async fn config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    match &state.config {
        Some(config) => Json(config.clone()),
        None => Json(serde_json::json!({
            "error": "config not loaded",
            "model": "unknown",
            "version": env!("CARGO_PKG_VERSION"),
        })),
    }
}

async fn profiles_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let profiles = state.profile_manager.list_profiles();
    Json(serde_json::json!({
        "profiles": profiles,
        "active_profile": state.profile_manager.active_id(),
        "runtime_profile": state.profile_id,
        "profiles_dir": state.profile_manager.profiles_dir().display().to_string(),
    }))
}

async fn create_profile_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateProfileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let name = body.name.trim();
    if name.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "profile name is required",
        ));
    }
    let profile = state
        .profile_manager
        .create_profile(name)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "profile": {
                "id": profile.id,
                "name": profile.name,
                "base_dir": profile.base_dir.display().to_string(),
                "config_path": profile.config_path().display().to_string(),
                "memory_dir": profile.memory_dir().display().to_string(),
                "permissions_path": profile.permissions_path().display().to_string(),
            },
            "active_profile": state.profile_manager.active_id(),
            "runtime_profile": state.profile_id,
            "restart_required": false,
        })),
    ))
}

async fn switch_profile_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<SwitchProfileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = body.profile.trim();
    if profile.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "profile is required"));
    }
    state
        .profile_manager
        .switch_profile(profile)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({
        "active_profile": state.profile_manager.active_id(),
        "runtime_profile": state.profile_id,
        "restart_required": state.profile_manager.active_id() != state.profile_id,
        "message": "profile switch persisted; restart the daemon to move memory/session roots",
    })))
}

async fn delete_profile_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .profile_manager
        .delete_profile(&id)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({
        "deleted": id,
        "active_profile": state.profile_manager.active_id(),
        "runtime_profile": state.profile_id,
    })))
}

async fn workspace_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let workspace_root = state.workspace_root.clone();
    let workspace_canonical = workspace_root.canonicalize().ok();
    Json(serde_json::json!({
        "workspace_root": workspace_root.display().to_string(),
        "workspace_canonical": workspace_canonical.map(|path| path.display().to_string()),
        "profile_id": state.profile_id,
        "config_home": state.config_home.display().to_string(),
        "sessions_db": state.config_home.join("sessions.db").display().to_string(),
        "memory_dir": state.config_home.join("memory").display().to_string(),
    }))
}

async fn workspaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "workspaces": [{
            "id": "current",
            "name": state.workspace_root.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("workspace"),
            "path": state.workspace_root.display().to_string(),
            "active": true,
            "profile_id": state.profile_id,
        }]
    }))
}

async fn workspace_files_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<WorkspaceFilesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let dir = resolve_existing_workspace_path(&state.workspace_root, params.dir.as_deref())
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if !dir.is_dir() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "path is not a directory",
        ));
    }

    let mut files = fs::read_dir(&dir)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .flatten()
        .filter_map(|entry| workspace_file_item(&root, entry.path()))
        .collect::<Vec<_>>();
    files.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
    });
    files.truncate(500);

    Ok(Json(serde_json::json!({
        "workspace_root": state.workspace_root.display().to_string(),
        "dir": workspace_relative_path(&root, &dir),
        "files": files,
    })))
}

async fn create_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateWorkspaceFileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let target = resolve_new_workspace_file_path(&state.workspace_root, &body.path)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if target.exists() && target.is_dir() {
        return Err(api_error(StatusCode::BAD_REQUEST, "path is a directory"));
    }
    fs::write(&target, body.content)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "path": workspace_relative_path(&root, &target),
            "created": true,
        })),
    ))
}

async fn raw_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RawFileParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let file = resolve_existing_workspace_path(&state.workspace_root, Some(&params.path))
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if !file.is_file() {
        return Err(api_error(StatusCode::BAD_REQUEST, "path is not a file"));
    }
    let bytes =
        fs::read(&file).map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    ))
}

async fn approval_pending_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let pending = match &state.approval_gate {
        Some(gate) => gate.get_pending_requests().await,
        None => Vec::new(),
    };
    Json(serde_json::json!(pending))
}

async fn approval_config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let cfg = match &state.approval_gate {
        Some(gate) => gate.config().read().await.clone(),
        None => ApprovalConfig::default(),
    };
    Json(serde_json::json!(cfg))
}

async fn update_approval_config_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(config): Json<ApprovalConfig>,
) -> impl IntoResponse {
    if let Some(gate) = &state.approval_gate {
        gate.update_config(config.clone()).await;
    }
    Json(serde_json::json!(config))
}

async fn toggle_solo_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let mut cfg = match &state.approval_gate {
        Some(gate) => gate.config().read().await.clone(),
        None => ApprovalConfig::default(),
    };
    cfg.solo_mode = !cfg.solo_mode;
    if let Some(gate) = &state.approval_gate {
        gate.update_config(cfg.clone()).await;
    }
    Json(serde_json::json!(cfg))
}

async fn approval_history_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(50)
        .min(200);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let history = match &state.approval_gate {
        Some(gate) => gate.history().list_history(limit, offset).await.0,
        None => Vec::new(),
    };
    Json(serde_json::json!(history))
}

async fn audit_export_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .min(500);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let source = params.get("source").map(String::as_str).unwrap_or("all");
    let include_approval = matches!(source, "all" | "approval");
    let include_memory = matches!(source, "all" | "memory");

    let (approval, approval_total) = if include_approval {
        match &state.approval_gate {
            Some(gate) => gate.history().list_history(limit + offset, 0).await,
            None => (Vec::new(), 0),
        }
    } else {
        (Vec::new(), 0)
    };
    let memory = if include_memory {
        match &state.memory_manager {
            Some(manager) => manager.audit_entries(limit + offset).unwrap_or_default(),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let memory_total = memory.len();

    let mut records = Vec::new();
    for entry in &approval {
        records.push(serde_json::json!({
            "source": "approval",
            "timestamp": entry.resolved_at,
            "id": entry.id,
            "summary": entry.command,
            "record": entry,
        }));
    }
    for entry in &memory {
        records.push(serde_json::json!({
            "source": "memory",
            "timestamp": entry.timestamp,
            "id": entry.entry_id,
            "summary": entry.summary,
            "record": entry,
        }));
    }
    records.sort_by(|a, b| {
        b.get("timestamp")
            .and_then(|v| v.as_str())
            .cmp(&a.get("timestamp").and_then(|v| v.as_str()))
    });
    let total = records.len();
    let records = records
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    Json(serde_json::json!({
        "kind": "audit_export",
        "generated_at": chrono::Utc::now(),
        "source": source,
        "limit": limit,
        "offset": offset,
        "total": total,
        "totals": {
            "approval": approval_total,
            "memory": memory_total,
        },
        "records": records,
        "approval": approval,
        "memory": memory,
    }))
}

async fn approval_respond_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ApprovalRespondRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(gate) = &state.approval_gate else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            "approval gate not configured",
        ));
    };
    let persistence = match body.persistence.as_deref().unwrap_or("once") {
        "session" => ApprovalPersistence::Session,
        "always" => ApprovalPersistence::Always,
        _ => ApprovalPersistence::Once,
    };
    let verdict = if body.approved {
        ApprovalVerdict::Approved
    } else {
        ApprovalVerdict::Denied {
            reason: body.reason.unwrap_or_else(|| "denied by user".to_string()),
        }
    };
    let Some(request) = gate.resolve_approval(&body.id, verdict, persistence).await else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            "approval request not found",
        ));
    };
    Ok(Json(serde_json::json!({
        "id": body.id,
        "resolved": true,
        "approved": body.approved,
        "tool": "bash",
        "action": request.command,
    })))
}

// ── Session messages handler ────────────────────────────────────

async fn get_session_messages(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let offset = params.offset.unwrap_or(0);
    let from_seq = params.from_seq;
    let limit = params.limit.unwrap_or(50).min(500);

    // Try unified store for DB-backed pagination
    if state.has_unified_store() {
        let total = state
            .session_kernel
            .stored_message_count(&id)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);
        let db_messages = if let Some(seq) = from_seq {
            state
                .session_kernel
                .stored_messages_from_sequence(&id, seq, limit)
                .await
                .unwrap_or(Some(Vec::new()))
                .unwrap_or_default()
        } else {
            state
                .session_kernel
                .stored_messages(&id, offset, limit)
                .await
                .unwrap_or(Some(Vec::new()))
                .unwrap_or_default()
        };
        let messages: Vec<serde_json::Value> = db_messages
            .iter()
            .map(|m| {
                let blocks: Vec<serde_json::Value> =
                    serde_json::from_str(&m.content_json).unwrap_or_default();
                let mut val = serde_json::json!({
                    "session_id": m.session_id,
                    "sequence": m.sequence,
                    "role": m.role,
                    "blocks": blocks,
                    "created_at_ms": m.created_at_ms,
                });
                if let Some(ref tu) = m.token_usage_json {
                    if let Ok(usage) = serde_json::from_str::<serde_json::Value>(tu) {
                        val["token_usage"] = usage;
                    }
                }
                if let Some(ref tid) = m.tool_use_id {
                    val["tool_use_id"] = serde_json::Value::String(tid.clone());
                }
                if let Some(ref tn) = m.tool_name {
                    val["tool_name"] = serde_json::Value::String(tn.clone());
                }
                val
            })
            .collect();
        let next_seq = db_messages.last().map(|m| m.sequence + 1);
        let has_more = next_seq
            .map(|seq| seq < total)
            .unwrap_or_else(|| from_seq.unwrap_or(offset) < total);
        return Ok(Json(serde_json::json!({
            "session_id": id,
            "messages": messages,
            "total": total,
            "offset": offset,
            "from_seq": from_seq,
            "next_seq": next_seq,
            "limit": limit,
            "has_more": has_more,
        })));
    }

    // Fallback: in-memory session
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    let runtime_guard = runtime_entry.lock().await;
    let session = runtime_guard.session();

    let all_messages: Vec<serde_json::Value> = session
        .messages
        .iter()
        .map(|msg| {
            let role = match msg.role {
                runtime::MessageRole::System => "system",
                runtime::MessageRole::User => "user",
                runtime::MessageRole::Assistant => "assistant",
                runtime::MessageRole::Tool => "tool",
            };
            let blocks: Vec<serde_json::Value> = msg
                .blocks
                .iter()
                .map(|block| match block {
                    runtime::ContentBlock::Text { text } => {
                        serde_json::json!({"type": "text", "text": text})
                    }
                    runtime::ContentBlock::Thinking { thinking, signature } => {
                        let mut val = serde_json::json!({"type": "thinking", "thinking": thinking});
                        if let Some(sig) = signature {
                            val["signature"] = serde_json::Value::String(sig.clone());
                        }
                        val
                    }
                    runtime::ContentBlock::ToolUse { id, name, input } => {
                        serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
                    }
                    runtime::ContentBlock::ToolResult { tool_use_id, tool_name, output, is_error } => {
                        serde_json::json!({"type": "tool_result", "tool_use_id": tool_use_id, "tool_name": tool_name, "output": output, "is_error": is_error})
                    }
                })
                .collect();

            let mut val = serde_json::json!({"role": role, "blocks": blocks});
            if let Some(usage) = &msg.usage {
                val["usage"] = serde_json::json!({
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                });
            }
            val
        })
        .collect();

    let total = all_messages.len();
    let start = from_seq.unwrap_or(offset);
    let messages: Vec<serde_json::Value> =
        all_messages.into_iter().skip(start).take(limit).collect();
    let next_seq = (!messages.is_empty()).then_some(start + messages.len());
    let has_more = next_seq.map(|seq| seq < total).unwrap_or(start < total);

    Ok(Json(serde_json::json!({
        "session_id": id,
        "messages": messages,
        "total": total,
        "offset": offset,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

async fn get_session_events(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(500);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_page(&id, from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session events: {e}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };
    let events: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(|event| {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
                .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
            serde_json::json!({
                "session_id": event.session_id,
                "type": event.event_type,
                "sequence": event.sequence,
                "created_at_ms": event.created_at_ms,
                "payload": payload,
            })
        })
        .collect();
    let has_more = events.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "events": events,
        "total": total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

fn runtime_run_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "run": payload,
    })
}

fn runtime_run_tree_summary(runs: &[serde_json::Value]) -> serde_json::Value {
    use std::collections::{BTreeMap, BTreeSet};

    let mut parents: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut children: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut latest_status: BTreeMap<String, String> = BTreeMap::new();
    let mut failed_count = 0usize;
    let mut completed_count = 0usize;
    let mut running_count = 0usize;

    for event in runs {
        let run = &event["run"];
        let Some(run_id) = run.get("run_id").and_then(|value| value.as_str()) else {
            continue;
        };
        let parent = run
            .get("parent_run_id")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        parents.entry(run_id.to_string()).or_insert(parent.clone());
        if let Some(parent_id) = parent {
            children
                .entry(parent_id)
                .or_default()
                .insert(run_id.to_string());
        }
        if let Some(status) = run.get("status").and_then(|value| value.as_str()) {
            latest_status.insert(run_id.to_string(), status.to_string());
        }
    }

    for status in latest_status.values() {
        match status.as_str() {
            "completed" => completed_count += 1,
            "failed" | "timeout" | "cancelled" => failed_count += 1,
            "running" => running_count += 1,
            _ => {}
        }
    }

    let roots = parents
        .iter()
        .filter_map(|(run_id, parent)| {
            let is_root = match parent.as_ref() {
                Some(parent_id) => !parents.contains_key(parent_id),
                None => true,
            };
            if is_root {
                Some(run_id.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let root_count = roots.len();
    let child_map = children
        .into_iter()
        .map(|(parent, child_ids)| (parent, child_ids.into_iter().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();

    serde_json::json!({
        "roots": roots,
        "children": child_map,
        "summary": {
            "event_count": runs.len(),
            "span_count": parents.len(),
            "root_count": root_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "running_count": running_count,
        }
    })
}

async fn get_session_runs(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_by_type_page(&id, "RuntimeRun", from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load runtime runs: {e}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let runs: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(runtime_run_event_json)
        .collect();
    let tree = runtime_run_tree_summary(&runs);
    let next_seq = runs
        .last()
        .and_then(|event| event["sequence"].as_u64())
        .map(|sequence| sequence as usize + 1);
    let has_more = runs.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "runs": runs,
        "tree": tree,
        "total": total,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

fn context_envelope_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    let envelope = payload
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let envelope_id = payload
        .get("envelope_id")
        .cloned()
        .or_else(|| envelope.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    let run_id = payload
        .get("run_id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "envelope_id": envelope_id,
        "run_id": run_id,
        "envelope": envelope,
    })
}

async fn get_session_context_history(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_by_type_page(&id, "ContextEnvelope", from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load context timeline: {e}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let envelopes: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(context_envelope_event_json)
        .collect();
    let next_seq = envelopes
        .last()
        .and_then(|event| event["sequence"].as_u64())
        .map(|sequence| sequence as usize + 1);
    let has_more = envelopes.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "envelopes": envelopes,
        "total": total,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

async fn get_context_envelope_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(envelope_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.has_unified_store() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    }

    let Some(event) = state
        .session_kernel
        .context_event_by_envelope_id(&envelope_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load context envelope: {e}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("context envelope {envelope_id} not found"),
            }),
        ));
    };

    Ok(Json(serde_json::json!({
        "enabled": true,
        "source": "history",
        "context": context_envelope_event_json(event),
    })))
}

async fn get_context_recommendation_stats(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetRecommendationStatsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(200).min(500);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_by_type_page(&id, "ContextRecommendationAction", from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load context recommendation stats: {e}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let event_count = stored_events.len();
    let mut grouped: HashMap<String, serde_json::Value> = HashMap::new();
    for event in stored_events {
        let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let Some(recommendation) = payload
            .get("recommendation")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let action = payload
            .get("action")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("acknowledged");
        let entry = grouped.entry(recommendation.to_string()).or_insert_with(|| {
            serde_json::json!({
                "recommendation": recommendation,
                "count": 0_u64,
                "actions": {},
                "latest_envelope_id": null,
                "latest_created_at_ms": 0_u64,
            })
        });
        let count = entry["count"].as_u64().unwrap_or(0) + 1;
        entry["count"] = serde_json::json!(count);
        let action_count = entry["actions"][action].as_u64().unwrap_or(0) + 1;
        entry["actions"][action] = serde_json::json!(action_count);
        if event.created_at_ms >= entry["latest_created_at_ms"].as_u64().unwrap_or(0) {
            entry["latest_created_at_ms"] = serde_json::json!(event.created_at_ms);
            entry["latest_envelope_id"] = payload
                .get("envelope_id")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
        }
    }

    let mut recommendations: Vec<serde_json::Value> = grouped.into_values().collect();
    recommendations.sort_by(|left, right| {
        right["count"]
            .as_u64()
            .cmp(&left["count"].as_u64())
            .then_with(|| {
                left["recommendation"]
                    .as_str()
                    .cmp(&right["recommendation"].as_str())
            })
    });

    Ok(Json(serde_json::json!({
        "session_id": id,
        "recommendations": recommendations,
        "total": total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": event_count < total,
    })))
}

async fn resolve_evidence_ref_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(reference) = params.get("ref").map(|value| value.trim()).filter(|value| !value.is_empty()) else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "ref query parameter is required".to_string(),
            }),
        ));
    };
    let session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next());

    let resolved = if let Some(path) = reference.strip_prefix("workspace://changed-file/") {
        resolve_workspace_evidence(&state.workspace_root, reference, path)
    } else if let Some(symbol) = reference.strip_prefix("workspace://symbol/") {
        serde_json::json!({
            "ref": reference,
            "kind": "workspace_symbol",
            "available": true,
            "symbol": symbol,
        })
    } else if let Some(session_ref) = reference.strip_prefix("session://") {
        resolve_session_evidence(&state, reference, session_ref).await
    } else if reference.starts_with("tool://") {
        resolve_tool_evidence(&state, reference, session_id.as_deref()).await
    } else if reference.starts_with("agent://") {
        serde_json::json!({
            "ref": reference,
            "kind": "agent",
            "available": false,
            "reason": "agent evidence payload drilldown is not persisted yet",
        })
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("unsupported evidence ref: {reference}"),
            }),
        ));
    };

    Ok(Json(resolved))
}

fn resolve_workspace_evidence(root: &FsPath, reference: &str, relative: &str) -> serde_json::Value {
    const MAX_BYTES: u64 = 256 * 1024;
    const PREVIEW_BYTES: usize = 4096;

    let path = root.join(relative);
    let Ok(canonical_root) = root.canonicalize() else {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "workspace root unavailable",
        });
    };
    let Ok(canonical_path) = path.canonicalize() else {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "file unavailable",
        });
    };
    if !canonical_path.starts_with(&canonical_root) {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "file is outside workspace",
        });
    }
    let Ok(metadata) = std::fs::metadata(&canonical_path) else {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "file metadata unavailable",
        });
    };
    if !metadata.is_file() {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "path is not a file",
        });
    }
    if metadata.len() > MAX_BYTES {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": true,
            "truncated": true,
            "size_bytes": metadata.len(),
            "reason": "file exceeds preview limit",
        });
    }
    let preview = std::fs::read_to_string(&canonical_path)
        .map(|content| content.chars().take(PREVIEW_BYTES).collect::<String>())
        .unwrap_or_default();
    serde_json::json!({
        "ref": reference,
        "kind": "workspace_file",
        "available": true,
        "path": relative,
        "size_bytes": metadata.len(),
        "preview": preview,
        "truncated": metadata.len() as usize > PREVIEW_BYTES,
    })
}

async fn resolve_session_evidence(
    state: &AppState,
    reference: &str,
    session_ref: &str,
) -> serde_json::Value {
    let session_id = session_ref.split('/').next().unwrap_or_default();
    if session_id.is_empty() {
        return serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": false,
            "reason": "missing session id",
        });
    }
    match state.session_kernel.stored_session(session_id).await {
        Ok(Some(session)) => serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": true,
            "session": {
                "session_id": session.session_id,
                "platform": session.platform,
                "model": session.model,
                "created_at": session.created_at,
                "last_activity": session.last_activity,
                "message_count": session.message_count,
                "status": session.status,
            },
        }),
        Ok(None) => serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": false,
            "reason": "session not found",
        }),
        Err(error) => serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": false,
            "reason": format!("session lookup failed: {error}"),
        }),
    }
}

async fn resolve_tool_evidence(
    state: &AppState,
    reference: &str,
    session_id: Option<&str>,
) -> serde_json::Value {
    let Some(session_id) = session_id else {
        return serde_json::json!({
            "ref": reference,
            "kind": "tool",
            "available": false,
            "reason": "session_id is required for tool evidence",
        });
    };
    let tool_id = reference
        .strip_prefix("tool://")
        .and_then(|tail| tail.split('/').next())
        .unwrap_or_default();
    let Some((_, events)) = state
        .session_kernel
        .stored_events_page(session_id, 0, 500)
        .await
        .ok()
        .flatten()
    else {
        return serde_json::json!({
            "ref": reference,
            "kind": "tool",
            "available": false,
            "reason": "session events unavailable",
        });
    };
    let matches = events
        .into_iter()
        .filter_map(|event| {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json).ok()?;
            let id_matches = payload
                .get("id")
                .and_then(|value| value.as_str())
                .is_some_and(|id| id == tool_id)
                || payload
                    .get("tool_use_id")
                    .and_then(|value| value.as_str())
                    .is_some_and(|id| id == tool_id);
            id_matches.then(|| serde_json::json!({
                "type": event.event_type,
                "sequence": event.sequence,
                "created_at_ms": event.created_at_ms,
                "payload": payload,
            }))
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "ref": reference,
        "kind": "tool",
        "available": !matches.is_empty(),
        "session_id": session_id,
        "events": matches,
    })
}

async fn record_context_recommendation_action(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ContextRecommendationActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if body.envelope_id.trim().is_empty() || body.recommendation.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "envelope_id and recommendation are required".to_string(),
            }),
        ));
    }
    let action = if body.action.trim().is_empty() {
        "acknowledged".to_string()
    } else {
        body.action
    };
    let payload = serde_json::json!({
        "type": "ContextRecommendationAction",
        "session_id": id.clone(),
        "envelope_id": body.envelope_id,
        "recommendation": body.recommendation,
        "action": action,
        "note": body.note,
    });
    state
        .session_kernel
        .append_timeline_event(&id, "ContextRecommendationAction", payload.clone())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to record context recommendation action: {e}"),
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "event": payload,
    })))
}

// ── Session messages search handler ───────────────────────────────

async fn search_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<SearchMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(db_messages) = state
        .session_kernel
        .search_stored_messages(&params.q, params.limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("search failed: {e}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let results: Vec<SearchMessagesItem> = db_messages
        .into_iter()
        .map(|m| {
            let blocks: Vec<serde_json::Value> =
                serde_json::from_str(&m.content_json).unwrap_or_default();
            let content_preview = blocks
                .iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join(" ");
            let preview = if content_preview.len() > 200 {
                format!("{}...", &content_preview[..200])
            } else {
                content_preview
            };
            SearchMessagesItem {
                session_id: m.session_id,
                sequence: m.sequence,
                role: m.role,
                blocks,
                content_preview: preview,
                tool_use_id: m.tool_use_id,
                tool_name: m.tool_name,
                created_at_ms: m.created_at_ms,
            }
        })
        .collect();

    let total = results.len();
    Ok(Json(SearchMessagesResponse {
        query: params.q,
        results,
        total,
    }))
}

// ── Session compaction handler ──────────────────────────────────

async fn compact_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    let mut runtime_guard = runtime_entry.lock().await;
    let result = runtime_guard.compact(runtime::CompactionConfig::default());

    // Apply the compacted session back if compaction actually happened
    if result.removed_message_count > 0 {
        *runtime_guard.session_mut() = result.compacted_session.clone();
    }
    let session_snapshot = runtime_guard.session().clone();
    drop(runtime_guard);

    state
        .session_kernel
        .sync_runtime_session_snapshot(&id, &session_snapshot)
        .await
        .map_err(|e| {
            tracing::error!(session_id = %id, error = %e, "failed to sync compacted session to unified store");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to sync compacted session: {e}"),
                }),
            )
        })?;

    tracing::info!(%id, removed = result.removed_message_count, "API session compacted");

    Ok(Json(serde_json::json!({
        "session_id": id,
        "compacted": result.removed_message_count > 0,
        "removed_message_count": result.removed_message_count,
        "summary": result.formatted_summary,
    })))
}

// ── Session stats handler ────────────────────────────────────────

async fn get_session_stats_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    let runtime_guard = runtime_entry.lock().await;
    let session = runtime_guard.session();
    let messages = &session.messages;

    let user_count = messages
        .iter()
        .filter(|m| m.role == runtime::MessageRole::User)
        .count();
    let assistant_count = messages
        .iter()
        .filter(|m| m.role == runtime::MessageRole::Assistant)
        .count();
    let tool_count = messages
        .iter()
        .filter(|m| m.role == runtime::MessageRole::Tool)
        .count();

    let total_input_tokens: u32 = messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| u.input_tokens)
        .sum();
    let total_output_tokens: u32 = messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| u.output_tokens)
        .sum();

    let mut tool_usage: HashMap<String, usize> = HashMap::new();
    for msg in messages {
        if msg.role == runtime::MessageRole::Assistant {
            for block in &msg.blocks {
                if let runtime::ContentBlock::ToolUse { name, .. } = block {
                    *tool_usage.entry(name.clone()).or_insert(0) += 1;
                }
            }
        }
    }

    let duration_ms = session.updated_at_ms.saturating_sub(session.created_at_ms);

    Ok(Json(serde_json::json!({
        "session_id": id,
        "message_count": messages.len(),
        "message_counts": {
            "user": user_count,
            "assistant": assistant_count,
            "tool": tool_count,
        },
        "tokens": {
            "input": total_input_tokens,
            "output": total_output_tokens,
            "total": total_input_tokens + total_output_tokens,
        },
        "tool_usage": tool_usage,
        "duration_ms": duration_ms,
    })))
}

// ── Session PATCH / update handler ──────────────────────────────

async fn update_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut found = false;

    // Update active runtime if this session is currently loaded.
    if let Some(runtime_entry) = state.active_runtime(&id) {
        found = true;
        let mut runtime_guard = runtime_entry.lock().await;
        let mut session = runtime_guard.session_mut_async().await;
        if let Some(ref model) = body.model {
            session.model = Some(model.clone());
        }
    }

    // Persist to UnifiedSessionStore if available (read-modify-write)
    if state.has_unified_store() {
        match state.session_kernel.stored_session(&id).await {
            Ok(Some(mut record)) => {
                found = true;
                if let Some(ref model) = body.model {
                    record.model = Some(model.clone());
                }
                if let Some(ref title) = body.title {
                    let mut meta: serde_json::Value = record
                        .metadata_json
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(serde_json::json!({}));
                    meta["title"] = serde_json::Value::String(title.clone());
                    record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
                }
                if let Some(ref metadata) = body.metadata {
                    let mut meta: serde_json::Value = record
                        .metadata_json
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(serde_json::json!({}));
                    if let Some(obj) = meta.as_object_mut() {
                        if let Some(new_obj) = metadata.as_object() {
                            for (k, v) in new_obj {
                                obj.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
                }
                state
                    .session_kernel
                    .update_stored_session(&record)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: format!("failed to update session: {e}"),
                            }),
                        )
                    })?;
            }
            Ok(None) => {}
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load session: {e}"),
                    }),
                ));
            }
        }
    }

    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ));
    }

    Ok(Json(serde_json::json!({
        "session_id": id,
        "updated": true,
    })))
}

// ── Auth handlers (public) ──────────────────────────────────────

async fn login_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match &state.auth_token {
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "auth not configured".to_string(),
            }),
        )),
        Some(expected) if expected == &body.token => {
            tracing::info!("API login successful");
            Ok(Json(serde_json::json!({
                "success": true,
                "token": body.token,
            })))
        }
        Some(_) => Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid token".to_string(),
            }),
        )),
    }
}

async fn verify_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let auth_token = match &state.auth_token {
        None => {
            return Ok(Json(serde_json::json!({
                "valid": true,
                "auth_required": false,
            })));
        }
        Some(token) => token,
    };

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(h) if h == format!("Bearer {auth_token}") => Ok(Json(serde_json::json!({
            "valid": true,
            "auth_required": true,
        }))),
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid or missing token".to_string(),
            }),
        )),
    }
}

// ── SSE drop guard ───────────────────────────────────────────────

/// Wraps an `UnboundedReceiverStream` and unsubscribes from the event bus
/// when the stream is dropped (client disconnects), preventing sender leaks.
struct SseStream {
    rx: ReceiverStream<String>,
    session_id: String,
    event_bus: Arc<SessionEventBus>,
    tx: mpsc::Sender<String>,
}

impl Stream for SseStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.rx.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(data)) => {
                std::task::Poll::Ready(Some(Ok(Event::default().data(data))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Drop for SseStream {
    fn drop(&mut self) {
        let event_bus = self.event_bus.clone();
        let session_id = self.session_id.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            event_bus.unsubscribe(&session_id, &tx).await;
        });
    }
}

// ── SSE stream handler ──────────────────────────────────────────

async fn sse_stream_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(256);
    // Clone tx before subscribing — one copy moves into the event bus,
    // the other stays with SseStream for cleanup on drop.
    let bus_tx = tx.clone();
    let event_bus = state.event_bus();
    event_bus.subscribe(&session_id, bus_tx).await;
    let _ = tx
        .send(
            serde_json::json!({
                "type": "Connected",
                "session_id": session_id,
            })
            .to_string(),
        )
        .await;

    let stream = SseStream {
        rx: ReceiverStream::new(rx),
        session_id,
        event_bus,
        tx,
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        body::to_bytes,
        http::{Request, StatusCode},
    };
    use memory::config::{BudgetConfig, StoreConfig};
    use runtime::permission_enforcer::DestructivePatternDetector;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_profile_manager() -> Arc<ProfileManager> {
        let dir = std::env::temp_dir().join(format!("cowd-api-profiles-{}", uuid::Uuid::new_v4()));
        let manager = Arc::new(ProfileManager::new_with_profiles_dir(dir));
        manager.initialize().unwrap();
        manager
    }

    fn test_session_kernel(
        sessions: Arc<ActiveSessions>,
        store: Option<Arc<UnifiedSessionStore>>,
        event_bus: Arc<SessionEventBus>,
    ) -> Arc<SessionKernel> {
        Arc::new(SessionKernel::new(sessions, store, event_bus))
    }

    fn test_task_kernel() -> Arc<TaskKernel> {
        let path =
            std::env::temp_dir().join(format!("cowd-api-task-{}.json", uuid::Uuid::new_v4()));
        Arc::new(TaskKernel::open(path).expect("task kernel should open"))
    }

    fn test_state() -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new(); // returns Arc<Self>
        Arc::new(AppState {
            session_kernel: test_session_kernel(sessions.clone(), None, event_bus.clone()),
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        })
    }

    fn test_state_with_store(store: Arc<UnifiedSessionStore>) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        Arc::new(AppState {
            session_kernel: test_session_kernel(
                sessions.clone(),
                Some(store.clone()),
                event_bus.clone(),
            ),
            sessions,
            memory_manager: None,
            unified_store: Some(store),
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        })
    }

    fn test_memory_config(sqlite_path: &std::path::Path) -> memory::MemoryConfig {
        memory::MemoryConfig {
            store: StoreConfig {
                sqlite_path: sqlite_path.to_path_buf(),
                blob_dir: sqlite_path.parent().unwrap().join("blobs"),
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 8_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_state_with_memory(memory_manager: Arc<CognitiveContextManager>) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        Arc::new(AppState {
            session_kernel: test_session_kernel(sessions.clone(), None, event_bus.clone()),
            sessions,
            memory_manager: Some(memory_manager),
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: None,
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        })
    }

    fn test_approval_gate() -> Arc<SmartApprovalGate> {
        Arc::new(SmartApprovalGate::new(
            Arc::new(DestructivePatternDetector::new(std::env::temp_dir())),
            ApprovalConfig::default(),
            None,
        ))
    }

    fn test_state_with_approval_gate(gate: Arc<SmartApprovalGate>) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        Arc::new(AppState {
            session_kernel: test_session_kernel(sessions.clone(), None, event_bus.clone()),
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: Some(gate),
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        })
    }

    fn test_state_with_workspace(workspace_root: PathBuf, config_home: PathBuf) -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        Arc::new(AppState {
            session_kernel: test_session_kernel(sessions.clone(), None, event_bus.clone()),
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: None,
            auth_token: None,
            workspace_root,
            config_home,
            profile_id: "enterprise".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        })
    }

    #[test]
    fn app_state_compat_fields_share_session_kernel_handles() {
        let state = test_state_with_store(Arc::new(UnifiedSessionStore::open_in_memory().unwrap()));

        assert!(Arc::ptr_eq(
            &state.session_kernel.active_sessions(),
            &state.sessions
        ));
        assert!(Arc::ptr_eq(
            &state.session_kernel.event_bus(),
            &state.event_bus
        ));
        assert!(Arc::ptr_eq(
            &state
                .session_kernel
                .unified_store()
                .expect("kernel store should exist"),
            state
                .unified_store
                .as_ref()
                .expect("compat store should exist")
        ));
    }

    fn test_temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cowd-api-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn workspace_api_reports_profile_and_lists_files() {
        let workspace = test_temp_dir("workspace-list");
        let config_home = test_temp_dir("workspace-config");
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        std::fs::write(workspace.join("src").join("main.rs"), "fn main() {}\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let workspace_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(workspace_response.status(), StatusCode::OK);
        let body = to_bytes(workspace_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile_id"], "enterprise");
        assert_eq!(json["workspace_root"], workspace.display().to_string());

        let files_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?dir=src")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(files_response.status(), StatusCode::OK);
        let body = to_bytes(files_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["dir"], "src");
        assert_eq!(json["files"][0]["name"], "main.rs");
        assert_eq!(json["files"][0]["path"], "src/main.rs");
        assert_eq!(json["files"][0]["type"], "file");
    }

    #[tokio::test]
    async fn workspace_api_creates_reads_and_rejects_escape_paths() {
        let workspace = test_temp_dir("workspace-create");
        let config_home = test_temp_dir("workspace-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/workspace/files")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "path": "notes/audit.txt",
                            "content": "workspace isolation verified"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        assert_eq!(
            std::fs::read_to_string(workspace.join("notes/audit.txt")).unwrap(),
            "workspace isolation verified"
        );

        let raw_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/file/raw?path=notes/audit.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw_response.status(), StatusCode::OK);
        let body = to_bytes(raw_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"workspace isolation verified");

        let escape_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?dir=..")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(escape_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn profile_api_creates_switches_and_deletes_profiles() {
        let app = api_router(test_state());

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/profiles")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["active_profile"], "default");
        assert_eq!(json["runtime_profile"], "default");

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "name": "Enterprise Ops" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile"]["id"], "enterprise_ops");
        assert_eq!(json["restart_required"], false);

        let switch_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles/switch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "profile": "enterprise_ops" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(switch_response.status(), StatusCode::OK);
        let body = to_bytes(switch_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["active_profile"], "enterprise_ops");
        assert_eq!(json["runtime_profile"], "default");
        assert_eq!(json["restart_required"], true);

        let delete_active_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/profiles/enterprise_ops")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_active_response.status(), StatusCode::BAD_REQUEST);

        let switch_back_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles/switch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "profile": "default" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(switch_back_response.status(), StatusCode::OK);

        let delete_response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/profiles/enterprise_ops")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_returns_empty() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_reads_unified_store_metadata() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                "stored-session",
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_filters_and_paginates_unified_store() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());

        let mut auth_a = new_api_session_record("auth-a", Some("claude-sonnet-4-6".into()));
        auth_a.metadata_json = Some(serde_json::json!({"title":"Auth Audit A"}).to_string());
        auth_a.message_count = 3;
        auth_a.last_activity = "2026-06-04T00:03:00Z".to_string();
        store.create_session(&auth_a).await.unwrap();

        let mut auth_b = new_api_session_record("auth-b", Some("claude-sonnet-4-6".into()));
        auth_b.metadata_json = Some(serde_json::json!({"title":"Auth Audit B"}).to_string());
        auth_b.message_count = 8;
        auth_b.last_activity = "2026-06-04T00:08:00Z".to_string();
        store.create_session(&auth_b).await.unwrap();

        let mut closed = new_api_session_record("auth-closed", Some("claude-sonnet-4-6".into()));
        closed.metadata_json = Some(serde_json::json!({"title":"Auth Closed"}).to_string());
        closed.status = "closed".to_string();
        closed.message_count = 99;
        store.create_session(&closed).await.unwrap();

        let mut other_model =
            new_api_session_record("auth-other-model", Some("claude-haiku-4-5".into()));
        other_model.metadata_json =
            Some(serde_json::json!({"title":"Auth Other Model"}).to_string());
        store.create_session(&other_model).await.unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions?q=auth&model=claude-sonnet-4-6&status=active&sort=message_count&order=desc&limit=1&offset=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 2);
        assert_eq!(json["limit"], 1);
        assert_eq!(json["sessions"][0]["id"], "auth-b");
        assert_eq!(json["sessions"][0]["status"], "active");
        assert_eq!(json["sessions"][0]["model"], "claude-sonnet-4-6");
    }

    #[tokio::test]
    async fn get_session_prefers_unified_store_metadata() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "metadata-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("stored-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], session_id);
        assert_eq!(json["model"], "stored-model");
        assert!(json["created_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn patch_session_updates_cold_store_metadata() {
        let session_id = "patch-session";
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/sessions/{session_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"title":"Patch Session Title","model":"patched-model"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let record = store
            .get_session(session_id)
            .await
            .unwrap()
            .expect("stored session");
        assert_eq!(record.model.as_deref(), Some("patched-model"));
        assert!(
            record
                .metadata_json
                .as_deref()
                .unwrap_or("")
                .contains("Patch Session Title")
        );
    }

    #[tokio::test]
    async fn sync_runtime_session_persists_messages_and_events_idempotently() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        let session_id = "sync-session";
        let mut session = runtime::Session::new();
        session.session_id = session_id.to_string();
        session.model = Some("test-model".to_string());
        session.push_user_text("hello store").unwrap();
        session
            .push_message(runtime::ConversationMessage::assistant(vec![
                runtime::ContentBlock::Text {
                    text: "hello user".to_string(),
                },
            ]))
            .unwrap();

        sync_runtime_session_metadata_to_store(&store, session_id, &session)
            .await
            .unwrap();
        sync_runtime_session_metadata_to_store(&store, session_id, &session)
            .await
            .unwrap();

        let record = store.get_session(session_id).await.unwrap().unwrap();
        let messages = store.get_messages(session_id, 0, 10).await.unwrap();
        let events = store.get_events(session_id, 0).await.unwrap();

        assert_eq!(record.message_count, 2);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[1].role, "assistant");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "message_appended");
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);

        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "TextDelta".to_string(),
                event_json: serde_json::json!({
                    "type": "TextDelta",
                    "content": "streamed",
                })
                .to_string(),
                sequence: 99,
                created_at_ms: 99,
            })
            .await
            .unwrap();

        session.messages.truncate(1);
        sync_runtime_session_metadata_to_store(&store, session_id, &session)
            .await
            .unwrap();
        let record = store.get_session(session_id).await.unwrap().unwrap();
        let messages = store.get_messages(session_id, 0, 10).await.unwrap();
        let events = store.get_events(session_id, 0).await.unwrap();

        assert_eq!(record.message_count, 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, "user");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].event_type, "TextDelta");
    }

    #[tokio::test]
    async fn append_session_timeline_event_persists_stream_events_in_order() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        let session_id = "timeline-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        append_session_timeline_event(
            &store,
            session_id,
            "TurnStarted",
            serde_json::json!({"type":"TurnStarted"}),
        )
        .await;
        append_session_timeline_event(
            &store,
            session_id,
            "TextDelta",
            serde_json::json!({"type":"TextDelta","content":"hello"}),
        )
        .await;

        let events = store.get_events(session_id, 0).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "TurnStarted");
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].event_type, "TextDelta");
        assert_eq!(events[1].sequence, 1);
        assert!(events[1].event_json.contains("hello"));
    }

    #[tokio::test]
    async fn session_messages_support_sequence_paging_and_limit_cap() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "message-page-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let messages: Vec<memory::store::session::SessionMessage> = (0..1000)
            .map(|i| memory::store::session::SessionMessage {
                session_id: session_id.to_string(),
                sequence: i,
                role: if i % 2 == 0 { "user" } else { "assistant" }.to_string(),
                content_json: serde_json::json!([{"type":"text","text":format!("message {i}")}])
                    .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: i as u64,
            })
            .collect();
        store.insert_messages_batch(&messages).await.unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/messages?from_seq=990&limit=999"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1000);
        assert_eq!(json["limit"], 500);
        assert_eq!(json["from_seq"], 990);
        assert_eq!(json["next_seq"], 1000);
        assert_eq!(json["has_more"], false);
        assert_eq!(json["messages"].as_array().unwrap().len(), 10);
        assert_eq!(json["messages"][0]["sequence"], 990);
        assert_eq!(json["messages"][9]["sequence"], 999);
    }

    #[tokio::test]
    async fn delete_session_removes_cold_store_record() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cold-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store.get_session(session_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn session_events_reads_unified_store_event_log() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "event-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({
                    "type": "message_appended",
                    "sequence": 0,
                    "role": "user",
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 1_234,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/events?from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["events"][0]["type"], "message_appended");
        assert_eq!(json["events"][0]["sequence"], 0);
        assert_eq!(json["events"][0]["payload"]["role"], "user");
        assert_eq!(json["has_more"], false);
    }

    fn test_context_envelope(
        session_id: &str,
        envelope_id: &str,
        intent: &str,
    ) -> serde_json::Value {
        let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile: ContextProfile::MainTurn,
            identity: ContextIdentity::main(session_id),
            intent: intent.to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![ContextItem::new(
                format!("{envelope_id}-item"),
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                "orientation",
            )],
            omitted: Vec::new(),
            total_budget_tokens: 4_000,
        });
        envelope.id = envelope_id.to_string();
        serde_json::json!({
            "type": "ContextEnvelope",
            "envelope_id": envelope.id,
            "run_id": format!("run-{envelope_id}"),
            "session_id": session_id,
            "envelope": envelope,
        })
    }

    #[tokio::test]
    async fn session_context_history_reads_context_events_only() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-history-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"skip"}),
            ),
            (
                1,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-1", "first"),
            ),
            (
                2,
                "ToolStart",
                serde_json::json!({"type":"ToolStart","name":"skip"}),
            ),
            (
                3,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-2", "second"),
            ),
        ] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 2);
        assert_eq!(json["envelopes"][0]["sequence"], 1);
        assert_eq!(json["envelopes"][0]["envelope_id"], "env-1");
        assert_eq!(json["envelopes"][0]["run_id"], "run-env-1");
        assert_eq!(json["envelopes"][1]["envelope"]["intent"], "second");
    }

    #[tokio::test]
    async fn context_envelope_route_reads_by_envelope_id() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-id-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-target", "inspect").to_string(),
                sequence: 4,
                created_at_ms: 4,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-target")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["source"], "history");
        assert_eq!(json["context"]["session_id"], session_id);
        assert_eq!(json["context"]["sequence"], 4);
        assert_eq!(json["context"]["envelope"]["id"], "env-target");
    }

    #[tokio::test]
    async fn context_recommendation_action_records_session_event() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-recommendation-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store.clone());
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/sessions/{session_id}/context/recommendations"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "envelope_id": "env-1",
                            "recommendation": "Start a handoff",
                            "action": "acknowledged",
                            "note": "handled"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = store.get_events(session_id, 0).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "ContextRecommendationAction");
        let payload: serde_json::Value = serde_json::from_str(&events[0].event_json).unwrap();
        assert_eq!(payload["envelope_id"], "env-1");
        assert_eq!(payload["recommendation"], "Start a handoff");
        assert_eq!(payload["note"], "handled");
    }

    #[tokio::test]
    async fn context_recommendation_stats_groups_actions() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-recommendation-stats-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, action) in [(0, "acknowledged"), (1, "applied")] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: "ContextRecommendationAction".to_string(),
                    event_json: serde_json::json!({
                        "type": "ContextRecommendationAction",
                        "session_id": session_id,
                        "envelope_id": format!("env-{sequence}"),
                        "recommendation": "Start a handoff",
                        "action": action,
                    })
                    .to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context/recommendations?limit=20"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(json["recommendations"][0]["recommendation"], "Start a handoff");
        assert_eq!(json["recommendations"][0]["count"], 2);
        assert_eq!(
            json["recommendations"][0]["actions"]["acknowledged"],
            1
        );
        assert_eq!(json["recommendations"][0]["actions"]["applied"], 1);
        assert_eq!(json["recommendations"][0]["latest_envelope_id"], "env-1");
    }

    #[test]
    fn task_resume_context_packet_summarizes_current_task() {
        let path = std::env::temp_dir().join(format!(
            "cowd-api-task-packet-{}.json",
            uuid::Uuid::new_v4()
        ));
        let kernel = TaskKernel::open(path.clone()).unwrap();
        let task = kernel.start_goal("ship context runtime", true).unwrap();
        let phase_id = task.phases[0].id.clone();
        kernel
            .record_phase_artifact(
                &task.id,
                &phase_id,
                "evidence",
                "test",
                "cargo test -p runtime context_runtime",
            )
            .unwrap();
        let task = kernel.current().unwrap();

        let packet = task_resume_context_packet("session-task", &task);

        assert_eq!(packet.session_id, "session-task");
        assert_eq!(packet.source, ResumeContextSource::TaskRegistry);
        assert!(
            packet
                .active_task
                .as_deref()
                .is_some_and(|task| task.contains("ship context runtime"))
        );
        assert!(
            packet
                .recent_decisions
                .iter()
                .any(|event| event.contains("artifact"))
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn tools_returns_list() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn approval_routes_resolve_pending_gate_request() {
        let gate = test_approval_gate();
        let state = test_state_with_approval_gate(gate.clone());
        let app = api_router(state);

        let eval_gate = gate.clone();
        let eval = tokio::spawn(async move {
            eval_gate
                .evaluate("bash", r#"{"command":"rm -rf /tmp/cowd-approval-e2e"}"#)
                .await
        });

        let pending_json = loop {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/approval/pending")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if json.as_array().is_some_and(|items| !items.is_empty()) {
                break json;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let request_id = pending_json[0]["id"].as_str().unwrap().to_string();
        assert!(
            pending_json[0]["command"]
                .as_str()
                .unwrap()
                .contains("rm -rf")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": request_id,
                            "approved": true,
                            "persistence": "once"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let result = eval.await.unwrap();
        assert!(matches!(
            result,
            runtime::approval_gate::ApprovalGateResult::Approved { .. }
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        let history_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history_response.status(), StatusCode::OK);
        let body = to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let history: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(history[0]["request_id"], pending_json[0]["id"]);

        let export_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/audit/export?source=approval&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(export_response.status(), StatusCode::OK);
        let body = to_bytes(export_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let export: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(export["kind"], "audit_export");
        assert_eq!(export["source"], "approval");
        assert_eq!(export["totals"]["approval"], 1);
        assert_eq!(export["records"][0]["source"], "approval");
        assert_eq!(
            export["records"][0]["record"]["request_id"],
            pending_json[0]["id"]
        );
    }

    #[tokio::test]
    async fn approval_config_and_solo_routes_are_available_without_gate() {
        let app = api_router(test_state());
        let config_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(config_response.status(), StatusCode::OK);
        let body = to_bytes(config_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["solo_mode"], false);

        let solo_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/solo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(solo_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn context_current_returns_degraded_envelope_without_memory() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=ship&session_id=session-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["envelope"]["identity"]["session_id"], "session-1");
        assert_eq!(json["envelope"]["intent"], "ship");
        assert_eq!(
            json["envelope"]["assembled"]["stable_head"][0],
            "cowd-context-runtime:v0.8.13"
        );
        assert_eq!(
            json["envelope"]["diagnostics"]["degraded_sources"][0],
            "Memory"
        );
        assert_eq!(json["lean_probe"]["envelope_id"], json["envelope"]["id"]);
        assert_eq!(json["lean_probe"]["pressure_level"], "Nominal");
        assert_eq!(json["lean_probe"]["degradation_path"], "SourceFallback");
        assert_eq!(
            json["policy_decision"]["action"],
            "PreferOrientationPacket"
        );
        assert_eq!(
            json["policy_decision"]["stable_head_hash"],
            json["lean_probe"]["stable_head_hash"]
        );
    }

    #[tokio::test]
    async fn context_current_accepts_profile_query_for_synthetic_envelope() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=ship&session_id=session-1&profile=yolo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["envelope"]["profile"], "YoloGoal");
        assert_eq!(json["envelope"]["identity"]["mode"], "YoloGoal");
        assert_eq!(
            json["envelope"]["budget"]["leases"][0]["source"],
            "Task"
        );
        assert!(
            json["envelope"]["assembled"]["runtime_header"][0]
                .as_str()
                .unwrap()
                .contains("profile:YoloGoal")
        );
    }

    #[tokio::test]
    async fn session_runs_route_reads_runtime_run_events_only() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-runs-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"skip"}),
            ),
            (
                1,
                "RuntimeRun",
                runtime_run_started_payload(
                    session_id,
                    "run-1",
                    ContextProfile::MainTurn,
                    "ship",
                    10,
                ),
            ),
            (
                2,
                "RuntimeRun",
                runtime_run_completed_payload(
                    session_id,
                    "run-1",
                    ContextProfile::MainTurn,
                    "completed",
                    Some(2),
                    Some("ctx-1".to_string()),
                    None,
                    10,
                    25,
                ),
            ),
            (
                3,
                "RuntimeRun",
                serde_json::json!({
                    "type": "RuntimeRun",
                    "phase": "completed",
                    "run_id": "agent-run-1",
                    "parent_run_id": "run-1",
                    "session_id": session_id,
                    "kind": "agent_task",
                    "profile": ContextProfile::SubAgent,
                    "status": "failed",
                    "started_at_ms": 11,
                    "completed_at_ms": 20,
                    "duration_ms": 9,
                    "error": "review failed",
                    "refs": [],
                }),
            ),
        ] {
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/runs?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 3);
        assert_eq!(json["runs"].as_array().unwrap().len(), 3);
        assert_eq!(json["runs"][0]["run"]["phase"], "started");
        assert_eq!(json["runs"][0]["run"]["kind"], "main_turn");
        assert_eq!(json["runs"][0]["run"]["started_at_ms"], 10);
        assert_eq!(json["runs"][1]["run"]["status"], "completed");
        assert_eq!(json["runs"][1]["run"]["context_envelope_id"], "ctx-1");
        assert_eq!(json["runs"][1]["run"]["duration_ms"], 15);
        assert_eq!(json["runs"][1]["run"]["refs"][0]["type"], "context_envelope");
        assert_eq!(json["tree"]["roots"][0], "run-1");
        assert_eq!(json["tree"]["children"]["run-1"][0], "agent-run-1");
        assert_eq!(json["tree"]["summary"]["span_count"], 2);
        assert_eq!(json["tree"]["summary"]["failed_count"], 1);
    }

    #[tokio::test]
    async fn runtime_run_large_page_query_is_bounded() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-runs-large-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        for sequence in 0..120u64 {
            let run_id = format!("run-{sequence}");
            store
                .append_event(&memory::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: "RuntimeRun".to_string(),
                    event_json: runtime_run_completed_payload(
                        session_id,
                        &run_id,
                        ContextProfile::MainTurn,
                        "completed",
                        Some(1),
                        None,
                        None,
                        sequence.saturating_mul(10),
                        sequence.saturating_mul(10).saturating_add(3),
                    )
                    .to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence,
                })
                .await
                .unwrap();
        }

        let app = api_router(test_state_with_store(store));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/runs?limit=25"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["total"], 120);
        assert_eq!(json["runs"].as_array().unwrap().len(), 25);
        assert_eq!(json["tree"]["summary"]["span_count"], 25);
        assert_eq!(json["tree"]["summary"]["root_count"], 25);
    }

    #[tokio::test]
    async fn evidence_resolver_reads_tool_events_by_ref() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "evidence-tool-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&memory::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ToolComplete".to_string(),
                event_json: serde_json::json!({
                    "type": "ToolComplete",
                    "id": "tool-1",
                    "name": "bash",
                    "summary": "tests passed",
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 1,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/evidence/resolve?session_id={session_id}&ref=tool%3A%2F%2Ftool-1%2Fevidence%2Fevent-1"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["available"], true);
        assert_eq!(json["kind"], "tool");
        assert_eq!(json["events"][0]["payload"]["summary"], "tests passed");
    }

    #[tokio::test]
    async fn evidence_resolver_rejects_unsupported_refs() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/evidence/resolve?ref=unknown%3A%2F%2Fvalue")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn task_api_starts_reports_and_blocks_after_repeated_failures() {
        let app = api_router(test_state());
        let start_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "finish v0.8.10",
                            "yolo_mode": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start_response.status(), StatusCode::CREATED);
        let start_body = to_bytes(start_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: serde_json::Value = serde_json::from_slice(&start_body).unwrap();
        let task_id = started["id"].as_str().expect("task id").to_string();
        assert_eq!(started["status"], "running");
        assert_eq!(started["yolo_mode"], true);

        for reason in ["first", "second", "external input required"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/tasks/{task_id}/failure"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({ "reason": reason }).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tasks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["tasks"][0]["status"], "blocked");
        assert_eq!(
            status_json["tasks"][0]["blocker_reason"],
            "external input required"
        );
    }

    #[tokio::test]
    async fn task_api_records_phase_artifacts_and_review() {
        let app = api_router(test_state());
        let start_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "ship task phase",
                            "yolo_mode": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start_response.status(), StatusCode::CREATED);
        let start_body = to_bytes(start_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: serde_json::Value = serde_json::from_slice(&start_body).unwrap();
        let task_id = started["id"].as_str().unwrap().to_string();

        let phase_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "browser-e2e",
                            "objective": "cover WebUI task panel",
                            "plan": ["add playwright spec"],
                            "acceptance": ["2 e2e tests pass"],
                            "test_commands": ["cd webui && npm run test:e2e"],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(phase_response.status(), StatusCode::CREATED);
        let phase_body = to_bytes(phase_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let phase_json: serde_json::Value = serde_json::from_slice(&phase_body).unwrap();
        let phase_id = phase_json["phases"].as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(phase_json["current_phase"], "browser-e2e");

        let artifact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases/{phase_id}/artifacts"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "kind": "test",
                            "label": "playwright",
                            "value": "2 passed",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(artifact_response.status(), StatusCode::OK);

        let review_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases/{phase_id}/review"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "result": "accepted",
                            "completed": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(review_response.status(), StatusCode::OK);
        let review_body = to_bytes(review_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let review_json: serde_json::Value = serde_json::from_slice(&review_body).unwrap();
        let reviewed_phase = review_json["phases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|phase| phase["id"] == phase_id)
            .unwrap();
        assert_eq!(reviewed_phase["status"], "completed");
        assert_eq!(reviewed_phase["review_result"], "accepted");
        assert_eq!(reviewed_phase["artifacts"][0]["label"], "playwright");
    }

    #[tokio::test]
    async fn memory_without_config_returns_disabled() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["status"], "disabled");
        assert_eq!(json["context_health"]["level"], "unavailable");
        assert_eq!(json["kernel_health"]["degraded"], true);
        assert_eq!(
            json["kernel_health"]["degraded_reasons"][0],
            "memory not configured"
        );
    }

    #[tokio::test]
    async fn memory_maintenance_without_config_degrades() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["degraded_reason"], "memory not configured");
        assert!(json["candidates"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_maintenance_scan_and_transition() {
        let dir = std::env::temp_dir().join(format!(
            "cowd-api-maintenance-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        let id = MemoryId::new_v4();
        manager
            .remember(MemoryEntry {
                id,
                layer: MemoryLayer::L2,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::UserExplicit,
                title: "Old context rule".to_string(),
                content: "Prefer bounded context packets".to_string(),
                embedding: None,
                tags: vec![],
                relations: vec![],
                confidence: 0.7,
                access_count: 0,
                staleness: 0.95,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::Session("maintenance-test".to_string()),
                session_id: None,
                source_agent: None,
                visibility: AgentVisibility::Shared,
            })
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let scan_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/maintenance")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"stale_threshold":0.9}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scan_response.status(), StatusCode::OK);
        let scan_body = to_bytes(scan_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let scan_json: serde_json::Value = serde_json::from_slice(&scan_body).unwrap();
        let candidate_id = scan_json["candidates"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(scan_json["candidates"][0]["kind"], "stale");

        let ack_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/memory/maintenance/{candidate_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"status":"acknowledged"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack_response.status(), StatusCode::OK);
        let ack_body = to_bytes(ack_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let ack_json: serde_json::Value = serde_json::from_slice(&ack_body).unwrap();
        assert_eq!(ack_json["candidate"]["status"], "acknowledged");
    }

    #[tokio::test]
    async fn memory_maintenance_rejects_invalid_status_filter() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance?status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["degraded_reason"], "memory not configured");

        let dir = std::env::temp_dir().join(format!(
            "cowd-api-maintenance-invalid-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance?status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn memory_recall_explain_reports_source_mode_and_score() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::ProjectKnowledge,
                "SessionKernel migration",
                "SessionKernel owns durable sessions and task phase review evidence.",
                Priority::High,
                vec!["session".into(), "task".into()],
                MemoryScope::Project("api-test".to_string()),
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/recall/explain?q=SessionKernel&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["query"], "SessionKernel");
        assert_eq!(json["degraded"], false);
        assert_eq!(json["results"][0]["source_layer"], "L3");
        assert_eq!(json["results"][0]["category"], "ProjectKnowledge");
        assert!(json["results"][0]["score"].as_f64().is_some());
        assert!(json["results"][0]["mode"].as_str().is_some());
        assert!(
            json["results"][0]["snippet"]
                .as_str()
                .unwrap_or_default()
                .contains("SessionKernel")
        );

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_packet_returns_explainable_packet() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-packet-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L2,
            category: MemoryCategory::ProjectKnowledge,
            priority: Priority::High,
            source: MemorySource::UserExplicit,
            title: "PACKET_API_ALPHA".to_string(),
            content: "PACKET_API_ALPHA should appear in an explainable packet.".to_string(),
            embedding: None,
            tags: vec!["packet".to_string()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Session("api-memory-packet".to_string()),
            session_id: Some("api-memory-packet".to_string()),
            source_agent: Some("api".to_string()),
            visibility: AgentVisibility::Shared,
        };
        manager.remember(entry).await.unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/packet?q=PACKET_API_ALPHA&max_items=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert!(json["packet"]["selected"].as_array().unwrap().len() <= 5);

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_links_returns_kernel_links() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let target_id = manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Reference,
                "Link Target",
                "target",
                Priority::Normal,
                vec!["api-link".to_string()],
                MemoryScope::Global,
            )
            .await
            .unwrap();
        let source = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: "Link Source".to_string(),
            content: "source".to_string(),
            embedding: None,
            tags: vec![],
            relations: vec![memory::Relation {
                target_id,
                kind: memory::RelationKind::DependsOn,
                strength: 0.8,
                temporal: None,
                entity: None,
            }],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Shared,
        };
        manager.remember(source).await.unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/links")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["total"].as_u64().unwrap() >= 1);

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_layers_and_entries_read_real_store() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                MemoryLayer::L4,
                MemoryCategory::Shared,
                "Team Decision",
                "Use SessionKernel as the source of truth for v0.8.10.",
                Priority::High,
                vec!["team_relevant".into()],
                MemoryScope::Global,
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let status_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["enabled"], true);
        assert_eq!(status_json["status"], "ready");
        assert_eq!(status_json["context_health"]["level"], "healthy");
        assert_eq!(status_json["kernel_health"]["degraded"], false);
        assert_eq!(status_json["kernel_health"]["stale_pressure"], 0.0);
        assert!(
            status_json["kernel_health"]["evidence_coverage"]
                .as_f64()
                .is_some()
        );

        let layers_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/layers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(layers_response.status(), StatusCode::OK);
        let layers_body = to_bytes(layers_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let layers_json: serde_json::Value = serde_json::from_slice(&layers_body).unwrap();
        let l4_count = layers_json["layers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|layer| layer["layer"] == "L4")
            .and_then(|layer| layer["entry_count"].as_u64())
            .unwrap_or_default();
        assert_eq!(l4_count, 1);

        let entries_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries_response.status(), StatusCode::OK);
        let entries_body = to_bytes(entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let entries_json: serde_json::Value = serde_json::from_slice(&entries_body).unwrap();
        assert_eq!(entries_json["entries"].as_array().unwrap().len(), 1);
        assert_eq!(entries_json["entries"][0]["title"], "Team Decision");
    }

    #[tokio::test]
    async fn memory_entry_update_route_updates_real_store() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-update-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L3")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "Update target",
                            "content": "original memory content",
                            "category": "Reference",
                            "priority": "Normal",
                            "tags": ["before"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let create_body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let create_json: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let id = create_json["id"].as_str().unwrap();

        let update_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/memory/entry/{id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "content": "updated memory content",
                            "priority": "High",
                            "tags": ["after", "webui"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update_response.status(), StatusCode::OK);

        let entries_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries_response.status(), StatusCode::OK);
        let entries_body = to_bytes(entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let entries_json: serde_json::Value = serde_json::from_slice(&entries_body).unwrap();
        let entry = entries_json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == id)
            .unwrap();
        assert_eq!(entry["content"], "updated memory content");
        assert_eq!(entry["priority"], "High");
        assert_eq!(entry["tags"][0], "after");
    }

    #[tokio::test]
    async fn audit_export_includes_memory_write_audit() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-audit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L3")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "audit-export-memory",
                            "content": "COWD_AUDIT_EXPORT_MEMORY_WRITE",
                            "category": "Reference",
                            "priority": "High",
                            "tags": ["audit", "e2e"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let export_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/audit/export?source=memory&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(export_response.status(), StatusCode::OK);
        let body = to_bytes(export_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "audit_export");
        assert_eq!(json["source"], "memory");
        assert_eq!(json["totals"]["memory"], 1);
        assert_eq!(json["records"][0]["source"], "memory");
        assert_eq!(
            json["records"][0]["record"]["summary"],
            "COWD_AUDIT_EXPORT_MEMORY_WRITE"
        );
        assert_eq!(json["memory"][0]["operation"], "Create");
        assert_eq!(json["memory"][0]["layer"], "L3");

        let _ = std::fs::remove_dir_all(tmp);
    }

    #[tokio::test]
    async fn memory_symbol_links_roundtrip_real_store() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-symbol-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let memory_id = manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Reference,
                "Auth impact note",
                "authenticate_user controls login policy and API auth behavior.",
                Priority::High,
                vec!["symbol".into(), "auth".into()],
                MemoryScope::Global,
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let link_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/symbol-links")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "symbol_id": "src/auth.rs:authenticate_user:42",
                            "memory_id": memory_id.to_string(),
                            "turn_index": 7,
                            "reference_type": "impact"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(link_response.status(), StatusCode::CREATED);

        let lookup_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/symbol-links?symbol=authenticate_user")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lookup_response.status(), StatusCode::OK);
        let body = to_bytes(lookup_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1);
        assert_eq!(json["entries"][0]["id"], memory_id.to_string());
        assert_eq!(json["entries"][0]["title"], "Auth impact note");
    }

    #[tokio::test]
    async fn config_returns_version() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_auth_allows_no_auth_configuration() {
        let state = test_state();
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/verify")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_required_when_token_set() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        let state = Arc::new(AppState {
            session_kernel: test_session_kernel(sessions.clone(), None, event_bus.clone()),
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_passes_with_valid_token() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        let state = Arc::new(AppState {
            session_kernel: test_session_kernel(sessions.clone(), None, event_bus.clone()),
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            approval_gate: None,
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: default_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            task_kernel: test_task_kernel(),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
