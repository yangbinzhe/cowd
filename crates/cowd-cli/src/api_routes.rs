// ── API Routes (T7) — Minimal API using ActiveSessions ────────
// Core session management routes shared between TUI and HTTP API.
// DO NOT delete old server/mod.rs yet (T16 will do that).

use std::{collections::HashMap, convert::Infallible, sync::Arc};

use axum::{
    body::Body,
    extract::{Path, Query, State as AxumState},
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, sse::{Event, KeepAlive, Sse}},
    routing::{get, post},
    Router,
};
use futures::stream::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;

use tools::GlobalToolRegistry;

use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use memory::cognitive::CognitiveContextManager;
use memory::session_store::UnifiedSessionStore;

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub sessions: Arc<ActiveSessions>,
    pub memory_manager: Option<Arc<CognitiveContextManager>>,
    pub unified_store: Option<Arc<UnifiedSessionStore>>,
    pub tool_registry: Arc<GlobalToolRegistry>,
    pub config: Option<serde_json::Value>,
    pub event_bus: Arc<SessionEventBus>,
    pub auth_token: Option<String>,
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
        .route("/api/sessions/:id", get(get_session).patch(update_session_handler).delete(delete_session))
        .route("/api/sessions/:id/messages", get(get_session_messages).post(send_message))
        .route("/api/sessions/:id/stream", get(sse_stream_handler))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/stats", get(get_session_stats_handler))
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route("/api/memory/performance", get(performance_handler))
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    public_routes.merge(protected_routes).with_state(state)
}

// ── Response types ─────────────────────────────────────────────

#[derive(Serialize)]
struct SessionInfo {
    id: String,
    status: String,
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
struct LoginRequest {
    token: String,
}

// ── Query param types ────────────────────────────────────────────

#[derive(Deserialize)]
struct ListSessionsParams {
    #[serde(default = "default_sort")]
    sort: String,
    #[serde(default = "default_order")]
    order: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

fn default_sort() -> String { "updated_at".to_string() }
fn default_order() -> String { "desc".to_string() }

#[derive(Deserialize)]
struct GetMessagesParams {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SearchMessagesParams {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

fn default_search_limit() -> usize { 20 }

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

// ── Handlers ───────────────────────────────────────────────────

async fn health_handler() -> &'static str {
    "OK"
}

async fn list_sessions(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<ListSessionsParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.unwrap_or(0);

    // Try unified store first for DB-backed listing
    if let Some(ref store) = state.unified_store {
        let all = store.list_sessions().await.unwrap_or_default();
        let total = all.len();
        let sessions: Vec<SessionInfo> = all
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|r| SessionInfo {
                id: r.session_id,
                status: "active".to_string(),
            })
            .collect();
        return Json(serde_json::json!({
            "sessions": sessions,
            "total": total,
            "offset": offset,
            "limit": limit,
        }));
    }

    // Fallback: in-memory active sessions
    let ids = state.sessions.list();
    let total = ids.len();
    let sessions: Vec<SessionInfo> = ids
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|id| SessionInfo {
            id,
            status: "active".to_string(),
        })
        .collect();
    Json(serde_json::json!({
        "sessions": sessions,
        "total": total,
        "offset": offset,
        "limit": limit,
    }))
}

async fn create_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested");

    let session = runtime::Session::new();
    let runtime = crate::build_runtime(
        session,
        &session_id,
        body.model.unwrap_or_else(|| "claude-sonnet-4-6".to_string()),
        vec![],
        true,
        true,
        None,
        runtime::PermissionMode::WorkspaceWrite,
        None,
        None,
    )
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("failed to build runtime: {e}"),
            }),
        )
    })?;

    if let Err(e) = state.sessions.register(session_id.clone(), runtime) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("failed to register session: {e}"),
            }),
        ));
    }

    let info = SessionInfo {
        id: session_id,
        status: "active".to_string(),
    };
    Ok((StatusCode::CREATED, Json(info)))
}

async fn get_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.sessions.get(&id).is_some() {
        Ok(Json(SessionInfo {
            id,
            status: "active".to_string(),
        }))
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
    if state.sessions.remove(&id).is_some() {
        if let Some(ref store) = state.unified_store {
            let _ = store.delete_session(&id).await;
        }
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

async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");

    let session_id = id.clone();
    let event_bus = Arc::clone(&state.event_bus);

    // Phase 1: Subscribe runtime EventBus → forward TextDelta to SessionEventBus
    // This phase is Send — it only spawns a background task and drops the guard.
    {
        let runtime_guard = runtime_entry.lock().await;
        if let Some(bus) = runtime_guard.bus() {
            let mut rx = bus.subscribe();
            let eb = event_bus.clone();
            let sid = session_id.clone();
            tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    if let runtime::bus::Event::TextDelta { content } = event {
                        let sse = serde_json::json!({"type":"TextDelta","content":content});
                        let _ = eb.broadcast(&sid, &sse.to_string()).await;
                    }
                }
            });
        }
    } // runtime_guard dropped — no MutexGuard held across the .await below

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
            timeout(TURN_TIMEOUT, runtime_guard.run_turn_async(&content, &runtime::permissions::SharedPrompter::none())).await
        })
    }).await;

    // Phase 3: Process result — all work here is Send (tokio::sync channels, serde, Json)
    match turn_result {
        Ok(Ok(Ok(summary))) => {
            let final_text = summary.assistant_messages.last()
                .map(|msg| {
                    msg.blocks.iter()
                        .filter_map(|block| match block {
                            runtime::ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("")
                })
                .unwrap_or_default();

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
            event_bus.broadcast(&session_id, &sse_data.to_string()).await;

            Ok(Json(response))
        }
        Ok(Ok(Err(e))) => {
            let error_msg = e.to_string();

            let sse_data = serde_json::json!({
                "type": "TurnError",
                "session_id": &session_id,
                "error": error_msg,
            });
            event_bus.broadcast(&session_id, &sse_data.to_string()).await;

            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
        Ok(Err(_elapsed)) => {
            let error_msg = format!("turn timed out after {}s", TURN_TIMEOUT.as_secs());

            let sse_data = serde_json::json!({
                "type": "TurnError",
                "session_id": &session_id,
                "error": error_msg,
            });
            event_bus.broadcast(&session_id, &sse_data.to_string()).await;

            Err((
                StatusCode::REQUEST_TIMEOUT,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
        Err(join_err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("task join error: {join_err}") }),
        )),
    }
}

// ── Memory / Tools / Config handlers ───────────────────────────

async fn memory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        let layers = mgr.list_layers().await;
        let vector_count = mgr.vector_index_count();
        Json(serde_json::json!({
            "enabled": true,
            "layers": layers,
            "vector_count": vector_count,
            "session_store": true,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "message": "memory not configured"
        }))
    }
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

async fn performance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
        let report = mgr.performance_report();
        Json(serde_json::json!(report))
    } else {
        Json(serde_json::json!({
            "error": "memory not configured",
        }))
    }
}

async fn tools_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let tools: Vec<serde_json::Value> = state.tool_registry
        .definitions(None)
        .iter()
        .map(|t| serde_json::json!({
            "name": t.name,
            "description": t.description,
            "enabled": true,
        }))
        .collect();
    Json(serde_json::json!({ "tools": tools, "count": tools.len() }))
}

async fn config_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    match &state.config {
        Some(config) => Json(config.clone()),
        None => Json(serde_json::json!({
            "error": "config not loaded",
            "model": "unknown",
            "version": env!("CARGO_PKG_VERSION"),
        })),
    }
}

// ── Session messages handler ────────────────────────────────────

async fn get_session_messages(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(50);

    // Try unified store for DB-backed pagination
    if let Some(ref store) = state.unified_store {
        let total = store.get_message_count(&id).await.unwrap_or(0);
        let db_messages = store.get_messages(&id, offset, limit).await.unwrap_or_default();
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
        let has_more = offset + messages.len() < total;
        return Ok(Json(serde_json::json!({
            "session_id": id,
            "messages": messages,
            "total": total,
            "offset": offset,
            "limit": limit,
            "has_more": has_more,
        })));
    }

    // Fallback: in-memory session
    let runtime_entry = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
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
    let messages: Vec<serde_json::Value> = all_messages
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();
    let has_more = offset + messages.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "messages": messages,
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": has_more,
    })))
}

// ── Session messages search handler ───────────────────────────────

async fn search_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<SearchMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = state.unified_store.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse {
            error: "session store not available".to_string(),
        }))
    })?;

    let db_messages = store.search_messages(&params.q, None, params.limit).await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse {
                error: format!("search failed: {e}"),
            }))
        })?;

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
    let runtime_entry = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    let mut runtime_guard = runtime_entry.lock().await;
    let result = runtime_guard.compact(runtime::CompactionConfig::default());

    // Apply the compacted session back if compaction actually happened
    if result.removed_message_count > 0 {
        *runtime_guard.session_mut() = result.compacted_session.clone();
    }

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
    let runtime_entry = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    let runtime_guard = runtime_entry.lock().await;
    let session = runtime_guard.session();
    let messages = &session.messages;

    let user_count = messages.iter().filter(|m| m.role == runtime::MessageRole::User).count();
    let assistant_count = messages.iter().filter(|m| m.role == runtime::MessageRole::Assistant).count();
    let tool_count = messages.iter().filter(|m| m.role == runtime::MessageRole::Tool).count();

    let total_input_tokens: u32 = messages.iter()
        .filter_map(|m| m.usage.as_ref())
        .map(|u| u.input_tokens)
        .sum();
    let total_output_tokens: u32 = messages.iter()
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
    // Update in-memory session first
    let runtime_entry = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    {
        let mut runtime_guard = runtime_entry.lock().await;
        let mut session = runtime_guard.session_mut();
        if let Some(ref model) = body.model {
            session.model = Some(model.clone());
        }
    }

    // Persist to UnifiedSessionStore if available (read-modify-write)
    if let Some(ref store) = state.unified_store {
        if let Ok(Some(mut record)) = store.get_session(&id).await {
            if let Some(ref model) = body.model {
                record.model = Some(model.clone());
            }
            if let Some(ref title) = body.title {
                let mut meta: serde_json::Value = record.metadata_json
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(serde_json::json!({}));
                meta["title"] = serde_json::Value::String(title.clone());
                record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
            }
            if let Some(ref metadata) = body.metadata {
                let mut meta: serde_json::Value = record.metadata_json
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
            let _ = store.update_session(&record).await;
        }
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
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "auth not configured".to_string(),
                }),
            ));
        }
        Some(token) => token,
    };

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(h) if h == format!("Bearer {auth_token}") => {
            Ok(Json(serde_json::json!({ "valid": true })))
        }
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
    state.event_bus.subscribe(&session_id, bus_tx).await;

    let stream = SseStream {
        rx: ReceiverStream::new(rx),
        session_id,
        event_bus: state.event_bus.clone(),
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
        http::{Request, StatusCode},
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new(); // returns Arc<Self>
        Arc::new(AppState {
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            auth_token: None,
        })
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
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
    async fn memory_without_config_returns_disabled() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
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
    async fn auth_required_when_token_set() {
        let sessions = Arc::new(ActiveSessions::new());
        let tools = Arc::new(GlobalToolRegistry::builtin());
        let event_bus = SessionEventBus::new();
        let state = Arc::new(AppState {
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            auth_token: Some("test-token".into()),
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
            sessions,
            memory_manager: None,
            unified_store: None,
            tool_registry: tools,
            config: None,
            event_bus,
            auth_token: Some("test-token".into()),
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
