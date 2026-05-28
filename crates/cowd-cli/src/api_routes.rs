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
use tokio_stream::wrappers::UnboundedReceiverStream;

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
        .route("/api/sessions/:id", get(get_session).delete(delete_session))
        .route("/api/sessions/:id/messages", get(get_session_messages).post(send_message))
        .route("/api/sessions/:id/stream", get(sse_stream_handler))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/search", get(memory_search_handler))
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
struct SessionListResponse {
    sessions: Vec<SessionInfo>,
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

// ── Handlers ───────────────────────────────────────────────────

async fn health_handler() -> &'static str {
    "OK"
}

async fn list_sessions(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let ids = state.sessions.list();
    let sessions: Vec<SessionInfo> = ids
        .into_iter()
        .map(|id| SessionInfo {
            id,
            status: "active".to_string(),
        })
        .collect();
    Json(SessionListResponse { sessions })
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
            let _ = store.delete_session(&id);
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

    // Subscribe to the runtime's internal EventBus so we can forward
    // real-time TextDelta events to the SessionEventBus for SSE subscribers.
    let rx = {
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.subscribe_to_bus()
    };

    // Spawn a background task that bridges runtime::bus::Event → SessionEventBus.
    if let Some(mut rx) = rx {
        let sse_bus = Arc::clone(&event_bus);
        let sid = session_id.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                if let runtime::bus::Event::TextDelta { content } = event {
                    let sse = serde_json::json!({"type":"TextDelta","content":content});
                    let _ = sse_bus.broadcast(&sid, &sse.to_string()).await;
                }
            }
        });
    }

    let mut runtime_guard = runtime_entry.lock().await;

<<<<<<< Updated upstream
    // Subscribe runtime EventBus → forward TextDelta to SessionEventBus
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

    const TURN_TIMEOUT: Duration = Duration::from_secs(300);

    match timeout(TURN_TIMEOUT, runtime_guard.run_turn_async(&body.content, &runtime::permissions::SharedPrompter::none())).await {
        Ok(Ok(summary)) => {
=======
    let sse_cb_event_bus = Arc::clone(&event_bus);
    let sse_cb_session_id = session_id.clone();
    let sse_cb: Arc<dyn Fn(String) + Send + Sync> = Arc::new(move |sse_data: String| {
        let bus = Arc::clone(&sse_cb_event_bus);
        let sid = sse_cb_session_id.clone();
        tokio::spawn(async move {
            bus.broadcast(&sid, &sse_data).await;
        });
    });
    runtime_guard.set_sse_callback(sse_cb);

    match runtime_guard.run_turn_async(&body.content, &runtime::permissions::SharedPrompter::none()).await {
        Ok(summary) => {
>>>>>>> Stashed changes
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
        Ok(Err(e)) => {
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
        Err(_elapsed) => {
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
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    let runtime_guard = runtime_entry.lock().await;
    let session = runtime_guard.session();

    let messages: Vec<serde_json::Value> = session
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

    Ok(Json(serde_json::json!({
        "session_id": id,
        "message_count": messages.len(),
        "messages": messages,
    })))
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
    rx: UnboundedReceiverStream<String>,
    session_id: String,
    event_bus: Arc<SessionEventBus>,
    tx: mpsc::UnboundedSender<String>,
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
    let (tx, rx) = mpsc::unbounded_channel();
    // Clone tx before subscribing — one copy moves into the event bus,
    // the other stays with SseStream for cleanup on drop.
    let bus_tx = tx.clone();
    state.event_bus.subscribe(&session_id, bus_tx).await;

    let stream = SseStream {
        rx: UnboundedReceiverStream::new(rx),
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
