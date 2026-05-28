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
use tokio_stream::wrappers::UnboundedReceiverStream;

use tools::GlobalToolRegistry;

use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;
use memory::cognitive::CognitiveContextManager;

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub sessions: Arc<ActiveSessions>,
    pub memory_manager: Option<Arc<CognitiveContextManager>>,
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
    let health_route = Router::new().route("/health", get(health_handler));

    let protected_routes = Router::new()
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route("/api/sessions/:id", get(get_session).delete(delete_session))
        .route("/api/sessions/:id/messages", post(send_message))
        .route("/api/sessions/:id/stream", get(sse_stream_handler))
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    health_route.merge(protected_routes).with_state(state)
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

    state.sessions.register(session_id.clone(), runtime);

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

    let mut runtime_guard = runtime_entry.lock().await;

    match runtime_guard.run_turn_async(&body.content, &runtime::permissions::SharedPrompter::none()).await {
        Ok(summary) => {
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
        Err(e) => {
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

// ── SSE stream handler ──────────────────────────────────────────

async fn sse_stream_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::unbounded_channel();
    state.event_bus.subscribe(&session_id, tx).await;
    let stream = UnboundedReceiverStream::new(rx).map(|s| Ok(Event::default().data(s)));
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}
