// ── API Routes (T7) — Minimal API using ActiveSessions ────────
// Core session management routes shared between TUI and HTTP API.
// DO NOT delete old server/mod.rs yet (T16 will do that).

use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use tools::GlobalToolRegistry;

use crate::gateway::ActiveSessions;
use memory::cognitive::CognitiveContextManager;

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub sessions: Arc<ActiveSessions>,
    pub memory_manager: Option<Arc<CognitiveContextManager>>,
    pub tool_registry: Arc<GlobalToolRegistry>,
    pub config: Option<serde_json::Value>,
}

// ── Router ─────────────────────────────────────────────────────

pub fn api_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route("/api/sessions/:id", get(get_session).delete(delete_session))
        .route("/api/sessions/:id/messages", post(send_message))
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
        .with_state(state)
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
    AxumState(_state): AxumState<Arc<AppState>>,
    Json(_body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested (full runtime wiring pending)");
    let info = SessionInfo {
        id: session_id,
        status: "pending".to_string(),
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
    let runtime_arc = state.sessions.remove(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");

    let mut runtime = Arc::try_unwrap(runtime_arc).map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse {
            error: format!("session {id} runtime is still referenced"),
        }))
    })?;

    let session_id = id.clone();
    let sessions = Arc::clone(&state.sessions);

    match runtime.run_turn_async(&body.content, &runtime::permissions::SharedPrompter::none()).await {
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

            sessions.register(session_id, runtime);

            Ok(Json(response))
        }
        Err(e) => {
            let error_msg = e.to_string();
            sessions.register(session_id, runtime);
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
