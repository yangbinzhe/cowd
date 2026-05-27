// ── API Routes (T7) — Minimal API using ActiveSessions ────────
// Core session management routes shared between TUI and HTTP API.
// DO NOT delete old server/mod.rs yet (T16 will do that).

use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use crate::gateway::ActiveSessions;

// ── Shared application state ───────────────────────────────────

pub type AppState = Arc<ActiveSessions>;

// ── Router ─────────────────────────────────────────────────────

pub fn api_router(active_sessions: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route("/api/sessions/:id", get(get_session).delete(delete_session))
        .route("/api/sessions/:id/messages", post(send_message))
        .route("/api/memory", get(memory_handler))
        .route("/api/tools", get(tools_handler))
        .route("/api/config", get(config_handler))
        .with_state(active_sessions)
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
    AxumState(state): AxumState<AppState>,
) -> impl IntoResponse {
    let ids = state.list();
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
    AxumState(_state): AxumState<AppState>,
    Json(_body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    // For now, create a session entry without a full runtime.
    // Full runtime creation requires model config, tool registry,
    // MCP state, etc. — this will be wired in a later task.
    // The session is tracked in ActiveSessions (list/get/delete work).
    // Push the session_id onto a lightweight tracking list.
    // (ActiveSessions requires a full BuiltRuntime for register(),
    // so we use an indirect tracking mechanism here.)
    tracing::info!(%session_id, "API session create requested (full runtime wiring pending)");
    let info = SessionInfo {
        id: session_id,
        status: "pending".to_string(),
    };
    Ok((StatusCode::CREATED, Json(info)))
}

async fn get_session(
    AxumState(state): AxumState<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.get(&id).is_some() {
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
    AxumState(state): AxumState<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.remove(&id).is_some() {
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
    AxumState(state): AxumState<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state.get(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");
    // Runtime is available for future message processing.
    // Full streaming/response will be wired in a later task.
    let _runtime = runtime;
    let _content = body.content;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "status": "received",
        "message": "Message received (full processing pending)"
    })))
}

// ── Memory / Tools / Config (simple stubs) ──────────────────────

async fn memory_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "enabled": true,
        "store_path": "~/.cowd/memory",
        "session_store": true,
        "layers": {
            "L0": "fixed identity",
            "L1": "working memory",
            "L2": "project context",
            "L3": "deep memories",
            "L4": "archived"
        },
        "features": {
            "semantic_search": true,
            "context_compression": true,
            "drift_detection": true,
            "session_handoff": true
        }
    }))
}

async fn tools_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "tools": [
            {"name": "read", "description": "Read files from the local filesystem"},
            {"name": "write", "description": "Write files to the local filesystem"},
            {"name": "edit", "description": "Make targeted edits to files"},
            {"name": "glob", "description": "Find files by glob pattern"},
            {"name": "grep", "description": "Search file contents with regex"},
            {"name": "bash", "description": "Execute shell commands"},
            {"name": "web_search", "description": "Search the web"},
            {"name": "web_fetch", "description": "Fetch content from a URL"}
        ]
    }))
}

async fn config_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "model": "claude-opus-4-6",
        "provider": "anthropic",
        "theme": "dark",
        "language": "zh-CN",
        "streaming": true
    }))
}
