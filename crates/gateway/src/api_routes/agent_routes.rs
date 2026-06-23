use std::sync::Arc;

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;

use crate::services::UpsertAgentTeamProfileRequest;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/agents/catalog", get(agent_catalog_handler))
        .route("/api/agents/directory", get(agent_directory_handler))
        .route("/api/agents/discover", get(agent_discover_handler))
        .route("/api/agents/assemble", post(agent_assemble_handler))
        .route("/api/agents/reputation", get(agent_reputation_handler))
        .route("/api/agents/runs", get(agent_runs_handler))
        .route("/api/runtime/agents", get(runtime_agents_list_handler))
        .route("/api/runtime/agents/:id", get(runtime_agent_detail_handler))
        .route(
            "/api/runtime/agents/:id/events",
            get(runtime_agent_events_handler),
        )
        .route(
            "/api/runtime/agents/:id/cancel",
            post(runtime_agent_cancel_handler),
        )
        .route(
            "/api/runtime/agents/:id/input",
            post(runtime_agent_input_handler),
        )
        .route(
            "/api/runtime/agents/:id/interrupt",
            post(runtime_agent_interrupt_handler),
        )
        .route(
            "/api/runtime/agents/:id/shutdown",
            post(runtime_agent_shutdown_handler),
        )
        .route(
            "/api/agents/team-profiles",
            get(agent_team_profiles_list_handler).post(agent_team_profile_create_handler),
        )
        .route(
            "/api/agents/team-profiles/:id",
            get(agent_team_profile_detail_handler)
                .put(agent_team_profile_update_handler)
                .delete(agent_team_profile_delete_handler),
        )
        .route(
            "/api/tasks/:id/agent-graph",
            get(task_agent_graph_handler).post(upsert_task_agent_graph_handler),
        )
}

#[derive(Deserialize)]
struct UpsertAgentGraphRequest {
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    nodes: Vec<Value>,
}

#[derive(Deserialize)]
struct AgentDiscoverQuery {
    #[serde(default)]
    task: String,
}

#[derive(Deserialize)]
struct AgentAssembleRequest {
    #[serde(default)]
    task: String,
}

#[derive(Debug, Deserialize)]
struct RuntimeAgentCommandRequest {
    #[serde(default)]
    payload: Option<Value>,
}

async fn agent_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .agent
        .catalog(&state.workspace_root)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn agent_directory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .agent
        .directory(&state.workspace_root)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn agent_discover_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<AgentDiscoverQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = query.task.trim();
    if task.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "task query is required"));
    }
    state
        .services
        .agent
        .discover(&state.workspace_root, task)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn agent_assemble_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<AgentAssembleRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = body.task.trim();
    if task.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "task is required"));
    }
    state
        .services
        .agent
        .assemble(&state.workspace_root, task)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn agent_reputation_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .agent
        .reputation(&state.workspace_root)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn agent_runs_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.agent.list_agent_graphs(&state.services.task))
}

async fn runtime_agents_list_handler() -> impl IntoResponse {
    Json(runtime::global_agent_lifecycle_service().projection())
}

async fn runtime_agent_detail_handler(
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime::global_agent_lifecycle_service()
        .get(&id)
        .map(|agent| {
            Json(serde_json::json!({
                "kind": "runtime.agent",
                "agent": agent,
            }))
        })
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "agent not found"))
}

async fn runtime_agent_events_handler(
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime::global_agent_lifecycle_service()
        .events(&id)
        .map(|events| {
            Json(serde_json::json!({
                "kind": "runtime.agent.events",
                "agentId": id,
                "count": events.len(),
                "events": events,
            }))
        })
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "agent not found"))
}

async fn runtime_agent_cancel_handler(
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime::global_agent_lifecycle_service()
        .cancel(&id)
        .map(|receipt| {
            Json(serde_json::json!({
                "kind": "runtime.agent.cancel",
                "receipt": receipt,
            }))
        })
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))
}

async fn runtime_agent_input_handler(
    Path(id): Path<String>,
    Json(body): Json<RuntimeAgentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(&id, runtime::AgentExecutionCommandKind::Input, body.payload)
}

async fn runtime_agent_interrupt_handler(
    Path(id): Path<String>,
    Json(body): Json<RuntimeAgentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(
        &id,
        runtime::AgentExecutionCommandKind::Interrupt,
        body.payload,
    )
}

async fn runtime_agent_shutdown_handler(
    Path(id): Path<String>,
    Json(body): Json<RuntimeAgentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(
        &id,
        runtime::AgentExecutionCommandKind::Shutdown,
        body.payload,
    )
}

fn runtime_agent_command_result(
    id: &str,
    command: runtime::AgentExecutionCommandKind,
    payload: Option<Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    runtime::global_agent_lifecycle_service()
        .command(id, command, payload)
        .map(|receipt| {
            Json(serde_json::json!({
                "kind": "runtime.agent.command",
                "receipt": receipt,
            }))
        })
        .map_err(|error| api_error(runtime_agent_command_error_status(&error), error))
}

fn runtime_agent_command_error_status(error: &str) -> StatusCode {
    if error.contains("agent not found") {
        StatusCode::NOT_FOUND
    } else if error.contains("does not expose a command channel") {
        StatusCode::CONFLICT
    } else if error.contains("failed to deliver agent command") {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::BAD_REQUEST
    }
}

async fn agent_team_profiles_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profiles = state
        .services
        .agent
        .list_team_profiles(&state.workspace_root)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "agents.team_profiles",
        "count": profiles.len(),
        "profiles": profiles,
        "storage": state.services.agent.team_profiles_path(&state.workspace_root),
    })))
}

async fn agent_team_profile_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .agent
        .get_team_profile(&state.workspace_root, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "team profile not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "agents.team_profile",
        "profile": profile,
    })))
}

async fn agent_team_profile_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<UpsertAgentTeamProfileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .agent
        .create_team_profile(&state.workspace_root, body)
        .map_err(|error| {
            let status = if error == "team profile id already exists" {
                StatusCode::CONFLICT
            } else if error == "team profile name is required" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            api_error(status, error)
        })?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "kind": "agents.team_profile.created",
            "profile": profile,
            "receipt": team_profile_receipt("create", &profile.id),
        })),
    ))
}

async fn agent_team_profile_update_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpsertAgentTeamProfileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .agent
        .update_team_profile(&state.workspace_root, &id, body)
        .map_err(|error| {
            let status = if error == "team profile name is required" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            api_error(status, error)
        })?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "team profile not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "agents.team_profile.updated",
        "profile": profile,
        "receipt": team_profile_receipt("update", &profile.id),
    })))
}

async fn agent_team_profile_delete_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let deleted = state
        .services
        .agent
        .delete_team_profile(&state.workspace_root, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    if !deleted {
        return Err(api_error(StatusCode::NOT_FOUND, "team profile not found"));
    }
    Ok(Json(serde_json::json!({
        "kind": "agents.team_profile.deleted",
        "profile_id": id,
        "receipt": team_profile_receipt("delete", &id),
    })))
}

async fn task_agent_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let graph = state
        .services
        .agent
        .agent_graph(&state.services.task, &id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "agent graph not found"))?;
    Ok(Json(graph))
}

async fn upsert_task_agent_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpsertAgentGraphRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let graph = state
        .services
        .agent
        .upsert_agent_graph(
            &state.services.task,
            &state.services.session,
            &id,
            body.objective,
            body.nodes,
        )
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(graph))
}

fn team_profile_receipt(action: &str, id: &str) -> Value {
    serde_json::json!({
        "request_id": format!("agent-team-profile-{action}-{id}"),
        "mode": "live",
        "status": "ok",
        "changed_refs": [format!("agent-team-profile:{id}")],
        "audit_ref": format!("agent-team-profile:{action}:{id}"),
        "warnings": [],
        "next_actions": ["open profile", "reuse in agent graph", "evaluate team run"],
    })
}
