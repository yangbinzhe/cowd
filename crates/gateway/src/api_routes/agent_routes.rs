use std::sync::Arc;

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use harness_contract::execution_graph::{ExecutionEdge, ExecutionNodeSpec};
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
        .route(
            "/api/agents/execution-graphs",
            get(execution_graphs_handler),
        )
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
            "/api/tasks/:id/execution-graph",
            get(task_execution_graph_handler).post(register_task_execution_graph_handler),
        )
}

#[derive(Deserialize)]
struct RegisterExecutionGraphRequest {
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    nodes: Vec<ExecutionNodeSpec>,
    #[serde(default)]
    edges: Vec<ExecutionEdge>,
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

async fn execution_graphs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let value = state
        .services
        .agent
        .list_execution_graphs(&state.services.task)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(value))
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

async fn runtime_agent_cancel_handler(Path(id): Path<String>) -> impl IntoResponse {
    Json(agent_execution_capability_unavailable(
        "runtime.agent.cancel",
        &id,
        None,
        None,
    ))
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
    Ok(Json(agent_execution_capability_unavailable(
        "runtime.agent.command",
        id,
        Some(command),
        payload,
    )))
}

fn agent_execution_capability_unavailable(
    kind: &str,
    agent_id: &str,
    command: Option<runtime::AgentExecutionCommandKind>,
    payload: Option<Value>,
) -> Value {
    serde_json::json!({
        "kind": kind,
        "ok": false,
        "status": "capability_unavailable",
        "capability": "agent_execution",
        "available_in": "V5",
        "side_effects_started": false,
        "agent_id": agent_id,
        "command": command,
        "payload": payload,
    })
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

async fn task_execution_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let graph = state
        .services
        .agent
        .execution_graph(&state.services.task, &id)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "execution graph not found"))?;
    Ok(Json(graph))
}

async fn register_task_execution_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<RegisterExecutionGraphRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let graph = state
        .services
        .agent
        .register_execution_graph(
            &state.services.task,
            &id,
            body.objective,
            body.nodes,
            body.edges,
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
        "next_actions": ["open profile", "reuse in execution graph", "evaluate team run"],
    })
}
