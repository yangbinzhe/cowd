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
    #[serde(rename = "commandId")]
    command_id: Option<String>,
    #[serde(rename = "expectedRevision")]
    expected_revision: Option<u64>,
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

async fn runtime_agents_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    Ok(Json(serde_json::json!({
        "kind": "runtime.agent.list",
        "agents": runtime.agent_runtime().list(),
    })))
}

async fn runtime_agent_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_services(&state)?
        .agent_runtime()
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
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    if runtime.agent_runtime().get(&id).is_none() {
        return Err(api_error(StatusCode::NOT_FOUND, "agent not found"));
    }
    let events = runtime.agent_runtime().events(&id);
    Ok(Json(serde_json::json!({
        "kind": "runtime.agent.events",
        "agentId": id,
        "count": events.len(),
        "events": events,
    })))
}

async fn runtime_agent_cancel_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(
        &state,
        &id,
        harness_contract::agent::AgentCommand::Cancel,
        RuntimeAgentCommandRequest {
            command_id: None,
            expected_revision: None,
            payload: None,
        },
    )
    .await
}

async fn runtime_agent_input_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<RuntimeAgentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(
        &state,
        &id,
        harness_contract::agent::AgentCommand::SendInput,
        body,
    )
    .await
}

async fn runtime_agent_interrupt_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<RuntimeAgentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(
        &state,
        &id,
        harness_contract::agent::AgentCommand::Interrupt,
        body,
    )
    .await
}

async fn runtime_agent_shutdown_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<RuntimeAgentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_agent_command_result(
        &state,
        &id,
        harness_contract::agent::AgentCommand::Shutdown,
        body,
    )
    .await
}

async fn runtime_agent_command_result(
    state: &Arc<AppState>,
    id: &str,
    command: harness_contract::agent::AgentCommand,
    body: RuntimeAgentCommandRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(state)?;
    let snapshot = runtime
        .agent_runtime()
        .get(id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "agent not found"))?;
    let input = body.payload.map(|payload| match payload {
        Value::String(text) => harness_contract::agent::AgentInput::UserSupplement(text),
        value => harness_contract::agent::AgentInput::ControlContext(value),
    });
    let receipt = runtime
        .agent_runtime()
        .command(harness_contract::agent::AgentCommandRequest {
            command_id: body
                .command_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            agent_id: id.to_string(),
            expected_revision: body.expected_revision.unwrap_or(snapshot.revision),
            command,
            input,
        })
        .await;
    Ok(Json(serde_json::json!({
        "kind": "runtime.agent.command",
        "receipt": receipt,
    })))
}

fn runtime_services(
    state: &AppState,
) -> Result<Arc<runtime::RuntimeServices>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "RuntimeServices is unavailable",
            )
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
