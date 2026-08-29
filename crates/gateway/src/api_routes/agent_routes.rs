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

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            surface::gateway_api::paths::API_AGENTS_CATALOG.template(),
            get(agent_catalog_handler),
        )
        .route(
            surface::gateway_api::paths::API_AGENTS_DIRECTORY.template(),
            get(agent_directory_handler),
        )
        .route(
            surface::gateway_api::paths::API_AGENTS_DISCOVER.template(),
            get(agent_discover_handler),
        )
        .route(
            surface::gateway_api::paths::API_AGENTS_ASSEMBLE.template(),
            post(agent_assemble_handler),
        )
        .route(
            surface::gateway_api::paths::API_AGENTS_SELF_MODELS.template(),
            get(agent_self_models_handler),
        )
        .route(
            surface::gateway_api::paths::API_TEAM_TEMPLATES.template(),
            get(team_templates_handler),
        )
        .route(
            surface::gateway_api::paths::API_TEAM_TEMPLATES_INSTANTIATE.template(),
            post(team_template_instantiate_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_TEAMS_BY_ID_WORKING_STATE.template(),
            get(team_working_state_handler),
        )
        .route(
            surface::gateway_api::paths::API_AGENTS_EXECUTION_GRAPHS.template(),
            get(execution_graphs_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS.template(),
            get(runtime_agents_list_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID.template(),
            get(runtime_agent_detail_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_EVENTS.template(),
            get(runtime_agent_events_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_CANCEL.template(),
            post(runtime_agent_cancel_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_INPUT.template(),
            post(runtime_agent_input_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_INTERRUPT.template(),
            post(runtime_agent_interrupt_handler),
        )
        .route(
            surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_SHUTDOWN.template(),
            post(runtime_agent_shutdown_handler),
        )
        .route(
            surface::gateway_api::paths::API_TASKS_BY_ID_EXECUTION_GRAPH.template(),
            get(task_execution_graph_handler),
        )
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
    let runtime = runtime_services(&state)?;
    Ok(Json(state.services.agent.catalog(&runtime)))
}

async fn agent_directory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    Ok(Json(state.services.agent.directory(&runtime)))
}

async fn agent_discover_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<AgentDiscoverQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = query.task.trim();
    if task.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "task query is required"));
    }
    let runtime = runtime_services(&state)?;
    Ok(Json(state.services.agent.discover(&runtime, task)))
}

async fn agent_assemble_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<AgentAssembleRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = body.task.trim();
    if task.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "task is required"));
    }
    let runtime = runtime_services(&state)?;
    Ok(Json(state.services.agent.assemble(&runtime, task)))
}

async fn agent_self_models_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    Ok(Json(state.services.agent.self_models(&runtime)))
}

/// Read-only Team Template projection. Template creation and release decisions
/// remain Runtime commands; Gateway never rebuilds a team definition from
/// workspace files or a browser payload.
async fn team_templates_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    let templates = runtime
        .definition_registry()
        .runnable_team_catalog()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "team_templates",
        "templates": templates,
        "source": "runtime.definition_catalog",
    })))
}

/// Gateway accepts declarative template intent only. Runtime resolves the
/// immutable template/Agent revisions and owns graph construction.
async fn team_template_instantiate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<harness_contract::team::TeamInstantiationRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    let projection = runtime
        .team_runtime()
        .instantiate(request)
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "kind": "runtime.team.instantiated",
            "team": projection,
        })),
    ))
}

async fn team_working_state_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = runtime_services(&state)?;
    let state = runtime
        .team_runtime()
        .working_state(&id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(serde_json::json!({
        "kind": "runtime.team.working_state",
        "working_state": state,
    })))
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
