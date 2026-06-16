use std::sync::Arc;

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::{AgentRunGraph, AgentTaskNode};
use serde::Deserialize;
use serde_json::Value;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/agents/catalog", get(agent_catalog_handler))
        .route("/api/agents/directory", get(agent_directory_handler))
        .route("/api/agents/discover", get(agent_discover_handler))
        .route("/api/agents/assemble", post(agent_assemble_handler))
        .route("/api/agents/reputation", get(agent_reputation_handler))
        .route("/api/agents/runs", get(agent_runs_handler))
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
    nodes: Vec<AgentTaskNode>,
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

async fn agent_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    commands::handle_agents_slash_command_json(Some("list"), &state.workspace_root)
        .map(Json)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn agent_directory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let catalog = commands::handle_agents_slash_command_json(Some("list"), &state.workspace_root)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let agents = catalog
        .get("agents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(Json(serde_json::json!({
        "kind": "agents.directory",
        "agents": agents,
        "summary": catalog.get("summary").cloned().unwrap_or_else(|| serde_json::json!({})),
        "source": "agents.catalog",
    })))
}

async fn agent_discover_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<AgentDiscoverQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = query.task.trim();
    if task.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "task query is required"));
    }
    commands::handle_agents_slash_command_json(
        Some(&format!("discover {task}")),
        &state.workspace_root,
    )
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
    let discovery = commands::handle_agents_slash_command_json(
        Some(&format!("discover {task}")),
        &state.workspace_root,
    )
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "agents.assemble",
        "task": task,
        "agents": discovery.get("agents").cloned().unwrap_or_else(|| serde_json::json!([])),
        "team": discovery.get("team").cloned().unwrap_or_else(|| serde_json::json!(null)),
        "source": "agents.discover",
    })))
}

async fn agent_reputation_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let catalog = commands::handle_agents_slash_command_json(Some("list"), &state.workspace_root)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let agents = catalog
        .get("agents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let reputation: Vec<Value> = agents
        .iter()
        .map(|agent| {
            serde_json::json!({
                "agent_id": agent.get("id").or_else(|| agent.get("name")).cloned().unwrap_or_else(|| serde_json::json!("unknown")),
                "name": agent.get("name").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
                "reputation": agent.get("reputation").cloned().unwrap_or_else(|| serde_json::json!(null)),
                "status": agent.get("status").or_else(|| agent.get("active")).cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "kind": "agents.reputation",
        "items": reputation,
        "summary": {
            "total": agents.len(),
            "scored": reputation.iter().filter(|item| !item.get("reputation").unwrap_or(&Value::Null).is_null()).count(),
        },
    })))
}

async fn agent_runs_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "kind": "agent_run_graphs",
        "runs": state.task_kernel.list_agent_graphs(),
    }))
}

async fn task_agent_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let graph = state
        .task_kernel
        .agent_graph(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "agent graph not found"))?;
    Ok(Json(graph))
}

async fn upsert_task_agent_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpsertAgentGraphRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let objective = body
        .objective
        .or_else(|| {
            state
                .task_kernel
                .agent_graph(&id)
                .map(|graph| graph.objective)
        })
        .unwrap_or_else(|| "agent run".to_string());
    let mut graph = AgentRunGraph::new(id.clone(), objective);
    for node in body.nodes {
        graph
            .add_node(node)
            .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    }
    graph
        .validate_acyclic()
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    let task = state
        .task_kernel
        .upsert_agent_graph(&id, graph.clone())
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    append_agent_runtime_event(&state, &task.id, &graph)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(graph))
}

async fn append_agent_runtime_event(
    state: &AppState,
    session_id: &str,
    graph: &AgentRunGraph,
) -> Result<(), String> {
    state
        .session_kernel
        .append_runtime_event(
            session_id,
            memory::RuntimeEventScope::Workgraph,
            "agent.run_graph.updated",
            serde_json::json!({ "graph": graph }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}
