use std::sync::Arc;

use axum::{
    extract::{Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use runtime::{AgentRunGraph, AgentTaskNode};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
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
