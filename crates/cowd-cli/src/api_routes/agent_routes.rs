use std::{fs, path::PathBuf, sync::Arc};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::{AgentRunGraph, AgentTaskNode};
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AgentTeamProfile {
    id: String,
    name: String,
    #[serde(default)]
    objective: String,
    #[serde(default)]
    leader: Option<String>,
    #[serde(default)]
    members: Vec<String>,
    #[serde(default)]
    policy: Value,
    #[serde(default)]
    evaluation: Value,
    #[serde(default)]
    reputation: Value,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Deserialize)]
struct UpsertAgentTeamProfileRequest {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    objective: String,
    #[serde(default)]
    leader: Option<String>,
    #[serde(default)]
    members: Vec<String>,
    #[serde(default)]
    policy: Value,
    #[serde(default)]
    evaluation: Value,
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

async fn agent_team_profiles_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profiles = load_team_profiles(&state)?;
    Ok(Json(serde_json::json!({
        "kind": "agents.team_profiles",
        "count": profiles.len(),
        "profiles": profiles,
        "storage": team_profiles_path(&state.workspace_root),
    })))
}

async fn agent_team_profile_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profiles = load_team_profiles(&state)?;
    let profile = profiles
        .into_iter()
        .find(|profile| profile.id == id)
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
    let mut profiles = load_team_profiles(&state)?;
    let profile = build_team_profile(body, None)?;
    if profiles.iter().any(|existing| existing.id == profile.id) {
        return Err(api_error(StatusCode::CONFLICT, "team profile id already exists"));
    }
    profiles.push(profile.clone());
    save_team_profiles(&state, &profiles)?;
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
    let mut profiles = load_team_profiles(&state)?;
    let index = profiles
        .iter()
        .position(|profile| profile.id == id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "team profile not found"))?;
    let profile = build_team_profile(body, Some(&profiles[index]))?;
    profiles[index] = profile.clone();
    save_team_profiles(&state, &profiles)?;
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
    let mut profiles = load_team_profiles(&state)?;
    let before = profiles.len();
    profiles.retain(|profile| profile.id != id);
    if profiles.len() == before {
        return Err(api_error(StatusCode::NOT_FOUND, "team profile not found"));
    }
    save_team_profiles(&state, &profiles)?;
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

fn team_profiles_path(workspace_root: &std::path::Path) -> PathBuf {
    workspace_root
        .join(".cowd")
        .join("agents")
        .join("team-profiles.json")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn normalize_team_profile_id(value: &str) -> String {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if normalized.is_empty() {
        format!("team-{}", now_ms())
    } else {
        normalized
    }
}

fn load_team_profiles(
    state: &AppState,
) -> Result<Vec<AgentTeamProfile>, (StatusCode, Json<ErrorResponse>)> {
    let path = team_profiles_path(&state.workspace_root);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(&path).map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read team profiles: {error}"),
        )
    })?;
    serde_json::from_str(&text).map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to parse team profiles: {error}"),
        )
    })
}

fn save_team_profiles(
    state: &AppState,
    profiles: &[AgentTeamProfile],
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let path = team_profiles_path(&state.workspace_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create team profile directory: {error}"),
            )
        })?;
    }
    let text = serde_json::to_string_pretty(profiles).map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to serialize team profiles: {error}"),
        )
    })?;
    fs::write(&path, text).map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to write team profiles: {error}"),
        )
    })
}

fn build_team_profile(
    body: UpsertAgentTeamProfileRequest,
    existing: Option<&AgentTeamProfile>,
) -> Result<AgentTeamProfile, (StatusCode, Json<ErrorResponse>)> {
    if body.name.trim().is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "team profile name is required"));
    }
    let created_at_ms = existing.map(|profile| profile.created_at_ms).unwrap_or_else(now_ms);
    let id = existing
        .map(|profile| profile.id.clone())
        .or_else(|| body.id.clone())
        .unwrap_or_else(|| body.name.clone());
    let mut reputation = existing
        .map(|profile| profile.reputation.clone())
        .unwrap_or_else(|| serde_json::json!({}));
    if reputation.is_null() {
        reputation = serde_json::json!({});
    }
    Ok(AgentTeamProfile {
        id: normalize_team_profile_id(&id),
        name: body.name.trim().to_string(),
        objective: body.objective.trim().to_string(),
        leader: body.leader.filter(|leader| !leader.trim().is_empty()),
        members: body
            .members
            .into_iter()
            .map(|member| member.trim().to_string())
            .filter(|member| !member.is_empty())
            .collect(),
        policy: body.policy,
        evaluation: body.evaluation,
        reputation,
        created_at_ms,
        updated_at_ms: now_ms(),
    })
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
