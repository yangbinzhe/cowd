use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use memory::store::session::SessionRecord;
use runtime::{
    AgentNodeStatus, AgentRole, AgentRunGraph, AgentTaskNode, IaccActionExecutionRequest,
    IaccActionFeedback, IaccFact, IaccFactInput, IaccIncident, IaccStore, IaccStoreError,
    IACC_SCHEMA_VERSION,
};
use serde::Deserialize;

use crate::task_kernel::TaskRecord;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/iacc/health", get(iacc_health_handler))
        .route("/api/iacc/facts/ingest", post(iacc_fact_ingest_handler))
        .route("/api/iacc/metrics", get(iacc_metrics_handler))
        .route("/api/iacc/metrics/:id", get(iacc_metric_detail_handler))
        .route(
            "/api/iacc/metrics/recompute",
            post(iacc_metric_recompute_handler),
        )
        .route("/api/iacc/changes", get(iacc_changes_handler))
        .route("/api/iacc/attention/hot", get(iacc_attention_hot_handler))
        .route(
            "/api/iacc/evidence/build",
            post(iacc_evidence_build_handler),
        )
        .route("/api/iacc/evidence/:id", get(iacc_evidence_get_handler))
        .route(
            "/api/iacc/evidence/:id/context",
            get(iacc_evidence_context_handler),
        )
        .route("/api/iacc/incidents", post(iacc_incident_create_handler))
        .route("/api/iacc/incidents/:id", get(iacc_incident_get_handler))
        .route(
            "/api/iacc/incidents/:id/analyze",
            post(iacc_incident_analyze_handler),
        )
        .route("/api/iacc/analyses/:id", get(iacc_analysis_get_handler))
        .route(
            "/api/iacc/analyses/:analysis_id/actions/:action_id/execute",
            post(iacc_action_execute_handler),
        )
        .route("/api/iacc/executions/:id", get(iacc_execution_get_handler))
        .route(
            "/api/iacc/executions/:id/feedback",
            post(iacc_execution_feedback_handler),
        )
}

#[derive(Debug, Deserialize)]
struct IaccFactIngestRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<IaccFactInput>,
}

#[derive(Debug, Deserialize)]
struct IaccEvidenceBuildRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    attention_id: Option<String>,
    #[serde(default)]
    problem_statement: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IaccIncidentCreateRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    attention_id: Option<String>,
    #[serde(default)]
    evidence_packet_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IaccExecutionFeedbackRequest {
    outcome: String,
    note: String,
    #[serde(default)]
    metric_delta: Option<f64>,
}

async fn iacc_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let health = store
        .health()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.health",
        "status": "ready",
        "schema_version": health.schema_version,
        "expected_schema_version": IACC_SCHEMA_VERSION,
        "fact_count": health.fact_count,
        "metric_definition_count": health.metric_definition_count,
        "metric_state_count": health.metric_state_count,
        "change_count": health.change_count,
        "attention_count": health.attention_count,
        "evidence_count": health.evidence_count,
        "incident_count": health.incident_count,
        "analysis_count": health.analysis_count,
        "execution_count": health.execution_count,
        "store": iacc_store_path(&state.workspace_root),
        "capabilities": [
            "fact_ingest",
            "metric_recompute",
            "metric_state",
            "change_event",
            "attention_hot",
            "evidence_packet_build",
            "evidence_packet_get",
            "evidence_context_item",
            "incident_agent_graph",
            "incident_operational_analysis",
            "action_execution_feedback"
        ],
    })))
}

async fn iacc_fact_ingest_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccFactIngestRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if request.facts.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "at least one IACC fact is required",
        ));
    }
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut facts = Vec::with_capacity(request.facts.len());
    let mut attention = Vec::with_capacity(request.facts.len());
    for input in request.facts {
        let fact = IaccFact::from_input(input);
        let item = store
            .ingest_fact(&fact)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        facts.push(fact);
        attention.push(item);
    }
    Ok(Json(serde_json::json!({
        "kind": "iacc.fact.ingest",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "ingested": facts.len(),
        "facts": facts,
        "attention": attention,
    })))
}

async fn iacc_metrics_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let metrics = store
        .list_metric_definitions()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.metrics",
        "metrics": metrics,
    })))
}

async fn iacc_metric_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let states = store
        .metric_states(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if states.is_empty() {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            "IACC metric state not found",
        ));
    }
    Ok(Json(serde_json::json!({
        "kind": "iacc.metric",
        "metric_id": id,
        "states": states,
    })))
}

async fn iacc_metric_recompute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let result = store
        .recompute_metrics()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.metrics.recompute",
        "result": result,
    })))
}

async fn iacc_changes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let changes = store
        .list_changes(100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.changes",
        "changes": changes,
    })))
}

async fn iacc_attention_hot_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let items = store
        .list_attention(50)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.attention.hot",
        "items": items,
    })))
}

async fn iacc_evidence_build_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccEvidenceBuildRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = store
        .build_evidence_packet(
            request.attention_id.as_deref(),
            request.problem_statement.as_deref(),
        )
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.evidence.packet",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "packet": packet,
    })))
}

async fn iacc_evidence_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = store
        .get_evidence_packet(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.evidence.packet",
        "packet": packet,
    })))
}

async fn iacc_evidence_context_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = store
        .get_evidence_packet(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.evidence.context_item",
        "context_item": packet.to_context_item(),
    })))
}

async fn iacc_incident_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccIncidentCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = match request.evidence_packet_id.as_deref() {
        Some(packet_id) => store
            .get_evidence_packet(packet_id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC evidence packet not found"))?,
        None => store
            .build_evidence_packet(request.attention_id.as_deref(), request.title.as_deref())
            .map_err(|error| match error {
                IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?,
    };
    let title = request
        .title
        .clone()
        .unwrap_or_else(|| packet.problem_statement.clone());
    let task = state
        .task_kernel
        .start_goal(format!("IACC incident analysis: {title}"), false)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let mut graph = task
        .agent_graph
        .clone()
        .unwrap_or_else(|| AgentRunGraph::from_objective(task.id.clone(), task.objective.clone()));
    enrich_iacc_agent_graph(&mut graph, &packet)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    let task = state
        .task_kernel
        .upsert_agent_graph(&task.id, graph.clone())
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    append_iacc_agent_runtime_event(&state, &task, &graph)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;

    let mut incident = IaccIncident::new(title);
    incident.attention_id = packet.attention_id.clone();
    incident.evidence_packet_id = Some(packet.packet_id.clone());
    incident.task_id = Some(task.id.clone());
    incident.agent_graph_id = Some(graph.graph_id.clone());
    let incident = store
        .create_incident(&incident)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.incident",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident": incident,
        "task": task,
        "agent_graph": graph,
        "context_item": packet.to_context_item(),
    })))
}

async fn iacc_incident_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC incident not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.incident",
        "incident": incident,
    })))
}

async fn iacc_incident_analyze_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let analysis = store.analyze_incident(&id).map_err(|error| match error {
        IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.operational_analysis",
        "analysis": analysis,
    })))
}

async fn iacc_analysis_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let analysis = store
        .get_analysis(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC operational analysis not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.operational_analysis",
        "analysis": analysis,
    })))
}

async fn iacc_action_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((analysis_id, action_id)): AxumPath<(String, String)>,
    Json(request): Json<IaccActionExecutionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .execute_recommended_action(&analysis_id, &action_id, &request)
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.action_execution",
        "execution": execution,
    })))
}

async fn iacc_execution_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .get_execution(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC action execution not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.action_execution",
        "execution": execution,
    })))
}

async fn iacc_execution_feedback_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccExecutionFeedbackRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .record_execution_feedback(
            &id,
            IaccActionFeedback::new(request.outcome, request.note, request.metric_delta),
        )
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.action_execution",
        "execution": execution,
    })))
}

fn enrich_iacc_agent_graph(
    graph: &mut AgentRunGraph,
    packet: &runtime::IaccEvidencePacket,
) -> Result<(), runtime::AgentGraphError> {
    let now = now_ms();
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "iacc_researcher".to_string(),
            role: AgentRole::Researcher,
            title: "IACC Evidence Research".to_string(),
            objective: "Validate IACC evidence packet and identify missing evidence".to_string(),
            depends_on: vec!["planner".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("iacc_researcher".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "iacc_reviewer".to_string(),
            role: AgentRole::Reviewer,
            title: "IACC Insight Review".to_string(),
            objective: "Review confidence, conflicts, and governance readiness".to_string(),
            depends_on: vec!["iacc_researcher".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("iacc_reviewer".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "iacc_merger".to_string(),
            role: AgentRole::Merger,
            title: "IACC Decision Merge".to_string(),
            objective: "Merge agent findings into one governed operating decision".to_string(),
            depends_on: vec!["iacc_reviewer".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("iacc_merger".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    let reference = format!("iacc:evidence:{}", packet.packet_id);
    graph.add_evidence(
        "planner",
        "iacc_evidence_packet",
        reference.clone(),
        packet.problem_statement.clone(),
    )?;
    graph.add_evidence(
        "iacc_researcher",
        "iacc_evidence_packet",
        reference,
        format!(
            "metric_evidence={}, change_evidence={}, missing_evidence={}",
            packet.metric_evidence.len(),
            packet.change_evidence.len(),
            packet.missing_evidence.len()
        ),
    )?;
    Ok(())
}

fn ensure_agent_node(
    graph: &mut AgentRunGraph,
    node: AgentTaskNode,
) -> Result<(), runtime::AgentGraphError> {
    if graph.nodes.iter().any(|existing| existing.id == node.id) {
        return Ok(());
    }
    graph.add_node(node)
}

async fn append_iacc_agent_runtime_event(
    state: &AppState,
    task: &TaskRecord,
    graph: &AgentRunGraph,
) -> Result<(), String> {
    ensure_iacc_task_session_record(state, task)
        .await
        .map_err(|error| format!("failed to prepare IACC task runtime session: {error}"))?;
    state
        .session_kernel
        .append_runtime_event(
            &task.id,
            memory::RuntimeEventScope::Workgraph,
            "iacc.agent_graph.updated",
            serde_json::json!({ "graph": graph }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn ensure_iacc_task_session_record(
    state: &AppState,
    task: &TaskRecord,
) -> Result<(), String> {
    let Some(store) = state.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    let metadata_json = serde_json::json!({
        "kind": "iacc.incident.task",
        "task_id": task.id,
        "objective": task.objective,
        "yolo_mode": task.yolo_mode,
        "current_phase": task.current_phase,
    })
    .to_string();
    let mut record = SessionRecord {
        session_id: task.id.clone(),
        platform: "iacc".to_string(),
        chat_id: task.id.clone(),
        user_id: None,
        model: None,
        created_at: now.clone(),
        last_activity: now,
        message_count: task.audit.len() as i64,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata_json),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: task.status.as_str().to_string(),
    };
    if let Some(existing) = store
        .get_session(&task.id)
        .await
        .map_err(|error| error.to_string())?
    {
        record.created_at = existing.created_at;
        store
            .update_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    } else {
        store
            .create_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn open_iacc_store(state: &AppState) -> Result<IaccStore, IaccStoreError> {
    let path = iacc_store_path(&state.workspace_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            IaccStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?;
    }
    IaccStore::open(path)
}

pub(super) fn iacc_store_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(".cowd").join("iacc.sqlite")
}
