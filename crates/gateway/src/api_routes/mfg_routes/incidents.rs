use axum::extract::Extension;
use sha2::{Digest, Sha256};

use crate::api_routes::{principal_actor_id, AuthenticatedPrincipal};

use super::*;

struct MfgSkillGraphResolver {
    graph_id: String,
    backend: Arc<dyn runtime::execution_core::ScopedNodeBackend>,
}

impl runtime::execution_core::graph::executors::ScopedNodeBackendResolver
    for MfgSkillGraphResolver
{
    fn resolve(
        &self,
        ticket: &runtime::execution_core::NodeExecutionTicket,
    ) -> Option<Arc<dyn runtime::execution_core::ScopedNodeBackend>> {
        (ticket.graph_id == self.graph_id).then(|| Arc::clone(&self.backend))
    }
}

#[derive(Debug, Deserialize)]
struct MfgSkillRuntimeResult {
    execution_id: String,
    status: String,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: chrono::DateTime<chrono::Utc>,
    tool_results: Vec<app_mfg::MfgSkillToolResult>,
}

fn mfg_skill_runtime_ids(idempotency_key: &str) -> (String, String) {
    let digest = format!("{:x}", Sha256::digest(idempotency_key.as_bytes()));
    (
        format!("mfg-skill-graph-{digest}"),
        format!("mfg-skill-tools-{digest}"),
    )
}

fn canonical_mfg_skill_runtime_graph(
    idempotency_key: &str,
    payload: &crate::services::MfgSkillExecutionPayload,
) -> Result<harness_contract::execution_graph::ExecutionGraph, String> {
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec};

    let (graph_id, node_id) = mfg_skill_runtime_ids(idempotency_key);
    let mut graph = ExecutionGraph::new(format!("MFG skill execution {}", payload.execution_id));
    graph.id = graph_id;
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::ToolBatch,
        "cross_plane_connector",
        serde_json::to_string(payload).map_err(|error| error.to_string())?,
    );
    node.id = node_id;
    node.idempotency_key = format!("{idempotency_key}:tools");
    graph.nodes.push(node);
    Ok(graph)
}

fn load_mfg_skill_runtime_owner(
    state: &AppState,
    idempotency_key: &str,
    execution_id: &str,
    incident_id: &str,
    skill_id: &str,
) -> Result<Option<crate::services::MfgSkillExecutionPayload>, String> {
    let runtime_services = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| "Runtime services are unavailable for MFG skill execution".to_string())?;
    let (graph_id, node_id) = mfg_skill_runtime_ids(idempotency_key);
    let graph = match runtime_services.graph_state_store().load(&graph_id) {
        Ok(graph) => graph,
        Err(runtime::execution_core::ExecutionStateStoreError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let node = graph
        .nodes
        .iter()
        .find(|node| {
            node.id == node_id
                && node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                && node.executor_kind == "cross_plane_connector"
        })
        .ok_or_else(|| format!("MFG skill graph {graph_id} has no canonical Runtime tool node"))?;
    let payload =
        serde_json::from_str::<crate::services::MfgSkillExecutionPayload>(&node.payload_ref)
            .map_err(|error| {
                format!("MFG skill graph {graph_id} has invalid owner payload: {error}")
            })?;
    let payload_incident_id = payload
        .planned_run
        .execution_context
        .as_ref()
        .map(|context| context.incident_id.as_str());
    let canonical = canonical_mfg_skill_runtime_graph(idempotency_key, &payload)?;
    if graph.objective != canonical.objective
        || graph.parent_execution != canonical.parent_execution
        || graph.nodes != canonical.nodes
        || graph.edges != canonical.edges
        || payload.execution_id != execution_id
        || payload.skill_id != skill_id
        || payload_incident_id != Some(incident_id)
    {
        return Err(format!(
            "MFG skill graph {graph_id} is bound to another canonical execution"
        ));
    }
    Ok(Some(payload))
}

async fn execute_mfg_skill_runtime_graph(
    state: &AppState,
    idempotency_key: &str,
    payload: crate::services::MfgSkillExecutionPayload,
) -> Result<
    (
        harness_contract::execution_graph::ExecutionGraphProjection,
        crate::services::MfgSkillExecutionPayload,
    ),
    String,
> {
    use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeKind};
    use runtime::execution_core::ExecutionGraphHost;

    let runtime_services = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| "Runtime services are unavailable for MFG skill execution".to_string())?;
    let graph = canonical_mfg_skill_runtime_graph(idempotency_key, &payload)?;
    let graph_id = graph.id.clone();
    let backend = Arc::new(crate::services::GatewayMfgSkillExecutor::new(
        state.services.matrix.clone(),
        state.services.cross_plane.runtime_control(),
        state.config_home.clone(),
    ));
    match runtime_services.graph_state_store().load(&graph_id) {
        Ok(existing) => {
            let existing_node = existing
                .nodes
                .iter()
                .find(|node| {
                    node.id == graph.nodes[0].id
                        && node.kind == ExecutionNodeKind::ToolBatch
                        && node.executor_kind == "cross_plane_connector"
                })
                .ok_or_else(|| {
                    format!("MFG skill graph {graph_id} has no canonical Runtime tool node")
                })?;
            let persisted_payload =
                serde_json::from_str::<crate::services::MfgSkillExecutionPayload>(
                    &existing_node.payload_ref,
                )
                .map_err(|error| {
                    format!("MFG skill graph {graph_id} has invalid owner payload: {error}")
                })?;
            let canonical = canonical_mfg_skill_runtime_graph(idempotency_key, &persisted_payload)?;
            if existing.objective != canonical.objective
                || existing.parent_execution != canonical.parent_execution
                || existing.nodes != canonical.nodes
                || existing.edges != canonical.edges
                || persisted_payload.execution_id != payload.execution_id
                || persisted_payload.skill_id != payload.skill_id
                || persisted_payload
                    .planned_run
                    .execution_context
                    .as_ref()
                    .map(|context| context.incident_id.as_str())
                    != payload
                        .planned_run
                        .execution_context
                        .as_ref()
                        .map(|context| context.incident_id.as_str())
            {
                return Err(format!(
                    "MFG skill graph {graph_id} is bound to another canonical execution"
                ));
            }
            let projection = runtime_services
                .graph_runner()
                .graph_projection(&graph_id)
                .await
                .map_err(|error| error.to_string())?;
            if projection
                .nodes
                .iter()
                .all(|node| node.status.is_terminal())
            {
                return Ok((projection, persisted_payload));
            }
            runtime_services
                .cross_plane_connector_executor()
                .install_resolver(Arc::new(MfgSkillGraphResolver {
                    graph_id: graph_id.clone(),
                    backend,
                }));
            runtime_services
                .graph_runner()
                .command_graph(
                    &graph_id,
                    ExecutionGraphCommand::Advance {
                        expected_revision: projection.revision,
                    },
                )
                .await
                .map(|receipt| (receipt.graph, persisted_payload))
                .map_err(|error| error.to_string())
        }
        Err(runtime::execution_core::ExecutionStateStoreError::NotFound(_)) => {
            runtime_services
                .cross_plane_connector_executor()
                .install_resolver(Arc::new(MfgSkillGraphResolver { graph_id, backend }));
            runtime_services
                .graph_runner()
                .submit_graph(
                    graph,
                    ExecutionGraphCommand::Start {
                        expected_revision: 0,
                    },
                )
                .await
                .map(|receipt| (receipt.graph, payload))
                .map_err(|error| error.to_string())
        }
        Err(error) => Err(error.to_string()),
    }
}

pub(super) async fn mfg_incident_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<MfgIncidentCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = mfg_idempotency_key(&headers, None)
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    let incident_id = stable_mfg_resource_id("incident", &idempotency_key);
    let task_id = stable_mfg_resource_id("task", &idempotency_key);
    let workflow_id = stable_mfg_resource_id("mfg-workflow", &idempotency_key);
    let packet = match request.evidence_packet_id.as_deref() {
        Some(packet_id) => {
            let mfg_packet = state
                .services
                .mfg
                .get_evidence_packet(&state.config_home, packet_id)
                .map_err(|error| {
                    mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                })?;
            match mfg_packet {
                Some(packet) => packet,
                None => state
                    .services
                    .matrix
                    .get_evidence_packet(&state.config_home, packet_id)
                    .map_err(|error| {
                        mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    })?
                    .map(|packet| {
                        state
                            .services
                            .mfg
                            .upsert_evidence_packet(&state.config_home, &packet)
                    })
                    .transpose()
                    .map_err(|error| {
                        mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    })?
                    .ok_or_else(|| {
                        mfg_api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found")
                    })?,
            }
        }
        None => state
            .services
            .mfg
            .build_evidence_packet_idempotent(
                &state.config_home,
                &stable_mfg_resource_id("evidence", &idempotency_key),
                request.attention_id.as_deref(),
                request.title.as_deref(),
            )
            .map_err(|error| match error {
                MfgRepositoryError::NotFound(message) => {
                    mfg_api_error(StatusCode::NOT_FOUND, message)
                }
                other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?,
    };
    let title = request
        .title
        .clone()
        .unwrap_or_else(|| packet.problem_statement.clone());
    let task = state
        .services
        .task
        .start_goal_idempotent(&task_id, format!("MFG incident analysis: {title}"), false)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let mut incident = MfgIncident::new(title);
    incident.incident_id = incident_id;
    incident.attention_id = packet.attention_id.clone();
    incident.evidence_packet_id = Some(packet.packet_id.clone());
    incident.task_id = Some(task.id.clone());
    let (incident, workflow_graph) = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| {
            store.create_incident_workflow_idempotent(&incident, &packet, &workflow_id)
        })
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident": incident,
        "task": task,
        "workflow_graph": workflow_graph,
        "context_item": state.services.context.structured_evidence_item(&packet),
    })))
}

pub(super) async fn mfg_incident_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incident = state
        .services
        .mfg
        .get_incident(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident",
        "incident": incident,
    })))
}

pub(super) async fn mfg_incident_room_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incident = state
        .services
        .mfg
        .get_incident(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let evidence_packet = incident
        .evidence_packet_id
        .as_deref()
        .and_then(|packet_id| {
            state
                .services
                .mfg
                .get_evidence_packet(&state.config_home, packet_id)
                .ok()
                .flatten()
        });
    let quality_gate = evidence_packet.as_ref().and_then(|packet| {
        state
            .services
            .mfg
            .evaluate_evidence_quality(&state.config_home, &packet.packet_id)
            .ok()
    });
    let analysis = state
        .services
        .mfg
        .latest_analysis_for_incident(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let executions = state
        .services
        .mfg
        .list_executions_for_incident(&state.config_home, &id, 20)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let memory_cases = state
        .services
        .mfg
        .search_memory_cases(&state.config_home, Some(&id), 10)
        .unwrap_or_default();
    let playbooks = state
        .services
        .mfg
        .recommend_playbooks_for_incident(&state.config_home, &id, 5)
        .unwrap_or_default();
    let workflow_graph = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| store.workflow_graph_for_incident(&id))
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let assignments = state
        .services
        .mfg
        .list_assignments(&state.config_home, None, Some(&id), 100)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident_room",
        "incident": incident,
        "evidence_packet": evidence_packet,
        "quality_gate": quality_gate,
        "analysis": analysis,
        "executions": executions,
        "memory_cases": memory_cases,
        "playbooks": playbooks,
        "workflow_graph": workflow_graph,
        "canonical_task_ref": incident.task_id.as_ref().map(|task_id| format!("task:{task_id}")),
        "assignments": assignments,
    })))
}

pub(super) async fn mfg_incident_analyze_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = mfg_idempotency_key(&headers, None)
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    let analysis_id = stable_mfg_resource_id("analysis", &idempotency_key);
    let analysis = state
        .services
        .mfg
        .analyze_incident_idempotent(&state.config_home, &id, &analysis_id)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => mfg_api_error(StatusCode::NOT_FOUND, message),
            other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.operational_analysis",
        "analysis": analysis,
    })))
}

pub(super) async fn mfg_incident_case_promote_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let promotion = state
        .services
        .mfg
        .promote_incident_to_memory_case(&state.config_home, &id)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => mfg_api_error(StatusCode::NOT_FOUND, message),
            other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.memory_case.promotion",
        "memory_case": promotion.memory_case,
        "playbook": promotion.playbook,
    })))
}

pub(super) async fn mfg_memory_case_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let memory_case = state
        .services
        .mfg
        .get_memory_case(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG memory case not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.memory_case",
        "memory_case": memory_case,
    })))
}

pub(super) async fn mfg_memory_case_search_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<MfgCaseSearchQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cases = state
        .services
        .mfg
        .search_memory_cases(
            &state.config_home,
            query.q.as_deref(),
            query.limit.unwrap_or(20).min(100),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.memory_case.search",
        "items": cases,
    })))
}

pub(super) async fn mfg_playbook_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgPlaybookUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let playbook = state
        .services
        .mfg
        .upsert_playbook(
            &state.config_home,
            &request.playbook,
            request.expected_revision,
        )
        .map_err(mfg_mutation_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.playbook",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "revision": playbook.revision,
        "playbook": playbook,
    })))
}

pub(super) async fn mfg_playbook_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let playbook = state
        .services
        .mfg
        .get_playbook(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG playbook not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.playbook",
        "playbook": playbook,
    })))
}

pub(super) async fn mfg_incident_playbook_recommend_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgPlaybookRecommendRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let playbooks = state
        .services
        .mfg
        .recommend_playbooks_for_incident(
            &state.config_home,
            &id,
            request.limit.unwrap_or(5).min(20),
        )
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => mfg_api_error(StatusCode::NOT_FOUND, message),
            other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.playbook.recommendation",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident_id": id,
        "playbooks": playbooks,
    })))
}

pub(super) async fn mfg_incident_skill_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgSkillPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let context = state
        .services
        .mfg
        .incident_context(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let plan = state.services.mfg.plan_server_skills(
        &context.incident,
        context.analysis.as_ref(),
        context.packet.as_ref(),
        request.limit.unwrap_or(3).clamp(1, 8),
    );
    let mut workflow_graph = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| {
            store
                .workflow_graph_for_incident(&id)?
                .ok_or_else(|| MfgRepositoryError::NotFound(format!("workflow for {id}")))
        })
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    workflow_graph
        .plan_skills(&plan)
        .map_err(|error| mfg_api_error(StatusCode::CONFLICT, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident_id": id,
        "plan": plan,
        "workflow_graph": workflow_graph,
    })))
}

pub(super) async fn mfg_incident_skill_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((id, skill_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<MfgSkillRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = mfg_idempotency_key(&headers, None)
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    let execution_id = stable_mfg_resource_id("skill-execution", &idempotency_key);
    let session_id = request.session_id.clone();
    let context = state
        .services
        .mfg
        .incident_context(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    if let Some(run) = state
        .services
        .mfg
        .get_skill_run(&state.config_home, &execution_id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        if run.status != "completed"
            || run
                .runtime_execution_ref
                .as_deref()
                .is_none_or(str::is_empty)
            || run.tool_results.len() != run.tool_plan.len()
        {
            return Err(mfg_api_error(
                StatusCode::CONFLICT,
                "existing MFG skill execution is not backed by a terminal Runtime graph",
            ));
        }
        let workflow_graph = state
            .services
            .mfg
            .open_store(&state.config_home)
            .and_then(|store| store.workflow_graph_for_incident(&id))
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .ok_or_else(|| {
                mfg_api_error(StatusCode::NOT_FOUND, "MFG incident workflow not found")
            })?;
        append_mfg_execution_outcome(
            &state,
            session_id
                .as_deref()
                .or(context.incident.task_id.as_deref()),
            mfg_skill_run_execution_outcome(&run),
        )
        .await
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
        return Ok(Json(serde_json::json!({
            "kind": "mfg.skill.run",
            "request_id": request.request_id,
            "session_id": session_id,
            "incident_id": id,
            "skill_run": run,
            "workflow_graph": workflow_graph,
        })));
    }
    let existing_owner =
        load_mfg_skill_runtime_owner(&state, &idempotency_key, &execution_id, &id, &skill_id)
            .map_err(|error| mfg_api_error(StatusCode::CONFLICT, error))?;
    let payload = if let Some(payload) = existing_owner {
        if request.expected_revision != Some(payload.expected_incident_revision) {
            return Err(mfg_api_error(
                StatusCode::CONFLICT,
                format!(
                    "MFG skill retry must preserve original incident revision {}",
                    payload.expected_incident_revision
                ),
            ));
        }
        payload
    } else {
        if request.expected_revision != Some(context.incident.revision) {
            return Err(mfg_api_error(
                StatusCode::CONFLICT,
                format!(
                    "incident revision conflict: expected {:?}, actual {}",
                    request.expected_revision, context.incident.revision
                ),
            ));
        }
        let skill = state
            .services
            .mfg
            .skill_manifest(&skill_id)
            .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG skill not found"))?;
        let mut planned_run = state.services.mfg.run_server_skill(
            &context.incident,
            &skill,
            context.analysis.as_ref(),
            context.packet.as_ref(),
        );
        planned_run.execution_id = Some(execution_id.clone());
        if planned_run.execution_context.is_none() {
            return Err(mfg_api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MFG skill run omitted its canonical execution context",
            ));
        }
        crate::services::MfgSkillExecutionPayload {
            execution_id: execution_id.clone(),
            skill_id: skill_id.clone(),
            expected_incident_revision: context.incident.revision,
            planned_run,
            evidence_confidence: context
                .packet
                .as_ref()
                .map(|packet| packet.confidence)
                .unwrap_or(0.5),
        }
    };
    let (projection, persisted_payload) =
        execute_mfg_skill_runtime_graph(&state, &idempotency_key, payload)
            .await
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let evidence_confidence = persisted_payload.evidence_confidence;
    let mut run = persisted_payload.planned_run;
    let (_, tool_node_id) = mfg_skill_runtime_ids(&idempotency_key);
    let tool_node = projection
        .nodes
        .iter()
        .find(|node| {
            node.node_id == tool_node_id
                && node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                && node.executor_kind == "cross_plane_connector"
        })
        .ok_or_else(|| {
            mfg_api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MFG skill Runtime graph omitted its tool node",
            )
        })?;
    if tool_node.status != harness_contract::execution_graph::ExecutionNodeStatus::Completed {
        return Err(mfg_api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "MFG skill Runtime tool node ended in {:?}",
                tool_node.status
            ),
        ));
    }
    let runtime_result = tool_node
        .result_ref
        .as_deref()
        .ok_or_else(|| {
            mfg_api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "MFG skill Runtime tool node omitted its result receipt",
            )
        })
        .and_then(|result| {
            serde_json::from_str::<MfgSkillRuntimeResult>(result).map_err(|error| {
                mfg_api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("invalid MFG skill Runtime result: {error}"),
                )
            })
        })?;
    if runtime_result.execution_id != execution_id
        || runtime_result.status != "completed"
        || runtime_result.tool_results.len() != run.tool_plan.len()
        || runtime_result
            .tool_results
            .iter()
            .any(|result| result.status != "completed")
    {
        return Err(mfg_api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MFG skill Runtime graph did not complete every declared tool",
        ));
    }
    run.status = "completed".to_string();
    run.summary = format!(
        "MFG skill {} completed {} governed Runtime tool calls",
        run.skill_id,
        runtime_result.tool_results.len(),
    );
    run.tool_results = runtime_result.tool_results;
    run.runtime_execution_ref = Some(format!("runtime-execution://{}", projection.graph_id));
    run.runtime_commit_cursor = Some(projection.commit_cursor);
    run.telemetry = Some(app_mfg::MfgSkillTelemetry {
        started_at: runtime_result.started_at,
        completed_at: runtime_result.completed_at,
        elapsed_ms: runtime_result
            .completed_at
            .signed_duration_since(runtime_result.started_at)
            .num_milliseconds()
            .max(0) as u64,
        tool_call_count: run.tool_results.len(),
        evidence_ref_count: run
            .execution_context
            .as_ref()
            .map(|context| context.evidence_refs.len())
            .unwrap_or_default(),
        confidence: evidence_confidence,
    });
    if let Some(report) = run.structured_report.as_object_mut() {
        report.insert(
            "status".to_string(),
            serde_json::Value::String("completed".to_string()),
        );
        report.insert(
            "runtime_execution_ref".to_string(),
            serde_json::Value::String(format!("runtime-execution://{}", projection.graph_id)),
        );
        report.insert(
            "runtime_commit_cursor".to_string(),
            serde_json::json!(projection.commit_cursor),
        );
        report.insert(
            "tool_results".to_string(),
            serde_json::to_value(&run.tool_results).unwrap_or_default(),
        );
    }
    let (run, workflow_graph) = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| store.record_skill_run_and_complete_workflow(&run))
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    append_mfg_execution_outcome(
        &state,
        session_id
            .as_deref()
            .or(context.incident.task_id.as_deref()),
        mfg_skill_run_execution_outcome(&run),
    )
    .await
    .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill.run",
        "request_id": request.request_id,
        "session_id": session_id,
        "incident_id": id,
        "skill_run": run,
        "workflow_graph": workflow_graph,
    })))
}

pub(super) async fn mfg_incident_skill_runs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incident = state
        .services
        .mfg
        .get_incident(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let runs = state
        .services
        .mfg
        .list_skill_runs_for_incident(&state.config_home, &incident.incident_id, 24)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill.run_list",
        "incident_id": incident.incident_id,
        "items": runs,
    })))
}

pub(super) async fn mfg_skill_run_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let run = state
        .services
        .mfg
        .get_skill_run(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG skill run not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill.run",
        "skill_run": run,
    })))
}

pub(super) async fn mfg_analysis_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let analysis = state
        .services
        .mfg
        .get_analysis(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            mfg_api_error(StatusCode::NOT_FOUND, "MFG operational analysis not found")
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.operational_analysis",
        "analysis": analysis,
    })))
}

pub(super) async fn mfg_action_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((analysis_id, action_id)): AxumPath<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(mut intent): Json<MfgActionExecutionIntent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mode = normalize_mfg_action_mode(&intent.mode)
        .map_err(|error| mfg_api_error(StatusCode::UNPROCESSABLE_ENTITY, error))?;
    let dry_run = mode == "dry_run";
    intent.mode = mode.to_string();
    let action_capability = if dry_run {
        "mfg.read"
    } else {
        "mfg.execution.operate"
    };
    require_mfg_capability(&principal, action_capability)?;
    if !dry_run {
        let analysis = state
            .services
            .mfg
            .get_analysis(&state.config_home, &analysis_id)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .ok_or_else(|| {
                mfg_api_error(StatusCode::NOT_FOUND, "MFG operational analysis not found")
            })?;
        if intent.expected_revision != Some(analysis.revision) {
            return Err(mfg_api_error(
                StatusCode::CONFLICT,
                format!(
                    "analysis revision conflict: expected {:?}, actual {}",
                    intent.expected_revision, analysis.revision
                ),
            ));
        }
    }
    let request = intent.into_request(principal_actor_id(&principal));
    let execution_result = if dry_run {
        state.services.mfg.preview_recommended_action(
            &state.config_home,
            &analysis_id,
            &action_id,
            &request,
        )
    } else {
        let idempotency_key = mfg_idempotency_key(&headers, None)
            .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
        let execution_id = stable_mfg_resource_id("execution", &idempotency_key);
        state.services.mfg.execute_recommended_action_idempotent(
            &state.config_home,
            &analysis_id,
            &action_id,
            &execution_id,
            &request,
        )
    };
    let execution = execution_result.map_err(|error| match error {
        MfgRepositoryError::NotFound(message) => mfg_api_error(StatusCode::NOT_FOUND, message),
        other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    if !dry_run {
        let incident = state
            .services
            .mfg
            .get_incident(&state.config_home, &execution.incident_id)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        append_mfg_execution_outcome(
            &state,
            incident
                .as_ref()
                .and_then(|incident| incident.task_id.as_deref())
                .or(Some(execution.incident_id.as_str())),
            mfg_action_execution_outcome(&execution),
        )
        .await
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.action_execution",
        "execution": execution,
    })))
}

pub(super) async fn mfg_execution_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let execution = state
        .services
        .mfg
        .get_execution(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG action execution not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.action_execution",
        "execution": execution,
    })))
}

pub(super) async fn mfg_execution_cross_plane_bridge_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(intent): Json<MfgCrossPlaneBridgeIntent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mode = normalize_mfg_action_mode(&intent.mode)
        .map_err(|error| mfg_api_error(StatusCode::UNPROCESSABLE_ENTITY, error))?;
    let action_capability = if mode == "dry_run" {
        "mfg.read"
    } else {
        "mfg.execution.operate"
    };
    require_mfg_capability(&principal, action_capability)?;
    let body_idempotency_key = intent.idempotency_key.clone();
    let mut request = intent.into_request(principal_actor_id(&principal));
    request.mode = mode.to_string();
    let execution = state
        .services
        .mfg
        .get_execution(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG action execution not found"))?;
    let idempotency_key = if mode == "dry_run"
        && body_idempotency_key.is_none()
        && headers.get("idempotency-key").is_none()
    {
        None
    } else {
        Some(
            mfg_idempotency_key(&headers, body_idempotency_key)
                .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?,
        )
    };
    request.idempotency_key = idempotency_key.clone();
    let requested_action = state
        .services
        .mfg
        .cross_plane_action_from_execution(&execution, &request);
    let now = chrono::Utc::now();
    let snapshot = crate::api_routes::connector_routes::connector_snapshot(&state);
    let (action, decision, evidence) =
        state
            .services
            .cross_plane
            .decide_connector_action(&snapshot, requested_action, mode, now);

    if mode == "dry_run" {
        let receipt = state.services.cross_plane.preview_action(
            idempotency_key,
            mode.to_string(),
            action,
            decision,
        );
        return Ok(Json(serde_json::json!({
            "kind": "mfg.cross_plane_action_bridge",
            "mode": receipt.mode.clone(),
            "status": receipt.status.clone(),
            "dispatch_status": receipt.dispatch_status.clone(),
            "execution": execution,
            "cross_plane_execution_receipt": receipt,
            "idempotent_replay": false,
        })));
    }

    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            if !state
                .services
                .mfg
                .execution_bridge_receipt_matches(&receipt, &action)
            {
                return Err(mfg_api_error(
                    StatusCode::CONFLICT,
                    "MFG execution bridge idempotency key belongs to another cross-plane action",
                ));
            }
            let execution = state
                .services
                .mfg
                .attach_execution_cross_plane_receipt(&state.config_home, &execution, &receipt)
                .map_err(|error| {
                    mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                })?;
            return Ok(Json(serde_json::json!({
                "kind": "mfg.cross_plane_action_bridge",
                "mode": receipt.mode,
                "status": receipt.status,
                "dispatch_status": receipt.dispatch_status,
                "execution": execution,
                "cross_plane_execution_receipt": receipt,
                "idempotent_replay": true,
            })));
        }
    }

    let receipt = if mode == "commit" && decision.decision == runtime::PolicyDecisionKind::Allow {
        let graph_key = idempotency_key
            .clone()
            .unwrap_or_else(|| format!("mfg-{}", uuid::Uuid::new_v4()));
        let target = runtime::CrossPlaneDispatchTarget::from_action(
            &action,
            Some("feishu"),
            Some("send_text"),
        )
        .unwrap_or_default();
        let executor = std::sync::Arc::new(crate::services::GatewayCrossPlaneExecutor::new(
            state.services.surface.clone(),
            target.clone(),
            state.services.cross_plane.runtime_control(),
        ));
        let projection = state
            .services
            .cross_plane
            .execute_commit_graph(&action, &decision, &graph_key, Some(&target), executor)
            .await
            .map_err(mfg_cross_plane_graph_error)?;
        state
            .services
            .cross_plane
            .record_message_dispatch_graph(
                graph_key,
                action,
                decision,
                evidence,
                target,
                &projection,
            )
            .map_err(mfg_cross_plane_error)?
    } else {
        state
            .services
            .cross_plane
            .record_non_commit_action(
                idempotency_key,
                mode.to_string(),
                action,
                decision,
                evidence,
            )
            .map_err(mfg_cross_plane_error)?
    };
    let execution = state
        .services
        .mfg
        .attach_execution_cross_plane_receipt(&state.config_home, &execution, &receipt)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cross_plane_action_bridge",
        "mode": receipt.mode.clone(),
        "status": receipt.status.clone(),
        "dispatch_status": receipt.dispatch_status.clone(),
        "execution": execution,
        "cross_plane_execution_receipt": receipt,
        "idempotent_replay": false,
    })))
}
