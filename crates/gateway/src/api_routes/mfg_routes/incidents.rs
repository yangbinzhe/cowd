use axum::extract::Extension;

use crate::api_routes::{principal_actor_id, AuthenticatedPrincipal};

use super::*;

pub(super) async fn mfg_incident_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgIncidentCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let packet = match request.evidence_packet_id.as_deref() {
        Some(packet_id) => {
            let mfg_packet = state
                .services
                .mfg
                .get_evidence_packet(&state.config_home, packet_id)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
            match mfg_packet {
                Some(packet) => packet,
                None => state
                    .services
                    .matrix
                    .get_evidence_packet(&state.config_home, packet_id)
                    .map_err(|error| {
                        api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    })?
                    .map(|packet| {
                        state
                            .services
                            .mfg
                            .upsert_evidence_packet(&state.config_home, &packet)
                    })
                    .transpose()
                    .map_err(|error| {
                        api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    })?
                    .ok_or_else(|| {
                        api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found")
                    })?,
            }
        }
        None => state
            .services
            .mfg
            .build_evidence_packet(
                &state.config_home,
                request.attention_id.as_deref(),
                request.title.as_deref(),
            )
            .map_err(|error| match error {
                MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?,
    };
    let title = request
        .title
        .clone()
        .unwrap_or_else(|| packet.problem_statement.clone());
    let task = state
        .services
        .task
        .start_goal(format!("MFG incident analysis: {title}"), false)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let mut incident = MfgIncident::new(title);
    incident.attention_id = packet.attention_id.clone();
    incident.evidence_packet_id = Some(packet.packet_id.clone());
    incident.task_id = Some(task.id.clone());
    let (incident, workflow_graph) = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| store.create_incident_workflow(&incident, &packet))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let executions = state
        .services
        .mfg
        .list_executions_for_incident(&state.config_home, &id, 20)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let assignments = state
        .services
        .mfg
        .list_assignments(&state.config_home, None, Some(&id), 100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let analysis = state
        .services
        .mfg
        .analyze_incident(&state.config_home, &id)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
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
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG memory case not found"))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .upsert_playbook(&state.config_home, &request.playbook)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.playbook",
        "request_id": request.request_id,
        "session_id": request.session_id,
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG playbook not found"))?;
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
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let plan = state.services.mfg.plan_server_skills(
        &context.incident,
        context.analysis.as_ref(),
        context.packet.as_ref(),
        request.limit.unwrap_or(3).clamp(1, 8),
    );
    let workflow_graph = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| store.plan_incident_workflow_skills(&id, &plan))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    Json(request): Json<MfgSkillRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let context = state
        .services
        .mfg
        .incident_context(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let skill = state
        .services
        .mfg
        .skill_manifest(&skill_id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG skill not found"))?;
    let run = state.services.mfg.run_server_skill(
        &context.incident,
        &skill,
        context.analysis.as_ref(),
        context.packet.as_ref(),
    );
    let (run, workflow_graph) = state
        .services
        .mfg
        .open_store(&state.config_home)
        .and_then(|store| store.record_skill_run_and_complete_workflow(&run))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    append_mfg_execution_outcome(
        &state,
        session_id
            .as_deref()
            .or(context.incident.task_id.as_deref()),
        mfg_skill_run_execution_outcome(&run),
    )
    .await
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let runs = state
        .services
        .mfg
        .list_skill_runs_for_incident(&state.config_home, &incident.incident_id, 24)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG skill run not found"))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG operational analysis not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.operational_analysis",
        "analysis": analysis,
    })))
}

pub(super) async fn mfg_action_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((analysis_id, action_id)): AxumPath<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(intent): Json<MfgActionExecutionIntent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let request = intent.into_request(principal_actor_id(&principal));
    let execution = state
        .services
        .mfg
        .execute_recommended_action(&state.config_home, &analysis_id, &action_id, &request)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    let incident = state
        .services
        .mfg
        .get_incident(&state.config_home, &execution.incident_id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    append_mfg_execution_outcome(
        &state,
        incident
            .as_ref()
            .and_then(|incident| incident.task_id.as_deref())
            .or(Some(execution.incident_id.as_str())),
        mfg_action_execution_outcome(&execution),
    )
    .await
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG action execution not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.action_execution",
        "execution": execution,
    })))
}

pub(super) async fn mfg_execution_cross_plane_bridge_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(intent): Json<MfgCrossPlaneBridgeIntent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let request = intent.into_request(principal_actor_id(&principal));
    let execution = state
        .services
        .mfg
        .get_execution(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG action execution not found"))?;
    let mode = state.services.mfg.normalize_bridge_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);

    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            let execution = state
                .services
                .mfg
                .attach_execution_cross_plane_receipt(&state.config_home, &execution, &receipt)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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

    let action = state
        .services
        .mfg
        .cross_plane_action_from_execution(&execution, &request);
    let now = chrono::Utc::now();
    let snapshot = crate::api_routes::connector_routes::connector_snapshot(&state);
    let (action, decision, evidence) = state
        .services
        .cross_plane
        .decide_connector_action(&snapshot, action, &mode, now);
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
            .execute_commit_graph(&action, &decision, &graph_key, executor)
            .await
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        state
            .services
            .cross_plane
            .record_non_commit_action(idempotency_key, mode.clone(), action, decision, evidence)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    };
    let execution = state
        .services
        .mfg
        .attach_execution_cross_plane_receipt(&state.config_home, &execution, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
