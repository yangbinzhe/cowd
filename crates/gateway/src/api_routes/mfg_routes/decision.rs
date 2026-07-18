use axum::extract::Extension;

use crate::api_routes::AuthenticatedPrincipal;

use super::*;

pub(super) async fn mfg_skills_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill_pack",
        "domain": "server_manufacturing",
        "items": state.services.mfg.skill_pack(),
    })))
}

pub(super) async fn mfg_skill_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let skill = state
        .services
        .mfg
        .skill_manifest(&id)
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG skill not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill",
        "skill": skill,
    })))
}

pub(super) async fn mfg_command_center_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = state
        .services
        .mfg
        .health(&state.config_home)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let attention = state
        .services
        .mfg
        .list_attention(&state.config_home, 10)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let changes = state
        .services
        .mfg
        .list_changes(&state.config_home, 10)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let skills = state.services.mfg.skill_pack();
    Ok(Json(serde_json::json!({
        "kind": "mfg.command_center",
        "health": health,
        "risk_queue": attention,
        "recent_changes": changes,
        "skill_count": skills.len(),
        "operating_lanes": [
            "supply_risk",
            "clear_to_build",
            "capacity",
            "quality",
            "delivery",
            "procurement",
            "plan_change"
        ],
    })))
}

pub(super) async fn mfg_command_center_live_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incidents = state
        .services
        .mfg
        .list_incidents(&state.config_home, 12)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let attention = state
        .services
        .mfg
        .list_attention(&state.config_home, 12)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let action_queue = state
        .services
        .mfg
        .list_recent_action_executions(&state.config_home, 12)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let skill_queue = state
        .services
        .mfg
        .list_recent_skill_runs(&state.config_home, 12)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.command_center.live",
        "incident_queue": incidents,
        "attention_queue": attention,
        "action_queue": action_queue,
        "skill_queue": skill_queue,
        "captured_at": chrono::Utc::now(),
    })))
}

pub(super) async fn mfg_decision_trace_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MatrixDecisionTraceQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = state
        .services
        .mfg
        .list_source_packs(&state.config_home, 1)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let fact = state
        .services
        .mfg
        .list_facts(&state.config_home, 1)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let entity = state
        .services
        .mfg
        .list_entities(&state.config_home, 1)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let metric = state
        .services
        .mfg
        .list_metric_definitions(&state.config_home)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let attention = state
        .services
        .mfg
        .list_attention(&state.config_home, 1)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let evidence = state
        .services
        .mfg
        .list_evidence_packets(&state.config_home, 1)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let incident = if let Some(id) = query.incident_id.as_deref() {
        state
            .services
            .mfg
            .get_incident(&state.config_home, id)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        state
            .services
            .mfg
            .list_incidents(&state.config_home, 1)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .into_iter()
            .next()
    };
    let analysis = if let Some(incident) = incident.as_ref() {
        state
            .services
            .mfg
            .latest_analysis_for_incident(&state.config_home, &incident.incident_id)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        None
    };
    let action = state
        .services
        .mfg
        .list_recent_action_executions(&state.config_home, 1)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let report = if let Some(id) = query.report_id.as_deref() {
        state
            .services
            .mfg
            .get_cockpit_report(&state.config_home, id)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        None
    };
    let report = match report {
        Some(report)
            if cockpit_report_accessible_to(&state, &report, &principal).map_err(|error| {
                mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
            })? =>
        {
            Some(report)
        }
        Some(_) => {
            return Err(mfg_api_error(
                StatusCode::NOT_FOUND,
                "MFG cockpit report was not found in the verified principal scope",
            ));
        }
        None => None,
    };
    let delivery_state = report
        .as_ref()
        .map(MfgCockpitReportDeliveryState::from_report);

    let source_pack_ref = source_pack
        .as_ref()
        .map(|pack| format!("source-pack://{}", pack.source_pack_id))
        .or_else(|| fact.as_ref().and_then(|fact| fact.source_ref.clone()))
        .unwrap_or_else(|| "source-pack://pending".to_string());
    let fact_ref = fact
        .as_ref()
        .map(|fact| fact.fact_id.clone())
        .unwrap_or_else(|| "fact pending".to_string());
    let entity_ref = entity
        .as_ref()
        .map(|entity| entity.entity_id.clone())
        .or_else(|| {
            fact.as_ref()
                .and_then(|fact| fact.entity_refs.first().cloned())
        })
        .unwrap_or_else(|| "entity pending".to_string());
    let metric_ref = metric
        .as_ref()
        .map(|metric| metric.metric_id.clone())
        .or_else(|| fact.as_ref().and_then(|fact| fact.metric_key.clone()))
        .unwrap_or_else(|| "metric pending".to_string());
    let attention_ref = attention
        .as_ref()
        .map(|item| item.attention_id.clone())
        .unwrap_or_else(|| "attention pending".to_string());
    let evidence_ref = evidence
        .as_ref()
        .map(|packet| packet.packet_id.clone())
        .or_else(|| {
            incident
                .as_ref()
                .and_then(|item| item.evidence_packet_id.clone())
        })
        .unwrap_or_else(|| "evidence pending".to_string());
    let incident_ref = incident
        .as_ref()
        .map(|item| item.incident_id.clone())
        .unwrap_or_else(|| "incident pending".to_string());
    let action_ref = action
        .as_ref()
        .map(|item| item.execution_id.clone())
        .or_else(|| {
            analysis
                .as_ref()
                .and_then(|item| item.recommended_actions.first())
                .map(|item| item.action_id.clone())
        })
        .unwrap_or_else(|| "action pending".to_string());
    let report_ref = report
        .as_ref()
        .map(|item| item.report_id.clone())
        .or_else(|| query.report_id.clone())
        .unwrap_or_else(|| "report pending".to_string());

    let rows = vec![
        serde_json::json!({
            "stage": "source",
            "ref": source_pack_ref,
            "domain": "Matrix data plane",
            "signal": source_pack.as_ref().map(|pack| pack.source_name.as_str()).unwrap_or("source pending"),
            "next": "validate source pack / ingest plan",
            "endpoint": "/api/matrix/source-packs/:id",
        }),
        serde_json::json!({
            "stage": "fact",
            "ref": fact_ref,
            "domain": "cowd structured core",
            "signal": fact.as_ref().map(|fact| fact.fact_type.as_str()).unwrap_or("fact pending"),
            "next": "bind facts to entities and metrics",
            "endpoint": "/api/matrix/facts/ingest",
        }),
        serde_json::json!({
            "stage": "entity",
            "ref": entity_ref,
            "domain": entity.as_ref().map(|entity| entity.entity_type.as_str()).unwrap_or("Matrix entity graph"),
            "signal": entity.as_ref().map(|entity| entity.display_name.as_str()).unwrap_or("resolution pending"),
            "next": "trace relations and impact paths",
            "endpoint": "/api/matrix/entities/:id/impact-path",
        }),
        serde_json::json!({
            "stage": "metric",
            "ref": metric_ref,
            "domain": "Matrix metric engine",
            "signal": metric.as_ref().map(|metric| metric.name.as_str()).unwrap_or("lineage pending"),
            "next": "materialize snapshot / attention plan",
            "endpoint": "/api/matrix/metrics/:id/lineage",
        }),
        serde_json::json!({
            "stage": "attention",
            "ref": attention_ref,
            "domain": "Matrix attention",
            "signal": attention
                .as_ref()
                .map(|item| format!("{:?}", item.severity))
                .unwrap_or_else(|| "hot queue pending".to_string()),
            "next": "build evidence packet",
            "endpoint": "/api/matrix/attention/hot",
        }),
        serde_json::json!({
            "stage": "evidence",
            "ref": evidence_ref,
            "domain": "cowd context evidence",
            "signal": evidence
                .as_ref()
                .map(|packet| format!("confidence {:.2}", packet.confidence))
                .unwrap_or_else(|| "quality gate pending".to_string()),
            "next": "open incident room",
            "endpoint": "/api/matrix/evidence/:id",
        }),
        serde_json::json!({
            "stage": "incident",
            "ref": incident_ref,
            "domain": "MFG application",
            "signal": incident.as_ref().map(|item| item.status.as_str()).unwrap_or("analysis pending"),
            "next": "plan skills and actions",
            "endpoint": "/api/apps/mfg/incidents/:id/room",
        }),
        serde_json::json!({
            "stage": "action",
            "ref": action_ref,
            "domain": "MFG + cross-plane",
            "signal": action.as_ref().map(|item| item.status.as_str()).unwrap_or_else(|| {
                analysis
                    .as_ref()
                    .map(|_| "recommended")
                    .unwrap_or("dry-run pending")
            }),
            "next": "receipt / feedback / report",
            "endpoint": "/api/apps/mfg/analyses/:analysis_id/actions/:action_id/execute",
        }),
        serde_json::json!({
            "stage": "report",
            "ref": report_ref,
            "domain": "MFG cockpit",
            "signal": delivery_state
                .as_ref()
                .map(|state| state.classification.as_str())
                .or_else(|| report.as_ref().map(|item| item.status.as_str()))
                .unwrap_or("report pending"),
            "next": "delivery state / retry governance",
            "endpoint": "/api/apps/mfg/cockpit/reports/:id/delivery-state",
        }),
    ];

    Ok(Json(serde_json::json!({
        "kind": "mfg.decision_trace",
        "status": "ready",
        "chain": "source -> fact -> metric -> evidence -> incident -> action -> report",
        "rows": rows,
        "refs": {
            "source_pack_id": source_pack.as_ref().map(|item| item.source_pack_id.clone()),
            "fact_id": fact.as_ref().map(|item| item.fact_id.clone()),
            "entity_id": entity.as_ref().map(|item| item.entity_id.clone()),
            "metric_id": metric.as_ref().map(|item| item.metric_id.clone()),
            "attention_id": attention.as_ref().map(|item| item.attention_id.clone()),
            "evidence_packet_id": evidence.as_ref().map(|item| item.packet_id.clone()),
            "incident_id": incident.as_ref().map(|item| item.incident_id.clone()),
            "analysis_id": analysis.as_ref().map(|item| item.analysis_id.clone()),
            "action_execution_id": action.as_ref().map(|item| item.execution_id.clone()),
            "report_id": report.as_ref().map(|item| item.report_id.clone()),
        },
        "objects": {
            "source_pack": source_pack,
            "fact": fact,
            "entity": entity,
            "metric": metric,
            "attention": attention,
            "evidence": evidence,
            "incident": incident,
            "analysis": analysis,
            "action": action,
            "report": report,
            "delivery_state": delivery_state,
        },
    })))
}
