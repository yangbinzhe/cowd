use std::sync::Arc;

use app_mfg::{
    MfgActionExecutionRequest, MfgActionFeedback, MfgCockpitProfile, MfgCockpitProfileInput,
    MfgCockpitReportDeliveryState, MfgCockpitReportRequest, MfgCockpitReportSnapshot, MfgIncident,
    MfgPlaybook, MfgRepositoryError,
};
use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::services::{
    MfgCockpitReportDeliveryOutcome, MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
};

use super::mfg_outcomes::{
    append_mfg_execution_outcome, mfg_action_execution_outcome, mfg_skill_run_execution_outcome,
};
mod cockpit;
mod decision;
mod incidents;
use super::{api_error, AppState, ErrorResponse};
use cockpit::*;
use decision::*;
use incidents::*;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/apps/mfg/app", get(mfg_app_handler))
        .route(
            "/api/apps/mfg/production/governance",
            get(mfg_production_governance_handler),
        )
        .route("/api/apps/mfg/skills", get(mfg_skills_handler))
        .route("/api/apps/mfg/skills/:id", get(mfg_skill_get_handler))
        .route(
            "/api/apps/mfg/skill-runs/:id",
            get(mfg_skill_run_get_handler),
        )
        .route(
            "/api/apps/mfg/command-center",
            get(mfg_command_center_handler),
        )
        .route(
            "/api/apps/mfg/command-center/live",
            get(mfg_command_center_live_handler),
        )
        .route(
            "/api/apps/mfg/decision-trace",
            get(mfg_decision_trace_handler),
        )
        .route(
            "/api/apps/mfg/domain/server-manufacturing",
            get(mfg_server_manufacturing_domain_handler),
        )
        .route(
            "/api/apps/mfg/domain/server-manufacturing/seed",
            post(mfg_server_manufacturing_seed_handler),
        )
        .route(
            "/api/apps/mfg/ontology/server-manufacturing",
            get(mfg_server_manufacturing_ontology_handler),
        )
        .route(
            "/api/apps/mfg/ontology/server-manufacturing/seed",
            post(mfg_server_manufacturing_ontology_seed_handler),
        )
        .route("/api/apps/mfg/incidents", get(mfg_incidents_list_handler))
        .route("/api/apps/mfg/incidents", post(mfg_incident_create_handler))
        .route("/api/apps/mfg/incidents/:id", get(mfg_incident_get_handler))
        .route(
            "/api/apps/mfg/incidents/:id/room",
            get(mfg_incident_room_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/analyze",
            post(mfg_incident_analyze_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/cases/promote",
            post(mfg_incident_case_promote_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/playbooks/recommend",
            post(mfg_incident_playbook_recommend_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/skills/plan",
            post(mfg_incident_skill_plan_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/skills/:skill_id/run",
            post(mfg_incident_skill_run_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/skills",
            get(mfg_incident_skill_runs_handler),
        )
        .route("/api/apps/mfg/cases/:id", get(mfg_memory_case_get_handler))
        .route(
            "/api/apps/mfg/cases/search",
            get(mfg_memory_case_search_handler),
        )
        .route(
            "/api/apps/mfg/playbooks/upsert",
            post(mfg_playbook_upsert_handler),
        )
        .route("/api/apps/mfg/playbooks/:id", get(mfg_playbook_get_handler))
        .route("/api/apps/mfg/analyses/:id", get(mfg_analysis_get_handler))
        .route(
            "/api/apps/mfg/analyses/:analysis_id/actions/:action_id/execute",
            post(mfg_action_execute_handler),
        )
        .route(
            "/api/apps/mfg/executions/:id",
            get(mfg_execution_get_handler),
        )
        .route(
            "/api/apps/mfg/executions/:id/cross-plane/execute",
            post(mfg_execution_cross_plane_bridge_handler),
        )
        .route(
            "/api/apps/mfg/executions/:id/feedback",
            post(mfg_execution_feedback_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/upsert",
            post(mfg_cockpit_profile_upsert_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id",
            get(mfg_cockpit_profile_get_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id/projection",
            get(mfg_cockpit_projection_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id/reports/generate",
            post(mfg_cockpit_report_generate_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/schedules/run",
            post(mfg_cockpit_report_schedule_run_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id",
            get(mfg_cockpit_report_get_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id/deliver",
            post(mfg_cockpit_report_deliver_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id/delivery-state",
            get(mfg_cockpit_report_delivery_state_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id/delivery/retry",
            post(mfg_cockpit_report_delivery_retry_handler),
        )
}

async fn mfg_app_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mfg.app_descriptor())
}

#[derive(Debug, Deserialize)]
struct MfgCockpitProfileUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    profile: MfgCockpitProfileInput,
}

#[derive(Debug, Deserialize)]
struct MfgCockpitReportGenerateRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    report: MfgCockpitReportRequest,
}

#[derive(Debug, Deserialize)]
struct MfgCockpitReportScheduleRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    cadence: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    report_id_prefix: Option<String>,
    #[serde(default)]
    delivery_ref: Option<String>,
    #[serde(default)]
    deliver: bool,
    #[serde(default = "default_matrix_bridge_mode")]
    mode: String,
    #[serde(default)]
    actor_principal: Option<String>,
    #[serde(default)]
    actor_identity_ref: Option<String>,
    #[serde(default)]
    source_channel: Option<String>,
    #[serde(default)]
    requested_capability: Option<String>,
    #[serde(default)]
    provider_account: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MfgIncidentCreateRequest {
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
struct MfgExecutionFeedbackRequest {
    outcome: String,
    note: String,
    #[serde(default)]
    metric_delta: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct MfgCaseSearchQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MfgPlaybookUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    playbook: MfgPlaybook,
}

#[derive(Debug, Deserialize)]
struct MfgPlaybookRecommendRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MfgSkillPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MfgSkillRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MfgCockpitReportDeliveryRetryRequest {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    actor_principal: Option<String>,
    #[serde(default)]
    actor_identity_ref: Option<String>,
    #[serde(default)]
    source_channel: Option<String>,
    #[serde(default)]
    requested_capability: Option<String>,
    #[serde(default)]
    provider_account: Option<String>,
    #[serde(default)]
    target_ref: Option<String>,
    #[serde(default)]
    resource_ref: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    template_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MatrixDecisionTraceQuery {
    #[serde(default)]
    incident_id: Option<String>,
    #[serde(default)]
    report_id: Option<String>,
}

fn default_matrix_bridge_mode() -> String {
    "dry_run".to_string()
}

#[derive(Debug, Clone, Serialize)]
struct MfgProductionGovernanceBundle {
    auth_token_configured: bool,
    approval_gate_configured: bool,
    session_store_ready: bool,
    surface_runtime_ready: bool,
    audit_export_surface: bool,
    cross_plane_audit_surface: bool,
    runbook_present: bool,
    health_capability_present: bool,
}

async fn mfg_production_governance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let bundle = MfgProductionGovernanceBundle {
        auth_token_configured: state.auth_token.is_some(),
        approval_gate_configured: state.services.approval.is_configured(),
        session_store_ready: state.services.session.has_unified_store(),
        surface_runtime_ready: state.services.surface.is_runtime_available(),
        audit_export_surface: true,
        cross_plane_audit_surface: true,
        runbook_present: state
            .workspace_root
            .join("docs/operator/mfg-production-runbook.md")
            .is_file(),
        health_capability_present: mfg_application_capabilities()
            .contains(&"production_governance_bundle"),
    };

    let checks = [
        bundle.auth_token_configured,
        bundle.approval_gate_configured,
        bundle.session_store_ready,
        bundle.surface_runtime_ready,
        bundle.audit_export_surface,
        bundle.cross_plane_audit_surface,
        bundle.runbook_present,
        bundle.health_capability_present,
    ];
    let score = checks.iter().filter(|ok| **ok).count();
    let status = if score == checks.len() {
        "ready"
    } else {
        "attention"
    };
    let mut reasons = Vec::new();
    if !bundle.auth_token_configured {
        reasons.push("auth_token_not_configured");
    }
    if !bundle.approval_gate_configured {
        reasons.push("approval_gate_missing");
    }
    if !bundle.session_store_ready {
        reasons.push("session_store_unavailable");
    }
    if !bundle.surface_runtime_ready {
        reasons.push("surface_runtime_unavailable");
    }
    if !bundle.runbook_present {
        reasons.push("production_runbook_missing");
    }

    Ok(Json(serde_json::json!({
        "kind": "mfg.production_governance",
        "status": status,
        "bundle": bundle,
        "readiness": {
            "score": score,
            "total": checks.len(),
            "ready": score == checks.len(),
            "reasons": reasons,
        },
        "evidence": {
            "audit_export_route": "/api/audit/export",
            "cross_plane_audit_route": "/api/cross-plane/audit",
            "production_runbook": "docs/operator/mfg-production-runbook.md",
        }
    })))
}

fn mfg_application_capabilities() -> Vec<&'static str> {
    vec![
        "data_plane_adapter",
        "data_plane_ingest_plan",
        "data_plane_watermark",
        "data_plane_replay_policy",
        "connector_runtime",
        "connector_run_receipt",
        "connector_quality_report",
        "server_manufacturing_ontology",
        "entity_match_candidate",
        "entity_conflict_decision",
        "entity_survivorship_rule",
        "metric_attention_plan",
        "metric_snapshot_materialize",
        "metric_attention_scoring",
        "incremental_metric_focus",
        "cockpit_report_snapshot",
        "scheduled_report_foundation",
        "cockpit_report_delivery_bridge",
        "cockpit_report_payload_templates",
        "cockpit_report_schedule_runner",
        "cockpit_report_delivery_retry_state",
        "cockpit_report_webui_visibility",
        "production_operation_package",
        "production_governance_bundle",
        "memory_case_promotion",
        "playbook_recommendation",
        "mfg_skill_pack",
        "incident_skill_agent_graph",
        "command_center_projection",
        "incident_room_projection",
        "source_onboarding_pack",
        "source_pack_delta_plan",
        "production_pilot_gate",
        "personal_cockpit_projection",
        "cockpit_profile_thresholds",
        "evidence_quality_gate",
        "insight_quality_gate",
        "cross_plane_action_bridge",
        "incremental_compute_job",
        "scoped_metric_recompute",
        "metric_dependency_graph",
        "metric_lineage",
        "fact_type_metric_impact",
        "server_manufacturing_domain_pack",
        "server_manufacturing_seed",
        "entity_relation_network",
        "entity_source_key_resolution",
        "entity_impact_trace",
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
        "action_execution_feedback",
        "skill_execution_record",
        "skill_execution_query",
        "incident_queue",
        "command_center_live",
        "incident_list",
    ]
}

async fn mfg_incidents_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incidents = state
        .services
        .mfg
        .list_incidents(&state.config_home, 50)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident.list",
        "items": incidents,
    })))
}

async fn mfg_server_manufacturing_domain_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.domain_pack",
        "pack": state.services.mfg.domain_pack(),
    })))
}

async fn mfg_server_manufacturing_seed_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .mfg
        .seed_mfg_domain(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.domain_seed",
        "result": result,
    })))
}

async fn mfg_server_manufacturing_ontology_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.ontology_pack",
        "pack": state.services.mfg.ontology_pack(),
    })))
}

async fn mfg_server_manufacturing_ontology_seed_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let pack = state
        .services
        .mfg
        .seed_mfg_ontology(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.ontology_seed",
        "pack": pack,
    })))
}

async fn mfg_execution_feedback_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgExecutionFeedbackRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let execution = state
        .services
        .mfg
        .record_execution_feedback(
            &state.config_home,
            &id,
            MfgActionFeedback::new(request.outcome, request.note, request.metric_delta),
        )
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.action_execution",
        "execution": execution,
    })))
}
