use std::sync::Arc;

use app_mfg::{
    MfgActionExecution, MfgActionExecutionRequest, MfgActionFeedback, MfgCockpitProfile,
    MfgCockpitProfileInput, MfgCockpitReportDeliveryState, MfgCockpitReportRequest,
    MfgCockpitReportSnapshot, MfgIncident, MfgPlaybook, MfgRepositoryError, MfgSkillRun,
};
use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use matrix_core::{
    MatrixComputeJobInput, MatrixConnectorRunInput, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixEntity, MatrixEntityInput, MatrixEvidencePacket,
    MatrixFact, MatrixFactInput, MatrixMetricDependency, MatrixMetricDependencyInput,
    MatrixRelation, MatrixRelationInput, MatrixSourcePack, MATRIX_SCHEMA_VERSION,
};
use memory::store::session::SessionRecord;
use runtime::execution_outcome::{
    CowdExecutionOutcome, CowdExecutionOutcomeKind, CowdExecutionOutcomeStatus, CowdExecutionRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::services::{
    GatewayMatrixRepositoryError as MatrixStoreError, MfgCockpitReportDeliveryOutcome,
    MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .merge(matrix_kernel_router())
        .merge(mfg_app_router())
}

fn mfg_app_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/apps/mfg/app", get(mfg_app_handler))
        .route(
            "/api/apps/mfg/production/governance",
            get(matrix_production_governance_handler),
        )
        .route("/api/apps/mfg/skills", get(mfg_skills_handler))
        .route("/api/apps/mfg/skills/:id", get(mfg_skill_get_handler))
        .route(
            "/api/apps/mfg/skill-runs/:id",
            get(mfg_skill_run_get_handler),
        )
        .route(
            "/api/apps/mfg/command-center",
            get(matrix_command_center_handler),
        )
        .route(
            "/api/apps/mfg/command-center/live",
            get(matrix_command_center_live_handler),
        )
        .route(
            "/api/apps/mfg/decision-trace",
            get(matrix_decision_trace_handler),
        )
        .route(
            "/api/apps/mfg/domain/server-manufacturing",
            get(matrix_server_manufacturing_domain_handler),
        )
        .route(
            "/api/apps/mfg/domain/server-manufacturing/seed",
            post(matrix_server_manufacturing_seed_handler),
        )
        .route(
            "/api/apps/mfg/ontology/server-manufacturing",
            get(matrix_server_manufacturing_ontology_handler),
        )
        .route(
            "/api/apps/mfg/ontology/server-manufacturing/seed",
            post(matrix_server_manufacturing_ontology_seed_handler),
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
        .route(
            "/api/apps/mfg/analyses/:id",
            get(matrix_analysis_get_handler),
        )
        .route(
            "/api/apps/mfg/analyses/:analysis_id/actions/:action_id/execute",
            post(matrix_action_execute_handler),
        )
        .route(
            "/api/apps/mfg/executions/:id",
            get(matrix_execution_get_handler),
        )
        .route(
            "/api/apps/mfg/executions/:id/cross-plane/execute",
            post(matrix_execution_cross_plane_bridge_handler),
        )
        .route(
            "/api/apps/mfg/executions/:id/feedback",
            post(matrix_execution_feedback_handler),
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

fn matrix_kernel_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/matrix/health", get(matrix_health_handler))
        .route(
            "/api/matrix/data-plane/health",
            get(matrix_data_plane_health_handler),
        )
        .route(
            "/api/matrix/data-plane/ingest-plan",
            post(matrix_data_plane_ingest_plan_handler),
        )
        .route(
            "/api/matrix/source-packs/upsert",
            post(matrix_source_pack_upsert_handler),
        )
        .route(
            "/api/matrix/source-packs/:id",
            get(matrix_source_pack_get_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/validate",
            post(matrix_source_pack_validate_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/ingest-file",
            post(matrix_source_pack_ingest_file_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/delta-plan",
            post(matrix_source_pack_delta_plan_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/connector-runs/plan",
            post(matrix_source_pack_connector_run_plan_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/connector-runs/run",
            post(matrix_source_pack_connector_run_execute_handler),
        )
        .route(
            "/api/matrix/connector-runs/:id",
            get(matrix_connector_run_get_handler),
        )
        .route("/api/matrix/entities", get(matrix_entities_handler))
        .route(
            "/api/matrix/entities/upsert",
            post(matrix_entity_upsert_handler),
        )
        .route(
            "/api/matrix/entities/resolve-source-key",
            post(matrix_entity_resolve_source_key_handler),
        )
        .route(
            "/api/matrix/entities/match-candidate",
            post(matrix_entity_match_candidate_handler),
        )
        .route(
            "/api/matrix/entities/conflict-decision",
            post(matrix_entity_conflict_decision_handler),
        )
        .route("/api/matrix/entities/:id", get(matrix_entity_get_handler))
        .route(
            "/api/matrix/entities/:id/relations",
            get(matrix_entity_relations_handler),
        )
        .route(
            "/api/matrix/entities/:id/impact-path",
            get(matrix_entity_impact_path_handler),
        )
        .route(
            "/api/matrix/relations/upsert",
            post(matrix_relation_upsert_handler),
        )
        .route("/api/matrix/facts/ingest", post(matrix_fact_ingest_handler))
        .route("/api/matrix/metrics", get(matrix_metrics_handler))
        .route("/api/matrix/metrics/:id", get(matrix_metric_detail_handler))
        .route(
            "/api/matrix/metrics/:id/lineage",
            get(matrix_metric_lineage_handler),
        )
        .route(
            "/api/matrix/metrics/attention-plan",
            post(matrix_metric_attention_plan_handler),
        )
        .route(
            "/api/matrix/metrics/snapshots/materialize",
            post(matrix_metric_snapshot_materialize_handler),
        )
        .route(
            "/api/matrix/metrics/recompute",
            post(matrix_metric_recompute_handler),
        )
        .route(
            "/api/matrix/metric-dependencies/upsert",
            post(matrix_metric_dependency_upsert_handler),
        )
        .route(
            "/api/matrix/metric-dependencies/affected-by-fact-type",
            post(matrix_metric_affected_by_fact_type_handler),
        )
        .route(
            "/api/matrix/compute/jobs/plan",
            post(matrix_compute_job_plan_handler),
        )
        .route(
            "/api/matrix/compute/jobs/:id",
            get(matrix_compute_job_get_handler),
        )
        .route(
            "/api/matrix/compute/jobs/:id/run",
            post(matrix_compute_job_run_handler),
        )
        .route("/api/matrix/changes", get(matrix_changes_handler))
        .route(
            "/api/matrix/attention/hot",
            get(matrix_attention_hot_handler),
        )
        .route(
            "/api/matrix/evidence/build",
            post(matrix_evidence_build_handler),
        )
        .route("/api/matrix/evidence/:id", get(matrix_evidence_get_handler))
        .route(
            "/api/matrix/evidence/:id/quality-gate",
            post(matrix_evidence_quality_gate_handler),
        )
        .route(
            "/api/matrix/evidence/:id/context",
            get(matrix_evidence_context_handler),
        )
        .route(
            "/api/matrix/quality-gates/:id",
            get(matrix_quality_gate_get_handler),
        )
}

async fn matrix_app_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mfg.app_descriptor())
}

async fn mfg_app_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mfg.app_descriptor())
}

#[derive(Debug, Deserialize)]
struct MatrixFactIngestRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
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
struct MatrixEntityUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    entity: MatrixEntityInput,
}

#[derive(Debug, Deserialize)]
struct MatrixEntityResolveSourceKeyRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    source_system: String,
    source_key: String,
}

#[derive(Debug, Deserialize)]
struct MatrixEntityMatchCandidateRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    left_entity_id: String,
    right_entity_id: String,
}

#[derive(Debug, Deserialize)]
struct MatrixEntityConflictDecisionRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    candidate_id: String,
    survivor_entity_id: String,
    retired_entity_id: String,
    survivorship_rule: String,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MatrixRelationUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    relation: MatrixRelationInput,
}

#[derive(Debug, Deserialize)]
struct MatrixMetricDependencyUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    dependency: MatrixMetricDependencyInput,
}

#[derive(Debug, Deserialize)]
struct MatrixMetricAttentionPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    trigger_fact_type: String,
    #[serde(default)]
    entity_scope: Option<String>,
    #[serde(default)]
    period: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MatrixMetricSnapshotMaterializeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    metric_ids: Vec<String>,
    #[serde(default)]
    scope_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MfgAffectedByFactTypeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    fact_type: String,
}

#[derive(Debug, Deserialize)]
struct MfgComputeJobPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    job: MatrixComputeJobInput,
}

#[derive(Debug, Deserialize)]
struct MatrixDataPlaneIngestPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    ingest: MatrixDataPlaneIngestPlanInput,
}

#[derive(Debug, Deserialize)]
struct MatrixEvidenceBuildRequest {
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

#[derive(Debug, Deserialize)]
struct MatrixSourcePackUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    source_pack: MatrixSourcePack,
}

#[derive(Debug, Deserialize)]
struct MatrixSourcePackIngestFileRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
}

#[derive(Debug, Deserialize)]
struct MfgConnectorRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    run: Option<MatrixConnectorRunInput>,
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

async fn matrix_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = state
        .services
        .matrix
        .repository_health(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let capabilities = matrix_health_capabilities();
    let store_path = state
        .services
        .matrix
        .store_path(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.health",
        "status": "ready",
        "schema_version": health.schema_version,
        "expected_schema_version": MATRIX_SCHEMA_VERSION,
        "fact_count": health.fact_count,
        "metric_definition_count": health.metric_definition_count,
        "metric_state_count": health.metric_state_count,
        "change_count": health.change_count,
        "attention_count": health.attention_count,
        "evidence_count": health.evidence_count,
        "entity_count": health.entity_count,
        "relation_count": health.relation_count,
        "metric_dependency_count": health.metric_dependency_count,
        "compute_job_count": health.compute_job_count,
        "quality_gate_count": health.quality_gate_count,
        "source_pack_count": health.source_pack_count,
        "data_plane_watermark_count": health.data_plane_watermark_count,
        "connector_run_count": health.connector_run_count,
        "ontology_pack_count": health.ontology_pack_count,
        "entity_match_candidate_count": health.entity_match_candidate_count,
        "entity_conflict_decision_count": health.entity_conflict_decision_count,
        "metric_snapshot_count": health.metric_snapshot_count,
        "store": store_path,
        "capabilities": capabilities,
    })))
}

#[derive(Debug, Clone, Serialize)]
struct MfgProductionGovernanceBundle {
    auth_token_configured: bool,
    approval_gate_configured: bool,
    session_store_ready: bool,
    platform_runtime_ready: bool,
    audit_export_surface: bool,
    cross_plane_audit_surface: bool,
    runbook_present: bool,
    health_capability_present: bool,
}

async fn matrix_production_governance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let bundle = MfgProductionGovernanceBundle {
        auth_token_configured: state.auth_token.is_some(),
        approval_gate_configured: state.services.approval.is_configured(),
        session_store_ready: state.services.session.has_unified_store(),
        platform_runtime_ready: state.platform_runtime.is_some(),
        audit_export_surface: true,
        cross_plane_audit_surface: true,
        runbook_present: state
            .workspace_root
            .join("docs/operator/mfg-production-runbook.md")
            .is_file(),
        health_capability_present: matrix_health_capabilities()
            .contains(&"production_governance_bundle"),
    };

    let checks = [
        bundle.auth_token_configured,
        bundle.approval_gate_configured,
        bundle.session_store_ready,
        bundle.platform_runtime_ready,
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
    if !bundle.platform_runtime_ready {
        reasons.push("platform_runtime_unavailable");
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

fn matrix_health_capabilities() -> Vec<&'static str> {
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

async fn matrix_data_plane_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = state
        .services
        .matrix
        .data_plane_health(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.data_plane.health",
        "health": health,
    })))
}

async fn matrix_data_plane_ingest_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixDataPlaneIngestPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let plan = state
        .services
        .matrix
        .plan_data_plane_ingest(&state.config_home, request.ingest)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    append_matrix_execution_outcome(
        &state,
        session_id.as_deref(),
        matrix_ingest_plan_outcome(&plan),
    )
    .await
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.data_plane.ingest_plan",
        "request_id": request.request_id,
        "session_id": session_id,
        "plan": plan,
    })))
}

async fn mfg_skills_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill_pack",
        "domain": "server_manufacturing",
        "items": state.services.mfg.skill_pack(),
    })))
}

async fn mfg_skill_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let skill = state
        .services
        .mfg
        .skill_manifest(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG skill not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill",
        "skill": skill,
    })))
}

async fn matrix_command_center_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = state
        .services
        .mfg
        .health(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let attention = state
        .services
        .mfg
        .list_attention(&state.config_home, 10)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let changes = state
        .services
        .mfg
        .list_changes(&state.config_home, 10)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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

async fn matrix_command_center_live_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incidents = state
        .services
        .mfg
        .list_incidents(&state.config_home, 12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let attention = state
        .services
        .mfg
        .list_attention(&state.config_home, 12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let action_queue = state
        .services
        .mfg
        .list_recent_action_executions(&state.config_home, 12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let skill_queue = state
        .services
        .mfg
        .list_recent_skill_runs(&state.config_home, 12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.command_center.live",
        "incident_queue": incidents,
        "attention_queue": attention,
        "action_queue": action_queue,
        "skill_queue": skill_queue,
        "captured_at": chrono::Utc::now(),
    })))
}

async fn matrix_decision_trace_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MatrixDecisionTraceQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = state
        .services
        .mfg
        .list_source_packs(&state.config_home, 1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let fact = state
        .services
        .mfg
        .list_facts(&state.config_home, 1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let entity = state
        .services
        .mfg
        .list_entities(&state.config_home, 1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let metric = state
        .services
        .mfg
        .list_metric_definitions(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let attention = state
        .services
        .mfg
        .list_attention(&state.config_home, 1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let evidence = state
        .services
        .mfg
        .list_evidence_packets(&state.config_home, 1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let incident = if let Some(id) = query.incident_id.as_deref() {
        state
            .services
            .mfg
            .get_incident(&state.config_home, id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        state
            .services
            .mfg
            .list_incidents(&state.config_home, 1)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .into_iter()
            .next()
    };
    let analysis = if let Some(incident) = incident.as_ref() {
        state
            .services
            .mfg
            .latest_analysis_for_incident(&state.config_home, &incident.incident_id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        None
    };
    let action = state
        .services
        .mfg
        .list_recent_action_executions(&state.config_home, 1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let report = if let Some(id) = query.report_id.as_deref() {
        state
            .services
            .mfg
            .get_cockpit_report(&state.config_home, id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        None
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

async fn matrix_source_pack_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixSourcePackUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = state
        .services
        .matrix
        .upsert_source_pack(&state.config_home, request.source_pack)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.source_pack",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack": source_pack,
    })))
}

async fn matrix_source_pack_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = state
        .services
        .matrix
        .get_source_pack(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG source pack not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.source_pack",
        "source_pack": source_pack,
    })))
}

async fn matrix_source_pack_validate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let validation = state
        .services
        .matrix
        .validate_source_pack(&state.config_home, &id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.source_pack.validation",
        "validation": validation,
    })))
}

async fn matrix_source_pack_delta_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let delta_plan = state
        .services
        .matrix
        .source_pack_delta_plan(&state.config_home, &id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.source_pack.delta_plan",
        "delta_plan": delta_plan,
    })))
}

async fn matrix_source_pack_ingest_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MatrixSourcePackIngestFileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .matrix
        .validate_source_pack(&state.config_home, &id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    let mut attention = Vec::new();
    for input in request.facts {
        let fact = MatrixFact::from_input(input);
        let item = state
            .services
            .matrix
            .ingest_fact(&state.config_home, &fact)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        attention.push(item);
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.source_pack.ingest_file",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack_id": id,
        "ingested": attention.len(),
        "attention": attention,
    })))
}

async fn matrix_source_pack_connector_run_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgConnectorRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut input = request.run.unwrap_or(MatrixConnectorRunInput {
        run_id: None,
        mode: Some("plan".to_string()),
        resource_ref: None,
        partition_ref: None,
        credential_ref: None,
        expected_rows: None,
        checksum: None,
    });
    input.mode = Some("plan".to_string());
    let run = state
        .services
        .matrix
        .plan_connector_run(&state.config_home, &id, input)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.connector_run.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "run": run,
    })))
}

async fn matrix_source_pack_connector_run_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgConnectorRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut input = request.run.unwrap_or(MatrixConnectorRunInput {
        run_id: None,
        mode: Some("run".to_string()),
        resource_ref: None,
        partition_ref: None,
        credential_ref: None,
        expected_rows: None,
        checksum: None,
    });
    input.mode = Some("run".to_string());
    let run = state
        .services
        .matrix
        .plan_connector_run(&state.config_home, &id, input)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.connector_run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "run": run,
    })))
}

async fn matrix_connector_run_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let run = state
        .services
        .matrix
        .get_connector_run(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG connector run not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.connector_run",
        "run": run,
    })))
}

async fn mfg_cockpit_profile_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgCockpitProfileUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .mfg
        .upsert_cockpit_profile(
            &state.config_home,
            &MfgCockpitProfile::from_input(request.profile),
        )
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.profile",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "profile": profile,
    })))
}

async fn mfg_cockpit_profile_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let profile = state
        .services
        .mfg
        .get_cockpit_profile(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit profile not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.profile",
        "profile": profile,
    })))
}

async fn mfg_cockpit_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let projection = state
        .services
        .mfg
        .cockpit_projection(&state.config_home, &id)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.projection",
        "projection": projection,
    })))
}

async fn mfg_cockpit_report_generate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportGenerateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .generate_cockpit_report(&state.config_home, &id, request.report)
        .map_err(|error| match error {
            MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "report": report,
    })))
}

async fn mfg_cockpit_report_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "report": report,
    })))
}

async fn mfg_cockpit_report_deliver_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportDeliveryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let outcome = deliver_mfg_cockpit_report(&state, report, request)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery",
        "mode": outcome.mode,
        "status": outcome.status,
        "dispatch_status": outcome.dispatch_status,
        "report": outcome.report,
        "delivery_payload": outcome.delivery_payload,
        "cross_plane_execution_receipt": outcome.cross_plane_execution_receipt,
        "idempotent_replay": outcome.idempotent_replay,
    })))
}

async fn mfg_cockpit_report_delivery_state_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let delivery_state = MfgCockpitReportDeliveryState::from_report(&report);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_state",
        "report_id": report.report_id,
        "delivery_state": delivery_state,
    })))
}

async fn mfg_cockpit_report_delivery_retry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportDeliveryRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let report = state
        .services
        .mfg
        .get_cockpit_report(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let before_state = MfgCockpitReportDeliveryState::from_report(&report);
    if !before_state.retryable && !request.force {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "MFG cockpit report delivery is not retryable: {}",
                before_state.classification
            ),
        ));
    }
    let delivery_request = mfg_retry_delivery_request(&report, &before_state, request);
    let outcome = deliver_mfg_cockpit_report(&state, report, delivery_request)?;
    let after_state = MfgCockpitReportDeliveryState::from_report(&outcome.report);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_retry",
        "before_state": before_state,
        "after_state": after_state,
        "delivery": outcome,
    })))
}

async fn mfg_cockpit_report_schedule_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgCockpitReportScheduleRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let limit = request.limit.unwrap_or(50).clamp(1, 100);
    let profiles = state
        .services
        .mfg
        .list_cockpit_profiles(&state.config_home, request.cadence.as_deref(), limit)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut items = Vec::new();
    let mut delivery_count = 0usize;

    for profile in profiles {
        let report_id = request.report_id_prefix.as_ref().map(|prefix| {
            format!(
                "{}-{}",
                prefix.trim().trim_end_matches('-'),
                profile.profile_id
            )
        });
        let report = state
            .services
            .mfg
            .generate_cockpit_report(
                &state.config_home,
                &profile.profile_id,
                MfgCockpitReportRequest {
                    report_id,
                    cadence: request
                        .cadence
                        .clone()
                        .or_else(|| Some(profile.cadence.clone())),
                    delivery_ref: request
                        .delivery_ref
                        .clone()
                        .or_else(|| default_mfg_schedule_delivery_ref(&profile, &request)),
                    note: Some("scheduled cockpit report".to_string()),
                },
            )
            .map_err(|error| match error {
                MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;

        if request.deliver {
            let delivery_request =
                mfg_schedule_delivery_request(&profile, &report, &request, delivery_count);
            let outcome = deliver_mfg_cockpit_report(&state, report, delivery_request)?;
            delivery_count += 1;
            items.push(serde_json::json!({
                "profile_id": profile.profile_id,
                "owner_ref": profile.owner_ref,
                "cadence": profile.cadence,
                "report": outcome.report,
                "delivery": outcome,
            }));
        } else {
            items.push(serde_json::json!({
                "profile_id": profile.profile_id,
                "owner_ref": profile.owner_ref,
                "cadence": profile.cadence,
                "report": report,
                "delivery": null,
            }));
        }
    }

    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_schedule_run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "cadence": request.cadence,
        "matched_profile_count": items.len(),
        "generated_report_count": items.len(),
        "delivery_count": delivery_count,
        "items": items,
    })))
}

async fn matrix_server_manufacturing_domain_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.domain_pack",
        "pack": state.services.mfg.domain_pack(),
    })))
}

async fn matrix_server_manufacturing_seed_handler(
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

async fn matrix_server_manufacturing_ontology_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.ontology_pack",
        "pack": state.services.mfg.ontology_pack(),
    })))
}

async fn matrix_server_manufacturing_ontology_seed_handler(
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

async fn matrix_entities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entities = state
        .services
        .matrix
        .list_entities(&state.config_home, 100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entities",
        "entities": entities,
    })))
}

async fn matrix_entity_match_candidate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityMatchCandidateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let candidate = state
        .services
        .matrix
        .propose_entity_match(
            &state.config_home,
            &request.left_entity_id,
            &request.right_entity_id,
        )
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity.match_candidate",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "candidate": candidate,
    })))
}

async fn matrix_entity_conflict_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityConflictDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let decision = state
        .services
        .matrix
        .decide_entity_conflict(
            &state.config_home,
            &request.candidate_id,
            &request.survivor_entity_id,
            &request.retired_entity_id,
            &request.survivorship_rule,
            request.notes,
        )
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity.conflict_decision",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "decision": decision,
    })))
}

async fn matrix_entity_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entity = state
        .services
        .matrix
        .upsert_entity(
            &state.config_home,
            &MatrixEntity::from_input(request.entity),
        )
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "entity": entity,
    })))
}

async fn matrix_entity_resolve_source_key_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityResolveSourceKeyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entity = state
        .services
        .matrix
        .resolve_entity_by_source_key(
            &state.config_home,
            &request.source_system,
            &request.source_key,
        )
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG entity source key not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity.resolution",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_system": request.source_system,
        "source_key": request.source_key,
        "entity": entity,
    })))
}

async fn matrix_entity_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entity = state
        .services
        .matrix
        .get_entity(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG entity not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity",
        "entity": entity,
    })))
}

async fn matrix_relation_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixRelationUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let relation = state
        .services
        .matrix
        .upsert_relation(
            &state.config_home,
            &MatrixRelation::from_input(request.relation),
        )
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.relation",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "relation": relation,
    })))
}

async fn matrix_entity_relations_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let relations = state
        .services
        .matrix
        .list_entity_relations(&state.config_home, &id, 100)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity.relations",
        "entity_id": id,
        "relations": relations,
    })))
}

async fn matrix_entity_impact_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let trace = state
        .services
        .matrix
        .impact_trace(&state.config_home, &id, 3)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.entity.impact_path",
        "trace": trace,
    })))
}

async fn matrix_fact_ingest_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixFactIngestRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if request.facts.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "at least one MFG fact is required",
        ));
    }
    let session_id = request.session_id.clone();
    let mut facts = Vec::with_capacity(request.facts.len());
    let mut attention = Vec::with_capacity(request.facts.len());
    for input in request.facts {
        let fact = MatrixFact::from_input(input);
        let item = state
            .services
            .matrix
            .ingest_fact(&state.config_home, &fact)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        append_matrix_execution_outcome(&state, session_id.as_deref(), matrix_fact_outcome(&fact))
            .await
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
        facts.push(fact);
        attention.push(item);
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.fact.ingest",
        "request_id": request.request_id,
        "session_id": session_id,
        "ingested": facts.len(),
        "facts": facts,
        "attention": attention,
    })))
}

async fn matrix_metrics_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metrics = state
        .services
        .matrix
        .list_metric_definitions(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metrics",
        "metrics": metrics,
    })))
}

async fn matrix_metric_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let states = state
        .services
        .matrix
        .metric_states(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if states.is_empty() {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            "MFG metric state not found",
        ));
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.metric",
        "metric_id": id,
        "states": states,
    })))
}

async fn matrix_metric_lineage_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lineage = state
        .services
        .matrix
        .metric_lineage(&state.config_home, &id, 6)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metric.lineage",
        "lineage": lineage,
    })))
}

async fn matrix_metric_attention_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixMetricAttentionPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let plan = state
        .services
        .matrix
        .plan_metric_attention(
            &state.config_home,
            &request.trigger_fact_type,
            request.entity_scope,
            request.period,
            request.limit.unwrap_or(12),
        )
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metric_attention.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
    })))
}

async fn matrix_metric_snapshot_materialize_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixMetricSnapshotMaterializeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if request.metric_ids.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "at least one metric_id is required",
        ));
    }
    let snapshot = state
        .services
        .matrix
        .materialize_metric_snapshot(&state.config_home, request.metric_ids, request.scope_ref)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metric_snapshot",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "snapshot": snapshot,
    })))
}

async fn matrix_metric_dependency_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixMetricDependencyUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let dependency = state
        .services
        .matrix
        .upsert_metric_dependency(
            &state.config_home,
            &MatrixMetricDependency::from_input(request.dependency),
        )
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metric_dependency",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "dependency": dependency,
    })))
}

async fn matrix_metric_affected_by_fact_type_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgAffectedByFactTypeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metric_ids = state
        .services
        .matrix
        .metrics_affected_by_fact_type(&state.config_home, &request.fact_type)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metric_dependency.affected_by_fact_type",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "fact_type": request.fact_type,
        "metric_ids": metric_ids,
    })))
}

async fn matrix_compute_job_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgComputeJobPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let plan = state
        .services
        .matrix
        .plan_compute_job_for_fact_type(&state.config_home, request.job)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.compute.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
    })))
}

async fn matrix_compute_job_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .services
        .matrix
        .get_compute_job(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG compute job not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.compute.job",
        "job": job,
    })))
}

async fn matrix_compute_job_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .services
        .matrix
        .run_compute_job(&state.config_home, &id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.compute.job",
        "job": job,
    })))
}

async fn matrix_metric_recompute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .matrix
        .recompute_metrics(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metrics.recompute",
        "result": result,
    })))
}

async fn matrix_changes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let changes = state
        .services
        .matrix
        .list_changes(&state.config_home, 100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.changes",
        "changes": changes,
    })))
}

async fn matrix_attention_hot_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = state
        .services
        .matrix
        .list_attention(&state.config_home, 50)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.attention.hot",
        "items": items,
    })))
}

async fn matrix_evidence_build_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEvidenceBuildRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let packet = state
        .services
        .matrix
        .build_evidence_packet(
            &state.config_home,
            request.attention_id.as_deref(),
            request.problem_statement.as_deref(),
        )
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    append_matrix_execution_outcome(
        &state,
        session_id.as_deref(),
        matrix_evidence_packet_outcome(&packet),
    )
    .await
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.evidence.packet",
        "request_id": request.request_id,
        "session_id": session_id,
        "packet": packet,
    })))
}

async fn matrix_evidence_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let packet = state
        .services
        .matrix
        .get_evidence_packet(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.evidence.packet",
        "packet": packet,
    })))
}

async fn matrix_evidence_quality_gate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let gate = state
        .services
        .matrix
        .evaluate_evidence_quality(&state.config_home, &id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.quality_gate",
        "gate": gate,
    })))
}

async fn matrix_quality_gate_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let gate = state
        .services
        .matrix
        .get_quality_gate(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG quality gate not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.quality_gate",
        "gate": gate,
    })))
}

async fn matrix_evidence_context_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let packet = state
        .services
        .matrix
        .get_evidence_packet(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.evidence.context_item",
        "context_item": state.services.context.structured_evidence_item(&packet),
    })))
}

async fn mfg_incident_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgIncidentCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let packet = match request.evidence_packet_id.as_deref() {
        Some(packet_id) => state
            .services
            .mfg
            .get_evidence_packet(&state.config_home, packet_id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found"))?,
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
    let (task, graph) = state
        .services
        .enrich_mfg_evidence_agent_graph(&task, &packet)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;

    let mut incident = MfgIncident::new(title);
    incident.attention_id = packet.attention_id.clone();
    incident.evidence_packet_id = Some(packet.packet_id.clone());
    incident.task_id = Some(task.id.clone());
    incident.agent_graph_id = Some(graph.graph_id.clone());
    let incident = state
        .services
        .mfg
        .create_incident(&state.config_home, &incident)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident": incident,
        "task": task,
        "agent_graph": graph,
        "context_item": state.services.context.structured_evidence_item(&packet),
    })))
}

async fn mfg_incident_get_handler(
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

async fn mfg_incident_room_handler(
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
    let agent_graph = incident
        .task_id
        .as_deref()
        .and_then(|task_id| state.services.task.agent_graph(task_id).ok().flatten());
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident_room",
        "incident": incident,
        "evidence_packet": evidence_packet,
        "quality_gate": quality_gate,
        "analysis": analysis,
        "executions": executions,
        "memory_cases": memory_cases,
        "playbooks": playbooks,
        "agent_graph": agent_graph,
    })))
}

async fn mfg_incident_analyze_handler(
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

async fn mfg_incident_case_promote_handler(
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

async fn mfg_memory_case_get_handler(
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

async fn mfg_memory_case_search_handler(
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

async fn mfg_playbook_upsert_handler(
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

async fn mfg_playbook_get_handler(
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

async fn mfg_incident_playbook_recommend_handler(
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

async fn mfg_incident_skill_plan_handler(
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
    let graph = state
        .services
        .plan_mfg_skill_agent_nodes(&context.incident, &plan)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident_id": id,
        "plan": plan,
        "agent_graph": graph,
    })))
}

async fn mfg_incident_skill_run_handler(
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
    let run = state
        .services
        .mfg
        .record_skill_run(&state.config_home, &run)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let graph = state
        .services
        .complete_mfg_skill_agent_node(&context.incident, &run)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    append_matrix_execution_outcome(
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
        "agent_graph": graph,
    })))
}

async fn mfg_incident_skill_runs_handler(
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

async fn mfg_skill_run_get_handler(
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

async fn matrix_analysis_get_handler(
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

async fn matrix_action_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((analysis_id, action_id)): AxumPath<(String, String)>,
    Json(request): Json<MfgActionExecutionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
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
    append_matrix_execution_outcome(
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

async fn matrix_execution_get_handler(
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

async fn matrix_execution_cross_plane_bridge_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCrossPlaneBridgeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state.services.cross_plane.ensure_loaded(&state.config_home);
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
    let snapshot = super::connector_routes::connector_snapshot(&state);
    let (action, decision, evidence) = state
        .services
        .cross_plane
        .decide_connector_action(&snapshot, action, &mode, now);
    let receipt = state.services.mfg.record_cross_plane_bridge_receipt(
        state.services.cross_plane.control(),
        idempotency_key,
        mode.clone(),
        action,
        decision,
        evidence,
    );
    state.services.cross_plane.save_state(&state.config_home);
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

async fn matrix_execution_feedback_handler(
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

fn deliver_mfg_cockpit_report(
    state: &AppState,
    report: MfgCockpitReportSnapshot,
    request: MfgCockpitReportDeliveryRequest,
) -> Result<MfgCockpitReportDeliveryOutcome, (StatusCode, Json<ErrorResponse>)> {
    state.services.cross_plane.ensure_loaded(&state.config_home);
    let mode = state.services.mfg.normalize_bridge_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    let delivery_payload = state
        .services
        .mfg
        .report_delivery_payload(&report, &request);

    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            if !state
                .services
                .mfg
                .report_delivery_receipt_matches(&receipt, &report)
            {
                return Err(api_error(
                    StatusCode::CONFLICT,
                    "MFG cockpit report delivery idempotency key belongs to another cross-plane action",
                ));
            }
            let report = state
                .services
                .mfg
                .attach_report_delivery_receipt(&state.config_home, &report, &receipt)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
            return Ok(MfgCockpitReportDeliveryOutcome {
                mode: receipt.mode.clone(),
                status: receipt.status.clone(),
                dispatch_status: receipt.dispatch_status.clone(),
                report,
                delivery_payload,
                cross_plane_execution_receipt: receipt,
                idempotent_replay: true,
            });
        }
    }

    let action = state
        .services
        .mfg
        .report_delivery_action(&report, &request, &delivery_payload);
    let now = chrono::Utc::now();
    let snapshot = super::connector_routes::connector_snapshot(state);
    let (action, decision, evidence) = state
        .services
        .cross_plane
        .decide_connector_action(&snapshot, action, &mode, now);
    let receipt = state.services.mfg.record_cross_plane_bridge_receipt(
        state.services.cross_plane.control(),
        idempotency_key,
        mode.clone(),
        action,
        decision,
        evidence,
    );
    state.services.cross_plane.save_state(&state.config_home);
    let report = state
        .services
        .mfg
        .attach_report_delivery_receipt(&state.config_home, &report, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(MfgCockpitReportDeliveryOutcome {
        mode: receipt.mode.clone(),
        status: receipt.status.clone(),
        dispatch_status: receipt.dispatch_status.clone(),
        report,
        delivery_payload,
        cross_plane_execution_receipt: receipt,
        idempotent_replay: false,
    })
}

fn default_mfg_schedule_delivery_ref(
    profile: &MfgCockpitProfile,
    request: &MfgCockpitReportScheduleRunRequest,
) -> Option<String> {
    let channel = request
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("feishu");
    profile
        .owner_ref
        .strip_prefix("user:")
        .filter(|user| !user.trim().is_empty())
        .map(|user| format!("channel://{channel}/user/{}", user.trim()))
}

fn mfg_schedule_delivery_request(
    profile: &MfgCockpitProfile,
    report: &MfgCockpitReportSnapshot,
    request: &MfgCockpitReportScheduleRunRequest,
    delivery_index: usize,
) -> MfgCockpitReportDeliveryRequest {
    MfgCockpitReportDeliveryRequest {
        mode: request.mode.clone(),
        idempotency_key: Some(format!(
            "mfg-schedule:{}:{}:{}",
            request
                .request_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("report-run"),
            report.report_id,
            delivery_index
        )),
        actor_principal: request
            .actor_principal
            .clone()
            .or_else(|| Some(profile.owner_ref.clone())),
        actor_identity_ref: request.actor_identity_ref.clone(),
        source_channel: request
            .source_channel
            .clone()
            .or_else(|| Some("mfg.report.schedule".to_string())),
        requested_capability: request.requested_capability.clone(),
        provider_account: request.provider_account.clone(),
        target_ref: report.delivery_ref.clone(),
        resource_ref: None,
        channel: request.channel.clone(),
        template_id: request.template_id.clone(),
    }
}

fn mfg_retry_delivery_request(
    report: &MfgCockpitReportSnapshot,
    state: &MfgCockpitReportDeliveryState,
    request: MfgCockpitReportDeliveryRetryRequest,
) -> MfgCockpitReportDeliveryRequest {
    let latest_receipt_id = state
        .latest_receipt
        .as_ref()
        .map(|receipt| receipt.cross_plane_receipt_id.as_str())
        .unwrap_or("no-receipt");
    MfgCockpitReportDeliveryRequest {
        mode: request.mode,
        idempotency_key: request.idempotency_key.or_else(|| {
            Some(format!(
                "mfg-retry:{}:{}:{}",
                report.report_id,
                latest_receipt_id,
                state.attempt_count + 1
            ))
        }),
        actor_principal: request
            .actor_principal
            .or_else(|| Some(report.owner_ref.clone())),
        actor_identity_ref: request.actor_identity_ref,
        source_channel: request
            .source_channel
            .or_else(|| Some("mfg.report.retry".to_string())),
        requested_capability: request.requested_capability,
        provider_account: request.provider_account,
        target_ref: request.target_ref.or_else(|| report.delivery_ref.clone()),
        resource_ref: request.resource_ref,
        channel: request.channel,
        template_id: request.template_id,
    }
}

async fn append_matrix_execution_outcome(
    state: &AppState,
    session_id: Option<&str>,
    outcome: CowdExecutionOutcome,
) -> Result<(), String> {
    let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    let Some(store) = state.services.session.unified_store() else {
        return Ok(());
    };
    ensure_matrix_outcome_session_record(state, session_id)
        .await
        .map_err(|error| format!("failed to prepare MFG outcome session: {error}"))?;
    let sequence = store
        .next_event_sequence(session_id)
        .await
        .map_err(|error| error.to_string())?;
    let event = outcome.to_runtime_event(session_id.to_string(), sequence);
    store
        .append_runtime_event(&event)
        .await
        .map_err(|error| error.to_string())
}

fn matrix_ingest_plan_outcome(plan: &MatrixDataPlaneIngestPlan) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!("structured-ingest:{}", plan.batch_id),
        kind: CowdExecutionOutcomeKind::StructuredIngest,
        status: CowdExecutionOutcomeStatus::Planned,
        title: format!("Structured ingest plan for {}", plan.fact_type),
        summary: format!(
            "Plan {} ingests {} estimated rows from {} partition {}.",
            plan.batch_id, plan.estimated_rows, plan.source_ref, plan.partition_ref
        ),
        domain: Some("matrix".to_string()),
        refs: vec![
            CowdExecutionRef {
                ref_type: "structured_source".to_string(),
                id: plan.source_ref.clone(),
                label: Some(plan.source_ref.clone()),
            },
            CowdExecutionRef {
                ref_type: "structured_batch".to_string(),
                id: plan.batch_id.clone(),
                label: Some(plan.fact_type.clone()),
            },
        ],
        evidence_refs: Vec::new(),
        metrics: plan.affected_metric_ids.clone(),
        payload: serde_json::to_value(plan).unwrap_or(Value::Null),
        created_at: plan.planned_at,
    }
}

fn matrix_fact_outcome(fact: &MatrixFact) -> CowdExecutionOutcome {
    let mut refs = vec![CowdExecutionRef {
        ref_type: "structured_fact".to_string(),
        id: fact.fact_id.clone(),
        label: Some(fact.fact_type.clone()),
    }];
    if let Some(source_ref) = fact.source_ref.as_ref() {
        refs.push(CowdExecutionRef {
            ref_type: "structured_source".to_string(),
            id: source_ref.clone(),
            label: Some(source_ref.clone()),
        });
    }
    CowdExecutionOutcome {
        outcome_id: format!("structured-fact:{}", fact.fact_id),
        kind: CowdExecutionOutcomeKind::StructuredFact,
        status: CowdExecutionOutcomeStatus::Succeeded,
        title: format!("Structured fact {}", fact.fact_type),
        summary: format!(
            "Fact {} of type {} references {} entities with confidence {:.2}.",
            fact.fact_id,
            fact.fact_type,
            fact.entity_refs.len(),
            fact.confidence
        ),
        domain: Some("matrix".to_string()),
        refs,
        evidence_refs: fact
            .source_ref
            .iter()
            .map(|source_ref| format!("structured-source:{source_ref}"))
            .collect(),
        metrics: fact.metric_key.iter().cloned().collect(),
        payload: serde_json::to_value(fact).unwrap_or(Value::Null),
        created_at: fact.event_time,
    }
}

fn matrix_evidence_packet_outcome(packet: &MatrixEvidencePacket) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!("structured-evidence:{}", packet.packet_id),
        kind: CowdExecutionOutcomeKind::StructuredEvidence,
        status: if packet.missing_evidence.is_empty() {
            CowdExecutionOutcomeStatus::Succeeded
        } else {
            CowdExecutionOutcomeStatus::Partial
        },
        title: format!("Evidence packet {}", packet.packet_id),
        summary: format!(
            "Evidence packet for '{}' has {} metric items, {} change items and confidence {:.2}.",
            packet.problem_statement,
            packet.metric_evidence.len(),
            packet.change_evidence.len(),
            packet.confidence
        ),
        domain: Some("matrix".to_string()),
        refs: vec![CowdExecutionRef {
            ref_type: "structured_evidence".to_string(),
            id: packet.packet_id.clone(),
            label: packet.attention_id.clone(),
        }],
        evidence_refs: packet
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect(),
        metrics: packet
            .metric_evidence
            .iter()
            .filter_map(|item| item.get("metric_id").and_then(Value::as_str))
            .map(ToString::to_string)
            .collect(),
        payload: serde_json::to_value(packet).unwrap_or(Value::Null),
        created_at: packet.created_at,
    }
}

fn mfg_action_execution_outcome(execution: &MfgActionExecution) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!("manufacturing-action:{}", execution.execution_id),
        kind: CowdExecutionOutcomeKind::ApplicationAction,
        status: execution_status(&execution.status),
        title: execution.title.clone(),
        summary: format!(
            "MFG action {} for incident {} is {}.",
            execution.action_id, execution.incident_id, execution.status
        ),
        domain: Some("mfg".to_string()),
        refs: vec![
            CowdExecutionRef {
                ref_type: "mfg_execution".to_string(),
                id: execution.execution_id.clone(),
                label: Some(execution.action_type.clone()),
            },
            CowdExecutionRef {
                ref_type: "mfg_incident".to_string(),
                id: execution.incident_id.clone(),
                label: None,
            },
        ],
        evidence_refs: execution
            .cross_plane_receipts
            .iter()
            .filter_map(|receipt| receipt.audit_record_id.clone())
            .collect(),
        metrics: Vec::new(),
        payload: execution.receipt.clone(),
        created_at: execution.created_at,
    }
}

fn mfg_skill_run_execution_outcome(run: &MfgSkillRun) -> CowdExecutionOutcome {
    CowdExecutionOutcome {
        outcome_id: format!(
            "skill-run:{}",
            run.execution_id
                .clone()
                .unwrap_or_else(|| format!("{}:{}", run.incident_id, run.skill_id))
        ),
        kind: CowdExecutionOutcomeKind::SkillRun,
        status: execution_status(&run.status),
        title: format!("Skill run {}", run.skill_id),
        summary: run.summary.clone(),
        domain: Some("mfg".to_string()),
        refs: vec![
            CowdExecutionRef {
                ref_type: "mfg_skill".to_string(),
                id: run.skill_id.clone(),
                label: run.agent_node_id.clone(),
            },
            CowdExecutionRef {
                ref_type: "mfg_incident".to_string(),
                id: run.incident_id.clone(),
                label: None,
            },
        ],
        evidence_refs: run
            .execution_context
            .as_ref()
            .map(|context| context.evidence_refs.clone())
            .unwrap_or_default(),
        metrics: run
            .execution_context
            .as_ref()
            .map(|context| context.metric_keys.clone())
            .unwrap_or_default(),
        payload: run.structured_report.clone(),
        created_at: run
            .telemetry
            .as_ref()
            .map(|telemetry| telemetry.completed_at)
            .unwrap_or_else(chrono::Utc::now),
    }
}

fn execution_status(status: &str) -> CowdExecutionOutcomeStatus {
    match status {
        "planned" | "dry_run_ready" | "queued_for_human_review" => {
            CowdExecutionOutcomeStatus::Planned
        }
        "running" | "cross_plane_dispatched" => CowdExecutionOutcomeStatus::Running,
        "completed" | "success" | "feedback_resolved" => CowdExecutionOutcomeStatus::Succeeded,
        "failed" | "error" | "feedback_rejected" => CowdExecutionOutcomeStatus::Failed,
        "blocked" | "cross_plane_blocked" => CowdExecutionOutcomeStatus::Blocked,
        _ => CowdExecutionOutcomeStatus::Partial,
    }
}

async fn ensure_matrix_outcome_session_record(
    state: &AppState,
    session_id: &str,
) -> Result<(), String> {
    let Some(store) = state.services.session.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    if let Some(mut record) = store
        .get_session(session_id)
        .await
        .map_err(|error| error.to_string())?
    {
        record.last_activity = now;
        record.platform = "mfg".to_string();
        store
            .update_session(&record)
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let metadata_json = serde_json::json!({
        "kind": "mfg.execution_outcome.session",
        "session_id": session_id,
    })
    .to_string();
    let record = SessionRecord {
        session_id: session_id.to_string(),
        platform: "mfg".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model: None,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata_json),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    };
    store
        .create_session(&record)
        .await
        .map_err(|error| error.to_string())
}
