use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use cowd_app_mfg::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_domain_pack, server_manufacturing_ontology_pack,
    server_manufacturing_skill_pack, skill_agent_node_id, MfgActionExecution,
    MfgActionExecutionRequest, MfgActionFeedback, MfgCockpitProfile, MfgCockpitProfileInput,
    MfgCockpitReportDeliveryPayload, MfgCockpitReportDeliveryPayloadRequest,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportDeliveryState, MfgCockpitReportRequest,
    MfgCockpitReportSnapshot, MfgCrossPlaneBridgeReceipt, MfgIncident, MfgPlaybook,
    MfgSkillManifest, MfgSkillPlan, MfgSkillRun, MfgStore,
};
use memory::store::session::SessionRecord;
use runtime::execution_outcome::CowdExecutionOutcome;
use runtime::{
    AgentNodeStatus, AgentRole, AgentRunGraph, AgentTaskNode, CrossPlaneAction,
    CrossPlaneAuditRecord, CrossPlaneExecutionReceipt, CrossPlaneRisk, DataClassification,
    IdentityTrust, MatrixComputeJobInput, MatrixConnectorRunInput, MatrixDataPlaneIngestPlanInput,
    MatrixEntity, MatrixEntityInput, MatrixFact, MatrixFactInput, MatrixMetricDependency,
    MatrixMetricDependencyInput, MatrixRelation, MatrixRelationInput, MatrixSourcePack,
    MatrixStore, MatrixStoreError, PolicyDecisionKind, MATRIX_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};

use crate::task_kernel::TaskRecord;

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
        .route("/api/apps/mfg/skills", get(matrix_skills_handler))
        .route("/api/apps/mfg/skills/:id", get(matrix_skill_get_handler))
        .route(
            "/api/apps/mfg/skill-runs/:id",
            get(matrix_skill_run_get_handler),
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
        .route(
            "/api/apps/mfg/incidents",
            get(matrix_incidents_list_handler),
        )
        .route(
            "/api/apps/mfg/incidents",
            post(matrix_incident_create_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id",
            get(matrix_incident_get_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/room",
            get(matrix_incident_room_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/analyze",
            post(matrix_incident_analyze_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/cases/promote",
            post(matrix_incident_case_promote_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/playbooks/recommend",
            post(matrix_incident_playbook_recommend_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/skills/plan",
            post(matrix_incident_skill_plan_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/skills/:skill_id/run",
            post(matrix_incident_skill_run_handler),
        )
        .route(
            "/api/apps/mfg/incidents/:id/skills",
            get(matrix_incident_skill_runs_handler),
        )
        .route(
            "/api/apps/mfg/cases/:id",
            get(matrix_memory_case_get_handler),
        )
        .route(
            "/api/apps/mfg/cases/search",
            get(matrix_memory_case_search_handler),
        )
        .route(
            "/api/apps/mfg/playbooks/upsert",
            post(matrix_playbook_upsert_handler),
        )
        .route(
            "/api/apps/mfg/playbooks/:id",
            get(matrix_playbook_get_handler),
        )
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
            post(matrix_cockpit_profile_upsert_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id",
            get(matrix_cockpit_profile_get_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id/projection",
            get(matrix_cockpit_projection_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id/reports/generate",
            post(matrix_cockpit_report_generate_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/schedules/run",
            post(matrix_cockpit_report_schedule_run_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id",
            get(matrix_cockpit_report_get_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id/deliver",
            post(matrix_cockpit_report_deliver_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id/delivery-state",
            get(matrix_cockpit_report_delivery_state_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/reports/:id/delivery/retry",
            post(matrix_cockpit_report_delivery_retry_handler),
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

async fn matrix_app_handler() -> impl IntoResponse {
    Json(cowd_app_mfg::manufacturing_app_descriptor())
}

async fn mfg_app_handler() -> impl IntoResponse {
    Json(cowd_app_mfg::manufacturing_app_descriptor())
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

#[derive(Debug, Deserialize)]
struct MfgCrossPlaneBridgeRequest {
    #[serde(default = "default_matrix_bridge_mode")]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
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
}

#[derive(Debug, Deserialize)]
struct MfgCockpitReportDeliveryRequest {
    #[serde(default = "default_matrix_bridge_mode")]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
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
struct MfgCockpitReportDeliveryRetryRequest {
    #[serde(default = "default_matrix_bridge_mode")]
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

#[derive(Debug, Clone, Serialize)]
struct MfgCockpitReportDeliveryOutcome {
    mode: String,
    status: String,
    dispatch_status: String,
    report: MfgCockpitReportSnapshot,
    delivery_payload: MfgCockpitReportDeliveryPayload,
    cross_plane_execution_receipt: CrossPlaneExecutionReceipt,
    idempotent_replay: bool,
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let health = store
        .health()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let capabilities = matrix_health_capabilities();
    Ok(Json(serde_json::json!({
        "kind": "mfg.health",
        "status": "ready",
        "schema_version": health.schema_version,
        "expected_schema_version": MATRIX_SCHEMA_VERSION,
        "fact_count": health.fact_count,
        "metric_definition_count": health.metric_definition_count,
        "metric_state_count": health.metric_state_count,
        "change_count": health.change_count,
        "attention_count": health.attention_count,
        "evidence_count": health.evidence_count,
        "incident_count": health.incident_count,
        "analysis_count": health.analysis_count,
        "execution_count": health.execution_count,
        "entity_count": health.entity_count,
        "relation_count": health.relation_count,
        "metric_dependency_count": health.metric_dependency_count,
        "compute_job_count": health.compute_job_count,
        "quality_gate_count": health.quality_gate_count,
        "cockpit_profile_count": health.cockpit_profile_count,
        "cockpit_report_count": health.cockpit_report_count,
        "memory_case_count": health.memory_case_count,
        "playbook_count": health.playbook_count,
        "source_pack_count": health.source_pack_count,
        "data_plane_watermark_count": health.data_plane_watermark_count,
        "connector_run_count": health.connector_run_count,
        "ontology_pack_count": health.ontology_pack_count,
        "entity_match_candidate_count": health.entity_match_candidate_count,
        "entity_conflict_decision_count": health.entity_conflict_decision_count,
        "metric_snapshot_count": health.metric_snapshot_count,
        "skill_execution_count": health.skill_execution_count,
        "store": matrix_store_path(&state.workspace_root),
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
        "server_manufacturing_skill_pack",
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let health = store
        .data_plane_health()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.data_plane.health",
        "health": health,
    })))
}

async fn matrix_data_plane_ingest_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixDataPlaneIngestPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let plan = store
        .plan_data_plane_ingest(request.ingest)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    append_matrix_execution_outcome(
        &state,
        session_id.as_deref(),
        CowdExecutionOutcome::from(&plan),
    )
    .await
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.data_plane.ingest_plan",
        "request_id": request.request_id,
        "session_id": session_id,
        "plan": plan,
    })))
}

async fn matrix_skills_handler() -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill_pack",
        "domain": "server_manufacturing",
        "items": server_manufacturing_skill_pack(),
    })))
}

async fn matrix_skill_get_handler(
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let skill = find_matrix_skill(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG skill not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill",
        "skill": skill,
    })))
}

async fn matrix_command_center_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let health = store
        .health()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let attention = store
        .list_attention(10)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let changes = store
        .list_changes(10)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let skills = server_manufacturing_skill_pack();
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
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incidents = store
        .list_incidents(12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let attention = store
        .list_attention(12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let action_queue = store
        .list_recent_action_executions(12)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let skill_queue = store
        .list_recent_skill_runs(12)
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
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let source_pack = store
        .list_source_packs(1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let fact = store
        .list_facts(1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let entity = store
        .list_entities(1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let metric = store
        .list_metric_definitions()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let attention = store
        .list_attention(1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let evidence = store
        .list_evidence_packets(1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let incident = if let Some(id) = query.incident_id.as_deref() {
        store
            .get_incident(id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        store
            .list_incidents(1)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .into_iter()
            .next()
    };
    let analysis = if let Some(incident) = incident.as_ref() {
        store
            .latest_analysis_for_incident(&incident.incident_id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    } else {
        None
    };
    let action = store
        .list_recent_action_executions(1)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .next();
    let report = if let Some(id) = query.report_id.as_deref() {
        store
            .get_cockpit_report(id)
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

async fn matrix_incidents_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incidents = store
        .list_incidents(50)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let source_pack = store
        .upsert_source_pack(request.source_pack)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let source_pack = store
        .get_source_pack(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let validation = store
        .validate_source_pack(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let delta_plan = store
        .source_pack_delta_plan(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    store
        .validate_source_pack(&id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    let mut attention = Vec::new();
    for input in request.facts {
        let fact = MatrixFact::from_input(input);
        let item = store
            .ingest_fact(&fact)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    let run = store
        .plan_connector_run(&id, input)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    let run = store
        .plan_connector_run(&id, input)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let run = store
        .get_connector_run(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG connector run not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.connector_run",
        "run": run,
    })))
}

async fn matrix_cockpit_profile_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgCockpitProfileUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let profile = store
        .upsert_cockpit_profile(&MfgCockpitProfile::from_input(request.profile))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.profile",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "profile": profile,
    })))
}

async fn matrix_cockpit_profile_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let profile = store
        .get_cockpit_profile(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit profile not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.profile",
        "profile": profile,
    })))
}

async fn matrix_cockpit_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let projection = store.cockpit_projection(&id).map_err(|error| match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.projection",
        "projection": projection,
    })))
}

async fn matrix_cockpit_report_generate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportGenerateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report =
        store
            .generate_cockpit_report(&id, request.report)
            .map_err(|error| match error {
                MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "report": report,
    })))
}

async fn matrix_cockpit_report_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report",
        "report": report,
    })))
}

async fn matrix_cockpit_report_deliver_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportDeliveryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let outcome = deliver_matrix_cockpit_report(&state, &store, report, request)?;
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

async fn matrix_cockpit_report_delivery_state_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG cockpit report not found"))?;
    let delivery_state = MfgCockpitReportDeliveryState::from_report(&report);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_state",
        "report_id": report.report_id,
        "delivery_state": delivery_state,
    })))
}

async fn matrix_cockpit_report_delivery_retry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgCockpitReportDeliveryRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
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
    let delivery_request = matrix_retry_delivery_request(&report, &before_state, request);
    let outcome = deliver_matrix_cockpit_report(&state, &store, report, delivery_request)?;
    let after_state = MfgCockpitReportDeliveryState::from_report(&outcome.report);
    Ok(Json(serde_json::json!({
        "kind": "mfg.cockpit.report_delivery_retry",
        "before_state": before_state,
        "after_state": after_state,
        "delivery": outcome,
    })))
}

async fn matrix_cockpit_report_schedule_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgCockpitReportScheduleRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let limit = request.limit.unwrap_or(50).clamp(1, 100);
    let profiles = store
        .list_cockpit_profiles(request.cadence.as_deref(), limit)
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
        let report = store
            .generate_cockpit_report(
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
                        .or_else(|| default_matrix_schedule_delivery_ref(&profile, &request)),
                    note: Some("scheduled cockpit report".to_string()),
                },
            )
            .map_err(|error| match error {
                MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;

        if request.deliver {
            let delivery_request =
                matrix_schedule_delivery_request(&profile, &report, &request, delivery_count);
            let outcome = deliver_matrix_cockpit_report(&state, &store, report, delivery_request)?;
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
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.domain_pack",
        "pack": server_manufacturing_domain_pack(),
    })))
}

async fn matrix_server_manufacturing_seed_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let result = store
        .seed_mfg_domain()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.domain_seed",
        "result": result,
    })))
}

async fn matrix_server_manufacturing_ontology_handler(
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "mfg.ontology_pack",
        "pack": server_manufacturing_ontology_pack(),
    })))
}

async fn matrix_server_manufacturing_ontology_seed_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let pack = store
        .seed_mfg_ontology()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.ontology_seed",
        "pack": pack,
    })))
}

async fn matrix_entities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entities = store
        .list_entities(100)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let candidate = store
        .propose_entity_match(&request.left_entity_id, &request.right_entity_id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let decision = store
        .decide_entity_conflict(
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entity = store
        .upsert_entity(&MatrixEntity::from_input(request.entity))
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entity = store
        .resolve_entity_by_source_key(&request.source_system, &request.source_key)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entity = store
        .get_entity(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let relation = store
        .upsert_relation(&MatrixRelation::from_input(request.relation))
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let relations = store
        .list_entity_relations(&id, 100)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let trace = store.impact_trace(&id, 3).map_err(|error| match error {
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut facts = Vec::with_capacity(request.facts.len());
    let mut attention = Vec::with_capacity(request.facts.len());
    for input in request.facts {
        let fact = MatrixFact::from_input(input);
        let item = store
            .ingest_fact(&fact)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        append_matrix_execution_outcome(
            &state,
            session_id.as_deref(),
            CowdExecutionOutcome::from(&fact),
        )
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let metrics = store
        .list_metric_definitions()
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let states = store
        .metric_states(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let lineage = store
        .metric_lineage(&id, 6)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let plan = store
        .plan_metric_attention(
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let snapshot = store
        .materialize_metric_snapshot(request.metric_ids, request.scope_ref)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let dependency = store
        .upsert_metric_dependency(&MatrixMetricDependency::from_input(request.dependency))
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let metric_ids = store
        .metrics_affected_by_fact_type(&request.fact_type)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let plan = store
        .plan_compute_job_for_fact_type(request.job)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let job = store
        .get_compute_job(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let job = store.run_compute_job(&id).map_err(|error| match error {
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let result = store
        .recompute_metrics()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.metrics.recompute",
        "result": result,
    })))
}

async fn matrix_changes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let changes = store
        .list_changes(100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.changes",
        "changes": changes,
    })))
}

async fn matrix_attention_hot_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let items = store
        .list_attention(50)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = store
        .build_evidence_packet(
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
        CowdExecutionOutcome::from(&packet),
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = store
        .get_evidence_packet(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let gate = store
        .evaluate_evidence_quality(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let gate = store
        .get_quality_gate(&id)
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
    let store = open_matrix_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = store
        .get_evidence_packet(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.evidence.context_item",
        "context_item": packet.to_context_item(),
    })))
}

async fn matrix_incident_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgIncidentCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let packet = match request.evidence_packet_id.as_deref() {
        Some(packet_id) => store
            .get_evidence_packet(packet_id)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
            .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG evidence packet not found"))?,
        None => store
            .build_evidence_packet(request.attention_id.as_deref(), request.title.as_deref())
            .map_err(|error| match error {
                MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?,
    };
    let title = request
        .title
        .clone()
        .unwrap_or_else(|| packet.problem_statement.clone());
    let task = state
        .task_kernel
        .start_goal(format!("MFG incident analysis: {title}"), false)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let mut graph = task
        .agent_graph
        .clone()
        .unwrap_or_else(|| AgentRunGraph::from_objective(task.id.clone(), task.objective.clone()));
    enrich_matrix_agent_graph(&mut graph, &packet)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?;
    let task = state
        .task_kernel
        .upsert_agent_graph(&task.id, graph.clone())
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    append_matrix_agent_runtime_event(&state, &task, &graph)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;

    let mut incident = MfgIncident::new(title);
    incident.attention_id = packet.attention_id.clone();
    incident.evidence_packet_id = Some(packet.packet_id.clone());
    incident.task_id = Some(task.id.clone());
    incident.agent_graph_id = Some(graph.graph_id.clone());
    let incident = store
        .create_incident(&incident)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident": incident,
        "task": task,
        "agent_graph": graph,
        "context_item": packet.to_context_item(),
    })))
}

async fn matrix_incident_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.incident",
        "incident": incident,
    })))
}

async fn matrix_incident_room_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let evidence_packet = incident
        .evidence_packet_id
        .as_deref()
        .and_then(|packet_id| store.get_evidence_packet(packet_id).ok().flatten());
    let quality_gate = evidence_packet
        .as_ref()
        .and_then(|packet| store.evaluate_evidence_quality(&packet.packet_id).ok());
    let analysis = store
        .latest_analysis_for_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let executions = store
        .list_executions_for_incident(&id, 20)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let memory_cases = store.search_memory_cases(Some(&id), 10).unwrap_or_default();
    let playbooks = store
        .recommend_playbooks_for_incident(&id, 5)
        .unwrap_or_default();
    let agent_graph = incident
        .task_id
        .as_deref()
        .and_then(|task_id| state.task_kernel.agent_graph(task_id));
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

async fn matrix_incident_analyze_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let analysis = store.analyze_incident(&id).map_err(|error| match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.operational_analysis",
        "analysis": analysis,
    })))
}

async fn matrix_incident_case_promote_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let promotion = store
        .promote_incident_to_memory_case(&id)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.memory_case.promotion",
        "memory_case": promotion.memory_case,
        "playbook": promotion.playbook,
    })))
}

async fn matrix_memory_case_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let memory_case = store
        .get_memory_case(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG memory case not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.memory_case",
        "memory_case": memory_case,
    })))
}

async fn matrix_memory_case_search_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<MfgCaseSearchQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let cases = store
        .search_memory_cases(query.q.as_deref(), query.limit.unwrap_or(20).min(100))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.memory_case.search",
        "items": cases,
    })))
}

async fn matrix_playbook_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgPlaybookUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let playbook = store
        .upsert_playbook(&request.playbook)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.playbook",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "playbook": playbook,
    })))
}

async fn matrix_playbook_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let playbook = store
        .get_playbook(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG playbook not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.playbook",
        "playbook": playbook,
    })))
}

async fn matrix_incident_playbook_recommend_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgPlaybookRecommendRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let playbooks = store
        .recommend_playbooks_for_incident(&id, request.limit.unwrap_or(5).min(20))
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
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

async fn matrix_incident_skill_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgSkillPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let analysis = store.analyze_incident(&id).ok();
    let packet = incident
        .evidence_packet_id
        .as_deref()
        .and_then(|packet_id| store.get_evidence_packet(packet_id).ok().flatten());
    let plan = plan_server_manufacturing_skills(
        &incident,
        analysis.as_ref(),
        packet.as_ref(),
        request.limit.unwrap_or(3).clamp(1, 8),
    );
    let graph = plan_matrix_skill_agent_nodes(&state, &incident, &plan)
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

async fn matrix_incident_skill_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((id, skill_id)): AxumPath<(String, String)>,
    Json(request): Json<MfgSkillRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let analysis = store.analyze_incident(&id).ok();
    let packet = incident
        .evidence_packet_id
        .as_deref()
        .and_then(|packet_id| store.get_evidence_packet(packet_id).ok().flatten());
    let skill = find_matrix_skill(&skill_id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG skill not found"))?;
    let run = run_server_manufacturing_skill(&incident, &skill, analysis.as_ref(), packet.as_ref());
    let run = store
        .record_skill_run(&run)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let graph = complete_matrix_skill_agent_node(&state, &incident, &run)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    append_matrix_execution_outcome(
        &state,
        session_id.as_deref().or(incident.task_id.as_deref()),
        CowdExecutionOutcome::from(&run),
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

async fn matrix_incident_skill_runs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG incident not found"))?;
    let runs = store
        .list_skill_runs_for_incident(&incident.incident_id, 24)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.skill.run_list",
        "incident_id": incident.incident_id,
        "items": runs,
    })))
}

async fn matrix_skill_run_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let run = store
        .get_skill_run(&id)
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
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let analysis = store
        .get_analysis(&id)
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
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .execute_recommended_action(&analysis_id, &action_id, &request)
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    let incident = store
        .get_incident(&execution.incident_id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    append_matrix_execution_outcome(
        &state,
        incident
            .as_ref()
            .and_then(|incident| incident.task_id.as_deref())
            .or(Some(execution.incident_id.as_str())),
        CowdExecutionOutcome::from(&execution),
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
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .get_execution(&id)
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
    super::cross_plane_routes::ensure_cross_plane_loaded(&state);
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .get_execution(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG action execution not found"))?;
    let mode = normalize_matrix_bridge_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);

    if let Some(key) = &idempotency_key {
        if let Some(receipt) =
            super::cross_plane_routes::cross_plane_control().find_execution_by_idempotency_key(key)
        {
            let execution = attach_matrix_cross_plane_receipt(&store, &execution, &receipt)
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

    let action = matrix_cross_plane_action_from_execution(&execution, &request);
    let now = chrono::Utc::now();
    let (action, decision, evidence) =
        super::cross_plane_routes::decide_connector_action(&state, action, &mode, now);
    let (status, dispatch_status, blockers, audit_result, audit_summary) =
        matrix_cross_plane_bridge_outcome(&mode, &decision);
    let audit_record = CrossPlaneAuditRecord::new(
        action.clone(),
        decision.clone(),
        audit_result,
        audit_summary,
    )
    .with_evidence(evidence);
    let audit_record_id = audit_record.id.clone();
    super::cross_plane_routes::cross_plane_control().record_audit(audit_record);
    let receipt = CrossPlaneExecutionReceipt::new(
        idempotency_key,
        mode.clone(),
        status.clone(),
        dispatch_status.clone(),
        action,
        decision,
        blockers,
        Some(audit_record_id),
    );
    super::cross_plane_routes::cross_plane_control().record_execution(receipt.clone());
    super::cross_plane_routes::save_cross_plane_state(&state);
    let execution = attach_matrix_cross_plane_receipt(&store, &execution, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.cross_plane_action_bridge",
        "mode": mode,
        "status": status,
        "dispatch_status": dispatch_status,
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
    let store = open_mfg_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .record_execution_feedback(
            &id,
            MfgActionFeedback::new(request.outcome, request.note, request.metric_delta),
        )
        .map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.action_execution",
        "execution": execution,
    })))
}

fn attach_matrix_cross_plane_receipt(
    store: &MfgStore,
    execution: &MfgActionExecution,
    receipt: &CrossPlaneExecutionReceipt,
) -> Result<MfgActionExecution, MatrixStoreError> {
    store.attach_cross_plane_receipt(
        &execution.execution_id,
        MfgCrossPlaneBridgeReceipt::new(
            execution.execution_id.clone(),
            receipt.id.clone(),
            receipt.status.clone(),
            receipt.dispatch_status.clone(),
            receipt.audit_record_id.clone(),
        ),
    )
}

fn deliver_matrix_cockpit_report(
    state: &AppState,
    store: &MfgStore,
    report: MfgCockpitReportSnapshot,
    request: MfgCockpitReportDeliveryRequest,
) -> Result<MfgCockpitReportDeliveryOutcome, (StatusCode, Json<ErrorResponse>)> {
    super::cross_plane_routes::ensure_cross_plane_loaded(state);
    let mode = normalize_matrix_bridge_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    let delivery_payload = matrix_report_delivery_payload(&report, &request);

    if let Some(key) = &idempotency_key {
        if let Some(receipt) =
            super::cross_plane_routes::cross_plane_control().find_execution_by_idempotency_key(key)
        {
            if !matrix_report_delivery_receipt_matches(&receipt, &report) {
                return Err(api_error(
                    StatusCode::CONFLICT,
                    "MFG cockpit report delivery idempotency key belongs to another cross-plane action",
                ));
            }
            let report = attach_matrix_report_delivery_receipt(store, &report, &receipt)
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

    let action = matrix_report_delivery_action(&report, &request, &delivery_payload);
    let now = chrono::Utc::now();
    let (action, decision, evidence) =
        super::cross_plane_routes::decide_connector_action(state, action, &mode, now);
    let (status, dispatch_status, blockers, audit_result, audit_summary) =
        matrix_cross_plane_bridge_outcome(&mode, &decision);
    let audit_record = CrossPlaneAuditRecord::new(
        action.clone(),
        decision.clone(),
        audit_result,
        audit_summary,
    )
    .with_evidence(evidence);
    let audit_record_id = audit_record.id.clone();
    super::cross_plane_routes::cross_plane_control().record_audit(audit_record);
    let receipt = CrossPlaneExecutionReceipt::new(
        idempotency_key,
        mode.clone(),
        status.clone(),
        dispatch_status.clone(),
        action,
        decision,
        blockers,
        Some(audit_record_id),
    );
    super::cross_plane_routes::cross_plane_control().record_execution(receipt.clone());
    super::cross_plane_routes::save_cross_plane_state(state);
    let report = attach_matrix_report_delivery_receipt(store, &report, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(MfgCockpitReportDeliveryOutcome {
        mode,
        status,
        dispatch_status,
        report,
        delivery_payload,
        cross_plane_execution_receipt: receipt,
        idempotent_replay: false,
    })
}

fn attach_matrix_report_delivery_receipt(
    store: &MfgStore,
    report: &MfgCockpitReportSnapshot,
    receipt: &CrossPlaneExecutionReceipt,
) -> Result<MfgCockpitReportSnapshot, MatrixStoreError> {
    store.attach_cockpit_report_delivery(
        &report.report_id,
        MfgCockpitReportDeliveryReceipt::new(
            report.report_id.clone(),
            receipt.id.clone(),
            receipt.status.clone(),
            receipt.dispatch_status.clone(),
            receipt.audit_record_id.clone(),
        ),
    )
}

fn default_matrix_schedule_delivery_ref(
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

fn matrix_schedule_delivery_request(
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

fn matrix_retry_delivery_request(
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

fn matrix_report_delivery_receipt_matches(
    receipt: &CrossPlaneExecutionReceipt,
    report: &MfgCockpitReportSnapshot,
) -> bool {
    receipt.action.session_id.as_deref() == Some(report.report_id.as_str())
}

fn find_matrix_skill(skill_id: &str) -> Option<MfgSkillManifest> {
    server_manufacturing_skill_pack()
        .into_iter()
        .find(|skill| skill.skill_id == skill_id)
}

async fn plan_matrix_skill_agent_nodes(
    state: &AppState,
    incident: &MfgIncident,
    plan: &MfgSkillPlan,
) -> Result<Option<AgentRunGraph>, String> {
    let Some(task_id) = incident.task_id.as_deref() else {
        return Ok(None);
    };
    let Some(mut graph) = state.task_kernel.agent_graph(task_id) else {
        return Ok(None);
    };
    let now = now_ms();
    let dependency = if graph.nodes.iter().any(|node| node.id == "matrix_reviewer") {
        "matrix_reviewer"
    } else {
        "planner"
    };
    for skill in &plan.selected_skills {
        let node_id = skill_agent_node_id(&skill.skill_id);
        ensure_agent_node(
            &mut graph,
            AgentTaskNode {
                id: node_id.clone(),
                role: AgentRole::Researcher,
                title: skill.role.clone(),
                objective: skill.analysis_method.clone(),
                depends_on: vec![dependency.to_string()],
                status: AgentNodeStatus::Pending,
                assigned_agent: Some(skill.skill_id.clone()),
                result: None,
                error: None,
                created_at_ms: now,
                updated_at_ms: now,
            },
        )
        .map_err(|error| error.to_string())?;
        graph
            .add_evidence(
                &node_id,
                "matrix_skill_manifest",
                format!("mfg:skill:{}", skill.skill_id),
                format!(
                    "inputs={}, metrics={}, evidence={}",
                    skill.input_fact_types.join(","),
                    skill.input_metric_keys.join(","),
                    skill.required_evidence.join(",")
                ),
            )
            .map_err(|error| error.to_string())?;
    }
    let task = state
        .task_kernel
        .upsert_agent_graph(task_id, graph.clone())?;
    append_matrix_agent_runtime_event(state, &task, &graph).await?;
    Ok(Some(graph))
}

async fn complete_matrix_skill_agent_node(
    state: &AppState,
    incident: &MfgIncident,
    run: &MfgSkillRun,
) -> Result<Option<AgentRunGraph>, String> {
    let Some(task_id) = incident.task_id.as_deref() else {
        return Ok(None);
    };
    let Some(mut graph) = state.task_kernel.agent_graph(task_id) else {
        return Ok(None);
    };
    let node_id = run
        .agent_node_id
        .clone()
        .unwrap_or_else(|| skill_agent_node_id(&run.skill_id));
    if !graph.nodes.iter().any(|node| node.id == node_id) {
        let skill = find_matrix_skill(&run.skill_id)
            .ok_or_else(|| format!("MFG skill {} not found", run.skill_id))?;
        let now = now_ms();
        ensure_agent_node(
            &mut graph,
            AgentTaskNode {
                id: node_id.clone(),
                role: AgentRole::Researcher,
                title: skill.role,
                objective: skill.analysis_method,
                depends_on: vec!["planner".to_string()],
                status: AgentNodeStatus::Pending,
                assigned_agent: Some(run.skill_id.clone()),
                result: None,
                error: None,
                created_at_ms: now,
                updated_at_ms: now,
            },
        )
        .map_err(|error| error.to_string())?;
    }
    if let Some(node) = graph.nodes.iter_mut().find(|node| node.id == node_id) {
        node.status = AgentNodeStatus::Completed;
        node.result = Some(run.summary.clone());
        node.updated_at_ms = now_ms();
    }
    graph
        .add_evidence(
            &node_id,
            "matrix_skill_run",
            format!("mfg:skill-run:{}:{}", incident.incident_id, run.skill_id),
            run.summary.clone(),
        )
        .map_err(|error| error.to_string())?;
    let task = state
        .task_kernel
        .upsert_agent_graph(task_id, graph.clone())?;
    append_matrix_agent_runtime_event(state, &task, &graph).await?;
    Ok(Some(graph))
}

fn matrix_report_delivery_payload(
    report: &MfgCockpitReportSnapshot,
    request: &MfgCockpitReportDeliveryRequest,
) -> MfgCockpitReportDeliveryPayload {
    MfgCockpitReportDeliveryPayload::from_report(
        report,
        MfgCockpitReportDeliveryPayloadRequest {
            channel: request.channel.clone(),
            template_id: request.template_id.clone(),
            target_ref: request
                .target_ref
                .clone()
                .or_else(|| report.delivery_ref.clone()),
            requested_capability: request.requested_capability.clone(),
        },
    )
}

fn matrix_report_delivery_action(
    report: &MfgCockpitReportSnapshot,
    request: &MfgCockpitReportDeliveryRequest,
    delivery_payload: &MfgCockpitReportDeliveryPayload,
) -> CrossPlaneAction {
    let actor_principal = request
        .actor_principal
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(report.owner_ref.as_str());
    let requested_capability = request
        .requested_capability
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(delivery_payload.requested_capability.as_str());
    let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
    action.actor_identity_ref = request.actor_identity_ref.clone();
    action.source_channel = Some(
        request
            .source_channel
            .clone()
            .unwrap_or_else(|| "mfg.report".to_string()),
    );
    action.session_id = Some(report.report_id.clone());
    action.provider_account = request.provider_account.clone();
    action.target_ref = request
        .target_ref
        .clone()
        .or_else(|| delivery_payload.target_ref.clone())
        .or_else(|| report.delivery_ref.clone());
    action.resource_ref = request
        .resource_ref
        .clone()
        .or_else(|| Some(delivery_payload.resource_ref.clone()));
    action.risk = CrossPlaneRisk::Low;
    action.data_classification = DataClassification::Internal;
    action.identity_trust = IdentityTrust::Unknown;
    action
}

fn matrix_cross_plane_action_from_execution(
    execution: &MfgActionExecution,
    request: &MfgCrossPlaneBridgeRequest,
) -> CrossPlaneAction {
    let actor_principal = request
        .actor_principal
        .as_deref()
        .or(execution.operator_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("mfg:operator");
    let requested_capability = request
        .requested_capability
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_matrix_cross_plane_capability(execution));
    let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
    action.actor_identity_ref = request.actor_identity_ref.clone();
    action.source_channel = Some(
        request
            .source_channel
            .clone()
            .unwrap_or_else(|| "mfg".to_string()),
    );
    action.session_id = Some(execution.incident_id.clone());
    action.provider_account = request.provider_account.clone();
    action.target_ref = request.target_ref.clone();
    action.resource_ref = request.resource_ref.clone().or_else(|| {
        Some(format!(
            "text://{}",
            default_matrix_bridge_message(execution)
        ))
    });
    action.risk = matrix_cross_plane_risk(execution);
    action.data_classification = DataClassification::Internal;
    action.identity_trust = IdentityTrust::Unknown;
    action
}

fn default_matrix_cross_plane_capability(execution: &MfgActionExecution) -> &'static str {
    match execution.action_type.as_str() {
        "supplier_recovery" | "plan_bom_reconciliation" | "evidence_review" => {
            "channel.feishu.send_text"
        }
        _ => "channel.feishu.send_text",
    }
}

fn default_matrix_bridge_message(execution: &MfgActionExecution) -> String {
    format!(
        "MFG action {} [{}]: {}; incident={}; execution={}",
        execution.action_type,
        execution.owner_role,
        execution.title,
        execution.incident_id,
        execution.execution_id
    )
}

fn matrix_cross_plane_risk(execution: &MfgActionExecution) -> CrossPlaneRisk {
    if execution.governance.contains("human_review") || execution.mode == "commit" {
        CrossPlaneRisk::Medium
    } else {
        CrossPlaneRisk::Low
    }
}

fn normalize_matrix_bridge_mode(mode: &str) -> String {
    match mode.trim().to_ascii_lowercase().as_str() {
        "commit" | "live" | "execute" => "commit".to_string(),
        _ => "dry_run".to_string(),
    }
}

fn matrix_cross_plane_bridge_outcome(
    mode: &str,
    decision: &runtime::CrossPlanePolicyDecision,
) -> (String, String, Vec<String>, String, String) {
    if decision.decision == PolicyDecisionKind::Allow {
        if mode == "dry_run" {
            return (
                "planned".to_string(),
                "dry_run".to_string(),
                Vec::new(),
                "dry_run".to_string(),
                "matrix_cross_plane_bridge_dry_run_plan".to_string(),
            );
        }
        return (
            "planned".to_string(),
            "human_review_required".to_string(),
            vec!["mfg:human_review_required".to_string()],
            "planned".to_string(),
            "matrix_cross_plane_bridge_queued_for_human_review".to_string(),
        );
    }
    (
        "blocked".to_string(),
        "policy_blocked".to_string(),
        vec![format!("policy:{}", decision.reason)],
        "blocked".to_string(),
        "matrix_cross_plane_bridge_policy_blocked".to_string(),
    )
}

fn enrich_matrix_agent_graph(
    graph: &mut AgentRunGraph,
    packet: &runtime::MatrixEvidencePacket,
) -> Result<(), runtime::AgentGraphError> {
    let now = now_ms();
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "matrix_researcher".to_string(),
            role: AgentRole::Researcher,
            title: "MFG Evidence Research".to_string(),
            objective: "Validate MFG evidence packet and identify missing evidence".to_string(),
            depends_on: vec!["planner".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("matrix_researcher".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "matrix_reviewer".to_string(),
            role: AgentRole::Reviewer,
            title: "MFG Insight Review".to_string(),
            objective: "Review confidence, conflicts, and governance readiness".to_string(),
            depends_on: vec!["matrix_researcher".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("matrix_reviewer".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "matrix_merger".to_string(),
            role: AgentRole::Merger,
            title: "MFG Decision Merge".to_string(),
            objective: "Merge agent findings into one governed operating decision".to_string(),
            depends_on: vec!["matrix_reviewer".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("matrix_merger".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    let reference = format!("mfg:evidence:{}", packet.packet_id);
    graph.add_evidence(
        "planner",
        "matrix_evidence_packet",
        reference.clone(),
        packet.problem_statement.clone(),
    )?;
    graph.add_evidence(
        "matrix_researcher",
        "matrix_evidence_packet",
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

async fn append_matrix_agent_runtime_event(
    state: &AppState,
    task: &TaskRecord,
    graph: &AgentRunGraph,
) -> Result<(), String> {
    ensure_matrix_task_session_record(state, task)
        .await
        .map_err(|error| format!("failed to prepare MFG task runtime session: {error}"))?;
    state
        .services
        .session
        .append_runtime_event(
            &task.id,
            memory::RuntimeEventScope::Workgraph,
            "mfg.agent_graph.updated",
            serde_json::json!({ "graph": graph }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn append_matrix_execution_outcome(
    state: &AppState,
    session_id: Option<&str>,
    outcome: CowdExecutionOutcome,
) -> Result<(), String> {
    let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    let Some(store) = state.unified_store() else {
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

async fn ensure_matrix_outcome_session_record(
    state: &AppState,
    session_id: &str,
) -> Result<(), String> {
    let Some(store) = state.unified_store() else {
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

async fn ensure_matrix_task_session_record(
    state: &AppState,
    task: &TaskRecord,
) -> Result<(), String> {
    let Some(store) = state.unified_store() else {
        return Ok(());
    };
    let now = chrono::Utc::now().to_rfc3339();
    let metadata_json = serde_json::json!({
        "kind": "mfg.incident.task",
        "task_id": task.id,
        "objective": task.objective,
        "yolo_mode": task.yolo_mode,
        "current_phase": task.current_phase,
    })
    .to_string();
    let mut record = SessionRecord {
        session_id: task.id.clone(),
        platform: "mfg".to_string(),
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

fn open_matrix_store(state: &AppState) -> Result<MatrixStore, MatrixStoreError> {
    let path = matrix_store_path(&state.workspace_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            MatrixStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?;
    }
    MatrixStore::open(path)
}

fn open_mfg_store(state: &AppState) -> Result<MfgStore, MatrixStoreError> {
    let path = matrix_store_path(&state.workspace_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            MatrixStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?;
    }
    MfgStore::open(path)
}

pub(super) fn matrix_store_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(".cowd").join("matrix.sqlite")
}
