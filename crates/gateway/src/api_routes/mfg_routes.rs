use std::sync::Arc;

use app_mfg::{
    mfg_widget_catalog, MfgActionExecutionRequest, MfgActionFeedback, MfgAlertCommand,
    MfgAlertCommandInput, MfgAlertRule, MfgAlertRuleInput, MfgAlertSubscription,
    MfgAlertSubscriptionInput, MfgAssignment, MfgAssignmentCommand, MfgAssignmentCommandInput,
    MfgAssignmentInput, MfgCockpitProfile, MfgCockpitProfileInput, MfgCockpitReportDeliveryState,
    MfgCockpitReportRequest, MfgCockpitReportSnapshot, MfgIncident, MfgPlaybook,
    MfgRepositoryError,
};
use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use matrix_core::{
    MatrixComputeJobInput, MatrixConnectorRunInput, MatrixDataPlaneIngestPlanInput, MatrixEntity,
    MatrixEntityInput, MatrixFact, MatrixFactInput, MatrixMetricDependency,
    MatrixMetricDependencyInput, MatrixRelation, MatrixRelationInput, MatrixSourcePack,
    MATRIX_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};

use crate::services::{
    GatewayMatrixRepositoryError as MatrixStoreError, MfgCockpitReportDeliveryOutcome,
    MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
};

use super::matrix_outcomes::{
    append_matrix_execution_outcome, matrix_evidence_packet_outcome, matrix_fact_outcome,
    matrix_ingest_plan_outcome,
};
use super::mfg_outcomes::{
    append_mfg_execution_outcome, mfg_action_execution_outcome, mfg_skill_run_execution_outcome,
};
mod cockpit;
mod decision;
mod incidents;
mod operations;
use super::{api_error, AppState, ErrorResponse};
use cockpit::*;
use decision::*;
use incidents::*;
use operations::*;

/// Public MFG bridge intent. Gateway authentication owns the effective actor;
/// an actor field in an HTTP body is rejected by this closed schema.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgCrossPlaneBridgeIntent {
    #[serde(default = "default_mfg_bridge_mode")]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
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

impl MfgCrossPlaneBridgeIntent {
    pub(super) fn into_request(self, actor_principal: String) -> MfgCrossPlaneBridgeRequest {
        MfgCrossPlaneBridgeRequest {
            mode: self.mode,
            idempotency_key: self.idempotency_key,
            actor_principal,
            actor_identity_ref: self.actor_identity_ref,
            source_channel: self.source_channel,
            requested_capability: self.requested_capability,
            provider_account: self.provider_account,
            target_ref: self.target_ref,
            resource_ref: self.resource_ref,
        }
    }
}

/// Public MFG delivery intent. It cannot deserialize service-owned actor data.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgCockpitReportDeliveryIntent {
    #[serde(default = "default_mfg_bridge_mode")]
    mode: String,
    #[serde(default)]
    idempotency_key: Option<String>,
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

impl MfgCockpitReportDeliveryIntent {
    pub(super) fn into_request(self, actor_principal: String) -> MfgCockpitReportDeliveryRequest {
        MfgCockpitReportDeliveryRequest {
            mode: self.mode,
            idempotency_key: self.idempotency_key,
            actor_principal,
            actor_identity_ref: self.actor_identity_ref,
            source_channel: self.source_channel,
            requested_capability: self.requested_capability,
            provider_account: self.provider_account,
            target_ref: self.target_ref,
            resource_ref: self.resource_ref,
            channel: self.channel,
            template_id: self.template_id,
        }
    }
}

fn default_mfg_bridge_mode() -> String {
    "dry_run".to_string()
}

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/apps/mfg/app", get(mfg_app_handler))
        .route(
            "/api/apps/mfg/production/governance",
            get(mfg_production_governance_handler),
        )
        .route(
            "/api/apps/mfg/reality/health",
            get(mfg_reality_health_handler),
        )
        .route(
            "/api/apps/mfg/reality/data-plane/health",
            get(mfg_reality_data_plane_health_handler),
        )
        .route(
            "/api/apps/mfg/reality/data-plane/ingest-plan",
            post(mfg_reality_data_plane_ingest_plan_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/upsert",
            post(mfg_reality_source_pack_upsert_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/:id",
            get(mfg_reality_source_pack_get_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/:id/validate",
            post(mfg_reality_source_pack_validate_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/:id/ingest-file",
            post(mfg_reality_source_pack_ingest_file_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/:id/delta-plan",
            post(mfg_reality_source_pack_delta_plan_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/:id/connector-runs/plan",
            post(mfg_reality_source_pack_connector_run_plan_handler),
        )
        .route(
            "/api/apps/mfg/reality/source-packs/:id/connector-runs/run",
            post(mfg_reality_source_pack_connector_run_execute_handler),
        )
        .route(
            "/api/apps/mfg/reality/connector-runs/:id",
            get(mfg_reality_connector_run_get_handler),
        )
        .route(
            "/api/apps/mfg/reality/metrics",
            get(mfg_reality_metrics_handler),
        )
        .route(
            "/api/apps/mfg/reality/metrics/attention-plan",
            post(mfg_reality_metric_attention_plan_handler),
        )
        .route(
            "/api/apps/mfg/reality/metrics/snapshots/materialize",
            post(mfg_reality_metric_snapshot_materialize_handler),
        )
        .route(
            "/api/apps/mfg/reality/metrics/recompute",
            post(mfg_reality_metric_recompute_handler),
        )
        .route(
            "/api/apps/mfg/reality/metrics/:id",
            get(mfg_reality_metric_detail_handler),
        )
        .route(
            "/api/apps/mfg/reality/metrics/:id/lineage",
            get(mfg_reality_metric_lineage_handler),
        )
        .route(
            "/api/apps/mfg/reality/metric-dependencies/upsert",
            post(mfg_reality_metric_dependency_upsert_handler),
        )
        .route(
            "/api/apps/mfg/reality/metric-dependencies/affected-by-fact-type",
            post(mfg_reality_metric_affected_by_fact_type_handler),
        )
        .route(
            "/api/apps/mfg/reality/compute/jobs/plan",
            post(mfg_reality_compute_job_plan_handler),
        )
        .route(
            "/api/apps/mfg/reality/compute/jobs/:id",
            get(mfg_reality_compute_job_get_handler),
        )
        .route(
            "/api/apps/mfg/reality/compute/jobs/:id/run",
            post(mfg_reality_compute_job_run_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities",
            get(mfg_reality_entities_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/upsert",
            post(mfg_reality_entity_upsert_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/resolve-source-key",
            post(mfg_reality_entity_resolve_source_key_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/match-candidate",
            post(mfg_reality_entity_match_candidate_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/conflict-decision",
            post(mfg_reality_entity_conflict_decision_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/:id",
            get(mfg_reality_entity_get_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/:id/relations",
            get(mfg_reality_entity_relations_handler),
        )
        .route(
            "/api/apps/mfg/reality/entities/:id/impact-path",
            get(mfg_reality_entity_impact_path_handler),
        )
        .route(
            "/api/apps/mfg/reality/relations/upsert",
            post(mfg_reality_relation_upsert_handler),
        )
        .route(
            "/api/apps/mfg/reality/facts/ingest",
            post(mfg_reality_fact_ingest_handler),
        )
        .route(
            "/api/apps/mfg/reality/changes",
            get(mfg_reality_changes_handler),
        )
        .route(
            "/api/apps/mfg/reality/attention/hot",
            get(mfg_reality_attention_hot_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/build",
            post(mfg_reality_evidence_build_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/:id",
            get(mfg_reality_evidence_get_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/:id/quality-gate",
            post(mfg_reality_evidence_quality_gate_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/:id/context",
            get(mfg_reality_evidence_context_handler),
        )
        .route(
            "/api/apps/mfg/reality/quality-gates/:id",
            get(mfg_reality_quality_gate_get_handler),
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
            "/api/apps/mfg/cockpit/profiles",
            get(mfg_cockpit_profile_list_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/upsert",
            post(mfg_cockpit_profile_upsert_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id",
            get(mfg_cockpit_profile_get_handler).delete(mfg_cockpit_profile_delete_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id/clone",
            post(mfg_cockpit_profile_clone_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/profiles/:id/share",
            post(mfg_cockpit_profile_share_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/widget-catalog",
            get(mfg_cockpit_widget_catalog_handler),
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
        .route(
            "/api/apps/mfg/focus/alert-rules",
            get(mfg_alert_rule_list_handler).post(mfg_alert_rule_upsert_handler),
        )
        .route(
            "/api/apps/mfg/focus/alerts",
            get(mfg_alert_occurrence_list_handler),
        )
        .route(
            "/api/apps/mfg/focus/alert-subscriptions",
            get(mfg_alert_subscription_list_handler).post(mfg_alert_subscription_upsert_handler),
        )
        .route(
            "/api/apps/mfg/focus/alerts/:id/command",
            post(mfg_alert_command_handler),
        )
        .route(
            "/api/apps/mfg/focus/forecasts",
            get(mfg_forecast_list_handler),
        )
        .route(
            "/api/apps/mfg/assignments",
            get(mfg_assignment_list_handler).post(mfg_assignment_upsert_handler),
        )
        .route(
            "/api/apps/mfg/assignments/:id",
            get(mfg_assignment_get_handler),
        )
        .route(
            "/api/apps/mfg/assignments/:id/command",
            post(mfg_assignment_command_handler),
        )
        .route("/api/apps/mfg/live", get(mfg_live_projection_handler))
}

fn mfg_mutation_error(error: MfgRepositoryError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        MfgRepositoryError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        conflict @ (MfgRepositoryError::RevisionConflict { .. }
        | MfgRepositoryError::CommandRejected(_)) => {
            api_error(StatusCode::CONFLICT, conflict.to_string())
        }
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
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
struct MfgCockpitProfileListQuery {
    #[serde(default)]
    cadence: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct MfgCockpitProfileDeleteQuery {
    expected_revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MfgCockpitProfileCloneRequest {
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MfgCockpitProfileShareRequest {
    expected_revision: u64,
    sharing_policy: app_mfg::MfgDashboardSharingPolicy,
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

#[derive(Debug, Deserialize)]
struct MfgRealityFactIngestRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
}

#[derive(Debug, Deserialize)]
struct MfgRealityEntityUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    entity: MatrixEntityInput,
}

#[derive(Debug, Deserialize)]
struct MfgRealityEntityResolveSourceKeyRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    source_system: String,
    source_key: String,
}

#[derive(Debug, Deserialize)]
struct MfgRealityEntityMatchCandidateRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    left_entity_id: String,
    right_entity_id: String,
}

#[derive(Debug, Deserialize)]
struct MfgRealityEntityConflictDecisionRequest {
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
struct MfgRealityRelationUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    relation: MatrixRelationInput,
}

#[derive(Debug, Deserialize)]
struct MfgRealityMetricDependencyUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    dependency: MatrixMetricDependencyInput,
}

#[derive(Debug, Deserialize)]
struct MfgRealityMetricAttentionPlanRequest {
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
struct MfgRealityMetricSnapshotMaterializeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    metric_ids: Vec<String>,
    #[serde(default)]
    scope_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MfgRealityAffectedByFactTypeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    fact_type: String,
}

#[derive(Debug, Deserialize)]
struct MfgRealityComputeJobPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    job: MatrixComputeJobInput,
}

#[derive(Debug, Deserialize)]
struct MfgRealityDataPlaneIngestPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    ingest: MatrixDataPlaneIngestPlanInput,
}

#[derive(Debug, Deserialize)]
struct MfgRealityEvidenceBuildRequest {
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
struct MfgRealitySourcePackUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    source_pack: MatrixSourcePack,
}

#[derive(Debug, Deserialize)]
struct MfgRealitySourcePackIngestFileRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
}

#[derive(Debug, Deserialize)]
struct MfgRealityConnectorRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    run: Option<MatrixConnectorRunInput>,
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

async fn mfg_reality_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = state
        .services
        .matrix
        .repository_health(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let store_path = state
        .services
        .matrix
        .store_path(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.health",
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
        "capabilities": mfg_application_capabilities(),
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_data_plane_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = state
        .services
        .matrix
        .data_plane_health(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.data_plane.health",
        "health": health,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_data_plane_ingest_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityDataPlaneIngestPlanRequest>,
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
        "kind": "mfg.reality.data_plane.ingest_plan",
        "request_id": request.request_id,
        "session_id": session_id,
        "plan": plan,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealitySourcePackUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = state
        .services
        .matrix
        .upsert_source_pack(&state.config_home, request.source_pack)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.source_pack",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack": source_pack,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = state
        .services
        .matrix
        .get_source_pack(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG Reality source pack not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.source_pack",
        "source_pack": source_pack,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_validate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let validation = state
        .services
        .matrix
        .validate_source_pack(&state.config_home, &id)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.source_pack.validation",
        "validation": validation,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_delta_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let delta_plan = state
        .services
        .matrix
        .source_pack_delta_plan(&state.config_home, &id)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.source_pack.delta_plan",
        "delta_plan": delta_plan,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_ingest_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgRealitySourcePackIngestFileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .matrix
        .validate_source_pack(&state.config_home, &id)
        .map_err(matrix_error)?;
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
        "kind": "mfg.reality.source_pack.ingest_file",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack_id": id,
        "ingested": attention.len(),
        "attention": attention,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_connector_run_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgRealityConnectorRunRequest>,
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
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.connector_run.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "run": run,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_source_pack_connector_run_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MfgRealityConnectorRunRequest>,
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
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.connector_run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "run": run,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_connector_run_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let run = state
        .services
        .matrix
        .get_connector_run(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG Reality connector run not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.connector_run",
        "run": run,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metrics_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metrics = state
        .services
        .matrix
        .list_metric_definitions(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metrics",
        "metrics": metrics,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_detail_handler(
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
            "MFG Reality metric state not found",
        ));
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metric",
        "metric_id": id,
        "states": states,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_lineage_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lineage = state
        .services
        .matrix
        .metric_lineage(&state.config_home, &id, 6)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metric.lineage",
        "schema_version": "matrix.metric_lineage.v1",
        "lineage": lineage,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_attention_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityMetricAttentionPlanRequest>,
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
        "kind": "mfg.reality.metric_attention.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_snapshot_materialize_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityMetricSnapshotMaterializeRequest>,
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
        "kind": "mfg.reality.metric_snapshot",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "snapshot": snapshot,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_dependency_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityMetricDependencyUpsertRequest>,
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
        "kind": "mfg.reality.metric_dependency",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "dependency": dependency,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_affected_by_fact_type_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityAffectedByFactTypeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metric_ids = state
        .services
        .matrix
        .metrics_affected_by_fact_type(&state.config_home, &request.fact_type)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metric_dependency.affected_by_fact_type",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "fact_type": request.fact_type,
        "metric_ids": metric_ids,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_compute_job_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityComputeJobPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let plan = state
        .services
        .matrix
        .plan_compute_job_for_fact_type(&state.config_home, request.job)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.compute.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_compute_job_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .services
        .matrix
        .get_compute_job(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG Reality compute job not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.compute.job",
        "job": job,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_compute_job_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .services
        .matrix
        .run_compute_job(&state.config_home, &id)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.compute.job",
        "job": job,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_metric_recompute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .matrix
        .recompute_metrics(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metrics.recompute",
        "result": result,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entities = state
        .services
        .matrix
        .list_entities(&state.config_home, 100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entities",
        "entities": entities,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entity = state
        .services
        .matrix
        .get_entity(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG Reality entity not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity",
        "entity": entity,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityEntityUpsertRequest>,
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
        "kind": "mfg.reality.entity",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "entity": entity,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_resolve_source_key_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityEntityResolveSourceKeyRequest>,
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
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "MFG Reality entity source key not found",
            )
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.resolution",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_system": request.source_system,
        "source_key": request.source_key,
        "entity": entity,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_match_candidate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityEntityMatchCandidateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let candidate = state
        .services
        .matrix
        .propose_entity_match(
            &state.config_home,
            &request.left_entity_id,
            &request.right_entity_id,
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.match_candidate",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "candidate": candidate,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_conflict_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityEntityConflictDecisionRequest>,
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
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.conflict_decision",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "decision": decision,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_relations_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let relations = state
        .services
        .matrix
        .list_entity_relations(&state.config_home, &id, 100)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.relations",
        "schema_version": "matrix.entity_relations.v1",
        "entity_id": id,
        "relations": relations,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_impact_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let trace = state
        .services
        .matrix
        .impact_trace(&state.config_home, &id, 3)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.impact_path",
        "schema_version": "matrix.entity_impact.v1",
        "trace": trace,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_relation_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityRelationUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let relation = state
        .services
        .matrix
        .upsert_relation(
            &state.config_home,
            &MatrixRelation::from_input(request.relation),
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.relation",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "relation": relation,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_fact_ingest_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityFactIngestRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if request.facts.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "at least one MFG Reality fact is required",
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
        "kind": "mfg.reality.fact.ingest",
        "request_id": request.request_id,
        "session_id": session_id,
        "ingested": facts.len(),
        "facts": facts,
        "attention": attention,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_changes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let changes = state
        .services
        .matrix
        .list_changes(&state.config_home, 100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.changes",
        "changes": changes,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_attention_hot_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = state
        .services
        .matrix
        .list_attention(&state.config_home, 50)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.attention.hot",
        "items": items,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_evidence_build_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityEvidenceBuildRequest>,
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
        .map_err(matrix_error)?;
    append_matrix_execution_outcome(
        &state,
        session_id.as_deref(),
        matrix_evidence_packet_outcome(&packet),
    )
    .await
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.evidence.packet",
        "request_id": request.request_id,
        "session_id": session_id,
        "packet": packet,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_evidence_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let packet = state
        .services
        .matrix
        .get_evidence_packet(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "MFG Reality evidence packet not found",
            )
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.evidence.packet",
        "packet": packet,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_evidence_quality_gate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let gate = state
        .services
        .matrix
        .evaluate_evidence_quality(&state.config_home, &id)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.quality_gate",
        "gate": gate,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_quality_gate_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let gate = state
        .services
        .matrix
        .get_quality_gate(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "MFG Reality quality gate not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.quality_gate",
        "gate": gate,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_evidence_context_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let packet = state
        .services
        .matrix
        .get_evidence_packet(&state.config_home, &id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                "MFG Reality evidence packet not found",
            )
        })?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.evidence.context_item",
        "context_item": state.services.context.structured_evidence_item(&packet),
        "boundary": mfg_reality_boundary(),
    })))
}

fn matrix_error(error: MatrixStoreError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

fn mfg_reality_boundary() -> serde_json::Value {
    serde_json::json!({
        "consumer": "mfg",
        "application": "server_manufacturing",
        "core": "reality",
        "engine": "matrix",
        "ownership": "MFG consumes Reality Core projections; Reality Core owns Matrix management.",
    })
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

#[cfg(test)]
mod tests {
    use super::{MfgCockpitReportDeliveryIntent, MfgCrossPlaneBridgeIntent};

    #[test]
    fn mfg_effect_intents_reject_client_supplied_actor_principals() {
        let bridge = serde_json::from_str::<MfgCrossPlaneBridgeIntent>(
            r#"{"actor_principal":"user:forged","mode":"dry_run"}"#,
        );
        let delivery = serde_json::from_str::<MfgCockpitReportDeliveryIntent>(
            r#"{"actor_principal":"user:forged","mode":"dry_run"}"#,
        );

        assert!(bridge.is_err());
        assert!(delivery.is_err());
    }

    #[test]
    fn mfg_effect_intents_construct_server_owned_actor() {
        let intent: MfgCrossPlaneBridgeIntent =
            serde_json::from_str(r#"{"requested_capability":"channel.feishu.send"}"#)
                .expect("valid MFG bridge intent");
        let request = intent.into_request("principal:verified-human".to_string());

        assert_eq!(request.actor_principal, "principal:verified-human");
        assert_eq!(
            request.requested_capability.as_deref(),
            Some("channel.feishu.send")
        );
    }
}
