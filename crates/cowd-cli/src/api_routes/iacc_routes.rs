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
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_domain_pack, server_manufacturing_skill_pack, skill_agent_node_id,
    AgentNodeStatus, AgentRole, AgentRunGraph, AgentTaskNode, CrossPlaneAction,
    CrossPlaneAuditRecord, CrossPlaneExecutionReceipt, CrossPlaneRisk, DataClassification,
    IaccActionExecution, IaccActionExecutionRequest, IaccActionFeedback, IaccCockpitProfile,
    IaccCockpitProfileInput, IaccCockpitReportDeliveryPayload,
    IaccCockpitReportDeliveryPayloadRequest, IaccCockpitReportDeliveryReceipt,
    IaccCockpitReportDeliveryState, IaccCockpitReportRequest, IaccCockpitReportSnapshot,
    IaccComputeJobInput, IaccCrossPlaneBridgeReceipt, IaccEntity, IaccEntityInput, IaccFact,
    IaccFactInput, IaccIncident, IaccMetricDependency, IaccMetricDependencyInput, IaccPlaybook,
    IaccRelation, IaccRelationInput, IaccSkillManifest, IaccSkillPlan, IaccSkillRun, IaccStore,
    IaccStoreError, IdentityTrust, PolicyDecisionKind, IACC_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};

use crate::task_kernel::TaskRecord;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/iacc/health", get(iacc_health_handler))
        .route("/api/iacc/skills", get(iacc_skills_handler))
        .route("/api/iacc/skills/:id", get(iacc_skill_get_handler))
        .route("/api/iacc/command-center", get(iacc_command_center_handler))
        .route(
            "/api/iacc/cockpit/profiles/upsert",
            post(iacc_cockpit_profile_upsert_handler),
        )
        .route(
            "/api/iacc/cockpit/profiles/:id",
            get(iacc_cockpit_profile_get_handler),
        )
        .route(
            "/api/iacc/cockpit/profiles/:id/projection",
            get(iacc_cockpit_projection_handler),
        )
        .route(
            "/api/iacc/cockpit/profiles/:id/reports/generate",
            post(iacc_cockpit_report_generate_handler),
        )
        .route(
            "/api/iacc/cockpit/reports/schedules/run",
            post(iacc_cockpit_report_schedule_run_handler),
        )
        .route(
            "/api/iacc/cockpit/reports/:id",
            get(iacc_cockpit_report_get_handler),
        )
        .route(
            "/api/iacc/cockpit/reports/:id/deliver",
            post(iacc_cockpit_report_deliver_handler),
        )
        .route(
            "/api/iacc/cockpit/reports/:id/delivery-state",
            get(iacc_cockpit_report_delivery_state_handler),
        )
        .route(
            "/api/iacc/cockpit/reports/:id/delivery/retry",
            post(iacc_cockpit_report_delivery_retry_handler),
        )
        .route(
            "/api/iacc/domain/server-manufacturing",
            get(iacc_server_manufacturing_domain_handler),
        )
        .route(
            "/api/iacc/domain/server-manufacturing/seed",
            post(iacc_server_manufacturing_seed_handler),
        )
        .route("/api/iacc/entities", get(iacc_entities_handler))
        .route(
            "/api/iacc/entities/upsert",
            post(iacc_entity_upsert_handler),
        )
        .route(
            "/api/iacc/entities/resolve-source-key",
            post(iacc_entity_resolve_source_key_handler),
        )
        .route("/api/iacc/entities/:id", get(iacc_entity_get_handler))
        .route(
            "/api/iacc/entities/:id/relations",
            get(iacc_entity_relations_handler),
        )
        .route(
            "/api/iacc/entities/:id/impact-path",
            get(iacc_entity_impact_path_handler),
        )
        .route(
            "/api/iacc/relations/upsert",
            post(iacc_relation_upsert_handler),
        )
        .route("/api/iacc/facts/ingest", post(iacc_fact_ingest_handler))
        .route("/api/iacc/metrics", get(iacc_metrics_handler))
        .route("/api/iacc/metrics/:id", get(iacc_metric_detail_handler))
        .route(
            "/api/iacc/metrics/:id/lineage",
            get(iacc_metric_lineage_handler),
        )
        .route(
            "/api/iacc/metrics/recompute",
            post(iacc_metric_recompute_handler),
        )
        .route(
            "/api/iacc/metric-dependencies/upsert",
            post(iacc_metric_dependency_upsert_handler),
        )
        .route(
            "/api/iacc/metric-dependencies/affected-by-fact-type",
            post(iacc_metric_affected_by_fact_type_handler),
        )
        .route(
            "/api/iacc/compute/jobs/plan",
            post(iacc_compute_job_plan_handler),
        )
        .route(
            "/api/iacc/compute/jobs/:id",
            get(iacc_compute_job_get_handler),
        )
        .route(
            "/api/iacc/compute/jobs/:id/run",
            post(iacc_compute_job_run_handler),
        )
        .route("/api/iacc/changes", get(iacc_changes_handler))
        .route("/api/iacc/attention/hot", get(iacc_attention_hot_handler))
        .route(
            "/api/iacc/evidence/build",
            post(iacc_evidence_build_handler),
        )
        .route("/api/iacc/evidence/:id", get(iacc_evidence_get_handler))
        .route(
            "/api/iacc/evidence/:id/quality-gate",
            post(iacc_evidence_quality_gate_handler),
        )
        .route(
            "/api/iacc/evidence/:id/context",
            get(iacc_evidence_context_handler),
        )
        .route(
            "/api/iacc/quality-gates/:id",
            get(iacc_quality_gate_get_handler),
        )
        .route("/api/iacc/incidents", post(iacc_incident_create_handler))
        .route("/api/iacc/incidents/:id", get(iacc_incident_get_handler))
        .route(
            "/api/iacc/incidents/:id/room",
            get(iacc_incident_room_handler),
        )
        .route(
            "/api/iacc/incidents/:id/analyze",
            post(iacc_incident_analyze_handler),
        )
        .route(
            "/api/iacc/incidents/:id/cases/promote",
            post(iacc_incident_case_promote_handler),
        )
        .route(
            "/api/iacc/incidents/:id/playbooks/recommend",
            post(iacc_incident_playbook_recommend_handler),
        )
        .route(
            "/api/iacc/incidents/:id/skills/plan",
            post(iacc_incident_skill_plan_handler),
        )
        .route(
            "/api/iacc/incidents/:id/skills/:skill_id/run",
            post(iacc_incident_skill_run_handler),
        )
        .route("/api/iacc/cases/:id", get(iacc_memory_case_get_handler))
        .route(
            "/api/iacc/cases/search",
            get(iacc_memory_case_search_handler),
        )
        .route(
            "/api/iacc/playbooks/upsert",
            post(iacc_playbook_upsert_handler),
        )
        .route("/api/iacc/playbooks/:id", get(iacc_playbook_get_handler))
        .route("/api/iacc/analyses/:id", get(iacc_analysis_get_handler))
        .route(
            "/api/iacc/analyses/:analysis_id/actions/:action_id/execute",
            post(iacc_action_execute_handler),
        )
        .route("/api/iacc/executions/:id", get(iacc_execution_get_handler))
        .route(
            "/api/iacc/executions/:id/cross-plane/execute",
            post(iacc_execution_cross_plane_bridge_handler),
        )
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
struct IaccCockpitProfileUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    profile: IaccCockpitProfileInput,
}

#[derive(Debug, Deserialize)]
struct IaccCockpitReportGenerateRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    report: IaccCockpitReportRequest,
}

#[derive(Debug, Deserialize)]
struct IaccCockpitReportScheduleRunRequest {
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
    #[serde(default = "default_iacc_bridge_mode")]
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
struct IaccEntityUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    entity: IaccEntityInput,
}

#[derive(Debug, Deserialize)]
struct IaccEntityResolveSourceKeyRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    source_system: String,
    source_key: String,
}

#[derive(Debug, Deserialize)]
struct IaccRelationUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    relation: IaccRelationInput,
}

#[derive(Debug, Deserialize)]
struct IaccMetricDependencyUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    dependency: IaccMetricDependencyInput,
}

#[derive(Debug, Deserialize)]
struct IaccAffectedByFactTypeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    fact_type: String,
}

#[derive(Debug, Deserialize)]
struct IaccComputeJobPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    job: IaccComputeJobInput,
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

#[derive(Debug, Deserialize)]
struct IaccCaseSearchQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct IaccPlaybookUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    playbook: IaccPlaybook,
}

#[derive(Debug, Deserialize)]
struct IaccPlaybookRecommendRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct IaccSkillPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct IaccSkillRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IaccCrossPlaneBridgeRequest {
    #[serde(default = "default_iacc_bridge_mode")]
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
struct IaccCockpitReportDeliveryRequest {
    #[serde(default = "default_iacc_bridge_mode")]
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
struct IaccCockpitReportDeliveryRetryRequest {
    #[serde(default = "default_iacc_bridge_mode")]
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
struct IaccCockpitReportDeliveryOutcome {
    mode: String,
    status: String,
    dispatch_status: String,
    report: IaccCockpitReportSnapshot,
    delivery_payload: IaccCockpitReportDeliveryPayload,
    cross_plane_execution_receipt: CrossPlaneExecutionReceipt,
    idempotent_replay: bool,
}

fn default_iacc_bridge_mode() -> String {
    "dry_run".to_string()
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
        "entity_count": health.entity_count,
        "relation_count": health.relation_count,
        "metric_dependency_count": health.metric_dependency_count,
        "compute_job_count": health.compute_job_count,
        "quality_gate_count": health.quality_gate_count,
        "cockpit_profile_count": health.cockpit_profile_count,
        "cockpit_report_count": health.cockpit_report_count,
        "memory_case_count": health.memory_case_count,
        "playbook_count": health.playbook_count,
        "store": iacc_store_path(&state.workspace_root),
        "capabilities": [
            "cockpit_report_snapshot",
            "scheduled_report_foundation",
            "cockpit_report_delivery_bridge",
            "cockpit_report_payload_templates",
            "cockpit_report_schedule_runner",
            "cockpit_report_delivery_retry_state",
            "cockpit_report_webui_visibility",
            "production_operation_package",
            "memory_case_promotion",
            "playbook_recommendation",
            "server_manufacturing_skill_pack",
            "incident_skill_agent_graph",
            "command_center_projection",
            "incident_room_projection",
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
            "action_execution_feedback"
        ],
    })))
}

async fn iacc_skills_handler() -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "iacc.skill_pack",
        "domain": "server_manufacturing",
        "items": server_manufacturing_skill_pack(),
    })))
}

async fn iacc_skill_get_handler(
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let skill = find_iacc_skill(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC skill not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.skill",
        "skill": skill,
    })))
}

async fn iacc_command_center_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
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
        "kind": "iacc.command_center",
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

async fn iacc_cockpit_profile_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccCockpitProfileUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let profile = store
        .upsert_cockpit_profile(&IaccCockpitProfile::from_input(request.profile))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.profile",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "profile": profile,
    })))
}

async fn iacc_cockpit_profile_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let profile = store
        .get_cockpit_profile(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC cockpit profile not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.profile",
        "profile": profile,
    })))
}

async fn iacc_cockpit_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let projection = store.cockpit_projection(&id).map_err(|error| match error {
        IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.projection",
        "projection": projection,
    })))
}

async fn iacc_cockpit_report_generate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccCockpitReportGenerateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report =
        store
            .generate_cockpit_report(&id, request.report)
            .map_err(|error| match error {
                IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.report",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "report": report,
    })))
}

async fn iacc_cockpit_report_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC cockpit report not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.report",
        "report": report,
    })))
}

async fn iacc_cockpit_report_deliver_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccCockpitReportDeliveryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC cockpit report not found"))?;
    let outcome = deliver_iacc_cockpit_report(&state, &store, report, request)?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.report_delivery",
        "mode": outcome.mode,
        "status": outcome.status,
        "dispatch_status": outcome.dispatch_status,
        "report": outcome.report,
        "delivery_payload": outcome.delivery_payload,
        "cross_plane_execution_receipt": outcome.cross_plane_execution_receipt,
        "idempotent_replay": outcome.idempotent_replay,
    })))
}

async fn iacc_cockpit_report_delivery_state_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC cockpit report not found"))?;
    let delivery_state = IaccCockpitReportDeliveryState::from_report(&report);
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.report_delivery_state",
        "report_id": report.report_id,
        "delivery_state": delivery_state,
    })))
}

async fn iacc_cockpit_report_delivery_retry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccCockpitReportDeliveryRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let report = store
        .get_cockpit_report(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC cockpit report not found"))?;
    let before_state = IaccCockpitReportDeliveryState::from_report(&report);
    if !before_state.retryable && !request.force {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!(
                "IACC cockpit report delivery is not retryable: {}",
                before_state.classification
            ),
        ));
    }
    let delivery_request = iacc_retry_delivery_request(&report, &before_state, request);
    let outcome = deliver_iacc_cockpit_report(&state, &store, report, delivery_request)?;
    let after_state = IaccCockpitReportDeliveryState::from_report(&outcome.report);
    Ok(Json(serde_json::json!({
        "kind": "iacc.cockpit.report_delivery_retry",
        "before_state": before_state,
        "after_state": after_state,
        "delivery": outcome,
    })))
}

async fn iacc_cockpit_report_schedule_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccCockpitReportScheduleRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
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
                IaccCockpitReportRequest {
                    report_id,
                    cadence: request
                        .cadence
                        .clone()
                        .or_else(|| Some(profile.cadence.clone())),
                    delivery_ref: request
                        .delivery_ref
                        .clone()
                        .or_else(|| default_iacc_schedule_delivery_ref(&profile, &request)),
                    note: Some("scheduled cockpit report".to_string()),
                },
            )
            .map_err(|error| match error {
                IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
                other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;

        if request.deliver {
            let delivery_request =
                iacc_schedule_delivery_request(&profile, &report, &request, delivery_count);
            let outcome = deliver_iacc_cockpit_report(&state, &store, report, delivery_request)?;
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
        "kind": "iacc.cockpit.report_schedule_run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "cadence": request.cadence,
        "matched_profile_count": items.len(),
        "generated_report_count": items.len(),
        "delivery_count": delivery_count,
        "items": items,
    })))
}

async fn iacc_server_manufacturing_domain_handler(
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "kind": "iacc.domain_pack",
        "pack": server_manufacturing_domain_pack(),
    })))
}

async fn iacc_server_manufacturing_seed_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let result = store
        .seed_server_manufacturing_domain()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.domain_seed",
        "result": result,
    })))
}

async fn iacc_entities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entities = store
        .list_entities(100)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.entities",
        "entities": entities,
    })))
}

async fn iacc_entity_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccEntityUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entity = store
        .upsert_entity(&IaccEntity::from_input(request.entity))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.entity",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "entity": entity,
    })))
}

async fn iacc_entity_resolve_source_key_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccEntityResolveSourceKeyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entity = store
        .resolve_entity_by_source_key(&request.source_system, &request.source_key)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC entity source key not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.entity.resolution",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_system": request.source_system,
        "source_key": request.source_key,
        "entity": entity,
    })))
}

async fn iacc_entity_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let entity = store
        .get_entity(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC entity not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.entity",
        "entity": entity,
    })))
}

async fn iacc_relation_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccRelationUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let relation = store
        .upsert_relation(&IaccRelation::from_input(request.relation))
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.relation",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "relation": relation,
    })))
}

async fn iacc_entity_relations_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let relations = store
        .list_entity_relations(&id, 100)
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.entity.relations",
        "entity_id": id,
        "relations": relations,
    })))
}

async fn iacc_entity_impact_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let trace = store.impact_trace(&id, 3).map_err(|error| match error {
        IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.entity.impact_path",
        "trace": trace,
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

async fn iacc_metric_lineage_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let lineage = store
        .metric_lineage(&id, 6)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.metric.lineage",
        "lineage": lineage,
    })))
}

async fn iacc_metric_dependency_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccMetricDependencyUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let dependency = store
        .upsert_metric_dependency(&IaccMetricDependency::from_input(request.dependency))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.metric_dependency",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "dependency": dependency,
    })))
}

async fn iacc_metric_affected_by_fact_type_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccAffectedByFactTypeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let metric_ids = store
        .metrics_affected_by_fact_type(&request.fact_type)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.metric_dependency.affected_by_fact_type",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "fact_type": request.fact_type,
        "metric_ids": metric_ids,
    })))
}

async fn iacc_compute_job_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccComputeJobPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let plan = store
        .plan_compute_job_for_fact_type(request.job)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.compute.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
    })))
}

async fn iacc_compute_job_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let job = store
        .get_compute_job(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC compute job not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.compute.job",
        "job": job,
    })))
}

async fn iacc_compute_job_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let job = store.run_compute_job(&id).map_err(|error| match error {
        IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.compute.job",
        "job": job,
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

async fn iacc_evidence_quality_gate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let gate = store
        .evaluate_evidence_quality(&id)
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.quality_gate",
        "gate": gate,
    })))
}

async fn iacc_quality_gate_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let gate = store
        .get_quality_gate(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC quality gate not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.quality_gate",
        "gate": gate,
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

async fn iacc_incident_room_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC incident not found"))?;
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
        "kind": "iacc.incident_room",
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

async fn iacc_incident_case_promote_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let promotion = store
        .promote_incident_to_memory_case(&id)
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.memory_case.promotion",
        "memory_case": promotion.memory_case,
        "playbook": promotion.playbook,
    })))
}

async fn iacc_memory_case_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let memory_case = store
        .get_memory_case(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC memory case not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.memory_case",
        "memory_case": memory_case,
    })))
}

async fn iacc_memory_case_search_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<IaccCaseSearchQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let cases = store
        .search_memory_cases(query.q.as_deref(), query.limit.unwrap_or(20).min(100))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.memory_case.search",
        "items": cases,
    })))
}

async fn iacc_playbook_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<IaccPlaybookUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let playbook = store
        .upsert_playbook(&request.playbook)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.playbook",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "playbook": playbook,
    })))
}

async fn iacc_playbook_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let playbook = store
        .get_playbook(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC playbook not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.playbook",
        "playbook": playbook,
    })))
}

async fn iacc_incident_playbook_recommend_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccPlaybookRecommendRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let playbooks = store
        .recommend_playbooks_for_incident(&id, request.limit.unwrap_or(5).min(20))
        .map_err(|error| match error {
            IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.playbook.recommendation",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident_id": id,
        "playbooks": playbooks,
    })))
}

async fn iacc_incident_skill_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccSkillPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC incident not found"))?;
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
    let graph = plan_iacc_skill_agent_nodes(&state, &incident, &plan)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.skill.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident_id": id,
        "plan": plan,
        "agent_graph": graph,
    })))
}

async fn iacc_incident_skill_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath((id, skill_id)): AxumPath<(String, String)>,
    Json(request): Json<IaccSkillRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let incident = store
        .get_incident(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC incident not found"))?;
    let skill = find_iacc_skill(&skill_id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC skill not found"))?;
    let run = run_server_manufacturing_skill(&incident, &skill);
    let graph = complete_iacc_skill_agent_node(&state, &incident, &run)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.skill.run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "incident_id": id,
        "skill_run": run,
        "agent_graph": graph,
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

async fn iacc_execution_cross_plane_bridge_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<IaccCrossPlaneBridgeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    super::cross_plane_routes::ensure_cross_plane_loaded(&state);
    let store = open_iacc_store(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let execution = store
        .get_execution(&id)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "IACC action execution not found"))?;
    let mode = normalize_iacc_bridge_mode(&request.mode);
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
            let execution = attach_iacc_cross_plane_receipt(&store, &execution, &receipt)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
            return Ok(Json(serde_json::json!({
                "kind": "iacc.cross_plane_action_bridge",
                "mode": receipt.mode,
                "status": receipt.status,
                "dispatch_status": receipt.dispatch_status,
                "execution": execution,
                "cross_plane_execution_receipt": receipt,
                "idempotent_replay": true,
            })));
        }
    }

    let action = iacc_cross_plane_action_from_execution(&execution, &request);
    let now = chrono::Utc::now();
    let (action, decision, evidence) =
        super::cross_plane_routes::decide_connector_action(&state, action, &mode, now);
    let (status, dispatch_status, blockers, audit_result, audit_summary) =
        iacc_cross_plane_bridge_outcome(&mode, &decision);
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
    let execution = attach_iacc_cross_plane_receipt(&store, &execution, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "iacc.cross_plane_action_bridge",
        "mode": mode,
        "status": status,
        "dispatch_status": dispatch_status,
        "execution": execution,
        "cross_plane_execution_receipt": receipt,
        "idempotent_replay": false,
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

fn attach_iacc_cross_plane_receipt(
    store: &IaccStore,
    execution: &IaccActionExecution,
    receipt: &CrossPlaneExecutionReceipt,
) -> Result<IaccActionExecution, IaccStoreError> {
    store.attach_cross_plane_receipt(
        &execution.execution_id,
        IaccCrossPlaneBridgeReceipt::new(
            execution.execution_id.clone(),
            receipt.id.clone(),
            receipt.status.clone(),
            receipt.dispatch_status.clone(),
            receipt.audit_record_id.clone(),
        ),
    )
}

fn deliver_iacc_cockpit_report(
    state: &AppState,
    store: &IaccStore,
    report: IaccCockpitReportSnapshot,
    request: IaccCockpitReportDeliveryRequest,
) -> Result<IaccCockpitReportDeliveryOutcome, (StatusCode, Json<ErrorResponse>)> {
    super::cross_plane_routes::ensure_cross_plane_loaded(state);
    let mode = normalize_iacc_bridge_mode(&request.mode);
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    let delivery_payload = iacc_report_delivery_payload(&report, &request);

    if let Some(key) = &idempotency_key {
        if let Some(receipt) =
            super::cross_plane_routes::cross_plane_control().find_execution_by_idempotency_key(key)
        {
            if !iacc_report_delivery_receipt_matches(&receipt, &report) {
                return Err(api_error(
                    StatusCode::CONFLICT,
                    "IACC cockpit report delivery idempotency key belongs to another cross-plane action",
                ));
            }
            let report = attach_iacc_report_delivery_receipt(store, &report, &receipt)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
            return Ok(IaccCockpitReportDeliveryOutcome {
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

    let action = iacc_report_delivery_action(&report, &request, &delivery_payload);
    let now = chrono::Utc::now();
    let (action, decision, evidence) =
        super::cross_plane_routes::decide_connector_action(state, action, &mode, now);
    let (status, dispatch_status, blockers, audit_result, audit_summary) =
        iacc_cross_plane_bridge_outcome(&mode, &decision);
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
    let report = attach_iacc_report_delivery_receipt(store, &report, &receipt)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(IaccCockpitReportDeliveryOutcome {
        mode,
        status,
        dispatch_status,
        report,
        delivery_payload,
        cross_plane_execution_receipt: receipt,
        idempotent_replay: false,
    })
}

fn attach_iacc_report_delivery_receipt(
    store: &IaccStore,
    report: &IaccCockpitReportSnapshot,
    receipt: &CrossPlaneExecutionReceipt,
) -> Result<IaccCockpitReportSnapshot, IaccStoreError> {
    store.attach_cockpit_report_delivery(
        &report.report_id,
        IaccCockpitReportDeliveryReceipt::new(
            report.report_id.clone(),
            receipt.id.clone(),
            receipt.status.clone(),
            receipt.dispatch_status.clone(),
            receipt.audit_record_id.clone(),
        ),
    )
}

fn default_iacc_schedule_delivery_ref(
    profile: &IaccCockpitProfile,
    request: &IaccCockpitReportScheduleRunRequest,
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

fn iacc_schedule_delivery_request(
    profile: &IaccCockpitProfile,
    report: &IaccCockpitReportSnapshot,
    request: &IaccCockpitReportScheduleRunRequest,
    delivery_index: usize,
) -> IaccCockpitReportDeliveryRequest {
    IaccCockpitReportDeliveryRequest {
        mode: request.mode.clone(),
        idempotency_key: Some(format!(
            "iacc-schedule:{}:{}:{}",
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
            .or_else(|| Some("iacc.report.schedule".to_string())),
        requested_capability: request.requested_capability.clone(),
        provider_account: request.provider_account.clone(),
        target_ref: report.delivery_ref.clone(),
        resource_ref: None,
        channel: request.channel.clone(),
        template_id: request.template_id.clone(),
    }
}

fn iacc_retry_delivery_request(
    report: &IaccCockpitReportSnapshot,
    state: &IaccCockpitReportDeliveryState,
    request: IaccCockpitReportDeliveryRetryRequest,
) -> IaccCockpitReportDeliveryRequest {
    let latest_receipt_id = state
        .latest_receipt
        .as_ref()
        .map(|receipt| receipt.cross_plane_receipt_id.as_str())
        .unwrap_or("no-receipt");
    IaccCockpitReportDeliveryRequest {
        mode: request.mode,
        idempotency_key: request.idempotency_key.or_else(|| {
            Some(format!(
                "iacc-retry:{}:{}:{}",
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
            .or_else(|| Some("iacc.report.retry".to_string())),
        requested_capability: request.requested_capability,
        provider_account: request.provider_account,
        target_ref: request.target_ref.or_else(|| report.delivery_ref.clone()),
        resource_ref: request.resource_ref,
        channel: request.channel,
        template_id: request.template_id,
    }
}

fn iacc_report_delivery_receipt_matches(
    receipt: &CrossPlaneExecutionReceipt,
    report: &IaccCockpitReportSnapshot,
) -> bool {
    receipt.action.session_id.as_deref() == Some(report.report_id.as_str())
}

fn find_iacc_skill(skill_id: &str) -> Option<IaccSkillManifest> {
    server_manufacturing_skill_pack()
        .into_iter()
        .find(|skill| skill.skill_id == skill_id)
}

async fn plan_iacc_skill_agent_nodes(
    state: &AppState,
    incident: &IaccIncident,
    plan: &IaccSkillPlan,
) -> Result<Option<AgentRunGraph>, String> {
    let Some(task_id) = incident.task_id.as_deref() else {
        return Ok(None);
    };
    let Some(mut graph) = state.task_kernel.agent_graph(task_id) else {
        return Ok(None);
    };
    let now = now_ms();
    let dependency = if graph.nodes.iter().any(|node| node.id == "iacc_reviewer") {
        "iacc_reviewer"
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
                "iacc_skill_manifest",
                format!("iacc:skill:{}", skill.skill_id),
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
    append_iacc_agent_runtime_event(state, &task, &graph).await?;
    Ok(Some(graph))
}

async fn complete_iacc_skill_agent_node(
    state: &AppState,
    incident: &IaccIncident,
    run: &IaccSkillRun,
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
        let skill = find_iacc_skill(&run.skill_id)
            .ok_or_else(|| format!("IACC skill {} not found", run.skill_id))?;
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
            "iacc_skill_run",
            format!("iacc:skill-run:{}:{}", incident.incident_id, run.skill_id),
            run.summary.clone(),
        )
        .map_err(|error| error.to_string())?;
    let task = state
        .task_kernel
        .upsert_agent_graph(task_id, graph.clone())?;
    append_iacc_agent_runtime_event(state, &task, &graph).await?;
    Ok(Some(graph))
}

fn iacc_report_delivery_payload(
    report: &IaccCockpitReportSnapshot,
    request: &IaccCockpitReportDeliveryRequest,
) -> IaccCockpitReportDeliveryPayload {
    IaccCockpitReportDeliveryPayload::from_report(
        report,
        IaccCockpitReportDeliveryPayloadRequest {
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

fn iacc_report_delivery_action(
    report: &IaccCockpitReportSnapshot,
    request: &IaccCockpitReportDeliveryRequest,
    delivery_payload: &IaccCockpitReportDeliveryPayload,
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
            .unwrap_or_else(|| "iacc.report".to_string()),
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

fn iacc_cross_plane_action_from_execution(
    execution: &IaccActionExecution,
    request: &IaccCrossPlaneBridgeRequest,
) -> CrossPlaneAction {
    let actor_principal = request
        .actor_principal
        .as_deref()
        .or(execution.operator_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("iacc:operator");
    let requested_capability = request
        .requested_capability
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_iacc_cross_plane_capability(execution));
    let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
    action.actor_identity_ref = request.actor_identity_ref.clone();
    action.source_channel = Some(
        request
            .source_channel
            .clone()
            .unwrap_or_else(|| "iacc".to_string()),
    );
    action.session_id = Some(execution.incident_id.clone());
    action.provider_account = request.provider_account.clone();
    action.target_ref = request.target_ref.clone();
    action.resource_ref = request
        .resource_ref
        .clone()
        .or_else(|| Some(format!("text://{}", default_iacc_bridge_message(execution))));
    action.risk = iacc_cross_plane_risk(execution);
    action.data_classification = DataClassification::Internal;
    action.identity_trust = IdentityTrust::Unknown;
    action
}

fn default_iacc_cross_plane_capability(execution: &IaccActionExecution) -> &'static str {
    match execution.action_type.as_str() {
        "supplier_recovery" | "plan_bom_reconciliation" | "evidence_review" => {
            "channel.feishu.send_text"
        }
        _ => "channel.feishu.send_text",
    }
}

fn default_iacc_bridge_message(execution: &IaccActionExecution) -> String {
    format!(
        "IACC action {} [{}]: {}; incident={}; execution={}",
        execution.action_type,
        execution.owner_role,
        execution.title,
        execution.incident_id,
        execution.execution_id
    )
}

fn iacc_cross_plane_risk(execution: &IaccActionExecution) -> CrossPlaneRisk {
    if execution.governance.contains("human_review") || execution.mode == "commit" {
        CrossPlaneRisk::Medium
    } else {
        CrossPlaneRisk::Low
    }
}

fn normalize_iacc_bridge_mode(mode: &str) -> String {
    match mode.trim().to_ascii_lowercase().as_str() {
        "commit" | "live" | "execute" => "commit".to_string(),
        _ => "dry_run".to_string(),
    }
}

fn iacc_cross_plane_bridge_outcome(
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
                "iacc_cross_plane_bridge_dry_run_plan".to_string(),
            );
        }
        return (
            "planned".to_string(),
            "human_review_required".to_string(),
            vec!["iacc:human_review_required".to_string()],
            "planned".to_string(),
            "iacc_cross_plane_bridge_queued_for_human_review".to_string(),
        );
    }
    (
        "blocked".to_string(),
        "policy_blocked".to_string(),
        vec![format!("policy:{}", decision.reason)],
        "blocked".to_string(),
        "iacc_cross_plane_bridge_policy_blocked".to_string(),
    )
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
