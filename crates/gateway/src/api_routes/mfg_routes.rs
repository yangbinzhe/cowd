use std::sync::Arc;

use app_mfg::{
    mfg_widget_catalog, MfgActionExecutionRequest, MfgActionFeedback, MfgAlertCommand,
    MfgAlertCommandInput, MfgAlertRule, MfgAlertRuleInput, MfgAlertSubscription,
    MfgAlertSubscriptionInput, MfgAssignment, MfgAssignmentCommand, MfgAssignmentCommandInput,
    MfgAssignmentInput, MfgCockpitProfile, MfgCockpitReportDeliveryState, MfgCockpitReportRequest,
    MfgCockpitReportSnapshot, MfgIncident, MfgPlaybook, MfgRepositoryError,
};
use axum::{
    body::Body,
    extract::{MatchedPath, Path as AxumPath, Query, State as AxumState},
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use matrix_core::{
    MatrixComputeJobInput, MatrixConnectorRunInput, MatrixDataPlaneIngestPlanInput, MatrixEntity,
    MatrixEntityInput, MatrixFact, MatrixFactInput, MatrixMetricDependency,
    MatrixMetricDependencyInput, MatrixRelation, MatrixRelationInput, MatrixSourcePack,
    MATRIX_SCHEMA_VERSION,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::services::{
    GatewayMatrixRepositoryError as MatrixStoreError, MfgCockpitReportDeliveryOutcome,
    MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
};

use super::matrix_outcomes::{
    append_matrix_execution_outcome, matrix_evidence_packet_outcome, matrix_fact_outcome,
};
use super::mfg_outcomes::{
    append_mfg_execution_outcome, mfg_action_execution_outcome, mfg_skill_run_execution_outcome,
};
mod cockpit;
mod incidents;
mod operations;
use super::{principal_actor_id, AppState, AuthenticatedPrincipal, ErrorResponse};
use cockpit::*;
use incidents::*;
use operations::*;

pub(super) fn start_review_reconciler(state: &Arc<AppState>) {
    if state.services.runtime.is_none() {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let Some(lifecycle) = state.services.mfg.begin_review_reconciler() else {
        return;
    };
    let state_weak = Arc::downgrade(state);
    let handle = runtime.spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(lifecycle_owner) = lifecycle.upgrade() else {
                break;
            };
            if lifecycle_owner.is_cancelled() {
                break;
            }
            let Some(state) = state_weak.upgrade() else {
                break;
            };
            if let Err((status, error)) = reconcile_mfg_report_review_saga(&state, None, 32).await {
                tracing::warn!(
                    status = %status,
                    error = ?error.0,
                    "MFG report review reconciler iteration failed"
                );
            }
        }
    });
    state.services.mfg.install_review_reconciler(handle);
}

/// Public MFG bridge intent. Gateway authentication owns the effective actor;
/// an actor field in an HTTP body is rejected by this closed schema.
#[derive(Debug, Deserialize, JsonSchema)]
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
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgCockpitReportDeliveryIntent {
    #[serde(default = "default_mfg_bridge_mode")]
    mode: String,
    #[serde(default)]
    expected_revision: Option<u64>,
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

/// Public action execution intent. The authenticated gateway principal is the
/// only source of the effective operator id.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgActionExecutionIntent {
    #[serde(default = "default_mfg_bridge_mode")]
    mode: String,
    #[serde(default)]
    expected_revision: Option<u64>,
    #[serde(default)]
    note: Option<String>,
}

impl MfgActionExecutionIntent {
    pub(super) fn into_request(self, operator_id: String) -> MfgActionExecutionRequest {
        MfgActionExecutionRequest {
            mode: self.mode,
            operator_id: Some(operator_id),
            note: self.note,
        }
    }
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

pub(super) fn register_mfg_openapi_schemas(
    registry: &mut app_mfg_contract::MfgOpenApiSchemaRegistry,
) {
    app_bundle_mfg::register_mfg_openapi_schemas(registry);
    registry.register_type::<MfgCrossPlaneBridgeIntent>("MfgCrossPlaneBridgeIntent");
    registry.register_type::<MfgCockpitReportDeliveryIntent>("MfgCockpitReportDeliveryIntent");
    registry.register_type::<MfgActionExecutionIntent>("MfgActionExecutionIntent");
    registry
        .register_type::<MfgCockpitReportScheduleRunRequest>("MfgCockpitReportScheduleRunRequest");
    registry.register_type::<MfgIncidentCreateRequest>("MfgIncidentCreateRequest");
    registry.register_type::<MfgExecutionFeedbackRequest>("MfgExecutionFeedbackRequest");
    registry.register_type::<MfgCaseSearchQuery>("MfgCaseSearchQuery");
    registry.register_type::<MfgPlaybookUpsertRequest>("MfgPlaybookUpsertRequest");
    registry.register_type::<MfgPlaybookRecommendRequest>("MfgPlaybookRecommendRequest");
    registry.register_type::<MfgSkillPlanRequest>("MfgSkillPlanRequest");
    registry.register_type::<MfgSkillRunRequest>("MfgSkillRunRequest");
    registry.register_type::<MfgCockpitReportDeliveryRetryRequest>(
        "MfgCockpitReportDeliveryRetryRequest",
    );
    registry.register_type::<MfgReportReviewListQuery>("MfgReportReviewListQuery");
    registry.register_type::<MatrixDecisionTraceQuery>("MatrixDecisionTraceQuery");
    registry.register_type::<MfgRealityFactIngestRequest>("MfgRealityFactIngestRequest");
    registry.register_type::<MfgRealityEntityUpsertRequest>("MfgRealityEntityUpsertRequest");
    registry.register_type::<MfgRealityEntityResolveSourceKeyRequest>(
        "MfgRealityEntityResolveSourceKeyRequest",
    );
    registry.register_type::<MfgRealityEntityMatchCandidateRequest>(
        "MfgRealityEntityMatchCandidateRequest",
    );
    registry.register_type::<MfgRealityEntityConflictDecisionRequest>(
        "MfgRealityEntityConflictDecisionRequest",
    );
    registry.register_type::<MfgRealityRelationUpsertRequest>("MfgRealityRelationUpsertRequest");
    registry.register_type::<MfgRealityMetricDependencyUpsertRequest>(
        "MfgRealityMetricDependencyUpsertRequest",
    );
    registry.register_type::<MfgRealityMetricAttentionPlanRequest>(
        "MfgRealityMetricAttentionPlanRequest",
    );
    registry.register_type::<MfgRealityMetricSnapshotMaterializeRequest>(
        "MfgRealityMetricSnapshotMaterializeRequest",
    );
    registry.register_type::<MfgRealityAffectedByFactTypeRequest>(
        "MfgRealityAffectedByFactTypeRequest",
    );
    registry.register_type::<MfgRealityComputeJobPlanRequest>("MfgRealityComputeJobPlanRequest");
    registry.register_type::<MfgRealityDataPlaneIngestPlanRequest>(
        "MfgRealityDataPlaneIngestPlanRequest",
    );
    registry.register_type::<MfgRealityEvidenceBuildRequest>("MfgRealityEvidenceBuildRequest");
    registry
        .register_type::<MfgRealitySourcePackUpsertRequest>("MfgRealitySourcePackUpsertRequest");
    registry.register_type::<MfgRealitySourcePackIngestFileRequest>(
        "MfgRealitySourcePackIngestFileRequest",
    );
    registry.register_type::<MfgRealityConnectorRunRequest>("MfgRealityConnectorRunRequest");
    registry.register_type::<MfgAlertListQuery>("MfgAlertListQuery");
    registry.register_type::<MfgAlertRuleUpsertRequest>("MfgAlertRuleUpsertRequest");
    registry
        .register_type::<MfgAlertSubscriptionUpsertRequest>("MfgAlertSubscriptionUpsertRequest");
    registry.register_type::<MfgAlertCommandRequest>("MfgAlertCommandRequest");
    registry.register_type::<MfgForecastQuery>("MfgForecastQuery");
    registry.register_type::<MfgAssignmentListQuery>("MfgAssignmentListQuery");
    registry.register_type::<MfgAssignmentUpsertRequest>("MfgAssignmentUpsertRequest");
    registry.register_type::<MfgAssignmentCommandRequest>("MfgAssignmentCommandRequest");
    registry.register_type::<MfgLiveQuery>("MfgLiveQuery");
}

pub(super) fn mfg_request_schema_component(route_id: app_mfg_contract::MfgRouteId) -> &'static str {
    use app_mfg_contract::MfgRouteId as R;
    match route_id {
        R::RealityDataPlaneIngestPlan => "MfgRealityDataPlaneIngestPlanRequest",
        R::RealitySourcePackUpsert => "MfgRealitySourcePackUpsertRequest",
        R::RealitySourcePackIngestFile => "MfgRealitySourcePackIngestFileRequest",
        R::RealityConnectorRunPlan | R::RealityConnectorRunExecute => {
            "MfgRealityConnectorRunRequest"
        }
        R::RealityMetricAttentionPlan => "MfgRealityMetricAttentionPlanRequest",
        R::RealityMetricSnapshotMaterialize => "MfgRealityMetricSnapshotMaterializeRequest",
        R::RealityMetricDependencyUpsert => "MfgRealityMetricDependencyUpsertRequest",
        R::RealityMetricDependencyAffectedPlan => "MfgRealityAffectedByFactTypeRequest",
        R::RealityComputeJobPlan => "MfgRealityComputeJobPlanRequest",
        R::RealityEntityUpsert => "MfgRealityEntityUpsertRequest",
        R::RealityEntityResolveSourceKey => "MfgRealityEntityResolveSourceKeyRequest",
        R::RealityEntityMatchCandidate => "MfgRealityEntityMatchCandidateRequest",
        R::RealityEntityConflictDecision => "MfgRealityEntityConflictDecisionRequest",
        R::RealityRelationUpsert => "MfgRealityRelationUpsertRequest",
        R::RealityFactIngest => "MfgRealityFactIngestRequest",
        R::RealityEvidenceBuild => "MfgRealityEvidenceBuildRequest",
        R::DecisionTraceGet => "MatrixDecisionTraceQuery",
        R::IncidentList => "MfgIncidentListQuery",
        R::IncidentCreate => "MfgIncidentCreateRequest",
        R::IncidentPlaybookRecommend => "MfgPlaybookRecommendRequest",
        R::IncidentSkillPlan => "MfgSkillPlanRequest",
        R::IncidentSkillRun => "MfgSkillRunRequest",
        R::CaseSearch => "MfgCaseSearchQuery",
        R::PlaybookUpsert => "MfgPlaybookUpsertRequest",
        R::AnalysisActionExecute => "MfgActionExecutionIntent",
        R::ExecutionCrossPlaneExecute => "MfgCrossPlaneBridgeIntent",
        R::ExecutionFeedbackCreate => "MfgExecutionFeedbackRequest",
        R::CockpitProfileList => "MfgCockpitProfileListQuery",
        R::CockpitProfileUpsert => "MfgCockpitProfileUpsertRequest",
        R::CockpitProfileDelete => "MfgCockpitProfileDeleteQuery",
        R::CockpitProfileClone => "MfgCockpitProfileCloneRequest",
        R::CockpitProfileShare => "MfgCockpitProfileShareRequest",
        R::CockpitProjectionGet | R::CockpitWidgetProjectionGet => "MfgCockpitProjectionQuery",
        R::ReportGenerate => "MfgCockpitReportGenerateRequest",
        R::ReportScheduleRun => "MfgCockpitReportScheduleRunRequest",
        R::ReportList => "MfgCockpitReportListQuery",
        R::ReportDeliver => "MfgCockpitReportDeliveryIntent",
        R::ReportDeliveryRetry => "MfgCockpitReportDeliveryRetryRequest",
        R::ReportReviewRequest => "MfgReportDeliveryReviewCreateRequest",
        R::ReportReviewList => "MfgReportReviewListQuery",
        R::ReportReviewDecide => "MfgReportDeliveryReviewDecisionRequest",
        R::AlertRuleList | R::AlertList | R::AlertSubscriptionList => "MfgAlertListQuery",
        R::AlertRuleUpsert => "MfgAlertRuleUpsertRequest",
        R::AlertSubscriptionUpsert => "MfgAlertSubscriptionUpsertRequest",
        R::AlertCommand => "MfgAlertCommandRequest",
        R::ForecastList => "MfgForecastQuery",
        R::AssignmentList => "MfgAssignmentListQuery",
        R::AssignmentUpsert => "MfgAssignmentUpsertRequest",
        R::AssignmentCommand => "MfgAssignmentCommandRequest",
        R::LiveStream => "MfgLiveQuery",
        _ => "MfgNoBodyRequestV1",
    }
}

pub(super) fn router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
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
            "/api/apps/mfg/reality/compute/jobs/:id/run",
            post(mfg_reality_compute_job_run_handler),
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
            "/api/apps/mfg/reality/relations/upsert",
            post(mfg_reality_relation_upsert_handler),
        )
        .route(
            "/api/apps/mfg/reality/facts/ingest",
            post(mfg_reality_fact_ingest_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/build",
            post(mfg_reality_evidence_build_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/:id/quality-gate",
            post(mfg_reality_evidence_quality_gate_handler),
        )
        .route(
            "/api/apps/mfg/reality/evidence/:id/context",
            get(mfg_reality_evidence_context_handler),
        )
        .route("/api/apps/mfg/incidents", post(mfg_incident_create_handler))
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
            "/api/apps/mfg/playbooks/upsert",
            post(mfg_playbook_upsert_handler),
        )
        .route(
            "/api/apps/mfg/analyses/:analysis_id/actions/:action_id/execute",
            post(mfg_action_execute_handler),
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
            "/api/apps/mfg/cockpit/reports/schedules/run",
            post(mfg_cockpit_report_schedule_run_handler),
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
            "/api/apps/mfg/cockpit/reports/:id/reviews",
            post(mfg_cockpit_report_review_request_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/report-reviews",
            get(mfg_cockpit_report_review_list_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/report-reviews/:id",
            get(mfg_cockpit_report_review_get_handler),
        )
        .route(
            "/api/apps/mfg/cockpit/report-reviews/:id/decision",
            post(mfg_cockpit_report_review_decision_handler),
        )
        .route(
            "/api/apps/mfg/focus/alert-rules",
            post(mfg_alert_rule_upsert_handler),
        )
        .route(
            "/api/apps/mfg/focus/alert-subscriptions",
            post(mfg_alert_subscription_upsert_handler),
        )
        .route(
            "/api/apps/mfg/focus/alerts/:id/command",
            post(mfg_alert_command_handler),
        )
        .route(
            "/api/apps/mfg/assignments",
            post(mfg_assignment_upsert_handler),
        )
        .route(
            "/api/apps/mfg/assignments/:id/command",
            post(mfg_assignment_command_handler),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            mfg_capability_middleware,
        ))
}

async fn mfg_capability_middleware(
    AxumState(state): AxumState<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let permit = match state.services.capacity.admit_blocking().await {
        Ok(permit) => permit,
        Err(overload) => {
            return mfg_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                app_mfg_contract::MfgErrorCode::Internal,
                format!(
                    "MFG blocking capacity exhausted; retry after {} ms",
                    overload.retry_after_ms
                ),
                true,
            );
        }
    };
    let runtime = tokio::runtime::Handle::current();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.block_on(mfg_capability_middleware_blocking(state, request, next))
    })
    .await
    {
        Ok(response) => response,
        Err(error) => mfg_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            app_mfg_contract::MfgErrorCode::Internal,
            format!("MFG blocking worker failed: {error}"),
            true,
        ),
    }
}

/// 所有 MFG handler 的同步 repository I/O 都运行在有界 blocking worker
/// 内；handler 自身仍可 await Runtime/connector I/O，不占用 Tokio async worker。
async fn mfg_capability_middleware_blocking(
    state: Arc<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().as_str();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path());
    let Some(contract) = app_mfg_contract::route::mfg_route_contract_by_method_path(method, path)
    else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::ContractMismatch,
                message: format!(
                    "MFG route is not present in the canonical contract: {method} {path}"
                ),
                http_status: 500,
                details: serde_json::json!({"method": method, "path": path}),
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: Vec::new(),
                request_id: None,
                receipt_ref: None,
            }),
        )
            .into_response();
    };
    if contract.availability != app_mfg_contract::MfgActionAvailability::Active {
        return (
            StatusCode::NOT_FOUND,
            Json(app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::ScopeNotFound,
                message: "MFG route is declared but not active in this version".to_string(),
                http_status: 404,
                details: serde_json::json!({"route_id": contract.route_id.as_str()}),
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: Vec::new(),
                request_id: None,
                receipt_ref: None,
            }),
        )
            .into_response();
    }
    let Some(principal) = request
        .extensions()
        .get::<AuthenticatedPrincipal>()
        .cloned()
    else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(app_mfg_contract::MfgApiErrorV1::authentication_required(
                "verified principal is missing",
            )),
        )
            .into_response();
    };
    let granted = &principal.0.claims().capabilities;
    let required = match &contract.capability {
        app_mfg_contract::MfgCapabilityRequirement::One { capability } => {
            vec![capability.as_str()]
        }
        app_mfg_contract::MfgCapabilityRequirement::All { capabilities } => capabilities
            .iter()
            .copied()
            .map(app_mfg_contract::MfgCapabilityId::as_str)
            .collect(),
        // Per-action handlers perform the stronger check after parsing the
        // closed action/mode. Read is the minimum transport capability.
        app_mfg_contract::MfgCapabilityRequirement::PerAction => vec!["mfg.read"],
    };
    if let Some(missing) = required
        .into_iter()
        .find(|required| !granted.iter().any(|capability| capability == required))
    {
        return (
            StatusCode::FORBIDDEN,
            Json(app_mfg_contract::MfgApiErrorV1::capability_denied(missing)),
        )
            .into_response();
    }
    if contract.class != app_mfg_contract::MfgMutationClass::Read {
        return mfg_mutation_ledger_middleware(state, request, next, contract, principal).await;
    }
    let response = next.run(request).await;
    if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::ValidationFailed,
                message: "MFG request payload could not be decoded by the canonical contract"
                    .to_string(),
                http_status: StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
                details: serde_json::Value::Null,
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::Reload,
                    label: "Review the request and try again".to_string(),
                    target: None,
                    enabled: true,
                }],
                request_id: None,
                receipt_ref: None,
            }),
        )
            .into_response();
    }
    response
}

async fn mfg_mutation_ledger_middleware(
    state: Arc<AppState>,
    request: Request<Body>,
    next: Next,
    contract: app_mfg_contract::MfgRouteContract,
    principal: AuthenticatedPrincipal,
) -> Response {
    use app_mfg_contract::{MfgMutationClass, MfgReceiptStatus, MfgReceiptV1};
    use sha2::{Digest, Sha256};

    const MAX_MFG_MUTATION_BODY: usize = 64 * 1024 * 1024;
    let request_path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or_default().to_string();
    let header_key = request
        .headers()
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let header_correlation = request
        .headers()
        .get("x-cowd-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let is_json_body = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.contains("json"));
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_MFG_MUTATION_BODY).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return mfg_error_response(
                StatusCode::BAD_REQUEST,
                app_mfg_contract::MfgErrorCode::ValidationFailed,
                format!("failed to read MFG request body: {error}"),
                false,
            );
        }
    };
    let body_json = if bytes.is_empty() || !is_json_body {
        serde_json::Value::Null
    } else {
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(value) => value,
            Err(error) => {
                return mfg_error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    app_mfg_contract::MfgErrorCode::ValidationFailed,
                    format!("MFG request body is not valid JSON: {error}"),
                    false,
                );
            }
        }
    };
    if matches!(
        contract.route_id,
        app_mfg_contract::MfgRouteId::AnalysisActionExecute
            | app_mfg_contract::MfgRouteId::ExecutionCrossPlaneExecute
            | app_mfg_contract::MfgRouteId::ReportDeliver
            | app_mfg_contract::MfgRouteId::ReportDeliveryRetry
    ) {
        let mode = find_json_string(&body_json, "mode").unwrap_or_default();
        if let Err(message) = normalize_mfg_action_mode(&mode) {
            return mfg_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                app_mfg_contract::MfgErrorCode::ValidationFailed,
                message.to_string(),
                false,
            );
        }
    }
    let body_key = find_json_string(&body_json, "idempotency_key");
    let query_pairs = parse_mfg_query_pairs(&query);
    let query_key = query_pairs
        .iter()
        .find(|(name, _)| name == "idempotency_key")
        .map(|(_, value)| value.clone());
    let legacy_key = body_key.or(query_key);
    let idempotency_key = match (header_key, legacy_key) {
        (Some(header), Some(legacy)) if header != legacy => {
            return mfg_error_response(
                StatusCode::BAD_REQUEST,
                app_mfg_contract::MfgErrorCode::IdempotencyConflict,
                "Idempotency-Key header conflicts with legacy body/query value".to_string(),
                false,
            );
        }
        (Some(header), _) => Some(header),
        (None, legacy) => legacy,
    };
    let action_id = resolve_mfg_action_id(contract.route_id, &body_json);
    let Some(action_contract) = app_mfg_contract::mfg_action_contracts()
        .into_iter()
        .find(|action| action.action_id.as_str() == action_id)
    else {
        return mfg_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            app_mfg_contract::MfgErrorCode::ContractMismatch,
            format!("resolved action is absent from the canonical contract: {action_id}"),
            false,
        );
    };
    let resource_ref = resolve_mfg_resource_ref(
        contract.route_id,
        &request_path,
        &body_json,
        idempotency_key.as_deref(),
    );
    if let Some(missing) = action_contract
        .required_capabilities
        .iter()
        .find(|required| {
            !principal
                .0
                .claims()
                .capabilities
                .iter()
                .any(|granted| granted == *required)
        })
    {
        return (
            StatusCode::FORBIDDEN,
            Json(app_mfg_contract::MfgApiErrorV1::capability_denied(
                missing.clone(),
            )),
        )
            .into_response();
    }
    let expected_revision = find_json_u64(&body_json, "expected_revision").or_else(|| {
        query_pairs
            .iter()
            .find(|(name, _)| name == "expected_revision")
            .and_then(|(_, value)| value.parse::<u64>().ok())
    });
    if let app_mfg_contract::MfgMutationSemantics::DurableReceipt { revision, .. } =
        action_contract.mutation
    {
        match revision {
            app_mfg_contract::MfgRevisionSemantics::Required if expected_revision.is_none() => {
                return mfg_error_response(
                    StatusCode::CONFLICT,
                    app_mfg_contract::MfgErrorCode::RevisionConflict,
                    format!(
                        "action {} requires expected_revision",
                        action_contract.action_id.as_str()
                    ),
                    false,
                );
            }
            app_mfg_contract::MfgRevisionSemantics::CreateOnly if expected_revision.is_some() => {
                return mfg_error_response(
                    StatusCode::CONFLICT,
                    app_mfg_contract::MfgErrorCode::RevisionConflict,
                    format!(
                        "create action {} does not accept expected_revision",
                        action_contract.action_id.as_str()
                    ),
                    false,
                );
            }
            _ => {}
        }
    }
    let digest_body = if is_json_body {
        let mut digest_body = body_json.clone();
        remove_json_field(&mut digest_body, "idempotency_key");
        canonicalize_mfg_json(&digest_body)
    } else {
        serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string())
    };
    let digest_query = query_pairs
        .iter()
        .filter(|(name, _)| name != "idempotency_key")
        .cloned()
        .collect::<std::collections::BTreeMap<_, _>>();
    let digest_value = if digest_query.is_empty() {
        digest_body
    } else {
        serde_json::json!({
            "body": digest_body,
            "query": digest_query,
        })
    };
    let digest_payload = serde_json::to_vec(&canonicalize_mfg_json(&digest_value))
        .unwrap_or_else(|_| bytes.to_vec());
    let payload_digest = format!("sha256:{:x}", Sha256::digest(&digest_payload));
    let actor_principal = principal_actor_id(&principal);
    let durable = !matches!(contract.class, MfgMutationClass::Preview)
        && !matches!(action_contract.class, MfgMutationClass::Preview);
    let idempotency_key = if durable {
        match idempotency_key {
            Some(key) => key,
            None => {
                return mfg_error_response(
                    StatusCode::BAD_REQUEST,
                    app_mfg_contract::MfgErrorCode::ValidationFailed,
                    "Idempotency-Key is required for durable MFG mutations".to_string(),
                    false,
                );
            }
        }
    } else {
        idempotency_key.unwrap_or_else(|| format!("preview-{}", uuid::Uuid::new_v4()))
    };
    let correlation_id =
        header_correlation.unwrap_or_else(|| format!("mfg-correlation:{idempotency_key}"));
    let mut generic_claim_acquired = false;
    if durable {
        match state.services.mfg.claim_mutation_receipt(
            &state.config_home,
            &idempotency_key,
            &actor_principal,
            &action_id,
            &resource_ref,
            expected_revision,
            &payload_digest,
            &correlation_id,
        ) {
            Ok(app_mfg::MfgMutationClaim::Acquired(_)) => {
                generic_claim_acquired = true;
            }
            Ok(app_mfg::MfgMutationClaim::NativeRecovery(_))
                if mfg_route_uses_native_business_receipt(contract.route_id) =>
            {
                // The native repository will replay its committed business
                // receipt before any write; Gateway can then finalize the
                // canonical outer receipt without repeating the effect.
            }
            Ok(app_mfg::MfgMutationClaim::NativeRecovery(_)) => {
                return mfg_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    app_mfg_contract::MfgErrorCode::ContractMismatch,
                    "native business recovery is not valid for this MFG route".to_string(),
                    false,
                );
            }
            Ok(app_mfg::MfgMutationClaim::Replayed(mut receipt, mut response)) => {
                receipt.status = MfgReceiptStatus::Replayed;
                // Preserve the route-level replay signal when the canonical
                // outer receipt short-circuits before the route handler.  A
                // consumer that already renders this field must never see a
                // stale `false` alongside a replayed MFG receipt.
                if let Some(object) = response.as_object_mut() {
                    if object.contains_key("idempotent_replay") {
                        object.insert(
                            "idempotent_replay".to_string(),
                            serde_json::Value::Bool(true),
                        );
                    }
                }
                attach_mfg_receipt(&mut response, &receipt);
                return json_response_with_receipt(StatusCode::OK, response, &receipt);
            }
            Ok(app_mfg::MfgMutationClaim::Pending(_))
                if contract.route_id == app_mfg_contract::MfgRouteId::AssignmentCommand
                    && action_id == "mfg.assignment.complete" =>
            {
                // Completion uses a durable assignment reservation and reads
                // an already-terminal Runtime owner. Re-entry reconciles that
                // saga; the repository CAS prevents a second reservation.
            }
            Ok(app_mfg::MfgMutationClaim::Pending(_))
                if mfg_route_supports_owner_recovery(contract.route_id, &action_id) =>
            {
                // These handlers bind this exact canonical key to a stable
                // owner identity, transition journal, durable outbox, or
                // idempotent owner CAS before applying their effect.
            }
            Ok(app_mfg::MfgMutationClaim::Pending(receipt)) => {
                return mfg_pending_claim_response(&receipt);
            }
            Err(error) => {
                return mfg_error_response(
                    StatusCode::CONFLICT,
                    app_mfg_contract::MfgErrorCode::IdempotencyConflict,
                    error.to_string(),
                    false,
                );
            }
        }
    }

    let request = Request::from_parts(parts, Body::from(bytes));
    let response = next.run(request).await;
    if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
        if generic_claim_acquired {
            let _ = state.services.mfg.release_mutation_claim(
                &state.config_home,
                &idempotency_key,
                &actor_principal,
                &action_id,
                &payload_digest,
            );
        }
        return mfg_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            app_mfg_contract::MfgErrorCode::ValidationFailed,
            "MFG request payload could not be decoded by the canonical contract".to_string(),
            false,
        );
    }
    if response.status().is_client_error()
        && mfg_route_uses_native_business_receipt(contract.route_id)
    {
        // Native 4xx responses are produced by validation/CAS before their
        // domain commit. Assignment completion may have a durable reservation,
        // but re-claiming the same key resumes that explicit saga.
        let _ = state.services.mfg.release_mutation_claim(
            &state.config_home,
            &idempotency_key,
            &actor_principal,
            &action_id,
            &payload_digest,
        );
    }
    if !response.status().is_success() {
        return response;
    }
    if durable {
        // Domain mutation 已经提交：立即唤醒 live observer。Hub 不携带
        // response/raw payload，observer 仍按各自 principal 从 durable log
        // 读取并裁剪。
        state.services.mfg.notify_live_mutation();
    }
    let (mut response_parts, response_body) = response.into_parts();
    let response_bytes = match axum::body::to_bytes(response_body, MAX_MFG_MUTATION_BODY).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return mfg_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                app_mfg_contract::MfgErrorCode::Internal,
                format!("failed to capture MFG mutation response: {error}"),
                true,
            );
        }
    };
    let mut response_json = serde_json::from_slice::<serde_json::Value>(&response_bytes)
        .unwrap_or_else(|_| serde_json::json!({"data": String::from_utf8_lossy(&response_bytes)}));
    if let Some(object) = response_json.as_object_mut() {
        object.insert(
            "correlation_id".to_string(),
            serde_json::Value::String(correlation_id.clone()),
        );
    }
    let result_revision = find_json_u64(&response_json, "revision")
        .or_else(|| find_json_u64(&response_json, "current_revision"));
    let receipt = if durable {
        match state.services.mfg.record_mutation_receipt(
            &state.config_home,
            &idempotency_key,
            &actor_principal,
            &action_id,
            &resource_ref,
            expected_revision,
            result_revision,
            &payload_digest,
            &response_json,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                return mfg_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    app_mfg_contract::MfgErrorCode::Internal,
                    format!("failed to persist MFG mutation receipt: {error}"),
                    true,
                );
            }
        }
    } else {
        let now = chrono::Utc::now();
        MfgReceiptV1 {
            receipt_id: format!("preview-receipt-{}", uuid::Uuid::new_v4()),
            idempotency_key,
            actor_principal,
            action_id: action_contract.action_id,
            resource_ref,
            expected_revision,
            result_revision,
            payload_digest,
            correlation_id: Some(correlation_id),
            status: MfgReceiptStatus::Preview,
            response: response_json.clone(),
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            created_at: now,
            updated_at: now,
        }
    };
    if durable {
        // receipt 自身也是 durable live event；第二次无载荷唤醒确保第一轮
        // delta 与 receipt 提交竞态时，订阅者仍能继续推进 cursor。
        state.services.mfg.notify_live_mutation();
    }
    attach_mfg_receipt(&mut response_json, &receipt);
    response_parts
        .headers
        .remove(axum::http::header::CONTENT_LENGTH);
    if let Ok(value) = axum::http::HeaderValue::from_bytes(receipt.receipt_id.as_bytes()) {
        response_parts
            .headers
            .insert("x-cowd-mfg-receipt-id", value);
    }
    response_parts.headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    let body = serde_json::to_vec(&response_json).unwrap_or_else(|_| response_bytes.to_vec());
    Response::from_parts(response_parts, Body::from(body))
}

fn mfg_route_uses_native_business_receipt(route_id: app_mfg_contract::MfgRouteId) -> bool {
    use app_mfg_contract::MfgRouteId as R;

    matches!(
        route_id,
        R::CockpitProfileUpsert
            | R::CockpitProfileDelete
            | R::CockpitProfileClone
            | R::CockpitProfileShare
            | R::AlertRuleUpsert
            | R::AlertSubscriptionUpsert
            | R::AlertCommand
            | R::AssignmentUpsert
            | R::AssignmentCommand
    )
}

fn mfg_route_supports_owner_recovery(
    route_id: app_mfg_contract::MfgRouteId,
    action_id: &str,
) -> bool {
    use app_mfg_contract::MfgRouteId as R;

    mfg_route_uses_native_business_receipt(route_id)
        || matches!(
            route_id,
            R::RealityEvidenceQualityGate
                | R::IncidentCreate
                | R::IncidentAnalyze
                | R::IncidentSkillRun
                | R::ExecutionCrossPlaneExecute
                | R::ExecutionFeedbackCreate
                | R::ReportGenerate
                | R::ReportDeliver
                | R::ReportDeliveryRetry
                | R::ReportReviewRequest
                | R::ReportReviewDecide
        )
        || (route_id == R::AnalysisActionExecute && action_id == "mfg.analysis.action.commit")
}

fn mfg_pending_claim_response(receipt: &app_mfg_contract::MfgReceiptV1) -> Response {
    (
        StatusCode::CONFLICT,
        Json(app_mfg_contract::MfgApiErrorV1 {
            code: app_mfg_contract::MfgErrorCode::IdempotencyConflict,
            message:
                "the original mutation is still pending or its outcome requires reconciliation"
                    .to_string(),
            http_status: StatusCode::CONFLICT.as_u16(),
            details: serde_json::json!({
                "status": "pending",
                "resource_ref": receipt.resource_ref,
                "correlation_id": receipt.correlation_id,
            }),
            retryable: false,
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            recovery_actions: vec![
                app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::Reload,
                    label: "Reload the canonical resource before deciding the outcome".to_string(),
                    target: Some(receipt.resource_ref.clone()),
                    enabled: true,
                },
                app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::Resync,
                    label: "Reconcile the pending mutation with its domain owner".to_string(),
                    target: receipt.correlation_id.clone(),
                    enabled: true,
                },
            ],
            request_id: receipt.correlation_id.clone(),
            receipt_ref: Some(receipt.receipt_id.clone()),
        }),
    )
        .into_response()
}

fn resolve_mfg_action_id(
    route_id: app_mfg_contract::MfgRouteId,
    body: &serde_json::Value,
) -> String {
    use app_mfg_contract::MfgRouteId as R;
    let raw_mode = find_json_string(body, "mode").unwrap_or_default();
    let mode = normalize_mfg_action_mode(&raw_mode)
        .unwrap_or("commit")
        .to_string();
    match route_id {
        R::RealitySourcePackUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.reality.source_pack.update"
            } else {
                "mfg.reality.source_pack.create"
            }
        }
        R::RealityMetricDependencyUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.reality.metric_dependency.update"
            } else {
                "mfg.reality.metric_dependency.create"
            }
        }
        R::RealityEntityUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.reality.entity.update"
            } else {
                "mfg.reality.entity.create"
            }
        }
        R::RealityRelationUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.reality.relation.update"
            } else {
                "mfg.reality.relation.create"
            }
        }
        R::PlaybookUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.playbook.update"
            } else {
                "mfg.playbook.create"
            }
        }
        R::CockpitProfileUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.cockpit.profile.update"
            } else {
                "mfg.cockpit.profile.create"
            }
        }
        R::AlertRuleUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.alert_rule.update"
            } else {
                "mfg.alert_rule.create"
            }
        }
        R::AlertSubscriptionUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.alert_subscription.update"
            } else {
                "mfg.alert_subscription.create"
            }
        }
        R::AssignmentUpsert => {
            if find_json_u64(body, "expected_revision").is_some() {
                "mfg.assignment.update"
            } else {
                "mfg.assignment.create"
            }
        }
        R::AlertCommand => match find_json_string(body, "command")
            .unwrap_or_default()
            .as_str()
        {
            "acknowledge" => "mfg.alert.acknowledge",
            "snooze" => "mfg.alert.snooze",
            "resolve" => "mfg.alert.resolve",
            "escalate" => "mfg.alert.escalate",
            _ => route_id.as_str(),
        },
        R::AssignmentCommand => match find_json_string(body, "command")
            .unwrap_or_default()
            .as_str()
        {
            "assign" => "mfg.assignment.assign",
            "claim" => "mfg.assignment.claim",
            "transfer" => "mfg.assignment.transfer",
            "unassign" => "mfg.assignment.unassign",
            "watch" => "mfg.assignment.watch",
            "request_update" => "mfg.assignment.request_update",
            "escalate" => "mfg.assignment.escalate",
            "start" => "mfg.assignment.start",
            "complete" => "mfg.assignment.complete",
            _ => route_id.as_str(),
        },
        R::AnalysisActionExecute => {
            if mode.is_empty() || matches!(mode.as_str(), "dry_run" | "plan") {
                "mfg.analysis.action.dry_run"
            } else {
                "mfg.analysis.action.commit"
            }
        }
        R::ExecutionCrossPlaneExecute => {
            if mode.is_empty() || matches!(mode.as_str(), "dry_run" | "plan") {
                "mfg.execution.cross_plane.dry_run"
            } else {
                "mfg.execution.cross_plane.commit"
            }
        }
        R::ReportDeliver => {
            if mode.is_empty() || matches!(mode.as_str(), "dry_run" | "plan") {
                "mfg.report.deliver.dry_run"
            } else {
                "mfg.report.deliver.commit"
            }
        }
        R::ReportDeliveryRetry => {
            if mode.is_empty() || matches!(mode.as_str(), "dry_run" | "plan") {
                "mfg.report.delivery.retry_dry_run"
            } else {
                "mfg.report.delivery.retry_commit"
            }
        }
        R::ReportScheduleRun => {
            if find_json_bool(body, "deliver").unwrap_or(false) {
                "mfg.report.schedule.generate_and_deliver"
            } else {
                "mfg.report.schedule.generate_only"
            }
        }
        R::ReportReviewDecide => {
            match find_json_string(body, "decision")
                .unwrap_or_default()
                .as_str()
            {
                "force_retry" => "mfg.report.review.force_retry",
                "reroute" => "mfg.report.review.reroute",
                "abandon" => "mfg.report.review.abandon",
                "resolve" => "mfg.report.review.resolve",
                "reject" => "mfg.report.review.reject",
                _ => route_id.as_str(),
            }
        }
        R::IncidentSkillRun => "mfg.skill.run",
        _ => route_id.as_str(),
    }
    .to_string()
}

fn normalize_mfg_action_mode(mode: &str) -> Result<&'static str, &'static str> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "" | "dry_run" | "plan" => Ok("dry_run"),
        "commit" | "live" | "execute" => Ok("commit"),
        _ => Err("mode must be one of dry_run, plan, commit, live, or execute"),
    }
}

fn resolve_mfg_resource_ref(
    route_id: app_mfg_contract::MfgRouteId,
    request_path: &str,
    body: &serde_json::Value,
    idempotency_key: Option<&str>,
) -> String {
    use app_mfg_contract::MfgRouteId as R;
    let path_id = |prefix: &str| {
        request_path
            .split(prefix)
            .nth(1)
            .and_then(|suffix| suffix.split('/').next())
            .filter(|id| !id.trim().is_empty())
            .map(str::to_string)
    };
    let identified = match route_id {
        R::IncidentCreate => idempotency_key
            .map(|key| format!("mfg:incident:{}", stable_mfg_resource_id("incident", key))),
        R::IncidentAnalyze | R::IncidentPlaybookRecommend | R::IncidentSkillPlan => {
            path_id("/incidents/").map(|id| format!("mfg:incident:{id}"))
        }
        R::IncidentCasePromote => path_id("/incidents/").map(|id| format!("mfg:incident:{id}")),
        R::AnalysisActionExecute => path_id("/analyses/").and_then(|analysis_id| {
            request_path
                .split("/actions/")
                .nth(1)
                .and_then(|suffix| suffix.split('/').next())
                .filter(|id| !id.trim().is_empty())
                .map(|action_id| format!("mfg:analysis:{analysis_id}:action:{action_id}"))
        }),
        R::AlertCommand => path_id("/alerts/").map(|id| format!("mfg:alert-occurrence:{id}")),
        R::AlertRuleUpsert => find_json_string(body, "rule_id")
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("mfg:alert-rule:{id}"))
            .or_else(|| {
                idempotency_key.map(|key| {
                    format!(
                        "mfg:alert-rule:{}",
                        stable_mfg_resource_id("alert-rule", key)
                    )
                })
            }),
        R::AlertSubscriptionUpsert => find_json_string(body, "subscription_id")
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("mfg:alert-subscription:{id}"))
            .or_else(|| {
                idempotency_key.map(|key| {
                    format!(
                        "mfg:alert-subscription:{}",
                        stable_mfg_resource_id("alert-subscription", key)
                    )
                })
            }),
        R::AssignmentUpsert => find_json_string(body, "assignment_id")
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("mfg:assignment:{id}"))
            .or_else(|| {
                idempotency_key.map(|key| {
                    format!(
                        "mfg:assignment:{}",
                        stable_mfg_resource_id("assignment", key)
                    )
                })
            }),
        R::AssignmentCommand => path_id("/assignments/").map(|id| format!("mfg:assignment:{id}")),
        R::ExecutionFeedbackCreate => {
            path_id("/executions/").map(|id| format!("mfg:execution:{id}"))
        }
        R::ExecutionCrossPlaneExecute => {
            path_id("/executions/").map(|id| format!("mfg:execution:{id}"))
        }
        R::CockpitProfileUpsert => find_json_string(body, "profile_id")
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("mfg:cockpit-profile:{id}"))
            .or_else(|| {
                idempotency_key.map(|key| {
                    format!(
                        "mfg:cockpit-profile:{}",
                        stable_mfg_resource_id("cockpit-profile", key)
                    )
                })
            }),
        R::CockpitProfileDelete | R::CockpitProfileShare => {
            path_id("/profiles/").map(|id| format!("mfg:cockpit-profile:{id}"))
        }
        R::CockpitProfileClone => find_json_string(body, "profile_id")
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("mfg:cockpit-profile:{id}"))
            .or_else(|| {
                idempotency_key.map(|key| {
                    format!(
                        "mfg:cockpit-profile:{}",
                        stable_mfg_resource_id("cockpit-profile", key)
                    )
                })
            }),
        R::ReportGenerate => path_id("/profiles/").map(|id| format!("mfg:cockpit-profile:{id}")),
        R::ReportScheduleRun => Some("mfg:cockpit-report-schedule:due".to_string()),
        R::ReportDeliver | R::ReportDeliveryRetry | R::ReportReviewRequest => {
            path_id("/reports/").map(|id| format!("mfg:cockpit-report:{id}"))
        }
        R::RealityEvidenceQualityGate => {
            path_id("/evidence/").map(|id| format!("mfg:evidence:{id}"))
        }
        R::IncidentSkillRun => path_id("/incidents/").and_then(|incident_id| {
            request_path
                .split("/skills/")
                .nth(1)
                .and_then(|suffix| suffix.split('/').next())
                .filter(|id| !id.trim().is_empty())
                .map(|skill_id| format!("mfg:incident:{incident_id}:skill:{skill_id}"))
        }),
        R::RealitySourcePackUpsert => {
            find_json_string(body, "source_pack_id").map(|id| format!("matrix:source_pack:{id}"))
        }
        R::RealitySourcePackValidate
        | R::RealitySourcePackIngestFile
        | R::RealitySourcePackDeltaPlan
        | R::RealityConnectorRunPlan
        | R::RealityConnectorRunExecute => {
            path_id("/source-packs/").map(|id| format!("matrix:source_pack:{id}"))
        }
        R::RealityDataPlaneIngestPlan => Some("matrix:data-plane:ingest-plan".to_string()),
        R::RealityMetricAttentionPlan => Some("matrix:metrics:attention-plan".to_string()),
        R::RealityMetricSnapshotMaterialize => {
            Some("matrix:metric-snapshot:materialize".to_string())
        }
        R::RealityMetricRecompute => Some("matrix:metrics".to_string()),
        R::RealityMetricDependencyUpsert => find_json_string(body, "dependency_id")
            .map(|id| format!("matrix:metric_dependency:{id}"))
            .or_else(|| {
                Some(format!(
                    "matrix:metric_dependency:{}:{}:{}",
                    find_json_string(body, "upstream_metric_id")?,
                    find_json_string(body, "downstream_metric_id")?,
                    find_json_string(body, "dependency_type")?,
                ))
            }),
        R::RealityEntityUpsert => find_json_string(body, "entity_id")
            .map(|id| format!("matrix:entity:{id}"))
            .or_else(|| {
                Some(format!(
                    "matrix:entity:{}:{}",
                    find_json_string(body, "entity_type")?,
                    find_json_string(body, "canonical_key")?,
                ))
            }),
        R::RealityRelationUpsert => find_json_string(body, "relation_id")
            .map(|id| format!("matrix:relation:{id}"))
            .or_else(|| {
                Some(format!(
                    "matrix:relation:{}:{}:{}",
                    find_json_string(body, "relation_type")?,
                    find_json_string(body, "from_entity_id")?,
                    find_json_string(body, "to_entity_id")?,
                ))
            }),
        R::PlaybookUpsert => {
            find_json_string(body, "playbook_id").map(|id| format!("mfg:playbook:{id}"))
        }
        R::RealityMetricDependencyAffectedPlan => find_json_string(body, "fact_type")
            .map(|fact_type| format!("matrix:fact-type:{fact_type}:metric-impact")),
        R::RealityComputeJobPlan => find_json_string(body, "job_id")
            .map(|id| format!("matrix:compute-job:{id}"))
            .or_else(|| Some("matrix:compute-job:new".to_string())),
        R::RealityComputeJobExecute => {
            path_id("/compute/jobs/").map(|id| format!("matrix:compute-job:{id}"))
        }
        R::RealityEntityResolveSourceKey => find_json_string(body, "source_system")
            .zip(find_json_string(body, "source_key"))
            .map(|(source_system, source_key)| {
                format!("matrix:source-key:{source_system}:{source_key}")
            }),
        R::RealityEntityMatchCandidate => Some("matrix:entity-match-candidate:preview".to_string()),
        R::RealityEntityConflictDecision => find_json_string(body, "candidate_id")
            .map(|id| format!("matrix:entity-match-candidate:{id}")),
        R::RealityFactIngest => Some("matrix:facts:ingest".to_string()),
        R::RealityEvidenceBuild => Some("matrix:evidence:new".to_string()),
        R::DomainServerManufacturingSeed => Some("mfg:domain:server-manufacturing".to_string()),
        R::OntologyServerManufacturingSeed => Some("mfg:ontology:server-manufacturing".to_string()),
        R::ReportReviewDecide => request_path
            .split("/report-reviews/")
            .nth(1)
            .and_then(|suffix| suffix.split('/').next())
            .filter(|id| !id.trim().is_empty())
            .map(|id| format!("mfg:report-review:{id}")),
        _ => None,
    };
    identified.unwrap_or_else(|| format!("mfg:http:{request_path}"))
}

fn parse_mfg_query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                decode_mfg_query_component(name),
                decode_mfg_query_component(value),
            )
        })
        .collect()
}

fn decode_mfg_query_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = (bytes[index + 1] as char).to_digit(16);
                let low = (bytes[index + 2] as char).to_digit(16);
                if let (Some(high), Some(low)) = (high, low) {
                    decoded.push(((high << 4) | low) as u8);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn find_json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(object) => object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                object
                    .values()
                    .find_map(|value| find_json_string(value, key))
            }),
        serde_json::Value::Array(items) => {
            items.iter().find_map(|value| find_json_string(value, key))
        }
        _ => None,
    }
}

fn find_json_u64(value: &serde_json::Value, key: &str) -> Option<u64> {
    match value {
        serde_json::Value::Object(object) => object
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .or_else(|| object.values().find_map(|value| find_json_u64(value, key))),
        serde_json::Value::Array(items) => items.iter().find_map(|value| find_json_u64(value, key)),
        _ => None,
    }
}

fn find_json_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
    match value {
        serde_json::Value::Object(object) => object
            .get(key)
            .and_then(serde_json::Value::as_bool)
            .or_else(|| object.values().find_map(|value| find_json_bool(value, key))),
        serde_json::Value::Array(items) => {
            items.iter().find_map(|value| find_json_bool(value, key))
        }
        _ => None,
    }
}

fn canonicalize_mfg_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let sorted = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_mfg_json(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            serde_json::to_value(sorted).unwrap_or(serde_json::Value::Null)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonicalize_mfg_json).collect())
        }
        _ => value.clone(),
    }
}

fn remove_json_field(value: &mut serde_json::Value, key: &str) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove(key);
            for value in object.values_mut() {
                remove_json_field(value, key);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                remove_json_field(item, key);
            }
        }
        _ => {}
    }
}

fn attach_mfg_receipt(response: &mut serde_json::Value, receipt: &app_mfg_contract::MfgReceiptV1) {
    if let Some(object) = response.as_object_mut() {
        let canonical = serde_json::to_value(receipt).unwrap_or(serde_json::Value::Null);
        object.insert("_mfg_receipt".to_string(), canonical.clone());
        object.insert("receipt".to_string(), canonical);
    }
}

fn json_response_with_receipt(
    status: StatusCode,
    response: serde_json::Value,
    receipt: &app_mfg_contract::MfgReceiptV1,
) -> Response {
    let mut response = (status, Json(response)).into_response();
    if let Ok(value) = axum::http::HeaderValue::from_bytes(receipt.receipt_id.as_bytes()) {
        response
            .headers_mut()
            .insert("x-cowd-mfg-receipt-id", value);
    }
    response
}

fn mfg_error_response(
    status: StatusCode,
    code: app_mfg_contract::MfgErrorCode,
    message: String,
    retryable: bool,
) -> Response {
    (
        status,
        Json(app_mfg_contract::MfgApiErrorV1 {
            code,
            message,
            http_status: status.as_u16(),
            details: serde_json::Value::Null,
            retryable,
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            recovery_actions: if retryable {
                vec![app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
                    label: "Retry the same intent".to_string(),
                    target: None,
                    enabled: true,
                }]
            } else {
                Vec::new()
            },
            request_id: None,
            receipt_ref: None,
        }),
    )
        .into_response()
}

pub(super) fn require_mfg_capability(
    principal: &AuthenticatedPrincipal,
    capability: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal
        .0
        .claims()
        .capabilities
        .iter()
        .any(|granted| granted == capability)
    {
        Ok(())
    } else {
        Err(mfg_api_error(
            StatusCode::FORBIDDEN,
            format!("required capability is not granted: {capability}"),
        ))
    }
}

pub(super) fn mfg_api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<ErrorResponse>) {
    use app_mfg_contract::{MfgErrorCode, MfgRecoveryAction, MfgRecoveryActionKind};

    let message = message.into();
    let (code, retryable, recovery_actions) = match status {
        StatusCode::UNAUTHORIZED => (
            MfgErrorCode::AuthenticationRequired,
            false,
            vec![MfgRecoveryAction {
                kind: MfgRecoveryActionKind::RequestAccess,
                label: "Authenticate again".to_string(),
                target: Some("/api/auth/login".to_string()),
                enabled: true,
            }],
        ),
        StatusCode::FORBIDDEN => (
            MfgErrorCode::CapabilityDenied,
            false,
            vec![MfgRecoveryAction {
                kind: MfgRecoveryActionKind::RequestAccess,
                label: "Request access".to_string(),
                target: None,
                enabled: true,
            }],
        ),
        StatusCode::NOT_FOUND => (MfgErrorCode::ScopeNotFound, false, Vec::new()),
        StatusCode::CONFLICT => (
            MfgErrorCode::RevisionConflict,
            false,
            vec![
                MfgRecoveryAction {
                    kind: MfgRecoveryActionKind::Compare,
                    label: "Compare changes".to_string(),
                    target: None,
                    enabled: true,
                },
                MfgRecoveryAction {
                    kind: MfgRecoveryActionKind::Reload,
                    label: "Reload current state".to_string(),
                    target: None,
                    enabled: true,
                },
            ],
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            MfgErrorCode::RateLimited,
            true,
            vec![MfgRecoveryAction {
                kind: MfgRecoveryActionKind::RetrySameIntent,
                label: "Retry the same intent".to_string(),
                target: None,
                enabled: true,
            }],
        ),
        status if status.is_client_error() => (MfgErrorCode::ValidationFailed, false, Vec::new()),
        _ => (
            MfgErrorCode::Internal,
            true,
            vec![MfgRecoveryAction {
                kind: MfgRecoveryActionKind::Reload,
                label: "Reload".to_string(),
                target: None,
                enabled: true,
            }],
        ),
    };
    let error = app_mfg_contract::MfgApiErrorV1 {
        code,
        message,
        http_status: status.as_u16(),
        details: serde_json::Value::Null,
        retryable,
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        recovery_actions,
        request_id: None,
        receipt_ref: None,
    };
    let encoded = serde_json::to_string(&error).unwrap_or_else(|_| {
        "{\"code\":\"internal\",\"message\":\"serialization failed\"}".to_string()
    });
    (
        status,
        Json(ErrorResponse {
            error: format!("__mfg_api_error_v1__:{encoded}"),
        }),
    )
}

pub(super) fn mfg_cross_plane_error(
    error: runtime::CrossPlaneRuntimeError,
) -> (StatusCode, Json<ErrorResponse>) {
    let status = if matches!(
        error,
        runtime::CrossPlaneRuntimeError::IdempotencyConflict(_)
    ) {
        StatusCode::CONFLICT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    mfg_api_error(status, error.to_string())
}

pub(super) fn mfg_cross_plane_graph_error(
    error: crate::services::CrossPlaneCommitGraphError,
) -> (StatusCode, Json<ErrorResponse>) {
    let status = match &error {
        crate::services::CrossPlaneCommitGraphError::CanonicalActionConflict(_) => {
            StatusCode::CONFLICT
        }
        crate::services::CrossPlaneCommitGraphError::Runtime(
            runtime::CrossPlaneRuntimeError::IdempotencyConflict(_),
        ) => StatusCode::CONFLICT,
        crate::services::CrossPlaneCommitGraphError::Runtime(_)
        | crate::services::CrossPlaneCommitGraphError::State(_)
        | crate::services::CrossPlaneCommitGraphError::Execution(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    mfg_api_error(status, error.to_string())
}

pub(super) fn mfg_typed_api_error(
    error: app_mfg_contract::MfgApiErrorV1,
) -> (StatusCode, Json<ErrorResponse>) {
    let status =
        StatusCode::from_u16(error.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let encoded = serde_json::to_string(&error).unwrap_or_else(|_| {
        "{\"code\":\"internal\",\"message\":\"failed to serialize typed MFG error\"}".to_string()
    });
    (
        status,
        Json(ErrorResponse {
            error: format!("__mfg_api_error_v1__:{encoded}"),
        }),
    )
}

fn mfg_mutation_error(error: MfgRepositoryError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        MfgRepositoryError::NotFound(message) => mfg_api_error(StatusCode::NOT_FOUND, message),
        MfgRepositoryError::CommandRejected(message)
            if message.to_ascii_lowercase().contains("idempotency") =>
        {
            mfg_typed_api_error(app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::IdempotencyConflict,
                message,
                http_status: StatusCode::CONFLICT.as_u16(),
                details: serde_json::Value::Null,
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::Compare,
                    label: "Use the original intent or create a new governed intent".to_string(),
                    target: None,
                    enabled: true,
                }],
                request_id: None,
                receipt_ref: None,
            })
        }
        conflict @ (MfgRepositoryError::RevisionConflict { .. }
        | MfgRepositoryError::CommandRejected(_)) => {
            mfg_api_error(StatusCode::CONFLICT, conflict.to_string())
        }
        other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

async fn mfg_app_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(state.services.mfg.app_descriptor())
}

async fn mfg_contract_handler(
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Json<app_mfg_contract::MfgFrontendContractV1> {
    use app_mfg_contract::{
        MfgActionAvailability, MfgConsumer, MfgFrontendContractV1, MfgSurfaceContract,
        MfgSurfaceKind, MfgSurfaceRole,
    };

    let routes = app_mfg_contract::mfg_route_contracts();
    let actions = app_mfg_contract::mfg_action_contracts();
    let active_route_count = routes
        .iter()
        .filter(|route| route.availability == MfgActionAvailability::Active)
        .count();
    let planned_route_count = routes.len().saturating_sub(active_route_count);
    let webui_routes = routes
        .iter()
        .filter(|route| {
            route.availability == MfgActionAvailability::Active
                && route.consumers.contains(&MfgConsumer::Webui)
        })
        .map(|route| route.route_id)
        .collect::<Vec<_>>();
    let webui_route_set = webui_routes
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let cli_routes = vec![
        app_mfg_contract::MfgRouteId::ContractGet,
        app_mfg_contract::MfgRouteId::AppGet,
    ];
    let tui_routes = app_mfg_contract::mfg_tui_route_contracts()
        .into_iter()
        .map(|route| route.route_id)
        .collect::<Vec<_>>();
    let tui_actions = app_mfg_contract::mfg_tui_action_contracts()
        .into_iter()
        .map(|action| action.action_id)
        .collect::<Vec<_>>();
    let webui_actions = actions
        .iter()
        .filter(|action| {
            action.availability == MfgActionAvailability::Active
                && webui_route_set.contains(&action.route_id)
        })
        .map(|action| action.action_id)
        .collect();

    Json(MfgFrontendContractV1 {
        kind: "mfg.frontend_contract".to_string(),
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        generated_at: chrono::Utc::now(),
        app_id: "mfg.manufacturing".to_string(),
        active_route_count,
        planned_route_count,
        routes,
        actions,
        surfaces: vec![
            MfgSurfaceContract {
                surface: MfgSurfaceKind::Management,
                role: MfgSurfaceRole::EnhancedManagement,
                entrypoints: vec![
                    "/api/apps/mfg/app".to_string(),
                    "/api/apps/mfg/contract".to_string(),
                ],
                routes: webui_routes,
                actions: webui_actions,
            },
            MfgSurfaceContract {
                surface: MfgSurfaceKind::Tui,
                role: MfgSurfaceRole::ConsoleOperationalControl,
                entrypoints: vec![
                    "/api/apps/mfg/app".to_string(),
                    "/api/apps/mfg/contract".to_string(),
                    "/mfg".to_string(),
                ],
                routes: tui_routes,
                actions: tui_actions,
            },
            MfgSurfaceContract {
                surface: MfgSurfaceKind::Cli,
                role: MfgSurfaceRole::MinimalCoreControl,
                entrypoints: vec![
                    "/api/apps/mfg/app".to_string(),
                    "/api/apps/mfg/contract".to_string(),
                ],
                routes: cli_routes,
                actions: Vec::new(),
            },
        ],
        granted_capabilities: principal.0.claims().capabilities.clone(),
    })
}

pub(super) fn mfg_idempotency_key(
    headers: &HeaderMap,
    legacy_value: Option<String>,
) -> Result<String, MfgIdempotencyKeyError> {
    let header_value = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let legacy_value = legacy_value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    match (header_value, legacy_value) {
        (Some(header), Some(legacy)) if header != legacy => Err(MfgIdempotencyKeyError {
            message: "Idempotency-Key header conflicts with legacy body/query idempotency_key"
                .to_string(),
        }),
        (Some(header), _) => Ok(header),
        (None, Some(legacy)) => Ok(legacy),
        (None, None) => Err(MfgIdempotencyKeyError {
            message: "Idempotency-Key header is required for this MFG mutation".to_string(),
        }),
    }
}

#[derive(Debug, Clone)]
pub(super) struct MfgIdempotencyKeyError {
    pub(super) message: String,
}

pub(super) fn stable_mfg_resource_id(prefix: &str, idempotency_key: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{prefix}:{idempotency_key}").as_bytes());
    format!("{prefix}-{digest:x}")[..prefix.len() + 1 + 20].to_string()
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MfgExecutionFeedbackRequest {
    outcome: String,
    note: String,
    #[serde(default)]
    metric_delta: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgCaseSearchQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgPlaybookUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    playbook: MfgPlaybook,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgPlaybookRecommendRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgSkillPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgSkillRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MfgCockpitReportDeliveryRetryRequest {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    expected_revision: Option<u64>,
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

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct MfgReportReviewListQuery {
    #[serde(default)]
    report_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct MatrixDecisionTraceQuery {
    #[serde(default)]
    incident_id: Option<String>,
    #[serde(default)]
    report_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityFactIngestRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityEntityUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    entity: MatrixEntityInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityEntityResolveSourceKeyRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    source_system: String,
    source_key: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityEntityMatchCandidateRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    left_entity_id: String,
    right_entity_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityRelationUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    relation: MatrixRelationInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityMetricDependencyUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    dependency: MatrixMetricDependencyInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityMetricSnapshotMaterializeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    metric_ids: Vec<String>,
    #[serde(default)]
    scope_ref: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityAffectedByFactTypeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    fact_type: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityComputeJobPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    job: MatrixComputeJobInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealityDataPlaneIngestPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    ingest: MatrixDataPlaneIngestPlanInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealitySourcePackUpsertRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    source_pack: MatrixSourcePack,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MfgRealitySourcePackIngestFileRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
}

#[derive(Debug, Deserialize, JsonSchema)]
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let store_path = state
        .services
        .matrix
        .store_path(&state.config_home)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    let outcome = state
        .services
        .matrix
        .upsert_source_pack_checked(
            &state.config_home,
            request.source_pack,
            request.expected_revision,
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.source_pack",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack": outcome.resource,
        "created": outcome.created,
        "previous_revision": outcome.previous_revision,
        "revision": outcome.revision,
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG Reality source pack not found"))?;
    let revision = state
        .services
        .matrix
        .resource_revision(
            &state.config_home,
            "source_pack",
            &source_pack.source_pack_id,
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.source_pack",
        "source_pack": source_pack,
        "revision": revision,
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
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            mfg_api_error(StatusCode::NOT_FOUND, "MFG Reality connector run not found")
        })?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if states.is_empty() {
        return Err(mfg_api_error(
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut dependency_revisions = std::collections::BTreeMap::new();
    for dependency in lineage
        .upstream_dependencies
        .iter()
        .chain(lineage.downstream_dependencies.iter())
    {
        dependency_revisions.insert(
            dependency.dependency_id.clone(),
            state
                .services
                .matrix
                .resource_revision(
                    &state.config_home,
                    "metric_dependency",
                    &dependency.dependency_id,
                )
                .map_err(matrix_error)?,
        );
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metric.lineage",
        "schema_version": "matrix.metric_lineage.v1",
        "lineage": lineage,
        "dependency_revisions": dependency_revisions,
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        return Err(mfg_api_error(
            StatusCode::BAD_REQUEST,
            "at least one metric_id is required",
        ));
    }
    let snapshot = state
        .services
        .matrix
        .materialize_metric_snapshot(&state.config_home, request.metric_ids, request.scope_ref)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    let outcome = state
        .services
        .matrix
        .upsert_metric_dependency_checked(
            &state.config_home,
            &MatrixMetricDependency::from_input(request.dependency),
            request.expected_revision,
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.metric_dependency",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "dependency": outcome.resource,
        "created": outcome.created,
        "previous_revision": outcome.previous_revision,
        "revision": outcome.revision,
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG Reality compute job not found"))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut revisions = std::collections::BTreeMap::new();
    for entity in &entities {
        revisions.insert(
            entity.entity_id.clone(),
            state
                .services
                .matrix
                .resource_revision(&state.config_home, "entity", &entity.entity_id)
                .map_err(matrix_error)?,
        );
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entities",
        "entities": entities,
        "revisions": revisions,
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG Reality entity not found"))?;
    let revision = state
        .services
        .matrix
        .resource_revision(&state.config_home, "entity", &entity.entity_id)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity",
        "entity": entity,
        "revision": revision,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_entity_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityEntityUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let outcome = state
        .services
        .matrix
        .upsert_entity_checked(
            &state.config_home,
            &MatrixEntity::from_input(request.entity),
            request.expected_revision,
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "entity": outcome.resource,
        "created": outcome.created,
        "previous_revision": outcome.previous_revision,
        "revision": outcome.revision,
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            mfg_api_error(
                StatusCode::NOT_FOUND,
                "MFG Reality entity source key not found",
            )
        })?;
    let revision = state
        .services
        .matrix
        .resource_revision(&state.config_home, "entity", &entity.entity_id)
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.resolution",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_system": request.source_system,
        "source_key": request.source_key,
        "entity": entity,
        "revision": revision,
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
    let mut revisions = std::collections::BTreeMap::new();
    for relation in &relations {
        revisions.insert(
            relation.relation_id.clone(),
            state
                .services
                .matrix
                .resource_revision(&state.config_home, "relation", &relation.relation_id)
                .map_err(matrix_error)?,
        );
    }
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.entity.relations",
        "schema_version": "matrix.entity_relations.v1",
        "entity_id": id,
        "relations": relations,
        "revisions": revisions,
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
    let outcome = state
        .services
        .matrix
        .upsert_relation_checked(
            &state.config_home,
            &MatrixRelation::from_input(request.relation),
            request.expected_revision,
        )
        .map_err(matrix_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.reality.relation",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "relation": outcome.resource,
        "created": outcome.created,
        "previous_revision": outcome.previous_revision,
        "revision": outcome.revision,
        "boundary": mfg_reality_boundary(),
    })))
}

async fn mfg_reality_fact_ingest_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MfgRealityFactIngestRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if request.facts.is_empty() {
        return Err(mfg_api_error(
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
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        append_matrix_execution_outcome(&state, session_id.as_deref(), matrix_fact_outcome(&fact))
            .await
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
    .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            mfg_api_error(
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
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = mfg_idempotency_key(&headers, None)
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    let gate_id = stable_mfg_resource_id("quality-gate", &idempotency_key);
    let gate = state
        .services
        .matrix
        .evaluate_evidence_quality_with_gate_id(&state.config_home, &id, &gate_id)
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            mfg_api_error(StatusCode::NOT_FOUND, "MFG Reality quality gate not found")
        })?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            mfg_api_error(
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
        MatrixStoreError::NotFound(message) => mfg_api_error(StatusCode::NOT_FOUND, message),
        conflict @ MatrixStoreError::RevisionConflict { .. } => {
            mfg_api_error(StatusCode::CONFLICT, conflict.to_string())
        }
        other => mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
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
    Query(query): Query<app_mfg_contract::MfgIncidentListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let incidents = state
        .services
        .mfg
        .list_incidents(&state.config_home, query.limit.unwrap_or(50).clamp(1, 500))
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
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
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.ontology_seed",
        "pack": pack,
    })))
}

async fn mfg_execution_feedback_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MfgExecutionFeedbackRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let actor = principal_actor_id(&principal);
    let execution = state
        .services
        .mfg
        .record_execution_feedback(
            &state.config_home,
            &id,
            MfgActionFeedback::new_attributed(
                request.outcome,
                request.note,
                request.metric_delta,
                actor,
            ),
        )
        .map_err(mfg_mutation_error)?;
    Ok(Json(serde_json::json!({
        "kind": "mfg.action_execution",
        "execution": execution,
    })))
}

#[cfg(test)]
mod tests {
    use super::{
        mfg_api_error, mfg_contract_handler, mfg_idempotency_key, normalize_mfg_action_mode,
        parse_mfg_query_pairs, resolve_mfg_action_id, resolve_mfg_resource_ref,
        MfgActionExecutionIntent, MfgCockpitReportDeliveryIntent,
        MfgCockpitReportDeliveryRetryRequest, MfgCrossPlaneBridgeIntent,
        MfgExecutionFeedbackRequest,
    };
    use axum::{
        extract::Extension,
        http::{HeaderMap, HeaderValue, StatusCode},
    };

    #[tokio::test]
    async fn tui_contract_is_operational_with_exact_derived_route_and_action_inventories() {
        let principal = super::super::AuthenticatedPrincipal(super::super::test_human_principal());
        let expected_capabilities = principal.0.claims().capabilities.clone();
        let contract = mfg_contract_handler(Extension(principal)).await.0;
        let surface = contract
            .surfaces
            .iter()
            .find(|surface| surface.surface == app_mfg_contract::MfgSurfaceKind::Tui)
            .expect("TUI surface");
        let expected_routes = app_mfg_contract::mfg_tui_route_contracts()
            .into_iter()
            .map(|route| route.route_id)
            .collect::<std::collections::BTreeSet<_>>();
        let expected_actions = app_mfg_contract::mfg_tui_action_contracts()
            .into_iter()
            .map(|action| action.action_id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            surface.role,
            app_mfg_contract::MfgSurfaceRole::ConsoleOperationalControl
        );
        assert_eq!(
            surface
                .routes
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected_routes
        );
        assert_eq!(
            surface
                .actions
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected_actions
        );
        assert_eq!(contract.granted_capabilities, expected_capabilities);
        assert_eq!(
            super::mfg_request_schema_component(app_mfg_contract::MfgRouteId::IncidentList),
            "MfgIncidentListQuery"
        );
    }

    #[test]
    fn mfg_effect_intents_reject_client_supplied_actor_principals() {
        let bridge = serde_json::from_str::<MfgCrossPlaneBridgeIntent>(
            r#"{"actor_principal":"user:forged","mode":"dry_run"}"#,
        );
        let delivery = serde_json::from_str::<MfgCockpitReportDeliveryIntent>(
            r#"{"actor_principal":"user:forged","mode":"dry_run"}"#,
        );
        let action = serde_json::from_str::<MfgActionExecutionIntent>(
            r#"{"operator_id":"user:forged","mode":"commit"}"#,
        );
        let feedback = serde_json::from_str::<MfgExecutionFeedbackRequest>(
            r#"{"outcome":"resolved","note":"forged","actor_ref":"user:forged"}"#,
        );

        assert!(bridge.is_err());
        assert!(delivery.is_err());
        assert!(action.is_err());
        assert!(feedback.is_err());
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

        let action: MfgActionExecutionIntent =
            serde_json::from_str(r#"{"mode":"dry_run","note":"inspect only"}"#)
                .expect("valid MFG action intent");
        let request = action.into_request("principal:verified-human".to_string());
        assert_eq!(
            request.operator_id.as_deref(),
            Some("principal:verified-human")
        );
    }

    #[test]
    fn capability_denial_and_scope_hiding_use_distinct_typed_errors() {
        let (_, axum::Json(capability)) =
            mfg_api_error(StatusCode::FORBIDDEN, "mfg.report.deliver is required");
        let (_, axum::Json(scope)) =
            mfg_api_error(StatusCode::NOT_FOUND, "resource is outside verified scope");
        let capability = serde_json::to_value(capability).expect("capability error");
        let scope = serde_json::to_value(scope).expect("scope error");
        assert_eq!(capability["code"], "capability_denied");
        assert_eq!(capability["http_status"], 403);
        assert_eq!(scope["code"], "scope_not_found");
        assert_eq!(scope["http_status"], 404);
        for (status, expected_code) in [
            (StatusCode::BAD_REQUEST, "validation_failed"),
            (StatusCode::UNAUTHORIZED, "authentication_required"),
            (StatusCode::CONFLICT, "revision_conflict"),
            (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
            (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        ] {
            let (_, axum::Json(error)) = mfg_api_error(status, "fixture");
            let error = serde_json::to_value(error).expect("typed MFG error");
            assert_eq!(error["code"], expected_code);
            assert_eq!(error["http_status"], status.as_u16());
        }
    }

    #[test]
    fn idempotency_header_is_canonical_and_conflicting_legacy_value_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", HeaderValue::from_static("intent-1"));
        assert_eq!(
            mfg_idempotency_key(&headers, None).unwrap(),
            "intent-1".to_string()
        );
        assert!(mfg_idempotency_key(&headers, Some("intent-2".to_string())).is_err());
        assert_eq!(
            mfg_idempotency_key(&HeaderMap::new(), Some("legacy-intent".to_string())).unwrap(),
            "legacy-intent".to_string()
        );
    }

    #[test]
    fn mutation_query_context_is_percent_decoded_before_identity_and_revision_checks() {
        let pairs = parse_mfg_query_pairs(
            "expected_revision=7&idempotency_key=webui-mfg%3Amfg.cockpit.profile.delete%3Aabc",
        );
        assert_eq!(
            pairs,
            vec![
                ("expected_revision".to_string(), "7".to_string()),
                (
                    "idempotency_key".to_string(),
                    "webui-mfg:mfg.cockpit.profile.delete:abc".to_string()
                ),
            ]
        );
    }

    #[test]
    fn every_active_mutation_route_resolves_a_domain_resource_not_an_http_fallback() {
        let fixture = serde_json::json!({
            "source_pack_id": "source-1",
            "dependency_id": "dependency-1",
            "entity_id": "entity-1",
            "relation_id": "relation-1",
            "playbook_id": "playbook-1",
            "profile_id": "profile-1",
            "rule_id": "rule-1",
            "subscription_id": "subscription-1",
            "assignment_id": "assignment-1",
            "fact_type": "fact-1",
            "job_id": "job-1",
            "candidate_id": "candidate-1",
            "source_system": "erp",
            "source_key": "part-1",
            "command": "complete",
            "decision": "resolve",
            "mode": "commit",
            "deliver": true
        });
        for route in app_mfg_contract::mfg_route_contracts()
            .into_iter()
            .filter(|route| route.availability == app_mfg_contract::MfgActionAvailability::Active)
            .filter(|route| route.class != app_mfg_contract::MfgMutationClass::Read)
        {
            let path = route
                .path
                .replace(":analysis_id", "analysis-1")
                .replace(":action_id", "action-1")
                .replace(":skill_id", "skill-1")
                .replace(":instance_id", "instance-1")
                .replace(":id", "object-1");
            let resource =
                resolve_mfg_resource_ref(route.route_id, &path, &fixture, Some("intent-1"));
            assert!(
                !resource.starts_with("mfg:http:"),
                "{} fell back to transport identity: {resource}",
                route.route_id.as_str()
            );
        }
    }

    #[test]
    fn multi_action_and_resource_resolution_is_derived_from_canonical_request_fields() {
        let create = serde_json::json!({
            "entity": {
                "entity_type": "part",
                "canonical_key": "gpu"
            }
        });
        let update = serde_json::json!({
            "expected_revision": 3,
            "entity": {
                "entity_id": "entity-gpu",
                "entity_type": "part",
                "canonical_key": "gpu"
            }
        });
        assert_eq!(
            resolve_mfg_action_id(app_mfg_contract::MfgRouteId::RealityEntityUpsert, &create),
            "mfg.reality.entity.create"
        );
        assert_eq!(
            resolve_mfg_action_id(app_mfg_contract::MfgRouteId::RealityEntityUpsert, &update),
            "mfg.reality.entity.update"
        );
        assert_eq!(
            resolve_mfg_resource_ref(
                app_mfg_contract::MfgRouteId::RealityEntityUpsert,
                "/api/apps/mfg/reality/entities/upsert",
                &create,
                None,
            ),
            "matrix:entity:part:gpu"
        );
        assert_eq!(
            resolve_mfg_resource_ref(
                app_mfg_contract::MfgRouteId::RealityEntityUpsert,
                "/api/apps/mfg/reality/entities/upsert",
                &update,
                None,
            ),
            "matrix:entity:entity-gpu"
        );
        assert_eq!(
            resolve_mfg_action_id(
                app_mfg_contract::MfgRouteId::ReportScheduleRun,
                &serde_json::json!({"deliver": true}),
            ),
            "mfg.report.schedule.generate_and_deliver"
        );
        for (route, dry_run, commit) in [
            (
                app_mfg_contract::MfgRouteId::AnalysisActionExecute,
                "mfg.analysis.action.dry_run",
                "mfg.analysis.action.commit",
            ),
            (
                app_mfg_contract::MfgRouteId::ExecutionCrossPlaneExecute,
                "mfg.execution.cross_plane.dry_run",
                "mfg.execution.cross_plane.commit",
            ),
            (
                app_mfg_contract::MfgRouteId::ReportDeliver,
                "mfg.report.deliver.dry_run",
                "mfg.report.deliver.commit",
            ),
            (
                app_mfg_contract::MfgRouteId::ReportDeliveryRetry,
                "mfg.report.delivery.retry_dry_run",
                "mfg.report.delivery.retry_commit",
            ),
        ] {
            assert_eq!(
                resolve_mfg_action_id(route, &serde_json::json!({"mode": "dry_run"})),
                dry_run
            );
            assert_eq!(
                resolve_mfg_action_id(route, &serde_json::json!({"mode": "commit"})),
                commit
            );
            assert_eq!(
                resolve_mfg_action_id(route, &serde_json::json!({"mode": " PlAn "})),
                dry_run
            );
            assert_eq!(
                resolve_mfg_action_id(route, &serde_json::json!({"mode": " ExEcUtE "})),
                commit
            );
        }
        assert!(normalize_mfg_action_mode("unknown").is_err());
        assert_eq!(
            resolve_mfg_action_id(
                app_mfg_contract::MfgRouteId::ReportScheduleRun,
                &serde_json::json!({"deliver": false}),
            ),
            "mfg.report.schedule.generate_only"
        );
        assert_eq!(
            resolve_mfg_action_id(
                app_mfg_contract::MfgRouteId::IncidentSkillRun,
                &serde_json::json!({}),
            ),
            "mfg.skill.run"
        );
        assert_eq!(
            resolve_mfg_resource_ref(
                app_mfg_contract::MfgRouteId::AnalysisActionExecute,
                "/api/apps/mfg/analyses/analysis-1/actions/action-1/execute",
                &serde_json::json!({"mode": "commit", "expected_revision": 4}),
                None,
            ),
            "mfg:analysis:analysis-1:action:action-1"
        );
        for (command, expected) in [
            ("acknowledge", "mfg.alert.acknowledge"),
            ("snooze", "mfg.alert.snooze"),
            ("resolve", "mfg.alert.resolve"),
            ("escalate", "mfg.alert.escalate"),
        ] {
            assert_eq!(
                resolve_mfg_action_id(
                    app_mfg_contract::MfgRouteId::AlertCommand,
                    &serde_json::json!({"command": command, "expected_revision": 2}),
                ),
                expected
            );
        }
        assert_eq!(
            resolve_mfg_action_id(
                app_mfg_contract::MfgRouteId::AssignmentUpsert,
                &serde_json::json!({"assignment": {"assignment_id": "assignment-1"}}),
            ),
            "mfg.assignment.create"
        );
        assert_eq!(
            resolve_mfg_action_id(
                app_mfg_contract::MfgRouteId::AssignmentUpsert,
                &serde_json::json!({
                    "assignment": {
                        "assignment_id": "assignment-1",
                        "expected_revision": 3
                    }
                }),
            ),
            "mfg.assignment.update"
        );
        for (decision, expected) in [
            ("force_retry", "mfg.report.review.force_retry"),
            ("reroute", "mfg.report.review.reroute"),
            ("abandon", "mfg.report.review.abandon"),
            ("resolve", "mfg.report.review.resolve"),
            ("reject", "mfg.report.review.reject"),
        ] {
            assert_eq!(
                resolve_mfg_action_id(
                    app_mfg_contract::MfgRouteId::ReportReviewDecide,
                    &serde_json::json!({"decision": decision, "expected_revision": 2}),
                ),
                expected
            );
        }
    }

    #[test]
    fn public_report_retry_rejects_legacy_force_bypass() {
        assert!(
            serde_json::from_value::<MfgCockpitReportDeliveryRetryRequest>(
                serde_json::json!({"mode": "commit", "force": true})
            )
            .is_err()
        );
    }
}
