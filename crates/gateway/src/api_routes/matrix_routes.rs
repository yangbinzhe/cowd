use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, Query as AxumQuery, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use connector::SourceReadPlan;
use matrix_core::{
    MatrixComputeJobInput, MatrixConnectorRunInput, MatrixDataPlaneIngestPlanInput, MatrixEntity,
    MatrixEntityInput, MatrixFact, MatrixFactInput, MatrixMetricDependency,
    MatrixMetricDependencyInput, MatrixRelation, MatrixRelationInput, MatrixSourceKind,
    MatrixSourcePack, MatrixSourceSnapshotInput, MATRIX_SCHEMA_VERSION,
};
use serde::Deserialize;
use serde_json::Value;

use crate::services::GatewayMatrixRepositoryError as MatrixStoreError;

use super::matrix_outcomes::{
    append_matrix_execution_outcome, matrix_evidence_packet_outcome, matrix_fact_outcome,
    matrix_ingest_plan_outcome,
};
use super::{api_error, AppState, ErrorResponse};

mod entities;
mod evidence;
mod metrics;
mod source;

use entities::*;
use evidence::*;
use metrics::*;
use source::*;

pub(super) fn router() -> Router<Arc<AppState>> {
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
            "/api/matrix/source-packs/:id/snapshots/plan",
            post(matrix_source_snapshot_plan_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/snapshots/run",
            post(matrix_source_snapshot_run_handler),
        )
        .route(
            "/api/matrix/source-packs/:id/snapshots",
            get(matrix_source_pack_snapshots_handler),
        )
        .route(
            "/api/matrix/connector-runs/:id",
            get(matrix_connector_run_get_handler),
        )
        .route(
            "/api/matrix/source-snapshots/:id",
            get(matrix_source_snapshot_get_handler),
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
struct MatrixAffectedByFactTypeRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    fact_type: String,
}

#[derive(Debug, Deserialize)]
struct MatrixComputeJobPlanRequest {
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
struct MatrixConnectorRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    run: Option<MatrixConnectorRunInput>,
}

#[derive(Debug, Deserialize)]
struct MatrixSourceSnapshotPlanRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    resource_ref: Option<String>,
    #[serde(default)]
    estimated_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct MatrixSourceSnapshotRunRequest {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    snapshot: Option<MatrixSourceSnapshotInput>,
    #[serde(default)]
    source_read_plan: Option<SourceReadPlan>,
    #[serde(default)]
    rows: Vec<Value>,
    #[serde(default)]
    facts: Vec<MatrixFactInput>,
}

#[derive(Debug, Deserialize)]
struct MatrixSourceSnapshotListQuery {
    #[serde(default)]
    limit: Option<usize>,
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
        "source_snapshot_count": health.source_snapshot_count,
        "ontology_pack_count": health.ontology_pack_count,
        "entity_match_candidate_count": health.entity_match_candidate_count,
        "entity_conflict_decision_count": health.entity_conflict_decision_count,
        "metric_snapshot_count": health.metric_snapshot_count,
        "store": store_path,
        "capabilities": capabilities,
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
        "source_snapshot_plan",
        "source_snapshot_capture",
        "source_snapshot_apply",
        "source_snapshot_repository",
        "entity_match_candidate",
        "entity_conflict_decision",
        "entity_survivorship_rule",
        "entity_relation_network",
        "entity_source_key_resolution",
        "entity_impact_trace",
        "fact_ingest",
        "metric_state",
        "metric_recompute",
        "metric_attention_plan",
        "metric_attention_scoring",
        "metric_snapshot_materialize",
        "metric_dependency_graph",
        "metric_lineage",
        "fact_type_metric_impact",
        "incremental_metric_focus",
        "incremental_compute_job",
        "scoped_metric_recompute",
        "source_onboarding_pack",
        "source_pack_delta_plan",
        "change_event",
        "attention_hot",
        "evidence_packet_build",
        "evidence_packet_get",
        "evidence_context_item",
        "evidence_quality_gate",
        "insight_quality_gate",
    ]
}
