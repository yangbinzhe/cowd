use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State as AxumState},
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
use serde::Deserialize;

use crate::services::GatewayMatrixRepositoryError as MatrixStoreError;

use super::matrix_outcomes::{
    append_matrix_execution_outcome, matrix_evidence_packet_outcome, matrix_fact_outcome,
    matrix_ingest_plan_outcome,
};
use super::{api_error, AppState, ErrorResponse};

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

fn matrix_health_capabilities() -> Vec<&'static str> {
    vec![
        "data_plane_adapter",
        "data_plane_ingest_plan",
        "data_plane_watermark",
        "data_plane_replay_policy",
        "connector_runtime",
        "connector_run_receipt",
        "connector_quality_report",
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
        "kind": "matrix.source_pack",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix source pack not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_pack",
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
        "kind": "matrix.source_pack.validation",
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
        "kind": "matrix.source_pack.delta_plan",
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
        "kind": "matrix.source_pack.ingest_file",
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
    Json(request): Json<MatrixConnectorRunRequest>,
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
        "kind": "matrix.connector_run.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "run": run,
    })))
}

async fn matrix_source_pack_connector_run_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MatrixConnectorRunRequest>,
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
        "kind": "matrix.connector_run",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix connector run not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.connector_run",
        "run": run,
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
        "kind": "matrix.entities",
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
        "kind": "matrix.entity.match_candidate",
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
        "kind": "matrix.entity.conflict_decision",
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
        "kind": "matrix.entity",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix entity source key not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entity.resolution",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix entity not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entity",
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
        "kind": "matrix.relation",
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
        "kind": "matrix.entity.relations",
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
        "kind": "matrix.entity.impact_path",
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
            "at least one Matrix fact is required",
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
        "kind": "matrix.fact.ingest",
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
        "kind": "matrix.metrics",
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
            "Matrix metric state not found",
        ));
    }
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric",
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
        "kind": "matrix.metric.lineage",
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
        "kind": "matrix.metric_attention.plan",
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
        "kind": "matrix.metric_snapshot",
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
        "kind": "matrix.metric_dependency",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "dependency": dependency,
    })))
}

async fn matrix_metric_affected_by_fact_type_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixAffectedByFactTypeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metric_ids = state
        .services
        .matrix
        .metrics_affected_by_fact_type(&state.config_home, &request.fact_type)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric_dependency.affected_by_fact_type",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "fact_type": request.fact_type,
        "metric_ids": metric_ids,
    })))
}

async fn matrix_compute_job_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixComputeJobPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let plan = state
        .services
        .matrix
        .plan_compute_job_for_fact_type(&state.config_home, request.job)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.compute.plan",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix compute job not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.compute.job",
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
        "kind": "matrix.compute.job",
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
        "kind": "matrix.metrics.recompute",
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
        "kind": "matrix.changes",
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
        "kind": "matrix.attention.hot",
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
        "kind": "matrix.evidence.packet",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.evidence.packet",
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
        "kind": "matrix.quality_gate",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix quality gate not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.quality_gate",
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
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix evidence packet not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.evidence.context_item",
        "context_item": state.services.context.structured_evidence_item(&packet),
    })))
}
