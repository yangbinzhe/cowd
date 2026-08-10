use super::*;

pub(super) async fn matrix_metrics_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metrics = matrix_call!(state, list_metric_definitions())
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metrics",
        "metrics": metrics,
    })))
}

pub(super) async fn matrix_metric_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lookup_id = id.clone();
    let states = matrix_call!(state, metric_states(&lookup_id))
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

pub(super) async fn matrix_metric_lineage_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lineage = matrix_call!(state, metric_lineage(&id, 6))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric.lineage",
        "lineage": lineage,
    })))
}

pub(super) async fn matrix_metric_attention_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixMetricAttentionPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let trigger_fact_type = request.trigger_fact_type.clone();
    let entity_scope = request.entity_scope.clone();
    let period = request.period.clone();
    let limit = request.limit.unwrap_or(12);
    let plan = matrix_call!(
        state,
        plan_metric_attention(&trigger_fact_type, entity_scope, period, limit)
    )
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric_attention.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
    })))
}

pub(super) async fn matrix_metric_snapshot_materialize_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixMetricSnapshotMaterializeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if request.metric_ids.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "at least one metric_id is required",
        ));
    }
    let metric_ids = request.metric_ids.clone();
    let scope_ref = request.scope_ref.clone();
    let snapshot = matrix_call!(state, materialize_metric_snapshot(metric_ids, scope_ref))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric_snapshot",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "snapshot": snapshot,
    })))
}

pub(super) async fn matrix_metric_dependency_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixMetricDependencyUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let dependency_input = MatrixMetricDependency::from_input(request.dependency);
    let dependency = matrix_call!(state, upsert_metric_dependency(&dependency_input))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric_dependency",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "dependency": dependency,
    })))
}

pub(super) async fn matrix_metric_affected_by_fact_type_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixAffectedByFactTypeRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let fact_type = request.fact_type.clone();
    let metric_ids = matrix_call!(state, metrics_affected_by_fact_type(&fact_type))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metric_dependency.affected_by_fact_type",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "fact_type": request.fact_type,
        "metric_ids": metric_ids,
    })))
}

pub(super) async fn matrix_compute_job_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixComputeJobPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = request.job.clone();
    let plan = matrix_call!(state, plan_compute_job_for_fact_type(job))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.compute.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
    })))
}

pub(super) async fn matrix_compute_job_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = matrix_call!(state, get_compute_job(&id))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix compute job not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.compute.job",
        "job": job,
    })))
}

pub(super) async fn matrix_compute_job_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = matrix_call!(state, run_compute_job(&id)).map_err(|error| match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.compute.job",
        "job": job,
    })))
}

pub(super) async fn matrix_metric_recompute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = matrix_call!(state, recompute_metrics())
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.metrics.recompute",
        "result": result,
    })))
}

pub(super) async fn matrix_changes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let changes = matrix_call!(state, list_changes(100))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.changes",
        "changes": changes,
    })))
}

pub(super) async fn matrix_attention_hot_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = matrix_call!(state, list_attention(50))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.attention.hot",
        "items": items,
    })))
}
