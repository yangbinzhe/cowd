use super::*;

pub(super) async fn matrix_evidence_build_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEvidenceBuildRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let packet = state
        .services
        .matrix
        .build_evidence_packet(
            &state.config_home,
            None,
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

pub(super) async fn matrix_evidence_get_handler(
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

pub(super) async fn matrix_evidence_quality_gate_handler(
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

pub(super) async fn matrix_quality_gate_get_handler(
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

pub(super) async fn matrix_evidence_context_handler(
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
