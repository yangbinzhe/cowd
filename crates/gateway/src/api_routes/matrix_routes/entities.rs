use super::*;

pub(super) async fn matrix_entities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let entities = matrix_call!(state, list_entities(100))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entities",
        "entities": entities,
    })))
}

pub(super) async fn matrix_entity_match_candidate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityMatchCandidateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let left_entity_id = request.left_entity_id.clone();
    let right_entity_id = request.right_entity_id.clone();
    let candidate = matrix_call!(
        state,
        propose_entity_match(&left_entity_id, &right_entity_id)
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

pub(super) async fn matrix_entity_conflict_decision_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityConflictDecisionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let candidate_id = request.candidate_id.clone();
    let survivor_entity_id = request.survivor_entity_id.clone();
    let retired_entity_id = request.retired_entity_id.clone();
    let survivorship_rule = request.survivorship_rule.clone();
    let notes = request.notes.clone();
    let decision = matrix_call!(
        state,
        decide_entity_conflict(
            &candidate_id,
            &survivor_entity_id,
            &retired_entity_id,
            &survivorship_rule,
            notes
        )
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

pub(super) async fn matrix_entity_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let input = MatrixEntity::from_input(request.entity);
    let entity = matrix_call!(state, upsert_entity(&input))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entity",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "entity": entity,
    })))
}

pub(super) async fn matrix_entity_resolve_source_key_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixEntityResolveSourceKeyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_system = request.source_system.clone();
    let source_key = request.source_key.clone();
    let entity = matrix_call!(
        state,
        resolve_entity_by_source_key(&source_system, &source_key)
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

pub(super) async fn matrix_entity_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lookup_id = id.clone();
    let entity = matrix_call!(state, get_entity(&lookup_id))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix entity not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entity",
        "entity": entity,
    })))
}

pub(super) async fn matrix_relation_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixRelationUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let input = MatrixRelation::from_input(request.relation);
    let relation = matrix_call!(state, upsert_relation(&input)).map_err(|error| match error {
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

pub(super) async fn matrix_entity_relations_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lookup_id = id.clone();
    let relations = matrix_call!(state, list_entity_relations(&lookup_id, 100)).map_err(
        |error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        },
    )?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entity.relations",
        "entity_id": id,
        "relations": relations,
    })))
}

pub(super) async fn matrix_entity_impact_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let trace = matrix_call!(state, impact_trace(&id, 3)).map_err(|error| match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.entity.impact_path",
        "trace": trace,
    })))
}

pub(super) async fn matrix_fact_ingest_handler(
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
        let persisted_fact = fact.clone();
        let item = matrix_call!(state, ingest_fact(&persisted_fact))
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        append_matrix_execution_summary(&state, session_id.as_deref(), matrix_fact_summary(&fact))
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
