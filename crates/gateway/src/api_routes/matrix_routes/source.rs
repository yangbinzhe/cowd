use super::*;

pub(super) async fn matrix_data_plane_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let health = matrix_call!(state, data_plane_health())
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.data_plane.health",
        "health": health,
    })))
}

pub(super) async fn matrix_data_plane_ingest_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixDataPlaneIngestPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = request.session_id.clone();
    let ingest = request.ingest;
    let plan = matrix_call!(state, plan_data_plane_ingest(ingest))
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

pub(super) async fn matrix_source_pack_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MatrixSourcePackUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack_input = request.source_pack;
    let source_pack = matrix_call!(state, upsert_source_pack(source_pack_input))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_pack",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack": source_pack,
    })))
}

pub(super) async fn matrix_source_pack_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack = matrix_call!(state, get_source_pack(&id))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix source pack not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_pack",
        "source_pack": source_pack,
    })))
}

pub(super) async fn matrix_source_pack_validate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let validation =
        matrix_call!(state, validate_source_pack(&id)).map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_pack.validation",
        "validation": validation,
    })))
}

pub(super) async fn matrix_source_pack_delta_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let delta_plan =
        matrix_call!(state, source_pack_delta_plan(&id)).map_err(|error| match error {
            MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
            other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_pack.delta_plan",
        "delta_plan": delta_plan,
    })))
}

pub(super) async fn matrix_source_pack_ingest_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MatrixSourcePackIngestFileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack_id = id.clone();
    matrix_call!(state, validate_source_pack(&source_pack_id)).map_err(|error| match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    let mut attention = Vec::new();
    for input in request.facts {
        let fact = MatrixFact::from_input(input);
        let item = matrix_call!(state, ingest_fact(&fact))
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

pub(super) async fn matrix_source_pack_connector_run_plan_handler(
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
    let run = matrix_call!(state, plan_connector_run(&id, input)).map_err(|error| match error {
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

pub(super) async fn matrix_source_pack_connector_run_execute_handler(
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
    let run = matrix_call!(state, plan_connector_run(&id, input)).map_err(|error| match error {
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

pub(super) async fn matrix_connector_run_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let run = matrix_call!(state, get_connector_run(&id))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix connector run not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.connector_run",
        "run": run,
    })))
}

pub(super) async fn matrix_source_snapshot_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MatrixSourceSnapshotPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let resource_ref = request.resource_ref.clone();
    let plan = matrix_call!(
        state,
        plan_source_snapshot(&id, resource_ref, request.estimated_rows)
    )
    .map_err(|error| match error {
        MatrixStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    })?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_snapshot.plan",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "plan": plan,
    })))
}

pub(super) async fn matrix_source_snapshot_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<MatrixSourceSnapshotRunRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let lookup_source_pack_id = id.clone();
    let source_pack = matrix_call!(state, get_source_pack(&lookup_source_pack_id))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix source pack not found"))?;

    source_pack
        .validate()
        .blockers
        .is_empty()
        .then_some(())
        .ok_or_else(|| {
            api_error(
                StatusCode::BAD_REQUEST,
                "Matrix source pack is not ready for snapshot execution",
            )
        })?;

    let mut rows = request.rows;
    let mut resource_ref = request
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.resource_ref.clone());
    let mut source_kind = request
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.source_kind)
        .unwrap_or_else(|| source_kind_for_access_mode(&source_pack.access_mode));
    let mut schema_version = request
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.schema_version.clone());
    let mut checksum = request
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.checksum.clone());
    let mut capture_metadata = serde_json::json!({
        "source": "matrix.source_snapshot_run",
        "delivery": "direct_rows",
    });

    if let Some(read_plan) = request.source_read_plan.as_ref() {
        let manifest =
            connector::source_adapter_manifest(&read_plan.adapter_id).ok_or_else(|| {
                api_error(
                    StatusCode::BAD_REQUEST,
                    format!("unsupported source adapter: {}", read_plan.adapter_id),
                )
            })?;
        source_kind = source_kind_for_adapter(&read_plan.adapter_id);
        resource_ref = Some(read_plan.resource_ref.clone());
        if manifest.requires_sidecar {
            if rows.is_empty() {
                let batch = state
                    .services
                    .surface
                    .read_source_batch(read_plan)
                    .await
                    .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
                resource_ref = Some(batch.resource_ref.clone());
                schema_version = Some(format!(
                    "source:{}:{}",
                    batch.adapter_id, batch.schema.table_name
                ));
                checksum = Some(batch.checksum.clone());
                capture_metadata = serde_json::json!({
                    "adapter_id": batch.adapter_id,
                    "adapter_family": manifest.family,
                    "delivery": "edge_source_connector",
                    "resource_ref": batch.resource_ref,
                    "table": batch.table,
                    "schema": batch.schema,
                    "cursor": batch.cursor,
                    "batch_row_count": batch.rows.len(),
                    "source_row_count": batch.row_count,
                    "checksum": batch.checksum,
                    "truncated": batch.truncated,
                });
                rows = batch.rows;
            } else {
                capture_metadata = serde_json::json!({
                    "adapter_id": read_plan.adapter_id,
                    "adapter_family": manifest.family,
                    "delivery": "sidecar_rows",
                    "row_count": rows.len(),
                });
            }
        } else {
            let batch = connector::read_local_source_batch(read_plan).map_err(|error| {
                api_error(
                    StatusCode::BAD_REQUEST,
                    format!("source batch read failed: {error}"),
                )
            })?;
            resource_ref = Some(batch.resource_ref.clone());
            schema_version = Some(format!(
                "source:{}:{}",
                batch.adapter_id, batch.schema.table_name
            ));
            checksum = Some(batch.checksum.clone());
            capture_metadata = serde_json::json!({
                "adapter_id": batch.adapter_id,
                "delivery": "connector_local_read",
                "resource_ref": batch.resource_ref,
                "table": batch.table,
                "schema": batch.schema,
                "cursor": batch.cursor,
                "batch_row_count": batch.rows.len(),
                "source_row_count": batch.row_count,
                "checksum": batch.checksum,
                "truncated": batch.truncated,
            });
            rows = batch.rows;
        }
    }

    if rows.is_empty() && request.facts.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "source snapshot run requires rows, facts, or an executable source_read_plan",
        ));
    }

    let row_count = if rows.is_empty() {
        request.facts.len()
    } else {
        rows.len()
    };
    let snapshot_input = normalized_snapshot_input(
        request.snapshot,
        &id,
        &source_pack,
        source_kind,
        resource_ref,
        schema_version.unwrap_or_else(|| "source_rows:v1".to_string()),
        row_count as u64,
        checksum.or_else(|| (!rows.is_empty()).then(|| stable_rows_checksum(&rows))),
        capture_metadata,
    );
    let snapshot = matrix_call!(state, create_source_snapshot(snapshot_input))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let apply_report = if rows.is_empty() {
        None
    } else {
        let source_pack_id = id.clone();
        let applied_snapshot = snapshot.clone();
        Some(
            matrix_call!(
                state,
                apply_source_snapshot_rows(&source_pack_id, applied_snapshot, &rows)
            )
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?,
        )
    };

    let mut fact_attention = Vec::new();
    for mut input in request.facts {
        if input.snapshot_id.is_none() {
            input.snapshot_id = Some(snapshot.snapshot_id.clone());
        }
        let fact = MatrixFact::from_input(input);
        let attention = matrix_call!(state, ingest_fact(&fact))
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        fact_attention.push(attention);
    }

    Ok(Json(serde_json::json!({
        "kind": "matrix.source_snapshot.run",
        "request_id": request.request_id,
        "session_id": request.session_id,
        "source_pack_id": id,
        "snapshot": snapshot,
        "apply_report": apply_report,
        "fact_ingested": fact_attention.len(),
        "fact_attention": fact_attention,
    })))
}

pub(super) async fn matrix_source_pack_snapshots_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    AxumQuery(query): AxumQuery<MatrixSourceSnapshotListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source_pack_id = id.clone();
    let snapshots = matrix_call!(
        state,
        list_source_snapshots(Some(&source_pack_id), query.limit.unwrap_or(100))
    )
    .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_snapshot.list",
        "source_pack_id": id,
        "snapshots": snapshots,
    })))
}

pub(super) async fn matrix_source_snapshot_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let snapshot = matrix_call!(state, get_source_snapshot(&id))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Matrix source snapshot not found"))?;
    Ok(Json(serde_json::json!({
        "kind": "matrix.source_snapshot",
        "snapshot": snapshot,
    })))
}

fn normalized_snapshot_input(
    input: Option<MatrixSourceSnapshotInput>,
    source_pack_id: &str,
    source_pack: &MatrixSourcePack,
    source_kind: MatrixSourceKind,
    resource_ref: Option<String>,
    schema_version: String,
    row_count: u64,
    checksum: Option<String>,
    capture_metadata: Value,
) -> MatrixSourceSnapshotInput {
    let mut input = input.unwrap_or(MatrixSourceSnapshotInput {
        snapshot_id: None,
        source_pack_id: None,
        source_system: source_pack.source_name.clone(),
        source_kind,
        resource_ref: None,
        business_period: None,
        captured_at: None,
        schema_version: None,
        row_count: None,
        checksum: None,
        confidence: None,
        metadata: Value::Null,
    });
    if input.source_pack_id.is_none() {
        input.source_pack_id = Some(source_pack_id.to_string());
    }
    if input.source_system.trim().is_empty() {
        input.source_system = source_pack.source_name.clone();
    }
    if input.resource_ref.is_none() {
        input.resource_ref = resource_ref;
    }
    if input.schema_version.is_none() {
        input.schema_version = Some(schema_version);
    }
    if input.row_count.is_none() {
        input.row_count = Some(row_count);
    }
    if input.checksum.is_none() {
        input.checksum = checksum;
    }
    input.metadata = merge_capture_metadata(input.metadata, capture_metadata);
    input
}

fn merge_capture_metadata(existing: Value, capture_metadata: Value) -> Value {
    match existing {
        Value::Null => serde_json::json!({ "capture": capture_metadata }),
        Value::Object(mut object) => {
            object.insert("capture".to_string(), capture_metadata);
            Value::Object(object)
        }
        other => serde_json::json!({
            "source_metadata": other,
            "capture": capture_metadata,
        }),
    }
}

fn source_kind_for_adapter(adapter_id: &str) -> MatrixSourceKind {
    match adapter_id {
        "csv" | "jsonl" | "local_file_batch" => MatrixSourceKind::File,
        "sqlite" | "postgres" | "mysql" | "mariadb" => MatrixSourceKind::Db,
        "feishu_bitable" | "lark_bitable" => MatrixSourceKind::Api,
        _ => MatrixSourceKind::Connector,
    }
}

fn source_kind_for_access_mode(access_mode: &str) -> MatrixSourceKind {
    match access_mode {
        "api" => MatrixSourceKind::Api,
        "db_view" | "database_view" | "database_file" | "database_service" | "sqlite" => {
            MatrixSourceKind::Db
        }
        "file" | "batch_file" | "file_batch" | "manual_upload" => MatrixSourceKind::File,
        "manual" => MatrixSourceKind::Manual,
        "rpa" => MatrixSourceKind::Rpa,
        _ => MatrixSourceKind::Connector,
    }
}

fn stable_rows_checksum(rows: &[Value]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for row in rows {
        hasher.update(serde_json::to_vec(row).unwrap_or_default());
        hasher.update(b"\n");
    }
    format!("sha256:{:x}", hasher.finalize())
}
