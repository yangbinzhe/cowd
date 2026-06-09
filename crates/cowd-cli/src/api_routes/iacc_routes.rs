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
use runtime::{IaccFact, IaccFactInput, IaccStore, IaccStoreError, IACC_SCHEMA_VERSION};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/iacc/health", get(iacc_health_handler))
        .route("/api/iacc/facts/ingest", post(iacc_fact_ingest_handler))
        .route("/api/iacc/metrics", get(iacc_metrics_handler))
        .route("/api/iacc/metrics/:id", get(iacc_metric_detail_handler))
        .route(
            "/api/iacc/metrics/recompute",
            post(iacc_metric_recompute_handler),
        )
        .route("/api/iacc/changes", get(iacc_changes_handler))
        .route("/api/iacc/attention/hot", get(iacc_attention_hot_handler))
        .route(
            "/api/iacc/evidence/build",
            post(iacc_evidence_build_handler),
        )
        .route("/api/iacc/evidence/:id", get(iacc_evidence_get_handler))
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
        "store": iacc_store_path(&state.workspace_root),
        "capabilities": [
            "fact_ingest",
            "metric_recompute",
            "metric_state",
            "change_event",
            "attention_hot",
            "evidence_packet_build",
            "evidence_packet_get"
        ],
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
