use std::{path::Path, sync::Arc};

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::capability::{CowdCapabilityRegistry, CowdSurface};
use runtime::iacc::{IaccDataPlaneIngestPlanInput, IaccStore, IaccStoreError};
use runtime::projection::CowdProjection;
use runtime::release_gate::CowdReleaseGateReport;
use runtime::structured_data::{
    CowdIngestPlan, CowdStructuredEvidence, CowdStructuredFact, CowdStructuredSource, CowdWatermark,
};
use runtime::surface_contract::CowdSurfaceParityContract;
use serde::{Deserialize, Serialize};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/cowd/capabilities", get(capabilities_handler))
        .route("/api/cowd/projection", get(projection_handler))
        .route("/api/cowd/surfaces", get(surfaces_handler))
        .route("/api/cowd/release-gate", get(release_gate_handler))
        .route(
            "/api/cowd/structured/sources",
            get(structured_sources_handler),
        )
        .route(
            "/api/cowd/structured/sources/:id",
            get(structured_source_get_handler),
        )
        .route(
            "/api/cowd/structured/ingest-plan",
            post(structured_ingest_plan_handler),
        )
        .route("/api/cowd/structured/facts", get(structured_facts_handler))
        .route(
            "/api/cowd/structured/evidence",
            get(structured_evidence_handler),
        )
        .route(
            "/api/cowd/structured/watermarks",
            get(structured_watermarks_handler),
        )
}

#[derive(Debug, Deserialize)]
struct ProjectionQuery {
    #[serde(default)]
    surface: Option<String>,
}

async fn capabilities_handler() -> impl IntoResponse {
    Json(CowdCapabilityRegistry::core())
}

async fn projection_handler(
    Query(query): Query<ProjectionQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let surface = parse_surface(query.surface.as_deref())?;
    let registry = CowdCapabilityRegistry::core();
    Ok(Json(CowdProjection::for_surface(&registry, surface)))
}

async fn surfaces_handler() -> impl IntoResponse {
    let registry = CowdCapabilityRegistry::core();
    Json(CowdSurfaceParityContract::from_registry(&registry))
}

async fn release_gate_handler() -> impl IntoResponse {
    Json(CowdReleaseGateReport::evaluate())
}

async fn structured_sources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)?;
    let items = store
        .list_source_packs(100)
        .map_err(store_error)?
        .iter()
        .map(CowdStructuredSource::from)
        .collect::<Vec<_>>();
    Ok(Json(structured_collection(
        "cowd.structured.sources",
        items,
        100,
    )))
}

async fn structured_source_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)?;
    let Some(source_pack) = store.get_source_pack(&id).map_err(store_error)? else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("source not found: {id}"),
        ));
    };
    Ok(Json(CowdStructuredSource::from(&source_pack)))
}

async fn structured_ingest_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(input): Json<IaccDataPlaneIngestPlanInput>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)?;
    let plan = store.plan_data_plane_ingest(input).map_err(store_error)?;
    Ok(Json(CowdIngestPlan::from(&plan)))
}

async fn structured_facts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)?;
    let items = store
        .list_facts(100)
        .map_err(store_error)?
        .iter()
        .map(CowdStructuredFact::from)
        .collect::<Vec<_>>();
    Ok(Json(structured_collection(
        "cowd.structured.facts",
        items,
        100,
    )))
}

async fn structured_evidence_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)?;
    let items = store
        .list_evidence_packets(100)
        .map_err(store_error)?
        .iter()
        .map(CowdStructuredEvidence::from)
        .collect::<Vec<_>>();
    Ok(Json(structured_collection(
        "cowd.structured.evidence",
        items,
        100,
    )))
}

async fn structured_watermarks_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = open_iacc_store(&state)?;
    let items = store
        .list_data_plane_watermarks(100)
        .map_err(store_error)?
        .iter()
        .map(CowdWatermark::from)
        .collect::<Vec<_>>();
    Ok(Json(structured_collection(
        "cowd.structured.watermarks",
        items,
        100,
    )))
}

fn parse_surface(surface: Option<&str>) -> Result<CowdSurface, (StatusCode, Json<ErrorResponse>)> {
    match surface
        .unwrap_or("webui")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "webui" => Ok(CowdSurface::Webui),
        "tui" => Ok(CowdSurface::Tui),
        "cli" => Ok(CowdSurface::Cli),
        other => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("unsupported cowd projection surface: {other}"),
        )),
    }
}

fn structured_collection<T>(kind: &'static str, items: Vec<T>, limit: usize) -> serde_json::Value
where
    T: Serialize,
{
    let count = items.len();
    serde_json::json!({
        "kind": kind,
        "contract": "cowd.structured_data.v1",
        "items": items,
        "count": count,
        "limit": limit,
        "source": "cowd.structured_data.core",
        "backing": "iacc_adapter",
        "list_status": "ready",
    })
}

fn open_iacc_store(state: &AppState) -> Result<IaccStore, (StatusCode, Json<ErrorResponse>)> {
    let path = iacc_store_path(&state.workspace_root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create iacc store directory: {error}"),
            )
        })?;
    }
    IaccStore::open(path).map_err(store_error)
}

fn iacc_store_path(workspace_root: &Path) -> std::path::PathBuf {
    workspace_root.join(".cowd").join("iacc.sqlite")
}

fn store_error(error: IaccStoreError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        IaccStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}
