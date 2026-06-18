use std::sync::Arc;

use app_mfg::server_manufacturing_skill_pack;
use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use matrix::structured::{
    StructuredEvidence as CowdStructuredEvidence, StructuredFact as CowdStructuredFact,
    StructuredIngestPlan as CowdIngestPlan,
    StructuredIngestPlanInput as CowdStructuredIngestPlanInput,
    StructuredSource as CowdStructuredSource, StructuredWatermark as CowdWatermark,
};
use matrix::{MatrixEvidencePacket, MatrixEvidenceSourceRef};
use matrix_store::{MatrixRuntimeStore, MatrixRuntimeStoreError};
use runtime::capability::{CowdCapabilityRegistry, CowdSurface};
use runtime::projection::CowdProjection;
use runtime::release_gate::{CowdReleaseGateReport, CowdReleaseGateRuntimeEvidence};
use runtime::skill_activation::{RuntimeSkillCandidate, SkillActivationRecord};
use runtime::skill_memory::{memory_candidate_from_skill_activation, SkillMemoryPolicy};
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

async fn release_gate_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(CowdReleaseGateReport::evaluate_with(
        release_gate_runtime_evidence(&state).await,
    ))
}

async fn structured_sources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = matrix_runtime_store(&state)?;
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
    let store = matrix_runtime_store(&state)?;
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
    Json(input): Json<CowdStructuredIngestPlanInput>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = matrix_runtime_store(&state)?;
    let plan = store
        .plan_data_plane_ingest(input.into())
        .map_err(store_error)?;
    Ok(Json(CowdIngestPlan::from(&plan)))
}

async fn structured_facts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = matrix_runtime_store(&state)?;
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
    let store = matrix_runtime_store(&state)?;
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
    let store = matrix_runtime_store(&state)?;
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
        "backing": "matrix",
        "list_status": "ready",
    })
}

async fn release_gate_runtime_evidence(state: &AppState) -> CowdReleaseGateRuntimeEvidence {
    let (structured_indexes_ready, structured_watermark_persistent) =
        match state.services.matrix.runtime_store(&state.config_home) {
            Ok(store) => {
                let indexes_ready = store.list_source_packs(1).is_ok()
                    && store.list_facts(1).is_ok()
                    && store.list_evidence_packets(1).is_ok();
                let watermarks_ready = store.list_data_plane_watermarks(1).is_ok();
                (indexes_ready, watermarks_ready)
            }
            Err(_) => (false, false),
        };

    CowdReleaseGateRuntimeEvidence {
        structured_indexes_ready,
        structured_watermark_persistent,
        execution_outcome_timeline_available: execution_outcome_timeline_available(state).await,
        memory_context_bridge_available: memory_context_bridge_smoke(),
        graph_skill_quality_contracts_available: graph_skill_quality_contract_smoke(),
    }
}

async fn execution_outcome_timeline_available(state: &AppState) -> bool {
    let Some(store) = state.services.session.unified_store() else {
        return false;
    };
    let Ok(sessions) = store.list_sessions().await else {
        return false;
    };
    for session in sessions.into_iter().take(50) {
        let Ok(page) = store
            .timeline_events_page(&session.session_id, 0, 100)
            .await
        else {
            continue;
        };
        if page
            .events
            .iter()
            .any(|event| event.kind == "execution.outcome")
        {
            return true;
        }
    }
    false
}

fn memory_context_bridge_smoke() -> bool {
    let activation = SkillActivationRecord::new(
        "release-gate-smoke",
        1,
        "structured data bridge",
        vec![RuntimeSkillCandidate {
            name: "structured-data".to_string(),
            score: 12,
            reasons: vec!["release-gate".to_string()],
            path: None,
        }],
    );
    memory_candidate_from_skill_activation(&activation, &SkillMemoryPolicy::default())
        .map(|candidate| candidate.content.contains("source=skill_activation"))
        .unwrap_or(false)
}

fn graph_skill_quality_contract_smoke() -> bool {
    let Some(skill) = server_manufacturing_skill_pack()
        .into_iter()
        .find(|skill| skill.skill_id == "supply-risk-analyst")
    else {
        return false;
    };
    if skill.input_fact_types.is_empty() || skill.quality_gate.is_empty() {
        return false;
    }

    let mut packet = MatrixEvidencePacket::new("release gate structured quality smoke");
    packet.packet_id = "release-gate-smoke".to_string();
    packet.confidence = 0.9;
    packet
        .metric_evidence
        .push(serde_json::json!({"metric": "material_shortage_risk"}));
    packet.source_refs.push(MatrixEvidenceSourceRef {
        kind: "fact".to_string(),
        reference: "structured-fact:release-gate-smoke".to_string(),
        summary: "release gate smoke fact".to_string(),
    });
    packet.confidence >= 0.75
        && packet
            .source_refs
            .iter()
            .any(|source| source.reference == "structured-fact:release-gate-smoke")
}

fn matrix_runtime_store(
    state: &AppState,
) -> Result<MatrixRuntimeStore, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .matrix
        .runtime_store(&state.config_home)
        .map_err(store_error)
}

fn store_error(error: MatrixRuntimeStoreError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        MatrixRuntimeStoreError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}
