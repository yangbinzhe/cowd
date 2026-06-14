use std::{path::Path, sync::Arc};

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::capability::{CowdCapabilityRegistry, CowdSurface};
use runtime::iacc::{
    server_manufacturing_skill_pack, IaccDataPlaneIngestPlanInput, IaccEvidencePacket,
    IaccEvidenceSourceRef, IaccStore, IaccStoreError,
};
use runtime::projection::CowdProjection;
use runtime::quality_gate::CowdStructuredQualityGate;
use runtime::release_gate::{CowdReleaseGateReport, CowdReleaseGateRuntimeEvidence};
use runtime::skill_activation::{RuntimeSkillCandidate, SkillActivationRecord};
use runtime::skill_dependency::CowdSkillStructuredDependency;
use runtime::skill_memory::{memory_candidate_from_skill_activation, SkillMemoryPolicy};
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

async fn release_gate_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(CowdReleaseGateReport::evaluate_with(
        release_gate_runtime_evidence(&state).await,
    ))
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

async fn release_gate_runtime_evidence(state: &AppState) -> CowdReleaseGateRuntimeEvidence {
    let store_path = iacc_store_path(&state.workspace_root);
    if let Some(parent) = store_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let (structured_indexes_ready, structured_watermark_persistent) =
        match IaccStore::open(store_path) {
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
    let Some(store) = state.unified_store() else {
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
    let dependency = CowdSkillStructuredDependency::from(&skill);
    if dependency.required_fact_types.is_empty() || dependency.quality_gate.is_empty() {
        return false;
    }

    let mut packet = IaccEvidencePacket::new("release gate structured quality smoke");
    packet.packet_id = "release-gate-smoke".to_string();
    packet.confidence = 0.9;
    packet
        .metric_evidence
        .push(serde_json::json!({"metric": "material_shortage_risk"}));
    packet.source_refs.push(IaccEvidenceSourceRef {
        kind: "fact".to_string(),
        reference: "structured-fact:release-gate-smoke".to_string(),
        summary: "release gate smoke fact".to_string(),
    });
    let evidence = CowdStructuredEvidence::from(&packet);
    let gate = CowdStructuredQualityGate::for_structured_evidence(&evidence);
    gate.decision == "pass"
        && gate
            .structured_refs
            .contains(&"structured-fact:release-gate-smoke".to_string())
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
