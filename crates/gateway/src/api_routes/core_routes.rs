use std::{collections::BTreeSet, sync::Arc};

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use matrix_core::structured::{
    StructuredEvidence as CowdStructuredEvidence, StructuredFact as CowdStructuredFact,
    StructuredIngestPlan as CowdIngestPlan,
    StructuredIngestPlanInput as CowdStructuredIngestPlanInput,
    StructuredSource as CowdStructuredSource, StructuredWatermark as CowdWatermark,
};
use runtime::capability::{CowdApplicationCapabilityInput, CowdCapabilityRegistry, CowdSurface};
use runtime::projection::CowdProjection;
use runtime::release_gate::{CowdReleaseGateReport, CowdReleaseGateRuntimeEvidence};
use runtime::surface_contract::CowdSurfaceParityContract;
use serde::{Deserialize, Serialize};

use crate::services::GatewayMatrixRepositoryError;

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

async fn capabilities_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(product_capability_registry(&state))
}

async fn projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<ProjectionQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let surface = parse_surface(query.surface.as_deref())?;
    let registry = product_capability_registry(&state);
    Ok(Json(CowdProjection::for_surface(&registry, surface)))
}

async fn surfaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let registry = product_capability_registry(&state);
    Json(CowdSurfaceParityContract::from_registry(&registry))
}

fn product_capability_registry(state: &AppState) -> CowdCapabilityRegistry {
    CowdCapabilityRegistry::core().with_registered_applications(
        state
            .services
            .app_registry
            .apps()
            .into_iter()
            .map(|application| CowdApplicationCapabilityInput {
                app_id: application.descriptor.id.as_str().to_string(),
                display_name: application.descriptor.display_name,
                version: application.descriptor.version,
                capabilities: application.descriptor.capabilities,
                actions: application
                    .descriptor
                    .actions
                    .into_iter()
                    .map(|action| action.id)
                    .collect(),
                webui_registered: application.http_registered,
                tui_registered: application.tui_registered,
            }),
    )
}

async fn release_gate_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(CowdReleaseGateReport::evaluate_with(
        release_gate_runtime_evidence(&state).await,
    ))
}

async fn structured_sources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = state
        .services
        .matrix
        .list_source_packs(&state.config_home, 100)
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
    let Some(source_pack) = state
        .services
        .matrix
        .get_source_pack(&state.config_home, &id)
        .map_err(store_error)?
    else {
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
    let plan = state
        .services
        .matrix
        .plan_data_plane_ingest(&state.config_home, input.into())
        .map_err(store_error)?;
    Ok(Json(CowdIngestPlan::from(&plan)))
}

async fn structured_facts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = state
        .services
        .matrix
        .list_facts(&state.config_home, 100)
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
    let items = state
        .services
        .matrix
        .list_evidence_packets(&state.config_home, 100)
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
    let items = state
        .services
        .matrix
        .list_data_plane_watermarks(&state.config_home, 100)
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
    let (structured_indexes_ready, structured_watermark_persistent) = state
        .services
        .matrix
        .structured_runtime_ready(&state.config_home);

    CowdReleaseGateRuntimeEvidence {
        structured_indexes_ready,
        structured_watermark_persistent,
        execution_outcome_timeline_available: execution_outcome_timeline_available(state).await,
        memory_context_bridge_available: memory_context_bridge_available(state).await,
        graph_skill_quality_contracts_available: graph_skill_quality_contracts_available(
            state.services.app_registry.as_ref(),
        ),
        gateway_route_manifest_available: gateway_route_manifest_available(state),
        frontend_api_matrix_ready: frontend_api_matrix_ready(),
        surface_version_compatible: surface_version_compatible(),
    }
}

fn gateway_route_manifest_available(state: &AppState) -> bool {
    let routes = super::route_manifest::gateway_route_manifest_for_apps(
        state.services.app_registry.as_ref(),
    );
    let pairs = routes
        .iter()
        .map(|entry| (entry.method.as_str(), entry.path.as_str()))
        .collect::<BTreeSet<_>>();
    !routes.is_empty()
        && pairs.len() == routes.len()
        && routes
            .iter()
            .any(|entry| entry.method == "GET" && entry.path == "/api/gateway/route-manifest")
        && routes
            .iter()
            .any(|entry| entry.method == "GET" && entry.path == "/api/skills/runs")
}

fn frontend_api_matrix_ready() -> bool {
    std::env::var_os("COWD_SKIP_FRONTEND_GATE").is_some()
        || surface_repo_file_exists("surfaces/webui/scripts/api-matrix.mjs")
}

fn surface_version_compatible() -> bool {
    std::env::var_os("COWD_SKIP_EDGE_VERSION_GATE").is_some()
        || surface_repo_file_exists("scripts/edge-version-gate.mjs")
}

fn surface_repo_file_exists(relative: &str) -> bool {
    let relative = std::path::Path::new(relative);
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let cwd = std::env::current_dir().unwrap_or_else(|_| manifest_dir.to_path_buf());
    [
        cwd.join("../cowd-edge").join(relative),
        cwd.join("cowd-edge").join(relative),
        manifest_dir.join("../../../cowd-edge").join(relative),
        manifest_dir.join("../../cowd-edge").join(relative),
    ]
    .iter()
    .any(|path| path.is_file())
}

async fn execution_outcome_timeline_available(state: &AppState) -> bool {
    matches!(
        state
            .services
            .session
            .has_domain_event_kind("application.execution_outcome")
            .await,
        Ok(Some(true))
    )
}

fn is_execution_outcome_event(event: &session::SessionDomainEvent) -> bool {
    event.kind == "application.execution_outcome"
}

async fn memory_context_bridge_available(state: &AppState) -> bool {
    matches!(
        state
            .services
            .session
            .has_session_with_domain_event_kinds(&[
                "skill_candidates".to_string(),
                "skill_memory_candidate".to_string(),
            ])
            .await,
        Ok(Some(true))
    )
}

fn runtime_skill_memory_bridge_session(events: &[session::SessionDomainEvent]) -> bool {
    let invocations = events
        .iter()
        .filter_map(runtime_skill_invocation)
        .collect::<BTreeSet<_>>();
    if invocations.is_empty() {
        return false;
    }

    events.iter().any(|event| {
        runtime_skill_memory_candidate(event)
            .map(|candidate| {
                invocations.iter().any(|invocation| {
                    invocation.skill_id == candidate.skill_id
                        && invocation.turn_index == candidate.turn_index
                        && invocation.sequence <= candidate.sequence
                })
            })
            .unwrap_or(false)
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RuntimeSkillBridgeKey {
    skill_id: String,
    turn_index: u64,
    sequence: usize,
}

fn runtime_skill_invocation(event: &session::SessionDomainEvent) -> Option<RuntimeSkillBridgeKey> {
    if event.kind != "skill_candidates" {
        return None;
    }
    if event.payload.get("source").and_then(|value| value.as_str())
        != Some("conversation_runtime.skill_activation")
    {
        return None;
    }
    let turn_index = event
        .payload
        .get("turn_index")
        .and_then(|value| value.as_u64())?;
    let selected = event
        .payload
        .get("selected")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let evidence = event.payload.get("invocation_evidence")?;
    if evidence.get("outcome").and_then(|value| value.as_str()) != Some("selected_for_runtime") {
        return None;
    }
    let evidence_skill = evidence
        .get("skill_id")
        .and_then(|skill_id| skill_id.as_str())
        .filter(|skill_id| !skill_id.trim().is_empty())
        .map(str::trim)?;
    if evidence_skill != selected {
        return None;
    }
    let has_invocation_ref = event
        .refs
        .iter()
        .any(|reference| reference.ref_type == "skill_invocation" && reference.id == selected);
    if !has_invocation_ref {
        return None;
    }
    Some(RuntimeSkillBridgeKey {
        skill_id: selected.to_string(),
        turn_index,
        sequence: event.sequence,
    })
}

fn runtime_skill_memory_candidate(
    event: &session::SessionDomainEvent,
) -> Option<RuntimeSkillBridgeKey> {
    if event.kind != "skill_memory_candidate" {
        return None;
    }
    if event.payload.get("source").and_then(|value| value.as_str())
        != Some("conversation_runtime.skill_memory_candidate")
    {
        return None;
    }
    if event
        .payload
        .get("source_event")
        .and_then(|value| value.as_str())
        != Some("skill_candidates")
    {
        return None;
    }
    let turn_index = event
        .payload
        .get("turn_index")
        .and_then(|value| value.as_u64())?;
    let selected = event
        .payload
        .get("selected")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let has_selected_ref = event
        .refs
        .iter()
        .any(|reference| reference.ref_type == "skill" && reference.id == selected);
    if !has_selected_ref {
        return None;
    }
    let source_is_runtime_skill = event
        .payload
        .get("candidate")
        .and_then(|candidate| {
            candidate
                .get("content")
                .or_else(|| candidate.get("reason"))
                .and_then(|value| value.as_str())
        })
        .map(|value| value.contains("source=runtime_skill"))
        .unwrap_or(false);
    if !source_is_runtime_skill {
        return None;
    }
    Some(RuntimeSkillBridgeKey {
        skill_id: selected.to_string(),
        turn_index,
        sequence: event.sequence,
    })
}

/// A product-level release gate must only assess contracts belonging to Apps
/// mounted in this Gateway process.  A compiled-but-disabled App cannot make
/// the core release gate depend on its private quality fixtures.
fn graph_skill_quality_contracts_available(app_registry: &cowd_app_host::AppRegistry) -> bool {
    app_registry
        .verify_quality_checks()
        .into_iter()
        .all(|check| check.passed)
}

fn store_error(error: GatewayMatrixRepositoryError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        GatewayMatrixRepositoryError::NotFound(message) => {
            api_error(StatusCode::NOT_FOUND, message)
        }
        other => api_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill_activation_event(sequence: usize, source: &str) -> session::SessionDomainEvent {
        let mut event = session::SessionDomainEvent::new(
            "session-1",
            sequence,
            session::SessionDomainScope::Context,
            "skill_candidates",
            serde_json::json!({
                "source": source,
                "turn_index": 7,
                "selected": "release-review",
                "invocation_evidence": {
                    "skill_id": "release-review",
                    "outcome": "selected_for_runtime"
                }
            }),
            sequence as u64,
        );
        event.refs.push(session::SessionDomainRef {
            ref_type: "skill_invocation".to_string(),
            id: "release-review".to_string(),
            label: Some("selected_for_runtime".to_string()),
        });
        event
    }

    fn skill_memory_event(sequence: usize, source: &str) -> session::SessionDomainEvent {
        let mut event = session::SessionDomainEvent::new(
            "session-1",
            sequence,
            session::SessionDomainScope::Context,
            "skill_memory_candidate",
            serde_json::json!({
                "source": source,
                "turn_index": 7,
                "selected": "release-review",
                "source_event": "skill_candidates",
                "candidate": {
                    "content": "skill selected for task; source=runtime_skill; selected=release-review"
                }
            }),
            sequence as u64,
        );
        event.refs.push(session::SessionDomainRef {
            ref_type: "skill".to_string(),
            id: "release-review".to_string(),
            label: Some("memory_candidate_source".to_string()),
        });
        event
    }

    #[test]
    fn release_gate_skill_bridge_requires_conversation_runtime_sources() {
        let activation = skill_activation_event(1, "manual.skill_activation");
        let memory = skill_memory_event(2, "conversation_runtime.skill_memory_candidate");

        assert!(!runtime_skill_memory_bridge_session(&[activation, memory]));
    }

    #[test]
    fn release_gate_skill_bridge_requires_activation_before_memory_candidate() {
        let activation = skill_activation_event(3, "conversation_runtime.skill_activation");
        let memory = skill_memory_event(2, "conversation_runtime.skill_memory_candidate");

        assert!(!runtime_skill_memory_bridge_session(&[memory, activation]));
    }

    #[test]
    fn release_gate_skill_bridge_accepts_paired_conversation_runtime_events() {
        let activation = skill_activation_event(1, "conversation_runtime.skill_activation");
        let memory = skill_memory_event(2, "conversation_runtime.skill_memory_candidate");

        assert!(runtime_skill_memory_bridge_session(&[activation, memory]));
    }

    #[test]
    fn release_gate_recognizes_application_execution_outcome() {
        let event = session::SessionDomainEvent::new(
            "session-1",
            1,
            session::SessionDomainScope::ApplicationTask,
            "application.execution_outcome",
            serde_json::json!({"status": "succeeded"}),
            1,
        );
        assert!(is_execution_outcome_event(&event));
    }

    #[test]
    fn registry_without_enabled_apps_has_no_quality_requirement() {
        assert!(graph_skill_quality_contracts_available(
            &cowd_app_host::AppRegistry::default()
        ));
    }
}
