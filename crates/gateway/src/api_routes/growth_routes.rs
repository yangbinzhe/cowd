use std::sync::Arc;

use axum::{
    extract::State as AxumState, http::StatusCode, response::IntoResponse, routing::get, Json,
    Router,
};

use super::AppState;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/growth/status", get(growth_status_handler))
        .route("/api/growth/events", get(growth_events_handler))
}

async fn growth_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    match growth_logs(&state) {
        Ok((events, promotions)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "kind": "growth.status",
                "ok": true,
                "envelope": state.services.growth.event_log_contract(),
                "event_count": events.len(),
                "promotion_count": promotions.len(),
                "sources": growth_sources(&events),
                "projection": growth_projection_health(&state),
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "kind": "growth.status",
                "ok": false,
                "envelope": state.services.growth.event_log_contract(),
                "degraded_reason": error,
                "projection": growth_projection_health(&state),
            })),
        )
            .into_response(),
    }
}

async fn growth_events_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    match growth_logs(&state) {
        Ok((events, promotions)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "kind": "growth.events",
                "ok": true,
                "envelope": state.services.growth.event_log_contract(),
                "total": events.len(),
                "events": events,
                "promotions": promotions,
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "kind": "growth.events",
                "ok": false,
                "envelope": state.services.growth.event_log_contract(),
                "degraded_reason": error,
            })),
        )
            .into_response(),
    }
}

fn growth_logs(
    state: &AppState,
) -> Result<
    (
        Vec<harness_contract::growth::GrowthEvent>,
        Vec<crate::services::GrowthPromotionReceipt>,
    ),
    String,
> {
    Ok((
        state.services.growth.durable_event_log()?,
        state.services.growth.durable_promotion_log()?,
    ))
}

fn growth_sources(events: &[harness_contract::growth::GrowthEvent]) -> Vec<String> {
    let mut sources = events
        .iter()
        .map(|event| event.source_event_kind.clone())
        .collect::<Vec<_>>();
    sources.sort();
    sources.dedup();
    sources
}

fn growth_projection_health(state: &AppState) -> Option<serde_json::Value> {
    state
        .services
        .runtime
        .as_ref()?
        .runtime_services()
        .event_reactor_health()
        .ok()?
        .lanes
        .into_iter()
        .find(|lane| lane.projection_id == crate::services::GROWTH_PROJECTOR_ID)
        .map(|lane| {
            serde_json::json!({
                "projection_id": lane.projection_id,
                "worker_running": lane.worker_running,
                "checkpoint_cursor": lane.checkpoint_cursor,
                "latest_commit_cursor": lane.latest_commit_cursor,
                "lag_commits": lane.lag_commits,
                "consecutive_failures": lane.consecutive_failures,
                "total_passes": lane.total_passes,
                "total_scanned_commits": lane.total_scanned_commits,
                "total_matched_events": lane.total_matched_events,
                "last_pass_duration_ms": lane.last_pass_duration_ms,
                "last_success_at_ms": lane.last_success_at_ms,
                "last_error": lane.last_error,
                "dead_lettered": crate::services::growth_projection_lane::growth_dead_lettered(),
            })
        })
}
