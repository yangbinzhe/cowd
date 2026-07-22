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
