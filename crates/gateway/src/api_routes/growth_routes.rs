use std::sync::Arc;

use axum::{extract::State as AxumState, response::IntoResponse, routing::get, Json, Router};

use super::AppState;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/growth/status", get(growth_status_handler))
        .route("/api/growth/events", get(growth_events_handler))
}

async fn growth_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let events = state.services.growth.event_log();
    Json(serde_json::json!({
        "kind": "growth.status",
        "ok": true,
        "envelope": state.services.growth.event_log_contract(),
        "event_count": events.len(),
        "sources": growth_sources(&events),
    }))
}

async fn growth_events_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let events = state.services.growth.event_log();
    Json(serde_json::json!({
        "kind": "growth.events",
        "ok": true,
        "envelope": state.services.growth.event_log_contract(),
        "total": events.len(),
        "events": events,
    }))
}

fn growth_sources(events: &[ai_kernel::growth::GrowthEvent]) -> Vec<String> {
    let mut sources = events
        .iter()
        .map(|event| event.source_event_kind.clone())
        .collect::<Vec<_>>();
    sources.sort();
    sources.dedup();
    sources
}
