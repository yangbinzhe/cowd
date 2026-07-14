use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State as AxumState},
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use surface::{SurfaceActionRequest, SurfaceSendRequest};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/surfaces", get(list_surfaces_handler))
        .route("/api/surfaces/health", get(surface_health_handler))
        .route("/api/surfaces/:id", get(get_surface_handler))
        .route("/api/surfaces/:id/routes", get(get_surface_routes_handler))
        .route(
            "/api/surfaces/:id/resources",
            get(get_surface_resources_handler),
        )
        .route("/api/surfaces/:id/status", get(get_surface_status_handler))
        .route("/api/surfaces/:id/health", get(get_surface_health_handler))
        .route(
            "/api/surfaces/:id/health-check",
            post(post_surface_health_check_handler),
        )
        .route("/api/surfaces/:id/events", get(get_surface_events_handler))
        .route("/api/surfaces/:id/start", post(start_surface_handler))
        .route("/api/surfaces/:id/stop", post(stop_surface_handler))
        .route("/api/surfaces/:id/restart", post(restart_surface_handler))
        .route("/api/surfaces/:id/repair", post(repair_surface_handler))
        .route("/api/surfaces/:id/send", post(send_surface_handler))
        .route("/api/surfaces/:id/action", post(action_surface_handler))
        .route("/api/surfaces/:id/inbox", get(get_surface_inbox_handler))
        .route("/api/surfaces/:id/outbox", get(get_surface_outbox_handler))
        .route(
            "/api/surfaces/:id/messages",
            get(get_surface_messages_handler),
        )
        .route(
            "/api/surfaces/:id/messages/archive",
            post(archive_surface_messages_handler),
        )
        .route(
            "/api/surfaces/:id/messages/purge-archived-events",
            post(purge_archived_surface_messages_handler),
        )
        .route(
            "/api/surfaces/:id/deliveries",
            get(get_surface_deliveries_handler),
        )
        .route(
            "/api/surfaces/:id/trigger-events",
            get(get_surface_trigger_events_handler),
        )
        .route(
            "/api/surfaces/:id/trigger-events/retry",
            post(retry_surface_trigger_event_handler),
        )
        .route(
            "/api/surfaces/:id/inbox/:message_id/replay",
            post(replay_surface_inbox_handler),
        )
        .route(
            "/api/surfaces/:id/outbox/:delivery_id/retry",
            post(retry_surface_outbox_handler),
        )
        .route(
            "/api/surfaces/:id/outbox/:delivery_id/dead-letter",
            post(dead_letter_surface_outbox_handler),
        )
        .route("/s/:surface/*path", get(surface_static_handler))
        .route(
            "/surface-callback/:surface/*path",
            get(surface_callback_handler).post(surface_callback_handler),
        )
}

#[derive(Debug, Deserialize)]
struct SurfaceSendBody {
    recipient: String,
    #[serde(default)]
    thread: Option<String>,
    text: String,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    metadata: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SurfaceActionBody {
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct DeadLetterBody {
    #[serde(default = "default_dead_letter_reason")]
    reason: String,
}

#[derive(Debug, Deserialize)]
struct ArchiveMessagesBody {
    #[serde(default)]
    older_than_ms: Option<i64>,
    #[serde(default = "default_archive_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct PurgeMessagesBody {
    #[serde(default)]
    older_than_ms: Option<i64>,
    #[serde(default = "default_archive_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct TriggerEventRetryBody {
    idempotency_key: String,
}

fn default_archive_limit() -> usize {
    100
}

fn default_dead_letter_reason() -> String {
    "operator moved delivery to dead letter".to_string()
}

async fn list_surfaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "kind": "surface.registry",
        "registry": state.services.surface.snapshot(),
    }))
}

async fn surface_health_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let snapshot = state.services.surface.snapshot();
    Json(serde_json::json!({
        "kind": "surface.health",
        "status": state.services.surface.health().status,
        "surface_count": snapshot.surfaces.len(),
        "host": state.services.surface.health(),
        "registry": snapshot,
        "runtime": state.services.surface.runtime_snapshots(),
    }))
}

async fn get_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let normalized = surface::normalize_surface_id(&id);
    let surface = state
        .services
        .surface
        .snapshot()
        .surfaces
        .into_iter()
        .find(|surface| surface.id == normalized)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("surface `{id}` not found")))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.detail",
        "surface": surface,
    })))
}

async fn get_surface_routes_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let routes = state
        .services
        .surface
        .routes(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("surface `{id}` not found")))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.routes",
        "surface": routes.surface,
        "routes": routes.routes,
    })))
}

async fn get_surface_resources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let resources = state
        .services
        .surface
        .resources(&id)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("surface `{id}` not found")))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.resources",
        "surface": resources.surface,
        "resources": resources.resources,
    })))
}

async fn get_surface_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .check_surface_health(&id)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}

async fn get_surface_status_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let normalized = surface::normalize_surface_id(&id);
    let runtime = state
        .services
        .surface
        .runtime_snapshot(&normalized)
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("surface `{id}` not found")))?;
    let events = state.services.surface.supervisor_events(&normalized).await;
    Ok(Json(serde_json::json!({
        "kind": "surface.status",
        "surface": normalized,
        "runtime": runtime,
        "events": events,
    })))
}

async fn get_surface_trigger_events_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let surface = surface::normalize_surface_id(&id);
    let events = state.services.surface.trigger_events(&surface);
    Json(serde_json::json!({
        "kind": "surface.trigger_events",
        "surface": surface,
        "events": events,
    }))
}

async fn retry_surface_trigger_event_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<TriggerEventRetryBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let surface = surface::normalize_surface_id(&id);
    let event = state
        .services
        .surface
        .retry_trigger_event(&surface, &body.idempotency_key)
        .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.trigger_event.retry_accepted",
        "surface": surface,
        "event": event,
    })))
}

async fn post_surface_health_check_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    get_surface_health_handler(AxumState(state), Path(id)).await
}

async fn start_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state
        .services
        .surface
        .start_surface(&id)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.supervisor.start",
        "surface": surface::normalize_surface_id(&id),
        "runtime": runtime,
    })))
}

async fn stop_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state
        .services
        .surface
        .stop_surface(&id)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.supervisor.stop",
        "surface": surface::normalize_surface_id(&id),
        "runtime": runtime,
    })))
}

async fn restart_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state
        .services
        .surface
        .restart_surface(&id)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.supervisor.restart",
        "surface": surface::normalize_surface_id(&id),
        "runtime": runtime,
    })))
}

async fn repair_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state
        .services
        .surface
        .repair_surface(&id)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.supervisor.repair",
        "surface": surface::normalize_surface_id(&id),
        "runtime": runtime,
    })))
}

async fn get_surface_events_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let events = state.services.surface.events(&id).await;
    let supervisor_events = state.services.surface.supervisor_events(&id).await;
    Ok(Json(serde_json::json!({
        "kind": "surface.events",
        "surface": surface::normalize_surface_id(&id),
        "events": events,
        "supervisor_events": supervisor_events,
    })))
}

async fn send_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SurfaceSendBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: id,
            recipient: body.recipient,
            thread: body.thread,
            text: body.text,
            idempotency_key: body.idempotency_key,
            metadata: body.metadata,
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}

async fn action_surface_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SurfaceActionBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let result = state
        .services
        .surface
        .action(SurfaceActionRequest {
            surface: id,
            action: body.action,
            payload: body.payload,
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}

async fn get_surface_inbox_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let surface = surface::normalize_surface_id(&id);
    Ok(Json(serde_json::json!({
        "kind": "surface.inbox",
        "surface": surface,
        "inbox": state.services.surface.inbox(&id),
        "snapshot": state.services.surface.message_snapshot(&id),
    })))
}

async fn get_surface_outbox_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let surface = surface::normalize_surface_id(&id);
    Ok(Json(serde_json::json!({
        "kind": "surface.outbox",
        "surface": surface,
        "outbox": state.services.surface.outbox(&id),
        "dead_letters": state.services.surface.message_snapshot(&id).dead_letters,
    })))
}

async fn get_surface_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let snapshot = state.services.surface.message_snapshot(&id);
    Ok(Json(serde_json::json!({
        "kind": "surface.messages",
        "surface": surface::normalize_surface_id(&id),
        "message_root": state.services.surface.message_store_root(),
        "snapshot": snapshot,
    })))
}

async fn archive_surface_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ArchiveMessagesBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let archived = state
        .services
        .surface
        .archive_dead_letters(&id, body.older_than_ms, body.limit.clamp(1, 1000))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.messages.archive",
        "surface": surface::normalize_surface_id(&id),
        "archived_count": archived.len(),
        "archived": archived,
        "snapshot": state.services.surface.message_snapshot(&id),
    })))
}

async fn purge_archived_surface_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<PurgeMessagesBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let purged_count = state
        .services
        .surface
        .purge_archived_events(&id, body.older_than_ms, body.limit.clamp(1, 1000))
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.messages.purge_archived_events",
        "surface": surface::normalize_surface_id(&id),
        "purged_count": purged_count,
        "snapshot": state.services.surface.message_snapshot(&id),
    })))
}

async fn get_surface_deliveries_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(&id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let surface = surface::normalize_surface_id(&id);
    Ok(Json(serde_json::json!({
        "kind": "surface.deliveries",
        "surface": surface,
        "deliveries": state.services.surface.delivery_events(&id),
    })))
}

async fn replay_surface_inbox_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, message_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let record = state
        .services
        .surface
        .replay_inbox_message(&id, &message_id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.inbox.replay",
        "surface": surface::normalize_surface_id(&id),
        "message_id": message_id,
        "record": record,
        "status": "queued",
    })))
}

async fn retry_surface_outbox_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, delivery_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    ensure_surface_delivery(&state, &id, &delivery_id)?;
    let result = state
        .services
        .surface
        .retry_outbox_delivery(&delivery_id)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.outbox.retry",
        "surface": surface::normalize_surface_id(&id),
        "delivery_id": delivery_id,
        "result": result,
    })))
}

async fn dead_letter_surface_outbox_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((id, delivery_id)): Path<(String, String)>,
    Json(body): Json<DeadLetterBody>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    ensure_surface_delivery(&state, &id, &delivery_id)?;
    let record = state
        .services
        .surface
        .dead_letter_outbox_delivery(&delivery_id, body.reason)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    Ok(Json(serde_json::json!({
        "kind": "surface.outbox.dead_letter",
        "surface": surface::normalize_surface_id(&id),
        "delivery_id": delivery_id,
        "record": record,
    })))
}

fn ensure_surface_delivery(
    state: &AppState,
    id: &str,
    delivery_id: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if !state.services.surface.has_surface(id) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface `{id}` not found"),
        ));
    }
    let normalized = surface::normalize_surface_id(id);
    let belongs_to_surface = state
        .services
        .surface
        .outbox(&normalized)
        .iter()
        .any(|record| record.delivery_id == delivery_id);
    if !belongs_to_surface {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface delivery `{normalized}/{delivery_id}` not found"),
        ));
    }
    Ok(())
}

async fn surface_static_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((surface, path)): Path<(String, String)>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let Some(file) = state
        .services
        .surface
        .resolve_static(&surface, &path)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?
    else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("surface static resource `{surface}/{path}` not found"),
        ));
    };
    let bytes = tokio::fs::read(&file.file_path)
        .await
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error.to_string()))?;
    let content_type = content_type_for_path(&file.file_path);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header("x-cowd-edge-surface", file.surface)
        .header("x-cowd-edge-spa-fallback", file.spa_fallback.to_string())
        .body(axum::body::Body::from(bytes))
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn surface_callback_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    method: Method,
    Path((surface, path)): Path<(String, String)>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let payload = serde_json::from_slice::<serde_json::Value>(&body).unwrap_or_else(|_| {
        serde_json::json!({
            "raw": String::from_utf8_lossy(&body).to_string()
        })
    });
    let result = state
        .services
        .surface
        .callback(&surface, &format!("/{path}"), method.as_str(), payload)
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error))?;
    Ok(Json(result))
}

fn content_type_for_path(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
    {
        "css" => "text/css; charset=utf-8",
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}
