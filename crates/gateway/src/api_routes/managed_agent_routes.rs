//! Gateway facade for Runtime-owned Managed Agent definitions and views.
//!
//! Surfaces can inspect, register and trigger definitions here, but all
//! deduplication, scheduling, execution, effect fencing and recovery remain
//! inside `runtime::ManagedAgentDispatcher`.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Extension, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use super::{AppState, AuthenticatedPrincipal, ErrorResponse, api_error};

#[derive(Debug, Deserialize)]
struct ManualTriggerRequest {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct DispatchRequest {
    #[serde(default = "default_dispatcher_id")]
    dispatcher_id: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_dispatcher_id() -> String {
    "gateway-api".to_string()
}

fn default_limit() -> usize {
    16
}

fn runtime_services(
    state: &AppState,
) -> Result<Arc<runtime::RuntimeServices>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.runtime_services())
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime_managed_agents_unavailable",
            )
        })
}

fn require_definition_manager(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.is_human_interactive() && principal.0.has_capability("definition.manage") {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "managed_agent_human_definition_manage_capability_required",
        ))
    }
}

fn require_runtime_maintenance(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.is_human_interactive()
        && principal.0.has_capability("runtime.maintenance.manage")
    {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "managed_agent_human_runtime_maintenance_capability_required",
        ))
    }
}

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/runtime/managed-agents",
            get(managed_agent_projection_handler),
        )
        .route(
            "/api/runtime/managed-agents/definitions",
            get(managed_agent_definitions_handler).post(managed_agent_definition_create_handler),
        )
        .route(
            "/api/runtime/managed-agents/:id/trigger",
            post(managed_agent_manual_trigger_handler),
        )
        .route(
            "/api/runtime/managed-agents/:id/health/reset",
            post(managed_agent_health_reset_handler),
        )
        .route(
            "/api/runtime/managed-agents/dispatch",
            post(managed_agent_dispatch_handler),
        )
        .route(
            "/api/runtime/managed-agents/events",
            post(managed_agent_event_handler),
        )
        .route(
            "/api/runtime/managed-agents/effects",
            get(managed_agent_effects_handler),
        )
}

async fn managed_agent_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_services(&state)?
        .managed_agents()
        .projection()
        .map(Json)
        .map_err(managed_error)
}

async fn managed_agent_definitions_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_services(&state)?
        .managed_agents()
        .definitions()
        .map(|definitions| {
            Json(serde_json::json!({
                "kind": "runtime.managed_agent.definitions",
                "definitions": definitions,
            }))
        })
        .map_err(managed_error)
}

async fn managed_agent_definition_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(definition): Json<harness_contract::managed_agent::ManagedAgentDefinition>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_definition_manager(&principal)?;
    runtime_services(&state)?
        .register_managed_agent(definition)
        .map(|definition| {
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "kind": "runtime.managed_agent.definition_registered",
                    "definition": definition,
                })),
            )
        })
        .map_err(managed_error)
}

async fn managed_agent_manual_trigger_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    Json(request): Json<ManualTriggerRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_definition_manager(&principal)?;
    if request.request_id.trim().is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "request_id_is_required"));
    }
    runtime_services(&state)?
        .trigger_managed_agent_manual(&id, &request.request_id)
        .map(|invocation| {
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "kind": "runtime.managed_agent.manual_trigger_accepted",
                    "invocation": invocation,
                })),
            )
        })
        .map_err(managed_error)
}

async fn managed_agent_dispatch_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<DispatchRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_runtime_maintenance(&principal)?;
    if request.dispatcher_id.trim().is_empty() || request.limit == 0 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "dispatcher_id_and_positive_limit_are_required",
        ));
    }
    runtime_services(&state)?
        .dispatch_managed_agents(&request.dispatcher_id, request.limit)
        .await
        .map(Json)
        .map_err(managed_error)
}

async fn managed_agent_health_reset_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_runtime_maintenance(&principal)?;
    runtime_services(&state)?
        .reset_managed_agent_health(&id)
        .map(Json)
        .map_err(managed_error)
}

async fn managed_agent_event_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(event): Json<harness_contract::managed_agent::ManagedAgentTriggerEvent>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    // This is a controlled injection point for an authenticated operator or
    // test. Connector adapters call the same Runtime method after Gateway
    // normalizes their transport event.
    require_runtime_maintenance(&principal)?;
    runtime_services(&state)?
        .accept_managed_agent_event(event)
        .map(Json)
        .map_err(managed_error)
}

async fn managed_agent_effects_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    runtime_services(&state)?
        .managed_agents()
        .outbox()
        .map(|effects| {
            Json(serde_json::json!({
                "kind": "runtime.managed_agent.effects",
                "effects": effects,
            }))
        })
        .map_err(managed_error)
}

fn managed_error(error: impl ToString) -> (StatusCode, Json<ErrorResponse>) {
    api_error(StatusCode::BAD_REQUEST, error.to_string())
}
