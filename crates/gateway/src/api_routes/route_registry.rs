//! Typed registration for stable Gateway routes.
//!
//! A route spec is the single stable identity for an Axum registration and
//! its externally visible schema. The execution projection family is the
//! first migration boundary; other families can move here without creating a
//! second manifest/OpenAPI list.

use std::{marker::PhantomData, sync::Arc};

use axum::{
    routing::{get, post},
    Router,
};
use harness_contract::{
    live::{
        CreateLiveSubscriptionRequest, LiveEnvelope, LiveSubscription, PatchLiveSubscriptionRequest,
    },
    projection::{
        ExecutionCommandReceipt, ExecutionCommandRequest, ExecutionProjection,
        SessionEvidenceProjection, SessionExecutionIndexProjection,
        SessionExecutionIndicesProjection, TurnEvidenceProjection,
    },
};

use super::{runtime_routes, AppState};

/// Build-generated metadata for literal Axum route registrations. The build
/// script watches every route source and emits this registry once; runtime
/// contract/OpenAPI consumers never parse Rust source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GeneratedRouteMetadata {
    pub(crate) method: &'static str,
    pub(crate) path: &'static str,
    pub(crate) source: &'static str,
    pub(crate) handler: &'static str,
}

include!(concat!(env!("OUT_DIR"), "/gateway_route_registry.rs"));

pub(crate) fn generated_route_metadata() -> &'static [GeneratedRouteMetadata] {
    GENERATED_ROUTE_METADATA
}

pub(crate) struct TypedRouteSpec<Request, Query, Response> {
    pub(crate) method: &'static str,
    pub(crate) path: &'static str,
    pub(crate) operation_id: &'static str,
    pub(crate) _request: PhantomData<Request>,
    pub(crate) _query: PhantomData<Query>,
    pub(crate) _response: PhantomData<Response>,
}

impl<Request, Query, Response> TypedRouteSpec<Request, Query, Response> {
    const fn new(method: &'static str, path: &'static str, operation_id: &'static str) -> Self {
        Self {
            method,
            path,
            operation_id,
            _request: PhantomData,
            _query: PhantomData,
            _response: PhantomData,
        }
    }

    fn metadata(
        &self,
        request_schema: Option<&'static str>,
        response_schema: &'static str,
        streaming: bool,
    ) -> StableRouteMetadata {
        StableRouteMetadata {
            method: self.method,
            path: self.path.to_string(),
            operation_id: self.operation_id.to_string(),
            request_schema: request_schema.map(str::to_string),
            response_schema: response_schema.to_string(),
            streaming,
        }
    }
}

/// Non-generic metadata consumed by capability/OpenAPI generation. It is
/// derived from a `TypedRouteSpec`; no feature-specific string matching is
/// permitted outside this registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StableRouteMetadata {
    pub(crate) method: &'static str,
    pub(crate) path: String,
    pub(crate) operation_id: String,
    pub(crate) request_schema: Option<String>,
    pub(crate) response_schema: String,
    pub(crate) streaming: bool,
}

fn execution_projection_snapshot_spec(
) -> TypedRouteSpec<(), runtime_routes::ExecutionProjectionQuery, ExecutionProjection> {
    TypedRouteSpec::new(
        "GET",
        "/api/runtime/executions/:id",
        "runtime_execution_projection_get",
    )
}

fn execution_projection_command_spec(
) -> TypedRouteSpec<ExecutionCommandRequest, (), ExecutionCommandReceipt> {
    TypedRouteSpec::new(
        "POST",
        "/api/runtime/executions/:id/commands",
        "runtime_execution_projection_command",
    )
}

fn session_execution_indices_spec() -> TypedRouteSpec<(), (), SessionExecutionIndicesProjection> {
    TypedRouteSpec::new(
        "GET",
        "/api/sessions/executions",
        "session_execution_indices_get",
    )
}

fn session_execution_index_spec() -> TypedRouteSpec<(), (), SessionExecutionIndexProjection> {
    TypedRouteSpec::new(
        "GET",
        "/api/sessions/:id/execution",
        "session_execution_index_get",
    )
}

fn session_evidence_spec() -> TypedRouteSpec<(), (), SessionEvidenceProjection> {
    TypedRouteSpec::new("GET", "/api/sessions/:id/evidence", "session_evidence_get")
}

fn turn_evidence_spec() -> TypedRouteSpec<(), (), TurnEvidenceProjection> {
    TypedRouteSpec::new(
        "GET",
        "/api/sessions/:id/turns/:turn_id/evidence",
        "session_turn_evidence_get",
    )
}

fn live_create_spec() -> TypedRouteSpec<CreateLiveSubscriptionRequest, (), LiveSubscription> {
    TypedRouteSpec::new(
        "POST",
        "/api/runtime/live-subscriptions",
        "runtime_live_subscription_create",
    )
}

fn live_patch_spec() -> TypedRouteSpec<PatchLiveSubscriptionRequest, (), LiveSubscription> {
    TypedRouteSpec::new(
        "PATCH",
        "/api/runtime/live-subscriptions/:id",
        "runtime_live_subscription_patch",
    )
}

fn live_delete_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        "DELETE",
        "/api/runtime/live-subscriptions/:id",
        "runtime_live_subscription_delete",
    )
}

fn live_stream_spec() -> TypedRouteSpec<(), (), LiveEnvelope> {
    TypedRouteSpec::new("GET", "/api/runtime/live/:id", "runtime_live_stream_get")
}

pub(crate) fn typed_route_metadata() -> Vec<StableRouteMetadata> {
    vec![
        execution_projection_snapshot_spec().metadata(None, "ExecutionProjection", false),
        execution_projection_command_spec().metadata(
            Some("ExecutionCommandRequest"),
            "ExecutionCommandReceipt",
            false,
        ),
        session_execution_indices_spec().metadata(None, "SessionExecutionIndicesProjection", false),
        session_execution_index_spec().metadata(None, "SessionExecutionIndexProjection", false),
        session_evidence_spec().metadata(None, "SessionEvidenceProjection", false),
        turn_evidence_spec().metadata(None, "TurnEvidenceProjection", false),
        live_create_spec().metadata(
            Some("CreateLiveSubscriptionRequest"),
            "LiveSubscription",
            false,
        ),
        live_patch_spec().metadata(
            Some("PatchLiveSubscriptionRequest"),
            "LiveSubscription",
            false,
        ),
        live_delete_spec().metadata(None, "Empty", false),
        live_stream_spec().metadata(None, "LiveEnvelope", true),
    ]
}

pub(crate) fn stable_route_metadata(method: &str, path: &str) -> Option<StableRouteMetadata> {
    typed_route_metadata()
        .into_iter()
        .find(|spec| spec.method == method && spec.path == path)
}

pub(super) fn register_execution_projection_routes(
    router: Router<Arc<AppState>>,
) -> Router<Arc<AppState>> {
    let snapshot = execution_projection_snapshot_spec();
    let command = execution_projection_command_spec();
    router
        .route(snapshot.path, get(runtime_routes::get_execution_projection))
        .route(
            command.path,
            post(runtime_routes::execute_projection_command),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_specs_cover_projection_and_live_contracts() {
        let specs = typed_route_metadata();
        assert_eq!(specs.len(), 10);
        assert_eq!(specs[0].operation_id, "runtime_execution_projection_get");
        assert_eq!(specs[0].response_schema, "ExecutionProjection");
        assert_eq!(
            specs[1].request_schema,
            Some("ExecutionCommandRequest".to_string())
        );
        assert_eq!(specs[1].response_schema, "ExecutionCommandReceipt");
        assert_eq!(
            specs[2].response_schema,
            "SessionExecutionIndicesProjection"
        );
        assert_eq!(specs[3].operation_id, "session_execution_index_get");
        assert_eq!(specs[4].response_schema, "SessionEvidenceProjection");
        assert_eq!(specs[5].response_schema, "TurnEvidenceProjection");
        assert_eq!(
            specs[6].request_schema,
            Some("CreateLiveSubscriptionRequest".to_string())
        );
        assert_eq!(specs[9].response_schema, "LiveEnvelope");
        assert!(specs[9].streaming);
    }
}
