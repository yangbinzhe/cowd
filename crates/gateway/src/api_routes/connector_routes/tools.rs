use super::*;
use crate::api_routes::{principal_actor_id, AuthenticatedPrincipal};
use axum::{http::StatusCode, Extension};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServiceExecuteRequest {
    #[serde(default)]
    actor_identity_ref: Option<String>,
    #[serde(default)]
    source_channel: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    tool_id: String,
    resource_id: String,
    title: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

pub(super) async fn connector_services_handler() -> impl IntoResponse {
    let registry = builtin_service_connector_registry();
    Json(serde_json::json!({
        "kind": "connector_services",
        "services": registry.services(),
    }))
}

pub(super) async fn connector_service_tools_handler(
    Path(service_id): Path<String>,
) -> impl IntoResponse {
    let registry = builtin_service_connector_registry();
    let Some(connector) = registry.connector(&service_id) else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "connector service not found",
                "service": service_id,
            })),
        )
            .into_response();
    };
    Json(serde_json::json!({
        "kind": "connector_service_tools",
        "service": connector.metadata(),
        "tools": connector.capabilities(),
    }))
    .into_response()
}

pub(super) async fn connector_service_execute_handler(
    Path(service_id): Path<String>,
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ServiceExecuteRequest>,
) -> impl IntoResponse {
    let registry = builtin_service_connector_registry();
    let Some(connector) = registry.connector(&service_id) else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "connector service not found",
                "service": service_id,
            })),
        )
            .into_response();
    };
    let service_metadata = connector.metadata();
    let mode = request.mode.as_deref().unwrap_or("dry_run");
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(key) = &idempotency_key {
        if let Some(receipt) = state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(key)
        {
            return Json(serde_json::json!({
                "kind": "connector_service_execution",
                "service": service_metadata.id,
                "replayed": true,
                "receipt": receipt,
            }))
            .into_response();
        }
    }

    let service_request = ServiceToolRequest {
        tool_id: request.tool_id.clone(),
        resource_id: request.resource_id,
        title: request.title,
        input: serde_json::json!({}),
    };
    let preview_resource = ExternalResourceRef::new(
        &service_metadata.id,
        "document",
        &service_request.resource_id,
        &service_request.title,
    );
    let action = state.services.connector.service_action(
        principal_actor_id(&principal),
        request.tool_id,
        request.actor_identity_ref,
        request.source_channel,
        request.session_id,
        &service_metadata.id,
        Some(preview_resource.reference.clone()),
    );

    let snapshot = connector_snapshot(&state);
    let (action, decision, evidence) = state.services.cross_plane.decide_connector_action(
        &snapshot,
        action,
        mode,
        chrono::Utc::now(),
    );

    let policy_allowed = state.services.connector.policy_allows(&decision);
    let allowed = policy_allowed;
    let graph_key = idempotency_key
        .clone()
        .unwrap_or_else(|| format!("connector-{}", uuid::Uuid::new_v4()));
    let execution_graph = if mode == "commit" && allowed {
        let executor = Arc::new(crate::services::GatewayConnectorServiceExecutor::new(
            service_metadata.id.clone(),
            service_request.clone(),
        ));
        state
            .services
            .cross_plane
            .execute_commit_graph(&action, &decision, &graph_key, executor)
            .await
            .ok()
    } else {
        None
    };
    let graph_registered = execution_graph.is_some();
    let graph_completed = execution_graph.as_ref().is_some_and(|graph| {
        graph.nodes.iter().any(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
                && node.status == harness_contract::execution_graph::ExecutionNodeStatus::Completed
        })
    });
    let status = if mode == "commit" && graph_completed {
        "executed"
    } else if allowed && mode != "commit" {
        "dry_run"
    } else {
        "blocked"
    };
    let dispatch_status = if mode == "commit" && graph_completed {
        "service_executed"
    } else {
        "not_dispatched"
    };
    let mut blockers = Vec::new();
    if !policy_allowed {
        blockers.push(format!("policy:{}", decision.reason));
    }
    if mode == "commit" && !graph_registered {
        blockers.push("execution_graph:registration_failed".to_string());
    }
    let audit_summary = if blockers.is_empty() {
        format!("{} {status}", service_metadata.id)
    } else {
        blockers.join("; ")
    };
    let receipt = match state.services.connector.record_service_execution_receipt(
        &state.services.cross_plane,
        idempotency_key,
        mode,
        status,
        dispatch_status,
        action,
        decision,
        blockers,
        evidence,
        audit_summary,
        execution_graph.as_ref().map(|graph| graph.graph_id.clone()),
    ) {
        Ok(receipt) => receipt,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response();
        }
    };
    let service_result = ServiceToolResult {
        status: status.to_string(),
        tool_id: receipt.action.requested_capability.clone(),
        resource: Some(preview_resource.clone()),
        output: serde_json::json!({
            "summary": format!("Connector service {} {} for {}", service_metadata.id, status, preview_resource.reference),
            "read_only": true,
        }),
    };
    let mut resource_persisted = false;
    let mut resource_degraded_reason = None;
    if let Some(resource) = service_result.resource.clone() {
        match state
            .services
            .connector
            .upsert_resource(&state.workspace_root, &resource)
        {
            Ok(_) => {
                resource_persisted = true;
            }
            Err(error) => {
                resource_degraded_reason = Some(format!("resource directory unavailable: {error}"));
            }
        }
    }

    Json(serde_json::json!({
        "kind": "connector_service_execution",
        "service": service_metadata.id,
        "replayed": false,
        "result": service_result,
        "resource_persisted": resource_persisted,
        "resource_degraded_reason": resource_degraded_reason,
        "execution_graph": execution_graph,
        "receipt": receipt,
    }))
    .into_response()
}
