use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct MockDocsExecuteRequest {
    actor_principal: String,
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

pub(super) async fn mock_docs_tools_handler() -> impl IntoResponse {
    let connector = MockDocsServiceConnector::new();
    Json(serde_json::json!({
        "kind": "connector_service_tools",
        "service": connector.metadata(),
        "tools": connector.capabilities(),
    }))
}

pub(super) async fn mock_docs_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<MockDocsExecuteRequest>,
) -> impl IntoResponse {
    state.services.cross_plane.ensure_loaded(&state.config_home);
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
                "service": "mock.docs",
                "replayed": true,
                "receipt": receipt,
            }));
        }
    }

    let service_request = ServiceToolRequest {
        tool_id: request.tool_id.clone(),
        resource_id: request.resource_id,
        title: request.title,
        input: serde_json::json!({}),
    };
    let preview_resource = ExternalResourceRef::new(
        "mock.docs",
        "document",
        &service_request.resource_id,
        &service_request.title,
    );
    let action = state.services.connector.service_action(
        request.actor_principal,
        request.tool_id,
        request.actor_identity_ref,
        request.source_channel,
        request.session_id,
        "mock.docs",
        Some(preview_resource.reference.clone()),
    );

    let snapshot = connector_snapshot(&state);
    let (action, decision, mut evidence) = state.services.cross_plane.decide_connector_action(
        &snapshot,
        action,
        mode,
        chrono::Utc::now(),
    );
    state.services.cross_plane.save_state(&state.config_home);

    let policy_allowed = state.services.connector.policy_allows(&decision);
    let mut allowed = policy_allowed;
    let mut bulkhead_guard = None;
    let mut bulkhead_blocker = None;
    if mode == "commit" && allowed {
        match connector_service_bulkhead().try_acquire("mock.docs") {
            Ok(guard) => {
                bulkhead_guard = Some(guard);
            }
            Err(error) => {
                allowed = false;
                bulkhead_blocker = Some(connector_bulkhead_blocker(error));
            }
        }
    }
    let status = if mode == "commit" && allowed {
        "executed"
    } else if allowed {
        "dry_run"
    } else {
        "blocked"
    };
    let dispatch_status = if mode == "commit" && allowed {
        "service_mock_executed"
    } else {
        "not_dispatched"
    };
    let mut blockers = Vec::new();
    if !policy_allowed {
        blockers.push(format!("policy:{}", decision.reason));
    }
    if let Some(blocker) = bulkhead_blocker {
        blockers.push(blocker);
    }
    if mode == "commit" && allowed {
        if let Some((grant_id, remaining)) = state
            .services
            .cross_plane
            .consume_matched_grant_for_decision(&decision)
        {
            evidence.consumed_grant_id = Some(grant_id);
            evidence.remaining_uses_after = Some(remaining);
        }
    }
    let audit_summary = if blockers.is_empty() {
        format!("mock.docs {status}")
    } else {
        blockers.join("; ")
    };
    let receipt = state.services.connector.record_service_execution_receipt(
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
    );
    state.services.cross_plane.save_state(&state.config_home);
    let service_result = if mode == "commit" && allowed {
        let result = MockDocsServiceConnector::new().execute_tool(service_request);
        connector_service_bulkhead().record_success("mock.docs");
        drop(bulkhead_guard);
        result
    } else {
        ServiceToolResult {
            status: status.to_string(),
            tool_id: receipt.action.requested_capability.clone(),
            resource: Some(preview_resource.clone()),
            output: serde_json::json!({
                "summary": format!("Mock docs service {} for {}", status, preview_resource.reference),
                "read_only": true,
            }),
        }
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
        "service": "mock.docs",
        "replayed": false,
        "result": service_result,
        "resource_persisted": resource_persisted,
        "resource_degraded_reason": resource_degraded_reason,
        "receipt": receipt,
    }))
}

fn connector_bulkhead_blocker(error: ConnectorBulkheadRejection) -> String {
    match error {
        ConnectorBulkheadRejection::Busy {
            provider,
            in_flight,
            max_in_flight,
        } => format!("connector.bulkhead:{provider}:busy:{in_flight}/{max_in_flight}"),
        ConnectorBulkheadRejection::CoolingDown { provider } => {
            format!("connector.bulkhead:{provider}:cooling_down")
        }
    }
}
