use super::*;

#[derive(Debug, Default, Deserialize)]
pub(super) struct ConnectorResourceQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConnectorResourceRevalidateRequest {
    reference: String,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConnectorResourcePromoteMemoryRequest {
    reference: String,
    #[serde(default)]
    session_id: Option<String>,
}

pub(super) async fn connector_resources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<ConnectorResourceQuery>,
) -> impl IntoResponse {
    Json(connector_resources_snapshot(
        &state,
        query.limit,
        query.offset,
        query.q.as_deref(),
    ))
}

pub(crate) fn connector_resources_snapshot(
    state: &AppState,
    limit: Option<usize>,
    offset: Option<usize>,
    query: Option<&str>,
) -> serde_json::Value {
    let limit = limit
        .unwrap_or(DEFAULT_CONNECTOR_RESOURCE_PAGE)
        .clamp(1, MAX_CONNECTOR_RESOURCE_PAGE);
    let offset = offset.unwrap_or(0);
    let (resources, error) = list_durable_resources(state, limit, offset, query);
    let total = resources.len();
    serde_json::json!({
        "kind": "connector_resources",
        "ok": error.is_none(),
        "status": if error.is_some() { "degraded" } else { "available" },
        "degraded_reason": error,
        "limit": limit,
        "offset": offset,
        "resources": resources,
        "total": total,
    })
}

pub(super) async fn connector_resource_revalidate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<ConnectorResourceRevalidateRequest>,
) -> impl IntoResponse {
    Json(connector_resource_revalidate_snapshot(
        &state,
        &request.reference,
        request.state.as_deref(),
    ))
}

pub(crate) fn connector_resource_revalidate_snapshot(
    state: &AppState,
    reference: &str,
    state_value: Option<&str>,
) -> serde_json::Value {
    let reference = reference.trim();
    if reference.is_empty() {
        return serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": false,
            "reason": "reference is required",
        });
    }
    let desired_state = state_value.unwrap_or("indexed");
    let result = state.services.connector.mark_resource_state(
        &state.workspace_root,
        reference,
        desired_state,
    );
    match result {
        Ok((changed, resource, reason)) => serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": changed && reason.is_none(),
            "state": desired_state,
            "changed": changed,
            "resource": resource,
            "reason": reason,
        }),
        Err(error) => serde_json::json!({
            "kind": "connector_resource_revalidation",
            "ok": false,
            "state": desired_state,
            "changed": false,
            "resource": null,
            "reason": error.to_string(),
        }),
    }
}

pub(super) async fn connector_resource_promote_memory_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<ConnectorResourcePromoteMemoryRequest>,
) -> impl IntoResponse {
    Json(
        connector_resource_promote_memory_snapshot(&state, &request.reference, request.session_id)
            .await,
    )
}

pub(crate) async fn connector_resource_promote_memory_snapshot(
    state: &AppState,
    reference: &str,
    session_id: Option<String>,
) -> serde_json::Value {
    let Some(memory_manager) = state.services.memory.manager() else {
        return serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": "memory not configured",
        });
    };
    let reference = reference.trim();
    if reference.is_empty() {
        return serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": "reference is required",
        });
    }
    let resource = match state
        .services
        .connector
        .get_resource(&state.workspace_root, reference)
    {
        Ok(Some(resource)) => resource,
        Ok(None) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reason": "resource ref not found",
            });
        }
        Err(error) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reason": error.to_string(),
            });
        }
    };
    let content = connector_resource_memory_content(&resource);
    let memory_scope = session_id
        .clone()
        .map(MemoryScope::Session)
        .unwrap_or_else(|| MemoryScope::Project("connector-resource".to_string()));
    match find_existing_connector_resource_memory(&memory_manager, &memory_scope, reference).await {
        Ok(Some(existing_id)) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": true,
                "replayed": true,
                "memory_id": existing_id,
                "layer": "L3",
                "reference": reference,
                "reason": "resource memory already exists",
            });
        }
        Ok(None) => {}
        Err(error) => {
            return serde_json::json!({
                "kind": "connector_resource_memory_promotion",
                "ok": false,
                "reference": reference,
                "reason": format!("memory dedup failed: {error}"),
            });
        }
    }

    let id = MemoryId::new_v4();
    let entry = MemoryEntry {
        id,
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: format!("Connector resource: {}", resource.title),
        content,
        embedding: None,
        tags: vec![
            "connector_resource".to_string(),
            connector_resource_reference_tag(reference),
            resource.provider.clone(),
            resource.resource_type.clone(),
        ],
        relations: vec![],
        confidence: 0.86,
        access_count: 0,
        staleness: if resource.indexed_state == "stale" {
            0.35
        } else {
            0.0
        },
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope: memory_scope,
        session_id,
        source_agent: Some("connector-resource-bridge".to_string()),
        visibility: AgentVisibility::Shared,
    };
    match state
        .services
        .memory
        .remember_entry_with_context(entry, "connector-resource-bridge", "api")
        .await
    {
        Ok(()) => serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": true,
            "memory_id": id,
            "layer": "L3",
            "reference": reference,
        }),
        Err(error) => serde_json::json!({
            "kind": "connector_resource_memory_promotion",
            "ok": false,
            "reason": error.to_string(),
        }),
    }
}

async fn find_existing_connector_resource_memory(
    memory_manager: &Arc<GatewayMemoryManager>,
    scope: &MemoryScope,
    reference: &str,
) -> Result<Option<MemoryId>, String> {
    let entries = memory_manager
        .tagged_candidates(memory::TaggedLookup {
            scope: scope.clone(),
            tags_any: vec![connector_resource_reference_tag(reference)],
            source_agent: Some("connector-resource-bridge".to_string()),
            limit: 2,
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(entries.into_iter().next().map(|entry| entry.id))
}

fn connector_resource_reference_tag(reference: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(reference.trim().as_bytes());
    format!("connector-ref:{:x}", hasher.finalize())
}

pub(super) fn list_durable_resources(
    state: &AppState,
    limit: usize,
    offset: usize,
    query: Option<&str>,
) -> (Vec<ExternalResourceRef>, Option<String>) {
    match state
        .services
        .connector
        .list_resources(&state.workspace_root, limit, offset, query)
    {
        Ok(resources) => (resources, None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    }
}

fn connector_resource_memory_content(resource: &ExternalResourceRef) -> String {
    let mut lines = vec![
        format!("resource: {}", resource.title),
        format!("ref: {}", resource.reference),
        format!("provider: {}", resource.provider),
        format!("type: {}", resource.resource_type),
        format!("indexed_state: {}", resource.indexed_state),
        "body_policy: metadata_only".to_string(),
        "evidence: resolve resource ref before relying on external body content".to_string(),
    ];
    if let Some(source) = &resource.source {
        lines.push(format!("source: {source}"));
    }
    if let Some(permissions) = &resource.permissions_summary {
        lines.push(format!("permissions: {permissions}"));
    }
    lines.join("\n")
}
