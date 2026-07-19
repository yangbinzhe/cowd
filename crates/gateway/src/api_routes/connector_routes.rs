use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State as AxumState},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use connector::{
    builtin_service_connector_registry, builtin_source_adapter_manifests, default_capabilities,
    CapabilityManifest, ConnectorHealth, ConnectorRegistrySnapshot, ExternalResourceRef,
    ProviderAccount, ServiceConnector, ServiceToolRequest, ServiceToolResult, SourceConnectorState,
    SourceIncrementalRunRequest, SourceIncrementalRunResult, SourceReadPlan, SourceWatermark,
};
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::MemoryScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::services::GatewayMemoryManager;

use super::{message_connector_routes, AppState};

mod mcp;
mod resources;
mod tools;

use mcp::*;
use resources::*;
use tools::*;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/connectors/summary", get(connector_summary_handler))
        .route("/api/connectors/accounts", get(connector_accounts_handler))
        .route(
            "/api/connectors/capabilities",
            get(connector_capabilities_handler),
        )
        .route("/api/connectors/mcp/servers", get(mcp_servers_handler))
        .route(
            "/api/connectors/resources",
            get(connector_resources_handler),
        )
        .route("/api/connectors/sources", get(connector_sources_handler))
        .route(
            "/api/connectors/sources/:adapter_id/state",
            get(connector_source_state_handler),
        )
        .route(
            "/api/connectors/sources/:adapter_id/run-incremental",
            axum::routing::post(connector_source_run_incremental_handler),
        )
        .route(
            "/api/connectors/sources/:adapter_id/poll-events",
            axum::routing::post(connector_source_poll_events_handler),
        )
        .route(
            "/api/connectors/resources/revalidate",
            axum::routing::post(connector_resource_revalidate_handler),
        )
        .route(
            "/api/connectors/resources/promote-memory",
            axum::routing::post(connector_resource_promote_memory_handler),
        )
        .route("/api/connectors/services", get(connector_services_handler))
        .route(
            "/api/connectors/services/:service_id/tools",
            get(connector_service_tools_handler),
        )
        .route(
            "/api/connectors/services/:service_id/execute",
            axum::routing::post(connector_service_execute_handler),
        )
}

async fn connector_sources_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let mut adapters = Vec::new();
    for manifest in builtin_source_adapter_manifests() {
        let runtime_state = source_state_for_manifest(&state, &manifest).await;
        adapters.push(serde_json::json!({
            "manifest": manifest,
            "adapter_id": runtime_state.adapter_id,
            "runtime_state": runtime_state,
        }));
    }
    Json(serde_json::json!({
        "kind": "connector.source_adapters",
        "adapters": adapters,
    }))
}

#[derive(Debug, Deserialize)]
struct ConnectorSourceRunRequest {
    #[serde(default)]
    resource_ref: Option<String>,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    watermark: Option<SourceWatermark>,
    #[serde(default)]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
struct ConnectorSourcePollEventsRequest {
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    events: Vec<Value>,
    #[serde(default)]
    event_fixture_path: Option<String>,
}

async fn connector_source_state_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(adapter_id): Path<String>,
) -> impl IntoResponse {
    let manifest = connector::source_adapter_manifest(&adapter_id);
    let runtime_state = match manifest.as_ref() {
        Some(manifest) => source_state_for_manifest(&state, manifest).await,
        None => source_static_state(&adapter_id, "unsupported_adapter"),
    };
    Json(serde_json::json!({
        "kind": "connector.source.state",
        "adapter_id": adapter_id,
        "state": runtime_state,
    }))
}

async fn connector_source_run_incremental_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(adapter_id): Path<String>,
    Json(request): Json<ConnectorSourceRunRequest>,
) -> impl IntoResponse {
    let Some(manifest) = connector::source_adapter_manifest(&adapter_id) else {
        return Json(serde_json::json!({
            "kind": "connector.source.incremental_run",
            "adapter_id": adapter_id,
            "status": "unsupported_adapter",
            "degraded_reason": "unsupported source adapter",
        }));
    };
    let resource_ref = request.resource_ref.clone().unwrap_or_default();
    let canonical_watermark = if resource_ref.trim().is_empty() {
        None
    } else {
        match state.services.matrix.connector_source_watermark(
            &state.config_home,
            &adapter_id,
            &resource_ref,
            request.table.as_deref(),
        ) {
            Ok(watermark) => watermark,
            Err(error) => {
                return Json(serde_json::json!({
                    "kind": "connector.source.incremental_run",
                    "adapter_id": adapter_id,
                    "status": "degraded_matrix_watermark_read_failed",
                    "degraded_reason": error.to_string(),
                }));
            }
        }
    };
    let mut request = request;
    request.watermark = canonical_watermark.or(request.watermark);
    let sidecar_streamed = manifest.requires_sidecar;
    let mut run = if sidecar_streamed {
        let request = SourceIncrementalRunRequest {
            adapter_id: adapter_id.clone(),
            resource_ref,
            table: request.table.clone(),
            limit: request.limit,
            watermark: request.watermark.clone(),
            expected_revision: request
                .watermark
                .as_ref()
                .map(|watermark| watermark.revision),
            metadata: request.metadata.clone(),
        };
        consume_sidecar_source_stream(&state, request).await
    } else {
        run_local_source_incremental(&adapter_id, request)
    };

    if !sidecar_streamed {
        if let Some(batch) = run.batch.as_ref() {
            match state.services.matrix.ingest_source_record_batch(
                &state.config_home,
                batch,
                run.watermark_before.clone(),
                run.watermark_after.clone(),
            ) {
                Ok(receipt) => {
                    run.watermark_after = receipt.watermark_after.clone();
                    run.receipt = Some(receipt);
                    if run.status == "ok" {
                        run.status = "ingested".to_string();
                    }
                }
                Err(error) => {
                    run.status = "degraded_matrix_ingest_failed".to_string();
                    run.degraded_reason = Some(error.to_string());
                    run.receipt = None;
                }
            }
        }
    }
    let matrix_refs = run
        .receipt
        .as_ref()
        .map(|receipt| receipt.matrix_refs.clone())
        .unwrap_or_default();
    Json(serde_json::json!({
        "kind": "connector.source.incremental_run",
        "adapter_id": adapter_id,
        "state": source_state_for_manifest(&state, &manifest).await,
        "result": run,
        "watermark_before": run.watermark_before,
        "watermark_after": run.watermark_after,
        "degraded_reason": run.degraded_reason,
        "receipt": run.receipt,
        "matrix_refs": matrix_refs,
    }))
}

async fn consume_sidecar_source_stream(
    state: &Arc<AppState>,
    request: SourceIncrementalRunRequest,
) -> SourceIncrementalRunResult {
    let watermark_before = request.watermark.clone();
    let mut stream = match state
        .services
        .surface
        .source_incremental_stream(&request)
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            return SourceIncrementalRunResult {
                status: "degraded_sidecar_failed".to_string(),
                chunk_index: 0,
                final_chunk: true,
                batch: None,
                watermark_before,
                watermark_after: None,
                degraded_reason: Some(error),
                receipt: None,
            };
        }
    };
    let mut expected_chunk = 0usize;
    let mut total_rows = 0usize;
    let mut all_refs = Vec::new();
    let mut final_result = None;
    while let Some(chunk) = stream.recv().await {
        let mut chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                return source_stream_failure(watermark_before, error);
            }
        };
        if chunk.chunk_index != expected_chunk {
            return source_stream_failure(
                watermark_before,
                format!(
                    "source stream chunk gap: expected {expected_chunk}, got {}",
                    chunk.chunk_index
                ),
            );
        }
        expected_chunk = expected_chunk.saturating_add(1);
        let Some(batch) = chunk.batch.take() else {
            return source_stream_failure(
                watermark_before,
                format!("source stream chunk {} has no batch", chunk.chunk_index),
            );
        };
        let receipt = match state.services.matrix.ingest_source_record_chunk(
            &state.config_home,
            &batch,
            watermark_before.clone(),
            chunk.watermark_after.clone(),
            chunk.final_chunk,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                // 返回后 stream 被丢弃，H2 数据流立即取消；候选水位不会提交。
                return source_stream_failure(watermark_before, error.to_string());
            }
        };
        total_rows = total_rows.saturating_add(receipt.row_count);
        all_refs.extend(receipt.matrix_refs.iter().cloned());
        if chunk.final_chunk {
            let mut receipt = receipt;
            receipt.row_count = total_rows;
            receipt.matrix_refs = all_refs;
            chunk.watermark_after = receipt.watermark_after.clone();
            chunk.receipt = Some(receipt);
            chunk.status = if chunk.status == "ok" {
                "ingested".to_string()
            } else {
                chunk.status
            };
            final_result = Some(chunk);
            break;
        }
    }
    final_result.unwrap_or_else(|| {
        source_stream_failure(
            watermark_before,
            "source stream ended before final chunk".to_string(),
        )
    })
}

fn source_stream_failure(
    watermark_before: Option<SourceWatermark>,
    error: String,
) -> SourceIncrementalRunResult {
    SourceIncrementalRunResult {
        status: "degraded_source_stream_failed".to_string(),
        chunk_index: 0,
        final_chunk: true,
        batch: None,
        watermark_before,
        watermark_after: None,
        degraded_reason: Some(error),
        receipt: None,
    }
}

async fn connector_source_poll_events_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(adapter_id): Path<String>,
    Json(request): Json<ConnectorSourcePollEventsRequest>,
) -> impl IntoResponse {
    let mut payload = request.payload;
    if !request.events.is_empty() {
        payload["events"] = Value::Array(request.events);
    }
    if let Some(path) = request.event_fixture_path {
        payload["event_fixture_path"] = Value::String(path);
    }
    match state
        .services
        .surface
        .source_event_poll(&adapter_id, payload)
        .await
    {
        Ok(batch) => {
            let managed_agent = submit_source_batch_to_managed_agents(&state, &adapter_id, &batch);
            let managed_status = managed_agent
                .as_ref()
                .map(|report| report.status.as_str())
                .unwrap_or("unavailable");
            Json(serde_json::json!({
                "kind": "connector.source.event_batch",
                "adapter_id": adapter_id,
                "status": if batch.event_count == 0 { "degraded" } else { "ok" },
                "degraded_reason": if batch.event_count == 0 { Some("requires_external_event_source") } else { None },
                "event_batch": batch,
                "managed_agent": managed_agent,
                "managed_agent_status": managed_status,
            }))
        }
        Err(error) => Json(serde_json::json!({
            "kind": "connector.source.event_batch",
            "adapter_id": adapter_id,
            "status": "degraded_sidecar_failed",
            "degraded_reason": error,
        })),
    }
}

#[derive(Debug, Serialize)]
struct ManagedAgentSourceForwarding {
    status: String,
    accepted: usize,
    suppressed: usize,
    rejected: Vec<String>,
    event_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    degraded_reason: Option<String>,
}

/// Connector sidecars only return source transport facts. Gateway normalizes
/// them once and submits Runtime's canonical trigger event; Runtime remains
/// the sole owner of matching, ordering, idempotency and overlap decisions.
fn submit_source_batch_to_managed_agents(
    state: &AppState,
    adapter_id: &str,
    batch: &connector::SourceEventBatch,
) -> Option<ManagedAgentSourceForwarding> {
    let runtime = state.services.runtime.as_ref()?;
    let source_capabilities = source_event_capabilities(adapter_id);
    let mut accepted = 0;
    let mut suppressed = 0;
    let mut rejected = Vec::new();
    let mut event_ids = Vec::new();
    for (index, event) in batch.events.iter().enumerate() {
        let normalized = normalize_source_event(
            adapter_id,
            batch.resource_ref.as_deref(),
            event,
            index,
            &source_capabilities,
        );
        let event_id = normalized.event_id.clone();
        match runtime
            .runtime_services()
            .accept_managed_agent_event(normalized)
        {
            Ok(report) => {
                accepted += report.accepted.len();
                suppressed += report.suppressed.len();
                rejected.extend(report.rejected);
                event_ids.push(event_id);
            }
            Err(error) => rejected.push(format!("{event_id}: {error}")),
        }
    }
    let status = if rejected.is_empty() {
        "accepted".to_string()
    } else if accepted + suppressed > 0 {
        "partially_degraded".to_string()
    } else {
        "degraded".to_string()
    };
    Some(ManagedAgentSourceForwarding {
        status,
        accepted,
        suppressed,
        rejected: rejected.clone(),
        event_ids,
        degraded_reason: (!rejected.is_empty())
            .then(|| "one or more normalized source events were rejected by Runtime".to_string()),
    })
}

fn source_event_capabilities(adapter_id: &str) -> Vec<String> {
    let mut capabilities = vec![
        "connector.source.event.receive".to_string(),
        format!("connector.source.adapter:{adapter_id}"),
    ];
    if let Some(manifest) = connector::source_adapter_manifest(adapter_id) {
        capabilities.push(format!("connector.source.family:{}", manifest.family));
        if manifest.supports_event_subscription {
            capabilities.push("connector.source.event.subscription".to_string());
        }
    }
    capabilities.sort();
    capabilities.dedup();
    capabilities
}

fn normalize_source_event(
    adapter_id: &str,
    resource_ref: Option<&str>,
    event: &Value,
    index: usize,
    source_capabilities: &[String],
) -> harness_contract::managed_agent::ManagedAgentTriggerEvent {
    let canonical = serde_json::to_vec(event).unwrap_or_default();
    let digest = format!("sha256:{:x}", Sha256::digest(&canonical));
    let event_id = value_string(event, "event_id")
        .or_else(|| value_string(event, "id"))
        .unwrap_or_else(|| format!("{adapter_id}:{index}:{digest}"));
    let event_type = value_string(event, "event_type")
        .or_else(|| value_string(event, "type"))
        .or_else(|| value_string(event, "kind"))
        .unwrap_or_else(|| "source.record.changed".to_string());
    let subject = value_string(event, "subject")
        .or_else(|| value_string(event, "resource_ref"))
        .or_else(|| resource_ref.map(str::to_string))
        .unwrap_or_else(|| adapter_id.to_string());
    let mut attributes = BTreeMap::new();
    attributes.insert("adapter_id".to_string(), adapter_id.to_string());
    if let Some(resource_ref) = resource_ref {
        attributes.insert("resource_ref".to_string(), resource_ref.to_string());
    }
    if let Some(object) = event.as_object() {
        for (key, value) in object {
            if let Some(value) = value.as_str() {
                attributes.insert(key.clone(), value.to_string());
            } else if value.is_boolean() || value.is_number() {
                attributes.insert(key.clone(), value.to_string());
            }
        }
    }
    let occurred_at_ms = event
        .get("occurred_at_ms")
        .and_then(Value::as_u64)
        .or_else(|| event.get("timestamp_ms").and_then(Value::as_u64))
        .unwrap_or_else(now_ms);
    harness_contract::managed_agent::ManagedAgentTriggerEvent {
        event_id: event_id.clone(),
        source_id: adapter_id.to_string(),
        source_kind: "connector_source".to_string(),
        event_type,
        subject,
        payload_ref: format!(
            "connector-source:{adapter_id}:{}:{event_id}",
            resource_ref.unwrap_or("default")
        ),
        payload_digest: digest,
        occurred_at_ms,
        source_sequence: event
            .get("source_sequence")
            .and_then(Value::as_u64)
            .or_else(|| event.get("sequence").and_then(Value::as_u64)),
        idempotency_key: format!("connector-source:{adapter_id}:{event_id}"),
        source_capabilities: source_capabilities.to_vec(),
        attributes,
        trace_refs: vec![format!("connector-source:{adapter_id}:{event_id}")],
    }
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

async fn source_state_for_manifest(
    state: &AppState,
    manifest: &connector::SourceAdapterManifest,
) -> SourceConnectorState {
    if manifest.requires_sidecar {
        state
            .services
            .surface
            .source_state(&manifest.adapter_id)
            .await
            .unwrap_or_else(|error| {
                let mut runtime_state =
                    source_static_state(&manifest.adapter_id, "sidecar_unavailable");
                runtime_state.degraded_reason = Some(error);
                runtime_state
            })
    } else {
        source_static_state(&manifest.adapter_id, "ready")
    }
}

fn source_static_state(adapter_id: &str, status: &str) -> SourceConnectorState {
    SourceConnectorState {
        adapter_id: adapter_id.to_string(),
        surface_id: format!("source-{adapter_id}"),
        status: status.to_string(),
        capabilities: vec![
            "source.schema_discovery".to_string(),
            "source.snapshot".to_string(),
            "source.incremental".to_string(),
        ],
        last_run_at_ms: None,
        last_error: None,
        degraded_reason: (status != "ready").then(|| status.to_string()),
        watermarks: Vec::new(),
    }
}

fn run_local_source_incremental(
    adapter_id: &str,
    request: ConnectorSourceRunRequest,
) -> SourceIncrementalRunResult {
    let read_plan = SourceReadPlan {
        adapter_id: adapter_id.to_string(),
        resource_ref: request.resource_ref.unwrap_or_default(),
        table: request.table.clone(),
        fields: Vec::new(),
        limit: request.limit,
        offset: request
            .watermark
            .as_ref()
            .and_then(|watermark| watermark.offset),
        cursor: request.watermark.as_ref().and_then(|watermark| {
            watermark
                .cursor
                .clone()
                .or_else(|| watermark.high_watermark.clone())
        }),
        metadata: request.metadata,
    };
    match connector::read_local_source_batch(&read_plan) {
        Ok(batch) => {
            let after = SourceWatermark {
                adapter_id: adapter_id.to_string(),
                resource_ref: batch.resource_ref.clone(),
                table: batch.table.clone(),
                strategy: "offset".to_string(),
                cursor: batch.cursor.next_offset.map(|offset| offset.to_string()),
                offset: Some(
                    batch
                        .cursor
                        .next_offset
                        .unwrap_or(batch.cursor.offset.saturating_add(batch.rows.len())),
                ),
                high_watermark: None,
                checksum: Some(batch.checksum.clone()),
                revision: request
                    .watermark
                    .as_ref()
                    .map_or(0, |watermark| watermark.revision),
                updated_at_ms: chrono::Utc::now().timestamp_millis(),
            };
            SourceIncrementalRunResult {
                status: "ok".to_string(),
                chunk_index: 0,
                final_chunk: true,
                batch: Some(batch),
                watermark_before: request.watermark,
                watermark_after: Some(after),
                degraded_reason: Some("degraded_incremental_offset_only".to_string()),
                receipt: None,
            }
        }
        Err(error) => SourceIncrementalRunResult {
            status: "degraded_local_read_failed".to_string(),
            chunk_index: 0,
            final_chunk: true,
            batch: None,
            watermark_before: request.watermark,
            watermark_after: None,
            degraded_reason: Some(error.to_string()),
            receipt: None,
        },
    }
}

const MAX_CONNECTOR_RESOURCE_PAGE: usize = 200;
const DEFAULT_CONNECTOR_RESOURCE_PAGE: usize = 100;

pub(super) fn connector_snapshot(state: &AppState) -> ConnectorRegistrySnapshot {
    let config = state.runtime_config_json_snapshot();
    let platforms = message_connector_routes::configured_platforms(config.as_ref());
    let mut accounts = platforms
        .iter()
        .filter(|platform| platform.enabled || platform.configured)
        .map(|platform| {
            let runtime = state
                .services
                .surface
                .runtime_snapshot(&message_connector_routes::platform_surface_id(platform));
            account_from_platform(platform, runtime.as_ref())
        })
        .collect::<Vec<_>>();
    let mcp_servers = configured_mcp_servers(config.as_ref());
    accounts.extend(mcp_servers.iter().map(account_from_mcp_server));
    let service_registry = builtin_service_connector_registry();
    accounts.extend(
        service_registry
            .connector_refs()
            .into_iter()
            .map(account_from_service_connector),
    );
    let mut capabilities = base_connector_capabilities().to_vec();
    for platform in platforms {
        for operation in platform.capabilities {
            let capability = manifest_from_platform_capability(&platform.platform_type, &operation);
            if !capabilities
                .iter()
                .any(|item| item.capability_id == capability.capability_id)
            {
                capabilities.push(capability);
            }
        }
    }
    for server in &mcp_servers {
        let capability = CapabilityManifest::mcp_server(&server.name);
        if !capabilities
            .iter()
            .any(|item| item.capability_id == capability.capability_id)
        {
            capabilities.push(capability);
        }
    }
    accounts.sort_by(|left, right| {
        left.provider
            .cmp(&right.provider)
            .then_with(|| left.account_id.cmp(&right.account_id))
    });
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    let (resources, resource_error) = list_durable_resources(state, 100, 0, None);
    let mut snapshot = ConnectorRegistrySnapshot::new(accounts, capabilities, resources);
    if let Some(error) = resource_error {
        snapshot.degraded = true;
        snapshot
            .degraded_reasons
            .push(format!("resource_directory:{error}"));
    }
    snapshot
}

fn base_connector_capabilities() -> &'static [CapabilityManifest] {
    static BASE_CONNECTOR_CAPABILITIES: OnceLock<Vec<CapabilityManifest>> = OnceLock::new();
    BASE_CONNECTOR_CAPABILITIES.get_or_init(|| {
        let mut capabilities = default_capabilities();
        capabilities.extend(builtin_service_connector_registry().capabilities());
        capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
        capabilities.dedup_by(|left, right| left.capability_id == right.capability_id);
        capabilities
    })
}

fn account_from_service_connector(connector: &dyn ServiceConnector) -> ProviderAccount {
    let metadata = connector.metadata();
    let mut account = ProviderAccount::new(
        metadata.provider.clone(),
        metadata.id.clone(),
        if metadata.read_only {
            "local_readonly"
        } else {
            "service"
        },
    );
    account.enabled_bindings = connector
        .capabilities()
        .into_iter()
        .map(|capability| capability.capability_id)
        .collect();
    account.health = ConnectorHealth::ready();
    account
}

fn account_from_platform(
    platform: &message_connector_routes::PlatformReadiness,
    runtime: Option<&surface::SurfaceRuntimeSnapshot>,
) -> ProviderAccount {
    let mut account = ProviderAccount::new(
        platform.platform_type.clone(),
        platform.name.clone(),
        auth_mode_for_platform(&platform.platform_type),
    );
    account.secret_refs = vec![format!("config://gateway/platforms/{}", platform.name)];
    account.scopes = platform.scopes.clone();
    account.enabled_bindings = platform
        .capabilities
        .iter()
        .map(|operation| {
            manifest_from_platform_capability(&platform.platform_type, operation).capability_id
        })
        .collect();
    account.health = match platform.status {
        "disabled" => ConnectorHealth::disabled("platform is disabled"),
        "degraded" => ConnectorHealth::degraded(format!(
            "missing required fields: {}",
            platform.missing_required.join(", ")
        )),
        "configured" => match runtime.map(|runtime| runtime.status) {
            Some(surface::SurfaceRuntimeStatus::Ready) => ConnectorHealth::ready(),
            Some(status) => ConnectorHealth::degraded(format!(
                "managed edge runtime is {}",
                runtime_status_name(status)
            )),
            None => ConnectorHealth::degraded("managed edge runtime is unavailable"),
        },
        other => ConnectorHealth::degraded(format!("platform status is {other}")),
    };
    account
}

fn runtime_status_name(status: surface::SurfaceRuntimeStatus) -> &'static str {
    use surface::SurfaceRuntimeStatus;

    match status {
        SurfaceRuntimeStatus::Builtin => "builtin",
        SurfaceRuntimeStatus::Discovered => "discovered",
        SurfaceRuntimeStatus::Starting => "starting",
        SurfaceRuntimeStatus::Ready => "ready",
        SurfaceRuntimeStatus::Degraded => "degraded",
        SurfaceRuntimeStatus::Restarting => "restarting",
        SurfaceRuntimeStatus::Unavailable => "unavailable",
        SurfaceRuntimeStatus::Disabled => "disabled",
        SurfaceRuntimeStatus::Failed => "failed",
        SurfaceRuntimeStatus::CircuitOpen => "circuit-open",
    }
}

#[cfg(test)]
mod platform_account_tests {
    use super::*;
    use connector::ConnectorHealthStatus;
    use surface::{SurfaceLifecycle, SurfaceRuntimeSnapshot, SurfaceRuntimeStatus};

    fn configured_lark() -> message_connector_routes::PlatformReadiness {
        message_connector_routes::PlatformReadiness {
            name: "lark".to_string(),
            platform_type: "lark".to_string(),
            enabled: true,
            status: "configured",
            configured: true,
            credential_present: true,
            missing_required: Vec::new(),
            scopes: Vec::new(),
            capabilities: vec!["message.send.text".to_string()],
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn configured_lark_account_uses_canonical_feishu_runtime_health() {
        let platform = configured_lark();
        assert_eq!(
            message_connector_routes::platform_surface_id(&platform),
            "feishu"
        );

        let discovered = SurfaceRuntimeSnapshot::discovered("feishu", SurfaceLifecycle::Managed);
        let account = account_from_platform(&platform, Some(&discovered));
        assert_eq!(account.health.status, ConnectorHealthStatus::Degraded);
        assert_eq!(
            account.health.reason.as_deref(),
            Some("managed edge runtime is discovered")
        );

        let mut ready = discovered;
        ready.status = SurfaceRuntimeStatus::Ready;
        ready.active = true;
        let account = account_from_platform(&platform, Some(&ready));
        assert_eq!(account.health.status, ConnectorHealthStatus::Ready);
        assert!(account.health.reason.is_none());
    }
}

fn manifest_from_platform_capability(platform_type: &str, operation: &str) -> CapabilityManifest {
    CapabilityManifest::channel(
        platform_type,
        normalize_platform_capability_operation(operation),
    )
}

fn normalize_platform_capability_operation(operation: &str) -> String {
    match operation.trim() {
        "message.ingress" => "ingress".to_string(),
        "message.send.text" => "send_text".to_string(),
        "message.send.image" => "send_image".to_string(),
        "message.send.voice" => "send_voice".to_string(),
        "message.send.document" => "send_file".to_string(),
        "message.send.video" => "send_video".to_string(),
        "message.send.card" => "send_card".to_string(),
        "message.edit" => "edit".to_string(),
        "message.delete" => "delete".to_string(),
        "message.chat.info" => "chat_info".to_string(),
        "message.callback" => "callback".to_string(),
        other => other.replace('.', "_"),
    }
}

fn auth_mode_for_platform(platform_type: &str) -> &'static str {
    match platform_type {
        "feishu" | "wecom" => "app_secret",
        "wechat-ilink" | "wechat_ilink" | "wechat" => "qr_session",
        "email" => "smtp_imap",
        _ => "config",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_event_normalization_preserves_transport_facts_only() {
        let event = serde_json::json!({
            "id": "row-change-42",
            "type": "record.updated",
            "subject": "order-17",
            "sequence": 8,
            "priority": "high",
            "deleted": false,
            "attempt": 3,
        });
        let capabilities = source_event_capabilities("feishu-bitable");

        let normalized = normalize_source_event(
            "feishu-bitable",
            Some("bitable://app/table"),
            &event,
            0,
            &capabilities,
        );

        assert_eq!(normalized.event_id, "row-change-42");
        assert_eq!(
            normalized.idempotency_key,
            "connector-source:feishu-bitable:row-change-42"
        );
        assert_eq!(normalized.source_id, "feishu-bitable");
        assert_eq!(normalized.source_kind, "connector_source");
        assert_eq!(normalized.event_type, "record.updated");
        assert_eq!(normalized.subject, "order-17");
        assert_eq!(normalized.source_sequence, Some(8));
        assert_eq!(
            normalized.attributes.get("priority"),
            Some(&"high".to_string())
        );
        assert_eq!(
            normalized.attributes.get("deleted"),
            Some(&"false".to_string())
        );
        assert_eq!(normalized.attributes.get("attempt"), Some(&"3".to_string()));
        assert_eq!(
            normalized.attributes.get("resource_ref"),
            Some(&"bitable://app/table".to_string())
        );
        assert!(normalized
            .source_capabilities
            .contains(&"connector.source.event.receive".to_string()));
        assert!(normalized.payload_digest.starts_with("sha256:"));
    }
}
