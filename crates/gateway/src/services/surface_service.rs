use std::sync::Arc;

use connector::{
    SourceConnectorState, SourceEventBatch, SourceIncrementalRunRequest,
    SourceIncrementalRunResult, SourceReadPlan, SourceRecordBatch,
};
use surface::{
    SurfaceActionRequest, SurfaceFrame, SurfaceOperationResult, SurfaceRegistrySnapshot,
    SurfaceRuntimeSnapshot, SurfaceSendRequest, SurfaceSupervisorEvent,
};
use tokio::sync::{broadcast, mpsc};

use crate::surface_host::{
    SurfaceDeliveryEvent, SurfaceDiscoveryReport, SurfaceHost, SurfaceHostHealth,
    SurfaceInboxReceipt, SurfaceInboxRecord, SurfaceIngressClaim, SurfaceMessageSnapshot,
    SurfaceOutboxRecord, SurfaceResourceSummary, SurfaceRouteSummary, SurfaceStaticFile,
    SurfaceTriggerEventReceipt, SurfaceTriggerEventRecord,
};
use harness_contract::managed_agent::ManagedAgentTriggerEvent;

use super::{service_envelope, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct SurfaceService {
    label: &'static str,
    owner: &'static str,
    host: Arc<SurfaceHost>,
}

impl SurfaceService {
    pub(crate) fn new() -> Self {
        Self {
            label: "surface",
            owner: "0.9.380 Surface service boundary",
            host: Arc::new(SurfaceHost::default()),
        }
    }

    pub(crate) fn with_host(host: Arc<SurfaceHost>) -> Self {
        Self {
            host,
            ..Self::new()
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        self.label
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn is_runtime_available(&self) -> bool {
        true
    }

    pub(crate) fn snapshot(&self) -> SurfaceRegistrySnapshot {
        self.host.snapshot()
    }

    pub(crate) fn health(&self) -> SurfaceHostHealth {
        self.host.health()
    }

    pub(crate) fn runtime_snapshots(&self) -> Vec<SurfaceRuntimeSnapshot> {
        self.host.runtime_snapshots()
    }

    pub(crate) fn runtime_snapshot(&self, id: &str) -> Option<SurfaceRuntimeSnapshot> {
        self.host.runtime_snapshot(id)
    }

    pub(crate) fn set_configs(
        &self,
        configs: std::collections::BTreeMap<String, serde_json::Value>,
    ) {
        self.host.set_configs(configs);
    }

    pub(crate) fn set_webui_static_resource(&self, dir: Option<&std::path::Path>) {
        self.host.set_webui_static_resource(dir);
    }

    pub(crate) async fn reload_manifests(&self) -> SurfaceDiscoveryReport {
        self.host.reload_manifests().await
    }

    pub(crate) fn has_surface(&self, id: &str) -> bool {
        self.host.get(id).is_some()
    }

    pub(crate) fn routes(&self, id: &str) -> Option<SurfaceRouteSummary> {
        self.host.routes(id)
    }

    pub(crate) fn resources(&self, id: &str) -> Option<SurfaceResourceSummary> {
        self.host.resources(id)
    }

    pub(crate) fn resolve_static(
        &self,
        id: &str,
        requested_path: &str,
    ) -> Result<Option<SurfaceStaticFile>, String> {
        self.host
            .resolve_static(id, requested_path)
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn send(
        &self,
        request: SurfaceSendRequest,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .send(request)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) fn record_inbox_received(
        &self,
        surface: &str,
        message_id: &str,
        payload: &serde_json::Value,
        runtime_session_id: &str,
        thread_id: Option<String>,
        sender_id: Option<String>,
    ) -> Result<SurfaceInboxReceipt, String> {
        self.host.record_inbox_received(
            surface,
            message_id,
            payload,
            runtime_session_id,
            thread_id,
            sender_id,
        )
    }

    pub(crate) fn mark_inbox_processing(&self, idempotency_key: &str) -> Result<(), String> {
        self.host.mark_inbox_processing(idempotency_key)
    }

    pub(crate) fn mark_inbox_processed(
        &self,
        idempotency_key: &str,
        runtime_turn_id: Option<String>,
    ) -> Result<(), String> {
        self.host
            .mark_inbox_processed(idempotency_key, runtime_turn_id)
    }

    pub(crate) fn mark_inbox_admitted(
        &self,
        idempotency_key: &str,
        correlation: crate::surface_host::SurfaceTurnCorrelation,
    ) -> Result<(), String> {
        self.host.mark_inbox_admitted(idempotency_key, correlation)
    }

    pub(crate) fn record_inbox_terminal_delivery(
        &self,
        idempotency_key: &str,
        terminal_id: &str,
    ) -> Result<(), String> {
        self.host
            .record_inbox_terminal_delivery(idempotency_key, terminal_id)
    }

    pub(crate) fn mark_inbox_replied(&self, idempotency_key: &str) -> Result<(), String> {
        self.host.mark_inbox_replied(idempotency_key)
    }

    pub(crate) fn mark_inbox_reply_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.host.mark_inbox_reply_failed(idempotency_key, error)
    }

    pub(crate) fn mark_inbox_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.host.mark_inbox_failed(idempotency_key, error)
    }

    pub(crate) fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &ManagedAgentTriggerEvent,
        payload: &serde_json::Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        self.host
            .record_trigger_event_received(surface, event_type, trigger, payload)
    }

    pub(crate) fn mark_trigger_event_dispatching(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        self.host.mark_trigger_event_dispatching(idempotency_key)
    }

    pub(crate) fn mark_trigger_event_accepted(
        &self,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.host.mark_trigger_event_accepted(idempotency_key)
    }

    pub(crate) fn mark_trigger_event_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.host.mark_trigger_event_failed(idempotency_key, error)
    }

    pub(crate) fn retry_trigger_event(
        &self,
        surface: &str,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.host.retry_trigger_event(surface, idempotency_key)
    }

    pub(crate) fn due_trigger_event_retries(
        &self,
    ) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        self.host.due_trigger_event_retries()
    }

    pub(crate) fn claim_ingress_frames(
        &self,
        claim_owner: &str,
        limit: usize,
        lease_ms: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String> {
        self.host.claim_ingress_frames(claim_owner, limit, lease_ms)
    }

    pub(crate) fn complete_ingress_frame(&self, record_key: &str) -> Result<(), String> {
        self.host.complete_ingress_frame(record_key)
    }

    pub(crate) fn fail_ingress_frame(&self, record_key: &str, error: &str) -> Result<(), String> {
        self.host.fail_ingress_frame(record_key, error)
    }

    pub(crate) fn inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String> {
        self.host.inbox(surface)
    }

    pub(crate) fn outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.host.outbox(surface)
    }

    pub(crate) fn trigger_events(
        &self,
        surface: &str,
    ) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        self.host.trigger_events(surface)
    }

    pub(crate) fn all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String> {
        self.host.all_inbox()
    }

    pub(crate) fn all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.host.all_outbox()
    }

    pub(crate) fn delivery_events(
        &self,
        surface: &str,
    ) -> Result<Vec<SurfaceDeliveryEvent>, String> {
        self.host.delivery_events(surface)
    }

    pub(crate) fn message_snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String> {
        self.host.message_snapshot(surface)
    }

    pub(crate) fn message_store_root(&self) -> std::path::PathBuf {
        self.host.message_store_root()
    }

    pub(crate) fn archive_dead_letters(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.host
            .archive_dead_letters(surface, older_than_ms, limit)
    }

    pub(crate) fn purge_archived_events(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        self.host
            .purge_archived_events(surface, older_than_ms, limit)
    }

    pub(crate) fn replay_inbox_message(
        &self,
        surface: &str,
        message_id: &str,
    ) -> Result<SurfaceInboxRecord, String> {
        self.host.replay_inbox_message(surface, message_id)
    }

    pub(crate) async fn retry_outbox_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .retry_outbox_delivery(delivery_id)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) fn dead_letter_outbox_delivery(
        &self,
        delivery_id: &str,
        reason: impl Into<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        self.host.dead_letter_outbox_delivery(delivery_id, reason)
    }

    pub(crate) async fn action(
        &self,
        request: SurfaceActionRequest,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .action(request)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn read_source_batch(
        &self,
        read_plan: &SourceReadPlan,
    ) -> Result<SourceRecordBatch, String> {
        let surface = source_connector_surface_id(&read_plan.adapter_id);
        let result = self
            .action(SurfaceActionRequest {
                surface: surface.clone(),
                action: "source.read_batch".to_string(),
                payload: serde_json::to_value(read_plan)
                    .map_err(|error| format!("source read plan encode failed: {error}"))?,
            })
            .await?;
        if let Some(error) = result.error {
            return Err(format!(
                "source connector `{surface}` failed: {}",
                error.message
            ));
        }
        let payload = result
            .payload
            .ok_or_else(|| format!("source connector `{surface}` returned no payload"))?;
        let batch_value = payload
            .get("source_batch")
            .cloned()
            .unwrap_or_else(|| payload.clone());
        let batch = serde_json::from_value::<SourceRecordBatch>(batch_value).map_err(|error| {
            format!("source connector `{surface}` returned invalid batch: {error}")
        })?;
        if batch.adapter_id != read_plan.adapter_id {
            return Err(format!(
                "source connector `{surface}` returned adapter `{}` for requested `{}`",
                batch.adapter_id, read_plan.adapter_id
            ));
        }
        Ok(batch)
    }

    pub(crate) async fn source_state(
        &self,
        adapter_id: &str,
    ) -> Result<SourceConnectorState, String> {
        let surface = source_connector_surface_id(adapter_id);
        let result = self
            .action(SurfaceActionRequest {
                surface: surface.clone(),
                action: "source.state".to_string(),
                payload: serde_json::json!({ "adapter_id": adapter_id }),
            })
            .await?;
        let payload = self.source_action_payload(&surface, result)?;
        serde_json::from_value::<SourceConnectorState>(
            payload
                .get("state")
                .cloned()
                .unwrap_or_else(|| payload.clone()),
        )
        .map_err(|error| format!("source connector `{surface}` returned invalid state: {error}"))
    }

    pub(crate) async fn source_incremental_stream(
        &self,
        request: &SourceIncrementalRunRequest,
    ) -> Result<mpsc::Receiver<Result<SourceIncrementalRunResult, String>>, String> {
        let surface = source_connector_surface_id(&request.adapter_id);
        let mut operations = self
            .host
            .action_stream(SurfaceActionRequest {
                surface: surface.clone(),
                action: "source.incremental.run".to_string(),
                payload: serde_json::json!({ "request": request }),
            })
            .await
            .map_err(|error| error.to_string())?;
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(operation) = operations.recv().await {
                let decoded = operation
                    .map_err(|error| error.to_string())
                    .and_then(|result| {
                        if let Some(error) = result.error {
                            return Err(format!(
                                "source connector `{surface}` failed: {}",
                                error.message
                            ));
                        }
                        let payload = result.payload.ok_or_else(|| {
                            format!("source connector `{surface}` returned no payload")
                        })?;
                        serde_json::from_value::<SourceIncrementalRunResult>(payload).map_err(
                            |error| {
                                format!(
                                    "source connector `{surface}` returned invalid incremental chunk: {error}"
                                )
                            },
                        )
                    });
                if tx.send(decoded).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }

    pub(crate) async fn source_event_poll(
        &self,
        adapter_id: &str,
        payload: serde_json::Value,
    ) -> Result<SourceEventBatch, String> {
        let surface = source_connector_surface_id(adapter_id);
        let result = self
            .action(SurfaceActionRequest {
                surface: surface.clone(),
                action: "source.event.poll".to_string(),
                payload,
            })
            .await?;
        let payload = self.source_action_payload(&surface, result)?;
        serde_json::from_value::<SourceEventBatch>(payload).map_err(|error| {
            format!("source connector `{surface}` returned invalid event batch: {error}")
        })
    }

    fn source_action_payload(
        &self,
        surface: &str,
        result: SurfaceOperationResult,
    ) -> Result<serde_json::Value, String> {
        if let Some(error) = result.error {
            return Err(format!(
                "source connector `{surface}` failed: {}",
                error.message
            ));
        }
        result
            .payload
            .ok_or_else(|| format!("source connector `{surface}` returned no payload"))
    }

    pub(crate) async fn callback(
        &self,
        surface: &str,
        path: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .callback(surface, path, method, payload)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn check_surface_health(
        &self,
        surface: &str,
    ) -> Result<SurfaceOperationResult, String> {
        self.host
            .check_surface_health(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn events(&self, surface: &str) -> Vec<surface::SurfaceFrame> {
        self.host.events(surface).await
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<SurfaceFrame> {
        self.host.subscribe_events()
    }

    pub(crate) async fn supervisor_events(&self, surface: &str) -> Vec<SurfaceSupervisorEvent> {
        self.host.supervisor_events(surface).await
    }

    pub(crate) async fn start_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .start_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn stop_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .stop_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn restart_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .restart_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn repair_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, String> {
        self.host
            .repair_surface(surface)
            .await
            .map_err(|error| error.to_string())
    }
}

fn source_connector_surface_id(adapter_id: &str) -> String {
    match adapter_id {
        "feishu_bitable" => "feishu-bitable".to_string(),
        "lark_bitable" => "lark-bitable".to_string(),
        other => surface::normalize_surface_id(other),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use connector::SourceReadPlan;

    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    #[tokio::test]
    async fn read_source_batch_invokes_edge_source_connector() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-source-service-test-{}",
            uuid::Uuid::new_v4()
        ));
        let connector_dir = root.join("feishu-bitable");
        fs::create_dir_all(&connector_dir).unwrap();
        let sidecar = connector_dir.join("cowd-edge-fixture-source");
        fs::write(
            &sidecar,
            r#"#!/usr/bin/env sh
read _line
printf '%s\n' '{"type":"ok","id":"reply","payload":{"status":"ok","source_batch":{"adapter_id":"feishu_bitable","resource_ref":"feishu-bitable://app/table","table":"orders","schema":{"table_name":"orders","fields":[{"name":"sku","data_type":"string","nullable":false}],"primary_key":[]},"rows":[{"sku":"A1"}],"cursor":{"offset":0,"limit":100,"next_offset":null},"row_count":1,"checksum":"fixture","truncated":false}}}'
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&sidecar, permissions).unwrap();
        fs::write(
            connector_dir.join(surface::SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "feishu-bitable",
                "name": "Feishu Bitable Source Connector",
                "version": "1.0.0",
                "kind": "source-connector",
                "runtime": {
                    "kind": "one-shot",
                    "entry": "cowd-edge-fixture-source",
                    "transport": "stdio-jsonl"
                },
                "capabilities": ["source.snapshot"],
                "default_enabled": true
            }"#,
        )
        .unwrap();

        let host = Arc::new(crate::surface_host::SurfaceHost::new(vec![root.clone()]));
        assert_eq!(host.discover().discovered, 1);
        let service = SurfaceService::with_host(host);
        let batch = service
            .read_source_batch(&SourceReadPlan {
                adapter_id: "feishu_bitable".to_string(),
                resource_ref: "feishu-bitable://app/table".to_string(),
                table: Some("orders".to_string()),
                fields: Vec::new(),
                limit: None,
                offset: None,
                cursor: None,
                metadata: serde_json::Value::Null,
            })
            .await
            .unwrap();

        assert_eq!(batch.adapter_id, "feishu_bitable");
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.schema.table_name, "orders");

        let _ = fs::remove_dir_all(root);
    }
}
