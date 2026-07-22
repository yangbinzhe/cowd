use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use surface::{
    normalize_surface_id, SurfaceFailureKind, SurfaceFrame, SurfaceLifecycle, SurfaceRuntimeError,
    SurfaceRuntimeSnapshot, SurfaceRuntimeStatus, SurfaceSupervisorEvent,
};
use tokio::sync::Mutex as AsyncMutex;

use super::{managed_actions, SurfaceHost};
use super::{
    SurfaceDeliveryEvent, SurfaceInboxReceipt, SurfaceInboxRecord, SurfaceIngressClaim,
    SurfaceMessageSnapshot, SurfaceOutboxRecord, SurfaceTriggerEventReceipt,
    SurfaceTriggerEventRecord, SurfaceTurnCorrelation,
};
use harness_contract::managed_agent::ManagedAgentTriggerEvent;

impl SurfaceHost {
    pub(crate) async fn events(&self, surface: &str) -> Vec<SurfaceFrame> {
        let surface = normalize_surface_id(surface);
        let process = self.managed.lock().await.get(&surface).cloned();
        let Some(process) = process else {
            return Vec::new();
        };
        let events = process.events.lock().await;
        events.iter().cloned().collect()
    }

    pub(crate) async fn supervisor_events(&self, surface: &str) -> Vec<SurfaceSupervisorEvent> {
        let surface = normalize_surface_id(surface);
        let ledger = self.ledger.lock().await;
        ledger
            .get(&surface)
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default()
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
        self.messages.record_inbox_received(
            surface,
            message_id,
            payload,
            runtime_session_id,
            thread_id,
            sender_id,
        )
    }

    pub(crate) fn mark_inbox_processing(&self, idempotency_key: &str) -> Result<(), String> {
        self.messages.mark_inbox_processing(idempotency_key)
    }

    pub(crate) fn mark_inbox_processed(
        &self,
        idempotency_key: &str,
        runtime_turn_id: Option<String>,
    ) -> Result<(), String> {
        self.messages
            .mark_inbox_processed(idempotency_key, runtime_turn_id)
    }

    pub(crate) fn mark_inbox_admitted(
        &self,
        idempotency_key: &str,
        correlation: SurfaceTurnCorrelation,
    ) -> Result<(), String> {
        self.messages
            .mark_inbox_admitted(idempotency_key, correlation)
    }

    pub(crate) fn record_inbox_terminal_delivery(
        &self,
        idempotency_key: &str,
        terminal_id: &str,
    ) -> Result<(), String> {
        self.messages
            .record_inbox_terminal_delivery(idempotency_key, terminal_id)
    }

    pub(crate) fn mark_inbox_replied(&self, idempotency_key: &str) -> Result<(), String> {
        self.messages.mark_inbox_replied(idempotency_key)
    }

    pub(crate) fn mark_inbox_reply_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.messages
            .mark_inbox_reply_failed(idempotency_key, &error.into())
    }

    pub(crate) fn mark_inbox_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.messages
            .mark_inbox_failed(idempotency_key, &error.into())
    }

    pub(crate) fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &ManagedAgentTriggerEvent,
        payload: &serde_json::Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        self.messages
            .record_trigger_event_received(surface, event_type, trigger, payload)
    }

    pub(crate) fn mark_trigger_event_dispatching(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        self.messages
            .mark_trigger_event_dispatching(idempotency_key)
    }

    pub(crate) fn mark_trigger_event_accepted(
        &self,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.messages.mark_trigger_event_accepted(idempotency_key)
    }

    pub(crate) fn mark_trigger_event_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.messages
            .mark_trigger_event_failed(idempotency_key, &error.into())
    }

    pub(crate) fn retry_trigger_event(
        &self,
        surface: &str,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.messages.retry_trigger_event(surface, idempotency_key)
    }

    pub(crate) fn due_trigger_event_retries(
        &self,
    ) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        self.messages.due_trigger_event_retries()
    }

    pub(crate) fn claim_ingress_frames(
        &self,
        claim_owner: &str,
        limit: usize,
        lease_ms: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String> {
        self.messages
            .claim_ingress_frames(claim_owner, limit, lease_ms)
    }

    pub(crate) fn complete_ingress_frame(&self, record_key: &str) -> Result<(), String> {
        self.messages.complete_ingress_frame(record_key)
    }

    pub(crate) fn fail_ingress_frame(&self, record_key: &str, error: &str) -> Result<(), String> {
        self.messages.fail_ingress_frame(record_key, error)
    }

    pub(crate) fn inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String> {
        self.messages.list_inbox(surface)
    }

    pub(crate) fn outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.messages.list_outbox(surface)
    }

    pub(crate) fn trigger_events(
        &self,
        surface: &str,
    ) -> Result<Vec<SurfaceTriggerEventRecord>, String> {
        self.messages.list_trigger_events(surface)
    }

    pub(crate) fn all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String> {
        self.messages.list_all_inbox()
    }

    pub(crate) fn all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.messages.list_all_outbox()
    }

    pub(crate) fn delivery_events(
        &self,
        surface: &str,
    ) -> Result<Vec<SurfaceDeliveryEvent>, String> {
        self.messages.list_delivery_events(surface)
    }

    pub(crate) fn message_snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String> {
        self.messages.snapshot(surface)
    }

    pub(crate) fn archive_dead_letters(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        self.messages
            .archive_dead_letters(surface, older_than_ms, limit)
    }

    pub(crate) fn purge_archived_events(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        self.messages
            .purge_archived_events(surface, older_than_ms, limit)
    }

    pub(crate) fn replay_inbox_message(
        &self,
        surface: &str,
        message_id: &str,
    ) -> Result<SurfaceInboxRecord, String> {
        let surface = normalize_surface_id(surface);
        let record = self
            .messages
            .get_inbox_message(&surface, message_id)?
            .ok_or_else(|| format!("surface inbox `{surface}/{message_id}` not found"))?;
        let replay_message_id = format!("{}:replay:{}", record.message_id, uuid::Uuid::new_v4());
        let mut payload = record.payload_json.clone();
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "message_id".to_string(),
                serde_json::Value::String(replay_message_id),
            );
            let metadata = object
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(metadata) = metadata.as_object_mut() {
                metadata.insert(
                    "replayed_from_message_id".to_string(),
                    serde_json::Value::String(record.message_id.clone()),
                );
                metadata.insert(
                    "replay_source".to_string(),
                    serde_json::Value::String("gateway.surface_inbox".to_string()),
                );
            }
        }
        self.event_tx
            .send(SurfaceFrame::Event {
                surface,
                event: "message.received".to_string(),
                payload,
            })
            .map_err(|error| error.to_string())?;
        Ok(record)
    }

    pub(super) async fn set_runtime(&self, snapshot: SurfaceRuntimeSnapshot) {
        self.runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(snapshot.surface.clone(), snapshot);
    }

    pub(super) async fn push_ledger(&self, event: SurfaceSupervisorEvent) {
        push_supervisor_event(&self.ledger, event).await;
    }

    pub(super) async fn mark_runtime_error(
        &self,
        surface: &str,
        status: SurfaceRuntimeStatus,
        kind: SurfaceFailureKind,
        message: impl Into<String>,
    ) -> SurfaceRuntimeSnapshot {
        let surface = normalize_surface_id(surface);
        let error = SurfaceRuntimeError::new(kind, message);
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = status;
        snapshot.active = false;
        snapshot.last_error = Some(error.clone());
        snapshot.available_actions = managed_actions(snapshot.circuit_open);
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::error(&surface, status, error))
            .await;
        snapshot
    }
}

pub(super) async fn push_supervisor_event(
    ledger: &Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    event: SurfaceSupervisorEvent,
) {
    let mut ledger = ledger.lock().await;
    let events = ledger.entry(event.surface.clone()).or_default();
    events.push_back(event);
    while events.len() > 500 {
        events.pop_front();
    }
}
