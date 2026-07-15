use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{Duration as ChronoDuration, Utc};
use harness_contract::managed_agent::ManagedAgentTriggerEvent;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surface::{normalize_surface_id, SurfaceOperationResult, SurfaceSendRequest};

const INBOX_FILE: &str = "surface_inbox.jsonl";
const OUTBOX_FILE: &str = "surface_outbox.jsonl";
const EVENT_FILE: &str = "surface_delivery_event.jsonl";
const TRIGGER_EVENT_FILE: &str = "surface_trigger_event.jsonl";
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SurfaceTurnCorrelation {
    /// Stable transport identity retained with the original inbox record.
    pub surface: String,
    pub message_id: String,
    pub inbox_idempotency_key: String,
    /// Canonical SessionIngress and Runtime identities.  These are never
    /// inferred from message text or timestamps during recovery.
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub reply_to_message_id: String,
    pub reply_idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    /// Monotonic terminal-delivery observation revision.  It protects
    /// restart/replay reconciliation while the unchanged outbox key provides
    /// the side-effect idempotency boundary.
    #[serde(default)]
    pub terminal_delivery_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SurfaceInboxRecord {
    pub id: String,
    pub surface: String,
    pub message_id: String,
    pub idempotency_key: String,
    pub thread_id: Option<String>,
    pub sender_id: Option<String>,
    pub payload_hash: String,
    pub payload_summary: String,
    pub payload_json: serde_json::Value,
    pub status: String,
    pub received_at_ms: i64,
    pub updated_at_ms: i64,
    pub runtime_session_id: Option<String>,
    pub runtime_turn_id: Option<String>,
    /// Additive migration carrier for canonical ingress/terminal recovery.
    /// Older JSONL records deserialize without it and remain readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation: Option<SurfaceTurnCorrelation>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SurfaceOutboxRecord {
    pub delivery_id: String,
    pub surface: String,
    pub recipient: String,
    pub thread_id: Option<String>,
    pub idempotency_key: String,
    pub text_hash: String,
    pub text_summary: String,
    pub request_json: serde_json::Value,
    pub status: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_retry_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub sent_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub source_session_id: Option<String>,
    pub reply_to_message_id: Option<String>,
}

/// Durable handoff from a Surface transport into Runtime's Managed Agent
/// trigger boundary.  The Surface layer owns only delivery/recovery of this
/// transport fact; Runtime remains the owner of matching, authorization,
/// idempotency and invocation scheduling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SurfaceTriggerEventRecord {
    pub idempotency_key: String,
    pub surface: String,
    pub event_type: String,
    pub trigger: ManagedAgentTriggerEvent,
    pub payload_json: serde_json::Value,
    pub status: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_retry_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub accepted_at_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SurfaceDeliveryEvent {
    pub event_id: String,
    pub surface: String,
    pub delivery_id: Option<String>,
    pub message_id: Option<String>,
    pub kind: String,
    pub status: String,
    pub detail_json: serde_json::Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SurfaceMessageSnapshot {
    pub kind: &'static str,
    pub surface: String,
    pub message_root: PathBuf,
    pub inbox: Vec<SurfaceInboxRecord>,
    pub active_inbox: Vec<SurfaceInboxRecord>,
    pub terminal_inbox: Vec<SurfaceInboxRecord>,
    pub trigger_events: Vec<SurfaceTriggerEventRecord>,
    pub active_trigger_events: Vec<SurfaceTriggerEventRecord>,
    pub failed_trigger_events: Vec<SurfaceTriggerEventRecord>,
    pub outbox: Vec<SurfaceOutboxRecord>,
    pub active_outbox: Vec<SurfaceOutboxRecord>,
    pub terminal_outbox: Vec<SurfaceOutboxRecord>,
    pub deliveries: Vec<SurfaceDeliveryEvent>,
    pub dead_letters: Vec<SurfaceOutboxRecord>,
    pub archived_outbox: Vec<SurfaceOutboxRecord>,
    pub archived_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct SurfaceInboxReceipt {
    pub record: SurfaceInboxRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SurfaceTriggerEventReceipt {
    pub record: SurfaceTriggerEventRecord,
    pub duplicate: bool,
}

#[derive(Debug, Default)]
struct SurfaceMessageState {
    inbox: BTreeMap<String, SurfaceInboxRecord>,
    outbox: BTreeMap<String, SurfaceOutboxRecord>,
    trigger_events: BTreeMap<String, SurfaceTriggerEventRecord>,
    events: BTreeMap<String, SurfaceDeliveryEvent>,
}

#[derive(Debug, Clone)]
pub(crate) struct SurfaceMessageStore {
    root: PathBuf,
    state: Arc<Mutex<SurfaceMessageState>>,
}

impl SurfaceMessageStore {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let state = load_state(&root).unwrap_or_default();
        Self {
            root,
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub(crate) fn default_root(config_home: &Path) -> PathBuf {
        std::env::var_os("COWD_SURFACE_MESSAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| config_home.join("surface-messages"))
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
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
        let surface = normalize_surface_id(surface);
        let idempotency_key = inbound_idempotency_key(&surface, message_id);
        let mut state = self.lock_state()?;
        if let Some(existing) = state.inbox.get(&idempotency_key) {
            return Ok(SurfaceInboxReceipt {
                record: existing.clone(),
                duplicate: true,
            });
        }
        let now = now_ms();
        let record = SurfaceInboxRecord {
            id: idempotency_key.clone(),
            surface: surface.clone(),
            message_id: message_id.to_string(),
            idempotency_key: idempotency_key.clone(),
            thread_id,
            sender_id,
            payload_hash: hash_json(payload),
            payload_summary: summarize_json(payload, 240),
            payload_json: payload.clone(),
            status: "received".to_string(),
            received_at_ms: now,
            updated_at_ms: now,
            runtime_session_id: Some(runtime_session_id.to_string()),
            runtime_turn_id: None,
            correlation: None,
            last_error: None,
        };
        state.inbox.insert(idempotency_key, record.clone());
        self.append_record(INBOX_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface,
            delivery_id: None,
            message_id: Some(message_id.to_string()),
            kind: "inbox.received".to_string(),
            status: "received".to_string(),
            detail_json: serde_json::json!({
                "runtime_session_id": runtime_session_id,
                "payload_summary": record.payload_summary,
            }),
            created_at_ms: now,
        })?;
        Ok(SurfaceInboxReceipt {
            record,
            duplicate: false,
        })
    }

    pub(crate) fn mark_inbox_processing(&self, idempotency_key: &str) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "processing", None, None)
    }

    pub(crate) fn mark_inbox_processed(
        &self,
        idempotency_key: &str,
        runtime_turn_id: Option<String>,
    ) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "processed", runtime_turn_id, None)
    }

    pub(crate) fn mark_inbox_admitted(
        &self,
        idempotency_key: &str,
        correlation: SurfaceTurnCorrelation,
    ) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let record = state
            .inbox
            .get_mut(idempotency_key)
            .ok_or_else(|| format!("surface inbox `{idempotency_key}` not found"))?;
        record.status = "processed".to_string();
        record.updated_at_ms = now_ms();
        record.runtime_session_id = Some(correlation.session_id.clone());
        record.runtime_turn_id = Some(correlation.turn_id.clone());
        record.correlation = Some(correlation);
        record.last_error = None;
        let record = record.clone();
        self.append_record(INBOX_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: Some(record.message_id.clone()),
            kind: "inbox.processed".to_string(),
            status: "processed".to_string(),
            detail_json: serde_json::json!({
                "runtime_turn_id": record.runtime_turn_id,
                "execution_id": record.correlation.as_ref().map(|item| item.execution_id.clone()),
                "reply_idempotency_key": record.correlation.as_ref().map(|item| item.reply_idempotency_key.clone()),
            }),
            created_at_ms: now_ms(),
        })
    }

    pub(crate) fn record_inbox_terminal_delivery(
        &self,
        idempotency_key: &str,
        terminal_id: &str,
    ) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let record = state
            .inbox
            .get_mut(idempotency_key)
            .ok_or_else(|| format!("surface inbox `{idempotency_key}` not found"))?;
        let correlation = record
            .correlation
            .as_mut()
            .ok_or_else(|| format!("surface inbox `{idempotency_key}` has no turn correlation"))?;
        if correlation.terminal_id.as_deref() != Some(terminal_id) {
            correlation.terminal_id = Some(terminal_id.to_string());
            correlation.terminal_delivery_revision =
                correlation.terminal_delivery_revision.saturating_add(1);
        }
        record.updated_at_ms = now_ms();
        let record = record.clone();
        self.append_record(INBOX_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: Some(record.message_id.clone()),
            kind: "inbox.terminal_delivery_observed".to_string(),
            status: record.status.clone(),
            detail_json: serde_json::json!({
                "terminal_id": terminal_id,
                "terminal_delivery_revision": record.correlation.as_ref().map(|item| item.terminal_delivery_revision),
            }),
            created_at_ms: now_ms(),
        })
    }

    pub(crate) fn mark_inbox_replied(&self, idempotency_key: &str) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "replied", None, None)
    }

    pub(crate) fn mark_inbox_reply_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "reply_failed", None, Some(error.into()))
    }

    pub(crate) fn mark_inbox_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.update_inbox_status(idempotency_key, "failed", None, Some(error.into()))
    }

    pub(crate) fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &ManagedAgentTriggerEvent,
        payload: &serde_json::Value,
    ) -> Result<SurfaceTriggerEventReceipt, String> {
        let surface = normalize_surface_id(surface);
        let idempotency_key = trigger.idempotency_key.clone();
        let mut state = self.lock_state()?;
        if let Some(existing) = state.trigger_events.get(&idempotency_key) {
            return Ok(SurfaceTriggerEventReceipt {
                record: existing.clone(),
                duplicate: true,
            });
        }
        let now = now_ms();
        let record = SurfaceTriggerEventRecord {
            idempotency_key: idempotency_key.clone(),
            surface: surface.clone(),
            event_type: event_type.to_string(),
            trigger: trigger.clone(),
            payload_json: payload.clone(),
            status: "received".to_string(),
            attempts: 0,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            next_retry_at_ms: Some(now),
            created_at_ms: now,
            updated_at_ms: now,
            accepted_at_ms: None,
            last_error: None,
        };
        state.trigger_events.insert(idempotency_key, record.clone());
        self.append_record(TRIGGER_EVENT_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface,
            delivery_id: None,
            message_id: None,
            kind: "trigger_event.received".to_string(),
            status: "received".to_string(),
            detail_json: serde_json::json!({
                "event_id": trigger.event_id,
                "event_type": event_type,
                "idempotency_key": record.idempotency_key,
            }),
            created_at_ms: now,
        })?;
        Ok(SurfaceTriggerEventReceipt {
            record,
            duplicate: false,
        })
    }

    pub(crate) fn mark_trigger_event_dispatching(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String> {
        let current = self
            .state
            .lock()
            .map_err(|_| "surface message store lock poisoned".to_string())?
            .trigger_events
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| format!("surface trigger event `{idempotency_key}` not found"))?;
        if !matches!(current.status.as_str(), "received" | "retry_scheduled") {
            return Ok(None);
        }
        self.update_trigger_event(idempotency_key, |record| {
            record.status = "dispatching".to_string();
            record.attempts = record.attempts.saturating_add(1);
            record.next_retry_at_ms = None;
            record.last_error = None;
        })
        .map(Some)
    }

    pub(crate) fn mark_trigger_event_accepted(
        &self,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        self.update_trigger_event(idempotency_key, |record| {
            record.status = "accepted".to_string();
            record.next_retry_at_ms = None;
            record.accepted_at_ms = Some(now_ms());
            record.last_error = None;
        })
    }

    pub(crate) fn mark_trigger_event_failed(
        &self,
        idempotency_key: &str,
        error: impl Into<String>,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let error = error.into();
        self.update_trigger_event(idempotency_key, |record| {
            record.last_error = Some(error.clone());
            if record.attempts < record.max_attempts {
                record.status = "retry_scheduled".to_string();
                record.next_retry_at_ms = Some(next_retry_at_ms(record.attempts));
            } else {
                record.status = "dead_letter".to_string();
                record.next_retry_at_ms = None;
            }
        })
    }

    pub(crate) fn retry_trigger_event(
        &self,
        surface: &str,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let surface = normalize_surface_id(surface);
        let current = self
            .state
            .lock()
            .map_err(|_| "surface message store lock poisoned".to_string())?
            .trigger_events
            .get(idempotency_key)
            .cloned()
            .ok_or_else(|| format!("surface trigger event `{idempotency_key}` not found"))?;
        if current.surface != surface {
            return Err(format!(
                "surface trigger event `{idempotency_key}` does not belong to surface `{surface}`"
            ));
        }
        if current.status != "dead_letter" {
            return Err(format!(
                "operator retry is only allowed for dead_letter trigger events; current status is {}",
                current.status
            ));
        }
        self.update_trigger_event(idempotency_key, |record| {
            record.status = "received".to_string();
            record.attempts = 0;
            record.next_retry_at_ms = Some(now_ms());
            record.last_error = None;
        })
    }

    pub(crate) fn queue_outbox(
        &self,
        request: &SurfaceSendRequest,
        source_session_id: Option<String>,
        reply_to_message_id: Option<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let surface = normalize_surface_id(&request.surface);
        let idempotency_key = request.idempotency_key.clone().unwrap_or_else(|| {
            outbound_idempotency_key(
                &surface,
                reply_to_message_id.as_deref(),
                &request.recipient,
                &request.text,
            )
        });
        let mut state = self.lock_state()?;
        if let Some(existing) = state.outbox.get(&idempotency_key) {
            return Ok(existing.clone());
        }
        let now = now_ms();
        let delivery_id = format!("surface-delivery-{}", uuid::Uuid::new_v4());
        let request_json = serde_json::to_value(request).map_err(|error| error.to_string())?;
        let record = SurfaceOutboxRecord {
            delivery_id: delivery_id.clone(),
            surface: surface.clone(),
            recipient: request.recipient.clone(),
            thread_id: request.thread.clone(),
            idempotency_key: idempotency_key.clone(),
            text_hash: hash_str(&request.text),
            text_summary: summarize_text(&request.text, 240),
            request_json,
            status: "queued".to_string(),
            attempts: 0,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            next_retry_at_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
            sent_at_ms: None,
            last_error: None,
            source_session_id,
            reply_to_message_id,
        };
        state.outbox.insert(idempotency_key, record.clone());
        self.append_record(OUTBOX_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface,
            delivery_id: Some(delivery_id),
            message_id: record.reply_to_message_id.clone(),
            kind: "outbox.queued".to_string(),
            status: "queued".to_string(),
            detail_json: serde_json::json!({
                "recipient": record.recipient,
                "thread_id": record.thread_id,
                "text_summary": record.text_summary,
            }),
            created_at_ms: now,
        })?;
        Ok(record)
    }

    pub(crate) fn mark_delivery_sending(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            if is_terminal_outbox_status(&record.status) {
                return;
            }
            record.status = "sending".to_string();
            record.attempts = record.attempts.saturating_add(1);
            record.updated_at_ms = now_ms();
            record.next_retry_at_ms = None;
            record.last_error = None;
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let status = if outbox_is_failure_notice(&updated) {
                "failure_notifying"
            } else {
                "replying"
            };
            let error = outbox_failure_reason(&updated);
            let _ = self.mark_inbox_status_by_message_id(&updated.surface, reply_to, status, error);
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_sent(
        &self,
        delivery_id: &str,
        result: &SurfaceOperationResult,
    ) -> Result<SurfaceOutboxRecord, String> {
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.status = "sent".to_string();
            record.updated_at_ms = now_ms();
            record.sent_at_ms = Some(record.updated_at_ms);
            record.next_retry_at_ms = None;
            record.last_error = None;
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: result.message_id.clone(),
            kind: "outbox.sent".to_string(),
            status: "sent".to_string(),
            detail_json: serde_json::to_value(result).unwrap_or_else(|_| serde_json::json!({})),
            created_at_ms: now_ms(),
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let (status, error) = if outbox_is_failure_notice(&updated) {
                ("failed_notified", outbox_failure_reason(&updated))
            } else {
                ("replied", None)
            };
            let _ = self.mark_inbox_status_by_message_id(&updated.surface, reply_to, status, error);
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_failed(
        &self,
        delivery_id: &str,
        error: impl Into<String>,
        retryable: bool,
    ) -> Result<SurfaceOutboxRecord, String> {
        let error = error.into();
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.updated_at_ms = now_ms();
            record.last_error = Some(error.clone());
            if retryable && record.attempts < record.max_attempts {
                record.status = "retry_scheduled".to_string();
                record.next_retry_at_ms = Some(next_retry_at_ms(record.attempts));
            } else {
                record.status = "dead_letter".to_string();
                record.next_retry_at_ms = None;
            }
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: updated.reply_to_message_id.clone(),
            kind: if updated.status == "dead_letter" {
                "outbox.dead_letter".to_string()
            } else {
                "outbox.retry_scheduled".to_string()
            },
            status: updated.status.clone(),
            detail_json: serde_json::json!({
                "attempts": updated.attempts,
                "max_attempts": updated.max_attempts,
                "next_retry_at_ms": updated.next_retry_at_ms,
                "last_error": updated.last_error,
            }),
            created_at_ms: now_ms(),
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let inbox_status = if updated.status == "dead_letter" {
                "reply_failed"
            } else {
                "reply_retry_scheduled"
            };
            let _ = self.mark_inbox_status_by_message_id(
                &updated.surface,
                reply_to,
                inbox_status,
                updated.last_error.clone(),
            );
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_dead_letter(
        &self,
        delivery_id: &str,
        reason: impl Into<String>,
    ) -> Result<SurfaceOutboxRecord, String> {
        let reason = reason.into();
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.status = "dead_letter".to_string();
            record.updated_at_ms = now_ms();
            record.next_retry_at_ms = None;
            record.last_error = Some(reason.clone());
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: updated.reply_to_message_id.clone(),
            kind: "outbox.dead_letter".to_string(),
            status: "dead_letter".to_string(),
            detail_json: serde_json::json!({
                "reason": reason,
                "attempts": updated.attempts,
            }),
            created_at_ms: now_ms(),
        })?;
        if let Some(reply_to) = updated.reply_to_message_id.as_deref() {
            let inbox_status = if updated.status == "dead_letter" {
                "reply_failed"
            } else {
                "reply_retry_scheduled"
            };
            let _ = self.mark_inbox_status_by_message_id(
                &updated.surface,
                reply_to,
                inbox_status,
                updated.last_error.clone(),
            );
        }
        Ok(updated)
    }

    pub(crate) fn mark_delivery_replayed(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOutboxRecord, String> {
        let current = self
            .get_outbox_by_delivery(delivery_id)
            .ok_or_else(|| format!("surface delivery `{delivery_id}` not found"))?;
        if current.status != "dead_letter" {
            return Err(format!(
                "operator retry is only allowed for dead_letter deliveries; current status is {}",
                current.status
            ));
        }
        let updated = self.update_outbox_by_delivery(delivery_id, |record| {
            record.status = "queued".to_string();
            record.attempts = 0;
            record.updated_at_ms = now_ms();
            record.next_retry_at_ms = None;
            record.last_error = None;
        })?;
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: updated.surface.clone(),
            delivery_id: Some(updated.delivery_id.clone()),
            message_id: updated.reply_to_message_id.clone(),
            kind: "outbox.operator_retry_requested".to_string(),
            status: "queued".to_string(),
            detail_json: serde_json::json!({"attempts": updated.attempts, "operator_action": "retry"}),
            created_at_ms: now_ms(),
        })?;
        Ok(updated)
    }

    pub(crate) fn archive_dead_letters(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        let now = now_ms();
        let mut state = self.lock_state()?;
        let mut archived = Vec::new();
        let keys = state
            .outbox
            .iter()
            .filter(|(_, record)| {
                record.surface == surface
                    && record.status == "dead_letter"
                    && older_than_ms.is_none_or(|threshold| record.updated_at_ms <= threshold)
            })
            .take(limit.max(1))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(record) = state.outbox.get_mut(&key) {
                record.status = "archived".to_string();
                record.updated_at_ms = now;
                record.next_retry_at_ms = None;
                archived.push(record.clone());
            }
        }
        for record in &archived {
            self.append_record(OUTBOX_FILE, record)?;
        }
        drop(state);
        for record in &archived {
            self.push_event(SurfaceDeliveryEvent {
                event_id: new_event_id(),
                surface: record.surface.clone(),
                delivery_id: Some(record.delivery_id.clone()),
                message_id: record.reply_to_message_id.clone(),
                kind: "outbox.dead_letter_archived".to_string(),
                status: "archived".to_string(),
                detail_json: serde_json::json!({
                    "attempts": record.attempts,
                    "max_attempts": record.max_attempts,
                    "last_error": record.last_error,
                }),
                created_at_ms: now_ms(),
            })?;
        }
        Ok(archived)
    }

    pub(crate) fn purge_archived_events(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<usize, String> {
        let surface = normalize_surface_id(surface);
        let limit = limit.max(1);
        let mut state = self.lock_state()?;
        let archived_delivery_ids = state
            .outbox
            .values()
            .filter(|record| {
                record.surface == surface
                    && record.status == "archived"
                    && older_than_ms.is_none_or(|threshold| record.updated_at_ms <= threshold)
            })
            .map(|record| record.delivery_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if archived_delivery_ids.is_empty() {
            return Ok(0);
        }

        let purged_event_ids = state
            .events
            .iter()
            .filter(|(_, event)| {
                event.surface == surface
                    && event
                        .delivery_id
                        .as_ref()
                        .is_some_and(|delivery_id| archived_delivery_ids.contains(delivery_id))
                    && older_than_ms.is_none_or(|threshold| event.created_at_ms <= threshold)
            })
            .take(limit)
            .map(|(event_id, _)| event_id.clone())
            .collect::<Vec<_>>();

        for event_id in &purged_event_ids {
            state.events.remove(event_id);
        }
        let remaining = state.events.values().cloned().collect::<Vec<_>>();
        drop(state);
        if !purged_event_ids.is_empty() {
            self.rewrite_records(EVENT_FILE, &remaining)?;
        }
        Ok(purged_event_ids.len())
    }

    pub(crate) fn get_outbox_by_delivery(&self, delivery_id: &str) -> Option<SurfaceOutboxRecord> {
        self.state.lock().ok().and_then(|state| {
            state
                .outbox
                .values()
                .find(|record| record.delivery_id == delivery_id)
                .cloned()
        })
    }

    pub(crate) fn due_retry_deliveries(&self) -> Vec<SurfaceOutboxRecord> {
        let now = now_ms();
        self.state
            .lock()
            .map(|state| {
                state
                    .outbox
                    .values()
                    .filter(|record| {
                        record.status == "retry_scheduled"
                            && record.attempts < record.max_attempts
                            && record.next_retry_at_ms.is_some_and(|due| due <= now)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn due_trigger_event_retries(&self) -> Vec<SurfaceTriggerEventRecord> {
        let now = now_ms();
        self.state
            .lock()
            .map(|state| {
                state
                    .trigger_events
                    .values()
                    .filter(|record| {
                        matches!(record.status.as_str(), "received" | "retry_scheduled")
                            && record.attempts < record.max_attempts
                            && record.next_retry_at_ms.is_some_and(|due| due <= now)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn get_inbox_message(
        &self,
        surface: &str,
        message_id: &str,
    ) -> Option<SurfaceInboxRecord> {
        let surface = normalize_surface_id(surface);
        self.state.lock().ok().and_then(|state| {
            state
                .inbox
                .values()
                .find(|record| {
                    record.surface == surface
                        && (record.message_id == message_id || record.id == message_id)
                })
                .cloned()
        })
    }

    pub(crate) fn list_inbox(&self, surface: &str) -> Vec<SurfaceInboxRecord> {
        let surface = normalize_surface_id(surface);
        self.state
            .lock()
            .map(|state| {
                state
                    .inbox
                    .values()
                    .filter(|record| record.surface == surface)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn list_outbox(&self, surface: &str) -> Vec<SurfaceOutboxRecord> {
        let surface = normalize_surface_id(surface);
        self.state
            .lock()
            .map(|state| {
                state
                    .outbox
                    .values()
                    .filter(|record| record.surface == surface)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn list_all_inbox(&self) -> Vec<SurfaceInboxRecord> {
        self.state
            .lock()
            .map(|state| state.inbox.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn list_all_outbox(&self) -> Vec<SurfaceOutboxRecord> {
        self.state
            .lock()
            .map(|state| state.outbox.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn list_trigger_events(&self, surface: &str) -> Vec<SurfaceTriggerEventRecord> {
        let surface = normalize_surface_id(surface);
        self.state
            .lock()
            .map(|state| {
                state
                    .trigger_events
                    .values()
                    .filter(|record| record.surface == surface)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn list_delivery_events(&self, surface: &str) -> Vec<SurfaceDeliveryEvent> {
        let surface = normalize_surface_id(surface);
        self.state
            .lock()
            .map(|state| {
                state
                    .events
                    .values()
                    .filter(|event| event.surface == surface)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn snapshot(&self, surface: &str) -> SurfaceMessageSnapshot {
        let surface = normalize_surface_id(surface);
        let inbox = self.list_inbox(&surface);
        let outbox = self.list_outbox(&surface);
        let trigger_events = self.list_trigger_events(&surface);
        let active_inbox = inbox
            .iter()
            .filter(|record| is_active_inbox_status(&record.status))
            .cloned()
            .collect();
        let terminal_inbox = inbox
            .iter()
            .filter(|record| !is_active_inbox_status(&record.status))
            .cloned()
            .collect();
        let active_trigger_events = trigger_events
            .iter()
            .filter(|record| is_active_trigger_event_status(&record.status))
            .cloned()
            .collect();
        let failed_trigger_events = trigger_events
            .iter()
            .filter(|record| record.status == "dead_letter")
            .cloned()
            .collect();
        let active_outbox = outbox
            .iter()
            .filter(|record| is_active_outbox_status(&record.status))
            .cloned()
            .collect();
        let terminal_outbox = outbox
            .iter()
            .filter(|record| is_terminal_outbox_status(&record.status))
            .cloned()
            .collect();
        let dead_letters = outbox
            .iter()
            .filter(|record| record.status == "dead_letter")
            .cloned()
            .collect();
        let archived_outbox = outbox
            .iter()
            .filter(|record| record.status == "archived")
            .cloned()
            .collect::<Vec<_>>();
        let archived_count = archived_outbox.len();
        SurfaceMessageSnapshot {
            kind: "surface.message_snapshot",
            surface: surface.clone(),
            message_root: self.root.clone(),
            inbox,
            active_inbox,
            terminal_inbox,
            trigger_events,
            active_trigger_events,
            failed_trigger_events,
            outbox,
            active_outbox,
            terminal_outbox,
            deliveries: self.list_delivery_events(&surface),
            dead_letters,
            archived_outbox,
            archived_count,
        }
    }

    fn update_inbox_status(
        &self,
        idempotency_key: &str,
        status: &str,
        runtime_turn_id: Option<String>,
        error: Option<String>,
    ) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let record = state
            .inbox
            .get_mut(idempotency_key)
            .ok_or_else(|| format!("surface inbox `{idempotency_key}` not found"))?;
        record.status = status.to_string();
        record.updated_at_ms = now_ms();
        if runtime_turn_id.is_some() {
            record.runtime_turn_id = runtime_turn_id;
        }
        record.last_error = error;
        let record = record.clone();
        self.append_record(INBOX_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface,
            delivery_id: None,
            message_id: Some(record.message_id),
            kind: format!("inbox.{status}"),
            status: status.to_string(),
            detail_json: serde_json::json!({
                "runtime_turn_id": record.runtime_turn_id,
                "last_error": record.last_error,
            }),
            created_at_ms: now_ms(),
        })
    }

    fn update_trigger_event(
        &self,
        idempotency_key: &str,
        update: impl FnOnce(&mut SurfaceTriggerEventRecord),
    ) -> Result<SurfaceTriggerEventRecord, String> {
        let mut state = self.lock_state()?;
        let record = state
            .trigger_events
            .get_mut(idempotency_key)
            .ok_or_else(|| format!("surface trigger event `{idempotency_key}` not found"))?;
        update(record);
        record.updated_at_ms = now_ms();
        let record = record.clone();
        self.append_record(TRIGGER_EVENT_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: None,
            kind: format!("trigger_event.{}", record.status),
            status: record.status.clone(),
            detail_json: serde_json::json!({
                "event_id": record.trigger.event_id,
                "event_type": record.event_type,
                "idempotency_key": record.idempotency_key,
                "attempts": record.attempts,
                "max_attempts": record.max_attempts,
                "next_retry_at_ms": record.next_retry_at_ms,
                "last_error": record.last_error,
            }),
            created_at_ms: now_ms(),
        })?;
        Ok(record)
    }

    fn mark_inbox_status_by_message_id(
        &self,
        surface: &str,
        message_id: &str,
        status: &str,
        error: Option<String>,
    ) -> Result<Option<SurfaceInboxRecord>, String> {
        let surface = normalize_surface_id(surface);
        let mut state = self.lock_state()?;
        let Some(key) = state.inbox.iter().find_map(|(key, record)| {
            (record.surface == surface && record.message_id == message_id).then(|| key.clone())
        }) else {
            return Ok(None);
        };
        let record = state
            .inbox
            .get_mut(&key)
            .ok_or_else(|| format!("surface inbox `{surface}/{message_id}` not found"))?;
        if record.status == status && record.last_error == error {
            return Ok(Some(record.clone()));
        }
        record.status = status.to_string();
        record.updated_at_ms = now_ms();
        record.last_error = error;
        let record = record.clone();
        self.append_record(INBOX_FILE, &record)?;
        drop(state);
        self.push_event(SurfaceDeliveryEvent {
            event_id: new_event_id(),
            surface: record.surface.clone(),
            delivery_id: None,
            message_id: Some(record.message_id.clone()),
            kind: format!("inbox.{status}"),
            status: status.to_string(),
            detail_json: serde_json::json!({
                "runtime_turn_id": record.runtime_turn_id,
                "last_error": record.last_error,
            }),
            created_at_ms: now_ms(),
        })?;
        Ok(Some(record))
    }

    fn update_outbox_by_delivery(
        &self,
        delivery_id: &str,
        update: impl FnOnce(&mut SurfaceOutboxRecord),
    ) -> Result<SurfaceOutboxRecord, String> {
        let mut state = self.lock_state()?;
        let key = state
            .outbox
            .iter()
            .find_map(|(key, record)| (record.delivery_id == delivery_id).then(|| key.clone()))
            .ok_or_else(|| format!("surface delivery `{delivery_id}` not found"))?;
        let record = state
            .outbox
            .get_mut(&key)
            .ok_or_else(|| format!("surface delivery `{delivery_id}` not found"))?;
        update(record);
        let record = record.clone();
        self.append_record(OUTBOX_FILE, &record)?;
        Ok(record)
    }

    fn push_event(&self, event: SurfaceDeliveryEvent) -> Result<(), String> {
        let mut state = self.lock_state()?;
        state.events.insert(event.event_id.clone(), event.clone());
        self.append_record(EVENT_FILE, &event)
    }

    fn append_record<T: Serialize>(&self, file: &str, record: &T) -> Result<(), String> {
        fs::create_dir_all(&self.root).map_err(|error| error.to_string())?;
        let path = self.root.join(file);
        let mut writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        serde_json::to_writer(&mut writer, record).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())
    }

    fn rewrite_records<T: Serialize>(&self, file: &str, records: &[T]) -> Result<(), String> {
        fs::create_dir_all(&self.root).map_err(|error| error.to_string())?;
        let path = self.root.join(file);
        let mut writer = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        for record in records {
            serde_json::to_writer(&mut writer, record).map_err(|error| error.to_string())?;
            writer.write_all(b"\n").map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, SurfaceMessageState>, String> {
        self.state
            .lock()
            .map_err(|_| "surface message store lock poisoned".to_string())
    }
}

pub(crate) fn inbound_idempotency_key(surface: &str, message_id: &str) -> String {
    format!("{}:{}", normalize_surface_id(surface), message_id)
}

fn outbound_idempotency_key(
    surface: &str,
    reply_to_message_id: Option<&str>,
    recipient: &str,
    text: &str,
) -> String {
    format!(
        "{}:{}:{}:{}",
        normalize_surface_id(surface),
        reply_to_message_id.unwrap_or("manual"),
        recipient,
        hash_str(text)
    )
}

fn load_state(root: &Path) -> Result<SurfaceMessageState, String> {
    let mut state = SurfaceMessageState {
        inbox: read_latest(root.join(INBOX_FILE), |record: &SurfaceInboxRecord| {
            record.idempotency_key.clone()
        })?,
        outbox: read_latest(root.join(OUTBOX_FILE), |record: &SurfaceOutboxRecord| {
            record.idempotency_key.clone()
        })?,
        trigger_events: read_latest(
            root.join(TRIGGER_EVENT_FILE),
            |record: &SurfaceTriggerEventRecord| record.idempotency_key.clone(),
        )?,
        events: read_latest(root.join(EVENT_FILE), |record: &SurfaceDeliveryEvent| {
            record.event_id.clone()
        })?,
    };
    normalize_terminal_outbox_state(&mut state);
    normalize_trigger_event_state(&mut state);
    reconcile_inbox_with_outbox(&mut state);
    Ok(state)
}

fn normalize_trigger_event_state(state: &mut SurfaceMessageState) {
    let now = now_ms();
    for record in state.trigger_events.values_mut() {
        if matches!(record.status.as_str(), "received" | "dispatching") {
            record.status = "retry_scheduled".to_string();
            record.next_retry_at_ms = Some(now);
            record.updated_at_ms = record.updated_at_ms.max(now);
            record.last_error =
                Some("gateway restarted before Runtime acknowledged the trigger event".to_string());
        }
        if is_terminal_trigger_event_status(&record.status) {
            record.next_retry_at_ms = None;
        }
        if record.attempts > record.max_attempts {
            record.attempts = record.max_attempts;
        }
    }
}

fn normalize_terminal_outbox_state(state: &mut SurfaceMessageState) {
    for record in state.outbox.values_mut() {
        if is_terminal_outbox_status(&record.status) {
            record.next_retry_at_ms = None;
        }
        if record.status == "dead_letter" && record.attempts > record.max_attempts {
            record.attempts = record.max_attempts;
        }
    }
}

fn reconcile_inbox_with_outbox(state: &mut SurfaceMessageState) {
    for inbox in state.inbox.values_mut() {
        let related = state.outbox.values().filter(|outbox| {
            outbox.surface == inbox.surface
                && outbox
                    .reply_to_message_id
                    .as_deref()
                    .is_some_and(|message_id| message_id == inbox.message_id)
        });
        let mut sent = None;
        let mut failed = None;
        let mut active = None;
        for outbox in related {
            match outbox.status.as_str() {
                "sent" => sent = Some(outbox),
                "dead_letter" => failed = Some(outbox),
                "queued" | "sending" | "retry_scheduled" => active = Some(outbox),
                _ => {}
            }
        }
        if let Some(outbox) = sent {
            inbox.status = if outbox_is_failure_notice(outbox) {
                "failed_notified"
            } else {
                "replied"
            }
            .to_string();
            inbox.updated_at_ms = inbox.updated_at_ms.max(outbox.updated_at_ms);
            inbox.last_error = outbox_failure_reason(outbox);
        } else if let Some(outbox) = failed {
            inbox.status = "reply_failed".to_string();
            inbox.updated_at_ms = inbox.updated_at_ms.max(outbox.updated_at_ms);
            inbox.last_error = outbox.last_error.clone();
        } else if let Some(outbox) = active {
            inbox.status = if outbox.status == "retry_scheduled" {
                "reply_retry_scheduled"
            } else if outbox_is_failure_notice(outbox) {
                "failure_notifying"
            } else {
                "replying"
            }
            .to_string();
            inbox.updated_at_ms = inbox.updated_at_ms.max(outbox.updated_at_ms);
            inbox.last_error = outbox
                .last_error
                .clone()
                .or_else(|| outbox_failure_reason(outbox));
        } else if is_active_inbox_status(&inbox.status) && inbox.correlation.is_none() {
            inbox.status = "failed".to_string();
            inbox.updated_at_ms = now_ms();
            inbox.last_error = Some(
                "surface processing was interrupted by gateway restart before a reply was queued"
                    .to_string(),
            );
        }
    }
}

fn is_active_inbox_status(status: &str) -> bool {
    matches!(
        status,
        "received"
            | "processing"
            | "processed"
            | "replying"
            | "failure_notifying"
            | "reply_retry_scheduled"
    )
}

fn is_active_outbox_status(status: &str) -> bool {
    matches!(status, "queued" | "sending" | "retry_scheduled")
}

fn is_terminal_outbox_status(status: &str) -> bool {
    matches!(status, "sent" | "dead_letter" | "cancelled" | "archived")
}

fn is_active_trigger_event_status(status: &str) -> bool {
    matches!(status, "received" | "dispatching" | "retry_scheduled")
}

fn is_terminal_trigger_event_status(status: &str) -> bool {
    matches!(status, "accepted" | "dead_letter")
}

fn read_latest<T>(
    path: PathBuf,
    key_fn: impl Fn(&T) -> String,
) -> Result<BTreeMap<String, T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    let Ok(file) = std::fs::File::open(path) else {
        return Ok(BTreeMap::new());
    };
    let reader = std::io::BufReader::new(file);
    let mut records = BTreeMap::new();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<T>(&line).map_err(|error| error.to_string())?;
        records.insert(key_fn(&record), record);
    }
    Ok(records)
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn next_retry_at_ms(attempts: u32) -> i64 {
    let exponent = attempts.saturating_sub(1).min(6);
    let delay_secs = 2_i64.pow(exponent).min(60);
    (Utc::now() + ChronoDuration::seconds(delay_secs)).timestamp_millis()
}

fn new_event_id() -> String {
    format!("surface-delivery-event-{}", uuid::Uuid::new_v4())
}

fn hash_json(value: &serde_json::Value) -> String {
    hash_str(&serde_json::to_string(value).unwrap_or_default())
}

fn hash_str(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn summarize_json(value: &serde_json::Value, limit: usize) -> String {
    summarize_text(&serde_json::to_string(value).unwrap_or_default(), limit)
}

fn summarize_text(value: &str, limit: usize) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        compact
    } else {
        format!(
            "{}...",
            compact
                .chars()
                .take(limit.saturating_sub(3))
                .collect::<String>()
        )
    }
}

fn outbox_is_failure_notice(record: &SurfaceOutboxRecord) -> bool {
    record
        .request_json
        .get("metadata")
        .and_then(|metadata| metadata.get("failure_notice"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn outbox_failure_reason(record: &SurfaceOutboxRecord) -> Option<String> {
    record
        .request_json
        .get("metadata")
        .and_then(|metadata| metadata.get("failure_reason"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trigger_event(id: &str) -> ManagedAgentTriggerEvent {
        ManagedAgentTriggerEvent {
            event_id: format!("surface-event:feishu:message.received:{id}"),
            source_id: "feishu".to_string(),
            source_kind: "surface".to_string(),
            event_type: "message.received".to_string(),
            subject: "surface:feishu:chat-1".to_string(),
            payload_ref: format!("surface-event:{id}"),
            payload_digest: "sha256:test".to_string(),
            occurred_at_ms: 1,
            source_sequence: None,
            idempotency_key: format!("surface-event:feishu:message.received:{id}"),
            source_capabilities: vec!["surface.event.receive".to_string()],
            attributes: BTreeMap::new(),
            trace_refs: vec![format!("surface:feishu:event:{id}")],
        }
    }

    #[test]
    fn trigger_event_handoff_recovers_an_unacknowledged_runtime_delivery() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-trigger-event-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SurfaceMessageStore::new(&root);
        let event = trigger_event("event-1");
        let received = store
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &event,
                &serde_json::json!({"message_id": "event-1", "text": "hello"}),
            )
            .unwrap();
        assert!(!received.duplicate);
        let claimed = store
            .mark_trigger_event_dispatching(&received.record.idempotency_key)
            .unwrap()
            .expect("fresh event must be claimable");
        assert_eq!(claimed.status, "dispatching");
        assert_eq!(claimed.attempts, 1);

        let reloaded = SurfaceMessageStore::new(&root);
        let recovered = reloaded.due_trigger_event_retries();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, "retry_scheduled");
        let retried = reloaded
            .mark_trigger_event_dispatching(&received.record.idempotency_key)
            .unwrap()
            .expect("recovered event must be re-claimable");
        assert_eq!(retried.attempts, 2);
        reloaded
            .mark_trigger_event_accepted(&received.record.idempotency_key)
            .unwrap();

        let duplicate = reloaded
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &event,
                &serde_json::json!({"message_id": "event-1", "text": "hello"}),
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.record.status, "accepted");
        let snapshot = reloaded.snapshot("feishu");
        assert!(snapshot.active_trigger_events.is_empty());
        assert!(snapshot.failed_trigger_events.is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn trigger_event_delivery_has_a_bounded_dead_letter_terminal_state() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-trigger-event-dead-letter-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SurfaceMessageStore::new(&root);
        let event = trigger_event("event-2");
        let received = store
            .record_trigger_event_received(
                "feishu",
                "message.received",
                &event,
                &serde_json::json!({"message_id": "event-2"}),
            )
            .unwrap();
        let key = received.record.idempotency_key;
        let mut terminal = None;
        for _ in 0..DEFAULT_MAX_ATTEMPTS {
            store
                .mark_trigger_event_dispatching(&key)
                .unwrap()
                .expect("retryable event must be claimable");
            terminal = Some(
                store
                    .mark_trigger_event_failed(&key, "runtime unavailable")
                    .unwrap(),
            );
            if terminal
                .as_ref()
                .is_some_and(|record| record.status == "dead_letter")
            {
                break;
            }
            let mut state = store.lock_state().unwrap();
            state
                .trigger_events
                .get_mut(&key)
                .expect("record")
                .next_retry_at_ms = Some(now_ms());
        }
        let terminal = terminal.expect("terminal record");
        assert_eq!(terminal.status, "dead_letter");
        assert_eq!(terminal.attempts, DEFAULT_MAX_ATTEMPTS);
        assert!(store.due_trigger_event_retries().is_empty());
        assert_eq!(store.snapshot("feishu").failed_trigger_events.len(), 1);
        let retried = store.retry_trigger_event("feishu", &key).unwrap();
        assert_eq!(retried.status, "received");
        assert_eq!(retried.attempts, 0);
        assert!(store
            .mark_trigger_event_dispatching(&key)
            .unwrap()
            .is_some());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_inbox_dedupes_across_store_reload() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-inbox-store-{}", uuid::Uuid::new_v4()));
        let store = SurfaceMessageStore::new(&root);
        let receipt = store
            .record_inbox_received(
                "Lark",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "surface:feishu:sender",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
            )
            .unwrap();
        assert!(!receipt.duplicate);

        let reloaded = SurfaceMessageStore::new(&root);
        let duplicate = reloaded
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "surface:feishu:sender",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(reloaded.list_inbox("feishu").len(), 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_outbox_records_retry_and_dead_letter_states() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-outbox-store-{}", uuid::Uuid::new_v4()));
        let store = SurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: Some("thread-1".to_string()),
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({"reply_to": "msg-1"}),
        };
        let queued = store
            .queue_outbox(
                &request,
                Some("session-1".to_string()),
                Some("msg-1".to_string()),
            )
            .unwrap();
        assert_eq!(queued.status, "queued");
        let sending = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        assert_eq!(sending.attempts, 1);
        let retry = store
            .mark_delivery_failed(&queued.delivery_id, "transport timeout", true)
            .unwrap();
        assert_eq!(retry.status, "retry_scheduled");
        let dead = store
            .mark_delivery_dead_letter(&queued.delivery_id, "operator closed")
            .unwrap();
        assert_eq!(dead.status, "dead_letter");
        let sending_after_terminal = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        assert_eq!(sending_after_terminal.status, "dead_letter");
        assert_eq!(sending_after_terminal.attempts, dead.attempts);
        assert_eq!(store.snapshot("feishu").dead_letters.len(), 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn caller_owned_idempotency_key_survives_payload_replay() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-idempotent-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SurfaceMessageStore::new(&root);
        let mut request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "first attempt".to_string(),
            idempotency_key: Some("cross-plane:stable-send".to_string()),
            metadata: serde_json::json!({}),
        };
        let first = store.queue_outbox(&request, None, None).unwrap();
        request.text = "retry after crash".to_string();
        let replay = store.queue_outbox(&request, None, None).unwrap();
        assert_eq!(first.delivery_id, replay.delivery_id);
        assert_eq!(first.idempotency_key, "cross-plane:stable-send");
    }

    #[test]
    fn surface_retry_dead_letter_requires_operator_action() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-retry-store-{}", uuid::Uuid::new_v4()));
        let store = SurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };
        let queued = store.queue_outbox(&request, None, None).unwrap();
        let active_retry = store.mark_delivery_replayed(&queued.delivery_id);
        assert!(active_retry.is_err());

        let _ = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        let _ = store
            .mark_delivery_failed(&queued.delivery_id, "transport timeout", false)
            .unwrap();
        let replayed = store.mark_delivery_replayed(&queued.delivery_id).unwrap();
        assert_eq!(replayed.status, "queued");
        assert_eq!(replayed.attempts, 0);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_archive_dead_letters_preserves_audit_state() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-archive-store-{}", uuid::Uuid::new_v4()));
        let store = SurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };
        let queued = store.queue_outbox(&request, None, None).unwrap();
        let _ = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        let _ = store
            .mark_delivery_failed(&queued.delivery_id, "permanent failure", false)
            .unwrap();
        let archived = store.archive_dead_letters("feishu", None, 10).unwrap();
        assert_eq!(archived.len(), 1);
        let snapshot = store.snapshot("feishu");
        assert!(snapshot.dead_letters.is_empty());
        assert_eq!(snapshot.archived_count, 1);
        assert_eq!(snapshot.archived_outbox[0].status, "archived");
        assert!(snapshot
            .deliveries
            .iter()
            .any(|event| event.kind == "outbox.dead_letter_archived"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_inbox_reaches_replied_after_reply_delivery() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-replied-store-{}", uuid::Uuid::new_v4()));
        let store = SurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "feishu:user:chat",
                Some("chat".to_string()),
                Some("user".to_string()),
            )
            .unwrap();
        store
            .mark_inbox_processing(&inbox.record.idempotency_key)
            .unwrap();
        store
            .mark_inbox_processed(&inbox.record.idempotency_key, Some("turn-1".to_string()))
            .unwrap();
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "chat".to_string(),
            thread: Some("chat".to_string()),
            text: "reply".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({"reply_to": "msg-1"}),
        };
        let delivery = store
            .queue_outbox(
                &request,
                Some("feishu:user:chat".to_string()),
                Some("msg-1".to_string()),
            )
            .unwrap();
        store.mark_delivery_sending(&delivery.delivery_id).unwrap();
        store
            .mark_delivery_sent(
                &delivery.delivery_id,
                &SurfaceOperationResult::ok(
                    "feishu",
                    serde_json::json!({"status": "sent", "message_id": "reply-1"}),
                ),
            )
            .unwrap();

        let snapshot = store.snapshot("feishu");
        assert_eq!(snapshot.inbox[0].status, "replied");
        assert!(snapshot.active_inbox.is_empty());
        assert_eq!(snapshot.terminal_inbox.len(), 1);

        let reloaded = SurfaceMessageStore::new(&root);
        assert_eq!(reloaded.snapshot("feishu").inbox[0].status, "replied");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn admitted_surface_turn_correlation_survives_reload_and_terminal_replay() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-surface-correlation-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "wecom",
                "message-1",
                &serde_json::json!({"text": "investigate"}),
                "surface:wecom:chat-1",
                Some("thread-1".to_string()),
                Some("user-1".to_string()),
            )
            .unwrap();
        let correlation = SurfaceTurnCorrelation {
            surface: "wecom".to_string(),
            message_id: "message-1".to_string(),
            inbox_idempotency_key: inbox.record.idempotency_key.clone(),
            session_id: "surface:wecom:chat-1".to_string(),
            turn_id: "surface-turn:abc".to_string(),
            execution_id: "session-ingress:abc".to_string(),
            reply_to_message_id: "message-1".to_string(),
            reply_idempotency_key: "surface-reply:wecom:message-1".to_string(),
            terminal_id: None,
            terminal_delivery_revision: 0,
        };
        store
            .mark_inbox_admitted(&inbox.record.idempotency_key, correlation)
            .unwrap();
        store
            .record_inbox_terminal_delivery(&inbox.record.idempotency_key, "turn-terminal:req-1")
            .unwrap();
        // Re-observing the same terminal must not create a second recovery
        // revision or a second delivery identity.
        store
            .record_inbox_terminal_delivery(&inbox.record.idempotency_key, "turn-terminal:req-1")
            .unwrap();

        let reloaded = SurfaceMessageStore::new(&root);
        let record = reloaded
            .get_inbox_message("wecom", "message-1")
            .expect("correlation must be durable");
        let correlation = record.correlation.expect("canonical correlation");
        assert_eq!(record.status, "processed");
        assert_eq!(correlation.execution_id, "session-ingress:abc");
        assert_eq!(
            correlation.terminal_id.as_deref(),
            Some("turn-terminal:req-1")
        );
        assert_eq!(correlation.terminal_delivery_revision, 1);
        assert_eq!(
            correlation.reply_idempotency_key,
            "surface-reply:wecom:message-1"
        );
        assert!(reloaded
            .snapshot("wecom")
            .active_inbox
            .iter()
            .any(|item| item.message_id == "message-1"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_failure_notice_marks_inbox_failed_notified() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-failure-notice-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "inspect readme"}),
                "feishu:user:chat",
                Some("chat".to_string()),
                Some("user".to_string()),
            )
            .unwrap();
        store
            .mark_inbox_processing(&inbox.record.idempotency_key)
            .unwrap();
        store
            .mark_inbox_failed(&inbox.record.idempotency_key, "turn timed out after 240s")
            .unwrap();
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "chat".to_string(),
            thread: Some("chat".to_string()),
            text: "failed".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({
                "reply_to": "msg-1",
                "failure_notice": true,
                "failure_reason": "turn timed out after 240s"
            }),
        };
        let delivery = store
            .queue_outbox(
                &request,
                Some("feishu:user:chat".to_string()),
                Some("msg-1".to_string()),
            )
            .unwrap();
        store.mark_delivery_sending(&delivery.delivery_id).unwrap();
        assert_eq!(
            store.snapshot("feishu").inbox[0].status,
            "failure_notifying"
        );
        store
            .mark_delivery_sent(
                &delivery.delivery_id,
                &SurfaceOperationResult::ok(
                    "feishu",
                    serde_json::json!({"status": "sent", "message_id": "reply-1"}),
                ),
            )
            .unwrap();

        let snapshot = store.snapshot("feishu");
        assert_eq!(snapshot.inbox[0].status, "failed_notified");
        assert_eq!(
            snapshot.inbox[0].last_error.as_deref(),
            Some("turn timed out after 240s")
        );
        assert!(snapshot.active_inbox.is_empty());
        assert_eq!(snapshot.terminal_inbox.len(), 1);

        let reloaded = SurfaceMessageStore::new(&root);
        assert_eq!(
            reloaded.snapshot("feishu").inbox[0].status,
            "failed_notified"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reload_marks_orphan_active_inbox_as_failed() {
        let root = std::env::temp_dir().join(format!(
            "cowd-edge-orphan-inbox-store-{}",
            uuid::Uuid::new_v4()
        ));
        let store = SurfaceMessageStore::new(&root);
        let inbox = store
            .record_inbox_received(
                "feishu",
                "msg-1",
                &serde_json::json!({"text": "hello"}),
                "feishu:user:chat",
                Some("chat".to_string()),
                Some("user".to_string()),
            )
            .unwrap();
        store
            .mark_inbox_processing(&inbox.record.idempotency_key)
            .unwrap();

        let reloaded = SurfaceMessageStore::new(&root);
        let snapshot = reloaded.snapshot("feishu");
        assert_eq!(snapshot.inbox[0].status, "failed");
        assert!(snapshot.active_inbox.is_empty());
        assert_eq!(
            snapshot.inbox[0].last_error.as_deref(),
            Some("surface processing was interrupted by gateway restart before a reply was queued")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn surface_purge_archived_events_keeps_outbox_terminal_state() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-purge-store-{}", uuid::Uuid::new_v4()));
        let store = SurfaceMessageStore::new(&root);
        let request = SurfaceSendRequest {
            surface: "feishu".to_string(),
            recipient: "user-1".to_string(),
            thread: None,
            text: "hello".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };
        let queued = store.queue_outbox(&request, None, None).unwrap();
        let _ = store.mark_delivery_sending(&queued.delivery_id).unwrap();
        let _ = store
            .mark_delivery_failed(&queued.delivery_id, "transport timeout", false)
            .unwrap();
        let archived = store.archive_dead_letters("feishu", None, 10).unwrap();
        assert_eq!(archived.len(), 1);
        assert!(!store.list_delivery_events("feishu").is_empty());

        let purged = store.purge_archived_events("feishu", None, 100).unwrap();
        assert!(purged > 0);
        let snapshot = store.snapshot("feishu");
        assert_eq!(snapshot.archived_count, 1);
        assert_eq!(snapshot.archived_outbox[0].delivery_id, queued.delivery_id);
        assert!(store
            .list_delivery_events("feishu")
            .iter()
            .all(|event| event.delivery_id.as_deref() != Some(&queued.delivery_id)));

        let reloaded = SurfaceMessageStore::new(&root);
        assert_eq!(reloaded.snapshot("feishu").archived_count, 1);
        assert!(reloaded
            .list_delivery_events("feishu")
            .iter()
            .all(|event| event.delivery_id.as_deref() != Some(&queued.delivery_id)));

        let _ = std::fs::remove_dir_all(root);
    }
}
