//! Durable message-ledger contract shared by Surface hosts and storage adapters.
//!
//! This module deliberately contains facts and operations, not a database
//! implementation.  Gateway supplies the current SQLite adapter; a later
//! storage crate can implement the same contract without changing transport
//! and orchestration callers.

use std::path::PathBuf;

use harness_contract::managed_agent::ManagedAgentTriggerEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{normalize_surface_id, SurfaceFrame, SurfaceOperationResult, SurfaceSendRequest};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceTurnCorrelation {
    pub surface: String,
    pub message_id: String,
    pub inbox_idempotency_key: String,
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub reply_to_message_id: String,
    pub reply_idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    #[serde(default)]
    pub terminal_delivery_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceInboxRecord {
    pub id: String,
    pub surface: String,
    pub message_id: String,
    pub idempotency_key: String,
    pub thread_id: Option<String>,
    pub sender_id: Option<String>,
    pub payload_hash: String,
    pub payload_summary: String,
    pub payload_json: Value,
    pub status: String,
    pub received_at_ms: i64,
    pub updated_at_ms: i64,
    pub runtime_session_id: Option<String>,
    pub runtime_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation: Option<SurfaceTurnCorrelation>,
    #[serde(default)]
    pub session_projections: Vec<SurfaceSessionProjectionRecord>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceSessionProjectionDraft {
    pub phase: String,
    pub session_id: String,
    pub scope: String,
    pub kind: String,
    pub status: String,
    pub payload_json: Value,
    #[serde(default)]
    pub phase_offset_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceSessionProjectionRecord {
    pub phase: String,
    pub event_id: String,
    pub session_id: String,
    pub scope: String,
    pub kind: String,
    pub status: String,
    pub payload_json: Value,
    pub created_at_ms: u64,
    pub projection_state: String,
    pub projected_at_ms: Option<i64>,
    pub last_error: Option<String>,
}

impl SurfaceInboxRecord {
    pub fn stage_session_projections(
        &mut self,
        drafts: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String> {
        for draft in drafts {
            if draft.phase.trim().is_empty()
                || draft.session_id.trim().is_empty()
                || draft.scope.trim().is_empty()
                || draft.kind.trim().is_empty()
                || draft.status.trim().is_empty()
            {
                return Err("surface Session projection draft is missing identity".to_string());
            }
            let event_id = surface_projection_event_id(&self.idempotency_key, &draft.phase);
            let candidate = SurfaceSessionProjectionRecord {
                phase: draft.phase.clone(),
                event_id,
                session_id: draft.session_id.clone(),
                scope: draft.scope.clone(),
                kind: draft.kind.clone(),
                status: draft.status.clone(),
                payload_json: draft.payload_json.clone(),
                created_at_ms: u64::try_from(self.received_at_ms)
                    .map_err(|_| "surface inbox received timestamp is negative".to_string())?
                    .saturating_add(draft.phase_offset_ms),
                projection_state: "pending".to_string(),
                projected_at_ms: None,
                last_error: None,
            };
            if let Some(existing) = self
                .session_projections
                .iter()
                .find(|record| record.phase == candidate.phase)
            {
                let semantically_equal = existing.event_id == candidate.event_id
                    && existing.session_id == candidate.session_id
                    && existing.scope == candidate.scope
                    && existing.kind == candidate.kind
                    && existing.status == candidate.status
                    && existing.payload_json == candidate.payload_json
                    && existing.created_at_ms == candidate.created_at_ms;
                if !semantically_equal {
                    return Err(format!(
                        "surface Session projection phase `{}` has conflicting durable content",
                        candidate.phase
                    ));
                }
                continue;
            }
            self.session_projections.push(candidate);
        }
        self.session_projections
            .sort_by_key(|record| record.created_at_ms);
        Ok(())
    }

    pub fn mark_session_projection_applied(
        &mut self,
        event_id: &str,
        projected_at_ms: i64,
    ) -> Result<(), String> {
        let projection = self
            .session_projections
            .iter_mut()
            .find(|record| record.event_id == event_id)
            .ok_or_else(|| format!("surface Session projection `{event_id}` not found"))?;
        projection.projection_state = "applied".to_string();
        projection.projected_at_ms = Some(projected_at_ms);
        projection.last_error = None;
        Ok(())
    }

    pub fn mark_session_projection_failed(
        &mut self,
        event_id: &str,
        error: &str,
    ) -> Result<(), String> {
        let projection = self
            .session_projections
            .iter_mut()
            .find(|record| record.event_id == event_id)
            .ok_or_else(|| format!("surface Session projection `{event_id}` not found"))?;
        projection.projection_state = "pending".to_string();
        projection.last_error = Some(error.to_string());
        Ok(())
    }
}

#[must_use]
pub fn surface_projection_event_id(inbox_key: &str, phase: &str) -> String {
    let digest = Sha256::digest(format!("{inbox_key}:{phase}").as_bytes());
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("surface-projection:{phase}:{suffix}")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceOutboxRecord {
    pub delivery_id: String,
    pub surface: String,
    pub recipient: String,
    pub thread_id: Option<String>,
    pub idempotency_key: String,
    pub text_hash: String,
    pub text_summary: String,
    pub request_json: Value,
    pub status: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_retry_at_ms: Option<i64>,
    #[serde(default)]
    pub claim_owner: Option<String>,
    #[serde(default)]
    pub lease_until_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub sent_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub source_session_id: Option<String>,
    pub reply_to_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceTriggerEventRecord {
    pub idempotency_key: String,
    pub surface: String,
    pub event_type: String,
    pub trigger: ManagedAgentTriggerEvent,
    pub payload_json: Value,
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
pub struct SurfaceDeliveryEvent {
    pub event_id: String,
    pub surface: String,
    pub delivery_id: Option<String>,
    pub message_id: Option<String>,
    pub kind: String,
    pub status: String,
    pub detail_json: Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceMessageSnapshot {
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
pub struct SurfaceInboxReceipt {
    pub record: SurfaceInboxRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub struct SurfaceTriggerEventReceipt {
    pub record: SurfaceTriggerEventRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub struct SurfaceIngressClaim {
    pub record_key: String,
    pub frame: SurfaceFrame,
}

/// Full durable ingress state used only for quiesced backend migration.
/// It retains ownership/lease facts so a restart after copy has the same
/// recovery behavior as the source ledger.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceIngressFrameRecord {
    pub record_key: String,
    pub surface: String,
    pub session_id: String,
    pub status: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_retry_at_ms: Option<i64>,
    pub claim_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub frame: SurfaceFrame,
    pub last_error: Option<String>,
}

/// Backend-neutral, complete Surface ledger migration carrier.
///
/// The snapshot excludes diagnostic paths, connection URLs and any secret.
/// It is only valid when the source has been quiesced by its operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SurfaceMessageLedgerMigrationSnapshot {
    pub inbox: Vec<SurfaceInboxRecord>,
    pub outbox: Vec<SurfaceOutboxRecord>,
    pub trigger_events: Vec<SurfaceTriggerEventRecord>,
    pub delivery_events: Vec<SurfaceDeliveryEvent>,
    pub ingress_frames: Vec<SurfaceIngressFrameRecord>,
}

impl SurfaceMessageLedgerMigrationSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        validate_unique(
            self.inbox
                .iter()
                .map(|record| record.idempotency_key.as_str()),
            "surface inbox idempotency key",
        )?;
        validate_unique(
            self.outbox
                .iter()
                .map(|record| record.idempotency_key.as_str()),
            "surface outbox idempotency key",
        )?;
        validate_unique(
            self.outbox.iter().map(|record| record.delivery_id.as_str()),
            "surface delivery id",
        )?;
        validate_unique(
            self.trigger_events
                .iter()
                .map(|record| record.idempotency_key.as_str()),
            "surface trigger idempotency key",
        )?;
        validate_unique(
            self.delivery_events
                .iter()
                .map(|record| record.event_id.as_str()),
            "surface delivery event id",
        )?;
        validate_unique(
            self.ingress_frames
                .iter()
                .map(|record| record.record_key.as_str()),
            "surface ingress record key",
        )?;
        for record in &self.inbox {
            if record.surface.trim().is_empty()
                || record.message_id.trim().is_empty()
                || record.idempotency_key.trim().is_empty()
            {
                return Err("surface inbox migration record is missing identity".to_string());
            }
            let mut phases = std::collections::BTreeSet::new();
            for projection in &record.session_projections {
                if projection.event_id.trim().is_empty()
                    || projection.session_id.trim().is_empty()
                    || projection.kind.trim().is_empty()
                    || !matches!(projection.projection_state.as_str(), "pending" | "applied")
                    || !phases.insert(projection.phase.as_str())
                {
                    return Err(
                        "surface inbox migration projection is invalid or duplicated".to_string(),
                    );
                }
            }
        }
        for record in &self.outbox {
            if record.surface.trim().is_empty()
                || record.delivery_id.trim().is_empty()
                || record.idempotency_key.trim().is_empty()
            {
                return Err("surface outbox migration record is missing identity".to_string());
            }
        }
        for record in &self.trigger_events {
            if record.surface.trim().is_empty()
                || record.event_type.trim().is_empty()
                || record.idempotency_key.trim().is_empty()
            {
                return Err("surface trigger migration record is missing identity".to_string());
            }
        }
        for record in &self.delivery_events {
            if record.surface.trim().is_empty() || record.event_id.trim().is_empty() {
                return Err(
                    "surface delivery event migration record is missing identity".to_string(),
                );
            }
        }
        for record in &self.ingress_frames {
            if record.record_key.trim().is_empty()
                || record.surface.trim().is_empty()
                || record.session_id.trim().is_empty()
                || record.status.trim().is_empty()
                || record.max_attempts == 0
                || record.attempts > record.max_attempts
            {
                return Err("surface ingress migration record is invalid".to_string());
            }
            match &record.frame {
                SurfaceFrame::Event { surface, .. }
                    if normalize_surface_id(surface) == normalize_surface_id(&record.surface) => {}
                SurfaceFrame::Event { .. } => {
                    return Err("surface ingress frame surface does not match record".to_string())
                }
                _ => return Err("surface ingress migration frame must be an event".to_string()),
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<String, String> {
        self.validate()?;
        let mut canonical = self.clone();
        canonical
            .inbox
            .sort_by(|left, right| left.idempotency_key.cmp(&right.idempotency_key));
        canonical
            .outbox
            .sort_by(|left, right| left.idempotency_key.cmp(&right.idempotency_key));
        canonical
            .trigger_events
            .sort_by(|left, right| left.idempotency_key.cmp(&right.idempotency_key));
        canonical
            .delivery_events
            .sort_by(|left, right| left.event_id.cmp(&right.event_id));
        canonical
            .ingress_frames
            .sort_by(|left, right| left.record_key.cmp(&right.record_key));
        let payload = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
        Ok(format!("{:x}", Sha256::digest(payload)))
    }
}

fn validate_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    name: &str,
) -> Result<(), String> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(format!("{name} must not be empty"));
    }
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(format!("duplicate {name} in migration snapshot"));
    }
    Ok(())
}

/// Canonical durable operations for a Surface message ledger.
///
/// Methods return storage errors explicitly: an unavailable ledger is not an
/// empty inbox, outbox, or delivery history.  The contract has no generic
/// methods so it is safe to inject behind `Arc<dyn SurfaceMessageLedger>`.
pub trait SurfaceMessageLedger: std::fmt::Debug + Send + Sync {
    fn diagnostic_root(&self) -> PathBuf;
    fn persist_ingress_frame(&self, frame: &SurfaceFrame) -> Result<String, String>;
    fn claim_ingress_frames(
        &self,
        claim_owner: &str,
        limit: usize,
        lease_ms: i64,
    ) -> Result<Vec<SurfaceIngressClaim>, String>;
    fn complete_ingress_frame(&self, record_key: &str) -> Result<(), String>;
    fn fail_ingress_frame(&self, record_key: &str, error: &str) -> Result<(), String>;
    fn record_inbox_received(
        &self,
        surface: &str,
        message_id: &str,
        payload: &Value,
        runtime_session_id: &str,
        thread_id: Option<String>,
        sender_id: Option<String>,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<SurfaceInboxReceipt, String>;
    fn mark_inbox_processing(
        &self,
        idempotency_key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String>;
    fn mark_inbox_processed(
        &self,
        idempotency_key: &str,
        runtime_turn_id: Option<String>,
    ) -> Result<(), String>;
    fn mark_inbox_admitted(
        &self,
        idempotency_key: &str,
        correlation: SurfaceTurnCorrelation,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String>;
    fn record_inbox_terminal_delivery(
        &self,
        idempotency_key: &str,
        terminal_id: &str,
    ) -> Result<(), String>;
    fn mark_inbox_replied(
        &self,
        idempotency_key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String>;
    fn stage_inbox_projections(
        &self,
        idempotency_key: &str,
        projections: &[SurfaceSessionProjectionDraft],
    ) -> Result<(), String>;
    fn mark_inbox_projection_applied(
        &self,
        idempotency_key: &str,
        event_id: &str,
        projected_at_ms: i64,
    ) -> Result<(), String>;
    fn mark_inbox_projection_failed(
        &self,
        idempotency_key: &str,
        event_id: &str,
        error: &str,
    ) -> Result<(), String>;
    fn mark_inbox_reply_failed(&self, idempotency_key: &str, error: &str) -> Result<(), String>;
    fn mark_inbox_failed(&self, idempotency_key: &str, error: &str) -> Result<(), String>;
    fn record_trigger_event_received(
        &self,
        surface: &str,
        event_type: &str,
        trigger: &ManagedAgentTriggerEvent,
        payload: &Value,
    ) -> Result<SurfaceTriggerEventReceipt, String>;
    fn mark_trigger_event_dispatching(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<SurfaceTriggerEventRecord>, String>;
    fn mark_trigger_event_accepted(
        &self,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String>;
    fn mark_trigger_event_failed(
        &self,
        idempotency_key: &str,
        error: &str,
    ) -> Result<SurfaceTriggerEventRecord, String>;
    fn retry_trigger_event(
        &self,
        surface: &str,
        idempotency_key: &str,
    ) -> Result<SurfaceTriggerEventRecord, String>;
    fn queue_outbox(
        &self,
        request: &SurfaceSendRequest,
        source_session_id: Option<String>,
        reply_to_message_id: Option<String>,
    ) -> Result<SurfaceOutboxRecord, String>;
    fn mark_delivery_sending(&self, delivery_id: &str) -> Result<SurfaceOutboxRecord, String>;
    fn mark_delivery_sent(
        &self,
        delivery_id: &str,
        result: &SurfaceOperationResult,
    ) -> Result<SurfaceOutboxRecord, String>;
    fn mark_delivery_failed(
        &self,
        delivery_id: &str,
        error: &str,
        retryable: bool,
    ) -> Result<SurfaceOutboxRecord, String>;
    fn mark_delivery_dead_letter(
        &self,
        delivery_id: &str,
        reason: &str,
    ) -> Result<SurfaceOutboxRecord, String>;
    fn mark_delivery_replayed(&self, delivery_id: &str) -> Result<SurfaceOutboxRecord, String>;
    fn archive_dead_letters(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SurfaceOutboxRecord>, String>;
    fn purge_archived_events(
        &self,
        surface: &str,
        older_than_ms: Option<i64>,
        limit: usize,
    ) -> Result<usize, String>;
    fn get_outbox_by_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<SurfaceOutboxRecord>, String>;
    fn due_retry_deliveries(&self) -> Result<Vec<SurfaceOutboxRecord>, String>;
    fn due_trigger_event_retries(&self) -> Result<Vec<SurfaceTriggerEventRecord>, String>;
    fn get_inbox_message(
        &self,
        surface: &str,
        message_id: &str,
    ) -> Result<Option<SurfaceInboxRecord>, String>;
    fn list_inbox(&self, surface: &str) -> Result<Vec<SurfaceInboxRecord>, String>;
    fn list_outbox(&self, surface: &str) -> Result<Vec<SurfaceOutboxRecord>, String>;
    fn list_all_inbox(&self) -> Result<Vec<SurfaceInboxRecord>, String>;
    fn list_all_outbox(&self) -> Result<Vec<SurfaceOutboxRecord>, String>;
    fn list_trigger_events(&self, surface: &str) -> Result<Vec<SurfaceTriggerEventRecord>, String>;
    fn list_delivery_events(&self, surface: &str) -> Result<Vec<SurfaceDeliveryEvent>, String>;
    fn snapshot(&self, surface: &str) -> Result<SurfaceMessageSnapshot, String>;
    fn export_migration_snapshot(&self) -> Result<SurfaceMessageLedgerMigrationSnapshot, String>;
    fn import_migration_snapshot(
        &self,
        snapshot: &SurfaceMessageLedgerMigrationSnapshot,
    ) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inbox() -> SurfaceInboxRecord {
        SurfaceInboxRecord {
            id: "feishu:message-1".to_string(),
            surface: "feishu".to_string(),
            message_id: "message-1".to_string(),
            idempotency_key: "feishu:message-1".to_string(),
            thread_id: None,
            sender_id: None,
            payload_hash: "hash".to_string(),
            payload_summary: "hello".to_string(),
            payload_json: serde_json::json!({"text":"hello"}),
            status: "received".to_string(),
            received_at_ms: 100,
            updated_at_ms: 100,
            runtime_session_id: Some("session-1".to_string()),
            runtime_turn_id: None,
            correlation: None,
            session_projections: Vec::new(),
            last_error: None,
        }
    }

    fn draft(payload: Value) -> SurfaceSessionProjectionDraft {
        SurfaceSessionProjectionDraft {
            phase: "received".to_string(),
            session_id: "session-1".to_string(),
            scope: "message".to_string(),
            kind: "surface.message_received".to_string(),
            status: "received".to_string(),
            payload_json: payload,
            phase_offset_ms: 0,
        }
    }

    #[test]
    fn projection_payload_is_immutable_across_retries() {
        let mut inbox = inbox();
        inbox
            .stage_session_projections(&[draft(serde_json::json!({"text":"hello"}))])
            .unwrap();
        let event_id = inbox.session_projections[0].event_id.clone();
        inbox
            .stage_session_projections(&[draft(serde_json::json!({"text":"hello"}))])
            .unwrap();
        assert_eq!(inbox.session_projections.len(), 1);
        assert_eq!(inbox.session_projections[0].event_id, event_id);
        assert!(inbox
            .stage_session_projections(&[draft(serde_json::json!({"text":"changed"}))])
            .is_err());
    }

    #[test]
    fn failed_projection_remains_pending_until_applied() {
        let mut inbox = inbox();
        inbox
            .stage_session_projections(&[draft(serde_json::json!({"text":"hello"}))])
            .unwrap();
        let event_id = inbox.session_projections[0].event_id.clone();
        inbox
            .mark_session_projection_failed(&event_id, "temporary")
            .unwrap();
        assert_eq!(inbox.session_projections[0].projection_state, "pending");
        inbox
            .mark_session_projection_applied(&event_id, 200)
            .unwrap();
        assert_eq!(inbox.session_projections[0].projection_state, "applied");
        assert_eq!(inbox.session_projections[0].projected_at_ms, Some(200));
    }
}
