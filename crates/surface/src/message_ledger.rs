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

use crate::{SurfaceFrame, SurfaceOperationResult, SurfaceSendRequest};

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
    pub last_error: Option<String>,
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
    ) -> Result<SurfaceInboxReceipt, String>;
    fn mark_inbox_processing(&self, idempotency_key: &str) -> Result<(), String>;
    fn mark_inbox_processed(
        &self,
        idempotency_key: &str,
        runtime_turn_id: Option<String>,
    ) -> Result<(), String>;
    fn mark_inbox_admitted(
        &self,
        idempotency_key: &str,
        correlation: SurfaceTurnCorrelation,
    ) -> Result<(), String>;
    fn record_inbox_terminal_delivery(
        &self,
        idempotency_key: &str,
        terminal_id: &str,
    ) -> Result<(), String>;
    fn mark_inbox_replied(&self, idempotency_key: &str) -> Result<(), String>;
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
}
