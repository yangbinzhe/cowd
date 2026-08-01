//! SQLite-backed durable Session repository.
//!
//! The database path is retained while a bounded connection pool serves
//! synchronous repository operations. WAL mode permits concurrent readers
//! while writes remain transactionally serialized by SQLite.
//!
//! ## Schema
//!
//! Two tables are managed:
//!
//! * `sessions` – one row per conversation session.
//! * `session_memories` – many-to-many join between sessions and memory IDs.

use std::path::Path;

use chrono::{DateTime, Utc};
use harness_contract::turn::InputRoutingDecision;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain::{
        SessionBranchActivation, SessionBranchActivationPhase, SessionBranchActivationTransition,
        SessionDomainEvent, SessionDomainRef, SessionDomainScope, SessionLifecycleIntent,
        SessionLifecyclePhase, SessionLifecyclePlan, SessionLifecycleTransition,
        SESSION_DOMAIN_EVENT_TYPE,
    },
    error::SessionError,
    persistence::Result,
};

// ---------------------------------------------------------------------------
// Sentinel for in-memory databases (tests only)
// ---------------------------------------------------------------------------

const IN_MEMORY_PATH: &str = ":memory:";

fn new_pool(db_path: &str, max_size: u32) -> Result<Pool<SqliteConnectionManager>> {
    let manager = SqliteConnectionManager::file(db_path);
    Pool::builder()
        .max_size(max_size)
        .build(manager)
        .map_err(|e| SessionError::Store(e.to_string()))
}

fn sql_err(e: rusqlite::Error) -> SessionError {
    SessionError::Store(e.to_string())
}

/// Configure per-connection pragmas (WAL mode, foreign keys, busy timeout).
/// Execute a pragma that may return rows (rusqlite 0.31 treats this as an error).
fn exec_pragma(conn: &Connection, sql: &str) -> Result<()> {
    match conn.execute(sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::ExecuteReturnedResults) => Ok(()),
        Err(e) => Err(sql_err(e)),
    }
}

fn set_conn_pragmas(conn: &Connection) -> Result<()> {
    exec_pragma(conn, "PRAGMA journal_mode=WAL")?;
    exec_pragma(conn, "PRAGMA foreign_keys=ON")?;
    exec_pragma(conn, "PRAGMA busy_timeout=5000")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

/// FTS5 search result for sessions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSearchResult {
    pub session_id: String,
    pub platform: String,
    pub chat_id: String,
    pub user_id: Option<String>,
    pub created_at: String,
    pub last_activity: String,
    pub message_count: i64,
    /// Highlighted snippet from metadata_json
    pub snippet: Option<String>,
}

/// Filter/sort/page options for DB-backed session listing.
#[derive(Debug, Clone, Default)]
pub struct SessionListOptions<'a> {
    pub query: Option<&'a str>,
    pub model: Option<&'a str>,
    pub status: Option<&'a str>,
    /// Principal that owns sessions through the canonical metadata contract.
    pub owner_principal_id: Option<&'a str>,
    /// Explicit Session/Mission grants resolved by the authenticated caller.
    pub visible_session_ids: &'a [String],
    /// Trusted maintenance callers may request the complete catalog.
    pub unrestricted: bool,
    /// Administrative/history callers may explicitly include tombstoned rows.
    /// User-facing discovery excludes them unless a concrete status is requested.
    pub include_deleted: bool,
    pub sort: &'a str,
    pub order: &'a str,
    pub limit: usize,
    pub offset: usize,
}

/// A page of session records plus the total number of rows matching the
/// filters before pagination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionListPage {
    pub records: Vec<SessionRecord>,
    pub total: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionUsageBucket {
    pub session_count: usize,
    pub message_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionUsageSummary {
    pub session_count: usize,
    pub message_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub estimated_cost_usd: f64,
    pub by_platform: std::collections::BTreeMap<String, SessionUsageBucket>,
    pub by_model: std::collections::BTreeMap<String, SessionUsageBucket>,
    pub recent_sessions: Vec<SessionRecord>,
}

/// A single message within a conversation session.
///
/// Each message belongs to a session and is ordered by `sequence`.
/// The `content_json` field stores the message blocks as a JSON array
/// of `ContentBlock` objects (text, tool_use, tool_result, etc.).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessage {
    /// Immutable cross-surface identity. Sequence is ordering metadata, not a
    /// durable identity: clients must use this value for replay and dedupe.
    pub stable_message_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub role: String,
    pub content_json: String,
    pub blocks_count: usize,
    pub tool_use_id: Option<String>,
    pub tool_name: Option<String>,
    pub token_usage_json: Option<String>,
    pub created_at_ms: u64,
}

/// Durable state of one Session -> Runtime materialization request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxStatus {
    Pending,
    Claimed,
    RetryScheduled,
    Materialized,
    BlockedMaterialization,
}

impl OutboxStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::RetryScheduled => "retry_scheduled",
            Self::Materialized => "materialized",
            Self::BlockedMaterialization => "blocked_materialization",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "materialized" => Ok(Self::Materialized),
            "blocked_materialization" => Ok(Self::BlockedMaterialization),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session runtime outbox status `{other}`").into(),
            )),
        }
    }
}

/// Failure classes determine whether the bridge may retry automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxFailureClass {
    Retryable,
    Permanent,
    AuthorizationBlocked,
    CorruptPayload,
}

impl OutboxFailureClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::AuthorizationBlocked => "authorization_blocked",
            Self::CorruptPayload => "corrupt_payload",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "retryable" => Ok(Self::Retryable),
            "permanent" => Ok(Self::Permanent),
            "authorization_blocked" => Ok(Self::AuthorizationBlocked),
            "corrupt_payload" => Ok(Self::CorruptPayload),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session runtime outbox failure class `{other}`").into(),
            )),
        }
    }
}

/// Stable IDs supplied by ingress for one user message and Runtime request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeOutboxRequest {
    /// Stable user-visible SessionInputId. This identity is independent from
    /// transport idempotency and Runtime dispatch request identities.
    pub input_id: String,
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    /// Session authority generation observed when ingress was accepted.
    pub session_generation: u64,
    /// Durable classification result. Storage persists but never reclassifies
    /// this value on its own.
    pub decision: InputRoutingDecision,
    /// Existing turn targeted by supplement/control decisions. New-turn
    /// decisions leave this empty and use `turn_id` as their execution turn.
    pub target_turn_id: Option<String>,
    /// Versioned classifier evidence/reason payload retained for replay.
    pub classification_json: Option<String>,
    pub created_at_ms: u64,
    /// Opaque, versioned Runtime-owned ingress options. Session persists this
    /// value but never interprets it, preserving the Session→Runtime boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_options_json: Option<String>,
}

/// Canonical durable lifecycle of one Session ingress, including rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRuntimeInputStatus {
    Accepted,
    Classified,
    Queued,
    RejectedDuplicate,
    RejectedPolicy,
    Claimed,
    Running,
    Reclassified,
    Completed,
    Supplemented,
    Failed,
    Blocked,
    Cancelled,
    Expired,
}

impl SessionRuntimeInputStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Classified => "classified",
            Self::Queued => "queued",
            Self::RejectedDuplicate => "rejected_duplicate",
            Self::RejectedPolicy => "rejected_policy",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Reclassified => "reclassified",
            Self::Completed => "completed",
            Self::Supplemented => "supplemented",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "accepted" => Ok(Self::Accepted),
            "classified" => Ok(Self::Classified),
            "queued" | "pending" | "retry_scheduled" => Ok(Self::Queued),
            "rejected_duplicate" => Ok(Self::RejectedDuplicate),
            "rejected_policy" => Ok(Self::RejectedPolicy),
            "claimed" => Ok(Self::Claimed),
            "running" => Ok(Self::Running),
            "reclassified" => Ok(Self::Reclassified),
            "completed" | "materialized" => Ok(Self::Completed),
            "supplemented" => Ok(Self::Supplemented),
            "failed" => Ok(Self::Failed),
            "blocked" | "blocked_materialization" => Ok(Self::Blocked),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session runtime input status `{other}`").into(),
            )),
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RejectedDuplicate
                | Self::RejectedPolicy
                | Self::Completed
                | Self::Supplemented
                | Self::Failed
                | Self::Cancelled
                | Self::Expired
        )
    }

    #[must_use]
    pub const fn is_runnable(self) -> bool {
        matches!(self, Self::Queued | Self::Reclassified)
    }

    #[must_use]
    pub const fn holds_claim(self) -> bool {
        matches!(self, Self::Claimed | Self::Running)
    }

    /// Canonical terminal status for classifier rejection decisions. Every
    /// durable backend must use this mapping and persist the rejection rather
    /// than returning a validation error.
    #[must_use]
    pub const fn for_rejection(decision: InputRoutingDecision) -> Option<Self> {
        match decision {
            InputRoutingDecision::RejectDuplicate => Some(Self::RejectedDuplicate),
            InputRoutingDecision::RejectPolicy => Some(Self::RejectedPolicy),
            _ => None,
        }
    }

    /// Canonical Session-domain event kind for an ingress lifecycle state.
    /// Backend adapters use the same names so SQLite and PostgreSQL timelines
    /// remain replay-compatible.
    #[must_use]
    pub const fn timeline_event_kind(self) -> &'static str {
        match self {
            Self::Accepted => "session.input.accepted.v1",
            Self::Classified => "session.input.classified.v1",
            Self::Queued => "session.input.queued.v1",
            Self::RejectedDuplicate => "session.input.rejected_duplicate.v1",
            Self::RejectedPolicy => "session.input.rejected_policy.v1",
            Self::Claimed => "session.input.claimed.v1",
            Self::Running => "session.input.running.v1",
            Self::Reclassified => "session.input.reclassified.v1",
            Self::Completed => "session.input.completed.v1",
            Self::Supplemented => "session.input.supplemented.v1",
            Self::Failed => "session.input.failed.v1",
            Self::Blocked => "session.input.blocked.v1",
            Self::Cancelled => "session.input.cancelled.v1",
            Self::Expired => "session.input.expired.v1",
        }
    }
}

fn input_decision_as_str(decision: InputRoutingDecision) -> &'static str {
    match decision {
        InputRoutingDecision::StartNewTurn => "start_new_turn",
        InputRoutingDecision::SupplementCurrentTurn => "supplement_current_turn",
        InputRoutingDecision::InterruptAndReplan => "interrupt_and_replan",
        InputRoutingDecision::EnqueueNextStep => "enqueue_next_step",
        InputRoutingDecision::SpawnSubtask => "spawn_subtask",
        InputRoutingDecision::RouteCrossSession => "route_cross_session",
        InputRoutingDecision::CreateNewSession => "create_new_session",
        InputRoutingDecision::ControlOrApproval => "control_or_approval",
        InputRoutingDecision::RejectDuplicate => "reject_duplicate",
        InputRoutingDecision::RejectPolicy => "reject_policy",
    }
}

fn parse_input_decision(value: &str) -> rusqlite::Result<InputRoutingDecision> {
    match value {
        "start_new_turn" => Ok(InputRoutingDecision::StartNewTurn),
        "supplement_current_turn" => Ok(InputRoutingDecision::SupplementCurrentTurn),
        "interrupt_and_replan" => Ok(InputRoutingDecision::InterruptAndReplan),
        "enqueue_next_step" => Ok(InputRoutingDecision::EnqueueNextStep),
        "spawn_subtask" => Ok(InputRoutingDecision::SpawnSubtask),
        "route_cross_session" => Ok(InputRoutingDecision::RouteCrossSession),
        "create_new_session" => Ok(InputRoutingDecision::CreateNewSession),
        "control_or_approval" => Ok(InputRoutingDecision::ControlOrApproval),
        "reject_duplicate" => Ok(InputRoutingDecision::RejectDuplicate),
        "reject_policy" => Ok(InputRoutingDecision::RejectPolicy),
        other => Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("unknown session input decision `{other}`").into(),
        )),
    }
}

fn decision_requires_target_turn(decision: InputRoutingDecision) -> bool {
    matches!(
        decision,
        InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval
    )
}

/// Durable input-admission authority for one Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInputAdmission {
    pub session_id: String,
    pub generation: u64,
    pub open: bool,
}

/// Persisted Session input. `session_generation`, `claim_token`, and
/// `revision` jointly fence every worker-owned transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeOutboxRecord {
    pub input_id: String,
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub session_generation: u64,
    pub decision: InputRoutingDecision,
    pub target_turn_id: Option<String>,
    pub classification_json: Option<String>,
    pub status: SessionRuntimeInputStatus,
    pub runtime_commit_cursor: Option<u64>,
    pub attempts: u32,
    pub next_attempt_at_ms: u64,
    pub claim_owner: Option<String>,
    pub claim_token: Option<String>,
    /// Immutable identity of one acquired claim. It changes only when a new
    /// worker claim is issued; lease renewals advance `revision` but preserve
    /// this epoch so terminal publication can use exact equality.
    #[serde(default)]
    pub claim_fence_epoch: Option<u64>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<OutboxFailureClass>,
    pub last_error: Option<String>,
    pub revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub terminal_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_options_json: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeOutboxHealth {
    pub runnable_depth: usize,
    pub oldest_runnable_created_at_ms: Option<u64>,
    pub accepted: usize,
    pub classified: usize,
    pub queued: usize,
    pub rejected_duplicate: usize,
    pub rejected_policy: usize,
    pub claimed: usize,
    pub running: usize,
    pub reclassified: usize,
    pub completed: usize,
    pub supplemented: usize,
    pub failed: usize,
    pub blocked: usize,
    pub cancelled: usize,
    pub expired: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTerminalExecutionFence {
    pub request_id: String,
    /// Immutable ingress cursor. Binding terminal publication to the exact
    /// Session input row prevents a valid claim tuple from being replayed
    /// against a different queued input.
    pub input_sequence: usize,
    pub session_generation: u64,
    pub claim_owner: String,
    pub claim_token: String,
    /// Immutable epoch allocated when this exact owner/token claim is created.
    /// Unlike the row revision it never changes during lease renewal.
    pub claim_fence_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTerminalTranscriptCommit {
    pub terminal_message_id: String,
    pub ingress_message_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub messages: Vec<SessionMessage>,
    pub runtime_commit_cursor: u64,
    /// Highest durable Session input sequence incorporated into this terminal
    /// candidate. A newer accepted input fences this candidate.
    pub consumed_input_sequence: usize,
    pub created_at_ms: u64,
    pub fence: SessionTerminalExecutionFence,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTerminalTranscriptReceipt {
    pub messages: Vec<SessionMessage>,
    pub inserted: bool,
    pub input: SessionRuntimeOutboxRecord,
}

/// Intent written by the Session authority and materialized by the Runtime
/// bridge into the workspace Mission aggregate.  This is deliberately a
/// separate outbox from ingress: Session records are owned by this SQLite
/// store, while Mission events are owned by RuntimeEventStore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMissionOutboxOperation {
    Register,
    Start,
    Close,
}

impl SessionMissionOutboxOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Start => "start",
            Self::Close => "close",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "register" => Ok(Self::Register),
            "start" => Ok(Self::Start),
            "close" => Ok(Self::Close),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session mission outbox operation `{other}`").into(),
            )),
        }
    }
}

/// Stable request identity for one Session -> Mission lifecycle intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMissionOutboxRequest {
    pub request_id: String,
    pub session_id: String,
    pub title: String,
    pub workspace_key: String,
    pub operation: SessionMissionOutboxOperation,
    pub created_at_ms: u64,
}

/// Atomic branch command. The backend captures source messages before the
/// supplied cutoff and creates every target-side durable artifact in one
/// transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionBranchRequest {
    /// Stable identity shared by the branch database transaction and the
    /// post-commit Runtime activation receipt.
    pub operation_id: String,
    pub source_session_id: String,
    /// Immutable source cutoff. Callers must capture it before issuing the
    /// command so an identical retry can prove the same branch identity.
    pub source_message_count: usize,
    pub target: SessionRecord,
    pub mission_outbox: SessionMissionOutboxRequest,
    pub source_event_json: String,
    pub target_event_json: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionBranchResult {
    pub target: SessionRecord,
    pub copied_message_count: usize,
    pub source_message_count: usize,
    pub activation: SessionBranchActivation,
}

/// Atomic `planned -> admission_fenced` command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionLifecycleFenceRequest {
    pub transition: SessionLifecycleTransition,
    pub actor: String,
    pub reason: String,
    pub transitional_status: String,
    pub event: SessionEvent,
}

/// Atomic `runtime_drained -> tombstone_committed` command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionLifecycleTombstoneRequest {
    pub transition: SessionLifecycleTransition,
    pub record: SessionRecord,
    pub mission_outbox: SessionMissionOutboxRequest,
    pub event: SessionEvent,
}

/// Persisted Mission lifecycle work item. `revision` protects transitions so
/// a stale bridge process cannot acknowledge or fail another worker's lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMissionOutboxRecord {
    pub request_id: String,
    pub session_id: String,
    pub title: String,
    pub workspace_key: String,
    pub operation: SessionMissionOutboxOperation,
    pub status: OutboxStatus,
    pub attempts: u32,
    pub next_attempt_at_ms: u64,
    pub claim_owner: Option<String>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<OutboxFailureClass>,
    pub last_error: Option<String>,
    pub revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Durable, body-free recovery projection for one Session.
///
/// The transcript remains authoritative in `messages`; this manifest is the
/// transactionally maintained index used to decide whether startup must
/// hydrate the transcript into a Runtime carrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecoveryManifest {
    pub session_id: String,
    pub durable_cursor: u64,
    pub event_cursor: u64,
    pub history_revision: u64,
    pub transcript_messages: u64,
    pub transcript_bytes: u64,
    pub latest_checkpoint_sequence: Option<u64>,
    pub latest_checkpoint_event_id: Option<String>,
    pub index_generation: u64,
    pub indexed_through_sequence: Option<u64>,
    pub index_card_count: u64,
    pub index_pending: bool,
    pub in_flight_turn: bool,
    pub pending_approval: bool,
    pub active_writer_or_attachment: bool,
    pub mission_agent_team_continuation: bool,
    pub last_activity_ms: u64,
    pub manifest_revision: u64,
}

pub const SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const CONTEXT_INDEX_CARD_SCHEMA_VERSION: u32 = 1;

/// Indexed checkpoint and history coverage used to activate a Runtime carrier
/// without hydrating the complete durable transcript.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionActivationManifest {
    pub schema_version: u32,
    pub recovery: SessionRecoveryManifest,
    pub projection_generation: u64,
    pub index_complete: bool,
}

/// Body-free metadata for exact history navigation and timeline rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessageMetadata {
    pub stable_message_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub role: String,
    pub blocks_count: usize,
    pub tool_use_id: Option<String>,
    pub tool_name: Option<String>,
    pub created_at_ms: u64,
    pub content_bytes: usize,
}

/// A deterministic, rebuildable navigation card over immutable message rows.
///
/// Cards never replace source messages. Their source range and digest allow
/// Runtime to decide which exact rows to expand while preserving auditability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIndexCard {
    pub schema_version: u32,
    pub card_id: String,
    pub parent_card_id: Option<String>,
    pub session_id: String,
    pub source_start_sequence: usize,
    pub source_end_sequence: usize,
    pub source_message_count: usize,
    pub source_digest: String,
    pub summary: String,
    pub scope: String,
    pub authority: String,
    pub generation: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIndexCoverage {
    pub session_id: String,
    pub source_messages: usize,
    pub covered_messages: usize,
    pub card_count: usize,
    pub indexed_through_sequence: Option<usize>,
    pub generation: u64,
    pub complete: bool,
    pub source_digest: String,
    pub card_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionProjectionRecoveryState {
    Ready,
    ManifestRebuilt,
    IndexPending,
    CheckpointMissing,
    CheckpointMalformed,
    SchemaUnsupported,
}

/// Bounded Runtime activation payload. Source messages remain in the store and
/// are expanded later through exact reads when a card is selected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveSessionProjection {
    pub manifest: SessionActivationManifest,
    pub latest_checkpoint: Option<SessionEvent>,
    pub post_checkpoint_tail: Vec<SessionMessage>,
    pub recent_metadata: Vec<SessionMessageMetadata>,
    pub context_cards: Vec<ContextIndexCard>,
    pub recovery_state: SessionProjectionRecoveryState,
}

impl SessionActivationManifest {
    #[must_use]
    pub fn from_recovery(recovery: SessionRecoveryManifest) -> Self {
        let indexed_messages = recovery
            .indexed_through_sequence
            .map_or(0, |sequence| sequence.saturating_add(1));
        let index_complete =
            !recovery.index_pending && indexed_messages >= recovery.transcript_messages;
        Self {
            schema_version: SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION,
            projection_generation: recovery.manifest_revision,
            recovery,
            index_complete,
        }
    }
}

/// Deterministically rebuild the navigation index from authoritative messages.
///
/// The caller chooses when to run this potentially expensive operation. Normal
/// appends only enqueue work; a background projector or repair command invokes
/// this builder and atomically swaps the resulting cards.
#[must_use]
pub fn build_context_index_cards(
    session_id: &str,
    messages: &[SessionMessage],
    card_span: usize,
    parent_span: usize,
    generation: u64,
    now_ms: u64,
) -> Vec<ContextIndexCard> {
    let card_span = card_span.max(1);
    let parent_span = parent_span.max(2);
    let mut leaves = messages
        .chunks(card_span)
        .map(|chunk| build_leaf_context_card(session_id, chunk, generation, now_ms))
        .collect::<Vec<_>>();
    if leaves.len() <= 1 {
        return leaves;
    }
    let mut parents = Vec::new();
    for children in leaves.chunks_mut(parent_span) {
        let source_start_sequence = children[0].source_start_sequence;
        let source_end_sequence = children
            .last()
            .map_or(source_start_sequence, |card| card.source_end_sequence);
        let mut digest = Sha256::new();
        let mut summaries = Vec::new();
        for child in children.iter() {
            digest.update(child.card_id.as_bytes());
            digest.update(child.source_digest.as_bytes());
            if summaries.len() < 4 {
                summaries.push(child.summary.clone());
            }
        }
        let source_digest = format!("{:x}", digest.finalize());
        let card_id = format!(
            "ctx-parent:{}:{}:{}:{}",
            session_id,
            source_start_sequence,
            source_end_sequence,
            &source_digest[..16]
        );
        for child in children.iter_mut() {
            child.parent_card_id = Some(card_id.clone());
        }
        parents.push(ContextIndexCard {
            schema_version: CONTEXT_INDEX_CARD_SCHEMA_VERSION,
            card_id,
            parent_card_id: None,
            session_id: session_id.to_string(),
            source_start_sequence,
            source_end_sequence,
            source_message_count: children.iter().map(|card| card.source_message_count).sum(),
            source_digest,
            summary: summaries.join(" | "),
            scope: format!("session:{session_id}"),
            authority: "session_history_index".to_string(),
            generation,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        });
    }
    leaves.extend(parents);
    leaves
}

fn build_leaf_context_card(
    session_id: &str,
    messages: &[SessionMessage],
    generation: u64,
    now_ms: u64,
) -> ContextIndexCard {
    let source_start_sequence = messages.first().map_or(0, |message| message.sequence);
    let source_end_sequence = messages.last().map_or(source_start_sequence, |message| {
        message.sequence.saturating_add(1)
    });
    let mut digest = Sha256::new();
    let mut summaries = Vec::new();
    for message in messages {
        digest.update(message.stable_message_id.as_bytes());
        digest.update(message.sequence.to_le_bytes());
        digest.update(message.role.as_bytes());
        digest.update(message.content_json.as_bytes());
        if summaries.len() < 6 {
            let text = message_summary(message);
            if !text.is_empty() {
                summaries.push(format!("{}: {}", message.role, text));
            }
        }
    }
    let source_digest = format!("{:x}", digest.finalize());
    ContextIndexCard {
        schema_version: CONTEXT_INDEX_CARD_SCHEMA_VERSION,
        card_id: format!(
            "ctx-leaf:{}:{}:{}:{}",
            session_id,
            source_start_sequence,
            source_end_sequence,
            &source_digest[..16]
        ),
        parent_card_id: None,
        session_id: session_id.to_string(),
        source_start_sequence,
        source_end_sequence,
        source_message_count: messages.len(),
        source_digest,
        summary: summaries.join(" | "),
        scope: format!("session:{session_id}"),
        authority: "session_history_index".to_string(),
        generation,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    }
}

fn message_summary(message: &SessionMessage) -> String {
    let Ok(blocks) = serde_json::from_str::<serde_json::Value>(&message.content_json) else {
        return String::new();
    };
    let mut text = String::new();
    if let Some(items) = blocks.as_array() {
        for item in items {
            if let Some(value) = item.get("text").and_then(serde_json::Value::as_str) {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(value);
                if text.chars().count() >= 240 {
                    break;
                }
            }
        }
    }
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(240)
        .collect()
}

#[must_use]
pub fn context_index_source_digest(messages: &[SessionMessage]) -> String {
    let mut digest = Sha256::new();
    for message in messages {
        digest.update(message.stable_message_id.as_bytes());
        digest.update(message.sequence.to_le_bytes());
        digest.update(message.role.as_bytes());
        digest.update(message.content_json.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

#[must_use]
pub fn context_index_card_digest(cards: &[ContextIndexCard]) -> String {
    let mut digest = Sha256::new();
    for card in cards.iter().filter(|card| card.parent_card_id.is_some()) {
        digest.update(card.card_id.as_bytes());
        digest.update(card.source_digest.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

impl SessionRecoveryManifest {
    #[must_use]
    pub const fn requires_hydration(&self) -> bool {
        self.in_flight_turn
            || self.pending_approval
            || self.active_writer_or_attachment
            || self.mission_agent_team_continuation
    }
}

/// Explicit recovery signals whose source of truth is outside the transcript.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRecoverySignal {
    PendingApproval,
    ActiveWriterOrAttachment,
    MissionAgentTeamContinuation,
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;").map_err(sql_err)?;

    // Execute each DDL statement individually to avoid rusqlite's execute_batch
    // returning "Execute returned results" errors when FTS5 virtual tables or
    // triggers are involved in a multi-statement batch.
    let statements: &[&str] = &[
        r"CREATE TABLE IF NOT EXISTS sessions (
            session_id    TEXT PRIMARY KEY,
            platform      TEXT NOT NULL,
            chat_id       TEXT NOT NULL,
            user_id       TEXT,
            model         TEXT,
            created_at    TEXT NOT NULL,
            last_activity TEXT NOT NULL,
            message_count INTEGER NOT NULL DEFAULT 0,
            reset_policy  TEXT NOT NULL,
            metadata_json TEXT,
            input_tokens  INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
            status TEXT NOT NULL DEFAULT 'active',
            created_at_ms INTEGER NOT NULL DEFAULT 0,
            updated_at_ms INTEGER NOT NULL DEFAULT 0,
            input_generation INTEGER NOT NULL DEFAULT 1,
            input_admission_open INTEGER NOT NULL DEFAULT 1
        )",
        r"CREATE TABLE IF NOT EXISTS session_memories (
            session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
            memory_id  TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (session_id, memory_id)
        )",
        r"CREATE VIRTUAL TABLE IF NOT EXISTS sessions_fts USING fts5(
            session_id UNINDEXED,
            platform,
            chat_id,
            user_id,
            metadata_json,
            content=sessions,
            content_rowid=rowid
        )",
        r"CREATE TRIGGER IF NOT EXISTS sessions_fts_ai AFTER INSERT ON sessions BEGIN
            INSERT INTO sessions_fts(rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES (new.rowid, new.session_id, new.platform, new.chat_id, new.user_id, new.metadata_json);
        END",
        r"CREATE TRIGGER IF NOT EXISTS sessions_fts_ad AFTER DELETE ON sessions BEGIN
            INSERT INTO sessions_fts(sessions_fts, rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES ('delete', old.rowid, old.session_id, old.platform, old.chat_id, old.user_id, old.metadata_json);
        END",
        r"CREATE TRIGGER IF NOT EXISTS sessions_fts_au AFTER UPDATE ON sessions BEGIN
            INSERT INTO sessions_fts(sessions_fts, rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES ('delete', old.rowid, old.session_id, old.platform, old.chat_id, old.user_id, old.metadata_json);
            INSERT INTO sessions_fts(rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES (new.rowid, new.session_id, new.platform, new.chat_id, new.user_id, new.metadata_json);
        END",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_platform      ON sessions(platform)",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON sessions(last_activity)",
        r"CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stable_message_id TEXT NOT NULL,
            session_id      TEXT NOT NULL,
            sequence        INTEGER NOT NULL,
            role            TEXT NOT NULL,
            content_json    TEXT NOT NULL,
            blocks_count    INTEGER NOT NULL DEFAULT 1,
            tool_use_id     TEXT,
            tool_name       TEXT,
            token_usage_json TEXT,
            created_at_ms   INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            UNIQUE(stable_message_id),
            UNIQUE(session_id, sequence)
        )",
        r"CREATE INDEX IF NOT EXISTS idx_messages_session     ON messages(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_messages_session_seq ON messages(session_id, sequence)",
        r"CREATE TABLE IF NOT EXISTS session_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_json TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        )",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_session     ON session_events(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_session_seq ON session_events(session_id, sequence)",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_session_type_seq
            ON session_events(session_id, event_type, sequence)",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_context_envelope_id
            ON session_events(json_extract(event_json, '$.envelope.id'))
            WHERE event_type = 'ContextEnvelope'",
        r"CREATE UNIQUE INDEX IF NOT EXISTS uq_session_domain_event_id
            ON session_events(session_id, json_extract(event_json, '$.event_id'))
            WHERE event_type = 'SessionDomainEvent'
              AND json_extract(event_json, '$.event_id') IS NOT NULL",
        r"CREATE INDEX IF NOT EXISTS idx_session_domain_kind_sequence
            ON session_events(session_id, json_extract(event_json, '$.kind'), sequence)
            WHERE event_type = 'SessionDomainEvent'",
        r"CREATE TABLE IF NOT EXISTS session_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event_idx INTEGER NOT NULL,
            messages_json TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        )",
        r"CREATE INDEX IF NOT EXISTS idx_session_snapshots_session ON session_snapshots(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_session_snapshots_latest  ON session_snapshots(session_id, event_idx DESC)",
    ];

    for stmt in statements {
        conn.execute_batch(stmt).map_err(sql_err)?;
    }

    if let Err(error) = ensure_session_event_sequence_constraint(conn) {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(error);
    }

    ensure_messages_schema(conn)?;
    ensure_session_runtime_outbox_schema(conn)?;
    ensure_session_mission_outbox_schema(conn)?;
    ensure_session_operation_schema(conn)?;
    ensure_session_recovery_manifest_schema(conn)?;

    let existing_session_columns = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(sessions)")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(sql_err)?;
        let mut columns = std::collections::BTreeSet::new();
        for row in rows {
            columns.insert(row.map_err(sql_err)?);
        }
        columns
    };
    if !existing_session_columns.contains("input_tokens") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("output_tokens") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("estimated_cost_usd") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN estimated_cost_usd REAL NOT NULL DEFAULT 0.0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("status") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN status TEXT NOT NULL DEFAULT 'active'",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("created_at_ms") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN created_at_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("updated_at_ms") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("input_generation") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN input_generation INTEGER NOT NULL DEFAULT 1",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("input_admission_open") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN input_admission_open INTEGER NOT NULL DEFAULT 1",
            [],
        )
        .map_err(sql_err)?;
    }
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_sessions_status_model_last_activity
            ON sessions(status COLLATE NOCASE, model COLLATE NOCASE, last_activity DESC)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_sessions_model_last_activity
            ON sessions(model COLLATE NOCASE, last_activity DESC)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_message_count ON sessions(message_count)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_sessions_owner_activity
            ON sessions(
                json_extract(metadata_json, '$.owner_principal_id'),
                last_activity DESC,
                session_id ASC
            )",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_session_activity
            ON session_runtime_outbox(
                session_id,
                updated_at_ms DESC,
                sequence DESC,
                request_id DESC
            )",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_session_domain_global_kind
            ON session_events(
                json_extract(event_json, '$.kind'),
                session_id,
                sequence
            )
            WHERE event_type='SessionDomainEvent'",
        [],
    )
    .map_err(sql_err)?;
    // The reconciliation reads and writes the token summary columns. Legacy
    // databases receive those columns above, so this must remain after every
    // sessions-table ALTER and before the schema transaction commits.
    reconcile_legacy_session_summaries(conn)?;

    conn.execute_batch("COMMIT;").map_err(sql_err)?;
    Ok(())
}

fn ensure_session_operation_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_lifecycle_intents (
            operation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            disposition TEXT NOT NULL,
            phase TEXT NOT NULL,
            last_stable_phase TEXT NOT NULL,
            expected_generation INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            last_error TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_session_lifecycle_active
            ON session_lifecycle_intents(session_id)
            WHERE phase != 'unloaded';
        CREATE INDEX IF NOT EXISTS idx_session_lifecycle_recovery
            ON session_lifecycle_intents(phase, updated_at_ms, operation_id);

        CREATE TABLE IF NOT EXISTS session_branch_activations (
            operation_id TEXT PRIMARY KEY,
            source_session_id TEXT NOT NULL,
            target_session_id TEXT NOT NULL UNIQUE,
            source_message_count INTEGER NOT NULL,
            phase TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            last_error TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (source_session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            FOREIGN KEY (target_session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_session_branch_activation_recovery
            ON session_branch_activations(phase, updated_at_ms, operation_id);
        "#,
    )
    .map_err(sql_err)
}

fn ensure_session_recovery_manifest_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_recovery_manifest (
            session_id TEXT PRIMARY KEY,
            durable_cursor INTEGER NOT NULL DEFAULT 0,
            event_cursor INTEGER NOT NULL DEFAULT 0,
            history_revision INTEGER NOT NULL DEFAULT 0,
            transcript_messages INTEGER NOT NULL DEFAULT 0,
            transcript_bytes INTEGER NOT NULL DEFAULT 0,
            latest_checkpoint_sequence INTEGER,
            latest_checkpoint_event_id TEXT,
            index_generation INTEGER NOT NULL DEFAULT 0,
            indexed_through_sequence INTEGER,
            index_card_count INTEGER NOT NULL DEFAULT 0,
            index_pending INTEGER NOT NULL DEFAULT 0,
            in_flight_turn INTEGER NOT NULL DEFAULT 0,
            pending_approval INTEGER NOT NULL DEFAULT 0,
            active_writer_or_attachment INTEGER NOT NULL DEFAULT 0,
            mission_agent_team_continuation INTEGER NOT NULL DEFAULT 0,
            last_activity_ms INTEGER NOT NULL DEFAULT 0,
            manifest_revision INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );
        "#,
    )
    .map_err(sql_err)?;
    let columns = table_columns(conn, "session_recovery_manifest")?;
    for (column, ddl) in [
        (
            "event_cursor",
            "ALTER TABLE session_recovery_manifest ADD COLUMN event_cursor INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "latest_checkpoint_sequence",
            "ALTER TABLE session_recovery_manifest ADD COLUMN latest_checkpoint_sequence INTEGER",
        ),
        (
            "latest_checkpoint_event_id",
            "ALTER TABLE session_recovery_manifest ADD COLUMN latest_checkpoint_event_id TEXT",
        ),
        (
            "index_generation",
            "ALTER TABLE session_recovery_manifest ADD COLUMN index_generation INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "indexed_through_sequence",
            "ALTER TABLE session_recovery_manifest ADD COLUMN indexed_through_sequence INTEGER",
        ),
        (
            "index_card_count",
            "ALTER TABLE session_recovery_manifest ADD COLUMN index_card_count INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "index_pending",
            "ALTER TABLE session_recovery_manifest ADD COLUMN index_pending INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        if !columns.contains(column) {
            conn.execute(ddl, []).map_err(sql_err)?;
        }
    }
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_context_index_outbox (
            session_id TEXT NOT NULL,
            source_sequence INTEGER NOT NULL,
            operation TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at_ms INTEGER NOT NULL DEFAULT 0,
            updated_at_ms INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(session_id, source_sequence, operation),
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_session_context_index_outbox_pending
            ON session_context_index_outbox(status, updated_at_ms, session_id);

        CREATE TABLE IF NOT EXISTS session_context_index_cards (
            card_id TEXT PRIMARY KEY,
            parent_card_id TEXT,
            session_id TEXT NOT NULL,
            source_start_sequence INTEGER NOT NULL,
            source_end_sequence INTEGER NOT NULL,
            source_message_count INTEGER NOT NULL,
            source_digest TEXT NOT NULL,
            summary TEXT NOT NULL,
            scope TEXT NOT NULL,
            authority TEXT NOT NULL,
            generation INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_session_context_cards_range
            ON session_context_index_cards(
                session_id, source_start_sequence, source_end_sequence, generation
            );
        CREATE INDEX IF NOT EXISTS idx_session_context_cards_parent
            ON session_context_index_cards(session_id, parent_card_id);

        CREATE INDEX IF NOT EXISTS idx_session_recovery_required
            ON session_recovery_manifest(
                in_flight_turn,
                pending_approval,
                active_writer_or_attachment,
                mission_agent_team_continuation,
                last_activity_ms DESC
            );
        INSERT INTO session_recovery_manifest (
            session_id,
            durable_cursor,
            event_cursor,
            history_revision,
            transcript_messages,
            transcript_bytes,
            latest_checkpoint_sequence,
            latest_checkpoint_event_id,
            in_flight_turn,
            active_writer_or_attachment,
            mission_agent_team_continuation,
            last_activity_ms,
            manifest_revision
        )
        SELECT
            sessions.session_id,
            COALESCE((
                SELECT MAX(sequence) + 1 FROM messages
                 WHERE messages.session_id = sessions.session_id
            ), 0),
            COALESCE((
                SELECT MAX(sequence) + 1 FROM session_events
                 WHERE session_events.session_id = sessions.session_id
            ), 0),
            COALESCE((
                SELECT COUNT(*) FROM messages
                 WHERE messages.session_id = sessions.session_id
            ), 0),
            COALESCE((
                SELECT COUNT(*) FROM messages
                 WHERE messages.session_id = sessions.session_id
            ), 0),
            COALESCE((
                SELECT SUM(
                    length(CAST(stable_message_id AS BLOB))
                    + length(CAST(session_id AS BLOB))
                    + length(CAST(role AS BLOB))
                    + length(CAST(content_json AS BLOB))
                    + length(CAST(COALESCE(token_usage_json, '') AS BLOB))
                    + length(CAST(COALESCE(tool_use_id, '') AS BLOB))
                    + length(CAST(COALESCE(tool_name, '') AS BLOB))
                )
                FROM messages WHERE messages.session_id = sessions.session_id
            ), 0),
            (
                SELECT MAX(sequence) FROM session_events
                 WHERE session_events.session_id = sessions.session_id
                   AND event_type = 'SessionDomainEvent'
                   AND json_extract(event_json, '$.kind') =
                       'memory.semantic_checkpoint.created'
            ),
            (
                SELECT json_extract(event_json, '$.event_id')
                  FROM session_events
                 WHERE session_events.session_id = sessions.session_id
                   AND event_type = 'SessionDomainEvent'
                   AND json_extract(event_json, '$.kind') =
                       'memory.semantic_checkpoint.created'
                 ORDER BY sequence DESC
                 LIMIT 1
            ),
            EXISTS(
                SELECT 1 FROM session_runtime_outbox
                 WHERE session_runtime_outbox.session_id = sessions.session_id
                   AND status IN (
                       'accepted', 'classified', 'queued', 'claimed',
                       'running', 'reclassified'
                   )
            ),
            COALESCE((
                SELECT CASE
                    WHEN json_array_length(
                        json_extract(event_json, '$.snapshot.attachments')
                    ) > 0 THEN 1 ELSE 0
                END
                  FROM session_events
                 WHERE session_events.session_id = sessions.session_id
                   AND event_type = 'session.lifecycle.v1'
                 ORDER BY sequence DESC
                 LIMIT 1
            ), 0),
            EXISTS(
                SELECT 1 FROM session_mission_outbox
                 WHERE session_mission_outbox.session_id = sessions.session_id
                   AND operation = 'start'
                   AND status IN ('pending', 'claimed', 'retry_scheduled')
            ),
            MAX(sessions.updated_at_ms, sessions.created_at_ms),
            1
        FROM sessions
        WHERE TRUE
        ON CONFLICT(session_id) DO UPDATE SET
            durable_cursor = excluded.durable_cursor,
            event_cursor = excluded.event_cursor,
            transcript_messages = excluded.transcript_messages,
            transcript_bytes = excluded.transcript_bytes,
            latest_checkpoint_sequence = excluded.latest_checkpoint_sequence,
            latest_checkpoint_event_id = excluded.latest_checkpoint_event_id,
            in_flight_turn = excluded.in_flight_turn,
            active_writer_or_attachment =
                excluded.active_writer_or_attachment,
            mission_agent_team_continuation =
                excluded.mission_agent_team_continuation,
            last_activity_ms = MAX(
                session_recovery_manifest.last_activity_ms,
                excluded.last_activity_ms
            );

        CREATE TRIGGER IF NOT EXISTS session_recovery_session_insert
        AFTER INSERT ON sessions BEGIN
            INSERT OR IGNORE INTO session_recovery_manifest(
                session_id, last_activity_ms, manifest_revision
            ) VALUES (
                NEW.session_id, MAX(NEW.created_at_ms, NEW.updated_at_ms), 1
            );
        END;
        CREATE TRIGGER IF NOT EXISTS session_recovery_session_update
        AFTER UPDATE OF status, last_activity, updated_at_ms ON sessions BEGIN
            UPDATE session_recovery_manifest
               SET last_activity_ms = MAX(last_activity_ms, NEW.updated_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;

        CREATE TRIGGER IF NOT EXISTS session_recovery_message_insert
        AFTER INSERT ON messages BEGIN
            UPDATE session_recovery_manifest
               SET durable_cursor = MAX(durable_cursor, NEW.sequence + 1),
                   history_revision = history_revision + 1,
                   transcript_messages = transcript_messages + 1,
                   transcript_bytes = transcript_bytes
                       + length(CAST(NEW.stable_message_id AS BLOB))
                       + length(CAST(NEW.session_id AS BLOB))
                       + length(CAST(NEW.role AS BLOB))
                       + length(CAST(NEW.content_json AS BLOB))
                       + length(CAST(COALESCE(NEW.token_usage_json, '') AS BLOB))
                       + length(CAST(COALESCE(NEW.tool_use_id, '') AS BLOB))
                       + length(CAST(COALESCE(NEW.tool_name, '') AS BLOB)),
                   last_activity_ms = MAX(last_activity_ms, NEW.created_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;
        CREATE TRIGGER IF NOT EXISTS session_recovery_message_delete
        AFTER DELETE ON messages BEGIN
            UPDATE session_recovery_manifest
               SET history_revision = history_revision + 1,
                   transcript_messages = MAX(0, transcript_messages - 1),
                   transcript_bytes = MAX(
                       0,
                       transcript_bytes
                           - length(CAST(OLD.stable_message_id AS BLOB))
                           - length(CAST(OLD.session_id AS BLOB))
                           - length(CAST(OLD.role AS BLOB))
                           - length(CAST(OLD.content_json AS BLOB))
                           - length(CAST(COALESCE(OLD.token_usage_json, '') AS BLOB))
                           - length(CAST(COALESCE(OLD.tool_use_id, '') AS BLOB))
                           - length(CAST(COALESCE(OLD.tool_name, '') AS BLOB))
                   ),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = OLD.session_id;
        END;
        CREATE TRIGGER IF NOT EXISTS session_recovery_message_update
        AFTER UPDATE ON messages BEGIN
            UPDATE session_recovery_manifest
               SET durable_cursor = MAX(durable_cursor, NEW.sequence + 1),
                   history_revision = history_revision + 1,
                   transcript_bytes = MAX(
                       0,
                       transcript_bytes
                           - length(CAST(OLD.stable_message_id AS BLOB))
                           - length(CAST(OLD.session_id AS BLOB))
                           - length(CAST(OLD.role AS BLOB))
                           - length(CAST(OLD.content_json AS BLOB))
                           - length(CAST(COALESCE(OLD.token_usage_json, '') AS BLOB))
                           - length(CAST(COALESCE(OLD.tool_use_id, '') AS BLOB))
                           - length(CAST(COALESCE(OLD.tool_name, '') AS BLOB))
                           + length(CAST(NEW.stable_message_id AS BLOB))
                           + length(CAST(NEW.session_id AS BLOB))
                           + length(CAST(NEW.role AS BLOB))
                           + length(CAST(NEW.content_json AS BLOB))
                           + length(CAST(COALESCE(NEW.token_usage_json, '') AS BLOB))
                           + length(CAST(COALESCE(NEW.tool_use_id, '') AS BLOB))
                           + length(CAST(COALESCE(NEW.tool_name, '') AS BLOB))
                   ),
                   last_activity_ms = MAX(last_activity_ms, NEW.created_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;

        CREATE TRIGGER IF NOT EXISTS session_recovery_lifecycle_event_insert
        AFTER INSERT ON session_events
        WHEN NEW.event_type = 'session.lifecycle.v1' BEGIN
            UPDATE session_recovery_manifest
               SET active_writer_or_attachment = CASE
                       WHEN json_array_length(
                           json_extract(NEW.event_json, '$.snapshot.attachments')
                       ) > 0 THEN 1 ELSE 0
                   END,
                   last_activity_ms = MAX(last_activity_ms, NEW.created_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;

        DROP TRIGGER IF EXISTS session_recovery_event_cursor_insert;
        CREATE TRIGGER session_recovery_event_cursor_insert
        AFTER INSERT ON session_events BEGIN
            UPDATE session_recovery_manifest
               SET event_cursor = MAX(event_cursor, NEW.sequence + 1),
                   latest_checkpoint_sequence = CASE
                       WHEN NEW.event_type = 'SessionDomainEvent'
                        AND json_extract(NEW.event_json, '$.kind') =
                            'memory.semantic_checkpoint.created'
                       THEN NEW.sequence
                       ELSE latest_checkpoint_sequence
                   END,
                   latest_checkpoint_event_id = CASE
                       WHEN NEW.event_type = 'SessionDomainEvent'
                        AND json_extract(NEW.event_json, '$.kind') =
                            'memory.semantic_checkpoint.created'
                       THEN json_extract(NEW.event_json, '$.event_id')
                       ELSE latest_checkpoint_event_id
                   END,
                   index_pending = CASE
                       WHEN NEW.event_type = 'SessionDomainEvent'
                        AND json_extract(NEW.event_json, '$.kind') =
                            'memory.semantic_checkpoint.created'
                       THEN 1
                       ELSE index_pending
                   END,
                   last_activity_ms = MAX(last_activity_ms, NEW.created_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
            INSERT INTO session_context_index_outbox(
                session_id, source_sequence, operation, status,
                created_at_ms, updated_at_ms
            )
            SELECT
                NEW.session_id, 0, 'reconcile', 'pending',
                NEW.created_at_ms, NEW.created_at_ms
            WHERE NEW.event_type = 'SessionDomainEvent'
              AND json_extract(NEW.event_json, '$.kind') =
                  'memory.semantic_checkpoint.created'
            ON CONFLICT(session_id, source_sequence, operation) DO UPDATE SET
                status='pending',
                updated_at_ms=MAX(updated_at_ms, excluded.updated_at_ms);
        END;

        CREATE TRIGGER IF NOT EXISTS session_context_index_message_insert
        AFTER INSERT ON messages BEGIN
            INSERT INTO session_context_index_outbox(
                session_id, source_sequence, operation, status,
                created_at_ms, updated_at_ms
            ) VALUES (
                NEW.session_id, 0, 'reconcile', 'pending',
                NEW.created_at_ms, NEW.created_at_ms
            )
            ON CONFLICT(session_id, source_sequence, operation) DO UPDATE SET
                status='pending',
                updated_at_ms=MAX(updated_at_ms, excluded.updated_at_ms);
            UPDATE session_recovery_manifest
               SET index_pending=1,
                   manifest_revision=manifest_revision + 1
             WHERE session_id=NEW.session_id;
        END;
        CREATE TRIGGER IF NOT EXISTS session_context_index_message_update
        AFTER UPDATE ON messages BEGIN
            INSERT INTO session_context_index_outbox(
                session_id, source_sequence, operation, status,
                created_at_ms, updated_at_ms
            ) VALUES (
                NEW.session_id, 0, 'reconcile', 'pending',
                NEW.created_at_ms, NEW.created_at_ms
            )
            ON CONFLICT(session_id, source_sequence, operation) DO UPDATE SET
                status='pending',
                updated_at_ms=MAX(updated_at_ms, excluded.updated_at_ms);
            UPDATE session_recovery_manifest
               SET index_pending=1,
                   manifest_revision=manifest_revision + 1
             WHERE session_id=NEW.session_id;
        END;
        CREATE TRIGGER IF NOT EXISTS session_context_index_message_delete
        AFTER DELETE ON messages BEGIN
            INSERT INTO session_context_index_outbox(
                session_id, source_sequence, operation, status,
                created_at_ms, updated_at_ms
            ) VALUES (
                OLD.session_id, 0, 'reconcile', 'pending',
                OLD.created_at_ms, OLD.created_at_ms
            )
            ON CONFLICT(session_id, source_sequence, operation) DO UPDATE SET
                status='pending',
                updated_at_ms=MAX(updated_at_ms, excluded.updated_at_ms);
            UPDATE session_recovery_manifest
               SET index_pending=1,
                   manifest_revision=manifest_revision + 1
             WHERE session_id=OLD.session_id;
        END;

        CREATE TRIGGER IF NOT EXISTS session_recovery_runtime_outbox_insert
        AFTER INSERT ON session_runtime_outbox BEGIN
            UPDATE session_recovery_manifest
               SET in_flight_turn = EXISTS(
                       SELECT 1 FROM session_runtime_outbox
                        WHERE session_id = NEW.session_id
                          AND status IN (
                              'accepted', 'classified', 'queued', 'claimed',
                              'running', 'reclassified'
                          )
                   ),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;
        CREATE TRIGGER IF NOT EXISTS session_recovery_runtime_outbox_update
        AFTER UPDATE OF status ON session_runtime_outbox BEGIN
            UPDATE session_recovery_manifest
               SET in_flight_turn = EXISTS(
                       SELECT 1 FROM session_runtime_outbox
                        WHERE session_id = NEW.session_id
                          AND status IN (
                              'accepted', 'classified', 'queued', 'claimed',
                              'running', 'reclassified'
                          )
                   ),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;

        CREATE TRIGGER IF NOT EXISTS session_recovery_mission_outbox_insert
        AFTER INSERT ON session_mission_outbox BEGIN
            UPDATE session_recovery_manifest
               SET mission_agent_team_continuation = EXISTS(
                       SELECT 1 FROM session_mission_outbox
                        WHERE session_id = NEW.session_id
                          AND operation = 'start'
                          AND status IN ('pending', 'claimed', 'retry_scheduled')
                   ),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;
        CREATE TRIGGER IF NOT EXISTS session_recovery_mission_outbox_update
        AFTER UPDATE OF status ON session_mission_outbox BEGIN
            UPDATE session_recovery_manifest
               SET mission_agent_team_continuation = EXISTS(
                       SELECT 1 FROM session_mission_outbox
                        WHERE session_id = NEW.session_id
                          AND operation = 'start'
                          AND status IN ('pending', 'claimed', 'retry_scheduled')
                   ),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;
        "#,
    )
    .map_err(sql_err)
}

fn reconcile_legacy_session_summaries(conn: &Connection) -> Result<()> {
    const MIGRATION_ID: &str = "session.0003.reconcile-message-summaries";
    conn.execute(
        "CREATE TABLE IF NOT EXISTS session_store_migrations (
            migration_id TEXT PRIMARY KEY,
            applied_at_ms INTEGER NOT NULL
        )",
        [],
    )
    .map_err(sql_err)?;
    let applied = conn
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM session_store_migrations WHERE migration_id=?1
            )",
            params![MIGRATION_ID],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sql_err)?;
    if applied {
        return Ok(());
    }
    // Legacy databases can already contain `sessions` rows before the
    // external-content FTS table and its UPDATE trigger are installed.
    // Rebuild first; otherwise the trigger's delete command targets a missing
    // FTS row and SQLite reports the database image as malformed.
    conn.execute(
        "INSERT INTO sessions_fts(sessions_fts) VALUES('rebuild')",
        [],
    )
    .map_err(sql_err)?;
    conn.execute_batch(
        r"UPDATE sessions
              SET message_count = (
                      SELECT COUNT(*) FROM messages WHERE session_id=sessions.session_id
                  ),
                  input_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.input_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.input_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.input_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=sessions.session_id
                  ), 0),
                  output_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.output_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.output_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.output_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=sessions.session_id
                  ), 0)",
    )
    .map_err(sql_err)?;
    conn.execute(
        "INSERT INTO session_store_migrations(migration_id, applied_at_ms)
         VALUES (?1, ?2)",
        params![MIGRATION_ID, Utc::now().timestamp_millis().max(0)],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn ensure_session_event_sequence_constraint(conn: &Connection) -> Result<()> {
    let duplicate_groups: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (\
                 SELECT session_id, sequence FROM session_events \
                 GROUP BY session_id, sequence HAVING COUNT(*) > 1\
             )",
            [],
            |row| row.get(0),
        )
        .map_err(sql_err)?;
    if duplicate_groups > 0 {
        // Legacy writers could race while allocating sequence numbers. Keep
        // every event and derive one stable session-local order from the
        // existing sequence, timestamp, and row id; then keep JSON payload
        // sequence metadata consistent with the durable column. This runs in
        // the schema transaction before the unique index is created.
        conn.execute_batch(
            r"
            WITH resequenced AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY session_id
                        ORDER BY sequence ASC, created_at_ms ASC, id ASC
                    ) - 1 AS next_sequence
                  FROM session_events
            )
            UPDATE session_events
               SET sequence = (
                       SELECT next_sequence
                         FROM resequenced
                        WHERE resequenced.id = session_events.id
                   ),
                   event_json = CASE
                       WHEN json_valid(event_json) THEN json_set(
                           event_json,
                           '$.sequence',
                           (
                               SELECT next_sequence
                                 FROM resequenced
                                WHERE resequenced.id = session_events.id
                           )
                       )
                       ELSE event_json
                   END
             WHERE id IN (SELECT id FROM resequenced);
            ",
        )
        .map_err(sql_err)?;
        tracing::warn!(
            duplicate_groups,
            "repaired legacy duplicate session event sequences before enforcing uniqueness"
        );
    }
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_session_events_session_sequence \
         ON session_events(session_id, sequence)",
    )
    .map_err(sql_err)?;
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<std::collections::BTreeSet<String>> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(sql_err)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sql_err)?;
    let mut columns = std::collections::BTreeSet::new();
    for row in rows {
        columns.insert(row.map_err(sql_err)?);
    }
    Ok(columns)
}

fn ensure_messages_schema(conn: &Connection) -> Result<()> {
    let existing_message_columns = table_columns(conn, "messages")?;
    if !existing_message_columns.contains("stable_message_id") {
        conn.execute("ALTER TABLE messages ADD COLUMN stable_message_id TEXT", [])
            .map_err(sql_err)?;
        conn.execute_batch(
            r#"
            UPDATE messages
               SET stable_message_id = 'legacy:' || lower(hex(CAST(session_id AS BLOB))) || ':' || sequence
             WHERE stable_message_id IS NULL OR stable_message_id = '';
            "#,
        )
        .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("content_json") {
        conn.execute(
            r#"ALTER TABLE messages ADD COLUMN content_json TEXT NOT NULL DEFAULT '[{"type":"text","text":""}]'"#,
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("blocks_count") {
        conn.execute(
            "ALTER TABLE messages ADD COLUMN blocks_count INTEGER NOT NULL DEFAULT 1",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("tool_use_id") {
        conn.execute("ALTER TABLE messages ADD COLUMN tool_use_id TEXT", [])
            .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("tool_name") {
        conn.execute("ALTER TABLE messages ADD COLUMN tool_name TEXT", [])
            .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("token_usage_json") {
        conn.execute("ALTER TABLE messages ADD COLUMN token_usage_json TEXT", [])
            .map_err(sql_err)?;
    }

    if table_columns(conn, "message_blocks").is_ok_and(|columns| columns.contains("block_type")) {
        conn.execute_batch(
            r#"
            UPDATE messages
               SET content_json = COALESCE(
                       (
                         SELECT json_group_array(json(block_json))
                           FROM (
                             SELECT json_object(
                                      'type',
                                      CASE
                                        WHEN block_type = 'tool_use' THEN 'tool_use'
                                        WHEN block_type = 'tool_result' THEN 'tool_result'
                                        WHEN block_type = 'thinking' THEN 'thinking'
                                        ELSE 'text'
                                      END,
                                      'text', COALESCE(text, tool_output, ''),
                                      'id', COALESCE(tool_id, ''),
                                      'name', COALESCE(tool_name, ''),
                                      'input', COALESCE(tool_input, ''),
                                      'content', COALESCE(tool_output, ''),
                                      'is_error', CASE WHEN is_error = 0 THEN json('false') ELSE json('true') END
                                    ) AS block_json
                               FROM message_blocks
                              WHERE message_blocks.message_id = messages.id
                              ORDER BY block_order ASC
                           )
                       ),
                       content_json
                   ),
                   blocks_count = COALESCE(
                       (
                         SELECT COUNT(*)
                           FROM message_blocks
                          WHERE message_blocks.message_id = messages.id
                       ),
                       blocks_count
                   ),
                   tool_use_id = COALESCE(
                       (
                         SELECT tool_id
                           FROM message_blocks
                          WHERE message_blocks.message_id = messages.id
                            AND tool_id IS NOT NULL
                          ORDER BY block_order ASC
                          LIMIT 1
                       ),
                       tool_use_id
                   ),
                   tool_name = COALESCE(
                       (
                         SELECT tool_name
                           FROM message_blocks
                          WHERE message_blocks.message_id = messages.id
                            AND tool_name IS NOT NULL
                          ORDER BY block_order ASC
                          LIMIT 1
                       ),
                       tool_name
                   )
             WHERE EXISTS (
                       SELECT 1
                         FROM message_blocks
                        WHERE message_blocks.message_id = messages.id
                   );
            "#,
        )
        .map_err(sql_err)?;
    }

    conn.execute_batch(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_stable_message_id
            ON messages(stable_message_id);
        CREATE TRIGGER IF NOT EXISTS messages_stable_id_required_insert
        BEFORE INSERT ON messages
        WHEN NEW.stable_message_id IS NULL OR NEW.stable_message_id = ''
        BEGIN
            SELECT RAISE(ABORT, 'messages.stable_message_id is required');
        END;
        CREATE TRIGGER IF NOT EXISTS messages_stable_id_required_update
        BEFORE UPDATE OF stable_message_id ON messages
        WHEN NEW.stable_message_id IS NULL OR NEW.stable_message_id = ''
        BEGIN
            SELECT RAISE(ABORT, 'messages.stable_message_id is required');
        END;
        DROP TRIGGER IF EXISTS messages_fts_ai;
        DROP TRIGGER IF EXISTS messages_fts_ad;
        DROP TRIGGER IF EXISTS messages_fts_au;
        DROP TABLE IF EXISTS messages_fts;
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            session_id UNINDEXED,
            role,
            content_text,
            tool_name,
            content=messages,
            content_rowid=id
        );
        CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id, new.role,
                    (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
                    new.tool_name);
        END;
        CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.id, old.session_id, old.role, NULL, old.tool_name);
        END;
        CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.id, old.session_id, old.role, NULL, old.tool_name);
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id, new.role,
                    (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
                    new.tool_name);
        END;
        INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
        SELECT id,
               session_id,
               role,
               (SELECT group_concat(json_extract(value,'$.text'),' ')
                  FROM json_each(messages.content_json)
                 WHERE json_extract(value,'$.type')='text'),
               tool_name
          FROM messages;
        "#,
    )
    .map_err(sql_err)?;

    Ok(())
}

fn ensure_session_runtime_outbox_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_runtime_outbox (
            input_id TEXT NOT NULL UNIQUE,
            request_id TEXT PRIMARY KEY,
            turn_id TEXT NOT NULL UNIQUE,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            session_generation INTEGER NOT NULL DEFAULT 1,
            decision TEXT NOT NULL DEFAULT 'start_new_turn',
            target_turn_id TEXT,
            classification_json TEXT,
            status TEXT NOT NULL,
            runtime_commit_cursor INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at_ms INTEGER NOT NULL,
            claim_owner TEXT,
            claim_token TEXT,
            claim_fence_epoch INTEGER,
            claim_expires_at_ms INTEGER,
            failure_class TEXT,
            last_error TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            terminal_at_ms INTEGER,
            runtime_options_json TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            FOREIGN KEY (message_id) REFERENCES messages(stable_message_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_claim
            ON session_runtime_outbox(
                status, next_attempt_at_ms, claim_expires_at_ms, sequence
            );
        CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_session
            ON session_runtime_outbox(session_id, sequence);
        CREATE TABLE IF NOT EXISTS session_runtime_outbox_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            action TEXT NOT NULL,
            actor TEXT,
            reason TEXT,
            from_status TEXT NOT NULL,
            to_status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (request_id) REFERENCES session_runtime_outbox(request_id) ON DELETE CASCADE
        );
        "#,
    )
    .map_err(sql_err)?;
    let columns = table_columns(conn, "session_runtime_outbox")?;
    if !columns.contains("runtime_options_json") {
        conn.execute(
            "ALTER TABLE session_runtime_outbox ADD COLUMN runtime_options_json TEXT",
            [],
        )
        .map_err(sql_err)?;
    }
    let additions = [
        (
            "input_id",
            "ALTER TABLE session_runtime_outbox ADD COLUMN input_id TEXT",
        ),
        (
            "session_generation",
            "ALTER TABLE session_runtime_outbox ADD COLUMN session_generation INTEGER NOT NULL DEFAULT 1",
        ),
        (
            "decision",
            "ALTER TABLE session_runtime_outbox ADD COLUMN decision TEXT NOT NULL DEFAULT 'start_new_turn'",
        ),
        (
            "target_turn_id",
            "ALTER TABLE session_runtime_outbox ADD COLUMN target_turn_id TEXT",
        ),
        (
            "classification_json",
            "ALTER TABLE session_runtime_outbox ADD COLUMN classification_json TEXT",
        ),
        (
            "claim_token",
            "ALTER TABLE session_runtime_outbox ADD COLUMN claim_token TEXT",
        ),
        (
            "claim_fence_epoch",
            "ALTER TABLE session_runtime_outbox ADD COLUMN claim_fence_epoch INTEGER",
        ),
        (
            "terminal_at_ms",
            "ALTER TABLE session_runtime_outbox ADD COLUMN terminal_at_ms INTEGER",
        ),
    ];
    for (column, ddl) in additions {
        if !columns.contains(column) {
            conn.execute(ddl, []).map_err(sql_err)?;
        }
    }
    // Canonicalise legacy outbox states in place. Readers also accept the old
    // spellings so an interrupted migration remains recoverable.
    conn.execute_batch(
        r"
        UPDATE session_runtime_outbox SET status = 'queued'
         WHERE status IN ('pending', 'retry_scheduled');
        UPDATE session_runtime_outbox SET input_id = request_id
         WHERE input_id IS NULL OR trim(input_id) = '';
        UPDATE session_runtime_outbox SET status = 'completed',
               terminal_at_ms = COALESCE(terminal_at_ms, updated_at_ms)
         WHERE status = 'materialized';
        UPDATE session_runtime_outbox SET status = 'blocked'
         WHERE status = 'blocked_materialization';
        UPDATE session_runtime_outbox
           SET claim_token = COALESCE(
               claim_token,
               CASE WHEN status IN ('claimed', 'running')
                    THEN 'legacy:' || request_id || ':' || revision
                    ELSE NULL END
           );
        UPDATE session_runtime_outbox
           SET claim_fence_epoch = revision
         WHERE claim_fence_epoch IS NULL
           AND claim_token IS NOT NULL
           AND status IN ('claimed', 'running');
        DROP INDEX IF EXISTS idx_session_runtime_outbox_claim;
        CREATE INDEX idx_session_runtime_outbox_claim
            ON session_runtime_outbox(
                session_id, session_generation, status, sequence,
                next_attempt_at_ms, claim_expires_at_ms
            );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_session_runtime_outbox_input_id
            ON session_runtime_outbox(input_id);
        ",
    )
    .map_err(sql_err)?;
    Ok(())
}

fn ensure_session_mission_outbox_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_mission_outbox (
            request_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            title TEXT NOT NULL,
            workspace_key TEXT NOT NULL,
            operation TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at_ms INTEGER NOT NULL,
            claim_owner TEXT,
            claim_expires_at_ms INTEGER,
            failure_class TEXT,
            last_error TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_session_mission_outbox_claim
            ON session_mission_outbox(workspace_key, status, next_attempt_at_ms, claim_expires_at_ms, created_at_ms);
        CREATE INDEX IF NOT EXISTS idx_session_mission_outbox_session
            ON session_mission_outbox(workspace_key, session_id, created_at_ms);
        CREATE TABLE IF NOT EXISTS session_mission_outbox_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            action TEXT NOT NULL,
            actor TEXT,
            reason TEXT,
            from_status TEXT NOT NULL,
            to_status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (request_id) REFERENCES session_mission_outbox(request_id) ON DELETE CASCADE
        );
        "#,
    )
    .map_err(sql_err)
}

// ---------------------------------------------------------------------------
// SessionRecord
// ---------------------------------------------------------------------------

/// A serialisable snapshot of a single session's metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    /// Unique session identifier (UUID string).
    pub session_id: String,
    /// Platform name (e.g. `"telegram"`, `"api_server"`).
    pub platform: String,
    /// Platform-native chat / room identifier.
    pub chat_id: String,
    /// Optional platform-native user identifier.
    pub user_id: Option<String>,
    /// Active model name for this session (if any).
    pub model: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// ISO 8601 timestamp of the last received message.
    pub last_activity: String,
    /// Number of messages processed in this session.
    pub message_count: i64,
    /// Name of the [`SessionResetPolicy`] variant (stored as text).
    pub reset_policy: String,
    /// Optional JSON blob for arbitrary extra metadata.
    pub metadata_json: Option<String>,
    /// Cumulative input tokens (prompt).
    pub input_tokens: i64,
    /// Cumulative output tokens (completion).
    pub output_tokens: i64,
    /// Estimated total cost in USD.
    pub estimated_cost_usd: f64,
    /// Lifecycle status (`active`, `closed`, etc.).
    pub status: String,
}

// ---------------------------------------------------------------------------
// Row → SessionRecord mapper
// ---------------------------------------------------------------------------

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        session_id: row.get(0)?,
        platform: row.get(1)?,
        chat_id: row.get(2)?,
        user_id: row.get(3)?,
        model: row.get(4)?,
        created_at: row.get(5)?,
        last_activity: row.get(6)?,
        message_count: row.get(7)?,
        reset_policy: row.get(8)?,
        metadata_json: row.get(9)?,
        input_tokens: row.get(10)?,
        output_tokens: row.get(11)?,
        estimated_cost_usd: row.get(12)?,
        status: row.get(13)?,
    })
}

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionMessage> {
    Ok(SessionMessage {
        stable_message_id: row.get(0)?,
        session_id: row.get(1)?,
        sequence: row.get::<_, i64>(2)? as usize,
        role: row.get(3)?,
        content_json: row.get(4)?,
        blocks_count: row.get::<_, i64>(5)? as usize,
        tool_use_id: row.get(6)?,
        tool_name: row.get(7)?,
        token_usage_json: row.get(8)?,
        created_at_ms: row.get::<_, i64>(9)? as u64,
    })
}

fn row_to_recovery_manifest(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecoveryManifest> {
    Ok(SessionRecoveryManifest {
        session_id: row.get(0)?,
        durable_cursor: row.get::<_, i64>(1)?.max(0) as u64,
        event_cursor: row.get::<_, i64>(2)?.max(0) as u64,
        history_revision: row.get::<_, i64>(3)?.max(0) as u64,
        transcript_messages: row.get::<_, i64>(4)?.max(0) as u64,
        transcript_bytes: row.get::<_, i64>(5)?.max(0) as u64,
        latest_checkpoint_sequence: row
            .get::<_, Option<i64>>(6)?
            .map(|value| value.max(0) as u64),
        latest_checkpoint_event_id: row.get(7)?,
        index_generation: row.get::<_, i64>(8)?.max(0) as u64,
        indexed_through_sequence: row
            .get::<_, Option<i64>>(9)?
            .map(|value| value.max(0) as u64),
        index_card_count: row.get::<_, i64>(10)?.max(0) as u64,
        index_pending: row.get(11)?,
        in_flight_turn: row.get(12)?,
        pending_approval: row.get(13)?,
        active_writer_or_attachment: row.get(14)?,
        mission_agent_team_continuation: row.get(15)?,
        last_activity_ms: row.get::<_, i64>(16)?.max(0) as u64,
        manifest_revision: row.get::<_, i64>(17)?.max(0) as u64,
    })
}

fn row_to_message_metadata(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionMessageMetadata> {
    Ok(SessionMessageMetadata {
        stable_message_id: row.get(0)?,
        session_id: row.get(1)?,
        sequence: row.get::<_, i64>(2)?.max(0) as usize,
        role: row.get(3)?,
        blocks_count: row.get::<_, i64>(4)?.max(0) as usize,
        tool_use_id: row.get(5)?,
        tool_name: row.get(6)?,
        created_at_ms: row.get::<_, i64>(7)?.max(0) as u64,
        content_bytes: row.get::<_, i64>(8)?.max(0) as usize,
    })
}

fn row_to_context_index_card(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContextIndexCard> {
    Ok(ContextIndexCard {
        schema_version: CONTEXT_INDEX_CARD_SCHEMA_VERSION,
        card_id: row.get(0)?,
        parent_card_id: row.get(1)?,
        session_id: row.get(2)?,
        source_start_sequence: row.get::<_, i64>(3)?.max(0) as usize,
        source_end_sequence: row.get::<_, i64>(4)?.max(0) as usize,
        source_message_count: row.get::<_, i64>(5)?.max(0) as usize,
        source_digest: row.get(6)?,
        summary: row.get(7)?,
        scope: row.get(8)?,
        authority: row.get(9)?,
        generation: row.get::<_, i64>(10)?.max(0) as u64,
        created_at_ms: row.get::<_, i64>(11)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(12)?.max(0) as u64,
    })
}

fn sqlite_decode_error(error: SessionError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn row_to_lifecycle_intent(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionLifecycleIntent> {
    Ok(SessionLifecycleIntent {
        operation_id: row.get(0)?,
        session_id: row.get(1)?,
        disposition: crate::SessionCloseDisposition::parse(&row.get::<_, String>(2)?)
            .map_err(sqlite_decode_error)?,
        phase: SessionLifecyclePhase::parse(&row.get::<_, String>(3)?)
            .map_err(sqlite_decode_error)?,
        last_stable_phase: SessionLifecyclePhase::parse(&row.get::<_, String>(4)?)
            .map_err(sqlite_decode_error)?,
        expected_generation: row.get::<_, i64>(5)?.max(0) as u64,
        created_at_ms: row.get::<_, i64>(6)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(7)?.max(0) as u64,
        last_error: row.get(8)?,
        revision: row.get::<_, i64>(9)?.max(0) as u64,
    })
}

fn query_lifecycle_intent(
    conn: &Connection,
    operation_id: &str,
) -> Result<Option<SessionLifecycleIntent>> {
    conn.query_row(
        r"SELECT operation_id, session_id, disposition, phase, last_stable_phase,
                  expected_generation, created_at_ms, updated_at_ms, last_error, revision
             FROM session_lifecycle_intents WHERE operation_id=?1",
        params![operation_id],
        row_to_lifecycle_intent,
    )
    .optional()
    .map_err(sql_err)
}

fn row_to_branch_activation(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionBranchActivation> {
    Ok(SessionBranchActivation {
        operation_id: row.get(0)?,
        source_session_id: row.get(1)?,
        target_session_id: row.get(2)?,
        source_message_count: row.get::<_, i64>(3)?.max(0) as usize,
        phase: SessionBranchActivationPhase::parse(&row.get::<_, String>(4)?)
            .map_err(sqlite_decode_error)?,
        created_at_ms: row.get::<_, i64>(5)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(6)?.max(0) as u64,
        last_error: row.get(7)?,
        revision: row.get::<_, i64>(8)?.max(0) as u64,
    })
}

fn query_branch_activation(
    conn: &Connection,
    operation_id: &str,
) -> Result<Option<SessionBranchActivation>> {
    conn.query_row(
        r"SELECT operation_id, source_session_id, target_session_id,
                  source_message_count, phase, created_at_ms, updated_at_ms,
                  last_error, revision
             FROM session_branch_activations WHERE operation_id=?1",
        params![operation_id],
        row_to_branch_activation,
    )
    .optional()
    .map_err(sql_err)
}

fn append_allocated_event_tx(
    tx: &rusqlite::Transaction<'_>,
    event: &SessionEvent,
) -> Result<SessionEvent> {
    let sequence = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1
               FROM session_events WHERE session_id=?1",
            params![event.session_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_err)?;
    let sequence = usize::try_from(sequence)
        .map_err(|_| SessionError::Store("Session event sequence overflow".to_string()))?;
    let event_json = event_json_with_allocated_sequence(event, sequence)?;
    tx.execute(
        r"INSERT INTO session_events
            (session_id, event_type, event_json, sequence, created_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event.session_id,
            event.event_type,
            event_json,
            sequence as i64,
            event.created_at_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    let mut stored = event.clone();
    stored.sequence = sequence;
    stored.event_json = event_json_with_allocated_sequence(event, sequence)?;
    Ok(stored)
}

fn transition_lifecycle_intent_tx(
    tx: &rusqlite::Transaction<'_>,
    transition: &SessionLifecycleTransition,
) -> Result<SessionLifecycleIntent> {
    let current = query_lifecycle_intent(tx, &transition.operation_id)?.ok_or_else(|| {
        SessionError::Store(format!(
            "Session lifecycle intent `{}` does not exist",
            transition.operation_id
        ))
    })?;
    transition.validate(&current)?;
    let last_stable_phase = if transition.next_phase == SessionLifecyclePhase::Failed {
        current.last_stable_phase
    } else {
        transition.next_phase
    };
    let changed = tx
        .execute(
            r"UPDATE session_lifecycle_intents
                 SET phase=?1, last_stable_phase=?2, updated_at_ms=?3,
                     last_error=?4, revision=revision+1
               WHERE operation_id=?5 AND phase=?6 AND revision=?7",
            params![
                transition.next_phase.as_str(),
                last_stable_phase.as_str(),
                transition.updated_at_ms as i64,
                transition.error,
                transition.operation_id,
                transition.expected_phase.as_str(),
                transition.expected_revision as i64,
            ],
        )
        .map_err(sql_err)?;
    if changed != 1 {
        return Err(SessionError::Store(format!(
            "Session lifecycle intent `{}` changed during transition",
            transition.operation_id
        )));
    }
    query_lifecycle_intent(tx, &transition.operation_id)?.ok_or_else(|| {
        SessionError::Store(format!(
            "Session lifecycle intent `{}` disappeared after transition",
            transition.operation_id
        ))
    })
}

fn row_to_outbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRuntimeOutboxRecord> {
    Ok(SessionRuntimeOutboxRecord {
        input_id: row.get(0)?,
        request_id: row.get(1)?,
        turn_id: row.get(2)?,
        message_id: row.get(3)?,
        session_id: row.get(4)?,
        sequence: row.get::<_, i64>(5)? as usize,
        session_generation: row.get::<_, i64>(6)?.max(0) as u64,
        decision: parse_input_decision(&row.get::<_, String>(7)?)?,
        target_turn_id: row.get(8)?,
        classification_json: row.get(9)?,
        status: SessionRuntimeInputStatus::parse(&row.get::<_, String>(10)?)?,
        runtime_commit_cursor: row.get::<_, Option<i64>>(11)?.map(|value| value as u64),
        attempts: row.get::<_, i64>(12)? as u32,
        next_attempt_at_ms: row.get::<_, i64>(13)? as u64,
        claim_owner: row.get(14)?,
        claim_token: row.get(15)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(16)?.map(|value| value as u64),
        failure_class: row
            .get::<_, Option<String>>(17)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(18)?,
        revision: row.get::<_, i64>(19)? as u64,
        created_at_ms: row.get::<_, i64>(20)? as u64,
        updated_at_ms: row.get::<_, i64>(21)? as u64,
        terminal_at_ms: row.get::<_, Option<i64>>(22)?.map(|value| value as u64),
        runtime_options_json: row.get(23)?,
        claim_fence_epoch: row.get::<_, Option<i64>>(24)?.map(|value| value as u64),
    })
}

fn query_outbox(conn: &Connection, request_id: &str) -> Result<Option<SessionRuntimeOutboxRecord>> {
    conn.query_row(
        r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                  session_generation, decision, target_turn_id, classification_json,
                  status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                  claim_owner, claim_token, claim_expires_at_ms, failure_class,
                  last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                  runtime_options_json, claim_fence_epoch
             FROM session_runtime_outbox WHERE request_id = ?1",
        params![request_id],
        row_to_outbox,
    )
    .optional()
    .map_err(sql_err)
}

fn query_outbox_by_input_id(
    conn: &Connection,
    input_id: &str,
) -> Result<Option<SessionRuntimeOutboxRecord>> {
    conn.query_row(
        r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                  session_generation, decision, target_turn_id, classification_json,
                  status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                  claim_owner, claim_token, claim_expires_at_ms, failure_class,
                  last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                  runtime_options_json, claim_fence_epoch
             FROM session_runtime_outbox WHERE input_id = ?1",
        params![input_id],
        row_to_outbox,
    )
    .optional()
    .map_err(sql_err)
}

fn request_from_outbox(record: &SessionRuntimeOutboxRecord) -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: record.input_id.clone(),
        request_id: record.request_id.clone(),
        turn_id: record.turn_id.clone(),
        message_id: record.message_id.clone(),
        session_generation: record.session_generation,
        decision: record.decision,
        target_turn_id: record.target_turn_id.clone(),
        classification_json: record.classification_json.clone(),
        created_at_ms: record.created_at_ms,
        runtime_options_json: record.runtime_options_json.clone(),
    }
}

fn refresh_session_message_summary_tx(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
    activity_ms: u64,
) -> Result<()> {
    let activity_ms = i64::try_from(activity_ms).unwrap_or(i64::MAX);
    let activity = DateTime::<Utc>::from_timestamp_millis(activity_ms)
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    tx.execute(
        r"UPDATE sessions
              SET message_count = (
                      SELECT COUNT(*) FROM messages WHERE session_id = ?1
                  ),
                  last_activity = CASE
                      WHEN updated_at_ms <= ?3 THEN ?2
                      ELSE last_activity
                  END,
                  updated_at_ms = MAX(updated_at_ms, ?3)
            WHERE session_id = ?1",
        params![session_id, activity, activity_ms],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn refresh_session_usage_summary_tx(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
) -> Result<()> {
    tx.execute(
        r"UPDATE sessions
              SET input_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.input_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.input_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.input_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=?1
                  ), 0),
                  output_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.output_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.output_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.output_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=?1
                  ), 0)
            WHERE session_id=?1",
        params![session_id],
    )
    .map_err(sql_err)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_outbox_history(
    tx: &rusqlite::Transaction<'_>,
    record: &SessionRuntimeOutboxRecord,
    action: &str,
    actor: Option<&str>,
    reason: Option<&str>,
    from_status: &str,
    to_status: &str,
    now_ms: u64,
) -> Result<()> {
    tx.execute(
        r"INSERT INTO session_runtime_outbox_history
            (request_id, action, actor, reason, from_status, to_status, attempts, created_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            record.request_id,
            action,
            actor,
            reason,
            from_status,
            to_status,
            record.attempts as i64,
            now_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn validate_outbox_identity(
    message: &SessionMessage,
    request: &SessionRuntimeOutboxRequest,
) -> Result<()> {
    if message.session_id.trim().is_empty()
        || request.request_id.trim().is_empty()
        || request.turn_id.trim().is_empty()
        || request.message_id.trim().is_empty()
    {
        return Err(SessionError::Store(
            "session/runtime outbox identities must be non-empty".to_string(),
        ));
    }
    if request.message_id != message.stable_message_id {
        return Err(SessionError::Store(
            "runtime outbox message_id must equal the durable message identity".to_string(),
        ));
    }
    validate_runtime_input_request(request)?;
    Ok(())
}

fn validate_runtime_input_request(request: &SessionRuntimeOutboxRequest) -> Result<()> {
    if request.input_id.trim().is_empty()
        || request.request_id.trim().is_empty()
        || request.turn_id.trim().is_empty()
        || request.message_id.trim().is_empty()
        || request.session_generation == 0
    {
        return Err(SessionError::Store(
            "durable session input requires non-empty identities and a positive generation"
                .to_string(),
        ));
    }
    if decision_requires_target_turn(request.decision)
        && request.target_turn_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(SessionError::Store(format!(
            "decision `{}` requires target_turn_id",
            input_decision_as_str(request.decision)
        )));
    }
    Ok(())
}

fn query_input_admission(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<SessionInputAdmission>> {
    conn.query_row(
        "SELECT session_id, input_generation, input_admission_open
           FROM sessions WHERE session_id = ?1",
        params![session_id],
        |row| {
            Ok(SessionInputAdmission {
                session_id: row.get(0)?,
                generation: row.get::<_, i64>(1)?.max(0) as u64,
                open: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(sql_err)
}

fn require_input_admission(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
    generation: u64,
) -> Result<()> {
    let admission = query_input_admission(tx, session_id)?
        .ok_or_else(|| SessionError::Store(format!("session `{session_id}` not found")))?;
    if !admission.open {
        return Err(SessionError::Store(format!(
            "session `{session_id}` input admission is closed"
        )));
    }
    if admission.generation != generation {
        return Err(SessionError::Store(format!(
            "session `{session_id}` generation mismatch: expected {}, current {}",
            generation, admission.generation
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_input_timeline_event(
    tx: &rusqlite::Transaction<'_>,
    request: &SessionRuntimeOutboxRequest,
    session_id: &str,
    input_sequence: usize,
    kind: &str,
    status: SessionRuntimeInputStatus,
    actor: Option<&str>,
    reason: Option<&str>,
    created_at_ms: u64,
) -> Result<()> {
    let sequence = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1
               FROM session_events WHERE session_id = ?1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_err)? as usize;
    let mut refs = vec![
        SessionDomainRef {
            ref_type: "session_input".to_string(),
            id: request.input_id.clone(),
            label: None,
        },
        SessionDomainRef {
            ref_type: "message".to_string(),
            id: request.message_id.clone(),
            label: None,
        },
        SessionDomainRef {
            ref_type: "turn".to_string(),
            id: request.turn_id.clone(),
            label: None,
        },
    ];
    if let Some(target_turn_id) = request.target_turn_id.as_ref() {
        refs.push(SessionDomainRef {
            ref_type: "target_turn".to_string(),
            id: target_turn_id.clone(),
            label: None,
        });
    }
    let mut event = SessionDomainEvent::new(
        session_id,
        sequence,
        SessionDomainScope::Message,
        kind,
        serde_json::json!({
            "input_id": request.input_id,
            "request_id": request.request_id,
            "message_id": request.message_id,
            "turn_id": request.turn_id,
            "input_sequence": input_sequence,
            "session_generation": request.session_generation,
            "decision": input_decision_as_str(request.decision),
            "target_turn_id": request.target_turn_id,
            "classification": request.classification_json,
            "actor": actor,
            "reason": reason,
        }),
        created_at_ms,
    );
    event.event_id = format!(
        "session-input:{}:{}:{}",
        request.request_id, request.session_generation, kind
    );
    event.correlation_id = Some(request.request_id.clone());
    event.status = Some(status.as_str().to_string());
    event.refs = refs;
    let stored = event.to_session_event().map_err(|error| {
        SessionError::Store(format!("session input event encode failed: {error}"))
    })?;
    tx.execute(
        "INSERT INTO session_events
            (session_id, event_type, event_json, sequence, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            stored.session_id,
            stored.event_type,
            stored.event_json,
            stored.sequence as i64,
            stored.created_at_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn append_admission_timeline_event(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
    previous_generation: u64,
    admission: &SessionInputAdmission,
    actor: &str,
    reason: &str,
    created_at_ms: u64,
) -> Result<()> {
    let sequence = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1
               FROM session_events WHERE session_id = ?1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_err)? as usize;
    let mut event = SessionDomainEvent::new(
        session_id,
        sequence,
        SessionDomainScope::Session,
        if admission.open {
            "session.input.generation.advanced.v1"
        } else {
            "session.input.admission.closed.v1"
        },
        serde_json::json!({
            "previous_generation": previous_generation,
            "generation": admission.generation,
            "admission_open": admission.open,
            "actor": actor,
            "reason": reason,
        }),
        created_at_ms,
    );
    event.event_id = format!(
        "session-input-admission:{session_id}:{}",
        admission.generation
    );
    event.status = Some(if admission.open { "open" } else { "closed" }.to_string());
    let stored = event.to_session_event().map_err(|error| {
        SessionError::Store(format!("session admission event encode failed: {error}"))
    })?;
    tx.execute(
        "INSERT INTO session_events
            (session_id, event_type, event_json, sequence, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            stored.session_id,
            stored.event_type,
            stored.event_json,
            stored.sequence as i64,
            stored.created_at_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn insert_runtime_input_outbox(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
    input_sequence: usize,
    request: &SessionRuntimeOutboxRequest,
) -> Result<SessionRuntimeOutboxRecord> {
    tx.execute(
        r"INSERT INTO session_runtime_outbox
            (input_id, request_id, turn_id, message_id, session_id, sequence,
             session_generation, decision, target_turn_id, classification_json,
             status, attempts, next_attempt_at_ms, revision, created_at_ms,
             updated_at_ms, runtime_options_json)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                   'accepted', 0, ?11, 0, ?11, ?11, ?12)",
        params![
            request.input_id,
            request.request_id,
            request.turn_id,
            request.message_id,
            session_id,
            input_sequence as i64,
            request.session_generation as i64,
            input_decision_as_str(request.decision),
            request.target_turn_id,
            request.classification_json,
            request.created_at_ms as i64,
            request.runtime_options_json,
        ],
    )
    .map_err(sql_err)?;
    let accepted = query_outbox(tx, &request.request_id)?.ok_or_else(|| {
        SessionError::Store("accepted session input produced no readable row".to_string())
    })?;
    append_outbox_history(
        tx,
        &accepted,
        "accept",
        None,
        None,
        "none",
        SessionRuntimeInputStatus::Accepted.as_str(),
        request.created_at_ms,
    )?;
    append_input_timeline_event(
        tx,
        request,
        session_id,
        input_sequence,
        SessionRuntimeInputStatus::Accepted.timeline_event_kind(),
        SessionRuntimeInputStatus::Accepted,
        None,
        None,
        request.created_at_ms,
    )?;
    tx.execute(
        "UPDATE session_runtime_outbox
            SET status = 'classified', revision = revision + 1
          WHERE request_id = ?1 AND status = 'accepted' AND revision = 0",
        params![request.request_id],
    )
    .map_err(sql_err)?;
    let classified = query_outbox(tx, &request.request_id)?.ok_or_else(|| {
        SessionError::Store("classified session input produced no readable row".to_string())
    })?;
    append_outbox_history(
        tx,
        &classified,
        "classify",
        None,
        request.classification_json.as_deref(),
        SessionRuntimeInputStatus::Accepted.as_str(),
        SessionRuntimeInputStatus::Classified.as_str(),
        request.created_at_ms,
    )?;
    append_input_timeline_event(
        tx,
        request,
        session_id,
        input_sequence,
        SessionRuntimeInputStatus::Classified.timeline_event_kind(),
        SessionRuntimeInputStatus::Classified,
        None,
        None,
        request.created_at_ms,
    )?;
    let final_status = SessionRuntimeInputStatus::for_rejection(request.decision)
        .unwrap_or(SessionRuntimeInputStatus::Queued);
    let terminal_at_ms = final_status
        .is_terminal()
        .then_some(request.created_at_ms as i64);
    tx.execute(
        "UPDATE session_runtime_outbox
            SET status = ?2, terminal_at_ms = ?3, revision = revision + 1
          WHERE request_id = ?1 AND status = 'classified' AND revision = 1",
        params![request.request_id, final_status.as_str(), terminal_at_ms],
    )
    .map_err(sql_err)?;
    let finalized = query_outbox(tx, &request.request_id)?.ok_or_else(|| {
        SessionError::Store("finalized session input produced no readable row".to_string())
    })?;
    append_outbox_history(
        tx,
        &finalized,
        if final_status.is_terminal() {
            "reject"
        } else {
            "queue"
        },
        None,
        request.classification_json.as_deref(),
        SessionRuntimeInputStatus::Classified.as_str(),
        final_status.as_str(),
        request.created_at_ms,
    )?;
    append_input_timeline_event(
        tx,
        request,
        session_id,
        input_sequence,
        final_status.timeline_event_kind(),
        final_status,
        None,
        request.classification_json.as_deref(),
        request.created_at_ms,
    )?;
    Ok(finalized)
}

fn row_to_mission_outbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionMissionOutboxRecord> {
    Ok(SessionMissionOutboxRecord {
        request_id: row.get(0)?,
        session_id: row.get(1)?,
        title: row.get(2)?,
        workspace_key: row.get(3)?,
        operation: SessionMissionOutboxOperation::parse(&row.get::<_, String>(4)?)?,
        status: OutboxStatus::parse(&row.get::<_, String>(5)?)?,
        attempts: row.get::<_, i64>(6)? as u32,
        next_attempt_at_ms: row.get::<_, i64>(7)? as u64,
        claim_owner: row.get(8)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
        failure_class: row
            .get::<_, Option<String>>(10)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(11)?,
        revision: row.get::<_, i64>(12)? as u64,
        created_at_ms: row.get::<_, i64>(13)? as u64,
        updated_at_ms: row.get::<_, i64>(14)? as u64,
    })
}

fn query_mission_outbox(
    conn: &Connection,
    request_id: &str,
) -> Result<Option<SessionMissionOutboxRecord>> {
    conn.query_row(
        r"SELECT request_id, session_id, title, workspace_key, operation, status,
                  attempts, next_attempt_at_ms, claim_owner, claim_expires_at_ms,
                  failure_class, last_error, revision, created_at_ms, updated_at_ms
             FROM session_mission_outbox WHERE request_id = ?1",
        params![request_id],
        row_to_mission_outbox,
    )
    .optional()
    .map_err(sql_err)
}

#[allow(clippy::too_many_arguments)]
fn append_mission_outbox_history(
    tx: &rusqlite::Transaction<'_>,
    record: &SessionMissionOutboxRecord,
    action: &str,
    actor: Option<&str>,
    reason: Option<&str>,
    from_status: &str,
    to_status: &str,
    now_ms: u64,
) -> Result<()> {
    tx.execute(
        r"INSERT INTO session_mission_outbox_history
            (request_id, action, actor, reason, from_status, to_status, attempts, created_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            record.request_id,
            action,
            actor,
            reason,
            from_status,
            to_status,
            record.attempts as i64,
            now_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn validate_mission_outbox_request(request: &SessionMissionOutboxRequest) -> Result<()> {
    if request.request_id.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.title.trim().is_empty()
        || request.workspace_key.trim().is_empty()
    {
        return Err(SessionError::Store(
            "session/mission outbox identities must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn insert_mission_outbox(
    tx: &rusqlite::Transaction<'_>,
    request: &SessionMissionOutboxRequest,
) -> Result<SessionMissionOutboxRecord> {
    if let Some(existing) = query_mission_outbox(tx, &request.request_id)? {
        if existing.session_id == request.session_id
            && existing.workspace_key == request.workspace_key
            && existing.operation == request.operation
        {
            // A title is mutable presentation metadata, not part of the
            // Session -> Mission lifecycle identity. Surface registration and
            // Runtime hydration may legitimately supply different display
            // titles for the same durable register intent.
            return Ok(existing);
        }
        return Err(SessionError::Store(format!(
            "mission outbox request_id `{}` is already bound to another lifecycle intent",
            request.request_id
        )));
    }
    tx.execute(
        r"INSERT INTO session_mission_outbox
            (request_id, session_id, title, workspace_key, operation, status, attempts,
             next_attempt_at_ms, revision, created_at_ms, updated_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, 0, ?6, ?6)",
        params![
            request.request_id,
            request.session_id,
            request.title,
            request.workspace_key,
            request.operation.as_str(),
            request.created_at_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    let record = query_mission_outbox(tx, &request.request_id)?.ok_or_else(|| {
        SessionError::Store("mission outbox insert produced no readable row".to_string())
    })?;
    append_mission_outbox_history(
        tx,
        &record,
        "enqueue",
        None,
        None,
        "none",
        OutboxStatus::Pending.as_str(),
        request.created_at_ms,
    )?;
    Ok(record)
}

// ---------------------------------------------------------------------------
// SessionEvent / SessionSnapshot
// ---------------------------------------------------------------------------

/// A recorded mutation event for a session, enabling event-sourced
/// reconstruction and time-travel debugging.
///
/// Each event is associated with a monotonically-increasing `sequence`
/// that orders it within the session's event log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    pub session_id: String,
    pub event_type: String,
    pub event_json: String,
    pub sequence: usize,
    pub created_at_ms: u64,
}

/// A full-message-list snapshot taken at a specific event index, used
/// as a basis for fast replay from that point forward.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_id: String,
    /// Event sequence index this snapshot corresponds to.
    pub event_idx: usize,
    /// Full JSON array of all messages at that point in time.
    pub messages_json: String,
    pub created_at_ms: u64,
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEvent> {
    Ok(SessionEvent {
        session_id: row.get(1)?,
        event_type: row.get(2)?,
        event_json: row.get(3)?,
        sequence: row.get::<_, i64>(4)? as usize,
        created_at_ms: row.get::<_, i64>(5)? as u64,
    })
}

fn row_to_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionSnapshot> {
    Ok(SessionSnapshot {
        session_id: row.get(1)?,
        event_idx: row.get::<_, i64>(2)? as usize,
        messages_json: row.get(3)?,
        created_at_ms: row.get::<_, i64>(4)? as u64,
    })
}

fn escape_like_pattern(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn fts_literal_terms(input: &str) -> Option<String> {
    let terms = input
        .split_whitespace()
        .map(|term| term.trim())
        .filter(|term| !term.is_empty())
        .take(12)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    (!terms.is_empty()).then(|| terms.join(" OR "))
}

fn session_sort_expression(sort: &str) -> &'static str {
    match sort {
        "created_at" => "created_at",
        "message_count" => "message_count",
        "model" => "COALESCE(model, '') COLLATE NOCASE",
        "title" => "COALESCE(json_extract(metadata_json, '$.title'), '') COLLATE NOCASE",
        _ => "last_activity",
    }
}

fn session_sort_order(order: &str) -> &'static str {
    if order.eq_ignore_ascii_case("asc") {
        "ASC"
    } else {
        "DESC"
    }
}

fn iso_to_ms(value: &str) -> i64 {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|_| Utc::now().timestamp_millis())
}

fn session_list_where_clause(opts: &SessionListOptions<'_>) -> (String, Vec<Value>) {
    let mut where_parts: Vec<&'static str> = Vec::new();
    let mut values = Vec::new();

    if !opts.unrestricted {
        let visible_ids =
            serde_json::to_string(opts.visible_session_ids).unwrap_or_else(|_| "[]".to_string());
        where_parts.push(
            "(json_extract(metadata_json, '$.owner_principal_id') = ?
              OR session_id IN (SELECT value FROM json_each(?)))",
        );
        values.push(Value::Text(
            opts.owner_principal_id.unwrap_or_default().to_string(),
        ));
        values.push(Value::Text(visible_ids));
    }

    if let Some(status) = opts.status.map(str::trim).filter(|s| !s.is_empty()) {
        where_parts.push("status = ? COLLATE NOCASE");
        values.push(Value::Text(status.to_string()));
    } else if !opts.include_deleted {
        where_parts.push("status NOT IN ('deleted', 'deleting') COLLATE NOCASE");
    }
    if let Some(model) = opts.model.map(str::trim).filter(|s| !s.is_empty()) {
        where_parts.push("model = ? COLLATE NOCASE");
        values.push(Value::Text(model.to_string()));
    }
    if let Some(query) = opts.query.map(str::trim).filter(|s| !s.is_empty()) {
        where_parts.push(
            "(session_id LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR platform LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR chat_id LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR COALESCE(user_id, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR COALESCE(model, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR status LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR COALESCE(metadata_json, '') LIKE ? ESCAPE '\\' COLLATE NOCASE)",
        );
        let pattern = format!("%{}%", escape_like_pattern(query));
        for _ in 0..7 {
            values.push(Value::Text(pattern.clone()));
        }
    }

    let clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };
    (clause, values)
}

// ---------------------------------------------------------------------------
// SqliteSessionStore
// ---------------------------------------------------------------------------

/// Persistent, SQLite-backed session store.
///
/// Uses an r2d2 connection pool so that multiple concurrent operations can
/// share a bounded set of WAL-enabled connections.
///
/// # In-memory mode (tests)
///
/// Pass `":memory:"` as the path. Pool size is 1 for in-memory so that all
/// operations share the same database handle.
#[derive(Debug, Clone)]
pub struct SqliteSessionStore {
    pool: Pool<SqliteConnectionManager>,
}

fn validate_terminal_transcript(
    terminal_message_id: &str,
    ingress_message_id: &str,
    session_id: &str,
    messages: &[SessionMessage],
) -> Result<()> {
    if terminal_message_id.trim().is_empty()
        || ingress_message_id.trim().is_empty()
        || session_id.trim().is_empty()
        || messages.is_empty()
        || messages
            .last()
            .is_none_or(|message| message.stable_message_id != terminal_message_id)
    {
        return Err(SessionError::InvalidArgument(
            "terminal transcript requires a non-empty session, ingress, terminal ID, and final row"
                .to_string(),
        ));
    }
    if messages.iter().any(|message| {
        message.stable_message_id.trim().is_empty()
            || message.session_id != session_id
            || message.role.trim().is_empty()
            || serde_json::from_str::<serde_json::Value>(&message.content_json)
                .ok()
                .and_then(|value| value.as_array().cloned())
                .is_none()
    }) {
        return Err(SessionError::InvalidArgument(
            "terminal transcript contains an invalid message row".to_string(),
        ));
    }
    let unique_ids = messages
        .iter()
        .map(|message| message.stable_message_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if unique_ids.len() != messages.len() {
        return Err(SessionError::InvalidArgument(
            "terminal transcript contains duplicate stable message IDs".to_string(),
        ));
    }
    Ok(())
}

fn validate_terminal_commit(request: &SessionTerminalTranscriptCommit) -> Result<()> {
    if request.turn_id.trim().is_empty()
        || request.runtime_commit_cursor == 0
        || request.consumed_input_sequence < request.fence.input_sequence
        || request.fence.request_id.trim().is_empty()
        || request.fence.session_generation == 0
        || request.fence.claim_owner.trim().is_empty()
        || request.fence.claim_token.trim().is_empty()
        || request.fence.claim_fence_epoch == 0
    {
        return Err(SessionError::InvalidArgument(
            "terminal commit requires complete turn, cursor and live execution fence identity"
                .to_string(),
        ));
    }
    Ok(())
}

fn append_terminal_transcript_tx(
    tx: &rusqlite::Transaction<'_>,
    terminal_message_id: &str,
    ingress_message_id: &str,
    session_id: &str,
    messages: &[SessionMessage],
    created_at_ms: u64,
) -> Result<(Vec<SessionMessage>, bool)> {
    let load = |message_id: &str| -> Result<Option<SessionMessage>> {
        tx.query_row(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM messages WHERE stable_message_id = ?1",
            params![message_id],
            |row| {
                Ok(SessionMessage {
                    stable_message_id: row.get(0)?,
                    session_id: row.get(1)?,
                    sequence: row.get::<_, i64>(2)? as usize,
                    role: row.get(3)?,
                    content_json: row.get(4)?,
                    blocks_count: row.get::<_, i64>(5)? as usize,
                    tool_use_id: row.get(6)?,
                    tool_name: row.get(7)?,
                    token_usage_json: row.get(8)?,
                    created_at_ms: row.get::<_, i64>(9)? as u64,
                })
            },
        )
        .optional()
        .map_err(sql_err)
    };
    let terminal_exists = load(terminal_message_id)?.is_some();
    let mut existing = Vec::with_capacity(messages.len());
    for requested in messages {
        match load(&requested.stable_message_id)? {
            Some(committed) => {
                if committed.session_id != requested.session_id
                    || committed.role != requested.role
                    || committed.content_json != requested.content_json
                    || committed.blocks_count != requested.blocks_count
                    || committed.tool_use_id != requested.tool_use_id
                    || committed.tool_name != requested.tool_name
                    || committed.token_usage_json != requested.token_usage_json
                {
                    return Err(SessionError::Store(format!(
                        "terminal transcript message_id `{}` conflicts with committed content",
                        requested.stable_message_id
                    )));
                }
                existing.push(committed);
            }
            None if terminal_exists => {
                return Err(SessionError::Store(format!(
                    "terminal transcript `{terminal_message_id}` is partially committed"
                )));
            }
            None => {}
        }
    }
    if terminal_exists {
        if existing.len() != messages.len() {
            return Err(SessionError::Store(format!(
                "terminal transcript `{terminal_message_id}` is partially committed"
            )));
        }
        existing.sort_by_key(|message| message.sequence);
        return Ok((existing, false));
    }
    if !existing.is_empty() {
        return Err(SessionError::Store(format!(
            "terminal transcript `{terminal_message_id}` collides with existing intermediate rows"
        )));
    }
    tx.query_row(
        "SELECT sequence FROM messages
          WHERE stable_message_id=?1 AND session_id=?2 AND role='user'",
        params![ingress_message_id, session_id],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(sql_err)?
    .ok_or_else(|| {
        SessionError::Store(format!(
            "terminal transcript ingress `{ingress_message_id}` is not committed"
        ))
    })?;
    let first_sequence = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM messages WHERE session_id=?1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_err)
        .and_then(|sequence| {
            usize::try_from(sequence)
                .map_err(|_| SessionError::Store("terminal transcript sequence overflow".into()))
        })?;
    let mut committed = Vec::with_capacity(messages.len());
    for (index, requested) in messages.iter().enumerate() {
        let mut message = requested.clone();
        message.sequence = first_sequence.saturating_add(index);
        message.created_at_ms = created_at_ms.saturating_add(index as u64);
        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                message.stable_message_id,
                message.session_id,
                message.sequence as i64,
                message.role,
                message.content_json,
                message.blocks_count as i64,
                message.tool_use_id,
                message.tool_name,
                message.token_usage_json,
                message.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        committed.push(message);
    }
    let last_created_at = committed
        .last()
        .map_or(created_at_ms, |message| message.created_at_ms);
    refresh_session_message_summary_tx(tx, session_id, last_created_at)?;
    refresh_session_usage_summary_tx(tx, session_id)?;
    Ok((committed, true))
}

fn load_committed_terminal_transcript_tx(
    tx: &rusqlite::Transaction<'_>,
    terminal_message_id: &str,
    messages: &[SessionMessage],
) -> Result<Vec<SessionMessage>> {
    let mut committed = Vec::with_capacity(messages.len());
    for requested in messages {
        let existing = tx
            .query_row(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms
                   FROM messages WHERE stable_message_id = ?1",
                params![requested.stable_message_id],
                |row| {
                    Ok(SessionMessage {
                        stable_message_id: row.get(0)?,
                        session_id: row.get(1)?,
                        sequence: row.get::<_, i64>(2)? as usize,
                        role: row.get(3)?,
                        content_json: row.get(4)?,
                        blocks_count: row.get::<_, i64>(5)? as usize,
                        tool_use_id: row.get(6)?,
                        tool_name: row.get(7)?,
                        token_usage_json: row.get(8)?,
                        created_at_ms: row.get::<_, i64>(9)? as u64,
                    })
                },
            )
            .optional()
            .map_err(sql_err)?
            .ok_or_else(|| {
                SessionError::StaleExecutionFence(format!(
                    "completed terminal transcript `{terminal_message_id}` does not match replay"
                ))
            })?;
        if existing.session_id != requested.session_id
            || existing.role != requested.role
            || existing.content_json != requested.content_json
            || existing.blocks_count != requested.blocks_count
            || existing.tool_use_id != requested.tool_use_id
            || existing.tool_name != requested.tool_name
            || existing.token_usage_json != requested.token_usage_json
        {
            return Err(SessionError::StaleExecutionFence(format!(
                "completed terminal transcript `{terminal_message_id}` content does not match replay"
            )));
        }
        committed.push(existing);
    }
    if committed
        .windows(2)
        .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(SessionError::StaleExecutionFence(format!(
            "completed terminal transcript `{terminal_message_id}` order does not match replay"
        )));
    }
    if committed
        .last()
        .is_none_or(|message| message.stable_message_id != terminal_message_id)
    {
        return Err(SessionError::StaleExecutionFence(format!(
            "completed terminal transcript `{terminal_message_id}` identity does not match replay"
        )));
    }
    Ok(committed)
}

impl SqliteSessionStore {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Open (or create) a session database at `path`.
    ///
    /// Creates any missing parent directories and initialises the schema if
    /// the database is new.
    pub fn open(path: &Path) -> Result<Self> {
        let handle = storage::StorageHandle::sqlite(
            "session",
            path.to_path_buf(),
            "memory",
            "session_store_path_adapter_since_0.9.315",
        );
        Self::open_storage_handle(&handle)
    }

    /// Open a session database through a typed storage handle.
    pub fn open_storage_handle(handle: &storage::StorageHandle) -> Result<Self> {
        if handle.backend != storage::StorageBackendKind::Sqlite {
            return Err(SessionError::Store(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        let path = &handle.path;
        let db_path = path
            .to_str()
            .ok_or_else(|| SessionError::Store("non-UTF-8 session db path".to_string()))?
            .to_owned();
        // Create parent directories if needed (skip for ":memory:").
        if db_path != IN_MEMORY_PATH {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    SessionError::Store(format!("cannot create session db dir: {e}"))
                })?;
            }
        }
        let pool = new_pool(&db_path, 10)?;
        let store = Self { pool };
        let conn = store.conn()?;
        init_schema(&conn)?;
        Ok(store)
    }

    /// Open an in-memory session database (useful for testing).
    pub fn open_in_memory() -> Result<Self> {
        let pool = new_pool(IN_MEMORY_PATH, 1)?;
        let store = Self { pool };
        let conn = store.conn()?;
        init_schema(&conn)?;
        Ok(store)
    }

    pub fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        let conn = self
            .pool
            .get()
            .map_err(|e| SessionError::Store(e.to_string()))?;
        set_conn_pragmas(&conn)?;
        Ok(conn)
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// Insert a new session record.
    ///
    /// Uses `INSERT OR IGNORE` so calling this for an already-existing session
    /// is a harmless no-op.
    pub fn create_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT OR IGNORE INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Retrieve a session record by its ID, or `None` if not found.
    pub fn get_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, estimated_cost_usd, status
               FROM sessions WHERE session_id = ?1",
            params![session_id],
            row_to_record,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Read the body-free recovery projection for one Session.
    pub fn get_session_recovery_manifest(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecoveryManifest>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT session_id, durable_cursor, event_cursor, history_revision,
                     transcript_messages, transcript_bytes,
                     latest_checkpoint_sequence, latest_checkpoint_event_id,
                     index_generation, indexed_through_sequence, index_card_count,
                     index_pending,
                     in_flight_turn,
                     pending_approval, active_writer_or_attachment,
                     mission_agent_team_continuation, last_activity_ms,
                     manifest_revision
                FROM session_recovery_manifest
               WHERE session_id=?1",
            params![session_id],
            row_to_recovery_manifest,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Rebuild the body-free activation manifest from canonical rows.
    ///
    /// This repair path intentionally leaves source messages and events
    /// untouched. It marks the navigation index pending so the asynchronous
    /// projector can verify/rebuild cards after activation.
    pub fn rebuild_session_recovery_manifest(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> Result<Option<SessionRecoveryManifest>> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        let inserted = tx
            .execute(
                r"INSERT OR IGNORE INTO session_recovery_manifest(
                       session_id, last_activity_ms, manifest_revision
                   )
                   SELECT session_id, MAX(created_at_ms, updated_at_ms), 1
                     FROM sessions
                    WHERE session_id=?1",
                params![session_id],
            )
            .map_err(sql_err)?;
        if inserted == 0 {
            let session_exists = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id=?1)",
                    params![session_id],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sql_err)?;
            if !session_exists {
                tx.commit().map_err(sql_err)?;
                return Ok(None);
            }
        }
        tx.execute(
            r"UPDATE session_recovery_manifest
                  SET durable_cursor=COALESCE((
                          SELECT MAX(sequence)+1 FROM messages
                           WHERE session_id=?1
                      ),0),
                      event_cursor=COALESCE((
                          SELECT MAX(sequence)+1 FROM session_events
                           WHERE session_id=?1
                      ),0),
                      history_revision=COALESCE((
                          SELECT COUNT(*) FROM messages WHERE session_id=?1
                      ),0),
                      transcript_messages=COALESCE((
                          SELECT COUNT(*) FROM messages WHERE session_id=?1
                      ),0),
                      transcript_bytes=COALESCE((
                          SELECT SUM(
                              length(CAST(stable_message_id AS BLOB))
                              + length(CAST(session_id AS BLOB))
                              + length(CAST(role AS BLOB))
                              + length(CAST(content_json AS BLOB))
                              + length(CAST(COALESCE(token_usage_json,'') AS BLOB))
                              + length(CAST(COALESCE(tool_use_id,'') AS BLOB))
                              + length(CAST(COALESCE(tool_name,'') AS BLOB))
                          ) FROM messages WHERE session_id=?1
                      ),0),
                      latest_checkpoint_sequence=(
                          SELECT MAX(sequence) FROM session_events
                           WHERE session_id=?1
                             AND event_type='SessionDomainEvent'
                             AND json_extract(event_json,'$.kind')=
                                 'memory.semantic_checkpoint.created'
                      ),
                      latest_checkpoint_event_id=(
                          SELECT json_extract(event_json,'$.event_id')
                            FROM session_events
                           WHERE session_id=?1
                             AND event_type='SessionDomainEvent'
                             AND json_extract(event_json,'$.kind')=
                                 'memory.semantic_checkpoint.created'
                           ORDER BY sequence DESC LIMIT 1
                      ),
                      index_generation=COALESCE((
                          SELECT MAX(generation) FROM session_context_index_cards
                           WHERE session_id=?1
                      ),0),
                      indexed_through_sequence=(
                          SELECT MAX(source_end_sequence)
                            FROM session_context_index_cards WHERE session_id=?1
                      ),
                      index_card_count=COALESCE((
                          SELECT COUNT(*) FROM session_context_index_cards
                           WHERE session_id=?1
                      ),0),
                      index_pending=CASE WHEN EXISTS(
                          SELECT 1 FROM messages WHERE session_id=?1
                      ) OR EXISTS(
                          SELECT 1 FROM session_events
                           WHERE session_id=?1
                             AND event_type='SessionDomainEvent'
                             AND json_extract(event_json,'$.kind')=
                                 'memory.semantic_checkpoint.created'
                      ) THEN 1 ELSE 0 END,
                      in_flight_turn=EXISTS(
                          SELECT 1 FROM session_runtime_outbox
                           WHERE session_id=?1
                             AND status IN (
                                 'accepted','classified','queued','claimed',
                                 'running','reclassified'
                             )
                      ),
                      active_writer_or_attachment=COALESCE((
                          SELECT CASE WHEN json_array_length(
                              json_extract(event_json,'$.snapshot.attachments')
                          ) > 0 THEN 1 ELSE 0 END
                            FROM session_events
                           WHERE session_id=?1
                             AND event_type='session.lifecycle.v1'
                           ORDER BY sequence DESC LIMIT 1
                      ),0),
                      mission_agent_team_continuation=EXISTS(
                          SELECT 1 FROM session_mission_outbox
                           WHERE session_id=?1
                             AND operation='start'
                             AND status IN ('pending','claimed','retry_scheduled')
                      ),
                      last_activity_ms=MAX(last_activity_ms,?2),
                      manifest_revision=manifest_revision+1
                WHERE session_id=?1",
            params![session_id, now_ms as i64],
        )
        .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO session_context_index_outbox(
                   session_id, source_sequence, operation, status,
                   created_at_ms, updated_at_ms
               )
               SELECT ?1,0,'reconcile','pending',?2,?2
                WHERE EXISTS(SELECT 1 FROM messages WHERE session_id=?1)
               ON CONFLICT(session_id, source_sequence, operation) DO UPDATE SET
                   status='pending',
                   updated_at_ms=MAX(updated_at_ms,excluded.updated_at_ms)",
            params![session_id, now_ms as i64],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        self.get_session_recovery_manifest(session_id)
    }

    /// Page active Session manifests without reading transcript rows.
    pub fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionRecoveryManifest>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT manifest.session_id, manifest.durable_cursor,
                         manifest.event_cursor, manifest.history_revision,
                         manifest.transcript_messages, manifest.transcript_bytes,
                         manifest.latest_checkpoint_sequence,
                         manifest.latest_checkpoint_event_id,
                         manifest.index_generation,
                         manifest.indexed_through_sequence,
                         manifest.index_card_count,
                         manifest.index_pending,
                         manifest.in_flight_turn,
                         manifest.pending_approval,
                         manifest.active_writer_or_attachment,
                         manifest.mission_agent_team_continuation,
                         manifest.last_activity_ms, manifest.manifest_revision
                    FROM session_recovery_manifest AS manifest
                    JOIN sessions ON sessions.session_id=manifest.session_id
                   WHERE sessions.status='active'
                   ORDER BY manifest.last_activity_ms DESC, manifest.session_id ASC
                   LIMIT ?1 OFFSET ?2",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(
                params![limit as i64, offset as i64],
                row_to_recovery_manifest,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Update one external durable recovery signal without overwriting other
    /// independently-owned signals.
    pub fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> Result<SessionRecoveryManifest> {
        let column = match signal {
            SessionRecoverySignal::PendingApproval => "pending_approval",
            SessionRecoverySignal::ActiveWriterOrAttachment => "active_writer_or_attachment",
            SessionRecoverySignal::MissionAgentTeamContinuation => {
                "mission_agent_team_continuation"
            }
        };
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "UPDATE session_recovery_manifest
                    SET {column}=?2,
                        last_activity_ms=MAX(last_activity_ms, ?3),
                        manifest_revision=manifest_revision + 1
                  WHERE session_id=?1"
            ),
            params![session_id, active, observed_at_ms as i64],
        )
        .map_err(sql_err)?;
        drop(conn);
        self.get_session_recovery_manifest(session_id)?
            .ok_or_else(|| {
                SessionError::Store(format!(
                    "session recovery manifest `{session_id}` does not exist"
                ))
            })
    }

    /// Overwrite all mutable fields of an existing session record.
    ///
    /// `session_id` is used as the lookup key; the row is silently unchanged
    /// if it does not exist.
    pub fn update_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"UPDATE sessions SET
               platform      = ?2,
               chat_id       = ?3,
               user_id       = ?4,
               model         = ?5,
               last_activity = ?6,
               message_count = ?7,
               reset_policy  = ?8,
               metadata_json = ?9,
               input_tokens  = ?10,
               output_tokens = ?11,
               estimated_cost_usd = ?12,
               status = ?13,
               updated_at_ms = ?14
               WHERE session_id = ?1",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Upsert a session record (insert or replace all fields).
    ///
    /// Equivalent to calling [`create_session`] then [`update_session`].  Use
    /// this when you don't know whether the row already exists.
    pub fn upsert_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
               ON CONFLICT(session_id) DO UPDATE SET
                 platform = excluded.platform,
                 chat_id = excluded.chat_id,
                 user_id = excluded.user_id,
                 model = excluded.model,
                 created_at = excluded.created_at,
                 last_activity = excluded.last_activity,
                 message_count = excluded.message_count,
                 reset_policy = excluded.reset_policy,
                 metadata_json = excluded.metadata_json,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 estimated_cost_usd = excluded.estimated_cost_usd,
                 status = excluded.status,
                 created_at_ms = excluded.created_at_ms,
                 updated_at_ms = excluded.updated_at_ms",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Atomically persist a Session record and enqueue its Mission lifecycle
    /// registration. The Runtime bridge is the only consumer of this intent;
    /// callers never need to dual-write the Session and Mission stores.
    pub fn upsert_session_with_mission_outbox(
        &self,
        session: &SessionRecord,
        request: &SessionMissionOutboxRequest,
    ) -> Result<SessionMissionOutboxRecord> {
        validate_mission_outbox_request(request)?;
        if request.session_id != session.session_id {
            return Err(SessionError::Store(
                "session/mission outbox session identity does not match record".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
               ON CONFLICT(session_id) DO UPDATE SET
                 platform = excluded.platform,
                 chat_id = excluded.chat_id,
                 user_id = excluded.user_id,
                 model = excluded.model,
                 created_at = excluded.created_at,
                 last_activity = excluded.last_activity,
                 message_count = excluded.message_count,
                 reset_policy = excluded.reset_policy,
                 metadata_json = excluded.metadata_json,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 estimated_cost_usd = excluded.estimated_cost_usd,
                 status = excluded.status,
                 created_at_ms = excluded.created_at_ms,
                 updated_at_ms = excluded.updated_at_ms",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        let record = insert_mission_outbox(&tx, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(record)
    }

    pub fn plan_session_lifecycle(
        &self,
        plan: &SessionLifecyclePlan,
    ) -> Result<SessionLifecycleIntent> {
        if plan.operation_id.trim().is_empty()
            || plan.session_id.trim().is_empty()
            || plan.expected_generation == 0
        {
            return Err(SessionError::Store(
                "Session lifecycle plan requires non-empty identities and a positive generation"
                    .to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        if let Some(existing) = query_lifecycle_intent(&tx, &plan.operation_id)? {
            if existing.session_id == plan.session_id
                && existing.disposition == plan.disposition
                && existing.expected_generation == plan.expected_generation
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(SessionError::Store(format!(
                "Session lifecycle operation `{}` is bound to another identity",
                plan.operation_id
            )));
        }
        let admission = query_input_admission(&tx, &plan.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", plan.session_id))
        })?;
        if admission.generation != plan.expected_generation || !admission.open {
            return Err(SessionError::Store(format!(
                "Session lifecycle plan `{}` expected open generation {}, found generation {} open={}",
                plan.operation_id,
                plan.expected_generation,
                admission.generation,
                admission.open
            )));
        }
        tx.execute(
            r"INSERT INTO session_lifecycle_intents
                (operation_id, session_id, disposition, phase, last_stable_phase,
                 expected_generation, created_at_ms, updated_at_ms, last_error, revision)
               VALUES (?1, ?2, ?3, 'planned', 'planned', ?4, ?5, ?5, NULL, 0)",
            params![
                plan.operation_id,
                plan.session_id,
                plan.disposition.as_str(),
                plan.expected_generation as i64,
                plan.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        let intent = query_lifecycle_intent(&tx, &plan.operation_id)?.ok_or_else(|| {
            SessionError::Store("Session lifecycle plan produced no readable row".to_string())
        })?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    pub fn get_session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionLifecycleIntent>> {
        let conn = self.conn()?;
        query_lifecycle_intent(&conn, operation_id)
    }

    pub fn list_recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionLifecycleIntent>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT operation_id, session_id, disposition, phase, last_stable_phase,
                          expected_generation, created_at_ms, updated_at_ms, last_error, revision
                     FROM session_lifecycle_intents
                    WHERE phase != 'unloaded'
                    ORDER BY updated_at_ms ASC, operation_id ASC
                    LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(params![limit as i64], row_to_lifecycle_intent)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn fence_session_lifecycle(
        &self,
        request: &SessionLifecycleFenceRequest,
    ) -> Result<SessionLifecycleIntent> {
        if request.actor.trim().is_empty()
            || request.reason.trim().is_empty()
            || request.transitional_status.trim().is_empty()
        {
            return Err(SessionError::Store(
                "Session lifecycle fence requires actor, reason, and transitional status"
                    .to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current =
            query_lifecycle_intent(&tx, &request.transition.operation_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "Session lifecycle intent `{}` does not exist",
                    request.transition.operation_id
                ))
            })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::AdmissionFenced
            || request.event.session_id != current.session_id
        {
            return Err(SessionError::Store(
                "Session lifecycle fence identity or phase is invalid".to_string(),
            ));
        }
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if admission.generation != current.expected_generation || !admission.open {
            return Err(SessionError::Store(format!(
                "Session lifecycle fence `{}` lost generation authority",
                current.operation_id
            )));
        }
        let active = {
            let mut statement = tx
                .prepare(
                    r"SELECT request_id FROM session_runtime_outbox
                       WHERE session_id=?1 AND session_generation=?2
                         AND status IN (
                             'accepted','classified','queued','claimed',
                             'running','reclassified','blocked'
                         )
                       ORDER BY sequence ASC, request_id ASC",
                )
                .map_err(sql_err)?;
            let rows = statement
                .query_map(
                    params![current.session_id, current.expected_generation as i64],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };
        let next_generation = current
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| SessionError::Store("Session generation overflow".to_string()))?;
        let changed = tx
            .execute(
                r"UPDATE sessions
                     SET input_generation=?1, input_admission_open=0, status=?2,
                         last_activity=?3, updated_at_ms=MAX(updated_at_ms, ?4)
                   WHERE session_id=?5 AND input_generation=?6
                     AND input_admission_open=1",
                params![
                    next_generation as i64,
                    request.transitional_status,
                    DateTime::<Utc>::from_timestamp_millis(request.transition.updated_at_ms as i64)
                        .unwrap_or_else(Utc::now)
                        .to_rfc3339(),
                    request.transition.updated_at_ms as i64,
                    current.session_id,
                    current.expected_generation as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "Session lifecycle fence `{}` changed during admission close",
                current.operation_id
            )));
        }
        for request_id in active {
            let before = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "outbox `{request_id}` disappeared during lifecycle fence"
                ))
            })?;
            tx.execute(
                r"UPDATE session_runtime_outbox
                     SET status='expired', claim_owner=NULL, claim_token=NULL,
                         claim_fence_epoch=NULL,
                         claim_expires_at_ms=NULL, last_error=?1,
                         terminal_at_ms=?2, updated_at_ms=?2, revision=revision+1
                   WHERE request_id=?3 AND session_generation=?4 AND revision=?5",
                params![
                    request.reason,
                    request.transition.updated_at_ms as i64,
                    request_id,
                    current.expected_generation as i64,
                    before.revision as i64,
                ],
            )
            .map_err(sql_err)?;
            let expired = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!("expired outbox `{request_id}` disappeared"))
            })?;
            append_outbox_history(
                &tx,
                &expired,
                "lifecycle_fence",
                Some(&request.actor),
                Some(&request.reason),
                before.status.as_str(),
                SessionRuntimeInputStatus::Expired.as_str(),
                request.transition.updated_at_ms,
            )?;
        }
        let closed = SessionInputAdmission {
            session_id: current.session_id.clone(),
            generation: next_generation,
            open: false,
        };
        append_admission_timeline_event(
            &tx,
            &current.session_id,
            current.expected_generation,
            &closed,
            &request.actor,
            &request.reason,
            request.transition.updated_at_ms,
        )?;
        append_allocated_event_tx(&tx, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&tx, &request.transition)?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    pub fn transition_session_lifecycle(
        &self,
        transition: &SessionLifecycleTransition,
    ) -> Result<SessionLifecycleIntent> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let intent = transition_lifecycle_intent_tx(&tx, transition)?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    pub fn commit_session_lifecycle_tombstone(
        &self,
        request: &SessionLifecycleTombstoneRequest,
    ) -> Result<SessionLifecycleIntent> {
        validate_mission_outbox_request(&request.mission_outbox)?;
        if request.mission_outbox.operation != SessionMissionOutboxOperation::Close {
            return Err(SessionError::Store(
                "Session tombstone requires a close Mission outbox intent".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current =
            query_lifecycle_intent(&tx, &request.transition.operation_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "Session lifecycle intent `{}` does not exist",
                    request.transition.operation_id
                ))
            })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::TombstoneCommitted
            || request.record.session_id != current.session_id
            || request.mission_outbox.session_id != current.session_id
            || request.event.session_id != current.session_id
        {
            return Err(SessionError::Store(
                "Session lifecycle tombstone identity or phase is invalid".to_string(),
            ));
        }
        let changed = tx
            .execute(
                r"UPDATE sessions SET
                     platform=?2, chat_id=?3, user_id=?4, model=?5,
                     last_activity=?6, message_count=?7, reset_policy=?8,
                     metadata_json=?9, input_tokens=?10, output_tokens=?11,
                     estimated_cost_usd=?12, status=?13, updated_at_ms=?14
                   WHERE session_id=?1 AND input_generation=?15
                     AND input_admission_open=0",
                params![
                    request.record.session_id,
                    request.record.platform,
                    request.record.chat_id,
                    request.record.user_id,
                    request.record.model,
                    request.record.last_activity,
                    request.record.message_count,
                    request.record.reset_policy,
                    request.record.metadata_json,
                    request.record.input_tokens,
                    request.record.output_tokens,
                    request.record.estimated_cost_usd,
                    request.record.status,
                    request.transition.updated_at_ms as i64,
                    current.expected_generation.saturating_add(1) as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "Session lifecycle tombstone `{}` lost fenced Session authority",
                current.operation_id
            )));
        }
        insert_mission_outbox(&tx, &request.mission_outbox)?;
        append_allocated_event_tx(&tx, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&tx, &request.transition)?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    /// Permanently remove a session and all its memory associations.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // FK ON DELETE CASCADE handles cleanup; manual delete is belt-and-suspenders
        tx.execute(
            "DELETE FROM session_memories WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    /// Atomically delete the Session authority record and enqueue a Mission
    /// close intent. The outbox deliberately has no foreign key to `sessions`:
    /// deleting the record must not erase the close command before Runtime has
    /// materialized it.
    pub fn delete_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> Result<bool> {
        validate_mission_outbox_request(request)?;
        if request.operation != SessionMissionOutboxOperation::Close {
            return Err(SessionError::Store(
                "session deletion requires a close mission outbox operation".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
                params![request.session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?
            != 0;
        if !exists {
            tx.commit().map_err(sql_err)?;
            return Ok(false);
        }
        tx.execute(
            "DELETE FROM session_memories WHERE session_id = ?1",
            params![request.session_id],
        )
        .map_err(sql_err)?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![request.session_id],
        )
        .map_err(sql_err)?;
        insert_mission_outbox(&tx, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(true)
    }

    /// List all session records ordered by `last_activity DESC`.
    pub fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd, status
                   FROM sessions ORDER BY last_activity DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt.query_map([], row_to_record).map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// List a filtered, sorted page of sessions directly in SQLite.
    ///
    /// This is the API-facing path for large workspaces. It avoids loading all
    /// sessions into memory before filtering and paginating.
    pub fn list_sessions_page(&self, opts: &SessionListOptions<'_>) -> Result<SessionListPage> {
        let conn = self.conn()?;
        let limit = opts.limit.clamp(1, 500);
        let offset = opts.offset;
        let (where_sql, mut values) = session_list_where_clause(opts);

        let count_sql = format!("SELECT COUNT(*) FROM sessions{where_sql}");
        let total: i64 = conn
            .query_row(&count_sql, params_from_iter(values.iter()), |row| {
                row.get(0)
            })
            .map_err(sql_err)?;

        let sort_expr = session_sort_expression(opts.sort);
        let sort_order = session_sort_order(opts.order);
        let page_sql = format!(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, estimated_cost_usd, status
                 FROM sessions{where_sql}
                ORDER BY {sort_expr} {sort_order}, session_id ASC
                LIMIT ? OFFSET ?"
        );
        values.push(Value::Integer(limit as i64));
        values.push(Value::Integer(offset as i64));

        let mut stmt = conn.prepare(&page_sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(SessionListPage {
            records,
            total: total as usize,
        })
    }

    pub fn session_usage_summary(&self, recent_limit: usize) -> Result<SessionUsageSummary> {
        let conn = self.conn()?;
        let (session_count, message_count, input_tokens, output_tokens, estimated_cost_usd) = conn
            .query_row(
                "SELECT COUNT(*),COALESCE(SUM(message_count),0),
                        COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),
                        COALESCE(SUM(estimated_cost_usd),0)
                   FROM sessions WHERE status NOT IN ('deleted','deleting')",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .map_err(sql_err)?;
        let load_buckets =
            |column: &str| -> Result<std::collections::BTreeMap<String, SessionUsageBucket>> {
                let sql = format!(
                    "SELECT COALESCE(NULLIF(TRIM({column}),''),'unknown'),COUNT(*),
                        COALESCE(SUM(message_count),0),COALESCE(SUM(input_tokens),0),
                        COALESCE(SUM(output_tokens),0),COALESCE(SUM(estimated_cost_usd),0)
                   FROM sessions WHERE status NOT IN ('deleted','deleting')
                  GROUP BY 1 ORDER BY 1"
                );
                let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            SessionUsageBucket {
                                session_count: row.get::<_, i64>(1)? as usize,
                                message_count: row.get(2)?,
                                input_tokens: row.get(3)?,
                                output_tokens: row.get(4)?,
                                estimated_cost_usd: row.get(5)?,
                            },
                        ))
                    })
                    .map_err(sql_err)?;
                rows.collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()
                    .map_err(sql_err)
            };
        let recent_sessions = self
            .list_sessions_page(&SessionListOptions {
                unrestricted: true,
                include_deleted: false,
                sort: "last_activity",
                order: "desc",
                limit: recent_limit.clamp(1, 200),
                ..SessionListOptions::default()
            })?
            .records;
        Ok(SessionUsageSummary {
            session_count: session_count as usize,
            message_count,
            input_tokens,
            output_tokens,
            estimated_cost_usd,
            by_platform: load_buckets("platform")?,
            by_model: load_buckets("model")?,
            recent_sessions,
        })
    }

    /// Discover Session metadata and transcript matches inside the current
    /// Session's durable workspace/actor boundary.
    ///
    /// The current Session row is the authority source. A caller cannot widen
    /// the query by supplying a workspace or principal in tool input.
    pub fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<SessionListPage> {
        let conn = self.conn()?;
        let limit = limit.clamp(1, 100);
        let query = query.map(str::trim).filter(|query| !query.is_empty());
        let mut values = vec![Value::Text(current_session_id.to_string())];
        let mut query_clause = String::new();

        if let Some(query) = query {
            let like = format!("%{}%", escape_like_pattern(query));
            values.push(Value::Text(like));
            query_clause.push_str(
                r" AND (
                       s.session_id LIKE ? ESCAPE '\' COLLATE NOCASE
                    OR s.platform LIKE ? ESCAPE '\' COLLATE NOCASE
                    OR s.chat_id LIKE ? ESCAPE '\' COLLATE NOCASE
                    OR COALESCE(s.metadata_json, '') LIKE ? ESCAPE '\' COLLATE NOCASE",
            );
            for _ in 0..3 {
                values.push(values[1].clone());
            }
            if let Some(fts_query) = fts_literal_terms(query) {
                values.push(Value::Text(fts_query));
                query_clause.push_str(
                    r" OR EXISTS (
                           SELECT 1
                             FROM messages m
                             JOIN messages_fts ON m.id = messages_fts.rowid
                            WHERE m.session_id = s.session_id
                              AND messages_fts MATCH ?
                       )",
                );
            }
            query_clause.push(')');
        }

        let authority_clause = r"
            FROM sessions s
            JOIN sessions current ON current.session_id = ?
           WHERE s.status NOT IN ('deleted', 'deleting')
             AND (
                    s.session_id = current.session_id
                 OR (
                        NULLIF(json_extract(current.metadata_json, '$.workspace_root'), '') IS NOT NULL
                    AND json_extract(s.metadata_json, '$.workspace_root')
                        = json_extract(current.metadata_json, '$.workspace_root')
                    AND (
                           (
                               NULLIF(json_extract(current.metadata_json, '$.owner_principal_id'), '') IS NOT NULL
                           AND json_extract(s.metadata_json, '$.owner_principal_id')
                               = json_extract(current.metadata_json, '$.owner_principal_id')
                           )
                        OR (
                               NULLIF(json_extract(current.metadata_json, '$.owner_principal_id'), '') IS NULL
                           AND NULLIF(current.user_id, '') IS NOT NULL
                           AND s.platform = current.platform
                           AND s.user_id = current.user_id
                           )
                       )
                    )
                 )";
        let count_sql = format!("SELECT COUNT(*) {authority_clause}{query_clause}");
        let total = conn
            .query_row(&count_sql, params_from_iter(values.iter()), |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql_err)?;

        let page_sql = format!(
            r"SELECT s.session_id, s.platform, s.chat_id, s.user_id, s.model,
                      s.created_at, s.last_activity, s.message_count, s.reset_policy,
                      s.metadata_json, s.input_tokens, s.output_tokens,
                      s.estimated_cost_usd, s.status
                 {authority_clause}{query_clause}
                ORDER BY s.last_activity DESC, s.session_id ASC
                LIMIT ? OFFSET ?"
        );
        values.push(Value::Integer(limit as i64));
        values.push(Value::Integer(offset as i64));
        let mut stmt = conn.prepare(&page_sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(sql_err)?);
        }
        Ok(SessionListPage {
            records,
            total: total.max(0) as usize,
        })
    }

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd, status
                   FROM sessions WHERE platform = ?1 ORDER BY last_activity DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![platform], row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// List all sessions bound to a workspace root through metadata_json.
    ///
    /// This is the DB-backed replacement for the deprecated filesystem
    /// `SessionStore` workspace namespace. Records without a
    /// `metadata_json.workspace_root` value are intentionally excluded.
    pub fn list_sessions_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd, status
                   FROM sessions
                  WHERE json_extract(metadata_json, '$.workspace_root') = ?1
                  ORDER BY last_activity DESC, session_id ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![workspace_root], row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// Search sessions using FTS5 full-text search.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub fn search_sessions(&self, query: &str, limit: usize) -> Result<Vec<SessionSearchResult>> {
        let conn = self.conn()?;

        // Join sessions with FTS5 and get snippets
        let sql = r"
            SELECT s.session_id, s.platform, s.chat_id, s.user_id,
                   s.created_at, s.last_activity, s.message_count,
                   snippet(sessions_fts, 4, '<mark>', '</mark>', '...', 32) as snippet
            FROM sessions s
            JOIN sessions_fts fts ON s.session_id = fts.session_id
            WHERE sessions_fts MATCH ?1
            ORDER BY rank
            LIMIT ?2
        ";

        let mut stmt = conn.prepare(sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, limit as i64], |row| {
                Ok(SessionSearchResult {
                    session_id: row.get(0)?,
                    platform: row.get(1)?,
                    chat_id: row.get(2)?,
                    user_id: row.get(3)?,
                    created_at: row.get(4)?,
                    last_activity: row.get(5)?,
                    message_count: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .map_err(sql_err)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(sql_err)?);
        }
        Ok(results)
    }

    /// Search sessions with platform filter.
    pub fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        let conn = self.conn()?;

        let sql = r"
            SELECT s.session_id, s.platform, s.chat_id, s.user_id,
                   s.created_at, s.last_activity, s.message_count,
                   snippet(sessions_fts, 4, '<mark>', '</mark>', '...', 32) as snippet
            FROM sessions s
            JOIN sessions_fts fts ON s.session_id = fts.session_id
            WHERE sessions_fts MATCH ?1 AND s.platform = ?2
            ORDER BY rank
            LIMIT ?3
        ";

        let mut stmt = conn.prepare(sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, platform, limit as i64], |row| {
                Ok(SessionSearchResult {
                    session_id: row.get(0)?,
                    platform: row.get(1)?,
                    chat_id: row.get(2)?,
                    user_id: row.get(3)?,
                    created_at: row.get(4)?,
                    last_activity: row.get(5)?,
                    message_count: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .map_err(sql_err)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(sql_err)?);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let conn = self.conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            r"INSERT OR IGNORE INTO session_memories (session_id, memory_id, created_at)
               VALUES (?1, ?2, ?3)",
            params![session_id, memory_id, now],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Return all memory IDs associated with `session_id`.
    pub fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT memory_id FROM session_memories WHERE session_id = ?1 ORDER BY created_at",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        let mut ids = Vec::new();
        for r in rows {
            ids.push(r.map_err(sql_err)?);
        }
        Ok(ids)
    }

    /// Remove the association between a session and a memory.
    pub fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM session_memories WHERE session_id = ?1 AND memory_id = ?2",
            params![session_id, memory_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Message persistence
    // -----------------------------------------------------------------------

    /// Insert a single message (INSERT OR REPLACE on the (session_id, sequence)
    /// unique constraint).
    pub fn insert_message(&self, msg: &SessionMessage) -> Result<()> {
        let conn = self.conn()?;
        let message_id = if msg.stable_message_id.trim().is_empty() {
            legacy_message_id(&msg.session_id, msg.sequence)
        } else {
            msg.stable_message_id.clone()
        };
        conn.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
               ON CONFLICT(session_id, sequence) DO UPDATE SET
                   role = excluded.role,
                   content_json = excluded.content_json,
                   blocks_count = excluded.blocks_count,
                   tool_use_id = excluded.tool_use_id,
                   tool_name = excluded.tool_name,
                   token_usage_json = excluded.token_usage_json,
                   created_at_ms = excluded.created_at_ms",
            params![
                message_id,
                msg.session_id,
                msg.sequence as i64,
                msg.role,
                msg.content_json,
                msg.blocks_count as i64,
                msg.tool_use_id,
                msg.tool_name,
                msg.token_usage_json,
                msg.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    pub fn commit_terminal_transcript_if_fenced(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> Result<SessionTerminalTranscriptReceipt> {
        validate_terminal_transcript(
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
        )?;
        validate_terminal_commit(request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let admission = query_input_admission(&tx, &request.session_id)?.ok_or_else(|| {
            SessionError::StaleExecutionFence(format!(
                "session `{}` no longer exists",
                request.session_id
            ))
        })?;
        let current = query_outbox(&tx, &request.fence.request_id)?.ok_or_else(|| {
            SessionError::StaleExecutionFence(format!(
                "input `{}` no longer exists",
                request.fence.request_id
            ))
        })?;
        if current.status == SessionRuntimeInputStatus::Completed
            && current.runtime_commit_cursor == Some(request.runtime_commit_cursor)
        {
            if current.session_id != request.session_id
                || current.message_id != request.ingress_message_id
                || current.turn_id != request.turn_id
                || current.sequence != request.fence.input_sequence
                || current.session_generation != request.fence.session_generation
                || current.claim_owner.as_deref() != Some(request.fence.claim_owner.as_str())
                || current.claim_token.as_deref() != Some(request.fence.claim_token.as_str())
                || current.claim_fence_epoch != Some(request.fence.claim_fence_epoch)
            {
                return Err(SessionError::StaleExecutionFence(format!(
                    "completed input `{}` identity does not match terminal replay",
                    request.fence.request_id
                )));
            }
            let messages = load_committed_terminal_transcript_tx(
                &tx,
                &request.terminal_message_id,
                &request.messages,
            )?;
            tx.commit().map_err(sql_err)?;
            return Ok(SessionTerminalTranscriptReceipt {
                messages,
                inserted: false,
                input: current,
            });
        }
        let fence_valid = current.session_id == request.session_id
            && current.message_id == request.ingress_message_id
            && current.turn_id == request.turn_id
            && current.sequence == request.fence.input_sequence
            && current.status == SessionRuntimeInputStatus::Running
            && current.session_generation == request.fence.session_generation
            && admission.generation == request.fence.session_generation
            && admission.open
            && current.claim_owner.as_deref() == Some(request.fence.claim_owner.as_str())
            && current.claim_token.as_deref() == Some(request.fence.claim_token.as_str())
            && current.claim_fence_epoch == Some(request.fence.claim_fence_epoch)
            && current
                .claim_expires_at_ms
                .is_some_and(|expires| expires > request.created_at_ms);
        if !fence_valid {
            return Err(SessionError::StaleExecutionFence(format!(
                "request={} generation={} claim_fence_epoch={} current_status={:?} current_revision={}",
                request.fence.request_id,
                request.fence.session_generation,
                request.fence.claim_fence_epoch,
                current.status,
                current.revision
            )));
        }
        let newest_pending_sequence = tx
            .query_row(
                r"SELECT MAX(sequence)
                     FROM session_runtime_outbox
                    WHERE session_id=?1 AND session_generation=?2
                      AND sequence>?3
                      AND status NOT IN (
                        'rejected_duplicate','rejected_policy','completed',
                        'supplemented','failed','cancelled','expired'
                      )
                      AND decision IN (
                        'supplement_current_turn',
                        'interrupt_and_replan',
                        'control_or_approval'
                      )",
                params![
                    request.session_id,
                    request.fence.session_generation as i64,
                    request.fence.input_sequence as i64,
                ],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(sql_err)?
            .map(|value| value.max(0) as usize);
        if newest_pending_sequence
            .is_some_and(|sequence| sequence > request.consumed_input_sequence)
        {
            return Err(SessionError::StaleExecutionFence(format!(
                "terminal input cursor {} is behind pending Session input {}",
                request.consumed_input_sequence,
                newest_pending_sequence.unwrap_or_default()
            )));
        }
        let consumed_request_ids = {
            let mut statement = tx
                .prepare(
                    r"SELECT request_id
                         FROM session_runtime_outbox
                        WHERE session_id=?1 AND session_generation=?2
                          AND sequence>?3 AND sequence<=?4
                          AND status IN ('accepted','classified','queued','reclassified')
                          AND decision IN (
                            'supplement_current_turn',
                            'interrupt_and_replan',
                            'control_or_approval'
                          )
                        ORDER BY sequence ASC",
                )
                .map_err(sql_err)?;
            let request_ids = statement
                .query_map(
                    params![
                        request.session_id,
                        request.fence.session_generation as i64,
                        request.fence.input_sequence as i64,
                        request.consumed_input_sequence as i64,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sql_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?;
            request_ids
        };
        for request_id in consumed_request_ids {
            let before = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "consumed Session input `{request_id}` disappeared during terminal commit"
                ))
            })?;
            let changed = tx
                .execute(
                    r"UPDATE session_runtime_outbox
                          SET status='supplemented', terminal_at_ms=?1,
                              claim_owner=NULL, claim_token=NULL,
                              claim_fence_epoch=NULL, claim_expires_at_ms=NULL,
                              failure_class=NULL, last_error=NULL,
                              updated_at_ms=?1, revision=revision+1
                        WHERE request_id=?2 AND revision=?3
                          AND status IN ('accepted','classified','queued','reclassified')",
                    params![
                        request.created_at_ms as i64,
                        request_id,
                        before.revision as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed != 1 {
                return Err(SessionError::StaleExecutionFence(format!(
                    "consumed Session input `{request_id}` changed during terminal commit"
                )));
            }
            let supplemented = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "supplemented Session input `{request_id}` disappeared"
                ))
            })?;
            append_outbox_history(
                &tx,
                &supplemented,
                "terminal_input_cursor_commit",
                Some(&request.fence.claim_owner),
                None,
                before.status.as_str(),
                SessionRuntimeInputStatus::Supplemented.as_str(),
                request.created_at_ms,
            )?;
            append_input_timeline_event(
                &tx,
                &request_from_outbox(&supplemented),
                &supplemented.session_id,
                supplemented.sequence,
                SessionRuntimeInputStatus::Supplemented.timeline_event_kind(),
                SessionRuntimeInputStatus::Supplemented,
                Some(&request.fence.claim_owner),
                None,
                request.created_at_ms,
            )?;
        }
        let (messages, inserted) = append_terminal_transcript_tx(
            &tx,
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
            request.created_at_ms,
        )?;
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status='completed', runtime_commit_cursor=?1,
                          claim_expires_at_ms=NULL,
                          terminal_at_ms=?2, failure_class=NULL, last_error=NULL,
                          updated_at_ms=?2, revision=revision+1
                    WHERE request_id=?3 AND sequence=?4 AND status='running'
                      AND session_generation=?5 AND claim_owner=?6
                      AND claim_token=?7 AND claim_fence_epoch=?8 AND revision=?9",
                params![
                    request.runtime_commit_cursor as i64,
                    request.created_at_ms as i64,
                    request.fence.request_id,
                    request.fence.input_sequence as i64,
                    request.fence.session_generation as i64,
                    request.fence.claim_owner,
                    request.fence.claim_token,
                    request.fence.claim_fence_epoch as i64,
                    current.revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::StaleExecutionFence(format!(
                "input `{}` changed during terminal commit",
                request.fence.request_id
            )));
        }
        let completed = query_outbox(&tx, &request.fence.request_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "completed input `{}` disappeared",
                request.fence.request_id
            ))
        })?;
        append_outbox_history(
            &tx,
            &completed,
            "terminal_commit",
            Some(&request.fence.claim_owner),
            None,
            SessionRuntimeInputStatus::Running.as_str(),
            SessionRuntimeInputStatus::Completed.as_str(),
            request.created_at_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&completed),
            &completed.session_id,
            completed.sequence,
            SessionRuntimeInputStatus::Completed.timeline_event_kind(),
            SessionRuntimeInputStatus::Completed,
            Some(&request.fence.claim_owner),
            None,
            request.created_at_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(SessionTerminalTranscriptReceipt {
            messages,
            inserted,
            input: completed,
        })
    }

    /// Insert multiple messages in a single transaction.
    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        {
            let mut stmt = tx
                .prepare(
                    r"INSERT INTO messages
                       (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                      ON CONFLICT(session_id, sequence) DO UPDATE SET
                          role = excluded.role,
                          content_json = excluded.content_json,
                          blocks_count = excluded.blocks_count,
                          tool_use_id = excluded.tool_use_id,
                          tool_name = excluded.tool_name,
                          token_usage_json = excluded.token_usage_json,
                          created_at_ms = excluded.created_at_ms",
                )
                .map_err(sql_err)?;
            for msg in messages {
                stmt.execute(params![
                    if msg.stable_message_id.trim().is_empty() {
                        legacy_message_id(&msg.session_id, msg.sequence)
                    } else {
                        msg.stable_message_id.clone()
                    },
                    msg.session_id,
                    msg.sequence as i64,
                    msg.role,
                    msg.content_json,
                    msg.blocks_count as i64,
                    msg.tool_use_id,
                    msg.tool_name,
                    msg.token_usage_json,
                    msg.created_at_ms as i64,
                ])
                .map_err(sql_err)?;
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn copy_session_messages_at_cutoff(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        source_message_count: usize,
    ) -> Result<usize> {
        if source_session_id.trim().is_empty()
            || target_session_id.trim().is_empty()
            || source_session_id == target_session_id
        {
            return Err(SessionError::Store(
                "branch copy requires distinct non-empty source and target sessions".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        for session_id in [source_session_id, target_session_id] {
            let exists = tx
                .query_row(
                    "SELECT 1 FROM sessions WHERE session_id = ?1",
                    params![session_id],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sql_err)?
                .is_some();
            if !exists {
                return Err(SessionError::Store(format!(
                    "branch session `{session_id}` does not exist"
                )));
            }
        }
        let target_count = tx
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![target_session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?;
        if target_count != 0 {
            return Err(SessionError::Store(format!(
                "branch target `{target_session_id}` already contains messages"
            )));
        }
        let copied = tx
            .execute(
                r"INSERT INTO messages
                    (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                     tool_use_id, tool_name, token_usage_json, created_at_ms)
                  SELECT 'branch:' || ?2 || ':' || stable_message_id,
                         ?2, sequence, role, content_json, blocks_count,
                         tool_use_id, tool_name, token_usage_json, created_at_ms
                    FROM messages
                   WHERE session_id = ?1 AND sequence < ?3
                   ORDER BY sequence",
                params![
                    source_session_id,
                    target_session_id,
                    source_message_count as i64
                ],
            )
            .map_err(sql_err)?;
        let last_created_at = tx
            .query_row(
                "SELECT COALESCE(MAX(created_at_ms), 0) FROM messages WHERE session_id = ?1",
                params![target_session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?
            .max(0) as u64;
        refresh_session_message_summary_tx(&tx, target_session_id, last_created_at)?;
        refresh_session_usage_summary_tx(&tx, target_session_id)?;
        tx.commit().map_err(sql_err)?;
        Ok(copied)
    }

    pub fn branch_session_at_cutoff(
        &self,
        request: &SessionBranchRequest,
    ) -> Result<SessionBranchResult> {
        validate_mission_outbox_request(&request.mission_outbox)?;
        if request.operation_id.trim().is_empty()
            || request.source_session_id.trim().is_empty()
            || request.target.session_id.trim().is_empty()
            || request.source_session_id == request.target.session_id
            || request.mission_outbox.session_id != request.target.session_id
        {
            return Err(SessionError::Store(
                "branch requires distinct source/target identities and a target-bound mission intent"
                    .to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let source_exists = tx
            .query_row(
                "SELECT 1 FROM sessions WHERE session_id = ?1",
                params![request.source_session_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_err)?
            .is_some();
        if !source_exists {
            return Err(SessionError::Store(format!(
                "branch source `{}` does not exist",
                request.source_session_id
            )));
        }
        if let Some(activation) = query_branch_activation(&tx, &request.operation_id)? {
            if activation.source_session_id != request.source_session_id
                || activation.target_session_id != request.target.session_id
                || activation.source_message_count != request.source_message_count
            {
                return Err(SessionError::Store(format!(
                    "branch operation `{}` is bound to another source/cutoff/target identity",
                    request.operation_id
                )));
            }
            let target = tx
                .query_row(
                    r"SELECT session_id, platform, chat_id, user_id, model,
                              created_at, last_activity, message_count, reset_policy,
                              metadata_json, input_tokens, output_tokens,
                              estimated_cost_usd, status
                         FROM sessions WHERE session_id=?1",
                    params![request.target.session_id],
                    row_to_record,
                )
                .optional()
                .map_err(sql_err)?
                .ok_or_else(|| {
                    SessionError::Store(format!(
                        "branch operation `{}` has no durable target",
                        request.operation_id
                    ))
                })?;
            tx.commit().map_err(sql_err)?;
            return Ok(SessionBranchResult {
                target,
                copied_message_count: activation.source_message_count,
                source_message_count: activation.source_message_count,
                activation,
            });
        }
        let target_exists = tx
            .query_row(
                "SELECT 1 FROM sessions WHERE session_id = ?1",
                params![request.target.session_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_err)?
            .is_some();
        if target_exists {
            return Err(SessionError::Store(format!(
                "branch target `{}` already exists",
                request.target.session_id
            )));
        }
        let source_count = tx
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![request.source_session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?;
        let source_count = usize::try_from(source_count).map_err(|_| {
            SessionError::Store("branch source message count exceeds usize".to_string())
        })?;
        if request.source_message_count > source_count {
            return Err(SessionError::Store(format!(
                "branch cutoff {} exceeds source message count {source_count}",
                request.source_message_count
            )));
        }
        let cutoff = request.source_message_count;

        tx.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9, 0, 0, 0, ?10, ?11, ?12)",
            params![
                request.target.session_id,
                request.target.platform,
                request.target.chat_id,
                request.target.user_id,
                request.target.model,
                request.target.created_at,
                request.target.last_activity,
                request.target.reset_policy,
                request.target.metadata_json,
                request.target.status,
                iso_to_ms(&request.target.created_at),
                iso_to_ms(&request.target.last_activity),
            ],
        )
        .map_err(sql_err)?;
        let copied = tx
            .execute(
                r"INSERT INTO messages
                    (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                     tool_use_id, tool_name, token_usage_json, created_at_ms)
                  SELECT 'branch:' || ?2 || ':' || stable_message_id,
                         ?2, sequence, role, content_json, blocks_count,
                         tool_use_id, tool_name, token_usage_json, created_at_ms
                    FROM messages
                   WHERE session_id = ?1 AND sequence < ?3
                   ORDER BY sequence",
                params![
                    request.source_session_id,
                    request.target.session_id,
                    i64::try_from(cutoff).map_err(|_| SessionError::Store(
                        "branch cutoff exceeds SQLite i64 range".to_string()
                    ))?
                ],
            )
            .map_err(sql_err)?;
        let last_created_at = tx
            .query_row(
                "SELECT COALESCE(MAX(created_at_ms), 0) FROM messages WHERE session_id = ?1",
                params![request.target.session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?
            .max(0) as u64;
        refresh_session_message_summary_tx(&tx, &request.target.session_id, last_created_at)?;
        refresh_session_usage_summary_tx(&tx, &request.target.session_id)?;
        insert_mission_outbox(&tx, &request.mission_outbox)?;

        for (session_id, event_type, event_json) in [
            (
                request.source_session_id.as_str(),
                "SessionBranched",
                request.source_event_json.as_str(),
            ),
            (
                request.target.session_id.as_str(),
                "BranchCreated",
                request.target_event_json.as_str(),
            ),
        ] {
            let event_json = branch_event_json(event_json, copied, cutoff)?;
            let sequence: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                    params![session_id],
                    |row| row.get(0),
                )
                .map_err(sql_err)?;
            let sequence_usize = usize::try_from(sequence).map_err(|_| {
                SessionError::Store("branch event sequence exceeds usize".to_string())
            })?;
            let event = SessionEvent {
                session_id: session_id.to_string(),
                event_type: event_type.to_string(),
                event_json,
                sequence: sequence_usize,
                created_at_ms: request.created_at_ms,
            };
            let event_json = event_json_with_allocated_sequence(&event, sequence_usize)?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    session_id,
                    event_type,
                    event_json,
                    sequence,
                    i64::try_from(request.created_at_ms).map_err(|_| SessionError::Store(
                        "branch timestamp exceeds SQLite i64 range".to_string()
                    ))?,
                ],
            )
            .map_err(sql_err)?;
        }
        tx.execute(
            r"INSERT INTO session_branch_activations
                (operation_id, source_session_id, target_session_id,
                 source_message_count, phase, created_at_ms, updated_at_ms,
                 last_error, revision)
               VALUES (?1, ?2, ?3, ?4, 'branch_committed', ?5, ?5, NULL, 0)",
            params![
                request.operation_id,
                request.source_session_id,
                request.target.session_id,
                cutoff as i64,
                request.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        let activation = query_branch_activation(&tx, &request.operation_id)?.ok_or_else(|| {
            SessionError::Store("branch transaction produced no activation receipt".to_string())
        })?;
        tx.commit().map_err(sql_err)?;

        let mut target = request.target.clone();
        target.message_count = i64::try_from(copied).map_err(|_| {
            SessionError::Store("branch message count exceeds i64 range".to_string())
        })?;
        Ok(SessionBranchResult {
            target,
            copied_message_count: copied,
            source_message_count: cutoff,
            activation,
        })
    }

    pub fn get_session_branch_activation(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionBranchActivation>> {
        let conn = self.conn()?;
        query_branch_activation(&conn, operation_id)
    }

    pub fn list_recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionBranchActivation>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT operation_id, source_session_id, target_session_id,
                          source_message_count, phase, created_at_ms, updated_at_ms,
                          last_error, revision
                     FROM session_branch_activations
                    WHERE phase != 'activated'
                    ORDER BY updated_at_ms ASC, operation_id ASC
                    LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(params![limit as i64], row_to_branch_activation)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn transition_session_branch_activation(
        &self,
        transition: &SessionBranchActivationTransition,
    ) -> Result<SessionBranchActivation> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_branch_activation(&tx, &transition.operation_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "Session branch activation `{}` does not exist",
                transition.operation_id
            ))
        })?;
        transition.validate(&current)?;
        let changed = tx
            .execute(
                r"UPDATE session_branch_activations
                     SET phase=?1, updated_at_ms=?2, last_error=?3,
                         revision=revision+1
                   WHERE operation_id=?4 AND phase=?5 AND revision=?6",
                params![
                    transition.next_phase.as_str(),
                    transition.updated_at_ms as i64,
                    transition.error,
                    transition.operation_id,
                    transition.expected_phase.as_str(),
                    transition.expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "Session branch activation `{}` changed during transition",
                transition.operation_id
            )));
        }
        let activation =
            query_branch_activation(&tx, &transition.operation_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "Session branch activation `{}` disappeared after transition",
                    transition.operation_id
                ))
            })?;
        tx.commit().map_err(sql_err)?;
        Ok(activation)
    }

    /// Atomically persist a stable user message and its Runtime ingress outbox row.
    ///
    /// Reusing `request_id` with identical identities is idempotent. Reusing it
    /// for another turn/message is rejected rather than silently overwriting data.
    pub fn append_message_with_runtime_outbox(
        &self,
        message: &SessionMessage,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        validate_outbox_identity(message, request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;

        if let Some(existing) = query_outbox(&tx, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.turn_id == request.turn_id
                && existing.message_id == request.message_id
                && existing.session_id == message.session_id
                && existing.sequence == message.sequence
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(SessionError::Store(format!(
                "outbox request_id `{}` is already bound to another message",
                request.request_id
            )));
        }
        require_input_admission(&tx, &message.session_id, request.session_generation)?;

        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                request.message_id,
                message.session_id,
                message.sequence as i64,
                message.role,
                message.content_json,
                message.blocks_count as i64,
                message.tool_use_id,
                message.tool_name,
                message.token_usage_json,
                message.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, &message.session_id, message.created_at_ms)?;
        let stored =
            insert_runtime_input_outbox(&tx, &message.session_id, message.sequence, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(stored)
    }

    /// Persist an ingress message and Runtime request while allocating the
    /// session-local message sequence inside the same write transaction.
    ///
    /// Surface and Gateway callers must use this entry point for live input;
    /// accepting a caller-computed sequence would create a race between
    /// concurrent surfaces writing to the same session.
    pub fn append_ingress_with_runtime_outbox(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        validate_runtime_input_request(request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        if let Some(existing) = query_outbox(&tx, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.session_id == session_id
                && existing.message_id == request.message_id
                && existing.turn_id == request.turn_id
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(SessionError::Store(format!(
                "outbox request `{}` conflicts with its committed ingress",
                request.request_id
            )));
        }
        require_input_admission(&tx, session_id, request.session_generation)?;
        let sequence = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)? as usize;
        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![
                request.message_id,
                session_id,
                sequence as i64,
                role,
                content_json.unwrap_or("[]"),
                created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, session_id, created_at_ms)?;
        let stored = insert_runtime_input_outbox(&tx, session_id, sequence, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(stored)
    }

    /// Claim due ingress rows under a renewable lease.
    pub fn claim_session_runtime_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(SessionError::Store(
                "outbox claim requires worker_id, positive lease and positive limit".to_string(),
            ));
        }
        let claim_expires_at_ms = now_ms.saturating_add(lease_ms);
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let candidates = {
            let mut stmt = tx
                .prepare(
                    r"WITH ordered AS (
                           SELECT o.request_id, o.revision, o.status, o.session_id,
                                  o.session_generation, o.sequence, o.next_attempt_at_ms,
                                  o.claim_expires_at_ms,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY o.session_id, o.session_generation
                                      ORDER BY o.sequence ASC, o.request_id ASC
                                  ) AS session_rank
                             FROM session_runtime_outbox o
                             JOIN sessions s ON s.session_id = o.session_id
                            WHERE o.status IN (
                                      'accepted', 'classified', 'queued', 'claimed',
                                      'running', 'reclassified'
                                  )
                              AND o.session_generation = s.input_generation
                              AND s.input_admission_open = 1
                       )
                       SELECT request_id, revision, status, session_id, session_generation
                         FROM ordered candidate
                        WHERE session_rank = 1
                          AND (
                              (status IN ('queued', 'reclassified')
                                  AND next_attempt_at_ms <= ?1)
                              OR (status IN ('claimed', 'running')
                                  AND claim_expires_at_ms <= ?1)
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM session_runtime_outbox held
                               WHERE held.session_id = candidate.session_id
                                 AND held.session_generation = candidate.session_generation
                                 AND held.request_id != candidate.request_id
                                 AND held.status IN ('claimed', 'running')
                                 AND held.claim_expires_at_ms > ?1
                          )
                        ORDER BY next_attempt_at_ms ASC, sequence ASC, request_id ASC
                        LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![now_ms as i64, limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?.max(0) as u64,
                    ))
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };

        let mut claimed = Vec::with_capacity(candidates.len());
        for (request_id, revision, from_status, session_id, session_generation) in candidates {
            let claim_token = uuid::Uuid::new_v4().to_string();
            let changed = tx
                .execute(
                    r"UPDATE session_runtime_outbox
                          SET status = 'claimed',
                              attempts = attempts + 1,
                              claim_owner = ?1,
                              claim_token = ?2,
                              claim_fence_epoch = revision + 1,
                              claim_expires_at_ms = ?3,
                              updated_at_ms = ?4,
                              revision = revision + 1
                        WHERE request_id = ?5 AND revision = ?6
                          AND session_id = ?7 AND session_generation = ?8
                          AND (
                              (status IN ('queued', 'reclassified')
                                  AND next_attempt_at_ms <= ?4)
                              OR (status IN ('claimed', 'running')
                                  AND claim_expires_at_ms <= ?4)
                          )
                          AND EXISTS (
                              SELECT 1 FROM sessions
                               WHERE sessions.session_id = ?7
                                 AND sessions.input_generation = ?8
                                 AND sessions.input_admission_open = 1
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM session_runtime_outbox earlier
                               WHERE earlier.session_id = ?7
                                 AND earlier.session_generation = ?8
                                 AND earlier.sequence < session_runtime_outbox.sequence
                                 AND earlier.status IN (
                                     'accepted', 'classified', 'queued', 'claimed',
                                     'running', 'reclassified'
                                 )
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM session_runtime_outbox held
                               WHERE held.session_id = ?7
                                 AND held.session_generation = ?8
                                 AND held.request_id != ?5
                                 AND held.status IN ('claimed', 'running')
                                 AND held.claim_expires_at_ms > ?4
                          )",
                    params![
                        worker_id,
                        claim_token,
                        claim_expires_at_ms as i64,
                        now_ms as i64,
                        request_id,
                        revision as i64,
                        session_id,
                        session_generation as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed == 1 {
                let record = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                    SessionError::Store(format!("claimed outbox `{request_id}` disappeared"))
                })?;
                append_outbox_history(
                    &tx,
                    &record,
                    if matches!(from_status.as_str(), "claimed" | "running") {
                        "reclaim"
                    } else {
                        "claim"
                    },
                    Some(worker_id),
                    Some(record.claim_token.as_deref().unwrap_or_default()),
                    &from_status,
                    SessionRuntimeInputStatus::Claimed.as_str(),
                    now_ms,
                )?;
                claimed.push(record);
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(claimed)
    }

    /// Move a claimed input into Runtime execution. This is a separate fenced
    /// transition so terminal writes can prove that execution actually began.
    #[allow(clippy::too_many_arguments)]
    pub fn mark_session_runtime_outbox_running(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        self.transition_owned_outbox(
            request_id,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Claimed],
            |tx, current| {
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = 'running', updated_at_ms = ?1,
                              revision = revision + 1
                        WHERE request_id = ?2 AND status = 'claimed'
                          AND session_generation = ?3 AND claim_owner = ?4
                          AND claim_token = ?5 AND revision = ?6",
                    params![
                        now_ms as i64,
                        request_id,
                        session_generation as i64,
                        worker_id,
                        claim_token,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok(("start", SessionRuntimeInputStatus::Running, current.status))
            },
        )
    }

    /// Ack a running ingress row after Runtime has durably committed it.
    #[allow(clippy::too_many_arguments)]
    pub fn ack_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        terminal_status: SessionRuntimeInputStatus,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if !matches!(
            terminal_status,
            SessionRuntimeInputStatus::Completed
                | SessionRuntimeInputStatus::Supplemented
                | SessionRuntimeInputStatus::Cancelled
        ) {
            return Err(SessionError::Store(
                "ack terminal status must be completed, supplemented, or cancelled".to_string(),
            ));
        }
        self.transition_owned_outbox(
            request_id,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Running],
            |tx, current| {
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = ?1, runtime_commit_cursor = ?2,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              claim_token = NULL, claim_fence_epoch = NULL,
                              terminal_at_ms = ?3,
                              failure_class = NULL, last_error = NULL,
                              updated_at_ms = ?3, revision = revision + 1
                        WHERE request_id = ?4 AND status = 'running'
                          AND session_generation = ?5 AND claim_owner = ?6
                          AND claim_token = ?7 AND revision = ?8",
                    params![
                        terminal_status.as_str(),
                        runtime_commit_cursor as i64,
                        now_ms as i64,
                        request_id,
                        session_generation as i64,
                        worker_id,
                        claim_token,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok(("ack", terminal_status, current.status))
            },
        )
    }

    /// Extend a live ingress claim. The revision is advanced so stale workers
    /// can no longer acknowledge or fail work after ownership has moved.
    #[allow(clippy::too_many_arguments)]
    pub fn renew_session_runtime_outbox_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if lease_ms == 0 {
            return Err(SessionError::Store(
                "outbox lease renewal requires a positive lease".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("session runtime outbox `{request_id}` not found"))
        })?;
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if !current.status.holds_claim()
            || current.session_generation != session_generation
            || admission.generation != session_generation
            || !admission.open
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.claim_token.as_deref() != Some(claim_token)
            || current.revision != expected_revision
            || current
                .claim_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
        {
            return Err(SessionError::Store(format!(
                "stale outbox lease renewal for `{request_id}`"
            )));
        }
        let expires_at = now_ms.saturating_add(lease_ms);
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET claim_expires_at_ms = ?1, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND status = 'claimed'
                      AND session_generation = ?4 AND claim_owner = ?5
                      AND claim_token = ?6 AND revision = ?7",
                params![
                    expires_at as i64,
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    worker_id,
                    claim_token,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        let changed = if changed == 0 && current.status == SessionRuntimeInputStatus::Running {
            tx.execute(
                r"UPDATE session_runtime_outbox
                      SET claim_expires_at_ms = ?1, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND status = 'running'
                      AND session_generation = ?4 AND claim_owner = ?5
                      AND claim_token = ?6 AND revision = ?7",
                params![
                    expires_at as i64,
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    worker_id,
                    claim_token,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?
        } else {
            changed
        };
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "outbox lease for `{request_id}` changed during renewal"
            )));
        }
        let renewed = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("renewed outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &renewed,
            "renew_lease",
            Some(worker_id),
            None,
            current.status.as_str(),
            current.status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(renewed)
    }

    /// Classify a failed claim and either schedule retry or block it.
    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        self.transition_owned_outbox(
            request_id,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
            |tx, current| {
                let retry = failure_class == OutboxFailureClass::Retryable
                    && current.attempts < max_attempts.max(1);
                let next_status = if retry {
                    SessionRuntimeInputStatus::Queued
                } else if matches!(
                    failure_class,
                    OutboxFailureClass::AuthorizationBlocked | OutboxFailureClass::CorruptPayload
                ) {
                    SessionRuntimeInputStatus::Blocked
                } else {
                    SessionRuntimeInputStatus::Failed
                };
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = ?1, next_attempt_at_ms = ?2,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              claim_token = NULL, claim_fence_epoch = NULL,
                              terminal_at_ms = ?3,
                              failure_class = ?4, last_error = ?5,
                              updated_at_ms = ?6, revision = revision + 1
                        WHERE request_id = ?7
                          AND status IN ('claimed', 'running')
                          AND session_generation = ?8 AND claim_owner = ?9
                          AND claim_token = ?10 AND revision = ?11",
                    params![
                        next_status.as_str(),
                        if retry { retry_at_ms } else { now_ms } as i64,
                        if next_status == SessionRuntimeInputStatus::Failed {
                            Some(now_ms as i64)
                        } else {
                            None
                        },
                        failure_class.as_str(),
                        error,
                        now_ms as i64,
                        request_id,
                        session_generation as i64,
                        worker_id,
                        claim_token,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok((
                    if retry {
                        "retry"
                    } else if next_status == SessionRuntimeInputStatus::Blocked {
                        "block"
                    } else {
                        "fail"
                    },
                    next_status,
                    current.status,
                ))
            },
        )
    }

    /// Reclassify worker-owned supplement/control work when its target turn is
    /// no longer live. Decision replacement, claim release, queue visibility,
    /// history, and Session timeline are committed atomically.
    #[allow(clippy::too_many_arguments)]
    pub fn requeue_claimed_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let candidate = SessionRuntimeOutboxRequest {
            input_id: "validation".to_string(),
            request_id: request_id.to_string(),
            turn_id: "validation".to_string(),
            message_id: "validation".to_string(),
            session_generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: classification_json.map(str::to_string),
            created_at_ms: now_ms,
            runtime_options_json: None,
        };
        validate_runtime_input_request(&candidate)?;
        if worker_id.trim().is_empty() || claim_token.trim().is_empty() || reason.trim().is_empty()
        {
            return Err(SessionError::Store(
                "claimed input requeue requires worker, claim token, and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| SessionError::Store(format!("outbox `{request_id}` not found")))?;
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if !current.status.holds_claim()
            || current.session_generation != session_generation
            || admission.generation != session_generation
            || !admission.open
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.claim_token.as_deref() != Some(claim_token)
            || current.revision != expected_revision
            || current
                .claim_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
        {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` generation/token/status/revision fence mismatch"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET decision = ?1, target_turn_id = ?2, classification_json = ?3,
                          status = 'reclassified', next_attempt_at_ms = ?4,
                          claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL,
                          claim_expires_at_ms = NULL, failure_class = NULL,
                          last_error = NULL, terminal_at_ms = NULL,
                          updated_at_ms = ?4, revision = revision + 1
                    WHERE request_id = ?5
                      AND session_generation = ?6
                      AND claim_owner = ?7 AND claim_token = ?8
                      AND revision = ?9 AND status IN ('claimed', 'running')",
                params![
                    input_decision_as_str(decision),
                    target_turn_id,
                    classification_json,
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    worker_id,
                    claim_token,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` changed during claimed requeue"
            )));
        }
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("requeued outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "owner_reclassify_requeue",
            Some(worker_id),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Reclassified.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(worker_id),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Manually release a blocked row while retaining attempts and audit history.
    pub fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "manual outbox retry requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| SessionError::Store(format!("outbox `{request_id}` not found")))?;
        if current.status != SessionRuntimeInputStatus::Blocked
            || current.session_generation != session_generation
            || current.revision != expected_revision
        {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` is not blocked at revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'queued', next_attempt_at_ms = ?1,
                          claim_owner = NULL, claim_expires_at_ms = NULL,
                          claim_token = NULL, claim_fence_epoch = NULL,
                          terminal_at_ms = NULL,
                          failure_class = NULL, last_error = NULL, updated_at_ms = ?1,
                          revision = revision + 1
                    WHERE request_id = ?2 AND status = 'blocked'
                      AND session_generation = ?3 AND revision = ?4",
                params![
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    expected_revision as i64
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` changed during manual retry"
            )));
        }
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("retried outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "manual_retry",
            Some(actor),
            Some(reason),
            SessionRuntimeInputStatus::Blocked.as_str(),
            SessionRuntimeInputStatus::Queued.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Cancel a non-terminal durable input. Incrementing the revision and
    /// clearing the claim token immediately fences any in-flight worker.
    pub fn cancel_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "session input cancellation requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox_by_input_id(&tx, input_id)?
            .ok_or_else(|| SessionError::Store(format!("session input `{input_id}` not found")))?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || current.status.is_terminal()
        {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` cannot be cancelled at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'cancelled', claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL,
                          claim_expires_at_ms = NULL, last_error = ?1,
                          terminal_at_ms = ?2, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE input_id = ?3 AND session_generation = ?4
                      AND revision = ?5
                      AND status NOT IN (
                          'rejected_duplicate', 'rejected_policy',
                          'completed', 'supplemented', 'failed', 'cancelled', 'expired'
                      )",
                params![
                    reason,
                    now_ms as i64,
                    input_id,
                    session_generation as i64,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` changed during cancellation"
            )));
        }
        let updated = query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
            SessionError::Store(format!("cancelled session input `{input_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "cancel",
            Some(actor),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Cancelled.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.cancelled.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Replace a queued classification without creating another input source
    /// of truth. Claimed/running rows must first be cancelled by their owner.
    #[allow(clippy::too_many_arguments)]
    pub fn reclassify_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let candidate = SessionRuntimeOutboxRequest {
            input_id: input_id.to_string(),
            request_id: "validation".to_string(),
            turn_id: "validation".to_string(),
            message_id: "validation".to_string(),
            session_generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: classification_json.map(str::to_string),
            created_at_ms: now_ms,
            runtime_options_json: None,
        };
        validate_runtime_input_request(&candidate)?;
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "session input reclassification requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        require_input_admission(
            &tx,
            &query_outbox_by_input_id(&tx, input_id)?
                .ok_or_else(|| {
                    SessionError::Store(format!("session input `{input_id}` not found"))
                })?
                .session_id,
            session_generation,
        )?;
        let current = query_outbox_by_input_id(&tx, input_id)?
            .ok_or_else(|| SessionError::Store(format!("session input `{input_id}` not found")))?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || !matches!(
                current.status,
                SessionRuntimeInputStatus::Accepted
                    | SessionRuntimeInputStatus::Classified
                    | SessionRuntimeInputStatus::Queued
                    | SessionRuntimeInputStatus::Reclassified
                    | SessionRuntimeInputStatus::Blocked
            )
        {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` is not reclassifiable at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET decision = ?1, target_turn_id = ?2, classification_json = ?3,
                          status = 'reclassified', next_attempt_at_ms = ?4,
                          failure_class = NULL, last_error = NULL, terminal_at_ms = NULL,
                          claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL, claim_expires_at_ms = NULL,
                          updated_at_ms = ?4, revision = revision + 1
                    WHERE input_id = ?5 AND session_generation = ?6
                      AND revision = ?7
                      AND status IN ('accepted', 'classified', 'queued', 'reclassified', 'blocked')",
                params![
                    input_decision_as_str(decision),
                    target_turn_id,
                    classification_json,
                    now_ms as i64,
                    input_id,
                    session_generation as i64,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` changed during reclassification"
            )));
        }
        let updated = query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "reclassified session input `{input_id}` disappeared"
            ))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "reclassify",
            Some(actor),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Reclassified.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    pub fn get_session_input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionInputAdmission>> {
        let conn = self.conn()?;
        query_input_admission(&conn, session_id)
    }

    /// Close admission and revoke the current generation atomically. Every
    /// active row from the revoked generation becomes expired in the same
    /// transaction, so stale workers can never commit terminal state.
    pub fn close_session_input_admission(
        &self,
        session_id: &str,
        expected_generation: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission> {
        self.advance_session_input_generation(
            session_id,
            expected_generation,
            false,
            actor,
            reason,
            now_ms,
        )
    }

    /// Advance Session authority and choose whether the new generation accepts
    /// ingress. This is used by branch/reopen flows after their durable
    /// lifecycle mutation has selected the new owner.
    pub fn advance_session_input_generation(
        &self,
        session_id: &str,
        expected_generation: u64,
        open: bool,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "session generation advance requires actor and reason".to_string(),
            ));
        }
        let next_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| SessionError::Store("session generation overflow".to_string()))?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_input_admission(&tx, session_id)?
            .ok_or_else(|| SessionError::Store(format!("session `{session_id}` not found")))?;
        if current.generation != expected_generation {
            return Err(SessionError::Store(format!(
                "session `{session_id}` generation changed from expected {expected_generation}"
            )));
        }
        let active = {
            let mut stmt = tx
                .prepare(
                    r"SELECT request_id FROM session_runtime_outbox
                       WHERE session_id = ?1 AND session_generation = ?2
                         AND status IN (
                             'accepted', 'classified', 'queued', 'claimed',
                             'running', 'reclassified', 'blocked'
                         )
                       ORDER BY sequence ASC, request_id ASC",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![session_id, expected_generation as i64], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };
        let changed = tx
            .execute(
                r"UPDATE sessions
                      SET input_generation = ?1, input_admission_open = ?2,
                          updated_at_ms = MAX(updated_at_ms, ?3)
                    WHERE session_id = ?4 AND input_generation = ?5",
                params![
                    next_generation as i64,
                    open,
                    now_ms as i64,
                    session_id,
                    expected_generation as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session `{session_id}` generation changed during advance"
            )));
        }
        for request_id in active {
            let before = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "outbox `{request_id}` disappeared during generation advance"
                ))
            })?;
            tx.execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'expired', claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL,
                          claim_expires_at_ms = NULL, last_error = ?1,
                          terminal_at_ms = ?2, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND session_generation = ?4
                      AND revision = ?5",
                params![
                    reason,
                    now_ms as i64,
                    request_id,
                    expected_generation as i64,
                    before.revision as i64,
                ],
            )
            .map_err(sql_err)?;
            let expired = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!("expired outbox `{request_id}` disappeared"))
            })?;
            append_outbox_history(
                &tx,
                &expired,
                "generation_expire",
                Some(actor),
                Some(reason),
                before.status.as_str(),
                SessionRuntimeInputStatus::Expired.as_str(),
                now_ms,
            )?;
        }
        let admission = query_input_admission(&tx, session_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "session `{session_id}` disappeared after generation advance"
            ))
        })?;
        append_admission_timeline_event(
            &tx,
            session_id,
            expected_generation,
            &admission,
            actor,
            reason,
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(admission)
    }

    pub fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        query_outbox(&conn, request_id)
    }

    pub fn get_session_runtime_outbox_by_input_id(
        &self,
        input_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        query_outbox_by_input_id(&conn, input_id)
    }

    /// Bounded durable ingress history for one Session.  Runtime/Suface
    /// observers use it only to recover execution identity and ingress state;
    /// detailed execution facts remain owned by Runtime's graph projection.
    pub fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch
                    FROM session_runtime_outbox
                   WHERE session_id = ?1
                   ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                   LIMIT ?2",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, limit.clamp(1, 500) as i64],
                row_to_outbox,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Fetch a bounded execution history for several Sessions with one query.
    ///
    /// The row-number bound is per Session, so a busy Session cannot starve the
    /// remaining page. This is the durable recovery path for catalog views;
    /// active execution truth is reconciled from Runtime memory afterwards.
    pub fn session_runtime_outbox_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let session_ids_json = serde_json::to_string(session_ids)
            .map_err(|error| SessionError::Store(error.to_string()))?;
        let mut stmt = conn
            .prepare(
                r"WITH ranked AS (
                    SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                           session_generation, decision, target_turn_id, classification_json,
                           status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                           claim_owner, claim_token, claim_expires_at_ms, failure_class,
                           last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                           runtime_options_json, claim_fence_epoch,
                           ROW_NUMBER() OVER (
                               PARTITION BY session_id
                               ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                           ) AS row_number
                      FROM session_runtime_outbox
                     WHERE session_id IN (SELECT value FROM json_each(?1))
                )
                SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                       session_generation, decision, target_turn_id, classification_json,
                       status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                       claim_owner, claim_token, claim_expires_at_ms, failure_class,
                       last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                       runtime_options_json, claim_fence_epoch
                  FROM ranked
                 WHERE row_number <= ?2
                 ORDER BY session_id ASC, updated_at_ms DESC, sequence DESC, request_id DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_ids_json, per_session_limit.clamp(1, 500) as i64],
                row_to_outbox,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Bounded durable work that may still need observer recovery after a
    /// Gateway restart.  Materialized ingress is terminal for this carrier;
    /// the terminal transcript/outbox remains the source for reply recovery.
    pub fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch
                    FROM session_runtime_outbox
                   WHERE status NOT IN (
                       'rejected_duplicate', 'rejected_policy',
                       'completed', 'supplemented', 'failed', 'cancelled', 'expired'
                   )
                   ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                   LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![limit.clamp(1, 500) as i64], row_to_outbox)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn session_runtime_outbox_health(&self) -> Result<SessionRuntimeOutboxHealth> {
        let conn = self.conn()?;
        let mut health = SessionRuntimeOutboxHealth::default();
        let mut stmt = conn
            .prepare("SELECT status, COUNT(*) FROM session_runtime_outbox GROUP BY status")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_err)?;
        for row in rows {
            let (status, count) = row.map_err(sql_err)?;
            let count = count as usize;
            match SessionRuntimeInputStatus::parse(&status).map_err(sql_err)? {
                SessionRuntimeInputStatus::Accepted => health.accepted = count,
                SessionRuntimeInputStatus::Classified => health.classified = count,
                SessionRuntimeInputStatus::Queued => health.queued = count,
                SessionRuntimeInputStatus::RejectedDuplicate => {
                    health.rejected_duplicate = count;
                }
                SessionRuntimeInputStatus::RejectedPolicy => health.rejected_policy = count,
                SessionRuntimeInputStatus::Claimed => health.claimed = count,
                SessionRuntimeInputStatus::Running => health.running = count,
                SessionRuntimeInputStatus::Reclassified => health.reclassified = count,
                SessionRuntimeInputStatus::Completed => health.completed = count,
                SessionRuntimeInputStatus::Supplemented => health.supplemented = count,
                SessionRuntimeInputStatus::Failed => health.failed = count,
                SessionRuntimeInputStatus::Blocked => health.blocked = count,
                SessionRuntimeInputStatus::Cancelled => health.cancelled = count,
                SessionRuntimeInputStatus::Expired => health.expired = count,
            }
        }
        health.runnable_depth = health
            .accepted
            .saturating_add(health.classified)
            .saturating_add(health.queued)
            .saturating_add(health.claimed)
            .saturating_add(health.running)
            .saturating_add(health.reclassified);
        health.oldest_runnable_created_at_ms = conn
            .query_row(
                "SELECT MIN(created_at_ms) FROM session_runtime_outbox
                  WHERE status IN ('accepted','classified','queued','claimed','running','reclassified')",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(sql_err)?
            .map(|value| value.max(0) as u64);
        Ok(health)
    }

    /// Return blocked ingress rows for operational inspection. The bounded
    /// result is ordered deterministically so operators can retry the oldest
    /// poison item first.
    pub fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch
                    FROM session_runtime_outbox
                   WHERE status = 'blocked'
                   ORDER BY updated_at_ms ASC, sequence ASC, request_id ASC
                   LIMIT ?1",
            )
            .map_err(sql_err)?;
        let records = stmt
            .query_map(params![limit.clamp(1, 500) as i64], row_to_outbox)
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err)?;
        Ok(records)
    }

    /// Claim due Session -> Mission lifecycle intents for one Runtime
    /// workspace. A gateway serving another workspace can never materialize
    /// these rows because the workspace key is part of the claim predicate.
    pub fn claim_session_mission_outbox(
        &self,
        workspace_key: &str,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionMissionOutboxRecord>> {
        if workspace_key.trim().is_empty()
            || worker_id.trim().is_empty()
            || lease_ms == 0
            || limit == 0
        {
            return Err(SessionError::Store(
                "mission outbox claim requires workspace, worker, positive lease and positive limit"
                    .to_string(),
            ));
        }
        let expires_at = now_ms.saturating_add(lease_ms);
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let candidates = {
            let mut stmt = tx
                .prepare(
                    r"SELECT request_id, revision, status
                        FROM session_mission_outbox
                       WHERE workspace_key = ?1
                         AND ((status IN ('pending', 'retry_scheduled') AND next_attempt_at_ms <= ?2)
                           OR (status = 'claimed' AND claim_expires_at_ms <= ?2))
                       ORDER BY next_attempt_at_ms ASC, created_at_ms ASC, request_id ASC
                       LIMIT ?3",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![workspace_key, now_ms as i64, limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };
        let mut claimed = Vec::with_capacity(candidates.len());
        for (request_id, revision, from_status) in candidates {
            let changed = tx
                .execute(
                    r"UPDATE session_mission_outbox
                          SET status = 'claimed', attempts = attempts + 1,
                              claim_owner = ?1, claim_expires_at_ms = ?2,
                              updated_at_ms = ?3, revision = revision + 1
                        WHERE request_id = ?4 AND workspace_key = ?5 AND revision = ?6
                          AND ((status IN ('pending', 'retry_scheduled') AND next_attempt_at_ms <= ?3)
                            OR (status = 'claimed' AND claim_expires_at_ms <= ?3))",
                    params![
                        worker_id,
                        expires_at as i64,
                        now_ms as i64,
                        request_id,
                        workspace_key,
                        revision as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed == 1 {
                let record = query_mission_outbox(&tx, &request_id)?.ok_or_else(|| {
                    SessionError::Store(format!(
                        "claimed mission outbox `{request_id}` disappeared"
                    ))
                })?;
                append_mission_outbox_history(
                    &tx,
                    &record,
                    if from_status == "claimed" {
                        "reclaim"
                    } else {
                        "claim"
                    },
                    Some(worker_id),
                    None,
                    &from_status,
                    OutboxStatus::Claimed.as_str(),
                    now_ms,
                )?;
                claimed.push(record);
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(claimed)
    }

    /// Acknowledge an applied Mission lifecycle intent.
    pub fn ack_session_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord> {
        self.transition_claimed_mission_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            |tx, current| {
                tx.execute(
                    r"UPDATE session_mission_outbox
                          SET status = 'materialized', claim_owner = NULL,
                              claim_expires_at_ms = NULL, failure_class = NULL,
                              last_error = NULL, updated_at_ms = ?1,
                              revision = revision + 1
                        WHERE request_id = ?2 AND status = 'claimed'
                          AND claim_owner = ?3 AND revision = ?4",
                    params![
                        now_ms as i64,
                        request_id,
                        worker_id,
                        expected_revision as i64
                    ],
                )
                .map_err(sql_err)?;
                Ok(("ack", OutboxStatus::Materialized, current.status))
            },
        )
    }

    /// Record a failed Session -> Mission application. Transient Runtime
    /// failures retry with bounded attempts; invalid payloads remain visible
    /// as blocked rows rather than spinning indefinitely.
    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord> {
        self.transition_claimed_mission_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            |tx, current| {
                let retry = failure_class == OutboxFailureClass::Retryable
                    && current.attempts < max_attempts.max(1);
                let next = if retry {
                    OutboxStatus::RetryScheduled
                } else {
                    OutboxStatus::BlockedMaterialization
                };
                tx.execute(
                    r"UPDATE session_mission_outbox
                          SET status = ?1, next_attempt_at_ms = ?2,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              failure_class = ?3, last_error = ?4,
                              updated_at_ms = ?5, revision = revision + 1
                        WHERE request_id = ?6 AND status = 'claimed'
                          AND claim_owner = ?7 AND revision = ?8",
                    params![
                        next.as_str(),
                        if retry { retry_at_ms } else { now_ms } as i64,
                        failure_class.as_str(),
                        error,
                        now_ms as i64,
                        request_id,
                        worker_id,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok((if retry { "retry" } else { "block" }, next, current.status))
            },
        )
    }

    pub fn get_session_mission_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionMissionOutboxRecord>> {
        let conn = self.conn()?;
        query_mission_outbox(&conn, request_id)
    }

    fn transition_claimed_mission_outbox<F>(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        transition: F,
    ) -> Result<SessionMissionOutboxRecord>
    where
        F: FnOnce(
            &rusqlite::Transaction<'_>,
            &SessionMissionOutboxRecord,
        ) -> Result<(&'static str, OutboxStatus, OutboxStatus)>,
    {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_mission_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("mission outbox `{request_id}` not found"))
        })?;
        if current.status != OutboxStatus::Claimed
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.revision != expected_revision
        {
            return Err(SessionError::Store(format!(
                "mission outbox `{request_id}` claim owner/status/revision mismatch"
            )));
        }
        let (action, to_status, from_status) = transition(&tx, &current)?;
        let updated = query_mission_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "transitioned mission outbox `{request_id}` disappeared"
            ))
        })?;
        if updated.revision != expected_revision + 1 || updated.status != to_status {
            return Err(SessionError::Store(format!(
                "mission outbox `{request_id}` transition lost an optimistic update"
            )));
        }
        append_mission_outbox_history(
            &tx,
            &updated,
            action,
            Some(worker_id),
            updated.last_error.as_deref(),
            from_status.as_str(),
            to_status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    #[allow(clippy::too_many_arguments)]
    fn transition_owned_outbox<F>(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        allowed_statuses: &[SessionRuntimeInputStatus],
        transition: F,
    ) -> Result<SessionRuntimeOutboxRecord>
    where
        F: FnOnce(
            &rusqlite::Transaction<'_>,
            &SessionRuntimeOutboxRecord,
        ) -> Result<(
            &'static str,
            SessionRuntimeInputStatus,
            SessionRuntimeInputStatus,
        )>,
    {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| SessionError::Store(format!("outbox `{request_id}` not found")))?;
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if !allowed_statuses.contains(&current.status)
            || current.session_generation != session_generation
            || admission.generation != session_generation
            || !admission.open
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.claim_token.as_deref() != Some(claim_token)
            || current.revision != expected_revision
            || current
                .claim_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
        {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` generation/token/status/revision fence mismatch"
            )));
        }
        let (action, to_status, from_status) = transition(&tx, &current)?;
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("transitioned outbox `{request_id}` disappeared"))
        })?;
        if updated.revision != expected_revision + 1 || updated.status != to_status {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` transition lost an optimistic update"
            )));
        }
        append_outbox_history(
            &tx,
            &updated,
            action,
            Some(worker_id),
            updated.last_error.as_deref(),
            from_status.as_str(),
            to_status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Retrieve messages for a session with pagination.
    pub fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role, content_json,
                          blocks_count, tool_use_id, tool_name,
                          token_usage_json, created_at_ms
                   FROM messages
                  WHERE session_id = ?1
                  ORDER BY sequence ASC
                  LIMIT ?2 OFFSET ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, limit as i64, offset as i64],
                row_to_message,
            )
            .map_err(sql_err)?;
        let mut msgs = Vec::new();
        for r in rows {
            msgs.push(r.map_err(sql_err)?);
        }
        Ok(msgs)
    }

    /// Retrieve messages for a session starting at `from_sequence`.
    ///
    /// This keyset-style path is stable for deep history paging because it
    /// uses the `(session_id, sequence)` index instead of scanning through a
    /// large OFFSET window.
    pub fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role, content_json,
                          blocks_count, tool_use_id, tool_name,
                          token_usage_json, created_at_ms
                   FROM messages
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_sequence as i64, limit as i64],
                row_to_message,
            )
            .map_err(sql_err)?;
        let mut msgs = Vec::new();
        for r in rows {
            msgs.push(r.map_err(sql_err)?);
        }
        Ok(msgs)
    }

    pub fn get_message_by_stable_id(
        &self,
        session_id: &str,
        stable_message_id: &str,
    ) -> Result<Option<SessionMessage>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT stable_message_id, session_id, sequence, role, content_json,
                     blocks_count, tool_use_id, tool_name, token_usage_json,
                     created_at_ms
                FROM messages
               WHERE session_id=?1 AND stable_message_id=?2",
            params![session_id, stable_message_id],
            row_to_message,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn get_message_by_sequence(
        &self,
        session_id: &str,
        sequence: usize,
    ) -> Result<Option<SessionMessage>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT stable_message_id, session_id, sequence, role, content_json,
                     blocks_count, tool_use_id, tool_name, token_usage_json,
                     created_at_ms
                FROM messages
               WHERE session_id=?1 AND sequence=?2",
            params![session_id, sequence as i64],
            row_to_message,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn get_message_metadata_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessageMetadata>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role,
                         blocks_count, tool_use_id, tool_name, created_at_ms,
                         length(CAST(content_json AS BLOB))
                    FROM messages
                   WHERE session_id=?1 AND sequence>=?2
                   ORDER BY sequence ASC
                   LIMIT ?3",
            )
            .map_err(sql_err)?;
        let result = statement
            .query_map(
                params![
                    session_id,
                    from_sequence as i64,
                    limit.clamp(1, 2_048) as i64
                ],
                row_to_message_metadata,
            )
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err);
        result
    }

    pub fn get_context_index_cards(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ContextIndexCard>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT card_id, parent_card_id, session_id,
                         source_start_sequence, source_end_sequence,
                         source_message_count, source_digest, summary, scope,
                         authority, generation, created_at_ms, updated_at_ms
                    FROM session_context_index_cards
                   WHERE session_id=?1
                   ORDER BY
                       CASE WHEN parent_card_id IS NULL THEN 0 ELSE 1 END,
                       source_start_sequence DESC
                   LIMIT ?2",
            )
            .map_err(sql_err)?;
        let result = statement
            .query_map(
                params![session_id, limit.clamp(1, 2_048) as i64],
                row_to_context_index_card,
            )
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err);
        result
    }

    /// Atomically replace one Session's rebuildable navigation index.
    ///
    /// This is intentionally an explicit background operation. Message
    /// appends only enqueue outbox rows in their own transaction.
    pub fn reconcile_session_context_index(
        &self,
        session_id: &str,
        card_span: usize,
        parent_span: usize,
        now_ms: u64,
    ) -> Result<ContextIndexCoverage> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let messages = {
            let mut statement = tx
                .prepare(
                    r"SELECT stable_message_id, session_id, sequence, role, content_json,
                             blocks_count, tool_use_id, tool_name, token_usage_json,
                             created_at_ms
                        FROM messages
                       WHERE session_id=?1
                       ORDER BY sequence ASC",
                )
                .map_err(sql_err)?;
            let result = statement
                .query_map(params![session_id], row_to_message)
                .map_err(sql_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?;
            result
        };
        let current_generation: u64 = tx
            .query_row(
                "SELECT index_generation FROM session_recovery_manifest WHERE session_id=?1",
                params![session_id],
                |row| Ok(row.get::<_, i64>(0)?.max(0) as u64),
            )
            .optional()
            .map_err(sql_err)?
            .ok_or_else(|| {
                SessionError::Store(format!(
                    "session activation manifest `{session_id}` does not exist"
                ))
            })?;
        let generation = current_generation.saturating_add(1);
        let cards = build_context_index_cards(
            session_id,
            &messages,
            card_span,
            parent_span,
            generation,
            now_ms,
        );
        tx.execute(
            "DELETE FROM session_context_index_cards WHERE session_id=?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        for card in &cards {
            tx.execute(
                r"INSERT INTO session_context_index_cards(
                       card_id, parent_card_id, session_id,
                       source_start_sequence, source_end_sequence,
                       source_message_count, source_digest, summary, scope,
                       authority, generation, created_at_ms, updated_at_ms
                   ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
                params![
                    card.card_id,
                    card.parent_card_id,
                    card.session_id,
                    card.source_start_sequence as i64,
                    card.source_end_sequence as i64,
                    card.source_message_count as i64,
                    card.source_digest,
                    card.summary,
                    card.scope,
                    card.authority,
                    card.generation as i64,
                    card.created_at_ms as i64,
                    card.updated_at_ms as i64,
                ],
            )
            .map_err(sql_err)?;
        }
        let indexed_through_sequence = messages.last().map(|message| message.sequence);
        tx.execute(
            r"UPDATE session_recovery_manifest
                  SET index_generation=?2,
                      indexed_through_sequence=?3,
                      index_card_count=?4,
                      index_pending=0,
                      manifest_revision=manifest_revision + 1
                WHERE session_id=?1",
            params![
                session_id,
                generation as i64,
                indexed_through_sequence.map(|value| value as i64),
                cards.len() as i64,
            ],
        )
        .map_err(sql_err)?;
        tx.execute(
            r"UPDATE session_context_index_outbox
                  SET status='completed', attempts=attempts + 1,
                      updated_at_ms=?2
                WHERE session_id=?1 AND status!='completed'",
            params![session_id, now_ms as i64],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let leaf_cards = cards
            .iter()
            .filter(|card| card.parent_card_id.is_some() || cards.len() == 1)
            .cloned()
            .collect::<Vec<_>>();
        let covered_messages = leaf_cards
            .iter()
            .map(|card| card.source_message_count)
            .sum();
        Ok(ContextIndexCoverage {
            session_id: session_id.to_string(),
            source_messages: messages.len(),
            covered_messages,
            card_count: cards.len(),
            indexed_through_sequence,
            generation,
            complete: covered_messages == messages.len(),
            source_digest: context_index_source_digest(&messages),
            card_digest: context_index_card_digest(&cards),
        })
    }

    /// Retrieve ALL messages for a session (unbounded, no pagination).
    pub fn get_all_messages(&self, session_id: &str) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms
                 FROM messages WHERE session_id = ?1 ORDER BY sequence ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id], row_to_message)
            .map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        if messages.len() > 1000 {
            tracing::warn!(
                session_id,
                count = messages.len(),
                "get_all_messages: large session, consider pagination"
            );
        }
        Ok(messages)
    }

    /// Count the number of messages in a session.
    pub fn get_message_count(&self, session_id: &str) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Delete all messages in a session starting from `from_sequence` (inclusive).
    ///
    /// Returns the number of rows deleted.
    pub fn delete_messages_from(&self, session_id: &str, from_sequence: usize) -> Result<usize> {
        let conn = self.conn()?;
        let removed = conn
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(removed)
    }

    /// Search messages using FTS5 full-text search.
    ///
    /// Optionally filter by `session_id`. Searches across role and
    /// extracted text content from `content_json`.
    pub fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        if let Some(sid) = session_id {
            let mut stmt = conn
                .prepare(
                    r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                              m.blocks_count, m.tool_use_id, m.tool_name,
                              m.token_usage_json, m.created_at_ms
                       FROM messages m
                       JOIN messages_fts fts ON m.id = fts.rowid
                      WHERE messages_fts MATCH ?1 AND m.session_id = ?2
                      ORDER BY rank
                      LIMIT ?3",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![query, sid, limit as i64], row_to_message)
                .map_err(sql_err)?;
            let mut msgs = Vec::new();
            for r in rows {
                msgs.push(r.map_err(sql_err)?);
            }
            Ok(msgs)
        } else {
            let mut stmt = conn
                .prepare(
                    r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                              m.blocks_count, m.tool_use_id, m.tool_name,
                              m.token_usage_json, m.created_at_ms
                       FROM messages m
                       JOIN messages_fts fts ON m.id = fts.rowid
                      WHERE messages_fts MATCH ?1
                      ORDER BY rank
                      LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![query, limit as i64], row_to_message)
                .map_err(sql_err)?;
            let mut msgs = Vec::new();
            for r in rows {
                msgs.push(r.map_err(sql_err)?);
            }
            Ok(msgs)
        }
    }

    /// Search only the supplied session authority set.  Gateway resolves that
    /// set before issuing the query so an unauthorised high-ranked FTS row can
    /// neither displace an authorised result nor be exposed to the caller.
    pub fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        if session_ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let scope_json = serde_json::to_string(session_ids).map_err(|error| {
            SessionError::Store(format!("encode search session scope: {error}"))
        })?;
        let mut stmt = conn
            .prepare(
                r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                          m.blocks_count, m.tool_use_id, m.tool_name,
                          m.token_usage_json, m.created_at_ms
                     FROM messages m
                     JOIN messages_fts fts ON m.id = fts.rowid
                    WHERE messages_fts MATCH ?1
                      AND m.session_id IN (SELECT value FROM json_each(?2))
                    ORDER BY rank
                    LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, scope_json, limit as i64], row_to_message)
            .map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        Ok(messages)
    }

    pub fn search_messages_visible(
        &self,
        query: &str,
        owner_principal_id: Option<&str>,
        visible_session_ids: &[String],
        unrestricted: bool,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let visible_json = serde_json::to_string(visible_session_ids).map_err(|error| {
            SessionError::Store(format!("encode visible Session scope: {error}"))
        })?;
        let mut stmt = conn
            .prepare(
                r"SELECT message.stable_message_id, message.session_id, message.sequence,
                          message.role, message.content_json, message.blocks_count,
                          message.tool_use_id, message.tool_name,
                          message.token_usage_json, message.created_at_ms
                     FROM messages AS message
                     JOIN messages_fts AS fts ON message.id=fts.rowid
                     JOIN sessions AS session ON session.session_id=message.session_id
                    WHERE messages_fts MATCH ?1
                      AND session.status NOT IN ('deleted','deleting')
                      AND (
                          ?4
                          OR json_extract(session.metadata_json, '$.owner_principal_id')=?2
                          OR session.session_id IN (
                              SELECT value FROM json_each(?3)
                          )
                      )
                    ORDER BY rank
                    LIMIT ?5",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![
                    query,
                    owner_principal_id.unwrap_or_default(),
                    visible_json,
                    unrestricted,
                    limit.clamp(1, 500) as i64
                ],
                row_to_message,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    // -----------------------------------------------------------------------
    // Event log
    // -----------------------------------------------------------------------

    /// Append a mutation event to the session's event log.
    pub fn append_event(&self, event: &SessionEvent) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event.event_json,
                event.sequence as i64,
                event.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Allocate the next session-local sequence and append one event in the
    /// same SQLite transaction. The input sequence is treated as a placeholder.
    pub fn append_event_allocating_sequence(&self, event: &SessionEvent) -> Result<SessionEvent> {
        let mut appended = self.append_events_allocating_sequence(std::slice::from_ref(event))?;
        appended
            .pop()
            .ok_or_else(|| SessionError::Store("event allocation returned no row".to_string()))
    }

    pub fn append_session_domain_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
        event_id: &str,
    ) -> Result<(SessionEvent, bool)> {
        if event.event_type != SESSION_DOMAIN_EVENT_TYPE || event_id.trim().is_empty() {
            return Err(SessionError::Store(
                "idempotent domain append requires SessionDomainEvent and a non-empty event_id"
                    .to_string(),
            ));
        }
        let encoded_event_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|value| {
                value
                    .get("event_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                SessionError::Store(
                    "idempotent domain append requires event_json.event_id".to_string(),
                )
            })?;
        if encoded_event_id != event_id {
            return Err(SessionError::Store(
                "idempotent domain append event_id does not match event_json".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let existing = tx
            .query_row(
                r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                    FROM session_events
                   WHERE session_id = ?1
                     AND event_type = ?2
                     AND json_extract(event_json, '$.event_id') = ?3
                   LIMIT 1",
                params![event.session_id, SESSION_DOMAIN_EVENT_TYPE, event_id],
                row_to_event,
            )
            .optional()
            .map_err(sql_err)?;
        if let Some(existing) = existing {
            if !SessionDomainEvent::semantically_equivalent(&existing, event).map_err(|error| {
                SessionError::Store(format!(
                    "failed to compare idempotent session-domain event content: {error}"
                ))
            })? {
                return Err(SessionError::IdempotencyConflict {
                    namespace: "session_domain_event",
                    key: event_id.to_string(),
                });
            }
            tx.commit().map_err(sql_err)?;
            return Ok((existing, true));
        }

        let sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![event.session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let stored_sequence = usize::try_from(sequence).map_err(|_| {
            SessionError::Store(
                "allocated session event sequence is negative or too large".to_string(),
            )
        })?;
        let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
        let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
            SessionError::Store("session event timestamp exceeds SQLite i64 range".to_string())
        })?;
        tx.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event_json,
                sequence,
                created_at_ms,
            ],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let mut stored = event.clone();
        stored.sequence = stored_sequence;
        stored.event_json = event_json;
        Ok((stored, false))
    }

    pub fn get_session_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> Result<Option<SessionEvent>> {
        if event_id.trim().is_empty() {
            return Ok(None);
        }
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                FROM session_events
               WHERE session_id = ?1
                 AND event_type = ?2
                 AND json_extract(event_json, '$.event_id') = ?3
               LIMIT 1",
            params![session_id, SESSION_DOMAIN_EVENT_TYPE, event_id],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Allocate contiguous sequences and append a same-session event batch in
    /// one `BEGIN IMMEDIATE` transaction.
    pub fn append_events_allocating_sequence(
        &self,
        events: &[SessionEvent],
    ) -> Result<Vec<SessionEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(SessionError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let first_sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;

        let mut appended = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                SessionError::Store("session event batch offset exceeds i64 range".to_string())
            })?;
            let sequence = first_sequence.checked_add(offset).ok_or_else(|| {
                SessionError::Store("session event sequence overflow".to_string())
            })?;
            let stored_sequence = usize::try_from(sequence).map_err(|_| {
                SessionError::Store(
                    "allocated session event sequence is negative or too large".to_string(),
                )
            })?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
                SessionError::Store("session event timestamp exceeds SQLite i64 range".to_string())
            })?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.session_id,
                    event.event_type,
                    event_json,
                    sequence,
                    created_at_ms,
                ],
            )
            .map_err(sql_err)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            appended.push(stored);
        }
        tx.commit().map_err(sql_err)?;
        Ok(appended)
    }

    /// Atomically append a compaction event bundle unless its semantic
    /// checkpoint has already committed for this session. `None` means a
    /// previous attempt committed the exact checkpoint and the caller must
    /// reuse it instead of emitting duplicate facts/events.
    pub fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        events: &[SessionEvent],
        checkpoint_id: &str,
    ) -> Result<Option<Vec<SessionEvent>>> {
        if events.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(SessionError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(SessionError::Store(
                "checkpoint-aware event batch requires a non-empty checkpoint_id".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists: i64 = tx
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                    WHERE session_id = ?1
                      AND event_type = ?2
                      AND json_extract(event_json, '$.kind') = 'memory.semantic_checkpoint.created'
                      AND json_extract(event_json, '$.payload.checkpoint.checkpoint_id') = ?3",
                params![session_id, SESSION_DOMAIN_EVENT_TYPE, checkpoint_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if exists > 0 {
            tx.commit().map_err(sql_err)?;
            return Ok(None);
        }

        let first_sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut appended = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                SessionError::Store("session event batch offset exceeds i64 range".to_string())
            })?;
            let sequence = first_sequence.checked_add(offset).ok_or_else(|| {
                SessionError::Store("session event sequence overflow".to_string())
            })?;
            let stored_sequence = usize::try_from(sequence).map_err(|_| {
                SessionError::Store(
                    "allocated session event sequence is negative or too large".to_string(),
                )
            })?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
                SessionError::Store("session event timestamp exceeds SQLite i64 range".to_string())
            })?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.session_id,
                    event.event_type,
                    event_json,
                    sequence,
                    created_at_ms,
                ],
            )
            .map_err(sql_err)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            appended.push(stored);
        }
        tx.commit().map_err(sql_err)?;
        Ok(Some(appended))
    }

    /// Atomically de-duplicate a context envelope and allocate its sequence.
    pub fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> Result<Option<SessionEvent>> {
        if event.event_type != "ContextEnvelope" {
            return self.append_event_allocating_sequence(event).map(Some);
        }
        let envelope_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|payload| {
                payload
                    .pointer("/envelope/id")
                    .or_else(|| payload.get("envelope_id"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                SessionError::Store(
                    "ContextEnvelope append requires envelope.id or envelope_id".to_string(),
                )
            })?;

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists: i64 = tx
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                  WHERE event_type = 'ContextEnvelope'
                    AND COALESCE(
                        json_extract(event_json, '$.envelope.id'),
                        json_extract(event_json, '$.envelope_id')
                    ) = ?1",
                params![envelope_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if exists > 0 {
            tx.commit().map_err(sql_err)?;
            return Ok(None);
        }
        let sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![event.session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event.event_json,
                sequence,
                event.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let mut stored = event.clone();
        stored.sequence = sequence as usize;
        Ok(Some(stored))
    }

    /// Append a context envelope event only if this envelope id is not already present.
    ///
    /// Returns `true` when a row was inserted and `false` when an existing
    /// `ContextEnvelope` row with the same `envelope.id` already exists.
    pub fn append_context_envelope_event_if_absent(&self, event: &SessionEvent) -> Result<bool> {
        self.append_context_envelope_event_if_absent_allocating_sequence(event)
            .map(|stored| stored.is_some())
    }

    /// Retrieve events for a session starting from `from_seq` (inclusive).
    /// Ordered by sequence ascending.
    pub fn get_events(&self, session_id: &str, from_seq: usize) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND sequence >= ?2
                 ORDER BY sequence ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id, from_seq as i64], row_to_event)
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Retrieve at most `limit` events for a session starting from `from_seq`.
    /// Ordered by sequence ascending.
    pub fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND sequence >= ?2
                 ORDER BY sequence ASC
                 LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Retrieve canonical Session-domain events only.
    pub fn get_session_domain_timeline_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        self.get_events_by_type_limited(session_id, SESSION_DOMAIN_EVENT_TYPE, from_seq, limit)
    }

    pub fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> Result<usize> {
        self.count_events_by_type_from(session_id, SESSION_DOMAIN_EVENT_TYPE, from_seq)
    }

    pub fn get_session_domain_events_by_kind_limited(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                    FROM session_events
                   WHERE session_id = ?1
                     AND event_type = ?2
                     AND json_extract(event_json, '$.kind') = ?3
                     AND sequence >= ?4
                   ORDER BY sequence ASC
                   LIMIT ?5",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![
                    session_id,
                    SESSION_DOMAIN_EVENT_TYPE,
                    kind,
                    from_seq as i64,
                    limit as i64,
                ],
                row_to_event,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Resolve the newest event of one kind through the covering expression
    /// index. This is O(log n) and never scans an arbitrary prefix.
    pub fn get_latest_session_domain_event_by_kind(
        &self,
        session_id: &str,
        kind: &str,
    ) -> Result<Option<SessionEvent>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                FROM session_events
               WHERE session_id=?1
                 AND event_type=?2
                 AND json_extract(event_json, '$.kind')=?3
               ORDER BY sequence DESC
               LIMIT 1",
            params![session_id, SESSION_DOMAIN_EVENT_TYPE, kind],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn count_session_domain_events_by_kind_from(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                   WHERE session_id = ?1
                     AND event_type = ?2
                     AND json_extract(event_json, '$.kind') = ?3
                     AND sequence >= ?4",
                params![session_id, SESSION_DOMAIN_EVENT_TYPE, kind, from_seq as i64,],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        usize::try_from(count)
            .map_err(|_| SessionError::Store("domain event count exceeds usize".to_string()))
    }

    pub fn has_session_domain_event_kind(&self, kind: &str) -> Result<bool> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT EXISTS(
                SELECT 1 FROM session_events
                 WHERE event_type=?1
                   AND json_extract(event_json, '$.kind')=?2
                 LIMIT 1
            )",
            params![SESSION_DOMAIN_EVENT_TYPE, kind],
            |row| row.get(0),
        )
        .map_err(sql_err)
    }

    pub fn has_session_with_domain_event_kinds(&self, kinds: &[String]) -> Result<bool> {
        if kinds.is_empty() {
            return Ok(false);
        }
        let conn = self.conn()?;
        let kinds_json = serde_json::to_string(kinds)
            .map_err(|error| SessionError::Store(format!("encode event kinds: {error}")))?;
        conn.query_row(
            r"SELECT EXISTS(
                SELECT session_id
                  FROM session_events
                 WHERE event_type=?1
                   AND json_extract(event_json, '$.kind') IN (
                       SELECT value FROM json_each(?2)
                   )
                 GROUP BY session_id
                HAVING COUNT(DISTINCT json_extract(event_json, '$.kind')) >= ?3
                 LIMIT 1
            )",
            params![SESSION_DOMAIN_EVENT_TYPE, kinds_json, kinds.len() as i64],
            |row| row.get(0),
        )
        .map_err(sql_err)
    }

    /// Retrieve at most `limit` events of one type for a session.
    /// Ordered by sequence ascending.
    pub fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3
                 ORDER BY sequence ASC
                 LIMIT ?4",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, event_type, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Count events for a session starting from `from_seq`.
    pub fn count_events_from(&self, session_id: &str, from_seq: usize) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_seq as i64],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Count events of one type for a session starting from `from_seq`.
    pub fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3",
                params![session_id, event_type, from_seq as i64],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Retrieve a context envelope event by its envelope id.
    pub fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
              FROM session_events
              WHERE event_type = 'ContextEnvelope'
                AND json_extract(event_json, '$.envelope.id') = ?1
              ORDER BY created_at_ms DESC
              LIMIT 1",
            params![envelope_id],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Return the next append sequence for a session event.
    pub fn next_event_sequence(&self, session_id: &str) -> Result<usize> {
        let conn = self.conn()?;
        let next: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(next.max(0) as usize)
    }

    /// Delete all events from `from_sequence` onward in a session.
    pub fn delete_events_from(&self, session_id: &str, from_sequence: usize) -> Result<usize> {
        let conn = self.conn()?;
        let deleted = conn
            .execute(
                "DELETE FROM session_events WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Delete events of one type from `from_sequence` onward in a session.
    pub fn delete_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let deleted = conn
            .execute(
                "DELETE FROM session_events WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3",
                params![session_id, event_type, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Save a full-message-list snapshot at a given event index.
    pub fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_snapshots
               (session_id, event_idx, messages_json, created_at_ms)
              VALUES (?1, ?2, ?3, ?4)",
            params![
                snapshot.session_id,
                snapshot.event_idx as i64,
                snapshot.messages_json,
                snapshot.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Return the most recent snapshot for a session, or `None`.
    pub fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, session_id, event_idx, messages_json, created_at_ms
             FROM session_snapshots
             WHERE session_id = ?1
             ORDER BY event_idx DESC
             LIMIT 1",
            params![session_id],
            row_to_snapshot,
        )
        .optional()
        .map_err(sql_err)
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_before(&self, cutoff_iso8601: &str) -> Result<usize> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // Remove associated memories first.
        tx.execute(
            r"DELETE FROM session_memories WHERE session_id IN (
                SELECT session_id FROM sessions WHERE last_activity < ?1
              )",
            params![cutoff_iso8601],
        )
        .map_err(sql_err)?;
        let removed = tx
            .execute(
                "DELETE FROM sessions WHERE last_activity < ?1",
                params![cutoff_iso8601],
            )
            .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(removed)
    }

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`,
    /// cleaning up both the SQLite records and any corresponding JSONL/JSON
    /// files on disk under `sessions_dir`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_with_files(&self, cutoff_iso8601: &str, sessions_dir: &Path) -> Result<usize> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT session_id FROM sessions WHERE last_activity < ?1")
            .map_err(sql_err)?;
        let ids: Vec<String> = stmt
            .query_map(params![cutoff_iso8601], |row| row.get::<_, String>(0))
            .map_err(sql_err)?
            .filter_map(|r| r.ok())
            .collect();
        let count = ids.len();
        for id in &ids {
            self.delete_session(id)?;
            for ext in &["jsonl", "json"] {
                let path = sessions_dir.join(format!("{id}.{ext}"));
                let _ = std::fs::remove_file(&path);
                if *ext == "jsonl" {
                    if let Ok(entries) = std::fs::read_dir(sessions_dir) {
                        for entry in entries.flatten() {
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if name_str.starts_with(&format!("{id}.rot-"))
                                && name_str.ends_with(".jsonl")
                            {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    /// Mark a session as closed.
    ///
    /// Updates the session's status to `'closed'` and refreshes
    /// `last_activity`.  Messages are preserved for auditing.
    pub fn mark_session_closed(&self, session_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE sessions SET status = 'closed', last_activity = ?1 WHERE session_id = ?2",
            params![chrono::Utc::now().to_rfc3339(), session_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }
}

fn legacy_message_id(session_id: &str, sequence: usize) -> String {
    let mut encoded = String::with_capacity(session_id.len() * 2);
    for byte in session_id.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    format!("legacy:{encoded}:{sequence}")
}

fn branch_event_json(raw: &str, copied: usize, cutoff: usize) -> Result<String> {
    let mut payload = serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
        SessionError::Store(format!("branch event JSON must be valid: {error}"))
    })?;
    let object = payload
        .as_object_mut()
        .ok_or_else(|| SessionError::Store("branch event JSON must be an object".to_string()))?;
    object.insert(
        "copied_message_count".to_string(),
        serde_json::json!(copied),
    );
    object.insert(
        "source_message_cutoff".to_string(),
        serde_json::json!(cutoff),
    );
    serde_json::to_string(&payload)
        .map_err(|error| SessionError::Store(format!("branch event encode failed: {error}")))
}

fn event_json_with_allocated_sequence(event: &SessionEvent, sequence: usize) -> Result<String> {
    if event.event_type != SESSION_DOMAIN_EVENT_TYPE {
        return Ok(event.event_json.clone());
    }
    let mut payload =
        serde_json::from_str::<serde_json::Value>(&event.event_json).map_err(|error| {
            SessionError::Store(format!(
                "session domain event JSON must be valid before sequence allocation: {error}"
            ))
        })?;
    let object = payload.as_object_mut().ok_or_else(|| {
        SessionError::Store("session domain event JSON must be an object".to_string())
    })?;
    object.insert("sequence".to_string(), serde_json::json!(sequence));
    serde_json::to_string(&payload).map_err(|error| {
        SessionError::Store(format!("session domain event JSON encode failed: {error}"))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_store() -> (SqliteSessionStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let store = SqliteSessionStore::open(&path).expect("open session store");
        (store, dir)
    }

    fn make_record(id: &str) -> SessionRecord {
        SessionRecord {
            session_id: id.to_string(),
            platform: "test".to_string(),
            chat_id: "chat-1".to_string(),
            user_id: Some("user-1".to_string()),
            model: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            last_activity: "2024-01-01T00:01:00Z".to_string(),
            message_count: 1,
            reset_policy: "None".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    #[test]
    fn test_create_and_get() {
        let (store, _dir) = make_store();
        let rec = make_record("session-001");
        store.create_session(&rec).unwrap();
        let loaded = store.get_session("session-001").unwrap().unwrap();
        assert_eq!(loaded.session_id, "session-001");
        assert_eq!(loaded.platform, "test");
        assert_eq!(loaded.message_count, 1);
    }

    #[test]
    fn create_session_populates_millisecond_timestamps() {
        let (store, _dir) = make_store();
        let rec = make_record("session-ms");
        store.create_session(&rec).unwrap();
        let conn = store.conn().unwrap();
        let (created_at_ms, updated_at_ms): (i64, i64) = conn
            .query_row(
                "SELECT created_at_ms, updated_at_ms FROM sessions WHERE session_id = ?1",
                params!["session-ms"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(created_at_ms, 1_704_067_200_000);
        assert_eq!(updated_at_ms, 1_704_067_260_000);
    }

    #[test]
    fn test_update_session() {
        let (store, _dir) = make_store();
        let mut rec = make_record("session-002");
        store.create_session(&rec).unwrap();
        rec.message_count = 42;
        rec.last_activity = "2024-01-02T00:00:00Z".to_string();
        store.update_session(&rec).unwrap();
        let loaded = store.get_session("session-002").unwrap().unwrap();
        assert_eq!(loaded.message_count, 42);
    }

    #[test]
    fn test_upsert_session() {
        let (store, _dir) = make_store();
        let mut rec = make_record("session-003");
        store.upsert_session(&rec).unwrap();
        rec.message_count = 99;
        store.upsert_session(&rec).unwrap();
        let loaded = store.get_session("session-003").unwrap().unwrap();
        assert_eq!(loaded.message_count, 99);
    }

    #[test]
    fn test_delete_session() {
        let (store, _dir) = make_store();
        let rec = make_record("session-004");
        store.create_session(&rec).unwrap();
        store.delete_session("session-004").unwrap();
        assert!(store.get_session("session-004").unwrap().is_none());
    }

    #[test]
    fn test_list_sessions() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s1")).unwrap();
        store.create_session(&make_record("s2")).unwrap();
        let list = store.list_sessions().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn scoped_message_search_preserves_authorized_results_when_other_sessions_rank_first() {
        let (store, _dir) = make_store();
        for session_id in ["foreign", "authorized"] {
            store.create_session(&make_record(session_id)).unwrap();
        }
        for (session_id, sequence) in [("foreign", 0), ("authorized", 0)] {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("{session_id}:{sequence}"),
                    session_id: session_id.to_string(),
                    sequence,
                    role: "user".to_string(),
                    content_json: r#"[{"type":"text","text":"tenant ranked search phrase"}]"#
                        .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 1,
                })
                .unwrap();
        }

        let results = store
            .search_messages_in_sessions("tenant", &["authorized".to_string()], 1)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].session_id, "authorized");
    }

    #[test]
    fn list_sessions_by_workspace_root_filters_and_orders_by_activity() {
        let (store, _dir) = make_store();
        let workspace_a = "/tmp/cowd-workspace-a";
        let workspace_b = "/tmp/cowd-workspace-b";

        let mut older = make_record("workspace-a-older");
        older.last_activity = "2024-01-01T00:00:00Z".to_string();
        older.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
        store.create_session(&older).unwrap();

        let mut newer = make_record("workspace-a-newer");
        newer.last_activity = "2024-01-02T00:00:00Z".to_string();
        newer.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
        store.create_session(&newer).unwrap();

        let mut other_workspace = make_record("workspace-b");
        other_workspace.last_activity = "2024-01-03T00:00:00Z".to_string();
        other_workspace.metadata_json =
            Some(serde_json::json!({"workspace_root": workspace_b}).to_string());
        store.create_session(&other_workspace).unwrap();

        let records = store
            .list_sessions_by_workspace_root(workspace_a)
            .expect("workspace sessions should list");

        assert_eq!(
            records
                .iter()
                .map(|record| record.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["workspace-a-newer", "workspace-a-older"]
        );
    }

    #[test]
    fn list_sessions_page_filters_sorts_and_counts_at_scale() {
        let (store, _dir) = make_store();
        {
            let mut conn = store.conn().unwrap();
            let tx = conn.transaction().unwrap();
            {
                let mut stmt = tx
                    .prepare(
                        r"INSERT INTO sessions
                           (session_id, platform, chat_id, user_id, model,
                            created_at, last_activity, message_count, reset_policy, metadata_json,
                            input_tokens, output_tokens, estimated_cost_usd, status)
                           VALUES (?1, 'api_server', ?1, NULL, ?2, ?3, ?3, ?4, 'none', ?5, 0, 0, 0.0, ?6)",
                    )
                    .unwrap();
                for i in 0..10_000 {
                    let model = if i % 2 == 0 {
                        "claude-sonnet-4-6"
                    } else {
                        "claude-haiku-4-5"
                    };
                    let status = if i % 3 == 0 { "active" } else { "closed" };
                    let ts = format!(
                        "2026-06-04T{:02}:{:02}:{:02}Z",
                        (i / 3600) % 24,
                        (i / 60) % 60,
                        i % 60
                    );
                    let title =
                        serde_json::json!({"title": format!("Perf Session {i:05}")}).to_string();
                    stmt.execute(params![
                        format!("perf-{i:05}"),
                        model,
                        ts,
                        i as i64,
                        title,
                        status
                    ])
                    .unwrap();
                }
            }
            tx.commit().unwrap();
        }

        let page = store
            .list_sessions_page(&SessionListOptions {
                model: Some("claude-sonnet-4-6"),
                status: Some("active"),
                unrestricted: true,
                sort: "last_activity",
                order: "desc",
                limit: 7,
                offset: 0,
                ..SessionListOptions::default()
            })
            .unwrap();

        assert_eq!(page.total, 1667);
        assert_eq!(page.records.len(), 7);
        assert!(page
            .records
            .windows(2)
            .all(|pair| pair[0].last_activity >= pair[1].last_activity));
        assert!(page
            .records
            .iter()
            .all(|r| r.model.as_deref() == Some("claude-sonnet-4-6") && r.status == "active"));
    }

    #[test]
    fn list_sessions_page_applies_owner_grants_and_tombstone_visibility_in_sql() {
        let (store, _dir) = make_store();
        for (id, owner, status) in [
            ("owned", "principal-a", "active"),
            ("granted", "principal-b", "closed"),
            ("hidden", "principal-b", "active"),
            ("deleted", "principal-a", "deleted"),
        ] {
            let mut record = make_record(id);
            record.status = status.to_string();
            record.metadata_json =
                Some(serde_json::json!({"owner_principal_id": owner}).to_string());
            store.create_session(&record).unwrap();
        }

        let grants = vec!["granted".to_string()];
        let page = store
            .list_sessions_page(&SessionListOptions {
                owner_principal_id: Some("principal-a"),
                visible_session_ids: &grants,
                sort: "last_activity",
                order: "desc",
                limit: 20,
                ..SessionListOptions::default()
            })
            .unwrap();
        let ids = page
            .records
            .iter()
            .map(|record| record.session_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(page.total, 2);
        assert_eq!(ids, std::collections::BTreeSet::from(["granted", "owned"]));
    }

    #[test]
    fn list_sessions_page_escapes_like_wildcards() {
        let (store, _dir) = make_store();
        let mut literal = make_record("literal-percent");
        literal.metadata_json = Some(serde_json::json!({"title":"Auth% Literal"}).to_string());
        store.create_session(&literal).unwrap();

        let mut wildcard = make_record("wildcard-match");
        wildcard.metadata_json = Some(serde_json::json!({"title":"Auth Wildcard"}).to_string());
        store.create_session(&wildcard).unwrap();

        let page = store
            .list_sessions_page(&SessionListOptions {
                query: Some("Auth%"),
                unrestricted: true,
                limit: 20,
                ..SessionListOptions::default()
            })
            .unwrap();

        assert_eq!(page.total, 1);
        assert_eq!(page.records[0].session_id, "literal-percent");
    }

    #[test]
    fn status_model_recent_session_query_uses_composite_index() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-index")).unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT session_id FROM sessions
                  WHERE status = ?1 COLLATE NOCASE AND model = ?2 COLLATE NOCASE
                  ORDER BY last_activity DESC
                  LIMIT 20 OFFSET 0",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["active", "claude-sonnet-4-6"], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_sessions_status_model_last_activity"),
            "expected composite index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn get_events_limited_pages_from_sequence_and_counts_total() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-events")).unwrap();
        for i in 0..1000 {
            store
                .append_event(&SessionEvent {
                    session_id: "s-events".to_string(),
                    event_type: "message_appended".to_string(),
                    event_json: serde_json::json!({"sequence": i}).to_string(),
                    sequence: i,
                    created_at_ms: i as u64,
                })
                .unwrap();
        }

        let events = store.get_events_limited("s-events", 990, 5).unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].sequence, 990);
        assert_eq!(events[4].sequence, 994);
        assert_eq!(store.count_events_from("s-events", 990).unwrap(), 10);
    }

    #[test]
    fn get_events_by_type_pages_context_envelopes_only() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-events"))
            .unwrap();
        for (sequence, event_type) in [
            (0, "TextDelta"),
            (1, "ContextEnvelope"),
            (2, "ToolStart"),
            (3, "ContextEnvelope"),
        ] {
            store
                .append_event(&SessionEvent {
                    session_id: "s-context-events".to_string(),
                    event_type: event_type.to_string(),
                    event_json: serde_json::json!({
                        "envelope_id": format!("env-{sequence}"),
                        "envelope": {"id": format!("env-{sequence}")}
                    })
                    .to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        let events = store
            .get_events_by_type_limited("s-context-events", "ContextEnvelope", 0, 10)
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].sequence, 3);
        assert_eq!(
            store
                .count_events_by_type_from("s-context-events", "ContextEnvelope", 0)
                .unwrap(),
            2
        );
    }

    #[test]
    fn get_context_event_by_envelope_id_reads_json_payload() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-context-id")).unwrap();
        store
            .append_event(&SessionEvent {
                session_id: "s-context-id".to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: serde_json::json!({
                    "envelope_id": "env-target",
                    "envelope": {"id": "env-target", "intent": "ship"}
                })
                .to_string(),
                sequence: 7,
                created_at_ms: 7,
            })
            .unwrap();

        let event = store
            .get_context_event_by_envelope_id("env-target")
            .unwrap()
            .expect("context event");
        assert_eq!(event.session_id, "s-context-id");
        assert_eq!(event.sequence, 7);
        assert!(event.event_json.contains("ship"));
    }

    #[test]
    fn append_context_envelope_event_if_absent_skips_duplicate_envelope_id() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-once"))
            .unwrap();
        let first = SessionEvent {
            session_id: "s-context-once".to_string(),
            event_type: "ContextEnvelope".to_string(),
            event_json: serde_json::json!({
                "envelope_id": "env-once",
                "envelope": {"id": "env-once", "intent": "first"}
            })
            .to_string(),
            sequence: 1,
            created_at_ms: 1,
        };
        let duplicate = SessionEvent {
            sequence: 2,
            created_at_ms: 2,
            event_json: serde_json::json!({
                "envelope_id": "env-once",
                "envelope": {"id": "env-once", "intent": "duplicate"}
            })
            .to_string(),
            ..first.clone()
        };

        assert!(store
            .append_context_envelope_event_if_absent(&first)
            .unwrap());
        assert!(!store
            .append_context_envelope_event_if_absent(&duplicate)
            .unwrap());

        let events = store
            .get_events_by_type_limited("s-context-once", "ContextEnvelope", 0, 10)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 0);
        assert!(events[0].event_json.contains("first"));
    }

    #[test]
    fn delete_events_from_removes_tail_only() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-events-delete"))
            .unwrap();
        for i in 0..5 {
            store
                .append_event(&SessionEvent {
                    session_id: "s-events-delete".to_string(),
                    event_type: "message_appended".to_string(),
                    event_json: serde_json::json!({"sequence": i}).to_string(),
                    sequence: i,
                    created_at_ms: i as u64,
                })
                .unwrap();
        }

        assert_eq!(store.delete_events_from("s-events-delete", 3).unwrap(), 2);
        let events = store.get_events("s-events-delete", 0).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[2].sequence, 2);
    }

    #[test]
    fn delete_events_by_type_from_preserves_other_event_types() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-events-delete-type"))
            .unwrap();
        for (sequence, event_type) in [
            (0, "message_appended"),
            (1, "TextDelta"),
            (2, "message_appended"),
            (3, "ToolStart"),
        ] {
            store
                .append_event(&SessionEvent {
                    session_id: "s-events-delete-type".to_string(),
                    event_type: event_type.to_string(),
                    event_json: serde_json::json!({"sequence": sequence}).to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        assert_eq!(
            store
                .delete_events_by_type_from("s-events-delete-type", "message_appended", 0)
                .unwrap(),
            2
        );
        let events = store.get_events("s-events-delete-type", 0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "TextDelta");
        assert_eq!(events[1].event_type, "ToolStart");
    }

    #[test]
    fn next_event_sequence_uses_max_sequence_plus_one() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-next-event")).unwrap();
        assert_eq!(store.next_event_sequence("s-next-event").unwrap(), 0);

        for sequence in [0, 5, 2] {
            store
                .append_event(&SessionEvent {
                    session_id: "s-next-event".to_string(),
                    event_type: "TextDelta".to_string(),
                    event_json: serde_json::json!({"sequence": sequence}).to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        assert_eq!(store.next_event_sequence("s-next-event").unwrap(), 6);
    }

    #[test]
    fn allocating_sequence_appends_contiguous_batch_atomically() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-atomic-batch"))
            .unwrap();
        let events = ["first", "second", "third"].map(|event_type| SessionEvent {
            session_id: "s-atomic-batch".to_string(),
            event_type: event_type.to_string(),
            event_json: "{}".to_string(),
            sequence: usize::MAX,
            created_at_ms: 1,
        });

        let appended = store
            .append_events_allocating_sequence(&events)
            .expect("atomic batch should append");
        assert_eq!(
            appended
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(store.get_events("s-atomic-batch", 0).unwrap().len(), 3);
    }

    #[test]
    fn allocating_sequence_is_atomic_across_parallel_sqlite_connections() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-parallel-sqlite"))
            .unwrap();
        let store = std::sync::Arc::new(store);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(100));
        let mut workers = Vec::new();
        for index in 0..100usize {
            let store = std::sync::Arc::clone(&store);
            let barrier = std::sync::Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                store
                    .append_event_allocating_sequence(&SessionEvent {
                        session_id: "s-parallel-sqlite".to_string(),
                        event_type: "parallel".to_string(),
                        event_json: format!(r#"{{"index":{index}}}"#),
                        sequence: usize::MAX,
                        created_at_ms: index as u64,
                    })
                    .unwrap()
                    .sequence
            }));
        }
        let mut sequences = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn session_event_sequence_constraint_rejects_duplicate() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-unique-event"))
            .unwrap();
        let event = SessionEvent {
            session_id: "s-unique-event".to_string(),
            event_type: "first".to_string(),
            event_json: "{}".to_string(),
            sequence: 0,
            created_at_ms: 1,
        };
        store.append_event(&event).unwrap();
        let mut duplicate = event;
        duplicate.event_type = "duplicate".to_string();
        assert!(store.append_event(&duplicate).is_err());
        assert_eq!(store.get_events("s-unique-event", 0).unwrap().len(), 1);
    }

    #[test]
    fn allocating_batch_rolls_back_when_runtime_envelope_is_invalid() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-batch-rollback"))
            .unwrap();
        let events = vec![
            SessionEvent {
                session_id: "s-batch-rollback".to_string(),
                event_type: "normal".to_string(),
                event_json: "{}".to_string(),
                sequence: usize::MAX,
                created_at_ms: 1,
            },
            SessionEvent {
                session_id: "s-batch-rollback".to_string(),
                event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
                event_json: "not-json".to_string(),
                sequence: usize::MAX,
                created_at_ms: 2,
            },
        ];
        assert!(store.append_events_allocating_sequence(&events).is_err());
        assert!(store.get_events("s-batch-rollback", 0).unwrap().is_empty());
    }

    #[test]
    fn checkpoint_batch_timestamp_overflow_rolls_back_without_partial_event() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-checkpoint-timestamp-overflow"))
            .unwrap();
        let checkpoint_id = "checkpoint-timestamp-overflow";
        let event = SessionEvent {
            session_id: "s-checkpoint-timestamp-overflow".to_string(),
            event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: serde_json::json!({
                "kind": "memory.semantic_checkpoint.created",
                "payload": {"checkpoint": {"checkpoint_id": checkpoint_id}},
            })
            .to_string(),
            sequence: usize::MAX,
            created_at_ms: u64::MAX,
        };

        assert!(store
            .append_events_allocating_sequence_if_checkpoint_absent(&[event], checkpoint_id)
            .is_err());
        assert!(store
            .get_events("s-checkpoint-timestamp-overflow", 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn event_page_query_uses_session_sequence_index() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-event-index")).unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["s-event-index", 100_i64, 20_i64], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_session_events_session_seq")
                || plan_text.contains("uq_session_events_session_sequence"),
            "expected event sequence index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn event_type_page_query_uses_session_type_sequence_index() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-event-index"))
            .unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3
                  ORDER BY sequence ASC
                  LIMIT ?4",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(
                params!["s-context-event-index", "ContextEnvelope", 100_i64, 20_i64],
                |row| row.get(3),
            )
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_session_events_session_type_seq"),
            "expected context event type index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn context_envelope_lookup_uses_envelope_id_index() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-envelope-index"))
            .unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE event_type = 'ContextEnvelope'
                    AND json_extract(event_json, '$.envelope.id') = ?1
                  ORDER BY created_at_ms DESC
                  LIMIT 1",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["env-indexed"], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_session_events_context_envelope_id"),
            "expected context envelope id index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn get_messages_from_sequence_pages_100k_history() {
        let (store, _dir) = make_store();
        let mut record = make_record("s-100k");
        record.message_count = 100_000;
        store.create_session(&record).unwrap();
        {
            let mut conn = store.conn().unwrap();
            let tx = conn.transaction().unwrap();
            {
                let mut stmt = tx
                    .prepare(
                        r"INSERT INTO messages
                           (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                            tool_use_id, tool_name, token_usage_json, created_at_ms)
                           VALUES (printf('bulk:%d', ?1), 's-100k', ?1, ?2, ?3, 1, NULL, NULL, NULL, ?4)",
                    )
                    .unwrap();
                for i in 0..100_000 {
                    let role = if i % 2 == 0 { "user" } else { "assistant" };
                    let content =
                        serde_json::json!([{"type":"text","text":format!("message {i}")}])
                            .to_string();
                    stmt.execute(params![i as i64, role, content, i as i64])
                        .unwrap();
                }
            }
            tx.commit().unwrap();
        }

        let page = store
            .get_messages_from_sequence("s-100k", 99_950, 50)
            .unwrap();
        assert_eq!(page.len(), 50);
        assert_eq!(page[0].sequence, 99_950);
        assert_eq!(page[49].sequence, 99_999);
        let outbox_rows: i64 = store
            .conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM session_context_index_outbox WHERE session_id='s-100k'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            outbox_rows, 1,
            "append indexing must remain O(1) per Session"
        );
    }

    #[test]
    fn exact_message_reads_and_metadata_page_preserve_stable_identity() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-exact-message"))
            .unwrap();
        for sequence in 0..3 {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("exact-{sequence}"),
                    session_id: "s-exact-message".to_string(),
                    sequence,
                    role: if sequence % 2 == 0 {
                        "user"
                    } else {
                        "assistant"
                    }
                    .to_string(),
                    content_json: serde_json::json!([
                        {"type":"text","text":format!("payload-{sequence}")}
                    ])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        assert_eq!(
            store
                .get_message_by_stable_id("s-exact-message", "exact-1")
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
        assert_eq!(
            store
                .get_message_by_sequence("s-exact-message", 2)
                .unwrap()
                .unwrap()
                .stable_message_id,
            "exact-2"
        );
        let metadata = store
            .get_message_metadata_page("s-exact-message", 1, 2)
            .unwrap();
        assert_eq!(
            metadata
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(metadata.iter().all(|message| message.content_bytes > 0));
    }

    #[test]
    fn latest_checkpoint_lookup_uses_full_index_beyond_legacy_page_boundary() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-late-checkpoint"))
            .unwrap();
        {
            let mut conn = store.conn().unwrap();
            let tx = conn.transaction().unwrap();
            {
                let mut statement = tx
                    .prepare(
                        r"INSERT INTO session_events(
                               session_id, event_type, event_json, sequence, created_at_ms
                           ) VALUES (
                               's-late-checkpoint', 'SessionDomainEvent', ?1, ?2, ?2
                           )",
                    )
                    .unwrap();
                for sequence in 0..5_000 {
                    let kind = if sequence == 4_999 {
                        "memory.semantic_checkpoint.created"
                    } else {
                        "runtime.progress"
                    };
                    let event_json = serde_json::json!({
                        "event_id": format!("event-{sequence}"),
                        "session_id": "s-late-checkpoint",
                        "sequence": sequence,
                        "scope": "runtime",
                        "kind": kind,
                        "payload": {},
                        "created_at_ms": sequence,
                    })
                    .to_string();
                    statement
                        .execute(params![event_json, sequence as i64])
                        .unwrap();
                }
            }
            tx.commit().unwrap();
        }

        let latest = store
            .get_latest_session_domain_event_by_kind(
                "s-late-checkpoint",
                "memory.semantic_checkpoint.created",
            )
            .unwrap()
            .unwrap();
        assert_eq!(latest.sequence, 4_999);
        let manifest = store
            .get_session_recovery_manifest("s-late-checkpoint")
            .unwrap()
            .unwrap();
        assert_eq!(manifest.event_cursor, 5_000);
        assert_eq!(manifest.latest_checkpoint_sequence, Some(4_999));
        assert_eq!(
            manifest.latest_checkpoint_event_id.as_deref(),
            Some("event-4999")
        );
    }

    #[test]
    fn context_index_reconciliation_is_complete_idempotent_and_repairable() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-index"))
            .unwrap();
        for sequence in 0..513 {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("index-{sequence}"),
                    session_id: "s-context-index".to_string(),
                    sequence,
                    role: "user".to_string(),
                    content_json: serde_json::json!([
                        {"type":"text","text":format!("indexed payload {sequence}")}
                    ])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }
        let first = store
            .reconcile_session_context_index("s-context-index", 128, 4, 1_000)
            .unwrap();
        assert!(first.complete);
        assert_eq!(first.source_messages, 513);
        assert_eq!(first.covered_messages, 513);
        assert_eq!(first.indexed_through_sequence, Some(512));
        assert!(!first.source_digest.is_empty());
        {
            let conn = store.conn().unwrap();
            conn.execute(
                "DELETE FROM session_context_index_cards
                  WHERE card_id=(
                      SELECT card_id FROM session_context_index_cards
                       WHERE session_id='s-context-index' LIMIT 1
                  )",
                [],
            )
            .unwrap();
        }
        let repaired = store
            .reconcile_session_context_index("s-context-index", 128, 4, 2_000)
            .unwrap();
        assert!(repaired.complete);
        assert_eq!(repaired.source_digest, first.source_digest);
        assert_eq!(repaired.generation, first.generation + 1);
        assert_eq!(
            store
                .get_context_index_cards("s-context-index", 64)
                .unwrap()
                .len(),
            repaired.card_count
        );
    }

    #[test]
    fn missing_manifest_rebuilds_from_authoritative_history_and_checkpoint() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-manifest-rebuild"))
            .unwrap();
        store
            .insert_message(&SessionMessage {
                stable_message_id: "manifest-message".to_string(),
                session_id: "s-manifest-rebuild".to_string(),
                sequence: 0,
                role: "user".to_string(),
                content_json: r#"[{"type":"text","text":"authoritative"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 10,
            })
            .unwrap();
        store
            .append_event(&SessionEvent {
                session_id: "s-manifest-rebuild".to_string(),
                event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
                event_json: serde_json::json!({
                    "event_id": "checkpoint-rebuild",
                    "session_id": "s-manifest-rebuild",
                    "sequence": 0,
                    "scope": "runtime",
                    "kind": "memory.semantic_checkpoint.created",
                    "payload": {},
                    "created_at_ms": 11
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 11,
            })
            .unwrap();
        let conn = store.conn().unwrap();
        conn.execute(
            "DELETE FROM session_recovery_manifest WHERE session_id='s-manifest-rebuild'",
            [],
        )
        .unwrap();
        drop(conn);

        let rebuilt = store
            .rebuild_session_recovery_manifest("s-manifest-rebuild", 12)
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt.durable_cursor, 1);
        assert_eq!(rebuilt.event_cursor, 1);
        assert_eq!(rebuilt.transcript_messages, 1);
        assert_eq!(rebuilt.latest_checkpoint_sequence, Some(0));
        assert_eq!(
            rebuilt.latest_checkpoint_event_id.as_deref(),
            Some("checkpoint-rebuild")
        );
        assert!(rebuilt.index_pending);
        let pending: i64 = store
            .conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM session_context_index_outbox
                  WHERE session_id='s-manifest-rebuild' AND status='pending'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 1);
    }

    #[test]
    fn semantic_checkpoint_alone_enqueues_context_index_reconciliation() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-checkpoint-index-outbox"))
            .unwrap();
        store
            .append_event(&SessionEvent {
                session_id: "s-checkpoint-index-outbox".to_string(),
                event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
                event_json: serde_json::json!({
                    "event_id": "checkpoint-only",
                    "session_id": "s-checkpoint-index-outbox",
                    "sequence": 0,
                    "scope": "runtime",
                    "kind": "memory.semantic_checkpoint.created",
                    "payload": {},
                    "created_at_ms": 20
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 20,
            })
            .unwrap();
        let manifest = store
            .get_session_recovery_manifest("s-checkpoint-index-outbox")
            .unwrap()
            .unwrap();
        assert!(manifest.index_pending);
        let pending: i64 = store
            .conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM session_context_index_outbox
                  WHERE session_id='s-checkpoint-index-outbox' AND status='pending'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 1);
    }

    #[test]
    fn message_sequence_page_query_uses_session_sequence_index() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-message-index"))
            .unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT session_id, sequence, role, content_json,
                         blocks_count, tool_use_id, tool_name,
                         token_usage_json, created_at_ms
                  FROM messages
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["s-message-index", 99_950_i64, 50_i64], |row| {
                row.get(3)
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_messages_session_seq"),
            "expected message sequence index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn branch_copy_uses_stable_cutoff_and_rejects_nonempty_target() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("branch-source")).unwrap();
        store.create_session(&make_record("branch-target")).unwrap();
        for sequence in 0..3 {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("source-{sequence}"),
                    session_id: "branch-source".to_string(),
                    sequence,
                    role: "user".to_string(),
                    content_json: format!(r#"[{{"type":"text","text":"{sequence}"}}]"#),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        let copied = store
            .copy_session_messages_at_cutoff("branch-source", "branch-target", 2)
            .unwrap();
        assert_eq!(copied, 2);
        let target = store.get_all_messages("branch-target").unwrap();
        assert_eq!(target.len(), 2);
        assert_eq!(target[0].stable_message_id, "branch:branch-target:source-0");
        assert_eq!(target[1].sequence, 1);
        assert!(store
            .copy_session_messages_at_cutoff("branch-source", "branch-target", 3)
            .is_err());
        assert_eq!(store.get_message_count("branch-source").unwrap(), 3);
    }

    #[test]
    fn test_list_by_platform() {
        let (store, _dir) = make_store();
        let mut rec = make_record("s-tg");
        rec.platform = "telegram".to_string();
        store.create_session(&rec).unwrap();
        store.create_session(&make_record("s-test")).unwrap();
        let tg = store.list_sessions_by_platform("telegram").unwrap();
        assert_eq!(tg.len(), 1);
        assert_eq!(tg[0].session_id, "s-tg");
    }

    #[test]
    fn test_memory_associations() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-mem")).unwrap();
        store.associate_memory("s-mem", "mem-1").unwrap();
        store.associate_memory("s-mem", "mem-2").unwrap();
        // Idempotent
        store.associate_memory("s-mem", "mem-1").unwrap();
        let mems = store.get_session_memories("s-mem").unwrap();
        assert_eq!(mems.len(), 2);
        store.disassociate_memory("s-mem", "mem-1").unwrap();
        let mems = store.get_session_memories("s-mem").unwrap();
        assert_eq!(mems.len(), 1);
        assert_eq!(mems[0], "mem-2");
    }

    #[test]
    fn conn_sets_busy_timeout() {
        let store = SqliteSessionStore::open_in_memory().unwrap();
        let conn = store.conn().unwrap();
        let timeout: i32 = conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert!(timeout > 0, "busy_timeout should be > 0, got {}", timeout);
    }

    #[test]
    fn open_migrates_legacy_message_block_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("legacy-sessions.db");
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY,
                    platform TEXT DEFAULT '',
                    chat_id TEXT DEFAULT '',
                    user_id TEXT DEFAULT '',
                    model TEXT,
                    created_at TEXT NOT NULL DEFAULT '',
                    last_activity TEXT NOT NULL DEFAULT '',
                    message_count INTEGER DEFAULT 0,
                    reset_policy TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT DEFAULT '{}',
                    created_at_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    usage_input INTEGER DEFAULT 0,
                    usage_output INTEGER DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(session_id, sequence)
                );
                CREATE TABLE message_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL,
                    block_order INTEGER NOT NULL,
                    block_type TEXT NOT NULL,
                    text TEXT,
                    signature TEXT,
                    tool_id TEXT,
                    tool_name TEXT,
                    tool_input TEXT,
                    tool_output TEXT,
                    is_error INTEGER DEFAULT 0,
                    created_at_ms INTEGER NOT NULL
                );
                INSERT INTO sessions(session_id, message_count) VALUES ('legacy', 1);
                INSERT INTO messages(session_id, sequence, role, created_at_ms)
                    VALUES ('legacy', 0, 'user', 1);
                INSERT INTO message_blocks(message_id, session_id, block_order, block_type, text, created_at_ms)
                    VALUES (1, 'legacy', 0, 'text', 'resume survives migration', 1);
                "#,
            )
            .unwrap();
        }

        let store = SqliteSessionStore::open(&db).unwrap();
        let messages = store.get_all_messages("legacy").unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0]
            .content_json
            .contains("resume survives migration"));
        store
            .insert_message(&SessionMessage {
                stable_message_id: "legacy:new-write".to_string(),
                session_id: "legacy".to_string(),
                sequence: 1,
                role: "assistant".to_string(),
                content_json: r#"[{"type":"text","text":"new write works"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 2,
            })
            .unwrap();
        assert_eq!(store.get_message_count("legacy").unwrap(), 2);
    }

    #[test]
    fn open_repairs_legacy_duplicate_event_sequences_without_dropping_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("duplicate-events.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                user_id TEXT,
                model TEXT,
                created_at TEXT NOT NULL,
                last_activity TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                reset_policy TEXT NOT NULL,
                metadata_json TEXT,
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at_ms INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE session_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_json TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            INSERT INTO sessions(session_id, platform, chat_id, created_at, last_activity, reset_policy)
            VALUES ('duplicate-session', 'test', 'chat', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', 'None');
            INSERT INTO session_events(session_id, event_type, event_json, sequence, created_at_ms)
            VALUES ('duplicate-session', 'one', '{}', 0, 1),
                   ('duplicate-session', 'two', '{}', 0, 2);
            "#,
        )
        .unwrap();
        drop(conn);

        let store = SqliteSessionStore::open(&path).expect("legacy events are resequenced");
        let events = store.get_events("duplicate-session", 0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| {
                    serde_json::from_str::<serde_json::Value>(&event.event_json)
                        .ok()
                        .and_then(|value| value["sequence"].as_u64())
                })
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1)]
        );
    }

    #[test]
    fn test_prune_before() {
        let (store, _dir) = make_store();
        let mut old = make_record("old-session");
        old.last_activity = "2020-01-01T00:00:00Z".to_string();
        store.create_session(&old).unwrap();
        store.create_session(&make_record("new-session")).unwrap();
        let removed = store.prune_before("2021-01-01T00:00:00Z").unwrap();
        assert_eq!(removed, 1);
        assert!(store.get_session("old-session").unwrap().is_none());
        assert!(store.get_session("new-session").unwrap().is_some());
    }

    fn outbox_message(session_id: &str) -> SessionMessage {
        SessionMessage {
            stable_message_id: "message-1".to_string(),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "user".to_string(),
            content_json: r#"[{"type":"text","text":"run this"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 100,
        }
    }

    fn outbox_request() -> SessionRuntimeOutboxRequest {
        SessionRuntimeOutboxRequest {
            input_id: "input-1".to_string(),
            request_id: "request-1".to_string(),
            turn_id: "turn-1".to_string(),
            message_id: "message-1".to_string(),
            session_generation: 1,
            decision: InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: Some(r#"{"code":"new_turn"}"#.to_string()),
            created_at_ms: 100,
            runtime_options_json: None,
        }
    }

    fn ingress_request(
        id: &str,
        generation: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        created_at_ms: u64,
    ) -> SessionRuntimeOutboxRequest {
        SessionRuntimeOutboxRequest {
            input_id: format!("input-{id}"),
            request_id: format!("request-{id}"),
            turn_id: format!("turn-{id}"),
            message_id: format!("message-{id}"),
            session_generation: generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: Some(format!(r#"{{"classification":"{id}"}}"#)),
            created_at_ms,
            runtime_options_json: None,
        }
    }

    #[test]
    fn source_message_and_outbox_are_atomic_and_idempotent() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-outbox")).unwrap();
        let message = outbox_message("s-outbox");
        let request = outbox_request();

        let first = store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        let duplicate = store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        assert_eq!(first, duplicate);
        assert_eq!(first.status, SessionRuntimeInputStatus::Queued);
        assert_eq!(first.input_id, "input-1");
        assert_eq!(first.revision, 2);
        assert_eq!(
            store
                .get_session_runtime_outbox_by_input_id("input-1")
                .unwrap(),
            Some(first.clone())
        );
        let timeline = store
            .get_session_domain_timeline_limited("s-outbox", 0, 10)
            .unwrap();
        assert_eq!(timeline.len(), 3);
        assert_eq!(
            timeline
                .iter()
                .map(|event| { SessionDomainEvent::from_session_event(event).unwrap().kind })
                .collect::<Vec<_>>(),
            vec![
                SessionRuntimeInputStatus::Accepted
                    .timeline_event_kind()
                    .to_string(),
                SessionRuntimeInputStatus::Classified
                    .timeline_event_kind()
                    .to_string(),
                SessionRuntimeInputStatus::Queued
                    .timeline_event_kind()
                    .to_string(),
            ]
        );
        assert_eq!(store.get_message_count("s-outbox").unwrap(), 1);
        assert_eq!(
            store
                .get_session("s-outbox")
                .unwrap()
                .unwrap()
                .message_count,
            1
        );

        let mut conflicting = request;
        conflicting.turn_id = "turn-other".to_string();
        assert!(store
            .append_message_with_runtime_outbox(&message, &conflicting)
            .is_err());
        assert_eq!(store.get_message_count("s-outbox").unwrap(), 1);
    }

    #[test]
    fn classifier_rejections_are_auditable_terminal_inputs_and_never_runnable() {
        for (suffix, decision, expected_status) in [
            (
                "duplicate",
                InputRoutingDecision::RejectDuplicate,
                SessionRuntimeInputStatus::RejectedDuplicate,
            ),
            (
                "policy",
                InputRoutingDecision::RejectPolicy,
                SessionRuntimeInputStatus::RejectedPolicy,
            ),
        ] {
            let (store, _dir) = make_store();
            let session_id = format!("s-reject-{suffix}");
            store.create_session(&make_record(&session_id)).unwrap();
            let request = ingress_request(suffix, 1, decision, None, 100);

            let stored = store
                .append_ingress_with_runtime_outbox(
                    &session_id,
                    "user",
                    Some(r#"[{"type":"text","text":"classified rejection"}]"#),
                    100,
                    &request,
                )
                .expect("rejection is durable, not a validation error");
            assert_eq!(stored.status, expected_status);
            assert!(stored.status.is_terminal());
            assert_eq!(stored.terminal_at_ms, Some(100));
            assert_eq!(store.get_message_count(&session_id).unwrap(), 1);
            assert!(store
                .claim_session_runtime_outbox("worker", 100, 1_000, 10)
                .unwrap()
                .is_empty());
            assert!(store.active_session_runtime_outbox(10).unwrap().is_empty());

            let timeline = store
                .get_session_domain_timeline_limited(&session_id, 0, 10)
                .unwrap()
                .into_iter()
                .map(|event| SessionDomainEvent::from_session_event(&event).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(timeline.len(), 3);
            assert_eq!(
                timeline
                    .iter()
                    .map(|event| event.kind.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    SessionRuntimeInputStatus::Accepted.timeline_event_kind(),
                    SessionRuntimeInputStatus::Classified.timeline_event_kind(),
                    expected_status.timeline_event_kind(),
                ]
            );
            assert_eq!(
                timeline.last().and_then(|event| event.status.as_deref()),
                Some(expected_status.as_str())
            );
            assert_eq!(
                timeline
                    .last()
                    .and_then(|event| event.payload["decision"].as_str()),
                Some(input_decision_as_str(decision))
            );

            let health = store.session_runtime_outbox_health().unwrap();
            assert_eq!(
                health.rejected_duplicate,
                usize::from(decision == InputRoutingDecision::RejectDuplicate)
            );
            assert_eq!(
                health.rejected_policy,
                usize::from(decision == InputRoutingDecision::RejectPolicy)
            );
        }
    }

    #[test]
    fn runtime_options_remain_opaque_and_durable_with_session_ingress() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-runtime-options"))
            .unwrap();
        let message = outbox_message("s-runtime-options");
        let mut request = outbox_request();
        request.request_id = "request-runtime-options".to_string();
        request.runtime_options_json = Some(
            r#"{"profile":"surface_quick_reply","pre_messages":[{"role":"user","blocks":[]}]}"#
                .to_string(),
        );

        let first = store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        assert_eq!(first.runtime_options_json, request.runtime_options_json);
        let reloaded = store
            .get_session_runtime_outbox(&request.request_id)
            .unwrap()
            .expect("outbox record must persist");
        assert_eq!(reloaded.runtime_options_json, request.runtime_options_json);
    }

    #[test]
    fn claim_returns_only_each_session_runnable_head() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("session-a")).unwrap();
        store.create_session(&make_record("session-b")).unwrap();
        for (session_id, id, timestamp) in [
            ("session-a", "a-1", 100),
            ("session-a", "a-2", 101),
            ("session-b", "b-1", 102),
            ("session-b", "b-2", 103),
        ] {
            store
                .append_ingress_with_runtime_outbox(
                    session_id,
                    "user",
                    Some(r#"[{"type":"text","text":"queued"}]"#),
                    timestamp,
                    &ingress_request(id, 1, InputRoutingDecision::StartNewTurn, None, timestamp),
                )
                .unwrap();
        }

        let first = store
            .claim_session_runtime_outbox("worker", 200, 1_000, 10)
            .unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(
            first
                .iter()
                .map(|record| record.input_id.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["input-a-1", "input-b-1"])
        );
        assert!(store
            .claim_session_runtime_outbox("other", 201, 1_000, 10)
            .unwrap()
            .is_empty());

        let a = first
            .into_iter()
            .find(|record| record.session_id == "session-a")
            .unwrap();
        let token = a.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                &a.request_id,
                "worker",
                a.session_generation,
                &token,
                a.revision,
                202,
            )
            .unwrap();
        store
            .ack_session_runtime_outbox(
                &running.request_id,
                "worker",
                running.session_generation,
                &token,
                running.revision,
                SessionRuntimeInputStatus::Completed,
                1,
                203,
            )
            .unwrap();
        let next = store
            .claim_session_runtime_outbox("worker", 204, 1_000, 10)
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].input_id, "input-a-2");
    }

    #[test]
    fn input_id_drives_reclassify_cancel_and_terminal_outcomes() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("session-input-id"))
            .unwrap();
        let queued = store
            .append_ingress_with_runtime_outbox(
                "session-input-id",
                "user",
                Some(r#"[{"type":"text","text":"supplement"}]"#),
                100,
                &ingress_request(
                    "reclassify",
                    1,
                    InputRoutingDecision::StartNewTurn,
                    None,
                    100,
                ),
            )
            .unwrap();
        let reclassified = store
            .reclassify_session_runtime_outbox(
                "input-reclassify",
                1,
                queued.revision,
                InputRoutingDecision::SupplementCurrentTurn,
                Some("turn-active"),
                Some(r#"{"classification":"supplement"}"#),
                "user",
                "continuation of active turn",
                101,
            )
            .unwrap();
        assert_eq!(reclassified.status, SessionRuntimeInputStatus::Reclassified);
        assert_eq!(reclassified.target_turn_id.as_deref(), Some("turn-active"));
        let claimed = store
            .claim_session_runtime_outbox("worker", 102, 1_000, 1)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                103,
            )
            .unwrap();
        let supplemented = store
            .ack_session_runtime_outbox(
                &running.request_id,
                "worker",
                running.session_generation,
                &token,
                running.revision,
                SessionRuntimeInputStatus::Supplemented,
                7,
                104,
            )
            .unwrap();
        assert_eq!(supplemented.status, SessionRuntimeInputStatus::Supplemented);

        let queued = store
            .append_ingress_with_runtime_outbox(
                "session-input-id",
                "user",
                Some(r#"[{"type":"text","text":"cancel"}]"#),
                105,
                &ingress_request("cancel", 1, InputRoutingDecision::StartNewTurn, None, 105),
            )
            .unwrap();
        let cancelled = store
            .cancel_session_runtime_outbox(
                "input-cancel",
                1,
                queued.revision,
                "user",
                "no longer needed",
                106,
            )
            .unwrap();
        assert_eq!(cancelled.status, SessionRuntimeInputStatus::Cancelled);
        assert_eq!(cancelled.terminal_at_ms, Some(106));
        assert_eq!(
            store
                .get_session_runtime_outbox_by_input_id("input-cancel")
                .unwrap(),
            Some(cancelled)
        );

        store
            .append_ingress_with_runtime_outbox(
                "session-input-id",
                "user",
                None,
                107,
                &ingress_request(
                    "worker-cancel",
                    1,
                    InputRoutingDecision::StartNewTurn,
                    None,
                    107,
                ),
            )
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("worker", 108, 1_000, 1)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                109,
            )
            .unwrap();
        let cancelled_by_owner = store
            .ack_session_runtime_outbox(
                &running.request_id,
                "worker",
                running.session_generation,
                &token,
                running.revision,
                SessionRuntimeInputStatus::Cancelled,
                0,
                110,
            )
            .unwrap();
        assert_eq!(
            cancelled_by_owner.status,
            SessionRuntimeInputStatus::Cancelled
        );
    }

    #[test]
    fn generation_advance_closes_admission_and_fences_stale_claims() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("session-generation"))
            .unwrap();
        assert_eq!(
            store
                .get_session_input_admission("session-generation")
                .unwrap()
                .unwrap(),
            SessionInputAdmission {
                session_id: "session-generation".to_string(),
                generation: 1,
                open: true,
            }
        );
        store
            .append_ingress_with_runtime_outbox(
                "session-generation",
                "user",
                None,
                100,
                &ingress_request(
                    "generation-1",
                    1,
                    InputRoutingDecision::StartNewTurn,
                    None,
                    100,
                ),
            )
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("worker", 101, 1_000, 1)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let closed = store
            .close_session_input_admission("session-generation", 1, "lifecycle", "archive", 102)
            .unwrap();
        assert_eq!(closed.generation, 2);
        assert!(!closed.open);
        assert!(store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                103,
            )
            .is_err());
        let expired = store
            .get_session_runtime_outbox_by_input_id("input-generation-1")
            .unwrap()
            .unwrap();
        assert_eq!(expired.status, SessionRuntimeInputStatus::Expired);
        assert_eq!(expired.terminal_at_ms, Some(102));
        assert!(store
            .append_ingress_with_runtime_outbox(
                "session-generation",
                "user",
                None,
                104,
                &ingress_request(
                    "generation-2-closed",
                    2,
                    InputRoutingDecision::StartNewTurn,
                    None,
                    104,
                ),
            )
            .is_err());
        let reopened = store
            .advance_session_input_generation(
                "session-generation",
                2,
                true,
                "branch",
                "new branch authority",
                105,
            )
            .unwrap();
        assert_eq!(reopened.generation, 3);
        assert!(reopened.open);
        assert!(store
            .append_ingress_with_runtime_outbox(
                "session-generation",
                "user",
                None,
                106,
                &ingress_request(
                    "generation-3",
                    3,
                    InputRoutingDecision::StartNewTurn,
                    None,
                    106,
                ),
            )
            .is_ok());
    }

    #[test]
    fn claimed_target_loss_reclassifies_and_requeues_under_owner_fence() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("session-target-loss"))
            .unwrap();
        store
            .append_ingress_with_runtime_outbox(
                "session-target-loss",
                "user",
                None,
                100,
                &ingress_request(
                    "target-loss",
                    1,
                    InputRoutingDecision::SupplementCurrentTurn,
                    Some("turn-ended"),
                    100,
                ),
            )
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("worker", 101, 1_000, 1)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        assert!(store
            .requeue_claimed_session_runtime_outbox(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                "wrong-token",
                claimed.revision,
                InputRoutingDecision::StartNewTurn,
                None,
                Some(r#"{"classification":"target_ended"}"#),
                "target turn no longer exists",
                102,
            )
            .is_err());
        let requeued = store
            .requeue_claimed_session_runtime_outbox(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                InputRoutingDecision::StartNewTurn,
                None,
                Some(r#"{"classification":"target_ended"}"#),
                "target turn no longer exists",
                102,
            )
            .unwrap();
        assert_eq!(requeued.status, SessionRuntimeInputStatus::Reclassified);
        assert_eq!(requeued.decision, InputRoutingDecision::StartNewTurn);
        assert_eq!(requeued.target_turn_id, None);
        assert_eq!(requeued.claim_owner, None);
        assert_eq!(requeued.claim_token, None);
        assert!(store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                103,
            )
            .is_err());
        let reclaimed = store
            .claim_session_runtime_outbox("worker-next", 103, 1_000, 1)
            .unwrap()
            .remove(0);
        assert_eq!(reclaimed.input_id, claimed.input_id);
        assert_ne!(reclaimed.claim_token, Some(token));
        let timeline = store
            .get_session_domain_timeline_limited("session-target-loss", 0, 10)
            .unwrap();
        assert_eq!(timeline.len(), 4);
        assert!(timeline[3]
            .event_json
            .contains("target turn no longer exists"));
    }

    #[test]
    fn source_transaction_rolls_back_when_outbox_identity_conflicts() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-rollback")).unwrap();
        let first = outbox_message("s-rollback");
        let request = outbox_request();
        store
            .append_message_with_runtime_outbox(&first, &request)
            .unwrap();

        let mut second = first;
        second.sequence = 1;
        let conflicting = SessionRuntimeOutboxRequest {
            input_id: "input-2".to_string(),
            request_id: "request-2".to_string(),
            turn_id: "turn-2".to_string(),
            message_id: "message-1".to_string(),
            session_generation: 1,
            decision: InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            created_at_ms: 101,
            runtime_options_json: None,
        };
        assert!(store
            .append_message_with_runtime_outbox(&second, &conflicting)
            .is_err());
        assert_eq!(store.get_message_count("s-rollback").unwrap(), 1);
        assert!(store
            .get_session_runtime_outbox("request-2")
            .unwrap()
            .is_none());
    }

    #[test]
    fn duplicate_input_id_rolls_back_message_and_outbox_atomically() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-input-identity"))
            .unwrap();
        store
            .append_ingress_with_runtime_outbox(
                "s-input-identity",
                "user",
                None,
                100,
                &ingress_request("identity", 1, InputRoutingDecision::StartNewTurn, None, 100),
            )
            .unwrap();
        let mut duplicate = ingress_request(
            "other-request",
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            101,
        );
        duplicate.input_id = "input-identity".to_string();
        assert!(store
            .append_ingress_with_runtime_outbox("s-input-identity", "user", None, 101, &duplicate,)
            .is_err());
        assert_eq!(store.get_message_count("s-input-identity").unwrap(), 1);
        assert!(store
            .get_session_runtime_outbox(&duplicate.request_id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn legacy_runtime_outbox_schema_migrates_in_place_and_remains_readable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-runtime-outbox.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r"
                PRAGMA foreign_keys=ON;
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    user_id TEXT,
                    model TEXT,
                    created_at TEXT NOT NULL,
                    last_activity TEXT NOT NULL,
                    message_count INTEGER NOT NULL DEFAULT 0,
                    reset_policy TEXT NOT NULL,
                    metadata_json TEXT,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stable_message_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    blocks_count INTEGER NOT NULL DEFAULT 1,
                    tool_use_id TEXT,
                    tool_name TEXT,
                    token_usage_json TEXT,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(session_id, sequence)
                );
                CREATE TABLE session_runtime_outbox (
                    request_id TEXT PRIMARY KEY,
                    turn_id TEXT NOT NULL UNIQUE,
                    message_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    runtime_commit_cursor INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at_ms INTEGER NOT NULL,
                    claim_owner TEXT,
                    claim_expires_at_ms INTEGER,
                    failure_class TEXT,
                    last_error TEXT,
                    revision INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    runtime_options_json TEXT
                );
                INSERT INTO sessions (
                    session_id, platform, chat_id, created_at, last_activity,
                    reset_policy, created_at_ms, updated_at_ms
                ) VALUES (
                    'legacy-session', 'test', 'chat', '2024-01-01T00:00:00Z',
                    '2024-01-01T00:00:00Z', 'None', 100, 100
                );
                INSERT INTO messages (
                    stable_message_id, session_id, sequence, role,
                    content_json, created_at_ms
                ) VALUES (
                    'legacy-message', 'legacy-session', 0, 'user', '[]', 100
                );
                INSERT INTO messages (
                    stable_message_id, session_id, sequence, role,
                    content_json, created_at_ms
                ) VALUES (
                    'legacy-running-message', 'legacy-session', 1, 'user', '[]', 121
                );
                INSERT INTO session_runtime_outbox (
                    request_id, turn_id, message_id, session_id, sequence,
                    status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                    revision, created_at_ms, updated_at_ms
                ) VALUES (
                    'legacy-request', 'legacy-turn', 'legacy-message',
                    'legacy-session', 0, 'materialized', 9, 1, 100, 4, 100, 120
                );
                INSERT INTO session_runtime_outbox (
                    request_id, turn_id, message_id, session_id, sequence,
                    status, attempts, next_attempt_at_ms, claim_owner,
                    claim_expires_at_ms, revision, created_at_ms, updated_at_ms
                ) VALUES (
                    'legacy-running-request', 'legacy-running-turn',
                    'legacy-running-message', 'legacy-session', 1, 'running',
                    1, 100, 'legacy-worker', 999, 4, 121, 121
                );
                ",
            )
            .unwrap();
        }

        let store = SqliteSessionStore::open(&path).unwrap();
        let migrated = store
            .get_session_runtime_outbox("legacy-request")
            .unwrap()
            .unwrap();
        assert_eq!(migrated.input_id, "legacy-request");
        assert_eq!(migrated.session_generation, 1);
        assert_eq!(migrated.decision, InputRoutingDecision::StartNewTurn);
        assert_eq!(migrated.status, SessionRuntimeInputStatus::Completed);
        assert_eq!(migrated.terminal_at_ms, Some(120));
        let migrated_running = store
            .get_session_runtime_outbox("legacy-running-request")
            .unwrap()
            .unwrap();
        assert_eq!(migrated_running.status, SessionRuntimeInputStatus::Running);
        assert!(migrated_running
            .claim_token
            .as_deref()
            .is_some_and(|token| token.starts_with("legacy:legacy-running-request:4")));
        assert_eq!(migrated_running.claim_fence_epoch, Some(4));
        assert_eq!(
            store
                .get_session_input_admission("legacy-session")
                .unwrap()
                .unwrap()
                .generation,
            1
        );
    }

    #[test]
    fn multiple_supplements_keep_distinct_turn_identities_for_one_target() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("session-supplements"))
            .unwrap();

        let first = store
            .append_ingress_with_runtime_outbox(
                "session-supplements",
                "user",
                Some(r#"[{"type":"text","text":"first supplement"}]"#),
                100,
                &ingress_request(
                    "supplement-1",
                    1,
                    InputRoutingDecision::SupplementCurrentTurn,
                    Some("turn-active"),
                    100,
                ),
            )
            .unwrap();
        let second = store
            .append_ingress_with_runtime_outbox(
                "session-supplements",
                "user",
                Some(r#"[{"type":"text","text":"second supplement"}]"#),
                101,
                &ingress_request(
                    "supplement-2",
                    1,
                    InputRoutingDecision::SupplementCurrentTurn,
                    Some("turn-active"),
                    101,
                ),
            )
            .unwrap();

        assert_ne!(first.turn_id, second.turn_id);
        assert_eq!(first.target_turn_id.as_deref(), Some("turn-active"));
        assert_eq!(second.target_turn_id.as_deref(), Some("turn-active"));
    }

    fn mission_outbox_request(
        session_id: &str,
        operation: SessionMissionOutboxOperation,
    ) -> SessionMissionOutboxRequest {
        SessionMissionOutboxRequest {
            request_id: format!("mission:workspace-a:{:?}:{session_id}", operation),
            session_id: session_id.to_string(),
            title: format!("Session {session_id}"),
            workspace_key: "workspace-a".to_string(),
            operation,
            created_at_ms: 100,
        }
    }

    #[test]
    fn session_and_mission_registration_are_atomic_idempotent_and_workspace_scoped() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-outbox");
        let request =
            mission_outbox_request("s-mission-outbox", SessionMissionOutboxOperation::Register);

        let first = store
            .upsert_session_with_mission_outbox(&record, &request)
            .unwrap();
        let duplicate = store
            .upsert_session_with_mission_outbox(&record, &request)
            .unwrap();
        assert_eq!(first, duplicate);
        assert!(store.get_session("s-mission-outbox").unwrap().is_some());
        assert!(store
            .claim_session_mission_outbox("workspace-other", "worker", 100, 50, 10)
            .unwrap()
            .is_empty());
        let claimed = store
            .claim_session_mission_outbox("workspace-a", "worker", 100, 50, 10)
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let done = store
            .ack_session_mission_outbox(&request.request_id, "worker", claimed[0].revision, 101)
            .unwrap();
        assert_eq!(done.status, OutboxStatus::Materialized);
    }

    #[test]
    fn mission_registration_ignores_display_title_drift_for_the_same_lifecycle_intent() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-title");
        let request =
            mission_outbox_request("s-mission-title", SessionMissionOutboxOperation::Register);
        let first = store
            .upsert_session_with_mission_outbox(&record, &request)
            .unwrap();

        let mut hydrated_record = record;
        hydrated_record.metadata_json = Some(r#"{"title":"Runtime hydrated title"}"#.to_string());
        let mut hydrated_request = request;
        hydrated_request.title = "Runtime hydrated title".to_string();
        let duplicate = store
            .upsert_session_with_mission_outbox(&hydrated_record, &hydrated_request)
            .unwrap();

        assert_eq!(first.request_id, duplicate.request_id);
        assert_eq!(first.title, duplicate.title);
    }

    #[test]
    fn session_mission_upsert_preserves_existing_ingress_message_and_runtime_outbox() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-preserve-ingress");
        let mission = mission_outbox_request(
            "s-mission-preserve-ingress",
            SessionMissionOutboxOperation::Register,
        );
        store
            .upsert_session_with_mission_outbox(&record, &mission)
            .unwrap();
        let ingress = SessionRuntimeOutboxRequest {
            input_id: "input-preserve".to_string(),
            request_id: "ingress-preserve".to_string(),
            turn_id: "turn-preserve".to_string(),
            message_id: "message-preserve".to_string(),
            session_generation: 1,
            decision: InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            created_at_ms: 101,
            runtime_options_json: None,
        };
        store
            .append_ingress_with_runtime_outbox(
                &record.session_id,
                "user",
                Some(r#"[{"type":"text","text":"preserve this turn"}]"#),
                101,
                &ingress,
            )
            .unwrap();

        let mut refreshed = record;
        refreshed.metadata_json = Some(r#"{"title":"Refreshed session"}"#.to_string());
        let mut refreshed_mission = mission;
        refreshed_mission.title = "Refreshed session".to_string();
        store
            .upsert_session_with_mission_outbox(&refreshed, &refreshed_mission)
            .unwrap();

        assert_eq!(store.get_message_count(&refreshed.session_id).unwrap(), 1);
        assert!(store
            .get_session_runtime_outbox(&ingress.request_id)
            .unwrap()
            .is_some());
    }

    #[test]
    fn session_delete_queues_close_without_foreign_key_loss() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-close");
        store
            .upsert_session_with_mission_outbox(
                &record,
                &mission_outbox_request("s-mission-close", SessionMissionOutboxOperation::Register),
            )
            .unwrap();
        let close = mission_outbox_request("s-mission-close", SessionMissionOutboxOperation::Close);
        assert!(store.delete_session_with_mission_outbox(&close).unwrap());
        assert!(store.get_session("s-mission-close").unwrap().is_none());
        let claimed = store
            .claim_session_mission_outbox("workspace-a", "worker", 100, 50, 10)
            .unwrap();
        assert!(claimed.iter().any(|record| {
            record.request_id == close.request_id
                && record.operation == SessionMissionOutboxOperation::Close
        }));
    }

    #[test]
    fn outbox_claim_lease_retry_block_manual_retry_and_ack_are_guarded() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-lifecycle")).unwrap();
        store
            .append_message_with_runtime_outbox(&outbox_message("s-lifecycle"), &outbox_request())
            .unwrap();

        let first = store
            .claim_session_runtime_outbox("worker-a", 100, 50, 10)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].attempts, 1);
        assert!(store
            .claim_session_runtime_outbox("worker-b", 149, 50, 10)
            .unwrap()
            .is_empty());
        let reclaimed = store
            .claim_session_runtime_outbox("worker-b", 150, 50, 10)
            .unwrap();
        assert_eq!(reclaimed[0].attempts, 2);
        let reclaimed_token = reclaimed[0].claim_token.clone().unwrap();

        let retry = store
            .fail_session_runtime_outbox(
                "request-1",
                "worker-b",
                reclaimed[0].session_generation,
                &reclaimed_token,
                reclaimed[0].revision,
                OutboxFailureClass::Retryable,
                "runtime unavailable",
                250,
                3,
                151,
            )
            .unwrap();
        assert_eq!(retry.status, SessionRuntimeInputStatus::Queued);
        assert!(store
            .claim_session_runtime_outbox("worker-c", 249, 50, 10)
            .unwrap()
            .is_empty());
        let final_claim = store
            .claim_session_runtime_outbox("worker-c", 250, 50, 10)
            .unwrap();
        assert_eq!(final_claim[0].attempts, 3);
        let final_token = final_claim[0].claim_token.clone().unwrap();
        let blocked = store
            .fail_session_runtime_outbox(
                "request-1",
                "worker-c",
                final_claim[0].session_generation,
                &final_token,
                final_claim[0].revision,
                OutboxFailureClass::AuthorizationBlocked,
                "retry exhausted",
                500,
                3,
                251,
            )
            .unwrap();
        assert_eq!(blocked.status, SessionRuntimeInputStatus::Blocked);

        let pending = store
            .retry_blocked_session_runtime_outbox(
                "request-1",
                blocked.session_generation,
                blocked.revision,
                "operator-1",
                "runtime recovered",
                300,
            )
            .unwrap();
        assert_eq!(pending.status, SessionRuntimeInputStatus::Queued);
        assert_eq!(pending.attempts, 3);
        let claimed = store
            .claim_session_runtime_outbox("worker-d", 300, 50, 10)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                "request-1",
                "worker-d",
                claimed.session_generation,
                &token,
                claimed.revision,
                301,
            )
            .unwrap();
        let done = store
            .ack_session_runtime_outbox(
                "request-1",
                "worker-d",
                running.session_generation,
                &token,
                running.revision,
                SessionRuntimeInputStatus::Completed,
                42,
                302,
            )
            .unwrap();
        assert_eq!(done.status, SessionRuntimeInputStatus::Completed);
        assert_eq!(done.terminal_at_ms, Some(302));
        assert_eq!(done.runtime_commit_cursor, Some(42));
        assert_eq!(store.session_runtime_outbox_health().unwrap().completed, 1);
    }

    #[test]
    fn outbox_lease_renewal_rejects_stale_ack_and_prevents_reclaim() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-renew")).unwrap();
        store
            .append_message_with_runtime_outbox(&outbox_message("s-renew"), &outbox_request())
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("worker-a", 100, 50, 1)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let renewed = store
            .renew_session_runtime_outbox_lease(
                "request-1",
                "worker-a",
                claimed.session_generation,
                &token,
                claimed.revision,
                140,
                50,
            )
            .unwrap();
        assert!(store
            .claim_session_runtime_outbox("worker-b", 151, 50, 1)
            .unwrap()
            .is_empty());
        assert!(store
            .mark_session_runtime_outbox_running(
                "request-1",
                "worker-a",
                claimed.session_generation,
                &token,
                claimed.revision,
                152,
            )
            .is_err());
        let running = store
            .mark_session_runtime_outbox_running(
                "request-1",
                "worker-a",
                renewed.session_generation,
                &token,
                renewed.revision,
                153,
            )
            .unwrap();
        assert!(store
            .ack_session_runtime_outbox(
                "request-1",
                "worker-a",
                running.session_generation,
                "wrong-token",
                running.revision,
                SessionRuntimeInputStatus::Completed,
                7,
                154,
            )
            .is_err());
        let done = store
            .ack_session_runtime_outbox(
                "request-1",
                "worker-a",
                running.session_generation,
                &token,
                running.revision,
                SessionRuntimeInputStatus::Completed,
                7,
                154,
            )
            .unwrap();
        assert_eq!(done.status, SessionRuntimeInputStatus::Completed);
    }

    #[test]
    fn recovery_manifest_tracks_transcript_outbox_and_external_signals() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-recovery")).unwrap();
        let initial = store
            .get_session_recovery_manifest("s-recovery")
            .unwrap()
            .unwrap();
        assert_eq!(initial.transcript_messages, 0);
        assert!(!initial.requires_hydration());

        let mut message = outbox_message("s-recovery");
        message.stable_message_id = "recovery-message".to_string();
        message.content_json = r#"[{"type":"text","text":"恢复中文"}]"#.to_string();
        let mut request = outbox_request();
        request.message_id = message.stable_message_id.clone();
        request.request_id = "recovery-request".to_string();
        request.turn_id = "recovery-turn".to_string();
        store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        let pending = store
            .get_session_recovery_manifest("s-recovery")
            .unwrap()
            .unwrap();
        assert_eq!(pending.durable_cursor, 1);
        assert_eq!(pending.transcript_messages, 1);
        let expected_bytes = message.stable_message_id.len()
            + message.session_id.len()
            + message.role.len()
            + message.content_json.len()
            + message.token_usage_json.as_ref().map_or(0, String::len)
            + message.tool_use_id.as_ref().map_or(0, String::len)
            + message.tool_name.as_ref().map_or(0, String::len);
        assert_eq!(pending.transcript_bytes, expected_bytes as u64);
        assert!(pending.in_flight_turn);

        let claimed = store
            .claim_session_runtime_outbox("worker", 100, 1_000, 1)
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                101,
            )
            .unwrap();
        store
            .ack_session_runtime_outbox(
                &running.request_id,
                "worker",
                running.session_generation,
                &token,
                running.revision,
                SessionRuntimeInputStatus::Completed,
                1,
                102,
            )
            .unwrap();
        let settled = store
            .set_session_recovery_signal(
                "s-recovery",
                SessionRecoverySignal::PendingApproval,
                true,
                103,
            )
            .unwrap();
        assert!(!settled.in_flight_turn);
        assert!(settled.pending_approval);
        assert!(settled.requires_hydration());
        assert_eq!(
            store
                .list_active_session_recovery_manifests(0, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn recovery_manifest_backfills_existing_transcript_without_body_loss() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("recovery-backfill.db");
        {
            let store = SqliteSessionStore::open(&path).unwrap();
            store.create_session(&make_record("s-backfill")).unwrap();
            store.insert_message(&outbox_message("s-backfill")).unwrap();
            let connection = store.conn().unwrap();
            connection
                .execute_batch(
                    "DROP TRIGGER IF EXISTS session_recovery_session_insert;
                     DROP TRIGGER IF EXISTS session_recovery_session_update;
                     DROP TRIGGER IF EXISTS session_recovery_message_insert;
                     DROP TRIGGER IF EXISTS session_recovery_message_delete;
                     DROP TRIGGER IF EXISTS session_recovery_message_update;
                     DROP TRIGGER IF EXISTS session_recovery_lifecycle_event_insert;
                     DROP TRIGGER IF EXISTS session_recovery_runtime_outbox_insert;
                     DROP TRIGGER IF EXISTS session_recovery_runtime_outbox_update;
                     DROP TRIGGER IF EXISTS session_recovery_mission_outbox_insert;
                     DROP TRIGGER IF EXISTS session_recovery_mission_outbox_update;
                     DROP TABLE session_recovery_manifest;",
                )
                .unwrap();
        }
        let reopened = SqliteSessionStore::open(&path).unwrap();
        let manifest = reopened
            .get_session_recovery_manifest("s-backfill")
            .unwrap()
            .unwrap();
        assert_eq!(manifest.transcript_messages, 1);
        assert_eq!(manifest.durable_cursor, 1);
        assert!(manifest.transcript_bytes > 0);
    }
}
