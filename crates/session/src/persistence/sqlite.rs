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
use harness_contract::{task::TaskRouteHint, turn::InputRoutingDecision};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlite_pool_tracker::SqlitePoolGuard;

use crate::{
    domain::{
        SessionBranchActivation, SessionBranchActivationPhase, SessionBranchActivationTransition,
        SessionDomainEvent, SessionDomainRef, SessionDomainScope, SessionLifecycleIntent,
        SessionLifecyclePhase, SessionLifecyclePlan, SessionLifecycleTransition,
        SESSION_DOMAIN_EVENT_TYPE,
    },
    error::SessionError,
    persistence::{
        domain::{
            ingress::{
                applied_input_projection, decision_requires_target_turn, input_decision_as_str,
                parse_input_decision as parse_input_decision_value,
            },
            lifecycle::{validate_fence_metadata, validate_plan_identity},
            query::bounded_limit,
            terminal::{validate_terminal_commit, validate_terminal_transcript},
        },
        Result,
    },
};

#[path = "sqlite/ingress.rs"]
mod ingress;
#[path = "sqlite/lifecycle.rs"]
mod lifecycle;
#[path = "sqlite/query.rs"]
mod query;
#[path = "sqlite/terminal.rs"]
mod terminal;

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
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionUsageSummary {
    pub session_count: usize,
    pub message_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_route_hint: Option<TaskRouteHint>,
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
    /// Delivered to the target Runtime turn, but not yet covered by that
    /// turn's durable terminal commit cursor.
    Attached,
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
            Self::Attached => "attached",
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
            "attached" => Ok(Self::Attached),
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
            Self::Attached => "session.input.attached.v1",
            Self::Completed => "session.input.completed.v1",
            Self::Supplemented => "session.input.supplemented.v1",
            Self::Failed => "session.input.failed.v1",
            Self::Blocked => "session.input.blocked.v1",
            Self::Cancelled => "session.input.cancelled.v1",
            Self::Expired => "session.input.expired.v1",
        }
    }
}

fn parse_input_decision(value: &str) -> rusqlite::Result<InputRoutingDecision> {
    parse_input_decision_value(value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("unknown session input decision `{value}`").into(),
        )
    })
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_route_hint: Option<TaskRouteHint>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_receipt:
        Option<harness_contract::input_disposition::SessionInputApplicationReceipt>,
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
    pub attached: usize,
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
    pub event: SessionEvent,
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

/// Mutable Session presence projection.
///
/// Reader/writer attachments are online coordination state, not an immutable
/// business journal. The selected Session backend keeps exactly one row per
/// Session so repeated Surface reaffirmation cannot amplify event history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPresenceProjection {
    pub session_id: String,
    pub state: String,
    pub attachments_json: String,
    pub next_sequence: usize,
    pub revision: u64,
    pub updated_at_ms: u64,
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
    ensure_session_operation_schema(conn)?;
    ensure_session_recovery_manifest_schema(conn)?;
    ensure_session_presence_projection_schema(conn)?;

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
            0,
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

        "#,
    )
    .map_err(sql_err)
}

fn ensure_session_presence_projection_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_presence_projection (
            session_id TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            attachments_json TEXT NOT NULL,
            next_sequence INTEGER NOT NULL,
            revision INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            CHECK (json_valid(attachments_json)),
            CHECK (json_type(attachments_json) = 'array')
        );

        DROP TRIGGER IF EXISTS session_recovery_lifecycle_event_insert;
        DELETE FROM session_events WHERE event_type = 'session.lifecycle.v1';

        UPDATE session_recovery_manifest
           SET active_writer_or_attachment = COALESCE((
                   SELECT json_array_length(presence.attachments_json) > 0
                     FROM session_presence_projection AS presence
                    WHERE presence.session_id = session_recovery_manifest.session_id
               ), 0);

        CREATE TRIGGER IF NOT EXISTS session_recovery_presence_insert
        AFTER INSERT ON session_presence_projection BEGIN
            UPDATE session_recovery_manifest
               SET active_writer_or_attachment =
                       json_array_length(NEW.attachments_json) > 0,
                   last_activity_ms = MAX(last_activity_ms, NEW.updated_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;

        CREATE TRIGGER IF NOT EXISTS session_recovery_presence_update
        AFTER UPDATE ON session_presence_projection BEGIN
            UPDATE session_recovery_manifest
               SET active_writer_or_attachment =
                       json_array_length(NEW.attachments_json) > 0,
                   last_activity_ms = MAX(last_activity_ms, NEW.updated_at_ms),
                   manifest_revision = manifest_revision + 1
             WHERE session_id = NEW.session_id;
        END;

        CREATE TRIGGER IF NOT EXISTS session_recovery_presence_delete
        AFTER DELETE ON session_presence_projection BEGIN
            UPDATE session_recovery_manifest
               SET active_writer_or_attachment = 0,
                   manifest_revision = manifest_revision + 1
             WHERE session_id = OLD.session_id;
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
            task_route_hint_json TEXT,
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
            application_receipt_json TEXT,
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
    if !columns.contains("task_route_hint_json") {
        conn.execute(
            "ALTER TABLE session_runtime_outbox ADD COLUMN task_route_hint_json TEXT",
            [],
        )
        .map_err(sql_err)?;
    }
    if !columns.contains("application_receipt_json") {
        conn.execute(
            "ALTER TABLE session_runtime_outbox ADD COLUMN application_receipt_json TEXT",
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
        CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_target_turn
            ON session_runtime_outbox(target_turn_id, session_id, session_generation, sequence)
            WHERE target_turn_id IS NOT NULL;
        ",
    )
    .map_err(sql_err)?;
    Ok(())
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
        status: row.get(12)?,
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
        task_route_hint: row
            .get::<_, Option<String>>(10)?
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        10,
                        rusqlite::types::Type::Text,
                        error.into(),
                    )
                })
            })
            .transpose()?,
        status: SessionRuntimeInputStatus::parse(&row.get::<_, String>(11)?)?,
        runtime_commit_cursor: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
        attempts: row.get::<_, i64>(13)? as u32,
        next_attempt_at_ms: row.get::<_, i64>(14)? as u64,
        claim_owner: row.get(15)?,
        claim_token: row.get(16)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(17)?.map(|value| value as u64),
        failure_class: row
            .get::<_, Option<String>>(18)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(19)?,
        revision: row.get::<_, i64>(20)? as u64,
        created_at_ms: row.get::<_, i64>(21)? as u64,
        updated_at_ms: row.get::<_, i64>(22)? as u64,
        terminal_at_ms: row.get::<_, Option<i64>>(23)?.map(|value| value as u64),
        runtime_options_json: row.get(24)?,
        claim_fence_epoch: row.get::<_, Option<i64>>(25)?.map(|value| value as u64),
        application_receipt: row
            .get::<_, Option<String>>(26)?
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        26,
                        rusqlite::types::Type::Text,
                        error.into(),
                    )
                })
            })
            .transpose()?,
    })
}

fn query_outbox(conn: &Connection, request_id: &str) -> Result<Option<SessionRuntimeOutboxRecord>> {
    conn.query_row(
        r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                  session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                  status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                  claim_owner, claim_token, claim_expires_at_ms, failure_class,
                  last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                  runtime_options_json, claim_fence_epoch, application_receipt_json
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
                  session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                  status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                  claim_owner, claim_token, claim_expires_at_ms, failure_class,
                  last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                  runtime_options_json, claim_fence_epoch, application_receipt_json
             FROM session_runtime_outbox WHERE input_id = ?1",
        params![input_id],
        row_to_outbox,
    )
    .optional()
    .map_err(sql_err)
}

fn runtime_turn_is_terminal(
    conn: &Connection,
    session_id: &str,
    session_generation: u64,
    turn_id: &str,
) -> Result<bool> {
    conn.query_row(
        r"SELECT EXISTS(
              SELECT 1
                FROM session_runtime_outbox
               WHERE session_id=?1 AND session_generation=?2 AND turn_id=?3
                 AND status IN (
                   'rejected_duplicate','rejected_policy','completed','supplemented',
                   'failed','cancelled','expired'
                 )
            )",
        params![session_id, session_generation as i64, turn_id],
        |row| row.get(0),
    )
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
        task_route_hint: record.task_route_hint.clone(),
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
             session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
             status, attempts, next_attempt_at_ms, revision, created_at_ms,
             updated_at_ms, runtime_options_json)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                   'accepted', 0, ?12, 0, ?12, ?12, ?13)",
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
            request
                .task_route_hint
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| SessionError::Store(error.to_string()))?,
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
    _pool_tracker: SqlitePoolGuard,
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
#[path = "sqlite/tests.rs"]
mod tests;
