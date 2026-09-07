//! Backend-independent durable Session data transfer objects and index helpers.
//!
//! Database adapters own row decoding and SQL errors. These values define the
//! stable Session port shared by PostgreSQL, Runtime, Gateway, and surfaces.

use harness_contract::{task::TaskRouteHint, turn::InputRoutingDecision};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SessionBranchActivation, SessionLifecycleTransition};

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

    pub fn parse(value: &str) -> crate::error::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "materialized" => Ok(Self::Materialized),
            "blocked_materialization" => Ok(Self::BlockedMaterialization),
            other => Err(crate::SessionError::Store(format!(
                "unknown session runtime outbox status `{other}`"
            ))),
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

    pub fn parse(value: &str) -> crate::error::Result<Self> {
        match value {
            "retryable" => Ok(Self::Retryable),
            "permanent" => Ok(Self::Permanent),
            "authorization_blocked" => Ok(Self::AuthorizationBlocked),
            "corrupt_payload" => Ok(Self::CorruptPayload),
            other => Err(crate::SessionError::Store(format!(
                "unknown session runtime outbox failure class `{other}`"
            ))),
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

    pub fn parse(value: &str) -> crate::error::Result<Self> {
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
            other => Err(crate::SessionError::Store(format!(
                "unknown session runtime input status `{other}`"
            ))),
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
