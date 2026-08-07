//! Complete durable session backend contract.
//!
//! Application code uses [`super::UnifiedSessionStore`].
//! This port is intentionally synchronous because the existing SQLite and
//! PostgreSQL executors own bounded connection pools; the public facade keeps
//! its async API without adding a process-wide mutex around database work.

use crate::error::Result;
use crate::persistence::sqlite::{
    ContextIndexCard, ContextIndexCoverage, OutboxFailureClass, SessionBranchRequest,
    SessionBranchResult, SessionEvent, SessionInputAdmission, SessionLifecycleFenceRequest,
    SessionLifecycleTombstoneRequest, SessionListOptions, SessionListPage, SessionMessage,
    SessionMessageMetadata, SessionMissionOutboxRecord, SessionMissionOutboxRequest,
    SessionPresenceProjection, SessionRecord, SessionRecoveryManifest, SessionRecoverySignal,
    SessionRuntimeInputStatus, SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord,
    SessionRuntimeOutboxRequest, SessionSearchResult, SessionSnapshot,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageSummary,
    SqliteSessionStore,
};
use crate::{
    SessionBranchActivation, SessionBranchActivationTransition, SessionLifecycleIntent,
    SessionLifecyclePlan, SessionLifecycleTransition,
};

macro_rules! session_store_backend_contract {
    ($macro:ident) => {
        $macro! {
            (create_session, (session: &SessionRecord), Result<()>),
            (get_session, (session_id: &str), Result<Option<SessionRecord>>),
            (get_sessions_by_ids, (session_ids: &[String]), Result<Vec<SessionRecord>>),
            (get_session_recovery_manifest, (session_id: &str), Result<Option<SessionRecoveryManifest>>),
            (get_session_presence_projection, (session_id: &str), Result<Option<SessionPresenceProjection>>),
            (upsert_session_presence_projection, (projection: &SessionPresenceProjection), Result<()>),
            (compare_and_upsert_session_presence_projection, (projection: &SessionPresenceProjection, expected_revision: Option<u64>), Result<bool>),
            (delete_session_presence_projection, (session_id: &str), Result<()>),
            (get_session_recovery_manifests_by_ids, (session_ids: &[String]), Result<Vec<SessionRecoveryManifest>>),
            (rebuild_session_recovery_manifest, (session_id: &str, now_ms: u64), Result<Option<SessionRecoveryManifest>>),
            (list_active_session_recovery_manifests, (offset: usize, limit: usize), Result<Vec<SessionRecoveryManifest>>),
            (list_required_session_recovery_manifests, (offset: usize, limit: usize), Result<Vec<SessionRecoveryManifest>>),
            (set_session_recovery_signal, (session_id: &str, signal: SessionRecoverySignal, active: bool, observed_at_ms: u64), Result<SessionRecoveryManifest>),
            (update_session, (session: &SessionRecord), Result<()>),
            (upsert_session, (session: &SessionRecord), Result<()>),
            (upsert_session_with_mission_outbox, (session: &SessionRecord, request: &SessionMissionOutboxRequest), Result<SessionMissionOutboxRecord>),
            (plan_session_lifecycle, (plan: &SessionLifecyclePlan), Result<SessionLifecycleIntent>),
            (get_session_lifecycle_intent, (operation_id: &str), Result<Option<SessionLifecycleIntent>>),
            (list_recoverable_session_lifecycle_intents, (limit: usize), Result<Vec<SessionLifecycleIntent>>),
            (fence_session_lifecycle, (request: &SessionLifecycleFenceRequest), Result<SessionLifecycleIntent>),
            (transition_session_lifecycle, (transition: &SessionLifecycleTransition), Result<SessionLifecycleIntent>),
            (commit_session_lifecycle_tombstone, (request: &SessionLifecycleTombstoneRequest), Result<SessionLifecycleIntent>),
            (delete_session, (session_id: &str), Result<()>),
            (delete_session_with_mission_outbox, (request: &SessionMissionOutboxRequest), Result<bool>),
            (mark_session_closed, (session_id: &str), Result<()>),
            (list_sessions, (), Result<Vec<SessionRecord>>),
            (list_sessions_page, (opts: &SessionListOptions<'_>), Result<SessionListPage>),
            (session_usage_summary, (recent_limit: usize), Result<SessionUsageSummary>),
            (discover_browsable_sessions, (current_session_id: &str, query: Option<&str>, limit: usize, offset: usize), Result<SessionListPage>),
            (list_sessions_by_platform, (platform: &str), Result<Vec<SessionRecord>>),
            (list_sessions_by_workspace_root, (workspace_root: &str), Result<Vec<SessionRecord>>),
            (search_sessions, (query: &str, limit: usize), Result<Vec<SessionSearchResult>>),
            (search_sessions_by_platform, (query: &str, platform: &str, limit: usize), Result<Vec<SessionSearchResult>>),
            (associate_memory, (session_id: &str, memory_id: &str), Result<()>),
            (get_session_memories, (session_id: &str), Result<Vec<String>>),
            (disassociate_memory, (session_id: &str, memory_id: &str), Result<()>),
            (append_event, (event: &SessionEvent), Result<()>),
            (append_event_allocating_sequence, (event: &SessionEvent), Result<SessionEvent>),
            (append_session_domain_event_if_absent_allocating_sequence, (event: &SessionEvent, event_id: &str), Result<(SessionEvent, bool)>),
            (get_session_domain_event_by_id, (session_id: &str, event_id: &str), Result<Option<SessionEvent>>),
            (append_events_allocating_sequence, (events: &[SessionEvent]), Result<Vec<SessionEvent>>),
            (append_events_allocating_sequence_if_checkpoint_absent, (events: &[SessionEvent], checkpoint_id: &str), Result<Option<Vec<SessionEvent>>>),
            (append_context_envelope_event_if_absent, (event: &SessionEvent), Result<bool>),
            (append_context_envelope_event_if_absent_allocating_sequence, (event: &SessionEvent), Result<Option<SessionEvent>>),
            (get_events, (session_id: &str, from_seq: usize), Result<Vec<SessionEvent>>),
            (get_events_limited, (session_id: &str, from_seq: usize, limit: usize), Result<Vec<SessionEvent>>),
            (get_session_domain_timeline_limited, (session_id: &str, from_seq: usize, limit: usize), Result<Vec<SessionEvent>>),
            (count_session_domain_timeline_from, (session_id: &str, from_seq: usize), Result<usize>),
            (get_session_domain_events_by_kind_limited, (session_id: &str, kind: &str, from_seq: usize, limit: usize), Result<Vec<SessionEvent>>),
            (get_latest_session_domain_event_by_kind, (session_id: &str, kind: &str), Result<Option<SessionEvent>>),
            (count_session_domain_events_by_kind_from, (session_id: &str, kind: &str, from_seq: usize), Result<usize>),
            (has_session_domain_event_kind, (kind: &str), Result<bool>),
            (has_session_with_domain_event_kinds, (kinds: &[String]), Result<bool>),
            (get_events_by_type_limited, (session_id: &str, event_type: &str, from_seq: usize, limit: usize), Result<Vec<SessionEvent>>),
            (count_events_from, (session_id: &str, from_seq: usize), Result<usize>),
            (count_events_by_type_from, (session_id: &str, event_type: &str, from_seq: usize), Result<usize>),
            (get_context_event_by_envelope_id, (envelope_id: &str), Result<Option<SessionEvent>>),
            (next_event_sequence, (session_id: &str), Result<usize>),
            (delete_events_from, (session_id: &str, from_sequence: usize), Result<usize>),
            (delete_events_by_type_from, (session_id: &str, event_type: &str, from_sequence: usize), Result<usize>),
            (save_snapshot, (snapshot: &SessionSnapshot), Result<()>),
            (get_latest_snapshot, (session_id: &str), Result<Option<SessionSnapshot>>),
            (prune_before, (cutoff_iso8601: &str), Result<usize>),
            (insert_message, (msg: &SessionMessage), Result<()>),
            (commit_terminal_transcript_if_fenced, (request: &SessionTerminalTranscriptCommit), Result<SessionTerminalTranscriptReceipt>),
            (insert_messages_batch, (messages: &[SessionMessage]), Result<()>),
            (copy_session_messages_at_cutoff, (source_session_id: &str, target_session_id: &str, source_message_count: usize), Result<usize>),
            (branch_session_at_cutoff, (request: &SessionBranchRequest), Result<SessionBranchResult>),
            (get_session_branch_activation, (operation_id: &str), Result<Option<SessionBranchActivation>>),
            (list_recoverable_session_branch_activations, (limit: usize), Result<Vec<SessionBranchActivation>>),
            (transition_session_branch_activation, (transition: &SessionBranchActivationTransition), Result<SessionBranchActivation>),
            (append_message_with_runtime_outbox, (message: &SessionMessage, request: &SessionRuntimeOutboxRequest), Result<SessionRuntimeOutboxRecord>),
            (append_ingress_with_runtime_outbox, (session_id: &str, role: &str, content_json: Option<&str>, created_at_ms: u64, request: &SessionRuntimeOutboxRequest), Result<SessionRuntimeOutboxRecord>),
            (claim_session_runtime_outbox, (worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (mark_session_runtime_outbox_running, (request_id: &str, worker_id: &str, session_generation: u64, claim_token: &str, expected_revision: u64, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (ack_session_runtime_outbox, (request_id: &str, worker_id: &str, session_generation: u64, claim_token: &str, expected_revision: u64, terminal_status: SessionRuntimeInputStatus, runtime_commit_cursor: u64, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (renew_session_runtime_outbox_lease, (request_id: &str, worker_id: &str, session_generation: u64, claim_token: &str, expected_revision: u64, now_ms: u64, lease_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (fail_session_runtime_outbox, (request_id: &str, worker_id: &str, session_generation: u64, claim_token: &str, expected_revision: u64, failure_class: OutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (requeue_claimed_session_runtime_outbox, (request_id: &str, worker_id: &str, session_generation: u64, claim_token: &str, expected_revision: u64, decision: harness_contract::turn::InputRoutingDecision, target_turn_id: Option<&str>, classification_json: Option<&str>, reason: &str, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (retry_blocked_session_runtime_outbox, (request_id: &str, session_generation: u64, expected_revision: u64, actor: &str, reason: &str, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (cancel_session_runtime_outbox, (input_id: &str, session_generation: u64, expected_revision: u64, actor: &str, reason: &str, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (reclassify_session_runtime_outbox, (input_id: &str, session_generation: u64, expected_revision: u64, decision: harness_contract::turn::InputRoutingDecision, target_turn_id: Option<&str>, classification_json: Option<&str>, actor: &str, reason: &str, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (get_session_input_admission, (session_id: &str), Result<Option<SessionInputAdmission>>),
            (close_session_input_admission, (session_id: &str, expected_generation: u64, actor: &str, reason: &str, now_ms: u64), Result<SessionInputAdmission>),
            (advance_session_input_generation, (session_id: &str, expected_generation: u64, open: bool, actor: &str, reason: &str, now_ms: u64), Result<SessionInputAdmission>),
            (get_session_runtime_outbox, (request_id: &str), Result<Option<SessionRuntimeOutboxRecord>>),
            (get_session_runtime_outbox_by_input_id, (input_id: &str), Result<Option<SessionRuntimeOutboxRecord>>),
            (session_runtime_outbox_for_session, (session_id: &str, limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (session_runtime_outbox_for_sessions, (session_ids: &[String], per_session_limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (active_session_runtime_outbox, (limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (session_runtime_outbox_health, (), Result<SessionRuntimeOutboxHealth>),
            (blocked_session_runtime_outbox, (limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (claim_session_mission_outbox, (workspace_key: &str, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize), Result<Vec<SessionMissionOutboxRecord>>),
            (ack_session_mission_outbox, (request_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64), Result<SessionMissionOutboxRecord>),
            (fail_session_mission_outbox, (request_id: &str, worker_id: &str, expected_revision: u64, failure_class: OutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64), Result<SessionMissionOutboxRecord>),
            (get_session_mission_outbox, (request_id: &str), Result<Option<SessionMissionOutboxRecord>>),
            (get_messages, (session_id: &str, offset: usize, limit: usize), Result<Vec<SessionMessage>>),
            (get_messages_from_sequence, (session_id: &str, from_sequence: usize, limit: usize), Result<Vec<SessionMessage>>),
            (get_messages_in_ranges, (session_id: &str, ranges: &[(usize, usize)], limit: usize), Result<Vec<SessionMessage>>),
            (get_message_by_stable_id, (session_id: &str, stable_message_id: &str), Result<Option<SessionMessage>>),
            (get_message_by_sequence, (session_id: &str, sequence: usize), Result<Option<SessionMessage>>),
            (get_message_metadata_page, (session_id: &str, from_sequence: usize, limit: usize), Result<Vec<SessionMessageMetadata>>),
            (get_context_index_cards, (session_id: &str, limit: usize), Result<Vec<ContextIndexCard>>),
            (reconcile_session_context_index, (session_id: &str, card_span: usize, parent_span: usize, now_ms: u64), Result<ContextIndexCoverage>),
            (get_all_messages, (session_id: &str), Result<Vec<SessionMessage>>),
            (get_message_count, (session_id: &str), Result<usize>),
            (delete_messages_from, (session_id: &str, from_sequence: usize), Result<usize>),
            (search_messages, (query: &str, session_id: Option<&str>, limit: usize), Result<Vec<SessionMessage>>),
            (search_messages_in_sessions, (query: &str, session_ids: &[String], limit: usize), Result<Vec<SessionMessage>>),
            (search_messages_visible, (query: &str, owner_principal_id: Option<&str>, visible_session_ids: &[String], unrestricted: bool, limit: usize), Result<Vec<SessionMessage>>)
        }
    };
}

macro_rules! declare_session_store_backend {
    ($(($name:ident, ($($arg:ident: $arg_ty:ty),*), $result:ty)),+ $(,)?) => {
        $(fn $name(&self, $($arg: $arg_ty),*) -> $result;)+
    };
}

/// Complete durable session contract. A selected backend must implement every
/// business operation; no default `unsupported` method is permitted.
#[allow(clippy::too_many_arguments)]
pub trait SessionStoreBackend: std::fmt::Debug + Send + Sync {
    session_store_backend_contract!(declare_session_store_backend);
}

macro_rules! delegate_to_sqlite {
    ($(($name:ident, ($($arg:ident: $arg_ty:ty),*), $result:ty)),+ $(,)?) => {
        $(fn $name(&self, $($arg: $arg_ty),*) -> $result { self.$name($($arg),*) })+
    };
}

impl SessionStoreBackend for SqliteSessionStore {
    session_store_backend_contract!(delegate_to_sqlite);
}

pub type SharedSessionStoreBackend = std::sync::Arc<dyn SessionStoreBackend>;
