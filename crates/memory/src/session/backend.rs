//! Complete durable session backend contract.
//!
//! Application code keeps using [`crate::session_store::UnifiedSessionStore`].
//! This port is intentionally synchronous because the existing SQLite and
//! PostgreSQL executors own bounded connection pools; the public facade keeps
//! its async API without adding a process-wide mutex around database work.

use crate::store::session::{
    OutboxFailureClass, SessionEvent, SessionListOptions, SessionListPage, SessionMessage,
    SessionMissionOutboxRecord, SessionMissionOutboxRequest, SessionRecord,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SqliteSessionStore,
};
use crate::store::Result;

macro_rules! session_store_backend_contract {
    ($macro:ident) => {
        $macro! {
            (create_session, (session: &SessionRecord), Result<()>),
            (get_session, (session_id: &str), Result<Option<SessionRecord>>),
            (update_session, (session: &SessionRecord), Result<()>),
            (upsert_session, (session: &SessionRecord), Result<()>),
            (upsert_session_with_mission_outbox, (session: &SessionRecord, request: &SessionMissionOutboxRequest), Result<SessionMissionOutboxRecord>),
            (delete_session, (session_id: &str), Result<()>),
            (delete_session_with_mission_outbox, (request: &SessionMissionOutboxRequest), Result<bool>),
            (mark_session_closed, (session_id: &str), Result<()>),
            (list_sessions, (), Result<Vec<SessionRecord>>),
            (list_sessions_page, (opts: &SessionListOptions<'_>), Result<SessionListPage>),
            (list_sessions_by_platform, (platform: &str), Result<Vec<SessionRecord>>),
            (list_sessions_by_workspace_root, (workspace_root: &str), Result<Vec<SessionRecord>>),
            (search_sessions, (query: &str, limit: usize), Result<Vec<SessionSearchResult>>),
            (search_sessions_by_platform, (query: &str, platform: &str, limit: usize), Result<Vec<SessionSearchResult>>),
            (associate_memory, (session_id: &str, memory_id: &str), Result<()>),
            (get_session_memories, (session_id: &str), Result<Vec<String>>),
            (disassociate_memory, (session_id: &str, memory_id: &str), Result<()>),
            (append_event, (event: &SessionEvent), Result<()>),
            (append_event_allocating_sequence, (event: &SessionEvent), Result<SessionEvent>),
            (append_events_allocating_sequence, (events: &[SessionEvent]), Result<Vec<SessionEvent>>),
            (append_events_allocating_sequence_if_checkpoint_absent, (events: &[SessionEvent], checkpoint_id: &str), Result<Option<Vec<SessionEvent>>>),
            (append_context_envelope_event_if_absent, (event: &SessionEvent), Result<bool>),
            (append_context_envelope_event_if_absent_allocating_sequence, (event: &SessionEvent), Result<Option<SessionEvent>>),
            (get_events, (session_id: &str, from_seq: usize), Result<Vec<SessionEvent>>),
            (get_events_limited, (session_id: &str, from_seq: usize, limit: usize), Result<Vec<SessionEvent>>),
            (get_session_domain_timeline_limited, (session_id: &str, from_seq: usize, limit: usize), Result<Vec<SessionEvent>>),
            (count_session_domain_timeline_from, (session_id: &str, from_seq: usize), Result<usize>),
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
            (append_terminal_message_idempotent, (message_id: &str, session_id: &str, content_json: &str, token_usage_json: Option<&str>, created_at_ms: u64), Result<(SessionMessage, bool)>),
            (append_terminal_transcript_idempotent, (terminal_message_id: &str, ingress_message_id: &str, session_id: &str, messages: &[SessionMessage], created_at_ms: u64), Result<(Vec<SessionMessage>, bool)>),
            (insert_messages_batch, (messages: &[SessionMessage]), Result<()>),
            (append_message_with_runtime_outbox, (message: &SessionMessage, request: &SessionRuntimeOutboxRequest), Result<SessionRuntimeOutboxRecord>),
            (append_ingress_with_runtime_outbox, (session_id: &str, role: &str, content_json: Option<&str>, created_at_ms: u64, request: &SessionRuntimeOutboxRequest), Result<SessionRuntimeOutboxRecord>),
            (claim_session_runtime_outbox, (worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (ack_session_runtime_outbox, (request_id: &str, worker_id: &str, expected_revision: u64, runtime_commit_cursor: u64, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (renew_session_runtime_outbox_lease, (request_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64, lease_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (fail_session_runtime_outbox, (request_id: &str, worker_id: &str, expected_revision: u64, failure_class: OutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (retry_blocked_session_runtime_outbox, (request_id: &str, expected_revision: u64, actor: &str, reason: &str, now_ms: u64), Result<SessionRuntimeOutboxRecord>),
            (get_session_runtime_outbox, (request_id: &str), Result<Option<SessionRuntimeOutboxRecord>>),
            (session_runtime_outbox_for_session, (session_id: &str, limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (active_session_runtime_outbox, (limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (session_runtime_outbox_health, (), Result<SessionRuntimeOutboxHealth>),
            (blocked_session_runtime_outbox, (limit: usize), Result<Vec<SessionRuntimeOutboxRecord>>),
            (claim_session_mission_outbox, (workspace_key: &str, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize), Result<Vec<SessionMissionOutboxRecord>>),
            (ack_session_mission_outbox, (request_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64), Result<SessionMissionOutboxRecord>),
            (fail_session_mission_outbox, (request_id: &str, worker_id: &str, expected_revision: u64, failure_class: OutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64), Result<SessionMissionOutboxRecord>),
            (get_session_mission_outbox, (request_id: &str), Result<Option<SessionMissionOutboxRecord>>),
            (get_messages, (session_id: &str, offset: usize, limit: usize), Result<Vec<SessionMessage>>),
            (get_messages_from_sequence, (session_id: &str, from_sequence: usize, limit: usize), Result<Vec<SessionMessage>>),
            (get_all_messages, (session_id: &str), Result<Vec<SessionMessage>>),
            (get_message_count, (session_id: &str), Result<usize>),
            (delete_messages_from, (session_id: &str, from_sequence: usize), Result<usize>),
            (search_messages, (query: &str, session_id: Option<&str>, limit: usize), Result<Vec<SessionMessage>>),
            (search_messages_in_sessions, (query: &str, session_ids: &[String], limit: usize), Result<Vec<SessionMessage>>)
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
