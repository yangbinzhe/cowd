//! Durable, transactional runtime lifecycle event store.
//!
//! A committed transaction is the only externally visible write unit. Graph,
//! node, goal, agent, team, and mission projections therefore observe one
//! monotonic commit cursor and never a partially appended multi-stream update.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex, OnceLock,
};
use std::time::{Duration, Instant};

use rusqlite::{
    params, params_from_iter, types::Value as SqliteValue, Connection, OptionalExtension,
    Transaction,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{SqliteConnectionLease, SqliteExecutor, StorageHandle};
use thiserror::Error;

const STORE_SCHEMA_VERSION: i64 = 10;
const SCOPE_REPLAY_PAGE_SIZE: usize = 1_024;
const EVENT_SCHEMA_VERSION: u32 = 1;
const MAX_TRANSACTION_EVENTS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;
const SESSION_TERMINAL_ARTIFACT_REF_PREFIX: &str = "terminal_artifact_v1:";
/// Latest Session terminal artifact payload schema emitted by Runtime.
///
/// Gateway consumers accept every positive version through this value so a
/// writer/reader rollout cannot drift through duplicated numeric literals.
pub const SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION: u64 = 3;
/// Projection lanes share SQLite's single writer with foreground lifecycle
/// commits.  They must yield quickly under a sustained write load, but an
/// immediate (0ms) failure turns ordinary writer hand-off into noisy failed
/// projection passes.  This short bounded wait preserves foreground priority
/// while allowing WAL's normal writer hand-off to settle before the reactor's
/// durable retry/backoff policy takes over.
const BACKGROUND_PROJECTION_BUSY_TIMEOUT_MS: u64 = 250;

thread_local! {
    static PROJECTION_WORK_CLASS: Cell<Option<RuntimeProjectionWorkClass>> =
        const { Cell::new(None) };
}

mod ports;
pub use ports::*;
mod domain;
pub use domain::{
    request_hash as runtime_event_request_hash,
    request_hash_with_terminal as runtime_event_request_hash_with_terminal,
    validate_decision_lease_claims as validate_runtime_decision_lease_claims,
    validate_event as validate_runtime_event,
    validate_fenced_terminal as validate_runtime_fenced_terminal,
    validate_transaction as validate_runtime_event_transaction,
};
mod sqlite;
#[cfg(test)]
use sqlite::{create_current_tables, table_has_column};
use sqlite::{validate_migration_snapshot, SqliteRuntimeEventStore};

/// The sole Runtime-facing durable event-store API. Runtime callers depend on
/// lifecycle semantics rather than a concrete database, path, pragma, or SQL
/// schema. Backend adapters are composed explicitly at the trusted host root.
#[derive(Debug)]
pub struct RuntimeEventStore {
    backend: Arc<dyn RuntimeEventStoreBackend>,
    commit_signal: tokio::sync::watch::Sender<u64>,
    /// Monotonic time of the latest non-projection commit. Maintenance lanes
    /// use this signal to yield during a foreground burst without reducing
    /// their idle catch-up throughput or starving indefinitely.
    last_foreground_commit_ms: AtomicU64,
    /// Per-stream serialization for the read-revision-then-append window.
    /// One stream (for example `session:<id>`) is written by many Runtime
    /// tasks (model events, early-tool authorizations, approval grants), so
    /// the optimistic-CAS append can race with itself. A per-stream
    /// in-process lock removes the TOCTOU window while keeping different
    /// streams fully parallel. Guards are acquired in sorted stream order so
    /// multi-stream transactions can never deadlock.
    stream_locks: StdMutex<std::collections::HashMap<String, Arc<StdMutex<()>>>>,
}

impl RuntimeEventStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::try_open(path).map_err(|error| error.to_string())
    }

    pub fn try_open(path: impl AsRef<Path>) -> RuntimeEventStoreResult<Self> {
        SqliteRuntimeEventStore::try_open(path).map(|store| Self::from_backend(Arc::new(store)))
    }

    pub fn open_in_memory() -> Result<Self, String> {
        Self::try_open_in_memory().map_err(|error| error.to_string())
    }

    pub fn try_open_in_memory() -> RuntimeEventStoreResult<Self> {
        SqliteRuntimeEventStore::try_open_in_memory()
            .map(|store| Self::from_backend(Arc::new(store)))
    }

    pub fn run_projection_work<T>(
        &self,
        class: RuntimeProjectionWorkClass,
        work: impl FnOnce() -> T,
    ) -> T {
        PROJECTION_WORK_CLASS.with(|slot| {
            let previous = slot.replace(Some(class));
            struct Restore<'a> {
                slot: &'a Cell<Option<RuntimeProjectionWorkClass>>,
                previous: Option<RuntimeProjectionWorkClass>,
            }
            impl Drop for Restore<'_> {
                fn drop(&mut self) {
                    self.slot.set(self.previous);
                }
            }
            let _restore = Restore { slot, previous };
            work()
        })
    }

    #[must_use]
    pub fn current_projection_work_class() -> Option<RuntimeProjectionWorkClass> {
        PROJECTION_WORK_CLASS.with(Cell::get)
    }

    #[must_use]
    pub fn from_backend(backend: Arc<dyn RuntimeEventStoreBackend>) -> Self {
        let latest_cursor = backend
            .all_events(1)
            .ok()
            .and_then(|events| events.first().map(|event| event.commit_cursor))
            .unwrap_or_default();
        let (commit_signal, _) = tokio::sync::watch::channel(latest_cursor);
        Self {
            backend,
            commit_signal,
            last_foreground_commit_ms: AtomicU64::new(0),
            stream_locks: StdMutex::new(std::collections::HashMap::new()),
        }
    }

    fn with_stream_locks<T>(&self, stream_ids: &[String], work: impl FnOnce() -> T) -> T {
        let unique = stream_ids
            .iter()
            .filter(|stream_id| !stream_id.is_empty())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut locks = self
            .stream_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        let arcs = unique
            .into_iter()
            .map(|stream_id| {
                Arc::clone(
                    locks
                        .entry(stream_id)
                        .or_insert_with(|| Arc::new(StdMutex::new(()))),
                )
            })
            .collect::<Vec<_>>();
        drop(locks);
        let _guards = arcs
            .iter()
            .map(|arc| {
                arc.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            })
            .collect::<Vec<_>>();
        work()
    }

    /// Runs `work` while holding the per-stream in-process write lock. The
    /// lock covers the whole read-revision-then-append window, so callers that
    /// first read `stream_revision` and then append can never be interrupted
    /// by another in-process writer on the same stream.
    pub fn with_stream_lock<T>(&self, stream_id: &str, work: impl FnOnce() -> T) -> T {
        self.with_stream_locks(&[stream_id.to_string()], work)
    }

    pub fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        let stream_id = input.stream_id.clone();
        let event = self.with_stream_lock(&stream_id, || self.backend.append(input))?;
        self.publish_commit(event.commit_cursor);
        Ok(event)
    }

    pub fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        let receipt =
            self.with_stream_locks(&stream_ids, || self.backend.append_transaction(request))?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    /// Appends without acquiring the per-stream lock. Only callers that
    /// already hold [`Self::with_stream_lock`] for every expected stream may
    /// use this; it avoids a non-reentrant deadlock on the same stream.
    pub(crate) fn append_transaction_locked(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let receipt = self.backend.append_transaction(request)?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    pub fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        let receipt = self.with_stream_locks(&stream_ids, || {
            self.backend
                .append_transaction_with_terminal(request, terminal)
        })?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn consume_verified_decision_lease(
        &self,
        lease_id: &str,
        principal_id: &str,
        review_id: &str,
        action: &str,
        scope: &str,
        evidence_digest: &str,
        credential_epoch: u64,
        consumed_at_ms: u64,
    ) -> RuntimeEventStoreResult<()> {
        self.backend.consume_verified_decision_lease(
            lease_id,
            principal_id,
            review_id,
            action,
            scope,
            evidence_digest,
            credential_epoch,
            consumed_at_ms,
        )
    }

    pub(crate) fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        let receipt = self.with_stream_locks(&stream_ids, || {
            self.backend
                .append_transaction_with_verified_decision_lease(request, lease)
        })?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    pub fn append_batch_if_revision(
        &self,
        stream_id: impl Into<String>,
        expected_revision: u64,
        transaction_id: impl Into<String>,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let stream_id = stream_id.into();
        let lock_stream_id = stream_id.clone();
        let receipt = self.with_stream_lock(&lock_stream_id, || {
            self.backend.append_batch_if_revision(
                stream_id,
                expected_revision,
                transaction_id.into(),
                events,
            )
        })?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }

    #[must_use]
    pub fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.commit_signal.subscribe()
    }

    fn publish_commit(&self, cursor: u64) {
        if Self::current_projection_work_class().is_none() {
            self.last_foreground_commit_ms
                .store(monotonic_elapsed_ms(), Ordering::Release);
        }
        if cursor > *self.commit_signal.borrow() {
            self.commit_signal.send_replace(cursor);
        }
    }

    pub(crate) fn foreground_quiet_remaining(&self, quiet_period: Duration) -> Duration {
        let last = self.last_foreground_commit_ms.load(Ordering::Acquire);
        if last == 0 {
            return Duration::ZERO;
        }
        let quiet_ms = u64::try_from(quiet_period.as_millis()).unwrap_or(u64::MAX);
        let elapsed_ms = monotonic_elapsed_ms().saturating_sub(last);
        Duration::from_millis(quiet_ms.saturating_sub(elapsed_ms))
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        self.backend.events_after_cursor(cursor, max_commits)
    }

    pub fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        self.backend
            .projection_scan_page(cursor, interest, max_commits, max_events, max_bytes)
    }

    #[must_use]
    pub fn background_projection_capacity_hint(&self) -> usize {
        self.backend.background_projection_capacity_hint().max(1)
    }

    pub fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        self.backend.projection_checkpoint(projection_id)
    }

    pub fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        self.backend.projection_checkpoints_with_prefix(prefix)
    }

    pub fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.backend
            .put_projection_checkpoint(projection_id, source_cursor, payload, updated_at_ms)
    }

    pub fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.backend.compare_and_put_projection_checkpoint(
            projection_id,
            source_cursor,
            expected_revision,
            payload,
            updated_at_ms,
        )
    }

    pub fn delete_projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        self.backend.delete_projection_checkpoint(projection_id)
    }

    #[must_use]
    pub fn current_commit_cursor(&self) -> u64 {
        *self.commit_signal.borrow()
    }

    pub fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        self.backend
            .event_by_idempotency_key(stream_id, idempotency_key)
    }

    pub fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        self.backend.stream_revision(stream_id)
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        tracing::trace!(stream_id, "reading complete Runtime event stream");
        self.backend.list_stream(stream_id)
    }

    pub fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.list_stream_page_desc(stream_id, limit, offset)
    }

    pub fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        self.backend.stream_event_count(stream_id)
    }

    pub fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .execution_events_for_session(session_id, after_position, limit)
    }

    pub fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .events_for_root_execution(root_execution_id, after_position, limit)
    }

    pub fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .events_for_root_execution_kind(root_execution_id, kind, after_position, limit)
    }

    pub fn events_for_activity(
        &self,
        activity_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .events_for_activity(activity_id, after_position, limit)
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.list_scope(scope, limit)
    }

    pub fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .list_scope_page_asc(scope, after_position, limit)
    }

    pub fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .list_scope_stream_prefix_page_asc(scope, stream_prefix, after_position, limit)
    }

    pub fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend
            .list_scope_kind_page_asc(scope, kind, after_position, limit)
    }

    /// Replay a complete scope in durable commit order without a hidden
    /// cardinality ceiling. Projectors use this API; bounded UI views keep
    /// using [`Self::list_scope`].
    pub fn replay_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut events = Vec::new();
        let mut after_position = None;
        loop {
            let page = self.list_scope_page_asc(scope, after_position, SCOPE_REPLAY_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after_position = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let complete = page.len() < SCOPE_REPLAY_PAGE_SIZE;
            events.extend(page);
            if complete {
                break;
            }
        }
        Ok(events)
    }

    /// Replay one aggregate family without materialising unrelated events in
    /// the same domain. Prefixes are exact text prefixes, not SQL patterns.
    pub fn replay_scope_stream_prefix(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if stream_prefix.is_empty() {
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
        let mut after_position = None;
        loop {
            let page = self.list_scope_stream_prefix_page_asc(
                scope,
                stream_prefix,
                after_position,
                SCOPE_REPLAY_PAGE_SIZE,
            )?;
            if page.is_empty() {
                break;
            }
            after_position = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let complete = page.len() < SCOPE_REPLAY_PAGE_SIZE;
            events.extend(page);
            if complete {
                break;
            }
        }
        Ok(events)
    }

    /// Replay one event kind in durable commit order using the backend's
    /// `(scope, kind, commit_cursor)` index.
    pub fn replay_scope_kind(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut events = Vec::new();
        let mut after_position = None;
        loop {
            let page =
                self.list_scope_kind_page_asc(scope, kind, after_position, SCOPE_REPLAY_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after_position = page
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            let complete = page.len() < SCOPE_REPLAY_PAGE_SIZE;
            events.extend(page);
            if complete {
                break;
            }
        }
        Ok(events)
    }

    pub fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        self.backend.stream_ids_for_scope(scope)
    }

    pub fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        self.backend
            .stream_ids_for_scope_kind_at_sequence(scope, kind, sequence)
    }

    pub fn stream_ids_for_scope_kind_at_sequence_page(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
        after: Option<(u64, String)>,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<(String, u64)>> {
        self.backend
            .stream_ids_for_scope_kind_at_sequence_page(scope, kind, sequence, after, limit)
    }

    pub fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        self.backend
            .latest_stream_statuses_for_scope_kind_at_sequence(scope, kind, sequence)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.backend.all_events(limit)
    }

    pub fn latest_for_stream(
        &self,
        stream_id: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        self.backend.latest_for_stream(stream_id)
    }

    pub fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        self.backend.latest_for_stream_kind(stream_id, kind)
    }

    pub fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        #[cfg(any(test, feature = "test-fixtures"))]
        {
            self.backend.enqueue_session_terminal(
                terminal_id,
                message_id,
                session_id,
                commit_cursor,
                payload_ref,
            )
        }
        #[cfg(not(any(test, feature = "test-fixtures")))]
        {
            let _ = (
                terminal_id,
                message_id,
                session_id,
                commit_cursor,
                payload_ref,
            );
            Err(RuntimeEventStoreError::InvalidTransaction(
                "unfenced terminal enqueue is test-only; use append_transaction_with_terminal"
                    .to_string(),
            ))
        }
    }

    pub fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        self.backend
            .claim_session_terminals(worker_id, now_ms, lease_ms, limit)
    }

    pub fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        self.backend.session_terminal(terminal_id)
    }

    pub fn has_unsettled_session_terminals(
        &self,
        session_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        self.backend.has_unsettled_session_terminals(session_id)
    }

    pub fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        self.backend
            .materialized_session_terminals_after(session_id, after_commit_cursor, limit)
    }

    pub fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        self.backend.session_terminal_health()
    }

    pub fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        self.backend.blocked_session_terminals(limit)
    }

    pub fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend
            .retry_session_terminal(terminal_id, actor, reason, now_ms)
    }

    pub fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend.adopt_session_terminal_fence(request)
    }

    pub fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend
            .ack_session_terminal(terminal_id, worker_id, expected_revision, now_ms)
    }

    /// Settle a claimed terminal that lost the durable execution fence to a
    /// different terminal outcome (most commonly user cancellation). Unlike
    /// `materialized`, this state never asserts that an assistant transcript
    /// was written.
    pub fn suppress_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend.suppress_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            reason,
            now_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.backend.fail_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            class,
            error,
            retry_at_ms,
            max_attempts,
            now_ms,
        )
    }

    /// Export a canonical, read-only migration payload from a quiesced source.
    pub fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
        self.backend.export_migration_snapshot()
    }

    /// Import a migration payload into an empty, already verified target.
    /// Normal Runtime execution must never call this API.
    pub fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        self.backend.import_migration_snapshot(snapshot)?;
        if let Some(commit) = snapshot.commits.last() {
            self.publish_commit(commit.commit_cursor);
        }
        Ok(())
    }
}

fn monotonic_elapsed_ms() -> u64 {
    static PROCESS_CLOCK: OnceLock<Instant> = OnceLock::new();
    u64::try_from(
        PROCESS_CLOCK
            .get_or_init(Instant::now)
            .elapsed()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(1)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
