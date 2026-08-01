//! Unified session store — the canonical wrapper for session persistence.
//!
//! `UnifiedSessionStore` delegates to a complete selected durable backend,
//! while retaining explicit SQLite construction helpers for SQLite topology.
//!
//! # Migration from SqliteSessionStore
//!
//! Replace:
//!
//! ```rust,no_run
//! use session::SqliteSessionStore;
//! use std::path::Path;
//! let store = SqliteSessionStore::open(Path::new("sessions.db")).unwrap();
//! ```

//! with:

//! ```rust,no_run
//! use session::UnifiedSessionStore;
//! use std::path::Path;
//! let store = UnifiedSessionStore::open(Path::new("sessions.db")).unwrap();
//! ```

use std::path::Path;
use std::sync::Arc;

use crate::domain::{
    SessionBranchActivation, SessionBranchActivationTransition, SessionDomainEvent,
    SessionDomainEventPage, SessionLifecycleIntent, SessionLifecyclePlan,
    SessionLifecycleTransition, SESSION_DOMAIN_EVENT_TYPE,
};
use crate::error::{Result, SessionError};
use crate::persistence::backend::{SessionStoreBackend, SharedSessionStoreBackend};
use crate::persistence::execution_plane::{
    StorageExecutionLane, StorageExecutionPlane, StorageExecutionPlaneConfig,
    StorageExecutionPlaneStats,
};
use crate::persistence::history::SessionContextPage;
use crate::persistence::sqlite::{
    ContextIndexCard, ContextIndexCoverage, OutboxFailureClass, SessionEvent,
    SessionInputAdmission, SessionLifecycleFenceRequest, SessionLifecycleTombstoneRequest,
    SessionListOptions, SessionListPage, SessionMessage, SessionMessageMetadata,
    SessionMissionOutboxRecord, SessionMissionOutboxRequest, SessionRecord,
    SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SessionTerminalTranscriptCommit,
    SessionTerminalTranscriptReceipt, SessionUsageSummary, SqliteSessionStore,
};

// ---------------------------------------------------------------------------
// UnifiedSessionStore
// ---------------------------------------------------------------------------

/// Canonical session store for the cowd AI assistant.
///
/// Holds a complete selected session backend through `Arc`. Each concrete
/// backend owns its bounded connection pool; no process-wide async mutex is
/// introduced, so unrelated sessions remain independently schedulable.
///
/// # Example
///
/// ```rust,no_run
/// use session::UnifiedSessionStore;
/// use std::path::Path;
///
/// let store = UnifiedSessionStore::open(Path::new("sessions.db")).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct UnifiedSessionStore {
    inner: SharedSessionStoreBackend,
    execution: Arc<StorageExecutionPlane>,
}

impl UnifiedSessionStore {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Open (or create) a session database at `path`.
    ///
    /// Creates any missing parent directories and initialises the schema if
    /// the database is new.
    pub fn open(path: &Path) -> Result<Self> {
        let store = SqliteSessionStore::open(path)?;
        Ok(Self {
            inner: Arc::new(store),
            execution: Arc::new(StorageExecutionPlane::sqlite_default_plane()),
        })
    }

    /// Open a SQLite session database from an explicitly SQLite storage
    /// handle. PostgreSQL composition uses [`Self::from_backend`] instead.
    pub fn open_sqlite_storage_handle(handle: &storage::StorageHandle) -> Result<Self> {
        Self::open_sqlite_storage_handle_with_execution_config(
            handle,
            StorageExecutionPlaneConfig::default(),
        )
    }

    /// Open a SQLite session database with an explicit bounded execution
    /// plane. Process composition roots use this constructor so SQLite and
    /// PostgreSQL obey the same concurrency and overload policy.
    pub fn open_sqlite_storage_handle_with_execution_config(
        handle: &storage::StorageHandle,
        config: StorageExecutionPlaneConfig,
    ) -> Result<Self> {
        if handle.backend != storage::StorageBackendKind::Sqlite {
            return Err(SessionError::Store(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        let store = SqliteSessionStore::open_storage_handle(handle)?;
        Ok(Self {
            inner: Arc::new(store),
            execution: Arc::new(StorageExecutionPlane::new_sqlite(config)?),
        })
    }

    /// Open an in-memory session database (useful for testing).
    pub fn open_in_memory() -> Result<Self> {
        let store = SqliteSessionStore::open_in_memory()?;
        Ok(Self {
            inner: Arc::new(store),
            execution: Arc::new(StorageExecutionPlane::sqlite_default_plane()),
        })
    }

    /// Build the application-facing store from the selected durable backend.
    /// Composition roots use this to inject PostgreSQL without exposing a
    /// driver, URL, path, or adapter type to Gateway/Runtime callers.
    #[must_use]
    pub fn from_backend(inner: SharedSessionStoreBackend) -> Self {
        Self {
            inner,
            execution: Arc::new(StorageExecutionPlane::default_plane()),
        }
    }

    pub fn from_backend_with_execution_config(
        inner: SharedSessionStoreBackend,
        config: StorageExecutionPlaneConfig,
    ) -> Result<Self> {
        Ok(Self {
            inner,
            execution: Arc::new(StorageExecutionPlane::new(config)?),
        })
    }

    #[must_use]
    pub fn execution_stats(&self) -> StorageExecutionPlaneStats {
        self.execution.stats()
    }

    /// Derive the read-only capability consumed by memory reconstruction.
    #[must_use]
    pub fn history_reader(&self) -> crate::persistence::SessionHistoryReader {
        crate::persistence::SessionHistoryReader::new(self.clone())
    }

    /// Stop accepting repository operations, drain every admitted lane, and
    /// wait for all blocking backend work to finish. All clones share this
    /// lifecycle boundary.
    pub async fn shutdown_and_drain(
        &self,
        timeout: std::time::Duration,
    ) -> Result<StorageExecutionPlaneStats> {
        self.execution.shutdown_and_drain(timeout).await
    }

    async fn execute_on<T, F>(
        &self,
        lane: StorageExecutionLane,
        write: bool,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&dyn SessionStoreBackend) -> Result<T> + Send + 'static,
    {
        let backend = Arc::clone(&self.inner);
        self.execution
            .execute(lane, write, move || operation(backend.as_ref()))
            .await
    }

    async fn execute_read<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&dyn SessionStoreBackend) -> Result<T> + Send + 'static,
    {
        self.execute_on(StorageExecutionLane::InteractiveRead, false, operation)
            .await
    }

    async fn execute_write<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&dyn SessionStoreBackend) -> Result<T> + Send + 'static,
    {
        self.execute_on(StorageExecutionLane::InteractiveWrite, true, operation)
            .await
    }

    async fn execute_background<T, F>(&self, write: bool, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&dyn SessionStoreBackend) -> Result<T> + Send + 'static,
    {
        self.execute_on(StorageExecutionLane::Background, write, operation)
            .await
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// Insert a new session record.
    ///
    /// Uses `INSERT OR IGNORE` so calling this for an already-existing session
    /// is a harmless no-op.
    pub async fn create_session(&self, session: &SessionRecord) -> Result<()> {
        let session = session.clone();
        self.execute_write(move |backend| backend.create_session(&session))
            .await
    }

    /// Retrieve a session record by its ID, or `None` if not found.
    pub async fn get_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_session(&session_id))
            .await
    }

    /// Read one body-free durable recovery manifest.
    pub async fn get_session_recovery_manifest(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecoveryManifest>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_session_recovery_manifest(&session_id))
            .await
    }

    /// Rebuild a missing/corrupt activation manifest from authoritative
    /// Session rows without hydrating message bodies into Runtime memory.
    pub async fn rebuild_session_recovery_manifest(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> Result<Option<SessionRecoveryManifest>> {
        let session_id = session_id.to_string();
        self.execute_background(true, move |backend| {
            backend.rebuild_session_recovery_manifest(&session_id, now_ms)
        })
        .await
    }

    /// Page active recovery manifests without loading transcript rows.
    pub async fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionRecoveryManifest>> {
        self.execute_background(false, move |backend| {
            backend.list_active_session_recovery_manifests(offset, limit)
        })
        .await
    }

    /// Persist one externally-owned recovery signal.
    pub async fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> Result<SessionRecoveryManifest> {
        let session_id = session_id.to_string();
        self.execute_write(move |backend| {
            backend.set_session_recovery_signal(&session_id, signal, active, observed_at_ms)
        })
        .await
    }

    /// Overwrite all mutable fields of an existing session record.
    ///
    /// `session_id` is used as the lookup key; the row is silently unchanged
    /// if it does not exist.
    pub async fn update_session(&self, session: &SessionRecord) -> Result<()> {
        let session = session.clone();
        self.execute_write(move |backend| backend.update_session(&session))
            .await
    }

    /// Upsert a session record (insert or replace all fields).
    ///
    /// Equivalent to calling [`create_session`] then [`update_session`].  Use
    /// this when you don't know whether the row already exists.
    pub async fn upsert_session(&self, session: &SessionRecord) -> Result<()> {
        let session = session.clone();
        self.execute_write(move |backend| backend.upsert_session(&session))
            .await
    }

    /// Atomically persist a Session record and its durable Mission lifecycle
    /// intent. The Gateway bridge later materializes this into RuntimeEventStore.
    pub async fn upsert_session_with_mission_outbox(
        &self,
        session: &SessionRecord,
        request: &SessionMissionOutboxRequest,
    ) -> Result<SessionMissionOutboxRecord> {
        let session = session.clone();
        let request = request.clone();
        self.execute_write(move |backend| {
            backend.upsert_session_with_mission_outbox(&session, &request)
        })
        .await
    }

    pub async fn plan_session_lifecycle(
        &self,
        plan: &SessionLifecyclePlan,
    ) -> Result<SessionLifecycleIntent> {
        let plan = plan.clone();
        self.execute_write(move |backend| backend.plan_session_lifecycle(&plan))
            .await
    }

    pub async fn get_session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionLifecycleIntent>> {
        let operation_id = operation_id.to_string();
        self.execute_read(move |backend| backend.get_session_lifecycle_intent(&operation_id))
            .await
    }

    pub async fn list_recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionLifecycleIntent>> {
        self.execute_background(false, move |backend| {
            backend.list_recoverable_session_lifecycle_intents(limit)
        })
        .await
    }

    pub async fn fence_session_lifecycle(
        &self,
        request: &SessionLifecycleFenceRequest,
    ) -> Result<SessionLifecycleIntent> {
        let request = request.clone();
        self.execute_write(move |backend| backend.fence_session_lifecycle(&request))
            .await
    }

    pub async fn transition_session_lifecycle(
        &self,
        transition: &SessionLifecycleTransition,
    ) -> Result<SessionLifecycleIntent> {
        let transition = transition.clone();
        self.execute_write(move |backend| backend.transition_session_lifecycle(&transition))
            .await
    }

    pub async fn commit_session_lifecycle_tombstone(
        &self,
        request: &SessionLifecycleTombstoneRequest,
    ) -> Result<SessionLifecycleIntent> {
        let request = request.clone();
        self.execute_write(move |backend| backend.commit_session_lifecycle_tombstone(&request))
            .await
    }

    /// Permanently remove a session and all its memory associations.
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        let session_id = session_id.to_string();
        self.execute_write(move |backend| backend.delete_session(&session_id))
            .await
    }

    /// Atomically delete a Session and queue the matching Mission close intent.
    pub async fn delete_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> Result<bool> {
        let request = request.clone();
        self.execute_write(move |backend| backend.delete_session_with_mission_outbox(&request))
            .await
    }

    /// Mark a session as closed.
    ///
    /// Updates the session's status to `'closed'` and refreshes
    /// `last_activity`.  Messages are preserved for auditing.
    pub async fn mark_session_closed(&self, session_id: &str) -> Result<()> {
        let session_id = session_id.to_string();
        self.execute_write(move |backend| backend.mark_session_closed(&session_id))
            .await
    }

    /// List all session records ordered by `last_activity DESC`.
    pub async fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        self.execute_read(|backend| backend.list_sessions()).await
    }

    /// List a filtered, sorted page of sessions directly in the backing store.
    pub async fn list_sessions_page(
        &self,
        opts: &SessionListOptions<'_>,
    ) -> Result<SessionListPage> {
        let query = opts.query.map(str::to_string);
        let model = opts.model.map(str::to_string);
        let status = opts.status.map(str::to_string);
        let owner_principal_id = opts.owner_principal_id.map(str::to_string);
        let visible_session_ids = opts.visible_session_ids.to_vec();
        let unrestricted = opts.unrestricted;
        let include_deleted = opts.include_deleted;
        let sort = opts.sort.to_string();
        let order = opts.order.to_string();
        let limit = opts.limit;
        let offset = opts.offset;
        self.execute_read(move |backend| {
            backend.list_sessions_page(&SessionListOptions {
                query: query.as_deref(),
                model: model.as_deref(),
                status: status.as_deref(),
                owner_principal_id: owner_principal_id.as_deref(),
                visible_session_ids: &visible_session_ids,
                unrestricted,
                include_deleted,
                sort: &sort,
                order: &order,
                limit,
                offset,
            })
        })
        .await
    }

    /// Discover sessions that share the current Session's durable workspace
    /// and actor identity. Message content participates in the query, but only
    /// records inside that authority boundary can affect ranking or results.
    pub async fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<SessionListPage> {
        let current_session_id = current_session_id.to_string();
        let query = query.map(str::to_string);
        self.execute_read(move |backend| {
            backend.discover_browsable_sessions(
                &current_session_id,
                query.as_deref(),
                limit,
                offset,
            )
        })
        .await
    }

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub async fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        let platform = platform.to_string();
        self.execute_read(move |backend| backend.list_sessions_by_platform(&platform))
            .await
    }

    /// List sessions bound to one workspace root, ordered by `last_activity DESC`.
    ///
    /// The workspace root is stored in `metadata_json.workspace_root`. This is
    /// the canonical DB-backed replacement for the deprecated runtime
    /// filesystem `SessionStore` namespace.
    pub async fn list_sessions_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<SessionRecord>> {
        let workspace_root = workspace_root.to_string();
        self.execute_read(move |backend| backend.list_sessions_by_workspace_root(&workspace_root))
            .await
    }

    /// Search sessions using the selected backend's full-text capability.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub async fn search_sessions(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        let query = query.to_string();
        self.execute_read(move |backend| backend.search_sessions(&query, limit))
            .await
    }

    /// Search sessions with platform filter.
    pub async fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        let query = query.to_string();
        let platform = platform.to_string();
        self.execute_read(move |backend| {
            backend.search_sessions_by_platform(&query, &platform, limit)
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub async fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let session_id = session_id.to_string();
        let memory_id = memory_id.to_string();
        self.execute_write(move |backend| backend.associate_memory(&session_id, &memory_id))
            .await
    }

    /// Return all memory IDs associated with `session_id`.
    pub async fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_session_memories(&session_id))
            .await
    }

    /// Remove the association between a session and a memory.
    pub async fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let session_id = session_id.to_string();
        let memory_id = memory_id.to_string();
        self.execute_write(move |backend| backend.disassociate_memory(&session_id, &memory_id))
            .await
    }

    // -----------------------------------------------------------------------
    // Event log
    // -----------------------------------------------------------------------

    /// Append a mutation event to the session's event log.
    pub async fn append_event(&self, event: &SessionEvent) -> Result<()> {
        let event = event.clone();
        self.execute_write(move |backend| backend.append_event(&event))
            .await
    }

    /// Atomically allocate the next session-local sequence and append an event.
    pub async fn append_event_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> Result<SessionEvent> {
        let event = event.clone();
        self.execute_write(move |backend| backend.append_event_allocating_sequence(&event))
            .await
    }

    /// Atomically allocate contiguous sequences and append a same-session batch.
    pub async fn append_events_allocating_sequence(
        &self,
        events: &[SessionEvent],
    ) -> Result<Vec<SessionEvent>> {
        let events = events.to_vec();
        self.execute_write(move |backend| backend.append_events_allocating_sequence(&events))
            .await
    }

    /// Append a context envelope event unless the same envelope id already exists.
    pub async fn append_context_envelope_event_if_absent(
        &self,
        event: &SessionEvent,
    ) -> Result<bool> {
        let event = event.clone();
        self.execute_write(move |backend| backend.append_context_envelope_event_if_absent(&event))
            .await
    }

    /// Atomically de-duplicate a context envelope and allocate its sequence.
    pub async fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> Result<Option<SessionEvent>> {
        let event = event.clone();
        self.execute_write(move |backend| {
            backend.append_context_envelope_event_if_absent_allocating_sequence(&event)
        })
        .await
    }

    /// Append a canonical session-domain event to the session event log.
    pub async fn append_session_domain_event(&self, event: &SessionDomainEvent) -> Result<()> {
        self.append_session_domain_event_allocating_sequence(event)
            .await
            .map(|_| ())
    }

    /// Persist a domain event using the store-owned sequence allocator.
    pub async fn append_session_domain_event_allocating_sequence(
        &self,
        event: &SessionDomainEvent,
    ) -> Result<SessionEvent> {
        let event = event.to_session_event()?;
        self.append_event_allocating_sequence(&event).await
    }

    /// Atomically append a canonical domain event once per
    /// `session_id + event_id`. The receipt returns the durable row and whether
    /// it was replayed from an earlier successful commit.
    pub async fn append_session_domain_event_if_absent_allocating_sequence(
        &self,
        event: &SessionDomainEvent,
    ) -> Result<(SessionEvent, bool)> {
        let event_id = event.event_id.clone();
        let event = event.to_session_event()?;
        self.execute_write(move |backend| {
            backend.append_session_domain_event_if_absent_allocating_sequence(&event, &event_id)
        })
        .await
    }

    pub async fn get_session_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> Result<Option<SessionDomainEvent>> {
        let session_id = session_id.to_string();
        let event_id = event_id.to_string();
        self.execute_read(move |backend| {
            backend.get_session_domain_event_by_id(&session_id, &event_id)
        })
        .await?
        .map(|event| SessionDomainEvent::from_session_event(&event))
        .transpose()
        .map_err(Into::into)
    }

    /// Persist related session-domain records as one ordered transaction. This
    /// is used for compaction so its continuation checkpoint and the context
    /// boundary cannot become independently visible.
    pub async fn append_session_domain_events_allocating_sequence(
        &self,
        events: &[SessionDomainEvent],
    ) -> Result<Vec<SessionEvent>> {
        let mut wire_events = Vec::with_capacity(events.len());
        for event in events {
            wire_events.push(event.to_session_event()?);
        }
        self.append_events_allocating_sequence(&wire_events).await
    }

    /// Atomically commit a semantic checkpoint bundle exactly once per
    /// `session_id + checkpoint_id`. `Ok(false)` means an earlier attempt
    /// already committed the same durable checkpoint.
    pub async fn append_session_domain_events_if_checkpoint_absent(
        &self,
        events: &[SessionDomainEvent],
        checkpoint_id: &str,
    ) -> Result<bool> {
        let mut wire_events = Vec::with_capacity(events.len());
        for event in events {
            wire_events.push(event.to_session_event()?);
        }
        let checkpoint_id = checkpoint_id.to_string();
        self.execute_write(move |backend| {
            backend.append_events_allocating_sequence_if_checkpoint_absent(
                &wire_events,
                &checkpoint_id,
            )
        })
        .await
        .map(|result| result.is_some())
    }

    /// Retrieve events for a session starting from `from_seq` (inclusive).
    pub async fn get_events(&self, session_id: &str, from_seq: usize) -> Result<Vec<SessionEvent>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_events(&session_id, from_seq))
            .await
    }

    /// Retrieve at most `limit` events for a session from `from_seq`.
    pub async fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_events_limited(&session_id, from_seq, limit))
            .await
    }

    pub async fn get_session_domain_events_by_kind_limited(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let session_id = session_id.to_string();
        let kind = kind.to_string();
        self.execute_read(move |backend| {
            backend.get_session_domain_events_by_kind_limited(&session_id, &kind, from_seq, limit)
        })
        .await
    }

    pub async fn get_latest_session_domain_event_by_kind(
        &self,
        session_id: &str,
        kind: &str,
    ) -> Result<Option<SessionEvent>> {
        let session_id = session_id.to_string();
        let kind = kind.to_string();
        self.execute_read(move |backend| {
            backend.get_latest_session_domain_event_by_kind(&session_id, &kind)
        })
        .await
    }

    pub async fn has_session_domain_event_kind(&self, kind: &str) -> Result<bool> {
        let kind = kind.to_string();
        self.execute_read(move |backend| backend.has_session_domain_event_kind(&kind))
            .await
    }

    pub async fn has_session_with_domain_event_kinds(&self, kinds: &[String]) -> Result<bool> {
        let kinds = kinds.to_vec();
        self.execute_read(move |backend| backend.has_session_with_domain_event_kinds(&kinds))
            .await
    }

    pub async fn count_session_domain_events_by_kind_from(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let session_id = session_id.to_string();
        let kind = kind.to_string();
        self.execute_read(move |backend| {
            backend.count_session_domain_events_by_kind_from(&session_id, &kind, from_seq)
        })
        .await
    }

    async fn get_session_domain_timeline_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| {
            backend.get_session_domain_timeline_limited(&session_id, from_seq, limit)
        })
        .await
    }

    async fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| {
            backend.count_session_domain_timeline_from(&session_id, from_seq)
        })
        .await
    }

    /// Retrieve at most `limit` events of one type for a session.
    pub async fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let session_id = session_id.to_string();
        let event_type = event_type.to_string();
        self.execute_read(move |backend| {
            backend.get_events_by_type_limited(&session_id, &event_type, from_seq, limit)
        })
        .await
    }

    /// Count events for a session from `from_seq`.
    pub async fn count_events_from(&self, session_id: &str, from_seq: usize) -> Result<usize> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.count_events_from(&session_id, from_seq))
            .await
    }

    /// Count events of one type for a session from `from_seq`.
    pub async fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let session_id = session_id.to_string();
        let event_type = event_type.to_string();
        self.execute_read(move |backend| {
            backend.count_events_by_type_from(&session_id, &event_type, from_seq)
        })
        .await
    }

    /// Retrieve a page of canonical session-domain events.
    pub async fn session_domain_events_page(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<SessionDomainEventPage> {
        let limit = clamp_event_page_limit(limit);
        let total = self
            .count_events_by_type_from(session_id, SESSION_DOMAIN_EVENT_TYPE, from_seq)
            .await?;
        let events = self
            .get_events_by_type_limited(session_id, SESSION_DOMAIN_EVENT_TYPE, from_seq, limit)
            .await?
            .into_iter()
            .map(|event| {
                SessionDomainEvent::from_session_event(&event).map_err(SessionError::Serialization)
            })
            .collect::<Result<Vec<_>>>()?;
        let next_seq = events.last().map(|event| event.sequence + 1);
        let has_more = events.len() < total;

        Ok(SessionDomainEventPage {
            total,
            events,
            next_seq,
            has_more,
        })
    }

    pub async fn session_usage_summary(&self, recent_limit: usize) -> Result<SessionUsageSummary> {
        self.execute_read(move |backend| backend.session_usage_summary(recent_limit))
            .await
    }

    /// Retrieve the canonical Session-domain timeline.
    ///
    /// Legacy transcript/runtime rows are intentionally not projected into a
    /// synthetic domain DTO. Their owners must migrate them explicitly.
    pub async fn timeline_events_page(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<SessionDomainEventPage> {
        let limit = clamp_event_page_limit(limit);
        let total = self
            .count_session_domain_timeline_from(session_id, from_seq)
            .await?;
        let events = self
            .get_session_domain_timeline_limited(session_id, from_seq, limit)
            .await?
            .into_iter()
            .map(|event| {
                SessionDomainEvent::from_session_event(&event).map_err(SessionError::Serialization)
            })
            .collect::<Result<Vec<_>>>()?;
        let next_seq = events.last().map(|event| event.sequence + 1);
        let has_more = events.len() < total;

        Ok(SessionDomainEventPage {
            total,
            events,
            next_seq,
            has_more,
        })
    }

    /// Retrieve a context envelope event by its envelope id.
    pub async fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>> {
        let envelope_id = envelope_id.to_string();
        self.execute_read(move |backend| backend.get_context_event_by_envelope_id(&envelope_id))
            .await
    }

    /// Return the next append sequence for a session event.
    pub async fn next_event_sequence(&self, session_id: &str) -> Result<usize> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.next_event_sequence(&session_id))
            .await
    }

    /// Delete all events from `from_sequence` onward in a session.
    ///
    /// Returns the number of deleted events.
    pub async fn delete_events_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        let session_id = session_id.to_string();
        self.execute_write(move |backend| backend.delete_events_from(&session_id, from_sequence))
            .await
    }

    /// Delete events of one type from `from_sequence` onward in a session.
    ///
    /// Returns the number of deleted events.
    pub async fn delete_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        let session_id = session_id.to_string();
        let event_type = event_type.to_string();
        self.execute_write(move |backend| {
            backend.delete_events_by_type_from(&session_id, &event_type, from_sequence)
        })
        .await
    }

    /// Save a full-message-list snapshot at a given event index.
    pub async fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<()> {
        let snapshot = snapshot.clone();
        self.execute_write(move |backend| backend.save_snapshot(&snapshot))
            .await
    }

    /// Return the most recent snapshot for a session, or `None`.
    pub async fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_latest_snapshot(&session_id))
            .await
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`.
    ///
    /// Returns the number of sessions that were removed.
    pub async fn prune_before(&self, cutoff_iso8601: &str) -> Result<usize> {
        let cutoff_iso8601 = cutoff_iso8601.to_string();
        self.execute_background(true, move |backend| backend.prune_before(&cutoff_iso8601))
            .await
    }

    // -----------------------------------------------------------------------
    // Messages
    // -----------------------------------------------------------------------

    /// Insert a single message into a session.
    pub async fn insert_message(&self, msg: &SessionMessage) -> Result<()> {
        let msg = msg.clone();
        self.execute_write(move |backend| backend.insert_message(&msg))
            .await
    }

    /// Atomically materialize a Runtime transcript and settle the exact
    /// Session input claim that produced it.
    pub async fn commit_terminal_transcript_if_fenced(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> Result<SessionTerminalTranscriptReceipt> {
        let request = request.clone();
        self.execute_write(move |backend| backend.commit_terminal_transcript_if_fenced(&request))
            .await
    }

    /// Insert multiple messages into a session in a single batch.
    pub async fn insert_messages_batch(&self, messages: &[SessionMessage]) -> Result<()> {
        let messages = messages.to_vec();
        self.execute_write(move |backend| backend.insert_messages_batch(&messages))
            .await
    }

    /// Copy a stable source prefix into an empty branch transcript in one
    /// backend transaction. Concurrent source appends after the captured
    /// cutoff are deliberately excluded.
    pub async fn copy_session_messages_at_cutoff(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        source_message_count: usize,
    ) -> Result<usize> {
        let source_session_id = source_session_id.to_string();
        let target_session_id = target_session_id.to_string();
        self.execute_write(move |backend| {
            backend.copy_session_messages_at_cutoff(
                &source_session_id,
                &target_session_id,
                source_message_count,
            )
        })
        .await
    }

    pub async fn branch_session_at_cutoff(
        &self,
        request: &crate::persistence::sqlite::SessionBranchRequest,
    ) -> Result<crate::persistence::sqlite::SessionBranchResult> {
        let request = request.clone();
        self.execute_write(move |backend| backend.branch_session_at_cutoff(&request))
            .await
    }

    pub async fn get_session_branch_activation(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionBranchActivation>> {
        let operation_id = operation_id.to_string();
        self.execute_read(move |backend| backend.get_session_branch_activation(&operation_id))
            .await
    }

    pub async fn list_recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionBranchActivation>> {
        self.execute_background(false, move |backend| {
            backend.list_recoverable_session_branch_activations(limit)
        })
        .await
    }

    pub async fn transition_session_branch_activation(
        &self,
        transition: &SessionBranchActivationTransition,
    ) -> Result<SessionBranchActivation> {
        let transition = transition.clone();
        self.execute_write(move |backend| backend.transition_session_branch_activation(&transition))
            .await
    }

    /// Persist a source message and Runtime ingress request atomically.
    pub async fn append_message_with_runtime_outbox(
        &self,
        message: &SessionMessage,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let message = message.clone();
        let request = request.clone();
        self.execute_write(move |backend| {
            backend.append_message_with_runtime_outbox(&message, &request)
        })
        .await
    }

    /// Atomically allocate a message sequence and persist the Runtime ingress
    /// work item. This is the canonical live-input API.
    pub async fn append_ingress_with_runtime_outbox(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let session_id = session_id.to_string();
        let role = role.to_string();
        let content_json = content_json.map(str::to_string);
        let request = request.clone();
        self.execute_write(move |backend| {
            backend.append_ingress_with_runtime_outbox(
                &session_id,
                &role,
                content_json.as_deref(),
                created_at_ms,
                &request,
            )
        })
        .await
    }

    pub async fn claim_session_runtime_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let worker_id = worker_id.to_string();
        self.execute_write(move |backend| {
            backend.claim_session_runtime_outbox(&worker_id, now_ms, lease_ms, limit)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn mark_session_runtime_outbox_running(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        let claim_token = claim_token.to_string();
        self.execute_write(move |backend| {
            backend.mark_session_runtime_outbox_running(
                &request_id,
                &worker_id,
                session_generation,
                &claim_token,
                expected_revision,
                now_ms,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ack_session_runtime_outbox(
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
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        let claim_token = claim_token.to_string();
        self.execute_write(move |backend| {
            backend.ack_session_runtime_outbox(
                &request_id,
                &worker_id,
                session_generation,
                &claim_token,
                expected_revision,
                terminal_status,
                runtime_commit_cursor,
                now_ms,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn renew_session_runtime_outbox_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        let claim_token = claim_token.to_string();
        self.execute_write(move |backend| {
            backend.renew_session_runtime_outbox_lease(
                &request_id,
                &worker_id,
                session_generation,
                &claim_token,
                expected_revision,
                now_ms,
                lease_ms,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fail_session_runtime_outbox(
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
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        let claim_token = claim_token.to_string();
        let error = error.to_string();
        self.execute_write(move |backend| {
            backend.fail_session_runtime_outbox(
                &request_id,
                &worker_id,
                session_generation,
                &claim_token,
                expected_revision,
                failure_class,
                &error,
                retry_at_ms,
                max_attempts,
                now_ms,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn requeue_claimed_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        decision: harness_contract::turn::InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        let claim_token = claim_token.to_string();
        let target_turn_id = target_turn_id.map(str::to_string);
        let classification_json = classification_json.map(str::to_string);
        let reason = reason.to_string();
        self.execute_write(move |backend| {
            backend.requeue_claimed_session_runtime_outbox(
                &request_id,
                &worker_id,
                session_generation,
                &claim_token,
                expected_revision,
                decision,
                target_turn_id.as_deref(),
                classification_json.as_deref(),
                &reason,
                now_ms,
            )
        })
        .await
    }

    pub async fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let request_id = request_id.to_string();
        let actor = actor.to_string();
        let reason = reason.to_string();
        self.execute_write(move |backend| {
            backend.retry_blocked_session_runtime_outbox(
                &request_id,
                session_generation,
                expected_revision,
                &actor,
                &reason,
                now_ms,
            )
        })
        .await
    }

    pub async fn cancel_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let input_id = input_id.to_string();
        let actor = actor.to_string();
        let reason = reason.to_string();
        self.execute_write(move |backend| {
            backend.cancel_session_runtime_outbox(
                &input_id,
                session_generation,
                expected_revision,
                &actor,
                &reason,
                now_ms,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reclassify_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        decision: harness_contract::turn::InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let input_id = input_id.to_string();
        let target_turn_id = target_turn_id.map(str::to_string);
        let classification_json = classification_json.map(str::to_string);
        let actor = actor.to_string();
        let reason = reason.to_string();
        self.execute_write(move |backend| {
            backend.reclassify_session_runtime_outbox(
                &input_id,
                session_generation,
                expected_revision,
                decision,
                target_turn_id.as_deref(),
                classification_json.as_deref(),
                &actor,
                &reason,
                now_ms,
            )
        })
        .await
    }

    pub async fn get_session_input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionInputAdmission>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_session_input_admission(&session_id))
            .await
    }

    pub async fn close_session_input_admission(
        &self,
        session_id: &str,
        expected_generation: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission> {
        let session_id = session_id.to_string();
        let actor = actor.to_string();
        let reason = reason.to_string();
        self.execute_write(move |backend| {
            backend.close_session_input_admission(
                &session_id,
                expected_generation,
                &actor,
                &reason,
                now_ms,
            )
        })
        .await
    }

    pub async fn advance_session_input_generation(
        &self,
        session_id: &str,
        expected_generation: u64,
        open: bool,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission> {
        let session_id = session_id.to_string();
        let actor = actor.to_string();
        let reason = reason.to_string();
        self.execute_write(move |backend| {
            backend.advance_session_input_generation(
                &session_id,
                expected_generation,
                open,
                &actor,
                &reason,
                now_ms,
            )
        })
        .await
    }

    pub async fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let request_id = request_id.to_string();
        self.execute_read(move |backend| backend.get_session_runtime_outbox(&request_id))
            .await
    }

    pub async fn get_session_runtime_outbox_by_input_id(
        &self,
        input_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let input_id = input_id.to_string();
        self.execute_read(move |backend| backend.get_session_runtime_outbox_by_input_id(&input_id))
            .await
    }

    pub async fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| {
            backend.session_runtime_outbox_for_session(&session_id, limit)
        })
        .await
    }

    pub async fn session_runtime_outbox_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let session_ids = session_ids.to_vec();
        self.execute_read(move |backend| {
            backend.session_runtime_outbox_for_sessions(&session_ids, per_session_limit)
        })
        .await
    }

    pub async fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        self.execute_background(false, move |backend| {
            backend.active_session_runtime_outbox(limit)
        })
        .await
    }

    pub async fn session_runtime_outbox_health(&self) -> Result<SessionRuntimeOutboxHealth> {
        self.execute_background(false, |backend| backend.session_runtime_outbox_health())
            .await
    }

    pub async fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        self.execute_background(false, move |backend| {
            backend.blocked_session_runtime_outbox(limit)
        })
        .await
    }

    pub async fn claim_session_mission_outbox(
        &self,
        workspace_key: &str,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionMissionOutboxRecord>> {
        let workspace_key = workspace_key.to_string();
        let worker_id = worker_id.to_string();
        self.execute_write(move |backend| {
            backend.claim_session_mission_outbox(
                &workspace_key,
                &worker_id,
                now_ms,
                lease_ms,
                limit,
            )
        })
        .await
    }

    pub async fn ack_session_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord> {
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        self.execute_write(move |backend| {
            backend.ack_session_mission_outbox(&request_id, &worker_id, expected_revision, now_ms)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fail_session_mission_outbox(
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
        let request_id = request_id.to_string();
        let worker_id = worker_id.to_string();
        let error = error.to_string();
        self.execute_write(move |backend| {
            backend.fail_session_mission_outbox(
                &request_id,
                &worker_id,
                expected_revision,
                failure_class,
                &error,
                retry_at_ms,
                max_attempts,
                now_ms,
            )
        })
        .await
    }

    pub async fn get_session_mission_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionMissionOutboxRecord>> {
        let request_id = request_id.to_string();
        self.execute_read(move |backend| backend.get_session_mission_outbox(&request_id))
            .await
    }

    /// Retrieve messages for a session with pagination.
    pub async fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_messages(&session_id, offset, limit))
            .await
    }

    /// Retrieve messages for a session starting at `from_sequence`.
    pub async fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| {
            backend.get_messages_from_sequence(&session_id, from_sequence, limit)
        })
        .await
    }

    pub async fn get_message_by_stable_id(
        &self,
        session_id: &str,
        stable_message_id: &str,
    ) -> Result<Option<SessionMessage>> {
        let session_id = session_id.to_string();
        let stable_message_id = stable_message_id.to_string();
        self.execute_read(move |backend| {
            backend.get_message_by_stable_id(&session_id, &stable_message_id)
        })
        .await
    }

    pub async fn get_message_by_sequence(
        &self,
        session_id: &str,
        sequence: usize,
    ) -> Result<Option<SessionMessage>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_message_by_sequence(&session_id, sequence))
            .await
    }

    pub async fn get_messages_in_ranges(
        &self,
        session_id: &str,
        ranges: &[(usize, usize)],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let session_id = session_id.to_string();
        let ranges = ranges.to_vec();
        self.execute_read(move |backend| {
            backend.get_messages_in_ranges(&session_id, &ranges, limit)
        })
        .await
    }

    pub async fn get_message_metadata_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessageMetadata>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| {
            backend.get_message_metadata_page(&session_id, from_sequence, limit)
        })
        .await
    }

    pub async fn get_context_index_cards(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ContextIndexCard>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_context_index_cards(&session_id, limit))
            .await
    }

    /// Read the activation manifest and navigation index through one admitted
    /// backend operation. Exact transcript ranges are loaded only after
    /// Runtime relevance selection.
    pub async fn page_in_session_context(
        &self,
        session_id: &str,
        card_limit: usize,
    ) -> Result<Option<SessionContextPage>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| {
            let Some(recovery) = backend.get_session_recovery_manifest(&session_id)? else {
                return Ok(None);
            };
            let context_cards = backend.get_context_index_cards(&session_id, card_limit)?;
            Ok(Some(SessionContextPage {
                manifest: crate::persistence::SessionActivationManifest::from_recovery(recovery),
                context_cards,
            }))
        })
        .await
    }

    pub async fn reconcile_session_context_index(
        &self,
        session_id: &str,
        card_span: usize,
        parent_span: usize,
        now_ms: u64,
    ) -> Result<ContextIndexCoverage> {
        let session_id = session_id.to_string();
        self.execute_background(false, move |backend| {
            backend.reconcile_session_context_index(&session_id, card_span, parent_span, now_ms)
        })
        .await
    }

    /// Retrieve ALL messages for a session (unbounded, no pagination).
    pub async fn get_all_messages(&self, session_id: &str) -> Result<Vec<SessionMessage>> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_all_messages(&session_id))
            .await
    }

    /// Get the total number of messages in a session.
    pub async fn get_message_count(&self, session_id: &str) -> Result<usize> {
        let session_id = session_id.to_string();
        self.execute_read(move |backend| backend.get_message_count(&session_id))
            .await
    }

    /// Delete all messages from `from_sequence` onward in a session.
    ///
    /// Returns the number of deleted messages.
    pub async fn delete_messages_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        let session_id = session_id.to_string();
        self.execute_write(move |backend| backend.delete_messages_from(&session_id, from_sequence))
            .await
    }

    /// Search messages using FTS5 full-text search.
    ///
    /// Optionally scoped to a single session.
    pub async fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let query = query.to_string();
        let session_id = session_id.map(str::to_string);
        self.execute_read(move |backend| {
            backend.search_messages(&query, session_id.as_deref(), limit)
        })
        .await
    }

    /// Search a pre-authorized set of sessions in one FTS query.  The caller
    /// owns the authority decision; this store preserves the resulting scope
    /// in SQL so result ranking cannot be distorted by other tenants.
    pub async fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let query = query.to_string();
        let session_ids = session_ids.to_vec();
        self.execute_read(move |backend| {
            backend.search_messages_in_sessions(&query, &session_ids, limit)
        })
        .await
    }

    pub async fn search_messages_visible(
        &self,
        query: &str,
        owner_principal_id: Option<&str>,
        visible_session_ids: &[String],
        unrestricted: bool,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let query = query.to_string();
        let owner_principal_id = owner_principal_id.map(str::to_string);
        let visible_session_ids = visible_session_ids.to_vec();
        self.execute_read(move |backend| {
            backend.search_messages_visible(
                &query,
                owner_principal_id.as_deref(),
                &visible_session_ids,
                unrestricted,
                limit,
            )
        })
        .await
    }
}

fn clamp_event_page_limit(limit: usize) -> usize {
    limit.clamp(1, 500)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::SessionDomainScope;

    fn make_record(id: &str) -> SessionRecord {
        SessionRecord {
            session_id: id.to_string(),
            platform: "test".to_string(),
            chat_id: "chat-1".to_string(),
            user_id: Some("user-1".to_string()),
            model: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            last_activity: "2024-01-01T00:01:00Z".to_string(),
            message_count: 0,
            reset_policy: "None".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    fn make_message(session_id: &str, sequence: usize) -> SessionMessage {
        SessionMessage {
            stable_message_id: format!("{session_id}:{sequence}"),
            session_id: session_id.to_string(),
            sequence,
            role: "user".to_string(),
            content_json: format!(r#"[{{"type":"text","text":"message-{sequence}"}}]"#),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: sequence as u64,
        }
    }

    #[tokio::test]
    async fn session_domain_events_page_returns_only_canonical_events() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("s-runtime-page"))
            .await
            .unwrap();
        store
            .append_event(&SessionEvent {
                session_id: "s-runtime-page".to_string(),
                event_type: "TextDelta".to_string(),
                event_json: serde_json::json!({"text": "legacy"}).to_string(),
                sequence: 0,
                created_at_ms: 1,
            })
            .await
            .unwrap();
        store
            .append_session_domain_event(&SessionDomainEvent::new(
                "s-runtime-page",
                1,
                SessionDomainScope::Turn,
                "turn.completed",
                serde_json::json!({"ok": true}),
                2,
            ))
            .await
            .unwrap();

        let page = store
            .session_domain_events_page("s-runtime-page", 0, 50)
            .await
            .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].kind, "turn.completed");
        assert_eq!(page.next_seq, Some(2));
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn domain_event_allocator_keeps_column_and_envelope_sequence_equal() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("s-runtime-allocated"))
            .await
            .unwrap();
        let stored = store
            .append_session_domain_event_allocating_sequence(&SessionDomainEvent::new(
                "s-runtime-allocated",
                9_999,
                SessionDomainScope::Turn,
                "turn.completed",
                serde_json::json!({"ok": true}),
                2,
            ))
            .await
            .unwrap();
        assert_eq!(stored.sequence, 0);

        let page = store
            .session_domain_events_page("s-runtime-allocated", 0, 10)
            .await
            .unwrap();
        assert_eq!(page.events[0].sequence, 0);
        assert_eq!(page.next_seq, Some(1));
    }

    #[tokio::test]
    async fn domain_event_batch_is_contiguous_and_visible_as_one_compaction_pair() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("s-runtime-compaction-batch"))
            .await
            .unwrap();
        let events = [
            SessionDomainEvent::new(
                "s-runtime-compaction-batch",
                0,
                SessionDomainScope::Context,
                "context.session_compacted",
                serde_json::json!({"checkpoint_id":"checkpoint-1"}),
                1,
            ),
            SessionDomainEvent::new(
                "s-runtime-compaction-batch",
                0,
                SessionDomainScope::Memory,
                "memory.semantic_checkpoint.created",
                serde_json::json!({"checkpoint_id":"checkpoint-1"}),
                1,
            ),
        ];
        let stored = store
            .append_session_domain_events_allocating_sequence(&events)
            .await
            .unwrap();
        assert_eq!(
            stored
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let page = store
            .session_domain_events_page("s-runtime-compaction-batch", 0, 10)
            .await
            .unwrap();
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.events[0].kind, "context.session_compacted");
        assert_eq!(page.events[1].kind, "memory.semantic_checkpoint.created");
    }

    #[tokio::test]
    async fn checkpoint_batch_retry_reuses_the_committed_bundle() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("s-runtime-compaction-dedup"))
            .await
            .unwrap();
        let checkpoint_id = "checkpoint-stable-1";
        let events = || {
            vec![
                SessionDomainEvent::new(
                    "s-runtime-compaction-dedup",
                    0,
                    SessionDomainScope::Context,
                    "context.session_compacted",
                    serde_json::json!({"checkpoint_id":checkpoint_id}),
                    1,
                ),
                SessionDomainEvent::new(
                    "s-runtime-compaction-dedup",
                    0,
                    SessionDomainScope::Memory,
                    "memory.semantic_checkpoint.created",
                    serde_json::json!({"checkpoint":{"checkpoint_id":checkpoint_id}}),
                    1,
                ),
            ]
        };
        assert!(store
            .append_session_domain_events_if_checkpoint_absent(&events(), checkpoint_id)
            .await
            .unwrap());
        assert!(!store
            .append_session_domain_events_if_checkpoint_absent(&events(), checkpoint_id)
            .await
            .unwrap());
        assert_eq!(
            store
                .session_domain_events_page("s-runtime-compaction-dedup", 0, 10)
                .await
                .unwrap()
                .events
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn timeline_events_page_excludes_legacy_rows() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("s-runtime-timeline"))
            .await
            .unwrap();
        store
            .append_event(&SessionEvent {
                session_id: "s-runtime-timeline".to_string(),
                event_type: "ToolStart".to_string(),
                event_json: serde_json::json!({"tool": "shell"}).to_string(),
                sequence: 0,
                created_at_ms: 1,
            })
            .await
            .unwrap();
        store
            .append_session_domain_event(&SessionDomainEvent::new(
                "s-runtime-timeline",
                1,
                SessionDomainScope::Memory,
                "memory.pulse.created",
                serde_json::json!({"candidates": 3}),
                2,
            ))
            .await
            .unwrap();

        let page = store
            .timeline_events_page("s-runtime-timeline", 0, 1)
            .await
            .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].kind, "memory.pulse.created");
        assert_eq!(page.events[0].scope, SessionDomainScope::Memory);
        assert_eq!(page.next_seq, Some(2));
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn concurrent_allocating_appends_produce_one_ordered_sequence() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("s-concurrent-sequence"))
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for index in 0..100usize {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .append_event_allocating_sequence(&SessionEvent {
                        session_id: "s-concurrent-sequence".to_string(),
                        event_type: "concurrent".to_string(),
                        event_json: format!(r#"{{"index":{index}}}"#),
                        sequence: usize::MAX,
                        created_at_ms: index as u64,
                    })
                    .await
                    .unwrap()
                    .sequence
            }));
        }
        let mut allocated = Vec::new();
        for task in tasks {
            allocated.push(task.await.unwrap());
        }
        allocated.sort_unstable();
        assert_eq!(allocated, (0..100).collect::<Vec<_>>());

        let replay = store.get_events("s-concurrent-sequence", 0).await.unwrap();
        assert_eq!(replay.len(), 100);
        assert!(replay
            .iter()
            .enumerate()
            .all(|(expected, event)| event.sequence == expected));
    }

    #[tokio::test]
    async fn list_sessions_by_workspace_root_uses_db_metadata_namespace() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        let workspace_a = "/tmp/cowd-unified-a";
        let workspace_b = "/tmp/cowd-unified-b";

        let mut older = make_record("unified-a-older");
        older.last_activity = "2024-01-01T00:00:00Z".to_string();
        older.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
        store.create_session(&older).await.unwrap();

        let mut newer = make_record("unified-a-newer");
        newer.last_activity = "2024-01-02T00:00:00Z".to_string();
        newer.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
        store.create_session(&newer).await.unwrap();

        let mut other = make_record("unified-b");
        other.metadata_json = Some(serde_json::json!({"workspace_root": workspace_b}).to_string());
        store.create_session(&other).await.unwrap();

        let records = store
            .list_sessions_by_workspace_root(workspace_a)
            .await
            .expect("workspace sessions should list");

        assert_eq!(
            records
                .iter()
                .map(|record| record.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["unified-a-newer", "unified-a-older"]
        );
    }

    #[tokio::test]
    async fn repository_operations_are_attributed_to_their_semantic_lanes() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("lane-routing"))
            .await
            .unwrap();
        assert!(store.get_session("lane-routing").await.unwrap().is_some());
        let _ = store
            .list_active_session_recovery_manifests(0, 10)
            .await
            .unwrap();

        let stats = store.execution_stats();
        assert_eq!(stats.interactive_write.submitted, 1);
        assert_eq!(stats.interactive_read.submitted, 1);
        assert_eq!(stats.background.submitted, 1);
        assert_eq!(stats.submitted, 3);
        assert_eq!(stats.completed, 3);
        assert_eq!(stats.active, 0);
        assert_eq!(stats.queued, 0);
    }

    #[tokio::test]
    async fn cold_context_page_uses_one_admitted_read_operation() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("context-page"))
            .await
            .unwrap();
        let before = store.execution_stats();

        let page = store
            .page_in_session_context("context-page", 128)
            .await
            .expect("context page read")
            .expect("session context page");

        let after = store.execution_stats();
        assert_eq!(page.manifest.recovery.session_id, "context-page");
        assert_eq!(
            after
                .interactive_read
                .submitted
                .saturating_sub(before.interactive_read.submitted),
            1,
            "manifest and cards must share one admitted read"
        );
    }

    #[tokio::test]
    async fn selected_context_ranges_share_one_admitted_read_and_remain_exact() {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        store
            .create_session(&make_record("context-ranges"))
            .await
            .unwrap();
        let messages = (0..20)
            .map(|sequence| make_message("context-ranges", sequence))
            .collect::<Vec<_>>();
        store.insert_messages_batch(&messages).await.unwrap();
        let before = store.execution_stats();

        let selected = store
            .get_messages_in_ranges("context-ranges", &[(2, 5), (12, 15)], 32)
            .await
            .expect("selected ranges");

        let after = store.execution_stats();
        assert_eq!(
            selected
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4, 12, 13, 14]
        );
        assert_eq!(
            after
                .interactive_read
                .submitted
                .saturating_sub(before.interactive_read.submitted),
            1
        );
    }
}
