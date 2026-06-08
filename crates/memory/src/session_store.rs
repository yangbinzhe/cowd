//! Unified session store — the canonical wrapper for session persistence.
//!
//! `UnifiedSessionStore` delegates to [`SqliteSessionStore`] internally,
//! providing a stable public API while the backend implementation can evolve
//! independently.
//!
//! # Migration from SqliteSessionStore
//!
//! Replace:
//!
//! ```rust,no_run
//! use cowd_memory::store::session::SqliteSessionStore;
//! use std::path::Path;
//! let store = SqliteSessionStore::open(Path::new("sessions.db")).unwrap();
//! ```

//! with:

//! ```rust,no_run
//! use cowd_memory::UnifiedSessionStore;
//! use std::path::Path;
//! let store = UnifiedSessionStore::open(Path::new("sessions.db")).unwrap();
//! ```

use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::runtime_event::{RuntimeEvent, RuntimeEventPage, RUNTIME_EVENT_TYPE};
use crate::store::session::{
    SessionEvent, SessionListOptions, SessionListPage, SessionMessage, SessionRecord,
    SessionSearchResult, SessionSnapshot, SqliteSessionStore,
};
use crate::store::Result;

// ---------------------------------------------------------------------------
// UnifiedSessionStore
// ---------------------------------------------------------------------------

/// Canonical session store for the cowd AI assistant.
///
/// Wraps [`SqliteSessionStore`] with `Arc<Mutex<>>` for safe shared access
/// across threads.  The public API mirrors the inner store exactly — every
/// method opens a fresh SQLite connection under the hood, so WAL-mode
/// concurrency is preserved.
///
/// # Example
///
/// ```rust,no_run
/// use cowd_memory::UnifiedSessionStore;
/// use std::path::Path;
///
/// let store = UnifiedSessionStore::open(Path::new("sessions.db")).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct UnifiedSessionStore {
    inner: Arc<tokio::sync::Mutex<SqliteSessionStore>>,
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
            inner: Arc::new(Mutex::new(store)),
        })
    }

    /// Open an in-memory session database (useful for testing).
    pub fn open_in_memory() -> Result<Self> {
        let store = SqliteSessionStore::open_in_memory()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(store)),
        })
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// Insert a new session record.
    ///
    /// Uses `INSERT OR IGNORE` so calling this for an already-existing session
    /// is a harmless no-op.
    pub async fn create_session(&self, session: &SessionRecord) -> Result<()> {
        self.inner.lock().await.create_session(session)
    }

    /// Retrieve a session record by its ID, or `None` if not found.
    pub async fn get_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        self.inner.lock().await.get_session(session_id)
    }

    /// Overwrite all mutable fields of an existing session record.
    ///
    /// `session_id` is used as the lookup key; the row is silently unchanged
    /// if it does not exist.
    pub async fn update_session(&self, session: &SessionRecord) -> Result<()> {
        self.inner.lock().await.update_session(session)
    }

    /// Upsert a session record (insert or replace all fields).
    ///
    /// Equivalent to calling [`create_session`] then [`update_session`].  Use
    /// this when you don't know whether the row already exists.
    pub async fn upsert_session(&self, session: &SessionRecord) -> Result<()> {
        self.inner.lock().await.upsert_session(session)
    }

    /// Permanently remove a session and all its memory associations.
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        self.inner.lock().await.delete_session(session_id)
    }

    /// Mark a session as closed.
    ///
    /// Updates the session's status to `'closed'` and refreshes
    /// `last_activity`.  Messages are preserved for auditing.
    pub async fn mark_session_closed(&self, session_id: &str) -> Result<()> {
        self.inner.lock().await.mark_session_closed(session_id)
    }

    /// List all session records ordered by `last_activity DESC`.
    pub async fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        self.inner.lock().await.list_sessions()
    }

    /// List a filtered, sorted page of sessions directly in the backing store.
    pub async fn list_sessions_page(
        &self,
        opts: &SessionListOptions<'_>,
    ) -> Result<SessionListPage> {
        self.inner.lock().await.list_sessions_page(opts)
    }

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub async fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        self.inner.lock().await.list_sessions_by_platform(platform)
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
        self.inner
            .lock()
            .await
            .list_sessions_by_workspace_root(workspace_root)
    }

    /// Search sessions using FTS5 full-text search.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub async fn search_sessions(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        self.inner.lock().await.search_sessions(query, limit)
    }

    /// Search sessions with platform filter.
    pub async fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        self.inner
            .lock()
            .await
            .search_sessions_by_platform(query, platform, limit)
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub async fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        self.inner
            .lock()
            .await
            .associate_memory(session_id, memory_id)
    }

    /// Return all memory IDs associated with `session_id`.
    pub async fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        self.inner.lock().await.get_session_memories(session_id)
    }

    /// Remove the association between a session and a memory.
    pub async fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        self.inner
            .lock()
            .await
            .disassociate_memory(session_id, memory_id)
    }

    // -----------------------------------------------------------------------
    // Event log
    // -----------------------------------------------------------------------

    /// Append a mutation event to the session's event log.
    pub async fn append_event(&self, event: &SessionEvent) -> Result<()> {
        self.inner.lock().await.append_event(event)
    }

    /// Append a context envelope event unless the same envelope id already exists.
    pub async fn append_context_envelope_event_if_absent(
        &self,
        event: &SessionEvent,
    ) -> Result<bool> {
        self.inner
            .lock()
            .await
            .append_context_envelope_event_if_absent(event)
    }

    /// Append a canonical runtime event to the session event log.
    pub async fn append_runtime_event(&self, event: &RuntimeEvent) -> Result<()> {
        let event = event.to_session_event()?;
        self.append_event(&event).await
    }

    /// Retrieve events for a session starting from `from_seq` (inclusive).
    pub async fn get_events(&self, session_id: &str, from_seq: usize) -> Result<Vec<SessionEvent>> {
        self.inner.lock().await.get_events(session_id, from_seq)
    }

    /// Retrieve at most `limit` events for a session from `from_seq`.
    pub async fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        self.inner
            .lock()
            .await
            .get_events_limited(session_id, from_seq, limit)
    }

    /// Retrieve at most `limit` events of one type for a session.
    pub async fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        self.inner
            .lock()
            .await
            .get_events_by_type_limited(session_id, event_type, from_seq, limit)
    }

    /// Count events for a session from `from_seq`.
    pub async fn count_events_from(&self, session_id: &str, from_seq: usize) -> Result<usize> {
        self.inner
            .lock()
            .await
            .count_events_from(session_id, from_seq)
    }

    /// Count events of one type for a session from `from_seq`.
    pub async fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> Result<usize> {
        self.inner
            .lock()
            .await
            .count_events_by_type_from(session_id, event_type, from_seq)
    }

    /// Retrieve a page of canonical runtime events.
    pub async fn runtime_events_page(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<RuntimeEventPage> {
        let limit = clamp_event_page_limit(limit);
        let total = self
            .count_events_by_type_from(session_id, RUNTIME_EVENT_TYPE, from_seq)
            .await?;
        let events = self
            .get_events_by_type_limited(session_id, RUNTIME_EVENT_TYPE, from_seq, limit)
            .await?
            .into_iter()
            .map(|event| RuntimeEvent::from_session_event_lossy(&event))
            .collect::<Vec<_>>();
        let next_seq = events.last().map(|event| event.sequence + 1);
        let has_more = events.len() < total;

        Ok(RuntimeEventPage {
            total,
            events,
            next_seq,
            has_more,
        })
    }

    /// Retrieve a runtime-shaped projection of every session event type.
    pub async fn timeline_events_page(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<RuntimeEventPage> {
        let limit = clamp_event_page_limit(limit);
        let total = self.count_events_from(session_id, from_seq).await?;
        let events = self
            .get_events_limited(session_id, from_seq, limit)
            .await?
            .into_iter()
            .map(|event| RuntimeEvent::from_session_event_lossy(&event))
            .collect::<Vec<_>>();
        let next_seq = events.last().map(|event| event.sequence + 1);
        let has_more = events.len() < total;

        Ok(RuntimeEventPage {
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
        self.inner
            .lock()
            .await
            .get_context_event_by_envelope_id(envelope_id)
    }

    /// Return the next append sequence for a session event.
    pub async fn next_event_sequence(&self, session_id: &str) -> Result<usize> {
        self.inner.lock().await.next_event_sequence(session_id)
    }

    /// Delete all events from `from_sequence` onward in a session.
    ///
    /// Returns the number of deleted events.
    pub async fn delete_events_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        self.inner
            .lock()
            .await
            .delete_events_from(session_id, from_sequence)
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
        self.inner
            .lock()
            .await
            .delete_events_by_type_from(session_id, event_type, from_sequence)
    }

    /// Save a full-message-list snapshot at a given event index.
    pub async fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<()> {
        self.inner.lock().await.save_snapshot(snapshot)
    }

    /// Return the most recent snapshot for a session, or `None`.
    pub async fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        self.inner.lock().await.get_latest_snapshot(session_id)
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`.
    ///
    /// Returns the number of sessions that were removed.
    pub async fn prune_before(&self, cutoff_iso8601: &str) -> Result<usize> {
        self.inner.lock().await.prune_before(cutoff_iso8601)
    }

    // -----------------------------------------------------------------------
    // Messages
    // -----------------------------------------------------------------------

    /// Insert a single message into a session.
    pub async fn insert_message(&self, msg: &SessionMessage) -> Result<()> {
        self.inner.lock().await.insert_message(msg)
    }

    /// Insert multiple messages into a session in a single batch.
    pub async fn insert_messages_batch(&self, messages: &[SessionMessage]) -> Result<()> {
        self.inner.lock().await.insert_messages_batch(messages)
    }

    /// Retrieve messages for a session with pagination.
    pub async fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        self.inner
            .lock()
            .await
            .get_messages(session_id, offset, limit)
    }

    /// Retrieve messages for a session starting at `from_sequence`.
    pub async fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        self.inner
            .lock()
            .await
            .get_messages_from_sequence(session_id, from_sequence, limit)
    }

    /// Retrieve ALL messages for a session (unbounded, no pagination).
    pub async fn get_all_messages(&self, session_id: &str) -> Result<Vec<SessionMessage>> {
        self.inner.lock().await.get_all_messages(session_id)
    }

    /// Get the total number of messages in a session.
    pub async fn get_message_count(&self, session_id: &str) -> Result<usize> {
        self.inner.lock().await.get_message_count(session_id)
    }

    /// Delete all messages from `from_sequence` onward in a session.
    ///
    /// Returns the number of deleted messages.
    pub async fn delete_messages_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        self.inner
            .lock()
            .await
            .delete_messages_from(session_id, from_sequence)
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
        self.inner
            .lock()
            .await
            .search_messages(query, session_id, limit)
    }
}

fn clamp_event_page_limit(limit: usize) -> usize {
    limit.clamp(1, 500)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_event::RuntimeEventScope;

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

    #[tokio::test]
    async fn runtime_events_page_returns_only_canonical_events() {
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
            .append_runtime_event(&RuntimeEvent::new(
                "s-runtime-page",
                1,
                RuntimeEventScope::Turn,
                "turn.completed",
                serde_json::json!({"ok": true}),
                2,
            ))
            .await
            .unwrap();

        let page = store
            .runtime_events_page("s-runtime-page", 0, 50)
            .await
            .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].kind, "turn.completed");
        assert_eq!(page.next_seq, Some(2));
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn timeline_events_page_projects_legacy_and_runtime_events() {
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
            .append_runtime_event(&RuntimeEvent::new(
                "s-runtime-timeline",
                1,
                RuntimeEventScope::Memory,
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
        assert_eq!(page.total, 2);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].kind, "ToolStart");
        assert_eq!(page.events[0].scope, RuntimeEventScope::Tool);
        assert_eq!(page.next_seq, Some(1));
        assert!(page.has_more);
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
}
