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

use crate::store::session::{SessionEvent, SessionMessage, SessionRecord, SessionSearchResult, SessionSnapshot, SqliteSessionStore};
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

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub async fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        self.inner.lock().await.list_sessions_by_platform(platform)
    }

    /// Search sessions using FTS5 full-text search.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub async fn search_sessions(&self, query: &str, limit: usize) -> Result<Vec<SessionSearchResult>> {
        self.inner.lock().await.search_sessions(query, limit)
    }

    /// Search sessions with platform filter.
    pub async fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        self.inner.lock().await.search_sessions_by_platform(query, platform, limit)
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub async fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        self.inner.lock().await.associate_memory(session_id, memory_id)
    }

    /// Return all memory IDs associated with `session_id`.
    pub async fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        self.inner.lock().await.get_session_memories(session_id)
    }

    /// Remove the association between a session and a memory.
    pub async fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        self.inner.lock().await.disassociate_memory(session_id, memory_id)
    }

    // -----------------------------------------------------------------------
    // Event log
    // -----------------------------------------------------------------------

    /// Append a mutation event to the session's event log.
    pub async fn append_event(&self, event: &SessionEvent) -> Result<()> {
        self.inner.lock().await.append_event(event)
    }

    /// Retrieve events for a session starting from `from_seq` (inclusive).
    pub async fn get_events(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> Result<Vec<SessionEvent>> {
        self.inner.lock().await.get_events(session_id, from_seq)
    }

    /// Save a full-message-list snapshot at a given event index.
    pub async fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<()> {
        self.inner.lock().await.save_snapshot(snapshot)
    }

    /// Return the most recent snapshot for a session, or `None`.
    pub async fn get_latest_snapshot(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionSnapshot>> {
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
        self.inner.lock().await.get_messages(session_id, offset, limit)
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
        self.inner.lock().await.delete_messages_from(session_id, from_sequence)
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
        self.inner.lock().await.search_messages(query, session_id, limit)
    }
}
