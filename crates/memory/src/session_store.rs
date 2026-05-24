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
//! use cowd_memory::SqliteSessionStore;
//! let store = SqliteSessionStore::open(path)?;
//! ```
//!
//! with:
//!
//! ```rust,no_run
//! use cowd_memory::UnifiedSessionStore;
//! let store = UnifiedSessionStore::open(path)?;
//! ```

use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::store::session::{SessionRecord, SessionSearchResult, SqliteSessionStore};
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
    inner: Arc<Mutex<SqliteSessionStore>>,
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
    pub fn create_session(&self, session: &SessionRecord) -> Result<()> {
        self.inner.lock().unwrap().create_session(session)
    }

    /// Retrieve a session record by its ID, or `None` if not found.
    pub fn get_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        self.inner.lock().unwrap().get_session(session_id)
    }

    /// Overwrite all mutable fields of an existing session record.
    ///
    /// `session_id` is used as the lookup key; the row is silently unchanged
    /// if it does not exist.
    pub fn update_session(&self, session: &SessionRecord) -> Result<()> {
        self.inner.lock().unwrap().update_session(session)
    }

    /// Upsert a session record (insert or replace all fields).
    ///
    /// Equivalent to calling [`create_session`] then [`update_session`].  Use
    /// this when you don't know whether the row already exists.
    pub fn upsert_session(&self, session: &SessionRecord) -> Result<()> {
        self.inner.lock().unwrap().upsert_session(session)
    }

    /// Permanently remove a session and all its memory associations.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        self.inner.lock().unwrap().delete_session(session_id)
    }

    /// List all session records ordered by `last_activity DESC`.
    pub fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        self.inner.lock().unwrap().list_sessions()
    }

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        self.inner.lock().unwrap().list_sessions_by_platform(platform)
    }

    /// Search sessions using FTS5 full-text search.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub fn search_sessions(&self, query: &str, limit: usize) -> Result<Vec<SessionSearchResult>> {
        self.inner.lock().unwrap().search_sessions(query, limit)
    }

    /// Search sessions with platform filter.
    pub fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        self.inner.lock().unwrap().search_sessions_by_platform(query, platform, limit)
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        self.inner.lock().unwrap().associate_memory(session_id, memory_id)
    }

    /// Return all memory IDs associated with `session_id`.
    pub fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        self.inner.lock().unwrap().get_session_memories(session_id)
    }

    /// Remove the association between a session and a memory.
    pub fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        self.inner.lock().unwrap().disassociate_memory(session_id, memory_id)
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_before(&self, cutoff_iso8601: &str) -> Result<usize> {
        self.inner.lock().unwrap().prune_before(cutoff_iso8601)
    }
}
