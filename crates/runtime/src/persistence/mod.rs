//! Persistence protocol — unified async storage abstraction.
//!
//! Defines the [`PersistenceProtocol`] trait that all storage backends
//! must implement. Business logic depends ONLY on this trait, never on
//! concrete backends. This enables pluggable storage (SQLite, in-memory,
//! cached decorator) without changing any business code.

pub mod sqlite;

use std::sync::Arc;
use async_trait::async_trait;
use crate::session::{ConversationMessage, SessionRecord};

// ── Persistence Protocol ────────────────────────────────────────

#[async_trait]
pub trait PersistenceProtocol: Send + Sync + 'static {
    // ── Session CRUD ──
    async fn create_session(&self, record: &SessionRecord) -> Result<()>;
    async fn get_session(&self, id: &str) -> Result<Option<SessionRecord>>;
    async fn list_sessions(&self) -> Result<Vec<SessionRecord>>;
    async fn update_session(&self, id: &str, record: &SessionRecord) -> Result<()>;
    async fn delete_session(&self, id: &str) -> Result<()>;

    // ── Type-native messages (zero JSON overhead) ──
    /// Fire-and-forget: returns immediately, actual write happens in background.
    /// Errors are logged but not propagated to caller.
    async fn append_message(&self, session_id: &str, msg: &ConversationMessage) -> Result<()>;
    /// Batch append within a single transaction. Awaited for consistency.
    async fn append_messages(&self, session_id: &str, msgs: &[ConversationMessage]) -> Result<()>;
    async fn get_messages(&self, session_id: &str) -> Result<Vec<ConversationMessage>>;
    async fn get_messages_range(&self, session_id: &str, from: usize, limit: usize) -> Result<Vec<ConversationMessage>>;
    async fn get_message_count(&self, session_id: &str) -> Result<usize>;
    async fn delete_messages_from(&self, session_id: &str, sequence: usize) -> Result<()>;

    // ── Full-text search ──
    async fn search_messages(&self, query: &str) -> Result<Vec<ConversationMessage>>;
    async fn search_sessions(&self, query: &str) -> Result<Vec<SessionRecord>>;

    // ── Snapshots ──
    async fn save_snapshot(&self, session_id: &str, messages: &[ConversationMessage]) -> Result<()>;
    async fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<Vec<ConversationMessage>>>;

    // ── Lifecycle ──
    /// Run cleanup based on the backend's configured CleanupConfig.
    /// Returns the number of sessions deleted.
    async fn cleanup(&self) -> Result<usize>;
    /// Force flush all pending writes + WAL checkpoint.
    async fn flush(&self) -> Result<()>;
    /// Return current store statistics.
    async fn stats(&self) -> Result<StoreStats>;
}

// ── Cleanup Configuration ───────────────────────────────────────

/// Cleanup policy: max_sessions and max_days are independently configurable.
/// Either can be None to disable that dimension.
/// Both None = never cleanup. OR semantics when both are set.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Keep at most N most-recent sessions. None = no count-based cleanup.
    pub max_sessions: Option<usize>,
    /// Keep sessions active within N days. None = no age-based cleanup.
    pub max_days: Option<u32>,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            max_sessions: Some(2000),
            max_days: Some(60),
        }
    }
}

// ── Store Statistics ────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct StoreStats {
    pub session_count: usize,
    pub message_count: usize,
    pub db_size_bytes: u64,
}

// ── Global Persistence Singleton ────────────────────────────────

static PERSISTENCE: std::sync::OnceLock<Arc<dyn PersistenceProtocol>> = std::sync::OnceLock::new();

/// Initialize the global persistence backend. Must be called once at startup.
pub fn init_persistence(backend: Arc<dyn PersistenceProtocol>) {
    let _ = PERSISTENCE.set(backend);
}

/// Get the global persistence backend. Panics if not initialized.
pub fn persistence() -> &'static Arc<dyn PersistenceProtocol> {
    PERSISTENCE.get().expect("persistence not initialized — call init_persistence() at startup")
}

// ── Error type alias ────────────────────────────────────────────

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
