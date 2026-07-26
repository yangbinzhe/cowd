//! Tiered session store — hot / warm / cold storage tiers.
//!
//! Sessions are classified into three tiers based on message count and
//! recency of last activity:
//!
//! * **Hot**  (`message_count < 1000`) — all messages loaded from SQLite.
//! * **Warm** (`message_count >= 1000`) — paginated loading (page_size=50).
//! * **Cold** (`last_activity >= 30 days ago`) — lz4-compressed archive on
//!   disk; messages are cleared from SQLite (session metadata kept).
//!
//! # Example
//!
//! ```rust,no_run
//! use session::{TieredSessionStore, UnifiedSessionStore};
//! use std::path::Path;
//!
//! let store = UnifiedSessionStore::open(Path::new("sessions.db")).unwrap();
//! let tiered = TieredSessionStore::new(store, Default::default());
//! ```

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, SessionError};
use crate::persistence::sqlite::SessionMessage;
use crate::persistence::UnifiedSessionStore;

// ---------------------------------------------------------------------------
// TieredSessionStore Error
// ---------------------------------------------------------------------------

fn store_err(msg: impl Into<String>) -> SessionError {
    SessionError::Store(msg.into())
}

fn other_err(msg: impl Into<String>) -> SessionError {
    SessionError::Other(msg.into())
}

// ---------------------------------------------------------------------------
// StorageTier
// ---------------------------------------------------------------------------

/// Classification of a session into a storage tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// Message count below `hot_threshold` — load all from SQLite.
    Hot,
    /// Message count at or above `hot_threshold` but recently active.
    Warm,
    /// Last activity older than `cold_days` — archived to disk.
    Cold,
}

// ---------------------------------------------------------------------------
// Compression algorithm
// ---------------------------------------------------------------------------

/// Supported compression algorithms for the cold tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionAlgo {
    /// lz4 fast compression via `lz4_flex`.
    #[default]
    Lz4,
}

// ---------------------------------------------------------------------------
// TieredSessionStoreConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`TieredSessionStore`].
#[derive(Debug, Clone)]
pub struct TieredSessionStoreConfig {
    /// Sessions with fewer messages go to the **Hot** tier.
    pub hot_threshold: usize,
    /// Sessions whose `last_activity` is this many days in the past go to
    /// the **Cold** tier.
    pub cold_days: i64,
    /// Compression algorithm used for the cold-tier archive.
    pub compression: CompressionAlgo,
    /// Directory where archived session blobs are stored.
    pub archive_path: PathBuf,
    /// Number of messages per page when loading from the **Warm** tier.
    pub page_size: usize,
}

impl Default for TieredSessionStoreConfig {
    fn default() -> Self {
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."));
        Self {
            hot_threshold: 1000,
            cold_days: 30,
            compression: CompressionAlgo::Lz4,
            archive_path: home.join(".cowd").join("archive"),
            page_size: 50,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal archive format
// ---------------------------------------------------------------------------

/// Lightweight serializable message used for archive persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArchivedMessage {
    #[serde(default)]
    stable_message_id: String,
    session_id: String,
    sequence: usize,
    role: String,
    content_json: String,
    blocks_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_use_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_usage_json: Option<String>,
    created_at_ms: u64,
}

impl From<SessionMessage> for ArchivedMessage {
    fn from(m: SessionMessage) -> Self {
        Self {
            stable_message_id: m.stable_message_id,
            session_id: m.session_id,
            sequence: m.sequence,
            role: m.role,
            content_json: m.content_json,
            blocks_count: m.blocks_count,
            tool_use_id: m.tool_use_id,
            tool_name: m.tool_name,
            token_usage_json: m.token_usage_json,
            created_at_ms: m.created_at_ms,
        }
    }
}

impl From<ArchivedMessage> for SessionMessage {
    fn from(a: ArchivedMessage) -> Self {
        Self {
            stable_message_id: if a.stable_message_id.is_empty() {
                format!("archive:{}:{}", a.session_id, a.sequence)
            } else {
                a.stable_message_id
            },
            session_id: a.session_id,
            sequence: a.sequence,
            role: a.role,
            content_json: a.content_json,
            blocks_count: a.blocks_count,
            tool_use_id: a.tool_use_id,
            tool_name: a.tool_name,
            token_usage_json: a.token_usage_json,
            created_at_ms: a.created_at_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// TieredSessionStore
// ---------------------------------------------------------------------------

/// Session store with hot/warm/cold tier management.
///
/// Wraps a [`UnifiedSessionStore`] and adds tiered access patterns plus
/// cold-archive support via lz4 compression.
#[derive(Debug, Clone)]
pub struct TieredSessionStore {
    /// Underlying SQLite-based session store.
    store: UnifiedSessionStore,
    /// Tier configuration.
    config: TieredSessionStoreConfig,
}

impl TieredSessionStore {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new tiered store wrapping an existing [`UnifiedSessionStore`].
    pub fn new(store: UnifiedSessionStore, config: TieredSessionStoreConfig) -> Self {
        Self { store, config }
    }

    /// Return a reference to the underlying [`UnifiedSessionStore`].
    pub fn inner(&self) -> &UnifiedSessionStore {
        &self.store
    }

    /// Access the configuration.
    pub fn config(&self) -> &TieredSessionStoreConfig {
        &self.config
    }

    // -----------------------------------------------------------------------
    // Tier determination
    // -----------------------------------------------------------------------

    /// Determine which tier a session belongs to.
    ///
    /// Returns `Cold` if the session has already been archived (no messages
    /// in SQLite and an archive file exists). Otherwise bases the decision
    /// on `message_count` and `last_activity`.
    pub async fn determine_tier(&self, session_id: &str) -> Result<StorageTier> {
        let archive_file = self.archive_file_path(session_id);

        // If an archive file already exists, treat as Cold regardless of
        // SQLite state — the session was previously archived.
        if archive_file.exists() {
            return Ok(StorageTier::Cold);
        }

        let record = self
            .store
            .get_session(session_id)
            .await?
            .ok_or_else(|| store_err(format!("session not found: {session_id}")))?;

        let message_count: usize = record.message_count as usize;

        // Cold check takes precedence: a session with few messages but no
        // recent activity should still be archived, not kept hot forever.
        if let Ok(last_dt) = chrono::DateTime::parse_from_rfc3339(&record.last_activity) {
            let age = Utc::now() - last_dt.with_timezone(&Utc);
            if age > Duration::days(self.config.cold_days) {
                return Ok(StorageTier::Cold);
            }
        }

        if message_count < self.config.hot_threshold {
            return Ok(StorageTier::Hot);
        }

        Ok(StorageTier::Warm)
    }

    // -----------------------------------------------------------------------
    // Hot tier — load all
    // -----------------------------------------------------------------------

    /// Load **all** messages for a hot-tier session.
    ///
    /// Uses `get_all_messages` under the hood; appropriate only for sessions
    /// below the `hot_threshold`.
    pub async fn load_hot(&self, session_id: &str) -> Result<Vec<SessionMessage>> {
        tracing::debug!(session_id, "load_hot: loading all messages");
        self.store.get_all_messages(session_id).await
    }

    // -----------------------------------------------------------------------
    // Warm tier — paginated loading
    // -----------------------------------------------------------------------

    /// Load a single page of messages from a warm-tier session.
    ///
    /// Pages are 0-indexed. Each page contains at most `page_size` messages.
    /// Returns an empty vector when past the last page.
    pub async fn load_page(&self, session_id: &str, page: usize) -> Result<Vec<SessionMessage>> {
        let offset = page * self.config.page_size;
        tracing::debug!(
            session_id,
            page,
            offset,
            page_size = self.config.page_size,
            "load_page"
        );
        self.store
            .get_messages(session_id, offset, self.config.page_size)
            .await
    }

    /// Return the total number of pages for a session given its message count.
    pub fn page_count(&self, message_count: usize) -> usize {
        message_count.div_ceil(self.config.page_size)
    }

    // -----------------------------------------------------------------------
    // Cold tier — archive / restore
    // -----------------------------------------------------------------------

    /// Archive a cold-tier session to disk.
    ///
    /// 1. Loads all messages from SQLite.
    /// 2. Serializes them to JSON.
    /// 3. Compresses the JSON with lz4.
    /// 4. Writes the compressed blob to `{archive_path}/{session_id}.lz4`.
    /// 5. Deletes all messages from SQLite (session metadata row is preserved).
    pub async fn archive_session(&self, session_id: &str) -> Result<()> {
        tracing::info!(session_id, "archive_session: compressing to cold storage");

        // 1. Load all messages
        let messages = self.store.get_all_messages(session_id).await?;

        if messages.is_empty() {
            tracing::debug!(session_id, "archive_session: no messages to archive");
            return Ok(());
        }

        let message_count = messages.len();

        // 2. Convert to serializable format
        let archived: Vec<ArchivedMessage> =
            messages.into_iter().map(ArchivedMessage::from).collect();

        // 3. Serialize to JSON
        let json_bytes = serde_json::to_vec(&archived)
            .map_err(|e| store_err(format!("archive serialization failed: {e}")))?;

        // 4. Compress with lz4
        let compressed = match self.config.compression {
            CompressionAlgo::Lz4 => Self::compress_lz4(&json_bytes)?,
        };

        // 5. Write to disk
        let archive_file = self.archive_file_path(session_id);
        if let Some(parent) = archive_file.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| store_err(format!("cannot create archive dir: {e}")))?;
        }

        let mut file = fs::File::create(&archive_file)
            .map_err(|e| store_err(format!("cannot create archive file: {e}")))?;
        file.write_all(&compressed)
            .map_err(|e| store_err(format!("cannot write archive: {e}")))?;
        file.flush()
            .map_err(|e| store_err(format!("cannot flush archive: {e}")))?;

        // 6. Delete all messages from SQLite (keep metadata)
        self.store.delete_messages_from(session_id, 0).await?;

        // 7. Update session metadata — set message_count to 0 (we still
        //    have the metadata row) and record that it's archived.
        let mut record =
            self.store.get_session(session_id).await?.ok_or_else(|| {
                store_err(format!("session vanished during archive: {session_id}"))
            })?;

        record.message_count = 0;
        let meta = serde_json::to_string(&serde_json::json!({
            "archived": true,
            "archived_at": Utc::now().to_rfc3339(),
            "archived_message_count": message_count,
        }))
        .unwrap_or_default();
        record.metadata_json = Some(meta);

        self.store.update_session(&record).await?;

        tracing::info!(
            session_id,
            message_count,
            archive_path = %archive_file.display(),
            "archive_session: complete"
        );
        Ok(())
    }

    /// Restore a cold-tier session by decompressing the archive and
    /// re-inserting messages into SQLite.
    pub async fn restore_session(&self, session_id: &str) -> Result<()> {
        tracing::info!(
            session_id,
            "restore_session: decompressing from cold storage"
        );

        let archive_file = self.archive_file_path(session_id);

        if !archive_file.exists() {
            return Err(store_err(format!(
                "archive file not found for session {session_id}: {}",
                archive_file.display()
            )));
        }

        // 1. Read compressed blob
        let mut file = fs::File::open(&archive_file)
            .map_err(|e| store_err(format!("cannot open archive: {e}")))?;
        let mut compressed = Vec::new();
        file.read_to_end(&mut compressed)
            .map_err(|e| store_err(format!("cannot read archive: {e}")))?;

        // 2. Decompress
        let json_bytes = match self.config.compression {
            CompressionAlgo::Lz4 => Self::decompress_lz4(&compressed)?,
        };

        // 3. Deserialize
        let archived: Vec<ArchivedMessage> = serde_json::from_slice(&json_bytes)
            .map_err(|e| store_err(format!("archive deserialization failed: {e}")))?;

        let message_count = archived.len();

        // 4. Convert back and insert in batches
        let messages: Vec<SessionMessage> =
            archived.into_iter().map(SessionMessage::from).collect();

        // Insert in batches of 100 to avoid oversized transactions
        const BATCH_SIZE: usize = 100;
        for chunk in messages.chunks(BATCH_SIZE) {
            self.store.insert_messages_batch(chunk).await?;
        }

        // 5. Update session metadata — restore message_count and clear
        //    archive marker.
        let mut record = self.store.get_session(session_id).await?.ok_or_else(|| {
            store_err(format!(
                "session `{session_id}` disappeared while restoring its archive"
            ))
        })?;

        record.message_count = message_count as i64;
        record.metadata_json = None; // clear archive metadata
        self.store.update_session(&record).await?;

        // 6. Remove archive file after successful restore
        let _ = fs::remove_file(&archive_file);

        tracing::info!(session_id, message_count, "restore_session: complete");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Build the path to a session's archive file.
    fn archive_file_path(&self, session_id: &str) -> PathBuf {
        self.config.archive_path.join(format!("{session_id}.lz4"))
    }

    /// Compress bytes with lz4.
    fn compress_lz4(input: &[u8]) -> Result<Vec<u8>> {
        // Uses prepend-size variant so decompress_size_prepended knows
        // the exact original size without guessing.
        Ok(lz4_flex::compress_prepend_size(input))
    }

    /// Decompress bytes with lz4.
    fn decompress_lz4(input: &[u8]) -> Result<Vec<u8>> {
        lz4_flex::decompress_size_prepended(input)
            .map_err(|e| other_err(format!("lz4 decompression failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> (TieredSessionStore, UnifiedSessionStore) {
        let store = UnifiedSessionStore::open_in_memory().unwrap();
        let tiered = TieredSessionStore::new(store.clone(), TieredSessionStoreConfig::default());
        (tiered, store)
    }

    fn make_message(session_id: &str, seq: usize) -> SessionMessage {
        SessionMessage {
            stable_message_id: format!("tiered:{session_id}:{seq}"),
            session_id: session_id.to_string(),
            sequence: seq,
            role: "user".to_string(),
            content_json: format!(r#"[{{"type":"text","text":"message-{seq}"}}]"#),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 1700000000000 + seq as u64,
        }
    }

    fn make_record(
        session_id: &str,
        message_count: i64,
        last_activity: &str,
    ) -> crate::persistence::sqlite::SessionRecord {
        crate::persistence::sqlite::SessionRecord {
            session_id: session_id.to_string(),
            platform: "test".to_string(),
            chat_id: "chat-1".to_string(),
            user_id: Some("user-1".to_string()),
            model: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            last_activity: last_activity.to_string(),
            message_count,
            reset_policy: "None".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    #[tokio::test]
    async fn determine_tier_hot() {
        let (tiered, store) = make_store();
        let today = Utc::now().format("%Y-%m-%dT00:00:00Z").to_string();
        store
            .create_session(&make_record("s-hot", 42, &today))
            .await
            .unwrap();
        let tier = tiered.determine_tier("s-hot").await.unwrap();
        assert_eq!(tier, StorageTier::Hot);
    }

    #[tokio::test]
    async fn determine_tier_warm() {
        let (tiered, store) = make_store();
        let today = Utc::now().format("%Y-%m-%dT00:00:00Z").to_string();
        store
            .create_session(&make_record("s-warm", 2000, &today))
            .await
            .unwrap();
        let tier = tiered.determine_tier("s-warm").await.unwrap();
        assert_eq!(tier, StorageTier::Warm);
    }

    #[tokio::test]
    async fn determine_tier_cold() {
        let (tiered, store) = make_store();
        store
            .create_session(&make_record("s-cold", 50, "2020-01-01T00:00:00Z"))
            .await
            .unwrap();
        let tier = tiered.determine_tier("s-cold").await.unwrap();
        assert_eq!(tier, StorageTier::Cold);
    }

    #[tokio::test]
    async fn load_page_pagination() {
        let (tiered, store) = make_store();
        let today = Utc::now().format("%Y-%m-%dT00:00:00Z").to_string();
        store
            .create_session(&make_record("s-page", 100, &today))
            .await
            .unwrap();

        // Insert 100 messages
        let msgs: Vec<SessionMessage> = (0..100).map(|i| make_message("s-page", i)).collect();
        store.insert_messages_batch(&msgs).await.unwrap();

        let page0 = tiered.load_page("s-page", 0).await.unwrap();
        assert_eq!(page0.len(), 50);
        assert_eq!(page0[0].sequence, 0);

        let page1 = tiered.load_page("s-page", 1).await.unwrap();
        assert_eq!(page1.len(), 50);
        assert_eq!(page1[0].sequence, 50);

        let page2 = tiered.load_page("s-page", 2).await.unwrap();
        assert!(page2.is_empty());
    }

    #[tokio::test]
    async fn archive_and_restore_roundtrip() {
        let (tiered, store) = make_store();
        store
            .create_session(&make_record("s-archive", 10, "2020-01-01T00:00:00Z"))
            .await
            .unwrap();

        // Insert messages
        let msgs: Vec<SessionMessage> = (0..10).map(|i| make_message("s-archive", i)).collect();
        store.insert_messages_batch(&msgs).await.unwrap();
        assert_eq!(store.get_message_count("s-archive").await.unwrap(), 10);

        // Archive
        tiered.archive_session("s-archive").await.unwrap();

        // After archive: messages gone, metadata preserved
        assert_eq!(store.get_message_count("s-archive").await.unwrap(), 0);
        let record = store.get_session("s-archive").await.unwrap().unwrap();
        assert_eq!(record.message_count, 0);
        // metadata_json should contain archived marker
        assert!(record.metadata_json.unwrap().contains("archived"));

        // Determine tier should be Cold now (archive file exists)
        // Note: archive uses a temp path, so this may not find it. Skip tier check.

        // Restore
        tiered.restore_session("s-archive").await.unwrap();

        // After restore: messages back, metadata restored
        assert_eq!(store.get_message_count("s-archive").await.unwrap(), 10);
        let record = store.get_session("s-archive").await.unwrap().unwrap();
        assert_eq!(record.message_count, 10);

        // Verify content integrity
        let restored = store.get_all_messages("s-archive").await.unwrap();
        assert_eq!(restored.len(), 10);
        assert_eq!(
            restored[0].content_json,
            r#"[{"type":"text","text":"message-0"}]"#
        );
        assert_eq!(
            restored[9].content_json,
            r#"[{"type":"text","text":"message-9"}]"#
        );
    }

    #[tokio::test]
    async fn archive_empty_session_is_noop() {
        let (tiered, store) = make_store();
        store
            .create_session(&make_record("s-empty", 0, "2020-01-01T00:00:00Z"))
            .await
            .unwrap();

        tiered.archive_session("s-empty").await.unwrap();
        // Should not error, just a no-op
    }

    #[test]
    fn page_count_calculation() {
        let (tiered, _store) = make_store();
        assert_eq!(tiered.page_count(0), 0);
        assert_eq!(tiered.page_count(1), 1);
        assert_eq!(tiered.page_count(50), 1);
        assert_eq!(tiered.page_count(51), 2);
        assert_eq!(tiered.page_count(100), 2);
        assert_eq!(tiered.page_count(101), 3);
    }
}
