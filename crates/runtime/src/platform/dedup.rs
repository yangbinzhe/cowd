//! Persistent LRU message dedup store.
//!
//! Tracks seen message IDs with TTL-based expiration and LRU eviction.
//! Mirrors Hermes' `_seen_message_ids` pattern for cross-platform
//! duplicate detection.
//!
//! ## Design
//!
//! - New entries are appended to the end of a `VecDeque`
//! - Oldest entries are evicted when capacity is reached (FIFO/LRU)
//! - Entries older than `ttl_seconds` are considered expired
//! - Optional JSON file persistence with atomic write (temp → rename)

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Current Unix timestamp in seconds.
#[inline]
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Persistent LRU message dedup store.
///
/// Tracks seen message IDs with TTL-based expiration and LRU eviction.
/// Mirrors Hermes' `_seen_message_ids` pattern.
///
/// # Example
///
/// ```ignore
/// use runtime::platform::dedup::DedupStore;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let store = DedupStore::new(10, 3600);
/// assert!(!store.is_duplicate("msg_1").await);
/// assert!(store.is_duplicate("msg_1").await); // duplicate within TTL
/// # });
/// ```
pub struct DedupStore {
    /// Message ID → seen_at (Unix timestamp seconds), ordered by insertion.
    seen_ids: RwLock<VecDeque<(String, i64)>>,
    /// Maximum number of entries before eviction.
    max_size: usize,
    /// Time-to-live in seconds.
    ttl_seconds: i64,
    /// Optional persistence file path.
    state_path: Option<PathBuf>,
    /// Whether there are unsaved changes.
    dirty: RwLock<bool>,
}

impl DedupStore {
    /// Create a new `DedupStore` without persistence.
    pub fn new(max_size: usize, ttl_seconds: i64) -> Self {
        Self {
            seen_ids: RwLock::new(VecDeque::with_capacity(max_size.min(64))),
            max_size,
            ttl_seconds,
            state_path: None,
            dirty: RwLock::new(false),
        }
    }

    /// Create a new `DedupStore` with JSON file persistence.
    ///
    /// On creation, loads existing state from `state_path` if the file
    /// exists. The file format is a JSON array of `[message_id, seen_at]`
    /// pairs.
    pub fn with_persistence(max_size: usize, ttl_seconds: i64, state_path: PathBuf) -> Self {
        let seen_ids = Self::load_from_path(&state_path, max_size);
        Self {
            seen_ids: RwLock::new(seen_ids),
            max_size,
            ttl_seconds,
            state_path: Some(state_path),
            dirty: RwLock::new(false),
        }
    }

    /// Load seen_ids from a JSON file.
    ///
    /// Returns an empty `VecDeque` if the file does not exist or cannot
    /// be parsed.
    fn load_from_path(path: &Path, max_size: usize) -> VecDeque<(String, i64)> {
        if !path.exists() {
            return VecDeque::with_capacity(max_size.min(64));
        }

        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(
                    "DedupStore: failed to open state file {}: {}",
                    path.display(),
                    e
                );
                return VecDeque::with_capacity(max_size.min(64));
            }
        };

        let pairs: Vec<(String, i64)> = match serde_json::from_reader(file) {
            Ok(pairs) => pairs,
            Err(e) => {
                tracing::warn!(
                    "DedupStore: failed to parse state file {}: {}",
                    path.display(),
                    e
                );
                return VecDeque::with_capacity(max_size.min(64));
            }
        };

        // Truncate to max_size in case the persisted file grew beyond
        // the new limit.
        let effective = pairs.len().min(max_size);
        let mut deque = VecDeque::with_capacity(max_size.min(64));
        deque.extend(pairs.into_iter().take(effective));
        deque
    }

    /// Check whether `message_id` is a duplicate (already seen within TTL).
    ///
    /// # Returns
    ///
    /// - `true` if the message ID exists and its `seen_at` is within TTL.
    /// - `false` otherwise (not seen, or seen but expired). In both
    ///   cases the entry is added (or updated) to the store.
    pub async fn is_duplicate(&self, message_id: &str) -> bool {
        let mut guard = self.seen_ids.write().await;
        let now = now_secs();

        // Check if already present.
        if let Some(pos) = guard.iter().position(|(id, _)| id == message_id) {
            let (_, seen_at) = &guard[pos];
            if now - seen_at < self.ttl_seconds {
                // Still within TTL → true duplicate.
                return true;
            }
            // Expired — remove the stale entry, then fall through to add.
            guard.remove(pos);
        }

        // Evict oldest if at capacity before adding.
        while guard.len() >= self.max_size {
            guard.pop_front();
        }

        guard.push_back((message_id.to_string(), now));

        // Mark dirty if persistence is enabled.
        if self.state_path.is_some() {
            *self.dirty.write().await = true;
        }

        false
    }

    /// Explicitly mark `message_id` as seen without checking for duplicates.
    ///
    /// This unconditionally adds (or refreshes) the entry. Useful for
    /// explicit tracking scenarios where the caller already handled
    /// dedup logic upstream.
    pub async fn mark_seen(&self, message_id: &str) {
        let mut guard = self.seen_ids.write().await;

        // Remove existing entry if present (so we can push a fresh one).
        if let Some(pos) = guard.iter().position(|(id, _)| id == message_id) {
            guard.remove(pos);
        }

        // Evict if at capacity.
        while guard.len() >= self.max_size {
            guard.pop_front();
        }

        guard.push_back((message_id.to_string(), now_secs()));

        if self.state_path.is_some() {
            *self.dirty.write().await = true;
        }
    }

    /// Persist the current state to disk.
    ///
    /// Uses atomic write (temp file → rename) to avoid corruption.
    /// Returns immediately if no `state_path` is configured or if there
    /// are no unsaved changes.
    pub async fn persist(&self) -> Result<(), io::Error> {
        let state_path = match &self.state_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        // Fast-path: skip if nothing changed.
        if !*self.dirty.read().await {
            return Ok(());
        }

        // Hold the write lock on seen_ids while we persist so the
        // snapshot is consistent.
        let guard = self.seen_ids.read().await;
        let entries: Vec<&(String, i64)> = guard.iter().collect();

        // Atomic write: temp file → rename.
        let temp_path = state_path.with_extension("tmp");
        let file = fs::File::create(&temp_path)?;
        serde_json::to_writer(file, &entries)?;
        fs::rename(&temp_path, &state_path)?;

        // Drop the read guard before acquiring the dirty write lock.
        drop(guard);
        *self.dirty.write().await = false;

        Ok(())
    }

    /// Number of entries currently in the store.
    pub async fn len(&self) -> usize {
        self.seen_ids.read().await.len()
    }

    /// Whether the store is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl Drop for DedupStore {
    fn drop(&mut self) {
        if let Some(state_path) = &self.state_path {
            // Best-effort synchronous persist during shutdown.
            if let Ok(guard) = self.seen_ids.try_write() {
                let entries: Vec<&(String, i64)> = guard.iter().collect();
                if let Ok(file) = fs::File::create(state_path) {
                    let _ = serde_json::to_writer(file, &entries);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: create a store that will be cleaned up after the test.
    fn temp_persistent_store(max_size: usize, ttl_seconds: i64) -> (DedupStore, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("dedup_state.json");
        let store = DedupStore::with_persistence(max_size, ttl_seconds, path);
        (store, dir)
    }

    #[tokio::test]
    async fn test_duplicate_detection_within_ttl() {
        let store = DedupStore::new(10, 3600);
        assert!(!store.is_duplicate("msg_1").await, "first sighting → not dup");
        assert!(store.is_duplicate("msg_1").await, "second sighting → dup");
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn test_expired_entry_not_duplicate() {
        let store = DedupStore::new(10, 0); // ttl=0 → everything expires immediately
        assert!(!store.is_duplicate("msg_1").await, "first sighting");
        assert!(!store.is_duplicate("msg_1").await, "expired → not dup, re-added");
        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn test_eviction_at_capacity() {
        let store = DedupStore::new(3, 3600);

        // Fill the store.
        assert!(!store.is_duplicate("a").await);
        assert!(!store.is_duplicate("b").await);
        assert!(!store.is_duplicate("c").await);
        assert_eq!(store.len().await, 3);

        // Insert "d" — should evict "a" (oldest).
        assert!(!store.is_duplicate("d").await);
        assert_eq!(store.len().await, 3);

        // "b" and "c" still tracked; "d" was just added.
        assert!(store.is_duplicate("b").await);
        assert!(store.is_duplicate("c").await);
        assert!(store.is_duplicate("d").await);

        // "a" was evicted — not a duplicate (but re-added by this call).
        assert!(!store.is_duplicate("a").await);
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let (store, dir) = temp_persistent_store(100, 3600);

        // Add some entries.
        store.is_duplicate("msg_a").await;
        store.is_duplicate("msg_b").await;
        store.is_duplicate("msg_c").await;

        // Persist.
        store.persist().await.expect("persist should succeed");

        // Re-load from the same file.
        let path = dir.path().join("dedup_state.json");
        let store2 = DedupStore::with_persistence(100, 3600, path);

        // Previously seen messages should now be duplicates.
        assert!(store2.is_duplicate("msg_a").await);
        assert!(store2.is_duplicate("msg_b").await);
        assert!(store2.is_duplicate("msg_c").await);

        // New message should not be a duplicate.
        assert!(!store2.is_duplicate("msg_new").await);
    }

    #[tokio::test]
    async fn test_is_duplicate_automatically_marks_seen() {
        let store = DedupStore::new(10, 3600);

        // Call is_duplicate on a new message → should mark it as seen.
        assert!(!store.is_duplicate("unique_msg").await, "first → not dup");
        assert_eq!(store.len().await, 1);

        // Now it's a duplicate.
        assert!(store.is_duplicate("unique_msg").await, "second → dup");
    }

    #[tokio::test]
    async fn test_empty_store_returns_len_zero() {
        let store = DedupStore::new(10, 3600);
        assert_eq!(store.len().await, 0);
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn test_mark_seen_adds_entry() {
        let store = DedupStore::new(10, 3600);

        store.mark_seen("explicit_msg").await;
        assert_eq!(store.len().await, 1);
        assert!(store.is_duplicate("explicit_msg").await, "should be seen now");
    }

    #[tokio::test]
    async fn test_mark_seen_evicts_oldest_at_capacity() {
        let store = DedupStore::new(2, 3600);

        store.mark_seen("a").await;
        store.mark_seen("b").await;
        assert_eq!(store.len().await, 2);

        // This should evict "a".
        store.mark_seen("c").await;
        assert_eq!(store.len().await, 2);

        // "b" and "c" are tracked.
        assert!(store.is_duplicate("b").await);
        assert!(store.is_duplicate("c").await);

        // "a" was evicted — re-checking it re-adds it (evicting "b").
        assert!(!store.is_duplicate("a").await);
    }

    #[tokio::test]
    async fn test_persist_skips_when_clean() {
        let (store, _dir) = temp_persistent_store(100, 3600);

        // Fresh store is clean — persist should be a no-op.
        store.persist().await.expect("persist on clean store");
    }

    #[tokio::test]
    async fn test_persist_without_state_path_is_noop() {
        let store = DedupStore::new(10, 3600);
        store.is_duplicate("some_msg").await;
        store.persist().await.expect("persist without path is no-op");
    }
}
