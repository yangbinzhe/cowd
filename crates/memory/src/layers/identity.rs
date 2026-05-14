//! L0 – Identity layer.
//!
//! Stores immutable, global facts about the user and their environment:
//! name, language preferences, timezone, etc.  Entries here are rarely
//! updated and always injected into the context window.
//!
//! Entries in this layer are:
//! - ~200 tokens per entry
//! - Permanently persistent (never evicted)
//! - Always loaded into every system prompt turn

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    layers::{LayerManager, Result},
    store::MemoryStore,
    types::{
        MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, PreparedContext,
        Priority, TokenBudget,
    },
};

/// Token budget for the identity layer (~200 tokens).
const IDENTITY_TOKEN_BUDGET: u64 = 200;

/// Manager for the L0 identity layer.
///
/// This layer holds a single, canonical "identity" entry describing the
/// assistant persona, hard constraints, and global user preferences.  The
/// entry is always surfaced in the context window and is never evicted.
pub struct IdentityLayer {
    store: Arc<dyn MemoryStore>,
}

impl IdentityLayer {
    /// Create a new `IdentityLayer` backed by the given store.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self { store }
    }

    /// Load all L0 identity entries from the store.
    pub async fn load(&self) -> Result<Vec<MemoryEntry>> {
        let entries = self.store.search_by_layer(MemoryLayer::L0).await?;
        Ok(entries)
    }

    /// Set (create or update) the primary identity entry.
    ///
    /// If an entry with the given title already exists it is overwritten;
    /// otherwise a new one is created.
    pub async fn set(&self, title: &str, content: &str) -> Result<MemoryId> {
        // Check for an existing entry with this title by loading all L0 entries.
        let existing = self.store.search_by_layer(MemoryLayer::L0).await?;
        if let Some(mut entry) = existing.into_iter().find(|e| e.title == title) {
            entry.content = content.to_string();
            entry.updated_at = Utc::now();
            self.store.update(&entry).await?;
            return Ok(entry.id);
        }

        // Create a new identity entry.
        let now = Utc::now();
        let entry = MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L0,
            category: MemoryCategory::UserPreference,
            priority: Priority::Critical,
            source: MemorySource::UserExplicit,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: vec!["identity".to_string()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: None,
            session_id: None,
        };
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }
}

// ---------------------------------------------------------------------------
// LayerManager implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LayerManager for IdentityLayer {
    fn layer(&self) -> MemoryLayer {
        MemoryLayer::L0
    }

    /// Insert a new entry into the identity layer.
    ///
    /// The entry's layer field is overridden to `L0` and its priority
    /// forced to `Critical`.
    async fn insert(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        entry.layer = MemoryLayer::L0;
        entry.priority = Priority::Critical;
        entry.staleness = 0.0;
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    /// Remove an identity entry.
    async fn remove(&self, id: &MemoryId) -> Result<()> {
        self.store.delete(id).await
    }

    /// Prepare context from L0: all identity entries within budget.
    async fn prepare_context(&self, budget: &TokenBudget) -> Result<PreparedContext> {
        let available = budget.available.min(IDENTITY_TOKEN_BUDGET);
        let mut entries = self.store.search_by_layer(MemoryLayer::L0).await?;

        // Sort by priority (Critical first) then by created_at.
        entries.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then(b.created_at.cmp(&a.created_at))
        });

        // Truncate to budget.
        let mut used_tokens: u64 = 0;
        let mut kept = Vec::new();
        for e in entries {
            let tokens = estimate_tokens(&e.content);
            if used_tokens + tokens > available {
                break;
            }
            used_tokens += tokens;
            kept.push(e);
        }

        Ok(PreparedContext {
            entries: kept,
            total_tokens: used_tokens,
            budget: budget.clone(),
            depth_scale: 1.0,
            prepared_at: Utc::now(),
        })
    }

    /// L0 entries are never evicted; this is a no-op.
    async fn tick(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Estimate token count from content length (4 chars ≈ 1 token).
fn estimate_tokens(content: &str) -> u64 {
    (content.len() as u64).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::sqlite::SqliteStore;

    fn in_memory() -> Arc<dyn MemoryStore> {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        Arc::new(SqliteStore::open_path(&tmp.path().join("test.db")).unwrap())
    }

    #[tokio::test]
    async fn set_creates_new_entry() {
        let layer = IdentityLayer::new(in_memory());
        let id = layer.set("Assistant", "You are helpful.").await.unwrap();
        let loaded = layer.load().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, id);
        assert_eq!(loaded[0].title, "Assistant");
        assert_eq!(loaded[0].layer, MemoryLayer::L0);
        assert_eq!(loaded[0].priority, Priority::Critical);
    }

    #[tokio::test]
    async fn set_updates_existing_entry_by_title() {
        let layer = IdentityLayer::new(in_memory());
        let id1 = layer.set("Role", "V1").await.unwrap();
        let id2 = layer.set("Role", "V2").await.unwrap();
        assert_eq!(id1, id2);
        let entries = layer.load().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "V2");
    }

    #[tokio::test]
    async fn set_multiple_distinct_titles() {
        let layer = IdentityLayer::new(in_memory());
        layer.set("Name", "Alice").await.unwrap();
        layer.set("Role", "Engineer").await.unwrap();
        layer.set("Lang", "Rust").await.unwrap();
        assert_eq!(layer.load().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn insert_overrides_layer_and_priority() {
        let layer = IdentityLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Decision,
            priority: Priority::Low,
            source: MemorySource::AutoExtracted,
            title: "test".into(),
            content: "content".into(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: None,
            session_id: None,
        };
        let id = layer.insert(entry).await.unwrap();
        let got = layer.load().await.unwrap().into_iter().find(|e| e.id == id).unwrap();
        assert_eq!(got.layer, MemoryLayer::L0);
        assert_eq!(got.priority, Priority::Critical);
    }

    #[tokio::test]
    async fn remove_deletes_entry() {
        let layer = IdentityLayer::new(in_memory());
        let id = layer.set("T", "C").await.unwrap();
        assert_eq!(layer.load().await.unwrap().len(), 1);
        layer.remove(&id).await.unwrap();
        assert_eq!(layer.load().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn prepare_context_respects_budget() {
        let layer = IdentityLayer::new(in_memory());
        layer.set("T", &"x".repeat(1000)).await.unwrap(); // ~250 tokens

        let budget = TokenBudget { total: 2000, reserved_system: 0, reserved_response: 0, allocated_memory: 0, allocated_conversation: 0, available: 200 };
        let ctx = layer.prepare_context(&budget).await.unwrap();
        // 250 tokens > 200 available, so entry should be cut
        assert!(ctx.entries.is_empty());
    }

    #[tokio::test]
    async fn prepare_context_includes_entry_within_budget() {
        let layer = IdentityLayer::new(in_memory());
        layer.set("Short", "hi").await.unwrap(); // ~1 token

        let budget = TokenBudget { total: 1000, reserved_system: 0, reserved_response: 0, allocated_memory: 0, allocated_conversation: 0, available: 200 };
        let ctx = layer.prepare_context(&budget).await.unwrap();
        assert_eq!(ctx.entries.len(), 1);
    }

    #[test]
    fn layer_returns_l0() {
        let layer = IdentityLayer::new(in_memory());
        assert_eq!(layer.layer(), MemoryLayer::L0);
    }

    #[tokio::test]
    async fn tick_is_noop() {
        let layer = IdentityLayer::new(in_memory());
        layer.set("T", "C").await.unwrap();
        layer.tick().await.unwrap();
        // Entry should still exist (L0 never prunes)
        assert_eq!(layer.load().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn load_returns_empty_when_no_entries() {
        let layer = IdentityLayer::new(in_memory());
        assert!(layer.load().await.unwrap().is_empty());
    }
}
