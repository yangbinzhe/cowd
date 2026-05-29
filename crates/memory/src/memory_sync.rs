//! P8.4 — MemorySyncProtocol for cross-agent L4 memory synchronisation.
//!
//! The `MemorySyncProtocol` bridges agent-private L3 (deep) and team-shared L4
//! (shared) layers.  It pushes tagged L3 entries to L4 so peer agents can
//! discover them, and pulls relevant L4 entries back into an agent's private
//! L3 store.
//!
//! # Real-time subscription
//!
//! Callers subscribe to the underlying [`L4EventBus`] via [`subscribe`] to
//! receive push notifications whenever *any* agent writes to L4.  This avoids
//! polling and enables immediate cross-agent awareness.
//!
//! # Oracle-corrected API
//!
//! - Uses [`MemoryOrchestrator::team_query`] + tag filtering (not a
//!   non-existent `search_by_tags`).
//! - Uses [`MemoryOrchestrator::remember`] (not a non-existent `insert_entry`).
//! - Subscribes to the orchestrator's existing [`L4EventBus`] — no new channel
//!   is created.
//! - [`L4Event::operation`] is the [`L4Operation`] enum; `timestamp_ms` is
//!   `u64`.

use std::sync::Arc;

use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    layers::shared::{L4Event, L4EventBus},
    orchestrator::MemoryOrchestrator,
    types::{AgentVisibility, MemoryEntry, MemoryLayer, MemorySource},
};

/// Default maximum number of L4 entries returned by a single import.
const DEFAULT_IMPORT_LIMIT: usize = 20;

// ---------------------------------------------------------------------------
// MemorySyncProtocol
// ---------------------------------------------------------------------------

/// Cross-agent synchronisation protocol for L3 ↔ L4 memory layers.
///
/// # Typical workflow
///
/// 1. Agent writes important decisions to its L3 store tagged `"team_relevant"`.
/// 2. Agent calls [`sync_to_l4`] to push those entries to the shared L4 layer.
/// 3. Peer agent calls [`import_from_l4`] on a topic to pull relevant L4
///    knowledge into its own L3 store.
/// 4. All agents subscribe via [`subscribe`] for real-time push notifications.
pub struct MemorySyncProtocol {
    orchestrator: Arc<MemoryOrchestrator>,
    event_bus: Arc<L4EventBus>,
}

impl MemorySyncProtocol {
    /// Create a new sync protocol backed by the given orchestrator.
    ///
    /// The `event_bus` should be the same bus that the orchestrator's
    /// [`SharedLayer`] publishes to — typically retrieved via
    /// [`MemoryOrchestrator::l4_event_bus`].
    pub fn new(orchestrator: Arc<MemoryOrchestrator>, event_bus: Arc<L4EventBus>) -> Self {
        Self {
            orchestrator,
            event_bus,
        }
    }

    /// Push the given agent's L3 entries tagged `"team_relevant"` to L4.
    ///
    /// For each *existing* L3 entry id in `entry_ids`, this method:
    ///
    /// 1. Fetches the entry from the store.
    /// 2. Checks that the entry's tags contain `"team_relevant"`.
    /// 3. Creates an L4 copy with `visibility = Shared` and calls
    ///    [`MemoryOrchestrator::remember`].
    ///
    /// Existing L3 entries are **not** modified — only new L4 copies are
    /// created.  The `agent_id` is stamped into the L4 copy's `source_agent`
    /// field.
    ///
    /// # Return
    ///
    /// The number of entries that were successfully synchronised to L4.
    ///
    /// # Errors
    ///
    /// Propagates store errors from the underlying orchestrator.
    pub async fn sync_to_l4(
        &self,
        agent_id: &str,
        entry_ids: &[String],
    ) -> crate::orchestrator::Result<usize> {
        let mut synced = 0usize;

        for raw_id in entry_ids {
            // Parse the string id into a UUID.
            let id = match Uuid::parse_str(raw_id) {
                Ok(id) => id,
                Err(_) => continue,
            };

            // Fetch the L3 entry.
            let Some(entry) = self.orchestrator.recall(&id).await? else {
                continue;
            };

            // Only sync entries explicitly tagged for team sharing.
            if !entry.tags.iter().any(|t| t == "team_relevant") {
                continue;
            }

            // Build the L4 copy.
            let l4 = MemoryEntry {
                id: Uuid::new_v4(),
                layer: MemoryLayer::L4,
                category: entry.category,
                priority: entry.priority,
                source: MemorySource::Import,
                title: entry.title.clone(),
                content: entry.content.clone(),
                embedding: entry.embedding.clone(),
                tags: entry.tags.clone(),
                relations: entry.relations.clone(),
                confidence: entry.confidence,
                access_count: 0,
                staleness: 0.0,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                last_accessed_at: None,
                scope: entry.scope.clone(),
                session_id: entry.session_id.clone(),
                source_agent: Some(agent_id.to_string()),
                visibility: AgentVisibility::Shared,
            };

            self.orchestrator.remember(l4).await?;
            synced += 1;
        }

        Ok(synced)
    }

    /// Import relevant L4 entries into the agent's L3 store.
    ///
    /// Queries the shared L4 layer via [`MemoryOrchestrator::team_query`] for
    /// entries matching `topic`, then creates L3 copies owned by `agent_id`.
    ///
    /// # Return
    ///
    /// The list of freshly imported L3 entries.
    ///
    /// # Errors
    ///
    /// Propagates store errors from the underlying orchestrator.
    pub async fn import_from_l4(
        &self,
        topic: &str,
        agent_id: &str,
    ) -> crate::orchestrator::Result<Vec<MemoryEntry>> {
        // Query L4 for topic-relevant shared entries.
        let l4_entries = self
            .orchestrator
            .team_query(topic, None, DEFAULT_IMPORT_LIMIT)
            .await?;

        let mut imported = Vec::with_capacity(l4_entries.len());

        for l4 in l4_entries {
            // Build an L3 copy that the agent owns.
            let l3 = MemoryEntry {
                id: Uuid::new_v4(),
                layer: MemoryLayer::L3,
                category: l4.category,
                priority: l4.priority,
                source: MemorySource::Import,
                title: l4.title,
                content: l4.content,
                embedding: l4.embedding,
                tags: l4.tags,
                relations: l4.relations,
                confidence: l4.confidence,
                access_count: 0,
                staleness: 0.0,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                last_accessed_at: None,
                scope: l4.scope,
                session_id: l4.session_id,
                source_agent: Some(agent_id.to_string()),
                visibility: AgentVisibility::Private,
            };

            self.orchestrator.remember(l3.clone()).await?;
            imported.push(l3);
        }

        Ok(imported)
    }

    /// Subscribe to real-time L4 change notifications.
    ///
    /// Returns a [`broadcast::Receiver`] that yields [`L4Event`]s whenever
    /// *any* agent inserts, updates, or deletes an L4 entry.  The caller
    /// should drain the receiver in its event loop via
    /// [`broadcast::Receiver::try_recv`].
    ///
    /// The receiver taps the same [`L4EventBus`] that the orchestrator's
    /// [`SharedLayer`] publishes to — no extra channel is created.
    pub fn subscribe(&self) -> broadcast::Receiver<L4Event> {
        self.event_bus.subscribe()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MemoryConfig;
    use crate::layers::shared::L4Operation;
    use crate::store::sqlite::SqliteStore;
    use crate::types::{MemoryCategory, Priority};
    use crate::project_scope::MemoryScope;

    fn in_memory_store() -> Arc<dyn crate::store::MemoryStore> {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        Arc::new(SqliteStore::open_path(&tmp.path().join("test.db")).unwrap())
    }

    async fn build_protocol() -> MemorySyncProtocol {
        let store = in_memory_store();
        let orch = Arc::new(
            MemoryOrchestrator::from_store(MemoryConfig::default(), store, None).unwrap(),
        );
        let bus = orch.l4_event_bus().cloned().unwrap();
        MemorySyncProtocol::new(orch, bus)
    }

    #[tokio::test]
    async fn sync_to_l4_pushes_tagged_entries() {
        let proto = build_protocol().await;

        // Write an L3 entry tagged "team_relevant".
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Decision,
            priority: Priority::High,
            source: MemorySource::AutoExtracted,
            title: "shared-decision".into(),
            content: "Team should use Rust".into(),
            embedding: None,
            tags: vec!["team_relevant".into()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: Some("agent-a".into()),
            visibility: AgentVisibility::Private,
        };
        let id = proto.orchestrator.remember(entry).await.unwrap();

        let count = proto
            .sync_to_l4("agent-a", &[id.to_string()])
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn sync_to_l4_skips_non_team_relevant() {
        let proto = build_protocol().await;

        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Decision,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "private-note".into(),
            content: "nothing to share".into(),
            embedding: None,
            tags: vec!["private".into()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: Some("agent-a".into()),
            visibility: AgentVisibility::Private,
        };
        let id = proto.orchestrator.remember(entry).await.unwrap();

        let count = proto
            .sync_to_l4("agent-a", &[id.to_string()])
            .await
            .unwrap();
        assert_eq!(count, 0, "should skip entries without team_relevant tag");
    }

    #[tokio::test]
    async fn import_from_l4_returns_entries() {
        let proto = build_protocol().await;

        // Seed an L4 entry first via team_remember.
        proto
            .orchestrator
            .team_remember(
                "import-test",
                "shared content for import",
                Priority::Normal,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();

        let imported = proto
            .import_from_l4("import-test", "agent-b")
            .await
            .unwrap();
        assert!(!imported.is_empty());
        for entry in &imported {
            assert_eq!(entry.layer, MemoryLayer::L3);
            assert_eq!(entry.source_agent.as_deref(), Some("agent-b"));
        }
    }

    #[tokio::test]
    async fn subscribe_yields_events_on_l4_write() {
        let proto = build_protocol().await;
        let mut rx = proto.subscribe();

        // Write to L4 and verify the event bus fires.
        proto
            .orchestrator
            .team_remember(
                "subscription-test",
                "test content",
                Priority::Normal,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_millis(200), async {
            loop {
                match rx.try_recv() {
                    Ok(ev) => return ev,
                    Err(broadcast::error::TryRecvError::Empty) => {
                        tokio::task::yield_now().await;
                    }
                    Err(_) => panic!("receiver closed"),
                }
            }
        })
        .await
        .expect("should receive event within timeout");

        assert_eq!(event.operation, L4Operation::Insert);
    }

    #[tokio::test]
    async fn sync_to_l4_ignores_invalid_uuids() {
        let proto = build_protocol().await;
        let count = proto
            .sync_to_l4("agent-c", &["not-a-uuid".into()])
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sync_to_l4_ignores_missing_entries() {
        let proto = build_protocol().await;
        let fake = uuid::Uuid::new_v4().to_string();
        let count = proto.sync_to_l4("agent-d", &[fake]).await.unwrap();
        assert_eq!(count, 0);
    }
}
