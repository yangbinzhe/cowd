use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use super::{HotResidencyRegistry, HotResidentClass, HotStateMetrics};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HotSessionSnapshot {
    pub session_id: String,
    pub generation: u64,
    pub revision: u64,
    pub accepted_cursor: u64,
    pub durable_cursor: Option<u64>,
    pub runtime_cursor: u64,
    pub current_turn_id: Option<String>,
    pub pending_inputs: usize,
    pub inbox_refs: Vec<String>,
    pub current_execution_ids: Vec<String>,
    pub execution_graph_refs: Vec<String>,
    pub context_refs: Vec<String>,
    pub memory_refs: Vec<String>,
    pub reality_refs: Vec<String>,
    /// Immutable durable context projection for the active Session. These
    /// fields are a reconstructible hot copy, never a second write authority.
    pub context_manifest: Option<session::SessionActivationManifest>,
    pub context_cards: Vec<session::ContextIndexCard>,
    pub pending_approvals: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub estimated_bytes: u64,
}

pub struct HotSessionRegistry {
    shards: Vec<RwLock<HashMap<String, Arc<HotSessionSnapshot>>>>,
    residency: Arc<HotResidencyRegistry>,
    metrics: Arc<HotStateMetrics>,
}

impl HotSessionRegistry {
    pub(super) fn new(
        shards: usize,
        residency: Arc<HotResidencyRegistry>,
        metrics: Arc<HotStateMetrics>,
    ) -> Self {
        Self {
            shards: (0..shards).map(|_| RwLock::new(HashMap::new())).collect(),
            residency,
            metrics,
        }
    }

    #[must_use]
    pub fn get(&self, session_id: &str) -> Option<Arc<HotSessionSnapshot>> {
        let snapshot = self.shards[self.shard(session_id)]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned();
        if snapshot.is_some() {
            self.metrics.session_hit();
            self.residency.touch(&resident_id(session_id));
        } else {
            self.metrics.session_miss();
        }
        snapshot
    }

    #[must_use]
    pub fn get_many(&self, session_ids: &[String]) -> Vec<Arc<HotSessionSnapshot>> {
        session_ids
            .iter()
            .filter_map(|session_id| self.get(session_id))
            .collect()
    }

    /// Merge one domain contribution into an immutable Session snapshot.
    /// The registry owns the aggregate revision so unrelated source cursors
    /// never overwrite each other.
    pub fn update(
        &self,
        session_id: &str,
        update: impl FnOnce(&mut HotSessionSnapshot),
    ) -> Arc<HotSessionSnapshot> {
        let shard_index = self.shard(session_id);
        let mut shard = self.shards[shard_index]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut snapshot = shard
            .get(session_id)
            .map(|current| (**current).clone())
            .unwrap_or_else(|| HotSessionSnapshot {
                session_id: session_id.to_string(),
                ..HotSessionSnapshot::default()
            });
        update(&mut snapshot);
        snapshot.session_id = session_id.to_string();
        snapshot.revision = snapshot.revision.saturating_add(1);
        snapshot.estimated_bytes = estimate_snapshot_bytes(&snapshot);
        let revision = snapshot.revision;
        let estimated_bytes = snapshot.estimated_bytes;
        let pinned = snapshot.pending_inputs > 0
            || snapshot.pending_approvals > 0
            || !snapshot.current_execution_ids.is_empty();
        let published = Arc::new(snapshot);
        shard.insert(session_id.to_string(), Arc::clone(&published));
        drop(shard);
        self.residency.upsert(
            resident_id(session_id),
            HotResidentClass::Session,
            session_id.to_string(),
            estimated_bytes,
            Some(revision),
        );
        self.update_pin(session_id, pinned);
        self.evict_idle_under_pressure();
        published
    }

    /// Invalidate only the reconstructible context projection after the
    /// canonical transcript changes. Runtime/session lifecycle state remains
    /// resident while the background indexer publishes a new generation.
    pub fn invalidate_context(&self, session_id: &str) -> Arc<HotSessionSnapshot> {
        self.update(session_id, |snapshot| {
            snapshot.context_manifest = None;
            snapshot.context_cards.clear();
            snapshot
                .context_refs
                .retain(|reference| !reference.starts_with("session-context:"));
        })
    }

    pub fn remove(&self, session_id: &str) {
        self.shards[self.shard(session_id)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.residency.remove(&resident_id(session_id));
    }

    fn shard(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (self.shards.len() - 1)
    }

    fn update_pin(&self, session_id: &str, pinned: bool) {
        if pinned {
            self.residency
                .pin(&resident_id(session_id), "active_session");
        } else {
            self.residency
                .unpin(&resident_id(session_id), "active_session");
        }
    }

    fn evict_idle_under_pressure(&self) {
        if !self.residency.pressure_high() {
            return;
        }
        for candidate in self
            .residency
            .eviction_candidates(HotResidentClass::Session)
        {
            if self.residency.resident_bytes() <= self.residency.target_low_watermark() {
                break;
            }
            if candidate.reconstruct_cursor.is_none() {
                continue;
            }
            let session_id = candidate.owner_id;
            let shard_index = self.shard(&session_id);
            let mut shard = self.shards[shard_index]
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let removable = shard.get(&session_id).is_some_and(|snapshot| {
                snapshot.pending_inputs == 0
                    && snapshot.pending_approvals == 0
                    && snapshot.current_execution_ids.is_empty()
            });
            if removable {
                shard.remove(&session_id);
                drop(shard);
                self.residency.remove(&resident_id(&session_id));
            }
        }
    }
}

fn resident_id(session_id: &str) -> String {
    format!("session:{session_id}")
}

fn estimate_snapshot_bytes(snapshot: &HotSessionSnapshot) -> u64 {
    fn strings_bytes(values: &[String]) -> usize {
        values.iter().map(String::len).sum()
    }

    let mut bytes = std::mem::size_of::<HotSessionSnapshot>()
        .saturating_add(snapshot.session_id.len())
        .saturating_add(snapshot.current_turn_id.as_ref().map_or(0, String::len))
        .saturating_add(strings_bytes(&snapshot.inbox_refs))
        .saturating_add(strings_bytes(&snapshot.current_execution_ids))
        .saturating_add(strings_bytes(&snapshot.execution_graph_refs))
        .saturating_add(strings_bytes(&snapshot.context_refs))
        .saturating_add(strings_bytes(&snapshot.memory_refs))
        .saturating_add(strings_bytes(&snapshot.reality_refs));
    if let Some(manifest) = &snapshot.context_manifest {
        bytes = bytes
            .saturating_add(std::mem::size_of::<session::SessionActivationManifest>())
            .saturating_add(manifest.recovery.session_id.len())
            .saturating_add(
                manifest
                    .recovery
                    .latest_checkpoint_event_id
                    .as_ref()
                    .map_or(0, String::len),
            );
    }
    for card in &snapshot.context_cards {
        bytes = bytes
            .saturating_add(std::mem::size_of::<session::ContextIndexCard>())
            .saturating_add(card.card_id.len())
            .saturating_add(card.parent_card_id.as_ref().map_or(0, String::len))
            .saturating_add(card.session_id.len())
            .saturating_add(card.source_digest.len())
            .saturating_add(card.summary.len())
            .saturating_add(card.scope.len())
            .saturating_add(card.authority.len());
    }
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_core::hot_state::{
        HotMemoryBudget, HotStateMemoryConfig, HotStateMetrics,
    };
    use std::sync::RwLock as StdRwLock;

    fn registry() -> HotSessionRegistry {
        let metrics = Arc::new(HotStateMetrics::default());
        let budget = Arc::new(StdRwLock::new(HotMemoryBudget::resolve(
            &HotStateMemoryConfig::default(),
        )));
        let residency = Arc::new(HotResidencyRegistry::new(budget, metrics.clone()));
        HotSessionRegistry::new(4, residency, metrics)
    }

    #[test]
    fn independent_domain_updates_preserve_one_aggregate_snapshot() {
        let registry = registry();
        registry.update("session-a", |snapshot| {
            snapshot.accepted_cursor = 4;
            snapshot.pending_inputs = 2;
        });
        registry.update("session-a", |snapshot| {
            snapshot.current_execution_ids = vec!["execution-a".to_string()];
            snapshot.context_refs = vec!["context:a".to_string()];
        });

        let snapshot = registry.get("session-a").expect("hot session");
        assert_eq!(snapshot.accepted_cursor, 4);
        assert_eq!(snapshot.pending_inputs, 2);
        assert_eq!(snapshot.current_execution_ids, vec!["execution-a"]);
        assert_eq!(snapshot.context_refs, vec!["context:a"]);
        assert_eq!(snapshot.revision, 2);
    }

    #[test]
    fn active_snapshot_is_pinned_until_work_drains() {
        let registry = registry();
        registry.update("session-a", |snapshot| {
            snapshot.pending_inputs = 1;
            snapshot.accepted_cursor = 1;
        });
        let resident = registry
            .residency
            .snapshot(&resident_id("session-a"))
            .expect("resident Session");
        assert_eq!(resident.pin_reasons, vec!["active_session"]);

        registry.update("session-a", |snapshot| {
            snapshot.pending_inputs = 0;
            snapshot.runtime_cursor = 1;
        });
        let resident = registry
            .residency
            .snapshot(&resident_id("session-a"))
            .expect("resident Session");
        assert!(resident.pin_reasons.is_empty());
        assert_eq!(resident.reconstruct_cursor, Some(2));
    }

    #[test]
    fn batch_read_preserves_requested_order_and_skips_cold_sessions() {
        let registry = registry();
        registry.update("session-b", |_| {});
        registry.update("session-a", |_| {});

        let snapshots = registry.get_many(&[
            "session-a".to_string(),
            "missing".to_string(),
            "session-b".to_string(),
        ]);
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["session-a", "session-b"]
        );
    }

    #[test]
    fn context_invalidation_preserves_runtime_state_and_removes_stale_generation() {
        let registry = registry();
        registry.update("session-a", |snapshot| {
            snapshot.pending_inputs = 1;
            snapshot.current_execution_ids = vec!["execution-a".to_string()];
            snapshot.context_refs = vec![
                "session-context:session-a:7".to_string(),
                "memory:stable".to_string(),
            ];
            snapshot.context_cards.push(session::ContextIndexCard {
                schema_version: session::CONTEXT_INDEX_CARD_SCHEMA_VERSION,
                card_id: "card-a".to_string(),
                parent_card_id: None,
                session_id: "session-a".to_string(),
                source_start_sequence: 0,
                source_end_sequence: 1,
                source_message_count: 1,
                source_digest: "digest".to_string(),
                summary: "stale".to_string(),
                scope: "session".to_string(),
                authority: "session".to_string(),
                generation: 7,
                created_at_ms: 1,
                updated_at_ms: 1,
            });
        });

        let invalidated = registry.invalidate_context("session-a");

        assert_eq!(invalidated.pending_inputs, 1);
        assert_eq!(invalidated.current_execution_ids, vec!["execution-a"]);
        assert!(invalidated.context_manifest.is_none());
        assert!(invalidated.context_cards.is_empty());
        assert_eq!(invalidated.context_refs, vec!["memory:stable"]);
    }
}
