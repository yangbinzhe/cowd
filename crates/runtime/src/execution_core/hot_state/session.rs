use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use super::{HotResidencyRegistry, HotResidentClass, HotStateMetrics};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
        snapshot.estimated_bytes = serde_json::to_vec(&snapshot)
            .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .unwrap_or_default();
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
}
