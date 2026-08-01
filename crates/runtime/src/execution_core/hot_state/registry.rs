use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{HotMemoryBudget, HotStateMetrics};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotResidentClass {
    ExecutionGraph,
    Session,
    Transcript,
    DerivedProjection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotResidencySnapshot {
    pub resident_id: String,
    pub class: HotResidentClass,
    pub owner_id: String,
    pub estimated_bytes: u64,
    pub last_access_ms: u64,
    pub pin_reasons: Vec<String>,
    pub reconstruct_cursor: Option<u64>,
}

pub struct HotResidencyRegistry {
    budget: Arc<RwLock<HotMemoryBudget>>,
    metrics: Arc<HotStateMetrics>,
    entries: Mutex<HashMap<String, HotResidencySnapshot>>,
}

impl HotResidencyRegistry {
    pub(super) fn new(budget: Arc<RwLock<HotMemoryBudget>>, metrics: Arc<HotStateMetrics>) -> Self {
        Self {
            budget,
            metrics,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn upsert(
        &self,
        resident_id: impl Into<String>,
        class: HotResidentClass,
        owner_id: impl Into<String>,
        estimated_bytes: u64,
        reconstruct_cursor: Option<u64>,
    ) {
        let resident_id = resident_id.into();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pin_reasons = entries
            .get(&resident_id)
            .map(|entry| entry.pin_reasons.clone())
            .unwrap_or_default();
        entries.insert(
            resident_id.clone(),
            HotResidencySnapshot {
                resident_id,
                class,
                owner_id: owner_id.into(),
                estimated_bytes,
                last_access_ms: now_ms(),
                pin_reasons,
                reconstruct_cursor,
            },
        );
        self.publish_bytes(&entries);
    }

    pub fn touch(&self, resident_id: &str) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get_mut(resident_id) {
            entry.last_access_ms = now_ms();
        }
    }

    pub fn pin(&self, resident_id: &str, reason: &str) -> bool {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = entries.get_mut(resident_id) else {
            return false;
        };
        if !entry.pin_reasons.iter().any(|current| current == reason) {
            entry.pin_reasons.push(reason.to_string());
            entry.pin_reasons.sort();
        }
        true
    }

    pub fn unpin(&self, resident_id: &str, reason: &str) -> bool {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = entries.get_mut(resident_id) else {
            return false;
        };
        let before = entry.pin_reasons.len();
        entry.pin_reasons.retain(|current| current != reason);
        before != entry.pin_reasons.len()
    }

    pub fn remove(&self, resident_id: &str) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.remove(resident_id).is_some() {
            self.metrics.evicted();
            self.publish_bytes(&entries);
        }
    }

    #[must_use]
    pub fn pressure_high(&self) -> bool {
        self.resident_bytes()
            >= self
                .budget
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .high_watermark_bytes
    }

    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|entry| entry.estimated_bytes)
            .sum()
    }

    #[must_use]
    pub fn eviction_candidates(&self, class: HotResidentClass) -> Vec<HotResidencySnapshot> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|entry| entry.class == class && entry.pin_reasons.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.last_access_ms);
        entries
    }

    #[must_use]
    pub fn snapshot(&self, resident_id: &str) -> Option<HotResidencySnapshot> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(resident_id)
            .cloned()
    }

    #[must_use]
    pub fn target_low_watermark(&self) -> u64 {
        self.budget
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .low_watermark_bytes
    }

    fn publish_bytes(&self, entries: &HashMap<String, HotResidencySnapshot>) {
        self.metrics
            .set_resident_bytes(entries.values().map(|entry| entry.estimated_bytes).sum());
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}
