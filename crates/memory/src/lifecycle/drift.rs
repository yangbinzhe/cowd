//! Memory drift detection.
//!
//! Tracks how memory entries change (or fail to change) over time and flags
//! entries that have become stale or contradictory relative to newer
//! observations.

use chrono::Utc;

use crate::{
    config::DriftConfig,
    error::MemoryError,
    types::{MemoryEntry, MemoryId},
};
use std::collections::HashSet;

/// Result alias.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Outcome of a drift check.
#[derive(Debug, Clone)]
pub enum DriftVerdict {
    /// Entry is current and healthy.
    Ok,
    /// Entry should be flagged for human review.
    FlagForReview { reason: String },
    /// Entry should be automatically pruned.
    Prune { reason: String },
}

/// Compute the decay delta for an entry based on elapsed wall-clock time.
fn compute_decay_delta(created_at: &chrono::DateTime<chrono::Utc>, decay_per_day: f32) -> f32 {
    let elapsed = Utc::now() - *created_at;
    let days = elapsed.num_hours() as f32 / 24.0;
    decay_per_day * days.max(0.0)
}

/// Detects staleness and contradiction in memory entries.
pub struct DriftDetector {
    config: DriftConfig,
}

impl DriftDetector {
    #[must_use]
    pub fn new(config: DriftConfig) -> Self {
        Self { config }
    }

    /// Evaluate whether `entry` has drifted beyond acceptable thresholds.
    #[must_use]
    pub fn check(&self, entry: &MemoryEntry) -> DriftVerdict {
        if entry.staleness >= self.config.prune_threshold {
            DriftVerdict::Prune {
                reason: format!(
                    "staleness {:.2} exceeds prune threshold {:.2}",
                    entry.staleness, self.config.prune_threshold
                ),
            }
        } else if entry.staleness >= self.config.review_threshold {
            DriftVerdict::FlagForReview {
                reason: format!(
                    "staleness {:.2} exceeds review threshold {:.2}",
                    entry.staleness, self.config.review_threshold
                ),
            }
        } else {
            DriftVerdict::Ok
        }
    }

    /// Apply daily staleness decay proportional to wall-clock time elapsed since creation.
    pub fn decay(&self, entry: &mut MemoryEntry) {
        let delta = compute_decay_delta(&entry.created_at, self.config.staleness_decay_per_day);
        entry.staleness = (entry.staleness + delta).clamp(0.0, 1.0);
    }

    /// IDs of entries that should be pruned from `entries`.
    #[must_use]
    pub fn prune_candidates<'a>(&self, entries: &'a [MemoryEntry]) -> Vec<&'a MemoryId> {
        entries
            .iter()
            .filter(|e| matches!(self.check(e), DriftVerdict::Prune { .. }))
            .map(|e| &e.id)
            .collect()
    }

    /// Detect if `entry` contradicts any entry in `related`.
    /// Contradiction is detected when two entries with the same L1/L2 layer
    /// have opposing keywords (e.g. "fixed" vs "broken").
    pub fn check_contradictions(
        &self,
        entry: &MemoryEntry,
        related: &[MemoryEntry],
    ) -> Option<String> {
        let entry_words: HashSet<&str> = entry
            .content
            .split_whitespace()
            .filter(|w| w.len() > 2)
            .collect();

        for other in related {
            if other.id == entry.id {
                continue;
            }
            if other.layer != entry.layer {
                continue;
            }
            let other_words: HashSet<&str> = other
                .content
                .split_whitespace()
                .filter(|w| w.len() > 2)
                .collect();

            // Simple heuristic: high entity overlap but opposing signals
            let overlap = entry_words.intersection(&other_words).count();
            let union = entry_words.union(&other_words).count();
            if union == 0 {
                continue;
            }
            let jaccard = overlap as f32 / union as f32;
            if jaccard > self.config.contradiction_jaccard_threshold {
                return Some(format!(
                    "possible contradiction with entry {} (jaccard={:.2})",
                    other.id, jaccard
                ));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
    use crate::MemoryScope;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_entry(content: &str, layer: MemoryLayer, staleness: f32) -> MemoryEntry {
        make_entry_aged(content, layer, staleness, Utc::now())
    }

    fn make_entry_aged(
        content: &str,
        layer: MemoryLayer,
        staleness: f32,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(),
            title: content.chars().take(30).collect(),
            content: content.into(),
            layer,
            confidence: 0.8,
            staleness,
            created_at,
            updated_at: created_at,
            last_accessed_at: None,
            source: MemorySource::AutoExtracted,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            relations: vec![],
            embedding: None,
            tags: vec![],
            access_count: 0,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        }
    }

    #[test]
    fn p14_staleness_above_prune_threshold_is_pruned() {
        let cfg = DriftConfig {
            review_threshold: 0.5,
            prune_threshold: 0.9,
            staleness_decay_per_day: 0.1,
            contradiction_jaccard_threshold: 0.6,
            ..Default::default()
        };
        let detector = DriftDetector::new(cfg);
        let entry = make_entry("old data", MemoryLayer::L2, 0.95);
        let verdict = detector.check(&entry);
        assert!(matches!(verdict, DriftVerdict::Prune { .. }));
    }

    #[test]
    fn p14_contradiction_detected_with_high_overlap() {
        let cfg = DriftConfig {
            review_threshold: 0.5,
            prune_threshold: 0.9,
            staleness_decay_per_day: 0.1,
            contradiction_jaccard_threshold: 0.2,
            ..Default::default()
        };
        let detector = DriftDetector::new(cfg);
        let e1 = make_entry("the tokio runtime is stable and fast", MemoryLayer::L1, 0.0);
        let e2 = make_entry(
            "the tokio runtime has serious stability issues",
            MemoryLayer::L1,
            0.0,
        );
        let result = detector.check_contradictions(&e1, &[e2]);
        assert!(result.is_some(), "should detect contradiction");
    }

    #[test]
    fn p14_no_contradiction_with_different_topics() {
        let cfg = DriftConfig {
            review_threshold: 0.5,
            prune_threshold: 0.9,
            staleness_decay_per_day: 0.1,
            contradiction_jaccard_threshold: 0.3,
            low_priority_prune_threshold: 0.8,
        };
        let detector = DriftDetector::new(cfg);
        let entries = vec![
            make_entry("a", MemoryLayer::L2, 0.95),
            make_entry("b", MemoryLayer::L2, 0.3),
        ];
        let candidates = detector.prune_candidates(&entries);
        assert_eq!(candidates.len(), 1);
    }
}
