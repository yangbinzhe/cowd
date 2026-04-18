//! Memory drift detection.
//!
//! Tracks how memory entries change (or fail to change) over time and flags
//! entries that have become stale or contradictory relative to newer
//! observations.
//!
//! TODO: implement drift detection algorithm.

use crate::{
    config::DriftConfig,
    error::MemoryError,
    types::{MemoryEntry, MemoryId},
};

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

    /// Apply daily staleness decay to a mutable entry.
    pub fn decay(&self, entry: &mut MemoryEntry) {
        entry.staleness =
            (entry.staleness + self.config.staleness_decay_per_day).clamp(0.0, 1.0);
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
}
