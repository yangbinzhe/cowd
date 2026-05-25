//! ConflictResolver — auto-resolve knowledge graph contradictions
//!
//! Consumes FactChecker's detect_conflict() output and produces
//! automated verdicts: KeepExisting, ReplaceWithNew, PromoteConsensus, FlagForReview.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ─── Core types ────────────────────────────────────────────────────────────────

/// Auto-resolution engine for knowledge-graph contradictions.
///
/// Uses per-agent reliability weights (defaults match `FactChecker`),
/// confidence scores, and consensus counts to decide which triple
/// to keep when two triples with the same (subject, predicate) but
/// different objects conflict.
#[derive(Debug, Clone)]
pub struct ConflictResolver {
    /// Per-agent reliability weights.
    /// Default: Orchestrator=1.0, Reviewer=0.8, Executor=0.6, unknown=0.4
    pub agent_weights: HashMap<String, f32>,
}

/// A single detected conflict between an existing triple and a new triple.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictInfo {
    /// ID of the existing triple (already in the graph).
    pub existing_id: String,
    /// ID of the new triple (the one being proposed).
    pub new_id: String,
    /// The subject entity this conflict is about.
    pub subject: String,
    /// The predicate this conflict is about.
    pub predicate: String,
    /// The object value in the existing triple.
    pub existing_object: String,
    /// The object value in the new (proposed) triple.
    pub new_object: String,
    /// Confidence of the existing triple in `[0.0, 1.0]`.
    pub existing_confidence: f32,
    /// Confidence of the new triple in `[0.0, 1.0]`.
    pub new_confidence: f32,
    /// The agent that produced the **new** triple.
    pub new_agent: String,
    /// How many distinct agents agree on the new value.
    pub consensus_count: usize,
    /// Conflict score from `FactChecker::detect_conflict()`.
    pub conflict_score: f32,
}

/// Automated resolution decision for a single conflict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Verdict {
    /// Keep the existing triple; discard the new one.
    KeepExisting(String),
    /// Replace the existing triple with the new one; mark old as invalidated.
    ReplaceWithNew(String),
    /// Promote the consensus triple's confidence to 0.95.
    PromoteConsensus(String),
    /// Cannot auto-decide — requires human or orchestrator review.
    FlagForReview(String),
}

/// Summary report after resolving all conflicts in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrectionReport {
    /// Number of conflicts resolved (KeepExisting + ReplaceWithNew + PromoteConsensus).
    pub corrected: usize,
    /// Number of triples pruned / invalidated.
    pub pruned: usize,
    /// Number of conflicts flagged for manual review.
    pub flagged: usize,
}

// ─── Resolution logic ──────────────────────────────────────────────────────────

impl ConflictResolver {
    /// Create a new resolver with default agent weights.
    ///
    /// Default weights (matching `FactChecker`):
    ///   Orchestrator=1.0, Reviewer=0.8, Executor=0.6, unknown=0.4
    pub fn new() -> Self {
        let mut agent_weights = HashMap::new();
        agent_weights.insert("Orchestrator".to_string(), 1.0);
        agent_weights.insert("Reviewer".to_string(), 0.8);
        agent_weights.insert("Executor".to_string(), 0.6);
        agent_weights.insert("unknown".to_string(), 0.4);
        Self { agent_weights }
    }

    /// Look up the reliability weight for an agent.
    pub fn agent_weight(&self, agent: &str) -> f32 {
        self.agent_weights
            .get(agent)
            .copied()
            .unwrap_or_else(|| {
                self.agent_weights
                    .get("unknown")
                    .copied()
                    .unwrap_or(0.4)
            })
    }

    /// Compute a weighted confidence score for a triple.
    ///
    /// `weighted = new_confidence * agent_weight`, clamped to `[0.0, 1.0]`.
    fn weighted_confidence(&self, confidence: f32, agent: &str) -> f32 {
        let weight = self.agent_weight(agent);
        (confidence * weight).clamp(0.0, 1.0)
    }

    /// Resolve a single conflict and return the automated verdict.
    ///
    /// Decision tree (applied in strict priority order):
    ///
    /// 1. **Consensus** (`consensus_count >= 3`)
    ///    → `PromoteConsensus` — multiple distinct agents agree, so the
    ///      new value is likely correct.
    ///
    /// 2. **Existing clearly stronger** (`existing_confidence > weighted_new + 0.3`)
    ///    → `KeepExisting` — the existing triple has significantly higher
    ///      effective confidence.
    ///
    /// 3. **New clearly stronger** (`weighted_new > existing_confidence`)
    ///    → `ReplaceWithNew` — the new triple (with agent weight factored in)
    ///      has higher effective confidence.
    ///
    /// 4. **Too close to call**
    ///    → `FlagForReview` — the effective confidences are within the
    ///      margin; a human or orchestrator must decide.
    pub fn resolve(&self, conflict: &ConflictInfo) -> Verdict {
        // Priority 1: Consensus of 3+ distinct agents overrides everything.
        if conflict.consensus_count >= 3 {
            return Verdict::PromoteConsensus(conflict.new_id.clone());
        }

        let weighted_new = self.weighted_confidence(conflict.new_confidence, &conflict.new_agent);

        // Priority 2: Existing is significantly more confident.
        if conflict.existing_confidence > weighted_new + 0.3 {
            return Verdict::KeepExisting(conflict.existing_id.clone());
        }

        // Priority 3: New (weighted) beats existing.
        if weighted_new > conflict.existing_confidence {
            return Verdict::ReplaceWithNew(conflict.new_id.clone());
        }

        // Priority 4: Too close → flag for review.
        Verdict::FlagForReview(conflict.new_id.clone())
    }

    /// Resolve a batch of conflicts and produce a summary report.
    pub fn resolve_all(&self, conflicts: &[ConflictInfo]) -> (Vec<Verdict>, CorrectionReport) {
        let mut corrected = 0usize;
        let mut pruned = 0usize;
        let mut flagged = 0usize;
        let mut verdicts = Vec::with_capacity(conflicts.len());

        for conflict in conflicts {
            let verdict = self.resolve(conflict);
            match &verdict {
                Verdict::KeepExisting(_) => {
                    corrected += 1;
                    pruned += 1; // the new triple is discarded
                }
                Verdict::ReplaceWithNew(_) => {
                    corrected += 1;
                    pruned += 1; // the existing triple is invalidated
                }
                Verdict::PromoteConsensus(_) => {
                    corrected += 1;
                }
                Verdict::FlagForReview(_) => {
                    flagged += 1;
                }
            }
            verdicts.push(verdict);
        }

        (
            verdicts,
            CorrectionReport {
                corrected,
                pruned,
                flagged,
            },
        )
    }
}

impl Default for ConflictResolver {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a ConflictInfo with defaults.
    fn conflict_info(
        existing_confidence: f32,
        new_confidence: f32,
        new_agent: &str,
        consensus_count: usize,
    ) -> ConflictInfo {
        ConflictInfo {
            existing_id: "t_existing".to_string(),
            new_id: "t_new".to_string(),
            subject: "Alice".to_string(),
            predicate: "child_of".to_string(),
            existing_object: "Bob".to_string(),
            new_object: "Charlie".to_string(),
            existing_confidence,
            new_confidence,
            new_agent: new_agent.to_string(),
            consensus_count,
            conflict_score: 0.5,
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    #[test]
    fn test_resolve_by_consensus() {
        // 3+ distinct agents agree on the new value → PromoteConsensus
        let resolver = ConflictResolver::new();
        let conflict = conflict_info(0.9, 0.5, "unknown", /* consensus_count */ 3);

        let verdict = resolver.resolve(&conflict);
        assert_eq!(
            verdict,
            Verdict::PromoteConsensus("t_new".to_string()),
            "consensus_count >= 3 should yield PromoteConsensus"
        );
    }

    #[test]
    fn test_resolve_by_confidence() {
        // Existing has much higher confidence (0.9 vs 0.5) → KeepExisting
        let resolver = ConflictResolver::new();
        let conflict = conflict_info(0.9, 0.5, "unknown", /* consensus_count */ 0);

        let verdict = resolver.resolve(&conflict);
        assert_eq!(
            verdict,
            Verdict::KeepExisting("t_existing".to_string()),
            "existing 0.9 >> new 0.5 should yield KeepExisting"
        );
    }

    #[test]
    fn test_resolve_by_agent_weight() {
        // New agent weight 0.9 × confidence 0.85 = 0.765 > existing 0.6 → ReplaceWithNew
        let mut resolver = ConflictResolver::new();
        resolver.agent_weights.insert("TrustedAgent".to_string(), 0.9);

        let conflict = conflict_info(0.6, 0.85, "TrustedAgent", /* consensus_count */ 0);

        let verdict = resolver.resolve(&conflict);
        assert_eq!(
            verdict,
            Verdict::ReplaceWithNew("t_new".to_string()),
            "weighted_new 0.765 > existing 0.6 should yield ReplaceWithNew"
        );
    }

    #[test]
    fn test_resolve_flag_for_review() {
        // Low confidence + low weight → too close to auto-decide → FlagForReview
        let resolver = ConflictResolver::new();
        // existing=0.45, new=0.4, agent=unknown (weight=0.4)
        // weighted_new = 0.4 * 0.4 = 0.16
        // existing 0.45 not > 0.16 + 0.3 (0.46) → no KeepExisting
        // weighted_new 0.16 not > existing 0.45 → no ReplaceWithNew
        // → FlagForReview
        let conflict = conflict_info(0.45, 0.4, "unknown", /* consensus_count */ 0);

        let verdict = resolver.resolve(&conflict);
        assert_eq!(
            verdict,
            Verdict::FlagForReview("t_new".to_string()),
            "close confidences without consensus should yield FlagForReview"
        );
    }

    #[test]
    fn test_resolve_empty_conflicts() {
        // No conflicts → empty report
        let resolver = ConflictResolver::new();
        let conflicts: Vec<ConflictInfo> = vec![];
        let (_verdicts, report) = resolver.resolve_all(&conflicts);

        assert_eq!(report.corrected, 0, "no conflicts → nothing corrected");
        assert_eq!(report.pruned, 0, "no conflicts → nothing pruned");
        assert_eq!(report.flagged, 0, "no conflicts → nothing flagged");
    }

    #[test]
    fn test_resolve_all_mixed_batch() {
        let resolver = ConflictResolver::new();
        let mut resolver_weighted = ConflictResolver::new();
        resolver_weighted
            .agent_weights
            .insert("TrustedAgent".to_string(), 0.9);
        let resolver_default = ConflictResolver::new();

        let conflicts = vec![
            // 1. Consensus (3 agents agree on new) → PromoteConsensus
            conflict_info(0.9, 0.5, "Orchestrator", 3),
            // 2. Existing clearly stronger → KeepExisting
            conflict_info(0.95, 0.3, "unknown", 0),
            // 3. New (weighted) beats existing → ReplaceWithNew
            ConflictInfo {
                existing_id: "t3_exist".to_string(),
                new_id: "t3_new".to_string(),
                subject: "Bob".to_string(),
                predicate: "partner_of".to_string(),
                existing_object: "Carol".to_string(),
                new_object: "Dave".to_string(),
                existing_confidence: 0.5,
                new_confidence: 0.9,
                new_agent: "TrustedAgent".to_string(),
                consensus_count: 0,
                conflict_score: 0.7,
            },
            // 4. Too close → FlagForReview
            conflict_info(0.45, 0.4, "unknown", 0),
        ];

        let resolver = resolver_default; // use default, but need TrustedAgent weight
        let mut resolver = ConflictResolver::new();
        resolver.agent_weights.insert("TrustedAgent".to_string(), 0.9);

        let (verdicts, report) = resolver.resolve_all(&conflicts);

        assert_eq!(verdicts.len(), 4);
        assert!(matches!(verdicts[0], Verdict::PromoteConsensus(_)));
        assert!(matches!(verdicts[1], Verdict::KeepExisting(_)));
        assert!(matches!(verdicts[2], Verdict::ReplaceWithNew(_)));
        assert!(matches!(verdicts[3], Verdict::FlagForReview(_)));

        // corrected = 3 (PromoteConsensus + KeepExisting + ReplaceWithNew)
        // pruned = 2 (KeepExisting prunes new, ReplaceWithNew prunes old)
        // flagged = 1
        assert_eq!(report.corrected, 3);
        assert_eq!(report.pruned, 2);
        assert_eq!(report.flagged, 1);
    }
}
