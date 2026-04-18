//! Compression quality verification.
//!
//! After compaction, verifies that key information (decisions, entities,
//! relations) has been retained in the summary. Low retention rates
//! trigger warnings logged to the audit trail.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// QualityMetrics
// ---------------------------------------------------------------------------

/// Metrics measuring how well a compaction preserved important information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityMetrics {
    /// Fraction of pre-compaction decisions found in the summary.
    pub decision_retention: f64,
    /// Fraction of named entities (files, URLs, tools) found in the summary.
    pub entity_retention: f64,
    /// Fraction of relation keywords found in the summary.
    pub relation_retention: f64,
    /// Ratio of unique non-whitespace content to total summary length.
    pub information_density: f64,
    /// Overall quality score (0.0 - 1.0), weighted average.
    pub overall_score: f64,
}

impl QualityMetrics {
    /// Threshold below which a warning should be emitted.
    pub const WARNING_THRESHOLD: f64 = 0.80;

    /// Returns true if the overall quality score is below the warning threshold.
    #[must_use]
    pub fn needs_warning(&self) -> bool {
        self.overall_score < Self::WARNING_THRESHOLD
    }
}

// ---------------------------------------------------------------------------
// verify_compaction
// ---------------------------------------------------------------------------

/// Verify that compaction preserved key information.
///
/// Extracts decisions, entities, and relations from the original messages,
/// then checks whether they appear in the summary text.
pub fn verify_compaction(original_texts: &[&str], summary: &str) -> QualityMetrics {
    let decisions = extract_decisions(original_texts);
    let entities = extract_entities(original_texts);
    let relations = extract_relations(original_texts);

    let summary_lower = summary.to_ascii_lowercase();

    let decision_retention = retention_rate(&decisions, &summary_lower);
    let entity_retention = retention_rate(&entities, &summary_lower);
    let relation_retention = retention_rate(&relations, &summary_lower);
    let information_density = compute_density(summary);

    let overall_score = decision_retention * 0.40
        + entity_retention * 0.30
        + relation_retention * 0.15
        + information_density * 0.15;

    QualityMetrics {
        decision_retention,
        entity_retention,
        relation_retention,
        information_density,
        overall_score,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract decision-like phrases from text.
fn extract_decisions(texts: &[&str]) -> Vec<String> {
    let keywords = [
        "decided", "chosen", "agreed", "will use", "we should", "let's use",
        "decided to", "opted for", "going with", "settled on",
    ];
    let mut decisions = Vec::new();
    for text in texts {
        for line in text.lines() {
            let lower = line.to_ascii_lowercase();
            for kw in &keywords {
                if lower.contains(kw) {
                    decisions.push(line.trim().to_string());
                    break;
                }
            }
        }
    }
    decisions
}

/// Extract named entities (file paths, URLs, tool names).
fn extract_entities(texts: &[&str]) -> Vec<String> {
    let mut entities = Vec::new();
    for text in texts {
        for word in text.split_whitespace() {
            let cleaned = word.trim_matches(|c: char| c == ',' || c == '.' || c == ':' || c == ';');
            // File paths
            if cleaned.contains('/') && cleaned.len() > 5 {
                entities.push(cleaned.to_string());
            }
            // URLs
            if cleaned.starts_with("http://") || cleaned.starts_with("https://") {
                entities.push(cleaned.to_string());
            }
        }
    }
    entities
}

/// Extract relation-like phrases.
fn extract_relations(texts: &[&str]) -> Vec<String> {
    let patterns = ["depends on", "related to", "uses", "implements", "extends"];
    let mut relations = Vec::new();
    for text in texts {
        let lower = text.to_ascii_lowercase();
        for pat in &patterns {
            if lower.contains(pat) {
                relations.push(pat.to_string());
            }
        }
    }
    relations
}

/// Calculate what fraction of `items` appear in `summary_lower`.
fn retention_rate(items: &[String], summary_lower: &str) -> f64 {
    if items.is_empty() {
        return 1.0; // No items to lose = perfect retention
    }
    let found = items.iter().filter(|item| summary_lower.contains(&item.to_ascii_lowercase())).count();
    found as f64 / items.len() as f64
}

/// Compute information density: ratio of unique non-whitespace content to total.
fn compute_density(text: &str) -> f64 {
    if text.is_empty() {
        return 0.0;
    }
    let total_chars = text.len();
    let non_ws: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    if non_ws.is_empty() {
        return 0.0;
    }
    // Deduplicate by looking at unique trigrams
    let trigrams: std::collections::HashSet<&str> = non_ws
        .as_bytes()
        .windows(3)
        .map(|w| std::str::from_utf8(w).unwrap_or(""))
        .filter(|s| !s.is_empty())
        .collect();
    let unique_ratio = trigrams.len() as f64 / (non_ws.len().saturating_sub(2)).max(1) as f64;
    unique_ratio.min(1.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_compaction_with_full_retention() {
        let originals = ["We decided to use Rust for the backend", "The file src/main.rs depends on lib.rs"];
        let summary = "We decided to use Rust for the backend. The file src/main.rs depends on lib.rs.";
        let metrics = verify_compaction(&originals, summary);
        assert!(metrics.decision_retention >= 0.9);
        assert!(metrics.entity_retention >= 0.9);
        assert!(metrics.overall_score >= 0.7);
    }

    #[test]
    fn verify_compaction_with_poor_retention() {
        let originals = ["We decided to use Rust", "The file src/main.rs is important"];
        let summary = "General discussion about programming.";
        let metrics = verify_compaction(&originals, summary);
        assert!(metrics.overall_score < 0.5);
    }

    #[test]
    fn empty_originals_give_perfect_score() {
        let metrics = verify_compaction(&[], "Some summary");
        assert_eq!(metrics.decision_retention, 1.0);
        assert_eq!(metrics.entity_retention, 1.0);
    }

    #[test]
    fn needs_warning_below_threshold() {
        let metrics = QualityMetrics {
            decision_retention: 0.5,
            entity_retention: 0.5,
            relation_retention: 0.5,
            information_density: 0.5,
            overall_score: 0.5,
        };
        assert!(metrics.needs_warning());
    }

    #[test]
    fn no_warning_above_threshold() {
        let metrics = QualityMetrics {
            decision_retention: 0.9,
            entity_retention: 0.9,
            relation_retention: 0.9,
            information_density: 0.9,
            overall_score: 0.9,
        };
        assert!(!metrics.needs_warning());
    }

    #[test]
    fn extract_decisions_finds_keywords() {
        let texts = ["We decided to use PostgreSQL", "No decision here"];
        let decisions = extract_decisions(&texts);
        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].contains("PostgreSQL"));
    }

    #[test]
    fn extract_entities_finds_paths() {
        let texts = ["Edit the file src/lib.rs"];
        let entities = extract_entities(&texts);
        assert!(entities.iter().any(|e| e.contains("src/lib.rs")));
    }
}
