// P1-1: Coherence scoring — Jaccard similarity for memory-context relevance.
// Derived from claude-context's coherence scoring approach.
// Replaces the inline keyword-match lambda in prepare_memory_context.

use std::collections::HashSet;

/// Compute Jaccard similarity between two text strings.
/// Returns a value in [0.0, 1.0] where 1.0 = identical word sets.
pub fn jaccard_similarity(query: &str, content: &str) -> f32 {
    let q_words: HashSet<&str> = query
        .split_whitespace()
        .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|s| !s.is_empty())
        .collect();
    let c_words: HashSet<&str> = content
        .split_whitespace()
        .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|s| !s.is_empty())
        .collect();

    let overlap = q_words.intersection(&c_words).count();
    let union = q_words.union(&c_words).count();
    if union == 0 {
        0.0
    } else {
        overlap as f32 / union as f32
    }
}

/// Return true when the entry is sufficiently relevant to the query.
/// L0 (identity) entries are always considered relevant.
pub fn is_relevant(entry_content: &str, query: &str, threshold: f32, is_identity: bool) -> bool {
    if is_identity {
        return true;
    }
    let score = jaccard_similarity(query, entry_content);
    score >= threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p11_identical_strings_score_one() {
        let score = jaccard_similarity("hello world", "hello world");
        assert!((score - 1.0).abs() < 0.01);
    }

    #[test]
    fn p11_no_overlap_scores_zero() {
        let score = jaccard_similarity("rust programming", "python pandas");
        assert!((score - 0.0).abs() < 0.01);
    }

    #[test]
    fn p11_partial_overlap_has_intermediate_score() {
        let score = jaccard_similarity("rust async tokio", "rust sync std");
        assert!(score > 0.1 && score < 1.0, "score={score}");
    }

    #[test]
    fn p11_high_relevance_passes_threshold() {
        assert!(is_relevant("fixing tokio async bug", "tokio runtime panic", 0.1, false));
    }

    #[test]
    fn p11_low_relevance_fails_threshold() {
        assert!(!is_relevant("python flask web app", "rust tokio runtime", 0.2, false));
    }

    #[test]
    fn p11_identity_always_relevant() {
        assert!(is_relevant("anything", "completely different query", 0.5, true));
    }

    #[test]
    fn p11_empty_strings() {
        let score = jaccard_similarity("", "");
        assert_eq!(score, 0.0);
    }
}
