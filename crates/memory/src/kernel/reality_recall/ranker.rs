use std::{cmp::Ordering, collections::HashSet};

use super::RecallCandidate;

pub fn rank_candidates(candidates: &mut [RecallCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .scores
            .final_score
            .partial_cmp(&left.scores.final_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| format!("{:?}", right.layer).cmp(&format!("{:?}", left.layer)))
            .then_with(|| right.title.cmp(&left.title))
    });
}

pub fn rank_and_deduplicate_candidates(candidates: &mut Vec<RecallCandidate>) {
    rank_candidates(candidates);
    let mut seen = HashSet::new();
    candidates.retain(|candidate| seen.insert(recall_candidate_dedup_key(candidate)));
}

fn recall_candidate_dedup_key(candidate: &RecallCandidate) -> String {
    let title = normalize_recall_text(&candidate.title);
    let content = normalize_recall_text(&candidate.content_preview);
    if title.len().saturating_add(content.len()) < 12 {
        return format!(
            "{:?}:{}",
            candidate.source,
            candidate
                .evidence
                .refs
                .first()
                .cloned()
                .unwrap_or_else(|| candidate.id.to_string())
        );
    }
    format!("{}\n{}", title, content)
}

fn normalize_recall_text(text: &str) -> String {
    text.to_lowercase()
        .replace('…', "...")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use crate::types::MemoryLayer;
    use harness_contract::reality::{RealityBoundary, RecallSourceKind};

    use super::*;

    fn candidate(title: &str, content: &str, score: f32) -> RecallCandidate {
        RecallCandidate::from_external(
            title,
            content,
            MemoryLayer::L3,
            RecallSourceKind::Memory,
            score,
            score,
            vec![format!("test:{title}")],
            RealityBoundary::Observed,
        )
    }

    #[test]
    fn rank_and_deduplicate_candidates_keeps_best_duplicate() {
        let mut candidates = vec![
            candidate("User preference", "Do not expand endlessly", 0.5),
            candidate("User preference", "Do not expand endlessly", 0.9),
            candidate("Architecture", "Keep evidence", 0.7),
        ];

        rank_and_deduplicate_candidates(&mut candidates);

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].title, "User preference");
        assert!(candidates[0].scores.final_score > 0.9);
        assert_eq!(candidates[1].title, "Architecture");
    }
}
