use std::cmp::Ordering;

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
