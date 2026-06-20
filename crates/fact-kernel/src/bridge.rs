use serde::{Deserialize, Serialize};

use crate::growth::{GrowthCandidate, PromotionDecision};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeDecision {
    pub candidate: GrowthCandidate,
    pub decision: PromotionDecision,
    pub reason: String,
}

impl BridgeDecision {
    #[must_use]
    pub fn promote(candidate: GrowthCandidate, reason: impl Into<String>) -> Self {
        Self {
            candidate,
            decision: PromotionDecision::Promote,
            reason: reason.into(),
        }
    }
}
