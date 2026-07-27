use serde::{Deserialize, Serialize};

use crate::growth::{GrowthCandidate, PromotionDecision};

const PROMOTION_CONFIDENCE_FLOOR: u16 = 7_000;

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

    #[must_use]
    pub fn hold(candidate: GrowthCandidate, reason: impl Into<String>) -> Self {
        Self {
            candidate,
            decision: PromotionDecision::Hold,
            reason: reason.into(),
        }
    }

    #[must_use]
    pub fn reject(candidate: GrowthCandidate, reason: impl Into<String>) -> Self {
        Self {
            candidate,
            decision: PromotionDecision::Reject,
            reason: reason.into(),
        }
    }
}

#[must_use]
pub fn decide_candidate_promotion(candidate: GrowthCandidate) -> BridgeDecision {
    match &candidate {
        GrowthCandidate::Memory(memory) if !memory.boundary.promotion_allowed => {
            BridgeDecision::reject(
                candidate,
                "hypothetical memory candidate cannot be promoted",
            )
        }
        GrowthCandidate::Matrix(matrix) if !matrix.boundary.promotion_allowed => {
            BridgeDecision::reject(candidate, "hypothetical matrix fact cannot be promoted")
        }
        GrowthCandidate::Memory(memory) if memory.evidence.is_empty() => {
            BridgeDecision::hold(candidate, "memory candidate has no evidence")
        }
        GrowthCandidate::Matrix(matrix) if matrix.evidence.is_empty() => {
            BridgeDecision::hold(candidate, "matrix fact has no evidence")
        }
        GrowthCandidate::Memory(memory)
            if memory
                .confidence
                .basis_points()
                .is_none_or(|value| value < PROMOTION_CONFIDENCE_FLOOR) =>
        {
            BridgeDecision::hold(
                candidate,
                "memory candidate confidence is below promotion floor",
            )
        }
        GrowthCandidate::Matrix(matrix)
            if matrix
                .confidence
                .basis_points()
                .is_none_or(|value| value < PROMOTION_CONFIDENCE_FLOOR) =>
        {
            BridgeDecision::hold(candidate, "matrix fact confidence is below promotion floor")
        }
        GrowthCandidate::PolicyLearning { confidence, .. }
            if confidence
                .basis_points()
                .is_none_or(|value| value < PROMOTION_CONFIDENCE_FLOOR) =>
        {
            BridgeDecision::hold(
                candidate,
                "policy learning confidence is below promotion floor",
            )
        }
        _ => BridgeDecision::promote(candidate, "candidate satisfies promotion policy"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        bridge::decide_candidate_promotion,
        core::{Confidence, FactEvidenceId, FactId, FactSource, SourceKind},
        growth::{GrowthCandidate, PromotionDecision},
        hypothesis::HypothesisBoundary,
        matrix::MatrixFact,
        memory::MemoryCandidate,
    };

    fn source() -> FactSource {
        FactSource {
            kind: SourceKind::Runtime,
            id: "runtime-test".to_string(),
            label: None,
        }
    }

    #[test]
    fn hypothetical_memory_candidate_is_rejected() {
        let decision = decide_candidate_promotion(GrowthCandidate::Memory(MemoryCandidate {
            summary: "simulated preference".to_string(),
            source: source(),
            evidence: vec![FactEvidenceId::new()],
            confidence: Confidence::from_basis_points(9_000),
            boundary: HypothesisBoundary::hypothetical("scenario-1"),
            tags: vec!["simulation".to_string()],
        }));

        assert_eq!(decision.decision, PromotionDecision::Reject);
    }

    #[test]
    fn observed_matrix_fact_with_evidence_and_confidence_promotes() {
        let decision = decide_candidate_promotion(GrowthCandidate::Matrix(MatrixFact {
            id: FactId::new(),
            entity: "system".to_string(),
            predicate: "passes_gate".to_string(),
            value: json!(true),
            source: source(),
            evidence: vec![FactEvidenceId::new()],
            confidence: Confidence::from_basis_points(8_500),
            boundary: HypothesisBoundary::observed(),
        }));

        assert_eq!(decision.decision, PromotionDecision::Promote);
    }

    #[test]
    fn low_confidence_policy_learning_is_held() {
        let decision = decide_candidate_promotion(GrowthCandidate::PolicyLearning {
            summary: "possible tool instability".to_string(),
            confidence: Confidence::from_basis_points(4_500),
        });

        assert_eq!(decision.decision, PromotionDecision::Hold);
    }
}
