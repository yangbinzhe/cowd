use serde::{Deserialize, Serialize};

use crate::core::{Confidence, EvidencePacket};
use crate::matrix::MatrixFact;
use crate::memory::MemoryCandidate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrowthSignalKind {
    UserPreference,
    RepeatedApproval,
    RepeatedRejection,
    ToolReliability,
    RiskPattern,
    VerificationFailure,
    StrategyOutcome,
    FactConflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrowthSignal {
    pub kind: GrowthSignalKind,
    pub summary: String,
    pub evidence: Vec<EvidencePacket>,
    pub confidence: Confidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GrowthCandidate {
    Memory(MemoryCandidate),
    Matrix(MatrixFact),
    PolicyLearning {
        summary: String,
        confidence: Confidence,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionDecision {
    Promote,
    Hold,
    Reject,
}
