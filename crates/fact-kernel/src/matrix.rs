use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::{Confidence, EvidenceId, FactId, FactSource};
use crate::hypothesis::HypothesisBoundary;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixFact {
    pub id: FactId,
    pub entity: String,
    pub predicate: String,
    pub value: Value,
    pub source: FactSource,
    pub evidence: Vec<EvidenceId>,
    pub confidence: Confidence,
    pub boundary: HypothesisBoundary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixRelation {
    pub subject: FactId,
    pub relation: String,
    pub object: FactId,
    pub confidence: Confidence,
}
