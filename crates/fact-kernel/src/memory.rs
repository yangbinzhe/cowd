use serde::{Deserialize, Serialize};

use crate::core::{Confidence, FactEvidenceId, FactSource};
use crate::hypothesis::HypothesisBoundary;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCandidate {
    pub summary: String,
    pub source: FactSource,
    pub evidence: Vec<FactEvidenceId>,
    pub confidence: Confidence,
    pub boundary: HypothesisBoundary,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallQuery {
    pub query: String,
    pub limit: usize,
    pub include_hypothetical: bool,
}

impl RecallQuery {
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            limit: 20,
            include_hypothetical: false,
        }
    }
}
