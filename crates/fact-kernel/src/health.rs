use serde::{Deserialize, Serialize};

use crate::core::FactId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactHealthIssueKind {
    Duplicate,
    Conflict,
    Stale,
    UnknownConfidence,
    LowConfidence,
    MissingEvidence,
    HypothesisLeak,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactHealthIssue {
    pub fact_id: Option<FactId>,
    pub kind: FactHealthIssueKind,
    pub detail: String,
}
