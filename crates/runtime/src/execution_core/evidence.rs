use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEvidenceSummary {
    pub kind: String,
    pub evidence_refs: Vec<String>,
    pub summary: String,
    pub missing: Vec<String>,
}

impl RuntimeEvidenceSummary {
    #[must_use]
    pub fn from_refs(kind: impl Into<String>, evidence_refs: Vec<String>) -> Self {
        let count = evidence_refs.len();
        Self {
            kind: kind.into(),
            evidence_refs,
            summary: format!("{count} evidence refs available for synthesis"),
            missing: Vec::new(),
        }
    }
}
