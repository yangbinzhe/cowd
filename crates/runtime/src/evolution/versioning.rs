use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::candidate::EvolutionCandidate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionVersionRecord {
    pub version_id: String,
    pub candidate_id: String,
    pub kind: String,
    pub adapter: String,
    pub previous_ref: String,
    pub new_ref: String,
    pub comparison_report_ref: String,
    pub promotion_artifact: String,
    pub rollback_artifact: String,
    pub enabled_scope: Vec<String>,
    pub created_at_ms: u128,
    pub created_by: String,
}

impl EvolutionVersionRecord {
    #[must_use]
    pub fn from_candidate(candidate: &EvolutionCandidate) -> Self {
        Self {
            version_id: format!("evo-version-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            kind: candidate.kind.as_str().to_string(),
            adapter: candidate.promotion_adapter.clone(),
            previous_ref: candidate.baseline_ref.clone(),
            new_ref: candidate.candidate_ref.clone(),
            comparison_report_ref: candidate
                .comparison_report_ref
                .clone()
                .unwrap_or_else(|| "comparison:missing".to_string()),
            promotion_artifact: candidate
                .artifact_path
                .clone()
                .unwrap_or_else(|| "artifact:missing".to_string()),
            rollback_artifact: format!("rollback:{}", candidate.rollback_strategy),
            enabled_scope: candidate.scope.clone(),
            created_at_ms: now_ms(),
            created_by: "runtime::evolution".to_string(),
        }
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
