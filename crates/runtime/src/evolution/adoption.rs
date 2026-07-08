use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::candidate::{EvolutionCandidate, EvolutionCandidateStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionAdoptionReceipt {
    pub receipt_id: String,
    pub candidate_id: String,
    pub requested_status: EvolutionCandidateStatus,
    pub accepted: bool,
    pub reason: String,
    pub required_eval_id: Option<String>,
    pub comparison_report_ref: Option<String>,
    pub rollback_strategy: String,
    pub mainline_modified: bool,
    pub created_at_ms: u128,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionAdoptionManager;

impl EvolutionAdoptionManager {
    #[must_use]
    pub fn evaluate(
        candidate: &EvolutionCandidate,
        requested_status: EvolutionCandidateStatus,
        passed_eval_id: Option<String>,
    ) -> EvolutionAdoptionReceipt {
        let requires_eval = matches!(
            requested_status,
            EvolutionCandidateStatus::ApprovedForAdoption | EvolutionCandidateStatus::Evaluated
        );
        let has_comparison = candidate.comparison_report_ref.is_some() || passed_eval_id.is_some();
        let accepted = !candidate.mainline_modified
            && !candidate.rollback_strategy.trim().is_empty()
            && (!requires_eval || has_comparison);
        let reason = if accepted {
            "candidate decision accepted by adoption manager".to_string()
        } else if candidate.mainline_modified {
            "candidate cannot be adopted because mainline_modified=true".to_string()
        } else if candidate.rollback_strategy.trim().is_empty() {
            "candidate cannot be adopted without rollback strategy".to_string()
        } else {
            "candidate requires comparison report before adoption".to_string()
        };
        EvolutionAdoptionReceipt {
            receipt_id: format!("evo-adoption-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            requested_status,
            accepted,
            reason,
            required_eval_id: passed_eval_id,
            comparison_report_ref: candidate.comparison_report_ref.clone(),
            rollback_strategy: candidate.rollback_strategy.clone(),
            mainline_modified: false,
            created_at_ms: now_ms(),
        }
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
