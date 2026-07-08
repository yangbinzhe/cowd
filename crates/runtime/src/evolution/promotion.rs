use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    adoption::{EvolutionAdoptionManager, EvolutionAdoptionReceipt},
    candidate::{EvolutionCandidate, EvolutionCandidateStatus},
    versioning::EvolutionVersionRecord,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionPromotionReceipt {
    pub promotion_id: String,
    pub candidate_id: String,
    pub adapter: String,
    pub accepted: bool,
    pub reason: String,
    pub version_record: Option<EvolutionVersionRecord>,
    pub adoption_receipt: EvolutionAdoptionReceipt,
    pub created_at_ms: u128,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionPromotionManager;

impl EvolutionPromotionManager {
    #[must_use]
    pub fn promote(candidate: &EvolutionCandidate) -> EvolutionPromotionReceipt {
        let adoption_receipt = EvolutionAdoptionManager::evaluate(
            candidate,
            EvolutionCandidateStatus::ApprovedForAdoption,
            candidate.comparison_report_ref.clone(),
        );
        let version_record = if adoption_receipt.accepted {
            Some(EvolutionVersionRecord::from_candidate(candidate))
        } else {
            None
        };
        EvolutionPromotionReceipt {
            promotion_id: format!("evo-promotion-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            adapter: candidate.promotion_adapter.clone(),
            accepted: adoption_receipt.accepted,
            reason: adoption_receipt.reason.clone(),
            version_record,
            adoption_receipt,
            created_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionPromotionAdapter;

impl EvolutionPromotionAdapter {
    #[must_use]
    pub fn adapter_for(candidate: &EvolutionCandidate) -> String {
        candidate.promotion_adapter.clone()
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
