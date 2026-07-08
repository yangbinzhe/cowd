use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    candidate::EvolutionCandidate, memory_scope::EvolutionMemoryScope,
    promotion::EvolutionPromotionReceipt, rollback::EvolutionRollbackReceipt,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvolutionMemoryRecord {
    pub record_id: String,
    pub kind: String,
    pub candidate_id: String,
    pub version_id: Option<String>,
    pub source_eval: Option<String>,
    pub scope: EvolutionMemoryScope,
    pub goal_ids: Vec<String>,
    pub confidence: f64,
    pub staleness: f64,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub activation_policy: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionMemoryBridge;

impl EvolutionMemoryBridge {
    #[must_use]
    pub fn from_promotion(
        candidate: &EvolutionCandidate,
        receipt: &EvolutionPromotionReceipt,
    ) -> EvolutionMemoryRecord {
        let scope =
            EvolutionMemoryScope::for_goals(candidate.owner.clone(), candidate.goal_ids.clone());
        EvolutionMemoryRecord {
            record_id: format!("evo-memory-{}", Uuid::new_v4()),
            kind: if receipt.accepted {
                "adopted_policy".to_string()
            } else {
                "rejected_candidate".to_string()
            },
            candidate_id: candidate.candidate_id.clone(),
            version_id: receipt
                .version_record
                .as_ref()
                .map(|record| record.version_id.clone()),
            source_eval: candidate.comparison_report_ref.clone(),
            goal_ids: candidate.goal_ids.clone(),
            confidence: if receipt.accepted { 0.85 } else { 0.55 },
            staleness: 0.0,
            summary: receipt.reason.clone(),
            evidence_refs: vec![
                receipt.promotion_id.clone(),
                candidate
                    .comparison_report_ref
                    .clone()
                    .unwrap_or_else(|| "comparison:missing".to_string()),
            ],
            activation_policy: scope.activation_policy.clone(),
            scope,
        }
    }

    #[must_use]
    pub fn from_rollback(receipt: &EvolutionRollbackReceipt) -> EvolutionMemoryRecord {
        let scope = EvolutionMemoryScope::for_goals("runtime", Vec::new());
        EvolutionMemoryRecord {
            record_id: format!("evo-memory-{}", Uuid::new_v4()),
            kind: "recovery_pattern".to_string(),
            candidate_id: receipt.candidate_id.clone(),
            version_id: Some(receipt.version_id.clone()),
            source_eval: None,
            goal_ids: Vec::new(),
            confidence: 0.8,
            staleness: 0.0,
            summary: receipt.reason.clone(),
            evidence_refs: vec![
                receipt.rollback_id.clone(),
                receipt.rollback_artifact.clone(),
            ],
            activation_policy: scope.activation_policy.clone(),
            scope,
        }
    }

    #[must_use]
    pub fn should_activate(
        record: &EvolutionMemoryRecord,
        task: &str,
        goal_ids: &[String],
    ) -> bool {
        let task = task.to_ascii_lowercase();
        task.contains("evolution")
            || task.contains("self")
            || task.contains("进化")
            || record.goal_ids.iter().any(|goal| goal_ids.contains(goal))
    }
}
