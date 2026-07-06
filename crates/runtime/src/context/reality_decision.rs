//! Turn-level Reality decision over memory, knowledge, facts, and recovery.

use fact_kernel::{FactExtractionBatch, FactReviewDecisionKind, FactReviewReceipt};
use serde::{Deserialize, Serialize};

use crate::context_runtime::{
    RuntimeCompressionCheckpointRef, RuntimeContextGovernanceReport, RuntimeContextMemoryDecision,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealityRuntimeDecision {
    pub kind: String,
    pub decision_id: String,
    pub governance_report_id: String,
    pub session_id: String,
    pub selected_memory: Vec<RealityMemoryDecision>,
    pub suppressed_memory: Vec<RealityMemoryDecision>,
    pub omitted_valuable_memory: Vec<RealityMemoryDecision>,
    pub recall_quality: RealityRecallQualityReport,
    pub knowledge: RealityKnowledgeDecision,
    pub fact_plan: RealityFactPlan,
    pub conflict_plan: Vec<String>,
    pub contamination_warnings: Vec<String>,
    pub context_budget_plan: RealityContextBudgetPlan,
    pub checkpoint: Option<RuntimeCompressionCheckpointRef>,
    pub resume_pointers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealityMemoryDecision {
    pub item_id: String,
    pub selected: bool,
    pub reason: String,
    pub token_estimate: u64,
    pub suppression_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityRecallQualityReport {
    pub selected_count: usize,
    pub suppressed_count: usize,
    pub omitted_valuable_count: usize,
    pub noise_ratio_bp: u16,
    pub conflict_pressure: usize,
    pub cross_project_contamination: bool,
    pub global_knowledge_activation_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityKnowledgeDecision {
    pub activated_pack_ids: Vec<String>,
    pub suppressed_namespaces: Vec<String>,
    pub activation_reasons: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub compliance_warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityFactPlan {
    pub candidate_count: usize,
    pub promoted_count: usize,
    pub held_count: usize,
    pub rejected_count: usize,
    pub conflict_count: usize,
    pub decisions: Vec<RealityFactPlanItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityFactPlanItem {
    pub candidate_id: String,
    pub decision: String,
    pub reason: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityContextBudgetPlan {
    pub pressure: String,
    pub selected_memory_tokens: u64,
    pub suppressed_memory_tokens: u64,
    pub checkpoint_required: bool,
    pub recovery_pointer_count: usize,
}

impl RealityRuntimeDecision {
    #[must_use]
    pub fn from_governance(
        governance: &RuntimeContextGovernanceReport,
        fact_batch: Option<&FactExtractionBatch>,
        review_receipt: Option<&FactReviewReceipt>,
    ) -> Self {
        let selected_memory = governance
            .selected_memory
            .iter()
            .map(memory_decision)
            .collect::<Vec<_>>();
        let suppressed_memory = governance
            .omitted_memory
            .iter()
            .map(memory_decision)
            .collect::<Vec<_>>();
        let omitted_valuable_memory = suppressed_memory
            .iter()
            .filter(|item| {
                item.token_estimate >= 128
                    || !item.reason.contains("low")
                    || item.reason.contains("project")
            })
            .cloned()
            .collect::<Vec<_>>();
        let omitted_valuable_count = omitted_valuable_memory.len();
        let selected_tokens = selected_memory
            .iter()
            .map(|item| item.token_estimate)
            .sum::<u64>();
        let suppressed_tokens = suppressed_memory
            .iter()
            .map(|item| item.token_estimate)
            .sum::<u64>();
        let total_memory = selected_memory.len() + suppressed_memory.len();
        let noise_ratio_bp = if total_memory == 0 {
            0
        } else {
            ((suppressed_memory.len() * 10_000) / total_memory).min(u16::MAX as usize) as u16
        };
        let cross_project_contamination =
            governance.contamination_notes.iter().any(|note: &String| {
                let note = note.to_ascii_lowercase();
                note.contains("cross-project")
                    || note.contains("project")
                    || note.contains("suppressed_for_current_turn")
            });
        let knowledge = RealityKnowledgeDecision {
            activated_pack_ids: governance.knowledge.activated_pack_ids.clone(),
            suppressed_namespaces: governance.knowledge.suppressed_namespaces.clone(),
            activation_reasons: governance
                .knowledge
                .activated_pack_ids
                .iter()
                .map(|pack| {
                    format!(
                        "activated pack {pack} for session {} current intent",
                        governance.session_id
                    )
                })
                .chain(
                    governance
                        .knowledge
                        .suppressed_namespaces
                        .iter()
                        .map(|namespace| {
                            format!("suppressed namespace {namespace} due governance/compliance")
                        }),
                )
                .collect(),
            evidence_refs: governance.knowledge.evidence_refs.clone(),
            compliance_warnings: governance.knowledge.compliance_warnings.clone(),
        };
        let fact_plan = fact_plan_from(fact_batch, review_receipt);
        let mut conflict_plan = governance.conflict_notes.clone();
        conflict_plan.extend(
            fact_plan
                .decisions
                .iter()
                .filter(|item| item.decision == "conflict")
                .map(|item| format!("hold conflicting fact candidate {}", item.candidate_id)),
        );
        let checkpoint_required = governance.compression_checkpoint.is_some()
            || noise_ratio_bp >= 3_000
            || !governance.contamination_notes.is_empty();
        let resume_pointers = governance
            .compression_checkpoint
            .as_ref()
            .map(|checkpoint| {
                vec![
                    format!("checkpoint:{}", checkpoint.checkpoint_id),
                    format!("context_epoch:{}", governance.context_epoch),
                    format!("governance_report:{}", governance.report_id),
                ]
            })
            .unwrap_or_else(|| {
                vec![
                    format!("context_epoch:{}", governance.context_epoch),
                    format!("governance_report:{}", governance.report_id),
                ]
            });
        let recovery_pointer_count = resume_pointers.len();
        Self {
            kind: "runtime.reality_runtime_decision".to_string(),
            decision_id: format!("reality-decision-{}", governance.report_id),
            governance_report_id: governance.report_id.clone(),
            session_id: governance.session_id.clone(),
            selected_memory,
            suppressed_memory,
            omitted_valuable_memory,
            recall_quality: RealityRecallQualityReport {
                selected_count: governance.selected_memory.len(),
                suppressed_count: governance.omitted_memory.len(),
                omitted_valuable_count,
                noise_ratio_bp,
                conflict_pressure: conflict_plan.len(),
                cross_project_contamination,
                global_knowledge_activation_reason: (!knowledge.activated_pack_ids.is_empty())
                    .then(|| {
                        "knowledge activation was governed by namespace/scope policy".to_string()
                    }),
            },
            knowledge,
            fact_plan,
            conflict_plan,
            contamination_warnings: governance.contamination_notes.clone(),
            context_budget_plan: RealityContextBudgetPlan {
                pressure: pressure_label(noise_ratio_bp, selected_tokens, suppressed_tokens),
                selected_memory_tokens: selected_tokens,
                suppressed_memory_tokens: suppressed_tokens,
                checkpoint_required,
                recovery_pointer_count,
            },
            checkpoint: governance.compression_checkpoint.clone(),
            resume_pointers,
        }
    }
}

fn memory_decision(memory: &RuntimeContextMemoryDecision) -> RealityMemoryDecision {
    RealityMemoryDecision {
        item_id: memory.item_id.clone(),
        selected: memory.selected,
        reason: memory.reason.clone(),
        token_estimate: memory.token_estimate,
        suppression_reason: (!memory.selected).then(|| memory.reason.clone()),
    }
}

fn fact_plan_from(
    fact_batch: Option<&FactExtractionBatch>,
    review_receipt: Option<&FactReviewReceipt>,
) -> RealityFactPlan {
    let mut decisions = Vec::new();
    if let Some(receipt) = review_receipt {
        decisions.extend(receipt.decisions.iter().map(|decision| {
            RealityFactPlanItem {
                candidate_id: decision.candidate.candidate_id.as_str().to_string(),
                decision: fact_decision_label(decision.decision).to_string(),
                reason: decision.reason.clone(),
                evidence_refs: decision
                    .candidate
                    .evidence
                    .iter()
                    .map(|evidence| evidence.as_str().to_string())
                    .collect(),
            }
        }));
        return RealityFactPlan {
            candidate_count: receipt.decisions.len(),
            promoted_count: receipt.promoted.len(),
            held_count: receipt.held.len(),
            rejected_count: receipt.rejected.len(),
            conflict_count: receipt.conflicts.len(),
            decisions,
        };
    }
    if let Some(batch) = fact_batch {
        decisions.extend(batch.candidates.iter().map(|candidate| {
            RealityFactPlanItem {
                candidate_id: candidate.candidate_id.as_str().to_string(),
                decision: "hold".to_string(),
                reason: "fact candidate awaiting fact-kernel review".to_string(),
                evidence_refs: candidate
                    .evidence
                    .iter()
                    .map(|evidence| evidence.as_str().to_string())
                    .collect(),
            }
        }));
        return RealityFactPlan {
            candidate_count: batch.candidates.len(),
            promoted_count: 0,
            held_count: batch.candidates.len(),
            rejected_count: 0,
            conflict_count: 0,
            decisions,
        };
    }
    RealityFactPlan {
        candidate_count: 0,
        promoted_count: 0,
        held_count: 0,
        rejected_count: 0,
        conflict_count: 0,
        decisions,
    }
}

fn fact_decision_label(decision: FactReviewDecisionKind) -> &'static str {
    match decision {
        FactReviewDecisionKind::Promote => "promote",
        FactReviewDecisionKind::Hold => "hold",
        FactReviewDecisionKind::Reject => "reject",
        FactReviewDecisionKind::Conflict => "conflict",
    }
}

fn pressure_label(noise_ratio_bp: u16, selected_tokens: u64, suppressed_tokens: u64) -> String {
    if noise_ratio_bp >= 5_000 || suppressed_tokens > selected_tokens {
        "high".to_string()
    } else if noise_ratio_bp >= 3_000 {
        "elevated".to_string()
    } else {
        "nominal".to_string()
    }
}
