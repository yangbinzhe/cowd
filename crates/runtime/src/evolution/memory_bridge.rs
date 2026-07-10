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
        let summary = promotion_memory_summary(candidate, receipt);
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
            summary,
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
        if task.contains("evolution")
            || task.contains("self")
            || task.contains("进化")
            || task.contains("自我")
        {
            return true;
        }
        if record.goal_ids.iter().any(|goal| goal_ids.contains(goal)) {
            return true;
        }

        let haystack = record_search_text(record);
        activation_keywords(&task)
            .iter()
            .any(|keyword| haystack.contains(keyword))
    }
}

fn promotion_memory_summary(
    candidate: &EvolutionCandidate,
    receipt: &EvolutionPromotionReceipt,
) -> String {
    let scope = if candidate.scope.is_empty() {
        "scope=unspecified".to_string()
    } else {
        format!("scope={}", candidate.scope.join(","))
    };
    let gates = if candidate.adoption_gate.is_empty() {
        "gates=unspecified".to_string()
    } else {
        format!("gates={}", candidate.adoption_gate.join(" | "))
    };
    format!(
        "promotion_reason={}; candidate_kind={}; expected_change={}; {}; {}",
        receipt.reason,
        candidate.kind.as_str(),
        candidate.expected_change,
        scope,
        gates
    )
}

fn record_search_text(record: &EvolutionMemoryRecord) -> String {
    [
        record.kind.as_str(),
        record.summary.as_str(),
        record.scope.scope_id.as_str(),
        record.scope.owner.as_str(),
        &record.scope.goal_ids.join(" "),
        &record.goal_ids.join(" "),
        &record.activation_policy.join(" "),
        &record.evidence_refs.join(" "),
    ]
    .join(" ")
    .to_ascii_lowercase()
}

fn activation_keywords(task: &str) -> Vec<&'static str> {
    let mut keywords = Vec::new();
    if task.contains("tool")
        || task.contains("工具")
        || task.contains("loop")
        || task.contains("循环")
        || task.contains("重复")
        || task.contains("新颖")
        || task.contains("novelty")
    {
        keywords.extend([
            "runtime_policy",
            "tool_loop",
            "low_novelty",
            "low-novelty",
            "loop",
            "工具",
            "循环",
            "新颖",
        ]);
    }
    if task.contains("context") || task.contains("上下文") || task.contains("压缩") {
        keywords.extend(["context_policy", "context", "上下文", "compression", "压缩"]);
    }
    if task.contains("memory") || task.contains("记忆") || task.contains("召回") {
        keywords.extend(["memory_governance", "memory", "记忆", "recall", "召回"]);
    }
    if task.contains("agent") || task.contains("团队") || task.contains("协同") {
        keywords.extend(["team_template", "agent", "team", "团队", "协同"]);
    }
    if task.contains("eval") || task.contains("评测") || task.contains("测试") {
        keywords.extend(["eval_scenario", "eval", "evaluation", "评测"]);
    }
    keywords
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EvolutionCandidate, EvolutionCandidateKind, EvolutionCandidateStatus,
        EvolutionPromotionReceipt, EvolutionVersionRecord,
    };

    fn candidate() -> EvolutionCandidate {
        EvolutionCandidate {
            candidate_id: "candidate-1".to_string(),
            mission_id: None,
            proposal_id: "proposal-1".to_string(),
            goal_ids: Vec::new(),
            kind: EvolutionCandidateKind::RuntimePolicy,
            owner: "runtime".to_string(),
            scope: vec!["crates/runtime/src/conversation".to_string()],
            trigger_signal_ids: vec!["signal-1".to_string()],
            affected_files_or_modules: vec!["crates/runtime/src/conversation".to_string()],
            generated_artifacts: Vec::new(),
            eval_scenario_ids: Vec::new(),
            promotion_adapter: "runtime_policy_overlay".to_string(),
            autonomy_level: "sandbox_only".to_string(),
            risk_boundaries: Vec::new(),
            approval_required: true,
            baseline_ref: "baseline".to_string(),
            candidate_ref: "candidate".to_string(),
            target_owner: "runtime".to_string(),
            target_files_or_modules: vec!["crates/runtime/src/conversation".to_string()],
            artifact_root: None,
            baseline_command: "cargo metadata --format-version 1 --no-deps".to_string(),
            candidate_command: "cargo metadata --format-version 1 --no-deps".to_string(),
            verification_command: "cargo metadata --format-version 1 --no-deps".to_string(),
            artifact_path: None,
            expected_change: "reduce low novelty tool loop before repeating file reads".to_string(),
            adoption_gate: vec!["loop guard evidence exists".to_string()],
            rollback_strategy: "archive applied policy".to_string(),
            status: EvolutionCandidateStatus::ApprovedForAdoption,
            mainline_modified: false,
            human_approval_required: true,
            comparison_report_ref: Some("comparison-1".to_string()),
            version_record_ref: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    fn promotion(candidate: &EvolutionCandidate) -> EvolutionPromotionReceipt {
        EvolutionPromotionReceipt {
            promotion_id: "promotion-1".to_string(),
            candidate_id: candidate.candidate_id.clone(),
            adapter: candidate.promotion_adapter.clone(),
            accepted: true,
            reason: "candidate decision accepted by adoption manager".to_string(),
            version_record: Some(EvolutionVersionRecord::from_candidate(candidate)),
            adoption_receipt: crate::EvolutionAdoptionReceipt {
                receipt_id: "adoption-1".to_string(),
                candidate_id: candidate.candidate_id.clone(),
                requested_status: EvolutionCandidateStatus::ApprovedForAdoption,
                accepted: true,
                reason: "candidate decision accepted by adoption manager".to_string(),
                required_eval_id: Some("comparison-1".to_string()),
                comparison_report_ref: Some("comparison-1".to_string()),
                rollback_strategy: candidate.rollback_strategy.clone(),
                mainline_modified: false,
                created_at_ms: 1,
            },
            created_at_ms: 1,
        }
    }

    #[test]
    fn promotion_memory_preserves_candidate_semantics() {
        let candidate = candidate();
        let record = EvolutionMemoryBridge::from_promotion(&candidate, &promotion(&candidate));

        assert!(record.summary.contains("runtime_policy"));
        assert!(record.summary.contains("expected_change="));
        assert!(EvolutionMemoryBridge::should_activate(
            &record,
            "低新颖度工具循环治理",
            &[],
        ));
    }
}
