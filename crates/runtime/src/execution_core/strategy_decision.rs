use harness_contract::core::{
    ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, TaskComplexity, TaskRisk,
};
use harness_contract::strategy::{decide_strategy, CollaborationLiftEstimate, StrategyInput};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::pattern_catalog::ExecutionPatternCatalog;
use crate::collaboration_template::{CollaborationTemplateId, CollaborationTemplateMatcher};
use crate::context_runtime::ContextProfile;
use crate::evidence_planner::{plan_evidence, EvidenceAcquisitionMode};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionDecision {
    pub decision_id: String,
    pub user_intent_preview: String,
    pub recommended_pattern: ExecutionPattern,
    pub candidate_patterns: Vec<RuntimeExecutionPatternCandidate>,
    pub selected_modifiers: Vec<ExecutionModifier>,
    pub selected_gates: Vec<ExecutionPolicyGate>,
    pub evidence_mode: EvidenceAcquisitionMode,
    pub recommended_template: Option<CollaborationTemplateId>,
    pub recommended_actions: Vec<RuntimeExecutionActionHint>,
    pub collaboration_lift: CollaborationLiftEstimate,
    pub risk: TaskRisk,
    pub complexity: TaskComplexity,
    pub confidence: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionPatternCandidate {
    pub pattern: ExecutionPattern,
    pub why: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionActionHint {
    pub action: String,
    pub template_hint: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeActionSelectionReport {
    pub intent_preview: String,
    pub profile: Option<ContextProfile>,
    pub selected_action: String,
    pub fallback_action: String,
    pub recommended_pattern: ExecutionPattern,
    pub recommended_template: Option<CollaborationTemplateId>,
    pub expected_projection: Vec<String>,
    pub stateful: bool,
    pub reason: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Default)]
pub struct StrategyDecisionEngine;

impl StrategyDecisionEngine {
    #[must_use]
    pub fn decide(
        &self,
        user_input: &str,
        context_profile: Option<ContextProfile>,
    ) -> RuntimeExecutionDecision {
        build_runtime_execution_decision_inner(user_input, context_profile)
    }
}

#[must_use]
pub fn build_runtime_execution_decision(
    user_input: &str,
    context_profile: Option<ContextProfile>,
) -> RuntimeExecutionDecision {
    StrategyDecisionEngine.decide(user_input, context_profile)
}

fn build_runtime_execution_decision_inner(
    user_input: &str,
    context_profile: Option<ContextProfile>,
) -> RuntimeExecutionDecision {
    let strategy = decide_strategy(&StrategyInput::from_prompt(user_input));
    let evidence = plan_evidence(user_input);
    let template_decision = CollaborationTemplateMatcher::default().decide(user_input, &strategy);
    let recommended_pattern = strategy.pattern;
    let mut candidate_patterns = vec![RuntimeExecutionPatternCandidate {
        pattern: recommended_pattern,
        why: "canonical strategy decision".to_string(),
    }];
    if evidence.mode == EvidenceAcquisitionMode::ComplexEvidence
        && recommended_pattern != ExecutionPattern::Explore
    {
        candidate_patterns.push(RuntimeExecutionPatternCandidate {
            pattern: ExecutionPattern::Explore,
            why: "complex evidence can be acquired through an evidence graph".to_string(),
        });
    }
    if template_decision.template_id == CollaborationTemplateId::DebateConsensus
        && recommended_pattern != ExecutionPattern::Deliberate
    {
        candidate_patterns.push(RuntimeExecutionPatternCandidate {
            pattern: ExecutionPattern::Deliberate,
            why: "material tradeoffs can be compiled as a deliberation graph".to_string(),
        });
    }

    let catalog = ExecutionPatternCatalog::current();
    let recommended_spec = catalog.find(recommended_pattern);
    RuntimeExecutionDecision {
        decision_id: format!("execution-decision-{}", Uuid::new_v4()),
        user_intent_preview: user_input.chars().take(180).collect(),
        recommended_pattern,
        candidate_patterns,
        selected_modifiers: strategy.modifiers.clone(),
        selected_gates: strategy.gates.clone(),
        evidence_mode: evidence.mode,
        recommended_template: Some(template_decision.template_id),
        recommended_actions: action_hints(
            recommended_pattern,
            &strategy.modifiers,
            &strategy.gates,
            &template_decision.template_id,
        ),
        collaboration_lift: strategy.collaboration_lift,
        risk: strategy.understanding.risk,
        complexity: strategy.understanding.complexity,
        confidence: f32::from(strategy.confidence) / 100.0,
        reasons: strategy
            .reasons
            .into_iter()
            .chain(recommended_spec.map(|spec| spec.summary.clone()))
            .chain(context_profile.map(|profile| format!("context profile: {profile:?}")))
            .collect(),
    }
}

#[must_use]
pub fn build_runtime_action_selection_report(
    user_input: &str,
    context_profile: Option<ContextProfile>,
) -> RuntimeActionSelectionReport {
    let decision = build_runtime_execution_decision(user_input, context_profile);
    let selected = decision
        .recommended_actions
        .first()
        .cloned()
        .unwrap_or_else(|| RuntimeExecutionActionHint {
            action: "direct".to_string(),
            template_hint: None,
            reason: "the canonical strategy selected the direct fast path".to_string(),
        });
    let stateful = selected.action != "direct";
    RuntimeActionSelectionReport {
        intent_preview: user_input.chars().take(180).collect(),
        profile: context_profile,
        selected_action: selected.action.clone(),
        fallback_action: fallback_action_for(&selected.action).to_string(),
        recommended_pattern: decision.recommended_pattern,
        recommended_template: decision.recommended_template,
        expected_projection: expected_projection_for(&selected.action)
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        stateful,
        reason: selected.reason,
        confidence: decision.confidence,
    }
}

fn fallback_action_for(action: &str) -> &'static str {
    match action {
        "request_team" | "request_deliberation" | "request_background_review" => {
            "request_rewoo_evidence"
        }
        "request_parallel_tools" | "request_rewoo_evidence" => "direct",
        "request_risk_gate" => "direct",
        _ => "direct",
    }
}

fn expected_projection_for(action: &str) -> &'static [&'static str] {
    match action {
        "request_team" => &[
            "mission.team_projection",
            "mission.agent_projection",
            "mission.workgraph_projection",
            "mission.evidence_projection",
        ],
        "request_parallel_tools" | "request_rewoo_evidence" => &[
            "runtime.tool_dag",
            "runtime.tool_schedule",
            "runtime.evidence_refs",
        ],
        "request_deliberation" => &["runtime.deliberation_graph", "mission.evidence_projection"],
        "request_background_review" => &["mission.steward_projection", "runtime.workgraph"],
        "request_risk_gate" => &["mission.conflict_projection", "mission.approval_projection"],
        _ => &["runtime.execution_decision"],
    }
}

fn action_hints(
    pattern: ExecutionPattern,
    modifiers: &[ExecutionModifier],
    gates: &[ExecutionPolicyGate],
    template_hint: &CollaborationTemplateId,
) -> Vec<RuntimeExecutionActionHint> {
    if gates.contains(&ExecutionPolicyGate::Approval) {
        return vec![RuntimeExecutionActionHint {
            action: "request_risk_gate".to_string(),
            template_hint: None,
            reason: "critical execution requires approval before graph dispatch".to_string(),
        }];
    }
    let template = Some(template_hint.as_str().to_string());
    match pattern {
        ExecutionPattern::Direct => Vec::new(),
        ExecutionPattern::Explore => vec![RuntimeExecutionActionHint {
            action: if modifiers.contains(&ExecutionModifier::Parallel) {
                "request_parallel_tools".to_string()
            } else {
                "request_rewoo_evidence".to_string()
            },
            template_hint: template,
            reason: "acquire checked evidence before synthesis".to_string(),
        }],
        ExecutionPattern::Execute => vec![RuntimeExecutionActionHint {
            action: "request_subagent".to_string(),
            template_hint: template,
            reason: "compile a bounded execution and verification graph".to_string(),
        }],
        ExecutionPattern::Deliberate => vec![RuntimeExecutionActionHint {
            action: "request_deliberation".to_string(),
            template_hint: Some("debate_consensus".to_string()),
            reason: "resolve competing options with evidence-backed arbitration".to_string(),
        }],
        ExecutionPattern::Collaborate => vec![RuntimeExecutionActionHint {
            action: "request_team".to_string(),
            template_hint: template,
            reason: "positive collaboration lift supports a governed team graph".to_string(),
        }],
        ExecutionPattern::Supervise => vec![RuntimeExecutionActionHint {
            action: "request_background_review".to_string(),
            template_hint: template,
            reason: "long-running work belongs to a supervised mission graph".to_string(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complex_multi_agent_work_selects_collaboration() {
        let decision = build_runtime_execution_decision(
            "复杂架构需要多 Agent 并行分析 runtime gateway memory 并审查回归",
            Some(ContextProfile::DeepInvestigation),
        );
        assert_eq!(decision.recommended_pattern, ExecutionPattern::Collaborate);
        assert_eq!(decision.recommended_actions[0].action, "request_team");
    }

    #[test]
    fn direct_work_has_no_stateful_action() {
        let report = build_runtime_action_selection_report("解释这个名称", None);
        assert_eq!(report.selected_action, "direct");
        assert!(!report.stateful);
    }
}
