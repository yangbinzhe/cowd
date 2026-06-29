use harness_contract::core::{ExecutionMode, StrategyDecorator, TaskRisk};
use harness_contract::strategy::{decide_strategy, StrategyInput};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::mode_catalog::ExecutionModeCatalog;
use crate::collaboration_template::{CollaborationTemplateId, CollaborationTemplateMatcher};
use crate::context_runtime::ContextProfile;
use crate::evidence_planner::{plan_evidence, EvidenceAcquisitionMode};
use crate::runtime_control::{ComplexityLevel, RuntimeControlPolicy, TaskComplexityInput};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionDecision {
    pub decision_id: String,
    pub user_intent_preview: String,
    pub recommended_mode: ExecutionMode,
    pub candidate_modes: Vec<RuntimeExecutionModeCandidate>,
    pub selected_decorators: Vec<StrategyDecorator>,
    pub evidence_mode: EvidenceAcquisitionMode,
    pub recommended_template: Option<CollaborationTemplateId>,
    pub recommended_actions: Vec<RuntimeExecutionActionHint>,
    pub risk: TaskRisk,
    pub complexity: ComplexityLevel,
    pub confidence: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionModeCandidate {
    pub mode: ExecutionMode,
    pub why: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionActionHint {
    pub action: String,
    pub template_hint: Option<String>,
    pub reason: String,
}

#[must_use]
pub fn build_runtime_execution_decision(
    user_input: &str,
    context_profile: Option<ContextProfile>,
) -> RuntimeExecutionDecision {
    let strategy = decide_strategy(&StrategyInput::from_prompt(user_input));
    let evidence = plan_evidence(user_input);
    let complexity_profile =
        RuntimeControlPolicy::default().profile_task(&TaskComplexityInput::new(
            user_input.to_string(),
            context_profile.unwrap_or(ContextProfile::MainTurn),
        ));
    let template_decision = CollaborationTemplateMatcher::default().decide(user_input, &strategy);
    let recommended_mode = select_mode(strategy.mode, evidence.mode, complexity_profile.level);
    let mut candidate_modes = vec![RuntimeExecutionModeCandidate {
        mode: recommended_mode,
        why: "runtime strategy, evidence profile, and task complexity agree on this mode"
            .to_string(),
    }];
    if evidence.mode == EvidenceAcquisitionMode::ComplexEvidence {
        candidate_modes.push(RuntimeExecutionModeCandidate {
            mode: ExecutionMode::ParallelReadFanout,
            why: "complex evidence can be gathered in independent read-only branches".to_string(),
        });
    }
    if template_decision.template_id == CollaborationTemplateId::DebateConsensus {
        candidate_modes.push(RuntimeExecutionModeCandidate {
            mode: ExecutionMode::DeliberationSearch,
            why: "decision-heavy work benefits from debate and consensus".to_string(),
        });
    }

    let catalog = ExecutionModeCatalog::current();
    let recommended_spec = catalog.find(recommended_mode);
    RuntimeExecutionDecision {
        decision_id: format!("execution-decision-{}", Uuid::new_v4()),
        user_intent_preview: user_input.chars().take(180).collect(),
        recommended_mode,
        candidate_modes,
        selected_decorators: strategy.decorators.clone(),
        evidence_mode: evidence.mode,
        recommended_template: Some(template_decision.template_id),
        recommended_actions: action_hints(recommended_mode, &template_decision.template_id),
        risk: strategy.understanding.risk,
        complexity: complexity_profile.level,
        confidence: f32::from(strategy.confidence) / 100.0,
        reasons: strategy
            .reasons
            .into_iter()
            .chain(recommended_spec.map(|spec| spec.summary.clone()))
            .collect(),
    }
}

fn select_mode(
    strategy_mode: ExecutionMode,
    evidence_mode: EvidenceAcquisitionMode,
    complexity: ComplexityLevel,
) -> ExecutionMode {
    match (strategy_mode, evidence_mode, complexity) {
        (ExecutionMode::RiskGate | ExecutionMode::HumanConfirm, _, _) => strategy_mode,
        (
            _,
            EvidenceAcquisitionMode::ComplexEvidence,
            ComplexityLevel::Complex | ComplexityLevel::Critical,
        ) => ExecutionMode::SupervisorSubagents,
        (_, EvidenceAcquisitionMode::ComplexEvidence, _) => ExecutionMode::ParallelReadFanout,
        (ExecutionMode::ReActLoop, EvidenceAcquisitionMode::SmallEvidence, _) => {
            ExecutionMode::ParallelReadFanout
        }
        _ => strategy_mode,
    }
}

fn action_hints(
    mode: ExecutionMode,
    template_hint: &CollaborationTemplateId,
) -> Vec<RuntimeExecutionActionHint> {
    let template = Some(template_hint.as_str().to_string());
    match mode {
        ExecutionMode::DirectAnswer => Vec::new(),
        ExecutionMode::ParallelReadFanout => vec![RuntimeExecutionActionHint {
            action: "request_rewoo_evidence".to_string(),
            template_hint: template,
            reason: "batch independent evidence before answering".to_string(),
        }],
        ExecutionMode::DeliberationSearch => vec![RuntimeExecutionActionHint {
            action: "request_deliberation".to_string(),
            template_hint: Some("debate_consensus".to_string()),
            reason: "compare options and merge a defensible decision".to_string(),
        }],
        ExecutionMode::ReflexionRetry => vec![RuntimeExecutionActionHint {
            action: "request_reflexion_retry".to_string(),
            template_hint: template,
            reason: "recover from repeated or failed execution".to_string(),
        }],
        ExecutionMode::SupervisorSubagents
        | ExecutionMode::PlanExecute
        | ExecutionMode::BackgroundReview
        | ExecutionMode::ParallelWorktree => vec![RuntimeExecutionActionHint {
            action: "request_team".to_string(),
            template_hint: template,
            reason: "runtime-owned team/subagent orchestration is suitable".to_string(),
        }],
        ExecutionMode::RiskGate | ExecutionMode::HumanConfirm => vec![RuntimeExecutionActionHint {
            action: "request_risk_gate".to_string(),
            template_hint: None,
            reason: "policy gate or human confirmation is required".to_string(),
        }],
        _ => vec![RuntimeExecutionActionHint {
            action: "plan_only".to_string(),
            template_hint: template,
            reason: "ask runtime for a bounded execution plan before acting".to_string(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_execution_decision_selects_parallel_or_team_for_complex_work() {
        let decision = build_runtime_execution_decision(
            "全盘分析架构并沉浸式实现、审查、测试和回归",
            Some(ContextProfile::DeepInvestigation),
        );
        assert!(matches!(
            decision.recommended_mode,
            ExecutionMode::SupervisorSubagents
                | ExecutionMode::ParallelReadFanout
                | ExecutionMode::PlanExecute
        ));
        assert!(!decision.recommended_actions.is_empty());
    }
}
