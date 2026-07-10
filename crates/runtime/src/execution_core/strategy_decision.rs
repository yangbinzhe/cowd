use harness_contract::core::{
    ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, TaskComplexity, TaskRisk,
};
use harness_contract::strategy::{
    decide_strategy, CollaborationLiftEstimate, StrategyDecision, StrategyInput,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::pattern_catalog::{ExecutionPatternCatalog, RuntimeCompileTarget};
use crate::collaboration_template::{CollaborationTemplateId, CollaborationTemplateMatcher};
use crate::context_runtime::ContextProfile;
use crate::evidence_planner::EvidenceAcquisitionMode;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionDecision {
    pub decision_id: String,
    pub user_intent_preview: String,
    pub strategy: StrategyDecision,
    pub candidate_patterns: Vec<RuntimeExecutionPatternCandidate>,
    pub evidence_mode: EvidenceAcquisitionMode,
    pub recommended_template: Option<CollaborationTemplateId>,
    pub recommended_actions: Vec<RuntimeExecutionActionHint>,
    pub compile_target: RuntimeCompileTarget,
    pub resource_health: StrategyResourceHealth,
    pub lease: StrategyLease,
    pub executable: bool,
    pub blocked_reasons: Vec<String>,
    pub confidence: f32,
    pub reasons: Vec<String>,
}

impl RuntimeExecutionDecision {
    #[must_use]
    pub const fn pattern(&self) -> ExecutionPattern {
        self.strategy.pattern
    }

    #[must_use]
    pub fn modifiers(&self) -> &[ExecutionModifier] {
        &self.strategy.modifiers
    }

    #[must_use]
    pub fn gates(&self) -> &[ExecutionPolicyGate] {
        &self.strategy.gates
    }

    #[must_use]
    pub const fn collaboration_lift(&self) -> &CollaborationLiftEstimate {
        &self.strategy.collaboration_lift
    }

    #[must_use]
    pub const fn risk(&self) -> TaskRisk {
        self.strategy.understanding.risk
    }

    #[must_use]
    pub const fn complexity(&self) -> TaskComplexity {
        self.strategy.understanding.complexity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyResourceHealth {
    pub provider_available: bool,
    pub tools_available: bool,
    pub collaboration_available: bool,
    pub mission_available: bool,
    pub observed: bool,
}

impl Default for StrategyResourceHealth {
    fn default() -> Self {
        Self {
            provider_available: true,
            tools_available: true,
            collaboration_available: true,
            mission_available: true,
            observed: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyLease {
    pub lease_id: String,
    pub input_fingerprint: u64,
    pub locked_pattern: ExecutionPattern,
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
        self.decide_with_input(
            StrategyInput::from_prompt(user_input),
            context_profile,
            StrategyResourceHealth::default(),
        )
    }

    #[must_use]
    pub fn decide_with_input(
        &self,
        input: StrategyInput,
        context_profile: Option<ContextProfile>,
        resource_health: StrategyResourceHealth,
    ) -> RuntimeExecutionDecision {
        build_runtime_execution_decision_inner(input, context_profile, resource_health)
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
    input: StrategyInput,
    context_profile: Option<ContextProfile>,
    resource_health: StrategyResourceHealth,
) -> RuntimeExecutionDecision {
    let user_input = input.prompt.clone();
    let requested_template = input
        .proposal
        .as_ref()
        .and_then(|proposal| proposal.template.clone());
    let mut strategy = decide_strategy(&input);
    let mut blocked_reasons = Vec::new();
    if strategy.pattern == ExecutionPattern::Collaborate && !resource_health.collaboration_available
    {
        if let Err(error) = strategy.retarget(
            ExecutionPattern::Execute,
            "collaboration backend unavailable; compiled as execution graph",
        ) {
            blocked_reasons.push(error);
        }
    }
    if strategy.pattern == ExecutionPattern::Supervise && !resource_health.mission_available {
        if let Err(error) = strategy.retarget(
            ExecutionPattern::Execute,
            "mission runtime unavailable; compiled as bounded execution graph",
        ) {
            blocked_reasons.push(error);
        }
    }
    if !resource_health.provider_available {
        blocked_reasons.push("provider runtime unavailable".to_string());
    }
    if !resource_health.tools_available
        && matches!(
            strategy.pattern,
            ExecutionPattern::Explore | ExecutionPattern::Execute
        )
    {
        let unavailable_pattern = strategy.pattern;
        if let Err(error) = strategy.retarget(
            ExecutionPattern::Direct,
            format!(
                "tool runtime unavailable for {}; using the contract-safe model-only path",
                unavailable_pattern.as_str()
            ),
        ) {
            blocked_reasons.push(format!(
                "tool runtime unavailable for {} compile target: {error}",
                unavailable_pattern.as_str()
            ));
        }
    }
    if !resource_health.observed {
        strategy
            .reasons
            .push("resource health is assumed for detached planning".to_string());
    }
    let template_decision = CollaborationTemplateMatcher::default().decide(&user_input, &strategy);
    let template_reason = requested_template.map(|requested| {
        if requested == template_decision.template_id.as_str() {
            format!("validated model template proposal: {requested}")
        } else {
            format!(
                "model template proposal `{requested}` rejected; strategy selected `{}`",
                template_decision.template_id.as_str()
            )
        }
    });
    let recommended_pattern = strategy.pattern;
    let mut candidate_patterns = vec![RuntimeExecutionPatternCandidate {
        pattern: recommended_pattern,
        why: "canonical strategy decision".to_string(),
    }];
    let evidence_mode = evidence_mode_for_strategy(&strategy);
    if evidence_mode == EvidenceAcquisitionMode::ComplexEvidence
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
        strategy: strategy.clone(),
        candidate_patterns,
        evidence_mode,
        recommended_template: Some(template_decision.template_id),
        recommended_actions: if blocked_reasons.is_empty() {
            action_hints(
                recommended_pattern,
                &strategy.modifiers,
                &strategy.gates,
                &template_decision.template_id,
            )
        } else {
            Vec::new()
        },
        compile_target: recommended_spec.map_or(RuntimeCompileTarget::InlineModel, |spec| {
            spec.compile_target
        }),
        resource_health,
        lease: StrategyLease {
            lease_id: format!("strategy-lease-{}", Uuid::new_v4()),
            input_fingerprint: model_protocol::prompt_cache::stable_hash_bytes(
                user_input.as_bytes(),
            ),
            locked_pattern: recommended_pattern,
        },
        executable: blocked_reasons.is_empty(),
        blocked_reasons,
        confidence: f32::from(strategy.confidence) / 100.0,
        reasons: strategy
            .reasons
            .into_iter()
            .chain(recommended_spec.map(|spec| spec.summary.clone()))
            .chain(template_reason)
            .chain(context_profile.map(|profile| format!("context profile: {profile:?}")))
            .collect(),
    }
}

fn evidence_mode_for_strategy(strategy: &StrategyDecision) -> EvidenceAcquisitionMode {
    match strategy.pattern {
        ExecutionPattern::Direct => EvidenceAcquisitionMode::SmallEvidence,
        ExecutionPattern::Explore => {
            if matches!(
                strategy.understanding.complexity,
                TaskComplexity::Complex | TaskComplexity::Strategic
            ) {
                EvidenceAcquisitionMode::ComplexEvidence
            } else {
                EvidenceAcquisitionMode::LargeEvidence
            }
        }
        ExecutionPattern::Execute => EvidenceAcquisitionMode::MediumEvidence,
        ExecutionPattern::Deliberate
        | ExecutionPattern::Collaborate
        | ExecutionPattern::Supervise => EvidenceAcquisitionMode::ComplexEvidence,
    }
}

#[must_use]
pub fn build_runtime_action_selection_report(
    user_input: &str,
    context_profile: Option<ContextProfile>,
) -> RuntimeActionSelectionReport {
    let decision = build_runtime_execution_decision(user_input, context_profile);
    action_selection_report_for_decision(&decision, context_profile)
}

#[must_use]
pub fn action_selection_report_for_decision(
    decision: &RuntimeExecutionDecision,
    context_profile: Option<ContextProfile>,
) -> RuntimeActionSelectionReport {
    if !decision.executable {
        return RuntimeActionSelectionReport {
            intent_preview: decision.user_intent_preview.clone(),
            profile: context_profile,
            selected_action: "blocked".to_string(),
            fallback_action: "blocked".to_string(),
            recommended_pattern: decision.pattern(),
            recommended_template: decision.recommended_template,
            expected_projection: vec!["runtime.execution_decision".to_string()],
            stateful: false,
            reason: decision.blocked_reasons.join("; "),
            confidence: decision.confidence,
        };
    }
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
        intent_preview: decision.user_intent_preview.clone(),
        profile: context_profile,
        selected_action: selected.action.clone(),
        fallback_action: fallback_action_for(&selected.action).to_string(),
        recommended_pattern: decision.pattern(),
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
        assert_eq!(decision.pattern(), ExecutionPattern::Collaborate);
        assert_eq!(decision.recommended_actions[0].action, "request_team");
    }

    #[test]
    fn direct_work_has_no_stateful_action() {
        let report = build_runtime_action_selection_report("解释这个名称", None);
        assert_eq!(report.selected_action, "direct");
        assert!(!report.stateful);
    }

    #[test]
    fn resource_downgrade_retargets_the_whole_strategy_contract() {
        let decision = StrategyDecisionEngine.decide_with_input(
            StrategyInput::from_prompt("使用多 Agent 并行审查 runtime gateway memory 并汇总结果"),
            None,
            StrategyResourceHealth {
                collaboration_available: false,
                ..StrategyResourceHealth::default()
            },
        );

        assert_eq!(decision.pattern(), ExecutionPattern::Execute);
        assert_eq!(
            decision.compile_target,
            RuntimeCompileTarget::ExecutionGraph
        );
        assert!(decision
            .modifiers()
            .iter()
            .all(|modifier| ExecutionPattern::Execute.supports_modifier(*modifier)));
        assert!(decision
            .gates()
            .iter()
            .all(|gate| ExecutionPattern::Execute.supports_gate(*gate)));
        assert!(decision.executable);
    }

    #[test]
    fn unavailable_tools_block_execution_without_dropping_policy_gates() {
        let decision = StrategyDecisionEngine.decide_with_input(
            StrategyInput::from_prompt("修改 runtime 的高风险权限实现，并执行工具验证全部变更")
                .with_explicit_write(true)
                .with_risk_override(TaskRisk::Critical),
            None,
            StrategyResourceHealth {
                tools_available: false,
                observed: true,
                ..StrategyResourceHealth::default()
            },
        );

        assert_eq!(decision.pattern(), ExecutionPattern::Execute);
        assert!(!decision.executable);
        assert!(decision.recommended_actions.is_empty());
        assert!(decision.gates().contains(&ExecutionPolicyGate::Permission));
        assert!(decision.gates().contains(&ExecutionPolicyGate::Risk));
        assert!(decision.gates().contains(&ExecutionPolicyGate::Approval));
        assert!(decision
            .blocked_reasons
            .iter()
            .any(|reason| reason.contains("tool runtime unavailable")));
        let report = action_selection_report_for_decision(&decision, None);
        assert_eq!(report.selected_action, "blocked");
        assert_eq!(report.fallback_action, "blocked");
        assert!(!report.stateful);
    }

    #[test]
    fn unavailable_tools_only_degrade_when_direct_preserves_required_gates() {
        let decision = StrategyDecisionEngine.decide_with_input(
            StrategyInput::from_prompt("并行检查 README 的说明并总结"),
            None,
            StrategyResourceHealth {
                tools_available: false,
                observed: true,
                ..StrategyResourceHealth::default()
            },
        );

        assert_eq!(decision.pattern(), ExecutionPattern::Direct);
        assert!(decision.executable);
        assert!(decision.blocked_reasons.is_empty());
        assert_eq!(decision.gates(), &[ExecutionPolicyGate::Budget]);
    }

    #[test]
    fn unavailable_provider_blocks_even_the_direct_fast_path() {
        let decision = StrategyDecisionEngine.decide_with_input(
            StrategyInput::from_prompt("解释这个名称"),
            None,
            StrategyResourceHealth {
                provider_available: false,
                observed: true,
                ..StrategyResourceHealth::default()
            },
        );

        assert_eq!(decision.pattern(), ExecutionPattern::Direct);
        assert!(!decision.executable);
        assert!(decision.recommended_actions.is_empty());
        assert_eq!(
            decision.blocked_reasons,
            vec!["provider runtime unavailable".to_string()]
        );
    }

    #[test]
    fn collaboration_backend_does_not_require_the_turn_tool_host() {
        let decision = StrategyDecisionEngine.decide_with_input(
            StrategyInput::from_prompt(
                "use multi-agent collaboration to review runtime, gateway, and memory",
            ),
            None,
            StrategyResourceHealth {
                tools_available: false,
                collaboration_available: true,
                observed: true,
                ..StrategyResourceHealth::default()
            },
        );

        assert_eq!(decision.pattern(), ExecutionPattern::Collaborate);
        assert!(decision.executable);
        assert!(decision.blocked_reasons.is_empty());
    }
}
