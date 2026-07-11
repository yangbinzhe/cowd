//! Behavior policy checks for avoiding over-engineering while preserving safety.

use crate::core::{ExecutionPattern, TaskRisk};
use crate::strategy::StrategyDecision;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendedScope {
    Direct,
    MinimalPatch,
    PlannedChange,
    ExecutionGraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorPolicyDecision {
    pub necessity: String,
    pub reuse_opportunities: Vec<String>,
    pub overengineering_risks: Vec<String>,
    pub safety_exceptions: Vec<String>,
    pub recommended_scope: RecommendedScope,
    pub enforcement: BehaviorPolicyEnforcement,
    pub eval_checks: Vec<String>,
}

impl BehaviorPolicyDecision {
    #[must_use]
    pub fn has_overengineering_risk(&self) -> bool {
        !self.overengineering_risks.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorPolicyEnforcement {
    pub allow_execution: bool,
    pub requires_scope_downgrade: bool,
    pub requires_human_review: bool,
}

#[must_use]
pub fn decide_behavior_policy(prompt: &str, strategy: &StrategyDecision) -> BehaviorPolicyDecision {
    let prompt_lower = prompt.to_ascii_lowercase();
    let mut reuse_opportunities = Vec::new();
    let mut overengineering_risks = Vec::new();
    let mut safety_exceptions = Vec::new();

    if prompt_lower.contains("重构")
        || prompt_lower.contains("refactor")
        || prompt_lower.contains("implement")
        || prompt_lower.contains("修复")
    {
        reuse_opportunities
            .push("reuse existing modules and service owners before adding crates".to_string());
        reuse_opportunities.push(
            "prefer existing runtime trace, memory, matrix, policy and eval chains".to_string(),
        );
    }

    if matches!(
        strategy.pattern,
        ExecutionPattern::Collaborate | ExecutionPattern::Deliberate | ExecutionPattern::Supervise
    ) && !prompt_lower.contains("复杂")
        && !prompt_lower.contains("全量")
        && !prompt_lower.contains("架构")
    {
        overengineering_risks
            .push("heavy execution mode selected without explicit complexity signal".to_string());
    }

    if matches!(
        strategy.understanding.risk,
        TaskRisk::High | TaskRisk::Critical
    ) || prompt_lower.contains("安全")
        || prompt_lower.contains("权限")
        || prompt_lower.contains("数据")
    {
        safety_exceptions.push(
            "minimal-scope policy must not remove validation, approval, data protection or tests"
                .to_string(),
        );
    }

    let recommended_scope = match strategy.pattern {
        ExecutionPattern::Direct => RecommendedScope::Direct,
        ExecutionPattern::Explore => RecommendedScope::Direct,
        ExecutionPattern::Execute => RecommendedScope::PlannedChange,
        ExecutionPattern::Deliberate
        | ExecutionPattern::Collaborate
        | ExecutionPattern::Supervise => RecommendedScope::ExecutionGraph,
    };
    let requires_scope_downgrade = !overengineering_risks.is_empty();
    let requires_human_review = requires_scope_downgrade
        && matches!(
            strategy.understanding.risk,
            TaskRisk::High | TaskRisk::Critical
        );

    BehaviorPolicyDecision {
        necessity: "perform the smallest change that satisfies the requested capability without removing safety gates".to_string(),
        reuse_opportunities,
        overengineering_risks,
        safety_exceptions,
        recommended_scope,
        enforcement: BehaviorPolicyEnforcement {
            allow_execution: true,
            requires_scope_downgrade,
            requires_human_review,
        },
        eval_checks: vec![
            "minimal_scope".to_string(),
            "reuse_existing".to_string(),
            "safety_preserved".to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::{decide_strategy, StrategyInput};

    #[test]
    fn refactor_prompt_gets_reuse_guidance() {
        let strategy = decide_strategy(&StrategyInput::from_prompt("重构 runtime"));
        let decision = decide_behavior_policy("重构 runtime", &strategy);
        assert!(!decision.reuse_opportunities.is_empty());
    }
}
