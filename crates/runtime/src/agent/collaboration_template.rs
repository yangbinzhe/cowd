//! Strategy-facing template selection.
//!
//! This is deliberately only a semantic matcher. Role topology, scheduling,
//! memory writes, and execution belong respectively to the versioned protocol
//! registry, ExecutionGraphRunner, and Memory maintenance pipeline.

use harness_contract::core::{ExecutionModifier, ExecutionPattern, TaskComplexity, TaskRisk};
use harness_contract::strategy::{StrategyDecision, TaskDomain};
use harness_contract::team::TeamTemplateId;
use serde::{Deserialize, Serialize};

pub use harness_contract::team::TeamTemplateId as CollaborationTemplateId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationDecision {
    pub template_id: TeamTemplateId,
    /// A versioned protocol is selected only for the templates V6 can compile.
    /// V5 TeamRuntime templates remain graph-owned Team commands.
    pub protocol_id: Option<String>,
    pub rationale: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CollaborationTemplateMatcher;

impl CollaborationTemplateMatcher {
    #[must_use]
    pub fn decide(&self, user_input: &str, strategy: &StrategyDecision) -> CollaborationDecision {
        let normalized = user_input.to_ascii_lowercase();
        let (template_id, rationale) = if contains_any(
            &normalized,
            &[
                "incident",
                "outage",
                "production",
                "p0",
                "p1",
                "rollback",
                "故障",
                "事故",
                "线上",
                "回滚",
            ],
        ) || matches!(
            strategy.understanding.risk,
            TaskRisk::Critical
        ) {
            (
                TeamTemplateId::IncidentResponse,
                "critical or incident-like task needs typed triage and mitigation evidence",
            )
        } else if contains_any(
            &normalized,
            &[
                "long-running",
                "roadmap",
                "milestone",
                "multi-stage",
                "长期",
                "阶段",
                "里程碑",
                "全盘",
            ],
        ) || matches!(strategy.understanding.complexity, TaskComplexity::Strategic)
        {
            (
                TeamTemplateId::LongRunningProject,
                "strategic work belongs to the Mission/Schedule protocol",
            )
        } else if contains_any(
            &normalized,
            &["tradeoff", "pros", "cons", "debate", "是否", "利弊", "权衡"],
        ) || matches!(strategy.understanding.domain, TaskDomain::Architecture)
            && !strategy.understanding.requires_write
        {
            (
                TeamTemplateId::DebateConsensus,
                "material tradeoff needs evidence arbitration rather than string consensus",
            )
        } else if contains_any(
            &normalized,
            &[
                "research",
                "compare",
                "investigate",
                "survey",
                "调研",
                "研究",
                "对比",
                "分析",
            ],
        ) || strategy.pattern == ExecutionPattern::Explore
            || strategy.uses_modifier(ExecutionModifier::WithExternalResearch)
        {
            (
                TeamTemplateId::FanoutResearchSynthesis,
                "independent evidence work can use the V5 fanout Team graph",
            )
        } else if contains_any(
            &normalized,
            &[
                "implement",
                "refactor",
                "fix",
                "compile",
                "test",
                "落地",
                "实现",
                "重构",
                "修复",
                "编译",
                "测试",
            ],
        ) || strategy.understanding.requires_write
            || matches!(
                strategy.understanding.domain,
                TaskDomain::Bugfix | TaskDomain::Backend | TaskDomain::Frontend | TaskDomain::Test
            )
        {
            (
                TeamTemplateId::ImplementationReviewFix,
                "write-oriented work needs the review-fix graph protocol",
            )
        } else if matches!(
            strategy.pattern,
            ExecutionPattern::Execute | ExecutionPattern::Collaborate
        ) || strategy.uses_modifier(ExecutionModifier::WithVerifier)
        {
            (
                TeamTemplateId::ExecuteReview,
                "bounded work can use the V5 execute-review Team graph",
            )
        } else {
            (
                TeamTemplateId::SingleExecutor,
                "simple low-risk work should avoid coordination overhead",
            )
        };
        CollaborationDecision {
            template_id,
            protocol_id: protocol_id(template_id).map(str::to_string),
            rationale: rationale.to_string(),
        }
    }
}

#[must_use]
pub const fn protocol_id(template_id: TeamTemplateId) -> Option<&'static str> {
    match template_id {
        TeamTemplateId::DebateConsensus => Some("debate@1"),
        TeamTemplateId::ImplementationReviewFix => Some("review_fix@1"),
        TeamTemplateId::IncidentResponse => Some("incident@1"),
        TeamTemplateId::SingleExecutor
        | TeamTemplateId::ExecuteReview
        | TeamTemplateId::FanoutResearchSynthesis
        | TeamTemplateId::LongRunningProject => None,
    }
}

fn contains_any(input: &str, terms: &[&str]) -> bool {
    terms.iter().any(|term| input.contains(term))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::strategy::{decide_strategy, StrategyInput};

    fn matches(prompt: &str, expected: TeamTemplateId) {
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        assert_eq!(
            CollaborationTemplateMatcher
                .decide(prompt, &strategy)
                .template_id,
            expected
        );
    }

    #[test]
    fn selects_protocol_templates_without_embedded_role_loops() {
        matches("分析架构取舍和利弊", TeamTemplateId::DebateConsensus);
        matches(
            "重构并修复这个模块",
            TeamTemplateId::ImplementationReviewFix,
        );
        matches("线上事故需要回滚", TeamTemplateId::IncidentResponse);
        let strategy = decide_strategy(&StrategyInput::from_prompt("分析架构取舍"));
        assert_eq!(
            CollaborationTemplateMatcher
                .decide("分析架构取舍", &strategy)
                .protocol_id
                .as_deref(),
            Some("debate@1")
        );
    }
}
