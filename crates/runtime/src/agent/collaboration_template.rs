//! Strategy-facing template selection.
//!
//! This is deliberately only a semantic matcher. Role topology, scheduling,
//! memory writes, and execution belong respectively to the versioned protocol
//! registry, RuntimeExecutionSupervisor, and Memory maintenance pipeline.

use harness_contract::core::{ExecutionModifier, ExecutionPattern, TaskComplexity, TaskRisk};
use harness_contract::strategy::{StrategyDecision, TaskDomain};
use serde::{Deserialize, Serialize};

/// Strategy-level reference to one durable Team Template family.
///
/// This is only a recommendation vocabulary. It never constructs a graph or
/// carries role definitions; Runtime turns it into a versioned
/// `TeamTemplateSelector` before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationTemplateId {
    DirectExecutor,
    PlannerExecutorVerifier,
    ParallelResearchSynthesis,
    ImplementationReviewFix,
    DebateCriticArbiter,
    IncidentResponse,
    MatrixScenarioEnsemble,
    LongRunningWorkstreams,
}

impl CollaborationTemplateId {
    /// Stable template identifier advertised to models and policy contracts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.template_path()
    }

    #[must_use]
    pub const fn template_path(self) -> &'static str {
        match self {
            Self::DirectExecutor => "cowd/direct-executor",
            Self::PlannerExecutorVerifier => "cowd/planner-executor-verifier",
            Self::ParallelResearchSynthesis => "cowd/parallel-research-synthesis",
            Self::ImplementationReviewFix => "cowd/implementation-review-fix",
            Self::DebateCriticArbiter => "cowd/debate-critic-arbiter",
            Self::IncidentResponse => "cowd/incident-response",
            Self::MatrixScenarioEnsemble => "cowd/matrix-scenario-ensemble",
            Self::LongRunningWorkstreams => "cowd/long-running-workstreams",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationDecision {
    pub template_id: CollaborationTemplateId,
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
                CollaborationTemplateId::IncidentResponse,
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
                "全面规划",
                "规划",
                "演进",
                "evolve",
                "roadmap",
            ],
        ) {
            (
                CollaborationTemplateId::LongRunningWorkstreams,
                "explicitly long-running work belongs to the Mission/Schedule protocol",
            )
        } else if contains_any(
            &normalized,
            &[
                "tradeoff", "pros", "cons", "debate", "是否", "利弊", "权衡", "取舍",
            ],
        ) {
            (
                CollaborationTemplateId::DebateCriticArbiter,
                "material tradeoff needs evidence arbitration rather than string consensus",
            )
        } else if contains_any(
            &normalized,
            &[
                "research",
                "researcher",
                "compare",
                "investigate",
                "survey",
                "调研",
                "研究",
                "研究员",
                "对比",
                "分析",
                "并行审查",
            ],
        ) || strategy.pattern == ExecutionPattern::Explore
            || strategy.uses_modifier(ExecutionModifier::WithExternalResearch)
        {
            (
                CollaborationTemplateId::ParallelResearchSynthesis,
                "independent evidence work can use the V5 fanout Team graph",
            )
        } else if matches!(strategy.understanding.domain, TaskDomain::Architecture)
            && !strategy.understanding.requires_write
        {
            (
                CollaborationTemplateId::DebateCriticArbiter,
                "material tradeoff needs evidence arbitration rather than string consensus",
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
                CollaborationTemplateId::ImplementationReviewFix,
                "write-oriented work needs the review-fix graph protocol",
            )
        } else if matches!(strategy.understanding.complexity, TaskComplexity::Strategic) {
            (
                CollaborationTemplateId::LongRunningWorkstreams,
                "strategic work without a more specific protocol uses supervised workstreams",
            )
        } else if matches!(
            strategy.pattern,
            ExecutionPattern::Execute | ExecutionPattern::Collaborate
        ) || strategy.uses_modifier(ExecutionModifier::WithVerifier)
        {
            (
                CollaborationTemplateId::PlannerExecutorVerifier,
                "bounded work can use the V5 execute-review Team graph",
            )
        } else {
            (
                CollaborationTemplateId::DirectExecutor,
                "simple low-risk work should avoid coordination overhead",
            )
        };
        CollaborationDecision {
            template_id,
            rationale: rationale.to_string(),
        }
    }
}

fn contains_any(input: &str, terms: &[&str]) -> bool {
    terms.iter().any(|term| input.contains(term))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::strategy::{decide_strategy, StrategyInput};

    fn matches(prompt: &str, expected: CollaborationTemplateId) {
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
        matches(
            "分析架构取舍和利弊",
            CollaborationTemplateId::DebateCriticArbiter,
        );
        matches(
            "重构并修复这个模块",
            CollaborationTemplateId::ImplementationReviewFix,
        );
        matches(
            "线上事故需要回滚",
            CollaborationTemplateId::IncidentResponse,
        );
        matches(
            "请使用多 Agent 团队并行审查三个模块，每个研究员读取真实代码并由综合者对比证据",
            CollaborationTemplateId::ParallelResearchSynthesis,
        );
        let strategy = decide_strategy(&StrategyInput::from_prompt("分析架构取舍"));
        assert!(CollaborationTemplateMatcher
            .decide("分析架构取舍", &strategy)
            .template_id
            .as_str()
            .contains("debate-critic-arbiter"));
    }
}
