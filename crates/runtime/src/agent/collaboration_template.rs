//! Strategy-facing template selection.
//!
//! This is deliberately only a semantic matcher. Role topology, scheduling,
//! memory writes, and execution belong respectively to the versioned protocol
//! registry, RuntimeExecutionSupervisor, and Memory maintenance pipeline.

use harness_contract::core::{ExecutionModifier, ExecutionPattern, TaskComplexity, TaskRisk};
use harness_contract::strategy::{
    automatic_team_is_structurally_required, StrategyDecision, TaskDomain,
};
use serde::{Deserialize, Serialize};

use crate::definition_registry::RuntimeTeamTemplateCatalogEntry;

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
        } else if automatic_team_is_structurally_required(&strategy.understanding)
            && !strategy.understanding.requires_write
        {
            // Candidate selection has already established that this objective
            // needs independent, tool-backed ownership.  Preserve that same
            // typed fact when choosing a topology: a broad code-domain label
            // must not silently replace a read-only evidence Team with a
            // write/review template whose role contract is incompatible with
            // the generated focus plan.
            (
                CollaborationTemplateId::ParallelResearchSynthesis,
                "independent evidence obligations require the parallel research topology",
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

    /// Fallback matcher consumed against the Registry-built catalog.
    ///
    /// The keyword rules above are deliberately low-confidence: they only
    /// name a builtin family. The Registry snapshot is the template truth, so
    /// this method returns `None` when the selected family is not actually
    /// published/runnable, and custom published templates participate through
    /// the catalog instead of being invented here.
    #[must_use]
    pub fn decide_from_catalog<'a>(
        &self,
        user_input: &str,
        strategy: &StrategyDecision,
        catalog: &'a [RuntimeTeamTemplateCatalogEntry],
    ) -> Option<(&'a RuntimeTeamTemplateCatalogEntry, CollaborationDecision)> {
        let decision = self.decide(user_input, strategy);
        let entry = catalog.iter().find(|entry| {
            entry
                .revision_ref
                .template_id
                .as_str()
                .ends_with(decision.template_id.template_path())
        })?;
        Some((entry, decision))
    }
}

fn contains_any(input: &str, terms: &[&str]) -> bool {
    terms.iter().any(|term| input.contains(term))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::strategy::{decide_strategy, StrategyInput};
    use harness_contract::team::{
        TeamRoleDependency, TeamTemplateDefinitionId, TeamTemplateRevisionRef, TeamTopologyContract,
    };

    fn catalog_entry(template_path: &str, revision: u64) -> RuntimeTeamTemplateCatalogEntry {
        RuntimeTeamTemplateCatalogEntry {
            revision_ref: TeamTemplateRevisionRef {
                template_id: TeamTemplateDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    template_path,
                )
                .expect("template id"),
                revision,
            },
            name: template_path.to_string(),
            content_digest: format!("digest:{template_path}:{revision}"),
            team_markdown_digest: Some(format!("markdown:{template_path}")),
            topology: TeamTopologyContract {
                protocol_ref: "review_fix@1".to_string(),
                require_synthesis: true,
                require_review: true,
            },
            role_count: 0,
            roles: Vec::new(),
            dependencies: Vec::<TeamRoleDependency>::new(),
            result_fields: vec!["summary".to_string()],
        }
    }

    fn full_catalog() -> Vec<RuntimeTeamTemplateCatalogEntry> {
        vec![
            catalog_entry("cowd/direct-executor", 1),
            catalog_entry("cowd/implementation-review-fix", 1),
            catalog_entry("cowd/debate-critic-arbiter", 1),
        ]
    }

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
    fn structurally_required_read_only_team_uses_the_matching_evidence_topology() {
        let strategy = decide_strategy(
            &StrategyInput::from_prompt("审视 runtime、gateway 和 webui 的独立证据职责")
                .with_understanding(harness_contract::strategy::TaskUnderstanding {
                    domain: TaskDomain::Backend,
                    complexity: TaskComplexity::Complex,
                    risk: TaskRisk::Medium,
                    requires_write: false,
                    requires_external_facts: false,
                    requires_tool_evidence: true,
                    requests_parallelism: false,
                    requests_multi_agent: false,
                    required_team_count: 0,
                    forbids_team: false,
                    requests_deep_plan: false,
                    requests_deliberation: false,
                    requests_background: false,
                    likely_single_file: false,
                    independent_workstreams: 3,
                    uncertainty: 2,
                    estimated_duration: harness_contract::strategy::TaskDuration::Extended,
                    collaboration_reference: Default::default(),
                }),
        );

        let decision = CollaborationTemplateMatcher
            .decide("审视 runtime、gateway 和 webui 的独立证据职责", &strategy);

        assert_eq!(
            decision.template_id,
            CollaborationTemplateId::ParallelResearchSynthesis
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

    #[test]
    fn catalog_matcher_requires_the_registry_truth_and_never_fabricates() {
        let strategy = decide_strategy(&StrategyInput::from_prompt("重构并修复这个模块"));
        let decision = CollaborationTemplateMatcher.decide("重构并修复这个模块", &strategy);
        assert_eq!(
            decision.template_id,
            CollaborationTemplateId::ImplementationReviewFix
        );

        let catalog = full_catalog();
        let (entry, matched) = CollaborationTemplateMatcher
            .decide_from_catalog("重构并修复这个模块", &strategy, &catalog)
            .expect("builtin template is published in the registry catalog");
        assert_eq!(
            entry.revision_ref.template_id.as_str(),
            "builtin/cowd/implementation-review-fix"
        );
        assert_eq!(matched.template_id, decision.template_id);
        assert_eq!(
            entry.content_digest,
            "digest:cowd/implementation-review-fix:1"
        );
    }

    #[test]
    fn catalog_matcher_returns_none_when_selected_family_is_not_published() {
        let strategy = decide_strategy(&StrategyInput::from_prompt("重构并修复这个模块"));
        let custom_only = vec![catalog_entry("cowd/custom-review-team", 3)];
        assert!(
            CollaborationTemplateMatcher
                .decide_from_catalog("重构并修复这个模块", &strategy, &custom_only)
                .is_none(),
            "a keyword match must not fabricate a template absent from the registry catalog"
        );
    }

    #[test]
    fn catalog_matcher_never_uses_display_name_as_behavior_truth() {
        let strategy = decide_strategy(&StrategyInput::from_prompt("分析架构取舍和利弊"));
        let catalog = vec![catalog_entry("cowd/debate-critic-arbiter", 2)];
        let (entry, decision) = CollaborationTemplateMatcher
            .decide_from_catalog("分析架构取舍和利弊", &strategy, &catalog)
            .expect("debate family is published");
        assert_eq!(
            decision.template_id,
            CollaborationTemplateId::DebateCriticArbiter
        );
        assert_eq!(entry.revision_ref.revision, 2);
        assert_eq!(
            entry.revision_ref.template_id.as_str(),
            "builtin/cowd/debate-critic-arbiter"
        );
    }
}
