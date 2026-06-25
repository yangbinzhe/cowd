//! Runtime-owned collaboration template catalog and matcher.
//!
//! The catalog is the stable contract between strategy routing and later
//! TeamRuntime/MissionRuntime execution. It decides how a task should be
//! organized without spawning agents or executing tools.

use harness_contract::core::{ExecutionMode, StrategyDecorator, TaskComplexity, TaskRisk};
use harness_contract::strategy::{StrategyDecision, TaskDomain};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationTemplateId {
    SingleExecutor,
    PlanExecuteReview,
    FanoutResearchSynthesis,
    DebateConsensus,
    ImplementationReviewFix,
    IncidentResponse,
    LongRunningProject,
}

impl CollaborationTemplateId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SingleExecutor => "single_executor",
            Self::PlanExecuteReview => "plan_execute_review",
            Self::FanoutResearchSynthesis => "fanout_research_synthesis",
            Self::DebateConsensus => "debate_consensus",
            Self::ImplementationReviewFix => "implementation_review_fix",
            Self::IncidentResponse => "incident_response",
            Self::LongRunningProject => "long_running_project",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationTemplate {
    pub template_id: CollaborationTemplateId,
    pub label: String,
    pub agent_roles: Vec<CollaborationRoleSpec>,
    pub context_visibility: CollaborationContextVisibility,
    pub memory_policy: String,
    pub evidence_policy: String,
    pub handoff_contract: String,
    pub review_contract: String,
    pub merge_contract: String,
    pub stop_condition: String,
    pub human_approval_points: Vec<String>,
    pub budget_policy: BudgetPolicy,
    pub max_parallelism: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationRoleSpec {
    pub role_id: String,
    pub responsibility: String,
    pub allowed_tools: Vec<String>,
    pub evidence_duties: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationContextVisibility {
    FullShared,
    RoleScoped,
    SummaryOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    pub max_turns: usize,
    pub max_parallel_agents: usize,
    pub checkpoint_after: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationPlan {
    pub template_id: CollaborationTemplateId,
    pub reason: String,
    pub agents: Vec<CollaborationPlanAgent>,
    pub context_visibility: CollaborationContextVisibility,
    pub memory_policy: String,
    pub evidence_policy: String,
    pub handoff_contract: String,
    pub review_contract: String,
    pub merge_contract: String,
    pub budget_policy: BudgetPolicy,
    pub max_parallelism: usize,
    pub human_approval_points: Vec<String>,
    pub stop_condition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationPlanAgent {
    pub role_id: String,
    pub responsibility: String,
    pub allowed_tools: Vec<String>,
    pub evidence_duties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationDecision {
    pub template_id: CollaborationTemplateId,
    pub rationale: String,
    pub plan: CollaborationPlan,
}

#[derive(Debug, Clone)]
pub struct CollaborationTemplateCatalog {
    templates: Vec<CollaborationTemplate>,
}

impl Default for CollaborationTemplateCatalog {
    fn default() -> Self {
        Self {
            templates: built_in_templates(),
        }
    }
}

impl CollaborationTemplateCatalog {
    #[must_use]
    pub fn built_in() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn templates(&self) -> &[CollaborationTemplate] {
        &self.templates
    }

    #[must_use]
    pub fn get(&self, template_id: CollaborationTemplateId) -> Option<&CollaborationTemplate> {
        self.templates
            .iter()
            .find(|template| template.template_id == template_id)
    }

    #[must_use]
    pub fn plan(
        &self,
        template_id: CollaborationTemplateId,
        reason: impl Into<String>,
    ) -> CollaborationPlan {
        let template = self.get(template_id).unwrap_or_else(|| {
            self.get(CollaborationTemplateId::SingleExecutor)
                .expect("built-in single executor template")
        });
        CollaborationPlan {
            template_id: template.template_id,
            reason: reason.into(),
            agents: template
                .agent_roles
                .iter()
                .map(|role| CollaborationPlanAgent {
                    role_id: role.role_id.clone(),
                    responsibility: role.responsibility.clone(),
                    allowed_tools: role.allowed_tools.clone(),
                    evidence_duties: role.evidence_duties.clone(),
                })
                .collect(),
            context_visibility: template.context_visibility,
            memory_policy: template.memory_policy.clone(),
            evidence_policy: template.evidence_policy.clone(),
            handoff_contract: template.handoff_contract.clone(),
            review_contract: template.review_contract.clone(),
            merge_contract: template.merge_contract.clone(),
            budget_policy: template.budget_policy.clone(),
            max_parallelism: template.max_parallelism,
            human_approval_points: template.human_approval_points.clone(),
            stop_condition: template.stop_condition.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CollaborationTemplateMatcher {
    catalog: CollaborationTemplateCatalog,
}

impl Default for CollaborationTemplateMatcher {
    fn default() -> Self {
        Self {
            catalog: CollaborationTemplateCatalog::default(),
        }
    }
}

impl CollaborationTemplateMatcher {
    #[must_use]
    pub fn new(catalog: CollaborationTemplateCatalog) -> Self {
        Self { catalog }
    }

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
                "critical or incident-like task needs commander/triage/remediation split",
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
                CollaborationTemplateId::LongRunningProject,
                "strategic or multi-stage task needs durable planning and periodic review",
            )
        } else if contains_any(
            &normalized,
            &["tradeoff", "pros", "cons", "debate", "是否", "利弊", "权衡"],
        ) || matches!(strategy.understanding.domain, TaskDomain::Architecture)
            && !strategy.understanding.requires_write
        {
            (
                CollaborationTemplateId::DebateConsensus,
                "decision-heavy task benefits from opposing analysis and consensus",
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
        ) || strategy.mode == ExecutionMode::ParallelReadFanout
            || strategy.uses_decorator(StrategyDecorator::WithExternalResearch)
        {
            (
                CollaborationTemplateId::FanoutResearchSynthesis,
                "research or fanout task should split evidence gathering from synthesis",
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
                "write-oriented task needs implementation, review, and fix ownership",
            )
        } else if matches!(
            strategy.mode,
            ExecutionMode::PlanExecute | ExecutionMode::SupervisorSubagents
        ) || strategy.uses_decorator(StrategyDecorator::WithVerifier)
        {
            (
                CollaborationTemplateId::PlanExecuteReview,
                "planned task should keep planner/executor/reviewer responsibilities explicit",
            )
        } else {
            (
                CollaborationTemplateId::SingleExecutor,
                "simple low-risk task should avoid unnecessary collaboration overhead",
            )
        };
        let rationale = rationale.to_string();
        CollaborationDecision {
            template_id,
            plan: self.catalog.plan(template_id, rationale.clone()),
            rationale,
        }
    }
}

fn built_in_templates() -> Vec<CollaborationTemplate> {
    vec![
        template(
            CollaborationTemplateId::SingleExecutor,
            "Single executor",
            vec![role(
                "executor",
                "Complete the request directly and record only necessary evidence.",
                &["read", "write", "tool_call"],
                &["final_answer"],
            )],
            CollaborationContextVisibility::FullShared,
            "recall only directly relevant memory",
            "cite evidence only when claims depend on files, tools, or external facts",
            "no handoff",
            "self-check before finalization",
            "single final response",
            "final answer produced or blocker recorded",
            vec![],
            BudgetPolicy {
                max_turns: 1,
                max_parallel_agents: 1,
                checkpoint_after: "before risky write".to_string(),
            },
            1,
        ),
        template(
            CollaborationTemplateId::PlanExecuteReview,
            "Plan, execute, review",
            vec![
                role(
                    "planner",
                    "Clarify objective, split work, and define completion checks.",
                    &["read", "search"],
                    &["plan", "acceptance_checks"],
                ),
                role(
                    "executor",
                    "Implement the plan and attach tool evidence.",
                    &["read", "write", "tool_call"],
                    &["changes", "tool_results"],
                ),
                role(
                    "reviewer",
                    "Audit the result against the objective and risks.",
                    &["read", "test"],
                    &["review_findings", "residual_risk"],
                ),
            ],
            CollaborationContextVisibility::RoleScoped,
            "share stable task facts and reviewer findings",
            "executor and reviewer must attach evidence refs",
            "planner hands acceptance checks to executor",
            "reviewer must approve or return fixes",
            "executor merges reviewed fixes into one response",
            "review passed or explicit blocker recorded",
            vec!["high_risk_write", "external_side_effect"],
            BudgetPolicy {
                max_turns: 6,
                max_parallel_agents: 2,
                checkpoint_after: "plan accepted".to_string(),
            },
            2,
        ),
        template(
            CollaborationTemplateId::FanoutResearchSynthesis,
            "Fanout research synthesis",
            vec![
                role(
                    "researcher_a",
                    "Gather primary evidence for the first dimension.",
                    &["read", "search", "web"],
                    &["source_notes"],
                ),
                role(
                    "researcher_b",
                    "Gather independent evidence for the second dimension.",
                    &["read", "search", "web"],
                    &["source_notes"],
                ),
                role(
                    "synthesizer",
                    "Deduplicate evidence, compare confidence, and produce conclusion.",
                    &["read"],
                    &["synthesis", "confidence"],
                ),
                role(
                    "verifier",
                    "Check freshness, contradictions, and source quality.",
                    &["read", "search"],
                    &["verification"],
                ),
            ],
            CollaborationContextVisibility::SummaryOnly,
            "write durable findings only after synthesis and verification",
            "primary evidence required for non-obvious claims",
            "researchers hand bounded notes to synthesizer",
            "verifier checks source quality before finalization",
            "synthesizer merges evidence with confidence labels",
            "verified synthesis produced or missing evidence listed",
            vec!["paid_api", "external_write"],
            BudgetPolicy {
                max_turns: 8,
                max_parallel_agents: 3,
                checkpoint_after: "evidence fanout complete".to_string(),
            },
            3,
        ),
        template(
            CollaborationTemplateId::DebateConsensus,
            "Debate consensus",
            vec![
                role(
                    "proposer",
                    "Argue for the strongest viable option.",
                    &["read", "search"],
                    &["benefits", "assumptions"],
                ),
                role(
                    "skeptic",
                    "Identify failure modes, hidden costs, and invalid assumptions.",
                    &["read", "search"],
                    &["risks", "counter_evidence"],
                ),
                role(
                    "arbiter",
                    "Weigh evidence and produce an actionable decision.",
                    &["read"],
                    &["decision_record"],
                ),
            ],
            CollaborationContextVisibility::FullShared,
            "persist final decision and rejected alternatives when durable",
            "explicit evidence for risks and assumptions",
            "proposer and skeptic hand positions to arbiter",
            "arbiter must state tradeoffs",
            "merge into one decision record",
            "decision made with tradeoffs or escalation requested",
            vec!["irreversible_decision"],
            BudgetPolicy {
                max_turns: 5,
                max_parallel_agents: 2,
                checkpoint_after: "positions submitted".to_string(),
            },
            2,
        ),
        template(
            CollaborationTemplateId::ImplementationReviewFix,
            "Implementation review fix",
            vec![
                role(
                    "implementer",
                    "Change code according to the target architecture.",
                    &["read", "write", "test"],
                    &["diff_summary", "test_results"],
                ),
                role(
                    "reviewer",
                    "Review correctness, boundaries, regressions, and missing tests.",
                    &["read", "test"],
                    &["findings"],
                ),
                role(
                    "fixer",
                    "Apply reviewer-required fixes and prepare final state.",
                    &["read", "write", "test"],
                    &["fix_receipts"],
                ),
            ],
            CollaborationContextVisibility::RoleScoped,
            "record reusable lessons after tests or review findings",
            "test or compile evidence required when feasible",
            "implementer hands diff summary to reviewer",
            "reviewer findings are mandatory before finalization",
            "fixer resolves findings into final patch",
            "review has no blocking findings and validation is recorded",
            vec!["destructive_command", "schema_change", "release_publish"],
            BudgetPolicy {
                max_turns: 10,
                max_parallel_agents: 2,
                checkpoint_after: "first implementation pass".to_string(),
            },
            2,
        ),
        template(
            CollaborationTemplateId::IncidentResponse,
            "Incident response",
            vec![
                role(
                    "commander",
                    "Maintain timeline, scope, decisions, and escalation state.",
                    &["read", "status"],
                    &["timeline", "decisions"],
                ),
                role(
                    "triage",
                    "Identify symptoms, blast radius, and likely causes.",
                    &["read", "search", "logs"],
                    &["triage_notes"],
                ),
                role(
                    "remediator",
                    "Prepare and execute approved remediation steps.",
                    &["read", "write", "rollback"],
                    &["remediation_receipts"],
                ),
                role(
                    "scribe",
                    "Capture user-facing summary and follow-up actions.",
                    &["read"],
                    &["post_incident_notes"],
                ),
            ],
            CollaborationContextVisibility::FullShared,
            "incident facts can become durable only after commander approval",
            "timeline and remediation evidence are mandatory",
            "triage hands candidate causes to commander/remediator",
            "commander approves remediation path",
            "scribe merges timeline and outcome",
            "service stable or escalation/blocker recorded",
            vec!["remediation", "rollback", "external_notification"],
            BudgetPolicy {
                max_turns: 12,
                max_parallel_agents: 3,
                checkpoint_after: "before remediation".to_string(),
            },
            3,
        ),
        template(
            CollaborationTemplateId::LongRunningProject,
            "Long-running project",
            vec![
                role(
                    "mission_planner",
                    "Create milestones, dependencies, and review gates.",
                    &["read", "search"],
                    &["milestone_plan"],
                ),
                role(
                    "workstream_owner",
                    "Execute current milestone and report progress.",
                    &["read", "write", "test"],
                    &["progress_receipts"],
                ),
                role(
                    "integrator",
                    "Merge workstream outputs and update mission projection.",
                    &["read", "write"],
                    &["integration_notes"],
                ),
                role(
                    "auditor",
                    "Check drift, incomplete work, and regression risk.",
                    &["read", "test"],
                    &["audit_report"],
                ),
            ],
            CollaborationContextVisibility::SummaryOnly,
            "promote stable decisions and recurring failures into memory",
            "each milestone requires evidence receipts",
            "planner hands milestone contract to workstream owner",
            "auditor reviews every milestone before completion",
            "integrator maintains mission projection",
            "all milestones complete or next blocker/action recorded",
            vec!["scope_change", "high_risk_write", "budget_overrun"],
            BudgetPolicy {
                max_turns: 30,
                max_parallel_agents: 3,
                checkpoint_after: "each milestone".to_string(),
            },
            3,
        ),
    ]
}

fn template(
    template_id: CollaborationTemplateId,
    label: &str,
    agent_roles: Vec<CollaborationRoleSpec>,
    context_visibility: CollaborationContextVisibility,
    memory_policy: &str,
    evidence_policy: &str,
    handoff_contract: &str,
    review_contract: &str,
    merge_contract: &str,
    stop_condition: &str,
    human_approval_points: Vec<&str>,
    budget_policy: BudgetPolicy,
    max_parallelism: usize,
) -> CollaborationTemplate {
    CollaborationTemplate {
        template_id,
        label: label.to_string(),
        agent_roles,
        context_visibility,
        memory_policy: memory_policy.to_string(),
        evidence_policy: evidence_policy.to_string(),
        handoff_contract: handoff_contract.to_string(),
        review_contract: review_contract.to_string(),
        merge_contract: merge_contract.to_string(),
        stop_condition: stop_condition.to_string(),
        human_approval_points: human_approval_points
            .into_iter()
            .map(str::to_string)
            .collect(),
        budget_policy,
        max_parallelism,
    }
}

fn role(
    role_id: &str,
    responsibility: &str,
    allowed_tools: &[&str],
    evidence_duties: &[&str],
) -> CollaborationRoleSpec {
    CollaborationRoleSpec {
        role_id: role_id.to_string(),
        responsibility: responsibility.to_string(),
        allowed_tools: allowed_tools
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        evidence_duties: evidence_duties
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::strategy::{decide_strategy, StrategyInput};

    #[test]
    fn catalog_contains_all_built_in_templates() {
        let catalog = CollaborationTemplateCatalog::built_in();
        let ids = catalog
            .templates()
            .iter()
            .map(|template| template.template_id)
            .collect::<Vec<_>>();

        assert_eq!(ids.len(), 7);
        assert!(ids.contains(&CollaborationTemplateId::SingleExecutor));
        assert!(ids.contains(&CollaborationTemplateId::PlanExecuteReview));
        assert!(ids.contains(&CollaborationTemplateId::FanoutResearchSynthesis));
        assert!(ids.contains(&CollaborationTemplateId::DebateConsensus));
        assert!(ids.contains(&CollaborationTemplateId::ImplementationReviewFix));
        assert!(ids.contains(&CollaborationTemplateId::IncidentResponse));
        assert!(ids.contains(&CollaborationTemplateId::LongRunningProject));
    }

    #[test]
    fn matcher_selects_specialized_templates() {
        assert_match(
            "production p0 outage needs rollback analysis",
            CollaborationTemplateId::IncidentResponse,
        );
        assert_match(
            "deep research and compare current harness projects",
            CollaborationTemplateId::FanoutResearchSynthesis,
        );
        assert_match(
            "implement refactor then compile and test",
            CollaborationTemplateId::ImplementationReviewFix,
        );
        assert_match(
            "分析这个架构选择的利弊，是否应该拆 crate",
            CollaborationTemplateId::DebateConsensus,
        );
        assert_match(
            "长期分阶段推进 mission runtime 里程碑",
            CollaborationTemplateId::LongRunningProject,
        );
        assert_match(
            "explain this function",
            CollaborationTemplateId::SingleExecutor,
        );
    }

    #[test]
    fn collaboration_plan_is_explainable_and_bounded() {
        let matcher = CollaborationTemplateMatcher::default();
        let strategy = decide_strategy(&StrategyInput::from_prompt(
            "implement refactor then compile and test",
        ));
        let decision = matcher.decide("implement refactor then compile and test", &strategy);

        assert_eq!(
            decision.template_id,
            CollaborationTemplateId::ImplementationReviewFix
        );
        assert_eq!(decision.plan.max_parallelism, 2);
        assert!(decision
            .plan
            .agents
            .iter()
            .any(|agent| agent.role_id == "reviewer"));
        assert_eq!(
            decision.plan.context_visibility,
            CollaborationContextVisibility::RoleScoped
        );
        assert!(decision.plan.evidence_policy.contains("test"));
        assert!(decision.plan.handoff_contract.contains("diff summary"));
        assert!(decision.plan.review_contract.contains("mandatory"));
        assert!(decision.plan.merge_contract.contains("final patch"));
        assert_eq!(decision.plan.budget_policy.max_parallel_agents, 2);
        assert!(decision.rationale.contains("write-oriented"));
        assert!(!decision.plan.stop_condition.is_empty());
    }

    fn assert_match(prompt: &str, expected: CollaborationTemplateId) {
        let matcher = CollaborationTemplateMatcher::default();
        let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
        let decision = matcher.decide(prompt, &strategy);
        assert_eq!(decision.template_id, expected, "{prompt}");
    }
}
