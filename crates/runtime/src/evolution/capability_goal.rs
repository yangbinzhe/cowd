use serde::{Deserialize, Serialize};

use super::candidate_kind::EvolutionCandidateKind;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionCapabilityGoal {
    pub goal_id: String,
    pub name: String,
    pub metric_ids: Vec<String>,
    pub target_owner: String,
    pub success_criteria: Vec<String>,
    pub default_candidate_kinds: Vec<EvolutionCandidateKind>,
}

impl EvolutionCapabilityGoal {
    #[must_use]
    pub fn for_kind(kind: EvolutionCandidateKind) -> Self {
        let (goal_id, name, metrics, owner, criteria) = match kind {
            EvolutionCandidateKind::RuntimePolicy => (
                "execution_efficiency",
                "Execution efficiency",
                vec!["completion_quality", "tool_loop_reduction"],
                "runtime",
                vec!["runtime policy candidate has measurable behavior gate"],
            ),
            EvolutionCandidateKind::ContextPolicy => (
                "context_precision",
                "Context precision",
                vec!["token_efficiency", "recall_precision"],
                "runtime",
                vec!["context candidate proves lower noise and bounded token cost"],
            ),
            EvolutionCandidateKind::MemoryGovernance => (
                "memory_pollution_control",
                "Memory pollution control",
                vec!["recall_precision", "cross_scope_noise"],
                "reality_core",
                vec!["memory candidate proves scoped activation and no unrelated recall"],
            ),
            EvolutionCandidateKind::RealityGovernance => (
                "fact_consistency",
                "Fact consistency",
                vec!["relation_reasoning", "conflict_resolution"],
                "reality_core",
                vec!["reality candidate preserves evidence and conflict semantics"],
            ),
            EvolutionCandidateKind::ToolContract => (
                "tool_success_rate",
                "Tool success rate",
                vec!["batch_efficiency", "failure_recovery"],
                "tools",
                vec!["tool candidate has contract, permission, and execution evidence"],
            ),
            EvolutionCandidateKind::SkillPackage => (
                "workflow_reuse",
                "Workflow reuse",
                vec!["instruction_reduction", "reuse_success"],
                "skill",
                vec!["skill package contains instructions, manifest, and rollback metadata"],
            ),
            EvolutionCandidateKind::TeamTemplate => (
                "complex_task_success",
                "Complex task success",
                vec!["parallel_efficiency", "conflict_resolution"],
                "runtime",
                vec!["team template maps roles, boundaries, evidence, and intervention"],
            ),
            EvolutionCandidateKind::SessionPolicy => (
                "task_continuity",
                "Task continuity",
                vec!["cross_session_control", "handoff_quality"],
                "runtime",
                vec!["session policy has continuity and isolation gates"],
            ),
            EvolutionCandidateKind::ProviderProfile => (
                "model_fit",
                "Model fit",
                vec!["latency_stability", "protocol_success"],
                "runtime",
                vec!["provider profile has protocol, context, timeout, and fallback evidence"],
            ),
            EvolutionCandidateKind::EvalScenario => (
                "regression_coverage",
                "Regression coverage",
                vec!["scenario_precision", "terminal_gate"],
                "harness_eval",
                vec!["eval scenario has deterministic and real-model evidence plan"],
            ),
            EvolutionCandidateKind::SurfaceProjection => (
                "observability",
                "Observability",
                vec!["control_coverage", "projection_latency"],
                "surface",
                vec!["surface projection covers compact and detail control paths"],
            ),
            EvolutionCandidateKind::CodePatch => (
                "defect_resolution",
                "Defect resolution",
                vec!["regression_safety", "compile_success"],
                "runtime",
                vec!["code patch remains apply-ready and never auto-writes mainline"],
            ),
            EvolutionCandidateKind::ArchitecturePlan => (
                "future_evolvability",
                "Future evolvability",
                vec!["structural_integrity", "owner_clarity"],
                "runtime",
                vec!["architecture plan contains impact matrix and deletion gates"],
            ),
        };
        Self {
            goal_id: goal_id.to_string(),
            name: name.to_string(),
            metric_ids: metrics.into_iter().map(str::to_string).collect(),
            target_owner: owner.to_string(),
            success_criteria: criteria.into_iter().map(str::to_string).collect(),
            default_candidate_kinds: vec![kind],
        }
    }
}
