use serde::{Deserialize, Serialize};

use super::diagnosis::EvolutionRootCauseKind;
use super::planner::EvolutionProposalKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionCandidateKind {
    RuntimePolicy,
    ContextPolicy,
    MemoryGovernance,
    RealityGovernance,
    ToolContract,
    SkillPackage,
    TeamTemplate,
    SessionPolicy,
    ProviderProfile,
    EvalScenario,
    SurfaceProjection,
    CodePatch,
    ArchitecturePlan,
}

impl EvolutionCandidateKind {
    pub const ALL: [Self; 13] = [
        Self::RuntimePolicy,
        Self::ContextPolicy,
        Self::MemoryGovernance,
        Self::RealityGovernance,
        Self::ToolContract,
        Self::SkillPackage,
        Self::TeamTemplate,
        Self::SessionPolicy,
        Self::ProviderProfile,
        Self::EvalScenario,
        Self::SurfaceProjection,
        Self::CodePatch,
        Self::ArchitecturePlan,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimePolicy => "runtime_policy",
            Self::ContextPolicy => "context_policy",
            Self::MemoryGovernance => "memory_governance",
            Self::RealityGovernance => "reality_governance",
            Self::ToolContract => "tool_contract",
            Self::SkillPackage => "skill_package",
            Self::TeamTemplate => "team_template",
            Self::SessionPolicy => "session_policy",
            Self::ProviderProfile => "provider_profile",
            Self::EvalScenario => "eval_scenario",
            Self::SurfaceProjection => "surface_projection",
            Self::CodePatch => "code_patch",
            Self::ArchitecturePlan => "architecture_plan",
        }
    }

    #[must_use]
    pub const fn promotion_adapter(self) -> &'static str {
        match self {
            Self::RuntimePolicy => "RuntimePolicyPromotion",
            Self::ContextPolicy => "ContextPolicyPromotion",
            Self::MemoryGovernance => "MemoryGovernancePromotion",
            Self::RealityGovernance => "RealityGovernancePromotion",
            Self::ToolContract => "ToolContractPromotion",
            Self::SkillPackage => "SkillPackagePromotion",
            Self::TeamTemplate => "TeamTemplatePromotion",
            Self::SessionPolicy => "SessionPolicyPromotion",
            Self::ProviderProfile => "ProviderProfilePromotion",
            Self::EvalScenario => "EvalScenarioPromotion",
            Self::SurfaceProjection => "SurfaceProjectionPromotion",
            Self::CodePatch => "CodePatchPromotion",
            Self::ArchitecturePlan => "ArchitecturePlanPromotion",
        }
    }

    #[must_use]
    pub const fn default_scenarios(self) -> &'static [&'static str] {
        match self {
            Self::RuntimePolicy => &["tool_batch_efficiency", "complex_strategy_selection"],
            Self::ContextPolicy => &["context_pressure", "memory_reality_context_governance"],
            Self::MemoryGovernance => &["memory_recall_precision", "pollution_control"],
            Self::RealityGovernance => &["fact_consistency", "relation_reasoning"],
            Self::ToolContract => &["tool_batch_efficiency", "tool_error_recovery"],
            Self::SkillPackage => &["workflow_reuse"],
            Self::TeamTemplate => &["team_agent_execution_outcome"],
            Self::SessionPolicy => &["cross_session_control", "task_continuity"],
            Self::ProviderProfile => &["provider_rounds", "protocol_smoke"],
            Self::EvalScenario => &["scenario_self_validation"],
            Self::SurfaceProjection => &["api_ui_action_coverage"],
            Self::CodePatch => &["cargo_check_test", "relevant_regression"],
            Self::ArchitecturePlan => &["impact_matrix", "architecture_gate"],
        }
    }
}

#[must_use]
pub fn candidate_kind_from_proposal(kind: &EvolutionProposalKind) -> EvolutionCandidateKind {
    match kind {
        EvolutionProposalKind::PlanDraft => EvolutionCandidateKind::ArchitecturePlan,
        EvolutionProposalKind::SkillDraft => EvolutionCandidateKind::SkillPackage,
        EvolutionProposalKind::TestScenario => EvolutionCandidateKind::EvalScenario,
        EvolutionProposalKind::ToolCapabilityRequest => EvolutionCandidateKind::ToolContract,
        EvolutionProposalKind::ConnectorCapabilityRequest => EvolutionCandidateKind::ToolContract,
        EvolutionProposalKind::MemoryGovernanceAdjustment => {
            EvolutionCandidateKind::MemoryGovernance
        }
    }
}

#[must_use]
pub fn candidate_kinds_from_root_cause(
    kind: &EvolutionRootCauseKind,
) -> Vec<EvolutionCandidateKind> {
    match kind {
        EvolutionRootCauseKind::RuntimeControlPolicyGap => {
            vec![EvolutionCandidateKind::RuntimePolicy]
        }
        EvolutionRootCauseKind::ToolContractGap => vec![EvolutionCandidateKind::ToolContract],
        EvolutionRootCauseKind::ContextPolicyGap => vec![EvolutionCandidateKind::ContextPolicy],
        EvolutionRootCauseKind::MemoryGovernanceGap => {
            vec![EvolutionCandidateKind::MemoryGovernance]
        }
        EvolutionRootCauseKind::TeamLifecycleGap => vec![EvolutionCandidateKind::TeamTemplate],
        EvolutionRootCauseKind::EvalCoverageGap => vec![EvolutionCandidateKind::EvalScenario],
        EvolutionRootCauseKind::SurfaceProjectionGap => {
            vec![EvolutionCandidateKind::SurfaceProjection]
        }
        EvolutionRootCauseKind::ProviderModelAffordanceGap => {
            vec![EvolutionCandidateKind::ProviderProfile]
        }
    }
}
