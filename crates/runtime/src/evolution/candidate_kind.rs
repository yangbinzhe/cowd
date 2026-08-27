use serde::{Deserialize, Serialize};

use super::diagnosis::EvolutionRootCauseKind;
use super::planner::EvolutionProposalKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionCandidateKind {
    AgentDefinition,
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

/// Typed owner routing for a proposed reusable asset. This is intentionally
/// not a string capability claim: only the three named owner paths are
/// executable/governed, while every other advertised kind is explicit about
/// the absence of a generic promotion adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionPromotionRoute {
    AgentDefinitionGovernance,
    TeamTemplateGovernance,
    SkillRevisionGovernance,
    KnowledgeCandidateOnly,
    PromotionAdapterUnavailable,
}

impl EvolutionCandidateKind {
    pub const ALL: [Self; 14] = [
        Self::AgentDefinition,
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
            Self::AgentDefinition => "agent_definition",
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
            Self::AgentDefinition => "AgentDefinitionPromotion",
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
    pub const fn promotion_route(self) -> EvolutionPromotionRoute {
        match self {
            Self::AgentDefinition => EvolutionPromotionRoute::AgentDefinitionGovernance,
            Self::TeamTemplate => EvolutionPromotionRoute::TeamTemplateGovernance,
            Self::SkillPackage => EvolutionPromotionRoute::SkillRevisionGovernance,
            Self::MemoryGovernance => EvolutionPromotionRoute::KnowledgeCandidateOnly,
            Self::RuntimePolicy
            | Self::ContextPolicy
            | Self::RealityGovernance
            | Self::ToolContract
            | Self::SessionPolicy
            | Self::ProviderProfile
            | Self::EvalScenario
            | Self::SurfaceProjection
            | Self::CodePatch
            | Self::ArchitecturePlan => EvolutionPromotionRoute::PromotionAdapterUnavailable,
        }
    }

    #[must_use]
    pub const fn default_scenarios(self) -> &'static [&'static str] {
        match self {
            Self::AgentDefinition => &["agent_definition_behavior", "agent_definition_safety"],
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

#[must_use]
pub fn candidate_kind_from_goal_id(goal_id: &str) -> Option<EvolutionCandidateKind> {
    match goal_id {
        "execution_efficiency" => Some(EvolutionCandidateKind::RuntimePolicy),
        "context_precision" => Some(EvolutionCandidateKind::ContextPolicy),
        "memory_pollution_control" => Some(EvolutionCandidateKind::MemoryGovernance),
        "fact_consistency" => Some(EvolutionCandidateKind::RealityGovernance),
        "tool_success_rate" => Some(EvolutionCandidateKind::ToolContract),
        "workflow_reuse" => Some(EvolutionCandidateKind::SkillPackage),
        "complex_task_success" => Some(EvolutionCandidateKind::TeamTemplate),
        "task_continuity" => Some(EvolutionCandidateKind::SessionPolicy),
        "model_fit" => Some(EvolutionCandidateKind::ProviderProfile),
        "regression_coverage" => Some(EvolutionCandidateKind::EvalScenario),
        "observability" => Some(EvolutionCandidateKind::SurfaceProjection),
        "defect_resolution" => Some(EvolutionCandidateKind::CodePatch),
        "future_evolvability" => Some(EvolutionCandidateKind::ArchitecturePlan),
        _ => None,
    }
}

#[must_use]
pub fn candidate_kind_from_goal_ids(goal_ids: &[String]) -> Option<EvolutionCandidateKind> {
    goal_ids
        .iter()
        .find_map(|goal_id| candidate_kind_from_goal_id(goal_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advertised_kind_has_an_explicit_promotion_route() {
        for kind in EvolutionCandidateKind::ALL {
            match kind.promotion_route() {
                EvolutionPromotionRoute::AgentDefinitionGovernance
                | EvolutionPromotionRoute::TeamTemplateGovernance
                | EvolutionPromotionRoute::SkillRevisionGovernance
                | EvolutionPromotionRoute::KnowledgeCandidateOnly
                | EvolutionPromotionRoute::PromotionAdapterUnavailable => {}
            }
        }
        assert_eq!(
            EvolutionCandidateKind::SkillPackage.promotion_route(),
            EvolutionPromotionRoute::SkillRevisionGovernance
        );
        assert_eq!(
            EvolutionCandidateKind::CodePatch.promotion_route(),
            EvolutionPromotionRoute::PromotionAdapterUnavailable
        );
    }
}
