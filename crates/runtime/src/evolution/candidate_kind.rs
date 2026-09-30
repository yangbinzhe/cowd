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
    SkillRevisionGovernance,
    KnowledgeCandidateOnly,
    PromotionAdapterUnavailable,
}

impl EvolutionCandidateKind {
    /// The kinds the runtime can actually promote - i.e. the ones a caller may
    /// ADVERTISE. A kind is only listed here when `promotion_route` returns a
    /// governed route, so "advertised" and "executable" stay the same set.
    ///
    /// The enum still carries the other ten kinds, because durable records refer
    /// to them and dropping the variants would make history unreadable. They are
    /// deliberately absent from `ALL` so nothing iterates over them as if they
    /// were live: `promotion_route` returns `PromotionAdapterUnavailable` for
    /// each of them, which is the honest statement of the gap.
    pub const ALL: [Self; 3] = [
        Self::AgentDefinition,
        Self::SkillPackage,
        Self::MemoryGovernance,
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
        EvolutionRootCauseKind::TeamLifecycleGap => vec![EvolutionCandidateKind::RuntimePolicy],
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
        "complex_task_success" => Some(EvolutionCandidateKind::RuntimePolicy),
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
                | EvolutionPromotionRoute::SkillRevisionGovernance
                | EvolutionPromotionRoute::KnowledgeCandidateOnly => {}
                EvolutionPromotionRoute::PromotionAdapterUnavailable => panic!(
                    "`{}` is advertised in ALL but has no promotion route",
                    kind.as_str()
                ),
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

    /// "Advertised" and "executable" must stay the same set. This is the whole
    /// point of trimming `ALL`: a kind with no executor may not be iterated as if
    /// it were a live capability, which is what the thirteen-entry list did.
    #[test]
    fn advertised_equals_executable() {
        let advertised = EvolutionCandidateKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>();
        let executable = [
            EvolutionCandidateKind::AgentDefinition,
            EvolutionCandidateKind::SkillPackage,
            EvolutionCandidateKind::MemoryGovernance,
        ]
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
        assert_eq!(advertised, executable);

        // The ten unavailable kinds stay in the enum (history refers to them) but
        // are absent from ALL.
        for kind in [
            EvolutionCandidateKind::RuntimePolicy,
            EvolutionCandidateKind::ContextPolicy,
            EvolutionCandidateKind::RealityGovernance,
            EvolutionCandidateKind::ToolContract,
            EvolutionCandidateKind::SessionPolicy,
            EvolutionCandidateKind::ProviderProfile,
            EvolutionCandidateKind::EvalScenario,
            EvolutionCandidateKind::SurfaceProjection,
            EvolutionCandidateKind::CodePatch,
            EvolutionCandidateKind::ArchitecturePlan,
        ] {
            assert_eq!(
                kind.promotion_route(),
                EvolutionPromotionRoute::PromotionAdapterUnavailable,
                "`{}` must stay honest about having no executor",
                kind.as_str()
            );
            assert!(
                !EvolutionCandidateKind::ALL.contains(&kind),
                "`{}` has no executor and must not be advertised",
                kind.as_str()
            );
        }
    }
}
