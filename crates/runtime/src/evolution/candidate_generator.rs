use super::{
    artifact_builder::EvolutionArtifactBuilder,
    candidate::{EvolutionCandidate, EvolutionCandidateStatus},
    candidate_kind::{
        candidate_kind_from_goal_ids, candidate_kind_from_proposal,
        candidate_kinds_from_root_cause, EvolutionCandidateKind,
    },
    planner::EvolutionProposal,
};

#[derive(Debug, Clone, Default)]
pub struct EvolutionCandidateGenerator;

impl EvolutionCandidateGenerator {
    #[must_use]
    pub fn generate(
        proposal: &EvolutionProposal,
        baseline_ref: impl Into<String>,
        candidate_ref: impl Into<String>,
    ) -> EvolutionCandidate {
        let kind = proposal
            .root_cause_kind
            .as_ref()
            .and_then(|root_cause| {
                candidate_kinds_from_root_cause(root_cause)
                    .into_iter()
                    .next()
            })
            .or_else(|| candidate_kind_from_goal_ids(&proposal.goal_ids))
            .unwrap_or_else(|| candidate_kind_from_proposal(&proposal.kind));
        Self::generate_kind(proposal, kind, baseline_ref, candidate_ref)
    }

    #[must_use]
    pub fn generate_kind(
        proposal: &EvolutionProposal,
        kind: EvolutionCandidateKind,
        baseline_ref: impl Into<String>,
        candidate_ref: impl Into<String>,
    ) -> EvolutionCandidate {
        let now = now_ms();
        let target_owner = if proposal.target_owner.trim().is_empty() {
            "runtime".to_string()
        } else {
            proposal.target_owner.clone()
        };
        let target_files_or_modules = if proposal.candidate_scope.is_empty() {
            vec!["crates/runtime/src/evolution".to_string()]
        } else {
            proposal.candidate_scope.clone()
        };
        EvolutionCandidate {
            candidate_id: format!("evo-candidate-{}", uuid::Uuid::new_v4()),
            mission_id: proposal.mission_id.clone(),
            proposal_id: proposal.proposal_id.clone(),
            goal_ids: proposal.goal_ids.clone(),
            kind,
            owner: target_owner.clone(),
            scope: target_files_or_modules.clone(),
            trigger_signal_ids: proposal.source_signal_ids.clone(),
            affected_files_or_modules: target_files_or_modules.clone(),
            generated_artifacts: Vec::new(),
            eval_scenario_ids: kind
                .default_scenarios()
                .iter()
                .map(|scenario| (*scenario).to_string())
                .collect(),
            promotion_adapter: kind.promotion_adapter().to_string(),
            autonomy_level: "sandbox_only".to_string(),
            risk_boundaries: proposal.risk.boundaries.clone(),
            approval_required: proposal.risk.approval_required,
            baseline_ref: baseline_ref.into(),
            candidate_ref: candidate_ref.into(),
            target_owner,
            target_files_or_modules,
            artifact_root: None,
            baseline_command: deterministic_command("baseline", kind),
            candidate_command: deterministic_command("candidate", kind),
            verification_command: deterministic_command("verify", kind),
            artifact_path: None,
            expected_change: proposal.target_improvement.clone(),
            adoption_gate: proposal.acceptance_gates.clone(),
            rollback_strategy: proposal.rollback_strategy.clone(),
            status: EvolutionCandidateStatus::Draft,
            mainline_modified: false,
            human_approval_required: proposal.risk.approval_required,
            comparison_report_ref: None,
            version_record_ref: None,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    pub fn generate_with_artifacts(
        artifact_root: impl AsRef<std::path::Path>,
        proposal: &EvolutionProposal,
        baseline_ref: impl Into<String>,
        candidate_ref: impl Into<String>,
    ) -> Result<EvolutionCandidate, String> {
        let mut candidate = Self::generate(proposal, baseline_ref, candidate_ref);
        let artifacts = EvolutionArtifactBuilder::build(&artifact_root, &candidate)?;
        candidate.generated_artifacts = artifacts.clone();
        candidate.artifact_root = Some(
            artifact_root
                .as_ref()
                .join(&candidate.candidate_id)
                .display()
                .to_string(),
        );
        candidate.artifact_path = artifacts.first().map(|artifact| artifact.path.clone());
        candidate.status = EvolutionCandidateStatus::SandboxReady;
        Ok(candidate)
    }
}

fn deterministic_command(kind: &str, candidate_kind: EvolutionCandidateKind) -> String {
    format!(
        "cowd-evolution-{kind} --candidate-kind {} --deterministic-artifact-check",
        candidate_kind.as_str()
    )
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EvolutionDiagnosisEngine, EvolutionSignal, EvolutionSignalSource, EvolutionSignalType,
    };

    fn signal(signal_type: EvolutionSignalType) -> EvolutionSignal {
        EvolutionSignal::new(crate::EvolutionSignalInput {
            signal_type,
            source: EvolutionSignalSource {
                owner: "runtime".to_string(),
                session_id: Some("session-kind".to_string()),
                agent_id: None,
                team_id: None,
                run_id: None,
            },
            evidence_refs: vec!["scenario:candidate-kind".to_string()],
            severity: crate::EvolutionSignalSeverity::Warning,
            summary: "candidate kind scenario".to_string(),
            suggested_action: "generate terminal typed candidate".to_string(),
            immediate_task_can_continue: true,
        })
    }

    #[test]
    fn generator_prefers_root_cause_over_plan_draft_fallback() {
        let cases = [
            (
                EvolutionSignalType::LowNoveltyToolLoop,
                EvolutionCandidateKind::RuntimePolicy,
            ),
            (
                EvolutionSignalType::ContextPressure,
                EvolutionCandidateKind::ContextPolicy,
            ),
            (
                EvolutionSignalType::AgentFailurePattern,
                EvolutionCandidateKind::TeamTemplate,
            ),
            (
                EvolutionSignalType::EvalFailure,
                EvolutionCandidateKind::EvalScenario,
            ),
            (
                EvolutionSignalType::MissingToolCapability,
                EvolutionCandidateKind::ToolContract,
            ),
            (
                EvolutionSignalType::MemoryNoise,
                EvolutionCandidateKind::MemoryGovernance,
            ),
        ];

        for (signal_type, expected_kind) in cases {
            let signals = vec![signal(signal_type)];
            let diagnosis = EvolutionDiagnosisEngine::diagnose(&signals);
            let proposal = EvolutionProposal::from_diagnosis(&diagnosis, &signals);
            let candidate =
                EvolutionCandidateGenerator::generate(&proposal, "baseline", "candidate");

            assert_eq!(candidate.kind, expected_kind);
            assert_eq!(
                candidate.promotion_adapter,
                expected_kind.promotion_adapter()
            );
        }
    }

    #[test]
    fn generator_uses_goal_ids_before_generic_proposal_kind() {
        let mut proposal = EvolutionProposal {
            proposal_id: "proposal-goal".to_string(),
            kind: crate::EvolutionProposalKind::PlanDraft,
            mission_id: Some("mission-goal".to_string()),
            goal_ids: vec!["context_precision".to_string()],
            diagnosis_id: None,
            root_cause_kind: None,
            target_owner: "runtime".to_string(),
            candidate_scope: vec!["crates/runtime/src/context".to_string()],
            problem_statement: "context precision drift".to_string(),
            current_evidence: vec!["context:pressure".to_string()],
            target_improvement: "reduce context noise".to_string(),
            expected_benefit: "better recall precision".to_string(),
            risk: crate::EvolutionProposalRisk {
                level: "medium".to_string(),
                boundaries: vec!["sandbox_eval_required".to_string()],
                approval_required: true,
            },
            acceptance_gates: vec!["context candidate has evidence".to_string()],
            rollback_strategy: "archive sandbox".to_string(),
            source_signal_ids: vec!["signal-goal".to_string()],
            created_at_ms: 1,
            status: "draft".to_string(),
        };
        let candidate = EvolutionCandidateGenerator::generate(&proposal, "baseline", "candidate");
        assert_eq!(candidate.kind, EvolutionCandidateKind::ContextPolicy);

        proposal.goal_ids = vec!["unknown-goal".to_string()];
        let fallback = EvolutionCandidateGenerator::generate(&proposal, "baseline", "candidate");
        assert_eq!(fallback.kind, EvolutionCandidateKind::ArchitecturePlan);
    }
}
