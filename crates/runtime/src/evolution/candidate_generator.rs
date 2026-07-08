use super::{
    artifact_builder::EvolutionArtifactBuilder,
    candidate::{EvolutionCandidate, EvolutionCandidateStatus},
    candidate_kind::{candidate_kind_from_proposal, EvolutionCandidateKind},
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
            .map(|_| candidate_kind_from_proposal(&proposal.kind))
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
