use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{candidate::EvolutionCandidate, candidate_kind::EvolutionCandidateKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionGeneratedArtifact {
    pub path: String,
    pub content_type: String,
    pub purpose: String,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionArtifactBuilder;

impl EvolutionArtifactBuilder {
    pub fn build(
        root: impl AsRef<Path>,
        candidate: &EvolutionCandidate,
    ) -> Result<Vec<EvolutionGeneratedArtifact>, String> {
        let root = root.as_ref().join(&candidate.candidate_id);
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        let artifacts = artifact_specs(candidate.kind);
        let mut written = Vec::new();
        for (relative, content_type, purpose) in artifacts {
            let path = root.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            let payload = artifact_payload(candidate, relative);
            fs::write(&path, payload).map_err(|error| error.to_string())?;
            written.push(EvolutionGeneratedArtifact {
                path: path.display().to_string(),
                content_type: content_type.to_string(),
                purpose: purpose.to_string(),
            });
        }
        Ok(written)
    }
}

fn artifact_specs(
    kind: EvolutionCandidateKind,
) -> &'static [(&'static str, &'static str, &'static str)] {
    match kind {
        EvolutionCandidateKind::RuntimePolicy => &[(
            "runtime-policy.json",
            "application/json",
            "runtime policy overlay",
        )],
        EvolutionCandidateKind::ContextPolicy => &[(
            "context-policy.json",
            "application/json",
            "context policy overlay",
        )],
        EvolutionCandidateKind::MemoryGovernance => &[(
            "memory-governance.json",
            "application/json",
            "memory governance policy",
        )],
        EvolutionCandidateKind::RealityGovernance => &[(
            "reality-governance.json",
            "application/json",
            "reality governance policy",
        )],
        EvolutionCandidateKind::ToolContract => {
            &[("tool-contract.json", "application/json", "tool contract")]
        }
        EvolutionCandidateKind::SkillPackage => &[
            ("SKILL.md", "text/markdown", "skill instructions"),
            ("skill-manifest.json", "application/json", "skill manifest"),
        ],
        EvolutionCandidateKind::TeamTemplate => &[(
            "team-template.json",
            "application/json",
            "team collaboration template",
        )],
        EvolutionCandidateKind::SessionPolicy => {
            &[("session-policy.json", "application/json", "session policy")]
        }
        EvolutionCandidateKind::ProviderProfile => &[(
            "provider-profile-patch.yaml",
            "application/yaml",
            "provider profile patch",
        )],
        EvolutionCandidateKind::EvalScenario => &[(
            "eval-scenario.json",
            "application/json",
            "harness eval scenario",
        )],
        EvolutionCandidateKind::SurfaceProjection => &[
            (
                "api-contract.json",
                "application/json",
                "projection API contract",
            ),
            (
                "ui-change-plan.json",
                "application/json",
                "surface change plan",
            ),
        ],
        EvolutionCandidateKind::CodePatch => &[
            ("patch-plan.json", "application/json", "patch plan"),
            (
                "candidate.patch",
                "text/x-diff",
                "apply-ready patch placeholder",
            ),
        ],
        EvolutionCandidateKind::ArchitecturePlan => &[
            ("architecture-plan.md", "text/markdown", "architecture plan"),
            ("impact-matrix.json", "application/json", "impact matrix"),
        ],
    }
}

fn artifact_payload(candidate: &EvolutionCandidate, relative: &str) -> String {
    if relative.ends_with(".md") {
        return format!(
            "# {}\n\nCandidate: {}\nKind: {}\nMission: {}\nProposal: {}\n\nExpected change:\n{}\n\nRollback:\n{}\n",
            relative,
            candidate.candidate_id,
            candidate.kind.as_str(),
            candidate.mission_id.as_deref().unwrap_or("-"),
            candidate.proposal_id,
            candidate.expected_change,
            candidate.rollback_strategy
        );
    }
    if relative.ends_with(".patch") {
        return format!(
            "# candidate patch is intentionally inert until promotion approval\n# candidate_id={}\n",
            candidate.candidate_id
        );
    }
    if relative.ends_with(".yaml") {
        return format!(
            "candidate_id: {}\nkind: {}\npromotion_adapter: {}\n",
            candidate.candidate_id,
            candidate.kind.as_str(),
            candidate.promotion_adapter
        );
    }
    serde_json::to_string_pretty(&json!({
        "candidate_id": candidate.candidate_id,
        "kind": candidate.kind,
        "mission_id": candidate.mission_id,
        "proposal_id": candidate.proposal_id,
        "goal_ids": candidate.goal_ids,
        "promotion_adapter": candidate.promotion_adapter,
        "eval_scenario_ids": candidate.eval_scenario_ids,
        "autonomy_level": candidate.autonomy_level,
        "risk_boundaries": candidate.risk_boundaries,
        "approval_required": candidate.human_approval_required,
        "rollback_plan": candidate.rollback_strategy,
    }))
    .unwrap_or_else(|_| "{}".to_string())
}

#[must_use]
pub fn artifact_root_for(base: impl AsRef<Path>, candidate_id: &str) -> PathBuf {
    base.as_ref().join(candidate_id)
}
