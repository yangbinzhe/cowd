//! Runtime consumption layer for system-level Skill packages.
//!
//! The `skill` crate owns package inspection and registry. Runtime only decides
//! which already-profiled skills an agent can see and how a selected skill is
//! invoked inside a session.

use harness_contract::skill::{
    AgentSkillProfile, SkillAdapterKind, SkillCapabilityProfile, SkillEntrypoint,
    SkillInvocationEvidence,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSelectionInput {
    pub query: String,
    pub agent_profile: AgentSkillProfile,
    pub available_skills: Vec<SkillCapabilityProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSelectionCandidate {
    pub skill_id: String,
    pub name: String,
    pub score: u32,
    pub adapter: SkillAdapterKind,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSelectionResult {
    pub selected: Option<SkillSelectionCandidate>,
    pub candidates: Vec<SkillSelectionCandidate>,
}

#[derive(Debug, Clone, Default)]
pub struct SkillSelector;

impl SkillSelector {
    #[must_use]
    pub fn select(input: SkillSelectionInput) -> SkillSelectionResult {
        let query_terms = query_terms(&input.query);
        let visible_refs = visible_skill_refs(&input.agent_profile);
        let adapter_ceiling = &input.agent_profile.adapter_ceiling;
        let mut candidates = input
            .available_skills
            .into_iter()
            .filter(|skill| {
                !input
                    .agent_profile
                    .hidden_skill_refs
                    .iter()
                    .any(|hidden| matches_skill_ref(skill, hidden))
            })
            .filter(|skill| {
                visible_refs.is_empty()
                    || visible_refs
                        .iter()
                        .any(|reference| matches_skill_ref(skill, reference))
            })
            .filter_map(|skill| {
                let adapter = select_adapter(&skill, adapter_ceiling)?;
                let mut score = 0_u32;
                let mut reasons = Vec::new();
                let name_lower = skill.name.to_ascii_lowercase();
                let id_lower = skill.skill_id.to_ascii_lowercase();
                for term in &query_terms {
                    if name_lower.contains(term) || id_lower.contains(term) {
                        score += 8;
                        reasons.push(format!("name:{term}"));
                    }
                    for summary in &skill.inspection_summary {
                        if summary.to_ascii_lowercase().contains(term) {
                            score += 2;
                            reasons.push(format!("summary:{term}"));
                        }
                    }
                }
                if visible_refs
                    .iter()
                    .any(|reference| matches_skill_ref(&skill, reference))
                {
                    score += 6;
                    reasons.push("agent_profile.visible".to_string());
                }
                if score == 0 && visible_refs.is_empty() {
                    score = 1;
                    reasons.push("available".to_string());
                }
                Some(SkillSelectionCandidate {
                    skill_id: skill.skill_id,
                    name: skill.name,
                    score,
                    adapter,
                    reasons,
                })
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.name.cmp(&right.name))
        });
        let selected = candidates.first().cloned();
        SkillSelectionResult {
            selected,
            candidates,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInvocation {
    pub skill_id: String,
    pub skill_version: Option<String>,
    pub adapter: SkillAdapterKind,
    pub entrypoint: Option<SkillEntrypoint>,
}

impl SkillInvocation {
    #[must_use]
    pub fn from_profile(
        profile: &SkillCapabilityProfile,
        adapter_ceiling: &[SkillAdapterKind],
    ) -> Option<Self> {
        let adapter = select_adapter(profile, adapter_ceiling)?;
        let entrypoint = profile
            .entrypoints
            .iter()
            .find(|entrypoint| entrypoint.adapter == adapter)
            .cloned();
        Some(Self {
            skill_id: profile.skill_id.clone(),
            skill_version: profile.version.clone(),
            adapter,
            entrypoint,
        })
    }

    #[must_use]
    pub fn to_evidence(&self, outcome: impl Into<String>) -> SkillInvocationEvidence {
        SkillInvocationEvidence {
            skill_id: self.skill_id.clone(),
            skill_version: self.skill_version.clone(),
            adapter: self.adapter,
            entrypoint: self.entrypoint.as_ref().map(|entry| entry.path.clone()),
            outcome: outcome.into(),
            evidence_refs: Vec::new(),
        }
    }
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|term| term.len() >= 2)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn visible_skill_refs(profile: &AgentSkillProfile) -> Vec<String> {
    profile
        .baseline_skill_refs
        .iter()
        .chain(profile.template_skill_refs.iter())
        .chain(profile.team_skill_refs.iter())
        .chain(profile.task_skill_refs.iter())
        .chain(profile.explicit_grants.iter())
        .cloned()
        .collect()
}

fn matches_skill_ref(skill: &SkillCapabilityProfile, reference: &str) -> bool {
    skill.skill_id.eq_ignore_ascii_case(reference) || skill.name.eq_ignore_ascii_case(reference)
}

fn select_adapter(
    skill: &SkillCapabilityProfile,
    adapter_ceiling: &[SkillAdapterKind],
) -> Option<SkillAdapterKind> {
    skill
        .adapters
        .iter()
        .copied()
        .find(|adapter| adapter_ceiling.is_empty() || adapter_ceiling.contains(adapter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::{
        SkillDetectedRuntime, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
    };

    fn profile(id: &str, name: &str, adapters: Vec<SkillAdapterKind>) -> SkillCapabilityProfile {
        SkillCapabilityProfile {
            skill_id: id.to_string(),
            name: name.to_string(),
            version: Some("1.0.0".to_string()),
            source_root: "/tmp/skill".to_string(),
            package_fingerprint: "abc".to_string(),
            kind: SkillKind::Document,
            lifecycle_status: SkillLifecycleStatus::UsablePrompt,
            adapters,
            risk_level: SkillRiskLevel::Low,
            entrypoints: vec![SkillEntrypoint {
                runtime: SkillDetectedRuntime::Markdown,
                path: "SKILL.md".to_string(),
                adapter: SkillAdapterKind::PromptOnly,
                command_hint: None,
            }],
            inspection_summary: vec!["release planning".to_string()],
        }
    }

    #[test]
    fn agent_skill_profile_filters_visible_skills() {
        let input = SkillSelectionInput {
            query: "release".to_string(),
            agent_profile: AgentSkillProfile {
                baseline_skill_refs: vec!["release-plan".to_string()],
                template_skill_refs: Vec::new(),
                team_skill_refs: Vec::new(),
                task_skill_refs: Vec::new(),
                explicit_grants: Vec::new(),
                hidden_skill_refs: Vec::new(),
                adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
            },
            available_skills: vec![
                profile(
                    "release-plan",
                    "Release Plan",
                    vec![SkillAdapterKind::PromptOnly],
                ),
                profile(
                    "debug-plan",
                    "Debug Plan",
                    vec![SkillAdapterKind::PromptOnly],
                ),
            ],
        };

        let result = SkillSelector::select(input);
        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.selected.unwrap().skill_id, "release-plan");
    }

    #[test]
    fn skill_selector_respects_adapter_ceiling() {
        let input = SkillSelectionInput {
            query: "python".to_string(),
            agent_profile: AgentSkillProfile {
                baseline_skill_refs: Vec::new(),
                template_skill_refs: Vec::new(),
                team_skill_refs: Vec::new(),
                task_skill_refs: Vec::new(),
                explicit_grants: Vec::new(),
                hidden_skill_refs: Vec::new(),
                adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
            },
            available_skills: vec![profile(
                "py-runner",
                "Python Runner",
                vec![SkillAdapterKind::SandboxExec],
            )],
        };

        let result = SkillSelector::select(input);
        assert!(result.selected.is_none());
    }

    #[test]
    fn skill_invocation_records_evidence() {
        let profile = profile(
            "release-plan",
            "Release Plan",
            vec![SkillAdapterKind::PromptOnly],
        );
        let invocation =
            SkillInvocation::from_profile(&profile, &[SkillAdapterKind::PromptOnly]).unwrap();
        let evidence = invocation.to_evidence("selected");
        assert_eq!(evidence.skill_id, "release-plan");
        assert_eq!(evidence.entrypoint.as_deref(), Some("SKILL.md"));
    }
}
