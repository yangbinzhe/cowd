//! Runtime consumption layer for system-level Skill packages.
//!
//! The `skill` crate owns package inspection and registry. Runtime only decides
//! which already-profiled skills an agent can see and how a selected skill is
//! invoked inside a session.

pub mod activation;
pub mod dependency;
pub mod memory;

pub use activation::{RuntimeSkillCandidate, RuntimeSkillCandidateSource, SkillActivationRecord};
pub use dependency::CowdSkillStructuredDependency;
pub use memory::{
    memory_candidate_from_skill_activation, skill_memory_candidate_session_event, SkillMemoryPolicy,
};

use std::collections::BTreeMap;

use harness_contract::skill::{
    AgentSkillProfile, SkillAdapterKind, SkillCapabilityProfile, SkillEntrypoint,
    SkillInvocationEvidence,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillActivationInput {
    pub session_id: String,
    pub turn_index: usize,
    pub query: String,
    pub capability_refs: Vec<String>,
    pub available_profiles: Vec<SkillCapabilityProfile>,
    pub agent_profile: AgentSkillProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillActivationDecision {
    pub activation: SkillActivationRecord,
    pub selection: SkillSelectionResult,
    pub selected_invocation: Option<SkillInvocation>,
    pub invocation_evidence: Option<SkillInvocationEvidence>,
    pub structured_dependencies: Vec<CowdSkillStructuredDependency>,
}

/// A PromptOnly Skill payload inspected by the Skill layer and made available
/// to Runtime. Runtime selects the asset but never scans package paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSkillPromptAsset {
    pub skill_id: String,
    pub version: Option<String>,
    pub content: String,
    pub source_ref: String,
}

/// Runtime-owned snapshot of inspected Skill capabilities and bounded
/// PromptOnly payloads. The composition root may replace the snapshot after
/// package discovery; workers only consume it and never scan packages.
#[derive(Debug, Clone, Default)]
pub struct RuntimeSkillCatalog {
    profiles: Vec<SkillCapabilityProfile>,
    prompt_assets: Vec<RuntimeSkillPromptAsset>,
}

impl RuntimeSkillCatalog {
    #[must_use]
    pub fn new(
        profiles: Vec<SkillCapabilityProfile>,
        prompt_assets: Vec<RuntimeSkillPromptAsset>,
    ) -> Self {
        Self {
            profiles,
            prompt_assets,
        }
    }

    #[must_use]
    pub fn profiles(&self) -> Vec<SkillCapabilityProfile> {
        self.profiles.clone()
    }

    #[must_use]
    pub fn prompt_assets(&self) -> Vec<RuntimeSkillPromptAsset> {
        self.prompt_assets.clone()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SkillActivationEngine;

impl SkillActivationEngine {
    #[must_use]
    pub fn activate(input: SkillActivationInput) -> SkillActivationDecision {
        let profile_by_id = input
            .available_profiles
            .iter()
            .map(|profile| (profile.skill_id.clone(), profile.clone()))
            .collect::<BTreeMap<_, _>>();
        let selection = SkillSelector::select(SkillSelectionInput {
            query: input.query.clone(),
            agent_profile: input.agent_profile.clone(),
            available_skills: input.available_profiles,
        });
        let mut candidates = selection
            .candidates
            .iter()
            .map(|candidate| {
                let profile = profile_by_id.get(&candidate.skill_id);
                RuntimeSkillCandidate {
                    name: candidate.skill_id.clone(),
                    score: candidate.score,
                    reasons: candidate.reasons.clone(),
                    path: profile.map(|profile| profile.source_root.clone()),
                    source: RuntimeSkillCandidateSource::Profile,
                }
            })
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            candidates = fallback_capability_candidates(&input.capability_refs);
        }

        let selected_invocation = selection
            .selected
            .as_ref()
            .and_then(|selected| profile_by_id.get(&selected.skill_id))
            .and_then(|profile| {
                SkillInvocation::from_profile(profile, &input.agent_profile.adapter_ceiling)
            });
        let structured_dependencies = selection
            .selected
            .as_ref()
            .and_then(|selected| profile_by_id.get(&selected.skill_id))
            .map(|profile| {
                profile
                    .structured_dependencies
                    .iter()
                    .map(|dependency| {
                        CowdSkillStructuredDependency::from_contract(&profile.skill_id, dependency)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let invocation_evidence = selected_invocation
            .as_ref()
            .map(|invocation| invocation.to_evidence("selected_for_runtime"));
        let activation =
            SkillActivationRecord::new(input.session_id, input.turn_index, input.query, candidates)
                .with_invocation_evidence(invocation_evidence.clone())
                .with_structured_dependencies(structured_dependencies.clone());

        SkillActivationDecision {
            activation,
            selection,
            selected_invocation,
            invocation_evidence,
            structured_dependencies,
        }
    }
}

fn fallback_capability_candidates(capability_refs: &[String]) -> Vec<RuntimeSkillCandidate> {
    capability_refs
        .iter()
        .filter(|capability| !capability.trim().is_empty())
        .map(|capability| RuntimeSkillCandidate {
            name: capability.clone(),
            score: 5,
            reasons: vec!["capability_ref_fallback".to_string()],
            path: None,
            source: RuntimeSkillCandidateSource::CapabilityRefFallback,
        })
        .collect()
}

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
                if score == 0 {
                    return None;
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
        // A single generic summary-token match (for example `runtime` or
        // `agent`) is discovery evidence, not authorization to inject an
        // entire Skill prompt. Selection requires either an explicit visible
        // grant or a direct name/id match.
        let selected = candidates
            .first()
            .filter(|candidate| candidate.score >= MIN_SKILL_SELECTION_SCORE)
            .cloned();
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

const MIN_SKILL_SELECTION_SCORE: u32 = 6;

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
        SkillStructuredDependency,
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
            structured_dependencies: Vec::new(),
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

    #[test]
    fn skill_activation_engine_selects_profile_and_records_invocation_evidence() {
        let decision = SkillActivationEngine::activate(SkillActivationInput {
            session_id: "session-1".to_string(),
            turn_index: 3,
            query: "release planning".to_string(),
            capability_refs: vec!["planning".to_string()],
            available_profiles: vec![profile(
                "release-plan",
                "Release Plan",
                vec![SkillAdapterKind::PromptOnly],
            )],
            agent_profile: AgentSkillProfile {
                adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
                ..AgentSkillProfile::default()
            },
        });

        assert_eq!(
            decision.activation.selected.as_deref(),
            Some("release-plan")
        );
        assert_eq!(
            decision
                .invocation_evidence
                .as_ref()
                .map(|evidence| evidence.skill_id.as_str()),
            Some("release-plan")
        );
        let event = decision.activation.to_session_domain_event(9);
        assert_eq!(
            event.payload["invocation_evidence"]["outcome"],
            "selected_for_runtime"
        );
        assert!(event
            .refs
            .iter()
            .any(|reference| reference.ref_type == "skill_invocation"));
    }

    #[test]
    fn skill_activation_engine_projects_structured_dependencies() {
        let mut profile = profile(
            "supply-risk",
            "Supply Risk",
            vec![SkillAdapterKind::PromptOnly],
        );
        profile
            .structured_dependencies
            .push(SkillStructuredDependency {
                domain: "supply_chain".to_string(),
                required_fact_types: vec!["supplier.lead_time".to_string()],
                required_metric_keys: vec!["shortage_risk".to_string()],
                required_evidence: vec!["recent_supplier_signal".to_string()],
                quality_gate: "evidence_quality_gate".to_string(),
            });

        let decision = SkillActivationEngine::activate(SkillActivationInput {
            session_id: "session-1".to_string(),
            turn_index: 4,
            query: "supply risk".to_string(),
            capability_refs: Vec::new(),
            available_profiles: vec![profile],
            agent_profile: AgentSkillProfile {
                adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
                ..AgentSkillProfile::default()
            },
        });

        assert_eq!(decision.structured_dependencies.len(), 1);
        let event = decision.activation.to_session_domain_event(10);
        assert_eq!(
            event.payload["structured_dependencies"][0]["domain"],
            "supply_chain"
        );
        assert!(event
            .refs
            .iter()
            .any(|reference| reference.ref_type == "skill_dependency"));
    }

    #[test]
    fn skill_activation_engine_falls_back_to_capability_refs_without_profiles() {
        let decision = SkillActivationEngine::activate(SkillActivationInput {
            session_id: "session-1".to_string(),
            turn_index: 1,
            query: "review rust tests".to_string(),
            capability_refs: vec!["review".to_string(), "rust".to_string()],
            available_profiles: Vec::new(),
            agent_profile: AgentSkillProfile::default(),
        });

        assert_eq!(decision.activation.selected.as_deref(), None);
        assert!(decision.invocation_evidence.is_none());
        assert_eq!(
            decision.activation.candidates[0].reasons,
            vec!["capability_ref_fallback".to_string()]
        );
        assert_eq!(
            decision.activation.candidates[0].source,
            RuntimeSkillCandidateSource::CapabilityRefFallback
        );
        let event = decision.activation.to_session_domain_event(2);
        assert!(event.payload.get("invocation_evidence").is_some());
        assert!(!event
            .refs
            .iter()
            .any(|reference| reference.ref_type == "skill"));
    }

    #[test]
    fn skill_activation_engine_does_not_select_unrelated_available_profile() {
        let mut profile = profile(
            "supply-risk",
            "Supply Risk",
            vec![SkillAdapterKind::PromptOnly],
        );
        profile
            .structured_dependencies
            .push(SkillStructuredDependency {
                domain: "supply_chain".to_string(),
                required_fact_types: vec!["supplier.lead_time".to_string()],
                required_metric_keys: vec!["shortage_risk".to_string()],
                required_evidence: vec!["recent_supplier_signal".to_string()],
                quality_gate: "evidence_quality_gate".to_string(),
            });

        let decision = SkillActivationEngine::activate(SkillActivationInput {
            session_id: "session-1".to_string(),
            turn_index: 5,
            query: "summarize rust compile warnings".to_string(),
            capability_refs: vec!["review".to_string()],
            available_profiles: vec![profile],
            agent_profile: AgentSkillProfile {
                adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
                ..AgentSkillProfile::default()
            },
        });

        assert_eq!(decision.activation.selected.as_deref(), None);
        assert!(decision.invocation_evidence.is_none());
        assert!(decision.structured_dependencies.is_empty());
        assert_eq!(
            decision.activation.candidates[0].reasons,
            vec!["capability_ref_fallback".to_string()]
        );
    }

    #[test]
    fn generic_summary_term_does_not_activate_unrelated_skill() {
        let mut candidate = profile(
            "commit-version-gate",
            "Commit Version Gate",
            vec![SkillAdapterKind::PromptOnly],
        );
        candidate.inspection_summary = vec!["runtime governance".to_string()];

        let result = SkillSelector::select(SkillSelectionInput {
            query: "review runtime architecture".to_string(),
            agent_profile: AgentSkillProfile::default(),
            available_skills: vec![candidate],
        });

        assert!(result.selected.is_none());
        assert_eq!(result.candidates[0].score, 2);
    }
}
