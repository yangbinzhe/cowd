//! Skill activation signals that can be reviewed by the memory layer.

use super::activation::RuntimeSkillCandidateSource;
use super::SkillActivationRecord;
use crate::agent_collaboration::{MemoryPulseCandidate, MemoryPulseKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillMemoryPolicy {
    pub capture_no_match: bool,
    pub capture_low_confidence: bool,
    pub capture_selected: bool,
    pub low_confidence_score: u32,
}

impl Default for SkillMemoryPolicy {
    fn default() -> Self {
        Self {
            capture_no_match: true,
            capture_low_confidence: true,
            capture_selected: true,
            low_confidence_score: 4,
        }
    }
}

#[must_use]
pub fn memory_candidate_from_skill_activation(
    activation: &SkillActivationRecord,
    policy: &SkillMemoryPolicy,
) -> Option<MemoryPulseCandidate> {
    if activation.candidates.is_empty() && policy.capture_no_match {
        return Some(MemoryPulseCandidate {
            kind: MemoryPulseKind::Remember,
            content: format!(
                "skill activation gap; source=runtime_skill; query={}; no matching skill candidates",
                activation.query
            ),
        });
    }

    let selected_name = activation.selected.as_ref()?;
    let selected = activation.candidates.iter().find(|candidate| {
        candidate.name == *selected_name && candidate.source == RuntimeSkillCandidateSource::Profile
    })?;
    if policy.capture_low_confidence && selected.score <= policy.low_confidence_score {
        return Some(MemoryPulseCandidate {
            kind: MemoryPulseKind::Refresh,
            content: format!(
                "low confidence skill activation; source=runtime_skill; query={}; selected={}; score={}; reasons={}",
                activation.query,
                selected.name,
                selected.score,
                selected.reasons.join(",")
            ),
        });
    }

    if policy.capture_selected {
        return Some(MemoryPulseCandidate {
            kind: MemoryPulseKind::Refresh,
            content: format!(
                "skill selected for task; source=runtime_skill; query={}; selected={}; score={}; reasons={}",
                activation.query,
                selected.name,
                selected.score,
                selected.reasons.join(",")
            ),
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::super::RuntimeSkillCandidate;
    use super::*;

    #[test]
    fn skill_memory_no_match_creates_memory_gap_candidate() {
        let activation = SkillActivationRecord::new("s1", 1, "unknown workflow", Vec::new());

        let candidate =
            memory_candidate_from_skill_activation(&activation, &SkillMemoryPolicy::default())
                .unwrap();

        assert_eq!(candidate.kind, MemoryPulseKind::Remember);
        assert!(candidate.content.contains("skill activation gap"));
    }

    #[test]
    fn skill_memory_selected_skill_creates_refresh_candidate() {
        let activation = SkillActivationRecord::new(
            "s1",
            1,
            "prepare release",
            vec![RuntimeSkillCandidate {
                name: "release".to_string(),
                score: 12,
                reasons: vec!["tags:1".to_string()],
                path: None,
                source: RuntimeSkillCandidateSource::Profile,
            }],
        );

        let candidate =
            memory_candidate_from_skill_activation(&activation, &SkillMemoryPolicy::default())
                .unwrap();

        assert_eq!(candidate.kind, MemoryPulseKind::Refresh);
        assert!(candidate.content.contains("selected=release"));
    }

    #[test]
    fn skill_memory_ignores_capability_ref_fallback_candidates() {
        let activation = SkillActivationRecord::new(
            "s1",
            1,
            "review rust warnings",
            vec![RuntimeSkillCandidate {
                name: "review".to_string(),
                score: 5,
                reasons: vec!["capability_ref_fallback".to_string()],
                path: None,
                source: RuntimeSkillCandidateSource::CapabilityRefFallback,
            }],
        );

        let candidate =
            memory_candidate_from_skill_activation(&activation, &SkillMemoryPolicy::default());

        assert!(candidate.is_none());
    }
}
