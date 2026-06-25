//! Skill activation signals that can be reviewed by the memory layer.

use crate::agent_collaboration::{MemoryPulseCandidate, MemoryPulseKind};
use crate::skill_activation::SkillActivationRecord;

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
                "skill activation gap; source=skill_activation; query={}; no matching skill candidates",
                activation.query
            ),
        });
    }

    let selected = activation.candidates.first()?;
    if policy.capture_low_confidence && selected.score <= policy.low_confidence_score {
        return Some(MemoryPulseCandidate {
            kind: MemoryPulseKind::Refresh,
            content: format!(
                "low confidence skill activation; source=skill_activation; query={}; selected={}; score={}; reasons={}",
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
                "skill selected for task; source=skill_activation; query={}; selected={}; score={}; reasons={}",
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
    use super::*;
    use crate::skill_activation::RuntimeSkillCandidate;

    #[test]
    fn no_match_creates_memory_gap_candidate() {
        let activation = SkillActivationRecord::new("s1", 1, "unknown workflow", Vec::new());

        let candidate =
            memory_candidate_from_skill_activation(&activation, &SkillMemoryPolicy::default())
                .unwrap();

        assert_eq!(candidate.kind, MemoryPulseKind::Remember);
        assert!(candidate.content.contains("skill activation gap"));
    }

    #[test]
    fn selected_skill_creates_refresh_candidate() {
        let activation = SkillActivationRecord::new(
            "s1",
            1,
            "prepare release",
            vec![RuntimeSkillCandidate {
                name: "release".to_string(),
                score: 12,
                reasons: vec!["tags:1".to_string()],
                path: None,
            }],
        );

        let candidate =
            memory_candidate_from_skill_activation(&activation, &SkillMemoryPolicy::default())
                .unwrap();

        assert_eq!(candidate.kind, MemoryPulseKind::Refresh);
        assert!(candidate.content.contains("selected=release"));
    }
}
