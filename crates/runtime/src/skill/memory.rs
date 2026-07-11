//! Skill activation signals that can be reviewed by the memory layer.

use super::activation::RuntimeSkillCandidateSource;
use super::SkillActivationRecord;
use chrono::Utc;
use memory::{MaintenanceCandidate, MaintenanceCandidateAction, MaintenanceCandidateStatus};
use uuid::Uuid;

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
) -> Option<MaintenanceCandidate> {
    if activation.candidates.is_empty() && policy.capture_no_match {
        return Some(maintenance_candidate(
            MaintenanceCandidateAction::Remember,
            "Review skill activation gap",
            format!(
                "skill activation gap; source=runtime_skill; query={}; no matching skill candidates",
                activation.query
            ),
            0.7,
            activation,
        ));
    }

    let selected_name = activation.selected.as_ref()?;
    let selected = activation.candidates.iter().find(|candidate| {
        candidate.name == *selected_name && candidate.source == RuntimeSkillCandidateSource::Profile
    })?;
    if policy.capture_low_confidence && selected.score <= policy.low_confidence_score {
        return Some(maintenance_candidate(
            MaintenanceCandidateAction::Refresh,
            "Refresh low-confidence skill activation",
            format!(
                "low confidence skill activation; source=runtime_skill; query={}; selected={}; score={}; reasons={}",
                activation.query,
                selected.name,
                selected.score,
                selected.reasons.join(",")
            ),
            0.55,
            activation,
        ));
    }

    if policy.capture_selected {
        return Some(maintenance_candidate(
            MaintenanceCandidateAction::Refresh,
            "Refresh selected skill activation",
            format!(
                "skill selected for task; source=runtime_skill; query={}; selected={}; score={}; reasons={}",
                activation.query,
                selected.name,
                selected.score,
                selected.reasons.join(",")
            ),
            0.6,
            activation,
        ));
    }

    None
}

fn maintenance_candidate(
    action: MaintenanceCandidateAction,
    summary: &str,
    reason: String,
    confidence: f32,
    activation: &SkillActivationRecord,
) -> MaintenanceCandidate {
    let now = Utc::now();
    MaintenanceCandidate {
        id: Uuid::new_v4().to_string(),
        kind: action.candidate_kind(),
        status: MaintenanceCandidateStatus::Open,
        entry_ids: Vec::new(),
        summary: summary.to_string(),
        reason: format!("memory_action={}; {reason}", action.as_str()),
        confidence,
        source: Some("runtime_skill".to_string()),
        source_ref: Some(format!(
            "skill_activation:{}:{}",
            activation.session_id, activation.turn_index
        )),
        created_at: now,
        updated_at: now,
    }
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

        assert_eq!(
            candidate.kind,
            memory::MaintenanceCandidateKind::RelationshipRefresh
        );
        assert_eq!(candidate.status, MaintenanceCandidateStatus::Open);
        assert!(candidate.entry_ids.is_empty());
        assert_eq!(candidate.source.as_deref(), Some("runtime_skill"));
        assert_eq!(
            candidate.source_ref.as_deref(),
            Some("skill_activation:s1:1")
        );
        assert!(candidate.reason.contains("memory_action=remember"));
        assert!(candidate.reason.contains("skill activation gap"));
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

        assert_eq!(
            candidate.kind,
            memory::MaintenanceCandidateKind::RelationshipRefresh
        );
        assert_eq!(candidate.status, MaintenanceCandidateStatus::Open);
        assert!(candidate.entry_ids.is_empty());
        assert!(candidate.reason.contains("memory_action=refresh"));
        assert!(candidate.reason.contains("selected=release"));
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
