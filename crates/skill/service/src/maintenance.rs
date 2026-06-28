//! Skill usage maintenance and lifecycle recommendations.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillUsageSignal {
    pub skill_id: String,
    pub selected_count: u32,
    pub success_count: u32,
    pub failure_count: u32,
    pub correction_count: u32,
    pub activation_gap_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillMaintenanceAction {
    KeepActive,
    GenerateRevisionCandidate,
    Deprecate,
    Archive,
}

#[must_use]
pub fn evaluate_skill_maintenance(signal: &SkillUsageSignal) -> SkillMaintenanceAction {
    if signal.selected_count == 0 && signal.activation_gap_count >= 3 {
        return SkillMaintenanceAction::GenerateRevisionCandidate;
    }
    if signal.failure_count >= 3 && signal.success_count == 0 {
        return SkillMaintenanceAction::Deprecate;
    }
    if signal.correction_count >= 2 {
        return SkillMaintenanceAction::GenerateRevisionCandidate;
    }
    if signal.selected_count == 0 {
        return SkillMaintenanceAction::Archive;
    }
    SkillMaintenanceAction::KeepActive
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_maintenance_recommends_revision_for_repeated_corrections() {
        let action = evaluate_skill_maintenance(&SkillUsageSignal {
            skill_id: "plan-review".to_string(),
            selected_count: 5,
            success_count: 3,
            failure_count: 1,
            correction_count: 2,
            activation_gap_count: 0,
        });
        assert_eq!(action, SkillMaintenanceAction::GenerateRevisionCandidate);
    }
}
