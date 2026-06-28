use harness_contract::skill::SkillInspectionReport;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillActionKind {
    Validate,
    Plan,
    Run,
}

impl SkillActionKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validate => "validate",
            Self::Plan => "plan",
            Self::Run => "run",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Rejected,
}

impl SkillRunStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRunPlan {
    pub summary: String,
    #[serde(default)]
    pub steps: Vec<String>,
    #[serde(default)]
    pub required_tools: Vec<String>,
    #[serde(default)]
    pub expected_side_effects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRunEvidence {
    pub kind: String,
    pub summary: String,
    #[serde(default)]
    pub refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRunReceipt {
    pub run_id: String,
    pub skill_id: String,
    pub action: SkillActionKind,
    pub status: SkillRunStatus,
    pub reason: String,
    pub risk_level: String,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
    pub tool_permission_summary: String,
    #[serde(default)]
    pub evidence: Vec<SkillRunEvidence>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillRunRecord {
    pub run_id: String,
    pub skill_id: String,
    pub action: SkillActionKind,
    pub status: SkillRunStatus,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub inspection: Option<SkillInspectionReport>,
    #[serde(default)]
    pub plan: Option<SkillRunPlan>,
    #[serde(default)]
    pub receipt: Option<SkillRunReceipt>,
    #[serde(default)]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_action_kind_uses_stable_wire_names() {
        assert_eq!(SkillActionKind::Validate.as_str(), "validate");
        assert_eq!(SkillActionKind::Plan.as_str(), "plan");
        assert_eq!(SkillActionKind::Run.as_str(), "run");
    }

    #[test]
    fn skill_run_record_serializes_receipt() {
        let record = SkillRunRecord {
            run_id: "skillrun-1".to_string(),
            skill_id: "review".to_string(),
            action: SkillActionKind::Validate,
            status: SkillRunStatus::Succeeded,
            created_at: "2026-06-28T00:00:00Z".to_string(),
            updated_at: "2026-06-28T00:00:00Z".to_string(),
            session_id: None,
            inspection: None,
            plan: None,
            receipt: Some(SkillRunReceipt {
                run_id: "skillrun-1".to_string(),
                skill_id: "review".to_string(),
                action: SkillActionKind::Validate,
                status: SkillRunStatus::Succeeded,
                reason: "validated".to_string(),
                risk_level: "low".to_string(),
                blocked_reasons: Vec::new(),
                tool_permission_summary: "no tool execution requested".to_string(),
                evidence: Vec::new(),
            }),
            error: None,
        };

        let json = serde_json::to_value(record).expect("skill run record should serialize");
        assert_eq!(json["action"], "validate");
        assert_eq!(json["status"], "succeeded");
        assert_eq!(
            json["receipt"]["tool_permission_summary"],
            "no tool execution requested"
        );
    }
}
