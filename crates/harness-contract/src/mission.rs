//! Pure Mission scheduling contracts.
//!
//! A schedule describes *when* a Mission may submit work. It never contains a
//! conversation transcript, executor state, or a direct side effect.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::reality::EvidenceRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionStatus {
    Draft,
    Active,
    Paused,
    Completed,
    Cancelled,
    Failed,
}

impl MissionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionEntityRef {
    pub id: String,
    pub linked_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionAggregate {
    pub mission_id: String,
    pub workspace_id: String,
    pub objective: String,
    pub status: MissionStatus,
    pub revision: u64,
    pub strategy_ref: Option<String>,
    pub session_refs: Vec<MissionEntityRef>,
    pub task_refs: Vec<MissionEntityRef>,
    pub graph_refs: Vec<MissionEntityRef>,
    pub team_run_refs: Vec<MissionEntityRef>,
    pub agent_run_refs: Vec<MissionEntityRef>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionMutationReceipt {
    pub mission_id: String,
    pub accepted_revision: u64,
    pub status: MissionStatus,
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleTrigger {
    At {
        at_ms: u64,
    },
    Interval {
        every_ms: u64,
    },
    Cron {
        expression: String,
        timezone: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionScheduleStatus {
    Scheduled,
    Paused,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionSchedule {
    pub schedule_id: String,
    pub mission_id: String,
    pub target_session_id: String,
    pub objective: String,
    pub trigger: ScheduleTrigger,
    pub autonomy_profile: String,
    pub permission_lease: String,
    pub priority: u8,
    pub next_at_ms: u64,
    pub status: MissionScheduleStatus,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionScheduleFire {
    pub fire_id: String,
    pub schedule_id: String,
    pub due_at_ms: u64,
    pub correlation_id: String,
    pub target_session_id: String,
    pub objective: String,
    pub permission_lease: String,
    pub priority: u8,
    pub status: String,
    pub graph_id: Option<String>,
    pub error: Option<String>,
}

/// Command vocabulary for the Mission control plane. These are requests, not
/// mutable Mission state: Runtime validates current revision, authorization,
/// graph ownership and approval policy before it accepts one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionCommandAction {
    Activate,
    Background,
    Pause,
    Resume,
    Cancel,
    Close,
    Input,
    Approve,
    Reject,
    Replan,
    Link,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MissionCommandTarget {
    Mission { mission_id: String },
    Session { session_id: String },
    Agent { agent_id: String },
    Approval { approval_id: String },
    Relation { relation_id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionCommand {
    pub command_id: String,
    pub action: MissionCommandAction,
    pub target: MissionCommandTarget,
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub correlation_id: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionCommandReceipt {
    pub command_id: String,
    pub action: MissionCommandAction,
    pub target: MissionCommandTarget,
    pub accepted_revision: u64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
    #[serde(default)]
    pub result: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_contract_serializes_without_execution_state() {
        let schedule = MissionSchedule {
            schedule_id: "schedule-1".to_string(),
            mission_id: "mission-1".to_string(),
            target_session_id: "session-1".to_string(),
            objective: "inspect nightly evidence".to_string(),
            trigger: ScheduleTrigger::Interval { every_ms: 60_000 },
            autonomy_profile: "assisted".to_string(),
            permission_lease: "read_only".to_string(),
            priority: 64,
            next_at_ms: 123,
            status: MissionScheduleStatus::Scheduled,
            revision: 1,
        };
        let value = serde_json::to_value(schedule).expect("schedule serializes");
        assert!(value.get("conversation_history").is_none());
        assert!(value.get("executor_state").is_none());
    }

    #[test]
    fn mission_command_is_a_revision_checked_intent_not_a_state_snapshot() {
        let command = MissionCommand {
            command_id: "mission-command-1".to_string(),
            action: MissionCommandAction::Replan,
            target: MissionCommandTarget::Session {
                session_id: "session-1".to_string(),
            },
            actor: "human".to_string(),
            expected_revision: Some(7),
            correlation_id: "corr-1".to_string(),
            payload: serde_json::json!({"reason": "new evidence"}),
            evidence_refs: vec![EvidenceRef::new(
                "mission_command",
                "evidence:1",
                crate::reality::RealityBoundary::Observed,
            )],
        };
        let value = serde_json::to_value(command).expect("command serializes");
        assert!(value.get("session_history").is_none());
        assert_eq!(value["action"], "replan");
        assert_eq!(value["expected_revision"], 7);
    }
}
