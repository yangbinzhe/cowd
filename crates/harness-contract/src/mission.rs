//! Pure Mission scheduling contracts.
//!
//! A schedule describes *when* a Mission may submit work. It never contains a
//! conversation transcript, executor state, or a direct side effect.

use std::collections::BTreeMap;

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
    Create,
    Activate,
    Background,
    Pause,
    Resume,
    Cancel,
    Close,
    Input,
    Continue,
    Branch,
    Approve,
    Reject,
    Replan,
    Link,
    Unlink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MissionCommandTarget {
    Mission { mission_id: String },
    Session { session_id: String },
    Task { task_id: String },
    Graph { graph_id: String },
    Team { team_id: String },
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

/// Durable phase of one Gateway-owned cross-domain Mission command saga.
///
/// Runtime persists this record, but Gateway owns the orchestration. A caller
/// can replay a command from any phase without guessing whether its effect
/// happened before a process failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionCommandSagaPhase {
    Reserved,
    EffectCommitted,
    ReceiptCommitted,
    Finalized,
    Rejected,
    ReconciliationRequired,
}

impl MissionCommandSagaPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finalized | Self::Rejected)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionCommandSagaRecord {
    pub schema_version: u32,
    pub command: MissionCommand,
    pub phase: MissionCommandSagaPhase,
    pub revision: u64,
    pub reserved_target_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<MissionCommandReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub updated_at_ms: u64,
}

pub const MISSION_CONTROL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionWorkspaceProjection {
    pub workspace_id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_session_id: Option<String>,
    pub session_count: usize,
    pub running_agent_count: usize,
    pub pending_approval_count: usize,
    pub recovery_required_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlSummary {
    pub session_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_session_id: Option<String>,
    pub background_session_count: usize,
    pub paused_session_count: usize,
    pub closed_session_count: usize,
    pub task_count: usize,
    pub team_count: usize,
    pub agent_count: usize,
    pub pending_approval_count: usize,
    pub recovery_required_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlTaskNode {
    pub task_id: String,
    pub mission_id: String,
    pub source_session_id: String,
    pub objective: String,
    pub status: String,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_phase_id: Option<String>,
    pub phase_count: usize,
    pub graph_count: usize,
    pub failure_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker_reason: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlSessionNode {
    pub session_id: String,
    pub title: String,
    pub status: String,
    pub lifecycle: String,
    pub hydration: String,
    pub active: bool,
    pub attachment_count: usize,
    pub team_count: usize,
    pub agent_count: usize,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlTeamNode {
    pub team_id: String,
    pub graph_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub agent_count: usize,
    #[serde(default)]
    pub detail: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlAgentNode {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    #[serde(default)]
    pub detail: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlApprovalNode {
    pub approval_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_session_id: Option<String>,
    #[serde(default)]
    pub detail: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlEventLine {
    pub event_id: String,
    pub stream_id: String,
    pub cursor: u64,
    pub transaction_index: u32,
    pub scope: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlEventDigest {
    pub total_recent_events: usize,
    pub scope_counts: BTreeMap<String, usize>,
    pub latest_errors: Vec<MissionControlEventLine>,
    pub recovery_required: Vec<MissionControlEventLine>,
    pub latest: Vec<MissionControlEventLine>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlActionReadiness {
    pub action: String,
    pub available: bool,
    pub reason: String,
    pub requires_approval: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_marker: Option<String>,
    pub target_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlReadiness {
    pub kind: String,
    pub ready_count: usize,
    pub blocked_count: usize,
    pub actions: Vec<MissionControlActionReadiness>,
}

/// One materialized Mission Control read model. Domain details remain typed at
/// their canonical owner; `Value` fields here are bounded nested projections,
/// not mutable state or transport-specific raw response envelopes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionControlProjection {
    pub schema_version: u32,
    pub kind: String,
    pub workspace: MissionWorkspaceProjection,
    pub summary: MissionControlSummary,
    pub control_readiness: MissionControlReadiness,
    pub mission: Value,
    pub sessions: Vec<MissionControlSessionNode>,
    pub tasks: Vec<MissionControlTaskNode>,
    pub teams: Vec<MissionControlTeamNode>,
    pub agents: Vec<MissionControlAgentNode>,
    pub approvals: Vec<MissionControlApprovalNode>,
    pub relations: Value,
    pub execution_graphs: Value,
    pub conflicts: Value,
    pub evidence: Value,
    pub capabilities: Value,
    pub event_digest: MissionControlEventDigest,
    pub health: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionMaterializedSnapshot {
    pub schema_version: u32,
    pub kind: String,
    pub cursor: u64,
    pub revision: u64,
    pub needs_resync: bool,
    pub projection: MissionControlProjection,
}

/// Bounded incremental update. `patch` contains only domains named by
/// `changed_domains`; consumers must request a snapshot when `needs_resync`
/// is true instead of applying an incomplete patch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionProjectionDelta {
    pub schema_version: u32,
    pub kind: String,
    pub from_cursor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_revision: Option<u64>,
    pub to_cursor: u64,
    pub revision: u64,
    pub needs_resync: bool,
    #[serde(default)]
    pub changed_domains: Vec<String>,
    #[serde(default)]
    pub events: Vec<MissionControlEventLine>,
    #[serde(default)]
    pub patch: Value,
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
