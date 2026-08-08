//! Pure Mission scheduling contracts.
//!
//! A schedule describes *when* a Mission may submit work. It never contains a
//! conversation transcript, executor state, or a direct side effect.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::policy::PermissionMode;
use crate::reality::EvidenceRef;
use crate::task::TaskMissionAssignment;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissionAggregate {
    pub mission_id: String,
    pub workspace_id: String,
    pub objective: String,
    pub status: MissionStatus,
    pub revision: u64,
    pub strategy_ref: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskMissionAssignmentCommand {
    pub operation_id: String,
    pub workspace_id: String,
    pub task_ids: Vec<String>,
    pub target_mission_id: String,
    pub assignment: TaskMissionAssignment,
    pub actor: String,
    pub expected_task_revisions: BTreeMap<String, u64>,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskMissionAssignmentPreviewItem {
    pub task_id: String,
    pub current_mission_id: String,
    pub target_mission_id: String,
    pub current_revision: u64,
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskMissionAssignmentPreview {
    pub operation_id: String,
    pub target_mission_id: String,
    pub items: Vec<TaskMissionAssignmentPreviewItem>,
    pub confirm_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskMissionAssignmentReceipt {
    pub operation_id: String,
    pub target_mission_id: String,
    pub task_revisions: BTreeMap<String, u64>,
    pub assignment: TaskMissionAssignment,
    pub applied_at_ms: u64,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionFocusCommand {
    pub session_id: String,
    pub mission_id: String,
    pub actor: String,
    pub expected_session_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionFocusReceipt {
    pub session_id: String,
    pub mission_id: String,
    pub accepted_session_revision: u64,
    pub actor: String,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MissionOrganizationAction {
    KeepDefault,
    JoinExisting,
    CreateCluster,
    ProposeConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MissionOrganizationStatus {
    Pending,
    Claimed,
    Applied,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MissionOrganizationDecision {
    pub decision_id: String,
    pub workspace_id: String,
    pub task_ids: Vec<String>,
    pub action: MissionOrganizationAction,
    pub target_mission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_objective: Option<String>,
    pub status: MissionOrganizationStatus,
    pub reason: String,
    pub candidate_count: usize,
    pub provider_invoked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_model: Option<String>,
    #[serde(default)]
    pub provider_input_tokens: u64,
    #[serde(default)]
    pub provider_output_tokens: u64,
    #[serde(default)]
    pub elapsed_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_reason: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
    pub attempt: u32,
    pub next_attempt_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<String>,
    pub revision: u64,
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
    pub permission_ceiling: PermissionMode,
    pub priority: u8,
    pub next_at_ms: u64,
    pub status: MissionScheduleStatus,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionScheduleFire {
    pub fire_id: String,
    pub schedule_id: String,
    pub mission_id: String,
    pub due_at_ms: u64,
    pub correlation_id: String,
    pub target_session_id: String,
    pub objective: String,
    pub permission_ceiling: PermissionMode,
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

pub const MISSION_CONTROL_SCHEMA_VERSION: u32 = 5;

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
    pub pending_organization_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlMissionSummary {
    pub mission_id: String,
    pub objective: String,
    pub status: String,
    pub revision: u64,
    pub session_count: usize,
    pub task_count: usize,
    pub graph_count: usize,
    pub team_count: usize,
    pub agent_count: usize,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlTaskNode {
    pub task_id: String,
    pub mission_id: String,
    pub kind: String,
    pub root_task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    pub origin_session_id: String,
    pub objective: String,
    pub status: String,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_phase_id: Option<String>,
    pub phase_count: usize,
    pub graph_count: usize,
    pub turn_count: usize,
    pub assignment_source: String,
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
    pub contributing_task_count: usize,
    #[serde(default)]
    pub contributing_task_ids: Vec<String>,
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
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
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
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
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
pub struct MissionControlGraphNode {
    pub node_id: String,
    pub kind: String,
    pub label: String,
    pub status: String,
    pub mission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlGraphEdge {
    pub edge_id: String,
    pub kind: String,
    pub from_node_id: String,
    pub to_node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionControlGraphProjection {
    pub schema_version: u32,
    pub mission_id: String,
    pub nodes: Vec<MissionControlGraphNode>,
    pub edges: Vec<MissionControlGraphEdge>,
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
    pub selected_mission_id: String,
    pub missions: Vec<MissionControlMissionSummary>,
    pub mission: Value,
    pub sessions: Vec<MissionControlSessionNode>,
    pub tasks: Vec<MissionControlTaskNode>,
    pub teams: Vec<MissionControlTeamNode>,
    pub agents: Vec<MissionControlAgentNode>,
    pub approvals: Vec<MissionControlApprovalNode>,
    pub organization_decisions: Vec<MissionOrganizationDecision>,
    pub mission_graph: MissionControlGraphProjection,
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
            permission_ceiling: PermissionMode::ReadOnly,
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
            evidence_refs: vec![EvidenceRef::observed("mission_command", "evidence:1")],
        };
        let value = serde_json::to_value(command).expect("command serializes");
        assert!(value.get("session_history").is_none());
        assert_eq!(value["action"], "replan");
        assert_eq!(value["expected_revision"], 7);
    }
}
