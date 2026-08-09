//! Declarative Task input and canonical durable Task aggregate contracts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::reality::EvidenceRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(String);

impl TaskId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("task-{}", Uuid::new_v4()))
    }

    #[must_use]
    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Reviewing,
    Completed,
    Blocked,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Root,
    Delegated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskOrigin {
    User,
    Schedule,
    Mission,
    Delegated,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskMissionAssignment {
    Default,
    Automatic,
    ExplicitLocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskTurnRole {
    Primary,
    Additional,
    Review,
    Handoff,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskTurnBinding {
    pub binding_id: String,
    pub task_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub role: TaskTurnRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
    pub bound_at_ms: u64,
}

impl TaskTurnBinding {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.binding_id.trim().is_empty() {
            return Err("binding_id must not be empty");
        }
        if self.task_id.trim().is_empty() {
            return Err("task_id must not be empty");
        }
        if self.session_id.trim().is_empty() {
            return Err("session_id must not be empty");
        }
        if self.turn_id.trim().is_empty() {
            return Err("turn_id must not be empty");
        }
        Ok(())
    }
}

pub fn validate_turn_bindings(bindings: &[TaskTurnBinding]) -> Result<(), String> {
    let mut identities = std::collections::BTreeSet::new();
    let mut primary_turns = std::collections::BTreeSet::new();
    for binding in bindings {
        binding.validate().map_err(str::to_string)?;
        let identity = (
            binding.task_id.as_str(),
            binding.session_id.as_str(),
            binding.turn_id.as_str(),
        );
        if !identities.insert(identity) {
            return Err(format!(
                "duplicate task/turn binding for task `{}` and turn `{}`",
                binding.task_id, binding.turn_id
            ));
        }
        if binding.role == TaskTurnRole::Primary
            && !primary_turns.insert((binding.session_id.as_str(), binding.turn_id.as_str()))
        {
            return Err(format!(
                "turn `{}` has more than one primary task binding",
                binding.turn_id
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TaskRouteHint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default)]
    pub compound_objectives: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_id: Option<String>,
}

/// Durable routing preference owned by one Session. A focus is advisory: the
/// Runtime Task Router validates it against current Task state for every Turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionTaskFocus {
    pub task_id: String,
    pub actor: String,
    pub revision: u64,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from_session_id: Option<String>,
}

/// Durable Mission routing preference for future Root Tasks in one Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionMissionFocus {
    pub mission_id: String,
    pub actor: String,
    pub revision: u64,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct SessionRoutingFocus {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<SessionTaskFocus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission: Option<SessionMissionFocus>,
}

impl SessionRoutingFocus {
    #[must_use]
    pub fn route_hint(&self) -> Option<TaskRouteHint> {
        let task_id = self.task.as_ref().map(|focus| focus.task_id.clone());
        let mission_id = self
            .task
            .is_none()
            .then(|| self.mission.as_ref().map(|focus| focus.mission_id.clone()))
            .flatten();
        (task_id.is_some() || mission_id.is_some()).then_some(TaskRouteHint {
            task_id,
            mission_id,
            compound_objectives: Vec::new(),
            handoff_id: None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionFocusMutation {
    TaskSet,
    TaskCleared,
    MissionSet,
    MissionCleared,
    TaskInvalidated,
    MissionInvalidated,
    FocusInvalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionFocusReceipt {
    pub session_id: String,
    pub mutation: SessionFocusMutation,
    pub accepted_revision: u64,
    pub actor: String,
    pub updated_at_ms: u64,
    pub focus: SessionRoutingFocus,
    #[serde(default)]
    pub evidence_refs: Vec<crate::reality::EvidenceRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TaskRouteDecision {
    NoTask {
        reason: String,
    },
    Continue {
        task_id: String,
        role: TaskTurnRole,
    },
    CreateRoot {
        spec: TaskSpec,
        mission_id: String,
        assignment: TaskMissionAssignment,
    },
    CreateCompound {
        primary: TaskSpec,
        additional: Vec<TaskSpec>,
        mission_id: String,
        assignment: TaskMissionAssignment,
    },
    CreateSuccessor {
        predecessor_task_id: String,
        spec: TaskSpec,
        mission_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRouteReceipt {
    pub route_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub decision: TaskRouteDecision,
    #[serde(default)]
    pub candidate_task_ids: Vec<String>,
    pub source: String,
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
    pub elapsed_ms: u64,
    pub created_at_ms: u64,
}

impl TaskStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Reviewing => "reviewing",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhaseStatus {
    Pending,
    Running,
    Reviewing,
    Completed,
    Blocked,
    Cancelled,
    Failed,
}

impl TaskPhaseStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Reviewing => "reviewing",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskPhaseTerminalReceipt {
    pub outcome: String,
    pub evidence_refs: Vec<EvidenceRef>,
    pub completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskPhaseArtifact {
    pub kind: String,
    pub label: String,
    pub value: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskPhase {
    pub phase_id: String,
    pub name: String,
    pub objective: String,
    pub status: TaskPhaseStatus,
    pub revision: u64,
    pub dependency_refs: Vec<String>,
    pub plan: Vec<String>,
    pub acceptance: Vec<String>,
    pub test_commands: Vec<String>,
    pub artifacts: Vec<TaskPhaseArtifact>,
    pub review_result: Option<String>,
    pub terminal_receipt: Option<TaskPhaseTerminalReceipt>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskExecutionPolicy {
    pub yolo_mode: bool,
    pub max_failures_before_block: u32,
}

impl Default for TaskExecutionPolicy {
    fn default() -> Self {
        Self {
            yolo_mode: false,
            max_failures_before_block: 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskGraphRef {
    pub graph_id: String,
    pub revision: u64,
    pub linked_at_ms: u64,
}

/// Immutable Gateway-authored provenance for an application-owned structured
/// task.  The application can request a registered result contract, but it
/// cannot author or replace any field in this binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskApplicationProvenance {
    pub producer_id: String,
    pub workspace_id: String,
    pub surface: String,
    pub result_contract_id: String,
    pub result_schema_id: String,
    pub result_schema_version: u16,
    pub result_schema_digest: String,
    pub result_max_bytes: u64,
    pub request_digest: String,
}

impl TaskApplicationProvenance {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("producer_id", self.producer_id.as_str()),
            ("workspace_id", self.workspace_id.as_str()),
            ("surface", self.surface.as_str()),
            ("result_contract_id", self.result_contract_id.as_str()),
            ("result_schema_id", self.result_schema_id.as_str()),
        ] {
            validate_identity(field, value)?;
        }
        if self.result_schema_version == 0 || self.result_max_bytes == 0 {
            return Err(
                "application result contract version and size must be positive".to_string(),
            );
        }
        for (field, digest) in [
            ("result_schema_digest", self.result_schema_digest.as_str()),
            ("request_digest", self.request_digest.as_str()),
        ] {
            if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(format!("{field} must be a 64-character hexadecimal digest"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskAggregate {
    pub task_id: String,
    pub mission_id: String,
    pub kind: TaskKind,
    pub origin: TaskOrigin,
    pub origin_session_id: String,
    pub origin_turn_id: String,
    pub root_task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor_task_id: Option<String>,
    pub mission_assignment: TaskMissionAssignment,
    pub mission_assignment_revision: u64,
    pub mission_assigned_by: String,
    #[serde(default)]
    pub mission_assignment_evidence_refs: Vec<EvidenceRef>,
    pub objective: String,
    pub status: TaskStatus,
    pub revision: u64,
    pub current_phase_id: Option<String>,
    pub phases: Vec<TaskPhase>,
    pub execution_policy: TaskExecutionPolicy,
    pub failure_count: u32,
    pub blocker_reason: Option<String>,
    pub strategy_ref: Option<String>,
    pub graph_refs: Vec<TaskGraphRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_provenance: Option<TaskApplicationProvenance>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl TaskAggregate {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity("task_id", &self.task_id)?;
        validate_identity("mission_id", &self.mission_id)?;
        validate_identity("origin_session_id", &self.origin_session_id)?;
        validate_identity("origin_turn_id", &self.origin_turn_id)?;
        validate_identity("root_task_id", &self.root_task_id)?;
        validate_identity("mission_assigned_by", &self.mission_assigned_by)?;
        validate_identity("objective", &self.objective)?;
        if self.revision == 0 || self.mission_assignment_revision == 0 {
            return Err("task and mission assignment revisions must be positive".to_string());
        }
        if let Some(provenance) = &self.application_provenance {
            provenance.validate()?;
        }
        validate_task_lineage(
            self.kind,
            &self.task_id,
            &self.root_task_id,
            self.parent_task_id.as_deref(),
            self.predecessor_task_id.as_deref(),
            self.origin,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPhaseSpec {
    pub name: String,
    pub objective: String,
    pub dependency_refs: Vec<String>,
    pub plan: Vec<String>,
    pub acceptance: Vec<String>,
    pub test_commands: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub objective: String,
    pub phases: Vec<TaskPhaseSpec>,
    pub execution_policy: TaskExecutionPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_provenance: Option<TaskApplicationProvenance>,
}

impl TaskSpec {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            objective: objective.into(),
            phases: Vec::new(),
            execution_policy: TaskExecutionPolicy::default(),
            application_provenance: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskCreateCommand {
    pub task_id: String,
    pub mission_id: String,
    pub kind: TaskKind,
    pub origin: TaskOrigin,
    pub origin_session_id: String,
    pub origin_turn_id: String,
    pub root_task_id: String,
    pub parent_task_id: Option<String>,
    pub predecessor_task_id: Option<String>,
    pub mission_assignment: TaskMissionAssignment,
    pub mission_assigned_by: String,
    pub spec: TaskSpec,
    pub evidence_refs: Vec<EvidenceRef>,
}

impl TaskCreateCommand {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity("task_id", &self.task_id)?;
        validate_identity("mission_id", &self.mission_id)?;
        validate_identity("origin_session_id", &self.origin_session_id)?;
        validate_identity("origin_turn_id", &self.origin_turn_id)?;
        validate_identity("root_task_id", &self.root_task_id)?;
        validate_identity("mission_assigned_by", &self.mission_assigned_by)?;
        validate_identity("objective", &self.spec.objective)?;
        validate_task_lineage(
            self.kind,
            &self.task_id,
            &self.root_task_id,
            self.parent_task_id.as_deref(),
            self.predecessor_task_id.as_deref(),
            self.origin,
        )
    }
}

fn validate_task_lineage(
    kind: TaskKind,
    task_id: &str,
    root_task_id: &str,
    parent_task_id: Option<&str>,
    predecessor_task_id: Option<&str>,
    origin: TaskOrigin,
) -> Result<(), String> {
    if predecessor_task_id == Some(task_id) {
        return Err("task cannot be its own predecessor".to_string());
    }
    match kind {
        TaskKind::Root => {
            if root_task_id != task_id || parent_task_id.is_some() {
                return Err(
                    "root task requires root_task_id == task_id and no parent_task_id".to_string(),
                );
            }
            if origin == TaskOrigin::Delegated {
                return Err("root task cannot use delegated origin".to_string());
            }
        }
        TaskKind::Delegated => {
            let parent = parent_task_id
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "delegated task requires parent_task_id".to_string())?;
            if root_task_id == task_id || parent == task_id {
                return Err("delegated task must reference another root and parent".to_string());
            }
            if predecessor_task_id.is_some() {
                return Err("delegated task cannot be a root-task successor".to_string());
            }
        }
    }
    Ok(())
}

fn validate_identity(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskCommandReceipt {
    pub task_id: String,
    pub accepted_revision: u64,
    pub status: TaskStatus,
    pub outbox_id: String,
    pub evidence_refs: Vec<EvidenceRef>,
}

/// Surface-safe result of one committed Task command.
///
/// The aggregate stays flattened on the wire so existing Task projections
/// retain their shape, while the durable receipt makes idempotency and
/// evidence projection directly inspectable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskCommandOutcome {
    #[serde(flatten)]
    pub aggregate: TaskAggregate,
    pub command_receipt: TaskCommandReceipt,
}

impl std::ops::Deref for TaskCommandOutcome {
    type Target = TaskAggregate;

    fn deref(&self) -> &Self::Target {
        &self.aggregate
    }
}
