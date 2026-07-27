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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskPhaseTerminalReceipt {
    pub outcome: String,
    pub evidence_refs: Vec<EvidenceRef>,
    pub completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPhaseArtifact {
    pub kind: String,
    pub label: String,
    pub value: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskGraphRef {
    pub graph_id: String,
    pub revision: u64,
    pub linked_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskAggregate {
    pub task_id: String,
    pub mission_id: String,
    pub source_session_id: String,
    pub source_turn_id: String,
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
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
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
}

impl TaskSpec {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            objective: objective.into(),
            phases: Vec::new(),
            execution_policy: TaskExecutionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskCreateCommand {
    pub task_id: String,
    pub mission_id: String,
    pub source_session_id: String,
    pub source_turn_id: String,
    pub spec: TaskSpec,
    pub evidence_refs: Vec<EvidenceRef>,
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
