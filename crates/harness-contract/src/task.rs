use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Proposed,
    Active,
    Blocked,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhaseStatus {
    Pending,
    Active,
    Completed,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPhase {
    pub id: String,
    pub title: String,
    pub status: TaskPhaseStatus,
    pub acceptance: Vec<String>,
}

impl TaskPhase {
    #[must_use]
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            id: format!("phase-{}", Uuid::new_v4()),
            title: title.into(),
            status: TaskPhaseStatus::Pending,
            acceptance: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub objective: String,
    pub status: TaskStatus,
    pub phases: Vec<TaskPhase>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TaskSpec {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: TaskId::new(),
            objective: objective.into(),
            status: TaskStatus::Proposed,
            phases: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTurnBinding {
    pub task_id: TaskId,
    pub turn_id: crate::turn::TurnId,
    pub session_id: Option<String>,
    pub bound_at: DateTime<Utc>,
}

impl TaskTurnBinding {
    #[must_use]
    pub fn new(task_id: TaskId, turn_id: crate::turn::TurnId) -> Self {
        Self {
            task_id,
            turn_id,
            session_id: None,
            bound_at: Utc::now(),
        }
    }
}
