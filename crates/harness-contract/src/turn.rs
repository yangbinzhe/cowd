use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TurnId(String);

impl TurnId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("turn-{}", Uuid::new_v4()))
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

impl Default for TurnId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TurnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Pending,
    Running,
    PendingApproval,
    Resuming,
    Completed,
    Failed,
    Denied,
    Fallback,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnInput {
    pub turn_id: TurnId,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub prompt: String,
    pub mode_hint: Option<crate::core::ExecutionMode>,
    pub created_at: DateTime<Utc>,
}

impl TurnInput {
    #[must_use]
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            turn_id: TurnId::new(),
            session_id: None,
            task_id: None,
            prompt: prompt.into(),
            mode_hint: None,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnEvent {
    pub turn_id: TurnId,
    pub status: TurnStatus,
    pub message: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl TurnEvent {
    #[must_use]
    pub fn new(turn_id: TurnId, status: TurnStatus) -> Self {
        Self {
            turn_id,
            status,
            message: None,
            occurred_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnReceipt {
    pub turn_id: TurnId,
    pub status: TurnStatus,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub events: Vec<TurnEvent>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl TurnReceipt {
    #[must_use]
    pub fn from_input(input: &TurnInput, status: TurnStatus) -> Self {
        Self {
            turn_id: input.turn_id.clone(),
            status,
            session_id: input.session_id.clone(),
            task_id: input.task_id.clone(),
            events: Vec::new(),
            completed_at: None,
        }
    }
}
