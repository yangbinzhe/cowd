use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

impl TurnStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::PendingApproval => "pending_approval",
            Self::Resuming => "resuming",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Denied => "denied",
            Self::Fallback => "fallback",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnJournalPhase {
    Submitted,
    Running,
    AssistantStarted,
    ToolStarted,
    ToolProgress,
    ToolCompleted,
    ToolFailed,
    ApprovalRequested,
    ApprovalResolved,
    ContextCompressed,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl TurnJournalPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Running => "running",
            Self::AssistantStarted => "assistant_started",
            Self::ToolStarted => "tool_started",
            Self::ToolProgress => "tool_progress",
            Self::ToolCompleted => "tool_completed",
            Self::ToolFailed => "tool_failed",
            Self::ApprovalRequested => "approval_requested",
            Self::ApprovalResolved => "approval_resolved",
            Self::ContextCompressed => "context_compressed",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnJournalEnvelope {
    pub session_id: String,
    pub turn_id: TurnId,
    pub event_id: String,
    pub sequence: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub event_type: String,
    pub phase: TurnJournalPhase,
    pub source: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

impl TurnJournalEnvelope {
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        turn_id: TurnId,
        phase: TurnJournalPhase,
        source: impl Into<String>,
        payload: Value,
    ) -> Self {
        let session_id = session_id.into();
        let event_type = format!("turn.{}", phase.as_str());
        let idempotency_key = format!("{session_id}:{turn_id}:{event_type}");
        Self {
            session_id,
            turn_id,
            event_id: format!("turn-event-{}", Uuid::new_v4()),
            sequence: 0,
            run_id: None,
            event_type,
            phase,
            source: source.into(),
            idempotency_key,
            payload,
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn with_sequence(mut self, sequence: usize) -> Self {
        self.sequence = sequence;
        self
    }

    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    #[must_use]
    pub fn with_event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_type = event_type.into();
        self.idempotency_key = format!("{}:{}:{}", self.session_id, self.turn_id, self.event_type);
        self
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_report_id: Option<String>,
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
            context_report_id: None,
            completed_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_journal_envelope_has_stable_idempotency_key() {
        let envelope = TurnJournalEnvelope::new(
            "session-a",
            TurnId::from_string("turn-test"),
            TurnJournalPhase::Submitted,
            "gateway.runtime_service",
            serde_json::json!({"prompt": "hello"}),
        );

        assert_eq!(envelope.event_type, "turn.submitted");
        assert_eq!(envelope.phase, TurnJournalPhase::Submitted);
        assert_eq!(
            envelope.idempotency_key,
            "session-a:turn-test:turn.submitted"
        );
    }
}
