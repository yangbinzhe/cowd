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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionInputId(String);

impl SessionInputId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("input-{}", Uuid::new_v4()))
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

impl Default for SessionInputId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionInputId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputSourceKind {
    Webui,
    Tui,
    Surface,
    Api,
    Agent,
    Steward,
    Runtime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputPayloadKind {
    Text,
    Resource,
    Media,
    Control,
    Approval,
    Clarification,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionInputStatus {
    Received,
    Persisted,
    Classified,
    AttachedToTurn,
    QueuedNext,
    InterruptRequested,
    DispatchedSubtask,
    DispatchedSession,
    NewSessionCreated,
    ControlResolved,
    Consumed,
    Cancelled,
    Failed,
    RejectedDuplicate,
    RejectedPolicy,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputRoutingDecision {
    StartNewTurn,
    SupplementCurrentTurn,
    InterruptAndReplan,
    EnqueueNextStep,
    SpawnSubtask,
    RouteCrossSession,
    CreateNewSession,
    ControlOrApproval,
    RejectDuplicate,
    RejectPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnInputCheckpoint {
    TurnStart,
    BeforeProviderRequest,
    AfterProviderResponse,
    AfterToolResult,
    BeforeFinalAnswer,
    BeforeCompaction,
}

impl TurnInputCheckpoint {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TurnStart => "turn_start",
            Self::BeforeProviderRequest => "before_provider_request",
            Self::AfterProviderResponse => "after_provider_response",
            Self::AfterToolResult => "after_tool_result",
            Self::BeforeFinalAnswer => "before_final_answer",
            Self::BeforeCompaction => "before_compaction",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRoutingReason {
    pub code: String,
    pub summary: String,
    pub confidence_basis_points: u16,
}

impl InputRoutingReason {
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        summary: impl Into<String>,
        confidence_basis_points: u16,
    ) -> Self {
        Self {
            code: code.into(),
            summary: summary.into(),
            confidence_basis_points: confidence_basis_points.min(10_000),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInputEnvelope {
    pub input_id: SessionInputId,
    pub session_id: String,
    pub source_kind: InputSourceKind,
    pub payload_kind: InputPayloadKind,
    pub content: String,
    pub content_preview: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_message_id: Option<String>,
    pub idempotency_key: String,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

impl SessionInputEnvelope {
    #[must_use]
    pub fn text(
        session_id: impl Into<String>,
        source_kind: InputSourceKind,
        content: impl Into<String>,
    ) -> Self {
        let session_id = session_id.into();
        let content = content.into();
        let input_id = SessionInputId::new();
        let idempotency_key = format!("{session_id}:{}", input_id.as_str());
        Self {
            input_id,
            session_id,
            source_kind,
            payload_kind: InputPayloadKind::Text,
            content_preview: preview_chars(&content, 160),
            content,
            source_ref: None,
            source_message_id: None,
            idempotency_key,
            metadata: Value::Object(Default::default()),
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn with_payload_kind(mut self, payload_kind: InputPayloadKind) -> Self {
        self.payload_kind = payload_kind;
        self
    }

    #[must_use]
    pub fn with_source_ref(mut self, source_ref: impl Into<String>) -> Self {
        self.source_ref = Some(source_ref.into());
        self
    }

    #[must_use]
    pub fn with_source_message_id(mut self, source_message_id: impl Into<String>) -> Self {
        let source_message_id = source_message_id.into();
        self.idempotency_key = format!(
            "{}:{:?}:{}",
            self.session_id, self.source_kind, source_message_id
        );
        self.source_message_id = Some(source_message_id);
        self
    }

    #[must_use]
    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = idempotency_key.into();
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInputReceipt {
    pub input_id: SessionInputId,
    pub session_id: String,
    pub status: SessionInputStatus,
    pub decision: InputRoutingDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<InputRoutingReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<TurnId>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnInboxItem {
    pub input_id: SessionInputId,
    pub session_id: String,
    pub status: SessionInputStatus,
    pub decision: InputRoutingDecision,
    pub content_preview: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<TurnInputCheckpoint>,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnInboxSnapshot {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    pub pending_count: usize,
    pub consumed_count: usize,
    #[serde(default)]
    pub items: Vec<TurnInboxItem>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInputProjection {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<TurnId>,
    pub total: usize,
    pub pending_count: usize,
    pub queued_next_count: usize,
    pub consumed_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_decision: Option<InputRoutingDecision>,
    #[serde(default)]
    pub inputs: Vec<TurnInboxItem>,
    pub updated_at: DateTime<Utc>,
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
    pub mode_hint: Option<crate::core::ExecutionPattern>,
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

fn preview_chars(value: &str, max_chars: usize) -> String {
    let mut preview = String::new();
    for ch in value.chars().take(max_chars) {
        preview.push(ch);
    }
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
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

    #[test]
    fn session_input_envelope_uses_source_message_as_idempotency_key() {
        let envelope =
            SessionInputEnvelope::text("session-a", InputSourceKind::Surface, "supplement context")
                .with_source_message_id("msg-1");

        assert_eq!(envelope.session_id, "session-a");
        assert_eq!(envelope.source_message_id.as_deref(), Some("msg-1"));
        assert!(envelope.idempotency_key.contains("msg-1"));
        assert_eq!(envelope.content_preview, "supplement context");
    }
}
