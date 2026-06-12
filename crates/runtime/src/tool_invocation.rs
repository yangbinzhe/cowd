//! Lightweight tool invocation facts for runtime/session observability.

use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::tool_orchestrator::ToolSafetyCategory;

const INPUT_PREVIEW_CHARS: usize = 240;
const OUTPUT_PREVIEW_CHARS: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolInvocationStatus {
    Running,
    Completed,
    Failed,
    Denied,
    TimedOut,
}

impl ToolInvocationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Denied => "denied",
            Self::TimedOut => "timed_out",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolFailureKind {
    PermissionDenied,
    ApprovalDenied,
    GateDenied,
    ExecutionError,
    Timeout,
    Panic,
    HookDenied,
    Unknown,
}

impl ToolFailureKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PermissionDenied => "permission_denied",
            Self::ApprovalDenied => "approval_denied",
            Self::GateDenied => "gate_denied",
            Self::ExecutionError => "execution_error",
            Self::Timeout => "timeout",
            Self::Panic => "panic",
            Self::HookDenied => "hook_denied",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationRecord {
    pub invocation_id: String,
    pub session_id: String,
    pub turn_index: usize,
    pub tool_call_id: String,
    pub tool_name: String,
    pub input_hash: String,
    pub input_preview: String,
    pub safety_category: ToolSafetyCategory,
    pub status: ToolInvocationStatus,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub output_preview: Option<String>,
    pub is_error: Option<bool>,
    pub failure_kind: Option<ToolFailureKind>,
}

impl ToolInvocationRecord {
    #[must_use]
    pub fn started(
        session_id: impl Into<String>,
        turn_index: usize,
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        input: &str,
        safety_category: ToolSafetyCategory,
        started_at_ms: u64,
    ) -> Self {
        Self {
            invocation_id: format!("tool-inv-{}", Uuid::new_v4()),
            session_id: session_id.into(),
            turn_index,
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            input_hash: stable_hash(input),
            input_preview: preview(input, INPUT_PREVIEW_CHARS),
            safety_category,
            status: ToolInvocationStatus::Running,
            started_at_ms,
            ended_at_ms: None,
            duration_ms: None,
            output_preview: None,
            is_error: None,
            failure_kind: None,
        }
    }

    #[must_use]
    pub fn completed(mut self, output: &str, ended_at_ms: u64) -> Self {
        self.status = ToolInvocationStatus::Completed;
        self.ended_at_ms = Some(ended_at_ms);
        self.duration_ms = Some(ended_at_ms.saturating_sub(self.started_at_ms));
        self.output_preview = Some(preview(output, OUTPUT_PREVIEW_CHARS));
        self.is_error = Some(false);
        self.failure_kind = None;
        self
    }

    #[must_use]
    pub fn failed(mut self, kind: ToolFailureKind, output: &str, ended_at_ms: u64) -> Self {
        self.status = match kind {
            ToolFailureKind::Timeout => ToolInvocationStatus::TimedOut,
            ToolFailureKind::ApprovalDenied
            | ToolFailureKind::GateDenied
            | ToolFailureKind::HookDenied
            | ToolFailureKind::PermissionDenied => ToolInvocationStatus::Denied,
            _ => ToolInvocationStatus::Failed,
        };
        self.ended_at_ms = Some(ended_at_ms);
        self.duration_ms = Some(ended_at_ms.saturating_sub(self.started_at_ms));
        self.output_preview = Some(preview(output, OUTPUT_PREVIEW_CHARS));
        self.is_error = Some(true);
        self.failure_kind = Some(kind);
        self
    }

    #[must_use]
    pub fn to_runtime_event(&self, sequence: usize, kind: impl Into<String>) -> RuntimeEvent {
        let payload = serde_json::json!({
            "invocation_id": self.invocation_id,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "turn_index": self.turn_index,
            "status": self.status.as_str(),
            "safety_category": self.safety_category,
            "input_hash": self.input_hash,
            "input_preview": self.input_preview,
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "duration_ms": self.duration_ms,
            "output_preview": self.output_preview,
            "is_error": self.is_error,
            "failure_kind": self.failure_kind.map(ToolFailureKind::as_str),
        });
        let mut event = RuntimeEvent::new(
            self.session_id.clone(),
            sequence,
            RuntimeEventScope::Tool,
            kind,
            payload,
            self.ended_at_ms.unwrap_or(self.started_at_ms),
        );
        event.status = Some(self.status.as_str().to_string());
        event.span_id = Some(self.invocation_id.clone());
        event.correlation_id = Some(self.tool_call_id.clone());
        event.refs = vec![
            RuntimeRef {
                ref_type: "tool_invocation".to_string(),
                id: self.invocation_id.clone(),
                label: Some(self.tool_name.clone()),
            },
            RuntimeRef {
                ref_type: "tool_call".to_string(),
                id: self.tool_call_id.clone(),
                label: Some(self.tool_name.clone()),
            },
            RuntimeRef {
                ref_type: "tool".to_string(),
                id: self.tool_name.clone(),
                label: None,
            },
        ];
        event
    }
}

#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn stable_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn preview(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_record_has_stable_input_hash_and_preview() {
        let input = "x".repeat(INPUT_PREVIEW_CHARS + 20);
        let first = ToolInvocationRecord::started(
            "session-1",
            3,
            "toolu-1",
            "read",
            &input,
            ToolSafetyCategory::ReadOnly,
            100,
        );
        let second = ToolInvocationRecord::started(
            "session-1",
            3,
            "toolu-2",
            "read",
            &input,
            ToolSafetyCategory::ReadOnly,
            101,
        );

        assert_eq!(first.input_hash, second.input_hash);
        assert_eq!(first.input_preview.chars().count(), INPUT_PREVIEW_CHARS);
        assert_eq!(first.status, ToolInvocationStatus::Running);
    }

    #[test]
    fn completed_record_runtime_event_has_tool_scope_refs() {
        let record = ToolInvocationRecord::started(
            "session-1",
            4,
            "toolu-1",
            "read",
            "{\"path\":\"README.md\"}",
            ToolSafetyCategory::ReadOnly,
            100,
        )
        .completed("ok", 125);

        let event = record.to_runtime_event(9, "tool.invocation.completed");

        assert_eq!(event.scope, RuntimeEventScope::Tool);
        assert_eq!(event.status.as_deref(), Some("completed"));
        assert_eq!(event.payload["duration_ms"], 25);
        assert!(event
            .refs
            .iter()
            .any(|reference| { reference.ref_type == "tool_call" && reference.id == "toolu-1" }));
        assert!(event
            .refs
            .iter()
            .any(|reference| { reference.ref_type == "tool" && reference.id == "read" }));
    }

    #[test]
    fn failed_record_preserves_failure_kind() {
        let record = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-1",
            "bash",
            "exit 1",
            ToolSafetyCategory::Destructive,
            200,
        )
        .failed(ToolFailureKind::ExecutionError, "boom", 240);

        assert_eq!(record.status, ToolInvocationStatus::Failed);
        assert_eq!(record.failure_kind, Some(ToolFailureKind::ExecutionError));
        let event = record.to_runtime_event(10, "tool.invocation.failed");
        assert_eq!(event.payload["failure_kind"], "execution_error");
        assert_eq!(event.payload["output_preview"], "boom");
    }
}
