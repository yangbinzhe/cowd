//! Lightweight tool invocation facts for runtime/session observability.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::tool_orchestrator::ToolSafetyCategory;
use crate::{RuntimeSessionEvent, RuntimeSessionEventKind, RuntimeSessionEventRef};

const INPUT_PREVIEW_CHARS: usize = 240;
const OUTPUT_PREVIEW_CHARS: usize = 500;
pub const DEFAULT_OUTPUT_REF_MIN_LINES: usize = 2000;
pub const TOOL_CONTRACT_VERSION: u32 = 2;

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
            Self::ExecutionError => "execution_error",
            Self::Timeout => "timeout",
            Self::Panic => "panic",
            Self::HookDenied => "hook_denied",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutputRef {
    pub ref_id: String,
    pub tool_call_id: String,
    pub line_count: usize,
    pub byte_count: usize,
    pub sha256: String,
    pub search_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationRecord {
    pub contract_version: u32,
    pub invocation_id: String,
    #[serde(default)]
    pub governed_plan_id: Option<String>,
    #[serde(default)]
    pub governed_plan_revision: Option<u64>,
    pub session_id: String,
    pub turn_index: usize,
    pub tool_call_id: String,
    pub tool_name: String,
    pub advertised_registration_id: String,
    pub effective_registration_id: String,
    pub input_hash: String,
    pub input_preview: String,
    pub model_visible_preview: String,
    pub safety_category: ToolSafetyCategory,
    pub status: ToolInvocationStatus,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub output_line_count: Option<usize>,
    pub output_byte_count: Option<usize>,
    pub output_preview: Option<String>,
    pub output_ref: Option<ToolOutputRef>,
    pub full_output_ref: Option<String>,
    pub raw_output_tokens: Option<u64>,
    pub preview_tokens: Option<u64>,
    pub context_saved_tokens: Option<u64>,
    pub context_saved_ratio: Option<u16>,
    pub stale_registration: bool,
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
        let tool_name = tool_name.into();
        let input_preview = preview(input, INPUT_PREVIEW_CHARS);
        let registration_id = registration_id(&tool_name, safety_category);
        Self {
            contract_version: TOOL_CONTRACT_VERSION,
            invocation_id: format!("tool-inv-{}", Uuid::new_v4()),
            governed_plan_id: None,
            governed_plan_revision: None,
            session_id: session_id.into(),
            turn_index,
            tool_call_id: tool_call_id.into(),
            tool_name,
            advertised_registration_id: registration_id.registration_id.clone(),
            effective_registration_id: registration_id.registration_id,
            input_hash: stable_hash(input),
            input_preview: input_preview.clone(),
            model_visible_preview: input_preview,
            safety_category,
            status: ToolInvocationStatus::Running,
            started_at_ms,
            ended_at_ms: None,
            duration_ms: None,
            output_line_count: None,
            output_byte_count: None,
            output_preview: None,
            output_ref: None,
            full_output_ref: None,
            raw_output_tokens: None,
            preview_tokens: None,
            context_saved_tokens: None,
            context_saved_ratio: None,
            stale_registration: false,
            is_error: None,
            failure_kind: None,
        }
    }

    #[must_use]
    pub fn with_governed_plan(mut self, plan_id: impl Into<String>, plan_revision: u64) -> Self {
        self.governed_plan_id = Some(plan_id.into());
        self.governed_plan_revision = Some(plan_revision);
        self
    }

    #[must_use]
    pub fn with_effective_registration_id(
        mut self,
        effective_registration_id: impl Into<String>,
    ) -> Self {
        self.effective_registration_id = effective_registration_id.into();
        self.stale_registration = self.advertised_registration_id != self.effective_registration_id;
        self
    }

    #[must_use]
    pub fn with_full_output_ref(mut self, full_output_ref: impl Into<String>) -> Self {
        let full_output_ref = full_output_ref.into();
        if !full_output_ref.trim().is_empty() {
            if let Some(output_ref) = self.output_ref.as_mut() {
                output_ref.ref_id.clone_from(&full_output_ref);
                output_ref.search_hint = format!(
                    "Use evidence_retrieve with evidence_ref `{full_output_ref}` and a focused query."
                );
            }
            self.full_output_ref = Some(full_output_ref);
        }
        self
    }

    #[must_use]
    pub fn completed(mut self, output: &str, ended_at_ms: u64) -> Self {
        self = self.with_output_digest(output, DEFAULT_OUTPUT_REF_MIN_LINES);
        self.status = ToolInvocationStatus::Completed;
        self.ended_at_ms = Some(ended_at_ms);
        self.duration_ms = Some(ended_at_ms.saturating_sub(self.started_at_ms));
        self.is_error = Some(false);
        self.failure_kind = None;
        self
    }

    #[must_use]
    pub fn completed_with_output_policy(
        mut self,
        output: &str,
        ended_at_ms: u64,
        output_ref_min_lines: usize,
    ) -> Self {
        self = self.with_output_digest(output, output_ref_min_lines);
        self.status = ToolInvocationStatus::Completed;
        self.ended_at_ms = Some(ended_at_ms);
        self.duration_ms = Some(ended_at_ms.saturating_sub(self.started_at_ms));
        self.is_error = Some(false);
        self.failure_kind = None;
        self
    }

    #[must_use]
    pub fn failed(mut self, kind: ToolFailureKind, output: &str, ended_at_ms: u64) -> Self {
        self = self.with_output_digest(output, DEFAULT_OUTPUT_REF_MIN_LINES);
        self.apply_failure(kind, ended_at_ms)
    }

    #[must_use]
    pub fn failed_with_output_policy(
        mut self,
        kind: ToolFailureKind,
        output: &str,
        ended_at_ms: u64,
        output_ref_min_lines: usize,
    ) -> Self {
        self = self.with_output_digest(output, output_ref_min_lines);
        self.apply_failure(kind, ended_at_ms)
    }

    fn apply_failure(mut self, kind: ToolFailureKind, ended_at_ms: u64) -> Self {
        self.status = match kind {
            ToolFailureKind::Timeout => ToolInvocationStatus::TimedOut,
            ToolFailureKind::ApprovalDenied
            | ToolFailureKind::HookDenied
            | ToolFailureKind::PermissionDenied => ToolInvocationStatus::Denied,
            _ => ToolInvocationStatus::Failed,
        };
        self.ended_at_ms = Some(ended_at_ms);
        self.duration_ms = Some(ended_at_ms.saturating_sub(self.started_at_ms));
        self.is_error = Some(true);
        self.failure_kind = Some(kind);
        self
    }

    fn with_output_digest(mut self, output: &str, output_ref_min_lines: usize) -> Self {
        let line_count = output.lines().count();
        let byte_count = output.len();
        let output_preview = preview(output, OUTPUT_PREVIEW_CHARS);
        let raw_tokens = estimate_tokens(output);
        let preview_tokens = estimate_tokens(&output_preview);
        let saved_tokens = raw_tokens.saturating_sub(preview_tokens);
        self.output_line_count = Some(line_count);
        self.output_byte_count = Some(byte_count);
        self.output_preview = Some(output_preview.clone());
        self.model_visible_preview = output_preview;
        self.output_ref = large_output_ref(
            &self.tool_call_id,
            output,
            line_count,
            byte_count,
            output_ref_min_lines,
        );
        if let Some(output_ref) = &self.output_ref {
            self.full_output_ref = Some(output_ref.ref_id.clone());
        }
        self.raw_output_tokens = Some(raw_tokens);
        self.preview_tokens = Some(preview_tokens);
        self.context_saved_tokens = Some(saved_tokens);
        self.context_saved_ratio = Some(if raw_tokens == 0 {
            0
        } else {
            ((saved_tokens.saturating_mul(10_000)) / raw_tokens).min(10_000) as u16
        });
        self
    }

    #[must_use]
    pub fn to_runtime_event(
        &self,
        sequence: usize,
        kind: RuntimeSessionEventKind,
    ) -> RuntimeSessionEvent {
        let payload = serde_json::json!({
            "contract_version": self.contract_version,
            "invocation_id": self.invocation_id,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "turn_index": self.turn_index,
            "status": self.status.as_str(),
            "advertised_registration_id": self.advertised_registration_id,
            "effective_registration_id": self.effective_registration_id,
            "model_visible_preview": self.model_visible_preview,
            "safety_category": self.safety_category,
            "input_hash": self.input_hash,
            "input_preview": self.input_preview,
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "duration_ms": self.duration_ms,
            "output_line_count": self.output_line_count,
            "output_byte_count": self.output_byte_count,
            "output_preview": self.output_preview,
            "output_ref": self.output_ref,
            "full_output_ref": self.full_output_ref,
            "raw_output_tokens": self.raw_output_tokens,
            "preview_tokens": self.preview_tokens,
            "context_saved_tokens": self.context_saved_tokens,
            "context_saved_ratio": self.context_saved_ratio,
            "stale_registration": self.stale_registration,
            "is_error": self.is_error,
            "failure_kind": self.failure_kind.map(ToolFailureKind::as_str),
        });
        let mut event = RuntimeSessionEvent::new(
            self.session_id.clone(),
            sequence,
            kind,
            payload,
            self.ended_at_ms.unwrap_or(self.started_at_ms),
        );
        event.status = Some(self.status.as_str().to_string());
        event.span_id = Some(self.invocation_id.clone());
        event.correlation_id = Some(self.tool_call_id.clone());
        event.refs = vec![
            RuntimeSessionEventRef {
                ref_type: "tool_invocation".to_string(),
                id: self.invocation_id.clone(),
                label: Some(self.tool_name.clone()),
            },
            RuntimeSessionEventRef {
                ref_type: "tool_call".to_string(),
                id: self.tool_call_id.clone(),
                label: Some(self.tool_name.clone()),
            },
            RuntimeSessionEventRef {
                ref_type: "tool".to_string(),
                id: self.tool_name.clone(),
                label: None,
            },
        ];
        event
    }

    #[must_use]
    pub fn evidence_reference(&self) -> String {
        self.full_output_ref
            .as_ref()
            .cloned()
            .or_else(|| {
                self.output_ref
                    .as_ref()
                    .map(|reference| reference.ref_id.clone())
            })
            .unwrap_or_else(|| self.invocation_id.clone())
    }

    #[must_use]
    pub fn evidence_summary(&self) -> String {
        let mut parts = vec![
            format!("tool `{}`", self.tool_name),
            format!("status {}", self.status.as_str()),
        ];
        if let Some(duration_ms) = self.duration_ms {
            parts.push(format!("duration {duration_ms}ms"));
        }
        if let Some(line_count) = self.output_line_count {
            parts.push(format!("output {line_count} lines"));
        }
        if let Some(failure_kind) = self.failure_kind {
            parts.push(format!("failure {}", failure_kind.as_str()));
        }
        if self.output_ref.is_some() {
            parts.push("large output indexed by reference".to_string());
        }
        parts.join(", ")
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

struct RegistrationId {
    registration_id: String,
}

fn registration_id(tool_name: &str, safety_category: ToolSafetyCategory) -> RegistrationId {
    let normalized = tool_name.trim().to_ascii_lowercase().replace(' ', "_");
    let safety = match safety_category {
        ToolSafetyCategory::ReadOnly => "read_only",
        ToolSafetyCategory::WriteLocal => "write_local",
        ToolSafetyCategory::Network => "network",
        ToolSafetyCategory::Destructive => "destructive",
    };
    RegistrationId {
        registration_id: format!(
            "tool-reg:v{}:{}:{}",
            TOOL_CONTRACT_VERSION, safety, normalized
        ),
    }
}

fn estimate_tokens(content: &str) -> u64 {
    (content.chars().count() as u64).div_ceil(4).max(1)
}

fn large_output_ref(
    tool_call_id: &str,
    output: &str,
    line_count: usize,
    byte_count: usize,
    output_ref_min_lines: usize,
) -> Option<ToolOutputRef> {
    if line_count < output_ref_min_lines && byte_count < 16_000 {
        return None;
    }
    let sha256 = stable_hash(output);
    let short_hash: String = sha256.chars().take(16).collect();
    Some(ToolOutputRef {
        ref_id: format!("tool-output:{tool_call_id}:{short_hash}"),
        tool_call_id: tool_call_id.to_string(),
        line_count,
        byte_count,
        sha256,
        search_hint: format!("Tool output `{tool_call_id}` will receive an evidence reference."),
    })
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
    fn tool_contract_v2_runtime_event_has_scope_refs_and_savings() {
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

        let event = record.to_runtime_event(9, RuntimeSessionEventKind::ToolInvocationCompleted);

        assert_eq!(event.kind.scope(), session::SessionDomainScope::Tool);
        assert_eq!(event.status.as_deref(), Some("completed"));
        assert_eq!(event.payload["contract_version"], TOOL_CONTRACT_VERSION);
        assert_eq!(event.payload["stale_registration"], false);
        assert!(event.payload["advertised_registration_id"]
            .as_str()
            .unwrap()
            .starts_with("tool-reg:v2:read_only:read"));
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
        let event = record.to_runtime_event(10, RuntimeSessionEventKind::ToolInvocationFailed);
        assert_eq!(event.payload["failure_kind"], "execution_error");
        assert_eq!(event.payload["output_preview"], "boom");
    }

    #[test]
    fn large_output_event_uses_reference_without_full_body() {
        let output = (0..80)
            .map(|idx| {
                format!(
                    "line {idx} unique-large-output-token-{idx} {}",
                    "x".repeat(24)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let record = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-large",
            "bash",
            "generate",
            ToolSafetyCategory::WriteLocal,
            200,
        )
        .completed_with_output_policy(&output, 250, 3);

        let event = record.to_runtime_event(10, RuntimeSessionEventKind::ToolInvocationCompleted);
        assert_eq!(event.payload["output_line_count"], 80);
        assert_eq!(event.payload["output_ref"]["tool_call_id"], "toolu-large");
        assert!(event.payload["raw_output_tokens"].as_u64().unwrap() > 0);
        assert!(event.payload["preview_tokens"].as_u64().unwrap() > 0);
        assert!(event.payload["context_saved_tokens"].as_u64().unwrap() > 0);
        assert!(event.payload["context_saved_ratio"].as_u64().unwrap() > 0);
        assert!(event.payload["full_output_ref"]
            .as_str()
            .unwrap()
            .starts_with("tool-output:toolu-large"));
        assert_eq!(
            event.payload["output_ref"]["search_hint"],
            "Tool output `toolu-large` will receive an evidence reference."
        );
        let serialized = serde_json::to_string(&event).unwrap();
        assert!(!serialized.contains("unique-large-output-token-79"));
    }

    #[test]
    fn immutable_evidence_ref_replaces_the_transient_output_hint() {
        let output = "large evidence\n".repeat(100);
        let record = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-large",
            "bash",
            "generate",
            ToolSafetyCategory::WriteLocal,
            200,
        )
        .completed_with_output_policy(&output, 250, 3)
        .with_full_output_ref("tool://tool-raw-toolu-large-deadbeef");

        assert_eq!(
            record.evidence_reference(),
            "tool://tool-raw-toolu-large-deadbeef"
        );
        assert!(record
            .output_ref
            .as_ref()
            .is_some_and(|reference| reference.search_hint.contains("evidence_retrieve")));
    }

    #[test]
    fn agent_evidence_summary_uses_refs_for_large_output() {
        let output = (0..80)
            .map(|idx| format!("line {idx} {}", "x".repeat(24)))
            .collect::<Vec<_>>()
            .join("\n");
        let record = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-large",
            "bash",
            "generate",
            ToolSafetyCategory::WriteLocal,
            200,
        )
        .completed_with_output_policy(&output, 250, 3);

        assert!(record.evidence_reference().starts_with("tool-output:"));
        let summary = record.evidence_summary();
        assert!(summary.contains("tool `bash`"));
        assert!(summary.contains("large output indexed by reference"));
        assert!(!summary.contains("line 79"));
    }

    #[test]
    fn stale_registration_is_computed_from_advertised_and_effective_ids() {
        let record = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-stale",
            "read",
            "{}",
            ToolSafetyCategory::ReadOnly,
            200,
        )
        .with_effective_registration_id("tool-reg:v2:read_only:read:newer");

        assert!(record.stale_registration);
        let event = record.to_runtime_event(10, RuntimeSessionEventKind::ToolInvocationStarted);
        assert_eq!(event.payload["stale_registration"], true);
    }
}
