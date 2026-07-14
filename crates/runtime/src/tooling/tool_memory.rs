//! Policy-gated conversion from tool invocation facts to memory candidates.

use crate::tool_invocation::{ToolFailureKind, ToolInvocationRecord, ToolInvocationStatus};
use chrono::Utc;
use memory::{MaintenanceCandidate, MaintenanceCandidateAction, MaintenanceCandidateStatus};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolMemoryCandidatePolicy {
    pub capture_failures: bool,
    pub capture_slow_tools: bool,
    pub slow_tool_ms: u64,
}

impl Default for ToolMemoryCandidatePolicy {
    fn default() -> Self {
        Self {
            capture_failures: true,
            capture_slow_tools: true,
            slow_tool_ms: 30_000,
        }
    }
}

#[must_use]
pub fn memory_candidate_from_tool_invocation(
    invocation: &ToolInvocationRecord,
    policy: &ToolMemoryCandidatePolicy,
) -> Option<MaintenanceCandidate> {
    if policy.capture_failures && is_memory_worthy_failure(invocation.failure_kind) {
        return Some(maintenance_candidate(
            MaintenanceCandidateAction::Remember,
            "Review tool failure pattern",
            format_candidate_content("tool failure pattern", invocation),
            0.7,
            invocation.evidence_reference(),
        ));
    }

    let is_slow_success = invocation.status == ToolInvocationStatus::Completed
        && invocation
            .duration_ms
            .is_some_and(|duration_ms| duration_ms >= policy.slow_tool_ms);
    if policy.capture_slow_tools && is_slow_success {
        return Some(maintenance_candidate(
            MaintenanceCandidateAction::Refresh,
            "Refresh slow tool observation",
            format_candidate_content("slow tool observation", invocation),
            0.6,
            invocation.evidence_reference(),
        ));
    }

    None
}

fn maintenance_candidate(
    action: MaintenanceCandidateAction,
    summary: &str,
    reason: String,
    confidence: f32,
    source_ref: String,
) -> MaintenanceCandidate {
    let now = Utc::now();
    MaintenanceCandidate {
        id: Uuid::new_v4().to_string(),
        kind: action.candidate_kind(),
        status: MaintenanceCandidateStatus::Open,
        entry_ids: Vec::new(),
        summary: summary.to_string(),
        reason: format!("memory_action={}; {reason}", action.as_str()),
        confidence,
        source: Some("tool_invocation".to_string()),
        source_ref: Some(source_ref),
        created_at: now,
        updated_at: now,
    }
}

fn is_memory_worthy_failure(kind: Option<ToolFailureKind>) -> bool {
    matches!(
        kind,
        Some(ToolFailureKind::ExecutionError | ToolFailureKind::Timeout | ToolFailureKind::Panic)
    )
}

fn format_candidate_content(label: &str, invocation: &ToolInvocationRecord) -> String {
    format!(
        "{label}; source=tool_invocation:{}; tool={}; status={}; ref={}; summary={}",
        invocation.invocation_id,
        invocation.tool_name,
        invocation.status.as_str(),
        invocation.evidence_reference(),
        invocation.evidence_summary()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_invocation::ToolInvocationRecord;
    use crate::tool_orchestrator::ToolSafetyCategory;

    #[test]
    fn ordinary_success_does_not_create_memory_candidate() {
        let invocation = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-1",
            "read",
            "README.md",
            ToolSafetyCategory::ReadOnly,
            100,
        )
        .completed("ok", 120);

        let candidate = memory_candidate_from_tool_invocation(
            &invocation,
            &ToolMemoryCandidatePolicy::default(),
        );

        assert!(candidate.is_none());
    }

    #[test]
    fn execution_failure_creates_reviewable_memory_candidate() {
        let invocation = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-1",
            "bash",
            "scripts/validate.sh unit-fast",
            ToolSafetyCategory::WriteLocal,
            100,
        )
        .failed(ToolFailureKind::ExecutionError, "test failed", 140);

        let candidate = memory_candidate_from_tool_invocation(
            &invocation,
            &ToolMemoryCandidatePolicy::default(),
        )
        .unwrap();

        assert_eq!(
            candidate.kind,
            memory::MaintenanceCandidateKind::RelationshipRefresh
        );
        assert_eq!(candidate.status, MaintenanceCandidateStatus::Open);
        assert!(candidate.entry_ids.is_empty());
        assert_eq!(candidate.source.as_deref(), Some("tool_invocation"));
        assert_eq!(
            candidate.source_ref.as_deref(),
            Some(invocation.invocation_id.as_str())
        );
        assert!(candidate.reason.contains("memory_action=remember"));
        assert!(candidate.reason.contains("tool failure pattern"));
        assert!(candidate.reason.contains("tool=bash"));
        assert!(!candidate.reason.contains("test failed"));
    }

    #[test]
    fn slow_completed_tool_creates_refresh_candidate() {
        let policy = ToolMemoryCandidatePolicy {
            slow_tool_ms: 25,
            ..ToolMemoryCandidatePolicy::default()
        };
        let invocation = ToolInvocationRecord::started(
            "session-1",
            1,
            "toolu-1",
            "web_fetch",
            "{}",
            ToolSafetyCategory::Network,
            100,
        )
        .completed("ok", 130);

        let candidate = memory_candidate_from_tool_invocation(&invocation, &policy).unwrap();

        assert_eq!(
            candidate.kind,
            memory::MaintenanceCandidateKind::RelationshipRefresh
        );
        assert_eq!(candidate.status, MaintenanceCandidateStatus::Open);
        assert!(candidate.entry_ids.is_empty());
        assert!(candidate.reason.contains("memory_action=refresh"));
        assert!(candidate.reason.contains("slow tool observation"));
        assert!(candidate.reason.contains("duration 30ms"));
    }
}
