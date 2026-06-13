//! Policy-gated conversion from tool invocation facts to memory candidates.

use crate::agent_collaboration::{MemoryPulseCandidate, MemoryPulseKind};
use crate::tool_invocation::{ToolFailureKind, ToolInvocationRecord, ToolInvocationStatus};

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
) -> Option<MemoryPulseCandidate> {
    if policy.capture_failures && is_memory_worthy_failure(invocation.failure_kind) {
        return Some(MemoryPulseCandidate {
            kind: MemoryPulseKind::Remember,
            content: format_candidate_content("tool failure pattern", invocation),
        });
    }

    let is_slow_success = invocation.status == ToolInvocationStatus::Completed
        && invocation
            .duration_ms
            .is_some_and(|duration_ms| duration_ms >= policy.slow_tool_ms);
    if policy.capture_slow_tools && is_slow_success {
        return Some(MemoryPulseCandidate {
            kind: MemoryPulseKind::Refresh,
            content: format_candidate_content("slow tool observation", invocation),
        });
    }

    None
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
            "npm test",
            ToolSafetyCategory::WriteLocal,
            100,
        )
        .failed(ToolFailureKind::ExecutionError, "test failed", 140);

        let candidate = memory_candidate_from_tool_invocation(
            &invocation,
            &ToolMemoryCandidatePolicy::default(),
        )
        .unwrap();

        assert_eq!(candidate.kind, MemoryPulseKind::Remember);
        assert!(candidate.content.contains("tool failure pattern"));
        assert!(candidate.content.contains("tool=bash"));
        assert!(!candidate.content.contains("test failed"));
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

        assert_eq!(candidate.kind, MemoryPulseKind::Refresh);
        assert!(candidate.content.contains("slow tool observation"));
        assert!(candidate.content.contains("duration 30ms"));
    }
}
