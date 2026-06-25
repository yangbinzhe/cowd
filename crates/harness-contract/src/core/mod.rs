//! Shared contracts for Cowd AI kernel crates.
//!
//! This crate intentionally has no dependency on `runtime`, `gateway`, or UI
//! crates. It defines the stable language used by strategy, task, context,
//! verification, workgraph, transaction, and evaluation layers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type AiKernelResult<T> = Result<T, AiKernelError>;

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum AiKernelError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("policy denied: {0}")]
    PolicyDenied(String),
    #[error("degraded: {0}")]
    Degraded(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl AiKernelError {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::Unsupported(_) => "unsupported",
            Self::Conflict(_) => "conflict",
            Self::PolicyDenied(_) => "policy_denied",
            Self::Degraded(_) => "degraded",
            Self::Internal(_) => "internal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskComplexity {
    Trivial,
    Simple,
    Moderate,
    Complex,
    Strategic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    DirectAnswer,
    FastEdit,
    ExploreThenAnswer,
    PlanExecute,
    ReActLoop,
    DeliberationSearch,
    ReflexionRetry,
    SupervisorSubagents,
    ParallelReadFanout,
    ParallelWorktree,
    BackgroundReview,
    RiskGate,
    HumanConfirm,
}

impl ExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectAnswer => "direct_answer",
            Self::FastEdit => "fast_edit",
            Self::ExploreThenAnswer => "explore_then_answer",
            Self::PlanExecute => "plan_execute",
            Self::ReActLoop => "react_loop",
            Self::DeliberationSearch => "deliberation_search",
            Self::ReflexionRetry => "reflexion_retry",
            Self::SupervisorSubagents => "supervisor_subagents",
            Self::ParallelReadFanout => "parallel_read_fanout",
            Self::ParallelWorktree => "parallel_worktree",
            Self::BackgroundReview => "background_review",
            Self::RiskGate => "risk_gate",
            Self::HumanConfirm => "human_confirm",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDecorator {
    WithExternalResearch,
    WithSemanticRetrieval,
    WithSymbolGraph,
    WithMemoryRecall,
    WithMatrixEvidence,
    WithCheckpoint,
    WithVerifier,
    WithReviewer,
    WithGuardrails,
    WithTrace,
    WithReflection,
    WithWorktreeIsolation,
}

impl StrategyDecorator {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WithExternalResearch => "with_external_research",
            Self::WithSemanticRetrieval => "with_semantic_retrieval",
            Self::WithSymbolGraph => "with_symbol_graph",
            Self::WithMemoryRecall => "with_memory_recall",
            Self::WithMatrixEvidence => "with_matrix_evidence",
            Self::WithCheckpoint => "with_checkpoint",
            Self::WithVerifier => "with_verifier",
            Self::WithReviewer => "with_reviewer",
            Self::WithGuardrails => "with_guardrails",
            Self::WithTrace => "with_trace",
            Self::WithReflection => "with_reflection",
            Self::WithWorktreeIsolation => "with_worktree_isolation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelRef {
    pub ref_type: String,
    pub id: String,
    pub label: Option<String>,
}

impl KernelRef {
    #[must_use]
    pub fn new(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            ref_type: ref_type.into(),
            id: id.into(),
            label: None,
        }
    }

    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRef(pub KernelRef);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRef(pub KernelRef);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRef(pub KernelRef);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRef(pub KernelRef);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRef(pub KernelRef);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRef(pub KernelRef);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KernelEvent {
    pub event_id: String,
    pub kind: String,
    pub refs: Vec<KernelRef>,
    pub created_at: DateTime<Utc>,
}

impl KernelEvent {
    #[must_use]
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            event_id: format!("kernel-event-{}", uuid::Uuid::new_v4()),
            kind: kind.into(),
            refs: Vec::new(),
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn with_ref(mut self, reference: KernelRef) -> Self {
        self.refs.push(reference);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelCapability {
    StrategyRouting,
    TaskControl,
    ContextEpoch,
    WorkGraph,
    ToolTransaction,
    VerificationLedger,
    Evaluation,
    GrowthLoop,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kind_is_stable_wire_value() {
        assert_eq!(
            AiKernelError::PolicyDenied("write denied".to_string()).kind(),
            "policy_denied"
        );
    }

    #[test]
    fn execution_mode_wire_names_are_stable() {
        assert_eq!(ExecutionMode::DirectAnswer.as_str(), "direct_answer");
        assert_eq!(ExecutionMode::PlanExecute.as_str(), "plan_execute");
    }

    #[test]
    fn kernel_event_has_id_and_kind() {
        let event = KernelEvent::new("strategy.decided")
            .with_ref(KernelRef::new("task", "task-1").with_label("demo"));
        assert!(event.event_id.starts_with("kernel-event-"));
        assert_eq!(event.kind, "strategy.decided");
        assert_eq!(event.refs.len(), 1);
    }
}
