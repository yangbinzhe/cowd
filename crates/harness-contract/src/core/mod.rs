//! Shared contracts for Cowd AI kernel crates.
//!
//! This crate intentionally has no dependency on `runtime`, `gateway`, or UI
//! crates. It defines the stable language used by strategy, task, context,
//! verification, execution_graph, transaction, and evaluation layers.

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
pub enum ExecutionPattern {
    Direct,
    Explore,
    Execute,
    Deliberate,
    Collaborate,
    Supervise,
}

impl ExecutionPattern {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Explore => "explore",
            Self::Execute => "execute",
            Self::Deliberate => "deliberate",
            Self::Collaborate => "collaborate",
            Self::Supervise => "supervise",
        }
    }

    #[must_use]
    pub const fn supported_modifiers(self) -> &'static [ExecutionModifier] {
        use ExecutionModifier::{
            Background, BoundedChange, Parallel, WithCheckpoint, WithExternalResearch,
            WithGuardrails, WithMatrixEvidence, WithReviewer, WithTrace, WithVerifier,
            WithWorktreeIsolation,
        };

        match self {
            Self::Direct => &[WithTrace],
            Self::Explore => &[Parallel, WithExternalResearch, WithTrace],
            Self::Execute => &[
                BoundedChange,
                Parallel,
                WithExternalResearch,
                WithCheckpoint,
                WithVerifier,
                WithGuardrails,
            ],
            Self::Deliberate => &[
                Parallel,
                WithExternalResearch,
                WithReviewer,
                WithMatrixEvidence,
                WithTrace,
            ],
            Self::Collaborate => &[
                Parallel,
                WithExternalResearch,
                WithReviewer,
                WithTrace,
                WithVerifier,
                WithWorktreeIsolation,
            ],
            Self::Supervise => &[
                Background,
                Parallel,
                WithExternalResearch,
                WithCheckpoint,
                WithReviewer,
                WithTrace,
            ],
        }
    }

    #[must_use]
    pub fn supports_modifier(self, modifier: ExecutionModifier) -> bool {
        self.supported_modifiers().contains(&modifier)
    }

    #[must_use]
    pub const fn supported_gates(self) -> &'static [ExecutionPolicyGate] {
        use ExecutionPolicyGate::{Approval, Budget, Permission, Risk};

        match self {
            Self::Direct => &[Budget],
            Self::Explore => &[Budget, Permission],
            Self::Execute | Self::Collaborate | Self::Supervise => {
                &[Budget, Permission, Risk, Approval]
            }
            Self::Deliberate => &[Budget, Risk],
        }
    }

    #[must_use]
    pub fn supports_gate(self, gate: ExecutionPolicyGate) -> bool {
        self.supported_gates().contains(&gate)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionModifier {
    Parallel,
    BoundedChange,
    Background,
    WithExternalResearch,
    WithMatrixEvidence,
    WithCheckpoint,
    WithVerifier,
    WithReviewer,
    WithGuardrails,
    WithTrace,
    WithWorktreeIsolation,
}

impl ExecutionModifier {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parallel => "parallel",
            Self::BoundedChange => "bounded_change",
            Self::Background => "background",
            Self::WithExternalResearch => "with_external_research",
            Self::WithMatrixEvidence => "with_matrix_evidence",
            Self::WithCheckpoint => "with_checkpoint",
            Self::WithVerifier => "with_verifier",
            Self::WithReviewer => "with_reviewer",
            Self::WithGuardrails => "with_guardrails",
            Self::WithTrace => "with_trace",
            Self::WithWorktreeIsolation => "with_worktree_isolation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPolicyGate {
    Risk,
    Approval,
    Permission,
    Budget,
}

impl ExecutionPolicyGate {
    pub const ALL: [Self; 4] = [Self::Budget, Self::Permission, Self::Risk, Self::Approval];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Risk => "risk",
            Self::Approval => "approval",
            Self::Permission => "permission",
            Self::Budget => "budget",
        }
    }

    #[must_use]
    pub const fn is_required_for(self, risk: TaskRisk, requires_write: bool) -> bool {
        match self {
            Self::Budget => true,
            Self::Permission => requires_write,
            Self::Risk => matches!(risk, TaskRisk::High | TaskRisk::Critical),
            Self::Approval => matches!(risk, TaskRisk::Critical),
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
    ExecutionGraph,
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
    fn execution_pattern_wire_names_are_stable() {
        assert_eq!(ExecutionPattern::Direct.as_str(), "direct");
        assert_eq!(ExecutionPattern::Execute.as_str(), "execute");
    }

    #[test]
    fn execution_pattern_modifier_support_is_explicit() {
        use ExecutionModifier::{
            Background, BoundedChange, Parallel, WithCheckpoint, WithExternalResearch,
            WithGuardrails, WithMatrixEvidence, WithReviewer, WithTrace, WithVerifier,
            WithWorktreeIsolation,
        };

        assert_eq!(ExecutionPattern::Direct.supported_modifiers(), &[WithTrace]);
        assert_eq!(
            ExecutionPattern::Explore.supported_modifiers(),
            &[Parallel, WithExternalResearch, WithTrace]
        );
        assert_eq!(
            ExecutionPattern::Execute.supported_modifiers(),
            &[
                BoundedChange,
                Parallel,
                WithExternalResearch,
                WithCheckpoint,
                WithVerifier,
                WithGuardrails,
            ]
        );
        assert_eq!(
            ExecutionPattern::Deliberate.supported_modifiers(),
            &[
                Parallel,
                WithExternalResearch,
                WithReviewer,
                WithMatrixEvidence,
                WithTrace,
            ]
        );
        assert_eq!(
            ExecutionPattern::Collaborate.supported_modifiers(),
            &[
                Parallel,
                WithExternalResearch,
                WithReviewer,
                WithTrace,
                WithVerifier,
                WithWorktreeIsolation,
            ]
        );
        assert_eq!(
            ExecutionPattern::Supervise.supported_modifiers(),
            &[
                Background,
                Parallel,
                WithExternalResearch,
                WithCheckpoint,
                WithReviewer,
                WithTrace,
            ]
        );
    }

    #[test]
    fn execution_pattern_gate_support_is_explicit() {
        use ExecutionPolicyGate::{Approval, Budget, Permission, Risk};

        assert_eq!(ExecutionPattern::Direct.supported_gates(), &[Budget]);
        assert_eq!(
            ExecutionPattern::Explore.supported_gates(),
            &[Budget, Permission]
        );
        assert_eq!(
            ExecutionPattern::Execute.supported_gates(),
            &[Budget, Permission, Risk, Approval]
        );
        assert_eq!(
            ExecutionPattern::Deliberate.supported_gates(),
            &[Budget, Risk]
        );
        assert_eq!(
            ExecutionPattern::Collaborate.supported_gates(),
            &[Budget, Permission, Risk, Approval]
        );
        assert_eq!(
            ExecutionPattern::Supervise.supported_gates(),
            &[Budget, Permission, Risk, Approval]
        );
    }

    #[test]
    fn policy_gate_requirements_follow_typed_task_signals() {
        use ExecutionPolicyGate::{Approval, Budget, Permission, Risk};

        assert!(Budget.is_required_for(TaskRisk::Low, false));
        assert!(Permission.is_required_for(TaskRisk::Medium, true));
        assert!(!Permission.is_required_for(TaskRisk::Medium, false));
        assert!(Risk.is_required_for(TaskRisk::High, false));
        assert!(!Risk.is_required_for(TaskRisk::Medium, true));
        assert!(Approval.is_required_for(TaskRisk::Critical, false));
        assert!(!Approval.is_required_for(TaskRisk::High, true));
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
