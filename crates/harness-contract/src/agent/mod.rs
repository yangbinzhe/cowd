//! Declarative agent contracts for Cowd AI execution.
//!
//! This crate describes what an agent is allowed and expected to do. It does
//! not execute tools, spawn processes, or own runtime orchestration.

use crate::context::{ContextBudgetLeaseRef, EvidenceAccessRef};
use crate::core::{ExecutionPattern, TaskRisk};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod definition;

pub use crate::evaluation::{
    EvaluationContract, EvaluationMetricDirection, EvaluationMetricSource, EvaluationMetricSpec,
    EvaluationMissingValuePolicy, EvaluationMultiplicityCorrection, EvaluationPolicyFloor,
    EvaluationScenarioObservation, EvaluationScenarioSpec, EvaluationStoppingRule,
};
pub use definition::{
    AgentBindingSnapshot, AgentCapability, AgentCapabilityContract, AgentCognitivePolicy,
    AgentDataLease, AgentDefinitionId, AgentDefinitionManifest, AgentDefinitionRevision,
    AgentDefinitionRevisionRef, AgentEvaluationBinding, AgentEvaluationContract,
    AgentExecutorPolicy, AgentInstanceRef, AgentModelPolicy, AgentReleaseBinding,
    CognitiveReadScope, CognitiveWriteMode, DefaultPointer, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    RevisionSelector, ValidationError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTerminalStatus {
    Completed,
    Failed,
    Cancelled,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskIntent {
    /// Optional Runtime-catalog identity selected by a protocol or Team role.
    /// It is only compilation input; it never becomes the runtime instance
    /// identity exposed on a durable `AgentTaskPacket`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_agent_id: Option<String>,
    /// Exact Definition revision mandated by a durable Team Template or
    /// another Runtime-owned planner. When absent, Runtime may use the
    /// selected catalog entry or a bounded builtin fallback for a generic
    /// dynamic request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_ref: Option<AgentDefinitionRevisionRef>,
    /// Optional role grant ceiling supplied with `definition_ref`. Runtime
    /// intersects it with the Definition contract; an empty value never means
    /// "all capabilities".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub granted_capabilities: Vec<AgentCapability>,
    pub run_id: String,
    pub task_id: String,
    pub session_id: String,
    pub mission_id: Option<String>,
    pub team_id: Option<String>,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub objective: String,
    pub acceptance: Vec<String>,
    pub constraints: Vec<String>,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    pub permission_lease: String,
    pub model_lease: String,
    pub budget_lease: ContextBudgetLeaseRef,
    /// Runtime-issued lifecycle fence for a Managed Agent invocation.  The
    /// binding compiler carries this unchanged into the executable packet;
    /// only Runtime can use it to authorize fenced external effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_invocation: Option<crate::managed_agent::ManagedAgentInvocationFence>,
    pub idempotency_key: String,
}

/// An executable, immutable Agent command prepared by Runtime.  Planning
/// components create [`AgentTaskIntent`]; only Runtime binding compilation can
/// produce this packet for a new graph node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskPacket {
    pub run_id: String,
    pub agent_id: String,
    pub task_id: String,
    pub session_id: String,
    pub mission_id: Option<String>,
    pub team_id: Option<String>,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub objective: String,
    pub acceptance: Vec<String>,
    pub constraints: Vec<String>,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    pub permission_lease: String,
    pub model_lease: String,
    pub budget_lease: ContextBudgetLeaseRef,
    /// The exact Runtime-compiled execution Binding. Runtime refuses to
    /// execute an unbound packet; the optional wire form exists only so an
    /// older persisted graph can be upgraded by the Runtime before its first
    /// `agent.prepared` event is committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<AgentBindingSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_invocation: Option<crate::managed_agent::ManagedAgentInvocationFence>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReturnPacket {
    pub run_id: String,
    pub agent_id: String,
    pub task_id: String,
    pub session_id: String,
    pub mission_id: Option<String>,
    pub team_id: Option<String>,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub status: AgentTerminalStatus,
    pub outcome: String,
    pub acceptance: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub changes: Vec<String>,
    pub conflicts: Vec<String>,
    pub unresolved: Vec<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub model: String,
    pub provider: String,
    pub tool_calls: u64,
    pub failure: Option<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum AgentSpecError {
    #[error("missing field: {0}")]
    MissingField(String),
    #[error("invalid contract: {0}")]
    InvalidContract(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentExecutorKind {
    CowdNative,
    ExternalCli,
    McpBacked,
    ManualReview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolPermission {
    ReadOnly,
    WriteWorkspace,
    ConnectorAction,
    MatrixWrite,
    MemoryCandidateOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentPolicyRequirement {
    RequiresApproval,
    RequiresMatrixEvidence,
    RequiresVerification,
    RequiresWorktreeIsolation,
    RequiresHumanReview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentMemoryScope {
    None,
    CandidateOnly,
    Session,
    Team,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOutputContract {
    pub required_fields: Vec<String>,
    pub evidence_required: bool,
}

impl AgentOutputContract {
    #[must_use]
    pub fn reviewable() -> Self {
        Self {
            required_fields: vec![
                "summary".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
            evidence_required: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSpec {
    pub id: String,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub executor: AgentExecutorKind,
    pub model: Option<String>,
    pub tools: Vec<AgentToolPermission>,
    pub policies: Vec<AgentPolicyRequirement>,
    pub os_env: Vec<String>,
    pub context_profile: String,
    pub memory_scope: AgentMemoryScope,
    pub matrix_requirements: Vec<String>,
    pub subagents: Vec<String>,
    pub output_contract: AgentOutputContract,
}

impl AgentSpec {
    #[must_use]
    pub fn cowd_native(
        id: impl Into<String>,
        name: impl Into<String>,
        instructions: impl Into<String>,
    ) -> Self {
        let name = name.into();
        Self {
            id: id.into(),
            description: format!("{name} agent"),
            name,
            instructions: instructions.into(),
            executor: AgentExecutorKind::CowdNative,
            model: None,
            tools: vec![AgentToolPermission::ReadOnly],
            policies: vec![AgentPolicyRequirement::RequiresVerification],
            os_env: Vec::new(),
            context_profile: "main_turn".to_string(),
            memory_scope: AgentMemoryScope::CandidateOnly,
            matrix_requirements: Vec::new(),
            subagents: Vec::new(),
            output_contract: AgentOutputContract::reviewable(),
        }
    }

    #[must_use]
    pub fn reviewer() -> Self {
        Self::cowd_native(
            "agent-spec-reviewer",
            "reviewer",
            "Review implementation evidence, risks, and regressions.",
        )
        .with_policy(AgentPolicyRequirement::RequiresHumanReview)
        .with_matrix_requirement("review_evidence")
    }

    #[must_use]
    pub fn worker() -> Self {
        Self::cowd_native(
            "agent-spec-worker",
            "worker",
            "Execute a bounded task and return evidence-backed output.",
        )
    }

    #[must_use]
    pub fn for_turn(prompt: &str, mode: ExecutionPattern, risk: TaskRisk) -> Self {
        let mut spec = match mode {
            ExecutionPattern::Direct => Self::cowd_native(
                "agent-spec-direct",
                "direct",
                "Answer directly from the provided context and cite limitations.",
            ),
            ExecutionPattern::Explore => Self::cowd_native(
                "agent-spec-explore",
                "explore",
                "Acquire and compare evidence before returning a synthesis.",
            )
            .with_policy(AgentPolicyRequirement::RequiresMatrixEvidence)
            .with_matrix_requirement("runtime_ai_kernel_trace"),
            ExecutionPattern::Execute => Self::cowd_native(
                "agent-spec-execute",
                "execute",
                "Plan, execute, verify, and return evidence-backed output.",
            )
            .with_tool(AgentToolPermission::WriteWorkspace)
            .with_policy(AgentPolicyRequirement::RequiresMatrixEvidence)
            .with_matrix_requirement("runtime_ai_kernel_trace"),
            ExecutionPattern::Deliberate => Self::cowd_native(
                "agent-spec-deliberate",
                "deliberate",
                "Compare conflicting proposals and synthesize an evidence-backed decision.",
            )
            .with_policy(AgentPolicyRequirement::RequiresMatrixEvidence)
            .with_policy(AgentPolicyRequirement::RequiresHumanReview)
            .with_matrix_requirement("deliberation_evidence"),
            ExecutionPattern::Collaborate | ExecutionPattern::Supervise => Self::cowd_native(
                "agent-spec-execution_graph",
                "execution_graph",
                "Coordinate decomposed work with review and synthesis evidence.",
            )
            .with_tool(AgentToolPermission::WriteWorkspace)
            .with_policy(AgentPolicyRequirement::RequiresMatrixEvidence)
            .with_policy(AgentPolicyRequirement::RequiresHumanReview)
            .with_matrix_requirement("execution_graph_quality")
            .with_matrix_requirement("synthesis_evidence"),
        };
        spec.context_profile = mode.as_str().to_string();
        spec.instructions = format!(
            "{} Prompt summary: {}",
            spec.instructions,
            prompt.trim().chars().take(240).collect::<String>()
        );
        spec = spec
            .with_tool(AgentToolPermission::MemoryCandidateOnly)
            .with_matrix_requirement("harness_receipt")
            .with_matrix_requirement("verification_report");
        if matches!(risk, TaskRisk::High | TaskRisk::Critical) {
            spec = spec
                .with_policy(AgentPolicyRequirement::RequiresApproval)
                .with_policy(AgentPolicyRequirement::RequiresHumanReview);
        }
        if matches!(mode, ExecutionPattern::Collaborate) {
            spec = spec.with_policy(AgentPolicyRequirement::RequiresWorktreeIsolation);
        }
        spec
    }

    #[must_use]
    pub fn with_tool(mut self, permission: AgentToolPermission) -> Self {
        if !self.tools.contains(&permission) {
            self.tools.push(permission);
        }
        self
    }

    #[must_use]
    pub fn with_policy(mut self, requirement: AgentPolicyRequirement) -> Self {
        if !self.policies.contains(&requirement) {
            self.policies.push(requirement);
        }
        self
    }

    #[must_use]
    pub fn with_matrix_requirement(mut self, requirement: impl Into<String>) -> Self {
        let requirement = requirement.into();
        if !self.matrix_requirements.contains(&requirement) {
            self.matrix_requirements.push(requirement);
        }
        self
    }

    pub fn validate(&self) -> Result<(), AgentSpecError> {
        if self.id.trim().is_empty() {
            return Err(AgentSpecError::MissingField("id".to_string()));
        }
        if self.name.trim().is_empty() {
            return Err(AgentSpecError::MissingField("name".to_string()));
        }
        if self.instructions.trim().is_empty() {
            return Err(AgentSpecError::MissingField("instructions".to_string()));
        }
        if self.tools.is_empty() {
            return Err(AgentSpecError::InvalidContract(
                "agent must declare at least one tool permission".to_string(),
            ));
        }
        if self.output_contract.evidence_required
            && !self
                .output_contract
                .required_fields
                .iter()
                .any(|field| field == "evidence")
        {
            return Err(AgentSpecError::InvalidContract(
                "evidence-required contract must include evidence field".to_string(),
            ));
        }
        Ok(())
    }
}

/// Backend-neutral lifecycle state for an agent run. Execution graph node
/// state remains owned by `ExecutionGraph`; this type describes only the
/// delegated agent itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Prepared,
    Starting,
    Running,
    WaitingInput,
    WaitingApproval,
    Paused,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl AgentStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Blocked | Self::Completed | Self::Failed | Self::Cancelled
        )
    }

    #[must_use]
    pub const fn terminal_status(self) -> Option<AgentTerminalStatus> {
        match self {
            Self::Completed => Some(AgentTerminalStatus::Completed),
            Self::Failed => Some(AgentTerminalStatus::Failed),
            Self::Cancelled => Some(AgentTerminalStatus::Cancelled),
            Self::Blocked => Some(AgentTerminalStatus::Blocked),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum AgentInput {
    UserSupplement(String),
    PeerMessage {
        from_agent_id: String,
        message: String,
    },
    ControlContext(serde_json::Value),
    ApprovalResult {
        approval_id: String,
        approved: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCommand {
    SendInput,
    Interrupt,
    Pause,
    Resume,
    Cancel,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCommandRejectReason {
    NotFound,
    StaleRevision,
    Terminal,
    UnsupportedByBackend,
    InvalidInput,
    PermissionDenied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCommandRequest {
    pub command_id: String,
    pub agent_id: String,
    pub expected_revision: u64,
    pub command: AgentCommand,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<AgentInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCommandReceipt {
    pub command_id: String,
    pub agent_id: String,
    pub accepted_revision: u64,
    pub status: AgentStatus,
    pub accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<AgentCommandRejectReason>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLifecycleEvent {
    pub event_id: String,
    pub agent_id: String,
    pub revision: u64,
    pub status: AgentStatus,
    pub kind: String,
    pub message: String,
    pub created_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_worker_spec_validates() {
        AgentSpec::worker().validate().expect("valid worker spec");
    }

    #[test]
    fn execute_spec_declares_matrix_and_memory_contracts() {
        let spec = AgentSpec::for_turn("重构 runtime", ExecutionPattern::Execute, TaskRisk::Medium);

        assert!(spec.tools.contains(&AgentToolPermission::WriteWorkspace));
        assert!(spec
            .policies
            .contains(&AgentPolicyRequirement::RequiresMatrixEvidence));
        assert!(spec
            .matrix_requirements
            .contains(&"harness_receipt".to_string()));
        spec.validate().expect("derived spec should validate");
    }

    #[test]
    fn empty_instructions_are_rejected() {
        let mut spec = AgentSpec::worker();
        spec.instructions.clear();
        assert!(matches!(
            spec.validate(),
            Err(AgentSpecError::MissingField(field)) if field == "instructions"
        ));
    }
}
