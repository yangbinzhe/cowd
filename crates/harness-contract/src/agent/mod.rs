//! Declarative agent contracts for Cowd AI execution.
//!
//! This crate describes what an agent is allowed and expected to do. It does
//! not execute tools, spawn processes, or own runtime orchestration.

use crate::context::{ChildExecutionBudgetReservation, EvidenceAccessRef};
use crate::core::{ExecutionPattern, TaskRisk};
use crate::execution::{ExecutionIdentity, ExecutionIdentityKind};
use crate::policy::PermissionMode;
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

/// Default hard wall-time for Runtime-compiled delegated work when the
/// calling control plane did not provide a narrower product deadline.
pub const DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS: u64 = 30 * 60 * 1_000;

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
    pub principal_id: String,
    pub source_turn_id: String,
    pub run_id: String,
    pub task_id: String,
    pub root_task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    pub session_id: String,
    pub mission_id: String,
    pub team_id: Option<String>,
    /// Typed Team-slot identity supplied by the Team compiler. It replaces
    /// semantic `team_role:` / `role_slot:` / `focus_*:` constraint strings.
    /// Direct Agents leave it empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_role_identity: Option<crate::team::TeamRoleIdentity>,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub objective: String,
    /// Runtime-compiled requirement truth. `acceptance` remains a legacy
    /// criterion carrier for durable migration only.
    #[serde(default)]
    pub required_acceptance: crate::context::RequiredAcceptance,
    /// Typed output-shape and evidence checks compiled by the Team planner.
    /// These checks are never recovered from free-form constraint strings for
    /// newly planned work.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_acceptance: Vec<crate::team::TeamAcceptanceRequirement>,
    /// Runtime-selected requirement for this exact managed Team Agent to
    /// submit one collaboration escalation after source evidence exists.
    /// The Agent never receives Program identity or revision authority.
    #[serde(default)]
    pub requires_managed_collaboration_escalation: bool,
    pub acceptance: Vec<String>,
    pub constraints: Vec<String>,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    /// Runtime-cropped filesystem/network scopes carried into the executable
    /// worker. Empty means no workspace resource authority, never "all".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_scopes: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    pub permission_ceiling: PermissionMode,
    pub model_lease: String,
    pub budget_lease: ChildExecutionBudgetReservation,
    /// Runtime-issued absolute wall-clock deadline for this attempt. Zero is
    /// never a valid production deadline; it is reserved for legacy decode so
    /// Runtime can fail the old packet closed before dispatch.
    #[serde(default)]
    pub deadline_at_ms: u64,
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
#[serde(try_from = "AgentAssignmentWire", into = "AgentAssignmentWire")]
pub struct AgentAssignment {
    pub execution_identity: ExecutionIdentity,
    pub definition_ref: AgentDefinitionRevisionRef,
    pub instance_id: String,
    pub run_id: String,
    pub role_id: String,
    pub task_id: String,
    pub root_task_id: String,
    pub session_id: String,
    pub mission_id: String,
    pub team_run_id: Option<String>,
    pub graph_id: String,
    pub node_id: String,
    pub scope_refs: Vec<String>,
    pub capability_policy: Vec<AgentCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentAssignmentWire {
    execution_identity: ExecutionIdentity,
    definition_ref: AgentDefinitionRevisionRef,
    instance_id: String,
    run_id: String,
    role_id: String,
    task_id: String,
    root_task_id: String,
    session_id: String,
    mission_id: String,
    team_run_id: Option<String>,
    graph_id: String,
    node_id: String,
    scope_refs: Vec<String>,
    capability_policy: Vec<AgentCapability>,
}

impl AgentAssignment {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.definition_ref.validate()?;
        self.execution_identity
            .validate()
            .map_err(|error| ValidationError::InvalidContract {
                message: error.to_string(),
            })?;
        if self.execution_identity.kind() != ExecutionIdentityKind::AgentNode {
            return Err(ValidationError::InvalidContract {
                message: "Agent assignment requires an agent-node execution identity".to_string(),
            });
        }
        for (field, value) in [
            ("assignment.instance_id", self.instance_id.as_str()),
            ("assignment.run_id", self.run_id.as_str()),
            ("assignment.role_id", self.role_id.as_str()),
            ("assignment.task_id", self.task_id.as_str()),
            ("assignment.root_task_id", self.root_task_id.as_str()),
            ("assignment.session_id", self.session_id.as_str()),
            ("assignment.mission_id", self.mission_id.as_str()),
            ("assignment.graph_id", self.graph_id.as_str()),
            ("assignment.node_id", self.node_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ValidationError::MissingField {
                    field: field.to_string(),
                });
            }
        }
        if self.execution_identity.task_id() != Some(self.task_id.as_str())
            || self.execution_identity.mission_id() != Some(self.mission_id.as_str())
            || self.execution_identity.session_id() != Some(self.session_id.as_str())
            || self.execution_identity.graph_id() != Some(self.graph_id.as_str())
            || self.execution_identity.agent_run_id() != Some(self.run_id.as_str())
            || self.execution_identity.node_id() != Some(self.node_id.as_str())
            || self.execution_identity.team_run_id() != self.team_run_id.as_deref()
        {
            return Err(ValidationError::InvalidContract {
                message: "Agent assignment duplicates conflicting execution lineage".to_string(),
            });
        }
        Ok(())
    }
}

impl TryFrom<AgentAssignmentWire> for AgentAssignment {
    type Error = ValidationError;

    fn try_from(value: AgentAssignmentWire) -> Result<Self, Self::Error> {
        let assignment = Self {
            execution_identity: value.execution_identity,
            definition_ref: value.definition_ref,
            instance_id: value.instance_id,
            run_id: value.run_id,
            role_id: value.role_id,
            task_id: value.task_id,
            root_task_id: value.root_task_id,
            session_id: value.session_id,
            mission_id: value.mission_id,
            team_run_id: value.team_run_id,
            graph_id: value.graph_id,
            node_id: value.node_id,
            scope_refs: value.scope_refs,
            capability_policy: value.capability_policy,
        };
        assignment.validate()?;
        Ok(assignment)
    }
}

impl From<AgentAssignment> for AgentAssignmentWire {
    fn from(value: AgentAssignment) -> Self {
        Self {
            execution_identity: value.execution_identity,
            definition_ref: value.definition_ref,
            instance_id: value.instance_id,
            run_id: value.run_id,
            role_id: value.role_id,
            task_id: value.task_id,
            root_task_id: value.root_task_id,
            session_id: value.session_id,
            mission_id: value.mission_id,
            team_run_id: value.team_run_id,
            graph_id: value.graph_id,
            node_id: value.node_id,
            scope_refs: value.scope_refs,
            capability_policy: value.capability_policy,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskPacket {
    pub assignment: AgentAssignment,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub objective: String,
    #[serde(default)]
    pub required_acceptance: crate::context::RequiredAcceptance,
    /// Typed output-shape and evidence checks inherited from the planning
    /// intent. Legacy durable packets deserialize to an empty value and are
    /// migrated fail-closed at the Runtime boundary.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_acceptance: Vec<crate::team::TeamAcceptanceRequirement>,
    #[serde(default)]
    pub requires_managed_collaboration_escalation: bool,
    /// The semantic role identity used while the Team compiler assembles the
    /// graph.  Once the Team binding is frozen, `team_role` binds this exact
    /// value to its immutable binding digest before persistence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_role_identity: Option<crate::team::TeamRoleIdentity>,
    /// Frozen Team binding fragment for an executable Team Agent. It is
    /// attached after all Team slots have been resolved and before the graph
    /// is persisted. A Team-bound packet without this proof fails closed at
    /// the Runtime boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_role: Option<crate::team::TeamRoleAssignment>,
    pub acceptance: Vec<String>,
    pub constraints: Vec<String>,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    /// Exact resource ceiling compiled from the parent graph node. The
    /// in-process worker enforces it again at every tool boundary.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_scopes: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    pub permission_ceiling: PermissionMode,
    /// Session execution-policy revision observed when Runtime compiled this
    /// packet. A worker rechecks it before the first side effect.
    #[serde(default)]
    pub policy_revision: u64,
    pub model_lease: String,
    pub budget_lease: ChildExecutionBudgetReservation,
    /// Durable absolute wall-clock deadline inherited from the owning root or
    /// Team execution. Runtime enforces it at resource admission and while an
    /// Agent provider/tool future is active.
    #[serde(default)]
    pub deadline_at_ms: u64,
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

impl AgentTaskPacket {
    #[must_use]
    pub fn team_role_assignment(&self) -> Option<&crate::team::TeamRoleAssignment> {
        self.team_role.as_ref()
    }

    /// Validate the immutable role fence for an executable Team slot.
    ///
    /// A Team id is not merely a display grouping: it means the packet is
    /// governed by a frozen Team binding.  Therefore a live executor must
    /// reject missing, substituted, or partially reconstructed role facts
    /// instead of falling back to `team_role:` strings or a graph-node name.
    pub fn validate_team_role_binding(&self) -> Result<(), &'static str> {
        match (
            self.team_id().is_some(),
            self.team_role_identity.as_ref(),
            self.team_role.as_ref(),
        ) {
            (false, None, None) => Ok(()),
            (false, _, _) => Err("non-Team Agent packet carries Team role facts"),
            (true, Some(identity), Some(assignment)) => {
                identity.validate()?;
                assignment.validate()?;
                if assignment.identity != *identity {
                    return Err("Team role assignment does not match packet identity");
                }
                if self.assignment.role_id != identity.role_id {
                    return Err("Agent assignment role differs from Team role identity");
                }
                Ok(())
            }
            (true, _, _) => Err("Team Agent packet lacks its frozen role binding"),
        }
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        self.assignment.run_id.as_str()
    }

    #[must_use]
    pub fn agent_id(&self) -> &str {
        self.assignment.instance_id.as_str()
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        self.assignment.task_id.as_str()
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        self.assignment.session_id.as_str()
    }

    #[must_use]
    pub fn mission_id(&self) -> &str {
        self.assignment.mission_id.as_str()
    }

    #[must_use]
    pub fn team_id(&self) -> Option<&str> {
        self.assignment.team_run_id.as_deref()
    }

    #[must_use]
    pub fn graph_id(&self) -> &str {
        self.assignment.graph_id.as_str()
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        self.assignment.node_id.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReturnPacket {
    pub run_id: String,
    pub agent_id: String,
    pub task_id: String,
    pub session_id: String,
    pub mission_id: String,
    pub team_id: Option<String>,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub status: AgentTerminalStatus,
    pub outcome: String,
    /// Optional wording candidate for the root presentation gate.  Delivery
    /// facts stay in the Runtime-owned DeliveryEnvelope and cannot be supplied
    /// through this model-facing packet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer_candidate: Option<crate::outcome::AnswerCandidate>,
    /// Runtime-computed observation truth. It is never copied from the task's
    /// required acceptance.
    #[serde(default)]
    pub observed_acceptance: crate::context::ObservedAcceptance,
    /// The one Runtime-owned evaluation of the frozen acceptance contract and
    /// receipt snapshot.  A return packet may carry raw observations, but no
    /// downstream consumer is allowed to derive a second verdict from them.
    /// Missing data is a legacy/untrusted carrier and must fail closed at a
    /// governed graph boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_evaluation: Option<crate::acceptance::AcceptanceEvaluation>,
    pub acceptance: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    /// Legacy external-backend change hints. These are never sufficient for a
    /// Team acceptance decision.
    pub changes: Vec<String>,
    /// Cowd-native before/after receipts. External process Team backends are
    /// rejected and this field is ignored outside the in-process trust path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_change_receipts: Vec<AgentChangeReceipt>,
    pub conflicts: Vec<String>,
    pub unresolved: Vec<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    pub model: String,
    pub provider: String,
    pub tool_calls: u64,
    #[serde(default)]
    pub duplicate_tool_calls: u64,
    #[serde(default)]
    pub max_tool_concurrency_observed: u64,
    #[serde(default)]
    pub parallel_tool_batches: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_write_attempt_paths: Vec<String>,
    /// Legacy durable carrier. New in-process executions write typed evidence
    /// into `observed_acceptance`; raw values are migration/display-only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_observed_resource_scopes: Vec<String>,
    pub failure: Option<String>,
}

/// Cowd-native Runtime-generated proof that a bounded workspace write changed
/// bytes. Team execution rejects external process backends before they can
/// submit this field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentChangeReceipt {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_sha256: Option<String>,
    pub after_sha256: String,
    pub write_sequence: u64,
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

    #[test]
    fn agent_return_requires_canonical_mission_and_keeps_additive_change_receipts() {
        let mut wire = serde_json::json!({
            "run_id": "run",
            "agent_id": "agent",
            "task_id": "task",
            "session_id": "session",
            "mission_id": null,
            "team_id": null,
            "graph_id": "graph",
            "node_id": "node",
            "attempt": 1,
            "expected_graph_revision": 0,
            "status": "completed",
            "outcome": "done",
            "acceptance": [],
            "evidence_refs": [],
            "changes": ["src/lib.rs"],
            "conflicts": [],
            "unresolved": [],
            "input_tokens": 1,
            "output_tokens": 1,
            "cached_tokens": 0,
            "model": "model",
            "provider": "provider",
            "tool_calls": 1,
            "failure": null
        });
        assert!(serde_json::from_value::<AgentReturnPacket>(wire.clone()).is_err());
        wire["mission_id"] = serde_json::json!("mission");
        let mut returned: AgentReturnPacket =
            serde_json::from_value(wire).expect("canonical Agent result remains readable");
        assert_eq!(returned.changes, vec!["src/lib.rs"]);
        assert!(returned.runtime_change_receipts.is_empty());
        assert!(returned.answer_candidate.is_none());

        returned.runtime_change_receipts.push(AgentChangeReceipt {
            path: "src/lib.rs".to_string(),
            before_sha256: Some("a".repeat(64)),
            after_sha256: "b".repeat(64),
            write_sequence: 2,
        });
        let round_trip: AgentReturnPacket = serde_json::from_value(
            serde_json::to_value(&returned).expect("encode additive receipt"),
        )
        .expect("decode additive receipt");
        assert_eq!(round_trip, returned);
    }

    #[test]
    fn assignment_deserialization_rejects_conflicting_duplicate_lineage() {
        let graph = ExecutionIdentity::for_task_graph(
            "principal",
            "workspace",
            "mission",
            "task",
            "session",
            "turn",
            "graph",
        )
        .expect("graph identity");
        let assignment = AgentAssignment {
            execution_identity: ExecutionIdentity::for_agent_node(&graph, "run", "node")
                .expect("agent identity"),
            definition_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/test")
                    .expect("definition id"),
                1,
            )
            .expect("definition ref"),
            instance_id: "agent".to_string(),
            run_id: "run".to_string(),
            role_id: "worker".to_string(),
            task_id: "task".to_string(),
            root_task_id: "task".to_string(),
            session_id: "session".to_string(),
            mission_id: "mission".to_string(),
            team_run_id: None,
            graph_id: "graph".to_string(),
            node_id: "node".to_string(),
            scope_refs: Vec::new(),
            capability_policy: Vec::new(),
        };
        let mut wire = serde_json::to_value(assignment).expect("serialize assignment");
        wire["mission_id"] = serde_json::json!("another-mission");
        assert!(serde_json::from_value::<AgentAssignment>(wire).is_err());
    }
}
