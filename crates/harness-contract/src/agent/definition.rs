//! Durable, versioned Agent Definition contracts.
//!
//! These types model persisted Definition assets. They deliberately do not
//! replace the execution-only [`super::AgentSpec`] contract.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{AgentOutputContract, AgentTaskIntent, AgentTaskPacket};
use crate::team::AgentDisplayIdentity;

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ValidationError {
    #[error("missing required field `{field}`")]
    MissingField { field: String },
    #[error("invalid identifier in `{field}`: `{value}` ({reason})")]
    InvalidIdentifier {
        field: String,
        value: String,
        reason: String,
    },
    #[error("invalid reference in `{field}`: `{value}` ({reason})")]
    InvalidReference {
        field: String,
        value: String,
        reason: String,
    },
    #[error("duplicate value in `{field}`: `{value}`")]
    DuplicateValue { field: String, value: String },
    #[error("invalid contract: {message}")]
    InvalidContract { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionScope {
    Builtin,
    User,
    Workspace,
}

impl DefinitionScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::User => "user",
            Self::Workspace => "workspace",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "builtin" => Some(Self::Builtin),
            "user" => Some(Self::User),
            "workspace" => Some(Self::Workspace),
            _ => None,
        }
    }
}

/// A scope-qualified, durable Agent Definition identifier, for example
/// `workspace/cowd/review-implementer`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentDefinitionId(String);

impl AgentDefinitionId {
    pub fn new(scope: DefinitionScope, local_id: impl AsRef<str>) -> Result<Self, ValidationError> {
        Self::try_from(format!("{}/{}", scope.as_str(), local_id.as_ref()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn scope(&self) -> DefinitionScope {
        // Construction validates the first segment. Deserialization remains
        // defensive: a corrupted persisted value cannot crash a caller.
        DefinitionScope::parse(self.0.split('/').next().unwrap_or_default())
            .unwrap_or(DefinitionScope::Workspace)
    }
}

impl TryFrom<String> for AgentDefinitionId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_qualified_id("agent_definition_id", &value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for AgentDefinitionId {
    type Error = ValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl AsRef<str> for AgentDefinitionId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentDefinitionRevisionRef {
    pub definition_id: AgentDefinitionId,
    pub revision: u64,
}

impl AgentDefinitionRevisionRef {
    pub fn new(definition_id: AgentDefinitionId, revision: u64) -> Result<Self, ValidationError> {
        validate_revision("revision", revision)?;
        Ok(Self {
            definition_id,
            revision,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_revision("revision", self.revision)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevisionLifecycle {
    Draft,
    Validated,
    Published,
    Deprecated,
    Archived,
    Revoked,
}

impl RevisionLifecycle {
    #[must_use]
    pub const fn can_create_new_binding(self) -> bool {
        matches!(self, Self::Published)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChannel {
    Shadow,
    Canary,
    Stable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseAssignmentStatus {
    Active,
    Stopped,
    Quarantined,
    Superseded,
}

/// Immutable release provenance carried by an executable Binding selected by
/// Runtime. It makes Canary execution attributable without allowing a model,
/// planner, or surface to select a release channel itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReleaseBinding {
    pub assignment_id: String,
    pub generation: u64,
    pub channel: ReleaseChannel,
}

/// Runtime-internal provenance for an isolated paired evaluation run.
///
/// This is deliberately separate from a release channel: an evaluation may
/// execute a published candidate before it is released, but it never grants
/// Canary/Stable traffic or changes a default pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEvaluationBinding {
    pub candidate_id: String,
    pub scenario_ref: String,
}

impl AgentEvaluationBinding {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("binding.evaluation.candidate_id", &self.candidate_id)?;
        validate_reference("binding.evaluation.scenario_ref", &self.scenario_ref)
    }
}

impl AgentReleaseBinding {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("binding.release.assignment_id", &self.assignment_id)?;
        if self.generation == 0 {
            return Err(ValidationError::InvalidContract {
                message: "binding.release.generation must be greater than zero".to_string(),
            });
        }
        if self.channel == ReleaseChannel::Shadow {
            return Err(ValidationError::InvalidContract {
                message: "an executable Agent Binding cannot use the shadow release channel"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// The authority that approved an assignment or default pointer.
///
/// Builtin definitions are authorized by Cowd release attestations, while
/// user and workspace definitions require a human approval reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReleaseAuthorization {
    HumanApproval { approval_ref: String },
    ReleaseAuthorityAttestation { attestation_ref: String },
}

impl ReleaseAuthorization {
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::HumanApproval { approval_ref } => {
                validate_reference("approval_ref", approval_ref)
            }
            Self::ReleaseAuthorityAttestation { attestation_ref } => {
                validate_reference("attestation_ref", attestation_ref)
            }
        }
    }

    #[must_use]
    pub const fn is_human_approval(&self) -> bool {
        matches!(self, Self::HumanApproval { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseAssignment {
    pub scope: DefinitionScope,
    pub revision_ref: AgentDefinitionRevisionRef,
    pub channel: ReleaseChannel,
    pub status: ReleaseAssignmentStatus,
    pub authorization: ReleaseAuthorization,
    pub content_digest: String,
}

impl ReleaseAssignment {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.revision_ref.validate()?;
        if self.scope != self.revision_ref.definition_id.scope() {
            return Err(ValidationError::InvalidContract {
                message: "release assignment scope must match the definition scope".to_string(),
            });
        }
        self.authorization.validate()?;
        validate_scope_authorization(self.scope, &self.authorization)?;
        validate_digest("content_digest", &self.content_digest)
    }

    #[must_use]
    pub fn is_active_stable(&self) -> bool {
        self.channel == ReleaseChannel::Stable && self.status == ReleaseAssignmentStatus::Active
    }

    /// Eligibility predicate for `LatestApprovedStable` resolution.
    #[must_use]
    pub fn is_active_approved_stable(&self) -> bool {
        self.is_active_stable() && self.authorization.is_human_approval()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevisionSelector {
    LatestApprovedStable,
    ExactApprovedRevision { revision: u64 },
    DefaultPointer,
}

impl RevisionSelector {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if let Self::ExactApprovedRevision { revision } = self {
            validate_revision("selector.revision", *revision)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultPointer {
    pub scope: DefinitionScope,
    pub definition_id: AgentDefinitionId,
    pub selector: RevisionSelector,
    pub authorization: ReleaseAuthorization,
}

impl DefaultPointer {
    #[must_use]
    pub fn latest(
        scope: DefinitionScope,
        definition_id: AgentDefinitionId,
        authorization: ReleaseAuthorization,
    ) -> Self {
        Self {
            scope,
            definition_id,
            selector: RevisionSelector::LatestApprovedStable,
            authorization,
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.scope != self.definition_id.scope() {
            return Err(ValidationError::InvalidContract {
                message: "default pointer scope must match the definition scope".to_string(),
            });
        }
        self.selector.validate()?;
        self.authorization.validate()?;
        validate_scope_authorization(self.scope, &self.authorization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentExecutorPolicy {
    CowdNative,
    ProcessJsonl {
        command_ref: String,
    },
    McpBacked {
        server_ref: String,
        tool_prefixes: Vec<String>,
    },
    ManualReview,
}

impl AgentExecutorPolicy {
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::CowdNative | Self::ManualReview => Ok(()),
            Self::ProcessJsonl { command_ref } => {
                validate_reference("executor.command_ref", command_ref)
            }
            Self::McpBacked {
                server_ref,
                tool_prefixes,
            } => {
                validate_reference("executor.server_ref", server_ref)?;
                validate_unique_non_empty("executor.tool_prefixes", tool_prefixes)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentModelPolicy {
    pub profile: String,
    #[serde(default)]
    pub allowed_models: Vec<String>,
    pub fallback_allowed: bool,
}

impl AgentModelPolicy {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("model_policy.profile", &self.profile)?;
        validate_unique_non_empty("model_policy.allowed_models", &self.allowed_models)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CognitiveReadScope {
    Session,
    Team,
    WorkspaceKnowledge,
    Project,
    DefinitionLineage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CognitiveWriteMode {
    None,
    CandidateOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCognitivePolicy {
    pub context_profile: String,
    #[serde(default)]
    pub read_scopes: Vec<CognitiveReadScope>,
    pub write_mode: CognitiveWriteMode,
    pub team_working_state_visible: bool,
}

impl AgentCognitivePolicy {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("cognitive_policy.context_profile", &self.context_profile)?;
        let mut scopes = BTreeSet::new();
        for scope in &self.read_scopes {
            if !scopes.insert(*scope as u8) {
                return Err(ValidationError::DuplicateValue {
                    field: "cognitive_policy.read_scopes".to_string(),
                    value: format!("{scope:?}"),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCapability {
    Read,
    Search,
    Write,
    Test,
    Network,
    ConnectorAction,
    MatrixWrite,
}

impl AgentCapability {
    /// Stable capability name used by runtime projections and selectors.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Search => "search",
            Self::Write => "write",
            Self::Test => "test",
            Self::Network => "network",
            Self::ConnectorAction => "connector_action",
            Self::MatrixWrite => "matrix_write",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCapabilityContract {
    pub capability_ceiling: Vec<AgentCapability>,
    #[serde(default)]
    pub skill_refs: Vec<String>,
    #[serde(default)]
    pub approval_required_for: Vec<AgentCapability>,
}

impl AgentCapabilityContract {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.capability_ceiling.is_empty() {
            return Err(ValidationError::MissingField {
                field: "capability_contract.capability_ceiling".to_string(),
            });
        }
        validate_unique_capabilities(
            "capability_contract.capability_ceiling",
            &self.capability_ceiling,
        )?;
        validate_unique_non_empty("capability_contract.skill_refs", &self.skill_refs)?;
        validate_unique_capabilities(
            "capability_contract.approval_required_for",
            &self.approval_required_for,
        )?;
        if let Some(capability) = self
            .approval_required_for
            .iter()
            .find(|capability| !self.capability_ceiling.contains(capability))
        {
            return Err(ValidationError::InvalidContract {
                message: format!(
                    "approval-required capability {capability:?} is outside the capability ceiling"
                ),
            });
        }
        Ok(())
    }
}

/// Agent Definitions use the shared structural evaluation contract. Keeping
/// the alias preserves the domain vocabulary without creating a second metric
/// schema for Team Definitions.
pub type AgentEvaluationContract = crate::evaluation::EvaluationContract;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinitionManifest {
    pub api_version: String,
    pub definition_id: AgentDefinitionId,
    pub revision: u64,
    pub name: String,
    pub description: String,
    pub lifecycle: RevisionLifecycle,
    pub executor: AgentExecutorPolicy,
    pub model_policy: AgentModelPolicy,
    pub cognitive_policy: AgentCognitivePolicy,
    pub capability_contract: AgentCapabilityContract,
    pub output_contract: AgentOutputContract,
    pub evaluation: AgentEvaluationContract,
    pub instructions_digest: String,
}

impl AgentDefinitionManifest {
    #[must_use]
    pub fn revision_ref(&self) -> AgentDefinitionRevisionRef {
        AgentDefinitionRevisionRef {
            definition_id: self.definition_id.clone(),
            revision: self.revision,
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.api_version != "cowd.agent/v1" {
            return Err(ValidationError::InvalidContract {
                message: "agent definition api_version must be cowd.agent/v1".to_string(),
            });
        }
        validate_revision("revision", self.revision)?;
        validate_reference("name", &self.name)?;
        validate_reference("description", &self.description)?;
        self.executor.validate()?;
        self.model_policy.validate()?;
        self.cognitive_policy.validate()?;
        self.capability_contract.validate()?;
        validate_output_contract(&self.output_contract)?;
        self.evaluation.validate()?;
        validate_digest("instructions_digest", &self.instructions_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinitionRevision {
    pub revision_ref: AgentDefinitionRevisionRef,
    pub manifest: AgentDefinitionManifest,
    pub content_digest: String,
}

/// Immutable identity of one concurrent execution of an Agent Definition.
/// A Definition Revision can create many instances; an instance never doubles
/// as the durable Definition identifier or the mutable runtime record key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentInstanceRef {
    pub instance_id: String,
    pub role_slot_id: Option<String>,
}

impl AgentInstanceRef {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("instance.instance_id", &self.instance_id)?;
        if let Some(role_slot_id) = &self.role_slot_id {
            validate_reference("instance.role_slot_id", role_slot_id)?;
        }
        Ok(())
    }
}

/// Immutable cognitive-data boundary compiled for one Agent Binding. It is a
/// lease description, not a mutable Memory/Fact/Matrix owner: Runtime ports
/// enforce it again when data is recalled or written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDataLease {
    pub session_id: String,
    pub task_id: String,
    pub team_id: Option<String>,
    pub read_scopes: Vec<CognitiveReadScope>,
    pub write_mode: CognitiveWriteMode,
    pub team_working_state_visible: bool,
    #[serde(default)]
    pub fact_boundaries: Vec<String>,
    /// Exact durable Fact references granted to this Binding. A blank list
    /// never widens recall; it means the Runtime may use only an explicitly
    /// granted boundary query, if one exists.
    #[serde(default)]
    pub fact_refs: Vec<String>,
    #[serde(default)]
    pub matrix_snapshot_refs: Vec<String>,
}

impl AgentDataLease {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("data_lease.session_id", &self.session_id)?;
        validate_reference("data_lease.task_id", &self.task_id)?;
        if let Some(team_id) = &self.team_id {
            validate_reference("data_lease.team_id", team_id)?;
        }
        let mut scopes = BTreeSet::new();
        for scope in &self.read_scopes {
            if !scopes.insert(*scope as u8) {
                return Err(ValidationError::DuplicateValue {
                    field: "data_lease.read_scopes".to_string(),
                    value: format!("{scope:?}"),
                });
            }
        }
        validate_unique_non_empty("data_lease.fact_boundaries", &self.fact_boundaries)?;
        validate_unique_non_empty("data_lease.fact_refs", &self.fact_refs)?;
        validate_unique_non_empty(
            "data_lease.matrix_snapshot_refs",
            &self.matrix_snapshot_refs,
        )?;
        if self.fact_boundaries.iter().any(|boundary| {
            !matches!(
                boundary.as_str(),
                "observed" | "inferred" | "hypothetical" | "conflict"
            )
        }) {
            return Err(ValidationError::InvalidContract {
                message: "data_lease.fact_boundaries contains an unsupported Fact reality boundary"
                    .to_string(),
            });
        }
        if self
            .fact_refs
            .iter()
            .any(|reference| !reference.starts_with("fact:"))
        {
            return Err(ValidationError::InvalidReference {
                field: "data_lease.fact_refs".to_string(),
                value: self.fact_refs.join(","),
                reason: "Fact references must use the `fact:` prefix".to_string(),
            });
        }
        if self
            .matrix_snapshot_refs
            .iter()
            .any(|reference| !reference.starts_with("matrix:source_snapshot:"))
        {
            return Err(ValidationError::InvalidReference {
                field: "data_lease.matrix_snapshot_refs".to_string(),
                value: self.matrix_snapshot_refs.join(","),
                reason: "Matrix snapshot references must use the `matrix:source_snapshot:` prefix"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// A fully resolved, immutable execution Binding. It captures every Decision
/// that must remain stable for a run: the exact Definition revision and
/// content, instance identity, effective capability grants, selected runtime
/// artifacts and cognitive-data lease. New runs may compile a newer Binding;
/// active runs must never re-resolve a default pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBindingSnapshot {
    pub binding_id: String,
    pub definition_ref: AgentDefinitionRevisionRef,
    pub definition_digest: String,
    /// Normalized immutable instructions from the resolved Definition
    /// revision. The Runtime compiler verifies this content against the
    /// Definition's instruction digest before a packet is persisted.
    pub instructions: String,
    pub instance: AgentInstanceRef,
    pub executor: AgentExecutorPolicy,
    pub model_policy: AgentModelPolicy,
    pub effective_capabilities: Vec<AgentCapability>,
    #[serde(default)]
    pub skill_refs: Vec<String>,
    #[serde(default)]
    pub tool_contract_refs: Vec<String>,
    pub data_lease: AgentDataLease,
    /// Present when Runtime selected a concrete authorized release assignment
    /// (including Canary). Existing persisted Bindings remain readable, but
    /// newly compiled Bindings carry this provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<AgentReleaseBinding>,
    /// Present only on Runtime-issued isolated paired evaluation work. It is
    /// checked against the governance candidate before candidate code can be
    /// resolved, so it cannot act as a general unpublished-definition escape
    /// hatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<AgentEvaluationBinding>,
    /// Immutable human-facing display identity compiled from the frozen Team
    /// role and Agent Definition. `None` on legacy or unbound packets; the
    /// Runtime attaches it to every Team-slot Binding before the graph is
    /// persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<AgentDisplayIdentity>,
    pub binding_digest: String,
}

impl AgentBindingSnapshot {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("binding.binding_id", &self.binding_id)?;
        self.definition_ref.validate()?;
        validate_digest("binding.definition_digest", &self.definition_digest)?;
        validate_reference("binding.instructions", &self.instructions)?;
        self.instance.validate()?;
        self.executor.validate()?;
        self.model_policy.validate()?;
        if self.effective_capabilities.is_empty() {
            return Err(ValidationError::MissingField {
                field: "binding.effective_capabilities".to_string(),
            });
        }
        validate_unique_capabilities(
            "binding.effective_capabilities",
            &self.effective_capabilities,
        )?;
        validate_unique_non_empty("binding.skill_refs", &self.skill_refs)?;
        validate_unique_non_empty("binding.tool_contract_refs", &self.tool_contract_refs)?;
        self.data_lease.validate()?;
        if let Some(release) = &self.release {
            release.validate()?;
        }
        if let Some(evaluation) = &self.evaluation {
            evaluation.validate()?;
        }
        validate_digest("binding.binding_digest", &self.binding_digest)
    }

    /// Materialize the only executable task packet for a newly planned Agent
    /// node. Planning code may describe an intent, but it cannot select the
    /// runtime identity, effective tools, Skills, or data lease itself.
    pub fn compile_task_packet(
        &self,
        intent: AgentTaskIntent,
        execution_identity: crate::execution::ExecutionIdentity,
    ) -> Result<AgentTaskPacket, ValidationError> {
        self.validate()?;
        validate_reference("task.principal_id", &intent.principal_id)?;
        validate_reference("task.source_turn_id", &intent.source_turn_id)?;
        validate_reference("task.run_id", &intent.run_id)?;
        validate_reference("task.task_id", &intent.task_id)?;
        validate_reference("task.root_task_id", &intent.root_task_id)?;
        validate_reference("task.session_id", &intent.session_id)?;
        validate_reference("task.graph_id", &intent.graph_id)?;
        validate_reference("task.node_id", &intent.node_id)?;
        validate_reference("task.idempotency_key", &intent.idempotency_key)?;
        if self.data_lease.session_id != intent.session_id
            || self.data_lease.task_id != intent.task_id
            || self.data_lease.team_id != intent.team_id
        {
            return Err(ValidationError::InvalidContract {
                message: "Binding data lease does not match task identity".to_string(),
            });
        }
        if execution_identity.principal_id() != intent.principal_id
            || execution_identity.session_id() != Some(intent.session_id.as_str())
            || execution_identity.turn_id() != Some(intent.source_turn_id.as_str())
        {
            return Err(ValidationError::InvalidContract {
                message: "Execution identity principal or source turn does not match task intent"
                    .to_string(),
            });
        }
        if let Some(managed_invocation) = &intent.managed_invocation {
            managed_invocation.validate()?;
        }
        let required_acceptance = if intent.required_acceptance.is_empty() {
            crate::context::RequiredAcceptance {
                criteria: intent.acceptance.clone(),
                evidence_obligations: Vec::new(),
            }
        } else {
            if intent.required_acceptance.criteria != intent.acceptance {
                return Err(ValidationError::InvalidContract {
                    message: "typed required acceptance criteria must match the durable criterion carrier"
                        .to_string(),
                });
            }
            intent.required_acceptance.clone()
        };
        let assignment = super::AgentAssignment {
            execution_identity,
            definition_ref: self.definition_ref.clone(),
            instance_id: self.instance.instance_id.clone(),
            run_id: intent.run_id,
            role_id: self
                .instance
                .role_slot_id
                .clone()
                .unwrap_or_else(|| "agent".to_string()),
            task_id: intent.task_id,
            root_task_id: intent.root_task_id,
            session_id: intent.session_id,
            mission_id: intent.mission_id,
            team_run_id: intent.team_id,
            graph_id: intent.graph_id,
            node_id: intent.node_id,
            scope_refs: intent.resource_scopes.clone(),
            capability_policy: self.effective_capabilities.clone(),
        };
        assignment.validate()?;
        Ok(AgentTaskPacket {
            assignment,
            attempt: intent.attempt,
            expected_graph_revision: intent.expected_graph_revision,
            objective: intent.objective,
            required_acceptance,
            output_acceptance: intent.output_acceptance,
            acceptance: intent.acceptance,
            constraints: intent.constraints,
            context_refs: intent.context_refs,
            evidence_refs: intent.evidence_refs,
            resource_scopes: intent.resource_scopes,
            allowed_tools: self.tool_contract_refs.clone(),
            allowed_skills: self.skill_refs.clone(),
            permission_ceiling: intent.permission_ceiling,
            policy_revision: 0,
            model_lease: intent.model_lease,
            budget_lease: intent.budget_lease,
            deadline_at_ms: intent.deadline_at_ms,
            binding: Some(self.clone()),
            managed_invocation: intent.managed_invocation,
            idempotency_key: intent.idempotency_key,
        })
    }
}

impl AgentDefinitionRevision {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.revision_ref.validate()?;
        self.manifest.validate()?;
        if self.revision_ref != self.manifest.revision_ref() {
            return Err(ValidationError::InvalidContract {
                message: "revision reference must match manifest definition_id and revision"
                    .to_string(),
            });
        }
        validate_digest("content_digest", &self.content_digest)
    }
}

pub(crate) fn validate_qualified_id(field: &str, value: &str) -> Result<(), ValidationError> {
    let mut segments = value.split('/');
    let scope = segments.next().unwrap_or_default();
    if DefinitionScope::parse(scope).is_none() {
        return Err(ValidationError::InvalidIdentifier {
            field: field.to_string(),
            value: value.to_string(),
            reason: "first segment must be builtin, user, or workspace".to_string(),
        });
    }
    let local_segments = segments.collect::<Vec<_>>();
    if local_segments.is_empty()
        || local_segments.iter().any(|segment| {
            segment.is_empty()
                || !segment.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'-'
                        || byte == b'_'
                })
        })
    {
        return Err(ValidationError::InvalidIdentifier {
            field: field.to_string(),
            value: value.to_string(),
            reason:
                "local segments must use lowercase ascii letters, digits, hyphens, or underscores"
                    .to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_revision(field: &str, revision: u64) -> Result<(), ValidationError> {
    if revision == 0 {
        return Err(ValidationError::InvalidContract {
            message: format!("{field} must be greater than zero"),
        });
    }
    Ok(())
}

pub(crate) fn validate_reference(field: &str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        return Err(ValidationError::MissingField {
            field: field.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_digest(field: &str, value: &str) -> Result<(), ValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(ValidationError::InvalidReference {
            field: field.to_string(),
            value: value.to_string(),
            reason: "must be a lowercase SHA-256 hex digest".to_string(),
        });
    }
    Ok(())
}

fn validate_scope_authorization(
    scope: DefinitionScope,
    authorization: &ReleaseAuthorization,
) -> Result<(), ValidationError> {
    let valid = match scope {
        DefinitionScope::Builtin => matches!(
            authorization,
            ReleaseAuthorization::ReleaseAuthorityAttestation { .. }
        ),
        DefinitionScope::User | DefinitionScope::Workspace => authorization.is_human_approval(),
    };
    if !valid {
        return Err(ValidationError::InvalidContract {
            message: "builtin releases require a release authority attestation; user and workspace releases require human approval"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_unique_non_empty(field: &str, values: &[String]) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_reference(field, value)?;
        if !seen.insert(value) {
            return Err(ValidationError::DuplicateValue {
                field: field.to_string(),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

fn validate_unique_capabilities(
    field: &str,
    capabilities: &[AgentCapability],
) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for capability in capabilities {
        if !seen.insert(*capability as u8) {
            return Err(ValidationError::DuplicateValue {
                field: field.to_string(),
                value: format!("{capability:?}"),
            });
        }
    }
    Ok(())
}

fn validate_output_contract(contract: &AgentOutputContract) -> Result<(), ValidationError> {
    if contract.required_fields.is_empty() {
        return Err(ValidationError::MissingField {
            field: "output_contract.required_fields".to_string(),
        });
    }
    validate_unique_non_empty("output_contract.required_fields", &contract.required_fields)?;
    if contract.evidence_required
        && !contract
            .required_fields
            .iter()
            .any(|field| field == "evidence")
    {
        return Err(ValidationError::InvalidContract {
            message: "evidence-required output contract must include the evidence field"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn manifest() -> AgentDefinitionManifest {
        AgentDefinitionManifest {
            api_version: "cowd.agent/v1".to_string(),
            definition_id: AgentDefinitionId::try_from("workspace/cowd/reviewer").unwrap(),
            revision: 1,
            name: "Reviewer".to_string(),
            description: "Reviews implementation evidence.".to_string(),
            lifecycle: RevisionLifecycle::Published,
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "coding-balanced".to_string(),
                allowed_models: vec!["gpt-5".to_string()],
                fallback_allowed: true,
            },
            cognitive_policy: AgentCognitivePolicy {
                context_profile: "sub_agent".to_string(),
                read_scopes: vec![CognitiveReadScope::Session, CognitiveReadScope::Team],
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: true,
            },
            capability_contract: AgentCapabilityContract {
                capability_ceiling: vec![AgentCapability::Read, AgentCapability::Search],
                skill_refs: vec!["code-review@2".to_string()],
                approval_required_for: vec![],
            },
            output_contract: AgentOutputContract::reviewable(),
            evaluation: AgentEvaluationContract::single_release_gate(
                "agent-review-code",
                "evidence_required",
            ),
            instructions_digest: digest('a'),
        }
    }

    #[test]
    fn qualified_agent_definition_id_requires_scope_and_safe_segments() {
        assert_eq!(
            AgentDefinitionId::try_from("workspace/cowd/reviewer")
                .unwrap()
                .scope(),
            DefinitionScope::Workspace
        );
        assert!(AgentDefinitionId::try_from("reviewer").is_err());
        assert!(AgentDefinitionId::try_from("workspace/Reviewer").is_err());
    }

    #[test]
    fn definition_revision_requires_matching_manifest_and_digests() {
        let manifest = manifest();
        let revision = AgentDefinitionRevision {
            revision_ref: manifest.revision_ref(),
            manifest,
            content_digest: digest('b'),
        };
        revision.validate().unwrap();

        let mut mismatched = revision;
        mismatched.revision_ref.revision = 2;
        assert!(matches!(
            mismatched.validate(),
            Err(ValidationError::InvalidContract { .. })
        ));
    }

    #[test]
    fn release_and_pointer_are_orthogonal_but_validate_their_scope() {
        let id = AgentDefinitionId::try_from("user/reviewer").unwrap();
        let assignment = ReleaseAssignment {
            scope: DefinitionScope::User,
            revision_ref: AgentDefinitionRevisionRef::new(id.clone(), 2).unwrap(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/42".to_string(),
            },
            content_digest: digest('c'),
        };
        assignment.validate().unwrap();
        assert!(assignment.is_active_approved_stable());

        let pointer = DefaultPointer::latest(
            DefinitionScope::User,
            id,
            ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/43".to_string(),
            },
        );
        pointer.validate().unwrap();
        assert_eq!(pointer.selector, RevisionSelector::LatestApprovedStable);
    }

    #[test]
    fn builtin_releases_require_release_authority_attestations() {
        let id = AgentDefinitionId::try_from("builtin/direct").unwrap();
        let assignment = ReleaseAssignment {
            scope: DefinitionScope::Builtin,
            revision_ref: AgentDefinitionRevisionRef::new(id, 1).unwrap(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: "release-attestation/test-candidate".to_string(),
            },
            content_digest: digest('d'),
        };
        assignment.validate().unwrap();
        assert!(!assignment.is_active_approved_stable());
    }

    #[test]
    fn capability_approval_cannot_expand_the_ceiling() {
        let mut invalid = manifest();
        invalid.capability_contract.approval_required_for = vec![AgentCapability::Write];
        assert!(matches!(
            invalid.validate(),
            Err(ValidationError::InvalidContract { .. })
        ));
    }
}
