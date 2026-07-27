use std::fmt;

use harness_contract::context::EvidenceAccessRef;
use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::ExecutionParentBinding;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolId {
    Debate,
    Jps,
    ReviewFix,
    Incident,
}

impl ProtocolId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debate => "debate",
            Self::Jps => "jps",
            Self::ReviewFix => "review_fix",
            Self::Incident => "incident",
        }
    }
}

impl fmt::Display for ProtocolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProtocolRef {
    pub id: ProtocolId,
    pub version: u32,
}

impl ProtocolRef {
    #[must_use]
    pub const fn new(id: ProtocolId, version: u32) -> Self {
        Self { id, version }
    }
}

impl fmt::Display for ProtocolRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}@{}", self.id, self.version)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolAvailability {
    Available,
    Unavailable {
        available_in: String,
        reason: String,
    },
}

impl ProtocolAvailability {
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolExecutorKind {
    AgentTask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSpec {
    pub required_fields: Vec<String>,
    pub evidence_required: bool,
    pub allows_unresolved: bool,
}

/// Declares how a protocol role may acquire evidence. This is part of the
/// protocol contract, rather than an incidental consequence of graph
/// topology: a dependent role can legitimately gather new evidence, while a
/// framing or synthesis role should consume the bounded objective/upstream
/// packet it already owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleEvidenceMode {
    /// The role frames, classifies, or structures the supplied objective and
    /// must not turn itself into a workspace-wide research worker.
    ObjectiveOnly,
    /// The role may use its authorized tools to obtain missing, role-specific
    /// evidence. It still receives predecessor results when dependencies exist.
    Acquire,
    /// The role assesses or synthesizes canonical predecessor output and must
    /// explicitly report unresolved evidence rather than silently rediscover it.
    UpstreamOnly,
}

impl RoleEvidenceMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectiveOnly => "objective_only",
            Self::Acquire => "acquire",
            Self::UpstreamOnly => "upstream_only",
        }
    }
}

impl OutputSpec {
    #[must_use]
    pub fn evidence_backed(required_fields: &[&str], allows_unresolved: bool) -> Self {
        Self {
            required_fields: required_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            evidence_required: true,
            allows_unresolved,
        }
    }

    #[must_use]
    pub fn structured(required_fields: &[&str], allows_unresolved: bool) -> Self {
        Self {
            required_fields: required_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            evidence_required: false,
            allows_unresolved,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleSpec {
    pub id: String,
    pub responsibility: String,
    pub executor: ProtocolExecutorKind,
    pub min_instances: usize,
    pub max_instances: usize,
    pub output: OutputSpec,
    #[serde(default = "default_role_evidence_mode")]
    pub evidence_mode: RoleEvidenceMode,
}

const fn default_role_evidence_mode() -> RoleEvidenceMode {
    RoleEvidenceMode::Acquire
}

impl RoleSpec {
    #[must_use]
    pub fn agent(
        id: impl Into<String>,
        responsibility: impl Into<String>,
        min_instances: usize,
        max_instances: usize,
        output: OutputSpec,
    ) -> Self {
        Self {
            id: id.into(),
            responsibility: responsibility.into(),
            executor: ProtocolExecutorKind::AgentTask,
            min_instances,
            max_instances,
            output,
            evidence_mode: RoleEvidenceMode::Acquire,
        }
    }

    #[must_use]
    pub fn with_evidence_mode(mut self, evidence_mode: RoleEvidenceMode) -> Self {
        self.evidence_mode = evidence_mode;
        self
    }

    #[must_use]
    pub const fn has_variable_cardinality(&self) -> bool {
        self.min_instances != self.max_instances
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleDependencyKind {
    All,
    CrossFanout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDependencySpec {
    pub consumer_role: String,
    pub provider_role: String,
    pub kind: RoleDependencyKind,
}

impl RoleDependencySpec {
    #[must_use]
    pub fn all(consumer_role: impl Into<String>, provider_role: impl Into<String>) -> Self {
        Self {
            consumer_role: consumer_role.into(),
            provider_role: provider_role.into(),
            kind: RoleDependencyKind::All,
        }
    }

    #[must_use]
    pub fn cross_fanout(
        consumer_role: impl Into<String>,
        provider_role: impl Into<String>,
    ) -> Self {
        Self {
            consumer_role: consumer_role.into(),
            provider_role: provider_role.into(),
            kind: RoleDependencyKind::CrossFanout,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopPolicy {
    pub max_agent_attempts: u32,
    pub stop_on_verification_failure: bool,
    pub allows_unresolved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairTrigger {
    MissingEvidence,
    VerificationFailure,
    ConstraintConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairPolicy {
    pub max_revisions: u32,
    pub repair_role: Option<String>,
    pub triggers: Vec<RepairTrigger>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolSpec {
    pub id: ProtocolId,
    pub version: u32,
    pub summary: String,
    pub availability: ProtocolAvailability,
    pub roles: Vec<RoleSpec>,
    pub dependencies: Vec<RoleDependencySpec>,
    pub verify_after_roles: Vec<String>,
    pub output: OutputSpec,
    pub stop_policy: StopPolicy,
    pub repair_policy: RepairPolicy,
}

impl ProtocolSpec {
    #[must_use]
    pub const fn protocol_ref(&self) -> ProtocolRef {
        ProtocolRef::new(self.id, self.version)
    }

    #[must_use]
    pub fn role(&self, id: &str) -> Option<&RoleSpec> {
        self.roles.iter().find(|role| role.id == id)
    }
}

/// Pure compiler input. All execution identities and leases are supplied by
/// the caller; protocol compilation neither allocates runtime records nor
/// persists graph state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolCompileRequest {
    pub protocol: ProtocolRef,
    pub graph_id: String,
    pub principal_id: String,
    pub source_turn_id: String,
    pub session_id: String,
    pub mission_id: String,
    pub team_id: Option<String>,
    pub objective: String,
    #[serde(default)]
    pub parent_execution: Option<ExecutionParentBinding>,
    pub context_refs: Vec<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    pub permission_lease: String,
    pub model_lease: String,
    #[serde(default)]
    pub backend_constraint: Option<String>,
    pub budget_lease_id: String,
    pub budget_tokens: u64,
    pub budget_revision: u64,
    pub resource_scopes: Vec<String>,
    /// Number of members in the protocol's independent fan-out stage.
    pub fanout: usize,
    /// A caller may request the protocol's single, declared repair branch.
    /// No protocol may add an implicit retry loop when this is false.
    #[serde(default)]
    pub enable_repair: bool,
}

impl ProtocolCompileRequest {
    #[must_use]
    pub fn new(
        protocol: ProtocolRef,
        graph_id: impl Into<String>,
        principal_id: impl Into<String>,
        mission_id: impl Into<String>,
        session_id: impl Into<String>,
        source_turn_id: impl Into<String>,
        objective: impl Into<String>,
    ) -> Self {
        Self {
            protocol,
            graph_id: graph_id.into(),
            principal_id: principal_id.into(),
            source_turn_id: source_turn_id.into(),
            session_id: session_id.into(),
            mission_id: mission_id.into(),
            team_id: None,
            objective: objective.into(),
            parent_execution: None,
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".to_string(),
            model_lease: "default".to_string(),
            backend_constraint: None,
            budget_lease_id: "protocol-budget".to_string(),
            budget_tokens: 0,
            budget_revision: 0,
            resource_scopes: Vec::new(),
            fanout: 2,
            enable_repair: false,
        }
    }
}
