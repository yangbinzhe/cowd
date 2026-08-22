//! Frozen execution-time Team/Agent identity bindings.
//!
//! These snapshots are compiled once before graph registration and persisted
//! transactionally with the graph. Execution, recovery, projection and Surface
//! only read the Binding; they never re-read a mutable latest definition.

use serde::{Deserialize, Serialize};

use super::definition::{RoleCardinalityPolicy, RolePartitionPolicy};

/// Typed role behavior facet. Behavior dispatch is driven by these tagged
/// facets, never by a raw role-name string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoleBehaviorFacet {
    Reducer { mode: String },
    Verification { mode: String },
    ReacquireEvidence { required: bool },
    TerminalCandidate { required: bool },
    UpstreamConsumption { required: bool },
}

impl RoleBehaviorFacet {
    /// Stable semantic key used when validating a published role contract.
    ///
    /// A role may declare several distinct facets, but declaring the same
    /// facet twice gives the runtime two conflicting sources for one behavior
    /// decision.  The key intentionally ignores presentation text and role
    /// identifiers: behavior is an immutable, typed part of the Template
    /// revision itself.
    #[must_use]
    pub fn kind_key(&self) -> &'static str {
        match self {
            Self::Reducer { .. } => "reducer",
            Self::Verification { .. } => "verification",
            Self::ReacquireEvidence { .. } => "reacquire_evidence",
            Self::TerminalCandidate { .. } => "terminal_candidate",
            Self::UpstreamConsumption { .. } => "upstream_consumption",
        }
    }

    /// Validate the facet's local contract.  The execution graph owns
    /// topology validation; this only rejects an empty semantic mode before a
    /// Template revision can be published.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Reducer { mode } | Self::Verification { mode } if mode.trim().is_empty() => {
                Err("behavior facet mode must not be empty")
            }
            _ => Ok(()),
        }
    }
}

/// Immutable semantic role binding captured before graph registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleBindingSnapshot {
    pub role_id: String,
    pub slot: u32,
    pub focus: Option<String>,
    pub role_name: String,
    pub role_description: String,
    pub behavior: Vec<RoleBehaviorFacet>,
    pub agent_definition_ref: String,
    pub agent_name: String,
    pub agent_description: String,
    pub agent_definition_digest: String,
    pub responsibility: String,
    pub cardinality: RoleCardinalityPolicy,
    pub partition: RolePartitionPolicy,
    pub task_contract_ref: String,
    pub acceptance: Vec<String>,
    pub team_markdown_fragment: Option<String>,
}

/// Typed semantic identity compiled for a concrete Team slot before an
/// `AgentTaskPacket` is persisted.  It deliberately contains no display text:
/// names/locales are presentation data, while this identity participates in
/// execution, evidence and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleIdentity {
    pub role_id: String,
    /// One-based slot number within the role's immutable cardinality.
    pub slot: u32,
    pub focus_id: String,
    pub focus_boundary: String,
    pub evidence_responsibility: String,
    pub focus_scope_hash: String,
    pub overlap_budget_bp: u16,
    pub novelty_target_bp: u16,
    #[serde(default)]
    pub output_acceptance: Vec<String>,
}

impl TeamRoleIdentity {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.role_id.trim().is_empty()
            || self.slot == 0
            || self.focus_id.trim().is_empty()
            || self.focus_boundary.trim().is_empty()
            || self.evidence_responsibility.trim().is_empty()
            || self.focus_scope_hash.trim().is_empty()
        {
            return Err("team role identity is incomplete");
        }
        if self.overlap_budget_bp > 10_000 || self.novelty_target_bp > 10_000 {
            return Err("team role identity percentage is outside basis-point range");
        }
        Ok(())
    }
}

/// The exact frozen Team binding fragment an executable Agent slot consumes.
///
/// `identity` is plan semantics and `behavior` is the only behavior dispatch
/// carrier.  The binding id/digest fence this fragment to the Team snapshot
/// that was persisted with the graph, so an active Agent can never infer its
/// role from constraints, a node id, or a mutable template default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleAssignment {
    pub team_binding_id: String,
    pub team_binding_digest: String,
    pub identity: TeamRoleIdentity,
    #[serde(default)]
    pub behavior: Vec<RoleBehaviorFacet>,
}

impl TeamRoleAssignment {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.team_binding_id.trim().is_empty() || self.team_binding_digest.trim().is_empty() {
            return Err("team role assignment has no frozen Team binding reference");
        }
        self.identity.validate()
    }
}

/// Immutable human-facing team display identity. Machine ids never serve as
/// the normal UI title.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamDisplayIdentity {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_display_name: Option<String>,
    pub role_label: String,
    pub focus_label: Option<String>,
    pub locale: String,
    pub provenance: String,
    pub digest: String,
}

/// Immutable human-facing agent display identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDisplayIdentity {
    /// Stable machine identity of the agent instance this display describes.
    /// Surfaces use it to join display identity to agent activities and
    /// graph nodes; it is never a display title.
    #[serde(default)]
    pub agent_id: String,
    /// Typed role id (researcher/synthesizer/...). Display-only join key.
    #[serde(default)]
    pub role_id: String,
    /// Human-facing role description (e.g. "供应链专家"), when declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_display_name: Option<String>,
    pub label: String,
    pub role_label: String,
    pub focus_label: Option<String>,
    pub locale: String,
    pub provenance: String,
    pub digest: String,
}

/// Team binding frozen once and persisted transactionally with the graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamBindingSnapshot {
    pub binding_id: String,
    pub template_ref: String,
    pub template_digest: String,
    pub template_name: String,
    pub template_description: String,
    pub team_instructions: String,
    pub roles: Vec<TeamRoleBindingSnapshot>,
    pub strategy_decision_id: String,
    pub strategy_decision_revision: u64,
    pub strategy_decision_lease: String,
    pub strategy_turn_ref: String,
    pub display_identity: TeamDisplayIdentity,
    pub binding_digest: String,
}

/// Team lifecycle is a separate axis from execution-node status. Non-terminal
/// states are fixed; terminal delivery is projected from `DeliveryStatus`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamLifecycleState {
    Preparing,
    Running,
    WaitingDependency,
    WaitingApproval,
    WaitingExternal,
    WaitingInput,
    Paused,
    Terminal,
}

/// Delivery status is projected only after graph execution terminal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Complete,
    Partial,
    Unavailable,
    Cancelled,
}
