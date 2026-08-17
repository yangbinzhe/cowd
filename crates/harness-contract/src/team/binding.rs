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
