//! Frozen execution-time Team/Agent identity bindings.
//!
//! These snapshots are compiled once before graph registration and persisted
//! transactionally with the graph. Execution, recovery, projection and Surface
//! only read the Binding; they never re-read a mutable latest definition.

use serde::{Deserialize, Serialize};

/// Typed behavior facets for a team role. Dispatch must not be driven by raw
/// role-name strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleBehaviorContract {
    pub reducer: String,
    pub verification: String,
    pub reacquire_evidence: String,
    pub terminal_candidate: String,
    pub upstream_consumption: String,
}

/// Immutable semantic role binding captured before graph registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleBindingSnapshot {
    pub role_id: String,
    pub slot: u32,
    pub focus: Option<String>,
    pub role_name: String,
    pub role_description: String,
    pub behavior: TeamRoleBehaviorContract,
    pub agent_definition_ref: String,
    pub agent_name: String,
    pub agent_description: String,
    pub agent_definition_digest: String,
    pub responsibility: String,
    pub cardinality: String,
    pub partition: String,
    pub task_contract_ref: String,
    pub acceptance: Vec<String>,
    pub team_markdown_fragment: Option<String>,
}

/// Immutable human-facing team display identity. Machine ids never serve as
/// the normal UI title.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamDisplayIdentity {
    pub label: String,
    pub role_label: String,
    pub focus_label: Option<String>,
    pub locale: String,
    pub provenance: String,
    pub digest: String,
}

/// Immutable human-facing agent display identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDisplayIdentity {
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

/// Team lifecycle is a separate axis from execution-node status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamLifecycleState {
    Preparing,
    Active,
    WaitingForPredecessor,
    Delivering,
    Completed,
    Failed,
    Cancelled,
}

/// Delivery status is projected only from a terminal lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    Partial,
    FailedValidation,
    Recoverable,
    Delivered,
}
