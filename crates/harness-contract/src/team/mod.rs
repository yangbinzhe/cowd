//! Stable contracts for a graph-owned multi-agent Team.
//!
//! Team definitions and instantiation intents are declarative. They never
//! carry a surface-built execution graph, mutable Agent runtime identity, or
//! scheduler state. Runtime is the only owner that turns them into work.

use serde::{Deserialize, Serialize};

use crate::context::EvidenceAccessRef;

pub mod binding;
pub mod definition;
pub mod instantiation;

pub use crate::evaluation::EvaluationContract as TeamEvaluationContract;
pub use binding::{
    AgentDisplayIdentity, DeliveryStatus, RoleBehaviorFacet, TeamBindingSnapshot,
    TeamDisplayIdentity, TeamLifecycleState, TeamRoleAssignment, TeamRoleBindingSnapshot,
    TeamRoleIdentity,
};
pub use definition::{
    RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract, TeamRoleDataflowContract,
    TeamRoleDefinition, TeamRoleDependency, TeamRoleTaskContract, TeamTemplateDefinitionId,
    TeamTemplateManifest, TeamTemplateRevision, TeamTemplateRevisionRef, TeamTopologyContract,
};
pub use instantiation::{
    focus_scope_hash, FocusPartitionPlan, FocusPartitionSlot, TeamAcceptanceCheck,
    TeamAcceptanceRequirement, TeamInstantiationRequest, TeamRoleBindingOverride,
    TeamRoleCardinalityOverride, TeamSelectionMode, TeamStrategyBinding, TeamStructuredOutputField,
    TeamTemplateSelector,
};

/// A graph-derived role trace exposed to projections. It is not an executable
/// task definition and cannot be submitted back to Runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTaskTrace {
    pub task_id: String,
    pub role_id: String,
    pub agent_id: String,
    pub run_id: String,
    pub node_id: String,
    pub status: String,
    pub result_ref: Option<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub failure: Option<String>,
}

/// The terminal Team result is a reference projection, never a second graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRunResult {
    pub team_id: String,
    pub graph_id: String,
    pub graph_revision: u64,
    pub result_ref: String,
    pub evidence_refs: Vec<EvidenceAccessRef>,
}
