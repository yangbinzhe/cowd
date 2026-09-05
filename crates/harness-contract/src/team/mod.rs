//! Stable contracts for descriptive Team templates and acceptance checks.
//!
//! Templates are optional model hints, not execution plans or state owners.
//! Dynamic teams and work are authored through Agent Actions and projected
//! from the Agentic Program journal.

pub mod definition;

pub use crate::evaluation::EvaluationContract as TeamEvaluationContract;
pub use definition::{
    RoleBehaviorFacet, RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract,
    TeamRoleDataflowContract, TeamRoleDefinition, TeamRoleDependency, TeamRoleTaskContract,
    TeamTemplateDefinitionId, TeamTemplateManifest, TeamTemplateRevision, TeamTemplateRevisionRef,
    TeamTopologyContract,
};
