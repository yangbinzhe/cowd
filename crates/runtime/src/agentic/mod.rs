//! Agent-first collaboration runtime.
//!
//! This module is the only owner of model-directed collaboration mutations.
//! It consumes small semantic actions, persists one Program journal and emits
//! bounded observations/projections. Physical execution remains owned by the
//! existing scheduler and ToolHost.

mod action_service;
mod execution;
mod ingress;
mod program;
mod read_model;
mod roster;
pub(crate) mod supervision;
mod topic;
mod work_market;

pub(crate) use action_service::AgenticTopicObservationAck;
pub use action_service::{AgentActionService, AgentActionServiceError};
pub(crate) use execution::{start_agentic_claim_heartbeat, AgenticClaimHeartbeatGuard};
pub use execution::{AgenticDispatchContext, AgenticDispatchReceipt};
pub use program::{
    AgentMemberProjection, AgenticArtifactProjection, AgenticCompletionRequestProjection,
    AgenticMembershipLifecycle, AgenticMembershipProjection, AgenticObjectiveVerdictProjection,
    AgenticProgramProjection, AgenticProgramStatus, AgenticTaskProjection, AgenticTaskRetirement,
    AgenticTaskStatus, AgenticTeamLifecycle, AgenticTeamProjection, AgenticTopicEntryProjection,
};
pub(crate) use read_model::AgenticReadModel;
pub(crate) use work_market::task_dependency_satisfied;
