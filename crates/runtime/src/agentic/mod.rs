//! Agent-first collaboration runtime.
//!
//! This module is the only owner of model-directed collaboration mutations.
//! It consumes small semantic actions, persists one Program journal and emits
//! bounded observations/projections. Physical execution remains owned by the
//! existing scheduler and ToolHost.

mod action_service;
pub(crate) mod continuation;
pub(crate) mod coordination;
mod execution;
mod ingress;
pub(crate) mod issues;
mod program;
mod read_model;
pub(crate) mod review_evidence;
mod roster;
pub(crate) mod supervision;
mod topic;
pub(crate) mod topic_delivery;
mod work_market;

pub use action_service::{AgentActionService, AgentActionServiceError};
pub(crate) use action_service::{AgenticTopicObservationAck, TopicObservationKind};
pub(crate) use execution::start_agentic_claim_heartbeat;
pub use execution::{AgenticDispatchContext, AgenticDispatchReceipt};
pub use program::{
    AgentMemberProjection, AgenticArtifactProjection, AgenticCompletionRequestProjection,
    AgenticMembershipLifecycle, AgenticMembershipProjection, AgenticObjectiveVerdictProjection,
    AgenticProgramProjection, AgenticProgramStatus, AgenticTaskProjection, AgenticTaskRetirement,
    AgenticTaskStatus, AgenticTeamLifecycle, AgenticTeamProjection, AgenticTopicEntryProjection,
};
pub(crate) use read_model::AgenticReadModel;
pub(crate) use work_market::task_dependency_satisfied;
