//! Agent-first collaboration runtime.
//!
//! This module is the only owner of model-directed collaboration mutations.
//! It consumes small semantic actions, persists one Program journal and emits
//! bounded observations/projections. Physical execution remains owned by the
//! existing scheduler and ToolHost.

mod action_service;
mod execution;
mod program;
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
    AgenticObjectiveVerdictProjection, AgenticProgramProjection, AgenticProgramStatus,
    AgenticTaskProjection, AgenticTaskStatus, AgenticTeamProjection, AgenticTopicEntryProjection,
};
