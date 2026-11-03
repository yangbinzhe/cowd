//! Agent-first collaboration runtime.
//!
//! This module is the only owner of model-directed collaboration mutations.
//! It consumes small semantic actions, persists one Program journal and emits
//! bounded observations/projections. Physical execution remains owned by the
//! existing scheduler and ToolHost.

mod action_service;
pub mod action_invocation;
pub(crate) mod continuation;
pub(crate) mod coordination;
pub(crate) mod execution;
mod ingress;
pub(crate) mod issues;
pub(crate) mod order_contract;
mod program;
mod read_model;
pub(crate) mod review_evidence;
mod roster;
pub(crate) mod supervision;
mod topic;
pub(crate) mod topic_delivery;
mod work_market;

pub(crate) use action_service::{
    validate_receipt_index_record, AgenticTopicObservationAck, AgenticWaitDisposition,
    TopicObservationKind,
};
pub use action_service::{AgentActionService, AgentActionServiceError, OriginalCommittedMember, OriginalMemberSubmission};
pub(crate) use action_service::{OriginalCommittedConclusion, OriginalCommittedSingle, OriginalReplayedAction};
pub(crate) use action_service::process_original_member_replay;
pub(crate) use execution::start_agentic_claim_heartbeat;
pub use execution::{AgenticDispatchContext, AgenticDispatchReceipt};
pub(crate) use ingress::agentic_program_completion_gap;
pub use program::{
    AgentMemberProjection, AgenticArtifactProjection, AgenticCompletionRequestProjection,
    AgenticMembershipLifecycle, AgenticMembershipProjection, AgenticObjectiveVerdictProjection,
    AgenticProgramProjection, AgenticProgramStatus, AgenticTaskAttemptProjection,
    AgenticTaskProjection, AgenticTaskRetirement,
    AgenticTaskStatus, AgenticTeamLifecycle, AgenticTeamProjection, AgenticTopicEntryProjection,
};
pub(crate) use read_model::AgenticReadModel;
pub(crate) use work_market::task_dependency_satisfied;

pub(crate) use program::registered_attempt_generation;
