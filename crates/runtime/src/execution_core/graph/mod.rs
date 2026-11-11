mod commit_service;
pub(crate) use commit_service::{validate_node_ticket_start, validate_node_ticket_start_at_revision, bounded_tool_effect_outcome, tool_effect_outcome_requires_truncation};
#[cfg(test)]
pub(crate) use commit_service::bounded_process_tests;
mod compiler;
mod events;
pub mod executors;
mod host;
mod recovery;
mod registry;
mod runner;
pub(crate) use runner::PumpCancellationOwner;
#[cfg(feature = "test-fixtures")]
pub(crate) use runner::k05_completion_trace;
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) use runner::k05_trace_emit;
mod state_store;

pub mod resources;

/// Budget for proving that a retired binding's physical effects are really gone.
///
/// The proof itself is mandatory - the runtime never claims a closed effect it
/// has not observed. The budget only bounds how long that proof may take, and
/// it has to survive ordinary CPU contention: a 5s cap turned normal load
/// spikes into a failed turn, because a physical source that needs longer to
/// reap left the proof unfinished and the turn died with it. The budget stays
/// bounded so a genuinely stuck physical effect still fails closed. It lives
/// here so the execution runner and the test fixture that waits for the
/// retirement can share one bound.
pub(crate) const PHYSICAL_CLOSURE_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);

pub(crate) use commit_service::{validate_external_outcome_binding_from_source, execution_lineage_stream_id, process_closure_document, receipt_replay_contract, TerminalScopeReader};
pub(crate) use commit_service::{read_effect_admission, read_effect_admission_intent, EffectAdmission};
pub use commit_service::{
    CallRefusalOwner, CommittedCallRefusal, DurableAgentToolReceipt, ExecutionCommitError, ExecutionCommitReceipt, ExecutionCommitService,
    ExecutionEffectState, ProcessClosurePublicationAuthority, ToolEffectState,
};
pub(crate) use commit_service::PreEffectRefusal;
pub(crate) use commit_service::OriginalFreshEffectOwner;
#[cfg(test)]
pub(crate) use commit_service::ToolReceiptQuery;
pub use compiler::{ExecutionCompileError, ExecutionCompileRequest, ExecutionGraphCompiler};
pub use events::{ExecutionGraphEvent, ExecutionNodeBinding};
pub use executors::{ScopedNodeBackend, ScopedNodeExecutor};
pub use host::{ExecutionGraphHost, ExecutionGraphHostReceipt};
pub use recovery::{ExecutionGraphRecovery, ExecutionRecoveryError};
pub use registry::{
    ExecutionGraphReplan, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError, NodeExecutorRegistry,
};
pub(crate) use registry::{OriginalSynthesizeReplan, OriginalSynthesizeReplanHandle};
pub use resources::{
    ExecutionAdmissionPolicy, ExecutionResourceKind, ExecutionResourceLease,
    ExecutionResourceManager, ExecutionResourceSnapshot, ExecutionServiceClass,
    ResourceAcquireError, ResourceAdmissionDecision, ResourceAdmissionObservation,
    ResourceAdmissionObservationStatus, ResourceAdmissionRequest, ResourceGrantReceipt,
    ResourceLimitAdjustment, ResourceObservation, ResourceObservationFreshness, ResourceQuota,
    ResourceResultClass, ResourceWaitReason, ScopeLockError, ScopeLockLease, ScopeLockManager,
    ScopeLockMode, ScopeLockRequest, ScopedResource, WorktreeLease, WorktreeLeaseError,
    WorktreeLeaseManager, WorktreeLeaseRecord, WorktreeLeaseRequest, WorktreeLeaseStatus,
    WorktreeOwnership,
};
#[cfg(test)]
pub(crate) use runner::validate_worktree_path;
#[cfg(test)]
pub(crate) use commit_service::ExecutionTerminalCommit;
pub(crate) use runner::{ExecutionGraphRunner, OriginalPumpTask};
pub use runner::{ExecutionRunReport, ExecutionRunnerError};
pub use state_store::{graph_id_for_lookup, ExecutionGraphStateStore, ExecutionStateStoreError};

#[cfg(test)]
mod tests;
