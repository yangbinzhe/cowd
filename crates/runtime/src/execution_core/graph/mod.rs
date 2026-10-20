mod commit_service;
#[cfg(test)]
pub(crate) use commit_service::bounded_process_tests;
mod compiler;
mod events;
pub mod executors;
mod host;
mod recovery;
mod registry;
mod runner;
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

pub(crate) use commit_service::{process_closure_document, TerminalScopeReader};
pub use commit_service::{
    DurableAgentToolReceipt, ExecutionCommitError, ExecutionCommitReceipt, ExecutionCommitService,
    ExecutionEffectState, ProcessClosurePublicationAuthority, ToolEffectState,
};
pub(crate) use commit_service::{PreEffectRefusal, ToolReceiptQuery};
pub use compiler::{ExecutionCompileError, ExecutionCompileRequest, ExecutionGraphCompiler};
pub use events::{ExecutionGraphEvent, ExecutionNodeBinding};
pub use executors::{ScopedNodeBackend, ScopedNodeExecutor};
pub use host::{ExecutionGraphHost, ExecutionGraphHostReceipt};
pub use recovery::{ExecutionGraphRecovery, ExecutionRecoveryError};
pub use registry::{
    ExecutionGraphReplan, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError, NodeExecutorRegistry,
};
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
pub(crate) use runner::ExecutionGraphRunner;
pub use runner::{ExecutionRunReport, ExecutionRunnerError};
pub use state_store::{graph_id_for_lookup, ExecutionGraphStateStore, ExecutionStateStoreError};

#[cfg(test)]
mod tests;
