mod commit_service;
mod compiler;
mod events;
pub mod executors;
mod host;
mod recovery;
mod registry;
mod runner;
mod state_store;

pub mod resources;

pub use commit_service::{
    ExecutionCommitError, ExecutionCommitReceipt, ExecutionCommitService, ExecutionEffectState,
    ToolEffectState,
};
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
pub use state_store::{ExecutionGraphStateStore, ExecutionStateStoreError};

#[cfg(test)]
mod tests;
