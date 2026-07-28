//! Scoped resources used by the execution graph runner.
//!
//! The managers in this module are deliberately instance owned. A runtime
//! host creates one set per workspace (or isolation domain) and injects it
//! into graph executors. None of the managers owns graph state or performs
//! user-worktree cleanup.

mod manager;
mod scope_lock;
mod worktree_lease;

pub use manager::{
    ExecutionAdmissionPolicy, ExecutionResourceKind, ExecutionResourceLease,
    ExecutionResourceManager, ExecutionResourceSnapshot, ExecutionServiceClass,
    ResourceAcquireError, ResourceAdmissionDecision, ResourceAdmissionObservation,
    ResourceAdmissionObservationStatus, ResourceAdmissionRequest, ResourceGrantReceipt,
    ResourceLimitAdjustment, ResourceObservation, ResourceObservationFreshness, ResourceQuota,
    ResourceResultClass, ResourceWaitReason,
};
pub use scope_lock::{
    ScopeLockError, ScopeLockLease, ScopeLockManager, ScopeLockMode, ScopeLockRequest,
    ScopedResource,
};
pub use worktree_lease::{
    ReclaimedWorktreeLease, WorktreeLease, WorktreeLeaseError, WorktreeLeaseManager,
    WorktreeLeaseRecord, WorktreeLeaseRequest, WorktreeLeaseStatus, WorktreeOwnership,
};
