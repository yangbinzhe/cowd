//! External-wait authority contracts (PLAN W1 / C4).
//!
//! `ExecutionNodeStatus::WaitingExternal` is the one non-terminal status the
//! graph can only leave through a matching external resolution. It is reserved
//! for executors that own a *real*, durably registered external dependency:
//!
//! * **Class A (Barrier)** — the Agent-first Program barrier. Its authority is
//!   structural: the frozen expected-child set plus same-generation durable
//!   terminal receipts (see PLAN C5-6). It does not need a registration record.
//! * **Class B (Authoritative)** — an approval / session-handoff / connector
//!   dependency. Its authority is a durable registration minted by a trusted
//!   subsystem, carrying a correlation id and an optional expiry.
//!
//! This module holds only the wire/contract types and pure predicates; the
//! durable registry and the trusted minting capability live in the runtime.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::{ExecutionGraph, ExecutionNodeSpec};

/// Executor kinds permitted to park a node in [`ExecutionNodeStatus::WaitingExternal`].
///
/// `WaitingExternal` can only be left by a matching external resolution, so it
/// is reserved for executors that own a real external dependency. See PLAN C4.
pub const WAITING_EXTERNAL_EXECUTOR_KINDS: &[&str] =
    &["agentic_program_wait", "approval", "session_dispatch"];

/// How a `WaitingExternal` node's authority is established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalWaitClass {
    /// Structural barrier: resolved by the frozen expected-child predicate.
    Barrier,
    /// Registered authoritative dependency, resolved by a durable subject.
    Authoritative,
}

/// Trusted subsystem that may register a class-B external dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalWaitIssuer {
    /// Reserved for the class-A barrier (structural, no registration needed).
    ProgramBarrier,
    ApprovalQueue,
    SessionHandoff,
    EdgeConnector,
    HumanInput,
}

/// A durable registration for a class-B external wait.
///
/// `registration_id` and `correlation_id` are stable across retries so replay
/// is idempotent; `nonce` is unpredictable and is carried for audit only (it is
/// not an authorization input — see the design's §1.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWaitRegistration {
    pub registration_id: String,
    pub graph_id: String,
    pub node_id: String,
    pub class: ExternalWaitClass,
    pub issuer: ExternalWaitIssuer,
    /// The durable subject the wait is anchored to (approval_id / handoff id /
    /// program id). Resolution must be derived from this subject.
    pub subject_ref: String,
    pub correlation_id: String,
    pub nonce: String,
    pub expires_at_ms: Option<u64>,
    pub generation: u64,
}

/// A class-A barrier's frozen expected children (PLAN C5-6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierExpectedChildren {
    pub generation: u64,
    pub child_ids: BTreeSet<String>,
}

/// Why an external wait is not admissible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalWaitError {
    MissingRegistration { graph_id: String, node_id: String },
    CorrelationMismatch { node_id: String },
    Expired { node_id: String, expires_at_ms: u64 },
    ClassMismatch { node_id: String },
    SubjectGone { subject_ref: String },
    UnauthorizedExecutor { executor_kind: String },
}

impl std::fmt::Display for ExternalWaitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRegistration { graph_id, node_id } => write!(
                formatter,
                "node `{node_id}` in graph `{graph_id}` has no external-wait registration"
            ),
            Self::CorrelationMismatch { node_id } => {
                write!(formatter, "external-wait correlation mismatch for `{node_id}`")
            }
            Self::Expired {
                node_id,
                expires_at_ms,
            } => write!(
                formatter,
                "external-wait registration for `{node_id}` expired at {expires_at_ms}"
            ),
            Self::ClassMismatch { node_id } => {
                write!(formatter, "external-wait class mismatch for `{node_id}`")
            }
            Self::SubjectGone { subject_ref } => {
                write!(formatter, "external-wait subject `{subject_ref}` no longer exists")
            }
            Self::UnauthorizedExecutor { executor_kind } => write!(
                formatter,
                "executor `{executor_kind}` may not own an external wait"
            ),
        }
    }
}

impl std::error::Error for ExternalWaitError {}

/// Whether an executor kind may park a node in `WaitingExternal`.
#[must_use]
pub fn executor_may_wait_external(executor_kind: &str) -> bool {
    WAITING_EXTERNAL_EXECUTOR_KINDS.contains(&executor_kind)
}

/// Class-A structural predicate: the node must be framework-owned (its
/// executor kind is authorized and its payload is runtime-authored). This is a
/// necessary condition; the frozen-child predicate is checked at release time.
#[must_use]
pub fn barrier_node_is_framework_owned(node: &ExecutionNodeSpec) -> bool {
    executor_may_wait_external(&node.executor_kind)
}

/// Whether a registration is currently admissible for `node_id`.
pub fn external_wait_admissible(
    registration: &ExternalWaitRegistration,
    graph: &ExecutionGraph,
    node_id: &str,
    correlation_id: &str,
    now_ms: u64,
) -> Result<(), ExternalWaitError> {
    if registration.graph_id != graph.id || registration.node_id != node_id {
        return Err(ExternalWaitError::MissingRegistration {
            graph_id: graph.id.clone(),
            node_id: node_id.to_string(),
        });
    }
    if registration.correlation_id != correlation_id {
        return Err(ExternalWaitError::CorrelationMismatch {
            node_id: node_id.to_string(),
        });
    }
    if registration
        .expires_at_ms
        .is_some_and(|expires_at| now_ms >= expires_at)
    {
        return Err(ExternalWaitError::Expired {
            node_id: node_id.to_string(),
            expires_at_ms: registration.expires_at_ms.unwrap_or_default(),
        });
    }
    Ok(())
}
