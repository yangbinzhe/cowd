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

/// Whether an executor kind may park a node in `WaitingExternal`.
#[must_use]
pub fn executor_may_wait_external(executor_kind: &str) -> bool {
    WAITING_EXTERNAL_EXECUTOR_KINDS.contains(&executor_kind)
}
