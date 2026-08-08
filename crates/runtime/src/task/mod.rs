//! Runtime-owned Task domain.
//!
//! Session owns durable interaction admission; Task owns business work,
//! cross-turn bindings, lifecycle and Mission assignment. Mission consumes
//! Task projections and never mutates Task membership indirectly.

pub mod aggregate;
pub mod lifecycle;
pub mod router;
pub mod runtime_port;
mod store;

pub use aggregate::*;
pub use lifecycle::*;
pub use router::*;
pub use runtime_port::*;
pub use store::*;

#[must_use]
pub(crate) fn is_organization_candidate(task: &TaskAggregate) -> bool {
    task.kind == TaskKind::Root
        && task.origin != TaskOrigin::System
        && task.mission_assignment != TaskMissionAssignment::ExplicitLocked
        && !task.status.is_terminal()
}
