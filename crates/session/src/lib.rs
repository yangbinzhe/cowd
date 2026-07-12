#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod event_bus;
pub mod lease;
pub mod lifecycle;

pub use event_bus::{EventSender, SessionEventBus};
pub use lease::{SessionLease, SessionLeaseRegistry};
pub use lifecycle::{
    SessionActor, SessionAttachment, SessionLifecycleEvent, SessionLifecycleKernel,
    SessionLifecycleSnapshot, SessionLifecycleState,
};
