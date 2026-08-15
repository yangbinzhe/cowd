//! Product-neutral lifecycle primitives for supervised local workers.
//!
//! Domain supervisors own policy and state. This crate owns only process,
//! credential, Unix-socket, HTTP/2 channel, log, cancellation, and cleanup
//! mechanics.

#![cfg_attr(test, allow(clippy::expect_used))]

mod cancellation;
mod channel;
mod credential;
mod error;
mod generation;
mod log_buffer;
mod process;
mod runtime_dir;

pub use cancellation::CancellationToken;
pub use channel::ManagedH2Channel;
pub use credential::{CredentialLease, CredentialSecret};
pub use error::{ManagedWorkerError, ManagedWorkerResult};
pub use generation::GenerationFence;
pub use log_buffer::LogSnapshot;
pub use process::{ManagedWorkerHandle, ManagedWorkerSpec, WorkerExit};
pub use runtime_dir::WorkerRuntimeDir;
