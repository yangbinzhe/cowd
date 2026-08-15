//! Product-neutral lifecycle primitives for supervised local workers.
//!
//! Domain supervisors own policy and state. This crate owns only process,
//! credential, Unix-socket, HTTP/2 channel, log, cancellation, and cleanup
//! mechanics.
