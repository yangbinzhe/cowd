//! Manufacturing application layer for cowd.
//!
//! This crate is the application-facing MFG boundary. The current implementation
//! re-exports the runtime compatibility module while the storage and route
//! layers migrate away from kernel ownership.

mod store;

pub use runtime::mfg::*;
pub use store::MfgStore;
