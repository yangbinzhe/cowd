//! Compatibility path for the modular Runtime event store.

#[path = "event_store/mod.rs"]
mod implementation;

pub use implementation::*;
