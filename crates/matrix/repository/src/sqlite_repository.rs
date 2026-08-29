//! Compatibility path for the modular SQLite Matrix repository.

#[path = "sqlite/mod.rs"]
mod implementation;

pub use implementation::*;
