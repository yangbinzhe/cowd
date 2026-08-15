//! Protocol-driven host services for dynamically admitted applications.
//!
//! Product applications are discovered from signed bundles at startup and
//! execute only through the managed-worker transport. This crate owns the
//! generic catalog and supervisor; it contains no product descriptor,
//! storage lease, HTTP router, or native terminal ABI.

pub mod catalog;
pub mod supervisor;
