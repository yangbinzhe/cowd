//! Stable, product-neutral wire contracts shared by Cowd and independent APPs.
//!
//! This crate intentionally contains data contracts and validation only. It has
//! no dependency on Cowd runtime, Gateway, storage, UI, or business APP code.

mod catalog;
mod digest;
mod error;
mod identity;
mod invocation;
mod manifest;
mod stream;
mod surface;
mod transport;

pub use catalog::*;
pub use error::*;
pub use identity::*;
pub use invocation::*;
pub use manifest::*;
pub use stream::*;
pub use surface::*;
pub use transport::*;

/// The only wire revision implemented by this crate.
pub const PROTOCOL_REVISION_V1: u16 = 1;

/// Default maximum unary request size.
pub const DEFAULT_UNARY_REQUEST_BYTES: u64 = 1_048_576;

/// Maximum encoded size of one stream frame.
pub const MAX_STREAM_FRAME_BYTES: u64 = 1_048_576;
