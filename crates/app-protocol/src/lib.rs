//! Stable, product-neutral wire contracts shared by Cowd and independent APPs.
//!
//! This crate intentionally contains data contracts and validation only. It has
//! no dependency on Cowd runtime, Gateway, storage, UI, or business APP code.

mod catalog;
mod error;
mod identity;
mod invocation;
mod manifest;
mod stream;
mod surface;

pub use catalog::*;
pub use error::*;
pub use identity::*;
pub use invocation::*;
pub use manifest::*;
pub use stream::*;
pub use surface::*;

/// The only wire revision implemented by this crate.
pub const PROTOCOL_REVISION_V1: u16 = 1;

/// MIME type for unary JSON messages.
pub const UNARY_CONTENT_TYPE_V1: &str = "application/vnd.cowd.app+json;version=1";

/// MIME type for newline-delimited stream frames.
pub const STREAM_CONTENT_TYPE_V1: &str = "application/vnd.cowd.app.ndjson;version=1";

/// Default maximum unary request size.
pub const DEFAULT_UNARY_REQUEST_BYTES: u64 = 1_048_576;

/// Maximum encoded size of one stream frame.
pub const MAX_STREAM_FRAME_BYTES: u64 = 1_048_576;
