//! Typed Gateway transport support.
//!
//! Domain-facing client methods remain on `GatewayApiClient` during P2, while
//! all path construction is centralized here. P6 moves those methods behind
//! the domain modules without changing their public behavior.

pub(crate) mod platform;
pub(crate) mod runtime;
pub(crate) mod session;
pub(crate) mod transport;

pub(crate) use transport::{render_route, route_with_query};
