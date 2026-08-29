//! Public Gateway transport catalog shared by all surfaces.
//!
//! This module owns transport identity only. Domain request/response types stay
//! in their domain crates and are mapped by Gateway adapters.

mod catalog;
mod route;
mod schema;

pub use catalog::{
    find_path, find_route, gateway_paths, gateway_routes, paths, routes, GATEWAY_PATHS,
    GATEWAY_ROUTES,
};
pub use route::{GatewayHttpMethod, GatewayPathKey, GatewayRouteSpec, RouteRenderError};
pub use schema::{
    gateway_route_catalog_digest, ExceptionalGatewaySchemaFamily, GatewaySchemaIdentity,
    EXCEPTIONAL_GATEWAY_SCHEMAS,
};

pub const API_PREFIX: &str = "/api/";
