use serde::Serialize;

use crate::gateway_health::GatewayHealthSnapshot;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayServiceManifest {
    pub(crate) kind: &'static str,
    pub(crate) version: &'static str,
    pub(crate) runtime_host: &'static str,
    pub(crate) boundary_status: &'static str,
    pub(crate) gateway: &'static str,
    pub(crate) api_router: &'static str,
    pub(crate) static_webui: &'static str,
    pub(crate) control_channel: &'static str,
    /// APP ids actually registered in this Gateway process.  Browser clients
    /// use this public bootstrap fact to avoid mounting a statically bundled
    /// contribution when the server startup policy disabled it.
    pub(crate) enabled_app_ids: Vec<String>,
    pub(crate) health: GatewayHealthSnapshot,
}

pub(crate) fn webui_manifest(
    health: GatewayHealthSnapshot,
    enabled_app_ids: Vec<String>,
) -> GatewayServiceManifest {
    GatewayServiceManifest {
        kind: "cowd.webui.manifest",
        version: env!("CARGO_PKG_VERSION"),
        runtime_host: "gateway internal runtime host",
        boundary_status: "0620_final_boundary",
        gateway: "HTTP/SSE/WebUI access layer for the runtime host",
        api_router: "gateway service route table",
        static_webui: "gateway served browser console",
        control_channel: "runtime host local control channel",
        enabled_app_ids,
        health,
    }
}
