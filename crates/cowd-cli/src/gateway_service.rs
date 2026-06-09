use serde::Serialize;

use crate::gateway_health::GatewayHealthSnapshot;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayServiceManifest {
    pub(crate) kind: &'static str,
    pub(crate) daemon: &'static str,
    pub(crate) gateway: &'static str,
    pub(crate) api_router: &'static str,
    pub(crate) static_webui: &'static str,
    pub(crate) socket_control: &'static str,
    pub(crate) health: GatewayHealthSnapshot,
}

pub(crate) fn webui_manifest(health: GatewayHealthSnapshot) -> GatewayServiceManifest {
    GatewayServiceManifest {
        kind: "cowd.webui.manifest",
        daemon: "runtime owner process",
        gateway: "daemon embedded HTTP/SSE access layer",
        api_router: "gateway internal route table",
        static_webui: "gateway served browser console",
        socket_control: "local low-latency daemon control plane",
        health,
    }
}
