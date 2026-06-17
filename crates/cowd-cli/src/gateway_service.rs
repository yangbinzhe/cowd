use serde::Serialize;

use crate::gateway_health::GatewayHealthSnapshot;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayServiceManifest {
    pub(crate) kind: &'static str,
    pub(crate) runtime_host: &'static str,
    pub(crate) daemon: &'static str,
    pub(crate) compat_delete_by: &'static str,
    pub(crate) gateway: &'static str,
    pub(crate) api_router: &'static str,
    pub(crate) static_webui: &'static str,
    pub(crate) socket_transition: &'static str,
    pub(crate) health: GatewayHealthSnapshot,
}

pub(crate) fn webui_manifest(health: GatewayHealthSnapshot) -> GatewayServiceManifest {
    GatewayServiceManifest {
        kind: "cowd.webui.manifest",
        runtime_host: "gateway internal runtime host",
        daemon: "compat alias for runtime_host",
        compat_delete_by: "0.9.293",
        gateway: "HTTP/SSE/WebUI access layer for the runtime host",
        api_router: "gateway service route table",
        static_webui: "gateway served browser console",
        socket_transition: "temporary local socket transition for TUI; delete_by 0.9.293",
        health,
    }
}
