use serde::Serialize;

use crate::api_routes::AppState;
use crate::gateway_static::{resolve_static_webui_source, StaticWebUiSource};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayProcessSnapshot {
    pub(crate) pid: Option<u32>,
    pub(crate) address: Option<String>,
    pub(crate) pid_file: String,
    pub(crate) addr_file: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayRuntimeSnapshot {
    pub(crate) unified_store: bool,
    pub(crate) memory_manager: bool,
    pub(crate) platform_runtime: bool,
    pub(crate) event_bus: bool,
    pub(crate) session_kernel: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayHealthSnapshot {
    pub(crate) status: String,
    pub(crate) gateway: &'static str,
    pub(crate) api_router: &'static str,
    pub(crate) process: GatewayProcessSnapshot,
    pub(crate) static_webui: StaticWebUiSource,
    pub(crate) runtime: GatewayRuntimeSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayReadinessSnapshot {
    pub(crate) ready: bool,
    pub(crate) status: String,
    pub(crate) required: Vec<String>,
    pub(crate) degraded: Vec<String>,
    pub(crate) health: GatewayHealthSnapshot,
}

pub(crate) fn gateway_health_snapshot(state: &AppState) -> GatewayHealthSnapshot {
    let server_status = crate::server::get_server_status().ok().flatten();
    let static_webui = resolve_static_webui_source();
    let runtime = GatewayRuntimeSnapshot {
        unified_store: state.has_unified_store(),
        memory_manager: state.memory_manager.is_some(),
        platform_runtime: state.platform_runtime.is_some(),
        event_bus: true,
        session_kernel: true,
    };
    let status = if static_webui.available && runtime.session_kernel && runtime.event_bus {
        "healthy"
    } else {
        "degraded"
    };

    GatewayHealthSnapshot {
        status: status.to_string(),
        gateway: "daemon-http-gateway",
        api_router: "embedded-router",
        process: GatewayProcessSnapshot {
            pid: server_status.as_ref().map(|info| info.pid),
            address: server_status.map(|info| info.address),
            pid_file: crate::server::pid_file().display().to_string(),
            addr_file: crate::server::addr_file().display().to_string(),
        },
        static_webui,
        runtime,
    }
}

pub(crate) fn gateway_readiness_snapshot(state: &AppState) -> GatewayReadinessSnapshot {
    let health = gateway_health_snapshot(state);
    let mut degraded = Vec::new();
    if !health.static_webui.available {
        degraded.push("static_webui.index_missing".to_string());
    }
    if !health.runtime.session_kernel {
        degraded.push("runtime.session_kernel_unavailable".to_string());
    }
    if !health.runtime.event_bus {
        degraded.push("runtime.event_bus_unavailable".to_string());
    }

    let ready = degraded.is_empty();
    GatewayReadinessSnapshot {
        ready,
        status: if ready { "ready" } else { "degraded" }.to_string(),
        required: vec![
            "daemon-http-gateway".to_string(),
            "api-router".to_string(),
            "static-webui-index".to_string(),
            "session-kernel".to_string(),
            "event-bus".to_string(),
        ],
        degraded,
        health,
    }
}
