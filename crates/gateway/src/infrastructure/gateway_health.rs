use serde::Serialize;
use storage::{SqlitePragmaConfig, StorageHealth, StorageLockDiagnostics, StorageRegistry};

use crate::api_routes::AppState;
use crate::gateway_static::StaticWebUiSource;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayProcessSnapshot {
    pub(crate) pid: Option<u32>,
    pub(crate) address: Option<String>,
    pub(crate) discovery_warning: Option<String>,
    pub(crate) pid_file: String,
    pub(crate) addr_file: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayRuntimeSnapshot {
    pub(crate) service_layer: bool,
    pub(crate) unified_store: bool,
    pub(crate) memory_manager: bool,
    pub(crate) surface_runtime: bool,
    pub(crate) event_bus: bool,
    pub(crate) session_kernel: bool,
    pub(crate) provider_transport: Option<runtime::ProviderTransportPoolStats>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayHealthSnapshot {
    pub(crate) status: String,
    pub(crate) gateway: &'static str,
    pub(crate) api_router: &'static str,
    pub(crate) process: GatewayProcessSnapshot,
    pub(crate) static_webui: StaticWebUiSource,
    pub(crate) runtime: GatewayRuntimeSnapshot,
    pub(crate) storage: StorageGatewaySnapshot,
    pub(crate) capacity: crate::gateway_capacity::GatewayCapacitySnapshot,
    pub(crate) performance: Vec<runtime::execution_core::performance::PerformanceMetricSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StorageGatewaySnapshot {
    pub(crate) registry: StorageHealth,
    pub(crate) migrations: Vec<storage::StorageMigration>,
    pub(crate) locks: Vec<StorageLockDiagnostics>,
    pub(crate) executors: Vec<storage::SqliteExecutorHealth>,
    pub(crate) postgres: Option<storage::PostgresExecutorHealth>,
    pub(crate) session_execution: Option<memory::StorageExecutionPlaneStats>,
    pub(crate) artifacts: Option<runtime::ArtifactStoreStats>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayReadinessSnapshot {
    pub(crate) ready: bool,
    pub(crate) status: String,
    pub(crate) required: Vec<String>,
    pub(crate) optional: Vec<String>,
    pub(crate) optional_missing: Vec<String>,
    pub(crate) degraded: Vec<String>,
    pub(crate) health: GatewayHealthSnapshot,
}

pub(crate) fn gateway_health_snapshot(state: &AppState) -> GatewayHealthSnapshot {
    let (server_status, server_status_error) = match crate::server::get_server_status() {
        Ok(status) => (status, None),
        Err(error) => (None, Some(error.to_string())),
    };
    let static_webui = state.static_webui.clone();
    let runtime = GatewayRuntimeSnapshot {
        service_layer: state.services.has_minimum_service_contract(),
        unified_store: state.has_unified_store(),
        memory_manager: state.services.memory.manager().is_some(),
        surface_runtime: state.services.surface.is_runtime_available(),
        event_bus: true,
        session_kernel: true,
        provider_transport: state
            .services
            .runtime
            .as_ref()
            .map(|service| service.runtime_services().provider_transport_pool().stats()),
    };
    let mut storage_registry = state.services.selected_storage.as_ref().map_or_else(
        || {
            StorageRegistry::default_for_config_home(&state.config_home)
                .with_workspace(&state.workspace_root)
                .and_then(StorageRegistry::with_surface_messages)
                .unwrap_or_else(|error| {
                    tracing::error!(%error, "gateway health storage inventory is incomplete");
                    StorageRegistry::default_for_config_home(&state.config_home)
                })
        },
        |selected| selected.registry.clone(),
    );
    for endpoint in state.services.app_registry.storage_endpoints() {
        if let Err(error) = storage_registry.register_endpoint(endpoint) {
            tracing::error!(%error, "gateway health skipped a duplicate APP storage endpoint");
        }
    }
    let pragma = SqlitePragmaConfig::default();
    let storage = StorageGatewaySnapshot {
        registry: storage_registry.health(),
        migrations: storage::MigrationRunner::from_registry(&storage_registry).status(),
        locks: storage_registry
            .endpoints
            .iter()
            .filter(|endpoint| matches!(endpoint.backend, storage::StorageBackendKind::Sqlite))
            .map(|endpoint| {
                StorageLockDiagnostics::for_handle(&endpoint.as_handle(), pragma.busy_timeout_ms)
            })
            .collect(),
        executors: storage::StorageRuntime::global().sqlite_health(),
        postgres: state
            .services
            .selected_storage
            .as_ref()
            .and_then(|selected| selected.postgres_executor.as_ref())
            .map(storage::PostgresExecutor::health),
        session_execution: state
            .services
            .selected_storage
            .as_ref()
            .map(|selected| selected.session_store.execution_stats()),
        artifacts: state
            .services
            .artifact_store()
            .and_then(|store| store.stats().ok()),
    };
    let process_discovery_warning = server_status_error.or_else(|| {
        server_status
            .as_ref()
            .and_then(|info| info.discovery_warning.clone())
    });
    let status =
        if runtime.session_kernel && runtime.event_bus && process_discovery_warning.is_none() {
            "healthy"
        } else {
            "degraded"
        };

    GatewayHealthSnapshot {
        status: status.to_string(),
        gateway: "gateway-runtime-host",
        api_router: "gateway-api-router",
        process: GatewayProcessSnapshot {
            pid: server_status.as_ref().map(|info| info.pid),
            address: server_status.as_ref().map(|info| info.address.clone()),
            discovery_warning: process_discovery_warning,
            pid_file: crate::server::pid_file().display().to_string(),
            addr_file: crate::server::addr_file().display().to_string(),
        },
        static_webui,
        runtime,
        storage,
        capacity: state.services.capacity.snapshot(),
        performance: runtime::execution_core::performance::performance_snapshot(),
    }
}

pub(crate) fn gateway_readiness_snapshot(state: &AppState) -> GatewayReadinessSnapshot {
    let health = gateway_health_snapshot(state);
    let mut degraded = Vec::new();
    if !health.runtime.session_kernel {
        degraded.push("runtime.session_kernel_unavailable".to_string());
    }
    if !health.runtime.event_bus {
        degraded.push("runtime.event_bus_unavailable".to_string());
    }
    if health.process.discovery_warning.is_some() {
        degraded.push("gateway.process_discovery_degraded".to_string());
    }
    if !health
        .storage
        .registry
        .endpoints
        .iter()
        .all(|endpoint| endpoint.writable_parent)
    {
        degraded.push("storage.parent_not_writable".to_string());
    }
    if health.capacity.status == "overloaded" {
        degraded.push("capacity.data_lane_overloaded".to_string());
    }

    let ready = degraded.is_empty();
    let optional_missing = if health.static_webui.available {
        Vec::new()
    } else {
        vec![format!(
            "static_webui.{}",
            health.static_webui.status.as_str()
        )]
    };
    GatewayReadinessSnapshot {
        ready,
        status: if ready { "ready" } else { "degraded" }.to_string(),
        required: vec![
            "gateway-runtime-host".to_string(),
            "gateway-api-router".to_string(),
            "session-kernel".to_string(),
            "event-bus".to_string(),
            "storage-registry".to_string(),
            "capacity-controller".to_string(),
        ],
        optional: vec!["static-webui".to_string()],
        optional_missing,
        degraded,
        health,
    }
}
