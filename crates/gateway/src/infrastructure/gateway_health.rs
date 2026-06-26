use serde::Serialize;
use storage::{
    MigrationRunner, SqlitePragmaConfig, StorageHealth, StorageLockDiagnostics, StorageRegistry,
};

use crate::api_routes::AppState;
use crate::gateway_static::StaticWebUiSource;
use crate::services::growth_storage_migrations;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayProcessSnapshot {
    pub(crate) pid: Option<u32>,
    pub(crate) address: Option<String>,
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
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StorageGatewaySnapshot {
    pub(crate) registry: StorageHealth,
    pub(crate) migrations: Vec<storage::StorageMigration>,
    pub(crate) locks: Vec<StorageLockDiagnostics>,
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
    let server_status = crate::server::get_server_status().ok().flatten();
    let static_webui = state.static_webui.clone();
    let runtime = GatewayRuntimeSnapshot {
        service_layer: state.services.has_minimum_service_contract(),
        unified_store: state.has_unified_store(),
        memory_manager: state.services.memory.manager().is_some(),
        surface_runtime: state.services.surface.is_runtime_available(),
        event_bus: true,
        session_kernel: true,
    };
    let storage_registry = StorageRegistry::default_for_config_home(&state.config_home);
    let pragma = SqlitePragmaConfig::default();
    let mut migrations = MigrationRunner::from_registry(&storage_registry).status();
    migrations.extend(inspect_growth_migrations(&storage_registry));
    let storage = StorageGatewaySnapshot {
        registry: storage_registry.health(),
        migrations,
        locks: storage_registry
            .handles
            .iter()
            .filter(|handle| matches!(handle.backend, storage::StorageBackendKind::Sqlite))
            .map(|handle| StorageLockDiagnostics::for_handle(handle, pragma.busy_timeout_ms))
            .collect(),
    };
    let status = if runtime.session_kernel && runtime.event_bus {
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
            address: server_status.map(|info| info.address),
            pid_file: crate::server::pid_file().display().to_string(),
            addr_file: crate::server::addr_file().display().to_string(),
        },
        static_webui,
        runtime,
        storage,
    }
}

fn inspect_growth_migrations(registry: &StorageRegistry) -> Vec<storage::StorageMigration> {
    let Ok(handle) = registry.sqlite_handle("growth") else {
        return Vec::new();
    };
    let specs = growth_storage_migrations();
    if !handle.path.exists() {
        return specs
            .into_iter()
            .map(|spec| storage::StorageMigration {
                id: spec.id.to_string(),
                domain: spec.domain.to_string(),
                version: spec.version,
                status: "pending".to_string(),
                target: handle.path.clone(),
                description: spec.description.to_string(),
                error: None,
            })
            .collect();
    }
    match storage::SqliteConnectionFactory::default().open_handle(handle) {
        Ok(connection) => {
            match MigrationRunner::inspect_sqlite_domain(&connection, handle, &specs) {
                Ok(reports) => reports,
                Err(error) => specs
                    .into_iter()
                    .map(|spec| storage::StorageMigration {
                        id: spec.id.to_string(),
                        domain: spec.domain.to_string(),
                        version: spec.version,
                        status: "failed".to_string(),
                        target: handle.path.clone(),
                        description: spec.description.to_string(),
                        error: Some(error.to_string()),
                    })
                    .collect(),
            }
        }
        Err(error) => specs
            .into_iter()
            .map(|spec| storage::StorageMigration {
                id: spec.id.to_string(),
                domain: spec.domain.to_string(),
                version: spec.version,
                status: "failed".to_string(),
                target: handle.path.clone(),
                description: spec.description.to_string(),
                error: Some(error.to_string()),
            })
            .collect(),
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
    if !health
        .storage
        .registry
        .handles
        .iter()
        .all(|handle| handle.writable_parent)
    {
        degraded.push("storage.parent_not_writable".to_string());
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
        ],
        optional: vec!["static-webui".to_string()],
        optional_missing,
        degraded,
        health,
    }
}
