use serde::Serialize;
use storage::{
    SqlitePragmaConfig, StorageBackendKind, StorageHealth, StorageLockDiagnostics, StorageRegistry,
};

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
    pub(crate) session_repository: bool,
    pub(crate) session_projection: crate::event_bus::SessionProjectionHubMetrics,
    pub(crate) session_workers: Option<crate::session_runtime_bridge::SessionWorkerHealth>,
    pub(crate) session_working_set:
        Option<crate::services::session_service::activation::SessionWorkingSetProjection>,
    pub(crate) session_ingress: Option<session::SessionRuntimeOutboxHealth>,
    pub(crate) provider_transport: Option<runtime::ProviderTransportPoolStats>,
    pub(crate) hot_state: Option<runtime::execution_core::HotStateHealth>,
    pub(crate) outcome_projection: Option<runtime::OutcomeProjectionHealth>,
    pub(crate) evolution_projection: Option<runtime::EvolutionProjectorHealth>,
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
    pub(crate) session_execution: Option<session::StorageExecutionPlaneStats>,
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

pub(crate) async fn gateway_health_snapshot(state: &AppState) -> GatewayHealthSnapshot {
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
        session_repository: state.has_unified_store(),
        session_projection: state.services.session.event_bus().metrics(),
        session_workers: state.services.session.worker_health().ok(),
        session_working_set: state.services.session.working_set_projection().await.ok(),
        session_ingress: state.services.session.runtime_outbox_health().await.ok(),
        provider_transport: state
            .services
            .runtime
            .as_ref()
            .map(|service| service.runtime_services().provider_transport_pool().stats()),
        hot_state: state
            .services
            .runtime
            .as_ref()
            .map(|service| service.runtime_services().hot_state_health()),
        outcome_projection: state
            .services
            .runtime
            .as_ref()
            .and_then(|service| service.runtime_services().outcome_projection_health().ok()),
        evolution_projection: state
            .services
            .runtime
            .as_ref()
            .and_then(|service| service.runtime_services().evolution_projector_health().ok()),
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
    let workers_healthy = runtime
        .session_workers
        .as_ref()
        .is_some_and(session_workers_healthy);
    let ingress_healthy = runtime
        .session_ingress
        .as_ref()
        .is_some_and(|health| health.blocked == 0);
    let projections_healthy = runtime
        .outcome_projection
        .as_ref()
        .is_some_and(outcome_projection_healthy)
        && runtime
            .evolution_projection
            .as_ref()
            .is_some_and(evolution_projection_healthy);
    let status = if runtime.session_repository
        && workers_healthy
        && ingress_healthy
        && projections_healthy
        && process_discovery_warning.is_none()
    {
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

pub(crate) async fn gateway_readiness_snapshot(state: &AppState) -> GatewayReadinessSnapshot {
    let health = gateway_health_snapshot(state).await;
    let mut degraded = Vec::new();
    if !health.runtime.session_repository {
        degraded.push("runtime.session_repository_unavailable".to_string());
    }
    if health
        .runtime
        .session_workers
        .as_ref()
        .is_none_or(|workers| !session_workers_healthy(workers))
    {
        degraded.push("runtime.session_workers_degraded".to_string());
    }
    if health
        .runtime
        .session_ingress
        .as_ref()
        .is_none_or(|ingress| ingress.blocked > 0)
    {
        degraded.push("runtime.session_ingress_degraded".to_string());
    }
    if health.process.discovery_warning.is_some() {
        degraded.push("gateway.process_discovery_degraded".to_string());
    }
    if health
        .runtime
        .outcome_projection
        .as_ref()
        .is_none_or(|projection| !outcome_projection_healthy(projection))
    {
        degraded.push("runtime.outcome_projector_degraded".to_string());
    }
    if health
        .runtime
        .evolution_projection
        .as_ref()
        .is_none_or(|projection| !evolution_projection_healthy(projection))
    {
        degraded.push("runtime.evolution_projector_degraded".to_string());
    }
    if !storage_endpoints_ready(&health.storage) {
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
            "session-service".to_string(),
            "session-projection".to_string(),
            "storage-registry".to_string(),
            "capacity-controller".to_string(),
            "session-worker-supervisor".to_string(),
        ],
        optional: vec!["static-webui".to_string()],
        optional_missing,
        degraded,
        health,
    }
}

fn storage_endpoints_ready(storage: &StorageGatewaySnapshot) -> bool {
    storage
        .registry
        .endpoints
        .iter()
        .all(|endpoint| storage_endpoint_ready(endpoint, storage.postgres.is_some()))
}

fn storage_endpoint_ready(
    endpoint: &storage::StorageEndpointHealth,
    postgres_executor_ready: bool,
) -> bool {
    if endpoint.backend == StorageBackendKind::Postgres {
        // PostgreSQL endpoints intentionally have no filesystem path. Their
        // connection and migration readiness is established by the selected
        // executor during composition, not by a parent-directory probe.
        postgres_executor_ready
    } else {
        endpoint.writable_parent
    }
}

fn session_workers_healthy(health: &crate::session_runtime_bridge::SessionWorkerHealth) -> bool {
    health.accepting
        && health.recovery_completed_at_ms > 0
        && health.recovery.failed == 0
        && crate::session_runtime_bridge::REQUIRED_SESSION_WORKERS
            .iter()
            .all(|name| {
                health.workers.get(*name).is_some_and(|worker| {
                    worker.state == crate::session_runtime_bridge::SessionWorkerState::Running
                        && worker.last_backend_success_at_ms.is_some()
                        && worker.consecutive_backend_failures == 0
                })
            })
        && [
            "lifecycle_reconciliation",
            "branch_activation_reconciliation",
        ]
        .iter()
        .all(|name| {
            health
                .reconciliation
                .get(*name)
                .is_some_and(|progress| progress.consecutive_failures == 0)
        })
}

fn outcome_projection_healthy(health: &runtime::OutcomeProjectionHealth) -> bool {
    health.worker_running
        && health.consecutive_failures == 0
        && (health.latest_commit_cursor == 0 || health.checkpoint_cursor > 0)
}

fn evolution_projection_healthy(health: &runtime::EvolutionProjectorHealth) -> bool {
    health.worker_running
        && health.consecutive_failures == 0
        && (health.latest_commit_cursor == 0 || health.source_cursor > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn complete_worker_health() -> crate::session_runtime_bridge::SessionWorkerHealth {
        let workers = crate::session_runtime_bridge::REQUIRED_SESSION_WORKERS
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    crate::session_runtime_bridge::SessionWorkerObservation {
                        state: crate::session_runtime_bridge::SessionWorkerState::Running,
                        restart_count: 0,
                        last_error: None,
                        next_retry_at_ms: None,
                        last_backend_success_at_ms: Some(1),
                        last_backend_error_at_ms: None,
                        last_backend_error: None,
                        consecutive_backend_failures: 0,
                        oldest_queue_age_ms: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        crate::session_runtime_bridge::SessionWorkerHealth {
            accepting: true,
            forced_aborts: 0,
            claim_lease_lost: 0,
            workers,
            recovery: Default::default(),
            recovery_completed_at_ms: 1,
            reconciliation: [
                "lifecycle_reconciliation",
                "branch_activation_reconciliation",
            ]
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    crate::session_runtime_bridge::SessionReconciliationProgress::default(),
                )
            })
            .collect(),
        }
    }

    #[test]
    fn readiness_requires_every_named_session_worker() {
        let mut health = complete_worker_health();
        assert!(session_workers_healthy(&health));

        health.workers.remove("terminal_delivery");
        assert!(!session_workers_healthy(&health));
    }

    #[test]
    fn empty_worker_map_is_never_healthy() {
        let mut health = complete_worker_health();
        health.workers.clear();
        assert!(!session_workers_healthy(&health));
    }

    #[test]
    fn worker_backend_failure_or_missing_success_degrades_readiness() {
        let mut health = complete_worker_health();
        let ingress = health.workers.get_mut("ingress").unwrap();
        ingress.consecutive_backend_failures = 1;
        ingress.last_backend_error = Some("repository unavailable".to_string());
        assert!(!session_workers_healthy(&health));

        let mut health = complete_worker_health();
        health
            .workers
            .get_mut("working_set_cleanup")
            .unwrap()
            .last_backend_success_at_ms = None;
        assert!(!session_workers_healthy(&health));
    }

    #[test]
    fn starting_worker_is_not_ready_and_reconciliation_error_degrades_health() {
        let mut health = complete_worker_health();
        health.workers.get_mut("ingress").unwrap().state =
            crate::session_runtime_bridge::SessionWorkerState::Starting;
        assert!(!session_workers_healthy(&health));

        health.workers.get_mut("ingress").unwrap().state =
            crate::session_runtime_bridge::SessionWorkerState::Running;
        health
            .reconciliation
            .get_mut("lifecycle_reconciliation")
            .unwrap()
            .consecutive_failures = 1;
        assert!(!session_workers_healthy(&health));
    }

    #[test]
    fn health_snapshot_serializes_continuous_reconciliation_progress() {
        let mut health = complete_worker_health();
        let progress = health
            .reconciliation
            .get_mut("branch_activation_reconciliation")
            .unwrap();
        progress.scan_count = 7;
        progress.pending_count = 3;
        progress.oldest_pending_age_ms = Some(2_500);
        progress.last_success_at_ms = Some(10_000);
        progress.last_error = Some("previous transient failure".to_string());

        let value = serde_json::to_value(health).unwrap();
        let progress = &value["reconciliation"]["branch_activation_reconciliation"];
        assert_eq!(progress["scan_count"], 7);
        assert_eq!(progress["pending_count"], 3);
        assert_eq!(progress["oldest_pending_age_ms"], 2_500);
        assert_eq!(progress["last_success_at_ms"], 10_000);
        assert_eq!(progress["last_error"], "previous transient failure");
    }

    #[test]
    fn postgres_endpoint_readiness_uses_the_selected_executor() {
        let postgres = storage::StorageEndpointHealth {
            id: "session".to_string(),
            domain: storage::StorageDomainId::Session,
            scope: storage::StorageScope::Global,
            backend: StorageBackendKind::Postgres,
            owner: "test".to_string(),
            present: false,
            writable_parent: false,
        };
        assert!(storage_endpoint_ready(&postgres, true));
        assert!(!storage_endpoint_ready(&postgres, false));
    }

    #[test]
    fn outcome_projector_health_exposes_stopped_or_failed_projection() {
        let mut health = runtime::OutcomeProjectionHealth {
            worker_running: true,
            checkpoint_cursor: 10,
            latest_commit_cursor: 10,
            ..Default::default()
        };
        assert!(outcome_projection_healthy(&health));
        health.consecutive_failures = 1;
        assert!(!outcome_projection_healthy(&health));
        health.consecutive_failures = 0;
        health.worker_running = false;
        assert!(!outcome_projection_healthy(&health));
    }

    #[test]
    fn evolution_projector_health_requires_a_live_progressing_worker() {
        let mut health = runtime::EvolutionProjectorHealth {
            source_cursor: 0,
            latest_commit_cursor: 10,
            lag_commits: 10,
            dead_letter_count: 0,
            worker_running: true,
            consecutive_failures: 0,
            scan_commit_limit: 128,
            scan_event_limit: 10_000,
            scan_byte_limit: 32 * 1024 * 1024,
            scan_wall_limit_ms: 50,
        };
        assert!(!evolution_projection_healthy(&health));
        health.source_cursor = 1;
        assert!(evolution_projection_healthy(&health));
        health.consecutive_failures = 2;
        assert!(!evolution_projection_healthy(&health));
    }
}
