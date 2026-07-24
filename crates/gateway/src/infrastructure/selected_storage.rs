//! One process-wide storage composition root.
//!
//! Domain services receive stable ports from this value. No request handler,
//! APP or Runtime turn is allowed to choose a driver or derive a database
//! path after startup.

use std::{path::Path, sync::Arc};

use fact_kernel::FactLedger;
use matrix_repository::MatrixStore;
use memory::{KnowledgeFabric, KnowledgeStore, MemoryStore, UnifiedSessionStore};
use storage::{
    PostgresConnectionConfig, PostgresExecutor, ResolvedPostgresUrl, SecretRefResolver,
    StorageDomainId, StorageEndpoint, StorageRegistry, StorageScope,
};
use surface::SurfaceMessageLedger;

use crate::task_kernel::TaskKernel;

#[derive(Clone)]
pub(crate) struct SelectedStorageTopology {
    pub(crate) backend: runtime::StorageBackendSelection,
    pub(crate) registry: StorageRegistry,
    pub(crate) postgres_executor: Option<PostgresExecutor>,
    pub(crate) app_topology: cowd_product_apps::AppStorageTopology,
    pub(crate) session_store: Arc<UnifiedSessionStore>,
    pub(crate) memory_store: Arc<dyn MemoryStore>,
    pub(crate) knowledge_store: Arc<dyn KnowledgeStore>,
    pub(crate) knowledge_fabric: KnowledgeFabric,
    pub(crate) runtime_event_store: Arc<runtime::RuntimeEventStore>,
    pub(crate) task_kernel: Arc<TaskKernel>,
    pub(crate) fact_ledger: Arc<dyn FactLedger>,
    pub(crate) matrix_store: Arc<dyn MatrixStore>,
    pub(crate) approval_ledger: approval::SharedApprovalHistoryLedger,
    pub(crate) surface_messages: Arc<dyn SurfaceMessageLedger>,
    pub(crate) connector_factory: Arc<dyn connector::ResourceDirectoryFactory>,
    pub(crate) connector_handle: storage::StorageHandle,
}

impl SelectedStorageTopology {
    pub(crate) fn compose_for_runtime(
        config: &runtime::StorageTopologyConfig,
        apps: &runtime::AppsConfig,
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        Self::compose(config, Some(apps), config_home, workspace_root)
    }

    pub(crate) fn compose_for_maintenance(
        config: &runtime::StorageTopologyConfig,
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        Self::compose(config, None, config_home, workspace_root)
    }

    fn compose(
        config: &runtime::StorageTopologyConfig,
        activation_apps: Option<&runtime::AppsConfig>,
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        let registry = base_registry(config_home, workspace_root)?;
        match config.backend {
            runtime::StorageBackendSelection::Sqlite => {
                Self::sqlite(registry, config.session_execution)
            }
            runtime::StorageBackendSelection::Postgres => {
                if let Some(apps) = activation_apps {
                    crate::storage_cutover::validate_active_manifest(
                        config_home,
                        workspace_root,
                        config,
                        apps,
                    )?;
                }
                let postgres = config.postgres.as_ref().ok_or_else(|| {
                    "storage.backend=postgres requires storage.postgres".to_string()
                })?;
                let executor = connect_postgres(postgres)?;
                let session_execution = config.session_execution;
                std::thread::spawn(move || Self::postgres(registry, executor, session_execution))
                    .join()
                    .map_err(|_| {
                        "PostgreSQL domain adapter initialization thread panicked".to_string()
                    })?
            }
        }
    }

    fn sqlite(
        registry: StorageRegistry,
        session_execution: runtime::SessionStorageExecutionConfig,
    ) -> Result<Self, String> {
        registry.ensure_directories().map_err(stringify)?;
        let session_endpoint = endpoint(&registry, &StorageDomainId::Session, None)?;
        let memory_endpoint = endpoint(&registry, &StorageDomainId::Memory, None)?;
        let knowledge_endpoint = endpoint(&registry, &StorageDomainId::Knowledge, None)?;
        let fact_endpoint = endpoint(&registry, &StorageDomainId::Fact, None)?;
        let growth_endpoint = endpoint(&registry, &StorageDomainId::Growth, None)?;
        let matrix_endpoint = endpoint(&registry, &StorageDomainId::Matrix, None)?;
        let task_endpoint = endpoint(&registry, &StorageDomainId::Tasks, None)?;
        let approval_endpoint = endpoint(&registry, &StorageDomainId::Approval, None)?;
        let legacy_approval_endpoint =
            endpoint(&registry, &StorageDomainId::ApprovalHistory, None)?;
        let surface_endpoint = endpoint(&registry, &StorageDomainId::SurfaceMessages, None)?;
        let workspace_scope = workspace_scope(&registry)?;
        let runtime_endpoint = endpoint(
            &registry,
            &StorageDomainId::RuntimeEvents,
            Some(&workspace_scope),
        )?;
        let connector_endpoint = endpoint(
            &registry,
            &StorageDomainId::ConnectorDirectory,
            Some(&workspace_scope),
        )?;

        let session_store = Arc::new(
            UnifiedSessionStore::open_sqlite_storage_handle_with_execution_config(
                &session_endpoint.as_handle(),
                memory::StorageExecutionPlaneConfig {
                    workers: session_execution.workers,
                    queue_capacity: session_execution.queue_capacity,
                },
            )
            .map_err(stringify)?,
        );
        let memory_store: Arc<dyn MemoryStore> = Arc::new(
            memory::store::sqlite::SqliteStore::open_storage_handle(&memory_endpoint.as_handle())
                .map_err(stringify)?,
        );
        let knowledge_store: Arc<dyn KnowledgeStore> = Arc::new(
            memory::SqliteKnowledgeStore::open(&knowledge_endpoint.path).map_err(stringify)?,
        );
        let knowledge_fabric = KnowledgeFabric::with_store(Arc::clone(&knowledge_store));
        let runtime_event_store = Arc::new(
            runtime::RuntimeEventStore::try_open(&runtime_endpoint.path).map_err(stringify)?,
        );
        let task_kernel = Arc::new(
            TaskKernel::open_storage_handle(&task_endpoint.as_handle()).map_err(stringify)?,
        );
        let fact_ledger: Arc<dyn FactLedger> = Arc::new(
            fact_sqlite::SqliteFactLedger::open_with_legacy_growth(
                &fact_endpoint,
                &growth_endpoint,
            )
            .map_err(stringify)?,
        );
        let matrix_store = matrix_repository::MatrixStoreHandle::new(matrix_endpoint.clone())
            .open()
            .map_err(stringify)?;
        let sqlite_approval =
            approval::SqliteApprovalHistoryLedger::open(&approval_endpoint).map_err(stringify)?;
        if sqlite_approval
            .export_migration_snapshot()
            .map_err(stringify)?
            .entries
            .is_empty()
        {
            sqlite_approval
                .import_legacy_json(legacy_approval_endpoint.path)
                .map_err(stringify)?;
        }
        let approval_ledger: approval::SharedApprovalHistoryLedger = Arc::new(sqlite_approval);
        let surface_messages: Arc<dyn SurfaceMessageLedger> = Arc::new(
            crate::surface_host::SqliteSurfaceMessageStore::from_storage_endpoint(
                &surface_endpoint,
            ),
        );
        let connector_factory: Arc<dyn connector::ResourceDirectoryFactory> =
            Arc::new(connector::SqliteResourceDirectoryFactory);

        Ok(Self {
            backend: runtime::StorageBackendSelection::Sqlite,
            registry,
            postgres_executor: None,
            app_topology: cowd_product_apps::AppStorageTopology::Sqlite,
            session_store,
            memory_store,
            knowledge_store,
            knowledge_fabric,
            runtime_event_store,
            task_kernel,
            fact_ledger,
            matrix_store,
            approval_ledger,
            surface_messages,
            connector_factory,
            connector_handle: connector_endpoint.as_handle(),
        })
    }

    fn postgres(
        mut registry: StorageRegistry,
        executor: PostgresExecutor,
        session_execution: runtime::SessionStorageExecutionConfig,
    ) -> Result<Self, String> {
        replace_business_endpoints_with_postgres(&mut registry)?;
        let workspace_scope = workspace_scope(&registry)?;
        let connector_endpoint = endpoint(
            &registry,
            &StorageDomainId::ConnectorDirectory,
            Some(&workspace_scope),
        )?;

        let session =
            session_postgres::PostgresSessionStore::new(executor.clone()).map_err(stringify)?;
        let session_store = Arc::new(
            UnifiedSessionStore::from_backend_with_execution_config(
                Arc::new(session),
                memory::StorageExecutionPlaneConfig {
                    workers: session_execution.workers,
                    queue_capacity: session_execution.queue_capacity,
                },
            )
            .map_err(stringify)?,
        );
        let memory_store: Arc<dyn MemoryStore> = Arc::new(
            memory_postgres::PostgresMemoryStore::new(executor.clone()).map_err(stringify)?,
        );
        let knowledge_store: Arc<dyn KnowledgeStore> = Arc::new(
            memory_postgres::PostgresKnowledgeStore::new(executor.clone()).map_err(stringify)?,
        );
        let knowledge_fabric = KnowledgeFabric::with_store(Arc::clone(&knowledge_store));
        let runtime_event_store = Arc::new(
            runtime_postgres::PostgresRuntimeEventStore::new(executor.clone())
                .map_err(stringify)?
                .into_runtime_event_store(),
        );
        let task_kernel = Arc::new(TaskKernel::from_runtime_kernel(
            runtime_postgres::PostgresTaskStore::new(executor.clone())
                .map_err(stringify)?
                .into_task_kernel(),
        ));
        let fact_ledger: Arc<dyn FactLedger> =
            Arc::new(fact_postgres::PostgresFactLedger::new(executor.clone()).map_err(stringify)?);
        let matrix_store: Arc<dyn MatrixStore> = Arc::new(
            matrix_repository::PostgresMatrixRepository::new(executor.clone())
                .map_err(stringify)?,
        );
        let approval_ledger: approval::SharedApprovalHistoryLedger = Arc::new(
            approval::PostgresApprovalHistoryLedger::new(executor.clone()).map_err(stringify)?,
        );
        let surface_messages: Arc<dyn SurfaceMessageLedger> = Arc::new(
            surface_postgres::PostgresSurfaceMessageLedger::new(executor.clone())
                .map_err(stringify)?,
        );
        let connector = Arc::new(
            connector_postgres::PostgresResourceDirectory::new(executor.clone())
                .map_err(stringify)?,
        );
        let connector_factory: Arc<dyn connector::ResourceDirectoryFactory> = Arc::new(
            connector_postgres::PostgresResourceDirectoryFactory::new(connector),
        );

        Ok(Self {
            backend: runtime::StorageBackendSelection::Postgres,
            registry,
            postgres_executor: Some(executor.clone()),
            app_topology: cowd_product_apps::AppStorageTopology::Postgres { executor },
            session_store,
            memory_store,
            knowledge_store,
            knowledge_fabric,
            runtime_event_store,
            task_kernel,
            fact_ledger,
            matrix_store,
            approval_ledger,
            surface_messages,
            connector_factory,
            connector_handle: connector_endpoint.as_handle(),
        })
    }

    #[must_use]
    pub(crate) const fn backend_label(&self) -> &'static str {
        match self.backend {
            runtime::StorageBackendSelection::Sqlite => "sqlite",
            runtime::StorageBackendSelection::Postgres => "postgres",
        }
    }

    pub(crate) fn health_projection(&self) -> serde_json::Value {
        serde_json::json!({
            "backend": self.backend_label(),
            "endpoint_count": self.registry.endpoints.len(),
            "postgres": self.postgres_executor.as_ref().map(PostgresExecutor::health),
        })
    }
}

fn base_registry(config_home: &Path, workspace_root: &Path) -> Result<StorageRegistry, String> {
    StorageRegistry::default_for_config_home(config_home)
        .with_workspace(workspace_root)
        .and_then(StorageRegistry::with_surface_messages)
        .map_err(stringify)
}

fn endpoint(
    registry: &StorageRegistry,
    domain: &StorageDomainId,
    scope: Option<&StorageScope>,
) -> Result<StorageEndpoint, String> {
    scope
        .map_or_else(
            || registry.endpoint(domain),
            |scope| registry.endpoint_in_scope(domain, scope),
        )
        .cloned()
        .map_err(stringify)
}

fn workspace_scope(registry: &StorageRegistry) -> Result<StorageScope, String> {
    let mut scopes = registry
        .endpoints
        .iter()
        .filter_map(|endpoint| match &endpoint.scope {
            StorageScope::Workspace { .. } => Some(endpoint.scope.clone()),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    if scopes.len() != 1 {
        return Err(format!(
            "selected storage registry contains {} workspace scopes; expected one",
            scopes.len()
        ));
    }
    Ok(scopes.pop_first().expect("one workspace scope"))
}

fn replace_business_endpoints_with_postgres(registry: &mut StorageRegistry) -> Result<(), String> {
    let domains = [
        StorageDomainId::Session,
        StorageDomainId::Memory,
        StorageDomainId::Knowledge,
        StorageDomainId::Fact,
        StorageDomainId::Growth,
        StorageDomainId::Matrix,
        StorageDomainId::Tasks,
        StorageDomainId::Approval,
        StorageDomainId::SurfaceMessages,
    ];
    for domain in domains {
        registry
            .replace_endpoint(StorageEndpoint::postgres(
                domain,
                StorageScope::Global,
                "cowd-selected-storage",
                "postgres-selected-since-0.9.581",
            ))
            .map_err(stringify)?;
    }
    let workspace_scope = workspace_scope(registry)?;
    for domain in [
        StorageDomainId::RuntimeEvents,
        StorageDomainId::ConnectorDirectory,
    ] {
        registry
            .replace_endpoint(StorageEndpoint::postgres(
                domain,
                workspace_scope.clone(),
                "cowd-selected-storage",
                "postgres-selected-since-0.9.581",
            ))
            .map_err(stringify)?;
    }
    Ok(())
}

struct EnvSecretRefResolver;

impl SecretRefResolver for EnvSecretRefResolver {
    fn resolve_postgres_url(
        &self,
        secret_ref: &str,
    ) -> Result<ResolvedPostgresUrl, storage::StorageError> {
        let variable = secret_ref.strip_prefix("env:").ok_or_else(|| {
            storage::StorageError::Other(
                "PostgreSQL secret_ref must use the env:VARIABLE form".to_string(),
            )
        })?;
        if variable.is_empty() {
            return Err(storage::StorageError::Other(
                "PostgreSQL environment variable name is empty".to_string(),
            ));
        }
        std::env::var(variable)
            .map(ResolvedPostgresUrl::new)
            .map_err(|_| {
                storage::StorageError::Other(format!(
                    "PostgreSQL secret environment variable `{variable}` is unavailable"
                ))
            })
    }
}

fn connect_postgres(config: &runtime::PostgresTopologyConfig) -> Result<PostgresExecutor, String> {
    let mut connection = PostgresConnectionConfig::new(
        config.logical_identity.clone(),
        config.secret_ref.clone(),
        format!("cowd-{}", env!("CARGO_PKG_VERSION")),
    );
    connection.max_connections = config.max_connections;
    connection.min_idle_connections = config.min_idle_connections;
    connection.checkout_timeout_ms = config.checkout_timeout_ms;
    std::thread::spawn(move || PostgresExecutor::connect(connection, &EnvSecretRefResolver))
        .join()
        .map_err(|_| "PostgreSQL executor initialization thread panicked".to_string())?
        .map_err(stringify)
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_topology_selects_every_business_domain_once() {
        let home = tempfile::tempdir().expect("config home");
        let workspace = tempfile::tempdir().expect("workspace");
        let topology = SelectedStorageTopology::compose_for_runtime(
            &runtime::StorageTopologyConfig::default(),
            &runtime::AppsConfig::default(),
            home.path(),
            workspace.path(),
        )
        .expect("SQLite topology");
        assert_eq!(topology.backend_label(), "sqlite");
        assert!(topology.postgres_executor.is_none());
        assert_eq!(
            topology
                .registry
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.domain == StorageDomainId::Session)
                .count(),
            1
        );
    }

    #[test]
    fn postgres_runtime_startup_fails_before_secret_resolution_without_cutover_manifest() {
        let home = tempfile::tempdir().expect("config home");
        let workspace = tempfile::tempdir().expect("workspace");
        let config = runtime::StorageTopologyConfig {
            backend: runtime::StorageBackendSelection::Postgres,
            postgres: Some(runtime::PostgresTopologyConfig {
                logical_identity: "test".to_string(),
                secret_ref: "env:THIS_MUST_NOT_BE_READ".to_string(),
                ..runtime::PostgresTopologyConfig::default()
            }),
            ..runtime::StorageTopologyConfig::default()
        };
        let error = SelectedStorageTopology::compose_for_runtime(
            &config,
            &runtime::AppsConfig::default(),
            home.path(),
            workspace.path(),
        )
        .err()
        .expect("missing activation blocks startup");
        assert!(error.contains("activation manifest"));
    }
}
