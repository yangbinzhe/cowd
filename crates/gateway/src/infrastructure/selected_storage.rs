//! One process-wide storage composition root.
//!
//! Domain services receive stable ports from this value. No request handler,
//! APP or Runtime turn is allowed to choose a driver or derive a database
//! path after startup.

use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use fact_kernel::FactLedger;
use matrix_repository::MatrixStore;
use memory::{KnowledgeFabric, KnowledgeStore, MemoryStore};
use session::UnifiedSessionStore;
use storage::{
    PostgresConnectionConfig, PostgresExecutor, PostgresMigrationMode, PostgresPoolLaneConfig,
    PostgresPoolSet, PostgresPoolSetConfig, ResolvedPostgresUrl, SecretRefResolver,
    StorageDomainId, StorageEndpoint, StorageRegistry, StorageScope,
};
use surface::SurfaceMessageLedger;

#[derive(Clone)]
pub(crate) struct SelectedStorageTopology {
    pub(crate) backend: runtime::StorageBackendSelection,
    pub(crate) registry: StorageRegistry,
    pub(crate) postgres_executor: Option<PostgresExecutor>,
    pub(crate) app_topology: cowd_product_apps::AppStorageTopology,
    pub(crate) session_store: Arc<UnifiedSessionStore>,
    pub(crate) memory_store: Arc<dyn MemoryStore>,
    pub(crate) memory_maintenance_queue: memory::MaintenanceQueue,
    pub(crate) knowledge_store: Arc<dyn KnowledgeStore>,
    pub(crate) knowledge_fabric: KnowledgeFabric,
    pub(crate) runtime_event_store: Arc<runtime::RuntimeEventStore>,
    pub(crate) task_service: Arc<runtime::TaskAggregateService>,
    pub(crate) fact_ledger: Arc<dyn FactLedger>,
    pub(crate) matrix_store: Arc<dyn MatrixStore>,
    pub(crate) surface_messages: Arc<dyn SurfaceMessageLedger>,
    pub(crate) connector_factory: Arc<dyn connector::ResourceDirectoryFactory>,
    pub(crate) connector_handle: storage::StorageHandle,
    pub(crate) artifact_store: Arc<runtime::ArtifactStore>,
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
                Self::sqlite(registry, config.session_execution, config.artifacts)
            }
            runtime::StorageBackendSelection::Postgres => {
                let postgres = config.postgres.as_ref().ok_or_else(|| {
                    "storage.backend=postgres requires storage.postgres".to_string()
                })?;
                let migration_mode = if activation_apps.is_some() {
                    PostgresMigrationMode::RuntimeReadiness
                } else {
                    PostgresMigrationMode::Maintenance
                };
                let executor = connect_postgres(postgres, migration_mode, config_home)?;
                let session_execution = config.session_execution;
                let artifacts = config.artifacts;
                std::thread::spawn(move || {
                    Self::postgres(registry, executor, session_execution, artifacts)
                })
                .join()
                .map_err(|_| {
                    "PostgreSQL domain adapter initialization thread panicked".to_string()
                })??
                .verify_runtime_readiness()
            }
        }
    }

    fn sqlite(
        registry: StorageRegistry,
        session_execution: runtime::SessionStorageExecutionConfig,
        artifacts: runtime::ArtifactStorageConfig,
    ) -> Result<Self, String> {
        registry.ensure_directories().map_err(stringify)?;
        let session_endpoint = endpoint(&registry, &StorageDomainId::Session, None)?;
        let memory_endpoint = endpoint(&registry, &StorageDomainId::Memory, None)?;
        let knowledge_endpoint = endpoint(&registry, &StorageDomainId::Knowledge, None)?;
        let fact_endpoint = endpoint(&registry, &StorageDomainId::Fact, None)?;
        let growth_endpoint = endpoint(&registry, &StorageDomainId::Growth, None)?;
        let matrix_endpoint = endpoint(&registry, &StorageDomainId::Matrix, None)?;
        let surface_endpoint = endpoint(&registry, &StorageDomainId::SurfaceMessages, None)?;
        let workspace_scope = workspace_scope(&registry)?;
        let task_endpoint = endpoint(&registry, &StorageDomainId::Tasks, Some(&workspace_scope))?;
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
        let blob_endpoint = endpoint(&registry, &StorageDomainId::Blobs, None)?;
        let artifact_store = Arc::new(
            runtime::ArtifactStore::sqlite(blob_endpoint.path, artifacts.into())
                .map_err(|error| error.to_string())?,
        );

        let session_store = Arc::new(
            UnifiedSessionStore::open_sqlite_storage_handle_with_execution_config(
                &session_endpoint.as_handle(),
                session::StorageExecutionPlaneConfig {
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
        let memory_maintenance_queue =
            memory::MaintenanceQueue::open_sqlite(&memory_endpoint.path).map_err(stringify)?;
        let knowledge_store: Arc<dyn KnowledgeStore> = Arc::new(
            memory::SqliteKnowledgeStore::open(&knowledge_endpoint.path).map_err(stringify)?,
        );
        let knowledge_fabric = KnowledgeFabric::with_store(Arc::clone(&knowledge_store));
        let runtime_event_store = Arc::new(
            runtime::RuntimeEventStore::try_open(&runtime_endpoint.path).map_err(stringify)?,
        );
        let task_service = Arc::new(
            runtime::TaskAggregateService::open_storage_handle(&task_endpoint.as_handle())
                .map_err(stringify)?,
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
        let surface_messages: Arc<dyn SurfaceMessageLedger> = Arc::new(
            crate::surface_host::SqliteSurfaceMessageStore::from_storage_endpoint(
                &surface_endpoint,
            )?,
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
            memory_maintenance_queue,
            knowledge_store,
            knowledge_fabric,
            runtime_event_store,
            task_service,
            fact_ledger,
            matrix_store,
            surface_messages,
            connector_factory,
            connector_handle: connector_endpoint.as_handle(),
            artifact_store,
        })
    }

    fn postgres(
        mut registry: StorageRegistry,
        executor: PostgresExecutor,
        session_execution: runtime::SessionStorageExecutionConfig,
        artifacts: runtime::ArtifactStorageConfig,
    ) -> Result<Self, String> {
        replace_business_endpoints_with_postgres(&mut registry)?;
        let workspace_scope = workspace_scope(&registry)?;
        let connector_endpoint = endpoint(
            &registry,
            &StorageDomainId::ConnectorDirectory,
            Some(&workspace_scope),
        )?;
        let blob_endpoint = endpoint(&registry, &StorageDomainId::Blobs, None)?;
        let artifact_store = Arc::new(
            runtime::ArtifactStore::new(
                blob_endpoint.path,
                Arc::new(
                    runtime_postgres::PostgresArtifactRepository::new(executor.clone())
                        .map_err(stringify)?,
                ),
                artifacts.into(),
            )
            .map_err(stringify)?,
        );

        let session =
            session_postgres::PostgresSessionStore::new(executor.clone()).map_err(stringify)?;
        let online_read_connections = executor
            .health()
            .lanes
            .iter()
            .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
            .map_or(1, |lane| lane.max_connections);
        let session_workers =
            postgres_session_workers(session_execution.workers, online_read_connections);
        let session_store = Arc::new(
            UnifiedSessionStore::from_backend_with_execution_config(
                Arc::new(session),
                session::StorageExecutionPlaneConfig {
                    workers: session_workers,
                    queue_capacity: session_execution.queue_capacity,
                },
            )
            .map_err(stringify)?,
        );
        let memory_store: Arc<dyn MemoryStore> = Arc::new(
            memory_postgres::PostgresMemoryStore::new(executor.clone()).map_err(stringify)?,
        );
        let memory_maintenance_queue = memory::MaintenanceQueue::from_backend(Arc::new(
            memory_postgres::PostgresMaintenanceQueue::new(executor.clone()).map_err(stringify)?,
        ));
        let knowledge_store: Arc<dyn KnowledgeStore> = Arc::new(
            memory_postgres::PostgresKnowledgeStore::new(executor.clone()).map_err(stringify)?,
        );
        let knowledge_fabric = KnowledgeFabric::with_store(Arc::clone(&knowledge_store));
        let runtime_event_store = Arc::new(
            runtime_postgres::PostgresRuntimeEventStore::new(executor.clone())
                .map_err(stringify)?
                .into_runtime_event_store(),
        );
        let task_service = Arc::new(
            runtime_postgres::PostgresTaskStore::new(executor.clone())
                .map_err(stringify)?
                .into_task_service(),
        );
        let fact_ledger: Arc<dyn FactLedger> =
            Arc::new(fact_postgres::PostgresFactLedger::new(executor.clone()).map_err(stringify)?);
        let matrix_store: Arc<dyn MatrixStore> = Arc::new(
            matrix_repository::PostgresMatrixRepository::new(executor.clone())
                .map_err(stringify)?,
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
            memory_maintenance_queue,
            knowledge_store,
            knowledge_fabric,
            runtime_event_store,
            task_service,
            fact_ledger,
            matrix_store,
            surface_messages,
            connector_factory,
            connector_handle: connector_endpoint.as_handle(),
            artifact_store,
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

    fn verify_runtime_readiness(self) -> Result<Self, String> {
        if let Some(executor) = &self.postgres_executor {
            executor
                .verify_registered_migration_catalogs()
                .map_err(stringify)?;
        }
        Ok(self)
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
    scopes
        .pop_first()
        .ok_or_else(|| "selected storage has no workspace scope".to_string())
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
    // The selected topology is an operational inventory, not a catalogue of
    // historical defaults. PostgreSQL composition injects every live database
    // adapter above, so retaining unused SQLite endpoints would advertise a
    // false dual-backend runtime and make health diagnostics probe stale files.
    registry
        .endpoints
        .retain(|endpoint| endpoint.backend != storage::StorageBackendKind::Sqlite);
    Ok(())
}

struct ConfigHomeSecretRefResolver {
    secret_root: PathBuf,
}

impl ConfigHomeSecretRefResolver {
    fn new(config_home: &Path) -> Self {
        Self {
            secret_root: config_home.join("secrets"),
        }
    }

    fn resolve_file(&self, secret_id: &str) -> Result<ResolvedPostgresUrl, storage::StorageError> {
        let mut components = Path::new(secret_id).components();
        let Some(Component::Normal(_)) = components.next() else {
            return Err(storage::StorageError::Other(
                "PostgreSQL file secret id must be one safe file name".to_string(),
            ));
        };
        if components.next().is_some() {
            return Err(storage::StorageError::Other(
                "PostgreSQL file secret id must be one safe file name".to_string(),
            ));
        }
        let path = self.secret_root.join(secret_id);
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut secret = options.open(&path).map_err(|error| {
            #[cfg(unix)]
            if error.raw_os_error() == Some(libc::ELOOP) {
                return storage::StorageError::Other(format!(
                    "PostgreSQL file secret `{secret_id}` must be a regular non-symlink file"
                ));
            }
            storage::StorageError::Other(format!(
                "PostgreSQL file secret `{secret_id}` is unavailable"
            ))
        })?;
        let metadata = secret.metadata().map_err(|_| {
            storage::StorageError::Other(format!(
                "PostgreSQL file secret `{secret_id}` metadata is unavailable"
            ))
        })?;
        if !metadata.file_type().is_file() {
            return Err(storage::StorageError::Other(format!(
                "PostgreSQL file secret `{secret_id}` must be a regular non-symlink file"
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(storage::StorageError::Other(format!(
                    "PostgreSQL file secret `{secret_id}` permissions must not grant group or other access"
                )));
            }
        }
        let mut value = String::new();
        secret.read_to_string(&mut value).map_err(|_| {
            storage::StorageError::Other(format!(
                "PostgreSQL file secret `{secret_id}` cannot be read"
            ))
        })?;
        let value = value.trim_end_matches(['\r', '\n']);
        if value.is_empty() || value.contains(['\r', '\n']) {
            return Err(storage::StorageError::Other(format!(
                "PostgreSQL file secret `{secret_id}` must contain exactly one non-empty value"
            )));
        }
        Ok(ResolvedPostgresUrl::new(value))
    }
}

impl SecretRefResolver for ConfigHomeSecretRefResolver {
    fn resolve_postgres_url(
        &self,
        secret_ref: &str,
    ) -> Result<ResolvedPostgresUrl, storage::StorageError> {
        if let Some(variable) = secret_ref.strip_prefix("env:") {
            if variable.is_empty() {
                return Err(storage::StorageError::Other(
                    "PostgreSQL environment variable name is empty".to_string(),
                ));
            }
            return std::env::var(variable)
                .map(ResolvedPostgresUrl::new)
                .map_err(|_| {
                    storage::StorageError::Other(format!(
                        "PostgreSQL secret environment variable `{variable}` is unavailable"
                    ))
                });
        }
        if let Some(secret_id) = secret_ref.strip_prefix("file:") {
            return self.resolve_file(secret_id);
        }
        Err(storage::StorageError::Other(
            "PostgreSQL secret_ref must use env:VARIABLE or file:SECRET_ID".to_string(),
        ))
    }
}

fn connect_postgres(
    config: &runtime::PostgresTopologyConfig,
    migration_mode: PostgresMigrationMode,
    config_home: &Path,
) -> Result<PostgresExecutor, String> {
    let mut connection = PostgresConnectionConfig::new(
        config.logical_identity.clone(),
        config.secret_ref.clone(),
        format!("cowd-{}", env!("CARGO_PKG_VERSION")),
    );
    connection.max_connections = config.max_connections;
    connection.min_idle_connections = None;
    connection.checkout_timeout_ms = config.online_read.checkout_timeout_ms;
    let [critical_max, online_read_max, background_max] =
        resolved_postgres_lane_sizes(config.max_connections, config);
    let pool_set = PostgresPoolSetConfig {
        connection,
        server_reserve: config.server_reserve,
        critical: PostgresPoolLaneConfig::new(
            critical_max,
            config
                .critical
                .min_idle_connections
                .map(|minimum| minimum.min(critical_max)),
            config.critical.checkout_timeout_ms,
        ),
        online_read: PostgresPoolLaneConfig::new(
            online_read_max,
            config
                .online_read
                .min_idle_connections
                .map(|minimum| minimum.min(online_read_max)),
            config.online_read.checkout_timeout_ms,
        ),
        background: PostgresPoolLaneConfig::new(
            background_max,
            config
                .background
                .min_idle_connections
                .map(|minimum| minimum.min(background_max)),
            config.background.checkout_timeout_ms,
        ),
    };
    let resolver = ConfigHomeSecretRefResolver::new(config_home);
    std::thread::spawn(move || {
        PostgresPoolSet::connect_with_migration_mode(pool_set, &resolver, migration_mode)
    })
    .join()
    .map_err(|_| "PostgreSQL executor initialization thread panicked".to_string())?
    .map(|pool_set| pool_set.executor())
    .map_err(stringify)
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn postgres_session_workers(configured: usize, max_connections: u32) -> usize {
    configured.max(1).min(max_connections.max(1) as usize)
}

fn resolved_postgres_lane_sizes(total: u32, config: &runtime::PostgresTopologyConfig) -> [u32; 3] {
    match (
        config.critical.max_connections,
        config.online_read.max_connections,
        config.background.max_connections,
    ) {
        (Some(critical), Some(online_read), Some(background)) => {
            [critical, online_read, background]
        }
        _ => {
            let critical = (total / 3).max(1);
            let online_read = (total / 2).max(1);
            let background = total
                .saturating_sub(critical)
                .saturating_sub(online_read)
                .max(1);
            [critical, online_read, background]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn postgres_session_workers_are_bounded_by_the_online_read_lane() {
        assert_eq!(postgres_session_workers(32, 16), 16);
        assert_eq!(postgres_session_workers(2, 16), 2);
        assert_eq!(postgres_session_workers(8, 4), 4);
    }

    #[test]
    fn default_postgres_budget_splits_into_three_isolated_lanes() {
        let config = runtime::PostgresTopologyConfig::default();
        assert_eq!(resolved_postgres_lane_sizes(48, &config), [16, 24, 8]);
    }

    #[test]
    fn postgres_registry_contains_no_sqlite_endpoint_after_selection() {
        let home = tempfile::tempdir().expect("config home");
        let workspace = tempfile::tempdir().expect("workspace");
        let mut registry = base_registry(home.path(), workspace.path()).expect("base registry");

        replace_business_endpoints_with_postgres(&mut registry).expect("PostgreSQL endpoints");

        assert!(registry
            .endpoints
            .iter()
            .all(|endpoint| endpoint.backend != storage::StorageBackendKind::Sqlite));
        assert_eq!(
            registry
                .endpoint(&StorageDomainId::Tasks)
                .expect("PostgreSQL Task endpoint")
                .backend,
            storage::StorageBackendKind::Postgres
        );
    }

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
        let workspace_scope = StorageScope::workspace_for_root(workspace.path());
        let workspace_tasks = topology
            .registry
            .endpoint_in_scope(&StorageDomainId::Tasks, &workspace_scope)
            .expect("workspace task endpoint");
        let global_tasks = topology
            .registry
            .endpoint(&StorageDomainId::Tasks)
            .expect("legacy global task endpoint");
        assert!(workspace_tasks.path.exists());
        assert!(!global_tasks.path.exists());
    }

    #[test]
    fn postgres_runtime_startup_does_not_depend_on_historical_cutover_manifest() {
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
        .expect("missing secret blocks startup after topology selection");
        assert!(error.contains("THIS_MUST_NOT_BE_READ"));
        assert!(!error.contains("activation manifest"));
    }

    #[test]
    fn config_home_file_secret_is_resolved_without_exposing_its_value() {
        let home = tempfile::tempdir().expect("config home");
        let secret_root = home.path().join("secrets");
        fs::create_dir_all(&secret_root).expect("secret root");
        let secret_path = secret_root.join("postgres-primary");
        let mut secret = fs::File::create(&secret_path).expect("secret file");
        writeln!(secret, "postgres://user:password@localhost/cowd").expect("secret value");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600))
                .expect("secret permissions");
        }

        let resolver = ConfigHomeSecretRefResolver::new(home.path());
        let resolved = resolver
            .resolve_postgres_url("file:postgres-primary")
            .expect("file secret");
        assert_eq!(format!("{resolved:?}"), "ResolvedPostgresUrl(<redacted>)");
    }

    #[test]
    fn config_home_file_secret_rejects_path_escape_and_unknown_scheme() {
        let home = tempfile::tempdir().expect("config home");
        let resolver = ConfigHomeSecretRefResolver::new(home.path());

        let traversal = resolver
            .resolve_postgres_url("file:../outside")
            .expect_err("path traversal must fail")
            .to_string();
        assert!(traversal.contains("safe file name"));
        let unknown = resolver
            .resolve_postgres_url("literal:postgres://localhost/cowd")
            .expect_err("unknown scheme must fail")
            .to_string();
        assert!(unknown.contains("env:VARIABLE or file:SECRET_ID"));
    }

    #[cfg(unix)]
    #[test]
    fn config_home_file_secret_rejects_symlinks_and_broad_permissions() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let home = tempfile::tempdir().expect("config home");
        let secret_root = home.path().join("secrets");
        fs::create_dir_all(&secret_root).expect("secret root");
        let target = secret_root.join("target");
        fs::write(&target, "postgres://localhost/cowd").expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).expect("target mode");
        symlink(&target, secret_root.join("linked")).expect("symlink");
        let broad = secret_root.join("broad");
        fs::write(&broad, "postgres://localhost/cowd").expect("broad");
        fs::set_permissions(&broad, fs::Permissions::from_mode(0o644)).expect("broad mode");
        let resolver = ConfigHomeSecretRefResolver::new(home.path());

        assert!(resolver
            .resolve_postgres_url("file:linked")
            .expect_err("symlink must fail")
            .to_string()
            .contains("non-symlink"));
        assert!(resolver
            .resolve_postgres_url("file:broad")
            .expect_err("broad permissions must fail")
            .to_string()
            .contains("permissions"));
    }
}
