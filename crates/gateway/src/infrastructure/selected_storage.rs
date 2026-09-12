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
    pub(crate) registry: StorageRegistry,
    pub(crate) postgres_executor: PostgresExecutor,
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
    #[cfg(any(test, feature = "test-support"))]
    test_namespace: Option<Arc<TestPostgresNamespace>>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
struct TestPostgresNamespace {
    executor: PostgresExecutor,
    name: String,
    _permit: TestPostgresFixturePermit,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
struct TestPostgresFixturePermit;

#[cfg(any(test, feature = "test-support"))]
fn acquire_test_postgres_fixture_permit() -> TestPostgresFixturePermit {
    let (active, available) = test_postgres_fixture_capacity();
    let mut active = active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while *active >= 8 {
        active = available
            .wait(active)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    *active += 1;
    TestPostgresFixturePermit
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for TestPostgresFixturePermit {
    fn drop(&mut self) {
        let (active, available) = test_postgres_fixture_capacity();
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = active.saturating_sub(1);
        available.notify_one();
    }
}

#[cfg(any(test, feature = "test-support"))]
fn test_postgres_fixture_capacity() -> &'static (std::sync::Mutex<usize>, std::sync::Condvar) {
    static CAPACITY: std::sync::OnceLock<(std::sync::Mutex<usize>, std::sync::Condvar)> =
        std::sync::OnceLock::new();
    CAPACITY.get_or_init(|| (std::sync::Mutex::new(0), std::sync::Condvar::new()))
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for TestPostgresNamespace {
    fn drop(&mut self) {
        // `name` is generated below and validated by `scoped_namespace`; it is
        // never caller-controlled. Cleanup is deliberately limited to this
        // fixture-owned schema and can therefore not touch public/user data.
        let statement = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.name);
        if let Ok(mut connection) = self.executor.checkout_critical() {
            if let Err(error) = connection.batch_execute(&statement) {
                tracing::warn!(
                    schema = %self.name,
                    %error,
                    "failed to remove isolated PostgreSQL test namespace"
                );
            }
        }
    }
}

impl SelectedStorageTopology {
    pub(crate) fn compose_for_runtime(
        config: &runtime::StorageTopologyConfig,
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        Self::compose(config, true, config_home, workspace_root)
    }

    pub(crate) fn compose_for_maintenance(
        config: &runtime::StorageTopologyConfig,
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        Self::compose(config, false, config_home, workspace_root)
    }

    /// Build the complete production topology inside a fixture-owned schema.
    ///
    /// Database-dependent tests intentionally fail when the required URL is
    /// absent. This prevents `cargo test` from silently proving a different
    /// SQLite architecture than the one shipped by Gateway.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn compose_for_test(
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        let permit = acquire_test_postgres_fixture_permit();
        let mut postgres = runtime::PostgresTopologyConfig::default();
        postgres.logical_identity = format!("cowd-test-{}", std::process::id());
        postgres.secret_ref = "env:COWD_TEST_POSTGRES_URL".to_string();
        // All isolated schemas share one bounded process pool. Domain work
        // still retains separate workload lanes while schema count does not
        // multiply server connections.
        postgres.max_connections = 24;
        postgres.server_reserve = 4;
        postgres.critical.max_connections = Some(8);
        postgres.online_read.max_connections = Some(12);
        postgres.background.max_connections = Some(4);
        postgres.critical.min_idle_connections = None;
        postgres.online_read.min_idle_connections = None;
        postgres.background.min_idle_connections = None;
        let config = runtime::StorageTopologyConfig {
            backend: runtime::StorageBackendSelection::Postgres,
            postgres: Some(postgres),
            session_execution: runtime::SessionStorageExecutionConfig {
                workers: 1,
                queue_capacity: 64,
            },
            ..runtime::StorageTopologyConfig::default()
        };
        let registry = base_registry(config_home, workspace_root)?;
        let executor = shared_test_postgres_executor(
            config.postgres.as_ref().expect("test PostgreSQL config"),
            config_home,
        )?;
        let name = format!(
            "cowdtest_{}_{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        executor
            .checkout_critical()
            .map_err(stringify)?
            .batch_execute(&format!("CREATE SCHEMA \"{name}\""))
            .map_err(stringify)?;
        let scoped = executor.scoped_namespace(&name).map_err(stringify)?;
        let guard = Arc::new(TestPostgresNamespace {
            executor,
            name,
            _permit: permit,
        });
        let result = Self::postgres(registry, scoped, config.session_execution, config.artifacts);
        match result {
            Ok(mut topology) => {
                topology.test_namespace = Some(guard);
                topology.verify_runtime_readiness()
            }
            Err(error) => {
                drop(guard);
                Err(error)
            }
        }
    }

    fn compose(
        config: &runtime::StorageTopologyConfig,
        runtime_mode: bool,
        config_home: &Path,
        workspace_root: &Path,
    ) -> Result<Self, String> {
        let registry = base_registry(config_home, workspace_root)?;
        let _ = config.backend;
        Self::compose_postgres(config, runtime_mode, config_home, registry)
    }

    fn compose_postgres(
        config: &runtime::StorageTopologyConfig,
        runtime_mode: bool,
        config_home: &Path,
        registry: StorageRegistry,
    ) -> Result<Self, String> {
        let postgres = config
            .postgres
            .as_ref()
            .ok_or_else(|| "storage.backend=postgres requires storage.postgres".to_string())?;
        let migration_mode = if runtime_mode {
            PostgresMigrationMode::RuntimeReadiness
        } else {
            PostgresMigrationMode::Maintenance
        };
        let executor = connect_postgres(postgres, migration_mode, config_home)?;
        let session_execution = config.session_execution;
        let artifacts = config.artifacts;
        std::thread::spawn(move || Self::postgres(registry, executor, session_execution, artifacts))
            .join()
            .map_err(|_| "PostgreSQL domain adapter initialization thread panicked".to_string())??
            .verify_runtime_readiness()
    }

    fn postgres(
        registry: StorageRegistry,
        executor: PostgresExecutor,
        session_execution: runtime::SessionStorageExecutionConfig,
        artifacts: runtime::ArtifactStorageConfig,
    ) -> Result<Self, String> {
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
            registry,
            postgres_executor: executor.clone(),
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
            #[cfg(any(test, feature = "test-support"))]
            test_namespace: None,
        })
    }

    #[must_use]
    pub(crate) const fn backend_label(&self) -> &'static str {
        "postgres"
    }

    pub(crate) fn health_projection(&self) -> serde_json::Value {
        serde_json::json!({
            "backend": self.backend_label(),
            "effective_backend": self.backend_label(),
            "endpoint_count": self.registry.endpoints.len(),
            "postgres": self.postgres_executor.health(),
        })
    }

    fn verify_runtime_readiness(self) -> Result<Self, String> {
        self.postgres_executor
            .verify_registered_migration_catalogs()
            .map_err(stringify)?;
        Ok(self)
    }
}

#[cfg(any(test, feature = "test-support"))]
fn shared_test_postgres_executor(
    config: &runtime::PostgresTopologyConfig,
    config_home: &Path,
) -> Result<PostgresExecutor, String> {
    static EXECUTOR: std::sync::OnceLock<Result<PostgresExecutor, String>> =
        std::sync::OnceLock::new();
    EXECUTOR
        .get_or_init(|| connect_postgres(config, PostgresMigrationMode::Maintenance, config_home))
        .clone()
}

fn base_registry(config_home: &Path, workspace_root: &Path) -> Result<StorageRegistry, String> {
    StorageRegistry::postgres_for_config_home(config_home)
        .with_postgres_workspace(workspace_root)
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
    fn registry_contains_only_supported_endpoint_kinds_after_selection() {
        let home = tempfile::tempdir().expect("config home");
        let workspace = tempfile::tempdir().expect("workspace");
        let registry = base_registry(home.path(), workspace.path()).expect("base registry");

        assert!(registry.endpoints.iter().all(|endpoint| matches!(
            endpoint.backend,
            storage::StorageBackendKind::Postgres
                | storage::StorageBackendKind::FileJson
                | storage::StorageBackendKind::Directory
                | storage::StorageBackendKind::BlobDirectory
        )));
        assert_eq!(
            registry
                .endpoint(&StorageDomainId::Tasks)
                .expect("PostgreSQL Task endpoint")
                .backend,
            storage::StorageBackendKind::Postgres
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn pg_only_every_store_identity_composes_all_domains_over_postgres() {
        let home = tempfile::tempdir().expect("config home");
        let workspace = tempfile::tempdir().expect("workspace");
        let topology = SelectedStorageTopology::compose_for_test(home.path(), workspace.path())
            .expect("isolated PostgreSQL topology");
        assert_eq!(topology.backend_label(), "postgres");
        let health = topology.health_projection();
        assert_eq!(health["backend"], "postgres");
        assert_eq!(health["effective_backend"], "postgres");
        assert!(
            health["endpoint_count"].as_u64().unwrap_or(0) > 0,
            "registry must bind every configured domain endpoint"
        );
        // Registry: catalog verification stays callable and never falls back.
        topology
            .postgres_executor
            .verify_registered_migration_catalogs()
            .expect("registered migration catalogs");
        // No domain may fall back to a local file database.
        for forbidden in ["session.sqlite", "memory.sqlite", "fact.sqlite", "matrix.sqlite"] {
            assert!(
                !home.path().join(forbidden).exists(),
                "{forbidden} must not be created by a PostgreSQL-only topology"
            );
        }
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
        let error =
            SelectedStorageTopology::compose_for_runtime(&config, home.path(), workspace.path())
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
