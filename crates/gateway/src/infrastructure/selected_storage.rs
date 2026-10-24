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
    preserve_failure: std::sync::atomic::AtomicBool,
    executor: PostgresExecutor,
    name: String,
    _permit: TestPostgresFixturePermit,
    #[cfg(test)]
    session_execution: runtime::SessionStorageExecutionConfig,
    #[cfg(test)]
    artifacts: runtime::ArtifactStorageConfig,
}

#[cfg(test)]
pub(crate) struct DetachedTestPostgresRestartLease {
    registry: StorageRegistry,
    executor: PostgresExecutor,
    namespace: Arc<TestPostgresNamespace>,
}

#[cfg(test)]
impl DetachedTestPostgresRestartLease {
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace.name
    }

    pub(crate) fn rebuild(&self) -> Result<SelectedStorageTopology, String> {
        if Arc::strong_count(&self.namespace) != 1 {
            return Err("previous test topology still owns the restart namespace".into());
        }
        let mut topology = SelectedStorageTopology::postgres(
            self.registry.clone(),
            self.executor.clone(),
            self.namespace.session_execution,
            self.namespace.artifacts,
        )?;
        topology.test_namespace = Some(Arc::clone(&self.namespace));
        topology.verify_runtime_readiness()
    }
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
        if self.preserve_failure.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
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
    #[cfg(test)]
    pub(crate) fn preserve_failed_test_namespace(&self) -> Option<&str> {
        let namespace = self.test_namespace.as_ref()?;
        namespace.preserve_failure.store(true, std::sync::atomic::Ordering::Release);
        Some(&namespace.name)
    }

    pub(crate) fn artifact_maintenance_guard(&self) -> ArtifactMaintenanceGuard {
        ArtifactMaintenanceGuard {
            #[cfg(any(test, feature = "test-support"))]
            _namespace: self.test_namespace.clone(),
        }
    }
    pub(crate) fn quiesced_artifact_roots(&self) -> runtime::QuiescedContentRoots {
        self.runtime_event_store.quiesced_content_roots()
    }
    pub(crate) fn into_exclusive_artifact_store(self) -> Result<runtime::ArtifactStore, String> {
        let artifact_store = Arc::clone(&self.artifact_store);
        drop(self);
        Arc::try_unwrap(artifact_store)
            .map_err(|_| "artifact maintenance requires exclusive local ownership".to_string())
    }

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
        let budget = storage::test_fixture_pool_budget(3, Some(24))?;
        let permit = acquire_test_postgres_fixture_permit();
        let mut postgres = runtime::PostgresTopologyConfig::default();
        postgres.logical_identity = format!("cowd-test-{}", std::process::id());
        postgres.secret_ref = "env:COWD_TEST_POSTGRES_URL".to_string();
        // All isolated schemas share one bounded process pool. Domain work
        // still retains separate workload lanes while schema count does not
        // multiply server connections.
        postgres.max_connections = budget;
        postgres.server_reserve = 4;
        postgres.critical.max_connections = None;
        postgres.online_read.max_connections = None;
        postgres.background.max_connections = None;
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
            preserve_failure: std::sync::atomic::AtomicBool::new(false),
            executor,
            name,
            _permit: permit,
            #[cfg(test)]
            session_execution: config.session_execution,
            #[cfg(test)]
            artifacts: config.artifacts,
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

    #[cfg(test)]
    pub(crate) fn detach_for_test_restart(
        self,
    ) -> Result<DetachedTestPostgresRestartLease, String> {
        let namespace = self
            .test_namespace
            .ok_or_else(|| "restart requires an isolated test namespace".to_string())?;
        if Arc::strong_count(&namespace) != 1 {
            return Err("cannot detach while another test topology owns the namespace".into());
        }
        Ok(DetachedTestPostgresRestartLease {
            registry: self.registry,
            executor: self.postgres_executor,
            namespace,
        })
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
        let memory_owner = Arc::new(
            memory_postgres::PostgresMemoryStore::new(executor.clone()).map_err(stringify)?,
        );
        let memory_store: Arc<dyn MemoryStore> = memory_owner.clone();
        let memory_maintenance_queue = memory::MaintenanceQueue::from_backend(Arc::new(
            memory_postgres::PostgresMaintenanceQueue::new(executor.clone()).map_err(stringify)?,
        ));
        let knowledge_store: Arc<dyn KnowledgeStore> = Arc::new(
            memory_postgres::PostgresKnowledgeStore::new(executor.clone()).map_err(stringify)?,
        );
        let knowledge_fabric = KnowledgeFabric::with_store(Arc::clone(&knowledge_store))
            .with_memory_sources(memory_owner);
        let runtime_event_store = Arc::new(
            runtime_postgres::PostgresRuntimeEventStore::new(executor.clone())
                .map_err(stringify)?
                .into_runtime_event_store(),
        );
        runtime_event_store
            .bind_artifact_store(Arc::clone(&artifact_store))
            .map_err(stringify)?;
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

pub(crate) struct ArtifactMaintenanceGuard {
    #[cfg(any(test, feature = "test-support"))]
    _namespace: Option<Arc<TestPostgresNamespace>>,
}

#[cfg(any(test, feature = "test-support"))]
type TestPostgresExecutorKey = (Result<[u8; 32], String>, runtime::PostgresTopologyConfig);

#[cfg(any(test, feature = "test-support"))]
struct TestPostgresExecutorCache {
    key: TestPostgresExecutorKey,
    result: Result<PostgresExecutor, String>,
}

#[cfg(any(test, feature = "test-support"))]
impl TestPostgresExecutorCache {
    fn get(&self, key: &TestPostgresExecutorKey) -> Result<PostgresExecutor, String> {
        if &self.key != key {
            return Err(
                "PostgreSQL test executor configuration changed; start a new process".into(),
            );
        }
        self.result.clone()
    }
}

#[cfg(any(test, feature = "test-support"))]
fn shared_test_postgres_executor(
    config: &runtime::PostgresTopologyConfig,
    config_home: &Path,
) -> Result<PostgresExecutor, String> {
    static EXECUTOR: std::sync::OnceLock<TestPostgresExecutorCache> = std::sync::OnceLock::new();
    // Resolve on every request to detect changed secret values. Only the first
    // snapshot can connect; later mismatches cannot create another pool.
    let snapshot = ConfigHomeSecretRefResolver::new(config_home)
        .resolve_postgres_url(&config.secret_ref)
        .map(|url| storage::test_fixture_postgres_url_snapshot(&config.secret_ref, &url))
        .map_err(|_| "PostgreSQL test URL could not be resolved".to_string());
    let key = (
        snapshot
            .as_ref()
            .map(|(token, _)| *token)
            .map_err(Clone::clone),
        config.clone(),
    );
    EXECUTOR
        .get_or_init(|| {
            let result = snapshot.and_then(|(_, resolver)| {
                // Use the same lane rule as production, with the frozen resolver.
                let mut connection = PostgresConnectionConfig::new(
                    config.logical_identity.clone(),
                    config.secret_ref.clone(),
                    format!("cowd-{}", env!("CARGO_PKG_VERSION")),
                );
                connection.max_connections = config.max_connections;
                connection.min_idle_connections = None;
                connection.checkout_timeout_ms = config.online_read.checkout_timeout_ms;
                let [critical, online_read, background] =
                    resolved_postgres_lane_sizes(config.max_connections, config);
                let lane = |maximum, minimum: Option<u32>, timeout| {
                    PostgresPoolLaneConfig::new(
                        maximum,
                        minimum.map(|minimum| minimum.min(maximum)),
                        timeout,
                    )
                };
                let pool_set = PostgresPoolSetConfig {
                    connection,
                    server_reserve: config.server_reserve,
                    critical: lane(
                        critical,
                        config.critical.min_idle_connections,
                        config.critical.checkout_timeout_ms,
                    ),
                    online_read: lane(
                        online_read,
                        config.online_read.min_idle_connections,
                        config.online_read.checkout_timeout_ms,
                    ),
                    background: lane(
                        background,
                        config.background.min_idle_connections,
                        config.background.checkout_timeout_ms,
                    ),
                };
                std::thread::spawn(move || {
                    PostgresPoolSet::connect_with_migration_mode(
                        pool_set,
                        &resolver,
                        PostgresMigrationMode::Maintenance,
                    )
                })
                .join()
                .map_err(|_| "PostgreSQL test executor initialization thread panicked".to_string())?
                .map(|pool_set| pool_set.executor())
                .map_err(stringify)
            });
            TestPostgresExecutorCache {
                key: key.clone(),
                result,
            }
        })
        .get(&key)
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

    #[cfg(unix)]
    fn fixture_runtime_executor(
        fixture: &SelectedStorageTopology,
        home: &Path,
    ) -> PostgresExecutor {
        let namespace = fixture
            .test_namespace
            .as_ref()
            .expect("original fixture namespace");
        let config = runtime::PostgresTopologyConfig {
            logical_identity: fixture.postgres_executor.logical_identity().to_string(),
            secret_ref: "env:COWD_TEST_POSTGRES_URL".into(),
            max_connections: 12,
            ..runtime::PostgresTopologyConfig::default()
        };
        connect_postgres(&config, PostgresMigrationMode::RuntimeReadiness, home)
            .unwrap()
            .scoped_namespace(&namespace.name)
            .unwrap()
    }

    #[cfg(unix)]
    fn fixture_runtime_topology(
        registry: StorageRegistry,
        executor: PostgresExecutor,
        namespace: &Arc<TestPostgresNamespace>,
    ) -> Result<SelectedStorageTopology, String> {
        assert_eq!(
            executor.migration_mode(),
            PostgresMigrationMode::RuntimeReadiness
        );
        let mut topology = SelectedStorageTopology::postgres(
            registry,
            executor,
            namespace.session_execution,
            namespace.artifacts,
        )?;
        topology.test_namespace = Some(Arc::clone(namespace));
        topology.verify_runtime_readiness()
    }

    #[cfg(unix)]
    fn fixture_database_snapshot(executor: &PostgresExecutor) -> Vec<(String, String)> {
        let mut connection = executor.checkout_online_read().unwrap();
        let tables = connection.query(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename", &[],
        ).unwrap().iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>();
        let mut snapshot = Vec::new();
        for table in tables {
            let quoted = table.replace('"', "\"\"");
            let contents = connection.query_one(&format!(
                "SELECT COALESCE(jsonb_agg(to_jsonb(snapshot_row) ORDER BY to_jsonb(snapshot_row)::TEXT),'[]'::JSONB)::TEXT FROM \"{quoted}\" snapshot_row"
            ), &[]).unwrap().get::<_, String>(0);
            snapshot.push((table, contents));
        }
        let catalogs = connection.query_one(
            "SELECT COALESCE(jsonb_agg(jsonb_build_array(oid::TEXT,relname,relkind::TEXT,relfilenode::TEXT) ORDER BY relname),'[]'::JSONB)::TEXT FROM pg_class WHERE relnamespace=current_schema()::regnamespace", &[],
        ).unwrap().get::<_, String>(0);
        snapshot.push(("pg_class".into(), catalogs));
        snapshot
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and existing runtime POSIX scratch"]
    fn artifact_consumption_releases_event_binding_but_never_bypasses_either_owner_gate() {
        for case in ["exclusive", "outer_arc", "inner_store", "event_owner"] {
            let home = crate::storage_cutover::tests::posix_scratch();
            let workspace = crate::storage_cutover::tests::posix_scratch();
            let topology =
                SelectedStorageTopology::compose_for_test(home.path(), workspace.path()).unwrap();
            let namespace = Arc::clone(topology.test_namespace.as_ref().unwrap());
            match case {
                "exclusive" => {
                    let mut store = topology.into_exclusive_artifact_store().unwrap();
                    assert!(store.reconcile_quiesced_orphans(false, 100).is_err());
                    assert_eq!(
                        store
                            .reconcile_quiesced_orphans(true, 100)
                            .unwrap()
                            .removed_objects,
                        0
                    );
                }
                "outer_arc" => {
                    let owner = Arc::clone(&topology.artifact_store);
                    assert!(topology.into_exclusive_artifact_store().is_err());
                    assert!(owner.stats().is_ok());
                }
                "inner_store" => {
                    let owner = (*topology.artifact_store).clone();
                    let mut store = topology.into_exclusive_artifact_store().unwrap();
                    assert!(store.reconcile_quiesced_orphans(true, 100).is_err());
                    drop(owner);
                    assert!(store.reconcile_quiesced_orphans(true, 100).is_ok());
                }
                "event_owner" => {
                    let events = Arc::clone(&topology.runtime_event_store);
                    assert!(topology.into_exclusive_artifact_store().is_err());
                    drop(events);
                }
                _ => unreachable!(),
            }
            assert!(namespace
                .executor
                .checkout_online_read()
                .unwrap()
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)",
                    &[&namespace.name],
                )
                .unwrap()
                .get::<_, bool>(0));
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and existing runtime POSIX scratch"]
    fn artifact_caller_uses_runtime_readiness_preserves_unknown_and_repeats_idempotently() {
        use crate::storage_cutover::{
            execute_quiesced_artifact_cleanup,
            tests::{evidence_json, posix_scratch, write_evidence},
        };
        use runtime::ArtifactMetadataRepository;

        let home = posix_scratch();
        let workspace = posix_scratch();
        let fixture =
            SelectedStorageTopology::compose_for_test(home.path(), workspace.path()).unwrap();
        let namespace = Arc::clone(fixture.test_namespace.as_ref().unwrap());
        let executor = fixture_runtime_executor(&fixture, home.path());
        let registry = fixture.registry.clone();
        let identity = executor.logical_identity().to_string();
        let repository =
            runtime_postgres::PostgresArtifactRepository::new(fixture.postgres_executor.clone())
                .unwrap();
        let write = |name: &str, body: &[u8]| {
            let path = workspace.path().join(name);
            fs::write(&path, body).unwrap();
            fixture
                .artifact_store
                .write_path_blocking(
                    runtime::ArtifactWriteDescriptor {
                        media_type: "application/octet-stream".into(),
                        visibility_scope: "session:caller".into(),
                        expected_bytes: None,
                        original_name: None,
                    },
                    &path,
                )
                .unwrap()
        };
        let orphan_body =
            vec![b'o'; fixture.artifact_store.config().compact_threshold_bytes as usize + 1];
        let orphan = write("orphan", &orphan_body);
        fixture
            .artifact_store
            .delete(&orphan, "session:caller")
            .unwrap();
        fixture
            .postgres_executor
            .checkout_critical()
            .unwrap()
            .execute(
                "UPDATE artifact_objects SET created_at_ms=0 WHERE sha256=$1",
                &[&orphan.sha256],
            )
            .unwrap();
        let audit = write("audit", b"permanent audit pin");
        fixture
            .artifact_store
            .pin(&audit, "prepared-unknown-owner", i64::MAX as u64)
            .unwrap();
        assert!(fixture
            .artifact_store
            .delete(&audit, "session:caller")
            .is_err());
        let live = write("live", b"live unpinned record must remain");
        let young = write("young", b"within original orphan grace");
        fixture
            .artifact_store
            .delete(&young, "session:caller")
            .unwrap();
        fixture
            .runtime_event_store
            .append(runtime::RuntimeEventInput {
                stream_id: "session:caller".into(),
                scope: runtime::RuntimeEventScope::Session,
                kind: "maintenance.fixture.audit".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::to_value(&audit).unwrap(),
            })
            .unwrap();
        let events_before = fixture
            .runtime_event_store
            .list_stream("session:caller")
            .unwrap();
        let unknown = runtime::ArtifactObjectRecord {
            sha256: format!("sha256:{}", "f".repeat(64)),
            bytes: 4,
            tier: runtime::ArtifactObjectTier::Compact,
            compact_body: Some(b"????".to_vec()),
            created_at_ms: 0,
            generation: "unknown-reserved-generation".into(),
        };
        fixture.postgres_executor.checkout_critical().unwrap().execute(
            "INSERT INTO artifact_objects(sha256,generation,bytes,tier,compact_body,created_at_ms,inflight) VALUES($1,$2,$3,'compact',$4,0,1)",
            &[&unknown.sha256,&unknown.generation,&(unknown.bytes as i64),&unknown.compact_body],
        ).unwrap();
        let pins_before = fixture.artifact_store.stats().unwrap().pins;
        drop(fixture);
        let before_readiness = fixture_database_snapshot(&executor);
        let evidence_path = workspace.path().join("evidence");
        write_evidence(
            &evidence_path,
            &serde_json::to_vec(&evidence_json(
                workspace.path(),
                &identity,
                std::time::SystemTime::now(),
            ))
            .unwrap(),
        );
        let pid_path = home.path().join("absent-pid");
        let run = || {
            execute_quiesced_artifact_cleanup(
                &identity,
                workspace.path(),
                &evidence_path,
                &pid_path,
                || {
                    let topology =
                        fixture_runtime_topology(registry.clone(), executor.clone(), &namespace)?;
                    assert_eq!(fixture_database_snapshot(&executor), before_readiness);
                    assert_eq!(executor.health().migration_transaction_count, 0);
                    Ok(topology)
                },
            )
        };
        let report = run().unwrap();
        assert_eq!(report["removed_objects"], 1);
        assert_eq!(report["reclaimed_bytes"], orphan_body.len() as u64);
        assert_eq!(report["full_reconciliation_complete"], false);
        assert_eq!(
            report["canonical_transaction_reconciliation"],
            "closed_typed_exact_roots"
        );
        assert_eq!(
            report["unknown_reservations_count"],
            serde_json::Value::Null
        );
        assert_eq!(report["unknown_pins_count"], serde_json::Value::Null);
        assert_eq!(report["remaining_candidates"], "unknown");
        assert!(report["reclaimed_bytes_basis"]
            .as_str()
            .unwrap()
            .contains("not_measured_disk"));
        assert!(repository.object(&orphan.sha256).unwrap().is_none());
        assert_eq!(repository.object(&unknown.sha256).unwrap(), Some(unknown));
        assert!(repository.object(&young.sha256).unwrap().is_some());
        let topology =
            fixture_runtime_topology(registry.clone(), executor.clone(), &namespace).unwrap();
        assert_eq!(
            topology
                .artifact_store
                .read_blocking(&audit, "session:caller", None)
                .unwrap(),
            b"permanent audit pin"
        );
        assert_eq!(
            topology
                .artifact_store
                .read_blocking(&live, "session:caller", None)
                .unwrap(),
            b"live unpinned record must remain"
        );
        assert_eq!(topology.artifact_store.stats().unwrap().pins, pins_before);
        assert_eq!(
            topology
                .runtime_event_store
                .list_stream("session:caller")
                .unwrap(),
            events_before
        );
        drop(topology);
        let after_cleanup = fixture_database_snapshot(&executor);
        let second = execute_quiesced_artifact_cleanup(
            &identity,
            workspace.path(),
            &evidence_path,
            &pid_path,
            || {
                let topology =
                    fixture_runtime_topology(registry.clone(), executor.clone(), &namespace)?;
                assert_eq!(fixture_database_snapshot(&executor), after_cleanup);
                Ok(topology)
            },
        )
        .unwrap();
        assert_eq!(second["removed_objects"], 0);
        assert_eq!(second["full_reconciliation_complete"], false);
        assert_eq!(executor.health().migration_transaction_count, 0);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and existing runtime POSIX scratch"]
    fn runtime_artifact_composition_rejects_bad_catalogs_without_repair_or_ddl() {
        use crate::storage_cutover::tests::posix_scratch;

        for damage in [
            "checksum",
            "missing_catalog",
            "extra_migration",
            "missing_table",
        ] {
            let home = posix_scratch();
            let workspace = posix_scratch();
            let fixture =
                SelectedStorageTopology::compose_for_test(home.path(), workspace.path()).unwrap();
            let namespace = Arc::clone(fixture.test_namespace.as_ref().unwrap());
            let executor = fixture_runtime_executor(&fixture, home.path());
            let registry = fixture.registry.clone();
            let sql = match damage {
                "checksum" => "UPDATE cowd_schema_catalogs SET checksum='invalid' WHERE domain='runtime_artifact'",
                "missing_catalog" => "DELETE FROM cowd_schema_catalogs WHERE domain='runtime_artifact'",
                "extra_migration" => "INSERT INTO cowd_schema_migrations(id,domain,version,description,checksum) SELECT 'runtime_artifact.extra',domain,version,description,checksum FROM cowd_schema_migrations WHERE domain='runtime_artifact'",
                "missing_table" => "DROP TABLE artifact_pins",
                _ => unreachable!(),
            };
            fixture
                .postgres_executor
                .checkout_critical()
                .unwrap()
                .batch_execute(sql)
                .unwrap();
            drop(fixture);
            let before = fixture_database_snapshot(&executor);
            assert!(
                fixture_runtime_topology(registry, executor.clone(), &namespace).is_err(),
                "{damage}"
            );
            assert_eq!(fixture_database_snapshot(&executor), before, "{damage}");
            assert_eq!(executor.health().migration_transaction_count, 0);
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and existing runtime POSIX scratch"]
    fn artifact_caller_revalidates_identity_local_pid_freshness_and_ownership_after_compose() {
        use crate::storage_cutover::{
            execute_quiesced_artifact_cleanup,
            tests::{evidence_json, posix_scratch, write_evidence},
        };
        use std::time::{Duration, SystemTime};

        let home = posix_scratch();
        let workspace = posix_scratch();
        let fixture =
            SelectedStorageTopology::compose_for_test(home.path(), workspace.path()).unwrap();
        let namespace = Arc::clone(fixture.test_namespace.as_ref().unwrap());
        let executor = fixture_runtime_executor(&fixture, home.path());
        let registry = fixture.registry.clone();
        let identity = executor.logical_identity().to_string();
        drop(fixture);
        for case in [
            "identity",
            "local_pid",
            "expired",
            "outer_owner",
            "inner_owner",
            "readiness_error",
        ] {
            let evidence_path = workspace.path().join(format!("evidence-{case}"));
            let pid_path = home.path().join(format!("pid-{case}"));
            let declared_identity = if case == "identity" {
                "different-configured-label"
            } else {
                &identity
            };
            let mut declaration =
                evidence_json(workspace.path(), declared_identity, SystemTime::now());
            let topology =
                fixture_runtime_topology(registry.clone(), executor.clone(), &namespace).unwrap();
            let expiry = SystemTime::now() + Duration::from_secs(2);
            if case == "expired" {
                declaration["expires_at_unix_ms"] = serde_json::json!(expiry
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    as u64);
            }
            write_evidence(&evidence_path, &serde_json::to_vec(&declaration).unwrap());
            let before = fixture_database_snapshot(&executor);
            let mut outer_owner = None;
            let mut inner_owner = None;
            let mut composed = false;
            let result = execute_quiesced_artifact_cleanup(
                declared_identity,
                workspace.path(),
                &evidence_path,
                &pid_path,
                || {
                    composed = true;
                    match case {
                        "local_pid" => {
                            fs::write(&pid_path, std::process::id().to_string()).unwrap()
                        }
                        "expired" => std::thread::sleep(
                            expiry.duration_since(SystemTime::now()).unwrap_or_default()
                                + Duration::from_millis(10),
                        ),
                        "outer_owner" => outer_owner = Some(Arc::clone(&topology.artifact_store)),
                        "inner_owner" => inner_owner = Some((*topology.artifact_store).clone()),
                        "readiness_error" => return Err("fixture readiness rejected".into()),
                        _ => {}
                    }
                    Ok(topology)
                },
            );
            assert!(
                composed,
                "{case}: gate must have been reached after initial admission"
            );
            assert!(result.is_err(), "{case}");
            assert_eq!(fixture_database_snapshot(&executor), before, "{case}");
            assert_eq!(executor.health().migration_transaction_count, 0);
            drop(outer_owner);
            drop(inner_owner);
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and existing runtime POSIX scratch"]
    fn artifact_caller_keeps_fixed_batch_limit_and_never_reports_full_reconciliation() {
        use crate::storage_cutover::{
            execute_quiesced_artifact_cleanup,
            tests::{evidence_json, posix_scratch, write_evidence},
        };
        use runtime::ArtifactMetadataRepository;

        let home = posix_scratch();
        let workspace = posix_scratch();
        let fixture =
            SelectedStorageTopology::compose_for_test(home.path(), workspace.path()).unwrap();
        let namespace = Arc::clone(fixture.test_namespace.as_ref().unwrap());
        let executor = fixture_runtime_executor(&fixture, home.path());
        let registry = fixture.registry.clone();
        let identity = executor.logical_identity().to_string();
        let repository =
            runtime_postgres::PostgresArtifactRepository::new(fixture.postgres_executor.clone())
                .unwrap();
        let mut hashes = Vec::new();
        for index in 0..101 {
            let path = workspace.path().join("source");
            fs::write(&path, format!("batch-only-ready-object-{index}")).unwrap();
            let artifact = fixture
                .artifact_store
                .write_path_blocking(
                    runtime::ArtifactWriteDescriptor {
                        media_type: "text/plain".into(),
                        visibility_scope: "session:batch".into(),
                        expected_bytes: None,
                        original_name: None,
                    },
                    &path,
                )
                .unwrap();
            fixture
                .artifact_store
                .delete(&artifact, "session:batch")
                .unwrap();
            fixture
                .postgres_executor
                .checkout_critical()
                .unwrap()
                .execute(
                    "UPDATE artifact_objects SET created_at_ms=0 WHERE sha256=$1",
                    &[&artifact.sha256],
                )
                .unwrap();
            hashes.push(artifact.sha256);
        }
        drop(fixture);
        let evidence = workspace.path().join("evidence");
        write_evidence(
            &evidence,
            &serde_json::to_vec(&evidence_json(
                workspace.path(),
                &identity,
                std::time::SystemTime::now(),
            ))
            .unwrap(),
        );
        for expected in [100, 1, 0] {
            let report = execute_quiesced_artifact_cleanup(
                &identity,
                workspace.path(),
                &evidence,
                &home.path().join("absent-pid"),
                || fixture_runtime_topology(registry.clone(), executor.clone(), &namespace),
            )
            .unwrap();
            assert_eq!(report["removed_objects"], expected);
            assert!(report["examined"].as_u64().unwrap() <= 100);
            assert_eq!(report["full_reconciliation_complete"], false);
            assert_eq!(report["remaining_candidates"], "unknown");
        }
        for hash in hashes {
            assert!(repository.object(&hash).unwrap().is_none());
        }
        assert_eq!(executor.health().migration_transaction_count, 0);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL and existing runtime POSIX scratch"]
    fn artifact_caller_retries_seeded_generation_states_and_reports_io_failure_without_rollback() {
        use crate::storage_cutover::{
            execute_quiesced_artifact_cleanup,
            tests::{evidence_json, posix_scratch, write_evidence},
        };
        use runtime::ArtifactMetadataRepository;

        for case in ["unlink_before_finish", "io_failure"] {
            let home = posix_scratch();
            let workspace = posix_scratch();
            let fixture =
                SelectedStorageTopology::compose_for_test(home.path(), workspace.path()).unwrap();
            let namespace = Arc::clone(fixture.test_namespace.as_ref().unwrap());
            let executor = fixture_runtime_executor(&fixture, home.path());
            let registry = fixture.registry.clone();
            let identity = executor.logical_identity().to_string();
            let repository = runtime_postgres::PostgresArtifactRepository::new(
                fixture.postgres_executor.clone(),
            )
            .unwrap();
            let body =
                vec![b'r'; fixture.artifact_store.config().compact_threshold_bytes as usize + 1];
            let source = workspace.path().join("source");
            fs::write(&source, &body).unwrap();
            let artifact = fixture
                .artifact_store
                .write_path_blocking(
                    runtime::ArtifactWriteDescriptor {
                        media_type: "application/octet-stream".into(),
                        visibility_scope: "session:retry".into(),
                        expected_bytes: None,
                        original_name: None,
                    },
                    &source,
                )
                .unwrap();
            fixture
                .artifact_store
                .delete(&artifact, "session:retry")
                .unwrap();
            fixture
                .postgres_executor
                .checkout_critical()
                .unwrap()
                .execute(
                    "UPDATE artifact_objects SET created_at_ms=0 WHERE sha256=$1",
                    &[&artifact.sha256],
                )
                .unwrap();
            let object = repository.object(&artifact.sha256).unwrap().unwrap();
            let digest = artifact.sha256.strip_prefix("sha256:").unwrap();
            let blob_path = registry
                .endpoint(&StorageDomainId::Blobs)
                .unwrap()
                .path
                .join("objects")
                .join(&digest[..2])
                .join(digest)
                .join(&object.generation);
            assert!(blob_path.is_file());
            if case == "unlink_before_finish" {
                assert!(repository
                    .claim_object_deletion(&object.sha256, &object.generation)
                    .unwrap());
                fs::remove_file(&blob_path).unwrap();
            } else {
                fs::remove_file(&blob_path).unwrap();
                fs::create_dir(&blob_path).unwrap();
            }
            drop(fixture);
            let evidence = workspace.path().join("evidence");
            write_evidence(
                &evidence,
                &serde_json::to_vec(&evidence_json(
                    workspace.path(),
                    &identity,
                    std::time::SystemTime::now(),
                ))
                .unwrap(),
            );
            let run = || {
                execute_quiesced_artifact_cleanup(
                    &identity,
                    workspace.path(),
                    &evidence,
                    &home.path().join("absent-pid"),
                    || fixture_runtime_topology(registry.clone(), executor.clone(), &namespace),
                )
            };
            if case == "io_failure" {
                let error = run().unwrap_err();
                assert!(error.contains("partial FS/PG deletion may have occurred"));
                assert!(error.contains("no global rollback"));
                assert!(repository.object(&object.sha256).unwrap().is_some());
                fs::remove_dir(&blob_path).unwrap();
                fs::write(&blob_path, &body).unwrap();
            }
            assert_eq!(run().unwrap()["removed_objects"], 1);
            assert!(!blob_path.exists());
            assert!(repository.object(&object.sha256).unwrap().is_none());
            let mut replacement = object.clone();
            replacement.generation = "replacement-unknown-reservation".into();
            executor.checkout_critical().unwrap().execute(
                "INSERT INTO artifact_objects(sha256,generation,bytes,tier,created_at_ms,inflight) VALUES($1,$2,$3,'blob',0,1)",
                &[&replacement.sha256,&replacement.generation,&(replacement.bytes as i64)],
            ).unwrap();
            assert!(!repository
                .claim_object_deletion(&object.sha256, &object.generation)
                .unwrap());
            repository
                .finish_object_deletion(&object.sha256, &object.generation)
                .unwrap();
            assert_eq!(run().unwrap()["removed_objects"], 0);
            assert_eq!(
                repository.object(&object.sha256).unwrap(),
                Some(replacement)
            );
            assert_eq!(executor.health().migration_transaction_count, 0);
        }
    }

    #[test]
    fn postgres_session_workers_are_bounded_by_the_online_read_lane() {
        assert_eq!(postgres_session_workers(32, 16), 16);
        assert_eq!(postgres_session_workers(2, 16), 2);
        assert_eq!(postgres_session_workers(8, 4), 4);
    }

    #[test]
    #[ignore = "requires isolated COWD_TEST_POSTGRES_URL and explicit COWD_TEST_POOL_CONNECTIONS=4"]
    fn fixture_pool_budget_reuses_scopes_with_real_connections() {
        use std::time::{Duration, Instant};

        let budget = storage::test_fixture_pool_budget(3, Some(24)).expect("checked fixture budget");
        assert_eq!(
            budget, 4,
            "runner must explicitly allocate four connections"
        );
        let barrier = match (
            std::env::var_os("COWD_TEST_PG_READY_FILE"),
            std::env::var_os("COWD_TEST_PG_RELEASE_FILE"),
        ) {
            (None, None) => None,
            (Some(ready), Some(release)) => {
                let ready = PathBuf::from(ready);
                let release = PathBuf::from(release);
                assert_ne!(
                    ready, release,
                    "ready and release must be separate own files"
                );
                assert!(
                    !release.try_exists().expect("check release file"),
                    "release file must start absent"
                );
                Some((ready, release))
            }
            _ => panic!("set both COWD_TEST_PG_READY_FILE and COWD_TEST_PG_RELEASE_FILE"),
        };
        let fail_after_ready = match std::env::var("COWD_TEST_PG_FAIL_AFTER_READY") {
            Err(std::env::VarError::NotPresent) => false,
            Ok(value) if value == "0" => false,
            Ok(value) if value == "1" => true,
            _ => panic!("COWD_TEST_PG_FAIL_AFTER_READY must be 0 or 1 when set"),
        };
        // tempfile respects the parent's TMPDIR. No service or alternate fixture
        // creator is involved: only these two root topologies share the cache.
        let home = tempfile::tempdir().expect("config home");
        let workspace = tempfile::tempdir().expect("workspace");
        let first = SelectedStorageTopology::compose_for_test(home.path(), workspace.path())
            .expect("first scoped topology");
        let first_schema = first.test_namespace.as_ref().unwrap().name.clone();
        assert_eq!(first.postgres_executor.health().max_connections, budget);
        assert_eq!(
            first
                .postgres_executor
                .health()
                .lanes
                .iter()
                .map(|lane| lane.max_connections)
                .collect::<Vec<_>>(),
            vec![1, 2, 1]
        );
        let first_pid = {
            let mut client = first
                .postgres_executor
                .checkout_critical()
                .expect("first critical client");
            client
                .batch_execute("CREATE VIEW fixture_pool_scope_probe AS SELECT 1::INTEGER AS marker")
                .expect("first schema view");
            let row = client.query_one(
                "SELECT pg_backend_pid(), current_schema()::TEXT, marker FROM fixture_pool_scope_probe", &[],
            ).expect("first schema identity");
            assert_eq!(row.get::<_, String>(1), first_schema);
            assert_eq!(row.get::<_, i32>(2), 1);
            row.get::<_, i32>(0)
        }; // Return the only critical slot before composing the second scope.
        let second = SelectedStorageTopology::compose_for_test(home.path(), workspace.path())
            .expect("second scoped topology");
        let second_schema = second.test_namespace.as_ref().unwrap().name.clone();
        assert_ne!(first_schema, second_schema);
        assert_eq!(second.postgres_executor.health().max_connections, budget);
        {
            let mut client = second
                .postgres_executor
                .checkout_critical()
                .expect("second critical client");
            client
                .batch_execute("CREATE VIEW fixture_pool_scope_probe AS SELECT 2::INTEGER AS marker")
                .expect("second schema view");
            let row = client.query_one(
                "SELECT pg_backend_pid(), current_schema()::TEXT, marker FROM fixture_pool_scope_probe", &[],
            ).expect("second schema identity");
            assert_eq!(
                row.get::<_, i32>(0),
                first_pid,
                "one critical slot must reuse its backend across scopes"
            );
            assert_eq!(row.get::<_, String>(1), second_schema);
            assert_eq!(row.get::<_, i32>(2), 2);
        }
        let executor = &first.postgres_executor;
        // Acquire exactly the original three-lane allocation from one executor.
        // All topology/migration work above has finished before these are held.
        let mut clients = [
            executor.checkout_critical().expect("hold critical slot"),
            executor
                .checkout_online_read()
                .expect("hold first online slot"),
            executor
                .checkout_online_read()
                .expect("hold second online slot"),
            executor
                .checkout_background()
                .expect("hold background slot"),
        ];
        let backend_pids = clients.iter_mut().map(|client| {
            client.query_one(
                "SELECT pg_backend_pid(), current_schema()::TEXT, marker FROM fixture_pool_scope_probe", &[],
            ).map(|row| {
                assert_eq!(row.get::<_, String>(1), first_schema);
                assert_eq!(row.get::<_, i32>(2), 1, "returning to first scope must restore its view");
                row.get::<_, i32>(0)
            }).expect("held backend identity")
        }).collect::<Vec<i32>>();
        assert_eq!(backend_pids[0], first_pid);
        assert_eq!(
            backend_pids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            4
        );
        assert_eq!(
            executor
                .health()
                .lanes
                .iter()
                .map(|lane| lane.active_connections)
                .sum::<u32>(),
            budget
        );
        let ready = serde_json::json!({
            "process_id": std::process::id(),
            "budget": budget,
            "backend_pids": backend_pids,
            "schemas": [first_schema, second_schema],
        });
        println!("{ready}");
        if let Some((ready_path, release_path)) = barrier {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(ready_path)
                .expect("create exact own ready file");
            file.write_all(&serde_json::to_vec(&ready).expect("serialize ready record"))
                .expect("write ready record");
            file.sync_all().expect("flush ready record");
            let deadline = Instant::now() + Duration::from_secs(10);
            while !release_path.try_exists().expect("check runner release") {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "runner release did not arrive within ten seconds"
                );
                std::thread::sleep(remaining.min(Duration::from_millis(10)));
            }
        }
        if fail_after_ready {
            // Real unwind oracle: clients drop before the topology namespace
            // guards, so cleanup can reacquire the single critical slot.
            panic!("requested PostgreSQL fixture failure after ready/release");
        }
        drop(clients);
        drop(second);
        drop(first);
        // The process cache retains its pool until process exit. The parent
        // checks server-side reaping; dropping these scopes is not that proof.
    }

    #[test]
    fn fixture_budget_preserves_three_lanes_and_total() {
        let config = runtime::PostgresTopologyConfig::default();
        assert_eq!(resolved_postgres_lane_sizes(4, &config), [1, 2, 1]);
        assert_eq!(resolved_postgres_lane_sizes(24, &config), [8, 12, 4]);
        for budget in 3..=96 {
            let lanes = resolved_postgres_lane_sizes(budget, &config);
            assert!(lanes.iter().all(|size| *size >= 1));
            assert_eq!(lanes.iter().sum::<u32>(), budget);
        }
    }

    #[test]
    fn fixture_cache_rejects_changed_url_role_and_budget_and_keeps_failure() {
        let config = runtime::PostgresTopologyConfig::default();
        let snapshot = |value| {
            storage::test_fixture_postgres_url_snapshot(
                "same-reference",
                &ResolvedPostgresUrl::new(value),
            )
            .0
        };
        let token = snapshot("postgres://role_a:secret@localhost/fixture");
        let key = (Ok(token), config.clone());
        let cache = std::sync::OnceLock::new();
        let attempts = std::cell::Cell::new(0);
        let read = |key: &TestPostgresExecutorKey| {
            cache
                .get_or_init(|| {
                    attempts.set(attempts.get() + 1);
                    TestPostgresExecutorCache {
                        key: key.clone(),
                        result: Err("original failed attempt".into()),
                    }
                })
                .get(key)
        };
        assert_eq!(read(&key).unwrap_err(), "original failed attempt");
        for url in [
            "postgres://role_a:secret@localhost/other",
            "postgres://role_b:secret@localhost/fixture",
            "postgres://role_a:changed@localhost/fixture",
        ] {
            let error = read(&(Ok(snapshot(url)), config.clone())).unwrap_err();
            assert!(error.contains("configuration changed"));
            assert!(!error.contains(url));
        }
        let mut changed = config.clone();
        changed.max_connections = 4;
        assert!(read(&(Ok(token), changed))
            .unwrap_err()
            .contains("configuration changed"));
        let mut changed = config.clone();
        changed.online_read.checkout_timeout_ms += 1;
        assert!(read(&(Ok(token), changed))
            .unwrap_err()
            .contains("configuration changed"));
        assert!(read(&(Err("URL unavailable".into()), config))
            .unwrap_err()
            .contains("configuration changed"));
        assert_eq!(read(&key).unwrap_err(), "original failed attempt");
        assert_eq!(attempts.get(), 1, "changed keys must not initialize a pool");
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
        for forbidden in [
            "session.sqlite",
            "memory.sqlite",
            "fact.sqlite",
            "matrix.sqlite",
        ] {
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
