//! Static product composition for all reviewed Cowd applications.
//!
//! The generated catalogue chooses which external APP bundles participate in
//! this binary. This crate owns only generic validation, provisioning and
//! registration; it contains no product-domain logic or APP-specific branch.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use cowd_app_host::{
    AppProductContext, AppRegistry, AppRegistryError, AppStorageLease, AppStorageLeases,
    StaticAppProduct, TuiAppSurfaceContribution,
};
use cowd_app_sdk::{
    AppDescriptor, AppStorageBackend, AppStorageProvision, AppStorageReadiness,
    AppStorageRequirement, AppStorageScope, CowdAppContext,
};
use thiserror::Error;

mod generated;

/// Every APP bundle statically linked into this build.
#[must_use]
pub fn compiled_products() -> Vec<StaticAppProduct> {
    generated::compiled_products()
}

/// Descriptors that the runtime startup policy admits for this process.
#[must_use]
pub fn enabled_descriptors(is_enabled: &dyn Fn(&str) -> bool) -> Vec<AppDescriptor> {
    compiled_products()
        .into_iter()
        .filter_map(|product| {
            let descriptor = product.descriptor();
            is_enabled(descriptor.id.as_str()).then_some(descriptor)
        })
        .collect()
}

/// Host-selected relational deployment. PostgreSQL carries an already
/// resolved, bounded executor; neither this value nor APP metadata contains a
/// connection URL.
#[derive(Clone)]
pub enum AppStorageTopology {
    Sqlite,
    #[cfg(feature = "app-postgres")]
    Postgres {
        executor: storage::PostgresExecutor,
    },
}

impl AppStorageTopology {
    fn backend(&self) -> storage::StorageBackendKind {
        match self {
            Self::Sqlite => storage::StorageBackendKind::Sqlite,
            #[cfg(feature = "app-postgres")]
            Self::Postgres { .. } => storage::StorageBackendKind::Postgres,
        }
    }
}

/// Result of provisioning the enabled catalogue. The endpoint registry is
/// retained for topology/cutover composition; per-APP leases are attached to
/// the `AppRegistry` only after the APP contribution registers successfully.
pub struct AppStorageProvisioning {
    pub registry: storage::StorageRegistry,
    leases: BTreeMap<String, AppStorageLeases>,
}

impl AppStorageProvisioning {
    fn leases_for(&self, app_id: &str) -> Option<AppStorageLeases> {
        self.leases.get(app_id).cloned()
    }
}

fn resolve_backend(
    requested: &AppStorageBackend,
    selected: &storage::StorageBackendKind,
) -> Result<storage::StorageBackendKind, AppStorageResolutionError> {
    match requested {
        AppStorageBackend::Relational
            if matches!(
                selected,
                storage::StorageBackendKind::Sqlite | storage::StorageBackendKind::Postgres
            ) =>
        {
            Ok(selected.clone())
        }
        AppStorageBackend::Sqlite if *selected == storage::StorageBackendKind::Sqlite => {
            Ok(storage::StorageBackendKind::Sqlite)
        }
        AppStorageBackend::Postgres if *selected == storage::StorageBackendKind::Postgres => {
            Ok(storage::StorageBackendKind::Postgres)
        }
        AppStorageBackend::FileJson => Ok(storage::StorageBackendKind::FileJson),
        AppStorageBackend::Directory => Ok(storage::StorageBackendKind::Directory),
        AppStorageBackend::BlobDirectory => Ok(storage::StorageBackendKind::BlobDirectory),
        _ => Err(AppStorageResolutionError::BackendMismatch {
            requested: requested.clone(),
            selected: selected.clone(),
        }),
    }
}

fn resolved_scope(
    app_id: &str,
    requested: &AppStorageScope,
    registry: &storage::StorageRegistry,
) -> Result<storage::StorageScope, AppStorageResolutionError> {
    match requested {
        AppStorageScope::App => Ok(storage::StorageScope::App {
            app_id: app_id.to_string(),
        }),
        AppStorageScope::Workspace => {
            let scopes = registry
                .endpoints
                .iter()
                .filter_map(|endpoint| match &endpoint.scope {
                    storage::StorageScope::Workspace { .. } => Some(endpoint.scope.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            if scopes.len() != 1 {
                return Err(AppStorageResolutionError::WorkspaceScopeUnavailable {
                    app_id: app_id.to_string(),
                    observed: scopes.len(),
                });
            }
            Ok(scopes.into_iter().next().expect("one workspace scope"))
        }
    }
}

fn projection_backend(backend: &storage::StorageBackendKind) -> AppStorageBackend {
    match backend {
        storage::StorageBackendKind::Sqlite => AppStorageBackend::Sqlite,
        storage::StorageBackendKind::Postgres => AppStorageBackend::Postgres,
        storage::StorageBackendKind::FileJson => AppStorageBackend::FileJson,
        storage::StorageBackendKind::Directory => AppStorageBackend::Directory,
        storage::StorageBackendKind::BlobDirectory => AppStorageBackend::BlobDirectory,
    }
}

#[cfg(feature = "app-postgres")]
fn provision_postgres_namespace(
    executor: storage::PostgresExecutor,
    namespace: String,
) -> Result<(), AppStorageResolutionError> {
    // Startup may run inside Tokio. The synchronous postgres driver owns its
    // own runtime, so isolate this bounded readiness operation on a plain OS
    // thread instead of risking a nested-runtime panic.
    std::thread::spawn(move || {
        let mut connection = executor.checkout()?;
        connection.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{namespace}\";"))?;
        connection.query_one("SELECT 1", &[])?;
        Ok::<(), storage::StorageError>(())
    })
    .join()
    .map_err(|_| AppStorageResolutionError::PostgresProvisionPanicked)??;
    Ok(())
}

fn provision_requirement(
    registry: &mut storage::StorageRegistry,
    topology: &AppStorageTopology,
    app_id: &cowd_app_sdk::AppId,
    requirement: &AppStorageRequirement,
) -> Result<AppStorageLease, AppStorageResolutionError> {
    let backend = resolve_backend(&requirement.backend, &topology.backend())
        .map_err(|error| error.with_requirement(app_id.to_string(), requirement.domain.clone()))?;
    let scope = resolved_scope(app_id.as_str(), &requirement.scope, registry)?;
    registry.register_app_storage(
        app_id.as_str(),
        &requirement.domain,
        scope.clone(),
        backend.clone(),
        requirement.migration.clone(),
    )?;
    let endpoint = registry
        .endpoint_in_scope(
            &storage::StorageDomainId::app(app_id.as_str(), &requirement.domain),
            &scope,
        )?
        .clone();
    let namespace = if backend == storage::StorageBackendKind::Postgres {
        endpoint.app_postgres_namespace()?
    } else {
        endpoint.logical_id()
    };
    let mut capabilities = requirement.required_capabilities();
    if requirement.backend == AppStorageBackend::Relational {
        capabilities.push(match backend {
            storage::StorageBackendKind::Sqlite => "sqlite".to_string(),
            storage::StorageBackendKind::Postgres => "postgres".to_string(),
            _ => unreachable!("relational requirement resolved to a non-relational backend"),
        });
        if backend == storage::StorageBackendKind::Postgres {
            capabilities.push("connection_pool".to_string());
        }
    }
    capabilities.sort();
    capabilities.dedup();
    let provision = AppStorageProvision {
        domain: requirement.domain.clone(),
        scope: requirement.scope.clone(),
        backend: projection_backend(&backend),
        logical_id: endpoint.logical_id(),
        namespace,
        migration: requirement.migration.clone(),
        migration_owner: app_id.clone(),
        capabilities,
        readiness: AppStorageReadiness::Ready,
    };
    match (&backend, topology) {
        (storage::StorageBackendKind::Sqlite, _) => {
            let executor = storage::SqliteExecutor::for_endpoint(&endpoint)?;
            Ok(AppStorageLease::sqlite(endpoint, provision, executor))
        }
        #[cfg(feature = "app-postgres")]
        (storage::StorageBackendKind::Postgres, AppStorageTopology::Postgres { executor }) => {
            provision_postgres_namespace(executor.clone(), provision.namespace.clone())?;
            Ok(AppStorageLease::postgres(
                endpoint,
                provision,
                executor.clone(),
            ))
        }
        (storage::StorageBackendKind::Postgres, _) => {
            Err(AppStorageResolutionError::PostgresExecutorUnavailable {
                app_id: app_id.to_string(),
                domain: requirement.domain.clone(),
            })
        }
        _ => {
            let directory = match backend {
                storage::StorageBackendKind::Directory
                | storage::StorageBackendKind::BlobDirectory => endpoint.path.as_path(),
                storage::StorageBackendKind::FileJson => endpoint
                    .path
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new(".")),
                _ => unreachable!("relational backends handled above"),
            };
            std::fs::create_dir_all(directory)?;
            Ok(AppStorageLease::artifact(endpoint, provision))
        }
    }
}

fn provision_products(
    products: &[StaticAppProduct],
    mut registry: storage::StorageRegistry,
    topology: &AppStorageTopology,
) -> Result<AppStorageProvisioning, AppStorageResolutionError> {
    let mut leases = BTreeMap::new();
    for product in products {
        let app_id = product.app_id();
        let contract = product.storage_contract();
        contract.validate_for(&app_id)?;
        let app_leases = contract
            .requirements
            .iter()
            .map(|requirement| provision_requirement(&mut registry, topology, &app_id, requirement))
            .collect::<Result<Vec<_>, _>>()?;
        let app_leases = AppStorageLeases::new(app_id.clone(), app_leases)?;
        if leases.insert(app_id.to_string(), app_leases).is_some() {
            return Err(AppStorageResolutionError::DuplicateApp(app_id.to_string()));
        }
    }
    Ok(AppStorageProvisioning { registry, leases })
}

/// Validate, provision and mount all enabled products through one catalogue
/// slice. The same attached registry state drives route, TUI/WebUI and health
/// projections, so disabled products cannot leave a storage-only surface.
pub fn register_enabled_with_storage(
    app_registry: &mut AppRegistry,
    context: CowdAppContext,
    storage_registry: storage::StorageRegistry,
    topology: AppStorageTopology,
    is_enabled: &dyn Fn(&str) -> bool,
) -> Result<AppStorageProvisioning, ProductAppRegistrationError> {
    let products = compiled_products()
        .into_iter()
        .filter(|product| is_enabled(product.app_id().as_str()))
        .collect::<Vec<_>>();
    register_products_with_storage(products, app_registry, context, storage_registry, topology)
}

fn register_products_with_storage(
    products: Vec<StaticAppProduct>,
    app_registry: &mut AppRegistry,
    context: CowdAppContext,
    storage_registry: storage::StorageRegistry,
    topology: AppStorageTopology,
) -> Result<AppStorageProvisioning, ProductAppRegistrationError> {
    let provisioning = provision_products(&products, storage_registry, &topology)?;
    for product in products {
        let app_id = product.app_id();
        let leases = provisioning
            .leases_for(app_id.as_str())
            .ok_or_else(|| AppStorageResolutionError::IncompleteProvision(app_id.to_string()))?;
        product.register(
            app_registry,
            AppProductContext::new(context.clone(), leases.clone()),
        )?;
        app_registry.attach_product_contract(
            &app_id,
            product.source_lock(),
            product.storage_contract(),
            leases,
        )?;
    }
    Ok(provisioning)
}

/// Current production default. V581 replaces this SQLite selection with the
/// process-wide selected topology; keeping the selection here explicit avoids
/// a hidden fallback.
pub fn register_enabled(
    registry: &mut AppRegistry,
    config_home: &Path,
    context: CowdAppContext,
    is_enabled: &dyn Fn(&str) -> bool,
) -> Result<(), ProductAppRegistrationError> {
    register_enabled_with_storage(
        registry,
        context,
        storage::StorageRegistry::default_for_config_home(config_home),
        AppStorageTopology::Sqlite,
        is_enabled,
    )?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum ProductAppRegistrationError {
    #[error(transparent)]
    Resolution(#[from] AppStorageResolutionError),
    #[error(transparent)]
    Registry(#[from] AppRegistryError),
}

#[derive(Debug, Error)]
pub enum AppStorageResolutionError {
    #[error(transparent)]
    Contract(#[from] cowd_app_sdk::AppContractError),
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error(transparent)]
    Registry(#[from] AppRegistryError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(
        "storage backend mismatch for {app_id}:{domain}: requested {requested:?}, selected {selected:?}"
    )]
    BackendMismatchAtRequirement {
        app_id: String,
        domain: String,
        requested: AppStorageBackend,
        selected: storage::StorageBackendKind,
    },
    #[error("storage backend mismatch: requested {requested:?}, selected {selected:?}")]
    BackendMismatch {
        requested: AppStorageBackend,
        selected: storage::StorageBackendKind,
    },
    #[error("workspace scope unavailable for app {app_id}: observed {observed} workspace scopes")]
    WorkspaceScopeUnavailable { app_id: String, observed: usize },
    #[error("postgres executor unavailable for {app_id}:{domain}")]
    PostgresExecutorUnavailable { app_id: String, domain: String },
    #[error("postgres APP provisioning thread panicked")]
    PostgresProvisionPanicked,
    #[error("duplicate compiled application {0}")]
    DuplicateApp(String),
    #[error("storage provision missing for enabled application {0}")]
    IncompleteProvision(String),
}

impl AppStorageResolutionError {
    fn with_requirement(self, app_id: String, domain: String) -> Self {
        match self {
            Self::BackendMismatch {
                requested,
                selected,
            } => Self::BackendMismatchAtRequirement {
                app_id,
                domain,
                requested,
                selected,
            },
            other => other,
        }
    }
}

/// All application terminal surfaces included in this build. The TUI filters
/// this result by Gateway's enabled APP identifiers before displaying a panel.
#[must_use]
pub fn tui_surface_contributions() -> Vec<TuiAppSurfaceContribution> {
    compiled_products()
        .into_iter()
        .filter_map(StaticAppProduct::tui_surface)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use async_trait::async_trait;
    use cowd_app_host::{AppContribution, StaticAppSourceLock};
    use cowd_app_sdk::{
        AppHostError, AppHostPorts, AppId, AppStorageContract, CapabilityApp,
        CredentialLifecycleCheck, CredentialLifecycleError, CredentialLifecyclePort, HostIntent,
        HostReceipt, InvocationContext, SDK_API_VERSION,
    };

    struct FixtureApp;

    impl CapabilityApp for FixtureApp {
        fn descriptor(&self) -> AppDescriptor {
            fixture_descriptor()
        }

        fn health(&self) -> cowd_app_sdk::AppHealth {
            cowd_app_sdk::AppHealth::Ready
        }
    }

    fn fixture_descriptor() -> AppDescriptor {
        AppDescriptor {
            id: AppId::parse("fixture").expect("fixture id"),
            display_name: "Fixture".to_string(),
            sdk_api: SDK_API_VERSION,
            version: "1.0.0".to_string(),
            capabilities: vec![],
            routes: vec![],
            actions: vec![],
            profile: None,
        }
    }

    fn fixture_requirements() -> Vec<AppStorageRequirement> {
        vec![AppStorageRequirement {
            domain: "primary".to_string(),
            backend: AppStorageBackend::Relational,
            scope: AppStorageScope::App,
            migration: "fixture_primary_v1".to_string(),
        }]
    }

    fn fixture_register(
        registry: &mut AppRegistry,
        context: AppProductContext,
    ) -> Result<(), AppRegistryError> {
        let app_id = AppId::parse("fixture")?;
        let lease = context
            .storage()
            .get("primary", &AppStorageScope::App)
            .ok_or_else(|| AppRegistryError::IncompleteStorageProvision(app_id.clone()))?;
        let relational_executor_ready = lease.sqlite_executor().is_some() || {
            #[cfg(feature = "app-postgres")]
            {
                lease.postgres_executor().is_some()
            }
            #[cfg(not(feature = "app-postgres"))]
            {
                false
            }
        };
        if !relational_executor_ready {
            return Err(AppRegistryError::IncompleteStorageProvision(app_id));
        }
        registry.register(AppContribution {
            app: Box::new(FixtureApp),
            http: None,
            tui: None,
        })
    }

    struct FixturePorts;

    fn unavailable() -> Result<HostReceipt, AppHostError> {
        Err(AppHostError::Unavailable("fixture".to_string()))
    }

    #[async_trait]
    impl cowd_app_sdk::RuntimePort for FixturePorts {
        async fn execute(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    #[async_trait]
    impl cowd_app_sdk::ApprovalPort for FixturePorts {
        async fn request(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    #[async_trait]
    impl cowd_app_sdk::CrossPlanePort for FixturePorts {
        async fn submit(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    #[async_trait]
    impl cowd_app_sdk::ConnectorPort for FixturePorts {
        async fn dispatch(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    #[async_trait]
    impl cowd_app_sdk::RealityPort for FixturePorts {
        async fn query(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    #[async_trait]
    impl cowd_app_sdk::WorkContextPort for FixturePorts {
        async fn execute(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    #[async_trait]
    impl cowd_app_sdk::PlatformPort for FixturePorts {
        async fn query(
            &self,
            _context: &InvocationContext,
            _intent: HostIntent,
        ) -> Result<HostReceipt, AppHostError> {
            unavailable()
        }
    }

    impl CredentialLifecyclePort for FixturePorts {
        fn verify(&self, _check: CredentialLifecycleCheck) -> Result<(), CredentialLifecycleError> {
            Ok(())
        }
    }

    impl AppHostPorts for FixturePorts {
        fn runtime(&self) -> &dyn cowd_app_sdk::RuntimePort {
            self
        }
        fn approval(&self) -> &dyn cowd_app_sdk::ApprovalPort {
            self
        }
        fn cross_plane(&self) -> &dyn cowd_app_sdk::CrossPlanePort {
            self
        }
        fn connector(&self) -> &dyn cowd_app_sdk::ConnectorPort {
            self
        }
        fn reality(&self) -> &dyn cowd_app_sdk::RealityPort {
            self
        }
        fn work_context(&self) -> &dyn cowd_app_sdk::WorkContextPort {
            self
        }
        fn platform(&self) -> &dyn cowd_app_sdk::PlatformPort {
            self
        }
        fn credential_lifecycle(&self) -> &dyn CredentialLifecyclePort {
            self
        }
    }

    fn fixture_product() -> StaticAppProduct {
        StaticAppProduct::new_provisioned(
            fixture_descriptor,
            fixture_register,
            None,
            fixture_requirements,
        )
        .with_source_lock(StaticAppSourceLock::new(
            "https://example.invalid/fixture",
            "0123456789abcdef0123456789abcdef01234567",
        ))
    }

    #[test]
    fn compiled_products_have_valid_unique_descriptors_and_source_locks() {
        let mut ids = BTreeSet::new();
        for product in compiled_products() {
            let descriptor = product.descriptor();
            descriptor.validate().expect("static APP descriptor");
            assert!(ids.insert(descriptor.id.clone()));
            product
                .source_lock()
                .expect("generated product source lock")
                .validate(&descriptor.id)
                .expect("valid generated source lock");
        }
    }

    #[test]
    fn relational_fixture_provisions_sqlite_without_exposing_path() {
        let products = vec![fixture_product()];
        let provisioning = provision_products(
            &products,
            storage::StorageRegistry::default_for_config_home("/tmp/cowd-fixture-storage"),
            &AppStorageTopology::Sqlite,
        )
        .expect("generic SQLite provision");
        let leases = provisioning.leases_for("fixture").expect("fixture leases");
        let lease = leases
            .get("primary", &AppStorageScope::App)
            .expect("primary lease");
        assert!(lease.sqlite_executor().is_some());
        assert_eq!(lease.provision().backend, AppStorageBackend::Sqlite);
        let json = serde_json::to_string(lease.provision()).expect("projection JSON");
        assert!(!json.contains(".sqlite"));
        assert!(!json.contains("/tmp/"));
    }

    #[test]
    fn provisioned_fixture_registers_from_the_same_storage_and_source_contract() {
        let config_home =
            std::env::temp_dir().join(format!("cowd-v579-fixture-register-{}", std::process::id()));
        let mut registry = AppRegistry::default();
        let provisioning = register_products_with_storage(
            vec![fixture_product()],
            &mut registry,
            CowdAppContext::new(Arc::new(FixturePorts)),
            storage::StorageRegistry::default_for_config_home(&config_home),
            AppStorageTopology::Sqlite,
        )
        .expect("generic provisioned registration");
        let app = registry
            .apps()
            .into_iter()
            .find(|app| app.descriptor.id.as_str() == "fixture")
            .expect("registered fixture");
        assert!(app.source_lock.is_some());
        assert_eq!(
            app.storage.as_ref().expect("fixture storage").provisions[0].backend,
            AppStorageBackend::Sqlite
        );
        assert_eq!(registry.storage_endpoints().len(), 1);
        assert_eq!(
            provisioning
                .leases_for("fixture")
                .expect("fixture leases")
                .provisions(),
            app.storage.expect("fixture storage").provisions
        );
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[test]
    fn exact_backend_mismatch_is_fail_closed_and_relational_is_selectable() {
        assert!(matches!(
            resolve_backend(
                &AppStorageBackend::Sqlite,
                &storage::StorageBackendKind::Postgres,
            ),
            Err(AppStorageResolutionError::BackendMismatch { .. })
        ));
        assert_eq!(
            resolve_backend(
                &AppStorageBackend::Relational,
                &storage::StorageBackendKind::Postgres,
            )
            .expect("relational selects PG"),
            storage::StorageBackendKind::Postgres
        );
    }

    #[test]
    fn current_locked_mfg_contract_is_backend_neutral_and_postgres_selectable() {
        let mfg = compiled_products()
            .into_iter()
            .find(|product| product.app_id().as_str() == "mfg")
            .expect("compiled MFG product");
        let contract = mfg.storage_contract();
        assert!(contract
            .requirements
            .iter()
            .all(|requirement| requirement.backend == AppStorageBackend::Relational));
        assert!(contract.requirements.iter().all(|requirement| {
            matches!(
                resolve_backend(&requirement.backend, &storage::StorageBackendKind::Postgres,),
                Ok(storage::StorageBackendKind::Postgres)
            )
        }));
    }

    #[test]
    fn workspace_requirement_needs_exactly_one_host_scope() {
        let app_id = AppId::parse("fixture").expect("fixture id");
        let empty = storage::StorageRegistry::default_for_config_home("/tmp/cowd-empty-scope");
        assert!(matches!(
            resolved_scope("fixture", &AppStorageScope::Workspace, &empty),
            Err(AppStorageResolutionError::WorkspaceScopeUnavailable { observed: 0, .. })
        ));
        let workspace = tempfile::tempdir().expect("workspace");
        let scoped = empty
            .with_workspace(workspace.path())
            .expect("workspace endpoints");
        assert_eq!(
            resolved_scope(app_id.as_str(), &AppStorageScope::Workspace, &scoped)
                .expect("one scope"),
            storage::StorageScope::workspace_for_root(workspace.path())
        );
    }

    #[test]
    fn disabled_catalogue_has_no_storage_provision() {
        let products = compiled_products()
            .into_iter()
            .filter(|product| product.app_id().as_str() == "never-enabled")
            .collect::<Vec<_>>();
        let provisioning = provision_products(
            &products,
            storage::StorageRegistry::default_for_config_home("/tmp/cowd-disabled-storage"),
            &AppStorageTopology::Sqlite,
        )
        .expect("empty catalogue");
        assert!(provisioning.leases.is_empty());
        assert!(provisioning
            .registry
            .endpoints
            .iter()
            .all(|endpoint| !matches!(endpoint.domain, storage::StorageDomainId::App { .. })));
    }

    #[test]
    fn malformed_duplicate_contract_is_rejected_before_provision() {
        let app_id = AppId::parse("fixture").expect("fixture id");
        let requirement = fixture_requirements().remove(0);
        let contract =
            AppStorageContract::new(app_id.clone(), vec![requirement.clone(), requirement]);
        assert!(contract.validate_for(&app_id).is_err());
    }

    #[cfg(feature = "app-postgres")]
    #[test]
    fn real_postgres_provision_when_configured() {
        let Ok(url) = std::env::var("COWD_TEST_POSTGRES_URL") else {
            eprintln!("COWD_TEST_POSTGRES_URL is absent; real PostgreSQL case skipped");
            return;
        };
        let resolver = storage::StaticSecretRefResolver::new([("v579-test".to_string(), url)]);
        let executor = storage::PostgresExecutor::connect(
            storage::PostgresConnectionConfig::new(
                "v579-app-provision",
                "v579-test",
                "cowd-v579-test",
            ),
            &resolver,
        )
        .expect("connect real PostgreSQL");
        let products = vec![fixture_product()];
        let provisioning = provision_products(
            &products,
            storage::StorageRegistry::default_for_config_home("/tmp/cowd-v579-pg"),
            &AppStorageTopology::Postgres {
                executor: executor.clone(),
            },
        )
        .expect("real PG provision");
        let leases = provisioning
            .leases_for("fixture")
            .expect("fixture PG leases");
        let lease = leases
            .get("primary", &AppStorageScope::App)
            .expect("primary PG lease");
        assert!(lease.postgres_executor().is_some());
        assert_eq!(lease.provision().backend, AppStorageBackend::Postgres);
        let namespace = lease.provision().namespace.clone();
        let workers = (0..8)
            .map(|_| {
                let executor = executor.clone();
                std::thread::spawn(move || {
                    let mut connection = executor.checkout().expect("worker checkout");
                    connection.query_one("SELECT 1", &[]).expect("worker query");
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("PG worker");
        }
        let cleanup = executor.clone();
        std::thread::spawn(move || {
            let mut connection = cleanup.checkout().expect("cleanup checkout");
            connection
                .batch_execute(&format!("DROP SCHEMA IF EXISTS \"{namespace}\" CASCADE"))
                .expect("drop fixture namespace");
        })
        .join()
        .expect("cleanup thread");
        assert!(executor.health().metrics.checkout_count >= 10);
    }
}
