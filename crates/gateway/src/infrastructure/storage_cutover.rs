//! Offline SQLite -> PostgreSQL cutover coordinator.
//!
//! Migration owns a maintenance barrier, copies each domain exactly once and
//! records canonical source/target evidence. Verification seals that complete
//! evidence set; cutover atomically publishes the only manifest accepted by
//! normal PostgreSQL startup. There is no dual-write or automatic fallback.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::selected_storage::{
    clear_fallback_marker, read_fallback_marker, SelectedStorageTopology,
};

const MANIFEST_VERSION: u32 = 1;
const REQUIRED_CORE_DOMAINS: &[&str] = &[
    "fact_ledger",
    "matrix",
    "session",
    "runtime_event",
    "runtime_task",
    "memory",
    "knowledge",
    "surface_message",
    "connector_directory",
    "apps",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CutoverStatus {
    Migrated,
    Verified,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CutoverManifest {
    manifest_version: u32,
    cowd_version: String,
    workspace_key: String,
    target_backend: String,
    target_logical_identity: String,
    target_secret_ref: String,
    status: CutoverStatus,
    product_sources: BTreeMap<String, cowd_app_sdk::AppSourceLock>,
    enabled_apps: Vec<String>,
    domains: BTreeMap<String, serde_json::Value>,
    digest: String,
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let command = match args {
        [command] => command.as_str(),
        [first, second] if first == "cleanup" && second == "--sqlite-residuals" => {
            "cleanup-sqlite-residuals"
        }
        _ => {
            return Err(
                "usage: cowd storage plan | upgrade | migrate | verify | cutover | adopt-postgres | fallback-status | cleanup [--sqlite-residuals]"
                    .to_string(),
            )
        }
    };
    let context = CutoverContext::load()?;
    match command {
        "plan" => context.plan(),
        "upgrade" => context.upgrade(),
        "migrate" => context.migrate(),
        "verify" => context.verify(),
        "cutover" => context.cutover(),
        "adopt-postgres" => context.adopt_postgres(),
        "fallback-status" => context.fallback_status(),
        "cleanup" => context.cleanup(),
        "cleanup-sqlite-residuals" => context.cleanup_sqlite_residuals(),
        _ => Err(
            "usage: cowd storage plan | upgrade | migrate | verify | cutover | adopt-postgres | fallback-status | cleanup [--sqlite-residuals]"
                .to_string(),
        ),
    }
}

pub(crate) fn validate_active_manifest(
    config_home: &Path,
    workspace_root: &Path,
    storage: &runtime::StorageTopologyConfig,
    apps: &runtime::AppsConfig,
) -> Result<(), String> {
    let path = active_path(config_home);
    if !path.is_file() {
        return Err(format!(
            "PostgreSQL storage has no verified activation manifest at {}; run `cowd storage migrate`, `verify`, then `cutover` while Gateway is stopped",
            path.display()
        ));
    }
    let manifest = read_manifest(&path)?;
    let postgres = postgres_target(storage)?;
    validate_manifest(
        &manifest,
        workspace_root,
        CutoverStatus::Active,
        postgres,
        &enabled_apps(apps),
        false,
    )
}

struct CutoverContext {
    config_home: PathBuf,
    workspace_root: PathBuf,
    runtime_config: runtime::RuntimeConfig,
}

impl CutoverContext {
    fn load() -> Result<Self, String> {
        let config_home = runtime::cowd_dirs::config_home_dir();
        let workspace_root = std::env::current_dir().map_err(stringify)?;
        let loaded = runtime::ConfigLoader::new(&workspace_root, &config_home)
            .load_with_diagnostics()
            .map_err(|error| format!("failed to load runtime configuration: {error}"))?;
        Ok(Self {
            config_home,
            workspace_root,
            runtime_config: loaded.config,
        })
    }

    fn require_postgres_target(&self) -> Result<&runtime::PostgresTopologyConfig, String> {
        postgres_target(self.runtime_config.storage())
    }

    fn plan(&self) -> Result<(), String> {
        let postgres = self.require_postgres_target()?;
        let source_registry = storage::StorageRegistry::default_for_config_home(&self.config_home)
            .with_workspace(&self.workspace_root)
            .and_then(storage::StorageRegistry::with_surface_messages)
            .map_err(stringify)?;
        let output = serde_json::json!({
            "operation": "sqlite_to_postgres_cutover",
            "gateway_must_be_stopped": true,
            "source_backend": "sqlite",
            "target": {
                "backend": "postgres",
                "logical_identity": postgres.logical_identity,
                "secret_ref": postgres.secret_ref,
                "max_connections": postgres.max_connections,
                "server_reserve": postgres.server_reserve,
                "lanes": {
                    "critical": postgres.critical,
                    "online_read": postgres.online_read,
                    "background": postgres.background,
                },
            },
            "workspace_key": workspace_key(&self.workspace_root),
            "source_endpoints": source_registry.inventory(),
            "domains": REQUIRED_CORE_DOMAINS,
            "product_sources": product_sources()?,
            "enabled_apps": enabled_apps(self.runtime_config.apps()),
            "commands": ["migrate", "verify", "cutover"],
            "fallback": "none",
            "dual_write": false,
        });
        print_json(&output)
    }

    /// Apply the schema catalog owned by the current binary without copying
    /// data or changing the selected backend. This is the only normal upgrade
    /// path after PostgreSQL has already become authoritative.
    fn upgrade(&self) -> Result<(), String> {
        self.require_postgres_target()?;
        ensure_gateway_stopped()?;
        let _guard = MaintenanceGuard::acquire(&self.config_home)?;
        let target = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        let mut registry = cowd_app_host::AppRegistry::default();
        cowd_product_apps::register_enabled_with_storage(
            &mut registry,
            crate::services::GatewayAppHostBinding::new().context(),
            target.registry,
            target.app_topology,
            &|app_id| self.runtime_config.apps().is_enabled(app_id),
        )
        .map_err(stringify)?;
        print_json(&serde_json::json!({
            "operation": "postgres_schema_upgrade",
            "backend": "postgres",
            "gateway_stopped": true,
            "cowd_version": env!("CARGO_PKG_VERSION"),
            "enabled_apps": enabled_apps(self.runtime_config.apps()),
            "status": "completed",
        }))
    }

    /// Explicitly re-adopt PostgreSQL after an `auto` fallback to SQLite.
    /// Requires the Gateway to be stopped; verifies PostgreSQL readiness and
    /// clears the fallback marker so the next cold start uses PostgreSQL.
    fn adopt_postgres(&self) -> Result<(), String> {
        let postgres = self.require_postgres_target()?;
        ensure_gateway_stopped()?;
        let _guard = MaintenanceGuard::acquire(&self.config_home)?;
        let target = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        if target.postgres_executor.is_none() {
            return Err(
                "storage adopt-postgres failed: resolved topology is not PostgreSQL".to_string(),
            );
        }
        clear_fallback_marker(&self.config_home);
        print_json(&serde_json::json!({
            "operation": "postgres_adopt",
            "backend": "postgres",
            "logical_identity": postgres.logical_identity,
            "gateway_stopped": true,
            "status": "completed",
            "fallback_marker": "cleared",
        }))
    }

    /// Report the configured backend, the effective backend (from a fallback
    /// marker written at cold start), and the fallback reason when active.
    fn fallback_status(&self) -> Result<(), String> {
        let configured = self.runtime_config.storage().backend;
        let marker = read_fallback_marker(&self.config_home);
        let effective = if marker.is_some() {
            "sqlite"
        } else {
            match configured {
                runtime::StorageBackendSelection::Sqlite => "sqlite",
                runtime::StorageBackendSelection::Postgres => "postgres",
                runtime::StorageBackendSelection::Auto => "postgres",
            }
        };
        print_json(&serde_json::json!({
            "configured_backend": match configured {
                runtime::StorageBackendSelection::Sqlite => "sqlite",
                runtime::StorageBackendSelection::Postgres => "postgres",
                runtime::StorageBackendSelection::Auto => "auto",
            },
            "preferred_backend": match self.runtime_config.storage().preferred {
                runtime::StorageBackendSelection::Sqlite => "sqlite",
                runtime::StorageBackendSelection::Postgres => "postgres",
                runtime::StorageBackendSelection::Auto => "auto",
            },
            "effective_backend": effective,
            "fallback_reason": marker.as_ref().and_then(|value| value.get("reason").and_then(serde_json::Value::as_str)),
            "fallback_at_ms": marker.as_ref().and_then(|value| value.get("at_ms")),
        }))
    }

    /// Clean transient runtime artifacts (P6): bash overflow outputs older
    /// than the 7-day TTL are removed. Evidence copies live in the artifact
    /// store and are unaffected by this cleanup.
    fn cleanup(&self) -> Result<(), String> {
        let artifact_dir = std::env::var_os("COWD_BASH_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| self.config_home.join("storage").join("bash-artifacts"));
        let removed = cleanup_bash_artifacts(&artifact_dir, Duration::from_secs(7 * 24 * 3600))
            .map_err(stringify)?;
        print_json(&serde_json::json!({
            "operation": "storage_cleanup",
            "artifact_dir": artifact_dir.display().to_string(),
            "removed_files": removed,
            "ttl_days": 7,
        }))
    }

    /// P13-A3/A4: archive SQLite residue only after PostgreSQL is
    /// authoritative, no fallback marker is active, and no live SQLite pool
    /// exists in this process. Files referenced by evidence or active config
    /// are kept. Files are moved to a timestamped trash directory, never hard
    /// deleted, so recovery remains possible.
    fn cleanup_sqlite_residuals(&self) -> Result<(), String> {
        let _postgres = self.require_postgres_target()?;
        ensure_gateway_stopped()?;
        if read_fallback_marker(&self.config_home).is_some() {
            return Err(
                "refusing sqlite-residual cleanup while the runtime is in SQLite fallback"
                    .to_string(),
            );
        }
        let live_pools = memory::sqlite_pool_instance_count();
        if live_pools > 0 {
            return Err(format!(
                "refusing sqlite-residual cleanup with {live_pools} live SQLite pools; stop all Gateway processes first"
            ));
        }
        let storage_dir = self.config_home.join("storage");
        let trash_dir = storage_dir.join(format!(
            "sqlite-residuals-trash-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |duration| duration.as_secs())
        ));
        let mut candidates = Vec::new();
        if let Ok(entries) = fs::read_dir(&storage_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if is_sqlite_residual_name(&name) {
                    candidates.push(entry.path());
                }
            }
        }
        candidates.sort();
        let mut moved = Vec::new();
        let mut referenced = Vec::new();
        if !candidates.is_empty() {
            fs::create_dir_all(&trash_dir).map_err(stringify)?;
            for path in candidates {
                let name = path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_default();
                if residual_is_referenced(&name, &self.config_home, &self.workspace_root) {
                    referenced.push(name);
                    continue;
                }
                let target = trash_dir.join(&name);
                fs::rename(&path, &target).map_err(|error| {
                    format!(
                        "failed to archive sqlite residual `{}`: {error}",
                        path.display()
                    )
                })?;
                moved.push(name);
            }
        }
        print_json(&serde_json::json!({
            "operation": "storage_cleanup_sqlite_residuals",
            "backend": "postgres",
            "gateway_stopped": true,
            "live_sqlite_pools": live_pools,
            "fallback_marker": "absent",
            "moved_files": moved,
            "referenced_files_kept": referenced,
            "trash_dir": trash_dir.display().to_string(),
            "recoverable": true,
        }))
    }

    fn migrate(&self) -> Result<(), String> {
        let postgres = self.require_postgres_target()?;
        ensure_gateway_stopped()?;
        let _guard = MaintenanceGuard::acquire(&self.config_home)?;
        if staged_path(&self.config_home).exists()
            || verified_path(&self.config_home).exists()
            || active_path(&self.config_home).exists()
        {
            return Err(
                "cutover evidence already exists; refusing to merge or overwrite a prior migration"
                    .to_string(),
            );
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(stringify)?;
        let domains = runtime.block_on(self.copy_domains_parallel())?;
        let mut manifest = CutoverManifest {
            manifest_version: MANIFEST_VERSION,
            cowd_version: env!("CARGO_PKG_VERSION").to_string(),
            workspace_key: workspace_key(&self.workspace_root),
            target_backend: "postgres".to_string(),
            target_logical_identity: postgres.logical_identity.clone(),
            target_secret_ref: postgres.secret_ref.clone(),
            status: CutoverStatus::Migrated,
            product_sources: product_sources()?,
            enabled_apps: enabled_apps(self.runtime_config.apps()),
            domains,
            digest: String::new(),
        };
        seal_manifest(&mut manifest)?;
        write_manifest(&staged_path(&self.config_home), &manifest)?;
        print_json(&manifest)
    }

    async fn copy_domains_parallel(&self) -> Result<BTreeMap<String, serde_json::Value>, String> {
        let source = Arc::new(SelectedStorageTopology::compose_for_maintenance(
            &runtime::StorageTopologyConfig::default(),
            &self.config_home,
            &self.workspace_root,
        )?);
        let target = Arc::new(SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?);
        let executor = target
            .postgres_executor
            .clone()
            .ok_or_else(|| "PostgreSQL executor was not composed".to_string())?;
        let detail_root = cutover_root(&self.config_home).join("domains");
        fs::create_dir_all(&detail_root).map_err(stringify)?;
        let mut jobs = Vec::new();

        {
            let source = Arc::clone(&source);
            let target = Arc::clone(&target);
            let path = detail_root.join("fact.json");
            jobs.push((
                "fact_ledger",
                tokio::task::spawn_blocking(move || {
                    record(
                        "fact_ledger",
                        fact_postgres::copy_quiesced_fact_ledger(
                            source.fact_ledger.as_ref(),
                            target.fact_ledger.as_ref(),
                            path,
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_registry = source.registry.clone();
            let executor = executor.clone();
            let path = detail_root.join("matrix.json");
            jobs.push((
                "matrix",
                tokio::task::spawn_blocking(move || {
                    let endpoint = source_registry
                        .endpoint(&storage::StorageDomainId::Matrix)
                        .map_err(stringify)?;
                    let source = matrix_repository::MatrixSqliteRepository::open_storage_handle(
                        &endpoint.as_handle(),
                    )
                    .map_err(stringify)?;
                    let target = matrix_repository::PostgresMatrixRepository::new(executor)
                        .map_err(stringify)?;
                    record(
                        "matrix",
                        matrix_repository::copy_quiesced_matrix_store(&source, &target, path)
                            .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_registry = source.registry.clone();
            let executor = executor.clone();
            let path = detail_root.join("session.json");
            jobs.push((
                "session",
                tokio::task::spawn_blocking(move || {
                    let endpoint = source_registry
                        .endpoint(&storage::StorageDomainId::Session)
                        .map_err(stringify)?;
                    let source =
                        session::SqliteSessionStore::open_storage_handle(&endpoint.as_handle())
                            .map_err(stringify)?;
                    let target =
                        session_postgres::PostgresSessionStore::new(executor).map_err(stringify)?;
                    record(
                        "session",
                        session_postgres::copy_quiesced_session_store(&source, &target, path)
                            .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source = Arc::clone(&source);
            let target = Arc::clone(&target);
            let path = detail_root.join("runtime-event.json");
            jobs.push((
                "runtime_event",
                tokio::task::spawn_blocking(move || {
                    record(
                        "runtime_event",
                        runtime_postgres::copy_quiesced_runtime_event_store(
                            source.runtime_event_store.as_ref(),
                            target.runtime_event_store.as_ref(),
                            path,
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source = Arc::clone(&source.task_service);
            let target = Arc::clone(&target.task_service);
            let path = detail_root.join("task.json");
            jobs.push((
                "runtime_task",
                tokio::task::spawn_blocking(move || {
                    record(
                        "runtime_task",
                        runtime_postgres::copy_quiesced_task_service(
                            source.as_ref(),
                            target.as_ref(),
                            path,
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_store = Arc::clone(&source.memory_store);
            let executor = executor.clone();
            jobs.push((
                "memory",
                tokio::task::spawn_blocking(move || {
                    let target =
                        memory_postgres::PostgresMemoryStore::new(executor).map_err(stringify)?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(stringify)?;
                    record(
                        "memory",
                        runtime
                            .block_on(memory_postgres::copy_quiesced_memory_store(
                                source_store.as_ref(),
                                &target,
                            ))
                            .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_store = Arc::clone(&source.knowledge_store);
            let executor = executor.clone();
            jobs.push((
                "knowledge",
                tokio::task::spawn_blocking(move || {
                    let target = memory_postgres::PostgresKnowledgeStore::new(executor)
                        .map_err(stringify)?;
                    record(
                        "knowledge",
                        memory_postgres::copy_quiesced_knowledge_store(
                            source_store.as_ref(),
                            &target,
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_messages = Arc::clone(&source.surface_messages);
            let executor = executor.clone();
            let path = detail_root.join("surface.json");
            jobs.push((
                "surface_message",
                tokio::task::spawn_blocking(move || {
                    let target = surface_postgres::PostgresSurfaceMessageLedger::new(executor)
                        .map_err(stringify)?;
                    record(
                        "surface_message",
                        surface_postgres::copy_quiesced_surface_message_ledger(
                            source_messages.as_ref(),
                            &target,
                            path,
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_factory = Arc::clone(&source.connector_factory);
            let source_handle = source.connector_handle.clone();
            let executor = executor.clone();
            jobs.push((
                "connector_directory",
                tokio::task::spawn_blocking(move || {
                    let source = source_factory.open(&source_handle).map_err(stringify)?;
                    let target = connector_postgres::PostgresResourceDirectory::new(executor)
                        .map_err(stringify)?;
                    record(
                        "connector_directory",
                        connector_postgres::copy_quiesced_resource_directory(
                            source.as_ref(),
                            &target,
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }
        {
            let source_registry = source.registry.clone();
            let target_registry = target.registry.clone();
            let executor = executor.clone();
            let enabled = enabled_apps(self.runtime_config.apps());
            jobs.push((
                "apps",
                tokio::task::spawn_blocking(move || {
                    let enabled = enabled.into_iter().collect::<BTreeSet<_>>();
                    record(
                        "apps",
                        cowd_product_apps::migrate_enabled_storage(
                            source_registry,
                            cowd_product_apps::AppStorageTopology::Sqlite,
                            target_registry,
                            cowd_product_apps::AppStorageTopology::Postgres { executor },
                            &|app_id| enabled.contains(app_id),
                        )
                        .map_err(stringify)?,
                    )
                }),
            ));
        }

        let mut domains = BTreeMap::new();
        for (scheduled_domain, job) in jobs {
            let (domain, evidence) = job
                .await
                .map_err(|error| {
                    format!("storage migration worker `{scheduled_domain}` failed: {error}")
                })?
                .map_err(|error| {
                    format!("storage migration domain `{scheduled_domain}` failed: {error}")
                })?;
            if domain != scheduled_domain {
                return Err(format!(
                    "storage migration worker `{scheduled_domain}` returned evidence for `{domain}`"
                ));
            }
            if domains.insert(domain.clone(), evidence).is_some() {
                return Err(format!("duplicate migration evidence for {domain}"));
            }
        }
        Ok(domains)
    }

    fn verify(&self) -> Result<(), String> {
        let postgres = self.require_postgres_target()?;
        ensure_gateway_stopped()?;
        let _guard = MaintenanceGuard::acquire(&self.config_home)?;
        if verified_path(&self.config_home).exists() || active_path(&self.config_home).exists() {
            return Err("verified or active cutover evidence already exists".to_string());
        }
        let mut manifest = read_manifest(&staged_path(&self.config_home))?;
        validate_manifest(
            &manifest,
            &self.workspace_root,
            CutoverStatus::Migrated,
            postgres,
            &enabled_apps(self.runtime_config.apps()),
            true,
        )?;
        validate_domain_evidence(&manifest)?;
        // Reopen every PostgreSQL adapter and APP storage provision through
        // the production composition code. This proves schema/readiness and
        // secret resolution without mutating the staged equality evidence.
        let target = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        let mut registry = cowd_app_host::AppRegistry::default();
        cowd_product_apps::register_enabled_with_storage(
            &mut registry,
            crate::services::GatewayAppHostBinding::new().context(),
            target.registry,
            target.app_topology,
            &|app_id| self.runtime_config.apps().is_enabled(app_id),
        )
        .map_err(stringify)?;
        manifest.status = CutoverStatus::Verified;
        seal_manifest(&mut manifest)?;
        write_manifest(&verified_path(&self.config_home), &manifest)?;
        print_json(&manifest)
    }

    fn cutover(&self) -> Result<(), String> {
        let postgres = self.require_postgres_target()?;
        ensure_gateway_stopped()?;
        let _guard = MaintenanceGuard::acquire(&self.config_home)?;
        if active_path(&self.config_home).exists() {
            return Err("an active cutover manifest already exists".to_string());
        }
        let mut manifest = read_manifest(&verified_path(&self.config_home))?;
        validate_manifest(
            &manifest,
            &self.workspace_root,
            CutoverStatus::Verified,
            postgres,
            &enabled_apps(self.runtime_config.apps()),
            true,
        )?;
        validate_domain_evidence(&manifest)?;
        manifest.status = CutoverStatus::Active;
        seal_manifest(&mut manifest)?;
        write_manifest(&active_path(&self.config_home), &manifest)?;
        print_json(&manifest)
    }
}

struct MaintenanceGuard {
    path: PathBuf,
}

impl MaintenanceGuard {
    fn acquire(config_home: &Path) -> Result<Self, String> {
        let root = cutover_root(config_home);
        fs::create_dir_all(&root).map_err(stringify)?;
        let path = root.join("maintenance.lock");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("failed to acquire storage maintenance barrier: {error}"))?;
        Ok(Self { path })
    }
}

impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path) {
            tracing::warn!(
                path = %self.path.display(),
                error = %error,
                "failed to release storage maintenance barrier"
            );
        }
    }
}

fn ensure_gateway_stopped() -> Result<(), String> {
    if let Some(server) = crate::server::get_server_status().map_err(stringify)? {
        return Err(format!(
            "Gateway pid {} is still running at {}; stop it before storage cutover",
            server.pid, server.address
        ));
    }
    Ok(())
}

fn record(domain: &str, evidence: impl Serialize) -> Result<(String, serde_json::Value), String> {
    serde_json::to_value(evidence)
        .map(|value| (domain.to_string(), value))
        .map_err(stringify)
}

fn product_sources() -> Result<BTreeMap<String, cowd_app_sdk::AppSourceLock>, String> {
    cowd_product_apps::compiled_products()
        .into_iter()
        .map(|product| {
            let app_id = product.app_id().to_string();
            product
                .source_lock()
                .map(|source| (app_id.clone(), source))
                .ok_or_else(|| format!("compiled APP {app_id} has no immutable source lock"))
        })
        .collect()
}

fn enabled_apps(config: &runtime::AppsConfig) -> Vec<String> {
    let mut apps = cowd_product_apps::compiled_products()
        .into_iter()
        .filter_map(|product| {
            let app_id = product.app_id().to_string();
            config.is_enabled(&app_id).then_some(app_id)
        })
        .collect::<Vec<_>>();
    apps.sort();
    apps
}

fn validate_domain_evidence(manifest: &CutoverManifest) -> Result<(), String> {
    for domain in REQUIRED_CORE_DOMAINS {
        let evidence = manifest
            .domains
            .get(*domain)
            .ok_or_else(|| format!("cutover manifest is missing {domain} evidence"))?;
        if *domain == "apps" {
            let app_evidence = serde_json::from_value::<
                Vec<cowd_app_host::AppStorageMigrationEvidence>,
            >(evidence.clone())
            .map_err(stringify)?;
            for enabled in &manifest.enabled_apps {
                let evidence = app_evidence
                    .iter()
                    .find(|evidence| evidence.app_id.as_str() == enabled)
                    .ok_or_else(|| format!("enabled APP {enabled} has no migration evidence"))?;
                evidence.validate_for(&evidence.app_id).map_err(stringify)?;
            }
        } else {
            let source = evidence
                .get("source_digest")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("{domain} evidence has no source_digest"))?;
            let target = evidence
                .get("target_digest")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("{domain} evidence has no target_digest"))?;
            if source.is_empty() || source != target {
                return Err(format!("{domain} source/target digest evidence differs"));
            }
        }
    }
    Ok(())
}

fn validate_manifest(
    manifest: &CutoverManifest,
    workspace_root: &Path,
    expected_status: CutoverStatus,
    postgres: &runtime::PostgresTopologyConfig,
    enabled_apps: &[String],
    require_current_build_identity: bool,
) -> Result<(), String> {
    if manifest.manifest_version != MANIFEST_VERSION
        || manifest.target_backend != "postgres"
        || manifest.target_logical_identity != postgres.logical_identity
        || manifest.target_secret_ref != postgres.secret_ref
        || manifest.status != expected_status
        || manifest.workspace_key != workspace_key(workspace_root)
    {
        return Err(
            "cutover manifest does not match the configured workspace or PostgreSQL target"
                .to_string(),
        );
    }
    if require_current_build_identity
        && (manifest.cowd_version != env!("CARGO_PKG_VERSION")
            || manifest.product_sources != product_sources()?
            || manifest.enabled_apps != enabled_apps)
    {
        return Err(
            "in-progress cutover evidence was produced by a different Cowd/App build; restart the offline migration with one immutable build"
                .to_string(),
        );
    }
    let expected = manifest_digest(manifest)?;
    if manifest.digest != expected {
        return Err("cutover manifest digest is invalid".to_string());
    }
    validate_domain_evidence(manifest)
}

fn postgres_target(
    storage: &runtime::StorageTopologyConfig,
) -> Result<&runtime::PostgresTopologyConfig, String> {
    if !matches!(
        storage.backend,
        runtime::StorageBackendSelection::Postgres | runtime::StorageBackendSelection::Auto
    ) {
        return Err(
            "storage migration requires storage.backend=postgres or auto (preferred=postgres) in the resolved configuration"
                .to_string(),
        );
    }
    storage
        .postgres
        .as_ref()
        .ok_or_else(|| "resolved PostgreSQL topology is missing".to_string())
}

fn seal_manifest(manifest: &mut CutoverManifest) -> Result<(), String> {
    manifest.digest = manifest_digest(manifest)?;
    Ok(())
}

fn manifest_digest(manifest: &CutoverManifest) -> Result<String, String> {
    let mut canonical = manifest.clone();
    canonical.digest.clear();
    let bytes = serde_json::to_vec(&canonical).map_err(stringify)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn workspace_key(workspace_root: &Path) -> String {
    storage::StorageScope::workspace_key_for_root(workspace_root)
}

fn cutover_root(config_home: &Path) -> PathBuf {
    config_home.join("storage").join("cutover")
}

fn staged_path(config_home: &Path) -> PathBuf {
    cutover_root(config_home).join("staged.json")
}

fn verified_path(config_home: &Path) -> PathBuf {
    cutover_root(config_home).join("verified.json")
}

fn active_path(config_home: &Path) -> PathBuf {
    cutover_root(config_home).join("active.json")
}

fn read_manifest(path: &Path) -> Result<CutoverManifest, String> {
    let bytes = fs::read(path).map_err(|error| {
        format!(
            "failed to read cutover manifest {}: {error}",
            path.display()
        )
    })?;
    serde_json::from_slice(&bytes).map_err(stringify)
}

fn write_manifest(path: &Path, manifest: &CutoverManifest) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(stringify)?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(manifest).map_err(stringify)?,
    )
    .map_err(stringify)?;
    fs::rename(&temporary, path).map_err(stringify)
}

fn print_json(value: &impl Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(stringify)?
    );
    Ok(())
}

fn is_sqlite_residual_name(name: &str) -> bool {
    if name.starts_with("sqlite-residuals-trash-") {
        return false;
    }
    name.ends_with(".sqlite")
        || name.ends_with(".sqlite3")
        || name.ends_with("-wal")
        || name.ends_with("-shm")
}

/// Reference-aware guard: a residual file is kept when its basename appears in
/// evidence stores, active session records, or the project `.cowd` directory.
/// Scanning is bounded to 256 MiB of text and 8 MiB per file; on any read
/// error the conservative answer is "referenced" (keep the file).
fn residual_is_referenced(name: &str, config_home: &Path, workspace_root: &Path) -> bool {
    let mut roots = vec![
        config_home.join("storage").join("evidence"),
        config_home.join("storage").join("sessions"),
        workspace_root.join(".cowd"),
    ];
    roots.retain(|root| root.exists());
    let mut scanned = 0usize;
    for root in roots {
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                return true;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let Ok(metadata) = entry.metadata() else {
                    return true;
                };
                if metadata.len() > 8 * 1024 * 1024 {
                    continue;
                }
                let Ok(bytes) = fs::read(&path) else {
                    return true;
                };
                scanned += bytes.len();
                if scanned > 256 * 1024 * 1024 {
                    return true;
                }
                if bytes
                    .windows(name.len())
                    .any(|window| window == name.as_bytes())
                {
                    return true;
                }
            }
        }
    }
    false
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_removes_only_expired_owned_artifacts() {
        let dir = tempfile::tempdir().expect("artifact dir");
        let root = dir.path();
        let old = root.join("111-aaa.out");
        let fresh = root.join("222-bbb.out");
        let unrelated = root.join("keep.txt");
        fs::write(&old, "old").expect("write old");
        fs::write(&fresh, "fresh").expect("write fresh");
        fs::write(&unrelated, "keep").expect("write keep");
        let old_time = SystemTime::now() - Duration::from_secs(8 * 24 * 3600);
        let _ = filetime_set(&old, old_time);

        let removed =
            cleanup_bash_artifacts(root, Duration::from_secs(7 * 24 * 3600)).expect("cleanup runs");

        assert_eq!(removed, 1);
        assert!(!old.exists());
        assert!(fresh.exists());
        assert!(unrelated.exists());
    }

    #[test]
    fn sealed_manifest_detects_mutation_and_workspace_drift() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut manifest = CutoverManifest {
            manifest_version: MANIFEST_VERSION,
            cowd_version: env!("CARGO_PKG_VERSION").to_string(),
            workspace_key: workspace_key(workspace.path()),
            target_backend: "postgres".to_string(),
            target_logical_identity: "test-primary".to_string(),
            target_secret_ref: "env:COWD_TEST_POSTGRES_URL".to_string(),
            status: CutoverStatus::Migrated,
            product_sources: product_sources().expect("product sources"),
            enabled_apps: Vec::new(),
            domains: BTreeMap::new(),
            digest: String::new(),
        };
        seal_manifest(&mut manifest).expect("seal manifest");
        assert_eq!(manifest.digest, manifest_digest(&manifest).unwrap());
        manifest.target_backend = "sqlite".to_string();
        assert_ne!(manifest.digest, manifest_digest(&manifest).unwrap());
    }

    #[test]
    fn active_manifest_is_historical_evidence_not_a_permanent_build_lock() {
        let workspace = tempfile::tempdir().expect("workspace");
        let postgres = runtime::PostgresTopologyConfig {
            logical_identity: "production-primary".to_string(),
            secret_ref: "env:COWD_TEST_POSTGRES_URL".to_string(),
            ..runtime::PostgresTopologyConfig::default()
        };
        let domains = REQUIRED_CORE_DOMAINS
            .iter()
            .map(|domain| {
                let evidence = if *domain == "apps" {
                    serde_json::json!([])
                } else {
                    serde_json::json!({
                        "source_digest": "sha256:historical",
                        "target_digest": "sha256:historical",
                    })
                };
                ((*domain).to_string(), evidence)
            })
            .collect();
        let mut manifest = CutoverManifest {
            manifest_version: MANIFEST_VERSION,
            cowd_version: "0.1.0-historical".to_string(),
            workspace_key: workspace_key(workspace.path()),
            target_backend: "postgres".to_string(),
            target_logical_identity: postgres.logical_identity.clone(),
            target_secret_ref: postgres.secret_ref.clone(),
            status: CutoverStatus::Active,
            product_sources: BTreeMap::from([(
                "historical-app".to_string(),
                cowd_app_sdk::AppSourceLock {
                    git: "https://example.invalid/historical-app".to_string(),
                    revision: "0123456789abcdef".to_string(),
                },
            )]),
            enabled_apps: Vec::new(),
            domains,
            digest: String::new(),
        };
        seal_manifest(&mut manifest).expect("seal manifest");

        validate_manifest(
            &manifest,
            workspace.path(),
            CutoverStatus::Active,
            &postgres,
            &[],
            false,
        )
        .expect("runtime startup accepts immutable historical evidence");
        assert!(
            validate_manifest(
                &manifest,
                workspace.path(),
                CutoverStatus::Active,
                &postgres,
                &[],
                true,
            )
            .is_err(),
            "an in-progress offline migration still requires one immutable build"
        );

        let mut wrong_target = postgres.clone();
        wrong_target.logical_identity = "different-database".to_string();
        assert!(
            validate_manifest(
                &manifest,
                workspace.path(),
                CutoverStatus::Active,
                &wrong_target,
                &[],
                false,
            )
            .is_err(),
            "database identity drift must remain a hard startup failure"
        );
    }

    #[test]
    fn sqlite_residual_name_matches_archivable_files() {
        assert!(is_sqlite_residual_name("memory.sqlite"));
        assert!(is_sqlite_residual_name("fact.sqlite-wal"));
        assert!(is_sqlite_residual_name("session.sqlite3-shm"));
        assert!(!is_sqlite_residual_name("bash-artifacts/111.out"));
        assert!(!is_sqlite_residual_name(
            "sqlite-residuals-trash-123/memory.sqlite"
        ));
    }

    #[test]
    fn reference_scan_keeps_residuals_named_in_evidence() {
        let dir = tempfile::tempdir().expect("evidence dir");
        let evidence = dir.path().join("storage").join("evidence");
        fs::create_dir_all(&evidence).expect("evidence dir");
        fs::write(
            evidence.join("artifact.json"),
            r#"{"ref":"storage/memory.sqlite"}"#,
        )
        .expect("evidence file");
        let workspace = tempfile::tempdir().expect("workspace dir");

        assert!(residual_is_referenced(
            "memory.sqlite",
            dir.path(),
            workspace.path()
        ));
        assert!(!residual_is_referenced(
            "fact.sqlite",
            dir.path(),
            workspace.path()
        ));
    }
}

#[cfg(unix)]
fn filetime_set(path: &Path, time: SystemTime) -> std::io::Result<()> {
    let times = fs::FileTimes::new().set_modified(time);
    fs::File::open(path)?.set_times(times)
}

/// Remove bash overflow artifacts older than `max_age`. Returns the number of
/// removed files. Only files matching the artifact convention (`.out` suffix
/// under the artifact directory) are considered.
fn cleanup_bash_artifacts(dir: &Path, max_age: Duration) -> Result<usize, String> {
    let now = SystemTime::now();
    let mut removed = 0usize;
    for entry in fs::read_dir(dir).map_err(stringify)? {
        let entry = entry.map_err(stringify)?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !file_name.ends_with(".out") {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .map_err(stringify)?;
        if now.duration_since(modified).map_err(stringify)? > max_age {
            fs::remove_file(&path).map_err(stringify)?;
            removed += 1;
        }
    }
    Ok(removed)
}
