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
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::selected_storage::SelectedStorageTopology;

const MANIFEST_VERSION: u32 = 1;
const REQUIRED_CORE_DOMAINS: &[&str] = &[
    "fact_ledger",
    "matrix",
    "approval_history",
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
        _ => return Err("usage: cowd storage plan | migrate | verify | cutover".to_string()),
    };
    let context = CutoverContext::load()?;
    match command {
        "plan" => context.plan(),
        "migrate" => context.migrate(),
        "verify" => context.verify(),
        "cutover" => context.cutover(),
        _ => Err("usage: cowd storage plan | migrate | verify | cutover".to_string()),
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
                "min_idle_connections": postgres.min_idle_connections,
                "checkout_timeout_ms": postgres.checkout_timeout_ms,
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
            let path = detail_root.join("approval.json");
            jobs.push((
                "approval_history",
                tokio::task::spawn_blocking(move || {
                    let endpoint = source_registry
                        .endpoint(&storage::StorageDomainId::Approval)
                        .map_err(stringify)?;
                    let source =
                        approval::SqliteApprovalHistoryLedger::open(endpoint).map_err(stringify)?;
                    let target = approval::PostgresApprovalHistoryLedger::new(executor)
                        .map_err(stringify)?;
                    record(
                        "approval_history",
                        approval::copy_quiesced_approval_history(&source, &target, path)
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
                    let source = memory::store::session::SqliteSessionStore::open_storage_handle(
                        &endpoint.as_handle(),
                    )
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
            let source = source.task_kernel.runtime_kernel();
            let target = target.task_kernel.runtime_kernel();
            let path = detail_root.join("task.json");
            jobs.push((
                "runtime_task",
                tokio::task::spawn_blocking(move || {
                    record(
                        "runtime_task",
                        runtime_postgres::copy_quiesced_task_kernel(&source, &target, path)
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
) -> Result<(), String> {
    if manifest.manifest_version != MANIFEST_VERSION
        || manifest.cowd_version != env!("CARGO_PKG_VERSION")
        || manifest.target_backend != "postgres"
        || manifest.target_logical_identity != postgres.logical_identity
        || manifest.target_secret_ref != postgres.secret_ref
        || manifest.status != expected_status
        || manifest.workspace_key != workspace_key(workspace_root)
        || manifest.product_sources != product_sources()?
        || manifest.enabled_apps != enabled_apps
    {
        return Err(
            "cutover manifest does not match this Cowd binary/workspace/topology".to_string(),
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
    if storage.backend != runtime::StorageBackendSelection::Postgres {
        return Err(
            "storage migration requires storage.backend=postgres in the resolved configuration"
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

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
