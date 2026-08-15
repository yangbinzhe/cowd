use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
use cowd_app_host::{
    catalog::{
        AdmittedApp, AppCatalogBuilder, AppCatalogPolicy, AppCatalogSnapshot, AppTrustStore,
        EffectiveAppPolicy, TrustedSigningKey,
    },
    supervisor::{
        AppRuntimeSupervisor, AppRuntimeSupervisorConfig, AppWorkerConnector, ConnectorFuture,
        SupervisorError,
    },
};
use cowd_app_protocol::{
    derive_channel_token_v1, format_bootstrap_authorization_v1, format_channel_authorization_v1,
    manifest_authorization_profile_digest_v1, manifest_capability_digest_v1, AppHandshakeRequestV1,
    AppHandshakeV1, AppHealthStatusV1, AppHealthV1, AppId, BootstrapSecretV1, ChannelPurposeV1,
    ChannelTokenV1, ProtocolValidate, APP_HANDSHAKE_PATH_V1, APP_HEALTH_PATH_V1,
    ENV_APP_CONFIG_FILE_V1, ENV_APP_CREDENTIAL_FILE_V1, ENV_APP_DATA_DIR_V1, ENV_APP_GENERATION_V1,
    ENV_APP_ID_V1, ENV_APP_LOG_FORMAT_V1, ENV_APP_SOCKET_V1, ENV_CORE_BRIDGE_SOCKET_V1,
    HEADER_AUTHORIZATION_V1, HEADER_CONTENT_TYPE_V1, PROTOCOL_REVISION_V1, UNARY_CONTENT_TYPE_V1,
};
use http_body_util::{BodyExt, Full};
use managed_worker_runtime::{
    recover_runtime_root, CancellationToken, GenerationFence, ManagedH2Channel,
    ManagedWorkerHandle, ManagedWorkerSpec, PeerCredentialPolicy, WorkerIsolationMode,
    WorkerResourceLimits,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

const MAX_TRUST_STORE_BYTES: u64 = 1024 * 1024;
pub(crate) const PROTOCOL_DIGEST_V1: &str =
    "sha256:072d80864a8addaecfc4f236d077f9a5f6eaeec2e587518da515b2b7e9768769";

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppPlatformError {
    #[error("APP platform configuration is invalid: {0}")]
    Configuration(String),
    #[error("APP platform I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("APP trust store is invalid: {0}")]
    Trust(String),
    #[error("APP catalog build failed: {0}")]
    Catalog(String),
    #[error("APP supervisor failed: {0}")]
    Supervisor(#[from] SupervisorError),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustStoreFileV1 {
    schema_version: u16,
    keys: Vec<TrustKeyFileV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustKeyFileV1 {
    key_id: String,
    public_key_base64url: String,
    revoked: bool,
}

#[derive(Debug)]
pub(crate) struct GatewayAppConnection {
    channel: ManagedH2Channel,
    token: ChannelTokenV1,
}

#[derive(Debug)]
pub(crate) struct GatewayAppConnector {
    launcher_path: PathBuf,
    launcher_sha256: String,
    gateway_instance: String,
    data_root: PathBuf,
    core_bridge_socket: PathBuf,
    cgroup_root: PathBuf,
    resources: WorkerResourceLimits,
    handshake_timeout: Duration,
}

pub(crate) type GatewayAppSupervisor = AppRuntimeSupervisor<GatewayAppConnector>;

pub(crate) struct GatewayAppPlatform {
    catalog: Arc<AppCatalogSnapshot>,
    supervisor: GatewayAppSupervisor,
}

impl GatewayAppPlatform {
    pub(crate) fn catalog(&self) -> &Arc<AppCatalogSnapshot> {
        &self.catalog
    }
    pub(crate) fn supervisor(&self) -> &GatewayAppSupervisor {
        &self.supervisor
    }

    pub(crate) async fn build(config: &runtime::AppsConfig) -> Result<Arc<Self>, AppPlatformError> {
        validate_config_paths(config)?;
        let expected_uid = unsafe { libc::geteuid() };
        let trust = match config.trust_store() {
            Some(path) => load_trust_store(path, expected_uid)?,
            None => AppTrustStore::default(),
        };
        let policy = AppCatalogPolicy {
            entries: config
                .configured_app_ids()
                .map(|id| {
                    let entry = config.entry(id);
                    (
                        AppId(id.to_owned()),
                        EffectiveAppPolicy {
                            enabled: entry.enabled,
                            required: entry.required,
                            activation: entry.activation,
                            config_file: entry.config_file,
                        },
                    )
                })
                .collect(),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let snapshot = Arc::new(
            AppCatalogBuilder::new(
                config.directories().to_vec(),
                policy,
                trust,
                expected_uid,
                now,
            )
            .build()
            .map_err(|error| AppPlatformError::Catalog(error.to_string()))?,
        );
        for app in snapshot.apps() {
            audit_complete_bundle(app, expected_uid)?;
        }
        for id in config.configured_app_ids() {
            let entry = config.entry(id);
            if entry.enabled && entry.required && snapshot.get(&AppId(id.to_owned())).is_none() {
                return Err(AppPlatformError::Configuration(format!(
                    "required APP `{id}` was not admitted"
                )));
            }
        }
        let gateway_instance = uuid::Uuid::new_v4().to_string();
        if snapshot.apps().len() == 0 {
            let connector = Arc::new(GatewayAppConnector::empty(gateway_instance));
            let supervisor =
                AppRuntimeSupervisor::new(snapshot.clone(), connector, supervisor_config(config))?;
            return Ok(Arc::new(Self {
                catalog: snapshot,
                supervisor,
            }));
        }
        let launcher = config.launcher().ok_or_else(|| {
            AppPlatformError::Configuration("launcher is required when an APP is admitted".into())
        })?;
        let launcher_path = canonical_regular_file(&launcher.path, expected_uid)?;
        verify_digest(&launcher_path, &launcher.sha256)?;
        let cgroup_root = config
            .cgroup_root()
            .ok_or_else(|| {
                AppPlatformError::Configuration(
                    "delegated cgroup_root is required when an APP is admitted".into(),
                )
            })?
            .to_path_buf();
        secure_dir(config.runtime_root(), expected_uid)?;
        secure_dir(config.data_root(), expected_uid)?;
        for app in snapshot.apps() {
            secure_dir(
                &config.data_root().join(&app.manifest.app_id.0),
                expected_uid,
            )?;
        }
        if let Ok(app_roots) = fs::read_dir(config.runtime_root()) {
            for root in app_roots
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
            {
                recover_runtime_root(&root, snapshot.generation().0.as_str(), &gateway_instance)
                    .map_err(|error| {
                        AppPlatformError::Catalog(format!("runtime recovery failed: {error}"))
                    })?;
            }
        }
        let r = config.resources();
        let connector = Arc::new(GatewayAppConnector {
            launcher_path,
            launcher_sha256: launcher.sha256.clone(),
            gateway_instance,
            data_root: config.data_root().to_path_buf(),
            core_bridge_socket: config.core_bridge_socket().to_path_buf(),
            cgroup_root,
            resources: WorkerResourceLimits {
                nofile: r.nofile,
                nproc: r.nproc,
                address_space_bytes: r.address_space_bytes,
                cpu_seconds: r.cpu_seconds,
                file_size_bytes: r.file_size_bytes,
                cgroup_memory_bytes: r.cgroup_memory_bytes,
                cgroup_pids: r.cgroup_pids,
                cgroup_cpu_quota_us: r.cgroup_cpu_quota_us,
                cgroup_cpu_period_us: r.cgroup_cpu_period_us,
            },
            handshake_timeout: Duration::from_millis(config.supervisor().handshake_timeout_ms),
        });
        let supervisor =
            AppRuntimeSupervisor::new(snapshot.clone(), connector, supervisor_config(config))?;
        supervisor.start_resident().await?;
        Ok(Arc::new(Self {
            catalog: snapshot,
            supervisor,
        }))
    }

    pub(crate) async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.supervisor.shutdown().await
    }
}

impl GatewayAppConnector {
    fn empty(gateway_instance: String) -> Self {
        Self {
            launcher_path: PathBuf::new(),
            launcher_sha256: String::new(),
            gateway_instance,
            data_root: PathBuf::new(),
            core_bridge_socket: PathBuf::new(),
            cgroup_root: PathBuf::new(),
            resources: WorkerResourceLimits::default(),
            handshake_timeout: Duration::from_secs(3),
        }
    }
}

impl AppWorkerConnector for GatewayAppConnector {
    type Connection = GatewayAppConnection;

    fn configure(&self, app: &AdmittedApp, mut spec: ManagedWorkerSpec) -> ManagedWorkerSpec {
        let data_dir = self.data_root.join(&app.manifest.app_id.0);
        spec.launcher_path = self.launcher_path.clone();
        spec.launcher_sha256.clone_from(&self.launcher_sha256);
        spec.gateway_instance.clone_from(&self.gateway_instance);
        spec.data_dir = data_dir.clone();
        spec.config_dir = app
            .policy
            .config_file
            .as_deref()
            .and_then(Path::parent)
            .unwrap_or(&app.bundle_root)
            .to_path_buf();
        spec.bundle_dir = app.bundle_root.clone();
        spec.read_only_dirs = vec![spec.config_dir.clone(), spec.bundle_dir.clone()];
        spec.isolation_mode = WorkerIsolationMode::Enforce;
        spec.resource_limits = self.resources.clone();
        spec.cgroup_root = Some(self.cgroup_root.clone());
        spec.socket_env = Some(ENV_APP_SOCKET_V1.to_owned());
        spec.credential_env = Some(ENV_APP_CREDENTIAL_FILE_V1.to_owned());
        spec.generation_env = Some(ENV_APP_GENERATION_V1.to_owned());
        let config_file = app
            .policy
            .config_file
            .as_ref()
            .map_or_else(String::new, |p| p.display().to_string());
        for (key, value) in [
            (ENV_APP_ID_V1, app.manifest.app_id.0.clone()),
            (
                ENV_CORE_BRIDGE_SOCKET_V1,
                self.core_bridge_socket.display().to_string(),
            ),
            (ENV_APP_DATA_DIR_V1, data_dir.display().to_string()),
            (ENV_APP_CONFIG_FILE_V1, config_file),
            (ENV_APP_LOG_FORMAT_V1, "json".to_owned()),
        ] {
            spec.env.insert(key.to_owned(), value);
            spec.allowed_env_keys.insert(key.to_owned());
        }
        for key in [
            ENV_APP_SOCKET_V1,
            ENV_APP_CREDENTIAL_FILE_V1,
            ENV_APP_GENERATION_V1,
        ] {
            spec.allowed_env_keys.insert(key.to_owned());
        }
        spec
    }

    fn connect<'a>(
        &'a self,
        app: &'a AdmittedApp,
        worker: &'a ManagedWorkerHandle,
        cancellation: &'a CancellationToken,
    ) -> ConnectorFuture<'a, Self::Connection> {
        Box::pin(async move {
            let raw = worker
                .take_bootstrap_secret()
                .await
                .map_err(worker_error(app))?;
            let encoded =
                std::str::from_utf8(raw.as_bytes()).map_err(|error| SupervisorError::Worker {
                    app_id: app.manifest.app_id.clone(),
                    detail: error.to_string(),
                })?;
            let secret =
                BootstrapSecretV1::parse_base64url(encoded).map_err(protocol_error(app))?;
            let fence =
                GenerationFence::new(app.generation.0.clone()).map_err(worker_error(app))?;
            let channel = ManagedH2Channel::connect_verified(
                worker.socket_path(),
                fence,
                cancellation,
                Instant::now() + self.handshake_timeout,
                PeerCredentialPolicy::ExactPid(worker.pid()),
            )
            .await
            .map_err(|error| SupervisorError::Worker {
                app_id: app.manifest.app_id.clone(),
                detail: error.to_string(),
            })?;
            let request = AppHandshakeRequestV1 {
                schema_version: 1,
                protocol_revision: PROTOCOL_REVISION_V1,
                app_id: app.manifest.app_id.clone(),
                generation: app.generation.clone(),
                gateway_pid: std::process::id(),
                worker_pid: worker.pid(),
            };
            request.validate().map_err(protocol_error(app))?;
            let request = hyper::Request::post(APP_HANDSHAKE_PATH_V1)
                .header(HEADER_CONTENT_TYPE_V1, UNARY_CONTENT_TYPE_V1)
                .header(
                    HEADER_AUTHORIZATION_V1,
                    format_bootstrap_authorization_v1(&secret),
                )
                .body(Full::new(Bytes::from(
                    serde_json::to_vec(&request).map_err(protocol_error(app))?,
                )))
                .map_err(protocol_error(app))?;
            let response = channel
                .send(
                    &app.generation.0,
                    request,
                    self.handshake_timeout,
                    cancellation,
                )
                .await
                .map_err(worker_error(app))?;
            if !response.status().is_success() {
                return Err(SupervisorError::Worker {
                    app_id: app.manifest.app_id.clone(),
                    detail: format!("handshake returned {}", response.status()),
                });
            }
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(protocol_error(app))?
                .to_bytes();
            let handshake: AppHandshakeV1 =
                serde_json::from_slice(&body).map_err(protocol_error(app))?;
            handshake.validate().map_err(protocol_error(app))?;
            let capability =
                manifest_capability_digest_v1(&app.manifest).map_err(protocol_error(app))?;
            let profiles = manifest_authorization_profile_digest_v1(&app.manifest)
                .map_err(protocol_error(app))?;
            if handshake.app_id != app.manifest.app_id
                || handshake.generation != app.generation
                || handshake.worker_pid != worker.pid()
                || handshake.protocol_revision != PROTOCOL_REVISION_V1
                || handshake.artifact_version != app.manifest.artifact_version
                || handshake.capability_digest != capability
                || handshake.authorization_profile_digest != profiles
            {
                return Err(SupervisorError::Worker {
                    app_id: app.manifest.app_id.clone(),
                    detail: "handshake identity or manifest digest mismatch".into(),
                });
            }
            let token = derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app.manifest.app_id,
                &app.generation,
                worker.pid(),
                &handshake.worker_nonce,
            )
            .map_err(protocol_error(app))?;
            Ok(GatewayAppConnection { channel, token })
        })
    }

    fn health<'a>(
        &'a self,
        app: &'a AdmittedApp,
        _worker: &'a ManagedWorkerHandle,
        connection: &'a Self::Connection,
        cancellation: &'a CancellationToken,
    ) -> ConnectorFuture<'a, ()> {
        Box::pin(async move {
            let request = hyper::Request::get(APP_HEALTH_PATH_V1)
                .header(
                    HEADER_AUTHORIZATION_V1,
                    format_channel_authorization_v1(&connection.token),
                )
                .body(Full::new(Bytes::new()))
                .map_err(protocol_error(app))?;
            let response = connection
                .channel
                .send(
                    &app.generation.0,
                    request,
                    self.handshake_timeout,
                    cancellation,
                )
                .await
                .map_err(worker_error(app))?;
            if !response.status().is_success() {
                return Err(SupervisorError::Worker {
                    app_id: app.manifest.app_id.clone(),
                    detail: format!("health returned {}", response.status()),
                });
            }
            let health: AppHealthV1 = serde_json::from_slice(
                &response
                    .into_body()
                    .collect()
                    .await
                    .map_err(protocol_error(app))?
                    .to_bytes(),
            )
            .map_err(protocol_error(app))?;
            health.validate().map_err(protocol_error(app))?;
            if health.app_id != app.manifest.app_id
                || health.generation != app.generation
                || !matches!(
                    health.status,
                    AppHealthStatusV1::Ready | AppHealthStatusV1::Degraded
                )
            {
                return Err(SupervisorError::Worker {
                    app_id: app.manifest.app_id.clone(),
                    detail: "health identity or state mismatch".into(),
                });
            }
            Ok(())
        })
    }
}

fn supervisor_config(config: &runtime::AppsConfig) -> AppRuntimeSupervisorConfig {
    let source = config.supervisor();
    AppRuntimeSupervisorConfig {
        runtime_root: config.runtime_root().to_path_buf(),
        max_starting_workers: source.max_starting_workers,
        max_active_workers: source.max_active_workers,
        max_waiters_per_app: source.max_waiters_per_app,
        activation_timeout: Duration::from_millis(source.activation_timeout_ms),
        shutdown_timeout: Duration::from_millis(source.graceful_shutdown_ms),
        idle_ttl: source.idle_ttl_seconds.map(Duration::from_secs),
        crash_window: Duration::from_secs(source.restart_window_seconds),
        crash_budget: source.max_restarts_per_window,
        ..AppRuntimeSupervisorConfig::default()
    }
}

fn validate_config_paths(config: &runtime::AppsConfig) -> Result<(), AppPlatformError> {
    let mut paths = config
        .directories()
        .iter()
        .map(PathBuf::as_path)
        .collect::<Vec<_>>();
    paths.extend([
        config.runtime_root(),
        config.data_root(),
        config.core_bridge_socket(),
    ]);
    paths.extend(config.trust_store());
    paths.extend(config.cgroup_root());
    if let Some(launcher) = config.launcher() {
        paths.push(&launcher.path);
    }
    for app_id in config.configured_app_ids() {
        if let Some(path) = config.entry(app_id).config_file {
            if !path.is_absolute() {
                return Err(AppPlatformError::Configuration(format!(
                    "APP paths must be absolute: {}",
                    path.display()
                )));
            }
        }
    }
    if let Some(path) = paths.into_iter().find(|path| !path.is_absolute()) {
        return Err(AppPlatformError::Configuration(format!(
            "APP paths must be absolute: {}",
            path.display()
        )));
    }
    if config.runtime_root() == config.data_root()
        || config.core_bridge_socket().starts_with(config.data_root())
    {
        return Err(AppPlatformError::Configuration(
            "runtime_root, data_root and core_bridge_socket must have separate ownership domains"
                .into(),
        ));
    }
    Ok(())
}

fn load_trust_store(path: &Path, expected_uid: u32) -> Result<AppTrustStore, AppPlatformError> {
    let path = canonical_secure_file(path, expected_uid, true)?;
    let metadata = fs::metadata(&path).map_err(io_error(&path))?;
    if metadata.len() > MAX_TRUST_STORE_BYTES {
        return Err(AppPlatformError::Trust("file exceeds 1 MiB".into()));
    }
    let decoded: TrustStoreFileV1 =
        serde_json::from_slice(&fs::read(&path).map_err(io_error(&path))?)
            .map_err(|e| AppPlatformError::Trust(e.to_string()))?;
    if decoded.schema_version != 1 {
        return Err(AppPlatformError::Trust("schema_version must be 1".into()));
    }
    let mut ids = BTreeSet::new();
    let mut keys = Vec::new();
    for key in decoded.keys {
        if key.key_id.trim().is_empty() || !ids.insert(key.key_id.clone()) {
            return Err(AppPlatformError::Trust(
                "key ids must be non-empty and unique".into(),
            ));
        }
        let public_key = URL_SAFE_NO_PAD
            .decode(&key.public_key_base64url)
            .map_err(|_| AppPlatformError::Trust("public key must be unpadded base64url".into()))?;
        if public_key.len() != 32 || URL_SAFE_NO_PAD.encode(&public_key) != key.public_key_base64url
        {
            return Err(AppPlatformError::Trust(
                "Ed25519 public key must be canonical 32-byte base64url".into(),
            ));
        }
        keys.push(TrustedSigningKey {
            key_id: key.key_id,
            public_key,
            revoked: key.revoked,
        });
    }
    Ok(AppTrustStore::new(keys))
}

fn audit_complete_bundle(app: &AdmittedApp, expected_uid: u32) -> Result<(), AppPlatformError> {
    let declared = app
        .manifest
        .integrity
        .files
        .keys()
        .map(PathBuf::from)
        .collect::<BTreeSet<_>>();
    audit_bundle_tree(&app.bundle_root, &declared, expected_uid).map_err(|error| {
        AppPlatformError::Catalog(format!(
            "bundle {} failed closed-tree audit: {error}",
            app.manifest.app_id
        ))
    })
}

fn audit_bundle_tree(
    bundle_root: &Path,
    declared: &BTreeSet<PathBuf>,
    expected_uid: u32,
) -> Result<(), String> {
    let mut actual = BTreeSet::new();
    let mut inodes = BTreeSet::new();
    let mut stack = vec![bundle_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let meta = fs::symlink_metadata(&dir).map_err(|error| error.to_string())?;
        if meta.file_type().is_symlink()
            || !meta.is_dir()
            || meta.uid() != expected_uid
            || meta.mode() & 0o222 != 0
        {
            return Err(format!("unsafe bundle directory {}", dir.display()));
        }
        for entry in fs::read_dir(&dir).map_err(|error| error.to_string())? {
            let path = entry.map_err(|error| error.to_string())?.path();
            let meta = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
            if meta.file_type().is_symlink()
                || meta.uid() != expected_uid
                || meta.mode() & 0o222 != 0
            {
                return Err(format!("unsafe bundle node {}", path.display()));
            }
            if meta.is_dir() {
                stack.push(path);
                continue;
            }
            if !meta.is_file() || meta.nlink() != 1 || !inodes.insert((meta.dev(), meta.ino())) {
                return Err(format!(
                    "special or multiply-linked bundle node {}",
                    path.display()
                ));
            }
            let relative = path
                .strip_prefix(bundle_root)
                .map_err(|_| "bundle path escaped root".to_owned())?
                .to_path_buf();
            if relative != Path::new("app.json") {
                actual.insert(relative);
            }
        }
    }
    if &actual != declared {
        return Err("bundle files differ from signed integrity set".into());
    }
    Ok(())
}

fn secure_dir(path: &Path, expected_uid: u32) -> Result<(), AppPlatformError> {
    fs::create_dir_all(path).map_err(io_error(path))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_error(path))?;
    let meta = fs::symlink_metadata(path).map_err(io_error(path))?;
    if !meta.is_dir() || meta.file_type().is_symlink() || meta.uid() != expected_uid {
        return Err(AppPlatformError::Configuration(format!(
            "unsafe directory {}",
            path.display()
        )));
    }
    Ok(())
}

fn canonical_regular_file(path: &Path, expected_uid: u32) -> Result<PathBuf, AppPlatformError> {
    canonical_secure_file(path, expected_uid, false)
}
fn canonical_secure_file(
    path: &Path,
    expected_uid: u32,
    exact_0600: bool,
) -> Result<PathBuf, AppPlatformError> {
    let before = fs::symlink_metadata(path).map_err(io_error(path))?;
    if before.file_type().is_symlink()
        || !before.is_file()
        || before.uid() != expected_uid
        || (exact_0600 && before.mode() & 0o777 != 0o600)
        || (!exact_0600 && before.mode() & 0o022 != 0)
    {
        return Err(AppPlatformError::Configuration(format!(
            "unsafe file {}",
            path.display()
        )));
    }
    fs::canonicalize(path).map_err(io_error(path))
}
fn verify_digest(path: &Path, expected: &str) -> Result<(), AppPlatformError> {
    let actual = format!(
        "sha256:{:x}",
        Sha256::digest(fs::read(path).map_err(io_error(path))?)
    );
    if actual != expected {
        return Err(AppPlatformError::Configuration(format!(
            "launcher digest mismatch at {}",
            path.display()
        )));
    }
    Ok(())
}
fn io_error(path: &Path) -> impl FnOnce(std::io::Error) -> AppPlatformError + '_ {
    move |source| AppPlatformError::Io {
        path: path.to_path_buf(),
        source,
    }
}
fn worker_error(
    app: &AdmittedApp,
) -> impl FnOnce(managed_worker_runtime::ManagedWorkerError) -> SupervisorError + '_ {
    move |error| SupervisorError::Worker {
        app_id: app.manifest.app_id.clone(),
        detail: error.to_string(),
    }
}
fn protocol_error<'a, E: std::fmt::Display>(
    app: &'a AdmittedApp,
) -> impl FnOnce(E) -> SupervisorError + 'a {
    move |error| SupervisorError::Worker {
        app_id: app.manifest.app_id.clone(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn trust_store_requires_owner_only_mode_and_strict_json() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("trust.json");
        fs::write(&path, format!(r#"{{"schema_version":1,"keys":[{{"key_id":"k1","public_key_base64url":"{}","revoked":false}}]}}"#, URL_SAFE_NO_PAD.encode([7_u8; 32]))).expect("write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("mode");
        let uid = unsafe { libc::geteuid() };
        load_trust_store(&path, uid).expect("strict trust store");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("mode");
        assert!(load_trust_store(&path, uid).is_err());
    }

    #[test]
    fn bundle_tree_rejects_extra_symlink_and_hardlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let uid = unsafe { libc::geteuid() };
        fs::write(dir.path().join("app.json"), b"{}").expect("manifest");
        fs::write(dir.path().join("worker"), b"signed").expect("worker");
        fs::set_permissions(
            dir.path().join("app.json"),
            fs::Permissions::from_mode(0o400),
        )
        .expect("manifest mode");
        fs::set_permissions(dir.path().join("worker"), fs::Permissions::from_mode(0o500))
            .expect("worker mode");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).expect("root mode");
        let declared = BTreeSet::from([PathBuf::from("worker")]);
        audit_bundle_tree(dir.path(), &declared, uid).expect("closed signed tree");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("open root");
        fs::write(dir.path().join("extra"), b"unsigned").expect("extra");
        fs::set_permissions(dir.path().join("extra"), fs::Permissions::from_mode(0o400))
            .expect("extra mode");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).expect("seal root");
        assert!(audit_bundle_tree(dir.path(), &declared, uid).is_err());
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("open root");
        fs::remove_file(dir.path().join("extra")).expect("remove extra");
        symlink("worker", dir.path().join("alias")).expect("symlink");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).expect("seal root");
        assert!(audit_bundle_tree(dir.path(), &declared, uid).is_err());
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("open root");
        fs::remove_file(dir.path().join("alias")).expect("remove alias");
        fs::hard_link(dir.path().join("worker"), dir.path().join("hard")).expect("hard link");
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).expect("seal root");
        assert!(audit_bundle_tree(dir.path(), &declared, uid).is_err());
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("cleanup root");
    }
}
