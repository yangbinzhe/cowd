// Test assertions intentionally use unwrap/expect; normal library builds remain strict.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

#[cfg(all(test, feature = "storage-postgres"))]
extern crate self as storage;
#[cfg(all(test, feature = "storage-postgres"))]
#[path = "../test-support/postgres_scope.rs"]
mod postgres_scope;

#[cfg(feature = "storage-postgres")]
mod postgres;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(feature = "storage-postgres")]
pub use postgres::{
    PostgresClient, PostgresConnection, PostgresConnectionConfig, PostgresExecutor,
    PostgresExecutorHealth, PostgresExecutorMetrics, PostgresMigrationMode,
    PostgresMigrationReport, PostgresMigrationSpec, PostgresPoolLaneConfig, PostgresPoolLaneHealth,
    PostgresPoolSet, PostgresPoolSetConfig, PostgresTransaction, PostgresWorkloadClass,
    ResolvedPostgresUrl, SecretRefResolver, StaticSecretRefResolver,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[cfg(feature = "storage-postgres")]
    #[error("postgres error: {0}")]
    Postgres(#[from] ::postgres::Error),
    #[error("{0}")]
    Other(String),
}

pub trait SessionRepository {
    type Error;
    fn storage_handle(&self) -> &StorageHandle;
}

pub trait TaskRepository {
    type Error;
    fn storage_handle(&self) -> &StorageHandle;
}

pub trait ResourceDirectoryRepository {
    type Error;
    fn storage_handle(&self) -> &StorageHandle;
}

pub trait MatrixRepository {
    type Error;
    fn storage_handle(&self) -> &StorageHandle;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendKind {
    Postgres,
    FileJson,
    Directory,
    BlobDirectory,
}

/// A stable, typed identity for every durable Cowd storage domain.
///
/// The identity is deliberately separate from the physical backend location:
/// a future PostgreSQL adapter must preserve the same domain and scope rather
/// than asking callers to branch on a filename or connection string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageDomainId {
    Session,
    Memory,
    Knowledge,
    Fact,
    Matrix,
    Tasks,
    Audit,
    Growth,
    RuntimeEvents,
    SurfaceMessages,
    ConnectorDirectory,
    AuditLog,
    Definitions,
    Blobs,
    App { app_id: String, domain: String },
}

impl StorageDomainId {
    #[must_use]
    pub fn app(app_id: impl Into<String>, domain: impl Into<String>) -> Self {
        Self::App {
            app_id: app_id.into(),
            domain: domain.into(),
        }
    }

    #[must_use]
    pub fn logical_name(&self) -> String {
        match self {
            Self::Session => "session".to_string(),
            Self::Memory => "memory".to_string(),
            Self::Knowledge => "knowledge".to_string(),
            Self::Fact => "fact".to_string(),
            Self::Matrix => "matrix".to_string(),
            Self::Tasks => "tasks".to_string(),
            Self::Audit => "audit".to_string(),
            Self::Growth => "growth".to_string(),
            Self::RuntimeEvents => "runtime_events".to_string(),
            Self::SurfaceMessages => "surface_messages".to_string(),
            Self::ConnectorDirectory => "connector_directory".to_string(),
            Self::AuditLog => "audit_log".to_string(),
            Self::Definitions => "definitions".to_string(),
            Self::Blobs => "blobs".to_string(),
            Self::App { app_id, domain } => format!("app:{app_id}:{domain}"),
        }
    }
}

/// The data-isolation scope of a durable storage endpoint.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StorageScope {
    Global,
    Workspace { key: String },
    App { app_id: String },
}

impl StorageScope {
    #[must_use]
    pub fn workspace_key_for_root(root: impl AsRef<Path>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(root.as_ref().as_os_str().as_encoded_bytes());
        let digest = format!("{:x}", hasher.finalize());
        digest[..24].to_string()
    }

    #[must_use]
    pub fn workspace_for_root(root: impl AsRef<Path>) -> Self {
        Self::Workspace {
            key: Self::workspace_key_for_root(root),
        }
    }
}

/// A resolved physical target for one durable domain. `path` is private to
/// backend adapters; application composition should pass endpoints, not paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEndpoint {
    pub domain: StorageDomainId,
    pub scope: StorageScope,
    pub backend: StorageBackendKind,
    pub path: PathBuf,
    pub owner: String,
    pub migration: String,
}

impl StorageEndpoint {
    #[must_use]
    pub fn new(
        domain: StorageDomainId,
        scope: StorageScope,
        backend: StorageBackendKind,
        path: impl Into<PathBuf>,
        owner: impl Into<String>,
        migration: impl Into<String>,
    ) -> Self {
        Self {
            domain,
            scope,
            backend,
            path: path.into(),
            owner: owner.into(),
            migration: migration.into(),
        }
    }

    /// A PostgreSQL endpoint carries no connection URL in the registry. The
    /// embedding host resolves its secret reference into a `PostgresExecutor`
    /// at composition time, so inventory and health cannot leak credentials.
    #[must_use]
    pub fn postgres(
        domain: StorageDomainId,
        scope: StorageScope,
        owner: impl Into<String>,
        migration: impl Into<String>,
    ) -> Self {
        Self::new(
            domain,
            scope,
            StorageBackendKind::Postgres,
            PathBuf::new(),
            owner,
            migration,
        )
    }

    #[must_use]
    pub fn as_handle(&self) -> StorageHandle {
        StorageHandle {
            domain: self.domain.logical_name(),
            backend: self.backend.clone(),
            path: self.path.clone(),
            owner: self.owner.clone(),
            migration: self.migration.clone(),
        }
    }

    #[must_use]
    pub fn logical_id(&self) -> String {
        match &self.scope {
            StorageScope::Global => self.domain.logical_name(),
            StorageScope::Workspace { key } => {
                format!("{}@workspace:{key}", self.domain.logical_name())
            }
            StorageScope::App { app_id } => format!("{}@app:{app_id}", self.domain.logical_name()),
        }
    }

    /// Deterministic PostgreSQL schema owned by one APP storage endpoint.
    /// The hash includes scope, so the same domain in separate workspaces
    /// cannot collide.  No deployment secret participates in the name.
    pub fn app_postgres_namespace(&self) -> Result<String, StorageError> {
        let StorageDomainId::App { app_id, domain } = &self.domain else {
            return Err(StorageError::Other(format!(
                "storage endpoint `{}` is not an application domain",
                self.logical_id()
            )));
        };
        if self.backend != StorageBackendKind::Postgres {
            return Err(StorageError::Other(format!(
                "storage endpoint `{}` is not postgres-backed",
                self.logical_id()
            )));
        }
        let mut digest = Sha256::new();
        digest.update(self.logical_id().as_bytes());
        let hash = format!("{:x}", digest.finalize());
        let segment = |value: &str| {
            value
                .chars()
                .map(|character| {
                    if character.is_ascii_lowercase() || character.is_ascii_digit() {
                        character
                    } else {
                        '_'
                    }
                })
                .take(20)
                .collect::<String>()
        };
        Ok(format!(
            "cowd_app_{}_{}_{}",
            segment(app_id),
            segment(domain),
            &hash[..12]
        ))
    }
}

fn endpoint_from_handle(handle: &StorageHandle) -> StorageEndpoint {
    let domain = match handle.domain.as_str() {
        "session" => StorageDomainId::Session,
        "memory" => StorageDomainId::Memory,
        "knowledge" => StorageDomainId::Knowledge,
        "fact" => StorageDomainId::Fact,
        "matrix" => StorageDomainId::Matrix,
        "tasks" => StorageDomainId::Tasks,
        "audit" => StorageDomainId::Audit,
        "growth" => StorageDomainId::Growth,
        "audit_log" => StorageDomainId::AuditLog,
        "definitions" => StorageDomainId::Definitions,
        "blobs" => StorageDomainId::Blobs,
        other => StorageDomainId::app("storage", other),
    };
    let scope = match &domain {
        StorageDomainId::App { app_id, .. } if app_id != "storage" => StorageScope::App {
            app_id: app_id.clone(),
        },
        _ => StorageScope::Global,
    };
    StorageEndpoint::new(
        domain,
        scope,
        handle.backend.clone(),
        handle.path.clone(),
        handle.owner.clone(),
        handle.migration.clone(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHandle {
    pub domain: String,
    pub backend: StorageBackendKind,
    pub path: PathBuf,
    pub owner: String,
    pub migration: String,
}

impl StorageHandle {
    #[must_use]
    pub fn postgres(
        domain: impl Into<String>,
        owner: impl Into<String>,
        migration: impl Into<String>,
    ) -> Self {
        Self {
            domain: domain.into(),
            backend: StorageBackendKind::Postgres,
            path: PathBuf::new(),
            owner: owner.into(),
            migration: migration.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub files: BTreeMap<String, PathBuf>,
    /// Named, non-blob directory roots. Domains that retain immutable
    /// revision trees must be registered here instead of deriving an ad-hoc
    /// path from a configuration directory.
    pub directories: BTreeMap<String, PathBuf>,
    pub blobs: PathBuf,
}

impl StorageLayout {
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        let root = config_home.as_ref().join("storage");
        let files_root = root.join("files");
        let files = BTreeMap::from([("audit_log".to_string(), files_root.join("audit.jsonl"))]);
        let directories = BTreeMap::from([(
            "definitions".to_string(),
            config_home.as_ref().join("definitions"),
        )]);
        Self {
            root: root.clone(),
            files,
            directories,
            blobs: root.join("blobs"),
        }
    }

    pub fn file_path(&self, domain: &str) -> Option<&Path> {
        self.files.get(domain).map(PathBuf::as_path)
    }

    pub fn directory_path(&self, domain: &str) -> Option<&Path> {
        self.directories.get(domain).map(PathBuf::as_path)
    }

    pub fn ensure_directories(&self) -> Result<(), StorageError> {
        fs::create_dir_all(&self.root)?;
        fs::create_dir_all(self.root.join("files"))?;
        fs::create_dir_all(&self.blobs)?;
        for path in self.files.values() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
        }
        for path in self.directories.values() {
            fs::create_dir_all(path)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageRegistry {
    pub layout: StorageLayout,
    pub endpoints: Vec<StorageEndpoint>,
}

impl StorageRegistry {
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        Self::postgres_for_config_home(config_home)
    }

    /// Build the canonical PostgreSQL inventory without first materializing
    /// historical SQLite endpoints. Connection secrets remain owned by the
    /// composition root; registry entries carry only logical ownership.
    #[must_use]
    pub fn postgres_for_config_home(config_home: impl AsRef<Path>) -> Self {
        let layout = StorageLayout::default_for_config_home(config_home);
        let mut endpoints = [
            StorageDomainId::Session,
            StorageDomainId::Memory,
            StorageDomainId::Knowledge,
            StorageDomainId::Fact,
            StorageDomainId::Growth,
            StorageDomainId::Matrix,
            StorageDomainId::Tasks,
            StorageDomainId::SurfaceMessages,
        ]
        .into_iter()
        .map(|domain| {
            StorageEndpoint::postgres(
                domain,
                StorageScope::Global,
                "cowd-selected-storage",
                "postgres-only-since-0.9.723",
            )
        })
        .collect::<Vec<_>>();
        for (domain, path) in &layout.files {
            endpoints.push(endpoint_from_handle(&StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::FileJson,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "file_path_registered_since_0.9.295".to_string(),
            }));
        }
        for (domain, path) in &layout.directories {
            endpoints.push(endpoint_from_handle(&StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::Directory,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "directory_registered_since_0.9.484".to_string(),
            }));
        }
        endpoints.push(endpoint_from_handle(&StorageHandle {
            domain: "blobs".to_string(),
            backend: StorageBackendKind::BlobDirectory,
            path: layout.blobs.clone(),
            owner: "storage".to_string(),
            migration: "blob_root_registered_since_0.9.295".to_string(),
        }));
        endpoints.sort_by_key(StorageEndpoint::logical_id);
        Self { layout, endpoints }
    }

    pub fn from_layout(layout: StorageLayout) -> Self {
        let mut endpoints = Vec::new();
        for domain in [
            StorageDomainId::Session,
            StorageDomainId::Memory,
            StorageDomainId::Knowledge,
            StorageDomainId::Fact,
            StorageDomainId::Growth,
            StorageDomainId::Matrix,
            StorageDomainId::Tasks,
            StorageDomainId::SurfaceMessages,
        ] {
            endpoints.push(StorageEndpoint::postgres(
                domain,
                StorageScope::Global,
                "cowd-selected-storage",
                "postgres-only-since-0.9.723",
            ));
        }
        for (domain, path) in &layout.files {
            endpoints.push(endpoint_from_handle(&StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::FileJson,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "file_path_registered_since_0.9.295".to_string(),
            }));
        }
        for (domain, path) in &layout.directories {
            endpoints.push(endpoint_from_handle(&StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::Directory,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "directory_registered_since_0.9.484".to_string(),
            }));
        }
        endpoints.push(endpoint_from_handle(&StorageHandle {
            domain: "blobs".to_string(),
            backend: StorageBackendKind::BlobDirectory,
            path: layout.blobs.clone(),
            owner: "storage".to_string(),
            migration: "blob_root_registered_since_0.9.295".to_string(),
        }));
        endpoints.sort_by_key(StorageEndpoint::logical_id);
        Self { layout, endpoints }
    }

    pub fn endpoint(&self, domain: &StorageDomainId) -> Result<&StorageEndpoint, StorageError> {
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.domain == *domain && endpoint.scope == StorageScope::Global)
            .ok_or_else(|| {
                StorageError::Other(format!(
                    "storage endpoint `{}` in global scope is not registered",
                    domain.logical_name()
                ))
            })
    }

    pub fn endpoint_in_scope(
        &self,
        domain: &StorageDomainId,
        scope: &StorageScope,
    ) -> Result<&StorageEndpoint, StorageError> {
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.domain == *domain && endpoint.scope == *scope)
            .ok_or_else(|| {
                StorageError::Other(format!(
                    "storage endpoint `{}` is not registered for scope {:?}",
                    domain.logical_name(),
                    scope
                ))
            })
    }

    pub fn register_endpoint(&mut self, endpoint: StorageEndpoint) -> Result<(), StorageError> {
        if self
            .endpoints
            .iter()
            .any(|current| current.domain == endpoint.domain && current.scope == endpoint.scope)
        {
            return Err(StorageError::Other(format!(
                "storage endpoint collision: {}",
                endpoint.logical_id()
            )));
        }
        self.endpoints.push(endpoint);
        self.endpoints.sort_by_key(StorageEndpoint::logical_id);
        Ok(())
    }

    pub fn replace_endpoint(&mut self, endpoint: StorageEndpoint) -> Result<(), StorageError> {
        let Some(index) = self.endpoints.iter().position(|current| {
            current.domain == endpoint.domain && current.scope == endpoint.scope
        }) else {
            return self.register_endpoint(endpoint);
        };
        self.endpoints[index] = endpoint;
        self.endpoints.sort_by_key(StorageEndpoint::logical_id);
        Ok(())
    }

    pub fn with_memory_root(mut self, root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let root = root.as_ref();
        self.replace_endpoint(StorageEndpoint::new(
            StorageDomainId::Blobs,
            StorageScope::Global,
            StorageBackendKind::BlobDirectory,
            root.join("blobs"),
            "memory",
            "memory_blob_root_override_registered_since_0.9.565",
        ))?;
        Ok(self)
    }

    /// Register one host-resolved APP endpoint.  The APP supplies only its
    /// logical domain and migration identity; the host supplies backend,
    /// scope and physical topology.
    pub fn with_app_storage(
        mut self,
        app_id: impl AsRef<str>,
        domain: impl AsRef<str>,
        scope: StorageScope,
        backend: StorageBackendKind,
        migration: impl Into<String>,
    ) -> Result<Self, StorageError> {
        self.register_app_storage(app_id, domain, scope, backend, migration)?;
        Ok(self)
    }

    pub fn register_app_storage(
        &mut self,
        app_id: impl AsRef<str>,
        domain: impl AsRef<str>,
        scope: StorageScope,
        backend: StorageBackendKind,
        migration: impl Into<String>,
    ) -> Result<(), StorageError> {
        let app_id = app_id.as_ref();
        let domain = domain.as_ref();
        if !is_storage_segment(app_id) || !is_storage_segment(domain) {
            return Err(StorageError::Other(format!(
                "invalid app storage identity `{app_id}:{domain}`"
            )));
        }
        match &scope {
            StorageScope::App {
                app_id: scoped_app_id,
            } if scoped_app_id == app_id => {}
            StorageScope::Workspace { .. } => {}
            _ => {
                return Err(StorageError::Other(format!(
                    "invalid scope for app storage identity `{app_id}:{domain}`"
                )))
            }
        }
        #[allow(
            clippy::unreachable,
            reason = "the exhaustive scope validator above rejects Global before path selection"
        )]
        let scoped_root = match &scope {
            StorageScope::App { .. } => self.layout.root.join("apps").join(app_id),
            StorageScope::Workspace { key } => self
                .layout
                .root
                .join("workspaces")
                .join(key)
                .join("apps")
                .join(app_id),
            StorageScope::Global => unreachable!("global APP scope was rejected above"),
        };
        let path = match backend {
            StorageBackendKind::Postgres => PathBuf::new(),
            StorageBackendKind::FileJson => scoped_root.join(format!("{domain}.json")),
            StorageBackendKind::Directory => scoped_root.join(domain),
            StorageBackendKind::BlobDirectory => scoped_root.join(format!("{domain}.blobs")),
        };
        self.register_endpoint(StorageEndpoint::new(
            StorageDomainId::app(app_id, domain),
            scope,
            backend,
            path,
            app_id,
            migration,
        ))?;
        Ok(())
    }

    pub fn with_workspace(self, workspace_root: impl AsRef<Path>) -> Result<Self, StorageError> {
        self.with_postgres_workspace(workspace_root)
    }

    /// Add workspace-scoped PostgreSQL owners and filesystem-only definition
    /// roots to a PostgreSQL registry. No retired database endpoint is ever
    /// inserted, even transiently.
    pub fn with_postgres_workspace(
        mut self,
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, StorageError> {
        let workspace_root = workspace_root.as_ref();
        let scope = StorageScope::workspace_for_root(workspace_root);
        for domain in [
            StorageDomainId::ConnectorDirectory,
            StorageDomainId::Tasks,
            StorageDomainId::RuntimeEvents,
        ] {
            self.register_endpoint(StorageEndpoint::postgres(
                domain,
                scope.clone(),
                "cowd-selected-storage",
                "postgres-workspace-only-since-0.9.723",
            ))?;
        }
        self.register_endpoint(StorageEndpoint::new(
            StorageDomainId::Definitions,
            scope,
            StorageBackendKind::Directory,
            workspace_root.join(".cowd").join("definitions"),
            "runtime",
            "workspace_definition_endpoint_since_0.9.565",
        ))?;
        Ok(self)
    }

    #[must_use]
    pub fn inventory(&self) -> Vec<StorageEndpoint> {
        self.endpoints.clone()
    }

    /// Materialize only the parent directories of resolved endpoints. Domain
    /// implementations remain responsible for schemas and migrations.
    pub fn ensure_directories(&self) -> Result<(), StorageError> {
        for endpoint in &self.endpoints {
            let directory = match endpoint.backend {
                StorageBackendKind::Directory | StorageBackendKind::BlobDirectory => {
                    endpoint.path.as_path()
                }
                StorageBackendKind::Postgres => continue,
                _ => endpoint.path.parent().unwrap_or_else(|| Path::new(".")),
            };
            fs::create_dir_all(directory)?;
        }
        Ok(())
    }

    pub fn health(&self) -> StorageHealth {
        StorageHealth::from_registry(self)
    }
}

fn owner_for_domain(domain: &str) -> &'static str {
    match domain {
        "session" => "session",
        "memory" => "memory",
        "knowledge" => "memory",
        "fact" => "fact-kernel",
        "matrix" => "matrix",
        "tasks" => "task",
        "audit" | "audit_log" => "audit",
        "growth" => "growth",
        "definitions" => "runtime",
        _ => "storage",
    }
}

fn is_storage_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHealth {
    pub status: String,
    pub root: PathBuf,
    pub endpoint_count: usize,
    pub present_count: usize,
    pub missing_count: usize,
    pub endpoints: Vec<StorageEndpointHealth>,
}

impl StorageHealth {
    pub fn from_registry(registry: &StorageRegistry) -> Self {
        let endpoints = registry
            .endpoints
            .iter()
            .map(StorageEndpointHealth::from_endpoint)
            .collect::<Vec<_>>();
        let present_count = endpoints.iter().filter(|endpoint| endpoint.present).count();
        let missing_count = endpoints.len().saturating_sub(present_count);
        Self {
            status: if missing_count == 0 {
                "ready".to_string()
            } else {
                "registered".to_string()
            },
            root: registry.layout.root.clone(),
            endpoint_count: endpoints.len(),
            present_count,
            missing_count,
            endpoints,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEndpointHealth {
    pub id: String,
    pub domain: StorageDomainId,
    pub scope: StorageScope,
    pub backend: StorageBackendKind,
    pub owner: String,
    pub present: bool,
    pub writable_parent: bool,
}

impl StorageEndpointHealth {
    fn from_endpoint(endpoint: &StorageEndpoint) -> Self {
        if endpoint.backend == StorageBackendKind::Postgres {
            return Self {
                id: endpoint.logical_id(),
                domain: endpoint.domain.clone(),
                scope: endpoint.scope.clone(),
                backend: endpoint.backend.clone(),
                owner: endpoint.owner.clone(),
                // Connection health is reported by PostgresExecutor. A
                // registry alone cannot and must not resolve a secret, so it
                // must not claim a remote endpoint is presently reachable.
                present: false,
                writable_parent: false,
            };
        }
        let parent = match endpoint.backend {
            StorageBackendKind::Directory | StorageBackendKind::BlobDirectory => {
                endpoint.path.as_path()
            }
            _ => endpoint.path.parent().unwrap_or_else(|| Path::new(".")),
        };
        Self {
            id: endpoint.logical_id(),
            domain: endpoint.domain.clone(),
            scope: endpoint.scope.clone(),
            backend: endpoint.backend.clone(),
            owner: endpoint.owner.clone(),
            present: endpoint.path.exists(),
            writable_parent: writable_directory_or_existing_ancestor(parent),
        }
    }
}

fn writable_directory_or_existing_ancestor(path: &Path) -> bool {
    let mut current = path;
    loop {
        if let Ok(metadata) = current.metadata() {
            return metadata.is_dir() && !metadata.permissions().readonly();
        }
        let Some(parent) = current.parent() else {
            return false;
        };
        current = parent;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_declares_postgres_business_domains() {
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config");
        for domain in [
            StorageDomainId::Session,
            StorageDomainId::Memory,
            StorageDomainId::Knowledge,
            StorageDomainId::Fact,
            StorageDomainId::Matrix,
            StorageDomainId::Tasks,
            StorageDomainId::Growth,
            StorageDomainId::SurfaceMessages,
        ] {
            let endpoint = registry
                .endpoint(&domain)
                .expect("registered business domain");
            assert_eq!(endpoint.backend, StorageBackendKind::Postgres);
            assert!(endpoint.path.as_os_str().is_empty());
        }
    }

    #[test]
    fn workspace_business_domains_are_postgres_and_definitions_remain_filesystem_owned() {
        let workspace = tempfile::tempdir().expect("workspace");
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config")
            .with_workspace(workspace.path())
            .expect("workspace registry");
        let scope = StorageScope::workspace_for_root(workspace.path());
        for domain in [
            StorageDomainId::ConnectorDirectory,
            StorageDomainId::Tasks,
            StorageDomainId::RuntimeEvents,
        ] {
            let endpoint = registry
                .endpoint_in_scope(&domain, &scope)
                .expect("workspace business domain");
            assert_eq!(endpoint.backend, StorageBackendKind::Postgres);
        }
        let definitions = registry
            .endpoint_in_scope(&StorageDomainId::Definitions, &scope)
            .expect("workspace definitions");
        assert_eq!(definitions.backend, StorageBackendKind::Directory);
    }

    #[test]
    fn app_postgres_namespaces_are_scope_stable_and_distinct() {
        let workspace = tempfile::tempdir().expect("workspace");
        let workspace_scope = StorageScope::workspace_for_root(workspace.path());
        let app_scope = StorageScope::App {
            app_id: "fixture".to_string(),
        };
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-app-storage")
            .with_app_storage(
                "fixture",
                "primary",
                app_scope.clone(),
                StorageBackendKind::Postgres,
                "fixture_primary_v1",
            )
            .expect("app endpoint")
            .with_app_storage(
                "fixture",
                "primary",
                workspace_scope.clone(),
                StorageBackendKind::Postgres,
                "fixture_workspace_primary_v1",
            )
            .expect("workspace app endpoint");
        let app = registry
            .endpoint_in_scope(&StorageDomainId::app("fixture", "primary"), &app_scope)
            .expect("app endpoint");
        let workspace_app = registry
            .endpoint_in_scope(
                &StorageDomainId::app("fixture", "primary"),
                &workspace_scope,
            )
            .expect("workspace app endpoint");
        assert_ne!(
            app.app_postgres_namespace().expect("app namespace"),
            workspace_app
                .app_postgres_namespace()
                .expect("workspace namespace")
        );
    }
}
