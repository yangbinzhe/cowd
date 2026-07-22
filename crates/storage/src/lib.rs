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

mod sqlite;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub use sqlite::SqliteStorage;
pub use sqlite::{SqliteConnectionFactory, SqlitePragmaConfig};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

pub trait StorageBackend: Send + Sync {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), StorageError>;
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    fn delete(&self, key: &str) -> Result<(), StorageError>;
    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    fn flush(&self) -> Result<(), StorageError>;
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
    Sqlite,
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
    Approval,
    Growth,
    RuntimeEvents,
    SurfaceMessages,
    ConnectorDirectory,
    ApprovalHistory,
    AlwaysApproved,
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
            Self::Approval => "approval".to_string(),
            Self::Growth => "growth".to_string(),
            Self::RuntimeEvents => "runtime_events".to_string(),
            Self::SurfaceMessages => "surface_messages".to_string(),
            Self::ConnectorDirectory => "connector_directory".to_string(),
            Self::ApprovalHistory => "approval_history".to_string(),
            Self::AlwaysApproved => "always_approved".to_string(),
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
    pub fn workspace_for_root(root: impl AsRef<Path>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(root.as_ref().as_os_str().as_encoded_bytes());
        let digest = format!("{:x}", hasher.finalize());
        Self::Workspace {
            key: digest[..24].to_string(),
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

    #[must_use]
    pub fn sqlite(
        domain: StorageDomainId,
        scope: StorageScope,
        path: impl Into<PathBuf>,
        owner: impl Into<String>,
        migration: impl Into<String>,
    ) -> Self {
        Self::new(
            domain,
            scope,
            StorageBackendKind::Sqlite,
            path,
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
        "approval" => StorageDomainId::Approval,
        "growth" => StorageDomainId::Growth,
        "approval_history" => StorageDomainId::ApprovalHistory,
        "always_approved" => StorageDomainId::AlwaysApproved,
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
    pub fn sqlite(
        domain: impl Into<String>,
        path: impl Into<PathBuf>,
        owner: impl Into<String>,
        migration: impl Into<String>,
    ) -> Self {
        Self {
            domain: domain.into(),
            backend: StorageBackendKind::Sqlite,
            path: path.into(),
            owner: owner.into(),
            migration: migration.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub sqlite: BTreeMap<String, PathBuf>,
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
        let sqlite = BTreeMap::from([
            ("session".to_string(), root.join("session.sqlite")),
            ("memory".to_string(), root.join("memory.sqlite")),
            ("knowledge".to_string(), root.join("knowledge.sqlite")),
            ("fact".to_string(), root.join("fact.sqlite")),
            ("matrix".to_string(), root.join("matrix.sqlite")),
            ("tasks".to_string(), root.join("tasks.sqlite")),
            ("audit".to_string(), root.join("audit.sqlite")),
            ("approval".to_string(), root.join("approval.sqlite")),
            ("growth".to_string(), root.join("growth.sqlite")),
        ]);
        let files_root = root.join("files");
        let files = BTreeMap::from([
            (
                "approval_history".to_string(),
                files_root.join("approval_history.json"),
            ),
            (
                "always_approved".to_string(),
                files_root.join("always_approved.json"),
            ),
            ("audit_log".to_string(), files_root.join("audit.jsonl")),
        ]);
        let directories = BTreeMap::from([(
            "definitions".to_string(),
            config_home.as_ref().join("definitions"),
        )]);
        Self {
            root: root.clone(),
            sqlite,
            files,
            directories,
            blobs: root.join("blobs"),
        }
    }

    pub fn sqlite_path(&self, domain: &str) -> Option<&Path> {
        self.sqlite.get(domain).map(PathBuf::as_path)
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
        for path in self.sqlite.values() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    pub backend: StorageBackendKind,
    pub layout: StorageLayout,
    pub sqlite: SqlitePragmaConfig,
}

impl StorageConfig {
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        Self {
            backend: StorageBackendKind::Sqlite,
            layout: StorageLayout::default_for_config_home(config_home),
            sqlite: SqlitePragmaConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageRegistry {
    pub layout: StorageLayout,
    pub endpoints: Vec<StorageEndpoint>,
}

impl StorageRegistry {
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        let layout = StorageLayout::default_for_config_home(config_home);
        Self::from_layout(layout)
    }

    pub fn from_layout(layout: StorageLayout) -> Self {
        let mut endpoints = Vec::new();
        for (domain, path) in &layout.sqlite {
            endpoints.push(endpoint_from_handle(&StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::Sqlite,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "managed_by_storage_layout_since_0.9.295".to_string(),
            }));
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
        endpoints.sort_by(|left, right| left.logical_id().cmp(&right.logical_id()));
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
        self.endpoints
            .sort_by(|left, right| left.logical_id().cmp(&right.logical_id()));
        Ok(())
    }

    pub fn replace_endpoint(&mut self, endpoint: StorageEndpoint) -> Result<(), StorageError> {
        let Some(index) = self.endpoints.iter().position(|current| {
            current.domain == endpoint.domain && current.scope == endpoint.scope
        }) else {
            return self.register_endpoint(endpoint);
        };
        self.endpoints[index] = endpoint;
        self.endpoints
            .sort_by(|left, right| left.logical_id().cmp(&right.logical_id()));
        Ok(())
    }

    pub fn with_memory_root(mut self, root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let root = root.as_ref();
        self.replace_endpoint(StorageEndpoint::sqlite(
            StorageDomainId::Memory,
            StorageScope::Global,
            root.join("memory.db"),
            "memory",
            "memory_root_override_registered_since_0.9.565",
        ))?;
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

    /// Register one APP-owned durable SQLite target using Cowd's generic
    /// location convention. The host never needs to know an APP's schema;
    /// it only provides a stable owner/scope/location envelope.
    pub fn with_app_sqlite(
        mut self,
        app_id: impl AsRef<str>,
        domain: impl AsRef<str>,
        migration: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let app_id = app_id.as_ref();
        let domain = domain.as_ref();
        if !is_storage_segment(app_id) || !is_storage_segment(domain) {
            return Err(StorageError::Other(format!(
                "invalid app storage identity `{app_id}:{domain}`"
            )));
        }
        self.register_endpoint(StorageEndpoint::sqlite(
            StorageDomainId::app(app_id, domain),
            StorageScope::App {
                app_id: app_id.to_string(),
            },
            self.layout
                .root
                .join("apps")
                .join(app_id)
                .join(format!("{domain}.sqlite")),
            app_id,
            migration,
        ))?;
        Ok(self)
    }

    pub fn with_workspace(
        mut self,
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, StorageError> {
        let workspace_root = workspace_root.as_ref();
        let scope = StorageScope::workspace_for_root(workspace_root);
        self.register_endpoint(StorageEndpoint::sqlite(
            StorageDomainId::ConnectorDirectory,
            scope.clone(),
            workspace_root
                .join(".cowd")
                .join("storage")
                .join("resource-directory.sqlite"),
            "connector",
            "workspace_scoped_storage_endpoint_since_0.9.565",
        ))?;
        self.register_endpoint(StorageEndpoint::new(
            StorageDomainId::Definitions,
            scope.clone(),
            StorageBackendKind::Directory,
            workspace_root.join(".cowd").join("definitions"),
            "runtime",
            "workspace_definition_endpoint_since_0.9.565",
        ))?;
        let StorageScope::Workspace { key } = scope else {
            unreachable!("workspace_for_root always creates a workspace scope");
        };
        self.register_endpoint(StorageEndpoint::sqlite(
            StorageDomainId::RuntimeEvents,
            StorageScope::Workspace { key: key.clone() },
            self.layout
                .root
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("runtime")
                .join("workspaces")
                .join(key)
                .join("runtime-events.sqlite"),
            "runtime",
            "workspace_runtime_event_endpoint_since_0.9.565",
        ))?;
        Ok(self)
    }

    pub fn with_surface_messages(self) -> Result<Self, StorageError> {
        let config_home = self
            .layout
            .root
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        self.with_surface_message_root(config_home.join("surface-messages"))
    }

    pub fn with_surface_message_root(
        mut self,
        root: impl AsRef<Path>,
    ) -> Result<Self, StorageError> {
        self.register_endpoint(StorageEndpoint::sqlite(
            StorageDomainId::SurfaceMessages,
            StorageScope::Global,
            root.as_ref().join("surface_messages.sqlite3"),
            "surface",
            "surface_message_endpoint_since_0.9.565",
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
        "approval" | "approval_history" | "always_approved" => "approval",
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRunner {
    pub migrations: Vec<StorageMigration>,
}

impl MigrationRunner {
    pub fn from_registry(registry: &StorageRegistry) -> Self {
        Self {
            migrations: registry
                .endpoints
                .iter()
                .map(|endpoint| StorageMigration {
                    id: format!("storage.{}.endpoint", endpoint.logical_id()),
                    domain: endpoint.domain.logical_name(),
                    version: 0,
                    status: "registered".to_string(),
                    target: endpoint.path.clone(),
                    description: endpoint.migration.clone(),
                    error: None,
                })
                .collect(),
        }
    }

    pub fn status(&self) -> Vec<StorageMigration> {
        self.migrations.clone()
    }

    pub fn run_sqlite_domain(
        connection: &rusqlite::Connection,
        handle: &StorageHandle,
        specs: &[StorageMigrationSpec],
    ) -> Result<Vec<StorageMigration>, StorageError> {
        if handle.backend != StorageBackendKind::Sqlite {
            return Err(StorageError::Other(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        ensure_schema_migrations_table(connection)?;
        let mut reports = Vec::new();
        for spec in specs {
            if spec.domain != handle.domain {
                return Err(StorageError::Other(format!(
                    "migration `{}` targets `{}` but handle domain is `{}`",
                    spec.id, spec.domain, handle.domain
                )));
            }
            reports.push(run_migration_spec(connection, handle, spec)?);
        }
        Ok(reports)
    }

    pub fn inspect_sqlite_domain(
        connection: &rusqlite::Connection,
        handle: &StorageHandle,
        specs: &[StorageMigrationSpec],
    ) -> Result<Vec<StorageMigration>, StorageError> {
        if handle.backend != StorageBackendKind::Sqlite {
            return Err(StorageError::Other(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        let schema_table_exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        let mut reports = Vec::new();
        for spec in specs {
            if spec.domain != handle.domain {
                return Err(StorageError::Other(format!(
                    "migration `{}` targets `{}` but handle domain is `{}`",
                    spec.id, spec.domain, handle.domain
                )));
            }
            let applied = if schema_table_exists {
                connection
                    .query_row(
                        "SELECT 1 FROM schema_migrations WHERE id = ?1",
                        rusqlite::params![spec.id],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .is_some()
            } else {
                false
            };
            reports.push(StorageMigration {
                id: spec.id.to_string(),
                domain: spec.domain.to_string(),
                version: spec.version,
                status: if applied {
                    "applied".to_string()
                } else {
                    "pending".to_string()
                },
                target: handle.path.clone(),
                description: spec.description.to_string(),
                error: None,
            });
        }
        Ok(reports)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMigration {
    pub id: String,
    pub domain: String,
    pub version: i64,
    pub status: String,
    pub target: PathBuf,
    pub description: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMigrationSpec {
    pub id: &'static str,
    pub domain: &'static str,
    pub version: i64,
    pub description: &'static str,
    pub statements: &'static [&'static str],
}

fn ensure_schema_migrations_table(connection: &rusqlite::Connection) -> Result<(), StorageError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            version INTEGER NOT NULL,
            description TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );",
    )?;
    Ok(())
}

fn run_migration_spec(
    connection: &rusqlite::Connection,
    handle: &StorageHandle,
    spec: &StorageMigrationSpec,
) -> Result<StorageMigration, StorageError> {
    let already_applied = connection
        .query_row(
            "SELECT 1 FROM schema_migrations WHERE id = ?1",
            rusqlite::params![spec.id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if already_applied {
        return Ok(StorageMigration {
            id: spec.id.to_string(),
            domain: spec.domain.to_string(),
            version: spec.version,
            status: "already_applied".to_string(),
            target: handle.path.clone(),
            description: spec.description.to_string(),
            error: None,
        });
    }

    for statement in spec.statements {
        if let Err(error) = connection.execute_batch(statement) {
            return Ok(StorageMigration {
                id: spec.id.to_string(),
                domain: spec.domain.to_string(),
                version: spec.version,
                status: "failed".to_string(),
                target: handle.path.clone(),
                description: spec.description.to_string(),
                error: Some(error.to_string()),
            });
        }
    }
    connection.execute(
        "INSERT INTO schema_migrations(id, domain, version, description, applied_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            spec.id,
            spec.domain,
            spec.version,
            spec.description,
            applied_at_now()
        ],
    )?;
    Ok(StorageMigration {
        id: spec.id.to_string(),
        domain: spec.domain.to_string(),
        version: spec.version,
        status: "applied".to_string(),
        target: handle.path.clone(),
        description: spec.description.to_string(),
        error: None,
    })
}

fn applied_at_now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLockDiagnostics {
    pub domain: String,
    pub path: PathBuf,
    pub backend: StorageBackendKind,
    pub status: String,
    pub exists: bool,
    pub wal_exists: bool,
    pub shm_exists: bool,
    pub locked_or_busy: bool,
    pub busy_timeout_ms: u64,
    pub last_error: Option<String>,
    pub suggested_action: Option<String>,
}

impl StorageLockDiagnostics {
    pub fn for_handle(handle: &StorageHandle, busy_timeout_ms: u64) -> Self {
        let path = handle.path.clone();
        let (status, locked_or_busy, last_error, suggested_action) =
            diagnose_sqlite_path(&path, &handle.backend, busy_timeout_ms);
        Self {
            domain: handle.domain.clone(),
            backend: handle.backend.clone(),
            status,
            locked_or_busy,
            last_error,
            suggested_action,
            wal_exists: path
                .with_extension(format!(
                    "{}-wal",
                    path.extension()
                        .and_then(|ext| ext.to_str())
                        .unwrap_or("sqlite")
                ))
                .exists()
                || PathBuf::from(format!("{}-wal", path.display())).exists(),
            shm_exists: path
                .with_extension(format!(
                    "{}-shm",
                    path.extension()
                        .and_then(|ext| ext.to_str())
                        .unwrap_or("sqlite")
                ))
                .exists()
                || PathBuf::from(format!("{}-shm", path.display())).exists(),
            exists: path.exists(),
            path,
            busy_timeout_ms,
        }
    }
}

fn diagnose_sqlite_path(
    path: &Path,
    backend: &StorageBackendKind,
    busy_timeout_ms: u64,
) -> (String, bool, Option<String>, Option<String>) {
    if !matches!(backend, StorageBackendKind::Sqlite) {
        return ("not_sqlite".to_string(), false, None, None);
    }
    if !path.exists() {
        return (
            "missing".to_string(),
            false,
            None,
            Some("database will be created by the owning runtime service".to_string()),
        );
    }
    match rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY) {
        Ok(connection) => {
            let _ = connection.busy_timeout(std::time::Duration::from_millis(busy_timeout_ms));
            if let Err(error) = sqlite_read_probe(&connection) {
                let message = error.to_string();
                return (
                    "error".to_string(),
                    is_busy_or_locked(&message),
                    Some(format!("read_probe failed: {message}")),
                    Some("retry after active writer completes or inspect stale locks".to_string()),
                );
            }

            match connection.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0)) {
                Ok(value) if value.eq_ignore_ascii_case("ok") => {
                    ("ok".to_string(), false, None, None)
                }
                Ok(value)
                    if is_readonly_fts_quick_check_limitation(&value)
                        && sqlite_has_fts_tables(&connection) =>
                {
                    readonly_fts_quick_check_skipped()
                }
                Ok(value) => (
                    "degraded".to_string(),
                    false,
                    Some(format!("quick_check={value}")),
                    Some("inspect sqlite integrity before writes".to_string()),
                ),
                Err(error) => {
                    let message = error.to_string();
                    if is_readonly_fts_quick_check_limitation(&message)
                        && sqlite_has_fts_tables(&connection)
                    {
                        return readonly_fts_quick_check_skipped();
                    }
                    (
                        "error".to_string(),
                        is_busy_or_locked(&message),
                        Some(format!("quick_check failed: {message}")),
                        Some(
                            "retry after active writer completes or inspect stale locks"
                                .to_string(),
                        ),
                    )
                }
            }
        }
        Err(error) => (
            "error".to_string(),
            is_busy_or_locked(&error),
            Some(error.to_string()),
            Some("check file permissions and active sqlite writers".to_string()),
        ),
    }
}

fn readonly_fts_quick_check_skipped() -> (String, bool, Option<String>, Option<String>) {
    (
        "ok".to_string(),
        false,
        Some("quick_check_skipped_for_readonly_fts".to_string()),
        Some(
            "sqlite FTS quick_check requires write access on this database; read probe passed"
                .to_string(),
        ),
    )
}

fn is_readonly_fts_quick_check_limitation(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("attempt to write a readonly database")
        || (message.contains("unable to validate the inverted index")
            && message.contains("readonly database"))
}

fn sqlite_read_probe(connection: &rusqlite::Connection) -> rusqlite::Result<()> {
    connection.query_row("SELECT COUNT(*) FROM sqlite_master", [], |_row| Ok(()))?;
    connection.query_row("PRAGMA database_list", [], |_row| Ok(()))?;
    Ok(())
}

fn sqlite_has_fts_tables(connection: &rusqlite::Connection) -> bool {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table'
                  AND lower(COALESCE(sql, '')) LIKE '%virtual table%'
                  AND lower(COALESCE(sql, '')) LIKE '%using fts%'
            )",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)
        .unwrap_or(false)
}

fn is_busy_or_locked(error: &impl std::fmt::Display) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("busy") || message.contains("locked")
}

pub enum StorageType {
    Sqlite { path: PathBuf },
}

pub fn create_storage(st: StorageType) -> Result<Box<dyn StorageBackend>, StorageError> {
    match st {
        StorageType::Sqlite { path } => {
            SqliteStorage::open(path).map(|s| Box::new(s) as Box<dyn StorageBackend>)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_layout_uses_storage_subdirectory() {
        let layout = StorageLayout::default_for_config_home("/tmp/cowd-config");
        assert_eq!(
            layout.sqlite_path("session").unwrap(),
            Path::new("/tmp/cowd-config/storage/session.sqlite")
        );
        assert_eq!(
            layout.file_path("approval_history").unwrap(),
            Path::new("/tmp/cowd-config/storage/files/approval_history.json")
        );
        assert_eq!(
            layout.directory_path("definitions").unwrap(),
            Path::new("/tmp/cowd-config/definitions")
        );
        assert_eq!(layout.blobs, Path::new("/tmp/cowd-config/storage/blobs"));
    }

    #[test]
    fn registry_declares_core_domains() {
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config");
        for domain in [
            "session",
            "memory",
            "matrix",
            "tasks",
            "audit",
            "approval",
            "growth",
            "definitions",
            "approval_history",
            "always_approved",
            "audit_log",
            "blobs",
        ] {
            assert!(
                registry
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint.domain.logical_name() == domain),
                "missing {domain}"
            );
        }
        assert!(registry
            .endpoint(&StorageDomainId::Session)
            .is_ok_and(|endpoint| endpoint.scope == StorageScope::Global));
        assert!(registry
            .endpoint(&StorageDomainId::ConnectorDirectory)
            .is_err());
    }

    #[test]
    fn registry_resolves_workspace_and_app_endpoints_without_core_special_cases() {
        let workspace = tempfile::tempdir().unwrap();
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config")
            .with_workspace(workspace.path())
            .unwrap()
            .with_app_sqlite("mfg", "primary", "test")
            .unwrap();
        let workspace_scope = StorageScope::workspace_for_root(workspace.path());
        let connector = registry
            .endpoint_in_scope(&StorageDomainId::ConnectorDirectory, &workspace_scope)
            .unwrap();
        assert!(connector
            .as_handle()
            .path
            .ends_with(".cowd/storage/resource-directory.sqlite"));
        let mfg_scope = StorageScope::App {
            app_id: "mfg".to_string(),
        };
        let mfg = registry
            .endpoint_in_scope(&StorageDomainId::app("mfg", "primary"), &mfg_scope)
            .unwrap();
        assert!(mfg
            .as_handle()
            .path
            .ends_with("storage/apps/mfg/primary.sqlite"));
        assert!(registry
            .with_app_sqlite("mfg", "primary", "duplicate")
            .is_err());
    }

    #[test]
    fn endpoint_health_is_logical_and_does_not_expose_paths() {
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config");
        let value = serde_json::to_value(registry.health()).unwrap();
        let endpoint = value["endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .find(|endpoint| endpoint["id"] == "session")
            .unwrap();
        assert!(endpoint.get("path").is_none());
    }

    #[test]
    fn migration_runner_reports_registered_targets() {
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config");
        let runner = MigrationRunner::from_registry(&registry);
        assert_eq!(runner.status().len(), registry.endpoints.len());
        assert!(runner
            .status()
            .iter()
            .any(|migration| migration.id == "storage.matrix.endpoint"));
    }

    #[test]
    fn migration_runner_applies_and_records_sqlite_migration() {
        let dir = tempfile::tempdir().unwrap();
        let handle =
            StorageHandle::sqlite("growth", dir.path().join("growth.sqlite"), "growth", "test");
        let connection = SqliteConnectionFactory::default()
            .open_handle(&handle)
            .unwrap();
        let specs = [StorageMigrationSpec {
            id: "growth.v1.init",
            domain: "growth",
            version: 1,
            description: "init growth schema",
            statements: &["CREATE TABLE growth_events(event_id TEXT PRIMARY KEY);"],
        }];
        let reports = MigrationRunner::run_sqlite_domain(&connection, &handle, &specs).unwrap();
        assert_eq!(reports[0].status, "applied");
        let applied: String = connection
            .query_row(
                "SELECT id FROM schema_migrations WHERE id = 'growth.v1.init'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(applied, "growth.v1.init");
    }

    #[test]
    fn migration_runner_skips_already_applied_migration() {
        let dir = tempfile::tempdir().unwrap();
        let handle =
            StorageHandle::sqlite("growth", dir.path().join("growth.sqlite"), "growth", "test");
        let connection = SqliteConnectionFactory::default()
            .open_handle(&handle)
            .unwrap();
        let specs = [StorageMigrationSpec {
            id: "growth.v1.init",
            domain: "growth",
            version: 1,
            description: "init growth schema",
            statements: &["CREATE TABLE growth_events(event_id TEXT PRIMARY KEY);"],
        }];
        let first = MigrationRunner::run_sqlite_domain(&connection, &handle, &specs).unwrap();
        let second = MigrationRunner::run_sqlite_domain(&connection, &handle, &specs).unwrap();
        assert_eq!(first[0].status, "applied");
        assert_eq!(second[0].status, "already_applied");
    }

    #[test]
    fn migration_runner_reports_failed_migration_without_recording_it() {
        let dir = tempfile::tempdir().unwrap();
        let handle =
            StorageHandle::sqlite("growth", dir.path().join("growth.sqlite"), "growth", "test");
        let connection = SqliteConnectionFactory::default()
            .open_handle(&handle)
            .unwrap();
        let specs = [StorageMigrationSpec {
            id: "growth.v1.bad",
            domain: "growth",
            version: 1,
            description: "bad growth schema",
            statements: &["CREATE TABLE broken ("],
        }];
        let reports = MigrationRunner::run_sqlite_domain(&connection, &handle, &specs).unwrap();
        assert_eq!(reports[0].status, "failed");
        assert!(reports[0].error.is_some());
        let recorded = connection
            .query_row(
                "SELECT id FROM schema_migrations WHERE id = 'growth.v1.bad'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap();
        assert!(recorded.is_none());
    }

    #[test]
    fn migration_runner_inspects_pending_and_applied_migrations() {
        let dir = tempfile::tempdir().unwrap();
        let handle =
            StorageHandle::sqlite("growth", dir.path().join("growth.sqlite"), "growth", "test");
        let connection = SqliteConnectionFactory::default()
            .open_handle(&handle)
            .unwrap();
        let specs = [StorageMigrationSpec {
            id: "growth.v1.init",
            domain: "growth",
            version: 1,
            description: "init growth schema",
            statements: &["CREATE TABLE growth_events(event_id TEXT PRIMARY KEY);"],
        }];
        let pending = MigrationRunner::inspect_sqlite_domain(&connection, &handle, &specs).unwrap();
        assert_eq!(pending[0].status, "pending");
        MigrationRunner::run_sqlite_domain(&connection, &handle, &specs).unwrap();
        let applied = MigrationRunner::inspect_sqlite_domain(&connection, &handle, &specs).unwrap();
        assert_eq!(applied[0].status, "applied");
    }

    #[test]
    fn health_reports_missing_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        let registry = StorageRegistry::default_for_config_home(dir.path());
        let health = registry.health();
        assert_eq!(health.status, "registered");
        assert_eq!(health.endpoint_count, registry.endpoints.len());
        assert!(health.endpoint_count >= 13);
        assert!(health.missing_count > 0);
    }

    #[test]
    fn sqlite_storage_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite");
        let s = SqliteStorage::open(&path).unwrap();
        s.write("k1", b"hello").unwrap();
        assert_eq!(s.read("k1").unwrap().unwrap(), b"hello");
    }

    #[test]
    fn lock_diagnostics_report_ok_for_existing_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite");
        let _ = SqliteStorage::open(&path).unwrap();
        let handle = StorageHandle {
            domain: "memory".to_string(),
            backend: StorageBackendKind::Sqlite,
            path,
            owner: "memory".to_string(),
            migration: "test".to_string(),
        };
        let diagnostics = StorageLockDiagnostics::for_handle(&handle, 5_000);
        assert_eq!(diagnostics.status, "ok");
        assert!(!diagnostics.locked_or_busy);
        assert_eq!(diagnostics.busy_timeout_ms, 5_000);
    }

    #[test]
    fn lock_diagnostics_do_not_mark_readable_fts_database_degraded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory.sqlite");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "CREATE VIRTUAL TABLE code_symbols_fts USING fts5(symbol, body);
                     INSERT INTO code_symbols_fts(symbol, body) VALUES ('main', 'fn main() {}');",
                )
                .unwrap();
        }

        let handle = StorageHandle {
            domain: "memory".to_string(),
            backend: StorageBackendKind::Sqlite,
            path,
            owner: "memory".to_string(),
            migration: "test".to_string(),
        };
        let diagnostics = StorageLockDiagnostics::for_handle(&handle, 5_000);

        assert_eq!(diagnostics.status, "ok");
        assert!(!diagnostics.locked_or_busy);
    }

    #[test]
    fn readonly_fts_quick_check_message_is_classified_as_limitation() {
        assert!(is_readonly_fts_quick_check_limitation(
            "quick_check=unable to validate the inverted index for FTS5 table main.sessions_fts: attempt to write a readonly database"
        ));
        assert!(is_readonly_fts_quick_check_limitation(
            "unable to validate the inverted index for FTS5 table main.sessions_fts: readonly database"
        ));
        assert!(!is_readonly_fts_quick_check_limitation(
            "database disk image is malformed"
        ));
    }

    #[test]
    fn direct_open_allowlist_is_machine_readable() {
        let raw = include_str!("../direct-open-allowlist.json");
        let entries: Vec<serde_json::Value> = serde_json::from_str(raw).unwrap();
        for entry in &entries {
            for key in [
                "file",
                "direct_open",
                "owner",
                "replacement",
                "status",
                "allowed_until_test",
            ] {
                assert!(entry.get(key).and_then(serde_json::Value::as_str).is_some());
            }
            let stale_deadline_key = ["delete", "_by"].concat();
            assert!(entry.get(&stale_deadline_key).is_none());
            let retired_status = ["owned_boundary", "_reviewed_", "0.9.305"].concat();
            assert_ne!(entry["status"].as_str().unwrap(), retired_status.as_str());
            assert_ne!(
                entry["allowed_until_test"].as_str().unwrap(),
                "cargo test -p memory --no-default-features -- --nocapture"
            );
        }
    }
}
