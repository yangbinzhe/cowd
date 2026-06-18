mod sqlite;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
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
    BlobDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHandle {
    pub domain: String,
    pub backend: StorageBackendKind,
    pub path: PathBuf,
    pub owner: String,
    pub migration: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub sqlite: BTreeMap<String, PathBuf>,
    pub files: BTreeMap<String, PathBuf>,
    pub blobs: PathBuf,
}

impl StorageLayout {
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        let root = config_home.as_ref().join("storage");
        let sqlite = BTreeMap::from([
            ("session".to_string(), root.join("session.sqlite")),
            ("memory".to_string(), root.join("memory.sqlite")),
            ("matrix".to_string(), root.join("matrix.sqlite")),
            (
                "resource_directory".to_string(),
                root.join("resource-directory.sqlite"),
            ),
            ("tasks".to_string(), root.join("tasks.sqlite")),
            ("audit".to_string(), root.join("audit.sqlite")),
            ("approval".to_string(), root.join("approval.sqlite")),
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
        Self {
            root: root.clone(),
            sqlite,
            files,
            blobs: root.join("blobs"),
        }
    }

    pub fn sqlite_path(&self, domain: &str) -> Option<&Path> {
        self.sqlite.get(domain).map(PathBuf::as_path)
    }

    pub fn file_path(&self, domain: &str) -> Option<&Path> {
        self.files.get(domain).map(PathBuf::as_path)
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
    pub handles: Vec<StorageHandle>,
}

impl StorageRegistry {
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        let layout = StorageLayout::default_for_config_home(config_home);
        Self::from_layout(layout)
    }

    pub fn from_layout(layout: StorageLayout) -> Self {
        let mut handles = Vec::new();
        for (domain, path) in &layout.sqlite {
            handles.push(StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::Sqlite,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "managed_by_storage_layout_since_0.9.295".to_string(),
            });
        }
        for (domain, path) in &layout.files {
            handles.push(StorageHandle {
                domain: domain.clone(),
                backend: StorageBackendKind::FileJson,
                path: path.clone(),
                owner: owner_for_domain(domain).to_string(),
                migration: "file_path_registered_since_0.9.295".to_string(),
            });
        }
        handles.push(StorageHandle {
            domain: "blobs".to_string(),
            backend: StorageBackendKind::BlobDirectory,
            path: layout.blobs.clone(),
            owner: "storage".to_string(),
            migration: "blob_root_registered_since_0.9.295".to_string(),
        });
        handles.sort_by(|left, right| left.domain.cmp(&right.domain));
        Self { layout, handles }
    }

    pub fn handle(&self, domain: &str) -> Option<&StorageHandle> {
        self.handles.iter().find(|handle| handle.domain == domain)
    }

    pub fn health(&self) -> StorageHealth {
        StorageHealth::from_registry(self)
    }
}

fn owner_for_domain(domain: &str) -> &'static str {
    match domain {
        "session" => "session",
        "memory" => "memory",
        "matrix" => "matrix",
        "resource_directory" => "connector",
        "tasks" => "task",
        "audit" | "audit_log" => "audit",
        "approval" | "approval_history" | "always_approved" => "approval",
        _ => "storage",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHealth {
    pub status: String,
    pub root: PathBuf,
    pub handle_count: usize,
    pub present_count: usize,
    pub missing_count: usize,
    pub handles: Vec<StorageHandleHealth>,
}

impl StorageHealth {
    pub fn from_registry(registry: &StorageRegistry) -> Self {
        let handles: Vec<StorageHandleHealth> = registry
            .handles
            .iter()
            .map(StorageHandleHealth::from_handle)
            .collect();
        let present_count = handles.iter().filter(|handle| handle.present).count();
        let missing_count = handles.len().saturating_sub(present_count);
        Self {
            status: if missing_count == 0 {
                "ready".to_string()
            } else {
                "registered".to_string()
            },
            root: registry.layout.root.clone(),
            handle_count: handles.len(),
            present_count,
            missing_count,
            handles,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHandleHealth {
    pub domain: String,
    pub backend: StorageBackendKind,
    pub path: PathBuf,
    pub owner: String,
    pub present: bool,
    pub writable_parent: bool,
}

impl StorageHandleHealth {
    fn from_handle(handle: &StorageHandle) -> Self {
        let parent = match handle.backend {
            StorageBackendKind::BlobDirectory => handle.path.as_path(),
            _ => handle.path.parent().unwrap_or_else(|| Path::new(".")),
        };
        Self {
            domain: handle.domain.clone(),
            backend: handle.backend.clone(),
            path: handle.path.clone(),
            owner: handle.owner.clone(),
            present: handle.path.exists(),
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
                .handles
                .iter()
                .map(|handle| StorageMigration {
                    id: format!("storage.{}.layout", handle.domain),
                    domain: handle.domain.clone(),
                    status: "registered".to_string(),
                    target: handle.path.clone(),
                })
                .collect(),
        }
    }

    pub fn status(&self) -> Vec<StorageMigration> {
        self.migrations.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMigration {
    pub id: String,
    pub domain: String,
    pub status: String,
    pub target: PathBuf,
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
            diagnose_sqlite_path(&path, &handle.backend);
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
        Ok(connection) => match connection
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
        {
            Ok(value) if value.eq_ignore_ascii_case("ok") => ("ok".to_string(), false, None, None),
            Ok(value) => (
                "degraded".to_string(),
                false,
                Some(format!("quick_check={value}")),
                Some("inspect sqlite integrity before writes".to_string()),
            ),
            Err(error) => (
                "error".to_string(),
                is_busy_or_locked(&StorageError::from(error)),
                Some("quick_check failed".to_string()),
                Some("retry after active writer completes or inspect stale locks".to_string()),
            ),
        },
        Err(error) => (
            "error".to_string(),
            is_busy_or_locked(&error),
            Some(error.to_string()),
            Some("check file permissions and active sqlite writers".to_string()),
        ),
    }
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
        assert_eq!(layout.blobs, Path::new("/tmp/cowd-config/storage/blobs"));
    }

    #[test]
    fn registry_declares_core_domains() {
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config");
        for domain in [
            "session",
            "memory",
            "matrix",
            "resource_directory",
            "tasks",
            "audit",
            "approval",
            "approval_history",
            "always_approved",
            "audit_log",
            "blobs",
        ] {
            assert!(registry.handle(domain).is_some(), "missing {domain}");
        }
    }

    #[test]
    fn migration_runner_reports_registered_targets() {
        let registry = StorageRegistry::default_for_config_home("/tmp/cowd-config");
        let runner = MigrationRunner::from_registry(&registry);
        assert_eq!(runner.status().len(), registry.handles.len());
        assert!(runner
            .status()
            .iter()
            .any(|migration| migration.id == "storage.matrix.layout"));
    }

    #[test]
    fn health_reports_missing_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        let registry = StorageRegistry::default_for_config_home(dir.path());
        let health = registry.health();
        assert_eq!(health.status, "registered");
        assert_eq!(health.handle_count, 11);
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
    fn direct_open_allowlist_is_machine_readable() {
        let raw = include_str!("../direct-open-allowlist.json");
        let entries: Vec<serde_json::Value> = serde_json::from_str(raw).unwrap();
        assert!(entries.len() >= 10);
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
            assert_eq!(
                entry["status"].as_str().unwrap(),
                "owned_boundary_reviewed_0.9.305"
            );
            assert_ne!(
                entry["allowed_until_test"].as_str().unwrap(),
                "cargo test -p memory --no-default-features -- --nocapture"
            );
        }
        assert!(entries
            .iter()
            .any(|entry| entry["file"] == "crates/memory/src/store/verbatim.rs"));
    }
}
