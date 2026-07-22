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

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use storage::{StorageBackendKind, StorageEndpoint};
use thiserror::Error;

pub mod cache;
pub mod contract;
pub mod ledger;

pub use cache::{ApprovalCacheDecision, ApprovalCacheKey};
pub use contract::{ApprovalDecision, ApprovalPersistence, ApprovalReceipt, ApprovalRequest};
pub use ledger::{
    copy_quiesced_approval_history, ApprovalHistoryError, ApprovalHistoryLedger,
    ApprovalHistoryMigrationManifest, ApprovalHistoryMigrationSnapshot, ApprovalHistoryResult,
    PostgresApprovalHistoryLedger, SharedApprovalHistoryLedger, SqliteApprovalHistoryLedger,
};

#[derive(Debug, Error)]
pub enum ApprovalRepositoryError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage handle missing: {0}")]
    MissingHandle(String),
    #[error("storage endpoint `{domain}` must use file_json backend")]
    InvalidBackend { domain: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalHistoryOutcome {
    Approved { persistence: String },
    Denied { reason: String },
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalHistoryEntry {
    pub id: String,
    pub request_id: String,
    pub command: String,
    pub normalized_command: String,
    pub risk_level: String,
    pub matched_patterns: Vec<String>,
    pub outcome: ApprovalHistoryOutcome,
    pub resolved_at: String,
}

/// Explicit user-managed policy artifact.  It intentionally contains only
/// allow-list rules; it never stores approval decisions or receipts.
pub trait ApprovalPolicyArtifact: Send + Sync {
    fn list_always_allowed(&self) -> Result<Vec<String>, ApprovalRepositoryError>;
    fn add_always_allowed(&self, normalized_command: &str) -> Result<(), ApprovalRepositoryError>;
    fn revoke_always_allowed(
        &self,
        normalized_command: &str,
    ) -> Result<(), ApprovalRepositoryError>;
}

#[derive(Debug, Clone)]
pub struct FileApprovalPolicyArtifact {
    always_allowed_path: PathBuf,
}

impl FileApprovalPolicyArtifact {
    pub fn from_storage_endpoint(
        endpoint: &StorageEndpoint,
    ) -> Result<Self, ApprovalRepositoryError> {
        if endpoint.backend != StorageBackendKind::FileJson {
            return Err(ApprovalRepositoryError::InvalidBackend {
                domain: endpoint.logical_id(),
            });
        }
        Ok(Self {
            always_allowed_path: endpoint.as_handle().path,
        })
    }

    #[must_use]
    pub fn new(always_allowed_path: PathBuf) -> Self {
        Self {
            always_allowed_path,
        }
    }
}

impl ApprovalPolicyArtifact for FileApprovalPolicyArtifact {
    fn list_always_allowed(&self) -> Result<Vec<String>, ApprovalRepositoryError> {
        read_json_or_default(&self.always_allowed_path)
    }

    fn add_always_allowed(&self, normalized_command: &str) -> Result<(), ApprovalRepositoryError> {
        let mut commands = self.list_always_allowed()?;
        if !commands.iter().any(|item| item == normalized_command) {
            commands.push(normalized_command.to_string());
            commands.sort();
        }
        write_json_pretty(&self.always_allowed_path, &commands)
    }

    fn revoke_always_allowed(
        &self,
        normalized_command: &str,
    ) -> Result<(), ApprovalRepositoryError> {
        let mut commands = self.list_always_allowed()?;
        commands.retain(|item| item != normalized_command);
        write_json_pretty(&self.always_allowed_path, &commands)
    }
}

fn read_json_or_default<T>(path: &Path) -> Result<T, ApprovalRepositoryError>
where
    T: for<'de> Deserialize<'de> + Default,
{
    match fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content).map_err(ApprovalRepositoryError::from),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error.into()),
    }
}

fn write_json_pretty<T>(path: &Path, value: &T) -> Result<(), ApprovalRepositoryError>
where
    T: Serialize + ?Sized,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_path = path.with_extension(format!("tmp-{}-{}", std::process::id(), uuid_suffix()));
    fs::write(&temp_path, serde_json::to_string_pretty(value)?)?;
    fs::rename(temp_path, path)?;
    Ok(())
}

fn uuid_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_allowed_revoke_persists() {
        let dir = tempfile::tempdir().unwrap();
        let artifact = FileApprovalPolicyArtifact::new(dir.path().join("always_approved.json"));
        artifact.add_always_allowed("cargo fmt").unwrap();
        artifact.revoke_always_allowed("cargo fmt").unwrap();
        assert!(artifact.list_always_allowed().unwrap().is_empty());
    }
}
