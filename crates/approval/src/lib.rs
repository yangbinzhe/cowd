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
use storage::StorageLayout;
use thiserror::Error;

pub mod cache;
pub mod contract;

pub use cache::{ApprovalCacheDecision, ApprovalCacheKey};
pub use contract::{ApprovalDecision, ApprovalPersistence, ApprovalReceipt, ApprovalRequest};

#[derive(Debug, Error)]
pub enum ApprovalRepositoryError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage handle missing: {0}")]
    MissingHandle(String),
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

pub trait ApprovalRepository {
    fn list_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApprovalHistoryEntry>, usize), ApprovalRepositoryError>;
    fn append_history(&self, entry: ApprovalHistoryEntry) -> Result<(), ApprovalRepositoryError>;
    fn list_always_allowed(&self) -> Result<Vec<String>, ApprovalRepositoryError>;
    fn add_always_allowed(&self, normalized_command: &str) -> Result<(), ApprovalRepositoryError>;
    fn revoke_always_allowed(
        &self,
        normalized_command: &str,
    ) -> Result<(), ApprovalRepositoryError>;
}

#[derive(Debug, Clone)]
pub struct FileApprovalRepository {
    history_path: PathBuf,
    always_allowed_path: PathBuf,
    max_history: usize,
}

impl FileApprovalRepository {
    pub fn from_storage_layout(layout: &StorageLayout) -> Result<Self, ApprovalRepositoryError> {
        let history_path = layout
            .file_path("approval_history")
            .ok_or_else(|| ApprovalRepositoryError::MissingHandle("approval_history".to_string()))?
            .to_path_buf();
        let always_allowed_path = layout
            .file_path("always_approved")
            .ok_or_else(|| ApprovalRepositoryError::MissingHandle("always_approved".to_string()))?
            .to_path_buf();
        Ok(Self {
            history_path,
            always_allowed_path,
            max_history: 200,
        })
    }

    pub fn new(history_path: PathBuf, always_allowed_path: PathBuf) -> Self {
        Self {
            history_path,
            always_allowed_path,
            max_history: 200,
        }
    }

    fn read_history(&self) -> Result<Vec<ApprovalHistoryEntry>, ApprovalRepositoryError> {
        read_json_or_default(&self.history_path)
    }

    fn write_history(
        &self,
        entries: &[ApprovalHistoryEntry],
    ) -> Result<(), ApprovalRepositoryError> {
        write_json_pretty(&self.history_path, entries)
    }
}

impl ApprovalRepository for FileApprovalRepository {
    fn list_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApprovalHistoryEntry>, usize), ApprovalRepositoryError> {
        let entries = self.read_history()?;
        let total = entries.len();
        let page = entries.into_iter().skip(offset).take(limit).collect();
        Ok((page, total))
    }

    fn append_history(&self, entry: ApprovalHistoryEntry) -> Result<(), ApprovalRepositoryError> {
        let mut entries = self.read_history()?;
        entries.insert(0, entry);
        if entries.len() > self.max_history {
            entries.truncate(self.max_history);
        }
        self.write_history(&entries)
    }

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
    fs::write(path, serde_json::to_string_pretty(value)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(id: &str) -> ApprovalHistoryEntry {
        ApprovalHistoryEntry {
            id: id.to_string(),
            request_id: format!("req-{id}"),
            command: "rm -rf target".to_string(),
            normalized_command: "rm -rf target".to_string(),
            risk_level: "critical".to_string(),
            matched_patterns: vec!["delete".to_string()],
            outcome: ApprovalHistoryOutcome::Approved {
                persistence: "once".to_string(),
            },
            resolved_at: "2026-06-18T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn repository_uses_storage_layout_paths() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StorageLayout::default_for_config_home(dir.path());
        let repo = FileApprovalRepository::from_storage_layout(&layout).unwrap();
        repo.append_history(sample_entry("1")).unwrap();
        repo.add_always_allowed("cargo test").unwrap();

        assert!(layout.file_path("approval_history").unwrap().exists());
        assert!(layout.file_path("always_approved").unwrap().exists());
        assert_eq!(repo.list_history(10, 0).unwrap().1, 1);
        assert_eq!(repo.list_always_allowed().unwrap(), vec!["cargo test"]);
    }

    #[test]
    fn always_allowed_revoke_persists() {
        let dir = tempfile::tempdir().unwrap();
        let repo = FileApprovalRepository::new(
            dir.path().join("approval_history.json"),
            dir.path().join("always_approved.json"),
        );
        repo.add_always_allowed("cargo fmt").unwrap();
        repo.revoke_always_allowed("cargo fmt").unwrap();
        assert!(repo.list_always_allowed().unwrap().is_empty());
    }
}
