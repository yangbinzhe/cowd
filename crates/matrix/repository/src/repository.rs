use std::path::Path;

use serde::{Deserialize, Serialize};
use storage::{StorageHandle, StorageLayout, StorageRegistry};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MatrixRepositoryError {
    #[error("storage handle missing: {0}")]
    MissingHandle(String),
    #[error("matrix backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixRepositoryConfig {
    pub handle: StorageHandle,
}

impl MatrixRepositoryConfig {
    pub fn from_storage_layout(layout: &StorageLayout) -> Result<Self, MatrixRepositoryError> {
        let registry = StorageRegistry::from_layout(layout.clone());
        let handle = registry
            .sqlite_handle("matrix")
            .map_err(|_| MatrixRepositoryError::MissingHandle("matrix".to_string()))?
            .clone();
        Ok(Self { handle })
    }

    pub fn from_config_home(config_home: impl AsRef<Path>) -> Result<Self, MatrixRepositoryError> {
        Self::from_storage_layout(&StorageLayout::default_for_config_home(config_home))
    }
}

pub trait MatrixRepository {
    fn db_path(&self) -> &Path;
    fn health_projection(&self) -> Result<serde_json::Value, MatrixRepositoryError>;
}

#[derive(Debug, Clone)]
pub struct MatrixRepositoryHandle {
    config: MatrixRepositoryConfig,
}

impl MatrixRepositoryHandle {
    pub fn new(config: MatrixRepositoryConfig) -> Self {
        Self { config }
    }

    pub fn from_config_home(config_home: impl AsRef<Path>) -> Result<Self, MatrixRepositoryError> {
        Ok(Self::new(MatrixRepositoryConfig::from_config_home(
            config_home,
        )?))
    }

    pub fn storage_handle(&self) -> &StorageHandle {
        &self.config.handle
    }
}

impl MatrixRepository for MatrixRepositoryHandle {
    fn db_path(&self) -> &Path {
        &self.config.handle.path
    }

    fn health_projection(&self) -> Result<serde_json::Value, MatrixRepositoryError> {
        Ok(serde_json::json!({
            "db_path": self.config.handle.path,
            "backend": "sqlite",
            "owner": self.config.handle.owner,
            "storage_domain": self.config.handle.domain,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_uses_storage_layout_matrix_path() {
        let dir = tempfile::tempdir().unwrap();
        let repo = MatrixRepositoryHandle::from_config_home(dir.path()).unwrap();

        assert!(repo.db_path().ends_with("storage/matrix.sqlite"));
        assert_eq!(repo.health_projection().unwrap()["owner"], "matrix");
    }
}
