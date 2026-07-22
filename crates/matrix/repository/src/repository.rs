use std::path::Path;

use serde::{Deserialize, Serialize};
use storage::{StorageDomainId, StorageEndpoint, StorageLayout, StorageRegistry};
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
    pub endpoint: StorageEndpoint,
}

impl MatrixRepositoryConfig {
    pub fn from_storage_registry(
        registry: &StorageRegistry,
    ) -> Result<Self, MatrixRepositoryError> {
        let endpoint = registry
            .endpoint(&StorageDomainId::Matrix)
            .map_err(|_| MatrixRepositoryError::MissingHandle("matrix".to_string()))?
            .clone();
        Ok(Self { endpoint })
    }

    pub fn from_storage_layout(layout: &StorageLayout) -> Result<Self, MatrixRepositoryError> {
        let registry = StorageRegistry::from_layout(layout.clone());
        Self::from_storage_registry(&registry)
    }

    pub fn from_config_home(config_home: impl AsRef<Path>) -> Result<Self, MatrixRepositoryError> {
        Self::from_storage_registry(&StorageRegistry::default_for_config_home(config_home))
    }
}

pub trait MatrixRepository {
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

    pub fn storage_endpoint(&self) -> &StorageEndpoint {
        &self.config.endpoint
    }
}

impl MatrixRepository for MatrixRepositoryHandle {
    fn health_projection(&self) -> Result<serde_json::Value, MatrixRepositoryError> {
        Ok(serde_json::json!({
            "backend": self.config.endpoint.backend,
            "owner": self.config.endpoint.owner,
            "storage_domain": self.config.endpoint.domain,
            "storage_scope": self.config.endpoint.scope,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_exposes_logical_matrix_storage_contract() {
        let dir = tempfile::tempdir().unwrap();
        let repo = MatrixRepositoryHandle::from_config_home(dir.path()).unwrap();

        assert_eq!(repo.health_projection().unwrap()["owner"], "matrix");
        assert_eq!(
            repo.health_projection().unwrap()["storage_domain"]["kind"],
            "matrix"
        );
    }
}
