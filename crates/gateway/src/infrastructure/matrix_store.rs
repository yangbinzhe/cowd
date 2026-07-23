use std::sync::Arc;

impl crate::services::MatrixService {
    /// Compose Matrix through its backend-neutral contract. V574 deliberately
    /// selects only the existing SQLite adapter; V575 extends this seam with
    /// the verified PostgreSQL adapter instead of bypassing the port.
    pub(crate) fn store(
        &self,
        config_home: impl AsRef<std::path::Path>,
    ) -> Result<Arc<dyn matrix_repository::MatrixStore>, matrix_repository::MatrixStoreError> {
        if let Some(store) = self.selected_store.as_ref() {
            return Ok(Arc::clone(store));
        }
        let registry = storage::StorageRegistry::default_for_config_home(config_home);
        let endpoint = registry
            .endpoint(&storage::StorageDomainId::Matrix)
            .cloned()
            .map_err(|error| matrix_repository::MatrixStoreError::Backend(error.to_string()))?;
        matrix_repository::MatrixStoreHandle::new(endpoint).open()
    }
}
