use std::path::Path;

impl crate::gateway_services::MatrixService {
    pub(crate) fn runtime_store(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<matrix_store::MatrixRuntimeStore, matrix_store::MatrixRuntimeStoreError> {
        let path = self.store_path(config_home).map_err(|error| {
            matrix_store::MatrixRuntimeStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(
                Box::new(error),
            ))
        })?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                matrix_store::MatrixRuntimeStoreError::Sqlite(
                    rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
                )
            })?;
        }
        matrix_store::open_matrix_runtime_store(path)
    }
}
