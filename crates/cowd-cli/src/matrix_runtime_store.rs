use std::path::Path;

impl crate::gateway_services::MatrixService {
    pub(crate) fn open_runtime_store(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<runtime::MatrixRuntimeStore, runtime::MatrixRuntimeStoreError> {
        let path = self.store_path(config_home).map_err(|error| {
            runtime::MatrixRuntimeStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(
                Box::new(error),
            ))
        })?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                runtime::MatrixRuntimeStoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(
                    Box::new(error),
                ))
            })?;
        }
        runtime::open_matrix_runtime_store(path)
    }
}
