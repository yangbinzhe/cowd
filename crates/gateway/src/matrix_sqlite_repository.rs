impl crate::services::MatrixService {
    pub(crate) fn sqlite_repository(
        &self,
        config_home: impl AsRef<std::path::Path>,
    ) -> Result<
        matrix_repository::MatrixSqliteRepository,
        matrix_repository::MatrixSqliteRepositoryError,
    > {
        let registry = storage::StorageRegistry::default_for_config_home(config_home);
        let handle = registry.sqlite_handle("matrix")?;
        if let Some(parent) = handle.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                matrix_repository::MatrixSqliteRepositoryError::Sqlite(
                    rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
                )
            })?;
        }
        matrix_repository::open_matrix_sqlite_repository_handle(handle)
    }
}
