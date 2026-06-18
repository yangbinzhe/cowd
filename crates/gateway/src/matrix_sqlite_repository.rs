use std::path::Path;

impl crate::gateway_services::MatrixService {
    pub(crate) fn sqlite_repository(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<
        matrix_repository::MatrixSqliteRepository,
        matrix_repository::MatrixSqliteRepositoryError,
    > {
        let path = self.store_path(config_home).map_err(|error| {
            matrix_repository::MatrixSqliteRepositoryError::Sqlite(
                rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
            )
        })?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                matrix_repository::MatrixSqliteRepositoryError::Sqlite(
                    rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
                )
            })?;
        }
        matrix_repository::open_matrix_sqlite_repository(path)
    }
}
