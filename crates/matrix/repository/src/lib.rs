mod data_plane;
mod repository;
mod sqlite_repository;

pub use data_plane::MatrixSqliteDataPlane;
pub use repository::{
    MatrixRepository, MatrixRepositoryConfig, MatrixRepositoryError, MatrixRepositoryHandle,
};
pub use sqlite_repository::{
    MatrixHealth, MatrixMetricRecomputeResult, MatrixSqliteRepository, MatrixSqliteRepositoryError,
};

pub fn open_matrix_sqlite_repository(
    path: impl AsRef<std::path::Path>,
) -> Result<MatrixSqliteRepository, MatrixSqliteRepositoryError> {
    MatrixSqliteRepository::open(path)
}
