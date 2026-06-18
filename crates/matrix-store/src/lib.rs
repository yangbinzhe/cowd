mod data_plane;
mod repository;
mod runtime_store;

pub use data_plane::MatrixSqliteDataPlane;
pub use repository::{
    MatrixRepository, MatrixRepositoryConfig, MatrixRepositoryError, MatrixRepositoryHandle,
};
pub use runtime_store::{
    MatrixHealth, MatrixMetricRecomputeResult, MatrixRuntimeStore, MatrixRuntimeStoreError,
};

pub fn open_matrix_runtime_store(
    path: impl AsRef<std::path::Path>,
) -> Result<MatrixRuntimeStore, MatrixRuntimeStoreError> {
    MatrixRuntimeStore::open(path)
}
