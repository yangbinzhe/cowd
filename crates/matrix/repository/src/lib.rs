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

mod data_plane;
mod migration;
mod ownership_import;
mod port;
mod postgres_repository;
mod repository;
mod sqlite_repository;

pub use data_plane::{MatrixDataPlaneBackend, MatrixLocalDataPlane};
pub use migration::{copy_quiesced_matrix_store, MatrixMigrationManifest, MatrixMigrationSnapshot};
pub use ownership_import::{MatrixOwnershipImportOutcome, MatrixOwnershipImportReceipt};
pub use port::{
    MatrixHealth, MatrixMetricRecomputeResult, MatrixRecallQuery, MatrixRevisioned, MatrixStore,
    MatrixStoreError, MatrixStoreHandle, MatrixStoreResult,
};
pub use postgres_repository::PostgresMatrixRepository;
pub use repository::{
    MatrixRepository, MatrixRepositoryConfig, MatrixRepositoryError, MatrixRepositoryHandle,
};
pub use sqlite_repository::{MatrixSqliteRepository, MatrixSqliteRepositoryError};

pub fn open_matrix_sqlite_repository(
    path: impl AsRef<std::path::Path>,
) -> Result<MatrixSqliteRepository, MatrixSqliteRepositoryError> {
    MatrixSqliteRepository::open(path)
}

pub fn open_matrix_sqlite_repository_handle(
    handle: &storage::StorageHandle,
) -> Result<MatrixSqliteRepository, MatrixSqliteRepositoryError> {
    MatrixSqliteRepository::open_storage_handle(handle)
}
