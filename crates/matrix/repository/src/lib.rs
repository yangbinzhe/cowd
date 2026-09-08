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
mod port;
mod postgres_repository;
mod repository;

pub use data_plane::MatrixLocalDataPlane;
pub use port::{
    MatrixCatalogPage, MatrixCatalogQuery, MatrixCatalogRecord, MatrixCatalogSnapshot,
    MatrixHealth, MatrixMetricRecomputeResult, MatrixRecallQuery, MatrixRevisioned, MatrixStore,
    MatrixStoreError, MatrixStoreHandle, MatrixStoreResult,
};
pub use postgres_repository::PostgresMatrixRepository;
pub use repository::{
    MatrixRepository, MatrixRepositoryConfig, MatrixRepositoryError, MatrixRepositoryHandle,
};
