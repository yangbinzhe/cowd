#[derive(Debug, thiserror::Error)]
pub enum SessionStoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("migration failed: {0}")]
    Migration(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}
