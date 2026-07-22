use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{types::Value, Connection};
use serde::{Deserialize, Serialize};

use crate::{StorageBackend, StorageBackendKind, StorageEndpoint, StorageError, StorageHandle};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlitePragmaConfig {
    pub busy_timeout_ms: u64,
    pub journal_mode: String,
    pub foreign_keys: bool,
    pub synchronous: String,
    pub temp_store: String,
    pub mmap_size_bytes: Option<u64>,
}

impl Default for SqlitePragmaConfig {
    fn default() -> Self {
        Self {
            busy_timeout_ms: 5_000,
            journal_mode: "WAL".to_string(),
            foreign_keys: true,
            synchronous: "NORMAL".to_string(),
            temp_store: "MEMORY".to_string(),
            mmap_size_bytes: None,
        }
    }
}

pub struct SqliteConnectionFactory {
    pragma: SqlitePragmaConfig,
}

impl SqliteConnectionFactory {
    pub fn new(pragma: SqlitePragmaConfig) -> Self {
        Self { pragma }
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<Connection, StorageError> {
        let connection = Connection::open(path)?;
        self.apply_pragmas(&connection)?;
        Ok(connection)
    }

    pub fn open_handle(&self, handle: &StorageHandle) -> Result<Connection, StorageError> {
        if handle.backend != StorageBackendKind::Sqlite {
            return Err(StorageError::Other(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        self.open(&handle.path)
    }

    pub fn open_in_memory(&self) -> Result<Connection, StorageError> {
        let connection = Connection::open_in_memory()?;
        self.apply_pragmas(&connection)?;
        Ok(connection)
    }

    pub fn apply_pragmas(&self, connection: &Connection) -> Result<(), StorageError> {
        set_pragma(
            connection,
            "busy_timeout",
            &self.pragma.busy_timeout_ms.to_string(),
        )?;
        set_pragma(
            connection,
            "journal_mode",
            &quote_pragma(&self.pragma.journal_mode),
        )?;
        set_pragma(
            connection,
            "foreign_keys",
            if self.pragma.foreign_keys {
                "ON"
            } else {
                "OFF"
            },
        )?;
        set_pragma(
            connection,
            "synchronous",
            &quote_pragma(&self.pragma.synchronous),
        )?;
        set_pragma(
            connection,
            "temp_store",
            &quote_pragma(&self.pragma.temp_store),
        )?;
        if let Some(mmap_size) = self.pragma.mmap_size_bytes {
            set_pragma(connection, "mmap_size", &mmap_size.to_string())?;
        }
        Ok(())
    }
}

fn quote_pragma(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn set_pragma(connection: &Connection, name: &str, value: &str) -> Result<(), StorageError> {
    let sql = format!("PRAGMA {name} = {value}");
    match connection.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::ExecuteReturnedResults) => {
            let _: Value = connection.query_row(&sql, [], |row| row.get(0))?;
            Ok(())
        }
        Err(error) => Err(StorageError::from(error)),
    }
}

impl Default for SqliteConnectionFactory {
    fn default() -> Self {
        Self::new(SqlitePragmaConfig::default())
    }
}

/// Bounded concurrency policy for one durable SQLite endpoint.  The profile
/// is selected by the storage owner, never by an individual repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteExecutionProfile {
    pub max_connections: u32,
    pub checkout_timeout_ms: u64,
    pub pragma: SqlitePragmaConfig,
}

impl SqliteExecutionProfile {
    #[must_use]
    pub fn for_endpoint(endpoint: &StorageEndpoint) -> Self {
        Self::for_domain_name(&endpoint.domain.logical_name())
    }

    #[must_use]
    pub fn for_domain_name(domain: &str) -> Self {
        let max_connections = match domain {
            "matrix" | "runtime_events" | "surface_messages" => 8,
            "resource_directory" | "connector_directory" | "growth" | "fact" | "tasks" => 4,
            _ => 2,
        };
        let mut pragma = SqlitePragmaConfig::default();
        if domain == "runtime_events" {
            // Runtime's revision fence is its control-plane source of truth;
            // preserve its pre-existing full-sync durability profile on every
            // pool connection, not just the startup connection.
            pragma.synchronous = "FULL".to_string();
        }
        Self {
            max_connections,
            checkout_timeout_ms: 5_000,
            pragma,
        }
    }

    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            max_connections: 1,
            checkout_timeout_ms: 5_000,
            pragma: SqlitePragmaConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteExecutorMetrics {
    pub checkout_count: u64,
    pub checkout_timeout_count: u64,
    pub checkout_wait_ms: u64,
    pub busy_or_locked_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteExecutorHealth {
    pub logical_identity: String,
    pub profile: SqliteExecutionProfile,
    pub metrics: SqliteExecutorMetrics,
}

#[derive(Debug, Default)]
struct SqliteExecutorCounters {
    checkout_count: AtomicU64,
    checkout_timeout_count: AtomicU64,
    checkout_wait_ms: AtomicU64,
    busy_or_locked_count: AtomicU64,
}

#[derive(Debug)]
struct SqliteExecutorInner {
    identity: String,
    profile: SqliteExecutionProfile,
    pool: Pool<SqliteConnectionManager>,
    counters: SqliteExecutorCounters,
}

/// The only production SQLite execution primitive.  It owns a bounded pool
/// whose connections all receive the same WAL/foreign-key/busy-timeout
/// profile.  Repository code can keep its SQL and transaction boundaries,
/// but it cannot serialize the whole domain behind a `Mutex<Connection>`.
#[derive(Debug, Clone)]
pub struct SqliteExecutor {
    inner: Arc<SqliteExecutorInner>,
}

impl SqliteExecutor {
    /// Gets the process-wide executor cached for this endpoint.
    pub fn for_endpoint(endpoint: &StorageEndpoint) -> Result<Self, StorageError> {
        StorageRuntime::global().sqlite(endpoint)
    }

    /// Transitional adapter for repositories that have not yet changed their
    /// constructor signature from `StorageHandle` to `StorageEndpoint`.
    pub fn for_handle(handle: &StorageHandle) -> Result<Self, StorageError> {
        StorageRuntime::global().sqlite_handle(handle)
    }

    /// Creates a bounded, shared-cache in-memory executor for test-only
    /// repositories. The one-connection profile preserves SQLite's in-memory
    /// database identity without reintroducing a process mutex.
    pub fn in_memory(identity: impl Into<String>) -> Result<Self, StorageError> {
        Self::open_memory(identity.into(), SqliteExecutionProfile::in_memory())
    }

    fn open_file(
        identity: String,
        path: PathBuf,
        profile: SqliteExecutionProfile,
    ) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let pragma = profile.pragma.clone();
        let manager = SqliteConnectionManager::file(path).with_init(move |connection| {
            SqliteConnectionFactory::new(pragma.clone())
                .apply_pragmas(connection)
                .map_err(storage_error_to_rusqlite)
        });
        Self::build(identity, profile, manager)
    }

    fn open_memory(
        identity: String,
        profile: SqliteExecutionProfile,
    ) -> Result<Self, StorageError> {
        let pragma = profile.pragma.clone();
        let manager = SqliteConnectionManager::memory().with_init(move |connection| {
            SqliteConnectionFactory::new(pragma.clone())
                .apply_pragmas(connection)
                .map_err(storage_error_to_rusqlite)
        });
        Self::build(identity, profile, manager)
    }

    fn build(
        identity: String,
        profile: SqliteExecutionProfile,
        manager: SqliteConnectionManager,
    ) -> Result<Self, StorageError> {
        let pool = Pool::builder()
            .max_size(profile.max_connections)
            .connection_timeout(Duration::from_millis(profile.checkout_timeout_ms))
            .build(manager)
            .map_err(|error| {
                StorageError::Other(format!("sqlite pool initialization failed: {error}"))
            })?;
        let executor = Self {
            inner: Arc::new(SqliteExecutorInner {
                identity,
                profile,
                pool,
                counters: SqliteExecutorCounters::default(),
            }),
        };
        // Eagerly create and validate one connection so setup errors are
        // reported at composition time rather than on a later request.
        drop(executor.checkout()?);
        Ok(executor)
    }

    pub fn checkout(&self) -> Result<PooledConnection<SqliteConnectionManager>, StorageError> {
        let started = Instant::now();
        let result = self.inner.pool.get_timeout(Duration::from_millis(
            self.inner.profile.checkout_timeout_ms,
        ));
        self.inner
            .counters
            .checkout_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner.counters.checkout_wait_ms.fetch_add(
            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        result.map_err(|error| {
            self.inner
                .counters
                .checkout_timeout_count
                .fetch_add(1, Ordering::Relaxed);
            StorageError::Other(format!(
                "sqlite executor `{}` checkout failed: {error}",
                self.inner.identity
            ))
        })
    }

    pub fn with_read<T>(
        &self,
        operation: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, StorageError> {
        let connection = self.checkout()?;
        let result = operation(&connection);
        self.record_sql_result(&result);
        result.map_err(StorageError::from)
    }

    pub fn with_immediate_transaction<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, StorageError> {
        let mut connection = self.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = operation(&transaction);
        self.record_sql_result(&result);
        let value = result?;
        transaction.commit()?;
        Ok(value)
    }

    pub fn record_sql_error(&self, error: &rusqlite::Error) {
        if matches!(
            error,
            rusqlite::Error::SqliteFailure(code, _)
                if matches!(
                    code.code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                )
        ) {
            self.inner
                .counters
                .busy_or_locked_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_sql_result<T>(&self, result: &rusqlite::Result<T>) {
        if let Err(error) = result {
            self.record_sql_error(error);
        }
    }

    #[must_use]
    pub fn metrics(&self) -> SqliteExecutorMetrics {
        SqliteExecutorMetrics {
            checkout_count: self.inner.counters.checkout_count.load(Ordering::Relaxed),
            checkout_timeout_count: self
                .inner
                .counters
                .checkout_timeout_count
                .load(Ordering::Relaxed),
            checkout_wait_ms: self.inner.counters.checkout_wait_ms.load(Ordering::Relaxed),
            busy_or_locked_count: self
                .inner
                .counters
                .busy_or_locked_count
                .load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn profile(&self) -> &SqliteExecutionProfile {
        &self.inner.profile
    }

    #[must_use]
    pub fn logical_identity(&self) -> &str {
        &self.inner.identity
    }

    #[must_use]
    pub fn health(&self) -> SqliteExecutorHealth {
        SqliteExecutorHealth {
            logical_identity: self.inner.identity.clone(),
            profile: self.inner.profile.clone(),
            metrics: self.metrics(),
        }
    }
}

/// Process-wide pool cache. Its mutex protects only cache insertion; SQL work
/// takes place after checkout and never holds this cache lock.
#[derive(Debug, Default)]
pub struct StorageRuntime {
    sqlite_executors: Mutex<BTreeMap<String, SqliteExecutor>>,
}

impl StorageRuntime {
    #[must_use]
    pub fn global() -> &'static Self {
        static RUNTIME: OnceLock<StorageRuntime> = OnceLock::new();
        RUNTIME.get_or_init(Self::default)
    }

    pub fn sqlite(&self, endpoint: &StorageEndpoint) -> Result<SqliteExecutor, StorageError> {
        if endpoint.backend != StorageBackendKind::Sqlite {
            return Err(StorageError::Other(format!(
                "storage endpoint `{}` is not sqlite-backed",
                endpoint.logical_id()
            )));
        }
        let identity = endpoint.logical_id();
        self.cached_file_executor(
            format!("{}:{}", identity, endpoint.path.display()),
            identity,
            endpoint.path.clone(),
            SqliteExecutionProfile::for_endpoint(endpoint),
        )
    }

    pub fn sqlite_handle(&self, handle: &StorageHandle) -> Result<SqliteExecutor, StorageError> {
        if handle.backend != StorageBackendKind::Sqlite {
            return Err(StorageError::Other(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        self.cached_file_executor(
            format!("legacy:{}:{}", handle.domain, handle.path.display()),
            format!("legacy:{}", handle.domain),
            handle.path.clone(),
            SqliteExecutionProfile::for_domain_name(&handle.domain),
        )
    }

    #[must_use]
    pub fn sqlite_health(&self) -> Vec<SqliteExecutorHealth> {
        let Ok(cache) = self.sqlite_executors.lock() else {
            return Vec::new();
        };
        let mut health = cache
            .values()
            .map(SqliteExecutor::health)
            .collect::<Vec<_>>();
        health.sort_by(|left, right| left.logical_identity.cmp(&right.logical_identity));
        health
    }

    fn cached_file_executor(
        &self,
        cache_key: String,
        identity: String,
        path: PathBuf,
        profile: SqliteExecutionProfile,
    ) -> Result<SqliteExecutor, StorageError> {
        let mut cache = self.sqlite_executors.lock().map_err(|error| {
            StorageError::Other(format!("sqlite executor cache poisoned: {error}"))
        })?;
        if let Some(executor) = cache.get(&cache_key) {
            return Ok(executor.clone());
        }
        let executor = SqliteExecutor::open_file(identity, path, profile)?;
        cache.insert(cache_key, executor.clone());
        Ok(executor)
    }
}

fn storage_error_to_rusqlite(error: StorageError) -> rusqlite::Error {
    match error {
        StorageError::Sqlite(error) => error,
        other => rusqlite::Error::ToSqlConversionFailure(Box::new(other)),
    }
}

pub struct SqliteStorage {
    executor: SqliteExecutor,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let handle = StorageHandle::sqlite(
            "kv",
            path.as_ref().to_path_buf(),
            "storage",
            "sqlite_storage_executor",
        );
        let executor = SqliteExecutor::for_handle(&handle)?;
        executor.with_immediate_transaction(|transaction| {
            transaction.execute_batch(
                "CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )",
            )
        })?;
        Ok(Self { executor })
    }
}

impl StorageBackend for SqliteStorage {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.executor.with_immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, value],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.executor.with_read(|connection| {
            let mut stmt = connection.prepare("SELECT value FROM kv WHERE key = ?1")?;
            let mut rows =
                stmt.query_map(rusqlite::params![key], |row| row.get::<_, Vec<u8>>(0))?;
            rows.next().transpose()
        })
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.executor.with_immediate_transaction(|transaction| {
            transaction.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![key])?;
            Ok(())
        })?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.executor.with_read(|connection| {
            let mut stmt = connection.prepare("SELECT key FROM kv WHERE key LIKE ?1")?;
            let pattern = format!("{prefix}%");
            let keys = stmt
                .query_map(rusqlite::params![pattern], |row| row.get(0))?
                .collect();
            keys
        })
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_factory_applies_core_pragmas() {
        let conn = SqliteConnectionFactory::default().open_in_memory().unwrap();
        let busy_timeout: u64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        let foreign_keys: u64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        let synchronous: u64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        let temp_store: u64 = conn
            .query_row("PRAGMA temp_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy_timeout, 5_000);
        assert_eq!(foreign_keys, 1);
        assert_eq!(synchronous, 1);
        assert_eq!(temp_store, 2);
    }

    #[test]
    fn executor_caches_endpoint_pool_and_applies_pragmas_to_every_checkout() {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = StorageEndpoint::sqlite(
            crate::StorageDomainId::RuntimeEvents,
            crate::StorageScope::Global,
            directory.path().join("runtime.sqlite"),
            "runtime",
            "test",
        );
        let first = SqliteExecutor::for_endpoint(&endpoint).unwrap();
        let second = SqliteExecutor::for_endpoint(&endpoint).unwrap();
        assert_eq!(first.logical_identity(), endpoint.logical_id());
        let foreign_keys: u64 = first
            .with_read(|connection| {
                connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            })
            .unwrap();
        assert_eq!(foreign_keys, 1);
        assert!(second.metrics().checkout_count >= 2);
    }

    #[test]
    fn executor_immediate_transactions_preserve_atomic_writes() {
        let executor = SqliteExecutor::in_memory("storage-executor-atomicity").unwrap();
        executor
            .with_immediate_transaction(|transaction| {
                transaction.execute_batch(
                    "CREATE TABLE events(id TEXT PRIMARY KEY, value TEXT NOT NULL);",
                )?;
                transaction.execute("INSERT INTO events(id, value) VALUES ('a', 'one')", [])?;
                Ok(())
            })
            .unwrap();
        let count: i64 = executor
            .with_read(|connection| {
                connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            })
            .unwrap();
        assert_eq!(count, 1);
    }
}
