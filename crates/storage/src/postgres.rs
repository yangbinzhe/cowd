//! PostgreSQL execution primitives for Cowd durable domains.
//!
//! This module intentionally contains no domain schema or business DTO.  It
//! owns the connection pool, secret-reference resolution, migration ledger,
//! checksum validation, and redacted diagnostics.  Domain crates own their
//! own SQL contracts and pass typed migration specifications here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use postgres::{
    types::ToSql, Config, Error as PostgresError, NoTls, Row, Statement, ToStatement, Transaction,
};
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::StorageError;

/// Resolves a reference held in configuration to a PostgreSQL URL at process
/// composition time.  The reference is observable; the resolved URL never is.
pub trait SecretRefResolver: Send + Sync {
    fn resolve_postgres_url(&self, secret_ref: &str) -> Result<ResolvedPostgresUrl, StorageError>;
}

/// A deliberately non-Debug wrapper around a resolved PostgreSQL URL.
///
/// The only way to inspect the value is inside this module while constructing
/// a driver configuration.  Diagnostics therefore cannot accidentally emit a
/// password-bearing URL through `Debug` or `Display`.
pub struct ResolvedPostgresUrl(String);

impl ResolvedPostgresUrl {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ResolvedPostgresUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResolvedPostgresUrl(<redacted>)")
    }
}

/// A resolver useful for composition tests and embedding hosts.  It never
/// exposes values through its own formatting implementation.
#[derive(Default)]
pub struct StaticSecretRefResolver {
    values: BTreeMap<String, ResolvedPostgresUrl>,
}

impl StaticSecretRefResolver {
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(reference, value)| (reference, ResolvedPostgresUrl::new(value)))
                .collect(),
        }
    }
}

impl SecretRefResolver for StaticSecretRefResolver {
    fn resolve_postgres_url(&self, secret_ref: &str) -> Result<ResolvedPostgresUrl, StorageError> {
        self.values
            .get(secret_ref)
            .map(|value| ResolvedPostgresUrl::new(value.as_str()))
            .ok_or_else(|| {
                StorageError::Other(format!(
                    "postgres secret reference `{secret_ref}` is unavailable"
                ))
            })
    }
}

/// Connection configuration safe to retain in endpoint inventory and health.
/// `secret_ref` identifies a secret in the embedding host; it is not a URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresConnectionConfig {
    pub logical_identity: String,
    pub secret_ref: String,
    pub application_name: String,
    pub max_connections: u32,
    pub min_idle_connections: Option<u32>,
    pub checkout_timeout_ms: u64,
}

impl PostgresConnectionConfig {
    #[must_use]
    pub fn new(
        logical_identity: impl Into<String>,
        secret_ref: impl Into<String>,
        application_name: impl Into<String>,
    ) -> Self {
        Self {
            logical_identity: logical_identity.into(),
            secret_ref: secret_ref.into(),
            application_name: application_name.into(),
            max_connections: 8,
            min_idle_connections: Some(1),
            checkout_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresWorkloadClass {
    Critical,
    OnlineRead,
    Background,
}

/// Controls whether domain constructors may mutate PostgreSQL schemas.
///
/// Normal Gateway composition only registers the schema catalog expected by
/// the running binary. Schema changes belong to an explicit, offline
/// maintenance command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresMigrationMode {
    Maintenance,
    RuntimeReadiness,
}

impl PostgresWorkloadClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::OnlineRead => "online_read",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresPoolLaneConfig {
    pub max_connections: u32,
    pub min_idle_connections: Option<u32>,
    pub checkout_timeout_ms: u64,
}

impl PostgresPoolLaneConfig {
    #[must_use]
    pub const fn new(
        max_connections: u32,
        min_idle_connections: Option<u32>,
        checkout_timeout_ms: u64,
    ) -> Self {
        Self {
            max_connections,
            min_idle_connections,
            checkout_timeout_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresPoolSetConfig {
    pub connection: PostgresConnectionConfig,
    pub server_reserve: u32,
    pub critical: PostgresPoolLaneConfig,
    pub online_read: PostgresPoolLaneConfig,
    pub background: PostgresPoolLaneConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresExecutorMetrics {
    pub checkout_count: u64,
    pub checkout_timeout_count: u64,
    pub checkout_wait_ms: u64,
    pub checkout_wait_p50_ms: u64,
    pub checkout_wait_p95_ms: u64,
    pub checkout_wait_p99_ms: u64,
    pub query_count: u64,
    pub query_error_count: u64,
    pub query_elapsed_ms: u64,
    pub transaction_count: u64,
    pub transaction_commit_count: u64,
    pub transaction_rollback_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresExecutorHealth {
    pub logical_identity: String,
    pub application_name: String,
    pub max_connections: u32,
    pub metrics: PostgresExecutorMetrics,
    pub migration_transaction_count: u64,
    pub runtime_readiness_query_count: u64,
    pub search_path_switch_count: u64,
    pub lanes: Vec<PostgresPoolLaneHealth>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresPoolLaneHealth {
    pub workload: PostgresWorkloadClass,
    pub max_connections: u32,
    pub active_connections: u32,
    pub idle_connections: u32,
    pub metrics: PostgresExecutorMetrics,
}

const CHECKOUT_WAIT_BUCKET_MS: [u64; 10] = [1, 2, 5, 10, 25, 50, 100, 250, 1_000, u64::MAX];

#[derive(Debug)]
struct PostgresExecutorCounters {
    checkout_count: AtomicU64,
    checkout_timeout_count: AtomicU64,
    checkout_wait_ms: AtomicU64,
    checkout_wait_buckets: [AtomicU64; CHECKOUT_WAIT_BUCKET_MS.len()],
    query_count: AtomicU64,
    query_error_count: AtomicU64,
    query_elapsed_ms: AtomicU64,
    transaction_count: AtomicU64,
    transaction_commit_count: AtomicU64,
    transaction_rollback_count: AtomicU64,
}

impl Default for PostgresExecutorCounters {
    fn default() -> Self {
        Self {
            checkout_count: AtomicU64::new(0),
            checkout_timeout_count: AtomicU64::new(0),
            checkout_wait_ms: AtomicU64::new(0),
            checkout_wait_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            query_count: AtomicU64::new(0),
            query_error_count: AtomicU64::new(0),
            query_elapsed_ms: AtomicU64::new(0),
            transaction_count: AtomicU64::new(0),
            transaction_commit_count: AtomicU64::new(0),
            transaction_rollback_count: AtomicU64::new(0),
        }
    }
}

impl PostgresExecutorCounters {
    fn record_checkout_wait(&self, elapsed: Duration) {
        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.checkout_wait_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        let bucket = CHECKOUT_WAIT_BUCKET_MS
            .iter()
            .position(|upper| elapsed_ms <= *upper)
            .unwrap_or(CHECKOUT_WAIT_BUCKET_MS.len() - 1);
        self.checkout_wait_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    fn record_query(&self, elapsed: Duration, failed: bool) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        self.query_elapsed_ms.fetch_add(
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if failed {
            self.query_error_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct PostgresPoolInner {
    max_connections: u32,
    // `postgres::Client::drop` drives its private Tokio runtime. Dropping the
    // final r2d2 pool handle from an application Tokio worker would therefore
    // panic with a nested-runtime error. Keep the pool movable so final close
    // can run on a plain OS thread.
    pool: Option<Pool<PostgresConnectionManager<NoTls>>>,
    counters: Arc<PostgresExecutorCounters>,
}

impl Drop for PostgresPoolInner {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        // Pool teardown is rare (process/test shutdown) and must not inherit a
        // caller's async-runtime context. Joining also guarantees all clients
        // are closed before the logical executor disappears.
        let _ = std::thread::spawn(move || drop(pool)).join();
    }
}

struct PostgresExecutorInner {
    config: PostgresConnectionConfig,
    critical: Arc<PostgresPoolInner>,
    online_read: Arc<PostgresPoolInner>,
    background: Arc<PostgresPoolInner>,
    migration_mode: PostgresMigrationMode,
    initialized_migration_ledgers: Mutex<BTreeSet<String>>,
    expected_migration_catalogs: Mutex<BTreeMap<(String, String), String>>,
    migration_transaction_count: AtomicU64,
    runtime_readiness_query_count: AtomicU64,
    search_path_switch_count: AtomicU64,
}

/// A bounded synchronous PostgreSQL executor.  Pool checkout is the only
/// synchronization point; query execution happens after a connection is
/// checked out, so independent requests can use distinct server connections.
#[derive(Clone)]
pub struct PostgresExecutor {
    inner: Arc<PostgresExecutorInner>,
    search_path: Option<Arc<str>>,
}

/// Process-wide owner for three isolated PostgreSQL workload lanes.
#[derive(Clone)]
pub struct PostgresPoolSet {
    executor: PostgresExecutor,
}

/// A checked-out connection whose synchronous driver calls are safe from both
/// ordinary threads and Tokio workers. The `postgres` crate owns a private
/// Tokio runtime per connection; every driver operation therefore has to run
/// inside `block_in_place` when Cowd calls a synchronous storage port from an
/// async Gateway task.
pub struct PostgresConnection {
    inner: Option<PooledConnection<PostgresConnectionManager<NoTls>>>,
    counters: Arc<PostgresExecutorCounters>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PooledSearchPath(String);

/// Transaction counterpart to [`PostgresConnection`]. Keeping this wrapper in
/// the storage foundation prevents every domain adapter from inventing its own
/// async/sync bridge.
pub struct PostgresTransaction<'a> {
    inner: Option<Transaction<'a>>,
    counters: Arc<PostgresExecutorCounters>,
}

/// Minimal backend-neutral SQL client surface used by domain helpers that work
/// with either a checked-out connection or a transaction.
pub trait PostgresClient {
    fn execute<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PostgresError>
    where
        T: ?Sized + ToStatement + Sync;

    fn query<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PostgresError>
    where
        T: ?Sized + ToStatement + Sync;

    fn query_one<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PostgresError>
    where
        T: ?Sized + ToStatement + Sync;

    fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, PostgresError>
    where
        T: ?Sized + ToStatement + Sync;

    fn prepare(&mut self, query: &str) -> Result<Statement, PostgresError>;

    fn batch_execute(&mut self, query: &str) -> Result<(), PostgresError>;
}

fn in_postgres_driver_context<F, T>(operation: F) -> T
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return operation();
    };
    match handle.runtime_flavor() {
        tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(operation),
        tokio::runtime::RuntimeFlavor::CurrentThread => {
            std::thread::scope(|scope| match scope.spawn(operation).join() {
                Ok(result) => result,
                Err(payload) => std::panic::resume_unwind(payload),
            })
        }
        _ => operation(),
    }
}

macro_rules! impl_postgres_client {
    ($type:ty) => {
        impl PostgresClient for $type {
            fn execute<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<u64, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                let started = Instant::now();
                let result =
                    in_postgres_driver_context(|| self.driver_mut().execute(query, params));
                self.counters()
                    .record_query(started.elapsed(), result.is_err());
                result
            }

            fn query<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Vec<Row>, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                let started = Instant::now();
                let result = in_postgres_driver_context(|| self.driver_mut().query(query, params));
                self.counters()
                    .record_query(started.elapsed(), result.is_err());
                result
            }

            fn query_one<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Row, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                let started = Instant::now();
                let result =
                    in_postgres_driver_context(|| self.driver_mut().query_one(query, params));
                self.counters()
                    .record_query(started.elapsed(), result.is_err());
                result
            }

            fn query_opt<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Option<Row>, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                let started = Instant::now();
                let result =
                    in_postgres_driver_context(|| self.driver_mut().query_opt(query, params));
                self.counters()
                    .record_query(started.elapsed(), result.is_err());
                result
            }

            fn prepare(&mut self, query: &str) -> Result<Statement, PostgresError> {
                let started = Instant::now();
                let result = in_postgres_driver_context(|| self.driver_mut().prepare(query));
                self.counters()
                    .record_query(started.elapsed(), result.is_err());
                result
            }

            fn batch_execute(&mut self, query: &str) -> Result<(), PostgresError> {
                let started = Instant::now();
                let result = in_postgres_driver_context(|| self.driver_mut().batch_execute(query));
                self.counters()
                    .record_query(started.elapsed(), result.is_err());
                result
            }
        }

        impl $type {
            pub fn execute<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<u64, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                PostgresClient::execute(self, query, params)
            }

            pub fn query<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Vec<Row>, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                PostgresClient::query(self, query, params)
            }

            pub fn query_one<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Row, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                PostgresClient::query_one(self, query, params)
            }

            pub fn query_opt<Q>(
                &mut self,
                query: &Q,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Option<Row>, PostgresError>
            where
                Q: ?Sized + ToStatement + Sync,
            {
                PostgresClient::query_opt(self, query, params)
            }

            pub fn prepare(&mut self, query: &str) -> Result<Statement, PostgresError> {
                PostgresClient::prepare(self, query)
            }

            pub fn batch_execute(&mut self, query: &str) -> Result<(), PostgresError> {
                PostgresClient::batch_execute(self, query)
            }
        }
    };
}

impl PostgresConnection {
    #[allow(
        clippy::expect_used,
        reason = "the driver is taken only by Drop, which cannot overlap a live mutable borrow"
    )]
    fn driver_mut(&mut self) -> &mut postgres::Client {
        self.inner
            .as_deref_mut()
            .expect("PostgresConnection driver is available until drop")
    }

    fn counters(&self) -> &PostgresExecutorCounters {
        &self.counters
    }

    pub fn transaction(&mut self) -> Result<PostgresTransaction<'_>, PostgresError> {
        let counters = Arc::clone(&self.counters);
        let transaction = in_postgres_driver_context(|| self.driver_mut().transaction())?;
        counters.transaction_count.fetch_add(1, Ordering::Relaxed);
        Ok(PostgresTransaction {
            inner: Some(transaction),
            counters,
        })
    }
}

impl_postgres_client!(PostgresConnection);

impl Drop for PostgresConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.inner.take() {
            in_postgres_driver_context(|| drop(connection));
        }
    }
}

impl<'a> PostgresTransaction<'a> {
    #[allow(
        clippy::expect_used,
        reason = "the driver is taken only by consuming commit, rollback, or Drop"
    )]
    fn driver_mut(&mut self) -> &mut Transaction<'a> {
        self.inner
            .as_mut()
            .expect("PostgresTransaction driver is available until commit or drop")
    }

    fn counters(&self) -> &PostgresExecutorCounters {
        &self.counters
    }

    #[allow(
        clippy::expect_used,
        reason = "commit consumes self, so the transaction cannot have been consumed earlier"
    )]
    pub fn commit(mut self) -> Result<(), PostgresError> {
        let transaction = self
            .inner
            .take()
            .expect("PostgresTransaction can only be committed once");
        let result = in_postgres_driver_context(|| transaction.commit());
        if result.is_ok() {
            self.counters
                .transaction_commit_count
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    #[allow(
        clippy::expect_used,
        reason = "rollback consumes self, so the transaction cannot have been consumed earlier"
    )]
    pub fn rollback(mut self) -> Result<(), PostgresError> {
        let transaction = self
            .inner
            .take()
            .expect("PostgresTransaction can only be rolled back once");
        let result = in_postgres_driver_context(|| transaction.rollback());
        if result.is_ok() {
            self.counters
                .transaction_rollback_count
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

impl_postgres_client!(PostgresTransaction<'_>);

impl Drop for PostgresTransaction<'_> {
    fn drop(&mut self) {
        if let Some(transaction) = self.inner.take() {
            in_postgres_driver_context(|| drop(transaction));
        }
    }
}

impl fmt::Debug for PostgresExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresExecutor")
            .field("logical_identity", &self.inner.config.logical_identity)
            .field("application_name", &self.inner.config.application_name)
            .field(
                "search_path",
                &self.search_path.as_deref().unwrap_or("public"),
            )
            .finish_non_exhaustive()
    }
}

impl PostgresExecutor {
    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, StorageError> {
        Self::connect_with_migration_mode(config, resolver, PostgresMigrationMode::Maintenance)
    }

    pub fn connect_with_migration_mode(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
        migration_mode: PostgresMigrationMode,
    ) -> Result<Self, StorageError> {
        validate_connection_config(&config)?;
        let resolved = resolver.resolve_postgres_url(&config.secret_ref)?;
        let client_config = parse_client_config(&config, &resolved)?;
        let pool = Arc::new(build_pool(
            &config,
            client_config,
            PostgresWorkloadClass::Critical,
            PostgresPoolLaneConfig::new(
                config.max_connections,
                config.min_idle_connections,
                config.checkout_timeout_ms,
            ),
        )?);
        Ok(Self {
            inner: Arc::new(PostgresExecutorInner {
                config,
                critical: Arc::clone(&pool),
                online_read: Arc::clone(&pool),
                background: pool,
                migration_mode,
                initialized_migration_ledgers: Mutex::new(BTreeSet::new()),
                expected_migration_catalogs: Mutex::new(BTreeMap::new()),
                migration_transaction_count: AtomicU64::new(0),
                runtime_readiness_query_count: AtomicU64::new(0),
                search_path_switch_count: AtomicU64::new(0),
            }),
            search_path: None,
        })
    }

    /// Return a capability-scoped view over the same bounded pool. A checkout
    /// changes the pooled connection's schema only when its recorded scope
    /// differs, so APP search paths cannot leak without paying a SET on every
    /// same-scope reuse.
    pub fn scoped_namespace(&self, namespace: &str) -> Result<Self, StorageError> {
        if namespace.is_empty()
            || namespace.len() > 63
            || !namespace
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || !namespace
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
        {
            return Err(StorageError::Other(format!(
                "invalid PostgreSQL namespace `{namespace}`"
            )));
        }
        Ok(Self {
            inner: Arc::clone(&self.inner),
            search_path: Some(Arc::from(namespace)),
        })
    }

    fn checkout_raw(
        &self,
        workload: PostgresWorkloadClass,
    ) -> Result<PooledConnection<PostgresConnectionManager<NoTls>>, StorageError> {
        let pool = self.pool(workload);
        let started = Instant::now();
        let result = pool
            .pool
            .as_ref()
            .ok_or_else(|| StorageError::Other("postgres pool is closed".to_string()))?
            .get();
        pool.counters.checkout_count.fetch_add(1, Ordering::Relaxed);
        pool.counters.record_checkout_wait(started.elapsed());
        let mut connection = result.map_err(|error| {
            pool.counters
                .checkout_timeout_count
                .fetch_add(1, Ordering::Relaxed);
            StorageError::Other(format!(
                "postgres {} pool checkout failed for `{}`: {error}",
                workload.as_str(),
                self.inner.config.logical_identity,
            ))
        })?;
        let search_path = self.search_path.as_deref().unwrap_or("public");
        let search_path_is_current = PooledConnection::extensions(&connection)
            .get::<PooledSearchPath>()
            .is_some_and(|current| current.0 == search_path);
        if !search_path_is_current {
            connection
                .batch_execute(&format!("SET search_path TO \"{search_path}\", public"))
                .map_err(|error| {
                    StorageError::Other(format!(
                        "postgres search_path initialization failed for `{}`: {error}",
                        self.inner.config.logical_identity
                    ))
                })?;
            self.inner
                .search_path_switch_count
                .fetch_add(1, Ordering::Relaxed);
            PooledConnection::extensions_mut(&mut connection)
                .insert(PooledSearchPath(search_path.to_string()));
        }
        Ok(connection)
    }

    fn pool(&self, workload: PostgresWorkloadClass) -> &PostgresPoolInner {
        match workload {
            PostgresWorkloadClass::Critical => &self.inner.critical,
            PostgresWorkloadClass::OnlineRead => &self.inner.online_read,
            PostgresWorkloadClass::Background => &self.inner.background,
        }
    }

    pub fn checkout(
        &self,
        workload: PostgresWorkloadClass,
    ) -> Result<PostgresConnection, StorageError> {
        in_postgres_driver_context(|| self.checkout_raw(workload)).map(|connection| {
            PostgresConnection {
                inner: Some(connection),
                counters: Arc::clone(&self.pool(workload).counters),
            }
        })
    }

    pub fn checkout_critical(&self) -> Result<PostgresConnection, StorageError> {
        self.checkout(PostgresWorkloadClass::Critical)
    }

    pub fn checkout_online_read(&self) -> Result<PostgresConnection, StorageError> {
        self.checkout(PostgresWorkloadClass::OnlineRead)
    }

    pub fn checkout_background(&self) -> Result<PostgresConnection, StorageError> {
        self.checkout(PostgresWorkloadClass::Background)
    }

    #[must_use]
    pub fn health(&self) -> PostgresExecutorHealth {
        let lanes = [
            PostgresWorkloadClass::Critical,
            PostgresWorkloadClass::OnlineRead,
            PostgresWorkloadClass::Background,
        ]
        .into_iter()
        .map(|workload| {
            let pool = self.pool(workload);
            PostgresPoolLaneHealth {
                workload,
                max_connections: pool.max_connections,
                active_connections: pool.pool.as_ref().map_or(0, |pool| {
                    pool.state()
                        .connections
                        .saturating_sub(pool.state().idle_connections)
                }),
                idle_connections: pool
                    .pool
                    .as_ref()
                    .map_or(0, |pool| pool.state().idle_connections),
                metrics: pool_metrics(pool),
            }
        })
        .collect::<Vec<_>>();
        let unique_pools = unique_pools(&self.inner);
        PostgresExecutorHealth {
            logical_identity: self.inner.config.logical_identity.clone(),
            application_name: self.inner.config.application_name.clone(),
            max_connections: unique_pools.iter().map(|pool| pool.max_connections).sum(),
            metrics: unique_pools.iter().map(|pool| pool_metrics(pool)).fold(
                PostgresExecutorMetrics::default(),
                |mut total, metrics| {
                    total.checkout_count =
                        total.checkout_count.saturating_add(metrics.checkout_count);
                    total.checkout_timeout_count = total
                        .checkout_timeout_count
                        .saturating_add(metrics.checkout_timeout_count);
                    total.checkout_wait_ms = total
                        .checkout_wait_ms
                        .saturating_add(metrics.checkout_wait_ms);
                    total.checkout_wait_p50_ms =
                        total.checkout_wait_p50_ms.max(metrics.checkout_wait_p50_ms);
                    total.checkout_wait_p95_ms =
                        total.checkout_wait_p95_ms.max(metrics.checkout_wait_p95_ms);
                    total.checkout_wait_p99_ms =
                        total.checkout_wait_p99_ms.max(metrics.checkout_wait_p99_ms);
                    total.query_count = total.query_count.saturating_add(metrics.query_count);
                    total.query_error_count = total
                        .query_error_count
                        .saturating_add(metrics.query_error_count);
                    total.query_elapsed_ms = total
                        .query_elapsed_ms
                        .saturating_add(metrics.query_elapsed_ms);
                    total.transaction_count = total
                        .transaction_count
                        .saturating_add(metrics.transaction_count);
                    total.transaction_commit_count = total
                        .transaction_commit_count
                        .saturating_add(metrics.transaction_commit_count);
                    total.transaction_rollback_count = total
                        .transaction_rollback_count
                        .saturating_add(metrics.transaction_rollback_count);
                    total
                },
            ),
            migration_transaction_count: self
                .inner
                .migration_transaction_count
                .load(Ordering::Relaxed),
            runtime_readiness_query_count: self
                .inner
                .runtime_readiness_query_count
                .load(Ordering::Relaxed),
            search_path_switch_count: self.inner.search_path_switch_count.load(Ordering::Relaxed),
            lanes,
        }
    }

    #[must_use]
    pub fn logical_identity(&self) -> &str {
        &self.inner.config.logical_identity
    }

    #[must_use]
    pub fn migration_mode(&self) -> PostgresMigrationMode {
        self.inner.migration_mode
    }

    /// Applies one domain's migrations under a transaction-scoped advisory
    /// lock.  A checksum mismatch fails closed before any statement runs.
    pub fn apply_migrations(
        &self,
        domain: &str,
        specs: &[PostgresMigrationSpec],
    ) -> Result<Vec<PostgresMigrationReport>, StorageError> {
        if domain.trim().is_empty() {
            return Err(StorageError::Other(
                "postgres migration domain must not be empty".to_string(),
            ));
        }
        let mut spec_ids = BTreeSet::new();
        for spec in specs {
            if spec.domain != domain {
                return Err(StorageError::Other(format!(
                    "postgres migration `{}` targets `{}` but requested domain is `{domain}`",
                    spec.id, spec.domain
                )));
            }
            if !spec_ids.insert(spec.id) {
                return Err(StorageError::Other(format!(
                    "duplicate postgres migration id `{}` in domain `{domain}`",
                    spec.id
                )));
            }
        }

        let catalog_checksum = migration_catalog_checksum(specs);
        if self.inner.migration_mode == PostgresMigrationMode::RuntimeReadiness {
            let namespace = self.search_path.as_deref().unwrap_or("public").to_string();
            let key = (namespace, domain.to_string());
            let mut expected = self.inner.expected_migration_catalogs.lock().map_err(|_| {
                StorageError::Other("postgres expected migration catalog lock poisoned".to_string())
            })?;
            if let Some(previous) = expected.insert(key, catalog_checksum.clone()) {
                if previous != catalog_checksum {
                    return Err(StorageError::Other(format!(
                        "conflicting PostgreSQL schema catalogs registered for domain `{domain}`"
                    )));
                }
            }
            return Ok(migration_reports(specs, "catalog_registered"));
        }

        let mut connection = self.checkout_critical()?;
        self.ensure_migration_ledger_once(&mut connection)?;
        let applied = load_applied_migrations(&mut connection, specs)?;
        let mut reports = Vec::with_capacity(specs.len());
        for spec in specs {
            let checksum = spec.checksum();
            if let Some(applied_checksum) = applied.get(spec.id) {
                if applied_checksum != &checksum {
                    return Err(StorageError::Other(format!(
                        "postgres migration checksum mismatch for `{}` in domain `{}`",
                        spec.id, spec.domain
                    )));
                }
                reports.push(PostgresMigrationReport {
                    id: spec.id.to_string(),
                    domain: spec.domain.to_string(),
                    version: spec.version,
                    checksum,
                    status: "already_applied".to_string(),
                    description: spec.description.to_string(),
                });
                continue;
            }
            self.inner
                .migration_transaction_count
                .fetch_add(1, Ordering::Relaxed);
            reports.push(apply_migration(&mut connection, spec)?);
        }
        store_migration_catalog(&mut connection, domain, &catalog_checksum)?;
        Ok(reports)
    }

    /// Verify all schema catalogs registered by Runtime domain constructors.
    ///
    /// This performs one bounded query per PostgreSQL namespace, never a query
    /// per domain and never a migration. Missing or changed schemas fail closed
    /// with an explicit maintenance action.
    pub fn verify_registered_migration_catalogs(&self) -> Result<usize, StorageError> {
        if self.inner.migration_mode != PostgresMigrationMode::RuntimeReadiness {
            return Ok(0);
        }
        let expected = self
            .inner
            .expected_migration_catalogs
            .lock()
            .map_err(|_| {
                StorageError::Other("postgres expected migration catalog lock poisoned".to_string())
            })?
            .clone();
        let mut by_namespace = BTreeMap::<String, BTreeMap<String, String>>::new();
        for ((namespace, domain), checksum) in expected {
            by_namespace
                .entry(namespace)
                .or_default()
                .insert(domain, checksum);
        }
        let mut verified = 0usize;
        for (namespace, catalogs) in by_namespace {
            let executor = if namespace == "public" {
                self.clone()
            } else {
                self.scoped_namespace(&namespace)?
            };
            let domains = catalogs.keys().cloned().collect::<Vec<_>>();
            let mut connection = executor.checkout_critical()?;
            self.inner
                .runtime_readiness_query_count
                .fetch_add(1, Ordering::Relaxed);
            let rows = connection
                .query(
                    "SELECT domain, checksum FROM cowd_schema_catalogs
                     WHERE domain = ANY($1)",
                    &[&domains],
                )
                .map_err(|error| {
                    StorageError::Other(format!(
                        "PostgreSQL schema catalog is unavailable in namespace `{namespace}`: \
                         {error}; stop Gateway and run `cowd storage upgrade`"
                    ))
                })?;
            let actual = rows
                .into_iter()
                .map(|row| Ok((row.try_get::<_, String>(0)?, row.try_get::<_, String>(1)?)))
                .collect::<Result<BTreeMap<_, _>, PostgresError>>()?;
            for (domain, checksum) in catalogs {
                match actual.get(&domain) {
                    Some(actual) if actual == &checksum => verified = verified.saturating_add(1),
                    Some(_) => {
                        return Err(StorageError::Other(format!(
                            "PostgreSQL schema catalog mismatch for `{namespace}.{domain}`; \
                             stop Gateway and run `cowd storage upgrade`"
                        )));
                    }
                    None => {
                        return Err(StorageError::Other(format!(
                            "PostgreSQL schema catalog is missing `{namespace}.{domain}`; \
                             stop Gateway and run `cowd storage upgrade`"
                        )));
                    }
                }
            }
        }
        Ok(verified)
    }

    fn ensure_migration_ledger_once(
        &self,
        connection: &mut PostgresConnection,
    ) -> Result<(), StorageError> {
        let namespace = self.search_path.as_deref().unwrap_or("public").to_string();
        let mut initialized = self
            .inner
            .initialized_migration_ledgers
            .lock()
            .map_err(|_| StorageError::Other("postgres migration ledger lock poisoned".into()))?;
        if initialized.contains(&namespace) {
            return Ok(());
        }
        self.inner
            .migration_transaction_count
            .fetch_add(1, Ordering::Relaxed);
        ensure_migration_ledger(connection)?;
        initialized.insert(namespace);
        Ok(())
    }
}

impl PostgresPoolSet {
    pub fn connect(
        config: PostgresPoolSetConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, StorageError> {
        Self::connect_with_migration_mode(config, resolver, PostgresMigrationMode::Maintenance)
    }

    pub fn connect_with_migration_mode(
        mut config: PostgresPoolSetConfig,
        resolver: &dyn SecretRefResolver,
        migration_mode: PostgresMigrationMode,
    ) -> Result<Self, StorageError> {
        validate_connection_config(&config.connection)?;
        for (workload, lane) in [
            (PostgresWorkloadClass::Critical, config.critical),
            (PostgresWorkloadClass::OnlineRead, config.online_read),
            (PostgresWorkloadClass::Background, config.background),
        ] {
            if lane.max_connections == 0
                || lane
                    .min_idle_connections
                    .is_some_and(|minimum| minimum > lane.max_connections)
                || lane.checkout_timeout_ms == 0
            {
                return Err(StorageError::Other(format!(
                    "postgres {} lane requires a non-zero size/timeout and min idle <= max",
                    workload.as_str()
                )));
            }
        }
        let resolved = resolver.resolve_postgres_url(&config.connection.secret_ref)?;
        let base = parse_client_config(&config.connection, &resolved)?;
        let server_budget = probe_server_budget(&base, config.server_reserve)?;
        constrain_pool_set_to_server_budget(&mut config, server_budget)?;
        let critical = Arc::new(build_pool(
            &config.connection,
            base.clone(),
            PostgresWorkloadClass::Critical,
            config.critical,
        )?);
        let online_read = Arc::new(build_pool(
            &config.connection,
            base.clone(),
            PostgresWorkloadClass::OnlineRead,
            config.online_read,
        )?);
        let background = Arc::new(build_pool(
            &config.connection,
            base,
            PostgresWorkloadClass::Background,
            config.background,
        )?);
        Ok(Self {
            executor: PostgresExecutor {
                inner: Arc::new(PostgresExecutorInner {
                    config: config.connection,
                    critical,
                    online_read,
                    background,
                    migration_mode,
                    initialized_migration_ledgers: Mutex::new(BTreeSet::new()),
                    expected_migration_catalogs: Mutex::new(BTreeMap::new()),
                    migration_transaction_count: AtomicU64::new(0),
                    runtime_readiness_query_count: AtomicU64::new(0),
                    search_path_switch_count: AtomicU64::new(0),
                }),
                search_path: None,
            },
        })
    }

    #[must_use]
    pub fn executor(&self) -> PostgresExecutor {
        self.executor.clone()
    }

    #[must_use]
    pub fn health(&self) -> PostgresExecutorHealth {
        self.executor.health()
    }
}

fn probe_server_budget(config: &Config, reserve: u32) -> Result<u32, StorageError> {
    let mut client = config.connect(NoTls).map_err(|error| {
        StorageError::Other(format!(
            "postgres server-capacity probe could not connect: {error}"
        ))
    })?;
    let row = client.query_one("SHOW max_connections", &[])?;
    let configured: String = row.get(0);
    let server_max = configured.parse::<u32>().map_err(|error| {
        StorageError::Other(format!(
            "postgres max_connections probe returned `{configured}`: {error}"
        ))
    })?;
    server_max.checked_sub(reserve).filter(|value| *value >= 3).ok_or_else(|| {
        StorageError::Other(format!(
            "postgres max_connections {server_max} leaves fewer than three Cowd lane connections after reserve {reserve}"
        ))
    })
}

fn constrain_pool_set_to_server_budget(
    config: &mut PostgresPoolSetConfig,
    server_budget: u32,
) -> Result<(), StorageError> {
    let requested = config
        .critical
        .max_connections
        .saturating_add(config.online_read.max_connections)
        .saturating_add(config.background.max_connections);
    if requested == 0 {
        return Err(StorageError::Other(
            "postgres pool-set connection budget must be non-zero".to_string(),
        ));
    }
    let effective = config.connection.max_connections.min(server_budget);
    if requested > effective {
        let mut lanes = [
            ((u64::from(config.critical.max_connections) * u64::from(effective))
                / u64::from(requested))
            .max(1) as u32,
            ((u64::from(config.online_read.max_connections) * u64::from(effective))
                / u64::from(requested))
            .max(1) as u32,
            ((u64::from(config.background.max_connections) * u64::from(effective))
                / u64::from(requested))
            .max(1) as u32,
        ];
        while lanes.iter().sum::<u32>() > effective {
            let Some((index, _)) = lanes
                .iter()
                .enumerate()
                .filter(|(_, value)| **value > 1)
                .max_by_key(|(_, value)| **value)
            else {
                return Err(StorageError::Other(
                    "postgres server budget cannot preserve three isolated workload lanes"
                        .to_string(),
                ));
            };
            lanes[index] -= 1;
        }
        while lanes.iter().sum::<u32>() < effective {
            lanes[1] += 1;
        }
        config.critical.max_connections = lanes[0];
        config.online_read.max_connections = lanes[1];
        config.background.max_connections = lanes[2];
    }
    for lane in [
        &mut config.critical,
        &mut config.online_read,
        &mut config.background,
    ] {
        lane.min_idle_connections = lane
            .min_idle_connections
            .map(|minimum| minimum.min(lane.max_connections));
    }
    config.connection.max_connections = effective;
    Ok(())
}

fn validate_connection_config(config: &PostgresConnectionConfig) -> Result<(), StorageError> {
    if config.logical_identity.trim().is_empty()
        || config.secret_ref.trim().is_empty()
        || config.application_name.trim().is_empty()
        || config.max_connections == 0
    {
        return Err(StorageError::Other(
            "postgres executor requires identity, secret reference, application name, and a non-zero pool size"
                .to_string(),
        ));
    }
    Ok(())
}

fn parse_client_config(
    config: &PostgresConnectionConfig,
    resolved: &ResolvedPostgresUrl,
) -> Result<Config, StorageError> {
    Config::from_str(resolved.as_str()).map_err(|_| {
        StorageError::Other(format!(
            "postgres connection config for `{}` is invalid",
            config.logical_identity
        ))
    })
}

fn build_pool(
    config: &PostgresConnectionConfig,
    mut client_config: Config,
    workload: PostgresWorkloadClass,
    lane: PostgresPoolLaneConfig,
) -> Result<PostgresPoolInner, StorageError> {
    client_config.application_name(&format!(
        "{}-{}",
        config.application_name,
        workload.as_str()
    ));
    let manager = PostgresConnectionManager::new(client_config, NoTls);
    let mut builder = Pool::builder()
        .max_size(lane.max_connections)
        .connection_timeout(Duration::from_millis(lane.checkout_timeout_ms));
    if let Some(minimum) = lane.min_idle_connections {
        builder = builder.min_idle(Some(minimum));
    }
    let pool = builder.build(manager).map_err(|error| {
        StorageError::Other(format!(
            "postgres {} pool for `{}` could not be created: {error}",
            workload.as_str(),
            config.logical_identity
        ))
    })?;
    Ok(PostgresPoolInner {
        max_connections: lane.max_connections,
        pool: Some(pool),
        counters: Arc::new(PostgresExecutorCounters::default()),
    })
}

fn pool_metrics(pool: &PostgresPoolInner) -> PostgresExecutorMetrics {
    let bucket_counts = pool
        .counters
        .checkout_wait_buckets
        .iter()
        .map(|bucket| bucket.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    PostgresExecutorMetrics {
        checkout_count: pool.counters.checkout_count.load(Ordering::Relaxed),
        checkout_timeout_count: pool.counters.checkout_timeout_count.load(Ordering::Relaxed),
        checkout_wait_ms: pool.counters.checkout_wait_ms.load(Ordering::Relaxed),
        checkout_wait_p50_ms: histogram_percentile(&bucket_counts, 50),
        checkout_wait_p95_ms: histogram_percentile(&bucket_counts, 95),
        checkout_wait_p99_ms: histogram_percentile(&bucket_counts, 99),
        query_count: pool.counters.query_count.load(Ordering::Relaxed),
        query_error_count: pool.counters.query_error_count.load(Ordering::Relaxed),
        query_elapsed_ms: pool.counters.query_elapsed_ms.load(Ordering::Relaxed),
        transaction_count: pool.counters.transaction_count.load(Ordering::Relaxed),
        transaction_commit_count: pool
            .counters
            .transaction_commit_count
            .load(Ordering::Relaxed),
        transaction_rollback_count: pool
            .counters
            .transaction_rollback_count
            .load(Ordering::Relaxed),
    }
}

fn histogram_percentile(bucket_counts: &[u64], percentile: u64) -> u64 {
    let total = bucket_counts.iter().copied().sum::<u64>();
    if total == 0 {
        return 0;
    }
    let target = total.saturating_mul(percentile).saturating_add(99) / 100;
    let mut cumulative = 0_u64;
    for (index, count) in bucket_counts.iter().copied().enumerate() {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= target {
            return CHECKOUT_WAIT_BUCKET_MS
                .get(index)
                .copied()
                .unwrap_or(u64::MAX);
        }
    }
    u64::MAX
}

fn unique_pools(inner: &PostgresExecutorInner) -> Vec<&PostgresPoolInner> {
    let mut pools = vec![inner.critical.as_ref()];
    if !Arc::ptr_eq(&inner.critical, &inner.online_read) {
        pools.push(inner.online_read.as_ref());
    }
    if !Arc::ptr_eq(&inner.critical, &inner.background)
        && !Arc::ptr_eq(&inner.online_read, &inner.background)
    {
        pools.push(inner.background.as_ref());
    }
    pools
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresMigrationSpec {
    pub id: &'static str,
    pub domain: &'static str,
    pub version: i64,
    pub description: &'static str,
    pub statements: &'static [&'static str],
}

impl PostgresMigrationSpec {
    #[must_use]
    pub fn checksum(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.id.as_bytes());
        digest.update([0]);
        digest.update(self.domain.as_bytes());
        digest.update([0]);
        digest.update(self.version.to_le_bytes());
        digest.update([0]);
        digest.update(self.description.as_bytes());
        for statement in self.statements {
            digest.update([0]);
            digest.update(statement.as_bytes());
        }
        format!("{:x}", digest.finalize())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresMigrationReport {
    pub id: String,
    pub domain: String,
    pub version: i64,
    pub checksum: String,
    pub status: String,
    pub description: String,
}

fn ensure_migration_ledger(connection: &mut PostgresConnection) -> Result<(), StorageError> {
    // Independent durable domains may initialize concurrently during process
    // composition. PostgreSQL can race on catalog insertion even for
    // `CREATE TABLE IF NOT EXISTS`, so serialize only this tiny ledger setup;
    // each domain's actual migration retains its own transaction-scoped lock.
    let mut transaction = connection.transaction()?;
    transaction.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1))",
        &[&"cowd-storage:migration-ledger"],
    )?;
    transaction.batch_execute(
        "CREATE TABLE IF NOT EXISTS cowd_schema_migrations (
            id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            version BIGINT NOT NULL,
            checksum TEXT NOT NULL,
            description TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS cowd_schema_catalogs (
            domain TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );",
    )?;
    transaction.commit()?;
    Ok(())
}

fn migration_catalog_checksum(specs: &[PostgresMigrationSpec]) -> String {
    let mut digest = Sha256::new();
    for spec in specs {
        digest.update(spec.id.as_bytes());
        digest.update([0]);
        digest.update(spec.checksum().as_bytes());
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

fn migration_reports(
    specs: &[PostgresMigrationSpec],
    status: &str,
) -> Vec<PostgresMigrationReport> {
    specs
        .iter()
        .map(|spec| PostgresMigrationReport {
            id: spec.id.to_string(),
            domain: spec.domain.to_string(),
            version: spec.version,
            checksum: spec.checksum(),
            status: status.to_string(),
            description: spec.description.to_string(),
        })
        .collect()
}

fn store_migration_catalog(
    connection: &mut PostgresConnection,
    domain: &str,
    checksum: &str,
) -> Result<(), StorageError> {
    connection.execute(
        "INSERT INTO cowd_schema_catalogs(domain, checksum, verified_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT(domain) DO UPDATE
             SET checksum=EXCLUDED.checksum, verified_at=EXCLUDED.verified_at",
        &[&domain, &checksum],
    )?;
    Ok(())
}

fn load_applied_migrations(
    connection: &mut PostgresConnection,
    specs: &[PostgresMigrationSpec],
) -> Result<BTreeMap<String, String>, StorageError> {
    if specs.is_empty() {
        return Ok(BTreeMap::new());
    }
    let ids = specs
        .iter()
        .map(|spec| spec.id.to_string())
        .collect::<Vec<_>>();
    connection
        .query(
            "SELECT id, checksum FROM cowd_schema_migrations WHERE id = ANY($1)",
            &[&ids],
        )?
        .into_iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect::<Result<BTreeMap<String, String>, PostgresError>>()
        .map_err(StorageError::from)
}

fn apply_migration(
    connection: &mut PostgresConnection,
    spec: &PostgresMigrationSpec,
) -> Result<PostgresMigrationReport, StorageError> {
    let checksum = spec.checksum();
    let mut transaction = connection.transaction()?;
    transaction.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1))",
        &[&format!("cowd-storage:{}", spec.domain)],
    )?;
    if let Some(row) = transaction.query_opt(
        "SELECT checksum FROM cowd_schema_migrations WHERE id = $1",
        &[&spec.id],
    )? {
        let applied_checksum: String = row.get(0);
        if applied_checksum != checksum {
            return Err(StorageError::Other(format!(
                "postgres migration checksum mismatch for `{}` in domain `{}`",
                spec.id, spec.domain
            )));
        }
        transaction.commit()?;
        return Ok(PostgresMigrationReport {
            id: spec.id.to_string(),
            domain: spec.domain.to_string(),
            version: spec.version,
            checksum,
            status: "already_applied".to_string(),
            description: spec.description.to_string(),
        });
    }
    for statement in spec.statements {
        transaction.batch_execute(statement)?;
    }
    transaction.execute(
        "INSERT INTO cowd_schema_migrations(id, domain, version, checksum, description)
         VALUES ($1, $2, $3, $4, $5)",
        &[
            &spec.id,
            &spec.domain,
            &spec.version,
            &checksum,
            &spec.description,
        ],
    )?;
    transaction.commit()?;
    Ok(PostgresMigrationReport {
        id: spec.id.to_string(),
        domain: spec.domain.to_string(),
        version: spec.version,
        checksum,
        status: "applied".to_string(),
        description: spec.description.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_pool_set() -> PostgresPoolSet {
        let url = std::env::var("COWD_TEST_POSTGRES_URL")
            .expect("COWD_TEST_POSTGRES_URL must name an isolated disposable database");
        assert!(
            !url.trim().is_empty(),
            "COWD_TEST_POSTGRES_URL must not be empty"
        );
        let resolver = StaticSecretRefResolver::new([("storage-pool-set-test".to_string(), url)]);
        let connection = PostgresConnectionConfig {
            max_connections: 5,
            min_idle_connections: None,
            checkout_timeout_ms: 500,
            ..PostgresConnectionConfig::new(
                "storage-pool-set-test",
                "storage-pool-set-test",
                "cowd-storage-pool-set-test",
            )
        };
        PostgresPoolSet::connect(
            PostgresPoolSetConfig {
                connection,
                server_reserve: 1,
                critical: PostgresPoolLaneConfig::new(2, Some(1), 500),
                online_read: PostgresPoolLaneConfig::new(2, Some(1), 500),
                background: PostgresPoolLaneConfig::new(1, Some(1), 100),
            },
            &resolver,
        )
        .expect("real PostgreSQL pool set")
    }

    #[test]
    fn resolved_postgres_url_is_redacted_from_debug_output() {
        let resolved = ResolvedPostgresUrl::new("postgres://user:secret@localhost/cowd");
        let debug = format!("{resolved:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn postgres_migration_checksum_is_stable_and_statement_sensitive() {
        let original = PostgresMigrationSpec {
            id: "test.0001",
            domain: "test",
            version: 1,
            description: "test",
            statements: &["CREATE TABLE test(id TEXT PRIMARY KEY)"],
        };
        let changed = PostgresMigrationSpec {
            statements: &["CREATE TABLE test(id TEXT PRIMARY KEY, state TEXT NOT NULL)"],
            ..original.clone()
        };
        assert_ne!(original.checksum(), changed.checksum());
        assert_ne!(
            migration_catalog_checksum(std::slice::from_ref(&original)),
            migration_catalog_checksum(std::slice::from_ref(&changed))
        );
    }

    #[test]
    fn pool_set_budget_preserves_all_lanes_when_server_capacity_is_lower() {
        let connection = PostgresConnectionConfig::new("test", "env:TEST", "cowd-test");
        let mut config = PostgresPoolSetConfig {
            connection: PostgresConnectionConfig {
                max_connections: 48,
                ..connection
            },
            server_reserve: 8,
            critical: PostgresPoolLaneConfig::new(16, Some(3), 250),
            online_read: PostgresPoolLaneConfig::new(24, Some(4), 500),
            background: PostgresPoolLaneConfig::new(8, Some(2), 2_000),
        };
        constrain_pool_set_to_server_budget(&mut config, 24).expect("calibrate");
        assert_eq!(
            config.critical.max_connections
                + config.online_read.max_connections
                + config.background.max_connections,
            24
        );
        assert!(config.critical.max_connections > 0);
        assert!(config.online_read.max_connections > 0);
        assert!(config.background.max_connections > 0);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_pool_set_isolates_background_saturation_from_critical_writes() {
        let pool_set = real_pool_set();
        let executor = pool_set.executor();
        let _background = executor
            .checkout_background()
            .expect("occupy the only background connection");
        let started = Instant::now();
        let mut critical = executor
            .checkout_critical()
            .expect("critical checkout must remain available");
        critical.query_one("SELECT 1", &[]).expect("critical query");
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "critical checkout waited behind background saturation"
        );
        let health = pool_set.health();
        assert_eq!(health.lanes.len(), 3);
        assert!(health.metrics.checkout_count >= 2);
        assert!(health.metrics.query_count >= 1);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_pool_set_resets_search_path_between_scoped_and_public_checkouts() {
        let pool_set = real_pool_set();
        let executor = pool_set.executor();
        let schema = format!("storage_test_{}", std::process::id());
        executor
            .checkout_critical()
            .expect("admin checkout")
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS \"{schema}\" CASCADE; CREATE SCHEMA \"{schema}\""
            ))
            .expect("create test schema");
        let scoped = executor.scoped_namespace(&schema).expect("scoped executor");
        scoped
            .checkout_critical()
            .expect("scoped checkout")
            .batch_execute("CREATE TABLE namespace_probe(value INTEGER NOT NULL)")
            .expect("create scoped table");

        let scoped_path: String = scoped
            .checkout_online_read()
            .expect("scoped read")
            .query_one("SHOW search_path", &[])
            .expect("read scoped path")
            .get(0);
        assert!(scoped_path.contains(&schema));

        let public_path: String = executor
            .checkout_online_read()
            .expect("public read")
            .query_one("SHOW search_path", &[])
            .expect("read public path")
            .get(0);
        assert!(public_path.contains("public"));
        assert!(!public_path.contains(&schema));
        assert!(executor
            .checkout_online_read()
            .expect("public table probe")
            .query_opt("SELECT to_regclass('namespace_probe')::text", &[])
            .expect("public table probe query")
            .and_then(|row| row.get::<_, Option<String>>(0))
            .is_none());

        executor
            .checkout_critical()
            .expect("cleanup checkout")
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .expect("drop test schema");
    }
}
