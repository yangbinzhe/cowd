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

/// Connection count a test fixture pool should use, sized off the concurrency
/// the suite will actually run.
///
/// Two properties of the executor make the fixture value different from the
/// production default:
///
/// 1. The three workload lanes share ONE pool (`connect_with_migration_mode`
///    clones the same handle into all three), so `max_connections` is the
///    ceiling for every test thread regardless of which lane it uses.
/// 2. `cargo test` runs one thread per core by default, and a fixture holds a
///    connection while it creates its schema and runs migrations. Some tests
///    hold two (a fixture plus a `reconnect`ed pool), so the ceiling needs
///    headroom over the thread count rather than matching it exactly.
///
/// A pool smaller than that turns any slow query into mass starvation: waiters
/// queue behind the checkout timeout and fail as a group, which reads as a
/// flaky fixture rather than a mis-sized pool.
///
/// It is deliberately NOT sized by doubling the machine's core count. When the
/// suite is split across concurrent `cargo test` processes each process still
/// derives its own ceiling from the core count, so N processes multiply the
/// cluster-wide connection demand by N for no reason. Set
/// `COWD_TEST_POOL_CONNECTIONS` to the real per-process concurrency (usually the
/// `--test-threads` value) when sharding the suite.
///
/// Production callers set `max_connections` explicitly; this is for fixtures.
#[must_use]
pub fn test_fixture_pool_size() -> u32 {
    test_fixture_pool_budget(1, None)
        .unwrap_or_else(|error| panic!("invalid PostgreSQL test fixture budget: {error}"))
}

/// Checked total connection budget for one test process, not per lane or server.
/// An absent override uses `fallback`, or the original CPU rule when it is None.
pub fn test_fixture_pool_budget(minimum: u32, fallback: Option<u32>) -> Result<u32, String> {
    let explicit = match std::env::var("COWD_TEST_POOL_CONNECTIONS") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err("COWD_TEST_POOL_CONNECTIONS must be a positive integer".into());
        }
    };
    checked_fixture_pool_budget(explicit.as_deref(), minimum, fallback)
}

fn checked_fixture_pool_budget(
    explicit: Option<&str>,
    minimum: u32,
    fallback: Option<u32>,
) -> Result<u32, String> {
    let budget = match explicit {
        Some(value) => value
            .trim()
            .parse::<u32>()
            .map_err(|_| "COWD_TEST_POOL_CONNECTIONS must be a positive integer".to_string())?,
        None => fallback.unwrap_or_else(|| fixture_pool_size(None)),
    };
    if budget < minimum.max(1) {
        return Err(format!(
            "COWD_TEST_POOL_CONNECTIONS must be at least {} for this fixture",
            minimum.max(1)
        ));
    }
    Ok(budget)
}

/// Snapshot a fixture's resolved URL for a private cache key and connection.
/// The token covers the actual URL (including credentials), not the reference.
/// This fixes client configuration only; it does not authenticate server identity.
pub fn test_fixture_postgres_url_snapshot(
    secret_ref: &str,
    resolved: &ResolvedPostgresUrl,
) -> ([u8; 32], StaticSecretRefResolver) {
    let token = Sha256::digest(resolved.as_str().as_bytes()).into();
    let resolver =
        StaticSecretRefResolver::new([(secret_ref.to_string(), resolved.as_str().to_string())]);
    (token, resolver)
}

/// The sizing rule itself, with the environment already resolved.
///
/// Split out so tests can exercise both branches WITHOUT mutating process-global
/// state: one test setting the variable while another clears it, both on parallel
/// threads, races - and a test that races proves nothing.
fn fixture_pool_size(explicit: Option<u32>) -> u32 {
    if let Some(explicit) = explicit {
        return explicit;
    }
    let threads = std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    // Half again over the thread count, not double: it covers the tests that
    // hold a second pool while staying proportional to real concurrency.
    u32::try_from(threads.saturating_mul(3) / 2).unwrap_or(u32::MAX)
}

/// Checkout ceiling a test fixture should use.
///
/// Production traffic gives up fast so a caller is not stuck behind a broken
/// backend. A fixture is different: it is the only thing running, so a short
/// ceiling converts one slow query into a queue-wide failure instead of just
/// making that one test slower.
#[must_use]
pub const fn test_fixture_checkout_timeout_ms() -> u64 {
    30_000
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
    pub transaction_hold_ms: u64,
    pub transaction_hold_p50_ms: u64,
    pub transaction_hold_p95_ms: u64,
    pub transaction_hold_p99_ms: u64,
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
    transaction_hold_ms: AtomicU64,
    transaction_hold_buckets: [AtomicU64; CHECKOUT_WAIT_BUCKET_MS.len()],
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
            transaction_hold_ms: AtomicU64::new(0),
            transaction_hold_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
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

    fn record_transaction_hold(&self, elapsed: Duration) {
        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.transaction_hold_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        let bucket = CHECKOUT_WAIT_BUCKET_MS
            .iter()
            .position(|upper| elapsed_ms <= *upper)
            .unwrap_or(CHECKOUT_WAIT_BUCKET_MS.len() - 1);
        self.transaction_hold_buckets[bucket].fetch_add(1, Ordering::Relaxed);
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
    expected_migration_catalogs: Mutex<BTreeMap<(String, String), ExpectedMigrationCatalog>>,
    migration_transaction_count: AtomicU64,
    runtime_readiness_query_count: AtomicU64,
    search_path_switch_count: AtomicU64,
}

#[derive(Clone, PartialEq, Eq)]
struct ExpectedMigrationCatalog {
    checksum: String,
    baseline: Option<(String, String)>,
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
    started: Instant,
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
            started: Instant::now(),
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
        self.counters
            .record_transaction_hold(self.started.elapsed());
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
        self.counters
            .record_transaction_hold(self.started.elapsed());
        result
    }
}

impl_postgres_client!(PostgresTransaction<'_>);

impl Drop for PostgresTransaction<'_> {
    fn drop(&mut self) {
        if let Some(transaction) = self.inner.take() {
            in_postgres_driver_context(|| drop(transaction));
            self.counters
                .record_transaction_hold(self.started.elapsed());
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
                    total.transaction_hold_ms = total
                        .transaction_hold_ms
                        .saturating_add(metrics.transaction_hold_ms);
                    total.transaction_hold_p50_ms = total
                        .transaction_hold_p50_ms
                        .max(metrics.transaction_hold_p50_ms);
                    total.transaction_hold_p95_ms = total
                        .transaction_hold_p95_ms
                        .max(metrics.transaction_hold_p95_ms);
                    total.transaction_hold_p99_ms = total
                        .transaction_hold_p99_ms
                        .max(metrics.transaction_hold_p99_ms);
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

        let baseline = specs
            .iter()
            .find(|spec| spec.id.ends_with(".current-baseline"));
        if baseline.is_some() && specs.len() != 1 {
            return Err(StorageError::Other(
                "a current PostgreSQL baseline must be the domain's only migration".into(),
            ));
        }
        let catalog_checksum = migration_catalog_checksum(specs);
        if self.inner.migration_mode == PostgresMigrationMode::RuntimeReadiness {
            let namespace = self.search_path.as_deref().unwrap_or("public").to_string();
            let key = (namespace, domain.to_string());
            let mut expected = self.inner.expected_migration_catalogs.lock().map_err(|_| {
                StorageError::Other("postgres expected migration catalog lock poisoned".to_string())
            })?;
            let catalog = ExpectedMigrationCatalog {
                checksum: catalog_checksum.clone(),
                baseline: baseline.map(|spec| (spec.id.to_string(), spec.checksum())),
            };
            if let Some(previous) = expected.get(&key) {
                if previous != &catalog {
                    return Err(StorageError::Other(format!(
                        "conflicting PostgreSQL schema catalogs registered for domain `{domain}`"
                    )));
                }
            } else {
                expected.insert(key, catalog);
            }
            return Ok(migration_reports(specs, "catalog_registered"));
        }

        let mut connection = self.checkout_critical()?;
        if let Some(spec) = baseline {
            self.inner
                .migration_transaction_count
                .fetch_add(1, Ordering::Relaxed);
            let mut transaction = connection.transaction()?;
            transaction.query_one(
                "SELECT pg_advisory_xact_lock(hashtext($1))",
                &[&format!("cowd-storage:{domain}")],
            )?;
            let namespace = self.search_path.as_deref().unwrap_or("public");
            transaction.batch_execute(&format!("SET LOCAL search_path TO \"{namespace}\""))?;
            let exists: bool = transaction
                .query_one("SELECT current_schema() = $1", &[&namespace])?
                .try_get::<_, Option<bool>>(0)?
                .unwrap_or(false);
            if !exists {
                return Err(StorageError::Other(
                    "PostgreSQL baseline namespace is unavailable".into(),
                ));
            }
            validate_clean_baseline_catalog(&mut transaction, spec)?;
            ensure_migration_ledger_in_transaction(&mut transaction)?;
            let report = apply_migration_in_transaction(&mut transaction, spec)?;
            store_migration_catalog(&mut transaction, domain, &catalog_checksum)?;
            transaction.commit()?;
            return Ok(vec![report]);
        }
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
        let mut by_namespace =
            BTreeMap::<String, BTreeMap<String, ExpectedMigrationCatalog>>::new();
        for ((namespace, domain), catalog) in expected {
            by_namespace
                .entry(namespace)
                .or_default()
                .insert(domain, catalog);
        }
        let mut verified = 0usize;
        for (namespace, catalogs) in by_namespace {
            let executor = self.scoped_namespace(&namespace)?;
            let domains = catalogs.keys().cloned().collect::<Vec<_>>();
            let baseline_domains = catalogs
                .iter()
                .filter(|(_, catalog)| catalog.baseline.is_some())
                .map(|(domain, _)| domain.clone())
                .collect::<Vec<_>>();
            let mut connection = executor.checkout_critical()?;
            self.inner
                .runtime_readiness_query_count
                .fetch_add(1, Ordering::Relaxed);
            let rows = connection
                .query(
                    &format!(
                        "SELECT requested.domain, catalog.checksum, migration.id, migration.checksum
                         FROM unnest($1::TEXT[]) AS requested(domain)
                         LEFT JOIN \"{namespace}\".cowd_schema_catalogs AS catalog
                           ON catalog.domain = requested.domain
                         LEFT JOIN LATERAL (
                             SELECT id, checksum FROM \"{namespace}\".cowd_schema_migrations
                             WHERE domain = requested.domain AND requested.domain = ANY($2::TEXT[])
                             ORDER BY id LIMIT 2
                         ) AS migration ON TRUE"
                    ),
                    &[&domains, &baseline_domains],
                )
                .map_err(|_| {
                    StorageError::Other(format!(
                        "PostgreSQL schema catalog is unavailable in namespace `{namespace}`: \
                         stop Gateway and run `cowd storage upgrade`"
                    ))
                })?;
            let mut identities = BTreeMap::<String, Vec<(String, String)>>::new();
            for row in rows {
                let domain: String = row.try_get(0)?;
                let checksum: Option<String> = row.try_get(1)?;
                let catalog = catalogs.get(&domain).ok_or_else(|| {
                    StorageError::Other("unregistered PostgreSQL schema catalog".into())
                })?;
                if checksum.as_deref() != Some(catalog.checksum.as_str()) {
                    return Err(StorageError::Other(format!(
                        "PostgreSQL schema catalog missing or mismatched for `{namespace}.{domain}`; \
                         stop Gateway and run `cowd storage upgrade`"
                    )));
                }
                let domain_identities = identities.entry(domain).or_default();
                if let Some(id) = row.try_get::<_, Option<String>>(2)? {
                    domain_identities.push((id, row.try_get(3)?));
                }
            }
            for (domain, catalog) in catalogs {
                let actual = identities.get(&domain);
                if actual.is_none()
                    || catalog.baseline.as_ref().is_some_and(|baseline| {
                        actual.map(Vec::as_slice) != Some(std::slice::from_ref(baseline))
                    })
                {
                    return Err(StorageError::Other(format!(
                        "PostgreSQL baseline catalog mismatch for `{namespace}.{domain}`; select a clean namespace"
                    )));
                }
                verified = verified.saturating_add(1);
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
    // Pool initialization may drop partially connected synchronous clients on
    // failure. Keep that cleanup outside the caller's Tokio runtime too.
    let pool = in_postgres_driver_context(move || builder.build(manager)).map_err(|error| {
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
        transaction_hold_ms: pool.counters.transaction_hold_ms.load(Ordering::Relaxed),
        transaction_hold_p50_ms: histogram_percentile(
            &pool
                .counters
                .transaction_hold_buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            50,
        ),
        transaction_hold_p95_ms: histogram_percentile(
            &pool
                .counters
                .transaction_hold_buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            95,
        ),
        transaction_hold_p99_ms: histogram_percentile(
            &pool
                .counters
                .transaction_hold_buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            99,
        ),
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
    let mut transaction = connection.transaction()?;
    ensure_migration_ledger_in_transaction(&mut transaction)?;
    transaction.commit()?;
    Ok(())
}

fn ensure_migration_ledger_in_transaction(
    transaction: &mut impl PostgresClient,
) -> Result<(), StorageError> {
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
    Ok(())
}

fn validate_clean_baseline_catalog(
    transaction: &mut impl PostgresClient,
    spec: &PostgresMigrationSpec,
) -> Result<(), StorageError> {
    let exists: bool = transaction
        .query_one(
            "SELECT to_regclass('cowd_schema_migrations') IS NOT NULL",
            &[],
        )?
        .try_get(0)?;
    if !exists {
        return Ok(());
    }
    let rows = transaction.query(
        "SELECT id, checksum, domain FROM cowd_schema_migrations
         WHERE domain = $1 OR id = $2 ORDER BY id LIMIT 2",
        &[&spec.domain, &spec.id],
    )?;
    let valid = match rows.as_slice() {
        [] => true,
        [row] => {
            row.try_get::<_, String>(0)? == spec.id
                && row.try_get::<_, String>(1)? == spec.checksum()
                && row.try_get::<_, String>(2)? == spec.domain
        }
        _ => false,
    };
    if !valid {
        return Err(StorageError::Other(
            "unsupported PostgreSQL baseline catalog; select a clean namespace".into(),
        ));
    }
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
    connection: &mut impl PostgresClient,
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
    let mut transaction = connection.transaction()?;
    transaction.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1))",
        &[&format!("cowd-storage:{}", spec.domain)],
    )?;
    let report = apply_migration_in_transaction(&mut transaction, spec)?;
    transaction.commit()?;
    Ok(report)
}

fn apply_migration_in_transaction(
    transaction: &mut impl PostgresClient,
    spec: &PostgresMigrationSpec,
) -> Result<PostgresMigrationReport, StorageError> {
    let checksum = spec.checksum();
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

    const CLEAN_BASELINE: PostgresMigrationSpec = PostgresMigrationSpec {
        id: "catalog_probe.current-baseline",
        domain: "catalog_probe",
        version: 1,
        description: "atomic current baseline fixture",
        statements: &[
            "CREATE TABLE catalog_probe_data(value TEXT NOT NULL)",
            "INSERT INTO catalog_probe_data VALUES('preserved')",
        ],
    };

    fn readiness_executor(fixture: &crate::postgres_scope::PostgresTestScope) -> PostgresExecutor {
        let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated PostgreSQL required");
        let resolver = StaticSecretRefResolver::new([("catalog-readiness".to_string(), url)]);
        let mut config = PostgresConnectionConfig::new(
            "catalog-readiness",
            "catalog-readiness",
            "catalog-readiness",
        );
        config.max_connections = test_fixture_pool_size();
        config.checkout_timeout_ms = test_fixture_checkout_timeout_ms();
        let executor = PostgresExecutor::connect_with_migration_mode(
            config,
            &resolver,
            PostgresMigrationMode::RuntimeReadiness,
        )
        .unwrap();
        fixture.bind(&executor)
    }

    fn baseline_snapshot(executor: &PostgresExecutor) -> String {
        executor.checkout_online_read().unwrap().query_one(
            "SELECT jsonb_build_object(
                'migrations', (SELECT jsonb_agg(to_jsonb(record) ORDER BY id) FROM cowd_schema_migrations record),
                'catalogs', (SELECT jsonb_agg(to_jsonb(record) ORDER BY domain) FROM cowd_schema_catalogs record),
                'data', (SELECT jsonb_agg(value ORDER BY value) FROM catalog_probe_data),
                'relations', (SELECT jsonb_agg(jsonb_build_array(oid::TEXT, relname, relkind::TEXT) ORDER BY relname)
                              FROM pg_class WHERE relnamespace = current_schema()::regnamespace),
                'columns', (SELECT jsonb_agg(jsonb_build_array(attrelid::TEXT, attname, atttypid::TEXT, attnotnull) ORDER BY attrelid, attnum)
                            FROM pg_attribute WHERE attrelid IN (SELECT oid FROM pg_class WHERE relnamespace = current_schema()::regnamespace)),
                'indexes', (SELECT jsonb_agg(indexdef ORDER BY indexname) FROM pg_indexes WHERE schemaname = current_schema())
            )::TEXT", &[],
        ).unwrap().get(0)
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_clean_baseline_catalog_is_atomic_and_readiness_rejects_extra_ids() {
        let fixture = crate::postgres_scope::PostgresTestScope::new();
        let executor = fixture.reconnect();
        let readiness = readiness_executor(&fixture);
        let before_registration = readiness.health();
        assert_eq!(
            readiness
                .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
                .unwrap()[0]
                .status,
            "catalog_registered"
        );
        assert_eq!(
            readiness.health().metrics.query_count,
            before_registration.metrics.query_count
        );
        assert!(readiness.verify_registered_migration_catalogs().is_err());
        assert_eq!(readiness.health().migration_transaction_count, 0);
        assert!(!executor.checkout_online_read().unwrap().query_one(
            "SELECT to_regclass('cowd_schema_migrations') IS NOT NULL OR to_regclass('cowd_schema_catalogs') IS NOT NULL", &[],
        ).unwrap().get::<_, bool>(0));

        assert_eq!(
            executor
                .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
                .unwrap()[0]
                .status,
            "applied"
        );
        assert_eq!(
            executor
                .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
                .unwrap()[0]
                .status,
            "already_applied"
        );
        let before_readiness = baseline_snapshot(&executor);
        let query_count = readiness.health().runtime_readiness_query_count;
        assert_eq!(readiness.verify_registered_migration_catalogs().unwrap(), 1);
        assert_eq!(
            readiness.health().runtime_readiness_query_count - query_count,
            1
        );
        assert_eq!(baseline_snapshot(&executor), before_readiness);
        executor
            .checkout_critical()
            .unwrap()
            .execute(
                "UPDATE cowd_schema_catalogs SET checksum='unknown-catalog' WHERE domain=$1",
                &[&CLEAN_BASELINE.domain],
            )
            .unwrap();
        let unknown_catalog = baseline_snapshot(&executor);
        assert!(readiness.verify_registered_migration_catalogs().is_err());
        assert_eq!(baseline_snapshot(&executor), unknown_catalog);
        executor
            .checkout_critical()
            .unwrap()
            .execute(
                "UPDATE cowd_schema_catalogs SET checksum=$1 WHERE domain=$2",
                &[
                    &migration_catalog_checksum(&[CLEAN_BASELINE.clone()]),
                    &CLEAN_BASELINE.domain,
                ],
            )
            .unwrap();
        let conflict = PostgresMigrationSpec {
            description: "conflicting expected checksum",
            ..CLEAN_BASELINE.clone()
        };
        assert!(readiness
            .apply_migrations(conflict.domain, &[conflict])
            .is_err());
        assert_eq!(readiness.verify_registered_migration_catalogs().unwrap(), 1);
        let mixed = PostgresMigrationSpec {
            id: "catalog_probe.other",
            ..CLEAN_BASELINE.clone()
        };
        for candidate in [&executor, &readiness] {
            assert!(candidate
                .apply_migrations(
                    CLEAN_BASELINE.domain,
                    &[CLEAN_BASELINE.clone(), mixed.clone()]
                )
                .is_err());
        }

        executor.checkout_critical().unwrap().execute(
            "INSERT INTO cowd_schema_migrations(id,domain,version,checksum,description)
             SELECT 'catalog_probe.old',domain,version,checksum,description FROM cowd_schema_migrations WHERE id=$1",
            &[&CLEAN_BASELINE.id],
        ).unwrap();
        let before = baseline_snapshot(&executor);
        assert!(executor
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .is_err());
        assert!(fixture
            .reconnect()
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .is_err());
        assert!(readiness.verify_registered_migration_catalogs().is_err());
        assert_eq!(baseline_snapshot(&executor), before);

        let other = PostgresMigrationSpec {
            id: "unrelated.1",
            domain: "unrelated",
            version: 1,
            description: "unrelated domain retains existing migration semantics",
            statements: &[
                "CREATE TABLE unrelated_data(value TEXT)",
                "INSERT INTO unrelated_data VALUES('other')",
            ],
        };
        assert_eq!(
            executor
                .apply_migrations(other.domain, &[other.clone()])
                .unwrap()[0]
                .status,
            "applied"
        );
        assert_eq!(
            executor
                .apply_migrations(other.domain, &[other.clone()])
                .unwrap()[0]
                .status,
            "already_applied"
        );
        let other_readiness = readiness_executor(&fixture);
        other_readiness
            .apply_migrations(other.domain, &[other])
            .unwrap();
        assert_eq!(
            other_readiness
                .verify_registered_migration_catalogs()
                .unwrap(),
            1
        );
        assert_eq!(
            executor
                .checkout_online_read()
                .unwrap()
                .query_one("SELECT value FROM unrelated_data", &[])
                .unwrap()
                .get::<_, String>(0),
            "other"
        );

        executor
            .checkout_critical()
            .unwrap()
            .execute(
                "DELETE FROM cowd_schema_migrations WHERE id='catalog_probe.old'",
                &[],
            )
            .unwrap();
        executor
            .checkout_critical()
            .unwrap()
            .execute(
                "UPDATE cowd_schema_migrations SET checksum='wrong' WHERE id=$1",
                &[&CLEAN_BASELINE.id],
            )
            .unwrap();
        let wrong_checksum = baseline_snapshot(&executor);
        assert!(executor
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .is_err());
        assert!(readiness.verify_registered_migration_catalogs().is_err());
        assert_eq!(baseline_snapshot(&executor), wrong_checksum);
        executor
            .checkout_critical()
            .unwrap()
            .execute(
                "UPDATE cowd_schema_migrations SET checksum=$1 WHERE id=$2",
                &[&CLEAN_BASELINE.checksum(), &CLEAN_BASELINE.id],
            )
            .unwrap();
        assert_eq!(readiness.verify_registered_migration_catalogs().unwrap(), 1);
        executor
            .checkout_critical()
            .unwrap()
            .execute(
                "DELETE FROM cowd_schema_migrations WHERE id=$1",
                &[&CLEAN_BASELINE.id],
            )
            .unwrap();
        assert!(readiness.verify_registered_migration_catalogs().is_err());

        let concurrent = crate::postgres_scope::PostgresTestScope::new();
        let peers = [concurrent.reconnect(), concurrent.reconnect()];
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let workers = peers
            .into_iter()
            .map(|peer| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    peer.apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
                        .unwrap()[0]
                        .status
                        .clone()
                })
            })
            .collect::<Vec<_>>();
        let statuses = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            statuses
                .iter()
                .filter(|status| status.as_str() == "applied")
                .count(),
            1
        );
        assert_eq!(
            statuses
                .iter()
                .filter(|status| status.as_str() == "already_applied")
                .count(),
            1
        );
        let mut connection = concurrent.reconnect().checkout_critical().unwrap();
        assert_eq!(
            connection
                .query_one("SELECT COUNT(*) FROM catalog_probe_data", &[])
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        assert_eq!(
            connection
                .query_one(
                    "SELECT COUNT(*) FROM cowd_schema_migrations WHERE domain=$1",
                    &[&CLEAN_BASELINE.domain]
                )
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        assert_eq!(
            connection
                .query_one(
                    "SELECT COUNT(*) FROM cowd_schema_catalogs WHERE domain=$1",
                    &[&CLEAN_BASELINE.domain]
                )
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        drop(connection);

        let failed = crate::postgres_scope::PostgresTestScope::new();
        let failed_executor = failed.reconnect();
        let bad = PostgresMigrationSpec {
            statements: &[
                "CREATE TABLE catalog_probe_data(value TEXT)",
                "INSERT INTO catalog_probe_data VALUES('rollback')",
                "SELECT 1/0",
            ],
            ..CLEAN_BASELINE.clone()
        };
        assert!(failed_executor
            .apply_migrations(bad.domain, &[bad])
            .is_err());
        assert!(!failed_executor.checkout_online_read().unwrap().query_one(
            "SELECT to_regclass('cowd_schema_migrations') IS NOT NULL OR to_regclass('cowd_schema_catalogs') IS NOT NULL OR to_regclass('catalog_probe_data') IS NOT NULL", &[],
        ).unwrap().get::<_, bool>(0));
        assert_eq!(
            failed_executor
                .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
                .unwrap()[0]
                .status,
            "applied"
        );

        let catalog_failure = crate::postgres_scope::PostgresTestScope::new();
        let catalog_executor = catalog_failure.reconnect();
        let prerequisite = PostgresMigrationSpec {
            id: "prerequisite.1",
            domain: "prerequisite",
            version: 1,
            description: "existing unrelated ledger fixture",
            statements: &[],
        };
        catalog_executor
            .apply_migrations(prerequisite.domain, &[prerequisite])
            .unwrap();
        catalog_executor.checkout_critical().unwrap().batch_execute(
            "CREATE FUNCTION reject_probe_catalog() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN IF NEW.domain='catalog_probe' THEN RAISE EXCEPTION 'catalog fixture rejection'; END IF;
             RETURN NEW; END $$;
             CREATE TRIGGER reject_probe_catalog BEFORE INSERT OR UPDATE ON cowd_schema_catalogs
             FOR EACH ROW EXECUTE FUNCTION reject_probe_catalog()",
        ).unwrap();
        let ledger_snapshot = |executor: &PostgresExecutor| -> String {
            executor.checkout_online_read().unwrap().query_one(
                "SELECT jsonb_build_array(
                    (SELECT jsonb_agg(to_jsonb(record) ORDER BY id) FROM cowd_schema_migrations record),
                    (SELECT jsonb_agg(to_jsonb(record) ORDER BY domain) FROM cowd_schema_catalogs record)
                )::TEXT", &[],
            ).unwrap().get(0)
        };
        let before_catalog_failure = ledger_snapshot(&catalog_executor);
        assert!(catalog_executor
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .is_err());
        assert_eq!(ledger_snapshot(&catalog_executor), before_catalog_failure);
        assert!(!catalog_executor
            .checkout_online_read()
            .unwrap()
            .query_one("SELECT to_regclass('catalog_probe_data') IS NOT NULL", &[],)
            .unwrap()
            .get::<_, bool>(0));

        let old = crate::postgres_scope::PostgresTestScope::new();
        let old_executor = old.reconnect();
        old_executor.checkout_critical().unwrap().batch_execute(
            "CREATE TABLE cowd_schema_migrations(id TEXT PRIMARY KEY, domain TEXT, checksum TEXT);
             INSERT INTO cowd_schema_migrations VALUES('catalog_probe.old','catalog_probe','old');
             CREATE TABLE old_sentinel(value TEXT); INSERT INTO old_sentinel VALUES('untouched')",
        ).unwrap();
        assert!(old_executor
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .is_err());
        let mut connection = old_executor.checkout_online_read().unwrap();
        assert!(!connection.query_one("SELECT to_regclass('cowd_schema_catalogs') IS NOT NULL OR to_regclass('catalog_probe_data') IS NOT NULL", &[]).unwrap().get::<_, bool>(0));
        assert_eq!(
            connection
                .query_one("SELECT checksum FROM cowd_schema_migrations", &[])
                .unwrap()
                .get::<_, String>(0),
            "old"
        );
        assert_eq!(
            connection
                .query_one("SELECT value FROM old_sentinel", &[])
                .unwrap()
                .get::<_, String>(0),
            "untouched"
        );
        drop(connection);

        let wrong_namespace = crate::postgres_scope::PostgresTestScope::new();
        let wrong_readiness = readiness_executor(&wrong_namespace);
        wrong_readiness
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .unwrap();
        assert!(wrong_readiness
            .verify_registered_migration_catalogs()
            .is_err());
        executor
            .checkout_critical()
            .unwrap()
            .batch_execute("DROP TABLE cowd_schema_catalogs")
            .unwrap();
        assert!(readiness.verify_registered_migration_catalogs().is_err());
        assert!(!executor
            .checkout_online_read()
            .unwrap()
            .query_one(
                "SELECT to_regclass('cowd_schema_catalogs') IS NOT NULL",
                &[],
            )
            .unwrap()
            .get::<_, bool>(0));
        let missing_ledger_readiness = readiness_executor(&concurrent);
        missing_ledger_readiness
            .apply_migrations(CLEAN_BASELINE.domain, &[CLEAN_BASELINE.clone()])
            .unwrap();
        assert_eq!(
            missing_ledger_readiness
                .verify_registered_migration_catalogs()
                .unwrap(),
            1
        );
        concurrent
            .reconnect()
            .checkout_critical()
            .unwrap()
            .batch_execute("DROP TABLE cowd_schema_migrations")
            .unwrap();
        assert!(missing_ledger_readiness
            .verify_registered_migration_catalogs()
            .is_err());
        assert!(!concurrent
            .reconnect()
            .checkout_online_read()
            .unwrap()
            .query_one(
                "SELECT to_regclass('cowd_schema_migrations') IS NOT NULL",
                &[],
            )
            .unwrap()
            .get::<_, bool>(0));
        assert_eq!(readiness.health().migration_transaction_count, 0);
    }

    #[test]
    fn an_explicit_pool_size_wins_over_the_derived_default() {
        assert_eq!(fixture_pool_size(Some(12)), 12);
        assert_eq!(fixture_pool_size(Some(1)), 1);
    }

    #[test]
    fn checked_fixture_budget_rejects_invalid_and_below_minimum_overrides() {
        for invalid in ["", "   ", "\t", "not-a-number", "0", "-4", "4294967296"] {
            for minimum in [1, 3] {
                assert!(checked_fixture_pool_budget(Some(invalid), minimum, Some(24)).is_err());
            }
        }
        for too_small in ["1", "2"] {
            assert!(checked_fixture_pool_budget(Some(too_small), 3, Some(24)).is_err());
        }
        assert!(checked_fixture_pool_budget(None, 3, Some(0)).is_err());
    }

    #[test]
    fn checked_fixture_budget_shares_explicit_values_and_preserves_fallbacks() {
        for explicit in ["3", "4", "24", " 12 "] {
            let runtime = checked_fixture_pool_budget(Some(explicit), 1, None).unwrap();
            let gateway = checked_fixture_pool_budget(Some(explicit), 3, Some(24)).unwrap();
            assert_eq!(runtime, gateway);
        }
        assert_eq!(checked_fixture_pool_budget(Some("1"), 1, None), Ok(1));
        assert_eq!(checked_fixture_pool_budget(None, 3, Some(24)), Ok(24));
        assert_eq!(
            checked_fixture_pool_budget(None, 1, None),
            Ok(fixture_pool_size(None))
        );
    }

    #[test]
    fn the_default_pool_gives_headroom_over_the_thread_count_without_doubling_it() {
        let threads = u32::try_from(
            std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get),
        )
        .unwrap_or(u32::MAX);
        let size = fixture_pool_size(None);
        assert!(
            size >= threads,
            "the pool must cover the default one-thread-per-core run: {size} < {threads}"
        );
        assert!(
            size < threads.saturating_mul(2),
            "the default must stay proportional, not double: {size} for {threads} threads"
        );
    }

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
        let fixture = crate::postgres_scope::PostgresTestScope::new();
        let scoped = fixture.bind(&executor);
        let schema: String = fixture
            .reconnect()
            .checkout_critical()
            .unwrap()
            .query_one("SELECT current_schema()", &[])
            .unwrap()
            .get(0);
        let probe = format!("probe_{}", schema.strip_prefix("cowdtest_").unwrap());
        scoped
            .checkout_critical()
            .expect("scoped checkout")
            .batch_execute(&format!("CREATE TABLE {probe}(value INTEGER NOT NULL)"))
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
            .query_opt("SELECT to_regclass($1)::text", &[&probe])
            .expect("public table probe query")
            .and_then(|row| row.get::<_, Option<String>>(0))
            .is_none());
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_failure_is_fail_closed_and_committed_work_survives_reconnect() {
        let fixture = crate::postgres_scope::PostgresTestScope::new();
        let executor = fixture.reconnect();
        {
            let mut connection = executor.checkout_critical().expect("fixture connection");
            connection
                .batch_execute("CREATE TABLE g65_marker(value TEXT NOT NULL)")
                .expect("marker table");
            connection
                .execute("INSERT INTO g65_marker(value) VALUES($1)", &[&"durable"])
                .expect("marker insert");
        }
        // Cold start against an unreachable PostgreSQL endpoint must fail closed with a
        // diagnosable error instead of opening any local fallback database.
        let bad_url = "postgres://postgres:wrong@127.0.0.1:1/cowd_release_closure".to_string();
        let resolver = StaticSecretRefResolver::new([("g65-bad".to_string(), bad_url)]);
        let bad = PostgresConnectionConfig::new("g65_bad", "g65-bad", "g65-bad");
        let error = PostgresExecutor::connect(bad, &resolver);
        assert!(
            error.is_err(),
            "unreachable PostgreSQL must not silently fall back to a local store"
        );
        // Reconnecting the same owned namespace must retain already committed work.
        let value: String = fixture
            .reconnect()
            .checkout_online_read()
            .expect("reopened connection")
            .query_one("SELECT value FROM g65_marker", &[])
            .expect("reopened query")
            .get(0);
        assert_eq!(value, "durable");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_transaction_hold_is_measured_and_not_held_across_external_await() {
        let pool_set = real_pool_set();
        let executor = pool_set.executor();
        let mut connection = executor.checkout_critical().expect("critical connection");
        let mut transaction = connection.transaction().expect("transaction");
        transaction
            .query_one("SELECT 1", &[])
            .expect("transaction query");
        std::thread::sleep(Duration::from_millis(20));
        transaction.commit().expect("commit");
        let committed_hold = pool_set.health().metrics.transaction_hold_ms;
        assert!(
            committed_hold >= 20,
            "transaction hold must include the in-transaction wait: {committed_hold}"
        );
        // An external await outside any transaction must not extend transaction hold.
        std::thread::sleep(Duration::from_millis(30));
        let after_hold = pool_set.health().metrics.transaction_hold_ms;
        assert_eq!(
            after_hold, committed_hold,
            "external await must not hold a database transaction"
        );
        drop(connection);
    }
}
