//! PostgreSQL execution primitives for Cowd durable domains.
//!
//! This module intentionally contains no domain schema or business DTO.  It
//! owns the connection pool, secret-reference resolution, migration ledger,
//! checksum validation, and redacted diagnostics.  Domain crates own their
//! own SQL contracts and pass typed migration specifications here.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use postgres::{Client, Config, NoTls};
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresExecutorMetrics {
    pub checkout_count: u64,
    pub checkout_timeout_count: u64,
    pub checkout_wait_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresExecutorHealth {
    pub logical_identity: String,
    pub application_name: String,
    pub max_connections: u32,
    pub metrics: PostgresExecutorMetrics,
}

#[derive(Debug, Default)]
struct PostgresExecutorCounters {
    checkout_count: AtomicU64,
    checkout_timeout_count: AtomicU64,
    checkout_wait_ms: AtomicU64,
}

struct PostgresExecutorInner {
    config: PostgresConnectionConfig,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    counters: PostgresExecutorCounters,
}

/// A bounded synchronous PostgreSQL executor.  Pool checkout is the only
/// synchronization point; query execution happens after a connection is
/// checked out, so independent requests can use distinct server connections.
#[derive(Clone)]
pub struct PostgresExecutor {
    inner: Arc<PostgresExecutorInner>,
}

impl fmt::Debug for PostgresExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresExecutor")
            .field("logical_identity", &self.inner.config.logical_identity)
            .field("application_name", &self.inner.config.application_name)
            .finish_non_exhaustive()
    }
}

impl PostgresExecutor {
    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, StorageError> {
        if config.logical_identity.trim().is_empty()
            || config.secret_ref.trim().is_empty()
            || config.application_name.trim().is_empty()
            || config.max_connections == 0
        {
            return Err(StorageError::Other(
                "postgres executor requires identity, secret reference, application name, and a non-zero pool size".to_string(),
            ));
        }
        let resolved = resolver.resolve_postgres_url(&config.secret_ref)?;
        let mut client_config = Config::from_str(resolved.as_str()).map_err(|_| {
            StorageError::Other(format!(
                "postgres connection config for `{}` is invalid",
                config.logical_identity
            ))
        })?;
        client_config.application_name(&config.application_name);
        let manager = PostgresConnectionManager::new(client_config, NoTls);
        let mut builder = Pool::builder()
            .max_size(config.max_connections)
            .connection_timeout(Duration::from_millis(config.checkout_timeout_ms));
        if let Some(min_idle_connections) = config.min_idle_connections {
            builder = builder.min_idle(Some(min_idle_connections));
        }
        let pool = builder.build(manager).map_err(|error| {
            StorageError::Other(format!(
                "postgres pool for `{}` could not be created: {error}",
                config.logical_identity
            ))
        })?;
        Ok(Self {
            inner: Arc::new(PostgresExecutorInner {
                config,
                pool,
                counters: PostgresExecutorCounters::default(),
            }),
        })
    }

    pub fn checkout(
        &self,
    ) -> Result<PooledConnection<PostgresConnectionManager<NoTls>>, StorageError> {
        let started = Instant::now();
        let result = self.inner.pool.get();
        self.inner
            .counters
            .checkout_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner.counters.checkout_wait_ms.fetch_add(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        result.map_err(|error| {
            self.inner
                .counters
                .checkout_timeout_count
                .fetch_add(1, Ordering::Relaxed);
            StorageError::Other(format!(
                "postgres pool checkout failed for `{}`: {error}",
                self.inner.config.logical_identity
            ))
        })
    }

    #[must_use]
    pub fn health(&self) -> PostgresExecutorHealth {
        PostgresExecutorHealth {
            logical_identity: self.inner.config.logical_identity.clone(),
            application_name: self.inner.config.application_name.clone(),
            max_connections: self.inner.config.max_connections,
            metrics: PostgresExecutorMetrics {
                checkout_count: self.inner.counters.checkout_count.load(Ordering::Relaxed),
                checkout_timeout_count: self
                    .inner
                    .counters
                    .checkout_timeout_count
                    .load(Ordering::Relaxed),
                checkout_wait_ms: self.inner.counters.checkout_wait_ms.load(Ordering::Relaxed),
            },
        }
    }

    #[must_use]
    pub fn logical_identity(&self) -> &str {
        &self.inner.config.logical_identity
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
        let mut connection = self.checkout()?;
        ensure_migration_ledger(&mut connection)?;
        let mut reports = Vec::with_capacity(specs.len());
        for spec in specs {
            if spec.domain != domain {
                return Err(StorageError::Other(format!(
                    "postgres migration `{}` targets `{}` but requested domain is `{domain}`",
                    spec.id, spec.domain
                )));
            }
            reports.push(apply_migration(&mut connection, spec)?);
        }
        Ok(reports)
    }
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

fn ensure_migration_ledger(connection: &mut Client) -> Result<(), StorageError> {
    connection.batch_execute(
        "CREATE TABLE IF NOT EXISTS cowd_schema_migrations (
            id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            version BIGINT NOT NULL,
            checksum TEXT NOT NULL,
            description TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );",
    )?;
    Ok(())
}

fn apply_migration(
    connection: &mut Client,
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
    }
}
