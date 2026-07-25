//! Durable approval decision history.
//!
//! User-managed always-allow rules are a file artifact.  Decision history is
//! not: it must survive restart and use a single selected durable owner.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use storage::{
    PostgresConnection, PostgresExecutor, PostgresMigrationSpec, PostgresTransaction,
    SqliteExecutor, StorageBackendKind, StorageEndpoint,
};
use thiserror::Error;

use crate::ApprovalHistoryEntry;

const APPROVAL_HISTORY_DOMAIN: &str = "approval_history";
const POSTGRES_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "approval-history.0001.ledger",
    domain: APPROVAL_HISTORY_DOMAIN,
    version: 1,
    description: "create durable approval decision ledger",
    statements: &[r#"
        CREATE TABLE IF NOT EXISTS approval_history_entry (
            id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL,
            resolved_at TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_approval_history_request_id
            ON approval_history_entry(request_id);
        CREATE INDEX IF NOT EXISTS idx_approval_history_resolved_at
            ON approval_history_entry(resolved_at DESC, id ASC);
    "#],
}];

#[derive(Debug, Error)]
pub enum ApprovalHistoryError {
    #[error("approval history storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("approval history sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("approval history postgres error: {0}")]
    Postgres(#[from] postgres::Error),
    #[error("approval history json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("approval history conflict for `{0}`")]
    Conflict(String),
    #[error("approval history backend error: {0}")]
    Backend(String),
    #[error("approval history migration source changed while quiesced")]
    SourceChanged,
    #[error("approval history target is not empty and does not match source")]
    TargetNotEmpty,
}

pub type ApprovalHistoryResult<T> = Result<T, ApprovalHistoryError>;

/// Complete durable history contract.  `append` is idempotent by decision id
/// and request id; a different payload for either key is rejected.
pub trait ApprovalHistoryLedger: Send + Sync {
    fn list(
        &self,
        limit: usize,
        offset: usize,
    ) -> ApprovalHistoryResult<(Vec<ApprovalHistoryEntry>, usize)>;
    fn get(&self, id: &str) -> ApprovalHistoryResult<Option<ApprovalHistoryEntry>>;
    fn append(&self, entry: ApprovalHistoryEntry) -> ApprovalHistoryResult<()>;
}

/// Backend-neutral, stable approval decision set used only by the maintenance
/// cutover path. Normal requests use `ApprovalHistoryLedger` operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApprovalHistoryMigrationSnapshot {
    pub schema_version: u32,
    pub entries: Vec<ApprovalHistoryEntry>,
}

impl ApprovalHistoryMigrationSnapshot {
    pub fn new(entries: Vec<ApprovalHistoryEntry>) -> ApprovalHistoryResult<Self> {
        let mut snapshot = Self {
            schema_version: 1,
            entries,
        };
        snapshot.entries.sort_by(|left, right| {
            left.resolved_at
                .cmp(&right.resolved_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> ApprovalHistoryResult<()> {
        if self.schema_version != 1 {
            return Err(ApprovalHistoryError::Backend(format!(
                "unsupported approval history snapshot schema {}",
                self.schema_version
            )));
        }
        let mut ids = BTreeSet::new();
        let mut request_ids = BTreeSet::new();
        for entry in &self.entries {
            if entry.id.trim().is_empty() || entry.request_id.trim().is_empty() {
                return Err(ApprovalHistoryError::Backend(
                    "approval history snapshot has empty identity".to_string(),
                ));
            }
            if !ids.insert(entry.id.as_str()) || !request_ids.insert(entry.request_id.as_str()) {
                return Err(ApprovalHistoryError::Conflict(entry.id.clone()));
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> ApprovalHistoryResult<String> {
        self.validate()?;
        Ok(format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(self)?)
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalHistoryMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub schema_version: u32,
    pub record_count: usize,
}

#[derive(Clone, Debug)]
pub struct SqliteApprovalHistoryLedger {
    executor: SqliteExecutor,
}

impl SqliteApprovalHistoryLedger {
    pub fn open(endpoint: &StorageEndpoint) -> ApprovalHistoryResult<Self> {
        if endpoint.backend != StorageBackendKind::Sqlite {
            return Err(ApprovalHistoryError::Backend(format!(
                "approval history endpoint `{}` is not SQLite",
                endpoint.logical_id()
            )));
        }
        let executor = SqliteExecutor::for_endpoint(endpoint)?;
        let connection = executor.checkout()?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS approval_history_entry (
                id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                resolved_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_approval_history_resolved_at
                ON approval_history_entry(resolved_at DESC, id ASC);",
        )?;
        Ok(Self { executor })
    }

    pub fn in_memory() -> ApprovalHistoryResult<Self> {
        let executor = SqliteExecutor::in_memory("approval-history-test")?;
        let connection = executor.checkout()?;
        connection.execute_batch(
            "CREATE TABLE approval_history_entry (
                id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                resolved_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX idx_approval_history_resolved_at
                ON approval_history_entry(resolved_at DESC, id ASC);",
        )?;
        Ok(Self { executor })
    }

    pub fn export_migration_snapshot(
        &self,
    ) -> ApprovalHistoryResult<ApprovalHistoryMigrationSnapshot> {
        let connection = self.executor.checkout()?;
        let mut statement = connection.prepare(
            "SELECT payload FROM approval_history_entry ORDER BY resolved_at ASC, id ASC",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let entries = rows
            .map(|row| serde_json::from_str(&row?).map_err(ApprovalHistoryError::from))
            .collect::<ApprovalHistoryResult<Vec<_>>>()?;
        ApprovalHistoryMigrationSnapshot::new(entries)
    }

    /// Read the former JSON history as a one-time migration source. The
    /// normal production path never calls this method.
    pub fn import_legacy_json(&self, path: impl AsRef<Path>) -> ApprovalHistoryResult<()> {
        let entries = match fs::read_to_string(path) {
            Ok(payload) => serde_json::from_str::<Vec<ApprovalHistoryEntry>>(&payload)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(ApprovalHistoryError::Backend(error.to_string())),
        };
        self.import_migration_snapshot(&ApprovalHistoryMigrationSnapshot::new(entries)?)
    }

    pub fn import_migration_snapshot(
        &self,
        snapshot: &ApprovalHistoryMigrationSnapshot,
    ) -> ApprovalHistoryResult<()> {
        snapshot.validate()?;
        let mut connection = self.executor.checkout()?;
        let transaction = connection.transaction()?;
        let count =
            transaction.query_row("SELECT COUNT(*) FROM approval_history_entry", [], |row| {
                row.get::<_, i64>(0)
            })? as usize;
        if count > 0 {
            let existing = export_sqlite_snapshot(&transaction)?;
            if existing.canonical_digest()? == snapshot.canonical_digest()? {
                transaction.commit()?;
                return Ok(());
            }
            return Err(ApprovalHistoryError::TargetNotEmpty);
        }
        for entry in &snapshot.entries {
            let payload = serde_json::to_string(entry)?;
            transaction.execute(
                "INSERT INTO approval_history_entry(id, request_id, resolved_at, payload)
                 VALUES (?1, ?2, ?3, ?4)",
                params![entry.id, entry.request_id, entry.resolved_at, payload],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

impl ApprovalHistoryLedger for SqliteApprovalHistoryLedger {
    fn list(
        &self,
        limit: usize,
        offset: usize,
    ) -> ApprovalHistoryResult<(Vec<ApprovalHistoryEntry>, usize)> {
        let connection = self.executor.checkout()?;
        let total =
            connection.query_row("SELECT COUNT(*) FROM approval_history_entry", [], |row| {
                row.get::<_, i64>(0)
            })? as usize;
        let mut statement = connection.prepare(
            "SELECT payload FROM approval_history_entry
             ORDER BY resolved_at DESC, id ASC LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(
            params![
                limit.clamp(1, 500) as i64,
                offset.min(i64::MAX as usize) as i64
            ],
            |row| row.get::<_, String>(0),
        )?;
        let entries = rows
            .map(|row| serde_json::from_str(&row?).map_err(ApprovalHistoryError::from))
            .collect::<ApprovalHistoryResult<Vec<_>>>()?;
        Ok((entries, total))
    }

    fn get(&self, id: &str) -> ApprovalHistoryResult<Option<ApprovalHistoryEntry>> {
        let connection = self.executor.checkout()?;
        connection
            .query_row(
                "SELECT payload FROM approval_history_entry WHERE id = ?1 OR request_id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|payload| serde_json::from_str(&payload).map_err(ApprovalHistoryError::from))
            .transpose()
    }

    fn append(&self, entry: ApprovalHistoryEntry) -> ApprovalHistoryResult<()> {
        let connection = self.executor.checkout()?;
        let payload = serde_json::to_string(&entry)?;
        let existing = connection
            .query_row(
                "SELECT payload FROM approval_history_entry WHERE id = ?1 OR request_id = ?2",
                params![entry.id, entry.request_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        match existing {
            Some(existing) if existing == payload => Ok(()),
            Some(_) => Err(ApprovalHistoryError::Conflict(entry.request_id)),
            None => {
                connection.execute(
                    "INSERT INTO approval_history_entry(id, request_id, resolved_at, payload)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![entry.id, entry.request_id, entry.resolved_at, payload],
                )?;
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PostgresApprovalHistoryLedger {
    executor: PostgresExecutor,
}

impl PostgresApprovalHistoryLedger {
    pub fn new(executor: PostgresExecutor) -> ApprovalHistoryResult<Self> {
        executor.apply_migrations(APPROVAL_HISTORY_DOMAIN, POSTGRES_MIGRATIONS)?;
        Ok(Self { executor })
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut PostgresConnection) -> ApprovalHistoryResult<T>,
    ) -> ApprovalHistoryResult<T> {
        let mut connection = self.executor.checkout_runtime()?;
        operation(&mut connection)
    }

    pub fn export_migration_snapshot(
        &self,
    ) -> ApprovalHistoryResult<ApprovalHistoryMigrationSnapshot> {
        self.with_connection(|connection| {
            let rows = connection.query(
                "SELECT payload FROM approval_history_entry ORDER BY resolved_at ASC, id ASC",
                &[],
            )?;
            let entries = rows
                .into_iter()
                .map(|row| {
                    serde_json::from_value(row.get::<_, Value>(0))
                        .map_err(ApprovalHistoryError::from)
                })
                .collect::<ApprovalHistoryResult<Vec<_>>>()?;
            ApprovalHistoryMigrationSnapshot::new(entries)
        })
    }

    pub fn import_migration_snapshot(
        &self,
        snapshot: &ApprovalHistoryMigrationSnapshot,
    ) -> ApprovalHistoryResult<()> {
        snapshot.validate()?;
        self.with_connection(|connection| {
            let mut transaction = connection.transaction()?;
            let count = transaction
                .query_one("SELECT COUNT(*) FROM approval_history_entry", &[])?
                .get::<_, i64>(0) as usize;
            if count > 0 {
                let existing = export_postgres_snapshot(&mut transaction)?;
                if existing.canonical_digest()? == snapshot.canonical_digest()? {
                    transaction.commit()?;
                    return Ok(());
                }
                return Err(ApprovalHistoryError::TargetNotEmpty);
            }
            for entry in &snapshot.entries {
                let payload = serde_json::to_value(entry)?;
                transaction.execute(
                    "INSERT INTO approval_history_entry(id, request_id, resolved_at, payload)
                     VALUES ($1, $2, $3, $4)",
                    &[&entry.id, &entry.request_id, &entry.resolved_at, &payload],
                )?;
            }
            transaction.commit()?;
            Ok(())
        })
    }
}

impl ApprovalHistoryLedger for PostgresApprovalHistoryLedger {
    fn list(
        &self,
        limit: usize,
        offset: usize,
    ) -> ApprovalHistoryResult<(Vec<ApprovalHistoryEntry>, usize)> {
        self.with_connection(|connection| {
            let total = connection
                .query_one("SELECT COUNT(*) FROM approval_history_entry", &[])?
                .get::<_, i64>(0) as usize;
            let rows = connection.query(
                "SELECT payload FROM approval_history_entry
                 ORDER BY resolved_at DESC, id ASC LIMIT $1 OFFSET $2",
                &[
                    &(limit.clamp(1, 500) as i64),
                    &(offset.min(i64::MAX as usize) as i64),
                ],
            )?;
            let entries = rows
                .into_iter()
                .map(|row| {
                    serde_json::from_value(row.get::<_, Value>(0))
                        .map_err(ApprovalHistoryError::from)
                })
                .collect::<ApprovalHistoryResult<Vec<_>>>()?;
            Ok((entries, total))
        })
    }

    fn get(&self, id: &str) -> ApprovalHistoryResult<Option<ApprovalHistoryEntry>> {
        self.with_connection(|connection| {
            connection
                .query_opt(
                    "SELECT payload FROM approval_history_entry WHERE id = $1 OR request_id = $1",
                    &[&id],
                )?
                .map(|row| {
                    serde_json::from_value(row.get::<_, Value>(0))
                        .map_err(ApprovalHistoryError::from)
                })
                .transpose()
        })
    }

    fn append(&self, entry: ApprovalHistoryEntry) -> ApprovalHistoryResult<()> {
        self.with_connection(|connection| {
            let payload = serde_json::to_value(&entry)?;
            let mut transaction = connection.transaction()?;
            let existing = transaction.query_opt(
                "SELECT payload FROM approval_history_entry WHERE id = $1 OR request_id = $2 FOR UPDATE",
                &[&entry.id, &entry.request_id],
            )?;
            match existing {
                Some(row) if row.get::<_, Value>(0) == payload => {}
                Some(_) => return Err(ApprovalHistoryError::Conflict(entry.request_id)),
                None => {
                    transaction.execute(
                        "INSERT INTO approval_history_entry(id, request_id, resolved_at, payload)
                         VALUES ($1, $2, $3, $4)",
                        &[&entry.id, &entry.request_id, &entry.resolved_at, &payload],
                    )?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
    }
}

pub type SharedApprovalHistoryLedger = Arc<dyn ApprovalHistoryLedger>;

/// Copy one quiesced SQLite ledger into PostgreSQL and prove both sides have
/// identical canonical decision history. No normal request path performs a
/// dual write.
pub fn copy_quiesced_approval_history(
    source: &SqliteApprovalHistoryLedger,
    target: &PostgresApprovalHistoryLedger,
    manifest_path: impl AsRef<Path>,
) -> ApprovalHistoryResult<ApprovalHistoryMigrationManifest> {
    let snapshot = source.export_migration_snapshot()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    if source.export_migration_snapshot()?.canonical_digest()? != source_digest {
        return Err(ApprovalHistoryError::SourceChanged);
    }
    let target_snapshot = target.export_migration_snapshot()?;
    let target_digest = target_snapshot.canonical_digest()?;
    if target_digest != source_digest {
        return Err(ApprovalHistoryError::Backend(
            "approval history target digest differs from source".to_string(),
        ));
    }
    let manifest = ApprovalHistoryMigrationManifest {
        domain: "approval_history".to_string(),
        source_digest,
        target_digest,
        schema_version: snapshot.schema_version,
        record_count: snapshot.entries.len(),
    };
    write_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn export_sqlite_snapshot(
    transaction: &rusqlite::Transaction<'_>,
) -> ApprovalHistoryResult<ApprovalHistoryMigrationSnapshot> {
    let mut statement = transaction
        .prepare("SELECT payload FROM approval_history_entry ORDER BY resolved_at ASC, id ASC")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let entries = rows
        .map(|row| serde_json::from_str(&row?).map_err(ApprovalHistoryError::from))
        .collect::<ApprovalHistoryResult<Vec<_>>>()?;
    ApprovalHistoryMigrationSnapshot::new(entries)
}

fn export_postgres_snapshot(
    transaction: &mut PostgresTransaction<'_>,
) -> ApprovalHistoryResult<ApprovalHistoryMigrationSnapshot> {
    let rows = transaction.query(
        "SELECT payload FROM approval_history_entry ORDER BY resolved_at ASC, id ASC",
        &[],
    )?;
    let entries = rows
        .into_iter()
        .map(|row| {
            serde_json::from_value(row.get::<_, Value>(0)).map_err(ApprovalHistoryError::from)
        })
        .collect::<ApprovalHistoryResult<Vec<_>>>()?;
    ApprovalHistoryMigrationSnapshot::new(entries)
}

fn write_manifest(
    path: &Path,
    manifest: &ApprovalHistoryMigrationManifest,
) -> ApprovalHistoryResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| ApprovalHistoryError::Backend(error.to_string()))?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&temporary, serde_json::to_vec_pretty(manifest)?)
        .map_err(|error| ApprovalHistoryError::Backend(error.to_string()))?;
    fs::rename(temporary, path)
        .map_err(|error| ApprovalHistoryError::Backend(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;

    use crate::{ApprovalHistoryEntry, ApprovalHistoryOutcome};
    use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

    use super::{
        copy_quiesced_approval_history, ApprovalHistoryLedger, PostgresApprovalHistoryLedger,
        SqliteApprovalHistoryLedger,
    };

    fn entry(id: &str) -> ApprovalHistoryEntry {
        ApprovalHistoryEntry {
            id: id.to_string(),
            request_id: format!("request-{id}"),
            command: "rm -rf target".to_string(),
            normalized_command: "rm -rf target".to_string(),
            risk_level: "critical".to_string(),
            matched_patterns: vec!["delete".to_string()],
            outcome: ApprovalHistoryOutcome::Denied {
                reason: "test".to_string(),
            },
            resolved_at: "2026-07-23T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn sqlite_ledger_is_idempotent_and_queryable() {
        let ledger = SqliteApprovalHistoryLedger::in_memory().unwrap();
        let value = entry("decision-1");
        ledger.append(value.clone()).unwrap();
        ledger.append(value).unwrap();
        assert_eq!(ledger.list(10, 0).unwrap().1, 1);
        assert!(ledger.get("request-decision-1").unwrap().is_some());
    }

    #[test]
    fn sqlite_legacy_json_is_a_one_time_import_source() {
        let root = tempfile::tempdir().unwrap();
        let legacy_path = root.path().join("approval_history.json");
        std::fs::write(
            &legacy_path,
            serde_json::to_vec(&vec![entry("legacy-decision")]).unwrap(),
        )
        .unwrap();
        let ledger = SqliteApprovalHistoryLedger::in_memory().unwrap();
        ledger.import_legacy_json(&legacy_path).unwrap();
        assert_eq!(ledger.list(10, 0).unwrap().1, 1);
        ledger.append(entry("new-decision")).unwrap();
        // A subsequent start never overwrites a live ledger from a mutable
        // legacy artifact.
        ledger.import_legacy_json(&legacy_path).unwrap_err();
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_copy_reopens_with_matching_digest() {
        let url = env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let source = SqliteApprovalHistoryLedger::in_memory().unwrap();
        source.append(entry("pg-decision-1")).unwrap();
        source.append(entry("pg-decision-2")).unwrap();
        let resolver = StaticSecretRefResolver::new([("approval.pg.test".to_string(), url)]);
        let target = PostgresApprovalHistoryLedger::new(
            storage::PostgresExecutor::connect(
                PostgresConnectionConfig::new(
                    "approval-history-postgres-test",
                    "approval.pg.test",
                    "cowd-approval-postgres-contract",
                ),
                &resolver,
            )
            .unwrap(),
        )
        .unwrap();
        target
            .executor()
            .checkout_runtime()
            .unwrap()
            .batch_execute("TRUNCATE TABLE approval_history_entry")
            .unwrap();
        let manifest_root = tempfile::tempdir().unwrap();
        let manifest = copy_quiesced_approval_history(
            &source,
            &target,
            manifest_root.path().join("approval-history.json"),
        )
        .unwrap();
        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert_eq!(manifest.record_count, 2);
        assert!(target.get("request-pg-decision-1").unwrap().is_some());
        let reopened = PostgresApprovalHistoryLedger::new(target.executor().clone()).unwrap();
        assert_eq!(reopened.list(10, 0).unwrap().1, 2);
        target
            .executor()
            .checkout_runtime()
            .unwrap()
            .batch_execute("TRUNCATE TABLE approval_history_entry")
            .unwrap();
    }
}
