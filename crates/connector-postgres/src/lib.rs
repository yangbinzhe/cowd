#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! PostgreSQL adapter for ConnectorDirectory.
//!
//! The crate depends on the connector port and the storage PostgreSQL feature;
//! the connector crate itself remains free of PostgreSQL in its normal
//! dependency graph.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use connector::{
    DurableResourceDirectoryRecord, ExternalResourceRef, ResourceDirectoryError,
    ResourceDirectoryFactory, ResourceDirectoryRepository, ResourceDirectoryResult,
    ResourceDirectorySourceBinding,
};
use postgres::Row;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{
    PostgresConnectionConfig, PostgresExecutor, PostgresMigrationSpec, SecretRefResolver,
    StorageBackendKind, StorageHandle,
};

const CONNECTOR_DIRECTORY_DOMAIN: &str = "connector_directory";
const CONNECTOR_DIRECTORY_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "connector_directory.0001.initial",
    domain: CONNECTOR_DIRECTORY_DOMAIN,
    version: 1,
    description: "create durable connector resource directory",
    statements: &[
        "CREATE TABLE IF NOT EXISTS connector_resources (
            reference TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            account_id TEXT,
            resource_type TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT,
            permissions_summary TEXT,
            digest TEXT,
            indexed_state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_connector_resources_provider
            ON connector_resources(provider)",
        "CREATE INDEX IF NOT EXISTS idx_connector_resources_last_seen
            ON connector_resources(last_seen_at DESC, reference ASC)",
        "CREATE TABLE IF NOT EXISTS connector_resource_sources (
            reference TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            attached_at TEXT NOT NULL,
            PRIMARY KEY(reference, source_kind, source_id)
        )",
    ],
}];

#[derive(Clone, Debug)]
pub struct PostgresResourceDirectory {
    executor: PostgresExecutor,
}

impl PostgresResourceDirectory {
    pub fn new(executor: PostgresExecutor) -> ResourceDirectoryResult<Self> {
        executor
            .apply_migrations(CONNECTOR_DIRECTORY_DOMAIN, CONNECTOR_DIRECTORY_MIGRATIONS)
            .map_err(ResourceDirectoryError::backend)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> ResourceDirectoryResult<Self> {
        PostgresExecutor::connect(config, resolver)
            .map_err(ResourceDirectoryError::backend)
            .and_then(Self::new)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }
}

impl ResourceDirectoryRepository for PostgresResourceDirectory {
    fn upsert(
        &self,
        resource: &ExternalResourceRef,
    ) -> ResourceDirectoryResult<ExternalResourceRef> {
        let now = Utc::now().to_rfc3339();
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .execute(
                "INSERT INTO connector_resources (
                    reference, provider, account_id, resource_type, title, source,
                    permissions_summary, digest, indexed_state, created_at, updated_at, last_seen_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $10)
                ON CONFLICT(reference) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    account_id = EXCLUDED.account_id,
                    resource_type = EXCLUDED.resource_type,
                    title = EXCLUDED.title,
                    source = EXCLUDED.source,
                    permissions_summary = EXCLUDED.permissions_summary,
                    digest = EXCLUDED.digest,
                    indexed_state = EXCLUDED.indexed_state,
                    updated_at = EXCLUDED.updated_at,
                    last_seen_at = EXCLUDED.last_seen_at",
                &[
                    &resource.reference,
                    &resource.provider,
                    &resource.account_id,
                    &resource.resource_type,
                    &resource.title,
                    &resource.source,
                    &resource.permissions_summary,
                    &resource.digest,
                    &resource.indexed_state,
                    &now,
                ],
            )
            .map_err(ResourceDirectoryError::backend)?;
        Ok(resource.clone())
    }

    fn get(&self, reference: &str) -> ResourceDirectoryResult<Option<ExternalResourceRef>> {
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .query_opt(
                "SELECT reference, provider, account_id, resource_type, title, source,
                    permissions_summary, digest, indexed_state
                   FROM connector_resources
                  WHERE reference = $1",
                &[&reference],
            )
            .map_err(ResourceDirectoryError::backend)?
            .map(|row| row_to_resource_ref(&row))
            .transpose()
    }

    fn list_recent(&self, limit: usize) -> ResourceDirectoryResult<Vec<ExternalResourceRef>> {
        self.list_page(limit, 0)
    }

    fn list_page(
        &self,
        limit: usize,
        offset: usize,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>> {
        let limit = i64::try_from(limit).map_err(ResourceDirectoryError::backend)?;
        let offset = i64::try_from(offset).map_err(ResourceDirectoryError::backend)?;
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .query(
                "SELECT reference, provider, account_id, resource_type, title, source,
                    permissions_summary, digest, indexed_state
                   FROM connector_resources
                  ORDER BY last_seen_at DESC, reference ASC
                  LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            )
            .map_err(ResourceDirectoryError::backend)?
            .iter()
            .map(row_to_resource_ref)
            .collect()
    }

    fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>> {
        let query = query.trim();
        if query.is_empty() {
            return self.list_recent(limit);
        }
        let pattern = format!(
            "%{}%",
            query
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        );
        let limit = i64::try_from(limit).map_err(ResourceDirectoryError::backend)?;
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .query(
                "SELECT reference, provider, account_id, resource_type, title, source,
                    permissions_summary, digest, indexed_state
                   FROM connector_resources
                  WHERE reference ILIKE $1 ESCAPE '\\'
                     OR title ILIKE $1 ESCAPE '\\'
                     OR resource_type ILIKE $1 ESCAPE '\\'
                     OR provider ILIKE $1 ESCAPE '\\'
                  ORDER BY last_seen_at DESC, reference ASC
                  LIMIT $2",
                &[&pattern, &limit],
            )
            .map_err(ResourceDirectoryError::backend)?
            .iter()
            .map(row_to_resource_ref)
            .collect()
    }

    fn mark_indexed(&self, reference: &str) -> ResourceDirectoryResult<bool> {
        self.update_indexed_state(reference, "indexed")
    }

    fn mark_stale(&self, reference: &str) -> ResourceDirectoryResult<bool> {
        self.update_indexed_state(reference, "stale")
    }

    fn attach_source(
        &self,
        reference: &str,
        source_kind: &str,
        source_id: &str,
    ) -> ResourceDirectoryResult<()> {
        let now = Utc::now().to_rfc3339();
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .execute(
                "INSERT INTO connector_resource_sources
                    (reference, source_kind, source_id, attached_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(reference, source_kind, source_id) DO UPDATE
                    SET attached_at = EXCLUDED.attached_at",
                &[&reference, &source_kind, &source_id, &now],
            )
            .map_err(ResourceDirectoryError::backend)?;
        Ok(())
    }

    fn list_sources(
        &self,
        reference: &str,
    ) -> ResourceDirectoryResult<Vec<ResourceDirectorySourceBinding>> {
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .query(
                "SELECT reference, source_kind, source_id, attached_at
                   FROM connector_resource_sources
                  WHERE reference = $1
                  ORDER BY source_kind ASC, source_id ASC",
                &[&reference],
            )
            .map_err(ResourceDirectoryError::backend)?
            .iter()
            .map(|row| {
                Ok(ResourceDirectorySourceBinding {
                    reference: row.try_get(0).map_err(ResourceDirectoryError::backend)?,
                    source_kind: row.try_get(1).map_err(ResourceDirectoryError::backend)?,
                    source_id: row.try_get(2).map_err(ResourceDirectoryError::backend)?,
                    attached_at: row.try_get(3).map_err(ResourceDirectoryError::backend)?,
                })
            })
            .collect()
    }

    fn export_records(&self) -> ResourceDirectoryResult<Vec<DurableResourceDirectoryRecord>> {
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .query(
                "SELECT reference, provider, account_id, resource_type, title, source,
                    permissions_summary, digest, indexed_state, created_at, updated_at, last_seen_at
                   FROM connector_resources
                  ORDER BY reference ASC",
                &[],
            )
            .map_err(ResourceDirectoryError::backend)?
            .iter()
            .map(row_to_durable_resource_directory_record)
            .collect()
    }

    fn import_record(
        &self,
        record: &DurableResourceDirectoryRecord,
    ) -> ResourceDirectoryResult<()> {
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .execute(
                "INSERT INTO connector_resources (
                    reference, provider, account_id, resource_type, title, source,
                    permissions_summary, digest, indexed_state, created_at, updated_at, last_seen_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT(reference) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    account_id = EXCLUDED.account_id,
                    resource_type = EXCLUDED.resource_type,
                    title = EXCLUDED.title,
                    source = EXCLUDED.source,
                    permissions_summary = EXCLUDED.permissions_summary,
                    digest = EXCLUDED.digest,
                    indexed_state = EXCLUDED.indexed_state,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    last_seen_at = EXCLUDED.last_seen_at",
                &[
                    &record.resource.reference,
                    &record.resource.provider,
                    &record.resource.account_id,
                    &record.resource.resource_type,
                    &record.resource.title,
                    &record.resource.source,
                    &record.resource.permissions_summary,
                    &record.resource.digest,
                    &record.resource.indexed_state,
                    &record.created_at,
                    &record.updated_at,
                    &record.last_seen_at,
                ],
            )
            .map_err(ResourceDirectoryError::backend)?;
        Ok(())
    }

    fn import_source_binding(
        &self,
        binding: &ResourceDirectorySourceBinding,
    ) -> ResourceDirectoryResult<()> {
        self.executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .execute(
                "INSERT INTO connector_resource_sources
                    (reference, source_kind, source_id, attached_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(reference, source_kind, source_id) DO UPDATE
                    SET attached_at = EXCLUDED.attached_at",
                &[
                    &binding.reference,
                    &binding.source_kind,
                    &binding.source_id,
                    &binding.attached_at,
                ],
            )
            .map_err(ResourceDirectoryError::backend)?;
        Ok(())
    }
}

impl PostgresResourceDirectory {
    fn update_indexed_state(
        &self,
        reference: &str,
        indexed_state: &str,
    ) -> ResourceDirectoryResult<bool> {
        let now = Utc::now().to_rfc3339();
        let changed = self
            .executor
            .checkout_runtime()
            .map_err(ResourceDirectoryError::backend)?
            .execute(
                "UPDATE connector_resources
                    SET indexed_state = $1, updated_at = $2
                  WHERE reference = $3",
                &[&indexed_state, &now, &reference],
            )
            .map_err(ResourceDirectoryError::backend)?;
        Ok(changed > 0)
    }
}

fn row_to_resource_ref(row: &Row) -> ResourceDirectoryResult<ExternalResourceRef> {
    Ok(ExternalResourceRef {
        reference: row.try_get(0).map_err(ResourceDirectoryError::backend)?,
        provider: row.try_get(1).map_err(ResourceDirectoryError::backend)?,
        account_id: row.try_get(2).map_err(ResourceDirectoryError::backend)?,
        resource_type: row.try_get(3).map_err(ResourceDirectoryError::backend)?,
        title: row.try_get(4).map_err(ResourceDirectoryError::backend)?,
        source: row.try_get(5).map_err(ResourceDirectoryError::backend)?,
        permissions_summary: row.try_get(6).map_err(ResourceDirectoryError::backend)?,
        digest: row.try_get(7).map_err(ResourceDirectoryError::backend)?,
        indexed_state: row.try_get(8).map_err(ResourceDirectoryError::backend)?,
    })
}

fn row_to_durable_resource_directory_record(
    row: &Row,
) -> ResourceDirectoryResult<DurableResourceDirectoryRecord> {
    Ok(DurableResourceDirectoryRecord {
        resource: ExternalResourceRef {
            reference: row.try_get(0).map_err(ResourceDirectoryError::backend)?,
            provider: row.try_get(1).map_err(ResourceDirectoryError::backend)?,
            account_id: row.try_get(2).map_err(ResourceDirectoryError::backend)?,
            resource_type: row.try_get(3).map_err(ResourceDirectoryError::backend)?,
            title: row.try_get(4).map_err(ResourceDirectoryError::backend)?,
            source: row.try_get(5).map_err(ResourceDirectoryError::backend)?,
            permissions_summary: row.try_get(6).map_err(ResourceDirectoryError::backend)?,
            digest: row.try_get(7).map_err(ResourceDirectoryError::backend)?,
            indexed_state: row.try_get(8).map_err(ResourceDirectoryError::backend)?,
        },
        created_at: row.try_get(9).map_err(ResourceDirectoryError::backend)?,
        updated_at: row.try_get(10).map_err(ResourceDirectoryError::backend)?,
        last_seen_at: row.try_get(11).map_err(ResourceDirectoryError::backend)?,
    })
}

#[derive(Clone)]
pub struct PostgresResourceDirectoryFactory {
    directory: Arc<PostgresResourceDirectory>,
}

impl PostgresResourceDirectoryFactory {
    #[must_use]
    pub fn new(directory: Arc<PostgresResourceDirectory>) -> Self {
        Self { directory }
    }
}

impl ResourceDirectoryFactory for PostgresResourceDirectoryFactory {
    fn open(
        &self,
        handle: &StorageHandle,
    ) -> ResourceDirectoryResult<Arc<dyn ResourceDirectoryRepository>> {
        if handle.backend != StorageBackendKind::Postgres
            || handle.domain != CONNECTOR_DIRECTORY_DOMAIN
        {
            return Err(ResourceDirectoryError::backend(
                "postgres resource directory received an incompatible storage endpoint",
            ));
        }
        Ok(self.directory.clone())
    }

    fn is_initialized(&self, handle: &StorageHandle) -> bool {
        handle.backend == StorageBackendKind::Postgres
            && handle.domain == CONNECTOR_DIRECTORY_DOMAIN
            && self
                .directory
                .executor
                .checkout_runtime()
                .and_then(|mut connection| {
                    connection
                        .query_one("SELECT to_regclass('connector_resources') IS NOT NULL", &[])
                        .map_err(storage::StorageError::from)
                })
                .map(|row| row.get::<_, bool>(0))
                .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDirectoryMigrationReceipt {
    pub domain: String,
    pub resource_count: usize,
    pub source_binding_count: usize,
    pub source_digest: String,
    pub target_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDirectoryCutoverManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub resource_count: usize,
    pub source_binding_count: usize,
    pub completed_at: String,
}

/// Copies a quiesced directory into a fresh target and verifies canonical
/// source/target digests. The caller owns the maintenance barrier; a second
/// source digest is taken before the manifest can be written, so concurrent
/// source mutation fails closed instead of producing a writable dual owner.
pub fn copy_quiesced_resource_directory(
    source: &dyn ResourceDirectoryRepository,
    target: &dyn ResourceDirectoryRepository,
) -> ResourceDirectoryResult<ResourceDirectoryMigrationReceipt> {
    let source_snapshot = snapshot(source)?;
    let source_digest = canonical_digest(&source_snapshot)?;
    for record in &source_snapshot.records {
        target.import_record(record)?;
    }
    for binding in &source_snapshot.bindings {
        target.import_source_binding(binding)?;
    }
    let source_after = snapshot(source)?;
    let source_after_digest = canonical_digest(&source_after)?;
    if source_after_digest != source_digest {
        return Err(ResourceDirectoryError::backend(
            "connector directory source changed while migration maintenance barrier was active",
        ));
    }
    let target_digest = canonical_digest(&snapshot(target)?)?;
    if target_digest != source_digest {
        return Err(ResourceDirectoryError::backend(
            "connector directory target digest differs from source after copy",
        ));
    }
    Ok(ResourceDirectoryMigrationReceipt {
        domain: CONNECTOR_DIRECTORY_DOMAIN.to_string(),
        resource_count: source_snapshot.records.len(),
        source_binding_count: source_snapshot.bindings.len(),
        source_digest,
        target_digest,
    })
}

/// Writes the cutover manifest atomically after a successful verified copy.
/// This does not change the active backend; V572 owns the global cutover.
pub fn write_cutover_manifest(
    path: impl AsRef<Path>,
    receipt: &ResourceDirectoryMigrationReceipt,
) -> ResourceDirectoryResult<ResourceDirectoryCutoverManifest> {
    let manifest = ResourceDirectoryCutoverManifest {
        domain: receipt.domain.clone(),
        source_digest: receipt.source_digest.clone(),
        target_digest: receipt.target_digest.clone(),
        resource_count: receipt.resource_count,
        source_binding_count: receipt.source_binding_count,
        completed_at: Utc::now().to_rfc3339(),
    };
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(ResourceDirectoryError::backend)?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(&manifest).map_err(ResourceDirectoryError::backend)?,
    )
    .map_err(ResourceDirectoryError::backend)?;
    fs::rename(&temporary, path).map_err(ResourceDirectoryError::backend)?;
    Ok(manifest)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ResourceDirectorySnapshot {
    records: Vec<DurableResourceDirectoryRecord>,
    bindings: Vec<ResourceDirectorySourceBinding>,
}

fn snapshot(
    directory: &dyn ResourceDirectoryRepository,
) -> ResourceDirectoryResult<ResourceDirectorySnapshot> {
    let mut records = directory.export_records()?;
    records.sort_by(|left, right| left.resource.reference.cmp(&right.resource.reference));
    let mut bindings = Vec::new();
    for record in &records {
        bindings.extend(directory.list_sources(&record.resource.reference)?);
    }
    bindings.sort_by(|left, right| {
        (&left.reference, &left.source_kind, &left.source_id).cmp(&(
            &right.reference,
            &right.source_kind,
            &right.source_id,
        ))
    });
    Ok(ResourceDirectorySnapshot { records, bindings })
}

fn canonical_digest(snapshot: &ResourceDirectorySnapshot) -> ResourceDirectoryResult<String> {
    let encoded = serde_json::to_vec(snapshot).map_err(ResourceDirectoryError::backend)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use connector::{ResourceDirectoryRepository, SqliteResourceDirectory};
    use storage::{PostgresConnectionConfig, PostgresMigrationSpec, StaticSecretRefResolver};

    use super::*;

    fn resource(id: &str, title: &str) -> ExternalResourceRef {
        ExternalResourceRef::new("feishu", "bitable", id, title)
    }

    #[test]
    fn postgres_resource_directory_migrates_restarts_and_copies_real_database() {
        let Some(url) = std::env::var("COWD_TEST_POSTGRES_URL").ok() else {
            eprintln!("skipping real PostgreSQL test: COWD_TEST_POSTGRES_URL is not set");
            return;
        };
        let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url)]);
        let directory = PostgresResourceDirectory::connect(
            PostgresConnectionConfig::new("connector-directory-test", "test.pg", "cowd-v567-test"),
            &resolver,
        )
        .expect("postgres directory opens");
        let checksum_original = PostgresMigrationSpec {
            id: "connector_directory.test_checksum",
            domain: "connector_directory_test_checksum",
            version: 1,
            description: "create checksum probe",
            statements: &["CREATE TABLE connector_directory_checksum_probe(id TEXT PRIMARY KEY)"],
        };
        directory
            .executor()
            .apply_migrations(checksum_original.domain, &[checksum_original.clone()])
            .expect("initial checksum migration applies");
        let checksum_changed = PostgresMigrationSpec {
            statements: &["CREATE TABLE connector_directory_checksum_probe(id TEXT PRIMARY KEY, state TEXT NOT NULL)"],
            ..checksum_original
        };
        let checksum_error = directory
            .executor()
            .apply_migrations(checksum_changed.domain, &[checksum_changed])
            .expect_err("changed migration checksum fails closed");
        assert!(checksum_error.to_string().contains("checksum mismatch"));
        let first = resource("v567-first", "PostgreSQL integration first resource");
        let second = resource("v567-second", "PostgreSQL integration second resource");
        directory.upsert(&first).expect("first upsert");
        directory.upsert(&second).expect("second upsert");
        let workers = (0..8)
            .map(|index| {
                let directory = directory.clone();
                std::thread::spawn(move || {
                    directory
                        .upsert(&resource(
                            &format!("v567-parallel-{index}"),
                            "parallel PostgreSQL resource",
                        ))
                        .expect("parallel upsert");
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("parallel worker completes");
        }
        assert_eq!(directory.list_page(32, 0).expect("parallel list").len(), 10);
        directory
            .attach_source(&first.reference, "bitable", "app-token/table-id")
            .expect("source attachment");
        assert_eq!(
            directory.search("integration", 10).expect("search").len(),
            2
        );
        assert!(directory
            .mark_indexed(&first.reference)
            .expect("mark indexed"));
        assert_eq!(
            directory
                .get(&first.reference)
                .expect("read back")
                .expect("first exists")
                .indexed_state,
            "indexed"
        );
        let restarted = PostgresResourceDirectory::connect(
            PostgresConnectionConfig::new(
                "connector-directory-test",
                "test.pg",
                "cowd-v567-test-restart",
            ),
            &resolver,
        )
        .expect("postgres directory restarts");
        assert_eq!(
            restarted
                .get(&first.reference)
                .expect("restart read back")
                .expect("first remains after restart")
                .indexed_state,
            "indexed"
        );
        let factory = PostgresResourceDirectoryFactory::new(Arc::new(restarted));
        assert!(factory.is_initialized(&StorageHandle::postgres(
            CONNECTOR_DIRECTORY_DOMAIN,
            "connector",
            "test"
        )));
        let mut connection = factory
            .directory
            .executor()
            .checkout_runtime()
            .expect("checkout target reset connection");
        connection
            .batch_execute(
                "DELETE FROM connector_resource_sources; DELETE FROM connector_resources;",
            )
            .expect("reset isolated target database");
        drop(connection);
        let temporary = tempfile::tempdir().expect("temporary directory");
        let source = SqliteResourceDirectory::open(temporary.path().join("source.sqlite"))
            .expect("sqlite source opens");
        let first = resource("v567-copy", "Copy source resource");
        source.upsert(&first).expect("source upsert");
        source
            .attach_source(&first.reference, "bitable", "source-id")
            .expect("source binding");
        let target = PostgresResourceDirectory::connect(
            PostgresConnectionConfig::new(
                "connector-directory-copy-test",
                "test.pg",
                "cowd-v567-copy-test",
            ),
            &resolver,
        )
        .expect("postgres target opens");
        let receipt = copy_quiesced_resource_directory(&source, &target).expect("copy succeeds");
        assert_eq!(receipt.source_digest, receipt.target_digest);
        let manifest = write_cutover_manifest(temporary.path().join("cutover.json"), &receipt)
            .expect("manifest writes atomically");
        assert_eq!(manifest.source_digest, manifest.target_digest);
    }
}
