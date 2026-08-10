//! SQLite implementation of the canonical Fact/Growth ledger.
//!
//! All callers use `fact_kernel::FactLedger`; this crate is the only place
//! where the SQLite schema and SQL for that domain are allowed to live.

use chrono::Utc;
use fact_kernel::{
    EvidencePacket, FactLedger, FactLedgerError, FactLedgerResult, FactLedgerSnapshot,
    FactRecallQuery, FactRecord, GrowthPromotionRecord,
};
use harness_contract::growth::GrowthEvent;
use rusqlite::{params, OptionalExtension};
use storage::{
    MigrationRunner, SqliteExecutor, StorageBackendKind, StorageDomainId, StorageEndpoint,
    StorageMigrationSpec,
};

const FACT_DOMAIN: &str = "fact";
const FACT_MIGRATIONS: &[StorageMigrationSpec] = &[
    StorageMigrationSpec {
        id: "fact.0002.ledger",
        domain: FACT_DOMAIN,
        version: 2,
        description: "create canonical fact, evidence, growth event, and promotion ledger",
        statements: &[
            "CREATE TABLE IF NOT EXISTS fact_records (
            fact_id TEXT PRIMARY KEY,
            fact_type TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )",
            "CREATE TABLE IF NOT EXISTS fact_evidence (
            evidence_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            collected_at TEXT NOT NULL
        )",
            "CREATE TABLE IF NOT EXISTS growth_events (
            event_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            source_event_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )",
            "CREATE TABLE IF NOT EXISTS growth_promotions (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            target TEXT NOT NULL,
            status TEXT NOT NULL,
            target_id TEXT,
            summary TEXT NOT NULL,
            error TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )",
            "CREATE INDEX IF NOT EXISTS idx_fact_records_updated
            ON fact_records(updated_at DESC, fact_id ASC)",
            "CREATE INDEX IF NOT EXISTS idx_fact_evidence_collected
            ON fact_evidence(collected_at DESC, evidence_id ASC)",
            "CREATE INDEX IF NOT EXISTS idx_growth_events_created
            ON growth_events(created_at DESC, event_id ASC)",
            "CREATE INDEX IF NOT EXISTS idx_growth_promotions_event_created
            ON growth_promotions(event_id, created_at DESC, id ASC)",
        ],
    },
    StorageMigrationSpec {
        id: "fact.0003.legacy_growth_import_marker",
        domain: FACT_DOMAIN,
        version: 3,
        description: "record verified imports from the legacy growth sqlite ledger",
        statements: &["CREATE TABLE IF NOT EXISTS fact_legacy_growth_imports (
            source_identity TEXT PRIMARY KEY,
            source_digest TEXT NOT NULL,
            imported_at TEXT NOT NULL
        )"],
    },
    StorageMigrationSpec {
        id: "fact.0004.bounded_recall",
        domain: FACT_DOMAIN,
        version: 4,
        description: "materialize Fact scope and boundary columns for authorized bounded recall",
        statements: &[
            "ALTER TABLE fact_records ADD COLUMN scope_key TEXT",
            "ALTER TABLE fact_records ADD COLUMN boundary TEXT",
            "UPDATE fact_records
             SET scope_key = json_extract(payload_json, '$.scope_key'),
                 boundary = json_extract(payload_json, '$.boundary')",
            "CREATE INDEX IF NOT EXISTS idx_fact_records_recall
             ON fact_records(scope_key, boundary, updated_at DESC, fact_id ASC)",
        ],
    },
];

#[derive(Debug, Clone)]
pub struct SqliteFactLedger {
    executor: SqliteExecutor,
}

impl SqliteFactLedger {
    pub fn open(endpoint: &StorageEndpoint) -> FactLedgerResult<Self> {
        if endpoint.domain != StorageDomainId::Fact {
            return Err(FactLedgerError::backend(format!(
                "fact ledger requires fact endpoint, received `{}`",
                endpoint.logical_id()
            )));
        }
        if endpoint.backend != StorageBackendKind::Sqlite {
            return Err(FactLedgerError::backend(format!(
                "fact ledger endpoint `{}` is not sqlite-backed",
                endpoint.logical_id()
            )));
        }
        let executor = SqliteExecutor::for_endpoint(endpoint).map_err(storage_error)?;
        let connection = executor.checkout().map_err(storage_error)?;
        let reports =
            MigrationRunner::run_sqlite_domain(&connection, &endpoint.as_handle(), FACT_MIGRATIONS)
                .map_err(storage_error)?;
        if let Some(failed) = reports.iter().find(|report| report.status == "failed") {
            return Err(FactLedgerError::backend(format!(
                "fact ledger migration `{}` failed: {}",
                failed.id,
                failed
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            )));
        }
        Ok(Self { executor })
    }

    /// Open the canonical ledger and import an older standalone Growth SQLite
    /// file exactly once per source digest. The import is only an upgrade
    /// bridge: normal V573+ reads and writes always use the Fact endpoint.
    pub fn open_with_legacy_growth(
        endpoint: &StorageEndpoint,
        legacy_growth_endpoint: &StorageEndpoint,
    ) -> FactLedgerResult<Self> {
        let ledger = Self::open(endpoint)?;
        ledger.import_legacy_growth(legacy_growth_endpoint)?;
        Ok(ledger)
    }

    #[must_use]
    pub fn executor(&self) -> &SqliteExecutor {
        &self.executor
    }

    fn snapshot(&self) -> FactLedgerResult<FactLedgerSnapshot> {
        let snapshot = FactLedgerSnapshot {
            facts: self.list_facts()?,
            evidence: self.list_evidence()?,
            growth_events: self.list_growth_events()?,
            growth_promotions: self.list_growth_promotions()?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn import_legacy_growth(&self, endpoint: &StorageEndpoint) -> FactLedgerResult<()> {
        if endpoint.domain != StorageDomainId::Growth {
            return Err(FactLedgerError::backend(format!(
                "legacy Growth import requires growth endpoint, received `{}`",
                endpoint.logical_id()
            )));
        }
        if endpoint.backend != StorageBackendKind::Sqlite {
            return Err(FactLedgerError::backend(format!(
                "legacy Growth endpoint `{}` is not sqlite-backed",
                endpoint.logical_id()
            )));
        }
        if !endpoint.path.exists() {
            return Ok(());
        }
        let source = SqliteExecutor::for_endpoint(endpoint).map_err(storage_error)?;
        let connection = source.checkout().map_err(storage_error)?;
        if !table_exists(&connection, "growth_events")? {
            return Ok(());
        }
        let growth_events = list_json_from_connection(
            &connection,
            "SELECT payload FROM growth_events ORDER BY created_at DESC, event_id ASC",
        )?;
        let growth_promotions = if table_exists(&connection, "growth_promotions")? {
            let mut statement = connection
                .prepare(
                    "SELECT id, event_id, target, status, target_id, summary, created_at
                     FROM growth_promotions ORDER BY created_at DESC, id ASC",
                )
                .map_err(sqlite_error)?;
            let records = statement
                .query_map([], |row| {
                    Ok(GrowthPromotionRecord {
                        id: row.get(0)?,
                        event_id: row.get(1)?,
                        target: row.get(2)?,
                        status: row.get(3)?,
                        target_id: row.get(4)?,
                        summary: row.get(5)?,
                        error: None,
                        created_at: row.get(6)?,
                    })
                })
                .map_err(sqlite_error)?
                .map(|row| row.map_err(sqlite_error))
                .collect::<FactLedgerResult<Vec<_>>>()?;
            records
        } else {
            Vec::new()
        };
        let snapshot = FactLedgerSnapshot {
            facts: Vec::new(),
            evidence: Vec::new(),
            growth_events,
            growth_promotions,
        };
        let source_digest = snapshot.canonical_digest()?;
        let source_identity = endpoint.logical_id();
        let mut target = self.executor.checkout().map_err(storage_error)?;
        let transaction = target.transaction().map_err(sqlite_error)?;
        let existing_marker = transaction
            .query_row(
                "SELECT source_digest FROM fact_legacy_growth_imports WHERE source_identity=?1",
                params![source_identity],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        if let Some(imported_digest) = existing_marker {
            if imported_digest == source_digest {
                return Ok(());
            }
            return Err(FactLedgerError::backend(format!(
                "legacy Growth source `{source_identity}` changed after its verified one-time import; quiesce the old writer and complete an explicit cutover"
            )));
        }
        for event in snapshot.growth_events {
            record_growth_event_on(&transaction, event)?;
        }
        for record in snapshot.growth_promotions {
            record_growth_promotion_on(&transaction, record)?;
        }
        transaction
            .execute(
                "INSERT INTO fact_legacy_growth_imports(source_identity, source_digest, imported_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(source_identity) DO UPDATE SET
                    source_digest=excluded.source_digest,
                    imported_at=excluded.imported_at",
                params![source_identity, source_digest, Utc::now().to_rfc3339()],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }
}

impl FactLedger for SqliteFactLedger {
    fn upsert_fact(&self, fact: FactRecord) -> FactLedgerResult<FactRecord> {
        let payload = serde_json::to_string(&fact).map_err(json_error)?;
        self.executor
            .checkout()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO fact_records(
                    fact_id, fact_type, status, payload_json, updated_at, scope_key, boundary
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(fact_id) DO UPDATE SET
                    fact_type=excluded.fact_type,
                    status=excluded.status,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at,
                    scope_key=excluded.scope_key,
                    boundary=excluded.boundary",
                params![
                    fact.id.as_str(),
                    fact.fact_type,
                    fact.status,
                    payload,
                    fact.updated_at.to_rfc3339(),
                    fact.scope_key,
                    fact.boundary.as_str(),
                ],
            )
            .map_err(sqlite_error)?;
        Ok(fact)
    }

    fn get_fact(&self, fact_id: &str) -> FactLedgerResult<Option<FactRecord>> {
        self.executor
            .checkout()
            .map_err(storage_error)?
            .query_row(
                "SELECT payload_json FROM fact_records WHERE fact_id = ?1",
                params![fact_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_error)?
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn list_facts(&self) -> FactLedgerResult<Vec<FactRecord>> {
        list_json(
            &self.executor,
            "SELECT payload_json FROM fact_records ORDER BY updated_at DESC, fact_id ASC",
        )
    }

    fn recall_facts(&self, query: &FactRecallQuery) -> FactLedgerResult<Vec<FactRecord>> {
        if !query.is_authorized() {
            return Ok(Vec::new());
        }
        let fact_ids = serde_json::to_string(&query.authorized_fact_ids).map_err(json_error)?;
        let scope_keys = serde_json::to_string(&query.authorized_scope_keys).map_err(json_error)?;
        let boundaries = serde_json::to_string(&query.authorized_boundaries).map_err(json_error)?;
        let terms = serde_json::to_string(&query.terms).map_err(json_error)?;
        let connection = self.executor.checkout().map_err(storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT payload_json FROM fact_records
                 WHERE (
                    fact_id IN (SELECT value FROM json_each(?1))
                    OR (
                        scope_key IN (SELECT value FROM json_each(?2))
                        AND boundary IN (SELECT value FROM json_each(?3))
                    )
                 )
                 AND (
                    json_array_length(?4) = 0
                    OR EXISTS (
                        SELECT 1 FROM json_each(?4) AS term
                        WHERE LOWER(json_extract(payload_json, '$.statement'))
                              LIKE '%' || term.value || '%'
                    )
                 )
                 ORDER BY COALESCE(
                            CAST(json_extract(payload_json, '$.confidence') AS INTEGER), 0
                          ) DESC,
                          updated_at DESC, fact_id ASC
                 LIMIT ?5",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map(
                params![fact_ids, scope_keys, boundaries, terms, query.limit as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(sqlite_error)?;
        rows.map(|row| {
            row.map_err(sqlite_error)
                .and_then(|payload| serde_json::from_str(&payload).map_err(json_error))
        })
        .collect()
    }

    fn upsert_evidence(&self, evidence: EvidencePacket) -> FactLedgerResult<EvidencePacket> {
        let payload = serde_json::to_string(&evidence).map_err(json_error)?;
        self.executor
            .checkout()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO fact_evidence(evidence_id, source_kind, payload_json, collected_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(evidence_id) DO UPDATE SET
                    source_kind=excluded.source_kind,
                    payload_json=excluded.payload_json,
                    collected_at=excluded.collected_at",
                params![
                    evidence.id.as_str(),
                    format!("{:?}", evidence.source.kind),
                    payload,
                    evidence.collected_at.to_rfc3339(),
                ],
            )
            .map_err(sqlite_error)?;
        Ok(evidence)
    }

    fn get_evidence(&self, evidence_id: &str) -> FactLedgerResult<Option<EvidencePacket>> {
        self.executor
            .checkout()
            .map_err(storage_error)?
            .query_row(
                "SELECT payload_json FROM fact_evidence WHERE evidence_id = ?1",
                params![evidence_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sqlite_error)?
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn list_evidence(&self) -> FactLedgerResult<Vec<EvidencePacket>> {
        list_json(
            &self.executor,
            "SELECT payload_json FROM fact_evidence ORDER BY collected_at DESC, evidence_id ASC",
        )
    }

    fn record_growth_event(&self, event: GrowthEvent) -> FactLedgerResult<()> {
        let payload = serde_json::to_string(&event).map_err(json_error)?;
        self.executor
            .checkout()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO growth_events(event_id, session_id, source_event_kind, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(event_id) DO UPDATE SET
                    session_id=excluded.session_id,
                    source_event_kind=excluded.source_event_kind,
                    payload_json=excluded.payload_json",
                params![
                    event.id,
                    event.session_id,
                    event.source_event_kind,
                    payload,
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn list_growth_events(&self) -> FactLedgerResult<Vec<GrowthEvent>> {
        list_json(
            &self.executor,
            "SELECT payload_json FROM growth_events ORDER BY created_at DESC, event_id ASC",
        )
    }

    fn record_growth_promotion(&self, record: GrowthPromotionRecord) -> FactLedgerResult<()> {
        let payload = serde_json::to_string(&record).map_err(json_error)?;
        self.executor
            .checkout()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO growth_promotions(
                    id, event_id, target, status, target_id, summary, error, payload_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                    event_id=excluded.event_id,
                    target=excluded.target,
                    status=excluded.status,
                    target_id=excluded.target_id,
                    summary=excluded.summary,
                    error=excluded.error,
                    payload_json=excluded.payload_json,
                    created_at=excluded.created_at",
                params![
                    record.id,
                    record.event_id,
                    record.target,
                    record.status,
                    record.target_id,
                    record.summary,
                    record.error,
                    payload,
                    record.created_at,
                ],
            )
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn list_growth_promotions(&self) -> FactLedgerResult<Vec<GrowthPromotionRecord>> {
        list_json(
            &self.executor,
            "SELECT payload_json FROM growth_promotions ORDER BY created_at DESC, id ASC",
        )
    }

    fn export_snapshot(&self) -> FactLedgerResult<FactLedgerSnapshot> {
        self.snapshot()
    }

    fn persist_growth_fact_batch(
        &self,
        batch: fact_kernel::FactGrowthBatch,
    ) -> FactLedgerResult<()> {
        let mut connection = self.executor.checkout().map_err(storage_error)?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        record_growth_event_on(&transaction, batch.event)?;
        upsert_evidence_on(&transaction, batch.evidence)?;
        for fact in batch.facts {
            upsert_fact_on(&transaction, fact)?;
        }
        for promotion in batch.promotions {
            record_growth_promotion_on(&transaction, promotion)?;
        }
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }
}

fn upsert_fact_on(connection: &rusqlite::Connection, fact: FactRecord) -> FactLedgerResult<()> {
    let payload = serde_json::to_string(&fact).map_err(json_error)?;
    connection
        .execute(
            "INSERT INTO fact_records(
                fact_id, fact_type, status, payload_json, updated_at, scope_key, boundary
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(fact_id) DO UPDATE SET fact_type=excluded.fact_type,
                 status=excluded.status, payload_json=excluded.payload_json,
                 updated_at=excluded.updated_at, scope_key=excluded.scope_key,
                 boundary=excluded.boundary",
            params![
                fact.id.as_str(),
                fact.fact_type,
                fact.status,
                payload,
                fact.updated_at.to_rfc3339(),
                fact.scope_key,
                fact.boundary.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn upsert_evidence_on(
    connection: &rusqlite::Connection,
    evidence: EvidencePacket,
) -> FactLedgerResult<()> {
    let payload = serde_json::to_string(&evidence).map_err(json_error)?;
    connection
        .execute(
            "INSERT INTO fact_evidence(evidence_id, source_kind, payload_json, collected_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(evidence_id) DO UPDATE SET source_kind=excluded.source_kind,
                 payload_json=excluded.payload_json, collected_at=excluded.collected_at",
            params![
                evidence.id.as_str(),
                format!("{:?}", evidence.source.kind),
                payload,
                evidence.collected_at.to_rfc3339(),
            ],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn record_growth_event_on(
    connection: &rusqlite::Connection,
    event: GrowthEvent,
) -> FactLedgerResult<()> {
    let payload = serde_json::to_string(&event).map_err(json_error)?;
    connection
        .execute(
            "INSERT INTO growth_events(event_id, session_id, source_event_kind, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(event_id) DO UPDATE SET session_id=excluded.session_id,
                 source_event_kind=excluded.source_event_kind, payload_json=excluded.payload_json",
            params![
                event.id,
                event.session_id,
                event.source_event_kind,
                payload,
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn record_growth_promotion_on(
    connection: &rusqlite::Connection,
    record: GrowthPromotionRecord,
) -> FactLedgerResult<()> {
    let payload = serde_json::to_string(&record).map_err(json_error)?;
    connection
        .execute(
            "INSERT INTO growth_promotions(id, event_id, target, status, target_id, summary, error, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO UPDATE SET event_id=excluded.event_id, target=excluded.target,
                 status=excluded.status, target_id=excluded.target_id, summary=excluded.summary,
                 error=excluded.error, payload_json=excluded.payload_json, created_at=excluded.created_at",
            params![
                record.id,
                record.event_id,
                record.target,
                record.status,
                record.target_id,
                record.summary,
                record.error,
                payload,
                record.created_at,
            ],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn list_json<T>(executor: &SqliteExecutor, sql: &str) -> FactLedgerResult<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    let connection = executor.checkout().map_err(storage_error)?;
    list_json_from_connection(&connection, sql)
}

fn list_json_from_connection<T>(
    connection: &rusqlite::Connection,
    sql: &str,
) -> FactLedgerResult<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error)?;
    rows.map(|row| {
        let payload = row.map_err(sqlite_error)?;
        serde_json::from_str(&payload).map_err(json_error)
    })
    .collect()
}

fn table_exists(connection: &rusqlite::Connection, table: &str) -> FactLedgerResult<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            params![table],
            |_| Ok(()),
        )
        .optional()
        .map(|entry| entry.is_some())
        .map_err(sqlite_error)
}

fn storage_error(error: storage::StorageError) -> FactLedgerError {
    FactLedgerError::backend(error.to_string())
}

fn sqlite_error(error: rusqlite::Error) -> FactLedgerError {
    FactLedgerError::backend(error.to_string())
}

fn json_error(error: serde_json::Error) -> FactLedgerError {
    FactLedgerError::backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fact_kernel::{
        Confidence, EvidencePacket, FactId, FactLedger, FactRecallQuery, FactRecord, FactSource,
        GrowthPromotionRecord, SourceKind,
    };
    use harness_contract::reality::RealityBoundary;
    use rusqlite::params;
    use storage::{SqliteConnectionFactory, StorageDomainId, StorageEndpoint, StorageScope};

    use super::SqliteFactLedger;

    fn endpoint(path: &std::path::Path) -> StorageEndpoint {
        StorageEndpoint::sqlite(
            StorageDomainId::Fact,
            StorageScope::Global,
            path,
            "fact-sqlite-test",
            "fact.0002.ledger",
        )
    }

    fn source() -> FactSource {
        FactSource {
            kind: SourceKind::Growth,
            id: "growth-test".to_string(),
            label: None,
        }
    }

    #[test]
    fn bounded_recall_finds_old_authorized_rows_and_rejects_cross_scope_rows() {
        let temp = tempfile::tempdir().unwrap();
        let ledger = SqliteFactLedger::open(&endpoint(&temp.path().join("fact.sqlite"))).unwrap();
        let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut authorized = FactRecord::new("policy", "recall-needle authorized old fact");
        authorized.id = FactId::from_string("fact-authorized-old");
        authorized.scope_key = Some("task:task-allowed".to_string());
        authorized.boundary = RealityBoundary::Observed;
        authorized.confidence = Confidence::from_basis_points(9_000);
        authorized.updated_at = base;
        ledger.upsert_fact(authorized).unwrap();
        let mut authorized_lower =
            FactRecord::new("policy", "recall-needle authorized lower-confidence fact");
        authorized_lower.id = FactId::from_string("fact-authorized-lower");
        authorized_lower.scope_key = Some("task:task-allowed".to_string());
        authorized_lower.boundary = RealityBoundary::Observed;
        authorized_lower.confidence = Confidence::from_basis_points(8_000);
        authorized_lower.updated_at = base + chrono::Duration::seconds(1);
        ledger.upsert_fact(authorized_lower).unwrap();

        let mut cross_scope = FactRecord::new("policy", "recall-needle forbidden fact");
        cross_scope.id = FactId::from_string("fact-cross-scope");
        cross_scope.scope_key = Some("task:task-other".to_string());
        cross_scope.boundary = RealityBoundary::Observed;
        cross_scope.confidence = Confidence::from_basis_points(10_000);
        cross_scope.updated_at = base + chrono::Duration::seconds(1_000);
        ledger.upsert_fact(cross_scope).unwrap();

        for index in 0..600 {
            let mut filler = FactRecord::new("filler", format!("new unrelated {index}"));
            filler.id = FactId::from_string(format!("fact-new-{index:03}"));
            filler.scope_key = Some("task:task-allowed".to_string());
            filler.boundary = RealityBoundary::Observed;
            filler.updated_at = base + chrono::Duration::seconds(i64::from(index) + 2_000);
            ledger.upsert_fact(filler).unwrap();
        }

        let result = ledger
            .recall_facts(&FactRecallQuery::new(
                Vec::new(),
                vec!["task:task-allowed".to_string()],
                vec!["observed".to_string()],
                "recall-needle",
                1,
            ))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id.as_str(), "fact-authorized-old");
        assert!(ledger
            .recall_facts(&FactRecallQuery::new(
                Vec::new(),
                Vec::new(),
                vec!["observed".to_string()],
                "",
                8,
            ))
            .unwrap()
            .is_empty());
    }

    fn growth_event() -> harness_contract::growth::GrowthEvent {
        harness_contract::growth::GrowthEvent::from_input(
            harness_contract::growth::GrowthEventInput {
                session_id: "growth-batch-session".to_string(),
                source_event_kind: "fact-sqlite-test".to_string(),
                strategy_pattern: harness_contract::core::ExecutionPattern::Execute,
                learning_record: harness_contract::growth::LearningRecord::from_input(
                    harness_contract::growth::GrowthInput {
                        selected_pattern: harness_contract::core::ExecutionPattern::Execute,
                        complexity: harness_contract::core::TaskComplexity::Moderate,
                        risk: harness_contract::core::TaskRisk::Medium,
                        context_omitted: 0,
                        tool_requires_checkpoint: false,
                        tool_requires_human_confirm: false,
                        verification_can_finalize: true,
                        bench_passed: true,
                    },
                ),
                evidence_refs: Vec::new(),
            },
        )
    }

    #[test]
    fn reopen_snapshot_and_concurrent_idempotent_fact_upsert_are_stable() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint = endpoint(&temp.path().join("fact.sqlite"));
        let ledger = Arc::new(SqliteFactLedger::open(&endpoint).unwrap());
        let mut fact = FactRecord::new("policy", "verify durable output");
        fact.id = FactId::from_string("fact-stable");
        fact.confidence = Confidence::from_basis_points(9_000);
        let workers = (0..16)
            .map(|_| {
                let ledger = Arc::clone(&ledger);
                let fact = fact.clone();
                std::thread::spawn(move || ledger.upsert_fact(fact).unwrap())
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().unwrap().id.as_str(), "fact-stable");
        }
        let evidence = EvidencePacket::new(source(), serde_json::json!({"event":"growth-1"}));
        ledger.upsert_evidence(evidence.clone()).unwrap();
        let promotion = GrowthPromotionRecord {
            id: GrowthPromotionRecord::stable_id(
                "growth-1",
                "fact.policy",
                Some("fact-stable"),
                "ok",
            ),
            event_id: "growth-1".to_string(),
            target: "fact.policy".to_string(),
            status: "promoted".to_string(),
            target_id: Some("fact-stable".to_string()),
            summary: "ok".to_string(),
            error: None,
            created_at: "2026-07-23T00:00:00Z".to_string(),
        };
        ledger.record_growth_promotion(promotion).unwrap();
        let first_digest = ledger
            .export_snapshot()
            .unwrap()
            .canonical_digest()
            .unwrap();
        drop(ledger);
        let reopened = SqliteFactLedger::open(&endpoint).unwrap();
        assert_eq!(reopened.list_facts().unwrap().len(), 1);
        assert_eq!(
            reopened
                .get_evidence(evidence.id.as_str())
                .unwrap()
                .unwrap()
                .id,
            evidence.id
        );
        assert_eq!(
            reopened
                .export_snapshot()
                .unwrap()
                .canonical_digest()
                .unwrap(),
            first_digest
        );
    }

    #[test]
    fn imports_legacy_growth_once_by_source_digest() {
        let temp = tempfile::tempdir().unwrap();
        let fact_endpoint = endpoint(&temp.path().join("fact.sqlite"));
        let growth_endpoint = StorageEndpoint::sqlite(
            StorageDomainId::Growth,
            StorageScope::Global,
            temp.path().join("growth.sqlite"),
            "legacy-growth-test",
            "growth.v1.init",
        );
        let growth_handle = growth_endpoint.as_handle();
        let connection = SqliteConnectionFactory::default()
            .open_handle(&growth_handle)
            .unwrap();
        connection
            .execute_batch(
                "CREATE TABLE growth_events (
                    event_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    source_event_kind TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE growth_promotions (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    target_id TEXT,
                    summary TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );",
            )
            .unwrap();
        let event = growth_event();
        connection
            .execute(
                "INSERT INTO growth_events(event_id, session_id, source_event_kind, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.id,
                    event.session_id,
                    event.source_event_kind,
                    serde_json::to_string(&event).unwrap(),
                    "2026-07-23T00:00:00Z",
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO growth_promotions(id, event_id, target, status, target_id, summary, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    "legacy-promotion",
                    event.id,
                    "fact.policy",
                    "promoted",
                    "legacy-fact",
                    "legacy import",
                    "{}",
                    "2026-07-23T00:00:00Z",
                ],
            )
            .unwrap();
        drop(connection);
        let ledger =
            SqliteFactLedger::open_with_legacy_growth(&fact_endpoint, &growth_endpoint).unwrap();
        assert_eq!(ledger.list_growth_events().unwrap().len(), 1);
        assert_eq!(ledger.list_growth_promotions().unwrap().len(), 1);
        let reopened =
            SqliteFactLedger::open_with_legacy_growth(&fact_endpoint, &growth_endpoint).unwrap();
        assert_eq!(reopened.list_growth_events().unwrap().len(), 1);
        assert_eq!(reopened.list_growth_promotions().unwrap().len(), 1);

        let growth_handle = growth_endpoint.as_handle();
        let connection = SqliteConnectionFactory::default()
            .open_handle(&growth_handle)
            .unwrap();
        let changed_event = harness_contract::growth::GrowthEvent {
            id: "legacy-event-after-cutover".to_string(),
            ..growth_event()
        };
        connection
            .execute(
                "INSERT INTO growth_events(event_id, session_id, source_event_kind, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    changed_event.id,
                    changed_event.session_id,
                    changed_event.source_event_kind,
                    serde_json::to_string(&changed_event).unwrap(),
                    "2026-07-23T00:01:00Z",
                ],
            )
            .unwrap();
        drop(connection);
        let error = SqliteFactLedger::open_with_legacy_growth(&fact_endpoint, &growth_endpoint)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("changed after its verified one-time import"));
    }

    #[test]
    fn fact_growth_batch_rolls_back_when_any_receipt_write_fails() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint = endpoint(&temp.path().join("fact.sqlite"));
        let ledger = SqliteFactLedger::open(&endpoint).unwrap();
        let connection = ledger.executor().checkout().unwrap();
        connection
            .execute_batch("DROP TABLE growth_promotions")
            .unwrap();
        drop(connection);
        let event = growth_event();
        let batch = fact_kernel::FactGrowthBatch {
            event: event.clone(),
            evidence: EvidencePacket::new(source(), serde_json::json!({"batch": true})),
            facts: Vec::new(),
            promotions: vec![GrowthPromotionRecord {
                id: "rollback-promotion".to_string(),
                event_id: event.id,
                target: "fact.policy".to_string(),
                status: "promoted".to_string(),
                target_id: None,
                summary: "must rollback".to_string(),
                error: None,
                created_at: "2026-07-23T00:00:00Z".to_string(),
            }],
        };
        assert!(ledger.persist_growth_fact_batch(batch).is_err());
        let connection = ledger.executor().checkout().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE growth_promotions (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    target_id TEXT,
                    summary TEXT NOT NULL,
                    error TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )",
            )
            .unwrap();
        drop(connection);
        assert!(ledger.list_growth_events().unwrap().is_empty());
        assert!(ledger.list_evidence().unwrap().is_empty());
    }
}
