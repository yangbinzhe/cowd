//! PostgreSQL implementation and verified migration helper for Fact/Growth.
//!
//! The adapter owns SQL, schema, pooling and copy verification.  Promotion
//! policy remains in `fact-kernel`; Gateway and Runtime only see its port.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use fact_kernel::{
    EvidencePacket, FactLedger, FactLedgerError, FactLedgerResult, FactLedgerSnapshot,
    FactRecallQuery, FactRecord, GrowthPromotionRecord,
};
use harness_contract::growth::GrowthEvent;
use postgres::Row;
use serde::{Deserialize, Serialize};
use storage::{
    PostgresClient, PostgresConnectionConfig, PostgresExecutor, PostgresMigrationSpec,
    SecretRefResolver,
};

const FACT_LEDGER_DOMAIN: &str = "fact";
const FACT_LEDGER_MIGRATIONS: &[PostgresMigrationSpec] = &[
    PostgresMigrationSpec {
        id: "fact.0002.ledger",
        domain: FACT_LEDGER_DOMAIN,
        version: 2,
        description: "create canonical fact, evidence, growth event, and promotion ledger",
        statements: &[
            "CREATE TABLE IF NOT EXISTS fact_records (
            fact_id TEXT PRIMARY KEY,
            fact_type TEXT NOT NULL,
            status TEXT NOT NULL,
            payload JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )",
            "CREATE TABLE IF NOT EXISTS fact_evidence (
            evidence_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            payload JSONB NOT NULL,
            collected_at TIMESTAMPTZ NOT NULL
        )",
            "CREATE TABLE IF NOT EXISTS growth_events (
            event_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            source_event_kind TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )",
            "CREATE TABLE IF NOT EXISTS growth_promotions (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            target TEXT NOT NULL,
            status TEXT NOT NULL,
            target_id TEXT,
            summary TEXT NOT NULL,
            error TEXT,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
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
    PostgresMigrationSpec {
        id: "fact.0004.bounded_recall",
        domain: FACT_LEDGER_DOMAIN,
        version: 4,
        description: "materialize Fact scope and boundary columns for authorized bounded recall",
        statements: &[
            "ALTER TABLE fact_records ADD COLUMN IF NOT EXISTS scope_key TEXT",
            "ALTER TABLE fact_records ADD COLUMN IF NOT EXISTS boundary TEXT",
            "UPDATE fact_records
             SET scope_key = payload->>'scope_key', boundary = payload->>'boundary'
             WHERE scope_key IS DISTINCT FROM payload->>'scope_key'
                OR boundary IS DISTINCT FROM payload->>'boundary'",
            "CREATE INDEX IF NOT EXISTS idx_fact_records_recall
             ON fact_records(scope_key, boundary, updated_at DESC, fact_id ASC)",
        ],
    },
];

#[derive(Clone, Debug)]
pub struct PostgresFactLedger {
    executor: PostgresExecutor,
}

impl PostgresFactLedger {
    pub fn new(executor: PostgresExecutor) -> FactLedgerResult<Self> {
        executor
            .apply_migrations(FACT_LEDGER_DOMAIN, FACT_LEDGER_MIGRATIONS)
            .map_err(storage_error)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> FactLedgerResult<Self> {
        PostgresExecutor::connect(config, resolver)
            .map_err(storage_error)
            .and_then(Self::new)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
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
}

impl FactLedger for PostgresFactLedger {
    fn upsert_fact(&self, fact: FactRecord) -> FactLedgerResult<FactRecord> {
        let payload = serde_json::to_value(&fact).map_err(json_error)?;
        self.executor
            .checkout_critical()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO fact_records(
                    fact_id, fact_type, status, payload, updated_at, scope_key, boundary
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT(fact_id) DO UPDATE SET
                    fact_type=EXCLUDED.fact_type,
                    status=EXCLUDED.status,
                    payload=EXCLUDED.payload,
                    updated_at=EXCLUDED.updated_at,
                    scope_key=EXCLUDED.scope_key,
                    boundary=EXCLUDED.boundary",
                &[
                    &fact.id.as_str(),
                    &fact.fact_type,
                    &fact.status,
                    &payload,
                    &fact.updated_at,
                    &fact.scope_key,
                    &fact.boundary.as_str(),
                ],
            )
            .map_err(postgres_error)?;
        Ok(fact)
    }

    fn get_fact(&self, fact_id: &str) -> FactLedgerResult<Option<FactRecord>> {
        self.executor
            .checkout_online_read()
            .map_err(storage_error)?
            .query_opt(
                "SELECT payload FROM fact_records WHERE fact_id=$1",
                &[&fact_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_json(&row))
            .transpose()
    }

    fn list_facts(&self) -> FactLedgerResult<Vec<FactRecord>> {
        list_json(
            &mut self
                .executor
                .checkout_online_read()
                .map_err(storage_error)?,
            "SELECT payload FROM fact_records ORDER BY updated_at DESC, fact_id ASC",
        )
    }

    fn recall_facts(&self, query: &FactRecallQuery) -> FactLedgerResult<Vec<FactRecord>> {
        if !query.is_authorized() {
            return Ok(Vec::new());
        }
        let limit = query.limit as i64;
        self.executor
            .checkout_online_read()
            .map_err(storage_error)?
            .query(
                "SELECT payload FROM fact_records
                 WHERE (
                    fact_id = ANY($1)
                    OR (scope_key = ANY($2) AND boundary = ANY($3))
                 )
                 AND (
                    cardinality($4::text[]) = 0
                    OR EXISTS (
                        SELECT 1 FROM unnest($4::text[]) AS term
                        WHERE LOWER(payload->>'statement') LIKE '%' || term || '%'
                    )
                 )
                 ORDER BY COALESCE((payload->>'confidence')::integer, 0) DESC,
                          updated_at DESC, fact_id ASC
                 LIMIT $5",
                &[
                    &query.authorized_fact_ids,
                    &query.authorized_scope_keys,
                    &query.authorized_boundaries,
                    &query.terms,
                    &limit,
                ],
            )
            .map_err(postgres_error)?
            .into_iter()
            .map(|row| row_json(&row))
            .collect()
    }

    fn upsert_evidence(&self, evidence: EvidencePacket) -> FactLedgerResult<EvidencePacket> {
        let payload = serde_json::to_value(&evidence).map_err(json_error)?;
        self.executor
            .checkout_critical()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO fact_evidence(evidence_id, source_kind, payload, collected_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(evidence_id) DO UPDATE SET
                    source_kind=EXCLUDED.source_kind,
                    payload=EXCLUDED.payload,
                    collected_at=EXCLUDED.collected_at",
                &[
                    &evidence.id.as_str(),
                    &format!("{:?}", evidence.source.kind),
                    &payload,
                    &evidence.collected_at,
                ],
            )
            .map_err(postgres_error)?;
        Ok(evidence)
    }

    fn get_evidence(&self, evidence_id: &str) -> FactLedgerResult<Option<EvidencePacket>> {
        self.executor
            .checkout_online_read()
            .map_err(storage_error)?
            .query_opt(
                "SELECT payload FROM fact_evidence WHERE evidence_id=$1",
                &[&evidence_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_json(&row))
            .transpose()
    }

    fn list_evidence(&self) -> FactLedgerResult<Vec<EvidencePacket>> {
        list_json(
            &mut self
                .executor
                .checkout_online_read()
                .map_err(storage_error)?,
            "SELECT payload FROM fact_evidence ORDER BY collected_at DESC, evidence_id ASC",
        )
    }

    fn record_growth_event(&self, event: GrowthEvent) -> FactLedgerResult<()> {
        let payload = serde_json::to_value(&event).map_err(json_error)?;
        self.executor
            .checkout_critical()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO growth_events(event_id, session_id, source_event_kind, payload, created_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(event_id) DO UPDATE SET
                    session_id=EXCLUDED.session_id,
                    source_event_kind=EXCLUDED.source_event_kind,
                    payload=EXCLUDED.payload",
                &[
                    &event.id,
                    &event.session_id,
                    &event.source_event_kind,
                    &payload,
                    &Utc::now(),
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    fn list_growth_events(&self) -> FactLedgerResult<Vec<GrowthEvent>> {
        list_json(
            &mut self
                .executor
                .checkout_online_read()
                .map_err(storage_error)?,
            "SELECT payload FROM growth_events ORDER BY created_at DESC, event_id ASC",
        )
    }

    fn record_growth_promotion(&self, record: GrowthPromotionRecord) -> FactLedgerResult<()> {
        let payload = serde_json::to_value(&record).map_err(json_error)?;
        let created_at = chrono::DateTime::parse_from_rfc3339(&record.created_at)
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .with_timezone(&Utc);
        self.executor
            .checkout_critical()
            .map_err(storage_error)?
            .execute(
                "INSERT INTO growth_promotions(
                    id, event_id, target, status, target_id, summary, error, payload, created_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT(id) DO UPDATE SET
                    event_id=EXCLUDED.event_id,
                    target=EXCLUDED.target,
                    status=EXCLUDED.status,
                    target_id=EXCLUDED.target_id,
                    summary=EXCLUDED.summary,
                    error=EXCLUDED.error,
                    payload=EXCLUDED.payload,
                    created_at=EXCLUDED.created_at",
                &[
                    &record.id,
                    &record.event_id,
                    &record.target,
                    &record.status,
                    &record.target_id,
                    &record.summary,
                    &record.error,
                    &payload,
                    &created_at,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    fn list_growth_promotions(&self) -> FactLedgerResult<Vec<GrowthPromotionRecord>> {
        list_json(
            &mut self
                .executor
                .checkout_online_read()
                .map_err(storage_error)?,
            "SELECT payload FROM growth_promotions ORDER BY created_at DESC, id ASC",
        )
    }

    fn export_snapshot(&self) -> FactLedgerResult<FactLedgerSnapshot> {
        self.snapshot()
    }

    fn import_snapshot(&self, snapshot: &FactLedgerSnapshot) -> FactLedgerResult<()> {
        snapshot.validate()?;
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut tx = connection.transaction().map_err(postgres_error)?;
        for fact in snapshot.facts.iter().cloned() {
            upsert_fact_in(&mut tx, fact)?;
        }
        for evidence in snapshot.evidence.iter().cloned() {
            upsert_evidence_in(&mut tx, evidence)?;
        }
        for event in snapshot.growth_events.iter().cloned() {
            record_growth_event_in(&mut tx, event)?;
        }
        for record in snapshot.growth_promotions.iter().cloned() {
            record_growth_promotion_in(&mut tx, record)?;
        }
        tx.commit().map_err(postgres_error)?;
        Ok(())
    }

    fn persist_growth_fact_batch(
        &self,
        batch: fact_kernel::FactGrowthBatch,
    ) -> FactLedgerResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut tx = connection.transaction().map_err(postgres_error)?;
        record_growth_event_in(&mut tx, batch.event)?;
        upsert_evidence_in(&mut tx, batch.evidence)?;
        for fact in batch.facts {
            upsert_fact_in(&mut tx, fact)?;
        }
        for promotion in batch.promotions {
            record_growth_promotion_in(&mut tx, promotion)?;
        }
        tx.commit().map_err(postgres_error)?;
        Ok(())
    }
}

fn upsert_fact_in(client: &mut impl PostgresClient, fact: FactRecord) -> FactLedgerResult<()> {
    let payload = serde_json::to_value(&fact).map_err(json_error)?;
    client
        .execute(
            "INSERT INTO fact_records(
                fact_id, fact_type, status, payload, updated_at, scope_key, boundary
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT(fact_id) DO UPDATE SET fact_type=EXCLUDED.fact_type, status=EXCLUDED.status,
                 payload=EXCLUDED.payload, updated_at=EXCLUDED.updated_at,
                 scope_key=EXCLUDED.scope_key, boundary=EXCLUDED.boundary",
            &[
                &fact.id.as_str(),
                &fact.fact_type,
                &fact.status,
                &payload,
                &fact.updated_at,
                &fact.scope_key,
                &fact.boundary.as_str(),
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn upsert_evidence_in(
    client: &mut impl PostgresClient,
    evidence: EvidencePacket,
) -> FactLedgerResult<()> {
    let payload = serde_json::to_value(&evidence).map_err(json_error)?;
    client
        .execute(
            "INSERT INTO fact_evidence(evidence_id, source_kind, payload, collected_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(evidence_id) DO UPDATE SET source_kind=EXCLUDED.source_kind,
                 payload=EXCLUDED.payload, collected_at=EXCLUDED.collected_at",
            &[
                &evidence.id.as_str(),
                &format!("{:?}", evidence.source.kind),
                &payload,
                &evidence.collected_at,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn record_growth_event_in(
    client: &mut impl PostgresClient,
    event: GrowthEvent,
) -> FactLedgerResult<()> {
    let payload = serde_json::to_value(&event).map_err(json_error)?;
    client
        .execute(
            "INSERT INTO growth_events(event_id, session_id, source_event_kind, payload, created_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(event_id) DO UPDATE SET session_id=EXCLUDED.session_id,
                 source_event_kind=EXCLUDED.source_event_kind, payload=EXCLUDED.payload",
            &[
                &event.id,
                &event.session_id,
                &event.source_event_kind,
                &payload,
                &Utc::now(),
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn record_growth_promotion_in(
    client: &mut impl PostgresClient,
    record: GrowthPromotionRecord,
) -> FactLedgerResult<()> {
    let payload = serde_json::to_value(&record).map_err(json_error)?;
    let created_at = chrono::DateTime::parse_from_rfc3339(&record.created_at)
        .map_err(|error| FactLedgerError::backend(error.to_string()))?
        .with_timezone(&Utc);
    client
        .execute(
            "INSERT INTO growth_promotions(id, event_id, target, status, target_id, summary, error, payload, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT(id) DO UPDATE SET event_id=EXCLUDED.event_id, target=EXCLUDED.target,
                 status=EXCLUDED.status, target_id=EXCLUDED.target_id, summary=EXCLUDED.summary,
                 error=EXCLUDED.error, payload=EXCLUDED.payload, created_at=EXCLUDED.created_at",
            &[
                &record.id,
                &record.event_id,
                &record.target,
                &record.status,
                &record.target_id,
                &record.summary,
                &record.error,
                &payload,
                &created_at,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn list_json<T>(client: &mut impl PostgresClient, sql: &str) -> FactLedgerResult<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    client
        .query(sql, &[])
        .map_err(postgres_error)?
        .iter()
        .map(row_json)
        .collect()
}

fn row_json<T>(row: &Row) -> FactLedgerResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let payload: serde_json::Value = row.get(0);
    serde_json::from_value(payload).map_err(json_error)
}

fn storage_error(error: storage::StorageError) -> FactLedgerError {
    FactLedgerError::backend(error.to_string())
}

fn postgres_error(error: postgres::Error) -> FactLedgerError {
    FactLedgerError::backend(error.to_string())
}

fn json_error(error: serde_json::Error) -> FactLedgerError {
    FactLedgerError::backend(error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactLedgerMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub fact_count: usize,
    pub evidence_count: usize,
    pub growth_event_count: usize,
    pub growth_promotion_count: usize,
}

/// Copy a quiesced source exactly once.  A second source snapshot proves the
/// owner did not change during copy; the target must have the same digest
/// before the manifest is written.  Normal runtime paths never call this.
pub fn copy_quiesced_fact_ledger(
    source: &dyn FactLedger,
    target: &dyn FactLedger,
    manifest_path: impl AsRef<Path>,
) -> FactLedgerResult<FactLedgerMigrationManifest> {
    let snapshot = source.export_snapshot()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_snapshot(&snapshot)?;
    let source_after_digest = source.export_snapshot()?.canonical_digest()?;
    if source_after_digest != source_digest {
        return Err(FactLedgerError::backend(
            "fact ledger source changed while migration maintenance barrier was active",
        ));
    }
    let target_digest = target.export_snapshot()?.canonical_digest()?;
    if target_digest != source_digest {
        return Err(FactLedgerError::backend(
            "fact ledger target digest differs from source after copy",
        ));
    }
    let manifest = FactLedgerMigrationManifest {
        domain: FACT_LEDGER_DOMAIN.to_string(),
        source_digest,
        target_digest,
        fact_count: snapshot.facts.len(),
        evidence_count: snapshot.evidence.len(),
        growth_event_count: snapshot.growth_events.len(),
        growth_promotion_count: snapshot.growth_promotions.len(),
    };
    write_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn write_manifest(
    manifest_path: &Path,
    manifest: &FactLedgerMigrationManifest,
) -> FactLedgerResult<()> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).map_err(|error| FactLedgerError::backend(error.to_string()))?;
    }
    let temporary = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(manifest).map_err(json_error)?,
    )
    .map_err(|error| FactLedgerError::backend(error.to_string()))?;
    fs::rename(temporary, manifest_path)
        .map_err(|error| FactLedgerError::backend(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fact_kernel::{
        Confidence, EvidencePacket, FactGrowthBatch, FactId, FactLedger, FactRecallQuery,
        FactRecord, FactSource, GrowthPromotionRecord, SourceKind,
    };
    use harness_contract::{
        core::{ExecutionPattern, TaskComplexity, TaskRisk},
        growth::{GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthInput, LearningRecord},
    };
    use storage::{
        PostgresConnectionConfig, StaticSecretRefResolver, StorageDomainId, StorageEndpoint,
        StorageScope,
    };

    use super::*;

    fn ledger_from_url(url: String, application_name: &str) -> PostgresFactLedger {
        let resolver = StaticSecretRefResolver::new([(String::from("test"), url)]);
        PostgresFactLedger::connect(
            PostgresConnectionConfig::new("fact-postgres-test", "test", application_name),
            &resolver,
        )
        .unwrap()
    }

    fn clear(ledger: &PostgresFactLedger) {
        ledger
            .executor()
            .checkout_background()
            .unwrap()
            .batch_execute(
                "TRUNCATE TABLE growth_promotions, growth_events, fact_evidence, fact_records",
            )
            .unwrap();
    }

    fn source() -> FactSource {
        FactSource {
            kind: SourceKind::Growth,
            id: "growth-postgres-contract".to_string(),
            label: None,
        }
    }

    fn growth_event() -> GrowthEvent {
        GrowthEvent::from_input(GrowthEventInput {
            session_id: "session-postgres-contract".to_string(),
            source_event_kind: "test.fact_growth".to_string(),
            strategy_pattern: ExecutionPattern::Execute,
            learning_record: LearningRecord::from_input(GrowthInput {
                selected_pattern: ExecutionPattern::Execute,
                complexity: TaskComplexity::Moderate,
                risk: TaskRisk::Medium,
                context_omitted: 0,
                tool_requires_checkpoint: false,
                tool_requires_human_confirm: false,
                verification_can_finalize: true,
                bench_passed: true,
            }),
            evidence_refs: vec![GrowthEvidenceRef::new(
                "test",
                "postgres-contract",
                "copy ledger",
            )],
        })
    }

    #[test]
    fn promotion_stable_id_is_deterministic() {
        assert_eq!(
            GrowthPromotionRecord::stable_id("e", "fact", Some("f"), "summary"),
            "e:fact:f"
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_bounded_recall_matches_authorization_order_and_limit_contract() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let ledger = ledger_from_url(url, "fact-postgres-recall-contract");
        clear(&ledger);
        for (id, scope, confidence) in [
            ("fact-pg-recall-low", "task:allowed", 8_000),
            ("fact-pg-recall-high", "task:allowed", 9_500),
            ("fact-pg-recall-cross", "task:other", 10_000),
        ] {
            let mut fact = FactRecord::new("policy", format!("postgres recall-needle {id}"));
            fact.id = FactId::from_string(id);
            fact.scope_key = Some(scope.to_string());
            fact.boundary = harness_contract::reality::RealityBoundary::Observed;
            fact.confidence = Confidence::from_basis_points(confidence);
            ledger.upsert_fact(fact).unwrap();
        }
        let recalled = ledger
            .recall_facts(&FactRecallQuery::new(
                Vec::new(),
                vec!["task:allowed".to_string()],
                vec!["observed".to_string()],
                "recall-needle",
                1,
            ))
            .unwrap();
        assert_eq!(recalled.len(), 1);
        assert_eq!(recalled[0].id.as_str(), "fact-pg-recall-high");
        clear(&ledger);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_reopens_and_serializes_competing_fact_upserts() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let ledger = Arc::new(ledger_from_url(url.clone(), "fact-postgres-contract"));
        clear(&ledger);
        let mut fact = FactRecord::new("policy", "verify durable output");
        fact.id = FactId::from_string("fact-postgres-concurrent");
        fact.confidence = Confidence::from_basis_points(9_500);
        let workers = (0..16)
            .map(|_| {
                let ledger = Arc::clone(&ledger);
                let fact = fact.clone();
                std::thread::spawn(move || ledger.upsert_fact(fact).unwrap())
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(
                worker.join().unwrap().id.as_str(),
                "fact-postgres-concurrent"
            );
        }
        let event = growth_event();
        ledger
            .persist_growth_fact_batch(FactGrowthBatch {
                event: event.clone(),
                evidence: EvidencePacket::new(source(), serde_json::json!({"batch": true})),
                facts: Vec::new(),
                promotions: vec![GrowthPromotionRecord {
                    id: GrowthPromotionRecord::stable_id(
                        &event.id,
                        "fact.policy",
                        Some("fact-postgres-concurrent"),
                        "batch committed",
                    ),
                    event_id: event.id,
                    target: "fact.policy".to_string(),
                    status: "promoted".to_string(),
                    target_id: Some("fact-postgres-concurrent".to_string()),
                    summary: "batch committed".to_string(),
                    error: None,
                    created_at: "2026-07-23T00:00:00Z".to_string(),
                }],
            })
            .unwrap();
        let digest = ledger
            .export_snapshot()
            .unwrap()
            .canonical_digest()
            .unwrap();
        drop(ledger);
        let reopened = ledger_from_url(url, "fact-postgres-reopen-contract");
        assert_eq!(reopened.list_facts().unwrap().len(), 1);
        assert_eq!(reopened.list_growth_events().unwrap().len(), 1);
        assert_eq!(reopened.list_growth_promotions().unwrap().len(), 1);
        assert_eq!(
            reopened
                .export_snapshot()
                .unwrap()
                .canonical_digest()
                .unwrap(),
            digest
        );
        clear(&reopened);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_sqlite_to_postgres_copy_is_digest_exact_and_reopens() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let target = ledger_from_url(url, "fact-postgres-copy-contract");
        clear(&target);
        let temp = tempfile::tempdir().unwrap();
        let endpoint = StorageEndpoint::sqlite(
            StorageDomainId::Fact,
            StorageScope::Global,
            temp.path().join("fact.sqlite"),
            "fact-postgres-copy-source",
            "fact.0002.ledger",
        );
        let source_ledger = fact_sqlite::SqliteFactLedger::open(&endpoint).unwrap();
        let mut fact = FactRecord::new("policy", "copy only after quiesce");
        fact.id = FactId::from_string("fact-postgres-copy");
        source_ledger.upsert_fact(fact).unwrap();
        source_ledger
            .upsert_evidence(EvidencePacket::new(
                source(),
                serde_json::json!({"copy": true}),
            ))
            .unwrap();
        let event = growth_event();
        source_ledger.record_growth_event(event.clone()).unwrap();
        source_ledger
            .record_growth_promotion(GrowthPromotionRecord {
                id: GrowthPromotionRecord::stable_id(&event.id, "fact.policy", None, "copied"),
                event_id: event.id,
                target: "fact.policy".to_string(),
                status: "promoted".to_string(),
                target_id: Some("fact-postgres-copy".to_string()),
                summary: "copied".to_string(),
                error: None,
                created_at: "2026-07-23T00:00:00Z".to_string(),
            })
            .unwrap();
        let manifest_path = temp.path().join("fact-cutover.json");
        let manifest = copy_quiesced_fact_ledger(&source_ledger, &target, &manifest_path).unwrap();
        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert!(manifest_path.exists());
        assert_eq!(target.list_facts().unwrap().len(), 1);
        assert_eq!(target.list_growth_events().unwrap().len(), 1);
        clear(&target);
    }
}
