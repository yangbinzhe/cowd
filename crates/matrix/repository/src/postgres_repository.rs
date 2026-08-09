//! PostgreSQL Matrix adapter.
//!
//! Each Matrix aggregate owns a distinct PostgreSQL table.  The typed DTO is
//! retained as JSONB so Matrix-core remains the canonical schema, while stable
//! expression indexes and dedicated source-key/revision tables keep query and
//! concurrency semantics in PostgreSQL rather than in an in-process cache.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::Utc;
use matrix_core::{
    build_metric_compute_jobs, MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob,
    MatrixComputeJobInput, MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput,
    MatrixDataPlane, MatrixDataPlaneHealth, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark, MatrixEntity,
    MatrixEntityConflictDecision, MatrixEntityMatchCandidate, MatrixEvidencePacket,
    MatrixEvidenceSourceRef, MatrixFact, MatrixImpactHop, MatrixImpactTrace,
    MatrixMetricAttentionPlan, MatrixMetricAttentionScore, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricSnapshotItem,
    MatrixMetricState, MatrixOntologyPack, MatrixQualityGateDecision, MatrixQueryPlan,
    MatrixQueryResult, MatrixRelation, MatrixScenarioResult, MatrixScenarioRun,
    MatrixScenarioRunStatus, MatrixScenarioSpec, MatrixSeverity, MatrixSourceDeltaPlan,
    MatrixSourceKind, MatrixSourcePack, MatrixSourcePackValidation, MatrixSourceSnapshot,
    MatrixSourceSnapshotApplyReport, MatrixSourceSnapshotInput, MatrixSourceSnapshotPlan,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use storage::{
    PostgresClient, PostgresConnection, PostgresConnectionConfig, PostgresExecutor,
    PostgresMigrationSpec, PostgresTransaction, SecretRefResolver,
};

use crate::migration::{canonicalize_payload, MATRIX_MIGRATION_TABLES};
use crate::port::matrix_store_operations;
use crate::{
    MatrixHealth, MatrixMetricRecomputeResult, MatrixMigrationSnapshot, MatrixRevisioned,
    MatrixSqliteDataPlane, MatrixStore, MatrixStoreError, MatrixStoreResult,
};

const MATRIX_DOMAIN: &str = "matrix";
const MATRIX_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "matrix.0001.aggregate-tables",
    domain: MATRIX_DOMAIN,
    version: 1,
    description: "create Matrix aggregate, revision, source-key, and PostgreSQL query indexes",
    statements: &[r#"
        CREATE TABLE IF NOT EXISTS matrix_schema (
            id SMALLINT PRIMARY KEY CHECK (id = 1),
            schema_version BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
        INSERT INTO matrix_schema(id, schema_version, updated_at)
        VALUES (1, 20, NOW())
        ON CONFLICT(id) DO UPDATE SET
            schema_version = GREATEST(matrix_schema.schema_version, EXCLUDED.schema_version),
            updated_at = EXCLUDED.updated_at;

        CREATE TABLE IF NOT EXISTS matrix_entity (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_matrix_entity_type_key
            ON matrix_entity ((payload->>'entity_type'), (payload->>'canonical_key'));
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_updated
            ON matrix_entity (updated_at DESC, id ASC);

        CREATE TABLE IF NOT EXISTS matrix_entity_source_key (
            source_system TEXT NOT NULL, source_key TEXT NOT NULL,
            entity_id TEXT NOT NULL REFERENCES matrix_entity(id) ON DELETE CASCADE,
            source_ref TEXT, created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(source_system, source_key)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_source_entity
            ON matrix_entity_source_key(entity_id);

        CREATE TABLE IF NOT EXISTS matrix_relation (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_matrix_relation_key
            ON matrix_relation ((payload->>'relation_type'), (payload->>'from_entity_id'), (payload->>'to_entity_id'));
        CREATE INDEX IF NOT EXISTS idx_matrix_relation_from
            ON matrix_relation ((payload->>'from_entity_id'), updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_relation_to
            ON matrix_relation ((payload->>'to_entity_id'), updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_fact (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_type_time
            ON matrix_fact ((payload->>'fact_type'), ((payload->>'event_time')) DESC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_metric
            ON matrix_fact ((payload->>'metric_key')) WHERE payload ? 'metric_key';

        CREATE TABLE IF NOT EXISTS matrix_attention_item (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_attention_priority
            ON matrix_attention_item (((payload->>'priority_score')::double precision) DESC, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_evidence_packet (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_quality_gate (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_quality_gate_target
            ON matrix_quality_gate ((payload->>'target_ref'), created_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_definition (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_metric_dependency (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_matrix_metric_dependency_key
            ON matrix_metric_dependency ((payload->>'upstream_metric_id'), (payload->>'downstream_metric_id'), (payload->>'dependency_type'));
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_dependency_downstream
            ON matrix_metric_dependency ((payload->>'downstream_metric_id'));
        CREATE TABLE IF NOT EXISTS matrix_metric_state (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_state_lookup
            ON matrix_metric_state ((payload->>'metric_id'), (payload->>'entity_scope'), (payload->>'period'), updated_at DESC);
        CREATE TABLE IF NOT EXISTS matrix_metric_snapshot (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_compute_job (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_compute_job_status
            ON matrix_compute_job ((payload->>'status'), ((payload->>'priority')::double precision) DESC, updated_at DESC);
        CREATE TABLE IF NOT EXISTS matrix_change_event (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_change_detected
            ON matrix_change_event (updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_source_pack (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_source_pack_source
            ON matrix_source_pack ((payload->>'source_name'), updated_at DESC);
        CREATE TABLE IF NOT EXISTS matrix_connector_run (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_connector_run_source
            ON matrix_connector_run ((payload->>'source_pack_id'), updated_at DESC);
        CREATE TABLE IF NOT EXISTS matrix_source_snapshot (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_source_snapshot_pack
            ON matrix_source_snapshot ((payload->>'source_pack_id'), updated_at DESC);
        CREATE TABLE IF NOT EXISTS matrix_ontology_pack (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_entity_match_candidate (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_entity_conflict_decision (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_scenario_spec (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS matrix_scenario_run (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_scenario_run_scenario
            ON matrix_scenario_run ((payload->>'scenario_id'), updated_at DESC);
        CREATE TABLE IF NOT EXISTS matrix_scenario_result (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_matrix_scenario_result_run
            ON matrix_scenario_result ((payload->>'run_id'));

        CREATE TABLE IF NOT EXISTS matrix_data_plane_watermark (
            id TEXT PRIMARY KEY, payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_watermark_updated
            ON matrix_data_plane_watermark (updated_at DESC, id ASC);
        CREATE TABLE IF NOT EXISTS matrix_resource_revision (
            resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL,
            revision BIGINT NOT NULL CHECK (revision > 0), updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(resource_kind, resource_id)
        );
    "#],
}];

const ENTITY: &str = "matrix_entity";
const RELATION: &str = "matrix_relation";
const FACT: &str = "matrix_fact";
const ATTENTION: &str = "matrix_attention_item";
const EVIDENCE: &str = "matrix_evidence_packet";
const QUALITY_GATE: &str = "matrix_quality_gate";
const METRIC_DEFINITION: &str = "matrix_metric_definition";
const METRIC_DEPENDENCY: &str = "matrix_metric_dependency";
const METRIC_STATE: &str = "matrix_metric_state";
const METRIC_SNAPSHOT: &str = "matrix_metric_snapshot";
const COMPUTE_JOB: &str = "matrix_compute_job";
const CHANGE: &str = "matrix_change_event";
const SOURCE_PACK: &str = "matrix_source_pack";
const CONNECTOR_RUN: &str = "matrix_connector_run";
const SOURCE_SNAPSHOT: &str = "matrix_source_snapshot";
const ONTOLOGY_PACK: &str = "matrix_ontology_pack";
const MATCH_CANDIDATE: &str = "matrix_entity_match_candidate";
const CONFLICT_DECISION: &str = "matrix_entity_conflict_decision";
const SCENARIO_SPEC: &str = "matrix_scenario_spec";
const SCENARIO_RUN: &str = "matrix_scenario_run";
const SCENARIO_RESULT: &str = "matrix_scenario_result";
const WATERMARK: &str = "matrix_data_plane_watermark";

#[derive(Clone, Debug)]
pub struct PostgresMatrixRepository {
    executor: PostgresExecutor,
}

impl PostgresMatrixRepository {
    pub fn new(executor: PostgresExecutor) -> MatrixStoreResult<Self> {
        executor
            .apply_migrations(MATRIX_DOMAIN, MATRIX_MIGRATIONS)
            .map_err(storage_error)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> MatrixStoreResult<Self> {
        PostgresExecutor::connect(config, resolver)
            .map_err(storage_error)
            .and_then(Self::new)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    /// Export the complete logical Matrix store for a maintenance-window
    /// digest comparison.  Physical timestamps are deliberately excluded;
    /// only typed payloads and optimistic revisions define the cutover.
    pub fn export_migration_snapshot(&self) -> MatrixStoreResult<MatrixMigrationSnapshot> {
        self.with_connection(|connection| {
            let mut tables = BTreeMap::new();
            for table in MATRIX_MIGRATION_TABLES {
                tables.insert(
                    (*table).to_string(),
                    export_migration_json_records(connection, table)?,
                );
            }
            let revisions = export_revisions(connection)?;
            MatrixMigrationSnapshot::new(
                scalar_i64(
                    connection,
                    "SELECT schema_version FROM matrix_schema WHERE id = 1",
                )?,
                tables,
                revisions,
            )
        })
    }

    /// Import one verified source snapshot into an empty PostgreSQL Matrix
    /// store.  Refusing a nonempty target prevents an accidental merge or
    /// second owner during cutover.
    pub fn import_migration_snapshot(
        &self,
        snapshot: &MatrixMigrationSnapshot,
    ) -> MatrixStoreResult<()> {
        snapshot.validate()?;
        self.with_transaction(|transaction| {
            for table in MATRIX_MIGRATION_TABLES {
                if count_table(transaction, table)? != 0 {
                    return Err(MatrixStoreError::Backend(format!(
                        "matrix migration target table `{table}` is not empty"
                    )));
                }
            }
            if count_table(transaction, "matrix_resource_revision")? != 0 {
                return Err(MatrixStoreError::Backend(
                    "matrix migration target revisions are not empty".to_string(),
                ));
            }
            for table in MATRIX_MIGRATION_TABLES {
                let Some(records) = snapshot.tables.get(*table) else {
                    continue;
                };
                for (id, payload) in records {
                    let persisted_id = if *table == WATERMARK {
                        let watermark = serde_json::from_value::<MatrixDataPlaneWatermark>(payload.clone())
                            .map_err(json_error)?;
                        watermark_resource_id(&watermark)
                    } else {
                        id.clone()
                    };
                    write_json(transaction, table, &persisted_id, payload)?;
                    if *table == ENTITY {
                        let entity = serde_json::from_value::<MatrixEntity>(payload.clone())
                            .map_err(json_error)?;
                        replace_entity_source_keys(transaction, &entity)?;
                    }
                }
            }
            for (key, revision) in &snapshot.revisions {
                let (resource_kind, resource_id) = key.split_once('\0').ok_or_else(|| {
                    MatrixStoreError::Backend("matrix migration revision key is invalid".to_string())
                })?;
                persist_revision(
                    transaction,
                    resource_kind,
                    &postgres_resource_revision_id(resource_kind, resource_id),
                    *revision,
                )?;
            }
            transaction
                .execute(
                    "UPDATE matrix_schema SET schema_version = GREATEST(schema_version, $1), updated_at = NOW() WHERE id = 1",
                    &[&snapshot.schema_version],
                )
                .map_err(postgres_error)?;
            Ok(())
        })
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut PostgresConnection) -> MatrixStoreResult<T>,
    ) -> MatrixStoreResult<T> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        operation(&mut connection)
    }

    fn health(&self) -> MatrixStoreResult<MatrixHealth> {
        self.with_connection(|connection| {
            Ok(MatrixHealth {
                schema_version: scalar_i64(
                    connection,
                    "SELECT schema_version FROM matrix_schema WHERE id = 1",
                )?,
                fact_count: count_table(connection, FACT)?,
                metric_definition_count: count_table(connection, METRIC_DEFINITION)?,
                metric_state_count: count_table(connection, METRIC_STATE)?,
                change_count: count_table(connection, CHANGE)?,
                attention_count: count_table(connection, ATTENTION)?,
                evidence_count: count_table(connection, EVIDENCE)?,
                entity_count: count_table(connection, ENTITY)?,
                relation_count: count_table(connection, RELATION)?,
                metric_dependency_count: count_table(connection, METRIC_DEPENDENCY)?,
                compute_job_count: count_table(connection, COMPUTE_JOB)?,
                quality_gate_count: count_table(connection, QUALITY_GATE)?,
                source_pack_count: count_table(connection, SOURCE_PACK)?,
                data_plane_watermark_count: count_table(connection, WATERMARK)?,
                connector_run_count: count_table(connection, CONNECTOR_RUN)?,
                source_snapshot_count: count_table(connection, SOURCE_SNAPSHOT)?,
                ontology_pack_count: count_table(connection, ONTOLOGY_PACK)?,
                entity_match_candidate_count: count_table(connection, MATCH_CANDIDATE)?,
                entity_conflict_decision_count: count_table(connection, CONFLICT_DECISION)?,
                metric_snapshot_count: count_table(connection, METRIC_SNAPSHOT)?,
                scenario_spec_count: count_table(connection, SCENARIO_SPEC)?,
                scenario_run_count: count_table(connection, SCENARIO_RUN)?,
                scenario_result_count: count_table(connection, SCENARIO_RESULT)?,
            })
        })
    }

    fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut PostgresTransaction<'_>) -> MatrixStoreResult<T>,
    ) -> MatrixStoreResult<T> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let value = operation(&mut transaction)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(value)
    }

    fn data_plane_health(&self) -> MatrixStoreResult<MatrixDataPlaneHealth> {
        let health = self.health()?;
        Ok(MatrixSqliteDataPlane::new(health.data_plane_watermark_count).health())
    }

    fn resource_revision_for_existing(
        &self,
        resource_kind: &str,
        resource_id: &str,
    ) -> MatrixStoreResult<u64> {
        self.with_connection(|connection| {
            resource_revision(
                connection,
                resource_kind,
                &postgres_resource_revision_id(resource_kind, resource_id),
            )
        })
    }

    fn upsert_entity(&self, entity: &MatrixEntity) -> MatrixStoreResult<MatrixEntity> {
        Ok(self.upsert_entity_revisioned(entity, None, false)?.resource)
    }

    fn upsert_entity_checked(
        &self,
        entity: &MatrixEntity,
        expected_revision: Option<u64>,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixEntity>> {
        self.upsert_entity_revisioned(entity, expected_revision, true)
    }

    fn upsert_entity_revisioned(
        &self,
        entity: &MatrixEntity,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixEntity>> {
        self.with_transaction(|transaction| {
            let existing =
                find_entity_by_canonical(transaction, &entity.entity_type, &entity.canonical_key)?;
            let resource_id = existing
                .as_ref()
                .map(|value| value.entity_id.as_str())
                .unwrap_or(entity.entity_id.as_str());
            let (previous_revision, revision, created) = prepare_revision(
                transaction,
                "entity",
                resource_id,
                existing.is_some(),
                expected_revision,
                enforce_revision,
            )?;
            let resource = save_entity(transaction, entity, existing)?;
            persist_revision(transaction, "entity", &resource.entity_id, revision)?;
            Ok(MatrixRevisioned {
                resource,
                previous_revision,
                revision,
                created,
            })
        })
    }

    fn get_entity(&self, entity_id: &str) -> MatrixStoreResult<Option<MatrixEntity>> {
        self.with_connection(|connection| read_json(connection, ENTITY, entity_id))
    }

    fn resolve_entity_by_source_key(
        &self,
        source_system: &str,
        source_key: &str,
    ) -> MatrixStoreResult<Option<MatrixEntity>> {
        self.with_connection(|connection| {
            let row = connection
                .query_opt(
                    "SELECT entity_id FROM matrix_entity_source_key WHERE source_system = $1 AND source_key = $2",
                    &[
                        &matrix_core::normalize_key(source_system),
                        &matrix_core::normalize_key(source_key),
                    ],
                )
                .map_err(postgres_error)?;
            row.map_or(Ok(None), |row| read_json(connection, ENTITY, row.get::<_, String>(0).as_str()))
        })
    }

    fn list_entities(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixEntity>> {
        self.with_connection(|connection| list_json(connection, ENTITY, limit))
    }

    fn get_ontology_pack(
        &self,
        ontology_id: &str,
    ) -> MatrixStoreResult<Option<MatrixOntologyPack>> {
        self.with_connection(|connection| read_json(connection, ONTOLOGY_PACK, ontology_id))
    }

    fn propose_entity_match(
        &self,
        left_entity_id: &str,
        right_entity_id: &str,
    ) -> MatrixStoreResult<MatrixEntityMatchCandidate> {
        self.with_connection(|connection| {
            let left = read_json(connection, ENTITY, left_entity_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(left_entity_id.to_string()))?;
            let right = read_json(connection, ENTITY, right_entity_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(right_entity_id.to_string()))?;
            matrix_core::match_candidate(&left, &right).ok_or_else(|| {
                MatrixStoreError::NotFound(
                    "entity match candidate below confidence threshold".to_string(),
                )
            })
        })
    }

    fn decide_entity_conflict(
        &self,
        candidate_id: &str,
        survivor_entity_id: &str,
        retired_entity_id: &str,
        survivorship_rule: &str,
        notes: Option<String>,
    ) -> MatrixStoreResult<MatrixEntityConflictDecision> {
        self.with_transaction(|transaction| {
            let survivor = read_json(transaction, ENTITY, survivor_entity_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(survivor_entity_id.to_string()))?;
            let retired = read_json(transaction, ENTITY, retired_entity_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(retired_entity_id.to_string()))?;
            let candidate = match read_json(transaction, MATCH_CANDIDATE, candidate_id)? {
                Some(candidate) => candidate,
                None => {
                    let candidate =
                        matrix_core::match_candidate(&survivor, &retired).ok_or_else(|| {
                            MatrixStoreError::NotFound(
                                "entity match candidate below confidence threshold".to_string(),
                            )
                        })?;
                    if candidate.candidate_id != candidate_id {
                        return Err(MatrixStoreError::NotFound(candidate_id.to_string()));
                    }
                    write_json(
                        transaction,
                        MATCH_CANDIDATE,
                        &candidate.candidate_id,
                        &candidate,
                    )?;
                    candidate
                }
            };
            let pair_matches = (candidate.left_entity_id == survivor_entity_id
                && candidate.right_entity_id == retired_entity_id)
                || (candidate.left_entity_id == retired_entity_id
                    && candidate.right_entity_id == survivor_entity_id);
            if !pair_matches {
                return Err(MatrixStoreError::InvalidScenario(
                    "entity conflict decision does not match the candidate pair".to_string(),
                ));
            }
            let decision = MatrixEntityConflictDecision {
                decision_id: format!("entity-conflict-decision-{}", uuid::Uuid::new_v4()),
                candidate_id: candidate_id.to_string(),
                decision: "merge".to_string(),
                survivor_entity_id: survivor.entity_id,
                retired_entity_id: retired.entity_id,
                survivorship_rule: survivorship_rule.to_string(),
                notes,
                decision_metadata: serde_json::json!({
                    "source": "matrix.entity_governance",
                    "policy": survivorship_rule,
                }),
                decided_at: Utc::now(),
            };
            write_json(
                transaction,
                CONFLICT_DECISION,
                &decision.decision_id,
                &decision,
            )?;
            Ok(decision)
        })
    }

    fn upsert_relation(&self, relation: &MatrixRelation) -> MatrixStoreResult<MatrixRelation> {
        Ok(self
            .upsert_relation_revisioned(relation, None, false)?
            .resource)
    }

    fn upsert_relation_checked(
        &self,
        relation: &MatrixRelation,
        expected_revision: Option<u64>,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixRelation>> {
        self.upsert_relation_revisioned(relation, expected_revision, true)
    }

    fn upsert_relation_revisioned(
        &self,
        relation: &MatrixRelation,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixRelation>> {
        self.with_transaction(|transaction| {
            if read_json::<_, MatrixEntity>(transaction, ENTITY, &relation.from_entity_id)?
                .is_none()
            {
                return Err(MatrixStoreError::NotFound(relation.from_entity_id.clone()));
            }
            if read_json::<_, MatrixEntity>(transaction, ENTITY, &relation.to_entity_id)?.is_none()
            {
                return Err(MatrixStoreError::NotFound(relation.to_entity_id.clone()));
            }
            let existing = find_relation_by_key(
                transaction,
                &relation.relation_type,
                &relation.from_entity_id,
                &relation.to_entity_id,
            )?;
            let resource_id = existing
                .as_ref()
                .map(|value| value.relation_id.as_str())
                .unwrap_or(relation.relation_id.as_str());
            let (previous_revision, revision, created) = prepare_revision(
                transaction,
                "relation",
                resource_id,
                existing.is_some(),
                expected_revision,
                enforce_revision,
            )?;
            let resource = save_relation(transaction, relation, existing)?;
            persist_revision(transaction, "relation", &resource.relation_id, revision)?;
            Ok(MatrixRevisioned {
                resource,
                previous_revision,
                revision,
                created,
            })
        })
    }

    fn list_entity_relations(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> MatrixStoreResult<Vec<MatrixRelation>> {
        self.with_connection(|connection| {
            if read_json::<_, MatrixEntity>(connection, ENTITY, entity_id)?.is_none() {
                return Err(MatrixStoreError::NotFound(entity_id.to_string()));
            }
            let rows = connection
                .query(
                    "SELECT payload FROM matrix_relation WHERE payload->>'from_entity_id' = $1 OR payload->>'to_entity_id' = $1 ORDER BY updated_at DESC, id ASC LIMIT $2",
                    &[&entity_id, &(limit.clamp(1, 500) as i64)],
                )
                .map_err(postgres_error)?;
            rows.into_iter()
                .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
                .collect()
        })
    }

    fn impact_trace(
        &self,
        entity_id: &str,
        max_depth: usize,
    ) -> MatrixStoreResult<MatrixImpactTrace> {
        self.with_connection(|connection| build_impact_trace(connection, entity_id, max_depth))
    }

    fn register_metric_definition(
        &self,
        definition: &MatrixMetricDefinition,
    ) -> MatrixStoreResult<()> {
        definition
            .query_plan()
            .validate()
            .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
        self.with_connection(|connection| {
            write_json(
                connection,
                METRIC_DEFINITION,
                &definition.metric_id,
                definition,
            )
        })
    }

    fn upsert_metric_dependency(
        &self,
        dependency: &MatrixMetricDependency,
    ) -> MatrixStoreResult<MatrixMetricDependency> {
        Ok(self
            .upsert_metric_dependency_revisioned(dependency, None, false)?
            .resource)
    }

    fn upsert_metric_dependency_checked(
        &self,
        dependency: &MatrixMetricDependency,
        expected_revision: Option<u64>,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixMetricDependency>> {
        self.upsert_metric_dependency_revisioned(dependency, expected_revision, true)
    }

    fn upsert_metric_dependency_revisioned(
        &self,
        dependency: &MatrixMetricDependency,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixMetricDependency>> {
        self.with_transaction(|transaction| {
            let existing = find_metric_dependency_by_key(
                transaction,
                &dependency.upstream_metric_id,
                &dependency.downstream_metric_id,
                &dependency.dependency_type,
            )?;
            let resource_id = existing
                .as_ref()
                .map(|value| value.dependency_id.as_str())
                .unwrap_or(dependency.dependency_id.as_str());
            let (previous_revision, revision, created) = prepare_revision(
                transaction,
                "metric_dependency",
                resource_id,
                existing.is_some(),
                expected_revision,
                enforce_revision,
            )?;
            let resource = save_metric_dependency(transaction, dependency, existing)?;
            persist_revision(
                transaction,
                "metric_dependency",
                &resource.dependency_id,
                revision,
            )?;
            Ok(MatrixRevisioned {
                resource,
                previous_revision,
                revision,
                created,
            })
        })
    }

    fn metric_lineage(
        &self,
        metric_id: &str,
        max_depth: usize,
    ) -> MatrixStoreResult<MatrixMetricLineage> {
        self.with_connection(|connection| metric_lineage(connection, metric_id, max_depth))
    }

    fn metrics_affected_by_fact_type(&self, fact_type: &str) -> MatrixStoreResult<Vec<String>> {
        self.with_connection(|connection| metrics_affected_by_fact_type(connection, fact_type))
    }

    fn plan_metric_attention(
        &self,
        trigger_fact_type: &str,
        entity_scope: Option<String>,
        period: Option<String>,
        limit: usize,
    ) -> MatrixStoreResult<MatrixMetricAttentionPlan> {
        self.with_connection(|connection| {
            let mut metric_ids = metrics_affected_by_fact_type(connection, trigger_fact_type)?;
            metric_ids.extend(metric_ids_for_fact_type(connection, trigger_fact_type)?);
            metric_ids.sort();
            metric_ids.dedup();
            let limit = limit.clamp(1, 24);
            let mut scored_metrics = Vec::new();
            for metric_id in metric_ids {
                let definition = read_json(connection, METRIC_DEFINITION, &metric_id)?
                    .unwrap_or_else(|| {
                        MatrixMetricDefinition::inferred(metric_id.clone(), trigger_fact_type)
                    });
                let lineage = metric_lineage(connection, &metric_id, 6)?;
                let latest = latest_metric_state_for_metric(connection, &metric_id)?;
                scored_metrics.push(MatrixMetricAttentionScore::new(
                    metric_id,
                    definition.business_priority,
                    lineage.impacted_metric_ids.len() + lineage.upstream_dependencies.len(),
                    latest
                        .as_ref()
                        .map(|state| format!("{:?}", state.status).to_ascii_lowercase()),
                    latest.as_ref().map(|state| state.delta),
                ));
            }
            scored_metrics.sort_by(|left, right| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        right
                            .business_priority
                            .partial_cmp(&left.business_priority)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });
            scored_metrics.truncate(limit);
            let selected_metric_ids = scored_metrics
                .iter()
                .map(|item| item.metric_id.clone())
                .collect::<Vec<_>>();
            let compute_jobs = build_metric_compute_jobs(
                trigger_fact_type,
                &selected_metric_ids,
                entity_scope.clone(),
                period.clone(),
            );
            Ok(MatrixMetricAttentionPlan {
                plan_id: format!("metric-attention-plan-{}", uuid::Uuid::new_v4()),
                trigger_fact_type: trigger_fact_type.to_string(),
                entity_scope,
                period,
                limit,
                scored_metrics,
                selected_metric_ids,
                compute_jobs,
                generated_at: Utc::now(),
            })
        })
    }

    fn materialize_metric_snapshot(
        &self,
        mut metric_ids: Vec<String>,
        scope_ref: Option<String>,
    ) -> MatrixStoreResult<MatrixMetricSnapshot> {
        self.with_connection(|connection| {
            metric_ids.sort();
            metric_ids.dedup();
            let items = metric_ids
                .iter()
                .map(|metric_id| {
                    Ok(MatrixMetricSnapshotItem {
                        metric_id: metric_id.clone(),
                        state: latest_metric_state_for_metric(connection, metric_id)?,
                    })
                })
                .collect::<MatrixStoreResult<Vec<_>>>()?;
            let state_count = items.iter().filter(|item| item.state.is_some()).count();
            let snapshot = MatrixMetricSnapshot {
                snapshot_id: format!("metric-snapshot-{}", uuid::Uuid::new_v4()),
                scope_ref: scope_ref.unwrap_or_else(|| "global".to_string()),
                metric_ids,
                items,
                created_at: Utc::now(),
                summary: format!("metric states materialized: {state_count}"),
            };
            write_json(
                connection,
                METRIC_SNAPSHOT,
                &snapshot.snapshot_id,
                &snapshot,
            )?;
            Ok(snapshot)
        })
    }

    fn plan_compute_job_for_fact_type(
        &self,
        input: MatrixComputeJobInput,
    ) -> MatrixStoreResult<MatrixComputePlan> {
        self.with_connection(|connection| {
            let mut affected_metric_ids = if input.metric_ids.is_empty() {
                metrics_affected_by_fact_type(connection, &input.trigger_fact_type)?
            } else {
                input.metric_ids.clone()
            };
            if affected_metric_ids.is_empty() {
                affected_metric_ids =
                    metric_ids_for_fact_type(connection, &input.trigger_fact_type)?;
            }
            affected_metric_ids.sort();
            affected_metric_ids.dedup();
            let mut job = MatrixComputeJob::from_input(MatrixComputeJobInput {
                metric_ids: affected_metric_ids.clone(),
                ..input
            });
            job.priority = compute_priority(&job);
            write_json(connection, COMPUTE_JOB, &job.job_id, &job)?;
            Ok(MatrixComputePlan {
                job,
                affected_metric_ids,
                planned_at: Utc::now(),
            })
        })
    }

    fn get_compute_job(&self, job_id: &str) -> MatrixStoreResult<Option<MatrixComputeJob>> {
        self.with_connection(|connection| read_json(connection, COMPUTE_JOB, job_id))
    }

    fn run_compute_job(&self, job_id: &str) -> MatrixStoreResult<MatrixComputeJob> {
        let mut job = self.with_connection(|connection| {
            let mut job: MatrixComputeJob = read_json(connection, COMPUTE_JOB, job_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(job_id.to_string()))?;
            job.status = "running".to_string();
            job.attempts = job.attempts.saturating_add(1);
            job.updated_at = Utc::now();
            write_json(connection, COMPUTE_JOB, &job.job_id, &job)?;
            Ok(job)
        })?;
        let recompute = self.recompute_metrics_for_metric_ids(&job.metric_ids)?;
        job.status = "completed".to_string();
        job.result_summary = serde_json::json!({
            "metric_ids": job.metric_ids,
            "metric_state_count": recompute.metric_state_count,
            "change_count": recompute.change_count,
            "attention_count": recompute.attention_count,
        });
        job.updated_at = Utc::now();
        self.with_connection(|connection| {
            write_json(connection, COMPUTE_JOB, &job.job_id, &job)?;
            Ok(job)
        })
    }

    fn ingest_fact(&self, fact: &MatrixFact) -> MatrixStoreResult<MatrixAttentionItem> {
        self.with_connection(|connection| {
            write_json(connection, FACT, &fact.fact_id, fact)?;
            let attention = MatrixAttentionItem::from_fact(
                &fact.fact_id,
                &fact.fact_type,
                fact.entity_refs.first().cloned(),
                fact.confidence,
            );
            write_json(connection, ATTENTION, &attention.attention_id, &attention)?;
            Ok(attention)
        })
    }

    fn list_attention(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixAttentionItem>> {
        self.with_connection(|connection| {
            list_json_ordered(
                connection,
                ATTENTION,
                "((payload->>'priority_score')::double precision) DESC, updated_at DESC, id ASC",
                limit,
            )
        })
    }

    fn list_facts(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixFact>> {
        self.with_connection(|connection| {
            list_json_ordered(
                connection,
                FACT,
                "(payload->>'event_time') DESC, id ASC",
                limit,
            )
        })
    }

    fn recompute_metrics(&self) -> MatrixStoreResult<MatrixMetricRecomputeResult> {
        self.recompute_metrics_with_filter(None)
    }

    fn recompute_metrics_for_metric_ids(
        &self,
        metric_ids: &[String],
    ) -> MatrixStoreResult<MatrixMetricRecomputeResult> {
        self.recompute_metrics_with_filter(Some(metric_ids.iter().cloned().collect()))
    }

    fn recompute_metrics_with_filter(
        &self,
        metric_filter: Option<BTreeSet<String>>,
    ) -> MatrixStoreResult<MatrixMetricRecomputeResult> {
        self.with_connection(|connection| recompute_metrics(connection, metric_filter.as_ref()))
    }

    fn list_metric_definitions(&self) -> MatrixStoreResult<Vec<MatrixMetricDefinition>> {
        self.with_connection(|connection| list_json(connection, METRIC_DEFINITION, 500))
    }

    fn metric_states(&self, metric_id: &str) -> MatrixStoreResult<Vec<MatrixMetricState>> {
        self.with_connection(|connection| states_for_metric(connection, metric_id))
    }

    fn list_changes(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixChangeEvent>> {
        self.with_connection(|connection| {
            list_json_ordered(
                connection,
                CHANGE,
                "(payload->>'detected_at') DESC, id ASC",
                limit,
            )
        })
    }

    fn plan_data_plane_ingest(
        &self,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> MatrixStoreResult<MatrixDataPlaneIngestPlan> {
        self.with_connection(|connection| {
            let source_ref = input.source_ref.clone();
            let mut plan =
                MatrixSqliteDataPlane::new(count_table(connection, WATERMARK)?).plan_ingest(input);
            if plan.affected_metric_ids.is_empty() {
                let mut affected = metrics_affected_by_fact_type(connection, &plan.fact_type)?;
                affected.extend(metric_ids_for_fact_type(connection, &plan.fact_type)?);
                if let Some(source_pack_id) = source_ref
                    .strip_prefix("source-pack://")
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                {
                    if let Some(source_pack) =
                        read_json::<_, MatrixSourcePack>(connection, SOURCE_PACK, source_pack_id)?
                    {
                        affected.extend(
                            source_pack
                                .fact_mappings
                                .iter()
                                .filter(|mapping| mapping.fact_type == plan.fact_type)
                                .map(|mapping| mapping.metric_key.clone()),
                        );
                    }
                }
                affected.sort();
                affected.dedup();
                plan.compute_jobs = affected
                    .iter()
                    .map(|metric_id| MatrixComputeJobInput {
                        job_id: Some(format!("compute-job-{}-{metric_id}", plan.batch_id)),
                        trigger_fact_type: plan.fact_type.clone(),
                        trigger_fact_refs: vec![format!(
                            "matrix:data-plane-batch:{}",
                            plan.batch_id
                        )],
                        entity_scope: None,
                        period: Some(plan.partition_ref.clone()),
                        metric_ids: vec![metric_id.clone()],
                        priority: Some(0.72),
                    })
                    .collect();
                plan.affected_metric_ids = affected;
            }
            Ok(plan)
        })
    }

    fn commit_data_plane_ingest(
        &self,
        plan: &MatrixDataPlaneIngestPlan,
    ) -> MatrixStoreResult<MatrixDataPlaneWatermark> {
        self.with_transaction(|transaction| {
            let resource_id = watermark_resource_id(&plan.watermark);
            let existing =
                read_json::<_, MatrixDataPlaneWatermark>(transaction, WATERMARK, &resource_id)?;
            let (_, revision, _) = prepare_revision(
                transaction,
                "data_plane_watermark",
                &resource_id,
                existing.is_some(),
                plan.expected_revision,
                true,
            )?;
            let mut watermark = plan.watermark.clone();
            watermark.revision = revision;
            write_json(transaction, WATERMARK, &resource_id, &watermark)?;
            persist_revision(transaction, "data_plane_watermark", &resource_id, revision)?;
            Ok(watermark)
        })
    }

    fn upsert_source_pack(
        &self,
        source_pack: MatrixSourcePack,
    ) -> MatrixStoreResult<MatrixSourcePack> {
        Ok(self
            .upsert_source_pack_revisioned(source_pack, None, false)?
            .resource)
    }

    fn upsert_source_pack_checked(
        &self,
        source_pack: MatrixSourcePack,
        expected_revision: Option<u64>,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixSourcePack>> {
        self.upsert_source_pack_revisioned(source_pack, expected_revision, true)
    }

    fn upsert_source_pack_revisioned(
        &self,
        source_pack: MatrixSourcePack,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> MatrixStoreResult<MatrixRevisioned<MatrixSourcePack>> {
        self.with_transaction(|transaction| {
            let mut source_pack = source_pack.normalized();
            let existing = read_json::<_, MatrixSourcePack>(
                transaction,
                SOURCE_PACK,
                &source_pack.source_pack_id,
            )?;
            if let Some(existing) = &existing {
                source_pack.created_at = existing.created_at;
            }
            let (previous_revision, revision, created) = prepare_revision(
                transaction,
                "source_pack",
                &source_pack.source_pack_id,
                existing.is_some(),
                expected_revision,
                enforce_revision,
            )?;
            write_json(
                transaction,
                SOURCE_PACK,
                &source_pack.source_pack_id,
                &source_pack,
            )?;
            persist_revision(
                transaction,
                "source_pack",
                &source_pack.source_pack_id,
                revision,
            )?;
            Ok(MatrixRevisioned {
                resource: source_pack,
                previous_revision,
                revision,
                created,
            })
        })
    }

    fn get_source_pack(&self, source_pack_id: &str) -> MatrixStoreResult<Option<MatrixSourcePack>> {
        self.with_connection(|connection| read_json(connection, SOURCE_PACK, source_pack_id))
    }

    fn list_source_packs(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixSourcePack>> {
        self.with_connection(|connection| list_json(connection, SOURCE_PACK, limit))
    }

    fn validate_source_pack(
        &self,
        source_pack_id: &str,
    ) -> MatrixStoreResult<MatrixSourcePackValidation> {
        self.with_connection(|connection| {
            read_json::<_, MatrixSourcePack>(connection, SOURCE_PACK, source_pack_id)?
                .map(|source_pack| source_pack.validate())
                .ok_or_else(|| MatrixStoreError::NotFound(source_pack_id.to_string()))
        })
    }

    fn source_pack_delta_plan(
        &self,
        source_pack_id: &str,
    ) -> MatrixStoreResult<MatrixSourceDeltaPlan> {
        self.with_connection(|connection| {
            let source_pack = read_json(connection, SOURCE_PACK, source_pack_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(source_pack_id.to_string()))?;
            source_pack_delta_plan(connection, &source_pack)
        })
    }

    fn plan_connector_run(
        &self,
        source_pack_id: &str,
        input: MatrixConnectorRunInput,
    ) -> MatrixStoreResult<MatrixConnectorRun> {
        self.with_connection(|connection| {
            let source_pack = read_json(connection, SOURCE_PACK, source_pack_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(source_pack_id.to_string()))?;
            let delta_plan = source_pack_delta_plan(connection, &source_pack)?;
            let run = MatrixConnectorRun::from_source_pack(&source_pack, &delta_plan, input);
            write_json(connection, CONNECTOR_RUN, &run.run_id, &run)?;
            Ok(run)
        })
    }

    fn get_connector_run(&self, run_id: &str) -> MatrixStoreResult<Option<MatrixConnectorRun>> {
        self.with_connection(|connection| read_json(connection, CONNECTOR_RUN, run_id))
    }

    fn plan_source_snapshot(
        &self,
        source_pack_id: &str,
        resource_ref: Option<String>,
        estimated_rows: Option<u64>,
    ) -> MatrixStoreResult<MatrixSourceSnapshotPlan> {
        self.with_connection(|connection| {
            let source_pack = read_json(connection, SOURCE_PACK, source_pack_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(source_pack_id.to_string()))?;
            let delta_plan = source_pack_delta_plan(connection, &source_pack)?;
            let quality_warnings = source_pack.validate().warnings;
            Ok(MatrixSourceSnapshotPlan {
                source_pack_id: source_pack.source_pack_id.clone(),
                source_ref: resource_ref.unwrap_or_else(|| source_pack.source_name.clone()),
                source_kind: source_kind_for_access_mode(&source_pack.access_mode),
                access_mode: source_pack.access_mode,
                refresh_mode: source_pack.refresh_mode,
                estimated_rows: estimated_rows.unwrap_or(0),
                fact_types: delta_plan.fact_types,
                affected_metric_ids: delta_plan.affected_metric_ids,
                quality_warnings,
                planned_at: Utc::now(),
            })
        })
    }

    fn upsert_source_snapshot(
        &self,
        snapshot: MatrixSourceSnapshot,
    ) -> MatrixStoreResult<MatrixSourceSnapshot> {
        self.with_connection(|connection| {
            write_json(
                connection,
                SOURCE_SNAPSHOT,
                &snapshot.snapshot_id,
                &snapshot,
            )?;
            Ok(snapshot)
        })
    }

    fn create_source_snapshot(
        &self,
        input: MatrixSourceSnapshotInput,
    ) -> MatrixStoreResult<MatrixSourceSnapshot> {
        self.upsert_source_snapshot(MatrixSourceSnapshot::from_input(input))
    }

    fn get_source_snapshot(
        &self,
        snapshot_id: &str,
    ) -> MatrixStoreResult<Option<MatrixSourceSnapshot>> {
        self.with_connection(|connection| read_json(connection, SOURCE_SNAPSHOT, snapshot_id))
    }

    fn list_source_snapshots(
        &self,
        source_pack_id: Option<&str>,
        limit: usize,
    ) -> MatrixStoreResult<Vec<MatrixSourceSnapshot>> {
        self.with_connection(|connection| {
            let limit = limit.clamp(1, 500) as i64;
            let rows = if let Some(source_pack_id) = source_pack_id {
                connection
                    .query(
                        "SELECT payload FROM matrix_source_snapshot WHERE payload->>'source_pack_id' = $1 ORDER BY (payload->>'captured_at') DESC, id ASC LIMIT $2",
                        &[&source_pack_id, &limit],
                    )
                    .map_err(postgres_error)?
            } else {
                connection
                    .query(
                        "SELECT payload FROM matrix_source_snapshot ORDER BY (payload->>'captured_at') DESC, id ASC LIMIT $1",
                        &[&limit],
                    )
                    .map_err(postgres_error)?
            };
            rows.into_iter()
                .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
                .collect()
        })
    }

    fn create_scenario_spec(
        &self,
        spec: MatrixScenarioSpec,
    ) -> MatrixStoreResult<MatrixScenarioSpec> {
        self.with_connection(|connection| {
            spec.validate().map_err(MatrixStoreError::InvalidScenario)?;
            if read_json::<_, MatrixSourceSnapshot>(
                connection,
                SOURCE_SNAPSHOT,
                &spec.base_snapshot.snapshot_id,
            )?
            .is_none()
            {
                return Err(MatrixStoreError::NotFound(format!(
                    "source snapshot for scenario: {}",
                    spec.base_snapshot.snapshot_id
                )));
            }
            write_json(connection, SCENARIO_SPEC, &spec.scenario_id, &spec)?;
            Ok(spec)
        })
    }

    fn get_scenario_spec(
        &self,
        scenario_id: &str,
    ) -> MatrixStoreResult<Option<MatrixScenarioSpec>> {
        self.with_connection(|connection| read_json(connection, SCENARIO_SPEC, scenario_id))
    }

    fn list_scenario_specs(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixScenarioSpec>> {
        self.with_connection(|connection| list_json(connection, SCENARIO_SPEC, limit))
    }

    fn start_scenario_run(
        &self,
        scenario_id: &str,
        parameters: Value,
    ) -> MatrixStoreResult<MatrixScenarioRun> {
        self.with_connection(|connection| {
            let spec = read_json::<_, MatrixScenarioSpec>(connection, SCENARIO_SPEC, scenario_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(scenario_id.to_string()))?;
            let run = MatrixScenarioRun::start(&spec, parameters);
            run.validate().map_err(MatrixStoreError::InvalidScenario)?;
            write_json(connection, SCENARIO_RUN, &run.run_id, &run)?;
            Ok(run)
        })
    }

    fn get_scenario_run(&self, run_id: &str) -> MatrixStoreResult<Option<MatrixScenarioRun>> {
        self.with_connection(|connection| read_json(connection, SCENARIO_RUN, run_id))
    }

    fn list_scenario_runs(
        &self,
        scenario_id: Option<&str>,
        limit: usize,
    ) -> MatrixStoreResult<Vec<MatrixScenarioRun>> {
        self.with_connection(|connection| {
            let limit = limit.clamp(1, 500) as i64;
            let rows = if let Some(scenario_id) = scenario_id {
                connection
                    .query(
                        "SELECT payload FROM matrix_scenario_run WHERE payload->>'scenario_id' = $1 ORDER BY (payload->>'started_at') DESC, id ASC LIMIT $2",
                        &[&scenario_id, &limit],
                    )
                    .map_err(postgres_error)?
            } else {
                connection
                    .query(
                        "SELECT payload FROM matrix_scenario_run ORDER BY (payload->>'started_at') DESC, id ASC LIMIT $1",
                        &[&limit],
                    )
                    .map_err(postgres_error)?
            };
            rows.into_iter()
                .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
                .collect()
        })
    }

    fn complete_scenario_run(
        &self,
        result: MatrixScenarioResult,
    ) -> MatrixStoreResult<MatrixScenarioResult> {
        self.with_transaction(|transaction| {
            let mut run =
                read_json::<_, MatrixScenarioRun>(transaction, SCENARIO_RUN, &result.run_id)?
                    .ok_or_else(|| MatrixStoreError::NotFound(result.run_id.clone()))?;
            if run.status != MatrixScenarioRunStatus::Running {
                return Err(MatrixStoreError::ScenarioState(format!(
                    "scenario run is not running: {}",
                    run.run_id
                )));
            }
            result
                .validate_for_run(&run)
                .map_err(MatrixStoreError::InvalidScenario)?;
            if read_scenario_result_for_run(transaction, &result.run_id)?.is_some() {
                return Err(MatrixStoreError::ScenarioState(format!(
                    "scenario run already has a result: {}",
                    result.run_id
                )));
            }
            run.status = MatrixScenarioRunStatus::Succeeded;
            run.completed_at = Some(result.completed_at);
            write_json(transaction, SCENARIO_RUN, &run.run_id, &run)?;
            write_json(transaction, SCENARIO_RESULT, &result.result_id, &result)?;
            Ok(result)
        })
    }

    fn get_scenario_result(&self, run_id: &str) -> MatrixStoreResult<Option<MatrixScenarioResult>> {
        self.with_connection(|connection| read_scenario_result_for_run(connection, run_id))
    }

    fn build_evidence_packet(
        &self,
        packet_id: Option<&str>,
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> MatrixStoreResult<MatrixEvidencePacket> {
        self.with_connection(|connection| {
            if let Some(packet_id) = packet_id {
                if let Some(existing) =
                    read_json::<_, MatrixEvidencePacket>(connection, EVIDENCE, packet_id)?
                {
                    return Ok(existing);
                }
            }
            let attention = match attention_id {
                Some(attention_id) => {
                    read_json::<_, MatrixAttentionItem>(connection, ATTENTION, attention_id)?
                        .ok_or_else(|| MatrixStoreError::NotFound(attention_id.to_string()))
                        .map(Some)?
                }
                None => list_json::<_, MatrixAttentionItem>(connection, ATTENTION, 1)?
                    .into_iter()
                    .next(),
            };
            let mut packet = MatrixEvidencePacket::new(problem_statement.unwrap_or_else(|| {
                attention
                    .as_ref()
                    .map(|item| item.title.as_str())
                    .unwrap_or("MATRIX operational evidence packet")
            }));
            if let Some(packet_id) = packet_id {
                packet.packet_id = packet_id.to_string();
            }
            packet.attention_id = attention.as_ref().map(|item| item.attention_id.clone());
            if let Some(attention) = attention {
                packet.confidence = attention.confidence.min(0.75);
                packet.business_context = serde_json::json!({
                    "business_domain": attention.business_domain,
                    "entity_ref": attention.entity_ref,
                    "period": attention.period,
                    "priority_score": attention.priority_score,
                    "reason_codes": attention.reason_codes,
                    "owner_roles": attention.owner_roles,
                });
                for reference in attention.linked_changes {
                    if let Some(change_id) = reference.strip_prefix("matrix:change:") {
                        if let Some(change) =
                            read_json::<_, MatrixChangeEvent>(connection, CHANGE, change_id)?
                        {
                            packet
                                .change_evidence
                                .push(serde_json::to_value(&change).map_err(json_error)?);
                            if let Some(metric_id) = change.metric_id.as_deref() {
                                if let Some(state) =
                                    latest_metric_state_for_metric(connection, metric_id)?
                                {
                                    packet
                                        .metric_evidence
                                        .push(serde_json::to_value(state).map_err(json_error)?);
                                }
                            }
                        }
                    }
                    packet.source_refs.push(MatrixEvidenceSourceRef {
                        kind: "change_or_fact".to_string(),
                        reference,
                        summary: "MATRIX attention evidence source".to_string(),
                    });
                }
                if !packet.metric_evidence.is_empty() {
                    packet
                        .missing_evidence
                        .retain(|item| !item.contains("metric_network"));
                    packet.confidence = packet.confidence.max(0.65);
                }
            }
            write_json_once(connection, EVIDENCE, &packet.packet_id, &packet)?;
            read_json(connection, EVIDENCE, &packet.packet_id)?.ok_or_else(|| {
                MatrixStoreError::NotFound(format!(
                    "canonical evidence packet {} disappeared after insert",
                    packet.packet_id
                ))
            })
        })
    }

    fn insert_ai_harness_evidence_packet(
        &self,
        packet: &MatrixEvidencePacket,
    ) -> MatrixStoreResult<MatrixEvidencePacket> {
        self.with_connection(|connection| {
            write_json(connection, EVIDENCE, &packet.packet_id, packet)?;
            Ok(packet.clone())
        })
    }

    fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> MatrixStoreResult<Option<MatrixEvidencePacket>> {
        self.with_connection(|connection| read_json(connection, EVIDENCE, packet_id))
    }

    fn list_evidence_packets(&self, limit: usize) -> MatrixStoreResult<Vec<MatrixEvidencePacket>> {
        self.with_connection(|connection| list_json(connection, EVIDENCE, limit))
    }

    fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> MatrixStoreResult<MatrixQualityGateDecision> {
        self.with_connection(|connection| {
            let packet = read_json::<_, MatrixEvidencePacket>(connection, EVIDENCE, packet_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(packet_id.to_string()))?;
            let decision = MatrixQualityGateDecision::for_evidence_packet(&packet);
            write_json(connection, QUALITY_GATE, &decision.gate_id, &decision)?;
            Ok(decision)
        })
    }

    fn evaluate_evidence_quality_with_gate_id(
        &self,
        packet_id: &str,
        gate_id: &str,
    ) -> MatrixStoreResult<MatrixQualityGateDecision> {
        self.with_transaction(|transaction| {
            if let Some(existing) =
                read_json::<_, MatrixQualityGateDecision>(transaction, QUALITY_GATE, gate_id)?
            {
                if existing.target_ref == format!("matrix:evidence:{packet_id}") {
                    return Ok(existing);
                }
                return Err(MatrixStoreError::InvalidScenario(format!(
                    "quality gate id {gate_id} is bound to another evidence packet"
                )));
            }
            let packet = read_json::<_, MatrixEvidencePacket>(transaction, EVIDENCE, packet_id)?
                .ok_or_else(|| MatrixStoreError::NotFound(packet_id.to_string()))?;
            let mut decision = MatrixQualityGateDecision::for_evidence_packet(&packet);
            decision.gate_id = gate_id.to_string();
            write_json(transaction, QUALITY_GATE, gate_id, &decision)?;
            Ok(decision)
        })
    }

    fn get_quality_gate(
        &self,
        gate_id: &str,
    ) -> MatrixStoreResult<Option<MatrixQualityGateDecision>> {
        self.with_connection(|connection| read_json(connection, QUALITY_GATE, gate_id))
    }

    fn list_data_plane_watermarks(
        &self,
        limit: usize,
    ) -> MatrixStoreResult<Vec<MatrixDataPlaneWatermark>> {
        self.with_connection(|connection| list_json(connection, WATERMARK, limit))
    }

    fn get_data_plane_watermark(
        &self,
        source_ref: &str,
        fact_type: &str,
        partition_ref: &str,
    ) -> MatrixStoreResult<Option<MatrixDataPlaneWatermark>> {
        self.with_connection(|connection| {
            let logical_id = format!("{source_ref}\0{fact_type}\0{partition_ref}");
            let id = postgres_resource_revision_id("data_plane_watermark", &logical_id);
            let Some(mut watermark) =
                read_json::<_, MatrixDataPlaneWatermark>(connection, WATERMARK, &id)?
            else {
                return Ok(None);
            };
            watermark.revision = resource_revision(connection, "data_plane_watermark", &id)?;
            Ok(Some(watermark))
        })
    }

    fn apply_source_snapshot_rows(
        &self,
        source_pack_id: &str,
        snapshot: MatrixSourceSnapshot,
        rows: &[Value],
    ) -> MatrixStoreResult<MatrixSourceSnapshotApplyReport> {
        self.with_transaction(|transaction| {
            let source_pack =
                read_json::<_, MatrixSourcePack>(transaction, SOURCE_PACK, source_pack_id)?
                    .ok_or_else(|| MatrixStoreError::NotFound(source_pack_id.to_string()))?;
            write_json(
                transaction,
                SOURCE_SNAPSHOT,
                &snapshot.snapshot_id,
                &snapshot,
            )?;
            let mut attention_count = 0usize;
            let mut relation_count = 0usize;
            let mut fact_refs = Vec::new();
            let mut warnings = BTreeSet::new();
            for row in rows {
                let row_hash = stable_json_hash(row);
                for mapping in &source_pack.entity_mappings {
                    let Some(source_key) = row_value(row, &mapping.source_key_field) else {
                        continue;
                    };
                    let entity = MatrixEntity::from_input(matrix_core::MatrixEntityInput {
                        entity_id: Some(stable_entity_id(
                            &source_pack.source_name,
                            &mapping.matrix_entity_type,
                            &source_key,
                        )),
                        entity_type: mapping.matrix_entity_type.clone(),
                        canonical_key: source_key.clone(),
                        display_name: Some(source_key.clone()),
                        source_keys: vec![matrix_core::MatrixSourceKey {
                            source_system: source_pack.source_name.clone(),
                            source_key,
                            source_ref: Some(format!("{}/row/{row_hash}", snapshot.reference())),
                        }],
                        attributes: row.clone(),
                        confidence: Some(snapshot.confidence),
                    });
                    let existing = find_entity_by_canonical(
                        transaction,
                        &entity.entity_type,
                        &entity.canonical_key,
                    )?;
                    save_entity(transaction, &entity, existing)?;
                }
                for mapping in &source_pack.fact_mappings {
                    let entity_refs = mapping
                        .entity_ref_fields
                        .iter()
                        .filter_map(|field| {
                            row_value(row, field).map(|source_key| {
                                stable_entity_reference_for_field(&source_pack, field, &source_key)
                            })
                        })
                        .collect::<Vec<_>>();
                    let dimensions = omit_fields(
                        row,
                        &mapping
                            .measure_fields
                            .iter()
                            .chain(std::iter::once(&mapping.dedup_key))
                            .cloned()
                            .collect::<Vec<_>>(),
                    );
                    let fact = MatrixFact::from_input(matrix_core::MatrixFactInput {
                        fact_id: Some(stable_fact_id(
                            &snapshot.snapshot_id,
                            &mapping.fact_type,
                            row_value(row, &mapping.dedup_key)
                                .as_deref()
                                .unwrap_or(&row_hash),
                        )),
                        snapshot_id: Some(snapshot.snapshot_id.clone()),
                        fact_type: mapping.fact_type.clone(),
                        entity_refs,
                        metric_key: Some(mapping.metric_key.clone()),
                        dimensions,
                        measures: pick_fields(row, &mapping.measure_fields),
                        event_time: mapping
                            .event_time_field
                            .as_deref()
                            .and_then(|field| row_value(row, field))
                            .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
                            .map(|value| value.with_timezone(&Utc))
                            .or(Some(snapshot.captured_at)),
                        valid_from: None,
                        valid_to: None,
                        source_ref: Some(format!("{}/row/{row_hash}", snapshot.reference())),
                        confidence: Some(snapshot.confidence),
                        raw_hash: Some(stable_json_hash(&serde_json::json!({
                            "row": row,
                            "mapping": mapping,
                            "snapshot": snapshot.snapshot_id,
                        }))),
                    });
                    let mut attention = MatrixAttentionItem::from_fact(
                        &fact.fact_id,
                        &fact.fact_type,
                        fact.entity_refs.first().cloned(),
                        fact.confidence,
                    );
                    attention.attention_id =
                        stable_attention_id("source_snapshot_apply", &fact.fact_id);
                    write_json(transaction, FACT, &fact.fact_id, &fact)?;
                    write_json(transaction, ATTENTION, &attention.attention_id, &attention)?;
                    attention_count += 1;
                    fact_refs.push(format!("matrix:fact:{}", fact.fact_id));
                }
                for mapping in &source_pack.relation_mappings {
                    let Some(from_key) = row_value(row, &mapping.from_source_key_field) else {
                        warnings.insert(format!(
                            "relation_mapping_missing_from_field:{}",
                            mapping.from_source_key_field
                        ));
                        continue;
                    };
                    let Some(to_key) = row_value(row, &mapping.to_source_key_field) else {
                        warnings.insert(format!(
                            "relation_mapping_missing_to_field:{}",
                            mapping.to_source_key_field
                        ));
                        continue;
                    };
                    let Some(from_entity_id) = stable_entity_id_for_field(
                        &source_pack,
                        &mapping.from_source_key_field,
                        &from_key,
                    ) else {
                        warnings.insert(format!(
                            "relation_mapping_missing_entity_mapping:{}",
                            mapping.from_source_key_field
                        ));
                        continue;
                    };
                    let Some(to_entity_id) = stable_entity_id_for_field(
                        &source_pack,
                        &mapping.to_source_key_field,
                        &to_key,
                    ) else {
                        warnings.insert(format!(
                            "relation_mapping_missing_entity_mapping:{}",
                            mapping.to_source_key_field
                        ));
                        continue;
                    };
                    let relation = MatrixRelation::from_input(matrix_core::MatrixRelationInput {
                        relation_id: Some(stable_relation_id(
                            &snapshot.snapshot_id,
                            &mapping.relation_type,
                            &from_entity_id,
                            &to_entity_id,
                            row_value(row, &mapping.dedup_key)
                                .as_deref()
                                .unwrap_or(&row_hash),
                        )),
                        relation_type: mapping.relation_type.clone(),
                        from_entity_id,
                        to_entity_id,
                        attributes: pick_fields(row, &mapping.attribute_fields),
                        confidence: Some(snapshot.confidence),
                    });
                    let existing = find_relation_by_key(
                        transaction,
                        &relation.relation_type,
                        &relation.from_entity_id,
                        &relation.to_entity_id,
                    )?;
                    save_relation(transaction, &relation, existing)?;
                    relation_count += 1;
                }
            }
            if source_pack.fact_mappings.is_empty() {
                warnings.insert("source_pack_has_no_fact_mappings".to_string());
            }
            Ok(MatrixSourceSnapshotApplyReport {
                snapshot_id: snapshot.snapshot_id,
                source_pack_id: source_pack_id.to_string(),
                status: "applied".to_string(),
                row_count: rows.len() as u64,
                fact_count: fact_refs.len(),
                relation_count,
                attention_count,
                warnings: warnings.into_iter().collect(),
                fact_refs,
                applied_at: Utc::now(),
            })
        })
    }
}

macro_rules! delegate_postgres_matrix_store_operations {
    ($($method:ident($($argument:ident: $argument_type:ty),*) -> $output:ty;)*) => {
        $(
            fn $method(&self, $($argument: $argument_type),*) -> MatrixStoreResult<$output> {
                PostgresMatrixRepository::$method(self, $($argument),*)
            }
        )*
    };
}

// This expansion is intentionally shared with the SQLite adapter: PostgreSQL
// cannot be selected unless it implements every typed Matrix operation.
impl MatrixStore for PostgresMatrixRepository {
    matrix_store_operations!(delegate_postgres_matrix_store_operations);
}

fn storage_error(error: storage::StorageError) -> MatrixStoreError {
    MatrixStoreError::Backend(error.to_string())
}

fn postgres_error(error: postgres::Error) -> MatrixStoreError {
    let detail = error.as_db_error().map_or_else(
        || error.to_string(),
        |database_error| format!("{:?}: {}", database_error.code(), database_error.message()),
    );
    MatrixStoreError::Backend(format!("postgres: {detail}"))
}

fn json_error(error: serde_json::Error) -> MatrixStoreError {
    MatrixStoreError::Backend(error.to_string())
}

fn scalar_i64<C: PostgresClient>(client: &mut C, sql: &str) -> MatrixStoreResult<i64> {
    client
        .query_one(sql, &[])
        .map(|row| row.get(0))
        .map_err(postgres_error)
}

fn count_table<C: PostgresClient>(client: &mut C, table: &str) -> MatrixStoreResult<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    client
        .query_one(&sql, &[])
        .map(|row| row.get::<_, i64>(0) as u64)
        .map_err(postgres_error)
}

fn write_json<C: PostgresClient, T: Serialize>(
    client: &mut C,
    table: &str,
    id: &str,
    value: &T,
) -> MatrixStoreResult<()> {
    let payload = serde_json::to_value(value).map_err(json_error)?;
    let sql = format!(
        "INSERT INTO {table}(id, payload, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) \
         ON CONFLICT(id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at"
    );
    client
        .execute(&sql, &[&id, &payload])
        .map_err(postgres_error)?;
    Ok(())
}

fn write_json_once<C: PostgresClient, T: Serialize>(
    client: &mut C,
    table: &str,
    id: &str,
    value: &T,
) -> MatrixStoreResult<()> {
    let payload = serde_json::to_value(value).map_err(json_error)?;
    let sql = format!(
        "INSERT INTO {table}(id, payload, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) \
         ON CONFLICT(id) DO NOTHING"
    );
    client
        .execute(&sql, &[&id, &payload])
        .map_err(postgres_error)?;
    Ok(())
}

fn read_json<C: PostgresClient, T: DeserializeOwned>(
    client: &mut C,
    table: &str,
    id: &str,
) -> MatrixStoreResult<Option<T>> {
    let sql = format!("SELECT payload FROM {table} WHERE id = $1");
    client
        .query_opt(&sql, &[&id])
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn list_json<C: PostgresClient, T: DeserializeOwned>(
    client: &mut C,
    table: &str,
    limit: usize,
) -> MatrixStoreResult<Vec<T>> {
    let sql = format!("SELECT payload FROM {table} ORDER BY updated_at DESC, id ASC LIMIT $1");
    client
        .query(&sql, &[&(limit.clamp(1, 500) as i64)])
        .map_err(postgres_error)?
        .into_iter()
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .collect()
}

fn list_json_ordered<C: PostgresClient, T: DeserializeOwned>(
    client: &mut C,
    table: &str,
    order_by: &str,
    limit: usize,
) -> MatrixStoreResult<Vec<T>> {
    let sql = format!("SELECT payload FROM {table} ORDER BY {order_by} LIMIT $1");
    client
        .query(&sql, &[&(limit.clamp(1, 500) as i64)])
        .map_err(postgres_error)?
        .into_iter()
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .collect()
}

fn export_json_records<C: PostgresClient>(
    client: &mut C,
    table: &str,
) -> MatrixStoreResult<BTreeMap<String, Value>> {
    let sql = format!("SELECT id, payload FROM {table} ORDER BY id ASC");
    let rows = client.query(&sql, &[]).map_err(postgres_error)?;
    rows.into_iter()
        .map(|row| {
            let id = row.get::<_, String>(0);
            let payload = canonicalize_payload(table, row.get::<_, Value>(1))?;
            Ok((id, payload))
        })
        .collect()
}

fn export_migration_json_records<C: PostgresClient>(
    client: &mut C,
    table: &str,
) -> MatrixStoreResult<BTreeMap<String, Value>> {
    let records = export_json_records(client, table)?;
    if table != WATERMARK {
        return Ok(records);
    }
    records
        .into_values()
        .map(|payload| {
            let watermark = serde_json::from_value::<MatrixDataPlaneWatermark>(payload.clone())
                .map_err(json_error)?;
            Ok((logical_watermark_resource_id(&watermark), payload))
        })
        .collect()
}

fn export_revisions<C: PostgresClient>(client: &mut C) -> MatrixStoreResult<BTreeMap<String, u64>> {
    let rows = client
        .query(
            "SELECT resource_kind, resource_id, revision \
             FROM matrix_resource_revision \
             ORDER BY resource_kind ASC, resource_id ASC",
            &[],
        )
        .map_err(postgres_error)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let resource_kind = row.get::<_, String>(0);
            let resource_id = row.get::<_, String>(1);
            (
                format!(
                    "{}\0{}",
                    resource_kind,
                    logical_postgres_resource_id(&resource_kind, &resource_id)
                ),
                row.get::<_, i64>(2) as u64,
            )
        })
        .collect())
}

fn watermark_resource_id(watermark: &MatrixDataPlaneWatermark) -> String {
    postgres_resource_revision_id(
        "data_plane_watermark",
        &logical_watermark_resource_id(watermark),
    )
}

fn logical_watermark_resource_id(watermark: &MatrixDataPlaneWatermark) -> String {
    format!(
        "{}\0{}\0{}",
        watermark.source_ref, watermark.fact_type, watermark.partition_ref
    )
}

/// PostgreSQL text values cannot contain NUL.  The wire and SQLite logical
/// key remain the existing three-part NUL-delimited value; only the physical
/// PostgreSQL revision/table key is a reversible JSON-encoded representation.
fn postgres_resource_revision_id(resource_kind: &str, resource_id: &str) -> String {
    if resource_kind != "data_plane_watermark" || !resource_id.contains('\0') {
        return resource_id.to_string();
    }
    let parts = resource_id.split('\0').collect::<Vec<_>>();
    if parts.len() != 3 {
        return resource_id.to_string();
    }
    format!(
        "matrix-watermark:{}",
        serde_json::to_string(&parts).unwrap_or_default()
    )
}

fn logical_postgres_resource_id(resource_kind: &str, resource_id: &str) -> String {
    let Some(encoded) = resource_id.strip_prefix("matrix-watermark:") else {
        return resource_id.to_string();
    };
    if resource_kind != "data_plane_watermark" {
        return resource_id.to_string();
    }
    serde_json::from_str::<Vec<String>>(encoded)
        .ok()
        .filter(|parts| parts.len() == 3)
        .map(|parts| parts.join("\0"))
        .unwrap_or_else(|| resource_id.to_string())
}

fn resource_revision<C: PostgresClient>(
    client: &mut C,
    resource_kind: &str,
    resource_id: &str,
) -> MatrixStoreResult<u64> {
    client
        .query_opt(
            "SELECT revision FROM matrix_resource_revision WHERE resource_kind = $1 AND resource_id = $2",
            &[&resource_kind, &resource_id],
        )
        .map_err(postgres_error)
        .map(|row| row.map_or(1, |row| row.get::<_, i64>(0) as u64))
}

fn prepare_revision<C: PostgresClient>(
    client: &mut C,
    resource_kind: &str,
    resource_id: &str,
    exists: bool,
    expected_revision: Option<u64>,
    enforce_revision: bool,
) -> MatrixStoreResult<(Option<u64>, u64, bool)> {
    let stored = client
        .query_opt(
            "SELECT revision FROM matrix_resource_revision WHERE resource_kind = $1 AND resource_id = $2 FOR UPDATE",
            &[&resource_kind, &resource_id],
        )
        .map_err(postgres_error)?
        .map(|row| row.get::<_, i64>(0) as u64);
    let actual = exists.then(|| stored.unwrap_or(1));
    if enforce_revision {
        let matches = if exists {
            expected_revision == actual
        } else {
            expected_revision.is_none()
        };
        if !matches {
            return Err(MatrixStoreError::RevisionConflict {
                resource_ref: format!("matrix:{resource_kind}:{resource_id}"),
                expected: expected_revision,
                actual,
            });
        }
    }
    let revision = actual.unwrap_or_default().checked_add(1).ok_or_else(|| {
        MatrixStoreError::RevisionConflict {
            resource_ref: format!("matrix:{resource_kind}:{resource_id}"),
            expected: expected_revision,
            actual,
        }
    })?;
    Ok((actual, revision, !exists))
}

fn persist_revision<C: PostgresClient>(
    client: &mut C,
    resource_kind: &str,
    resource_id: &str,
    revision: u64,
) -> MatrixStoreResult<()> {
    client
        .execute(
            "INSERT INTO matrix_resource_revision(resource_kind, resource_id, revision, updated_at) \
             VALUES ($1, $2, $3, NOW()) \
             ON CONFLICT(resource_kind, resource_id) DO UPDATE SET revision = EXCLUDED.revision, updated_at = EXCLUDED.updated_at",
            &[&resource_kind, &resource_id, &(revision as i64)],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn find_entity_by_canonical<C: PostgresClient>(
    client: &mut C,
    entity_type: &str,
    canonical_key: &str,
) -> MatrixStoreResult<Option<MatrixEntity>> {
    client
        .query_opt(
            "SELECT payload FROM matrix_entity WHERE payload->>'entity_type' = $1 AND payload->>'canonical_key' = $2",
            &[&entity_type, &canonical_key],
        )
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn save_entity<C: PostgresClient>(
    client: &mut C,
    entity: &MatrixEntity,
    existing: Option<MatrixEntity>,
) -> MatrixStoreResult<MatrixEntity> {
    let mut entity = entity.clone();
    if let Some(existing) = existing {
        entity.entity_id = existing.entity_id;
        entity.created_at = existing.created_at;
        entity.source_keys = merge_source_keys(&existing.source_keys, &entity.source_keys);
    }
    entity.updated_at = Utc::now();
    write_json(client, ENTITY, &entity.entity_id, &entity)?;
    replace_entity_source_keys(client, &entity)?;
    Ok(entity)
}

fn replace_entity_source_keys<C: PostgresClient>(
    client: &mut C,
    entity: &MatrixEntity,
) -> MatrixStoreResult<()> {
    client
        .execute(
            "DELETE FROM matrix_entity_source_key WHERE entity_id = $1",
            &[&entity.entity_id],
        )
        .map_err(postgres_error)?;
    for source_key in &entity.source_keys {
        client
            .execute(
                "INSERT INTO matrix_entity_source_key(source_system, source_key, entity_id, source_ref, created_at) \
                 VALUES ($1, $2, $3, $4, NOW()) \
                 ON CONFLICT(source_system, source_key) DO UPDATE SET entity_id = EXCLUDED.entity_id, source_ref = EXCLUDED.source_ref",
                &[
                    &source_key.normalized_system(),
                    &source_key.normalized_key(),
                    &entity.entity_id,
                    &source_key.source_ref,
                ],
            )
            .map_err(postgres_error)?;
    }
    Ok(())
}

fn merge_source_keys(
    existing: &[matrix_core::MatrixSourceKey],
    incoming: &[matrix_core::MatrixSourceKey],
) -> Vec<matrix_core::MatrixSourceKey> {
    let mut seen = BTreeSet::new();
    existing
        .iter()
        .chain(incoming)
        .filter(|key| seen.insert((key.normalized_system(), key.normalized_key())))
        .cloned()
        .collect()
}

fn find_relation_by_key<C: PostgresClient>(
    client: &mut C,
    relation_type: &str,
    from_entity_id: &str,
    to_entity_id: &str,
) -> MatrixStoreResult<Option<MatrixRelation>> {
    client
        .query_opt(
            "SELECT payload FROM matrix_relation WHERE payload->>'relation_type' = $1 AND payload->>'from_entity_id' = $2 AND payload->>'to_entity_id' = $3",
            &[&relation_type, &from_entity_id, &to_entity_id],
        )
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn save_relation<C: PostgresClient>(
    client: &mut C,
    relation: &MatrixRelation,
    existing: Option<MatrixRelation>,
) -> MatrixStoreResult<MatrixRelation> {
    let mut relation = relation.clone();
    if let Some(existing) = existing {
        relation.relation_id = existing.relation_id;
        relation.created_at = existing.created_at;
    }
    relation.updated_at = Utc::now();
    write_json(client, RELATION, &relation.relation_id, &relation)?;
    Ok(relation)
}

fn build_impact_trace<C: PostgresClient>(
    client: &mut C,
    root_entity_id: &str,
    max_depth: usize,
) -> MatrixStoreResult<MatrixImpactTrace> {
    if read_json::<_, MatrixEntity>(client, ENTITY, root_entity_id)?.is_none() {
        return Err(MatrixStoreError::NotFound(root_entity_id.to_string()));
    }
    let max_depth = max_depth.clamp(1, 5);
    let mut queue = VecDeque::from([(root_entity_id.to_string(), 0usize)]);
    let mut seen_entities = BTreeSet::from([root_entity_id.to_string()]);
    let mut seen_relations = BTreeSet::new();
    let mut hops = Vec::new();
    while let Some((entity_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        let rows = client
            .query(
                "SELECT payload FROM matrix_relation WHERE payload->>'from_entity_id' = $1 OR payload->>'to_entity_id' = $1 ORDER BY updated_at DESC, id ASC LIMIT 500",
                &[&entity_id],
            )
            .map_err(postgres_error)?;
        for row in rows {
            let relation: MatrixRelation =
                serde_json::from_value(row.get(0)).map_err(json_error)?;
            if !seen_relations.insert(relation.relation_id.clone()) {
                continue;
            }
            let next_entity_id = if relation.from_entity_id == entity_id {
                relation.to_entity_id.clone()
            } else {
                relation.from_entity_id.clone()
            };
            let traversal_direction = if relation.from_entity_id == entity_id {
                "outbound"
            } else {
                "inbound"
            }
            .to_string();
            let from_entity = read_json(client, ENTITY, &relation.from_entity_id)?;
            let to_entity = read_json(client, ENTITY, &relation.to_entity_id)?;
            hops.push(MatrixImpactHop {
                depth: depth + 1,
                traversal_direction,
                relation,
                from_entity,
                to_entity,
            });
            if seen_entities.insert(next_entity_id.clone()) {
                queue.push_back((next_entity_id, depth + 1));
            }
        }
    }
    let mut entities = Vec::new();
    for entity_id in seen_entities {
        if let Some(entity) = read_json(client, ENTITY, &entity_id)? {
            entities.push(entity);
        }
    }
    Ok(MatrixImpactTrace {
        root_entity_id: root_entity_id.to_string(),
        max_depth,
        entities,
        hops,
        generated_at: Utc::now(),
    })
}

fn find_metric_dependency_by_key<C: PostgresClient>(
    client: &mut C,
    upstream_metric_id: &str,
    downstream_metric_id: &str,
    dependency_type: &str,
) -> MatrixStoreResult<Option<MatrixMetricDependency>> {
    client
        .query_opt(
            "SELECT payload FROM matrix_metric_dependency WHERE payload->>'upstream_metric_id' = $1 AND payload->>'downstream_metric_id' = $2 AND payload->>'dependency_type' = $3",
            &[&upstream_metric_id, &downstream_metric_id, &dependency_type],
        )
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn save_metric_dependency<C: PostgresClient>(
    client: &mut C,
    dependency: &MatrixMetricDependency,
    existing: Option<MatrixMetricDependency>,
) -> MatrixStoreResult<MatrixMetricDependency> {
    let mut dependency = dependency.clone();
    if let Some(existing) = existing {
        dependency.dependency_id = existing.dependency_id;
        dependency.created_at = existing.created_at;
    }
    dependency.updated_at = Utc::now();
    write_json(
        client,
        METRIC_DEPENDENCY,
        &dependency.dependency_id,
        &dependency,
    )?;
    Ok(dependency)
}

fn all_json<C: PostgresClient, T: DeserializeOwned>(
    client: &mut C,
    table: &str,
) -> MatrixStoreResult<Vec<T>> {
    let sql = format!("SELECT payload FROM {table} ORDER BY id ASC");
    client
        .query(&sql, &[])
        .map_err(postgres_error)?
        .into_iter()
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .collect()
}

fn metric_lineage<C: PostgresClient>(
    client: &mut C,
    metric_id: &str,
    max_depth: usize,
) -> MatrixStoreResult<MatrixMetricLineage> {
    let max_depth = max_depth.clamp(1, 8);
    let dependencies = all_json::<_, MatrixMetricDependency>(client, METRIC_DEPENDENCY)?;
    let mut upstream_dependencies = Vec::new();
    let mut downstream_dependencies = Vec::new();
    let mut impacted_metric_ids = BTreeSet::new();
    let mut frontier = BTreeSet::from([metric_id.to_string()]);
    let mut visited = BTreeSet::new();
    for _ in 0..max_depth {
        let mut next = BTreeSet::new();
        for dependency in &dependencies {
            if frontier.contains(&dependency.downstream_metric_id) {
                upstream_dependencies.push(dependency.clone());
                if visited.insert(dependency.upstream_metric_id.clone()) {
                    next.insert(dependency.upstream_metric_id.clone());
                }
            }
            if frontier.contains(&dependency.upstream_metric_id) {
                downstream_dependencies.push(dependency.clone());
                if dependency.downstream_metric_id != metric_id {
                    impacted_metric_ids.insert(dependency.downstream_metric_id.clone());
                }
                if visited.insert(dependency.downstream_metric_id.clone()) {
                    next.insert(dependency.downstream_metric_id.clone());
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    upstream_dependencies.sort_by(|left, right| left.dependency_id.cmp(&right.dependency_id));
    upstream_dependencies.dedup_by(|left, right| left.dependency_id == right.dependency_id);
    downstream_dependencies.sort_by(|left, right| left.dependency_id.cmp(&right.dependency_id));
    downstream_dependencies.dedup_by(|left, right| left.dependency_id == right.dependency_id);
    Ok(MatrixMetricLineage {
        metric_id: metric_id.to_string(),
        upstream_dependencies,
        downstream_dependencies,
        impacted_metric_ids: impacted_metric_ids.into_iter().collect(),
        generated_at: Utc::now(),
    })
}

fn metrics_affected_by_fact_type<C: PostgresClient>(
    client: &mut C,
    fact_type: &str,
) -> MatrixStoreResult<Vec<String>> {
    let mut metric_ids = all_json::<_, MatrixMetricDependency>(client, METRIC_DEPENDENCY)?
        .into_iter()
        .filter(|dependency| {
            dependency
                .required_fact_types
                .iter()
                .any(|item| item == fact_type)
        })
        .map(|dependency| dependency.downstream_metric_id)
        .collect::<Vec<_>>();
    metric_ids.sort();
    metric_ids.dedup();
    Ok(metric_ids)
}

fn metric_ids_for_fact_type<C: PostgresClient>(
    client: &mut C,
    fact_type: &str,
) -> MatrixStoreResult<Vec<String>> {
    let mut metric_ids = all_json::<_, MatrixMetricDefinition>(client, METRIC_DEFINITION)?
        .into_iter()
        .filter(|definition| definition.inputs.iter().any(|input| input == fact_type))
        .map(|definition| definition.metric_id)
        .collect::<Vec<_>>();
    metric_ids.sort();
    metric_ids.dedup();
    Ok(metric_ids)
}

fn latest_metric_state_for_metric<C: PostgresClient>(
    client: &mut C,
    metric_id: &str,
) -> MatrixStoreResult<Option<MatrixMetricState>> {
    client
        .query_opt(
            "SELECT payload FROM matrix_metric_state WHERE payload->>'metric_id' = $1 ORDER BY updated_at DESC, id ASC LIMIT 1",
            &[&metric_id],
        )
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn latest_metric_state<C: PostgresClient>(
    client: &mut C,
    metric_id: &str,
    entity_scope: &str,
    period: &str,
) -> MatrixStoreResult<Option<MatrixMetricState>> {
    client
        .query_opt(
            "SELECT payload FROM matrix_metric_state WHERE payload->>'metric_id' = $1 AND payload->>'entity_scope' = $2 AND payload->>'period' = $3 ORDER BY updated_at DESC, id ASC LIMIT 1",
            &[&metric_id, &entity_scope, &period],
        )
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn states_for_metric<C: PostgresClient>(
    client: &mut C,
    metric_id: &str,
) -> MatrixStoreResult<Vec<MatrixMetricState>> {
    client
        .query(
            "SELECT payload FROM matrix_metric_state WHERE payload->>'metric_id' = $1 ORDER BY updated_at DESC, id ASC",
            &[&metric_id],
        )
        .map_err(postgres_error)?
        .into_iter()
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .collect()
}

fn compute_priority(job: &MatrixComputeJob) -> f32 {
    let metric_score = (job.metric_ids.len() as f32 / 8.0).min(1.0);
    let trigger_score = if job.trigger_fact_type.contains("shortage")
        || job.trigger_fact_type.contains("delivery")
        || job.trigger_fact_type.contains("quality")
    {
        0.9
    } else {
        0.55
    };
    (metric_score * 0.45 + trigger_score * 0.55).min(1.0)
}

fn recompute_metrics<C: PostgresClient>(
    client: &mut C,
    metric_filter: Option<&BTreeSet<String>>,
) -> MatrixStoreResult<MatrixMetricRecomputeResult> {
    ensure_metric_query_definitions(client, metric_filter)?;
    let query_results = postgres_metric_query_results(client, metric_filter)?;
    let mut states = Vec::new();
    let mut changes = Vec::new();
    let mut attention = Vec::new();
    for result in query_results {
        let previous = latest_metric_state(
            client,
            &result.metric_id,
            &result.entity_scope,
            &result.period,
        )?;
        let previous_value = previous.as_ref().map(|state| state.value);
        let delta = previous_value.map_or(result.value, |previous| result.value - previous);
        let delta_ratio = previous_value
            .and_then(|previous| (previous.abs() > f64::EPSILON).then_some(delta / previous));
        let state = MatrixMetricState {
            state_id: format!("metric-state-{}", uuid::Uuid::new_v4()),
            metric_id: result.metric_id.clone(),
            entity_scope: result.entity_scope.clone(),
            period: result.period.clone(),
            value: result.value,
            previous_value,
            delta,
            delta_ratio,
            status: MatrixMetricState::status_for_delta(delta),
            computed_at: Utc::now(),
            input_fact_refs: result.input_fact_refs.clone(),
            confidence: result.confidence,
        };
        write_json(client, METRIC_STATE, &state.state_id, &state)?;
        if delta.abs() > f64::EPSILON {
            let change = MatrixChangeEvent {
                change_id: format!("change-{}", uuid::Uuid::new_v4()),
                change_type: "metric_delta".to_string(),
                entity_ref: result.entity_scope,
                metric_id: Some(result.metric_id),
                from_value: previous_value.map(Value::from),
                to_value: Some(Value::from(result.value)),
                delta,
                period: result.period,
                detected_at: Utc::now(),
                source_fact_refs: result.input_fact_refs,
                severity_hint: MatrixChangeEvent::severity_for_delta(delta),
            };
            write_json(client, CHANGE, &change.change_id, &change)?;
            let item = attention_from_change(&change, &state);
            write_json(client, ATTENTION, &item.attention_id, &item)?;
            changes.push(change);
            attention.push(item);
        }
        states.push(state);
    }
    Ok(MatrixMetricRecomputeResult {
        metric_state_count: states.len(),
        change_count: changes.len(),
        attention_count: attention.len(),
        metric_states: states,
        changes,
        attention,
    })
}

fn ensure_metric_query_definitions<C: PostgresClient>(
    client: &mut C,
    metric_filter: Option<&BTreeSet<String>>,
) -> MatrixStoreResult<()> {
    let filter = metric_filter
        .map(|items| items.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let filter_clause = if metric_filter.is_some() {
        "AND (fact.payload->>'metric_key') = ANY($1)"
    } else {
        ""
    };
    let sql = format!(
        "SELECT DISTINCT ON (fact.payload->>'metric_key')
                fact.payload->>'metric_key', fact.payload->>'fact_type', fact.payload->'measures'
         FROM matrix_fact fact
         LEFT JOIN matrix_metric_definition definition
           ON definition.id = fact.payload->>'metric_key'
         WHERE fact.payload ? 'metric_key' AND definition.id IS NULL {filter_clause}
         ORDER BY fact.payload->>'metric_key', fact.payload->>'event_time', fact.id"
    );
    let rows = if metric_filter.is_some() {
        client.query(&sql, &[&filter]).map_err(postgres_error)?
    } else {
        client.query(&sql, &[]).map_err(postgres_error)?
    };
    for row in rows {
        let metric_id: String = row.get(0);
        let fact_type: String = row.get(1);
        let measures: Value = row.get(2);
        let object = measures.as_object().ok_or_else(|| {
            MatrixStoreError::Backend(format!(
                "metric {metric_id} measures must be a top-level object"
            ))
        })?;
        let candidates = object
            .iter()
            .filter(|(_, value)| value.as_f64().is_some())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            return Err(MatrixStoreError::Backend(format!(
                "metric {metric_id} must register one explicit measure (found {})",
                candidates.len()
            )));
        }
        let definition = MatrixMetricDefinition::inferred_for_measure(
            &metric_id,
            fact_type,
            candidates[0].clone(),
        );
        write_json(client, METRIC_DEFINITION, &metric_id, &definition)?;
    }
    Ok(())
}

fn postgres_metric_query_results<C: PostgresClient>(
    client: &mut C,
    metric_filter: Option<&BTreeSet<String>>,
) -> MatrixStoreResult<Vec<MatrixQueryResult>> {
    let mut definitions = all_json::<_, MatrixMetricDefinition>(client, METRIC_DEFINITION)?;
    if let Some(filter) = metric_filter {
        definitions.retain(|definition| filter.contains(&definition.metric_id));
    }
    definitions.sort_by(|left, right| left.metric_id.cmp(&right.metric_id));
    let mut results = Vec::new();
    for definition in definitions {
        let plan = definition.query_plan();
        plan.validate()
            .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
        results.extend(execute_postgres_metric_query(client, &plan)?);
    }
    Ok(results)
}

fn execute_postgres_metric_query<C: PostgresClient>(
    client: &mut C,
    plan: &MatrixQueryPlan,
) -> MatrixStoreResult<Vec<MatrixQueryResult>> {
    let sql = r#"
        SELECT COALESCE(fact.payload->'entity_refs'->>0, 'enterprise') AS entity_scope,
               COALESCE(fact.payload->'dimensions'->>'period', fact.payload->'dimensions'->>'week', 'current') AS period,
               fact.payload->>'fact_type' AS fact_type,
               SUM(CASE WHEN jsonb_typeof(fact.payload->'measures'->$2) = 'number'
                        THEN (fact.payload->'measures'->>$2)::double precision ELSE 0 END) AS numerator_sum,
               CASE WHEN $3::text IS NULL THEN NULL ELSE
                    SUM(CASE WHEN jsonb_typeof(fact.payload->'measures'->$3) = 'number'
                             THEN (fact.payload->'measures'->>$3)::double precision ELSE 0 END)
               END AS denominator_sum,
               ARRAY_AGG('matrix:fact:' || (fact.payload->>'fact_id')
                         ORDER BY fact.payload->>'event_time', fact.id) AS fact_refs,
               AVG((fact.payload->>'confidence')::double precision) AS confidence,
               BOOL_AND(jsonb_typeof(fact.payload->'measures'->$2) = 'number'
                    AND ($3::text IS NULL OR jsonb_typeof(fact.payload->'measures'->$3) = 'number')) AS valid_operands
        FROM matrix_fact fact
        WHERE fact.payload->>'metric_key' = $1
        GROUP BY entity_scope, period, fact_type
        ORDER BY entity_scope, period, fact_type
        LIMIT $4
    "#;
    let limit = i64::try_from(plan.cardinality_limit.saturating_add(1))
        .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
    let rows = client
        .query(
            sql,
            &[
                &plan.metric_id,
                &plan.numerator_measure,
                &plan.denominator_measure,
                &limit,
            ],
        )
        .map_err(postgres_error)?;
    if rows.len() > plan.cardinality_limit {
        return Err(MatrixStoreError::Backend(format!(
            "metric {} exceeds query cardinality limit {}",
            plan.metric_id, plan.cardinality_limit
        )));
    }
    rows.into_iter()
        .map(|row| {
            let valid_operands: bool = row.get(7);
            if !valid_operands {
                return Err(MatrixStoreError::Backend(format!(
                    "metric {} contains missing or non-numeric operands",
                    plan.metric_id
                )));
            }
            let numerator: f64 = row.get(3);
            let denominator: Option<f64> = row.get(4);
            let value = matrix_core::evaluate_matrix_formula(plan, numerator, denominator)
                .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
            Ok(MatrixQueryResult {
                metric_id: plan.metric_id.clone(),
                entity_scope: row.get(0),
                period: row.get(1),
                fact_type: row.get(2),
                value,
                input_fact_refs: row.get(5),
                confidence: row.get::<_, f64>(6) as f32,
            })
        })
        .collect()
}

fn attention_from_change(
    change: &MatrixChangeEvent,
    state: &MatrixMetricState,
) -> MatrixAttentionItem {
    let now = Utc::now();
    let severity = match change.severity_hint.as_str() {
        "critical" => MatrixSeverity::Critical,
        "warning" => MatrixSeverity::Warning,
        "normal" => MatrixSeverity::Normal,
        _ => MatrixSeverity::Unknown,
    };
    let severity_score = match severity {
        MatrixSeverity::Critical => 1.0,
        MatrixSeverity::Warning => 0.65,
        MatrixSeverity::Normal => 0.2,
        MatrixSeverity::Unknown => 0.35,
    };
    let urgency = if change.delta.abs() > 0.0 { 0.7 } else { 0.2 };
    let impact_scope = (change.delta.abs() / 100.0).min(1.0) as f32;
    let strategic_weight = 0.5_f32;
    let confidence = state.confidence;
    MatrixAttentionItem {
        attention_id: format!("attention-{}", uuid::Uuid::new_v4()),
        title: format!(
            "Metric {} changed by {} for {}",
            state.metric_id, change.delta, state.entity_scope
        ),
        business_domain: state
            .metric_id
            .split('_')
            .next()
            .unwrap_or("operations")
            .to_string(),
        entity_ref: Some(state.entity_scope.clone()),
        metric_refs: vec![state.metric_id.clone()],
        period: Some(state.period.clone()),
        priority_score: severity_score * 0.30
            + urgency * 0.20
            + impact_scope * 0.20
            + strategic_weight * 0.15
            + confidence * 0.10
            + 0.05,
        severity,
        urgency,
        strategic_weight,
        confidence,
        reason_codes: vec![
            "metric_recomputed".to_string(),
            "metric_delta_detected".to_string(),
        ],
        linked_changes: vec![format!("matrix:change:{}", change.change_id)],
        linked_anomalies: Vec::new(),
        linked_impacts: Vec::new(),
        owner_roles: vec!["operations_analyst".to_string()],
        status: "open".to_string(),
        created_at: now,
        updated_at: now,
    }
}

fn source_pack_delta_plan<C: PostgresClient>(
    client: &mut C,
    source_pack: &MatrixSourcePack,
) -> MatrixStoreResult<MatrixSourceDeltaPlan> {
    let mut fact_types = source_pack
        .fact_mappings
        .iter()
        .map(|mapping| mapping.fact_type.clone())
        .collect::<Vec<_>>();
    fact_types.sort();
    fact_types.dedup();
    let mut affected_metric_ids = Vec::new();
    for fact_type in &fact_types {
        affected_metric_ids.extend(metrics_affected_by_fact_type(client, fact_type)?);
        affected_metric_ids.extend(metric_ids_for_fact_type(client, fact_type)?);
    }
    affected_metric_ids.extend(
        source_pack
            .fact_mappings
            .iter()
            .map(|mapping| mapping.metric_key.clone()),
    );
    affected_metric_ids.sort();
    affected_metric_ids.dedup();
    Ok(MatrixSourceDeltaPlan {
        source_pack_id: source_pack.source_pack_id.clone(),
        fact_types,
        affected_metric_ids,
        compute_scope: "partitioned_by_source_period_entity".to_string(),
        planned_at: Utc::now(),
    })
}

fn source_kind_for_access_mode(access_mode: &str) -> MatrixSourceKind {
    match access_mode {
        "batch_file" | "file" | "manual_upload" => MatrixSourceKind::File,
        "db_view" | "database_view" => MatrixSourceKind::Db,
        "api" => MatrixSourceKind::Api,
        "rpa" => MatrixSourceKind::Rpa,
        "connector" => MatrixSourceKind::Connector,
        _ => MatrixSourceKind::Manual,
    }
}

fn read_scenario_result_for_run<C: PostgresClient>(
    client: &mut C,
    run_id: &str,
) -> MatrixStoreResult<Option<MatrixScenarioResult>> {
    client
        .query_opt(
            "SELECT payload FROM matrix_scenario_result WHERE payload->>'run_id' = $1",
            &[&run_id],
        )
        .map_err(postgres_error)?
        .map(|row| serde_json::from_value(row.get(0)).map_err(json_error))
        .transpose()
}

fn row_value(row: &Value, field: &str) -> Option<String> {
    row.get(field)
        .map(json_scalar_to_string)
        .filter(|value| !value.is_empty())
}

fn json_scalar_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn pick_fields(row: &Value, fields: &[String]) -> Value {
    if fields.is_empty() {
        return row.clone();
    }
    let mut object = serde_json::Map::new();
    for field in fields {
        object.insert(
            field.clone(),
            row.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(object)
}

fn omit_fields(row: &Value, fields: &[String]) -> Value {
    let Some(source) = row.as_object() else {
        return row.clone();
    };
    let mut object = serde_json::Map::new();
    for (key, value) in source {
        if !fields.iter().any(|field| field == key) {
            object.insert(key.clone(), value.clone());
        }
    }
    Value::Object(object)
}

fn stable_json_hash(value: &Value) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(value).unwrap_or_default())
    )
}

fn stable_suffix(parts: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(part.as_bytes());
        bytes.push(0);
    }
    format!("{:x}", Sha256::digest(bytes))
        .chars()
        .take(24)
        .collect()
}

fn stable_entity_id(source_name: &str, entity_type: &str, source_key: &str) -> String {
    format!(
        "entity-{}",
        stable_suffix(&[source_name, entity_type, source_key])
    )
}

fn stable_entity_id_for_field(
    source_pack: &MatrixSourcePack,
    source_key_field: &str,
    source_key: &str,
) -> Option<String> {
    source_pack
        .entity_mappings
        .iter()
        .find(|mapping| mapping.source_key_field == source_key_field)
        .map(|mapping| {
            stable_entity_id(
                &source_pack.source_name,
                &mapping.matrix_entity_type,
                source_key,
            )
        })
}

fn stable_entity_reference_for_field(
    source_pack: &MatrixSourcePack,
    source_key_field: &str,
    source_key: &str,
) -> String {
    stable_entity_id_for_field(source_pack, source_key_field, source_key)
        .map(|entity_id| format!("matrix:entity:{entity_id}"))
        .unwrap_or_else(|| {
            format!(
                "matrix:entity:{}",
                stable_suffix(&[&source_pack.source_name, source_key])
            )
        })
}

fn stable_fact_id(snapshot_id: &str, fact_type: &str, dedup_key: &str) -> String {
    format!(
        "fact-{}",
        stable_suffix(&[snapshot_id, fact_type, dedup_key])
    )
}

fn stable_relation_id(
    snapshot_id: &str,
    relation_type: &str,
    from_entity_id: &str,
    to_entity_id: &str,
    dedup_key: &str,
) -> String {
    format!(
        "relation-{}",
        stable_suffix(&[
            snapshot_id,
            relation_type,
            from_entity_id,
            to_entity_id,
            dedup_key
        ])
    )
}

fn stable_attention_id(source: &str, fact_id: &str) -> String {
    format!("attention-{}", stable_suffix(&[source, fact_id]))
}

#[cfg(test)]
mod tests {
    use std::env;

    use matrix_core::{
        MatrixDataPlaneIngestPlanInput, MatrixEntityInput, MatrixFactInput, MatrixSourceKey,
    };
    use storage::{StaticSecretRefResolver, StorageDomainId, StorageEndpoint, StorageScope};

    use super::*;
    use crate::{copy_quiesced_matrix_store, MatrixSqliteRepository};

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn real_postgres_adapter_preserves_matrix_snapshot() {
        let url = env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let source = MatrixSqliteRepository::in_memory().expect("sqlite source opens");
        let entity = source
            .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
                entity_id: Some("matrix-pg-migration-entity".to_string()),
                entity_type: "part".to_string(),
                canonical_key: "PG-MIGRATION-PART".to_string(),
                display_name: Some("PostgreSQL migration part".to_string()),
                source_keys: vec![MatrixSourceKey {
                    source_system: "erp".to_string(),
                    source_key: "pg-migration-part".to_string(),
                    source_ref: Some("erp://part/pg-migration-part".to_string()),
                }],
                attributes: serde_json::json!({"tier": "integration-test"}),
                confidence: Some(0.99),
            }))
            .expect("entity saves");
        source
            .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
                fact_id: Some("matrix-pg-migration-fact".to_string()),
                snapshot_id: Some("matrix-pg-migration-snapshot".to_string()),
                fact_type: "inventory.level".to_string(),
                entity_refs: vec![entity.reference()],
                metric_key: Some("inventory_level".to_string()),
                dimensions: serde_json::json!({"warehouse": "test"}),
                measures: serde_json::json!({"quantity": 42}),
                event_time: Some(Utc::now()),
                valid_from: None,
                valid_to: None,
                source_ref: Some("erp://inventory/test".to_string()),
                confidence: Some(0.98),
                raw_hash: Some("sha256:matrix-pg-migration".to_string()),
            }))
            .expect("fact saves");
        let mut load_definition = MatrixMetricDefinition::inferred_for_measure(
            "work_center_load",
            "manufacturing.work_center_load",
            "load_hours",
        );
        load_definition.formula_ref = matrix_core::MATRIX_FORMULA_RATIO_PERCENT_V1.to_string();
        load_definition.denominator_measure = Some("capacity_hours".to_string());
        source
            .register_metric_definition(&load_definition)
            .expect("ratio definition saves");
        source
            .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
                fact_id: Some("matrix-pg-load-fact".to_string()),
                snapshot_id: Some("matrix-pg-load-snapshot".to_string()),
                fact_type: "manufacturing.work_center_load".to_string(),
                entity_refs: vec!["work-center:pg".to_string()],
                metric_key: Some("work_center_load".to_string()),
                dimensions: serde_json::json!({"week": "2026-W30"}),
                measures: serde_json::json!({"load_hours": 188, "capacity_hours": 160}),
                event_time: Some(Utc::now()),
                valid_from: None,
                valid_to: None,
                source_ref: Some("mes://work-center/pg".to_string()),
                confidence: Some(0.9),
                raw_hash: Some("sha256:matrix-pg-load".to_string()),
            }))
            .expect("ratio fact saves");
        let plan = source
            .plan_data_plane_ingest(MatrixDataPlaneIngestPlanInput {
                source_ref: "erp://inventory/test".to_string(),
                fact_type: "inventory.level".to_string(),
                partition_ref: Some("2026-07-23".to_string()),
                high_watermark: Some("42".to_string()),
                estimated_rows: Some(1),
                raw_checksum: Some("sha256:matrix-pg-migration".to_string()),
                expected_revision: None,
                adapter_id: Some("integration-test".to_string()),
                strategy: Some("full_snapshot".to_string()),
                table: Some("inventory".to_string()),
                cursor: Some("42".to_string()),
                offset: Some(1),
                metric_ids: Vec::new(),
            })
            .expect("watermark plans");
        source
            .commit_data_plane_ingest(&plan)
            .expect("watermark commits");

        let resolver = StaticSecretRefResolver::new([("matrix.pg.test".to_string(), url)]);
        let target = PostgresMatrixRepository::connect(
            PostgresConnectionConfig::new(
                "matrix-postgres-integration-test",
                "matrix.pg.test",
                "cowd-matrix-postgres-contract",
            ),
            &resolver,
        )
        .expect("postgres target opens");
        let manifest_root = tempfile::tempdir().expect("manifest root");
        let manifest =
            copy_quiesced_matrix_store(&source, &target, manifest_root.path().join("matrix.json"))
                .expect("quiesced migration succeeds");
        let selected = crate::MatrixStoreHandle::new(StorageEndpoint::postgres(
            StorageDomainId::Matrix,
            StorageScope::Global,
            "matrix",
            "matrix.0001",
        ))
        .open_with_postgres_executor(target.executor().clone())
        .expect("injected PostgreSQL Matrix selection succeeds");

        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert!(manifest.record_count >= 3);
        assert_eq!(
            MatrixStore::health(&*selected)
                .expect("selected store health")
                .fact_count,
            2
        );
        assert!(MatrixStore::get_entity(&target, &entity.entity_id)
            .expect("entity reads")
            .is_some());
        let recompute = MatrixStore::recompute_metrics(&target)
            .expect("PostgreSQL executes the normalized query plan");
        let load = recompute
            .metric_states
            .iter()
            .find(|state| state.metric_id == "work_center_load")
            .expect("ratio metric state");
        assert!((load.value - 117.5).abs() < f64::EPSILON);
        assert!(
            MatrixStore::resolve_entity_by_source_key(&target, "erp", "pg-migration-part")
                .expect("source key resolves")
                .is_some()
        );
        assert_eq!(
            MatrixStore::list_facts(&target, 10)
                .expect("facts list")
                .len(),
            2
        );
        assert!(MatrixStore::get_data_plane_watermark(
            &target,
            "erp://inventory/test",
            "inventory.level",
            "2026-07-23",
        )
        .expect("watermark reads")
        .is_some());
        assert!(copy_quiesced_matrix_store(
            &source,
            &target,
            manifest_root.path().join("again.json")
        )
        .is_err());
    }
}
