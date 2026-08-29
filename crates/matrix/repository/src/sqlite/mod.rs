#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use storage::{SqliteExecutor, StorageHandle};
use thiserror::Error;

use crate::migration::canonicalize_payload;
use crate::{
    MatrixHealth, MatrixLocalDataPlane, MatrixMetricRecomputeResult, MatrixMigrationSnapshot,
    MatrixRecallQuery, MatrixRevisioned,
};
use matrix_core::{
    build_metric_compute_jobs, MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob,
    MatrixComputeJobInput, MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput,
    MatrixDataPlane, MatrixDataPlaneHealth, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark, MatrixEntity, MatrixEvidencePacket,
    MatrixEvidenceSourceRef, MatrixFact, MatrixImpactHop, MatrixImpactTrace,
    MatrixMetricAttentionPlan, MatrixMetricAttentionScore, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricSnapshotItem,
    MatrixMetricState, MatrixOntologyPack, MatrixQualityGateDecision, MatrixQueryInput,
    MatrixQueryResult, MatrixRelation, MatrixScenarioResult, MatrixScenarioRun,
    MatrixScenarioRunStatus, MatrixScenarioSpec, MatrixSeverity, MatrixSourceDeltaPlan,
    MatrixSourceKey, MatrixSourcePack, MatrixSourcePackValidation, MatrixSourceSnapshot,
    MatrixSourceSnapshotApplyReport, MatrixSourceSnapshotInput, MatrixSourceSnapshotPlan,
};

#[path = "entity.rs"]
mod entity;
use entity::*;
#[path = "metric.rs"]
mod metric;
use metric::*;
#[path = "evidence.rs"]
mod evidence;
use evidence::*;
#[path = "scenario.rs"]
mod scenario;
use scenario::*;

#[derive(Debug, Error)]
pub enum MatrixSqliteRepositoryError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid matrix metric query: {0}")]
    InvalidMetricQuery(String),
    #[error("matrix record not found: {0}")]
    NotFound(String),
    #[error("matrix migration error: {0}")]
    Migration(String),
    #[error("invalid matrix scenario: {0}")]
    InvalidScenario(String),
    #[error("matrix scenario state conflict: {0}")]
    ScenarioState(String),
    #[error(
        "matrix revision conflict for {resource_ref}: expected {expected:?}, actual {actual:?}"
    )]
    RevisionConflict {
        resource_ref: String,
        expected: Option<u64>,
        actual: Option<u64>,
    },
}

#[derive(Debug)]
pub struct MatrixSqliteRepository {
    executor: SqliteExecutor,
}

impl MatrixSqliteRepository {
    /// Atomically applies a pre-validated ownership split plan. This path is
    /// deliberately separate from normal upserts because migration timestamps
    /// are authoritative and must never be replaced with the local clock.
    pub fn import_ownership_split(
        &self,
        plan: &matrix_core::CoreMatrixImportPlan,
    ) -> Result<crate::MatrixOwnershipImportOutcome, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        crate::ownership_import::apply_sqlite(&mut connection, plan)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, MatrixSqliteRepositoryError> {
        let handle = StorageHandle::sqlite("matrix", path.as_ref(), "matrix", "matrix_executor");
        Self::open_storage_handle(&handle)
    }

    pub fn open_storage_handle(
        handle: &StorageHandle,
    ) -> Result<Self, MatrixSqliteRepositoryError> {
        Self::from_executor(SqliteExecutor::for_handle(handle)?)
    }

    pub fn in_memory() -> Result<Self, MatrixSqliteRepositoryError> {
        Self::from_executor(SqliteExecutor::in_memory("matrix-repository")?)
    }

    fn from_executor(executor: SqliteExecutor) -> Result<Self, MatrixSqliteRepositoryError> {
        let connection = executor.checkout()?;
        initialize_schema(&connection)?;
        Ok(Self { executor })
    }

    pub fn health(&self) -> Result<MatrixHealth, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        Ok(MatrixHealth {
            schema_version: schema_version(&connection)?,
            fact_count: count_table(&connection, "matrix_fact")?,
            metric_definition_count: count_table(&connection, "matrix_metric_definition")?,
            metric_state_count: count_table(&connection, "matrix_metric_state")?,
            change_count: count_table(&connection, "matrix_change_event")?,
            attention_count: count_table(&connection, "matrix_attention_item")?,
            evidence_count: count_table(&connection, "matrix_evidence_packet")?,
            entity_count: count_table(&connection, "matrix_entity")?,
            relation_count: count_table(&connection, "matrix_relation")?,
            metric_dependency_count: count_table(&connection, "matrix_metric_dependency")?,
            compute_job_count: count_table(&connection, "matrix_compute_job")?,
            quality_gate_count: count_table(&connection, "matrix_quality_gate")?,
            source_pack_count: count_table(&connection, "matrix_source_pack")?,
            data_plane_watermark_count: count_table(&connection, "matrix_data_plane_watermark")?,
            connector_run_count: count_table(&connection, "matrix_connector_run")?,
            source_snapshot_count: count_table(&connection, "matrix_source_snapshot")?,
            ontology_pack_count: count_table(&connection, "matrix_ontology_pack")?,
            entity_match_candidate_count: count_table(
                &connection,
                "matrix_entity_match_candidate",
            )?,
            entity_conflict_decision_count: count_table(
                &connection,
                "matrix_entity_conflict_decision",
            )?,
            metric_snapshot_count: count_table(&connection, "matrix_metric_snapshot")?,
            scenario_spec_count: count_table(&connection, "matrix_scenario_spec")?,
            scenario_run_count: count_table(&connection, "matrix_scenario_run")?,
            scenario_result_count: count_table(&connection, "matrix_scenario_result")?,
        })
    }

    /// Export every Matrix aggregate and optimistic revision for a verified
    /// maintenance-window migration.  Normal request paths must use the
    /// typed store operations instead.
    pub fn export_migration_snapshot(
        &self,
    ) -> Result<MatrixMigrationSnapshot, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut tables = BTreeMap::new();
        for (table, id_column, payload_column) in [
            ("matrix_entity", "entity_id", "entity_json"),
            ("matrix_relation", "relation_id", "relation_json"),
            ("matrix_attention_item", "attention_id", "attention_json"),
            ("matrix_evidence_packet", "packet_id", "packet_json"),
            ("matrix_quality_gate", "gate_id", "gate_json"),
            ("matrix_metric_definition", "metric_id", "definition_json"),
            (
                "matrix_metric_dependency",
                "dependency_id",
                "dependency_json",
            ),
            ("matrix_metric_state", "state_id", "state_json"),
            ("matrix_metric_snapshot", "snapshot_id", "snapshot_json"),
            ("matrix_compute_job", "job_id", "job_json"),
            ("matrix_change_event", "change_id", "change_json"),
            ("matrix_source_pack", "source_pack_id", "source_pack_json"),
            ("matrix_connector_run", "run_id", "run_json"),
            ("matrix_source_snapshot", "snapshot_id", "snapshot_json"),
            ("matrix_ontology_pack", "ontology_id", "pack_json"),
            (
                "matrix_entity_match_candidate",
                "candidate_id",
                "candidate_json",
            ),
            (
                "matrix_entity_conflict_decision",
                "decision_id",
                "decision_json",
            ),
            ("matrix_scenario_spec", "scenario_id", "spec_json"),
            ("matrix_scenario_run", "run_id", "run_json"),
            ("matrix_scenario_result", "result_id", "result_json"),
        ] {
            tables.insert(
                table.to_string(),
                export_json_records(&connection, table, id_column, payload_column)?,
            );
        }
        let facts = list_facts(&connection, i64::MAX as usize)?
            .into_iter()
            .map(|fact| {
                let id = fact.fact_id.clone();
                serde_json::to_value(fact)
                    .map_err(MatrixSqliteRepositoryError::from)
                    .and_then(|payload| {
                        canonicalize_payload("matrix_fact", payload).map_err(|error| {
                            MatrixSqliteRepositoryError::Migration(error.to_string())
                        })
                    })
                    .map(|payload| (id, payload))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        tables.insert("matrix_fact".to_string(), facts);
        tables.insert(
            "matrix_data_plane_watermark".to_string(),
            export_watermark_records(&connection)?,
        );
        let revisions = export_revisions(&connection)?;
        MatrixMigrationSnapshot::new(schema_version(&connection)?, tables, revisions)
            .map_err(|error| MatrixSqliteRepositoryError::Migration(error.to_string()))
    }

    pub fn data_plane_health(&self) -> Result<MatrixDataPlaneHealth, MatrixSqliteRepositoryError> {
        let health = self.health()?;
        Ok(MatrixLocalDataPlane::embedded_sqlite(health.data_plane_watermark_count).health())
    }

    pub fn plan_data_plane_ingest(
        &self,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> Result<MatrixDataPlaneIngestPlan, MatrixSqliteRepositoryError> {
        let source_ref = input.source_ref.clone();
        let mut plan =
            MatrixLocalDataPlane::embedded_sqlite(self.health()?.data_plane_watermark_count)
                .plan_ingest(input);
        if plan.affected_metric_ids.is_empty() {
            let connection = self.executor.checkout()?;
            let mut affected = metrics_affected_by_fact_type(&connection, &plan.fact_type)?;
            affected.extend(metric_ids_for_fact_type(&connection, &plan.fact_type)?);
            // A source-pack is the canonical declaration of the metrics that
            // its facts materialize. A newly saved pack need not already have
            // persisted metric dependencies, so fact-type lookup alone would
            // incorrectly return an empty first-use ingest plan.
            if let Some(source_pack_id) = source_ref
                .strip_prefix("source-pack://")
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                if let Some(source_pack) = find_source_pack(&connection, source_pack_id)? {
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
                    job_id: Some(format!("compute-job-{}-{}", plan.batch_id, metric_id)),
                    trigger_fact_type: plan.fact_type.clone(),
                    trigger_fact_refs: vec![format!("matrix:data-plane-batch:{}", plan.batch_id)],
                    entity_scope: None,
                    period: Some(plan.partition_ref.clone()),
                    metric_ids: vec![metric_id.clone()],
                    priority: Some(0.72),
                })
                .collect();
            plan.affected_metric_ids = affected;
        }
        Ok(plan)
    }

    /// Commit the durable cursor of an already-applied source batch.
    ///
    /// Planning deliberately has no persistence side effect: callers may use
    /// it for previews.  The source-ingestion pipeline invokes this only after
    /// its snapshot rows have been applied, so a watermark is never advanced
    /// for a batch that did not reach the Matrix store.
    pub fn commit_data_plane_ingest(
        &self,
        plan: &MatrixDataPlaneIngestPlan,
    ) -> Result<MatrixDataPlaneWatermark, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let resource_id = data_plane_watermark_resource_id(&plan.watermark);
        let existing = transaction
            .query_row(
                "SELECT watermark_json FROM matrix_data_plane_watermark
                 WHERE source_ref = ?1 AND fact_type = ?2 AND partition_ref = ?3",
                params![
                    plan.watermark.source_ref,
                    plan.watermark.fact_type,
                    plan.watermark.partition_ref,
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|json| serde_json::from_str::<MatrixDataPlaneWatermark>(&json))
            .transpose()?;
        if let Some(existing) = existing.as_ref() {
            if existing.last_batch_id == plan.watermark.last_batch_id {
                let existing = existing.clone();
                transaction.commit()?;
                return Ok(existing);
            }
        }
        let (_, revision, _) = prepare_matrix_resource_revision(
            &transaction,
            "data_plane_watermark",
            &resource_id,
            existing.is_some(),
            plan.expected_revision,
            true,
        )?;
        let mut watermark = plan.watermark.clone();
        watermark.revision = revision;
        upsert_data_plane_watermark(&transaction, &watermark)?;
        persist_matrix_resource_revision(
            &transaction,
            "data_plane_watermark",
            &resource_id,
            revision,
        )?;
        transaction.commit()?;
        Ok(watermark)
    }

    pub fn upsert_entity(
        &self,
        entity: &MatrixEntity,
    ) -> Result<MatrixEntity, MatrixSqliteRepositoryError> {
        Ok(self.upsert_entity_revisioned(entity, None, false)?.resource)
    }

    pub fn resource_revision_for_existing(
        &self,
        resource_kind: &str,
        resource_id: &str,
    ) -> Result<u64, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        Ok(connection
            .query_row(
                "SELECT revision FROM matrix_resource_revision
                 WHERE resource_kind = ?1 AND resource_id = ?2",
                params![resource_kind, resource_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(|revision| revision as u64)
            .unwrap_or(1))
    }

    pub fn upsert_entity_checked(
        &self,
        entity: &MatrixEntity,
        expected_revision: Option<u64>,
    ) -> Result<MatrixRevisioned<MatrixEntity>, MatrixSqliteRepositoryError> {
        self.upsert_entity_revisioned(entity, expected_revision, true)
    }

    fn upsert_entity_revisioned(
        &self,
        entity: &MatrixEntity,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> Result<MatrixRevisioned<MatrixEntity>, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing =
            find_entity_by_canonical(&transaction, &entity.entity_type, &entity.canonical_key)?;
        let resource_id = existing
            .as_ref()
            .map(|item| item.entity_id.as_str())
            .unwrap_or(entity.entity_id.as_str());
        let (previous_revision, revision, created) = prepare_matrix_resource_revision(
            &transaction,
            "entity",
            resource_id,
            existing.is_some(),
            expected_revision,
            enforce_revision,
        )?;
        let resource = upsert_entity(&transaction, entity)?;
        persist_matrix_resource_revision(&transaction, "entity", &resource.entity_id, revision)?;
        transaction.commit()?;
        Ok(MatrixRevisioned {
            resource,
            previous_revision,
            revision,
            created,
        })
    }

    pub fn get_entity(
        &self,
        entity_id: &str,
    ) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_entity(&connection, entity_id)
    }

    pub fn resolve_entity_by_source_key(
        &self,
        source_system: &str,
        source_key: &str,
    ) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_entity_by_source_key(&connection, source_system, source_key)
    }

    pub fn list_entities(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEntity>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_entities(&connection, limit)
    }

    pub fn get_ontology_pack(
        &self,
        ontology_id: &str,
    ) -> Result<Option<MatrixOntologyPack>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_ontology_pack(&connection, ontology_id)
    }

    pub fn propose_entity_match(
        &self,
        left_entity_id: &str,
        right_entity_id: &str,
    ) -> Result<matrix_core::MatrixEntityMatchCandidate, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let left = find_entity(&connection, left_entity_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(left_entity_id.to_string()))?;
        let right = find_entity(&connection, right_entity_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(right_entity_id.to_string()))?;
        let candidate = matrix_core::match_candidate(&left, &right).ok_or_else(|| {
            MatrixSqliteRepositoryError::NotFound(
                "entity match candidate below confidence threshold".to_string(),
            )
        })?;
        Ok(candidate)
    }

    pub fn decide_entity_conflict(
        &self,
        candidate_id: &str,
        survivor_entity_id: &str,
        retired_entity_id: &str,
        survivorship_rule: &str,
        notes: Option<String>,
    ) -> Result<matrix_core::MatrixEntityConflictDecision, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let survivor = find_entity(&transaction, survivor_entity_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(survivor_entity_id.to_string()))?;
        let retired = find_entity(&transaction, retired_entity_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(retired_entity_id.to_string()))?;
        let candidate = match find_entity_match_candidate(&transaction, candidate_id)? {
            Some(candidate) => candidate,
            None => {
                let candidate =
                    matrix_core::match_candidate(&survivor, &retired).ok_or_else(|| {
                        MatrixSqliteRepositoryError::NotFound(
                            "entity match candidate below confidence threshold".to_string(),
                        )
                    })?;
                if candidate.candidate_id != candidate_id {
                    return Err(MatrixSqliteRepositoryError::NotFound(
                        candidate_id.to_string(),
                    ));
                }
                insert_entity_match_candidate(&transaction, &candidate)?;
                candidate
            }
        };
        let candidate_pair_matches = (candidate.left_entity_id == survivor_entity_id
            && candidate.right_entity_id == retired_entity_id)
            || (candidate.left_entity_id == retired_entity_id
                && candidate.right_entity_id == survivor_entity_id);
        if !candidate_pair_matches {
            return Err(MatrixSqliteRepositoryError::InvalidScenario(
                "entity conflict decision does not match the candidate pair".to_string(),
            ));
        }
        let decision = matrix_core::MatrixEntityConflictDecision {
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
        insert_entity_conflict_decision(&transaction, &decision)?;
        transaction.commit()?;
        Ok(decision)
    }

    pub fn plan_metric_attention(
        &self,
        trigger_fact_type: &str,
        entity_scope: Option<String>,
        period: Option<String>,
        limit: usize,
    ) -> Result<MatrixMetricAttentionPlan, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut metric_ids = metrics_affected_by_fact_type(&connection, trigger_fact_type)?;
        metric_ids.extend(metric_ids_for_fact_type(&connection, trigger_fact_type)?);
        metric_ids.sort();
        metric_ids.dedup();
        let plan = build_metric_attention_plan(
            &connection,
            trigger_fact_type,
            entity_scope,
            period,
            metric_ids,
            limit,
        )?;
        Ok(plan)
    }

    pub fn materialize_metric_snapshot(
        &self,
        metric_ids: Vec<String>,
        scope_ref: Option<String>,
    ) -> Result<MatrixMetricSnapshot, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let snapshot = build_metric_snapshot(&connection, metric_ids, scope_ref)?;
        insert_metric_snapshot(&connection, &snapshot)?;
        Ok(snapshot)
    }

    pub fn upsert_relation(
        &self,
        relation: &MatrixRelation,
    ) -> Result<MatrixRelation, MatrixSqliteRepositoryError> {
        Ok(self
            .upsert_relation_revisioned(relation, None, false)?
            .resource)
    }

    pub fn upsert_relation_checked(
        &self,
        relation: &MatrixRelation,
        expected_revision: Option<u64>,
    ) -> Result<MatrixRevisioned<MatrixRelation>, MatrixSqliteRepositoryError> {
        self.upsert_relation_revisioned(relation, expected_revision, true)
    }

    fn upsert_relation_revisioned(
        &self,
        relation: &MatrixRelation,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> Result<MatrixRevisioned<MatrixRelation>, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = find_relation_by_key(
            &transaction,
            &relation.relation_type,
            &relation.from_entity_id,
            &relation.to_entity_id,
        )?;
        let resource_id = existing
            .as_ref()
            .map(|item| item.relation_id.as_str())
            .unwrap_or(relation.relation_id.as_str());
        let (previous_revision, revision, created) = prepare_matrix_resource_revision(
            &transaction,
            "relation",
            resource_id,
            existing.is_some(),
            expected_revision,
            enforce_revision,
        )?;
        let resource = upsert_relation(&transaction, relation)?;
        persist_matrix_resource_revision(
            &transaction,
            "relation",
            &resource.relation_id,
            revision,
        )?;
        transaction.commit()?;
        Ok(MatrixRevisioned {
            resource,
            previous_revision,
            revision,
            created,
        })
    }

    pub fn list_entity_relations(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> Result<Vec<MatrixRelation>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(MatrixSqliteRepositoryError::NotFound(entity_id.to_string()));
        }
        list_entity_relations(&connection, entity_id, limit)
    }

    pub fn impact_trace(
        &self,
        entity_id: &str,
        max_depth: usize,
    ) -> Result<MatrixImpactTrace, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(MatrixSqliteRepositoryError::NotFound(entity_id.to_string()));
        }
        build_impact_trace(&connection, entity_id, max_depth)
    }

    pub fn register_metric_definition(
        &self,
        definition: &MatrixMetricDefinition,
    ) -> Result<(), MatrixSqliteRepositoryError> {
        definition
            .query_plan()
            .validate()
            .map_err(|error| MatrixSqliteRepositoryError::InvalidMetricQuery(error.to_string()))?;
        let connection = self.executor.checkout()?;
        upsert_metric_definition(&connection, definition)
    }

    pub fn upsert_metric_dependency(
        &self,
        dependency: &MatrixMetricDependency,
    ) -> Result<MatrixMetricDependency, MatrixSqliteRepositoryError> {
        Ok(self
            .upsert_metric_dependency_revisioned(dependency, None, false)?
            .resource)
    }

    pub fn upsert_metric_dependency_checked(
        &self,
        dependency: &MatrixMetricDependency,
        expected_revision: Option<u64>,
    ) -> Result<MatrixRevisioned<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
        self.upsert_metric_dependency_revisioned(dependency, expected_revision, true)
    }

    fn upsert_metric_dependency_revisioned(
        &self,
        dependency: &MatrixMetricDependency,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> Result<MatrixRevisioned<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = find_metric_dependency_by_key(
            &transaction,
            &dependency.upstream_metric_id,
            &dependency.downstream_metric_id,
            &dependency.dependency_type,
        )?;
        let resource_id = existing
            .as_ref()
            .map(|item| item.dependency_id.as_str())
            .unwrap_or(dependency.dependency_id.as_str());
        let (previous_revision, revision, created) = prepare_matrix_resource_revision(
            &transaction,
            "metric_dependency",
            resource_id,
            existing.is_some(),
            expected_revision,
            enforce_revision,
        )?;
        let resource = upsert_metric_dependency(&transaction, dependency)?;
        persist_matrix_resource_revision(
            &transaction,
            "metric_dependency",
            &resource.dependency_id,
            revision,
        )?;
        transaction.commit()?;
        Ok(MatrixRevisioned {
            resource,
            previous_revision,
            revision,
            created,
        })
    }

    pub fn metric_lineage(
        &self,
        metric_id: &str,
        max_depth: usize,
    ) -> Result<MatrixMetricLineage, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        build_metric_lineage(&connection, metric_id, max_depth)
    }

    pub fn metrics_affected_by_fact_type(
        &self,
        fact_type: &str,
    ) -> Result<Vec<String>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        metrics_affected_by_fact_type(&connection, fact_type)
    }

    pub fn plan_compute_job_for_fact_type(
        &self,
        input: MatrixComputeJobInput,
    ) -> Result<MatrixComputePlan, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut affected_metric_ids = if input.metric_ids.is_empty() {
            metrics_affected_by_fact_type(&connection, &input.trigger_fact_type)?
        } else {
            input.metric_ids.clone()
        };
        if affected_metric_ids.is_empty() {
            affected_metric_ids = metric_ids_for_fact_type(&connection, &input.trigger_fact_type)?;
        }
        affected_metric_ids.sort();
        affected_metric_ids.dedup();
        let mut job = MatrixComputeJob::from_input(MatrixComputeJobInput {
            metric_ids: affected_metric_ids.clone(),
            ..input
        });
        job.priority = priority_for_compute_job(&job);
        upsert_compute_job(&connection, &job)?;
        Ok(MatrixComputePlan {
            job,
            affected_metric_ids,
            planned_at: Utc::now(),
        })
    }

    pub fn get_compute_job(
        &self,
        job_id: &str,
    ) -> Result<Option<MatrixComputeJob>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_compute_job(&connection, job_id)
    }

    pub fn run_compute_job(
        &self,
        job_id: &str,
    ) -> Result<MatrixComputeJob, MatrixSqliteRepositoryError> {
        let mut job = {
            let connection = self.executor.checkout()?;
            let mut job = find_compute_job(&connection, job_id)?
                .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(job_id.to_string()))?;
            job.status = "running".to_string();
            job.attempts += 1;
            job.updated_at = Utc::now();
            upsert_compute_job(&connection, &job)?;
            job
        };

        let filter = job.metric_ids.iter().cloned().collect::<BTreeSet<_>>();
        let recompute = self.recompute_metrics_with_filter(
            Some(&filter),
            job.entity_scope.as_deref(),
            job.period.as_deref(),
        )?;
        job.status = "completed".to_string();
        job.result_summary = serde_json::json!({
            "metric_ids": job.metric_ids.clone(),
            "metric_state_count": recompute.metric_state_count,
            "change_count": recompute.change_count,
            "attention_count": recompute.attention_count,
        });
        job.updated_at = Utc::now();
        let connection = self.executor.checkout()?;
        upsert_compute_job(&connection, &job)
    }

    pub fn ingest_fact(
        &self,
        fact: &MatrixFact,
    ) -> Result<MatrixAttentionItem, MatrixSqliteRepositoryError> {
        let attention = MatrixAttentionItem::from_fact(
            &fact.fact_id,
            &fact.fact_type,
            fact.entity_refs.first().cloned(),
            fact.confidence,
        );
        let connection = self.executor.checkout()?;
        connection.execute(
            r"INSERT OR REPLACE INTO matrix_fact (
                fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
                dimensions_json, measures_json, event_time, valid_from, valid_to,
                source_ref, confidence, raw_hash, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                fact.fact_id,
                fact.snapshot_id,
                fact.fact_type,
                serde_json::to_string(&fact.entity_refs)?,
                fact.metric_key,
                serde_json::to_string(&fact.dimensions)?,
                serde_json::to_string(&fact.measures)?,
                fact.event_time.to_rfc3339(),
                fact.valid_from.map(|value| value.to_rfc3339()),
                fact.valid_to.map(|value| value.to_rfc3339()),
                fact.source_ref,
                fact.confidence,
                fact.raw_hash,
                Utc::now().to_rfc3339(),
            ],
        )?;
        upsert_attention(&connection, &attention)?;
        Ok(attention)
    }

    pub fn upsert_source_pack(
        &self,
        source_pack: MatrixSourcePack,
    ) -> Result<MatrixSourcePack, MatrixSqliteRepositoryError> {
        Ok(self
            .upsert_source_pack_revisioned(source_pack, None, false)?
            .resource)
    }

    pub fn upsert_source_pack_checked(
        &self,
        source_pack: MatrixSourcePack,
        expected_revision: Option<u64>,
    ) -> Result<MatrixRevisioned<MatrixSourcePack>, MatrixSqliteRepositoryError> {
        self.upsert_source_pack_revisioned(source_pack, expected_revision, true)
    }

    fn upsert_source_pack_revisioned(
        &self,
        source_pack: MatrixSourcePack,
        expected_revision: Option<u64>,
        enforce_revision: bool,
    ) -> Result<MatrixRevisioned<MatrixSourcePack>, MatrixSqliteRepositoryError> {
        let mut source_pack = source_pack.normalized();
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = find_source_pack(&transaction, &source_pack.source_pack_id)?;
        if let Some(existing) = &existing {
            source_pack.created_at = existing.created_at.to_owned();
        }
        let (previous_revision, revision, created) = prepare_matrix_resource_revision(
            &transaction,
            "source_pack",
            &source_pack.source_pack_id,
            existing.is_some(),
            expected_revision,
            enforce_revision,
        )?;
        insert_source_pack(&transaction, &source_pack)?;
        persist_matrix_resource_revision(
            &transaction,
            "source_pack",
            &source_pack.source_pack_id,
            revision,
        )?;
        transaction.commit()?;
        Ok(MatrixRevisioned {
            resource: source_pack,
            previous_revision,
            revision,
            created,
        })
    }

    pub fn get_source_pack(
        &self,
        source_pack_id: &str,
    ) -> Result<Option<MatrixSourcePack>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_source_pack(&connection, source_pack_id)
    }

    pub fn list_source_packs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_source_packs(&connection, limit)
    }

    pub fn validate_source_pack(
        &self,
        source_pack_id: &str,
    ) -> Result<MatrixSourcePackValidation, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(source_pack_id.to_string()))?;
        Ok(source_pack.validate())
    }

    pub fn source_pack_delta_plan(
        &self,
        source_pack_id: &str,
    ) -> Result<MatrixSourceDeltaPlan, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(source_pack_id.to_string()))?;
        source_pack_delta_plan_for(&connection, &source_pack)
    }

    pub fn plan_connector_run(
        &self,
        source_pack_id: &str,
        input: MatrixConnectorRunInput,
    ) -> Result<MatrixConnectorRun, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(source_pack_id.to_string()))?;
        let delta_plan = source_pack_delta_plan_for(&connection, &source_pack)?;
        let run = MatrixConnectorRun::from_source_pack(&source_pack, &delta_plan, input);
        insert_connector_run(&connection, &run)?;
        Ok(run)
    }

    pub fn get_connector_run(
        &self,
        run_id: &str,
    ) -> Result<Option<MatrixConnectorRun>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_connector_run(&connection, run_id)
    }

    pub fn plan_source_snapshot(
        &self,
        source_pack_id: &str,
        resource_ref: Option<String>,
        estimated_rows: Option<u64>,
    ) -> Result<MatrixSourceSnapshotPlan, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(source_pack_id.to_string()))?;
        let delta_plan = source_pack_delta_plan_for(&connection, &source_pack)?;
        let validation = source_pack.validate();
        Ok(MatrixSourceSnapshotPlan {
            source_pack_id: source_pack.source_pack_id.clone(),
            source_ref: resource_ref.unwrap_or_else(|| source_pack.source_name.clone()),
            source_kind: source_kind_for_access_mode(&source_pack.access_mode),
            access_mode: source_pack.access_mode,
            refresh_mode: source_pack.refresh_mode,
            estimated_rows: estimated_rows.unwrap_or(0),
            fact_types: delta_plan.fact_types,
            affected_metric_ids: delta_plan.affected_metric_ids,
            quality_warnings: validation.warnings,
            planned_at: Utc::now(),
        })
    }

    pub fn upsert_source_snapshot(
        &self,
        snapshot: MatrixSourceSnapshot,
    ) -> Result<MatrixSourceSnapshot, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        insert_source_snapshot(&connection, &snapshot)?;
        Ok(snapshot)
    }

    pub fn create_source_snapshot(
        &self,
        input: MatrixSourceSnapshotInput,
    ) -> Result<MatrixSourceSnapshot, MatrixSqliteRepositoryError> {
        self.upsert_source_snapshot(MatrixSourceSnapshot::from_input(input))
    }

    pub fn get_source_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<MatrixSourceSnapshot>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_source_snapshot(&connection, snapshot_id)
    }

    pub fn list_source_snapshots(
        &self,
        source_pack_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MatrixSourceSnapshot>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_source_snapshots(&connection, source_pack_id, limit)
    }

    /// Persist an immutable scenario specification. The referenced source
    /// snapshot must already exist, so a scenario can never be detached from
    /// the exact data it was designed to explore.
    pub fn create_scenario_spec(
        &self,
        spec: MatrixScenarioSpec,
    ) -> Result<MatrixScenarioSpec, MatrixSqliteRepositoryError> {
        spec.validate()
            .map_err(MatrixSqliteRepositoryError::InvalidScenario)?;
        let connection = self.executor.checkout()?;
        let snapshot_id = &spec.base_snapshot.snapshot_id;
        if find_source_snapshot(&connection, snapshot_id)?.is_none() {
            return Err(MatrixSqliteRepositoryError::NotFound(format!(
                "source snapshot for scenario: {snapshot_id}"
            )));
        }
        insert_scenario_spec(&connection, &spec)?;
        Ok(spec)
    }

    pub fn get_scenario_spec(
        &self,
        scenario_id: &str,
    ) -> Result<Option<MatrixScenarioSpec>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_scenario_spec(&connection, scenario_id)
    }

    pub fn list_scenario_specs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixScenarioSpec>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_scenario_specs(&connection, limit)
    }

    /// Start a scenario against the exact immutable Specification and source
    /// snapshot. A caller supplies only parameters; it cannot swap a snapshot
    /// or alter scenario assumptions after the specification was recorded.
    pub fn start_scenario_run(
        &self,
        scenario_id: &str,
        parameters: Value,
    ) -> Result<MatrixScenarioRun, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let spec = find_scenario_spec(&connection, scenario_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(scenario_id.to_string()))?;
        let run = MatrixScenarioRun::start(&spec, parameters);
        run.validate()
            .map_err(MatrixSqliteRepositoryError::InvalidScenario)?;
        insert_scenario_run(&connection, &run)?;
        Ok(run)
    }

    pub fn get_scenario_run(
        &self,
        run_id: &str,
    ) -> Result<Option<MatrixScenarioRun>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_scenario_run(&connection, run_id)
    }

    pub fn list_scenario_runs(
        &self,
        scenario_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MatrixScenarioRun>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_scenario_runs(&connection, scenario_id, limit)
    }

    /// Complete exactly one running scenario. Results are rejected unless they
    /// preserve the run's scenario identity and immutable simulated boundary.
    pub fn complete_scenario_run(
        &self,
        result: MatrixScenarioResult,
    ) -> Result<MatrixScenarioResult, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut run = find_scenario_run(&transaction, &result.run_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(result.run_id.clone()))?;
        if run.status != MatrixScenarioRunStatus::Running {
            return Err(MatrixSqliteRepositoryError::ScenarioState(format!(
                "scenario run is not running: {}",
                run.run_id
            )));
        }
        result
            .validate_for_run(&run)
            .map_err(MatrixSqliteRepositoryError::InvalidScenario)?;
        run.status = MatrixScenarioRunStatus::Succeeded;
        run.completed_at = Some(result.completed_at);
        update_scenario_run(&transaction, &run)?;
        insert_scenario_result(&transaction, &result)?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn get_scenario_result(
        &self,
        run_id: &str,
    ) -> Result<Option<MatrixScenarioResult>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_scenario_result(&connection, run_id)
    }

    pub fn apply_source_snapshot_rows(
        &self,
        source_pack_id: &str,
        mut snapshot: MatrixSourceSnapshot,
        rows: &[Value],
    ) -> Result<MatrixSourceSnapshotApplyReport, MatrixSqliteRepositoryError> {
        let mut attention_count = 0usize;
        let mut relation_count = 0usize;
        let mut fact_refs = Vec::new();
        let mut warnings = BTreeSet::new();
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let source_pack = find_source_pack(&transaction, source_pack_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(source_pack_id.to_string()))?;
        insert_source_snapshot(&transaction, &snapshot)?;

        for row in rows {
            let row_hash = stable_json_hash(row);
            for mapping in &source_pack.entity_mappings {
                if let Some(source_key) = row_value(row, &mapping.source_key_field) {
                    let entity = MatrixEntity::from_input(matrix_core::MatrixEntityInput {
                        entity_id: Some(stable_entity_id(
                            &source_pack.source_name,
                            &mapping.matrix_entity_type,
                            &source_key,
                        )),
                        entity_type: mapping.matrix_entity_type.clone(),
                        canonical_key: source_key.clone(),
                        display_name: Some(source_key.clone()),
                        source_keys: vec![MatrixSourceKey {
                            source_system: source_pack.source_name.clone(),
                            source_key,
                            source_ref: Some(format!("{}/row/{row_hash}", snapshot.reference())),
                        }],
                        attributes: row.clone(),
                        confidence: Some(snapshot.confidence),
                    });
                    upsert_entity(&transaction, &entity)?;
                }
            }

            for mapping in &source_pack.fact_mappings {
                let source_ref = format!("{}/row/{row_hash}", snapshot.reference());
                let entity_refs = mapping
                    .entity_ref_fields
                    .iter()
                    .filter_map(|field| {
                        row_value(row, field).map(|value| {
                            stable_entity_reference_for_field(&source_pack, field, &value)
                        })
                    })
                    .collect::<Vec<_>>();
                let measures = pick_fields(row, &mapping.measure_fields);
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
                    measures,
                    event_time: mapping
                        .event_time_field
                        .as_deref()
                        .and_then(|field| row_value(row, field))
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
                        .map(|value| value.with_timezone(&Utc))
                        .or(Some(snapshot.captured_at)),
                    valid_from: None,
                    valid_to: None,
                    source_ref: Some(source_ref),
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
                transaction.execute(
                    r"INSERT OR REPLACE INTO matrix_fact (
                        fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
                        dimensions_json, measures_json, event_time, valid_from, valid_to,
                        source_ref, confidence, raw_hash, created_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        fact.fact_id,
                        fact.snapshot_id,
                        fact.fact_type,
                        serde_json::to_string(&fact.entity_refs)?,
                        fact.metric_key,
                        serde_json::to_string(&fact.dimensions)?,
                        serde_json::to_string(&fact.measures)?,
                        fact.event_time.to_rfc3339(),
                        fact.valid_from.map(|value| value.to_rfc3339()),
                        fact.valid_to.map(|value| value.to_rfc3339()),
                        fact.source_ref,
                        fact.confidence,
                        fact.raw_hash,
                        Utc::now().to_rfc3339(),
                    ],
                )?;
                upsert_attention(&transaction, &attention)?;
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
                let Some(to_entity_id) =
                    stable_entity_id_for_field(&source_pack, &mapping.to_source_key_field, &to_key)
                else {
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
                upsert_relation(&transaction, &relation)?;
                relation_count += 1;
            }
        }

        if source_pack.fact_mappings.is_empty() {
            warnings.insert("source_pack_has_no_fact_mappings".to_string());
        }

        let report = MatrixSourceSnapshotApplyReport {
            snapshot_id: snapshot.snapshot_id.clone(),
            source_pack_id: source_pack_id.to_string(),
            status: "applied".to_string(),
            row_count: rows.len() as u64,
            fact_count: fact_refs.len(),
            relation_count,
            attention_count,
            warnings: warnings.into_iter().collect(),
            fact_refs,
            applied_at: Utc::now(),
        };
        let mut metadata = snapshot.metadata.as_object().cloned().unwrap_or_default();
        metadata.insert("chunk_receipt".to_string(), serde_json::to_value(&report)?);
        snapshot.metadata = Value::Object(metadata);
        insert_source_snapshot(&transaction, &snapshot)?;
        transaction.commit()?;
        Ok(report)
    }

    pub fn list_attention(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut statement = connection.prepare(
            r"SELECT attention_json
              FROM matrix_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn list_facts(&self, limit: usize) -> Result<Vec<MatrixFact>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_facts(&connection, limit)
    }

    pub fn recall_facts(
        &self,
        query: &MatrixRecallQuery,
    ) -> Result<Vec<MatrixFact>, MatrixSqliteRepositoryError> {
        if !query.is_authorized() {
            return Ok(Vec::new());
        }
        let connection = self.executor.checkout()?;
        recall_facts(&connection, query)
    }

    pub fn recompute_metrics(
        &self,
    ) -> Result<MatrixMetricRecomputeResult, MatrixSqliteRepositoryError> {
        self.recompute_metrics_with_filter(None, None, None)
    }

    pub fn recompute_metrics_for_metric_ids(
        &self,
        metric_ids: &[String],
    ) -> Result<MatrixMetricRecomputeResult, MatrixSqliteRepositoryError> {
        let filter = metric_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.recompute_metrics_with_filter(Some(&filter), None, None)
    }

    fn recompute_metrics_with_filter(
        &self,
        metric_filter: Option<&BTreeSet<String>>,
        entity_scope: Option<&str>,
        period: Option<&str>,
    ) -> Result<MatrixMetricRecomputeResult, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let query_results = metric_query_results(&connection, metric_filter, entity_scope, period)?;

        let mut states = Vec::new();
        let mut changes = Vec::new();
        let mut attention = Vec::new();
        for result in query_results {
            let previous = latest_metric_state(
                &connection,
                &result.metric_id,
                &result.entity_scope,
                &result.period,
            )?;
            let previous_value = previous.as_ref().map(|state| state.value);
            let value = result.value;
            let delta = previous_value.map_or(value, |previous| value - previous);
            let delta_ratio = previous_value.and_then(|previous| {
                if previous.abs() > f64::EPSILON {
                    Some(delta / previous)
                } else {
                    None
                }
            });
            let state = MatrixMetricState {
                state_id: format!("metric-state-{}", uuid::Uuid::new_v4()),
                metric_id: result.metric_id.clone(),
                entity_scope: result.entity_scope.clone(),
                period: result.period.clone(),
                value,
                previous_value,
                delta,
                delta_ratio,
                status: MatrixMetricState::status_for_delta(delta),
                computed_at: Utc::now(),
                input_fact_refs: result.input_fact_refs.clone(),
                confidence: result.confidence,
            };
            insert_metric_state(&connection, &state)?;
            states.push(state.clone());

            if delta.abs() > f64::EPSILON {
                let change = MatrixChangeEvent {
                    change_id: format!("change-{}", uuid::Uuid::new_v4()),
                    change_type: "metric_delta".to_string(),
                    entity_ref: result.entity_scope.clone(),
                    metric_id: Some(result.metric_id.clone()),
                    from_value: previous_value.map(Value::from),
                    to_value: Some(Value::from(value)),
                    delta,
                    period: result.period.clone(),
                    detected_at: Utc::now(),
                    source_fact_refs: result.input_fact_refs.clone(),
                    severity_hint: MatrixChangeEvent::severity_for_delta(delta),
                };
                insert_change_event(&connection, &change)?;
                let item = attention_from_change(&change, &state);
                upsert_attention(&connection, &item)?;
                changes.push(change);
                attention.push(item);
            }
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

    pub fn list_metric_definitions(
        &self,
    ) -> Result<Vec<MatrixMetricDefinition>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut statement = connection.prepare(
            r"SELECT definition_json
              FROM matrix_metric_definition
              ORDER BY metric_id ASC",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn metric_states(
        &self,
        metric_id: &str,
    ) -> Result<Vec<MatrixMetricState>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut statement = connection.prepare(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC",
        )?;
        let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn list_changes(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixChangeEvent>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let mut statement = connection.prepare(
            r"SELECT change_json
              FROM matrix_change_event
              ORDER BY detected_at DESC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn build_evidence_packet(
        &self,
        packet_id: Option<&str>,
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        if let Some(packet_id) = packet_id {
            if let Some(existing) = find_evidence_packet(&connection, packet_id)? {
                return Ok(existing);
            }
        }
        let attention = match attention_id {
            Some(id) => Some(
                find_attention(&connection, id)?
                    .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(id.to_string()))?,
            ),
            None => latest_attention(&connection)?,
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
        if let Some(item) = attention {
            packet.confidence = item.confidence.min(0.75);
            packet.business_context = serde_json::json!({
                "business_domain": item.business_domain,
                "entity_ref": item.entity_ref,
                "period": item.period,
                "priority_score": item.priority_score,
                "reason_codes": item.reason_codes,
                "owner_roles": item.owner_roles,
            });
            for reference in item.linked_changes {
                if let Some(change_id) = reference.strip_prefix("matrix:change:") {
                    if let Some(change) = find_change(&connection, change_id)? {
                        packet.change_evidence.push(serde_json::to_value(&change)?);
                        if let Some(metric_id) = change.metric_id.as_deref() {
                            if let Some(state) =
                                latest_metric_state_for_metric(&connection, metric_id)?
                            {
                                packet.metric_evidence.push(serde_json::to_value(&state)?);
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
        insert_evidence_packet_once(&connection, &packet)?;
        find_evidence_packet(&connection, &packet.packet_id)?.ok_or_else(|| {
            MatrixSqliteRepositoryError::NotFound(format!(
                "canonical evidence packet {} disappeared after insert",
                packet.packet_id
            ))
        })
    }

    pub fn insert_ai_harness_evidence_packet(
        &self,
        packet: &MatrixEvidencePacket,
    ) -> Result<MatrixEvidencePacket, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        insert_evidence_packet(&connection, packet)?;
        Ok(packet.clone())
    }

    pub fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_evidence_packet(&connection, packet_id)
    }

    pub fn list_evidence_packets(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_evidence_packets(&connection, limit)
    }

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let packet = find_evidence_packet(&connection, packet_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(packet_id.to_string()))?;
        let decision = MatrixQualityGateDecision::for_evidence_packet(&packet);
        insert_quality_gate(&connection, &decision)?;
        Ok(decision)
    }

    pub fn evaluate_evidence_quality_with_gate_id(
        &self,
        packet_id: &str,
        gate_id: &str,
    ) -> Result<MatrixQualityGateDecision, MatrixSqliteRepositoryError> {
        let mut connection = self.executor.checkout()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some(existing) = find_quality_gate(&transaction, gate_id)? {
            if existing.target_ref == format!("matrix:evidence:{packet_id}") {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(MatrixSqliteRepositoryError::InvalidScenario(format!(
                "quality gate id {gate_id} is bound to another evidence packet"
            )));
        }
        let packet = find_evidence_packet(&transaction, packet_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(packet_id.to_string()))?;
        let mut decision = MatrixQualityGateDecision::for_evidence_packet(&packet);
        decision.gate_id = gate_id.to_string();
        insert_quality_gate(&transaction, &decision)?;
        transaction.commit()?;
        Ok(decision)
    }

    pub fn get_quality_gate(
        &self,
        gate_id: &str,
    ) -> Result<Option<MatrixQualityGateDecision>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        find_quality_gate(&connection, gate_id)
    }

    pub fn list_data_plane_watermarks(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixDataPlaneWatermark>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        list_data_plane_watermarks(&connection, limit)
    }

    pub fn get_data_plane_watermark(
        &self,
        source_ref: &str,
        fact_type: &str,
        partition_ref: &str,
    ) -> Result<Option<MatrixDataPlaneWatermark>, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let json = connection
            .query_row(
                "SELECT watermark_json FROM matrix_data_plane_watermark
                 WHERE source_ref = ?1 AND fact_type = ?2 AND partition_ref = ?3",
                params![source_ref, fact_type, partition_ref],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(json) = json else {
            return Ok(None);
        };
        let mut watermark = serde_json::from_str::<MatrixDataPlaneWatermark>(&json)?;
        let resource_id = data_plane_watermark_resource_id(&watermark);
        watermark.revision = connection
            .query_row(
                "SELECT revision FROM matrix_resource_revision
                 WHERE resource_kind = 'data_plane_watermark' AND resource_id = ?1",
                params![resource_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map_or_else(|| watermark.revision.max(1), |revision| revision as u64);
        Ok(Some(watermark))
    }
}

pub(crate) fn initialize_schema(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        r"CREATE TABLE IF NOT EXISTS matrix_schema (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        INSERT INTO matrix_schema (id, schema_version, updated_at)
        VALUES (1, 21, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
            schema_version = CASE
                WHEN matrix_schema.schema_version < excluded.schema_version
                THEN excluded.schema_version
                ELSE matrix_schema.schema_version
            END,
            updated_at = excluded.updated_at;

        CREATE TABLE IF NOT EXISTS matrix_entity (
            entity_id TEXT PRIMARY KEY,
            entity_type TEXT NOT NULL,
            canonical_key TEXT NOT NULL,
            display_name TEXT NOT NULL,
            source_keys_json TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            entity_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(entity_type, canonical_key)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_type
            ON matrix_entity(entity_type, canonical_key);

        CREATE TABLE IF NOT EXISTS matrix_entity_source_key (
            source_system TEXT NOT NULL,
            source_key TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            source_ref TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY(source_system, source_key),
            FOREIGN KEY(entity_id) REFERENCES matrix_entity(entity_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_source_entity
            ON matrix_entity_source_key(entity_id);

        CREATE TABLE IF NOT EXISTS matrix_relation (
            relation_id TEXT PRIMARY KEY,
            relation_type TEXT NOT NULL,
            from_entity_id TEXT NOT NULL,
            to_entity_id TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            relation_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(relation_type, from_entity_id, to_entity_id),
            FOREIGN KEY(from_entity_id) REFERENCES matrix_entity(entity_id) ON DELETE CASCADE,
            FOREIGN KEY(to_entity_id) REFERENCES matrix_entity(entity_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_relation_from
            ON matrix_relation(from_entity_id, relation_type);
        CREATE INDEX IF NOT EXISTS idx_matrix_relation_to
            ON matrix_relation(to_entity_id, relation_type);

        CREATE TABLE IF NOT EXISTS matrix_fact (
            fact_id TEXT PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            fact_type TEXT NOT NULL,
            entity_refs_json TEXT NOT NULL,
            metric_key TEXT,
            dimensions_json TEXT NOT NULL,
            measures_json TEXT NOT NULL,
            event_time TEXT NOT NULL,
            valid_from TEXT,
            valid_to TEXT,
            source_ref TEXT,
            confidence REAL NOT NULL,
            raw_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_type ON matrix_fact(fact_type);
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_snapshot ON matrix_fact(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_metric_time
            ON matrix_fact(metric_key, event_time ASC, fact_id ASC);
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_metric_scope_period_time
            ON matrix_fact(
                metric_key,
                COALESCE(json_extract(entity_refs_json, '$[0]'), 'enterprise'),
                COALESCE(
                    json_extract(dimensions_json, '$.period'),
                    json_extract(dimensions_json, '$.week'),
                    'current'
                ),
                event_time ASC,
                fact_id ASC
            );
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_recall
            ON matrix_fact(snapshot_id, event_time DESC, fact_id ASC);

        CREATE TABLE IF NOT EXISTS matrix_attention_item (
            attention_id TEXT PRIMARY KEY,
            priority_score REAL NOT NULL,
            status TEXT NOT NULL,
            attention_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_attention_priority
            ON matrix_attention_item(priority_score DESC, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_evidence_packet (
            packet_id TEXT PRIMARY KEY,
            attention_id TEXT,
            packet_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matrix_quality_gate (
            gate_id TEXT PRIMARY KEY,
            target_ref TEXT NOT NULL,
            gate_type TEXT NOT NULL,
            decision TEXT NOT NULL,
            score REAL NOT NULL,
            gate_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_quality_gate_target
            ON matrix_quality_gate(target_ref, created_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_definition (
            metric_id TEXT PRIMARY KEY,
            definition_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matrix_metric_state (
            state_id TEXT PRIMARY KEY,
            metric_id TEXT NOT NULL,
            entity_scope TEXT NOT NULL,
            period TEXT NOT NULL,
            value REAL NOT NULL,
            previous_value REAL,
            delta REAL NOT NULL,
            status TEXT NOT NULL,
            state_json TEXT NOT NULL,
            computed_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_state_lookup
            ON matrix_metric_state(metric_id, entity_scope, period, computed_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_dependency (
            dependency_id TEXT PRIMARY KEY,
            upstream_metric_id TEXT NOT NULL,
            downstream_metric_id TEXT NOT NULL,
            dependency_type TEXT NOT NULL,
            confidence REAL NOT NULL,
            dependency_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(upstream_metric_id, downstream_metric_id, dependency_type)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_dependency_upstream
            ON matrix_metric_dependency(upstream_metric_id, downstream_metric_id);
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_dependency_downstream
            ON matrix_metric_dependency(downstream_metric_id, upstream_metric_id);

        CREATE TABLE IF NOT EXISTS matrix_compute_job (
            job_id TEXT PRIMARY KEY,
            trigger_fact_type TEXT NOT NULL,
            status TEXT NOT NULL,
            priority REAL NOT NULL,
            job_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_compute_job_status
            ON matrix_compute_job(status, priority DESC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_compute_job_fact_type
            ON matrix_compute_job(trigger_fact_type, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_change_event (
            change_id TEXT PRIMARY KEY,
            metric_id TEXT,
            entity_ref TEXT NOT NULL,
            period TEXT NOT NULL,
            delta REAL NOT NULL,
            severity_hint TEXT NOT NULL,
            change_json TEXT NOT NULL,
            detected_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_change_detected
            ON matrix_change_event(detected_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_source_pack (
            source_pack_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            access_mode TEXT NOT NULL,
            refresh_mode TEXT NOT NULL,
            source_pack_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_source_pack_source
            ON matrix_source_pack(source_name, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_resource_revision (
            resource_kind TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(resource_kind, resource_id)
        );

        CREATE TABLE IF NOT EXISTS matrix_data_plane_watermark (
            source_ref TEXT NOT NULL,
            fact_type TEXT NOT NULL,
            partition_ref TEXT NOT NULL,
            high_watermark TEXT NOT NULL,
            last_batch_id TEXT NOT NULL,
            watermark_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(source_ref, fact_type, partition_ref)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_data_plane_watermark_updated
            ON matrix_data_plane_watermark(updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_connector_run (
            run_id TEXT PRIMARY KEY,
            source_pack_id TEXT NOT NULL,
            connector_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            run_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_connector_run_source
            ON matrix_connector_run(source_pack_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_connector_run_status
            ON matrix_connector_run(status, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_source_snapshot (
            snapshot_id TEXT PRIMARY KEY,
            source_pack_id TEXT,
            source_system TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            resource_ref TEXT,
            row_count INTEGER NOT NULL,
            checksum TEXT,
            snapshot_json TEXT NOT NULL,
            captured_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_source_snapshot_pack
            ON matrix_source_snapshot(source_pack_id, captured_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_source_snapshot_source
            ON matrix_source_snapshot(source_system, captured_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_ontology_pack (
            ontology_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            version TEXT NOT NULL,
            pack_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matrix_entity_match_candidate (
            candidate_id TEXT PRIMARY KEY,
            left_entity_id TEXT NOT NULL,
            right_entity_id TEXT NOT NULL,
            confidence REAL NOT NULL,
            status TEXT NOT NULL,
            candidate_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_match_candidate_entities
            ON matrix_entity_match_candidate(left_entity_id, right_entity_id);

        CREATE TABLE IF NOT EXISTS matrix_entity_conflict_decision (
            decision_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            survivor_entity_id TEXT NOT NULL,
            retired_entity_id TEXT NOT NULL,
            decision_json TEXT NOT NULL,
            decided_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_conflict_candidate
            ON matrix_entity_conflict_decision(candidate_id, decided_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_snapshot (
            snapshot_id TEXT PRIMARY KEY,
            scope_ref TEXT NOT NULL,
            metric_ids_json TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_snapshot_scope
            ON matrix_metric_snapshot(scope_ref, created_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_scenario_spec (
            scenario_id TEXT PRIMARY KEY,
            source_snapshot_id TEXT NOT NULL,
            transform_ref TEXT NOT NULL,
            spec_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(source_snapshot_id) REFERENCES matrix_source_snapshot(snapshot_id)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_scenario_spec_snapshot
            ON matrix_scenario_spec(source_snapshot_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_scenario_run (
            run_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL,
            source_snapshot_id TEXT NOT NULL,
            status TEXT NOT NULL,
            run_json TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY(scenario_id) REFERENCES matrix_scenario_spec(scenario_id),
            FOREIGN KEY(source_snapshot_id) REFERENCES matrix_source_snapshot(snapshot_id)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_scenario_run_scenario
            ON matrix_scenario_run(scenario_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_scenario_run_status
            ON matrix_scenario_run(status, started_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_scenario_result (
            result_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL UNIQUE,
            scenario_id TEXT NOT NULL,
            boundary TEXT NOT NULL CHECK (boundary = 'simulated'),
            result_json TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES matrix_scenario_run(run_id),
            FOREIGN KEY(scenario_id) REFERENCES matrix_scenario_spec(scenario_id)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_scenario_result_scenario
            ON matrix_scenario_result(scenario_id, completed_at DESC);

        ",
    )
}

fn schema_version(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row(
        "SELECT schema_version FROM matrix_schema WHERE id = 1",
        [],
        |row| row.get(0),
    )
}

fn count_table(connection: &Connection, table: &str) -> rusqlite::Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map(|value| value as u64)
}

fn export_json_records(
    connection: &Connection,
    table: &str,
    id_column: &str,
    payload_column: &str,
) -> Result<BTreeMap<String, Value>, MatrixSqliteRepositoryError> {
    let sql = format!("SELECT {id_column}, {payload_column} FROM {table} ORDER BY {id_column} ASC");
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut records = BTreeMap::new();
    for row in rows {
        let (id, payload) = row?;
        let payload = serde_json::from_str(&payload)?;
        records.insert(
            id,
            canonicalize_payload(table, payload)
                .map_err(|error| MatrixSqliteRepositoryError::Migration(error.to_string()))?,
        );
    }
    Ok(records)
}

fn export_watermark_records(
    connection: &Connection,
) -> Result<BTreeMap<String, Value>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT source_ref, fact_type, partition_ref, watermark_json \
         FROM matrix_data_plane_watermark \
         ORDER BY source_ref ASC, fact_type ASC, partition_ref ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut records = BTreeMap::new();
    for row in rows {
        let (source_ref, fact_type, partition_ref, payload) = row?;
        let payload = serde_json::from_str(&payload)?;
        records.insert(
            format!("{source_ref}\0{fact_type}\0{partition_ref}"),
            canonicalize_payload("matrix_data_plane_watermark", payload)
                .map_err(|error| MatrixSqliteRepositoryError::Migration(error.to_string()))?,
        );
    }
    Ok(records)
}

fn export_revisions(
    connection: &Connection,
) -> Result<BTreeMap<String, u64>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT resource_kind, resource_id, revision \
         FROM matrix_resource_revision \
         ORDER BY resource_kind ASC, resource_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)? as u64,
        ))
    })?;
    let mut revisions = BTreeMap::new();
    for row in rows {
        let (resource_kind, resource_id, revision) = row?;
        revisions.insert(format!("{resource_kind}\0{resource_id}"), revision);
    }
    Ok(revisions)
}

fn insert_ontology_pack(
    connection: &Connection,
    pack: &MatrixOntologyPack,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_ontology_pack (
            ontology_id, domain, version, pack_json, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            pack.ontology_id,
            pack.domain,
            pack.version,
            serde_json::to_string(pack)?,
            Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_ontology_pack(
    connection: &Connection,
    ontology_id: &str,
) -> Result<Option<MatrixOntologyPack>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT pack_json FROM matrix_ontology_pack WHERE ontology_id = ?1",
            params![ontology_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn insert_entity_match_candidate(
    connection: &Connection,
    candidate: &matrix_core::MatrixEntityMatchCandidate,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_entity_match_candidate (
            candidate_id, left_entity_id, right_entity_id, confidence, status,
            candidate_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            candidate.candidate_id,
            candidate.left_entity_id,
            candidate.right_entity_id,
            candidate.confidence,
            candidate.status,
            serde_json::to_string(candidate)?,
            candidate.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_entity_match_candidate(
    connection: &Connection,
    candidate_id: &str,
) -> Result<Option<matrix_core::MatrixEntityMatchCandidate>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT candidate_json FROM matrix_entity_match_candidate WHERE candidate_id = ?1",
            params![candidate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn insert_entity_conflict_decision(
    connection: &Connection,
    decision: &matrix_core::MatrixEntityConflictDecision,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_entity_conflict_decision (
            decision_id, candidate_id, survivor_entity_id, retired_entity_id,
            decision_json, decided_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            decision.decision_id,
            decision.candidate_id,
            decision.survivor_entity_id,
            decision.retired_entity_id,
            serde_json::to_string(decision)?,
            decision.decided_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn prepare_matrix_resource_revision(
    connection: &Connection,
    resource_kind: &str,
    resource_id: &str,
    exists: bool,
    expected_revision: Option<u64>,
    enforce_revision: bool,
) -> Result<(Option<u64>, u64, bool), MatrixSqliteRepositoryError> {
    let stored_revision = connection
        .query_row(
            "SELECT revision FROM matrix_resource_revision
             WHERE resource_kind = ?1 AND resource_id = ?2",
            params![resource_kind, resource_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map(|revision| revision as u64);
    let actual = if exists {
        stored_revision.or(Some(1))
    } else {
        None
    };
    if enforce_revision {
        let matches = if exists {
            expected_revision == actual
        } else {
            expected_revision.is_none()
        };
        if !matches {
            return Err(MatrixSqliteRepositoryError::RevisionConflict {
                resource_ref: format!("matrix:{resource_kind}:{resource_id}"),
                expected: expected_revision,
                actual,
            });
        }
    }
    let revision = actual.unwrap_or_default().checked_add(1).ok_or_else(|| {
        MatrixSqliteRepositoryError::RevisionConflict {
            resource_ref: format!("matrix:{resource_kind}:{resource_id}"),
            expected: expected_revision,
            actual,
        }
    })?;
    Ok((actual, revision, !exists))
}

fn persist_matrix_resource_revision(
    connection: &Connection,
    resource_kind: &str,
    resource_id: &str,
    revision: u64,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        "INSERT INTO matrix_resource_revision (
            resource_kind, resource_id, revision, updated_at
         ) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(resource_kind, resource_id) DO UPDATE SET
            revision = excluded.revision,
            updated_at = excluded.updated_at",
        params![
            resource_kind,
            resource_id,
            revision as i64,
            Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn insert_source_pack(
    connection: &Connection,
    source_pack: &MatrixSourcePack,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_source_pack (
            source_pack_id, source_name, access_mode, refresh_mode,
            source_pack_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            source_pack.source_pack_id,
            source_pack.source_name,
            source_pack.access_mode,
            source_pack.refresh_mode,
            serde_json::to_string(source_pack)?,
            source_pack.created_at.to_rfc3339(),
            source_pack.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_source_pack(
    connection: &Connection,
    source_pack_id: &str,
) -> Result<Option<MatrixSourcePack>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT source_pack_json FROM matrix_source_pack WHERE source_pack_id = ?1",
            params![source_pack_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_source_packs(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixSourcePack>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT source_pack_json
          FROM matrix_source_pack
          ORDER BY updated_at DESC, source_pack_id ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn source_pack_delta_plan_for(
    connection: &Connection,
    source_pack: &MatrixSourcePack,
) -> Result<MatrixSourceDeltaPlan, MatrixSqliteRepositoryError> {
    let mut fact_types = source_pack
        .fact_mappings
        .iter()
        .map(|mapping| mapping.fact_type.clone())
        .collect::<Vec<_>>();
    fact_types.sort();
    fact_types.dedup();
    let mut affected_metric_ids = Vec::new();
    for fact_type in &fact_types {
        affected_metric_ids.extend(metrics_affected_by_fact_type(connection, fact_type)?);
        affected_metric_ids.extend(metric_ids_for_fact_type(connection, fact_type)?);
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

fn insert_connector_run(
    connection: &Connection,
    run: &MatrixConnectorRun,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_connector_run (
            run_id, source_pack_id, connector_kind, status, run_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run.run_id,
            run.source_pack_id,
            run.connector_kind,
            run.status,
            serde_json::to_string(run)?,
            run.created_at.to_rfc3339(),
            run.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_connector_run(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<MatrixConnectorRun>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT run_json FROM matrix_connector_run WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn insert_source_snapshot(
    connection: &Connection,
    snapshot: &MatrixSourceSnapshot,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_source_snapshot (
            snapshot_id, source_pack_id, source_system, source_kind, resource_ref,
            row_count, checksum, snapshot_json, captured_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            snapshot.snapshot_id,
            snapshot.source_pack_id,
            snapshot.source_system,
            format!("{:?}", snapshot.source_kind).to_ascii_lowercase(),
            snapshot.resource_ref,
            snapshot.row_count as i64,
            snapshot.checksum,
            serde_json::to_string(snapshot)?,
            snapshot.captured_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_source_snapshot(
    connection: &Connection,
    snapshot_id: &str,
) -> Result<Option<MatrixSourceSnapshot>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT snapshot_json FROM matrix_source_snapshot WHERE snapshot_id = ?1",
            params![snapshot_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_source_snapshots(
    connection: &Connection,
    source_pack_id: Option<&str>,
    limit: usize,
) -> Result<Vec<MatrixSourceSnapshot>, MatrixSqliteRepositoryError> {
    let limit = limit.clamp(1, 500);
    let mut snapshots = Vec::new();
    if let Some(source_pack_id) = source_pack_id {
        let mut statement = connection.prepare(
            r"SELECT snapshot_json
              FROM matrix_source_snapshot
              WHERE source_pack_id = ?1
              ORDER BY captured_at DESC, snapshot_id ASC
              LIMIT ?2",
        )?;
        let rows = statement.query_map(params![source_pack_id, limit as i64], |row| {
            row.get::<_, String>(0)
        })?;
        for row in rows {
            snapshots.push(serde_json::from_str(&row?)?);
        }
    } else {
        let mut statement = connection.prepare(
            r"SELECT snapshot_json
              FROM matrix_source_snapshot
              ORDER BY captured_at DESC, snapshot_id ASC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        for row in rows {
            snapshots.push(serde_json::from_str(&row?)?);
        }
    }
    Ok(snapshots)
}

fn upsert_data_plane_watermark(
    connection: &Connection,
    watermark: &MatrixDataPlaneWatermark,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_data_plane_watermark (
            source_ref, fact_type, partition_ref, high_watermark, last_batch_id,
            watermark_json, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            watermark.source_ref,
            watermark.fact_type,
            watermark.partition_ref,
            watermark.high_watermark,
            watermark.last_batch_id,
            serde_json::to_string(watermark)?,
            watermark.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn data_plane_watermark_resource_id(watermark: &MatrixDataPlaneWatermark) -> String {
    format!(
        "{}\0{}\0{}",
        watermark.source_ref, watermark.fact_type, watermark.partition_ref
    )
}

fn list_data_plane_watermarks(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixDataPlaneWatermark>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT watermark_json
          FROM matrix_data_plane_watermark
          ORDER BY updated_at DESC, source_ref ASC, fact_type ASC, partition_ref ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn parse_rfc3339_utc(value: &str) -> Result<chrono::DateTime<Utc>, MatrixSqliteRepositoryError> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            MatrixSqliteRepositoryError::Json(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            )))
        })?
        .with_timezone(&Utc))
}

fn source_kind_for_access_mode(access_mode: &str) -> matrix_core::MatrixSourceKind {
    match access_mode {
        "api" => matrix_core::MatrixSourceKind::Api,
        "db_view" | "database_view" | "database_file" | "database_service" | "sqlite" => {
            matrix_core::MatrixSourceKind::Db
        }
        "file" | "batch_file" | "file_batch" | "manual_upload" => {
            matrix_core::MatrixSourceKind::File
        }
        "manual" => matrix_core::MatrixSourceKind::Manual,
        "connector" => matrix_core::MatrixSourceKind::Connector,
        "rpa" => matrix_core::MatrixSourceKind::Rpa,
        _ => matrix_core::MatrixSourceKind::Connector,
    }
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
    let mut object = serde_json::Map::new();
    if fields.is_empty() {
        return row.clone();
    }
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
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    stable_hash_bytes(&bytes)
}

fn stable_hash_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn stable_suffix(parts: &[&str]) -> String {
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(part.as_bytes());
        bytes.push(0);
    }
    stable_hash_bytes(&bytes)
        .trim_start_matches("sha256:")
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

fn stable_entity_reference(source_name: &str, source_key: &str) -> String {
    format!(
        "matrix:entity:{}",
        stable_suffix(&[source_name, source_key])
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
        .unwrap_or_else(|| stable_entity_reference(&source_pack.source_name, source_key))
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

fn parse_optional_rfc3339_utc(
    value: Option<String>,
) -> Result<Option<chrono::DateTime<Utc>>, MatrixSqliteRepositoryError> {
    value.as_deref().map(parse_rfc3339_utc).transpose()
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
    let priority_score = severity_score * 0.30
        + urgency * 0.20
        + impact_scope * 0.20
        + strategic_weight * 0.15
        + confidence * 0.10
        + 0.05;
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
        priority_score,
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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
