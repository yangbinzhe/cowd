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
    MatrixHealth, MatrixMetricRecomputeResult, MatrixMigrationSnapshot, MatrixRevisioned,
    MatrixSqliteDataPlane,
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
        Ok(MatrixSqliteDataPlane::new(health.data_plane_watermark_count).health())
    }

    pub fn plan_data_plane_ingest(
        &self,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> Result<MatrixDataPlaneIngestPlan, MatrixSqliteRepositoryError> {
        let source_ref = input.source_ref.clone();
        let mut plan = MatrixSqliteDataPlane::new(self.health()?.data_plane_watermark_count)
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
        let exists = transaction
            .query_row(
                "SELECT 1 FROM matrix_data_plane_watermark
                 WHERE source_ref = ?1 AND fact_type = ?2 AND partition_ref = ?3",
                params![
                    plan.watermark.source_ref,
                    plan.watermark.fact_type,
                    plan.watermark.partition_ref,
                ],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        let (_, revision, _) = prepare_matrix_resource_revision(
            &transaction,
            "data_plane_watermark",
            &resource_id,
            exists,
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

        let recompute = self.recompute_metrics_for_metric_ids(&job.metric_ids)?;
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
        snapshot: MatrixSourceSnapshot,
        rows: &[Value],
    ) -> Result<MatrixSourceSnapshotApplyReport, MatrixSqliteRepositoryError> {
        let mut attention_count = 0usize;
        let mut relation_count = 0usize;
        let mut fact_refs = Vec::new();
        let mut warnings = BTreeSet::new();
        let connection = self.executor.checkout()?;
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixSqliteRepositoryError::NotFound(source_pack_id.to_string()))?;
        insert_source_snapshot(&connection, &snapshot)?;

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
                    upsert_entity(&connection, &entity)?;
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
                upsert_relation(&connection, &relation)?;
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

    pub fn recompute_metrics(
        &self,
    ) -> Result<MatrixMetricRecomputeResult, MatrixSqliteRepositoryError> {
        self.recompute_metrics_with_filter(None)
    }

    pub fn recompute_metrics_for_metric_ids(
        &self,
        metric_ids: &[String],
    ) -> Result<MatrixMetricRecomputeResult, MatrixSqliteRepositoryError> {
        let filter = metric_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.recompute_metrics_with_filter(Some(&filter))
    }

    fn recompute_metrics_with_filter(
        &self,
        metric_filter: Option<&BTreeSet<String>>,
    ) -> Result<MatrixMetricRecomputeResult, MatrixSqliteRepositoryError> {
        let connection = self.executor.checkout()?;
        let query_results = metric_query_results(&connection, metric_filter)?;

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

fn initialize_schema(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        r"CREATE TABLE IF NOT EXISTS matrix_schema (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        INSERT INTO matrix_schema (id, schema_version, updated_at)
        VALUES (1, 20, datetime('now'))
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

fn upsert_entity(
    connection: &Connection,
    entity: &MatrixEntity,
) -> Result<MatrixEntity, MatrixSqliteRepositoryError> {
    let mut entity = entity.clone();
    if let Some(existing) =
        find_entity_by_canonical(connection, &entity.entity_type, &entity.canonical_key)?
    {
        entity.entity_id = existing.entity_id;
        entity.created_at = existing.created_at;
        entity.source_keys = merged_source_keys(&existing.source_keys, &entity.source_keys);
    }
    entity.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO matrix_entity (
            entity_id, entity_type, canonical_key, display_name, source_keys_json,
            attributes_json, confidence, entity_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(entity_id) DO UPDATE SET
            entity_type = excluded.entity_type,
            canonical_key = excluded.canonical_key,
            display_name = excluded.display_name,
            source_keys_json = excluded.source_keys_json,
            attributes_json = excluded.attributes_json,
            confidence = excluded.confidence,
            entity_json = excluded.entity_json,
            updated_at = excluded.updated_at",
        params![
            entity.entity_id,
            entity.entity_type,
            entity.canonical_key,
            entity.display_name,
            serde_json::to_string(&entity.source_keys)?,
            serde_json::to_string(&entity.attributes)?,
            entity.confidence,
            serde_json::to_string(&entity)?,
            entity.created_at.to_rfc3339(),
            entity.updated_at.to_rfc3339(),
        ],
    )?;
    connection.execute(
        "DELETE FROM matrix_entity_source_key WHERE entity_id = ?1",
        params![entity.entity_id],
    )?;
    for source_key in &entity.source_keys {
        connection.execute(
            r"INSERT INTO matrix_entity_source_key (
                source_system, source_key, entity_id, source_ref, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(source_system, source_key) DO UPDATE SET
                entity_id = excluded.entity_id,
                source_ref = excluded.source_ref",
            params![
                source_key.normalized_system(),
                source_key.normalized_key(),
                entity.entity_id,
                source_key.source_ref,
                Utc::now().to_rfc3339(),
            ],
        )?;
    }
    Ok(entity)
}

fn merged_source_keys(
    existing: &[MatrixSourceKey],
    incoming: &[MatrixSourceKey],
) -> Vec<MatrixSourceKey> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for source_key in existing.iter().chain(incoming.iter()) {
        let key = (source_key.normalized_system(), source_key.normalized_key());
        if seen.insert(key) {
            keys.push(source_key.clone());
        }
    }
    keys
}

fn find_entity(
    connection: &Connection,
    entity_id: &str,
) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT entity_json FROM matrix_entity WHERE entity_id = ?1",
            params![entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn find_entity_by_canonical(
    connection: &Connection,
    entity_type: &str,
    canonical_key: &str,
) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT entity_json
              FROM matrix_entity
              WHERE entity_type = ?1 AND canonical_key = ?2",
            params![entity_type, canonical_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn find_entity_by_source_key(
    connection: &Connection,
    source_system: &str,
    source_key: &str,
) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT e.entity_json
              FROM matrix_entity_source_key s
              JOIN matrix_entity e ON e.entity_id = s.entity_id
              WHERE s.source_system = ?1 AND s.source_key = ?2",
            params![
                matrix_core::normalize_key(source_system),
                matrix_core::normalize_key(source_key),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_entities(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEntity>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT entity_json
          FROM matrix_entity
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixEntity>(&row?)?))
        .collect()
}

fn upsert_relation(
    connection: &Connection,
    relation: &MatrixRelation,
) -> Result<MatrixRelation, MatrixSqliteRepositoryError> {
    if find_entity(connection, &relation.from_entity_id)?.is_none() {
        return Err(MatrixSqliteRepositoryError::NotFound(
            relation.from_entity_id.clone(),
        ));
    }
    if find_entity(connection, &relation.to_entity_id)?.is_none() {
        return Err(MatrixSqliteRepositoryError::NotFound(
            relation.to_entity_id.clone(),
        ));
    }

    let mut relation = relation.clone();
    if let Some(existing) = find_relation_by_key(
        connection,
        &relation.relation_type,
        &relation.from_entity_id,
        &relation.to_entity_id,
    )? {
        relation.relation_id = existing.relation_id;
        relation.created_at = existing.created_at;
    }
    relation.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO matrix_relation (
            relation_id, relation_type, from_entity_id, to_entity_id, attributes_json,
            confidence, relation_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(relation_id) DO UPDATE SET
            relation_type = excluded.relation_type,
            from_entity_id = excluded.from_entity_id,
            to_entity_id = excluded.to_entity_id,
            attributes_json = excluded.attributes_json,
            confidence = excluded.confidence,
            relation_json = excluded.relation_json,
            updated_at = excluded.updated_at",
        params![
            relation.relation_id,
            relation.relation_type,
            relation.from_entity_id,
            relation.to_entity_id,
            serde_json::to_string(&relation.attributes)?,
            relation.confidence,
            serde_json::to_string(&relation)?,
            relation.created_at.to_rfc3339(),
            relation.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(relation)
}

fn find_relation_by_key(
    connection: &Connection,
    relation_type: &str,
    from_entity_id: &str,
    to_entity_id: &str,
) -> Result<Option<MatrixRelation>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT relation_json
              FROM matrix_relation
              WHERE relation_type = ?1 AND from_entity_id = ?2 AND to_entity_id = ?3",
            params![relation_type, from_entity_id, to_entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_entity_relations(
    connection: &Connection,
    entity_id: &str,
    limit: usize,
) -> Result<Vec<MatrixRelation>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT relation_json
          FROM matrix_relation
          WHERE from_entity_id = ?1 OR to_entity_id = ?1
          ORDER BY updated_at DESC
          LIMIT ?2",
    )?;
    let rows = statement.query_map(params![entity_id, limit as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixRelation>(&row?)?))
        .collect()
}

fn build_impact_trace(
    connection: &Connection,
    root_entity_id: &str,
    max_depth: usize,
) -> Result<MatrixImpactTrace, MatrixSqliteRepositoryError> {
    let max_depth = max_depth.clamp(1, 5);
    let mut queue = VecDeque::from([(root_entity_id.to_string(), 0usize)]);
    let mut seen_entities = BTreeSet::from([root_entity_id.to_string()]);
    let mut seen_relations = BTreeSet::new();
    let mut hops = Vec::new();

    while let Some((entity_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for relation in list_entity_relations(connection, &entity_id, 500)? {
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
            let from_entity = find_entity(connection, &relation.from_entity_id)?;
            let to_entity = find_entity(connection, &relation.to_entity_id)?;
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
    for entity_id in &seen_entities {
        if let Some(entity) = find_entity(connection, entity_id)? {
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

fn upsert_attention(
    connection: &Connection,
    item: &MatrixAttentionItem,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_attention_item (
            attention_id, priority_score, status, attention_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            item.attention_id,
            item.priority_score,
            item.status,
            serde_json::to_string(item)?,
            item.created_at.to_rfc3339(),
            item.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn list_attention(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT attention_json
          FROM matrix_attention_item
          ORDER BY priority_score DESC, updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixAttentionItem>(&row?)?))
        .collect()
}

fn find_attention(
    connection: &Connection,
    attention_id: &str,
) -> Result<Option<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT attention_json FROM matrix_attention_item WHERE attention_id = ?1",
            params![attention_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn latest_attention(
    connection: &Connection,
) -> Result<Option<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT attention_json
              FROM matrix_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn insert_evidence_packet(
    connection: &Connection,
    packet: &MatrixEvidencePacket,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_evidence_packet (
            packet_id, attention_id, packet_json, created_at
        ) VALUES (?1, ?2, ?3, ?4)",
        params![
            packet.packet_id,
            packet.attention_id,
            serde_json::to_string(packet)?,
            packet.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn insert_evidence_packet_once(
    connection: &Connection,
    packet: &MatrixEvidencePacket,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR IGNORE INTO matrix_evidence_packet (
            packet_id, attention_id, packet_json, created_at
        ) VALUES (?1, ?2, ?3, ?4)",
        params![
            packet.packet_id,
            packet.attention_id,
            serde_json::to_string(packet)?,
            packet.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_evidence_packet(
    connection: &Connection,
    packet_id: &str,
) -> Result<Option<MatrixEvidencePacket>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT packet_json FROM matrix_evidence_packet WHERE packet_id = ?1",
            params![packet_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_evidence_packets(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEvidencePacket>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT packet_json
          FROM matrix_evidence_packet
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixEvidencePacket>(&row?)?))
        .collect()
}

fn insert_quality_gate(
    connection: &Connection,
    gate: &MatrixQualityGateDecision,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_quality_gate (
            gate_id, target_ref, gate_type, decision, score, gate_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            gate.gate_id,
            gate.target_ref,
            gate.gate_type,
            gate.decision,
            gate.score,
            serde_json::to_string(gate)?,
            gate.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_quality_gate(
    connection: &Connection,
    gate_id: &str,
) -> Result<Option<MatrixQualityGateDecision>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT gate_json FROM matrix_quality_gate WHERE gate_id = ?1",
            params![gate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_recent_quality_gates(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixQualityGateDecision>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT gate_json
          FROM matrix_quality_gate
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

#[derive(Debug, Clone)]
struct MetricSourceRow {
    fact_id: String,
    fact_type: String,
    metric_id: String,
    entity_scope: String,
    period: String,
    measures: Value,
    confidence: f32,
}

fn metric_source_rows(
    connection: &Connection,
) -> Result<Vec<MetricSourceRow>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT fact_id, fact_type, entity_refs_json, metric_key, dimensions_json,
            measures_json, confidence
          FROM matrix_fact
          WHERE metric_key IS NOT NULL
          ORDER BY event_time ASC, fact_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, f32>(6)?,
        ))
    })?;
    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            fact_type,
            entity_refs_json,
            metric_id,
            dimensions_json,
            measures_json,
            confidence,
        ) = row?;
        let entity_refs: Vec<String> = serde_json::from_str(&entity_refs_json)?;
        let dimensions: Value = serde_json::from_str(&dimensions_json)?;
        let measures: Value = serde_json::from_str(&measures_json)?;
        let entity_scope = entity_refs
            .first()
            .cloned()
            .unwrap_or_else(|| "enterprise".to_string());
        let period = dimensions
            .get("period")
            .or_else(|| dimensions.get("week"))
            .and_then(Value::as_str)
            .unwrap_or("current")
            .to_string();
        facts.push(MetricSourceRow {
            fact_id,
            fact_type,
            metric_id,
            entity_scope,
            period,
            measures,
            confidence,
        });
    }
    Ok(facts)
}

fn metric_query_results(
    connection: &Connection,
    metric_filter: Option<&BTreeSet<String>>,
) -> Result<Vec<MatrixQueryResult>, MatrixSqliteRepositoryError> {
    let rows = metric_source_rows(connection)?;
    let mut by_metric = BTreeMap::<String, Vec<MetricSourceRow>>::new();
    for row in rows {
        if metric_filter.is_some_and(|filter| !filter.contains(&row.metric_id)) {
            continue;
        }
        by_metric
            .entry(row.metric_id.clone())
            .or_default()
            .push(row);
    }
    let mut results = Vec::new();
    for (metric_id, rows) in by_metric {
        let fact_type = rows
            .first()
            .map(|row| row.fact_type.as_str())
            .unwrap_or("operations.metric");
        let mut definition = find_metric_definition(connection, &metric_id)?
            .unwrap_or_else(|| MatrixMetricDefinition::inferred(metric_id.clone(), fact_type));
        if definition.measure == "value"
            && rows
                .iter()
                .all(|row| row.measures.get("value").and_then(Value::as_f64).is_none())
        {
            definition.measure = infer_single_numeric_measure(&metric_id, &rows)?;
        }
        let plan = definition.query_plan();
        plan.validate()
            .map_err(|error| MatrixSqliteRepositoryError::InvalidMetricQuery(error.to_string()))?;
        upsert_metric_definition(connection, &definition)?;
        let inputs = rows
            .into_iter()
            .map(|row| {
                let numerator = explicit_measure(&row.measures, &plan.numerator_measure)?;
                let denominator = plan
                    .denominator_measure
                    .as_deref()
                    .map(|measure| explicit_measure(&row.measures, measure))
                    .transpose()?;
                Ok(MatrixQueryInput {
                    fact_ref: format!("matrix:fact:{}", row.fact_id),
                    fact_type: row.fact_type,
                    metric_id: row.metric_id,
                    entity_scope: row.entity_scope,
                    period: row.period,
                    numerator,
                    denominator,
                    confidence: row.confidence,
                })
            })
            .collect::<Result<Vec<_>, MatrixSqliteRepositoryError>>()?;
        results.extend(
            matrix_core::execute_matrix_query_plan(&plan, inputs).map_err(|error| {
                MatrixSqliteRepositoryError::InvalidMetricQuery(error.to_string())
            })?,
        );
    }
    Ok(results)
}

fn explicit_measure(measures: &Value, measure: &str) -> Result<f64, MatrixSqliteRepositoryError> {
    measures
        .get(measure)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
                "measure {measure} is missing or non-numeric"
            ))
        })
}

fn infer_single_numeric_measure(
    metric_id: &str,
    rows: &[MetricSourceRow],
) -> Result<String, MatrixSqliteRepositoryError> {
    let mut candidates = BTreeSet::new();
    for row in rows {
        let object = row.measures.as_object().ok_or_else(|| {
            MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
                "metric {metric_id} measures must be an object"
            ))
        })?;
        candidates.extend(
            object
                .iter()
                .filter(|(_, value)| value.as_f64().is_some())
                .map(|(key, _)| key.clone()),
        );
    }
    if candidates.len() != 1 {
        return Err(MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
            "metric {metric_id} must register one explicit measure (found {})",
            candidates.len()
        )));
    }
    candidates.into_iter().next().ok_or_else(|| {
        MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
            "metric {metric_id} has no explicit numeric measure"
        ))
    })
}

fn list_facts(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixFact>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
            dimensions_json, measures_json, event_time, valid_from, valid_to,
            source_ref, confidence, raw_hash
          FROM matrix_fact
          ORDER BY event_time DESC, fact_id ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, f32>(11)?,
            row.get::<_, String>(12)?,
        ))
    })?;

    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs_json,
            metric_key,
            dimensions_json,
            measures_json,
            event_time,
            valid_from,
            valid_to,
            source_ref,
            confidence,
            raw_hash,
        ) = row?;
        facts.push(MatrixFact {
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs: serde_json::from_str(&entity_refs_json)?,
            metric_key,
            dimensions: serde_json::from_str(&dimensions_json)?,
            measures: serde_json::from_str(&measures_json)?,
            event_time: parse_rfc3339_utc(&event_time)?,
            valid_from: parse_optional_rfc3339_utc(valid_from)?,
            valid_to: parse_optional_rfc3339_utc(valid_to)?,
            source_ref,
            confidence,
            raw_hash,
        });
    }
    Ok(facts)
}

fn upsert_metric_definition(
    connection: &Connection,
    definition: &MatrixMetricDefinition,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_metric_definition (
            metric_id, definition_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(metric_id) DO UPDATE SET
            definition_json = excluded.definition_json,
            updated_at = excluded.updated_at",
        params![
            definition.metric_id,
            serde_json::to_string(definition)?,
            definition.created_at.to_rfc3339(),
            definition.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_metric_definition(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricDefinition>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT definition_json FROM matrix_metric_definition WHERE metric_id = ?1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn upsert_metric_dependency(
    connection: &Connection,
    dependency: &MatrixMetricDependency,
) -> Result<MatrixMetricDependency, MatrixSqliteRepositoryError> {
    let mut dependency = dependency.clone();
    if let Some(existing) = find_metric_dependency_by_key(
        connection,
        &dependency.upstream_metric_id,
        &dependency.downstream_metric_id,
        &dependency.dependency_type,
    )? {
        dependency.dependency_id = existing.dependency_id;
        dependency.created_at = existing.created_at;
    }
    dependency.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO matrix_metric_dependency (
            dependency_id, upstream_metric_id, downstream_metric_id, dependency_type,
            confidence, dependency_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(dependency_id) DO UPDATE SET
            upstream_metric_id = excluded.upstream_metric_id,
            downstream_metric_id = excluded.downstream_metric_id,
            dependency_type = excluded.dependency_type,
            confidence = excluded.confidence,
            dependency_json = excluded.dependency_json,
            updated_at = excluded.updated_at",
        params![
            dependency.dependency_id,
            dependency.upstream_metric_id,
            dependency.downstream_metric_id,
            dependency.dependency_type,
            dependency.confidence,
            serde_json::to_string(&dependency)?,
            dependency.created_at.to_rfc3339(),
            dependency.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(dependency)
}

fn find_metric_dependency_by_key(
    connection: &Connection,
    upstream_metric_id: &str,
    downstream_metric_id: &str,
    dependency_type: &str,
) -> Result<Option<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT dependency_json
              FROM matrix_metric_dependency
              WHERE upstream_metric_id = ?1
                AND downstream_metric_id = ?2
                AND dependency_type = ?3",
            params![upstream_metric_id, downstream_metric_id, dependency_type],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_upstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          WHERE downstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn list_downstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          WHERE upstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn build_metric_lineage(
    connection: &Connection,
    metric_id: &str,
    max_depth: usize,
) -> Result<MatrixMetricLineage, MatrixSqliteRepositoryError> {
    let max_depth = max_depth.clamp(1, 6);
    let upstream_dependencies = list_upstream_metric_dependencies(connection, metric_id)?;
    let downstream_dependencies = list_downstream_metric_dependencies(connection, metric_id)?;
    let mut impacted = BTreeSet::new();
    let mut queue = VecDeque::from([(metric_id.to_string(), 0usize)]);
    while let Some((current_metric_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for dependency in list_downstream_metric_dependencies(connection, &current_metric_id)? {
            if impacted.insert(dependency.downstream_metric_id.clone()) {
                queue.push_back((dependency.downstream_metric_id, depth + 1));
            }
        }
    }
    Ok(MatrixMetricLineage {
        metric_id: metric_id.to_string(),
        upstream_dependencies,
        downstream_dependencies,
        impacted_metric_ids: impacted.into_iter().collect(),
        generated_at: Utc::now(),
    })
}

fn metrics_affected_by_fact_type(
    connection: &Connection,
    fact_type: &str,
) -> Result<Vec<String>, MatrixSqliteRepositoryError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let dependency: MatrixMetricDependency = serde_json::from_str(&row?)?;
        if dependency
            .required_fact_types
            .iter()
            .any(|candidate| candidate == fact_type)
        {
            impacted.insert(dependency.upstream_metric_id.clone());
            impacted.insert(dependency.downstream_metric_id.clone());
            for metric_id in build_metric_lineage(connection, &dependency.downstream_metric_id, 6)?
                .impacted_metric_ids
            {
                impacted.insert(metric_id);
            }
        }
    }
    Ok(impacted.into_iter().collect())
}

fn metric_ids_for_fact_type(
    connection: &Connection,
    fact_type: &str,
) -> Result<Vec<String>, MatrixSqliteRepositoryError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT definition_json
          FROM matrix_metric_definition
          ORDER BY metric_id ASC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let definition: MatrixMetricDefinition = serde_json::from_str(&row?)?;
        if definition.inputs.iter().any(|input| input == fact_type) {
            impacted.insert(definition.metric_id);
        }
    }
    Ok(impacted.into_iter().collect())
}

fn build_metric_attention_plan(
    connection: &Connection,
    trigger_fact_type: &str,
    entity_scope: Option<String>,
    period: Option<String>,
    metric_ids: Vec<String>,
    limit: usize,
) -> Result<MatrixMetricAttentionPlan, MatrixSqliteRepositoryError> {
    let limit = limit.clamp(1, 24);
    let mut scores = Vec::new();
    for metric_id in metric_ids {
        let definition = find_metric_definition(connection, &metric_id)?.unwrap_or_else(|| {
            MatrixMetricDefinition::inferred(metric_id.clone(), trigger_fact_type)
        });
        let lineage = build_metric_lineage(connection, &metric_id, 6)?;
        let latest = latest_metric_state_for_metric(connection, &metric_id)?;
        let latest_status = latest
            .as_ref()
            .map(|state| format!("{:?}", state.status).to_ascii_lowercase());
        let latest_delta = latest.as_ref().map(|state| state.delta);
        let score = MatrixMetricAttentionScore::new(
            metric_id.clone(),
            definition.business_priority,
            lineage.impacted_metric_ids.len() + lineage.upstream_dependencies.len(),
            latest_status,
            latest_delta,
        );
        scores.push(score);
    }
    scores.sort_by(|left, right| {
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
    scores.truncate(limit);
    let selected_metric_ids = scores
        .iter()
        .map(|score| score.metric_id.clone())
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
        scored_metrics: scores,
        selected_metric_ids,
        compute_jobs,
        generated_at: Utc::now(),
    })
}

fn build_metric_snapshot(
    connection: &Connection,
    metric_ids: Vec<String>,
    scope_ref: Option<String>,
) -> Result<MatrixMetricSnapshot, MatrixSqliteRepositoryError> {
    let mut unique_metric_ids = metric_ids;
    unique_metric_ids.sort();
    unique_metric_ids.dedup();
    let mut items = Vec::new();
    for metric_id in &unique_metric_ids {
        let state = latest_metric_state_for_metric(connection, metric_id)?;
        items.push(MatrixMetricSnapshotItem {
            metric_id: metric_id.clone(),
            state,
        });
    }
    let state_count = items.iter().filter(|item| item.state.is_some()).count();
    Ok(MatrixMetricSnapshot {
        snapshot_id: format!("metric-snapshot-{}", uuid::Uuid::new_v4()),
        scope_ref: scope_ref.unwrap_or_else(|| "global".to_string()),
        metric_ids: unique_metric_ids,
        items,
        created_at: Utc::now(),
        summary: format!("metric states materialized: {state_count}"),
    })
}

fn insert_metric_snapshot(
    connection: &Connection,
    snapshot: &MatrixMetricSnapshot,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_metric_snapshot (
            snapshot_id, scope_ref, metric_ids_json, snapshot_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            snapshot.snapshot_id,
            snapshot.scope_ref,
            serde_json::to_string(&snapshot.metric_ids)?,
            serde_json::to_string(snapshot)?,
            snapshot.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn insert_scenario_spec(
    connection: &Connection,
    spec: &MatrixScenarioSpec,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_scenario_spec (
            scenario_id, source_snapshot_id, transform_ref, spec_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            spec.scenario_id,
            spec.base_snapshot.snapshot_id,
            spec.transform_ref,
            serde_json::to_string(spec)?,
            spec.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_scenario_spec(
    connection: &Connection,
    scenario_id: &str,
) -> Result<Option<MatrixScenarioSpec>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT spec_json FROM matrix_scenario_spec WHERE scenario_id = ?1",
            params![scenario_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_scenario_specs(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixScenarioSpec>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT spec_json FROM matrix_scenario_spec ORDER BY created_at DESC, scenario_id ASC LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.max(1) as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn insert_scenario_run(
    connection: &Connection,
    run: &MatrixScenarioRun,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_scenario_run (
            run_id, scenario_id, source_snapshot_id, status, run_json, started_at, completed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run.run_id,
            run.scenario_id,
            run.base_snapshot.snapshot_id,
            scenario_run_status_name(run.status),
            serde_json::to_string(run)?,
            run.started_at.to_rfc3339(),
            run.completed_at.map(|value| value.to_rfc3339()),
        ],
    )?;
    Ok(())
}

fn update_scenario_run(
    connection: &Connection,
    run: &MatrixScenarioRun,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"UPDATE matrix_scenario_run
          SET status = ?2, run_json = ?3, completed_at = ?4
          WHERE run_id = ?1",
        params![
            run.run_id,
            scenario_run_status_name(run.status),
            serde_json::to_string(run)?,
            run.completed_at.map(|value| value.to_rfc3339()),
        ],
    )?;
    Ok(())
}

fn find_scenario_run(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<MatrixScenarioRun>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT run_json FROM matrix_scenario_run WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn list_scenario_runs(
    connection: &Connection,
    scenario_id: Option<&str>,
    limit: usize,
) -> Result<Vec<MatrixScenarioRun>, MatrixSqliteRepositoryError> {
    let (sql, parameter) = match scenario_id {
        Some(scenario_id) => (
            "SELECT run_json FROM matrix_scenario_run WHERE scenario_id = ?1 ORDER BY started_at DESC, run_id ASC LIMIT ?2",
            vec![
                rusqlite::types::Value::Text(scenario_id.to_string()),
                rusqlite::types::Value::Integer(limit.max(1) as i64),
            ],
        ),
        None => (
            "SELECT run_json FROM matrix_scenario_run ORDER BY started_at DESC, run_id ASC LIMIT ?1",
            vec![rusqlite::types::Value::Integer(limit.max(1) as i64)],
        ),
    };
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(rusqlite::params_from_iter(parameter), |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn insert_scenario_result(
    connection: &Connection,
    result: &MatrixScenarioResult,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_scenario_result (
            result_id, run_id, scenario_id, boundary, result_json, completed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            result.result_id,
            result.run_id,
            result.scenario_id,
            result.boundary,
            serde_json::to_string(result)?,
            result.completed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_scenario_result(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<MatrixScenarioResult>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT result_json FROM matrix_scenario_result WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

const fn scenario_run_status_name(status: MatrixScenarioRunStatus) -> &'static str {
    match status {
        MatrixScenarioRunStatus::Running => "running",
        MatrixScenarioRunStatus::Succeeded => "succeeded",
        MatrixScenarioRunStatus::Failed => "failed",
        MatrixScenarioRunStatus::Cancelled => "cancelled",
    }
}

fn priority_for_compute_job(job: &MatrixComputeJob) -> f32 {
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

fn upsert_compute_job(
    connection: &Connection,
    job: &MatrixComputeJob,
) -> Result<MatrixComputeJob, MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_compute_job (
            job_id, trigger_fact_type, status, priority, job_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(job_id) DO UPDATE SET
            trigger_fact_type = excluded.trigger_fact_type,
            status = excluded.status,
            priority = excluded.priority,
            job_json = excluded.job_json,
            updated_at = excluded.updated_at",
        params![
            job.job_id,
            job.trigger_fact_type,
            job.status,
            job.priority,
            serde_json::to_string(job)?,
            job.created_at.to_rfc3339(),
            job.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(job.clone())
}

fn find_compute_job(
    connection: &Connection,
    job_id: &str,
) -> Result<Option<MatrixComputeJob>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT job_json FROM matrix_compute_job WHERE job_id = ?1",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn latest_metric_state(
    connection: &Connection,
    metric_id: &str,
    entity_scope: &str,
    period: &str,
) -> Result<Option<MatrixMetricState>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1 AND entity_scope = ?2 AND period = ?3
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id, entity_scope, period],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn insert_metric_state(
    connection: &Connection,
    state: &MatrixMetricState,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_metric_state (
            state_id, metric_id, entity_scope, period, value, previous_value,
            delta, status, state_json, computed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            state.state_id,
            state.metric_id,
            state.entity_scope,
            state.period,
            state.value,
            state.previous_value,
            state.delta,
            format!("{:?}", state.status).to_ascii_lowercase(),
            serde_json::to_string(state)?,
            state.computed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn insert_change_event(
    connection: &Connection,
    change: &MatrixChangeEvent,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_change_event (
            change_id, metric_id, entity_ref, period, delta, severity_hint,
            change_json, detected_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            change.change_id,
            change.metric_id,
            change.entity_ref,
            change.period,
            change.delta,
            change.severity_hint,
            serde_json::to_string(change)?,
            change.detected_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_change(
    connection: &Connection,
    change_id: &str,
) -> Result<Option<MatrixChangeEvent>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT change_json FROM matrix_change_event WHERE change_id = ?1",
            params![change_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

fn latest_metric_state_for_metric(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricState>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
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
mod tests {
    use super::*;
    use matrix_core::{
        MatrixEntityInput, MatrixMetricDependencyInput, MatrixRelationInput,
        MatrixScenarioOutputContract, MatrixSnapshotRef, MatrixSourceEntityMapping,
        MatrixSourceFactMapping, MatrixSourceKind, MatrixSourceRelationMapping,
    };

    fn minimal_source_pack(id: &str) -> MatrixSourcePack {
        MatrixSourcePack {
            source_pack_id: id.to_string(),
            source_name: "revision-fixture".to_string(),
            owner: "test".to_string(),
            access_mode: "manual".to_string(),
            refresh_mode: "snapshot".to_string(),
            entity_mappings: Vec::new(),
            fact_mappings: Vec::new(),
            relation_mappings: Vec::new(),
            reconciliation_rules: Vec::new(),
            quality_rules: Vec::new(),
            freshness_sla: None,
            security_policy: None,
            metadata: Value::Null,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn sqlite_metric_query_uses_the_registered_formula_and_explicit_operands() {
        let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
        let mut definition = MatrixMetricDefinition::inferred_for_measure(
            "work_center_load",
            "manufacturing.work_center_load",
            "load_hours",
        );
        definition.formula_ref = matrix_core::MATRIX_FORMULA_RATIO_PERCENT_V1.to_string();
        definition.denominator_measure = Some("capacity_hours".to_string());
        repository
            .register_metric_definition(&definition)
            .expect("definition saves");
        repository
            .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
                fact_id: Some("load-fact".to_string()),
                snapshot_id: Some("load-snapshot".to_string()),
                fact_type: "manufacturing.work_center_load".to_string(),
                entity_refs: vec!["work-center:one".to_string()],
                metric_key: Some("work_center_load".to_string()),
                dimensions: serde_json::json!({"week": "2026-W30"}),
                measures: serde_json::json!({"load_hours": 188, "capacity_hours": 160}),
                event_time: None,
                valid_from: None,
                valid_to: None,
                source_ref: None,
                confidence: Some(0.9),
                raw_hash: None,
            }))
            .expect("fact saves");

        let result = repository.recompute_metrics().expect("query executes");

        assert_eq!(result.metric_states.len(), 1);
        assert!((result.metric_states[0].value - 117.5).abs() < f64::EPSILON);
    }

    #[test]
    fn unregistered_multi_measure_metric_fails_closed() {
        let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
        repository
            .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
                fact_id: Some("ambiguous-fact".to_string()),
                snapshot_id: Some("ambiguous-snapshot".to_string()),
                fact_type: "manufacturing.ambiguous".to_string(),
                entity_refs: vec!["work-center:one".to_string()],
                metric_key: Some("ambiguous_metric".to_string()),
                dimensions: serde_json::json!({"week": "2026-W30"}),
                measures: serde_json::json!({"load": 188, "capacity": 160}),
                event_time: None,
                valid_from: None,
                valid_to: None,
                source_ref: None,
                confidence: Some(0.9),
                raw_hash: None,
            }))
            .expect("fact saves");

        assert!(matches!(
            repository.recompute_metrics(),
            Err(MatrixSqliteRepositoryError::InvalidMetricQuery(message))
                if message.contains("register one explicit measure")
        ));
    }

    #[test]
    fn data_plane_ingest_plan_includes_metric_declared_by_source_pack() {
        let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
        let source_pack_id = "source-pack-ingest-metric";
        let mut source_pack = minimal_source_pack(source_pack_id);
        source_pack.fact_mappings = vec![MatrixSourceFactMapping {
            source_table: "manufacturing_events".to_string(),
            fact_type: "manufacturing.event".to_string(),
            metric_key: "manufacturing_event_count".to_string(),
            entity_ref_fields: vec!["asset_id".to_string()],
            measure_fields: Vec::new(),
            event_time_field: None,
            dedup_key: "event_id".to_string(),
            delta_signature: "updated_at".to_string(),
        }];
        repository
            .upsert_source_pack(source_pack)
            .expect("source pack saves");

        let plan = repository
            .plan_data_plane_ingest(MatrixDataPlaneIngestPlanInput {
                source_ref: format!("source-pack://{source_pack_id}"),
                fact_type: "manufacturing.event".to_string(),
                partition_ref: None,
                high_watermark: None,
                estimated_rows: None,
                raw_checksum: None,
                expected_revision: None,
                adapter_id: None,
                strategy: None,
                table: None,
                cursor: None,
                offset: None,
                metric_ids: Vec::new(),
            })
            .expect("ingest plan builds");

        assert!(plan
            .affected_metric_ids
            .iter()
            .any(|metric_id| metric_id == "manufacturing_event_count"));
        assert!(plan.compute_jobs.iter().any(|job| {
            job.metric_ids
                .iter()
                .any(|metric_id| metric_id == "manufacturing_event_count")
        }));
    }

    #[test]
    fn data_plane_watermark_commit_uses_revision_cas() {
        let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
        let input = |expected_revision| MatrixDataPlaneIngestPlanInput {
            source_ref: "bitable://app/orders".to_string(),
            fact_type: "source.feishu_bitable.row".to_string(),
            partition_ref: Some("orders".to_string()),
            high_watermark: Some("cursor-1".to_string()),
            estimated_rows: Some(2),
            raw_checksum: Some("sha256:rows".to_string()),
            expected_revision,
            adapter_id: Some("feishu_bitable".to_string()),
            strategy: Some("cursor_field".to_string()),
            table: Some("orders".to_string()),
            cursor: Some("cursor-1".to_string()),
            offset: Some(2),
            metric_ids: Vec::new(),
        };

        let first = repository
            .plan_data_plane_ingest(input(None))
            .expect("first plan");
        let committed = repository
            .commit_data_plane_ingest(&first)
            .expect("first commit");
        assert_eq!(committed.revision, 1);

        let stale = repository
            .plan_data_plane_ingest(input(None))
            .expect("stale plan");
        assert!(matches!(
            repository.commit_data_plane_ingest(&stale),
            Err(MatrixSqliteRepositoryError::RevisionConflict {
                expected: None,
                actual: Some(1),
                ..
            })
        ));

        let second = repository
            .plan_data_plane_ingest(input(Some(1)))
            .expect("second plan");
        let committed = repository
            .commit_data_plane_ingest(&second)
            .expect("second commit");
        assert_eq!(committed.revision, 2);
        let loaded = repository
            .get_data_plane_watermark(
                "bitable://app/orders",
                "source.feishu_bitable.row",
                "orders",
            )
            .expect("load")
            .expect("watermark");
        assert_eq!(loaded.revision, 2);
        assert_eq!(loaded.cursor.as_deref(), Some("cursor-1"));
    }

    #[test]
    fn checked_matrix_upserts_require_exact_revision_for_all_four_resources() {
        let repository = MatrixSqliteRepository::in_memory().unwrap();

        let source = repository
            .upsert_source_pack_checked(minimal_source_pack("source-revision"), None)
            .unwrap();
        assert!(source.created);
        assert_eq!(source.revision, 1);
        assert!(matches!(
            repository.upsert_source_pack_checked(minimal_source_pack("source-revision"), None),
            Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
        ));
        assert_eq!(
            repository
                .upsert_source_pack_checked(
                    minimal_source_pack("source-revision"),
                    Some(source.revision),
                )
                .unwrap()
                .revision,
            2
        );

        let left = MatrixEntity::from_input(MatrixEntityInput {
            entity_id: Some("entity-left".to_string()),
            entity_type: "part".to_string(),
            canonical_key: "left".to_string(),
            display_name: None,
            source_keys: Vec::new(),
            attributes: Value::Null,
            confidence: None,
        });
        let entity = repository.upsert_entity_checked(&left, None).unwrap();
        assert_eq!(entity.revision, 1);
        assert!(matches!(
            repository.upsert_entity_checked(&left, None),
            Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
        ));
        assert_eq!(
            repository
                .upsert_entity_checked(&left, Some(entity.revision))
                .unwrap()
                .revision,
            2
        );

        let right = MatrixEntity::from_input(MatrixEntityInput {
            entity_id: Some("entity-right".to_string()),
            entity_type: "part".to_string(),
            canonical_key: "right".to_string(),
            display_name: None,
            source_keys: Vec::new(),
            attributes: Value::Null,
            confidence: None,
        });
        repository.upsert_entity(&right).unwrap();
        let relation = MatrixRelation::from_input(MatrixRelationInput {
            relation_id: Some("relation-revision".to_string()),
            relation_type: "depends_on".to_string(),
            from_entity_id: left.entity_id.clone(),
            to_entity_id: right.entity_id.clone(),
            attributes: Value::Null,
            confidence: None,
        });
        let relation = repository.upsert_relation_checked(&relation, None).unwrap();
        assert_eq!(relation.revision, 1);
        assert!(matches!(
            repository.upsert_relation_checked(&relation.resource, None),
            Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
        ));
        assert_eq!(
            repository
                .upsert_relation_checked(&relation.resource, Some(relation.revision))
                .unwrap()
                .revision,
            2
        );

        let dependency = MatrixMetricDependency::from_input(MatrixMetricDependencyInput {
            dependency_id: Some("dependency-revision".to_string()),
            upstream_metric_id: "metric-a".to_string(),
            downstream_metric_id: "metric-b".to_string(),
            dependency_type: "derived_from".to_string(),
            entity_relation_type: None,
            required_fact_types: Vec::new(),
            transformation_ref: None,
            confidence: None,
            notes: None,
        });
        let dependency = repository
            .upsert_metric_dependency_checked(&dependency, None)
            .unwrap();
        assert_eq!(dependency.revision, 1);
        assert!(matches!(
            repository.upsert_metric_dependency_checked(&dependency.resource, None),
            Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
        ));
        assert_eq!(
            repository
                .upsert_metric_dependency_checked(&dependency.resource, Some(dependency.revision),)
                .unwrap()
                .revision,
            2
        );
    }

    #[test]
    fn entity_match_preview_is_pure_and_decision_materializes_the_stable_candidate() {
        let repository = MatrixSqliteRepository::in_memory().unwrap();
        for entity_id in ["entity-preview-left", "entity-preview-right"] {
            repository
                .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
                    entity_id: Some(entity_id.to_string()),
                    entity_type: "part".to_string(),
                    canonical_key: entity_id.to_string(),
                    display_name: Some("Shared preview identity".to_string()),
                    source_keys: Vec::new(),
                    attributes: Value::Null,
                    confidence: None,
                }))
                .unwrap();
        }

        let first = repository
            .propose_entity_match("entity-preview-left", "entity-preview-right")
            .unwrap();
        let second = repository
            .propose_entity_match("entity-preview-left", "entity-preview-right")
            .unwrap();
        assert_eq!(first.candidate_id, second.candidate_id);
        let preview_health = repository.health().unwrap();
        assert_eq!(preview_health.entity_match_candidate_count, 0);
        assert_eq!(preview_health.entity_conflict_decision_count, 0);

        let decision = repository
            .decide_entity_conflict(
                &first.candidate_id,
                "entity-preview-left",
                "entity-preview-right",
                "prefer_verified_source",
                Some("governed commit".to_string()),
            )
            .unwrap();
        assert_eq!(decision.candidate_id, first.candidate_id);
        let committed_health = repository.health().unwrap();
        assert_eq!(committed_health.entity_match_candidate_count, 1);
        assert_eq!(committed_health.entity_conflict_decision_count, 1);
    }

    #[test]
    fn source_snapshot_apply_maps_rows_to_matrix_records_idempotently() {
        let repository = MatrixSqliteRepository::in_memory().unwrap();
        let source_pack = MatrixSourcePack {
            source_pack_id: "source-pack-supply-orders".to_string(),
            source_name: "supply_fixture".to_string(),
            owner: "test".to_string(),
            access_mode: "file".to_string(),
            refresh_mode: "snapshot".to_string(),
            entity_mappings: vec![
                MatrixSourceEntityMapping {
                    source_entity: "supplier".to_string(),
                    matrix_entity_type: "supplier".to_string(),
                    source_key_field: "supplier_id".to_string(),
                },
                MatrixSourceEntityMapping {
                    source_entity: "part".to_string(),
                    matrix_entity_type: "part".to_string(),
                    source_key_field: "part_id".to_string(),
                },
            ],
            fact_mappings: vec![MatrixSourceFactMapping {
                source_table: "orders".to_string(),
                fact_type: "supply.order".to_string(),
                metric_key: "supply_qty".to_string(),
                entity_ref_fields: vec!["supplier_id".to_string(), "part_id".to_string()],
                measure_fields: vec!["qty".to_string()],
                event_time_field: Some("event_time".to_string()),
                dedup_key: "order_id".to_string(),
                delta_signature: "order_id".to_string(),
            }],
            relation_mappings: vec![MatrixSourceRelationMapping {
                source_table: "orders".to_string(),
                relation_type: "supplies".to_string(),
                from_source_key_field: "supplier_id".to_string(),
                to_source_key_field: "part_id".to_string(),
                attribute_fields: vec!["qty".to_string()],
                dedup_key: "order_id".to_string(),
            }],
            reconciliation_rules: vec!["source_snapshot_is_idempotent".to_string()],
            quality_rules: vec!["dedup_key_required".to_string()],
            freshness_sla: Some("manual".to_string()),
            security_policy: Some("test_fixture".to_string()),
            metadata: serde_json::json!({"fixture": true}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        repository.upsert_source_pack(source_pack).unwrap();

        let rows = vec![
            serde_json::json!({
                "order_id": "O1",
                "supplier_id": "S1",
                "part_id": "P1",
                "qty": 12,
                "event_time": "2026-07-02T00:00:00Z"
            }),
            serde_json::json!({
                "order_id": "O2",
                "supplier_id": "S2",
                "part_id": "P2",
                "qty": 4,
                "event_time": "2026-07-02T01:00:00Z"
            }),
        ];
        let snapshot = repository
            .create_source_snapshot(MatrixSourceSnapshotInput {
                snapshot_id: Some("snapshot-source-orders-1".to_string()),
                source_pack_id: Some("source-pack-supply-orders".to_string()),
                source_system: "supply_fixture".to_string(),
                source_kind: MatrixSourceKind::File,
                resource_ref: Some("file://orders.csv".to_string()),
                business_period: None,
                captured_at: None,
                schema_version: Some("source:csv:orders".to_string()),
                row_count: Some(rows.len() as u64),
                checksum: Some("sha256:test".to_string()),
                confidence: Some(0.96),
                metadata: Value::Null,
            })
            .unwrap();

        let report = repository
            .apply_source_snapshot_rows("source-pack-supply-orders", snapshot.clone(), &rows)
            .unwrap();
        assert_eq!(report.fact_count, 2);
        assert_eq!(report.relation_count, 2);
        assert!(report.warnings.is_empty());

        let supplier = repository
            .resolve_entity_by_source_key("supply_fixture", "S1")
            .unwrap()
            .expect("supplier entity should be indexed by source key");
        let relations = repository
            .list_entity_relations(&supplier.entity_id, 10)
            .unwrap();
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].relation_type, "supplies");

        let facts = repository.list_facts(10).unwrap();
        assert_eq!(facts.len(), 2);
        assert!(facts.iter().all(|fact| {
            fact.entity_refs
                .iter()
                .any(|reference| reference.starts_with("matrix:entity:entity-"))
        }));

        let snapshots = repository
            .list_source_snapshots(Some("source-pack-supply-orders"), 10)
            .unwrap();
        assert_eq!(snapshots.len(), 1);

        repository
            .apply_source_snapshot_rows("source-pack-supply-orders", snapshot, &rows)
            .unwrap();
        let health = repository.health().unwrap();
        assert_eq!(health.source_snapshot_count, 1);
        assert_eq!(health.fact_count, 2);
        assert_eq!(health.relation_count, 2);
        assert_eq!(health.attention_count, 2);
    }

    #[test]
    fn scenario_runs_are_bound_to_an_immutable_snapshot_and_stay_simulated() {
        let repository = MatrixSqliteRepository::in_memory().unwrap();
        let snapshot = repository
            .create_source_snapshot(MatrixSourceSnapshotInput {
                snapshot_id: Some("scenario-snapshot".to_string()),
                source_pack_id: None,
                source_system: "scenario-fixture".to_string(),
                source_kind: MatrixSourceKind::Manual,
                resource_ref: Some("fixture://scenario-input".to_string()),
                business_period: None,
                captured_at: None,
                schema_version: Some("scenario/v1".to_string()),
                row_count: Some(1),
                checksum: Some("fixture-checksum".to_string()),
                confidence: Some(1.0),
                metadata: serde_json::json!({"fixture": true}),
            })
            .unwrap();
        let spec = repository
            .create_scenario_spec(MatrixScenarioSpec::new(
                MatrixSnapshotRef::from_source_snapshot(&snapshot),
                serde_json::json!({"demand_change": 0.25}),
                "runtime/scenario/supply-risk@1",
                MatrixScenarioOutputContract {
                    required_outputs: vec!["shortage_risk".to_string()],
                    evidence_required: true,
                },
            ))
            .unwrap();
        let run = repository
            .start_scenario_run(&spec.scenario_id, serde_json::json!({"region": "east"}))
            .unwrap();
        let completed = repository
            .complete_scenario_run(MatrixScenarioResult::simulated(
                &run,
                serde_json::json!({"shortage_risk": "high"}),
                vec![snapshot.reference()],
            ))
            .unwrap();

        assert_eq!(completed.boundary, "simulated");
        assert_eq!(
            repository.get_scenario_result(&run.run_id).unwrap(),
            Some(completed)
        );
        assert_eq!(
            repository
                .get_scenario_run(&run.run_id)
                .unwrap()
                .unwrap()
                .status,
            MatrixScenarioRunStatus::Succeeded
        );
        let health = repository.health().unwrap();
        assert_eq!(health.scenario_spec_count, 1);
        assert_eq!(health.scenario_run_count, 1);
        assert_eq!(health.scenario_result_count, 1);
    }
}
