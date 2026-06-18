#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::MatrixSqliteDataPlane;
use matrix::{
    build_metric_compute_jobs, MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob,
    MatrixComputeJobInput, MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput,
    MatrixDataPlane, MatrixDataPlaneHealth, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark, MatrixEntity, MatrixEvidencePacket,
    MatrixEvidenceSourceRef, MatrixFact, MatrixImpactHop, MatrixImpactTrace,
    MatrixMetricAttentionPlan, MatrixMetricAttentionScore, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricSnapshotItem,
    MatrixMetricState, MatrixOntologyPack, MatrixQualityGateDecision, MatrixRelation,
    MatrixSeverity, MatrixSourceDeltaPlan, MatrixSourceKey, MatrixSourcePack,
    MatrixSourcePackValidation,
};

#[derive(Debug, Error)]
pub enum MatrixRuntimeStoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("matrix record not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixHealth {
    pub schema_version: i64,
    pub fact_count: u64,
    pub metric_definition_count: u64,
    pub metric_state_count: u64,
    pub change_count: u64,
    pub attention_count: u64,
    pub evidence_count: u64,
    pub entity_count: u64,
    pub relation_count: u64,
    pub metric_dependency_count: u64,
    pub compute_job_count: u64,
    pub quality_gate_count: u64,
    pub source_pack_count: u64,
    pub data_plane_watermark_count: u64,
    pub connector_run_count: u64,
    pub ontology_pack_count: u64,
    pub entity_match_candidate_count: u64,
    pub entity_conflict_decision_count: u64,
    pub metric_snapshot_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixMetricRecomputeResult {
    pub metric_state_count: usize,
    pub change_count: usize,
    pub attention_count: usize,
    pub metric_states: Vec<MatrixMetricState>,
    pub changes: Vec<MatrixChangeEvent>,
    pub attention: Vec<MatrixAttentionItem>,
}

#[derive(Debug)]
pub struct MatrixRuntimeStore {
    connection: Mutex<Connection>,
}

impl MatrixRuntimeStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MatrixRuntimeStoreError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn in_memory() -> Result<Self, MatrixRuntimeStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, MatrixRuntimeStoreError> {
        connection.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        connection.query_row("PRAGMA busy_timeout=5000", [], |_| Ok(()))?;
        connection.execute_batch("PRAGMA foreign_keys=ON;")?;
        initialize_schema(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn health(&self) -> Result<MatrixHealth, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        })
    }

    pub fn data_plane_health(&self) -> Result<MatrixDataPlaneHealth, MatrixRuntimeStoreError> {
        let health = self.health()?;
        Ok(MatrixSqliteDataPlane::new(health.data_plane_watermark_count).health())
    }

    pub fn plan_data_plane_ingest(
        &self,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> Result<MatrixDataPlaneIngestPlan, MatrixRuntimeStoreError> {
        let mut plan = MatrixSqliteDataPlane::new(self.health()?.data_plane_watermark_count)
            .plan_ingest(input);
        if plan.affected_metric_ids.is_empty() {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut affected = metrics_affected_by_fact_type(&connection, &plan.fact_type)?;
            affected.extend(metric_ids_for_fact_type(&connection, &plan.fact_type)?);
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
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_data_plane_watermark(&connection, &plan.watermark)?;
        Ok(plan)
    }

    pub fn upsert_entity(
        &self,
        entity: &MatrixEntity,
    ) -> Result<MatrixEntity, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_entity(&connection, entity)
    }

    pub fn get_entity(
        &self,
        entity_id: &str,
    ) -> Result<Option<MatrixEntity>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity(&connection, entity_id)
    }

    pub fn resolve_entity_by_source_key(
        &self,
        source_system: &str,
        source_key: &str,
    ) -> Result<Option<MatrixEntity>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity_by_source_key(&connection, source_system, source_key)
    }

    pub fn list_entities(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEntity>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_entities(&connection, limit)
    }

    pub fn get_ontology_pack(
        &self,
        ontology_id: &str,
    ) -> Result<Option<MatrixOntologyPack>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_ontology_pack(&connection, ontology_id)
    }

    pub fn propose_entity_match(
        &self,
        left_entity_id: &str,
        right_entity_id: &str,
    ) -> Result<matrix::MatrixEntityMatchCandidate, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let left = find_entity(&connection, left_entity_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(left_entity_id.to_string()))?;
        let right = find_entity(&connection, right_entity_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(right_entity_id.to_string()))?;
        let candidate = matrix::match_candidate(&left, &right).ok_or_else(|| {
            MatrixRuntimeStoreError::NotFound(
                "entity match candidate below confidence threshold".to_string(),
            )
        })?;
        insert_entity_match_candidate(&connection, &candidate)?;
        Ok(candidate)
    }

    pub fn decide_entity_conflict(
        &self,
        candidate_id: &str,
        survivor_entity_id: &str,
        retired_entity_id: &str,
        survivorship_rule: &str,
        notes: Option<String>,
    ) -> Result<matrix::MatrixEntityConflictDecision, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity_match_candidate(&connection, candidate_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(candidate_id.to_string()))?;
        let survivor = find_entity(&connection, survivor_entity_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(survivor_entity_id.to_string()))?;
        let retired = find_entity(&connection, retired_entity_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(retired_entity_id.to_string()))?;
        let decision = matrix::MatrixEntityConflictDecision {
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
        insert_entity_conflict_decision(&connection, &decision)?;
        Ok(decision)
    }

    pub fn plan_metric_attention(
        &self,
        trigger_fact_type: &str,
        entity_scope: Option<String>,
        period: Option<String>,
        limit: usize,
    ) -> Result<MatrixMetricAttentionPlan, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<MatrixMetricSnapshot, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = build_metric_snapshot(&connection, metric_ids, scope_ref)?;
        insert_metric_snapshot(&connection, &snapshot)?;
        Ok(snapshot)
    }

    pub fn upsert_relation(
        &self,
        relation: &MatrixRelation,
    ) -> Result<MatrixRelation, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_relation(&connection, relation)
    }

    pub fn list_entity_relations(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> Result<Vec<MatrixRelation>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(MatrixRuntimeStoreError::NotFound(entity_id.to_string()));
        }
        list_entity_relations(&connection, entity_id, limit)
    }

    pub fn impact_trace(
        &self,
        entity_id: &str,
        max_depth: usize,
    ) -> Result<MatrixImpactTrace, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(MatrixRuntimeStoreError::NotFound(entity_id.to_string()));
        }
        build_impact_trace(&connection, entity_id, max_depth)
    }

    pub fn register_metric_definition(
        &self,
        definition: &MatrixMetricDefinition,
    ) -> Result<(), MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_metric_definition(&connection, definition)
    }

    pub fn upsert_metric_dependency(
        &self,
        dependency: &MatrixMetricDependency,
    ) -> Result<MatrixMetricDependency, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_metric_dependency(&connection, dependency)
    }

    pub fn metric_lineage(
        &self,
        metric_id: &str,
        max_depth: usize,
    ) -> Result<MatrixMetricLineage, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        build_metric_lineage(&connection, metric_id, max_depth)
    }

    pub fn metrics_affected_by_fact_type(
        &self,
        fact_type: &str,
    ) -> Result<Vec<String>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics_affected_by_fact_type(&connection, fact_type)
    }

    pub fn plan_compute_job_for_fact_type(
        &self,
        input: MatrixComputeJobInput,
    ) -> Result<MatrixComputePlan, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<Option<MatrixComputeJob>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_compute_job(&connection, job_id)
    }

    pub fn run_compute_job(
        &self,
        job_id: &str,
    ) -> Result<MatrixComputeJob, MatrixRuntimeStoreError> {
        let mut job = {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut job = find_compute_job(&connection, job_id)?
                .ok_or_else(|| MatrixRuntimeStoreError::NotFound(job_id.to_string()))?;
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
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_compute_job(&connection, &job)
    }

    pub fn ingest_fact(
        &self,
        fact: &MatrixFact,
    ) -> Result<MatrixAttentionItem, MatrixRuntimeStoreError> {
        let attention = MatrixAttentionItem::from_fact(
            &fact.fact_id,
            &fact.fact_type,
            fact.entity_refs.first().cloned(),
            fact.confidence,
        );
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<MatrixSourcePack, MatrixRuntimeStoreError> {
        let source_pack = source_pack.normalized();
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        insert_source_pack(&connection, &source_pack)?;
        Ok(source_pack)
    }

    pub fn get_source_pack(
        &self,
        source_pack_id: &str,
    ) -> Result<Option<MatrixSourcePack>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_source_pack(&connection, source_pack_id)
    }

    pub fn list_source_packs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_source_packs(&connection, limit)
    }

    pub fn validate_source_pack(
        &self,
        source_pack_id: &str,
    ) -> Result<MatrixSourcePackValidation, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(source_pack_id.to_string()))?;
        Ok(source_pack.validate())
    }

    pub fn source_pack_delta_plan(
        &self,
        source_pack_id: &str,
    ) -> Result<MatrixSourceDeltaPlan, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(source_pack_id.to_string()))?;
        source_pack_delta_plan_for(&connection, &source_pack)
    }

    pub fn plan_connector_run(
        &self,
        source_pack_id: &str,
        input: MatrixConnectorRunInput,
    ) -> Result<MatrixConnectorRun, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(source_pack_id.to_string()))?;
        let delta_plan = source_pack_delta_plan_for(&connection, &source_pack)?;
        let run = MatrixConnectorRun::from_source_pack(&source_pack, &delta_plan, input);
        insert_connector_run(&connection, &run)?;
        Ok(run)
    }

    pub fn get_connector_run(
        &self,
        run_id: &str,
    ) -> Result<Option<MatrixConnectorRun>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_connector_run(&connection, run_id)
    }

    pub fn list_attention(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT attention_json
              FROM matrix_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn list_facts(&self, limit: usize) -> Result<Vec<MatrixFact>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_facts(&connection, limit)
    }

    pub fn recompute_metrics(
        &self,
    ) -> Result<MatrixMetricRecomputeResult, MatrixRuntimeStoreError> {
        self.recompute_metrics_with_filter(None)
    }

    pub fn recompute_metrics_for_metric_ids(
        &self,
        metric_ids: &[String],
    ) -> Result<MatrixMetricRecomputeResult, MatrixRuntimeStoreError> {
        let filter = metric_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.recompute_metrics_with_filter(Some(&filter))
    }

    fn recompute_metrics_with_filter(
        &self,
        metric_filter: Option<&BTreeSet<String>>,
    ) -> Result<MatrixMetricRecomputeResult, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let facts = metric_facts(&connection)?;
        let mut groups = BTreeMap::<MetricGroupKey, MetricAccumulator>::new();
        for fact in facts {
            if metric_filter.is_some_and(|filter| !filter.contains(&fact.metric_id)) {
                continue;
            }
            groups.entry(fact.key()).or_default().push(fact);
        }

        let mut states = Vec::new();
        let mut changes = Vec::new();
        let mut attention = Vec::new();
        for (key, accumulator) in groups {
            let definition =
                MatrixMetricDefinition::inferred(key.metric_id.clone(), &accumulator.fact_type);
            upsert_metric_definition(&connection, &definition)?;
            let previous =
                latest_metric_state(&connection, &key.metric_id, &key.entity_scope, &key.period)?;
            let previous_value = previous.as_ref().map(|state| state.value);
            let value = accumulator.value;
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
                metric_id: key.metric_id.clone(),
                entity_scope: key.entity_scope.clone(),
                period: key.period.clone(),
                value,
                previous_value,
                delta,
                delta_ratio,
                status: MatrixMetricState::status_for_delta(delta),
                computed_at: Utc::now(),
                input_fact_refs: accumulator.fact_ids.clone(),
                confidence: accumulator.confidence(),
            };
            insert_metric_state(&connection, &state)?;
            states.push(state.clone());

            if delta.abs() > f64::EPSILON {
                let change = MatrixChangeEvent {
                    change_id: format!("change-{}", uuid::Uuid::new_v4()),
                    change_type: "metric_delta".to_string(),
                    entity_ref: key.entity_scope.clone(),
                    metric_id: Some(key.metric_id.clone()),
                    from_value: previous_value.map(Value::from),
                    to_value: Some(Value::from(value)),
                    delta,
                    period: key.period.clone(),
                    detected_at: Utc::now(),
                    source_fact_refs: accumulator.fact_ids.clone(),
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
    ) -> Result<Vec<MatrixMetricDefinition>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<Vec<MatrixMetricState>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    ) -> Result<Vec<MatrixChangeEvent>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let attention = match attention_id {
            Some(id) => Some(
                find_attention(&connection, id)?
                    .ok_or_else(|| MatrixRuntimeStoreError::NotFound(id.to_string()))?,
            ),
            None => latest_attention(&connection)?,
        };
        let mut packet = MatrixEvidencePacket::new(problem_statement.unwrap_or_else(|| {
            attention
                .as_ref()
                .map(|item| item.title.as_str())
                .unwrap_or("MATRIX operational evidence packet")
        }));
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
        insert_evidence_packet(&connection, &packet)?;
        Ok(packet)
    }

    pub fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_evidence_packet(&connection, packet_id)
    }

    pub fn list_evidence_packets(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_evidence_packets(&connection, limit)
    }

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let packet = find_evidence_packet(&connection, packet_id)?
            .ok_or_else(|| MatrixRuntimeStoreError::NotFound(packet_id.to_string()))?;
        let decision = MatrixQualityGateDecision::for_evidence_packet(&packet);
        insert_quality_gate(&connection, &decision)?;
        Ok(decision)
    }

    pub fn get_quality_gate(
        &self,
        gate_id: &str,
    ) -> Result<Option<MatrixQualityGateDecision>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_quality_gate(&connection, gate_id)
    }

    pub fn list_data_plane_watermarks(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixDataPlaneWatermark>, MatrixRuntimeStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_data_plane_watermarks(&connection, limit)
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
        VALUES (1, 17, datetime('now'))
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

fn insert_ontology_pack(
    connection: &Connection,
    pack: &MatrixOntologyPack,
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixOntologyPack>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT pack_json FROM matrix_ontology_pack WHERE ontology_id = ?1",
            params![ontology_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn insert_entity_match_candidate(
    connection: &Connection,
    candidate: &matrix::MatrixEntityMatchCandidate,
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<matrix::MatrixEntityMatchCandidate>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT candidate_json FROM matrix_entity_match_candidate WHERE candidate_id = ?1",
            params![candidate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn insert_entity_conflict_decision(
    connection: &Connection,
    decision: &matrix::MatrixEntityConflictDecision,
) -> Result<(), MatrixRuntimeStoreError> {
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

fn upsert_entity(
    connection: &Connection,
    entity: &MatrixEntity,
) -> Result<MatrixEntity, MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixEntity>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT entity_json FROM matrix_entity WHERE entity_id = ?1",
            params![entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn find_entity_by_canonical(
    connection: &Connection,
    entity_type: &str,
    canonical_key: &str,
) -> Result<Option<MatrixEntity>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            r"SELECT entity_json
              FROM matrix_entity
              WHERE entity_type = ?1 AND canonical_key = ?2",
            params![entity_type, canonical_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn find_entity_by_source_key(
    connection: &Connection,
    source_system: &str,
    source_key: &str,
) -> Result<Option<MatrixEntity>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            r"SELECT e.entity_json
              FROM matrix_entity_source_key s
              JOIN matrix_entity e ON e.entity_id = s.entity_id
              WHERE s.source_system = ?1 AND s.source_key = ?2",
            params![
                matrix::normalize_key(source_system),
                matrix::normalize_key(source_key),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn list_entities(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEntity>, MatrixRuntimeStoreError> {
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
) -> Result<MatrixRelation, MatrixRuntimeStoreError> {
    if find_entity(connection, &relation.from_entity_id)?.is_none() {
        return Err(MatrixRuntimeStoreError::NotFound(
            relation.from_entity_id.clone(),
        ));
    }
    if find_entity(connection, &relation.to_entity_id)?.is_none() {
        return Err(MatrixRuntimeStoreError::NotFound(
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
) -> Result<Option<MatrixRelation>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            r"SELECT relation_json
              FROM matrix_relation
              WHERE relation_type = ?1 AND from_entity_id = ?2 AND to_entity_id = ?3",
            params![relation_type, from_entity_id, to_entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn list_entity_relations(
    connection: &Connection,
    entity_id: &str,
    limit: usize,
) -> Result<Vec<MatrixRelation>, MatrixRuntimeStoreError> {
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
) -> Result<MatrixImpactTrace, MatrixRuntimeStoreError> {
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
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Vec<MatrixAttentionItem>, MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixAttentionItem>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT attention_json FROM matrix_attention_item WHERE attention_id = ?1",
            params![attention_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn latest_attention(
    connection: &Connection,
) -> Result<Option<MatrixAttentionItem>, MatrixRuntimeStoreError> {
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
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn insert_evidence_packet(
    connection: &Connection,
    packet: &MatrixEvidencePacket,
) -> Result<(), MatrixRuntimeStoreError> {
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

fn find_evidence_packet(
    connection: &Connection,
    packet_id: &str,
) -> Result<Option<MatrixEvidencePacket>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT packet_json FROM matrix_evidence_packet WHERE packet_id = ?1",
            params![packet_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn list_evidence_packets(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEvidencePacket>, MatrixRuntimeStoreError> {
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
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixQualityGateDecision>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT gate_json FROM matrix_quality_gate WHERE gate_id = ?1",
            params![gate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn list_recent_quality_gates(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixQualityGateDecision>, MatrixRuntimeStoreError> {
    let mut statement = connection.prepare(
        r"SELECT gate_json
          FROM matrix_quality_gate
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricGroupKey {
    metric_id: String,
    entity_scope: String,
    period: String,
}

#[derive(Debug, Clone)]
struct MetricFactRow {
    fact_id: String,
    fact_type: String,
    metric_id: String,
    entity_scope: String,
    period: String,
    value: f64,
    confidence: f32,
}

impl MetricFactRow {
    fn key(&self) -> MetricGroupKey {
        MetricGroupKey {
            metric_id: self.metric_id.clone(),
            entity_scope: self.entity_scope.clone(),
            period: self.period.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MetricAccumulator {
    fact_type: String,
    value: f64,
    fact_ids: Vec<String>,
    confidence_sum: f32,
}

impl MetricAccumulator {
    fn push(&mut self, fact: MetricFactRow) {
        if self.fact_type.is_empty() {
            self.fact_type = fact.fact_type;
        }
        self.value += fact.value;
        self.fact_ids.push(format!("matrix:fact:{}", fact.fact_id));
        self.confidence_sum += fact.confidence;
    }

    fn confidence(&self) -> f32 {
        if self.fact_ids.is_empty() {
            0.0
        } else {
            self.confidence_sum / self.fact_ids.len() as f32
        }
    }
}

fn metric_facts(connection: &Connection) -> Result<Vec<MetricFactRow>, MatrixRuntimeStoreError> {
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
        let value = numeric_measure_sum(&measures);
        facts.push(MetricFactRow {
            fact_id,
            fact_type,
            metric_id,
            entity_scope,
            period,
            value,
            confidence,
        });
    }
    Ok(facts)
}

fn list_facts(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixFact>, MatrixRuntimeStoreError> {
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

fn numeric_measure_sum(value: &Value) -> f64 {
    match value {
        Value::Number(number) => number.as_f64().unwrap_or(0.0),
        Value::Object(map) => map.values().map(numeric_measure_sum).sum(),
        Value::Array(items) => items.iter().map(numeric_measure_sum).sum(),
        _ => 0.0,
    }
}

fn upsert_metric_definition(
    connection: &Connection,
    definition: &MatrixMetricDefinition,
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixMetricDefinition>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT definition_json FROM matrix_metric_definition WHERE metric_id = ?1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn upsert_metric_dependency(
    connection: &Connection,
    dependency: &MatrixMetricDependency,
) -> Result<MatrixMetricDependency, MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixMetricDependency>, MatrixRuntimeStoreError> {
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
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn list_upstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MatrixRuntimeStoreError> {
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
) -> Result<Vec<MatrixMetricDependency>, MatrixRuntimeStoreError> {
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
) -> Result<MatrixMetricLineage, MatrixRuntimeStoreError> {
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
) -> Result<Vec<String>, MatrixRuntimeStoreError> {
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
) -> Result<Vec<String>, MatrixRuntimeStoreError> {
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
) -> Result<MatrixMetricAttentionPlan, MatrixRuntimeStoreError> {
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
) -> Result<MatrixMetricSnapshot, MatrixRuntimeStoreError> {
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
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<MatrixComputeJob, MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixComputeJob>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT job_json FROM matrix_compute_job WHERE job_id = ?1",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn latest_metric_state(
    connection: &Connection,
    metric_id: &str,
    entity_scope: &str,
    period: &str,
) -> Result<Option<MatrixMetricState>, MatrixRuntimeStoreError> {
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
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn insert_metric_state(
    connection: &Connection,
    state: &MatrixMetricState,
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixChangeEvent>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT change_json FROM matrix_change_event WHERE change_id = ?1",
            params![change_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn latest_metric_state_for_metric(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricState>, MatrixRuntimeStoreError> {
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
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn insert_source_pack(
    connection: &Connection,
    source_pack: &MatrixSourcePack,
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixSourcePack>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT source_pack_json FROM matrix_source_pack WHERE source_pack_id = ?1",
            params![source_pack_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn list_source_packs(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixSourcePack>, MatrixRuntimeStoreError> {
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
) -> Result<MatrixSourceDeltaPlan, MatrixRuntimeStoreError> {
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
) -> Result<(), MatrixRuntimeStoreError> {
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
) -> Result<Option<MatrixConnectorRun>, MatrixRuntimeStoreError> {
    connection
        .query_row(
            "SELECT run_json FROM matrix_connector_run WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixRuntimeStoreError::from))
        .transpose()
}

fn upsert_data_plane_watermark(
    connection: &Connection,
    watermark: &MatrixDataPlaneWatermark,
) -> Result<(), MatrixRuntimeStoreError> {
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

fn list_data_plane_watermarks(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixDataPlaneWatermark>, MatrixRuntimeStoreError> {
    let mut statement = connection.prepare(
        r"SELECT watermark_json
          FROM matrix_data_plane_watermark
          ORDER BY updated_at DESC, source_ref ASC, fact_type ASC, partition_ref ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn parse_rfc3339_utc(value: &str) -> Result<chrono::DateTime<Utc>, MatrixRuntimeStoreError> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            MatrixRuntimeStoreError::Json(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            )))
        })?
        .with_timezone(&Utc))
}

fn parse_optional_rfc3339_utc(
    value: Option<String>,
) -> Result<Option<chrono::DateTime<Utc>>, MatrixRuntimeStoreError> {
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
