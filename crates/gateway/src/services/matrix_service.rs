use std::path::{Path, PathBuf};

use chrono::Utc;
use connector::{SourceIngestionReceipt, SourceRecordBatch, SourceWatermark};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixComputeJobInput,
    MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput, MatrixDataPlaneHealth,
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
    MatrixEntity, MatrixEntityConflictDecision, MatrixEntityInput, MatrixEntityMatchCandidate,
    MatrixEvidencePacket, MatrixFact, MatrixFactInput, MatrixImpactTrace,
    MatrixMetricAttentionPlan, MatrixMetricDefinition, MatrixMetricDependency, MatrixMetricLineage,
    MatrixMetricSnapshot, MatrixMetricState, MatrixQualityGateDecision, MatrixRelation,
    MatrixRelationInput, MatrixSourceDeltaPlan, MatrixSourceFactMapping, MatrixSourcePack,
    MatrixSourcePackValidation, MatrixSourceSnapshot, MatrixSourceSnapshotApplyReport,
    MatrixSourceSnapshotInput, MatrixSourceSnapshotPlan,
};
use matrix_repository::{MatrixHealth, MatrixRepository};
use serde_json::Value;

use super::{GatewayMatrixRepositoryError, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct MatrixService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl MatrixService {
    pub(crate) fn new() -> Self {
        Self {
            label: "matrix",
            owner: "0.9.297 Matrix core boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_ready",
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    pub(crate) fn repository_handle(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<
        ::matrix_repository::MatrixRepositoryHandle,
        ::matrix_repository::MatrixRepositoryError,
    > {
        ::matrix_repository::MatrixRepositoryHandle::from_config_home(config_home)
    }

    pub(crate) fn store_path(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<PathBuf, ::matrix_repository::MatrixRepositoryError> {
        Ok(self.repository_handle(config_home)?.db_path().to_path_buf())
    }

    pub(crate) fn health(&self) -> ServiceEnvelope {
        self.envelope("health")
    }

    pub(crate) fn structured_projection(&self) -> ServiceEnvelope {
        self.envelope("structured_projection")
    }

    pub(crate) fn repository(&self) -> ServiceEnvelope {
        self.envelope("repository")
    }

    pub(crate) fn resource_revision(
        &self,
        config_home: impl AsRef<Path>,
        resource_kind: &str,
        resource_id: &str,
    ) -> Result<u64, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .resource_revision_for_existing(resource_kind, resource_id)
    }

    pub(crate) fn repository_health(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MatrixHealth, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.health()
    }

    pub(crate) fn data_plane_health(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MatrixDataPlaneHealth, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.data_plane_health()
    }

    pub(crate) fn upsert_source_pack(
        &self,
        config_home: impl AsRef<Path>,
        source_pack: MatrixSourcePack,
    ) -> Result<MatrixSourcePack, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .upsert_source_pack(source_pack)
    }

    pub(crate) fn upsert_source_pack_checked(
        &self,
        config_home: impl AsRef<Path>,
        source_pack: MatrixSourcePack,
        expected_revision: Option<u64>,
    ) -> Result<matrix_repository::MatrixRevisioned<MatrixSourcePack>, GatewayMatrixRepositoryError>
    {
        self.sqlite_repository(config_home)?
            .upsert_source_pack_checked(source_pack, expected_revision)
    }

    pub(crate) fn list_source_packs(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .list_source_packs(limit)
    }

    pub(crate) fn get_source_pack(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
    ) -> Result<Option<MatrixSourcePack>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .get_source_pack(source_pack_id)
    }

    pub(crate) fn validate_source_pack(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
    ) -> Result<MatrixSourcePackValidation, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .validate_source_pack(source_pack_id)
    }

    pub(crate) fn source_pack_delta_plan(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
    ) -> Result<MatrixSourceDeltaPlan, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .source_pack_delta_plan(source_pack_id)
    }

    pub(crate) fn plan_connector_run(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
        input: MatrixConnectorRunInput,
    ) -> Result<MatrixConnectorRun, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .plan_connector_run(source_pack_id, input)
    }

    pub(crate) fn get_connector_run(
        &self,
        config_home: impl AsRef<Path>,
        run_id: &str,
    ) -> Result<Option<MatrixConnectorRun>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .get_connector_run(run_id)
    }

    pub(crate) fn plan_source_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
        resource_ref: Option<String>,
        estimated_rows: Option<u64>,
    ) -> Result<MatrixSourceSnapshotPlan, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.plan_source_snapshot(
            source_pack_id,
            resource_ref,
            estimated_rows,
        )
    }

    pub(crate) fn create_source_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        input: MatrixSourceSnapshotInput,
    ) -> Result<MatrixSourceSnapshot, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .create_source_snapshot(input)
    }

    pub(crate) fn get_source_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        snapshot_id: &str,
    ) -> Result<Option<MatrixSourceSnapshot>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .get_source_snapshot(snapshot_id)
    }

    pub(crate) fn list_source_snapshots(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MatrixSourceSnapshot>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .list_source_snapshots(source_pack_id, limit)
    }

    pub(crate) fn apply_source_snapshot_rows(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
        snapshot: MatrixSourceSnapshot,
        rows: &[Value],
    ) -> Result<MatrixSourceSnapshotApplyReport, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .apply_source_snapshot_rows(source_pack_id, snapshot, rows)
    }

    pub(crate) fn ingest_source_record_batch(
        &self,
        config_home: impl AsRef<Path>,
        batch: &SourceRecordBatch,
        watermark_before: Option<SourceWatermark>,
        watermark_after: Option<SourceWatermark>,
    ) -> Result<SourceIngestionReceipt, GatewayMatrixRepositoryError> {
        let repository = self.sqlite_repository(&config_home)?;
        let source_pack_id = source_pack_id_for_batch(batch);
        let table = batch
            .table
            .clone()
            .unwrap_or_else(|| batch.schema.table_name.clone());
        let now = Utc::now();
        let source_pack = MatrixSourcePack {
            source_pack_id: source_pack_id.clone(),
            source_name: format!("edge_source_{}", batch.adapter_id),
            owner: "gateway.connector_source".to_string(),
            access_mode: source_access_mode(&batch.adapter_id).to_string(),
            refresh_mode: "incremental".to_string(),
            entity_mappings: Vec::new(),
            fact_mappings: vec![MatrixSourceFactMapping {
                source_table: table.clone(),
                fact_type: format!("source.{}.row", batch.adapter_id),
                metric_key: format!("source_{}_rows", batch.adapter_id),
                entity_ref_fields: batch.schema.primary_key.clone(),
                measure_fields: Vec::new(),
                event_time_field: None,
                dedup_key: batch
                    .schema
                    .primary_key
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "_row_hash".to_string()),
                delta_signature: batch.checksum.clone(),
            }],
            relation_mappings: Vec::new(),
            reconciliation_rules: vec!["source_row_checksum_is_idempotency_key".to_string()],
            quality_rules: vec![
                "resource_ref_required".to_string(),
                "row_checksum_required".to_string(),
            ],
            freshness_sla: Some("watermark_driven".to_string()),
            security_policy: Some("edge_source_connector_contract".to_string()),
            metadata: serde_json::json!({
                "adapter_id": batch.adapter_id,
                "resource_ref": batch.resource_ref,
                "table": table,
                "schema": batch.schema,
            }),
            created_at: now,
            updated_at: now,
        };
        repository.upsert_source_pack(source_pack)?;
        let snapshot = repository.create_source_snapshot(MatrixSourceSnapshotInput {
            snapshot_id: None,
            source_pack_id: Some(source_pack_id.clone()),
            source_system: batch.adapter_id.clone(),
            source_kind: source_kind_for_batch(&batch.adapter_id),
            resource_ref: Some(batch.resource_ref.clone()),
            business_period: None,
            captured_at: Some(now),
            schema_version: Some(format!(
                "source:{}:{}",
                batch.adapter_id, batch.schema.table_name
            )),
            row_count: Some(batch.rows.len() as u64),
            checksum: Some(batch.checksum.clone()),
            confidence: Some(0.95),
            metadata: serde_json::json!({
                "delivery": "connector_source_incremental_run",
                "cursor": batch.cursor,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
            }),
        })?;
        let apply_report = repository.apply_source_snapshot_rows(
            &source_pack_id,
            snapshot.clone(),
            &batch.rows,
        )?;
        let plan = repository.plan_data_plane_ingest(MatrixDataPlaneIngestPlanInput {
            source_ref: batch.resource_ref.clone(),
            fact_type: format!("source.{}.row", batch.adapter_id),
            partition_ref: batch.table.clone(),
            high_watermark: watermark_after
                .as_ref()
                .and_then(|watermark| {
                    watermark
                        .high_watermark
                        .clone()
                        .or_else(|| watermark.cursor.clone())
                })
                .or_else(|| Some(batch.checksum.clone())),
            estimated_rows: Some(batch.rows.len() as u64),
            raw_checksum: Some(batch.checksum.clone()),
            metric_ids: Vec::new(),
        })?;
        let receipt_id = stable_receipt_id(&[
            batch.adapter_id.as_str(),
            batch.resource_ref.as_str(),
            batch.table.as_deref().unwrap_or(""),
            batch.checksum.as_str(),
        ]);
        Ok(SourceIngestionReceipt {
            receipt_id: format!("source-receipt-{receipt_id}"),
            adapter_id: batch.adapter_id.clone(),
            resource_ref: batch.resource_ref.clone(),
            row_count: batch.rows.len(),
            checksum: batch.checksum.clone(),
            watermark_before,
            watermark_after,
            matrix_refs: vec![
                format!("matrix:source_pack:{source_pack_id}"),
                format!("matrix:source_snapshot:{}", snapshot.snapshot_id),
                format!("matrix:apply_report:{}", apply_report.fact_count),
                format!("matrix:data_plane_batch:{}", plan.batch_id),
            ],
            created_at_ms: Utc::now().timestamp_millis(),
        })
    }

    pub(crate) fn plan_data_plane_ingest(
        &self,
        config_home: impl AsRef<Path>,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> Result<MatrixDataPlaneIngestPlan, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .plan_data_plane_ingest(input)
    }

    pub(crate) fn list_facts(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixFact>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.list_facts(limit)
    }

    pub(crate) fn ingest_fact(
        &self,
        config_home: impl AsRef<Path>,
        fact: &MatrixFact,
    ) -> Result<MatrixAttentionItem, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.ingest_fact(fact)
    }

    pub(crate) fn list_entities(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixEntity>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.list_entities(limit)
    }

    pub(crate) fn upsert_entity(
        &self,
        config_home: impl AsRef<Path>,
        entity: &MatrixEntity,
    ) -> Result<MatrixEntity, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.upsert_entity(entity)
    }

    pub(crate) fn upsert_entity_checked(
        &self,
        config_home: impl AsRef<Path>,
        entity: &MatrixEntity,
        expected_revision: Option<u64>,
    ) -> Result<matrix_repository::MatrixRevisioned<MatrixEntity>, GatewayMatrixRepositoryError>
    {
        self.sqlite_repository(config_home)?
            .upsert_entity_checked(entity, expected_revision)
    }

    pub(crate) fn get_entity(
        &self,
        config_home: impl AsRef<Path>,
        entity_id: &str,
    ) -> Result<Option<MatrixEntity>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.get_entity(entity_id)
    }

    pub(crate) fn resolve_entity_by_source_key(
        &self,
        config_home: impl AsRef<Path>,
        source_system: &str,
        source_key: &str,
    ) -> Result<Option<MatrixEntity>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .resolve_entity_by_source_key(source_system, source_key)
    }

    pub(crate) fn propose_entity_match(
        &self,
        config_home: impl AsRef<Path>,
        left_entity_id: &str,
        right_entity_id: &str,
    ) -> Result<MatrixEntityMatchCandidate, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .propose_entity_match(left_entity_id, right_entity_id)
    }

    pub(crate) fn decide_entity_conflict(
        &self,
        config_home: impl AsRef<Path>,
        candidate_id: &str,
        survivor_entity_id: &str,
        retired_entity_id: &str,
        survivorship_rule: &str,
        notes: Option<String>,
    ) -> Result<MatrixEntityConflictDecision, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.decide_entity_conflict(
            candidate_id,
            survivor_entity_id,
            retired_entity_id,
            survivorship_rule,
            notes,
        )
    }

    pub(crate) fn upsert_relation(
        &self,
        config_home: impl AsRef<Path>,
        relation: &MatrixRelation,
    ) -> Result<MatrixRelation, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .upsert_relation(relation)
    }

    pub(crate) fn upsert_relation_checked(
        &self,
        config_home: impl AsRef<Path>,
        relation: &MatrixRelation,
        expected_revision: Option<u64>,
    ) -> Result<matrix_repository::MatrixRevisioned<MatrixRelation>, GatewayMatrixRepositoryError>
    {
        self.sqlite_repository(config_home)?
            .upsert_relation_checked(relation, expected_revision)
    }

    pub(crate) fn list_entity_relations(
        &self,
        config_home: impl AsRef<Path>,
        entity_id: &str,
        limit: usize,
    ) -> Result<Vec<MatrixRelation>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .list_entity_relations(entity_id, limit)
    }

    pub(crate) fn impact_trace(
        &self,
        config_home: impl AsRef<Path>,
        entity_id: &str,
        max_depth: usize,
    ) -> Result<MatrixImpactTrace, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .impact_trace(entity_id, max_depth)
    }

    pub(crate) fn list_evidence_packets(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .list_evidence_packets(limit)
    }

    pub(crate) fn list_metric_definitions(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<Vec<MatrixMetricDefinition>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .list_metric_definitions()
    }

    pub(crate) fn metric_states(
        &self,
        config_home: impl AsRef<Path>,
        metric_id: &str,
    ) -> Result<Vec<MatrixMetricState>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .metric_states(metric_id)
    }

    pub(crate) fn metric_lineage(
        &self,
        config_home: impl AsRef<Path>,
        metric_id: &str,
        max_depth: usize,
    ) -> Result<MatrixMetricLineage, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .metric_lineage(metric_id, max_depth)
    }

    pub(crate) fn plan_metric_attention(
        &self,
        config_home: impl AsRef<Path>,
        trigger_fact_type: &str,
        entity_scope: Option<String>,
        period: Option<String>,
        limit: usize,
    ) -> Result<MatrixMetricAttentionPlan, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.plan_metric_attention(
            trigger_fact_type,
            entity_scope,
            period,
            limit,
        )
    }

    pub(crate) fn materialize_metric_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        metric_ids: Vec<String>,
        scope_ref: Option<String>,
    ) -> Result<MatrixMetricSnapshot, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .materialize_metric_snapshot(metric_ids, scope_ref)
    }

    pub(crate) fn upsert_metric_dependency(
        &self,
        config_home: impl AsRef<Path>,
        dependency: &MatrixMetricDependency,
    ) -> Result<MatrixMetricDependency, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .upsert_metric_dependency(dependency)
    }

    pub(crate) fn upsert_metric_dependency_checked(
        &self,
        config_home: impl AsRef<Path>,
        dependency: &MatrixMetricDependency,
        expected_revision: Option<u64>,
    ) -> Result<
        matrix_repository::MatrixRevisioned<MatrixMetricDependency>,
        GatewayMatrixRepositoryError,
    > {
        self.sqlite_repository(config_home)?
            .upsert_metric_dependency_checked(dependency, expected_revision)
    }

    pub(crate) fn metrics_affected_by_fact_type(
        &self,
        config_home: impl AsRef<Path>,
        fact_type: &str,
    ) -> Result<Vec<String>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .metrics_affected_by_fact_type(fact_type)
    }

    pub(crate) fn plan_compute_job_for_fact_type(
        &self,
        config_home: impl AsRef<Path>,
        input: MatrixComputeJobInput,
    ) -> Result<MatrixComputePlan, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .plan_compute_job_for_fact_type(input)
    }

    pub(crate) fn get_compute_job(
        &self,
        config_home: impl AsRef<Path>,
        job_id: &str,
    ) -> Result<Option<MatrixComputeJob>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.get_compute_job(job_id)
    }

    pub(crate) fn run_compute_job(
        &self,
        config_home: impl AsRef<Path>,
        job_id: &str,
    ) -> Result<MatrixComputeJob, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.run_compute_job(job_id)
    }

    pub(crate) fn recompute_metrics(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<matrix_repository::MatrixMetricRecomputeResult, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.recompute_metrics()
    }

    pub(crate) fn list_changes(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixChangeEvent>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.list_changes(limit)
    }

    pub(crate) fn list_attention(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?.list_attention(limit)
    }

    pub(crate) fn build_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> Result<MatrixEvidencePacket, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .build_evidence_packet(attention_id, problem_statement)
    }

    pub(crate) fn insert_ai_harness_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet: &MatrixEvidencePacket,
    ) -> Result<MatrixEvidencePacket, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .insert_ai_harness_evidence_packet(packet)
    }

    pub(crate) fn ingest_knowledge_bridge(
        &self,
        config_home: impl AsRef<Path>,
        input: memory::KnowledgeMatrixBridgeInput,
    ) -> Result<Vec<MatrixAttentionItem>, GatewayMatrixRepositoryError> {
        let repository = self.sqlite_repository(config_home)?;
        let source_pack = MatrixSourcePack {
            source_pack_id: input.source_pack_id.clone(),
            source_name: input.source_name.clone(),
            owner: "memory.knowledge_fabric".to_string(),
            access_mode: "internal_bridge".to_string(),
            refresh_mode: "on_knowledge_pack_update".to_string(),
            entity_mappings: Vec::new(),
            fact_mappings: input
                .facts
                .iter()
                .map(|fact| MatrixSourceFactMapping {
                    source_table: "knowledge_canon".to_string(),
                    fact_type: fact.fact_type.clone(),
                    metric_key: fact.fact_type.clone(),
                    entity_ref_fields: vec!["pack_id".to_string()],
                    measure_fields: vec!["confidence".to_string()],
                    event_time_field: None,
                    dedup_key: fact.fact_id.clone(),
                    delta_signature: fact.source_ref.clone(),
                })
                .collect(),
            relation_mappings: Vec::new(),
            reconciliation_rules: vec!["knowledge_fabric_is_source_of_truth".to_string()],
            quality_rules: vec!["evidence_ref_required".to_string()],
            freshness_sla: Some("on_update".to_string()),
            security_policy: Some("gateway_read_projection_only".to_string()),
            metadata: serde_json::json!({
                "kind": "knowledge_matrix_bridge",
                "pack_id": input.pack_id,
            }),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        repository.upsert_source_pack(source_pack)?;

        let pack_entity = MatrixEntity::from_input(MatrixEntityInput {
            entity_id: Some(input.pack_id.clone()),
            entity_type: "knowledge_pack".to_string(),
            canonical_key: input.pack_id.clone(),
            display_name: Some(input.source_name.clone()),
            source_keys: Vec::new(),
            attributes: serde_json::json!({"source_pack_id": input.source_pack_id}),
            confidence: Some(1.0),
        });
        repository.upsert_entity(&pack_entity)?;
        for fact in &input.facts {
            let fact_entity = MatrixEntity::from_input(MatrixEntityInput {
                entity_id: Some(fact.fact_id.clone()),
                entity_type: fact.fact_type.clone(),
                canonical_key: fact.fact_id.clone(),
                display_name: Some(fact.summary.clone()),
                source_keys: Vec::new(),
                attributes: serde_json::json!({"source_ref": fact.source_ref}),
                confidence: Some(fact.confidence),
            });
            repository.upsert_entity(&fact_entity)?;
        }

        for relation in input.relations {
            let matrix_relation = MatrixRelation::from_input(MatrixRelationInput {
                relation_id: Some(relation.relation_id),
                relation_type: relation.relation_type,
                from_entity_id: relation.from_ref,
                to_entity_id: relation.to_ref,
                attributes: serde_json::json!({"source": "knowledge_fabric"}),
                confidence: Some(relation.confidence),
            });
            repository.upsert_relation(&matrix_relation)?;
        }

        let mut attention = Vec::new();
        for fact in input.facts {
            let matrix_fact = MatrixFact::from_input(MatrixFactInput {
                fact_id: Some(fact.fact_id),
                snapshot_id: Some(input.source_pack_id.clone()),
                fact_type: fact.fact_type,
                entity_refs: vec![input.pack_id.clone()],
                metric_key: Some("knowledge_fabric".to_string()),
                dimensions: serde_json::json!({
                    "summary": fact.summary,
                    "evidence_refs": fact.evidence_refs,
                }),
                measures: serde_json::json!({"confidence": fact.confidence}),
                event_time: Some(chrono::Utc::now()),
                valid_from: None,
                valid_to: None,
                source_ref: Some(fact.source_ref),
                confidence: Some(fact.confidence),
                raw_hash: None,
            });
            attention.push(repository.ingest_fact(&matrix_fact)?);
        }
        Ok(attention)
    }

    pub(crate) fn get_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .get_evidence_packet(packet_id)
    }

    pub(crate) fn evaluate_evidence_quality(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .evaluate_evidence_quality(packet_id)
    }

    pub(crate) fn get_quality_gate(
        &self,
        config_home: impl AsRef<Path>,
        gate_id: &str,
    ) -> Result<Option<MatrixQualityGateDecision>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .get_quality_gate(gate_id)
    }

    pub(crate) fn list_data_plane_watermarks(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixDataPlaneWatermark>, GatewayMatrixRepositoryError> {
        self.sqlite_repository(config_home)?
            .list_data_plane_watermarks(limit)
    }

    pub(crate) fn structured_runtime_ready(&self, config_home: impl AsRef<Path>) -> (bool, bool) {
        let Ok(store) = self.sqlite_repository(config_home) else {
            return (false, false);
        };
        let indexes_ready = store.list_source_packs(1).is_ok()
            && store.list_facts(1).is_ok()
            && store.list_evidence_packets(1).is_ok();
        let watermarks_ready = store.list_data_plane_watermarks(1).is_ok();
        (indexes_ready, watermarks_ready)
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.health(),
            self.structured_projection(),
            self.repository(),
        ]
    }
}

fn source_pack_id_for_batch(batch: &SourceRecordBatch) -> String {
    let table = batch
        .table
        .as_deref()
        .unwrap_or(batch.schema.table_name.as_str());
    format!(
        "edge-source-{}-{}-{}",
        sanitize_id(&batch.adapter_id),
        sanitize_id(&batch.resource_ref),
        sanitize_id(table)
    )
}

fn source_access_mode(adapter_id: &str) -> &'static str {
    match adapter_id {
        "postgres" | "mysql" | "mariadb" | "sqlite" => "database_service",
        "feishu_bitable" | "lark_bitable" => "api",
        "csv" | "jsonl" | "local_file_batch" => "file",
        _ => "connector",
    }
}

fn source_kind_for_batch(adapter_id: &str) -> matrix_core::MatrixSourceKind {
    match adapter_id {
        "csv" | "jsonl" | "local_file_batch" => matrix_core::MatrixSourceKind::File,
        "sqlite" | "postgres" | "mysql" | "mariadb" => matrix_core::MatrixSourceKind::Db,
        "feishu_bitable" | "lark_bitable" => matrix_core::MatrixSourceKind::Api,
        _ => matrix_core::MatrixSourceKind::Connector,
    }
}

fn sanitize_id(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    normalized
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
        .chars()
        .take(80)
        .collect()
}

fn stable_receipt_id(parts: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
        .chars()
        .take(16)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
    };
    use memory::{DocumentContent, KnowledgeFabric};

    #[test]
    fn knowledge_matrix_bridge_writes_source_pack_and_facts() {
        let config_home = std::env::temp_dir().join(format!(
            "cowd-knowledge-matrix-bridge-{}",
            uuid::Uuid::new_v4()
        ));
        let fabric = KnowledgeFabric::new();
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::Domain("architecture".to_string()),
            KnowledgeActivationPolicy::DefaultForDomain,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new(
                "Architecture Matrix Rules",
                "must write knowledge rules to matrix\nStep 1. bridge canon",
            ),
        );
        let bridge = fabric
            .matrix_bridge_for_pack(&receipt.pack.pack_id)
            .expect("bridge input");
        let service = MatrixService::new();
        let attention = service
            .ingest_knowledge_bridge(&config_home, bridge)
            .expect("bridge ingest");

        assert!(!attention.is_empty());
        assert!(!service
            .list_source_packs(&config_home, 10)
            .expect("source packs")
            .is_empty());
        assert!(service
            .list_facts(&config_home, 10)
            .expect("facts")
            .iter()
            .any(|fact| fact.fact_type.starts_with("knowledge_")));
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[test]
    fn source_record_batch_ingests_to_source_pack_and_watermark() {
        let config_home =
            std::env::temp_dir().join(format!("cowd-source-record-batch-{}", uuid::Uuid::new_v4()));
        let service = MatrixService::new();
        let batch = SourceRecordBatch {
            adapter_id: "csv".to_string(),
            resource_ref: "file:///tmp/orders.csv".to_string(),
            table: Some("orders".to_string()),
            schema: connector::SourceTableSchema {
                table_name: "orders".to_string(),
                fields: vec![connector::SourceFieldSchema {
                    name: "order_id".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                }],
                primary_key: vec!["order_id".to_string()],
            },
            rows: vec![serde_json::json!({"order_id": "O-1", "qty": 3})],
            cursor: connector::SourceBatchCursor {
                offset: 0,
                limit: 10,
                next_offset: None,
            },
            row_count: 1,
            checksum: "sha256:test".to_string(),
            truncated: false,
        };
        let watermark_after = SourceWatermark {
            adapter_id: "csv".to_string(),
            resource_ref: batch.resource_ref.clone(),
            table: batch.table.clone(),
            strategy: "offset".to_string(),
            cursor: Some("1".to_string()),
            offset: Some(1),
            high_watermark: None,
            checksum: Some(batch.checksum.clone()),
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        let receipt = service
            .ingest_source_record_batch(&config_home, &batch, None, Some(watermark_after))
            .expect("source batch receipt");

        assert_eq!(receipt.row_count, 1);
        assert!(receipt
            .matrix_refs
            .iter()
            .any(|value| value.starts_with("matrix:source_pack:")));
        assert!(!service
            .list_source_packs(&config_home, 10)
            .expect("source packs")
            .is_empty());
        assert!(!service
            .list_source_snapshots(&config_home, None, 10)
            .expect("snapshots")
            .is_empty());
        assert!(!service
            .list_data_plane_watermarks(&config_home, 10)
            .expect("watermarks")
            .is_empty());
        let _ = std::fs::remove_dir_all(config_home);
    }
}
