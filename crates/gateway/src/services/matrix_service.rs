use std::path::Path;

use chrono::Utc;
use connector::{SourceIngestionReceipt, SourceRecordBatch, SourceWatermark};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixComputeJobInput,
    MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput, MatrixDataPlaneHealth,
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
    MatrixEntity, MatrixEntityConflictDecision, MatrixEntityMatchCandidate, MatrixEvidencePacket,
    MatrixFact, MatrixImpactTrace, MatrixMetricAttentionPlan, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricState,
    MatrixQualityGateDecision, MatrixRelation, MatrixSourceDeltaPlan, MatrixSourceFactMapping,
    MatrixSourcePack, MatrixSourcePackValidation, MatrixSourceSnapshot,
    MatrixSourceSnapshotApplyReport, MatrixSourceSnapshotInput, MatrixSourceSnapshotPlan,
};
use matrix_repository::MatrixHealth;
use serde_json::Value;

use super::{GatewayMatrixRepositoryError, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct MatrixService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    pub(crate) selected_store: Option<std::sync::Arc<dyn matrix_repository::MatrixStore>>,
    selected_endpoint: Option<storage::StorageEndpoint>,
}

impl MatrixService {
    pub(crate) fn new() -> Self {
        Self {
            label: "matrix",
            owner: "0.9.297 Matrix core boundary",
            selected_store: None,
            selected_endpoint: None,
        }
    }

    pub(crate) fn with_store(
        store: std::sync::Arc<dyn matrix_repository::MatrixStore>,
        endpoint: storage::StorageEndpoint,
    ) -> Self {
        Self {
            label: "matrix",
            owner: "0.9.581 selected Matrix storage boundary",
            selected_store: Some(store),
            selected_endpoint: Some(endpoint),
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

    pub(crate) fn storage_projection(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<serde_json::Value, GatewayMatrixRepositoryError> {
        let fallback;
        let endpoint = if let Some(endpoint) = self.selected_endpoint.as_ref() {
            endpoint
        } else {
            fallback = storage::StorageRegistry::default_for_config_home(config_home)
                .endpoint(&storage::StorageDomainId::Matrix)
                .cloned()
                .map_err(|error| GatewayMatrixRepositoryError::Backend(error.to_string()))?;
            &fallback
        };
        Ok(serde_json::json!({
            "logical_id": endpoint.logical_id(),
            "backend": endpoint.backend,
            "owner": endpoint.owner,
            "storage_domain": endpoint.domain,
            "storage_scope": endpoint.scope,
            "migration": endpoint.migration,
        }))
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
        self.store(config_home)?
            .resource_revision_for_existing(resource_kind, resource_id)
    }

    pub(crate) fn repository_health(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MatrixHealth, GatewayMatrixRepositoryError> {
        self.store(config_home)?.health()
    }

    pub(crate) fn data_plane_health(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MatrixDataPlaneHealth, GatewayMatrixRepositoryError> {
        self.store(config_home)?.data_plane_health()
    }

    pub(crate) fn upsert_source_pack(
        &self,
        config_home: impl AsRef<Path>,
        source_pack: MatrixSourcePack,
    ) -> Result<MatrixSourcePack, GatewayMatrixRepositoryError> {
        self.store(config_home)?.upsert_source_pack(source_pack)
    }

    pub(crate) fn upsert_source_pack_checked(
        &self,
        config_home: impl AsRef<Path>,
        source_pack: MatrixSourcePack,
        expected_revision: Option<u64>,
    ) -> Result<matrix_repository::MatrixRevisioned<MatrixSourcePack>, GatewayMatrixRepositoryError>
    {
        self.store(config_home)?
            .upsert_source_pack_checked(source_pack, expected_revision)
    }

    pub(crate) fn list_source_packs(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_source_packs(limit)
    }

    pub(crate) fn get_source_pack(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
    ) -> Result<Option<MatrixSourcePack>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_source_pack(source_pack_id)
    }

    pub(crate) fn validate_source_pack(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
    ) -> Result<MatrixSourcePackValidation, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .validate_source_pack(source_pack_id)
    }

    pub(crate) fn source_pack_delta_plan(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
    ) -> Result<MatrixSourceDeltaPlan, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .source_pack_delta_plan(source_pack_id)
    }

    pub(crate) fn plan_connector_run(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
        input: MatrixConnectorRunInput,
    ) -> Result<MatrixConnectorRun, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .plan_connector_run(source_pack_id, input)
    }

    pub(crate) fn get_connector_run(
        &self,
        config_home: impl AsRef<Path>,
        run_id: &str,
    ) -> Result<Option<MatrixConnectorRun>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_connector_run(run_id)
    }

    pub(crate) fn plan_source_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
        resource_ref: Option<String>,
        estimated_rows: Option<u64>,
    ) -> Result<MatrixSourceSnapshotPlan, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .plan_source_snapshot(source_pack_id, resource_ref, estimated_rows)
    }

    pub(crate) fn create_source_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        input: MatrixSourceSnapshotInput,
    ) -> Result<MatrixSourceSnapshot, GatewayMatrixRepositoryError> {
        self.store(config_home)?.create_source_snapshot(input)
    }

    pub(crate) fn get_source_snapshot(
        &self,
        config_home: impl AsRef<Path>,
        snapshot_id: &str,
    ) -> Result<Option<MatrixSourceSnapshot>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_source_snapshot(snapshot_id)
    }

    pub(crate) fn list_source_snapshots(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MatrixSourceSnapshot>, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .list_source_snapshots(source_pack_id, limit)
    }

    pub(crate) fn apply_source_snapshot_rows(
        &self,
        config_home: impl AsRef<Path>,
        source_pack_id: &str,
        snapshot: MatrixSourceSnapshot,
        rows: &[Value],
    ) -> Result<MatrixSourceSnapshotApplyReport, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .apply_source_snapshot_rows(source_pack_id, snapshot, rows)
    }

    pub(crate) fn ingest_source_record_batch(
        &self,
        config_home: impl AsRef<Path>,
        workspace_root: impl AsRef<Path>,
        batch: &SourceRecordBatch,
        watermark_before: Option<SourceWatermark>,
        watermark_after: Option<SourceWatermark>,
    ) -> Result<SourceIngestionReceipt, GatewayMatrixRepositoryError> {
        self.ingest_source_record_chunk(
            config_home,
            workspace_root,
            batch,
            watermark_before,
            watermark_after,
            0,
            true,
        )
    }

    pub(crate) fn ingest_source_record_chunk(
        &self,
        config_home: impl AsRef<Path>,
        workspace_root: impl AsRef<Path>,
        batch: &SourceRecordBatch,
        watermark_before: Option<SourceWatermark>,
        watermark_after: Option<SourceWatermark>,
        chunk_ordinal: usize,
        final_chunk: bool,
    ) -> Result<SourceIngestionReceipt, GatewayMatrixRepositoryError> {
        if batch.rows.len() > 1_000 {
            return Err(matrix_repository::MatrixStoreError::Backend(format!(
                "source chunk exceeds the 1000 row limit: {}",
                batch.rows.len()
            )));
        }
        let config_home = config_home.as_ref();
        let repository = self.store(config_home)?;
        let source_pack_id = source_pack_id_for_batch(batch);
        let table = batch
            .table
            .clone()
            .unwrap_or_else(|| batch.schema.table_name.clone());
        let now = Utc::now();
        let schema_identity = serde_json::to_string(&batch.schema).map_err(|error| {
            matrix_repository::MatrixStoreError::Backend(format!(
                "serialize source schema identity: {error}"
            ))
        })?;
        let mapping_signature = stable_receipt_id(&[
            batch.adapter_id.as_str(),
            batch.resource_ref.as_str(),
            table.as_str(),
            schema_identity.as_str(),
        ]);
        let mut source_pack = MatrixSourcePack {
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
                delta_signature: format!("schema-{mapping_signature}"),
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
        match repository.get_source_pack(&source_pack_id)? {
            Some(existing) if source_pack_contract_matches(&existing, &source_pack) => {}
            Some(existing) => {
                source_pack.created_at = existing.created_at;
                repository.upsert_source_pack(source_pack)?;
            }
            None => {
                repository.upsert_source_pack(source_pack)?;
            }
        }

        let chunk_identity = stable_source_chunk_identity(
            workspace_root.as_ref(),
            batch,
            watermark_before.as_ref(),
            chunk_ordinal,
        );
        let proposed_snapshot = MatrixSourceSnapshot::from_input(MatrixSourceSnapshotInput {
            snapshot_id: Some(format!("source-snapshot-{chunk_identity}")),
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
                "chunk_ordinal": chunk_ordinal,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
            }),
        });
        let (snapshot, apply_report) = repository
            .get_source_snapshot(&proposed_snapshot.snapshot_id)?
            .and_then(|snapshot| {
                source_snapshot_apply_report(&snapshot).map(|report| (snapshot, report))
            })
            .map_or_else(
                || {
                    repository
                        .apply_source_snapshot_rows(
                            &source_pack_id,
                            proposed_snapshot.clone(),
                            &batch.rows,
                        )
                        .map(|report| (proposed_snapshot, report))
                },
                Ok,
            )?;
        let mut matrix_refs = vec![
            format!("matrix:source_pack:{source_pack_id}"),
            format!("matrix:source_snapshot:{}", snapshot.snapshot_id),
            format!("matrix:apply_report:{}", apply_report.fact_count),
            format!("matrix:source_chunk_receipt:{}", snapshot.snapshot_id),
        ];
        let committed_source_watermark = if final_chunk {
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
                raw_checksum: watermark_after
                    .as_ref()
                    .and_then(|watermark| watermark.checksum.clone())
                    .or_else(|| Some(batch.checksum.clone())),
                expected_revision: watermark_before
                    .as_ref()
                    .map(|watermark| watermark.revision)
                    .filter(|revision| *revision > 0),
                adapter_id: Some(batch.adapter_id.clone()),
                strategy: watermark_after
                    .as_ref()
                    .map(|watermark| watermark.strategy.clone()),
                table: batch.table.clone(),
                cursor: watermark_after.as_ref().and_then(|watermark| {
                    watermark
                        .cursor
                        .clone()
                        .or_else(|| watermark.high_watermark.clone())
                }),
                offset: watermark_after
                    .as_ref()
                    .and_then(|watermark| watermark.offset)
                    .map(|offset| offset as u64),
                metric_ids: Vec::new(),
            })?;
            // 只有最后一块完成记录事务后，才允许推进唯一耐久水位。
            let committed_matrix_watermark = repository.commit_data_plane_ingest(&plan)?;
            matrix_refs.push(format!("matrix:data_plane_batch:{}", plan.batch_id));
            watermark_after.map(|mut watermark| {
                watermark.revision = committed_matrix_watermark.revision;
                watermark
            })
        } else {
            None
        };
        Ok(SourceIngestionReceipt {
            receipt_id: format!("source-receipt-{chunk_identity}"),
            adapter_id: batch.adapter_id.clone(),
            resource_ref: batch.resource_ref.clone(),
            row_count: batch.rows.len(),
            checksum: batch.checksum.clone(),
            watermark_before,
            watermark_after: committed_source_watermark,
            matrix_refs,
            created_at_ms: snapshot.captured_at.timestamp_millis(),
        })
    }

    pub(crate) fn plan_data_plane_ingest(
        &self,
        config_home: impl AsRef<Path>,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> Result<MatrixDataPlaneIngestPlan, GatewayMatrixRepositoryError> {
        self.store(config_home)?.plan_data_plane_ingest(input)
    }

    pub(crate) fn list_facts(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixFact>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_facts(limit)
    }

    pub(crate) fn ingest_fact(
        &self,
        config_home: impl AsRef<Path>,
        fact: &MatrixFact,
    ) -> Result<MatrixAttentionItem, GatewayMatrixRepositoryError> {
        self.store(config_home)?.ingest_fact(fact)
    }

    pub(crate) fn list_entities(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixEntity>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_entities(limit)
    }

    pub(crate) fn upsert_entity(
        &self,
        config_home: impl AsRef<Path>,
        entity: &MatrixEntity,
    ) -> Result<MatrixEntity, GatewayMatrixRepositoryError> {
        self.store(config_home)?.upsert_entity(entity)
    }

    pub(crate) fn upsert_entity_checked(
        &self,
        config_home: impl AsRef<Path>,
        entity: &MatrixEntity,
        expected_revision: Option<u64>,
    ) -> Result<matrix_repository::MatrixRevisioned<MatrixEntity>, GatewayMatrixRepositoryError>
    {
        self.store(config_home)?
            .upsert_entity_checked(entity, expected_revision)
    }

    pub(crate) fn get_entity(
        &self,
        config_home: impl AsRef<Path>,
        entity_id: &str,
    ) -> Result<Option<MatrixEntity>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_entity(entity_id)
    }

    pub(crate) fn resolve_entity_by_source_key(
        &self,
        config_home: impl AsRef<Path>,
        source_system: &str,
        source_key: &str,
    ) -> Result<Option<MatrixEntity>, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .resolve_entity_by_source_key(source_system, source_key)
    }

    pub(crate) fn propose_entity_match(
        &self,
        config_home: impl AsRef<Path>,
        left_entity_id: &str,
        right_entity_id: &str,
    ) -> Result<MatrixEntityMatchCandidate, GatewayMatrixRepositoryError> {
        self.store(config_home)?
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
        self.store(config_home)?.decide_entity_conflict(
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
        self.store(config_home)?.upsert_relation(relation)
    }

    pub(crate) fn upsert_relation_checked(
        &self,
        config_home: impl AsRef<Path>,
        relation: &MatrixRelation,
        expected_revision: Option<u64>,
    ) -> Result<matrix_repository::MatrixRevisioned<MatrixRelation>, GatewayMatrixRepositoryError>
    {
        self.store(config_home)?
            .upsert_relation_checked(relation, expected_revision)
    }

    pub(crate) fn list_entity_relations(
        &self,
        config_home: impl AsRef<Path>,
        entity_id: &str,
        limit: usize,
    ) -> Result<Vec<MatrixRelation>, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .list_entity_relations(entity_id, limit)
    }

    pub(crate) fn impact_trace(
        &self,
        config_home: impl AsRef<Path>,
        entity_id: &str,
        max_depth: usize,
    ) -> Result<MatrixImpactTrace, GatewayMatrixRepositoryError> {
        self.store(config_home)?.impact_trace(entity_id, max_depth)
    }

    pub(crate) fn list_evidence_packets(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_evidence_packets(limit)
    }

    pub(crate) fn list_metric_definitions(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<Vec<MatrixMetricDefinition>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_metric_definitions()
    }

    pub(crate) fn metric_states(
        &self,
        config_home: impl AsRef<Path>,
        metric_id: &str,
    ) -> Result<Vec<MatrixMetricState>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.metric_states(metric_id)
    }

    pub(crate) fn metric_lineage(
        &self,
        config_home: impl AsRef<Path>,
        metric_id: &str,
        max_depth: usize,
    ) -> Result<MatrixMetricLineage, GatewayMatrixRepositoryError> {
        self.store(config_home)?
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
        self.store(config_home)?.plan_metric_attention(
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
        self.store(config_home)?
            .materialize_metric_snapshot(metric_ids, scope_ref)
    }

    pub(crate) fn upsert_metric_dependency(
        &self,
        config_home: impl AsRef<Path>,
        dependency: &MatrixMetricDependency,
    ) -> Result<MatrixMetricDependency, GatewayMatrixRepositoryError> {
        self.store(config_home)?
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
        self.store(config_home)?
            .upsert_metric_dependency_checked(dependency, expected_revision)
    }

    pub(crate) fn metrics_affected_by_fact_type(
        &self,
        config_home: impl AsRef<Path>,
        fact_type: &str,
    ) -> Result<Vec<String>, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .metrics_affected_by_fact_type(fact_type)
    }

    pub(crate) fn plan_compute_job_for_fact_type(
        &self,
        config_home: impl AsRef<Path>,
        input: MatrixComputeJobInput,
    ) -> Result<MatrixComputePlan, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .plan_compute_job_for_fact_type(input)
    }

    pub(crate) fn get_compute_job(
        &self,
        config_home: impl AsRef<Path>,
        job_id: &str,
    ) -> Result<Option<MatrixComputeJob>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_compute_job(job_id)
    }

    pub(crate) fn run_compute_job(
        &self,
        config_home: impl AsRef<Path>,
        job_id: &str,
    ) -> Result<MatrixComputeJob, GatewayMatrixRepositoryError> {
        self.store(config_home)?.run_compute_job(job_id)
    }

    pub(crate) fn recompute_metrics(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<matrix_repository::MatrixMetricRecomputeResult, GatewayMatrixRepositoryError> {
        self.store(config_home)?.recompute_metrics()
    }

    pub(crate) fn list_changes(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixChangeEvent>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_changes(limit)
    }

    pub(crate) fn list_attention(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_attention(limit)
    }

    pub(crate) fn build_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: Option<&str>,
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> Result<MatrixEvidencePacket, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .build_evidence_packet(packet_id, attention_id, problem_statement)
    }

    pub(crate) fn insert_ai_harness_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet: &MatrixEvidencePacket,
    ) -> Result<MatrixEvidencePacket, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .insert_ai_harness_evidence_packet(packet)
    }

    pub(crate) fn get_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_evidence_packet(packet_id)
    }

    pub(crate) fn evaluate_evidence_quality(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .evaluate_evidence_quality(packet_id)
    }

    pub(crate) fn evaluate_evidence_quality_with_gate_id(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
        gate_id: &str,
    ) -> Result<MatrixQualityGateDecision, GatewayMatrixRepositoryError> {
        self.store(config_home)?
            .evaluate_evidence_quality_with_gate_id(packet_id, gate_id)
    }

    pub(crate) fn get_quality_gate(
        &self,
        config_home: impl AsRef<Path>,
        gate_id: &str,
    ) -> Result<Option<MatrixQualityGateDecision>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.get_quality_gate(gate_id)
    }

    pub(crate) fn connector_source_watermark(
        &self,
        config_home: impl AsRef<Path>,
        adapter_id: &str,
        resource_ref: &str,
        table: Option<&str>,
    ) -> Result<Option<SourceWatermark>, GatewayMatrixRepositoryError> {
        let fact_type = format!("source.{adapter_id}.row");
        let partition_ref = table.unwrap_or("default-partition");
        Ok(self
            .store(config_home)?
            .get_data_plane_watermark(resource_ref, &fact_type, partition_ref)?
            .map(|watermark| SourceWatermark {
                adapter_id: watermark
                    .adapter_id
                    .unwrap_or_else(|| adapter_id.to_string()),
                resource_ref: watermark.source_ref,
                table: watermark.table.or_else(|| table.map(str::to_string)),
                strategy: watermark.strategy.unwrap_or_else(|| {
                    if watermark.offset.is_some() {
                        "offset".to_string()
                    } else {
                        "cursor".to_string()
                    }
                }),
                cursor: watermark.cursor,
                offset: watermark.offset.map(|offset| offset as usize),
                high_watermark: Some(watermark.high_watermark),
                checksum: watermark.checksum,
                revision: watermark.revision,
                updated_at_ms: watermark.updated_at.timestamp_millis(),
            }))
    }

    pub(crate) fn list_data_plane_watermarks(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixDataPlaneWatermark>, GatewayMatrixRepositoryError> {
        self.store(config_home)?.list_data_plane_watermarks(limit)
    }

    pub(crate) fn structured_runtime_ready(&self, config_home: impl AsRef<Path>) -> (bool, bool) {
        let Ok(store) = self.store(config_home) else {
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

fn source_pack_contract_matches(existing: &MatrixSourcePack, proposed: &MatrixSourcePack) -> bool {
    let mut proposed = proposed.clone();
    proposed.created_at = existing.created_at;
    proposed.updated_at = existing.updated_at;
    existing == &proposed
}

fn stable_source_chunk_identity(
    workspace_root: &Path,
    batch: &SourceRecordBatch,
    watermark_before: Option<&SourceWatermark>,
    chunk_ordinal: usize,
) -> String {
    let workspace = workspace_root.to_string_lossy();
    let table = batch
        .table
        .as_deref()
        .unwrap_or(batch.schema.table_name.as_str());
    let source_cursor = watermark_before
        .and_then(|watermark| {
            watermark
                .cursor
                .clone()
                .or_else(|| watermark.high_watermark.clone())
                .or_else(|| watermark.offset.map(|offset| offset.to_string()))
        })
        .unwrap_or_else(|| "origin".to_string());
    let source_revision = watermark_before
        .map(|watermark| watermark.revision.to_string())
        .unwrap_or_else(|| "0".to_string());
    let batch_cursor = format!(
        "{}:{}:{}",
        batch.cursor.offset,
        batch.cursor.limit,
        batch
            .cursor
            .next_offset
            .map(|offset| offset.to_string())
            .unwrap_or_else(|| "end".to_string())
    );
    let chunk_ordinal = chunk_ordinal.to_string();
    stable_receipt_id(&[
        workspace.as_ref(),
        batch.adapter_id.as_str(),
        batch.resource_ref.as_str(),
        table,
        source_cursor.as_str(),
        source_revision.as_str(),
        batch_cursor.as_str(),
        chunk_ordinal.as_str(),
        batch.checksum.as_str(),
    ])
}

fn source_snapshot_apply_report(
    snapshot: &MatrixSourceSnapshot,
) -> Option<MatrixSourceSnapshotApplyReport> {
    snapshot
        .metadata
        .get("chunk_receipt")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
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

    #[test]
    fn source_record_batch_ingests_to_source_pack_and_watermark() {
        let config_home =
            std::env::temp_dir().join(format!("cowd-source-record-batch-{}", uuid::Uuid::new_v4()));
        let workspace_root = config_home.join("workspace");
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
        assert_eq!(
            stable_source_chunk_identity(&workspace_root, &batch, None, 0),
            stable_source_chunk_identity(&workspace_root, &batch, None, 0)
        );
        assert_ne!(
            stable_source_chunk_identity(&workspace_root, &batch, None, 0),
            stable_source_chunk_identity(&config_home.join("other-workspace"), &batch, None, 0)
        );
        let watermark_after = SourceWatermark {
            adapter_id: "csv".to_string(),
            resource_ref: batch.resource_ref.clone(),
            table: batch.table.clone(),
            strategy: "offset".to_string(),
            cursor: Some("1".to_string()),
            offset: Some(1),
            high_watermark: None,
            checksum: Some(batch.checksum.clone()),
            revision: 0,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        let staged = service
            .ingest_source_record_chunk(&config_home, &workspace_root, &batch, None, None, 0, false)
            .expect("source chunk stage");
        assert!(staged.watermark_after.is_none());
        assert!(service
            .list_data_plane_watermarks(&config_home, 10)
            .expect("staged watermarks")
            .is_empty());
        // 模拟最后一块数据及 receipt 已提交、进程却在 watermark 提交前退出。
        let durable_final_chunk = service
            .ingest_source_record_chunk(&config_home, &workspace_root, &batch, None, None, 1, false)
            .expect("durable final chunk before watermark");
        assert!(service
            .list_data_plane_watermarks(&config_home, 10)
            .expect("watermark before recovery")
            .is_empty());
        let receipt = service
            .ingest_source_record_chunk(
                &config_home,
                &workspace_root,
                &batch,
                None,
                Some(watermark_after.clone()),
                1,
                true,
            )
            .expect("source final chunk receipt");
        assert_eq!(receipt.receipt_id, durable_final_chunk.receipt_id);
        assert_eq!(receipt.created_at_ms, durable_final_chunk.created_at_ms);

        assert_eq!(receipt.row_count, 1);
        assert_eq!(
            receipt
                .watermark_after
                .as_ref()
                .expect("committed source watermark")
                .revision,
            1
        );
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
        let restored = service
            .connector_source_watermark(
                &config_home,
                &batch.adapter_id,
                &batch.resource_ref,
                batch.table.as_deref(),
            )
            .expect("restore connector watermark")
            .expect("committed connector watermark");
        assert_eq!(restored.revision, 1);
        assert_eq!(restored.resource_ref, batch.resource_ref);

        let health_before_replay = service
            .repository_health(&config_home)
            .expect("health before exact replay");
        let replay = service
            .ingest_source_record_chunk(
                &config_home,
                &workspace_root,
                &batch,
                None,
                Some(watermark_after),
                1,
                true,
            )
            .expect("exact source chunk replay");
        let health_after_replay = service
            .repository_health(&config_home)
            .expect("health after exact replay");
        assert_eq!(replay.receipt_id, receipt.receipt_id);
        assert_eq!(replay.created_at_ms, receipt.created_at_ms);
        assert_eq!(health_after_replay, health_before_replay);
        assert_eq!(
            replay
                .watermark_after
                .as_ref()
                .expect("replayed watermark")
                .revision,
            1
        );

        let mut oversized = batch.clone();
        oversized.rows = (0..1_001)
            .map(|index| serde_json::json!({"order_id": format!("O-{index}")}))
            .collect();
        oversized.row_count = oversized.rows.len();
        assert!(service
            .ingest_source_record_chunk(
                &config_home,
                &workspace_root,
                &oversized,
                None,
                None,
                2,
                false,
            )
            .is_err());
        let _ = std::fs::remove_dir_all(config_home);
    }
}
