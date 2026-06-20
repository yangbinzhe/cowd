use std::path::{Path, PathBuf};

use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixComputeJobInput,
    MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput, MatrixDataPlaneHealth,
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
    MatrixEntity, MatrixEntityConflictDecision, MatrixEntityMatchCandidate, MatrixEvidencePacket,
    MatrixFact, MatrixImpactTrace, MatrixMetricAttentionPlan, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricState,
    MatrixQualityGateDecision, MatrixRelation, MatrixSourceDeltaPlan, MatrixSourcePack,
    MatrixSourcePackValidation,
};
use matrix_repository::{MatrixHealth, MatrixRepository};

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
