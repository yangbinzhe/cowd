//! Backend-neutral Matrix persistence contract.
//!
//! Matrix has a broad, typed persistence surface.  Keeping that surface in a
//! single port prevents Gateway callers from silently choosing SQLite while a
//! different backend is selected.  Concrete adapters own SQL; this module
//! owns only domain operations and error semantics.

use std::sync::Arc;

use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixComputeJobInput,
    MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput, MatrixDataPlaneHealth,
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
    MatrixEntity, MatrixEntityConflictDecision, MatrixEntityMatchCandidate, MatrixEvidencePacket,
    MatrixFact, MatrixImpactTrace, MatrixMetricAttentionPlan, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricState,
    MatrixOntologyPack, MatrixQualityGateDecision, MatrixRelation, MatrixScenarioResult,
    MatrixScenarioRun, MatrixScenarioSpec, MatrixSourceDeltaPlan, MatrixSourcePack,
    MatrixSourcePackValidation, MatrixSourceSnapshot, MatrixSourceSnapshotApplyReport,
    MatrixSourceSnapshotInput, MatrixSourceSnapshotPlan,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::{PostgresExecutor, StorageBackendKind, StorageEndpoint};
use thiserror::Error;

use crate::{MatrixSqliteRepository, MatrixSqliteRepositoryError, PostgresMatrixRepository};

pub type MatrixStoreResult<T> = Result<T, MatrixStoreError>;

/// Storage-independent failure taxonomy used by Gateway and future adapters.
#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatrixStoreError {
    #[error("matrix record not found: {0}")]
    NotFound(String),
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
    #[error("matrix backend failure: {0}")]
    Backend(String),
}

impl From<MatrixSqliteRepositoryError> for MatrixStoreError {
    fn from(error: MatrixSqliteRepositoryError) -> Self {
        match error {
            MatrixSqliteRepositoryError::NotFound(message) => Self::NotFound(message),
            MatrixSqliteRepositoryError::Migration(message) => Self::Backend(message),
            MatrixSqliteRepositoryError::InvalidScenario(message) => Self::InvalidScenario(message),
            MatrixSqliteRepositoryError::ScenarioState(message) => Self::ScenarioState(message),
            MatrixSqliteRepositoryError::RevisionConflict {
                resource_ref,
                expected,
                actual,
            } => Self::RevisionConflict {
                resource_ref,
                expected,
                actual,
            },
            other => Self::Backend(other.to_string()),
        }
    }
}

/// Versioned optimistic-concurrency result shared by all Matrix adapters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixRevisioned<T> {
    pub resource: T,
    pub previous_revision: Option<u64>,
    pub revision: u64,
    pub created: bool,
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
    pub source_snapshot_count: u64,
    pub ontology_pack_count: u64,
    pub entity_match_candidate_count: u64,
    pub entity_conflict_decision_count: u64,
    pub metric_snapshot_count: u64,
    pub scenario_spec_count: u64,
    pub scenario_run_count: u64,
    pub scenario_result_count: u64,
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

macro_rules! matrix_store_operations {
    ($callback:ident) => {
        $callback! {
            health() -> MatrixHealth;
            data_plane_health() -> MatrixDataPlaneHealth;
            plan_data_plane_ingest(input: MatrixDataPlaneIngestPlanInput) -> MatrixDataPlaneIngestPlan;
            commit_data_plane_ingest(plan: &MatrixDataPlaneIngestPlan) -> MatrixDataPlaneWatermark;
            upsert_entity(entity: &MatrixEntity) -> MatrixEntity;
            resource_revision_for_existing(resource_kind: &str, resource_id: &str) -> u64;
            upsert_entity_checked(entity: &MatrixEntity, expected_revision: Option<u64>) -> MatrixRevisioned<MatrixEntity>;
            get_entity(entity_id: &str) -> Option<MatrixEntity>;
            resolve_entity_by_source_key(source_system: &str, source_key: &str) -> Option<MatrixEntity>;
            list_entities(limit: usize) -> Vec<MatrixEntity>;
            get_ontology_pack(ontology_id: &str) -> Option<MatrixOntologyPack>;
            propose_entity_match(left_entity_id: &str, right_entity_id: &str) -> MatrixEntityMatchCandidate;
            decide_entity_conflict(candidate_id: &str, survivor_entity_id: &str, retired_entity_id: &str, survivorship_rule: &str, notes: Option<String>) -> MatrixEntityConflictDecision;
            plan_metric_attention(trigger_fact_type: &str, entity_scope: Option<String>, period: Option<String>, limit: usize) -> MatrixMetricAttentionPlan;
            materialize_metric_snapshot(metric_ids: Vec<String>, scope_ref: Option<String>) -> MatrixMetricSnapshot;
            upsert_relation(relation: &MatrixRelation) -> MatrixRelation;
            upsert_relation_checked(relation: &MatrixRelation, expected_revision: Option<u64>) -> MatrixRevisioned<MatrixRelation>;
            list_entity_relations(entity_id: &str, limit: usize) -> Vec<MatrixRelation>;
            impact_trace(entity_id: &str, max_depth: usize) -> MatrixImpactTrace;
            register_metric_definition(definition: &MatrixMetricDefinition) -> ();
            upsert_metric_dependency(dependency: &MatrixMetricDependency) -> MatrixMetricDependency;
            upsert_metric_dependency_checked(dependency: &MatrixMetricDependency, expected_revision: Option<u64>) -> MatrixRevisioned<MatrixMetricDependency>;
            metric_lineage(metric_id: &str, max_depth: usize) -> MatrixMetricLineage;
            metrics_affected_by_fact_type(fact_type: &str) -> Vec<String>;
            plan_compute_job_for_fact_type(input: MatrixComputeJobInput) -> MatrixComputePlan;
            get_compute_job(job_id: &str) -> Option<MatrixComputeJob>;
            run_compute_job(job_id: &str) -> MatrixComputeJob;
            ingest_fact(fact: &MatrixFact) -> MatrixAttentionItem;
            upsert_source_pack(source_pack: MatrixSourcePack) -> MatrixSourcePack;
            upsert_source_pack_checked(source_pack: MatrixSourcePack, expected_revision: Option<u64>) -> MatrixRevisioned<MatrixSourcePack>;
            get_source_pack(source_pack_id: &str) -> Option<MatrixSourcePack>;
            list_source_packs(limit: usize) -> Vec<MatrixSourcePack>;
            validate_source_pack(source_pack_id: &str) -> MatrixSourcePackValidation;
            source_pack_delta_plan(source_pack_id: &str) -> MatrixSourceDeltaPlan;
            plan_connector_run(source_pack_id: &str, input: MatrixConnectorRunInput) -> MatrixConnectorRun;
            get_connector_run(run_id: &str) -> Option<MatrixConnectorRun>;
            plan_source_snapshot(source_pack_id: &str, resource_ref: Option<String>, estimated_rows: Option<u64>) -> MatrixSourceSnapshotPlan;
            upsert_source_snapshot(snapshot: MatrixSourceSnapshot) -> MatrixSourceSnapshot;
            create_source_snapshot(input: MatrixSourceSnapshotInput) -> MatrixSourceSnapshot;
            get_source_snapshot(snapshot_id: &str) -> Option<MatrixSourceSnapshot>;
            list_source_snapshots(source_pack_id: Option<&str>, limit: usize) -> Vec<MatrixSourceSnapshot>;
            create_scenario_spec(spec: MatrixScenarioSpec) -> MatrixScenarioSpec;
            get_scenario_spec(scenario_id: &str) -> Option<MatrixScenarioSpec>;
            list_scenario_specs(limit: usize) -> Vec<MatrixScenarioSpec>;
            start_scenario_run(scenario_id: &str, parameters: Value) -> MatrixScenarioRun;
            get_scenario_run(run_id: &str) -> Option<MatrixScenarioRun>;
            list_scenario_runs(scenario_id: Option<&str>, limit: usize) -> Vec<MatrixScenarioRun>;
            complete_scenario_run(result: MatrixScenarioResult) -> MatrixScenarioResult;
            get_scenario_result(run_id: &str) -> Option<MatrixScenarioResult>;
            apply_source_snapshot_rows(source_pack_id: &str, snapshot: MatrixSourceSnapshot, rows: &[Value]) -> MatrixSourceSnapshotApplyReport;
            list_attention(limit: usize) -> Vec<MatrixAttentionItem>;
            list_facts(limit: usize) -> Vec<MatrixFact>;
            recompute_metrics() -> MatrixMetricRecomputeResult;
            recompute_metrics_for_metric_ids(metric_ids: &[String]) -> MatrixMetricRecomputeResult;
            list_metric_definitions() -> Vec<MatrixMetricDefinition>;
            metric_states(metric_id: &str) -> Vec<MatrixMetricState>;
            list_changes(limit: usize) -> Vec<MatrixChangeEvent>;
            build_evidence_packet(packet_id: Option<&str>, attention_id: Option<&str>, problem_statement: Option<&str>) -> MatrixEvidencePacket;
            insert_ai_harness_evidence_packet(packet: &MatrixEvidencePacket) -> MatrixEvidencePacket;
            get_evidence_packet(packet_id: &str) -> Option<MatrixEvidencePacket>;
            list_evidence_packets(limit: usize) -> Vec<MatrixEvidencePacket>;
            evaluate_evidence_quality(packet_id: &str) -> MatrixQualityGateDecision;
            evaluate_evidence_quality_with_gate_id(packet_id: &str, gate_id: &str) -> MatrixQualityGateDecision;
            get_quality_gate(gate_id: &str) -> Option<MatrixQualityGateDecision>;
            list_data_plane_watermarks(limit: usize) -> Vec<MatrixDataPlaneWatermark>;
            get_data_plane_watermark(source_ref: &str, fact_type: &str, partition_ref: &str) -> Option<MatrixDataPlaneWatermark>;
        }
    };
}

// Kept crate-visible so every concrete adapter expands the exact same full
// operation list.  Adding an operation to the port therefore turns a missing
// adapter implementation into a compile error rather than a latent fallback.
pub(crate) use matrix_store_operations;

macro_rules! declare_matrix_store_operations {
    ($($method:ident($($argument:ident: $argument_type:ty),*) -> $output:ty;)*) => {
        $(fn $method(&self, $($argument: $argument_type),*) -> MatrixStoreResult<$output>;)*
    };
}

/// Full typed Matrix persistence surface.  An adapter cannot claim Matrix
/// support while omitting a source, scenario, evidence or data-plane path.
pub trait MatrixStore: Send + Sync {
    matrix_store_operations!(declare_matrix_store_operations);
}

macro_rules! delegate_matrix_store_operations {
    ($($method:ident($($argument:ident: $argument_type:ty),*) -> $output:ty;)*) => {
        $(
            fn $method(&self, $($argument: $argument_type),*) -> MatrixStoreResult<$output> {
                MatrixSqliteRepository::$method(self, $($argument),*).map_err(MatrixStoreError::from)
            }
        )*
    };
}

impl MatrixStore for MatrixSqliteRepository {
    matrix_store_operations!(delegate_matrix_store_operations);
}

/// Endpoint-bound composition seam.  SQLite can be opened from its local
/// endpoint; PostgreSQL must be provided by the composition root as an
/// already-resolved bounded executor.  This prevents a domain adapter from
/// reading secrets or falling back to a local file behind the caller's back.
#[derive(Debug, Clone)]
pub struct MatrixStoreHandle {
    endpoint: StorageEndpoint,
}

impl MatrixStoreHandle {
    #[must_use]
    pub fn new(endpoint: StorageEndpoint) -> Self {
        Self { endpoint }
    }

    #[must_use]
    pub fn storage_endpoint(&self) -> &StorageEndpoint {
        &self.endpoint
    }

    pub fn open(&self) -> MatrixStoreResult<Arc<dyn MatrixStore>> {
        if self.endpoint.backend != StorageBackendKind::Sqlite {
            return Err(MatrixStoreError::Backend(format!(
                "Matrix backend `{:?}` requires an injected backend executor",
                self.endpoint.backend
            )));
        }
        let handle = self.endpoint.as_handle();
        if let Some(parent) = handle.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
        }
        Ok(Arc::new(
            MatrixSqliteRepository::open_storage_handle(&handle).map_err(MatrixStoreError::from)?,
        ))
    }

    pub fn open_with_postgres_executor(
        &self,
        executor: PostgresExecutor,
    ) -> MatrixStoreResult<Arc<dyn MatrixStore>> {
        if self.endpoint.backend != StorageBackendKind::Postgres {
            return Err(MatrixStoreError::Backend(format!(
                "Matrix endpoint is `{:?}`, not PostgreSQL",
                self.endpoint.backend
            )));
        }
        Ok(Arc::new(PostgresMatrixRepository::new(executor)?))
    }
}

#[cfg(test)]
mod tests {
    use storage::{StorageDomainId, StorageScope};

    use super::*;

    fn assert_matrix_store<T: MatrixStore>() {}

    #[test]
    fn sqlite_adapter_implements_the_complete_matrix_store_contract() {
        assert_matrix_store::<MatrixSqliteRepository>();
    }

    #[test]
    fn postgres_adapter_implements_the_complete_matrix_store_contract() {
        assert_matrix_store::<PostgresMatrixRepository>();
    }

    #[test]
    fn unavailable_selected_backend_fails_closed_without_creating_sqlite() {
        let handle = MatrixStoreHandle::new(StorageEndpoint::postgres(
            StorageDomainId::Matrix,
            StorageScope::Global,
            "matrix",
            "matrix.0001",
        ));
        assert!(matches!(handle.open(), Err(MatrixStoreError::Backend(_))));
    }
}
