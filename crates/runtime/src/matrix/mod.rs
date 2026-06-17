//! Matrix structured fact engine contracts.

mod attention;
mod change;
mod compute;
mod connector_runtime;
mod data_plane;
mod entity;
mod evidence;
mod fact;
mod metric;
mod metric_attention;
mod metric_graph;
mod ontology;
mod quality;
mod relation;
mod source;
mod source_pack;

pub use crate::matrix_store::{
    MatrixHealth, MatrixMetricRecomputeResult, MatrixStore, MatrixStoreError,
};
pub use attention::{MatrixAttentionItem, MatrixSeverity};
pub use change::MatrixChangeEvent;
pub use compute::{MatrixComputeJob, MatrixComputeJobInput, MatrixComputePlan};
pub use connector_runtime::{
    MatrixConnectorQualityReport, MatrixConnectorReceipt, MatrixConnectorRun,
    MatrixConnectorRunInput,
};
pub use data_plane::{
    MatrixDataPlane, MatrixDataPlaneCapability, MatrixDataPlaneHealth, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark, MatrixSqliteDataPlane,
};
pub use entity::{normalize_key, MatrixEntity, MatrixEntityInput, MatrixSourceKey};
pub use evidence::{MatrixEvidencePacket, MatrixEvidenceSourceRef};
pub use fact::{MatrixFact, MatrixFactInput};
pub use metric::{MatrixMetricDefinition, MatrixMetricState, MatrixMetricStatus};
pub use metric_attention::{
    build_metric_compute_jobs, MatrixMetricAttentionPlan, MatrixMetricAttentionScore,
    MatrixMetricSnapshot, MatrixMetricSnapshotItem,
};
pub use metric_graph::{MatrixMetricDependency, MatrixMetricDependencyInput, MatrixMetricLineage};
pub use ontology::{
    match_candidate, MatrixEntityConflictDecision, MatrixEntityMatchCandidate,
    MatrixOntologyConcept, MatrixOntologyMetricBinding, MatrixOntologyPack, MatrixOntologyRelation,
};
pub use quality::MatrixQualityGateDecision;
pub use relation::{MatrixImpactHop, MatrixImpactTrace, MatrixRelation, MatrixRelationInput};
pub use source::{MatrixSourceKind, MatrixSourceSnapshot};
pub use source_pack::{
    MatrixSourceDeltaPlan, MatrixSourceEntityMapping, MatrixSourceFactMapping, MatrixSourcePack,
    MatrixSourcePackValidation,
};

pub const MATRIX_SCHEMA_VERSION: i64 = crate::matrix_store::MATRIX_SCHEMA_VERSION;

#[must_use]
pub fn matrix_reference(kind: &str, id: &str) -> String {
    format!("matrix:{kind}:{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_reference_uses_matrix_namespace() {
        assert_eq!(matrix_reference("fact", "f1"), "matrix:fact:f1");
    }

    #[test]
    fn matrix_store_opens_with_matrix_schema() {
        let store = MatrixStore::in_memory().expect("matrix store opens");
        let health = store.health().expect("matrix health loads");

        assert_eq!(health.schema_version, MATRIX_SCHEMA_VERSION);
    }
}
