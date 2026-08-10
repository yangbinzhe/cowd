//! Matrix structured fact engine contracts.

// Test assertions intentionally use unwrap/expect; normal library builds remain strict.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

#[path = "metric/attention.rs"]
mod attention;
#[path = "source/change.rs"]
mod change;
#[path = "metric/compute.rs"]
mod compute;
#[path = "source/connector_runtime.rs"]
mod connector_runtime;
#[path = "source/data_plane.rs"]
mod data_plane;
#[path = "entity/entity.rs"]
mod entity;
#[path = "fact/evidence.rs"]
mod evidence;
#[path = "fact/fact.rs"]
mod fact;
#[path = "metric/metric.rs"]
mod metric;
#[path = "metric/metric_attention.rs"]
mod metric_attention;
#[path = "metric/metric_graph.rs"]
mod metric_graph;
#[path = "entity/ontology.rs"]
mod ontology;
#[path = "metric/quality.rs"]
mod quality;
#[path = "metric/query.rs"]
mod query;
#[path = "entity/relation.rs"]
mod relation;
#[path = "source/scenario.rs"]
mod scenario;
#[path = "source/source.rs"]
mod source;
#[path = "source/source_pack.rs"]
mod source_pack;
#[path = "contract/structured.rs"]
pub mod structured;

pub use attention::{MatrixAttentionItem, MatrixSeverity};
pub use change::MatrixChangeEvent;
pub use compute::{MatrixComputeJob, MatrixComputeJobInput, MatrixComputePlan};
pub use connector_runtime::{
    MatrixConnectorQualityReport, MatrixConnectorReceipt, MatrixConnectorRun,
    MatrixConnectorRunInput,
};
pub use data_plane::{
    MatrixDataPlane, MatrixDataPlaneCapability, MatrixDataPlaneHealth, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
};
pub use entity::{normalize_key, MatrixEntity, MatrixEntityInput, MatrixSourceKey};
pub use evidence::{MatrixEvidencePacket, MatrixEvidenceSourceRef};
pub use fact::{
    MatrixFact, MatrixFactInput, AI_EVAL_RESULT_FACT, AI_EXECUTION_GRAPH_QUALITY_FACT,
    AI_GROWTH_SIGNAL_FACT, AI_STRATEGY_DECISION_FACT, AI_TOOL_TRANSACTION_RESULT_FACT,
    AI_VERIFICATION_RESULT_FACT,
};
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
pub use query::{
    evaluate_matrix_formula, execute_matrix_query_plan, matrix_formula_registry, MatrixAggregation,
    MatrixFormulaDefinition, MatrixFormulaKind, MatrixQueryInput, MatrixQueryPlan,
    MatrixQueryPlanError, MatrixQueryResult, MATRIX_FORMULA_RATIO_PERCENT_V1,
    MATRIX_FORMULA_SUM_V1, MATRIX_QUERY_PLAN_SCHEMA_V1,
};
pub use relation::{MatrixImpactHop, MatrixImpactTrace, MatrixRelation, MatrixRelationInput};
pub use scenario::{
    MatrixScenarioOutputContract, MatrixScenarioResult, MatrixScenarioRun, MatrixScenarioRunStatus,
    MatrixScenarioSpec, MatrixSnapshotRef,
};
pub use source::{
    MatrixSourceKind, MatrixSourceSnapshot, MatrixSourceSnapshotApplyReport,
    MatrixSourceSnapshotInput, MatrixSourceSnapshotPlan,
};
pub use source_pack::{
    MatrixSourceDeltaPlan, MatrixSourceEntityMapping, MatrixSourceFactMapping, MatrixSourcePack,
    MatrixSourcePackValidation, MatrixSourceRelationMapping,
};

pub const MATRIX_SCHEMA_VERSION: i64 = 19;

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
}
