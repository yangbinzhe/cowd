//! Matrix structured fact engine contracts.
//!
//! Matrix is the kernel-facing name for Cowd's structured fact engine. The
//! existing `iacc` module remains as a compatibility facade while callers move
//! from application-shaped naming to fact-engine naming.

pub use crate::iacc::{
    build_metric_compute_jobs, match_candidate, IaccActionExecution as MatrixActionExecution,
    IaccActionExecutionRequest as MatrixActionExecutionRequest,
    IaccActionFeedback as MatrixActionFeedback, IaccAttentionItem as MatrixAttentionItem,
    IaccAttributionCandidate as MatrixAttributionCandidate, IaccChangeEvent as MatrixChangeEvent,
    IaccCockpitProfile as MatrixCockpitProfile,
    IaccCockpitProfileInput as MatrixCockpitProfileInput,
    IaccCockpitProjection as MatrixCockpitProjection,
    IaccCockpitReportDeliveryPayload as MatrixCockpitReportDeliveryPayload,
    IaccCockpitReportDeliveryPayloadRequest as MatrixCockpitReportDeliveryPayloadRequest,
    IaccCockpitReportDeliveryReceipt as MatrixCockpitReportDeliveryReceipt,
    IaccCockpitReportDeliveryState as MatrixCockpitReportDeliveryState,
    IaccCockpitReportRequest as MatrixCockpitReportRequest,
    IaccCockpitReportSnapshot as MatrixCockpitReportSnapshot,
    IaccCockpitWidget as MatrixCockpitWidget, IaccComputeJob as MatrixComputeJob,
    IaccComputeJobInput as MatrixComputeJobInput, IaccComputePlan as MatrixComputePlan,
    IaccConnectorQualityReport as MatrixConnectorQualityReport,
    IaccConnectorReceipt as MatrixConnectorReceipt, IaccConnectorRun as MatrixConnectorRun,
    IaccConnectorRunInput as MatrixConnectorRunInput,
    IaccCrossPlaneBridgeReceipt as MatrixCrossPlaneBridgeReceipt, IaccDataPlane as MatrixDataPlane,
    IaccDataPlaneCapability as MatrixDataPlaneCapability,
    IaccDataPlaneHealth as MatrixDataPlaneHealth,
    IaccDataPlaneIngestPlan as MatrixDataPlaneIngestPlan,
    IaccDataPlaneIngestPlanInput as MatrixDataPlaneIngestPlanInput,
    IaccDataPlaneWatermark as MatrixDataPlaneWatermark, IaccDomainPack as MatrixDomainPack,
    IaccDomainScenario as MatrixDomainScenario, IaccDomainSeedPlan as MatrixDomainSeedPlan,
    IaccDomainSeedResult as MatrixDomainSeedResult, IaccEntity as MatrixEntity,
    IaccEntityConflictDecision as MatrixEntityConflictDecision,
    IaccEntityInput as MatrixEntityInput, IaccEntityMatchCandidate as MatrixEntityMatchCandidate,
    IaccEvidencePacket as MatrixEvidencePacket, IaccEvidenceSourceRef as MatrixEvidenceSourceRef,
    IaccFact as MatrixFact, IaccFactInput as MatrixFactInput, IaccHealth as MatrixHealth,
    IaccImpactHop as MatrixImpactHop, IaccImpactPath as MatrixImpactPath,
    IaccImpactTrace as MatrixImpactTrace, IaccIncident as MatrixIncident,
    IaccMemoryCase as MatrixMemoryCase, IaccMetricAttentionPlan as MatrixMetricAttentionPlan,
    IaccMetricAttentionScore as MatrixMetricAttentionScore,
    IaccMetricDefinition as MatrixMetricDefinition, IaccMetricDependency as MatrixMetricDependency,
    IaccMetricDependencyInput as MatrixMetricDependencyInput,
    IaccMetricLineage as MatrixMetricLineage,
    IaccMetricRecomputeResult as MatrixMetricRecomputeResult,
    IaccMetricSnapshot as MatrixMetricSnapshot, IaccMetricSnapshotItem as MatrixMetricSnapshotItem,
    IaccMetricState as MatrixMetricState, IaccMetricStatus as MatrixMetricStatus,
    IaccOntologyConcept as MatrixOntologyConcept,
    IaccOntologyMetricBinding as MatrixOntologyMetricBinding,
    IaccOntologyPack as MatrixOntologyPack, IaccOntologyRelation as MatrixOntologyRelation,
    IaccOperationalAnalysis as MatrixOperationalAnalysis, IaccPlaybook as MatrixPlaybook,
    IaccPlaybookStep as MatrixPlaybookStep, IaccQualityGateDecision as MatrixQualityGateDecision,
    IaccRecommendedAction as MatrixRecommendedAction, IaccRelation as MatrixRelation,
    IaccRelationInput as MatrixRelationInput, IaccSeverity as MatrixSeverity,
    IaccSkillManifest as MatrixSkillManifest, IaccSkillPlan as MatrixSkillPlan,
    IaccSkillRun as MatrixSkillRun, IaccSourceDeltaPlan as MatrixSourceDeltaPlan,
    IaccSourceEntityMapping as MatrixSourceEntityMapping,
    IaccSourceFactMapping as MatrixSourceFactMapping, IaccSourceKey as MatrixSourceKey,
    IaccSourceKind as MatrixSourceKind, IaccSourcePack as MatrixSourcePack,
    IaccSourcePackValidation as MatrixSourcePackValidation,
    IaccSourceSnapshot as MatrixSourceSnapshot, IaccSqliteDataPlane as MatrixSqliteDataPlane,
    IaccStore as MatrixStore, IaccStoreError as MatrixStoreError, IACC_SCHEMA_VERSION,
};

#[must_use]
pub fn matrix_reference(kind: &str, id: &str) -> String {
    format!("matrix:{kind}:{id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iacc::iacc_reference;

    #[test]
    fn matrix_reference_uses_matrix_namespace() {
        assert_eq!(matrix_reference("fact", "f1"), "matrix:fact:f1");
    }

    #[test]
    fn iacc_reference_remains_available_for_compatibility() {
        assert_eq!(iacc_reference("fact", "f1"), "iacc:fact:f1");
    }

    #[test]
    fn matrix_store_alias_opens_and_uses_iacc_store_compatibly() {
        let store = MatrixStore::in_memory().expect("matrix store opens");
        let health = store.health().expect("matrix health loads");

        assert_eq!(health.schema_version, IACC_SCHEMA_VERSION);
    }
}
