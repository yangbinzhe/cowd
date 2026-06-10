//! IACC structured operations cognition contracts.
//!
//! The IACC layer is an optional structured-data cognition substrate on top of
//! Cowd runtime. It stores operational facts, attention items, and bounded
//! evidence packets without replacing source systems, connector resources, or
//! memory runtime.

mod analysis;
mod attention;
mod change;
mod cockpit;
mod compute;
mod data_plane;
mod domain;
mod entity;
mod evidence;
mod execution;
mod fact;
mod incident;
mod memory_case;
mod metric;
mod metric_graph;
mod quality;
mod relation;
mod skill;
mod source;
mod source_pack;
mod store;

pub use analysis::{
    IaccAttributionCandidate, IaccImpactPath, IaccOperationalAnalysis, IaccRecommendedAction,
};
pub use attention::{IaccAttentionItem, IaccSeverity};
pub use change::IaccChangeEvent;
pub use cockpit::{
    IaccCockpitProfile, IaccCockpitProfileInput, IaccCockpitProjection,
    IaccCockpitReportDeliveryPayload, IaccCockpitReportDeliveryPayloadRequest,
    IaccCockpitReportDeliveryReceipt, IaccCockpitReportDeliveryState, IaccCockpitReportRequest,
    IaccCockpitReportSnapshot, IaccCockpitWidget,
};
pub use compute::{IaccComputeJob, IaccComputeJobInput, IaccComputePlan};
pub use data_plane::{
    IaccDataPlane, IaccDataPlaneCapability, IaccDataPlaneHealth, IaccDataPlaneIngestPlan,
    IaccDataPlaneIngestPlanInput, IaccDataPlaneWatermark, IaccSqliteDataPlane,
};
pub use domain::{
    server_manufacturing_domain_pack, server_manufacturing_seed_plan, IaccDomainPack,
    IaccDomainScenario, IaccDomainSeedPlan, IaccDomainSeedResult,
};
pub use entity::{IaccEntity, IaccEntityInput, IaccSourceKey};
pub use evidence::{IaccEvidencePacket, IaccEvidenceSourceRef};
pub use execution::{
    IaccActionExecution, IaccActionExecutionRequest, IaccActionFeedback,
    IaccCrossPlaneBridgeReceipt,
};
pub use fact::{IaccFact, IaccFactInput};
pub use incident::IaccIncident;
pub use memory_case::{IaccCasePromotion, IaccMemoryCase, IaccPlaybook, IaccPlaybookStep};
pub use metric::{IaccMetricDefinition, IaccMetricState, IaccMetricStatus};
pub use metric_graph::{IaccMetricDependency, IaccMetricDependencyInput, IaccMetricLineage};
pub use quality::IaccQualityGateDecision;
pub use relation::{IaccImpactHop, IaccImpactTrace, IaccRelation, IaccRelationInput};
pub use skill::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, skill_agent_node_id, IaccSkillManifest, IaccSkillPlan,
    IaccSkillRun,
};
pub use source::{IaccSourceKind, IaccSourceSnapshot};
pub use source_pack::{
    IaccSourceDeltaPlan, IaccSourceEntityMapping, IaccSourceFactMapping, IaccSourcePack,
    IaccSourcePackValidation,
};
pub use store::{
    IaccHealth, IaccMetricRecomputeResult, IaccStore, IaccStoreError, IACC_SCHEMA_VERSION,
};

#[must_use]
pub fn iacc_reference(kind: &str, id: &str) -> String {
    format!("iacc:{kind}:{id}")
}
