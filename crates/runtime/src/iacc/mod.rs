//! IACC structured operations cognition contracts.
//!
//! The IACC layer is an optional structured-data cognition substrate on top of
//! Cowd runtime. It stores operational facts, attention items, and bounded
//! evidence packets without replacing source systems, connector resources, or
//! memory runtime.

mod analysis;
mod attention;
mod change;
mod compute;
mod domain;
mod entity;
mod evidence;
mod execution;
mod fact;
mod incident;
mod metric;
mod metric_graph;
mod quality;
mod relation;
mod source;
mod store;

pub use analysis::{
    IaccAttributionCandidate, IaccImpactPath, IaccOperationalAnalysis, IaccRecommendedAction,
};
pub use attention::{IaccAttentionItem, IaccSeverity};
pub use change::IaccChangeEvent;
pub use compute::{IaccComputeJob, IaccComputeJobInput, IaccComputePlan};
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
pub use metric::{IaccMetricDefinition, IaccMetricState, IaccMetricStatus};
pub use metric_graph::{IaccMetricDependency, IaccMetricDependencyInput, IaccMetricLineage};
pub use quality::IaccQualityGateDecision;
pub use relation::{IaccImpactHop, IaccImpactTrace, IaccRelation, IaccRelationInput};
pub use source::{IaccSourceKind, IaccSourceSnapshot};
pub use store::{
    IaccHealth, IaccMetricRecomputeResult, IaccStore, IaccStoreError, IACC_SCHEMA_VERSION,
};

#[must_use]
pub fn iacc_reference(kind: &str, id: &str) -> String {
    format!("iacc:{kind}:{id}")
}
