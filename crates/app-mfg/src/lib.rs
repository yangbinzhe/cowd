//! Manufacturing application layer for cowd.
//!
//! This crate is the application-facing MFG boundary over Matrix structured
//! facts, Memory projections, skills and governed action dispatch.

pub mod analysis;
pub mod app;
pub mod cockpit;
pub mod domain;
pub mod execution;
pub mod incident;
pub mod memory_case;
pub mod ontology;
mod repository;
pub mod skill;
mod store;
pub mod workflow;

pub use analysis::{
    MfgAttributionCandidate, MfgImpactPath, MfgOperationalAnalysis, MfgRecommendedAction,
};
pub use app::{
    manufacturing_app_descriptor, MfgApplicationDescriptor, MfgApplicationDomain,
    MfgApplicationSurface, MfgApplicationSurfaceKind,
};
pub use cockpit::{
    MfgCockpitProfile, MfgCockpitProfileInput, MfgCockpitProjection,
    MfgCockpitReportDeliveryPayload, MfgCockpitReportDeliveryPayloadRequest,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportDeliveryState, MfgCockpitReportRequest,
    MfgCockpitReportSnapshot, MfgCockpitWidget,
};
pub use domain::{
    server_manufacturing_domain_pack, server_manufacturing_seed_plan, MfgDomainPack,
    MfgDomainScenario, MfgDomainSeedPlan, MfgDomainSeedResult,
};
pub use execution::{
    MfgActionExecution, MfgActionExecutionRequest, MfgActionFeedback, MfgCrossPlaneBridgeReceipt,
};
pub use incident::MfgIncident;
pub use memory_case::{MfgCasePromotion, MfgMemoryCase, MfgPlaybook, MfgPlaybookStep};
pub use ontology::server_manufacturing_ontology_pack;
pub use skill::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, skill_agent_node_id, MfgSkillManifest, MfgSkillPlan,
    MfgSkillRun,
};
pub use store::MfgStore;
pub use workflow::{
    MfgWorkflowEvidence, MfgWorkflowGraph, MfgWorkflowGraphError, MfgWorkflowNode,
    MfgWorkflowNodeKind, MfgWorkflowNodeStatus, MfgWorkflowReview, MfgWorkflowReviewVerdict,
    MfgWorkflowStatus,
};

pub use repository::{MfgHealth, MfgMetricRecomputeResult, MfgRepositoryError};

#[must_use]
pub fn mfg_seed_plan() -> MfgDomainSeedPlan {
    server_manufacturing_seed_plan()
}

#[must_use]
pub fn mfg_ontology_pack() -> matrix_core::MatrixOntologyPack {
    server_manufacturing_ontology_pack()
}
