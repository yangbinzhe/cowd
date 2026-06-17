//! Manufacturing application contracts.
//!
//! MFG is the manufacturing application layer built on Matrix structured facts,
//! Memory, runtime context, skills and governed action dispatch.

mod analysis;
mod app;
mod cockpit;
mod domain;
mod execution;
mod incident;
mod memory_case;
mod ontology;
mod skill;

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

#[must_use]
pub fn mfg_seed_plan() -> MfgDomainSeedPlan {
    server_manufacturing_seed_plan()
}

#[must_use]
pub fn mfg_ontology_pack() -> crate::matrix::MatrixOntologyPack {
    server_manufacturing_ontology_pack()
}
