//! Manufacturing application layer for cowd.
//!
//! This crate is the application-facing MFG boundary over Matrix structured
//! facts, Memory projections, skills and governed action dispatch.

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

pub mod analysis;
pub mod app;
pub mod cockpit;
pub mod domain;
pub mod execution;
pub mod incident;
pub mod memory_case;
pub mod ontology;
pub mod operations;
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
    default_mfg_widget_instances, mfg_cockpit_filter_merge_policy,
    mfg_cockpit_global_filter_schema, mfg_widget_catalog, MfgCockpitProfile,
    MfgCockpitProfileInput, MfgCockpitProjection, MfgCockpitReportDeliveryPayload,
    MfgCockpitReportDeliveryPayloadRequest, MfgCockpitReportDeliveryReceipt,
    MfgCockpitReportDeliveryState, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCockpitWidget, MfgCockpitWidgetProjection, MfgDashboardLayout, MfgDashboardScope,
    MfgDashboardSharingPolicy, MfgWidgetDefinition, MfgWidgetInstance, MfgWidgetPlacement,
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
pub use operations::{
    MfgAlertCommand, MfgAlertCommandInput, MfgAlertOccurrence, MfgAlertRule, MfgAlertRuleInput,
    MfgAlertSubscription, MfgAlertSubscriptionInput, MfgAssignment, MfgAssignmentCommand,
    MfgAssignmentCommandInput, MfgAssignmentInput, MfgCommandReceipt, MfgForecastProjection,
    MfgForecastSignal, MfgLiveProjection, MfgLiveProjectionEvent, MfgSurfaceNotificationTarget,
};
pub use skill::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, skill_agent_node_id, MfgSkillManifest, MfgSkillPlan,
    MfgSkillRun, MfgSkillTelemetry, MfgSkillToolCall, MfgSkillToolResult,
};
pub use store::MfgStore;
pub use workflow::{
    MfgWorkflowEvidence, MfgWorkflowGraph, MfgWorkflowGraphError, MfgWorkflowNode,
    MfgWorkflowNodeKind, MfgWorkflowNodeStatus, MfgWorkflowReview, MfgWorkflowReviewVerdict,
    MfgWorkflowStatus,
};

pub use repository::{MfgHealth, MfgMetricRecomputeResult, MfgMutationClaim, MfgRepositoryError};

#[must_use]
pub fn mfg_seed_plan() -> MfgDomainSeedPlan {
    server_manufacturing_seed_plan()
}

#[must_use]
pub fn mfg_ontology_pack() -> matrix_core::MatrixOntologyPack {
    server_manufacturing_ontology_pack()
}
