use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MfgCapabilityId {
    #[serde(rename = "mfg.read")]
    Read,
    #[serde(rename = "mfg.incident.operate")]
    IncidentOperate,
    #[serde(rename = "mfg.playbook.manage")]
    PlaybookManage,
    #[serde(rename = "mfg.alert.respond")]
    AlertRespond,
    #[serde(rename = "mfg.alert.manage")]
    AlertManage,
    #[serde(rename = "mfg.assignment.manage")]
    AssignmentManage,
    #[serde(rename = "mfg.assignment.lifecycle")]
    AssignmentLifecycle,
    #[serde(rename = "mfg.execution.operate")]
    ExecutionOperate,
    #[serde(rename = "mfg.execution.feedback")]
    ExecutionFeedback,
    #[serde(rename = "mfg.report.generate")]
    ReportGenerate,
    #[serde(rename = "mfg.report.deliver")]
    ReportDeliver,
    #[serde(rename = "mfg.report.review")]
    ReportReview,
    #[serde(rename = "mfg.skill.run")]
    SkillRun,
    #[serde(rename = "mfg.cockpit.manage")]
    CockpitManage,
    #[serde(rename = "mfg.data.manage")]
    DataManage,
}

impl MfgCapabilityId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "mfg.read",
            Self::IncidentOperate => "mfg.incident.operate",
            Self::PlaybookManage => "mfg.playbook.manage",
            Self::AlertRespond => "mfg.alert.respond",
            Self::AlertManage => "mfg.alert.manage",
            Self::AssignmentManage => "mfg.assignment.manage",
            Self::AssignmentLifecycle => "mfg.assignment.lifecycle",
            Self::ExecutionOperate => "mfg.execution.operate",
            Self::ExecutionFeedback => "mfg.execution.feedback",
            Self::ReportGenerate => "mfg.report.generate",
            Self::ReportDeliver => "mfg.report.deliver",
            Self::ReportReview => "mfg.report.review",
            Self::SkillRun => "mfg.skill.run",
            Self::CockpitManage => "mfg.cockpit.manage",
            Self::DataManage => "mfg.data.manage",
        }
    }

    pub const ALL: [Self; 15] = [
        Self::Read,
        Self::IncidentOperate,
        Self::PlaybookManage,
        Self::AlertRespond,
        Self::AlertManage,
        Self::AssignmentManage,
        Self::AssignmentLifecycle,
        Self::ExecutionOperate,
        Self::ExecutionFeedback,
        Self::ReportGenerate,
        Self::ReportDeliver,
        Self::ReportReview,
        Self::SkillRun,
        Self::CockpitManage,
        Self::DataManage,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgCoreProfileId {
    #[serde(rename = "core_legacy_0_9_530")]
    #[schemars(rename = "core_legacy_0_9_530")]
    CoreLegacy09530,
    #[serde(rename = "core_manager")]
    #[schemars(rename = "core_manager")]
    CoreManager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgProfileId {
    #[serde(rename = "mfg_viewer")]
    #[schemars(rename = "mfg_viewer")]
    MfgViewer,
    #[serde(rename = "mfg_legacy_0_9_529")]
    #[schemars(rename = "mfg_legacy_0_9_529")]
    MfgLegacy09529,
    #[serde(rename = "mfg_operator")]
    #[schemars(rename = "mfg_operator")]
    MfgOperator,
    #[serde(rename = "mfg_reviewer")]
    #[schemars(rename = "mfg_reviewer")]
    MfgReviewer,
    #[serde(rename = "mfg_manager")]
    #[schemars(rename = "mfg_manager")]
    MfgManager,
}

#[must_use]
pub fn core_profile_capabilities(profile: MfgCoreProfileId) -> &'static [&'static str] {
    const LEGACY: &[&str] = &[
        "approval.respond",
        "definition.manage",
        "evolution.release.manage",
        "runtime.maintenance.manage",
        "runtime.outbox.retry",
    ];
    const MANAGER: &[&str] = &[
        "approval.respond",
        "definition.manage",
        "definition.default.set",
        "definition.rollback",
        "evolution.release.manage",
        "runtime.maintenance.manage",
        "runtime.outbox.retry",
    ];
    match profile {
        MfgCoreProfileId::CoreLegacy09530 => LEGACY,
        MfgCoreProfileId::CoreManager => MANAGER,
    }
}

#[must_use]
pub fn mfg_profile_capabilities(profile: MfgProfileId) -> &'static [MfgCapabilityId] {
    use MfgCapabilityId as C;
    const VIEWER: &[C] = &[C::Read];
    const LEGACY: &[C] = &[
        C::Read,
        C::DataManage,
        C::IncidentOperate,
        C::PlaybookManage,
        C::AlertRespond,
        C::AlertManage,
        C::AssignmentManage,
        C::ExecutionOperate,
        C::ExecutionFeedback,
        C::ReportGenerate,
        C::ReportDeliver,
        C::SkillRun,
        C::CockpitManage,
    ];
    const OPERATOR: &[C] = &[
        C::Read,
        C::IncidentOperate,
        C::AlertRespond,
        C::AssignmentManage,
        C::ExecutionFeedback,
        C::ReportGenerate,
        C::ReportDeliver,
    ];
    const REVIEWER: &[C] = &[C::Read, C::ReportReview];
    const MANAGER: &[C] = &[
        C::Read,
        C::IncidentOperate,
        C::PlaybookManage,
        C::AlertRespond,
        C::AlertManage,
        C::AssignmentManage,
        C::AssignmentLifecycle,
        C::ExecutionOperate,
        C::ExecutionFeedback,
        C::ReportGenerate,
        C::ReportDeliver,
        C::ReportReview,
        C::SkillRun,
        C::CockpitManage,
        C::DataManage,
    ];
    match profile {
        MfgProfileId::MfgViewer => VIEWER,
        MfgProfileId::MfgLegacy09529 => LEGACY,
        MfgProfileId::MfgOperator => OPERATOR,
        MfgProfileId::MfgReviewer => REVIEWER,
        MfgProfileId::MfgManager => MANAGER,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgEntitlementProjectionV2 {
    pub core_profile_id: MfgCoreProfileId,
    pub mfg_profile_id: MfgProfileId,
    pub profile_revision: u64,
    pub credential_epoch: u64,
    #[serde(default)]
    pub ceiling: Vec<String>,
    #[serde(default)]
    pub granted: Vec<String>,
    #[serde(default)]
    pub denied: Vec<String>,
}

#[must_use]
pub fn active_mfg_capabilities_for_surface(surface_id: &str) -> Vec<String> {
    if surface_id == "legacy_gateway" {
        return MfgCapabilityId::ALL
            .iter()
            .copied()
            .map(|capability| capability.as_str().to_string())
            .collect();
    }
    let routes = crate::route::mfg_route_contracts();
    let route_visible = |route: &crate::route::MfgRouteContract| {
        if route.availability != crate::mutation::MfgActionAvailability::Active {
            return false;
        }
        match surface_id {
            "webui" => route.consumers.contains(&crate::route::MfgConsumer::Webui),
            "tui" => {
                route.consumers.contains(&crate::route::MfgConsumer::TuiP0)
                    || route.consumers.contains(&crate::route::MfgConsumer::TuiP1)
            }
            "cli" => matches!(
                route.route_id,
                crate::route::MfgRouteId::ContractGet | crate::route::MfgRouteId::AppGet
            ),
            "backend" => route
                .consumers
                .contains(&crate::route::MfgConsumer::Backend),
            _ => false,
        }
    };
    let mut capabilities = crate::mutation::mfg_action_contracts()
        .into_iter()
        .filter(|action| {
            action.availability == crate::mutation::MfgActionAvailability::Active
                && routes
                    .iter()
                    .find(|route| route.route_id == action.route_id)
                    .is_some_and(route_visible)
        })
        .flat_map(|action| action.required_capabilities)
        .collect::<Vec<_>>();
    if routes.iter().any(route_visible) {
        capabilities.push(MfgCapabilityId::Read.as_str().to_string());
    }
    capabilities.sort();
    capabilities.dedup();
    capabilities
}
