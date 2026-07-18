use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{route::MfgRouteId, version::MfgContractVersion};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgActionAvailability {
    Active,
    PlannedV541,
    PlannedV545,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgMutationClass {
    Read,
    Preview,
    Create,
    Update,
    Effect,
    CreateOrUpdate,
    PreviewOrEffect,
    UpdateOrEffect,
    PerAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgActionRisk {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgConfirmationKind {
    None,
    Target,
    TargetAndConfirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgRevisionSemantics {
    NotApplicable,
    CreateOnly,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgIdempotencySemantics {
    NotApplicablePureDryRun,
    Required,
    NaturalKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgMutationSemantics {
    ReadOnly,
    PreviewReceipt,
    DurableReceipt {
        revision: MfgRevisionSemantics,
        idempotency: MfgIdempotencySemantics,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgMutationContextV1 {
    pub idempotency_key: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub payload_digest: Option<String>,
    pub contract_version: MfgContractVersion,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MfgMultiActionId {
    #[serde(rename = "mfg.reality.source_pack.create")]
    #[schemars(rename = "mfg.reality.source_pack.create")]
    RealitySourcePackCreate,
    #[serde(rename = "mfg.reality.source_pack.update")]
    #[schemars(rename = "mfg.reality.source_pack.update")]
    RealitySourcePackUpdate,
    #[serde(rename = "mfg.reality.metric_dependency.create")]
    #[schemars(rename = "mfg.reality.metric_dependency.create")]
    RealityMetricDependencyCreate,
    #[serde(rename = "mfg.reality.metric_dependency.update")]
    #[schemars(rename = "mfg.reality.metric_dependency.update")]
    RealityMetricDependencyUpdate,
    #[serde(rename = "mfg.reality.entity.create")]
    #[schemars(rename = "mfg.reality.entity.create")]
    RealityEntityCreate,
    #[serde(rename = "mfg.reality.entity.update")]
    #[schemars(rename = "mfg.reality.entity.update")]
    RealityEntityUpdate,
    #[serde(rename = "mfg.reality.relation.create")]
    #[schemars(rename = "mfg.reality.relation.create")]
    RealityRelationCreate,
    #[serde(rename = "mfg.reality.relation.update")]
    #[schemars(rename = "mfg.reality.relation.update")]
    RealityRelationUpdate,
    #[serde(rename = "mfg.playbook.create")]
    #[schemars(rename = "mfg.playbook.create")]
    PlaybookCreate,
    #[serde(rename = "mfg.playbook.update")]
    #[schemars(rename = "mfg.playbook.update")]
    PlaybookUpdate,
    #[serde(rename = "mfg.cockpit.profile.create")]
    #[schemars(rename = "mfg.cockpit.profile.create")]
    CockpitProfileCreate,
    #[serde(rename = "mfg.cockpit.profile.update")]
    #[schemars(rename = "mfg.cockpit.profile.update")]
    CockpitProfileUpdate,
    #[serde(rename = "mfg.alert_rule.create")]
    #[schemars(rename = "mfg.alert_rule.create")]
    AlertRuleCreate,
    #[serde(rename = "mfg.alert_rule.update")]
    #[schemars(rename = "mfg.alert_rule.update")]
    AlertRuleUpdate,
    #[serde(rename = "mfg.alert_subscription.create")]
    #[schemars(rename = "mfg.alert_subscription.create")]
    AlertSubscriptionCreate,
    #[serde(rename = "mfg.alert_subscription.update")]
    #[schemars(rename = "mfg.alert_subscription.update")]
    AlertSubscriptionUpdate,
    #[serde(rename = "mfg.assignment.create")]
    #[schemars(rename = "mfg.assignment.create")]
    AssignmentCreate,
    #[serde(rename = "mfg.assignment.update")]
    #[schemars(rename = "mfg.assignment.update")]
    AssignmentUpdate,
    #[serde(rename = "mfg.alert.acknowledge")]
    #[schemars(rename = "mfg.alert.acknowledge")]
    AlertAcknowledge,
    #[serde(rename = "mfg.alert.snooze")]
    #[schemars(rename = "mfg.alert.snooze")]
    AlertSnooze,
    #[serde(rename = "mfg.alert.resolve")]
    #[schemars(rename = "mfg.alert.resolve")]
    AlertResolve,
    #[serde(rename = "mfg.alert.escalate")]
    #[schemars(rename = "mfg.alert.escalate")]
    AlertEscalate,
    #[serde(rename = "mfg.assignment.assign")]
    #[schemars(rename = "mfg.assignment.assign")]
    AssignmentAssign,
    #[serde(rename = "mfg.assignment.claim")]
    #[schemars(rename = "mfg.assignment.claim")]
    AssignmentClaim,
    #[serde(rename = "mfg.assignment.transfer")]
    #[schemars(rename = "mfg.assignment.transfer")]
    AssignmentTransfer,
    #[serde(rename = "mfg.assignment.unassign")]
    #[schemars(rename = "mfg.assignment.unassign")]
    AssignmentUnassign,
    #[serde(rename = "mfg.assignment.watch")]
    #[schemars(rename = "mfg.assignment.watch")]
    AssignmentWatch,
    #[serde(rename = "mfg.assignment.request_update")]
    #[schemars(rename = "mfg.assignment.request_update")]
    AssignmentRequestUpdate,
    #[serde(rename = "mfg.assignment.escalate")]
    #[schemars(rename = "mfg.assignment.escalate")]
    AssignmentEscalate,
    #[serde(rename = "mfg.assignment.start")]
    #[schemars(rename = "mfg.assignment.start")]
    AssignmentStart,
    #[serde(rename = "mfg.assignment.complete")]
    #[schemars(rename = "mfg.assignment.complete")]
    AssignmentComplete,
    #[serde(rename = "mfg.analysis.action.dry_run")]
    #[schemars(rename = "mfg.analysis.action.dry_run")]
    AnalysisActionDryRun,
    #[serde(rename = "mfg.analysis.action.commit")]
    #[schemars(rename = "mfg.analysis.action.commit")]
    AnalysisActionCommit,
    #[serde(rename = "mfg.execution.cross_plane.dry_run")]
    #[schemars(rename = "mfg.execution.cross_plane.dry_run")]
    ExecutionCrossPlaneDryRun,
    #[serde(rename = "mfg.execution.cross_plane.commit")]
    #[schemars(rename = "mfg.execution.cross_plane.commit")]
    ExecutionCrossPlaneCommit,
    #[serde(rename = "mfg.report.deliver.dry_run")]
    #[schemars(rename = "mfg.report.deliver.dry_run")]
    ReportDeliverDryRun,
    #[serde(rename = "mfg.report.deliver.commit")]
    #[schemars(rename = "mfg.report.deliver.commit")]
    ReportDeliverCommit,
    #[serde(rename = "mfg.report.schedule.generate_only")]
    #[schemars(rename = "mfg.report.schedule.generate_only")]
    ReportScheduleGenerateOnly,
    #[serde(rename = "mfg.report.schedule.generate_and_deliver")]
    #[schemars(rename = "mfg.report.schedule.generate_and_deliver")]
    ReportScheduleGenerateAndDeliver,
    #[serde(rename = "mfg.report.delivery.retry_dry_run")]
    #[schemars(rename = "mfg.report.delivery.retry_dry_run")]
    ReportDeliveryRetryDryRun,
    #[serde(rename = "mfg.report.delivery.retry_commit")]
    #[schemars(rename = "mfg.report.delivery.retry_commit")]
    ReportDeliveryRetryCommit,
    #[serde(rename = "mfg.report.review.force_retry")]
    #[schemars(rename = "mfg.report.review.force_retry")]
    ReportReviewForceRetry,
    #[serde(rename = "mfg.report.review.reroute")]
    #[schemars(rename = "mfg.report.review.reroute")]
    ReportReviewReroute,
    #[serde(rename = "mfg.report.review.abandon")]
    #[schemars(rename = "mfg.report.review.abandon")]
    ReportReviewAbandon,
    #[serde(rename = "mfg.report.review.resolve")]
    #[schemars(rename = "mfg.report.review.resolve")]
    ReportReviewResolve,
    #[serde(rename = "mfg.report.review.reject")]
    #[schemars(rename = "mfg.report.review.reject")]
    ReportReviewReject,
    #[serde(rename = "mfg.skill.run")]
    #[schemars(rename = "mfg.skill.run")]
    SkillRun,
}

impl MfgMultiActionId {
    pub const ALL: &'static [Self] = &[
        Self::RealitySourcePackCreate,
        Self::RealitySourcePackUpdate,
        Self::RealityMetricDependencyCreate,
        Self::RealityMetricDependencyUpdate,
        Self::RealityEntityCreate,
        Self::RealityEntityUpdate,
        Self::RealityRelationCreate,
        Self::RealityRelationUpdate,
        Self::PlaybookCreate,
        Self::PlaybookUpdate,
        Self::CockpitProfileCreate,
        Self::CockpitProfileUpdate,
        Self::AlertRuleCreate,
        Self::AlertRuleUpdate,
        Self::AlertSubscriptionCreate,
        Self::AlertSubscriptionUpdate,
        Self::AssignmentCreate,
        Self::AssignmentUpdate,
        Self::AlertAcknowledge,
        Self::AlertSnooze,
        Self::AlertResolve,
        Self::AlertEscalate,
        Self::AssignmentAssign,
        Self::AssignmentClaim,
        Self::AssignmentTransfer,
        Self::AssignmentUnassign,
        Self::AssignmentWatch,
        Self::AssignmentRequestUpdate,
        Self::AssignmentEscalate,
        Self::AssignmentStart,
        Self::AssignmentComplete,
        Self::AnalysisActionDryRun,
        Self::AnalysisActionCommit,
        Self::ExecutionCrossPlaneDryRun,
        Self::ExecutionCrossPlaneCommit,
        Self::ReportDeliverDryRun,
        Self::ReportDeliverCommit,
        Self::ReportScheduleGenerateOnly,
        Self::ReportScheduleGenerateAndDeliver,
        Self::ReportDeliveryRetryDryRun,
        Self::ReportDeliveryRetryCommit,
        Self::ReportReviewForceRetry,
        Self::ReportReviewReroute,
        Self::ReportReviewAbandon,
        Self::ReportReviewResolve,
        Self::ReportReviewReject,
        Self::SkillRun,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RealitySourcePackCreate => "mfg.reality.source_pack.create",
            Self::RealitySourcePackUpdate => "mfg.reality.source_pack.update",
            Self::RealityMetricDependencyCreate => "mfg.reality.metric_dependency.create",
            Self::RealityMetricDependencyUpdate => "mfg.reality.metric_dependency.update",
            Self::RealityEntityCreate => "mfg.reality.entity.create",
            Self::RealityEntityUpdate => "mfg.reality.entity.update",
            Self::RealityRelationCreate => "mfg.reality.relation.create",
            Self::RealityRelationUpdate => "mfg.reality.relation.update",
            Self::PlaybookCreate => "mfg.playbook.create",
            Self::PlaybookUpdate => "mfg.playbook.update",
            Self::CockpitProfileCreate => "mfg.cockpit.profile.create",
            Self::CockpitProfileUpdate => "mfg.cockpit.profile.update",
            Self::AlertRuleCreate => "mfg.alert_rule.create",
            Self::AlertRuleUpdate => "mfg.alert_rule.update",
            Self::AlertSubscriptionCreate => "mfg.alert_subscription.create",
            Self::AlertSubscriptionUpdate => "mfg.alert_subscription.update",
            Self::AssignmentCreate => "mfg.assignment.create",
            Self::AssignmentUpdate => "mfg.assignment.update",
            Self::AlertAcknowledge => "mfg.alert.acknowledge",
            Self::AlertSnooze => "mfg.alert.snooze",
            Self::AlertResolve => "mfg.alert.resolve",
            Self::AlertEscalate => "mfg.alert.escalate",
            Self::AssignmentAssign => "mfg.assignment.assign",
            Self::AssignmentClaim => "mfg.assignment.claim",
            Self::AssignmentTransfer => "mfg.assignment.transfer",
            Self::AssignmentUnassign => "mfg.assignment.unassign",
            Self::AssignmentWatch => "mfg.assignment.watch",
            Self::AssignmentRequestUpdate => "mfg.assignment.request_update",
            Self::AssignmentEscalate => "mfg.assignment.escalate",
            Self::AssignmentStart => "mfg.assignment.start",
            Self::AssignmentComplete => "mfg.assignment.complete",
            Self::AnalysisActionDryRun => "mfg.analysis.action.dry_run",
            Self::AnalysisActionCommit => "mfg.analysis.action.commit",
            Self::ExecutionCrossPlaneDryRun => "mfg.execution.cross_plane.dry_run",
            Self::ExecutionCrossPlaneCommit => "mfg.execution.cross_plane.commit",
            Self::ReportDeliverDryRun => "mfg.report.deliver.dry_run",
            Self::ReportDeliverCommit => "mfg.report.deliver.commit",
            Self::ReportScheduleGenerateOnly => "mfg.report.schedule.generate_only",
            Self::ReportScheduleGenerateAndDeliver => "mfg.report.schedule.generate_and_deliver",
            Self::ReportDeliveryRetryDryRun => "mfg.report.delivery.retry_dry_run",
            Self::ReportDeliveryRetryCommit => "mfg.report.delivery.retry_commit",
            Self::ReportReviewForceRetry => "mfg.report.review.force_retry",
            Self::ReportReviewReroute => "mfg.report.review.reroute",
            Self::ReportReviewAbandon => "mfg.report.review.abandon",
            Self::ReportReviewResolve => "mfg.report.review.resolve",
            Self::ReportReviewReject => "mfg.report.review.reject",
            Self::SkillRun => "mfg.skill.run",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|action_id| action_id.as_str() == value)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(untagged)]
pub enum MfgActionId {
    Route(MfgRouteId),
    Multi(MfgMultiActionId),
}

impl MfgActionId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Route(route) => route.as_str(),
            Self::Multi(action) => action.as_str(),
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        MfgMultiActionId::parse(value)
            .map(Self::Multi)
            .or_else(|| MfgRouteId::parse(value).map(Self::Route))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgActionContract {
    pub action_id: MfgActionId,
    pub route_id: MfgRouteId,
    pub availability: MfgActionAvailability,
    pub class: MfgMutationClass,
    pub risk: MfgActionRisk,
    pub confirmation: MfgConfirmationKind,
    pub mutation: MfgMutationSemantics,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub emits_live_event: bool,
}

#[must_use]
pub fn mfg_action_contracts() -> Vec<MfgActionContract> {
    let mut actions = crate::route::mfg_route_contracts()
        .into_iter()
        .filter_map(|route| {
            if matches!(
                route.route_id,
                MfgRouteId::AlertCommand | MfgRouteId::IncidentSkillRun
            ) {
                return None;
            }
            if matches!(
                &route.capability,
                crate::route::MfgCapabilityRequirement::PerAction
            ) {
                return None;
            }
            let mutation = match route.class {
                MfgMutationClass::Read => return None,
                MfgMutationClass::Preview => MfgMutationSemantics::PreviewReceipt,
                MfgMutationClass::Create => MfgMutationSemantics::DurableReceipt {
                    revision: MfgRevisionSemantics::CreateOnly,
                    idempotency: MfgIdempotencySemantics::Required,
                },
                MfgMutationClass::Update => MfgMutationSemantics::DurableReceipt {
                    revision: MfgRevisionSemantics::Required,
                    idempotency: MfgIdempotencySemantics::Required,
                },
                MfgMutationClass::Effect => MfgMutationSemantics::DurableReceipt {
                    // Route-owned effects such as ingest, recompute, execute,
                    // and seed have durable idempotency but no canonical CAS
                    // owner. Revision-owned effects are declared explicitly in
                    // the closed multi-action matrix below.
                    revision: MfgRevisionSemantics::NotApplicable,
                    idempotency: MfgIdempotencySemantics::Required,
                },
                MfgMutationClass::CreateOrUpdate
                | MfgMutationClass::PreviewOrEffect
                | MfgMutationClass::UpdateOrEffect
                | MfgMutationClass::PerAction => return None,
            };
            let required_capabilities = match route.capability {
                crate::route::MfgCapabilityRequirement::One { capability } => {
                    vec![capability.as_str().to_string()]
                }
                crate::route::MfgCapabilityRequirement::All { capabilities } => capabilities
                    .into_iter()
                    .map(|capability| capability.as_str().to_string())
                    .collect(),
                crate::route::MfgCapabilityRequirement::PerAction => Vec::new(),
            };
            Some(MfgActionContract {
                action_id: MfgActionId::Route(route.route_id),
                route_id: route.route_id,
                availability: route.availability,
                class: route.class,
                risk: route.risk,
                confirmation: route.confirmation,
                mutation,
                required_capabilities,
                emits_live_event: route.emits_live_event,
            })
        })
        .collect::<Vec<_>>();
    actions.extend(multi_action_contracts());
    actions.sort_by_key(|action| action.action_id.as_str());
    actions
}

#[must_use]
pub fn mfg_tui_action_contracts() -> Vec<MfgActionContract> {
    let routes = crate::route::mfg_tui_route_contracts()
        .into_iter()
        .map(|route| route.route_id)
        .collect::<std::collections::BTreeSet<_>>();
    mfg_action_contracts()
        .into_iter()
        .filter(|action| action.availability == MfgActionAvailability::Active)
        .filter(|action| routes.contains(&action.route_id))
        .collect()
}

fn multi_action_contracts() -> Vec<MfgActionContract> {
    use MfgActionRisk::{High as H, Low as L, Medium as M};
    use MfgConfirmationKind::{None as N, Target as T, TargetAndConfirm as TC};
    use MfgMultiActionId as A;
    use MfgMutationClass::{Create as C, Effect as E, Preview as P, Update as U};
    use MfgRouteId as R;

    let durable =
        |action_id, route_id, class, risk, confirmation, capabilities: &[&str], availability| {
            let revision = match (class, action_id) {
                (C, _) => MfgRevisionSemantics::CreateOnly,
                (U, _) => MfgRevisionSemantics::Required,
                (
                    E,
                    A::ExecutionCrossPlaneCommit
                    | A::ReportScheduleGenerateOnly
                    | A::ReportScheduleGenerateAndDeliver,
                ) => MfgRevisionSemantics::NotApplicable,
                (E, _) => MfgRevisionSemantics::Required,
                _ => MfgRevisionSemantics::NotApplicable,
            };
            MfgActionContract {
                action_id: MfgActionId::Multi(action_id),
                route_id,
                availability,
                class,
                risk,
                confirmation,
                mutation: MfgMutationSemantics::DurableReceipt {
                    revision,
                    idempotency: MfgIdempotencySemantics::Required,
                },
                required_capabilities: capabilities
                    .iter()
                    .map(|capability| (*capability).to_string())
                    .collect(),
                emits_live_event: true,
            }
        };
    let preview = |action_id, route_id| MfgActionContract {
        action_id: MfgActionId::Multi(action_id),
        route_id,
        availability: MfgActionAvailability::Active,
        class: P,
        risk: L,
        confirmation: N,
        mutation: MfgMutationSemantics::PreviewReceipt,
        required_capabilities: vec!["mfg.read".to_string()],
        emits_live_event: false,
    };

    vec![
        durable(
            A::RealitySourcePackCreate,
            R::RealitySourcePackUpsert,
            C,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealitySourcePackUpdate,
            R::RealitySourcePackUpsert,
            U,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealityMetricDependencyCreate,
            R::RealityMetricDependencyUpsert,
            C,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealityMetricDependencyUpdate,
            R::RealityMetricDependencyUpsert,
            U,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealityEntityCreate,
            R::RealityEntityUpsert,
            C,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealityEntityUpdate,
            R::RealityEntityUpsert,
            U,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealityRelationCreate,
            R::RealityRelationUpsert,
            C,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::RealityRelationUpdate,
            R::RealityRelationUpsert,
            U,
            M,
            T,
            &["mfg.data.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::PlaybookCreate,
            R::PlaybookUpsert,
            C,
            M,
            T,
            &["mfg.playbook.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::PlaybookUpdate,
            R::PlaybookUpsert,
            U,
            M,
            T,
            &["mfg.playbook.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::CockpitProfileCreate,
            R::CockpitProfileUpsert,
            C,
            M,
            T,
            &["mfg.cockpit.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::CockpitProfileUpdate,
            R::CockpitProfileUpsert,
            U,
            M,
            T,
            &["mfg.cockpit.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertRuleCreate,
            R::AlertRuleUpsert,
            C,
            M,
            T,
            &["mfg.alert.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertRuleUpdate,
            R::AlertRuleUpsert,
            U,
            M,
            T,
            &["mfg.alert.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertSubscriptionCreate,
            R::AlertSubscriptionUpsert,
            C,
            M,
            T,
            &["mfg.alert.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertSubscriptionUpdate,
            R::AlertSubscriptionUpsert,
            U,
            M,
            T,
            &["mfg.alert.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentCreate,
            R::AssignmentUpsert,
            C,
            M,
            T,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentUpdate,
            R::AssignmentUpsert,
            U,
            M,
            T,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertAcknowledge,
            R::AlertCommand,
            U,
            M,
            T,
            &["mfg.alert.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertSnooze,
            R::AlertCommand,
            U,
            M,
            T,
            &["mfg.alert.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertResolve,
            R::AlertCommand,
            U,
            M,
            T,
            &["mfg.alert.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AlertEscalate,
            R::AlertCommand,
            U,
            H,
            TC,
            &["mfg.alert.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentAssign,
            R::AssignmentCommand,
            U,
            M,
            T,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentClaim,
            R::AssignmentCommand,
            U,
            M,
            T,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentTransfer,
            R::AssignmentCommand,
            U,
            H,
            TC,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentUnassign,
            R::AssignmentCommand,
            U,
            H,
            TC,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentWatch,
            R::AssignmentCommand,
            U,
            L,
            N,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentRequestUpdate,
            R::AssignmentCommand,
            U,
            M,
            T,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentEscalate,
            R::AssignmentCommand,
            U,
            H,
            TC,
            &["mfg.assignment.manage"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentStart,
            R::AssignmentCommand,
            U,
            H,
            TC,
            &["mfg.assignment.lifecycle"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::AssignmentComplete,
            R::AssignmentCommand,
            E,
            H,
            TC,
            &["mfg.assignment.lifecycle"],
            MfgActionAvailability::Active,
        ),
        preview(A::AnalysisActionDryRun, R::AnalysisActionExecute),
        durable(
            A::AnalysisActionCommit,
            R::AnalysisActionExecute,
            E,
            H,
            TC,
            &["mfg.execution.operate"],
            MfgActionAvailability::Active,
        ),
        preview(A::ExecutionCrossPlaneDryRun, R::ExecutionCrossPlaneExecute),
        durable(
            A::ExecutionCrossPlaneCommit,
            R::ExecutionCrossPlaneExecute,
            E,
            H,
            TC,
            &["mfg.execution.operate"],
            MfgActionAvailability::Active,
        ),
        preview(A::ReportDeliverDryRun, R::ReportDeliver),
        durable(
            A::ReportDeliverCommit,
            R::ReportDeliver,
            E,
            H,
            TC,
            &["mfg.report.deliver"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportScheduleGenerateOnly,
            R::ReportScheduleRun,
            E,
            M,
            T,
            &["mfg.report.generate"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportScheduleGenerateAndDeliver,
            R::ReportScheduleRun,
            E,
            H,
            TC,
            &["mfg.report.generate", "mfg.report.deliver"],
            MfgActionAvailability::Active,
        ),
        preview(A::ReportDeliveryRetryDryRun, R::ReportDeliveryRetry),
        durable(
            A::ReportDeliveryRetryCommit,
            R::ReportDeliveryRetry,
            E,
            M,
            T,
            &["mfg.report.deliver"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportReviewForceRetry,
            R::ReportReviewDecide,
            E,
            H,
            TC,
            &["mfg.report.review", "approval.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportReviewReroute,
            R::ReportReviewDecide,
            E,
            H,
            TC,
            &["mfg.report.review", "approval.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportReviewAbandon,
            R::ReportReviewDecide,
            E,
            H,
            TC,
            &["mfg.report.review", "approval.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportReviewResolve,
            R::ReportReviewDecide,
            E,
            H,
            TC,
            &["mfg.report.review", "approval.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::ReportReviewReject,
            R::ReportReviewDecide,
            U,
            M,
            T,
            &["mfg.report.review", "approval.respond"],
            MfgActionAvailability::Active,
        ),
        durable(
            A::SkillRun,
            R::IncidentSkillRun,
            E,
            H,
            TC,
            &["mfg.skill.run"],
            MfgActionAvailability::Active,
        ),
    ]
}
