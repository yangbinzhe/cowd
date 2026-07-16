use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    capability::MfgCapabilityId,
    mutation::{MfgActionAvailability, MfgActionRisk, MfgConfirmationKind, MfgMutationClass},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgSchemaOwner {
    Contract,
    MatrixCore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgConsumer {
    Webui,
    TuiP0,
    TuiP1,
    Backend,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum MfgCapabilityRequirement {
    One { capability: MfgCapabilityId },
    All { capabilities: Vec<MfgCapabilityId> },
    PerAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgRouteContract {
    pub route_id: MfgRouteId,
    pub method: String,
    pub path: String,
    pub request_schema: String,
    pub response_schema: String,
    pub schema_owner: MfgSchemaOwner,
    pub class: MfgMutationClass,
    pub capability: MfgCapabilityRequirement,
    pub risk: MfgActionRisk,
    pub confirmation: MfgConfirmationKind,
    pub emits_live_event: bool,
    #[serde(default)]
    pub consumers: Vec<MfgConsumer>,
    pub availability: MfgActionAvailability,
}

macro_rules! mfg_route_contracts {
    (
        $(
            $variant:ident,
            $id:literal,
            $method:literal,
            $path:literal,
            $owner:ident,
            $class:ident,
            $capability:expr,
            $risk:ident,
            $confirmation:ident,
            $live:literal,
            [$($consumer:ident),* $(,)?],
            $availability:ident;
        )+
    ) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
            JsonSchema,
        )]
        pub enum MfgRouteId {
            $(
                #[serde(rename = $id)]
                #[schemars(rename = $id)]
                $variant,
            )+
        }

        impl MfgRouteId {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $id,)+
                }
            }

            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                Self::ALL
                    .iter()
                    .copied()
                    .find(|route_id| route_id.as_str() == value)
            }
        }

        #[must_use]
        pub fn mfg_route_contracts() -> Vec<MfgRouteContract> {
            vec![
                $(
                    MfgRouteContract {
                        route_id: MfgRouteId::$variant,
                        method: $method.to_string(),
                        path: $path.to_string(),
                        request_schema: format!("{}.request.v1", $id),
                        response_schema: format!("{}.response.v1", $id),
                        schema_owner: MfgSchemaOwner::$owner,
                        class: MfgMutationClass::$class,
                        capability: $capability,
                        risk: MfgActionRisk::$risk,
                        confirmation: MfgConfirmationKind::$confirmation,
                        emits_live_event: $live,
                        consumers: vec![$(MfgConsumer::$consumer,)*],
                        availability: MfgActionAvailability::$availability,
                    },
                )+
            ]
        }
    };
}

use MfgCapabilityId as C;

mfg_route_contracts! {
    ContractGet, "mfg.contract.get", "GET", "/api/apps/mfg/contract", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    AppGet, "mfg.app.get", "GET", "/api/apps/mfg/app", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    ProductionGovernanceGet, "mfg.production.governance.get", "GET", "/api/apps/mfg/production/governance", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityHealthGet, "mfg.reality.health.get", "GET", "/api/apps/mfg/reality/health", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityDataPlaneHealthGet, "mfg.reality.data_plane.health.get", "GET", "/api/apps/mfg/reality/data-plane/health", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityDataPlaneIngestPlan, "mfg.reality.data_plane.ingest_plan", "POST", "/api/apps/mfg/reality/data-plane/ingest-plan", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealitySourcePackUpsert, "mfg.reality.source_pack.upsert", "POST", "/api/apps/mfg/reality/source-packs/upsert", MatrixCore, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealitySourcePackGet, "mfg.reality.source_pack.get", "GET", "/api/apps/mfg/reality/source-packs/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealitySourcePackValidate, "mfg.reality.source_pack.validate", "POST", "/api/apps/mfg/reality/source-packs/:id/validate", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealitySourcePackIngestFile, "mfg.reality.source_pack.ingest_file", "POST", "/api/apps/mfg/reality/source-packs/:id/ingest-file", MatrixCore, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    RealitySourcePackDeltaPlan, "mfg.reality.source_pack.delta_plan", "POST", "/api/apps/mfg/reality/source-packs/:id/delta-plan", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityConnectorRunPlan, "mfg.reality.connector_run.plan", "POST", "/api/apps/mfg/reality/source-packs/:id/connector-runs/plan", MatrixCore, Create, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityConnectorRunExecute, "mfg.reality.connector_run.execute", "POST", "/api/apps/mfg/reality/source-packs/:id/connector-runs/run", MatrixCore, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    RealityConnectorRunGet, "mfg.reality.connector_run.get", "GET", "/api/apps/mfg/reality/connector-runs/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityMetricList, "mfg.reality.metric.list", "GET", "/api/apps/mfg/reality/metrics", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityMetricAttentionPlan, "mfg.reality.metric.attention_plan", "POST", "/api/apps/mfg/reality/metrics/attention-plan", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityMetricSnapshotMaterialize, "mfg.reality.metric_snapshot.materialize", "POST", "/api/apps/mfg/reality/metrics/snapshots/materialize", MatrixCore, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityMetricRecompute, "mfg.reality.metric.recompute", "POST", "/api/apps/mfg/reality/metrics/recompute", MatrixCore, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    RealityMetricGet, "mfg.reality.metric.get", "GET", "/api/apps/mfg/reality/metrics/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityMetricLineage, "mfg.reality.metric.lineage", "GET", "/api/apps/mfg/reality/metrics/:id/lineage", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityMetricDependencyUpsert, "mfg.reality.metric_dependency.upsert", "POST", "/api/apps/mfg/reality/metric-dependencies/upsert", MatrixCore, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityMetricDependencyAffectedPlan, "mfg.reality.metric_dependency.affected_plan", "POST", "/api/apps/mfg/reality/metric-dependencies/affected-by-fact-type", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityComputeJobPlan, "mfg.reality.compute_job.plan", "POST", "/api/apps/mfg/reality/compute/jobs/plan", MatrixCore, Create, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityComputeJobGet, "mfg.reality.compute_job.get", "GET", "/api/apps/mfg/reality/compute/jobs/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityComputeJobExecute, "mfg.reality.compute_job.execute", "POST", "/api/apps/mfg/reality/compute/jobs/:id/run", MatrixCore, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    RealityEntityList, "mfg.reality.entity.list", "GET", "/api/apps/mfg/reality/entities", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityEntityUpsert, "mfg.reality.entity.upsert", "POST", "/api/apps/mfg/reality/entities/upsert", MatrixCore, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityEntityResolveSourceKey, "mfg.reality.entity.resolve_source_key", "POST", "/api/apps/mfg/reality/entities/resolve-source-key", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityEntityMatchCandidate, "mfg.reality.entity.match_candidate", "POST", "/api/apps/mfg/reality/entities/match-candidate", MatrixCore, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityEntityConflictDecision, "mfg.reality.entity.conflict_decision", "POST", "/api/apps/mfg/reality/entities/conflict-decision", MatrixCore, Update, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    RealityEntityGet, "mfg.reality.entity.get", "GET", "/api/apps/mfg/reality/entities/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityEntityRelations, "mfg.reality.entity.relations", "GET", "/api/apps/mfg/reality/entities/:id/relations", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityEntityImpactPath, "mfg.reality.entity.impact_path", "GET", "/api/apps/mfg/reality/entities/:id/impact-path", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityRelationUpsert, "mfg.reality.relation.upsert", "POST", "/api/apps/mfg/reality/relations/upsert", MatrixCore, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityFactIngest, "mfg.reality.fact.ingest", "POST", "/api/apps/mfg/reality/facts/ingest", MatrixCore, Create, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    RealityChangeList, "mfg.reality.change.list", "GET", "/api/apps/mfg/reality/changes", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    RealityAttentionHot, "mfg.reality.attention.hot", "GET", "/api/apps/mfg/reality/attention/hot", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityEvidenceBuild, "mfg.reality.evidence.build", "POST", "/api/apps/mfg/reality/evidence/build", MatrixCore, Create, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui], Active;
    RealityEvidenceGet, "mfg.reality.evidence.get", "GET", "/api/apps/mfg/reality/evidence/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityEvidenceQualityGate, "mfg.reality.evidence.quality_gate", "POST", "/api/apps/mfg/reality/evidence/:id/quality-gate", MatrixCore, Create, MfgCapabilityRequirement::One { capability: C::DataManage }, Medium, Target, true, [Webui, TuiP1], Active;
    RealityEvidenceContext, "mfg.reality.evidence.context", "GET", "/api/apps/mfg/reality/evidence/:id/context", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    RealityQualityGateGet, "mfg.reality.quality_gate.get", "GET", "/api/apps/mfg/reality/quality-gates/:id", MatrixCore, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    SkillList, "mfg.skill.list", "GET", "/api/apps/mfg/skills", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    SkillGet, "mfg.skill.get", "GET", "/api/apps/mfg/skills/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    SkillRunGet, "mfg.skill_run.get", "GET", "/api/apps/mfg/skill-runs/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    CommandCenterGet, "mfg.command_center.get", "GET", "/api/apps/mfg/command-center", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    CommandCenterLiveGet, "mfg.command_center.live.get", "GET", "/api/apps/mfg/command-center/live", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    DecisionTraceGet, "mfg.decision_trace.get", "GET", "/api/apps/mfg/decision-trace", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    DomainServerManufacturingGet, "mfg.domain.server_manufacturing.get", "GET", "/api/apps/mfg/domain/server-manufacturing", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    DomainServerManufacturingSeed, "mfg.domain.server_manufacturing.seed", "POST", "/api/apps/mfg/domain/server-manufacturing/seed", Contract, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    OntologyServerManufacturingGet, "mfg.ontology.server_manufacturing.get", "GET", "/api/apps/mfg/ontology/server-manufacturing", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    OntologyServerManufacturingSeed, "mfg.ontology.server_manufacturing.seed", "POST", "/api/apps/mfg/ontology/server-manufacturing/seed", Contract, Effect, MfgCapabilityRequirement::One { capability: C::DataManage }, High, TargetAndConfirm, true, [Webui], Active;
    IncidentList, "mfg.incident.list", "GET", "/api/apps/mfg/incidents", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    IncidentCreate, "mfg.incident.create", "POST", "/api/apps/mfg/incidents", Contract, Create, MfgCapabilityRequirement::One { capability: C::IncidentOperate }, Medium, Target, true, [Webui, TuiP0], Active;
    IncidentGet, "mfg.incident.get", "GET", "/api/apps/mfg/incidents/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    IncidentRoomGet, "mfg.incident.room.get", "GET", "/api/apps/mfg/incidents/:id/room", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    IncidentAnalyze, "mfg.incident.analyze", "POST", "/api/apps/mfg/incidents/:id/analyze", Contract, Create, MfgCapabilityRequirement::One { capability: C::IncidentOperate }, Medium, Target, true, [Webui, TuiP0], Active;
    IncidentCasePromote, "mfg.incident.case.promote", "POST", "/api/apps/mfg/incidents/:id/cases/promote", Contract, Create, MfgCapabilityRequirement::One { capability: C::IncidentOperate }, Medium, Target, true, [Webui], Active;
    IncidentPlaybookRecommend, "mfg.incident.playbook.recommend", "POST", "/api/apps/mfg/incidents/:id/playbooks/recommend", Contract, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    IncidentSkillPlan, "mfg.incident.skill.plan", "POST", "/api/apps/mfg/incidents/:id/skills/plan", Contract, Preview, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    IncidentSkillRun, "mfg.incident.skill.run", "POST", "/api/apps/mfg/incidents/:id/skills/:skill_id/run", Contract, Effect, MfgCapabilityRequirement::One { capability: C::SkillRun }, High, TargetAndConfirm, true, [Webui, TuiP1], Active;
    IncidentSkillRunList, "mfg.incident.skill_run.list", "GET", "/api/apps/mfg/incidents/:id/skills", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    CaseGet, "mfg.case.get", "GET", "/api/apps/mfg/cases/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    CaseSearch, "mfg.case.search", "GET", "/api/apps/mfg/cases/search", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    PlaybookUpsert, "mfg.playbook.upsert", "POST", "/api/apps/mfg/playbooks/upsert", Contract, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::PlaybookManage }, Medium, Target, true, [Webui], Active;
    PlaybookGet, "mfg.playbook.get", "GET", "/api/apps/mfg/playbooks/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    AnalysisGet, "mfg.analysis.get", "GET", "/api/apps/mfg/analyses/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    AnalysisActionExecute, "mfg.analysis.action.execute", "POST", "/api/apps/mfg/analyses/:analysis_id/actions/:action_id/execute", Contract, PreviewOrEffect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui, TuiP0], Active;
    ExecutionGet, "mfg.execution.get", "GET", "/api/apps/mfg/executions/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    ExecutionCrossPlaneExecute, "mfg.execution.cross_plane.execute", "POST", "/api/apps/mfg/executions/:id/cross-plane/execute", Contract, PreviewOrEffect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui], Active;
    ExecutionFeedbackCreate, "mfg.execution.feedback.create", "POST", "/api/apps/mfg/executions/:id/feedback", Contract, Create, MfgCapabilityRequirement::One { capability: C::ExecutionFeedback }, Medium, Target, true, [Webui, TuiP0], Active;
    CockpitProfileList, "mfg.cockpit.profile.list", "GET", "/api/apps/mfg/cockpit/profiles", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    CockpitProfileUpsert, "mfg.cockpit.profile.upsert", "POST", "/api/apps/mfg/cockpit/profiles/upsert", Contract, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::CockpitManage }, Medium, Target, true, [Webui], Active;
    CockpitProfileGet, "mfg.cockpit.profile.get", "GET", "/api/apps/mfg/cockpit/profiles/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    CockpitProfileDelete, "mfg.cockpit.profile.delete", "DELETE", "/api/apps/mfg/cockpit/profiles/:id", Contract, Update, MfgCapabilityRequirement::One { capability: C::CockpitManage }, High, TargetAndConfirm, true, [Webui], Active;
    CockpitProfileClone, "mfg.cockpit.profile.clone", "POST", "/api/apps/mfg/cockpit/profiles/:id/clone", Contract, Create, MfgCapabilityRequirement::One { capability: C::CockpitManage }, Medium, Target, true, [Webui], Active;
    CockpitProfileShare, "mfg.cockpit.profile.share", "POST", "/api/apps/mfg/cockpit/profiles/:id/share", Contract, Update, MfgCapabilityRequirement::One { capability: C::CockpitManage }, High, TargetAndConfirm, true, [Webui], Active;
    CockpitWidgetCatalogGet, "mfg.cockpit.widget_catalog.get", "GET", "/api/apps/mfg/cockpit/widget-catalog", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    CockpitProjectionGet, "mfg.cockpit.projection.get", "GET", "/api/apps/mfg/cockpit/profiles/:id/projection", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    CockpitWidgetProjectionGet, "mfg.cockpit.widget_projection.get", "GET", "/api/apps/mfg/cockpit/profiles/:id/widgets/:instance_id/projection", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    ReportGenerate, "mfg.report.generate", "POST", "/api/apps/mfg/cockpit/profiles/:id/reports/generate", Contract, Create, MfgCapabilityRequirement::One { capability: C::ReportGenerate }, Medium, Target, true, [Webui, TuiP0], Active;
    ReportScheduleRun, "mfg.report.schedule.run", "POST", "/api/apps/mfg/cockpit/reports/schedules/run", Contract, Effect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui], Active;
    ReportList, "mfg.report.list", "GET", "/api/apps/mfg/cockpit/reports", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    ReportGet, "mfg.report.get", "GET", "/api/apps/mfg/cockpit/reports/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    ReportDeliver, "mfg.report.deliver", "POST", "/api/apps/mfg/cockpit/reports/:id/deliver", Contract, PreviewOrEffect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui, TuiP0], Active;
    ReportDeliveryStateGet, "mfg.report.delivery_state.get", "GET", "/api/apps/mfg/cockpit/reports/:id/delivery-state", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    ReportDeliveryRetry, "mfg.report.delivery.retry", "POST", "/api/apps/mfg/cockpit/reports/:id/delivery/retry", Contract, PreviewOrEffect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui, TuiP0], Active;
    ReportReviewRequest, "mfg.report.review.request", "POST", "/api/apps/mfg/cockpit/reports/:id/reviews", Contract, Create, MfgCapabilityRequirement::One { capability: C::ReportDeliver }, Medium, Target, true, [Webui, TuiP0], Active;
    ReportReviewList, "mfg.report.review.list", "GET", "/api/apps/mfg/cockpit/report-reviews", Contract, Read, MfgCapabilityRequirement::One { capability: C::ReportReview }, Low, None, false, [Webui, TuiP0], Active;
    ReportReviewGet, "mfg.report.review.get", "GET", "/api/apps/mfg/cockpit/report-reviews/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::ReportReview }, Low, None, false, [Webui, TuiP0], Active;
    ReportReviewDecide, "mfg.report.review.decide", "POST", "/api/apps/mfg/cockpit/report-reviews/:id/decision", Contract, Effect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui, TuiP0], Active;
    AlertRuleList, "mfg.alert_rule.list", "GET", "/api/apps/mfg/focus/alert-rules", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    AlertRuleUpsert, "mfg.alert_rule.upsert", "POST", "/api/apps/mfg/focus/alert-rules", Contract, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::AlertManage }, Medium, Target, true, [Webui], Active;
    AlertList, "mfg.alert.list", "GET", "/api/apps/mfg/focus/alerts", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    AlertSubscriptionList, "mfg.alert_subscription.list", "GET", "/api/apps/mfg/focus/alert-subscriptions", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui], Active;
    AlertSubscriptionUpsert, "mfg.alert_subscription.upsert", "POST", "/api/apps/mfg/focus/alert-subscriptions", Contract, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::AlertManage }, Medium, Target, true, [Webui], Active;
    AlertCommand, "mfg.alert.command", "POST", "/api/apps/mfg/focus/alerts/:id/command", Contract, Update, MfgCapabilityRequirement::One { capability: C::AlertRespond }, Medium, Target, true, [Webui, TuiP0], Active;
    ForecastList, "mfg.forecast.list", "GET", "/api/apps/mfg/focus/forecasts", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP1], Active;
    AssignmentList, "mfg.assignment.list", "GET", "/api/apps/mfg/assignments", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    AssignmentUpsert, "mfg.assignment.upsert", "POST", "/api/apps/mfg/assignments", Contract, CreateOrUpdate, MfgCapabilityRequirement::One { capability: C::AssignmentManage }, Medium, Target, true, [Webui, TuiP0], Active;
    AssignmentGet, "mfg.assignment.get", "GET", "/api/apps/mfg/assignments/:id", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    AssignmentCommand, "mfg.assignment.command", "POST", "/api/apps/mfg/assignments/:id/command", Contract, UpdateOrEffect, MfgCapabilityRequirement::PerAction, High, TargetAndConfirm, true, [Webui, TuiP0], Active;
    LiveStream, "mfg.live.stream", "GET", "/api/apps/mfg/live", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], Active;
    LiveSnapshot, "mfg.live.snapshot", "GET", "/api/apps/mfg/live/snapshot", Contract, Read, MfgCapabilityRequirement::One { capability: C::Read }, Low, None, false, [Webui, TuiP0], PlannedV545;
}

#[must_use]
pub fn mfg_route_contract(route_id: MfgRouteId) -> Option<MfgRouteContract> {
    mfg_route_contracts()
        .into_iter()
        .find(|contract| contract.route_id == route_id)
}

#[must_use]
pub fn mfg_route_contract_by_method_path(method: &str, path: &str) -> Option<MfgRouteContract> {
    mfg_route_contracts()
        .into_iter()
        .find(|contract| contract.method == method && contract.path == path)
}

/// Canonical TUI P0 read surface. Consumers must derive this set from route
/// semantics instead of maintaining a second hand-written inventory.
#[must_use]
pub fn mfg_tui_p0_read_route_contracts() -> Vec<MfgRouteContract> {
    mfg_route_contracts()
        .into_iter()
        .filter(|route| {
            route.availability == MfgActionAvailability::Active
                && route.consumers.contains(&MfgConsumer::TuiP0)
                && route.class == MfgMutationClass::Read
        })
        .collect()
}
