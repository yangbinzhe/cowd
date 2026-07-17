use crate::app::App;
use crate::gateway_client::GatewayApiClient;
use app_mfg_contract::{
    MfgApiErrorV1, MfgFrontendContractV1, MfgReadResponseV1, MfgReceiptV1, MfgRecoveryAction,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const MFG_DELTA_QUEUE_LIMIT: usize = 256;
pub(crate) const MFG_ALL_READ_SECTIONS: [&str; 20] = [
    "contract",
    "app",
    "command_center",
    "decision_trace",
    "live_stream",
    "incidents",
    "incident_detail",
    "incident_room",
    "analysis",
    "execution",
    "alert_rules",
    "alerts",
    "assignments",
    "assignment_detail",
    "reports",
    "report_detail",
    "delivery_state",
    "reviews",
    "review_detail",
    "insights",
];

#[must_use]
pub const fn mfg_route_section(route_id: app_mfg_contract::MfgRouteId) -> Option<&'static str> {
    use app_mfg_contract::MfgRouteId as R;
    match route_id {
        R::ContractGet => Some("contract"),
        R::AppGet => Some("app"),
        R::CommandCenterGet => Some("command_center"),
        R::DecisionTraceGet => Some("decision_trace"),
        R::IncidentList => Some("incidents"),
        R::IncidentGet => Some("incident_detail"),
        R::IncidentRoomGet => Some("incident_room"),
        R::AnalysisGet => Some("analysis"),
        R::ExecutionGet => Some("execution"),
        R::AlertRuleList => Some("alert_rules"),
        R::AlertList => Some("alerts"),
        R::AssignmentList => Some("assignments"),
        R::AssignmentGet => Some("assignment_detail"),
        R::ReportList => Some("reports"),
        R::ReportGet => Some("report_detail"),
        R::ReportDeliveryStateGet => Some("delivery_state"),
        R::ReportReviewList => Some("reviews"),
        R::ReportReviewGet => Some("review_detail"),
        R::LiveStream | R::LiveSnapshot => Some("live_stream"),
        R::RealityHealthGet
        | R::RealityDataPlaneHealthGet
        | R::RealityMetricList
        | R::RealityMetricGet
        | R::RealityMetricLineage
        | R::RealityAttentionHot
        | R::RealityEvidenceGet
        | R::RealityEvidenceContext
        | R::RealityQualityGateGet
        | R::SkillList
        | R::SkillGet
        | R::SkillRunGet
        | R::IncidentSkillRunList
        | R::ForecastList => Some("insights"),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgViewTab {
    #[default]
    Overview,
    Incidents,
    Alerts,
    Assignments,
    Reports,
    Reviews,
    Insights,
}

impl MfgViewTab {
    pub const ALL: [Self; 7] = [
        Self::Overview,
        Self::Incidents,
        Self::Alerts,
        Self::Assignments,
        Self::Reports,
        Self::Reviews,
        Self::Insights,
    ];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Incidents => "Incidents",
            Self::Alerts => "Alerts",
            Self::Assignments => "Assignments",
            Self::Reports => "Reports",
            Self::Reviews => "Reviews",
            Self::Insights => "Insights",
        }
    }

    #[must_use]
    pub const fn section_keys(self) -> &'static [&'static str] {
        match self {
            Self::Overview => &[
                "contract",
                "app",
                "command_center",
                "decision_trace",
                "live_stream",
            ],
            Self::Incidents => &[
                "incidents",
                "incident_detail",
                "incident_room",
                "analysis",
                "execution",
            ],
            Self::Alerts => &["alert_rules", "alerts"],
            Self::Assignments => &["assignments", "assignment_detail"],
            Self::Reports => &["reports", "report_detail", "delivery_state"],
            Self::Reviews => &["reviews", "review_detail"],
            Self::Insights => &["insights"],
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgViewFocus {
    #[default]
    Tabs,
    List,
    Detail,
    Backlinks,
    Actions,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgFreshness {
    #[default]
    Uninitialized,
    Refreshing,
    Fresh,
    Stale,
    Degraded,
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgConnectionStatus {
    #[default]
    Disconnected,
    Loading,
    ReadOnly,
    Operational,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgBacklinkKind {
    Runtime,
    Evidence,
    Approval,
    Surface,
}

impl MfgBacklinkKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Runtime => "Runtime",
            Self::Evidence => "Evidence",
            Self::Approval => "Approval",
            Self::Surface => "Surface",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgBacklink {
    pub kind: MfgBacklinkKind,
    pub target: String,
    pub label: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MfgItemSummary {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub status: String,
    pub severity: Option<String>,
    pub owner: Option<String>,
    pub sla: Option<String>,
    pub revision: Option<u64>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub backlinks: Vec<MfgBacklink>,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgPaginationState {
    pub cursor: Option<String>,
    pub next_cursor: Option<String>,
    pub loaded_count: usize,
    pub total_count: Option<usize>,
    pub limit: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MfgOperationsSnapshot {
    pub app_descriptor: Option<MfgReadResponseV1>,
    pub command_center: Option<MfgReadResponseV1>,
    #[serde(default)]
    pub incidents: Vec<MfgItemSummary>,
    pub incident_detail: Option<MfgReadResponseV1>,
    pub incident_detail_ref: Option<String>,
    pub incident_room: Option<MfgReadResponseV1>,
    pub analysis: Option<MfgReadResponseV1>,
    pub analysis_ref: Option<String>,
    pub decision_trace: Option<MfgReadResponseV1>,
    pub execution: Option<MfgReadResponseV1>,
    pub execution_ref: Option<String>,
    #[serde(default)]
    pub alert_rules: Vec<MfgItemSummary>,
    #[serde(default)]
    pub alerts: Vec<MfgItemSummary>,
    #[serde(default)]
    pub assignments: Vec<MfgItemSummary>,
    pub assignment_detail: Option<MfgReadResponseV1>,
    pub assignment_detail_ref: Option<String>,
    #[serde(default)]
    pub reports: Vec<MfgItemSummary>,
    pub report_detail: Option<MfgReadResponseV1>,
    pub report_detail_ref: Option<String>,
    pub delivery_state: Option<MfgReadResponseV1>,
    #[serde(default)]
    pub reviews: Vec<MfgItemSummary>,
    pub review_detail: Option<app_mfg_contract::MfgReportDeliveryReview>,
    pub review_detail_ref: Option<String>,
    #[serde(default)]
    pub p1_documents: BTreeMap<app_mfg_contract::MfgRouteId, MfgReadResponseV1>,
    #[serde(default)]
    pub insights: Vec<MfgItemSummary>,
    pub live_stream_available: bool,
    pub fetched_at: String,
    #[serde(default)]
    pub degraded_reasons: Vec<String>,
    #[serde(default)]
    pub pagination: BTreeMap<String, MfgPaginationState>,
    pub selection_revision: u64,
    #[serde(default)]
    pub granted_capabilities: Vec<String>,
    #[serde(default)]
    pub forbidden_sections: BTreeMap<String, String>,
    #[serde(default)]
    pub section_errors: BTreeMap<String, MfgApiErrorV1>,
    pub is_stale: bool,
    #[serde(default)]
    pub attempted_routes: BTreeSet<app_mfg_contract::MfgRouteId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgReadDelta {
    pub epoch: Option<String>,
    pub cursor: Option<String>,
    pub base_cursor: Option<String>,
    pub target_cursor: Option<String>,
    pub priority: u8,
    pub payload: Value,
}

impl Default for MfgReadDelta {
    fn default() -> Self {
        Self {
            epoch: None,
            cursor: None,
            base_cursor: None,
            target_cursor: None,
            priority: 3,
            payload: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgIntentStatus {
    Draft,
    AwaitingConfirmation,
    Ready,
    Submitting,
    Accepted,
    Replayed,
    Conflict,
    Forbidden,
    Failed,
    Cancelled,
}

impl MfgIntentStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::Replayed | Self::Conflict | Self::Forbidden | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgActionIntent {
    pub intent_id: String,
    pub action_id: app_mfg_contract::MfgActionId,
    pub route_id: app_mfg_contract::MfgRouteId,
    pub resource_ref: String,
    pub path_replacements: BTreeMap<String, String>,
    pub expected_revision: Option<u64>,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub payload_digest: String,
    pub request_body: Value,
    pub risk: app_mfg_contract::MfgActionRisk,
    pub confirmation: app_mfg_contract::MfgConfirmationKind,
    pub created_at: String,
    pub status: MfgIntentStatus,
    pub retryable: bool,
    pub last_error: Option<MfgApiErrorV1>,
    pub receipt: Option<MfgReceiptV1>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MfgActionSubmission {
    pub intent_id: String,
    pub action_id: app_mfg_contract::MfgActionId,
    pub route_id: app_mfg_contract::MfgRouteId,
    pub path_replacements: BTreeMap<String, String>,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub request_body: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgOperationsState {
    pub contract: Option<MfgFrontendContractV1>,
    pub command_center: Option<MfgReadResponseV1>,
    pub app_descriptor: Option<MfgReadResponseV1>,
    pub incidents: Vec<MfgItemSummary>,
    pub incident_detail: Option<MfgReadResponseV1>,
    pub incident_detail_ref: Option<String>,
    pub incident_room: Option<MfgReadResponseV1>,
    pub analysis: Option<MfgReadResponseV1>,
    pub analysis_ref: Option<String>,
    pub decision_trace: Option<MfgReadResponseV1>,
    pub executions: Option<MfgReadResponseV1>,
    pub execution_ref: Option<String>,
    pub alerts: Vec<MfgItemSummary>,
    pub alert_rules: Vec<MfgItemSummary>,
    pub assignments: Vec<MfgItemSummary>,
    pub assignment_detail: Option<MfgReadResponseV1>,
    pub assignment_detail_ref: Option<String>,
    pub reports: Vec<MfgItemSummary>,
    pub report_detail: Option<MfgReadResponseV1>,
    pub report_detail_ref: Option<String>,
    pub delivery_state: Option<MfgReadResponseV1>,
    pub reviews: Vec<MfgItemSummary>,
    pub review_detail: Option<app_mfg_contract::MfgReportDeliveryReview>,
    pub review_detail_ref: Option<String>,
    pub p1_documents: BTreeMap<app_mfg_contract::MfgRouteId, MfgReadResponseV1>,
    pub insights: Vec<MfgItemSummary>,
    pub receipts: Vec<MfgReceiptV1>,
    #[serde(default)]
    pub live_receipts: Vec<MfgReceiptV1>,
    #[serde(default)]
    pub latest_action_result: Option<Value>,
    pub selected_incident_id: Option<String>,
    pub selected_alert_id: Option<String>,
    pub selected_assignment_id: Option<String>,
    pub selected_report_id: Option<String>,
    pub selected_review_id: Option<String>,
    pub selected_insight_id: Option<String>,
    #[serde(default)]
    pub focused_evidence_ref: Option<String>,
    #[serde(default)]
    pub focused_quality_gate_id: Option<String>,
    pub pagination: BTreeMap<String, MfgPaginationState>,
    pub action_intents: Vec<MfgActionIntent>,
    pub selected_action_index: usize,
    pub live_epoch: Option<String>,
    pub live_cursor: Option<String>,
    #[serde(default)]
    pub live_generation: u64,
    #[serde(default)]
    pub live_reauthentication_count: u64,
    #[serde(default)]
    pub live_resync_url: Option<String>,
    pub freshness: MfgFreshness,
    pub connection: MfgConnectionStatus,
    pub last_updated_at: Option<String>,
    pub last_error: Option<MfgApiErrorV1>,
    pub recovery_actions: Vec<MfgRecoveryAction>,
    pub delta_queue: VecDeque<MfgReadDelta>,
    pub active_tab: MfgViewTab,
    pub focus: MfgViewFocus,
    pub list_scroll: usize,
    pub detail_scroll: usize,
    pub backlink_index: usize,
    pub generation: u64,
    pub applied_generation: u64,
    pub selection_revision: u64,
    pub refresh_requested: bool,
    pub refresh_in_flight: bool,
    pub live_stream_available: bool,
    pub degraded_reasons: Vec<String>,
    pub is_stale: bool,
    pub granted_capabilities: Vec<String>,
    pub forbidden_sections: BTreeMap<String, String>,
    pub section_errors: BTreeMap<String, MfgApiErrorV1>,
    pub attempted_routes: BTreeSet<app_mfg_contract::MfgRouteId>,
    pub last_backlink_intent: Option<MfgBacklink>,
    #[serde(skip)]
    pub runtime_strategy_cache: BTreeMap<String, MfgRuntimeStrategyProjection>,
    #[serde(default)]
    pending_runtime_backlink: Option<String>,
    #[serde(default)]
    pending_approval_backlink: Option<String>,
    #[serde(default)]
    pending_surface_receipt: Option<String>,
}

impl Default for MfgOperationsState {
    fn default() -> Self {
        Self {
            contract: None,
            command_center: None,
            app_descriptor: None,
            incidents: Vec::new(),
            incident_detail: None,
            incident_detail_ref: None,
            incident_room: None,
            analysis: None,
            analysis_ref: None,
            decision_trace: None,
            executions: None,
            execution_ref: None,
            alerts: Vec::new(),
            alert_rules: Vec::new(),
            assignments: Vec::new(),
            assignment_detail: None,
            assignment_detail_ref: None,
            reports: Vec::new(),
            report_detail: None,
            report_detail_ref: None,
            delivery_state: None,
            reviews: Vec::new(),
            review_detail: None,
            review_detail_ref: None,
            p1_documents: BTreeMap::new(),
            insights: Vec::new(),
            receipts: Vec::new(),
            live_receipts: Vec::new(),
            latest_action_result: None,
            selected_incident_id: None,
            selected_alert_id: None,
            selected_assignment_id: None,
            selected_report_id: None,
            selected_review_id: None,
            selected_insight_id: None,
            focused_evidence_ref: None,
            focused_quality_gate_id: None,
            pagination: BTreeMap::new(),
            action_intents: Vec::new(),
            selected_action_index: 0,
            live_epoch: None,
            live_cursor: None,
            live_generation: 0,
            live_reauthentication_count: 0,
            live_resync_url: None,
            freshness: MfgFreshness::Uninitialized,
            connection: MfgConnectionStatus::Disconnected,
            last_updated_at: None,
            last_error: None,
            recovery_actions: Vec::new(),
            delta_queue: VecDeque::new(),
            active_tab: MfgViewTab::Overview,
            focus: MfgViewFocus::Tabs,
            list_scroll: 0,
            detail_scroll: 0,
            backlink_index: 0,
            generation: 0,
            applied_generation: 0,
            selection_revision: 0,
            refresh_requested: true,
            refresh_in_flight: false,
            live_stream_available: false,
            degraded_reasons: Vec::new(),
            is_stale: false,
            granted_capabilities: Vec::new(),
            forbidden_sections: BTreeMap::new(),
            section_errors: BTreeMap::new(),
            attempted_routes: BTreeSet::new(),
            last_backlink_intent: None,
            runtime_strategy_cache: BTreeMap::new(),
            pending_runtime_backlink: None,
            pending_approval_backlink: None,
            pending_surface_receipt: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MfgRuntimeStrategyProjection {
    pub execution_id: String,
    pub strategy: harness_contract::projection::StrategyDecisionProjection,
    pub agents: Vec<harness_contract::projection::ProjectionEntity>,
    pub mfg_generation: u64,
    pub selection_revision: u64,
    pub live_generation: u64,
    pub live_epoch: Option<String>,
    pub live_reauthentication_count: u64,
}

impl MfgOperationsState {
    pub fn record_runtime_strategy_projection(
        &mut self,
        projection: &harness_contract::projection::ExecutionProjection,
        mfg_generation: u64,
        selection_revision: u64,
        live_generation: u64,
        live_epoch: Option<String>,
        live_reauthentication_count: u64,
    ) {
        self.runtime_strategy_cache.remove(&projection.execution_id);
        let Some(strategy) = projection.strategy.clone() else {
            return;
        };
        self.runtime_strategy_cache.insert(
            projection.execution_id.clone(),
            MfgRuntimeStrategyProjection {
                execution_id: projection.execution_id.clone(),
                strategy,
                agents: projection.agents.clone(),
                mfg_generation,
                selection_revision,
                live_generation,
                live_epoch,
                live_reauthentication_count,
            },
        );
    }

    pub fn invalidate_runtime_strategy_target(&mut self, target: &str) {
        if let Some(execution_id) = target
            .strip_prefix("runtime-execution://")
            .and_then(|target| target.split(['/', '?', '#']).next())
        {
            self.runtime_strategy_cache.remove(execution_id);
        }
    }

    #[must_use]
    pub fn accepts_runtime_backlink_result(
        &self,
        target: &str,
        mfg_generation: u64,
        selection_revision: u64,
        live_generation: u64,
        live_epoch: Option<&str>,
        live_reauthentication_count: u64,
    ) -> bool {
        self.generation == mfg_generation
            && self.selection_revision == selection_revision
            && self.live_generation == live_generation
            && self.live_epoch.as_deref() == live_epoch
            && self.live_reauthentication_count == live_reauthentication_count
            && self.selected_item().is_some_and(|item| {
                item.backlinks.iter().any(|backlink| {
                    backlink.kind == MfgBacklinkKind::Runtime && backlink.target == target
                })
            })
    }

    #[must_use]
    pub fn selected_runtime_strategy_projection(&self) -> Option<&MfgRuntimeStrategyProjection> {
        let execution_id = self
            .selected_item()?
            .backlinks
            .iter()
            .find(|backlink| {
                backlink.kind == MfgBacklinkKind::Runtime
                    && backlink.target.starts_with("runtime-execution://")
            })?
            .target
            .strip_prefix("runtime-execution://")?
            .split(['/', '?', '#'])
            .next()?;
        self.runtime_strategy_cache
            .get(execution_id)
            .filter(|projection| {
                projection.mfg_generation == self.generation
                    && projection.selection_revision == self.selection_revision
                    && projection.live_generation == self.live_generation
                    && projection.live_epoch == self.live_epoch
                    && projection.live_reauthentication_count == self.live_reauthentication_count
            })
    }

    pub fn request_refresh(&mut self) {
        self.refresh_requested = true;
    }

    pub fn take_refresh_request(&mut self) -> Option<u64> {
        if !self.refresh_requested || self.refresh_in_flight {
            return None;
        }
        self.refresh_requested = false;
        self.refresh_in_flight = true;
        self.attempted_routes.clear();
        self.runtime_strategy_cache.clear();
        self.generation = self.generation.saturating_add(1);
        self.freshness = MfgFreshness::Refreshing;
        self.connection = MfgConnectionStatus::Loading;
        Some(self.generation)
    }

    pub fn apply_contract(&mut self, generation: u64, contract: MfgFrontendContractV1) {
        if generation < self.generation || generation < self.applied_generation {
            return;
        }
        self.granted_capabilities = contract.granted_capabilities.clone();
        self.contract = Some(contract);
        self.attempted_routes
            .insert(app_mfg_contract::MfgRouteId::ContractGet);
        self.applied_generation = generation;
        self.last_error = None;
        self.recovery_actions.clear();
        self.is_stale = false;
    }

    pub fn apply_snapshot(&mut self, generation: u64, snapshot: MfgOperationsSnapshot) {
        if generation < self.generation || generation < self.applied_generation {
            return;
        }
        if snapshot.selection_revision != self.selection_revision {
            self.refresh_in_flight = false;
            return;
        }
        let previous_selected = self.selected_id().map(str::to_string);
        self.app_descriptor = snapshot.app_descriptor;
        self.command_center = snapshot.command_center;
        self.incidents = snapshot.incidents;
        self.incident_detail = snapshot.incident_detail;
        self.incident_detail_ref = snapshot.incident_detail_ref;
        self.incident_room = snapshot.incident_room;
        self.analysis = snapshot.analysis;
        self.analysis_ref = snapshot.analysis_ref;
        self.decision_trace = snapshot.decision_trace;
        self.executions = snapshot.execution;
        self.execution_ref = snapshot.execution_ref;
        self.alert_rules = snapshot.alert_rules;
        self.alerts = snapshot.alerts;
        self.assignments = snapshot.assignments;
        self.assignment_detail = snapshot.assignment_detail;
        self.assignment_detail_ref = snapshot.assignment_detail_ref;
        self.reports = snapshot.reports;
        self.report_detail = snapshot.report_detail;
        self.report_detail_ref = snapshot.report_detail_ref;
        self.delivery_state = snapshot.delivery_state;
        self.reviews = snapshot.reviews;
        self.review_detail = snapshot.review_detail;
        self.review_detail_ref = snapshot.review_detail_ref;
        self.p1_documents = snapshot.p1_documents;
        self.insights = snapshot.insights;
        self.live_stream_available = snapshot.live_stream_available;
        self.last_updated_at = Some(snapshot.fetched_at);
        self.degraded_reasons = snapshot.degraded_reasons;
        self.pagination = snapshot.pagination;
        self.granted_capabilities = snapshot.granted_capabilities;
        self.forbidden_sections = snapshot.forbidden_sections;
        self.section_errors = snapshot.section_errors;
        self.attempted_routes = snapshot.attempted_routes;
        self.enforce_access_recrop_from_errors();
        self.selected_incident_id =
            preserve_or_first(self.selected_incident_id.take(), &self.incidents);
        self.selected_alert_id = preserve_or_first(self.selected_alert_id.take(), &self.alerts);
        self.selected_assignment_id =
            preserve_or_first(self.selected_assignment_id.take(), &self.assignments);
        self.selected_report_id = preserve_or_first(self.selected_report_id.take(), &self.reports);
        self.selected_review_id = preserve_or_first(self.selected_review_id.take(), &self.reviews);
        self.selected_insight_id =
            preserve_or_first(self.selected_insight_id.take(), &self.insights);
        let selected = self.selected_id().map(str::to_string);
        let selected_index = selected
            .as_deref()
            .and_then(|id| self.current_items().iter().position(|item| item.id == id));
        self.list_scroll = selected_index.unwrap_or_default();
        if previous_selected != selected {
            self.detail_scroll = 0;
            self.backlink_index = 0;
        }
        self.applied_generation = generation;
        self.refresh_in_flight = false;
        self.last_error = self.section_errors.values().next().cloned();
        self.recovery_actions = self
            .section_errors
            .values()
            .flat_map(|error| error.recovery_actions.iter().cloned())
            .collect();
        self.is_stale = snapshot.is_stale;
        self.request_selected_runtime_strategy_projection();
        if self.degraded_reasons.is_empty() {
            self.freshness = MfgFreshness::Fresh;
            self.connection = if self.action_contracts().is_empty() {
                MfgConnectionStatus::ReadOnly
            } else {
                MfgConnectionStatus::Operational
            };
        } else {
            self.freshness = MfgFreshness::Degraded;
            self.connection = MfgConnectionStatus::Degraded;
        }
    }

    pub fn apply_error(&mut self, generation: u64, section: String, error: MfgApiErrorV1) {
        if generation < self.generation || generation < self.applied_generation {
            return;
        }
        self.applied_generation = generation;
        self.refresh_in_flight = false;
        self.recovery_actions = error.recovery_actions.clone();
        if section == "contract" {
            self.contract = None;
            self.live_generation = self.live_generation.saturating_add(1);
            self.live_stream_available = false;
        }
        match error.code {
            app_mfg_contract::MfgErrorCode::AuthenticationRequired => {
                self.clear_authorized_mfg_data();
                self.mark_all_mfg_sections_forbidden(&error.message);
            }
            app_mfg_contract::MfgErrorCode::CapabilityDenied => {
                if let Some(capability) = mfg_required_capability(&error) {
                    self.revoke_mfg_capability(capability, &error.message);
                } else {
                    self.forbidden_sections
                        .insert(section.clone(), error.message.clone());
                    self.clear_authorized_mfg_section(&section);
                }
            }
            _ => {}
        }
        if let Some(route_id) = mfg_route_for_section(&section) {
            self.attempted_routes.insert(route_id);
        }
        self.last_error = Some(error);
        if self.last_updated_at.is_some() {
            self.freshness = MfgFreshness::Stale;
            self.connection = MfgConnectionStatus::Degraded;
            self.is_stale = true;
        } else {
            self.freshness = MfgFreshness::Error;
            self.connection = MfgConnectionStatus::Failed;
            self.is_stale = false;
        }
    }

    pub fn begin_live_consumer(&mut self) -> u64 {
        self.live_generation = self.live_generation.saturating_add(1);
        self.live_stream_available = true;
        self.live_resync_url = None;
        self.live_generation
    }

    pub fn apply_live_envelope(
        &mut self,
        generation: u64,
        envelope: app_mfg_contract::MfgLiveEnvelopeV1,
    ) {
        if generation < self.live_generation {
            return;
        }
        self.live_generation = generation;
        match envelope {
            app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(snapshot) => {
                self.attempted_routes
                    .insert(app_mfg_contract::MfgRouteId::LiveSnapshot);
                self.attempted_routes
                    .insert(app_mfg_contract::MfgRouteId::LiveStream);
                self.live_epoch = Some(snapshot.view_epoch);
                self.live_cursor = Some(snapshot.cursor);
                self.live_resync_url = None;
                self.delta_queue.clear();
                self.apply_live_snapshot_state(snapshot.state);
                self.live_stream_available = true;
                self.last_error = None;
                self.recovery_actions.clear();
                self.connection = if self.action_contracts().is_empty() {
                    MfgConnectionStatus::ReadOnly
                } else {
                    MfgConnectionStatus::Operational
                };
                self.freshness = MfgFreshness::Fresh;
                self.last_updated_at = Some(snapshot.generated_at.to_rfc3339());
            }
            app_mfg_contract::MfgLiveEnvelopeV1::Delta(delta) => {
                self.attempted_routes
                    .insert(app_mfg_contract::MfgRouteId::LiveStream);
                if self.live_epoch.as_deref() != Some(delta.view_epoch.as_str())
                    || self.live_cursor.as_deref() != Some(delta.base_cursor.as_str())
                {
                    self.live_resync_url = Some("/api/apps/mfg/live/snapshot".to_string());
                    self.connection = MfgConnectionStatus::Degraded;
                    self.freshness = MfgFreshness::Stale;
                    return;
                }
                let priority = delta
                    .events
                    .iter()
                    .map(|event| {
                        app_mfg_contract::mfg_live_event_priority(&event.event_type, &event.payload)
                    })
                    .min()
                    .unwrap_or(3);
                for event in &delta.events {
                    self.apply_live_event(event);
                }
                self.live_cursor = Some(delta.target_cursor.clone());
                self.enqueue_delta(MfgReadDelta {
                    epoch: Some(delta.view_epoch),
                    cursor: Some(delta.target_cursor.clone()),
                    base_cursor: Some(delta.base_cursor),
                    target_cursor: Some(delta.target_cursor),
                    priority,
                    payload: serde_json::to_value(delta.events).unwrap_or_default(),
                });
                self.last_updated_at = Some(chrono::Utc::now().to_rfc3339());
                self.live_stream_available = true;
            }
            app_mfg_contract::MfgLiveEnvelopeV1::Heartbeat(heartbeat) => {
                if self.live_epoch.as_deref() != Some(heartbeat.view_epoch.as_str()) {
                    self.live_resync_url = Some("/api/apps/mfg/live/snapshot".to_string());
                    self.connection = MfgConnectionStatus::Degraded;
                    return;
                }
                self.live_cursor = Some(heartbeat.cursor);
                self.last_updated_at = Some(heartbeat.generated_at.to_rfc3339());
                self.live_stream_available = true;
            }
            app_mfg_contract::MfgLiveEnvelopeV1::Resync(resync) => {
                self.live_resync_url = Some(resync.snapshot_url);
                self.live_cursor = Some(resync.latest_cursor);
                self.live_epoch = None;
                self.connection = MfgConnectionStatus::Loading;
                self.freshness = MfgFreshness::Refreshing;
            }
        }
    }

    pub fn apply_live_error(&mut self, generation: u64, error: MfgApiErrorV1) {
        if generation < self.live_generation {
            return;
        }
        self.live_generation = generation;
        self.live_stream_available = false;
        let authorization_view_changed = matches!(
            error.details.get("reason").and_then(Value::as_str),
            Some("profile_revision_changed" | "credential_epoch_changed")
        );
        let authority_unavailable = matches!(
            error.details.get("reason").and_then(Value::as_str),
            Some("authority_unavailable")
        );
        let authorization_data_invalid = matches!(
            error.code,
            app_mfg_contract::MfgErrorCode::AuthenticationRequired
                | app_mfg_contract::MfgErrorCode::CapabilityDenied
        ) && !authority_unavailable;
        if authorization_data_invalid {
            // Never retain a projection cropped under an authorization view
            // that the live authority has rejected. The runner's next safe
            // reauthentication snapshot uses this incremented generation.
            self.clear_authorized_mfg_data();
        }
        if authorization_view_changed {
            self.live_reauthentication_count = self.live_reauthentication_count.saturating_add(1);
            // A fresh live snapshot recrops data, while the canonical contract
            // refresh recrops visible actions and capabilities on the same
            // authorization revision.
            self.request_refresh();
        }
        self.connection = MfgConnectionStatus::Degraded;
        self.freshness = MfgFreshness::Stale;
        self.recovery_actions = error.recovery_actions.clone();
        self.last_error = Some(error);
    }

    pub fn stop_live_consumer(&mut self, generation: u64) {
        if generation < self.live_generation {
            return;
        }
        self.live_generation = generation;
        self.live_stream_available = false;
    }

    pub fn enqueue_delta(&mut self, delta: MfgReadDelta) {
        if self.delta_queue.len() >= MFG_DELTA_QUEUE_LIMIT {
            if let Some(index) = self
                .delta_queue
                .iter()
                .position(|queued| queued.priority > delta.priority)
            {
                self.delta_queue.remove(index);
            } else if delta.priority <= 1 {
                self.delta_queue.clear();
                self.live_resync_url = Some("/api/apps/mfg/live/snapshot".to_string());
                self.connection = MfgConnectionStatus::Degraded;
                self.freshness = MfgFreshness::Stale;
            } else {
                self.delta_queue.pop_front();
            }
        }
        self.delta_queue.push_back(delta);
        while self.delta_queue.len() > MFG_DELTA_QUEUE_LIMIT {
            self.delta_queue.pop_front();
        }
    }

    fn apply_live_snapshot_state(&mut self, state: app_mfg_contract::MfgLiveSnapshotStateV1) {
        self.alert_rules = live_summary_list(&state.alerts, "rules", "alert_rule");
        self.alerts = live_summary_list(&state.alerts, "occurrences", "alert");
        self.assignments = live_summary_list(&state.assignments, "items", "assignment");
        self.incidents = live_summary_list(&state.incidents, "items", "incident");
        self.reports = live_summary_list(&state.reports, "items", "report");
        self.reviews = live_summary_list(&state.reviews, "items", "review");
        let mut executions = live_summary_list(&state.executions, "actions", "execution");
        executions.extend(live_summary_list(
            &state.cockpit,
            "profiles",
            "cockpit_profile",
        ));
        executions.extend(live_summary_list(
            &state.alerts,
            "subscriptions",
            "alert_subscription",
        ));
        executions.extend(live_summary_list(&state.executions, "skills", "skill_run"));
        executions.extend(live_summary_list(&state.incidents, "workflows", "workflow"));
        executions.extend(live_summary_list(&state.incidents, "analyses", "analysis"));
        executions.extend(live_summary_list(
            &state.incidents,
            "memory_cases",
            "memory_case",
        ));
        executions.extend(live_summary_list(&state.incidents, "playbooks", "playbook"));
        self.live_receipts = live_receipt_list(&state.receipts, "mutations");
        for (field, kind) in [
            ("entities", "entity"),
            ("relations", "relation"),
            ("facts", "fact"),
            ("attention", "attention"),
            ("evidence", "evidence"),
            ("quality_gates", "quality_gate"),
            ("metric_definitions", "metric_definition"),
            ("metric_dependencies", "metric_dependency"),
            ("metric_states", "metric_state"),
            ("metric_snapshots", "metric_snapshot"),
            ("watermarks", "watermark"),
            ("jobs", "compute_job"),
            ("changes", "metric_change"),
            ("source_packs", "source_pack"),
            ("connector_runs", "connector_run"),
            ("ontology_packs", "ontology"),
            ("entity_match_candidates", "entity_match_candidate"),
            ("entity_conflict_decisions", "entity_conflict_decision"),
        ] {
            executions.extend(live_summary_list(&state.data_compute, field, kind));
        }
        self.insights.retain(|item| {
            ![
                "execution",
                "cockpit_profile",
                "alert_subscription",
                "skill_run",
                "workflow",
                "analysis",
                "memory_case",
                "playbook",
                "receipt",
                "business_receipt",
                "entity",
                "relation",
                "fact",
                "attention",
                "evidence",
                "quality_gate",
                "metric_definition",
                "metric_dependency",
                "metric_state",
                "metric_snapshot",
                "watermark",
                "compute_job",
                "metric_change",
                "source_pack",
                "connector_run",
                "ontology",
                "entity_match_candidate",
                "entity_conflict_decision",
            ]
            .contains(&item.kind.as_str())
        });
        self.insights.extend(executions);
        self.preserve_live_selections();
    }

    fn apply_live_event(&mut self, event: &app_mfg_contract::MfgLiveEventV1) {
        for (field, kind) in [
            ("assignment", "assignment"),
            ("profile", "cockpit_profile"),
            ("occurrence", "alert"),
            ("rule", "alert_rule"),
            ("subscription", "alert_subscription"),
            ("incident", "incident"),
            ("report", "report"),
            ("review", "review"),
            ("execution", "execution"),
            ("skill_run", "skill_run"),
            ("workflow", "workflow"),
            ("analysis", "analysis"),
            ("memory_case", "memory_case"),
            ("playbook", "playbook"),
            ("receipt", "receipt"),
            ("entity", "entity"),
            ("relation", "relation"),
            ("fact", "fact"),
            ("attention", "attention"),
            ("evidence", "evidence"),
            ("quality_gate", "quality_gate"),
            ("metric_definition", "metric_definition"),
            ("metric_dependency", "metric_dependency"),
            ("metric_state", "metric_state"),
            ("metric_snapshot", "metric_snapshot"),
            ("watermark", "watermark"),
            ("job", "compute_job"),
            ("change", "metric_change"),
            ("source_pack", "source_pack"),
            ("connector_run", "connector_run"),
            ("ontology", "ontology"),
            ("entity_match_candidate", "entity_match_candidate"),
            ("entity_conflict_decision", "entity_conflict_decision"),
        ] {
            let Some(value) = exact_nested_value(&event.payload, field) else {
                continue;
            };
            if kind == "receipt" {
                if let Ok(receipt) =
                    serde_json::from_value::<app_mfg_contract::MfgReceiptV1>(value.clone())
                {
                    upsert_live_receipt(&mut self.live_receipts, receipt);
                }
                continue;
            }
            if let Some(summary) = live_item_summary(value, kind) {
                match kind {
                    "assignment" => upsert_live_summary(&mut self.assignments, summary),
                    "alert" => upsert_live_summary(&mut self.alerts, summary),
                    "alert_rule" => upsert_live_summary(&mut self.alert_rules, summary),
                    "incident" => upsert_live_summary(&mut self.incidents, summary),
                    "report" => upsert_live_summary(&mut self.reports, summary),
                    "review" => upsert_live_summary(&mut self.reviews, summary),
                    _ => upsert_live_summary(&mut self.insights, summary),
                }
            }
        }
        if event.event_type.ends_with(".deleted") {
            let id = event.subject_ref.rsplit(':').next().unwrap_or_default();
            for items in [
                &mut self.alert_rules,
                &mut self.alerts,
                &mut self.assignments,
                &mut self.incidents,
                &mut self.reports,
                &mut self.reviews,
                &mut self.insights,
            ] {
                items.retain(|item| item.id != id);
            }
        }
        self.preserve_live_selections();
    }

    fn preserve_live_selections(&mut self) {
        self.selected_incident_id =
            preserve_or_first(self.selected_incident_id.take(), &self.incidents);
        self.selected_alert_id = preserve_or_first(self.selected_alert_id.take(), &self.alerts);
        self.selected_assignment_id =
            preserve_or_first(self.selected_assignment_id.take(), &self.assignments);
        self.selected_report_id = preserve_or_first(self.selected_report_id.take(), &self.reports);
        self.selected_review_id = preserve_or_first(self.selected_review_id.take(), &self.reviews);
        self.selected_insight_id =
            preserve_or_first(self.selected_insight_id.take(), &self.insights);
    }

    fn clear_authorized_mfg_data(&mut self) {
        self.runtime_strategy_cache.clear();
        self.contract = None;
        self.command_center = None;
        self.app_descriptor = None;
        self.incidents.clear();
        self.incident_detail = None;
        self.incident_detail_ref = None;
        self.incident_room = None;
        self.analysis = None;
        self.analysis_ref = None;
        self.decision_trace = None;
        self.executions = None;
        self.execution_ref = None;
        self.alerts.clear();
        self.alert_rules.clear();
        self.assignments.clear();
        self.assignment_detail = None;
        self.assignment_detail_ref = None;
        self.reports.clear();
        self.report_detail = None;
        self.report_detail_ref = None;
        self.delivery_state = None;
        self.reviews.clear();
        self.review_detail = None;
        self.review_detail_ref = None;
        self.p1_documents.clear();
        self.insights.clear();
        self.receipts.clear();
        self.live_receipts.clear();
        self.latest_action_result = None;
        self.action_intents.clear();
        self.selected_action_index = 0;
        self.selected_incident_id = None;
        self.selected_alert_id = None;
        self.selected_assignment_id = None;
        self.selected_report_id = None;
        self.selected_review_id = None;
        self.selected_insight_id = None;
        self.focused_evidence_ref = None;
        self.focused_quality_gate_id = None;
        self.pagination.clear();
        self.granted_capabilities.clear();
        self.attempted_routes.clear();
        self.delta_queue.clear();
        self.live_epoch = None;
        self.live_cursor = None;
        self.live_generation = self.live_generation.saturating_add(1);
        self.live_resync_url = None;
        self.live_stream_available = false;
        self.list_scroll = 0;
        self.detail_scroll = 0;
        self.backlink_index = 0;
        self.last_backlink_intent = None;
        self.pending_runtime_backlink = None;
        self.pending_approval_backlink = None;
        self.pending_surface_receipt = None;
        self.degraded_reasons.clear();
        self.forbidden_sections.clear();
        self.section_errors.clear();
    }

    fn clear_authorized_mfg_section(&mut self, section: &str) {
        match section {
            "contract" => self.contract = None,
            "app" => self.app_descriptor = None,
            "command_center" => self.command_center = None,
            "decision_trace" => self.decision_trace = None,
            "live_stream" => {
                self.live_stream_available = false;
                self.live_epoch = None;
                self.live_cursor = None;
                self.live_generation = self.live_generation.saturating_add(1);
                self.live_resync_url = None;
            }
            "incidents" => {
                self.incidents.clear();
                self.selected_incident_id = None;
                self.pagination.remove("incidents");
            }
            "incident_detail" => {
                self.incident_detail = None;
                self.incident_detail_ref = None;
            }
            "incident_room" => {
                self.incident_room = None;
            }
            "analysis" => {
                self.analysis = None;
                self.analysis_ref = None;
            }
            "execution" => {
                self.executions = None;
                self.execution_ref = None;
            }
            "alert_rules" => {
                self.alert_rules.clear();
                self.pagination.remove("alert_rules");
            }
            "alerts" => {
                self.alerts.clear();
                self.selected_alert_id = None;
                self.pagination.remove("alerts");
            }
            "assignments" => {
                self.assignments.clear();
                self.selected_assignment_id = None;
                self.pagination.remove("assignments");
            }
            "assignment_detail" => {
                self.assignment_detail = None;
                self.assignment_detail_ref = None;
            }
            "reports" => {
                self.reports.clear();
                self.selected_report_id = None;
                self.pagination.remove("reports");
            }
            "report_detail" => {
                self.report_detail = None;
                self.report_detail_ref = None;
            }
            "delivery_state" => {
                self.delivery_state = None;
            }
            "reviews" => {
                self.reviews.clear();
                self.selected_review_id = None;
                self.pagination.remove("reviews");
            }
            "review_detail" => {
                self.review_detail = None;
                self.review_detail_ref = None;
            }
            "insights" => {
                self.p1_documents.clear();
                self.insights.clear();
                self.selected_insight_id = None;
                self.focused_evidence_ref = None;
                self.focused_quality_gate_id = None;
            }
            _ => {}
        }
        self.last_backlink_intent = None;
        self.list_scroll = 0;
        self.detail_scroll = 0;
        self.backlink_index = 0;
    }

    fn mark_all_mfg_sections_forbidden(&mut self, reason: &str) {
        for section in MFG_ALL_READ_SECTIONS {
            self.forbidden_sections
                .insert(section.to_string(), reason.to_string());
        }
    }

    fn revoke_mfg_capability(&mut self, capability: &str, reason: &str) {
        self.runtime_strategy_cache.clear();
        self.granted_capabilities
            .retain(|granted| granted != capability);
        let affected_actions = self
            .action_contracts()
            .into_iter()
            .filter(|action| {
                action
                    .required_capabilities
                    .iter()
                    .any(|required| required == capability)
            })
            .map(|action| action.action_id)
            .collect::<BTreeSet<_>>();
        for intent in &mut self.action_intents {
            if affected_actions.contains(&intent.action_id)
                && matches!(
                    intent.status,
                    MfgIntentStatus::Draft
                        | MfgIntentStatus::AwaitingConfirmation
                        | MfgIntentStatus::Ready
                        | MfgIntentStatus::Submitting
                )
            {
                intent.status = MfgIntentStatus::Forbidden;
                intent.retryable = false;
                intent.last_error = Some(app_mfg_contract::MfgApiErrorV1::capability_denied(
                    capability,
                ));
            }
        }
        for route in app_mfg_contract::mfg_tui_read_route_contracts() {
            if !mfg_route_requires_capability(&route.capability, capability) {
                continue;
            }
            if let Some(section) = mfg_route_section(route.route_id) {
                self.forbidden_sections
                    .insert(section.to_string(), reason.to_string());
                self.clear_authorized_mfg_section(section);
            }
        }
    }

    fn enforce_access_recrop_from_errors(&mut self) {
        let errors = self.section_errors.values().cloned().collect::<Vec<_>>();
        let attempted_routes = self.attempted_routes.clone();
        for error in errors {
            match error.code {
                app_mfg_contract::MfgErrorCode::AuthenticationRequired => {
                    self.clear_authorized_mfg_data();
                    self.mark_all_mfg_sections_forbidden(&error.message);
                }
                app_mfg_contract::MfgErrorCode::CapabilityDenied => {
                    if let Some(capability) = mfg_required_capability(&error) {
                        self.revoke_mfg_capability(capability, &error.message);
                    }
                }
                _ => {}
            }
        }
        self.attempted_routes = attempted_routes;
    }

    fn enforce_access_recrop_error(&mut self, error: &MfgApiErrorV1) {
        match error.code {
            app_mfg_contract::MfgErrorCode::AuthenticationRequired => {
                self.clear_authorized_mfg_data();
                self.mark_all_mfg_sections_forbidden(&error.message);
            }
            app_mfg_contract::MfgErrorCode::CapabilityDenied => {
                if let Some(capability) = mfg_required_capability(error) {
                    self.revoke_mfg_capability(capability, &error.message);
                }
            }
            _ => {}
        }
    }

    pub fn select_tab(&mut self, tab: MfgViewTab) {
        self.active_tab = tab;
        self.focused_evidence_ref = None;
        self.selected_action_index = 0;
        self.list_scroll = 0;
        self.detail_scroll = 0;
        self.backlink_index = 0;
        self.selection_revision = self.selection_revision.saturating_add(1);
        self.request_selected_runtime_strategy_projection();
        self.request_refresh();
    }

    pub fn cycle_tab(&mut self, backwards: bool) {
        let current = MfgViewTab::ALL
            .iter()
            .position(|tab| *tab == self.active_tab)
            .unwrap_or(0);
        let next = if backwards {
            current
                .checked_sub(1)
                .unwrap_or(MfgViewTab::ALL.len().saturating_sub(1))
        } else {
            (current + 1) % MfgViewTab::ALL.len()
        };
        self.select_tab(MfgViewTab::ALL[next]);
    }

    #[must_use]
    pub fn current_items(&self) -> &[MfgItemSummary] {
        match self.active_tab {
            MfgViewTab::Overview => &[],
            MfgViewTab::Incidents => &self.incidents,
            MfgViewTab::Alerts => &self.alerts,
            MfgViewTab::Assignments => &self.assignments,
            MfgViewTab::Reports => &self.reports,
            MfgViewTab::Reviews => &self.reviews,
            MfgViewTab::Insights => &self.insights,
        }
    }

    #[must_use]
    pub fn selected_id(&self) -> Option<&str> {
        match self.active_tab {
            MfgViewTab::Overview => None,
            MfgViewTab::Incidents => self.selected_incident_id.as_deref(),
            MfgViewTab::Alerts => self.selected_alert_id.as_deref(),
            MfgViewTab::Assignments => self.selected_assignment_id.as_deref(),
            MfgViewTab::Reports => self.selected_report_id.as_deref(),
            MfgViewTab::Reviews => self.selected_review_id.as_deref(),
            MfgViewTab::Insights => self.selected_insight_id.as_deref(),
        }
    }

    #[must_use]
    pub fn selected_item(&self) -> Option<&MfgItemSummary> {
        let selected_id = self.selected_id()?;
        self.current_items()
            .iter()
            .find(|item| item.id == selected_id)
    }

    pub fn move_selection(&mut self, down: bool) {
        let items = self.current_items();
        if items.is_empty() {
            return;
        }
        let current = self
            .selected_id()
            .and_then(|id| items.iter().position(|item| item.id == id))
            .unwrap_or(0);
        let next = if down {
            (current + 1).min(items.len().saturating_sub(1))
        } else {
            current.saturating_sub(1)
        };
        let selected = items[next].id.clone();
        match self.active_tab {
            MfgViewTab::Overview => {}
            MfgViewTab::Incidents => self.selected_incident_id = Some(selected),
            MfgViewTab::Alerts => self.selected_alert_id = Some(selected),
            MfgViewTab::Assignments => self.selected_assignment_id = Some(selected),
            MfgViewTab::Reports => self.selected_report_id = Some(selected),
            MfgViewTab::Reviews => self.selected_review_id = Some(selected),
            MfgViewTab::Insights => self.selected_insight_id = Some(selected),
        }
        self.list_scroll = next;
        self.detail_scroll = 0;
        self.backlink_index = 0;
        self.selection_revision = self.selection_revision.saturating_add(1);
        self.request_selected_runtime_strategy_projection();
        self.request_refresh();
    }

    pub fn activate_backlink(&mut self, kind: MfgBacklinkKind) -> Option<MfgBacklink> {
        let links = &self.selected_item()?.backlinks;
        let backlink = if kind == MfgBacklinkKind::Runtime {
            links
                .iter()
                .find(|link| {
                    link.kind == MfgBacklinkKind::Runtime
                        && link.target.starts_with("runtime-execution://")
                })
                .or_else(|| links.iter().find(|link| link.kind == kind))
                .cloned()?
        } else {
            links.iter().find(|link| link.kind == kind).cloned()?
        };
        self.last_backlink_intent = Some(backlink.clone());
        Some(backlink)
    }

    pub fn focus_evidence_backlink(&mut self, target: &str) {
        let evidence_ref = target
            .trim()
            .trim_start_matches("evidence://matrix/")
            .trim_start_matches("evidence://")
            .trim_start_matches("mfg:evidence:")
            .trim_start_matches("evidence:")
            .split(['?', '#'])
            .next()
            .unwrap_or_default()
            .trim();
        if evidence_ref.is_empty() {
            return;
        }
        self.active_tab = MfgViewTab::Insights;
        self.focus = MfgViewFocus::Detail;
        self.focused_evidence_ref = Some(evidence_ref.to_string());
        self.detail_scroll = 0;
        self.selection_revision = self.selection_revision.saturating_add(1);
        self.request_refresh();
    }

    pub fn request_runtime_backlink(&mut self, target: &str) {
        let target = target.trim();
        if target.starts_with("mfg-execution://")
            || target.starts_with("runtime-execution://")
            || target.starts_with("task://")
        {
            self.pending_runtime_backlink = Some(target.to_string());
        }
    }

    /// Selecting an MFG object that owns a canonical Runtime graph must fetch
    /// that graph without requiring an extra keyboard action.  MFG-local
    /// execution and task backlinks remain available for drill-down, but they
    /// are not strategy projections and therefore never populate this cache.
    pub fn request_selected_runtime_strategy_projection(&mut self) {
        let Some(target) = self.selected_item().and_then(|item| {
            item.backlinks
                .iter()
                .find(|backlink| {
                    backlink.kind == MfgBacklinkKind::Runtime
                        && backlink.target.starts_with("runtime-execution://")
                })
                .map(|backlink| backlink.target.clone())
        }) else {
            return;
        };
        let cached = target
            .strip_prefix("runtime-execution://")
            .and_then(|target| target.split(['/', '?', '#']).next())
            .and_then(|execution_id| self.runtime_strategy_cache.get(execution_id))
            .is_some_and(|projection| {
                projection.mfg_generation == self.generation
                    && projection.selection_revision == self.selection_revision
                    && projection.live_generation == self.live_generation
                    && projection.live_epoch == self.live_epoch
                    && projection.live_reauthentication_count == self.live_reauthentication_count
            });
        if !cached {
            self.pending_runtime_backlink = Some(target);
        }
    }

    pub fn take_runtime_backlink_request(&mut self) -> Option<String> {
        self.pending_runtime_backlink.take()
    }

    pub fn request_approval_backlink(&mut self, target: &str) {
        let target = target.trim();
        if target.starts_with("approval://") {
            self.pending_approval_backlink = Some(target.to_string());
        }
    }

    pub fn take_approval_backlink_request(&mut self) -> Option<String> {
        self.pending_approval_backlink.take()
    }

    pub fn request_surface_receipt(&mut self, target: &str) {
        let target = target.trim();
        if target.starts_with("receipt://cross-plane/") || target.starts_with("surface://") {
            self.pending_surface_receipt = Some(target.to_string());
        }
    }

    pub fn take_surface_receipt_request(&mut self) -> Option<String> {
        self.pending_surface_receipt.take()
    }

    pub fn adjust_page_limit(&mut self, increase: bool) -> bool {
        let key = match self.active_tab {
            MfgViewTab::Overview => return false,
            MfgViewTab::Incidents => "incidents",
            MfgViewTab::Alerts => "alerts",
            MfgViewTab::Assignments => "assignments",
            MfgViewTab::Reports => "reports",
            MfgViewTab::Reviews => "reviews",
            MfgViewTab::Insights => return false,
        };
        let pagination = self.pagination.entry(key.to_string()).or_default();
        let current = pagination.limit.max(50);
        let next = if increase {
            current.saturating_add(50).min(500)
        } else {
            current.saturating_sub(50).max(50)
        };
        if next == current {
            return false;
        }
        pagination.limit = next;
        self.request_refresh();
        true
    }

    #[must_use]
    pub fn action_contracts(&self) -> Vec<app_mfg_contract::MfgActionContract> {
        let Some(surface) = self.contract.as_ref().and_then(|contract| {
            contract
                .surfaces
                .iter()
                .find(|surface| surface.surface == app_mfg_contract::MfgSurfaceKind::Tui)
        }) else {
            return Vec::new();
        };
        let allowed = surface.actions.iter().copied().collect::<BTreeSet<_>>();
        app_mfg_contract::mfg_tui_action_contracts()
            .into_iter()
            .filter(|action| allowed.contains(&action.action_id))
            .collect()
    }

    #[must_use]
    pub fn selected_action_contract(&self) -> Option<app_mfg_contract::MfgActionContract> {
        let actions = self.visible_action_contracts();
        actions
            .get(
                self.selected_action_index
                    .min(actions.len().saturating_sub(1)),
            )
            .cloned()
    }

    pub fn move_action_selection(&mut self, down: bool) {
        let len = self.visible_action_contracts().len();
        if len == 0 {
            self.selected_action_index = 0;
            return;
        }
        self.selected_action_index = if down {
            (self.selected_action_index + 1).min(len.saturating_sub(1))
        } else {
            self.selected_action_index.saturating_sub(1)
        };
    }

    #[must_use]
    pub fn visible_action_contracts(&self) -> Vec<app_mfg_contract::MfgActionContract> {
        use app_mfg_contract::MfgRouteId as R;
        self.action_contracts()
            .into_iter()
            .filter(|action| self.action_is_capability_enabled(action))
            .filter(|action| match self.active_tab {
                MfgViewTab::Overview => matches!(
                    action.route_id,
                    R::IncidentCreate | R::AssignmentUpsert | R::ReportGenerate
                ),
                MfgViewTab::Incidents => matches!(
                    action.route_id,
                    R::IncidentCreate
                        | R::IncidentAnalyze
                        | R::AnalysisActionExecute
                        | R::ExecutionFeedbackCreate
                        | R::IncidentPlaybookRecommend
                        | R::IncidentSkillPlan
                        | R::IncidentSkillRun
                ),
                MfgViewTab::Alerts => action.route_id == R::AlertCommand,
                MfgViewTab::Assignments => {
                    matches!(action.route_id, R::AssignmentUpsert | R::AssignmentCommand)
                }
                MfgViewTab::Reports => matches!(
                    action.route_id,
                    R::ReportGenerate
                        | R::ReportDeliver
                        | R::ReportDeliveryRetry
                        | R::ReportReviewRequest
                ),
                MfgViewTab::Reviews => action.route_id == R::ReportReviewDecide,
                MfgViewTab::Insights => action.route_id == R::RealityEvidenceQualityGate,
            })
            .collect()
    }

    #[must_use]
    pub fn action_is_capability_enabled(
        &self,
        action: &app_mfg_contract::MfgActionContract,
    ) -> bool {
        action.required_capabilities.iter().all(|required| {
            self.granted_capabilities
                .iter()
                .any(|granted| granted == required)
        })
    }

    #[must_use]
    pub fn action_usability_label(
        &self,
        action: &app_mfg_contract::MfgActionContract,
    ) -> &'static str {
        if !self.action_is_capability_enabled(action) {
            return "denied";
        }
        if mfg_action_requires_explicit_input(action.action_id) {
            return if self.action_input_command(action.action_id).is_ok() {
                "input-required"
            } else {
                "blocked"
            };
        }
        if build_mfg_action_draft(self, action, None).is_ok() {
            "enabled"
        } else {
            "blocked"
        }
    }

    pub fn action_input_command(
        &self,
        action_id: app_mfg_contract::MfgActionId,
    ) -> Result<String, String> {
        let action = self
            .action_contracts()
            .into_iter()
            .find(|candidate| candidate.action_id == action_id)
            .ok_or_else(|| {
                format!(
                    "{} is not exposed by the current TUI contract",
                    action_id.as_str()
                )
            })?;
        if !self.action_is_capability_enabled(&action) {
            return Err(format!(
                "{} requires capabilities: {}",
                action.action_id.as_str(),
                action.required_capabilities.join(",")
            ));
        }
        let payload = mfg_action_input_template(self, &action)?;
        Ok(format!(
            "/mfg action {} {}",
            action.action_id.as_str(),
            serde_json::to_string(&payload).map_err(|error| error.to_string())?
        ))
    }

    pub fn prepare_selected_action(
        &mut self,
        override_payload: Option<Value>,
    ) -> Result<String, String> {
        let action = self
            .selected_action_contract()
            .ok_or_else(|| "No MFG action is available in the current contract.".to_string())?;
        self.prepare_action(action.action_id, override_payload)
    }

    pub fn prepare_action(
        &mut self,
        action_id: app_mfg_contract::MfgActionId,
        override_payload: Option<Value>,
    ) -> Result<String, String> {
        let action = self
            .action_contracts()
            .into_iter()
            .find(|candidate| candidate.action_id == action_id)
            .ok_or_else(|| {
                format!(
                    "{} is not exposed by the current TUI contract",
                    action_id.as_str()
                )
            })?;
        if !self.action_is_capability_enabled(&action) {
            return Err(format!(
                "{} requires capabilities: {}",
                action.action_id.as_str(),
                action.required_capabilities.join(",")
            ));
        }
        let candidate_intent_id = format!("mfg-intent-{}", uuid::Uuid::new_v4());
        let candidate_idempotency_key = format!("tui:{candidate_intent_id}");
        let mut draft = build_mfg_action_draft(self, &action, override_payload)?;
        if let Some(existing) = self.action_intents.iter_mut().rev().find(|intent| {
            let generated_create_target = match action.action_id {
                app_mfg_contract::MfgActionId::Route(
                    app_mfg_contract::MfgRouteId::IncidentCreate,
                ) => {
                    draft.resource_ref == "mfg:incident:new"
                        && intent.resource_ref.starts_with("mfg:incident:incident-")
                }
                app_mfg_contract::MfgActionId::Multi(
                    app_mfg_contract::MfgMultiActionId::AssignmentCreate,
                ) => {
                    draft.resource_ref == "mfg:assignment:new"
                        && intent
                            .resource_ref
                            .starts_with("mfg:assignment:assignment-")
                }
                _ => false,
            };
            intent.action_id == action.action_id
                && intent.path_replacements == draft.path_replacements
                && (intent.resource_ref == draft.resource_ref || generated_create_target)
                && intent.payload_digest == draft.payload_digest
                && intent.retryable
                && intent.status == MfgIntentStatus::Failed
        }) {
            existing.status =
                if existing.confirmation == app_mfg_contract::MfgConfirmationKind::None {
                    MfgIntentStatus::Ready
                } else {
                    MfgIntentStatus::AwaitingConfirmation
                };
            existing.last_error = None;
            return Ok(existing.intent_id.clone());
        }
        if action.action_id
            == app_mfg_contract::MfgActionId::Route(app_mfg_contract::MfgRouteId::IncidentCreate)
            && draft.resource_ref == "mfg:incident:new"
        {
            draft.resource_ref = format!(
                "mfg:incident:{}",
                stable_tui_mfg_resource_id("incident", &candidate_idempotency_key)
            );
        }
        if action.action_id
            == app_mfg_contract::MfgActionId::Multi(
                app_mfg_contract::MfgMultiActionId::AssignmentCreate,
            )
            && draft.resource_ref == "mfg:assignment:new"
        {
            draft.resource_ref = format!(
                "mfg:assignment:{}",
                stable_tui_mfg_resource_id("assignment", &candidate_idempotency_key)
            );
        }
        let intent_id = candidate_intent_id;
        let status = if action.confirmation == app_mfg_contract::MfgConfirmationKind::None {
            MfgIntentStatus::Ready
        } else {
            MfgIntentStatus::AwaitingConfirmation
        };
        self.action_intents.push(MfgActionIntent {
            idempotency_key: candidate_idempotency_key,
            correlation_id: format!("mfg-correlation:{intent_id}"),
            intent_id: intent_id.clone(),
            action_id: action.action_id,
            route_id: action.route_id,
            resource_ref: draft.resource_ref,
            path_replacements: draft.path_replacements,
            expected_revision: draft.expected_revision,
            payload_digest: draft.payload_digest,
            request_body: draft.request_body,
            risk: action.risk,
            confirmation: action.confirmation,
            created_at: chrono::Utc::now().to_rfc3339(),
            status,
            retryable: false,
            last_error: None,
            receipt: None,
        });
        Ok(intent_id)
    }

    pub fn confirm_pending_action(&mut self) -> Result<String, String> {
        let index = self
            .action_intents
            .iter_mut()
            .rposition(|intent| intent.status == MfgIntentStatus::AwaitingConfirmation)
            .ok_or_else(|| "No MFG action is awaiting confirmation.".to_string())?;
        let action_id = self.action_intents[index].action_id;
        let action = self
            .action_contracts()
            .into_iter()
            .find(|action| action.action_id == action_id)
            .ok_or_else(|| "The pending MFG action is no longer in the contract.".to_string())?;
        if !self.action_is_capability_enabled(&action) {
            self.action_intents[index].status = MfgIntentStatus::Forbidden;
            return Err(format!(
                "{} is no longer permitted; refresh the MFG contract",
                action_id.as_str()
            ));
        }
        let intent = &mut self.action_intents[index];
        intent.status = MfgIntentStatus::Ready;
        Ok(intent.intent_id.clone())
    }

    pub fn cancel_pending_action(&mut self) -> Result<String, String> {
        let intent = self
            .action_intents
            .iter_mut()
            .rev()
            .find(|intent| {
                matches!(
                    intent.status,
                    MfgIntentStatus::Draft
                        | MfgIntentStatus::AwaitingConfirmation
                        | MfgIntentStatus::Ready
                )
            })
            .ok_or_else(|| "No cancellable MFG action intent exists.".to_string())?;
        intent.status = MfgIntentStatus::Cancelled;
        Ok(intent.intent_id.clone())
    }

    pub fn retry_failed_action(&mut self) -> Result<String, String> {
        let index = self
            .action_intents
            .iter_mut()
            .rposition(|intent| intent.status == MfgIntentStatus::Failed && intent.retryable)
            .ok_or_else(|| "No retryable MFG action intent exists.".to_string())?;
        let action_id = self.action_intents[index].action_id;
        let action = self
            .action_contracts()
            .into_iter()
            .find(|action| action.action_id == action_id)
            .ok_or_else(|| "The failed MFG action is no longer in the contract.".to_string())?;
        if !self.action_is_capability_enabled(&action) {
            self.action_intents[index].status = MfgIntentStatus::Forbidden;
            self.action_intents[index].retryable = false;
            return Err(format!(
                "{} is no longer permitted; the old intent cannot be retried",
                action_id.as_str()
            ));
        }
        let intent = &mut self.action_intents[index];
        intent.status = if intent.confirmation == app_mfg_contract::MfgConfirmationKind::None {
            MfgIntentStatus::Ready
        } else {
            MfgIntentStatus::AwaitingConfirmation
        };
        intent.last_error = None;
        Ok(intent.intent_id.clone())
    }

    pub fn take_action_submission(&mut self) -> Option<MfgActionSubmission> {
        let index = self
            .action_intents
            .iter_mut()
            .position(|intent| intent.status == MfgIntentStatus::Ready)?;
        let action_id = self.action_intents[index].action_id;
        let permitted = self
            .action_contracts()
            .into_iter()
            .find(|action| action.action_id == action_id)
            .is_some_and(|action| self.action_is_capability_enabled(&action));
        if !permitted {
            self.action_intents[index].status = MfgIntentStatus::Forbidden;
            self.action_intents[index].retryable = false;
            return None;
        }
        let intent = &mut self.action_intents[index];
        intent.status = MfgIntentStatus::Submitting;
        Some(MfgActionSubmission {
            intent_id: intent.intent_id.clone(),
            action_id: intent.action_id,
            route_id: intent.route_id,
            path_replacements: intent.path_replacements.clone(),
            idempotency_key: intent.idempotency_key.clone(),
            correlation_id: intent.correlation_id.clone(),
            request_body: intent.request_body.clone(),
        })
    }

    pub fn apply_action_success(
        &mut self,
        intent_id: &str,
        response: app_mfg_contract::MfgMutationResponseV1,
    ) {
        let response_payload = Value::Object(response.payload.clone().into_iter().collect());
        let action_identity = self
            .action_intents
            .iter()
            .find(|intent| intent.intent_id == intent_id)
            .map(|intent| {
                (
                    intent.action_id.as_str().to_string(),
                    intent.resource_ref.clone(),
                )
            });
        let quality_gate_id = self
            .action_intents
            .iter()
            .find(|intent| {
                intent.intent_id == intent_id
                    && intent.route_id == app_mfg_contract::MfgRouteId::RealityEvidenceQualityGate
            })
            .and_then(|_| find_string_recursive_local(&response_payload, "gate_id"));
        let receipt = response.middleware_receipt.clone();
        let identity_error = self
            .action_intents
            .iter()
            .find(|intent| intent.intent_id == intent_id)
            .and_then(|intent| match receipt.as_ref() {
                Some(receipt)
                    if receipt.action_id == intent.action_id
                        && receipt.idempotency_key == intent.idempotency_key
                        && receipt.correlation_id.as_deref()
                            == Some(intent.correlation_id.as_str())
                        && receipt.resource_ref == intent.resource_ref
                        && receipt.payload_digest == intent.payload_digest
                        && receipt.expected_revision == intent.expected_revision =>
                {
                    None
                }
                receipt => Some(MfgApiErrorV1 {
                    code: app_mfg_contract::MfgErrorCode::ContractMismatch,
                    message: receipt.map_or_else(
                        || format!("MFG response omitted the governed receipt for {intent_id}"),
                        |receipt| {
                            format!(
                                "MFG receipt identity mismatch for intent {intent_id}: action={}, key={}, correlation={}, resource={}, digest={}, revision={:?}",
                                receipt.action_id.as_str(),
                                receipt.idempotency_key,
                                receipt.correlation_id.as_deref().unwrap_or("none"),
                                receipt.resource_ref,
                                receipt.payload_digest,
                                receipt.expected_revision,
                            )
                        },
                    ),
                    http_status: 409,
                    details: serde_json::json!({
                        "expected_action_id": intent.action_id.as_str(),
                        "expected_idempotency_key": intent.idempotency_key.clone(),
                        "expected_correlation_id": intent.correlation_id.clone(),
                        "expected_resource_ref": intent.resource_ref.clone(),
                        "expected_payload_digest": intent.payload_digest.clone(),
                        "expected_revision": intent.expected_revision,
                        "receipt_id": receipt.map(|receipt| receipt.receipt_id.clone()),
                    }),
                    retryable: false,
                    contract_version: app_mfg_contract::MfgContractVersion::default(),
                    recovery_actions: vec![MfgRecoveryAction {
                        kind: app_mfg_contract::MfgRecoveryActionKind::Reload,
                        label: "Reload the MFG contract and projection".to_string(),
                        target: Some("/mfg".to_string()),
                        enabled: true,
                    }],
                    request_id: None,
                    receipt_ref: receipt.map(|receipt| receipt.receipt_id.clone()),
                }),
            });
        if let Some(error) = identity_error {
            self.apply_action_error(intent_id, error);
            return;
        }
        if let Some(receipt) = receipt.as_ref() {
            self.receipts.insert(0, receipt.clone());
            self.receipts.truncate(20);
        }
        if let Some((action_id, resource_ref)) = action_identity {
            self.latest_action_result = Some(serde_json::json!({
                "intent_id": intent_id,
                "action_id": action_id,
                "resource_ref": resource_ref,
                "payload": response_payload,
                "receipt_response": receipt.as_ref().map(|receipt| receipt.response.clone()),
            }));
        }
        if let Some(gate_id) = quality_gate_id {
            self.focused_quality_gate_id = Some(gate_id);
        }
        if let Some(intent) = self
            .action_intents
            .iter_mut()
            .find(|intent| intent.intent_id == intent_id)
        {
            intent.status = if receipt.as_ref().is_some_and(|receipt| {
                receipt.status == app_mfg_contract::MfgReceiptStatus::Replayed
            }) {
                MfgIntentStatus::Replayed
            } else {
                MfgIntentStatus::Accepted
            };
            intent.receipt = receipt;
            intent.retryable = false;
            intent.last_error = None;
        }
        self.request_refresh();
    }

    pub fn apply_action_error(&mut self, intent_id: &str, error: MfgApiErrorV1) {
        if let Some(intent) = self
            .action_intents
            .iter_mut()
            .find(|intent| intent.intent_id == intent_id)
        {
            intent.status = match error.code {
                app_mfg_contract::MfgErrorCode::RevisionConflict
                | app_mfg_contract::MfgErrorCode::IdempotencyConflict => MfgIntentStatus::Conflict,
                app_mfg_contract::MfgErrorCode::AuthenticationRequired
                | app_mfg_contract::MfgErrorCode::CapabilityDenied => MfgIntentStatus::Forbidden,
                _ => MfgIntentStatus::Failed,
            };
            intent.retryable = error.retryable;
            intent.last_error = Some(error.clone());
        }
        self.last_error = Some(error.clone());
        self.recovery_actions = error.recovery_actions.clone();
        if matches!(
            error.code,
            app_mfg_contract::MfgErrorCode::AuthenticationRequired
                | app_mfg_contract::MfgErrorCode::CapabilityDenied
        ) {
            self.enforce_access_recrop_error(&error);
            self.request_refresh();
        }
    }

    #[must_use]
    pub fn pending_mutation_count(&self) -> usize {
        self.action_intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.status,
                    MfgIntentStatus::Draft
                        | MfgIntentStatus::AwaitingConfirmation
                        | MfgIntentStatus::Ready
                        | MfgIntentStatus::Submitting
                ) || (intent.status == MfgIntentStatus::Failed && intent.retryable)
            })
            .count()
    }

    #[must_use]
    pub fn latest_action_intent(&self) -> Option<&MfgActionIntent> {
        self.action_intents.last()
    }

    #[must_use]
    pub fn active_tab_forbidden(&self) -> Option<(&'static str, &str)> {
        self.active_tab.section_keys().iter().find_map(|section| {
            self.forbidden_sections
                .get(*section)
                .map(|reason| (*section, reason.as_str()))
        })
    }

    #[must_use]
    pub fn route_projection_status(
        &self,
        route_id: app_mfg_contract::MfgRouteId,
    ) -> Option<&'static str> {
        use app_mfg_contract::MfgRouteId as R;
        let section = mfg_route_section(route_id)?;
        if self.forbidden_sections.contains_key(section) {
            return Some("forbidden");
        }
        if self.section_errors.contains_key(section) {
            return Some("error");
        }
        if !matches!(route_id, R::LiveStream | R::LiveSnapshot)
            && !self.attempted_routes.contains(&route_id)
        {
            return Some("not-requested");
        }
        let status = |loaded: bool, selected: bool| {
            if loaded {
                "visible"
            } else if !selected {
                "awaiting-selection"
            } else {
                "pending"
            }
        };
        Some(match route_id {
            R::ContractGet => status(self.contract.is_some(), true),
            R::AppGet => status(self.app_descriptor.is_some(), true),
            R::CommandCenterGet => status(self.command_center.is_some(), true),
            R::DecisionTraceGet => status(self.decision_trace.is_some(), true),
            R::IncidentList => status(self.pagination.contains_key("incidents"), true),
            R::IncidentGet => status(
                self.incident_detail.is_some(),
                self.selected_incident_id.is_some(),
            ),
            R::IncidentRoomGet => status(
                self.incident_room.is_some(),
                self.selected_incident_id.is_some(),
            ),
            R::AnalysisGet => status(self.analysis.is_some(), self.selected_incident_id.is_some()),
            R::ExecutionGet => status(
                self.executions.is_some(),
                self.selected_incident_id.is_some(),
            ),
            R::AlertRuleList => status(self.pagination.contains_key("alert_rules"), true),
            R::AlertList => status(self.pagination.contains_key("alerts"), true),
            R::AssignmentList => status(self.pagination.contains_key("assignments"), true),
            R::AssignmentGet => status(
                self.assignment_detail.is_some(),
                self.selected_assignment_id.is_some(),
            ),
            R::ReportList => status(self.pagination.contains_key("reports"), true),
            R::ReportGet => status(
                self.report_detail.is_some(),
                self.selected_report_id.is_some(),
            ),
            R::ReportDeliveryStateGet => status(
                self.delivery_state.is_some(),
                self.selected_report_id.is_some(),
            ),
            R::ReportReviewList => status(self.pagination.contains_key("reviews"), true),
            R::ReportReviewGet => status(
                self.review_detail.is_some(),
                self.selected_review_id.is_some(),
            ),
            R::LiveStream | R::LiveSnapshot => {
                if self.live_stream_available
                    && self.live_epoch.is_some()
                    && self.live_cursor.is_some()
                {
                    "visible"
                } else if self.live_stream_available {
                    "connecting"
                } else {
                    "unavailable"
                }
            }
            _ => return None,
        })
    }

    #[must_use]
    pub fn current_detail(&self) -> Option<Value> {
        match self.active_tab {
            MfgViewTab::Overview => Some(serde_json::json!({
                "app": self.app_descriptor.as_ref(),
                "command_center": self.command_center.as_ref(),
                "decision_trace": self.decision_trace.as_ref(),
                "latest_action_result": self.latest_action_result,
                "live_stream": {
                    "available": self.live_stream_available,
                    "epoch": self.live_epoch.as_deref(),
                    "cursor": self.live_cursor.as_deref(),
                    "freshness": self.freshness,
                    "connection": self.connection,
                }
            })),
            MfgViewTab::Incidents => Some(serde_json::json!({
                "incident": self.incident_detail.as_ref(),
                "room": self.incident_room.as_ref(),
                "analysis": self.analysis.as_ref(),
                "execution": self.executions.as_ref(),
                "latest_action_result": self.latest_action_result,
            })),
            MfgViewTab::Alerts => self.selected_item().map(|item| item.raw.clone()),
            MfgViewTab::Assignments => self
                .assignment_detail
                .as_ref()
                .and_then(|document| serde_json::to_value(document).ok()),
            MfgViewTab::Reports => Some(serde_json::json!({
                "report": self.report_detail.as_ref(),
                "delivery_state": self.delivery_state.as_ref(),
                "latest_action_result": self.latest_action_result,
            })),
            MfgViewTab::Reviews => self
                .review_detail
                .as_ref()
                .and_then(|review| serde_json::to_value(review).ok()),
            MfgViewTab::Insights => Some(serde_json::json!({
                "focused_evidence_ref": self.focused_evidence_ref,
                "evidence_backlink_resolved": self.focused_evidence_ref.is_some()
                    && self.p1_documents.contains_key(
                        &app_mfg_contract::MfgRouteId::RealityEvidenceGet
                    )
                    && self.p1_documents.contains_key(
                        &app_mfg_contract::MfgRouteId::RealityEvidenceContext
                    )
                    && !self.section_errors.contains_key("evidence")
                    && !self.section_errors.contains_key("evidence_context"),
                "focused_quality_gate_id": self.focused_quality_gate_id,
                "selected": self.selected_item().map(|item| item.raw.clone()),
                "reality_health": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityHealthGet
                ),
                "data_plane_health": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityDataPlaneHealthGet
                ),
                "metric_detail": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityMetricGet
                ),
                "metric_lineage": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityMetricLineage
                ),
                "skill_detail": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::SkillGet
                ),
                "skill_run_detail": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::SkillRunGet
                ),
                "evidence": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityEvidenceGet
                ),
                "evidence_context": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityEvidenceContext
                ),
                "quality_gate": self.p1_documents.get(
                    &app_mfg_contract::MfgRouteId::RealityQualityGateGet
                ),
                "latest_action_result": self.latest_action_result,
            })),
        }
    }
}

fn mfg_route_for_section(section: &str) -> Option<app_mfg_contract::MfgRouteId> {
    use app_mfg_contract::MfgRouteId as R;
    Some(match section {
        "contract" => R::ContractGet,
        "app" => R::AppGet,
        "command_center" => R::CommandCenterGet,
        "decision_trace" => R::DecisionTraceGet,
        "incidents" => R::IncidentList,
        "incident_detail" => R::IncidentGet,
        "incident_room" => R::IncidentRoomGet,
        "analysis" => R::AnalysisGet,
        "execution" => R::ExecutionGet,
        "alert_rules" => R::AlertRuleList,
        "alerts" => R::AlertList,
        "assignments" => R::AssignmentList,
        "assignment_detail" => R::AssignmentGet,
        "reports" => R::ReportList,
        "report_detail" => R::ReportGet,
        "delivery_state" => R::ReportDeliveryStateGet,
        "reviews" => R::ReportReviewList,
        "review_detail" => R::ReportReviewGet,
        _ => return None,
    })
}

struct MfgActionDraft {
    resource_ref: String,
    path_replacements: BTreeMap<String, String>,
    expected_revision: Option<u64>,
    payload_digest: String,
    request_body: Value,
}

fn build_mfg_action_draft(
    state: &MfgOperationsState,
    action: &app_mfg_contract::MfgActionContract,
    override_payload: Option<Value>,
) -> Result<MfgActionDraft, String> {
    let (mut path_replacements, default_resource_ref, default_body) =
        match default_mfg_action_context(state, action) {
            Ok(context) => context,
            Err(_) if override_payload.is_some() => (BTreeMap::new(), String::new(), None),
            Err(error) => return Err(error),
        };
    let mut requested_resource_ref = None;
    let request_body = if let Some(override_payload) = override_payload {
        if let Some(object) = override_payload.as_object() {
            if let Some(path) = object.get("path").and_then(Value::as_object) {
                for (key, value) in path {
                    let value = value
                        .as_str()
                        .ok_or_else(|| format!("path replacement {key} must be a string"))?;
                    path_replacements.insert(key.clone(), value.to_string());
                }
            }
            if let Some(value) = object.get("resource_ref").and_then(Value::as_str) {
                requested_resource_ref = Some(value.to_string());
            }
            object.get("body").cloned().unwrap_or(override_payload)
        } else {
            override_payload
        }
    } else {
        default_body.ok_or_else(|| {
            format!(
                "{} requires explicit JSON: /mfg action {} {{\"body\":{{...}}}}",
                action.action_id.as_str(),
                action.action_id.as_str()
            )
        })?
    };
    let route = app_mfg_contract::mfg_route_contract(action.route_id)
        .ok_or_else(|| format!("{} route contract is missing", action.route_id.as_str()))?;
    for segment in route
        .path
        .split('/')
        .filter_map(|segment| segment.strip_prefix(':'))
    {
        if !path_replacements.contains_key(segment) {
            return Err(format!(
                "{} requires path.{}",
                action.action_id.as_str(),
                segment
            ));
        }
    }
    if path_replacements
        .values()
        .any(|value| value.trim().starts_with("<required:"))
    {
        return Err(format!(
            "{} path still contains <required:...> placeholders",
            action.action_id.as_str()
        ));
    }
    let resource_ref =
        canonical_tui_mfg_resource_ref(action.action_id, &path_replacements, &request_body)
            .or_else(|| (!default_resource_ref.is_empty()).then_some(default_resource_ref))
            .ok_or_else(|| {
                format!(
                    "{} request target cannot be derived from its path/body",
                    action.action_id.as_str()
                )
            })?;
    if requested_resource_ref
        .as_deref()
        .is_some_and(|requested| requested != resource_ref)
    {
        return Err(format!(
            "resource_ref must match the canonical request target {resource_ref}"
        ));
    }
    let resolved_action = resolve_tui_mfg_action_id(action.route_id, &request_body)?;
    if resolved_action != action.action_id {
        return Err(format!(
            "request payload resolves to {}, not selected action {}",
            resolved_action.as_str(),
            action.action_id.as_str()
        ));
    }
    validate_tui_mfg_manual_input(action.action_id, &request_body)?;
    let expected_revision = match action.mutation {
        app_mfg_contract::MfgMutationSemantics::DurableReceipt {
            revision: app_mfg_contract::MfgRevisionSemantics::Required,
            ..
        } => find_u64_recursive(&request_body, "expected_revision"),
        _ => None,
    };
    let payload_digest = stable_mfg_payload_digest(&request_body)?;
    Ok(MfgActionDraft {
        resource_ref,
        path_replacements,
        expected_revision,
        payload_digest,
        request_body,
    })
}

#[must_use]
pub const fn mfg_action_requires_explicit_input(action_id: app_mfg_contract::MfgActionId) -> bool {
    use app_mfg_contract::{MfgActionId as I, MfgMultiActionId as A, MfgRouteId as R};
    matches!(
        action_id,
        I::Route(R::IncidentCreate | R::ExecutionFeedbackCreate | R::ReportGenerate)
            | I::Multi(
                A::AlertSnooze
                    | A::AssignmentCreate
                    | A::AssignmentAssign
                    | A::AssignmentTransfer
                    | A::ReportReviewReroute
                    | A::ReportReviewResolve
            )
    )
}

fn mfg_action_input_template(
    state: &MfgOperationsState,
    action: &app_mfg_contract::MfgActionContract,
) -> Result<Value, String> {
    use app_mfg_contract::{MfgActionId as I, MfgMultiActionId as A, MfgRouteId as R};
    let mut path = default_mfg_action_context(state, action)
        .map(|(path, _, _)| path)
        .unwrap_or_default();
    let selected_revision = |items: &[MfgItemSummary], id: &str| {
        items
            .iter()
            .find(|item| item.id == id)
            .and_then(|item| item.revision)
            .ok_or_else(|| format!("{id} has no canonical revision; refresh before acting"))
    };
    let body = match action.action_id {
        I::Route(R::IncidentCreate) => serde_json::json!({
            "title": "<required:incident title>"
        }),
        I::Multi(A::AlertSnooze) => {
            let id = path
                .get("id")
                .ok_or_else(|| "Select an alert first.".to_string())?;
            serde_json::json!({
                "command": "snooze",
                "expected_revision": selected_revision(&state.alerts, id)?,
                "until": "<required:RFC3339 UTC timestamp>",
                "reason": "<required:snooze reason>"
            })
        }
        I::Multi(A::AssignmentCreate) => serde_json::json!({
            "assignment": {
                "assignment_id": "<required:stable assignment id>",
                "task_ref": "<required:task://task-id>",
                "assignee_ref": "<required:principal or team ref>",
                    "assignee_kind": "user",
                "watcher_refs": [],
                "priority": "normal",
                "notification_targets": [],
                "visibility": "private"
            }
        }),
        I::Multi(A::AssignmentAssign | A::AssignmentTransfer) => {
            let id = path
                .get("id")
                .ok_or_else(|| "Select an assignment first.".to_string())?;
            let command = if action.action_id == I::Multi(A::AssignmentAssign) {
                "assign"
            } else {
                "transfer"
            };
            serde_json::json!({
                "command": command,
                "expected_revision": selected_revision(&state.assignments, id)?,
                "target_ref": "<required:principal or team ref>",
                "reason": "<required:assignment reason>"
            })
        }
        I::Route(R::ExecutionFeedbackCreate) => {
            path.entry("id".to_string())
                .or_insert_with(|| "<required:execution id>".to_string());
            serde_json::json!({
                "outcome": "<required:observed outcome>",
                "note": "<required:evidence-backed feedback note>"
            })
        }
        I::Route(R::ReportGenerate) => {
            path.entry("id".to_string())
                .or_insert_with(|| "<required:cockpit profile id>".to_string());
            serde_json::json!({
                "report": {
                    "cadence": "manual",
                    "note": "<required:report purpose>"
                }
            })
        }
        I::Multi(A::ReportReviewReroute | A::ReportReviewResolve) => {
            let id = path
                .get("id")
                .ok_or_else(|| "Select a report review first.".to_string())?;
            let review = state
                .review_detail
                .as_ref()
                .filter(|review| review.review_id == *id)
                .ok_or_else(|| "Selected review detail is not loaded.".to_string())?;
            if action.action_id == I::Multi(A::ReportReviewReroute) {
                serde_json::json!({
                    "decision": "reroute",
                    "expected_revision": review.revision,
                    "reason": "<required:reroute reason>",
                    "evidence_refs": review.evidence_refs,
                    "reroute": {
                        "target_ref": "<required:channel:// or surface:// target>",
                        "provider_account": "<required:provider account>",
                        "channel": "<required:channel>",
                        "requested_capability": "<required:surface capability>"
                    }
                })
            } else {
                serde_json::json!({
                    "decision": "resolve",
                    "expected_revision": review.revision,
                    "reason": "<required:external disposition>",
                    "evidence_refs": if review.evidence_refs.is_empty() {
                        vec!["<required:evidence ref>".to_string()]
                    } else {
                        review.evidence_refs.clone()
                    }
                })
            }
        }
        _ => {
            return Err(format!(
                "{} does not require an explicit input template",
                action.action_id.as_str()
            ));
        }
    };
    Ok(serde_json::json!({"path": path, "body": body}))
}

fn validate_tui_mfg_manual_input(
    action_id: app_mfg_contract::MfgActionId,
    body: &Value,
) -> Result<(), String> {
    if !mfg_action_requires_explicit_input(action_id) {
        return Ok(());
    }
    if contains_required_mfg_placeholder(body) {
        return Err(format!(
            "{} input still contains <required:...> placeholders",
            action_id.as_str()
        ));
    }
    Ok(())
}

fn contains_required_mfg_placeholder(value: &Value) -> bool {
    match value {
        Value::String(value) => value.trim().starts_with("<required:"),
        Value::Array(values) => values.iter().any(contains_required_mfg_placeholder),
        Value::Object(values) => values.values().any(contains_required_mfg_placeholder),
        _ => false,
    }
}

fn canonical_tui_mfg_resource_ref(
    action_id: app_mfg_contract::MfgActionId,
    path: &BTreeMap<String, String>,
    body: &Value,
) -> Option<String> {
    use app_mfg_contract::{MfgActionId as I, MfgMultiActionId as A, MfgRouteId as R};
    let required = |name: &str| path.get(name).filter(|value| !value.trim().is_empty());
    match action_id {
        I::Route(R::IncidentCreate) => Some("mfg:incident:new".to_string()),
        I::Route(R::IncidentAnalyze) => Some(format!("mfg:incident:{}", required("id")?)),
        I::Multi(A::AnalysisActionDryRun | A::AnalysisActionCommit) => Some(format!(
            "mfg:analysis:{}:action:{}",
            required("analysis_id")?,
            required("action_id")?
        )),
        I::Multi(A::AlertAcknowledge | A::AlertSnooze | A::AlertResolve | A::AlertEscalate) => {
            Some(format!("mfg:alert-occurrence:{}", required("id")?))
        }
        I::Multi(A::AssignmentCreate) => {
            let id = find_string_recursive_local(body, "assignment_id")
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "new".to_string());
            Some(format!("mfg:assignment:{id}"))
        }
        I::Multi(
            A::AssignmentUpdate
            | A::AssignmentAssign
            | A::AssignmentClaim
            | A::AssignmentTransfer
            | A::AssignmentUnassign
            | A::AssignmentWatch
            | A::AssignmentRequestUpdate
            | A::AssignmentEscalate
            | A::AssignmentStart
            | A::AssignmentComplete,
        ) => {
            let id = required("id").cloned().or_else(|| {
                find_string_recursive_local(body, "assignment_id")
                    .filter(|value| !value.trim().is_empty())
            })?;
            Some(format!("mfg:assignment:{id}"))
        }
        I::Route(R::ExecutionFeedbackCreate) => Some(format!("mfg:execution:{}", required("id")?)),
        I::Route(R::ReportGenerate) => Some(format!("mfg:cockpit-profile:{}", required("id")?)),
        I::Multi(
            A::ReportDeliverDryRun
            | A::ReportDeliverCommit
            | A::ReportDeliveryRetryDryRun
            | A::ReportDeliveryRetryCommit,
        )
        | I::Route(R::ReportReviewRequest) => {
            Some(format!("mfg:cockpit-report:{}", required("id")?))
        }
        I::Multi(
            A::ReportReviewForceRetry
            | A::ReportReviewReroute
            | A::ReportReviewAbandon
            | A::ReportReviewResolve
            | A::ReportReviewReject,
        ) => Some(format!("mfg:report-review:{}", required("id")?)),
        I::Route(R::RealityEvidenceQualityGate) => {
            Some(format!("mfg:evidence:{}", required("id")?))
        }
        I::Route(R::IncidentPlaybookRecommend | R::IncidentSkillPlan) => {
            Some(format!("mfg:incident:{}", required("id")?))
        }
        I::Multi(A::SkillRun) => Some(format!(
            "mfg:incident:{}:skill:{}",
            required("id")?,
            required("skill_id")?
        )),
        _ => None,
    }
}

fn resolve_tui_mfg_action_id(
    route_id: app_mfg_contract::MfgRouteId,
    body: &Value,
) -> Result<app_mfg_contract::MfgActionId, String> {
    use app_mfg_contract::{MfgActionId, MfgMultiActionId as A, MfgRouteId as R};
    let multi = match route_id {
        R::AlertCommand => match find_string_recursive_local(body, "command").as_deref() {
            Some("acknowledge") => A::AlertAcknowledge,
            Some("snooze") => A::AlertSnooze,
            Some("resolve") => A::AlertResolve,
            Some("escalate") => A::AlertEscalate,
            _ => return Err("alert command must be a typed MFG alert action".to_string()),
        },
        R::AssignmentUpsert => {
            if find_u64_recursive(body, "expected_revision").is_some() {
                A::AssignmentUpdate
            } else {
                A::AssignmentCreate
            }
        }
        R::AssignmentCommand => match find_string_recursive_local(body, "command").as_deref() {
            Some("assign") => A::AssignmentAssign,
            Some("claim") => A::AssignmentClaim,
            Some("transfer") => A::AssignmentTransfer,
            Some("unassign") => A::AssignmentUnassign,
            Some("watch") => A::AssignmentWatch,
            Some("request_update") => A::AssignmentRequestUpdate,
            Some("escalate") => A::AssignmentEscalate,
            Some("start") => A::AssignmentStart,
            Some("complete") => A::AssignmentComplete,
            _ => {
                return Err("assignment command must be a typed MFG assignment action".to_string());
            }
        },
        R::AnalysisActionExecute => {
            match find_string_recursive_local(body, "mode")
                .unwrap_or_else(|| "dry_run".to_string())
                .as_str()
            {
                "dry_run" | "plan" => A::AnalysisActionDryRun,
                "commit" => A::AnalysisActionCommit,
                _ => return Err("analysis action mode must be dry_run or commit".to_string()),
            }
        }
        R::ReportDeliver => {
            match find_string_recursive_local(body, "mode")
                .unwrap_or_else(|| "dry_run".to_string())
                .as_str()
            {
                "dry_run" | "plan" => A::ReportDeliverDryRun,
                "commit" => A::ReportDeliverCommit,
                _ => return Err("report delivery mode must be dry_run or commit".to_string()),
            }
        }
        R::ReportDeliveryRetry => {
            match find_string_recursive_local(body, "mode")
                .unwrap_or_else(|| "dry_run".to_string())
                .as_str()
            {
                "dry_run" | "plan" => A::ReportDeliveryRetryDryRun,
                "commit" => A::ReportDeliveryRetryCommit,
                _ => return Err("report retry mode must be dry_run or commit".to_string()),
            }
        }
        R::ReportReviewDecide => match find_string_recursive_local(body, "decision").as_deref() {
            Some("force_retry") => A::ReportReviewForceRetry,
            Some("reroute") => A::ReportReviewReroute,
            Some("abandon") => A::ReportReviewAbandon,
            Some("resolve") => A::ReportReviewResolve,
            Some("reject") => A::ReportReviewReject,
            _ => return Err("report review requires a typed decision".to_string()),
        },
        R::IncidentSkillRun => A::SkillRun,
        _ => return Ok(MfgActionId::Route(route_id)),
    };
    Ok(MfgActionId::Multi(multi))
}

fn default_mfg_action_context(
    state: &MfgOperationsState,
    action: &app_mfg_contract::MfgActionContract,
) -> Result<(BTreeMap<String, String>, String, Option<Value>), String> {
    use app_mfg_contract::{MfgMultiActionId as A, MfgRouteId as R};
    let mut path = BTreeMap::new();
    let selected_incident = || {
        state
            .selected_incident_id
            .clone()
            .ok_or_else(|| "Select an incident first.".to_string())
    };
    let selected_alert = || {
        state
            .selected_alert_id
            .clone()
            .ok_or_else(|| "Select an alert first.".to_string())
    };
    let selected_assignment = || {
        state
            .selected_assignment_id
            .clone()
            .ok_or_else(|| "Select an assignment first.".to_string())
    };
    let selected_report = || {
        state
            .selected_report_id
            .clone()
            .ok_or_else(|| "Select a report first.".to_string())
    };
    let selected_review = || {
        state
            .selected_review_id
            .clone()
            .ok_or_else(|| "Select a report review first.".to_string())
    };
    let selected_revision = |items: &[MfgItemSummary], id: &str| {
        items
            .iter()
            .find(|item| item.id == id)
            .and_then(|item| item.revision)
            .ok_or_else(|| format!("{id} has no canonical revision; refresh before acting"))
    };
    let (resource_ref, body) = match action.action_id {
        app_mfg_contract::MfgActionId::Route(R::IncidentCreate) => {
            ("mfg:incident:new".to_string(), None)
        }
        app_mfg_contract::MfgActionId::Route(R::IncidentAnalyze) => {
            let id = selected_incident()?;
            path.insert("id".to_string(), id.clone());
            (format!("mfg:incident:{id}"), Some(serde_json::json!({})))
        }
        app_mfg_contract::MfgActionId::Multi(A::AnalysisActionDryRun)
        | app_mfg_contract::MfgActionId::Multi(A::AnalysisActionCommit) => {
            let value = state
                .analysis
                .as_ref()
                .and_then(|document| serde_json::to_value(document).ok())
                .ok_or_else(|| "Selected incident has no canonical analysis.".to_string())?;
            let analysis_id = find_string_recursive_local(&value, "analysis_id")
                .ok_or_else(|| "Analysis response has no analysis_id.".to_string())?;
            let action_id = find_string_recursive_local(&value, "action_id")
                .ok_or_else(|| "Analysis response has no action_id.".to_string())?;
            path.insert("analysis_id".to_string(), analysis_id.clone());
            path.insert("action_id".to_string(), action_id.clone());
            let mode = if action.action_id
                == app_mfg_contract::MfgActionId::Multi(A::AnalysisActionDryRun)
            {
                "dry_run"
            } else {
                "commit"
            };
            let body = if mode == "commit" {
                let expected_revision =
                    find_u64_recursive(&value, "revision").ok_or_else(|| {
                        "Canonical analysis has no revision; refresh before commit.".to_string()
                    })?;
                serde_json::json!({
                    "mode": mode,
                    "expected_revision": expected_revision,
                })
            } else {
                serde_json::json!({"mode": mode})
            };
            (
                format!("mfg:analysis:{analysis_id}:action:{action_id}"),
                Some(body),
            )
        }
        app_mfg_contract::MfgActionId::Multi(
            command @ (A::AlertAcknowledge | A::AlertSnooze | A::AlertResolve | A::AlertEscalate),
        ) => {
            let id = selected_alert()?;
            let revision = selected_revision(&state.alerts, &id)?;
            path.insert("id".to_string(), id.clone());
            let command_name = command
                .as_str()
                .strip_prefix("mfg.alert.")
                .unwrap_or(command.as_str());
            let body = (command != A::AlertSnooze).then(|| {
                serde_json::json!({
                    "command": command_name,
                    "expected_revision": revision,
                    "reason": "TUI governed action",
                })
            });
            (format!("mfg:alert-occurrence:{id}"), body)
        }
        app_mfg_contract::MfgActionId::Multi(A::AssignmentCreate) => {
            ("mfg:assignment:new".to_string(), None)
        }
        app_mfg_contract::MfgActionId::Multi(A::AssignmentUpdate) => {
            let id = selected_assignment()?;
            let item = state
                .assignments
                .iter()
                .find(|item| item.id == id)
                .ok_or_else(|| "Selected assignment is not loaded.".to_string())?;
            let input = assignment_input_from_summary(item)?;
            (
                format!("mfg:assignment:{id}"),
                Some(serde_json::json!({"assignment": input})),
            )
        }
        app_mfg_contract::MfgActionId::Multi(
            command @ (A::AssignmentAssign
            | A::AssignmentClaim
            | A::AssignmentTransfer
            | A::AssignmentUnassign
            | A::AssignmentWatch
            | A::AssignmentRequestUpdate
            | A::AssignmentEscalate
            | A::AssignmentStart
            | A::AssignmentComplete),
        ) => {
            let id = selected_assignment()?;
            let revision = selected_revision(&state.assignments, &id)?;
            path.insert("id".to_string(), id.clone());
            let command_name = command
                .as_str()
                .strip_prefix("mfg.assignment.")
                .unwrap_or(command.as_str());
            let body =
                (!matches!(command, A::AssignmentAssign | A::AssignmentTransfer)).then(|| {
                    serde_json::json!({
                        "command": command_name,
                        "expected_revision": revision,
                        "reason": "TUI governed action",
                    })
                });
            (format!("mfg:assignment:{id}"), body)
        }
        app_mfg_contract::MfgActionId::Route(R::ExecutionFeedbackCreate) => {
            let id = state
                .execution_ref
                .clone()
                .ok_or_else(|| "Selected incident has no canonical execution.".to_string())?;
            path.insert("id".to_string(), id.clone());
            (format!("mfg:execution:{id}"), None)
        }
        app_mfg_contract::MfgActionId::Route(R::ReportGenerate) => {
            let profile_id = state
                .selected_item()
                .and_then(|item| find_string_recursive_local(&item.raw, "profile_id"))
                .ok_or_else(|| {
                    "Report generation requires path.id for a canonical cockpit profile."
                        .to_string()
                })?;
            path.insert("id".to_string(), profile_id.clone());
            (format!("mfg:cockpit-profile:{profile_id}"), None)
        }
        app_mfg_contract::MfgActionId::Multi(A::ReportDeliverDryRun)
        | app_mfg_contract::MfgActionId::Multi(A::ReportDeliverCommit)
        | app_mfg_contract::MfgActionId::Multi(A::ReportDeliveryRetryDryRun)
        | app_mfg_contract::MfgActionId::Multi(A::ReportDeliveryRetryCommit) => {
            let id = selected_report()?;
            if action.action_id
                == app_mfg_contract::MfgActionId::Multi(A::ReportDeliveryRetryCommit)
            {
                let delivery = state
                    .delivery_state
                    .as_ref()
                    .and_then(|document| serde_json::to_value(document).ok())
                    .ok_or_else(|| {
                        "Load canonical delivery state before committing a retry.".to_string()
                    })?;
                if find_string_recursive_local(&delivery, "report_id").as_deref()
                    != Some(id.as_str())
                    || find_bool_recursive_local(&delivery, "retryable") != Some(true)
                {
                    return Err(
                        "Canonical delivery state is not retryable; use dry-run or request typed review."
                            .to_string(),
                    );
                }
            }
            path.insert("id".to_string(), id.clone());
            let dry_run = matches!(
                action.action_id,
                app_mfg_contract::MfgActionId::Multi(A::ReportDeliverDryRun)
                    | app_mfg_contract::MfgActionId::Multi(A::ReportDeliveryRetryDryRun)
            );
            let body = if dry_run {
                serde_json::json!({"mode": "dry_run"})
            } else {
                serde_json::json!({
                    "mode": "commit",
                    "expected_revision": selected_revision(&state.reports, &id)?,
                })
            };
            (format!("mfg:cockpit-report:{id}"), Some(body))
        }
        app_mfg_contract::MfgActionId::Route(R::ReportReviewRequest) => {
            let id = selected_report()?;
            let revision = selected_revision(&state.reports, &id)?;
            path.insert("id".to_string(), id.clone());
            let evidence_refs = state
                .reports
                .iter()
                .find(|item| item.id == id)
                .map(|item| item.evidence_refs.clone())
                .unwrap_or_default();
            (
                format!("mfg:cockpit-report:{id}"),
                Some(serde_json::json!({
                    "expected_report_revision": revision,
                    "reason": "Dead-letter review requested from TUI",
                    "evidence_refs": evidence_refs,
                })),
            )
        }
        app_mfg_contract::MfgActionId::Multi(
            decision @ (A::ReportReviewForceRetry
            | A::ReportReviewReroute
            | A::ReportReviewAbandon
            | A::ReportReviewResolve
            | A::ReportReviewReject),
        ) => {
            let id = selected_review()?;
            let review = state
                .review_detail
                .as_ref()
                .filter(|review| review.review_id == id)
                .ok_or_else(|| "Selected review detail is not loaded.".to_string())?;
            path.insert("id".to_string(), id.clone());
            let decision_name = decision
                .as_str()
                .strip_prefix("mfg.report.review.")
                .unwrap_or(decision.as_str());
            let manual = matches!(decision, A::ReportReviewReroute | A::ReportReviewResolve);
            (
                format!("mfg:report-review:{id}"),
                (!manual).then(|| {
                    serde_json::json!({
                        "decision": decision_name,
                        "expected_revision": review.revision,
                        "reason": "Governed TUI review decision",
                        "evidence_refs": review.evidence_refs,
                    })
                }),
            )
        }
        app_mfg_contract::MfgActionId::Route(R::RealityEvidenceQualityGate) => {
            let evidence = selected_evidence_ref(state)?;
            path.insert("id".to_string(), evidence.clone());
            (
                format!("mfg:evidence:{evidence}"),
                Some(serde_json::json!({})),
            )
        }
        app_mfg_contract::MfgActionId::Route(R::IncidentPlaybookRecommend) => {
            let id = selected_incident()?;
            path.insert("id".to_string(), id.clone());
            (
                format!("mfg:incident:{id}"),
                Some(serde_json::json!({"limit": 5})),
            )
        }
        app_mfg_contract::MfgActionId::Route(R::IncidentSkillPlan) => {
            let id = selected_incident()?;
            path.insert("id".to_string(), id.clone());
            (
                format!("mfg:incident:{id}"),
                Some(serde_json::json!({"limit": 3})),
            )
        }
        app_mfg_contract::MfgActionId::Multi(A::SkillRun) => {
            let incident_id = selected_incident()?;
            let skill_id = state
                .selected_insight_id
                .as_deref()
                .and_then(|selected| {
                    state
                        .insights
                        .iter()
                        .find(|item| item.id == selected && item.kind == "skill")
                })
                .map(|item| item.id.clone())
                .ok_or_else(|| {
                    "Select a canonical skill in Insights before running it.".to_string()
                })?;
            path.insert("id".to_string(), incident_id.clone());
            path.insert("skill_id".to_string(), skill_id.clone());
            let revision = selected_revision(&state.incidents, &incident_id)?;
            (
                format!("mfg:incident:{incident_id}:skill:{skill_id}"),
                Some(serde_json::json!({"expected_revision": revision})),
            )
        }
        _ => {
            return Err(format!(
                "{} has no TUI request builder",
                action.action_id.as_str()
            ));
        }
    };
    Ok((path, resource_ref, body))
}

fn assignment_input_from_summary(item: &MfgItemSummary) -> Result<Value, String> {
    let mut input = serde_json::Map::new();
    for key in [
        "assignment_id",
        "task_ref",
        "workflow_id",
        "workflow_node_id",
        "incident_id",
        "assignee_ref",
        "assignee_kind",
        "watcher_refs",
        "priority",
        "due_at",
        "sla_minutes",
        "notification_targets",
        "visibility",
    ] {
        if let Some(value) = item.raw.get(key) {
            input.insert(key.to_string(), value.clone());
        }
    }
    let revision = item
        .revision
        .ok_or_else(|| "Selected assignment has no canonical revision.".to_string())?;
    input.insert("expected_revision".to_string(), Value::from(revision));
    Ok(Value::Object(input))
}

fn selected_evidence_ref(state: &MfgOperationsState) -> Result<String, String> {
    state
        .focused_evidence_ref
        .clone()
        .or_else(|| {
            state.selected_item().and_then(|item| {
                item.backlinks
                    .iter()
                    .find(|backlink| backlink.kind == MfgBacklinkKind::Evidence)
                    .and_then(|backlink| {
                        backlink
                            .target
                            .strip_prefix("evidence://matrix/")
                            .map(str::to_string)
                    })
            })
        })
        .or_else(|| {
            state
                .incidents
                .iter()
                .find(|item| state.selected_incident_id.as_deref() == Some(item.id.as_str()))
                .and_then(|item| {
                    item.backlinks
                        .iter()
                        .find(|backlink| backlink.kind == MfgBacklinkKind::Evidence)
                        .and_then(|backlink| {
                            backlink
                                .target
                                .strip_prefix("evidence://matrix/")
                                .map(str::to_string)
                        })
                })
        })
        .ok_or_else(|| "Selected object has no canonical Matrix evidence packet.".to_string())
}

fn stable_mfg_payload_digest(value: &Value) -> Result<String, String> {
    let mut value = value.clone();
    remove_mfg_json_field(&mut value, "idempotency_key");
    let canonical = canonicalize_json(&value);
    let bytes = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn stable_tui_mfg_resource_id(prefix: &str, idempotency_key: &str) -> String {
    let digest = Sha256::digest(format!("{prefix}:{idempotency_key}").as_bytes());
    format!("{prefix}-{digest:x}")[..prefix.len() + 1 + 20].to_string()
}

fn remove_mfg_json_field(value: &mut Value, key: &str) {
    match value {
        Value::Object(object) => {
            object.remove(key);
            for value in object.values_mut() {
                remove_mfg_json_field(value, key);
            }
        }
        Value::Array(items) => {
            for value in items {
                remove_mfg_json_field(value, key);
            }
        }
        _ => {}
    }
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut sorted = BTreeMap::new();
            for (key, value) in object {
                sorted.insert(key.clone(), canonicalize_json(value));
            }
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        _ => value.clone(),
    }
}

fn find_string_recursive_local(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(object) => object
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                object
                    .values()
                    .find_map(|child| find_string_recursive_local(child, key))
            }),
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_string_recursive_local(child, key)),
        _ => None,
    }
}

fn find_u64_recursive(value: &Value, key: &str) -> Option<u64> {
    match value {
        Value::Object(object) => object.get(key).and_then(Value::as_u64).or_else(|| {
            object
                .values()
                .find_map(|child| find_u64_recursive(child, key))
        }),
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_u64_recursive(child, key)),
        _ => None,
    }
}

fn find_bool_recursive_local(value: &Value, key: &str) -> Option<bool> {
    match value {
        Value::Object(object) => object.get(key).and_then(Value::as_bool).or_else(|| {
            object
                .values()
                .find_map(|child| find_bool_recursive_local(child, key))
        }),
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_bool_recursive_local(child, key)),
        _ => None,
    }
}

pub(crate) fn mfg_required_capability(error: &MfgApiErrorV1) -> Option<&str> {
    error
        .details
        .get("required_capability")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|capability| !capability.is_empty())
}

pub(crate) fn mfg_route_requires_capability(
    requirement: &app_mfg_contract::MfgCapabilityRequirement,
    capability: &str,
) -> bool {
    match requirement {
        app_mfg_contract::MfgCapabilityRequirement::One {
            capability: required,
        } => required.as_str() == capability,
        app_mfg_contract::MfgCapabilityRequirement::All { capabilities } => capabilities
            .iter()
            .any(|required| required.as_str() == capability),
        app_mfg_contract::MfgCapabilityRequirement::PerAction => false,
    }
}

fn live_summary_list(value: &Value, field: &str, kind: &str) -> Vec<MfgItemSummary> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| live_item_summary(value, kind))
        .collect()
}

fn live_receipt_list(value: &Value, field: &str) -> Vec<MfgReceiptV1> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| serde_json::from_value(value.clone()).ok())
        .collect()
}

fn exact_nested_value<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    value
        .get(field)
        .or_else(|| value.get("payload").and_then(|payload| payload.get(field)))
}

fn live_item_summary(value: &Value, kind: &str) -> Option<MfgItemSummary> {
    let id_fields: &[&str] = match kind {
        "skill_run" => &["execution_id"],
        "cockpit_profile" => &["profile_id"],
        "alert_subscription" => &["subscription_id"],
        "assignment" => &["assignment_id"],
        "alert" => &["occurrence_id", "alert_id"],
        "alert_rule" => &["rule_id"],
        "incident" => &["incident_id"],
        "report" => &["report_id"],
        "review" => &["review_id"],
        "execution" => &["execution_id"],
        "workflow" => &["workflow_id"],
        "analysis" => &["analysis_id"],
        "memory_case" => &["case_id"],
        "playbook" => &["playbook_id"],
        "receipt" => &["receipt_id"],
        "entity" => &["entity_id"],
        "relation" => &["relation_id"],
        "fact" => &["fact_id"],
        "attention" => &["attention_id"],
        "evidence" => &["packet_id"],
        "quality_gate" => &["gate_id"],
        "metric_definition" => &["metric_id"],
        "metric_dependency" => &["dependency_id"],
        "metric_state" => &["state_id"],
        "metric_snapshot" => &["snapshot_id"],
        "watermark" => &["source_ref"],
        "compute_job" => &["job_id"],
        "metric_change" => &["change_id"],
        "source_pack" => &["source_pack_id"],
        "connector_run" => &["run_id"],
        "ontology" => &["ontology_id"],
        "entity_match_candidate" => &["candidate_id"],
        "entity_conflict_decision" => &["decision_id"],
        _ => &["id"],
    };
    let id = id_fields
        .iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))?
        .to_string();
    let title = ["title", "display_name", "summary", "objective"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .unwrap_or(&id)
        .to_string();
    let status = ["status", "state", "lifecycle"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string();
    let evidence_refs = value
        .get("evidence_refs")
        .or_else(|| value.pointer("/execution_context/evidence_refs"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut backlinks = Vec::new();
    if let Some(reference) = value
        .get("runtime_execution_ref")
        .and_then(Value::as_str)
        .filter(|reference| reference.starts_with("runtime-execution://"))
    {
        backlinks.push(MfgBacklink {
            kind: MfgBacklinkKind::Runtime,
            target: reference.to_string(),
            label: format!("Runtime execution {reference}"),
        });
    }
    if let Some(packet_id) = value
        .get("evidence_packet_id")
        .or_else(|| value.pointer("/execution_context/evidence_packet_id"))
        .and_then(Value::as_str)
    {
        backlinks.push(MfgBacklink {
            kind: MfgBacklinkKind::Evidence,
            target: format!("evidence://matrix/{packet_id}"),
            label: format!("Evidence {packet_id}"),
        });
    }
    Some(MfgItemSummary {
        id,
        kind: kind.to_string(),
        title,
        status,
        severity: ["severity", "priority", "risk"]
            .into_iter()
            .find_map(|field| value.get(field).and_then(Value::as_str))
            .map(str::to_string),
        owner: [
            "owner_ref",
            "assignee_ref",
            "reviewer_principal",
            "requester_principal",
        ]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .map(str::to_string),
        sla: None,
        revision: value.get("revision").and_then(Value::as_u64),
        evidence_refs,
        backlinks,
        raw: value.clone(),
    })
}

fn upsert_live_summary(items: &mut Vec<MfgItemSummary>, mut summary: MfgItemSummary) {
    if let Some(existing) = items.iter_mut().find(|item| item.id == summary.id) {
        if summary.backlinks.is_empty() {
            summary.backlinks.clone_from(&existing.backlinks);
        }
        *existing = summary;
    } else {
        items.push(summary);
    }
}

fn upsert_live_receipt(receipts: &mut Vec<MfgReceiptV1>, receipt: MfgReceiptV1) {
    if let Some(index) = receipts
        .iter()
        .position(|existing| existing.receipt_id == receipt.receipt_id)
    {
        receipts.remove(index);
    }
    receipts.insert(0, receipt);
    receipts.truncate(50_000);
}

fn preserve_or_first(selected: Option<String>, items: &[MfgItemSummary]) -> Option<String> {
    selected
        .filter(|id| items.iter().any(|item| &item.id == id))
        .or_else(|| items.first().map(|item| item.id.clone()))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskSummary {
    pub id: String,
    pub objective: String,
    pub status: String,
    pub current_phase: Option<String>,
    pub yolo_mode: bool,
    pub failure_count: u64,
    pub review_result: Option<String>,
    pub artifact_count: u64,
    pub blocker_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApprovalSummary {
    pub id: String,
    pub tool_name: String,
    pub risk: Option<String>,
    pub requester: Option<String>,
    pub input_preview: String,
    pub source_kind: Option<String>,
    pub resource_ref: Option<String>,
    pub review_ref: Option<String>,
}

impl ApprovalSummary {
    #[must_use]
    pub fn is_mfg_source(&self) -> bool {
        self.source_kind.as_deref() == Some("mfg")
    }

    #[must_use]
    pub fn is_mfg_review(&self) -> bool {
        self.is_mfg_source()
            && self
                .review_ref
                .as_deref()
                .is_some_and(|review| !review.trim().is_empty())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorAccountSummary {
    pub provider: String,
    pub account_id: String,
    pub auth_mode: String,
    pub status: String,
    pub reason: Option<String>,
    pub binding_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorCapabilitySummary {
    pub capability_id: String,
    pub provider: String,
    pub plane: String,
    pub risk: String,
    pub supports_commit: bool,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorResourceSummary {
    pub reference: String,
    pub provider: String,
    pub resource_type: String,
    pub title: String,
    pub indexed_state: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeActionReceiptSummary {
    pub status: String,
    pub dispatch_status: String,
    pub mode: String,
    pub capability: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceSummary {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub status: String,
    pub lifecycle: String,
    pub transport: String,
    pub capability_count: u64,
    pub route_count: u64,
    pub resource_count: u64,
    pub active: bool,
    pub pid: Option<u64>,
    pub consecutive_failures: u64,
    pub restart_count: u64,
    pub circuit_open: bool,
    pub next_retry_at: Option<String>,
    pub last_error: Option<String>,
    pub entry: Option<String>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceHealthSummary {
    pub status: String,
    pub surface_count: u64,
    pub external_surface_count: u64,
    pub route_count: u64,
    pub resource_count: u64,
    pub ready_count: u64,
    pub degraded_count: u64,
    pub failed_count: u64,
    pub circuit_open_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceEventSummary {
    pub surface: String,
    pub event_type: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageConnectorSummary {
    pub connector: String,
    pub name: String,
    pub configuration_status: String,
    pub runtime_status: String,
    pub enabled: bool,
    pub configured: bool,
    pub capability_count: u64,
    pub missing_required_count: u64,
    pub consecutive_failures: u64,
    pub restart_count: u64,
    pub circuit_open: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageEndpointSummary {
    pub endpoint_id: String,
    pub connector: String,
    pub kind: String,
    pub status: String,
    pub configured: bool,
    pub capability_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageRouteSummary {
    pub route_id: String,
    pub connector: String,
    pub policy: String,
    pub status: String,
    pub configured: bool,
    pub capability_count: u64,
    pub runtime_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageBindingSummary {
    pub binding_id: String,
    pub connector: String,
    pub endpoint: String,
    pub direction: String,
    pub status: String,
    pub runtime_session_id: Option<String>,
    pub resource_count: u64,
    pub last_seen_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CowdKernelSummary {
    pub capability_count: u64,
    pub projection_capability_count: u64,
    pub webui_tui_full_parity: bool,
    pub cli_is_minimal_control: bool,
    pub release_gate_status: String,
    pub release_gate_failed_checks: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayCapabilityRouteSummary {
    pub id: String,
    pub domain: String,
    pub title: String,
    pub method: String,
    pub path: String,
    pub risk: String,
    pub criticality: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayOpenAiToolSummary {
    pub name: String,
    pub description: String,
    pub parameter_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayCapabilityContractSummary {
    pub kind: String,
    pub schema_version: u64,
    pub owner: String,
    pub route_count: u64,
    pub capability_count: u64,
    pub p1_count: u64,
    pub ai_visible_count: u64,
    pub openapi_path_count: u64,
    pub openai_tool_count: u64,
    pub route_contract_parity: bool,
    pub sample_routes: Vec<GatewayCapabilityRouteSummary>,
    pub sample_tools: Vec<GatewayOpenAiToolSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StructuredDataSummary {
    pub source_count: u64,
    pub fact_count: u64,
    pub evidence_count: u64,
    pub watermark_count: u64,
    pub sample_sources: Vec<String>,
    pub sample_facts: Vec<String>,
    pub sample_evidence: Vec<String>,
    pub sample_watermarks: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RealityCoreSummary {
    pub status: String,
    pub fact_status: String,
    pub memory_status: String,
    pub matrix_status: String,
    pub matrix_context_status: String,
    pub growth_status: String,
    pub context_status: String,
    pub audit_status: String,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FactFlowSummary {
    pub source: String,
    pub session_id: Option<String>,
    pub stage_count: u64,
    pub event_count: u64,
    pub promotion_count: u64,
    pub boundary_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MissionSessionSummary {
    pub session_id: String,
    pub title: String,
    pub status: String,
    pub team_count: u64,
    pub agent_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MissionControlSummary {
    pub active_session_id: Option<String>,
    pub session_count: u64,
    pub active_count: u64,
    pub background_count: u64,
    pub paused_count: u64,
    pub closed_count: u64,
    pub team_count: u64,
    pub agent_count: u64,
    pub pending_approvals: u64,
    pub relation_count: u64,
    pub execution_graph_count: u64,
    pub conflict_count: u64,
    pub evidence_count: u64,
    pub capability_action_count: u64,
    pub event_count: u64,
    pub control_ready_count: u64,
    pub control_blocked_count: u64,
    pub control_requires_approval_count: u64,
    pub sessions: Vec<MissionSessionSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeControlSnapshot {
    pub gateway_running: bool,
    pub active_sessions: usize,
    pub uptime_secs: Option<u64>,
    pub session_ids: Vec<String>,
    pub runtime_readiness: Option<String>,
    pub runtime_components: Option<u64>,
    pub task_count: Option<u64>,
    pub tasks: Vec<TaskSummary>,
    pub pending_approvals: Option<u64>,
    pub approval_items: Vec<ApprovalSummary>,
    pub lease_owner: Option<String>,
    pub lease_mode: Option<String>,
    pub memory_status: Option<String>,
    pub memory_total_entries: Option<usize>,
    pub memory_vector_count: Option<usize>,
    pub memory_layer_counts: [usize; 5],
    pub memory_context_envelope_status: Option<String>,
    pub memory_context_envelope_compression: Option<String>,
    pub memory_context_envelope_used_ratio: Option<u64>,
    pub memory_context_envelope_checkpoint: Option<String>,
    pub cross_plane_grants_active: Option<u64>,
    pub cross_plane_actions_24h: Option<u64>,
    pub connector_accounts: Vec<ConnectorAccountSummary>,
    pub connector_capabilities: Vec<ConnectorCapabilitySummary>,
    pub connector_resources: Vec<ConnectorResourceSummary>,
    pub action_receipts: Vec<RuntimeActionReceiptSummary>,
    pub surfaces: Vec<SurfaceSummary>,
    pub surface_health: Option<SurfaceHealthSummary>,
    pub surface_events: Vec<SurfaceEventSummary>,
    pub message_connectors: Vec<MessageConnectorSummary>,
    pub message_endpoints: Vec<MessageEndpointSummary>,
    pub message_routes: Vec<MessageRouteSummary>,
    pub message_bindings: Vec<MessageBindingSummary>,
    pub cowd_kernel: Option<CowdKernelSummary>,
    pub gateway_capability_contract: Option<GatewayCapabilityContractSummary>,
    pub structured_data: Option<StructuredDataSummary>,
    pub reality_core: Option<RealityCoreSummary>,
    pub fact_flow: Option<FactFlowSummary>,
    pub mission_control: Option<MissionControlSummary>,
    pub connector_degraded_reasons: Vec<String>,
    pub degraded_reasons: Vec<String>,
}

impl RuntimeControlSnapshot {
    pub fn from_gateway_snapshot(value: &serde_json::Value) -> Self {
        let session_ids = value
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut state = Self {
            gateway_running: value
                .get("ok")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true),
            active_sessions: value
                .get("active_sessions")
                .and_then(serde_json::Value::as_u64)
                .map(|count| count as usize)
                .unwrap_or(session_ids.len()),
            uptime_secs: value.get("uptime_secs").and_then(serde_json::Value::as_u64),
            session_ids,
            ..Self::default()
        };
        if let Some(lease) = value
            .pointer("/leases/items")
            .and_then(serde_json::Value::as_array)
            .and_then(|items| items.first())
        {
            state.apply_lease_value(lease);
        }
        state
    }

    pub fn from_app(app: &App) -> Self {
        Self {
            gateway_running: app.server_running,
            active_sessions: app.active_api_sessions,
            uptime_secs: app.server_uptime_secs,
            runtime_readiness: app.gateway_runtime_readiness.clone(),
            runtime_components: app.gateway_runtime_components,
            task_count: app.gateway_task_count,
            tasks: app.gateway_tasks.clone(),
            pending_approvals: app.gateway_pending_approvals,
            approval_items: app.gateway_approval_items.clone(),
            lease_owner: app.gateway_lease_owner.clone(),
            lease_mode: app.gateway_lease_mode.clone(),
            memory_status: app.memory_status.clone(),
            memory_total_entries: app.memory_total_entries,
            memory_vector_count: app.memory_vector_count,
            memory_layer_counts: app.memory_layer_counts,
            memory_context_envelope_status: app.memory_context_envelope_status.clone(),
            memory_context_envelope_compression: app.memory_context_envelope_compression.clone(),
            memory_context_envelope_used_ratio: app.memory_context_envelope_used_ratio,
            memory_context_envelope_checkpoint: app.memory_context_envelope_checkpoint.clone(),
            cross_plane_grants_active: app.gateway_cross_plane_grants_active,
            cross_plane_actions_24h: app.gateway_cross_plane_actions_24h,
            connector_accounts: app.gateway_connector_accounts.clone(),
            connector_capabilities: app.gateway_connector_capabilities.clone(),
            connector_resources: app.gateway_connector_resources.clone(),
            action_receipts: app.gateway_action_receipts.clone(),
            surfaces: app.gateway_surfaces.clone(),
            surface_health: app.gateway_surface_health.clone(),
            surface_events: app.gateway_surface_events.clone(),
            message_connectors: app.gateway_message_connectors.clone(),
            message_endpoints: app.gateway_message_endpoints.clone(),
            message_routes: app.gateway_message_routes.clone(),
            message_bindings: app.gateway_message_bindings.clone(),
            cowd_kernel: app.gateway_cowd_kernel.clone(),
            gateway_capability_contract: app.gateway_capability_contract.clone(),
            structured_data: app.gateway_structured_data.clone(),
            reality_core: app.gateway_reality_core.clone(),
            fact_flow: app.gateway_fact_flow.clone(),
            mission_control: app.gateway_mission_control.clone(),
            connector_degraded_reasons: app.gateway_connector_degraded_reasons.clone(),
            degraded_reasons: app.gateway_degraded_reasons.clone(),
            ..Self::default()
        }
    }

    pub fn apply_lease_value(&mut self, lease: &serde_json::Value) {
        self.lease_owner = lease
            .get("owner")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.lease_mode = lease
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
    }

    pub fn apply_to_app(&self, app: &mut App) {
        app.server_running = self.gateway_running;
        app.server_uptime_secs = self.uptime_secs;
        app.active_api_sessions = self.active_sessions;
        app.gateway_runtime_readiness = self.runtime_readiness.clone();
        app.gateway_runtime_components = self.runtime_components;
        app.gateway_task_count = self.task_count;
        app.gateway_tasks = self.tasks.clone();
        app.gateway_pending_approvals = self.pending_approvals;
        app.gateway_approval_items = self.approval_items.clone();
        app.memory_status = self.memory_status.clone();
        app.memory_total_entries = self.memory_total_entries;
        app.memory_vector_count = self.memory_vector_count;
        app.memory_layer_counts = self.memory_layer_counts;
        app.memory_context_envelope_status = self.memory_context_envelope_status.clone();
        app.memory_context_envelope_compression = self.memory_context_envelope_compression.clone();
        app.memory_context_envelope_used_ratio = self.memory_context_envelope_used_ratio;
        app.memory_context_envelope_checkpoint = self.memory_context_envelope_checkpoint.clone();
        app.gateway_cross_plane_grants_active = self.cross_plane_grants_active;
        app.gateway_cross_plane_actions_24h = self.cross_plane_actions_24h;
        app.gateway_connector_accounts = self.connector_accounts.clone();
        app.gateway_connector_capabilities = self.connector_capabilities.clone();
        app.gateway_connector_resources = self.connector_resources.clone();
        app.gateway_action_receipts = self.action_receipts.clone();
        app.gateway_surfaces = self.surfaces.clone();
        app.gateway_surface_health = self.surface_health.clone();
        app.gateway_surface_events = self.surface_events.clone();
        app.gateway_message_connectors = self.message_connectors.clone();
        app.gateway_message_endpoints = self.message_endpoints.clone();
        app.gateway_message_routes = self.message_routes.clone();
        app.gateway_message_bindings = self.message_bindings.clone();
        app.gateway_cowd_kernel = self.cowd_kernel.clone();
        app.gateway_capability_contract = self.gateway_capability_contract.clone();
        app.gateway_structured_data = self.structured_data.clone();
        app.gateway_reality_core = self.reality_core.clone();
        app.gateway_fact_flow = self.fact_flow.clone();
        app.gateway_mission_control = self.mission_control.clone();
        app.gateway_connector_degraded_reasons = self.connector_degraded_reasons.clone();
        app.gateway_degraded_reasons = self.degraded_reasons.clone();
        app.gateway_lease_owner = self.lease_owner.clone();
        app.gateway_lease_mode = self.lease_mode.clone();
    }

    pub fn ingest_session_ids(&mut self, session_ids: Vec<String>) {
        self.active_sessions = session_ids.len();
        self.session_ids = session_ids;
    }

    pub fn ingest_session_list(&mut self, value: &serde_json::Value) {
        let sessions = value
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.get("id")
                            .or_else(|| item.get("session_id"))
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.ingest_session_ids(sessions);
    }

    pub fn ingest_runtime_control_plane(&mut self, value: &serde_json::Value) {
        self.runtime_readiness = value
            .pointer("/readiness/score")
            .or_else(|| value.pointer("/diagnostics/readiness_score"))
            .and_then(serde_json::Value::as_u64)
            .map(|score| format!("{score}%"))
            .or_else(|| Some("unknown".to_string()));
        self.runtime_components = value
            .pointer("/diagnostics/component_count")
            .and_then(serde_json::Value::as_u64);
    }

    pub fn ingest_task_status(&mut self, value: &serde_json::Value) {
        self.tasks = value
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .map(|tasks| {
                tasks
                    .iter()
                    .filter_map(task_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.task_count = Some(self.tasks.len() as u64);
    }

    pub fn ingest_pending_approvals(&mut self, value: &serde_json::Value) {
        self.approval_items = value
            .as_array()
            .or_else(|| value.get("approvals").and_then(serde_json::Value::as_array))
            .or_else(|| value.get("pending").and_then(serde_json::Value::as_array))
            .map(|items| {
                items
                    .iter()
                    .filter_map(approval_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.pending_approvals = Some(self.approval_items.len() as u64);
    }

    pub fn ingest_memory_status(&mut self, value: &serde_json::Value) {
        self.memory_status = value
            .get("status")
            .or_else(|| value.pointer("/memory/status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.memory_total_entries = value
            .get("total_entries")
            .or_else(|| value.pointer("/memory/total_entries"))
            .or_else(|| {
                value
                    .get("entries")
                    .and_then(|entries| entries.get("total"))
            })
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as usize);
        self.memory_vector_count = value
            .get("vector_count")
            .or_else(|| value.pointer("/memory/vector_count"))
            .or_else(|| {
                value
                    .get("vectors")
                    .and_then(|vectors| vectors.get("total"))
            })
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as usize);
        self.memory_layer_counts = memory_layer_counts_from_json(value);
        let envelope = value
            .get("context_envelope_projection")
            .or_else(|| value.pointer("/memory/context_envelope_projection"));
        self.memory_context_envelope_status = envelope
            .and_then(|item| item.get("status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.memory_context_envelope_compression = envelope
            .and_then(|item| item.get("compression_status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.memory_context_envelope_used_ratio = envelope
            .and_then(|item| item.get("used_ratio"))
            .and_then(serde_json::Value::as_f64)
            .map(|ratio| (ratio * 100.0).round().clamp(0.0, 100.0) as u64);
        self.memory_context_envelope_checkpoint = envelope
            .and_then(|item| item.get("latest_checkpoint_id"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
    }

    pub fn ingest_cross_plane_summary(&mut self, value: &serde_json::Value) {
        self.cross_plane_grants_active = value
            .pointer("/grants/active")
            .and_then(serde_json::Value::as_u64);
        self.cross_plane_actions_24h = value
            .pointer("/interop/actions_24h")
            .and_then(serde_json::Value::as_u64);
    }

    pub fn ingest_cowd_projection_state(
        &mut self,
        capabilities: &serde_json::Value,
        projection: &serde_json::Value,
        surfaces: &serde_json::Value,
        release_gate: &serde_json::Value,
    ) {
        let capability_count = capabilities
            .get("capability_count")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                capabilities
                    .get("capabilities")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| items.len() as u64)
            })
            .unwrap_or_default();
        let projection_capability_count = projection
            .get("capability_count")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                projection
                    .get("capabilities")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| items.len() as u64)
            })
            .unwrap_or_default();
        let release_gate_failed_checks = release_gate
            .get("checks")
            .and_then(serde_json::Value::as_array)
            .map(|checks| {
                checks
                    .iter()
                    .filter(|check| {
                        check.get("status").and_then(serde_json::Value::as_str) != Some("pass")
                    })
                    .count() as u64
            })
            .unwrap_or_default();
        self.cowd_kernel = Some(CowdKernelSummary {
            capability_count,
            projection_capability_count,
            webui_tui_full_parity: surfaces
                .get("webui_tui_full_parity")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            cli_is_minimal_control: surfaces
                .get("cli_is_minimal_control")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            release_gate_status: release_gate
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            release_gate_failed_checks,
        });
    }

    pub fn ingest_gateway_capability_contract(
        &mut self,
        contract: &serde_json::Value,
        openai_tools: &serde_json::Value,
    ) {
        let coverage = contract.get("coverage").unwrap_or(&serde_json::Value::Null);
        let tools = openai_tools
            .get("tools")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let sample_tools = tools
            .iter()
            .filter_map(gateway_openai_tool_summary)
            .take(8)
            .collect::<Vec<_>>();
        let sample_routes = contract
            .get("capabilities")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter(|item| {
                        item.pointer("/surface_visibility/tui")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                    })
                    .filter_map(gateway_capability_route_summary)
                    .take(14)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let contract_tool_count = coverage
            .get("openai_tool_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let actual_tool_count = openai_tools
            .get("tool_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(tools.len() as u64);
        if contract_tool_count != 0 && actual_tool_count != contract_tool_count {
            self.degrade(format!(
                "gateway openai tools count mismatch: contract={contract_tool_count}, tools={actual_tool_count}"
            ));
        }
        self.gateway_capability_contract = Some(GatewayCapabilityContractSummary {
            kind: contract
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("gateway.capability_contract")
                .to_string(),
            schema_version: contract
                .get("schema_version")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            owner: contract
                .get("owner")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("gateway")
                .to_string(),
            route_count: coverage
                .get("route_count")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| {
                    contract
                        .get("route_count")
                        .and_then(serde_json::Value::as_u64)
                })
                .unwrap_or_default(),
            capability_count: coverage
                .get("capability_count")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| {
                    contract
                        .get("capability_count")
                        .and_then(serde_json::Value::as_u64)
                })
                .unwrap_or_default(),
            p1_count: coverage
                .get("p1_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            ai_visible_count: coverage
                .get("ai_visible_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            openapi_path_count: coverage
                .get("openapi_path_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            openai_tool_count: actual_tool_count,
            route_contract_parity: coverage
                .get("route_contract_parity")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            sample_routes,
            sample_tools,
        });
    }

    pub fn ingest_structured_data(
        &mut self,
        sources: &serde_json::Value,
        facts: &serde_json::Value,
        evidence: &serde_json::Value,
        watermarks: &serde_json::Value,
    ) {
        self.structured_data = Some(StructuredDataSummary {
            source_count: structured_count(sources),
            fact_count: structured_count(facts),
            evidence_count: structured_count(evidence),
            watermark_count: structured_count(watermarks),
            sample_sources: structured_samples(sources, &["source_id", "source_ref", "id"]),
            sample_facts: structured_samples(facts, &["fact_id", "id"]),
            sample_evidence: structured_samples(evidence, &["evidence_id", "id"]),
            sample_watermarks: structured_samples(watermarks, &["source_ref", "id"]),
        });
    }

    pub fn ingest_reality_status(&mut self, value: &serde_json::Value) {
        self.reality_core = Some(RealityCoreSummary {
            status: value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            fact_status: value
                .pointer("/capabilities/fact_runtime/status")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| reality_component_status(value, "fact_kernel")),
            memory_status: reality_component_status(value, "memory"),
            matrix_status: reality_component_status(value, "matrix"),
            matrix_context_status: value
                .pointer("/capabilities/matrix_context_source/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            growth_status: reality_component_status(value, "growth"),
            context_status: reality_component_status(value, "context"),
            audit_status: reality_component_status(value, "audit"),
            degraded_reasons: value
                .get("degraded_reasons")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        });
    }

    pub fn ingest_fact_flow(
        &mut self,
        flow: &serde_json::Value,
        boundaries: Option<&serde_json::Value>,
    ) {
        self.fact_flow = Some(FactFlowSummary {
            source: flow
                .get("source")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            session_id: flow
                .get("session_id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            stage_count: json_array_len(flow, "stages"),
            event_count: json_array_len(flow, "events"),
            promotion_count: json_array_len(flow, "promotions"),
            boundary_count: boundaries
                .map(|value| json_array_len(value, "boundaries"))
                .unwrap_or_default(),
        });
    }

    pub fn ingest_mission_projection(&mut self, value: &serde_json::Value) {
        let projection = value.get("projection").unwrap_or(value);
        let mission = projection.get("mission").unwrap_or(projection);
        let sessions = mission
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(mission_session_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut active_count = 0;
        let mut background_count = 0;
        let mut paused_count = 0;
        let mut closed_count = 0;
        let mut team_count = 0;
        let mut agent_count = 0;
        for session in &sessions {
            match session.status.as_str() {
                "active" => active_count += 1,
                "background" => background_count += 1,
                "paused" => paused_count += 1,
                "closed" => closed_count += 1,
                _ => {}
            }
            team_count += session.team_count;
            agent_count += session.agent_count;
        }
        self.mission_control = Some(MissionControlSummary {
            active_session_id: mission
                .get("active_session_id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            session_count: sessions.len() as u64,
            active_count,
            background_count,
            paused_count,
            closed_count,
            team_count,
            agent_count,
            pending_approvals: mission
                .pointer("/approval_projection/pending_count")
                .or_else(|| projection.pointer("/approvals/pending_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            relation_count: mission
                .pointer("/relation_projection/relation_count")
                .or_else(|| projection.pointer("/relations/relation_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            execution_graph_count: mission
                .pointer("/execution_graph_projection/count")
                .or_else(|| projection.pointer("/execution_graphs/count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            conflict_count: mission
                .pointer("/conflict_projection/count")
                .or_else(|| projection.pointer("/conflicts/count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            evidence_count: mission
                .pointer("/evidence_projection/count")
                .or_else(|| projection.pointer("/evidence/count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            capability_action_count: mission
                .pointer("/capability_projection/action_contracts")
                .or_else(|| projection.pointer("/capabilities/action_contracts"))
                .and_then(serde_json::Value::as_array)
                .map(|items| items.len() as u64)
                .unwrap_or_default(),
            event_count: mission
                .get("events")
                .and_then(serde_json::Value::as_array)
                .map(|events| events.len() as u64)
                .or_else(|| {
                    projection
                        .pointer("/event_digest/latest")
                        .and_then(serde_json::Value::as_array)
                        .map(|events| events.len() as u64)
                })
                .unwrap_or_default(),
            control_ready_count: projection
                .pointer("/control_readiness/ready_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            control_blocked_count: projection
                .pointer("/control_readiness/blocked_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            control_requires_approval_count: projection
                .pointer("/control_readiness/actions")
                .and_then(serde_json::Value::as_array)
                .map(|actions| {
                    actions
                        .iter()
                        .filter(|action| {
                            action
                                .get("requires_approval")
                                .and_then(serde_json::Value::as_bool)
                                .unwrap_or(false)
                        })
                        .count() as u64
                })
                .unwrap_or_default(),
            sessions,
        });
    }

    pub fn ingest_connector_accounts(&mut self, value: &serde_json::Value) {
        self.connector_accounts = value
            .get("accounts")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(connector_account_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_connector_capabilities(&mut self, value: &serde_json::Value) {
        self.connector_capabilities = value
            .get("capabilities")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(connector_capability_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_connector_resources(&mut self, value: &serde_json::Value) {
        self.connector_resources = value
            .get("resources")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(connector_resource_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(reason) = value
            .get("degraded_reason")
            .and_then(serde_json::Value::as_str)
            .filter(|reason| !reason.trim().is_empty())
        {
            self.connector_degraded_reasons.push(reason.to_string());
        }
    }

    pub fn ingest_surface_registry(&mut self, value: &serde_json::Value) {
        self.surfaces = value
            .pointer("/registry/surfaces")
            .or_else(|| value.pointer("/surfaces"))
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(surface_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_surface_health(&mut self, value: &serde_json::Value) {
        let host = value.get("host").unwrap_or(value);
        self.surface_health = Some(SurfaceHealthSummary {
            status: host
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| {
                    value
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                })
                .to_string(),
            surface_count: host
                .get("surface_count")
                .or_else(|| value.get("surface_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(self.surfaces.len() as u64),
            external_surface_count: host
                .get("external_surface_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    self.surfaces
                        .iter()
                        .filter(|surface| surface.entry.is_some())
                        .count() as u64
                }),
            route_count: host
                .get("route_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    self.surfaces
                        .iter()
                        .map(|surface| surface.route_count)
                        .sum()
                }),
            resource_count: host
                .get("resource_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    self.surfaces
                        .iter()
                        .map(|surface| surface.resource_count)
                        .sum()
                }),
            ready_count: host
                .get("ready_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            degraded_count: host
                .get("degraded_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            failed_count: host
                .get("failed_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            circuit_open_count: host
                .get("circuit_open_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
        });
        if let Some(runtime) = value.get("runtime").and_then(serde_json::Value::as_array) {
            for item in runtime {
                let Some(surface_id) = item.get("surface").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                if let Some(surface) = self
                    .surfaces
                    .iter_mut()
                    .find(|surface| surface.id == surface_id)
                {
                    apply_surface_runtime(surface, item);
                }
            }
        }
    }

    pub fn ingest_surface_events(&mut self, surface: &str, value: &serde_json::Value) {
        let mut events = value
            .get("events")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| surface_event_summary_from_json(surface, item))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(supervisor_events) = value
            .get("supervisor_events")
            .and_then(serde_json::Value::as_array)
        {
            events.extend(
                supervisor_events
                    .iter()
                    .filter_map(|item| surface_event_summary_from_json(surface, item)),
            );
        }
        self.surface_events.append(&mut events);
        self.surface_events.truncate(24);
    }

    pub fn ingest_message_connectors(&mut self, value: &serde_json::Value) {
        self.message_connectors = value
            .get("connectors")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_connector_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_message_endpoints(&mut self, value: &serde_json::Value) {
        self.message_endpoints = value
            .get("endpoints")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_endpoint_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_message_routes(&mut self, value: &serde_json::Value) {
        self.message_routes = value
            .get("routes")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_route_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_message_bindings(&mut self, value: &serde_json::Value) {
        self.message_bindings = value
            .get("bindings")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_binding_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn begin_surface_event_refresh(&mut self) {
        self.surface_events.clear();
    }

    pub fn degrade(&mut self, reason: impl Into<String>) {
        self.degraded_reasons.push(reason.into());
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeControlLocalStore {
    snapshot: RuntimeControlSnapshot,
}

impl RuntimeControlLocalStore {
    pub fn from_app(app: &App) -> Self {
        Self {
            snapshot: RuntimeControlSnapshot::from_app(app),
        }
    }

    pub fn snapshot(&self) -> &RuntimeControlSnapshot {
        &self.snapshot
    }

    pub fn apply_to_app(&self, app: &mut App) {
        self.snapshot.apply_to_app(app);
    }

    pub fn apply_connector_resource_state(&mut self, reference: &str, state: &str) {
        for resource in &mut self.snapshot.connector_resources {
            if resource.reference == reference {
                resource.indexed_state = state.to_string();
            }
        }
    }

    pub fn push_action_receipt(
        &mut self,
        status: &str,
        dispatch_status: &str,
        mode: &str,
        capability: &str,
        idempotency_key: Option<String>,
    ) {
        self.snapshot.action_receipts.insert(
            0,
            RuntimeActionReceiptSummary {
                status: status.to_string(),
                dispatch_status: truncate_receipt_field(dispatch_status, 80),
                mode: mode.to_string(),
                capability: capability.to_string(),
                idempotency_key,
            },
        );
        self.snapshot.action_receipts.truncate(8);
    }
}

fn truncate_receipt_field(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn memory_layer_counts_from_json(value: &serde_json::Value) -> [usize; 5] {
    let mut counts = [0; 5];
    let layers = value
        .get("layers")
        .or_else(|| value.pointer("/memory/layers"))
        .and_then(serde_json::Value::as_array);
    if let Some(layers) = layers {
        for (fallback_idx, layer) in layers.iter().enumerate() {
            let count = layer
                .get("entry_count")
                .or_else(|| layer.get("count"))
                .or_else(|| layer.get("entries"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            let idx = layer
                .get("layer")
                .or_else(|| layer.get("name"))
                .or_else(|| layer.get("id"))
                .and_then(serde_json::Value::as_str)
                .and_then(memory_layer_index_from_str)
                .unwrap_or(fallback_idx);
            if idx < counts.len() {
                counts[idx] = count;
            }
        }
    }
    counts
}

fn memory_layer_index_from_str(value: &str) -> Option<usize> {
    let normalized = value.trim().to_ascii_uppercase();
    let mut chars = normalized.chars();
    while let Some(ch) = chars.next() {
        if ch == 'L' {
            if let Some(digit) = chars.next().and_then(|next| next.to_digit(10)) {
                let idx = digit as usize;
                if idx < 5 {
                    return Some(idx);
                }
            }
        }
    }
    None
}

fn task_summary_from_json(value: &serde_json::Value) -> Option<TaskSummary> {
    let id = value.get("id").and_then(serde_json::Value::as_str)?;
    let objective = value
        .get("objective")
        .or_else(|| value.get("title"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let status = value
        .get("status")
        .or_else(|| value.get("phase"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let current_phase = value
        .get("current_phase")
        .or_else(|| value.get("currentPhase"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let yolo_mode = value
        .get("yolo_mode")
        .or_else(|| value.get("yoloMode"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let failure_count = value
        .get("failure_count")
        .or_else(|| value.get("failureCount"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let review_result = value
        .get("review_result")
        .or_else(|| value.get("reviewResult"))
        .or_else(|| value.get("review"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let artifact_count = value
        .get("artifact_count")
        .or_else(|| value.get("artifactCount"))
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            value
                .get("artifacts")
                .and_then(serde_json::Value::as_array)
                .map(|items| items.len() as u64)
        })
        .unwrap_or_default();
    let blocker_reason = value
        .get("blocker_reason")
        .or_else(|| value.get("blockerReason"))
        .or_else(|| value.get("blocker"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    Some(TaskSummary {
        id: id.to_string(),
        objective,
        status,
        current_phase,
        yolo_mode,
        failure_count,
        review_result,
        artifact_count,
        blocker_reason,
    })
}

fn approval_summary_from_json(value: &serde_json::Value) -> Option<ApprovalSummary> {
    let id = value
        .get("id")
        .or_else(|| value.get("approval_id"))
        .or_else(|| value.get("approvalId"))
        .and_then(serde_json::Value::as_str)?;
    let tool_name = value
        .get("tool_name")
        .or_else(|| value.get("toolName"))
        .or_else(|| value.get("tool"))
        .or_else(|| value.get("capability"))
        .or_else(|| value.get("action"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let risk = value
        .get("risk")
        .or_else(|| value.get("risk_level"))
        .or_else(|| value.get("riskLevel"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let source = value.get("source");
    let requester = value
        .get("requester")
        .or_else(|| value.get("session_id"))
        .or_else(|| value.get("sessionId"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            source.and_then(|source| {
                [
                    "session_id",
                    "agent_id",
                    "team_id",
                    "mission_id",
                    "resource_ref",
                ]
                .into_iter()
                .find_map(|key| source.get(key).and_then(serde_json::Value::as_str))
                .map(ToOwned::to_owned)
            })
        });
    let input_preview = value
        .get("input_preview")
        .or_else(|| value.get("inputPreview"))
        .or_else(|| value.get("preview"))
        .or_else(|| value.get("command"))
        .or_else(|| value.get("summary"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(ApprovalSummary {
        id: id.to_string(),
        tool_name,
        risk,
        requester,
        input_preview,
        source_kind: source
            .and_then(|source| source.get("kind"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        resource_ref: source
            .and_then(|source| source.get("resource_ref"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        review_ref: source
            .and_then(|source| source.get("review_ref"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn structured_count(value: &serde_json::Value) -> u64 {
    value
        .get("count")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            value
                .get("items")
                .and_then(serde_json::Value::as_array)
                .map(|items| items.len() as u64)
        })
        .unwrap_or_default()
}

fn structured_samples(value: &serde_json::Value, keys: &[&str]) -> Vec<String> {
    value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    keys.iter()
                        .filter_map(|key| item.get(*key).and_then(serde_json::Value::as_str))
                        .find(|sample| !sample.trim().is_empty())
                        .map(ToOwned::to_owned)
                })
                .take(4)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn gateway_capability_route_summary(
    value: &serde_json::Value,
) -> Option<GatewayCapabilityRouteSummary> {
    let http = value.get("http")?;
    Some(GatewayCapabilityRouteSummary {
        id: value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        domain: value
            .get("domain")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("gateway")
            .to_string(),
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        method: http
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("GET")
            .to_string(),
        path: http
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        risk: value
            .get("risk")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        criticality: http
            .get("criticality")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("p2")
            .to_string(),
    })
}

fn gateway_openai_tool_summary(value: &serde_json::Value) -> Option<GatewayOpenAiToolSummary> {
    let function = value.get("function")?;
    let parameters = function
        .get("parameters")
        .and_then(|item| item.get("properties"))
        .and_then(serde_json::Value::as_object)
        .map(|properties| properties.len() as u64)
        .unwrap_or_default();
    Some(GatewayOpenAiToolSummary {
        name: function
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        description: function
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string(),
        parameter_count: parameters,
    })
}

fn reality_component_status(value: &serde_json::Value, component: &str) -> String {
    value
        .get("engines")
        .and_then(|engines| engines.get(component))
        .and_then(|engine| engine.get("status"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            value
                .get(component)
                .and_then(|engine| engine.get("status"))
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("unknown")
        .to_string()
}

fn mission_session_from_json(value: &serde_json::Value) -> Option<MissionSessionSummary> {
    let session_id = value
        .get("session_id")
        .or_else(|| value.get("id"))
        .and_then(serde_json::Value::as_str)?;
    Some(MissionSessionSummary {
        session_id: session_id.to_string(),
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(session_id)
            .to_string(),
        status: value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        team_count: value
            .get("active_team_ids")
            .or_else(|| value.get("team_ids"))
            .and_then(serde_json::Value::as_array)
            .map(|items| items.len() as u64)
            .unwrap_or_default(),
        agent_count: value
            .get("active_agent_ids")
            .or_else(|| value.get("agent_ids"))
            .and_then(serde_json::Value::as_array)
            .map(|items| items.len() as u64)
            .unwrap_or_default(),
    })
}

fn json_array_len(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default()
}

fn connector_account_from_json(value: &serde_json::Value) -> Option<ConnectorAccountSummary> {
    let provider = value.get("provider").and_then(serde_json::Value::as_str)?;
    let account_id = value
        .get("account_id")
        .or_else(|| value.get("accountId"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(provider);
    let health = value.get("health").unwrap_or(value);
    Some(ConnectorAccountSummary {
        provider: provider.to_string(),
        account_id: account_id.to_string(),
        auth_mode: value
            .get("auth_mode")
            .or_else(|| value.get("authMode"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        status: health
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        reason: health
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        binding_count: value
            .get("enabled_bindings")
            .or_else(|| value.get("enabledBindings"))
            .and_then(serde_json::Value::as_array)
            .map(|items| items.len() as u64)
            .unwrap_or_default(),
    })
}

fn connector_capability_from_json(value: &serde_json::Value) -> Option<ConnectorCapabilitySummary> {
    let capability_id = value
        .get("capability_id")
        .or_else(|| value.get("capabilityId"))
        .and_then(serde_json::Value::as_str)?;
    Some(ConnectorCapabilitySummary {
        capability_id: capability_id.to_string(),
        provider: value
            .get("provider")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        plane: value
            .get("plane")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        risk: value
            .get("risk")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        supports_commit: value
            .get("supports_commit")
            .or_else(|| value.get("supportsCommit"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        requires_approval: value
            .get("requires_approval")
            .or_else(|| value.get("requiresApproval"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    })
}

fn connector_resource_from_json(value: &serde_json::Value) -> Option<ConnectorResourceSummary> {
    let reference = value.get("reference").and_then(serde_json::Value::as_str)?;
    Some(ConnectorResourceSummary {
        reference: reference.to_string(),
        provider: value
            .get("provider")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        resource_type: value
            .get("resource_type")
            .or_else(|| value.get("resourceType"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("resource")
            .to_string(),
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(reference)
            .to_string(),
        indexed_state: value
            .get("indexed_state")
            .or_else(|| value.get("indexedState"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
    })
}

fn surface_summary_from_json(value: &serde_json::Value) -> Option<SurfaceSummary> {
    let id = value.get("id").and_then(serde_json::Value::as_str)?;
    let capabilities = value
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default();
    let routes = value
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default();
    let resources = value
        .get("resources")
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default();
    Some(SurfaceSummary {
        id: id.to_string(),
        name: value
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(id)
            .to_string(),
        kind: value
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        status: value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        lifecycle: value
            .get("lifecycle")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        transport: value
            .get("transport")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("stdio-jsonl")
            .to_string(),
        capability_count: capabilities,
        route_count: routes,
        resource_count: resources,
        active: false,
        pid: None,
        consecutive_failures: 0,
        restart_count: 0,
        circuit_open: false,
        next_retry_at: None,
        last_error: None,
        entry: value
            .get("entry")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        diagnostics: value
            .get("diagnostics")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .take(3)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    })
}

fn apply_surface_runtime(surface: &mut SurfaceSummary, value: &serde_json::Value) {
    if let Some(status) = value.get("status").and_then(serde_json::Value::as_str) {
        surface.status = status.to_string();
    }
    surface.active = value
        .get("active")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(surface.active);
    surface.pid = value.get("pid").and_then(serde_json::Value::as_u64);
    surface.consecutive_failures = value
        .get("consecutive_failures")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    surface.restart_count = value
        .get("restart_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    surface.circuit_open = value
        .get("circuit_open")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_default();
    surface.next_retry_at = value
        .get("next_retry_at")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    surface.last_error = value
        .pointer("/last_error/message")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
}

fn surface_event_summary_from_json(
    fallback_surface: &str,
    value: &serde_json::Value,
) -> Option<SurfaceEventSummary> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("status"))
        .and_then(serde_json::Value::as_str)?;
    let surface = value
        .get("surface")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback_surface);
    let detail = value
        .get("message")
        .or_else(|| value.get("code"))
        .or_else(|| value.get("payload"))
        .map(|item| match item.as_str() {
            Some(text) => text.to_string(),
            None => truncate_json(item),
        })
        .unwrap_or_default();
    Some(SurfaceEventSummary {
        surface: surface.to_string(),
        event_type: event_type.to_string(),
        detail,
    })
}

fn message_connector_from_json(value: &serde_json::Value) -> Option<MessageConnectorSummary> {
    let connector = json_string(value, &["connector", "id", "platform_type"])?;
    let runtime = value.get("runtime").unwrap_or(&serde_json::Value::Null);
    Some(MessageConnectorSummary {
        name: json_string(value, &["name"]).unwrap_or_else(|| connector.clone()),
        configuration_status: json_string(value, &["configuration_status", "status"])
            .unwrap_or_else(|| "unknown".to_string()),
        runtime_status: json_string(runtime, &["status"]).unwrap_or_else(|| {
            if runtime.is_null() {
                "not_running".to_string()
            } else {
                "unknown".to_string()
            }
        }),
        enabled: value
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        configured: value
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        capability_count: json_array_len(value, "capabilities"),
        missing_required_count: json_array_len(value, "missing_required"),
        consecutive_failures: runtime
            .get("consecutive_failures")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        restart_count: runtime
            .get("restart_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        circuit_open: runtime
            .get("circuit_open")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        connector,
    })
}

fn message_endpoint_from_json(value: &serde_json::Value) -> Option<MessageEndpointSummary> {
    let endpoint_id = json_string(value, &["endpoint_id", "id"])?;
    Some(MessageEndpointSummary {
        connector: json_string(value, &["connector"]).unwrap_or_else(|| "unknown".to_string()),
        kind: json_string(value, &["kind"]).unwrap_or_else(|| "unknown".to_string()),
        status: json_string(value, &["status"]).unwrap_or_else(|| "unknown".to_string()),
        configured: value
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        capability_count: json_array_len(value, "capabilities"),
        endpoint_id,
    })
}

fn message_route_from_json(value: &serde_json::Value) -> Option<MessageRouteSummary> {
    let route_id = json_string(value, &["route_id", "id"])?;
    let runtime = value.get("runtime").unwrap_or(&serde_json::Value::Null);
    Some(MessageRouteSummary {
        connector: json_string(value, &["connector"]).unwrap_or_else(|| "unknown".to_string()),
        policy: json_string(value, &["policy"]).unwrap_or_else(|| "origin".to_string()),
        status: json_string(value, &["status"]).unwrap_or_else(|| "unknown".to_string()),
        configured: value
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        capability_count: json_array_len(value, "capabilities"),
        runtime_status: json_string(runtime, &["status"]).unwrap_or_else(|| {
            if runtime.is_null() {
                "not_running".to_string()
            } else {
                "unknown".to_string()
            }
        }),
        route_id,
    })
}

fn message_binding_from_json(value: &serde_json::Value) -> Option<MessageBindingSummary> {
    let binding_id = json_string(value, &["binding_id", "id"])?;
    Some(MessageBindingSummary {
        connector: json_string(value, &["connector"]).unwrap_or_else(|| "unknown".to_string()),
        endpoint: json_string(value, &["endpoint"]).unwrap_or_else(|| "-".to_string()),
        direction: json_string(value, &["direction"]).unwrap_or_else(|| "unknown".to_string()),
        status: json_string(value, &["status", "outbound_status"])
            .unwrap_or_else(|| "unknown".to_string()),
        runtime_session_id: json_string(value, &["runtime_session_id", "source_session_id"]),
        resource_count: value
            .get("resource_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        last_seen_at_ms: value
            .get("last_seen_at_ms")
            .and_then(serde_json::Value::as_u64),
        binding_id,
    })
}

fn json_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .find(|item| !item.is_empty())
        .map(ToOwned::to_owned)
}

fn truncate_json(value: &serde_json::Value) -> String {
    let rendered = value.to_string();
    if rendered.chars().count() <= 96 {
        rendered
    } else {
        format!("{}...", rendered.chars().take(93).collect::<String>())
    }
}

pub async fn refresh_runtime_control_snapshot(
    gateway_client: Option<&GatewayApiClient>,
    session_id: Option<&str>,
) -> RuntimeControlSnapshot {
    let Some(projection) = gateway_client else {
        let mut snapshot = RuntimeControlSnapshot::default();
        snapshot.degrade("Gateway API unavailable");
        return snapshot;
    };

    let mut snapshot = match projection.runtime_snapshot().await {
        Ok(value) => RuntimeControlSnapshot::from_gateway_snapshot(&value),
        Err(err) => {
            let mut snapshot = RuntimeControlSnapshot::default();
            snapshot.degrade(format!("runtime host snapshot unavailable: {err}"));
            snapshot
        }
    };

    if snapshot.session_ids.is_empty() {
        match projection.list_sessions().await {
            Ok(value) => snapshot.ingest_session_list(&value),
            Err(err) => snapshot.degrade(format!("session list unavailable: {err}")),
        }
    }

    match projection.runtime_control_plane().await {
        Ok(value) => snapshot.ingest_runtime_control_plane(&value),
        Err(err) => snapshot.degrade(format!("Gateway API unavailable: {err}")),
    }
    match projection.task_status().await {
        Ok(value) => snapshot.ingest_task_status(&value),
        Err(err) => snapshot.degrade(format!("task Gateway API unavailable: {err}")),
    }
    match projection.pending_approvals().await {
        Ok(value) => snapshot.ingest_pending_approvals(&value),
        Err(err) => snapshot.degrade(format!("approval Gateway API unavailable: {err}")),
    }
    match projection.mission_control().await {
        Ok(value) => snapshot.ingest_mission_projection(&value),
        Err(err) => snapshot.degrade(format!("mission control projection unavailable: {err}")),
    }
    match projection.memory_status().await {
        Ok(value) => snapshot.ingest_memory_status(&value),
        Err(err) => snapshot.degrade(format!("memory Gateway API unavailable: {err}")),
    }
    let (reality_status, reality_flow, reality_boundaries) = tokio::join!(
        projection.reality_status(),
        projection.reality_flow(session_id),
        projection.reality_boundaries()
    );
    match (reality_status, reality_flow, reality_boundaries) {
        (Ok(status), Ok(flow), Ok(boundaries)) => {
            snapshot.ingest_reality_status(&status);
            snapshot.ingest_fact_flow(&flow, Some(&boundaries));
        }
        (status, flow, boundaries) => {
            let mut reasons = Vec::new();
            if let Err(err) = status {
                reasons.push(format!("status: {err}"));
            }
            if let Err(err) = flow {
                reasons.push(format!("fact flow: {err}"));
            }
            if let Err(err) = boundaries {
                reasons.push(format!("boundaries: {err}"));
            }
            snapshot.degrade(format!(
                "reality core projection unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    let (capabilities, projection_state, surfaces, release_gate) = tokio::join!(
        projection.cowd_capabilities(),
        projection.cowd_projection("tui"),
        projection.cowd_surfaces(),
        projection.cowd_release_gate()
    );
    match (capabilities, projection_state, surfaces, release_gate) {
        (Ok(capabilities), Ok(projection_state), Ok(surfaces), Ok(release_gate)) => snapshot
            .ingest_cowd_projection_state(
                &capabilities,
                &projection_state,
                &surfaces,
                &release_gate,
            ),
        (capabilities, projection_state, surfaces, release_gate) => {
            let mut reasons = Vec::new();
            if let Err(err) = capabilities {
                reasons.push(format!("capabilities: {err}"));
            }
            if let Err(err) = projection_state {
                reasons.push(format!("projection: {err}"));
            }
            if let Err(err) = surfaces {
                reasons.push(format!("surfaces: {err}"));
            }
            if let Err(err) = release_gate {
                reasons.push(format!("release gate: {err}"));
            }
            snapshot.degrade(format!(
                "cowd kernel projection unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    let (gateway_contract, openai_tools) = tokio::join!(
        projection.gateway_capability_contract(),
        projection.gateway_openai_tools()
    );
    match (gateway_contract, openai_tools) {
        (Ok(contract), Ok(tools)) => snapshot.ingest_gateway_capability_contract(&contract, &tools),
        (contract, tools) => {
            let mut reasons = Vec::new();
            if let Err(err) = contract {
                reasons.push(format!("contract: {err}"));
            }
            if let Err(err) = tools {
                reasons.push(format!("openai tools: {err}"));
            }
            snapshot.degrade(format!(
                "gateway capability contract unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    let (sources, facts, evidence, watermarks) = tokio::join!(
        projection.structured_sources(),
        projection.structured_facts(),
        projection.structured_evidence(),
        projection.structured_watermarks()
    );
    match (sources, facts, evidence, watermarks) {
        (Ok(sources), Ok(facts), Ok(evidence), Ok(watermarks)) => {
            snapshot.ingest_structured_data(&sources, &facts, &evidence, &watermarks);
        }
        (sources, facts, evidence, watermarks) => {
            let mut reasons = Vec::new();
            if let Err(err) = sources {
                reasons.push(format!("sources: {err}"));
            }
            if let Err(err) = facts {
                reasons.push(format!("facts: {err}"));
            }
            if let Err(err) = evidence {
                reasons.push(format!("evidence: {err}"));
            }
            if let Err(err) = watermarks {
                reasons.push(format!("watermarks: {err}"));
            }
            snapshot.degrade(format!(
                "structured data projection unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    match projection.cross_plane_summary().await {
        Ok(value) => snapshot.ingest_cross_plane_summary(&value),
        Err(err) => snapshot.degrade(format!("cross-plane projection unavailable: {err}")),
    }
    match projection.connector_accounts().await {
        Ok(value) => snapshot.ingest_connector_accounts(&value),
        Err(err) => snapshot.degrade(format!("connector accounts unavailable: {err}")),
    }
    match projection.connector_capabilities().await {
        Ok(value) => snapshot.ingest_connector_capabilities(&value),
        Err(err) => snapshot.degrade(format!("connector capabilities unavailable: {err}")),
    }
    match projection.connector_resources(None, 20, 0).await {
        Ok(value) => snapshot.ingest_connector_resources(&value),
        Err(err) => snapshot.degrade(format!("connector resources unavailable: {err}")),
    }
    let (message_connectors, message_endpoints, message_routes, message_bindings) = tokio::join!(
        projection.message_connectors(),
        projection.message_endpoints(),
        projection.message_routes(),
        projection.message_bindings()
    );
    match (
        message_connectors,
        message_endpoints,
        message_routes,
        message_bindings,
    ) {
        (Ok(connectors), Ok(endpoints), Ok(routes), Ok(bindings)) => {
            snapshot.ingest_message_connectors(&connectors);
            snapshot.ingest_message_endpoints(&endpoints);
            snapshot.ingest_message_routes(&routes);
            snapshot.ingest_message_bindings(&bindings);
        }
        (connectors, endpoints, routes, bindings) => {
            let mut reasons = Vec::new();
            if let Err(err) = connectors {
                reasons.push(format!("connectors: {err}"));
            }
            if let Err(err) = endpoints {
                reasons.push(format!("endpoints: {err}"));
            }
            if let Err(err) = routes {
                reasons.push(format!("routes: {err}"));
            }
            if let Err(err) = bindings {
                reasons.push(format!("bindings: {err}"));
            }
            snapshot.degrade(format!(
                "message plane projection unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    match projection.surface_registry().await {
        Ok(value) => snapshot.ingest_surface_registry(&value),
        Err(err) => snapshot.degrade(format!("surface registry unavailable: {err}")),
    }
    match projection.surface_health_summary().await {
        Ok(value) => snapshot.ingest_surface_health(&value),
        Err(err) => snapshot.degrade(format!("surface health unavailable: {err}")),
    }
    let surface_ids = snapshot
        .surfaces
        .iter()
        .map(|surface| surface.id.clone())
        .take(6)
        .collect::<Vec<_>>();
    snapshot.begin_surface_event_refresh();
    for surface_id in surface_ids {
        match projection.surface_events(&surface_id).await {
            Ok(value) => snapshot.ingest_surface_events(&surface_id, &value),
            Err(err) => {
                snapshot.degrade(format!("surface `{surface_id}` events unavailable: {err}"))
            }
        }
    }

    if let Some(session_id) = session_id {
        match projection.current_context(Some(session_id)).await {
            Ok(value) => {
                if value
                    .get("degraded")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
                {
                    let reason = value
                        .get("degraded_reason")
                        .and_then(|value| value.as_str())
                        .unwrap_or("context degraded");
                    snapshot.degrade(format!("context degraded: {reason}"));
                }
            }
            Err(err) => snapshot.degrade(format!("context Gateway API unavailable: {err}")),
        }
    }

    snapshot
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn operational_mfg_contract(
        granted_capabilities: Vec<String>,
    ) -> app_mfg_contract::MfgFrontendContractV1 {
        let routes = app_mfg_contract::mfg_route_contracts();
        let active_route_count = routes
            .iter()
            .filter(|route| route.availability == app_mfg_contract::MfgActionAvailability::Active)
            .count();
        app_mfg_contract::MfgFrontendContractV1 {
            kind: "mfg.frontend_contract".to_string(),
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            generated_at: chrono::Utc::now(),
            app_id: "mfg.manufacturing".to_string(),
            active_route_count,
            planned_route_count: routes.len().saturating_sub(active_route_count),
            routes,
            actions: app_mfg_contract::mfg_action_contracts(),
            surfaces: vec![app_mfg_contract::MfgSurfaceContract {
                surface: app_mfg_contract::MfgSurfaceKind::Tui,
                role: app_mfg_contract::MfgSurfaceRole::ConsoleOperationalControl,
                entrypoints: vec!["/mfg".to_string()],
                routes: app_mfg_contract::mfg_tui_route_contracts()
                    .into_iter()
                    .map(|route| route.route_id)
                    .collect(),
                actions: app_mfg_contract::mfg_tui_action_contracts()
                    .into_iter()
                    .map(|action| action.action_id)
                    .collect(),
            }],
            granted_capabilities,
        }
    }

    fn all_tui_action_capabilities() -> Vec<String> {
        app_mfg_contract::mfg_tui_action_contracts()
            .into_iter()
            .flat_map(|action| action.required_capabilities)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn governed_action_fixture() -> MfgOperationsState {
        let mut state = MfgOperationsState::default();
        let capabilities = all_tui_action_capabilities();
        state.contract = Some(operational_mfg_contract(capabilities.clone()));
        state.granted_capabilities = capabilities;
        state.incidents = vec![MfgItemSummary {
            id: "incident-1".to_string(),
            revision: Some(3),
            evidence_refs: vec!["matrix:evidence:evidence-1".to_string()],
            backlinks: vec![MfgBacklink {
                kind: MfgBacklinkKind::Evidence,
                target: "evidence://matrix/evidence-1".to_string(),
                label: "Evidence evidence-1".to_string(),
            }],
            ..MfgItemSummary::default()
        }];
        state.selected_incident_id = Some("incident-1".to_string());
        state.analysis = Some(app_mfg_contract::MfgReadResponseV1 {
            kind: Some("mfg.operational_analysis".to_string()),
            payload: BTreeMap::from([(
                "analysis".to_string(),
                serde_json::json!({
                    "analysis_id": "analysis-1",
                    "action_id": "action-1",
                    "revision": 4
                }),
            )]),
        });
        state.alerts = vec![MfgItemSummary {
            id: "alert-1".to_string(),
            revision: Some(5),
            ..MfgItemSummary::default()
        }];
        state.selected_alert_id = Some("alert-1".to_string());
        state.assignments = vec![MfgItemSummary {
            id: "assignment-1".to_string(),
            revision: Some(6),
            raw: serde_json::json!({
                "assignment_id": "assignment-1",
                "task_ref": "task://task-1",
                "assignee_ref": "principal:operator",
                "assignee_kind": "user",
                "watcher_refs": [],
                "priority": "normal",
                "notification_targets": [],
                "visibility": "private"
            }),
            ..MfgItemSummary::default()
        }];
        state.selected_assignment_id = Some("assignment-1".to_string());
        state.execution_ref = Some("execution-1".to_string());
        state.reports = vec![MfgItemSummary {
            id: "report-1".to_string(),
            revision: Some(7),
            evidence_refs: vec!["evidence-report-1".to_string()],
            raw: serde_json::json!({"profile_id": "profile-1"}),
            ..MfgItemSummary::default()
        }];
        state.selected_report_id = Some("report-1".to_string());
        state.delivery_state = Some(app_mfg_contract::MfgReadResponseV1 {
            kind: Some("mfg.cockpit.report_delivery_state".to_string()),
            payload: BTreeMap::from([(
                "delivery_state".to_string(),
                serde_json::json!({"report_id": "report-1", "retryable": true}),
            )]),
        });
        let now = chrono::Utc::now();
        state.reviews = vec![MfgItemSummary {
            id: "review-1".to_string(),
            revision: Some(8),
            ..MfgItemSummary::default()
        }];
        state.selected_review_id = Some("review-1".to_string());
        state.review_detail_ref = Some("review-1".to_string());
        state.review_detail = Some(app_mfg_contract::MfgReportDeliveryReview {
            review_id: "review-1".to_string(),
            report_id: "report-1".to_string(),
            report_revision: 7,
            delivery_revision: 2,
            dead_letter_digest: "sha256:dead-letter".to_string(),
            requester_principal: "principal:requester".to_string(),
            approval_id: Some("approval-1".to_string()),
            correlation_id: "review-correlation-1".to_string(),
            requested_action: None,
            decision: None,
            reviewer_principal: None,
            reason: String::new(),
            evidence_refs: vec!["evidence-review-1".to_string()],
            decision_lease_ref: None,
            effect_key: None,
            effect_payload: Value::Null,
            effect_receipt_ref: None,
            effect_error: None,
            status: app_mfg_contract::MfgReportDeliveryReviewStatus::PendingApproval,
            revision: 8,
            created_at: now,
            updated_at: now,
        });
        state.insights = vec![MfgItemSummary {
            id: "skill-1".to_string(),
            kind: "skill".to_string(),
            revision: Some(1),
            ..MfgItemSummary::default()
        }];
        state.selected_insight_id = Some("skill-1".to_string());
        state
    }

    pub(crate) fn operational_mfg_action_submissions() -> Vec<MfgActionSubmission> {
        let mut state = governed_action_fixture();
        app_mfg_contract::mfg_tui_action_contracts()
            .into_iter()
            .map(|action| {
                state
                    .prepare_action(action.action_id, explicit_action_payload(action.action_id))
                    .unwrap_or_else(|error| panic!("{}: {error}", action.action_id.as_str()));
                let intent = state.action_intents.last().expect("prepared MFG intent");
                MfgActionSubmission {
                    intent_id: intent.intent_id.clone(),
                    action_id: intent.action_id,
                    route_id: intent.route_id,
                    path_replacements: intent.path_replacements.clone(),
                    idempotency_key: intent.idempotency_key.clone(),
                    correlation_id: intent.correlation_id.clone(),
                    request_body: intent.request_body.clone(),
                }
            })
            .collect()
    }

    fn explicit_action_payload(action: app_mfg_contract::MfgActionId) -> Option<Value> {
        use app_mfg_contract::{MfgActionId as I, MfgMultiActionId as A, MfgRouteId as R};
        Some(match action {
            I::Route(R::IncidentCreate) => {
                serde_json::json!({"body": {"title": "Fixture incident"}})
            }
            I::Multi(A::AlertSnooze) => serde_json::json!({
                "path": {"id": "alert-1"},
                "body": {
                    "command": "snooze",
                    "expected_revision": 5,
                    "until": "2026-07-17T00:00:00Z",
                    "reason": "fixture"
                }
            }),
            I::Multi(A::AssignmentCreate) => serde_json::json!({
                "body": {
                    "assignment": {
                        "assignment_id": "assignment-new",
                        "task_ref": "task://task-1",
                        "assignee_ref": "principal:operator",
                        "assignee_kind": "user",
                        "watcher_refs": [],
                        "priority": "normal",
                        "notification_targets": [],
                        "visibility": "private"
                    }
                }
            }),
            I::Multi(A::AssignmentAssign | A::AssignmentTransfer) => serde_json::json!({
                "path": {"id": "assignment-1"},
                "body": {
                    "command": if action == I::Multi(A::AssignmentAssign) {
                        "assign"
                    } else {
                        "transfer"
                    },
                    "expected_revision": 6,
                    "target_ref": "principal:next",
                    "reason": "fixture"
                }
            }),
            I::Route(R::ExecutionFeedbackCreate) => serde_json::json!({
                "path": {"id": "execution-1"},
                "body": {"outcome": "observed", "note": "fixture evidence"}
            }),
            I::Route(R::ReportGenerate) => serde_json::json!({
                "path": {"id": "profile-1"},
                "body": {"report": {"cadence": "manual", "note": "fixture"}}
            }),
            I::Multi(A::ReportReviewReroute) => serde_json::json!({
                "path": {"id": "review-1"},
                "body": {
                    "decision": "reroute",
                    "expected_revision": 8,
                    "reason": "fixture",
                    "evidence_refs": ["evidence-review-1"],
                    "reroute": {
                        "target_ref": "channel://ops",
                        "provider_account": "account-1",
                        "channel": "ops",
                        "requested_capability": "surface.message.send"
                    }
                }
            }),
            I::Multi(A::ReportReviewResolve) => serde_json::json!({
                "path": {"id": "review-1"},
                "body": {
                    "decision": "resolve",
                    "expected_revision": 8,
                    "reason": "resolved externally",
                    "evidence_refs": ["evidence-review-1"]
                }
            }),
            _ => return None,
        })
    }

    fn gateway_snapshot() -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": 1,
            "runtime_host": "gateway-runtime-host",
            "active_sessions": 2,
            "uptime_secs": 9,
            "sessions": ["s1", "s2"],
            "leases": {"total": 0, "items": []},
        })
    }

    #[test]
    fn snapshot_ingests_gateway_capability_contract_summary() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_gateway_capability_contract(
            &serde_json::json!({
                "kind": "gateway.capability_contract",
                "schema_version": 1,
                "owner": "gateway",
                "route_count": 2,
                "capability_count": 2,
                "coverage": {
                    "route_count": 2,
                    "capability_count": 2,
                    "p1_count": 1,
                    "ai_visible_count": 2,
                    "openapi_path_count": 2,
                    "openai_tool_count": 1,
                    "route_contract_parity": true
                },
                "capabilities": [
                    {
                        "id": "gateway.surface.get",
                        "domain": "surface",
                        "title": "Surface registry",
                        "risk": "external",
                        "http": {"method": "GET", "path": "/api/surfaces", "criticality": "p1"},
                        "surface_visibility": {"tui": true}
                    },
                    {
                        "id": "gateway.hidden.get",
                        "domain": "gateway",
                        "title": "Hidden",
                        "risk": "read",
                        "http": {"method": "GET", "path": "/api/hidden", "criticality": "p2"},
                        "surface_visibility": {"tui": false}
                    }
                ]
            }),
            &serde_json::json!({
                "kind": "gateway.openai_tools",
                "tool_count": 1,
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "gateway_get_api_sessions",
                            "description": "List sessions",
                            "parameters": {
                                "type": "object",
                                "properties": {"limit": {"type": "integer"}}
                            }
                        }
                    }
                ]
            }),
        );

        let contract = snapshot
            .gateway_capability_contract
            .as_ref()
            .expect("contract summary");
        assert_eq!(contract.kind, "gateway.capability_contract");
        assert_eq!(contract.route_count, 2);
        assert_eq!(contract.capability_count, 2);
        assert!(contract.route_contract_parity);
        assert_eq!(contract.sample_routes.len(), 1);
        assert_eq!(contract.sample_routes[0].path, "/api/surfaces");
        assert_eq!(contract.sample_tools[0].name, "gateway_get_api_sessions");
        assert_eq!(contract.sample_tools[0].parameter_count, 1);

        let mut app = App::new("test-model", "session-test");
        snapshot.apply_to_app(&mut app);
        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(
            restored.gateway_capability_contract,
            snapshot.gateway_capability_contract
        );
    }

    #[test]
    fn approval_projection_preserves_typed_mfg_review_routing() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_pending_approvals(&serde_json::json!([{
            "approval_id": "mfg-approval:review-1",
            "action": "mfg.report.review.typed_decision",
            "summary": "Review dead-letter report",
            "risk": "high",
            "source": {
                "kind": "mfg",
                "resource_ref": "mfg:cockpit-report:report-1",
                "review_ref": "review-1"
            }
        }]));
        let approval = &snapshot.approval_items[0];
        assert!(approval.is_mfg_review());
        assert_eq!(approval.review_ref.as_deref(), Some("review-1"));
        assert_eq!(
            approval.resource_ref.as_deref(),
            Some("mfg:cockpit-report:report-1")
        );

        snapshot.ingest_pending_approvals(&serde_json::json!([{
            "approval_id": "mfg-approval:invalid",
            "action": "mfg.report.review.typed_decision",
            "source": {"kind": "mfg"}
        }]));
        let invalid = &snapshot.approval_items[0];
        assert!(invalid.is_mfg_source());
        assert!(!invalid.is_mfg_review());
    }

    #[test]
    fn snapshot_extracts_projection_summaries() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_session_ids(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        snapshot.ingest_runtime_control_plane(&serde_json::json!({
            "diagnostics": {
                "readiness_score": 87,
                "component_count": 12
            }
        }));
        snapshot.ingest_task_status(&serde_json::json!({
            "tasks": [
                {
                    "id": "t1",
                    "review_result": "accepted",
                    "artifacts": [{"path": "report.md"}],
                    "blocker_reason": "none"
                },
                {"id": "t2"}
            ]
        }));
        snapshot.ingest_pending_approvals(&serde_json::json!([
            {"id": "a1", "tool_name": "bash", "risk": "high", "preview": "rm -rf /tmp/x"}
        ]));
        snapshot.ingest_memory_status(&serde_json::json!({
            "status": "available"
        }));
        snapshot.ingest_cross_plane_summary(&serde_json::json!({
            "grants": {"active": 4},
            "interop": {"actions_24h": 7}
        }));
        snapshot.ingest_connector_accounts(&serde_json::json!({
            "accounts": [{
                "provider": "mock",
                "account_id": "mock-docs",
                "auth_mode": "none",
                "enabled_bindings": ["service.local.docs.read"],
                "health": {"status": "ready"}
            }]
        }));
        snapshot.ingest_connector_capabilities(&serde_json::json!({
            "capabilities": [{
                "capability_id": "service.local.docs.read",
                "provider": "mock",
                "plane": "service",
                "risk": "low",
                "supports_commit": true,
                "requires_approval": false
            }]
        }));
        snapshot.ingest_connector_resources(&serde_json::json!({
            "degraded_reason": "resource directory unavailable",
            "resources": [{
                "reference": "service://mock/docs/ready",
                "provider": "mock",
                "resource_type": "document",
                "title": "Ready Mock Document",
                "indexed_state": "indexed"
            }]
        }));

        assert!(snapshot.gateway_running);
        assert_eq!(snapshot.active_sessions, 3);
        assert_eq!(snapshot.runtime_readiness.as_deref(), Some("87%"));
        assert_eq!(snapshot.runtime_components, Some(12));
        assert_eq!(snapshot.task_count, Some(2));
        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[0].id, "t1");
        assert_eq!(snapshot.tasks[0].review_result.as_deref(), Some("accepted"));
        assert_eq!(snapshot.tasks[0].artifact_count, 1);
        assert_eq!(snapshot.tasks[0].blocker_reason.as_deref(), Some("none"));
        assert_eq!(snapshot.pending_approvals, Some(1));
        assert_eq!(snapshot.approval_items.len(), 1);
        assert_eq!(snapshot.approval_items[0].tool_name, "bash");
        assert_eq!(snapshot.memory_status.as_deref(), Some("available"));
        assert_eq!(snapshot.cross_plane_grants_active, Some(4));
        assert_eq!(snapshot.cross_plane_actions_24h, Some(7));
        assert_eq!(snapshot.connector_accounts.len(), 1);
        assert_eq!(snapshot.connector_accounts[0].status, "ready");
        assert_eq!(snapshot.connector_accounts[0].reason.as_deref(), None);
        assert_eq!(snapshot.connector_capabilities.len(), 1);
        assert!(snapshot.connector_capabilities[0].supports_commit);
        assert_eq!(snapshot.connector_resources.len(), 1);
        assert_eq!(snapshot.connector_resources[0].title, "Ready Mock Document");
        assert_eq!(
            snapshot.connector_degraded_reasons[0],
            "resource directory unavailable"
        );
    }

    #[test]
    fn snapshot_extracts_message_plane_and_round_trips_through_app() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_message_connectors(&serde_json::json!({
            "kind": "message.connector.registry",
            "connectors": [{
                "connector": "feishu",
                "name": "feishu",
                "configuration_status": "configured",
                "configured": true,
                "enabled": true,
                "missing_required": [],
                "capabilities": ["message.send.text", "message.send.image"],
                "runtime": {
                    "status": "ready",
                    "consecutive_failures": 0,
                    "restart_count": 1,
                    "circuit_open": false
                }
            }]
        }));
        snapshot.ingest_message_endpoints(&serde_json::json!({
            "kind": "message.endpoint.directory",
            "endpoints": [{
                "endpoint_id": "message:feishu:user",
                "connector": "feishu",
                "kind": "User",
                "configured": true,
                "status": "configured",
                "capabilities": ["message.send.text"]
            }]
        }));
        snapshot.ingest_message_routes(&serde_json::json!({
            "kind": "message.delivery.routes",
            "routes": [{
                "route_id": "message:feishu:default",
                "connector": "feishu",
                "policy": "origin",
                "status": "configured",
                "configured": true,
                "capabilities": ["message.send.text"],
                "runtime": {"status": "ready"}
            }]
        }));
        snapshot.ingest_message_bindings(&serde_json::json!({
            "kind": "message.conversation.bindings",
            "bindings": [{
                "binding_id": "message:feishu:user-1:thread-1",
                "connector": "feishu",
                "endpoint": "user-1",
                "direction": "inbound",
                "status": "processed",
                "runtime_session_id": "session-feishu",
                "resource_count": 2,
                "last_seen_at_ms": 42
            }]
        }));

        assert_eq!(snapshot.message_connectors.len(), 1);
        assert_eq!(snapshot.message_connectors[0].runtime_status, "ready");
        assert_eq!(snapshot.message_connectors[0].capability_count, 2);
        assert_eq!(snapshot.message_endpoints[0].kind, "User");
        assert_eq!(snapshot.message_routes[0].runtime_status, "ready");
        assert_eq!(
            snapshot.message_bindings[0].runtime_session_id.as_deref(),
            Some("session-feishu")
        );
        assert_eq!(snapshot.message_bindings[0].resource_count, 2);

        let mut app = App::new("model", "session-message-plane");
        snapshot.apply_to_app(&mut app);
        assert_eq!(app.gateway_message_connectors.len(), 1);
        assert_eq!(app.gateway_message_bindings[0].status, "processed");

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.message_connectors, snapshot.message_connectors);
        assert_eq!(restored.message_endpoints, snapshot.message_endpoints);
        assert_eq!(restored.message_routes, snapshot.message_routes);
        assert_eq!(restored.message_bindings, snapshot.message_bindings);
    }

    #[test]
    fn snapshot_extracts_cowd_and_structured_summaries() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_cowd_projection_state(
            &serde_json::json!({
                "capability_count": 9,
                "capabilities": []
            }),
            &serde_json::json!({
                "surface": "tui",
                "capability_count": 8,
                "capabilities": []
            }),
            &serde_json::json!({
                "webui_tui_full_parity": true,
                "cli_is_minimal_control": true
            }),
            &serde_json::json!({
                "status": "fail",
                "checks": [
                    {"check_id": "webui_tui_parity", "status": "pass"},
                    {"check_id": "structured_data", "status": "fail"}
                ]
            }),
        );
        snapshot.ingest_structured_data(
            &serde_json::json!({
                "count": 1,
                "items": [{"source_id": "pack-tui"}]
            }),
            &serde_json::json!({
                "items": [{"fact_id": "fact-tui"}]
            }),
            &serde_json::json!({
                "items": [{"evidence_id": "evidence-tui"}]
            }),
            &serde_json::json!({
                "items": [{"source_ref": "pack-tui", "high_watermark": "2026-06-14T00:00:00Z"}]
            }),
        );

        let kernel = snapshot.cowd_kernel.as_ref().expect("kernel summary");
        assert_eq!(kernel.capability_count, 9);
        assert_eq!(kernel.projection_capability_count, 8);
        assert!(kernel.webui_tui_full_parity);
        assert!(kernel.cli_is_minimal_control);
        assert_eq!(kernel.release_gate_status, "fail");
        assert_eq!(kernel.release_gate_failed_checks, 1);

        let data = snapshot
            .structured_data
            .as_ref()
            .expect("structured summary");
        assert_eq!(data.source_count, 1);
        assert_eq!(data.fact_count, 1);
        assert_eq!(data.evidence_count, 1);
        assert_eq!(data.watermark_count, 1);
        assert_eq!(data.sample_sources, vec!["pack-tui"]);
        assert_eq!(data.sample_facts, vec!["fact-tui"]);
        assert_eq!(data.sample_evidence, vec!["evidence-tui"]);
        assert_eq!(data.sample_watermarks, vec!["pack-tui"]);
    }

    #[test]
    fn snapshot_extracts_reality_core_and_fact_flow_summaries() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_reality_status(&serde_json::json!({
            "kind": "reality.status",
            "status": "ready",
            "engines": {
                "fact_kernel": {"status": "ready"},
                "memory": {"status": "ready"},
                "matrix": {"status": "ready"},
                "growth": {"status": "ready"},
                "context": {"status": "ready"},
                "audit": {"status": "ready"}
            },
            "capabilities": {
                "fact_runtime": {"status": "enabled_and_wired"},
                "matrix_context_source": {"status": "enabled_and_wired"}
            },
            "degraded_reasons": []
        }));
        snapshot.ingest_fact_flow(
            &serde_json::json!({
                "kind": "reality.fact_flow",
                "source": "growth.promotions",
                "session_id": "session-fact",
                "stages": [{"id": "capture"}, {"id": "promote"}],
                "events": [{"event_id": "event-1"}],
                "promotions": [{"target": "memory"}]
            }),
            Some(&serde_json::json!({
                "kind": "reality.boundaries",
                "boundaries": [{"name": "memory"}, {"name": "matrix"}]
            })),
        );

        let reality = snapshot.reality_core.as_ref().expect("reality summary");
        assert_eq!(reality.status, "ready");
        assert_eq!(reality.fact_status, "enabled_and_wired");
        assert_eq!(reality.memory_status, "ready");
        assert_eq!(reality.matrix_status, "ready");
        assert_eq!(reality.matrix_context_status, "enabled_and_wired");
        assert_eq!(reality.growth_status, "ready");

        let flow = snapshot.fact_flow.as_ref().expect("fact flow summary");
        assert_eq!(flow.source, "growth.promotions");
        assert_eq!(flow.session_id.as_deref(), Some("session-fact"));
        assert_eq!(flow.stage_count, 2);
        assert_eq!(flow.event_count, 1);
        assert_eq!(flow.promotion_count, 1);
        assert_eq!(flow.boundary_count, 2);
    }

    #[test]
    fn snapshot_extracts_mission_control_summary() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_mission_projection(&serde_json::json!({
            "envelope": {"service": "mission"},
            "control_readiness": {
                "ready_count": 5,
                "blocked_count": 2,
                "actions": [
                    {"action": "session.dispatch", "requires_approval": false},
                    {"action": "approval.decide", "requires_approval": true}
                ]
            },
            "mission": {
                "kind": "mission.runtime",
                "active_session_id": "session-a",
                "sessions": [
                    {
                        "session_id": "session-a",
                        "title": "Primary task",
                        "status": "active",
                        "active_team_ids": ["team-a"],
                        "active_agent_ids": ["agent-a", "agent-b"]
                    },
                    {
                        "session_id": "session-b",
                        "title": "Background audit",
                        "status": "background",
                        "active_team_ids": [],
                        "active_agent_ids": []
                    }
                ],
                "events": [{"sequence": 1}, {"sequence": 2}],
                "approval_projection": {"pending_count": 3},
                "relation_projection": {"relation_count": 4},
                "execution_graph_projection": {"count": 2},
                "conflict_projection": {"count": 1},
                "evidence_projection": {"count": 5},
                "capability_projection": {
                    "action_contracts": [
                        {"runtime_action": "use_team_template"},
                        {"runtime_action": "parallel_tool_batch"}
                    ]
                }
            }
        }));

        let mission = snapshot
            .mission_control
            .as_ref()
            .expect("mission control summary");
        assert_eq!(mission.active_session_id.as_deref(), Some("session-a"));
        assert_eq!(mission.session_count, 2);
        assert_eq!(mission.active_count, 1);
        assert_eq!(mission.background_count, 1);
        assert_eq!(mission.team_count, 1);
        assert_eq!(mission.agent_count, 2);
        assert_eq!(mission.pending_approvals, 3);
        assert_eq!(mission.relation_count, 4);
        assert_eq!(mission.execution_graph_count, 2);
        assert_eq!(mission.conflict_count, 1);
        assert_eq!(mission.evidence_count, 5);
        assert_eq!(mission.capability_action_count, 2);
        assert_eq!(mission.event_count, 2);
        assert_eq!(mission.control_ready_count, 5);
        assert_eq!(mission.control_blocked_count, 2);
        assert_eq!(mission.control_requires_approval_count, 1);
    }

    #[test]
    fn snapshot_round_trips_cowd_structured_through_app() {
        let mut app = App::new("claude-sonnet-4-6", "session-cowd-structured");
        let snapshot = RuntimeControlSnapshot {
            cowd_kernel: Some(CowdKernelSummary {
                capability_count: 12,
                projection_capability_count: 12,
                webui_tui_full_parity: true,
                cli_is_minimal_control: true,
                release_gate_status: "pass".to_string(),
                release_gate_failed_checks: 0,
            }),
            structured_data: Some(StructuredDataSummary {
                source_count: 2,
                fact_count: 3,
                evidence_count: 4,
                watermark_count: 1,
                sample_sources: vec!["pack-a".to_string()],
                sample_facts: vec!["fact-a".to_string()],
                sample_evidence: vec!["evidence-a".to_string()],
                sample_watermarks: vec!["pack-a".to_string()],
            }),
            reality_core: Some(RealityCoreSummary {
                status: "ready".to_string(),
                fact_status: "enabled_and_wired".to_string(),
                memory_status: "ready".to_string(),
                matrix_status: "ready".to_string(),
                matrix_context_status: "enabled_and_wired".to_string(),
                growth_status: "ready".to_string(),
                context_status: "ready".to_string(),
                audit_status: "ready".to_string(),
                degraded_reasons: Vec::new(),
            }),
            fact_flow: Some(FactFlowSummary {
                source: "growth.promotions".to_string(),
                session_id: Some("session-cowd-structured".to_string()),
                stage_count: 5,
                event_count: 2,
                promotion_count: 1,
                boundary_count: 4,
            }),
            mission_control: Some(MissionControlSummary {
                active_session_id: Some("session-cowd-structured".to_string()),
                session_count: 1,
                active_count: 1,
                background_count: 0,
                paused_count: 0,
                closed_count: 0,
                team_count: 1,
                agent_count: 2,
                pending_approvals: 0,
                relation_count: 0,
                execution_graph_count: 0,
                conflict_count: 0,
                evidence_count: 0,
                capability_action_count: 0,
                event_count: 1,
                control_ready_count: 2,
                control_blocked_count: 1,
                control_requires_approval_count: 0,
                sessions: vec![MissionSessionSummary {
                    session_id: "session-cowd-structured".to_string(),
                    title: "structured task".to_string(),
                    status: "active".to_string(),
                    team_count: 1,
                    agent_count: 2,
                }],
            }),
            ..RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot())
        };

        snapshot.apply_to_app(&mut app);
        assert_eq!(
            app.gateway_cowd_kernel
                .as_ref()
                .map(|kernel| kernel.release_gate_status.as_str()),
            Some("pass")
        );
        assert_eq!(
            app.gateway_structured_data
                .as_ref()
                .map(|data| data.fact_count),
            Some(3)
        );
        assert_eq!(
            app.gateway_reality_core
                .as_ref()
                .map(|reality| reality.status.as_str()),
            Some("ready")
        );
        assert_eq!(
            app.gateway_fact_flow.as_ref().map(|flow| flow.stage_count),
            Some(5)
        );
        assert_eq!(
            app.gateway_mission_control
                .as_ref()
                .map(|mission| mission.agent_count),
            Some(2)
        );

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.cowd_kernel, snapshot.cowd_kernel);
        assert_eq!(restored.structured_data, snapshot.structured_data);
        assert_eq!(restored.reality_core, snapshot.reality_core);
        assert_eq!(restored.fact_flow, snapshot.fact_flow);
        assert_eq!(restored.mission_control, snapshot.mission_control);
    }

    #[test]
    fn snapshot_reads_gateway_runtime_snapshot() {
        let snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": 1,
            "runtime_host": "gateway-runtime-host",
            "active_sessions": 2,
            "uptime_secs": 42,
            "sessions": ["s1", "s2"],
            "leases": {
                "total": 1,
                "items": [{
                    "session_id": "s1",
                    "owner": "tui:fast",
                    "mode": "collaborative"
                }]
            }
        }));

        assert!(snapshot.gateway_running);
        assert_eq!(snapshot.active_sessions, 2);
        assert_eq!(snapshot.uptime_secs, Some(42));
        assert_eq!(snapshot.session_ids, vec!["s1", "s2"]);
        assert_eq!(snapshot.lease_owner.as_deref(), Some("tui:fast"));
        assert_eq!(snapshot.lease_mode.as_deref(), Some("collaborative"));
    }

    #[test]
    fn snapshot_tracks_partial_degradation() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.degrade("task projection unavailable");
        snapshot.degrade("memory projection unavailable");

        assert!(snapshot.gateway_running);
        assert_eq!(snapshot.degraded_reasons.len(), 2);
        assert!(
            snapshot
                .degraded_reasons
                .iter()
                .any(|reason| reason.contains("task"))
        );
    }

    #[test]
    fn surface_event_refresh_replaces_previous_batch() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());

        snapshot.begin_surface_event_refresh();
        snapshot.ingest_surface_events(
            "webui",
            &serde_json::json!({
                "events": [{
                    "type": "surface.message.sent",
                    "surface": "webui",
                    "message": "first"
                }]
            }),
        );
        assert_eq!(snapshot.surface_events.len(), 1);
        assert_eq!(snapshot.surface_events[0].detail, "first");

        snapshot.begin_surface_event_refresh();
        snapshot.ingest_surface_events(
            "webui",
            &serde_json::json!({
                "events": [{
                    "type": "surface.message.sent",
                    "surface": "webui",
                    "message": "second"
                }]
            }),
        );

        assert_eq!(snapshot.surface_events.len(), 1);
        assert_eq!(snapshot.surface_events[0].detail, "second");
    }

    #[test]
    fn snapshot_round_trips_memory_status_through_app() {
        let mut app = App::new("claude-sonnet-4-6", "session-memory-status");
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_memory_status(&serde_json::json!({
            "status": "available",
            "total_entries": 42,
            "vector_count": 17,
            "layers": [
                {"layer": "L0", "entry_count": 1},
                {"layer": "L1", "entry_count": 2},
                {"layer": "L2", "entry_count": 3},
                {"layer": "L3", "entry_count": 4},
                {"layer": "L4", "entry_count": 5}
            ]
        }));

        snapshot.apply_to_app(&mut app);
        assert_eq!(app.memory_status.as_deref(), Some("available"));
        assert_eq!(app.memory_total_entries, Some(42));
        assert_eq!(app.memory_vector_count, Some(17));
        assert_eq!(app.memory_layer_counts, [1, 2, 3, 4, 5]);

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.memory_status.as_deref(), Some("available"));
        assert_eq!(restored.memory_total_entries, Some(42));
        assert_eq!(restored.memory_vector_count, Some(17));
        assert_eq!(restored.memory_layer_counts, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn snapshot_round_trips_action_receipts_through_app() {
        let mut app = App::new("claude-sonnet-4-6", "session-action-receipt");
        let snapshot = RuntimeControlSnapshot {
            action_receipts: vec![RuntimeActionReceiptSummary {
                status: "ok".to_string(),
                dispatch_status: "completed".to_string(),
                mode: "daemon-control".to_string(),
                capability: "daemon.task.complete".to_string(),
                idempotency_key: Some("task-1".to_string()),
            }],
            ..RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot())
        };

        snapshot.apply_to_app(&mut app);
        assert_eq!(app.gateway_action_receipts.len(), 1);

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.action_receipts.len(), 1);
        assert_eq!(
            restored.action_receipts[0].capability,
            "daemon.task.complete"
        );
    }

    #[test]
    fn local_store_records_receipts_without_mutating_gateway_lifecycle() {
        let mut app = App::new("claude-sonnet-4-6", "session-local-store");
        app.gateway_approval_items = vec![ApprovalSummary {
            id: "approval-1".to_string(),
            tool_name: "bash".to_string(),
            risk: Some("high".to_string()),
            requester: Some("session".to_string()),
            input_preview: "run command".to_string(),
            ..ApprovalSummary::default()
        }];
        app.gateway_pending_approvals = Some(1);
        app.gateway_tasks = vec![TaskSummary {
            id: "task-1".to_string(),
            objective: "finish task".to_string(),
            status: "blocked".to_string(),
            current_phase: None,
            yolo_mode: false,
            failure_count: 0,
            review_result: None,
            artifact_count: 0,
            blocker_reason: Some("waiting".to_string()),
        }];
        app.gateway_task_count = Some(1);
        app.gateway_connector_resources = vec![ConnectorResourceSummary {
            reference: "service://local.docs/document/1".to_string(),
            provider: "local.docs".to_string(),
            resource_type: "document".to_string(),
            title: "Doc".to_string(),
            indexed_state: "indexed".to_string(),
        }];

        let mut store = RuntimeControlLocalStore::from_app(&app);
        store.apply_connector_resource_state("service://local.docs/document/1", "stale");
        store.push_action_receipt(
            "failed",
            &"x".repeat(100),
            "daemon-control",
            "connector.resource.revalidate",
            Some("service://local.docs/document/1".to_string()),
        );
        store.apply_to_app(&mut app);

        assert_eq!(app.gateway_pending_approvals, Some(1));
        assert_eq!(app.gateway_approval_items.len(), 1);
        assert_eq!(app.gateway_tasks[0].status, "blocked");
        assert_eq!(
            app.gateway_tasks[0].blocker_reason.as_deref(),
            Some("waiting")
        );
        assert_eq!(app.gateway_connector_resources[0].indexed_state, "stale");
        assert_eq!(app.gateway_action_receipts.len(), 1);
        assert_eq!(
            app.gateway_action_receipts[0]
                .dispatch_status
                .chars()
                .count(),
            83
        );
        assert!(
            app.gateway_action_receipts[0]
                .dispatch_status
                .ends_with("...")
        );
    }

    #[test]
    fn mfg_refresh_and_selection_revisions_reject_stale_snapshots() {
        let mut state = MfgOperationsState::default();
        let generation = state.take_refresh_request().expect("initial refresh");
        state.active_tab = MfgViewTab::Incidents;
        state.incidents = vec![MfgItemSummary {
            id: "incident-1".to_string(),
            ..MfgItemSummary::default()
        }];
        state.selected_incident_id = Some("incident-1".to_string());
        state.move_selection(true);
        let stale = MfgOperationsSnapshot {
            selection_revision: state.selection_revision.saturating_sub(1),
            incidents: vec![MfgItemSummary {
                id: "stale".to_string(),
                ..MfgItemSummary::default()
            }],
            ..MfgOperationsSnapshot::default()
        };
        state.apply_snapshot(generation, stale);
        assert_eq!(state.incidents[0].id, "incident-1");
        assert!(!state.refresh_in_flight);
        assert!(state.take_refresh_request().is_some());
    }

    #[test]
    fn mfg_refresh_attempt_evidence_is_current_generation_only() {
        let mut state = MfgOperationsState::default();
        state
            .attempted_routes
            .insert(app_mfg_contract::MfgRouteId::IncidentList);
        let generation = state.take_refresh_request().expect("refresh");
        assert!(state.attempted_routes.is_empty());

        state.apply_contract(
            generation,
            app_mfg_contract::MfgFrontendContractV1 {
                kind: "mfg.frontend_contract".to_string(),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                generated_at: chrono::Utc::now(),
                app_id: "mfg.manufacturing".to_string(),
                active_route_count: 0,
                planned_route_count: 0,
                routes: Vec::new(),
                actions: Vec::new(),
                surfaces: Vec::new(),
                granted_capabilities: Vec::new(),
            },
        );
        assert_eq!(
            state.attempted_routes,
            BTreeSet::from([app_mfg_contract::MfgRouteId::ContractGet])
        );

        state.apply_snapshot(
            generation,
            MfgOperationsSnapshot {
                selection_revision: state.selection_revision,
                attempted_routes: BTreeSet::from([
                    app_mfg_contract::MfgRouteId::ContractGet,
                    app_mfg_contract::MfgRouteId::CommandCenterGet,
                ]),
                ..MfgOperationsSnapshot::default()
            },
        );
        assert_eq!(state.attempted_routes.len(), 2);
        assert!(
            !state
                .attempted_routes
                .contains(&app_mfg_contract::MfgRouteId::IncidentList)
        );
    }

    #[test]
    fn mfg_forbidden_refresh_redacts_cached_section_and_exposes_recovery() {
        let mut state = MfgOperationsState::default();
        state.reports = vec![MfgItemSummary {
            id: "report-1".to_string(),
            ..MfgItemSummary::default()
        }];
        state.last_updated_at = Some("2026-07-16T00:00:00Z".to_string());
        let generation = state.take_refresh_request().expect("refresh");
        let error = app_mfg_contract::MfgApiErrorV1::capability_denied("mfg.read");
        state.apply_error(generation, "reports".to_string(), error);
        assert!(state.reports.is_empty());
        assert!(state.is_stale);
        assert!(state.forbidden_sections.contains_key("reports"));
        assert!(!state.recovery_actions.is_empty());
    }

    #[test]
    fn mfg_detail_forbidden_recrops_every_projection_with_the_same_capability() {
        let mut state = MfgOperationsState::default();
        state.active_tab = MfgViewTab::Reviews;
        state
            .granted_capabilities
            .push("mfg.report.review".to_string());
        state.reviews = vec![MfgItemSummary {
            id: "review-1".to_string(),
            ..MfgItemSummary::default()
        }];
        state.review_detail = Some(app_mfg_contract::MfgReportDeliveryReview {
            review_id: "review-1".to_string(),
            report_id: "report-1".to_string(),
            report_revision: 1,
            delivery_revision: 1,
            dead_letter_digest: "digest".to_string(),
            requester_principal: "principal".to_string(),
            approval_id: None,
            correlation_id: "correlation".to_string(),
            requested_action: None,
            decision: None,
            reviewer_principal: None,
            reason: String::new(),
            evidence_refs: Vec::new(),
            decision_lease_ref: None,
            effect_key: None,
            effect_payload: Value::Null,
            effect_receipt_ref: None,
            effect_error: None,
            status: app_mfg_contract::MfgReportDeliveryReviewStatus::PendingApproval,
            revision: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        });
        let generation = state.take_refresh_request().expect("refresh");
        state.apply_error(
            generation,
            "review_detail".to_string(),
            app_mfg_contract::MfgApiErrorV1::capability_denied("mfg.report.review"),
        );
        assert_eq!(
            state.active_tab_forbidden().map(|(section, _)| section),
            Some("reviews")
        );
        assert!(state.reviews.is_empty());
        assert!(state.review_detail.is_none());
        assert!(state.forbidden_sections.contains_key("reviews"));
        assert!(state.forbidden_sections.contains_key("review_detail"));
        assert!(
            !state
                .granted_capabilities
                .iter()
                .any(|capability| capability == "mfg.report.review")
        );
        assert!(
            state
                .attempted_routes
                .contains(&app_mfg_contract::MfgRouteId::ReportReviewGet)
        );
    }

    #[test]
    fn mfg_authentication_loss_recrops_all_cached_mfg_data() {
        let mut state = MfgOperationsState::default();
        state.incidents.push(MfgItemSummary {
            id: "incident-1".to_string(),
            ..MfgItemSummary::default()
        });
        state.reviews.push(MfgItemSummary {
            id: "review-1".to_string(),
            ..MfgItemSummary::default()
        });
        state.granted_capabilities = vec!["mfg.read".to_string(), "mfg.report.review".to_string()];
        let generation = state.take_refresh_request().expect("refresh");
        state.apply_error(
            generation,
            "incident_detail".to_string(),
            app_mfg_contract::MfgApiErrorV1::authentication_required("token expired"),
        );
        assert!(state.incidents.is_empty());
        assert!(state.reviews.is_empty());
        assert!(state.granted_capabilities.is_empty());
        assert_eq!(state.forbidden_sections.len(), MFG_ALL_READ_SECTIONS.len());
    }

    #[test]
    fn mfg_partial_snapshot_keeps_typed_error_request_id_and_stale_flag() {
        let mut state = MfgOperationsState::default();
        state.last_updated_at = Some("2026-07-16T00:00:00Z".to_string());
        let generation = state.take_refresh_request().expect("refresh");
        let mut error = app_mfg_contract::MfgApiErrorV1::capability_denied("mfg.read");
        error.request_id = Some("request-403".to_string());
        let snapshot = MfgOperationsSnapshot {
            selection_revision: state.selection_revision,
            fetched_at: "2026-07-16T00:01:00Z".to_string(),
            degraded_reasons: vec!["reports: denied".to_string()],
            incidents: vec![MfgItemSummary {
                id: "incident-1".to_string(),
                ..MfgItemSummary::default()
            }],
            reviews: vec![MfgItemSummary {
                id: "review-1".to_string(),
                ..MfgItemSummary::default()
            }],
            granted_capabilities: vec!["mfg.read".to_string(), "mfg.report.review".to_string()],
            section_errors: BTreeMap::from([("reports".to_string(), error)]),
            attempted_routes: BTreeSet::from([
                app_mfg_contract::MfgRouteId::ContractGet,
                app_mfg_contract::MfgRouteId::ReportList,
            ]),
            is_stale: true,
            ..MfgOperationsSnapshot::default()
        };
        state.apply_snapshot(generation, snapshot);
        assert!(state.is_stale);
        assert_eq!(
            state
                .last_error
                .as_ref()
                .and_then(|error| error.request_id.as_deref()),
            Some("request-403")
        );
        assert!(!state.recovery_actions.is_empty());
        assert!(state.incidents.is_empty());
        assert_eq!(state.reviews.len(), 1);
        assert_eq!(
            state.granted_capabilities,
            vec!["mfg.report.review".to_string()]
        );
        assert_eq!(
            state.forbidden_sections.len(),
            app_mfg_contract::mfg_tui_read_route_contracts()
                .into_iter()
                .filter(|route| mfg_route_requires_capability(
                    &route.capability,
                    app_mfg_contract::MfgCapabilityId::Read.as_str()
                ))
                .filter_map(|route| mfg_route_section(route.route_id))
                .collect::<BTreeSet<_>>()
                .len()
        );
        assert_eq!(state.attempted_routes.len(), 2);
    }

    #[test]
    fn mfg_read_section_inventory_is_derived_equal_to_the_canonical_route_contract() {
        let canonical = app_mfg_contract::mfg_tui_read_route_contracts()
            .into_iter()
            .filter_map(|route| mfg_route_section(route.route_id))
            .collect::<BTreeSet<_>>();
        let guarded = MFG_ALL_READ_SECTIONS.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(guarded, canonical);
    }

    #[test]
    fn mfg_high_risk_cancel_never_submits_and_timeout_retry_reuses_the_key() {
        let mut state = MfgOperationsState::default();
        state.contract = Some(operational_mfg_contract(vec![
            "mfg.read".to_string(),
            "mfg.alert.respond".to_string(),
        ]));
        state.granted_capabilities = vec!["mfg.read".to_string(), "mfg.alert.respond".to_string()];
        state.alerts = vec![MfgItemSummary {
            id: "alert-1".to_string(),
            revision: Some(4),
            ..MfgItemSummary::default()
        }];
        state.selected_alert_id = Some("alert-1".to_string());

        let action_id =
            app_mfg_contract::MfgActionId::Multi(app_mfg_contract::MfgMultiActionId::AlertEscalate);
        let intent_id = state.prepare_action(action_id, None).unwrap();
        let intent = state.latest_action_intent().unwrap();
        assert_eq!(intent.status, MfgIntentStatus::AwaitingConfirmation);
        assert_eq!(intent.expected_revision, Some(4));
        let key = intent.idempotency_key.clone();
        assert!(state.take_action_submission().is_none());
        assert_eq!(state.cancel_pending_action().unwrap(), intent_id);
        assert_eq!(
            state.latest_action_intent().unwrap().status,
            MfgIntentStatus::Cancelled
        );
        assert_eq!(state.latest_action_intent().unwrap().idempotency_key, key);
        assert!(state.take_action_submission().is_none());

        let second = state.prepare_action(action_id, None).unwrap();
        state.confirm_pending_action().unwrap();
        let submission = state.take_action_submission().unwrap();
        assert_eq!(submission.intent_id, second);
        let timeout = app_mfg_contract::MfgApiErrorV1 {
            code: app_mfg_contract::MfgErrorCode::Internal,
            message: "request timed out".to_string(),
            http_status: 504,
            details: Value::Null,
            retryable: true,
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                kind: app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
                label: "Retry same intent".to_string(),
                target: None,
                enabled: true,
            }],
            request_id: Some("timeout-1".to_string()),
            receipt_ref: None,
        };
        state.apply_action_error(&second, timeout.clone());
        let retry_key = state
            .latest_action_intent()
            .unwrap()
            .idempotency_key
            .clone();
        state.retry_failed_action().unwrap();
        state.confirm_pending_action().unwrap();
        let retry = state.take_action_submission().unwrap();
        assert_eq!(retry.idempotency_key, retry_key);
    }

    #[test]
    fn create_action_reselection_and_retry_preserve_stable_target_identity() {
        let mut state = governed_action_fixture();
        let action_id =
            app_mfg_contract::MfgActionId::Route(app_mfg_contract::MfgRouteId::IncidentCreate);
        let payload = explicit_action_payload(action_id);
        let intent_id = state.prepare_action(action_id, payload.clone()).unwrap();
        let initial = state
            .action_intents
            .iter()
            .find(|intent| intent.intent_id == intent_id)
            .unwrap()
            .clone();
        assert!(initial.resource_ref.starts_with("mfg:incident:incident-"));
        assert_eq!(
            initial.resource_ref,
            format!(
                "mfg:incident:{}",
                stable_tui_mfg_resource_id("incident", &initial.idempotency_key)
            )
        );

        let timeout = app_mfg_contract::MfgApiErrorV1 {
            code: app_mfg_contract::MfgErrorCode::Internal,
            message: "request timed out after the business owner may have committed".to_string(),
            http_status: 504,
            details: Value::Null,
            retryable: true,
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                kind: app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
                label: "Retry same intent".to_string(),
                target: None,
                enabled: true,
            }],
            request_id: Some("timeout-create-1".to_string()),
            receipt_ref: None,
        };
        state.apply_action_error(&intent_id, timeout.clone());
        let reselected_id = state.prepare_action(action_id, payload).unwrap();
        let reselected = state.latest_action_intent().unwrap();
        assert_eq!(reselected_id, intent_id);
        assert_eq!(reselected.idempotency_key, initial.idempotency_key);
        assert_eq!(reselected.resource_ref, initial.resource_ref);

        state.apply_action_error(&intent_id, timeout);
        assert_eq!(state.retry_failed_action().unwrap(), intent_id);
        let retried = state.latest_action_intent().unwrap();
        assert_eq!(retried.idempotency_key, initial.idempotency_key);
        assert_eq!(retried.resource_ref, initial.resource_ref);

        state.apply_action_success(
            &intent_id,
            app_mfg_contract::MfgMutationResponseV1 {
                middleware_receipt: Some(app_mfg_contract::MfgReceiptV1 {
                    receipt_id: "receipt-create-1".to_string(),
                    idempotency_key: initial.idempotency_key.clone(),
                    actor_principal: "principal:tui".to_string(),
                    action_id,
                    resource_ref: initial.resource_ref,
                    expected_revision: None,
                    result_revision: Some(1),
                    payload_digest: initial.payload_digest,
                    correlation_id: Some(initial.correlation_id),
                    status: app_mfg_contract::MfgReceiptStatus::Completed,
                    response: serde_json::json!({"kind": "mfg.incident"}),
                    contract_version: app_mfg_contract::MfgContractVersion::default(),
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                }),
                ..app_mfg_contract::MfgMutationResponseV1::default()
            },
        );
        assert_eq!(
            state.latest_action_intent().unwrap().status,
            MfgIntentStatus::Accepted
        );
    }

    #[test]
    fn mfg_action_403_recrops_capability_and_requests_contract_refresh() {
        let mut state = governed_action_fixture();
        state.active_tab = MfgViewTab::Alerts;
        assert!(
            state
                .visible_action_contracts()
                .iter()
                .any(|action| action.action_id.as_str() == "mfg.alert.resolve")
        );
        state.action_intents.push(MfgActionIntent {
            intent_id: "intent-capability-loss".to_string(),
            action_id: app_mfg_contract::MfgActionId::Multi(
                app_mfg_contract::MfgMultiActionId::AlertResolve,
            ),
            route_id: app_mfg_contract::MfgRouteId::AlertCommand,
            resource_ref: "mfg:alert-occurrence:alert-1".to_string(),
            path_replacements: BTreeMap::from([("id".to_string(), "alert-1".to_string())]),
            expected_revision: Some(5),
            idempotency_key: "key-capability-loss".to_string(),
            correlation_id: "correlation-capability-loss".to_string(),
            payload_digest: "sha256:test".to_string(),
            request_body: serde_json::json!({}),
            risk: app_mfg_contract::MfgActionRisk::Medium,
            confirmation: app_mfg_contract::MfgConfirmationKind::Target,
            created_at: chrono::Utc::now().to_rfc3339(),
            status: MfgIntentStatus::Submitting,
            retryable: false,
            last_error: None,
            receipt: None,
        });
        state.action_intents.push(MfgActionIntent {
            intent_id: "intent-pending-after-capability-loss".to_string(),
            action_id: app_mfg_contract::MfgActionId::Multi(
                app_mfg_contract::MfgMultiActionId::AlertEscalate,
            ),
            route_id: app_mfg_contract::MfgRouteId::AlertCommand,
            resource_ref: "mfg:alert-occurrence:alert-1".to_string(),
            path_replacements: BTreeMap::from([("id".to_string(), "alert-1".to_string())]),
            expected_revision: Some(5),
            idempotency_key: "key-pending-capability-loss".to_string(),
            correlation_id: "correlation-pending-capability-loss".to_string(),
            payload_digest: "sha256:test-pending".to_string(),
            request_body: serde_json::json!({}),
            risk: app_mfg_contract::MfgActionRisk::High,
            confirmation: app_mfg_contract::MfgConfirmationKind::TargetAndConfirm,
            created_at: chrono::Utc::now().to_rfc3339(),
            status: MfgIntentStatus::AwaitingConfirmation,
            retryable: false,
            last_error: None,
            receipt: None,
        });
        state.refresh_requested = false;
        state.apply_action_error(
            "intent-capability-loss",
            app_mfg_contract::MfgApiErrorV1::capability_denied("mfg.alert.respond"),
        );
        assert!(state.refresh_requested);
        assert!(
            !state
                .granted_capabilities
                .iter()
                .any(|capability| capability == "mfg.alert.respond")
        );
        assert!(state.visible_action_contracts().is_empty());
        assert_eq!(
            state
                .action_intents
                .iter()
                .find(|intent| intent.intent_id == "intent-pending-after-capability-loss")
                .map(|intent| intent.status),
            Some(MfgIntentStatus::Forbidden)
        );
        assert!(state.confirm_pending_action().is_err());
        assert!(state.take_action_submission().is_none());
    }

    #[test]
    fn mfg_contextual_action_tabs_cover_the_entire_operational_registry_once_or_more() {
        let mut state = MfgOperationsState::default();
        let capabilities = all_tui_action_capabilities();
        state.contract = Some(operational_mfg_contract(capabilities.clone()));
        state.granted_capabilities = capabilities;
        let mut visible = BTreeSet::new();
        for tab in MfgViewTab::ALL {
            state.select_tab(tab);
            visible.extend(
                state
                    .visible_action_contracts()
                    .into_iter()
                    .map(|action| action.action_id),
            );
        }
        assert_eq!(
            visible,
            app_mfg_contract::mfg_tui_action_contracts()
                .into_iter()
                .map(|action| action.action_id)
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn mfg_payload_override_cannot_escalate_or_change_the_selected_action_semantics() {
        let mut state = MfgOperationsState::default();
        state.contract = Some(operational_mfg_contract(vec![
            "mfg.read".to_string(),
            "mfg.alert.respond".to_string(),
        ]));
        state.granted_capabilities = vec!["mfg.read".to_string(), "mfg.alert.respond".to_string()];
        state.alerts = vec![MfgItemSummary {
            id: "alert-1".to_string(),
            revision: Some(2),
            ..MfgItemSummary::default()
        }];
        state.selected_alert_id = Some("alert-1".to_string());
        let resolve =
            app_mfg_contract::MfgActionId::Multi(app_mfg_contract::MfgMultiActionId::AlertResolve);
        assert!(
            state
                .prepare_action(
                    resolve,
                    Some(serde_json::json!({
                        "body": {
                            "command": "escalate",
                            "expected_revision": 2,
                            "reason": "attempted action drift"
                        }
                    })),
                )
                .is_err()
        );

        state.granted_capabilities = vec!["mfg.read".to_string()];
        state.reports = vec![MfgItemSummary {
            id: "report-1".to_string(),
            ..MfgItemSummary::default()
        }];
        state.selected_report_id = Some("report-1".to_string());
        let dry_run = app_mfg_contract::MfgActionId::Multi(
            app_mfg_contract::MfgMultiActionId::ReportDeliverDryRun,
        );
        assert!(
            state
                .prepare_action(
                    dry_run,
                    Some(serde_json::json!({"body": {"mode": "commit"}})),
                )
                .is_err()
        );
        assert!(state.action_intents.is_empty());
    }

    #[test]
    fn mfg_receipt_identity_mismatch_is_never_reported_as_accepted() {
        let now = chrono::Utc::now();
        let mut state = MfgOperationsState::default();
        state.action_intents.push(MfgActionIntent {
            intent_id: "intent-1".to_string(),
            action_id: app_mfg_contract::MfgActionId::Multi(
                app_mfg_contract::MfgMultiActionId::AlertResolve,
            ),
            route_id: app_mfg_contract::MfgRouteId::AlertCommand,
            resource_ref: "mfg:alert-occurrence:alert-1".to_string(),
            path_replacements: BTreeMap::new(),
            expected_revision: Some(1),
            idempotency_key: "key-1".to_string(),
            correlation_id: "correlation-1".to_string(),
            payload_digest: "sha256:test".to_string(),
            request_body: serde_json::json!({}),
            risk: app_mfg_contract::MfgActionRisk::Medium,
            confirmation: app_mfg_contract::MfgConfirmationKind::Target,
            created_at: now.to_rfc3339(),
            status: MfgIntentStatus::Submitting,
            retryable: false,
            last_error: None,
            receipt: None,
        });
        state.apply_action_success(
            "intent-1",
            app_mfg_contract::MfgMutationResponseV1 {
                middleware_receipt: Some(app_mfg_contract::MfgReceiptV1 {
                    receipt_id: "receipt-1".to_string(),
                    idempotency_key: "key-1".to_string(),
                    actor_principal: "principal:tui".to_string(),
                    action_id: app_mfg_contract::MfgActionId::Multi(
                        app_mfg_contract::MfgMultiActionId::AlertResolve,
                    ),
                    resource_ref: "mfg:alert-occurrence:alert-2".to_string(),
                    expected_revision: Some(1),
                    result_revision: Some(2),
                    payload_digest: "sha256:test".to_string(),
                    correlation_id: Some("correlation-1".to_string()),
                    status: app_mfg_contract::MfgReceiptStatus::Completed,
                    response: serde_json::Value::Null,
                    contract_version: app_mfg_contract::MfgContractVersion::default(),
                    created_at: now,
                    updated_at: now,
                }),
                ..app_mfg_contract::MfgMutationResponseV1::default()
            },
        );
        let intent = state.latest_action_intent().unwrap();
        assert_eq!(intent.status, MfgIntentStatus::Failed);
        assert_eq!(
            intent.last_error.as_ref().map(|error| error.code),
            Some(app_mfg_contract::MfgErrorCode::ContractMismatch)
        );
        assert!(state.receipts.is_empty());
    }

    #[test]
    fn mfg_path_override_rebinds_target_and_rejects_split_resource_identity() {
        let mut state = MfgOperationsState::default();
        let capabilities = all_tui_action_capabilities();
        state.contract = Some(operational_mfg_contract(capabilities.clone()));
        state.granted_capabilities = capabilities;
        state.alerts = vec![MfgItemSummary {
            id: "alert-1".to_string(),
            revision: Some(2),
            ..MfgItemSummary::default()
        }];
        state.selected_alert_id = Some("alert-1".to_string());
        let action =
            app_mfg_contract::MfgActionId::Multi(app_mfg_contract::MfgMultiActionId::AlertResolve);
        let intent_id = state
            .prepare_action(
                action,
                Some(serde_json::json!({
                    "path": {"id": "alert-2"},
                    "body": {
                        "command": "resolve",
                        "expected_revision": 7,
                        "reason": "explicit alternate canonical target"
                    }
                })),
            )
            .expect("path override remains a single canonical target");
        let intent = state
            .action_intents
            .iter()
            .find(|intent| intent.intent_id == intent_id)
            .unwrap();
        assert_eq!(intent.path_replacements["id"], "alert-2");
        assert_eq!(
            intent.resource_ref,
            "mfg:alert-occurrence:alert-2".to_string()
        );

        assert!(
            state
                .prepare_action(
                    action,
                    Some(serde_json::json!({
                        "path": {"id": "alert-2"},
                        "resource_ref": "mfg:alert-occurrence:alert-1",
                        "body": {
                            "command": "resolve",
                            "expected_revision": 7,
                            "reason": "attempt split target"
                        }
                    })),
                )
                .is_err()
        );
    }

    #[test]
    fn every_operational_mfg_action_builds_a_bound_transport_intent() {
        let mut state = governed_action_fixture();
        for action in app_mfg_contract::mfg_tui_action_contracts() {
            let before = state.action_intents.len();
            let payload = explicit_action_payload(action.action_id);
            state
                .prepare_action(action.action_id, payload)
                .unwrap_or_else(|error| panic!("{}: {error}", action.action_id.as_str()));
            let intent = state.action_intents.last().unwrap();
            assert_eq!(state.action_intents.len(), before + 1);
            assert_eq!(intent.action_id, action.action_id);
            assert_eq!(intent.route_id, action.route_id);
            assert!(!intent.resource_ref.trim().is_empty());
            assert!(!intent.payload_digest.trim().is_empty());
            if matches!(
                action.mutation,
                app_mfg_contract::MfgMutationSemantics::DurableReceipt {
                    revision: app_mfg_contract::MfgRevisionSemantics::Required,
                    ..
                }
            ) {
                assert!(
                    intent.expected_revision.is_some(),
                    "{} omitted its frozen-matrix revision",
                    action.action_id.as_str()
                );
            }
            let replacements = intent
                .path_replacements
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect::<Vec<_>>();
            let path = crate::gateway_client::mfg_tui_route_path(action.route_id, &replacements)
                .unwrap_or_else(|error| panic!("{}: {error}", action.action_id.as_str()));
            assert!(!path.split('/').any(|segment| segment.starts_with(':')));
            assert_eq!(
                resolve_tui_mfg_action_id(action.route_id, &intent.request_body),
                Ok(action.action_id)
            );
        }
    }

    #[test]
    fn explicit_input_actions_open_fail_closed_editable_templates() {
        let state = governed_action_fixture();
        for action in app_mfg_contract::mfg_tui_action_contracts()
            .into_iter()
            .filter(|action| mfg_action_requires_explicit_input(action.action_id))
        {
            let command = state
                .action_input_command(action.action_id)
                .unwrap_or_else(|error| panic!("{}: {error}", action.action_id.as_str()));
            assert!(command.starts_with(&format!("/mfg action {} ", action.action_id.as_str())));
            assert!(command.contains("<required:"));
            let payload = command
                .splitn(4, ' ')
                .nth(3)
                .and_then(|payload| serde_json::from_str::<Value>(payload).ok())
                .expect("template JSON");
            let mut attempted = governed_action_fixture();
            assert!(
                attempted
                    .prepare_action(action.action_id, Some(payload))
                    .is_err(),
                "{} template must not submit before required values are edited",
                action.action_id.as_str()
            );
            assert!(attempted.action_intents.is_empty());
        }
    }

    #[test]
    fn each_operational_mfg_action_reducer_handles_terminal_transport_states() {
        for action in app_mfg_contract::mfg_tui_action_contracts() {
            let intent_id = format!("intent:{}", action.action_id.as_str());
            let idempotency_key = format!("key:{}", action.action_id.as_str());
            let base = MfgActionIntent {
                intent_id: intent_id.clone(),
                action_id: action.action_id,
                route_id: action.route_id,
                resource_ref: format!("mfg:test:{}", action.action_id.as_str()),
                path_replacements: BTreeMap::new(),
                expected_revision: Some(1),
                idempotency_key: idempotency_key.clone(),
                correlation_id: format!("correlation:{}", action.action_id.as_str()),
                payload_digest: "sha256:test".to_string(),
                request_body: serde_json::json!({}),
                risk: action.risk,
                confirmation: action.confirmation,
                created_at: chrono::Utc::now().to_rfc3339(),
                status: MfgIntentStatus::Submitting,
                retryable: false,
                last_error: None,
                receipt: None,
            };
            let receipt = |status| app_mfg_contract::MfgReceiptV1 {
                receipt_id: format!("receipt:{}", action.action_id.as_str()),
                idempotency_key: idempotency_key.clone(),
                actor_principal: "principal:tui".to_string(),
                action_id: action.action_id,
                resource_ref: base.resource_ref.clone(),
                expected_revision: base.expected_revision,
                result_revision: Some(2),
                payload_digest: base.payload_digest.clone(),
                correlation_id: Some(base.correlation_id.clone()),
                status,
                response: serde_json::json!({"ok": true}),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };

            let mut accepted = governed_action_fixture();
            accepted.action_intents.push(base.clone());
            accepted.apply_action_success(
                &intent_id,
                app_mfg_contract::MfgMutationResponseV1 {
                    middleware_receipt: Some(receipt(app_mfg_contract::MfgReceiptStatus::Accepted)),
                    ..app_mfg_contract::MfgMutationResponseV1::default()
                },
            );
            assert_eq!(
                accepted.latest_action_intent().unwrap().status,
                MfgIntentStatus::Accepted,
                "{}",
                action.action_id.as_str()
            );

            let mut replayed = governed_action_fixture();
            replayed.action_intents.push(base.clone());
            replayed.apply_action_success(
                &intent_id,
                app_mfg_contract::MfgMutationResponseV1 {
                    middleware_receipt: Some(receipt(app_mfg_contract::MfgReceiptStatus::Replayed)),
                    ..app_mfg_contract::MfgMutationResponseV1::default()
                },
            );
            assert_eq!(
                replayed.latest_action_intent().unwrap().status,
                MfgIntentStatus::Replayed
            );

            let mut forbidden = governed_action_fixture();
            forbidden.action_intents.push(base.clone());
            forbidden.apply_action_error(
                &intent_id,
                app_mfg_contract::MfgApiErrorV1::capability_denied(
                    action.required_capabilities[0].clone(),
                ),
            );
            assert_eq!(
                forbidden.latest_action_intent().unwrap().status,
                MfgIntentStatus::Forbidden
            );

            let mut conflict = governed_action_fixture();
            conflict.action_intents.push(base.clone());
            conflict.apply_action_error(
                &intent_id,
                app_mfg_contract::MfgApiErrorV1 {
                    code: app_mfg_contract::MfgErrorCode::RevisionConflict,
                    message: "revision conflict".to_string(),
                    http_status: 409,
                    details: Value::Null,
                    retryable: false,
                    contract_version: app_mfg_contract::MfgContractVersion::default(),
                    recovery_actions: Vec::new(),
                    request_id: Some("conflict-1".to_string()),
                    receipt_ref: None,
                },
            );
            assert_eq!(
                conflict.latest_action_intent().unwrap().status,
                MfgIntentStatus::Conflict
            );
            assert!(conflict.retry_failed_action().is_err());

            let mut timeout = governed_action_fixture();
            timeout.action_intents.push(base);
            timeout.apply_action_error(
                &intent_id,
                app_mfg_contract::MfgApiErrorV1 {
                    code: app_mfg_contract::MfgErrorCode::Internal,
                    message: "timeout".to_string(),
                    http_status: 504,
                    details: Value::Null,
                    retryable: true,
                    contract_version: app_mfg_contract::MfgContractVersion::default(),
                    recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                        kind: app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
                        label: "Retry same intent".to_string(),
                        target: None,
                        enabled: true,
                    }],
                    request_id: Some("timeout-1".to_string()),
                    receipt_ref: None,
                },
            );
            assert_eq!(
                timeout.latest_action_intent().unwrap().status,
                MfgIntentStatus::Failed
            );
            let stable_key = timeout
                .latest_action_intent()
                .unwrap()
                .idempotency_key
                .clone();
            timeout.retry_failed_action().unwrap();
            assert_eq!(
                timeout.latest_action_intent().unwrap().idempotency_key,
                stable_key
            );
        }
    }

    #[test]
    fn mfg_delta_queue_is_bounded() {
        let mut state = MfgOperationsState::default();
        for cursor in 0..1000 {
            state.enqueue_delta(MfgReadDelta {
                cursor: Some(cursor.to_string()),
                ..MfgReadDelta::default()
            });
        }
        assert_eq!(state.delta_queue.len(), MFG_DELTA_QUEUE_LIMIT);
        let expected_front = (1000 - MFG_DELTA_QUEUE_LIMIT).to_string();
        assert_eq!(
            state
                .delta_queue
                .front()
                .and_then(|delta| delta.cursor.as_deref()),
            Some(expected_front.as_str())
        );
    }

    #[test]
    fn mfg_live_snapshot_delta_and_generation_guard_update_canonical_state() {
        let mut state = MfgOperationsState::default();
        let generation = state.begin_live_consumer();
        let mut snapshot_state = app_mfg_contract::MfgLiveSnapshotStateV1::default();
        snapshot_state.assignments = serde_json::json!({
            "items": [{
                "assignment_id": "assignment-live-1",
                "assignee_ref": "principal:operator",
                "status": "assigned",
                "revision": 1
            }]
        });
        state.apply_live_envelope(
            generation,
            app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(app_mfg_contract::MfgLiveSnapshotV1 {
                view_epoch: "epoch-1".to_string(),
                cursor: "cursor-1".to_string(),
                generated_at: chrono::Utc::now(),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                state: snapshot_state,
            }),
        );
        assert_eq!(state.assignments[0].id, "assignment-live-1");
        state.apply_live_envelope(
            generation,
            app_mfg_contract::MfgLiveEnvelopeV1::Delta(app_mfg_contract::MfgLiveDeltaV1 {
                view_epoch: "epoch-1".to_string(),
                base_cursor: "cursor-1".to_string(),
                target_cursor: "cursor-2".to_string(),
                events: vec![app_mfg_contract::MfgLiveEventV1 {
                    event_type: "assignment.receipted".to_string(),
                    subject_ref: "mfg:assignment:assignment-live-1".to_string(),
                    revision: 2,
                    occurred_at: chrono::Utc::now(),
                    payload: serde_json::json!({
                        "assignment": {
                            "assignment_id": "assignment-live-1",
                            "assignee_ref": "principal:operator",
                            "status": "in_progress",
                            "revision": 2
                        }
                    }),
                }],
            }),
        );
        assert_eq!(state.assignments[0].status, "in_progress");
        assert_eq!(state.live_cursor.as_deref(), Some("cursor-2"));
        state.apply_live_envelope(
            generation.saturating_sub(1),
            app_mfg_contract::MfgLiveEnvelopeV1::Heartbeat(app_mfg_contract::MfgLiveHeartbeatV1 {
                view_epoch: "stale-epoch".to_string(),
                cursor: "stale-cursor".to_string(),
                generated_at: chrono::Utc::now(),
            }),
        );
        assert_eq!(state.live_cursor.as_deref(), Some("cursor-2"));
    }

    #[test]
    fn mfg_live_snapshot_and_delta_wire_every_contract_collection_into_tui_state() {
        let mut state = MfgOperationsState::default();
        let generation = state.begin_live_consumer();
        let snapshot_state = app_mfg_contract::MfgLiveSnapshotStateV1 {
            cockpit: serde_json::json!({"profiles": [{"profile_id": "profile-1"}]}),
            alerts: serde_json::json!({
                "rules": [{"rule_id": "rule-1"}],
                "subscriptions": [{"subscription_id": "subscription-1"}],
                "occurrences": [{"occurrence_id": "alert-1"}]
            }),
            assignments: serde_json::json!({"items": [{"assignment_id": "assignment-1"}]}),
            incidents: serde_json::json!({
                "items": [{"incident_id": "incident-1"}],
                "workflows": [{"workflow_id": "workflow-1"}],
                "analyses": [{"analysis_id": "analysis-1"}],
                "memory_cases": [{"case_id": "case-1"}],
                "playbooks": [{"playbook_id": "playbook-1"}]
            }),
            executions: serde_json::json!({
                "actions": [{"execution_id": "execution-1"}],
                "skills": [{"execution_id": "skill-1"}]
            }),
            reports: serde_json::json!({"items": [{"report_id": "report-1"}]}),
            reviews: serde_json::json!({"items": [{"review_id": "review-1"}]}),
            receipts: serde_json::json!({
                "commands": [{"receipt_id": "command-receipt-1"}],
                "mutations": [{
                    "receipt_id": "mutation-receipt-1",
                    "idempotency_key": "mutation-key-1",
                    "actor_principal": "principal:operator",
                    "action_id": "mfg.incident.create",
                    "resource_ref": "mfg:incident:incident-1",
                    "expected_revision": null,
                    "result_revision": 1,
                    "payload_digest": "sha256:mutation-1",
                    "correlation_id": "correlation:mutation-1",
                    "status": "completed",
                    "response": {"revision": 1},
                    "contract_version": "mfg.frontend.v1",
                    "created_at": "2026-07-16T00:00:00Z",
                    "updated_at": "2026-07-16T00:00:00Z"
                }]
            }),
            data_compute: serde_json::json!({
                "entities": [{"entity_id": "entity-1"}],
                "relations": [{"relation_id": "relation-1"}],
                "facts": [{"fact_id": "fact-1"}],
                "attention": [{"attention_id": "attention-1"}],
                "evidence": [{"packet_id": "evidence-1"}],
                "quality_gates": [{"gate_id": "gate-1"}],
                "metric_definitions": [{"metric_id": "metric-definition-1"}],
                "metric_dependencies": [{"dependency_id": "dependency-1"}],
                "metric_states": [{"state_id": "metric-state-1"}],
                "metric_snapshots": [{"snapshot_id": "metric-snapshot-1"}],
                "watermarks": [{"source_ref": "watermark-1"}],
                "jobs": [{"job_id": "job-1"}],
                "changes": [{"change_id": "change-1"}],
                "source_packs": [{"source_pack_id": "source-pack-1"}],
                "connector_runs": [{"run_id": "connector-1"}],
                "ontology_packs": [{"ontology_id": "ontology-1"}],
                "entity_match_candidates": [{"candidate_id": "candidate-1"}],
                "entity_conflict_decisions": [{"decision_id": "decision-1"}]
            }),
        };
        state.apply_live_envelope(
            generation,
            app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(app_mfg_contract::MfgLiveSnapshotV1 {
                view_epoch: "epoch-all".to_string(),
                cursor: "cursor-all-1".to_string(),
                generated_at: chrono::Utc::now(),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                state: snapshot_state,
            }),
        );
        assert_eq!(state.alert_rules[0].id, "rule-1");
        assert_eq!(state.alerts[0].id, "alert-1");
        assert_eq!(state.assignments[0].id, "assignment-1");
        assert_eq!(state.incidents[0].id, "incident-1");
        assert_eq!(state.reports[0].id, "report-1");
        assert_eq!(state.reviews[0].id, "review-1");
        let kinds = state
            .insights
            .iter()
            .map(|item| item.kind.as_str())
            .collect::<BTreeSet<_>>();
        for kind in [
            "cockpit_profile",
            "alert_subscription",
            "execution",
            "skill_run",
            "workflow",
            "analysis",
            "memory_case",
            "playbook",
            "entity",
            "relation",
            "fact",
            "attention",
            "evidence",
            "quality_gate",
            "metric_definition",
            "metric_dependency",
            "metric_state",
            "metric_snapshot",
            "watermark",
            "compute_job",
            "metric_change",
            "source_pack",
            "connector_run",
            "ontology",
            "entity_match_candidate",
            "entity_conflict_decision",
        ] {
            assert!(kinds.contains(kind), "TUI snapshot reducer omitted {kind}");
        }
        assert_eq!(state.live_receipts[0].receipt_id, "mutation-receipt-1");

        state.apply_live_envelope(
            generation,
            app_mfg_contract::MfgLiveEnvelopeV1::Delta(app_mfg_contract::MfgLiveDeltaV1 {
                view_epoch: "epoch-all".to_string(),
                base_cursor: "cursor-all-1".to_string(),
                target_cursor: "cursor-all-2".to_string(),
                events: vec![
                    app_mfg_contract::MfgLiveEventV1 {
                        event_type: "profile.upserted".to_string(),
                        subject_ref: "mfg:cockpit-profile:profile-2".to_string(),
                        revision: 2,
                        occurred_at: chrono::Utc::now(),
                        payload: serde_json::json!({"profile": {"profile_id": "profile-2"}}),
                    },
                    app_mfg_contract::MfgLiveEventV1 {
                        event_type: "alert_subscription.upserted".to_string(),
                        subject_ref: "mfg:alert-subscription:subscription-2".to_string(),
                        revision: 2,
                        occurred_at: chrono::Utc::now(),
                        payload: serde_json::json!({
                            "subscription": {"subscription_id": "subscription-2"}
                        }),
                    },
                    app_mfg_contract::MfgLiveEventV1 {
                        event_type: "receipt.completed".to_string(),
                        subject_ref: "mfg:receipt:receipt-2".to_string(),
                        revision: 2,
                        occurred_at: chrono::Utc::now(),
                        payload: serde_json::json!({"receipt": {
                            "receipt_id": "receipt-2",
                            "idempotency_key": "mutation-key-2",
                            "actor_principal": "principal:operator",
                            "action_id": "mfg.incident.create",
                            "resource_ref": "mfg:incident:incident-2",
                            "expected_revision": null,
                            "result_revision": 2,
                            "payload_digest": "sha256:mutation-2",
                            "correlation_id": "correlation:mutation-2",
                            "status": "completed",
                            "response": {"revision": 2},
                            "contract_version": "mfg.frontend.v1",
                            "created_at": "2026-07-16T00:00:01Z",
                            "updated_at": "2026-07-16T00:00:01Z"
                        }}),
                    },
                ],
            }),
        );
        for id in ["profile-2", "subscription-2"] {
            assert!(
                state.insights.iter().any(|item| item.id == id),
                "TUI delta reducer omitted {id}"
            );
        }
        assert!(
            state
                .live_receipts
                .iter()
                .any(|receipt| receipt.receipt_id == "receipt-2")
        );
    }

    #[test]
    fn mfg_live_cursor_gap_requires_snapshot_and_critical_overflow_is_fail_closed() {
        let mut state = MfgOperationsState::default();
        state.live_generation = 3;
        state.live_epoch = Some("epoch-1".to_string());
        state.live_cursor = Some("cursor-1".to_string());
        state.apply_live_envelope(
            3,
            app_mfg_contract::MfgLiveEnvelopeV1::Delta(app_mfg_contract::MfgLiveDeltaV1 {
                view_epoch: "epoch-1".to_string(),
                base_cursor: "wrong-base".to_string(),
                target_cursor: "cursor-2".to_string(),
                events: Vec::new(),
            }),
        );
        assert_eq!(
            state.live_resync_url.as_deref(),
            Some("/api/apps/mfg/live/snapshot")
        );

        state.live_resync_url = None;
        for cursor in 0..MFG_DELTA_QUEUE_LIMIT {
            state.enqueue_delta(MfgReadDelta {
                cursor: Some(cursor.to_string()),
                priority: 0,
                ..MfgReadDelta::default()
            });
        }
        state.enqueue_delta(MfgReadDelta {
            cursor: Some("overflow".to_string()),
            priority: 0,
            ..MfgReadDelta::default()
        });
        assert!(state.delta_queue.len() <= MFG_DELTA_QUEUE_LIMIT);
        assert_eq!(
            state.live_resync_url.as_deref(),
            Some("/api/apps/mfg/live/snapshot")
        );
    }

    #[test]
    fn mfg_live_reauthentication_count_survives_the_new_generation_snapshot() {
        let mut state = MfgOperationsState::default();
        state.live_generation = 4;
        state.apply_live_envelope(
            4,
            app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(app_mfg_contract::MfgLiveSnapshotV1 {
                view_epoch: "authorized".to_string(),
                cursor: "cursor-authorized".to_string(),
                generated_at: chrono::Utc::now(),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                state: app_mfg_contract::MfgLiveSnapshotStateV1 {
                    assignments: serde_json::json!({
                        "items": [{"assignment_id": "private-assignment", "status": "assigned"}],
                    }),
                    reports: serde_json::json!({
                        "items": [{"report_id": "private-report", "status": "generated"}],
                    }),
                    ..app_mfg_contract::MfgLiveSnapshotStateV1::default()
                },
            }),
        );
        assert_eq!(state.assignments.len(), 1);
        assert_eq!(state.reports.len(), 1);
        let mut error = app_mfg_contract::MfgApiErrorV1::authentication_required("profile changed");
        error.details = serde_json::json!({"reason": "profile_revision_changed"});
        state.apply_live_error(4, error);
        assert_eq!(state.live_reauthentication_count, 1);
        assert!(!state.live_stream_available);
        assert!(state.assignments.is_empty());
        assert!(state.reports.is_empty());
        assert!(state.live_epoch.is_none());
        assert!(state.live_cursor.is_none());
        assert!(state.refresh_requested);
        state.apply_live_envelope(
            5,
            app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(app_mfg_contract::MfgLiveSnapshotV1 {
                view_epoch: "recropped".to_string(),
                cursor: "cursor-recropped".to_string(),
                generated_at: chrono::Utc::now(),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                state: app_mfg_contract::MfgLiveSnapshotStateV1::default(),
            }),
        );
        assert_eq!(state.live_reauthentication_count, 1);
        assert!(state.live_stream_available);
        assert!(state.last_error.is_none());
        assert_eq!(state.live_epoch.as_deref(), Some("recropped"));
    }

    #[test]
    fn mfg_live_authority_restart_keeps_the_last_authorized_projection_visible() {
        let mut state = MfgOperationsState::default();
        state.live_generation = 4;
        state.apply_live_envelope(
            4,
            app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(app_mfg_contract::MfgLiveSnapshotV1 {
                view_epoch: "authorized".to_string(),
                cursor: "cursor-authorized".to_string(),
                generated_at: chrono::Utc::now(),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                state: app_mfg_contract::MfgLiveSnapshotStateV1 {
                    assignments: serde_json::json!({
                        "items": [{"assignment_id": "private-assignment", "status": "assigned"}],
                    }),
                    reports: serde_json::json!({
                        "items": [{"report_id": "private-report", "status": "generated"}],
                    }),
                    ..app_mfg_contract::MfgLiveSnapshotStateV1::default()
                },
            }),
        );
        let mut error =
            app_mfg_contract::MfgApiErrorV1::authentication_required("broker restarting");
        error.details = serde_json::json!({"reason": "authority_unavailable"});
        state.apply_live_error(4, error);

        assert!(!state.live_stream_available);
        assert_eq!(state.assignments.len(), 1);
        assert_eq!(state.reports.len(), 1);
        assert_eq!(state.live_epoch.as_deref(), Some("authorized"));
        assert_eq!(state.live_cursor.as_deref(), Some("cursor-authorized"));
        assert_eq!(state.live_reauthentication_count, 0);
        assert!(state.last_error.is_some());
    }

    #[test]
    fn mfg_live_capability_denial_clears_the_old_authorized_projection() {
        let mut state = MfgOperationsState::default();
        state.live_generation = 8;
        state.assignments.push(MfgItemSummary {
            id: "private-assignment".to_string(),
            kind: "assignment".to_string(),
            title: "private".to_string(),
            status: "assigned".to_string(),
            severity: None,
            owner: None,
            sla: None,
            revision: Some(1),
            evidence_refs: Vec::new(),
            backlinks: Vec::new(),
            raw: serde_json::json!({"assignment_id": "private-assignment"}),
        });
        let now = chrono::Utc::now();
        state.action_intents.push(MfgActionIntent {
            intent_id: "private-preview-intent".to_string(),
            action_id: app_mfg_contract::MfgActionId::Multi(
                app_mfg_contract::MfgMultiActionId::AlertResolve,
            ),
            route_id: app_mfg_contract::MfgRouteId::AlertCommand,
            resource_ref: "mfg:alert-occurrence:private-alert".to_string(),
            path_replacements: BTreeMap::new(),
            expected_revision: Some(1),
            idempotency_key: "private-preview-key".to_string(),
            correlation_id: "private-preview-correlation".to_string(),
            payload_digest: "sha256:private".to_string(),
            request_body: serde_json::json!({"private": true}),
            risk: app_mfg_contract::MfgActionRisk::Medium,
            confirmation: app_mfg_contract::MfgConfirmationKind::Target,
            created_at: now.to_rfc3339(),
            status: MfgIntentStatus::Accepted,
            retryable: false,
            last_error: None,
            receipt: Some(app_mfg_contract::MfgReceiptV1 {
                receipt_id: "private-preview-receipt".to_string(),
                idempotency_key: "private-preview-key".to_string(),
                actor_principal: "principal:tui".to_string(),
                action_id: app_mfg_contract::MfgActionId::Multi(
                    app_mfg_contract::MfgMultiActionId::AlertResolve,
                ),
                resource_ref: "mfg:alert-occurrence:private-alert".to_string(),
                expected_revision: Some(1),
                result_revision: None,
                payload_digest: "sha256:private".to_string(),
                correlation_id: Some("private-preview-correlation".to_string()),
                status: app_mfg_contract::MfgReceiptStatus::Preview,
                response: serde_json::json!({"private_payload": "must-disappear"}),
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                created_at: now,
                updated_at: now,
            }),
        });
        state.apply_live_error(
            8,
            app_mfg_contract::MfgApiErrorV1::capability_denied("mfg.read"),
        );
        assert!(state.assignments.is_empty());
        assert!(state.action_intents.is_empty());
        assert!(state.granted_capabilities.is_empty());
        assert_eq!(state.live_generation, 9);
        assert_eq!(state.live_reauthentication_count, 0);
        assert!(!state.live_stream_available);
    }

    #[test]
    fn mfg_pagination_intent_changes_the_gateway_limit_and_requests_refresh() {
        let mut state = MfgOperationsState::default();
        state.refresh_requested = false;
        state.active_tab = MfgViewTab::Incidents;
        assert!(state.adjust_page_limit(true));
        assert_eq!(state.pagination["incidents"].limit, 100);
        assert!(state.refresh_requested);
        state.refresh_requested = false;
        assert!(state.adjust_page_limit(false));
        assert_eq!(state.pagination["incidents"].limit, 50);
        assert!(state.refresh_requested);
    }

    #[test]
    fn mfg_backlink_intent_never_synthesizes_a_missing_target() {
        let mut state = MfgOperationsState::default();
        state.active_tab = MfgViewTab::Incidents;
        state.incidents = vec![MfgItemSummary {
            id: "incident-1".to_string(),
            backlinks: vec![MfgBacklink {
                kind: MfgBacklinkKind::Evidence,
                target: "evidence://packet-1".to_string(),
                label: "Evidence packet".to_string(),
            }],
            ..MfgItemSummary::default()
        }];
        state.selected_incident_id = Some("incident-1".to_string());
        assert_eq!(
            state
                .activate_backlink(MfgBacklinkKind::Evidence)
                .map(|link| link.target),
            Some("evidence://packet-1".to_string())
        );
        assert!(state.activate_backlink(MfgBacklinkKind::Runtime).is_none());
    }

    #[test]
    fn insights_detail_renders_the_selected_metric_lineage_and_focused_evidence_documents() {
        let mut state = MfgOperationsState::default();
        state.active_tab = MfgViewTab::Insights;
        state.focused_evidence_ref = Some("evidence-1".to_string());
        state.insights = vec![MfgItemSummary {
            id: "metric-1".to_string(),
            kind: "metric".to_string(),
            raw: serde_json::json!({"metric_id": "metric-1"}),
            ..MfgItemSummary::default()
        }];
        state.selected_insight_id = Some("metric-1".to_string());
        for (route, kind, payload) in [
            (
                app_mfg_contract::MfgRouteId::RealityMetricGet,
                "mfg.reality.metric",
                serde_json::json!({"metric_id": "metric-1"}),
            ),
            (
                app_mfg_contract::MfgRouteId::RealityMetricLineage,
                "mfg.reality.metric.lineage",
                serde_json::json!({"upstream": ["metric-0"]}),
            ),
            (
                app_mfg_contract::MfgRouteId::RealityEvidenceGet,
                "mfg.reality.evidence",
                serde_json::json!({"packet_id": "evidence-1"}),
            ),
            (
                app_mfg_contract::MfgRouteId::RealityEvidenceContext,
                "mfg.reality.evidence.context",
                serde_json::json!({"context_id": "context-1"}),
            ),
        ] {
            state.p1_documents.insert(
                route,
                MfgReadResponseV1 {
                    kind: Some(kind.to_string()),
                    payload: BTreeMap::from([("value".to_string(), payload)]),
                },
            );
        }
        let detail = state.current_detail().expect("Insights detail");
        assert_eq!(detail["focused_evidence_ref"], "evidence-1");
        assert_eq!(
            detail["selected"]["metric_id"],
            serde_json::Value::String("metric-1".to_string())
        );
        assert!(detail["metric_lineage"].is_object());
        assert!(detail["evidence"].is_object());
        assert!(detail["evidence_context"].is_object());
    }
}
