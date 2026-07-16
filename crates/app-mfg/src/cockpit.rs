use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitProfileInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    pub owner_ref: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub focus_refs: Vec<String>,
    #[serde(default)]
    pub focus_metric_ids: Vec<String>,
    #[serde(default)]
    pub thresholds: Value,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub cadence: Option<String>,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub scope: Option<MfgDashboardScope>,
    #[serde(default)]
    pub layout: Option<MfgDashboardLayout>,
    #[serde(default)]
    pub global_filters: Value,
    #[serde(default)]
    pub widget_instances: Vec<MfgWidgetInstance>,
    #[serde(default)]
    pub sharing_policy: Option<MfgDashboardSharingPolicy>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgDashboardScope {
    pub kind: String,
    #[serde(default)]
    pub scope_ref: Option<String>,
}

impl Default for MfgDashboardScope {
    fn default() -> Self {
        Self {
            kind: "personal".to_string(),
            scope_ref: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgDashboardLayout {
    pub columns: u16,
    pub row_height: u16,
    pub gap: u16,
}

impl Default for MfgDashboardLayout {
    fn default() -> Self {
        Self {
            columns: 12,
            row_height: 72,
            gap: 12,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgDashboardSharingPolicy {
    pub visibility: String,
    #[serde(default)]
    pub viewer_refs: Vec<String>,
    #[serde(default)]
    pub editor_refs: Vec<String>,
}

impl Default for MfgDashboardSharingPolicy {
    fn default() -> Self {
        Self {
            visibility: "private".to_string(),
            viewer_refs: Vec::new(),
            editor_refs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgWidgetPlacement {
    pub x: u16,
    pub y: u16,
    pub width: u16,
    pub height: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgWidgetInstance {
    pub instance_id: String,
    pub definition_id: String,
    pub placement: MfgWidgetPlacement,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub query: Value,
    #[serde(default = "default_true")]
    pub visible: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgWidgetDefinition {
    pub definition_id: String,
    pub title: String,
    pub renderer: String,
    pub renderer_version: u32,
    pub config_schema: Value,
    pub query_schema: Value,
    pub min_width: u16,
    pub min_height: u16,
    pub max_width: u16,
    pub max_height: u16,
    pub required_capability: String,
    pub default_placement: MfgWidgetPlacement,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitProfile {
    pub profile_id: String,
    pub owner_ref: String,
    pub display_name: String,
    #[serde(default)]
    pub focus_refs: Vec<String>,
    #[serde(default)]
    pub focus_metric_ids: Vec<String>,
    #[serde(default)]
    pub thresholds: Value,
    pub template_id: String,
    pub cadence: String,
    #[serde(default = "default_profile_revision")]
    pub revision: u64,
    #[serde(default)]
    pub scope: MfgDashboardScope,
    #[serde(default)]
    pub layout: MfgDashboardLayout,
    #[serde(default)]
    pub global_filters: Value,
    #[serde(default)]
    pub widget_instances: Vec<MfgWidgetInstance>,
    #[serde(default)]
    pub sharing_policy: MfgDashboardSharingPolicy,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitWidget {
    pub widget_id: String,
    pub widget_type: String,
    pub title: String,
    pub status: String,
    pub priority_score: f32,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub source_refs: Vec<String>,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default)]
    pub definition_id: String,
    #[serde(default = "default_renderer_version")]
    pub renderer_version: u32,
    #[serde(default)]
    pub freshness: Value,
    #[serde(default)]
    pub error: Option<String>,
}

const fn default_profile_revision() -> u64 {
    1
}
const fn default_renderer_version() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitProjection {
    pub projection_id: String,
    pub profile: MfgCockpitProfile,
    #[serde(default)]
    pub widgets: Vec<MfgCockpitWidget>,
    pub summary: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitWidgetProjection {
    pub projection_id: String,
    pub profile_id: String,
    pub profile_revision: u64,
    pub widget: MfgCockpitWidget,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitReportRequest {
    #[serde(default)]
    pub report_id: Option<String>,
    #[serde(default)]
    pub cadence: Option<String>,
    #[serde(default)]
    pub delivery_ref: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitReportDeliveryPayloadRequest {
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub target_ref: Option<String>,
    #[serde(default)]
    pub requested_capability: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitReportDeliveryPayload {
    pub payload_id: String,
    pub report_id: String,
    pub channel: String,
    pub template_id: String,
    #[serde(default)]
    pub target_ref: Option<String>,
    pub requested_capability: String,
    pub resource_ref: String,
    pub subject: String,
    pub text: String,
    pub markdown: String,
    #[serde(default)]
    pub body: Value,
    #[serde(default)]
    pub constraints: Vec<String>,
    pub rendered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitReportSnapshot {
    pub report_id: String,
    pub profile_id: String,
    pub owner_ref: String,
    pub cadence: String,
    pub title: String,
    pub summary: String,
    pub status: String,
    #[serde(default)]
    pub delivery_ref: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub delivery_receipts: Vec<MfgCockpitReportDeliveryReceipt>,
    pub projection: MfgCockpitProjection,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitReportDeliveryReceipt {
    pub delivery_id: String,
    pub report_id: String,
    pub cross_plane_receipt_id: String,
    pub cross_plane_status: String,
    pub cross_plane_dispatch_status: String,
    #[serde(default)]
    pub audit_record_id: Option<String>,
    pub delivered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCockpitReportDeliveryState {
    pub report_id: String,
    pub report_status: String,
    pub attempt_count: usize,
    pub retry_attempt_count: usize,
    pub max_attempts: usize,
    pub dead_lettered: bool,
    #[serde(default)]
    pub latest_receipt: Option<MfgCockpitReportDeliveryReceipt>,
    pub classification: String,
    pub retryable: bool,
    pub recommended_mode: String,
    #[serde(default)]
    pub reasons: Vec<String>,
}

impl MfgCockpitProfile {
    #[must_use]
    pub fn from_input(input: MfgCockpitProfileInput) -> Self {
        let now = Utc::now();
        let profile_id = input
            .profile_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("cockpit-profile-{}", uuid::Uuid::new_v4()));
        let display_name = input
            .display_name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| input.owner_ref.clone());
        Self {
            profile_id,
            owner_ref: input.owner_ref,
            display_name,
            focus_refs: input.focus_refs,
            focus_metric_ids: input.focus_metric_ids,
            thresholds: input.thresholds,
            template_id: input
                .template_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "mfg.default_ops".to_string()),
            cadence: input
                .cadence
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "daily".to_string()),
            revision: 1,
            scope: input.scope.unwrap_or_default(),
            layout: input.layout.unwrap_or_default(),
            global_filters: input.global_filters,
            widget_instances: if input.widget_instances.is_empty() {
                default_mfg_widget_instances()
            } else {
                input.widget_instances
            },
            sharing_policy: input.sharing_policy.unwrap_or_default(),
            created_at: now,
            updated_at: now,
        }
    }
}

impl MfgCockpitWidget {
    #[must_use]
    pub fn new(
        widget_type: impl Into<String>,
        title: impl Into<String>,
        status: impl Into<String>,
        priority_score: f32,
        data: Value,
        source_refs: Vec<String>,
    ) -> Self {
        Self {
            widget_id: format!("cockpit-widget-{}", uuid::Uuid::new_v4()),
            widget_type: widget_type.into(),
            title: title.into(),
            status: status.into(),
            priority_score,
            data,
            source_refs,
            instance_id: String::new(),
            definition_id: String::new(),
            renderer_version: 1,
            freshness: Value::Null,
            error: None,
        }
    }

    #[must_use]
    pub fn unavailable(
        instance: &MfgWidgetInstance,
        definition: Option<&MfgWidgetDefinition>,
        error: impl Into<String>,
    ) -> Self {
        let title = definition
            .map(|item| item.title.clone())
            .unwrap_or_else(|| instance.definition_id.clone());
        Self {
            widget_id: instance.instance_id.clone(),
            widget_type: instance.definition_id.clone(),
            title,
            status: "unavailable".to_string(),
            priority_score: 0.0,
            data: Value::Null,
            source_refs: Vec::new(),
            instance_id: instance.instance_id.clone(),
            definition_id: instance.definition_id.clone(),
            renderer_version: definition.map_or(1, |item| item.renderer_version),
            freshness: serde_json::json!({ "status": "unavailable" }),
            error: Some(error.into()),
        }
    }
}

impl MfgCockpitProfile {
    pub fn normalize_legacy(&mut self) {
        if self.revision == 0 {
            self.revision = 1;
        }
        if self.widget_instances.is_empty() {
            self.widget_instances = default_mfg_widget_instances();
        }
        if self.scope.kind.trim().is_empty() {
            self.scope = MfgDashboardScope::default();
        }
    }
}

#[must_use]
pub fn default_mfg_widget_instances() -> Vec<MfgWidgetInstance> {
    [
        ("attention", "attention.queue", 0, 0, 6, 4),
        ("quality", "quality.gates", 6, 0, 6, 4),
        ("actions", "action.executions", 0, 4, 6, 4),
        ("focus", "focus.summary", 6, 4, 6, 4),
    ]
    .into_iter()
    .map(|(id, definition, x, y, width, height)| MfgWidgetInstance {
        instance_id: format!("default-{id}"),
        definition_id: definition.to_string(),
        placement: MfgWidgetPlacement {
            x,
            y,
            width,
            height,
        },
        config: Value::Null,
        query: Value::Null,
        visible: true,
    })
    .collect()
}

#[must_use]
pub fn mfg_widget_catalog() -> Vec<MfgWidgetDefinition> {
    let definitions = [
        ("kpi.summary", "KPI summary", "kpi"),
        ("trend.metrics", "Metric trends", "line"),
        ("risk.matrix", "Risk matrix", "risk_matrix"),
        (
            "attention.queue",
            "Focused operational attention",
            "attention",
        ),
        ("incident.queue", "Incident queue", "incident"),
        ("workflow.progress", "Workflow progress", "workflow"),
        ("entity.impact", "Entity impact", "graph"),
        ("metric.lineage", "Metric lineage", "graph"),
        ("quality.gates", "Evidence and insight quality", "quality"),
        ("action.executions", "Governed action execution", "actions"),
        ("report.delivery", "Report delivery", "delivery"),
        ("data.freshness", "Data freshness", "freshness"),
        ("focus.summary", "Personal focus and thresholds", "focus"),
    ];
    definitions
        .into_iter()
        .enumerate()
        .map(|(index, (id, title, renderer))| MfgWidgetDefinition {
            definition_id: id.to_string(),
            title: title.to_string(),
            renderer: renderer.to_string(),
            renderer_version: 1,
            config_schema: mfg_widget_config_schema(id),
            query_schema: mfg_widget_query_schema(id),
            min_width: 3,
            min_height: 2,
            max_width: 12,
            max_height: 12,
            required_capability: "mfg.read".to_string(),
            default_placement: MfgWidgetPlacement {
                x: ((index * 3) % 12) as u16,
                y: ((index * 3) / 12 * 3) as u16,
                width: 6,
                height: 4,
            },
        })
        .collect()
}

/// Filter vocabulary shared by a dashboard and its widget instances.
/// Widget query values replace global values for the same key; omitted keys inherit.
#[must_use]
pub fn mfg_cockpit_global_filter_schema() -> Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "entity_refs": { "type": "array", "items": { "type": "string", "minLength": 1 }, "uniqueItems": true },
            "metric_ids": { "type": "array", "items": { "type": "string", "minLength": 1 }, "uniqueItems": true },
            "severities": { "type": "array", "items": { "enum": ["normal", "warning", "critical", "unknown"] }, "uniqueItems": true },
            "statuses": { "type": "array", "items": { "type": "string", "minLength": 1 }, "uniqueItems": true },
            "from": { "type": "string", "format": "date-time" },
            "to": { "type": "string", "format": "date-time" }
        },
        "additionalProperties": false
    })
}

#[must_use]
pub fn mfg_cockpit_filter_merge_policy() -> Value {
    serde_json::json!({
        "policy_id": "mfg.cockpit.filters.widget_overrides.v1",
        "precedence": ["profile.global_filters", "widget.query"],
        "semantics": "widget query replaces a global value for the same key; omitted keys inherit",
        "legacy_fallback": {
            "entity_refs": "profile.focus_refs",
            "metric_ids": "profile.focus_metric_ids"
        }
    })
}

fn mfg_widget_config_schema(definition_id: &str) -> Value {
    let mut properties = serde_json::Map::from_iter([
        (
            "title".to_string(),
            serde_json::json!({ "type": "string", "minLength": 1, "maxLength": 120 }),
        ),
        (
            "show_legend".to_string(),
            serde_json::json!({ "type": "boolean" }),
        ),
        (
            "refresh_interval_seconds".to_string(),
            serde_json::json!({ "type": "integer", "minimum": 10, "maximum": 3600 }),
        ),
    ]);
    if matches!(definition_id, "kpi.summary" | "trend.metrics") {
        properties.insert(
            "precision".to_string(),
            serde_json::json!({ "type": "integer", "minimum": 0, "maximum": 6 }),
        );
    }
    serde_json::json!({ "type": "object", "properties": properties, "additionalProperties": false })
}

fn mfg_widget_query_schema(definition_id: &str) -> Value {
    let mut properties = serde_json::Map::from_iter([(
        "limit".to_string(),
        serde_json::json!({ "type": "integer", "minimum": 1, "maximum": 100 }),
    )]);
    properties.insert(
        "from".to_string(),
        serde_json::json!({ "type": "string", "format": "date-time" }),
    );
    properties.insert(
        "to".to_string(),
        serde_json::json!({ "type": "string", "format": "date-time" }),
    );
    if matches!(
        definition_id,
        "attention.queue" | "risk.matrix" | "entity.impact"
    ) {
        properties.insert("entity_refs".to_string(), serde_json::json!({ "type": "array", "items": { "type": "string", "minLength": 1 }, "uniqueItems": true }));
    }
    if matches!(
        definition_id,
        "attention.queue" | "risk.matrix" | "metric.lineage" | "kpi.summary" | "trend.metrics"
    ) {
        properties.insert("metric_ids".to_string(), serde_json::json!({ "type": "array", "items": { "type": "string", "minLength": 1 }, "uniqueItems": true }));
    }
    if matches!(definition_id, "attention.queue" | "risk.matrix") {
        properties.insert("severities".to_string(), serde_json::json!({ "type": "array", "items": { "enum": ["normal", "warning", "critical", "unknown"] }, "uniqueItems": true }));
        properties.insert("statuses".to_string(), serde_json::json!({ "type": "array", "items": { "type": "string", "minLength": 1 }, "uniqueItems": true }));
    }
    serde_json::json!({ "type": "object", "properties": properties, "additionalProperties": false })
}

impl MfgCockpitReportSnapshot {
    #[must_use]
    pub fn from_projection(
        projection: MfgCockpitProjection,
        request: MfgCockpitReportRequest,
    ) -> Self {
        let report_id = request
            .report_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("cockpit-report-{}", uuid::Uuid::new_v4()));
        let cadence = request
            .cadence
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| projection.profile.cadence.clone());
        let title = format!(
            "MFG cockpit report for {} ({})",
            projection.profile.display_name, cadence
        );
        Self {
            report_id,
            profile_id: projection.profile.profile_id.clone(),
            owner_ref: projection.profile.owner_ref.clone(),
            cadence,
            title,
            summary: projection.summary.clone(),
            status: "generated".to_string(),
            delivery_ref: request.delivery_ref,
            note: request.note,
            delivery_receipts: Vec::new(),
            projection,
            created_at: Utc::now(),
        }
    }

    pub fn attach_delivery_receipt(&mut self, receipt: MfgCockpitReportDeliveryReceipt) {
        self.delivery_receipts
            .retain(|existing| existing.cross_plane_receipt_id != receipt.cross_plane_receipt_id);
        self.status = match receipt.cross_plane_status.as_str() {
            "planned" => "delivery_planned".to_string(),
            "dispatched" => "delivery_dispatched".to_string(),
            "blocked" => "delivery_blocked".to_string(),
            other => format!("delivery_{other}"),
        };
        self.delivery_receipts.push(receipt);
    }
}

impl MfgCockpitReportDeliveryPayload {
    #[must_use]
    pub fn from_report(
        report: &MfgCockpitReportSnapshot,
        request: MfgCockpitReportDeliveryPayloadRequest,
    ) -> Self {
        let target_ref = request
            .target_ref
            .filter(|value| !value.trim().is_empty())
            .or_else(|| report.delivery_ref.clone());
        let channel = infer_report_delivery_channel(
            request.channel.as_deref(),
            request.requested_capability.as_deref(),
            target_ref.as_deref(),
        );
        let template_id = request
            .template_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| default_report_delivery_template(&channel).to_string());
        let requested_capability = request
            .requested_capability
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| default_report_delivery_capability(&channel).to_string());
        let subject = format!("MFG {} cockpit report", report.cadence);
        let text = render_report_delivery_text(report, &template_id);
        let markdown = render_report_delivery_markdown(report, &template_id);
        let resource_ref = format!("text://{text}");
        let constraints = report_delivery_constraints(&channel, &requested_capability, &target_ref);
        Self {
            payload_id: format!("cockpit-payload-{}", uuid::Uuid::new_v4()),
            report_id: report.report_id.clone(),
            channel,
            template_id,
            target_ref,
            requested_capability,
            resource_ref,
            subject,
            text,
            markdown: markdown.clone(),
            body: serde_json::json!({
                "kind": "mfg.cockpit.report_delivery_payload",
                "report_id": report.report_id,
                "profile_id": report.profile_id,
                "owner_ref": report.owner_ref,
                "cadence": report.cadence,
                "title": report.title,
                "summary": report.summary,
                "markdown": markdown,
                "widget_count": report.projection.widgets.len(),
                "generated_at": report.created_at,
            }),
            constraints,
            rendered_at: Utc::now(),
        }
    }
}

impl MfgCockpitReportDeliveryReceipt {
    #[must_use]
    pub fn new(
        report_id: impl Into<String>,
        cross_plane_receipt_id: impl Into<String>,
        cross_plane_status: impl Into<String>,
        cross_plane_dispatch_status: impl Into<String>,
        audit_record_id: Option<String>,
    ) -> Self {
        Self {
            delivery_id: format!("cockpit-delivery-{}", uuid::Uuid::new_v4()),
            report_id: report_id.into(),
            cross_plane_receipt_id: cross_plane_receipt_id.into(),
            cross_plane_status: cross_plane_status.into(),
            cross_plane_dispatch_status: cross_plane_dispatch_status.into(),
            audit_record_id,
            delivered_at: Utc::now(),
        }
    }
}

impl MfgCockpitReportDeliveryState {
    #[must_use]
    pub fn from_report(report: &MfgCockpitReportSnapshot) -> Self {
        let latest_receipt = report.delivery_receipts.last().cloned();
        let retry_attempt_count = report
            .delivery_receipts
            .iter()
            .rev()
            .take_while(|receipt| {
                is_retryable_delivery_dispatch(&receipt.cross_plane_dispatch_status)
            })
            .count();
        let (classification, retryable, recommended_mode, reasons) =
            classify_report_delivery(report, latest_receipt.as_ref(), retry_attempt_count);
        Self {
            report_id: report.report_id.clone(),
            report_status: report.status.clone(),
            attempt_count: report.delivery_receipts.len(),
            retry_attempt_count,
            max_attempts: REPORT_DELIVERY_MAX_ATTEMPTS,
            dead_lettered: classification == "delivery_dead_lettered",
            latest_receipt,
            classification,
            retryable,
            recommended_mode,
            reasons,
        }
    }
}

fn classify_report_delivery(
    report: &MfgCockpitReportSnapshot,
    latest_receipt: Option<&MfgCockpitReportDeliveryReceipt>,
    retry_attempt_count: usize,
) -> (String, bool, String, Vec<String>) {
    let Some(receipt) = latest_receipt else {
        return (
            "not_delivered".to_string(),
            true,
            "dry_run".to_string(),
            vec!["delivery:not_attempted".to_string()],
        );
    };
    if retry_attempt_count >= REPORT_DELIVERY_MAX_ATTEMPTS {
        return (
            "delivery_dead_lettered".to_string(),
            false,
            "manual_review".to_string(),
            vec![
                "delivery:dead_lettered".to_string(),
                format!("delivery:retry_attempts_exhausted:{REPORT_DELIVERY_MAX_ATTEMPTS}"),
                format!("dispatch:{}", receipt.cross_plane_dispatch_status),
            ],
        );
    }
    match (
        receipt.cross_plane_status.as_str(),
        receipt.cross_plane_dispatch_status.as_str(),
    ) {
        ("blocked", "policy_blocked") => (
            "policy_blocked".to_string(),
            false,
            "dry_run".to_string(),
            vec!["policy:grant_or_identity_required".to_string()],
        ),
        ("blocked", dispatch_status) => (
            "delivery_blocked".to_string(),
            is_retryable_delivery_dispatch(dispatch_status),
            "dry_run".to_string(),
            vec![format!("dispatch:{dispatch_status}")],
        ),
        ("planned", "dry_run") => (
            "dry_run_planned".to_string(),
            false,
            "commit".to_string(),
            vec!["delivery:dry_run_only".to_string()],
        ),
        ("planned", "human_review_required") => (
            "awaiting_human_review".to_string(),
            false,
            "commit".to_string(),
            vec!["governance:human_review_required".to_string()],
        ),
        ("dispatched", dispatch_status) => (
            "sent".to_string(),
            false,
            "commit".to_string(),
            vec![format!("dispatch:{dispatch_status}")],
        ),
        (_, dispatch_status) if is_retryable_delivery_dispatch(dispatch_status) => (
            "delivery_retryable_failure".to_string(),
            true,
            "dry_run".to_string(),
            vec![format!("dispatch:{dispatch_status}")],
        ),
        _ => (
            report.status.clone(),
            false,
            "dry_run".to_string(),
            vec![format!(
                "delivery:{}:{}",
                receipt.cross_plane_status, receipt.cross_plane_dispatch_status
            )],
        ),
    }
}

const REPORT_DELIVERY_MAX_ATTEMPTS: usize = 3;

fn is_retryable_delivery_dispatch(dispatch_status: &str) -> bool {
    matches!(
        dispatch_status,
        "adapter_not_bound"
            | "target_not_ready"
            | "runtime_unavailable"
            | "dispatch_failed"
            | "send_failed"
            | "retryable_failure"
    ) || dispatch_status.contains("failed")
        || dispatch_status.contains("unavailable")
        || dispatch_status.contains("not_ready")
}

fn infer_report_delivery_channel(
    channel: Option<&str>,
    requested_capability: Option<&str>,
    target_ref: Option<&str>,
) -> String {
    if let Some(channel) = normalized_non_empty(channel) {
        return channel;
    }
    if let Some(capability) = normalized_non_empty(requested_capability) {
        if capability.contains(".feishu.") {
            return "feishu".to_string();
        }
        if capability.contains(".email.") {
            return "email".to_string();
        }
        if capability.contains(".webhook.") {
            return "webhook".to_string();
        }
    }
    if let Some(target_ref) = normalized_non_empty(target_ref) {
        if let Some(rest) = target_ref.strip_prefix("channel://") {
            if let Some(channel) = rest.split('/').next().filter(|value| !value.is_empty()) {
                return channel.to_string();
            }
        }
        if target_ref.starts_with("mailto:") {
            return "email".to_string();
        }
        if target_ref.starts_with("webhook://") {
            return "webhook".to_string();
        }
    }
    "feishu".to_string()
}

fn default_report_delivery_template(channel: &str) -> &'static str {
    match channel {
        "email" => "ops.email.standard",
        "webhook" => "ops.webhook.compact",
        _ => "ops.feishu.compact",
    }
}

fn default_report_delivery_capability(channel: &str) -> &'static str {
    match channel {
        "email" => "channel.email.send_text",
        "webhook" => "channel.webhook.send_text",
        _ => "channel.feishu.send_text",
    }
}

fn render_report_delivery_text(report: &MfgCockpitReportSnapshot, template_id: &str) -> String {
    let top_widgets = report
        .projection
        .widgets
        .iter()
        .take(3)
        .map(|widget| format!("{}={}", widget.title, widget.status))
        .collect::<Vec<_>>()
        .join("; ");
    match template_id {
        "ops.alert.compact" | "ops.feishu.compact" | "ops.webhook.compact" => format!(
            "{}: {}; widgets={}; top=[{}]; report={}",
            report.title,
            report.summary,
            report.projection.widgets.len(),
            top_widgets,
            report.report_id
        ),
        _ => format!(
            "{}: {}; cadence={}; widgets={}; report={}",
            report.title,
            report.summary,
            report.cadence,
            report.projection.widgets.len(),
            report.report_id
        ),
    }
}

fn render_report_delivery_markdown(report: &MfgCockpitReportSnapshot, template_id: &str) -> String {
    let widget_lines = report
        .projection
        .widgets
        .iter()
        .take(5)
        .map(|widget| format!("- {}: {}", widget.title, widget.status))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# {}\n\n{}\n\nTemplate: {}\nReport: {}\n\n{}",
        report.title, report.summary, template_id, report.report_id, widget_lines
    )
}

fn report_delivery_constraints(
    channel: &str,
    requested_capability: &str,
    target_ref: &Option<String>,
) -> Vec<String> {
    let mut constraints = vec![
        "payload_kind:text".to_string(),
        "cross_plane_policy_required".to_string(),
        "report_snapshot_required".to_string(),
        format!("channel:{channel}"),
        format!("capability:{requested_capability}"),
    ];
    if target_ref
        .as_ref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        constraints.push("target_ref_present".to_string());
    } else {
        constraints.push("target_ref_required".to_string());
    }
    constraints
}

fn normalized_non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
}
