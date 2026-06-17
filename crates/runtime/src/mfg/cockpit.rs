use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgCockpitProjection {
    pub projection_id: String,
    pub profile: MfgCockpitProfile,
    #[serde(default)]
    pub widgets: Vec<MfgCockpitWidget>,
    pub summary: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgCockpitReportDeliveryState {
    pub report_id: String,
    pub report_status: String,
    pub attempt_count: usize,
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
        }
    }
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
        let latest_receipt = report
            .delivery_receipts
            .iter()
            .max_by_key(|receipt| receipt.delivered_at)
            .cloned();
        let (classification, retryable, recommended_mode, reasons) =
            classify_report_delivery(report, latest_receipt.as_ref());
        Self {
            report_id: report.report_id.clone(),
            report_status: report.status.clone(),
            attempt_count: report.delivery_receipts.len(),
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
) -> (String, bool, String, Vec<String>) {
    let Some(receipt) = latest_receipt else {
        return (
            "not_delivered".to_string(),
            true,
            "dry_run".to_string(),
            vec!["delivery:not_attempted".to_string()],
        );
    };
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
