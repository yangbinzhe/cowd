use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitProfileInput {
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
pub struct IaccCockpitProfile {
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
pub struct IaccCockpitWidget {
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
pub struct IaccCockpitProjection {
    pub projection_id: String,
    pub profile: IaccCockpitProfile,
    #[serde(default)]
    pub widgets: Vec<IaccCockpitWidget>,
    pub summary: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitReportRequest {
    #[serde(default)]
    pub report_id: Option<String>,
    #[serde(default)]
    pub cadence: Option<String>,
    #[serde(default)]
    pub delivery_ref: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitReportSnapshot {
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
    pub delivery_receipts: Vec<IaccCockpitReportDeliveryReceipt>,
    pub projection: IaccCockpitProjection,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccCockpitReportDeliveryReceipt {
    pub delivery_id: String,
    pub report_id: String,
    pub cross_plane_receipt_id: String,
    pub cross_plane_status: String,
    pub cross_plane_dispatch_status: String,
    #[serde(default)]
    pub audit_record_id: Option<String>,
    pub delivered_at: DateTime<Utc>,
}

impl IaccCockpitProfile {
    #[must_use]
    pub fn from_input(input: IaccCockpitProfileInput) -> Self {
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
                .unwrap_or_else(|| "iacc.default_ops".to_string()),
            cadence: input
                .cadence
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "daily".to_string()),
            created_at: now,
            updated_at: now,
        }
    }
}

impl IaccCockpitWidget {
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

impl IaccCockpitReportSnapshot {
    #[must_use]
    pub fn from_projection(
        projection: IaccCockpitProjection,
        request: IaccCockpitReportRequest,
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
            "IACC cockpit report for {} ({})",
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

    pub fn attach_delivery_receipt(&mut self, receipt: IaccCockpitReportDeliveryReceipt) {
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

impl IaccCockpitReportDeliveryReceipt {
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
