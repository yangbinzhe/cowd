use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{MfgOperationalAnalysis, MfgRecommendedAction};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgActionExecutionRequest {
    #[serde(default = "default_execution_mode")]
    pub mode: String,
    #[serde(default)]
    pub operator_id: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgActionExecution {
    pub execution_id: String,
    pub analysis_id: String,
    pub incident_id: String,
    pub action_id: String,
    pub action_type: String,
    pub title: String,
    pub owner_role: String,
    pub mode: String,
    pub status: String,
    pub governance: String,
    #[serde(default)]
    pub operator_id: Option<String>,
    #[serde(default)]
    pub command_hint: Option<String>,
    #[serde(default)]
    pub receipt: Value,
    #[serde(default)]
    pub cross_plane_receipts: Vec<MfgCrossPlaneBridgeReceipt>,
    #[serde(default)]
    pub feedback: Option<MfgActionFeedback>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgCrossPlaneBridgeReceipt {
    pub bridge_id: String,
    pub execution_id: String,
    pub cross_plane_receipt_id: String,
    pub cross_plane_status: String,
    pub cross_plane_dispatch_status: String,
    #[serde(default)]
    pub audit_record_id: Option<String>,
    pub bridged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgActionFeedback {
    pub outcome: String,
    pub note: String,
    #[serde(default)]
    pub metric_delta: Option<f64>,
    pub recorded_at: DateTime<Utc>,
}

impl MfgActionExecution {
    #[must_use]
    pub fn from_action(
        analysis: &MfgOperationalAnalysis,
        action: &MfgRecommendedAction,
        request: &MfgActionExecutionRequest,
    ) -> Self {
        let now = Utc::now();
        let mode = normalized_mode(&request.mode);
        let status = if mode == "commit" {
            "queued_for_human_review"
        } else {
            "dry_run_ready"
        };
        Self {
            execution_id: format!("execution-{}", uuid::Uuid::new_v4()),
            analysis_id: analysis.analysis_id.clone(),
            incident_id: analysis.incident_id.clone(),
            action_id: action.action_id.clone(),
            action_type: action.action_type.clone(),
            title: action.title.clone(),
            owner_role: action.owner_role.clone(),
            mode: mode.to_string(),
            status: status.to_string(),
            governance: action.governance.clone(),
            operator_id: request.operator_id.clone(),
            command_hint: action.command_hint.clone(),
            receipt: serde_json::json!({
                "receipt_kind": "mfg.action.execution",
                "analysis_id": analysis.analysis_id,
                "incident_id": analysis.incident_id,
                "action_id": action.action_id,
                "action_type": action.action_type,
                "mode": mode,
                "status": status,
                "governance": action.governance,
                "required_evidence": action.required_evidence,
                "operator_id": request.operator_id,
                "note": request.note,
                "created_at": now,
            }),
            cross_plane_receipts: Vec::new(),
            feedback: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn attach_cross_plane_receipt(&mut self, receipt: MfgCrossPlaneBridgeReceipt) {
        self.cross_plane_receipts
            .retain(|existing| existing.cross_plane_receipt_id != receipt.cross_plane_receipt_id);
        let status = receipt.cross_plane_status.clone();
        let dispatch_status = receipt.cross_plane_dispatch_status.clone();
        self.cross_plane_receipts.push(receipt);
        self.status = match status.as_str() {
            "planned" => "cross_plane_planned".to_string(),
            "dispatched" => "cross_plane_dispatched".to_string(),
            "blocked" => "cross_plane_blocked".to_string(),
            _ => format!("cross_plane_{status}"),
        };
        self.updated_at = Utc::now();
        self.receipt =
            merge_cross_plane_receipt(&self.receipt, &self.cross_plane_receipts, &dispatch_status);
    }

    pub fn apply_feedback(&mut self, feedback: MfgActionFeedback) {
        self.status = match feedback.outcome.as_str() {
            "resolved" | "accepted" | "executed" => "feedback_resolved".to_string(),
            "rejected" => "feedback_rejected".to_string(),
            "needs_followup" => "feedback_needs_followup".to_string(),
            _ => "feedback_recorded".to_string(),
        };
        self.updated_at = feedback.recorded_at;
        self.receipt = merge_feedback_receipt(&self.receipt, &feedback, &self.status);
        self.feedback = Some(feedback);
    }
}

impl MfgCrossPlaneBridgeReceipt {
    #[must_use]
    pub fn new(
        execution_id: impl Into<String>,
        cross_plane_receipt_id: impl Into<String>,
        cross_plane_status: impl Into<String>,
        cross_plane_dispatch_status: impl Into<String>,
        audit_record_id: Option<String>,
    ) -> Self {
        Self {
            bridge_id: format!("mfg-bridge-{}", uuid::Uuid::new_v4()),
            execution_id: execution_id.into(),
            cross_plane_receipt_id: cross_plane_receipt_id.into(),
            cross_plane_status: cross_plane_status.into(),
            cross_plane_dispatch_status: cross_plane_dispatch_status.into(),
            audit_record_id,
            bridged_at: Utc::now(),
        }
    }
}

impl MfgActionFeedback {
    #[must_use]
    pub fn new(
        outcome: impl Into<String>,
        note: impl Into<String>,
        metric_delta: Option<f64>,
    ) -> Self {
        Self {
            outcome: outcome.into(),
            note: note.into(),
            metric_delta,
            recorded_at: Utc::now(),
        }
    }
}

fn default_execution_mode() -> String {
    "dry_run".to_string()
}

fn normalized_mode(mode: &str) -> &'static str {
    if mode == "commit" {
        "commit"
    } else {
        "dry_run"
    }
}

fn merge_feedback_receipt(receipt: &Value, feedback: &MfgActionFeedback, status: &str) -> Value {
    let mut receipt = receipt.clone();
    if let Some(map) = receipt.as_object_mut() {
        map.insert("status".to_string(), Value::String(status.to_string()));
        map.insert(
            "feedback".to_string(),
            serde_json::to_value(feedback).unwrap_or(Value::Null),
        );
    }
    receipt
}

fn merge_cross_plane_receipt(
    receipt: &Value,
    bridge_receipts: &[MfgCrossPlaneBridgeReceipt],
    dispatch_status: &str,
) -> Value {
    let mut receipt = receipt.clone();
    if let Some(map) = receipt.as_object_mut() {
        map.insert(
            "cross_plane_dispatch_status".to_string(),
            Value::String(dispatch_status.to_string()),
        );
        map.insert(
            "cross_plane_receipts".to_string(),
            serde_json::to_value(bridge_receipts).unwrap_or(Value::Null),
        );
    }
    receipt
}
