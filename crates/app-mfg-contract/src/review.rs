use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgReportDeliveryReviewDecision {
    ForceRetry,
    Reroute,
    Abandon,
    Resolve,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgReportDeliveryReviewStatus {
    ApprovalSubmissionPending,
    PendingApproval,
    DecisionPendingEffect,
    ApprovedPendingEffect,
    EffectAppliedForceRetry,
    EffectAppliedReroute,
    Abandoned,
    ResolvedExternal,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReportDeliveryReview {
    pub review_id: String,
    pub report_id: String,
    pub report_revision: u64,
    pub delivery_revision: u64,
    pub dead_letter_digest: String,
    pub requester_principal: String,
    #[serde(default)]
    pub approval_id: Option<String>,
    pub correlation_id: String,
    #[serde(default)]
    pub requested_action: Option<MfgReportDeliveryReviewDecision>,
    #[serde(default)]
    pub decision: Option<MfgReportDeliveryReviewDecision>,
    #[serde(default)]
    pub reviewer_principal: Option<String>,
    pub status: MfgReportDeliveryReviewStatus,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
