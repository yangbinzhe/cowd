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

impl MfgReportDeliveryReviewStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::EffectAppliedForceRetry
                | Self::EffectAppliedReroute
                | Self::Abandoned
                | Self::ResolvedExternal
                | Self::Rejected
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReportDeliveryReviewRerouteTarget {
    pub target_ref: String,
    pub provider_account: String,
    pub channel: String,
    pub requested_capability: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MfgReportDeliveryReviewCreateRequest {
    #[serde(default)]
    pub idempotency_key: Option<String>,
    pub expected_report_revision: u64,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MfgReportDeliveryReviewDecisionRequest {
    pub decision: MfgReportDeliveryReviewDecision,
    pub expected_revision: u64,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub reroute: Option<MfgReportDeliveryReviewRerouteTarget>,
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
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub decision_lease_ref: Option<String>,
    #[serde(default)]
    pub effect_key: Option<String>,
    #[serde(default)]
    pub effect_payload: serde_json::Value,
    #[serde(default)]
    pub effect_receipt_ref: Option<String>,
    #[serde(default)]
    pub effect_error: Option<String>,
    pub status: MfgReportDeliveryReviewStatus,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReportDeliveryReviewSummary {
    pub review_id: String,
    pub status: MfgReportDeliveryReviewStatus,
    pub revision: u64,
    #[serde(default)]
    pub requested_action: Option<MfgReportDeliveryReviewDecision>,
    #[serde(default)]
    pub decision: Option<MfgReportDeliveryReviewDecision>,
    pub updated_at: DateTime<Utc>,
}

impl From<&MfgReportDeliveryReview> for MfgReportDeliveryReviewSummary {
    fn from(review: &MfgReportDeliveryReview) -> Self {
        Self {
            review_id: review.review_id.clone(),
            status: review.status,
            revision: review.revision,
            requested_action: review.requested_action,
            decision: review.decision,
            updated_at: review.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReportDeliveryReviewEffect {
    pub effect_id: String,
    pub review_id: String,
    pub action: MfgReportDeliveryReviewDecision,
    pub effect_key: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    pub status: String,
    pub attempt_count: u64,
    #[serde(default)]
    pub next_attempt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub receipt_ref: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReportDeliveryReviewCollection {
    #[serde(default)]
    pub items: Vec<MfgReportDeliveryReview>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}
