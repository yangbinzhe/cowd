use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::version::MfgContractVersion;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgRecoveryActionKind {
    Reload,
    Compare,
    SaveAs,
    RetrySameIntent,
    RequestAccess,
    OpenApprovals,
    RequestManualReview,
    Resync,
    ChangeTarget,
    Abandon,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgRecoveryAction {
    pub kind: MfgRecoveryActionKind,
    pub label: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgErrorCode {
    AuthenticationRequired,
    CapabilityDenied,
    ScopeNotFound,
    ValidationFailed,
    RevisionConflict,
    IdempotencyConflict,
    RateLimited,
    Internal,
    ContractMismatch,
    ResyncRequired,
    ReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgApiErrorV1 {
    pub code: MfgErrorCode,
    pub message: String,
    pub http_status: u16,
    #[serde(default)]
    pub details: serde_json::Value,
    pub retryable: bool,
    pub contract_version: MfgContractVersion,
    #[serde(default)]
    pub recovery_actions: Vec<MfgRecoveryAction>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub receipt_ref: Option<String>,
}

impl MfgApiErrorV1 {
    #[must_use]
    pub fn authentication_required(message: impl Into<String>) -> Self {
        Self {
            code: MfgErrorCode::AuthenticationRequired,
            message: message.into(),
            http_status: 401,
            details: serde_json::Value::Null,
            retryable: false,
            contract_version: MfgContractVersion::default(),
            recovery_actions: vec![MfgRecoveryAction {
                kind: MfgRecoveryActionKind::RequestAccess,
                label: "Authenticate again".to_string(),
                target: Some("/api/auth/login".to_string()),
                enabled: true,
            }],
            request_id: None,
            receipt_ref: None,
        }
    }

    #[must_use]
    pub fn capability_denied(capability: impl Into<String>) -> Self {
        let capability = capability.into();
        Self {
            code: MfgErrorCode::CapabilityDenied,
            message: format!("required capability is not granted: {capability}"),
            http_status: 403,
            details: serde_json::json!({"required_capability": capability}),
            retryable: false,
            contract_version: MfgContractVersion::default(),
            recovery_actions: vec![MfgRecoveryAction {
                kind: MfgRecoveryActionKind::RequestAccess,
                label: "Request access".to_string(),
                target: None,
                enabled: true,
            }],
            request_id: None,
            receipt_ref: None,
        }
    }
}
