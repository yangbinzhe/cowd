use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{require_schema, ProtocolValidate, ProtocolValidationError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AppErrorCodeV1 {
    InvalidRequest,
    Unauthenticated,
    OperationNotGranted,
    AppNotFound,
    ReceiptNotFound,
    RevisionConflict,
    IdempotencyConflict,
    CallCycleDetected,
    ProtocolIncompatible,
    AppActivationOverloaded,
    RequestTooLarge,
    CursorExpired,
    DeadlineExceeded,
    AppUnavailable,
    DependencyUnavailable,
    InternalError,
}

impl AppErrorCodeV1 {
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::InvalidRequest => 400,
            Self::Unauthenticated => 401,
            Self::OperationNotGranted => 403,
            Self::AppNotFound | Self::ReceiptNotFound => 404,
            Self::RevisionConflict | Self::IdempotencyConflict | Self::CallCycleDetected => 409,
            Self::ProtocolIncompatible => 426,
            Self::AppActivationOverloaded => 429,
            Self::RequestTooLarge => 413,
            Self::CursorExpired => 410,
            Self::DeadlineExceeded => 504,
            Self::AppUnavailable | Self::DependencyUnavailable => 503,
            Self::InternalError => 500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppErrorDetailV1 {
    pub code: AppErrorCodeV1,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(default)]
    pub details: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppErrorResponseV1 {
    pub schema_version: u16,
    pub error: AppErrorDetailV1,
}

impl ProtocolValidate for AppErrorResponseV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppErrorResponseV1", self.schema_version)?;
        if self.error.message.trim().is_empty() || self.error.message.len() > 1024 {
            return Err(ProtocolValidationError::InvalidField {
                field: "error.message",
                reason: "must contain 1..=1024 bytes".to_owned(),
            });
        }
        if !self.error.retryable && self.error.retry_after_ms.is_some() {
            return Err(ProtocolValidationError::InvalidField {
                field: "error.retry_after_ms",
                reason: "is only valid for retryable errors".to_owned(),
            });
        }
        Ok(())
    }
}
