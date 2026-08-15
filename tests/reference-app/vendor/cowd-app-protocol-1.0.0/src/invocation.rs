use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    require_bounded, require_digest, require_schema, require_unique, DelegationKindV1,
    ExecutionContextV1, PrincipalContextV1, ProtocolValidate, ProtocolValidationError,
    Sha256Digest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperationKindV1 {
    Query,
    Command,
    Subscribe,
    Export,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperationDelegationV1 {
    User,
    Service,
    Either,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencySemanticsV1 {
    ReadOnly,
    Required,
    SubscriptionCursor,
    ContentAddressed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperationDescriptorV1 {
    pub operation_id: String,
    pub kind: OperationKindV1,
    pub input_schema_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub required_capability: String,
    pub delegation: OperationDelegationV1,
    pub tenant_scoped: bool,
    pub workspace_scoped: bool,
    pub read_only: bool,
    pub idempotency: IdempotencySemanticsV1,
    pub default_deadline_ms: u64,
    pub maximum_deadline_ms: u64,
    pub maximum_request_bytes: u64,
    pub maximum_response_bytes: u64,
    pub maximum_frame_bytes: u64,
    pub streaming: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_window_seconds: Option<u64>,
    pub degraded_read_allowed: bool,
    pub audit_classification: String,
}

impl ProtocolValidate for OperationDescriptorV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("operation_id", &self.operation_id, 256)?;
        require_bounded("required_capability", &self.required_capability, 256)?;
        require_bounded("audit_classification", &self.audit_classification, 128)?;
        self.input_schema_digest
            .validate_value("input_schema_digest")?;
        self.output_schema_digest
            .validate_value("output_schema_digest")?;
        if self.default_deadline_ms == 0
            || self.maximum_deadline_ms < self.default_deadline_ms
            || self.maximum_request_bytes == 0
            || self.maximum_response_bytes == 0
            || self.maximum_frame_bytes == 0
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "operation_limits",
                reason: "deadlines and byte limits must be positive and ordered".to_owned(),
            });
        }
        match self.kind {
            OperationKindV1::Query => {
                if !self.read_only
                    || self.idempotency != IdempotencySemanticsV1::ReadOnly
                    || self.streaming
                {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "query_semantics",
                        reason: "query must be read-only and unary".to_owned(),
                    });
                }
            }
            OperationKindV1::Command => {
                if self.read_only
                    || self.idempotency != IdempotencySemanticsV1::Required
                    || self.streaming
                {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "command_semantics",
                        reason: "command must require idempotency, may write and be unary"
                            .to_owned(),
                    });
                }
            }
            OperationKindV1::Subscribe => {
                if !self.streaming
                    || self.idempotency != IdempotencySemanticsV1::SubscriptionCursor
                    || self.replay_window_seconds.is_none()
                {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "subscribe_semantics",
                        reason: "subscribe must stream with a replay window".to_owned(),
                    });
                }
            }
            OperationKindV1::Export => {
                if !self.streaming || self.idempotency != IdempotencySemanticsV1::ContentAddressed {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "export_semantics",
                        reason: "export must be streaming and content-addressed".to_owned(),
                    });
                }
            }
        }
        if self.degraded_read_allowed && !self.read_only {
            return Err(ProtocolValidationError::InvalidField {
                field: "degraded_read_allowed",
                reason: "is valid only for read-only operations".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppInvocationEnvelopeV1 {
    pub schema_version: u16,
    pub operation_id: String,
    pub request_id: String,
    pub correlation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    pub deadline_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<String>,
    pub call_chain: Vec<String>,
    pub max_hops: u8,
    pub input_schema_digest: Sha256Digest,
    pub principal: PrincipalContextV1,
    pub execution: ExecutionContextV1,
    pub payload: Value,
}

impl AppInvocationEnvelopeV1 {
    pub fn validate_for(
        &self,
        descriptor: &OperationDescriptorV1,
    ) -> Result<(), ProtocolValidationError> {
        self.validate()?;
        descriptor.validate()?;
        if self.operation_id != descriptor.operation_id {
            return Err(ProtocolValidationError::InvalidField {
                field: "operation_id",
                reason: "does not match the selected operation".to_owned(),
            });
        }
        if self.input_schema_digest != descriptor.input_schema_digest {
            return Err(ProtocolValidationError::InvalidField {
                field: "input_schema_digest",
                reason: "does not match the operation descriptor".to_owned(),
            });
        }
        let delegation_matches = matches!(
            (self.principal.delegation, descriptor.delegation),
            (DelegationKindV1::User, OperationDelegationV1::User)
                | (DelegationKindV1::Service, OperationDelegationV1::Service)
                | (_, OperationDelegationV1::Either)
        );
        if !delegation_matches {
            return Err(ProtocolValidationError::InvalidField {
                field: "principal.delegation",
                reason: "does not satisfy the operation descriptor".to_owned(),
            });
        }
        if self
            .principal
            .granted_capabilities
            .binary_search(&descriptor.required_capability)
            .is_err()
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "principal.granted_capabilities",
                reason: "does not contain the operation's required capability".to_owned(),
            });
        }
        if descriptor.tenant_scoped && self.principal.tenant_id.trim().is_empty() {
            return Err(ProtocolValidationError::InvalidField {
                field: "principal.tenant_id",
                reason: "is required by the tenant-scoped operation".to_owned(),
            });
        }
        if descriptor.workspace_scoped && self.principal.workspace_id.trim().is_empty() {
            return Err(ProtocolValidationError::InvalidField {
                field: "principal.workspace_id",
                reason: "is required by the workspace-scoped operation".to_owned(),
            });
        }
        match descriptor.kind {
            OperationKindV1::Command if self.idempotency_key.is_none() => {
                return Err(ProtocolValidationError::InvalidField {
                    field: "idempotency_key",
                    reason: "is required for command operations".to_owned(),
                });
            }
            OperationKindV1::Query if self.idempotency_key.is_some() => {
                return Err(ProtocolValidationError::InvalidField {
                    field: "idempotency_key",
                    reason: "must not be used for query operations".to_owned(),
                });
            }
            _ => {}
        }
        Ok(())
    }

    /// Validate shape, operation authorization and the time-bound grant at a
    /// caller-supplied trusted clock instant.
    pub fn validate_at(
        &self,
        now_unix_ms: u64,
        descriptor: &OperationDescriptorV1,
    ) -> Result<(), ProtocolValidationError> {
        self.validate_for(descriptor)?;
        if self.effective_deadline_unix_ms() <= now_unix_ms {
            return Err(ProtocolValidationError::InvalidField {
                field: "invocation_expiry",
                reason: "deadline or authorization grant has expired".to_owned(),
            });
        }
        Ok(())
    }

    /// The earliest instant at which either the invocation deadline or the
    /// projected authorization grant expires.
    #[must_use]
    pub fn effective_deadline_unix_ms(&self) -> u64 {
        self.principal
            .expires_at_unix_ms
            .map_or(self.deadline_unix_ms, |expires_at| {
                self.deadline_unix_ms.min(expires_at)
            })
    }

    pub fn append_authority(&mut self, authority: String) -> Result<(), ProtocolValidationError> {
        require_authority(&authority)?;
        if self.call_chain.iter().any(|entry| entry == &authority)
            || self.call_chain.len() >= usize::from(self.max_hops)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "call_chain",
                reason: "CALL_CYCLE_DETECTED".to_owned(),
            });
        }
        self.call_chain.push(authority);
        Ok(())
    }
}

impl ProtocolValidate for AppInvocationEnvelopeV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppInvocationEnvelopeV1", self.schema_version)?;
        require_bounded("operation_id", &self.operation_id, 256)?;
        require_bounded("request_id", &self.request_id, 128)?;
        require_bounded("correlation_id", &self.correlation_id, 128)?;
        if let Some(causation_id) = &self.causation_id {
            require_bounded("causation_id", causation_id, 128)?;
        }
        if self.deadline_unix_ms == 0 {
            return Err(ProtocolValidationError::InvalidField {
                field: "deadline_unix_ms",
                reason: "must be non-zero".to_owned(),
            });
        }
        if let Some(key) = &self.idempotency_key {
            require_bounded("idempotency_key", key, 256)?;
        }
        if self.max_hops == 0 || self.max_hops > 4 {
            return Err(ProtocolValidationError::InvalidField {
                field: "max_hops",
                reason: "must be between 1 and 4".to_owned(),
            });
        }
        if self.call_chain.len() > usize::from(self.max_hops) {
            return Err(ProtocolValidationError::InvalidField {
                field: "call_chain",
                reason: "exceeds max_hops".to_owned(),
            });
        }
        require_unique("call_chain", self.call_chain.iter().map(String::as_str))?;
        for authority in &self.call_chain {
            require_authority(authority)?;
        }
        self.input_schema_digest
            .validate_value("input_schema_digest")?;
        self.principal.validate()?;
        self.execution.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppProviderRequestV1 {
    pub envelope: AppInvocationEnvelopeV1,
}

impl ProtocolValidate for AppProviderRequestV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        self.envelope.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppProviderResponseV1 {
    pub schema_version: u16,
    pub request_id: String,
    pub output_schema_digest: Sha256Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub payload: Value,
}

impl ProtocolValidate for AppProviderResponseV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppProviderResponseV1", self.schema_version)?;
        require_bounded("request_id", &self.request_id, 128)?;
        self.output_schema_digest
            .validate_value("output_schema_digest")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatusV1 {
    Accepted,
    Completed,
    Rejected,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DurableReceiptV1 {
    pub schema_version: u16,
    pub request_id: String,
    pub receipt_id: String,
    pub idempotency_key: String,
    pub status: ReceiptStatusV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_revision: Option<String>,
    pub replayed: bool,
    pub payload_digest: Sha256Digest,
    pub payload: Value,
}

impl ProtocolValidate for DurableReceiptV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("DurableReceiptV1", self.schema_version)?;
        require_bounded("request_id", &self.request_id, 128)?;
        require_bounded("receipt_id", &self.receipt_id, 256)?;
        require_bounded("idempotency_key", &self.idempotency_key, 256)?;
        self.payload_digest.validate_value("payload_digest")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppArtifactRefV1 {
    pub artifact_id: String,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub row_count: u64,
    pub created_unix_ms: u64,
    pub expires_unix_ms: u64,
    pub media_type: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl ProtocolValidate for AppArtifactRefV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("artifact_id", &self.artifact_id, 256)?;
        require_bounded("media_type", &self.media_type, 256)?;
        self.schema_digest.validate_value("schema_digest")?;
        self.content_digest.validate_value("content_digest")?;
        if self.created_unix_ms == 0 || self.expires_unix_ms <= self.created_unix_ms {
            return Err(ProtocolValidationError::InvalidField {
                field: "artifact_expiry",
                reason: "expiry must be later than creation".to_owned(),
            });
        }
        Ok(())
    }
}

pub type CoreBridgeOperationV1 = OperationDescriptorV1;
/// CoreBridge reverse calls use the exact same verified authorization
/// envelope as Gateway-to-APP calls; this alias introduces no second wire DTO.
pub type CoreBridgeInvocationV1 = AppInvocationEnvelopeV1;

fn require_authority(value: &str) -> Result<(), ProtocolValidationError> {
    require_bounded("call_chain.authority", value, 192)?;
    let valid = value.split_once(':').is_some_and(|(kind, identity)| {
        matches!(kind, "core" | "app" | "surface") && !identity.trim().is_empty()
    });
    if !valid {
        return Err(ProtocolValidationError::InvalidField {
            field: "call_chain.authority",
            reason: "must be core:<id>, app:<id>, or surface:<id>".to_owned(),
        });
    }
    Ok(())
}

#[allow(dead_code)]
fn _assert_digest_helper(value: &str) -> Result<(), ProtocolValidationError> {
    require_digest("digest", value)
}
