use std::{borrow::Cow, collections::BTreeSet, fmt};

use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::PROTOCOL_REVISION_V1;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolValidationError {
    #[error("invalid {field}: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("unsupported schema version for {type_name}: {actual}")]
    UnsupportedSchema {
        type_name: &'static str,
        actual: u16,
    },
    #[error("unsupported protocol revision: {actual}")]
    UnsupportedProtocol { actual: u16 },
    #[error("duplicate value in {field}: {value}")]
    Duplicate { field: &'static str, value: String },
    #[error("invalid JSON: {0}")]
    InvalidJson(String),
}

pub trait ProtocolValidate {
    fn validate(&self) -> Result<(), ProtocolValidationError>;
}

pub fn decode_strict<T>(bytes: &[u8]) -> Result<T, ProtocolValidationError>
where
    T: DeserializeOwned + ProtocolValidate,
{
    let value: T = serde_json::from_slice(bytes)
        .map_err(|error| ProtocolValidationError::InvalidJson(error.to_string()))?;
    value.validate()?;
    Ok(value)
}

pub(crate) fn require_schema(
    type_name: &'static str,
    actual: u16,
) -> Result<(), ProtocolValidationError> {
    if actual == 1 {
        Ok(())
    } else {
        Err(ProtocolValidationError::UnsupportedSchema { type_name, actual })
    }
}

pub(crate) fn require_protocol(actual: u16) -> Result<(), ProtocolValidationError> {
    if actual == PROTOCOL_REVISION_V1 {
        Ok(())
    } else {
        Err(ProtocolValidationError::UnsupportedProtocol { actual })
    }
}

pub(crate) fn require_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), ProtocolValidationError> {
    if value.trim().is_empty() {
        Err(ProtocolValidationError::InvalidField {
            field,
            reason: "must not be empty".to_owned(),
        })
    } else {
        Ok(())
    }
}

pub(crate) fn require_bounded(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), ProtocolValidationError> {
    require_non_empty(field, value)?;
    if value.len() > maximum {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: format!("must be at most {maximum} bytes"),
        });
    }
    Ok(())
}

pub(crate) fn require_digest(
    field: &'static str,
    value: &str,
) -> Result<(), ProtocolValidationError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: "must start with sha256:".to_owned(),
        });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: "must contain exactly 64 hexadecimal characters".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn require_unique<'a>(
    field: &'static str,
    values: impl IntoIterator<Item = &'a str>,
) -> Result<(), ProtocolValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ProtocolValidationError::Duplicate {
                field,
                value: value.to_owned(),
            });
        }
    }
    Ok(())
}

pub(crate) fn require_canonical_string_set(
    field: &'static str,
    values: &[String],
    maximum_items: usize,
    maximum_item_bytes: usize,
) -> Result<(), ProtocolValidationError> {
    if values.len() > maximum_items {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: format!("must contain at most {maximum_items} values"),
        });
    }
    for value in values {
        require_bounded(field, value, maximum_item_bytes)?;
    }
    require_unique(field, values.iter().map(String::as_str))?;
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: "must be in strictly ascending canonical order".to_owned(),
        });
    }
    Ok(())
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct AppId(pub String);

impl AppId {
    pub fn validate_value(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("app_id", &self.0, 64)?;
        if !self.0.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'-' | b'_'))
        }) {
            return Err(ProtocolValidationError::InvalidField {
                field: "app_id",
                reason: "must use lowercase ASCII letters, digits, '-' or '_'".to_owned(),
            });
        }
        Ok(())
    }
}

impl fmt::Display for AppId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct GenerationId(pub String);

impl GenerationId {
    pub fn validate_value(&self) -> Result<(), ProtocolValidationError> {
        require_digest("generation", &self.0)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct Sha256Digest(pub String);

impl Sha256Digest {
    pub fn validate_value(&self, field: &'static str) -> Result<(), ProtocolValidationError> {
        require_digest(field, &self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProtocolRangeV1 {
    pub minimum: u16,
    pub maximum: u16,
}

impl ProtocolRangeV1 {
    #[must_use]
    pub const fn exact_v1() -> Self {
        Self {
            minimum: PROTOCOL_REVISION_V1,
            maximum: PROTOCOL_REVISION_V1,
        }
    }
}

impl ProtocolValidate for ProtocolRangeV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.minimum == 0 || self.minimum > self.maximum {
            return Err(ProtocolValidationError::InvalidField {
                field: "protocol_range",
                reason: "minimum must be non-zero and no greater than maximum".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalContextV1 {
    pub subject: String,
    pub tenant_id: String,
    pub workspace_id: String,
    pub delegation: DelegationKindV1,
    pub grant_id: String,
    pub authorization_profile_id: String,
    pub authorization_revision: u64,
    pub granted_capabilities: Vec<String>,
    pub granted_scopes: Vec<String>,
    pub credential_epoch: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expires_at_unix_ms: Option<u64>,
}

impl ProtocolValidate for PrincipalContextV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("principal.subject", &self.subject, 256)?;
        if self.tenant_id.len() > 128 {
            return Err(ProtocolValidationError::InvalidField {
                field: "principal.tenant_id",
                reason: "must be at most 128 bytes".to_owned(),
            });
        }
        if self.workspace_id.len() > 256 {
            return Err(ProtocolValidationError::InvalidField {
                field: "principal.workspace_id",
                reason: "must be at most 256 bytes".to_owned(),
            });
        }
        require_bounded("principal.grant_id", &self.grant_id, 256)?;
        require_bounded(
            "principal.authorization_profile_id",
            &self.authorization_profile_id,
            128,
        )?;
        require_canonical_string_set(
            "principal.granted_capabilities",
            &self.granted_capabilities,
            128,
            256,
        )?;
        require_canonical_string_set("principal.granted_scopes", &self.granted_scopes, 128, 256)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DelegationKindV1 {
    User,
    Service,
}

#[allow(dead_code)]
#[derive(JsonSchema)]
#[serde(deny_unknown_fields)]
struct PrincipalContextV1Schema {
    subject: String,
    tenant_id: String,
    workspace_id: String,
    delegation: DelegationKindV1,
    grant_id: String,
    authorization_profile_id: String,
    authorization_revision: u64,
    granted_capabilities: Vec<String>,
    granted_scopes: Vec<String>,
    credential_epoch: u64,
    expires_at_unix_ms: RequiredNullableU64Schema,
}

#[allow(dead_code)]
struct RequiredNullableU64Schema;

impl JsonSchema for RequiredNullableU64Schema {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("RequiredNullableU64")
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": ["integer", "null"],
            "format": "uint64",
            "minimum": 0
        })
    }
}

impl JsonSchema for PrincipalContextV1 {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("PrincipalContextV1")
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        PrincipalContextV1Schema::json_schema(generator)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContextV1 {
    pub surface: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

impl ProtocolValidate for ExecutionContextV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("execution.surface", &self.surface, 128)?;
        for (field, value) in [
            ("session_id", self.session_id.as_deref()),
            ("turn_id", self.turn_id.as_deref()),
            ("task_id", self.task_id.as_deref()),
        ] {
            if let Some(value) = value {
                require_bounded(field, value, 256)?;
            }
        }
        Ok(())
    }
}
