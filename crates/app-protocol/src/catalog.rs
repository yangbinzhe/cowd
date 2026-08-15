use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    require_bounded, require_protocol, require_schema, require_unique, AppId, GenerationId,
    ProtocolValidate, ProtocolValidationError, Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppCatalogV1 {
    pub schema_version: u16,
    pub protocol_revision: u16,
    pub protocol_digest: Sha256Digest,
    pub catalog_generation: Sha256Digest,
    pub apps: Vec<AppCatalogEntryV1>,
}

impl ProtocolValidate for AppCatalogV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppCatalogV1", self.schema_version)?;
        require_protocol(self.protocol_revision)?;
        self.protocol_digest.validate_value("protocol_digest")?;
        self.catalog_generation
            .validate_value("catalog_generation")?;
        require_unique(
            "catalog.apps",
            self.apps.iter().map(|app| app.app_id.0.as_str()),
        )?;
        for app in &self.apps {
            app.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppCatalogEntryV1 {
    pub app_id: AppId,
    pub display_name: String,
    pub artifact_version: String,
    pub generation: GenerationId,
    pub required: bool,
    pub activation: AppActivationPolicyV1,
    pub lifecycle: AppLifecycleV1,
    pub compatibility: AppCompatibilityV1,
    pub web_surface: AppWebSurfaceV1,
    pub effective_capabilities: Vec<String>,
    pub effective_authorization_profile: String,
}

impl ProtocolValidate for AppCatalogEntryV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        self.app_id.validate_value()?;
        self.generation.validate_value()?;
        require_bounded("display_name", &self.display_name, 128)?;
        require_bounded("artifact_version", &self.artifact_version, 64)?;
        require_bounded(
            "effective_authorization_profile",
            &self.effective_authorization_profile,
            128,
        )?;
        require_unique(
            "effective_capabilities",
            self.effective_capabilities.iter().map(String::as_str),
        )?;
        self.lifecycle.validate()?;
        self.web_surface.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AppActivationPolicyV1 {
    Lazy,
    Resident,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AppLifecycleStateV1 {
    Mounted,
    Starting,
    Ready,
    Idle,
    Stopping,
    Stopped,
    Failed,
    Invalid,
    CircuitOpen,
    ProtocolIncompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppLifecycleV1 {
    pub state: AppLifecycleStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ProtocolValidate for AppLifecycleV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        if let Some(code) = &self.reason_code {
            require_bounded("lifecycle.reason_code", code, 128)?;
        }
        if !self.retryable && self.retry_after_ms.is_some() {
            return Err(ProtocolValidationError::InvalidField {
                field: "lifecycle.retry_after_ms",
                reason: "is valid only for retryable states".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppCompatibilityV1 {
    pub status: AppCompatibilityStatusV1,
    pub gateway_supported_minimum: u16,
    pub gateway_supported_maximum: u16,
    pub app_required_minimum: u16,
    pub app_required_maximum: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AppCompatibilityStatusV1 {
    Compatible,
    ProtocolIncompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppWebSurfaceV1 {
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_path: Option<String>,
    pub bridge_revision: u16,
}

impl ProtocolValidate for AppWebSurfaceV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.available {
            let entry = self.entry_path.as_deref().ok_or_else(|| {
                ProtocolValidationError::InvalidField {
                    field: "web_surface.entry_path",
                    reason: "is required when the web surface is available".to_owned(),
                }
            })?;
            require_bounded("web_surface.entry_path", entry, 1024)?;
            if !entry.starts_with("/apps/") || entry.contains("..") {
                return Err(ProtocolValidationError::InvalidField {
                    field: "web_surface.entry_path",
                    reason: "must be a normalized /apps/<app_id>/ path".to_owned(),
                });
            }
        } else if self.entry_path.is_some() {
            return Err(ProtocolValidationError::InvalidField {
                field: "web_surface.entry_path",
                reason: "must be absent when the web surface is unavailable".to_owned(),
            });
        }
        Ok(())
    }
}
