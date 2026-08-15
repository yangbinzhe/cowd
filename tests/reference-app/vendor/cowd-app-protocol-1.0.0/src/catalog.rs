use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::digest::canonical_digest_v1;
use crate::{
    require_bounded, require_protocol, require_schema, require_unique, AppId, AppManifestV1,
    GenerationId, OperationDescriptorV1, ProtocolValidate, ProtocolValidationError, Sha256Digest,
};

const CORE_OPERATION_CATALOG_DOMAIN_V1: &str = "cowd.core.operation-catalog/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
/// Gateway-generated, APP-scoped projection of Core operation descriptors.
///
/// Gateway preserves Core-owned schemas, kind, limits, delegation, audit policy
/// and required capabilities, then adds the signed APP capability named by the
/// matching `CoreBridgeRequirementV1`.
pub struct CoreOperationCatalogV1 {
    pub schema_version: u16,
    pub protocol_revision: u16,
    pub app_id: AppId,
    pub generation: GenerationId,
    pub catalog_digest: Sha256Digest,
    pub operations: Vec<OperationDescriptorV1>,
}

impl CoreOperationCatalogV1 {
    pub fn canonical_catalog_digest(&self) -> Result<Sha256Digest, ProtocolValidationError> {
        let mut payload = serde_json::to_value(self)
            .map_err(|error| ProtocolValidationError::InvalidJson(error.to_string()))?;
        payload
            .as_object_mut()
            .ok_or_else(|| {
                ProtocolValidationError::InvalidJson("catalog is not an object".to_owned())
            })?
            .remove("catalog_digest");
        canonical_digest_v1(CORE_OPERATION_CATALOG_DOMAIN_V1, &payload)
    }

    pub fn bind_canonical_catalog_digest(
        &mut self,
    ) -> Result<Sha256Digest, ProtocolValidationError> {
        let digest = self.canonical_catalog_digest()?;
        self.catalog_digest = digest.clone();
        Ok(digest)
    }

    pub fn validate_for_manifest(
        &self,
        manifest: &AppManifestV1,
        expected_generation: &GenerationId,
    ) -> Result<(), ProtocolValidationError> {
        self.validate()?;
        manifest.validate()?;
        if self.app_id != manifest.app_id {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_operation_catalog.app_id",
                reason: "does not match the signed APP manifest".to_owned(),
            });
        }
        if &self.generation != expected_generation {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_operation_catalog.generation",
                reason: "does not match the mounted APP generation".to_owned(),
            });
        }
        if self.operations.len() != manifest.core_bridge_requirements.len() {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_operation_catalog.operations",
                reason: "must contain exactly the APP-authorized operation subset".to_owned(),
            });
        }
        for requirement in &manifest.core_bridge_requirements {
            let descriptor = self
                .operations
                .binary_search_by(|operation| {
                    operation
                        .operation_id
                        .as_str()
                        .cmp(&requirement.core_operation_id)
                })
                .ok()
                .map(|index| &self.operations[index])
                .ok_or_else(|| ProtocolValidationError::InvalidField {
                    field: "core_operation_catalog.operations",
                    reason: "is missing a signed core operation requirement".to_owned(),
                })?;
            if descriptor.input_schema_digest != requirement.accepted_input_schema_digest
                || descriptor.output_schema_digest != requirement.accepted_output_schema_digest
            {
                return Err(ProtocolValidationError::InvalidField {
                    field: "core_operation_catalog.operations.schema_digest",
                    reason: "does not match the signed APP requirement".to_owned(),
                });
            }
            if descriptor.kind != requirement.kind || descriptor.streaming != requirement.streaming
            {
                return Err(ProtocolValidationError::InvalidField {
                    field: "core_operation_catalog.operations.kind",
                    reason: "kind or streaming mode does not match the signed APP requirement"
                        .to_owned(),
                });
            }
            if descriptor
                .required_capabilities
                .binary_search(&requirement.required_app_capability)
                .is_err()
            {
                return Err(ProtocolValidationError::InvalidField {
                    field: "core_operation_catalog.operations.required_capabilities",
                    reason:
                        "must include the signed APP capability projected onto the Core descriptor"
                            .to_owned(),
                });
            }
            let app_namespace = format!("{}.", manifest.app_id.0);
            if descriptor.required_capabilities.iter().any(|capability| {
                capability.starts_with(&app_namespace)
                    && capability != &requirement.required_app_capability
            }) {
                return Err(ProtocolValidationError::InvalidField {
                    field: "core_operation_catalog.operations.required_capabilities",
                    reason: "must not add an unsigned capability from the APP namespace".to_owned(),
                });
            }
        }
        Ok(())
    }
}

impl ProtocolValidate for CoreOperationCatalogV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("CoreOperationCatalogV1", self.schema_version)?;
        require_protocol(self.protocol_revision)?;
        self.app_id.validate_value()?;
        self.generation.validate_value()?;
        self.catalog_digest.validate_value("catalog_digest")?;
        if self.operations.len() > 1024 {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_operation_catalog.operations",
                reason: "must contain at most 1024 operations".to_owned(),
            });
        }
        require_unique(
            "core_operation_catalog.operations",
            self.operations
                .iter()
                .map(|operation| operation.operation_id.as_str()),
        )?;
        if self
            .operations
            .windows(2)
            .any(|pair| pair[0].operation_id >= pair[1].operation_id)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_operation_catalog.operations",
                reason: "must be sorted by operation_id in strictly ascending order".to_owned(),
            });
        }
        for operation in &self.operations {
            operation.validate()?;
        }
        if self.catalog_digest != self.canonical_catalog_digest()? {
            return Err(ProtocolValidationError::InvalidField {
                field: "catalog_digest",
                reason: "does not match the canonical APP-scoped operation catalog".to_owned(),
            });
        }
        Ok(())
    }
}

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
