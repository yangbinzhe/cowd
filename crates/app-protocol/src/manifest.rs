use std::collections::BTreeMap;
use std::path::{Component, Path};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    require_bounded, require_protocol, require_schema, require_unique, AppId, GenerationId,
    OperationDescriptorV1, ProtocolRangeV1, ProtocolValidate, ProtocolValidationError,
    Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppManifestV1 {
    pub schema_version: u16,
    pub app_id: AppId,
    pub display_name: String,
    pub artifact_version: String,
    pub required_protocol: ProtocolRangeV1,
    pub executable: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_root: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub authorization_profiles: Vec<AuthorizationProfileV1>,
    pub surfaces: AppSurfacesV1,
    pub integrity: BundleIntegrityV1,
    pub signature: BundleSignatureV1,
    pub sandbox: SandboxProfileV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<AppPresentationV1>,
}

impl ProtocolValidate for AppManifestV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppManifestV1", self.schema_version)?;
        self.app_id.validate_value()?;
        require_bounded("display_name", &self.display_name, 128)?;
        require_bounded("artifact_version", &self.artifact_version, 64)?;
        self.required_protocol.validate()?;
        require_relative_bundle_path("executable", &self.executable)?;
        if let Some(web_root) = &self.web_root {
            require_relative_bundle_path("web_root", web_root)?;
        }
        require_unique("capabilities", self.capabilities.iter().map(String::as_str))?;
        for capability in &self.capabilities {
            require_bounded("capability", capability, 192)?;
        }
        require_unique(
            "authorization_profiles",
            self.authorization_profiles
                .iter()
                .map(|profile| profile.profile_id.as_str()),
        )?;
        for profile in &self.authorization_profiles {
            profile.validate()?;
        }
        self.integrity.validate()?;
        self.signature.validate()?;
        self.sandbox.validate()?;
        if self.surfaces.web && self.web_root.is_none() {
            return Err(ProtocolValidationError::InvalidField {
                field: "web_root",
                reason: "is required when the web surface is enabled".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppSurfacesV1 {
    pub web: bool,
    pub tui_view: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationProfileV1 {
    pub profile_id: String,
    pub display_name: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub surface_capabilities: BTreeMap<String, Vec<String>>,
    pub is_default: bool,
}

impl ProtocolValidate for AuthorizationProfileV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("authorization_profile.profile_id", &self.profile_id, 128)?;
        require_bounded(
            "authorization_profile.display_name",
            &self.display_name,
            128,
        )?;
        require_unique(
            "authorization_profile.capabilities",
            self.capabilities.iter().map(String::as_str),
        )?;
        for capabilities in self.surface_capabilities.values() {
            require_unique(
                "authorization_profile.surface_capabilities",
                capabilities.iter().map(String::as_str),
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BundleIntegrityV1 {
    pub algorithm: IntegrityAlgorithmV1,
    pub files: BTreeMap<String, Sha256Digest>,
    pub manifest_digest: Sha256Digest,
}

impl ProtocolValidate for BundleIntegrityV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.files.is_empty() {
            return Err(ProtocolValidationError::InvalidField {
                field: "integrity.files",
                reason: "must include every executable and surface asset".to_owned(),
            });
        }
        for (path, digest) in &self.files {
            require_relative_bundle_path("integrity.files.path", path)?;
            digest.validate_value("integrity.files.digest")?;
        }
        self.manifest_digest
            .validate_value("integrity.manifest_digest")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityAlgorithmV1 {
    Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BundleSignatureV1 {
    pub algorithm: SignatureAlgorithmV1,
    pub key_id: String,
    pub signature: String,
    pub signed_digest: Sha256Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_digest: Option<Sha256Digest>,
}

impl ProtocolValidate for BundleSignatureV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("signature.key_id", &self.key_id, 256)?;
        require_bounded("signature.signature", &self.signature, 4096)?;
        self.signed_digest
            .validate_value("signature.signed_digest")?;
        if let Some(digest) = &self.provenance_digest {
            digest.validate_value("signature.provenance_digest")?;
        }
        if self.expires_unix_ms == Some(0) {
            return Err(ProtocolValidationError::InvalidField {
                field: "signature.expires_unix_ms",
                reason: "must be a non-zero timestamp".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignatureAlgorithmV1 {
    Ed25519,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SandboxProfileV1 {
    pub filesystem: FilesystemPolicyV1,
    pub network: NetworkPolicyV1,
    pub max_processes: u32,
    pub max_open_files: u32,
    pub max_memory_bytes: u64,
    pub cpu_quota_millis_per_second: u32,
}

impl ProtocolValidate for SandboxProfileV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.max_processes == 0
            || self.max_open_files < 16
            || self.max_memory_bytes < 16 * 1024 * 1024
            || self.cpu_quota_millis_per_second == 0
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "sandbox",
                reason: "resource limits must be non-zero and operationally viable".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemPolicyV1 {
    BundleReadOnlyDataReadWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkPolicyV1 {
    Deny,
    CapabilityAllowlist { capabilities: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppPresentationV1 {
    pub result_shape_revision: u16,
    #[serde(default)]
    pub view_ids: Vec<String>,
    #[serde(default)]
    pub core_navigation_kinds: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppHandshakeRequestV1 {
    pub schema_version: u16,
    pub protocol_revision: u16,
    pub app_id: AppId,
    pub generation: GenerationId,
    pub gateway_pid: u32,
    pub worker_pid: u32,
}

impl ProtocolValidate for AppHandshakeRequestV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppHandshakeRequestV1", self.schema_version)?;
        require_protocol(self.protocol_revision)?;
        self.app_id.validate_value()?;
        self.generation.validate_value()?;
        if self.gateway_pid == 0 || self.worker_pid == 0 {
            return Err(ProtocolValidationError::InvalidField {
                field: "pid",
                reason: "must be non-zero".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppHandshakeV1 {
    pub schema_version: u16,
    pub protocol_revision: u16,
    pub app_id: AppId,
    pub generation: GenerationId,
    pub artifact_version: String,
    pub worker_pid: u32,
    pub worker_nonce: String,
    pub operations: Vec<OperationDescriptorV1>,
    pub capability_digest: Sha256Digest,
    pub authorization_profile_digest: Sha256Digest,
}

impl ProtocolValidate for AppHandshakeV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppHandshakeV1", self.schema_version)?;
        require_protocol(self.protocol_revision)?;
        self.app_id.validate_value()?;
        self.generation.validate_value()?;
        require_bounded("artifact_version", &self.artifact_version, 64)?;
        require_bounded("worker_nonce", &self.worker_nonce, 512)?;
        self.capability_digest.validate_value("capability_digest")?;
        self.authorization_profile_digest
            .validate_value("authorization_profile_digest")?;
        require_unique(
            "operations",
            self.operations
                .iter()
                .map(|operation| operation.operation_id.as_str()),
        )?;
        for operation in &self.operations {
            operation.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppHealthV1 {
    pub schema_version: u16,
    pub app_id: AppId,
    pub generation: GenerationId,
    pub status: AppHealthStatusV1,
    #[serde(default)]
    pub checks: BTreeMap<String, HealthCheckV1>,
}

impl ProtocolValidate for AppHealthV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppHealthV1", self.schema_version)?;
        self.app_id.validate_value()?;
        self.generation.validate_value()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AppHealthStatusV1 {
    Starting,
    Ready,
    Degraded,
    Draining,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HealthCheckV1 {
    pub healthy: bool,
    pub message: String,
}

fn require_relative_bundle_path(
    field: &'static str,
    value: &str,
) -> Result<(), ProtocolValidationError> {
    require_bounded(field, value, 1024)?;
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: "must be a normalized relative path contained by the Bundle".to_owned(),
        });
    }
    Ok(())
}
