use std::collections::BTreeMap;
use std::path::{Component, Path};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::digest::canonical_digest_v1;
use crate::{
    app_operation_catalog_digest_v1, app_tui_view_action_request_schema_digest_v1,
    app_tui_view_action_response_schema_digest_v1, app_tui_view_open_request_schema_digest_v1,
    app_tui_view_open_response_schema_digest_v1, app_tui_view_patch_schema_digest_v1,
    app_tui_view_stream_request_schema_digest_v1, require_bounded, require_canonical_string_set,
    require_protocol, require_schema, require_unique, AppId, GenerationId, OperationDescriptorV1,
    OperationKindV1, ProtocolRangeV1, ProtocolValidate, ProtocolValidationError, Sha256Digest,
};

const APP_MANIFEST_SIGNED_PAYLOAD_DOMAIN_V1: &str = "cowd.app.manifest.signed-payload/v1";
const APP_MANIFEST_CAPABILITY_DOMAIN_V1: &str = "cowd.app.manifest.capabilities/v1";
const APP_MANIFEST_AUTHORIZATION_PROFILE_DOMAIN_V1: &str =
    "cowd.app.manifest.authorization-profiles/v1";

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
    pub operation_catalog_digest: Sha256Digest,
    pub core_bridge_requirements: Vec<CoreBridgeRequirementV1>,
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
        require_canonical_string_set("capabilities", &self.capabilities, 1024, 192)?;
        require_app_capability_namespace(
            "capabilities",
            &self.app_id,
            self.capabilities.iter().map(String::as_str),
        )?;
        validate_manifest_authorization_profiles(self)?;
        self.operation_catalog_digest
            .validate_value("operation_catalog_digest")?;
        require_canonical_requirements(&self.core_bridge_requirements)?;
        for requirement in &self.core_bridge_requirements {
            requirement.validate()?;
            let app_namespace = format!("{}.", self.app_id.0);
            if !requirement.app_operation_id.starts_with(&app_namespace)
                || requirement.app_operation_id.len() == app_namespace.len()
            {
                return Err(ProtocolValidationError::InvalidField {
                    field: "core_bridge_requirements.app_operation_id",
                    reason: format!("must use the `{app_namespace}` APP namespace"),
                });
            }
            for capability in &requirement.required_app_capabilities {
                if !capability.starts_with(&app_namespace)
                    || self.capabilities.binary_search(capability).is_err()
                {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "core_bridge_requirements.required_app_capabilities",
                        reason: "must be APP-namespaced capabilities declared by the manifest"
                            .to_owned(),
                    });
                }
            }
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
        match (&self.presentation, self.surfaces.tui_view) {
            (Some(presentation), tui_enabled) => {
                presentation.validate_for(&self.app_id, tui_enabled)?;
            }
            (None, true) => {
                return Err(ProtocolValidationError::InvalidField {
                    field: "presentation",
                    reason: "is required when the TUI view surface is enabled".to_owned(),
                });
            }
            (None, false) => {}
        }
        let expected = self.canonical_signed_digest()?;
        if self.integrity.manifest_digest != expected {
            return Err(ProtocolValidationError::InvalidField {
                field: "integrity.manifest_digest",
                reason: "does not match the canonical signed manifest payload".to_owned(),
            });
        }
        if self.signature.signed_digest != expected {
            return Err(ProtocolValidationError::InvalidField {
                field: "signature.signed_digest",
                reason: "does not match the canonical signed manifest payload".to_owned(),
            });
        }
        Ok(())
    }
}

impl AppManifestV1 {
    pub fn canonical_signed_digest(&self) -> Result<Sha256Digest, ProtocolValidationError> {
        let mut payload = serde_json::to_value(self)
            .map_err(|error| ProtocolValidationError::InvalidJson(error.to_string()))?;
        let object = payload.as_object_mut().ok_or_else(|| {
            ProtocolValidationError::InvalidJson("manifest is not an object".to_owned())
        })?;
        object
            .get_mut("integrity")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| {
                ProtocolValidationError::InvalidJson(
                    "manifest integrity is not an object".to_owned(),
                )
            })?
            .remove("manifest_digest");
        let signature = object
            .get_mut("signature")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| {
                ProtocolValidationError::InvalidJson(
                    "manifest signature is not an object".to_owned(),
                )
            })?;
        signature.remove("signature");
        signature.remove("signed_digest");
        canonical_digest_v1(APP_MANIFEST_SIGNED_PAYLOAD_DOMAIN_V1, &payload)
    }

    pub fn bind_canonical_signed_digest(
        &mut self,
    ) -> Result<Sha256Digest, ProtocolValidationError> {
        let digest = self.canonical_signed_digest()?;
        self.integrity.manifest_digest = digest.clone();
        self.signature.signed_digest = digest.clone();
        Ok(digest)
    }
}

pub fn manifest_capability_digest_v1(
    manifest: &AppManifestV1,
) -> Result<Sha256Digest, ProtocolValidationError> {
    manifest.app_id.validate_value()?;
    require_canonical_string_set("capabilities", &manifest.capabilities, 1024, 192)?;
    require_app_capability_namespace(
        "capabilities",
        &manifest.app_id,
        manifest.capabilities.iter().map(String::as_str),
    )?;
    canonical_digest_v1(
        APP_MANIFEST_CAPABILITY_DOMAIN_V1,
        &(
            manifest.schema_version,
            &manifest.app_id,
            &manifest.capabilities,
        ),
    )
}

pub fn manifest_authorization_profile_digest_v1(
    manifest: &AppManifestV1,
) -> Result<Sha256Digest, ProtocolValidationError> {
    manifest_capability_digest_v1(manifest)?;
    validate_manifest_authorization_profiles(manifest)?;
    canonical_digest_v1(
        APP_MANIFEST_AUTHORIZATION_PROFILE_DOMAIN_V1,
        &(
            manifest.schema_version,
            &manifest.app_id,
            &manifest.authorization_profiles,
        ),
    )
}

fn validate_manifest_authorization_profiles(
    manifest: &AppManifestV1,
) -> Result<(), ProtocolValidationError> {
    let profiles = &manifest.authorization_profiles;
    if profiles.len() > 256 {
        return Err(ProtocolValidationError::InvalidField {
            field: "authorization_profiles",
            reason: "must contain at most 256 profiles".to_owned(),
        });
    }
    require_unique(
        "authorization_profiles",
        profiles.iter().map(|profile| profile.profile_id.as_str()),
    )?;
    if profiles
        .windows(2)
        .any(|pair| pair[0].profile_id >= pair[1].profile_id)
    {
        return Err(ProtocolValidationError::InvalidField {
            field: "authorization_profiles",
            reason: "must be sorted by profile_id in strictly ascending order".to_owned(),
        });
    }
    let defaults = profiles.iter().filter(|profile| profile.is_default).count();
    if (!profiles.is_empty() && defaults != 1) || (profiles.is_empty() && defaults != 0) {
        return Err(ProtocolValidationError::InvalidField {
            field: "authorization_profiles.is_default",
            reason: "non-empty profiles must contain exactly one default".to_owned(),
        });
    }
    for profile in profiles {
        profile.validate()?;
        require_app_capability_namespace(
            "authorization_profile.capabilities",
            &manifest.app_id,
            profile.capabilities.iter().map(String::as_str),
        )?;
        require_app_capability_namespace(
            "authorization_profile.surface_capabilities",
            &manifest.app_id,
            profile
                .surface_capabilities
                .values()
                .flatten()
                .map(String::as_str),
        )?;
        for capability in profile
            .capabilities
            .iter()
            .chain(profile.surface_capabilities.values().flatten())
        {
            if manifest.capabilities.binary_search(capability).is_err() {
                return Err(ProtocolValidationError::InvalidField {
                    field: "authorization_profile.capabilities",
                    reason: "must be declared by the APP manifest".to_owned(),
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CoreBridgeRequirementV1 {
    pub app_operation_id: String,
    pub core_operation_id: String,
    pub accepted_input_schema_digest: Sha256Digest,
    pub accepted_output_schema_digest: Sha256Digest,
    #[schemars(length(min = 1, max = 64))]
    pub required_app_capabilities: Vec<String>,
    pub kind: OperationKindV1,
    pub streaming: bool,
}

impl ProtocolValidate for CoreBridgeRequirementV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded(
            "core_bridge_requirement.app_operation_id",
            &self.app_operation_id,
            256,
        )?;
        require_bounded(
            "core_bridge_requirement.core_operation_id",
            &self.core_operation_id,
            256,
        )?;
        if self.app_operation_id.starts_with("core.") {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_bridge_requirement.app_operation_id",
                reason: "must use the APP operation namespace, not the Core namespace".to_owned(),
            });
        }
        if !self.core_operation_id.starts_with("core.") {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_bridge_requirement.core_operation_id",
                reason: "must start with 'core.'".to_owned(),
            });
        }
        self.accepted_input_schema_digest
            .validate_value("core_bridge_requirement.accepted_input_schema_digest")?;
        self.accepted_output_schema_digest
            .validate_value("core_bridge_requirement.accepted_output_schema_digest")?;
        require_canonical_string_set(
            "core_bridge_requirement.required_app_capabilities",
            &self.required_app_capabilities,
            64,
            256,
        )?;
        if self.required_app_capabilities.is_empty() {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_bridge_requirement.required_app_capabilities",
                reason: "must contain at least one capability".to_owned(),
            });
        }
        let must_stream = matches!(
            self.kind,
            OperationKindV1::Subscribe | OperationKindV1::Export
        );
        if self.streaming != must_stream {
            return Err(ProtocolValidationError::InvalidField {
                field: "core_bridge_requirement.streaming",
                reason: "must agree with the operation kind".to_owned(),
            });
        }
        Ok(())
    }
}

fn require_app_capability_namespace<'a>(
    field: &'static str,
    app_id: &AppId,
    capabilities: impl IntoIterator<Item = &'a str>,
) -> Result<(), ProtocolValidationError> {
    let namespace = format!("{}.", app_id.0);
    if capabilities.into_iter().any(|capability| {
        capability
            .strip_prefix(&namespace)
            .is_none_or(str::is_empty)
    }) {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: format!("must remain within the APP-owned '{namespace}' namespace"),
        });
    }
    Ok(())
}

fn require_canonical_requirements(
    requirements: &[CoreBridgeRequirementV1],
) -> Result<(), ProtocolValidationError> {
    if requirements.len() > 1024 {
        return Err(ProtocolValidationError::InvalidField {
            field: "core_bridge_requirements",
            reason: "must contain at most 1024 requirements".to_owned(),
        });
    }
    if requirements.windows(2).any(|pair| {
        (&pair[0].app_operation_id, &pair[0].core_operation_id)
            >= (&pair[1].app_operation_id, &pair[1].core_operation_id)
    }) {
        return Err(ProtocolValidationError::InvalidField {
            field: "core_bridge_requirements",
            reason: "must be unique and sorted by (app_operation_id, core_operation_id)".to_owned(),
        });
    }
    Ok(())
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
        require_canonical_string_set(
            "authorization_profile.capabilities",
            &self.capabilities,
            1024,
            192,
        )?;
        if self.surface_capabilities.len() > 64 {
            return Err(ProtocolValidationError::InvalidField {
                field: "authorization_profile.surface_capabilities",
                reason: "must contain at most 64 surfaces".to_owned(),
            });
        }
        for (surface, capabilities) in &self.surface_capabilities {
            require_bounded("authorization_profile.surface", surface, 128)?;
            require_canonical_string_set(
                "authorization_profile.surface_capabilities",
                capabilities,
                1024,
                192,
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
    pub result_contracts: Vec<AppResultContractV1>,
    #[serde(default)]
    pub tui_views: Vec<AppTuiViewDescriptorV1>,
    #[serde(default)]
    pub core_navigation_kinds: Vec<String>,
}

impl AppPresentationV1 {
    fn validate_for(
        &self,
        app_id: &AppId,
        tui_enabled: bool,
    ) -> Result<(), ProtocolValidationError> {
        if self.result_shape_revision == 0 {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.result_shape_revision",
                reason: "must be non-zero".to_owned(),
            });
        }
        require_canonical_string_set(
            "presentation.core_navigation_kinds",
            &self.core_navigation_kinds,
            256,
            256,
        )?;
        if self.result_contracts.len() > 256 {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.result_contracts",
                reason: "must contain at most 256 contracts".to_owned(),
            });
        }
        if self
            .result_contracts
            .windows(2)
            .any(|pair| pair[0].contract_id >= pair[1].contract_id)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.result_contracts",
                reason: "must be sorted by contract_id in strictly ascending order".to_owned(),
            });
        }
        for contract in &self.result_contracts {
            contract.validate_for(app_id)?;
        }
        if self
            .tui_views
            .windows(2)
            .any(|pair| pair[0].view_id >= pair[1].view_id)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.tui_views",
                reason: "must be sorted by view_id in strictly ascending order".to_owned(),
            });
        }
        for view in &self.tui_views {
            view.validate_for(app_id)?;
        }
        if tui_enabled == self.tui_views.is_empty() {
            return Err(ProtocolValidationError::InvalidField {
                field: "surfaces.tui_view",
                reason: "must equal whether signed TUI view descriptors are present".to_owned(),
            });
        }
        Ok(())
    }

    fn validate_against_operations(
        &self,
        operations: &[OperationDescriptorV1],
    ) -> Result<(), ProtocolValidationError> {
        let operation = |operation_id: &str| {
            operations
                .binary_search_by_key(&operation_id, |candidate| candidate.operation_id.as_str())
                .ok()
                .map(|index| &operations[index])
                .ok_or_else(|| ProtocolValidationError::InvalidField {
                    field: "presentation.tui_views.operation_id",
                    reason: format!("operation `{operation_id}` is absent from the signed catalog"),
                })
        };
        let open_input = app_tui_view_open_request_schema_digest_v1()?;
        let open_output = app_tui_view_open_response_schema_digest_v1()?;
        let action_input = app_tui_view_action_request_schema_digest_v1()?;
        let action_output = app_tui_view_action_response_schema_digest_v1()?;
        let stream_input = app_tui_view_stream_request_schema_digest_v1()?;
        let stream_output = app_tui_view_patch_schema_digest_v1()?;
        for view in &self.tui_views {
            validate_tui_operation(
                operation(&view.open_operation_id)?,
                OperationKindV1::Query,
                &open_input,
                &open_output,
                "open_operation_id",
            )?;
            validate_tui_operation(
                operation(&view.action_operation_id)?,
                OperationKindV1::Command,
                &action_input,
                &action_output,
                "action_operation_id",
            )?;
            validate_tui_operation(
                operation(&view.stream_operation_id)?,
                OperationKindV1::Subscribe,
                &stream_input,
                &stream_output,
                "stream_operation_id",
            )?;
        }
        Ok(())
    }
}

/// A signed, bounded structured-result contract owned by one APP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppResultContractV1 {
    pub contract_id: String,
    pub schema_id: String,
    pub schema_version: u16,
    pub schema_digest: Sha256Digest,
    pub max_bytes: u64,
}

impl AppResultContractV1 {
    fn validate_for(&self, app_id: &AppId) -> Result<(), ProtocolValidationError> {
        require_bounded(
            "presentation.result_contracts.contract_id",
            &self.contract_id,
            256,
        )?;
        require_bounded(
            "presentation.result_contracts.schema_id",
            &self.schema_id,
            256,
        )?;
        let namespace = format!("{}.", app_id.0);
        if !self.contract_id.starts_with(&namespace) || self.contract_id.len() == namespace.len() {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.result_contracts.contract_id",
                reason: format!("must use the `{namespace}` APP namespace"),
            });
        }
        if self.schema_version == 0 {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.result_contracts.schema_version",
                reason: "must be non-zero".to_owned(),
            });
        }
        self.schema_digest
            .validate_value("presentation.result_contracts.schema_digest")?;
        if self.max_bytes == 0 || self.max_bytes > 64 * 1024 * 1024 {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.result_contracts.max_bytes",
                reason: "must be within 1..=64MiB".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppTuiViewDescriptorV1 {
    pub view_id: String,
    pub open_operation_id: String,
    pub action_operation_id: String,
    pub stream_operation_id: String,
}

impl AppTuiViewDescriptorV1 {
    fn validate_for(&self, app_id: &AppId) -> Result<(), ProtocolValidationError> {
        require_bounded("presentation.tui_views.view_id", &self.view_id, 256)?;
        let namespace = format!("{}.", app_id.0);
        for operation_id in std::iter::once(&self.open_operation_id)
            .chain(std::iter::once(&self.action_operation_id))
            .chain(std::iter::once(&self.stream_operation_id))
        {
            require_bounded("presentation.tui_views.operation_id", operation_id, 256)?;
            if !operation_id.starts_with(&namespace) || operation_id.len() == namespace.len() {
                return Err(ProtocolValidationError::InvalidField {
                    field: "presentation.tui_views.operation_id",
                    reason: format!("must use the `{namespace}` APP namespace"),
                });
            }
        }
        if self.open_operation_id == self.stream_operation_id
            || self.open_operation_id == self.action_operation_id
            || self.action_operation_id == self.stream_operation_id
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "presentation.tui_views.operation_id",
                reason: "open, action, and stream operation roles must be disjoint".to_owned(),
            });
        }
        Ok(())
    }
}

fn validate_tui_operation(
    operation: &OperationDescriptorV1,
    expected_kind: OperationKindV1,
    expected_input: &Sha256Digest,
    expected_output: &Sha256Digest,
    field: &'static str,
) -> Result<(), ProtocolValidationError> {
    if operation.kind != expected_kind
        || &operation.input_schema_digest != expected_input
        || &operation.output_schema_digest != expected_output
    {
        return Err(ProtocolValidationError::InvalidField {
            field,
            reason: "operation kind and TUI schema digests must match the frozen role".to_owned(),
        });
    }
    Ok(())
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
    pub operation_catalog_digest: Sha256Digest,
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
        let operation_catalog_digest =
            app_operation_catalog_digest_v1(&self.app_id, &self.operations)?;
        if self.operation_catalog_digest != operation_catalog_digest {
            return Err(ProtocolValidationError::InvalidField {
                field: "operation_catalog_digest",
                reason: "does not match the complete handshake operation catalog".to_owned(),
            });
        }
        Ok(())
    }
}

impl AppHandshakeV1 {
    /// Binds the live worker catalog to the signed manifest and validates every
    /// TUI route role against its frozen operation kind and schema digests.
    pub fn validate_against_manifest(
        &self,
        manifest: &AppManifestV1,
    ) -> Result<(), ProtocolValidationError> {
        self.validate()?;
        manifest.validate()?;
        if self.app_id != manifest.app_id || self.artifact_version != manifest.artifact_version {
            return Err(ProtocolValidationError::InvalidField {
                field: "handshake.identity",
                reason: "must match the signed manifest identity".to_owned(),
            });
        }
        if self.operation_catalog_digest != manifest.operation_catalog_digest {
            return Err(ProtocolValidationError::InvalidField {
                field: "operation_catalog_digest",
                reason: "does not match the signed manifest catalog digest".to_owned(),
            });
        }
        if self.capability_digest != manifest_capability_digest_v1(manifest)?
            || self.authorization_profile_digest
                != manifest_authorization_profile_digest_v1(manifest)?
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "handshake.authorization_digest",
                reason: "does not match the signed manifest authorization facts".to_owned(),
            });
        }
        for operation in &self.operations {
            for capability in &operation.required_capabilities {
                if manifest.capabilities.binary_search(capability).is_err() {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "operations.required_capabilities",
                        reason: "must be declared by the signed manifest".to_owned(),
                    });
                }
            }
        }
        for requirement in &manifest.core_bridge_requirements {
            let operation = self
                .operations
                .binary_search_by_key(&requirement.app_operation_id.as_str(), |candidate| {
                    candidate.operation_id.as_str()
                })
                .ok()
                .map(|index| &self.operations[index])
                .ok_or_else(|| ProtocolValidationError::InvalidField {
                    field: "core_bridge_requirements.app_operation_id",
                    reason: "must exist in the signed APP operation catalog".to_owned(),
                })?;
            if requirement
                .required_app_capabilities
                .iter()
                .any(|capability| {
                    operation
                        .required_capabilities
                        .binary_search(capability)
                        .is_err()
                })
            {
                return Err(ProtocolValidationError::InvalidField {
                    field: "core_bridge_requirements.required_app_capabilities",
                    reason: "must be included by the originating APP operation descriptor"
                        .to_owned(),
                });
            }
        }
        if let Some(presentation) = &manifest.presentation {
            presentation.validate_against_operations(&self.operations)?;
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
