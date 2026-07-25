//! Local authority for signed human identity envelopes and decision leases.
//!
//! This crate owns signing material.  Consumers receive only serialized
//! envelopes and public verification material; no caller can select the
//! principal kind, capabilities, or assurance encoded in a signed result.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use cowd_app_sdk::AppDescriptor;
use harness_contract::security::{
    DecisionLeaseClaims, PrincipalAssurance, PrincipalClaims, PrincipalKind, SignedDecisionLease,
    SignedPrincipalEnvelope, CORE_HUMAN_CAPABILITIES,
};
use ring::{
    digest,
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use subtle::ConstantTimeEq;
use thiserror::Error;

const KEY_FILE: &str = "authority.pk8";
const CREDENTIAL_STATE_FILE: &str = "credential-state.json";
const ENTITLEMENT_AUDIT_FILE: &str = "entitlement-audit.jsonl";
const CREDENTIAL_STATE_VERSION: u32 = 3;
const CATALOG_FILE: &str = "profile-catalog.json";
const KEY_ID: &str = "cowd-local-ed25519-v1";
const SOCKET_FILE: &str = "broker.sock";

/// Stable, product-neutral authorization catalogue. Product composition builds
/// this from the APP descriptors linked into the current Cowd binary; the
/// broker therefore never imports an APP contract or knows an APP's enum.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuthorizationCatalog {
    pub schema_version: u32,
    pub core_profiles: Vec<AuthorizationProfile>,
    pub apps: Vec<AuthorizationAppProfileCatalog>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuthorizationProfile {
    pub id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuthorizationAppProfileCatalog {
    pub app_id: String,
    pub default_profile_id: String,
    pub profiles: Vec<AuthorizationProfile>,
    #[serde(default)]
    pub surface_capabilities: BTreeMap<String, Vec<String>>,
}

/// Product-neutral projection returned to CLI, WebUI and TUI. APP selections
/// are key/value identifiers instead of product-specific Rust enums.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HumanEntitlementProjection {
    pub core_profile_id: String,
    #[serde(default)]
    pub app_profiles: BTreeMap<String, String>,
    pub profile_revision: u64,
    pub credential_epoch: u64,
    pub ceiling: Vec<String>,
    pub granted: Vec<String>,
    pub denied: Vec<String>,
}

impl AuthorizationCatalog {
    /// Compose the generic catalogue from already validated APP descriptors.
    pub fn from_app_descriptors(
        descriptors: impl IntoIterator<Item = AppDescriptor>,
    ) -> Result<Self, AuthBrokerError> {
        let mut apps = descriptors
            .into_iter()
            .filter_map(|descriptor| {
                descriptor
                    .profile
                    .map(|profile| AuthorizationAppProfileCatalog {
                        app_id: descriptor.id.as_str().to_string(),
                        default_profile_id: profile.default_profile_id,
                        profiles: profile
                            .profiles
                            .into_iter()
                            .map(|profile| AuthorizationProfile {
                                id: profile.id,
                                capabilities: profile.capabilities,
                            })
                            .collect(),
                        surface_capabilities: profile.surface_capabilities,
                    })
            })
            .collect::<Vec<_>>();
        apps.sort_by(|left, right| left.app_id.cmp(&right.app_id));
        let catalog = Self {
            schema_version: 1,
            core_profiles: vec![
                AuthorizationProfile {
                    id: "core_operator".to_string(),
                    capabilities: vec![
                        "approval.respond".to_string(),
                        "mission.observe".to_string(),
                    ],
                },
                AuthorizationProfile {
                    id: "core_manager".to_string(),
                    capabilities: CORE_HUMAN_CAPABILITIES
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                },
            ],
            apps,
        };
        catalog.validate()?;
        Ok(catalog)
    }

    #[must_use]
    pub fn default_selection(&self) -> (String, BTreeMap<String, String>) {
        let apps = self
            .apps
            .iter()
            .map(|app| (app.app_id.clone(), app.default_profile_id.clone()))
            .collect();
        ("core_operator".to_string(), apps)
    }

    pub fn capabilities_for(
        &self,
        core_profile_id: &str,
        app_profiles: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, AuthBrokerError> {
        let core = self
            .core_profiles
            .iter()
            .find(|profile| profile.id == core_profile_id)
            .ok_or_else(|| {
                AuthBrokerError::InvalidCredentialState(format!(
                    "unknown core profile {core_profile_id}"
                ))
            })?;
        if self.apps.len() != app_profiles.len()
            || self
                .apps
                .iter()
                .any(|app| !app_profiles.contains_key(&app.app_id))
        {
            return Err(AuthBrokerError::InvalidCredentialState(
                "app profile selection does not match the active APP catalogue".to_string(),
            ));
        }
        let mut capabilities = core.capabilities.clone();
        for app in &self.apps {
            let profile_id = app_profiles.get(&app.app_id).ok_or_else(|| {
                AuthBrokerError::InvalidCredentialState(format!(
                    "missing profile selection for app {}",
                    app.app_id
                ))
            })?;
            let profile = app
                .profiles
                .iter()
                .find(|profile| profile.id == *profile_id)
                .ok_or_else(|| {
                    AuthBrokerError::InvalidCredentialState(format!(
                        "unknown profile {profile_id} for app {}",
                        app.app_id
                    ))
                })?;
            capabilities.extend(profile.capabilities.clone());
        }
        capabilities.sort();
        capabilities.dedup();
        Ok(capabilities)
    }

    pub fn surface_capabilities(&self, surface_id: &str) -> Vec<String> {
        let mut capabilities = CORE_HUMAN_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        for app in &self.apps {
            if surface_id == "legacy_gateway" {
                // A bearer-only legacy Gateway request has no surface identity
                // header. Preserve the pre-APP behavior by accepting every
                // capability the selected APP profile exposes somewhere; the
                // route's own capability check remains the final guard.
                capabilities.extend(app.surface_capabilities.values().flatten().cloned());
            } else {
                capabilities.extend(
                    app.surface_capabilities
                        .get(surface_id)
                        .into_iter()
                        .flatten()
                        .cloned(),
                );
            }
        }
        capabilities.sort();
        capabilities.dedup();
        capabilities
    }

    pub fn digest(&self) -> Result<String, AuthBrokerError> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
        let value = digest::digest(&digest::SHA256, &encoded);
        Ok(format!("sha256:{}", hex(value.as_ref())))
    }

    pub fn validate(&self) -> Result<(), AuthBrokerError> {
        if self.schema_version != 1 || self.core_profiles.is_empty() {
            return Err(AuthBrokerError::InvalidCredentialState(
                "authorization catalogue schema is invalid".to_string(),
            ));
        }
        let mut ids = BTreeSet::new();
        for profile in &self.core_profiles {
            validate_catalog_profile("core", profile, &mut ids)?;
        }
        if !ids.contains("core_operator") || !ids.contains("core_manager") {
            return Err(AuthBrokerError::InvalidCredentialState(
                "authorization catalogue lacks required core profiles".to_string(),
            ));
        }
        let mut app_ids = BTreeSet::new();
        for app in &self.apps {
            if app.app_id.trim().is_empty() || !app_ids.insert(app.app_id.clone()) {
                return Err(AuthBrokerError::InvalidCredentialState(
                    "authorization catalogue has duplicate or empty APP ids".to_string(),
                ));
            }
            let mut profile_ids = BTreeSet::new();
            for profile in &app.profiles {
                validate_catalog_profile(&app.app_id, profile, &mut profile_ids)?;
            }
            if !profile_ids.contains(&app.default_profile_id) {
                return Err(AuthBrokerError::InvalidCredentialState(format!(
                    "APP {} has no valid default profile",
                    app.app_id
                )));
            }
            for capabilities in app.surface_capabilities.values() {
                if capabilities
                    .iter()
                    .any(|capability| capability.trim().is_empty())
                {
                    return Err(AuthBrokerError::InvalidCredentialState(format!(
                        "APP {} has an invalid surface capability",
                        app.app_id
                    )));
                }
            }
        }
        Ok(())
    }
}

fn validate_catalog_profile(
    owner: &str,
    profile: &AuthorizationProfile,
    ids: &mut BTreeSet<String>,
) -> Result<(), AuthBrokerError> {
    if profile.id.trim().is_empty()
        || !ids.insert(profile.id.clone())
        || profile
            .capabilities
            .iter()
            .any(|capability| capability.trim().is_empty())
    {
        return Err(AuthBrokerError::InvalidCredentialState(format!(
            "{owner} profile catalogue is invalid"
        )));
    }
    Ok(())
}

/// 运行 Cowd 单文件架构中的认证子进程角色。
///
/// 入口由主 `cowd` 二进制在解析任何公开 CLI 命令前分发。认证材料只从
/// stdin 注入一次，避免出现在进程参数和环境变量中。
#[doc(hidden)]
pub fn internal_process_entry(args: &[String]) -> ExitCode {
    let mut root = None;
    let mut socket = None;
    let mut catalog = None;
    let mut credential_stdin = false;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = args.next().map(PathBuf::from),
            "--socket" => socket = args.next().map(PathBuf::from),
            "--catalog" => catalog = args.next().map(PathBuf::from),
            "--credential-stdin" => credential_stdin = true,
            "--help" | "-h" => {
                eprintln!(
                    "usage: cowd __cowd_internal auth-broker --root <dir> --socket <path> --catalog <path> --credential-stdin"
                );
                return ExitCode::SUCCESS;
            }
            _ => {
                eprintln!("unsupported auth broker argument: {arg}");
                return ExitCode::from(64);
            }
        }
    }
    let (Some(root), Some(socket), Some(catalog)) = (root, socket, catalog) else {
        eprintln!("--root, --socket and --catalog are required");
        return ExitCode::from(64);
    };
    if !credential_stdin {
        eprintln!("--credential-stdin is required");
        return ExitCode::from(64);
    }
    let mut credential = String::new();
    if io::stdin().lock().read_line(&mut credential).is_err() || credential.trim().is_empty() {
        eprintln!("a non-empty enrollment credential is required on stdin");
        return ExitCode::from(64);
    }
    let catalog = match read_catalog(&catalog) {
        Ok(catalog) => catalog,
        Err(error) => {
            eprintln!("auth broker catalogue failed: {error}");
            return ExitCode::from(64);
        }
    };
    match serve_local(root, credential.trim(), socket, catalog) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("auth broker failed: {error}");
            ExitCode::from(1)
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthBrokerError {
    #[error("authentication credential is invalid")]
    InvalidCredential,
    #[error("authentication credential has been revoked")]
    CredentialRevoked,
    #[error("credential recovery is only available after revocation")]
    CredentialRecoveryUnavailable,
    #[error("credential state is invalid: {0}")]
    InvalidCredentialState(String),
    #[error("authority storage error: {0}")]
    Storage(String),
    #[error("authority crypto error: {0}")]
    Crypto(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("broker protocol error: {0}")]
    Protocol(String),
    #[error("requested capability is not granted to a local human principal: {0}")]
    CapabilityDenied(String),
    #[error("decision lease is invalid: {0}")]
    InvalidDecisionLease(String),
    #[error("entitlement update conflicts with current state")]
    EntitlementConflict,
    #[error("entitlement confirmation digest is invalid")]
    InvalidEntitlementConfirmation,
    #[error("broker peer uid does not match the socket owner")]
    PeerUidMismatch,
}

/// Non-secret credential lifecycle information exposed by the local broker.
/// The credential digest is deliberately never returned over the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialLifecycleStatus {
    Active,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CredentialLifecycleMetadata {
    pub credential_epoch: u64,
    #[serde(default = "default_profile_revision")]
    pub profile_revision: u64,
    pub status: CredentialLifecycleStatus,
    pub enrolled_at_ms: u64,
    pub updated_at_ms: u64,
}

const fn default_profile_revision() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HumanAuthenticationResult {
    pub public_key_base64: String,
    pub envelope: SignedPrincipalEnvelope,
    pub entitlement: HumanEntitlementProjection,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerRequest {
    AuthenticateHuman {
        credential: String,
        #[serde(default, alias = "capabilities")]
        requested_capabilities: Vec<String>,
        #[serde(default)]
        surface_id: Option<String>,
        ttl_ms: Option<u64>,
    },
    IssueDecisionLease {
        credential: String,
        review_id: String,
        action: String,
        scope: String,
        evidence_digest: String,
        expires_at_ms: u64,
    },
    CredentialLifecycle,
    GetHumanEntitlements {
        credential: String,
    },
    PreviewHumanEntitlements {
        credential: String,
        core_profile_id: String,
        #[serde(default)]
        app_profiles: BTreeMap<String, String>,
    },
    SetHumanEntitlements {
        credential: String,
        expected_credential_epoch: u64,
        expected_profile_revision: u64,
        core_profile_id: String,
        #[serde(default)]
        app_profiles: BTreeMap<String, String>,
        confirmation_digest: String,
    },
    RotateCredential {
        credential: String,
        replacement_credential: String,
    },
    RecoverCredential {
        credential: String,
        replacement_credential: String,
    },
    RevokeCredential {
        credential: String,
    },
    TrustMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerResponse {
    Principal {
        public_key_base64: String,
        envelope: SignedPrincipalEnvelope,
        entitlement: HumanEntitlementProjection,
    },
    DecisionLease {
        public_key_base64: String,
        lease: SignedDecisionLease,
    },
    CredentialLifecycle {
        lifecycle: CredentialLifecycleMetadata,
    },
    HumanEntitlements {
        entitlement: HumanEntitlementProjection,
    },
    HumanEntitlementPreview {
        entitlement: HumanEntitlementProjection,
        confirmation_digest: String,
    },
    TrustMetadata {
        key_id: String,
        public_key_base64: String,
    },
    Error {
        code: String,
        message: String,
    },
}

/// Client for the local broker process.  It has no signer and cannot read the
/// authority root; a successful call returns only a signed wire envelope.
#[derive(Debug, Clone)]
pub struct BrokerClient {
    socket_path: PathBuf,
}

impl BrokerClient {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    #[must_use]
    pub fn default_socket(root: impl AsRef<Path>) -> PathBuf {
        root.as_ref().join(SOCKET_FILE)
    }

    pub fn authenticate_human(
        &self,
        credential: &str,
        capabilities: Vec<String>,
        ttl_ms: Option<u64>,
    ) -> Result<(SignedPrincipalEnvelope, String), AuthBrokerError> {
        let result = self.authenticate_human_for_surface(
            credential,
            "legacy_gateway",
            capabilities,
            ttl_ms,
        )?;
        Ok((result.envelope, result.public_key_base64))
    }

    pub fn authenticate_human_for_surface(
        &self,
        credential: &str,
        surface_id: impl Into<String>,
        requested_capabilities: Vec<String>,
        ttl_ms: Option<u64>,
    ) -> Result<HumanAuthenticationResult, AuthBrokerError> {
        let response = self.request(BrokerRequest::AuthenticateHuman {
            credential: credential.to_string(),
            requested_capabilities,
            surface_id: Some(surface_id.into()),
            ttl_ms,
        })?;
        match response {
            BrokerResponse::Principal {
                envelope,
                public_key_base64,
                entitlement,
            } => Ok(HumanAuthenticationResult {
                public_key_base64,
                envelope,
                entitlement,
            }),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    pub fn human_entitlements(
        &self,
        credential: &str,
    ) -> Result<HumanEntitlementProjection, AuthBrokerError> {
        match self.request(BrokerRequest::GetHumanEntitlements {
            credential: credential.to_string(),
        })? {
            BrokerResponse::HumanEntitlements { entitlement } => Ok(entitlement),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    pub fn set_human_entitlements(
        &self,
        credential: &str,
        expected_credential_epoch: u64,
        expected_profile_revision: u64,
        core_profile_id: impl Into<String>,
        app_profiles: BTreeMap<String, String>,
        confirmation_digest: String,
    ) -> Result<HumanEntitlementProjection, AuthBrokerError> {
        match self.request(BrokerRequest::SetHumanEntitlements {
            credential: credential.to_string(),
            expected_credential_epoch,
            expected_profile_revision,
            core_profile_id: core_profile_id.into(),
            app_profiles,
            confirmation_digest,
        })? {
            BrokerResponse::HumanEntitlements { entitlement } => Ok(entitlement),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    pub fn preview_human_entitlements(
        &self,
        credential: &str,
        core_profile_id: impl Into<String>,
        app_profiles: BTreeMap<String, String>,
    ) -> Result<(HumanEntitlementProjection, String), AuthBrokerError> {
        match self.request(BrokerRequest::PreviewHumanEntitlements {
            credential: credential.to_string(),
            core_profile_id: core_profile_id.into(),
            app_profiles,
        })? {
            BrokerResponse::HumanEntitlementPreview {
                entitlement,
                confirmation_digest,
            } => Ok((entitlement, confirmation_digest)),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    pub fn trust_metadata(&self) -> Result<(String, String), AuthBrokerError> {
        match self.request(BrokerRequest::TrustMetadata)? {
            BrokerResponse::TrustMetadata {
                key_id,
                public_key_base64,
            } => Ok((key_id, public_key_base64)),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    pub fn issue_decision_lease(
        &self,
        credential: &str,
        review_id: impl Into<String>,
        action: impl Into<String>,
        scope: impl Into<String>,
        evidence_digest: impl Into<String>,
        expires_at_ms: u64,
    ) -> Result<(SignedDecisionLease, String), AuthBrokerError> {
        let response = self.request(BrokerRequest::IssueDecisionLease {
            credential: credential.to_string(),
            review_id: review_id.into(),
            action: action.into(),
            scope: scope.into(),
            evidence_digest: evidence_digest.into(),
            expires_at_ms,
        })?;
        match response {
            BrokerResponse::DecisionLease {
                lease,
                public_key_base64,
            } => Ok((lease, public_key_base64)),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    pub fn credential_lifecycle(&self) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        match self.request(BrokerRequest::CredentialLifecycle)? {
            BrokerResponse::CredentialLifecycle { lifecycle } => Ok(lifecycle),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    /// Replaces an active local credential and advances its epoch.  The
    /// current credential authorizes this operation; replacement material is
    /// hashed before it reaches persistent storage.
    pub fn rotate_credential(
        &self,
        credential: &str,
        replacement_credential: &str,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        self.request_lifecycle_update(BrokerRequest::RotateCredential {
            credential: credential.to_string(),
            replacement_credential: replacement_credential.to_string(),
        })
    }

    /// Re-enrolls a revoked local credential as a new epoch.  This is an
    /// explicit local recovery path, not an automatic fallback.
    pub fn recover_credential(
        &self,
        credential: &str,
        replacement_credential: &str,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        self.request_lifecycle_update(BrokerRequest::RecoverCredential {
            credential: credential.to_string(),
            replacement_credential: replacement_credential.to_string(),
        })
    }

    /// Revokes the active local credential and advances its epoch.  No
    /// principal or decision lease can be issued until explicit recovery.
    pub fn revoke_credential(
        &self,
        credential: &str,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        self.request_lifecycle_update(BrokerRequest::RevokeCredential {
            credential: credential.to_string(),
        })
    }

    fn request_lifecycle_update(
        &self,
        request: BrokerRequest,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        match self.request(request)? {
            BrokerResponse::CredentialLifecycle { lifecycle } => Ok(lifecycle),
            BrokerResponse::Error { message, .. } => Err(AuthBrokerError::Protocol(message)),
            _ => Err(AuthBrokerError::Protocol(
                "broker returned an unexpected response".to_string(),
            )),
        }
    }

    #[cfg(unix)]
    fn request(&self, request: BrokerRequest) -> Result<BrokerResponse, AuthBrokerError> {
        use std::os::unix::net::UnixStream;

        let mut stream = UnixStream::connect(&self.socket_path).map_err(storage_error)?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .map_err(storage_error)?;
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(3)))
            .map_err(storage_error)?;
        let encoded = serde_json::to_string(&request)
            .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
        stream
            .write_all(encoded.as_bytes())
            .and_then(|_| stream.write_all(b"\n"))
            .and_then(|_| stream.flush())
            .map_err(storage_error)?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .map_err(storage_error)?;
        serde_json::from_str(response.trim())
            .map_err(|error| AuthBrokerError::Protocol(error.to_string()))
    }

    #[cfg(not(unix))]
    fn request(&self, _request: BrokerRequest) -> Result<BrokerResponse, AuthBrokerError> {
        Err(AuthBrokerError::Protocol(
            "local auth broker requires Unix domain sockets".to_string(),
        ))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedCredentialState {
    version: u32,
    credential_digest: String,
    credential_epoch: u64,
    status: CredentialLifecycleStatus,
    enrolled_at_ms: u64,
    updated_at_ms: u64,
    catalog_digest: String,
    core_profile_id: String,
    #[serde(default)]
    app_profiles: BTreeMap<String, String>,
    profile_revision: u64,
    entitled_capabilities: Vec<String>,
    entitlement_updated_at_ms: u64,
    entitlement_updated_by: String,
    #[serde(default)]
    last_audit_ref: Option<String>,
}

/// The v2 state existed before APP profiles became product-neutral key/value
/// selections. It is intentionally only a deserialization boundary: after a
/// credential-verified startup it is rewritten as v3 and never participates
/// in normal authorization decisions again.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedCredentialStateV2 {
    version: u32,
    credential_digest: String,
    credential_epoch: u64,
    status: CredentialLifecycleStatus,
    enrolled_at_ms: u64,
    updated_at_ms: u64,
    core_profile_id: String,
    mfg_profile_id: String,
    profile_revision: u64,
    entitled_capabilities: Vec<String>,
    entitlement_updated_at_ms: u64,
    entitlement_updated_by: String,
    #[serde(default, rename = "last_audit_ref")]
    _legacy_last_audit_ref: Option<String>,
}

impl PersistedCredentialState {
    fn enroll(credential: &str, catalog: &AuthorizationCatalog) -> Result<Self, AuthBrokerError> {
        validate_credential_input(credential)?;
        let now = now_ms();
        let (core_profile_id, app_profiles) = catalog.default_selection();
        let entitled_capabilities = catalog.capabilities_for(&core_profile_id, &app_profiles)?;
        Ok(Self {
            version: CREDENTIAL_STATE_VERSION,
            credential_digest: hex(&credential_digest(credential)),
            credential_epoch: 1,
            status: CredentialLifecycleStatus::Active,
            enrolled_at_ms: now,
            updated_at_ms: now,
            catalog_digest: catalog.digest()?,
            core_profile_id,
            app_profiles,
            profile_revision: 1,
            entitled_capabilities,
            entitlement_updated_at_ms: now,
            entitlement_updated_by: "initial_enrollment".to_string(),
            last_audit_ref: None,
        })
    }

    fn migrate_from_v2(
        state: PersistedCredentialStateV2,
        catalog: &AuthorizationCatalog,
    ) -> Result<Self, AuthBrokerError> {
        if state.version != 2
            || state.credential_epoch == 0
            || state.profile_revision == 0
            || state.enrolled_at_ms == 0
            || state.updated_at_ms < state.enrolled_at_ms
        {
            return Err(AuthBrokerError::InvalidCredentialState(
                "credential state v2 shape is invalid".to_string(),
            ));
        }

        // A v2 core profile was product-specific. Preserve the only current
        // manager value; every other historical value receives the required
        // least-privilege core profile rather than an implicit elevation.
        let core_profile_id = if state.core_profile_id == "core_manager"
            && catalog
                .core_profiles
                .iter()
                .any(|profile| profile.id == "core_manager")
        {
            "core_manager".to_string()
        } else {
            "core_operator".to_string()
        };

        let (_, mut app_profiles) = catalog.default_selection();
        // v2 could only select MFG. This one-time compatibility adapter may
        // retain that profile only when the compiled generic catalogue still
        // advertises it; all other APPs remain at their catalogue defaults.
        if let Some(mfg) = catalog.apps.iter().find(|app| app.app_id == "mfg") {
            if mfg
                .profiles
                .iter()
                .any(|profile| profile.id == state.mfg_profile_id)
            {
                app_profiles.insert("mfg".to_string(), state.mfg_profile_id.clone());
            }
        }
        let now = now_ms().max(state.updated_at_ms);
        let credential_epoch = state.credential_epoch.checked_add(1).ok_or_else(|| {
            AuthBrokerError::InvalidCredentialState(
                "credential epoch cannot be advanced during v2 migration".to_string(),
            )
        })?;
        let profile_revision = state.profile_revision.checked_add(1).ok_or_else(|| {
            AuthBrokerError::InvalidCredentialState(
                "profile revision cannot be advanced during v2 migration".to_string(),
            )
        })?;
        let entitled_capabilities = catalog.capabilities_for(&core_profile_id, &app_profiles)?;
        let last_audit_ref = Some(format!(
            "auth-broker://migration/v2-to-v3/{credential_epoch}/{profile_revision}"
        ));
        Ok(Self {
            version: CREDENTIAL_STATE_VERSION,
            credential_digest: state.credential_digest,
            credential_epoch,
            status: state.status,
            enrolled_at_ms: state.enrolled_at_ms,
            updated_at_ms: now,
            catalog_digest: catalog.digest()?,
            core_profile_id,
            app_profiles,
            profile_revision,
            entitled_capabilities,
            entitlement_updated_at_ms: now,
            entitlement_updated_by: "credential_state_v2_catalog_migration".to_string(),
            last_audit_ref,
        })
    }

    fn digest(&self) -> Result<[u8; digest::SHA256_OUTPUT_LEN], AuthBrokerError> {
        decode_digest(&self.credential_digest)
    }

    fn lifecycle(&self) -> CredentialLifecycleMetadata {
        CredentialLifecycleMetadata {
            credential_epoch: self.credential_epoch,
            profile_revision: self.profile_revision,
            status: self.status,
            enrolled_at_ms: self.enrolled_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }

    fn validate(&self, catalog: &AuthorizationCatalog) -> Result<(), AuthBrokerError> {
        if self.version != CREDENTIAL_STATE_VERSION {
            return Err(AuthBrokerError::InvalidCredentialState(format!(
                "unsupported credential state version {}",
                self.version
            )));
        }
        if self.credential_epoch == 0 {
            return Err(AuthBrokerError::InvalidCredentialState(
                "credential epoch must be greater than zero".to_string(),
            ));
        }
        if self.enrolled_at_ms == 0 || self.updated_at_ms < self.enrolled_at_ms {
            return Err(AuthBrokerError::InvalidCredentialState(
                "credential lifecycle timestamps are invalid".to_string(),
            ));
        }
        if self.profile_revision == 0
            || self.entitlement_updated_at_ms < self.enrolled_at_ms
            || self.entitlement_updated_by.trim().is_empty()
        {
            return Err(AuthBrokerError::InvalidCredentialState(
                "entitlement metadata is invalid".to_string(),
            ));
        }
        if self.catalog_digest != catalog.digest()? {
            return Err(AuthBrokerError::InvalidCredentialState(
                "credential state catalogue digest does not match the running product".to_string(),
            ));
        }
        let expected = catalog.capabilities_for(&self.core_profile_id, &self.app_profiles)?;
        if self.entitled_capabilities != expected {
            return Err(AuthBrokerError::InvalidCredentialState(
                "entitled capabilities do not match the selected profiles".to_string(),
            ));
        }
        self.digest().map(|_| ())
    }

    fn advance_epoch(&mut self) -> Result<(), AuthBrokerError> {
        self.credential_epoch = self.credential_epoch.checked_add(1).ok_or_else(|| {
            AuthBrokerError::InvalidCredentialState(
                "credential epoch cannot be advanced further".to_string(),
            )
        })?;
        self.updated_at_ms = now_ms().max(self.updated_at_ms);
        Ok(())
    }

    fn entitlement_projection(
        &self,
        requested: &[String],
        catalog: &AuthorizationCatalog,
    ) -> Result<HumanEntitlementProjection, AuthBrokerError> {
        // The caller has already constrained a principal issuance request to
        // its surface. This projection is also used by profile inspection,
        // which has no transport surface at all, so it validates the profile
        // ceiling rather than reapplying a legacy Gateway scope.
        let requested = validated_human_capabilities(catalog, requested.to_vec())?;
        let granted = requested
            .iter()
            .filter(|capability| self.entitled_capabilities.contains(*capability))
            .cloned()
            .collect::<Vec<_>>();
        let denied = requested
            .into_iter()
            .filter(|capability| !granted.contains(capability))
            .collect::<Vec<_>>();
        Ok(HumanEntitlementProjection {
            core_profile_id: self.core_profile_id.clone(),
            app_profiles: self.app_profiles.clone(),
            profile_revision: self.profile_revision,
            credential_epoch: self.credential_epoch,
            ceiling: self.entitled_capabilities.clone(),
            granted,
            denied,
        })
    }

    /// A new product build may add an APP or change its profile capabilities.
    /// Reconcile only with the compile-time catalogue, then invalidate every
    /// previously issued envelope by advancing both revisions atomically.
    fn reconcile_catalog(
        &mut self,
        catalog: &AuthorizationCatalog,
    ) -> Result<bool, AuthBrokerError> {
        let catalog_digest = catalog.digest()?;
        if self.catalog_digest == catalog_digest {
            return Ok(false);
        }
        if !catalog
            .core_profiles
            .iter()
            .any(|profile| profile.id == self.core_profile_id)
        {
            return Err(AuthBrokerError::InvalidCredentialState(
                "selected core profile is unavailable in the running product".to_string(),
            ));
        }
        self.app_profiles
            .retain(|app_id, _| catalog.apps.iter().any(|app| &app.app_id == app_id));
        for app in &catalog.apps {
            let profile_valid = self
                .app_profiles
                .get(&app.app_id)
                .is_some_and(|profile_id| {
                    app.profiles.iter().any(|profile| profile.id == *profile_id)
                });
            if !profile_valid {
                self.app_profiles
                    .insert(app.app_id.clone(), app.default_profile_id.clone());
            }
        }
        self.catalog_digest = catalog_digest;
        self.entitled_capabilities =
            catalog.capabilities_for(&self.core_profile_id, &self.app_profiles)?;
        self.profile_revision = self.profile_revision.checked_add(1).ok_or_else(|| {
            AuthBrokerError::InvalidCredentialState(
                "profile revision cannot be advanced further".to_string(),
            )
        })?;
        self.entitlement_updated_at_ms = now_ms().max(self.updated_at_ms);
        self.entitlement_updated_by = "compiled_app_catalogue_reconciliation".to_string();
        self.advance_epoch()?;
        Ok(true)
    }
}

struct LocalAuthority {
    key_pair: Ed25519KeyPair,
    credential_state: PersistedCredentialState,
    credential_state_path: PathBuf,
    entitlement_audit_path: PathBuf,
    catalog: AuthorizationCatalog,
}

impl LocalAuthority {
    pub fn open_or_initialize(
        root: impl AsRef<Path>,
        human_credential: &str,
        catalog: AuthorizationCatalog,
    ) -> Result<Self, AuthBrokerError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_error)?;
        validate_credential_input(human_credential)?;
        catalog.validate()?;
        let credential_state_path = root.join(CREDENTIAL_STATE_FILE);
        let entitlement_audit_path = root.join(ENTITLEMENT_AUDIT_FILE);
        if root.join("human-credential.sha256").exists() {
            return Err(AuthBrokerError::InvalidCredentialState(
                "legacy credential state is unsupported; remove it and enroll again".to_string(),
            ));
        }
        let (mut credential_state, migrated_from_v2) = if credential_state_path.exists() {
            read_credential_state(&credential_state_path, &catalog)?
        } else {
            // Enrollment is intentionally only possible while no lifecycle
            // state exists.  Subsequent broker starts recover this state and
            // must present the registered credential.
            let state = PersistedCredentialState::enroll(human_credential, &catalog)?;
            persist_credential_state(&credential_state_path, &state, &catalog)?;
            (state, false)
        };
        let expected = credential_state.digest()?;
        let supplied = credential_digest(human_credential);
        if !bool::from(expected.ct_eq(&supplied)) {
            return Err(AuthBrokerError::InvalidCredential);
        }
        if migrated_from_v2 || credential_state.reconcile_catalog(&catalog)? {
            persist_credential_state(&credential_state_path, &credential_state, &catalog)?;
        }
        let key_path = root.join(KEY_FILE);
        let key_material = if key_path.exists() {
            fs::read(&key_path).map_err(storage_error)?
        } else {
            let material = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .map_err(|_| AuthBrokerError::Crypto("key generation failed".to_string()))?;
            write_private_bytes(&key_path, material.as_ref())?;
            material.as_ref().to_vec()
        };
        let key_pair = Ed25519KeyPair::from_pkcs8(&key_material)
            .map_err(|_| AuthBrokerError::Crypto("stored key is invalid".to_string()))?;
        Ok(Self {
            key_pair,
            credential_state,
            credential_state_path,
            entitlement_audit_path,
            catalog,
        })
    }

    #[must_use]
    pub fn public_key_base64(&self) -> String {
        BASE64.encode(self.key_pair.public_key().as_ref())
    }

    pub fn issue_human_principal_for_surface(
        &self,
        human_credential: &str,
        surface_id: &str,
        requested_capabilities: Vec<String>,
        ttl_ms: Option<u64>,
    ) -> Result<(SignedPrincipalEnvelope, HumanEntitlementProjection), AuthBrokerError> {
        self.verify_active_credential(human_credential)?;
        let requested_capabilities = if requested_capabilities.is_empty() {
            self.catalog.surface_capabilities(surface_id)
        } else {
            validated_surface_capabilities(&self.catalog, surface_id, requested_capabilities)?
        };
        let entitlement = self
            .credential_state
            .entitlement_projection(&requested_capabilities, &self.catalog)?;
        let now = now_ms();
        let claims = PrincipalClaims {
            principal_id: "local-human".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities: entitlement.granted.clone(),
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: KEY_ID.to_string(),
            issued_at_ms: now,
            expires_at_ms: ttl_ms.map(|ttl| now.saturating_add(ttl)),
            credential_fingerprint: format!("sha256:{}", self.credential_state.credential_digest),
            credential_epoch: self.credential_state.credential_epoch,
            profile_revision: self.credential_state.profile_revision,
        };
        let signature_base64 = self.sign(&claims)?;
        Ok((
            SignedPrincipalEnvelope {
                key_id: KEY_ID.to_string(),
                claims,
                signature_base64,
            },
            entitlement,
        ))
    }

    pub fn issue_decision_lease(
        &self,
        human_credential: &str,
        review_id: impl Into<String>,
        action: impl Into<String>,
        scope: impl Into<String>,
        evidence_digest: impl Into<String>,
        expires_at_ms: u64,
    ) -> Result<SignedDecisionLease, AuthBrokerError> {
        self.verify_active_credential(human_credential)?;
        let review_id = review_id.into();
        let action = action.into();
        let scope = scope.into();
        let evidence_digest = evidence_digest.into();
        if review_id.trim().is_empty()
            || action.trim().is_empty()
            || scope.trim().is_empty()
            || evidence_digest.trim().is_empty()
        {
            return Err(AuthBrokerError::InvalidDecisionLease(
                "review_id, action, scope and evidence_digest are required".to_string(),
            ));
        }
        let issued_at_ms = now_ms();
        if expires_at_ms <= issued_at_ms {
            return Err(AuthBrokerError::InvalidDecisionLease(
                "expiry must be in the future".to_string(),
            ));
        }
        let claims = DecisionLeaseClaims {
            lease_id: uuid::Uuid::new_v4().to_string(),
            principal_id: "local-human".to_string(),
            review_id,
            action,
            scope,
            evidence_digest,
            issuer: KEY_ID.to_string(),
            issued_at_ms,
            expires_at_ms,
            credential_epoch: self.credential_state.credential_epoch,
        };
        let signature_base64 = self.sign(&claims)?;
        Ok(SignedDecisionLease {
            key_id: KEY_ID.to_string(),
            claims,
            signature_base64,
        })
    }

    fn verify_credential(&self, candidate: &str) -> Result<(), AuthBrokerError> {
        let candidate = credential_digest(candidate);
        if bool::from(self.credential_state.digest()?.ct_eq(&candidate)) {
            Ok(())
        } else {
            Err(AuthBrokerError::InvalidCredential)
        }
    }

    fn verify_active_credential(&self, candidate: &str) -> Result<(), AuthBrokerError> {
        self.verify_credential(candidate)?;
        if self.credential_state.status != CredentialLifecycleStatus::Active {
            return Err(AuthBrokerError::CredentialRevoked);
        }
        Ok(())
    }

    fn credential_lifecycle(&self) -> CredentialLifecycleMetadata {
        self.credential_state.lifecycle()
    }

    fn human_entitlements(
        &self,
        credential: &str,
    ) -> Result<HumanEntitlementProjection, AuthBrokerError> {
        self.verify_active_credential(credential)?;
        self.credential_state
            .entitlement_projection(&self.credential_state.entitled_capabilities, &self.catalog)
    }

    fn set_human_entitlements(
        &mut self,
        credential: &str,
        expected_credential_epoch: u64,
        expected_profile_revision: u64,
        core_profile_id: String,
        app_profiles: BTreeMap<String, String>,
        confirmation_digest: &str,
    ) -> Result<HumanEntitlementProjection, AuthBrokerError> {
        self.verify_active_credential(credential)?;
        if self.credential_state.credential_epoch != expected_credential_epoch
            || self.credential_state.profile_revision != expected_profile_revision
        {
            return Err(AuthBrokerError::EntitlementConflict);
        }
        let next_capabilities = self
            .catalog
            .capabilities_for(&core_profile_id, &app_profiles)?;
        let expected_confirmation = entitlement_confirmation_digest(
            expected_credential_epoch,
            expected_profile_revision,
            &core_profile_id,
            &app_profiles,
            &next_capabilities,
        );
        if !bool::from(
            expected_confirmation
                .as_bytes()
                .ct_eq(confirmation_digest.as_bytes()),
        ) {
            return Err(AuthBrokerError::InvalidEntitlementConfirmation);
        }

        let previous_core = self.credential_state.core_profile_id.clone();
        let previous_app_profiles = self.credential_state.app_profiles.clone();
        let previous_capabilities = self.credential_state.entitled_capabilities.clone();
        let updated_at_ms = now_ms().max(self.credential_state.updated_at_ms);
        let mut next_state = self.credential_state.clone();
        next_state.core_profile_id = core_profile_id.clone();
        next_state.app_profiles = app_profiles.clone();
        next_state.profile_revision =
            next_state.profile_revision.checked_add(1).ok_or_else(|| {
                AuthBrokerError::InvalidCredentialState(
                    "profile revision cannot be advanced further".to_string(),
                )
            })?;
        next_state.entitled_capabilities = next_capabilities;
        next_state.entitlement_updated_at_ms = updated_at_ms;
        next_state.entitlement_updated_by = "local_same_uid_cli".to_string();
        next_state.advance_epoch()?;
        let audit_ref = format!(
            "auth-broker://entitlement/{}/{}",
            next_state.credential_epoch, next_state.profile_revision
        );
        next_state.last_audit_ref = Some(audit_ref.clone());
        self.append_entitlement_audit(serde_json::json!({
            "audit_ref": audit_ref,
            "event": "entitlement_profile_update",
            "status": "authorized",
            "updated_at_ms": updated_at_ms,
            "previous_core_profile_id": previous_core,
            "previous_app_profiles": previous_app_profiles,
            "previous_capabilities": previous_capabilities,
            "core_profile_id": core_profile_id,
            "app_profiles": app_profiles,
            "entitled_capabilities": next_state.entitled_capabilities,
            "credential_epoch": next_state.credential_epoch,
            "profile_revision": next_state.profile_revision,
            "updated_by": next_state.entitlement_updated_by,
        }))?;
        persist_credential_state(&self.credential_state_path, &next_state, &self.catalog)?;
        self.credential_state = next_state;
        self.human_entitlements(credential)
    }

    fn preview_human_entitlements(
        &self,
        credential: &str,
        core_profile_id: String,
        app_profiles: BTreeMap<String, String>,
    ) -> Result<(HumanEntitlementProjection, String), AuthBrokerError> {
        self.verify_active_credential(credential)?;
        let ceiling = self
            .catalog
            .capabilities_for(&core_profile_id, &app_profiles)?;
        let projection = HumanEntitlementProjection {
            core_profile_id: core_profile_id.clone(),
            app_profiles: app_profiles.clone(),
            profile_revision: self.credential_state.profile_revision,
            credential_epoch: self.credential_state.credential_epoch,
            ceiling: ceiling.clone(),
            granted: ceiling,
            denied: Vec::new(),
        };
        let confirmation_digest = entitlement_confirmation_digest(
            projection.credential_epoch,
            projection.profile_revision,
            &core_profile_id,
            &app_profiles,
            &projection.ceiling,
        );
        Ok((projection, confirmation_digest))
    }

    fn rotate_credential(
        &mut self,
        current_credential: &str,
        replacement_credential: &str,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        self.verify_active_credential(current_credential)?;
        validate_credential_input(replacement_credential)?;
        self.credential_state.credential_digest = hex(&credential_digest(replacement_credential));
        self.credential_state.advance_epoch()?;
        self.persist_credential_state()?;
        Ok(self.credential_lifecycle())
    }

    fn revoke_credential(
        &mut self,
        current_credential: &str,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        self.verify_active_credential(current_credential)?;
        self.credential_state.status = CredentialLifecycleStatus::Revoked;
        self.credential_state.advance_epoch()?;
        self.persist_credential_state()?;
        Ok(self.credential_lifecycle())
    }

    fn recover_credential(
        &mut self,
        recovery_credential: &str,
        replacement_credential: &str,
    ) -> Result<CredentialLifecycleMetadata, AuthBrokerError> {
        self.verify_credential(recovery_credential)?;
        if self.credential_state.status != CredentialLifecycleStatus::Revoked {
            return Err(AuthBrokerError::CredentialRecoveryUnavailable);
        }
        validate_credential_input(replacement_credential)?;
        self.credential_state.credential_digest = hex(&credential_digest(replacement_credential));
        self.credential_state.status = CredentialLifecycleStatus::Active;
        self.credential_state.advance_epoch()?;
        self.persist_credential_state()?;
        Ok(self.credential_lifecycle())
    }

    fn persist_credential_state(&self) -> Result<(), AuthBrokerError> {
        persist_credential_state(
            &self.credential_state_path,
            &self.credential_state,
            &self.catalog,
        )
    }

    fn append_entitlement_audit(&self, event: serde_json::Value) -> Result<(), AuthBrokerError> {
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&self.entitlement_audit_path)
            .map_err(storage_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                &self.entitlement_audit_path,
                fs::Permissions::from_mode(0o600),
            )
            .map_err(storage_error)?;
        }
        let mut encoded = serde_json::to_vec(&event)
            .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
        encoded.push(b'\n');
        file.write_all(&encoded).map_err(storage_error)?;
        file.sync_all().map_err(storage_error)?;
        if let Some(parent) = self.entitlement_audit_path.parent() {
            fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(storage_error)?;
        }
        Ok(())
    }

    fn sign<T: serde::Serialize>(&self, value: &T) -> Result<String, AuthBrokerError> {
        let encoded = serde_json::to_vec(value)
            .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
        Ok(BASE64.encode(self.key_pair.sign(&encoded).as_ref()))
    }
}

/// Test-only signing helpers. Production consumers use the broker protocol;
/// this module exists so dependent-crate tests do not force the installation
/// signer back into the Gateway production API surface.
#[cfg(feature = "test-support")]
pub mod test_support {
    use std::{collections::BTreeMap, path::Path};

    use harness_contract::security::{SignedDecisionLease, SignedPrincipalEnvelope};

    use super::{
        AuthBrokerError, AuthorizationAppProfileCatalog, AuthorizationCatalog,
        AuthorizationProfile, LocalAuthority,
    };

    /// Test-only permissive catalogue. It is not linked into production: the
    /// real broker receives its catalogue from the product APP registry.
    #[must_use]
    pub fn catalog_for_capabilities(capabilities: Vec<String>) -> AuthorizationCatalog {
        let mut capabilities = capabilities;
        capabilities.sort();
        capabilities.dedup();
        AuthorizationCatalog {
            schema_version: 1,
            core_profiles: vec![
                AuthorizationProfile {
                    id: "core_operator".to_string(),
                    capabilities: vec!["approval.respond".to_string()],
                },
                AuthorizationProfile {
                    id: "core_manager".to_string(),
                    capabilities: capabilities.clone(),
                },
            ],
            apps: vec![AuthorizationAppProfileCatalog {
                app_id: "fixture".to_string(),
                default_profile_id: "fixture_manager".to_string(),
                profiles: vec![AuthorizationProfile {
                    id: "fixture_manager".to_string(),
                    capabilities: capabilities.clone(),
                }],
                surface_capabilities: BTreeMap::from([
                    ("backend".to_string(), capabilities.clone()),
                    ("webui".to_string(), capabilities.clone()),
                    ("tui".to_string(), capabilities.clone()),
                    ("cli".to_string(), capabilities.clone()),
                ]),
            }],
        }
    }

    pub fn issue_human_principal(
        root: impl AsRef<Path>,
        credential: &str,
        capabilities: Vec<String>,
        ttl_ms: Option<u64>,
    ) -> Result<(SignedPrincipalEnvelope, String), AuthBrokerError> {
        let catalog = catalog_for_capabilities(capabilities.clone());
        let mut authority = LocalAuthority::open_or_initialize(root, credential, catalog)?;
        authority.credential_state.core_profile_id = "core_manager".to_string();
        authority.credential_state.entitled_capabilities = capabilities;
        let (envelope, _) = authority.issue_human_principal_for_surface(
            credential,
            "legacy_gateway",
            authority.credential_state.entitled_capabilities.clone(),
            ttl_ms,
        )?;
        Ok((envelope, authority.public_key_base64()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn issue_decision_lease(
        root: impl AsRef<Path>,
        credential: &str,
        review_id: impl Into<String>,
        action: impl Into<String>,
        scope: impl Into<String>,
        evidence_digest: impl Into<String>,
        expires_at_ms: u64,
    ) -> Result<(SignedDecisionLease, String), AuthBrokerError> {
        let authority = LocalAuthority::open_or_initialize(
            root,
            credential,
            catalog_for_capabilities(Vec::new()),
        )?;
        let lease = authority.issue_decision_lease(
            credential,
            review_id,
            action,
            scope,
            evidence_digest,
            expires_at_ms,
        )?;
        Ok((lease, authority.public_key_base64()))
    }
}

/// Serve the broker on a protected Unix socket until the process is stopped.
/// The authority is opened exactly once in this process; clients never obtain
/// the signing key or a file-system handle to its root.
#[cfg(unix)]
pub fn serve_local(
    root: impl AsRef<Path>,
    human_credential: &str,
    socket_path: impl AsRef<Path>,
    catalog: AuthorizationCatalog,
) -> Result<(), AuthBrokerError> {
    use std::os::unix::{fs::PermissionsExt, net::UnixListener};

    let mut authority = LocalAuthority::open_or_initialize(root, human_credential, catalog)?;
    let socket_path = socket_path.as_ref();
    if socket_path.exists() {
        fs::remove_file(socket_path).map_err(storage_error)?;
    }
    let listener = UnixListener::bind(socket_path).map_err(storage_error)?;
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600)).map_err(storage_error)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(error) = handle_client(&mut authority, stream) {
                    eprintln!("auth broker request failed: {error}");
                }
            }
            Err(error) => return Err(storage_error(error)),
        }
    }
    Ok(())
}

/// Serve the broker until the supplied shutdown predicate becomes true.
///
/// Production uses [`serve_local`]; the explicit lifecycle hook lets embedded
/// test fixtures release their listener, socket, and authority deterministically.
#[cfg(unix)]
pub fn serve_local_until(
    root: impl AsRef<Path>,
    human_credential: &str,
    socket_path: impl AsRef<Path>,
    catalog: AuthorizationCatalog,
    should_shutdown: impl Fn() -> bool,
) -> Result<(), AuthBrokerError> {
    use std::os::unix::{fs::PermissionsExt, net::UnixListener};

    let mut authority = LocalAuthority::open_or_initialize(root, human_credential, catalog)?;
    let socket_path = socket_path.as_ref();
    if socket_path.exists() {
        fs::remove_file(socket_path).map_err(storage_error)?;
    }
    let listener = UnixListener::bind(socket_path).map_err(storage_error)?;
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600)).map_err(storage_error)?;
    listener.set_nonblocking(true).map_err(storage_error)?;
    while !should_shutdown() {
        match listener.accept() {
            Ok((stream, _address)) => {
                if let Err(error) = handle_client(&mut authority, stream) {
                    eprintln!("auth broker request failed: {error}");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            Err(error) => return Err(storage_error(error)),
        }
    }
    let _ = fs::remove_file(socket_path);
    Ok(())
}

#[cfg(not(unix))]
pub fn serve_local(
    _root: impl AsRef<Path>,
    _human_credential: &str,
    _socket_path: impl AsRef<Path>,
    _catalog: AuthorizationCatalog,
) -> Result<(), AuthBrokerError> {
    Err(AuthBrokerError::Protocol(
        "local auth broker requires Unix domain sockets".to_string(),
    ))
}

#[cfg(not(unix))]
pub fn serve_local_until(
    _root: impl AsRef<Path>,
    _human_credential: &str,
    _socket_path: impl AsRef<Path>,
    _catalog: AuthorizationCatalog,
    _should_shutdown: impl Fn() -> bool,
) -> Result<(), AuthBrokerError> {
    Err(AuthBrokerError::Protocol(
        "local auth broker requires Unix domain sockets".to_string(),
    ))
}

#[cfg(unix)]
fn handle_client(
    authority: &mut LocalAuthority,
    stream: std::os::unix::net::UnixStream,
) -> Result<(), AuthBrokerError> {
    let peer = rustix::net::sockopt::socket_peercred(&stream)
        .map_err(|error| AuthBrokerError::Storage(error.to_string()))?;
    if peer.uid != rustix::process::getuid() {
        return Err(AuthBrokerError::PeerUidMismatch);
    }
    let mut reader = BufReader::new(stream.try_clone().map_err(storage_error)?);
    let mut line = String::new();
    reader.read_line(&mut line).map_err(storage_error)?;
    let response = match serde_json::from_str::<BrokerRequest>(line.trim()) {
        Ok(BrokerRequest::AuthenticateHuman {
            credential,
            requested_capabilities,
            surface_id,
            ttl_ms,
        }) => match authority.issue_human_principal_for_surface(
            &credential,
            surface_id.as_deref().unwrap_or("legacy_gateway"),
            requested_capabilities,
            ttl_ms,
        ) {
            Ok((envelope, entitlement)) => BrokerResponse::Principal {
                public_key_base64: authority.public_key_base64(),
                envelope,
                entitlement,
            },
            Err(error) => BrokerResponse::Error {
                code: "authentication_denied".to_string(),
                message: error.to_string(),
            },
        },
        Ok(BrokerRequest::IssueDecisionLease {
            credential,
            review_id,
            action,
            scope,
            evidence_digest,
            expires_at_ms,
        }) => match authority.issue_decision_lease(
            &credential,
            review_id,
            action,
            scope,
            evidence_digest,
            expires_at_ms,
        ) {
            Ok(lease) => BrokerResponse::DecisionLease {
                public_key_base64: authority.public_key_base64(),
                lease,
            },
            Err(error) => BrokerResponse::Error {
                code: "decision_lease_denied".to_string(),
                message: error.to_string(),
            },
        },
        Ok(BrokerRequest::CredentialLifecycle) => BrokerResponse::CredentialLifecycle {
            lifecycle: authority.credential_lifecycle(),
        },
        Ok(BrokerRequest::GetHumanEntitlements { credential }) => {
            match authority.human_entitlements(&credential) {
                Ok(entitlement) => BrokerResponse::HumanEntitlements { entitlement },
                Err(error) => BrokerResponse::Error {
                    code: "entitlement_read_denied".to_string(),
                    message: error.to_string(),
                },
            }
        }
        Ok(BrokerRequest::PreviewHumanEntitlements {
            credential,
            core_profile_id,
            app_profiles,
        }) => {
            match authority.preview_human_entitlements(&credential, core_profile_id, app_profiles) {
                Ok((entitlement, confirmation_digest)) => BrokerResponse::HumanEntitlementPreview {
                    entitlement,
                    confirmation_digest,
                },
                Err(error) => BrokerResponse::Error {
                    code: "entitlement_preview_denied".to_string(),
                    message: error.to_string(),
                },
            }
        }
        Ok(BrokerRequest::SetHumanEntitlements {
            credential,
            expected_credential_epoch,
            expected_profile_revision,
            core_profile_id,
            app_profiles,
            confirmation_digest,
        }) => match authority.set_human_entitlements(
            &credential,
            expected_credential_epoch,
            expected_profile_revision,
            core_profile_id,
            app_profiles,
            &confirmation_digest,
        ) {
            Ok(entitlement) => BrokerResponse::HumanEntitlements { entitlement },
            Err(error) => BrokerResponse::Error {
                code: "entitlement_update_denied".to_string(),
                message: error.to_string(),
            },
        },
        Ok(BrokerRequest::RotateCredential {
            credential,
            replacement_credential,
        }) => match authority.rotate_credential(&credential, &replacement_credential) {
            Ok(lifecycle) => BrokerResponse::CredentialLifecycle { lifecycle },
            Err(error) => BrokerResponse::Error {
                code: "credential_rotation_denied".to_string(),
                message: error.to_string(),
            },
        },
        Ok(BrokerRequest::RecoverCredential {
            credential,
            replacement_credential,
        }) => match authority.recover_credential(&credential, &replacement_credential) {
            Ok(lifecycle) => BrokerResponse::CredentialLifecycle { lifecycle },
            Err(error) => BrokerResponse::Error {
                code: "credential_recovery_denied".to_string(),
                message: error.to_string(),
            },
        },
        Ok(BrokerRequest::RevokeCredential { credential }) => {
            match authority.revoke_credential(&credential) {
                Ok(lifecycle) => BrokerResponse::CredentialLifecycle { lifecycle },
                Err(error) => BrokerResponse::Error {
                    code: "credential_revocation_denied".to_string(),
                    message: error.to_string(),
                },
            }
        }
        Ok(BrokerRequest::TrustMetadata) => BrokerResponse::TrustMetadata {
            key_id: KEY_ID.to_string(),
            public_key_base64: authority.public_key_base64(),
        },
        Err(error) => BrokerResponse::Error {
            code: "invalid_request".to_string(),
            message: error.to_string(),
        },
    };
    let encoded = serde_json::to_string(&response)
        .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
    let mut writer = stream;
    writer
        .write_all(encoded.as_bytes())
        .and_then(|_| writer.write_all(b"\n"))
        .and_then(|_| writer.flush())
        .map_err(storage_error)
}

fn validated_human_capabilities(
    catalog: &AuthorizationCatalog,
    capabilities: Vec<String>,
) -> Result<Vec<String>, AuthBrokerError> {
    let known = catalog
        .core_profiles
        .iter()
        .flat_map(|profile| profile.capabilities.iter())
        .chain(
            catalog
                .apps
                .iter()
                .flat_map(|app| app.profiles.iter())
                .flat_map(|profile| profile.capabilities.iter()),
        )
        .collect::<BTreeSet<_>>();
    let mut approved = Vec::new();
    for capability in capabilities {
        if !known.contains(&capability) {
            return Err(AuthBrokerError::CapabilityDenied(capability));
        }
        if !approved.contains(&capability) {
            approved.push(capability);
        }
    }
    Ok(approved)
}

fn validated_surface_capabilities(
    catalog: &AuthorizationCatalog,
    surface_id: &str,
    capabilities: Vec<String>,
) -> Result<Vec<String>, AuthBrokerError> {
    let capabilities = validated_human_capabilities(catalog, capabilities)?;
    let allowed = catalog.surface_capabilities(surface_id);
    for capability in &capabilities {
        if !allowed.contains(capability) {
            return Err(AuthBrokerError::CapabilityDenied(format!(
                "{capability} is not exposed by surface {surface_id}"
            )));
        }
    }
    Ok(capabilities)
}

fn credential_digest(value: &str) -> [u8; digest::SHA256_OUTPUT_LEN] {
    let digest = digest::digest(&digest::SHA256, value.as_bytes());
    let mut output = [0_u8; digest::SHA256_OUTPUT_LEN];
    output.copy_from_slice(digest.as_ref());
    output
}

fn validate_credential_input(credential: &str) -> Result<(), AuthBrokerError> {
    if credential.trim().is_empty() {
        return Err(AuthBrokerError::InvalidCredential);
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn write_private_bytes(path: &Path, content: &[u8]) -> Result<(), AuthBrokerError> {
    use std::io::Write;
    let temporary = path.with_extension("tmp");
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(storage_error)?;
    file.write_all(content).map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .map_err(storage_error)?;
    }
    fs::rename(temporary, path).map_err(storage_error)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(storage_error)?;
    }
    Ok(())
}

fn read_credential_state(
    path: &Path,
    catalog: &AuthorizationCatalog,
) -> Result<(PersistedCredentialState, bool), AuthBrokerError> {
    let bytes = fs::read(path).map_err(storage_error)?;
    let document = serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|error| AuthBrokerError::InvalidCredentialState(error.to_string()))?;
    let version = document
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            AuthBrokerError::InvalidCredentialState(
                "credential state version is missing".to_string(),
            )
        })?;
    let (state, migrated_from_v2) = match version {
        3 => (
            serde_json::from_value::<PersistedCredentialState>(document)
                .map_err(|error| AuthBrokerError::InvalidCredentialState(error.to_string()))?,
            false,
        ),
        2 => (
            PersistedCredentialState::migrate_from_v2(
                serde_json::from_value::<PersistedCredentialStateV2>(document)
                    .map_err(|error| AuthBrokerError::InvalidCredentialState(error.to_string()))?,
                catalog,
            )?,
            true,
        ),
        other => {
            return Err(AuthBrokerError::InvalidCredentialState(format!(
                "unsupported credential state version {other}"
            )));
        }
    };
    // Catalogue reconciliation runs after the enrollment credential has been
    // verified. The version and cryptographic shape must still be valid here.
    if state.version != CREDENTIAL_STATE_VERSION || state.credential_epoch == 0 {
        return Err(AuthBrokerError::InvalidCredentialState(
            "credential state shape is invalid".to_string(),
        ));
    }
    if !migrated_from_v2 && state.catalog_digest == catalog.digest()? {
        state.validate(catalog)?;
    }
    Ok((state, migrated_from_v2))
}

fn persist_credential_state(
    path: &Path,
    state: &PersistedCredentialState,
    catalog: &AuthorizationCatalog,
) -> Result<(), AuthBrokerError> {
    state.validate(catalog)?;
    let encoded = serde_json::to_vec_pretty(state)
        .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
    write_private_bytes(path, &encoded)
}

/// Persist the non-secret catalogue next to the broker authority. The file is
/// passed to the isolated child by path so neither the credential nor a
/// product-specific type needs to cross its process boundary.
pub fn write_catalog(path: &Path, catalog: &AuthorizationCatalog) -> Result<(), AuthBrokerError> {
    catalog.validate()?;
    let encoded = serde_json::to_vec_pretty(catalog)
        .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
    write_private_bytes(path, &encoded)
}

fn read_catalog(path: &Path) -> Result<AuthorizationCatalog, AuthBrokerError> {
    let bytes = fs::read(path).map_err(storage_error)?;
    let catalog = serde_json::from_slice::<AuthorizationCatalog>(&bytes)
        .map_err(|error| AuthBrokerError::InvalidCredentialState(error.to_string()))?;
    catalog.validate()?;
    Ok(catalog)
}

#[must_use]
pub fn catalog_file(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(CATALOG_FILE)
}

fn decode_digest(value: &str) -> Result<[u8; digest::SHA256_OUTPUT_LEN], AuthBrokerError> {
    let value = value.trim();
    if value.len() != digest::SHA256_OUTPUT_LEN * 2 {
        return Err(AuthBrokerError::Storage(
            "credential digest length is invalid".to_string(),
        ));
    }
    let mut output = [0_u8; digest::SHA256_OUTPUT_LEN];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| AuthBrokerError::Storage("credential digest is invalid".to_string()))?;
    }
    Ok(output)
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[must_use]
pub fn entitlement_confirmation_digest(
    credential_epoch: u64,
    profile_revision: u64,
    core_profile_id: &str,
    app_profiles: &BTreeMap<String, String>,
    capabilities: &[String],
) -> String {
    let document = serde_json::json!({
        "credential_epoch": credential_epoch,
        "profile_revision": profile_revision,
        "core_profile_id": core_profile_id,
        "app_profiles": app_profiles,
        "capabilities": capabilities,
    });
    let encoded = serde_json::to_vec(&document).unwrap_or_default();
    let digest = digest::digest(&digest::SHA256, &encoded);
    format!("sha256:{}", hex(digest.as_ref()))
}

fn storage_error(error: std::io::Error) -> AuthBrokerError {
    AuthBrokerError::Storage(error.to_string())
}

#[cfg(test)]
mod generic_catalog_tests {
    use super::*;

    fn fixture_catalog() -> AuthorizationCatalog {
        AuthorizationCatalog {
            schema_version: 1,
            core_profiles: vec![
                AuthorizationProfile {
                    id: "core_operator".to_string(),
                    capabilities: vec!["approval.respond".to_string()],
                },
                AuthorizationProfile {
                    id: "core_manager".to_string(),
                    capabilities: vec![
                        "approval.respond".to_string(),
                        "definition.manage".to_string(),
                    ],
                },
            ],
            apps: vec![AuthorizationAppProfileCatalog {
                app_id: "workbench".to_string(),
                default_profile_id: "viewer".to_string(),
                profiles: vec![
                    AuthorizationProfile {
                        id: "viewer".to_string(),
                        capabilities: vec!["workbench.read".to_string()],
                    },
                    AuthorizationProfile {
                        id: "manager".to_string(),
                        capabilities: vec![
                            "workbench.read".to_string(),
                            "workbench.manage".to_string(),
                            "workbench.webui.manage".to_string(),
                        ],
                    },
                ],
                surface_capabilities: BTreeMap::from([
                    (
                        "backend".to_string(),
                        vec!["workbench.read".to_string(), "workbench.manage".to_string()],
                    ),
                    ("tui".to_string(), vec!["workbench.read".to_string()]),
                    (
                        "webui".to_string(),
                        vec![
                            "workbench.read".to_string(),
                            "workbench.webui.manage".to_string(),
                        ],
                    ),
                ]),
            }],
        }
    }

    fn migration_catalog() -> AuthorizationCatalog {
        AuthorizationCatalog {
            schema_version: 1,
            core_profiles: vec![
                AuthorizationProfile {
                    id: "core_operator".to_string(),
                    capabilities: vec!["approval.respond".to_string()],
                },
                AuthorizationProfile {
                    id: "core_manager".to_string(),
                    capabilities: vec![
                        "approval.respond".to_string(),
                        "definition.manage".to_string(),
                    ],
                },
            ],
            apps: vec![AuthorizationAppProfileCatalog {
                app_id: "mfg".to_string(),
                default_profile_id: "mfg_viewer".to_string(),
                profiles: vec![
                    AuthorizationProfile {
                        id: "mfg_viewer".to_string(),
                        capabilities: vec!["mfg.read".to_string()],
                    },
                    AuthorizationProfile {
                        id: "mfg_manager".to_string(),
                        capabilities: vec!["mfg.read".to_string(), "mfg.manage".to_string()],
                    },
                ],
                surface_capabilities: BTreeMap::new(),
            }],
        }
    }

    fn v2_state(
        credential: &str,
        core_profile_id: &str,
        mfg_profile_id: &str,
    ) -> PersistedCredentialStateV2 {
        PersistedCredentialStateV2 {
            version: 2,
            credential_digest: hex(&credential_digest(credential)),
            credential_epoch: 7,
            status: CredentialLifecycleStatus::Active,
            enrolled_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_001_000,
            core_profile_id: core_profile_id.to_string(),
            mfg_profile_id: mfg_profile_id.to_string(),
            profile_revision: 4,
            entitled_capabilities: vec!["historical.capability".to_string()],
            entitlement_updated_at_ms: 1_700_000_001_000,
            entitlement_updated_by: "historical_state".to_string(),
            _legacy_last_audit_ref: Some("legacy-audit".to_string()),
        }
    }

    #[test]
    fn generic_catalog_rejects_surface_capability_outside_its_declared_surface() {
        let catalog = fixture_catalog();
        catalog.validate().expect("catalogue is valid");
        assert!(validated_surface_capabilities(
            &catalog,
            "tui",
            vec!["workbench.manage".to_string()],
        )
        .is_err());
        assert!(validated_surface_capabilities(
            &catalog,
            "backend",
            vec!["workbench.manage".to_string()],
        )
        .is_ok());
        assert!(validated_surface_capabilities(
            &catalog,
            "backend",
            vec!["workbench.webui.manage".to_string()],
        )
        .is_err());
        assert!(validated_surface_capabilities(
            &catalog,
            "legacy_gateway",
            vec!["workbench.webui.manage".to_string()],
        )
        .is_ok());
    }

    #[test]
    fn profile_preview_and_update_are_catalog_bound_and_epoch_invalidating() {
        let root = std::env::temp_dir().join(format!("cowd-auth-generic-{}", uuid::Uuid::new_v4()));
        let catalog = fixture_catalog();
        let mut authority =
            LocalAuthority::open_or_initialize(&root, "credential", catalog).expect("authority");
        let initial = authority.human_entitlements("credential").expect("initial");
        assert_eq!(initial.core_profile_id, "core_operator");
        assert_eq!(initial.app_profiles["workbench"], "viewer");
        let target_profiles = BTreeMap::from([("workbench".to_string(), "manager".to_string())]);
        let (preview, confirmation) = authority
            .preview_human_entitlements(
                "credential",
                "core_manager".to_string(),
                target_profiles.clone(),
            )
            .expect("preview");
        assert!(preview.ceiling.contains(&"workbench.manage".to_string()));
        let updated = authority
            .set_human_entitlements(
                "credential",
                initial.credential_epoch,
                initial.profile_revision,
                "core_manager".to_string(),
                target_profiles.clone(),
                &confirmation,
            )
            .expect("update");
        assert!(updated.credential_epoch > initial.credential_epoch);
        assert_eq!(updated.app_profiles["workbench"], "manager");
        assert!(matches!(
            authority.set_human_entitlements(
                "credential",
                initial.credential_epoch,
                initial.profile_revision,
                "core_manager".to_string(),
                target_profiles,
                &confirmation,
            ),
            Err(AuthBrokerError::EntitlementConflict)
        ));
        assert!(root.join(ENTITLEMENT_AUDIT_FILE).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn v2_state_migrates_once_with_catalog_bound_least_privilege() {
        let root = std::env::temp_dir().join(format!("cowd-auth-generic-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("root");
        let source = v2_state("credential", "core_legacy_0_9_530", "mfg_manager");
        let encoded = serde_json::to_vec_pretty(&source).expect("v2 encoding");
        write_private_bytes(&root.join(CREDENTIAL_STATE_FILE), &encoded).expect("state");

        let catalog = migration_catalog();
        let authority = LocalAuthority::open_or_initialize(&root, "credential", catalog.clone())
            .expect("v2 migration succeeds");
        let projection = authority
            .human_entitlements("credential")
            .expect("projection");
        assert_eq!(projection.core_profile_id, "core_operator");
        assert_eq!(projection.app_profiles["mfg"], "mfg_manager");
        assert_eq!(projection.credential_epoch, 8);
        assert_eq!(projection.profile_revision, 5);
        assert_eq!(
            projection.ceiling,
            catalog
                .capabilities_for("core_operator", &projection.app_profiles)
                .expect("current capabilities")
        );

        let saved = serde_json::from_slice::<serde_json::Value>(
            &fs::read(root.join(CREDENTIAL_STATE_FILE)).expect("saved state"),
        )
        .expect("saved json");
        assert_eq!(saved["version"], 3);
        assert_eq!(saved["catalog_digest"], catalog.digest().expect("digest"));
        assert!(saved.get("mfg_profile_id").is_none());
        assert!(saved["last_audit_ref"]
            .as_str()
            .is_some_and(|reference| reference.starts_with("auth-broker://migration/v2-to-v3/")));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn v2_unknown_app_profile_uses_current_default_without_elevation() {
        let migrated = PersistedCredentialState::migrate_from_v2(
            v2_state("credential", "core_manager", "mfg_legacy_0_9_529"),
            &migration_catalog(),
        )
        .expect("migration");
        assert_eq!(migrated.core_profile_id, "core_manager");
        assert_eq!(migrated.app_profiles["mfg"], "mfg_viewer");
        assert!(!migrated
            .entitled_capabilities
            .contains(&"historical.capability".to_string()));
    }

    #[test]
    fn v2_state_is_not_rewritten_before_credential_verification() {
        let root = std::env::temp_dir().join(format!("cowd-auth-generic-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("root");
        let encoded = serde_json::to_vec_pretty(&v2_state(
            "correct credential",
            "core_legacy_0_9_530",
            "mfg_viewer",
        ))
        .expect("v2 encoding");
        write_private_bytes(&root.join(CREDENTIAL_STATE_FILE), &encoded).expect("state");

        assert!(matches!(
            LocalAuthority::open_or_initialize(&root, "wrong credential", migration_catalog()),
            Err(AuthBrokerError::InvalidCredential)
        ));
        let saved = serde_json::from_slice::<serde_json::Value>(
            &fs::read(root.join(CREDENTIAL_STATE_FILE)).expect("unchanged state"),
        )
        .expect("saved json");
        assert_eq!(saved["version"], 2);
        let _ = fs::remove_dir_all(root);
    }
}
