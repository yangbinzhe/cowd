//! Local authority for signed human identity envelopes and decision leases.
//!
//! This crate owns signing material.  Consumers receive only serialized
//! envelopes and public verification material; no caller can select the
//! principal kind, capabilities, or assurance encoded in a signed result.

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use harness_contract::security::{
    DecisionLeaseClaims, PrincipalAssurance, PrincipalClaims, PrincipalKind, SignedDecisionLease,
    SignedPrincipalEnvelope,
};
use ring::{
    digest,
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use subtle::ConstantTimeEq;
use thiserror::Error;

const KEY_FILE: &str = "authority.pk8";
const LEGACY_CREDENTIAL_FILE: &str = "human-credential.sha256";
const CREDENTIAL_STATE_FILE: &str = "credential-state.json";
const CREDENTIAL_STATE_VERSION: u32 = 1;
const KEY_ID: &str = "cowd-local-ed25519-v1";
const SOCKET_FILE: &str = "broker.sock";

const HUMAN_CAPABILITIES: &[&str] = &[
    "approval.respond",
    "definition.manage",
    "definition.default.set",
    "definition.rollback",
    "evolution.release.manage",
    "runtime.maintenance.manage",
    "runtime.outbox.retry",
];

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
    pub status: CredentialLifecycleStatus,
    pub enrolled_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerRequest {
    AuthenticateHuman {
        credential: String,
        capabilities: Vec<String>,
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
    },
    DecisionLease {
        public_key_base64: String,
        lease: SignedDecisionLease,
    },
    CredentialLifecycle {
        lifecycle: CredentialLifecycleMetadata,
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
        let response = self.request(BrokerRequest::AuthenticateHuman {
            credential: credential.to_string(),
            capabilities,
            ttl_ms,
        })?;
        match response {
            BrokerResponse::Principal {
                envelope,
                public_key_base64,
            } => Ok((envelope, public_key_base64)),
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
}

impl PersistedCredentialState {
    fn enroll(credential: &str) -> Result<Self, AuthBrokerError> {
        validate_credential_input(credential)?;
        let now = now_ms();
        Ok(Self {
            version: CREDENTIAL_STATE_VERSION,
            credential_digest: hex(&credential_digest(credential)),
            credential_epoch: 1,
            status: CredentialLifecycleStatus::Active,
            enrolled_at_ms: now,
            updated_at_ms: now,
        })
    }

    fn from_legacy_digest(credential_digest: [u8; digest::SHA256_OUTPUT_LEN]) -> Self {
        let now = now_ms();
        Self {
            version: CREDENTIAL_STATE_VERSION,
            credential_digest: hex(&credential_digest),
            credential_epoch: 1,
            status: CredentialLifecycleStatus::Active,
            enrolled_at_ms: now,
            updated_at_ms: now,
        }
    }

    fn digest(&self) -> Result<[u8; digest::SHA256_OUTPUT_LEN], AuthBrokerError> {
        decode_digest(&self.credential_digest)
    }

    fn lifecycle(&self) -> CredentialLifecycleMetadata {
        CredentialLifecycleMetadata {
            credential_epoch: self.credential_epoch,
            status: self.status,
            enrolled_at_ms: self.enrolled_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }

    fn validate(&self) -> Result<(), AuthBrokerError> {
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
}

struct LocalAuthority {
    key_pair: Ed25519KeyPair,
    credential_state: PersistedCredentialState,
    credential_state_path: PathBuf,
}

impl LocalAuthority {
    pub fn open_or_initialize(
        root: impl AsRef<Path>,
        human_credential: &str,
    ) -> Result<Self, AuthBrokerError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_error)?;
        validate_credential_input(human_credential)?;
        let credential_state_path = root.join(CREDENTIAL_STATE_FILE);
        let legacy_credential_path = root.join(LEGACY_CREDENTIAL_FILE);
        let credential_state = if credential_state_path.exists() {
            read_credential_state(&credential_state_path)?
        } else if legacy_credential_path.exists() {
            let digest = decode_digest(
                &fs::read_to_string(&legacy_credential_path).map_err(storage_error)?,
            )?;
            let state = PersistedCredentialState::from_legacy_digest(digest);
            persist_credential_state(&credential_state_path, &state)?;
            fs::remove_file(&legacy_credential_path).map_err(storage_error)?;
            state
        } else {
            // Enrollment is intentionally only possible while no lifecycle
            // state exists.  Subsequent broker starts recover this state and
            // must present the registered credential.
            let state = PersistedCredentialState::enroll(human_credential)?;
            persist_credential_state(&credential_state_path, &state)?;
            state
        };
        let expected = credential_state.digest()?;
        let supplied = credential_digest(human_credential);
        if !bool::from(expected.ct_eq(&supplied)) {
            return Err(AuthBrokerError::InvalidCredential);
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
        })
    }

    #[must_use]
    pub fn public_key_base64(&self) -> String {
        BASE64.encode(self.key_pair.public_key().as_ref())
    }

    pub fn issue_human_principal(
        &self,
        human_credential: &str,
        capabilities: Vec<String>,
        ttl_ms: Option<u64>,
    ) -> Result<SignedPrincipalEnvelope, AuthBrokerError> {
        self.verify_active_credential(human_credential)?;
        let capabilities = validated_human_capabilities(capabilities)?;
        let now = now_ms();
        let claims = PrincipalClaims {
            principal_id: "local-human".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities,
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: KEY_ID.to_string(),
            issued_at_ms: now,
            expires_at_ms: ttl_ms.map(|ttl| now.saturating_add(ttl)),
            credential_fingerprint: format!("sha256:{}", self.credential_state.credential_digest),
            credential_epoch: self.credential_state.credential_epoch,
        };
        let signature_base64 = self.sign(&claims)?;
        Ok(SignedPrincipalEnvelope {
            key_id: KEY_ID.to_string(),
            claims,
            signature_base64,
        })
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
        persist_credential_state(&self.credential_state_path, &self.credential_state)
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
    use std::path::Path;

    use harness_contract::security::{SignedDecisionLease, SignedPrincipalEnvelope};

    use super::{AuthBrokerError, LocalAuthority};

    pub fn issue_human_principal(
        root: impl AsRef<Path>,
        credential: &str,
        capabilities: Vec<String>,
        ttl_ms: Option<u64>,
    ) -> Result<(SignedPrincipalEnvelope, String), AuthBrokerError> {
        let authority = LocalAuthority::open_or_initialize(root, credential)?;
        let envelope = authority.issue_human_principal(credential, capabilities, ttl_ms)?;
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
        let authority = LocalAuthority::open_or_initialize(root, credential)?;
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
) -> Result<(), AuthBrokerError> {
    use std::os::unix::{fs::PermissionsExt, net::UnixListener};

    let mut authority = LocalAuthority::open_or_initialize(root, human_credential)?;
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

#[cfg(not(unix))]
pub fn serve_local(
    _root: impl AsRef<Path>,
    _human_credential: &str,
    _socket_path: impl AsRef<Path>,
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
    let mut reader = BufReader::new(stream.try_clone().map_err(storage_error)?);
    let mut line = String::new();
    reader.read_line(&mut line).map_err(storage_error)?;
    let response = match serde_json::from_str::<BrokerRequest>(line.trim()) {
        Ok(BrokerRequest::AuthenticateHuman {
            credential,
            capabilities,
            ttl_ms,
        }) => match authority.issue_human_principal(&credential, capabilities, ttl_ms) {
            Ok(envelope) => BrokerResponse::Principal {
                public_key_base64: authority.public_key_base64(),
                envelope,
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

fn validated_human_capabilities(capabilities: Vec<String>) -> Result<Vec<String>, AuthBrokerError> {
    let mut approved = Vec::new();
    for capability in capabilities {
        if !HUMAN_CAPABILITIES.contains(&capability.as_str()) {
            return Err(AuthBrokerError::CapabilityDenied(capability));
        }
        if !approved.contains(&capability) {
            approved.push(capability);
        }
    }
    Ok(approved)
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
    let mut file = fs::File::create(&temporary).map_err(storage_error)?;
    file.write_all(content).map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .map_err(storage_error)?;
    }
    fs::rename(temporary, path).map_err(storage_error)
}

fn read_credential_state(path: &Path) -> Result<PersistedCredentialState, AuthBrokerError> {
    let bytes = fs::read(path).map_err(storage_error)?;
    let state = serde_json::from_slice::<PersistedCredentialState>(&bytes)
        .map_err(|error| AuthBrokerError::InvalidCredentialState(error.to_string()))?;
    state.validate()?;
    Ok(state)
}

fn persist_credential_state(
    path: &Path,
    state: &PersistedCredentialState,
) -> Result<(), AuthBrokerError> {
    state.validate()?;
    let encoded = serde_json::to_vec_pretty(state)
        .map_err(|error| AuthBrokerError::Serialization(error.to_string()))?;
    write_private_bytes(path, &encoded)
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

fn storage_error(error: std::io::Error) -> AuthBrokerError {
    AuthBrokerError::Storage(error.to_string())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn authority_rejects_forged_credential_and_keeps_stable_public_key() {
        let root = std::env::temp_dir().join(format!("cowd-auth-broker-{}", uuid::Uuid::new_v4()));
        let authority = LocalAuthority::open_or_initialize(&root, "credential").expect("authority");
        let public_key = authority.public_key_base64();
        assert!(authority
            .issue_human_principal("forged", vec!["approval.respond".to_string()], None)
            .is_err());
        let reopened = LocalAuthority::open_or_initialize(&root, "credential").expect("reopen");
        assert_eq!(public_key, reopened.public_key_base64());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn first_enrollment_persists_only_lifecycle_state_and_reopens_with_registered_credential() {
        let root = std::env::temp_dir().join(format!("cowd-auth-broker-{}", uuid::Uuid::new_v4()));
        let authority =
            LocalAuthority::open_or_initialize(&root, "first-credential").expect("enrollment");
        let lifecycle = authority.credential_lifecycle();
        assert_eq!(lifecycle.status, CredentialLifecycleStatus::Active);
        assert_eq!(lifecycle.credential_epoch, 1);

        let persisted = fs::read_to_string(root.join(CREDENTIAL_STATE_FILE)).expect("state file");
        assert!(!persisted.contains("first-credential"));
        assert!(!root.join(LEGACY_CREDENTIAL_FILE).exists());

        let reopened = LocalAuthority::open_or_initialize(&root, "first-credential")
            .expect("registered credential reopens authority");
        assert_eq!(authority.public_key_base64(), reopened.public_key_base64());
        assert_eq!(reopened.credential_lifecycle(), lifecycle);
        assert!(matches!(
            LocalAuthority::open_or_initialize(&root, "wrong-credential"),
            Err(AuthBrokerError::InvalidCredential)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn unix_client_receives_a_signed_principal_without_access_to_authority_files() {
        use std::os::unix::net::UnixListener;

        let root = std::env::temp_dir().join(format!("cowd-auth-broker-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("broker root");
        let socket = BrokerClient::default_socket(&root);
        let mut authority =
            LocalAuthority::open_or_initialize(&root, "credential").expect("authority");
        let listener = UnixListener::bind(&socket).expect("listener");
        let worker = std::thread::spawn(move || {
            for _ in 0..3 {
                let (stream, _) = listener.accept().expect("client connection");
                handle_client(&mut authority, stream).expect("broker response");
            }
        });

        let client = BrokerClient::new(&socket);
        let (envelope, public_key) = client
            .authenticate_human(
                "credential",
                vec!["approval.respond".to_string()],
                Some(1_000),
            )
            .expect("signed principal");
        let verified = runtime_free_verify(&envelope, &public_key);
        assert!(
            verified,
            "broker response must carry a verifiable signature"
        );
        assert!(client
            .authenticate_human("credential", vec!["not.allowed".to_string()], None)
            .is_err());
        let (lease, lease_public_key) = client
            .issue_decision_lease(
                "credential",
                "proposal:p-1",
                "publish_initial_stable",
                "definition.agent:workspace/cowd/reviewer",
                "sha256:evidence",
                now_ms().saturating_add(1_000),
            )
            .expect("signed decision lease");
        assert_eq!(public_key, lease_public_key);
        assert_eq!(lease.claims.issuer, KEY_ID);
        assert_eq!(lease.claims.principal_id, "local-human");
        worker.join().expect("broker worker");
        let _ = fs::remove_file(socket);
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn credential_rotation_revocation_and_recovery_are_epoch_bound_and_fail_closed() {
        use std::os::unix::net::UnixListener;

        let root = std::env::temp_dir().join(format!("cowd-auth-broker-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("broker root");
        let socket = BrokerClient::default_socket(&root);
        let mut authority =
            LocalAuthority::open_or_initialize(&root, "credential-v1").expect("authority");
        let listener = UnixListener::bind(&socket).expect("listener");
        let worker = std::thread::spawn(move || {
            for _ in 0..12 {
                let (stream, _) = listener.accept().expect("client connection");
                handle_client(&mut authority, stream).expect("broker response");
            }
        });

        let client = BrokerClient::new(&socket);
        let initial = client.credential_lifecycle().expect("initial lifecycle");
        assert_eq!(initial.status, CredentialLifecycleStatus::Active);
        assert_eq!(initial.credential_epoch, 1);
        let (old_envelope, _) = client
            .authenticate_human("credential-v1", vec!["approval.respond".to_string()], None)
            .expect("initial envelope");
        let (old_lease, _) = client
            .issue_decision_lease(
                "credential-v1",
                "proposal:p-1",
                "publish",
                "definition.agent:workspace/cowd/reviewer",
                "sha256:evidence-v1",
                now_ms().saturating_add(1_000),
            )
            .expect("initial lease");
        assert_eq!(old_envelope.claims.credential_epoch, 1);
        assert_eq!(old_lease.claims.credential_epoch, 1);

        let rotated = client
            .rotate_credential("credential-v1", "credential-v2")
            .expect("rotate credential");
        assert_eq!(rotated.status, CredentialLifecycleStatus::Active);
        assert_eq!(rotated.credential_epoch, 2);
        assert!(client
            .authenticate_human("credential-v1", vec!["approval.respond".to_string()], None)
            .is_err());
        assert!(client
            .issue_decision_lease(
                "credential-v1",
                "proposal:p-1",
                "publish",
                "definition.agent:workspace/cowd/reviewer",
                "sha256:evidence-v1",
                now_ms().saturating_add(1_000),
            )
            .is_err());
        let (rotated_envelope, _) = client
            .authenticate_human("credential-v2", vec!["approval.respond".to_string()], None)
            .expect("rotated credential signs a new envelope");
        assert_eq!(rotated_envelope.claims.credential_epoch, 2);

        let revoked = client
            .revoke_credential("credential-v2")
            .expect("revoke credential");
        assert_eq!(revoked.status, CredentialLifecycleStatus::Revoked);
        assert_eq!(revoked.credential_epoch, 3);
        assert!(client
            .authenticate_human("credential-v2", vec!["approval.respond".to_string()], None)
            .is_err());
        assert!(client
            .issue_decision_lease(
                "credential-v2",
                "proposal:p-1",
                "publish",
                "definition.agent:workspace/cowd/reviewer",
                "sha256:evidence-v2",
                now_ms().saturating_add(1_000),
            )
            .is_err());

        let recovered = client
            .recover_credential("credential-v2", "credential-v3")
            .expect("explicit recovery");
        assert_eq!(recovered.status, CredentialLifecycleStatus::Active);
        assert_eq!(recovered.credential_epoch, 4);
        let (recovered_envelope, _) = client
            .authenticate_human("credential-v3", vec!["approval.respond".to_string()], None)
            .expect("recovered credential signs a new envelope");
        assert_eq!(recovered_envelope.claims.credential_epoch, 4);

        worker.join().expect("broker worker");
        let reopened = LocalAuthority::open_or_initialize(&root, "credential-v3")
            .expect("recovered state persists across restart");
        assert_eq!(reopened.credential_lifecycle(), recovered);
        let _ = fs::remove_file(socket);
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    fn runtime_free_verify(envelope: &SignedPrincipalEnvelope, public_key: &str) -> bool {
        let Ok(public_key) = BASE64.decode(public_key) else {
            return false;
        };
        let Ok(signature) = BASE64.decode(&envelope.signature_base64) else {
            return false;
        };
        let Ok(payload) = serde_json::to_vec(&envelope.claims) else {
            return false;
        };
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public_key)
            .verify(&payload, &signature)
            .is_ok()
    }
}
