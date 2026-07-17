//! Runtime-side verification of signed caller identities.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use harness_contract::security::{
    DecisionLeaseClaims, PrincipalClaims, SignedDecisionLease, SignedPrincipalEnvelope,
};
use ring::signature::{UnparsedPublicKey, ED25519};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PrincipalVerificationError {
    #[error("principal envelope key id is unsupported")]
    UnsupportedKey,
    #[error("principal envelope signature is invalid")]
    InvalidSignature,
    #[error("principal envelope payload is invalid: {0}")]
    InvalidPayload(String),
    #[error("principal envelope has expired")]
    Expired,
    #[error("signed identity issuer does not match the pinned authority")]
    IssuerMismatch,
    #[error("principal or decision lease credential epoch is no longer current")]
    CredentialEpochMismatch,
    #[error("decision lease is not bound to the authenticated principal")]
    LeasePrincipalMismatch,
    #[error("decision lease does not match the protected action")]
    LeaseBindingMismatch,
    #[error("decision lease has expired")]
    LeaseExpired,
}

/// A verified identity can only be created through `PrincipalVerifier`.
/// Its fields remain private so HTTP payloads and Agent tools cannot forge it.
#[derive(Debug, Clone)]
pub struct VerifiedPrincipal {
    claims: PrincipalClaims,
}

impl VerifiedPrincipal {
    #[must_use]
    pub fn claims(&self) -> &PrincipalClaims {
        &self.claims
    }

    #[must_use]
    pub fn is_human_interactive(&self) -> bool {
        self.claims.is_human_interactive()
    }

    #[must_use]
    pub fn has_capability(&self, capability: &str) -> bool {
        self.claims.has_capability(capability)
    }

    #[must_use]
    pub fn credential_epoch(&self) -> u64 {
        self.claims.credential_epoch
    }

    /// Construct a verified principal only inside explicit test builds.
    ///
    /// Production callers must continue to pass through `PrincipalVerifier`;
    /// this fixture exists so downstream route tests can exercise permission
    /// matrices with multiple signed-identity shapes without exposing a
    /// forgeable constructor in normal builds.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub fn from_test_claims(claims: PrincipalClaims) -> Self {
        Self { claims }
    }
}

/// Exact protected operation to which a human decision lease must be bound.
/// All fields are compared by Runtime after signature verification; callers
/// cannot repurpose a lease issued for one review into another mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionLeaseExpectation {
    pub review_id: String,
    pub action: String,
    pub scope: String,
    pub evidence_digest: String,
}

impl DecisionLeaseExpectation {
    #[must_use]
    pub fn new(
        review_id: impl Into<String>,
        action: impl Into<String>,
        scope: impl Into<String>,
        evidence_digest: impl Into<String>,
    ) -> Self {
        Self {
            review_id: review_id.into(),
            action: action.into(),
            scope: scope.into(),
            evidence_digest: evidence_digest.into(),
        }
    }
}

/// Verified, operation-bound human authorization. Its claims are private so
/// only `PrincipalVerifier` can produce a value accepted by Runtime writers.
#[derive(Debug, Clone)]
pub struct VerifiedDecisionLease {
    claims: DecisionLeaseClaims,
}

impl VerifiedDecisionLease {
    #[must_use]
    pub fn lease_id(&self) -> &str {
        &self.claims.lease_id
    }

    #[must_use]
    pub fn principal_id(&self) -> &str {
        &self.claims.principal_id
    }

    #[must_use]
    pub fn review_id(&self) -> &str {
        &self.claims.review_id
    }

    #[must_use]
    pub fn action(&self) -> &str {
        &self.claims.action
    }

    #[must_use]
    pub fn scope(&self) -> &str {
        &self.claims.scope
    }

    #[must_use]
    pub fn evidence_digest(&self) -> &str {
        &self.claims.evidence_digest
    }

    #[must_use]
    pub fn credential_epoch(&self) -> u64 {
        self.claims.credential_epoch
    }
}

#[derive(Debug, Clone)]
pub struct PrincipalVerifier {
    public_key: Vec<u8>,
    key_id: String,
    expected_credential_epoch: Option<u64>,
}

impl PrincipalVerifier {
    pub fn from_base64(
        key_id: impl Into<String>,
        public_key_base64: &str,
    ) -> Result<Self, PrincipalVerificationError> {
        let public_key = BASE64
            .decode(public_key_base64)
            .map_err(|error| PrincipalVerificationError::InvalidPayload(error.to_string()))?;
        Ok(Self {
            public_key,
            key_id: key_id.into(),
            expected_credential_epoch: None,
        })
    }

    /// Bind verification to the current epoch supplied by the authority's
    /// trust metadata. This turns credential rotation and revocation into an
    /// immediate verifier-level fence rather than merely preventing future
    /// signatures from being issued.
    #[must_use]
    pub fn requiring_credential_epoch(mut self, epoch: u64) -> Self {
        self.expected_credential_epoch = Some(epoch);
        self
    }

    pub fn verify(
        &self,
        envelope: &SignedPrincipalEnvelope,
    ) -> Result<VerifiedPrincipal, PrincipalVerificationError> {
        if envelope.key_id != self.key_id {
            return Err(PrincipalVerificationError::UnsupportedKey);
        }
        if envelope.claims.issuer != self.key_id {
            return Err(PrincipalVerificationError::IssuerMismatch);
        }
        let payload = serde_json::to_vec(&envelope.claims)
            .map_err(|error| PrincipalVerificationError::InvalidPayload(error.to_string()))?;
        let signature = BASE64
            .decode(&envelope.signature_base64)
            .map_err(|error| PrincipalVerificationError::InvalidPayload(error.to_string()))?;
        UnparsedPublicKey::new(&ED25519, &self.public_key)
            .verify(&payload, &signature)
            .map_err(|_| PrincipalVerificationError::InvalidSignature)?;
        if envelope
            .claims
            .expires_at_ms
            .is_some_and(|expires_at| now_ms() > expires_at)
        {
            return Err(PrincipalVerificationError::Expired);
        }
        if self
            .expected_credential_epoch
            .is_some_and(|epoch| envelope.claims.credential_epoch != epoch)
        {
            return Err(PrincipalVerificationError::CredentialEpochMismatch);
        }
        Ok(VerifiedPrincipal {
            claims: envelope.claims.clone(),
        })
    }

    pub fn verify_decision_lease(
        &self,
        lease: &SignedDecisionLease,
        principal: &VerifiedPrincipal,
        expected: &DecisionLeaseExpectation,
    ) -> Result<VerifiedDecisionLease, PrincipalVerificationError> {
        if lease.key_id != self.key_id {
            return Err(PrincipalVerificationError::UnsupportedKey);
        }
        if lease.claims.issuer != self.key_id {
            return Err(PrincipalVerificationError::IssuerMismatch);
        }
        let payload = serde_json::to_vec(&lease.claims)
            .map_err(|error| PrincipalVerificationError::InvalidPayload(error.to_string()))?;
        let signature = BASE64
            .decode(&lease.signature_base64)
            .map_err(|error| PrincipalVerificationError::InvalidPayload(error.to_string()))?;
        UnparsedPublicKey::new(&ED25519, &self.public_key)
            .verify(&payload, &signature)
            .map_err(|_| PrincipalVerificationError::InvalidSignature)?;
        if now_ms() > lease.claims.expires_at_ms {
            return Err(PrincipalVerificationError::LeaseExpired);
        }
        if self
            .expected_credential_epoch
            .is_some_and(|epoch| lease.claims.credential_epoch != epoch)
        {
            return Err(PrincipalVerificationError::CredentialEpochMismatch);
        }
        if lease.claims.principal_id != principal.claims.principal_id
            || lease.claims.credential_epoch != principal.claims.credential_epoch
        {
            return Err(PrincipalVerificationError::LeasePrincipalMismatch);
        }
        if lease.claims.review_id != expected.review_id
            || lease.claims.action != expected.action
            || lease.claims.scope != expected.scope
            || lease.claims.evidence_digest != expected.evidence_digest
        {
            return Err(PrincipalVerificationError::LeaseBindingMismatch);
        }
        Ok(VerifiedDecisionLease {
            claims: lease.claims.clone(),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
pub(crate) fn test_human_interactive_principal() -> VerifiedPrincipal {
    VerifiedPrincipal {
        claims: PrincipalClaims {
            principal_id: "runtime-test-human".to_string(),
            kind: harness_contract::security::PrincipalKind::Human,
            scopes: vec!["runtime-test".to_string()],
            capabilities: vec![
                "approval.respond".to_string(),
                "evolution.release.manage".to_string(),
            ],
            assurance: harness_contract::security::PrincipalAssurance::HumanInteractive,
            issuer: "runtime-test".to_string(),
            issued_at_ms: now_ms(),
            expires_at_ms: None,
            credential_fingerprint: "test".to_string(),
            credential_epoch: 1,
            profile_revision: 1,
        },
    }
}

#[cfg(test)]
pub(crate) fn test_verified_decision_lease(
    review_id: impl Into<String>,
    action: impl Into<String>,
    scope: impl Into<String>,
    evidence_digest: impl Into<String>,
) -> VerifiedDecisionLease {
    VerifiedDecisionLease {
        claims: DecisionLeaseClaims {
            lease_id: format!("runtime-test-lease-{}", uuid::Uuid::new_v4()),
            principal_id: "runtime-test-human".to_string(),
            review_id: review_id.into(),
            action: action.into(),
            scope: scope.into(),
            evidence_digest: evidence_digest.into(),
            issuer: "runtime-test".to_string(),
            issued_at_ms: now_ms(),
            expires_at_ms: now_ms().saturating_add(60_000),
            credential_epoch: 1,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::security::{PrincipalAssurance, PrincipalKind, SignedDecisionLease};
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };

    #[test]
    fn verifier_rejects_tampered_principal_claims() {
        let key = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .expect("key material")
                .as_ref(),
        )
        .expect("key pair");
        let claims = PrincipalClaims {
            principal_id: "human".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities: vec!["approval.respond".to_string()],
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: "test".to_string(),
            issued_at_ms: 1,
            expires_at_ms: None,
            credential_fingerprint: "test".to_string(),
            credential_epoch: 1,
            profile_revision: 1,
        };
        let payload = serde_json::to_vec(&claims).expect("payload");
        let mut envelope = SignedPrincipalEnvelope {
            key_id: "test".to_string(),
            claims,
            signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
        };
        let verifier =
            PrincipalVerifier::from_base64("test", &BASE64.encode(key.public_key().as_ref()))
                .expect("verifier");
        assert!(verifier.verify(&envelope).is_ok());
        envelope.claims.capabilities.push("release".to_string());
        assert!(matches!(
            verifier.verify(&envelope),
            Err(PrincipalVerificationError::InvalidSignature)
        ));
    }

    #[test]
    fn verifier_binds_decision_lease_to_principal_and_evidence() {
        let key = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .expect("key material")
                .as_ref(),
        )
        .expect("key pair");
        let claims = PrincipalClaims {
            principal_id: "human".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities: vec!["evolution.release.manage".to_string()],
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: "test".to_string(),
            issued_at_ms: 1,
            expires_at_ms: None,
            credential_fingerprint: "test".to_string(),
            credential_epoch: 7,
            profile_revision: 1,
        };
        let principal_payload = serde_json::to_vec(&claims).expect("principal payload");
        let principal = SignedPrincipalEnvelope {
            key_id: "test".to_string(),
            claims,
            signature_base64: BASE64.encode(key.sign(&principal_payload).as_ref()),
        };
        let verifier =
            PrincipalVerifier::from_base64("test", &BASE64.encode(key.public_key().as_ref()))
                .expect("verifier");
        let principal = verifier.verify(&principal).expect("verified principal");
        let claims = DecisionLeaseClaims {
            lease_id: "lease-1".to_string(),
            principal_id: "human".to_string(),
            review_id: "candidate:c-1".to_string(),
            action: "promote".to_string(),
            scope: "evolution.candidate:c-1".to_string(),
            evidence_digest: "sha256:evidence".to_string(),
            issuer: "test".to_string(),
            issued_at_ms: now_ms(),
            expires_at_ms: now_ms().saturating_add(60_000),
            credential_epoch: 7,
        };
        let payload = serde_json::to_vec(&claims).expect("lease payload");
        let lease = SignedDecisionLease {
            key_id: "test".to_string(),
            claims,
            signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
        };
        let expected = DecisionLeaseExpectation::new(
            "candidate:c-1",
            "promote",
            "evolution.candidate:c-1",
            "sha256:evidence",
        );
        assert!(verifier
            .verify_decision_lease(&lease, &principal, &expected)
            .is_ok());
        let mismatched = DecisionLeaseExpectation::new(
            "candidate:c-1",
            "rollback",
            "evolution.candidate:c-1",
            "sha256:evidence",
        );
        assert!(matches!(
            verifier.verify_decision_lease(&lease, &principal, &mismatched),
            Err(PrincipalVerificationError::LeaseBindingMismatch)
        ));
    }

    #[test]
    fn verifier_rejects_signed_identity_from_a_stale_credential_epoch() {
        let key = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .expect("key material")
                .as_ref(),
        )
        .expect("key pair");
        let claims = PrincipalClaims {
            principal_id: "human".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities: vec!["approval.respond".to_string()],
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: "test".to_string(),
            issued_at_ms: now_ms(),
            expires_at_ms: Some(now_ms().saturating_add(60_000)),
            credential_fingerprint: "test".to_string(),
            credential_epoch: 3,
            profile_revision: 1,
        };
        let payload = serde_json::to_vec(&claims).expect("payload");
        let envelope = SignedPrincipalEnvelope {
            key_id: "test".to_string(),
            claims,
            signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
        };
        let verifier =
            PrincipalVerifier::from_base64("test", &BASE64.encode(key.public_key().as_ref()))
                .expect("verifier")
                .requiring_credential_epoch(4);
        assert!(matches!(
            verifier.verify(&envelope),
            Err(PrincipalVerificationError::CredentialEpochMismatch)
        ));
    }
}
