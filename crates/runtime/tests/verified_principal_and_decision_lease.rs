#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::{SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use harness_contract::security::{
    DecisionLeaseClaims, PrincipalAssurance, PrincipalClaims, PrincipalKind, SignedDecisionLease,
    SignedPrincipalEnvelope,
};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use runtime::{
    DecisionLeaseExpectation, PrincipalVerificationError, PrincipalVerifier, RuntimeServices,
    VerifiedPrincipal,
};

struct HumanAuthority {
    key_id: String,
    key_pair: Ed25519KeyPair,
    verifier: PrincipalVerifier,
    principal: VerifiedPrincipal,
}

impl HumanAuthority {
    fn new() -> Self {
        let key_pair = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .expect("Ed25519 key material")
                .as_ref(),
        )
        .expect("Ed25519 key pair");
        let key_id = "v0-test-authority".to_string();
        let verifier = PrincipalVerifier::from_base64(
            key_id.clone(),
            &BASE64.encode(key_pair.public_key().as_ref()),
        )
        .expect("principal verifier");
        let claims = PrincipalClaims {
            principal_id: "human:v0-operator".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["workspace:cowd".to_string()],
            capabilities: vec!["evolution.release.manage".to_string()],
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: key_id.clone(),
            issued_at_ms: now_ms(),
            expires_at_ms: Some(now_ms().saturating_add(60_000)),
            credential_fingerprint: "v0-human-fixture".to_string(),
            credential_epoch: 7,
            profile_revision: 1,
        };
        let principal = verifier
            .verify(&SignedPrincipalEnvelope {
                key_id: key_id.clone(),
                signature_base64: sign_json(&key_pair, &claims),
                claims,
            })
            .expect("verified human principal");
        Self {
            key_id,
            key_pair,
            verifier,
            principal,
        }
    }

    fn signed_lease(&self, expected: &DecisionLeaseExpectation) -> SignedDecisionLease {
        let claims = DecisionLeaseClaims {
            lease_id: format!("lease:v0:{}", uuid::Uuid::new_v4()),
            principal_id: self.principal.claims().principal_id.clone(),
            review_id: expected.review_id.clone(),
            action: expected.action.clone(),
            scope: expected.scope.clone(),
            evidence_digest: expected.evidence_digest.clone(),
            issuer: self.key_id.clone(),
            issued_at_ms: now_ms(),
            expires_at_ms: now_ms().saturating_add(60_000),
            credential_epoch: self.principal.credential_epoch(),
        };
        SignedDecisionLease {
            key_id: self.key_id.clone(),
            signature_base64: sign_json(&self.key_pair, &claims),
            claims,
        }
    }
}

#[test]
fn signed_principal_and_lease_are_bound_to_exact_operation_then_consumed_once() {
    let authority = HumanAuthority::new();
    let expected = DecisionLeaseExpectation::new(
        "review:v0",
        "evolution.release.promote_canary",
        "workspace:cowd",
        "sha256:verified-evidence",
    );
    let signed_lease = authority.signed_lease(&expected);
    let lease = authority
        .verifier
        .verify_decision_lease(&signed_lease, &authority.principal, &expected)
        .expect("matching lease");

    let mismatched_scope = DecisionLeaseExpectation::new(
        "review:v0",
        "evolution.release.promote_canary",
        "workspace:other",
        "sha256:verified-evidence",
    );
    assert!(matches!(
        authority.verifier.verify_decision_lease(
            &signed_lease,
            &authority.principal,
            &mismatched_scope,
        ),
        Err(PrincipalVerificationError::LeaseBindingMismatch)
    ));

    let services = RuntimeServices::in_memory().expect("runtime services");
    services
        .consume_verified_decision_lease(lease.clone())
        .expect("first consumption is durable");
    let replay = services.consume_verified_decision_lease(lease);
    assert!(
        replay.is_err(),
        "the same verified lease may not authorize a second mutation"
    );
}

#[test]
fn tampering_claims_after_signing_cannot_escalate_an_agent_to_human() {
    let authority = HumanAuthority::new();
    let mut claims = authority.principal.claims().clone();
    claims.kind = PrincipalKind::Agent;
    claims.capabilities = vec!["evolution.release.manage".to_string()];
    let mut envelope = SignedPrincipalEnvelope {
        key_id: authority.key_id.clone(),
        signature_base64: sign_json(&authority.key_pair, &claims),
        claims,
    };
    let verified_agent = authority
        .verifier
        .verify(&envelope)
        .expect("signed Agent identity remains an Agent");
    assert!(!verified_agent.is_human_interactive());

    envelope.claims.kind = PrincipalKind::Human;
    envelope.claims.assurance = PrincipalAssurance::HumanInteractive;
    assert!(matches!(
        authority.verifier.verify(&envelope),
        Err(PrincipalVerificationError::InvalidSignature)
    ));
}

fn sign_json<T: serde::Serialize>(key_pair: &Ed25519KeyPair, value: &T) -> String {
    let payload = serde_json::to_vec(value).expect("signed payload");
    BASE64.encode(key_pair.sign(&payload).as_ref())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
