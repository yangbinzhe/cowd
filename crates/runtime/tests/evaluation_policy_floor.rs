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
    DecisionLeaseExpectation, EvaluationPolicyChangeIntent, EvaluationPolicyChangeReview,
    PrincipalVerifier, ReleaseChangeReviewDecision, RuntimeServices, VerifiedDecisionLease,
    VerifiedPrincipal,
};

mod evolution_test_support;

fn signed_human_lease(
    review: &EvaluationPolicyChangeReview,
    action: &str,
) -> (VerifiedPrincipal, VerifiedDecisionLease) {
    let key = Ed25519KeyPair::from_pkcs8(
        Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .expect("key material")
            .as_ref(),
    )
    .expect("key");
    let key_id = "policy-test-authority";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    let claims = PrincipalClaims {
        principal_id: "human:policy-operator".to_string(),
        kind: PrincipalKind::Human,
        scopes: vec!["workspace".to_string()],
        capabilities: vec!["evolution.release.manage".to_string()],
        assurance: PrincipalAssurance::HumanInteractive,
        issuer: key_id.to_string(),
        issued_at_ms: now,
        expires_at_ms: Some(now + 60_000),
        credential_fingerprint: "fixture".to_string(),
        credential_epoch: 1,
    };
    let verifier =
        PrincipalVerifier::from_base64(key_id, &BASE64.encode(key.public_key().as_ref()))
            .expect("verifier");
    let payload = serde_json::to_vec(&claims).expect("principal payload");
    let principal = verifier
        .verify(&SignedPrincipalEnvelope {
            key_id: key_id.to_string(),
            claims: claims.clone(),
            signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
        })
        .expect("principal");
    let lease_claims = DecisionLeaseClaims {
        lease_id: format!("lease:{action}"),
        principal_id: claims.principal_id,
        review_id: review.review_id.clone(),
        action: action.to_string(),
        scope: review.scope_ref().to_string(),
        evidence_digest: review.evidence_digest(),
        issuer: key_id.to_string(),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
        credential_epoch: 1,
    };
    let payload = serde_json::to_vec(&lease_claims).expect("lease payload");
    let expected = DecisionLeaseExpectation::new(
        &lease_claims.review_id,
        &lease_claims.action,
        &lease_claims.scope,
        &lease_claims.evidence_digest,
    );
    let lease = verifier
        .verify_decision_lease(
            &SignedDecisionLease {
                key_id: key_id.to_string(),
                claims: lease_claims,
                signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
            },
            &principal,
            &expected,
        )
        .expect("lease");
    (principal, lease)
}

#[test]
fn policy_floor_can_only_change_through_its_own_human_review_and_verified_lease() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let before = services.evolution_evaluation_policy_floor();
    let mut next = before.clone();
    next.revision = before.revision + 1;
    next.minimum_samples = before.minimum_samples + 2;
    let review = services
        .request_evolution_evaluation_policy_change(EvaluationPolicyChangeIntent {
            request_id: "raise-minimum-samples".to_string(),
            next_policy: next.clone(),
            evidence_refs: vec!["audit:policy-rationale".to_string()],
        })
        .expect("policy review");

    let (principal, wrong_lease) = signed_human_lease(&review, "promote_canary");
    assert!(services
        .decide_evolution_evaluation_policy_change(
            &principal,
            &wrong_lease,
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "must fail".to_string()
        )
        .is_err());
    assert_eq!(
        services.evolution_evaluation_policy_floor(),
        before,
        "invalid lease cannot partially mutate the active floor"
    );
    assert_eq!(
        services
            .evolution_evaluation_policy_reviews()
            .expect("reviews")[0]
            .status,
        runtime::ReleaseChangeReviewStatus::Pending
    );

    let (_, lease) = signed_human_lease(&review, review.action_key());
    let applied = services
        .decide_evolution_evaluation_policy_change(
            &principal,
            &lease,
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "raise protected evaluation floor".to_string(),
        )
        .expect("policy decision")
        .expect("applied floor");
    assert_eq!(applied, next);
    assert_eq!(services.evolution_evaluation_policy_floor(), next);
    assert_eq!(
        services
            .evolution_evaluation_policy_reviews()
            .expect("review projection")[0]
            .status,
        runtime::ReleaseChangeReviewStatus::Approved
    );
}

#[tokio::test]
async fn raised_policy_floor_rejects_a_definition_candidate_with_a_weaker_baseline_contract() {
    let fixture = evolution_test_support::fixture();
    let before = fixture.services.evolution_evaluation_policy_floor();
    let mut next = before.clone();
    next.revision = before.revision + 1;
    next.minimum_samples = before.minimum_samples + 1;
    let review = fixture
        .services
        .request_evolution_evaluation_policy_change(EvaluationPolicyChangeIntent {
            request_id: "reject-weaker-agent-contract".to_string(),
            next_policy: next,
            evidence_refs: vec!["audit:raised-floor".to_string()],
        })
        .expect("policy review");
    let authority = evolution_test_support::HumanAuthority::new();
    let lease = authority.lease_for_expectation(DecisionLeaseExpectation::new(
        review.review_id.clone(),
        review.action_key(),
        review.scope_ref(),
        review.evidence_digest(),
    ));
    fixture
        .services
        .decide_evolution_evaluation_policy_change(
            authority.principal(),
            &lease,
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "raise minimum paired samples".to_string(),
        )
        .expect("approved policy");

    assert!(evolution_test_support::try_register_and_evaluate(
        &fixture,
        "candidate-below-raised-floor",
        1,
        2,
    )
    .await
    .is_err());
}
