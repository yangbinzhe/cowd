#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use evolution_test_support::{fixture, register_and_evaluate, HumanAuthority, CANDIDATE_ID};
use harness_contract::reality::EvidenceRef;
use runtime::{
    ReleaseChangeAction, ReleaseChangeRequest, ReleaseChangeReviewDecision,
    ReleaseChangeReviewStatus,
};

#[tokio::test]
async fn release_generation_fence_rejects_a_stale_canary_review_without_partial_rollout() {
    let fixture = fixture();
    let candidate = register_and_evaluate(&fixture, CANDIDATE_ID, 1, 2).await;
    let canary = fixture
        .services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("pending canary review");
    let authority = HumanAuthority::new();

    let pointer_review = fixture
        .services
        .request_evolution_release_change(ReleaseChangeRequest {
            request_id: "advance-generation-before-stale-canary".to_string(),
            subject: candidate.subject.clone(),
            action: ReleaseChangeAction::SetDefaultLatest,
            selector: None,
            candidate_id: None,
            evidence_refs: vec![EvidenceRef::new("audit", "advance-generation")],
        })
        .expect("pointer review");
    fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&pointer_review),
            &pointer_review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "advance release generation through a separate reviewed action".to_string(),
        )
        .expect("pointer decision");

    assert!(fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&canary),
            &canary.review_id,
            ReleaseChangeReviewDecision::Approve,
            "stale canary decision".to_string(),
        )
        .is_err());
    assert_eq!(
        fixture
            .services
            .evolution_release_review(&canary.review_id)
            .expect("canary projection")
            .status,
        ReleaseChangeReviewStatus::Superseded
    );
    assert_eq!(
        fixture
            .services
            .evolution_release_reviews()
            .expect("release reviews")
            .iter()
            .filter(|review| review.action == ReleaseChangeAction::PromoteCanary)
            .count(),
        1,
        "the stale decision cannot append a second Canary assignment or review"
    );
}
