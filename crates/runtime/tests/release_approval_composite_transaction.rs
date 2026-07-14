#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "evolution_test_support/mod.rs"]
mod evolution_test_support;

use evolution_test_support::{fixture, register_and_evaluate, HumanAuthority, CANDIDATE_ID};
use runtime::{
    DecisionLeaseExpectation, GlobalApprovalStatus, ReleaseChangeReviewDecision,
    ReleaseChangeReviewStatus,
};

#[tokio::test]
async fn invalid_release_lease_leaves_review_and_approval_pending_without_partial_rollout() {
    let fixture = fixture();
    let candidate = register_and_evaluate(&fixture, CANDIDATE_ID, 1, 2).await;
    let review = fixture
        .services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("pending Canary review");
    let authority = HumanAuthority::new();
    let wrong_scope = authority.lease_for_expectation(DecisionLeaseExpectation::new(
        review.review_id.clone(),
        review.action_key(),
        "workspace:not-the-candidate",
        review.evidence_digest(),
    ));

    assert!(fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &wrong_scope,
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "wrong scope must fail before the composite commit".to_string(),
        )
        .is_err());
    assert_eq!(
        fixture
            .services
            .evolution_release_review(&review.review_id)
            .expect("review projection")
            .status,
        ReleaseChangeReviewStatus::Pending
    );
    assert_eq!(
        fixture
            .services
            .approval_queue()
            .get(&review.approval_id)
            .expect("approval projection")
            .status,
        GlobalApprovalStatus::Pending
    );

    let assignment = fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&review),
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "verified human decision commits the complete release change".to_string(),
        )
        .expect("composite release transaction")
        .expect("Canary assignment");
    assert_eq!(assignment.approval_ref, review.approval_id);
    assert_eq!(
        fixture
            .services
            .evolution_release_review(&review.review_id)
            .expect("approved review")
            .status,
        ReleaseChangeReviewStatus::Approved
    );
}
