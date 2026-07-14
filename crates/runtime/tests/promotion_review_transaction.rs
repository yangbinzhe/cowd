#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use evolution_test_support::{fixture, register_and_evaluate, HumanAuthority, CANDIDATE_ID};
use runtime::{
    DecisionLeaseExpectation, GlobalApprovalStatus, ReleaseChangeAction,
    ReleaseChangeReviewDecision, ReleaseChangeReviewStatus,
};

#[tokio::test]
async fn human_canary_decision_commits_review_approval_and_assignment_together() {
    let fixture = fixture();
    let candidate = register_and_evaluate(&fixture, CANDIDATE_ID, 1, 2).await;
    let review = fixture
        .services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("pending canary review");
    let authority = HumanAuthority::new();

    assert!(fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for_expectation(DecisionLeaseExpectation::new(
                review.review_id.clone(),
                "evolution.release.promote_stable",
                review.subject.scope_ref(),
                review.evidence_digest(),
            )),
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "wrong lease must not write a partial decision".to_string(),
        )
        .is_err());
    assert_eq!(
        fixture
            .services
            .evolution_release_review(&review.review_id)
            .expect("review")
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
    assert!(fixture
        .services
        .evolution_release_reviews()
        .expect("reviews")
        .iter()
        .all(|item| item.action != ReleaseChangeAction::PromoteStable));

    let assignment = fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&review),
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve checked canary transaction".to_string(),
        )
        .expect("approved decision")
        .expect("assignment");
    assert_eq!(assignment.action, ReleaseChangeAction::PromoteCanary);
    assert_eq!(
        fixture
            .services
            .approval_queue()
            .get(&review.approval_id)
            .expect("approval projection")
            .status,
        GlobalApprovalStatus::Approved
    );
}
