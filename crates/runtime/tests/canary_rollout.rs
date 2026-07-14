#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use evolution_test_support::{fixture, register_and_evaluate, HumanAuthority, CANDIDATE_ID};
use harness_contract::agent::{AgentCapability, RevisionSelector};
use runtime::{AgentBindingRequest, ReleaseChangeReviewDecision};

#[tokio::test]
async fn approved_canary_routes_a_bound_agent_without_changing_stable_default() {
    let fixture = fixture();
    let candidate = register_and_evaluate(&fixture, CANDIDATE_ID, 1, 2).await;
    let review = fixture
        .services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("canary review");
    let authority = HumanAuthority::new();
    let assignment = fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&review),
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve deterministic fixture canary".to_string(),
        )
        .expect("canary decision")
        .expect("canary assignment");

    let mut request = AgentBindingRequest::new(
        fixture.definition_id.clone(),
        RevisionSelector::LatestApprovedStable,
        "instance:canary-routing",
        "session:canary-routing",
        "task:canary-routing",
    );
    request.granted_capabilities = vec![AgentCapability::Read];
    let binding = fixture
        .services
        .compile_agent_binding(request)
        .expect("canary binding");

    assert_eq!(binding.snapshot.definition_ref.revision, 2);
    assert_eq!(
        binding
            .snapshot
            .release
            .expect("canary provenance")
            .assignment_id,
        assignment.assignment_id
    );
    assert!(fixture.definition_id.as_str().starts_with("workspace/"));
    assert!(
        fixture
            .services
            .definition_registry()
            .resolve_agent(&fixture.definition_id, RevisionSelector::DefaultPointer)
            .is_err(),
        "Canary approval must not create a Stable default pointer"
    );
}
