#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use evolution_test_support::{fixture, HumanAuthority};
use harness_contract::agent::{AgentCapability, AgentDefinitionRevisionRef, RevisionSelector};
use runtime::{
    AgentBindingRequest, EvolutionCandidateSubject, ReleaseChangeAction, ReleaseChangeRequest,
    ReleaseChangeReviewDecision,
};

#[test]
fn published_definition_requires_a_human_initial_stable_decision_before_binding() {
    let fixture = fixture();
    let mut request = AgentBindingRequest::new(
        fixture.definition_id.clone(),
        RevisionSelector::LatestApprovedStable,
        "instance:initial-stable",
        "session:initial-stable",
        "task:initial-stable",
    );
    request.granted_capabilities = vec![AgentCapability::Read];
    assert!(
        fixture
            .services
            .compile_agent_binding(request.clone())
            .is_err(),
        "published alone is not runnable"
    );

    let review = fixture
        .services
        .request_evolution_release_change(ReleaseChangeRequest {
            request_id: "publish-initial-stable".to_string(),
            subject: EvolutionCandidateSubject::AgentDefinition {
                revision_ref: AgentDefinitionRevisionRef::new(fixture.definition_id.clone(), 1)
                    .expect("revision ref"),
            },
            action: ReleaseChangeAction::PublishInitialStable,
            selector: None,
            candidate_id: None,
            evidence_refs: vec!["evidence:definition-review".to_string()],
        })
        .expect("pending human review");
    let authority = HumanAuthority::new();
    fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&review),
            &review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "human accepts the initial capability boundary".to_string(),
        )
        .expect("human release decision")
        .expect("stable assignment");

    let bound = fixture
        .services
        .compile_agent_binding(request)
        .expect("approved initial stable binding");
    assert_eq!(bound.snapshot.definition_ref.revision, 1);
}
