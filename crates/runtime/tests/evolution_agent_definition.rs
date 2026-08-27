#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use evolution_test_support::{fixture, register_and_evaluate, CANDIDATE_ID};
use runtime::EvolutionCandidateLifecycle;

#[tokio::test]
async fn published_definition_revisions_are_registered_and_evaluated_through_runtime_port() {
    let fixture = fixture();
    let candidate = register_and_evaluate(&fixture, CANDIDATE_ID, 1, 2).await;

    assert_eq!(
        candidate.lifecycle,
        EvolutionCandidateLifecycle::EvaluatedEligible
    );
    assert_eq!(
        candidate
            .evaluation_baseline
            .as_ref()
            .and_then(runtime::EvolutionEvaluationBaseline::published_revision),
        Some(1),
        "new candidate events carry the versioned immutable baseline; the scalar is legacy-only"
    );
    assert_eq!(candidate.baseline_revision, 0);
    assert!(candidate.comparison_report_ref.is_some());
    assert!(candidate.comparison_report_digest.is_some());
    assert!(fixture
        .services
        .evolution_release_reviews()
        .expect("review projection")
        .is_empty());
}
