use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn runtime_owned_evolution_reviews_and_policy_are_projected_by_gateway() {
    let harness = GatewayTestHarness::in_memory().expect("test harness");

    let reviews = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/evolution/reviews")
                .body(Body::empty())
                .expect("review request"),
        )
        .await
        .expect("review response");
    assert_eq!(reviews.status(), StatusCode::OK);
    let reviews: serde_json::Value = serde_json::from_slice(
        &to_bytes(reviews.into_body(), usize::MAX)
            .await
            .expect("review body"),
    )
    .expect("review JSON");
    assert_eq!(reviews["kind"], "evolution.release_reviews");
    assert_eq!(reviews["owner"], "runtime");
    assert!(reviews["reviews"].is_array());

    let policy = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/evolution/evaluation-policy")
                .body(Body::empty())
                .expect("policy request"),
        )
        .await
        .expect("policy response");
    assert_eq!(policy.status(), StatusCode::OK);
    let policy: serde_json::Value = serde_json::from_slice(
        &to_bytes(policy.into_body(), usize::MAX)
            .await
            .expect("policy body"),
    )
    .expect("policy JSON");
    assert_eq!(policy["kind"], "evolution.evaluation_policy");
    assert_eq!(policy["owner"], "runtime");
    assert_eq!(policy["policy"]["revision"], 1);
    assert!(policy["policy"]["minimum_samples"].is_u64());
}
