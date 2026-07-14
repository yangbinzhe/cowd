use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn runtime_backed_team_template_catalog_has_a_stable_http_projection() {
    let harness = GatewayTestHarness::in_memory().expect("test harness");
    let response = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/team-templates")
                .body(Body::empty())
                .expect("team template request"),
        )
        .await
        .expect("team template response");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("team template body"),
    )
    .expect("team template JSON");
    assert_eq!(payload["kind"], "team_templates");
    assert_eq!(payload["source"], "runtime.definition_catalog");
    assert!(payload["templates"].is_array());
}
