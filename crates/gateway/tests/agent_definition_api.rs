use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn agent_discovery_uses_runtime_catalog_and_rejects_empty_intent() {
    let harness = GatewayTestHarness::in_memory().expect("test harness");

    let discovery = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/agents/discover?task=research%20a%20runtime%20route")
                .body(Body::empty())
                .expect("discovery request"),
        )
        .await
        .expect("discovery response");
    assert_eq!(discovery.status(), StatusCode::OK);
    let discovery: serde_json::Value = serde_json::from_slice(
        &to_bytes(discovery.into_body(), usize::MAX)
            .await
            .expect("discovery body"),
    )
    .expect("discovery JSON");
    assert_eq!(discovery["kind"], "agents");
    assert_eq!(discovery["action"], "discover");
    assert_eq!(discovery["source"], "runtime.definition_catalog");
    assert_eq!(discovery["task"], "research a runtime route");

    let empty = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/agents/discover?task=%20%20")
                .body(Body::empty())
                .expect("empty discovery request"),
        )
        .await
        .expect("empty discovery response");
    assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
    let empty: serde_json::Value = serde_json::from_slice(
        &to_bytes(empty.into_body(), usize::MAX)
            .await
            .expect("empty discovery body"),
    )
    .expect("empty discovery JSON");
    assert_eq!(empty["error"], "task query is required");
}
