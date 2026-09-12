use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn postgres_harness_isolates_sessions_and_survives_peer_shutdown() {
    let left = GatewayTestHarness::postgres().expect("left isolated PG harness");
    let right = GatewayTestHarness::postgres().expect("right isolated PG harness");
    for (harness, id) in [(&left, "pg-harness-left"), (&right, "pg-harness-right")] {
        let response = harness
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{id}/ensure"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"model":"test-model"}"#))
                    .expect("ensure request"),
            )
            .await
            .expect("ensure response");
        assert_eq!(response.status(), StatusCode::OK);
    }
    let absent = right
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/sessions/pg-harness-left")
                .body(Body::empty())
                .expect("cross-fixture request"),
        )
        .await
        .expect("cross-fixture response");
    assert_eq!(absent.status(), StatusCode::NOT_FOUND);
    drop(left);
    let retained = right
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/sessions/pg-harness-right")
                .body(Body::empty())
                .expect("surviving fixture request"),
        )
        .await
        .expect("surviving fixture response");
    assert_eq!(retained.status(), StatusCode::OK);
}

#[tokio::test]
async fn agent_discovery_uses_runtime_catalog_and_rejects_empty_intent() {
    let harness = GatewayTestHarness::postgres().expect("test harness");

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
