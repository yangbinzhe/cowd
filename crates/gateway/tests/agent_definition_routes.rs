use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn runtime_backed_agent_catalog_and_directory_are_exposed_by_the_real_router() {
    let harness = GatewayTestHarness::in_memory().expect("test harness");

    let catalog = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/agents/catalog")
                .body(Body::empty())
                .expect("catalog request"),
        )
        .await
        .expect("catalog response");
    assert_eq!(catalog.status(), StatusCode::OK);
    let catalog: serde_json::Value = serde_json::from_slice(
        &to_bytes(catalog.into_body(), usize::MAX)
            .await
            .expect("catalog body"),
    )
    .expect("catalog JSON");
    assert_eq!(catalog["kind"], "agents");
    assert_eq!(catalog["source"], "runtime.definition_catalog");
    assert!(catalog["agents"].is_array());
    assert!(catalog["summary"]["total"].is_u64());

    let directory = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/agents/directory")
                .body(Body::empty())
                .expect("directory request"),
        )
        .await
        .expect("directory response");
    assert_eq!(directory.status(), StatusCode::OK);
    let directory: serde_json::Value = serde_json::from_slice(
        &to_bytes(directory.into_body(), usize::MAX)
            .await
            .expect("directory body"),
    )
    .expect("directory JSON");
    assert_eq!(directory["kind"], "agents.directory");
    assert_eq!(directory["source"], "runtime.definition_catalog");
    assert!(directory["agents"].is_array());
}
