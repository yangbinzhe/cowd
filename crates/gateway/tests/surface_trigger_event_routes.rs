use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn trigger_event_retry_is_surface_scoped_and_only_revives_dead_letters() {
    let harness = GatewayTestHarness::in_memory().expect("test harness");
    let key = harness
        .seed_dead_letter_trigger_event("feishu", "surface-dead-letter-1")
        .expect("dead-letter fixture");

    let listed = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/surfaces/feishu/trigger-events")
                .body(Body::empty())
                .expect("trigger-event list request"),
        )
        .await
        .expect("trigger-event list response");
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: serde_json::Value = serde_json::from_slice(
        &to_bytes(listed.into_body(), usize::MAX)
            .await
            .expect("trigger-event list body"),
    )
    .expect("trigger-event list JSON");
    assert_eq!(listed["kind"], "surface.trigger_events");
    assert_eq!(listed["surface"], "feishu");
    assert_eq!(listed["events"][0]["idempotency_key"], key);
    assert_eq!(listed["events"][0]["status"], "dead_letter");

    let cross_surface = harness
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/surfaces/webui/trigger-events/retry")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"idempotency_key": key}).to_string(),
                ))
                .expect("cross-surface retry request"),
        )
        .await
        .expect("cross-surface retry response");
    assert_eq!(cross_surface.status(), StatusCode::CONFLICT);

    let retried = harness
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/surfaces/feishu/trigger-events/retry")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"idempotency_key": key}).to_string(),
                ))
                .expect("same-surface retry request"),
        )
        .await
        .expect("same-surface retry response");
    assert_eq!(retried.status(), StatusCode::OK);
    let retried: serde_json::Value = serde_json::from_slice(
        &to_bytes(retried.into_body(), usize::MAX)
            .await
            .expect("same-surface retry body"),
    )
    .expect("same-surface retry JSON");
    assert_eq!(retried["kind"], "surface.trigger_event.retry_accepted");
    assert_eq!(retried["surface"], "feishu");
    assert_eq!(retried["event"]["status"], "received");
    assert_eq!(retried["event"]["attempts"], 0);
}
