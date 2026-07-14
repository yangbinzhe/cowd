use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

#[tokio::test]
async fn evaluation_policy_promotion_stays_pending_until_the_typed_human_decision() {
    let harness = GatewayTestHarness::in_memory().expect("test harness");
    let request = serde_json::json!({
        "request_id": "gateway-policy-review-1",
        "next_policy": {
            "policy_id": "workspace/default-evaluation-policy",
            "revision": 2,
            "minimum_samples": 12,
            "minimum_confidence_basis_points": 9200,
            "require_fail_closed_for_protected_metrics": true,
            "require_protected_hard_gate": true,
            "require_target_improvement": true
        },
        "evidence_refs": ["evaluation:gateway-black-box"]
    });
    let created = harness
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/evolution/evaluation-policy/reviews")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("policy review request"),
        )
        .await
        .expect("policy review response");
    assert_eq!(created.status(), StatusCode::OK);
    let created: serde_json::Value = serde_json::from_slice(
        &to_bytes(created.into_body(), usize::MAX)
            .await
            .expect("policy review body"),
    )
    .expect("policy review JSON");
    assert_eq!(created["kind"], "evolution.evaluation_policy_review");
    assert_eq!(created["owner"], "runtime");
    assert_eq!(created["review"]["status"], "pending");
    let review_id = created["review"]["review_id"]
        .as_str()
        .expect("runtime generated review id");

    let decision = harness
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/api/evolution/evaluation-policy/reviews/{review_id}/decision"
                ))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"decision": "approve", "reason": "black-box approval"})
                        .to_string(),
                ))
                .expect("policy decision request"),
        )
        .await
        .expect("policy decision response");
    assert_eq!(decision.status(), StatusCode::OK);
    let decision: serde_json::Value = serde_json::from_slice(
        &to_bytes(decision.into_body(), usize::MAX)
            .await
            .expect("policy decision body"),
    )
    .expect("policy decision JSON");
    assert_eq!(decision["kind"], "evolution.evaluation_policy_decision");
    assert_eq!(decision["owner"], "runtime");
    assert_eq!(decision["policy"]["revision"], 2);
    assert_eq!(decision["policy"]["minimum_samples"], 12);
}
