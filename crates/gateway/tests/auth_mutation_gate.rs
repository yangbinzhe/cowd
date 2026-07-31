#![allow(clippy::expect_used, clippy::unwrap_used)]

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use gateway::test_support::GatewayTestHarness;
use tower::ServiceExt;

/// Gateway derives the actor from its authentication middleware.  A caller
/// must not be able to smuggle an actor/principal into a mutating payload and
/// have the route treat it as an authenticated identity.
#[tokio::test]
async fn protected_mutation_rejects_a_forged_actor_payload() {
    let harness = GatewayTestHarness::in_memory().expect("gateway test harness");
    let response = harness
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sessions/session-v0-auth/attach")
                .header("content-type", "application/json")
                .header("x-cowd-principal", "agent:forged")
                .body(Body::from(
                    r#"{"surface":"webui","actor_id":"agent:forged"}"#,
                ))
                .expect("forged actor request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "the route must reject caller-supplied actor fields instead of trusting them"
    );
}

#[tokio::test]
async fn protected_routes_require_the_configured_bearer_credential() {
    let harness = GatewayTestHarness::in_memory_with_auth_token("gateway-test-token")
        .expect("authenticated gateway test harness");

    for authorization in [None, Some("Bearer wrong-token")] {
        let mut request = Request::builder()
            .uri("/api/runtime/managed-agents")
            .body(Body::empty())
            .expect("protected request");
        if let Some(value) = authorization {
            request.headers_mut().insert(
                "authorization",
                value.parse().expect("valid authorization header"),
            );
        }
        let response = harness
            .router()
            .oneshot(request)
            .await
            .expect("gateway response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let accepted = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/runtime/managed-agents")
                .header("authorization", "Bearer gateway-test-token")
                .body(Body::empty())
                .expect("authenticated request"),
        )
        .await
        .expect("gateway response");
    assert_eq!(accepted.status(), StatusCode::OK);
}

#[tokio::test]
async fn lifecycle_keeps_each_authenticated_surface_attachment_distinct() {
    let harness = GatewayTestHarness::in_memory_with_auth_token("gateway-test-token")
        .expect("authenticated gateway test harness");
    let app = harness.router();

    let ensured = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sessions/session-surface-identity/ensure")
                .header("authorization", "Bearer gateway-test-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"test-model"}"#))
                .expect("ensure request"),
        )
        .await
        .expect("ensure response");
    assert_eq!(ensured.status(), StatusCode::OK);

    for (surface, observer_id, role) in [
        ("tui", "tui:auth-gate", "writer"),
        ("webui", "webui:auth-gate", "reader"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-surface-identity/attach")
                    .header("authorization", "Bearer gateway-test-token")
                    .header("content-type", "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(format!(
                        r#"{{"surface":"{surface}","role":"{role}"}}"#
                    )))
                    .expect("attach request"),
            )
            .await
            .expect("attach response");
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("attach body"),
        )
        .expect("attach json");
        assert_eq!(body["ok"], true);
    }

    let lifecycle = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/sessions/session-surface-identity/lifecycle")
                .header("authorization", "Bearer gateway-test-token")
                .body(Body::empty())
                .expect("lifecycle request"),
        )
        .await
        .expect("lifecycle response");
    assert_eq!(lifecycle.status(), StatusCode::OK);
    let lifecycle: serde_json::Value = serde_json::from_slice(
        &to_bytes(lifecycle.into_body(), usize::MAX)
            .await
            .expect("lifecycle body"),
    )
    .expect("lifecycle json");
    let attachments = lifecycle["snapshot"]["attachments"]
        .as_array()
        .expect("attachments array");
    assert_eq!(attachments.len(), 2);
    assert!(attachments.iter().any(|attachment| {
        attachment.pointer("/actor/surface") == Some(&serde_json::json!("tui"))
            && attachment.pointer("/actor/id")
                == Some(&serde_json::json!(
                    "principal:local-human:surface:tui:auth-gate"
                ))
    }));
    assert!(attachments.iter().any(|attachment| {
        attachment.pointer("/actor/surface") == Some(&serde_json::json!("webui"))
            && attachment.pointer("/actor/id")
                == Some(&serde_json::json!(
                    "principal:local-human:surface:webui:auth-gate"
                ))
    }));

    let detached = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sessions/session-surface-identity/detach")
                .header("authorization", "Bearer gateway-test-token")
                .header("content-type", "application/json")
                .header("x-cowd-observer-id", "tui:auth-gate")
                .body(Body::from(r#"{"surface":"tui"}"#))
                .expect("detach request"),
        )
        .await
        .expect("detach response");
    assert_eq!(detached.status(), StatusCode::OK);
    let detached: serde_json::Value = serde_json::from_slice(
        &to_bytes(detached.into_body(), usize::MAX)
            .await
            .expect("detach body"),
    )
    .expect("detach json");
    assert_eq!(detached["snapshot"]["state"], "attached");
    let attachments = detached["snapshot"]["attachments"]
        .as_array()
        .expect("attachments array");
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0]["actor"]["surface"], "webui");
}

#[tokio::test]
async fn every_session_mutation_route_fails_closed_without_a_writer_observer() {
    let harness = GatewayTestHarness::in_memory().expect("gateway test harness");
    let app = harness.router();
    let session_id = "writer-route-contract";
    let ensured = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/sessions/{session_id}/ensure"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"test-model"}"#))
                .expect("ensure request"),
        )
        .await
        .expect("ensure response");
    assert_eq!(ensured.status(), StatusCode::OK);

    for (path, body) in [
        (
            format!("/api/sessions/{session_id}/messages"),
            r#"{"content":"must not run"}"#,
        ),
        (
            format!("/api/sessions/{session_id}/inputs/input-1/cancel"),
            r#"{"reason":"must not run"}"#,
        ),
        (
            format!("/api/sessions/{session_id}/inputs/input-1/reclassify"),
            r#"{"decision":"enqueue_next_step","reason":"must not run"}"#,
        ),
        (
            format!("/api/sessions/{session_id}/cancel"),
            r#"{"reason":"must not run"}"#,
        ),
        (format!("/api/sessions/{session_id}/compact"), "{}"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&path)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("session mutation request"),
            )
            .await
            .expect("session mutation response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }

    let read_only_slash = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/slash/dispatch")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"command":"help","args":{}}"#))
                .expect("read-only slash request"),
        )
        .await
        .expect("read-only slash response");
    assert_ne!(read_only_slash.status(), StatusCode::FORBIDDEN);

    let mutating_slash = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/slash/dispatch")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"command":"compact","args":{{"session_id":"{session_id}"}}}}"#
                )))
                .expect("mutating slash request"),
        )
        .await
        .expect("mutating slash response");
    assert_eq!(mutating_slash.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn task_slash_dispatch_executes_the_gateway_owned_task_service() {
    let harness = GatewayTestHarness::in_memory().expect("gateway test harness");
    let app = harness.router();
    let session_id = "task-slash-auth-session";
    let observer_id = "tui:task-slash-auth";
    let ensured = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/sessions/{session_id}/ensure"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"test-model"}"#))
                .expect("task slash session ensure request"),
        )
        .await
        .expect("task slash session ensure response");
    assert_eq!(ensured.status(), StatusCode::OK);
    let attached = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/sessions/{session_id}/attach"))
                .header("content-type", "application/json")
                .header("x-cowd-observer-id", observer_id)
                .body(Body::from(r#"{"surface":"tui","role":"writer"}"#))
                .expect("task slash writer attach request"),
        )
        .await
        .expect("task slash writer attach response");
    assert_eq!(attached.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/slash/dispatch")
                .header("content-type", "application/json")
                .header("x-cowd-observer-id", observer_id)
                .body(Body::from(
                    serde_json::json!({
                        "command": "tasks",
                        "args": {
                            "input": "/tasks start --yolo prove task slash wiring",
                            "surface": "tui",
                            "session_id": session_id,
                        }
                    })
                    .to_string(),
                ))
                .expect("task slash request"),
        )
        .await
        .expect("task slash response");
    let status = response.status();
    let body: serde_json::Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("task slash body"),
    )
    .expect("task slash json");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["status"], "complete");
    assert_eq!(body["data"]["dispatch"], "task_service");
    assert_eq!(body["data"]["operation"], "start");
    assert_eq!(body["data"]["task"]["objective"], "prove task slash wiring");
    assert_eq!(body["data"]["task"]["execution_policy"]["yolo_mode"], true);
}

#[tokio::test]
async fn webui_login_uses_a_one_day_http_only_cookie_not_a_browser_token() {
    let harness = GatewayTestHarness::in_memory_with_auth_token("gateway-test-token")
        .expect("authenticated gateway test harness");
    let login = harness
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"token":"gateway-test-token"}"#))
                .expect("login request"),
        )
        .await
        .expect("login response");
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login
        .headers()
        .get("set-cookie")
        .expect("browser session cookie")
        .to_str()
        .expect("valid cookie")
        .to_string();
    assert!(cookie.contains("cowd_web_session="));
    assert!(cookie.contains("HttpOnly"));
    assert!(cookie.contains("Max-Age=86400"));
    assert!(!cookie.contains("gateway-test-token"));
    let body = to_bytes(login.into_body(), usize::MAX)
        .await
        .expect("login body");
    assert!(!String::from_utf8_lossy(&body).contains("gateway-test-token"));
    let login_body: serde_json::Value = serde_json::from_slice(&body).expect("login response JSON");
    assert_eq!(login_body["expires_in_seconds"], 86_400);

    let browser_session = cookie.split(';').next().expect("cookie pair");
    let accepted = harness
        .router()
        .oneshot(
            Request::builder()
                .uri("/api/runtime/managed-agents")
                .header("cookie", browser_session)
                .body(Body::empty())
                .expect("browser request"),
        )
        .await
        .expect("browser response");
    assert_eq!(accepted.status(), StatusCode::OK);
}

#[tokio::test]
async fn cross_plane_and_connector_routes_reject_payload_principals_and_inject_the_verified_actor()
{
    let harness = GatewayTestHarness::in_memory().expect("gateway test harness");

    let forged_connector = harness
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connectors/services/local.docs/execute")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"actor_principal":"user:forged","tool_id":"service.local.docs.read","resource_id":"doc","title":"Forged","mode":"dry_run"}"#,
                ))
                .expect("connector request"),
        )
        .await
        .expect("gateway response");
    assert_eq!(forged_connector.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let forged_action = harness
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/cross-plane/action/preflight")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"actor_principal":"user:forged","requested_capability":"service.local.docs.read"}"#,
                ))
                .expect("cross-plane request"),
        )
        .await
        .expect("gateway response");
    assert_eq!(forged_action.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let accepted = harness
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/cross-plane/action/preflight")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"requested_capability":"service.local.docs.read","risk":"low","data_classification":"internal","identity_trust":"unknown"}"#,
                ))
                .expect("trusted intent request"),
        )
        .await
        .expect("gateway response");
    assert_eq!(accepted.status(), StatusCode::OK);
    let body = to_bytes(accepted.into_body(), usize::MAX)
        .await
        .expect("response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
    assert_eq!(
        payload["action"]["actor_principal"],
        "principal:local-human"
    );
}
