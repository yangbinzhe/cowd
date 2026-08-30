#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use model_protocol::provider_failure::ProviderFailureScope;
use provider::ApiError;
use reqwest::StatusCode;

#[test]
fn provider_failure_classification_distinguishes_auth_rate_limit_internal_and_context() {
    let auth = ApiError::Api {
        status: StatusCode::UNAUTHORIZED,
        error_type: Some("invalid_api_key".to_string()),
        message: Some("invalid key".to_string()),
        request_id: Some("req-auth".to_string()),
        body: "{}".to_string(),
        retryable: false,
        retry_after: None,
        suggested_action: ApiError::suggested_action_for_status(StatusCode::UNAUTHORIZED),
    };
    assert_eq!(auth.safe_failure_class(), "provider_auth");
    assert!(!auth.is_retryable());
    assert_eq!(auth.request_id(), Some("req-auth"));

    let rate_limit = ApiError::Api {
        status: StatusCode::TOO_MANY_REQUESTS,
        error_type: Some("rate_limit".to_string()),
        message: Some("too many requests".to_string()),
        request_id: Some("req-429".to_string()),
        body: "{}".to_string(),
        retryable: true,
        retry_after: None,
        suggested_action: ApiError::suggested_action_for_status(StatusCode::TOO_MANY_REQUESTS),
    };
    assert_eq!(rate_limit.safe_failure_class(), "provider_rate_limit");
    assert!(rate_limit.is_retryable());

    let internal = ApiError::Api {
        status: StatusCode::BAD_GATEWAY,
        error_type: Some("bad_gateway".to_string()),
        message: Some("upstream unavailable".to_string()),
        request_id: None,
        body: "{}".to_string(),
        retryable: true,
        retry_after: None,
        suggested_action: ApiError::suggested_action_for_status(StatusCode::BAD_GATEWAY),
    };
    assert_eq!(internal.safe_failure_class(), "provider_error");
    assert!(internal.is_retryable());

    let context = ApiError::Api {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        error_type: Some("context_length_exceeded".to_string()),
        message: Some("maximum context length exceeded".to_string()),
        request_id: None,
        body: "{}".to_string(),
        retryable: false,
        retry_after: None,
        suggested_action: None,
    };
    assert_eq!(context.safe_failure_class(), "context_window");
    assert!(context.is_context_window_failure());
}

#[test]
fn retry_exhaustion_preserves_underlying_failure_class() {
    let exhausted = ApiError::RetriesExhausted {
        attempts: 3,
        last_error: Box::new(ApiError::Api {
            status: StatusCode::TOO_MANY_REQUESTS,
            error_type: Some("rate_limit".to_string()),
            message: Some("too many requests".to_string()),
            request_id: Some("req-last".to_string()),
            body: "{}".to_string(),
            retryable: true,
            retry_after: None,
            suggested_action: ApiError::suggested_action_for_status(StatusCode::TOO_MANY_REQUESTS),
        }),
    };

    assert_eq!(exhausted.safe_failure_class(), "provider_rate_limit");
    assert!(exhausted.is_retryable());
    assert_eq!(exhausted.request_id(), Some("req-last"));
}

#[test]
fn account_exhaustion_is_distinct_from_transient_rate_limit() {
    let deepseek_balance = ApiError::Api {
        status: StatusCode::PAYMENT_REQUIRED,
        error_type: Some("invalid_request_error".to_string()),
        message: Some("Insufficient Balance".to_string()),
        request_id: Some("req-deepseek-402".to_string()),
        body: r#"{"error":{"message":"Insufficient Balance"}}"#.to_string(),
        retryable: false,
        retry_after: None,
        suggested_action: ApiError::suggested_action_for_status(StatusCode::PAYMENT_REQUIRED),
    };
    assert_eq!(
        deepseek_balance.failure_scope(),
        ProviderFailureScope::Account
    );
    assert_eq!(deepseek_balance.safe_failure_class(), "provider_quota");
    assert!(!deepseek_balance.is_retryable());

    let token_plan_exhausted = ApiError::Api {
        status: StatusCode::TOO_MANY_REQUESTS,
        error_type: Some("insufficient_quota".to_string()),
        message: Some(
            "Your plan has been exhausted. It will reset at 2026-09-04T23:54:00Z".to_string(),
        ),
        request_id: Some("req-token-plan-429".to_string()),
        body: "{}".to_string(),
        retryable: true,
        retry_after: None,
        suggested_action: ApiError::suggested_action_for_status(StatusCode::TOO_MANY_REQUESTS),
    };
    assert_eq!(
        token_plan_exhausted.failure_scope(),
        ProviderFailureScope::Account
    );
    assert_eq!(token_plan_exhausted.safe_failure_class(), "provider_quota");
    assert!(!token_plan_exhausted.is_retryable());

    let transient_rate_limit = ApiError::Api {
        status: StatusCode::TOO_MANY_REQUESTS,
        error_type: Some("rate_limit".to_string()),
        message: Some("too many requests per second".to_string()),
        request_id: Some("req-transient-429".to_string()),
        body: "{}".to_string(),
        retryable: true,
        retry_after: None,
        suggested_action: ApiError::suggested_action_for_status(StatusCode::TOO_MANY_REQUESTS),
    };
    assert_eq!(
        transient_rate_limit.failure_scope(),
        ProviderFailureScope::Request
    );
    assert_eq!(
        transient_rate_limit.safe_failure_class(),
        "provider_rate_limit"
    );
    assert!(transient_rate_limit.is_retryable());
}

#[test]
fn paid_provider_replay_manifest_covers_every_observed_failure_family() {
    let manifest: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/test-governance/provider-failure-replay-v0.9.711.json"
    ))
    .expect("provider replay manifest JSON");
    assert_eq!(
        manifest["policy"]["offline_replay_required_before_paid_provider"],
        true
    );
    assert_eq!(
        manifest["policy"]["native_bailian_generation_allowed"],
        false
    );
    let ids = manifest["cases"]
        .as_array()
        .expect("replay cases")
        .iter()
        .filter_map(|case| case["id"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        "deepseek_http_402_insufficient_balance",
        "token_plan_http_429_plan_exhausted",
        "deepseek_reasoning_tool_continuation",
        "accepted_program_fresh_id_replan",
        "deepseek_scale_heading_semantic_split",
        "stale_gateway_binary_candidate_mismatch",
        "embedded_json_hijacked_terminal_contract",
    ] {
        assert!(ids.contains(required), "missing replay family {required}");
    }
}
