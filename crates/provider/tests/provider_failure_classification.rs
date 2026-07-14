#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

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
            suggested_action: ApiError::suggested_action_for_status(StatusCode::TOO_MANY_REQUESTS),
        }),
    };

    assert_eq!(exhausted.safe_failure_class(), "provider_rate_limit");
    assert!(exhausted.is_retryable());
    assert_eq!(exhausted.request_id(), Some("req-last"));
}
