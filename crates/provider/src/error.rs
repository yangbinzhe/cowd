use std::env::VarError;
use std::fmt::{Display, Formatter};
use std::time::Duration;

const GENERIC_FATAL_WRAPPER_MARKERS: &[&str] = &[
    "something went wrong while processing your request",
    "please try again, or use /new to start a fresh session",
];

const CONTEXT_WINDOW_ERROR_MARKERS: &[&str] = &[
    "maximum context length",
    "context window",
    "context length",
    "too many tokens",
    "prompt is too long",
    "input is too long",
    "request is too large",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityToolProtocolFailure {
    MalformedFrame,
    FrameTooLarge,
}

impl CompatibilityToolProtocolFailure {
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::MalformedFrame => "malformed compatibility tool-call frame",
            Self::FrameTooLarge => "compatibility tool-call frame exceeds byte limit",
        }
    }
}

#[derive(Debug)]
pub enum ApiError {
    MissingCredentials {
        provider: &'static str,
        env_vars: &'static [&'static str],
        /// Optional, runtime-computed hint appended to the error Display
        /// output. Populated when the provider resolver can infer what the
        /// user probably intended (e.g. an `OpenAI` key is set but Anthropic
        /// was selected because no Anthropic credentials exist).
        hint: Option<String>,
    },
    ContextWindowExceeded {
        model: String,
        estimated_input_tokens: u32,
        requested_output_tokens: u32,
        estimated_total_tokens: u32,
        context_window_tokens: u32,
    },
    ExpiredOAuthToken,
    Auth(String),
    InvalidApiKeyEnv(VarError),
    Http(reqwest::Error),
    Io(std::io::Error),
    Json {
        provider: String,
        model: String,
        body_snippet: String,
        source: serde_json::Error,
    },
    Api {
        status: reqwest::StatusCode,
        error_type: Option<String>,
        message: Option<String>,
        request_id: Option<String>,
        body: String,
        retryable: bool,
        /// Suggested user-facing action to resolve the error, if known.
        suggested_action: Option<String>,
    },
    RequestBodyTooLarge {
        model: String,
        estimated_bytes: usize,
        limit_bytes: usize,
    },
    RetriesExhausted {
        attempts: u32,
        last_error: Box<ApiError>,
    },
    InvalidSseFrame(&'static str),
    CompatibilityToolProtocol(CompatibilityToolProtocolFailure),
    BackoffOverflow {
        attempt: u32,
        base_delay: Duration,
    },
    NoProviderConfigured {
        model: String,
    },

    InvalidProviderConfig {
        provider: String,
        reason: String,
    },
}

impl ApiError {
    /// Whether this failure represents a provider-emitted compatibility tool
    /// frame that was recognisable as protocol data but invalid. Runtime uses
    /// this typed classification to avoid multiplying the same bad frame
    /// across fallback models.
    #[must_use]
    pub fn is_compatibility_tool_protocol_failure(&self) -> bool {
        match self {
            Self::CompatibilityToolProtocol(_) => true,
            Self::RetriesExhausted { last_error, .. } => {
                last_error.is_compatibility_tool_protocol_failure()
            }
            _ => false,
        }
    }

    #[must_use]
    pub const fn missing_credentials(
        provider: &'static str,
        env_vars: &'static [&'static str],
    ) -> Self {
        Self::MissingCredentials {
            provider,
            env_vars,
            hint: None,
        }
    }

    /// Build a `MissingCredentials` error carrying an extra, runtime-computed
    /// hint string that the Display impl appends after the canonical "missing
    /// <provider> credentials" message. Used by the provider resolver to
    /// suggest the likely fix when the user has credentials for a different
    /// provider already in the environment.
    #[must_use]
    pub fn missing_credentials_with_hint(
        provider: &'static str,
        env_vars: &'static [&'static str],
        hint: impl Into<String>,
    ) -> Self {
        Self::MissingCredentials {
            provider,
            env_vars,
            hint: Some(hint.into()),
        }
    }

    /// Build a `Self::Json` enriched with the provider name, the model that
    /// was requested, and the first 200 characters of the raw response body so
    /// that callers can diagnose deserialization failures without re-running
    /// the request.
    #[must_use]
    pub fn json_deserialize(
        provider: impl Into<String>,
        model: impl Into<String>,
        body: &str,
        source: serde_json::Error,
    ) -> Self {
        Self::Json {
            provider: provider.into(),
            model: model.into(),
            body_snippet: truncate_body_snippet(body, 200),
            source,
        }
    }

    /// Return a human-readable suggested action for common HTTP status codes.
    #[must_use]
    pub fn suggested_action_for_status(status: reqwest::StatusCode) -> Option<String> {
        match status.as_u16() {
            401 | 403 => Some("check your API key and permissions".to_string()),
            402 => Some(
                "top up the provider balance or switch to a model on another configured provider"
                    .to_string(),
            ),
            429 => Some("wait a moment and retry, or switch to a different model".to_string()),
            500 | 502 | 503 => {
                Some("the provider is experiencing issues; retry after a brief wait".to_string())
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http(error) => error.is_connect() || error.is_timeout() || error.is_request(),
            Self::Api { retryable, .. } => *retryable,
            Self::RetriesExhausted { last_error, .. } => last_error.is_retryable(),
            Self::MissingCredentials { .. }
            | Self::ContextWindowExceeded { .. }
            | Self::RequestBodyTooLarge { .. }
            | Self::ExpiredOAuthToken
            | Self::Auth(_)
            | Self::InvalidApiKeyEnv(_)
            | Self::Io(_)
            | Self::Json { .. }
            | Self::InvalidSseFrame(_)
            | Self::CompatibilityToolProtocol(_)
            | Self::BackoffOverflow { .. }
            | Self::NoProviderConfigured { .. }
            | Self::InvalidProviderConfig { .. } => false,
        }
    }

    /// Typed capacity signal for the runtime resource controller.
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Http(error) => error.is_timeout(),
            Self::Io(error) => error.kind() == std::io::ErrorKind::TimedOut,
            Self::RetriesExhausted { last_error, .. } => last_error.is_timeout(),
            _ => false,
        }
    }

    /// Provider-declared saturation, distinct from generic transport failure.
    #[must_use]
    pub fn is_downstream_overload(&self) -> bool {
        match self {
            Self::Api { status, .. } => matches!(status.as_u16(), 429 | 502 | 503 | 504),
            Self::RetriesExhausted { last_error, .. } => last_error.is_downstream_overload(),
            _ => false,
        }
    }

    #[must_use]
    pub fn is_context_window_failure(&self) -> bool {
        match self {
            Self::ContextWindowExceeded { .. } => true,
            Self::Api {
                status,
                message,
                body,
                ..
            } => {
                matches!(status.as_u16(), 400 | 413 | 422)
                    && (message
                        .as_deref()
                        .is_some_and(looks_like_context_window_error)
                        || looks_like_context_window_error(body))
            }
            Self::RetriesExhausted { last_error, .. } => last_error.is_context_window_failure(),
            Self::MissingCredentials { .. }
            | Self::RequestBodyTooLarge { .. }
            | Self::ExpiredOAuthToken
            | Self::Auth(_)
            | Self::InvalidApiKeyEnv(_)
            | Self::Http(_)
            | Self::Io(_)
            | Self::Json { .. }
            | Self::InvalidSseFrame(_)
            | Self::CompatibilityToolProtocol(_)
            | Self::BackoffOverflow { .. }
            | Self::NoProviderConfigured { .. }
            | Self::InvalidProviderConfig { .. } => false,
        }
    }

    /// Return a provider-declared context window only when the error exposes a
    /// concrete numeric limit. Runtime uses this to tighten an over-large
    /// configured/assumed window; generic context errors never calibrate it.
    #[must_use]
    pub fn context_window_limit_hint(&self) -> Option<u32> {
        match self {
            Self::ContextWindowExceeded {
                context_window_tokens,
                ..
            } => Some(*context_window_tokens),
            Self::Api { message, body, .. } => message
                .as_deref()
                .and_then(parse_context_window_limit)
                .or_else(|| parse_context_window_limit(body)),
            Self::RetriesExhausted { last_error, .. } => last_error.context_window_limit_hint(),
            Self::MissingCredentials { .. }
            | Self::ExpiredOAuthToken
            | Self::Auth(_)
            | Self::InvalidApiKeyEnv(_)
            | Self::Http(_)
            | Self::Io(_)
            | Self::Json { .. }
            | Self::RequestBodyTooLarge { .. }
            | Self::InvalidSseFrame(_)
            | Self::CompatibilityToolProtocol(_)
            | Self::BackoffOverflow { .. }
            | Self::NoProviderConfigured { .. }
            | Self::InvalidProviderConfig { .. } => None,
        }
    }

    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Api { request_id, .. } => request_id.as_deref(),
            Self::RetriesExhausted { last_error, .. } => last_error.request_id(),
            Self::MissingCredentials { .. }
            | Self::ContextWindowExceeded { .. }
            | Self::RequestBodyTooLarge { .. }
            | Self::ExpiredOAuthToken
            | Self::Auth(_)
            | Self::InvalidApiKeyEnv(_)
            | Self::Http(_)
            | Self::Io(_)
            | Self::Json { .. }
            | Self::InvalidSseFrame(_)
            | Self::CompatibilityToolProtocol(_)
            | Self::BackoffOverflow { .. }
            | Self::NoProviderConfigured { .. }
            | Self::InvalidProviderConfig { .. } => None,
        }
    }

    #[must_use]
    pub fn safe_failure_class(&self) -> &'static str {
        match self {
            Self::RetriesExhausted { .. } if self.is_context_window_failure() => "context_window",
            Self::RetriesExhausted { .. } if self.is_generic_fatal_wrapper() => {
                "provider_retry_exhausted"
            }
            Self::RetriesExhausted { last_error, .. } => last_error.safe_failure_class(),
            Self::MissingCredentials { .. } | Self::ExpiredOAuthToken | Self::Auth(_) => {
                "provider_auth"
            }
            Self::Api { status, .. } if matches!(status.as_u16(), 401 | 403) => "provider_auth",
            Self::ContextWindowExceeded { .. } => "context_window",
            Self::Api { .. } if self.is_context_window_failure() => "context_window",
            Self::Api { status, .. } if status.as_u16() == 429 => "provider_rate_limit",
            Self::Api { .. } if self.is_generic_fatal_wrapper() => "provider_internal",
            Self::Api { .. } => "provider_error",
            Self::Http(_)
            | Self::InvalidSseFrame(_)
            | Self::CompatibilityToolProtocol(_)
            | Self::BackoffOverflow { .. } => "provider_transport",
            Self::RequestBodyTooLarge { .. } => "request_too_large",
            Self::InvalidApiKeyEnv(_) | Self::Io(_) | Self::Json { .. } => "runtime_io",
            Self::NoProviderConfigured { .. } => "provider_auth",
            Self::InvalidProviderConfig { .. } => "runtime_io",
        }
    }

    #[must_use]
    pub fn is_generic_fatal_wrapper(&self) -> bool {
        match self {
            Self::Api { message, body, .. } => {
                message
                    .as_deref()
                    .is_some_and(looks_like_generic_fatal_wrapper)
                    || looks_like_generic_fatal_wrapper(body)
            }
            Self::RetriesExhausted { last_error, .. } => last_error.is_generic_fatal_wrapper(),
            Self::MissingCredentials { .. }
            | Self::ContextWindowExceeded { .. }
            | Self::RequestBodyTooLarge { .. }
            | Self::ExpiredOAuthToken
            | Self::Auth(_)
            | Self::InvalidApiKeyEnv(_)
            | Self::Http(_)
            | Self::Io(_)
            | Self::Json { .. }
            | Self::InvalidSseFrame(_)
            | Self::CompatibilityToolProtocol(_)
            | Self::BackoffOverflow { .. }
            | Self::NoProviderConfigured { .. }
            | Self::InvalidProviderConfig { .. } => false,
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCredentials {
                provider,
                env_vars,
                hint,
            } => {
                write!(
                    f,
                    "missing {provider} credentials; export {} before calling the {provider} API",
                    env_vars.join(" or ")
                )?;
                if cfg!(target_os = "windows") {
                    if let Some(primary) = env_vars.first() {
                        write!(
                            f,
                            " (on Windows, environment variables set in PowerShell only persist for the current session; use `setx {primary} <value>` to make it permanent, then open a new terminal, or place a `.env` file containing `{primary}=<value>` in the current working directory)"
                        )?;
                    } else {
                        write!(
                            f,
                            " (on Windows, environment variables set in PowerShell only persist for the current session; use `setx` to make them permanent, then open a new terminal, or place a `.env` file in the current working directory)"
                        )?;
                    }
                }
                if let Some(hint) = hint {
                    write!(f, " — hint: {hint}")?;
                }
                Ok(())
            }
            Self::ContextWindowExceeded {
                model,
                estimated_input_tokens,
                requested_output_tokens,
                estimated_total_tokens,
                context_window_tokens,
            } => write!(
                f,
                "context_window_blocked for {model}: estimated input {estimated_input_tokens} + requested output {requested_output_tokens} = {estimated_total_tokens} tokens exceeds the {context_window_tokens}-token context window; compact the session or reduce request size before retrying"
            ),
            Self::ExpiredOAuthToken => {
                write!(
                    f,
                    "saved OAuth token is expired and no refresh token is available"
                )
            }
            Self::Auth(message) => write!(f, "auth error: {message}"),
            Self::RequestBodyTooLarge {
                model,
                estimated_bytes,
                limit_bytes,
            } => write!(
                f,
                "request_body_too_large for {model}: estimated {estimated_bytes} bytes exceeds the {limit_bytes}-byte limit; reduce request size before retrying"
            ),
            Self::InvalidApiKeyEnv(error) => {
                write!(f, "failed to read credential environment variable: {error}")
            }
            Self::Http(error) => write!(f, "http error: {error}"),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Json {
                provider,
                model,
                body_snippet,
                source,
            } => write!(
                f,
                "failed to parse {provider} response for model {model}: {source}; first 200 chars of body: {body_snippet}"
            ),
            Self::Api {
                status,
                error_type,
                message,
                request_id,
                body,
                ..
            } => {
                if let (Some(error_type), Some(message)) = (error_type, message) {
                    write!(f, "api returned {status} ({error_type})")?;
                    if let Some(request_id) = request_id {
                        write!(f, " [trace {request_id}]")?;
                    }
                    write!(f, ": {message}")
                } else {
                    write!(f, "api returned {status}")?;
                    if let Some(request_id) = request_id {
                        write!(f, " [trace {request_id}]")?;
                    }
                    write!(f, ": {body}")
                }
            }
            Self::RetriesExhausted {
                attempts,
                last_error,
            } => write!(f, "api failed after {attempts} attempts: {last_error}"),
            Self::InvalidSseFrame(message) => write!(f, "invalid sse frame: {message}"),
            Self::CompatibilityToolProtocol(failure) => {
                write!(f, "invalid sse frame: {}", failure.message())
            }
            Self::BackoffOverflow {
                attempt,
                base_delay,
            } => write!(
                f,
                "retry backoff overflowed on attempt {attempt} with base delay {base_delay:?}"
            ),
            Self::NoProviderConfigured { model } => write!(
                f,
                "没有为模型 '{model}' 配置 provider。\n请在 ~/.cowd/config.yaml 的 providers 段添加配置，例如：\n  providers:\n    my_provider:\n      base_url: 'https://api.example.com/v1'\n      api_key: 'sk-...'\n      models: ['{model}']\n      protocol: 'completions'"
            ),
            Self::InvalidProviderConfig { provider, reason } => {
                write!(f, "invalid provider config for {provider}: {reason}")
            }
        }
    }
}

impl std::error::Error for ApiError {}

impl From<reqwest::Error> for ApiError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<std::io::Error> for ApiError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json {
            provider: "unknown".to_string(),
            model: "unknown".to_string(),
            body_snippet: String::new(),
            source: value,
        }
    }
}

impl From<VarError> for ApiError {
    fn from(value: VarError) -> Self {
        Self::InvalidApiKeyEnv(value)
    }
}

fn looks_like_generic_fatal_wrapper(text: &str) -> bool {
    let lowered = text.to_ascii_lowercase();
    GENERIC_FATAL_WRAPPER_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

fn looks_like_context_window_error(text: &str) -> bool {
    let lowered = text.to_ascii_lowercase();
    CONTEXT_WINDOW_ERROR_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

fn parse_context_window_limit(text: &str) -> Option<u32> {
    let lowered = text.to_ascii_lowercase();
    let marker_index = [
        "maximum context length",
        "maximum context window",
        "context window",
        "context length",
    ]
    .iter()
    .filter_map(|marker| lowered.find(marker).map(|index| (index, marker.len())))
    .map(|(index, marker_len)| index.saturating_add(marker_len))
    .min()?;
    let suffix = &lowered[marker_index..];
    let mut digits = String::new();
    let mut started = false;
    let mut kilo = false;
    for character in suffix.chars().take(96) {
        if character.is_ascii_digit() {
            started = true;
            digits.push(character);
            continue;
        }
        if !started {
            continue;
        }
        if started && matches!(character, ',' | '_' | ' ') {
            continue;
        }
        if started && character == 'k' {
            kilo = true;
            break;
        }
        if started {
            break;
        }
    }
    let parsed = digits.parse::<u64>().ok()?;
    let tokens = if kilo {
        parsed.saturating_mul(1_000)
    } else {
        parsed
    };
    (tokens >= 1_024 && tokens <= u64::from(u32::MAX)).then_some(tokens as u32)
}

/// Truncate `body` so the resulting snippet contains at most `max_chars`
/// characters (counted by Unicode scalar values, not bytes), preserving the
/// leading slice of the body that the caller most often needs to inspect.
fn truncate_body_snippet(body: &str, max_chars: usize) -> String {
    let mut taken_chars = 0;
    let mut byte_end = 0;
    for (offset, character) in body.char_indices() {
        if taken_chars >= max_chars {
            break;
        }
        taken_chars += 1;
        byte_end = offset + character.len_utf8();
    }
    if taken_chars >= max_chars && byte_end < body.len() {
        format!("{}…", &body[..byte_end])
    } else {
        body[..byte_end].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{truncate_body_snippet, ApiError, CompatibilityToolProtocolFailure};

    #[test]
    fn context_window_hint_requires_a_numeric_provider_limit() {
        let numeric = ApiError::Api {
            status: reqwest::StatusCode::BAD_REQUEST,
            error_type: None,
            message: Some("maximum context length is 16,385 tokens".to_string()),
            request_id: None,
            body: String::new(),
            retryable: false,
            suggested_action: None,
        };
        assert_eq!(numeric.context_window_limit_hint(), Some(16_385));

        let generic = ApiError::Api {
            status: reqwest::StatusCode::BAD_REQUEST,
            error_type: None,
            message: Some("context window exceeded; reduce the prompt".to_string()),
            request_id: None,
            body: String::new(),
            retryable: false,
            suggested_action: None,
        };
        assert_eq!(generic.context_window_limit_hint(), None);
    }

    #[test]
    fn json_deserialize_error_includes_provider_model_and_truncated_body_snippet() {
        let raw_body = format!("{}{}", "x".repeat(190), "_TAIL_PAST_200_CHARS_MARKER_");
        let source = serde_json::from_str::<serde_json::Value>("{not json")
            .expect_err("invalid json should fail to parse");

        let error = ApiError::json_deserialize("Anthropic", "claude-opus-4-6", &raw_body, source);
        let rendered = error.to_string();

        assert!(
            rendered.starts_with("failed to parse Anthropic response for model claude-opus-4-6: "),
            "rendered error should lead with provider and model: {rendered}"
        );
        assert!(
            rendered.contains("first 200 chars of body: "),
            "rendered error should label the body snippet: {rendered}"
        );
        let snippet = rendered
            .split("first 200 chars of body: ")
            .nth(1)
            .expect("snippet section should be present");
        assert!(
            snippet.starts_with(&"x".repeat(190)),
            "snippet should preserve the leading characters of the body: {snippet}"
        );
        assert!(
            snippet.ends_with('…'),
            "snippet should signal truncation with an ellipsis: {snippet}"
        );
        assert!(
            !snippet.contains("_TAIL_PAST_200_CHARS_MARKER_"),
            "snippet should drop characters past the 200-char cap: {snippet}"
        );
        assert_eq!(error.safe_failure_class(), "runtime_io");
        assert_eq!(error.request_id(), None);
        assert!(!error.is_retryable());
    }

    #[test]
    fn truncate_body_snippet_keeps_short_bodies_intact() {
        assert_eq!(truncate_body_snippet("hello", 200), "hello");
        assert_eq!(truncate_body_snippet("", 200), "");
    }

    #[test]
    fn truncate_body_snippet_caps_long_bodies_at_max_chars() {
        let body = "a".repeat(250);
        let snippet = truncate_body_snippet(&body, 200);
        assert_eq!(snippet.chars().count(), 201, "200 chars + ellipsis");
        assert!(snippet.ends_with('…'));
        assert!(snippet.starts_with(&"a".repeat(200)));
    }

    #[test]
    fn truncate_body_snippet_does_not_split_multibyte_characters() {
        let body = "한글한글한글한글한글한글";
        let snippet = truncate_body_snippet(body, 4);
        assert_eq!(snippet, "한글한글…");
    }

    #[test]
    fn detects_generic_fatal_wrapper_and_classifies_it_as_provider_internal() {
        let error = ApiError::Api {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            error_type: Some("api_error".to_string()),
            message: Some(
                "Something went wrong while processing your request. Please try again, or use /new to start a fresh session."
                    .to_string(),
            ),
            request_id: Some("req_jobdori_123".to_string()),
            body: String::new(),
            retryable: true,
            suggested_action: None,
        };

        assert!(error.is_generic_fatal_wrapper());
        assert_eq!(error.safe_failure_class(), "provider_internal");
        assert_eq!(error.request_id(), Some("req_jobdori_123"));
        assert!(error.to_string().contains("[trace req_jobdori_123]"));
    }

    #[test]
    fn retries_exhausted_preserves_nested_request_id_and_failure_class() {
        let error = ApiError::RetriesExhausted {
            attempts: 3,
            last_error: Box::new(ApiError::Api {
                status: reqwest::StatusCode::BAD_GATEWAY,
                error_type: Some("api_error".to_string()),
                message: Some(
                    "Something went wrong while processing your request. Please try again, or use /new to start a fresh session."
                        .to_string(),
                ),
                request_id: Some("req_nested_456".to_string()),
                body: String::new(),
                retryable: true,
                suggested_action: None,
            }),
        };

        assert!(error.is_generic_fatal_wrapper());
        assert_eq!(error.safe_failure_class(), "provider_retry_exhausted");
        assert_eq!(error.request_id(), Some("req_nested_456"));
    }

    #[test]
    fn resource_failures_keep_timeout_and_downstream_overload_distinct() {
        let overloaded = ApiError::Api {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            error_type: Some("rate_limit".to_string()),
            message: None,
            request_id: None,
            body: String::new(),
            retryable: true,
            suggested_action: None,
        };
        assert!(overloaded.is_downstream_overload());
        assert!(!overloaded.is_timeout());

        let timed_out = ApiError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "provider timed out",
        ));
        assert!(timed_out.is_timeout());
        assert!(!timed_out.is_downstream_overload());
    }

    #[test]
    fn payment_required_recommends_cross_provider_failover() {
        assert_eq!(
            ApiError::suggested_action_for_status(reqwest::StatusCode::PAYMENT_REQUIRED).as_deref(),
            Some("top up the provider balance or switch to a model on another configured provider")
        );
    }

    #[test]
    fn classifies_provider_context_window_errors() {
        let error = ApiError::Api {
            status: reqwest::StatusCode::BAD_REQUEST,
            error_type: Some("invalid_request_error".to_string()),
            message: Some(
                "This model's maximum context length is 200000 tokens, but your request used 230000 tokens."
                    .to_string(),
            ),
            request_id: Some("req_ctx_123".to_string()),
            body: String::new(),
            retryable: false,
            suggested_action: None,
        };

        assert!(error.is_context_window_failure());
        assert_eq!(error.safe_failure_class(), "context_window");
        assert_eq!(error.request_id(), Some("req_ctx_123"));
    }

    #[test]
    fn missing_credentials_without_hint_renders_the_canonical_message() {
        // given
        let error = ApiError::missing_credentials(
            "Anthropic",
            &["ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY"],
        );

        // when
        let rendered = error.to_string();

        // then
        assert!(
            rendered.starts_with(
                "missing Anthropic credentials; export ANTHROPIC_AUTH_TOKEN or ANTHROPIC_API_KEY before calling the Anthropic API"
            ),
            "rendered error should lead with the canonical missing-credential message: {rendered}"
        );
        assert!(
            !rendered.contains(" — hint: "),
            "no hint should be appended when none is supplied: {rendered}"
        );
    }

    #[test]
    fn missing_credentials_with_hint_appends_the_hint_after_base_message() {
        // given
        let error = ApiError::missing_credentials_with_hint(
            "Anthropic",
            &["ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY"],
            "I see OPENAI_API_KEY is set — if you meant to use the OpenAI-compat provider, prefix your model name with `openai/` so prefix routing selects it.",
        );

        // when
        let rendered = error.to_string();

        // then
        assert!(
            rendered.starts_with("missing Anthropic credentials;"),
            "hint should be appended, not replace the base message: {rendered}"
        );
        let hint_marker = " — hint: I see OPENAI_API_KEY is set — if you meant to use the OpenAI-compat provider, prefix your model name with `openai/` so prefix routing selects it.";
        assert!(
            rendered.ends_with(hint_marker),
            "rendered error should end with the hint: {rendered}"
        );
        // Classification semantics are unaffected by the presence of a hint.
        assert_eq!(error.safe_failure_class(), "provider_auth");
        assert!(!error.is_retryable());
        assert_eq!(error.request_id(), None);
    }

    #[test]
    fn compatibility_tool_protocol_failures_are_typed_and_preserve_retry_classification() {
        for failure in [
            CompatibilityToolProtocolFailure::MalformedFrame,
            CompatibilityToolProtocolFailure::FrameTooLarge,
        ] {
            let error = ApiError::CompatibilityToolProtocol(failure);
            assert!(error.is_compatibility_tool_protocol_failure());
            assert_eq!(error.safe_failure_class(), "provider_transport");
            assert_eq!(
                error.to_string(),
                format!("invalid sse frame: {}", failure.message())
            );

            let exhausted = ApiError::RetriesExhausted {
                attempts: 1,
                last_error: Box::new(error),
            };
            assert!(exhausted.is_compatibility_tool_protocol_failure());
        }
    }
}
