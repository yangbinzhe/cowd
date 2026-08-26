use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{json, Value};
use tracing;

use crate::client::build_provider_wire_request;
use crate::error::{ApiError, CompatibilityToolProtocolFailure};
use crate::types::{
    ContentBlockDelta, ContentBlockDeltaEvent, ContentBlockStartEvent, ContentBlockStopEvent,
    InputContentBlock, InputMessage, MessageDelta, MessageDeltaEvent, MessageRequest,
    MessageResponse, MessageStartEvent, MessageStopEvent, OutputContentBlock, StreamEvent,
    ToolChoice, ToolDefinition, ToolResultContentBlock, Usage,
};
use crate::{ProviderWireHeader, ProviderWireRequest};
use model_protocol::provider_capability::ProviderCapabilityProfile;

use super::{preflight_message_request, Provider, ProviderFuture};

pub const DEFAULT_XAI_BASE_URL: &str = "https://api.x.ai/v1";
pub const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
pub const DEFAULT_MOONSHOT_BASE_URL: &str = "https://api.moonshot.cn/v1";
pub const DEFAULT_DEEPSEEK_BASE_URL: &str = "https://api.deepseek.com/v1";
const REQUEST_ID_HEADER: &str = "request-id";
const ALT_REQUEST_ID_HEADER: &str = "x-request-id";
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(128);
// Standalone Provider consumers retain transport resilience. Governed Runtime
// requests explicitly call `without_retries()` so Runtime remains their sole
// retry/fallback owner.
const DEFAULT_MAX_RETRIES: u32 = 8;
// Some DeepSeek-compatible deployments emit a documented DSML envelope in
// `content` instead of OpenAI's structured `tool_calls`. This is intentionally
// narrow: generic XML-shaped model text must remain text and never become an
// executable tool invocation.
const DSML_TOOL_CALLS_OPEN: &str = "<｜｜DSML｜｜tool_calls>";
const DSML_TOOL_CALLS_CLOSE: &str = "</｜｜DSML｜｜tool_calls>";
const DSML_INVOKE_OPEN: &str = "<｜｜DSML｜｜invoke ";
const DSML_INVOKE_CLOSE: &str = "</｜｜DSML｜｜invoke>";
const DSML_PARAMETER_OPEN: &str = "<｜｜DSML｜｜parameter ";
const DSML_PARAMETER_CLOSE: &str = "</｜｜DSML｜｜parameter>";
const COMPAT_TOOL_FRAME_MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAiCompatConfig {
    pub provider_name: &'static str,
    pub api_key_env: &'static str,
    pub base_url_env: &'static str,
    pub default_base_url: &'static str,
    pub wire_protocol: OpenAiWireProtocol,
    /// Whether this endpoint supports OpenAI-compatible streamed usage chunks.
    pub request_stream_usage: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OpenAiWireProtocol {
    #[default]
    Completions,
    Responses,
}

const XAI_ENV_VARS: &[&str] = &["XAI_API_KEY"];
const OPENAI_ENV_VARS: &[&str] = &["OPENAI_API_KEY"];
const MOONSHOT_ENV_VARS: &[&str] = &["MOONSHOT_API_KEY"];
const DEEPSEEK_ENV_VARS: &[&str] = &["DEEPSEEK_API_KEY"];

impl OpenAiCompatConfig {
    #[must_use]
    pub const fn xai() -> Self {
        Self {
            provider_name: "xAI",
            api_key_env: "XAI_API_KEY",
            base_url_env: "XAI_BASE_URL",
            default_base_url: DEFAULT_XAI_BASE_URL,
            wire_protocol: OpenAiWireProtocol::Completions,
            request_stream_usage: false,
        }
    }

    #[must_use]
    pub const fn openai() -> Self {
        Self {
            provider_name: "OpenAI",
            api_key_env: "OPENAI_API_KEY",
            base_url_env: "OPENAI_BASE_URL",
            default_base_url: DEFAULT_OPENAI_BASE_URL,
            wire_protocol: OpenAiWireProtocol::Completions,
            request_stream_usage: true,
        }
    }

    /// Moonshot AI (Kimi family models) compatible endpoint.
    /// Uses the OpenAI-compatible REST shape at /v1.
    #[must_use]
    pub const fn moonshot() -> Self {
        Self {
            provider_name: "Moonshot",
            api_key_env: "MOONSHOT_API_KEY",
            base_url_env: "MOONSHOT_BASE_URL",
            default_base_url: DEFAULT_MOONSHOT_BASE_URL,
            wire_protocol: OpenAiWireProtocol::Completions,
            request_stream_usage: false,
        }
    }

    /// DeepSeek (DeepSeek-V3, DeepSeek-R1 family models) compatible endpoint.
    /// Uses the OpenAI-compatible REST shape at /v1.
    #[must_use]
    pub const fn deepseek() -> Self {
        Self {
            provider_name: "DeepSeek",
            api_key_env: "DEEPSEEK_API_KEY",
            base_url_env: "DEEPSEEK_BASE_URL",
            default_base_url: DEFAULT_DEEPSEEK_BASE_URL,
            wire_protocol: OpenAiWireProtocol::Completions,
            request_stream_usage: true,
        }
    }

    #[must_use]
    pub const fn with_wire_protocol(mut self, wire_protocol: OpenAiWireProtocol) -> Self {
        self.wire_protocol = wire_protocol;
        self
    }

    #[must_use]
    pub fn credential_env_vars(self) -> &'static [&'static str] {
        match self.provider_name {
            "xAI" => XAI_ENV_VARS,
            "OpenAI" => OPENAI_ENV_VARS,
            "Moonshot" => MOONSHOT_ENV_VARS,
            "DeepSeek" => DEEPSEEK_ENV_VARS,
            _ => &[],
        }
    }
}

pub(crate) fn read_env_api_key(config: OpenAiCompatConfig) -> Result<Option<String>, ApiError> {
    read_env_non_empty(config.api_key_env)
}

#[derive(Debug, Clone)]
pub struct OpenAiCompatClient {
    http: reqwest::Client,
    api_key: String,
    config: OpenAiCompatConfig,
    base_url: String,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    override_provider_name: Option<String>,
}

impl OpenAiCompatClient {
    const fn config(&self) -> OpenAiCompatConfig {
        self.config
    }

    pub fn wire_request(&self, request: &MessageRequest) -> Result<ProviderWireRequest, ApiError> {
        let (endpoint, protocol, body) = match self.wire_protocol() {
            OpenAiWireProtocol::Completions => (
                chat_completions_endpoint(&self.base_url),
                "openai_chat_completions",
                build_chat_completion_request(request, self.config()),
            ),
            OpenAiWireProtocol::Responses => (
                responses_endpoint(&self.base_url),
                "openai_responses",
                build_responses_request(request),
            ),
        };
        build_provider_wire_request(
            endpoint,
            protocol,
            vec![
                ProviderWireHeader {
                    name: "content-type".to_string(),
                    value: "application/json".to_string(),
                },
                ProviderWireHeader {
                    name: "authorization".to_string(),
                    value: "Bearer <redacted>".to_string(),
                },
            ],
            body,
            request.tools.as_deref(),
        )
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    #[must_use]
    pub const fn without_retries(mut self) -> Self {
        self.max_retries = 0;
        self
    }
    #[must_use]
    pub fn new(api_key: impl Into<String>, config: OpenAiCompatConfig) -> Self {
        Self::new_with_http(api_key, config, reqwest::Client::new())
    }

    #[must_use]
    pub fn new_with_http(
        api_key: impl Into<String>,
        config: OpenAiCompatConfig,
        http: reqwest::Client,
    ) -> Self {
        Self {
            http,
            api_key: api_key.into(),
            config,
            base_url: read_base_url(config),
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            override_provider_name: None,
        }
    }

    /// 从配置直接构造，不读环境变量。
    /// 与 `from_env` 不同，不调用 `read_base_url`，不使用 `OPENAI_BASE_URL`。
    #[must_use]
    pub fn new_custom(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        provider_name: impl Into<String>,
    ) -> Self {
        Self::new_custom_with_protocol(
            api_key,
            base_url,
            provider_name,
            OpenAiWireProtocol::Completions,
        )
    }

    #[must_use]
    pub fn new_custom_with_protocol(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        provider_name: impl Into<String>,
        wire_protocol: OpenAiWireProtocol,
    ) -> Self {
        Self::new_custom_with_protocol_and_http(
            api_key,
            base_url,
            provider_name,
            wire_protocol,
            reqwest::Client::new(),
        )
    }

    #[must_use]
    pub fn new_custom_with_protocol_and_http(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        provider_name: impl Into<String>,
        wire_protocol: OpenAiWireProtocol,
        http: reqwest::Client,
    ) -> Self {
        Self {
            http,
            api_key: api_key.into(),
            config: OpenAiCompatConfig::openai().with_wire_protocol(wire_protocol),
            base_url: base_url.into(),
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            override_provider_name: Some(provider_name.into()),
        }
    }

    pub fn from_env(config: OpenAiCompatConfig) -> Result<Self, ApiError> {
        let Some(api_key) = read_env_non_empty(config.api_key_env)? else {
            return Err(ApiError::missing_credentials(
                config.provider_name,
                config.credential_env_vars(),
            ));
        };
        Ok(Self::new_with_http(
            api_key,
            config,
            crate::http_client::build_http_client()?,
        ))
    }

    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    #[must_use]
    pub fn with_retry_policy(
        mut self,
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        self.max_retries = max_retries;
        self.initial_backoff = initial_backoff;
        self.max_backoff = max_backoff;
        self
    }

    fn provider_name(&self) -> &str {
        self.override_provider_name
            .as_deref()
            .unwrap_or(self.config.provider_name)
    }

    #[must_use]
    pub const fn wire_protocol(&self) -> OpenAiWireProtocol {
        self.config.wire_protocol
    }

    pub async fn send_message(
        &self,
        request: &MessageRequest,
    ) -> Result<MessageResponse, ApiError> {
        let request = MessageRequest {
            stream: false,
            ..request.clone()
        };
        preflight_message_request(&request)?;
        let response = self.send_with_retry(&request).await?;
        let request_id = request_id_from_headers(response.headers());
        let body = response.text().await.map_err(ApiError::from)?;
        // Some backends return {"error":{"message":"...","type":"...","code":...}}
        // instead of a valid completion object. Check for this before attempting
        // full deserialization so the user sees the actual error, not a cryptic
        // "missing field 'id'" parse failure.
        if let Ok(raw) = serde_json::from_str::<serde_json::Value>(&body) {
            if let Some(err_obj) = raw.get("error") {
                let msg = err_obj
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("provider returned an error")
                    .to_string();
                let code = err_obj
                    .get("code")
                    .and_then(serde_json::Value::as_u64)
                    .map(|c| c as u16);
                return Err(ApiError::Api {
                    status: reqwest::StatusCode::from_u16(code.unwrap_or(400))
                        .unwrap_or(reqwest::StatusCode::BAD_REQUEST),
                    error_type: err_obj
                        .get("type")
                        .and_then(|t| t.as_str())
                        .map(str::to_owned),
                    message: Some(msg),
                    request_id,
                    body,
                    retryable: false,
                    retry_after: None,
                    suggested_action: None,
                });
            }
        }
        let mut normalized = match self.wire_protocol() {
            OpenAiWireProtocol::Completions => {
                let payload =
                    serde_json::from_str::<ChatCompletionResponse>(&body).map_err(|error| {
                        ApiError::json_deserialize(
                            self.provider_name(),
                            &request.model,
                            &body,
                            error,
                        )
                    })?;
                normalize_chat_completion_response(
                    &request.model,
                    payload,
                    request.tools.as_deref().unwrap_or_default(),
                )?
            }
            OpenAiWireProtocol::Responses => {
                let payload =
                    serde_json::from_str::<ResponsesApiResponse>(&body).map_err(|error| {
                        ApiError::json_deserialize(
                            self.provider_name(),
                            &request.model,
                            &body,
                            error,
                        )
                    })?;
                normalize_responses_response(&request.model, payload)
            }
        };
        if normalized.request_id.is_none() {
            normalized.request_id = request_id;
        }
        Ok(normalized)
    }

    pub async fn stream_message(
        &self,
        request: &MessageRequest,
    ) -> Result<MessageStream, ApiError> {
        preflight_message_request(request)?;
        let response = self
            .send_with_retry(&request.clone().with_streaming())
            .await?;
        Ok(MessageStream {
            request_id: request_id_from_headers(response.headers()),
            response,
            parser: OpenAiSseParser::with_context(
                self.provider_name(),
                request.model.clone(),
                self.wire_protocol(),
            ),
            pending: VecDeque::new(),
            done: false,
            state: StreamState::new(
                request.model.clone(),
                request.tools.as_deref().unwrap_or_default(),
            ),
            transport_activity: None,
        })
    }

    async fn send_with_retry(
        &self,
        request: &MessageRequest,
    ) -> Result<reqwest::Response, ApiError> {
        let mut attempts = 0;

        let last_error = loop {
            attempts += 1;
            let retryable_error = match self.send_raw_request(request).await {
                Ok(response) => match expect_success(response).await {
                    Ok(response) => return Ok(response),
                    Err(error) if error.is_retryable() && attempts <= self.max_retries + 1 => error,
                    Err(error) => return Err(error),
                },
                Err(error) if error.is_retryable() && attempts <= self.max_retries + 1 => error,
                Err(error) => return Err(error),
            };

            if attempts > self.max_retries {
                break retryable_error;
            }

            tokio::time::sleep(self.jittered_backoff_for_attempt(attempts)?).await;
        };

        Err(ApiError::RetriesExhausted {
            attempts,
            last_error: Box::new(last_error),
        })
    }

    async fn send_raw_request(
        &self,
        request: &MessageRequest,
    ) -> Result<reqwest::Response, ApiError> {
        let request_url = match self.wire_protocol() {
            OpenAiWireProtocol::Completions => chat_completions_endpoint(&self.base_url),
            OpenAiWireProtocol::Responses => responses_endpoint(&self.base_url),
        };
        let request_body = match self.wire_protocol() {
            OpenAiWireProtocol::Completions => {
                build_chat_completion_request(request, self.config())
            }
            OpenAiWireProtocol::Responses => build_responses_request(request),
        };
        self.http
            .post(&request_url)
            .header("content-type", "application/json")
            .bearer_auth(&self.api_key)
            .json(&request_body)
            .send()
            .await
            .map_err(ApiError::from)
    }

    fn backoff_for_attempt(&self, attempt: u32) -> Result<Duration, ApiError> {
        let Some(multiplier) = 1_u32.checked_shl(attempt.saturating_sub(1)) else {
            return Err(ApiError::BackoffOverflow {
                attempt,
                base_delay: self.initial_backoff,
            });
        };
        Ok(self
            .initial_backoff
            .checked_mul(multiplier)
            .map_or(self.max_backoff, |delay| delay.min(self.max_backoff)))
    }

    fn jittered_backoff_for_attempt(&self, attempt: u32) -> Result<Duration, ApiError> {
        let base = self.backoff_for_attempt(attempt)?;
        Ok(base + jitter_for_base(base))
    }
}

/// Process-wide counter that guarantees distinct jitter samples even when
/// the system clock resolution is coarser than consecutive retry sleeps.
static JITTER_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a random additive jitter in `[0, base]` to decorrelate retries
/// Deserialize a JSON field as a `Vec<T>`, treating an explicit `null` value
/// the same as a missing field (i.e. as an empty vector).
/// Some OpenAI-compatible providers emit `"tool_calls": null` instead of
/// omitting the field or using `[]`, which serde's `#[serde(default)]` alone
/// does not tolerate — `default` only handles absent keys, not null values.
fn deserialize_null_as_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// from multiple concurrent clients. Entropy is drawn from the nanosecond
/// wall clock mixed with a monotonic counter and run through a splitmix64
/// finalizer; adequate for retry jitter (no cryptographic requirement).
fn jitter_for_base(base: Duration) -> Duration {
    let base_nanos = u64::try_from(base.as_nanos()).unwrap_or(u64::MAX);
    if base_nanos == 0 {
        return Duration::ZERO;
    }
    let raw_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let tick = JITTER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut mixed = raw_nanos
        .wrapping_add(tick)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^= mixed >> 31;
    let jitter_nanos = mixed % base_nanos.saturating_add(1);
    Duration::from_nanos(jitter_nanos)
}

impl Provider for OpenAiCompatClient {
    type Stream = MessageStream;

    fn send_message<'a>(
        &'a self,
        request: &'a MessageRequest,
    ) -> ProviderFuture<'a, MessageResponse> {
        Box::pin(async move { self.send_message(request).await })
    }

    fn stream_message<'a>(
        &'a self,
        request: &'a MessageRequest,
    ) -> ProviderFuture<'a, Self::Stream> {
        Box::pin(async move { self.stream_message(request).await })
    }
}

#[derive(Debug)]
pub struct MessageStream {
    request_id: Option<String>,
    response: reqwest::Response,
    parser: OpenAiSseParser,
    pending: VecDeque<StreamEvent>,
    done: bool,
    state: StreamState,
    transport_activity: Option<crate::TransportActivity>,
}

impl MessageStream {
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    pub fn set_transport_activity(&mut self, activity: crate::TransportActivity) {
        self.transport_activity = Some(activity);
    }

    pub async fn next_event(&mut self) -> Result<Option<StreamEvent>, ApiError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }

            if self.done {
                self.pending.extend(self.state.finish()?);
                if let Some(event) = self.pending.pop_front() {
                    return Ok(Some(event));
                }
                return Ok(None);
            }

            match self.response.chunk().await? {
                Some(chunk) => {
                    if let Some(activity) = &self.transport_activity {
                        activity.observe();
                    }
                    for parsed in self.parser.push(&chunk)? {
                        self.pending.extend(self.state.ingest_chunk(parsed)?);
                    }
                }
                None => {
                    self.done = true;
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct OpenAiSseParser {
    buffer: Vec<u8>,
    provider: String,
    model: String,
    wire_protocol: OpenAiWireProtocol,
}

impl OpenAiSseParser {
    fn with_context(
        provider: impl Into<String>,
        model: impl Into<String>,
        wire_protocol: OpenAiWireProtocol,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            provider: provider.into(),
            model: model.into(),
            wire_protocol,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<Vec<ChatCompletionChunk>, ApiError> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();

        while let Some(frame) = next_sse_frame(&mut self.buffer) {
            let event = match self.wire_protocol {
                OpenAiWireProtocol::Completions => {
                    parse_chat_sse_frame(&frame, &self.provider, &self.model)?
                }
                OpenAiWireProtocol::Responses => {
                    parse_responses_sse_frame(&frame, &self.provider, &self.model)?
                }
            };
            if let Some(event) = event {
                events.push(event);
            }
        }

        Ok(events)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct StreamState {
    model: String,
    message_started: bool,
    next_block_index: u32,
    public_reasoning_block_index: Option<u32>,
    public_reasoning_started: bool,
    public_reasoning_finished: bool,
    text_block_index: Option<u32>,
    text_started: bool,
    text_finished: bool,
    /// Whether the reasoning/thinking block has been started (DeepSeek thinking mode).
    private_reasoning_block_index: Option<u32>,
    reasoning_started: bool,
    /// Whether the reasoning/thinking block has finished.
    reasoning_finished: bool,
    finished: bool,
    stop_reason: Option<String>,
    usage: Option<Usage>,
    tool_calls: BTreeMap<u32, ToolCallState>,
    exposed_tool_names: BTreeSet<String>,
    // Hold only a possible provider tool-frame prefix for ordinary text. Once
    // a frame begins, buffer it until its structure can be validated. Runtime,
    // which owns the per-request exposure lease, rejects unexposed names
    // before any tool execution.
    dsml_prefix: String,
    dsml_frame: Option<String>,
}

impl StreamState {
    fn new(model: String, tools: &[ToolDefinition]) -> Self {
        Self {
            model,
            message_started: false,
            next_block_index: 0,
            public_reasoning_block_index: None,
            public_reasoning_started: false,
            public_reasoning_finished: false,
            text_block_index: None,
            text_started: false,
            text_finished: false,
            private_reasoning_block_index: None,
            reasoning_started: false,
            reasoning_finished: false,
            finished: false,
            stop_reason: None,
            usage: None,
            tool_calls: BTreeMap::new(),
            exposed_tool_names: tools.iter().map(|tool| tool.name.clone()).collect(),
            dsml_prefix: String::new(),
            dsml_frame: None,
        }
    }

    fn ingest_chunk(&mut self, chunk: ChatCompletionChunk) -> Result<Vec<StreamEvent>, ApiError> {
        let mut events = Vec::new();
        if !self.message_started {
            self.message_started = true;
            events.push(StreamEvent::MessageStart(MessageStartEvent {
                message: MessageResponse {
                    id: chunk.id.clone(),
                    kind: "message".to_string(),
                    role: "assistant".to_string(),
                    content: Vec::new(),
                    model: chunk.model.clone().unwrap_or_else(|| self.model.clone()),
                    stop_reason: None,
                    stop_sequence: None,
                    usage: Usage {
                        input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        cache_read_input_tokens: 0,
                        output_tokens: 0,
                    },
                    request_id: None,
                },
            }));
        }

        if let Some(usage) = chunk.usage {
            self.usage = Some(Usage {
                input_tokens: usage.normalized_input_tokens(),
                cache_creation_input_tokens: usage.normalized_cache_creation_tokens(),
                cache_read_input_tokens: usage.normalized_cache_read_tokens(),
                output_tokens: usage.normalized_output_tokens(),
            });
        }

        for choice in chunk.choices {
            if let Some(summary) = choice
                .delta
                .reasoning_summary
                .filter(|value| !value.is_empty())
            {
                if !self.public_reasoning_started {
                    let block_index = self.allocate_block_index();
                    self.public_reasoning_block_index = Some(block_index);
                    self.public_reasoning_started = true;
                    events.push(StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                        index: block_index,
                        content_block: OutputContentBlock::ReasoningSummary {
                            text: String::new(),
                        },
                    }));
                }
                events.push(StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                    index: self
                        .public_reasoning_block_index
                        .expect("started public reasoning has a block index"),
                    delta: ContentBlockDelta::ReasoningSummaryDelta { text: summary },
                }));
            }

            // DeepSeek thinking mode: reasoning_content comes before the final answer.
            // Emit a Thinking content block to preserve it for subsequent requests.
            if let Some(reasoning) = choice
                .delta
                .reasoning_content
                .filter(|value| !value.is_empty())
            {
                if !self.reasoning_started {
                    let block_index = self.allocate_block_index();
                    self.private_reasoning_block_index = Some(block_index);
                    self.reasoning_started = true;
                    events.push(StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                        index: block_index,
                        content_block: OutputContentBlock::Thinking {
                            thinking: String::new(),
                            signature: None,
                        },
                    }));
                }
                events.push(StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                    index: self
                        .private_reasoning_block_index
                        .expect("started private reasoning has a block index"),
                    delta: ContentBlockDelta::ThinkingDelta {
                        thinking: reasoning,
                    },
                }));
            }

            // DeepSeek/Anthropic thinking mode: signature must be
            // preserved and passed back in subsequent requests.
            if let Some(signature) = choice.delta.signature {
                if !self.reasoning_started {
                    let block_index = self.allocate_block_index();
                    self.private_reasoning_block_index = Some(block_index);
                    self.reasoning_started = true;
                    events.push(StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                        index: block_index,
                        content_block: OutputContentBlock::Thinking {
                            thinking: String::new(),
                            signature: None,
                        },
                    }));
                }
                events.push(StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                    index: self
                        .private_reasoning_block_index
                        .expect("started private reasoning has a block index"),
                    delta: ContentBlockDelta::SignatureDelta { signature },
                }));
            }

            if let Some(content) = choice.delta.content.filter(|value| !value.is_empty()) {
                self.ingest_text_content(content, &mut events)?;
            }

            for tool_call in choice.delta.tool_calls {
                let provider_index = tool_call.index;
                if !self.tool_calls.contains_key(&provider_index) {
                    let block_index = self.allocate_block_index();
                    self.tool_calls.insert(
                        provider_index,
                        ToolCallState::new(provider_index, block_index),
                    );
                }
                let state = self
                    .tool_calls
                    .get_mut(&provider_index)
                    .expect("tool state was inserted");
                state.apply(tool_call);
                let block_index = state.block_index();
                if !state.started {
                    if let Some(start_event) = state.start_event()? {
                        state.started = true;
                        events.push(StreamEvent::ContentBlockStart(start_event));
                    } else {
                        continue;
                    }
                }
                if let Some(delta_event) = state.delta_event() {
                    events.push(StreamEvent::ContentBlockDelta(delta_event));
                }
                if choice.finish_reason.as_deref() == Some("tool_calls") && !state.stopped {
                    state.stopped = true;
                    events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                        index: block_index,
                    }));
                }
            }

            if let Some(finish_reason) = choice.finish_reason {
                self.stop_reason = Some(normalize_finish_reason(&finish_reason));
                if finish_reason == "tool_calls" {
                    for state in self.tool_calls.values_mut() {
                        if state.started && !state.stopped {
                            state.stopped = true;
                            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                                index: state.block_index(),
                            }));
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    fn finish(&mut self) -> Result<Vec<StreamEvent>, ApiError> {
        if self.finished {
            return Ok(Vec::new());
        }
        self.finished = true;

        let mut events = Vec::new();
        if let Some(frame) = self.dsml_frame.take() {
            match parse_compat_tool_calls(&frame, &self.exposed_tool_names) {
                Ok(calls) => self.emit_dsml_tool_calls(calls, &mut events),
                Err(error) => {
                    log_rejected_compat_tool_frame(&self.model, &frame, error);
                    // Once a provider emitted any supported protocol marker,
                    // the bytes are no longer ordinary assistant prose.
                    return Err(compatibility_tool_protocol_error(error));
                }
            }
        }
        if !self.dsml_prefix.is_empty() {
            let pending_text = std::mem::take(&mut self.dsml_prefix);
            self.emit_text_content(pending_text, &mut events);
        }
        if self.public_reasoning_started && !self.public_reasoning_finished {
            self.public_reasoning_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .public_reasoning_block_index
                    .expect("started public reasoning has a block index"),
            }));
        }
        // Close private reasoning block if started but not yet finished.
        if self.reasoning_started && !self.reasoning_finished {
            self.reasoning_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .private_reasoning_block_index
                    .expect("started private reasoning has a block index"),
            }));
        }
        if self.text_started && !self.text_finished {
            self.text_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .text_block_index
                    .expect("started text has a block index"),
            }));
        }

        for state in self.tool_calls.values_mut() {
            if !state.started {
                if let Some(start_event) = state.start_event()? {
                    state.started = true;
                    events.push(StreamEvent::ContentBlockStart(start_event));
                    if let Some(delta_event) = state.delta_event() {
                        events.push(StreamEvent::ContentBlockDelta(delta_event));
                    }
                }
            }
            if state.started && !state.stopped {
                state.stopped = true;
                events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                    index: state.block_index(),
                }));
            }
        }

        if self.message_started {
            events.push(StreamEvent::MessageDelta(MessageDeltaEvent {
                delta: MessageDelta {
                    stop_reason: Some(
                        self.stop_reason
                            .clone()
                            .unwrap_or_else(|| "end_turn".to_string()),
                    ),
                    stop_sequence: None,
                },
                usage: self.usage.clone().unwrap_or(Usage {
                    input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                    output_tokens: 0,
                }),
            }));
            events.push(StreamEvent::MessageStop(MessageStopEvent {}));
        }
        Ok(events)
    }

    fn ingest_text_content(
        &mut self,
        content: String,
        events: &mut Vec<StreamEvent>,
    ) -> Result<(), ApiError> {
        if let Some(frame) = self.dsml_frame.as_mut() {
            frame.push_str(&content);
            if frame.len() > COMPAT_TOOL_FRAME_MAX_BYTES {
                return Err(ApiError::CompatibilityToolProtocol(
                    CompatibilityToolProtocolFailure::FrameTooLarge,
                ));
            }
            return Ok(());
        }

        self.dsml_prefix.push_str(&content);
        if let Some(marker_offset) = first_compat_tool_marker(&self.dsml_prefix) {
            let frame = self.dsml_prefix.split_off(marker_offset);
            if !self.dsml_prefix.is_empty() {
                let preceding_text = std::mem::take(&mut self.dsml_prefix);
                self.emit_text_content(preceding_text, events);
            }
            self.dsml_frame = Some(frame);
            let frame = self.dsml_frame.as_deref().unwrap_or_default();
            if frame.len() > COMPAT_TOOL_FRAME_MAX_BYTES {
                return Err(ApiError::CompatibilityToolProtocol(
                    CompatibilityToolProtocolFailure::FrameTooLarge,
                ));
            }
            return Ok(());
        }

        let retained = longest_compat_tool_prefix_suffix(&self.dsml_prefix);
        let released_len = self.dsml_prefix.len().saturating_sub(retained.len());
        if released_len > 0 {
            let released = self.dsml_prefix[..released_len].to_string();
            self.dsml_prefix = retained;
            self.emit_text_content(released, events);
        }
        Ok(())
    }

    fn emit_dsml_tool_calls(&mut self, calls: Vec<DsmlToolCall>, events: &mut Vec<StreamEvent>) {
        self.close_visible_content(events);
        for call in calls {
            let index = self.allocate_block_index();
            events.push(StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                index,
                content_block: OutputContentBlock::ToolUse {
                    id: call.id,
                    name: call.name,
                    input: json!({}),
                },
            }));
            events.push(StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                index,
                delta: ContentBlockDelta::InputJsonDelta {
                    partial_json: call.input.to_string(),
                },
            }));
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index,
            }));
        }
        self.stop_reason = Some("tool_use".to_string());
    }

    fn allocate_block_index(&mut self) -> u32 {
        let index = self.next_block_index;
        self.next_block_index = self.next_block_index.saturating_add(1);
        index
    }

    fn close_visible_content(&mut self, events: &mut Vec<StreamEvent>) {
        if self.public_reasoning_started && !self.public_reasoning_finished {
            self.public_reasoning_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .public_reasoning_block_index
                    .expect("started public reasoning has a block index"),
            }));
        }
        if self.reasoning_started && !self.reasoning_finished {
            self.reasoning_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .private_reasoning_block_index
                    .expect("started private reasoning has a block index"),
            }));
        }
        if self.text_started && !self.text_finished {
            self.text_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .text_block_index
                    .expect("started text has a block index"),
            }));
        }
    }

    fn emit_text_content(&mut self, content: String, events: &mut Vec<StreamEvent>) {
        if self.public_reasoning_started && !self.public_reasoning_finished {
            self.public_reasoning_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .public_reasoning_block_index
                    .expect("started public reasoning has a block index"),
            }));
        }
        // Close the private reasoning block if it was started before visible content.
        if self.reasoning_started && !self.reasoning_finished {
            self.reasoning_finished = true;
            events.push(StreamEvent::ContentBlockStop(ContentBlockStopEvent {
                index: self
                    .private_reasoning_block_index
                    .expect("started private reasoning has a block index"),
            }));
        }
        if !self.text_started {
            let block_index = self.allocate_block_index();
            self.text_block_index = Some(block_index);
            self.text_started = true;
            events.push(StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                index: block_index,
                content_block: OutputContentBlock::Text {
                    text: String::new(),
                },
            }));
        }
        events.push(StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
            index: self
                .text_block_index
                .expect("started text has a block index"),
            delta: ContentBlockDelta::TextDelta { text: content },
        }));
    }
}

#[derive(Debug)]
struct ToolCallState {
    openai_index: u32,
    block_index: u32,
    id: Option<String>,
    name: Option<String>,
    arguments: String,
    emitted_len: usize,
    started: bool,
    stopped: bool,
}

impl ToolCallState {
    const fn new(openai_index: u32, block_index: u32) -> Self {
        Self {
            openai_index,
            block_index,
            id: None,
            name: None,
            arguments: String::new(),
            emitted_len: 0,
            started: false,
            stopped: false,
        }
    }

    fn apply(&mut self, tool_call: DeltaToolCall) {
        self.openai_index = tool_call.index;
        if let Some(id) = tool_call.id {
            self.id = Some(id);
        }
        if let Some(name) = tool_call.function.name {
            self.name = Some(name);
        }
        if let Some(arguments) = tool_call.function.arguments {
            self.arguments.push_str(&arguments);
        }
    }

    const fn block_index(&self) -> u32 {
        self.block_index
    }

    #[allow(clippy::unnecessary_wraps)]
    fn start_event(&self) -> Result<Option<ContentBlockStartEvent>, ApiError> {
        let Some(name) = self.name.clone() else {
            return Ok(None);
        };
        let id = self
            .id
            .clone()
            .unwrap_or_else(|| format!("tool_call_{}", self.openai_index));
        Ok(Some(ContentBlockStartEvent {
            index: self.block_index(),
            content_block: OutputContentBlock::ToolUse {
                id,
                name,
                input: json!({}),
            },
        }))
    }

    fn delta_event(&mut self) -> Option<ContentBlockDeltaEvent> {
        if self.emitted_len >= self.arguments.len() {
            return None;
        }
        let delta = self.arguments[self.emitted_len..].to_string();
        self.emitted_len = self.arguments.len();
        Some(ContentBlockDeltaEvent {
            index: self.block_index(),
            delta: ContentBlockDelta::InputJsonDelta {
                partial_json: delta,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    id: String,
    model: String,
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    role: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ResponseToolCall>,
    /// DeepSeek thinking-mode reasoning content.
    /// Must be passed back verbatim in subsequent requests.
    #[serde(default)]
    reasoning_content: Option<String>,
    /// Signature for thinking content (e.g., Anthropic). Must be
    /// passed back verbatim in subsequent requests.
    #[serde(default)]
    signature: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponseToolCall {
    id: String,
    function: ResponseToolFunction,
}

#[derive(Debug, Deserialize)]
struct ResponseToolFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
    // DeepSeek-style cache split: prompt_tokens = hit + miss.
    #[serde(default)]
    prompt_cache_hit_tokens: u32,
    #[serde(default)]
    prompt_cache_miss_tokens: u32,
    // OpenAI Responses-style cache split.
    #[serde(default)]
    cached_input_tokens: u32,
    #[serde(default)]
    cache_creation_input_tokens: u32,
    // OpenAI Chat Completions style: prompt_tokens_details.cached_tokens.
    #[serde(default)]
    prompt_tokens_details: Option<PromptTokensDetails>,
}

impl OpenAiUsage {
    fn normalized_input_tokens(&self) -> u32 {
        if self.input_tokens > 0 {
            // OpenAI Responses: input_tokens already excludes cached tokens.
            self.input_tokens
        } else if self.prompt_cache_miss_tokens > 0 {
            self.prompt_cache_miss_tokens
        } else if let Some(details) = &self.prompt_tokens_details {
            // OpenAI Chat Completions: prompt_tokens includes cached tokens.
            self.prompt_tokens.saturating_sub(details.cached_tokens)
        } else {
            self.prompt_tokens
        }
    }

    const fn normalized_output_tokens(&self) -> u32 {
        if self.output_tokens > 0 {
            self.output_tokens
        } else {
            self.completion_tokens
        }
    }

    fn normalized_cache_creation_tokens(&self) -> u32 {
        // OpenAI-compatible providers bill cache writes at the miss rate and
        // already exclude cached tokens from `input_tokens`. Only an explicit
        // separate cache-write field is reported here; deriving it from the
        // miss would double-count the same tokens.
        if self.cache_creation_input_tokens > 0 {
            self.cache_creation_input_tokens
        } else {
            0
        }
    }

    fn normalized_cache_read_tokens(&self) -> u32 {
        if self.prompt_cache_hit_tokens > 0 {
            self.prompt_cache_hit_tokens
        } else if self.cached_input_tokens > 0 {
            self.cached_input_tokens
        } else if let Some(details) = &self.prompt_tokens_details {
            details.cached_tokens
        } else {
            0
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromptTokensDetails {
    #[serde(default)]
    cached_tokens: u32,
}

#[derive(Debug, Deserialize)]
struct ResponsesApiResponse {
    id: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    output: Vec<ResponsesOutputItem>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
    #[serde(default)]
    incomplete_details: Option<ResponsesIncompleteDetails>,
    #[serde(default)]
    error: Option<ResponsesError>,
}

#[derive(Debug, Deserialize)]
struct ResponsesIncompleteDetails {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesError {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesOutputItem {
    #[serde(rename = "type")]
    item_type: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
    #[serde(default)]
    content: Vec<ResponsesContentPart>,
    #[serde(default)]
    summary: Vec<ResponsesContentPart>,
}

#[derive(Debug, Deserialize)]
struct ResponsesContentPart {
    #[serde(rename = "type")]
    part_type: String,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesStreamFrame {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    response: Option<ResponsesApiResponse>,
    #[serde(default)]
    item: Option<ResponsesOutputItem>,
    #[serde(default)]
    delta: Option<String>,
    #[serde(default)]
    output_index: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChunk {
    id: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    choices: Vec<ChunkChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, Deserialize)]
struct ChunkChoice {
    delta: ChunkDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChunkDelta {
    #[serde(default)]
    content: Option<String>,
    /// Provider-safe public reasoning summary, distinct from private CoT.
    #[serde(default)]
    reasoning_summary: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
    tool_calls: Vec<DeltaToolCall>,
    /// DeepSeek thinking-mode reasoning content (streaming).
    #[serde(default)]
    reasoning_content: Option<String>,
    /// Signature for thinking content (streaming). Must be
    /// passed back verbatim in subsequent requests.
    #[serde(default)]
    signature: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeltaToolCall {
    #[serde(default)]
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: DeltaFunction,
}

#[derive(Debug, Default, Deserialize)]
struct DeltaFunction {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    error_type: Option<String>,
    message: Option<String>,
}

/// Returns true for models known to reject tuning parameters like temperature,
/// `top_p`, `frequency_penalty`, and `presence_penalty`. These are typically
/// reasoning/chain-of-thought models with fixed sampling.
fn is_reasoning_model(model: &str) -> bool {
    let lowered = model.to_ascii_lowercase();
    // Strip any provider/ prefix for the check (e.g. qwen/qwen-qwq -> qwen-qwq)
    let canonical = lowered.rsplit('/').next().unwrap_or(lowered.as_str());
    // OpenAI reasoning models
    canonical.starts_with("o1")
        || canonical.starts_with("o3")
        || canonical.starts_with("o4")
        // xAI reasoning: grok-3-mini always uses reasoning mode
        || canonical == "grok-3-mini"
        // Alibaba DashScope reasoning variants (QwQ + Qwen3-Thinking family)
        || canonical.starts_with("qwen-qwq")
        || canonical.starts_with("qwq")
        || canonical.contains("thinking")
}

/// Whether this exact configured model exposes the OpenAI-compatible
/// `enable_thinking` switch. Runtime resolves the data from the model registry
/// and carries it on the request; this adapter never guesses from model names.
fn supports_openai_compat_thinking_control(request: &MessageRequest) -> bool {
    request
        .protocol_capabilities
        .iter()
        .any(|capability| capability == "openai_compat_enable_thinking")
}

/// Strip routing prefix (e.g., "openai/gpt-4" → "gpt-4") for the wire.
/// The prefix is used only to select transport; the backend expects the
/// bare model id.
fn strip_routing_prefix(model: &str) -> &str {
    if let Some(pos) = model.find('/') {
        let prefix = &model[..pos];
        // Only strip if the prefix before "/" is a known routing prefix,
        // not if "/" appears in the middle of the model name for other reasons.
        if matches!(prefix, "openai" | "xai" | "grok" | "qwen") {
            &model[pos + 1..]
        } else {
            model
        }
    } else {
        model
    }
}

fn build_chat_completion_request(request: &MessageRequest, config: OpenAiCompatConfig) -> Value {
    let mut messages = Vec::new();
    if let Some(system) = request.system.as_ref().filter(|value| !value.is_empty()) {
        messages.push(json!({
            "role": "system",
            "content": system,
        }));
    }
    for message in &request.messages {
        messages.extend(translate_message(message));
    }
    // Sanitize: drop any `role:"tool"` message that does not have a valid
    // paired `role:"assistant"` with a `tool_calls` entry carrying the same
    // `id` immediately before it (directly or as part of a run of tool
    // results). OpenAI-compatible backends return 400 for orphaned tool
    // messages regardless of how they were produced (compaction, session
    // editing, resume, etc.). We drop rather than error so the request can
    // still proceed with the remaining history intact.
    messages = sanitize_tool_message_pairing(messages);

    // Strip routing prefix (e.g., "openai/gpt-4" → "gpt-4") for the wire.
    let wire_model = strip_routing_prefix(&request.model);

    // gpt-5* requires `max_completion_tokens`; older OpenAI models accept both.
    // We send the correct field based on the wire model name so gpt-5.x requests
    // don't fail with "unknown field max_tokens".
    let max_tokens_key = if wire_model.starts_with("gpt-5") {
        "max_completion_tokens"
    } else {
        "max_tokens"
    };

    let mut payload = json!({
        "model": wire_model,
        max_tokens_key: request.max_tokens,
        "messages": messages,
        "stream": request.stream,
    });

    if request.stream && should_request_stream_usage(config) {
        payload["stream_options"] = json!({ "include_usage": true });
    }

    if let Some(tools) = &request.tools {
        payload["tools"] =
            Value::Array(tools.iter().map(openai_tool_definition).collect::<Vec<_>>());
    }
    if let Some(tool_choice) = &request.tool_choice {
        if !ProviderCapabilityProfile::explicit_tool_choice_known_unsupported(
            &request.model,
            request.reasoning_effort.as_deref(),
        ) {
            payload["tool_choice"] = openai_tool_choice(tool_choice);
        }
    }
    if request
        .tools
        .as_ref()
        .is_some_and(|tools| !tools.is_empty())
    {
        if let Some(parallel_tool_calls) = request.parallel_tool_calls {
            payload["parallel_tool_calls"] = json!(parallel_tool_calls);
        }
    }

    // OpenAI-compatible tuning parameters — only included when explicitly set.
    // Reasoning models (o1/o3/o4/grok-3-mini) reject these params with 400;
    // silently strip them to avoid cryptic provider errors.
    if !is_reasoning_model(&request.model) {
        if let Some(temperature) = request.temperature {
            payload["temperature"] = json!(temperature);
        }
        if let Some(top_p) = request.top_p {
            payload["top_p"] = json!(top_p);
        }
        if let Some(frequency_penalty) = request.frequency_penalty {
            payload["frequency_penalty"] = json!(frequency_penalty);
        }
        if let Some(presence_penalty) = request.presence_penalty {
            payload["presence_penalty"] = json!(presence_penalty);
        }
    }
    // stop is generally safe for all providers
    if let Some(stop) = &request.stop {
        if !stop.is_empty() {
            payload["stop"] = json!(stop);
        }
    }
    // Qwen hybrid Chat Completions models use `enable_thinking`, not the
    // standard `reasoning_effort`, to disable their default thinking pass.
    if request.reasoning_effort.as_deref() == Some("none")
        && supports_openai_compat_thinking_control(request)
    {
        payload["enable_thinking"] = json!(false);
    }
    // reasoning_effort for OpenAI-compatible reasoning models (o4-mini, o3, etc.)
    else if let Some(effort) = &request.reasoning_effort {
        payload["reasoning_effort"] = json!(effort);
    }

    payload
}

fn build_responses_request(request: &MessageRequest) -> Value {
    let wire_model = strip_routing_prefix(&request.model);
    let mut input = Vec::new();
    if let Some(system) = request.system.as_ref().filter(|value| !value.is_empty()) {
        input.push(json!({
            "role": "system",
            "content": [{"type": "input_text", "text": system}],
        }));
    }
    for message in &request.messages {
        input.extend(translate_message_for_responses(message));
    }

    let mut payload = json!({
        "model": wire_model,
        "max_output_tokens": request.max_tokens,
        "input": input,
        "stream": request.stream,
    });

    if let Some(tools) = &request.tools {
        payload["tools"] = Value::Array(
            tools
                .iter()
                .map(responses_tool_definition)
                .collect::<Vec<_>>(),
        );
    }
    if let Some(tool_choice) = &request.tool_choice {
        if !ProviderCapabilityProfile::explicit_tool_choice_known_unsupported(
            &request.model,
            request.reasoning_effort.as_deref(),
        ) {
            payload["tool_choice"] = responses_tool_choice(tool_choice);
        }
    }
    if request
        .tools
        .as_ref()
        .is_some_and(|tools| !tools.is_empty())
    {
        if let Some(parallel_tool_calls) = request.parallel_tool_calls {
            payload["parallel_tool_calls"] = json!(parallel_tool_calls);
        }
    }
    if !is_reasoning_model(&request.model) {
        if let Some(temperature) = request.temperature {
            payload["temperature"] = json!(temperature);
        }
        if let Some(top_p) = request.top_p {
            payload["top_p"] = json!(top_p);
        }
    }
    if let Some(stop) = &request.stop {
        if !stop.is_empty() {
            payload["stop"] = json!(stop);
        }
    }
    if let Some(effort) = &request.reasoning_effort {
        payload["reasoning"] = json!({ "effort": effort });
    }

    payload
}

fn translate_message_for_responses(message: &InputMessage) -> Vec<Value> {
    match message.role.as_str() {
        "assistant" => translate_assistant_message_for_responses(message),
        _ => translate_user_message_for_responses(message),
    }
}

fn translate_assistant_message_for_responses(message: &InputMessage) -> Vec<Value> {
    let mut entries = Vec::new();
    let mut content = Vec::new();
    for block in &message.content {
        match block {
            InputContentBlock::Text { text } if !text.is_empty() => content.push(json!({
                "type": "output_text",
                "text": text,
            })),
            InputContentBlock::ToolUse { id, name, input } => entries.push(json!({
                "type": "function_call",
                "call_id": id,
                "name": name,
                "arguments": input.to_string(),
            })),
            InputContentBlock::Text { .. }
            | InputContentBlock::Image { .. }
            | InputContentBlock::ToolResult { .. }
            | InputContentBlock::Thinking { .. }
            | InputContentBlock::RedactedThinking { .. } => {}
        }
    }
    if !content.is_empty() {
        entries.insert(0, json!({ "role": "assistant", "content": content }));
    }
    entries
}

fn translate_user_message_for_responses(message: &InputMessage) -> Vec<Value> {
    let mut entries = Vec::new();
    let mut content = Vec::new();
    for block in &message.content {
        match block {
            InputContentBlock::Text { text } if !text.is_empty() => content.push(json!({
                "type": "input_text",
                "text": text,
            })),
            InputContentBlock::Image { source } => {
                let image_url = format!("data:{};base64,{}", source.media_type, source.data);
                content.push(json!({
                    "type": "input_image",
                    "image_url": image_url,
                }));
            }
            InputContentBlock::ToolResult {
                tool_use_id,
                content: result_content,
                ..
            } => entries.push(json!({
                "type": "function_call_output",
                "call_id": tool_use_id,
                "output": flatten_tool_result_content(result_content),
            })),
            InputContentBlock::Text { .. }
            | InputContentBlock::ToolUse { .. }
            | InputContentBlock::Thinking { .. }
            | InputContentBlock::RedactedThinking { .. } => {}
        }
    }
    if !content.is_empty() {
        entries.insert(0, json!({ "role": "user", "content": content }));
    }
    entries
}

fn translate_message(message: &InputMessage) -> Vec<Value> {
    match message.role.as_str() {
        "assistant" => {
            let mut text = String::new();
            let mut tool_calls = Vec::new();
            let mut reasoning = String::new();
            for block in &message.content {
                match block {
                    InputContentBlock::Text { text: value } => text.push_str(value),
                    InputContentBlock::ToolUse { id, name, input } => tool_calls.push(json!({
                        "id": id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": input.to_string(),
                        }
                    })),
                    InputContentBlock::ToolResult { .. } | InputContentBlock::Image { .. } => {}
                    InputContentBlock::Thinking {
                        thinking: value, ..
                    } => {
                        reasoning.push_str(value);
                    }
                    InputContentBlock::RedactedThinking { .. } => {}
                }
            }
            // A reasoning-only history frame is not a valid Chat Completions
            // assistant message for every OpenAI-compatible provider.  In
            // particular DeepSeek rejects `content: null` without tool_calls,
            // even when reasoning_content is present.  Reasoning is private
            // continuation state, not a user-visible assistant turn, so omit
            // the orphan frame instead of poisoning every subsequent retry.
            if text.trim().is_empty() && tool_calls.is_empty() {
                Vec::new()
            } else {
                let mut msg = serde_json::json!({
                    "role": "assistant",
                    "content": (!text.is_empty()).then_some(text),
                });
                // DeepSeek requires reasoning_content to be passed back in
                // subsequent requests when thinking mode is enabled.
                if !reasoning.is_empty() {
                    msg["reasoning_content"] = json!(reasoning);
                }
                // Only include tool_calls when non-empty: some providers reject
                // assistant messages with an explicit empty tool_calls array.
                if !tool_calls.is_empty() {
                    msg["tool_calls"] = json!(tool_calls);
                }
                vec![msg]
            }
        }
        _ if message
            .content
            .iter()
            .any(|block| matches!(block, InputContentBlock::Image { .. })) =>
        {
            let content = message
                .content
                .iter()
                .filter_map(|block| match block {
                    InputContentBlock::Text { text } if !text.is_empty() => Some(json!({
                        "type": "text",
                        "text": text,
                    })),
                    InputContentBlock::Image { source } => {
                        let data_url = format!("data:{};base64,{}", source.media_type, source.data);
                        Some(json!({
                            "type": "image_url",
                            "image_url": {
                                "url": data_url,
                            },
                        }))
                    }
                    InputContentBlock::ToolResult { content, .. } => {
                        let text = flatten_tool_result_content(content);
                        (!text.is_empty()).then(|| {
                            json!({
                                "type": "text",
                                "text": text,
                            })
                        })
                    }
                    InputContentBlock::Text { .. }
                    | InputContentBlock::ToolUse { .. }
                    | InputContentBlock::Thinking { .. }
                    | InputContentBlock::RedactedThinking { .. } => None,
                })
                .collect::<Vec<_>>();
            if content.is_empty() {
                Vec::new()
            } else {
                vec![json!({
                    "role": "user",
                    "content": content,
                })]
            }
        }
        _ => message
            .content
            .iter()
            .filter_map(|block| match block {
                InputContentBlock::Text { text } => Some(json!({
                    "role": "user",
                    "content": text,
                })),
                InputContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    is_error,
                } => Some(json!({
                    "role": "tool",
                    "tool_call_id": tool_use_id,
                    "content": flatten_tool_result_content(content),
                    "is_error": is_error,
                })),
                InputContentBlock::Image { .. }
                | InputContentBlock::ToolUse { .. }
                | InputContentBlock::Thinking { .. }
                | InputContentBlock::RedactedThinking { .. } => None,
            })
            .collect(),
    }
}

/// Remove `role:"tool"` messages from `messages` that have no valid paired
/// `role:"assistant"` message with a matching `tool_calls[].id` immediately
/// preceding them. This is a last-resort safety net at the request-building
/// layer — the compaction boundary fix (6e301c8) prevents the most common
/// producer path, but resume, session editing, or future compaction variants
/// could still create orphaned tool messages.
///
/// Algorithm: scan left-to-right. For each `role:"tool"` message, check the
/// immediately preceding non-tool message. If it's `role:"assistant"` with a
/// `tool_calls` array containing an entry whose `id` matches the tool
/// message's `tool_call_id`, the pair is valid and both are kept. Otherwise
/// the tool message is dropped.
fn sanitize_tool_message_pairing(messages: Vec<Value>) -> Vec<Value> {
    // Collect indices of tool messages that are orphaned.
    let mut drop_indices = std::collections::HashSet::new();
    for (i, msg) in messages.iter().enumerate() {
        if msg.get("role").and_then(|v| v.as_str()) != Some("tool") {
            continue;
        }
        let tool_call_id = msg
            .get("tool_call_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        // Find the nearest preceding non-tool message.
        let preceding = messages[..i]
            .iter()
            .rev()
            .find(|m| m.get("role").and_then(|v| v.as_str()) != Some("tool"));
        // A tool message is considered paired only when the nearest preceding
        // non-tool message is an assistant message whose `tool_calls` array
        // contains the matching id.  OpenAI-compatible backends reject a
        // `role:"tool"` entry after a user/system turn just as strictly as one
        // after a plain assistant turn, so preserving it is never safe.
        let preceding_role = preceding
            .and_then(|m| m.get("role"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if preceding_role != "assistant" {
            drop_indices.insert(i);
            continue;
        }
        let paired = preceding
            .and_then(|m| m.get("tool_calls").and_then(|tc| tc.as_array()))
            .is_some_and(|tool_calls| {
                tool_calls
                    .iter()
                    .any(|tc| tc.get("id").and_then(|v| v.as_str()) == Some(tool_call_id))
            });
        if !paired {
            drop_indices.insert(i);
        }
    }
    if drop_indices.is_empty() {
        return messages;
    }
    messages
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !drop_indices.contains(i))
        .map(|(_, m)| m)
        .collect()
}

fn flatten_tool_result_content(content: &[ToolResultContentBlock]) -> String {
    content
        .iter()
        .map(|block| match block {
            ToolResultContentBlock::Text { text } => text.clone(),
            ToolResultContentBlock::Json { value } => value.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Recursively ensure every object-type node in a JSON Schema has
/// `"properties"` (at least `{}`) and `"additionalProperties": false`.
/// The `OpenAI` `/responses` endpoint validates schemas strictly and rejects
/// objects that omit these fields; `/chat/completions` is lenient but also
/// accepts them, so we normalise unconditionally.
fn normalize_object_schema(schema: &mut Value) {
    if let Some(obj) = schema.as_object_mut() {
        if obj.get("type").and_then(Value::as_str) == Some("object") {
            obj.entry("properties").or_insert_with(|| json!({}));
            obj.entry("additionalProperties")
                .or_insert(Value::Bool(false));
        }
        // Recurse into properties values
        if let Some(props) = obj.get_mut("properties") {
            if let Some(props_obj) = props.as_object_mut() {
                let keys: Vec<String> = props_obj.keys().cloned().collect();
                for k in keys {
                    if let Some(v) = props_obj.get_mut(&k) {
                        normalize_object_schema(v);
                    }
                }
            }
        }
        // Recurse into items (arrays)
        if let Some(items) = obj.get_mut("items") {
            normalize_object_schema(items);
        }
    }
}

fn openai_tool_definition(tool: &ToolDefinition) -> Value {
    let mut parameters = tool.input_schema.clone();
    normalize_object_schema(&mut parameters);
    json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": parameters,
        }
    })
}

fn openai_tool_choice(tool_choice: &ToolChoice) -> Value {
    match tool_choice {
        ToolChoice::Auto => Value::String("auto".to_string()),
        ToolChoice::Any => Value::String("required".to_string()),
        ToolChoice::Tool { name } => json!({
            "type": "function",
            "function": { "name": name },
        }),
    }
}

fn responses_tool_definition(tool: &ToolDefinition) -> Value {
    let mut parameters = tool.input_schema.clone();
    normalize_object_schema(&mut parameters);
    json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": parameters,
        "strict": true,
    })
}

fn responses_tool_choice(tool_choice: &ToolChoice) -> Value {
    match tool_choice {
        ToolChoice::Auto => Value::String("auto".to_string()),
        ToolChoice::Any => Value::String("required".to_string()),
        ToolChoice::Tool { name } => json!({
            "type": "function",
            "name": name,
        }),
    }
}

fn should_request_stream_usage(config: OpenAiCompatConfig) -> bool {
    config.request_stream_usage
}

fn normalize_chat_completion_response(
    model: &str,
    response: ChatCompletionResponse,
    exposed_tools: &[ToolDefinition],
) -> Result<MessageResponse, ApiError> {
    let choice = response
        .choices
        .into_iter()
        .next()
        .ok_or(ApiError::InvalidSseFrame(
            "chat completion response missing choices",
        ))?;
    let mut content = Vec::new();
    // DeepSeek thinking mode: reasoning_content must be preserved and
    // passed back in subsequent requests. Convert to Thinking block.
    if let Some(reasoning) = choice
        .message
        .reasoning_content
        .filter(|value| !value.is_empty())
    {
        content.push(OutputContentBlock::Thinking {
            thinking: reasoning,
            signature: choice.message.signature,
        });
    }
    if let Some(text) = choice.message.content.filter(|value| !value.is_empty()) {
        let exposed_tool_names = exposed_tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<BTreeSet<_>>();
        if let Some(marker_offset) = first_compat_tool_marker(&text) {
            let (visible_prefix, protocol_frame) = text.split_at(marker_offset);
            match parse_compat_tool_calls(protocol_frame, &exposed_tool_names) {
                Ok(calls) => {
                    if !visible_prefix.is_empty() {
                        content.push(OutputContentBlock::Text {
                            text: visible_prefix.to_string(),
                        });
                    }
                    content.extend(calls.into_iter().map(|call| OutputContentBlock::ToolUse {
                        id: call.id,
                        name: call.name,
                        input: call.input,
                    }));
                }
                Err(error) => {
                    log_rejected_compat_tool_frame(model, protocol_frame, error);
                    return Err(compatibility_tool_protocol_error(error));
                }
            }
        } else {
            content.push(OutputContentBlock::Text { text });
        }
    }
    for tool_call in choice.message.tool_calls {
        content.push(OutputContentBlock::ToolUse {
            id: tool_call.id,
            name: tool_call.function.name,
            input: parse_tool_arguments(&tool_call.function.arguments),
        });
    }

    Ok(MessageResponse {
        id: response.id,
        kind: "message".to_string(),
        role: choice.message.role,
        content,
        model: response.model.if_empty_then(model.to_string()),
        stop_reason: choice
            .finish_reason
            .map(|value| normalize_finish_reason(&value)),
        stop_sequence: None,
        usage: Usage {
            input_tokens: response
                .usage
                .as_ref()
                .map_or(0, OpenAiUsage::normalized_input_tokens),
            cache_creation_input_tokens: response
                .usage
                .as_ref()
                .map_or(0, OpenAiUsage::normalized_cache_creation_tokens),
            cache_read_input_tokens: response
                .usage
                .as_ref()
                .map_or(0, OpenAiUsage::normalized_cache_read_tokens),
            output_tokens: response
                .usage
                .as_ref()
                .map_or(0, OpenAiUsage::normalized_output_tokens),
        },
        request_id: None,
    })
}

fn normalize_responses_response(model: &str, response: ResponsesApiResponse) -> MessageResponse {
    let mut content = Vec::new();
    let mut has_tool_call = false;
    for item in response.output {
        match item.item_type.as_str() {
            "message" => {
                for part in item.content {
                    if matches!(part.part_type.as_str(), "output_text" | "text") {
                        if let Some(text) = part.text.filter(|value| !value.is_empty()) {
                            content.push(OutputContentBlock::Text { text });
                        }
                    }
                }
            }
            "function_call" => {
                has_tool_call = true;
                content.push(OutputContentBlock::ToolUse {
                    id: item
                        .call_id
                        .or(item.id)
                        .unwrap_or_else(|| "call_0".to_string()),
                    name: item.name.unwrap_or_else(|| "unknown_tool".to_string()),
                    input: parse_tool_arguments(item.arguments.as_deref().unwrap_or("{}")),
                });
            }
            "reasoning" => {
                let text = item
                    .summary
                    .into_iter()
                    .filter_map(|part| part.text)
                    .collect::<Vec<_>>()
                    .join("");
                if !text.is_empty() {
                    content.push(OutputContentBlock::ReasoningSummary { text });
                }
            }
            _ => {}
        }
    }
    MessageResponse {
        id: response.id,
        kind: "message".to_string(),
        role: "assistant".to_string(),
        content,
        model: response.model.unwrap_or_else(|| model.to_string()),
        stop_reason: Some(if has_tool_call {
            "tool_use".to_string()
        } else {
            "end_turn".to_string()
        }),
        stop_sequence: None,
        usage: response
            .usage
            .as_ref()
            .map_or_else(Usage::default, |usage| Usage {
                input_tokens: usage.normalized_input_tokens(),
                cache_creation_input_tokens: usage.normalized_cache_creation_tokens(),
                cache_read_input_tokens: usage.normalized_cache_read_tokens(),
                output_tokens: usage.normalized_output_tokens(),
            }),
        request_id: None,
    }
}

fn parse_tool_arguments(arguments: &str) -> Value {
    serde_json::from_str(arguments).unwrap_or_else(|_| json!({ "raw": arguments }))
}

#[derive(Debug, Clone, PartialEq)]
struct DsmlToolCall {
    id: String,
    name: String,
    input: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompatToolFrameError {
    FrameTooLarge,
    UnrecognizedMarker,
    Unterminated,
    InvalidAttributes,
    InvalidArguments,
    DuplicateParameter,
    EmptyCalls,
    UnsupportedShape,
}

impl CompatToolFrameError {
    const fn class(self) -> &'static str {
        match self {
            Self::FrameTooLarge => "frame_too_large",
            Self::UnrecognizedMarker => "unrecognized_marker",
            Self::Unterminated => "unterminated",
            Self::InvalidAttributes => "invalid_attributes",
            Self::InvalidArguments => "invalid_arguments",
            Self::DuplicateParameter => "duplicate_parameter",
            Self::EmptyCalls => "empty_calls",
            Self::UnsupportedShape => "unsupported_shape",
        }
    }
}

fn compatibility_tool_protocol_error(error: CompatToolFrameError) -> ApiError {
    ApiError::CompatibilityToolProtocol(match error {
        CompatToolFrameError::FrameTooLarge => CompatibilityToolProtocolFailure::FrameTooLarge,
        CompatToolFrameError::UnrecognizedMarker
        | CompatToolFrameError::Unterminated
        | CompatToolFrameError::InvalidAttributes
        | CompatToolFrameError::InvalidArguments
        | CompatToolFrameError::DuplicateParameter
        | CompatToolFrameError::EmptyCalls
        | CompatToolFrameError::UnsupportedShape => {
            CompatibilityToolProtocolFailure::MalformedFrame
        }
    })
}

fn log_rejected_compat_tool_frame(model: &str, frame: &str, error: CompatToolFrameError) {
    tracing::warn!(
        provider = "openai_compat",
        model,
        frame_bytes = frame.len(),
        failure_class = error.class(),
        redacted_preview = "<compat-tool-frame-redacted>",
        "rejected malformed compatibility tool-call frame"
    );
}

fn parse_dsml_tool_calls(
    text: &str,
    _exposed_tool_names: &BTreeSet<String>,
) -> Result<Vec<DsmlToolCall>, CompatToolFrameError> {
    let body = text
        .trim()
        .strip_prefix(DSML_TOOL_CALLS_OPEN)
        .ok_or(CompatToolFrameError::UnrecognizedMarker)?
        .strip_suffix(DSML_TOOL_CALLS_CLOSE)
        .ok_or(CompatToolFrameError::Unterminated)?;
    let mut remaining = body.trim();
    let mut calls = Vec::new();

    while !remaining.is_empty() {
        let after_open = remaining
            .strip_prefix(DSML_INVOKE_OPEN)
            .ok_or(CompatToolFrameError::UnsupportedShape)?;
        let tag_end = after_open
            .find('>')
            .ok_or(CompatToolFrameError::Unterminated)?;
        let attributes = parse_dsml_attributes(&after_open[..tag_end])?;
        if attributes.len() != 1 {
            return Err(CompatToolFrameError::InvalidAttributes);
        }
        let name = attributes
            .get("name")
            .ok_or(CompatToolFrameError::InvalidAttributes)?
            .clone();
        let after_tag = &after_open[tag_end + 1..];
        let (parameters, after_close) = after_tag
            .split_once(DSML_INVOKE_CLOSE)
            .ok_or(CompatToolFrameError::Unterminated)?;
        let mut parameter_source = parameters.trim();
        let mut input = serde_json::Map::new();
        while !parameter_source.is_empty() {
            let after_parameter_open = parameter_source
                .strip_prefix(DSML_PARAMETER_OPEN)
                .ok_or(CompatToolFrameError::UnsupportedShape)?;
            let parameter_tag_end = after_parameter_open
                .find('>')
                .ok_or(CompatToolFrameError::Unterminated)?;
            let parameter_attributes =
                parse_dsml_attributes(&after_parameter_open[..parameter_tag_end])?;
            if parameter_attributes.len() != 2 {
                return Err(CompatToolFrameError::InvalidAttributes);
            }
            let parameter_name = parameter_attributes
                .get("name")
                .ok_or(CompatToolFrameError::InvalidAttributes)?
                .clone();
            let is_string = match parameter_attributes
                .get("string")
                .ok_or(CompatToolFrameError::InvalidAttributes)?
                .as_str()
            {
                "true" => true,
                "false" => false,
                _ => return Err(CompatToolFrameError::InvalidAttributes),
            };
            let parameter_after_tag = &after_parameter_open[parameter_tag_end + 1..];
            let (value, after_parameter_close) = parameter_after_tag
                .split_once(DSML_PARAMETER_CLOSE)
                .ok_or(CompatToolFrameError::Unterminated)?;
            let value = if is_string {
                Value::String(value.to_string())
            } else {
                serde_json::from_str(value).map_err(|_| CompatToolFrameError::InvalidArguments)?
            };
            if input.insert(parameter_name, value).is_some() {
                return Err(CompatToolFrameError::DuplicateParameter);
            }
            parameter_source = after_parameter_close.trim();
        }
        calls.push(DsmlToolCall {
            id: format!("dsml-tool-{}", calls.len()),
            name,
            input: Value::Object(input),
        });
        remaining = after_close.trim();
    }

    if calls.is_empty() {
        Err(CompatToolFrameError::EmptyCalls)
    } else {
        Ok(calls)
    }
}

fn parse_compat_tool_calls(
    text: &str,
    exposed_tool_names: &BTreeSet<String>,
) -> Result<Vec<DsmlToolCall>, CompatToolFrameError> {
    if text.len() > COMPAT_TOOL_FRAME_MAX_BYTES {
        return Err(CompatToolFrameError::FrameTooLarge);
    }
    let trimmed = text.trim();
    if trimmed.starts_with(DSML_TOOL_CALLS_OPEN) {
        parse_dsml_tool_calls(trimmed, exposed_tool_names)
    } else {
        Err(CompatToolFrameError::UnrecognizedMarker)
    }
}

fn parse_dsml_attributes(source: &str) -> Result<BTreeMap<String, String>, CompatToolFrameError> {
    let mut attributes = BTreeMap::new();
    for token in source.split_whitespace() {
        let (key, value) = token
            .split_once('=')
            .ok_or(CompatToolFrameError::InvalidAttributes)?;
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or(CompatToolFrameError::InvalidAttributes)?;
        if key.is_empty()
            || value.contains('<')
            || value.contains('>')
            || attributes
                .insert(key.to_string(), value.to_string())
                .is_some()
        {
            return Err(CompatToolFrameError::InvalidAttributes);
        }
    }
    if attributes.is_empty() {
        Err(CompatToolFrameError::InvalidAttributes)
    } else {
        Ok(attributes)
    }
}

fn first_compat_tool_marker(text: &str) -> Option<usize> {
    text.find(DSML_TOOL_CALLS_OPEN)
}

fn longest_compat_tool_prefix_suffix(text: &str) -> String {
    let max_marker_prefix = DSML_TOOL_CALLS_OPEN.len().saturating_sub(1);
    let max_len = text.len().min(max_marker_prefix);
    for length in (1..=max_len).rev() {
        if !text.is_char_boundary(text.len() - length) {
            continue;
        }
        let suffix = &text[text.len() - length..];
        if DSML_TOOL_CALLS_OPEN.starts_with(suffix) {
            return suffix.to_string();
        }
    }
    String::new()
}

fn next_sse_frame(buffer: &mut Vec<u8>) -> Option<String> {
    let separator = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, 2))
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| (position, 4))
        })?;

    let (position, separator_len) = separator;
    let frame = buffer.drain(..position + separator_len).collect::<Vec<_>>();
    let frame_len = frame.len().saturating_sub(separator_len);
    Some(String::from_utf8_lossy(&frame[..frame_len]).into_owned())
}

fn parse_chat_sse_frame(
    frame: &str,
    provider: &str,
    model: &str,
) -> Result<Option<ChatCompletionChunk>, ApiError> {
    let trimmed = frame.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut data_lines = Vec::new();
    for line in trimmed.lines() {
        if line.starts_with(':') {
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.trim_start());
        }
    }
    if data_lines.is_empty() {
        return Ok(None);
    }
    let payload = data_lines.join("\n");
    if payload == "[DONE]" {
        return Ok(None);
    }
    // Some backends embed an error object in a data: frame instead of using an
    // HTTP error status. Surface the error message directly rather than letting
    // ChatCompletionChunk deserialization fail with a cryptic 'missing field' error.
    if let Ok(raw) = serde_json::from_str::<serde_json::Value>(&payload) {
        if let Some(err_obj) = raw.get("error") {
            let msg = err_obj
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("provider returned an error in stream")
                .to_string();
            let code = err_obj
                .get("code")
                .and_then(serde_json::Value::as_u64)
                .map(|c| c as u16);
            let status = reqwest::StatusCode::from_u16(code.unwrap_or(400))
                .unwrap_or(reqwest::StatusCode::BAD_REQUEST);
            return Err(ApiError::Api {
                status,
                error_type: err_obj
                    .get("type")
                    .and_then(|t| t.as_str())
                    .map(str::to_owned),
                message: Some(msg),
                request_id: None,
                body: payload.clone(),
                retryable: false,
                retry_after: None,
                suggested_action: None,
            });
        }
    }
    serde_json::from_str::<ChatCompletionChunk>(&payload)
        .map(Some)
        .map_err(|error| {
            tracing::warn!(error = %error, "stream chunk parse error");
            ApiError::json_deserialize(provider, model, &payload, error)
        })
}

fn parse_responses_sse_frame(
    frame: &str,
    provider: &str,
    model: &str,
) -> Result<Option<ChatCompletionChunk>, ApiError> {
    let trimmed = frame.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut data_lines = Vec::new();
    for line in trimmed.lines() {
        if line.starts_with(':') {
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.trim_start());
        }
    }
    if data_lines.is_empty() {
        return Ok(None);
    }
    let payload = data_lines.join("\n");
    if payload == "[DONE]" {
        return Ok(None);
    }
    if let Ok(raw) = serde_json::from_str::<serde_json::Value>(&payload) {
        if let Some(err_obj) = raw.get("error") {
            let msg = err_obj
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("provider returned an error in stream")
                .to_string();
            let code = err_obj
                .get("code")
                .and_then(serde_json::Value::as_u64)
                .map(|c| c as u16);
            let status = reqwest::StatusCode::from_u16(code.unwrap_or(400))
                .unwrap_or(reqwest::StatusCode::BAD_REQUEST);
            return Err(ApiError::Api {
                status,
                error_type: err_obj
                    .get("type")
                    .and_then(|t| t.as_str())
                    .map(str::to_owned),
                message: Some(msg),
                request_id: None,
                body: payload.clone(),
                retryable: false,
                retry_after: None,
                suggested_action: None,
            });
        }
    }

    let frame = serde_json::from_str::<ResponsesStreamFrame>(&payload).map_err(|error| {
        tracing::warn!(error = %error, "responses stream chunk parse error");
        ApiError::json_deserialize(provider, model, &payload, error)
    })?;
    if frame.event_type == "response.failed" {
        let response = frame.response.as_ref();
        let error = response.and_then(|response| response.error.as_ref());
        let status = response
            .and_then(|response| response.status.as_deref())
            .unwrap_or("failed");
        let error_type = error
            .and_then(|error| error.code.clone())
            .unwrap_or_else(|| "response_failed".to_string());
        let retryable = [
            "rate_limit",
            "overloaded",
            "server_error",
            "temporarily_unavailable",
        ]
        .iter()
        .any(|marker| error_type.to_ascii_lowercase().contains(marker));
        return Err(ApiError::Api {
            status: if error_type.to_ascii_lowercase().contains("rate_limit") {
                reqwest::StatusCode::TOO_MANY_REQUESTS
            } else {
                reqwest::StatusCode::BAD_GATEWAY
            },
            error_type: Some(error_type),
            message: error.and_then(|error| error.message.clone()).or_else(|| {
                Some(format!(
                    "provider Responses stream ended with status `{status}`"
                ))
            }),
            request_id: response.map(|response| response.id.clone()),
            body: payload,
            retryable,
            retry_after: None,
            suggested_action: Some(
                "inspect the provider failure evidence before selecting a recovery action"
                    .to_string(),
            ),
        });
    }
    responses_stream_frame_to_chunk(frame, model)
}

fn responses_stream_frame_to_chunk(
    frame: ResponsesStreamFrame,
    fallback_model: &str,
) -> Result<Option<ChatCompletionChunk>, ApiError> {
    let event_type = frame.event_type.clone();
    let chunk = match event_type.as_str() {
        "response.created" => frame.response.map(|response| ChatCompletionChunk {
            id: response.id,
            model: response.model.or_else(|| Some(fallback_model.to_string())),
            choices: Vec::new(),
            usage: None,
        }),
        "response.output_text.delta" => frame.delta.map(|delta| ChatCompletionChunk {
            id: "responses_stream".to_string(),
            model: Some(fallback_model.to_string()),
            choices: vec![ChunkChoice {
                delta: ChunkDelta {
                    content: Some(delta),
                    ..ChunkDelta::default()
                },
                finish_reason: None,
            }],
            usage: None,
        }),
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            frame.delta.map(|delta| ChatCompletionChunk {
                id: "responses_stream".to_string(),
                model: Some(fallback_model.to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        // Providers expose this field as a public Responses
                        // stream item. Normalize it into Cowd's public
                        // reasoning channel, never its private-thinking type.
                        reasoning_summary: Some(delta),
                        ..ChunkDelta::default()
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
        }
        "response.function_call_arguments.delta" => {
            frame.delta.map(|arguments| ChatCompletionChunk {
                id: "responses_stream".to_string(),
                model: Some(fallback_model.to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        tool_calls: vec![DeltaToolCall {
                            index: frame.output_index.unwrap_or(0),
                            id: None,
                            function: DeltaFunction {
                                name: None,
                                arguments: Some(arguments),
                            },
                        }],
                        ..ChunkDelta::default()
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
        }
        "response.output_item.added" => frame.item.and_then(|item| {
            (item.item_type == "function_call").then(|| ChatCompletionChunk {
                id: "responses_stream".to_string(),
                model: Some(fallback_model.to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        tool_calls: vec![DeltaToolCall {
                            index: frame.output_index.unwrap_or(0),
                            id: item.call_id.or(item.id),
                            function: DeltaFunction {
                                name: item.name,
                                arguments: item.arguments,
                            },
                        }],
                        ..ChunkDelta::default()
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
        }),
        "response.completed" => frame.response.map(|response| {
            let has_tool_call = response
                .output
                .iter()
                .any(|item| item.item_type == "function_call");
            ChatCompletionChunk {
                id: response.id,
                model: response.model.or_else(|| Some(fallback_model.to_string())),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta::default(),
                    finish_reason: Some(if has_tool_call {
                        "tool_calls".to_string()
                    } else {
                        "stop".to_string()
                    }),
                }],
                usage: response.usage,
            }
        }),
        "response.incomplete" => frame.response.map(|response| {
            let finish_reason = match response
                .incomplete_details
                .as_ref()
                .and_then(|details| details.reason.as_deref())
            {
                Some("content_filter") => "content_filter",
                Some("max_output_tokens") | None => "length",
                Some(_) => "incomplete",
            };
            ChatCompletionChunk {
                id: response.id,
                model: response.model.or_else(|| Some(fallback_model.to_string())),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta::default(),
                    finish_reason: Some(finish_reason.to_string()),
                }],
                usage: response.usage,
            }
        }),
        _ => {
            return Err(ApiError::InvalidSseFrame(
                "unsupported OpenAI Responses event type",
            ));
        }
    };
    chunk.map(Some).ok_or(ApiError::InvalidSseFrame(
        "OpenAI Responses event is missing its required payload",
    ))
}

fn read_env_non_empty(key: &str) -> Result<Option<String>, ApiError> {
    match std::env::var(key) {
        Ok(value) if !value.is_empty() => Ok(Some(value)),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(super::dotenv_value(key)),
        Err(error) => Err(ApiError::from(error)),
    }
}

#[must_use]
pub fn has_api_key(key: &str) -> bool {
    read_env_non_empty(key)
        .ok()
        .and_then(std::convert::identity)
        .is_some()
}

#[must_use]
pub fn read_base_url(config: OpenAiCompatConfig) -> String {
    std::env::var(config.base_url_env).unwrap_or_else(|_| config.default_base_url.to_string())
}

fn chat_completions_endpoint(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

fn responses_endpoint(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.ends_with("/responses") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/responses")
    }
}

fn request_id_from_headers(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get(REQUEST_ID_HEADER)
        .or_else(|| headers.get(ALT_REQUEST_ID_HEADER))
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

async fn expect_success(response: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let request_id = request_id_from_headers(response.headers());
    let retry_after = retry_after_from_headers(response.headers());
    let body = response.text().await.unwrap_or_default();
    let parsed_error = serde_json::from_str::<ErrorEnvelope>(&body).ok();
    let retryable = is_retryable_status(status);

    tracing::error!(status = status.as_u16(), body = %body, retryable, "API request failed");

    Err(ApiError::Api {
        status,
        error_type: parsed_error
            .as_ref()
            .and_then(|error| error.error.error_type.clone()),
        message: parsed_error
            .as_ref()
            .and_then(|error| error.error.message.clone()),
        request_id,
        body,
        retryable,
        retry_after,
        suggested_action: ApiError::suggested_action_for_status(status),
    })
}

fn retry_after_from_headers(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(std::time::SystemTime::now())
        .ok()
}

const fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 409 | 429 | 500 | 502 | 503 | 504)
}

fn normalize_finish_reason(value: &str) -> String {
    match value {
        "stop" => "end_turn",
        "tool_calls" => "tool_use",
        other => other,
    }
    .to_string()
}

trait StringExt {
    fn if_empty_then(self, fallback: String) -> String;
}

impl StringExt for String {
    fn if_empty_then(self, fallback: String) -> String {
        if self.is_empty() {
            fallback
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_chat_completion_request, build_responses_request, chat_completions_endpoint,
        is_reasoning_model, normalize_finish_reason, openai_tool_choice, parse_compat_tool_calls,
        parse_dsml_tool_calls, parse_responses_sse_frame, parse_tool_arguments, responses_endpoint,
        retry_after_from_headers, ChatCompletionChunk, OpenAiCompatClient, OpenAiCompatConfig,
        OpenAiSseParser, OpenAiUsage, OpenAiWireProtocol, StreamState,
    };
    use crate::error::{ApiError, CompatibilityToolProtocolFailure};
    use crate::types::{
        ContentBlockDelta, ContentBlockDeltaEvent, ContentBlockStartEvent, ImageSource,
        InputContentBlock, InputMessage, MessageRequest, OutputContentBlock, StreamEvent,
        ToolChoice, ToolDefinition, ToolResultContentBlock,
    };
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    #[test]
    fn wire_evidence_uses_the_selected_openai_protocol_body() {
        let request = MessageRequest {
            model: "deepseek-v4-pro".to_string(),
            max_tokens: 256,
            messages: vec![InputMessage::user_text("inspect")],
            stream: true,
            ..Default::default()
        };
        let completion_client = OpenAiCompatClient::new_custom_with_protocol(
            "top-secret",
            "https://provider.test/v1",
            "test",
            OpenAiWireProtocol::Completions,
        );
        let completion_wire = completion_client
            .wire_request(&request)
            .expect("completion wire evidence");
        assert_eq!(
            completion_wire.body,
            build_chat_completion_request(&request, completion_client.config())
        );
        assert_eq!(
            completion_wire.endpoint,
            "https://provider.test/v1/chat/completions"
        );

        let responses_client = OpenAiCompatClient::new_custom_with_protocol(
            "top-secret",
            "https://provider.test/v1",
            "test",
            OpenAiWireProtocol::Responses,
        );
        let responses_wire = responses_client
            .wire_request(&request)
            .expect("responses wire evidence");
        assert_eq!(responses_wire.body, build_responses_request(&request));
        assert_eq!(
            responses_wire.endpoint,
            "https://provider.test/v1/responses"
        );
        assert!(!serde_json::to_string(&responses_wire)
            .expect("wire json")
            .contains("top-secret"));
    }

    #[test]
    fn governed_runtime_can_disable_transport_owned_retries() {
        let client = OpenAiCompatClient::new_custom_with_protocol(
            "test-key",
            "https://provider.test/v1",
            "test",
            OpenAiWireProtocol::Completions,
        );
        assert_eq!(client.max_retries, super::DEFAULT_MAX_RETRIES);
        assert_eq!(client.without_retries().max_retries, 0);
    }

    #[test]
    fn retry_after_supports_seconds_and_http_dates() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "7".parse().unwrap());
        assert_eq!(
            retry_after_from_headers(&headers),
            Some(std::time::Duration::from_secs(7))
        );

        let future = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        headers.insert(
            reqwest::header::RETRY_AFTER,
            httpdate::fmt_http_date(future).parse().unwrap(),
        );
        let delay = retry_after_from_headers(&headers).expect("HTTP-date delay");
        assert!((58..=60).contains(&delay.as_secs()));
    }

    #[test]
    fn request_translation_uses_openai_compatible_shape() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "grok-3".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage {
                    role: "user".to_string(),
                    content: vec![
                        InputContentBlock::Text {
                            text: "hello".to_string(),
                        },
                        InputContentBlock::ToolResult {
                            tool_use_id: "tool_1".to_string(),
                            content: vec![ToolResultContentBlock::Json {
                                value: json!({"ok": true}),
                            }],
                            is_error: false,
                        },
                    ],
                }],
                system: Some("be helpful".to_string()),
                tools: Some(vec![ToolDefinition {
                    name: "weather".to_string(),
                    description: Some("Get weather".to_string()),
                    input_schema: json!({"type": "object"}),
                }]),
                tool_choice: Some(ToolChoice::Auto),
                stream: false,
                ..Default::default()
            },
            OpenAiCompatConfig::xai(),
        );

        assert_eq!(payload["messages"][0]["role"], json!("system"));
        assert_eq!(payload["messages"][1]["role"], json!("user"));
        assert_eq!(payload["messages"][2]["role"], json!("tool"));
        assert_eq!(payload["tools"][0]["type"], json!("function"));
        assert_eq!(payload["tool_choice"], json!("auto"));
    }

    #[test]
    fn request_translation_uses_image_url_for_image_blocks() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "gpt-4o".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage {
                    role: "user".to_string(),
                    content: vec![
                        InputContentBlock::Text {
                            text: "describe it".to_string(),
                        },
                        InputContentBlock::Image {
                            source: ImageSource::base64("image/png", "aW1hZ2U="),
                        },
                    ],
                }],
                stream: false,
                ..Default::default()
            },
            OpenAiCompatConfig::openai(),
        );

        let messages = payload["messages"].as_array().unwrap();
        assert_eq!(messages[0]["role"], json!("user"));
        let content = messages[0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], json!("text"));
        assert_eq!(content[1]["type"], json!("image_url"));
        assert_eq!(
            content[1]["image_url"]["url"],
            json!("data:image/png;base64,aW1hZ2U=")
        );
    }

    #[test]
    fn responses_request_translation_uses_responses_shape() {
        let payload = build_responses_request(&MessageRequest {
            model: "openai/gpt-5".to_string(),
            max_tokens: 128,
            messages: vec![
                InputMessage {
                    role: "user".to_string(),
                    content: vec![
                        InputContentBlock::Text {
                            text: "inspect".to_string(),
                        },
                        InputContentBlock::Image {
                            source: ImageSource::base64("image/png", "aW1hZ2U="),
                        },
                    ],
                },
                InputMessage::user_tool_result("call_1", "done", false),
            ],
            system: Some("Use tools when needed.".to_string()),
            tools: Some(vec![ToolDefinition {
                name: "inspect_repo".to_string(),
                description: Some("Inspect repository".to_string()),
                input_schema: json!({"type": "object"}),
            }]),
            tool_choice: Some(ToolChoice::Auto),
            parallel_tool_calls: Some(true),
            stream: true,
            reasoning_effort: Some("medium".to_string()),
            ..Default::default()
        });

        assert_eq!(payload["model"], json!("gpt-5"));
        assert_eq!(payload["max_output_tokens"], json!(128));
        assert_eq!(payload["input"][0]["role"], json!("system"));
        assert_eq!(
            payload["input"][1]["content"][0]["type"],
            json!("input_text")
        );
        assert_eq!(
            payload["input"][1]["content"][1]["type"],
            json!("input_image")
        );
        assert_eq!(payload["input"][2]["type"], json!("function_call_output"));
        assert_eq!(payload["tools"][0]["type"], json!("function"));
        assert_eq!(payload["tools"][0]["name"], json!("inspect_repo"));
        assert_eq!(payload["tools"][0]["strict"], json!(true));
        assert_eq!(payload["parallel_tool_calls"], json!(true));
        assert_eq!(payload["reasoning"]["effort"], json!("medium"));
    }

    #[test]
    fn responses_public_reasoning_summary_is_not_private_thinking() {
        let chunk = parse_responses_sse_frame(
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"checked evidence\"}\n\n",
            "OpenAI",
            "gpt-5",
        )
        .expect("valid responses frame")
        .expect("reasoning chunk");
        let mut state = StreamState::new("gpt-5".to_string(), &[]);
        let events = state.ingest_chunk(chunk).expect("reasoning events");

        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                index: 0,
                content_block: OutputContentBlock::ReasoningSummary { .. },
            })
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: ContentBlockDelta::ReasoningSummaryDelta { text },
                ..
            }) if text == "checked evidence"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: ContentBlockDelta::ThinkingDelta { .. },
                ..
            })
        )));

        let text_chunk = parse_responses_sse_frame(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"final answer\"}\n\n",
            "OpenAI",
            "gpt-5",
        )
        .expect("valid responses frame")
        .expect("text chunk");
        let text_events = state.ingest_chunk(text_chunk).expect("text events");
        assert!(text_events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockStart(ContentBlockStartEvent {
                index: 1,
                content_block: OutputContentBlock::Text { .. },
            })
        )));
    }

    #[test]
    fn responses_reasoning_text_uses_public_reasoning_channel() {
        let chunk = parse_responses_sse_frame(
            "data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"checked repository state\"}\n\n",
            "DeepSeek",
            "deepseek-v4-flash",
        )
        .expect("valid responses frame")
        .expect("reasoning chunk");
        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[]);
        let events = state.ingest_chunk(chunk).expect("reasoning events");

        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: ContentBlockDelta::ReasoningSummaryDelta { text },
                ..
            }) if text == "checked repository state"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: ContentBlockDelta::ThinkingDelta { .. },
                ..
            })
        )));
    }

    #[test]
    fn responses_incomplete_emits_explicit_length_terminal() {
        let chunk = parse_responses_sse_frame(
            "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_limit\",\"model\":\"deepseek-v4-flash\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":13,\"output_tokens\":21}}}\n\n",
            "DeepSeek",
            "deepseek-v4-flash",
        )
        .expect("valid responses frame")
        .expect("terminal chunk");

        assert_eq!(chunk.id, "resp_limit");
        assert_eq!(chunk.choices[0].finish_reason.as_deref(), Some("length"));
        assert_eq!(
            chunk
                .usage
                .as_ref()
                .map(OpenAiUsage::normalized_output_tokens),
            Some(21)
        );
    }

    #[test]
    fn openai_usage_parses_cache_split_without_double_counting() {
        let deepseek: OpenAiUsage = serde_json::from_str(
            r#"{"prompt_tokens":1000,"prompt_cache_hit_tokens":800,"prompt_cache_miss_tokens":200,"completion_tokens":50}"#,
        )
        .expect("deepseek usage");
        assert_eq!(deepseek.normalized_input_tokens(), 200);
        assert_eq!(deepseek.normalized_cache_creation_tokens(), 0);
        assert_eq!(deepseek.normalized_cache_read_tokens(), 800);
        assert_eq!(deepseek.normalized_output_tokens(), 50);

        let chat_completions: OpenAiUsage = serde_json::from_str(
            r#"{"prompt_tokens":1000,"prompt_tokens_details":{"cached_tokens":700},"completion_tokens":50}"#,
        )
        .expect("chat completions usage");
        assert_eq!(chat_completions.normalized_input_tokens(), 300);
        assert_eq!(chat_completions.normalized_cache_creation_tokens(), 0);
        assert_eq!(chat_completions.normalized_cache_read_tokens(), 700);

        let responses: OpenAiUsage = serde_json::from_str(
            r#"{"input_tokens":300,"cached_input_tokens":700,"output_tokens":50}"#,
        )
        .expect("responses usage");
        assert_eq!(responses.normalized_input_tokens(), 300);
        assert_eq!(responses.normalized_cache_creation_tokens(), 0);
        assert_eq!(responses.normalized_cache_read_tokens(), 700);
        assert_eq!(responses.normalized_output_tokens(), 50);
    }

    #[test]
    fn responses_failed_preserves_provider_failure_evidence() {
        let error = parse_responses_sse_frame(
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"model\":\"deepseek-v4-flash\",\"status\":\"failed\",\"error\":{\"code\":\"upstream_overloaded\",\"message\":\"capacity unavailable\"}}}\n\n",
            "DeepSeek",
            "deepseek-v4-flash",
        )
        .expect_err("failed terminal must not become a silent EOF");

        assert!(matches!(
            error,
            ApiError::Api {
                status: reqwest::StatusCode::BAD_GATEWAY,
                error_type: Some(ref error_type),
                message: Some(ref message),
                request_id: Some(ref request_id),
                retryable: true,
                ..
            } if error_type == "upstream_overloaded"
                && message == "capacity unavailable"
                && request_id == "resp_failed"
        ));
    }

    #[test]
    fn responses_unknown_event_fails_typed_instead_of_becoming_eof() {
        let error = parse_responses_sse_frame(
            "data: {\"type\":\"response.future_event\",\"delta\":\"ignored before\"}\n\n",
            "OpenAI",
            "gpt-5",
        )
        .expect_err("an unverified event cannot be treated as an empty frame");

        assert!(matches!(
            error,
            ApiError::InvalidSseFrame("unsupported OpenAI Responses event type")
        ));
    }

    #[test]
    fn tool_schema_object_gets_strict_fields_for_responses_endpoint() {
        // OpenAI /responses endpoint rejects object schemas missing
        // "properties" and "additionalProperties". Verify normalize_object_schema
        // fills them in so the request shape is strict-validator-safe.
        use super::normalize_object_schema;

        // Bare object — no properties at all
        let mut schema = json!({"type": "object"});
        normalize_object_schema(&mut schema);
        assert_eq!(schema["properties"], json!({}));
        assert_eq!(schema["additionalProperties"], json!(false));

        // Nested object inside properties
        let mut schema2 = json!({
            "type": "object",
            "properties": {
                "location": {"type": "object", "properties": {"lat": {"type": "number"}}}
            }
        });
        normalize_object_schema(&mut schema2);
        assert_eq!(schema2["additionalProperties"], json!(false));
        assert_eq!(
            schema2["properties"]["location"]["additionalProperties"],
            json!(false)
        );

        // Existing properties/additionalProperties should not be overwritten
        let mut schema3 = json!({
            "type": "object",
            "properties": {"x": {"type": "string"}},
            "additionalProperties": true
        });
        normalize_object_schema(&mut schema3);
        assert_eq!(
            schema3["additionalProperties"],
            json!(true),
            "must not overwrite existing"
        );
    }

    #[test]
    fn reasoning_effort_is_included_when_set() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "o4-mini".to_string(),
                max_tokens: 1024,
                messages: vec![InputMessage::user_text("think hard")],
                reasoning_effort: Some("high".to_string()),
                ..Default::default()
            },
            OpenAiCompatConfig::openai(),
        );
        assert_eq!(payload["reasoning_effort"], json!("high"));
    }

    #[test]
    fn reasoning_effort_omitted_when_not_set() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "gpt-4o".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage::user_text("hello")],
                ..Default::default()
            },
            OpenAiCompatConfig::openai(),
        );
        assert!(payload.get("reasoning_effort").is_none());
    }

    #[test]
    fn openai_streaming_requests_include_usage_opt_in() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "gpt-5".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage::user_text("hello")],
                system: None,
                tools: None,
                tool_choice: None,
                stream: true,
                ..Default::default()
            },
            OpenAiCompatConfig::openai(),
        );

        assert_eq!(payload["stream_options"], json!({"include_usage": true}));
    }

    #[test]
    fn xai_streaming_requests_skip_openai_specific_usage_opt_in() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "grok-3".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage::user_text("hello")],
                system: None,
                tools: None,
                tool_choice: None,
                stream: true,
                ..Default::default()
            },
            OpenAiCompatConfig::xai(),
        );

        assert!(payload.get("stream_options").is_none());
    }

    #[test]
    fn deepseek_streaming_requests_include_supported_usage_opt_in() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "deepseek-v4-flash".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage::user_text("hello")],
                stream: true,
                ..Default::default()
            },
            OpenAiCompatConfig::deepseek(),
        );

        assert_eq!(payload["stream_options"], json!({"include_usage": true}));
    }

    #[test]
    fn deepseek_v4_thinking_omits_explicit_tool_choice_on_the_wire() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "deepseek-v4-flash".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage::user_text("inspect")],
                tools: Some(vec![ToolDefinition {
                    name: "read_file".to_string(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                }]),
                tool_choice: Some(ToolChoice::Auto),
                reasoning_effort: Some("high".to_string()),
                ..Default::default()
            },
            OpenAiCompatConfig::deepseek(),
        );

        assert!(
            payload.get("tool_choice").is_none(),
            "DeepSeek v4 thinking must not receive an explicit tool_choice field"
        );
        assert!(payload.get("tools").is_some());
    }

    #[test]
    fn deepseek_v4_default_and_non_thinking_omit_explicit_tool_choice_on_the_wire() {
        let payload = build_chat_completion_request(
            &MessageRequest {
                model: "deepseek-v4-flash".to_string(),
                max_tokens: 64,
                messages: vec![InputMessage::user_text("inspect")],
                tools: Some(vec![ToolDefinition {
                    name: "read_file".to_string(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                }]),
                tool_choice: Some(ToolChoice::Auto),
                ..Default::default()
            },
            OpenAiCompatConfig::deepseek(),
        );

        assert!(
            payload.get("tool_choice").is_none(),
            "DeepSeek v4 defaults to thinking mode and rejects tool_choice with 400"
        );
    }

    #[test]
    fn responses_wire_omits_tool_choice_for_deepseek_v4_thinking() {
        let payload = build_responses_request(&MessageRequest {
            model: "deepseek-v4-pro".to_string(),
            max_tokens: 64,
            messages: vec![InputMessage::user_text("inspect")],
            tools: Some(vec![ToolDefinition {
                name: "read_file".to_string(),
                description: None,
                input_schema: json!({"type":"object"}),
            }]),
            tool_choice: Some(ToolChoice::Any),
            reasoning_effort: Some("max".to_string()),
            ..Default::default()
        });

        assert!(payload.get("tool_choice").is_none());
        assert!(payload.get("tools").is_some());
    }

    fn parse_fixture(contents: &'static str) -> Vec<ChatCompletionChunk> {
        let mut parser = OpenAiSseParser::with_context(
            "DeepSeek",
            "deepseek-v4-flash",
            OpenAiWireProtocol::Completions,
        );
        parser
            .push(contents.as_bytes())
            .expect("fixture parses as Chat Completions SSE")
    }

    #[test]
    fn deepseek_v4_text_fixture_preserves_thinking_and_text() {
        let chunks = parse_fixture(include_str!("../../tests/fixtures/deepseek_v4/text.sse"));
        assert_eq!(
            chunks[0].choices[0].delta.reasoning_content.as_deref(),
            Some("Checking the repository state.")
        );
        assert_eq!(
            chunks[1].choices[0].delta.content.as_deref(),
            Some("The repository is ready.")
        );
        assert_eq!(chunks[2].choices[0].finish_reason.as_deref(), Some("stop"));

        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[]);
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(state.ingest_chunk(chunk).expect("ingest fixture chunk"));
        }
        events.extend(state.finish().expect("fixture finishes"));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::ThinkingDelta { thinking }
                    if thinking == "Checking the repository state.")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::TextDelta { text }
                    if text == "The repository is ready.")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::MessageDelta(delta)
                if delta.delta.stop_reason.as_deref() == Some("end_turn")
        )));
    }

    #[test]
    fn deepseek_v4_single_tool_fixture_yields_one_typed_tool_use() {
        let chunks = parse_fixture(include_str!(
            "../../tests/fixtures/deepseek_v4/single_tool.sse"
        ));
        let mut state = StreamState::new(
            "deepseek-v4-flash".to_string(),
            &[ToolDefinition {
                name: "read_file".to_string(),
                description: None,
                input_schema: json!({"type":"object"}),
            }],
        );
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(state.ingest_chunk(chunk).expect("ingest fixture chunk"));
        }
        events.extend(state.finish().expect("fixture finishes"));
        let starts = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ContentBlockStart(start) => Some(&start.content_block),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(starts.len(), 1);
        let OutputContentBlock::ToolUse { id, name, .. } = starts[0] else {
            panic!("expected one ToolUse");
        };
        assert_eq!(name, "read_file");
        assert_eq!(id, "call_read_1");
        let arguments = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ContentBlockDelta(delta) => match &delta.delta {
                    ContentBlockDelta::InputJsonDelta { partial_json } => {
                        Some(partial_json.clone())
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect::<String>();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&arguments).unwrap(),
            json!({"path":"Cargo.toml"})
        );
    }

    #[test]
    fn deepseek_v4_multi_tool_fixture_keeps_parallel_tool_calls_structured() {
        let chunks = parse_fixture(include_str!(
            "../../tests/fixtures/deepseek_v4/multi_tool.sse"
        ));
        let mut state = StreamState::new(
            "deepseek-v4-flash".to_string(),
            &[
                ToolDefinition {
                    name: "read_file".to_string(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                },
                ToolDefinition {
                    name: "tool_search".to_string(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                },
            ],
        );
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(state.ingest_chunk(chunk).expect("ingest fixture chunk"));
        }
        events.extend(state.finish().expect("fixture finishes"));
        let names = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ContentBlockStart(start) => match &start.content_block {
                    OutputContentBlock::ToolUse { name, .. } => Some(name.as_str()),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["read_file", "tool_search"]);
    }

    #[test]
    fn deepseek_v4_streamed_arguments_fixture_assembles_exact_json() {
        let chunks = parse_fixture(include_str!(
            "../../tests/fixtures/deepseek_v4/streamed_arguments.sse"
        ));
        let mut state = StreamState::new(
            "deepseek-v4-flash".to_string(),
            &[ToolDefinition {
                name: "read_file".to_string(),
                description: None,
                input_schema: json!({"type":"object"}),
            }],
        );
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(state.ingest_chunk(chunk).expect("ingest fixture chunk"));
        }
        events.extend(state.finish().expect("fixture finishes"));
        let arguments = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ContentBlockDelta(delta) => match &delta.delta {
                    ContentBlockDelta::InputJsonDelta { partial_json } => {
                        Some(partial_json.clone())
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect::<String>();
        assert_eq!(arguments, r#"{"path":"Cargo.toml"}"#);
    }

    #[test]
    fn deepseek_v4_unknown_frame_fixture_is_skipped_without_losing_text() {
        let chunks = parse_fixture(include_str!(
            "../../tests/fixtures/deepseek_v4/unknown_frame.sse"
        ));
        assert!(!chunks.is_empty());
        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[]);
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(state.ingest_chunk(chunk).expect("ingest fixture chunk"));
        }
        events.extend(state.finish().expect("fixture finishes"));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::TextDelta { text }
                    if text == "after unknown frames")
        )));
    }

    #[test]
    fn tool_choice_translation_supports_required_function() {
        assert_eq!(openai_tool_choice(&ToolChoice::Any), json!("required"));
        assert_eq!(
            openai_tool_choice(&ToolChoice::Tool {
                name: "weather".to_string(),
            }),
            json!({"type": "function", "function": {"name": "weather"}})
        );
    }

    #[test]
    fn parses_tool_arguments_fallback() {
        assert_eq!(
            parse_tool_arguments("{\"city\":\"Paris\"}"),
            json!({"city": "Paris"})
        );
        assert_eq!(parse_tool_arguments("not-json"), json!({"raw": "not-json"}));
    }

    #[test]
    fn missing_xai_api_key_is_provider_specific() {
        let _lock = env_lock();
        std::env::remove_var("XAI_API_KEY");
        let error = OpenAiCompatClient::from_env(OpenAiCompatConfig::xai())
            .expect_err("missing key should error");
        assert!(matches!(
            error,
            ApiError::MissingCredentials {
                provider: "xAI",
                ..
            }
        ));
    }

    #[test]
    fn endpoint_builder_accepts_base_urls_and_full_endpoints() {
        assert_eq!(
            chat_completions_endpoint("https://api.x.ai/v1"),
            "https://api.x.ai/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("https://api.x.ai/v1/"),
            "https://api.x.ai/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("https://api.x.ai/v1/chat/completions"),
            "https://api.x.ai/v1/chat/completions"
        );
        assert_eq!(
            responses_endpoint("https://api.openai.com/v1"),
            "https://api.openai.com/v1/responses"
        );
        assert_eq!(
            responses_endpoint("https://api.openai.com/v1/responses"),
            "https://api.openai.com/v1/responses"
        );
    }

    #[test]
    fn config_can_select_responses_wire_protocol() {
        let client = OpenAiCompatClient::new(
            "openai-test-key",
            OpenAiCompatConfig::openai().with_wire_protocol(OpenAiWireProtocol::Responses),
        );
        assert_eq!(client.wire_protocol(), OpenAiWireProtocol::Responses);
    }

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock")
    }

    #[test]
    fn normalizes_stop_reasons() {
        assert_eq!(normalize_finish_reason("stop"), "end_turn");
        assert_eq!(normalize_finish_reason("tool_calls"), "tool_use");
    }

    #[test]
    fn unverified_xml_tool_shape_remains_ordinary_text() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, StreamEvent};

        let tool = ToolDefinition {
            name: "read_file".to_string(),
            description: Some("read a source file".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let mut state = StreamState::new("step-3.7-flash".to_string(), &[tool]);
        let initial = state
            .ingest_chunk(ChatCompletionChunk {
                id: "message-1".to_string(),
                model: Some("step-3.7-flash".to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        content: Some(
                            "<tool_call><function=read_file><parameter=path>crates/runtime/src/lib.rs</parameter></function></tool_call>".to_string(),
                        ),
                        ..ChunkDelta::default()
                    },
                    finish_reason: Some("stop".to_string()),
                }],
                usage: None,
            })
            .expect("stream chunk");
        assert!(initial.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::TextDelta { text }
                    if text.contains("<tool_call>"))
        )));
        state.finish().expect("unverified XML remains text");
    }

    #[test]
    fn strict_dsml_parser_validates_structure_and_typed_parameters() {
        let exposed = std::collections::BTreeSet::from([
            "list_mcp_resources".to_string(),
            "read_mcp_resource".to_string(),
        ]);
        let calls = parse_dsml_tool_calls(
            "<｜｜DSML｜｜tool_calls>\n<｜｜DSML｜｜invoke name=\"list_mcp_resources\"></｜｜DSML｜｜invoke>\n<｜｜DSML｜｜invoke name=\"read_mcp_resource\"><｜｜DSML｜｜parameter name=\"uri\" string=\"true\">file://workspace/Cargo.toml</｜｜DSML｜｜parameter><｜｜DSML｜｜parameter name=\"line\" string=\"false\">12</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke>\n</｜｜DSML｜｜tool_calls>",
            &exposed,
        )
        .expect("strict DSML frame");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, "list_mcp_resources");
        assert_eq!(calls[0].input, json!({}));
        assert_eq!(calls[1].id, "dsml-tool-1");
        assert_eq!(
            calls[1].input,
            json!({"uri":"file://workspace/Cargo.toml", "line": 12})
        );
        let unavailable = parse_dsml_tool_calls(
            "<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"shell\"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>",
            &exposed,
        )
        .expect("transport parses a structurally valid call before Runtime exposure validation");
        assert_eq!(unavailable[0].name, "shell");
        assert!(parse_dsml_tool_calls(
            "<tool_call><function=read_mcp_resource></tool_call>",
            &exposed,
        )
        .is_err());
    }

    #[test]
    fn compatibility_parser_accepts_only_verified_dsml_shape() {
        let exposed = std::collections::BTreeSet::from([
            "tool_search".to_string(),
            "workspace_snapshot".to_string(),
        ]);
        for unverified in [
            "```json\n{\"tool\":\"tool_search\",\"arguments\":{}}\n```",
            "```tool_use\ntool_search\n{}\n```",
            "<tool_call><tool_name>workspace_snapshot</tool_name></tool_call>",
        ] {
            assert!(parse_compat_tool_calls(unverified, &exposed).is_err());
        }
    }

    #[test]
    fn generic_json_lookahead_is_bounded_and_released_as_text() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, StreamEvent};

        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[]);
        let ordinary_json = format!("```json\n{{{}", " ".repeat(5_000));
        let events = state
            .ingest_chunk(ChatCompletionChunk {
                id: "message-long-json".to_string(),
                model: Some("deepseek-v4-flash".to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        content: Some(ordinary_json.clone()),
                        ..ChunkDelta::default()
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
            .expect("generic JSON is ordinary text");
        assert!(
            events.iter().any(|event| matches!(
                event,
                StreamEvent::ContentBlockDelta(delta)
                    if matches!(&delta.delta, ContentBlockDelta::TextDelta { text }
                        if text == &ordinary_json)
            )),
            "a non-discriminated JSON fence must be released at the bounded lookahead"
        );
        assert!(
            state.dsml_frame.is_none(),
            "generic JSON must not remain quarantined after the lookahead cap"
        );
        state.finish().expect("ordinary JSON finishes normally");
    }

    #[test]
    fn nested_tool_shaped_json_does_not_claim_the_top_level_protocol() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, OutputContentBlock, StreamEvent};

        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[]);
        let ordinary_json =
            "```json\n{\"example\":{\"tool\":\"bash\",\"arguments\":{\"command\":\"pwd\"}}}\n```";
        let events = state
            .ingest_chunk(ChatCompletionChunk {
                id: "message-nested-json".to_string(),
                model: Some("deepseek-v4-flash".to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        content: Some(ordinary_json.to_string()),
                        ..ChunkDelta::default()
                    },
                    finish_reason: Some("stop".to_string()),
                }],
                usage: None,
            })
            .expect("nested example is ordinary text");
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::TextDelta { text }
                    if text == ordinary_json)
        )));
        assert!(events.iter().all(|event| !matches!(
            event,
            StreamEvent::ContentBlockStart(start)
                if matches!(start.content_block, OutputContentBlock::ToolUse { .. })
        )));
        state.finish().expect("nested example finishes normally");
    }

    #[test]
    fn verified_dsml_protocol_is_rejected_at_the_hard_byte_cap() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};

        let tool = ToolDefinition {
            name: "read_file".to_string(),
            description: Some("read a file".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[tool]);
        let oversized = format!(
            "<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"read_file\"><｜｜DSML｜｜parameter name=\"payload\" string=\"true\">{}</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>",
            "x".repeat(super::COMPAT_TOOL_FRAME_MAX_BYTES)
        );
        assert!(matches!(
            state.ingest_chunk(ChatCompletionChunk {
                id: "message-oversized-tool".to_string(),
                model: Some("deepseek-v4-flash".to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        content: Some(oversized),
                        ..ChunkDelta::default()
                    },
                    finish_reason: Some("stop".to_string()),
                }],
                usage: None,
            }),
            Err(ApiError::CompatibilityToolProtocol(
                CompatibilityToolProtocolFailure::FrameTooLarge
            ))
        ));
    }

    #[test]
    fn unverified_streaming_compatibility_frame_remains_text() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, StreamEvent};

        let tool = ToolDefinition {
            name: "tool_search".to_string(),
            description: Some("search tools".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[tool]);
        for content in ["```tool_", "use\ntool_search\n{\"pattern\":\"read\"}\n```"] {
            let events = state
                .ingest_chunk(ChatCompletionChunk {
                    id: "message-compat".to_string(),
                    model: Some("deepseek-v4-flash".to_string()),
                    choices: vec![ChunkChoice {
                        delta: ChunkDelta {
                            content: Some(content.to_string()),
                            ..ChunkDelta::default()
                        },
                        finish_reason: Some("stop".to_string()),
                    }],
                    usage: None,
                })
                .expect("stream chunk");
            assert!(events.iter().any(|event| matches!(
                event,
                StreamEvent::ContentBlockDelta(delta)
                    if matches!(&delta.delta, ContentBlockDelta::TextDelta { .. })
            )));
        }

        let terminal = state.finish().expect("stream finish");
        assert!(terminal.iter().any(|event| matches!(
            event,
            StreamEvent::MessageDelta(delta) if delta.delta.stop_reason.as_deref() == Some("end_turn")
        )));
    }

    #[test]
    fn streaming_strict_dsml_frame_becomes_tool_use_without_leaking_text() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, OutputContentBlock, StreamEvent};

        let tool = ToolDefinition {
            name: "list_mcp_resources".to_string(),
            description: Some("list resources".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[tool]);
        for content in [
            "<｜｜DSML｜｜tool_",
            "calls><｜｜DSML｜｜invoke name=\"list_mcp_resources\"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>",
        ] {
            let events = state
                .ingest_chunk(ChatCompletionChunk {
                    id: "message-dsml".to_string(),
                    model: Some("deepseek-v4-flash".to_string()),
                    choices: vec![ChunkChoice {
                        delta: ChunkDelta {
                            content: Some(content.to_string()),
                            ..ChunkDelta::default()
                        },
                        finish_reason: Some("stop".to_string()),
                    }],
                    usage: None,
                })
                .expect("stream chunk");
            assert!(events.iter().all(|event| !matches!(
                event,
                StreamEvent::ContentBlockDelta(delta)
                    if matches!(&delta.delta, ContentBlockDelta::TextDelta { text } if text.contains("DSML"))
            )));
        }
        let terminal = state.finish().expect("stream finish");
        assert!(terminal.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockStart(start)
                if matches!(&start.content_block, OutputContentBlock::ToolUse { name, input, .. }
                    if name == "list_mcp_resources" && input == &json!({}))
        )));
        assert!(terminal.iter().all(|event| !matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::TextDelta { text } if text.contains("DSML"))
        )));
        assert!(terminal.iter().any(|event| matches!(
            event,
            StreamEvent::MessageDelta(delta) if delta.delta.stop_reason.as_deref() == Some("tool_use")
        )));
    }

    #[test]
    fn streaming_unexposed_dsml_is_preserved_for_runtime_rejection() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, OutputContentBlock, StreamEvent};

        let tool = ToolDefinition {
            name: "read_file".to_string(),
            description: Some("read a source file".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let mut state = StreamState::new("deepseek-v4-flash".to_string(), &[tool]);
        let events = state
            .ingest_chunk(ChatCompletionChunk {
                id: "message-invalid-dsml".to_string(),
                model: Some("deepseek-v4-flash".to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        content: Some(
                            "<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"bash\"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"
                                .to_string(),
                        ),
                        ..ChunkDelta::default()
                    },
                        finish_reason: Some("stop".to_string()),
                    }],
                usage: None,
            })
            .expect("chunks are quarantined until finish");
        assert!(events.iter().all(|event| !matches!(
            event,
            StreamEvent::ContentBlockDelta(delta)
                if matches!(&delta.delta, ContentBlockDelta::TextDelta { text } if text.contains("DSML"))
        )));
        let terminal = state.finish().expect("structurally valid DSML frame");
        assert!(terminal.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockStart(start)
                if matches!(&start.content_block, OutputContentBlock::ToolUse { name, .. }
                    if name == "bash")
        )));
    }

    #[test]
    fn non_streaming_unexposed_dsml_is_preserved_for_runtime_rejection() {
        use super::{
            normalize_chat_completion_response, ChatChoice, ChatCompletionResponse, ChatMessage,
        };
        use crate::types::OutputContentBlock;

        let tool = ToolDefinition {
            name: "read_file".to_string(),
            description: Some("read a source file".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let response = ChatCompletionResponse {
            id: "message-invalid-dsml".to_string(),
            model: "deepseek-v4-flash".to_string(),
            choices: vec![ChatChoice {
                message: ChatMessage {
                    role: "assistant".to_string(),
                    content: Some(
                        "<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"bash\"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"
                            .to_string(),
                    ),
                    tool_calls: Vec::new(),
                    reasoning_content: None,
                    signature: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: None,
        };

        let response = normalize_chat_completion_response("deepseek-v4-flash", response, &[tool])
            .expect("structurally valid DSML frame");
        assert!(response.content.iter().any(
            |block| matches!(block, OutputContentBlock::ToolUse { name, .. } if name == "bash")
        ));
    }

    #[test]
    fn streaming_plain_text_with_tools_is_released_immediately() {
        use super::{ChatCompletionChunk, ChunkChoice, ChunkDelta, StreamState};
        use crate::types::{ContentBlockDelta, ContentBlockDeltaEvent, StreamEvent};

        let tool = ToolDefinition {
            name: "read_file".to_string(),
            description: Some("read a source file".to_string()),
            input_schema: json!({"type":"object"}),
        };
        let mut state = StreamState::new("step-3.7-flash".to_string(), &[tool]);
        let events = state
            .ingest_chunk(ChatCompletionChunk {
                id: "message-plain".to_string(),
                model: Some("step-3.7-flash".to_string()),
                choices: vec![ChunkChoice {
                    delta: ChunkDelta {
                        content: Some("ordinary answer".to_string()),
                        ..ChunkDelta::default()
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
            .expect("stream chunk");

        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: ContentBlockDelta::TextDelta { text },
                ..
            }) if text == "ordinary answer"
        )));
    }

    #[test]
    fn tuning_params_included_in_payload_when_set() {
        let request = MessageRequest {
            model: "gpt-4o".to_string(),
            protocol_capabilities: Vec::new(),
            max_tokens: 1024,
            context_window_limit: None,
            messages: vec![],
            system: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            stream: false,
            temperature: Some(0.7),
            top_p: Some(0.9),
            frequency_penalty: Some(0.5),
            presence_penalty: Some(0.3),
            stop: Some(vec!["\n".to_string()]),
            reasoning_effort: None,
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        assert_eq!(payload["temperature"], 0.7);
        assert_eq!(payload["top_p"], 0.9);
        assert_eq!(payload["frequency_penalty"], 0.5);
        assert_eq!(payload["presence_penalty"], 0.3);
        assert_eq!(payload["stop"], json!(["\n"]));
    }

    #[test]
    fn reasoning_model_strips_tuning_params() {
        let request = MessageRequest {
            model: "o1-mini".to_string(),
            max_tokens: 1024,
            messages: vec![],
            stream: false,
            temperature: Some(0.7),
            top_p: Some(0.9),
            frequency_penalty: Some(0.5),
            presence_penalty: Some(0.3),
            stop: Some(vec!["\n".to_string()]),
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        assert!(
            payload.get("temperature").is_none(),
            "reasoning model should strip temperature"
        );
        assert!(
            payload.get("top_p").is_none(),
            "reasoning model should strip top_p"
        );
        assert!(payload.get("frequency_penalty").is_none());
        assert!(payload.get("presence_penalty").is_none());
        // stop is safe for all providers
        assert_eq!(payload["stop"], json!(["\n"]));
    }

    #[test]
    fn grok_3_mini_is_reasoning_model() {
        assert!(is_reasoning_model("grok-3-mini"));
        assert!(is_reasoning_model("o1"));
        assert!(is_reasoning_model("o1-mini"));
        assert!(is_reasoning_model("o3-mini"));
        assert!(!is_reasoning_model("gpt-4o"));
        assert!(!is_reasoning_model("grok-3"));
        assert!(!is_reasoning_model("claude-sonnet-4-6"));
    }

    #[test]
    fn qwen_reasoning_variants_are_detected() {
        // QwQ reasoning model
        assert!(is_reasoning_model("qwen-qwq-32b"));
        assert!(is_reasoning_model("qwen/qwen-qwq-32b"));
        // Qwen3 thinking family
        assert!(is_reasoning_model("qwen3-30b-a3b-thinking"));
        assert!(is_reasoning_model("qwen/qwen3-30b-a3b-thinking"));
        // Bare qwq
        assert!(is_reasoning_model("qwq-plus"));
        // Regular Qwen models must NOT be classified as reasoning
        assert!(!is_reasoning_model("qwen-max"));
        assert!(!is_reasoning_model("qwen/qwen-plus"));
        assert!(!is_reasoning_model("qwen-turbo"));
    }

    #[test]
    fn configured_hybrid_none_effort_disables_thinking_at_wire_boundary() {
        let request = MessageRequest {
            model: "configured-hybrid".to_string(),
            protocol_capabilities: vec!["openai_compat_enable_thinking".to_string()],
            max_tokens: 1024,
            messages: vec![],
            stream: true,
            reasoning_effort: Some("none".to_string()),
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        assert_eq!(payload["enable_thinking"], json!(false));
        assert!(payload.get("reasoning_effort").is_none());

        let unsupported = MessageRequest {
            model: "gpt-4o".to_string(),
            protocol_capabilities: Vec::new(),
            ..request
        };
        let payload = build_chat_completion_request(&unsupported, OpenAiCompatConfig::openai());
        assert!(payload.get("enable_thinking").is_none());
        assert_eq!(payload["reasoning_effort"], json!("none"));
    }

    #[test]
    fn tuning_params_omitted_from_payload_when_none() {
        let request = MessageRequest {
            model: "gpt-4o".to_string(),
            max_tokens: 1024,
            messages: vec![],
            stream: false,
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        assert!(
            payload.get("temperature").is_none(),
            "temperature should be absent"
        );
        assert!(payload.get("top_p").is_none(), "top_p should be absent");
        assert!(payload.get("frequency_penalty").is_none());
        assert!(payload.get("presence_penalty").is_none());
        assert!(payload.get("stop").is_none());
    }

    #[test]
    fn gpt5_uses_max_completion_tokens_not_max_tokens() {
        // gpt-5* models require `max_completion_tokens`; legacy `max_tokens` causes
        // a request-validation failure. Verify the correct key is emitted.
        let request = MessageRequest {
            model: "gpt-5.2".to_string(),
            max_tokens: 512,
            messages: vec![],
            stream: false,
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        assert_eq!(
            payload["max_completion_tokens"],
            json!(512),
            "gpt-5.2 should emit max_completion_tokens"
        );
        assert!(
            payload.get("max_tokens").is_none(),
            "gpt-5.2 must not emit max_tokens"
        );
    }

    /// Regression test: some OpenAI-compatible providers emit `"tool_calls": null`
    /// in stream delta chunks instead of omitting the field or using `[]`.
    /// Before the fix this produced: `invalid type: null, expected a sequence`.
    #[test]
    fn delta_with_null_tool_calls_deserializes_as_empty_vec() {
        use super::deserialize_null_as_empty_vec;

        #[allow(dead_code)]
        #[derive(serde::Deserialize, Debug)]
        struct Delta {
            content: Option<String>,
            #[serde(default, deserialize_with = "deserialize_null_as_empty_vec")]
            tool_calls: Vec<super::DeltaToolCall>,
        }

        // Simulate the exact shape observed in the wild (gaebal-gajae repro 2026-04-09)
        let json = r#"{
            "content": "",
            "function_call": null,
            "refusal": null,
            "role": "assistant",
            "tool_calls": null
        }"#;
        let delta: Delta = serde_json::from_str(json)
            .expect("delta with tool_calls:null must deserialize without error");
        assert!(
            delta.tool_calls.is_empty(),
            "tool_calls:null must produce an empty vec, not an error"
        );
    }

    /// Regression: when building a multi-turn request where a prior assistant
    /// turn has no tool calls, the serialized assistant message must NOT include
    /// `tool_calls: []`. Some providers reject requests that carry an empty
    /// `tool_calls` array on assistant turns (gaebal-gajae repro 2026-04-09).
    #[test]
    fn assistant_message_without_tool_calls_omits_tool_calls_field() {
        use crate::types::{InputContentBlock, InputMessage};

        let request = MessageRequest {
            model: "gpt-4o".to_string(),
            max_tokens: 100,
            messages: vec![InputMessage {
                role: "assistant".to_string(),
                content: vec![InputContentBlock::Text {
                    text: "Hello".to_string(),
                }],
            }],
            stream: false,
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        let messages = payload["messages"].as_array().unwrap();
        let assistant_msg = messages
            .iter()
            .find(|m| m["role"] == "assistant")
            .expect("assistant message must be present");
        assert!(
            assistant_msg.get("tool_calls").is_none(),
            "assistant message without tool calls must omit tool_calls field: {assistant_msg:?}"
        );
    }

    #[test]
    fn reasoning_only_assistant_history_is_omitted_for_chat_compatibility() {
        use crate::types::{InputContentBlock, InputMessage};

        let request = MessageRequest {
            model: "deepseek-v4-flash".to_string(),
            max_tokens: 100,
            messages: vec![
                InputMessage::user_text("continue"),
                InputMessage {
                    role: "assistant".to_string(),
                    content: vec![InputContentBlock::Thinking {
                        thinking: "private reasoning without a visible answer".to_string(),
                        signature: None,
                    }],
                },
            ],
            stream: false,
            ..Default::default()
        };

        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        let messages = payload["messages"].as_array().expect("messages");
        assert_eq!(
            messages.len(),
            1,
            "reasoning-only assistant frame must be dropped"
        );
        assert_eq!(messages[0]["role"], "user");
    }

    /// Regression: assistant messages WITH tool calls must still include
    /// the `tool_calls` array (normal multi-turn tool-use flow).
    #[test]
    fn assistant_message_with_tool_calls_includes_tool_calls_field() {
        use crate::types::{InputContentBlock, InputMessage};

        let request = MessageRequest {
            model: "gpt-4o".to_string(),
            max_tokens: 100,
            messages: vec![InputMessage {
                role: "assistant".to_string(),
                content: vec![InputContentBlock::ToolUse {
                    id: "call_1".to_string(),
                    name: "read_file".to_string(),
                    input: serde_json::json!({"path": "/tmp/test"}),
                }],
            }],
            stream: false,
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        let messages = payload["messages"].as_array().unwrap();
        let assistant_msg = messages
            .iter()
            .find(|m| m["role"] == "assistant")
            .expect("assistant message must be present");
        let tool_calls = assistant_msg
            .get("tool_calls")
            .expect("assistant message with tool calls must include tool_calls field");
        assert!(tool_calls.is_array());
        assert_eq!(tool_calls.as_array().unwrap().len(), 1);
    }

    /// Orphaned tool messages (no preceding assistant `tool_calls`) must be
    /// dropped by the request-builder sanitizer. Regression for the second
    /// layer of the tool-pairing invariant fix (gaebal-gajae 2026-04-10).
    #[test]
    fn sanitize_drops_orphaned_tool_messages() {
        use super::sanitize_tool_message_pairing;

        // Valid pair: assistant with tool_calls → tool result
        let valid = vec![
            json!({"role": "assistant", "content": null, "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "search", "arguments": "{}"}}]}),
            json!({"role": "tool", "tool_call_id": "call_1", "content": "result"}),
        ];
        let out = sanitize_tool_message_pairing(valid);
        assert_eq!(out.len(), 2, "valid pair must be preserved");

        // Orphaned tool message: no preceding assistant tool_calls
        let orphaned = vec![
            json!({"role": "assistant", "content": "hi"}),
            json!({"role": "tool", "tool_call_id": "call_2", "content": "orphaned"}),
        ];
        let out = sanitize_tool_message_pairing(orphaned);
        assert_eq!(out.len(), 1, "orphaned tool message must be dropped");
        assert_eq!(out[0]["role"], json!("assistant"));

        // A tool result following a user turn is just as invalid. This is
        // the shape Runtime-authored tool recovery used to leak before it
        // began recording its synthetic assistant tool-use predecessor.
        let orphaned_after_user = vec![
            json!({"role": "user", "content": "continue"}),
            json!({"role": "tool", "tool_call_id": "call_2", "content": "orphaned"}),
        ];
        let out = sanitize_tool_message_pairing(orphaned_after_user);
        assert_eq!(
            out.len(),
            1,
            "tool messages after user turns must be dropped"
        );
        assert_eq!(out[0]["role"], json!("user"));

        // Mismatched tool_call_id
        let mismatched = vec![
            json!({"role": "assistant", "content": null, "tool_calls": [{"id": "call_3", "type": "function", "function": {"name": "f", "arguments": "{}"}}]}),
            json!({"role": "tool", "tool_call_id": "call_WRONG", "content": "bad"}),
        ];
        let out = sanitize_tool_message_pairing(mismatched);
        assert_eq!(out.len(), 1, "tool message with wrong id must be dropped");

        // Two tool results both valid (same preceding assistant)
        let two_results = vec![
            json!({"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_a", "type": "function", "function": {"name": "fa", "arguments": "{}"}},
                {"id": "call_b", "type": "function", "function": {"name": "fb", "arguments": "{}"}}
            ]}),
            json!({"role": "tool", "tool_call_id": "call_a", "content": "ra"}),
            json!({"role": "tool", "tool_call_id": "call_b", "content": "rb"}),
        ];
        let out = sanitize_tool_message_pairing(two_results);
        assert_eq!(out.len(), 3, "both valid tool results must be preserved");
    }

    #[test]
    fn non_gpt5_uses_max_tokens() {
        // Older OpenAI models expect `max_tokens`; verify gpt-4o is unaffected.
        let request = MessageRequest {
            model: "gpt-4o".to_string(),
            max_tokens: 512,
            messages: vec![],
            stream: false,
            ..Default::default()
        };
        let payload = build_chat_completion_request(&request, OpenAiCompatConfig::openai());
        assert_eq!(payload["max_tokens"], json!(512));
        assert!(
            payload.get("max_completion_tokens").is_none(),
            "gpt-4o must not emit max_completion_tokens"
        );
    }
}
