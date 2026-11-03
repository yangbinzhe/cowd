use crate::error::ApiError;
use crate::providers::anthropic::{self, AnthropicClient, AuthSource};
use crate::providers::openai_compat::{
    self, OpenAiCompatClient, OpenAiCompatConfig, OpenAiWireProtocol,
};
use crate::providers::{self, ProviderKind};
use crate::types::{MessageRequest, MessageResponse, StreamEvent};
use model_protocol::provider_config::ProviderConfig;
use model_protocol::provider_config::ProviderProtocol;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Secret-free representation of the exact HTTP payload selected by the
/// Provider adapter. The body is produced by the same builder used by the
/// transport, so Runtime evidence never has to approximate protocol mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderWireRequest {
    pub method: String,
    pub endpoint: String,
    pub protocol: String,
    pub headers: Vec<ProviderWireHeader>,
    pub body: Value,
    pub body_sha256: String,
    pub tool_schema_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderWireHeader {
    pub name: String,
    pub value: String,
}

/// Request-local protocol preparation. Only adapter preparation can construct
/// this carrier; callers can inspect evidence but cannot mutate its wire/body.
#[derive(Clone)]
pub struct PreparedProviderWire(std::sync::Arc<PreparedProviderWireInner>);

struct PreparedProviderWireInner {
    source_sha256: String,
    transport_binding: String,
    target: String,
    wire: ProviderWireRequest,
    body: bytes::Bytes,
}

impl std::fmt::Debug for PreparedProviderWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedProviderWire")
            .field("protocol", &self.0.wire.protocol)
            .field("body_bytes", &self.0.body.len())
            .field("body_sha256", &self.0.wire.body_sha256)
            .finish()
    }
}
impl PreparedProviderWire {
    pub fn wire_request(&self) -> &ProviderWireRequest {
        &self.0.wire
    }
    pub fn body_len(&self) -> usize {
        self.0.body.len()
    }
    pub(crate) fn body(&self) -> bytes::Bytes {
        self.0.body.clone()
    }
    pub(crate) fn validate(
        &self,
        source: &MessageRequest,
        target: &str,
        protocol: &str,
        transport_binding: &str,
    ) -> Result<(), ApiError> {
        if self.0.source_sha256 != request_source_digest(source)?
            || self.0.target != target
            || self.0.wire.protocol != protocol
            || self.0.transport_binding != transport_binding
        {
            return Err(ApiError::InvalidProviderConfig {
                provider: "prepared-wire".into(),
                reason: "stale_preparation: request or route changed; prepare again".into(),
            });
        }
        Ok(())
    }
}

struct WireDigestWriter(Sha256);
impl std::io::Write for WireDigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn serialized_digest<T: Serialize>(value: &T) -> Result<String, ApiError> {
    let mut writer = WireDigestWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(|e| ApiError::InvalidProviderConfig {
        provider: "prepared-wire".into(),
        reason: format!("source binding serialization failed:{e}"),
    })?;
    Ok(format!("{:x}", writer.0.finalize()))
}
pub(crate) fn preparation_transport_digest<T: Serialize>(value: &T) -> Result<String, ApiError> {
    serialized_digest(&("cowd.prepared-provider-transport/v1", value))
}
fn request_source_digest(request: &MessageRequest) -> Result<String, ApiError> {
    // Include transport-local fields skipped by the wire DTO serializer too.
    serialized_digest(&(
        "cowd.prepared-provider-source/v1",
        request,
        &request.protocol_capabilities,
        request.context_window_limit,
        &request.system_cache_prefix,
    ))
}

pub const MAX_PREPARED_WIRE_BYTES: usize = 16 * 1024 * 1024;
struct BoundedWireBody(Vec<u8>);
impl std::io::Write for BoundedWireBody {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_PREPARED_WIRE_BYTES {
            return Err(std::io::Error::other(
                "prepared wire exceeds resident body limit",
            ));
        }
        let needed = self.0.len() + bytes.len();
        if needed > self.0.capacity() {
            let capacity = self
                .0
                .capacity()
                .saturating_mul(2)
                .max(4096)
                .max(needed)
                .min(MAX_PREPARED_WIRE_BYTES);
            self.0
                .try_reserve_exact(capacity - self.0.len())
                .map_err(std::io::Error::other)?;
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn build_provider_wire_request(
    endpoint: String,
    protocol: &str,
    headers: Vec<ProviderWireHeader>,
    body: Value,
    source: &MessageRequest,
    transport_binding: &str,
) -> Result<PreparedProviderWire, ApiError> {
    let mut body_bytes = BoundedWireBody(Vec::new());
    serde_json::to_writer(&mut body_bytes, &body).map_err(|error| {
        ApiError::InvalidProviderConfig {
            provider: "wire-evidence".into(),
            reason: format!("failed to prepare bounded provider body:{error}"),
        }
    })?;
    let body_bytes = bytes::Bytes::from(body_bytes.0);
    let tool_schema_sha256 = source
        .tools
        .as_deref()
        .filter(|tools| !tools.is_empty())
        .map(|tools| serialized_digest(&tools))
        .transpose()?;
    let wire = ProviderWireRequest {
        method: "POST".to_string(),
        endpoint: endpoint
            .split_once('?')
            .map_or(endpoint.as_str(), |(path, _)| path)
            .to_string(),
        protocol: protocol.to_string(),
        headers,
        body,
        body_sha256: format!("{:x}", Sha256::digest(&body_bytes)),
        tool_schema_sha256,
    };
    Ok(PreparedProviderWire(std::sync::Arc::new(
        PreparedProviderWireInner {
            source_sha256: request_source_digest(source)?,
            transport_binding: transport_binding.into(),
            target: endpoint,
            wire,
            body: body_bytes,
        },
    )))
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ProviderClient {
    Anthropic(AnthropicClient),
    Xai(OpenAiCompatClient),
    OpenAi(OpenAiCompatClient),
}

impl ProviderClient {
    /// Install the physical send authority.
    ///
    /// Without one, every provider send is refused before it leaves the
    /// process. The authority carries an authenticated launcher identity and
    /// the durable confirmation owner; it is never derived from the request.
    #[must_use]
    pub fn with_physical_send_authority(
        self,
        authority: std::sync::Arc<crate::PhysicalSendAuthority>,
    ) -> Self {
        match self {
            Self::Anthropic(client) => {
                Self::Anthropic(client.with_physical_send_authority(authority))
            }
            Self::Xai(client) => Self::Xai(client.with_physical_send_authority(authority)),
            Self::OpenAi(client) => Self::OpenAi(client.with_physical_send_authority(authority)),
        }
    }

    /// Attach Runtime's request identity to this request-local client clone.
    #[must_use]
    pub fn with_original_provider_request_id(self, request_id: String) -> Self {
        match self {
            Self::Anthropic(client) => Self::Anthropic(client.with_original_provider_request_id(request_id)),
            Self::Xai(client) => Self::Xai(client.with_original_provider_request_id(request_id)),
            Self::OpenAi(client) => Self::OpenAi(client.with_original_provider_request_id(request_id)),
        }
    }

    #[must_use]
    pub fn with_diagnostic_physical_send(self, diagnostic: std::sync::Arc<crate::DiagnosticPhysicalSend>) -> Self {
        match self {
            Self::Anthropic(client) => Self::Anthropic(client.with_diagnostic_physical_send(diagnostic)),
            Self::Xai(client) => Self::Xai(client.with_diagnostic_physical_send(diagnostic)),
            Self::OpenAi(client) => Self::OpenAi(client.with_diagnostic_physical_send(diagnostic)),
        }
    }

    /// Disable transport-owned retries so Runtime remains the sole recovery
    /// and fallback owner for a governed attempt.
    #[must_use]
    pub fn without_retries(self) -> Self {
        match self {
            Self::Anthropic(client) => Self::Anthropic(client.without_retries()),
            Self::Xai(client) => Self::Xai(client.without_retries()),
            Self::OpenAi(client) => Self::OpenAi(client.without_retries()),
        }
    }
    pub fn from_model(model: &str) -> Result<Self, ApiError> {
        Self::from_model_with_anthropic_auth(model, None)
    }

    pub fn from_model_with_http(model: &str, http: reqwest::Client) -> Result<Self, ApiError> {
        Self::from_model_with_anthropic_auth_and_http(model, None, http)
    }

    pub fn from_model_with_anthropic_auth(
        model: &str,
        anthropic_auth: Option<AuthSource>,
    ) -> Result<Self, ApiError> {
        Self::from_model_with_anthropic_auth_and_http(
            model,
            anthropic_auth,
            crate::http_client::build_http_client()?,
        )
    }

    fn from_model_with_anthropic_auth_and_http(
        model: &str,
        anthropic_auth: Option<AuthSource>,
        http: reqwest::Client,
    ) -> Result<Self, ApiError> {
        let resolved_model = model.trim();
        if providers::requires_configured_provider_route(resolved_model) {
            return Err(ApiError::NoProviderConfigured {
                model: resolved_model.to_string(),
            });
        }
        match providers::detect_provider_kind(resolved_model) {
            ProviderKind::Anthropic => Ok(Self::Anthropic(match anthropic_auth {
                Some(auth) => AnthropicClient::from_auth_with_http(auth, http),
                None => {
                    AnthropicClient::from_auth_with_http(AuthSource::from_env_or_saved()?, http)
                        .with_base_url(anthropic::read_base_url())
                }
            })),
            ProviderKind::Xai => {
                let config = OpenAiCompatConfig::xai();
                let Some(api_key) = openai_compat::read_env_api_key(config)? else {
                    return Err(ApiError::missing_credentials(
                        config.provider_name,
                        config.credential_env_vars(),
                    ));
                };
                Ok(Self::Xai(OpenAiCompatClient::new_with_http(
                    api_key, config, http,
                )))
            }
            ProviderKind::OpenAi => {
                let config = match providers::metadata_for_model(resolved_model) {
                    Some(meta) if meta.auth_env == "DEEPSEEK_API_KEY" => {
                        OpenAiCompatConfig::deepseek()
                    }
                    Some(meta) if meta.auth_env == "MOONSHOT_API_KEY" => {
                        OpenAiCompatConfig::moonshot()
                    }
                    _ => OpenAiCompatConfig::openai(),
                };
                let config = if ProviderProtocol::detect(
                    "env",
                    config.default_base_url,
                    &[resolved_model.to_string()],
                ) == ProviderProtocol::Responses
                {
                    config.with_wire_protocol(OpenAiWireProtocol::Responses)
                } else {
                    config
                };
                let Some(api_key) = openai_compat::read_env_api_key(config)? else {
                    return Err(ApiError::missing_credentials(
                        config.provider_name,
                        config.credential_env_vars(),
                    ));
                };
                Ok(Self::OpenAi(OpenAiCompatClient::new_with_http(
                    api_key, config, http,
                )))
            }
        }
    }

    /// 从配置文件 ProviderConfig 直接构造，不读任何环境变量。
    pub fn from_config(provider: &ProviderConfig) -> Result<Self, ApiError> {
        Self::from_config_with_http(provider, crate::http_client::build_http_client()?)
    }

    pub fn from_config_with_http(
        provider: &ProviderConfig,
        http: reqwest::Client,
    ) -> Result<Self, ApiError> {
        let protocol = ProviderProtocol::effective_for_provider(provider).map_err(|reason| {
            ApiError::InvalidProviderConfig {
                provider: provider.name.clone(),
                reason,
            }
        })?;
        Self::from_config_with_effective_protocol_and_http(provider, protocol, http)
    }

    /// 从配置直接构造，不读任何环境变量。
    pub fn from_config_with_effective_protocol(
        provider: &ProviderConfig,
        protocol: ProviderProtocol,
    ) -> Result<Self, ApiError> {
        Self::from_config_with_effective_protocol_and_http(
            provider,
            protocol,
            crate::http_client::build_http_client()?,
        )
    }

    pub fn from_config_with_effective_protocol_and_http(
        provider: &ProviderConfig,
        protocol: ProviderProtocol,
        http: reqwest::Client,
    ) -> Result<Self, ApiError> {
        let api_key = resolve_config_api_key(provider)?;
        match protocol {
            ProviderProtocol::Anthropic => {
                let auth = AuthSource::ApiKey(api_key);
                Ok(Self::Anthropic(
                    AnthropicClient::from_auth_with_http(auth, http)
                        .with_base_url(&provider.base_url),
                ))
            }
            ProviderProtocol::Completions | ProviderProtocol::Responses => {
                let url = Self::normalize_openai_url(&provider.base_url);
                let wire_protocol = match protocol {
                    ProviderProtocol::Completions => OpenAiWireProtocol::Completions,
                    ProviderProtocol::Responses => OpenAiWireProtocol::Responses,
                    ProviderProtocol::Anthropic => {
                        return Err(ApiError::InvalidProviderConfig {
                            provider: provider.name.clone(),
                            reason: "Anthropic protocol must use the Anthropic client".to_string(),
                        });
                    }
                };
                Ok(Self::OpenAi(
                    OpenAiCompatClient::new_custom_with_protocol_and_http(
                        api_key,
                        url,
                        &provider.name,
                        wire_protocol,
                        http,
                    ),
                ))
            }
        }
    }

    /// 规范化 OpenAI 兼容 API 的 base URL。
    /// 只有裸主机（无路径）才追加 `/v1`；带显式 API 路径的 base URL
    /// （如 `/compatible-mode/v1`、`/api/paas/v4`）被视为完整 API 根，
    /// 追加 `/v1` 会破坏路径（例如 zhipu `/api/paas/v4` 变成
    /// `/v4/v1` 导致 404）。
    fn normalize_openai_url(base_url: &str) -> String {
        let trimmed = base_url.trim_end_matches('/');
        if trimmed.ends_with("/v1")
            || trimmed.ends_with("/responses")
            || trimmed.ends_with("/chat/completions")
        {
            return trimmed.to_string();
        }
        let after_scheme = trimmed.split_once("://").map_or(trimmed, |(_, rest)| rest);
        if after_scheme.contains('/') {
            trimmed.to_string()
        } else {
            format!("{trimmed}/v1")
        }
    }

    #[must_use]
    pub const fn provider_kind(&self) -> ProviderKind {
        match self {
            Self::Anthropic(_) => ProviderKind::Anthropic,
            Self::Xai(_) => ProviderKind::Xai,
            Self::OpenAi(_) => ProviderKind::OpenAi,
        }
    }

    pub async fn send_message(
        &self,
        request: &MessageRequest,
    ) -> Result<MessageResponse, ApiError> {
        match self {
            Self::Anthropic(client) => client.send_message(request).await,
            Self::Xai(client) | Self::OpenAi(client) => client.send_message(request).await,
        }
    }

    pub async fn stream_message(
        &self,
        request: &MessageRequest,
    ) -> Result<MessageStream, ApiError> {
        match self {
            Self::Anthropic(client) => client
                .stream_message(request)
                .await
                .map(MessageStream::Anthropic),
            Self::Xai(client) | Self::OpenAi(client) => client
                .stream_message(request)
                .await
                .map(MessageStream::OpenAiCompat),
        }
    }

    pub fn prepare_wire(&self, request: &MessageRequest) -> Result<PreparedProviderWire, ApiError> {
        match self {
            Self::Anthropic(client) => client.prepare_wire(request),
            Self::Xai(client) | Self::OpenAi(client) => client.prepare_wire(request),
        }
    }
    pub async fn stream_prepared_message(
        &self,
        request: &MessageRequest,
        prepared: &PreparedProviderWire,
    ) -> Result<MessageStream, ApiError> {
        match self {
            Self::Anthropic(client) => client
                .stream_prepared_message(request, prepared)
                .await
                .map(MessageStream::Anthropic),
            Self::Xai(client) | Self::OpenAi(client) => client
                .stream_prepared_message(request, prepared)
                .await
                .map(MessageStream::OpenAiCompat),
        }
    }
    pub async fn send_prepared_message(
        &self,
        request: &MessageRequest,
        prepared: &PreparedProviderWire,
    ) -> Result<MessageResponse, ApiError> {
        match self {
            Self::Anthropic(client) => client.send_prepared_message(request, prepared).await,
            Self::Xai(client) | Self::OpenAi(client) => {
                client.send_prepared_message(request, prepared).await
            }
        }
    }

    pub fn wire_request(&self, request: &MessageRequest) -> Result<ProviderWireRequest, ApiError> {
        match self {
            Self::Anthropic(client) => client.wire_request(request),
            Self::Xai(client) | Self::OpenAi(client) => client.wire_request(request),
        }
    }
}

#[derive(Debug)]
pub enum MessageStream {
    Anthropic(anthropic::MessageStream),
    OpenAiCompat(openai_compat::MessageStream),
}

impl MessageStream {
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Anthropic(stream) => stream.request_id(),
            Self::OpenAiCompat(stream) => stream.request_id(),
        }
    }

    pub fn set_transport_activity(&mut self, activity: crate::TransportActivity) {
        match self {
            Self::Anthropic(stream) => stream.set_transport_activity(activity),
            Self::OpenAiCompat(stream) => stream.set_transport_activity(activity),
        }
    }

    pub async fn next_event(&mut self) -> Result<Option<StreamEvent>, ApiError> {
        match self {
            Self::Anthropic(stream) => stream.next_event().await,
            Self::OpenAiCompat(stream) => stream.next_event().await,
        }
    }
}

pub use anthropic::{
    oauth_token_is_expired, resolve_saved_oauth_token, resolve_startup_auth_source, OAuthTokenSet,
};
#[must_use]
pub fn read_base_url() -> String {
    anthropic::read_base_url()
}

#[must_use]
pub fn read_xai_base_url() -> String {
    openai_compat::read_base_url(OpenAiCompatConfig::xai())
}

/// Resolve a provider credential without copying a secret into an isolated
/// configuration file. `api_key: env:NAME` is intentionally opt-in and is
/// resolved only at the final transport boundary; literal values retain their
/// existing behavior.
fn resolve_config_api_key(provider: &ProviderConfig) -> Result<String, ApiError> {
    let Some(variable) = provider.api_key.strip_prefix("env:") else {
        return Ok(provider.api_key.clone());
    };
    if variable.is_empty()
        || !variable
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ApiError::InvalidProviderConfig {
            provider: provider.name.clone(),
            reason: "api_key env reference must use `env:UPPERCASE_NAME`".to_string(),
        });
    }
    std::env::var(variable)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::InvalidProviderConfig {
            provider: provider.name.clone(),
            reason: format!("api_key environment variable `{variable}` is unavailable or empty"),
        })
}

#[cfg(test)]
mod tests {
    use super::{resolve_config_api_key, ProviderClient};
    use crate::error::ApiError;
    use crate::providers::{detect_provider_kind, ProviderKind};
    use crate::test_utils::{env_lock, EnvVarGuard};
    use model_protocol::provider_config::{
        EarlyToolStartMode, ParallelToolCallsMode, ProviderConfig,
    };

    fn configured_provider(api_key: &str) -> ProviderConfig {
        ProviderConfig {
            base_url: "https://provider.example/v1".to_string(),
            api_key: api_key.to_string(),
            models: vec!["test-model".to_string()],
            name: "test-provider".to_string(),
            protocol: Some("completions".to_string()),
            parallel_tool_calls: ParallelToolCallsMode::Auto,
            early_tool_start: EarlyToolStartMode::Auto,
        }
    }

    #[test]
    fn provider_detection_prefers_model_family() {
        assert_eq!(detect_provider_kind("grok-3"), ProviderKind::Xai);
        assert_eq!(
            detect_provider_kind("claude-sonnet-4-6"),
            ProviderKind::Anthropic
        );
    }

    #[test]
    fn qwen_model_requires_configured_provider_route() {
        let _lock = env_lock();
        let _openai = EnvVarGuard::set("OPENAI_API_KEY", Some("ambient-test-key"));

        let error = ProviderClient::from_model("qwen-plus")
            .expect_err("Qwen must not infer a provider from a model name or ambient credential");

        assert!(matches!(error, ApiError::NoProviderConfigured { .. }));
    }

    #[test]
    fn configured_qwen_provider_does_not_need_ambient_provider_routing() {
        let configured = ProviderConfig {
            models: vec!["qwen3.8-max".to_string()],
            ..configured_provider("configured-test-secret")
        };

        let client = ProviderClient::from_config(&configured)
            .expect("the configured provider registry is the only valid Qwen route");

        assert_eq!(client.provider_kind(), ProviderKind::OpenAi);
    }

    #[test]
    fn configured_provider_api_key_can_reference_a_process_environment_secret() {
        let _lock = env_lock();
        let _secret = EnvVarGuard::set("COWD_PROVIDER_TEST_SECRET", Some("test-secret"));

        let key = resolve_config_api_key(&configured_provider("env:COWD_PROVIDER_TEST_SECRET"))
            .expect("environment credential should resolve at transport composition");

        assert_eq!(key, "test-secret");
    }

    #[test]
    fn configured_provider_api_key_environment_reference_fails_closed() {
        let _lock = env_lock();
        let _missing = EnvVarGuard::set("COWD_PROVIDER_TEST_SECRET", None);

        let error = resolve_config_api_key(&configured_provider("env:COWD_PROVIDER_TEST_SECRET"))
            .expect_err("missing environment credential must not construct a client");

        assert!(error.to_string().contains("unavailable or empty"));
    }

    #[test]
    fn openai_base_url_normalization_keeps_explicit_api_paths() {
        assert_eq!(
            ProviderClient::normalize_openai_url("https://api.example.com"),
            "https://api.example.com/v1"
        );
        assert_eq!(
            ProviderClient::normalize_openai_url("https://api.example.com/"),
            "https://api.example.com/v1"
        );
        assert_eq!(
            ProviderClient::normalize_openai_url("https://api.example.com/v1"),
            "https://api.example.com/v1"
        );
        assert_eq!(
            ProviderClient::normalize_openai_url("https://open.bigmodel.cn/api/paas/v4"),
            "https://open.bigmodel.cn/api/paas/v4"
        );
        assert_eq!(
            ProviderClient::normalize_openai_url("https://provider.example/compatible-mode/v1"),
            "https://provider.example/compatible-mode/v1"
        );
    }
}

#[cfg(test)]
mod prepared_wire_tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn each_protocol_sends_the_exact_prepared_bytes_and_completes_without_count_api() {
        for protocol in ["completion", "responses", "anthropic"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let body = match protocol {
                "completion" => {
                    r#"{"id":"c","object":"chat.completion","model":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
                }
                "responses" => {
                    r#"{"id":"r","object":"response","status":"completed","model":"fixture","output":[{"id":"m","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}"#
                }
                _ => {
                    r#"{"id":"m","type":"message","role":"assistant","model":"fixture","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}"#
                }
            };
            let server = tokio::spawn(async move {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(2), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut raw = Vec::new();
                let bytes = loop {
                    let mut block = [0_u8; 4096];
                    let count = socket.read(&mut block).await.unwrap();
                    assert!(count > 0 && raw.len() < 256 * 1024);
                    raw.extend_from_slice(&block[..count]);
                    if let Some(offset) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&raw[..offset]);
                        assert!(!header.contains("count_tokens"));
                        let len = header
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|n| n.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if raw.len() >= offset + 4 + len {
                            break raw[offset + 4..offset + 4 + len].to_vec();
                        }
                    }
                };
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                bytes
            });
            let client = if protocol == "anthropic" {
                ProviderClient::Anthropic(
                    AnthropicClient::new("synthetic-wire-canary")
                        .with_base_url(format!("http://{address}")),
                )
            } else {
                ProviderClient::OpenAi(OpenAiCompatClient::new_custom_with_protocol(
                    "synthetic-wire-canary",
                    format!("http://{address}/v1"),
                    "fixture",
                    if protocol == "completion" {
                        OpenAiWireProtocol::Completions
                    } else {
                        OpenAiWireProtocol::Responses
                    },
                ))
            }
            .without_retries();
            let request = MessageRequest {
                model: "fixture".into(),
                max_tokens: 32,
                system: Some("system".into()),
                messages: vec![crate::types::InputMessage::user_text("payload")],
                ..Default::default()
            };
            let prepared = client.prepare_wire(&request).unwrap();
            let response = tokio::time::timeout(
                Duration::from_secs(2),
                client.send_prepared_message(&request, &prepared),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(response.content.iter().any(
                |part| matches!(part,crate::types::OutputContentBlock::Text {text} if text=="ok")
            ));
            let actual = server.await.unwrap();
            assert_eq!(actual.len(), prepared.body_len());
            assert_eq!(
                format!("{:x}", Sha256::digest(&actual)),
                prepared.wire_request().body_sha256
            );
            let actual_json: Value = serde_json::from_slice(&actual).unwrap();
            assert_eq!(actual_json["model"], "fixture");
            assert!(String::from_utf8(actual.clone())
                .unwrap()
                .contains("payload"));
            assert_eq!(actual_json, prepared.wire_request().body);
            assert!(!format!("{prepared:?}").contains("synthetic-wire-canary"));
        }
    }

    #[tokio::test]
    async fn prepared_wire_rejects_mutated_source_transport_metadata_and_foreign_route_before_http()
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = ProviderClient::OpenAi(OpenAiCompatClient::new_custom_with_protocol(
            "synthetic",
            format!("http://{address}/v1"),
            "fixture",
            OpenAiWireProtocol::Completions,
        ))
        .without_retries();
        let request = MessageRequest {
            model: "fixture".into(),
            max_tokens: 32,
            messages: vec![crate::types::InputMessage::user_text("original")],
            ..Default::default()
        };
        let prepared = client.prepare_wire(&request).unwrap();
        {
            let mut changed = request.clone();
            changed
                .protocol_capabilities
                .push("changed-transport-capability".into());
            let outcome = tokio::time::timeout(
                Duration::from_millis(500),
                client.send_prepared_message(&changed, &prepared),
            )
            .await
            .unwrap();
            assert!(outcome.is_err());
        }
        let mut changed = request.clone();
        changed.system_cache_prefix = Some("changed-skipped-source-field".into());
        assert!(client
            .send_prepared_message(&changed, &prepared)
            .await
            .is_err());
        let mut changed = request.clone();
        changed.messages[0] = crate::types::InputMessage::user_text("foreign business input");
        assert!(client
            .send_prepared_message(&changed, &prepared)
            .await
            .is_err());
        let foreign = ProviderClient::OpenAi(OpenAiCompatClient::new_custom_with_protocol(
            "synthetic",
            format!("http://{address}/other"),
            "fixture",
            OpenAiWireProtocol::Completions,
        ))
        .without_retries();
        assert!(foreign
            .send_prepared_message(&request, &prepared)
            .await
            .is_err());
        let foreign_account = ProviderClient::OpenAi(OpenAiCompatClient::new_custom_with_protocol(
            "other-synthetic-account",
            format!("http://{address}/v1"),
            "fixture",
            OpenAiWireProtocol::Completions,
        ))
        .without_retries();
        assert!(foreign_account
            .send_prepared_message(&request, &prepared)
            .await
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn prepared_body_refuses_overflow_without_growing_past_its_bound() {
        use std::io::Write;
        let mut body = BoundedWireBody(Vec::new());
        let block = vec![b'x'; 1024 * 1024];
        for _ in 0..16 {
            body.write_all(&block).unwrap();
        }
        assert_eq!(body.0.len(), 16 * 1024 * 1024);
        assert!(body.0.capacity() <= 16 * 1024 * 1024);
        assert!(body.write_all(b"extra").is_err());
        assert_eq!(body.0.len(), 16 * 1024 * 1024);
    }
}
