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

pub(crate) fn build_provider_wire_request(
    endpoint: String,
    protocol: &str,
    headers: Vec<ProviderWireHeader>,
    body: Value,
    tools: Option<&[crate::types::ToolDefinition]>,
) -> Result<ProviderWireRequest, ApiError> {
    let body_bytes =
        serde_json::to_vec(&body).map_err(|error| ApiError::InvalidProviderConfig {
            provider: "wire-evidence".to_string(),
            reason: format!("failed to serialize provider wire request: {error}"),
        })?;
    let tool_schema_sha256 = tools
        .filter(|tools| !tools.is_empty())
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|error| ApiError::InvalidProviderConfig {
            provider: "wire-evidence".to_string(),
            reason: format!("failed to serialize provider tool schemas: {error}"),
        })?
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)));
    Ok(ProviderWireRequest {
        method: "POST".to_string(),
        endpoint: endpoint
            .split_once('?')
            .map_or(endpoint.as_str(), |(path, _)| path)
            .to_string(),
        protocol: protocol.to_string(),
        headers,
        body,
        body_sha256: format!("{:x}", Sha256::digest(body_bytes)),
        tool_schema_sha256,
    })
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ProviderClient {
    Anthropic(AnthropicClient),
    Xai(OpenAiCompatClient),
    OpenAi(OpenAiCompatClient),
}

impl ProviderClient {
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
                // DashScope models (qwen-*) also return ProviderKind::OpenAi because they
                // speak the OpenAI wire format, but they need the DashScope config which
                // reads DASHSCOPE_API_KEY and points at dashscope.aliyuncs.com.
                let config = match providers::metadata_for_model(resolved_model) {
                    Some(meta) if meta.auth_env == "DASHSCOPE_API_KEY" => {
                        OpenAiCompatConfig::dashscope()
                    }
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
        match protocol {
            ProviderProtocol::Anthropic => {
                let auth = AuthSource::ApiKey(provider.api_key.clone());
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
                        provider.api_key.clone(),
                        url,
                        &provider.name,
                        wire_protocol,
                        http,
                    ),
                ))
            }
        }
    }

    /// 规范化 OpenAI 兼容 API 的 base URL：确保以 /v1 结尾
    fn normalize_openai_url(base_url: &str) -> String {
        let trimmed = base_url.trim_end_matches('/');
        if trimmed.ends_with("/v1")
            || trimmed.ends_with("/responses")
            || trimmed.ends_with("/chat/completions")
        {
            trimmed.to_string()
        } else if base_url.ends_with('/') {
            format!("{base_url}v1")
        } else {
            format!("{base_url}/v1")
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

#[cfg(test)]
mod tests {
    use super::ProviderClient;
    use crate::providers::{detect_provider_kind, ProviderKind};
    use crate::test_utils::{env_lock, EnvVarGuard};

    #[test]
    fn provider_detection_prefers_model_family() {
        assert_eq!(detect_provider_kind("grok-3"), ProviderKind::Xai);
        assert_eq!(
            detect_provider_kind("claude-sonnet-4-6"),
            ProviderKind::Anthropic
        );
    }

    #[test]
    fn dashscope_model_uses_dashscope_config_not_openai() {
        let _lock = env_lock();
        let _dashscope = EnvVarGuard::set("DASHSCOPE_API_KEY", Some("test-dashscope-key"));
        let _openai = EnvVarGuard::set("OPENAI_API_KEY", None);

        let client = ProviderClient::from_model("qwen-plus");

        assert!(
            client.is_ok(),
            "qwen-plus with DASHSCOPE_API_KEY set should build successfully, got: {:?}",
            client.err()
        );

        match client.unwrap() {
            ProviderClient::OpenAi(openai_client) => {
                assert!(
                    openai_client.base_url().contains("dashscope.aliyuncs.com"),
                    "qwen-plus should route to DashScope base URL (contains 'dashscope.aliyuncs.com'), got: {}",
                    openai_client.base_url()
                );
            }
            other => panic!("Expected ProviderClient::OpenAi for qwen-plus, got: {other:?}"),
        }
    }
}
