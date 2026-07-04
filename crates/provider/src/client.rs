use crate::error::ApiError;
use crate::providers::anthropic::{self, AnthropicClient, AuthSource};
use crate::providers::openai_compat::{
    self, OpenAiCompatClient, OpenAiCompatConfig, OpenAiWireProtocol,
};
use crate::providers::{self, ProviderKind};
use crate::types::{MessageRequest, MessageResponse, StreamEvent};
use model_protocol::prompt_cache::{PromptCache, PromptCacheRecord, PromptCacheStats};
use model_protocol::provider_config::{ProviderConfig, ProviderProtocol};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ProviderClient {
    Anthropic(AnthropicClient),
    Xai(OpenAiCompatClient),
    OpenAi(OpenAiCompatClient),
}

impl ProviderClient {
    pub fn from_model(model: &str) -> Result<Self, ApiError> {
        Self::from_model_with_anthropic_auth(model, None)
    }

    pub fn from_model_with_anthropic_auth(
        model: &str,
        anthropic_auth: Option<AuthSource>,
    ) -> Result<Self, ApiError> {
        let resolved_model = model.trim();
        match providers::detect_provider_kind(&resolved_model) {
            ProviderKind::Anthropic => Ok(Self::Anthropic(match anthropic_auth {
                Some(auth) => AnthropicClient::from_auth(auth),
                None => AnthropicClient::from_env()?,
            })),
            ProviderKind::Xai => Ok(Self::Xai(OpenAiCompatClient::from_env(
                OpenAiCompatConfig::xai(),
            )?)),
            ProviderKind::OpenAi => {
                // DashScope models (qwen-*) also return ProviderKind::OpenAi because they
                // speak the OpenAI wire format, but they need the DashScope config which
                // reads DASHSCOPE_API_KEY and points at dashscope.aliyuncs.com.
                let config = match providers::metadata_for_model(&resolved_model) {
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
                Ok(Self::OpenAi(OpenAiCompatClient::from_env(config)?))
            }
        }
    }

    /// 从配置文件 ProviderConfig 直接构造，不读任何环境变量。
    pub fn from_config(provider: &ProviderConfig) -> Result<Self, ApiError> {
        let protocol = ProviderProtocol::effective_for_provider(provider).map_err(|reason| {
            ApiError::InvalidProviderConfig {
                provider: provider.name.clone(),
                reason,
            }
        })?;
        Self::from_config_with_effective_protocol(provider, protocol)
    }

    /// 从配置直接构造，不读任何环境变量。
    pub fn from_config_with_effective_protocol(
        provider: &ProviderConfig,
        protocol: ProviderProtocol,
    ) -> Result<Self, ApiError> {
        match protocol {
            ProviderProtocol::Anthropic => {
                let auth = AuthSource::ApiKey(provider.api_key.clone());
                Ok(Self::Anthropic(
                    AnthropicClient::from_auth(auth).with_base_url(&provider.base_url),
                ))
            }
            ProviderProtocol::Completions | ProviderProtocol::Responses => {
                let url = Self::normalize_openai_url(&provider.base_url);
                let wire_protocol = match protocol {
                    ProviderProtocol::Completions => OpenAiWireProtocol::Completions,
                    ProviderProtocol::Responses => OpenAiWireProtocol::Responses,
                    ProviderProtocol::Anthropic => unreachable!("handled above"),
                };
                Ok(Self::OpenAi(OpenAiCompatClient::new_custom_with_protocol(
                    provider.api_key.clone(),
                    url,
                    &provider.name,
                    wire_protocol,
                )))
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

    #[must_use]
    pub fn with_prompt_cache(self, prompt_cache: PromptCache) -> Self {
        match self {
            Self::Anthropic(client) => Self::Anthropic(client.with_prompt_cache(prompt_cache)),
            other => other,
        }
    }

    #[must_use]
    pub fn prompt_cache_stats(&self) -> Option<PromptCacheStats> {
        match self {
            Self::Anthropic(client) => client.prompt_cache_stats(),
            Self::Xai(_) | Self::OpenAi(_) => None,
        }
    }

    #[must_use]
    pub fn take_last_prompt_cache_record(&self) -> Option<PromptCacheRecord> {
        match self {
            Self::Anthropic(client) => client.take_last_prompt_cache_record(),
            Self::Xai(_) | Self::OpenAi(_) => None,
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
