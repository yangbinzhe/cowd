use std::collections::HashMap;

/// Canonical model provider wire protocols supported by Cowd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderProtocol {
    /// Anthropic Messages API: `/v1/messages`.
    Anthropic,
    /// OpenAI-compatible Chat Completions API: `/chat/completions`.
    Completions,
    /// OpenAI Responses API: `/responses`.
    Responses,
}

impl ProviderProtocol {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::Completions => "completions",
            Self::Responses => "responses",
        }
    }

    /// Parse a configured protocol value.
    ///
    /// Valid values are `anthropic`, `completions`, or `responses`.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match normalize_protocol_name(value).as_str() {
            "anthropic" => Some(Self::Anthropic),
            "completions" => Some(Self::Completions),
            "responses" => Some(Self::Responses),
            _ => None,
        }
    }

    /// Return the configured protocol or infer one without doing network I/O.
    pub fn effective_for_provider(provider: &ProviderConfig) -> Result<Self, String> {
        if let Some(configured) = provider.protocol.as_deref().map(str::trim) {
            if configured.is_empty() {
                return Ok(Self::detect(
                    &provider.name,
                    &provider.base_url,
                    &provider.models,
                ));
            }
            return Self::parse(configured).ok_or_else(|| {
                format!(
                    "unsupported protocol '{configured}'. Valid values: \"anthropic\", \"completions\", \"responses\""
                )
            });
        }
        Ok(Self::detect(
            &provider.name,
            &provider.base_url,
            &provider.models,
        ))
    }

    /// Deterministic local protocol detection used when `protocol` is omitted.
    ///
    /// This deliberately avoids startup network probes: selecting a provider
    /// protocol must not consume tokens, block Gateway boot, or mutate upstream
    /// state. Explicit config always wins over this detector.
    #[must_use]
    pub fn detect(provider_name: &str, base_url: &str, models: &[String]) -> Self {
        let provider_name = provider_name.to_ascii_lowercase();
        let base_url = base_url.to_ascii_lowercase();
        if provider_name.contains("anthropic")
            || provider_name.contains("claude")
            || base_url.contains("anthropic.com")
            || models.iter().any(|model| model_starts(model, "claude"))
        {
            return Self::Anthropic;
        }
        if base_url.trim_end_matches('/').ends_with("/responses")
            || models.iter().any(|model| model_starts(model, "gpt-5"))
        {
            return Self::Responses;
        }
        Self::Completions
    }
}

fn normalize_protocol_name(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

fn model_starts(model: &str, prefix: &str) -> bool {
    let model = model.trim().to_ascii_lowercase();
    let canonical = model
        .rsplit_once('/')
        .map_or(model.as_str(), |(_, rest)| rest);
    canonical.starts_with(prefix)
}

/// Configuration for a single named provider endpoint.
///
/// Each provider has its own `base_url` and `api_key`, and declares the list
/// of model IDs it serves. When a model is requested, [`ProvidersConfig::resolve`]
/// searches this list to locate the matching provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    /// Base URL for the provider's OpenAI-compatible or Anthropic API.
    pub base_url: String,
    /// API key or bearer token for authenticating with this provider.
    pub api_key: String,
    /// List of model IDs served by this provider.
    pub models: Vec<String>,
    /// Short name identifying this provider entry.
    pub name: String,
    /// Optional protocol override: `"anthropic"`, `"completions"`, or `"responses"`.
    ///
    /// When absent, Cowd infers the protocol locally from provider name, base
    /// URL, and model names.
    pub protocol: Option<String>,
}

/// Named collection of provider configurations.
///
/// Providers are keyed by a short name. Use [`ProvidersConfig::resolve`] to
/// look up the `(base_url, api_key)` pair for a given model name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProvidersConfig {
    pub providers: HashMap<String, ProviderConfig>,
}

impl ProvidersConfig {
    /// Returns `true` if no providers are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Resolves a model name to its provider's `(base_url, api_key)` pair.
    ///
    /// Returns `None` if no provider claims the model; callers should then
    /// fall back to environment variables.
    #[must_use]
    pub fn resolve(&self, model_name: &str) -> Option<(&str, &str)> {
        for provider in self.providers.values() {
            if provider.models.iter().any(|m| m == model_name) {
                return Some((&provider.base_url, &provider.api_key));
            }
        }
        None
    }

    /// Resolves a model name to the full [`ProviderConfig`].
    #[must_use]
    pub fn resolve_full(&self, model: &str) -> Option<&ProviderConfig> {
        self.providers
            .values()
            .find(|p| p.models.iter().any(|m| m == model))
    }

    /// Returns the named provider if it exists.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ProviderConfig> {
        self.providers.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{ProviderConfig, ProviderProtocol};

    fn provider(
        name: &str,
        base_url: &str,
        models: &[&str],
        protocol: Option<&str>,
    ) -> ProviderConfig {
        ProviderConfig {
            name: name.to_string(),
            base_url: base_url.to_string(),
            api_key: "sk-test".to_string(),
            models: models.iter().map(|model| (*model).to_string()).collect(),
            protocol: protocol.map(str::to_string),
        }
    }

    #[test]
    fn provider_protocol_parses_only_canonical_values() {
        assert_eq!(
            ProviderProtocol::parse("anthropic"),
            Some(ProviderProtocol::Anthropic)
        );
        assert_eq!(
            ProviderProtocol::parse("completions"),
            Some(ProviderProtocol::Completions)
        );
        assert_eq!(
            ProviderProtocol::parse("responses"),
            Some(ProviderProtocol::Responses)
        );
        assert_eq!(ProviderProtocol::parse("openai-compat"), None);
        assert_eq!(ProviderProtocol::parse("openai_chat_completions"), None);
        assert_eq!(ProviderProtocol::parse("gemini-native"), None);
    }

    #[test]
    fn configured_protocol_wins_over_detection() {
        let cfg = provider(
            "openai",
            "https://api.openai.com/v1",
            &["gpt-5"],
            Some("completions"),
        );
        assert_eq!(
            ProviderProtocol::effective_for_provider(&cfg).unwrap(),
            ProviderProtocol::Completions
        );
    }

    #[test]
    fn missing_protocol_is_detected_without_network() {
        assert_eq!(
            ProviderProtocol::effective_for_provider(&provider(
                "anthropic",
                "https://api.anthropic.com",
                &["claude-sonnet-4-6"],
                None,
            ))
            .unwrap(),
            ProviderProtocol::Anthropic
        );
        assert_eq!(
            ProviderProtocol::effective_for_provider(&provider(
                "openai",
                "https://api.openai.com/v1",
                &["gpt-5"],
                None,
            ))
            .unwrap(),
            ProviderProtocol::Responses
        );
        assert_eq!(
            ProviderProtocol::effective_for_provider(&provider(
                "deepseek",
                "https://api.deepseek.com/v1",
                &["deepseek-v4-pro"],
                None,
            ))
            .unwrap(),
            ProviderProtocol::Completions
        );
    }
}
