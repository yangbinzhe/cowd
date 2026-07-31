use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::provider_capability::{CapabilityState, ProviderCapabilityProfile};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelToolCallsMode {
    #[default]
    Auto,
    Enabled,
    Disabled,
}

impl ParallelToolCallsMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }

    pub fn effective_request(
        self,
        capabilities: &ProviderCapabilityProfile,
    ) -> Result<Option<bool>, String> {
        let request_support = capabilities.supports_parallel_tool_calls_request.state;
        match (self, request_support) {
            (Self::Auto, CapabilityState::Supported) => Ok(Some(true)),
            (Self::Auto, CapabilityState::Unsupported | CapabilityState::Unknown) => Ok(None),
            (Self::Enabled, CapabilityState::Supported) => Ok(Some(true)),
            (Self::Disabled, CapabilityState::Supported) => Ok(Some(false)),
            (Self::Enabled | Self::Disabled, state) => Err(format!(
                "parallel_tool_calls={}: provider request capability is {state:?}",
                self.as_str()
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EarlyToolStartMode {
    #[default]
    Auto,
    Enabled,
    Disabled,
}

impl EarlyToolStartMode {
    pub const VERIFIED_CAPABILITY: &'static str = "early_tool_overlap_verified";

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }

    /// `auto` fails closed until the model catalog carries evidence from the
    /// paired early-overlap performance gate. Operators may opt in explicitly
    /// while collecting that evidence.
    #[must_use]
    pub fn effective(self, model: &str) -> bool {
        match self {
            Self::Enabled => true,
            Self::Disabled => false,
            Self::Auto => crate::model_registry::global_registry()
                .capacity_model_info(model)
                .is_some_and(|info| {
                    info.capabilities
                        .iter()
                        .any(|value| value == Self::VERIFIED_CAPABILITY)
                }),
        }
    }
}

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
    /// Provider-side multiple-tool proposal request. This never changes
    /// Runtime's independent local execution concurrency policy.
    pub parallel_tool_calls: ParallelToolCallsMode,
    /// Provider/model-specific gate for starting descriptor-proven read-only
    /// tools before the current response reaches its terminal frame.
    pub early_tool_start: EarlyToolStartMode,
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
    /// Returns `None` if no provider claims the model. Callers decide whether
    /// that is a configuration error; Gateway Runtime treats it as one and
    /// never infers a provider route from process environment variables.
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
    use super::{EarlyToolStartMode, ParallelToolCallsMode, ProviderConfig, ProviderProtocol};
    use crate::provider_capability::ProviderCapabilityProfile;

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
            parallel_tool_calls: Default::default(),
            early_tool_start: Default::default(),
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
    fn parallel_tool_request_mode_fails_closed_for_unsupported_protocols() {
        let anthropic =
            ProviderCapabilityProfile::resolve(ProviderProtocol::Anthropic, "claude-test");
        assert_eq!(
            ParallelToolCallsMode::Auto
                .effective_request(&anthropic)
                .expect("auto may omit unsupported hints"),
            None
        );
        assert!(ParallelToolCallsMode::Enabled
            .effective_request(&anthropic)
            .is_err());

        let responses = ProviderCapabilityProfile::resolve(ProviderProtocol::Responses, "gpt-test");
        assert_eq!(
            ParallelToolCallsMode::Disabled
                .effective_request(&responses)
                .expect("responses supports explicit disable"),
            Some(false)
        );
    }

    #[test]
    fn early_tool_start_auto_fails_closed_without_performance_evidence() {
        assert!(!EarlyToolStartMode::Auto.effective("unregistered-model"));
        assert!(EarlyToolStartMode::Enabled.effective("unregistered-model"));
        assert!(!EarlyToolStartMode::Disabled.effective("unregistered-model"));
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
