use std::collections::HashMap;

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
    /// Optional protocol override: `"anthropic"` or `"openai-compat"` (default).
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
