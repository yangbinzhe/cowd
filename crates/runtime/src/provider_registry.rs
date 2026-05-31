//! Global provider registry backed by [`std::sync::OnceLock`].
//!
//! Providers are lazily loaded from `~/.cowd/config.yaml` on first access.
//! No explicit `init_global_providers` call is required.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::config::{ProviderConfig, ProvidersConfig};

static GLOBAL_PROVIDERS: OnceLock<ProvidersConfig> = OnceLock::new();

fn lazy_load() -> &'static ProvidersConfig {
    GLOBAL_PROVIDERS.get_or_init(|| {
        let mut providers = HashMap::new();

        if let Ok(home) = std::env::var("HOME") {
            let cfg = std::path::PathBuf::from(home).join(".cowd").join("config.yaml");
            if let Ok(raw) = std::fs::read_to_string(&cfg) {
                if let Ok(val) = serde_yaml::from_str::<serde_yaml::Value>(&raw) {
                    if let Some(mapping) = val.get("providers").and_then(|v| v.as_mapping()) {
                        for (key, value) in mapping {
                            let name = match key.as_str() {
                                Some(s) => s.to_string(),
                                None => continue,
                            };
                            let entry = match value.as_mapping() {
                                Some(m) => m,
                                None => continue,
                            };
                            let base_url = entry.get("base_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let api_key = entry.get("api_key").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let models: Vec<String> = entry.get("models")
                                .and_then(|v| v.as_sequence())
                                .map(|seq| seq.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                                .unwrap_or_default();
                            let protocol = entry.get("protocol").and_then(|v| v.as_str()).map(str::to_string);

                            providers.insert(name.clone(), ProviderConfig {
                                name: name.clone(),
                                base_url,
                                api_key,
                                models,
                                protocol,
                            });
                        }
                    }
                }
            }
        }

        ProvidersConfig { providers }
    })
}

/// Initialize the global provider registry explicitly.
/// Not required—the registry lazy-initializes on first query.
pub fn init_global_providers(config: ProvidersConfig) {
    let _ = GLOBAL_PROVIDERS.set(config);
}

/// Look up the [`ProviderConfig`] that owns `model`.
pub fn resolve_global_provider(model: &str) -> Option<&'static ProviderConfig> {
    lazy_load().resolve_full(model)
}

/// Return the names of all configured providers.
pub fn list_all_providers() -> Vec<&'static str> {
    lazy_load().providers.keys().map(String::as_str).collect()
}

/// Return every model name registered under the given provider.
pub fn list_models_for_provider(provider_name: &str) -> Vec<&'static str> {
    lazy_load().providers.get(provider_name)
        .map(|cfg| cfg.models.iter().map(String::as_str).collect())
        .unwrap_or_default()
}

/// Return every model name across all configured providers.
pub fn list_all_models() -> Vec<&'static str> {
    lazy_load().providers.values()
        .flat_map(|cfg| cfg.models.iter().map(String::as_str))
        .collect()
}
