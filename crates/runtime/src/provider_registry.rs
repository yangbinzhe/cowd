//! Global provider registry backed by [`std::sync::RwLock`].
//!
//! Providers are lazily loaded from `config.yaml` (resolved via [`crate::cowd_dirs::config_home_dir()`]).
//! No explicit `init_global_providers` call is required.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::config::{ProviderConfig, ProvidersConfig};

static GLOBAL_PROVIDERS: RwLock<Option<ProvidersConfig>> = RwLock::new(None);

fn lazy_load() -> &'static ProvidersConfig {
    // Fast path: read-lock, check if loaded
    {
        let guard = GLOBAL_PROVIDERS.read().unwrap();
        if let Some(ref cfg) = *guard {
            // SAFETY: ProvidersConfig is never mutated after initial load.
            // We return a reference into the static RwLock. This is safe
            // because (1) the RwLock lives forever, (2) after the first write,
            // the value is never replaced (except by test reset).
            let ptr: *const ProvidersConfig = cfg;
            return unsafe { &*ptr };
        }
    }
    // Slow path: write-lock, check again, load from disk
    let mut guard = GLOBAL_PROVIDERS.write().unwrap();
    if guard.is_none() {
        // Load from config.yaml (existing loading code)
        let mut providers = HashMap::new();
        let cfg = crate::cowd_dirs::config_home_dir().join("config.yaml");
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
        *guard = Some(ProvidersConfig { providers });
    }
    // After write lock, we know it's initialized. Return a reference.
    let cfg = guard.as_ref().unwrap();
    let ptr: *const ProvidersConfig = cfg;
    unsafe { &*ptr }
}

/// Initialize the global provider registry explicitly.
/// Not required—the registry lazy-initializes on first query.
pub fn init_global_providers(config: ProvidersConfig) {
    let mut guard = GLOBAL_PROVIDERS.write().unwrap();
    if guard.is_none() {
        *guard = Some(config);
    }
}

/// Test-only API. Sets provider map directly without loading from config.
/// Public because integration tests in other crates need it.
pub fn set_test_providers(map: std::collections::HashMap<String, ProviderConfig>) {
    let mut guard = GLOBAL_PROVIDERS.write().unwrap();
    *guard = Some(ProvidersConfig { providers: map });
}

/// Test-only API. Resets the global provider registry to uninitialized.
/// Public because integration tests in other crates need it.
pub fn reset_for_test() {
    let mut guard = GLOBAL_PROVIDERS.write().unwrap();
    *guard = None;
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
