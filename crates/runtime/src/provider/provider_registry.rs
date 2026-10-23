//! Runtime-host-owned provider configuration snapshots.
//!
//! A registry is scoped to one runtime host. Requests pin an immutable snapshot,
//! so a successful reload affects only requests that start after the revision is
//! published. Invalid replacements never mutate the last valid snapshot.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::config::{ProviderConfig, ProviderProtocol, ProvidersConfig};
use model_protocol::provider_capability::ProviderCapabilityProfile;
use model_protocol::provider_config::ParallelToolCallsMode;

#[derive(Debug, Clone)]
pub struct ProviderRegistrySnapshot {
    config: Arc<ProvidersConfig>,
    model_context_windows: Arc<BTreeMap<String, u32>>,
    revision: u64,
}

impl ProviderRegistrySnapshot {
    #[must_use]
    pub fn configured_context_window(&self, model: &str) -> Option<u32> {
        let resolution = self.context_window_resolution(model);
        (resolution.source == provider::ModelContextWindowSource::Configured)
            .then_some(resolution.tokens)
    }

    pub(crate) fn context_window_resolution(
        &self,
        model: &str,
    ) -> provider::ModelContextWindowResolution {
        provider::model_context_window_resolution(model, Some(&self.model_context_windows))
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn config(&self) -> &ProvidersConfig {
        &self.config
    }

    #[must_use]
    pub fn resolve(&self, model: &str) -> Option<&ProviderConfig> {
        self.config.resolve_full(model)
    }

    #[must_use]
    pub fn provider_names(&self) -> Vec<String> {
        let mut names = self.config.providers.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    #[must_use]
    pub fn provider_name_for_model(&self, model: &str) -> Option<String> {
        self.config.providers.iter().find_map(|(name, provider)| {
            provider
                .models
                .iter()
                .any(|candidate| candidate == model)
                .then(|| name.clone())
        })
    }

    #[must_use]
    pub fn models_for_provider(&self, provider_name: &str) -> Vec<String> {
        self.config
            .providers
            .get(provider_name)
            .map(|provider| provider.models.clone())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn all_models(&self) -> Vec<String> {
        let mut models = self
            .config
            .providers
            .values()
            .flat_map(|provider| provider.models.iter().cloned())
            .collect::<Vec<_>>();
        models.sort();
        models.dedup();
        models
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRegistryDiagnostics {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ProviderRegistryDiagnostics {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRegistryUpdate {
    pub changed: bool,
    pub previous_revision: u64,
    pub revision: u64,
    pub provider_count: usize,
    pub model_count: usize,
    pub diagnostics: ProviderRegistryDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRegistryRejected {
    pub retained_revision: u64,
    pub diagnostics: ProviderRegistryDiagnostics,
}

#[derive(Debug)]
struct ProviderConfiguration {
    config: Arc<ProvidersConfig>,
    model_context_windows: Arc<BTreeMap<String, u32>>,
}

#[derive(Debug)]
pub struct ProviderRegistry {
    snapshot: RwLock<ProviderConfiguration>,
    revision: AtomicU64,
}

impl ProviderRegistry {
    pub fn new(config: ProvidersConfig) -> Result<Self, ProviderRegistryRejected> {
        Self::new_with_context_windows(config, BTreeMap::new())
    }

    pub fn new_with_context_windows(
        config: ProvidersConfig,
        model_context_windows: BTreeMap<String, u32>,
    ) -> Result<Self, ProviderRegistryRejected> {
        let mut diagnostics = validate_provider_config(&config);
        validate_context_windows(&model_context_windows, &mut diagnostics);
        if !diagnostics.is_valid() {
            return Err(ProviderRegistryRejected {
                retained_revision: 0,
                diagnostics,
            });
        }
        Ok(Self {
            snapshot: RwLock::new(ProviderConfiguration {
                config: Arc::new(config), model_context_windows: Arc::new(model_context_windows),
            }),
            revision: AtomicU64::new(1),
        })
    }

    #[must_use]
    pub fn empty() -> Self {
        Self {
            snapshot: RwLock::new(ProviderConfiguration {
                config: Arc::new(ProvidersConfig::default()), model_context_windows: Arc::new(BTreeMap::new()),
            }),
            revision: AtomicU64::new(1),
        }
    }

    #[must_use]
    pub fn pin(&self) -> ProviderRegistrySnapshot {
        let guard = self
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revision = self.revision.load(Ordering::Acquire);
        let config = guard.config.clone();
        let model_context_windows = guard.model_context_windows.clone();
        ProviderRegistrySnapshot { config, model_context_windows, revision }
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    pub fn replace(
        &self,
        config: ProvidersConfig,
    ) -> Result<ProviderRegistryUpdate, ProviderRegistryRejected> {
        self.replace_inner(config, None)
    }

    pub fn replace_with_context_windows(
        &self,
        config: ProvidersConfig,
        model_context_windows: BTreeMap<String, u32>,
    ) -> Result<ProviderRegistryUpdate, ProviderRegistryRejected> {
        self.replace_inner(config, Some(model_context_windows))
    }

    fn replace_inner(
        &self,
        config: ProvidersConfig,
        model_context_windows: Option<BTreeMap<String, u32>>,
    ) -> Result<ProviderRegistryUpdate, ProviderRegistryRejected> {
        let mut diagnostics = validate_provider_config(&config);
        if let Some(windows) = model_context_windows.as_ref() {
            validate_context_windows(windows, &mut diagnostics);
        }
        if !diagnostics.is_valid() {
            return Err(ProviderRegistryRejected {
                retained_revision: self.revision(),
                diagnostics,
            });
        }

        let provider_count = config.providers.len();
        let model_count = config
            .providers
            .values()
            .map(|provider| provider.models.len())
            .sum();
        let mut guard = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_revision = self.revision.load(Ordering::Relaxed);
        let windows = model_context_windows.map(Arc::new).unwrap_or_else(|| guard.model_context_windows.clone());
        if *guard.config == config && *guard.model_context_windows == *windows {
            return Ok(ProviderRegistryUpdate {
                changed: false,
                previous_revision,
                revision: previous_revision,
                provider_count,
                model_count,
                diagnostics,
            });
        }
        *guard = ProviderConfiguration { config: Arc::new(config), model_context_windows: windows };
        let revision = previous_revision.saturating_add(1);
        self.revision.store(revision, Ordering::Release);

        Ok(ProviderRegistryUpdate {
            changed: true,
            previous_revision,
            revision,
            provider_count,
            model_count,
            diagnostics,
        })
    }
}

fn validate_context_windows(windows: &BTreeMap<String, u32>, diagnostics: &mut ProviderRegistryDiagnostics) {
    for (model, tokens) in windows {
        if model.trim().is_empty() || *tokens < 1_024 {
            diagnostics.errors.push(format!("configured context window for `{model}` must have a nonempty model and at least 1024 tokens"));
        }
    }
}

fn validate_provider_config(config: &ProvidersConfig) -> ProviderRegistryDiagnostics {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut model_owners = HashMap::<&str, &str>::new();

    for (key, provider) in &config.providers {
        if key.trim().is_empty() {
            errors.push("provider key must not be empty".to_string());
        }
        if provider.name.trim().is_empty() {
            errors.push(format!("provider '{key}' has an empty name"));
        } else if provider.name != *key {
            errors.push(format!(
                "provider key '{key}' does not match configured name '{}'",
                provider.name
            ));
        }
        if provider.base_url.trim().is_empty() {
            errors.push(format!("provider '{key}' has an empty base_url"));
        }
        let protocol = match ProviderProtocol::effective_for_provider(provider) {
            Ok(protocol) => Some(protocol),
            Err(error) => {
                errors.push(format!("provider '{key}': {error}"));
                None
            }
        };

        let mut local_models = HashSet::new();
        for model in &provider.models {
            let model = model.trim();
            if model.is_empty() {
                errors.push(format!("provider '{key}' declares an empty model id"));
                continue;
            }
            if !local_models.insert(model) {
                warnings.push(format!(
                    "provider '{key}' declares model '{model}' more than once"
                ));
            }
            if let Some(previous) = model_owners.insert(model, key) {
                if previous != key {
                    errors.push(format!(
                        "model '{model}' is declared by both '{previous}' and '{key}'"
                    ));
                }
            }
            if provider.parallel_tool_calls != ParallelToolCallsMode::Auto {
                if let Some(protocol) = protocol {
                    let capabilities = ProviderCapabilityProfile::resolve(protocol, model);
                    if let Err(error) = provider
                        .parallel_tool_calls
                        .effective_request(&capabilities)
                    {
                        errors.push(format!("provider '{key}' model '{model}': {error}"));
                    }
                }
            }
        }
    }

    ProviderRegistryDiagnostics { errors, warnings }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn providers(model: &str) -> ProvidersConfig {
        ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                ProviderConfig {
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "secret".to_string(),
                    models: vec![model.to_string()],
                    protocol: Some("completions".to_string()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        }
    }

    #[test]
    fn pinned_snapshot_survives_later_reload() {
        let registry = ProviderRegistry::new(providers("old")).unwrap();
        let pinned = registry.pin();
        let update = registry.replace(providers("new")).unwrap();

        assert_eq!(pinned.revision(), 1);
        assert!(pinned.resolve("old").is_some());
        assert!(pinned.resolve("new").is_none());
        assert_eq!(update.revision, 2);
        assert!(update.changed);
        assert!(registry.pin().resolve("new").is_some());
    }

    #[test]
    fn configured_windows_and_routes_share_one_atomic_revision_and_invalid_reload_retains_both() {
        let registry = ProviderRegistry::new_with_context_windows(providers("private"),
            BTreeMap::from([("private".into(), 128_000)])).unwrap();
        let pinned = registry.pin();
        let update = registry.replace_with_context_windows(providers("private"),
            BTreeMap::from([("private".into(), 32_768)])).unwrap();
        assert!(update.changed);
        assert_eq!(pinned.configured_context_window("private"), Some(128_000));
        assert_eq!(registry.pin().configured_context_window("private"), Some(32_768));
        assert!(registry.replace_with_context_windows(providers("changed"),
            BTreeMap::from([("changed".into(), 0)])).is_err());
        let retained = registry.pin();
        assert_eq!(retained.revision(), update.revision);
        assert!(retained.resolve("private").is_some());
        assert_eq!(retained.configured_context_window("private"), Some(32_768));
        assert!(retained.resolve("changed").is_none());
        assert!(registry.replace(providers("private")).is_ok());
        assert_eq!(registry.pin().configured_context_window("private"), Some(32_768));
    }

    #[test]
    fn invalid_reload_retains_last_valid_snapshot_and_revision() {
        let registry = ProviderRegistry::new(providers("stable")).unwrap();
        let mut invalid = providers("broken");
        invalid.providers.get_mut("test").unwrap().protocol = Some("invalid".to_string());

        let rejected = registry.replace(invalid).unwrap_err();

        assert_eq!(rejected.retained_revision, 1);
        assert!(!rejected.diagnostics.errors.is_empty());
        assert_eq!(registry.revision(), 1);
        assert!(registry.pin().resolve("stable").is_some());
        assert!(registry.pin().resolve("broken").is_none());
    }

    #[test]
    fn registries_are_isolated() {
        let first = ProviderRegistry::new(providers("first")).unwrap();
        let second = ProviderRegistry::new(providers("second")).unwrap();

        first.replace(providers("first-v2")).unwrap();

        assert_eq!(first.revision(), 2);
        assert_eq!(second.revision(), 1);
        assert!(second.pin().resolve("second").is_some());
        assert!(second.pin().resolve("first-v2").is_none());
    }

    #[test]
    fn identical_reload_is_idempotent() {
        let registry = ProviderRegistry::new(providers("stable")).unwrap();

        let update = registry.replace(providers("stable")).unwrap();

        assert!(!update.changed);
        assert_eq!(update.previous_revision, 1);
        assert_eq!(update.revision, 1);
        assert_eq!(registry.revision(), 1);
    }

    #[test]
    fn snapshot_and_revision_are_pinned_as_one_atomic_pair() {
        let registry = Arc::new(ProviderRegistry::new(providers("model-1")).unwrap());
        let writer = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                for revision in 2..=100 {
                    registry
                        .replace(providers(&format!("model-{revision}")))
                        .unwrap();
                }
            })
        };

        for _ in 0..500 {
            let snapshot = registry.pin();
            assert_eq!(
                snapshot.all_models(),
                vec![format!("model-{}", snapshot.revision())]
            );
        }
        writer.join().unwrap();
    }
}
