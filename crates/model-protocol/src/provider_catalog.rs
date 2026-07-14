use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    model_registry::ModelRegistry,
    prompt_cache::hash_serializable,
    provider_config::{ProviderProtocol, ProvidersConfig},
};

pub const PROVIDER_CATALOG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCatalog {
    pub schema_version: u32,
    pub generation: String,
    pub sources: Vec<ProviderCatalogSource>,
    pub transforms: Vec<ProviderCatalogTransform>,
    pub providers: Vec<ProviderCatalogProvider>,
    pub models: Vec<ProviderCatalogModel>,
    pub profiles: Vec<ProviderCatalogProfile>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCatalogSource {
    pub id: String,
    pub kind: String,
    pub enabled: bool,
    pub priority: i32,
    pub status: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCatalogTransform {
    pub source: String,
    pub enabled: bool,
    pub priority: i32,
    pub filter_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCatalogProvider {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub source: String,
    pub status: String,
    pub configured_protocol: Option<String>,
    pub effective_protocol: String,
    pub protocol_configured: bool,
    pub credential_present: bool,
    pub model_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCatalogModel {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub provider: String,
    pub source: String,
    pub status: String,
    pub effective_protocol: String,
    pub protocol_configured: bool,
    pub selected: bool,
    pub context_window_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCatalogProfile {
    pub id: String,
    pub name: String,
    pub source: String,
    pub model: String,
    pub provider: Option<String>,
    pub effective_protocol: Option<String>,
    pub selected: bool,
    pub status: String,
}

pub struct ProviderCatalogInput<'a> {
    pub providers: &'a ProvidersConfig,
    pub registry: &'a ModelRegistry,
    pub configured_model: Option<&'a str>,
    pub aliases: &'a BTreeMap<String, String>,
    pub config_source: &'a str,
    pub extra_sources: Vec<ProviderCatalogSource>,
    pub transforms: Vec<ProviderCatalogTransform>,
    pub warnings: Vec<String>,
}

impl ProviderCatalog {
    #[must_use]
    pub fn from_input(input: ProviderCatalogInput<'_>) -> Self {
        let mut warnings = input.warnings;
        let mut provider_rows = input
            .providers
            .providers
            .values()
            .map(|provider| {
                let protocol_result = ProviderProtocol::effective_for_provider(provider);
                let (effective_protocol, status) = match protocol_result {
                    Ok(protocol) => (protocol.as_str().to_string(), "available".to_string()),
                    Err(error) => {
                        warnings.push(format!(
                            "provider {} has invalid protocol: {error}",
                            provider.name
                        ));
                        (format!("invalid:{error}"), "invalid".to_string())
                    }
                };
                ProviderCatalogProvider {
                    id: provider.name.clone(),
                    name: provider.name.clone(),
                    base_url: provider.base_url.clone(),
                    source: input.config_source.to_string(),
                    status,
                    configured_protocol: provider
                        .protocol
                        .as_ref()
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty()),
                    effective_protocol,
                    protocol_configured: provider
                        .protocol
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty()),
                    credential_present: !provider.api_key.trim().is_empty(),
                    model_count: provider.models.len(),
                }
            })
            .collect::<Vec<_>>();
        provider_rows.sort_by(|left, right| left.id.cmp(&right.id));

        let mut model_rows = input
            .providers
            .providers
            .values()
            .flat_map(|provider| {
                let protocol_result = ProviderProtocol::effective_for_provider(provider);
                let effective_protocol = protocol_result
                    .map(|protocol| protocol.as_str().to_string())
                    .unwrap_or_else(|error| format!("invalid:{error}"));
                let protocol_configured = provider
                    .protocol
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty());
                provider.models.iter().map(move |model_id| {
                    let registry_info = input.registry.get(model_id);
                    ProviderCatalogModel {
                        id: model_id.clone(),
                        name: model_id.clone(),
                        display_name: registry_info
                            .map(|info| info.display_name.clone())
                            .unwrap_or_else(|| model_id.clone()),
                        provider: provider.name.clone(),
                        source: input.config_source.to_string(),
                        status: "configured".to_string(),
                        effective_protocol: effective_protocol.clone(),
                        protocol_configured,
                        selected: input.configured_model == Some(model_id.as_str()),
                        context_window_tokens: registry_info
                            .map(|info| u64::from(info.context_window)),
                        max_output_tokens: registry_info
                            .map(|info| u64::from(info.max_output_tokens)),
                        capabilities: registry_info
                            .map(|info| info.capabilities.clone())
                            .unwrap_or_default(),
                    }
                })
            })
            .collect::<Vec<_>>();
        model_rows.sort_by(|left, right| {
            left.provider
                .cmp(&right.provider)
                .then_with(|| left.id.cmp(&right.id))
        });

        let configured_model_provider = input
            .configured_model
            .and_then(|model| input.providers.resolve_full(model))
            .map(|provider| {
                (
                    provider.name.clone(),
                    ProviderProtocol::effective_for_provider(provider)
                        .ok()
                        .map(|protocol| protocol.as_str().to_string()),
                )
            });

        let mut profiles = Vec::new();
        if let Some(model) = input.configured_model {
            profiles.push(ProviderCatalogProfile {
                id: "default".to_string(),
                name: "Default runtime model".to_string(),
                source: input.config_source.to_string(),
                model: model.to_string(),
                provider: configured_model_provider
                    .as_ref()
                    .map(|(provider, _)| provider.clone()),
                effective_protocol: configured_model_provider
                    .as_ref()
                    .and_then(|(_, protocol)| protocol.clone()),
                selected: true,
                status: if configured_model_provider.is_some() {
                    "resolved".to_string()
                } else {
                    "unresolved".to_string()
                },
            });
        }
        profiles.extend(input.aliases.iter().map(|(alias, model)| {
            let provider = input.providers.resolve_full(model);
            ProviderCatalogProfile {
                id: format!("alias:{alias}"),
                name: alias.clone(),
                source: "aliases".to_string(),
                model: model.clone(),
                provider: provider.map(|provider| provider.name.clone()),
                effective_protocol: provider.and_then(|provider| {
                    ProviderProtocol::effective_for_provider(provider)
                        .ok()
                        .map(|protocol| protocol.as_str().to_string())
                }),
                selected: input.configured_model == Some(model.as_str()),
                status: if provider.is_some() {
                    "resolved".to_string()
                } else {
                    "unresolved".to_string()
                },
            }
        }));
        profiles.sort_by(|left, right| left.id.cmp(&right.id));

        if input.providers.providers.is_empty() {
            warnings.push("no runtime providers are configured".to_string());
        }
        if input
            .configured_model
            .is_some_and(|model| input.providers.resolve_full(model).is_none())
        {
            warnings.push("configured default model is not declared by any provider".to_string());
        }
        warnings.sort();
        warnings.dedup();

        let mut sources = vec![
            ProviderCatalogSource {
                id: "runtime_config".to_string(),
                kind: input.config_source.to_string(),
                enabled: true,
                priority: 100,
                status: if input.providers.providers.is_empty() {
                    "empty".to_string()
                } else {
                    "ready".to_string()
                },
                summary: format!(
                    "{} providers / {} models",
                    provider_rows.len(),
                    model_rows.len()
                ),
            },
            ProviderCatalogSource {
                id: "model_registry".to_string(),
                kind: "registry".to_string(),
                enabled: true,
                priority: 80,
                status: "ready".to_string(),
                summary: "model metadata enriches configured provider models".to_string(),
            },
        ];
        sources.extend(input.extra_sources);
        sources.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.id.cmp(&right.id))
        });

        let mut transforms = input.transforms;
        if transforms.is_empty() {
            transforms = vec![
                ProviderCatalogTransform {
                    source: "runtime_config".to_string(),
                    enabled: true,
                    priority: 100,
                    filter_policy: "configured-provider-models-only".to_string(),
                },
                ProviderCatalogTransform {
                    source: "model_registry".to_string(),
                    enabled: true,
                    priority: 80,
                    filter_policy: "metadata-enrichment-only".to_string(),
                },
                ProviderCatalogTransform {
                    source: "aliases".to_string(),
                    enabled: true,
                    priority: 60,
                    filter_policy: "profile-projection".to_string(),
                },
            ];
        }
        transforms.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.source.cmp(&right.source))
        });

        let mut catalog = Self {
            schema_version: PROVIDER_CATALOG_SCHEMA_VERSION,
            generation: String::new(),
            sources,
            transforms,
            providers: provider_rows,
            models: model_rows,
            profiles,
            warnings,
        };
        catalog.generation = catalog.compute_generation();
        catalog
    }

    #[must_use]
    pub fn compute_generation(&self) -> String {
        let fingerprint = ProviderCatalogFingerprint {
            schema_version: self.schema_version,
            sources: &self.sources,
            transforms: &self.transforms,
            providers: &self.providers,
            models: &self.models,
            profiles: &self.profiles,
            warnings: &self.warnings,
        };
        format!(
            "provider-catalog-v1-{:016x}",
            hash_serializable(&fingerprint)
        )
    }
}

#[derive(Serialize)]
struct ProviderCatalogFingerprint<'a> {
    schema_version: u32,
    sources: &'a [ProviderCatalogSource],
    transforms: &'a [ProviderCatalogTransform],
    providers: &'a [ProviderCatalogProvider],
    models: &'a [ProviderCatalogModel],
    profiles: &'a [ProviderCatalogProfile],
    warnings: &'a [String],
}

#[cfg(test)]
mod tests {
    use super::{ProviderCatalog, ProviderCatalogInput};
    use crate::{
        model_registry::ModelRegistry,
        provider_config::{ProviderConfig, ProvidersConfig},
    };
    use std::collections::{BTreeMap, HashMap};

    fn providers() -> ProvidersConfig {
        let mut providers = HashMap::new();
        providers.insert(
            "openai".to_string(),
            ProviderConfig {
                base_url: "https://api.openai.com/v1".to_string(),
                api_key: "sk-test".to_string(),
                models: vec!["gpt-5-mini".to_string()],
                name: "openai".to_string(),
                protocol: Some("responses".to_string()),
            },
        );
        providers.insert(
            "deepseek".to_string(),
            ProviderConfig {
                base_url: "https://api.deepseek.com/v1".to_string(),
                api_key: String::new(),
                models: vec!["deepseek-v4-flash".to_string()],
                name: "deepseek".to_string(),
                protocol: Some("completions".to_string()),
            },
        );
        ProvidersConfig { providers }
    }

    #[test]
    fn catalog_projects_providers_models_profiles_and_generation() {
        let providers = providers();
        let registry = ModelRegistry::empty();
        let mut aliases = BTreeMap::new();
        aliases.insert("fast".to_string(), "deepseek-v4-flash".to_string());

        let catalog = ProviderCatalog::from_input(ProviderCatalogInput {
            providers: &providers,
            registry: &registry,
            configured_model: Some("gpt-5-mini"),
            aliases: &aliases,
            config_source: "config",
            extra_sources: Vec::new(),
            transforms: Vec::new(),
            warnings: Vec::new(),
        });

        assert_eq!(catalog.schema_version, 1);
        assert_eq!(catalog.providers.len(), 2);
        assert_eq!(catalog.models.len(), 2);
        assert!(catalog.models.iter().any(|model| model.id == "gpt-5-mini"
            && model.effective_protocol == "responses"
            && model.selected));
        assert!(catalog
            .profiles
            .iter()
            .any(|profile| profile.id == "alias:fast"
                && profile.provider.as_deref() == Some("deepseek")));
        assert!(catalog.generation.starts_with("provider-catalog-v1-"));
        assert_eq!(catalog.generation, catalog.compute_generation());
    }

    #[test]
    fn catalog_warns_for_unresolved_default_model() {
        let providers = providers();
        let registry = ModelRegistry::empty();
        let catalog = ProviderCatalog::from_input(ProviderCatalogInput {
            providers: &providers,
            registry: &registry,
            configured_model: Some("missing-model"),
            aliases: &BTreeMap::new(),
            config_source: "config",
            extra_sources: Vec::new(),
            transforms: Vec::new(),
            warnings: Vec::new(),
        });

        assert!(catalog
            .warnings
            .iter()
            .any(|warning| warning.contains("configured default model")));
        assert!(catalog
            .profiles
            .iter()
            .any(|profile| profile.id == "default" && profile.status == "unresolved"));
    }
}
