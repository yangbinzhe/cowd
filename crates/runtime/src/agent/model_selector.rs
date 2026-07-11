use std::sync::Arc;

use crate::ProviderRegistry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentModelSelection {
    pub model: String,
    pub provider: String,
    pub registry_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentModelSelectionError {
    NoConfiguredModel,
    RequestedModelUnavailable(String),
}

impl std::fmt::Display for AgentModelSelectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoConfiguredModel => {
                formatter.write_str("no configured provider model is available")
            }
            Self::RequestedModelUnavailable(model) => {
                write!(formatter, "requested agent model `{model}` is unavailable")
            }
        }
    }
}

impl std::error::Error for AgentModelSelectionError {}

/// Resolves a model from immutable task intent and the workspace-scoped provider
/// registry. There is deliberately no hidden model-name fallback.
#[derive(Clone)]
pub struct AgentModelSelector {
    registry: Arc<ProviderRegistry>,
}

impl AgentModelSelector {
    #[must_use]
    pub fn new(registry: Arc<ProviderRegistry>) -> Self {
        Self { registry }
    }

    pub fn select(
        &self,
        explicit_requirement: Option<&str>,
    ) -> Result<AgentModelSelection, AgentModelSelectionError> {
        let snapshot = self.registry.pin();
        let requested = explicit_requirement.filter(|value| !value.trim().is_empty());
        let model = match requested {
            Some(model) if snapshot.resolve(model).is_some() => model.to_string(),
            Some(model) => {
                return Err(AgentModelSelectionError::RequestedModelUnavailable(
                    model.into(),
                ))
            }
            None => snapshot
                .all_models()
                .into_iter()
                .next()
                .ok_or(AgentModelSelectionError::NoConfiguredModel)?,
        };
        let provider = snapshot
            .provider_name_for_model(&model)
            .ok_or_else(|| AgentModelSelectionError::RequestedModelUnavailable(model.clone()))?;
        Ok(AgentModelSelection {
            model,
            provider,
            registry_revision: snapshot.revision(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{ProviderConfig, ProvidersConfig};

    #[test]
    fn selector_never_falls_back_to_a_hard_coded_model() {
        let selector = AgentModelSelector::new(Arc::new(ProviderRegistry::empty()));
        assert!(matches!(
            selector.select(None),
            Err(AgentModelSelectionError::NoConfiguredModel)
        ));
    }

    #[test]
    fn selector_prefers_an_explicit_available_model() {
        let providers = ProvidersConfig {
            providers: HashMap::from([(
                "test".into(),
                ProviderConfig {
                    name: "test".into(),
                    base_url: "https://example.test/v1".into(),
                    api_key: "secret".into(),
                    models: vec!["fast".into(), "deep".into()],
                    protocol: Some("responses".into()),
                },
            )]),
        };
        let selector = AgentModelSelector::new(Arc::new(ProviderRegistry::new(providers).unwrap()));
        let selected = selector.select(Some("deep")).unwrap();
        assert_eq!(selected.model, "deep");
        assert_eq!(selected.provider, "test");
    }
}
