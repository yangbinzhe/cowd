use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::execution_core::graph::{ExecutionResourceKind, ResourceQuota};
use crate::ProviderRegistrySnapshot;

const TOKEN_PRESSURE_QUANTUM: u64 = 4_096;
const TOKEN_PRESSURE_PER_REQUEST: usize = 256;
// One ordinary concurrency slot carries 128K tokens of in-flight pressure.
// Requests near a 1M context therefore consume several ordinary slots instead
// of being treated like short prompts.
const TOKEN_PRESSURE_UNITS_PER_CONCURRENCY_SLOT: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderQuotaPolicy {
    pub minimum: usize,
    pub target: usize,
    pub maximum: usize,
    pub interactive_reserve: usize,
}

impl ProviderQuotaPolicy {
    #[must_use]
    pub const fn new(
        minimum: usize,
        target: usize,
        maximum: usize,
        interactive_reserve: usize,
    ) -> Self {
        Self {
            minimum,
            target,
            maximum,
            interactive_reserve,
        }
    }

    pub fn validate(self, context: &str) -> Result<Self, String> {
        ResourceQuota::new(self.minimum, self.target, self.maximum)
            .map_err(|error| format!("{context}: {error}"))?;
        if self.interactive_reserve > self.maximum {
            return Err(format!(
                "{context}: interactive reserve {} exceeds maximum {}",
                self.interactive_reserve, self.maximum
            ));
        }
        Ok(self)
    }

    #[must_use]
    pub const fn quota(self) -> ResourceQuota {
        ResourceQuota {
            minimum: self.minimum,
            target: self.target,
            maximum: self.maximum,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccountPolicy {
    pub provider_names: Vec<String>,
    #[serde(flatten)]
    pub quota: ProviderQuotaPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelPolicy {
    pub account: Option<String>,
    #[serde(flatten)]
    pub quota: ProviderQuotaPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProviderResourceConfig {
    pub global: ProviderQuotaPolicy,
    pub fallback: ProviderQuotaPolicy,
    pub accounts: BTreeMap<String, ProviderAccountPolicy>,
    pub models: BTreeMap<String, ProviderModelPolicy>,
}

impl Default for ProviderResourceConfig {
    fn default() -> Self {
        Self {
            global: ProviderQuotaPolicy::new(8, 64, 256, 8),
            fallback: ProviderQuotaPolicy::new(4, 32, 128, 8),
            accounts: BTreeMap::new(),
            models: BTreeMap::new(),
        }
    }
}

impl ProviderResourceConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.global.validate("runtime.resources.provider.global")?;
        self.fallback
            .validate("runtime.resources.provider.fallback")?;
        for (name, account) in &self.accounts {
            if name.trim().is_empty() {
                return Err("provider account name cannot be empty".to_string());
            }
            account
                .quota
                .validate(&format!("runtime.resources.provider.accounts.{name}"))?;
        }
        for (name, model) in &self.models {
            if name.trim().is_empty() {
                return Err("provider model name cannot be empty".to_string());
            }
            model
                .quota
                .validate(&format!("runtime.resources.provider.models.{name}"))?;
            if let Some(account) = model.account.as_deref() {
                if !self.accounts.contains_key(account) {
                    return Err(format!(
                        "provider model '{name}' references unknown account '{account}'"
                    ));
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn policy_for_model(&self, model: &str) -> ProviderQuotaPolicy {
        self.models
            .get(model)
            .map(|policy| policy.quota)
            .unwrap_or_else(|| default_model_policy(model, self.fallback))
    }

    #[must_use]
    pub fn account_for(&self, provider_name: &str, model: &str) -> String {
        self.models
            .get(model)
            .and_then(|policy| policy.account.clone())
            .or_else(|| {
                self.accounts.iter().find_map(|(account, policy)| {
                    policy
                        .provider_names
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(provider_name))
                        .then(|| account.clone())
                })
            })
            .unwrap_or_else(|| provider_name.to_string())
    }

    #[must_use]
    pub fn account_policy(&self, account: &str) -> ProviderQuotaPolicy {
        self.accounts
            .get(account)
            .map(|policy| policy.quota)
            .unwrap_or(self.global)
    }

    #[must_use]
    pub fn materialize(&self, registry: &ProviderRegistrySnapshot) -> ProviderResourceGeneration {
        let mut quotas = vec![(ExecutionResourceKind::Provider, self.global.quota())];
        let mut reserves = vec![(
            ExecutionResourceKind::Provider,
            self.global.interactive_reserve,
        )];
        for model in registry.all_models() {
            let Some(provider_name) = registry.provider_name_for_model(&model) else {
                continue;
            };
            let account = self.account_for(&provider_name, &model);
            let account_policy = self.account_policy(&account);
            let model_policy = self.policy_for_model(&model);
            insert_resource(
                &mut quotas,
                &mut reserves,
                ExecutionResourceKind::ProviderAccount(account.clone()),
                account_policy,
            );
            insert_resource(
                &mut quotas,
                &mut reserves,
                ExecutionResourceKind::ProviderModel(model),
                model_policy,
            );
            insert_resource(
                &mut quotas,
                &mut reserves,
                ExecutionResourceKind::ProviderTokenPool(account),
                token_pressure_policy(account_policy),
            );
        }
        ProviderResourceGeneration { quotas, reserves }
    }

    #[must_use]
    pub fn admission_demands(
        &self,
        provider_name: &str,
        model: &str,
        estimated_tokens: u64,
    ) -> Vec<(ExecutionResourceKind, usize)> {
        let account = self.account_for(provider_name, model);
        let pressure = estimated_tokens
            .saturating_add(TOKEN_PRESSURE_QUANTUM - 1)
            .saturating_div(TOKEN_PRESSURE_QUANTUM)
            .clamp(1, TOKEN_PRESSURE_PER_REQUEST as u64) as usize;
        vec![
            (ExecutionResourceKind::Provider, 1),
            (ExecutionResourceKind::ProviderAccount(account.clone()), 1),
            (ExecutionResourceKind::ProviderModel(model.to_string()), 1),
            (ExecutionResourceKind::ProviderTokenPool(account), pressure),
        ]
    }
}

#[derive(Debug, Clone)]
pub struct ProviderResourceGeneration {
    pub quotas: Vec<(ExecutionResourceKind, ResourceQuota)>,
    pub reserves: Vec<(ExecutionResourceKind, usize)>,
}

fn default_model_policy(model: &str, fallback: ProviderQuotaPolicy) -> ProviderQuotaPolicy {
    match model.trim().to_ascii_lowercase().as_str() {
        "deepseek-v4-flash" => ProviderQuotaPolicy::new(8, 64, 256, 16),
        "deepseek-v4-pro" => ProviderQuotaPolicy::new(4, 32, 128, 8),
        _ => fallback,
    }
}

fn token_pressure_policy(account: ProviderQuotaPolicy) -> ProviderQuotaPolicy {
    ProviderQuotaPolicy::new(
        account
            .minimum
            .saturating_mul(TOKEN_PRESSURE_UNITS_PER_CONCURRENCY_SLOT),
        account
            .target
            .saturating_mul(TOKEN_PRESSURE_UNITS_PER_CONCURRENCY_SLOT),
        account
            .maximum
            .saturating_mul(TOKEN_PRESSURE_UNITS_PER_CONCURRENCY_SLOT),
        account
            .interactive_reserve
            .saturating_mul(TOKEN_PRESSURE_UNITS_PER_CONCURRENCY_SLOT),
    )
}

fn insert_resource(
    quotas: &mut Vec<(ExecutionResourceKind, ResourceQuota)>,
    reserves: &mut Vec<(ExecutionResourceKind, usize)>,
    kind: ExecutionResourceKind,
    policy: ProviderQuotaPolicy,
) {
    if quotas.iter().any(|(existing, _)| existing == &kind) {
        return;
    }
    quotas.push((kind.clone(), policy.quota()));
    reserves.push((kind, policy.interactive_reserve));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deepseek_defaults_distinguish_pro_and_flash() {
        let config = ProviderResourceConfig::default();
        assert_eq!(config.policy_for_model("deepseek-v4-pro").target, 32);
        assert_eq!(config.policy_for_model("deepseek-v4-flash").target, 64);
        assert_eq!(
            config.policy_for_model("unknown").target,
            config.fallback.target
        );
    }

    #[test]
    fn admission_is_one_atomic_hierarchy_with_bounded_token_pressure() {
        let config = ProviderResourceConfig::default();
        let demands = config.admission_demands("deepseek", "deepseek-v4-pro", 2_000_000);
        assert_eq!(demands.len(), 4);
        assert!(demands.iter().any(|(kind, weight)| {
            matches!(kind, ExecutionResourceKind::ProviderTokenPool(account) if account == "deepseek")
                && *weight == TOKEN_PRESSURE_PER_REQUEST
        }));
        let pressure = token_pressure_policy(ProviderQuotaPolicy::new(4, 32, 128, 8));
        assert_eq!(pressure.maximum, 4_096);
        assert_eq!(
            pressure.maximum / TOKEN_PRESSURE_PER_REQUEST,
            16,
            "1M-class prompts must be more restrictive than the ordinary request ceiling"
        );
    }

    #[test]
    fn configured_accounts_and_models_use_the_public_flattened_shape() {
        let config: ProviderResourceConfig = serde_json::from_value(serde_json::json!({
            "global": {
                "minimum": 8,
                "target": 64,
                "maximum": 256,
                "interactiveReserve": 8
            },
            "fallback": {
                "minimum": 4,
                "target": 32,
                "maximum": 128,
                "interactiveReserve": 8
            },
            "accounts": {
                "deepseek-main": {
                    "providerNames": ["deepseek"],
                    "minimum": 8,
                    "target": 64,
                    "maximum": 256,
                    "interactiveReserve": 8
                }
            },
            "models": {
                "deepseek-v4-pro": {
                    "account": "deepseek-main",
                    "minimum": 4,
                    "target": 32,
                    "maximum": 128,
                    "interactiveReserve": 8
                }
            }
        }))
        .expect("provider resource config");
        config.validate().expect("valid provider resource config");
        assert_eq!(
            config.account_for("deepseek", "deepseek-v4-pro"),
            "deepseek-main"
        );
        assert_eq!(config.policy_for_model("deepseek-v4-pro").target, 32);
    }
}
