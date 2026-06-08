//! Unified model alias resolver and dynamic model registry.
//!
//! Two concerns live side-by-side:
//! * [`ModelRegistry`] — a dynamic, YAML-driven catalogue of 40+ models with
//!   pricing, token limits, and capabilities. Lives at `~/.cowd/models.yaml`.
//! * [`ModelResolver`] — a config-first alias resolver that chains
//!   user-defined aliases (`config.yaml aliases:`) with a small built-in
//!   fallback table. Cycle detection with max 10 hops.
//!
//! This module replaces:
//! * the CLI built-in alias table (`crates/cowd-cli/src/cli/mod.rs`)
//! * the API `MODEL_REGISTRY` and `resolve_model_alias` in
//!   `crates/api/src/providers/mod.rs`
//!
//! The built-in fallback table only retains the two entries from
//! `config-default.yaml`: `main → claude-sonnet-4-6`, `fast → claude-haiku-...`.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::cowd_dirs;

// ── Error types ────────────────────────────────────────────────────────────

/// Errors that can occur during model registry operations.
#[derive(Debug)]
pub enum ModelRegistryError {
    /// The YAML file was not found at the expected path.
    NotFound(PathBuf),
    /// An I/O error occurred while reading the file.
    Io(std::io::Error),
    /// The YAML could not be parsed.
    Parse(String),
}

impl fmt::Display for ModelRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(path) => write!(f, "models.yaml not found at {}", path.display()),
            Self::Io(e) => write!(f, "I/O error reading models.yaml: {e}"),
            Self::Parse(msg) => write!(f, "invalid models.yaml: {msg}"),
        }
    }
}

impl std::error::Error for ModelRegistryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// Alias cycle detected during resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircularAliasError {
    /// The alias chain that formed the cycle.
    pub chain: Vec<String>,
    /// The alias that closed the cycle.
    pub duplicate: String,
}

impl fmt::Display for CircularAliasError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "circular alias detected: {} → {} (already seen in chain {:?})",
            self.chain.join(" → "),
            self.duplicate,
            self.chain,
        )
    }
}

impl std::error::Error for CircularAliasError {}

// ── Pricing ────────────────────────────────────────────────────────────────

/// Per-million-token pricing stored in the model registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pricing {
    /// USD per 1M input tokens.
    #[serde(rename = "input_per_1m")]
    pub input_per_1m: f64,
    /// USD per 1M output tokens.
    #[serde(rename = "output_per_1m")]
    pub output_per_1m: f64,
    /// USD per 1M cache-write tokens (Anthropic-style prompt caching).
    #[serde(default, rename = "cache_write_per_1m")]
    pub cache_write_per_1m: Option<f64>,
    /// USD per 1M cache-read tokens.
    #[serde(default, rename = "cache_read_per_1m")]
    pub cache_read_per_1m: Option<f64>,
}

impl Pricing {
    /// Convert registry pricing into the runtime [`super::usage::ModelPricing`] type.
    #[must_use]
    pub fn to_model_pricing(&self) -> crate::usage::ModelPricing {
        crate::usage::ModelPricing {
            input_cost_per_million: self.input_per_1m,
            output_cost_per_million: self.output_per_1m,
            cache_creation_cost_per_million: self.cache_write_per_1m.unwrap_or(0.0),
            cache_read_cost_per_million: self.cache_read_per_1m.unwrap_or(0.0),
        }
    }
}

// ── Model info ─────────────────────────────────────────────────────────────

/// Metadata for a single model in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    /// Provider key (`anthropic`, `openai`, `deepseek`, `dashscope`, …).
    pub provider: String,
    /// Human-readable name for display.
    pub display_name: String,
    /// Maximum context window in tokens.
    pub context_window: u32,
    /// Maximum output tokens the model can generate.
    pub max_output_tokens: u32,
    /// Per-million-token pricing.
    pub pricing: Pricing,
    /// Capability tags (`text`, `vision`, `tool_use`, `reasoning`, …).
    pub capabilities: Vec<String>,
}

// ── YAML file shape ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelsFile {
    version: String,
    updated_at: String,
    models: HashMap<String, ModelInfo>,
}

// ── Model registry ─────────────────────────────────────────────────────────

/// Dynamic, YAML-driven model catalogue loaded from `~/.cowd/models.yaml`.
pub struct ModelRegistry {
    models: HashMap<String, ModelInfo>,
}

impl ModelRegistry {
    /// Load the model registry from the user's `~/.cowd/models.yaml`.
    ///
    /// Returns [`ModelRegistryError::NotFound`] when the file does not exist.
    pub fn load() -> Result<Self, ModelRegistryError> {
        let path = Self::default_path();
        if !path.exists() {
            return Err(ModelRegistryError::NotFound(path));
        }
        let content = std::fs::read_to_string(&path).map_err(ModelRegistryError::Io)?;
        let file: ModelsFile =
            serde_yaml::from_str(&content).map_err(|e| ModelRegistryError::Parse(e.to_string()))?;
        Ok(Self {
            models: file.models,
        })
    }

    /// Create an empty registry (used as fallback when the file is missing).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    /// Look up a model by its canonical name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ModelInfo> {
        self.models.get(name)
    }

    /// Return all models in the registry.
    #[must_use]
    pub fn list(&self) -> Vec<&ModelInfo> {
        self.models.values().collect()
    }

    /// Convenience: return pricing for a model.
    #[must_use]
    pub fn pricing_for(&self, name: &str) -> Option<&Pricing> {
        self.get(name).map(|info| &info.pricing)
    }

    /// Maximum output tokens for a model, falling back to 64K.
    #[must_use]
    pub fn max_output_tokens_for(&self, name: &str) -> u32 {
        self.get(name)
            .map(|info| info.max_output_tokens)
            .unwrap_or(64_000)
    }

    /// Context window for a model, falling back to 128K.
    #[must_use]
    pub fn context_window_for(&self, name: &str) -> u32 {
        self.get(name)
            .map(|info| info.context_window)
            .unwrap_or(128_000)
    }

    // ── internal helpers ────────────────────────────────────────────────

    fn default_path() -> PathBuf {
        cowd_dirs::user_dot_dir(&cowd_dirs::home_dir()).join("models.yaml")
    }
}

// ── Lazy global ────────────────────────────────────────────────────────────

/// Lazily-loaded global model registry. Falls back to an empty registry
/// when `models.yaml` is missing or malformed.
#[must_use]
pub fn global_registry() -> &'static ModelRegistry {
    use std::sync::LazyLock;
    static REGISTRY: LazyLock<ModelRegistry> =
        LazyLock::new(|| ModelRegistry::load().unwrap_or_else(|_| ModelRegistry::empty()));
    &REGISTRY
}

// ── Alias resolver ─────────────────────────────────────────────────────────

/// Config-first alias resolver with cycle detection.
///
/// Resolution order:
/// 1. User-defined aliases from `config.yaml` (highest priority).
/// 2. Built-in fallback aliases (`main → sonnet`, `fast → haiku`).
/// 3. Pass-through (returns `name` unchanged).
///
/// Cycle detection stops after 10 hops.
pub struct ModelResolver {
    /// Aliases loaded from the user's config file (`config.yaml aliases:`).
    config_aliases: HashMap<String, String>,
    /// Minimal built-in fallback table (from `config-default.yaml`).
    builtin_aliases: HashMap<String, String>,
}

impl ModelResolver {
    /// Create a resolver with user-defined aliases from config.
    #[must_use]
    pub fn new(config_aliases: HashMap<String, String>) -> Self {
        Self {
            config_aliases,
            builtin_aliases: Self::default_builtins(),
        }
    }

    /// Create a resolver with only the built-in fallback table (no config).
    #[must_use]
    pub fn default() -> Self {
        Self {
            config_aliases: HashMap::new(),
            builtin_aliases: Self::default_builtins(),
        }
    }

    /// Resolve an alias, returning the canonical model name.
    /// Falls back to returning the input unchanged when no alias matches.
    #[must_use]
    pub fn resolve(&self, name: &str) -> String {
        self.resolve_with_depth(name, 0)
            .unwrap_or_else(|_| name.trim().to_string())
    }

    /// Resolve with a hop-counter. Returns [`CircularAliasError`] if a cycle
    /// is detected within 10 hops.
    pub fn resolve_with_depth(&self, name: &str, depth: u8) -> Result<String, CircularAliasError> {
        const MAX_DEPTH: u8 = 10;
        if depth > MAX_DEPTH {
            return Err(CircularAliasError {
                chain: vec![name.to_string()],
                duplicate: name.to_string(),
            });
        }

        let trimmed = name.trim();
        let lower = trimmed.to_ascii_lowercase();

        // 1. Config aliases (highest priority)
        if let Some(resolved) = self.config_aliases.get(&lower) {
            let next = self.resolve_with_depth(resolved, depth + 1)?;
            // Guard against trivial self-reference
            if next == trimmed {
                return Err(CircularAliasError {
                    chain: vec![trimmed.to_string(), next],
                    duplicate: trimmed.to_string(),
                });
            }
            return Ok(next);
        }

        // 2. Built-in fallback
        if let Some(resolved) = self.builtin_aliases.get(&lower) {
            let next = self.resolve_with_depth(resolved, depth + 1)?;
            if next == trimmed {
                return Err(CircularAliasError {
                    chain: vec![trimmed.to_string(), next],
                    duplicate: trimmed.to_string(),
                });
            }
            return Ok(next);
        }

        // 3. Pass-through
        Ok(trimmed.to_string())
    }

    fn default_builtins() -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("main".to_string(), "claude-sonnet-4-6".to_string());
        map.insert("fast".to_string(), "claude-haiku-4-5-20251213".to_string());
        map
    }
}

impl Default for ModelResolver {
    fn default() -> Self {
        Self::default()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ModelResolver ───────────────────────────────────────────────────

    #[test]
    fn resolver_passthrough_unknown() {
        let resolver = ModelResolver::default();
        assert_eq!(resolver.resolve("claude-sonnet-4-6"), "claude-sonnet-4-6");
        assert_eq!(resolver.resolve("grok-3"), "grok-3");
    }

    #[test]
    fn resolver_builtin_main() {
        let resolver = ModelResolver::default();
        assert_eq!(resolver.resolve("main"), "claude-sonnet-4-6");
    }

    #[test]
    fn resolver_builtin_fast() {
        let resolver = ModelResolver::default();
        assert_eq!(resolver.resolve("fast"), "claude-haiku-4-5-20251213");
    }

    #[test]
    fn resolver_config_overrides_builtin() {
        let mut config = HashMap::new();
        config.insert("fast".to_string(), "claude-opus-4-6".to_string());
        let resolver = ModelResolver::new(config);
        // "fast" in config overrides the built-in "fast → haiku"
        assert_eq!(resolver.resolve("fast"), "claude-opus-4-6");
        // "main" is not in config, so falls back to built-in
        assert_eq!(resolver.resolve("main"), "claude-sonnet-4-6");
    }

    #[test]
    fn resolver_chain_resolution() {
        let mut config = HashMap::new();
        config.insert("smart".to_string(), "main".to_string());
        let resolver = ModelResolver::new(config);
        // smart → main → (built-in) claude-sonnet-4-6
        assert_eq!(resolver.resolve("smart"), "claude-sonnet-4-6");
    }

    #[test]
    fn resolver_max_depth_exceeded() {
        let mut config = HashMap::new();
        // Create a chain longer than 10
        for i in 0..12 {
            config.insert(format!("step{i}"), format!("step{}", i + 1));
        }
        let resolver = ModelResolver::new(config);
        // step0 → step1 → ... → step11 (11 hops, should fail)
        let result = resolver.resolve_with_depth("step0", 0);
        assert!(result.is_err());
    }

    #[test]
    fn resolver_circular_alias() {
        let mut config = HashMap::new();
        config.insert("a".to_string(), "b".to_string());
        config.insert("b".to_string(), "a".to_string());
        let resolver = ModelResolver::new(config);
        let result = resolver.resolve_with_depth("a", 0);
        assert!(result.is_err());
    }

    #[test]
    fn resolver_self_reference() {
        let mut config = HashMap::new();
        config.insert("x".to_string(), "x".to_string());
        let resolver = ModelResolver::new(config);
        let result = resolver.resolve_with_depth("x", 0);
        assert!(result.is_err());
    }

    #[test]
    fn resolver_trim_whitespace() {
        let resolver = ModelResolver::default();
        assert_eq!(resolver.resolve("  main  "), "claude-sonnet-4-6");
    }

    // ── ModelRegistry (unit tests, no file I/O) ─────────────────────────

    #[test]
    fn registry_get_existing() {
        let mut models = HashMap::new();
        models.insert(
            "test-model".to_string(),
            ModelInfo {
                provider: "test".to_string(),
                display_name: "Test Model".to_string(),
                context_window: 128_000,
                max_output_tokens: 16_000,
                pricing: Pricing {
                    input_per_1m: 1.0,
                    output_per_1m: 5.0,
                    cache_write_per_1m: None,
                    cache_read_per_1m: None,
                },
                capabilities: vec!["text".to_string()],
            },
        );
        let registry = ModelRegistry { models };

        let info = registry.get("test-model").expect("model should exist");
        assert_eq!(info.context_window, 128_000);
        assert_eq!(info.max_output_tokens, 16_000);
        assert_eq!(info.provider, "test");

        let pricing = registry
            .pricing_for("test-model")
            .expect("pricing should exist");
        assert_eq!(pricing.input_per_1m, 1.0);
    }

    #[test]
    fn registry_get_missing() {
        let registry = ModelRegistry::empty();
        assert!(registry.get("nope").is_none());
        assert!(registry.pricing_for("nope").is_none());
    }

    #[test]
    fn registry_fallbacks() {
        let registry = ModelRegistry::empty();
        assert_eq!(registry.max_output_tokens_for("unknown"), 64_000);
        assert_eq!(registry.context_window_for("unknown"), 128_000);
    }

    #[test]
    fn pricing_to_model_pricing_conversion() {
        let pricing = Pricing {
            input_per_1m: 3.0,
            output_per_1m: 15.0,
            cache_write_per_1m: Some(3.75),
            cache_read_per_1m: Some(0.30),
        };
        let mp = pricing.to_model_pricing();
        assert_eq!(mp.input_cost_per_million, 3.0);
        assert_eq!(mp.output_cost_per_million, 15.0);
        assert_eq!(mp.cache_creation_cost_per_million, 3.75);
        assert_eq!(mp.cache_read_cost_per_million, 0.30);
    }

    #[test]
    fn pricing_to_model_pricing_missing_cache() {
        let pricing = Pricing {
            input_per_1m: 2.0,
            output_per_1m: 8.0,
            cache_write_per_1m: None,
            cache_read_per_1m: None,
        };
        let mp = pricing.to_model_pricing();
        assert_eq!(mp.cache_creation_cost_per_million, 0.0);
        assert_eq!(mp.cache_read_cost_per_million, 0.0);
    }
}
