//! Configuration structures for the memory system.
//!
//! All config types are now self-contained — the unified `config` crate
//! has been removed and its types were inlined into their respective
//! consumer crates.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ── Inlined from former config crate ─────────────────────────────────────

/// Token budget configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetConfig {
    /// Total context window size
    #[serde(default = "default_context_window")]
    pub context_window: u64,
    /// Reserved for system prompt
    #[serde(default = "default_reserved_system")]
    pub reserved_system: u64,
    /// Reserved for response
    #[serde(default = "default_reserved_response")]
    pub reserved_response: u64,
    /// Warning threshold (0.0-1.0)
    #[serde(default = "default_warning_threshold")]
    pub warning_threshold: f32,
    /// Critical threshold (0.0-1.0)
    #[serde(default = "default_critical_threshold")]
    pub critical_threshold: f32,
}

fn default_context_window() -> u64 { 200_000 }
fn default_reserved_system() -> u64 { 10_000 }
fn default_reserved_response() -> u64 { 8_000 }
fn default_warning_threshold() -> f32 { 0.70 }
fn default_critical_threshold() -> f32 { 0.90 }

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            context_window: 200_000,
            reserved_system: 10_000,
            reserved_response: 8_000,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
        }
    }
}

impl BudgetConfig {
    /// Calculate the actual available tokens for user content and memory.
    pub fn available_tokens(&self) -> u64 {
        self.context_window
            .saturating_sub(self.reserved_system)
            .saturating_sub(self.reserved_response)
    }

    /// Get the warning threshold in actual token count.
    pub fn warning_tokens(&self) -> u64 {
        (self.context_window as f64 * self.warning_threshold as f64) as u64
    }

    /// Get the critical threshold in actual token count.
    pub fn critical_tokens(&self) -> u64 {
        (self.context_window as f64 * self.critical_threshold as f64) as u64
    }

    /// Create a budget optimized for a specific context window size.
    pub fn for_context_window(context_window: u64) -> Self {
        let reserved_system = ((context_window as f64 * 0.05).min(20_000.0)) as u64;
        let reserved_response = ((context_window as f64 * 0.04).min(16_000.0)) as u64;
        Self {
            context_window,
            reserved_system,
            reserved_response,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
        }
    }
}

/// Memory drift detection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftConfig {
    /// Staleness decay factor applied per day of inactivity.
    #[serde(default = "default_decay")]
    pub staleness_decay_per_day: f32,
    /// Staleness score above which an entry is flagged for review.
    #[serde(default = "default_review_threshold")]
    pub review_threshold: f32,
    /// Staleness score above which an entry is automatically pruned.
    #[serde(default = "default_prune_threshold")]
    pub prune_threshold: f32,
    /// Jaccard similarity threshold for contradiction detection.
    #[serde(default = "default_jaccard_threshold")]
    pub contradiction_jaccard_threshold: f32,
}

fn default_decay() -> f32 { 0.02 }
fn default_review_threshold() -> f32 { 0.7 }
fn default_prune_threshold() -> f32 { 0.95 }
fn default_jaccard_threshold() -> f32 { 0.6 }

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            staleness_decay_per_day: 0.02,
            review_threshold: 0.7,
            prune_threshold: 0.95,
            contradiction_jaccard_threshold: 0.6,
        }
    }
}

/// Performance budget for memory operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfBudget {
    #[serde(default = "default_hook_max_ms")]
    pub hook_max_ms: u64,
    #[serde(default = "default_inject_max_ms")]
    pub inject_max_ms: u64,
    #[serde(default = "default_warn_threshold_pct")]
    pub warn_threshold_pct: f64,
}

fn default_hook_max_ms() -> u64 { 500 }
fn default_inject_max_ms() -> u64 { 100 }
fn default_warn_threshold_pct() -> f64 { 0.8 }

impl Default for PerfBudget {
    fn default() -> Self {
        Self { hook_max_ms: 500, inject_max_ms: 100, warn_threshold_pct: 0.8 }
    }
}

/// Model-specific profile for adaptive compression thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelProfile {
    pub model_name: String,
    pub context_window: u64,
    pub memory_budget_ratio: f32,
    pub warning_threshold: f32,
    pub critical_threshold: f32,
    pub micro_threshold: usize,
    pub session_threshold: usize,
    pub compression_aggressiveness: f32,
}

impl Default for ModelProfile {
    fn default() -> Self {
        Self {
            model_name: "default".to_string(),
            context_window: 200_000,
            memory_budget_ratio: 0.30,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
            micro_threshold: 50,
            session_threshold: 10,
            compression_aggressiveness: 0.5,
        }
    }
}

impl ModelProfile {
    /// Find or create a profile for a given model name.
    pub fn for_model(model_name: &str) -> Self {
        let name_lower = model_name.to_lowercase();
        if name_lower.contains("haiku") || name_lower.contains("flash")
            || name_lower.contains("04-mini") || name_lower.contains("gpt-3.5-turbo")
        {
            if name_lower.contains("gpt-3.5-turbo") {
                Self {
                    model_name: model_name.to_string(), context_window: 16_385,
                    memory_budget_ratio: 0.15, warning_threshold: 0.50,
                    critical_threshold: 0.75, micro_threshold: 20,
                    session_threshold: 4, compression_aggressiveness: 0.75,
                }
            } else {
                Self {
                    model_name: model_name.to_string(), context_window: 8_192,
                    memory_budget_ratio: 0.10, warning_threshold: 0.40,
                    critical_threshold: 0.65, micro_threshold: 10,
                    session_threshold: 2, compression_aggressiveness: 0.85,
                }
            }
        } else if name_lower.contains("claude-3-5-sonnet") || name_lower.contains("claude-3.5") {
            Self {
                model_name: model_name.to_string(), context_window: 200_000,
                memory_budget_ratio: 0.35, warning_threshold: 0.70,
                critical_threshold: 0.90, micro_threshold: 50,
                session_threshold: 10, compression_aggressiveness: 0.5,
            }
        } else if name_lower.contains("claude-3-opus") {
            Self {
                model_name: model_name.to_string(), context_window: 200_000,
                memory_budget_ratio: 0.35, warning_threshold: 0.70,
                critical_threshold: 0.90, micro_threshold: 50,
                session_threshold: 10, compression_aggressiveness: 0.5,
            }
        } else if name_lower.contains("claude") {
            Self {
                model_name: model_name.to_string(), context_window: 200_000,
                memory_budget_ratio: 0.35, warning_threshold: 0.70,
                critical_threshold: 0.90, micro_threshold: 50,
                session_threshold: 10, compression_aggressiveness: 0.5,
            }
        } else if name_lower.contains("gpt-4o") {
            Self {
                model_name: model_name.to_string(), context_window: 128_000,
                memory_budget_ratio: 0.30, warning_threshold: 0.70,
                critical_threshold: 0.88, micro_threshold: 45,
                session_threshold: 9, compression_aggressiveness: 0.55,
            }
        } else if name_lower.contains("o1-preview") || name_lower.contains("o1-mini") {
            Self {
                model_name: model_name.to_string(), context_window: 128_000,
                memory_budget_ratio: 0.20, warning_threshold: 0.60,
                critical_threshold: 0.80, micro_threshold: 30,
                session_threshold: 6, compression_aggressiveness: 0.7,
            }
        } else if name_lower.contains("gpt-4") {
            Self {
                model_name: model_name.to_string(), context_window: 128_000,
                memory_budget_ratio: 0.25, warning_threshold: 0.65,
                critical_threshold: 0.85, micro_threshold: 40,
                session_threshold: 8, compression_aggressiveness: 0.6,
            }
        } else {
            let mut profile = Self::default();
            profile.model_name = model_name.to_string();
            profile
        }
    }
}

/// LLM summarization configuration for compression pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmSummarizerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_llm_model")]
    pub model: String,
}

impl LlmSummarizerConfig {
    /// Check if LLM summarization is properly configured.
    pub fn is_configured(&self) -> bool {
        self.enabled && !self.api_url.is_empty() && !self.api_key.is_empty()
    }
}

fn default_llm_model() -> String { "gpt-4o-mini".to_string() }

impl Eq for LlmSummarizerConfig {}

impl Default for LlmSummarizerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_url: String::new(),
            api_key: String::new(),
            model: "gpt-4o-mini".to_string(),
        }
    }
}

// ── Memory-specific types ────────────────────────────────────────────────

/// Top-level memory system configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    pub store: StoreConfig,
    pub compression: CompressionConfig,
    pub budget: BudgetConfig,
    pub extractor: ExtractorConfig,
    pub drift: DriftConfig,
    pub perf: PerfBudget,
    /// Target model name for adaptive compression thresholds.
    /// When set, compression parameters auto-adjust based on model profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            store: StoreConfig::default(),
            compression: CompressionConfig::default(),
            budget: BudgetConfig::default(),
            extractor: ExtractorConfig::default(),
            drift: DriftConfig::default(),
            perf: PerfBudget::default(),
            model: None,
        }
    }
}

/// Storage backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Path to the SQLite database file.
    pub sqlite_path: PathBuf,
    /// Directory for blob / file-system storage.
    pub blob_dir: PathBuf,
    /// Whether to enable the in-process vector index.
    pub enable_vector_index: bool,
    /// Maximum number of entries kept in the hot-cache.
    pub cache_capacity: usize,
    /// Vector embedding configuration.
    pub vector: VectorConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            sqlite_path: PathBuf::from("memory.db"),
            blob_dir: PathBuf::from("memory_blobs"),
            enable_vector_index: false,
            cache_capacity: 512,
            vector: VectorConfig::default(),
        }
    }
}

/// Remote embedding model configuration.
///
/// Supports OpenAI-compatible API format (also works with Ollama, vLLM, etc.).
/// When `model` or `api_url` is empty, the vector index operates in local-only
/// mode without generating embeddings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Whether remote embedding is enabled.
    pub enabled: bool,
    /// Embedding model name, e.g. `"text-embedding-3-small"`.
    pub model: String,
    /// Embedding API endpoint URL, e.g. `"https://api.openai.com/v1/embeddings"`.
    /// Supports OpenAI-compatible API format.
    pub api_url: String,
    /// API key for the embedding service.
    /// Can also be provided via the `CC_VECTOR_API_KEY` environment variable.
    pub api_key: String,
    /// Expected vector dimension (0 = auto-detect from first embedding call).
    pub dimension: usize,
    /// Timeout for embedding API calls in seconds.
    pub timeout_secs: u64,
    /// Maximum batch size for embedding requests.
    pub batch_size: usize,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            model: String::new(),
            api_url: String::new(),
            api_key: String::new(),
            dimension: 0,
            timeout_secs: 30,
            batch_size: 32,
        }
    }
}

/// Compression pipeline configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Minimum number of micro-entries before stage-1 kicks in.
    pub micro_threshold: usize,
    /// Minimum number of session summaries before stage-2 kicks in.
    pub session_threshold: usize,
    /// Enable stage-3 deep compression (requires LLM call).
    pub enable_deep_compression: bool,
    /// How aggressively to compress (0.0 = lossless, 1.0 = maximum).
    pub aggressiveness: f32,
    /// LLM summarization configuration for semantic compression.
    #[serde(default)]
    pub llm: LlmSummarizerConfig,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            micro_threshold: 50,
            session_threshold: 10,
            enable_deep_compression: true,
            aggressiveness: 0.5,
            llm: LlmSummarizerConfig::default(),
        }
    }
}

/// Background memory extractor configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorConfig {
    /// How often (in seconds) the extractor polls for new content.
    pub poll_interval_secs: u64,
    /// Maximum number of entries extracted per poll cycle.
    pub batch_size: usize,
    /// Minimum confidence score to keep an extracted entry.
    pub min_confidence: f32,
}

impl Default for ExtractorConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
        }
    }
}

// ── MemoryConfig methods ────────────────────────────────────────────────

impl MemoryConfig {
    /// Update configuration based on model profile.
    pub fn with_model_profile(mut self, model_name: &str) -> Self {
        let profile = ModelProfile::for_model(model_name);

        self.model = Some(model_name.to_string());
        self.budget = BudgetConfig::for_context_window(profile.context_window);
        self.compression.micro_threshold = profile.micro_threshold;
        self.compression.session_threshold = profile.session_threshold;
        self.compression.aggressiveness = profile.compression_aggressiveness;

        self
    }

    /// Get the recommended memory budget for the current model profile.
    pub fn recommended_memory_budget(&self) -> u64 {
        let available = self.budget.context_window
            - self.budget.reserved_system
            - self.budget.reserved_response;

        let profile = ModelProfile::for_model(&self.model_name().unwrap_or_default());

        (available as f64 * profile.memory_budget_ratio as f64) as u64
    }

    /// Get the model name (if set).
    pub fn model_name(&self) -> Option<String> {
        self.model.as_ref().map(|m| m.clone())
    }

    /// Set the target model name for adaptive compression.
    pub fn set_model(&mut self, model_name: String) {
        self.model = Some(model_name);
    }

    /// Override configuration from environment variables.
    ///
    /// Supported environment variables:
    /// - `CC_LLM_API_URL`: LLM API URL for summarization
    /// - `CC_LLM_API_KEY`: LLM API key
    /// - `CC_LLM_MODEL`: LLM model name
    /// - `CC_VECTOR_API_URL`: Vector embedding API URL
    /// - `CC_VECTOR_API_KEY`: Vector embedding API key
    pub fn with_env_overrides(mut self) -> Self {
        // LLM summarization overrides
        if let Ok(url) = std::env::var("CC_LLM_API_URL") {
            if !url.is_empty() {
                self.compression.llm.api_url = url;
            }
        }
        if let Ok(key) = std::env::var("CC_LLM_API_KEY") {
            if !key.is_empty() {
                self.compression.llm.api_key = key;
            }
        }
        if let Ok(model) = std::env::var("CC_LLM_MODEL") {
            if !model.is_empty() {
                self.compression.llm.model = model;
            }
        }
        if let Ok(enabled) = std::env::var("CC_LLM_ENABLED") {
            self.compression.llm.enabled = enabled.eq_ignore_ascii_case("true")
                || enabled.eq_ignore_ascii_case("1")
                || enabled.eq_ignore_ascii_case("yes");
        }

        // Vector embedding overrides
        if let Ok(url) = std::env::var("CC_VECTOR_API_URL") {
            if !url.is_empty() {
                self.store.vector.api_url = url;
            }
        }
        if let Ok(key) = std::env::var("CC_VECTOR_API_KEY") {
            if !key.is_empty() {
                self.store.vector.api_key = key;
            }
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_profile_detection() {
        let claude = ModelProfile::for_model("claude-3-5-sonnet-20241022");
        assert_eq!(claude.context_window, 200_000);
        assert_eq!(claude.memory_budget_ratio, 0.35);

        let gpt4o = ModelProfile::for_model("gpt-4o");
        assert_eq!(gpt4o.context_window, 128_000);

        let o1 = ModelProfile::for_model("o1-preview");
        assert_eq!(o1.context_window, 128_000);
        assert_eq!(o1.compression_aggressiveness, 0.7);
    }

    #[test]
    fn test_memory_config_with_model() {
        let config = MemoryConfig::default()
            .with_model_profile("gpt-4o");

        assert_eq!(config.budget.context_window, 128_000);
        assert_eq!(config.compression.micro_threshold, 45);
    }

    #[test]
    fn test_recommended_memory_budget() {
        let config = MemoryConfig::default()
            .with_model_profile("claude-3-5-sonnet-20241022");

        let budget = config.recommended_memory_budget();
        assert!(budget > 60000 && budget < 65000);
    }

    #[test]
    fn test_budget_config_dynamic_calculation() {
        let budget = BudgetConfig::for_context_window(128_000);

        assert_eq!(budget.context_window, 128_000);
        assert_eq!(budget.warning_threshold, 0.70);
        assert_eq!(budget.critical_threshold, 0.90);

        let available = budget.available_tokens();
        assert_eq!(available, 116_480);

        let warning = budget.warning_tokens();
        assert!(warning >= 89500 && warning <= 89700);

        let critical = budget.critical_tokens();
        assert!(critical >= 115000 && critical <= 116000);
    }

    #[test]
    fn test_budget_config_for_large_context() {
        let budget = BudgetConfig::for_context_window(200_000);

        assert!(budget.reserved_system <= 20_000);
        assert!(budget.reserved_response <= 16_000);
        assert!(budget.available_tokens() > 160_000);
    }

    #[test]
    fn test_small_model_aggressive_compression() {
        let small = ModelProfile::for_model("claude-3-5-haiku-20241022");
        assert_eq!(small.context_window, 8_192);
        assert!(small.compression_aggressiveness >= 0.8);
        assert!(small.memory_budget_ratio <= 0.15);

        let detected = ModelProfile::for_model("gpt-3.5-turbo-1106");
        assert_eq!(detected.context_window, 16_385);

        let mini = ModelProfile::for_model("04-mini");
        assert_eq!(mini.context_window, 8_192);
    }
}
