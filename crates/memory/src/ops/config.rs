//! Configuration structures for the memory system.
//!
//! All config types are now self-contained — the unified `config` crate
//! has been removed and its types were inlined into their respective
//! consumer crates.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::types::TokenBudget;

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
    /// True when runtime has already derived the final lease for this turn.
    /// In that mode memory must not apply another role multiplier.
    #[serde(default)]
    pub runtime_managed: bool,
    #[serde(default)]
    pub selected_item_limit: usize,
    #[serde(default)]
    pub l0_reserved: u64,
    #[serde(default)]
    pub l1_working: u64,
    #[serde(default)]
    pub l2_project: u64,
    #[serde(default)]
    pub l3_deep: u64,
    #[serde(default)]
    pub l3_checkpoint: u64,
    #[serde(default)]
    pub l4_shared: u64,
}

fn default_context_window() -> u64 {
    200_000
}
fn default_reserved_system() -> u64 {
    10_000
}
fn default_reserved_response() -> u64 {
    8_000
}
fn default_warning_threshold() -> f32 {
    0.70
}
fn default_critical_threshold() -> f32 {
    0.90
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            context_window: 200_000,
            reserved_system: 10_000,
            reserved_response: 8_000,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
            runtime_managed: false,
            selected_item_limit: 0,
            l0_reserved: 0,
            l1_working: 0,
            l2_project: 0,
            l3_deep: 0,
            l3_checkpoint: 0,
            l4_shared: 0,
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
            runtime_managed: false,
            selected_item_limit: 0,
            l0_reserved: 0,
            l1_working: 0,
            l2_project: 0,
            l3_deep: 0,
            l3_checkpoint: 0,
            l4_shared: 0,
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
    /// Staleness threshold above which Low-priority entries are evicted (L1).
    #[serde(default = "default_low_priority_prune_threshold")]
    pub low_priority_prune_threshold: f32,
}

fn default_decay() -> f32 {
    0.02
}
fn default_review_threshold() -> f32 {
    0.7
}
fn default_prune_threshold() -> f32 {
    0.95
}
fn default_jaccard_threshold() -> f32 {
    0.6
}
fn default_low_priority_prune_threshold() -> f32 {
    0.8
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            staleness_decay_per_day: 0.02,
            review_threshold: 0.7,
            prune_threshold: 0.95,
            contradiction_jaccard_threshold: 0.6,
            low_priority_prune_threshold: 0.8,
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

fn default_hook_max_ms() -> u64 {
    500
}
fn default_inject_max_ms() -> u64 {
    100
}
fn default_warn_threshold_pct() -> f64 {
    0.8
}

impl Default for PerfBudget {
    fn default() -> Self {
        Self {
            hook_max_ms: 500,
            inject_max_ms: 100,
            warn_threshold_pct: 0.8,
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

    /// Resolve the effective API key, using environment variable `CC_LLM_API_KEY`
    /// in preference to the config file value.
    pub fn resolved_api_key(&self) -> String {
        std::env::var("CC_LLM_API_KEY")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.api_key.clone())
    }

    /// Resolve the effective API URL, using environment variable `CC_LLM_API_URL`
    /// in preference to the config file value.
    pub fn resolved_api_url(&self) -> String {
        std::env::var("CC_LLM_API_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.api_url.clone())
    }
}

fn default_llm_model() -> String {
    "gpt-4o-mini".to_string()
}

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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryConfig {
    pub store: StoreConfig,
    pub compression: CompressionConfig,
    pub budget: BudgetConfig,
    pub layers: LayerConfig,
    pub extractor: ExtractorConfig,
    pub governance: GovernanceConfig,
    pub drift: DriftConfig,
    pub perf: PerfBudget,
    pub tuning: TuningConfig,
}

/// Autonomous memory and knowledge maintenance policy.
///
/// Foreground writes keep using the synchronous authority and duplicate gates.
/// This policy controls the bounded deep pass that reconciles residual state
/// without adding provider latency to a user turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernanceConfig {
    pub enabled: bool,
    pub startup_delay_secs: u64,
    pub deep_scan_hour_local: u8,
    pub max_candidates: usize,
    pub stale_threshold_bp: u16,
    pub low_confidence_threshold_bp: u16,
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            startup_delay_secs: 30,
            deep_scan_hour_local: 3,
            max_candidates: 256,
            stale_threshold_bp: 9_800,
            low_confidence_threshold_bp: 4_500,
        }
    }
}

/// Runtime-governed memory layer settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerConfig {
    pub l0_enabled: bool,
    pub l1_max_tokens: u32,
    pub l2_max_tokens: u32,
    pub l3_search_limit: u32,
    pub l4_enabled: bool,
}

impl Default for LayerConfig {
    fn default() -> Self {
        Self {
            l0_enabled: true,
            l1_max_tokens: 2000,
            l2_max_tokens: 3000,
            l3_search_limit: 5,
            l4_enabled: false,
        }
    }
}

/// Tunable thresholds that were previously hard-coded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TuningConfig {
    #[serde(default = "default_sandbox_min_lines")]
    pub sandbox_min_lines: usize,
    #[serde(default = "default_rebuild_confidence")]
    pub rebuild_confidence: f32,
    #[serde(default = "default_freshness_trigger")]
    pub freshness_trigger_ratio: f32,
    #[serde(default = "default_closet_rebuild_ticks")]
    pub closet_rebuild_ticks: u32,
    #[serde(default = "default_audit_truncate_len")]
    pub audit_truncate_len: usize,
    #[serde(default = "default_prefetch_hot_topics")]
    pub prefetch_hot_topics: usize,
    /// TTL in seconds for the L0 (identity) layer cache (default: 86400 = 24h).
    #[serde(default = "default_l0_cache_ttl")]
    pub l0_cache_ttl_secs: u64,
    /// TTL in seconds for the L1 (core/working) layer cache (default: 3600 = 1h).
    #[serde(default = "default_l1_cache_ttl")]
    pub l1_cache_ttl_secs: u64,
    /// TTL in seconds for the L2 (project) layer cache (default: 300 = 5min).
    #[serde(default = "default_l2_cache_ttl")]
    pub l2_cache_ttl_secs: u64,
    /// TTL in milliseconds for the whole `prepare_context` request cache.
    #[serde(default = "default_prepare_context_cache_ttl_ms")]
    pub prepare_context_cache_ttl_ms: u64,
}

fn default_sandbox_min_lines() -> usize {
    2000
}
fn default_rebuild_confidence() -> f32 {
    0.3
}
fn default_freshness_trigger() -> f32 {
    0.8
}
fn default_closet_rebuild_ticks() -> u32 {
    10
}
fn default_audit_truncate_len() -> usize {
    120
}
fn default_prefetch_hot_topics() -> usize {
    5
}
fn default_l0_cache_ttl() -> u64 {
    86400
}
fn default_l1_cache_ttl() -> u64 {
    3600
}
fn default_l2_cache_ttl() -> u64 {
    300
}
fn default_prepare_context_cache_ttl_ms() -> u64 {
    500
}
impl Default for TuningConfig {
    fn default() -> Self {
        Self {
            sandbox_min_lines: 2000,
            rebuild_confidence: 0.3,
            freshness_trigger_ratio: 0.8,
            closet_rebuild_ticks: 10,
            audit_truncate_len: 120,
            prefetch_hot_topics: 5,
            l0_cache_ttl_secs: 86400,
            l1_cache_ttl_secs: 3600,
            l2_cache_ttl_secs: 300,
            prepare_context_cache_ttl_ms: 500,
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
        let registry = storage::StorageRegistry::default_for_config_home(default_config_home());
        let sqlite_path = registry
            .endpoint(&storage::StorageDomainId::Memory)
            .map(|endpoint| endpoint.as_handle().path)
            .unwrap_or_else(|_| registry.layout.root.join("memory.sqlite"));
        let blob_dir = registry
            .endpoint(&storage::StorageDomainId::Blobs)
            .map(|endpoint| endpoint.as_handle().path)
            .unwrap_or_else(|_| registry.layout.blobs.clone());
        Self {
            sqlite_path,
            blob_dir,
            enable_vector_index: false,
            cache_capacity: 512,
            vector: VectorConfig::default(),
        }
    }
}

fn default_config_home() -> PathBuf {
    if let Some(path) = std::env::var_os("COWD_CONFIG_HOME") {
        return PathBuf::from(path);
    }

    let dot_dir = std::env::var("COWD_DIR_NAME").unwrap_or_else(|_| ".cowd".to_string());
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(dot_dir)
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

impl VectorConfig {
    /// Resolve the effective API key, using environment variable `COWD_VECTOR_API_KEY`
    /// in preference to the config file value.
    pub fn resolved_api_key(&self) -> String {
        std::env::var("COWD_VECTOR_API_KEY")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.api_key.clone())
    }

    /// Resolve the effective API URL, using environment variable `COWD_MEMORY_VECTOR_API_URL`
    /// in preference to the config file value.
    pub fn resolved_api_url(&self) -> String {
        std::env::var("COWD_MEMORY_VECTOR_API_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.api_url.clone())
    }

    /// Resolve the effective model name, using environment variable `COWD_MEMORY_VECTOR_MODEL`
    /// in preference to the config file value.
    pub fn resolved_model(&self) -> String {
        std::env::var("COWD_MEMORY_VECTOR_MODEL")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.model.clone())
    }
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
    /// Whether post-turn automatic extraction is enabled.
    pub enabled: bool,
    /// How often (in seconds) the extractor polls for new content.
    pub poll_interval_secs: u64,
    /// Maximum number of entries extracted per poll cycle.
    pub batch_size: usize,
    /// Minimum confidence score to keep an extracted entry.
    pub min_confidence: f32,
    /// Debounce window (in seconds) for background LLM extraction (Pass 5).
    /// Prevents the LLM from being invoked on every single turn — results are
    /// batched within this window to reduce API costs.
    pub extractor_debounce_secs: u64,
}

impl Default for ExtractorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
            extractor_debounce_secs: 30,
        }
    }
}

// ── BudgetCalculator ────────────────────────────────────────────────────

/// 统一的Token预算计算器, 消除分散在3处的计算逻辑
#[derive(Debug, Clone)]
pub struct BudgetCalculator {
    config: BudgetConfig,
}

impl BudgetCalculator {
    pub fn new(config: BudgetConfig) -> Self {
        Self { config }
    }

    pub fn base_available(&self) -> u64 {
        self.config
            .context_window
            .saturating_sub(self.config.reserved_system)
            .saturating_sub(self.config.reserved_response)
    }

    pub fn make_budget(&self) -> TokenBudget {
        TokenBudget {
            total: self.config.context_window,
            reserved_system: self.config.reserved_system,
            reserved_response: self.config.reserved_response,
            allocated_memory: 0,
            allocated_conversation: 0,
            available: self.base_available(),
        }
    }

    pub fn make_role_budget(&self, role: &str) -> TokenBudget {
        let multiplier = Self::role_multiplier(role);
        let role_available = (self.base_available() as f64 * multiplier) as u64;
        TokenBudget {
            total: self.config.context_window,
            reserved_system: self.config.reserved_system,
            reserved_response: self.config.reserved_response,
            allocated_memory: 0,
            allocated_conversation: 0,
            available: role_available,
        }
    }

    pub fn role_multiplier(role: &str) -> f64 {
        match role {
            "Planner" => 0.40,
            "Executor" => 0.25,
            "Reviewer" => 0.15,
            _ => 0.50,
        }
    }

    pub fn warning_tokens(&self) -> u64 {
        (self.base_available() as f64 * self.config.warning_threshold as f64) as u64
    }

    pub fn critical_tokens(&self) -> u64 {
        (self.base_available() as f64 * self.config.critical_threshold as f64) as u64
    }

    /// 计算某层的token预算
    pub fn layer_budget(&self, layer: crate::types::MemoryLayer, already_used: u64) -> u64 {
        let ratio = Self::layer_allocation_ratio(layer);
        let base = self.base_available().saturating_sub(already_used);
        ((base as f64) * ratio) as u64
    }

    /// 层分配比例
    pub fn layer_allocation_ratio(layer: crate::types::MemoryLayer) -> f64 {
        match layer {
            crate::types::MemoryLayer::L0 => 0.01,
            crate::types::MemoryLayer::L1 => 0.15,
            crate::types::MemoryLayer::L2 => 0.20,
            crate::types::MemoryLayer::L3 => 0.40,
            crate::types::MemoryLayer::L4 => 0.10,
        }
    }
}

// ── MemoryConfig methods ────────────────────────────────────────────────

impl MemoryConfig {
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
    fn test_budget_config_dynamic_calculation() {
        let budget = BudgetConfig::for_context_window(128_000);

        assert_eq!(budget.context_window, 128_000);
        assert_eq!(budget.warning_threshold, 0.70);
        assert_eq!(budget.critical_threshold, 0.90);

        let available = budget.available_tokens();
        assert_eq!(available, 116_480);

        let warning = budget.warning_tokens();
        assert!((89500..=89700).contains(&warning));

        let critical = budget.critical_tokens();
        assert!((115000..=116000).contains(&critical));
    }

    #[test]
    fn test_budget_config_for_large_context() {
        let budget = BudgetConfig::for_context_window(200_000);

        assert!(budget.reserved_system <= 20_000);
        assert!(budget.reserved_response <= 16_000);
        assert!(budget.available_tokens() > 160_000);
    }
}
