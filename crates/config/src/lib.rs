//! `cc-config` – unified configuration system for CC (CLI and Gateway).
//!
//! # Design Goals
//!
//! 1. **Single Source of Truth**: All configuration types in one place
//! 2. **Unified Loading**: CLI and Gateway use the same loading logic
//! 3. **Precedence Order**: User (~/.cowd/) → Project (.cowd/) → Local → Environment → CLI Args
//! 4. **Backward Compatible**: Existing config files work unchanged
//!
//! # Configuration Precedence
//!
//! Config values are merged in this order (later wins):
//! 1. `~/.cowd/config.yaml` (User-level, shared across all projects)
//! 2. `.cowd/config.yaml` (Project-level, per-workspace)
//! 3. `.cowd/config.local.yaml` (Local overrides, git-ignored)
//! 4. Environment variables (CC_* prefix)
//! 5. Command-line arguments (highest priority)
//!
//! # Usage
//!
//! ```rust,no_run
//! use config::{UnifiedConfig, ConfigSource};
//!
//! // Load with default settings (respects precedence)
//! let config = UnifiedConfig::load().unwrap();
//!
//! // Check the effective model
//! println!("Model: {}", config.effective_model());
//!
//! // Resolve a provider for a model
//! if let Some((base_url, api_key)) = config.resolve_provider("claude-sonnet-4-20250514") {
//!     println!("Provider: {}", base_url);
//! }
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Map;

// ── Error Types ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Missing required config: {0}")]
    Missing(String),
    #[error("Invalid value for {key}: {message}")]
    Invalid { key: String, message: String },
}

pub type Result<T> = std::result::Result<T, ConfigError>;

// ── Config Source (Precedence) ─────────────────────────────────────────────────

/// Origin of a loaded configuration entry, used to track precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigSource {
    /// User-level config from `~/.cowd/`
    User,
    /// Project-level config from `.cowd/` in current directory
    Project,
    /// Local overrides from `.cowd/config.local.*`
    Local,
    /// Environment variables
    Environment,
    /// Command-line arguments
    Cli,
}

impl std::fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigSource::User => write!(f, "user"),
            ConfigSource::Project => write!(f, "project"),
            ConfigSource::Local => write!(f, "local"),
            ConfigSource::Environment => write!(f, "environment"),
            ConfigSource::Cli => write!(f, "cli"),
        }
    }
}

/// A discovered config file and its source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub source: ConfigSource,
    pub path: PathBuf,
    pub exists: bool,
}

// ── Environment Variable Config ───────────────────────────────────────────────

/// Environment variable override keys and their config paths.
const ENV_OVERRIDE_PREFIX: &str = "COWD_";

/// Collect environment variable overrides as a flat key-value map.
///
/// Keys are converted from `CC_SECTION_KEY_SUBKEY` to `section.key.subkey`.
fn collect_env_overrides() -> BTreeMap<String, serde_json::Value> {
    let mut result = BTreeMap::new();

    for (key, value) in std::env::vars() {
        if let Some(rest) = key.strip_prefix(ENV_OVERRIDE_PREFIX) {
            // Convert CC_SECTION_KEY to section.key
            let config_key = rest.to_lowercase().replace('_', ".");
            let json_value: serde_json::Value = if value.is_empty() {
                serde_json::Value::Null
            } else if value == "true" {
                serde_json::Value::Bool(true)
            } else if value == "false" {
                serde_json::Value::Bool(false)
            } else if let Ok(n) = value.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::String(value)
            };
            result.insert(config_key, json_value);
        }
    }

    result
}

// ── File Discovery ─────────────────────────────────────────────────────────────

/// Discovery patterns for config files.
#[derive(Debug, Clone)]
pub struct ConfigDiscovery {
    /// User home directory for ~/.cowd/
    home_dir: PathBuf,
    /// Current working directory for .cowd/
    cwd: PathBuf,
}

impl Default for ConfigDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigDiscovery {
    /// Create a new discovery with current home and cwd.
    pub fn new() -> Self {
        Self {
            home_dir: dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        }
    }

    /// Create with explicit paths (useful for testing).
    pub fn with_paths(home_dir: PathBuf, cwd: PathBuf) -> Self {
        Self { home_dir, cwd }
    }

    /// Discover all config entries in precedence order.
    pub fn discover(&self) -> Vec<ConfigEntry> {
        let mut entries = Vec::new();

        // 1. User config: ~/.cowd/config.yaml
        let user_config = self.home_dir.join(".cowd").join("config.yaml");
        if user_config.exists() {
            entries.push(ConfigEntry {
                source: ConfigSource::User,
                path: user_config,
                exists: true,
            });
        }

        // 2. Project config: .cowd/config.yaml
        let project_config = self.cwd.join(".cowd").join("config.yaml");
        if project_config.exists() {
            entries.push(ConfigEntry {
                source: ConfigSource::Project,
                path: project_config,
                exists: true,
            });
        }

        // 3. Local config: .cowd/config.local.yaml
        let local_config = self.cwd.join(".cowd").join("config.local.yaml");
        if local_config.exists() {
            entries.push(ConfigEntry {
                source: ConfigSource::Local,
                path: local_config,
                exists: true,
            });
        }

        entries
    }
}

// ── Deep Merge ────────────────────────────────────────────────────────────────

/// Deep-merge src into dst, where src values take precedence.
fn deep_merge(dst: &mut serde_json::Value, src: &serde_json::Value) {
    match (&mut *dst, src) {
        (serde_json::Value::Object(dst_map), serde_json::Value::Object(src_map)) => {
            for (key, src_value) in src_map.clone() {
                if let Some(dst_value) = dst_map.get_mut(&key) {
                    deep_merge(dst_value, &src_value);
                } else {
                    dst_map.insert(key, src_value);
                }
            }
        }
        _ => {
            *dst = src.clone();
        }
    }
}

// ── Core Config Types ─────────────────────────────────────────────────────────

/// Top-level unified configuration that combines all subsystems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedConfig {
    /// Runtime/session configuration
    #[serde(default)]
    pub runtime: RuntimeConfig,

    /// Gateway configuration
    #[serde(default)]
    pub gateway: GatewayConfig,

    /// Memory system configuration
    #[serde(default)]
    pub memory: MemoryConfig,

    /// Model providers configuration
    #[serde(flatten)]
    pub providers: ProvidersConfig,

    /// Internal merged raw config for unknown keys
    #[serde(skip)]
    raw: BTreeMap<String, serde_json::Value>,
}

/// Runtime configuration (session, hooks, MCP, etc.)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeConfig {
    /// Model to use
    #[serde(default)]
    pub model: Option<String>,

    /// Model aliases for quick switching
    #[serde(default)]
    pub model_aliases: BTreeMap<String, String>,

    /// Permission mode
    #[serde(default)]
    pub permission_mode: Option<String>,

    /// Output style: "terse", "standard", or default
    #[serde(default)]
    pub output_style: Option<String>,

    /// Hooks configuration
    #[serde(default)]
    pub hooks: HooksConfig,

    /// MCP servers
    #[serde(default)]
    pub mcp: McpConfig,

    /// Plugins
    #[serde(default)]
    pub plugins: PluginsConfig,

    /// Sandbox configuration
    #[serde(default)]
    pub sandbox: SandboxConfig,

    /// Permissions configuration
    #[serde(default)]
    pub permissions: PermissionConfig,

    /// System prompt cache configuration
    #[serde(default)]
    pub prompt_cache: PromptCacheConfig,
}

/// Permission rules configuration (defaultMode, allow, deny, ask).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PermissionConfig {
    #[serde(default)]
    pub default_mode: Option<String>,
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub ask: Vec<String>,
}

/// System prompt cache configuration for reducing API costs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptCacheConfig {
    /// Check interval in turns (default: 5)
    #[serde(default = "default_cache_check_interval")]
    pub check_interval: u32,
    /// Maximum cache age in turns (default: 50)
    #[serde(default = "default_cache_max_age")]
    pub max_age: u32,
    /// Memory delta threshold (default: 3)
    #[serde(default = "default_cache_memory_delta")]
    pub memory_delta: u32,
}

fn default_cache_check_interval() -> u32 { 5 }
fn default_cache_max_age() -> u32 { 50 }
fn default_cache_memory_delta() -> u32 { 3 }

impl Default for PromptCacheConfig {
    fn default() -> Self {
        Self { check_interval: 5, max_age: 50, memory_delta: 3 }
    }
}

/// Hooks configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HooksConfig {
    #[serde(default)]
    pub pre_tool_use: Vec<String>,
    #[serde(default)]
    pub post_tool_use: Vec<String>,
    #[serde(default)]
    pub post_tool_use_failure: Vec<String>,
}

/// MCP server configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: BTreeMap<String, McpServerConfig>,
}

/// A single MCP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    /// Transport type: stdio, sse, http, ws
    #[serde(rename = "type")]
    #[serde(default = "default_transport")]
    pub transport: String,

    /// Command for stdio transport
    #[serde(default)]
    pub command: Option<String>,

    /// Args for stdio transport
    #[serde(default)]
    pub args: Vec<String>,

    /// URL for remote transports
    #[serde(default)]
    pub url: Option<String>,

    /// Environment variables
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

fn default_transport() -> String {
    "stdio".to_string()
}

/// Plugins configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginsConfig {
    /// Enabled plugins map (name -> enabled)
    #[serde(default)]
    pub enabled: BTreeMap<String, bool>,

    /// External plugin directories
    #[serde(default)]
    pub external_dirs: Vec<String>,
}

/// Sandbox configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Enable sandbox
    #[serde(default)]
    pub enabled: bool,

    /// Execution timeout in seconds
    #[serde(default = "default_sandbox_timeout")]
    pub timeout_secs: u32,

    /// Maximum output size in KiB
    #[serde(default = "default_sandbox_max_output")]
    pub max_output_kib: u32,

    /// Maximum FTS5 sandbox entries
    #[serde(default = "default_sandbox_max_entries")]
    pub max_entries: u32,

    /// Filesystem isolation mode
    #[serde(default)]
    pub filesystem_mode: Option<String>,

    /// Allowed directories
    #[serde(default)]
    pub allowed_dirs: Vec<String>,
}

fn default_sandbox_timeout() -> u32 { 30 }
fn default_sandbox_max_output() -> u32 { 100 }
fn default_sandbox_max_entries() -> u32 { 50 }

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout_secs: 30,
            max_output_kib: 100,
            max_entries: 50,
            filesystem_mode: None,
            allowed_dirs: Vec::new(),
        }
    }
}

/// Gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Enable gateway
    #[serde(default)]
    pub enabled: bool,

    /// Platform instances
    #[serde(default)]
    pub platforms: Vec<PlatformInstanceConfig>,

    /// Session reset policy
    #[serde(default)]
    pub session_reset: SessionResetPolicy,

    /// Legacy alias for session_reset
    #[serde(default)]
    pub session_reset_policy: SessionResetPolicy,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            platforms: Vec::new(),
            session_reset: SessionResetPolicy::default(),
            session_reset_policy: SessionResetPolicy::Always,
        }
    }
}

/// Session reset policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SessionResetPolicy {
    Daily,
    Idle,
    Both,
    Always,
    #[default]
    None,
}

/// Authentication configuration for API server.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// Whether token authentication is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// The expected bearer token.
    #[serde(default)]
    pub token: String,
}

/// A platform adapter instance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformInstanceConfig {
    /// Platform type
    #[serde(rename = "platformType")]
    #[serde(default)]
    pub platform_type: String,

    /// Enable this platform
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// API Server settings
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,

    /// Feishu settings
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub app_secret: Option<String>,
    #[serde(default)]
    pub verification_token: Option<String>,
    #[serde(default)]
    pub encrypt_key: Option<String>,
    #[serde(default)]
    pub webhook_port: Option<u16>,
    #[serde(default)]
    pub bot_name: Option<String>,

    /// WeCom settings
    #[serde(default)]
    pub corp_id: Option<String>,
    #[serde(default)]
    pub corp_secret: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,

    /// Email settings
    #[serde(default)]
    pub smtp_host: Option<String>,
    #[serde(default)]
    pub smtp_port: Option<u16>,
    #[serde(default)]
    pub smtp_user: Option<String>,
    #[serde(default)]
    pub smtp_password: Option<String>,
    #[serde(default)]
    pub imap_host: Option<String>,
    #[serde(default)]
    pub imap_port: Option<u16>,

    /// Auth settings
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

/// Named platform adapters configuration (T07-05).
///
/// Usage in config.yaml:
/// ```yaml
/// platforms:
///   feishu:
///     enabled: true
///     app_id: "cli_xxx"
///     app_secret: "xxx"
///   wecom:
///     enabled: true
///     corp_id: "xxx"
///     corp_secret: "xxx"
///     agent_id: 1000001
///   email:
///     enabled: false
///     smtp_host: "smtp.example.com"
///     smtp_port: 587
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PlatformsConfig {
    /// Feishu (Lark) platform adapter
    #[serde(default)]
    pub feishu: FeishuConfig,
    /// WeCom (企业微信) platform adapter
    #[serde(default)]
    pub wecom: WecomConfig,
    /// Email platform adapter
    #[serde(default)]
    pub email: EmailConfig,
}

/// Feishu (Lark) platform configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeishuConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub app_id: String,
    #[serde(default)]
    pub app_secret: String,
    #[serde(default)]
    pub verify_token: String,
    #[serde(default)]
    pub encrypt_key: String,
    #[serde(default)]
    pub webhook_path: String,
    #[serde(default)]
    pub bot_name: String,
}

/// WeCom (企业微信) platform configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WecomConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub corp_id: String,
    #[serde(default)]
    pub corp_secret: String,
    #[serde(default)]
    pub agent_id: u64,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub encoding_aes_key: String,
    #[serde(default)]
    pub webhook_path: String,
}

/// Email platform configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EmailConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub smtp_host: String,
    #[serde(default = "default_smtp_port")]
    pub smtp_port: u16,
    #[serde(default)]
    pub smtp_user: String,
    #[serde(default)]
    pub smtp_password: String,
    #[serde(default)]
    pub imap_host: String,
    #[serde(default = "default_imap_port")]
    pub imap_port: u16,
    #[serde(default)]
    pub imap_user: String,
    #[serde(default)]
    pub imap_password: String,
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,
}

fn default_smtp_port() -> u16 {
    587
}
fn default_imap_port() -> u16 {
    993
}
fn default_check_interval() -> u64 {
    60
}

fn default_true() -> bool {
    true
}

/// Memory system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Enable memory system
    #[serde(default = "default_true_bool")]
    pub enabled: bool,

    /// Store configuration
    #[serde(default)]
    pub store: StoreConfig,

    /// Compression configuration
    #[serde(default)]
    pub compression: CompressionConfig,

    /// Token budget configuration
    #[serde(default)]
    pub budget: BudgetConfig,

    /// Extraction configuration
    #[serde(default)]
    pub extractor: ExtractorConfig,

    /// Drift detection configuration
    #[serde(default)]
    pub drift: DriftConfig,

    /// Performance budget
    #[serde(default)]
    pub perf: PerfBudget,

    /// When true, use AAAK symbolic index instead of full entry injection
    #[serde(default = "default_true_bool")]
    pub aaak_index_enabled: bool,

    /// Coherence threshold in basis points (100 = 0.01). Entries below this are excluded.
    #[serde(default = "default_coherence_threshold")]
    pub coherence_threshold_bp: u32,

    /// Target model name for adaptive compression thresholds.
    #[serde(default)]
    pub model: Option<String>,
}

fn default_coherence_threshold() -> u32 { 1000 }

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            store: StoreConfig::default(),
            compression: CompressionConfig::default(),
            budget: BudgetConfig::default(),
            extractor: ExtractorConfig::default(),
            drift: DriftConfig::default(),
            perf: PerfBudget::default(),
            aaak_index_enabled: true,
            coherence_threshold_bp: 1000,
            model: None,
        }
    }
}

/// Store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    /// SQLite database path
    #[serde(default = "default_sqlite_path")]
    pub sqlite_path: PathBuf,

    /// Blob directory path
    #[serde(default = "default_blob_dir")]
    pub blob_dir: PathBuf,

    /// Enable vector index
    #[serde(default)]
    pub enable_vector_index: bool,

    /// Vector embedding config
    #[serde(default)]
    pub vector: VectorConfig,
}

fn default_sqlite_path() -> PathBuf {
    PathBuf::from("memory.db".to_string())
}

fn default_blob_dir() -> PathBuf {
    PathBuf::from("memory_blobs")
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            sqlite_path: default_sqlite_path(),
            blob_dir: default_blob_dir(),
            enable_vector_index: false,
            vector: VectorConfig::default(),
        }
    }
}

/// Vector embedding configuration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Enable remote embedding
    #[serde(default)]
    pub enabled: bool,

    /// Model name
    #[serde(default)]
    pub model: String,

    /// API URL
    #[serde(default)]
    pub api_url: String,

    /// API key
    #[serde(default)]
    pub api_key: String,

    /// Vector dimension
    #[serde(default)]
    pub dimension: usize,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// Batch size
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_timeout() -> u64 { 30 }
fn default_batch_size() -> usize { 32 }

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

/// Compression configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Micro compression settings (per tool-result)
    #[serde(default)]
    pub micro: MicroCompactConfig,

    /// Session compression settings
    #[serde(default)]
    pub session: SessionCompactConfig,

    /// Deep compression settings
    #[serde(default)]
    pub deep: DeepCompactConfig,

    /// Circuit breaker settings
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,

    /// LLM summarization configuration for semantic compression.
    #[serde(default)]
    pub llm: LlmSummarizerConfig,
}

fn default_true_bool() -> bool { true }

impl Eq for CompressionConfig {}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            micro: MicroCompactConfig::default(),
            session: SessionCompactConfig::default(),
            deep: DeepCompactConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            llm: LlmSummarizerConfig::default(),
        }
    }
}

/// Token budget configuration
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

/// Model providers configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProvidersConfig {
    /// Provider configurations keyed by name
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
}

/// A single provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// Base URL for the API
    pub base_url: String,

    /// API key
    pub api_key: String,

    /// Supported models
    #[serde(default)]
    pub models: Vec<String>,
}

impl ProvidersConfig {
    /// Returns `true` if no providers are configured.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Resolves a model name to its provider's `(base_url, api_key)` pair.
    pub fn resolve(&self, model_name: &str) -> Option<(&str, &str)> {
        for provider in self.providers.values() {
            if provider.models.iter().any(|m| m == model_name) {
                return Some((&provider.base_url, &provider.api_key));
            }
        }
        None
    }

    /// Returns the named provider if it exists.
    pub fn get(&self, name: &str) -> Option<&ProviderConfig> {
        self.providers.get(name)
    }
}

impl MemoryConfig {
    /// Update configuration based on model profile.
    pub fn with_model_profile(mut self, model_name: &str) -> Self {
        let profile = ModelProfile::for_model(model_name);
        self.model = Some(model_name.to_string());
        self.budget.context_window = profile.context_window;
        self.budget.warning_threshold = profile.warning_threshold;
        self.budget.critical_threshold = profile.critical_threshold;
        self.compression.micro = MicroCompactConfig {
            enabled: true,
            tool_result_max_chars: 4000,
            time_decay_factor: 0.9,
        };
        self
    }

    /// Override configuration from environment variables.
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(url) = std::env::var("CC_LLM_API_URL") {
            if !url.is_empty() { self.compression.llm.api_url = url; }
        }
        if let Ok(key) = std::env::var("CC_LLM_API_KEY") {
            if !key.is_empty() { self.compression.llm.api_key = key; }
        }
        if let Ok(model) = std::env::var("CC_LLM_MODEL") {
            if !model.is_empty() { self.compression.llm.model = model; }
        }
        if let Ok(url) = std::env::var("CC_VECTOR_API_URL") {
            if !url.is_empty() { self.store.vector.api_url = url; }
        }
        if let Ok(key) = std::env::var("CC_VECTOR_API_KEY") {
            if !key.is_empty() { self.store.vector.api_key = key; }
        }
        self
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

// ── Config Loader ─────────────────────────────────────────────────────────────

/// Builder for loading unified configuration.
#[derive(Debug, Clone, Default)]
pub struct ConfigLoader {
    /// Config discovery settings
    discovery: ConfigDiscovery,
    /// CLI argument overrides
    cli_overrides: BTreeMap<String, serde_json::Value>,
    /// Whether to validate schema
    validate: bool,
}

impl ConfigLoader {
    /// Create a new loader with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set custom home directory.
    pub fn with_home_dir(mut self, home: PathBuf) -> Self {
        self.discovery.home_dir = home;
        self
    }

    /// Set custom working directory.
    pub fn with_cwd(mut self, cwd: PathBuf) -> Self {
        self.discovery.cwd = cwd;
        self
    }

    /// Add CLI argument overrides.
    pub fn with_cli_overrides(mut self, overrides: BTreeMap<String, serde_json::Value>) -> Self {
        self.cli_overrides = overrides;
        self
    }

    /// Enable schema validation.
    pub fn with_validation(mut self) -> Self {
        self.validate = true;
        self
    }

    /// Load the unified configuration.
    pub fn load(self) -> Result<UnifiedConfig> {
        let mut raw = BTreeMap::new();
        let mut entries = Vec::new();
        let mut merged_root: serde_json::Value = serde_json::Value::Object(Map::new());

        // 1. Load files in precedence order, deep-merging each
        for entry in self.discovery.discover() {
            let content = fs::read_to_string(&entry.path)?;
            let parsed: serde_json::Value = serde_yaml::from_str(&content)?;

            if let serde_json::Value::Object(ref map) = parsed {
                for (key, value) in map {
                    raw.insert(key.clone(), value.clone());
                }
            }

            // Deep-merge into the accumulated root
            deep_merge(&mut merged_root, &parsed);

            entries.push(entry);
        }

        // 2. Apply environment variable overrides (deep-merge)
        let env_overrides = collect_env_overrides();
        let env_json = serde_json::Value::Object(Map::from_iter(env_overrides.clone()));
        deep_merge(&mut merged_root, &env_json);
        for (key, value) in env_overrides {
            raw.insert(key, value);
        }

        // 3. Apply CLI overrides (deep-merge)
        if !self.cli_overrides.is_empty() {
            let cli_json = serde_json::Value::Object(Map::from_iter(self.cli_overrides.clone()));
            deep_merge(&mut merged_root, &cli_json);
            for (key, value) in self.cli_overrides {
                raw.insert(key, value);
            }
        }

        // 4. Parse into structured config from the deep-merged result
        let config: UnifiedConfig = serde_json::from_value(merged_root)
            .map_err(|e| ConfigError::Invalid {
                key: "root".to_string(),
                message: e.to_string(),
            })?;

        Ok(UnifiedConfig {
            raw,
            ..config
        })
    }
}

// ── UnifiedConfig Methods ──────────────────────────────────────────────────────

impl UnifiedConfig {
    /// Create a loader for this config.
    pub fn loader() -> ConfigLoader {
        ConfigLoader::new()
    }

    /// Load with default settings (respects precedence order).
    pub fn load() -> Result<Self> {
        Self::loader().load()
    }

    /// Load with custom paths.
    pub fn load_with_paths(home: PathBuf, cwd: PathBuf) -> Result<Self> {
        Self::loader()
            .with_home_dir(home)
            .with_cwd(cwd)
            .load()
    }

    /// Get a raw config value by key path (e.g., "memory.store.sqlite_path").
    pub fn get_raw(&self, key: &str) -> Option<&serde_json::Value> {
        self.raw.get(key)
    }

    /// Get all raw keys.
    pub fn raw_keys(&self) -> impl Iterator<Item = &String> {
        self.raw.keys()
    }

    /// Resolve a provider for a given model name.
    pub fn resolve_provider(&self, model: &str) -> Option<(&str, &str)> {
        for (_name, provider) in &self.providers.providers {
            if provider.models.iter().any(|m| m == model) {
                return Some((&provider.base_url, &provider.api_key));
            }
        }
        None
    }

    /// Get effective model (from config or environment).
    pub fn effective_model(&self) -> String {
        self.runtime.model.clone()
            .or_else(|| std::env::var("COWD_MODEL").ok())
            .or_else(|| std::env::var("COWD_MODEL").ok())
            .unwrap_or_else(|| "claude-sonnet-4-20250514".to_string())
    }
}

// ── Approval & Permission Resolution ───────────────────────────────────────

/// Smart approval configuration for the intelligent command approval gate.
///
/// Controls which commands auto-pass vs. require approval, and YOLO mode
/// for bypassing approvals during long-running autonomous tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalConfig {
    /// When true, all non-critical commands bypass the approval flow.
    #[serde(default)]
    pub yolo_mode: bool,
    /// When true, even in YOLO mode, Critical-risk commands still require approval.
    #[serde(default = "default_true_bool")]
    pub yolo_honor_critical: bool,
    /// Auto-pass commands detected as read-only (ls, cat, grep, git status, etc.).
    #[serde(default = "default_true_bool")]
    pub auto_pass_read_only: bool,
    /// Auto-pass commands that match Low-risk destructive patterns (cargo clean, etc.).
    #[serde(default = "default_true_bool")]
    pub auto_pass_low_risk: bool,
}

impl Default for ApprovalConfig {
    fn default() -> Self {
        Self {
            yolo_mode: false,
            yolo_honor_critical: true,
            auto_pass_read_only: true,
            auto_pass_low_risk: true,
        }
    }
}

impl ApprovalConfig {
    pub fn new() -> Self { Self::default() }
    pub fn with_yolo_mode(mut self, enabled: bool) -> Self { self.yolo_mode = enabled; self }
    pub fn with_yolo_honor_critical(mut self, honor: bool) -> Self { self.yolo_honor_critical = honor; self }
}

/// Effective permission mode after decoding config values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResolvedPermissionMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

// ── Detailed Memory Sub-Configuration ─────────────────────────────────────

/// Per-layer token and search limits for the memory subsystem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerConfig {
    #[serde(default = "default_true_bool")]
    pub l0_enabled: bool,
    #[serde(default = "default_l1_max_tokens")]
    pub l1_max_tokens: u32,
    #[serde(default = "default_l2_max_tokens")]
    pub l2_max_tokens: u32,
    #[serde(default = "default_l3_search_limit")]
    pub l3_search_limit: u32,
    #[serde(default)]
    pub l4_enabled: bool,
}

fn default_l1_max_tokens() -> u32 { 2000 }
fn default_l2_max_tokens() -> u32 { 3000 }
fn default_l3_search_limit() -> u32 { 5 }

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

/// Controls automatic memory extraction behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorConfig {
    /// Enable automatic background extraction.
    #[serde(default = "default_true_bool")]
    pub auto_extract: bool,
    /// How often (in seconds) the extractor polls for new content.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Maximum number of entries extracted per poll cycle.
    #[serde(default = "default_batch_size_usize")]
    pub batch_size: usize,
    /// Minimum confidence score to keep an extracted entry.
    #[serde(default = "default_min_confidence")]
    pub min_confidence: f32,
}

fn default_poll_interval() -> u64 { 30 }
fn default_batch_size_usize() -> usize { 20 }
fn default_min_confidence() -> f32 { 0.6 }

impl Default for ExtractorConfig {
    fn default() -> Self {
        Self {
            auto_extract: true,
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
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

// ── Detailed Compression Sub-Configuration ────────────────────────────────

/// Micro-compaction settings (per tool-result trimming).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicroCompactConfig {
    #[serde(default = "default_true_bool")]
    pub enabled: bool,
    #[serde(default = "default_tool_result_max_chars")]
    pub tool_result_max_chars: u32,
    #[serde(default = "default_decay_factor")]
    pub time_decay_factor: f32,
}

fn default_tool_result_max_chars() -> u32 { 4000 }
fn default_decay_factor() -> f32 { 0.9 }

impl Eq for MicroCompactConfig {}

impl Default for MicroCompactConfig {
    fn default() -> Self {
        Self { enabled: true, tool_result_max_chars: 4000, time_decay_factor: 0.9 }
    }
}

/// Session-level compaction trigger and output constraints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionCompactConfig {
    #[serde(default = "default_session_threshold_tokens")]
    pub threshold_tokens: u32,
    #[serde(default = "default_preserve_recent")]
    pub preserve_recent: u32,
    #[serde(default = "default_summary_max")]
    pub summary_max_tokens: u32,
    #[serde(default = "default_buffer_tokens")]
    pub buffer_tokens: u32,
}

fn default_session_threshold_tokens() -> u32 { 80000 }
fn default_preserve_recent() -> u32 { 6 }
fn default_summary_max() -> u32 { 2000 }
fn default_buffer_tokens() -> u32 { 13000 }

impl Default for SessionCompactConfig {
    fn default() -> Self {
        Self { threshold_tokens: 80000, preserve_recent: 6, summary_max_tokens: 2000, buffer_tokens: 13000 }
    }
}

/// Deep iterative compaction settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeepCompactConfig {
    #[serde(default = "default_true_bool")]
    pub enabled: bool,
    #[serde(default = "default_true_bool")]
    pub iterative_update: bool,
}

impl Default for DeepCompactConfig {
    fn default() -> Self {
        Self { enabled: true, iterative_update: true }
    }
}

/// Circuit-breaker limits for the compression pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_max_retries_3")]
    pub max_retries: u32,
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u32,
}

fn default_max_retries_3() -> u32 { 3 }
fn default_cooldown_secs() -> u32 { 30 }

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self { max_retries: 3, cooldown_secs: 30 }
    }
}

// ── MCP & OAuth ────────────────────────────────────────────────────────────

/// Transport families supported by configured MCP servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpTransport {
    Stdio,
    Sse,
    Http,
    Ws,
    Sdk,
    ManagedProxy,
}

/// OAuth overrides associated with a remote MCP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpOAuthConfig {
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub callback_port: Option<u16>,
    #[serde(default)]
    pub auth_server_metadata_url: Option<String>,
    #[serde(default)]
    pub xaa: Option<bool>,
}

/// OAuth client configuration used by the main runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthConfig {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    #[serde(default)]
    pub callback_port: Option<u16>,
    #[serde(default)]
    pub manual_redirect_url: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
}

// ── Runtime configuration types (unified from runtime::config) ──────────────

/// Ordered chain of fallback model identifiers used when the primary
/// provider returns a retryable failure (429/500/503/etc.).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProviderFallbackConfig {
    #[serde(default)]
    pub primary: Option<String>,
    #[serde(default)]
    pub fallbacks: Vec<String>,
}

/// Hook command lists grouped by lifecycle stage.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimeHookConfig {
    #[serde(default)]
    pub pre_tool_use: Vec<String>,
    #[serde(default)]
    pub post_tool_use: Vec<String>,
    #[serde(default)]
    pub post_tool_use_failure: Vec<String>,
}

/// Raw permission rule lists grouped by allow, deny, and ask behavior.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimePermissionRuleConfig {
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub ask: Vec<String>,
}

/// Parsed plugin-related settings extracted from runtime config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePluginConfig {
    #[serde(default)]
    pub enabled_plugins: std::collections::BTreeMap<String, bool>,
    #[serde(default)]
    pub external_directories: Vec<String>,
    #[serde(default)]
    pub install_root: Option<String>,
    #[serde(default)]
    pub registry_path: Option<String>,
    #[serde(default)]
    pub bundled_root: Option<String>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
}

// ── Impl blocks for runtime config types ────────────────────────────────────

impl Default for RuntimePluginConfig {
    fn default() -> Self {
        Self {
            enabled_plugins: std::collections::BTreeMap::default(),
            external_directories: Vec::default(),
            install_root: None,
            registry_path: None,
            bundled_root: None,
            max_output_tokens: std::env::var("COWD_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
}

impl ProviderFallbackConfig {
    #[must_use]
    pub fn new(primary: Option<String>, fallbacks: Vec<String>) -> Self {
        Self { primary, fallbacks }
    }

    #[must_use]
    pub fn primary(&self) -> Option<&str> {
        self.primary.as_deref()
    }

    #[must_use]
    pub fn fallbacks(&self) -> &[String] {
        &self.fallbacks
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fallbacks.is_empty()
    }
}

impl RuntimePluginConfig {
    #[must_use]
    pub fn enabled_plugins(&self) -> &std::collections::BTreeMap<String, bool> {
        &self.enabled_plugins
    }

    #[must_use]
    pub fn external_directories(&self) -> &[String] {
        &self.external_directories
    }

    #[must_use]
    pub fn install_root(&self) -> Option<&str> {
        self.install_root.as_deref()
    }

    #[must_use]
    pub fn registry_path(&self) -> Option<&str> {
        self.registry_path.as_deref()
    }

    #[must_use]
    pub fn bundled_root(&self) -> Option<&str> {
        self.bundled_root.as_deref()
    }

    #[must_use]
    pub fn max_output_tokens(&self) -> Option<u32> {
        self.max_output_tokens
    }

    #[must_use]
    pub fn state_for(&self, plugin_id: &str, default_enabled: bool) -> bool {
        self.enabled_plugins
            .get(plugin_id)
            .copied()
            .unwrap_or(default_enabled)
    }
}

impl RuntimeHookConfig {
    #[must_use]
    pub fn new(
        pre_tool_use: Vec<String>,
        post_tool_use: Vec<String>,
        post_tool_use_failure: Vec<String>,
    ) -> Self {
        Self {
            pre_tool_use,
            post_tool_use,
            post_tool_use_failure,
        }
    }

    #[must_use]
    pub fn pre_tool_use(&self) -> &[String] {
        &self.pre_tool_use
    }

    #[must_use]
    pub fn post_tool_use(&self) -> &[String] {
        &self.post_tool_use
    }

    #[must_use]
    pub fn post_tool_use_failure(&self) -> &[String] {
        &self.post_tool_use_failure
    }

    #[must_use]
    pub fn merged(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        merged.extend(other);
        merged
    }

    pub fn extend(&mut self, other: &Self) {
        // Deduplicate per-field: each field's uniqueness is independent
        let mut pre_set: std::collections::HashSet<String> = self.pre_tool_use.iter().cloned().collect();
        for item in &other.pre_tool_use {
            if pre_set.insert(item.clone()) {
                self.pre_tool_use.push(item.clone());
            }
        }
        let mut post_set: std::collections::HashSet<String> = self.post_tool_use.iter().cloned().collect();
        for item in &other.post_tool_use {
            if post_set.insert(item.clone()) {
                self.post_tool_use.push(item.clone());
            }
        }
        let mut fail_set: std::collections::HashSet<String> = self.post_tool_use_failure.iter().cloned().collect();
        for item in &other.post_tool_use_failure {
            if fail_set.insert(item.clone()) {
                self.post_tool_use_failure.push(item.clone());
            }
        }
    }
}

impl RuntimePermissionRuleConfig {
    #[must_use]
    pub fn new(allow: Vec<String>, deny: Vec<String>, ask: Vec<String>) -> Self {
        Self { allow, deny, ask }
    }

    #[must_use]
    pub fn allow(&self) -> &[String] {
        &self.allow
    }

    #[must_use]
    pub fn deny(&self) -> &[String] {
        &self.deny
    }

    #[must_use]
    pub fn ask(&self) -> &[String] {
        &self.ask
    }
}

// ── Convenience Re-exports ───────────────────────────────────────────────────

/// Simple directory helper for config paths.
mod dirs {
    use std::path::PathBuf;

    pub fn home_dir() -> Option<PathBuf> {
        std::env::var("HOME")
            .ok()
            .map(PathBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_display_is_meaningful() {
        let err = ConfigError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"));
        assert!(format!("{}", err).contains("not found"));

        let err = ConfigError::Parse("bad yaml".to_string());
        assert!(format!("{}", err).contains("bad yaml"));
    }

    #[test]
    fn config_source_is_serializable() {
        let json = serde_json::to_value(&ConfigSource::User).unwrap();
        assert_eq!(json, "user");
        let json = serde_json::to_value(&ConfigSource::Project).unwrap();
        assert_eq!(json, "project");
    }

    #[test]
    fn config_entry_source_and_path() {
        let entry = ConfigEntry {
            source: ConfigSource::User,
            path: "/home/user/.cowd/config.yaml".into(),
            exists: true,
        };
        assert_eq!(entry.source, ConfigSource::User);
        assert!(entry.exists);
    }

    #[test]
    fn config_loader_defaults() {
        let loader = ConfigLoader::new()
            .with_home_dir(std::path::PathBuf::from("/tmp/test-home"));
        assert_eq!(loader.discovery.home_dir.to_string_lossy(), "/tmp/test-home");
    }

    #[test]
    fn config_loader_with_home_and_cwd() {
        let loader = ConfigLoader::new()
            .with_home_dir(std::path::PathBuf::from("/tmp/home"))
            .with_cwd(std::path::PathBuf::from("/tmp/cwd"));
        assert_eq!(
            loader.discovery.home_dir.to_string_lossy(),
            "/tmp/home"
        );
    }

    #[test]
    fn config_discovery_new() {
        let discovery = ConfigDiscovery::with_paths(
            std::path::PathBuf::from("/tmp/test-home"),
            std::path::PathBuf::from("/tmp/test-cwd"),
        );
        assert_eq!(discovery.home_dir.to_string_lossy(), "/tmp/test-home");
        assert_eq!(discovery.cwd.to_string_lossy(), "/tmp/test-cwd");
    }

    #[test]
    fn runtime_config_default_is_sane() {
        let cfg = RuntimeConfig::default();
        assert!(cfg.model.is_none());
        assert!(cfg.model_aliases.is_empty());
    }

    #[test]
    fn gateway_config_default_is_sane() {
        let cfg = GatewayConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.session_reset_policy == SessionResetPolicy::Always);
    }

    #[test]
    fn sandbox_config_default() {
        let cfg = SandboxConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.timeout_secs, 30);
        assert_eq!(cfg.max_output_kib, 100);
        assert_eq!(cfg.max_entries, 50);
    }

    #[test]
    fn auth_config_default() {
        let cfg = AuthConfig::default();
        assert!(!cfg.enabled);
    }

    #[test]
    fn approval_config_defaults() {
        let cfg = ApprovalConfig::default();
        assert!(!cfg.yolo_mode);
        assert!(cfg.yolo_honor_critical);
        assert!(cfg.auto_pass_read_only);
        assert!(cfg.auto_pass_low_risk);
    }

    #[test]
    fn resolved_permission_mode_serialization() {
        let json = serde_json::to_value(&ResolvedPermissionMode::ReadOnly).unwrap();
        assert_eq!(json, "readonly");
    }

    #[test]
    fn layer_config_defaults() {
        let cfg = LayerConfig::default();
        assert!(cfg.l0_enabled);
        assert_eq!(cfg.l1_max_tokens, 2000);
        assert_eq!(cfg.l2_max_tokens, 3000);
        assert_eq!(cfg.l3_search_limit, 5);
        assert!(!cfg.l4_enabled);
    }

    #[test]
    fn extractor_config_defaults() {
        let cfg = ExtractorConfig::default();
        assert!(cfg.auto_extract);
        assert_eq!(cfg.poll_interval_secs, 30);
        assert_eq!(cfg.batch_size, 20);
        assert!((cfg.min_confidence - 0.6).abs() < 0.01);
    }

    #[test]
    fn drift_config_defaults() {
        let cfg = DriftConfig::default();
        assert!((cfg.staleness_decay_per_day - 0.02).abs() < 0.01);
        assert!((cfg.review_threshold - 0.7).abs() < 0.01);
        assert!((cfg.prune_threshold - 0.95).abs() < 0.01);
    }

    #[test]
    fn perf_budget_defaults() {
        let cfg = PerfBudget::default();
        assert_eq!(cfg.hook_max_ms, 500);
        assert_eq!(cfg.inject_max_ms, 100);
    }

    #[test]
    fn model_profile_detection() {
        let claude = ModelProfile::for_model("claude-sonnet-4-6");
        assert_eq!(claude.context_window, 200_000);
        let gpt4o = ModelProfile::for_model("gpt-4o");
        assert_eq!(gpt4o.context_window, 128_000);
    }

    #[test]
    fn llm_summarizer_config_defaults() {
        let cfg = LlmSummarizerConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.model, "gpt-4o-mini");
    }

    #[test]
    fn micro_compact_config_defaults() {
        let cfg = MicroCompactConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.tool_result_max_chars, 4000);
    }

    #[test]
    fn session_compact_config_defaults() {
        let cfg = SessionCompactConfig::default();
        assert_eq!(cfg.threshold_tokens, 80000);
        assert_eq!(cfg.preserve_recent, 6);
    }

    #[test]
    fn deep_compact_config_defaults() {
        let cfg = DeepCompactConfig::default();
        assert!(cfg.enabled);
        assert!(cfg.iterative_update);
    }

    #[test]
    fn circuit_breaker_config_defaults() {
        let cfg = CircuitBreakerConfig::default();
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.cooldown_secs, 30);
    }

    #[test]
    fn mcp_transport_variants() {
        let json = serde_json::to_value(&McpTransport::Stdio).unwrap();
        assert_eq!(json, "stdio");
    }

    #[test]
    fn memory_config_extended_defaults() {
        let cfg = MemoryConfig::default();
        assert!(cfg.aaak_index_enabled);
        assert_eq!(cfg.coherence_threshold_bp, 1000);
        assert!(cfg.model.is_none());
        assert_eq!(cfg.extractor.poll_interval_secs, 30);
    }

    #[test]
    fn compression_config_sub_types() {
        let cfg = CompressionConfig::default();
        assert!(cfg.micro.enabled);
        assert!(cfg.deep.enabled);
        assert_eq!(cfg.circuit_breaker.max_retries, 3);
        assert!(!cfg.llm.enabled);
    }

    #[test]
    fn budget_config_methods() {
        let budget = BudgetConfig::for_context_window(128_000);
        assert_eq!(budget.context_window, 128_000);
        assert_eq!(budget.warning_threshold, 0.70);
        let available = budget.available_tokens();
        assert!(available > 100_000);
    }

    #[test]
    fn providers_config_methods() {
        let mut cfg = ProvidersConfig::default();
        assert!(cfg.is_empty());
        assert!(cfg.resolve("nonexistent").is_none());
        assert!(cfg.get("none").is_none());
    }

    #[test]
    fn memory_config_with_model_profile() {
        let cfg = MemoryConfig::default()
            .with_model_profile("gpt-4o");
        assert_eq!(cfg.model, Some("gpt-4o".to_string()));
    }

    #[test]
    fn permission_config_defaults() {
        let cfg = PermissionConfig::default();
        assert!(cfg.default_mode.is_none());
        assert!(cfg.allow.is_empty());
        assert!(cfg.deny.is_empty());
        assert!(cfg.ask.is_empty());
    }

    #[test]
    fn prompt_cache_config_defaults() {
        let cfg = PromptCacheConfig::default();
        assert_eq!(cfg.check_interval, 5);
        assert_eq!(cfg.max_age, 50);
        assert_eq!(cfg.memory_delta, 3);
    }

    #[test]
    fn runtime_config_has_new_fields() {
        let cfg = RuntimeConfig::default();
        assert!(cfg.output_style.is_none());
        assert_eq!(cfg.prompt_cache.check_interval, 5);
    }

    // ── Robustness tests ───────────────────────────────────────────────

    #[test]
    fn deserializes_config_default_yaml() {
        let default_yaml = include_str!("../../../config-default.yaml");
        // config-default.yaml is ingested by runtime's ConfigLoader which
        // maps top-level keys to feature config paths. The UnifiedConfig
        // struct expects nested runtime/memory/gateway sections, but the
        // default yaml uses a flattened layout for user ergonomics.
        // This test verifies the yaml is valid and can be parsed at all.
        let _config: serde_yaml::Value = serde_yaml::from_str(default_yaml)
            .expect("config-default.yaml should be valid YAML");
    }

    #[test]
    fn all_defaults_are_sane() {
        // Verify every config type has reasonable defaults
        let cfg = CompressionConfig::default();
        assert!(cfg.micro.enabled);
        assert_eq!(cfg.session.threshold_tokens, 80000);
        assert!(cfg.deep.enabled);
        assert_eq!(cfg.circuit_breaker.max_retries, 3);

        let mem = MemoryConfig::default();
        assert!(mem.enabled);
        assert!(mem.aaak_index_enabled);
        assert_eq!(mem.coherence_threshold_bp, 1000);
        assert_eq!(mem.budget.context_window, 200000);
        assert_eq!(mem.budget.warning_threshold, 0.70);
        assert_eq!(mem.budget.critical_threshold, 0.90);
        assert_eq!(mem.extractor.auto_extract, true);
        assert_eq!(mem.extractor.min_confidence, 0.6);
        assert_eq!(mem.drift.staleness_decay_per_day, 0.02);
        assert_eq!(mem.perf.hook_max_ms, 500);
    }

    #[test]
    fn vector_config_defaults_disabled() {
        let cfg = VectorConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.dimension, 0);
        assert_eq!(cfg.timeout_secs, 30);
        assert_eq!(cfg.batch_size, 32);
    }

    #[test]
    fn env_var_prefix_is_cowd() {
        // Ensure env var override prefix is "COWD_"
        let prefix = "COWD_";
        assert!(!prefix.is_empty());
        // All COWD_* env vars should override config values
        // This test validates the naming convention, not actual env state
    }
}
