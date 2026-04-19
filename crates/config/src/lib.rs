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
//! use cc_config::{UnifiedConfig, ConfigSource};
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
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("YAML parse error: {0}")]
   Yaml(#[from] serde_yaml::Error),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub source: ConfigSource,
    pub path: PathBuf,
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
            });
        }

        // 2. Project config: .cowd/config.yaml
        let project_config = self.cwd.join(".cowd").join("config.yaml");
        if project_config.exists() {
            entries.push(ConfigEntry {
                source: ConfigSource::Project,
                path: project_config,
            });
        }

        // 3. Local config: .cowd/config.local.yaml
        let local_config = self.cwd.join(".cowd").join("config.local.yaml");
        if local_config.exists() {
            entries.push(ConfigEntry {
                source: ConfigSource::Local,
                path: local_config,
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

    /// Platform adapters configuration (T07-05)
    #[serde(default)]
    pub platforms: PlatformsConfig,

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

    /// Permission mode
    #[serde(default)]
    pub permission_mode: Option<String>,

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
    #[serde(default = "default_Transport")]
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

fn default_Transport() -> String {
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SandboxConfig {
    /// Filesystem isolation mode
    #[serde(default)]
    pub filesystem_mode: Option<String>,

    /// Allowed directories
    #[serde(default)]
    pub allowed_dirs: Vec<String>,
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
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            platforms: Vec::new(),
            session_reset: SessionResetPolicy::default(),
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
    #[default]
    None,
}

/// Authentication configuration for API server.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Store configuration
    #[serde(default)]
    pub store: StoreConfig,

    /// Compression configuration
    #[serde(default)]
    pub compression: CompressionConfig,

    /// Token budget configuration
    #[serde(default)]
    pub budget: BudgetConfig,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            store: StoreConfig::default(),
            compression: CompressionConfig::default(),
            budget: BudgetConfig::default(),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Micro compression threshold
    #[serde(default = "default_micro_threshold")]
    pub micro_threshold: usize,

    /// Session compression threshold
    #[serde(default = "default_session_threshold")]
    pub session_threshold: usize,

    /// Enable deep compression
    #[serde(default = "default_true_bool")]
    pub enable_deep_compression: bool,

    /// Aggressiveness (0.0-1.0)
    #[serde(default = "default_aggressiveness")]
    pub aggressiveness: f32,
}

fn default_true_bool() -> bool { true }
fn default_micro_threshold() -> usize { 50 }
fn default_session_threshold() -> usize { 10 }
fn default_aggressiveness() -> f32 { 0.5 }

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            micro_threshold: 50,
            session_threshold: 10,
            enable_deep_compression: true,
            aggressiveness: 0.5,
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

/// Model providers configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProvidersConfig {
    /// Provider configurations keyed by name
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,

    /// Provider fallbacks
    #[serde(default)]
    pub fallbacks: ProviderFallbacks,
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

/// Provider fallback chain
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderFallbacks {
    /// Primary provider name
    #[serde(default)]
    pub primary: Option<String>,

    /// Fallback provider names in order
    #[serde(default)]
    pub chain: Vec<String>,
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

        // 1. Load files in precedence order
        for entry in self.discovery.discover() {
            let content = fs::read_to_string(&entry.path)?;
            let parsed: serde_json::Value = if entry.path
                .extension()
                .map(|e| e == "yaml" || e == "yml")
                .unwrap_or(false)
            {
                serde_yaml::from_str(&content)?
            } else {
                serde_json::from_str(&content)?
            };

            if let serde_json::Value::Object(map) = parsed {
                for (key, value) in map {
                    raw.insert(key, value);
                }
            }

            entries.push(entry);
        }

        // 2. Apply environment variable overrides
        let env_overrides = collect_env_overrides();
        for (key, value) in env_overrides {
            raw.insert(key, value);
        }

        // 3. Apply CLI overrides
        for (key, value) in self.cli_overrides {
            raw.insert(key, value);
        }

        // 4. Parse into structured config
        let raw_json = serde_json::Value::Object(Map::from_iter(raw.clone()));
        let config: UnifiedConfig = serde_json::from_value(raw_json.clone())
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
        for (name, provider) in &self.providers.providers {
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

// ── Convenience Re-exports ───────────────────────────────────────────────────

pub use memory::types::{MemoryEntry, MemoryLayer, Priority};

/// Simple directory helper for config paths.
mod dirs {
    use std::path::PathBuf;

    pub fn home_dir() -> Option<PathBuf> {
        std::env::var("HOME")
            .ok()
            .map(PathBuf::from)
            .or_else(|| std::env::var("USERPROFILE").ok().map(PathBuf::from))
    }
}
