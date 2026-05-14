use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use crate::json::JsonValue;
use crate::sandbox::{FilesystemIsolationMode, SandboxConfig};

// ── Re-export from unified config crate ──────────────────────────────
pub use config::{ApprovalConfig, ResolvedPermissionMode, McpTransport, McpOAuthConfig, OAuthConfig};
pub use config::{ConfigSource, ConfigEntry, ConfigError};

/// Prefix used for environment variable config overrides.
const ENV_OVERRIDE_PREFIX: &str = "COWD_";

/// Schema name advertised by generated settings files.
pub const COWD_SETTINGS_SCHEMA_NAME: &str = "SettingsSchema";

/// Fully merged runtime configuration plus parsed feature-specific views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    merged: BTreeMap<String, JsonValue>,
    loaded_entries: Vec<ConfigEntry>,
    feature_config: RuntimeFeatureConfig,
}

/// Parsed plugin-related settings extracted from runtime config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePluginConfig {
    enabled_plugins: BTreeMap<String, bool>,
    external_directories: Vec<String>,
    install_root: Option<String>,
    registry_path: Option<String>,
    bundled_root: Option<String>,
    max_output_tokens: Option<u32>,
}

/// Structured feature configuration consumed by runtime subsystems.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeFeatureConfig {
    hooks: RuntimeHookConfig,
    plugins: RuntimePluginConfig,
    mcp: McpConfigCollection,
    oauth: Option<OAuthConfig>,
    model: Option<String>,
    aliases: BTreeMap<String, String>,
    permission_mode: Option<ResolvedPermissionMode>,
    permission_rules: RuntimePermissionRuleConfig,
    approval: ApprovalConfig,
    sandbox: SandboxConfig,
    provider_fallbacks: ProviderFallbackConfig,
    providers: ProvidersConfig,
    trusted_roots: Vec<String>,
    memory: MemoryConfig,
    compression: CompressionConfig,
    gateway: GatewayConfig,
}

/// Ordered chain of fallback model identifiers used when the primary
/// provider returns a retryable failure (429/500/503/etc.). The chain is
/// strict: each entry is tried in order until one succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderFallbackConfig {
    primary: Option<String>,
    fallbacks: Vec<String>,
}

/// Configuration for a single named provider (OpenAI-compatible endpoint).
///
/// Each provider has its own `base_url` and `api_key`, and declares the list
/// of model IDs it serves. When a model is requested, [`ProvidersConfig::resolve`]
/// searches this list to locate the matching provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    /// Base URL for the provider's OpenAI-compatible API (e.g. `https://api.stepfun.com/v1`).
    pub base_url: String,
    /// API key (Bearer token) for authenticating with this provider.
    pub api_key: String,
    /// List of model IDs served by this provider.
    pub models: Vec<String>,
}

/// Named collection of provider configurations.
///
/// Providers are keyed by a short name (e.g. `"stepfun"`, `"bailian"`).
/// Use [`ProvidersConfig::resolve`] to look up the `(base_url, api_key)` pair
/// for a given model name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProvidersConfig {
    pub providers: HashMap<String, ProviderConfig>,
}

impl ProvidersConfig {
    /// Returns `true` if no providers are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Resolves a model name to its provider's `(base_url, api_key)` pair.
    ///
    /// Iterates over all configured providers and returns the credentials for
    /// the first provider whose `models` list contains `model_name`.
    ///
    /// Returns `None` if no provider claims the model; callers should then
    /// fall back to the `OPENAI_BASE_URL` / `OPENAI_API_KEY` environment variables.
    #[must_use]
    pub fn resolve(&self, model_name: &str) -> Option<(&str, &str)> {
        for provider in self.providers.values() {
            if provider.models.iter().any(|m| m == model_name) {
                return Some((&provider.base_url, &provider.api_key));
            }
        }
        None
    }

    /// Returns the named provider if it exists.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ProviderConfig> {
        self.providers.get(name)
    }
}

/// Hook command lists grouped by lifecycle stage.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeHookConfig {
    pre_tool_use: Vec<String>,
    post_tool_use: Vec<String>,
    post_tool_use_failure: Vec<String>,
}

/// Raw permission rule lists grouped by allow, deny, and ask behavior.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimePermissionRuleConfig {
    allow: Vec<String>,
    deny: Vec<String>,
    ask: Vec<String>,
}


/// Collection of configured MCP servers after scope-aware merging.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpConfigCollection {
    servers: BTreeMap<String, ScopedMcpServerConfig>,
}

/// MCP server config paired with the scope that defined it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedMcpServerConfig {
    pub scope: ConfigSource,
    pub config: McpServerConfig,
}


/// Scope-normalized MCP server configuration variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerConfig {
    Stdio(McpStdioServerConfig),
    Sse(McpRemoteServerConfig),
    Http(McpRemoteServerConfig),
    Ws(McpWebSocketServerConfig),
    Sdk(McpSdkServerConfig),
    ManagedProxy(McpManagedProxyServerConfig),
}

/// Configuration for an MCP server launched as a local stdio process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpStdioServerConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub tool_call_timeout_ms: Option<u64>,
}

/// Configuration for an MCP server reached over HTTP or SSE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRemoteServerConfig {
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub headers_helper: Option<String>,
    pub oauth: Option<McpOAuthConfig>,
}

/// Configuration for an MCP server reached over WebSocket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpWebSocketServerConfig {
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub headers_helper: Option<String>,
}

/// Configuration for an MCP server addressed through an SDK name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSdkServerConfig {
    pub name: String,
}

/// Configuration for an MCP managed-proxy endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpManagedProxyServerConfig {
    pub url: String,
    pub id: String,
}



// ---- Memory configuration ----

/// Memory subsystem configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryConfig {
    pub enabled: bool,
    pub store_path: Option<PathBuf>,
    pub layers: LayerConfig,
    pub extraction: ExtractionConfig,
    pub vector: VectorConfig,
    /// When true, use AAAK symbolic index instead of full entry injection
    /// for memory context, saving 70-85% tokens.
    pub aaak_index_enabled: bool,
    /// Jaccard similarity threshold for coherence filtering in basis points.
    /// 100 = 0.01, 1000 = 0.10 (default), 5000 = 0.50.
    /// Entries with score below this are excluded from context injection.
    pub coherence_threshold_bp: u32,
}

/// Per-layer token and search limits for the memory subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerConfig {
    pub l0_enabled: bool,
    pub l1_max_tokens: u32,
    pub l2_max_tokens: u32,
    pub l3_search_limit: u32,
    pub l4_enabled: bool,
}

/// Controls automatic memory extraction behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionConfig {
    pub auto_extract: bool,
}

/// Optional vector-search backend configuration.
///
/// Supports OpenAI-compatible embedding API format (also works with
/// Ollama, vLLM, LocalAI, etc.).
///
/// # Environment variable overrides
/// - `CC_MEMORY_VECTOR_MODEL`   – embedding model name
/// - `CC_MEMORY_VECTOR_API_URL` – embedding API endpoint URL
/// - `CC_VECTOR_API_KEY`        – API key / Bearer token
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorConfig {
    pub enabled: bool,
    /// Embedding model name, e.g. `"text-embedding-3-small"`.
    /// Overridden by `CC_MEMORY_VECTOR_MODEL`.
    pub embedding_model: String,
    /// Expected vector dimension (`0` = auto-detect from first API call).
    pub dimension: u32,
    /// Embedding API endpoint URL, e.g. `"https://api.openai.com/v1/embeddings"`.
    /// Overridden by `CC_MEMORY_VECTOR_API_URL`.
    pub api_url: String,
    /// API key for the embedding service.
    /// Overridden by `CC_VECTOR_API_KEY`.
    pub api_key: String,
    /// Timeout for embedding API calls in seconds (default: 30).
    pub timeout_secs: u64,
    /// Maximum batch size for embedding requests (default: 32).
    pub batch_size: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            store_path: None,
            layers: LayerConfig::default(),
            extraction: ExtractionConfig::default(),
            vector: VectorConfig::default(),
            aaak_index_enabled: true,
            coherence_threshold_bp: 1000,  // 0.10
        }
    }
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

impl Default for ExtractionConfig {
    fn default() -> Self {
        Self { auto_extract: true }
    }
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            embedding_model: String::new(),
            dimension: 0,
            api_url: String::new(),
            api_key: String::new(),
            timeout_secs: 30,
            batch_size: 32,
        }
    }
}

// ---- Compression configuration ----

/// Context-compression pipeline configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionConfig {
    pub micro: MicroCompactConfig,
    pub session: SessionCompactConfig,
    pub deep: DeepCompactConfig,
    pub circuit_breaker: CircuitBreakerConfig,
}

/// Micro-compaction settings (per tool-result trimming).
#[derive(Debug, Clone, PartialEq)]
pub struct MicroCompactConfig {
    pub enabled: bool,
    pub tool_result_max_chars: u32,
    pub time_decay_factor: f32,
}

impl Eq for MicroCompactConfig {}

/// Session-level compaction trigger and output constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCompactConfig {
    pub threshold_tokens: u32,
    pub preserve_recent: u32,
    pub summary_max_tokens: u32,
    pub buffer_tokens: u32,
}

/// Deep iterative compaction settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeepCompactConfig {
    pub enabled: bool,
    pub iterative_update: bool,
}

/// Circuit-breaker limits for the compression pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircuitBreakerConfig {
    pub max_retries: u32,
    pub cooldown_secs: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            micro: MicroCompactConfig::default(),
            session: SessionCompactConfig::default(),
            deep: DeepCompactConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

impl Default for MicroCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tool_result_max_chars: 4000,
            time_decay_factor: 0.9,
        }
    }
}

impl Default for SessionCompactConfig {
    fn default() -> Self {
        Self {
            threshold_tokens: 80000,
            preserve_recent: 6,
            summary_max_tokens: 2000,
            buffer_tokens: 13000,
        }
    }
}

impl Default for DeepCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            iterative_update: true,
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            cooldown_secs: 30,
        }
    }
}

// ---- Gateway configuration ----

/// Multi-platform gateway configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    pub enabled: bool,
    pub platforms: Vec<PlatformConfig>,
    pub session_reset: SessionResetPolicy,
}

/// Configuration for a single inbound platform adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformConfig {
    /// Discriminator: `"api_server"`, `"email"`, `"feishu"`, `"wecom"`, etc.
    pub platform_type: String,
    pub enabled: bool,
    /// Platform-specific JSON blob (opaque to the runtime core).
    pub extra: BTreeMap<String, JsonValue>,
}

/// Policy that determines when a gateway session is reset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SessionResetPolicy {
    Daily,
    Idle,
    Both,
    #[default]
    None,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            platforms: Vec::new(),
            session_reset: SessionResetPolicy::default(),
        }
    }
}

/// Discovers config files and merges them into a [`RuntimeConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigLoader {
    cwd: PathBuf,
    config_home: PathBuf,
}

impl ConfigLoader {
    #[must_use]
    pub fn new(cwd: impl Into<PathBuf>, config_home: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            config_home: config_home.into(),
        }
    }

    #[must_use]
    pub fn default_for(cwd: impl Into<PathBuf>) -> Self {
        let cwd = cwd.into();
        let config_home = default_config_home();
        Self { cwd, config_home }
    }

    #[must_use]
    pub fn config_home(&self) -> &Path {
        &self.config_home
    }

    #[must_use]
    pub fn discover(&self) -> Vec<ConfigEntry> {
        // Derive the user config base directory (~/.cc or overridden via env).
        // The config_home field already holds this resolved directory.
        let cc_user_dir = &self.config_home;

        // Legacy .cowd.json path (sibling of config_home, e.g. ~/.cowd.json).
        let user_legacy_path = cc_user_dir.parent().map_or_else(
            || PathBuf::from(".cowd.json"),
            |parent| parent.join(".cowd.json"),
        );

        vec![
            // ── Legacy: .cowd.json (lowest priority, deprecated) ─────────────────────
            ConfigEntry {
                exists: true,
                source: ConfigSource::User,
                path: user_legacy_path,
            },
            // ── User-level: ~/.cc paths (preferred config.*) ──────────────────────────
            ConfigEntry {
                exists: true,
                source: ConfigSource::User,
                path: cc_user_dir.join("settings.json"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::User,
                path: cc_user_dir.join("config.yaml"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::User,
                path: cc_user_dir.join("config.yml"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::User,
                path: cc_user_dir.join("config.json"),
            },
            // ── Project-level: .cowd/.claw paths ───────────────────────────────────────
            ConfigEntry {
                exists: true,
                source: ConfigSource::Project,
                path: self.cwd.join(".cowd.json"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Project,
                path: self.cwd.join(".cowd").join("settings.json"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Project,
                path: self.cwd.join(".cowd").join("config.yaml"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Project,
                path: self.cwd.join(".cowd").join("config.yml"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Project,
                path: self.cwd.join(".cowd").join("config.json"),
            },
            // ── Local overrides: .cc paths (highest priority) ─────────────────────────
            ConfigEntry {
                exists: true,
                source: ConfigSource::Local,
                path: self.cwd.join(".cowd").join("settings.local.json"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Local,
                path: self.cwd.join(".cowd").join("config.local.yaml"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Local,
                path: self.cwd.join(".cowd").join("config.local.yml"),
            },
            ConfigEntry {
                exists: true,
                source: ConfigSource::Local,
                path: self.cwd.join(".cowd").join("config.local.json"),
            },
        ]
    }

    pub fn load(&self) -> Result<RuntimeConfig, ConfigError> {
        let mut merged = BTreeMap::new();
        let mut loaded_entries = Vec::new();
        let mut mcp_servers = BTreeMap::new();
        let mut all_warnings = Vec::new();

        for entry in self.discover() {
            crate::config_validate::check_unsupported_format(&entry.path)?;
            let is_yaml = is_yaml_path(&entry.path);
            let parsed_opt = if is_yaml {
                read_optional_yaml_object(&entry.path)?
            } else {
                read_optional_json_object(&entry.path)?
            };
            let Some(parsed) = parsed_opt else {
                continue;
            };
            // Skip schema validation for YAML files (no line-number source available)
            if !is_yaml {
                let validation = crate::config_validate::validate_config_file(
                    &parsed.object,
                    &parsed.source,
                    &entry.path,
                );
                if !validation.is_ok() {
                    let first_error = &validation.errors[0];
                    return Err(ConfigError::Parse(first_error.to_string()));
                }
                all_warnings.extend(validation.warnings);
                validate_optional_hooks_config(&parsed.object, &entry.path)?;
            }
            merge_mcp_servers(&mut mcp_servers, entry.source, &parsed.object, &entry.path)?;
            deep_merge_objects(&mut merged, &parsed.object);
            loaded_entries.push(entry);
        }

        // Apply environment variable overrides (CC_* prefix) after file configs.
        let env_overrides = collect_env_overrides();
        deep_merge_objects(&mut merged, &env_overrides);

        // Inject config file `env:` section into the process environment.
        inject_config_env(&merged);

        for warning in &all_warnings {
            eprintln!("warning: {warning}");
        }

        let merged_value = JsonValue::Object(merged.clone());

        let feature_config = RuntimeFeatureConfig {
            hooks: parse_optional_hooks_config(&merged_value)?,
            plugins: parse_optional_plugin_config(&merged_value)?,
            mcp: McpConfigCollection {
                servers: mcp_servers,
            },
            oauth: parse_optional_oauth_config(&merged_value, "merged settings.oauth")?,
            model: parse_optional_model(&merged_value),
            aliases: parse_optional_aliases(&merged_value)?,
            permission_mode: parse_optional_permission_mode(&merged_value)?,
            permission_rules: parse_optional_permission_rules(&merged_value)?,
            approval: parse_optional_approval_config(&merged_value)?,
            sandbox: parse_optional_sandbox_config(&merged_value)?,
            provider_fallbacks: parse_optional_provider_fallbacks(&merged_value)?,
            providers: parse_optional_providers_config(&merged_value)?,
            trusted_roots: parse_optional_trusted_roots(&merged_value)?,
            memory: parse_optional_memory_config(&merged_value)?,
            compression: parse_optional_compression_config(&merged_value)?,
            gateway: parse_optional_gateway_config(&merged_value)?,
        };

        Ok(RuntimeConfig {
            merged,
            loaded_entries,
            feature_config,
        })
    }
}

impl RuntimeConfig {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            merged: BTreeMap::new(),
            loaded_entries: Vec::new(),
            feature_config: RuntimeFeatureConfig::default(),
        }
    }

    #[must_use]
    pub fn merged(&self) -> &BTreeMap<String, JsonValue> {
        &self.merged
    }

    #[must_use]
    pub fn loaded_entries(&self) -> &[ConfigEntry] {
        &self.loaded_entries
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.merged.get(key)
    }

    #[must_use]
    pub fn as_json(&self) -> JsonValue {
        JsonValue::Object(self.merged.clone())
    }

    #[must_use]
    pub fn feature_config(&self) -> &RuntimeFeatureConfig {
        &self.feature_config
    }

    #[must_use]
    pub fn mcp(&self) -> &McpConfigCollection {
        &self.feature_config.mcp
    }

    #[must_use]
    pub fn hooks(&self) -> &RuntimeHookConfig {
        &self.feature_config.hooks
    }

    #[must_use]
    pub fn plugins(&self) -> &RuntimePluginConfig {
        &self.feature_config.plugins
    }

    #[must_use]
    pub fn oauth(&self) -> Option<&OAuthConfig> {
        self.feature_config.oauth.as_ref()
    }

    #[must_use]
    pub fn model(&self) -> Option<&str> {
        self.feature_config.model.as_deref()
    }

    #[must_use]
    pub fn aliases(&self) -> &BTreeMap<String, String> {
        &self.feature_config.aliases
    }

    #[must_use]
    pub fn permission_mode(&self) -> Option<ResolvedPermissionMode> {
        self.feature_config.permission_mode
    }

    #[must_use]
    pub fn permission_rules(&self) -> &RuntimePermissionRuleConfig {
        &self.feature_config.permission_rules
    }

    #[must_use]
    pub fn approval(&self) -> &ApprovalConfig {
        &self.feature_config.approval
    }

    #[must_use]
    pub fn sandbox(&self) -> &SandboxConfig {
        &self.feature_config.sandbox
    }

    #[must_use]
    pub fn provider_fallbacks(&self) -> &ProviderFallbackConfig {
        &self.feature_config.provider_fallbacks
    }

    #[must_use]
    pub fn providers(&self) -> &ProvidersConfig {
        &self.feature_config.providers
    }

    #[must_use]
    pub fn trusted_roots(&self) -> &[String] {
        &self.feature_config.trusted_roots
    }

    #[must_use]
    pub fn memory(&self) -> &MemoryConfig {
        &self.feature_config.memory
    }

    #[must_use]
    pub fn compression(&self) -> &CompressionConfig {
        &self.feature_config.compression
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayConfig {
        &self.feature_config.gateway
    }
}

impl RuntimeFeatureConfig {
    #[must_use]
    pub fn with_hooks(mut self, hooks: RuntimeHookConfig) -> Self {
        self.hooks = hooks;
        self
    }

    #[must_use]
    pub fn with_plugins(mut self, plugins: RuntimePluginConfig) -> Self {
        self.plugins = plugins;
        self
    }

    #[must_use]
    pub fn hooks(&self) -> &RuntimeHookConfig {
        &self.hooks
    }

    #[must_use]
    pub fn plugins(&self) -> &RuntimePluginConfig {
        &self.plugins
    }

    #[must_use]
    pub fn mcp(&self) -> &McpConfigCollection {
        &self.mcp
    }

    #[must_use]
    pub fn oauth(&self) -> Option<&OAuthConfig> {
        self.oauth.as_ref()
    }

    #[must_use]
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    #[must_use]
    pub fn aliases(&self) -> &BTreeMap<String, String> {
        &self.aliases
    }

    #[must_use]
    pub fn permission_mode(&self) -> Option<ResolvedPermissionMode> {
        self.permission_mode
    }

    #[must_use]
    pub fn permission_rules(&self) -> &RuntimePermissionRuleConfig {
        &self.permission_rules
    }

    #[must_use]
    pub fn approval(&self) -> &ApprovalConfig {
        &self.approval
    }

    #[must_use]
    pub fn with_approval(mut self, approval: ApprovalConfig) -> Self {
        self.approval = approval;
        self
    }

    #[must_use]
    pub fn sandbox(&self) -> &SandboxConfig {
        &self.sandbox
    }

    #[must_use]
    pub fn provider_fallbacks(&self) -> &ProviderFallbackConfig {
        &self.provider_fallbacks
    }

    #[must_use]
    pub fn providers(&self) -> &ProvidersConfig {
        &self.providers
    }

    #[must_use]
    pub fn trusted_roots(&self) -> &[String] {
        &self.trusted_roots
    }

    #[must_use]
    pub fn memory(&self) -> &MemoryConfig {
        &self.memory
    }

    #[must_use]
    pub fn compression(&self) -> &CompressionConfig {
        &self.compression
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayConfig {
        &self.gateway
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

impl Default for RuntimePluginConfig {
    fn default() -> Self {
        Self {
            enabled_plugins: BTreeMap::default(),
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

impl RuntimePluginConfig {
    #[must_use]
    pub fn enabled_plugins(&self) -> &BTreeMap<String, bool> {
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

    pub fn set_max_output_tokens(&mut self, max_output_tokens: Option<u32>) {
        self.max_output_tokens = max_output_tokens;
    }

    pub fn set_plugin_state(&mut self, plugin_id: String, enabled: bool) {
        self.enabled_plugins.insert(plugin_id, enabled);
    }

    #[must_use]
    pub fn state_for(&self, plugin_id: &str, default_enabled: bool) -> bool {
        self.enabled_plugins
            .get(plugin_id)
            .copied()
            .unwrap_or(default_enabled)
    }
}

#[must_use]
/// Returns the default per-user config directory used by the runtime.
pub fn default_config_home() -> PathBuf {
    // CC_CONFIG_HOME takes highest priority.
    if let Some(path) = std::env::var_os("COWD_CONFIG_HOME") {
        return PathBuf::from(path);
    }
    std::env::var_os("HOME")
        .map(|home| PathBuf::from(home).join(".cowd"))
        .unwrap_or_else(|| PathBuf::from(".cowd"))
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
    pub fn merged(&self, other: &Self) -> Self {
        let mut merged = self.clone();
        merged.extend(other);
        merged
    }

    pub fn extend(&mut self, other: &Self) {
        extend_unique(&mut self.pre_tool_use, other.pre_tool_use());
        extend_unique(&mut self.post_tool_use, other.post_tool_use());
        extend_unique(
            &mut self.post_tool_use_failure,
            other.post_tool_use_failure(),
        );
    }

    #[must_use]
    pub fn post_tool_use_failure(&self) -> &[String] {
        &self.post_tool_use_failure
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

impl McpConfigCollection {
    #[must_use]
    pub fn servers(&self) -> &BTreeMap<String, ScopedMcpServerConfig> {
        &self.servers
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ScopedMcpServerConfig> {
        self.servers.get(name)
    }
}

impl ScopedMcpServerConfig {
    #[must_use]
    pub fn transport(&self) -> McpTransport {
        self.config.transport()
    }
}

impl McpServerConfig {
    #[must_use]
    pub fn transport(&self) -> McpTransport {
        match self {
            Self::Stdio(_) => McpTransport::Stdio,
            Self::Sse(_) => McpTransport::Sse,
            Self::Http(_) => McpTransport::Http,
            Self::Ws(_) => McpTransport::Ws,
            Self::Sdk(_) => McpTransport::Sdk,
            Self::ManagedProxy(_) => McpTransport::ManagedProxy,
        }
    }
}

/// Parsed JSON object paired with its raw source text for validation.
struct ParsedConfigFile {
    object: BTreeMap<String, JsonValue>,
    source: String,
}

/// Returns true if the given path has a `.yaml` or `.yml` extension.
fn is_yaml_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml"))
        .unwrap_or(false)
}

/// Convert a `serde_yaml::Value` into the project-internal `JsonValue`.
/// Returns `None` for YAML types that have no JSON equivalent (e.g. tagged values).
fn yaml_to_json(value: serde_yaml::Value) -> Option<JsonValue> {
    match value {
        serde_yaml::Value::Null => Some(JsonValue::Null),
        serde_yaml::Value::Bool(b) => Some(JsonValue::Bool(b)),
        serde_yaml::Value::Number(n) => {
            // Prefer integer representation; fall back to rounded float.
            if let Some(i) = n.as_i64() {
                Some(JsonValue::Number(i))
            } else if let Some(f) = n.as_f64() {
                #[allow(clippy::cast_possible_truncation)]
                Some(JsonValue::Number(f as i64))
            } else {
                None
            }
        }
        serde_yaml::Value::String(s) => Some(JsonValue::String(s)),
        serde_yaml::Value::Sequence(seq) => {
            let items = seq
                .into_iter()
                .map(yaml_to_json)
                .collect::<Option<Vec<_>>>()?;
            Some(JsonValue::Array(items))
        }
        serde_yaml::Value::Mapping(map) => {
            let mut object = BTreeMap::new();
            for (k, v) in map {
                let key = match k {
                    serde_yaml::Value::String(s) => s,
                    other => format!("{other:?}"),
                };
                if let Some(json_v) = yaml_to_json(v) {
                    object.insert(key, json_v);
                }
            }
            Some(JsonValue::Object(object))
        }
        serde_yaml::Value::Tagged(tagged) => yaml_to_json(tagged.value),
    }
}

fn read_optional_yaml_object(path: &Path) -> Result<Option<ParsedConfigFile>, ConfigError> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ConfigError::Io(error)),
    };

    if contents.trim().is_empty() {
        return Ok(Some(ParsedConfigFile {
            object: BTreeMap::new(),
            source: contents,
        }));
    }

    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&contents)
        .map_err(|e| ConfigError::Parse(format!("{}: {e}", path.display())))?;

    let Some(json_value) = yaml_to_json(yaml_value) else {
        return Err(ConfigError::Parse(format!(
            "{}: YAML file could not be converted to a config object",
            path.display()
        )));
    };

    let Some(object) = json_value.as_object() else {
        return Err(ConfigError::Parse(format!(
            "{}: top-level settings value must be a YAML mapping",
            path.display()
        )));
    };

    Ok(Some(ParsedConfigFile {
        object: object.clone(),
        source: contents,
    }))
}

/// Inject config file `env:` section into the process environment.
///
/// System environment variables take precedence — existing vars are not
/// overwritten. This preserves the priority chain:
/// system env > config file env > defaults.
fn inject_config_env(merged: &BTreeMap<String, JsonValue>) {
    if let Some(JsonValue::Object(env_obj)) = merged.get("env") {
        for (key, value) in env_obj {
            // Only set if not already present in system env.
            if std::env::var(key).is_err() {
                if let Some(v) = value.as_str() {
                    std::env::set_var(key, v);
                    tracing::debug!("injected config env: {key}");
                }
            }
        }
    }
}

/// Collect `CC_*` environment variables and convert them to a nested config
/// map that can be deep-merged on top of file-based configuration.
///
/// Mapping rules:
/// - Strip the `CC_` prefix.
/// - Split remaining name on `_` to build nested key path (all lowercase).
/// - Value type is inferred: `"true"`/`"false"` → bool, parseable integer → Number, else String.
///
/// Examples:
/// - `CC_MEMORY_ENABLED=false` → `{"memory": {"enabled": false}}`
/// - `CC_COMPRESSION_SESSION_THRESHOLD_TOKENS=50000` → `{"compression": {"session": {"threshold_tokens": 50000}}}`
/// - `CC_MODEL=opus` → `{"model": "opus"}`
fn collect_env_overrides() -> BTreeMap<String, JsonValue> {
    let mut root: BTreeMap<String, JsonValue> = BTreeMap::new();

    for (key_os, val_os) in std::env::vars_os() {
        let key = match key_os.into_string() {
            Ok(k) => k,
            Err(_) => continue,
        };
        let val = match val_os.into_string() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let Some(without_prefix) = key.strip_prefix(ENV_OVERRIDE_PREFIX) else {
            continue;
        };

        // Skip known non-config CC_* vars used elsewhere (e.g. COWD_MAX_OUTPUT_TOKENS).
        if without_prefix.is_empty() {
            continue;
        }

        // Split on '_' and lowercase each segment to get the nested path.
        let segments: Vec<String> = without_prefix
            .split('_')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase())
            .collect();

        if segments.is_empty() {
            continue;
        }

        let json_val = infer_env_value(&val);
        insert_nested(&mut root, &segments, json_val);
    }

    root
}

/// Infer the `JsonValue` type from a raw string environment variable value.
fn infer_env_value(value: &str) -> JsonValue {
    match value {
        "true" => JsonValue::Bool(true),
        "false" => JsonValue::Bool(false),
        _ => {
            if let Ok(n) = value.parse::<i64>() {
                JsonValue::Number(n)
            } else {
                JsonValue::String(value.to_string())
            }
        }
    }
}

/// Recursively insert `value` at `path` within `map`, creating intermediate
/// `Object` nodes as needed.
fn insert_nested(map: &mut BTreeMap<String, JsonValue>, path: &[String], value: JsonValue) {
    if path.is_empty() {
        return;
    }
    let head = &path[0];
    let tail = &path[1..];
    if tail.is_empty() {
        map.insert(head.clone(), value);
    } else {
        let child = map
            .entry(head.clone())
            .or_insert_with(|| JsonValue::Object(BTreeMap::new()));
        if let JsonValue::Object(child_map) = child {
            insert_nested(child_map, tail, value);
        } else {
            // Scalar already there – env override wins; replace with nested object.
            let mut new_map = BTreeMap::new();
            insert_nested(&mut new_map, tail, value);
            *child = JsonValue::Object(new_map);
        }
    }
}

fn read_optional_json_object(path: &Path) -> Result<Option<ParsedConfigFile>, ConfigError> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ConfigError::Io(error)),
    };

    if contents.trim().is_empty() {
        return Ok(Some(ParsedConfigFile {
            object: BTreeMap::new(),
            source: contents,
        }));
    }

    let parsed = match JsonValue::parse(&contents) {
        Ok(parsed) => parsed,
        Err(error) => return Err(ConfigError::Parse(format!("{}: {error}", path.display()))),
    };
    let Some(object) = parsed.as_object() else {
        return Err(ConfigError::Parse(format!(
            "{}: top-level settings value must be a JSON object",
            path.display()
        )));
    };
    Ok(Some(ParsedConfigFile {
        object: object.clone(),
        source: contents,
    }))
}

fn merge_mcp_servers(
    target: &mut BTreeMap<String, ScopedMcpServerConfig>,
    source: ConfigSource,
    root: &BTreeMap<String, JsonValue>,
    path: &Path,
) -> Result<(), ConfigError> {
    let Some(mcp_servers) = root.get("mcpServers") else {
        return Ok(());
    };
    let servers = expect_object(mcp_servers, &format!("{}: mcpServers", path.display()))?;
    for (name, value) in servers {
        let parsed = parse_mcp_server_config(
            name,
            value,
            &format!("{}: mcpServers.{name}", path.display()),
        )?;
        target.insert(
            name.clone(),
            ScopedMcpServerConfig {
                scope: source,
                config: parsed,
            },
        );
    }
    Ok(())
}

fn parse_optional_model(root: &JsonValue) -> Option<String> {
    root.as_object()
        .and_then(|object| object.get("model"))
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

fn parse_optional_aliases(root: &JsonValue) -> Result<BTreeMap<String, String>, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(BTreeMap::new());
    };
    Ok(optional_string_map(object, "aliases", "merged settings")?.unwrap_or_default())
}

fn parse_optional_hooks_config(root: &JsonValue) -> Result<RuntimeHookConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(RuntimeHookConfig::default());
    };
    parse_optional_hooks_config_object(object, "merged settings.hooks")
}

fn parse_optional_hooks_config_object(
    object: &BTreeMap<String, JsonValue>,
    context: &str,
) -> Result<RuntimeHookConfig, ConfigError> {
    let Some(hooks_value) = object.get("hooks") else {
        return Ok(RuntimeHookConfig::default());
    };
    let hooks = expect_object(hooks_value, context)?;
    Ok(RuntimeHookConfig {
        pre_tool_use: optional_string_array(hooks, "PreToolUse", context)?.unwrap_or_default(),
        post_tool_use: optional_string_array(hooks, "PostToolUse", context)?.unwrap_or_default(),
        post_tool_use_failure: optional_string_array(hooks, "PostToolUseFailure", context)?
            .unwrap_or_default(),
    })
}

fn validate_optional_hooks_config(
    root: &BTreeMap<String, JsonValue>,
    path: &Path,
) -> Result<(), ConfigError> {
    parse_optional_hooks_config_object(root, &format!("{}: hooks", path.display())).map(|_| ())
}

fn parse_optional_permission_rules(
    root: &JsonValue,
) -> Result<RuntimePermissionRuleConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(RuntimePermissionRuleConfig::default());
    };
    let Some(permissions) = object.get("permissions").and_then(JsonValue::as_object) else {
        return Ok(RuntimePermissionRuleConfig::default());
    };

    Ok(RuntimePermissionRuleConfig {
        allow: optional_string_array(permissions, "allow", "merged settings.permissions")?
            .unwrap_or_default(),
        deny: optional_string_array(permissions, "deny", "merged settings.permissions")?
            .unwrap_or_default(),
        ask: optional_string_array(permissions, "ask", "merged settings.permissions")?
            .unwrap_or_default(),
    })
}

fn parse_optional_approval_config(root: &JsonValue) -> Result<ApprovalConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(ApprovalConfig::default());
    };
    let Some(permissions) = object.get("permissions").and_then(JsonValue::as_object) else {
        return Ok(ApprovalConfig::default());
    };
    let Some(approval) = permissions.get("approval").and_then(JsonValue::as_object) else {
        return Ok(ApprovalConfig::default());
    };

    Ok(ApprovalConfig {
        yolo_mode: optional_bool(approval, "yolo_mode", "merged settings.permissions.approval")?.unwrap_or(false),
        yolo_honor_critical: optional_bool(approval, "yolo_honor_critical", "merged settings.permissions.approval")?.unwrap_or(true),
        auto_pass_read_only: optional_bool(approval, "auto_pass_read_only", "merged settings.permissions.approval")?.unwrap_or(true),
        auto_pass_low_risk: optional_bool(approval, "auto_pass_low_risk", "merged settings.permissions.approval")?.unwrap_or(true),
    })
}

fn parse_optional_plugin_config(root: &JsonValue) -> Result<RuntimePluginConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(RuntimePluginConfig::default());
    };

    let mut config = RuntimePluginConfig::default();
    if let Some(enabled_plugins) = object.get("enabledPlugins") {
        config.enabled_plugins = parse_bool_map(enabled_plugins, "merged settings.enabledPlugins")?;
    }

    let Some(plugins_value) = object.get("plugins") else {
        return Ok(config);
    };
    let plugins = expect_object(plugins_value, "merged settings.plugins")?;

    if let Some(enabled_value) = plugins.get("enabled") {
        config.enabled_plugins = parse_bool_map(enabled_value, "merged settings.plugins.enabled")?;
    }
    config.external_directories =
        optional_string_array(plugins, "externalDirectories", "merged settings.plugins")?
            .unwrap_or_default();
    config.install_root =
        optional_string_dual(plugins, "install_root", "merged settings.plugins")?.map(str::to_string);
    config.registry_path =
        optional_string_dual(plugins, "registry_path", "merged settings.plugins")?.map(str::to_string);
    config.bundled_root =
        optional_string_dual(plugins, "bundled_root", "merged settings.plugins")?.map(str::to_string);
    config.max_output_tokens = optional_u32(plugins, "maxOutputTokens", "merged settings.plugins")?
        .or_else(|| std::env::var("COWD_MAX_OUTPUT_TOKENS").ok().and_then(|v| v.parse().ok()));
    Ok(config)
}

fn parse_optional_permission_mode(
    root: &JsonValue,
) -> Result<Option<ResolvedPermissionMode>, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(None);
    };
    if let Some(mode) = object.get("permissionMode").and_then(JsonValue::as_str) {
        return parse_permission_mode_label(mode, "merged settings.permissionMode").map(Some);
    }
    let Some(mode) = object
        .get("permissions")
        .and_then(JsonValue::as_object)
        .and_then(|permissions| permissions.get("defaultMode"))
        .and_then(JsonValue::as_str)
    else {
        return Ok(None);
    };
    parse_permission_mode_label(mode, "merged settings.permissions.defaultMode").map(Some)
}

fn parse_permission_mode_label(
    mode: &str,
    context: &str,
) -> Result<ResolvedPermissionMode, ConfigError> {
    match mode {
        "default" | "plan" | "read-only" => Ok(ResolvedPermissionMode::ReadOnly),
        "acceptEdits" | "auto" | "workspace-write" => Ok(ResolvedPermissionMode::WorkspaceWrite),
        "dontAsk" | "danger-full-access" => Ok(ResolvedPermissionMode::DangerFullAccess),
        other => Err(ConfigError::Parse(format!(
            "{context}: unsupported permission mode {other}"
        ))),
    }
}

fn parse_optional_sandbox_config(root: &JsonValue) -> Result<SandboxConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(SandboxConfig::default());
    };
    let Some(sandbox_value) = object.get("sandbox") else {
        return Ok(SandboxConfig::default());
    };
    let sandbox = expect_object(sandbox_value, "merged settings.sandbox")?;
    let filesystem_mode = optional_string_dual(sandbox, "filesystem_mode", "merged settings.sandbox")?
        .map(parse_filesystem_mode_label)
        .transpose()?;
    Ok(SandboxConfig {
        enabled: optional_bool(sandbox, "enabled", "merged settings.sandbox")?,
        namespace_restrictions: optional_bool(
            sandbox,
            "namespaceRestrictions",
            "merged settings.sandbox",
        )?,
        network_isolation: optional_bool(sandbox, "networkIsolation", "merged settings.sandbox")?,
        filesystem_mode,
        allowed_mounts: optional_string_array(sandbox, "allowedMounts", "merged settings.sandbox")?
            .unwrap_or_default(),
    })
}

fn parse_optional_provider_fallbacks(
    root: &JsonValue,
) -> Result<ProviderFallbackConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(ProviderFallbackConfig::default());
    };
    let Some(value) = object.get("providerFallbacks") else {
        return Ok(ProviderFallbackConfig::default());
    };
    let entry = expect_object(value, "merged settings.providerFallbacks")?;
    let primary =
        optional_string(entry, "primary", "merged settings.providerFallbacks")?.map(str::to_string);
    let fallbacks = optional_string_array(entry, "fallbacks", "merged settings.providerFallbacks")?
        .unwrap_or_default();
    Ok(ProviderFallbackConfig { primary, fallbacks })
}

/// Parse the optional top-level `providers` mapping.
///
/// Expected YAML shape:
/// ```yaml
/// providers:
///   stepfun:
///     baseUrl: "https://api.stepfun.com/v1"
///     apiKey: "..."
///     models:
///       - "step-3.5-flash"
///   bailian:
///     baseUrl: "https://dashscope.aliyuncs.com/compatible-mode/v1"
///     apiKey: "sk-..."
///     models:
///       - "qwen3-coder-next"
/// ```
///
/// If the `providers` key is absent the function returns an empty
/// [`ProvidersConfig`] so callers can gracefully fall back to environment
/// variables (`OPENAI_BASE_URL` / `OPENAI_API_KEY`).
fn parse_optional_providers_config(root: &JsonValue) -> Result<ProvidersConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(ProvidersConfig::default());
    };
    let Some(providers_value) = object.get("providers") else {
        return Ok(ProvidersConfig::default());
    };
    let providers_map = expect_object(providers_value, "merged settings.providers")?;
    let mut providers = HashMap::new();
    for (name, value) in providers_map {
        let ctx = format!("merged settings.providers.{name}");
        let entry = expect_object(value, &ctx)?;
        let base_url = optional_string_dual(entry, "base_url", &ctx)?
            .map(str::to_string)
            .unwrap_or_default();
        let api_key = optional_string_dual(entry, "api_key", &ctx)?
            .map(str::to_string)
            .unwrap_or_default();
        let models =
            optional_string_array(entry, "models", &ctx)?.unwrap_or_default();
        providers.insert(
            name.clone(),
            ProviderConfig {
                base_url,
                api_key,
                models,
            },
        );
    }
    Ok(ProvidersConfig { providers })
}

fn parse_optional_trusted_roots(root: &JsonValue) -> Result<Vec<String>, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(Vec::new());
    };
    Ok(
        optional_string_array(object, "trustedRoots", "merged settings.trustedRoots")?
            .unwrap_or_default(),
    )
}

fn parse_optional_memory_config(root: &JsonValue) -> Result<MemoryConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(MemoryConfig::default());
    };
    let Some(mem_value) = object.get("memory") else {
        return Ok(MemoryConfig::default());
    };
    let mem = expect_object(mem_value, "merged settings.memory")?;
    let enabled = optional_bool(mem, "enabled", "merged settings.memory")?;
    let store_path = optional_string_dual(mem, "store_path", "merged settings.memory")?
        .map(PathBuf::from);
    let layers = if let Some(layers_val) = mem.get("layers") {
        let l = expect_object(layers_val, "merged settings.memory.layers")?;
        LayerConfig {
            l0_enabled: optional_bool(l, "l0Enabled", "merged settings.memory.layers")?.
                unwrap_or(LayerConfig::default().l0_enabled),
            l1_max_tokens: optional_u32(l, "l1MaxTokens", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l1_max_tokens),
            l2_max_tokens: optional_u32(l, "l2MaxTokens", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l2_max_tokens),
            l3_search_limit: optional_u32(l, "l3SearchLimit", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l3_search_limit),
            l4_enabled: optional_bool(l, "l4Enabled", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l4_enabled),
        }
    } else {
        LayerConfig::default()
    };
    let extraction = if let Some(ext_val) = mem.get("extraction") {
        let e = expect_object(ext_val, "merged settings.memory.extraction")?;
        ExtractionConfig {
            auto_extract: optional_bool(e, "autoExtract", "merged settings.memory.extraction")?
                .unwrap_or(ExtractionConfig::default().auto_extract),
        }
    } else {
        ExtractionConfig::default()
    };
    let vector = if let Some(vec_val) = mem.get("vector") {
        let v = expect_object(vec_val, "merged settings.memory.vector")?;
        let defaults = VectorConfig::default();
        // Static config values.
        let enabled = optional_bool(v, "enabled", "merged settings.memory.vector")?;
        let embedding_model = optional_string_dual(v, "embedding_model", "merged settings.memory.vector")?;
        let dimension = optional_u32(v, "dimension", "merged settings.memory.vector")?;
        let api_url = optional_string_dual(v, "api_url", "merged settings.memory.vector")?;
        let api_key = optional_string_dual(v, "api_key", "merged settings.memory.vector")?;
        let timeout_secs = optional_u64(v, "timeoutSecs", "merged settings.memory.vector")?;
        let batch_size = optional_usize(v, "batchSize", "merged settings.memory.vector")?;

        // Environment variable overrides.
        let resolved_model = std::env::var("COWD_MEMORY_VECTOR_MODEL")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| embedding_model.map(str::to_string))
            .unwrap_or(defaults.embedding_model);
        let resolved_api_url = std::env::var("COWD_MEMORY_VECTOR_API_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| api_url.map(str::to_string))
            .unwrap_or(defaults.api_url);
        let resolved_api_key = std::env::var("COWD_VECTOR_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| api_key.map(str::to_string))
            .unwrap_or(defaults.api_key);

        VectorConfig {
            enabled: enabled.unwrap_or(defaults.enabled),
            embedding_model: resolved_model,
            dimension: dimension.unwrap_or(defaults.dimension),
            api_url: resolved_api_url,
            api_key: resolved_api_key,
            timeout_secs: timeout_secs.unwrap_or(defaults.timeout_secs),
            batch_size: batch_size.unwrap_or(defaults.batch_size),
        }
    } else {
        // No vector section; still apply env var overrides.
        let defaults = VectorConfig::default();
        let embedding_model = std::env::var("COWD_MEMORY_VECTOR_MODEL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(defaults.embedding_model);
        let api_url = std::env::var("COWD_MEMORY_VECTOR_API_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(defaults.api_url);
        let api_key = std::env::var("COWD_VECTOR_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(defaults.api_key);
        VectorConfig {
            embedding_model,
            api_url,
            api_key,
            ..defaults
        }
    };
    Ok(MemoryConfig {
        enabled: enabled.unwrap_or(MemoryConfig::default().enabled),
        store_path,
        layers,
        extraction,
        vector,
        aaak_index_enabled: optional_bool(mem, "aaakIndexEnabled", "merged settings.memory")?
            .unwrap_or(MemoryConfig::default().aaak_index_enabled),
        coherence_threshold_bp: optional_u32(mem, "coherenceThreshold", "merged settings.memory")?
            .unwrap_or(MemoryConfig::default().coherence_threshold_bp),
    })
}

fn parse_optional_compression_config(root: &JsonValue) -> Result<CompressionConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(CompressionConfig::default());
    };
    let Some(cmp_value) = object.get("compression") else {
        return Ok(CompressionConfig::default());
    };
    let cmp = expect_object(cmp_value, "merged settings.compression")?;
    let micro = if let Some(micro_val) = cmp.get("micro") {
        let m = expect_object(micro_val, "merged settings.compression.micro")?;
        MicroCompactConfig {
            enabled: optional_bool(m, "enabled", "merged settings.compression.micro")?
                .unwrap_or(MicroCompactConfig::default().enabled),
            tool_result_max_chars: optional_u32(m, "toolResultMaxChars", "merged settings.compression.micro")?
                .unwrap_or(MicroCompactConfig::default().tool_result_max_chars),
            time_decay_factor: optional_f32(m, "timeDecayFactor", "merged settings.compression.micro")?
                .unwrap_or(MicroCompactConfig::default().time_decay_factor),
        }
    } else {
        MicroCompactConfig::default()
    };
    let session = if let Some(sess_val) = cmp.get("session") {
        let s = expect_object(sess_val, "merged settings.compression.session")?;
        SessionCompactConfig {
            threshold_tokens: optional_u32(s, "thresholdTokens", "merged settings.compression.session")?
                .unwrap_or(SessionCompactConfig::default().threshold_tokens),
            preserve_recent: optional_u32(s, "preserveRecent", "merged settings.compression.session")?
                .unwrap_or(SessionCompactConfig::default().preserve_recent),
            summary_max_tokens: optional_u32(s, "summaryMaxTokens", "merged settings.compression.session")?
                .unwrap_or(SessionCompactConfig::default().summary_max_tokens),
            buffer_tokens: optional_u32(s, "bufferTokens", "merged settings.compression.session")?
                .unwrap_or(SessionCompactConfig::default().buffer_tokens),
        }
    } else {
        SessionCompactConfig::default()
    };
    let deep = if let Some(deep_val) = cmp.get("deep") {
        let d = expect_object(deep_val, "merged settings.compression.deep")?;
        DeepCompactConfig {
            enabled: optional_bool(d, "enabled", "merged settings.compression.deep")?
                .unwrap_or(DeepCompactConfig::default().enabled),
            iterative_update: optional_bool(d, "iterativeUpdate", "merged settings.compression.deep")?
                .unwrap_or(DeepCompactConfig::default().iterative_update),
        }
    } else {
        DeepCompactConfig::default()
    };
    let circuit_breaker = if let Some(cb_val) = cmp.get("circuitBreaker") {
        let cb = expect_object(cb_val, "merged settings.compression.circuitBreaker")?;
        CircuitBreakerConfig {
            max_retries: optional_u32(cb, "maxRetries", "merged settings.compression.circuitBreaker")?
                .unwrap_or(CircuitBreakerConfig::default().max_retries),
            cooldown_secs: optional_u32(cb, "cooldownSecs", "merged settings.compression.circuitBreaker")?
                .unwrap_or(CircuitBreakerConfig::default().cooldown_secs),
        }
    } else {
        CircuitBreakerConfig::default()
    };
    Ok(CompressionConfig {
        micro,
        session,
        deep,
        circuit_breaker,
    })
}

fn parse_optional_gateway_config(root: &JsonValue) -> Result<GatewayConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(GatewayConfig::default());
    };
    let Some(gw_value) = object.get("gateway") else {
        return Ok(GatewayConfig::default());
    };
    let gw = expect_object(gw_value, "merged settings.gateway")?;
    let enabled = optional_bool(gw, "enabled", "merged settings.gateway")?
        .unwrap_or(GatewayConfig::default().enabled);
    let session_reset = optional_string_dual(gw, "session_reset", "merged settings.gateway")?
        .map(|s| parse_session_reset_policy(s, "merged settings.gateway.sessionReset"))
        .transpose()?
        .unwrap_or_default();
    let platforms = if let Some(plat_val) = gw.get("platforms") {
        let arr = expect_array(plat_val, "merged settings.gateway.platforms")?;
        arr.iter()
            .enumerate()
            .map(|(i, v)| {
                let ctx = format!("merged settings.gateway.platforms[{i}]");
                let p = expect_object(v, &ctx)?;
                Ok(PlatformConfig {
                    platform_type: expect_string(p, "platformType", &ctx)?.to_string(),
                    enabled: optional_bool(p, "enabled", &ctx)?.unwrap_or(true),
                    extra: p
                        .iter()
                        .filter(|(k, _)| k.as_str() != "platformType" && k.as_str() != "enabled")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?
    } else {
        Vec::new()
    };
    Ok(GatewayConfig {
        enabled,
        platforms,
        session_reset,
    })
}

fn parse_session_reset_policy(
    value: &str,
    context: &str,
) -> Result<SessionResetPolicy, ConfigError> {
    match value {
        "daily" => Ok(SessionResetPolicy::Daily),
        "idle" => Ok(SessionResetPolicy::Idle),
        "both" => Ok(SessionResetPolicy::Both),
        "none" => Ok(SessionResetPolicy::None),
        other => Err(ConfigError::Parse(format!(
            "{context}: unsupported session reset policy {other}"
        ))),
    }
}

fn optional_f32(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<f32>, ConfigError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    match value {
        JsonValue::Number(n) => Ok(Some(*n as f32)),
        other => Err(ConfigError::Parse(format!(
            "{context}.{key}: expected a number, got {}",
            json_value_type_name(other)
        ))),
    }
}

fn json_value_type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn expect_array<'a>(
    value: &'a JsonValue,
    context: &str,
) -> Result<&'a [JsonValue], ConfigError> {
    value.as_array().ok_or_else(|| {
        ConfigError::Parse(format!(
            "{context}: expected an array, got {}",
            json_value_type_name(value)
        ))
    })
}



fn parse_filesystem_mode_label(value: &str) -> Result<FilesystemIsolationMode, ConfigError> {
    match value {
        "off" | "none" => Ok(FilesystemIsolationMode::Off),
        "workspace-only" => Ok(FilesystemIsolationMode::WorkspaceOnly),
        "allow-list" => Ok(FilesystemIsolationMode::AllowList),
        other => Err(ConfigError::Parse(format!(
            "merged settings.sandbox.filesystemMode: unsupported filesystem mode {other}"
        ))),
    }
}

fn parse_optional_oauth_config(
    root: &JsonValue,
    context: &str,
) -> Result<Option<OAuthConfig>, ConfigError> {
    let Some(oauth_value) = root.as_object().and_then(|object| object.get("oauth")) else {
        return Ok(None);
    };
    let object = expect_object(oauth_value, context)?;
    let client_id = expect_string(object, "clientId", context)?.to_string();
    let authorize_url = expect_string(object, "authorizeUrl", context)?.to_string();
    let token_url = expect_string(object, "tokenUrl", context)?.to_string();
    let callback_port = optional_u16(object, "callbackPort", context)?;
    let manual_redirect_url =
        optional_string(object, "manualRedirectUrl", context)?.map(str::to_string);
    let scopes = optional_string_array(object, "scopes", context)?.unwrap_or_default();
    Ok(Some(OAuthConfig {
        client_id,
        authorize_url,
        token_url,
        callback_port,
        manual_redirect_url,
        scopes,
    }))
}

fn parse_mcp_server_config(
    server_name: &str,
    value: &JsonValue,
    context: &str,
) -> Result<McpServerConfig, ConfigError> {
    let object = expect_object(value, context)?;
    let server_type =
        optional_string(object, "type", context)?.unwrap_or_else(|| infer_mcp_server_type(object));
    match server_type {
        "stdio" => Ok(McpServerConfig::Stdio(McpStdioServerConfig {
            command: expect_string(object, "command", context)?.to_string(),
            args: optional_string_array(object, "args", context)?.unwrap_or_default(),
            env: optional_string_map(object, "env", context)?.unwrap_or_default(),
            tool_call_timeout_ms: optional_u64(object, "toolCallTimeoutMs", context)?,
        })),
        "sse" => Ok(McpServerConfig::Sse(parse_mcp_remote_server_config(
            object, context,
        )?)),
        "http" => Ok(McpServerConfig::Http(parse_mcp_remote_server_config(
            object, context,
        )?)),
        "ws" => Ok(McpServerConfig::Ws(McpWebSocketServerConfig {
            url: expect_string(object, "url", context)?.to_string(),
            headers: optional_string_map(object, "headers", context)?.unwrap_or_default(),
            headers_helper: optional_string(object, "headersHelper", context)?.map(str::to_string),
        })),
        "sdk" => Ok(McpServerConfig::Sdk(McpSdkServerConfig {
            name: expect_string(object, "name", context)?.to_string(),
        })),
        "claudeai-proxy" => Ok(McpServerConfig::ManagedProxy(McpManagedProxyServerConfig {
            url: expect_string(object, "url", context)?.to_string(),
            id: expect_string(object, "id", context)?.to_string(),
        })),
        other => Err(ConfigError::Parse(format!(
            "{context}: unsupported MCP server type for {server_name}: {other}"
        ))),
    }
}

fn infer_mcp_server_type(object: &BTreeMap<String, JsonValue>) -> &'static str {
    if object.contains_key("url") {
        "http"
    } else {
        "stdio"
    }
}

fn parse_mcp_remote_server_config(
    object: &BTreeMap<String, JsonValue>,
    context: &str,
) -> Result<McpRemoteServerConfig, ConfigError> {
    Ok(McpRemoteServerConfig {
        url: expect_string(object, "url", context)?.to_string(),
        headers: optional_string_map(object, "headers", context)?.unwrap_or_default(),
        headers_helper: optional_string(object, "headersHelper", context)?.map(str::to_string),
        oauth: parse_optional_mcp_oauth_config(object, context)?,
    })
}

fn parse_optional_mcp_oauth_config(
    object: &BTreeMap<String, JsonValue>,
    context: &str,
) -> Result<Option<McpOAuthConfig>, ConfigError> {
    let Some(value) = object.get("oauth") else {
        return Ok(None);
    };
    let oauth = expect_object(value, &format!("{context}.oauth"))?;
    Ok(Some(McpOAuthConfig {
        client_id: optional_string(oauth, "clientId", context)?.map(str::to_string),
        callback_port: optional_u16(oauth, "callbackPort", context)?,
        auth_server_metadata_url: optional_string(oauth, "authServerMetadataUrl", context)?
            .map(str::to_string),
        xaa: optional_bool(oauth, "xaa", context)?,
    }))
}

fn expect_object<'a>(
    value: &'a JsonValue,
    context: &str,
) -> Result<&'a BTreeMap<String, JsonValue>, ConfigError> {
    value
        .as_object()
        .ok_or_else(|| ConfigError::Parse(format!("{context}: expected JSON object")))
}

fn expect_string<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<&'a str, ConfigError> {
    object
        .get(key)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ConfigError::Parse(format!("{context}: missing string field {key}")))
}

fn optional_string<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<&'a str>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or_else(|| ConfigError::Parse(format!("{context}: field {key} must be a string"))),
    }
}

fn optional_bool(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<bool>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| ConfigError::Parse(format!("{context}: field {key} must be a boolean"))),
    }
}

fn optional_u16(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<u16>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(value) => {
            let Some(number) = value.as_i64() else {
                return Err(ConfigError::Parse(format!(
                    "{context}: field {key} must be an integer"
                )));
            };
            let number = u16::try_from(number).map_err(|_| {
                ConfigError::Parse(format!("{context}: field {key} is out of range"))
            })?;
            Ok(Some(number))
        }
    }
}

fn optional_u32(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<u32>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(value) => {
            let Some(number) = value.as_i64() else {
                return Err(ConfigError::Parse(format!(
                    "{context}: field {key} must be a non-negative integer"
                )));
            };
            let number = u32::try_from(number).map_err(|_| {
                ConfigError::Parse(format!("{context}: field {key} is out of range"))
            })?;
            Ok(Some(number))
        }
    }
}

fn optional_u64(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<u64>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(value) => {
            let Some(number) = value.as_i64() else {
                return Err(ConfigError::Parse(format!(
                    "{context}: field {key} must be a non-negative integer"
                )));
            };
            let number = u64::try_from(number).map_err(|_| {
                ConfigError::Parse(format!("{context}: field {key} is out of range"))
            })?;
            Ok(Some(number))
        }
    }
}

fn optional_usize(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<usize>, ConfigError> {
    match object.get(key) {
        Some(value) => {
            let Some(number) = value.as_i64() else {
                return Err(ConfigError::Parse(format!(
                    "{context}: field {key} must be a non-negative integer"
                )));
            };
            let number = usize::try_from(number).map_err(|_| {
                ConfigError::Parse(format!("{context}: field {key} is out of range"))
            })?;
            Ok(Some(number))
        }
        None => Ok(None),
    }
}

fn parse_bool_map(value: &JsonValue, context: &str) -> Result<BTreeMap<String, bool>, ConfigError> {
    let Some(map) = value.as_object() else {
        return Err(ConfigError::Parse(format!(
            "{context}: expected JSON object"
        )));
    };
    map.iter()
        .map(|(key, value)| {
            value
                .as_bool()
                .map(|enabled| (key.clone(), enabled))
                .ok_or_else(|| {
                    ConfigError::Parse(format!("{context}: field {key} must be a boolean"))
                })
        })
        .collect()
}

fn optional_string_array(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<Vec<String>>, ConfigError> {
    match object.get(key) {
        Some(value) => {
            let Some(array) = value.as_array() else {
                return Err(ConfigError::Parse(format!(
                    "{context}: field {key} must be an array"
                )));
            };
            array
                .iter()
                .map(|item| {
                    item.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                        ConfigError::Parse(format!(
                            "{context}: field {key} must contain only strings"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some)
        }
        None => Ok(None),
    }
}

fn optional_string_map(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<BTreeMap<String, String>>, ConfigError> {
    match object.get(key) {
        Some(value) => {
            let Some(map) = value.as_object() else {
                return Err(ConfigError::Parse(format!(
                    "{context}: field {key} must be an object"
                )));
            };
            map.iter()
                .map(|(entry_key, entry_value)| {
                    entry_value
                        .as_str()
                        .map(|text| (entry_key.clone(), text.to_string()))
                        .ok_or_else(|| {
                            ConfigError::Parse(format!(
                                "{context}: field {key} must contain only string values"
                            ))
                        })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(Some)
        }
        None => Ok(None),
    }
}

/// Convert a snake_case string to camelCase.
fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize = false;
    for c in s.chars() {
        if c == '_' {
            capitalize = true;
        } else if capitalize {
            result.push(c.to_ascii_uppercase());
            capitalize = false;
        } else {
            result.push(c);
        }
    }
    result
}

/// Look up a config value, supporting both snake_case (preferred) and camelCase (deprecated).
/// If found via camelCase only, emits a deprecation warning.
fn optional_string_dual<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<&'a str>, ConfigError> {
    // Try snake_case first.
    if let Some(_value) = object.get(snake_key) {
        return optional_string(object, snake_key, ctx);
    }

    // Convert snake_case to camelCase and try.
    let camel_key = to_camel_case(snake_key);
    if let Some(_value) = object.get(&camel_key) {
        tracing::warn!("config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})");
        return optional_string(object, &camel_key, ctx);
    }

    Ok(None)
}

fn deep_merge_objects(
    target: &mut BTreeMap<String, JsonValue>,
    source: &BTreeMap<String, JsonValue>,
) {
    for (key, value) in source {
        match (target.get_mut(key), value) {
            (Some(JsonValue::Object(existing)), JsonValue::Object(incoming)) => {
                deep_merge_objects(existing, incoming);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

fn extend_unique(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        push_unique(target, value.clone());
    }
}

fn push_unique(target: &mut Vec<String>, value: String) {
    if !target.iter().any(|existing| existing == &value) {
        target.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        deep_merge_objects, parse_permission_mode_label, ConfigLoader, ConfigSource,
        McpServerConfig, McpTransport, ResolvedPermissionMode, RuntimeHookConfig,
        RuntimePluginConfig, COWD_SETTINGS_SCHEMA_NAME,
    };
    use crate::json::JsonValue;
    use crate::sandbox::FilesystemIsolationMode;
    use std::fs;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct EnvVarGuard {
        key: String,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &str, value: Option<&str>) -> Self {
            let original = std::env::var(key).ok();
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
            Self { key: key.to_string(), original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(val) => std::env::set_var(&self.key, val),
                None => std::env::remove_var(&self.key),
            }
        }
    }

    // Serialize tests that mutate environment variables to avoid race conditions.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn temp_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("runtime-config-{nanos}"))
    }

    #[test]
    fn rejects_non_object_settings_files() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(home.join("settings.json"), "[]").expect("write bad settings");

        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");
        assert!(error
            .to_string()
            .contains("top-level settings value must be a JSON object"));

        if root.exists() {
            fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }

    #[test]
    fn loads_and_merges_claude_code_config_files_by_precedence() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.parent().expect("home parent").join(".cowd/config.json"),
            r#"{"model":"haiku","env":{"A":"1"},"mcpServers":{"home":{"command":"uvx","args":["home"]}}}"#,
        )
        .expect("write user compat config");
        fs::write(
            home.join("settings.json"),
            r#"{"model":"sonnet","env":{"A2":"1"},"hooks":{"PreToolUse":["base"]},"permissions":{"defaultMode":"plan","allow":["Read"],"deny":["Bash(rm -rf)"]}}"#,
        )
        .expect("write user settings");
        fs::write(
            cwd.join(".cowd/config.json"),
            r#"{"model":"project-compat","env":{"B":"2"}}"#,
        )
        .expect("write project compat config");
        fs::write(
            cwd.join(".cowd").join("settings.json"),
            r#"{"env":{"C":"3"},"hooks":{"PostToolUse":["project"],"PostToolUseFailure":["project-failure"]},"permissions":{"ask":["Edit"]},"mcpServers":{"project":{"command":"uvx","args":["project"]}}}"#,
        )
        .expect("write project settings");
        fs::write(
            cwd.join(".cowd").join("settings.local.json"),
            r#"{"model":"opus","permissionMode":"acceptEdits"}"#,
        )
        .expect("write local settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(COWD_SETTINGS_SCHEMA_NAME, "SettingsSchema");
        assert_eq!(loaded.loaded_entries().len(), 5);
        assert_eq!(loaded.loaded_entries()[0].source, ConfigSource::User);
        assert_eq!(
            loaded.get("model"),
            Some(&JsonValue::String("opus".to_string()))
        );
        assert_eq!(loaded.model(), Some("opus"));
        assert_eq!(
            loaded.permission_mode(),
            Some(ResolvedPermissionMode::WorkspaceWrite)
        );
        assert_eq!(
            loaded
                .get("env")
                .and_then(JsonValue::as_object)
                .expect("env object")
                .len(),
            4
        );
        assert!(loaded
            .get("hooks")
            .and_then(JsonValue::as_object)
            .expect("hooks object")
            .contains_key("PreToolUse"));
        assert!(loaded
            .get("hooks")
            .and_then(JsonValue::as_object)
            .expect("hooks object")
            .contains_key("PostToolUse"));
        assert_eq!(loaded.hooks().pre_tool_use(), &["base".to_string()]);
        assert_eq!(loaded.hooks().post_tool_use(), &["project".to_string()]);
        assert_eq!(
            loaded.hooks().post_tool_use_failure(),
            &["project-failure".to_string()]
        );
        assert_eq!(loaded.permission_rules().allow(), &["Read".to_string()]);
        assert_eq!(
            loaded.permission_rules().deny(),
            &["Bash(rm -rf)".to_string()]
        );
        assert_eq!(loaded.permission_rules().ask(), &["Edit".to_string()]);
        assert!(loaded.mcp().get("home").is_some());
        assert!(loaded.mcp().get("project").is_some());

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_sandbox_config() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            cwd.join(".cowd").join("settings.local.json"),
            r#"{
              "sandbox": {
                "enabled": true,
                "namespaceRestrictions": false,
                "networkIsolation": true,
                "filesystemMode": "allow-list",
                "allowedMounts": ["logs", "tmp/cache"]
              }
            }"#,
        )
        .expect("write local settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(loaded.sandbox().enabled, Some(true));
        assert_eq!(loaded.sandbox().namespace_restrictions, Some(false));
        assert_eq!(loaded.sandbox().network_isolation, Some(true));
        assert_eq!(
            loaded.sandbox().filesystem_mode,
            Some(FilesystemIsolationMode::AllowList)
        );
        assert_eq!(loaded.sandbox().allowed_mounts, vec!["logs", "tmp/cache"]);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_provider_fallbacks_chain_with_primary_and_ordered_fallbacks() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");
        fs::write(
            home.join("settings.json"),
            r#"{
              "providerFallbacks": {
                "primary": "claude-opus-4-6",
                "fallbacks": ["grok-3", "grok-3-mini"]
              }
            }"#,
        )
        .expect("write provider fallback settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let chain = loaded.provider_fallbacks();
        assert_eq!(chain.primary(), Some("claude-opus-4-6"));
        assert_eq!(
            chain.fallbacks(),
            &["grok-3".to_string(), "grok-3-mini".to_string()]
        );
        assert!(!chain.is_empty());

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn provider_fallbacks_default_is_empty_when_unset() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(home.join("settings.json"), "{}").expect("write empty settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let chain = loaded.provider_fallbacks();
        assert_eq!(chain.primary(), None);
        assert!(chain.fallbacks().is_empty());
        assert!(chain.is_empty());

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_trusted_roots_from_settings() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("settings.json"),
            r#"{"trustedRoots": ["/tmp/worktrees", "/home/user/projects"]}"#,
        )
        .expect("write settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let roots = loaded.trusted_roots();
        assert_eq!(roots, ["/tmp/worktrees", "/home/user/projects"]);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn trusted_roots_default_is_empty_when_unset() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(home.join("settings.json"), "{}").expect("write empty settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        assert!(loaded.trusted_roots().is_empty());

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_typed_mcp_and_oauth_config() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("settings.json"),
            r#"{
              "mcpServers": {
                "stdio-server": {
                  "command": "uvx",
                  "args": ["mcp-server"],
                  "env": {"TOKEN": "secret"}
                },
                "remote-server": {
                  "type": "http",
                  "url": "https://example.test/mcp",
                  "headers": {"Authorization": "Bearer token"},
                  "headersHelper": "helper.sh",
                  "oauth": {
                    "clientId": "mcp-client",
                    "callbackPort": 7777,
                    "authServerMetadataUrl": "https://issuer.test/.well-known/oauth-authorization-server",
                    "xaa": true
                  }
                }
              },
              "oauth": {
                "clientId": "runtime-client",
                "authorizeUrl": "https://console.test/oauth/authorize",
                "tokenUrl": "https://console.test/oauth/token",
                "callbackPort": 54545,
                "manualRedirectUrl": "https://console.test/oauth/callback",
                "scopes": ["org:read", "user:write"]
              }
            }"#,
        )
        .expect("write user settings");
        fs::write(
            cwd.join(".cowd").join("settings.local.json"),
            r#"{
              "mcpServers": {
                "remote-server": {
                  "type": "ws",
                  "url": "wss://override.test/mcp",
                  "headers": {"X-Env": "local"}
                }
              }
            }"#,
        )
        .expect("write local settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        let stdio_server = loaded
            .mcp()
            .get("stdio-server")
            .expect("stdio server should exist");
        assert_eq!(stdio_server.scope, ConfigSource::User);
        assert_eq!(stdio_server.transport(), McpTransport::Stdio);

        let remote_server = loaded
            .mcp()
            .get("remote-server")
            .expect("remote server should exist");
        assert_eq!(remote_server.scope, ConfigSource::Local);
        assert_eq!(remote_server.transport(), McpTransport::Ws);
        match &remote_server.config {
            McpServerConfig::Ws(config) => {
                assert_eq!(config.url, "wss://override.test/mcp");
                assert_eq!(
                    config.headers.get("X-Env").map(String::as_str),
                    Some("local")
                );
            }
            other => panic!("expected ws config, got {other:?}"),
        }

        let oauth = loaded.oauth().expect("oauth config should exist");
        assert_eq!(oauth.client_id, "runtime-client");
        assert_eq!(oauth.callback_port, Some(54_545));
        assert_eq!(oauth.scopes, vec!["org:read", "user:write"]);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn infers_http_mcp_servers_from_url_only_config() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("settings.json"),
            r#"{
              "mcpServers": {
                "remote": {
                  "url": "https://example.test/mcp"
                }
              }
            }"#,
        )
        .expect("write mcp settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        let remote_server = loaded
            .mcp()
            .get("remote")
            .expect("remote server should exist");
        assert_eq!(remote_server.transport(), McpTransport::Http);
        match &remote_server.config {
            McpServerConfig::Http(config) => {
                assert_eq!(config.url, "https://example.test/mcp");
            }
            other => panic!("expected http config, got {other:?}"),
        }

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_plugin_config_from_enabled_plugins() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("settings.json"),
            r#"{
              "enabledPlugins": {
                "tool-guard@builtin": true,
                "sample-plugin@external": false
              }
            }"#,
        )
        .expect("write user settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(
            loaded.plugins().enabled_plugins().get("tool-guard@builtin"),
            Some(&true)
        );
        assert_eq!(
            loaded
                .plugins()
                .enabled_plugins()
                .get("sample-plugin@external"),
            Some(&false)
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_plugin_config() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("settings.json"),
            r#"{
              "enabledPlugins": {
                "core-helpers@builtin": true
              },
              "plugins": {
                "externalDirectories": ["./external-plugins"],
                "installRoot": "plugin-cache/installed",
                "registryPath": "plugin-cache/installed.json",
                "bundledRoot": "./bundled-plugins"
              }
            }"#,
        )
        .expect("write plugin settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(
            loaded
                .plugins()
                .enabled_plugins()
                .get("core-helpers@builtin"),
            Some(&true)
        );
        assert_eq!(
            loaded.plugins().external_directories(),
            &["./external-plugins".to_string()]
        );
        assert_eq!(
            loaded.plugins().install_root(),
            Some("plugin-cache/installed")
        );
        assert_eq!(
            loaded.plugins().registry_path(),
            Some("plugin-cache/installed.json")
        );
        assert_eq!(loaded.plugins().bundled_root(), Some("./bundled-plugins"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn rejects_invalid_mcp_server_shapes() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("settings.json"),
            r#"{"mcpServers":{"broken":{"type":"http","url":123}}}"#,
        )
        .expect("write broken settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");

        // then
        assert!(error
            .to_string()
            .contains("mcpServers.broken: missing string field url"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_user_defined_model_aliases_from_settings() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("settings.json"),
            r#"{"aliases":{"fast":"claude-haiku-4-5-20251213","smart":"claude-opus-4-6"}}"#,
        )
        .expect("write user settings");
        fs::write(
            cwd.join(".cowd").join("settings.local.json"),
            r#"{"aliases":{"smart":"claude-sonnet-4-6","cheap":"grok-3-mini"}}"#,
        )
        .expect("write local settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let aliases = loaded.aliases();
        assert_eq!(
            aliases.get("fast").map(String::as_str),
            Some("claude-haiku-4-5-20251213")
        );
        assert_eq!(
            aliases.get("smart").map(String::as_str),
            Some("claude-sonnet-4-6")
        );
        assert_eq!(
            aliases.get("cheap").map(String::as_str),
            Some("grok-3-mini")
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn empty_settings_file_loads_defaults() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(home.join("settings.json"), "").expect("write empty settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("empty settings should still load");

        // then
        assert_eq!(loaded.loaded_entries().len(), 1);
        assert_eq!(loaded.permission_mode(), None);
        assert_eq!(loaded.plugins().enabled_plugins().len(), 0);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn deep_merge_objects_merges_nested_maps() {
        // given
        let mut target = JsonValue::parse(r#"{"env":{"A":"1","B":"2"},"model":"haiku"}"#)
            .expect("target JSON should parse")
            .as_object()
            .expect("target should be an object")
            .clone();
        let source =
            JsonValue::parse(r#"{"env":{"B":"override","C":"3"},"sandbox":{"enabled":true}}"#)
                .expect("source JSON should parse")
                .as_object()
                .expect("source should be an object")
                .clone();

        // when
        deep_merge_objects(&mut target, &source);

        // then
        let env = target
            .get("env")
            .and_then(JsonValue::as_object)
            .expect("env should remain an object");
        assert_eq!(env.get("A"), Some(&JsonValue::String("1".to_string())));
        assert_eq!(
            env.get("B"),
            Some(&JsonValue::String("override".to_string()))
        );
        assert_eq!(env.get("C"), Some(&JsonValue::String("3".to_string())));
        assert!(target.contains_key("sandbox"));
    }

    #[test]
    fn rejects_invalid_hook_entries_before_merge() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let project_settings = cwd.join(".cowd").join("settings.json");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("settings.json"),
            r#"{"hooks":{"PreToolUse":["base"]}}"#,
        )
        .expect("write user settings");
        fs::write(
            &project_settings,
            r#"{"hooks":{"PreToolUse":["project",42]}}"#,
        )
        .expect("write invalid project settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");

        // then — config validation now catches the mixed array before the hooks parser
        let rendered = error.to_string();
        assert!(
            rendered.contains("hooks.PreToolUse")
                && rendered.contains("must be an array of strings"),
            "expected validation error for hooks.PreToolUse, got: {rendered}"
        );
        assert!(!rendered.contains("merged settings.hooks"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn permission_mode_aliases_resolve_to_expected_modes() {
        // given / when / then
        assert_eq!(
            parse_permission_mode_label("plan", "test").expect("plan should resolve"),
            ResolvedPermissionMode::ReadOnly
        );
        assert_eq!(
            parse_permission_mode_label("acceptEdits", "test").expect("acceptEdits should resolve"),
            ResolvedPermissionMode::WorkspaceWrite
        );
        assert_eq!(
            parse_permission_mode_label("dontAsk", "test").expect("dontAsk should resolve"),
            ResolvedPermissionMode::DangerFullAccess
        );
    }

    #[test]
    fn hook_config_merge_preserves_uniques() {
        // given
        let base = RuntimeHookConfig::new(
            vec!["pre-a".to_string()],
            vec!["post-a".to_string()],
            vec!["failure-a".to_string()],
        );
        let overlay = RuntimeHookConfig::new(
            vec!["pre-a".to_string(), "pre-b".to_string()],
            vec!["post-a".to_string(), "post-b".to_string()],
            vec!["failure-b".to_string()],
        );

        // when
        let merged = base.merged(&overlay);

        // then
        assert_eq!(
            merged.pre_tool_use(),
            &["pre-a".to_string(), "pre-b".to_string()]
        );
        assert_eq!(
            merged.post_tool_use(),
            &["post-a".to_string(), "post-b".to_string()]
        );
        assert_eq!(
            merged.post_tool_use_failure(),
            &["failure-a".to_string(), "failure-b".to_string()]
        );
    }

    #[test]
    fn plugin_state_falls_back_to_default_for_unknown_plugin() {
        // given
        let mut config = RuntimePluginConfig::default();
        config.set_plugin_state("known".to_string(), true);

        // when / then
        assert!(config.state_for("known", false));
        assert!(config.state_for("missing", true));
        assert!(!config.state_for("missing", false));
    }

    #[test]
    fn validates_unknown_top_level_keys_with_line_and_field_name() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let user_settings = home.join("settings.json");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            &user_settings,
            "{\n  \"model\": \"opus\",\n  \"telemetry\": true\n}\n",
        )
        .expect("write user settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");

        // then
        let rendered = error.to_string();
        assert!(
            rendered.contains(&user_settings.display().to_string()),
            "error should include file path, got: {rendered}"
        );
        assert!(
            rendered.contains("line 3"),
            "error should include line number, got: {rendered}"
        );
        assert!(
            rendered.contains("telemetry"),
            "error should name the offending field, got: {rendered}"
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn validates_deprecated_top_level_keys_with_replacement_guidance() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let user_settings = home.join("settings.json");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            &user_settings,
            "{\n  \"model\": \"opus\",\n  \"allowedTools\": [\"Read\"]\n}\n",
        )
        .expect("write user settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");

        // then
        let rendered = error.to_string();
        assert!(
            rendered.contains(&user_settings.display().to_string()),
            "error should include file path, got: {rendered}"
        );
        assert!(
            rendered.contains("line 3"),
            "error should include line number, got: {rendered}"
        );
        assert!(
            rendered.contains("allowedTools"),
            "error should call out the unknown field, got: {rendered}"
        );
        // allowedTools is an unknown key; validator should name it in the error
        assert!(
            rendered.contains("allowedTools"),
            "error should name the offending field, got: {rendered}"
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn validates_wrong_type_for_known_field_with_field_path() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let user_settings = home.join("settings.json");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            &user_settings,
            "{\n  \"hooks\": {\n    \"PreToolUse\": \"not-an-array\"\n  }\n}\n",
        )
        .expect("write user settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");

        // then
        let rendered = error.to_string();
        assert!(
            rendered.contains(&user_settings.display().to_string()),
            "error should include file path, got: {rendered}"
        );
        assert!(
            rendered.contains("hooks"),
            "error should include field path component 'hooks', got: {rendered}"
        );
        assert!(
            rendered.contains("PreToolUse"),
            "error should describe the type mismatch, got: {rendered}"
        );
        assert!(
            rendered.contains("array"),
            "error should describe the expected type, got: {rendered}"
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn unknown_top_level_key_suggests_closest_match() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let user_settings = home.join("settings.json");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(&user_settings, "{\n  \"modle\": \"opus\"\n}\n").expect("write user settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");

        // then
        let rendered = error.to_string();
        assert!(
            rendered.contains("modle"),
            "error should name the offending field, got: {rendered}"
        );
        assert!(
            rendered.contains("model"),
            "error should suggest the closest known key, got: {rendered}"
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn max_output_tokens_reads_from_environment_variable() {
        // given — set environment variable
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("4096"));

        // when
        let config = RuntimePluginConfig::default();

        // then
        assert_eq!(config.max_output_tokens(), Some(4096));
    }

    #[test]
    fn max_output_tokens_falls_back_to_none_when_env_var_is_unset() {
        // given — ensure env var is unset
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", None);

        // when
        let config = RuntimePluginConfig::default();

        // then
        assert_eq!(config.max_output_tokens(), None);
    }

    #[test]
    fn max_output_tokens_falls_back_to_none_when_env_var_is_invalid() {
        // given — set invalid environment variable
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("not-a-number"));

        // when
        let config = RuntimePluginConfig::default();

        // then — should fall back to None (not panic)
        assert_eq!(config.max_output_tokens(), None);
    }
}
