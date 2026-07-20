use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::json::JsonValue;
use crate::runtime_control::RuntimeControlPolicy;
use crate::sandbox::{FilesystemIsolationMode, SandboxConfig};
pub use model_protocol::oauth::OAuthConfig;
pub use model_protocol::provider_config::{ProviderConfig, ProviderProtocol, ProvidersConfig};

// ── Config Error Types ─────────────────────────────────────────────────

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigDiagnosticSeverity {
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDiagnostic {
    pub severity: ConfigDiagnosticSeverity,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigLoadResult {
    pub config: RuntimeConfig,
    pub diagnostics: Vec<ConfigDiagnostic>,
}

// ── Config Source (Precedence) ─────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigSource {
    User,
    Project,
    Local,
    Environment,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub source: ConfigSource,
    pub path: PathBuf,
    pub exists: bool,
}

// ── Approval & Permission Resolution ───────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalConfig {
    #[serde(default)]
    pub solo_mode: bool,
    #[serde(default = "default_true_bool")]
    pub solo_honor_critical: bool,
    #[serde(default = "default_true_bool")]
    pub auto_pass_read_only: bool,
    #[serde(default = "default_true_bool")]
    pub auto_pass_low_risk: bool,
}

impl Default for ApprovalConfig {
    fn default() -> Self {
        Self {
            solo_mode: false,
            solo_honor_critical: true,
            auto_pass_read_only: true,
            auto_pass_low_risk: true,
        }
    }
}

impl ApprovalConfig {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_solo_mode(mut self, enabled: bool) -> Self {
        self.solo_mode = enabled;
        self
    }
    pub fn with_solo_honor_critical(mut self, honor: bool) -> Self {
        self.solo_honor_critical = honor;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResolvedPermissionMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

// ── MCP & OAuth Types ──────────────────────────────────────────────────

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

// ── Runtime Config Types ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimeHookConfig {
    #[serde(default)]
    pub pre_tool_use: Vec<String>,
    #[serde(default)]
    pub post_tool_use: Vec<String>,
    #[serde(default)]
    pub post_tool_use_failure: Vec<String>,
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
        let mut pre_set: std::collections::HashSet<String> =
            self.pre_tool_use.iter().cloned().collect();
        for item in &other.pre_tool_use {
            if pre_set.insert(item.clone()) {
                self.pre_tool_use.push(item.clone());
            }
        }
        let mut post_set: std::collections::HashSet<String> =
            self.post_tool_use.iter().cloned().collect();
        for item in &other.post_tool_use {
            if post_set.insert(item.clone()) {
                self.post_tool_use.push(item.clone());
            }
        }
        let mut fail_set: std::collections::HashSet<String> =
            self.post_tool_use_failure.iter().cloned().collect();
        for item in &other.post_tool_use_failure {
            if fail_set.insert(item.clone()) {
                self.post_tool_use_failure.push(item.clone());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimePermissionRuleConfig {
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub ask: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePluginConfig {
    #[serde(default)]
    pub enabled_plugins: BTreeMap<String, bool>,
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
    #[must_use]
    pub fn state_for(&self, plugin_id: &str, default_enabled: bool) -> bool {
        self.enabled_plugins
            .get(plugin_id)
            .copied()
            .unwrap_or(default_enabled)
    }
}

// ── Session Reset Policy ───────────────────────────────────────────────

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

// ── Layer Configuration ────────────────────────────────────────────────

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

fn default_l1_max_tokens() -> u32 {
    2000
}
fn default_l2_max_tokens() -> u32 {
    3000
}
fn default_l3_search_limit() -> u32 {
    5
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

// ── Vector Configuration ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub api_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub dimension: usize,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_timeout() -> u64 {
    30
}
fn default_batch_size() -> usize {
    32
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

// ── Compression Sub-Configuration ──────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MicroCompactConfig {
    #[serde(default = "default_true_bool")]
    pub enabled: bool,
    #[serde(default = "default_decay_factor")]
    pub time_decay_factor: f32,
}

fn default_decay_factor() -> f32 {
    0.9
}

impl Eq for MicroCompactConfig {}
impl Default for MicroCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            time_decay_factor: 0.9,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionCompactConfig {
    #[serde(default = "default_preserve_recent")]
    pub preserve_recent: u32,
    #[serde(default = "default_summary_max")]
    pub summary_max_tokens: u32,
}

fn default_preserve_recent() -> u32 {
    6
}
fn default_summary_max() -> u32 {
    2000
}

impl Default for SessionCompactConfig {
    fn default() -> Self {
        Self {
            preserve_recent: 6,
            summary_max_tokens: 2000,
        }
    }
}

/// Budget that Runtime may distribute to internal subsystems. It is explicitly
/// separate from provider request capacity and session compaction decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextBudgetConfig {
    #[serde(default = "default_subsystem_budget_ratio_bp")]
    pub subsystem_budget_ratio_bp: u32,
}

const fn default_subsystem_budget_ratio_bp() -> u32 {
    7000
}

impl Default for ContextBudgetConfig {
    fn default() -> Self {
        Self {
            subsystem_budget_ratio_bp: default_subsystem_budget_ratio_bp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeepCompactConfig {
    #[serde(default = "default_true_bool")]
    pub enabled: bool,
    #[serde(default = "default_true_bool")]
    pub iterative_update: bool,
}

impl Default for DeepCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            iterative_update: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_max_retries_3")]
    pub max_retries: u32,
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u32,
}

fn default_max_retries_3() -> u32 {
    3
}
fn default_cooldown_secs() -> u32 {
    30
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            cooldown_secs: 30,
        }
    }
}

// ── Compression Config ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CompressionConfig {
    #[serde(default)]
    pub micro: MicroCompactConfig,
    #[serde(default)]
    pub session: SessionCompactConfig,
    #[serde(default)]
    pub deep: DeepCompactConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub llm: LlmSummarizerConfig,
}

fn default_true_bool() -> bool {
    true
}

impl Eq for CompressionConfig {}

// ── LLM Summarizer Config ──────────────────────────────────────────────

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
    pub fn is_configured(&self) -> bool {
        self.enabled && !self.api_url.is_empty() && !self.api_key.is_empty()
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

/// Structured feature configuration consumed by runtime subsystems.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeFeatureConfig {
    hooks: RuntimeHookConfig,
    plugins: RuntimePluginConfig,
    mcp: McpConfigCollection,
    oauth: Option<OAuthConfig>,
    model: Option<String>,
    aliases: BTreeMap<String, String>,
    model_context_windows: BTreeMap<String, u32>,
    permission_mode: Option<ResolvedPermissionMode>,
    permission_rules: RuntimePermissionRuleConfig,
    approval: ApprovalConfig,
    sandbox: SandboxConfig,
    fallbacks: Vec<String>,
    providers: ProvidersConfig,
    trusted_roots: Vec<String>,
    memory: MemoryConfig,
    context_budget: ContextBudgetConfig,
    compression: CompressionConfig,
    gateway: GatewayConfig,
    gate_auto_fix: GateAutoFixConfig,
    runtime_control: RuntimeControlConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DomainProfile {
    #[default]
    Coding,
    Research,
    Office,
    Ops,
    Personal,
}

impl DomainProfile {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Coding => "coding",
            Self::Research => "research",
            Self::Office => "office",
            Self::Ops => "ops",
            Self::Personal => "personal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeControlConfig {
    pub scenario: DomainProfile,
    pub policy: RuntimeControlPolicy,
}

impl Default for RuntimeControlConfig {
    fn default() -> Self {
        Self {
            scenario: DomainProfile::Coding,
            policy: RuntimeControlPolicy::default(),
        }
    }
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
    pub store_enable_vector_index: bool,
    pub runtime: MemoryRuntimeConfig,
    pub layers: LayerConfig,
    pub extraction: ExtractionConfig,
    pub vector: VectorConfig,
    /// Jaccard similarity threshold for coherence filtering in basis points.
    /// 100 = 0.01, 1000 = 0.10 (default), 5000 = 0.50.
    /// Entries with score below this are excluded from context injection.
    pub coherence_threshold_bp: u32,
}

/// Runtime-owned memory execution switches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRuntimeConfig {
    pub use_runtime_budget: bool,
    pub semantic_checkpoint_enabled: bool,
    pub recall_checkpoint_limit: u32,
}

/// Controls automatic memory extraction behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionConfig {
    pub auto_extract: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            store_path: None,
            store_enable_vector_index: true,
            runtime: MemoryRuntimeConfig::default(),
            layers: LayerConfig::default(),
            extraction: ExtractionConfig::default(),
            vector: VectorConfig::default(),
            coherence_threshold_bp: 1000,
        }
    }
}

impl Default for MemoryRuntimeConfig {
    fn default() -> Self {
        Self {
            use_runtime_budget: true,
            semantic_checkpoint_enabled: true,
            recall_checkpoint_limit: 3,
        }
    }
}

impl Default for ExtractionConfig {
    fn default() -> Self {
        Self { auto_extract: true }
    }
}

// ---- Compression configuration ----
// (CompressionConfig and sub-types re-exported from config crate)

// ---- Gateway configuration ----

/// Multi-platform gateway configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GatewayConfig {
    pub enabled: bool,
    pub webui_dir: Option<PathBuf>,
    pub platforms: Vec<PlatformConfig>,
    pub session_reset: SessionResetPolicy,
    pub capacity: GatewayCapacityConfig,
}

/// Gateway 容量 override。`None` 使用基于逻辑 CPU 的受控默认值；所有值
/// 只从统一配置树读取，不接受分散环境变量覆盖。
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GatewayCapacityConfig {
    pub runtime_workers: Option<usize>,
    pub control_requests: Option<usize>,
    pub data_requests: Option<usize>,
    pub stream_connections: Option<usize>,
    pub blocking_requests: Option<usize>,
    pub queue_timeout_ms: Option<u64>,
}

/// Configuration for a single inbound platform adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformConfig {
    /// Discriminator: `"api_server"`, `"email"`, `"chat"`, `"wecom"`, etc.
    pub platform_type: String,
    pub enabled: bool,
    /// Platform-specific JSON blob (opaque to the runtime core).
    pub extra: BTreeMap<String, JsonValue>,
}

/// Configuration for gate auto-fix behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateAutoFixConfig {
    pub enabled: bool,
    pub max_attempts: usize,
}

impl Default for GateAutoFixConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
        }
    }
}

/// Resolve the default cowd config home directory.
#[must_use]
pub fn default_config_home() -> PathBuf {
    if let Some(path) = std::env::var_os("COWD_CONFIG_HOME") {
        return PathBuf::from(path);
    }
    std::env::var_os("HOME")
        .map(|home| PathBuf::from(home).join(".cowd"))
        .unwrap_or_else(|| PathBuf::from(".cowd"))
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
        let cc_user_dir = &self.config_home;
        let entry = |source, path: PathBuf| ConfigEntry {
            exists: path.exists(),
            source,
            path,
        };

        vec![
            // ── User-level: ~/.cc paths ──────────────────────────────────────
            entry(ConfigSource::User, cc_user_dir.join("config.yaml")),
            entry(ConfigSource::User, cc_user_dir.join("config.yml")),
            // ── Project-level: .cowd/ paths ──────────────────────────────────
            entry(
                ConfigSource::Project,
                self.cwd.join(".cowd").join("config.yaml"),
            ),
            entry(
                ConfigSource::Project,
                self.cwd.join(".cowd").join("config.yml"),
            ),
            // ── Local overrides: highest priority ────────────────────────────
            entry(
                ConfigSource::Local,
                self.cwd.join(".cowd").join("config.local.yaml"),
            ),
            entry(
                ConfigSource::Local,
                self.cwd.join(".cowd").join("config.local.yml"),
            ),
        ]
    }

    pub fn load(&self) -> Result<RuntimeConfig, ConfigError> {
        self.load_with_diagnostics().map(|result| result.config)
    }

    pub fn load_with_diagnostics(&self) -> Result<ConfigLoadResult, ConfigError> {
        let mut merged = BTreeMap::new();
        let mut loaded_entries = Vec::new();
        let mut mcp_servers = BTreeMap::new();
        let mut all_warnings = Vec::new();

        for entry in self.discover() {
            crate::config_validate::check_unsupported_format(&entry.path)?;
            let parsed_opt = read_optional_yaml_object(&entry.path)?;
            let Some(parsed) = parsed_opt else {
                continue;
            };
            // Validate schema
            {
                let validation = crate::config_validate::validate_config_file(
                    &parsed.object,
                    &parsed.source,
                    &entry.path,
                );
                if !validation.is_ok() {
                    let errors = validation
                        .errors
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join("\n");
                    return Err(ConfigError::Parse(errors));
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

        let mut diagnostics = all_warnings
            .iter()
            .map(|warning| {
                tracing::warn!("{warning}");
                ConfigDiagnostic {
                    severity: ConfigDiagnosticSeverity::Warning,
                    code: "config_validation_warning".to_string(),
                    message: warning.to_string(),
                }
            })
            .collect::<Vec<_>>();

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
            model_context_windows: parse_optional_model_context_windows(&merged_value)?,
            permission_mode: parse_optional_permission_mode(&merged_value)?,
            permission_rules: parse_optional_permission_rules(&merged_value)?,
            approval: parse_optional_approval_config(&merged_value)?,
            sandbox: parse_optional_sandbox_config(&merged_value)?,
            fallbacks: parse_fallbacks(&merged_value, &mut diagnostics),
            providers: parse_optional_providers_config(&merged_value)?,
            trusted_roots: parse_optional_trusted_roots(&merged_value)?,
            memory: parse_optional_memory_config(&merged_value)?,
            context_budget: parse_optional_context_budget_config(&merged_value)?,
            compression: parse_optional_compression_config(&merged_value)?,
            gateway: parse_optional_gateway_config(&merged_value)?,
            gate_auto_fix: parse_optional_gate_auto_fix_config(&merged_value)?,
            runtime_control: parse_optional_runtime_control_config(&merged_value)?,
        };

        Ok(ConfigLoadResult {
            config: RuntimeConfig {
                merged,
                loaded_entries,
                feature_config,
            },
            diagnostics,
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

    /// Return the merged configuration with all credential-bearing branches
    /// removed. This is the single redaction implementation for user-visible
    /// diagnostics; prompt construction must never consume it.
    #[must_use]
    pub fn redacted_json(&self) -> JsonValue {
        redact_json_value(self.as_json())
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
    pub fn model_context_windows(&self) -> &BTreeMap<String, u32> {
        &self.feature_config.model_context_windows
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
    pub fn fallbacks(&self) -> &[String] {
        &self.feature_config.fallbacks
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
    pub fn context_budget(&self) -> &ContextBudgetConfig {
        &self.feature_config.context_budget
    }

    #[must_use]
    pub fn compression(&self) -> &CompressionConfig {
        &self.feature_config.compression
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayConfig {
        &self.feature_config.gateway
    }

    #[must_use]
    pub fn gate_auto_fix(&self) -> &GateAutoFixConfig {
        &self.feature_config.gate_auto_fix
    }

    #[must_use]
    pub fn runtime_control(&self) -> &RuntimeControlConfig {
        &self.feature_config.runtime_control
    }
}

/// Redact a serde JSON projection with the same rules as [`RuntimeConfig`].
/// Gateway uses this only while shaping already-authorized user-facing API
/// responses. Keeping the traversal here prevents a second, weaker secret
/// filter from drifting in an outer layer.
#[must_use]
pub fn redact_serde_json(mut value: serde_json::Value) -> serde_json::Value {
    redact_serde_json_in_place(&mut value);
    value
}

fn redact_json_value(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(entries) => JsonValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| {
                    if is_sensitive_config_key(&key) {
                        (key, JsonValue::String("[redacted]".to_string()))
                    } else {
                        (key, redact_json_value(value))
                    }
                })
                .collect(),
        ),
        JsonValue::Array(values) => {
            JsonValue::Array(values.into_iter().map(redact_json_value).collect())
        }
        value => value,
    }
}

fn redact_serde_json_in_place(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(entries) => {
            for (key, child) in entries.iter_mut() {
                if is_sensitive_config_key(key) {
                    *child = serde_json::Value::String("[redacted]".to_string());
                } else {
                    redact_serde_json_in_place(child);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_serde_json_in_place(value);
            }
        }
        _ => {}
    }
}

fn is_sensitive_config_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    normalized.contains("apikey")
        || normalized == "token"
        || normalized.ends_with("token")
        || normalized == "secret"
        || normalized.ends_with("secret")
        || normalized == "password"
        || normalized.ends_with("password")
        || normalized == "authorization"
        || normalized.ends_with("authorization")
        || normalized == "headers"
        || normalized.ends_with("headers")
        || normalized == "env"
        || normalized.ends_with("env")
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
    pub fn with_runtime_control(mut self, runtime_control: RuntimeControlConfig) -> Self {
        self.runtime_control = runtime_control;
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
    pub fn model_context_windows(&self) -> &BTreeMap<String, u32> {
        &self.model_context_windows
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
    pub fn fallbacks(&self) -> &[String] {
        &self.fallbacks
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
    pub fn context_budget(&self) -> &ContextBudgetConfig {
        &self.context_budget
    }

    #[must_use]
    pub fn compression(&self) -> &CompressionConfig {
        &self.compression
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayConfig {
        &self.gateway
    }

    #[must_use]
    pub fn gate_auto_fix(&self) -> &GateAutoFixConfig {
        &self.gate_auto_fix
    }

    #[must_use]
    pub fn runtime_control(&self) -> &RuntimeControlConfig {
        &self.runtime_control
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

/// Parsed config object paired with its raw source text for validation.
struct ParsedConfigFile {
    object: BTreeMap<String, JsonValue>,
    source: String,
}

/// Convert a `serde_yaml::Value` into the project-internal `JsonValue`.
/// Returns `None` for YAML types that have no YAML equivalent (e.g. tagged values).
fn yaml_to_json(value: serde_yaml::Value) -> Option<JsonValue> {
    match value {
        serde_yaml::Value::Null => Some(JsonValue::Null),
        serde_yaml::Value::Bool(b) => Some(JsonValue::Bool(b)),
        serde_yaml::Value::Number(n) => {
            // Prefer integer representation; fall back to rounded float.
            if let Some(i) = n.as_i64() {
                Some(JsonValue::Number(i))
            } else {
                n.as_f64().map(|f| JsonValue::Number(f as i64))
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
            "{}: top-level config value must be an object",
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
/// - `CC_CONTEXT_BUDGET_SUBSYSTEM_BUDGET_RATIO_BP=7000` → `{"context": {"budget": {"subsystem": {"budget": {"ratio": {"bp": 7000}}}}}}`
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

fn parse_optional_model_context_windows(
    root: &JsonValue,
) -> Result<BTreeMap<String, u32>, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(BTreeMap::new());
    };
    let Some(val) = object.get("model_context_windows") else {
        return Ok(BTreeMap::new());
    };
    let map = val.as_object().ok_or_else(|| {
        ConfigError::Parse(
            "merged settings: field model_context_windows must be an object".to_string(),
        )
    })?;
    let mut result = BTreeMap::new();
    for (k, v) in map {
        let num: i64 = v.as_i64().ok_or_else(|| {
            ConfigError::Parse(format!(
                "merged settings: field model_context_windows.{k} must be a number"
            ))
        })?;
        let n: u32 = num.try_into().map_err(|_| {
            ConfigError::Parse(format!(
                "merged settings: field model_context_windows.{k} value out of u32 range"
            ))
        })?;
        if n < 1_024 {
            return Err(ConfigError::Parse(format!(
                "merged settings: field model_context_windows.{k} must be at least 1024"
            )));
        }
        result.insert(k.clone(), n);
    }
    Ok(result)
}

fn parse_optional_context_budget_config(
    root: &JsonValue,
) -> Result<ContextBudgetConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(ContextBudgetConfig::default());
    };
    let Some(value) = object.get("context_budget") else {
        return Ok(ContextBudgetConfig::default());
    };
    let budget = expect_object(value, "merged settings.context_budget")?;
    let ratio = optional_u32_dual(
        budget,
        "subsystem_budget_ratio_bp",
        "merged settings.context_budget",
    )?
    .unwrap_or(ContextBudgetConfig::default().subsystem_budget_ratio_bp);
    if !(1_000..=9_500).contains(&ratio) {
        return Err(ConfigError::Parse(
            "merged settings.context_budget.subsystem_budget_ratio_bp must be between 1000 and 9500"
                .to_string(),
        ));
    }
    Ok(ContextBudgetConfig {
        subsystem_budget_ratio_bp: ratio,
    })
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
        pre_tool_use: find_key_dual(hooks, "pre_tool_use", context)
            .map(|v| parse_json_string_array(v, "pre_tool_use", context))
            .transpose()?
            .unwrap_or_default(),
        post_tool_use: find_key_dual(hooks, "post_tool_use", context)
            .map(|v| parse_json_string_array(v, "post_tool_use", context))
            .transpose()?
            .unwrap_or_default(),
        post_tool_use_failure: find_key_dual(hooks, "post_tool_use_failure", context)
            .map(|v| parse_json_string_array(v, "post_tool_use_failure", context))
            .transpose()?
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
    let approval = object
        .get("approval")
        .and_then(JsonValue::as_object)
        .or_else(|| {
            object
                .get("permissions")
                .and_then(JsonValue::as_object)
                .and_then(|permissions| permissions.get("approval"))
                .and_then(JsonValue::as_object)
        });
    let Some(approval) = approval else {
        return Ok(ApprovalConfig::default());
    };

    Ok(ApprovalConfig {
        solo_mode: optional_bool(
            approval,
            "solo_mode",
            "merged settings.permissions.approval",
        )?
        .unwrap_or(false),
        solo_honor_critical: optional_bool(
            approval,
            "solo_honor_critical",
            "merged settings.permissions.approval",
        )?
        .unwrap_or(true),
        auto_pass_read_only: optional_bool(
            approval,
            "auto_pass_read_only",
            "merged settings.permissions.approval",
        )?
        .unwrap_or(true),
        auto_pass_low_risk: optional_bool(
            approval,
            "auto_pass_low_risk",
            "merged settings.permissions.approval",
        )?
        .unwrap_or(true),
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
        optional_string_array_dual(plugins, "external_directories", "merged settings.plugins")?
            .map(|v| {
                v.into_iter()
                    .map(|s| crate::cowd_dirs::expand_tilde(&s).display().to_string())
                    .collect()
            })
            .unwrap_or_default();
    config.install_root = optional_string_dual(plugins, "install_root", "merged settings.plugins")?
        .map(|s| crate::cowd_dirs::expand_tilde(s).display().to_string());
    config.registry_path =
        optional_string_dual(plugins, "registry_path", "merged settings.plugins")?
            .map(|s| crate::cowd_dirs::expand_tilde(s).display().to_string());
    config.bundled_root = optional_string_dual(plugins, "bundled_root", "merged settings.plugins")?
        .map(|s| crate::cowd_dirs::expand_tilde(s).display().to_string());
    config.max_output_tokens = optional_u32(plugins, "maxOutputTokens", "merged settings.plugins")?
        .or_else(|| {
            std::env::var("COWD_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
        });
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
        .and_then(|permissions| {
            permissions
                .get("defaultMode")
                .or_else(|| permissions.get("default_mode"))
        })
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
    let filesystem_mode =
        optional_string_dual(sandbox, "filesystem_mode", "merged settings.sandbox")?
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
        allowed_mounts: optional_string_array_dual(
            sandbox,
            "allowed_mounts",
            "merged settings.sandbox",
        )?
        .map(|v| {
            v.into_iter()
                .map(|s| crate::cowd_dirs::expand_tilde(&s).display().to_string())
                .collect()
        })
        .unwrap_or_default(),
    })
}

fn parse_fallbacks(root: &JsonValue, diagnostics: &mut Vec<ConfigDiagnostic>) -> Vec<String> {
    let Some(object) = root.as_object() else {
        return vec![];
    };
    if let Some(arr) = object.get("fallbacks").and_then(|v| v.as_array()) {
        return arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(str::to_string)
            .collect();
    }
    if let Some(v) = find_key_dual(object, "provider_fallbacks", "merged settings") {
        let msg = "'providerFallbacks' is deprecated, use 'fallbacks' instead. All per-model chains are now merged into a single global list.".to_string();
        tracing::warn!("{msg}");
        diagnostics.push(ConfigDiagnostic {
            severity: ConfigDiagnosticSeverity::Warning,
            code: "deprecated_provider_fallbacks".to_string(),
            message: msg,
        });
        return extract_fallbacks_from_legacy(v);
    }
    vec![]
}

fn extract_fallbacks_from_legacy(value: &JsonValue) -> Vec<String> {
    let mut models = vec![];
    let mut process = |entry: &BTreeMap<String, JsonValue>| {
        if let Some(fbs) = entry.get("fallbacks").and_then(|v| v.as_array()) {
            models.extend(fbs.iter().filter_map(|v| v.as_str()).map(str::to_string));
        }
    };
    match value {
        JsonValue::Array(items) => {
            for item in items {
                if let JsonValue::Object(ref entry) = item {
                    process(entry);
                }
            }
        }
        JsonValue::Object(ref entry) => process(entry),
        _ => {}
    }
    models
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
        let models = optional_string_array(entry, "models", &ctx)?.unwrap_or_default();
        let protocol = optional_string_dual(entry, "protocol", &ctx)?.map(str::to_string);

        if let Some(ref p) = protocol {
            if ProviderProtocol::parse(p).is_none() {
                return Err(ConfigError::Invalid {
                    key: format!("providers.{name}.protocol"),
                    message: format!(
                        "unsupported protocol '{p}'. Valid values: \"anthropic\", \"completions\", \"responses\""
                    ),
                });
            }
        }

        providers.insert(
            name.clone(),
            ProviderConfig {
                name: name.clone(),
                base_url,
                api_key,
                models,
                protocol,
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
        optional_string_array_dual(object, "trusted_roots", "merged settings")?
            .map(|v| {
                v.into_iter()
                    .map(|s| crate::cowd_dirs::expand_tilde(&s).display().to_string())
                    .collect()
            })
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
        .map(crate::cowd_dirs::expand_tilde);
    let store_enable_vector_index = if let Some(store_val) = mem.get("store") {
        let store = expect_object(store_val, "merged settings.memory.store")?;
        optional_bool_dual(store, "enable_vector_index", "merged settings.memory.store")?
            .unwrap_or(MemoryConfig::default().store_enable_vector_index)
    } else {
        MemoryConfig::default().store_enable_vector_index
    };
    let runtime = if let Some(runtime_val) = mem.get("runtime") {
        let r = expect_object(runtime_val, "merged settings.memory.runtime")?;
        MemoryRuntimeConfig {
            use_runtime_budget: optional_bool_dual(
                r,
                "use_runtime_budget",
                "merged settings.memory.runtime",
            )?
            .unwrap_or(MemoryRuntimeConfig::default().use_runtime_budget),
            semantic_checkpoint_enabled: optional_bool_dual(
                r,
                "semantic_checkpoint_enabled",
                "merged settings.memory.runtime",
            )?
            .unwrap_or(MemoryRuntimeConfig::default().semantic_checkpoint_enabled),
            recall_checkpoint_limit: optional_u32_dual(
                r,
                "recall_checkpoint_limit",
                "merged settings.memory.runtime",
            )?
            .unwrap_or(MemoryRuntimeConfig::default().recall_checkpoint_limit),
        }
    } else {
        MemoryRuntimeConfig::default()
    };
    let layers = if let Some(layers_val) = mem.get("layers") {
        let l = expect_object(layers_val, "merged settings.memory.layers")?;
        LayerConfig {
            l0_enabled: optional_bool_dual(l, "l0_enabled", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l0_enabled),
            l1_max_tokens: optional_u32_dual(l, "l1_max_tokens", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l1_max_tokens),
            l2_max_tokens: optional_u32_dual(l, "l2_max_tokens", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l2_max_tokens),
            l3_search_limit: optional_u32_dual(
                l,
                "l3_search_limit",
                "merged settings.memory.layers",
            )?
            .unwrap_or(LayerConfig::default().l3_search_limit),
            l4_enabled: optional_bool_dual(l, "l4_enabled", "merged settings.memory.layers")?
                .unwrap_or(LayerConfig::default().l4_enabled),
        }
    } else {
        LayerConfig::default()
    };
    let extraction = if let Some(ext_val) = mem.get("extraction").or_else(|| mem.get("extractor")) {
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
        let model_name = optional_string_dual(v, "model", "merged settings.memory.vector")?.or(
            optional_string_dual(v, "embedding_model", "merged settings.memory.vector")?,
        );
        let dimension = optional_usize(v, "dimension", "merged settings.memory.vector")?;
        let api_url = optional_string_dual(v, "api_url", "merged settings.memory.vector")?;
        let api_key = optional_string_dual(v, "api_key", "merged settings.memory.vector")?;
        let timeout_secs = optional_u64(v, "timeout_secs", "merged settings.memory.vector")?.or(
            optional_u64(v, "timeoutSecs", "merged settings.memory.vector")?,
        );
        let batch_size = optional_usize(v, "batch_size", "merged settings.memory.vector")?.or(
            optional_usize(v, "batchSize", "merged settings.memory.vector")?,
        );

        // Environment variable overrides.
        let resolved_model = std::env::var("COWD_MEMORY_VECTOR_MODEL")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| model_name.map(str::to_string))
            .unwrap_or(defaults.model.clone());
        let resolved_api_url = std::env::var("COWD_MEMORY_VECTOR_API_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| api_url.map(str::to_string))
            .unwrap_or(defaults.api_url.clone());
        let resolved_api_key = std::env::var("COWD_VECTOR_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| api_key.map(str::to_string))
            .unwrap_or(defaults.api_key.clone());

        VectorConfig {
            enabled: enabled.unwrap_or(defaults.enabled),
            model: resolved_model,
            dimension: dimension.unwrap_or(defaults.dimension),
            api_url: resolved_api_url,
            api_key: resolved_api_key,
            timeout_secs: timeout_secs.unwrap_or(defaults.timeout_secs),
            batch_size: batch_size.unwrap_or(defaults.batch_size),
        }
    } else {
        // No vector section; still apply env var overrides.
        let defaults = VectorConfig::default();
        let model = std::env::var("COWD_MEMORY_VECTOR_MODEL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(defaults.model.clone());
        let api_url = std::env::var("COWD_MEMORY_VECTOR_API_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(defaults.api_url.clone());
        let api_key = std::env::var("COWD_VECTOR_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(defaults.api_key);
        VectorConfig {
            model,
            api_url,
            api_key,
            ..defaults
        }
    };
    Ok(MemoryConfig {
        enabled: enabled.unwrap_or(MemoryConfig::default().enabled),
        store_path,
        store_enable_vector_index,
        runtime,
        layers,
        extraction,
        vector,
        coherence_threshold_bp: optional_u32_dual(
            mem,
            "coherence_threshold_bp",
            "merged settings.memory",
        )?
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
            enabled: optional_bool_dual(m, "enabled", "merged settings.compression.micro")?
                .unwrap_or(MicroCompactConfig::default().enabled),
            time_decay_factor: optional_f32_dual(
                m,
                "time_decay_factor",
                "merged settings.compression.micro",
            )?
            .unwrap_or(MicroCompactConfig::default().time_decay_factor),
        }
    } else {
        MicroCompactConfig::default()
    };
    let session = if let Some(sess_val) = cmp.get("session") {
        let s = expect_object(sess_val, "merged settings.compression.session")?;
        for removed in ["threshold_tokens", "threshold_ratio_bp", "buffer_tokens"] {
            if s.contains_key(removed) {
                return Err(ConfigError::Parse(format!(
                    "merged settings.compression.session.{removed} was removed; Runtime now compacts from candidate request pressure"
                )));
            }
        }
        SessionCompactConfig {
            preserve_recent: optional_u32_dual(
                s,
                "preserve_recent",
                "merged settings.compression.session",
            )?
            .unwrap_or(SessionCompactConfig::default().preserve_recent),
            summary_max_tokens: optional_u32_dual(
                s,
                "summary_max_tokens",
                "merged settings.compression.session",
            )?
            .unwrap_or(SessionCompactConfig::default().summary_max_tokens),
        }
    } else {
        SessionCompactConfig::default()
    };
    let deep = if let Some(deep_val) = cmp.get("deep") {
        let d = expect_object(deep_val, "merged settings.compression.deep")?;
        DeepCompactConfig {
            enabled: optional_bool_dual(d, "enabled", "merged settings.compression.deep")?
                .unwrap_or(DeepCompactConfig::default().enabled),
            iterative_update: optional_bool_dual(
                d,
                "iterative_update",
                "merged settings.compression.deep",
            )?
            .unwrap_or(DeepCompactConfig::default().iterative_update),
        }
    } else {
        DeepCompactConfig::default()
    };
    let circuit_breaker = if let Some(cb_val) =
        find_key_dual(cmp, "circuit_breaker", "merged settings.compression")
    {
        let cb = expect_object(cb_val, "merged settings.compression.circuitBreaker")?;
        CircuitBreakerConfig {
            max_retries: optional_u32_dual(
                cb,
                "max_retries",
                "merged settings.compression.circuitBreaker",
            )?
            .unwrap_or(CircuitBreakerConfig::default().max_retries),
            cooldown_secs: optional_u32_dual(
                cb,
                "cooldown_secs",
                "merged settings.compression.circuitBreaker",
            )?
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
        ..CompressionConfig::default()
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
    let webui_dir =
        optional_string_dual(gw, "webui_dir", "merged settings.gateway")?.map(PathBuf::from);
    let platforms = if let Some(plat_val) = gw.get("platforms") {
        let arr = expect_array(plat_val, "merged settings.gateway.platforms")?;
        arr.iter()
            .enumerate()
            .map(|(i, v)| {
                let ctx = format!("merged settings.gateway.platforms[{i}]");
                let p = expect_object(v, &ctx)?;
                Ok(PlatformConfig {
                    platform_type: expect_string(p, "platformType", &ctx)
                        .or_else(|_| expect_string(p, "platform_type", &ctx))
                        .or_else(|_| expect_string(p, "type", &ctx)) // fallback: "type" key
                        .map(|s| s.to_string())?,
                    enabled: optional_bool(p, "enabled", &ctx)?.unwrap_or(true),
                    extra: p
                        .iter()
                        .filter(|(k, _)| {
                            !matches!(
                                k.as_str(),
                                "platformType" | "platform_type" | "type" | "enabled"
                            )
                        })
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?
    } else {
        Vec::new()
    };
    let capacity = if let Some(value) = gw.get("capacity") {
        let value = expect_object(value, "merged settings.gateway.capacity")?;
        GatewayCapacityConfig {
            runtime_workers: optional_usize(
                value,
                "runtime_workers",
                "merged settings.gateway.capacity",
            )?,
            control_requests: optional_usize(
                value,
                "control_requests",
                "merged settings.gateway.capacity",
            )?,
            data_requests: optional_usize(
                value,
                "data_requests",
                "merged settings.gateway.capacity",
            )?,
            stream_connections: optional_usize(
                value,
                "stream_connections",
                "merged settings.gateway.capacity",
            )?,
            blocking_requests: optional_usize(
                value,
                "blocking_requests",
                "merged settings.gateway.capacity",
            )?,
            queue_timeout_ms: optional_u64(
                value,
                "queue_timeout_ms",
                "merged settings.gateway.capacity",
            )?,
        }
    } else {
        GatewayCapacityConfig::default()
    };
    Ok(GatewayConfig {
        enabled,
        webui_dir,
        platforms,
        session_reset,
        capacity,
    })
}

fn parse_optional_gate_auto_fix_config(root: &JsonValue) -> Result<GateAutoFixConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(GateAutoFixConfig::default());
    };
    let Some(gv) = object.get("gateAutoFix") else {
        return Ok(GateAutoFixConfig::default());
    };
    let cfg = expect_object(gv, "merged settings.gateAutoFix")?;
    let enabled = optional_bool(cfg, "enabled", "merged settings.gateAutoFix")?
        .unwrap_or(GateAutoFixConfig::default().enabled);
    let max_attempts = optional_usize(cfg, "maxAttempts", "merged settings.gateAutoFix")?
        .unwrap_or(GateAutoFixConfig::default().max_attempts);
    Ok(GateAutoFixConfig {
        enabled,
        max_attempts,
    })
}

fn parse_optional_runtime_control_config(
    root: &JsonValue,
) -> Result<RuntimeControlConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(RuntimeControlConfig::default());
    };
    let mut config = RuntimeControlConfig::default();
    let Some(runtime_value) = object.get("runtime") else {
        return Ok(config);
    };
    let runtime = expect_object(runtime_value, "merged settings.runtime")?;
    if let Some(scenario) = optional_string(runtime, "scenario", "merged settings.runtime")? {
        config.scenario = parse_domain_profile(scenario, "merged settings.runtime.scenario")?;
    }
    let Some(control_value) = runtime.get("control") else {
        apply_domain_profile_defaults(&mut config.policy, config.scenario);
        return Ok(config);
    };
    let control = expect_object(control_value, "merged settings.runtime.control")?;
    apply_domain_profile_defaults(&mut config.policy, config.scenario);
    if let Some(enabled) = optional_bool(control, "enabled", "merged settings.runtime.control")? {
        config.policy.enabled = enabled;
    }
    if let Some(agent_value) = control.get("agent") {
        let agent = expect_object(agent_value, "merged settings.runtime.control.agent")?;
        if let Some(enabled) =
            optional_bool(agent, "enabled", "merged settings.runtime.control.agent")?
        {
            config.policy.agent.enabled = enabled;
        }
        if let Some(max) = optional_usize(
            agent,
            "max_parallel_agents",
            "merged settings.runtime.control.agent",
        )? {
            config.policy.agent.max_parallel_agents = max;
        }
        if let Some(review) = optional_bool(
            agent,
            "review_on_conflict",
            "merged settings.runtime.control.agent",
        )? {
            config.policy.agent.review_on_conflict = review;
        }
        if let Some(required) = optional_bool(
            agent,
            "require_positive_lift",
            "merged settings.runtime.control.agent",
        )? {
            config.policy.agent.require_positive_lift = required;
        }
        if let Some(score) = optional_u16(
            agent,
            "min_collaboration_score",
            "merged settings.runtime.control.agent",
        )? {
            config.policy.agent.min_collaboration_score = score;
        }
    }
    if let Some(task_value) = control.get("task") {
        let task = expect_object(task_value, "merged settings.runtime.control.task")?;
        if let Some(enabled) = optional_bool(
            task,
            "auto_phase_for_yolo",
            "merged settings.runtime.control.task",
        )? {
            config.policy.task.auto_phase_for_yolo = enabled;
        }
        if let Some(review) = optional_bool(
            task,
            "review_after_each_phase",
            "merged settings.runtime.control.task",
        )? {
            config.policy.task.review_after_each_phase = review;
        }
        if let Some(max) = optional_u32(
            task,
            "max_failures_before_review",
            "merged settings.runtime.control.task",
        )? {
            config.policy.task.max_failures_before_review = max;
        }
    }
    if let Some(context_value) = control.get("context") {
        let context = expect_object(context_value, "merged settings.runtime.control.context")?;
        if let Some(preserve) = optional_bool(
            context,
            "preserve_stable_head",
            "merged settings.runtime.control.context",
        )? {
            config.policy.context.preserve_stable_head = preserve;
        }
        if let Some(tokens) = optional_u64(
            context,
            "yolo_budget_tokens",
            "merged settings.runtime.control.context",
        )? {
            config.policy.context.yolo_budget_tokens = tokens;
        }
        if let Some(tokens) = optional_u64(
            context,
            "collaboration_budget_tokens",
            "merged settings.runtime.control.context",
        )? {
            config.policy.context.collaboration_budget_tokens = tokens;
        }
        if let Some(tokens) = optional_u64(
            context,
            "review_budget_tokens",
            "merged settings.runtime.control.context",
        )? {
            config.policy.context.review_budget_tokens = tokens;
        }
        if let Some(pressure) = optional_u16(
            context,
            "degrade_on_pressure_bp",
            "merged settings.runtime.control.context",
        )? {
            config.policy.context.degrade_on_pressure_bp = pressure;
        }
    }
    if let Some(memory_value) = control.get("memory") {
        let memory = expect_object(memory_value, "merged settings.runtime.control.memory")?;
        if let Some(emit) = optional_bool(
            memory,
            "emit_pulses_from_execution_graph",
            "merged settings.runtime.control.memory",
        )? {
            config.policy.memory.emit_pulses_from_execution_graph = emit;
        }
        if let Some(review) = optional_bool(
            memory,
            "review_conflicts",
            "merged settings.runtime.control.memory",
        )? {
            config.policy.memory.review_conflicts = review;
        }
        if let Some(max) = optional_usize(
            memory,
            "max_candidates_per_turn",
            "merged settings.runtime.control.memory",
        )? {
            config.policy.memory.max_candidates_per_turn = max;
        }
    }
    if let Some(schedule_value) = control.get("mission_schedule") {
        let schedule = expect_object(
            schedule_value,
            "merged settings.runtime.control.mission_schedule",
        )?;
        if let Some(enabled) = optional_bool(
            schedule,
            "enabled",
            "merged settings.runtime.control.mission_schedule",
        )? {
            config.policy.mission_schedule.enabled = enabled;
        }
        if let Some(tick_interval_ms) = optional_u64(
            schedule,
            "tick_interval_ms",
            "merged settings.runtime.control.mission_schedule",
        )? {
            config.policy.mission_schedule.tick_interval_ms = tick_interval_ms;
        }
        if let Some(grace_ms) = optional_u64(
            schedule,
            "grace_ms",
            "merged settings.runtime.control.mission_schedule",
        )? {
            config.policy.mission_schedule.grace_ms = grace_ms;
        }
        config
            .policy
            .mission_schedule
            .validate()
            .map_err(ConfigError::Parse)?;
    }
    if let Some(permission_value) = control.get("permission") {
        let permission = expect_object(
            permission_value,
            "merged settings.runtime.control.permission",
        )?;
        if let Some(honor) = optional_bool(
            permission,
            "solo_honor_critical",
            "merged settings.runtime.control.permission",
        )? {
            config.policy.permission.solo_honor_critical = honor;
        }
        if let Some(review) = optional_bool(
            permission,
            "review_critical_actions",
            "merged settings.runtime.control.permission",
        )? {
            config.policy.permission.review_critical_actions = review;
        }
    }
    Ok(config)
}

fn parse_domain_profile(value: &str, context: &str) -> Result<DomainProfile, ConfigError> {
    match value {
        "coding" | "code" => Ok(DomainProfile::Coding),
        "research" => Ok(DomainProfile::Research),
        "office" | "work" => Ok(DomainProfile::Office),
        "ops" | "operations" => Ok(DomainProfile::Ops),
        "personal" => Ok(DomainProfile::Personal),
        other => Err(ConfigError::Parse(format!(
            "{context}: unsupported runtime scenario {other}"
        ))),
    }
}

fn apply_domain_profile_defaults(policy: &mut RuntimeControlPolicy, scenario: DomainProfile) {
    match scenario {
        DomainProfile::Coding => {}
        DomainProfile::Research => {
            policy.agent.min_collaboration_score = 60;
            policy.context.collaboration_budget_tokens = 14_000;
            policy.context.review_budget_tokens = 11_000;
            policy.memory.max_candidates_per_turn = 12;
        }
        DomainProfile::Office => {
            policy.agent.max_parallel_agents = 2;
            policy.agent.min_collaboration_score = 65;
            policy.context.yolo_budget_tokens = 8_000;
            policy.context.collaboration_budget_tokens = 8_000;
            policy.memory.max_candidates_per_turn = 6;
            policy.permission.review_critical_actions = true;
        }
        DomainProfile::Ops => {
            policy.task.max_failures_before_review = 1;
            policy.permission.review_critical_actions = true;
            policy.agent.review_on_conflict = true;
            policy.task.review_after_each_phase = true;
        }
        DomainProfile::Personal => {
            policy.agent.max_parallel_agents = 1;
            policy.agent.min_collaboration_score = 70;
            policy.context.yolo_budget_tokens = 9_000;
            policy.memory.max_candidates_per_turn = 5;
        }
    }
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

/// Look up a f32 config value, supporting both snake_case (preferred) and camelCase (deprecated).
fn optional_f32_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<f32>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_f32(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_f32(object, &camel_key, ctx);
    }
    Ok(None)
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

fn expect_array<'a>(value: &'a JsonValue, context: &str) -> Result<&'a [JsonValue], ConfigError> {
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
        .ok_or_else(|| ConfigError::Parse(format!("{context}: expected config object")))
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
            "{context}: expected config object"
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

fn parse_json_string_array(
    value: &JsonValue,
    key: &str,
    context: &str,
) -> Result<Vec<String>, ConfigError> {
    let Some(array) = value.as_array() else {
        return Err(ConfigError::Parse(format!(
            "{context}: field {key} must be an array"
        )));
    };
    array
        .iter()
        .map(|item| {
            item.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                ConfigError::Parse(format!("{context}: field {key} must contain only strings"))
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
        Some(value) => parse_json_string_array(value, key, context).map(Some),
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
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_string(object, &camel_key, ctx);
    }

    Ok(None)
}

/// Look up a boolean config value, supporting both snake_case (preferred) and camelCase (deprecated).
fn optional_bool_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<bool>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_bool(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_bool(object, &camel_key, ctx);
    }
    Ok(None)
}

/// Look up any config value by key, supporting snake_case (preferred), camelCase,
/// and PascalCase (deprecated). Emits a deprecation warning when a non-snake_case
/// key is used.
fn find_key_dual<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Option<&'a JsonValue> {
    // Try snake_case first.
    if let Some(value) = object.get(snake_key) {
        return Some(value);
    }
    // Try camelCase (lowercase first letter).
    let camel_key = to_camel_case(snake_key);
    if let Some(value) = object.get(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return Some(value);
    }
    // Try PascalCase (uppercase first letter).
    let pascal_key = {
        let mut chars = camel_key.chars();
        match chars.next() {
            Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
            None => return None,
        }
    };
    if let Some(value) = object.get(&pascal_key) {
        tracing::warn!(
            "config key '{pascal_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return Some(value);
    }
    None
}

/// Look up a string array config value, supporting both snake_case (preferred)
/// and camelCase/PascalCase (deprecated).
fn optional_string_array_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<Vec<String>>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_string_array(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_string_array(object, &camel_key, ctx);
    }
    let pascal_key = {
        let mut chars = camel_key.chars();
        match chars.next() {
            Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
            None => return Ok(None),
        }
    };
    if object.contains_key(&pascal_key) {
        tracing::warn!(
            "config key '{pascal_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_string_array(object, &pascal_key, ctx);
    }
    Ok(None)
}

/// Look up a u32 config value, supporting both snake_case (preferred) and camelCase (deprecated).
fn optional_u32_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<u32>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_u32(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_u32(object, &camel_key, ctx);
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

#[cfg(test)]
mod tests {
    use super::{
        deep_merge_objects, parse_optional_compression_config,
        parse_optional_context_budget_config, parse_optional_model_context_windows,
        parse_permission_mode_label, redact_serde_json, ConfigLoader, ConfigSource, DomainProfile,
        McpServerConfig, McpTransport, ProviderProtocol, ResolvedPermissionMode, RuntimeConfig,
        RuntimeFeatureConfig, RuntimeHookConfig, RuntimePluginConfig, SessionCompactConfig,
        COWD_SETTINGS_SCHEMA_NAME,
    };
    use crate::json::JsonValue;
    use crate::sandbox::FilesystemIsolationMode;
    use std::collections::BTreeMap;
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
            Self {
                key: key.to_string(),
                original,
            }
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
        fs::write(home.join("config.yaml"), "[]").expect("write bad settings");

        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should fail");
        assert!(error
            .to_string()
            .contains("top-level config value must be an object"));

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
            home.parent().expect("home parent").join(".cowd/config.yaml"),
            r#"{"model":"haiku","env":{"A":"1"},"mcpServers":{"home":{"command":"uvx","args":["home"]}}}"#,
        )
        .expect("write user compat config");
        fs::write(
            home.join("config.yaml"),
            r#"{"model":"sonnet","env":{"A2":"1"},"hooks":{"PreToolUse":["base"]},"permissions":{"defaultMode":"plan","allow":["Read"],"deny":["Bash(rm -rf)"]},"mcpServers":{"home":{"command":"uvx","args":["home"]}}}"#,
        )
        .expect("write user settings");
        fs::write(
            cwd.join(".cowd").join("config.yaml"),
            r#"{"model":"project-compat","env":{"B":"2","C":"3"},"hooks":{"PostToolUse":["project"],"PostToolUseFailure":["project-failure"]},"permissions":{"ask":["Edit"]},"mcpServers":{"project":{"command":"uvx","args":["project"]}}}"#,
        )
        .expect("write project settings");
        fs::write(
            cwd.join(".cowd").join("config.local.yaml"),
            r#"{"model":"opus","permissionMode":"acceptEdits"}"#,
        )
        .expect("write local settings");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(COWD_SETTINGS_SCHEMA_NAME, "SettingsSchema");
        assert_eq!(loaded.loaded_entries().len(), 3);
        assert_eq!(loaded.loaded_entries()[0].source, ConfigSource::User);
        assert!(loaded.loaded_entries()[1].source == ConfigSource::Project);
        assert!(loaded.loaded_entries()[2].source == ConfigSource::Local);
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
            3
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
    fn parses_snake_case_permission_mode_from_default_template_shape() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"
permissions:
  default_mode: "acceptEdits"
"#,
        )
        .expect("write config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(
            loaded.permission_mode(),
            Some(ResolvedPermissionMode::WorkspaceWrite)
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_top_level_approval_from_default_template_shape() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"
approval:
  solo_mode: true
  solo_honor_critical: false
  auto_pass_read_only: false
  auto_pass_low_risk: false
"#,
        )
        .expect("write config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert!(loaded.approval().solo_mode);
        assert!(!loaded.approval().solo_honor_critical);
        assert!(!loaded.approval().auto_pass_read_only);
        assert!(!loaded.approval().auto_pass_low_risk);

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
            cwd.join(".cowd").join("config.local.yaml"),
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
    fn config_runtime_control_merges_scenario_and_policy_overrides() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("config.yaml"),
            r#"{
              "runtime": {
                "scenario": "research",
                "control": {
                  "agent": {
                    "max_parallel_agents": 5
                  },
                  "context": {
                    "collaboration_budget_tokens": 16000
                  },
                  "mission_schedule": {
                    "tick_interval_ms": 1500,
                    "grace_ms": 120000
                  }
                }
              }
            }"#,
        )
        .expect("write user runtime control");
        fs::write(
            cwd.join(".cowd").join("config.local.yaml"),
            r#"{
              "runtime": {
                "control": {
                  "enabled": false,
                  "agent": {
                    "min_collaboration_score": 72
                  },
                  "task": {
                    "max_failures_before_review": 1
                  },
                  "memory": {
                    "max_candidates_per_turn": 3
                  }
                }
              }
            }"#,
        )
        .expect("write local runtime control");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("runtime control config should load");
        let runtime = loaded.runtime_control();

        assert_eq!(runtime.scenario, DomainProfile::Research);
        assert!(!runtime.policy.enabled);
        assert_eq!(runtime.policy.agent.max_parallel_agents, 5);
        assert_eq!(runtime.policy.agent.min_collaboration_score, 72);
        assert_eq!(runtime.policy.task.max_failures_before_review, 1);
        assert_eq!(runtime.policy.context.collaboration_budget_tokens, 16_000);
        assert_eq!(runtime.policy.memory.max_candidates_per_turn, 3);
        assert_eq!(runtime.policy.mission_schedule.tick_interval_ms, 1_500);
        assert_eq!(runtime.policy.mission_schedule.grace_ms, 120_000);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_provider_fallbacks_legacy_single_object_format() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");
        fs::write(
            home.join("config.yaml"),
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
        let chain = loaded.fallbacks();
        assert!(!chain.is_empty());
        assert_eq!(chain, &["grok-3".to_string(), "grok-3-mini".to_string()]);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_provider_fallbacks_array_format() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");
        fs::write(
            home.join("config.yaml"),
            r#"{
              "providerFallbacks": [
                {
                  "primary": "deepseek-v4-pro",
                  "fallbacks": ["deepseek-v4-flash", "qwen3.6-plus", "step-3.5-flash"]
                },
                {
                  "primary": "claude-sonnet-4-6",
                  "fallbacks": ["claude-haiku-4-6"]
                }
              ]
            }"#,
        )
        .expect("write provider fallback settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let chain = loaded.fallbacks();
        assert!(!chain.is_empty());
        assert_eq!(chain.len(), 4);
        assert!(chain.contains(&"deepseek-v4-flash".to_string()));
        assert!(chain.contains(&"qwen3.6-plus".to_string()));
        assert!(chain.contains(&"step-3.5-flash".to_string()));
        assert!(chain.contains(&"claude-haiku-4-6".to_string()));
        assert!(!chain.contains(&"nonexistent".to_string()));

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
        fs::write(home.join("config.yaml"), "{}").expect("write empty settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let chain = loaded.fallbacks();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn parses_provider_protocols_and_detects_when_unset() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"{
              "providers": {
                "openai": {
                  "base_url": "https://api.openai.com/v1",
                  "api_key": "sk-openai",
                  "models": ["gpt-5"],
                  "protocol": "responses"
                },
                "deepseek": {
                  "base_url": "https://api.deepseek.com/v1",
                  "api_key": "sk-deepseek",
                  "models": ["deepseek-v4-pro"],
                  "protocol": "completions"
                },
                "anthropic": {
                  "base_url": "https://api.anthropic.com",
                  "api_key": "sk-ant",
                  "models": ["claude-sonnet-4-6"]
                }
              }
            }"#,
        )
        .expect("write provider settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        // then
        let providers = loaded.providers();
        assert_eq!(
            ProviderProtocol::effective_for_provider(providers.get("openai").unwrap()).unwrap(),
            ProviderProtocol::Responses
        );
        assert_eq!(
            ProviderProtocol::effective_for_provider(providers.get("deepseek").unwrap()).unwrap(),
            ProviderProtocol::Completions
        );
        assert_eq!(
            ProviderProtocol::effective_for_provider(providers.get("anthropic").unwrap()).unwrap(),
            ProviderProtocol::Anthropic
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn rejects_unknown_provider_protocol() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"{
              "providers": {
                "gemini": {
                  "base_url": "https://generativelanguage.googleapis.com",
                  "api_key": "sk-test",
                  "models": ["gemini-2.5-pro"],
                  "protocol": "gemini-native"
                }
              }
            }"#,
        )
        .expect("write provider settings");

        // when
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("config should reject unsupported protocol");

        // then
        assert!(error.to_string().contains("providers.gemini.protocol"));
        assert!(error.to_string().contains("responses"));

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
            home.join("config.yaml"),
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
        fs::write(home.join("config.yaml"), "{}").expect("write empty settings");

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
            home.join("config.yaml"),
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
            cwd.join(".cowd").join("config.local.yaml"),
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
            home.join("config.yaml"),
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
            home.join("config.yaml"),
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
            home.join("config.yaml"),
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
            home.join("config.yaml"),
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
            home.join("config.yaml"),
            r#"{"aliases":{"fast":"claude-haiku-4-5-20251213","smart":"claude-opus-4-6"}}"#,
        )
        .expect("write user settings");
        fs::write(
            cwd.join(".cowd").join("config.local.yaml"),
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
        fs::write(home.join("config.yaml"), "").expect("write empty settings");

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
        let project_settings = cwd.join(".cowd").join("config.yaml");
        fs::create_dir_all(cwd.join(".cowd")).expect("project config dir");
        fs::create_dir_all(&home).expect("home config dir");

        fs::write(
            home.join("config.yaml"),
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
        config.enabled_plugins.insert("known".to_string(), true);

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
        let user_settings = home.join("config.yaml");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            &user_settings,
            "{\n  \"model\": \"opus\",\n  \"telemetry\": true\n}\n",
        )
        .expect("write user settings");

        // when
        let _config = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn validates_deprecated_top_level_keys_with_replacement_guidance() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let user_settings = home.join("config.yaml");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            &user_settings,
            "{\n  \"model\": \"opus\",\n  \"allowedTools\": [\"Read\"]\n}\n",
        )
        .expect("write user settings");

        // when
        let _config = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn validates_wrong_type_for_known_field_with_field_path() {
        // given
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let user_settings = home.join("config.yaml");
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
        let user_settings = home.join("config.yaml");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(&user_settings, "{\n  \"modle\": \"opus\"\n}\n").expect("write user settings");

        // when
        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load with warning");

        // then — config loads successfully; unknown key produces a stderr warning
        assert!(
            loaded.get("modle").is_some(),
            "unknown key should be present in merged config"
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn memory_vector_accepts_embedding_model_alias() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"
memory:
  enabled: true
  vector:
    enabled: true
    embedding_model: text-embedding-v4
    api_url: https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings
    api_key: test-key
    dimension: 0
    timeout_secs: 30
    batch_size: 32
"#,
        )
        .expect("write memory config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert!(loaded.memory().vector.enabled);
        assert_eq!(loaded.memory().vector.model, "text-embedding-v4");
        assert_eq!(
            loaded.memory().vector.api_url,
            "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"
        );
        assert_eq!(loaded.memory().vector.api_key, "test-key");
        assert_eq!(loaded.memory().vector.dimension, 0);
        assert_eq!(loaded.memory().vector.batch_size, 32);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn redacted_json_removes_nested_credential_and_transport_values() {
        let mut merged = BTreeMap::new();
        merged.insert(
            "providers".to_string(),
            JsonValue::parse(
                r#"{
                    "apiKey":"provider-secret",
                    "headers":{"Authorization":"Bearer secret"},
                    "env":{"TOKEN":"environment-secret"},
                    "nested":{"password":"password-secret","safe":"visible"}
                }"#,
            )
            .expect("fixture parses"),
        );
        let config = RuntimeConfig {
            merged,
            loaded_entries: Vec::new(),
            feature_config: RuntimeFeatureConfig::default(),
        };

        let rendered = config.redacted_json().render();
        assert!(!rendered.contains("provider-secret"));
        assert!(!rendered.contains("environment-secret"));
        assert!(!rendered.contains("password-secret"));
        assert!(rendered.contains("visible"));
        assert_eq!(
            redact_serde_json(serde_json::json!({"authorization":"secret","safe":"ok"})),
            serde_json::json!({"authorization":"[redacted]","safe":"ok"})
        );
    }

    #[test]
    fn gateway_webui_dir_reads_configured_static_asset_dir() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"
gateway:
  enabled: true
  webui_dir: "/tmp/cowd-edge-webui-dist"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
"#,
        )
        .expect("write gateway config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert!(loaded.gateway().enabled);
        assert_eq!(
            loaded.gateway().webui_dir.as_deref(),
            Some(std::path::Path::new("/tmp/cowd-edge-webui-dist"))
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn gateway_platform_accepts_snake_case_platform_type() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"
gateway:
  enabled: true
  platforms:
    - platform_type: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
"#,
        )
        .expect("write gateway config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        let platform = loaded
            .gateway()
            .platforms
            .first()
            .expect("platform should be parsed");
        assert_eq!(platform.platform_type, "api_server");
        assert!(!platform.extra.contains_key("platform_type"));
        assert_eq!(
            platform.extra.get("host").and_then(JsonValue::as_str),
            Some("127.0.0.1")
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn max_output_tokens_reads_from_environment_variable() {
        // given — set environment variable
        let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("4096"));

        // when
        let config = RuntimePluginConfig::default();

        // then
        assert_eq!(config.max_output_tokens(), Some(4096));
    }

    #[test]
    fn max_output_tokens_falls_back_to_none_when_env_var_is_unset() {
        // given — ensure env var is unset
        let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", None);

        // when
        let config = RuntimePluginConfig::default();

        // then
        assert_eq!(config.max_output_tokens(), None);
    }

    #[test]
    fn max_output_tokens_falls_back_to_none_when_env_var_is_invalid() {
        // given — set invalid environment variable
        let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("not-a-number"));

        // when
        let config = RuntimePluginConfig::default();

        // then — should fall back to None (not panic)
        assert_eq!(config.max_output_tokens(), None);
    }

    #[test]
    fn compression_session_defaults_to_semantic_checkpoint_controls() {
        let session = SessionCompactConfig::default();

        assert_eq!(session.preserve_recent, 6);
        assert_eq!(session.summary_max_tokens, 2000);
    }

    #[test]
    fn compression_rejects_removed_ratio_thresholds() {
        let root = JsonValue::parse(
            r#"{
                "compression": {
                    "micro": {
                        "time_decay_factor": 1
                    },
                    "session": {
                        "threshold_ratio_bp": 6500,
                        "preserve_recent": 12
                    },
                    "deep": {
                        "iterative_update": false
                    },
                    "circuit_breaker": {
                        "max_retries": 5,
                        "cooldown_secs": 60
                    }
                }
            }"#,
        )
        .expect("json should parse");

        let error = parse_optional_compression_config(&root)
            .expect_err("removed request-ratio threshold must be rejected");
        assert!(error.to_string().contains("threshold_ratio_bp was removed"));
    }

    #[test]
    fn parses_context_budget_separately_from_compression() {
        let root = JsonValue::parse(
            r#"{
                "context_budget": {
                    "subsystem_budget_ratio_bp": 6400
                }
            }"#,
        )
        .expect("json should parse");

        let budget = parse_optional_context_budget_config(&root)
            .expect("context budget config should parse");

        assert_eq!(budget.subsystem_budget_ratio_bp, 6400);
    }

    #[test]
    fn parses_model_context_window_override_and_rejects_invalid_small_value() {
        let root = JsonValue::parse(
            r#"{
                "model_context_windows": {
                    "private-model": 32768
                }
            }"#,
        )
        .expect("json should parse");
        let windows = parse_optional_model_context_windows(&root)
            .expect("context window override should parse");
        assert_eq!(windows["private-model"], 32_768);

        let invalid = JsonValue::parse(r#"{"model_context_windows":{"broken":1023}}"#)
            .expect("json should parse");
        assert!(parse_optional_model_context_windows(&invalid)
            .expect_err("sub-1024 context window must fail validation")
            .to_string()
            .contains("at least 1024"));
    }
}
