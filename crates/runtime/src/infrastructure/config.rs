use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::json::JsonValue;
use crate::runtime_control::RuntimeControlPolicy;
use crate::sandbox::{FilesystemIsolationMode, SandboxConfig};
use model_protocol::model_registry::ModelResolver;
pub use model_protocol::oauth::OAuthConfig;
pub use model_protocol::provider_config::{
    ParallelToolCallsMode, ProviderConfig, ProviderProtocol, ProvidersConfig,
};

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
    pub profile: harness_contract::policy::ApprovalProfile,
    #[serde(default)]
    pub low_risk_timeout: harness_contract::policy::LowRiskTimeoutAction,
}

impl Default for ApprovalConfig {
    fn default() -> Self {
        Self {
            profile: harness_contract::policy::ApprovalProfile::Balanced,
            low_risk_timeout: harness_contract::policy::LowRiskTimeoutAction::AutoApproveOnce,
        }
    }
}

impl ApprovalConfig {
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn with_profile(mut self, profile: harness_contract::policy::ApprovalProfile) -> Self {
        self.profile = profile;
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
}

impl Default for RuntimePluginConfig {
    fn default() -> Self {
        Self {
            enabled_plugins: BTreeMap::default(),
            external_directories: Vec::default(),
            install_root: None,
            registry_path: None,
            bundled_root: None,
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
        self.enabled && !self.model.trim().is_empty()
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
    workspace: Option<PathBuf>,
    hooks: RuntimeHookConfig,
    plugins: RuntimePluginConfig,
    mcp: McpConfigCollection,
    oauth: Option<OAuthConfig>,
    model: Option<String>,
    routing_mode: RoutingMode,
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
    session_history: crate::SessionHistoryConfig,
    gateway: GatewayConfig,
    apps: AppsConfig,
    storage: StorageTopologyConfig,
    gate_auto_fix: GateAutoFixConfig,
    runtime_control: RuntimeControlConfig,
    hot_state: crate::execution_core::hot_state::HotStateConfig,
    provider_resources: crate::ProviderResourceConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutingMode {
    #[default]
    Pinned,
    Auto,
}

/// Process-wide durable backend selection.  Credentials are deliberately
/// represented only by a secret reference; the resolved PostgreSQL URL never
/// enters Runtime configuration projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendSelection {
    #[default]
    Sqlite,
    Postgres,
    /// PostgreSQL is preferred; SQLite is used automatically when PostgreSQL
    /// is not configured or unavailable at cold start. Runtime fallback is
    /// deliberately process-scoped: no hot switching, no dual writes.
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgresTopologyConfig {
    pub logical_identity: String,
    pub secret_ref: String,
    pub max_connections: u32,
    pub server_reserve: u32,
    pub critical: PostgresLaneTopologyConfig,
    pub online_read: PostgresLaneTopologyConfig,
    pub background: PostgresLaneTopologyConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgresLaneTopologyConfig {
    pub max_connections: Option<u32>,
    pub min_idle_connections: Option<u32>,
    pub checkout_timeout_ms: u64,
}

impl Default for PostgresTopologyConfig {
    fn default() -> Self {
        Self {
            logical_identity: "cowd-primary".to_string(),
            secret_ref: String::new(),
            max_connections: 48,
            server_reserve: 8,
            critical: PostgresLaneTopologyConfig {
                max_connections: None,
                min_idle_connections: Some(3),
                checkout_timeout_ms: 250,
            },
            online_read: PostgresLaneTopologyConfig {
                max_connections: None,
                min_idle_connections: Some(4),
                checkout_timeout_ms: 500,
            },
            background: PostgresLaneTopologyConfig {
                max_connections: None,
                min_idle_connections: Some(2),
                checkout_timeout_ms: 2_000,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStorageExecutionConfig {
    pub workers: usize,
    pub queue_capacity: usize,
}

impl Default for SessionStorageExecutionConfig {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map_or(4, usize::from)
            .clamp(2, 16);
        Self {
            workers,
            queue_capacity: workers.saturating_mul(8),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactStorageConfig {
    pub compact_threshold_bytes: u64,
    pub max_object_bytes: u64,
    pub total_quota_bytes: u64,
    pub gc_high_water_bytes: u64,
    pub gc_low_water_bytes: u64,
    pub orphan_grace_ms: u64,
}

impl Default for ArtifactStorageConfig {
    fn default() -> Self {
        let defaults = crate::ArtifactStoreConfig::default();
        Self {
            compact_threshold_bytes: defaults.compact_threshold_bytes,
            max_object_bytes: defaults.max_object_bytes,
            total_quota_bytes: defaults.total_quota_bytes,
            gc_high_water_bytes: defaults.gc_high_water_bytes,
            gc_low_water_bytes: defaults.gc_low_water_bytes,
            orphan_grace_ms: defaults.orphan_grace_ms,
        }
    }
}

impl From<ArtifactStorageConfig> for crate::ArtifactStoreConfig {
    fn from(value: ArtifactStorageConfig) -> Self {
        Self {
            compact_threshold_bytes: value.compact_threshold_bytes,
            max_object_bytes: value.max_object_bytes,
            total_quota_bytes: value.total_quota_bytes,
            gc_high_water_bytes: value.gc_high_water_bytes,
            gc_low_water_bytes: value.gc_low_water_bytes,
            orphan_grace_ms: value.orphan_grace_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageTopologyConfig {
    pub backend: StorageBackendSelection,
    /// Preferred backend for `backend=auto`. Only `postgres` is supported.
    pub preferred: StorageBackendSelection,
    /// Fallback backend for `backend=auto`. Only `sqlite` is supported.
    pub fallback: StorageBackendSelection,
    /// PostgreSQL cold-start probe timeout used by `backend=auto`.
    pub fallback_probe_timeout_ms: u64,
    pub postgres: Option<PostgresTopologyConfig>,
    pub session_execution: SessionStorageExecutionConfig,
    pub artifacts: ArtifactStorageConfig,
}

impl Default for StorageTopologyConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendSelection::Auto,
            preferred: StorageBackendSelection::Postgres,
            fallback: StorageBackendSelection::Sqlite,
            fallback_probe_timeout_ms: 3_000,
            postgres: None,
            session_execution: SessionStorageExecutionConfig::default(),
            artifacts: ArtifactStorageConfig::default(),
        }
    }
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
    pub governance: MemoryGovernanceConfig,
    pub vector: VectorConfig,
    /// Jaccard similarity threshold for coherence filtering in basis points.
    /// 100 = 0.01, 1000 = 0.10 (default), 5000 = 0.50.
    /// Entries with score below this are excluded from context injection.
    pub coherence_threshold_bp: u32,
    pub identity: MemoryIdentityConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryIdentityConfig {
    pub role: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryGovernanceConfig {
    pub enabled: bool,
    pub startup_delay_secs: u64,
    pub deep_scan_hour_local: u8,
    pub max_candidates: usize,
    pub stale_threshold_bp: u16,
    pub low_confidence_threshold_bp: u16,
}

impl Default for MemoryGovernanceConfig {
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
            governance: MemoryGovernanceConfig::default(),
            vector: VectorConfig::default(),
            coherence_threshold_bp: 1000,
            identity: MemoryIdentityConfig::default(),
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

/// Startup policy for product applications that are already part of the
/// release artifact.  This is intentionally separate from application source
/// selection: configuration can enable or disable a reviewed APP, but can
/// never fetch or execute an arbitrary APP revision at runtime.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppsConfig {
    entries: BTreeMap<String, AppStartupConfig>,
}

impl AppsConfig {
    #[must_use]
    pub fn with_app_enabled(mut self, app_id: impl Into<String>, enabled: bool) -> Self {
        self.entries
            .insert(app_id.into(), AppStartupConfig { enabled });
        self
    }

    /// An APP not mentioned in configuration is enabled by default when the
    /// product contains it.  This preserves a full product's existing
    /// behaviour while still allowing an explicit operational kill switch.
    #[must_use]
    pub fn is_enabled(&self, app_id: &str) -> bool {
        self.entries
            .get(app_id)
            .map(|entry| entry.enabled)
            .unwrap_or(true)
    }

    #[must_use]
    pub fn configured_app_ids(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }
}

/// Per-APP startup policy.  Surface visibility deliberately does not live
/// here: a registered APP has one Gateway truth, and TUI/WebUI derive their
/// visible contributions from that truth instead of maintaining duplicate
/// surface switches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStartupConfig {
    pub enabled: bool,
}

impl Default for AppStartupConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Multi-platform gateway configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GatewayConfig {
    pub enabled: bool,
    pub webui_dir: Option<PathBuf>,
    pub platforms: Vec<PlatformConfig>,
    pub session_reset: SessionResetPolicy,
    pub capacity: GatewayCapacityConfig,
    pub recovery: SessionRecoveryConfig,
    pub presence: GatewayPresenceConfig,
    pub live: GatewayLiveConfig,
    pub translation: GatewayTranslationConfig,
}

/// Session attachment liveness policy. This is independent from multiplex
/// live-subscription leases: a Surface connection and an SSE subscription
/// have different owners and failure semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayPresenceConfig {
    pub ttl_seconds: u64,
}

impl Default for GatewayPresenceConfig {
    fn default() -> Self {
        Self { ttl_seconds: 3_600 }
    }
}

/// Gateway-owned derived-document translation policy. Translation is a
/// Surface management concern, not part of a conversation Runtime turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayTranslationConfig {
    pub model: Option<String>,
    pub cache_entries: usize,
}

impl Default for GatewayTranslationConfig {
    fn default() -> Self {
        Self {
            model: None,
            cache_entries: 256,
        }
    }
}

/// Gateway-owned multiplex live transport limits. These bounds protect the
/// shared SSE control plane without changing any durable Session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayLiveConfig {
    pub max_sources: usize,
    pub max_subscriptions_per_principal_instance: usize,
    pub queue_capacity: usize,
    pub checkpoint_max_bytes: usize,
    pub default_ttl_seconds: u64,
    pub max_ttl_seconds: u64,
    pub baseline_timeout_ms: u64,
}

impl Default for GatewayLiveConfig {
    fn default() -> Self {
        Self {
            max_sources: 32,
            max_subscriptions_per_principal_instance: 16,
            queue_capacity: 512,
            checkpoint_max_bytes: 6_144,
            default_ttl_seconds: 3_600,
            max_ttl_seconds: 86_400,
            baseline_timeout_ms: 15_000,
        }
    }
}

/// Gateway-owned hot Runtime working-set policy. Durable Session history is
/// never truncated by these limits; cold sessions remain attachable on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecoveryConfig {
    pub hot_bytes: usize,
    pub attached_bytes: usize,
    pub recent_bytes: usize,
    pub manifest_page_size: usize,
    pub hydrate_concurrency: usize,
    pub activation_tail_messages: usize,
    pub activation_metadata_messages: usize,
    pub context_card_cache_entries: usize,
    pub context_index_card_span: usize,
    pub context_index_parent_span: usize,
    pub stable_snapshot_attempts: usize,
    pub recent_window_ms: u64,
}

impl Default for SessionRecoveryConfig {
    fn default() -> Self {
        Self {
            hot_bytes: 512 * 1024 * 1024,
            attached_bytes: 128 * 1024 * 1024,
            recent_bytes: 256 * 1024 * 1024,
            manifest_page_size: 256,
            hydrate_concurrency: 8,
            activation_tail_messages: 256,
            activation_metadata_messages: 1_024,
            context_card_cache_entries: 256,
            context_index_card_span: 128,
            context_index_parent_span: 16,
            stable_snapshot_attempts: 16,
            recent_window_ms: 60_000,
        }
    }
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
        // P13: network domain policy is configurable while remaining env-first.
        // Illegal mode values are fail-closed: the process refuses to start
        // instead of silently widening network access.
        inject_network_domain_env(&merged)?;

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
            workspace: parse_optional_workspace(&merged_value)?,
            hooks: parse_optional_hooks_config(&merged_value)?,
            plugins: parse_optional_plugin_config(&merged_value)?,
            mcp: McpConfigCollection {
                servers: mcp_servers,
            },
            oauth: parse_optional_oauth_config(&merged_value, "merged settings.oauth")?,
            model: parse_optional_model(&merged_value),
            routing_mode: parse_routing_mode(&merged_value)?,
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
            session_history: parse_optional_session_history_config(&merged_value)?,
            gateway: parse_optional_gateway_config(&merged_value)?,
            apps: parse_optional_apps_config(&merged_value)?,
            storage: parse_optional_storage_config(&merged_value)?,
            gate_auto_fix: parse_optional_gate_auto_fix_config(&merged_value)?,
            runtime_control: parse_optional_runtime_control_config(&merged_value)?,
            hot_state: parse_optional_hot_state_config(&merged_value)?,
            provider_resources: parse_optional_provider_resource_config(&merged_value)?,
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
    pub fn workspace(&self) -> Option<&Path> {
        self.feature_config.workspace.as_deref()
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
    pub fn resolved_model(&self) -> Option<String> {
        self.feature_config.resolved_model()
    }

    /// Resolve the Gateway translation model through the same explicit alias
    /// table as conversation models. An omitted translation model inherits the
    /// active global model.
    #[must_use]
    pub fn resolved_gateway_translation_model(&self) -> Option<String> {
        let Some(model) = self.gateway().translation.model.as_deref() else {
            return self.resolved_model();
        };
        let aliases = self
            .aliases()
            .iter()
            .map(|(alias, target)| (alias.clone(), target.clone()))
            .collect::<HashMap<_, _>>();
        Some(ModelResolver::new(aliases).resolve(model))
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
    pub const fn routing_mode(&self) -> RoutingMode {
        self.feature_config.routing_mode
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
    pub fn session_history(&self) -> crate::SessionHistoryConfig {
        self.feature_config.session_history
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayConfig {
        &self.feature_config.gateway
    }

    #[must_use]
    pub fn apps(&self) -> &AppsConfig {
        &self.feature_config.apps
    }

    #[must_use]
    pub fn storage(&self) -> &StorageTopologyConfig {
        &self.feature_config.storage
    }

    #[must_use]
    pub fn gate_auto_fix(&self) -> &GateAutoFixConfig {
        &self.feature_config.gate_auto_fix
    }

    #[must_use]
    pub fn runtime_control(&self) -> &RuntimeControlConfig {
        &self.feature_config.runtime_control
    }

    #[must_use]
    pub fn hot_state(&self) -> &crate::execution_core::hot_state::HotStateConfig {
        &self.feature_config.hot_state
    }

    #[must_use]
    pub fn provider_resources(&self) -> &crate::ProviderResourceConfig {
        &self.feature_config.provider_resources
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
    pub fn workspace(&self) -> Option<&Path> {
        self.workspace.as_deref()
    }

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
    pub fn resolved_model(&self) -> Option<String> {
        let model = self.model()?;
        let aliases = self
            .aliases
            .iter()
            .map(|(alias, target)| (alias.clone(), target.clone()))
            .collect::<HashMap<_, _>>();
        Some(ModelResolver::new(aliases).resolve(model))
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
    pub const fn routing_mode(&self) -> RoutingMode {
        self.routing_mode
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
    pub const fn session_history(&self) -> crate::SessionHistoryConfig {
        self.session_history
    }

    #[must_use]
    pub fn gateway(&self) -> &GatewayConfig {
        &self.gateway
    }

    #[must_use]
    pub fn storage(&self) -> &StorageTopologyConfig {
        &self.storage
    }

    #[must_use]
    pub fn gate_auto_fix(&self) -> &GateAutoFixConfig {
        &self.gate_auto_fix
    }

    #[must_use]
    pub fn runtime_control(&self) -> &RuntimeControlConfig {
        &self.runtime_control
    }

    #[must_use]
    pub fn hot_state(&self) -> &crate::execution_core::hot_state::HotStateConfig {
        &self.hot_state
    }

    #[must_use]
    pub fn provider_resources(&self) -> &crate::ProviderResourceConfig {
        &self.provider_resources
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

/// P13: bridge `network.domain.{mode,allow,block}` from merged settings into
/// the process environment used by the tools crate. Existing environment
/// variables always win (env-first); config file values are the fallback.
/// Invalid mode values reject startup (fail-closed to Deny would otherwise
/// hide the misconfiguration while tools silently widen access).
fn inject_network_domain_env(merged: &BTreeMap<String, JsonValue>) -> Result<(), ConfigError> {
    let Some(JsonValue::Object(network)) = merged.get("network") else {
        return Ok(());
    };
    let Some(JsonValue::Object(domain)) = network.get("domain") else {
        return Ok(());
    };
    let set_if_absent = |env_key: &str, value: &str| {
        if std::env::var(env_key).is_err() {
            std::env::set_var(env_key, value);
        }
    };
    if let Some(JsonValue::String(mode)) = domain.get("mode") {
        let normalized = mode.trim().to_ascii_lowercase();
        if !matches!(normalized.as_str(), "allow" | "ask" | "deny") {
            return Err(ConfigError::Parse(format!(
                "network.domain.mode `{mode}` is invalid; legal values are allow, ask, deny (fail-closed)"
            )));
        }
        set_if_absent("COWD_NETWORK_DOMAIN_MODE", &normalized);
    }
    if let Some(JsonValue::Array(allow)) = domain.get("allow") {
        let joined = allow
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>()
            .join(",");
        set_if_absent("COWD_NETWORK_DOMAIN_ALLOW", &joined);
    }
    if let Some(JsonValue::Array(block)) = domain.get("block") {
        let joined = block
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>()
            .join(",");
        set_if_absent("COWD_NETWORK_DOMAIN_BLOCK", &joined);
    }
    Ok(())
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

fn parse_optional_workspace(root: &JsonValue) -> Result<Option<PathBuf>, ConfigError> {
    let Some(value) = root.as_object().and_then(|object| object.get("workspace")) else {
        return Ok(None);
    };
    let workspace = value.as_str().ok_or_else(|| {
        ConfigError::Parse("merged settings: field workspace must be a string".to_string())
    })?;
    let workspace = workspace.trim();
    if workspace.is_empty() {
        return Err(ConfigError::Parse(
            "merged settings: field workspace must not be empty".to_string(),
        ));
    }
    Ok(Some(PathBuf::from(workspace)))
}

fn parse_routing_mode(root: &JsonValue) -> Result<RoutingMode, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(RoutingMode::Pinned);
    };
    let Some(value) = object.get("routing_mode") else {
        return Ok(RoutingMode::Pinned);
    };
    let mode = value.as_str().ok_or_else(|| {
        ConfigError::Parse("merged settings: field routing_mode must be a string".to_string())
    })?;
    match mode {
        "pinned" => Ok(RoutingMode::Pinned),
        "auto" => Ok(RoutingMode::Auto),
        other => Err(ConfigError::Parse(format!(
            "merged settings: unsupported routing_mode `{other}`; expected pinned or auto"
        ))),
    }
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
    let approval = object.get("approval").and_then(JsonValue::as_object);
    let Some(approval) = approval else {
        return Ok(ApprovalConfig::default());
    };

    let profile = approval
        .get("profile")
        .and_then(JsonValue::as_str)
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "supervised" => Ok(harness_contract::policy::ApprovalProfile::Supervised),
            "balanced" => Ok(harness_contract::policy::ApprovalProfile::Balanced),
            "autonomous" => Ok(harness_contract::policy::ApprovalProfile::Autonomous),
            other => Err(ConfigError::Invalid {
                key: "approval.profile".to_string(),
                message: format!(
                    "unsupported value `{other}`; expected supervised, balanced, or autonomous"
                ),
            }),
        })
        .transpose()?
        .unwrap_or_default();
    let low_risk_timeout = approval
        .get("low_risk_timeout")
        .and_then(JsonValue::as_str)
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "auto_approve_once" => {
                Ok(harness_contract::policy::LowRiskTimeoutAction::AutoApproveOnce)
            }
            "pending" => Ok(harness_contract::policy::LowRiskTimeoutAction::Pending),
            other => Err(ConfigError::Invalid {
                key: "approval.low_risk_timeout".to_string(),
                message: format!(
                    "unsupported value `{other}`; expected auto_approve_once or pending"
                ),
            }),
        })
        .transpose()?
        .unwrap_or_default();
    Ok(ApprovalConfig {
        profile,
        low_risk_timeout,
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
    Ok(config)
}

fn parse_optional_permission_mode(
    root: &JsonValue,
) -> Result<Option<ResolvedPermissionMode>, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(None);
    };
    if object.contains_key("permissionMode") || object.contains_key("permission_mode") {
        return Err(ConfigError::Parse(
            "top-level permission mode was removed; use permissions.default_mode with read-only, workspace-write, or danger-full-access"
                .to_string(),
        ));
    }
    let permissions = object.get("permissions").and_then(JsonValue::as_object);
    if permissions.is_some_and(|permissions| permissions.contains_key("defaultMode")) {
        return Err(ConfigError::Parse(
            "merged settings.permissions.defaultMode was removed; use permissions.default_mode with read-only, workspace-write, or danger-full-access"
                .to_string(),
        ));
    }
    let Some(mode) = permissions
        .and_then(|permissions| permissions.get("default_mode"))
        .and_then(JsonValue::as_str)
    else {
        return Ok(None);
    };
    parse_permission_mode_label(mode, "merged settings.permissions.default_mode").map(Some)
}

fn parse_permission_mode_label(
    mode: &str,
    context: &str,
) -> Result<ResolvedPermissionMode, ConfigError> {
    match mode {
        "read-only" => Ok(ResolvedPermissionMode::ReadOnly),
        "workspace-write" => Ok(ResolvedPermissionMode::WorkspaceWrite),
        "danger-full-access" => Ok(ResolvedPermissionMode::DangerFullAccess),
        other => Err(ConfigError::Parse(format!(
            "{context}: unsupported permission mode {other}; expected read-only, workspace-write, or danger-full-access"
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
        network_isolation: optional_bool_dual(
            sandbox,
            "network_isolation",
            "merged settings.sandbox",
        )?
        .or(optional_bool(
            sandbox,
            "isolate_network",
            "merged settings.sandbox",
        )?),
        filesystem_mode,
        workspace_root: optional_string_dual(sandbox, "workspace_root", "merged settings.sandbox")?
            .map(|value| crate::cowd_dirs::expand_tilde(&value)),
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
/// [`ProvidersConfig`]. Runtime execution then remains unconfigured until a
/// provider is declared explicitly.
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
        let parallel_tool_calls = optional_string_dual(entry, "parallel_tool_calls", &ctx)?
            .map_or(Ok(ParallelToolCallsMode::Auto), |value| {
                ParallelToolCallsMode::parse(value).ok_or_else(|| ConfigError::Invalid {
                    key: format!("providers.{name}.parallel_tool_calls"),
                    message: format!(
                        "unsupported value '{value}'. Valid values: \"auto\", \"enabled\", \"disabled\""
                    ),
                })
            })?;
        let early_tool_start = optional_string_dual(entry, "early_tool_start", &ctx)?.map_or(
            Ok(model_protocol::provider_config::EarlyToolStartMode::Auto),
            |value| {
                model_protocol::provider_config::EarlyToolStartMode::parse(value).ok_or_else(|| {
                    ConfigError::Invalid {
                        key: format!("providers.{name}.early_tool_start"),
                        message: format!(
                            "unsupported value '{value}'. Valid values: \"auto\", \"enabled\", \"disabled\""
                        ),
                    }
                })
            },
        )?;

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
                parallel_tool_calls,
                early_tool_start,
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
    let identity = if let Some(identity_val) = mem.get("identity") {
        let identity = expect_object(identity_val, "merged settings.memory.identity")?;
        MemoryIdentityConfig {
            role: optional_string(identity, "role", "merged settings.memory.identity")?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            language: optional_string(identity, "language", "merged settings.memory.identity")?
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        }
    } else {
        MemoryIdentityConfig::default()
    };
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
            auto_extract: optional_bool_dual(
                e,
                "auto_extract",
                "merged settings.memory.extraction",
            )?
            .unwrap_or(ExtractionConfig::default().auto_extract),
        }
    } else {
        ExtractionConfig::default()
    };
    let governance = if let Some(value) = mem.get("governance") {
        let object = expect_object(value, "merged settings.memory.governance")?;
        let defaults = MemoryGovernanceConfig::default();
        let hour = optional_u32_dual(
            object,
            "deep_scan_hour_local",
            "merged settings.memory.governance",
        )?
        .unwrap_or(u32::from(defaults.deep_scan_hour_local));
        if hour > 23 {
            return Err(ConfigError::Invalid {
                key: "merged settings.memory.governance.deep_scan_hour_local".to_string(),
                message: "must be between 0 and 23".to_string(),
            });
        }
        MemoryGovernanceConfig {
            enabled: optional_bool_dual(object, "enabled", "merged settings.memory.governance")?
                .unwrap_or(defaults.enabled),
            startup_delay_secs: optional_u64(
                object,
                "startup_delay_secs",
                "merged settings.memory.governance",
            )?
            .unwrap_or(defaults.startup_delay_secs),
            deep_scan_hour_local: hour as u8,
            max_candidates: optional_usize(
                object,
                "max_candidates",
                "merged settings.memory.governance",
            )?
            .unwrap_or(defaults.max_candidates)
            .clamp(16, 2_000),
            stale_threshold_bp: optional_u32_dual(
                object,
                "stale_threshold_bp",
                "merged settings.memory.governance",
            )?
            .unwrap_or(u32::from(defaults.stale_threshold_bp))
            .min(10_000) as u16,
            low_confidence_threshold_bp: optional_u32_dual(
                object,
                "low_confidence_threshold_bp",
                "merged settings.memory.governance",
            )?
            .unwrap_or(u32::from(defaults.low_confidence_threshold_bp))
            .min(10_000) as u16,
        }
    } else {
        MemoryGovernanceConfig::default()
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
        governance,
        vector,
        coherence_threshold_bp: optional_u32_dual(
            mem,
            "coherence_threshold_bp",
            "merged settings.memory",
        )?
        .unwrap_or(MemoryConfig::default().coherence_threshold_bp),
        identity,
    })
}

fn parse_optional_session_history_config(
    root: &JsonValue,
) -> Result<crate::SessionHistoryConfig, ConfigError> {
    let defaults = crate::SessionHistoryConfig::default();
    let Some(object) = root.as_object() else {
        return Ok(defaults);
    };
    let Some(history_value) = object.get("session_history") else {
        return Ok(defaults);
    };
    let history = expect_object(history_value, "merged settings.session_history")?;
    let config = crate::SessionHistoryConfig {
        chunk_messages: optional_usize(
            history,
            "chunk_messages",
            "merged settings.session_history",
        )?
        .unwrap_or(defaults.chunk_messages),
        chunk_bytes: optional_usize(history, "chunk_bytes", "merged settings.session_history")?
            .unwrap_or(defaults.chunk_bytes),
        request_cache_entries: optional_usize(
            history,
            "request_cache_entries",
            "merged settings.session_history",
        )?
        .unwrap_or(defaults.request_cache_entries),
    };
    if config.chunk_messages == 0 || config.chunk_bytes == 0 || config.request_cache_entries == 0 {
        return Err(ConfigError::Parse(
            "merged settings.session_history values must be greater than zero".to_string(),
        ));
    }
    Ok(config)
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
    let recovery = if let Some(value) = gw.get("recovery") {
        let value = expect_object(value, "merged settings.gateway.recovery")?;
        let defaults = SessionRecoveryConfig::default();
        let recovery = SessionRecoveryConfig {
            hot_bytes: optional_usize(value, "hot_bytes", "merged settings.gateway.recovery")?
                .unwrap_or(defaults.hot_bytes),
            attached_bytes: optional_usize(
                value,
                "attached_bytes",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.attached_bytes),
            recent_bytes: optional_usize(
                value,
                "recent_bytes",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.recent_bytes),
            manifest_page_size: optional_usize(
                value,
                "manifest_page_size",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.manifest_page_size),
            hydrate_concurrency: optional_usize(
                value,
                "hydrate_concurrency",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.hydrate_concurrency),
            activation_tail_messages: optional_usize(
                value,
                "activation_tail_messages",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.activation_tail_messages),
            activation_metadata_messages: optional_usize(
                value,
                "activation_metadata_messages",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.activation_metadata_messages),
            context_card_cache_entries: optional_usize(
                value,
                "context_card_cache_entries",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.context_card_cache_entries),
            context_index_card_span: optional_usize(
                value,
                "context_index_card_span",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.context_index_card_span),
            context_index_parent_span: optional_usize(
                value,
                "context_index_parent_span",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.context_index_parent_span),
            stable_snapshot_attempts: optional_usize(
                value,
                "stable_snapshot_attempts",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.stable_snapshot_attempts),
            recent_window_ms: optional_u64(
                value,
                "recent_window_ms",
                "merged settings.gateway.recovery",
            )?
            .unwrap_or(defaults.recent_window_ms),
        };
        if recovery.hot_bytes == 0
            || recovery.manifest_page_size == 0
            || recovery.hydrate_concurrency == 0
            || recovery.activation_tail_messages == 0
            || recovery.activation_metadata_messages == 0
            || recovery.context_card_cache_entries == 0
            || recovery.context_index_card_span == 0
            || recovery.context_index_parent_span < 2
            || recovery.stable_snapshot_attempts == 0
            || recovery.attached_bytes > recovery.hot_bytes
            || recovery.recent_bytes > recovery.hot_bytes
        {
            return Err(ConfigError::Parse(
                "merged settings.gateway.recovery requires positive hot_bytes, manifest_page_size, hydrate_concurrency, activation tail/metadata/card budgets and stable_snapshot_attempts; context_index_parent_span must be at least 2; attached/recent budgets must not exceed hot_bytes"
                    .to_string(),
            ));
        }
        recovery
    } else {
        SessionRecoveryConfig::default()
    };
    let live = if let Some(value) = gw.get("live") {
        let value = expect_object(value, "merged settings.gateway.live")?;
        let defaults = GatewayLiveConfig::default();
        let live = GatewayLiveConfig {
            max_sources: optional_usize(value, "max_sources", "merged settings.gateway.live")?
                .unwrap_or(defaults.max_sources),
            max_subscriptions_per_principal_instance: optional_usize(
                value,
                "max_subscriptions_per_principal_instance",
                "merged settings.gateway.live",
            )?
            .unwrap_or(defaults.max_subscriptions_per_principal_instance),
            queue_capacity: optional_usize(
                value,
                "queue_capacity",
                "merged settings.gateway.live",
            )?
            .unwrap_or(defaults.queue_capacity),
            checkpoint_max_bytes: optional_usize(
                value,
                "checkpoint_max_bytes",
                "merged settings.gateway.live",
            )?
            .unwrap_or(defaults.checkpoint_max_bytes),
            default_ttl_seconds: optional_u64(
                value,
                "default_ttl_seconds",
                "merged settings.gateway.live",
            )?
            .unwrap_or(defaults.default_ttl_seconds),
            max_ttl_seconds: optional_u64(
                value,
                "max_ttl_seconds",
                "merged settings.gateway.live",
            )?
            .unwrap_or(defaults.max_ttl_seconds),
            baseline_timeout_ms: optional_u64(
                value,
                "baseline_timeout_ms",
                "merged settings.gateway.live",
            )?
            .unwrap_or(defaults.baseline_timeout_ms),
        };
        if live.max_sources == 0
            || live.max_subscriptions_per_principal_instance == 0
            || live.queue_capacity == 0
            || live.checkpoint_max_bytes < 1_024
            || live.default_ttl_seconds == 0
            || live.max_ttl_seconds < live.default_ttl_seconds
            || live.baseline_timeout_ms == 0
        {
            return Err(ConfigError::Parse(
                "merged settings.gateway.live requires positive source/subscription/queue/TTL/timeout limits, checkpoint_max_bytes >= 1024, and max_ttl_seconds >= default_ttl_seconds"
                    .to_string(),
            ));
        }
        live
    } else {
        GatewayLiveConfig::default()
    };
    let presence = if let Some(value) = gw.get("presence") {
        let value = expect_object(value, "merged settings.gateway.presence")?;
        let defaults = GatewayPresenceConfig::default();
        let ttl_seconds = optional_u64(value, "ttl_seconds", "merged settings.gateway.presence")?
            .unwrap_or(defaults.ttl_seconds);
        if ttl_seconds == 0 {
            return Err(ConfigError::Parse(
                "merged settings.gateway.presence.ttl_seconds must be positive".to_string(),
            ));
        }
        GatewayPresenceConfig { ttl_seconds }
    } else {
        GatewayPresenceConfig::default()
    };
    let translation = if let Some(value) = gw.get("translation") {
        let value = expect_object(value, "merged settings.gateway.translation")?;
        let defaults = GatewayTranslationConfig::default();
        let model = optional_string_dual(value, "model", "merged settings.gateway.translation")?
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(str::to_string);
        let cache_entries = optional_usize(
            value,
            "cache_entries",
            "merged settings.gateway.translation",
        )?
        .unwrap_or(defaults.cache_entries);
        if cache_entries > 4_096 {
            return Err(ConfigError::Parse(
                "merged settings.gateway.translation.cache_entries must not exceed 4096"
                    .to_string(),
            ));
        }
        GatewayTranslationConfig {
            model,
            cache_entries,
        }
    } else {
        GatewayTranslationConfig::default()
    };
    Ok(GatewayConfig {
        enabled,
        webui_dir,
        platforms,
        session_reset,
        capacity,
        recovery,
        presence,
        live,
        translation,
    })
}

fn parse_optional_apps_config(root: &JsonValue) -> Result<AppsConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(AppsConfig::default());
    };
    let Some(apps_value) = object.get("apps") else {
        return Ok(AppsConfig::default());
    };
    let apps = expect_object(apps_value, "merged settings.apps")?;
    let mut entries = BTreeMap::new();
    for (app_id, value) in apps {
        if app_id.trim().is_empty() {
            return Err(ConfigError::Parse(
                "merged settings.apps contains an empty application id".to_string(),
            ));
        }
        let context = format!("merged settings.apps.{app_id}");
        let entry = expect_object(value, &context)?;
        let enabled = optional_bool(entry, "enabled", &context)?.unwrap_or(true);
        entries.insert(app_id.clone(), AppStartupConfig { enabled });
    }
    Ok(AppsConfig { entries })
}

fn parse_postgres_lane_config(
    postgres: &BTreeMap<String, JsonValue>,
    key: &str,
    defaults: PostgresLaneTopologyConfig,
    parent_context: &str,
) -> Result<PostgresLaneTopologyConfig, ConfigError> {
    let Some(value) = postgres.get(key) else {
        return Ok(defaults);
    };
    let context = format!("{parent_context}.{key}");
    let lane = expect_object(value, &context)?;
    let config = PostgresLaneTopologyConfig {
        max_connections: optional_u32(lane, "maxConnections", &context)?,
        min_idle_connections: optional_u32(lane, "minIdleConnections", &context)?
            .or(defaults.min_idle_connections),
        checkout_timeout_ms: optional_u64(lane, "checkoutTimeoutMs", &context)?
            .unwrap_or(defaults.checkout_timeout_ms),
    };
    if config.max_connections == Some(0)
        || config
            .max_connections
            .zip(config.min_idle_connections)
            .is_some_and(|(maximum, minimum)| minimum > maximum)
        || !(100..=120_000).contains(&config.checkout_timeout_ms)
    {
        return Err(ConfigError::Parse(format!(
            "{context} requires maxConnections > 0, minIdleConnections <= maxConnections, and checkoutTimeoutMs 100..120000"
        )));
    }
    Ok(config)
}

fn parse_optional_storage_config(root: &JsonValue) -> Result<StorageTopologyConfig, ConfigError> {
    let Some(root) = root.as_object() else {
        return Ok(StorageTopologyConfig::default());
    };
    let Some(value) = root.get("storage") else {
        return Ok(StorageTopologyConfig::default());
    };
    let storage = expect_object(value, "merged settings.storage")?;
    let backend =
        match optional_string(storage, "backend", "merged settings.storage")?.unwrap_or("auto") {
            "sqlite" => StorageBackendSelection::Sqlite,
            "postgres" => StorageBackendSelection::Postgres,
            "auto" => StorageBackendSelection::Auto,
            other => {
                return Err(ConfigError::Parse(format!(
                    "merged settings.storage.backend must be sqlite, postgres, or auto, got {other}"
                )))
            }
        };
    let preferred = match optional_string(storage, "preferred", "merged settings.storage")?
        .unwrap_or("postgres")
    {
        "postgres" => StorageBackendSelection::Postgres,
        other => {
            return Err(ConfigError::Parse(format!(
                "merged settings.storage.preferred must be postgres, got {other}"
            )))
        }
    };
    let fallback = match optional_string(storage, "fallback", "merged settings.storage")?
        .unwrap_or("sqlite")
    {
        "sqlite" => StorageBackendSelection::Sqlite,
        other => {
            return Err(ConfigError::Parse(format!(
                "merged settings.storage.fallback must be sqlite, got {other}"
            )))
        }
    };
    let fallback_probe_timeout_ms =
        optional_u64(storage, "fallbackProbeTimeoutMs", "merged settings.storage")?
            .unwrap_or(3_000);
    if !(100..=60_000).contains(&fallback_probe_timeout_ms) {
        return Err(ConfigError::Parse(
            "merged settings.storage.fallbackProbeTimeoutMs must be 100..60000".to_string(),
        ));
    }
    let postgres = storage
        .get("postgres")
        .map(|value| {
            let value = expect_object(value, "merged settings.storage.postgres")?;
            let defaults = PostgresTopologyConfig::default();
            let logical_identity = optional_string(
                value,
                "logicalIdentity",
                "merged settings.storage.postgres",
            )?
            .unwrap_or(defaults.logical_identity.as_str())
            .trim()
            .to_string();
            let secret_ref = optional_string(
                value,
                "secretRef",
                "merged settings.storage.postgres",
            )?
            .unwrap_or_default()
            .trim()
            .to_string();
            let max_connections = optional_u32(
                value,
                "maxConnections",
                "merged settings.storage.postgres",
            )?
            .unwrap_or(defaults.max_connections);
            let server_reserve = optional_u32(
                value,
                "serverReserve",
                "merged settings.storage.postgres",
            )?
            .unwrap_or(defaults.server_reserve);
            if value.contains_key("minIdleConnections")
                || value.contains_key("checkoutTimeoutMs")
            {
                return Err(ConfigError::Parse(
                    "merged settings.storage.postgres uses per-lane minIdleConnections and checkoutTimeoutMs; root-level legacy fields are not supported".to_string(),
                ));
            }
            let critical = parse_postgres_lane_config(
                value,
                "critical",
                defaults.critical,
                "merged settings.storage.postgres",
            )?;
            let online_read = parse_postgres_lane_config(
                value,
                "onlineRead",
                defaults.online_read,
                "merged settings.storage.postgres",
            )?;
            let background = parse_postgres_lane_config(
                value,
                "background",
                defaults.background,
                "merged settings.storage.postgres",
            )?;
            let explicit_lane_sizes = [
                critical.max_connections,
                online_read.max_connections,
                background.max_connections,
            ];
            let explicit_count = explicit_lane_sizes
                .iter()
                .filter(|value| value.is_some())
                .count();
            let explicit_sum = explicit_lane_sizes.into_iter().flatten().sum::<u32>();
            if logical_identity.is_empty()
                || secret_ref.is_empty()
                || !(3..=1_024).contains(&max_connections)
                || server_reserve > 1_024
                || !matches!(explicit_count, 0 | 3)
                || (explicit_count == 3 && explicit_sum != max_connections)
            {
                return Err(ConfigError::Parse(
                    "merged settings.storage.postgres requires non-empty logicalIdentity/secretRef, maxConnections 3..1024, serverReserve <=1024, and either all three lane maxConnections summing to the total or none".to_string(),
                ));
            }
            Ok(PostgresTopologyConfig {
                logical_identity,
                secret_ref,
                max_connections,
                server_reserve,
                critical,
                online_read,
                background,
            })
        })
        .transpose()?;
    let session_execution = storage
        .get("sessionExecution")
        .map(|value| {
            let value = expect_object(value, "merged settings.storage.sessionExecution")?;
            let defaults = SessionStorageExecutionConfig::default();
            let workers = optional_usize(
                value,
                "workers",
                "merged settings.storage.sessionExecution",
            )?
            .unwrap_or(defaults.workers);
            let queue_capacity = optional_usize(
                value,
                "queueCapacity",
                "merged settings.storage.sessionExecution",
            )?
            .unwrap_or(defaults.queue_capacity);
            if !(1..=64).contains(&workers) || !(1..=65_536).contains(&queue_capacity) {
                return Err(ConfigError::Parse(
                    "merged settings.storage.sessionExecution requires workers 1..64 and queueCapacity 1..65536".to_string(),
                ));
            }
            Ok(SessionStorageExecutionConfig {
                workers,
                queue_capacity,
            })
        })
        .transpose()?
        .unwrap_or_default();
    let artifacts = storage
        .get("artifacts")
        .map(|value| {
            let value = expect_object(value, "merged settings.storage.artifacts")?;
            let defaults = ArtifactStorageConfig::default();
            let compact_threshold_bytes = optional_u64(
                value,
                "compactThresholdBytes",
                "merged settings.storage.artifacts",
            )?
            .unwrap_or(defaults.compact_threshold_bytes);
            let max_object_bytes =
                optional_u64(value, "maxObjectBytes", "merged settings.storage.artifacts")?
                    .unwrap_or(defaults.max_object_bytes);
            let total_quota_bytes = optional_u64(
                value,
                "totalQuotaBytes",
                "merged settings.storage.artifacts",
            )?
            .unwrap_or(defaults.total_quota_bytes);
            let gc_high_water_bytes = optional_u64(
                value,
                "gcHighWaterBytes",
                "merged settings.storage.artifacts",
            )?
            .unwrap_or(defaults.gc_high_water_bytes);
            let gc_low_water_bytes = optional_u64(
                value,
                "gcLowWaterBytes",
                "merged settings.storage.artifacts",
            )?
            .unwrap_or(defaults.gc_low_water_bytes);
            let orphan_grace_ms =
                optional_u64(value, "orphanGraceMs", "merged settings.storage.artifacts")?
                    .unwrap_or(defaults.orphan_grace_ms);
            let selected = ArtifactStorageConfig {
                compact_threshold_bytes,
                max_object_bytes,
                total_quota_bytes,
                gc_high_water_bytes,
                gc_low_water_bytes,
                orphan_grace_ms,
            };
            crate::ArtifactStoreConfig::from(selected)
                .validate()
                .map_err(|error| {
                    ConfigError::Parse(format!(
                        "merged settings.storage.artifacts is invalid: {error}"
                    ))
                })?;
            Ok::<ArtifactStorageConfig, ConfigError>(selected)
        })
        .transpose()?
        .unwrap_or_default();
    if backend == StorageBackendSelection::Postgres && postgres.is_none() {
        return Err(ConfigError::Parse(
            "merged settings.storage.postgres is required when backend=postgres".to_string(),
        ));
    }
    if backend == StorageBackendSelection::Auto
        && (preferred != StorageBackendSelection::Postgres
            || fallback != StorageBackendSelection::Sqlite)
    {
        return Err(ConfigError::Parse(
            "merged settings.storage.backend=auto supports only preferred=postgres and fallback=sqlite"
                .to_string(),
        ));
    }
    Ok(StorageTopologyConfig {
        backend,
        preferred,
        fallback,
        fallback_probe_timeout_ms,
        postgres,
        session_execution,
        artifacts,
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
    Ok(config)
}

fn parse_optional_hot_state_config(
    root: &JsonValue,
) -> Result<crate::execution_core::hot_state::HotStateConfig, ConfigError> {
    use crate::execution_core::hot_state::{
        HotStateConfig, HotStateMemoryConfig, LiveCheckpointConfig,
    };

    let Some(object) = root.as_object() else {
        return Ok(HotStateConfig::default());
    };
    let Some(runtime_value) = object.get("runtime") else {
        return Ok(HotStateConfig::default());
    };
    let runtime = expect_object(runtime_value, "merged settings.runtime")?;
    let Some(hot_state_value) = runtime.get("hot_state") else {
        return Ok(HotStateConfig::default());
    };
    let hot_state = expect_object(hot_state_value, "merged settings.runtime.hot_state")?;
    let defaults = HotStateConfig::default();
    let memory = if let Some(memory_value) = hot_state.get("memory") {
        let memory = expect_object(memory_value, "merged settings.runtime.hot_state.memory")?;
        let memory_defaults = HotStateMemoryConfig::default();
        HotStateMemoryConfig {
            ratio: optional_ratio(memory, "ratio", "merged settings.runtime.hot_state.memory")?
                .unwrap_or(memory_defaults.ratio),
            max_bytes: optional_human_bytes(
                memory,
                "max_bytes",
                "merged settings.runtime.hot_state.memory",
            )?,
            reserve_ratio: optional_ratio(
                memory,
                "reserve_ratio",
                "merged settings.runtime.hot_state.memory",
            )?
            .unwrap_or(memory_defaults.reserve_ratio),
            high_watermark: optional_ratio(
                memory,
                "high_watermark",
                "merged settings.runtime.hot_state.memory",
            )?
            .unwrap_or(memory_defaults.high_watermark),
            low_watermark: optional_ratio(
                memory,
                "low_watermark",
                "merged settings.runtime.hot_state.memory",
            )?
            .unwrap_or(memory_defaults.low_watermark),
        }
    } else {
        HotStateMemoryConfig::default()
    };
    let shards = match hot_state.get("shards") {
        Some(JsonValue::String(value)) if value.eq_ignore_ascii_case("auto") => 0,
        Some(_) => optional_usize(hot_state, "shards", "merged settings.runtime.hot_state")?
            .unwrap_or(defaults.shards),
        None => defaults.shards,
    };
    let live_checkpoint = if let Some(value) = hot_state.get("live_checkpoint") {
        let object = expect_object(value, "merged settings.runtime.hot_state.live_checkpoint")?;
        let checkpoint_defaults = LiveCheckpointConfig::default();
        LiveCheckpointConfig {
            min_interval_ms: optional_u64(
                object,
                "min_interval_ms",
                "merged settings.runtime.hot_state.live_checkpoint",
            )?
            .unwrap_or(checkpoint_defaults.min_interval_ms),
            max_revision_gap: optional_u64(
                object,
                "max_revision_gap",
                "merged settings.runtime.hot_state.live_checkpoint",
            )?
            .unwrap_or(checkpoint_defaults.max_revision_gap),
        }
    } else {
        LiveCheckpointConfig::default()
    };
    let config = HotStateConfig {
        memory,
        shards,
        materializer_queue_capacity: optional_usize(
            hot_state,
            "materializer_queue_capacity",
            "merged settings.runtime.hot_state",
        )?
        .unwrap_or(defaults.materializer_queue_capacity),
        live_checkpoint,
    };
    config.validate().map_err(ConfigError::Parse)?;
    Ok(config)
}

fn parse_optional_provider_resource_config(
    root: &JsonValue,
) -> Result<crate::ProviderResourceConfig, ConfigError> {
    let Some(object) = root.as_object() else {
        return Ok(crate::ProviderResourceConfig::default());
    };
    let Some(runtime_value) = object.get("runtime") else {
        return Ok(crate::ProviderResourceConfig::default());
    };
    let runtime = expect_object(runtime_value, "merged settings.runtime")?;
    let Some(resources_value) = runtime.get("resources") else {
        return Ok(crate::ProviderResourceConfig::default());
    };
    let resources = expect_object(resources_value, "merged settings.runtime.resources")?;
    let Some(provider) = resources.get("provider") else {
        return Ok(crate::ProviderResourceConfig::default());
    };
    let config = serde_json::from_str::<crate::ProviderResourceConfig>(&provider.render())
        .map_err(|error| {
            ConfigError::Parse(format!(
                "merged settings.runtime.resources.provider: {error}"
            ))
        })?;
    config.validate().map_err(ConfigError::Parse)?;
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
        }
        DomainProfile::Ops => {
            policy.task.max_failures_before_review = 1;
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

fn optional_ratio(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<f64>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(JsonValue::Number(value)) => Ok(Some(*value as f64)),
        Some(JsonValue::String(value)) => value.parse::<f64>().map(Some).map_err(|_| {
            ConfigError::Parse(format!("{context}: field {key} must be a decimal ratio"))
        }),
        Some(_) => Err(ConfigError::Parse(format!(
            "{context}: field {key} must be a decimal ratio"
        ))),
    }
}

fn optional_human_bytes(
    object: &BTreeMap<String, JsonValue>,
    key: &str,
    context: &str,
) -> Result<Option<u64>, ConfigError> {
    match object.get(key) {
        Some(JsonValue::Null) | None => Ok(None),
        Some(JsonValue::Number(value)) => u64::try_from(*value).map(Some).map_err(|_| {
            ConfigError::Parse(format!("{context}: field {key} must be non-negative"))
        }),
        Some(JsonValue::String(value)) => parse_human_bytes(value)
            .map(Some)
            .map_err(|reason| ConfigError::Parse(format!("{context}: field {key} {reason}"))),
        Some(_) => Err(ConfigError::Parse(format!(
            "{context}: field {key} must be bytes or a human-readable size"
        ))),
    }
}

fn parse_human_bytes(value: &str) -> Result<u64, &'static str> {
    let normalized = value.trim().to_ascii_lowercase();
    let (number, multiplier) = ["gib", "mib", "kib", "gb", "mb", "kb", "b"]
        .into_iter()
        .find_map(|suffix| {
            normalized.strip_suffix(suffix).map(|number| {
                let multiplier = match suffix {
                    "gib" => 1024_u64.pow(3),
                    "mib" => 1024_u64.pow(2),
                    "kib" => 1024,
                    "gb" => 1_000_000_000,
                    "mb" => 1_000_000,
                    "kb" => 1_000,
                    _ => 1,
                };
                (number.trim(), multiplier)
            })
        })
        .unwrap_or((normalized.as_str(), 1));
    number
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .ok_or("is not a valid positive byte size")
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
        parse_optional_context_budget_config, parse_optional_gateway_config,
        parse_optional_hot_state_config, parse_optional_model_context_windows,
        parse_optional_session_history_config, parse_optional_storage_config,
        parse_permission_mode_label, parse_routing_mode, redact_serde_json, ConfigLoader,
        ConfigSource, DomainProfile, McpServerConfig, McpTransport, ProviderProtocol,
        ResolvedPermissionMode, RoutingMode, RuntimeConfig, RuntimeFeatureConfig,
        RuntimeHookConfig, RuntimePluginConfig, SessionCompactConfig, StorageBackendSelection,
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
    fn parses_top_level_workspace_without_reusing_sandbox_policy() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        let workspace = root.join("configured-workspace");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::create_dir_all(&workspace).expect("configured workspace");
        fs::write(
            home.join("config.yaml"),
            format!(
                "workspace: {}\nsandbox:\n  workspace_root: /sandbox-only\n",
                workspace.display()
            ),
        )
        .expect("write workspace config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("workspace config");
        assert_eq!(loaded.workspace(), Some(workspace.as_path()));
        assert_eq!(
            loaded.sandbox().workspace_root.as_deref(),
            Some(std::path::Path::new("/sandbox-only"))
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn rejects_empty_top_level_workspace() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(home.join("config.yaml"), "workspace: '   '\n")
            .expect("write invalid workspace config");

        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("empty workspace must fail");
        assert!(error.to_string().contains("workspace must not be empty"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
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
            r#"{"model":"sonnet","env":{"A2":"1"},"hooks":{"PreToolUse":["base"]},"permissions":{"default_mode":"read-only","allow":["Read"],"deny":["Bash(rm -rf)"]},"mcpServers":{"home":{"command":"uvx","args":["home"]}}}"#,
        )
        .expect("write user settings");
        fs::write(
            cwd.join(".cowd").join("config.yaml"),
            r#"{"model":"project-compat","env":{"B":"2","C":"3"},"hooks":{"PostToolUse":["project"],"PostToolUseFailure":["project-failure"]},"permissions":{"ask":["Edit"]},"mcpServers":{"project":{"command":"uvx","args":["project"]}}}"#,
        )
        .expect("write project settings");
        fs::write(
            cwd.join(".cowd").join("config.local.yaml"),
            r#"{"model":"opus","permissions":{"default_mode":"workspace-write"}}"#,
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
  default_mode: "workspace-write"
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
  profile: autonomous
  low_risk_timeout: pending
"#,
        )
        .expect("write config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(
            loaded.approval().profile,
            harness_contract::policy::ApprovalProfile::Autonomous
        );
        assert_eq!(
            loaded.approval().low_risk_timeout,
            harness_contract::policy::LowRiskTimeoutAction::Pending
        );

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
            r#"{"model":"smart","aliases":{"fast":"claude-haiku-4-5-20251213","smart":"claude-opus-4-6"}}"#,
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
        assert_eq!(
            loaded.resolved_model().as_deref(),
            Some("claude-sonnet-4-6")
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
    fn permission_mode_contract_accepts_only_terminal_values() {
        // given / when / then
        assert_eq!(
            parse_permission_mode_label("read-only", "test").expect("read-only should resolve"),
            ResolvedPermissionMode::ReadOnly
        );
        assert_eq!(
            parse_permission_mode_label("workspace-write", "test")
                .expect("workspace-write should resolve"),
            ResolvedPermissionMode::WorkspaceWrite
        );
        assert_eq!(
            parse_permission_mode_label("danger-full-access", "test")
                .expect("danger-full-access should resolve"),
            ResolvedPermissionMode::DangerFullAccess
        );
        assert!(parse_permission_mode_label("plan", "test").is_err());
        assert!(parse_permission_mode_label("acceptEdits", "test").is_err());
        assert!(parse_permission_mode_label("dontAsk", "test").is_err());
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
    fn app_startup_policy_defaults_to_enabled_and_allows_explicit_disable() {
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"
apps:
  mfg:
    enabled: false
  future_app: {}
"#,
        )
        .expect("write app config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert!(!loaded.apps().is_enabled("mfg"));
        assert!(loaded.apps().is_enabled("future_app"));
        assert!(loaded.apps().is_enabled("unconfigured_app"));
        assert_eq!(
            loaded.apps().configured_app_ids().collect::<Vec<_>>(),
            vec!["future_app", "mfg"]
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
    fn memory_extraction_accepts_snake_case_auto_extract() {
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
  extraction:
    auto_extract: false
"#,
        )
        .expect("write memory config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert!(!loaded.memory().extraction.auto_extract);
        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn memory_governance_is_configurable_and_rejects_invalid_schedule() {
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
  governance:
    enabled: true
    startup_delay_secs: 5
    deep_scan_hour_local: 2
    max_candidates: 96
    stale_threshold_bp: 9900
    low_confidence_threshold_bp: 4000
"#,
        )
        .expect("write memory governance config");

        let loaded = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");
        assert!(loaded.memory().governance.enabled);
        assert_eq!(loaded.memory().governance.startup_delay_secs, 5);
        assert_eq!(loaded.memory().governance.deep_scan_hour_local, 2);
        assert_eq!(loaded.memory().governance.max_candidates, 96);
        assert_eq!(loaded.memory().governance.stale_threshold_bp, 9_900);
        assert_eq!(
            loaded.memory().governance.low_confidence_threshold_bp,
            4_000
        );

        fs::write(
            home.join("config.yaml"),
            "memory:\n  governance:\n    deep_scan_hour_local: 24\n",
        )
        .expect("write invalid memory governance config");
        assert!(ConfigLoader::new(&cwd, &home).load().is_err());
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
    fn provider_max_output_tokens_reads_from_environment_variable() {
        // given — set environment variable
        let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("4096"));

        // when
        let config = crate::ProviderResourceConfig::default();

        // then
        assert_eq!(config.max_output_tokens_override(), Some(4096));
    }

    #[test]
    fn provider_max_output_tokens_falls_back_to_none_when_env_var_is_unset() {
        // given — ensure env var is unset
        let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", None);

        // when
        let config = crate::ProviderResourceConfig::default();

        // then
        assert_eq!(config.max_output_tokens_override(), None);
    }

    #[test]
    fn provider_max_output_tokens_falls_back_to_none_when_env_var_is_invalid() {
        // given — set invalid environment variable
        let _env_lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVarGuard::set("COWD_MAX_OUTPUT_TOKENS", Some("not-a-number"));

        // when
        let config = crate::ProviderResourceConfig::default();

        // then — should fall back to None (not panic)
        assert_eq!(config.max_output_tokens_override(), None);
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
    fn parses_session_history_chunk_and_request_cache_limits() {
        let root = JsonValue::parse(
            r#"{"session_history":{"chunk_messages":64,"chunk_bytes":131072,"request_cache_entries":8}}"#,
        )
        .expect("json should parse");
        let history =
            parse_optional_session_history_config(&root).expect("history config should parse");
        assert_eq!(history.chunk_messages, 64);
        assert_eq!(history.chunk_bytes, 131_072);
        assert_eq!(history.request_cache_entries, 8);

        let invalid =
            JsonValue::parse(r#"{"session_history":{"request_cache_entries":0}}"#).unwrap();
        assert!(parse_optional_session_history_config(&invalid).is_err());
    }

    #[test]
    fn parses_gateway_recovery_working_set_and_rejects_invalid_budgets() {
        let root = JsonValue::parse(
            r#"{"gateway":{"recovery":{
                "hot_bytes":1048576,
                "attached_bytes":262144,
                "recent_bytes":524288,
                "recent_window_ms":90000,
                "manifest_page_size":64,
                "hydrate_concurrency":4,
                "activation_tail_messages":256,
                "activation_metadata_messages":1024,
                "context_card_cache_entries":128,
                "context_index_card_span":64,
                "context_index_parent_span":8,
                "stable_snapshot_attempts":8
            }}}"#,
        )
        .unwrap();
        let gateway = parse_optional_gateway_config(&root).unwrap();
        assert_eq!(gateway.recovery.hot_bytes, 1_048_576);
        assert_eq!(gateway.recovery.recent_window_ms, 90_000);
        assert_eq!(gateway.recovery.activation_tail_messages, 256);

        let invalid =
            JsonValue::parse(r#"{"gateway":{"recovery":{"hot_bytes":1024,"recent_bytes":2048}}}"#)
                .unwrap();
        assert!(parse_optional_gateway_config(&invalid).is_err());
        let invalid_parent =
            JsonValue::parse(r#"{"gateway":{"recovery":{"context_index_parent_span":1}}}"#)
                .unwrap();
        assert!(parse_optional_gateway_config(&invalid_parent).is_err());
    }

    #[test]
    fn parses_gateway_live_limits_and_rejects_unsafe_boundaries() {
        let root = JsonValue::parse(
            r#"{"gateway":{"live":{
                "max_sources":48,
                "max_subscriptions_per_principal_instance":3,
                "queue_capacity":768,
                "checkpoint_max_bytes":8192,
                "default_ttl_seconds":1800,
                "max_ttl_seconds":7200,
                "baseline_timeout_ms":9000
            }}}"#,
        )
        .unwrap();
        let live = parse_optional_gateway_config(&root).unwrap().live;
        assert_eq!(live.max_sources, 48);
        assert_eq!(live.queue_capacity, 768);
        assert_eq!(live.checkpoint_max_bytes, 8_192);
        assert_eq!(live.default_ttl_seconds, 1_800);
        assert_eq!(live.max_ttl_seconds, 7_200);
        assert_eq!(live.baseline_timeout_ms, 9_000);

        let invalid_header =
            JsonValue::parse(r#"{"gateway":{"live":{"checkpoint_max_bytes":512}}}"#).unwrap();
        assert!(parse_optional_gateway_config(&invalid_header).is_err());
        let invalid_ttl = JsonValue::parse(
            r#"{"gateway":{"live":{"default_ttl_seconds":7200,"max_ttl_seconds":3600}}}"#,
        )
        .unwrap();
        assert!(parse_optional_gateway_config(&invalid_ttl).is_err());
    }

    #[test]
    fn parses_gateway_presence_independently_from_live_subscription_ttl() {
        let root = JsonValue::parse(
            r#"{"gateway":{
                "presence":{"ttl_seconds":900},
                "live":{"default_ttl_seconds":1800}
            }}"#,
        )
        .unwrap();
        let gateway = parse_optional_gateway_config(&root).unwrap();
        assert_eq!(gateway.presence.ttl_seconds, 900);
        assert_eq!(gateway.live.default_ttl_seconds, 1_800);

        let invalid = JsonValue::parse(r#"{"gateway":{"presence":{"ttl_seconds":0}}}"#).unwrap();
        assert!(parse_optional_gateway_config(&invalid).is_err());
    }

    #[test]
    fn parses_gateway_translation_policy_and_bounds_cache() {
        let root =
            JsonValue::parse(r#"{"gateway":{"translation":{"model":"fast","cache_entries":512}}}"#)
                .unwrap();
        let translation = parse_optional_gateway_config(&root).unwrap().translation;
        assert_eq!(translation.model.as_deref(), Some("fast"));
        assert_eq!(translation.cache_entries, 512);

        let invalid =
            JsonValue::parse(r#"{"gateway":{"translation":{"cache_entries":4097}}}"#).unwrap();
        assert!(parse_optional_gateway_config(&invalid).is_err());
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

    #[test]
    fn routing_mode_is_pinned_by_default_and_rejects_unknown_values() {
        assert_eq!(
            parse_routing_mode(&JsonValue::parse("{}").unwrap()).unwrap(),
            RoutingMode::Pinned
        );
        assert_eq!(
            parse_routing_mode(&JsonValue::parse(r#"{"routing_mode":"auto"}"#).unwrap()).unwrap(),
            RoutingMode::Auto
        );
        assert!(
            parse_routing_mode(&JsonValue::parse(r#"{"routing_mode":"adaptive"}"#).unwrap())
                .expect_err("unknown routing mode must fail closed")
                .to_string()
                .contains("unsupported routing_mode")
        );
    }

    #[test]
    fn storage_topology_defaults_to_sqlite_and_postgres_is_strict() {
        let defaults = parse_optional_storage_config(&JsonValue::parse("{}").unwrap()).unwrap();
        assert_eq!(defaults.backend, StorageBackendSelection::Auto);
        assert_eq!(defaults.preferred, StorageBackendSelection::Postgres);
        assert_eq!(defaults.fallback, StorageBackendSelection::Sqlite);
        assert!(defaults.postgres.is_none());
        assert!(defaults.session_execution.workers > 0);
        assert!(defaults.session_execution.queue_capacity > 0);
        assert_eq!(defaults.artifacts, crate::ArtifactStorageConfig::default());

        let artifact_override = JsonValue::parse(
            r#"{"storage":{"artifacts":{"compactThresholdBytes":1024,"maxObjectBytes":2048,"totalQuotaBytes":8192,"gcHighWaterBytes":6144,"gcLowWaterBytes":4096,"orphanGraceMs":250}}}"#,
        )
        .unwrap();
        let selected = parse_optional_storage_config(&artifact_override).unwrap();
        assert_eq!(selected.artifacts.compact_threshold_bytes, 1_024);
        assert_eq!(selected.artifacts.max_object_bytes, 2_048);
        assert_eq!(selected.artifacts.total_quota_bytes, 8_192);
        assert_eq!(selected.artifacts.gc_high_water_bytes, 6_144);
        assert_eq!(selected.artifacts.gc_low_water_bytes, 4_096);
        assert_eq!(selected.artifacts.orphan_grace_ms, 250);

        let postgres = JsonValue::parse(
            r#"{"storage":{"backend":"postgres","sessionExecution":{"workers":6,"queueCapacity":72},"postgres":{"logicalIdentity":"cowd-test","secretRef":"env:COWD_TEST_POSTGRES_URL","maxConnections":24,"serverReserve":6,"critical":{"maxConnections":8,"minIdleConnections":2,"checkoutTimeoutMs":250},"onlineRead":{"maxConnections":12,"minIdleConnections":3,"checkoutTimeoutMs":500},"background":{"maxConnections":4,"minIdleConnections":1,"checkoutTimeoutMs":2000}}}}"#,
        )
        .unwrap();
        let selected = parse_optional_storage_config(&postgres).unwrap();
        assert_eq!(selected.backend, StorageBackendSelection::Postgres);
        assert_eq!(selected.session_execution.workers, 6);
        assert_eq!(selected.session_execution.queue_capacity, 72);
        let postgres = selected.postgres.unwrap();
        assert_eq!(postgres.secret_ref, "env:COWD_TEST_POSTGRES_URL");
        assert_eq!(postgres.max_connections, 24);
        assert_eq!(postgres.server_reserve, 6);
        assert_eq!(postgres.critical.max_connections, Some(8));
        assert_eq!(postgres.online_read.max_connections, Some(12));
        assert_eq!(postgres.background.max_connections, Some(4));

        let missing = JsonValue::parse(r#"{"storage":{"backend":"postgres"}}"#).unwrap();
        assert!(parse_optional_storage_config(&missing).is_err());
        let invalid = JsonValue::parse(
            r#"{"storage":{"backend":"postgres","postgres":{"logicalIdentity":"cowd","secretRef":"env:X","maxConnections":0}}}"#,
        )
        .unwrap();
        assert!(parse_optional_storage_config(&invalid).is_err());
        let invalid_execution = JsonValue::parse(
            r#"{"storage":{"sessionExecution":{"workers":0,"queueCapacity":10}}}"#,
        )
        .unwrap();
        assert!(parse_optional_storage_config(&invalid_execution).is_err());
        let invalid_artifacts = JsonValue::parse(
            r#"{"storage":{"artifacts":{"compactThresholdBytes":4096,"maxObjectBytes":1024}}}"#,
        )
        .unwrap();
        assert!(parse_optional_storage_config(&invalid_artifacts).is_err());
    }

    #[test]
    fn auto_storage_backend_parses_with_postgres_preference() {
        let root = JsonValue::parse(
            r#"{"storage":{"backend":"auto","preferred":"postgres","fallback":"sqlite","fallbackProbeTimeoutMs":5000}}"#,
        )
        .unwrap();
        let selected = parse_optional_storage_config(&root).expect("auto storage config");
        assert_eq!(selected.backend, StorageBackendSelection::Auto);
        assert_eq!(selected.preferred, StorageBackendSelection::Postgres);
        assert_eq!(selected.fallback, StorageBackendSelection::Sqlite);
        assert_eq!(selected.fallback_probe_timeout_ms, 5_000);

        let invalid =
            JsonValue::parse(r#"{"storage":{"backend":"auto","preferred":"sqlite"}}"#).unwrap();
        assert!(parse_optional_storage_config(&invalid).is_err());
    }

    #[test]
    fn parses_hot_state_budget_and_rejects_inverted_watermarks() {
        let root = JsonValue::parse(
            r#"{"runtime":{"hot_state":{"memory":{"ratio":"0.70","max_bytes":"512MiB","reserve_ratio":"0.20","high_watermark":"0.90","low_watermark":"0.75"},"shards":8,"materializer_queue_capacity":64}}}"#,
        )
        .unwrap();
        let config = parse_optional_hot_state_config(&root).unwrap();
        assert_eq!(config.memory.ratio, 0.70);
        assert_eq!(config.memory.max_bytes, Some(512 * 1024 * 1024));
        assert_eq!(config.shards, 8);

        let invalid = JsonValue::parse(
            r#"{"runtime":{"hot_state":{"memory":{"low_watermark":"0.95","high_watermark":"0.90"}}}}"#,
        )
        .unwrap();
        assert!(parse_optional_hot_state_config(&invalid).is_err());
    }

    #[test]
    fn network_domain_env_invalid_mode_rejects_startup_fail_closed() {
        let _guard = ENV_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // This test must not mutate process env: ConfigLoader tests run in
        // parallel and an invalid COWD_NETWORK_DOMAIN_MODE would fail them.
        // Startup rejection is covered by the config-file path and by a
        // direct check of the merged-map path below.
        let mode = EnvVarGuard::set("COWD_NETWORK_DOMAIN_MODE", None);
        let allow = EnvVarGuard::set("COWD_NETWORK_DOMAIN_ALLOW", None);
        let block = EnvVarGuard::set("COWD_NETWORK_DOMAIN_BLOCK", None);
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");

        fs::write(
            home.join("config.yaml"),
            "network:\n  domain:\n    mode: denny\n",
        )
        .expect("write invalid config");
        let error = ConfigLoader::new(&cwd, &home)
            .load()
            .expect_err("invalid network mode in config must reject startup");

        assert!(error.to_string().contains("network.domain.mode"));

        let merged =
            JsonValue::parse(r#"{"network":{"domain":{"mode":"denny"}}}"#).expect("merged map");
        let direct = super::inject_network_domain_env(merged.as_object().expect("object"))
            .expect_err("merged env override must also fail closed");
        assert!(direct.to_string().contains("network.domain.mode"));
        drop(mode);
        drop(allow);
        drop(block);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn network_domain_config_is_injected_when_env_is_absent() {
        let _guard = ENV_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mode = EnvVarGuard::set("COWD_NETWORK_DOMAIN_MODE", None);
        let allow = EnvVarGuard::set("COWD_NETWORK_DOMAIN_ALLOW", None);
        let block = EnvVarGuard::set("COWD_NETWORK_DOMAIN_BLOCK", None);
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            r#"network:
  domain:
    mode: deny
    allow:
      - docs.rs
    block:
      - evil.example
"#,
        )
        .expect("write config");

        let _config = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config with network domain should load");

        assert_eq!(
            std::env::var("COWD_NETWORK_DOMAIN_MODE").expect("mode injected"),
            "deny"
        );
        assert_eq!(
            std::env::var("COWD_NETWORK_DOMAIN_ALLOW").expect("allow injected"),
            "docs.rs"
        );
        assert_eq!(
            std::env::var("COWD_NETWORK_DOMAIN_BLOCK").expect("block injected"),
            "evil.example"
        );
        drop(mode);
        drop(allow);
        drop(block);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn network_domain_env_wins_over_config_file() {
        let _guard = ENV_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mode = EnvVarGuard::set("COWD_NETWORK_DOMAIN_MODE", Some("ask"));
        let allow = EnvVarGuard::set("COWD_NETWORK_DOMAIN_ALLOW", None);
        let block = EnvVarGuard::set("COWD_NETWORK_DOMAIN_BLOCK", None);
        let root = temp_dir();
        let cwd = root.join("project");
        let home = root.join("home").join(".cowd");
        fs::create_dir_all(&home).expect("home config dir");
        fs::create_dir_all(&cwd).expect("project dir");
        fs::write(
            home.join("config.yaml"),
            "network:\n  domain:\n    mode: deny\n",
        )
        .expect("write config");

        let _config = ConfigLoader::new(&cwd, &home)
            .load()
            .expect("config should load");

        assert_eq!(
            std::env::var("COWD_NETWORK_DOMAIN_MODE").expect("env preserved"),
            "ask"
        );
        drop(mode);
        drop(allow);
        drop(block);
        let _ = fs::remove_dir_all(&root);
    }
}
