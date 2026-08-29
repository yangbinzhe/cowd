//! Typed Runtime configuration schema and defaults.

use super::*;

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
    8000
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
pub(super) const ENV_OVERRIDE_PREFIX: &str = "COWD_";

/// Schema name advertised by generated settings files.
pub const COWD_SETTINGS_SCHEMA_NAME: &str = "SettingsSchema";

/// Fully merged runtime configuration plus parsed feature-specific views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub(super) merged: BTreeMap<String, JsonValue>,
    pub(super) loaded_entries: Vec<ConfigEntry>,
    pub(super) feature_config: RuntimeFeatureConfig,
}

/// Structured feature configuration consumed by runtime subsystems.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeFeatureConfig {
    pub(super) workspace: Option<PathBuf>,
    pub(super) hooks: RuntimeHookConfig,
    pub(super) plugins: RuntimePluginConfig,
    pub(super) mcp: McpConfigCollection,
    pub(super) oauth: Option<OAuthConfig>,
    pub(super) model: Option<String>,
    pub(super) routing_mode: RoutingMode,
    pub(super) aliases: BTreeMap<String, String>,
    pub(super) model_context_windows: BTreeMap<String, u32>,
    pub(super) permission_mode: Option<ResolvedPermissionMode>,
    pub(super) permission_rules: RuntimePermissionRuleConfig,
    pub(super) approval: ApprovalConfig,
    pub(super) sandbox: SandboxConfig,
    pub(super) fallbacks: Vec<String>,
    pub(super) providers: ProvidersConfig,
    pub(super) trusted_roots: Vec<String>,
    pub(super) memory: MemoryConfig,
    pub(super) context_budget: ContextBudgetConfig,
    pub(super) compression: CompressionConfig,
    pub(super) session_history: crate::SessionHistoryConfig,
    pub(super) gateway: GatewayConfig,
    pub(super) apps: AppsConfig,
    pub(super) storage: StorageTopologyConfig,
    pub(super) gate_auto_fix: GateAutoFixConfig,
    pub(super) runtime_control: RuntimeControlConfig,
    pub(super) hot_state: crate::execution_core::hot_state::HotStateConfig,
    pub(super) provider_resources: crate::ProviderResourceConfig,
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
    pub(super) servers: BTreeMap<String, ScopedMcpServerConfig>,
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

/// Process-wide APP discovery and supervision policy. Bundle contents remain
/// immutable for the lifetime of a Gateway process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppsConfig {
    pub(super) directories: Vec<PathBuf>,
    pub(super) trust_store: Option<PathBuf>,
    pub(super) launcher: Option<AppLauncherConfig>,
    pub(super) runtime_root: PathBuf,
    pub(super) data_root: PathBuf,
    pub(super) core_bridge_socket: PathBuf,
    pub(super) postgres_socket_dirs: Vec<PathBuf>,
    pub(super) cgroup_root: Option<PathBuf>,
    pub(super) resources: AppWorkerResourcesConfig,
    pub(super) supervisor: AppSupervisorConfig,
    pub(super) entries: BTreeMap<String, AppStartupConfig>,
}

impl Default for AppsConfig {
    fn default() -> Self {
        Self {
            directories: vec![crate::cowd_dirs::install_root_dir().join("apps")],
            trust_store: None,
            launcher: None,
            runtime_root: crate::cowd_dirs::config_home_dir().join("app-runtime"),
            data_root: crate::cowd_dirs::config_home_dir().join("app-data"),
            core_bridge_socket: crate::cowd_dirs::config_home_dir().join("core-bridge.sock"),
            postgres_socket_dirs: Vec::new(),
            cgroup_root: None,
            resources: AppWorkerResourcesConfig::default(),
            supervisor: AppSupervisorConfig::default(),
            entries: BTreeMap::new(),
        }
    }
}

impl AppsConfig {
    #[must_use]
    pub fn with_app_enabled(mut self, app_id: impl Into<String>, enabled: bool) -> Self {
        self.entries.insert(
            app_id.into(),
            AppStartupConfig {
                enabled,
                ..AppStartupConfig::default()
            },
        );
        self
    }

    #[must_use]
    pub fn directories(&self) -> &[PathBuf] {
        &self.directories
    }

    #[must_use]
    pub fn trust_store(&self) -> Option<&Path> {
        self.trust_store.as_deref()
    }
    #[must_use]
    pub fn launcher(&self) -> Option<&AppLauncherConfig> {
        self.launcher.as_ref()
    }
    #[must_use]
    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }
    #[must_use]
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }
    #[must_use]
    pub fn core_bridge_socket(&self) -> &Path {
        &self.core_bridge_socket
    }
    #[must_use]
    pub fn postgres_socket_dirs(&self) -> &[PathBuf] {
        &self.postgres_socket_dirs
    }
    #[must_use]
    pub fn cgroup_root(&self) -> Option<&Path> {
        self.cgroup_root.as_deref()
    }
    #[must_use]
    pub fn resources(&self) -> &AppWorkerResourcesConfig {
        &self.resources
    }

    #[must_use]
    pub fn supervisor(&self) -> &AppSupervisorConfig {
        &self.supervisor
    }

    #[must_use]
    pub fn entry(&self, app_id: &str) -> AppStartupConfig {
        self.entries.get(app_id).cloned().unwrap_or_default()
    }

    /// A discovered signed bundle is admitted unless startup policy disables it.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppLauncherConfig {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppWorkerResourcesConfig {
    pub nofile: u64,
    pub nproc: u64,
    pub address_space_bytes: u64,
    pub cpu_seconds: u64,
    pub file_size_bytes: u64,
    pub cgroup_memory_bytes: u64,
    pub cgroup_pids: u64,
    pub cgroup_cpu_quota_us: u64,
    pub cgroup_cpu_period_us: u64,
}

impl Default for AppWorkerResourcesConfig {
    fn default() -> Self {
        Self {
            nofile: 256,
            nproc: 4096,
            address_space_bytes: 512 * 1024 * 1024,
            cpu_seconds: 300,
            file_size_bytes: 16 * 1024 * 1024,
            cgroup_memory_bytes: 512 * 1024 * 1024,
            cgroup_pids: 64,
            cgroup_cpu_quota_us: 100_000,
            cgroup_cpu_period_us: 100_000,
        }
    }
}

/// Per-APP startup policy.  Surface visibility deliberately does not live
/// here: a registered APP has one Gateway truth, and TUI/WebUI derive their
/// visible contributions from that truth instead of maintaining duplicate
/// surface switches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStartupConfig {
    pub enabled: bool,
    pub required: bool,
    pub activation: AppActivationPolicyV1,
    pub config_file: Option<PathBuf>,
}

impl Default for AppStartupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            required: false,
            activation: AppActivationPolicyV1::Lazy,
            config_file: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppSupervisorConfig {
    pub max_active_workers: usize,
    pub max_starting_workers: usize,
    pub activation_timeout_ms: u64,
    pub handshake_timeout_ms: u64,
    pub graceful_shutdown_ms: u64,
    pub idle_ttl_seconds: Option<u64>,
    pub max_waiters_per_app: usize,
    pub restart_window_seconds: u64,
    pub max_restarts_per_window: usize,
}

impl Default for AppSupervisorConfig {
    fn default() -> Self {
        Self {
            max_active_workers: 16,
            max_starting_workers: 4,
            activation_timeout_ms: 10_000,
            handshake_timeout_ms: 3_000,
            graceful_shutdown_ms: 5_000,
            idle_ttl_seconds: Some(300),
            max_waiters_per_app: 256,
            restart_window_seconds: 60,
            max_restarts_per_window: 5,
        }
    }
}

/// Multi-platform gateway configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            webui_dir: Some(
                crate::cowd_dirs::install_root_dir()
                    .join("webui")
                    .join("dist"),
            ),
            platforms: Vec::new(),
            session_reset: SessionResetPolicy::default(),
            capacity: GatewayCapacityConfig::default(),
            recovery: SessionRecoveryConfig::default(),
            presence: GatewayPresenceConfig::default(),
            live: GatewayLiveConfig::default(),
            translation: GatewayTranslationConfig::default(),
        }
    }
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
