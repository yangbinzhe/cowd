use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use cowd_app_protocol::{AppActivationPolicyV1, AppId};
use serde::{Deserialize, Serialize};

use crate::json::JsonValue;
use crate::runtime_control::RuntimeControlPolicy;
use crate::sandbox::{FilesystemIsolationMode, SandboxConfig};
use model_protocol::model_registry::ModelResolver;
pub use model_protocol::oauth::OAuthConfig;
pub use model_protocol::provider_config::{
    ParallelToolCallsMode, ProviderConfig, ProviderProtocol, ProvidersConfig,
};

#[path = "config/schema.rs"]
mod schema;
pub use schema::*;

#[path = "config/load.rs"]
mod load;
pub use load::*;

#[path = "config/validate.rs"]
mod validate;
use validate::*;

#[cfg(test)]
#[path = "config/tests.rs"]
mod tests;

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
            "trust-all" | "trust_all" => Ok(harness_contract::policy::ApprovalProfile::TrustAll),
            other => Err(ConfigError::Invalid {
                key: "approval.profile".to_string(),
                message: format!(
                    "unsupported value `{other}`; expected supervised, balanced, autonomous, or trust-all"
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
///   qwen-tokenplan:
///     baseUrl: "https://configured-provider.example/v1"
///     apiKey: "env:COWD_PROVIDER_API_KEY"
///     models:
///       - "configured-qwen-model"
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
        let max_input_tokens =
            optional_usize(v, "max_input_tokens", "merged settings.memory.vector")?.or(
                optional_usize(v, "maxInputTokens", "merged settings.memory.vector")?,
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
            max_input_tokens: max_input_tokens.unwrap_or(defaults.max_input_tokens),
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
    let webui_dir = optional_string_dual(gw, "webui_dir", "merged settings.gateway")?
        .map(PathBuf::from)
        .or_else(|| GatewayConfig::default().webui_dir);
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
    reject_unknown_keys(
        apps,
        &[
            "directories",
            "trust_store",
            "launcher",
            "runtime_root",
            "data_root",
            "core_bridge_socket",
            "postgres_socket_dirs",
            "cgroup_root",
            "resources",
            "supervisor",
            "entries",
        ],
        "merged settings.apps",
    )?;

    let directories = match apps.get("directories") {
        Some(value) => {
            let values = expect_array(value, "merged settings.apps.directories")?;
            if values.is_empty() {
                return Err(ConfigError::Parse(
                    "merged settings.apps.directories must not be empty".to_string(),
                ));
            }
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|path| !path.trim().is_empty())
                        .map(PathBuf::from)
                        .ok_or_else(|| {
                            ConfigError::Parse(
                                "merged settings.apps.directories must contain non-empty paths"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        None => AppsConfig::default().directories,
    };
    let path_value = |key: &str, default: PathBuf| -> Result<PathBuf, ConfigError> {
        Ok(optional_string(apps, key, "merged settings.apps")?
            .map(PathBuf::from)
            .unwrap_or(default))
    };
    let defaults = AppsConfig::default();
    let trust_store =
        optional_string(apps, "trust_store", "merged settings.apps")?.map(PathBuf::from);
    let launcher = if let Some(value) = apps.get("launcher") {
        let context = "merged settings.apps.launcher";
        let object = expect_object(value, context)?;
        reject_unknown_keys(object, &["path", "sha256"], context)?;
        let path = optional_string(object, "path", context)?
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| ConfigError::Parse(format!("{context}.path is required")))?;
        let sha256 = optional_string(object, "sha256", context)?
            .filter(|v| {
                v.len() == 71
                    && v.starts_with("sha256:")
                    && v[7..]
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            })
            .ok_or_else(|| {
                ConfigError::Parse(format!(
                    "{context}.sha256 must be canonical sha256:<64 lowercase hex>"
                ))
            })?;
        Some(AppLauncherConfig {
            path: PathBuf::from(path),
            sha256: sha256.to_owned(),
        })
    } else {
        None
    };
    let runtime_root = path_value("runtime_root", defaults.runtime_root)?;
    let data_root = path_value("data_root", defaults.data_root)?;
    let core_bridge_socket = path_value("core_bridge_socket", defaults.core_bridge_socket)?;
    let postgres_socket_dirs = match apps.get("postgres_socket_dirs") {
        Some(value) => {
            let values = expect_array(value, "merged settings.apps.postgres_socket_dirs")?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|path| !path.trim().is_empty())
                        .map(PathBuf::from)
                        .ok_or_else(|| {
                            ConfigError::Parse(
                                "merged settings.apps.postgres_socket_dirs must contain non-empty paths"
                                    .to_owned(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        None => defaults.postgres_socket_dirs,
    };
    let cgroup_root =
        optional_string(apps, "cgroup_root", "merged settings.apps")?.map(PathBuf::from);
    let mut resources = AppWorkerResourcesConfig::default();
    if let Some(value) = apps.get("resources") {
        let context = "merged settings.apps.resources";
        let object = expect_object(value, context)?;
        reject_unknown_keys(
            object,
            &[
                "nofile",
                "nproc",
                "address_space_bytes",
                "cpu_seconds",
                "file_size_bytes",
                "cgroup_memory_bytes",
                "cgroup_pids",
                "cgroup_cpu_quota_us",
                "cgroup_cpu_period_us",
            ],
            context,
        )?;
        resources.nofile = optional_u64(object, "nofile", context)?.unwrap_or(resources.nofile);
        resources.nproc = optional_u64(object, "nproc", context)?.unwrap_or(resources.nproc);
        resources.address_space_bytes = optional_u64(object, "address_space_bytes", context)?
            .unwrap_or(resources.address_space_bytes);
        resources.cpu_seconds =
            optional_u64(object, "cpu_seconds", context)?.unwrap_or(resources.cpu_seconds);
        resources.file_size_bytes =
            optional_u64(object, "file_size_bytes", context)?.unwrap_or(resources.file_size_bytes);
        resources.cgroup_memory_bytes = optional_u64(object, "cgroup_memory_bytes", context)?
            .unwrap_or(resources.cgroup_memory_bytes);
        resources.cgroup_pids =
            optional_u64(object, "cgroup_pids", context)?.unwrap_or(resources.cgroup_pids);
        resources.cgroup_cpu_quota_us = optional_u64(object, "cgroup_cpu_quota_us", context)?
            .unwrap_or(resources.cgroup_cpu_quota_us);
        resources.cgroup_cpu_period_us = optional_u64(object, "cgroup_cpu_period_us", context)?
            .unwrap_or(resources.cgroup_cpu_period_us);
        if [
            resources.nofile,
            resources.nproc,
            resources.address_space_bytes,
            resources.cpu_seconds,
            resources.file_size_bytes,
            resources.cgroup_memory_bytes,
            resources.cgroup_pids,
            resources.cgroup_cpu_quota_us,
            resources.cgroup_cpu_period_us,
        ]
        .contains(&0)
        {
            return Err(ConfigError::Parse(format!(
                "{context} values must be positive"
            )));
        }
    }

    let mut supervisor = AppSupervisorConfig::default();
    if let Some(value) = apps.get("supervisor") {
        let context = "merged settings.apps.supervisor";
        let object = expect_object(value, context)?;
        reject_unknown_keys(
            object,
            &[
                "max_active_workers",
                "max_starting_workers",
                "activation_timeout_ms",
                "handshake_timeout_ms",
                "graceful_shutdown_ms",
                "idle_ttl_seconds",
                "max_waiters_per_app",
                "restart_window_seconds",
                "max_restarts_per_window",
            ],
            context,
        )?;
        supervisor.max_active_workers = optional_usize(object, "max_active_workers", context)?
            .unwrap_or(supervisor.max_active_workers);
        supervisor.max_starting_workers = optional_usize(object, "max_starting_workers", context)?
            .unwrap_or(supervisor.max_starting_workers);
        supervisor.activation_timeout_ms = optional_u64(object, "activation_timeout_ms", context)?
            .unwrap_or(supervisor.activation_timeout_ms);
        supervisor.handshake_timeout_ms = optional_u64(object, "handshake_timeout_ms", context)?
            .unwrap_or(supervisor.handshake_timeout_ms);
        supervisor.graceful_shutdown_ms = optional_u64(object, "graceful_shutdown_ms", context)?
            .unwrap_or(supervisor.graceful_shutdown_ms);
        if object.contains_key("idle_ttl_seconds") {
            supervisor.idle_ttl_seconds = optional_u64(object, "idle_ttl_seconds", context)?;
        }
        supervisor.max_waiters_per_app = optional_usize(object, "max_waiters_per_app", context)?
            .unwrap_or(supervisor.max_waiters_per_app);
        supervisor.restart_window_seconds =
            optional_u64(object, "restart_window_seconds", context)?
                .unwrap_or(supervisor.restart_window_seconds);
        supervisor.max_restarts_per_window =
            optional_usize(object, "max_restarts_per_window", context)?
                .unwrap_or(supervisor.max_restarts_per_window);
        if supervisor.max_active_workers == 0
            || supervisor.max_starting_workers == 0
            || supervisor.max_starting_workers > supervisor.max_active_workers
            || supervisor.activation_timeout_ms == 0
            || supervisor.handshake_timeout_ms == 0
            || supervisor.graceful_shutdown_ms == 0
            || supervisor.idle_ttl_seconds == Some(0)
            || supervisor.max_waiters_per_app == 0
            || supervisor.restart_window_seconds == 0
            || supervisor.max_restarts_per_window == 0
        {
            return Err(ConfigError::Parse(format!(
                "{context} requires positive limits and timeouts, max_starting_workers <= max_active_workers, and idle_ttl_seconds null or positive"
            )));
        }
    }

    let mut entries = BTreeMap::new();
    let configured_entries = match apps.get("entries") {
        Some(value) => expect_object(value, "merged settings.apps.entries")?,
        None => {
            return Ok(AppsConfig {
                directories,
                trust_store,
                launcher,
                runtime_root,
                data_root,
                core_bridge_socket,
                postgres_socket_dirs,
                cgroup_root,
                resources,
                supervisor,
                entries,
            })
        }
    };
    for (app_id, value) in configured_entries {
        AppId(app_id.clone()).validate_value().map_err(|error| {
            ConfigError::Parse(format!("merged settings.apps.entries.{app_id}: {error}"))
        })?;
        let context = format!("merged settings.apps.entries.{app_id}");
        let entry = expect_object(value, &context)?;
        reject_unknown_keys(
            entry,
            &["enabled", "required", "activation", "config_file"],
            &context,
        )?;
        let enabled = optional_bool(entry, "enabled", &context)?.unwrap_or(true);
        let required = optional_bool(entry, "required", &context)?.unwrap_or(false);
        let activation = match optional_string(entry, "activation", &context)?.unwrap_or("lazy") {
            "lazy" => AppActivationPolicyV1::Lazy,
            "resident" => AppActivationPolicyV1::Resident,
            value => {
                return Err(ConfigError::Parse(format!(
                    "{context}.activation must be lazy or resident, got {value}"
                )))
            }
        };
        let config_file = optional_string(entry, "config_file", &context)?.map(PathBuf::from);
        entries.insert(
            app_id.clone(),
            AppStartupConfig {
                enabled,
                required,
                activation,
                config_file,
            },
        );
    }
    Ok(AppsConfig {
        directories,
        trust_store,
        launcher,
        runtime_root,
        data_root,
        core_bridge_socket,
        postgres_socket_dirs,
        cgroup_root,
        resources,
        supervisor,
        entries,
    })
}

fn reject_unknown_keys(
    object: &BTreeMap<String, JsonValue>,
    allowed: &[&str],
    context: &str,
) -> Result<(), ConfigError> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(ConfigError::Parse(format!(
            "{context}: unsupported field {key}"
        )));
    }
    Ok(())
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
    if let Some(capacity_value) = control.get("capacity") {
        let capacity = expect_object(capacity_value, "merged settings.runtime.control.capacity")?;
        if let Some(profile_id) = optional_string(
            capacity,
            "profile_id",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.profile_id = profile_id.to_string();
        }
        if let Some(revision) = optional_u64(
            capacity,
            "revision",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.revision = revision;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_program_teams",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_program_teams = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_team_roles",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_team_roles = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_role_instances_per_team",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_role_instances_per_team = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_agent_nodes_per_team",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_agent_nodes_per_team = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_pending_instance",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_pending_instance = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_pending_per_class",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_pending_per_class = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_pending_per_key",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_pending_per_key = value;
        }
        if let Some(value) = optional_u64(
            capacity,
            "admission_aging_interval_ms",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.admission_aging_interval_ms = value;
        }
        if let Some(value) = optional_u64(
            capacity,
            "user_team_veto_window_ms",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.user_team_veto_window_ms = value;
        }
        if let Some(value) = optional_usize(
            capacity,
            "max_semantic_revisions_per_turn",
            "merged settings.runtime.control.capacity",
        )? {
            config.policy.capacity.max_semantic_revisions_per_turn = value;
        }
        config
            .policy
            .capacity
            .validate()
            .map_err(|message| ConfigError::Invalid {
                key: "merged settings.runtime.control.capacity".to_string(),
                message,
            })?;
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
