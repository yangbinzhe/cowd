use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    fs,
    hash::{Hash, Hasher},
    path::Path,
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use runtime::{init_global_providers, ConfigLoader, RuntimeConfig};
use serde::Serialize;
use serde_json::Value;

use crate::api_routes::AppState;

static CONFIG_RELOAD_STATUS: OnceLock<RwLock<ConfigReloadSnapshot>> = OnceLock::new();
static CONFIG_RELOAD_APPLY_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Serialize)]
struct ConfigFingerprintEntry {
    source: String,
    path: String,
    exists: bool,
    modified_ms: Option<u64>,
    len: Option<u64>,
    content_hash: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct ConfigFingerprint {
    digest: String,
    entries: Vec<ConfigFingerprintEntry>,
    computed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ConfigReloadSnapshot {
    kind: &'static str,
    status: String,
    applied: bool,
    trigger: String,
    last_checked_at_ms: u64,
    last_changed_at_ms: Option<u64>,
    last_applied_at_ms: Option<u64>,
    last_seen_digest: Option<String>,
    last_applied_digest: Option<String>,
    last_error: Option<String>,
    fingerprint: Option<ConfigFingerprint>,
    restart_required: Value,
    warnings: Vec<String>,
    last_report: Value,
}

impl Default for ConfigReloadSnapshot {
    fn default() -> Self {
        Self {
            kind: "gateway.config.reload.status",
            status: "uninitialized".to_string(),
            applied: false,
            trigger: "startup".to_string(),
            last_checked_at_ms: now_ms(),
            last_changed_at_ms: None,
            last_applied_at_ms: None,
            last_seen_digest: None,
            last_applied_digest: None,
            last_error: None,
            fingerprint: None,
            restart_required: serde_json::json!({
                "required": false,
                "unknown": true,
                "fields": [],
                "reason": "config reload status has not been initialized",
            }),
            warnings: Vec::new(),
            last_report: serde_json::json!({}),
        }
    }
}

pub(crate) fn initialize_config_reload_status(state: &Arc<AppState>) {
    let fingerprint = config_fingerprint(&state.workspace_root, &state.config_home);
    write_status(ConfigReloadSnapshot {
        status: "pending_initial_check".to_string(),
        trigger: "startup".to_string(),
        last_checked_at_ms: now_ms(),
        fingerprint: Some(fingerprint),
        restart_required: serde_json::json!({
            "required": false,
            "unknown": true,
            "fields": [],
            "reason": "initial config check has not run yet",
        }),
        last_report: serde_json::json!({
            "kind": "gateway.config.reload",
            "status": "pending_initial_check",
            "applied": false,
        }),
        ..ConfigReloadSnapshot::default()
    });
}

pub(crate) fn spawn_config_reload_watcher(
    state: Arc<AppState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        poll_config_reload_once(state.clone(), "initial").await;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            poll_config_reload_once(state.clone(), "auto").await;
        }
    })
}

pub(crate) fn status_value() -> Value {
    serde_json::to_value(read_status()).unwrap_or_else(|_| {
        serde_json::json!({
            "kind": "gateway.config.reload.status",
            "status": "unavailable",
            "applied": false,
        })
    })
}

pub(crate) async fn poll_config_reload_once(state: Arc<AppState>, trigger: &str) -> Value {
    let fingerprint = config_fingerprint(&state.workspace_root, &state.config_home);
    let digest = fingerprint.digest.clone();
    let previous = read_status();
    if previous.last_seen_digest.as_deref() == Some(digest.as_str()) {
        let mut next = previous;
        next.last_checked_at_ms = now_ms();
        next.fingerprint = Some(fingerprint);
        write_status(next);
        return status_value();
    }
    reload_gateway_config_with_fingerprint(&state, trigger, fingerprint).await
}

pub(crate) async fn force_gateway_config_reload(state: &Arc<AppState>, trigger: &str) -> Value {
    let fingerprint = config_fingerprint(&state.workspace_root, &state.config_home);
    reload_gateway_config_with_fingerprint(state, trigger, fingerprint).await
}

pub(crate) async fn reload_runtime_providers_from_disk(state: &Arc<AppState>) -> Value {
    let _guard = apply_lock().lock().await;
    match ConfigLoader::new(&state.workspace_root, &state.config_home).load() {
        Ok(runtime_config) => apply_runtime_providers(&runtime_config),
        Err(error) => {
            let message = format!("failed to load runtime config: {error}");
            tracing::warn!(
                target: "cowd.runtime.provider",
                applied = false,
                error = %error,
                "runtime providers reload skipped"
            );
            serde_json::json!({
                "kind": "runtime_provider_reload",
                "status": "failed",
                "applied": false,
                "source": "error",
                "provider_count": 0,
                "provider_model_count": 0,
                "provider_names": [],
                "configured_model": null,
                "configured_model_provider": null,
                "configured_model_resolved": false,
                "warnings": [message]
            })
        }
    }
}

async fn reload_gateway_config_with_fingerprint(
    state: &Arc<AppState>,
    trigger: &str,
    fingerprint: ConfigFingerprint,
) -> Value {
    let _guard = apply_lock().lock().await;
    let changed_at = now_ms();
    let previous = read_status();
    let loaded = ConfigLoader::new(&state.workspace_root, &state.config_home).load();
    let runtime_config = match loaded {
        Ok(config) => config,
        Err(error) => {
            let message = format!("failed to load runtime config: {error}");
            let report = serde_json::json!({
                "kind": "gateway.config.reload",
                "status": "invalid",
                "applied": false,
                "trigger": trigger,
                "source": "error",
                "config_entries": fingerprint.entries,
                "fingerprint": fingerprint.digest,
                "previous_applied_digest": previous.last_applied_digest,
                "warnings": [message.clone()],
            });
            write_status(ConfigReloadSnapshot {
                status: "invalid".to_string(),
                applied: false,
                trigger: trigger.to_string(),
                last_checked_at_ms: changed_at,
                last_changed_at_ms: Some(changed_at),
                last_applied_at_ms: previous.last_applied_at_ms,
                last_seen_digest: Some(fingerprint.digest.clone()),
                last_applied_digest: previous.last_applied_digest,
                last_error: Some(message.clone()),
                fingerprint: Some(fingerprint),
                restart_required: previous.restart_required,
                warnings: vec![message],
                last_report: report.clone(),
                ..ConfigReloadSnapshot::default()
            });
            tracing::warn!(
                target: "cowd.gateway.config_reload",
                trigger,
                applied = false,
                "gateway config reload rejected because config validation failed"
            );
            return report;
        }
    };

    let report = apply_runtime_config(state, &runtime_config, trigger, &fingerprint).await;
    let status = report
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("applied")
        .to_string();
    let warnings = report
        .get("warnings")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    write_status(ConfigReloadSnapshot {
        status,
        applied: true,
        trigger: trigger.to_string(),
        last_checked_at_ms: changed_at,
        last_changed_at_ms: Some(changed_at),
        last_applied_at_ms: Some(changed_at),
        last_seen_digest: Some(fingerprint.digest.clone()),
        last_applied_digest: Some(fingerprint.digest.clone()),
        last_error: None,
        fingerprint: Some(fingerprint),
        restart_required: report
            .get("restart_required")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({ "required": false, "fields": [] })),
        warnings,
        last_report: report.clone(),
        ..ConfigReloadSnapshot::default()
    });
    report
}

async fn apply_runtime_config(
    state: &Arc<AppState>,
    runtime_config: &RuntimeConfig,
    trigger: &str,
    fingerprint: &ConfigFingerprint,
) -> Value {
    let config_json = runtime_config_json_value(runtime_config);
    let provider_report = apply_runtime_providers(runtime_config);
    let mcp_report =
        match crate::runtime_host::install_runtime_mcp_service_from_config(runtime_config).await {
            Ok(report) => report,
            Err(error) => serde_json::json!({
                "ok": false,
                "status": "failed",
                "error": error,
            }),
        };

    let surface_configs = build_surface_runtime_configs(runtime_config.gateway());
    let surface_config_count = surface_configs.len();
    state.services.surface.set_configs(surface_configs);

    let static_webui = crate::gateway_static::resolve_static_webui_source(
        runtime_config.gateway().webui_dir.as_deref(),
    );
    if static_webui.available {
        state
            .services
            .surface
            .set_webui_static_resource(static_webui.configured_path.as_deref());
    } else {
        state.services.surface.set_webui_static_resource(None);
    }

    let edge_discovery = state.services.surface.reload_manifests().await;
    let restart_required = restart_required_report(state.startup_config_snapshot(), &config_json);
    let warnings = reload_warnings(
        &provider_report,
        &mcp_report,
        &static_webui,
        &restart_required,
    );
    let status = if restart_required
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "reload_needed"
    } else if warnings.is_empty() {
        "applied"
    } else {
        "attention"
    };

    tracing::info!(
        target: "cowd.gateway.config_reload",
        trigger,
        status,
        applied = true,
        "gateway config reload applied"
    );

    serde_json::json!({
        "kind": "gateway.config.reload",
        "status": status,
        "applied": true,
        "trigger": trigger,
        "source": if runtime_config.loaded_entries().is_empty() { "default" } else { "config" },
        "config_entries": runtime_config.loaded_entries(),
        "fingerprint": fingerprint.digest,
        "applied_sections": {
            "providers": provider_report,
            "mcp": mcp_report,
            "surface_runtime_configs": {
                "status": "applied",
                "count": surface_config_count,
            },
            "static_webui": static_webui,
            "edge_registry": edge_discovery,
        },
        "restart_required": restart_required,
        "warnings": warnings,
    })
}

pub(crate) fn apply_runtime_providers(runtime_config: &RuntimeConfig) -> Value {
    let source = if runtime_config.loaded_entries().is_empty() {
        "default"
    } else {
        "config"
    };
    let providers = runtime_config.providers().clone();
    let registry = model_protocol::model_registry::ModelRegistry::load()
        .unwrap_or_else(|_| model_protocol::model_registry::ModelRegistry::empty());
    let catalog = provider::ProviderCatalog::from_input(provider::ProviderCatalogInput {
        providers: &providers,
        registry: &registry,
        configured_model: runtime_config.model(),
        aliases: runtime_config.aliases(),
        config_source: source,
        extra_sources: Vec::new(),
        transforms: Vec::new(),
        warnings: Vec::new(),
    });
    let catalog_generation = catalog.generation.clone();
    let catalog_updated = now_ms();
    let provider_count = providers.providers.len();
    let provider_model_count: usize = providers
        .providers
        .values()
        .map(|provider| provider.models.len())
        .sum();
    let configured_model = runtime_config.model().map(str::to_string);
    let configured_model_provider = configured_model
        .as_deref()
        .and_then(|model| providers.resolve_full(model))
        .map(|provider| provider.name.clone());
    let configured_model_resolved =
        configured_model.is_none() || configured_model_provider.is_some();
    let mut provider_names: Vec<String> = providers.providers.keys().cloned().collect();
    provider_names.sort();

    init_global_providers(providers);

    tracing::info!(
        target: "cowd.runtime.provider",
        applied = true,
        source,
        provider_count,
        provider_model_count,
        configured_model = configured_model.as_deref().unwrap_or(""),
        configured_model_provider = configured_model_provider.as_deref().unwrap_or(""),
        configured_model_resolved,
        "runtime providers reloaded"
    );

    serde_json::json!({
        "kind": "runtime_provider_reload",
        "status": if provider_count == 0 { "unconfigured" } else if configured_model_resolved { "applied" } else { "attention" },
        "applied": true,
        "source": source,
        "catalog_generation": catalog_generation,
        "catalog_updated": catalog_updated,
        "catalog": {
            "generation": catalog.generation,
            "sources": catalog.sources,
            "transforms": catalog.transforms,
            "provider_count": catalog.providers.len(),
            "model_count": catalog.models.len(),
            "profile_count": catalog.profiles.len(),
            "warnings": catalog.warnings,
        },
        "provider_count": provider_count,
        "provider_model_count": provider_model_count,
        "provider_names": provider_names,
        "configured_model": configured_model,
        "configured_model_provider": configured_model_provider,
        "configured_model_resolved": configured_model_resolved,
        "warnings": if provider_count == 0 {
            serde_json::json!(["no runtime providers are configured"])
        } else if !configured_model_resolved {
            serde_json::json!(["configured default model is not declared by any provider"])
        } else {
            serde_json::json!([])
        }
    })
}

fn build_surface_runtime_configs(gw: &runtime::GatewayConfig) -> BTreeMap<String, Value> {
    if !gw.enabled {
        return BTreeMap::new();
    }
    gw.platforms
        .iter()
        .filter(|platform| platform.enabled && platform.platform_type != "api_server")
        .map(|platform| {
            let id = surface::normalize_surface_id(&platform.platform_type);
            let mut config = platform
                .extra
                .iter()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "platformType" | "platform_type" | "type" | "enabled"
                    )
                })
                .map(|(key, value)| (key.clone(), runtime_json_value_to_serde(value)))
                .collect::<serde_json::Map<_, _>>();
            config.insert(
                "platform_type".to_string(),
                Value::String(platform.platform_type.clone()),
            );
            (id, Value::Object(config))
        })
        .collect()
}

fn runtime_config_json_value(config: &RuntimeConfig) -> Value {
    runtime_json_value_to_serde(&config.as_json())
}

fn runtime_json_value_to_serde(value: &runtime::JsonValue) -> Value {
    match value {
        runtime::JsonValue::Null => Value::Null,
        runtime::JsonValue::Bool(value) => Value::Bool(*value),
        runtime::JsonValue::Number(value) => serde_json::json!(*value),
        runtime::JsonValue::String(value) => Value::String(value.clone()),
        runtime::JsonValue::Array(values) => {
            Value::Array(values.iter().map(runtime_json_value_to_serde).collect())
        }
        runtime::JsonValue::Object(entries) => Value::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), runtime_json_value_to_serde(value)))
                .collect(),
        ),
    }
}

fn restart_required_report(startup_config: Option<&Value>, current_config: &Value) -> Value {
    let Some(startup_config) = startup_config else {
        return serde_json::json!({
            "required": false,
            "unknown": true,
            "fields": [],
            "reason": "startup config snapshot unavailable; reloadable sections were applied explicitly",
        });
    };
    let mut fields = Vec::new();
    if api_server_addr(startup_config) != api_server_addr(current_config) {
        fields.push("gateway.platforms.api_server.host_or_port");
    }
    if api_server_auth_token(startup_config) != api_server_auth_token(current_config) {
        fields.push("gateway.platforms.api_server.auth_token");
    }
    if api_server_cors_origins(startup_config) != api_server_cors_origins(current_config) {
        fields.push("gateway.platforms.api_server.cors_origins");
    }
    if startup_config.get("memory") != current_config.get("memory") {
        fields.push("memory");
    }
    serde_json::json!({
        "required": !fields.is_empty(),
        "unknown": false,
        "fields": fields,
        "reason": if fields.is_empty() {
            "all changed sections are covered by hot reload"
        } else {
            "these sections require listener/router/runtime reconstruction"
        },
    })
}

fn reload_warnings(
    provider_report: &Value,
    mcp_report: &Value,
    static_webui: &crate::gateway_static::StaticWebUiSource,
    restart_required: &Value,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if provider_report
        .get("warnings")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty())
    {
        warnings.push("provider reload completed with warnings".to_string());
    }
    if mcp_report.get("status").and_then(Value::as_str) == Some("failed") {
        warnings.push("MCP service reload failed".to_string());
    }
    if !static_webui.available && static_webui.configured_path.is_some() {
        warnings.push("gateway.webui_dir is configured but index.html is missing".to_string());
    }
    if restart_required
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        warnings.push("some config changes require gateway restart".to_string());
    }
    warnings
}

fn api_server_platform(config: &Value) -> Option<&Value> {
    config
        .get("gateway")
        .and_then(|gateway| gateway.get("platforms"))
        .and_then(Value::as_array)
        .and_then(|platforms| {
            platforms.iter().find(|platform| {
                platform
                    .get("platformType")
                    .or_else(|| platform.get("platform_type"))
                    .or_else(|| platform.get("type"))
                    .and_then(Value::as_str)
                    == Some("api_server")
            })
        })
}

fn api_server_addr(config: &Value) -> Option<String> {
    let platform = api_server_platform(config)?;
    let host = platform
        .get("host")
        .and_then(Value::as_str)
        .unwrap_or("127.0.0.1");
    let port = platform.get("port").and_then(Value::as_i64).unwrap_or(8642);
    Some(format!("{host}:{port}"))
}

fn api_server_auth_token(config: &Value) -> Option<String> {
    api_server_platform(config)
        .and_then(|platform| {
            platform
                .get("auth_token")
                .or_else(|| platform.get("auth").and_then(|auth| auth.get("token")))
        })
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn api_server_cors_origins(config: &Value) -> Vec<String> {
    let mut origins: Vec<String> = api_server_platform(config)
        .and_then(|platform| platform.get("cors_origins"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    origins.sort();
    origins
}

fn config_fingerprint(workspace_root: &Path, config_home: &Path) -> ConfigFingerprint {
    let entries: Vec<ConfigFingerprintEntry> = ConfigLoader::new(workspace_root, config_home)
        .discover()
        .into_iter()
        .map(|entry| {
            let metadata = fs::metadata(&entry.path).ok();
            let modified_ms = metadata
                .as_ref()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(system_time_ms);
            let len = metadata.as_ref().map(fs::Metadata::len);
            let content_hash = if entry.exists {
                fs::read(&entry.path).ok().map(hash_bytes)
            } else {
                None
            };
            ConfigFingerprintEntry {
                source: entry.source.to_string(),
                path: entry.path.display().to_string(),
                exists: entry.exists,
                modified_ms,
                len,
                content_hash,
            }
        })
        .collect();
    let mut hasher = DefaultHasher::new();
    for entry in &entries {
        entry.source.hash(&mut hasher);
        entry.path.hash(&mut hasher);
        entry.exists.hash(&mut hasher);
        entry.modified_ms.hash(&mut hasher);
        entry.len.hash(&mut hasher);
        entry.content_hash.hash(&mut hasher);
    }
    ConfigFingerprint {
        digest: format!("{:016x}", hasher.finish()),
        entries,
        computed_at_ms: now_ms(),
    }
}

fn hash_bytes(bytes: Vec<u8>) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn system_time_ms(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
}

fn now_ms() -> u64 {
    system_time_ms(SystemTime::now()).unwrap_or(0)
}

fn status_lock() -> &'static RwLock<ConfigReloadSnapshot> {
    CONFIG_RELOAD_STATUS.get_or_init(|| RwLock::new(ConfigReloadSnapshot::default()))
}

fn apply_lock() -> &'static tokio::sync::Mutex<()> {
    CONFIG_RELOAD_APPLY_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn read_status() -> ConfigReloadSnapshot {
    status_lock()
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| ConfigReloadSnapshot::default())
}

fn write_status(snapshot: ConfigReloadSnapshot) {
    if let Ok(mut guard) = status_lock().write() {
        *guard = snapshot;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(label: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowd-config-reload-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&root).expect("create temp root");
        root
    }

    #[test]
    fn config_fingerprint_changes_when_config_content_changes() {
        let root = temp_root("fingerprint");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&config_home).unwrap();
        fs::write(config_home.join("config.yaml"), "model: one\n").unwrap();

        let first = config_fingerprint(&workspace, &config_home);
        fs::write(config_home.join("config.yaml"), "model: two\n").unwrap();
        let second = config_fingerprint(&workspace, &config_home);

        assert_ne!(first.digest, second.digest);
        let _ = fs::remove_dir_all(root);
    }
}
