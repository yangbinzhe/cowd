use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, VecDeque},
    fs,
    hash::{Hash, Hasher},
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use runtime::{ConfigLoader, RuntimeConfig};
use serde::Serialize;
use serde_json::Value;

use crate::api_routes::AppState;

/// Per-RuntimeHost config reload owner.
///
/// Mainline wiring must store one `Arc<ConfigReloadState>` in `RuntimeHost` or
/// `AppState` and pass that same instance to all watcher/API operations.
pub(crate) struct ConfigReloadState {
    status: RwLock<ConfigReloadSnapshot>,
    current_config: RwLock<Option<RuntimeConfig>>,
    apply_lock: tokio::sync::Mutex<()>,
}

impl ConfigReloadState {
    pub(crate) fn new() -> Self {
        Self {
            status: RwLock::new(ConfigReloadSnapshot::default()),
            current_config: RwLock::new(None),
            apply_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn read_status(&self) -> ConfigReloadSnapshot {
        self.status
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| ConfigReloadSnapshot::default())
    }

    fn write_status(&self, snapshot: ConfigReloadSnapshot) {
        if let Ok(mut guard) = self.status.write() {
            *guard = snapshot;
        }
    }

    pub(crate) fn current_config(&self) -> Option<RuntimeConfig> {
        self.current_config
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn write_current_config(&self, config: RuntimeConfig) {
        if let Ok(mut guard) = self.current_config.write() {
            *guard = Some(config);
        }
    }
}

impl Default for ConfigReloadState {
    fn default() -> Self {
        Self::new()
    }
}

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

pub(crate) fn initialize_config_reload_status(reload: &ConfigReloadState, state: &Arc<AppState>) {
    let fingerprint = config_fingerprint(&state.workspace_root, &state.config_home);
    if let Ok(config) = ConfigLoader::new(&state.workspace_root, &state.config_home).load() {
        reload.write_current_config(config);
    }
    reload.write_status(ConfigReloadSnapshot {
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
    reload: Arc<ConfigReloadState>,
    state: Arc<AppState>,
    interval: Duration,
    tasks: Arc<super::task_set::GatewayRuntimeTaskSet>,
) -> Result<u64, super::task_set::GatewayTaskSpawnError> {
    tasks.spawn(
        super::task_set::GatewayTaskKind::ConfigReload,
        None,
        move |cancellation| async move {
            poll_config_reload_once(&reload, state.clone(), "initial").await;
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        poll_config_reload_once(&reload, state.clone(), "auto").await;
                    }
                }
            }
        },
    )
}

pub(crate) fn status_value(reload: &ConfigReloadState) -> Value {
    serde_json::to_value(reload.read_status()).unwrap_or_else(|_| {
        serde_json::json!({
            "kind": "gateway.config.reload.status",
            "status": "unavailable",
            "applied": false,
        })
    })
}

pub(crate) async fn poll_config_reload_once(
    reload: &ConfigReloadState,
    state: Arc<AppState>,
    trigger: &str,
) -> Value {
    let report = poll_config_reload_sections_once(reload, Arc::clone(&state), trigger).await;
    if state.services.runtime.is_some() {
        if let Err(error) = reconcile_background_from_disk(reload, &state).await {
            if let Ok(mut status) = reload.status.write() {
                status.last_report["applied_sections"]["background_evolution"] = serde_json::json!({"effective":"error", "error":error, "scope":"instance_all_sessions"});
            }
        }
    }
    // Return the current original status projection after background apply,
    // never the pre-drain section snapshot.
    if state.services.runtime.is_some() {
        status_value(reload)
    } else {
        report
    }
}

async fn poll_config_reload_sections_once(
    reload: &ConfigReloadState,
    state: Arc<AppState>,
    trigger: &str,
) -> Value {
    let fingerprint =
        config_fingerprint_async(state.workspace_root.clone(), state.config_home.clone()).await;
    let digest = fingerprint.digest.clone();
    let previous = reload.read_status();
    if previous.last_seen_digest.as_deref() == Some(digest.as_str()) {
        if session_execution_policy_retry_pending(&previous) {
            if let (Some(runtime_config), Some(runtime)) =
                (reload.current_config(), state.services.runtime.as_ref())
            {
                let report = runtime
                    .update_execution_policy_defaults(
                        super::configured_runtime_permission_mode(&runtime_config),
                        runtime_config.approval().profile,
                    )
                    .await;
                let next =
                    merge_session_execution_policy_retry(previous, fingerprint, report, trigger);
                reload.write_status(next);
                return status_value(reload);
            }
        }
        let mut next = previous;
        next.last_checked_at_ms = now_ms();
        next.fingerprint = Some(fingerprint);
        reload.write_status(next);
        return status_value(reload);
    }
    reload_gateway_config_with_fingerprint(reload, &state, trigger, fingerprint).await
}

fn session_execution_policy_retry_pending(snapshot: &ConfigReloadSnapshot) -> bool {
    snapshot
        .last_report
        .pointer("/applied_sections/session_execution_policy/status")
        .and_then(Value::as_str)
        == Some("attention")
}

fn merge_session_execution_policy_retry(
    mut snapshot: ConfigReloadSnapshot,
    fingerprint: ConfigFingerprint,
    report: Value,
    trigger: &str,
) -> ConfigReloadSnapshot {
    let old_policy_warnings = snapshot
        .last_report
        .pointer("/applied_sections/session_execution_policy/warnings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    snapshot
        .warnings
        .retain(|warning| !old_policy_warnings.contains(warning));
    snapshot.warnings.extend(
        report
            .get("warnings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string),
    );
    if let Some(section) = snapshot
        .last_report
        .pointer_mut("/applied_sections/session_execution_policy")
    {
        *section = report;
    }
    let restart_required = snapshot
        .restart_required
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    snapshot.status = if restart_required {
        "reload_needed".to_string()
    } else if snapshot.warnings.is_empty() {
        "applied".to_string()
    } else {
        "attention".to_string()
    };
    snapshot.trigger = trigger.to_string();
    snapshot.last_checked_at_ms = now_ms();
    snapshot.fingerprint = Some(fingerprint);
    snapshot.last_report["status"] = Value::String(snapshot.status.clone());
    snapshot.last_report["trigger"] = Value::String(trigger.to_string());
    snapshot.last_report["warnings"] = serde_json::json!(snapshot.warnings);
    snapshot
}

/// The same instance reload owner serializes persisted operator mutations.
/// Authentication is checked here as well as by the public caller: a session
/// write grant or a model-issued slash is not instance management authority.
pub(crate) async fn background_control(
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
    principal: &runtime::VerifiedPrincipal,
    args: &Value,
    domain: &str,
) -> Result<Value, String> {
    require_background_operator(principal)?;
    if !matches!(domain, "evolution" | "growth") {
        return Err("unknown background domain".into());
    }
    let requested = parse_background_control(args, domain)?;
    let runtime = state
        .services
        .runtime
        .as_ref()
        .ok_or("runtime service unavailable")?;
    let services = runtime.runtime_services();
    let (desired, revision) = {
        let _apply = reload.apply_lock.lock().await;
        let config = ConfigLoader::new(&state.workspace_root, &state.config_home)
            .load()
            .map_err(|error| format!("background config load failed: {error}"))?;
        let previous = config_fingerprint(&state.workspace_root, &state.config_home);
        if let Some(expected) = args.get("expected_revision") {
            if expected.as_str() != Some(previous.digest.as_str()) {
                return Err("background config revision is stale".into());
            }
        }
        if let Some(enabled) = requested {
            let mut policy = config.background_work();
            if domain == "evolution" {
                policy.evolution = enabled;
            } else {
                policy.growth = enabled;
            }
            persist_background_work(&state.config_home, policy)?;
        }
        let config = ConfigLoader::new(&state.workspace_root, &state.config_home)
            .load()
            .map_err(|error| format!("background config persisted but reload failed: {error}"))?;
        let desired = config.background_work();
        // Persisted intent and new-work admission linearize under the original
        // apply lock. No drain or Provider await is held by that lock.
        if !desired.evolution {
            services.stop_evolution_background_admission();
        }
        if !desired.growth {
            services.stop_growth_background_admission();
        }
        reload.write_current_config(config);
        (
            desired,
            config_fingerprint(&state.workspace_root, &state.config_home).digest,
        )
    };
    if domain == "evolution" {
        reconcile_background_evolution(reload, state, desired, revision).await
    } else {
        reconcile_background_growth(reload, state, desired, revision).await
    }
}

async fn reconcile_background_evolution(
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
    desired: runtime::BackgroundWorkConfig,
    revision: String,
) -> Result<Value, String> {
    let services = state
        .services
        .runtime
        .as_ref()
        .ok_or("runtime service unavailable")?
        .runtime_services();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut failure = None;
    let mut physical_drained = false;
    if !services.background_work().evolution {
        match services.drain_evolution_background_physical(deadline).await {
            Ok(reports) => {
                physical_drained = true;
                failure = reports.into_iter().find_map(|report| report.last_error);
            }
            Err(error) => failure = Some(error),
        }
    }
    // A newer operator operation may have persisted while this waiter drained.
    // Never apply the old desired state over its successor.
    let _apply = reload.apply_lock.lock().await;
    let current = ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .map_err(|error| error.to_string())?;
    let current_revision = config_fingerprint(&state.workspace_root, &state.config_home).digest;
    if current_revision != revision {
        return Ok(
            serde_json::json!({"scope": "instance_all_sessions", "status": "superseded",
            "desired": current.background_work(), "revision": current_revision,
            "effective": "draining", "physical_drained": physical_drained}),
        );
    }
    let (durable_drained, pending_claims) = if services.background_work().evolution {
        (false, 0)
    } else {
        match services.evolution_background_durable_drain_status() {
            Ok(status) => status,
            Err(error) => {
                failure = Some(error);
                (false, 0)
            }
        }
    };
    if desired.evolution
        && !services.background_work().evolution
        && failure.is_none()
        && durable_drained
    {
        let binding = Arc::new(std::sync::OnceLock::new());
        binding
            .set(Arc::downgrade(&services))
            .map_err(|_| "evolution evaluator binding already initialized")?;
        services.install_evolution_eval_runner(super::configured_evolution_eval_runner(
            &state.config_home,
            binding,
        ));
        if let Err(error) = services.resume_evolution_background_admission() {
            failure = Some(error);
        }
    }
    let effective = if failure.is_some() {
        "error"
    } else if services.background_work().evolution {
        "on"
    } else if !desired.evolution && physical_drained && durable_drained {
        "off"
    } else {
        "draining"
    };
    let report = serde_json::json!({"scope": "instance_all_sessions", "status": effective,
        "desired": desired, "effective": effective, "revision": revision,
        "physical_drained": physical_drained,
        "durable_recovery": if services.background_work().evolution { "original_projection_owner" }
            else if durable_drained { "settled" } else { "pending" },
        "pending_analysis_claims": pending_claims, "pending_count_is_lower_bound": !durable_drained,
        "error": failure});
    // Preserve the result in the original reload status. This is an apply
    // projection, never an additional desired-policy writer.
    let mut status = reload
        .status
        .write()
        .map_err(|_| "config status lock poisoned")?;
    status.last_report["applied_sections"]["background_evolution"] = report.clone();
    if matches!(effective, "error" | "draining") {
        status.status = "attention".into();
        status.last_report["status"] = Value::String("attention".into());
    }
    Ok(report)
}

fn growth_store(state: &AppState) -> Result<&Arc<runtime::RuntimeEventStore>, String> {
    state
        .services
        .selected_storage
        .as_ref()
        .map(|s| &s.runtime_event_store)
        .ok_or_else(|| "Growth selected storage unavailable".into())
}
fn growth_lane(state: &AppState) -> Result<runtime::RuntimeProjectionLane, String> {
    crate::services::growth_projection_lane(
        state.config_home.clone(),
        Arc::clone(growth_store(state)?),
        state.services.growth.clone(),
        state.services.memory.clone(),
        state.services.matrix.clone(),
    )
}
async fn reconcile_background_growth(
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
    desired: runtime::BackgroundWorkConfig,
    revision: String,
) -> Result<Value, String> {
    let services = state
        .services
        .runtime
        .as_ref()
        .ok_or("runtime service unavailable")?
        .runtime_services();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut error = None;
    let mut physical = false;
    if !services.background_work().growth {
        match services.drain_growth_background_physical(deadline).await {
            Ok(_) => physical = true,
            Err(e) => error = Some(e),
        }
    }
    let _apply = reload.apply_lock.lock().await;
    let current = ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .map_err(|e| e.to_string())?;
    let current_revision = config_fingerprint(&state.workspace_root, &state.config_home).digest;
    if current_revision != revision {
        return Ok(
            serde_json::json!({"scope":"instance_all_sessions","status":"superseded","desired":current.background_work(),"revision":current_revision,"effective":"draining"}),
        );
    }
    if let Err(e) = crate::services::growth_projection_lane::initialize_growth_policy(
        growth_store(state)?,
        services.background_work().growth,
    ) {
        services.stop_growth_background_admission();
        error = Some(e);
    }
    let settled = match crate::services::growth_projection_lane::growth_checkpoint_status(
        growth_store(state)?,
    ) {
        Ok(v) => v,
        Err(e) => {
            error = Some(e);
            false
        }
    };
    if physical && error.is_none() {
        if settled && desired.growth {
            if let Err(e) =
                crate::services::growth_projection_lane::resume_growth_sources(growth_store(state)?)
                    .and_then(|()| {
                        services.register_growth_background_lane(growth_lane(state)?, true)
                    })
            {
                error = Some(e);
            }
        } else if !settled {
            // The same reactor owns recovery-only passes, including if the
            // original HTTP waiter is cancelled. Paused checkpoints forbid new sources.
            if let Err(e) = services.register_growth_background_lane(growth_lane(state)?, false) {
                error = Some(e);
            }
        }
    }
    let effective = if error.is_some() {
        "error"
    } else if services.background_work().growth {
        "on"
    } else if physical && settled && !desired.growth {
        "off"
    } else {
        "draining"
    };
    let report = serde_json::json!({"scope":"instance_all_sessions","status":effective,"desired":desired,
        "effective":effective,"revision":revision,"physical_drained":physical,"durable_recovery":if settled{"settled"}else{"pending"},"pending_page":!settled,"error":error});
    let mut status = reload
        .status
        .write()
        .map_err(|_| "config status lock poisoned")?;
    status.last_report["applied_sections"]["background_growth"] = report.clone();
    if matches!(effective, "error" | "draining") {
        status.status = "attention".into();
        status.last_report["status"] = "attention".into();
    }
    Ok(report)
}

async fn reconcile_background_from_disk(
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
) -> Result<Value, String> {
    let (desired, revision) = {
        let _guard = reload.apply_lock.lock().await;
        let config = ConfigLoader::new(&state.workspace_root, &state.config_home)
            .load()
            .map_err(|error| error.to_string())?;
        let desired = config.background_work();
        if !desired.evolution {
            state
                .services
                .runtime
                .as_ref()
                .ok_or("runtime service unavailable")?
                .runtime_services()
                .stop_evolution_background_admission();
        }
        if !desired.growth {
            state
                .services
                .runtime
                .as_ref()
                .ok_or("runtime service unavailable")?
                .runtime_services()
                .stop_growth_background_admission();
        }
        (
            desired,
            config_fingerprint(&state.workspace_root, &state.config_home).digest,
        )
    };
    let evolution =
        reconcile_background_evolution(reload, state, desired, revision.clone()).await?;
    let growth = reconcile_background_growth(reload, state, desired, revision).await?;
    Ok(serde_json::json!({"evolution":evolution,"growth":growth}))
}

pub(crate) fn require_background_operator(
    principal: &runtime::VerifiedPrincipal,
) -> Result<(), String> {
    if !principal.is_human_interactive() || !principal.has_capability("runtime.maintenance.manage")
    {
        return Err(
            "background control requires a verified human with runtime.maintenance.manage".into(),
        );
    }
    Ok(())
}

fn parse_background_control(args: &Value, command: &str) -> Result<Option<bool>, String> {
    let explicit = match args.get("mode") {
        Some(value) => Some(
            value
                .as_str()
                .ok_or("background control mode must be a string")?,
        ),
        None => None,
    };
    let input = match args.get("input") {
        Some(value) => {
            let mut words = value
                .as_str()
                .ok_or("background slash input must be a string")?
                .split_whitespace();
            if words.next().map(|word| word.trim_start_matches('/')) != Some(command) {
                return Err("background slash input belongs to another command".into());
            }
            let mode = words.next();
            if words.next().is_some() {
                return Err("background slash has excess arguments".into());
            }
            mode
        }
        None => None,
    };
    if explicit
        .zip(input)
        .is_some_and(|(left, right)| left != right)
    {
        return Err("background slash mode conflicts with its input".into());
    }
    match explicit.or(input).unwrap_or("status") {
        "status" => Ok(None),
        "on" => Ok(Some(true)),
        "off" => Ok(Some(false)),
        _ => Err("background control mode must be on|off|status".into()),
    }
}

fn persist_background_work(
    home: &Path,
    policy: runtime::BackgroundWorkConfig,
) -> Result<(), String> {
    use std::io::Write;
    fs::create_dir_all(home).map_err(|error| error.to_string())?;
    // ConfigLoader gives config.yml precedence over config.yaml. Mutating the
    // lower file would produce a convincing receipt with no effective change.
    let path = if home.join("config.yml").exists() {
        home.join("config.yml")
    } else {
        home.join("config.yaml")
    };
    let before = match fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.to_string()),
    };
    let mut value: serde_yaml::Value = match before.as_ref() {
        Some(bytes) => serde_yaml::from_slice(bytes).map_err(|error| error.to_string())?,
        None => serde_yaml::Value::Mapping(Default::default()),
    };
    let root = value
        .as_mapping_mut()
        .ok_or("config root must be a mapping")?;
    let runtime = root
        .entry(serde_yaml::Value::String("runtime".into()))
        .or_insert_with(|| serde_yaml::Value::Mapping(Default::default()))
        .as_mapping_mut()
        .ok_or("runtime config must be a mapping")?;
    runtime.insert(
        serde_yaml::Value::String("background".into()),
        serde_yaml::to_value(policy).map_err(|error| error.to_string())?,
    );
    let bytes = serde_yaml::to_string(&value).map_err(|error| error.to_string())?;
    let mut temporary = tempfile::NamedTempFile::new_in(home).map_err(|error| error.to_string())?;
    temporary
        .write_all(bytes.as_bytes())
        .map_err(|error| error.to_string())?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| error.to_string())?;
    let current = match fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.to_string()),
    };
    if current != before {
        return Err("operator config changed before persistence".into());
    }
    temporary
        .persist(&path)
        .map_err(|error| error.error.to_string())?;
    fs::File::open(home)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) async fn force_gateway_config_reload(
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
    trigger: &str,
) -> Value {
    let fingerprint =
        config_fingerprint_async(state.workspace_root.clone(), state.config_home.clone()).await;
    let report = reload_gateway_config_with_fingerprint(reload, state, trigger, fingerprint).await;
    if state.services.runtime.is_some() {
        let background = match reconcile_background_from_disk(reload, state).await {
            Ok(report) => report,
            Err(error) => serde_json::json!({"effective":"error", "status":"error", "error":error,
                "scope":"instance_all_sessions"}),
        };
        let mut report = report;
        if ["evolution", "growth"].iter().any(|domain| {
            matches!(
                background[*domain]["effective"].as_str(),
                Some("error" | "draining")
            )
        }) {
            report["status"] = Value::String("attention".into());
        }
        report["applied_sections"]["background_evolution"] = background["evolution"].clone();
        report["applied_sections"]["background_growth"] = background["growth"].clone();
        return report;
    }
    report
}

pub(crate) async fn reload_runtime_providers_from_disk(
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
) -> Value {
    let _guard = reload.apply_lock.lock().await;
    match ConfigLoader::new(&state.workspace_root, &state.config_home).load() {
        Ok(runtime_config) => apply_runtime_providers(state, &runtime_config),
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
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
    trigger: &str,
    fingerprint: ConfigFingerprint,
) -> Value {
    let _guard = reload.apply_lock.lock().await;
    let changed_at = now_ms();
    let previous = reload.read_status();
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
            reload.write_status(ConfigReloadSnapshot {
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

    let report = apply_runtime_config(reload, state, &runtime_config, trigger, &fingerprint).await;
    let applied = report
        .get("applied")
        .and_then(Value::as_bool)
        .unwrap_or(false);
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
    reload.write_status(ConfigReloadSnapshot {
        status,
        applied,
        trigger: trigger.to_string(),
        last_checked_at_ms: changed_at,
        last_changed_at_ms: Some(changed_at),
        last_applied_at_ms: if applied {
            Some(changed_at)
        } else {
            previous.last_applied_at_ms
        },
        last_seen_digest: Some(fingerprint.digest.clone()),
        last_applied_digest: if applied {
            Some(fingerprint.digest.clone())
        } else {
            previous.last_applied_digest
        },
        last_error: (!applied).then(|| "configuration validation failed".to_string()),
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
    reload: &ConfigReloadState,
    state: &Arc<AppState>,
    runtime_config: &RuntimeConfig,
    trigger: &str,
    fingerprint: &ConfigFingerprint,
) -> Value {
    let config_json = runtime_config_json_value(runtime_config);
    let provider_report = apply_runtime_providers(state, runtime_config);
    if !provider_report
        .get("applied")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return serde_json::json!({
            "kind": "gateway.config.reload",
            "status": "invalid",
            "applied": false,
            "trigger": trigger,
            "source": if runtime_config.loaded_entries().is_empty() { "default" } else { "config" },
            "config_entries": runtime_config.loaded_entries(),
            "fingerprint": fingerprint.digest,
            "applied_sections": {
                "providers": provider_report,
            },
            "restart_required": {
                "required": false,
                "unknown": false,
                "fields": [],
                "reason": "reload rejected before mutable sections were applied",
            },
            "warnings": ["provider configuration validation failed; retained the last valid runtime snapshot"],
        });
    }
    let provider_fallbacks_report = if let Some(runtime) = state.services.runtime.as_ref() {
        let fallbacks = runtime
            .runtime_services()
            .replace_provider_fallbacks(runtime_config.fallbacks().iter().cloned());
        serde_json::json!({
            "status": "applied",
            "activation_scope": "next_provider_request_in_existing_and_new_session_runtimes",
            "fallbacks": fallbacks,
        })
    } else {
        serde_json::json!({"status": "unavailable", "fallbacks": []})
    };
    let tool_host_report = match build_and_apply_tool_host_snapshot(state, runtime_config).await {
        Ok(report) => report,
        Err(error) => {
            return serde_json::json!({
                "kind": "gateway.config.reload",
                "status": "invalid",
                "applied": false,
                "trigger": trigger,
                "fingerprint": fingerprint.digest,
                "applied_sections": { "providers": provider_report },
                "restart_required": { "required": false, "fields": [] },
                "warnings": [format!("tool host snapshot validation failed; retained prior revision: {error}")],
            });
        }
    };
    let mcp_report = tool_host_report
        .get("mcp")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({"status": "unavailable"}));
    let mission_schedule_report = if let Some(runtime) = state.services.runtime.as_ref() {
        match runtime.runtime_services().update_mission_schedule_policy(
            runtime_config
                .runtime_control()
                .policy
                .mission_schedule
                .clone(),
        ) {
            Ok(()) => serde_json::json!({
                "status": "applied",
                "policy": runtime.runtime_services().mission_schedule_policy(),
            }),
            Err(error) => {
                return serde_json::json!({
                    "kind": "gateway.config.reload",
                    "status": "invalid",
                    "applied": false,
                    "trigger": trigger,
                    "fingerprint": fingerprint.digest,
                    "applied_sections": {
                        "providers": provider_report,
                        "tool_host": tool_host_report,
                        "mcp": mcp_report,
                    },
                    "restart_required": { "required": false, "fields": [] },
                    "warnings": [format!("mission schedule policy was rejected: {error}")],
                });
            }
        }
    } else {
        serde_json::json!({"status": "unavailable"})
    };
    let hot_state_report = if let Some(runtime) = state.services.runtime.as_ref() {
        match runtime
            .runtime_services()
            .update_hot_state_config(runtime_config.hot_state())
        {
            Ok(health) => serde_json::json!({
                "status": "applied",
                "activation_scope": "existing_and_new_hot_runtime_state",
                "health": health,
            }),
            Err(error) => {
                return serde_json::json!({
                    "kind": "gateway.config.reload",
                    "status": "invalid",
                    "applied": false,
                    "trigger": trigger,
                    "fingerprint": fingerprint.digest,
                    "restart_required": { "required": false, "fields": [] },
                    "warnings": [format!("hot-state budget was rejected: {error}")],
                });
            }
        }
    } else {
        serde_json::json!({"status": "unavailable"})
    };
    let execution_policy_report = if let Some(runtime) = state.services.runtime.as_ref() {
        let report = runtime
            .update_execution_policy_defaults(
                super::configured_runtime_permission_mode(runtime_config),
                runtime_config.approval().profile,
            )
            .await;
        runtime
            .runtime_services()
            .approval_coordinator()
            .update_config(runtime_config.approval().clone())
            .await;
        report
    } else {
        serde_json::json!({"status": "unavailable"})
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
    let resource_capabilities = state.services.refresh_resource_capabilities();
    let restart_required = restart_required_report(state.startup_config_snapshot(), &config_json);
    let mut warnings = reload_warnings(
        &provider_report,
        &mcp_report,
        &static_webui,
        &restart_required,
    );
    warnings.extend(
        execution_policy_report
            .get("warnings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string),
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
    reload.write_current_config(runtime_config.clone());

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
            "provider_fallbacks": provider_fallbacks_report,
            "tool_host": tool_host_report,
            "mcp": mcp_report,
            "mission_schedule": mission_schedule_report,
            "hot_state": hot_state_report,
            "session_execution_policy": execution_policy_report,
            "surface_runtime_configs": {
                "status": "applied",
                "count": surface_config_count,
            },
            "static_webui": static_webui,
            "edge_registry": edge_discovery,
            "resource_capabilities": {
                "status": "applied",
                "tools": resource_capabilities.tools,
                "skills": resource_capabilities.skills,
                "plugins": resource_capabilities.plugins,
                "mcp_resources": resource_capabilities.mcp_resources,
                "local_commands": resource_capabilities.local_commands,
                "provider_native": resource_capabilities.provider_native,
            },
        },
        "restart_required": restart_required,
        "warnings": warnings,
    })
}

async fn build_and_apply_tool_host_snapshot(
    state: &Arc<AppState>,
    runtime_config: &RuntimeConfig,
) -> Result<Value, String> {
    let loader = ConfigLoader::new(&state.workspace_root, &state.config_home);
    let mut bootstrap = crate::runtime_bootstrap::assemble_runtime_state_with_loader(
        &state.workspace_root,
        &loader,
        runtime_config,
    )
    .map_err(|error| error.to_string())?;
    let mcp_service =
        Arc::new(super::RuntimeMcpServiceAdapter::from_runtime_config(runtime_config).await);
    bootstrap.tool_registry = bootstrap
        .tool_registry
        .clone()
        .extend_runtime_tools(mcp_service.runtime_tool_definitions())?;
    let session_snapshot = bootstrap.session_snapshot();
    let catalog = Arc::new(bootstrap.tool_registry);
    let mcp_health = mcp::McpService::health(mcp_service.as_ref())
        .map_err(|error| format!("failed to validate MCP snapshot: {error}"))?;
    let Some(runtime) = state.services.runtime.as_ref() else {
        return Err("runtime service unavailable".to_string());
    };
    let tool_host = runtime.tool_host();
    let previous_revision = tool_host.revision();
    let revision = tool_host.replace_snapshot(tools::ToolHostSnapshot::new(
        catalog,
        Arc::new(tools::lsp_client::LspRegistry::new()),
        Some(mcp_service),
    ));
    runtime.replace_session_bootstrap(session_snapshot);
    crate::services::invalidate_workspace_skill_snapshot(&state.workspace_root);
    let skill_workspace = state.workspace_root.clone();
    let skill_assets = tokio::task::spawn_blocking(move || {
        crate::services::runtime_skill_assets_for_workspace(&skill_workspace)
    })
    .await
    .map_err(|error| format!("failed to rebuild Skill catalog snapshot: {error}"))?;
    let mut skill_catalog = runtime::RuntimeSkillCatalog::new(skill_assets.profiles);
    if let Some(source) = skill_assets.instruction_source {
        skill_catalog = skill_catalog.with_instruction_source(source);
    }
    runtime
        .runtime_services()
        .replace_skill_catalog(skill_catalog);
    Ok(serde_json::json!({
        "status": "applied",
        "previous_revision": previous_revision,
        "revision": revision,
        "cache": tool_host.cache_stats(),
        "mcp": {
            "status": "applied",
            "health": mcp_health,
        },
    }))
}

pub(crate) fn apply_runtime_providers(
    state: &Arc<AppState>,
    runtime_config: &RuntimeConfig,
) -> Value {
    let source = if runtime_config.loaded_entries().is_empty() {
        "default"
    } else {
        "config"
    };
    let providers = runtime_config.providers().clone();
    let registry = model_protocol::model_registry::global_registry();
    let catalog = provider::ProviderCatalog::from_input(provider::ProviderCatalogInput {
        providers: &providers,
        registry,
        model_context_windows: runtime_config.model_context_windows(),
        max_output_tokens_override: runtime_config
            .provider_resources()
            .max_output_tokens_override(),
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
    let configured_model = runtime_config.resolved_model();
    let configured_model_provider = configured_model
        .as_deref()
        .and_then(|model| providers.resolve_full(model))
        .map(|provider| provider.name.clone());
    let configured_model_resolved =
        configured_model.is_none() || configured_model_provider.is_some();
    let mut provider_names: Vec<String> = providers.providers.keys().cloned().collect();
    provider_names.sort();

    let catalog_report = serde_json::json!({
        "generation": catalog.generation,
        "sources": catalog.sources,
        "transforms": catalog.transforms,
        "provider_count": catalog.providers.len(),
        "model_count": catalog.models.len(),
        "profile_count": catalog.profiles.len(),
        "warnings": catalog.warnings,
        "activation_scope": provider_activation_scope(),
    });
    let Some(runtime_service) = state.services.runtime.as_ref() else {
        return serde_json::json!({
            "kind": "runtime_provider_reload",
            "status": "unavailable",
            "applied": false,
            "source": source,
            "retained_revision": null,
            "diagnostics": {
                "errors": ["RuntimeService is not attached; provider registry cannot be reloaded"],
                "warnings": [],
            },
        });
    };
    let provider_registry = runtime_service.provider_registry();
    if !configured_model_resolved {
        let retained_revision = provider_registry.revision();
        return serde_json::json!({
            "kind": "runtime_provider_reload",
            "status": "invalid",
            "applied": false,
            "source": source,
            "retained_revision": retained_revision,
            "catalog_generation": catalog_generation,
            "catalog_updated": catalog_updated,
            "catalog": catalog_report,
            "provider_count": provider_count,
            "provider_model_count": provider_model_count,
            "provider_names": provider_names,
            "configured_model": configured_model,
            "configured_model_provider": configured_model_provider,
            "configured_model_resolved": false,
            "diagnostics": {
                "errors": ["configured default model is not declared by any configured provider"],
                "warnings": [],
            },
        });
    }
    if let Err(error) = runtime_service
        .runtime_services()
        .provider_transport_pool()
        .checkout_default()
    {
        let retained_revision = provider_registry.revision();
        tracing::warn!(
            target: "cowd.runtime.provider",
            applied = false,
            source,
            retained_revision,
            error = %error,
            "runtime provider reload rejected by transport preflight; retaining last valid snapshot"
        );
        return serde_json::json!({
            "kind": "runtime_provider_reload",
            "status": "invalid",
            "applied": false,
            "source": source,
            "retained_revision": retained_revision,
            "catalog_generation": catalog_generation,
            "catalog_updated": catalog_updated,
            "catalog": catalog_report,
            "provider_count": provider_count,
            "provider_model_count": provider_model_count,
            "provider_names": provider_names,
            "configured_model": configured_model,
            "configured_model_provider": configured_model_provider,
            "configured_model_resolved": configured_model_resolved,
            "diagnostics": {
                "errors": [format!("provider transport preflight failed: {error}")],
                "warnings": [],
            },
        });
    }
    let update = match provider_registry.replace_with_context_windows(providers, runtime_config.model_context_windows().clone()) {
        Ok(update) => update,
        Err(rejected) => {
            tracing::warn!(
                target: "cowd.runtime.provider",
                applied = false,
                source,
                retained_revision = rejected.retained_revision,
                errors = ?rejected.diagnostics.errors,
                "runtime provider reload rejected; retaining last valid snapshot"
            );
            return serde_json::json!({
                "kind": "runtime_provider_reload",
                "status": "invalid",
                "applied": false,
                "source": source,
                "retained_revision": rejected.retained_revision,
                "catalog_generation": catalog_generation,
                "catalog_updated": catalog_updated,
                "catalog": catalog_report,
                "provider_count": provider_count,
                "provider_model_count": provider_model_count,
                "provider_names": provider_names,
                "configured_model": configured_model,
                "configured_model_provider": configured_model_provider,
                "configured_model_resolved": configured_model_resolved,
                "diagnostics": {
                    "errors": rejected.diagnostics.errors,
                    "warnings": rejected.diagnostics.warnings,
                },
            });
        }
    };
    let provider_resource_snapshots = match runtime_service
        .runtime_services()
        .replace_provider_resource_config(runtime_config.provider_resources().clone())
    {
        Ok(snapshots) => snapshots,
        Err(error) => {
            return serde_json::json!({
                "kind": "runtime_provider_reload",
                "status": "invalid",
                "applied": false,
                "source": source,
                "retained_revision": update.revision,
                "diagnostics": {
                    "errors": [format!("provider resource policy rejected: {error}")],
                    "warnings": [],
                },
            });
        }
    };
    if let Err(error) = runtime_service.replace_configured_model(configured_model.clone()) {
        tracing::error!(
            target: "cowd.runtime.provider",
            applied = false,
            source,
            error = %error,
            "provider registry accepted a model configuration that Runtime rejected"
        );
        return serde_json::json!({
            "kind": "runtime_provider_reload",
            "status": "invalid",
            "applied": false,
            "source": source,
            "retained_revision": update.revision,
            "configured_model": configured_model,
            "configured_model_provider": configured_model_provider,
            "configured_model_resolved": false,
            "diagnostics": {
                "errors": [error],
                "warnings": [],
            },
        });
    }

    tracing::info!(
        target: "cowd.runtime.provider",
        applied = true,
        source,
        provider_count,
        provider_model_count,
        provider_revision = update.revision,
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
        "changed": update.changed,
        "previous_revision": update.previous_revision,
        "revision": update.revision,
        "catalog_generation": catalog_generation,
        "catalog_updated": catalog_updated,
        "catalog": catalog_report,
        "provider_count": provider_count,
        "provider_model_count": provider_model_count,
        "provider_names": provider_names,
        "provider_resource_snapshots": provider_resource_snapshots,
        "configured_model": configured_model,
        "configured_model_provider": configured_model_provider,
        "configured_model_resolved": configured_model_resolved,
        "activation_scope": provider_activation_scope(),
        "warnings": if provider_count == 0 {
            serde_json::json!(["no runtime providers are configured"])
        } else if !configured_model_resolved {
            serde_json::json!(["configured default model is not declared by any provider"])
        } else {
            serde_json::json!(update.diagnostics.warnings)
        }
    })
}

fn provider_activation_scope() -> Value {
    serde_json::json!({
        "provider_credentials_and_protocol": "subsequent_provider_checkout",
        "model_capacity_overrides": "new_or_restored_session_runtime",
        "models_yaml": "gateway_restart",
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
            // 热加载必须与启动路径使用同一个 canonical surface id；
            // Lark/Feishu 共享 `feishu` sidecar，不能把配置留在 `lark` key。
            let id = surface::message::normalize_message_connector(&platform.platform_type);
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
    if startup_config.get("workspace") != current_config.get("workspace") {
        fields.push("workspace");
    }
    if startup_config.pointer("/runtime/hot_state/shards")
        != current_config.pointer("/runtime/hot_state/shards")
    {
        fields.push("runtime.hot_state.shards");
    }
    if startup_config.pointer("/runtime/hot_state/materializer_queue_capacity")
        != current_config.pointer("/runtime/hot_state/materializer_queue_capacity")
    {
        fields.push("runtime.hot_state.materializer_queue_capacity");
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
    let mut entries: Vec<ConfigFingerprintEntry> = ConfigLoader::new(workspace_root, config_home)
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
    let mut skill_roots = vec![workspace_root.join(".cowd").join("skills")];
    if let Ok(root) = crate::skill_static::default_skill_install_root() {
        skill_roots.push(root);
    }
    if let Ok(root) = skill::default_managed_skill_store_root() {
        skill_roots.push(root);
    }
    for root in skill_roots {
        collect_skill_fingerprint_entries(&root, &mut entries, 2_048);
    }
    entries.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.path.cmp(&right.path))
    });
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

async fn config_fingerprint_async(
    workspace_root: std::path::PathBuf,
    config_home: std::path::PathBuf,
) -> ConfigFingerprint {
    tokio::task::spawn_blocking(move || config_fingerprint(&workspace_root, &config_home))
        .await
        .unwrap_or_else(|error| {
            tracing::error!(
                %error,
                "config fingerprint worker failed; using an explicit failed fingerprint"
            );
            ConfigFingerprint {
                digest: format!("fingerprint-worker-failed-{error}"),
                entries: Vec::new(),
                computed_at_ms: now_ms(),
            }
        })
}

fn collect_skill_fingerprint_entries(
    root: &Path,
    entries: &mut Vec<ConfigFingerprintEntry>,
    max_files: usize,
) {
    // Breadth-first traversal observes shallow managed active pointers before
    // immutable historical revisions when the bounded watcher budget is full.
    let mut pending = VecDeque::from([root.to_path_buf()]);
    let mut skill_files = Vec::new();
    while let Some(path) = pending.pop_front() {
        if skill_files.len() >= max_files {
            break;
        }
        let Ok(directory) = fs::read_dir(&path) else {
            continue;
        };
        let mut directory = directory.flatten().collect::<Vec<_>>();
        directory.sort_by_key(fs::DirEntry::file_name);
        for entry in directory {
            let path = entry.path();
            if path.is_dir() {
                pending.push_back(path);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.eq_ignore_ascii_case("SKILL.md")
                        || name.eq_ignore_ascii_case("active.json")
                })
            {
                skill_files.push(path);
                if skill_files.len() >= max_files {
                    break;
                }
            }
        }
    }
    skill_files.sort();
    for path in skill_files {
        let metadata = fs::metadata(&path).ok();
        let is_active_pointer = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.eq_ignore_ascii_case("active.json"));
        entries.push(ConfigFingerprintEntry {
            source: "skill".to_string(),
            path: path.display().to_string(),
            exists: metadata.is_some(),
            modified_ms: metadata
                .as_ref()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(system_time_ms),
            len: metadata.as_ref().map(fs::Metadata::len),
            // SKILL.md bodies remain page-loaded. The small active pointer is
            // hashed because two SHA-256 revisions have identical length and
            // rapid rollback can share one filesystem timestamp tick.
            content_hash: is_active_pointer
                .then(|| fs::read(&path).ok().map(hash_bytes))
                .flatten(),
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    fn reload_state() -> ConfigReloadState {
        ConfigReloadState::new()
    }

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
    fn background_control_preserves_operator_precedence_other_domain_and_restart_config() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let workspace = root.path().join("workspace");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        let lower = b"model: preserved-model\nruntime:\n  background:\n    growth: false\n";
        fs::write(home.join("config.yaml"), lower).unwrap();
        fs::write(
            home.join("config.yml"),
            "runtime:\n  background:\n    evolution: true\n    growth: true\n",
        )
        .unwrap();
        let original = ConfigLoader::new(&workspace, &home)
            .load()
            .unwrap()
            .background_work();
        assert!(original.evolution && original.growth);
        persist_background_work(
            &home,
            runtime::BackgroundWorkConfig {
                evolution: false,
                growth: original.growth,
            },
        )
        .unwrap();
        assert_eq!(fs::read(home.join("config.yaml")).unwrap(), lower);
        let fresh = ConfigLoader::new(&workspace, &home).load().unwrap();
        assert_eq!(
            fresh.background_work(),
            runtime::BackgroundWorkConfig {
                evolution: false,
                growth: true
            }
        );
        assert_eq!(fresh.model(), Some("preserved-model"));
        let mut second = fresh.background_work();
        second.evolution = true;
        persist_background_work(&home, second).unwrap();
        assert_eq!(
            ConfigLoader::new(&workspace, &home)
                .load()
                .unwrap()
                .background_work(),
            runtime::BackgroundWorkConfig {
                evolution: true,
                growth: true
            }
        );
        assert_eq!(
            fs::read_dir(&home).unwrap().count(),
            2,
            "no temporary file retained"
        );
    }

    #[test]
    fn background_control_rejects_ambiguous_modes_and_persistence_failures_without_partial_write() {
        for args in [
            serde_json::json!({"mode":null}),
            serde_json::json!({"mode":"enable"}),
            serde_json::json!({"mode":"off", "input":"/evolution on"}),
            serde_json::json!({"input":"/growth off"}),
            serde_json::json!({"input":"/evolution off extra"}),
        ] {
            assert!(
                parse_background_control(&args, "evolution").is_err(),
                "{args}"
            );
        }
        assert_eq!(
            parse_background_control(&serde_json::json!({"input":"/evolution off"}), "evolution")
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            parse_background_control(&serde_json::json!({}), "evolution").unwrap(),
            None
        );
        let root = tempfile::tempdir().unwrap();
        let home = root.path();
        fs::write(home.join("config.yaml"), "model: unchanged\n").unwrap();
        fs::create_dir(home.join("config.yml")).unwrap();
        assert!(persist_background_work(
            home,
            runtime::BackgroundWorkConfig {
                evolution: false,
                growth: false
            }
        )
        .is_err());
        assert_eq!(
            fs::read(home.join("config.yaml")).unwrap(),
            b"model: unchanged\n"
        );
        assert_eq!(fs::read_dir(home).unwrap().count(), 2);
    }

    #[test]
    fn background_control_requires_verified_instance_management_not_session_write() {
        for (capabilities, permitted) in [
            (vec!["session.write".to_string()], false),
            (vec!["approval.manage".to_string()], false),
            (vec!["runtime.maintenance.manage".to_string()], true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let (envelope, public_key) = auth_broker::test_support::issue_human_principal(
                root.path(),
                "isolated-background-control-credential",
                capabilities,
                None,
            )
            .unwrap();
            let principal = runtime::PrincipalVerifier::from_base64(&envelope.key_id, &public_key)
                .unwrap()
                .verify(&envelope)
                .unwrap();
            assert_eq!(require_background_operator(&principal).is_ok(), permitted);
        }
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

    #[test]
    fn config_fingerprint_tracks_workspace_skill_instructions() {
        let root = temp_root("skill-fingerprint");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        let skill_root = workspace.join(".cowd/skills/research");
        fs::create_dir_all(&skill_root).unwrap();
        fs::create_dir_all(&config_home).unwrap();
        fs::write(skill_root.join("SKILL.md"), "# Research\n\nFirst").unwrap();

        let first = config_fingerprint(&workspace, &config_home);
        fs::write(skill_root.join("SKILL.md"), "# Research\n\nSecond").unwrap();
        let second = config_fingerprint(&workspace, &config_home);

        assert_ne!(first.digest, second.digest);
        assert!(second.entries.iter().any(|entry| entry.source == "skill"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_fingerprint_hashes_managed_active_pointer_content() {
        let root = temp_root("managed-active-pointer");
        let pointer = root.join("demo/active.json");
        fs::create_dir_all(pointer.parent().unwrap()).unwrap();
        fs::create_dir_all(root.join("demo/revisions/old/deep")).unwrap();
        fs::write(root.join("demo/revisions/old/deep/SKILL.md"), "old").unwrap();
        fs::write(&pointer, "{\"revision\":\"sha256:aaaa\"}\n").unwrap();
        let mut first = Vec::new();
        collect_skill_fingerprint_entries(&root, &mut first, 1);
        fs::write(&pointer, "{\"revision\":\"sha256:bbbb\"}\n").unwrap();
        let mut second = Vec::new();
        collect_skill_fingerprint_entries(&root, &mut second, 1);

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert!(first[0].content_hash.is_some());
        assert_ne!(first[0].content_hash, second[0].content_hash);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reload_status_is_scoped_to_its_runtime_host() {
        let first = reload_state();
        let second = reload_state();
        let mut first_status = first.read_status();
        first_status.status = "invalid".to_string();
        first_status.last_error = Some("broken provider config".to_string());
        first.write_status(first_status);

        assert_eq!(first.read_status().status, "invalid");
        assert_eq!(second.read_status().status, "uninitialized");
        assert!(second.read_status().last_error.is_none());
    }

    #[test]
    fn reload_state_retains_the_last_successful_runtime_config_snapshot() {
        let state = reload_state();
        assert!(state.current_config().is_none());

        state.write_current_config(RuntimeConfig::empty());

        assert_eq!(state.current_config(), Some(RuntimeConfig::empty()));
    }

    #[test]
    fn unchanged_config_retry_replaces_only_session_policy_attention() {
        let policy_warning = "Session retry-default retained its prior policy".to_string();
        let snapshot = ConfigReloadSnapshot {
            status: "attention".to_string(),
            applied: true,
            warnings: vec![policy_warning.clone()],
            restart_required: serde_json::json!({"required": false, "fields": []}),
            last_report: serde_json::json!({
                "kind": "gateway.config.reload",
                "status": "attention",
                "applied": true,
                "warnings": [policy_warning],
                "applied_sections": {
                    "session_execution_policy": {
                        "status": "attention",
                        "warnings": ["Session retry-default retained its prior policy"]
                    },
                    "providers": {"status": "applied"}
                }
            }),
            ..ConfigReloadSnapshot::default()
        };
        assert!(session_execution_policy_retry_pending(&snapshot));

        let merged = merge_session_execution_policy_retry(
            snapshot,
            ConfigFingerprint {
                digest: "same-digest".to_string(),
                entries: Vec::new(),
                computed_at_ms: 1,
            },
            serde_json::json!({
                "status": "applied",
                "updated_active_sessions": 1,
                "warnings": []
            }),
            "auto",
        );

        assert_eq!(merged.status, "applied");
        assert_eq!(merged.trigger, "auto");
        assert_eq!(merged.last_report["trigger"], "auto");
        assert!(merged.warnings.is_empty());
        assert_eq!(
            merged
                .last_report
                .pointer("/applied_sections/session_execution_policy/status"),
            Some(&serde_json::json!("applied"))
        );
        assert_eq!(
            merged
                .last_report
                .pointer("/applied_sections/providers/status"),
            Some(&serde_json::json!("applied"))
        );
        assert!(!session_execution_policy_retry_pending(&merged));
    }

    #[test]
    fn hot_reload_routes_lark_config_to_shared_feishu_surface() {
        let mut extra = BTreeMap::new();
        extra.insert(
            "app_id".to_string(),
            runtime::JsonValue::String("app-id".to_string()),
        );
        let gateway = runtime::GatewayConfig {
            enabled: true,
            platforms: vec![runtime::GatewayPlatformConfig {
                platform_type: "lark".to_string(),
                enabled: true,
                extra,
            }],
            ..Default::default()
        };

        let configs = build_surface_runtime_configs(&gateway);
        assert!(configs.contains_key("feishu"));
        assert!(!configs.contains_key("lark"));
        assert_eq!(configs["feishu"]["platform_type"], "lark");
    }

    #[test]
    fn provider_activation_scope_distinguishes_checkout_session_and_restart() {
        let scope = provider_activation_scope();
        assert_eq!(
            scope["provider_credentials_and_protocol"],
            "subsequent_provider_checkout"
        );
        assert_eq!(
            scope["model_capacity_overrides"],
            "new_or_restored_session_runtime"
        );
        assert_eq!(scope["models_yaml"], "gateway_restart");
    }

    #[test]
    fn model_context_window_changes_do_not_claim_gateway_restart() {
        let startup = serde_json::json!({"model_context_windows": {"model-a": 128000}});
        let current = serde_json::json!({"model_context_windows": {"model-a": 1000000}});
        let report = restart_required_report(Some(&startup), &current);
        assert_eq!(report["required"], false);
        assert_eq!(report["fields"], serde_json::json!([]));
    }

    #[test]
    fn workspace_changes_require_restart() {
        let startup = serde_json::json!({"workspace": "/workspace/alpha"});
        let current = serde_json::json!({"workspace": "/workspace/beta"});
        let report = restart_required_report(Some(&startup), &current);
        assert_eq!(report["required"], true);
        assert_eq!(report["fields"], serde_json::json!(["workspace"]));
    }

    #[test]
    fn hot_state_topology_changes_require_restart_but_memory_budget_does_not() {
        let startup = serde_json::json!({
            "runtime": {
                "hot_state": {
                    "shards": "auto",
                    "materializer_queue_capacity": 1024,
                    "memory": {"ratio": "0.60"}
                }
            }
        });
        let memory_only = serde_json::json!({
            "runtime": {
                "hot_state": {
                    "shards": "auto",
                    "materializer_queue_capacity": 1024,
                    "memory": {"ratio": "0.70"}
                }
            }
        });
        let topology = serde_json::json!({
            "runtime": {
                "hot_state": {
                    "shards": 32,
                    "materializer_queue_capacity": 2048,
                    "memory": {"ratio": "0.70"}
                }
            }
        });

        assert_eq!(
            restart_required_report(Some(&startup), &memory_only)["required"],
            false
        );
        let report = restart_required_report(Some(&startup), &topology);
        assert_eq!(report["required"], true);
        assert_eq!(
            report["fields"],
            serde_json::json!([
                "runtime.hot_state.shards",
                "runtime.hot_state.materializer_queue_capacity"
            ])
        );
    }
}
