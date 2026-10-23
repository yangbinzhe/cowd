use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock, Weak,
    },
};

use async_trait::async_trait;
use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillLifecycleStatus, SkillUsageKind,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use skill::{profile_skill_catalog_entry, profile_skill_package, SkillInfo, SkillRegistry};

#[derive(Clone, Default)]
pub(crate) struct RuntimeSkillAssets {
    pub profiles: Vec<SkillCapabilityProfile>,
    pub prompt_assets: Vec<runtime::RuntimeSkillPromptAsset>,
    pub instruction_source: Option<Arc<dyn runtime::RuntimeSkillInstructionSource>>,
}

impl std::fmt::Debug for RuntimeSkillAssets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeSkillAssets")
            .field("profiles", &self.profiles.len())
            .field("prompt_assets", &self.prompt_assets.len())
            .field("has_instruction_source", &self.instruction_source.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceSkillSnapshot {
    pub skills: Vec<SkillInfo>,
    pub assets: RuntimeSkillAssets,
}

struct CachedWorkspaceSkillSnapshot {
    snapshot: Arc<WorkspaceSkillSnapshot>,
}

struct WorkspaceSkillSnapshotCell {
    current: Mutex<Option<CachedWorkspaceSkillSnapshot>>,
    usage: Arc<RuntimeSkillUsageRelay>,
    metrics: Arc<SkillInstructionCacheMetrics>,
}

impl Default for WorkspaceSkillSnapshotCell {
    fn default() -> Self {
        Self {
            current: Mutex::new(None),
            usage: Arc::new(RuntimeSkillUsageRelay::default()),
            metrics: Arc::new(SkillInstructionCacheMetrics::default()),
        }
    }
}

fn skill_snapshot_cells() -> &'static Mutex<HashMap<PathBuf, Arc<WorkspaceSkillSnapshotCell>>> {
    static CELLS: OnceLock<Mutex<HashMap<PathBuf, Arc<WorkspaceSkillSnapshotCell>>>> =
        OnceLock::new();
    CELLS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn workspace_skill_snapshot(workspace_root: &Path) -> Arc<WorkspaceSkillSnapshot> {
    let key = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    let cell = {
        let mut cells = skill_snapshot_cells()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            cells
                .entry(key)
                .or_insert_with(|| Arc::new(WorkspaceSkillSnapshotCell::default())),
        )
    };
    let mut current = cell
        .current
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(cached) = current.as_ref() {
        return Arc::clone(&cached.snapshot);
    }
    let registry = SkillRegistry::discover(workspace_root);
    let skills = registry.list().unwrap_or_else(|error| {
        tracing::debug!(
            %error,
            workspace_root = %workspace_root.display(),
            "skill snapshot discovery degraded"
        );
        Vec::new()
    });
    let assets = runtime_skill_assets_from_snapshot(
        workspace_root,
        &skills,
        Arc::clone(&cell.usage),
        Arc::clone(&cell.metrics),
    );
    let snapshot = Arc::new(WorkspaceSkillSnapshot { skills, assets });
    *current = Some(CachedWorkspaceSkillSnapshot {
        snapshot: Arc::clone(&snapshot),
    });
    snapshot
}

pub(crate) fn attach_workspace_skill_usage_sink(
    workspace_root: &Path,
    sink: Arc<dyn runtime::RuntimeSkillUsageSink>,
) {
    let cell = workspace_skill_snapshot_cell(workspace_root);
    cell.usage.attach(sink);
}

#[must_use]
pub(crate) fn workspace_skill_cache_health(workspace_root: &Path) -> SkillCacheHealth {
    let cell = workspace_skill_snapshot_cell(workspace_root);
    let mut health = cell.metrics.health();
    let usage = cell.usage.health();
    health.usage_accepted = usage.accepted;
    health.usage_persisted = usage.persisted;
    health.usage_dropped = usage.dropped;
    health.usage_persistence_failures = usage.persistence_failures;
    health
}

fn workspace_skill_snapshot_cell(workspace_root: &Path) -> Arc<WorkspaceSkillSnapshotCell> {
    let key = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    let mut cells = skill_snapshot_cells()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        cells
            .entry(key)
            .or_insert_with(|| Arc::new(WorkspaceSkillSnapshotCell::default())),
    )
}

pub(crate) fn invalidate_workspace_skill_snapshot(workspace_root: &Path) {
    let key = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    if let Some(cell) = skill_snapshot_cells()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
        .cloned()
    {
        *cell
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

pub(crate) fn runtime_skill_profiles_for_workspace(
    workspace_root: &Path,
) -> Vec<SkillCapabilityProfile> {
    runtime_skill_assets_for_workspace(workspace_root).profiles
}

/// Gateway owns package discovery and inspection. Runtime receives only the
/// lightweight capability catalog and a lazy instruction source; selected
/// PromptOnly Markdown is loaded and cached without coupling Runtime to the
/// open Skill registry or package filesystem.
pub(crate) fn runtime_skill_assets_for_workspace(workspace_root: &Path) -> RuntimeSkillAssets {
    workspace_skill_snapshot(workspace_root).assets.clone()
}

fn runtime_skill_assets_from_snapshot(
    workspace_root: &Path,
    skills: &[SkillInfo],
    usage: Arc<RuntimeSkillUsageRelay>,
    metrics: Arc<SkillInstructionCacheMetrics>,
) -> RuntimeSkillAssets {
    let mut assets = RuntimeSkillAssets::default();
    for skill in skills
        .iter()
        .cloned()
        .filter(|skill| skill.shadowed_by.is_none())
    {
        let root = if skill.path.is_file() {
            skill
                .path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            skill.path.clone()
        };
        let profile = match profile_skill_catalog_entry(
            &skill.path,
            &skill.name,
            skill.description.as_deref(),
            skill.version.clone(),
        ) {
            Ok(profile) => profile,
            Err(error) => {
                tracing::debug!(
                    %error,
                    skill = %skill.name,
                    path = %root.display(),
                    "runtime skill profile skipped"
                );
                continue;
            }
        };
        assets.profiles.push(profile);
    }
    assets.instruction_source = Some(Arc::new(WorkspaceSkillInstructionSource::new(
        workspace_root,
        &assets.profiles,
        usage,
        metrics,
    )));
    assets
}

const SKILL_INSTRUCTION_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
struct SkillInstructionDescriptor {
    root: PathBuf,
    profile: SkillCapabilityProfile,
    inspected_profile: Arc<OnceLock<Result<SkillCapabilityProfile, String>>>,
    inspection_flight: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Clone)]
struct CachedSkillInstruction {
    asset: runtime::RuntimeSkillPromptAsset,
    bytes: usize,
    last_access: u64,
}

#[derive(Debug, Default)]
struct SkillInstructionCache {
    entries: HashMap<String, CachedSkillInstruction>,
    resident_bytes: usize,
    clock: u64,
}

#[derive(Debug)]
struct WorkspaceSkillInstructionSource {
    workspace_root: PathBuf,
    managed_store: Result<skill::ManagedSkillStore, String>,
    descriptors: Arc<HashMap<String, SkillInstructionDescriptor>>,
    cache: Mutex<SkillInstructionCache>,
    flights: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    usage: Arc<RuntimeSkillUsageRelay>,
    metrics: Arc<SkillInstructionCacheMetrics>,
}

impl WorkspaceSkillInstructionSource {
    fn new(
        workspace_root: &Path,
        profiles: &[SkillCapabilityProfile],
        usage: Arc<RuntimeSkillUsageRelay>,
        metrics: Arc<SkillInstructionCacheMetrics>,
    ) -> Self {
        let descriptors = profiles
            .iter()
            .cloned()
            .map(|profile| {
                (
                    profile.skill_id.clone(),
                    SkillInstructionDescriptor {
                        root: PathBuf::from(&profile.source_root),
                        profile,
                        inspected_profile: Arc::new(OnceLock::new()),
                        inspection_flight: Arc::new(tokio::sync::Mutex::new(())),
                    },
                )
            })
            .collect();
        Self {
            workspace_root: workspace_root.to_path_buf(),
            managed_store: skill::ManagedSkillStore::default_for_user()
                .map_err(|error| error.to_string()),
            descriptors: Arc::new(descriptors),
            cache: Mutex::new(SkillInstructionCache::default()),
            flights: Mutex::new(HashMap::new()),
            usage,
            metrics,
        }
    }

    fn cache_key(descriptor: &SkillInstructionDescriptor, revision: &str) -> String {
        format!(
            "{}:{}:{}",
            descriptor.profile.skill_id,
            revision,
            descriptor
                .profile
                .version
                .as_deref()
                .unwrap_or("unversioned")
        )
    }

    fn cached(&self, key: &str) -> Option<runtime::RuntimeSkillPromptAsset> {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.clock = cache.clock.saturating_add(1);
        let clock = cache.clock;
        let asset = cache.entries.get_mut(key).map(|entry| {
            entry.last_access = clock;
            entry.asset.clone()
        });
        if asset.is_some() {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
        }
        asset
    }

    fn insert(&self, key: String, asset: runtime::RuntimeSkillPromptAsset) {
        let bytes = asset
            .content
            .len()
            .saturating_add(asset.skill_id.len())
            .saturating_add(asset.source_ref.len())
            .saturating_add(asset.tool_refs.iter().map(String::len).sum::<usize>());
        if bytes > SKILL_INSTRUCTION_CACHE_MAX_BYTES {
            self.metrics.oversized.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.clock = cache.clock.saturating_add(1);
        let clock = cache.clock;
        if let Some(previous) = cache.entries.remove(&key) {
            cache.resident_bytes = cache.resident_bytes.saturating_sub(previous.bytes);
        }
        cache.resident_bytes = cache.resident_bytes.saturating_add(bytes);
        cache.entries.insert(
            key,
            CachedSkillInstruction {
                asset,
                bytes,
                last_access: clock,
            },
        );
        while cache.resident_bytes > SKILL_INSTRUCTION_CACHE_MAX_BYTES {
            let Some(victim) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(removed) = cache.entries.remove(&victim) {
                cache.resident_bytes = cache.resident_bytes.saturating_sub(removed.bytes);
                self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.metrics
            .resident_bytes
            .store(cache.resident_bytes as u64, Ordering::Relaxed);
        self.metrics
            .resident_entries
            .store(cache.entries.len() as u64, Ordering::Relaxed);
    }

    fn flight(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut flights = self
            .flights
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
            return flight;
        }
        let flight = Arc::new(tokio::sync::Mutex::new(()));
        flights.insert(key.to_string(), Arc::downgrade(&flight));
        flight
    }

    async fn inspected_profile(
        descriptor: &SkillInstructionDescriptor,
    ) -> Result<SkillCapabilityProfile, String> {
        if let Some(profile) = descriptor.inspected_profile.get() {
            return profile.clone();
        }
        let _inspection = descriptor.inspection_flight.lock().await;
        if let Some(profile) = descriptor.inspected_profile.get() {
            return profile.clone();
        }
        let root = descriptor.root.clone();
        let name = descriptor.profile.name.clone();
        let version = descriptor.profile.version.clone();
        let result = tokio::task::spawn_blocking(move || {
            profile_skill_package(&root, &name, version).map_err(|error| {
                format!("failed to inspect Skill package before activation: {error}")
            })
        })
        .await
        .map_err(|error| format!("Skill package inspector failed: {error}"))?;
        let _ = descriptor.inspected_profile.set(result.clone());
        result
    }
}

#[async_trait]
impl runtime::RuntimeSkillInstructionSource for WorkspaceSkillInstructionSource {
    fn inspect_revision(
        &self,
        skill_id: &str,
        revision: &str,
        receipt_id: &str,
    ) -> Result<harness_contract::skill::SkillInstallationEvidence, String> {
        let store = self.managed_store.as_ref().map_err(Clone::clone)?;
        let installed = store
            .resolve_installed(skill_id, revision, receipt_id)
            .map_err(|error| error.to_string())?;
        let workspace = self
            .workspace_root
            .canonicalize()
            .map_err(|error| error.to_string())?;
        let store_root = store
            .root()
            .canonicalize()
            .map_err(|error| error.to_string())?;
        let scope =
            serde_json::to_vec(&("cowd.skill.installation.scope.v1", workspace, store_root))
                .map_err(|error| error.to_string())?;
        let receipt = serde_json::to_vec(&installed.receipt).map_err(|error| error.to_string())?;
        Ok(harness_contract::skill::SkillInstallationEvidence {
            scope_ref: format!("sha256:{:x}", Sha256::digest(scope)),
            skill_id: installed.receipt.skill_id,
            revision: installed.receipt.revision,
            receipt_id: installed.receipt.install_id,
            receipt_digest: format!("sha256:{:x}", Sha256::digest(receipt)),
            inspection_fingerprint: installed.inspection_fingerprint,
        })
    }

    async fn load_instruction(
        &self,
        invocation: &runtime::SkillInvocation,
        usage_context: &runtime::RuntimeSkillUsageContext,
    ) -> Result<Option<runtime::RuntimeSkillPromptAsset>, String> {
        let Some(descriptor) = self.descriptors.get(&invocation.skill_id).cloned() else {
            return Ok(None);
        };
        let inspected = match Self::inspected_profile(&descriptor).await {
            Ok(profile) => profile,
            Err(error) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                let unavailable_revision =
                    format!("uninspectable:{}", descriptor.profile.package_fingerprint);
                self.usage.record(
                    invocation,
                    &unavailable_revision,
                    usage_context,
                    SkillUsageKind::Failure,
                );
                return Err(error);
            }
        };
        let revision = inspected.package_fingerprint.clone();
        match self.usage.active_pointer(&invocation.skill_id) {
            Ok(Some(pointer)) if pointer.active_revision != revision => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                self.usage.record(
                    invocation,
                    &revision,
                    usage_context,
                    SkillUsageKind::Failure,
                );
                return Err(format!(
                    "Skill {} revision {} is not the approved active revision {}",
                    invocation.skill_id, revision, pointer.active_revision
                ));
            }
            Err(error) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                self.usage.record(
                    invocation,
                    &revision,
                    usage_context,
                    SkillUsageKind::Failure,
                );
                return Err(format!(
                    "Skill {} active revision could not be verified: {error}",
                    invocation.skill_id
                ));
            }
            Ok(Some(_)) | Ok(None) => {}
        }
        let key = Self::cache_key(&descriptor, &revision);
        if let Some(asset) = self.cached(&key) {
            self.usage
                .record(invocation, &revision, usage_context, SkillUsageKind::Hit);
            return Ok(Some(asset));
        }
        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        self.usage
            .record(invocation, &revision, usage_context, SkillUsageKind::Miss);
        let flight = self.flight(&key);
        let _guard = flight.lock().await;
        if let Some(asset) = self.cached(&key) {
            self.usage
                .record(invocation, &revision, usage_context, SkillUsageKind::Hit);
            return Ok(Some(asset));
        }
        let root = descriptor.root.clone();
        let loaded =
            tokio::task::spawn_blocking(move || load_prompt_asset(&root, &inspected)).await;
        let asset = match loaded {
            Ok(Ok(asset)) => asset,
            Ok(Err(error)) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                self.usage.record(
                    invocation,
                    &revision,
                    usage_context,
                    SkillUsageKind::Failure,
                );
                return Err(error);
            }
            Err(error) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                self.usage.record(
                    invocation,
                    &revision,
                    usage_context,
                    SkillUsageKind::Failure,
                );
                return Err(format!("Skill instruction loader failed: {error}"));
            }
        };
        if let Some(asset) = asset.as_ref() {
            self.insert(key, asset.clone());
            self.metrics.loads.fetch_add(1, Ordering::Relaxed);
            self.usage
                .record(invocation, &revision, usage_context, SkillUsageKind::Load);
        }
        Ok(asset)
    }
}

fn load_prompt_asset(
    root: &Path,
    profile: &SkillCapabilityProfile,
) -> Result<Option<runtime::RuntimeSkillPromptAsset>, String> {
    if profile.lifecycle_status == SkillLifecycleStatus::Blocked {
        return Err(format!(
            "Skill {} is blocked by package inspection: {}",
            profile.skill_id,
            profile.inspection_summary.join(", ")
        ));
    }
    if !profile.adapters.contains(&SkillAdapterKind::PromptOnly) {
        return Ok(None);
    }
    Ok(prompt_asset_for_profile(root, profile))
}

fn prompt_asset_for_profile(
    root: &Path,
    profile: &SkillCapabilityProfile,
) -> Option<runtime::RuntimeSkillPromptAsset> {
    let entrypoint = profile
        .entrypoints
        .iter()
        .find(|entrypoint| entrypoint.adapter == SkillAdapterKind::PromptOnly)?;
    let root = root.canonicalize().ok()?;
    let path = root.join(&entrypoint.path).canonicalize().ok()?;
    if !path.starts_with(&root) || !path.is_file() {
        tracing::warn!(
            skill = %profile.skill_id,
            path = %path.display(),
            "runtime skill prompt asset escaped package root or is not a file"
        );
        return None;
    }
    let content = std::fs::read_to_string(&path).ok()?;
    let (content, tool_refs) = if profile.skill_id.starts_with("lark-") {
        (
            format!(
                "# Cowd Lark execution bridge\n\nUse this Skill only when the current user request is explicitly about Feishu/Lark operations (messaging, docs, sheets, slides, tasks, calendar, or contacts). For any other task, ignore this Skill and use normal workspace tools. When used, call `lark_cli_read` for reads and `lark_cli_write` for mutations; pass only argv entries after `lark-cli`. Never use Bash to locate credentials, never ask the user to repeat configured app credentials, and never run CLI auth/config/profile/update commands. The gateway supplies a short-lived bot token, enforces the official CLI risk class, and applies Cowd approval policy. If an operation requires user identity rather than bot identity, explain that boundary instead of silently changing identity.\n\n{content}"
            ),
            vec!["lark_cli_read".to_string(), "lark_cli_write".to_string()],
        )
    } else {
        (content, Vec::new())
    };
    if content.trim().is_empty() {
        return None;
    }
    Some(runtime::RuntimeSkillPromptAsset {
        skill_id: profile.skill_id.clone(),
        version: profile.version.clone(),
        content,
        source_ref: format!("skill://{}/{}", profile.skill_id, entrypoint.path),
        tool_refs,
    })
}

#[derive(Default)]
struct RuntimeSkillUsageRelay {
    sink: RwLock<Option<Arc<dyn runtime::RuntimeSkillUsageSink>>>,
}

impl std::fmt::Debug for RuntimeSkillUsageRelay {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeSkillUsageRelay")
            .field(
                "attached",
                &self
                    .sink
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some(),
            )
            .finish()
    }
}

impl RuntimeSkillUsageRelay {
    fn attach(&self, sink: Arc<dyn runtime::RuntimeSkillUsageSink>) {
        *self
            .sink
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sink);
    }

    fn record(
        &self,
        invocation: &runtime::SkillInvocation,
        revision: &str,
        context: &runtime::RuntimeSkillUsageContext,
        usage: SkillUsageKind,
    ) {
        let sink = self
            .sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sink) = sink else {
            return;
        };
        // Admission is best-effort observation, not proof of durable usage.
        // Maintenance consumes only original committed EventStore receipts.
        let _ = sink.observe(invocation, revision, context, usage);
    }

    fn health(&self) -> runtime::RuntimeSkillUsageSinkHealth {
        self.sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|sink| sink.health())
            .unwrap_or_default()
    }

    fn active_pointer(
        &self,
        skill_id: &str,
    ) -> Result<Option<harness_contract::skill::SkillActivePointer>, String> {
        let sink = self
            .sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned();
        sink.map_or(Ok(None), |sink| sink.active_pointer(skill_id))
    }
}

#[derive(Debug, Default)]
struct SkillInstructionCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    loads: AtomicU64,
    failures: AtomicU64,
    evictions: AtomicU64,
    oversized: AtomicU64,
    resident_bytes: AtomicU64,
    resident_entries: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct SkillCacheHealth {
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub failures: u64,
    pub evictions: u64,
    pub oversized: u64,
    pub resident_bytes: u64,
    pub resident_entries: u64,
    pub usage_accepted: u64,
    pub usage_persisted: u64,
    pub usage_dropped: u64,
    pub usage_persistence_failures: u64,
}

impl SkillInstructionCacheMetrics {
    fn health(&self) -> SkillCacheHealth {
        SkillCacheHealth {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            loads: self.loads.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            oversized: self.oversized.load(Ordering::Relaxed),
            resident_bytes: self.resident_bytes.load(Ordering::Relaxed),
            resident_entries: self.resident_entries.load(Ordering::Relaxed),
            usage_accepted: 0,
            usage_persisted: 0,
            usage_dropped: 0,
            usage_persistence_failures: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::{SkillActivePointer, SkillAdapterKind};
    use std::time::{Duration, Instant};

    struct TempWorkspace {
        root: std::path::PathBuf,
    }

    impl TempWorkspace {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowd-runtime-skill-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&root).expect("temp workspace should be created");
            Self { root }
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn usage_context(label: &str) -> runtime::RuntimeSkillUsageContext {
        runtime::RuntimeSkillUsageContext {
            workspace_identity: "workspace".to_string(),
            workload_fingerprint: format!("workload:{label}"),
            config_revision: "config".to_string(),
            evaluation_environment: "test".to_string(),
            execution_id: format!("execution:{label}"),
            session_id: "session".to_string(),
            turn_id: format!("turn:{label}"),
            observed_at_ms: 1,
        }
    }

    struct FixedPointerSink {
        pointer: SkillActivePointer,
    }

    impl runtime::RuntimeSkillUsageSink for FixedPointerSink {
        fn observe(
            &self,
            _invocation: &runtime::SkillInvocation,
            _skill_revision: &str,
            _context: &runtime::RuntimeSkillUsageContext,
            _usage: SkillUsageKind,
        ) -> Option<String> {
            None
        }

        fn health(&self) -> runtime::RuntimeSkillUsageSinkHealth {
            runtime::RuntimeSkillUsageSinkHealth::default()
        }

        fn active_pointer(&self, _skill_id: &str) -> Result<Option<SkillActivePointer>, String> {
            Ok(Some(self.pointer.clone()))
        }
    }

    struct UnavailablePointerSink;

    impl runtime::RuntimeSkillUsageSink for UnavailablePointerSink {
        fn observe(
            &self,
            _invocation: &runtime::SkillInvocation,
            _skill_revision: &str,
            _context: &runtime::RuntimeSkillUsageContext,
            _usage: SkillUsageKind,
        ) -> Option<String> {
            None
        }

        fn health(&self) -> runtime::RuntimeSkillUsageSinkHealth {
            runtime::RuntimeSkillUsageSinkHealth::default()
        }

        fn active_pointer(&self, _skill_id: &str) -> Result<Option<SkillActivePointer>, String> {
            Err("pointer store unavailable".to_string())
        }
    }

    #[test]
    fn runtime_skill_profile_provider_uses_workspace_registry() {
        let temp = TempWorkspace::new("profile-provider");
        let skill_root = temp
            .root
            .join(".cowd")
            .join("skills")
            .join("release-review");
        std::fs::create_dir_all(&skill_root).expect("skill root should be created");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Release Review\ndescription: Review release plans.\ntags: [release, review]\n---\n\nReview release evidence.",
        )
        .expect("skill should be written");

        let profiles = runtime_skill_profiles_for_workspace(&temp.root);

        let profile = profiles
            .iter()
            .find(|profile| profile.skill_id == "release-review")
            .expect("workspace skill profile should be discovered");
        assert_eq!(profile.name, "Release Review");
        assert!(profile.adapters.contains(&SkillAdapterKind::PromptOnly));
        assert!(profile
            .entrypoints
            .iter()
            .any(|entrypoint| entrypoint.path == "SKILL.md"));
    }

    #[tokio::test]
    async fn runtime_skill_assets_page_in_selected_prompt_only_instruction() {
        let temp = TempWorkspace::new("prompt-assets");
        let skill_root = temp
            .root
            .join(".cowd")
            .join("skills")
            .join("release-review");
        std::fs::create_dir_all(&skill_root).expect("skill root should be created");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Release Review\ndescription: Review release plans.\n---\n\n# Release\nRequire explicit evidence.",
        )
        .expect("skill should be written");

        let assets = runtime_skill_assets_for_workspace(&temp.root);

        assert!(assets
            .profiles
            .iter()
            .any(|profile| profile.skill_id == "release-review"));
        assert!(
            assets.prompt_assets.is_empty(),
            "cold Skill Markdown must not be resident in the catalog"
        );
        let source = assets
            .instruction_source
            .expect("workspace Skill source must be available");
        let invocation = runtime::SkillInvocation {
            skill_id: "release-review".to_string(),
            skill_version: None,
            adapter: SkillAdapterKind::PromptOnly,
            entrypoint: None,
        };
        let asset = source
            .load_instruction(&invocation, &usage_context("first"))
            .await
            .expect("instruction page-in")
            .expect("prompt asset");
        assert!(asset.content.contains("Require explicit evidence."));
        assert!(asset.tool_refs.is_empty());

        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Release Review\ndescription: Review release plans.\n---\n\n# Changed\nNew generation.",
        )
        .expect("Skill update");
        let pinned = source
            .load_instruction(&invocation, &usage_context("pinned"))
            .await
            .expect("pinned generation page-in")
            .expect("pinned asset");
        assert!(
            pinned.content.contains("Require explicit evidence."),
            "an active catalog generation must remain immutable"
        );

        invalidate_workspace_skill_snapshot(&temp.root);
        let reloaded = runtime_skill_assets_for_workspace(&temp.root);
        let updated = reloaded
            .instruction_source
            .expect("reloaded workspace Skill source")
            .load_instruction(&invocation, &usage_context("reloaded"))
            .await
            .expect("updated generation page-in")
            .expect("updated prompt asset");
        assert!(
            updated.content.contains("New generation."),
            "new turns must observe the atomically published generation"
        );
        let health = workspace_skill_cache_health(&temp.root);
        assert!(health.loads >= 2);
        assert!(health.hits >= 1);
        assert!(health.resident_entries >= 1);
        assert!(health.resident_bytes > 0);
    }

    #[tokio::test]
    async fn first_activation_performs_full_package_inspection_and_rejects_blocked_skill() {
        let temp = TempWorkspace::new("blocked-package");
        let skill_root = temp.root.join(".cowd").join("skills").join("unsafe-cache");
        std::fs::create_dir_all(skill_root.join("node_modules"))
            .expect("heavy dependency directory");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Unsafe Cache\ndescription: Catalog remains cheap.\n---\n\nNever injected.",
        )
        .expect("skill should be written");

        let assets = runtime_skill_assets_for_workspace(&temp.root);
        assert!(
            assets
                .profiles
                .iter()
                .any(|profile| profile.skill_id == "unsafe-cache"),
            "light catalog discovery must not recursively inspect the package"
        );
        let invocation = runtime::SkillInvocation {
            skill_id: "unsafe-cache".to_string(),
            skill_version: None,
            adapter: SkillAdapterKind::PromptOnly,
            entrypoint: None,
        };
        let error = assets
            .instruction_source
            .expect("instruction source")
            .load_instruction(&invocation, &usage_context("blocked"))
            .await
            .expect_err("full first-use inspection must block heavy package contents");
        assert!(error.contains("blocked by package inspection"));

        let health = workspace_skill_cache_health(&temp.root);
        assert_eq!(health.loads, 0);
        assert_eq!(health.failures, 1);
        assert_eq!(health.resident_entries, 0);
    }

    #[tokio::test]
    async fn approved_pointer_fences_only_future_instruction_activations() {
        let temp = TempWorkspace::new("active-pointer");
        let skill_root = temp.root.join(".cowd").join("skills").join("review");
        std::fs::create_dir_all(&skill_root).expect("skill root");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Review\nversion: 1.0.0\ndescription: Review evidence.\n---\n\nPinned old content.",
        )
        .expect("skill");
        let sink: Arc<dyn runtime::RuntimeSkillUsageSink> = Arc::new(FixedPointerSink {
            pointer: SkillActivePointer {
                skill_id: "review".to_string(),
                active_revision: "2.0.0".to_string(),
                previous_revision: Some("1.0.0".to_string()),
                generation: 1,
                source_draft_id: Some("draft".to_string()),
                approval_ref: "approval".to_string(),
                activated_at_ms: 2,
                installation: None,
            },
        });
        attach_workspace_skill_usage_sink(&temp.root, sink);
        let assets = runtime_skill_assets_for_workspace(&temp.root);
        let invocation = runtime::SkillInvocation {
            skill_id: "review".to_string(),
            skill_version: Some("1.0.0".to_string()),
            adapter: SkillAdapterKind::PromptOnly,
            entrypoint: None,
        };
        let error = assets
            .instruction_source
            .expect("source")
            .load_instruction(&invocation, &usage_context("future-turn"))
            .await
            .expect_err("new activation of old revision must be fenced");
        assert!(error.contains("approved active revision 2.0.0"));
        assert_eq!(workspace_skill_cache_health(&temp.root).failures, 1);
    }

    #[tokio::test]
    async fn approved_full_package_fingerprint_allows_exact_revision_page_in() {
        let temp = TempWorkspace::new("approved-exact-pointer");
        let skill_root = temp.root.join(".cowd").join("skills").join("review");
        std::fs::create_dir_all(&skill_root).expect("skill root");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Review\nversion: 1.0.0\ndescription: Review evidence.\n---\n\nApproved content.",
        )
        .expect("skill");
        let inspected = profile_skill_package(&skill_root, "Review", Some("1.0.0".to_string()))
            .expect("full inspection");
        let sink: Arc<dyn runtime::RuntimeSkillUsageSink> = Arc::new(FixedPointerSink {
            pointer: SkillActivePointer {
                skill_id: "review".to_string(),
                active_revision: inspected.package_fingerprint,
                previous_revision: None,
                generation: 1,
                source_draft_id: Some("draft".to_string()),
                approval_ref: "approval".to_string(),
                activated_at_ms: 2,
                installation: None,
            },
        });
        attach_workspace_skill_usage_sink(&temp.root, sink);
        let asset = runtime_skill_assets_for_workspace(&temp.root)
            .instruction_source
            .expect("source")
            .load_instruction(
                &runtime::SkillInvocation {
                    skill_id: "review".to_string(),
                    skill_version: Some("1.0.0".to_string()),
                    adapter: SkillAdapterKind::PromptOnly,
                    entrypoint: None,
                },
                &usage_context("approved-exact"),
            )
            .await
            .expect("approved revision")
            .expect("prompt");
        assert!(asset.content.contains("Approved content."));
    }

    #[test]
    fn installed_revision_reader_binds_original_receipt_scope_and_full_bytes() {
        use runtime::RuntimeSkillInstructionSource;
        let temp = TempWorkspace::new("installed-reader");
        let package_root = temp.root.join("package");
        std::fs::create_dir_all(&package_root).unwrap();
        std::fs::write(package_root.join("SKILL.md"), "---\nname: Review\nversion: 1.0.0\ndescription: Original receipt.\n---\nOriginal full bytes.\n").unwrap();
        // A higher-precedence catalog entry cannot substitute its bytes or
        // prevent exact inspection of the installed candidate being reviewed.
        let shadow = temp.root.join(".cowd/skills/review");
        std::fs::create_dir_all(&shadow).unwrap();
        std::fs::write(
            shadow.join("SKILL.md"),
            "---\nname: Review\ndescription: Foreign catalog candidate.\n---\nDifferent bytes.\n",
        )
        .unwrap();
        let store = skill::ManagedSkillStore::new(temp.root.join("installed"));
        let lifecycle = skill::SkillLifecycle::new(store.clone()).unwrap();
        let plan = lifecycle
            .plan(package_root.to_str().unwrap(), &temp.root)
            .unwrap();
        let receipt = lifecycle
            .commit(
                package_root.to_str().unwrap(),
                &temp.root,
                &plan.package_digest,
                false,
                "human:test",
            )
            .unwrap();
        let mut source = WorkspaceSkillInstructionSource::new(
            &temp.root,
            &[],
            Arc::new(RuntimeSkillUsageRelay::default()),
            Arc::new(SkillInstructionCacheMetrics::default()),
        );
        source.managed_store = Ok(store.clone());
        let evidence = source
            .inspect_revision("review", &receipt.revision, &receipt.install_id)
            .unwrap();
        assert_eq!(evidence.receipt_id, receipt.install_id);
        assert_eq!(evidence.revision, plan.package_digest);
        assert_eq!(evidence.inspection_fingerprint, receipt.plan_digest);
        assert!(source
            .inspect_revision("foreign", &receipt.revision, &receipt.install_id)
            .is_err());
        assert!(source
            .inspect_revision("review", &receipt.revision, "../foreign")
            .is_err());
        let other_workspace = temp.root.join("other-workspace");
        std::fs::create_dir_all(&other_workspace).unwrap();
        source.workspace_root = other_workspace;
        let other_scope = source
            .inspect_revision("review", &receipt.revision, &receipt.install_id)
            .unwrap();
        assert_ne!(other_scope.scope_ref, evidence.scope_ref);
        let receipt_path = store
            .root()
            .join("review/receipts")
            .join(format!("{}.json", receipt.install_id));
        let mut altered = serde_json::to_value(&receipt).unwrap();
        altered["plan_digest"] = serde_json::json!("fabricated inspection");
        std::fs::write(receipt_path, serde_json::to_vec(&altered).unwrap()).unwrap();
        assert!(source
            .inspect_revision("review", &receipt.revision, &receipt.install_id)
            .is_err());
    }

    #[tokio::test]
    async fn unavailable_active_pointer_fails_closed_before_page_in() {
        let temp = TempWorkspace::new("unavailable-pointer");
        let skill_root = temp.root.join(".cowd").join("skills").join("review");
        std::fs::create_dir_all(&skill_root).expect("skill root");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Review\ndescription: Review evidence.\n---\n\nNever loaded.",
        )
        .expect("skill");
        let sink: Arc<dyn runtime::RuntimeSkillUsageSink> = Arc::new(UnavailablePointerSink);
        attach_workspace_skill_usage_sink(&temp.root, sink);
        let assets = runtime_skill_assets_for_workspace(&temp.root);
        let invocation = runtime::SkillInvocation {
            skill_id: "review".to_string(),
            skill_version: None,
            adapter: SkillAdapterKind::PromptOnly,
            entrypoint: None,
        };
        let error = assets
            .instruction_source
            .expect("source")
            .load_instruction(&invocation, &usage_context("pointer-unavailable"))
            .await
            .expect_err("pointer verification failure must block page-in");
        assert!(error.contains("active revision could not be verified"));
        let health = workspace_skill_cache_health(&temp.root);
        assert_eq!(health.loads, 0);
        assert_eq!(health.failures, 1);
    }

    #[tokio::test]
    async fn canonical_runtime_usage_receipts_are_persisted_off_the_load_path() {
        let temp = TempWorkspace::new("usage-events");
        let skill_root = temp.root.join(".cowd").join("skills").join("research");
        std::fs::create_dir_all(&skill_root).expect("skill root");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Research\ndescription: Investigate evidence.\n---\n\nInvestigate.",
        )
        .expect("skill");
        let store = Arc::new(runtime::RuntimeEventStore::for_test());
        let recorder = Arc::new(
            runtime::RuntimeSkillUsageRecorder::new(Arc::clone(&store)).expect("usage worker"),
        );
        let usage_sink: Arc<dyn runtime::RuntimeSkillUsageSink> = recorder.clone();
        let source = runtime_skill_assets_for_workspace(&temp.root)
            .instruction_source
            .expect("instruction source");
        attach_workspace_skill_usage_sink(&temp.root, usage_sink);

        let invocation = runtime::SkillInvocation {
            skill_id: "research".to_string(),
            skill_version: None,
            adapter: SkillAdapterKind::PromptOnly,
            entrypoint: None,
        };
        source
            .load_instruction(&invocation, &usage_context("usage-first"))
            .await
            .expect("first load")
            .expect("prompt");
        source
            .load_instruction(&invocation, &usage_context("usage-hit"))
            .await
            .expect("cache hit")
            .expect("prompt");

        let drained = recorder.shutdown_and_drain(Duration::from_secs(2)).await;
        assert!(drained.clean, "{drained:?}");
        let events = store
            .list_scope(runtime::RuntimeEventScope::Skill, 10)
            .expect("original Skill receipts");
        assert!(
            events
                .iter()
                .any(|event| event.kind == runtime::SKILL_USAGE_RECEIPT_EVENT_KIND),
            "usage telemetry must persist through the selected RuntimeEventStore"
        );
        let expected_revision = profile_skill_package(&skill_root, "Research", None)
            .expect("full inspection")
            .package_fingerprint;
        assert!(
            events.iter().any(|event| {
                event.kind == runtime::SKILL_USAGE_RECEIPT_EVENT_KIND
                    && event
                        .payload
                        .get("receipt")
                        .and_then(|receipt| receipt.get("skill_revision"))
                        .and_then(serde_json::Value::as_str)
                        == Some(expected_revision.as_str())
            }),
            "Receipt and active pointer must use the full immutable package fingerprint"
        );
        assert!(
            workspace_skill_cache_health(&temp.root).usage_persisted > 0,
            "health projection must expose asynchronous persistence"
        );
    }

    #[tokio::test]
    #[ignore = "run scripts/test/lark-live.sh with COWD_LIVE_LARK_SKILL_TEST=1"]
    async fn live_cowd_lark_skills_are_discovered_and_selected_by_runtime() {
        assert_eq!(
            std::env::var("COWD_LIVE_LARK_SKILL_TEST").as_deref(),
            Ok("1"),
            "live Lark skill test requires COWD_LIVE_LARK_SKILL_TEST=1"
        );
        let assets = runtime_skill_assets_for_workspace(Path::new("."));
        for (query, expected) in [
            ("请使用 lark-base 查询多维表格", "lark-base"),
            ("请使用 lark-im 搜索群聊消息", "lark-im"),
        ] {
            let decision = runtime::skill::SkillActivationEngine::activate(
                runtime::skill::SkillActivationInput {
                    session_id: "lark-live-skill-test".to_string(),
                    turn_index: 0,
                    query: query.to_string(),
                    capability_refs: Vec::new(),
                    available_profiles: assets.profiles.clone(),
                    agent_profile: harness_contract::skill::AgentSkillProfile {
                        adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
                        ..Default::default()
                    },
                },
            );
            let selected = decision
                .activations
                .first()
                .and_then(|activation| activation.selected_invocation.as_ref())
                .expect("Lark skill should be selected");
            assert_eq!(selected.skill_id, expected);
            let prompt = assets
                .instruction_source
                .as_ref()
                .expect("live Skill instruction source")
                .load_instruction(selected, &usage_context(expected))
                .await
                .expect("live Skill page-in")
                .expect("selected Lark skill should have a prompt asset");
            assert!(prompt.content.contains("lark-cli"));
            assert_eq!(
                prompt.tool_refs,
                vec!["lark_cli_read".to_string(), "lark_cli_write".to_string()]
            );
        }
    }
}
