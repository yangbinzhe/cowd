use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock, Weak,
    },
};

use async_trait::async_trait;
use harness_contract::skill::{SkillActivePointer, SkillInstallationEvidence, SkillInstructionPin};
use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillLifecycleStatus, SkillUsageKind,
};
use runtime::RuntimeSkillInstructionSource;
use serde::Serialize;
use sha2::{Digest, Sha256};
use skill::{profile_skill_catalog_entry, SkillInfo, SkillRegistry};

#[derive(Clone, Default)]
pub(crate) struct RuntimeSkillAssets {
    pub profiles: Vec<SkillCapabilityProfile>,
    pub instruction_source: Option<Arc<dyn runtime::RuntimeSkillInstructionSource>>,
}

impl std::fmt::Debug for RuntimeSkillAssets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeSkillAssets")
            .field("profiles", &self.profiles.len())
            .field("has_instruction_source", &self.instruction_source.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkspaceSkillSnapshot {
    pub skills: Vec<SkillInfo>,
    pub assets: RuntimeSkillAssets,
    pub discovery_error: Option<String>,
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
    let (skills, discovery_error) = match registry.list() {
        Ok(skills) => (skills, None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    };
    let assets = runtime_skill_assets_from_snapshot(
        workspace_root,
        &skills,
        Arc::clone(&cell.usage),
        Arc::clone(&cell.metrics),
    );
    let snapshot = Arc::new(WorkspaceSkillSnapshot {
        skills,
        assets,
        discovery_error,
    });
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
    cache: Mutex<SkillInstructionCache>,
    flights: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    usage: Arc<RuntimeSkillUsageRelay>,
    metrics: Arc<SkillInstructionCacheMetrics>,
}

impl WorkspaceSkillInstructionSource {
    fn new(
        workspace_root: &Path,
        _profiles: &[SkillCapabilityProfile],
        usage: Arc<RuntimeSkillUsageRelay>,
        metrics: Arc<SkillInstructionCacheMetrics>,
    ) -> Self {
        Self {
            workspace_root: workspace_root.to_path_buf(),
            managed_store: skill::ManagedSkillStore::default_for_user()
                .map_err(|error| error.to_string()),
            cache: Mutex::new(SkillInstructionCache::default()),
            flights: Mutex::new(HashMap::new()),
            usage,
            metrics,
        }
    }

    fn pointer(&self, skill_id: &str) -> Result<SkillActivePointer, String> {
        let pointer = self
            .usage
            .active_pointer(skill_id)?
            .ok_or("Skill has no original human approval")?;
        self.check_pointer(&pointer)?;
        Ok(pointer)
    }

    fn check_pointer(&self, pointer: &SkillActivePointer) -> Result<(), String> {
        let installation = pointer
            .installation
            .as_ref()
            .ok_or("Skill installation authority is unavailable")?;
        if pointer.revoked != Some(false)
            || pointer.revocation_generation.is_none()
            || pointer.generation == 0
            || pointer.approval_ref.is_empty()
            || installation.skill_id != pointer.skill_id
            || installation.revision != pointer.active_revision
            || installation.scope_ref != self.scope()?
        {
            return Err(
                "Skill approval is revoked, unknown or belongs to a different installation".into(),
            );
        }
        Ok(())
    }

    fn scope(&self) -> Result<String, String> {
        let store = self.managed_store.as_ref().map_err(Clone::clone)?;
        let workspace = configured_namespace_path(&self.workspace_root)?;
        let root = configured_namespace_path(store.root())?;
        let bytes = serde_json::to_vec(&("cowd.skill.installation.scope.v1", workspace, root))
            .map_err(|e| e.to_string())?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    fn evidence(
        &self,
        receipt: &skill::SkillInstallReceiptV1,
    ) -> Result<SkillInstallationEvidence, String> {
        Ok(SkillInstallationEvidence {
            scope_ref: self.scope()?,
            skill_id: receipt.skill_id.clone(),
            revision: receipt.revision.clone(),
            receipt_id: receipt.install_id.clone(),
            receipt_digest: format!(
                "sha256:{:x}",
                Sha256::digest(serde_json::to_vec(receipt).map_err(|e| e.to_string())?)
            ),
            inspection_fingerprint: receipt.plan_digest.clone(),
        })
    }

    fn installed(&self, pointer: &SkillActivePointer) -> Result<skill::InstalledRevision, String> {
        self.check_pointer(pointer)?;
        let evidence = pointer
            .installation
            .as_ref()
            .ok_or("Skill installation is unavailable")?;
        let installed = self
            .managed_store
            .as_ref()
            .map_err(Clone::clone)?
            .resolve_installed(
                &pointer.skill_id,
                &pointer.active_revision,
                &evidence.receipt_id,
            )
            .map_err(|e| e.to_string())?;
        if self.evidence(&installed.receipt)? != *evidence {
            return Err("Skill receipt changed after original approval".into());
        }
        Ok(installed)
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
        let pin = &asset.pin;
        let installation = &pin.installation;
        let bytes = std::mem::size_of::<CachedSkillInstruction>()
            .saturating_add(key.capacity())
            .saturating_add(asset.content.capacity())
            .saturating_add(asset.skill_id.capacity())
            .saturating_add(asset.source_ref.capacity())
            .saturating_add(asset.version.as_ref().map_or(0, String::capacity))
            .saturating_add(asset.tool_refs.capacity() * std::mem::size_of::<String>())
            .saturating_add(asset.tool_refs.iter().map(String::capacity).sum::<usize>())
            .saturating_add(pin.tool_refs.capacity() * std::mem::size_of::<String>())
            .saturating_add(pin.tool_refs.iter().map(String::capacity).sum::<usize>())
            .saturating_add(pin.instruction_version.as_ref().map_or(0, String::capacity))
            .saturating_add(pin.approval_ref.capacity())
            .saturating_add(pin.skill_name.capacity())
            .saturating_add(pin.instruction_sha256.capacity())
            .saturating_add(installation.scope_ref.capacity())
            .saturating_add(installation.skill_id.capacity())
            .saturating_add(installation.revision.capacity())
            .saturating_add(installation.receipt_id.capacity())
            .saturating_add(installation.receipt_digest.capacity())
            .saturating_add(installation.inspection_fingerprint.capacity());
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
        flights.retain(|_, flight| flight.strong_count() > 0);
        let flight = Arc::new(tokio::sync::Mutex::new(()));
        flights.insert(key.to_string(), Arc::downgrade(&flight));
        flight
    }
}

#[async_trait]
impl runtime::RuntimeSkillInstructionSource for WorkspaceSkillInstructionSource {
    fn installation_scope(&self) -> Result<String, String> {
        self.scope()
    }

    fn approved_profiles(&self) -> Result<Option<Vec<SkillCapabilityProfile>>, String> {
        let store = self.managed_store.as_ref().map_err(Clone::clone)?;
        let mut profiles = Vec::new();
        for skill_id in store.installed_skill_ids().map_err(|e| e.to_string())? {
            let Some(pointer) = self.usage.active_pointer(&skill_id)? else {
                continue;
            };
            // Missing legacy authority is held. A source read error is never
            // turned into an empty successful catalog.
            if pointer.revoked != Some(false)
                || pointer.revocation_generation.is_none()
                || pointer.installation.is_none()
            {
                continue;
            }
            let evidence = pointer
                .installation
                .as_ref()
                .ok_or("Skill installation is unavailable")?;
            if evidence.skill_id != skill_id || evidence.revision != pointer.active_revision {
                return Err("Skill catalog pointer identity mismatch".into());
            }
            if evidence.scope_ref != self.scope()? {
                continue;
            }
            self.check_pointer(&pointer)?;
            let entry = store
                .installed_entry(&skill_id, &pointer.active_revision, &evidence.receipt_id)
                .map_err(|e| e.to_string())?;
            if self.evidence(&entry.receipt)? != *evidence {
                return Err("approved Skill catalog receipt changed".into());
            }
            let profile = match entry.receipt.profile {
                Some(mut profile) => {
                    profile.source_root = entry.package_root.to_string_lossy().into_owned();
                    profile
                }
                None => self.installed(&pointer)?.profile,
            };
            if profile.skill_id != skill_id
                || profile.package_fingerprint != pointer.active_revision
            {
                return Err("Skill catalog identity mismatch".into());
            }
            profiles.push(profile);
        }
        Ok(Some(profiles))
    }

    fn required_tool_pins(&self, refs: &[String]) -> Result<Vec<SkillInstructionPin>, String> {
        let profiles = self
            .approved_profiles()?
            .ok_or("approved Skill catalog is unavailable")?;
        let mut pins = Vec::new();
        for reference in refs {
            let profile = profiles
                .iter()
                .find(|profile| profile.matches_reference(reference))
                .ok_or("required Skill has no approved catalog entry")?;
            let pointer = self.pointer(&profile.skill_id)?;
            let asset = asset_from_installed(self.installed(&pointer)?, &pointer)?;
            if !asset.pin.matches_reference(reference) {
                return Err("Skill reference changed during admission".into());
            }
            if !pins.contains(&asset.pin) {
                pins.push(asset.pin);
            }
        }
        self.validate_pins(&pins)?;
        Ok(pins)
    }

    fn inspect_revision(
        &self,
        skill_id: &str,
        revision: &str,
        receipt_id: &str,
    ) -> Result<SkillInstallationEvidence, String> {
        let installed = self
            .managed_store
            .as_ref()
            .map_err(Clone::clone)?
            .resolve_installed(skill_id, revision, receipt_id)
            .map_err(|e| e.to_string())?;
        self.evidence(&installed.receipt)
    }

    fn validate_pins(&self, pins: &[SkillInstructionPin]) -> Result<(), String> {
        for pin in pins {
            let skill_id = &pin.installation.skill_id;
            let original = self
                .usage
                .pointer_at_generation(skill_id, pin.pointer_generation)?
                .ok_or("original Skill approval is unavailable")?;
            self.check_pointer(&original)?;
            if original.installation.as_ref() != Some(&pin.installation)
                || original.approval_ref != pin.approval_ref
                || original.revocation_generation != Some(pin.revocation_generation)
            {
                return Err("Skill pin does not authenticate to its original approval".into());
            }
            let current = self.pointer(skill_id)?;
            if current.generation < original.generation
                || current.revocation_generation != Some(pin.revocation_generation)
            {
                return Err("Skill pin was revoked".into());
            }
            let asset = asset_from_installed(self.installed(&original)?, &original)?;
            if asset.pin != *pin {
                return Err("Skill instruction bytes changed".into());
            }
            // Capture the native source again after package I/O. Upgrades may
            // retain an approved immutable old pin; revocation never may.
            let current = self.pointer(skill_id)?;
            if current.revocation_generation != Some(pin.revocation_generation) {
                return Err("Skill pin revoked during verification".into());
            }
        }
        Ok(())
    }

    fn profiles_for_pins(
        &self,
        pins: &[SkillInstructionPin],
    ) -> Result<Vec<SkillCapabilityProfile>, String> {
        self.validate_pins(pins)?;
        pins.iter()
            .map(|pin| {
                let pointer = self
                    .usage
                    .pointer_at_generation(&pin.installation.skill_id, pin.pointer_generation)?
                    .ok_or("original Skill approval is unavailable")?;
                let installed = self.installed(&pointer)?;
                let profile = installed.profile.clone();
                if asset_from_installed(installed, &pointer)?.pin != *pin {
                    return Err("original Skill profile pin mismatch".into());
                }
                Ok(profile)
            })
            .collect()
    }

    async fn load_pinned_instruction(
        &self,
        pin: &SkillInstructionPin,
        invocation: &runtime::SkillInvocation,
        usage_context: &runtime::RuntimeSkillUsageContext,
    ) -> Result<Option<runtime::RuntimeSkillPromptAsset>, String> {
        self.validate_pins(std::slice::from_ref(pin))?;
        if pin.installation.skill_id != invocation.skill_id
            || pin.instruction_version != invocation.skill_version
        {
            return Err("Skill invocation does not match its original Binding pin".into());
        }
        let pointer = self
            .usage
            .pointer_at_generation(&invocation.skill_id, pin.pointer_generation)?
            .ok_or("original Skill approval is unavailable")?;
        self.load_for_pointer(pointer, invocation, usage_context)
            .await
    }

    async fn load_instruction(
        &self,
        invocation: &runtime::SkillInvocation,
        usage_context: &runtime::RuntimeSkillUsageContext,
    ) -> Result<Option<runtime::RuntimeSkillPromptAsset>, String> {
        self.load_for_pointer(
            self.pointer(&invocation.skill_id)?,
            invocation,
            usage_context,
        )
        .await
    }
}

impl WorkspaceSkillInstructionSource {
    async fn load_for_pointer(
        &self,
        pointer: SkillActivePointer,
        invocation: &runtime::SkillInvocation,
        usage_context: &runtime::RuntimeSkillUsageContext,
    ) -> Result<Option<runtime::RuntimeSkillPromptAsset>, String> {
        let key = format!(
            "{}:{}:{}",
            pointer.skill_id,
            pointer.generation,
            pointer
                .revocation_generation
                .ok_or("unknown Skill revocation epoch")?
        );
        let result = async {
            let flight = self.flight(&key);
            let _guard = flight.lock().await;
            let asset = if let Some(asset) = self.cached(&key) {
                self.usage.record(
                    invocation,
                    &pointer.active_revision,
                    usage_context,
                    SkillUsageKind::Hit,
                );
                asset
            } else {
                self.metrics.misses.fetch_add(1, Ordering::Relaxed);
                self.usage.record(
                    invocation,
                    &pointer.active_revision,
                    usage_context,
                    SkillUsageKind::Miss,
                );
                let installed = self.installed(&pointer)?;
                let asset = asset_from_installed(installed, &pointer)?;
                self.metrics.loads.fetch_add(1, Ordering::Relaxed);
                self.usage.record(
                    invocation,
                    &pointer.active_revision,
                    usage_context,
                    SkillUsageKind::Load,
                );
                asset
            };
            if asset.skill_id != invocation.skill_id || asset.version != invocation.skill_version {
                return Err("selected Skill instruction identity/version changed".into());
            }
            self.validate_pins(std::slice::from_ref(&asset.pin))?;
            self.insert(key, asset.clone());
            Ok(Some(asset))
        }
        .await;
        if result.is_err() {
            self.metrics.failures.fetch_add(1, Ordering::Relaxed);
            self.usage.record(
                invocation,
                &pointer.active_revision,
                usage_context,
                SkillUsageKind::Failure,
            );
        }
        result
    }
}

// Namespace identity is configured, not inferred from retained body existence.
// Existing ancestors resolve normally; dangling symlinks and read errors hold.
fn configured_namespace_path(path: &Path) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(path)
    };
    let mut ancestor = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(mut resolved) => {
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(ancestor) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err("Skill namespace has a dangling symlink".into())
                    }
                    Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.to_string()),
                    _ => {}
                }
                let component = ancestor
                    .file_name()
                    .ok_or_else(|| error.to_string())?
                    .to_os_string();
                missing.push(component);
                ancestor = ancestor.parent().ok_or_else(|| error.to_string())?;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn instruction_tool_refs(skill_id: &str) -> Vec<String> {
    if skill_id.starts_with("lark-") {
        vec!["lark_cli_read".into(), "lark_cli_write".into()]
    } else {
        Vec::new()
    }
}

fn asset_from_installed(
    installed: skill::InstalledRevision,
    pointer: &SkillActivePointer,
) -> Result<runtime::RuntimeSkillPromptAsset, String> {
    let profile = &installed.profile;
    if profile.lifecycle_status == SkillLifecycleStatus::Blocked
        || !profile.adapters.contains(&SkillAdapterKind::PromptOnly)
    {
        return Err("Skill package has no admitted PromptOnly instruction".into());
    }
    let entrypoint = profile
        .entrypoints
        .iter()
        .find(|entry| entry.adapter == SkillAdapterKind::PromptOnly && entry.path == "SKILL.md")
        .ok_or("installed Skill prompt entrypoint is unavailable")?;
    let content = installed.prompt;
    let tool_refs = instruction_tool_refs(&profile.skill_id);
    let content = if profile.skill_id.starts_with("lark-") {
        format!(
                "# Cowd Lark execution bridge\n\nUse this Skill only when the current user request is explicitly about Feishu/Lark operations (messaging, docs, sheets, slides, tasks, calendar, or contacts). For any other task, ignore this Skill and use normal workspace tools. When used, call `lark_cli_read` for reads and `lark_cli_write` for mutations; pass only argv entries after `lark-cli`. Never use Bash to locate credentials, never ask the user to repeat configured app credentials, and never run CLI auth/config/profile/update commands. The gateway supplies a short-lived bot token, enforces the official CLI risk class, and applies Cowd approval policy. If an operation requires user identity rather than bot identity, explain that boundary instead of silently changing identity.\n\n{content}"
            )
    } else {
        content
    };
    if content.trim().is_empty() {
        return Err("Skill instruction is empty".into());
    }
    let pin = SkillInstructionPin {
        installation: pointer
            .installation
            .clone()
            .ok_or("Skill installation is unavailable")?,
        pointer_generation: pointer.generation,
        revocation_generation: pointer
            .revocation_generation
            .ok_or("unknown Skill revocation epoch")?,
        approval_ref: pointer.approval_ref.clone(),
        skill_name: profile.name.clone(),
        instruction_sha256: format!("sha256:{:x}", Sha256::digest(content.as_bytes())),
        instruction_version: profile.version.clone(),
        tool_refs: tool_refs.clone(),
    };
    Ok(runtime::RuntimeSkillPromptAsset {
        skill_id: profile.skill_id.clone(),
        version: profile.version.clone(),
        content,
        source_ref: format!(
            "skill://{}/{}@{}",
            profile.skill_id, entrypoint.path, pointer.active_revision
        ),
        tool_refs,
        pin,
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
        sink.ok_or("Skill pointer source is not attached")?
            .active_pointer(skill_id)
    }
    fn pointer_at_generation(
        &self,
        skill_id: &str,
        generation: u64,
    ) -> Result<Option<SkillActivePointer>, String> {
        let sink = self
            .sink
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or("Skill pointer source is not attached")?;
        sink.pointer_at_generation(skill_id, generation)
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
    use harness_contract::skill::{SkillRevisionReview, SkillRevisionReviewDecision};
    use runtime::{RuntimeSkillInstructionSource, RuntimeSkillUsageSink};
    use std::time::Duration;

    struct TempWorkspace {
        root: PathBuf,
    }
    impl TempWorkspace {
        fn new(label: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("cowd-skill-{label}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self { root }
        }
    }
    fn remove_owned_tree(path: &Path) {
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            return;
        };
        if metadata.file_type().is_symlink() {
            let _ = std::fs::remove_file(path);
            return;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                path,
                std::fs::Permissions::from_mode(if metadata.is_dir() { 0o700 } else { 0o600 }),
            );
        }
        if metadata.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    remove_owned_tree(&entry.path());
                }
            }
            let _ = std::fs::remove_dir(path);
        } else {
            let _ = std::fs::remove_file(path);
        }
    }
    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            remove_owned_tree(&self.root);
        }
    }

    fn usage_context(label: &str) -> runtime::RuntimeSkillUsageContext {
        runtime::RuntimeSkillUsageContext {
            workspace_identity: "workspace".into(),
            workload_fingerprint: format!("workload:{label}"),
            config_revision: "config".into(),
            evaluation_environment: "test".into(),
            execution_id: format!("execution:{label}"),
            session_id: "session".into(),
            turn_id: format!("turn:{label}"),
            observed_at_ms: 1,
        }
    }

    struct Fixture {
        temp: TempWorkspace,
        source: Arc<WorkspaceSkillInstructionSource>,
        store: Arc<runtime::RuntimeEventStore>,
        recorder: Arc<runtime::RuntimeSkillUsageRecorder>,
        governance: runtime::SkillRevisionGovernanceService,
        principal: runtime::VerifiedPrincipal,
        verifier: runtime::PrincipalVerifier,
    }
    impl Fixture {
        fn new(label: &str) -> Self {
            let temp = TempWorkspace::new(label);
            let store = Arc::new(runtime::RuntimeEventStore::for_test());
            let recorder =
                Arc::new(runtime::RuntimeSkillUsageRecorder::new(store.clone()).unwrap());
            let relay = Arc::new(RuntimeSkillUsageRelay::default());
            relay.attach(recorder.clone());
            let managed = skill::ManagedSkillStore::new(temp.root.join("installed"));
            let mut source = WorkspaceSkillInstructionSource::new(
                &temp.root,
                &[],
                relay,
                Arc::new(SkillInstructionCacheMetrics::default()),
            );
            source.managed_store = Ok(managed);
            let source = Arc::new(source);
            let catalog =
                runtime::RuntimeSkillCatalog::default().with_instruction_source(source.clone());
            let governance = runtime::SkillRevisionGovernanceService::new(
                store.clone(),
                Arc::new(runtime::ApprovalQueue::new(store.clone())),
                Arc::new(RwLock::new(catalog)),
            );
            let (signed, public_key) = auth_broker::test_support::issue_human_principal(
                temp.root.join("authority"),
                "test-local-human",
                vec!["skill.revision.manage".into()],
                None,
            )
            .unwrap();
            let verifier =
                runtime::PrincipalVerifier::from_base64(&signed.key_id, &public_key).unwrap();
            let principal = verifier.verify(&signed).unwrap();
            Self {
                temp,
                source,
                store,
                recorder,
                governance,
                principal,
                verifier,
            }
        }
        fn stage(&self, name: &str, version: &str, body: &str) -> skill::SkillInstallReceiptV1 {
            let path = self.temp.root.join("package");
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("SKILL.md"), format!("---\nname: {name}\nversion: {version}\ndescription: Review evidence.\n---\n{body}\n")).unwrap();
            let managed = self.source.managed_store.as_ref().unwrap().clone();
            let workspace = self.temp.root.clone();
            std::thread::spawn(move || {
                let lifecycle = skill::SkillLifecycle::new(managed).unwrap();
                let plan = lifecycle.plan(path.to_str().unwrap(), &workspace).unwrap();
                lifecycle
                    .commit(
                        path.to_str().unwrap(),
                        &workspace,
                        &plan.package_digest,
                        false,
                        "human:test-stage",
                    )
                    .unwrap()
            })
            .join()
            .expect("bounded installer fixture thread")
        }

        fn approve(&self, review: &SkillRevisionReview) -> SkillActivePointer {
            let expires = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 60_000;
            let (signed, _) = auth_broker::test_support::issue_decision_lease_for_capabilities(
                self.temp.root.join("authority"),
                "test-local-human",
                vec!["skill.revision.manage".into()],
                &review.review_id,
                review.action.action_key(),
                review.scope_ref(),
                &review.evidence_digest,
                expires,
            )
            .unwrap();
            let lease = self
                .verifier
                .verify_decision_lease(
                    &signed,
                    &self.principal,
                    &runtime::DecisionLeaseExpectation::new(
                        &review.review_id,
                        review.action.action_key(),
                        review.scope_ref(),
                        &review.evidence_digest,
                    ),
                )
                .unwrap();
            self.governance
                .decide_review(
                    &self.principal,
                    &lease,
                    &review.review_id,
                    SkillRevisionReviewDecision::Approve,
                    "explicit human decision",
                )
                .unwrap()
                .unwrap()
        }
        fn first(
            &self,
            receipt: &skill::SkillInstallReceiptV1,
            request: &str,
        ) -> SkillActivePointer {
            let review = self
                .governance
                .request_first_install(
                    &self.principal,
                    request,
                    &receipt.skill_id,
                    &receipt.revision,
                    &receipt.install_id,
                )
                .unwrap();
            self.approve(&review)
        }
        fn upgrade(&self, receipt: &skill::SkillInstallReceiptV1) -> SkillActivePointer {
            // Independent closed draft inputs at this domain seam. The
            // original maintenance projector/receipt chain has its own gates;
            // this reader test does not claim to exercise that worker.
            let current = self.governance.pointer(&receipt.skill_id).unwrap().unwrap();
            let draft: harness_contract::skill::SkillMaintenanceDraft = serde_json::from_value(serde_json::json!({
                "draft_id":"maintenance", "skill_id":receipt.skill_id, "base_revision":current.active_revision, "proposed_revision":receipt.revision,
                "workspace_identity":"workspace", "workload_fingerprint":"workload", "config_revision":"config", "evaluation_environment":"test",
                "canonical_counts":{"hits":0,"misses":0,"loads":0,"failures":1}, "legacy_counts":{"hits":0,"misses":0,"loads":0,"failures":0},
                "evidence_receipt_ids":["original-usage"], "outcome_refs":["outcome:execution"], "evidence_digest":"closed-original-evidence", "target":"new revision", "recommendation":"revise",
                "validation":{"receipt_schema_valid":true,"evidence_closed":true,"outcome_association_count":1,"verified_success_count":0,"terminal_failure_count":1,"missing_outcome_count":0,"notes":[]}, "created_at_ms":1,"schema_version":1
            })).unwrap();
            let review = self
                .governance
                .request_activation(
                    &self.principal,
                    "maintenance",
                    &draft,
                    &receipt.revision,
                    &receipt.install_id,
                )
                .unwrap();
            self.approve(&review)
        }
        fn invocation(
            receipt: &skill::SkillInstallReceiptV1,
            version: &str,
        ) -> runtime::SkillInvocation {
            runtime::SkillInvocation {
                skill_id: receipt.skill_id.clone(),
                skill_version: Some(version.into()),
                adapter: SkillAdapterKind::PromptOnly,
                entrypoint: None,
            }
        }
        async fn close(&self) {
            let report = self
                .recorder
                .shutdown_and_drain(Duration::from_secs(2))
                .await;
            assert!(report.clean, "{report:?}");
        }
    }

    #[tokio::test]
    async fn stage_first_human_approval_exact_page_in_and_durable_usage() {
        let fixture = Fixture::new("bootstrap");
        let receipt = fixture.stage("Review", "1.0.0", "ORIGINAL_FULL_APPROVED_BYTES");
        let invocation = Fixture::invocation(&receipt, "1.0.0");
        assert!(fixture.governance.pointer("review").unwrap().is_none());
        assert!(!fixture
            .source
            .managed_store
            .as_ref()
            .unwrap()
            .root()
            .join("review/active.json")
            .exists());
        assert!(fixture
            .source
            .approved_profiles()
            .unwrap()
            .unwrap()
            .is_empty());
        assert!(fixture
            .source
            .load_instruction(&invocation, &usage_context("unapproved"))
            .await
            .is_err());
        let pointer = fixture.first(&receipt, "first");
        assert_eq!(
            (
                pointer.generation,
                pointer.revocation_generation,
                pointer.revoked
            ),
            (1, Some(0), Some(false))
        );
        let profiles = fixture.source.approved_profiles().unwrap().unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].package_fingerprint, receipt.revision);
        let asset = fixture
            .source
            .load_instruction(&invocation, &usage_context("load"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(asset.content, "---\nname: Review\nversion: 1.0.0\ndescription: Review evidence.\n---\nORIGINAL_FULL_APPROVED_BYTES\n");
        assert_eq!(asset.pin.pointer_generation, 1);
        let cached = fixture
            .source
            .load_instruction(&invocation, &usage_context("hit"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached, asset);
        let replay = fixture
            .governance
            .request_first_install(
                &fixture.principal,
                "first",
                "review",
                &receipt.revision,
                &receipt.install_id,
            )
            .unwrap();
        assert_eq!(
            replay.status,
            harness_contract::skill::SkillRevisionReviewStatus::Approved
        );
        assert!(fixture
            .governance
            .request_first_install(
                &fixture.principal,
                "foreign-first",
                "review",
                &receipt.revision,
                &receipt.install_id
            )
            .is_err());
        fixture.close().await;
        let receipts = fixture
            .store
            .list_scope(runtime::RuntimeEventScope::Skill, 100)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == runtime::SKILL_USAGE_RECEIPT_EVENT_KIND)
            .collect::<Vec<_>>();
        assert!(receipts
            .iter()
            .any(|event| event.payload["receipt"]["skill_revision"] == receipt.revision));
        assert!(fixture.source.metrics.health().hits > 0);
        assert!(fixture.recorder.health().persisted > 0);
    }

    #[tokio::test]
    async fn ordinary_upgrade_keeps_original_pin_but_revocation_fences_cache_and_old_instructions()
    {
        let fixture = Fixture::new("revoke");
        let old = fixture.stage("Review", "1.0.0", "OLD_IMMUTABLE_BODY");
        fixture.first(&old, "first");
        let old_asset = fixture
            .source
            .load_instruction(&Fixture::invocation(&old, "1.0.0"), &usage_context("old"))
            .await
            .unwrap()
            .unwrap();
        let new = fixture.stage("Review", "2.0.0", "NEW_APPROVED_BODY");
        fixture.upgrade(&new);
        fixture
            .source
            .validate_pins(std::slice::from_ref(&old_asset.pin))
            .unwrap();
        let new_asset = fixture
            .source
            .load_instruction(&Fixture::invocation(&new, "2.0.0"), &usage_context("new"))
            .await
            .unwrap()
            .unwrap();
        assert!(new_asset.content.contains("NEW_APPROVED_BODY"));
        let pinned_profiles = fixture
            .source
            .profiles_for_pins(std::slice::from_ref(&old_asset.pin))
            .unwrap();
        assert_eq!(pinned_profiles[0].version.as_deref(), Some("1.0.0"));
        let old_again = fixture
            .source
            .load_pinned_instruction(
                &old_asset.pin,
                &Fixture::invocation(&old, "1.0.0"),
                &usage_context("queued-old-binding"),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(old_again.content.contains("OLD_IMMUTABLE_BODY"));
        assert!(!old_again.content.contains("NEW_APPROVED_BODY"));
        assert_eq!(old_again.pin, old_asset.pin);
        assert!(fixture
            .source
            .load_pinned_instruction(
                &old_asset.pin,
                &Fixture::invocation(&new, "2.0.0"),
                &usage_context("wrong-binding-version")
            )
            .await
            .is_err());
        let mut forged = old_asset.pin.clone();
        forged.pointer_generation = 2;
        assert!(fixture.source.validate_pins(&[forged]).is_err());
        // Revocation depends on original approval/scope, not intact body I/O.
        let installed = fixture
            .source
            .managed_store
            .as_ref()
            .unwrap()
            .installed_entry("review", &new.revision, &new.install_id)
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                &installed.package_root,
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        std::fs::remove_file(installed.package_root.join("SKILL.md")).unwrap();
        let review = fixture
            .governance
            .request_deactivation(
                &fixture.principal,
                "revoke",
                "review",
                2,
                "human revokes damaged package",
            )
            .unwrap();
        let pointer = fixture.approve(&review);
        assert_eq!(
            (
                pointer.generation,
                pointer.revocation_generation,
                pointer.revoked
            ),
            (3, Some(1), Some(true))
        );
        assert!(fixture
            .source
            .profiles_for_pins(std::slice::from_ref(&old_asset.pin))
            .is_err());
        assert!(fixture
            .source
            .load_pinned_instruction(
                &old_asset.pin,
                &Fixture::invocation(&old, "1.0.0"),
                &usage_context("revoked-old-binding")
            )
            .await
            .is_err());
        assert!(fixture
            .source
            .validate_pins(&[old_asset.pin, new_asset.pin])
            .is_err());
        assert!(fixture
            .source
            .load_instruction(
                &Fixture::invocation(&old, "1.0.0"),
                &usage_context("revoked")
            )
            .await
            .is_err());
        assert!(fixture
            .source
            .approved_profiles()
            .unwrap()
            .unwrap()
            .is_empty());
        assert_eq!(
            fixture
                .governance
                .request_deactivation(
                    &fixture.principal,
                    "revoke",
                    "review",
                    2,
                    "human revokes damaged package"
                )
                .unwrap()
                .review_id,
            review.review_id
        );
        assert!(fixture
            .governance
            .request_deactivation(
                &fixture.principal,
                "new-revoke",
                "review",
                2,
                "stale generation"
            )
            .is_err());
        fixture.close().await;
    }

    #[tokio::test]
    async fn human_can_revoke_after_installed_bytes_are_erased_and_old_pin_stays_invalid() {
        let fixture = Fixture::new("erased-store");
        let receipt = fixture.stage("Review", "1.0.0", "ERASED_APPROVED_BYTES");
        fixture.first(&receipt, "first");
        let asset = fixture
            .source
            .load_instruction(
                &Fixture::invocation(&receipt, "1.0.0"),
                &usage_context("before-erasure"),
            )
            .await
            .unwrap()
            .unwrap();
        let scope = fixture.source.installation_scope().unwrap();
        remove_owned_tree(fixture.source.managed_store.as_ref().unwrap().root());
        assert_eq!(fixture.source.installation_scope().unwrap(), scope);
        assert!(fixture
            .source
            .validate_pins(std::slice::from_ref(&asset.pin))
            .is_err());
        let review = fixture
            .governance
            .request_deactivation(
                &fixture.principal,
                "revoke-erased",
                "review",
                1,
                "erase supersedes original pin",
            )
            .unwrap();
        assert_eq!(fixture.approve(&review).revoked, Some(true));
        fixture.close().await;
    }

    #[tokio::test]
    async fn late_page_in_after_human_revocation_cannot_publish_old_cache() {
        let fixture = Fixture::new("late-cache");
        let receipt = fixture.stage("Review", "1.0.0", "APPROVED_BUT_REVOKED_BEFORE_PUBLICATION");
        fixture.first(&receipt, "first");
        let flight = fixture.source.flight("review:1:0");
        let guard = flight.lock().await;
        let invocation = Fixture::invocation(&receipt, "1.0.0");
        let context = usage_context("late");
        let future = fixture.source.load_instruction(&invocation, &context);
        futures::pin_mut!(future);
        assert!(
            futures::poll!(future.as_mut()).is_pending(),
            "actual original page-in is waiting at its existing flight"
        );
        let review = fixture
            .governance
            .request_deactivation(
                &fixture.principal,
                "revoke",
                "review",
                1,
                "revoke while original page-in waits",
            )
            .unwrap();
        fixture.approve(&review);
        drop(guard);
        assert!(future.await.is_err());
        assert!(fixture.source.cache.lock().unwrap().entries.is_empty());
        fixture.close().await;
    }

    #[tokio::test]
    async fn foreign_scope_is_excluded_without_blocking_local_approved_catalog() {
        let fixture = Fixture::new("scope");
        let foreign = fixture.stage("Foreign Review", "1.0.0", "FOREIGN_WORKSPACE_BODY");
        fixture.first(&foreign, "foreign");
        let other = fixture.temp.root.join("other-workspace");
        std::fs::create_dir_all(&other).unwrap();
        let mut source = WorkspaceSkillInstructionSource::new(
            &other,
            &[],
            fixture.source.usage.clone(),
            fixture.source.metrics.clone(),
        );
        source.managed_store = fixture.source.managed_store.clone();
        let source = Arc::new(source);
        let governance = runtime::SkillRevisionGovernanceService::new(
            fixture.store.clone(),
            Arc::new(runtime::ApprovalQueue::new(fixture.store.clone())),
            Arc::new(RwLock::new(
                runtime::RuntimeSkillCatalog::default().with_instruction_source(source.clone()),
            )),
        );
        let local = fixture.stage("Local Review", "1.0.0", "LOCAL_WORKSPACE_BODY");
        let review = governance
            .request_first_install(
                &fixture.principal,
                "local",
                &local.skill_id,
                &local.revision,
                &local.install_id,
            )
            .unwrap();
        // Same original store and human authority; the service still validates
        // the actual source's distinct workspace scope at decision time.
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let (signed, _) = auth_broker::test_support::issue_decision_lease_for_capabilities(
            fixture.temp.root.join("authority"),
            "test-local-human",
            vec!["skill.revision.manage".into()],
            &review.review_id,
            review.action.action_key(),
            review.scope_ref(),
            &review.evidence_digest,
            expires,
        )
        .unwrap();
        let lease = fixture
            .verifier
            .verify_decision_lease(
                &signed,
                &fixture.principal,
                &runtime::DecisionLeaseExpectation::new(
                    &review.review_id,
                    review.action.action_key(),
                    review.scope_ref(),
                    &review.evidence_digest,
                ),
            )
            .unwrap();
        governance
            .decide_review(
                &fixture.principal,
                &lease,
                &review.review_id,
                SkillRevisionReviewDecision::Approve,
                "local approval",
            )
            .unwrap();
        let profiles = source.approved_profiles().unwrap().unwrap();
        assert_eq!(
            profiles
                .iter()
                .map(|profile| profile.skill_id.as_str())
                .collect::<Vec<_>>(),
            vec!["local-review"]
        );
        assert!(source
            .load_instruction(
                &Fixture::invocation(&foreign, "1.0.0"),
                &usage_context("foreign")
            )
            .await
            .is_err());
        fixture.close().await;
    }

    #[tokio::test]
    async fn corrupt_receipt_and_unknown_native_pointer_cannot_authorize_page_in() {
        let fixture = Fixture::new("corruption");
        let receipt = fixture.stage("Review", "1.0.0", "APPROVED_BODY");
        fixture.first(&receipt, "first");
        let path = fixture
            .source
            .managed_store
            .as_ref()
            .unwrap()
            .root()
            .join("review/receipts")
            .join(format!("{}.json", receipt.install_id));
        let mut altered = serde_json::to_value(&receipt).unwrap();
        altered["profile"]["name"] = serde_json::json!("foreign injected catalog metadata");
        std::fs::write(path, serde_json::to_vec(&altered).unwrap()).unwrap();
        assert!(fixture.source.approved_profiles().is_err());
        assert!(fixture
            .source
            .load_instruction(
                &Fixture::invocation(&receipt, "1.0.0"),
                &usage_context("corrupt")
            )
            .await
            .is_err());
        fixture
            .store
            .append(runtime::RuntimeEventInput {
                stream_id: format!("skill-revision-pointer:{:x}", Sha256::digest(b"review")),
                scope: runtime::RuntimeEventScope::Skill,
                kind: "foreign-pointer".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert!(fixture.source.approved_profiles().is_err());
        fixture.close().await;
    }

    #[test]
    fn installed_revision_reader_binds_original_receipt_scope_and_full_bytes() {
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
    async fn approved_lark_instruction_preserves_original_bridge_and_tool_refs() {
        let fixture = Fixture::new("lark-bridge");
        let receipt = fixture.stage(
            "Lark Im",
            "1.0.0",
            "Use lark-cli to read authorized messages.",
        );
        fixture.first(&receipt, "first");
        let asset = fixture
            .source
            .load_instruction(
                &Fixture::invocation(&receipt, "1.0.0"),
                &usage_context("lark"),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(asset.content.contains("# Cowd Lark execution bridge"));
        assert!(asset
            .content
            .contains("Never use Bash to locate credentials"));
        assert!(asset
            .content
            .contains("Use lark-cli to read authorized messages."));
        assert_eq!(asset.tool_refs, vec!["lark_cli_read", "lark_cli_write"]);
        assert_eq!(
            fixture
                .source
                .required_tool_refs(&["lark-im@1.0.0".into()])
                .unwrap(),
            std::collections::BTreeSet::from(["lark_cli_read".into(), "lark_cli_write".into()])
        );
        assert!(fixture
            .source
            .required_tool_refs(&["lark-im@foreign".into()])
            .is_err());
        assert_eq!(
            fixture
                .source
                .required_tool_pins(&["Lark Im".into()])
                .unwrap()[0]
                .installation
                .skill_id,
            "lark-im"
        );
        fixture.source.validate_pins(&[asset.pin]).unwrap();
        fixture.close().await;
    }

    #[test]
    fn runtime_skill_profile_provider_uses_workspace_registry() {
        let temp = TempWorkspace::new("discovery");
        let path = temp.root.join(".cowd/skills/release-review");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("SKILL.md"), "---\nname: Release Review\ndescription: Review release plans.\n---\nRequire explicit evidence.").unwrap();
        let profiles = runtime_skill_profiles_for_workspace(&temp.root);
        let profile = profiles
            .iter()
            .find(|profile| profile.skill_id == "release-review")
            .unwrap();
        assert_eq!(profile.name, "Release Review");
        assert!(profile.adapters.contains(&SkillAdapterKind::PromptOnly));
        assert!(profile
            .entrypoints
            .iter()
            .any(|entry| entry.path == "SKILL.md"));
        assert_eq!(
            workspace_skill_cache_health(&temp.root).resident_entries,
            0,
            "management discovery must not materialize cold instructions"
        );
    }

    #[tokio::test]
    async fn first_activation_performs_full_package_inspection_and_rejects_blocked_skill() {
        let fixture = Fixture::new("blocked");
        let path = fixture.temp.root.join("blocked");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("SKILL.md"), "---\nname: Unsafe Cache\ndescription: Never injected.\n---\nNever execute package code.").unwrap();
        let managed = fixture.source.managed_store.as_ref().unwrap().clone();
        let workspace = fixture.temp.root.clone();
        std::thread::spawn(move || {
            let lifecycle = skill::SkillLifecycle::new(managed).unwrap();
            let plan = lifecycle.plan(path.to_str().unwrap(), &workspace).unwrap();
            assert!(plan.installable);
            assert!(plan.blockers.is_empty());
            std::fs::create_dir_all(path.join("node_modules")).unwrap();
            std::fs::write(path.join("node_modules/package.json"), "{}").unwrap();
            assert!(matches!(lifecycle.plan(path.to_str().unwrap(), &workspace),
                Err(skill::SkillLifecycleError::InvalidPackage(reason)) if reason.contains("node_modules")));
            assert!(lifecycle
                .commit(
                    path.to_str().unwrap(),
                    &workspace,
                    &plan.package_digest,
                    true,
                    "human:test"
                )
                .is_err());
        })
        .join()
        .expect("blocked installer fixture thread");
        assert!(fixture
            .governance
            .pointer("unsafe-cache")
            .unwrap()
            .is_none());
        fixture.close().await;
    }

    #[tokio::test]
    #[ignore = "requires configured original Lark skill packages; content-only gate, no Lark requests"]
    async fn live_cowd_lark_skills_are_discovered_and_selected_by_runtime() {
        assert_eq!(
            std::env::var("COWD_LIVE_LARK_SKILL_TEST").as_deref(),
            Ok("1")
        );
        let discovered = runtime_skill_assets_for_workspace(Path::new("."));
        let fixture = Fixture::new("live-lark");
        for expected in ["lark-base", "lark-im"] {
            let original = discovered
                .profiles
                .iter()
                .find(|profile| profile.skill_id == expected)
                .expect("configured original Lark package");
            let managed = fixture.source.managed_store.as_ref().unwrap().clone();
            let workspace = fixture.temp.root.clone();
            let original_root = original.source_root.clone();
            let receipt = std::thread::spawn(move || {
                let lifecycle = skill::SkillLifecycle::new(managed).unwrap();
                let plan = lifecycle.plan(&original_root, &workspace).unwrap();
                lifecycle
                    .commit(
                        &original_root,
                        &workspace,
                        &plan.package_digest,
                        false,
                        "human:isolated-test",
                    )
                    .unwrap()
            })
            .join()
            .expect("configured original package fixture thread");
            fixture.first(&receipt, expected);
            let profiles = fixture.source.approved_profiles().unwrap().unwrap();
            let decision = runtime::skill::SkillActivationEngine::activate(
                runtime::skill::SkillActivationInput {
                    session_id: "lark-gate".into(),
                    turn_index: 0,
                    query: format!("请使用 {expected} 查询消息和多维表格"),
                    capability_refs: vec![],
                    available_profiles: profiles,
                    agent_profile: harness_contract::skill::AgentSkillProfile {
                        adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
                        ..Default::default()
                    },
                },
            );
            let selected = decision
                .activations
                .iter()
                .filter_map(|activation| activation.selected_invocation.as_ref())
                .find(|invocation| invocation.skill_id == expected)
                .expect("selected approved Lark skill");
            let asset = fixture
                .source
                .load_instruction(selected, &usage_context(expected))
                .await
                .unwrap()
                .unwrap();
            assert!(asset.content.contains("lark-cli"));
            assert_eq!(asset.tool_refs, vec!["lark_cli_read", "lark_cli_write"]);
        }
        fixture.close().await;
    }
}
