use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use async_trait::async_trait;
use harness_contract::skill::{SkillAdapterKind, SkillCapabilityProfile};
use skill::{profile_skill_package, SkillInfo, SkillRegistry};

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

#[derive(Default)]
struct WorkspaceSkillSnapshotCell {
    current: Mutex<Option<CachedWorkspaceSkillSnapshot>>,
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
    let assets = runtime_skill_assets_from_snapshot(&skills);
    let snapshot = Arc::new(WorkspaceSkillSnapshot { skills, assets });
    *current = Some(CachedWorkspaceSkillSnapshot {
        snapshot: Arc::clone(&snapshot),
    });
    snapshot
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

fn runtime_skill_assets_from_snapshot(skills: &[SkillInfo]) -> RuntimeSkillAssets {
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
        let profile = match profile_skill_package(&root, &skill.name, None) {
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
    if !assets.profiles.is_empty() {
        assets.instruction_source = Some(Arc::new(WorkspaceSkillInstructionSource::new(
            &assets.profiles,
        )));
    }
    assets
}

const SKILL_INSTRUCTION_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
struct SkillInstructionDescriptor {
    root: PathBuf,
    profile: SkillCapabilityProfile,
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
    descriptors: Arc<HashMap<String, SkillInstructionDescriptor>>,
    cache: Mutex<SkillInstructionCache>,
    flights: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

impl WorkspaceSkillInstructionSource {
    fn new(profiles: &[SkillCapabilityProfile]) -> Self {
        let descriptors = profiles
            .iter()
            .cloned()
            .map(|profile| {
                (
                    profile.skill_id.clone(),
                    SkillInstructionDescriptor {
                        root: PathBuf::from(&profile.source_root),
                        profile,
                    },
                )
            })
            .collect();
        Self {
            descriptors: Arc::new(descriptors),
            cache: Mutex::new(SkillInstructionCache::default()),
            flights: Mutex::new(HashMap::new()),
        }
    }

    fn cache_key(descriptor: &SkillInstructionDescriptor) -> String {
        format!(
            "{}:{}:{}",
            descriptor.profile.skill_id,
            descriptor.profile.package_fingerprint,
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
        cache.entries.get_mut(key).map(|entry| {
            entry.last_access = clock;
            entry.asset.clone()
        })
    }

    fn insert(&self, key: String, asset: runtime::RuntimeSkillPromptAsset) {
        let bytes = asset
            .content
            .len()
            .saturating_add(asset.skill_id.len())
            .saturating_add(asset.source_ref.len())
            .saturating_add(asset.tool_refs.iter().map(String::len).sum::<usize>());
        if bytes > SKILL_INSTRUCTION_CACHE_MAX_BYTES {
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
            }
        }
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
}

#[async_trait]
impl runtime::RuntimeSkillInstructionSource for WorkspaceSkillInstructionSource {
    async fn load_instruction(
        &self,
        invocation: &runtime::SkillInvocation,
    ) -> Result<Option<runtime::RuntimeSkillPromptAsset>, String> {
        let Some(descriptor) = self.descriptors.get(&invocation.skill_id).cloned() else {
            return Ok(None);
        };
        let key = Self::cache_key(&descriptor);
        if let Some(asset) = self.cached(&key) {
            return Ok(Some(asset));
        }
        let flight = self.flight(&key);
        let _guard = flight.lock().await;
        if let Some(asset) = self.cached(&key) {
            return Ok(Some(asset));
        }
        let load_descriptor = descriptor.clone();
        let asset = tokio::task::spawn_blocking(move || {
            prompt_asset_for_profile(&load_descriptor.root, &load_descriptor.profile)
        })
        .await
        .map_err(|error| format!("Skill instruction loader failed: {error}"))?;
        if let Some(asset) = asset.as_ref() {
            self.insert(key, asset.clone());
        }
        Ok(asset)
    }
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
                "# Cowd Lark execution bridge\n\nThis Skill is already connected to the active Cowd Feishu/Lark bot configuration. For official CLI operations, call `lark_cli_read` for reads and `lark_cli_write` for mutations; pass only argv entries after `lark-cli`. Never use Bash to locate credentials, never ask the user to repeat configured app credentials, and never run CLI auth/config/profile/update commands. The gateway supplies a short-lived bot token, enforces the official CLI risk class, and applies Cowd approval policy. If an operation requires user identity rather than bot identity, explain that boundary instead of silently changing identity.\n\n{content}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::SkillAdapterKind;

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
            .load_instruction(&invocation)
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
            .load_instruction(&invocation)
            .await
            .expect("pinned generation page-in")
            .expect("pinned asset");
        assert!(
            pinned.content.contains("Require explicit evidence."),
            "an active catalog generation must remain immutable"
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
                .selected_invocation
                .as_ref()
                .expect("Lark skill should be selected");
            assert_eq!(selected.skill_id, expected);
            let prompt = assets
                .instruction_source
                .as_ref()
                .expect("live Skill instruction source")
                .load_instruction(selected)
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
