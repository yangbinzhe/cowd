//! Shared skill registry for Gateway projections, surfaces, and runtime skill
//! selection.
//!
//! This keeps skill discovery and path resolution in one place. It is
//! intentionally local-first: WebUI and remote hub concerns are out of scope for
//! the local skill package path.

use crate::skill_manifest::{
    get_related_skills, get_skill_description, get_skill_name, get_tags, matches_platform,
    parse_skill_file_header, ParsedSkill,
};
use crate::{default_managed_skill_store_root, list_managed_skill_entries, ManagedSkillStore};
use harness_contract::skill::SkillActivePointer;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRegistrySource {
    ProjectCowd,
    ProjectAgents,
    ProjectCodex,
    ProjectClaude,
    UserCowdManaged,
    UserCowdConfigHome,
    UserCodexHome,
    UserCowd,
    UserAgents,
    UserCodex,
    UserClaude,
    UserOpenCode,
}

impl SkillRegistrySource {
    #[must_use]
    pub const fn scope(self) -> SkillRegistryScope {
        match self {
            Self::ProjectCowd | Self::ProjectAgents | Self::ProjectCodex | Self::ProjectClaude => {
                SkillRegistryScope::Project
            }
            Self::UserCowdManaged | Self::UserCowdConfigHome | Self::UserCodexHome => {
                SkillRegistryScope::UserConfigHome
            }
            Self::UserCowd
            | Self::UserAgents
            | Self::UserCodex
            | Self::UserClaude
            | Self::UserOpenCode => SkillRegistryScope::UserHome,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRegistryScope {
    Project,
    UserConfigHome,
    UserHome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRegistryRootKind {
    ManagedStore,
    SkillsDir,
    LegacyCommandsDir,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRegistryRoot {
    pub source: SkillRegistrySource,
    pub path: PathBuf,
    pub kind: SkillRegistryRootKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInfo {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub path: PathBuf,
    pub root: PathBuf,
    pub source: SkillRegistrySource,
    pub scope: SkillRegistryScope,
    pub kind: SkillRegistryRootKind,
    pub tags: Vec<String>,
    pub related_skills: Vec<String>,
    pub platforms: Vec<String>,
    pub shadowed_by: Option<SkillRegistrySource>,
}

#[derive(Debug, Clone)]
pub struct SkillRegistry {
    roots: Vec<SkillRegistryRoot>,
    managed_approval: Option<Arc<dyn ManagedSkillApproval>>,
}

/// Trusted reader of the original governance pointer and installation scope.
pub trait ManagedSkillApproval: std::fmt::Debug + Send + Sync {
    fn scope(&self) -> std::io::Result<String>;
    fn active_pointer(&self, skill_id: &str) -> std::io::Result<Option<SkillActivePointer>>;
}

impl SkillRegistry {
    #[must_use]
    pub fn discover(cwd: &Path) -> Self {
        Self {
            roots: discover_skill_registry_roots(cwd),
            managed_approval: None,
        }
    }

    #[must_use]
    pub fn with_roots(roots: Vec<SkillRegistryRoot>) -> Self {
        Self {
            roots,
            managed_approval: None,
        }
    }

    /// Restrict managed discovery to the supplied original approval source.
    /// Without a source, discovery retains the installed inventory semantics.
    #[must_use]
    pub fn with_managed_approval(mut self, approval: Arc<dyn ManagedSkillApproval>) -> Self {
        self.managed_approval = Some(approval);
        self
    }

    #[must_use]
    pub fn roots(&self) -> &[SkillRegistryRoot] {
        &self.roots
    }

    pub fn list(&self) -> std::io::Result<Vec<SkillInfo>> {
        let mut skills = Vec::new();
        let mut active_sources = BTreeMap::<String, SkillRegistrySource>::new();

        for root in &self.roots {
            let mut root_skills = match list_root_skills(root, self.managed_approval.as_deref()) {
                Ok(skills) => skills,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => return Err(error),
            };
            root_skills.sort_by(|left, right| left.name.cmp(&right.name));

            for mut skill in root_skills {
                let key = skill.name.to_ascii_lowercase();
                if let Some(existing) = active_sources.get(&key) {
                    skill.shadowed_by = Some(*existing);
                } else {
                    active_sources.insert(key, skill.source);
                }
                skills.push(skill);
            }
        }

        Ok(skills)
    }

    pub fn resolve(&self, skill: &str) -> std::io::Result<SkillInfo> {
        let requested = normalize_skill_name(skill)?;
        for skill in self.list()? {
            if skill.shadowed_by.is_none() && skill.name.eq_ignore_ascii_case(requested) {
                return Ok(skill);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("unknown skill: {requested}"),
        ))
    }
}

#[must_use]
pub fn discover_skill_registry_roots(cwd: &Path) -> Vec<SkillRegistryRoot> {
    let mut roots = Vec::new();

    for ancestor in cwd.ancestors() {
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectCowd,
            ancestor.join(".cowd").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectAgents,
            ancestor.join(".agents").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectCodex,
            ancestor.join(".codex").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectClaude,
            ancestor.join(".claude").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectCowd,
            ancestor.join(".cowd").join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectCodex,
            ancestor.join(".codex").join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::ProjectClaude,
            ancestor.join(".claude").join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
    }

    if let Ok(managed_store) = default_managed_skill_store_root() {
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowdManaged,
            managed_store,
            SkillRegistryRootKind::ManagedStore,
        );
    }

    if let Ok(config_home) = env::var("COWD_CONFIG_HOME") {
        let config_home = PathBuf::from(config_home);
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowdConfigHome,
            config_home.join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowdConfigHome,
            config_home.join("skills").join("omc-learned"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowdConfigHome,
            config_home.join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
    }

    if let Ok(codex_home) = env::var("CODEX_HOME") {
        let codex_home = PathBuf::from(codex_home);
        push_root(
            &mut roots,
            SkillRegistrySource::UserCodexHome,
            codex_home.join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCodexHome,
            codex_home.join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
    }

    if let Some(home) = env::var_os("HOME").or_else(|| env::var_os("USERPROFILE")) {
        let home = PathBuf::from(home);
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowd,
            home.join(".cowd").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowd,
            home.join(".cowd").join("skills").join("omc-learned"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCowd,
            home.join(".cowd").join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserAgents,
            home.join(".agents").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCodex,
            home.join(".codex").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserCodex,
            home.join(".codex").join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserClaude,
            home.join(".claude").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserClaude,
            home.join(".claude").join("skills").join("omc-learned"),
            SkillRegistryRootKind::SkillsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserClaude,
            home.join(".claude").join("commands"),
            SkillRegistryRootKind::LegacyCommandsDir,
        );
        push_root(
            &mut roots,
            SkillRegistrySource::UserOpenCode,
            home.join(".config").join("opencode").join("skills"),
            SkillRegistryRootKind::SkillsDir,
        );
    }

    roots
}

fn push_root(
    roots: &mut Vec<SkillRegistryRoot>,
    source: SkillRegistrySource,
    path: PathBuf,
    kind: SkillRegistryRootKind,
) {
    if path.is_dir() && !roots.iter().any(|existing| existing.path == path) {
        roots.push(SkillRegistryRoot { source, path, kind });
    }
}

fn list_root_skills(
    root: &SkillRegistryRoot,
    approval: Option<&dyn ManagedSkillApproval>,
) -> std::io::Result<Vec<SkillInfo>> {
    if root.kind == SkillRegistryRootKind::ManagedStore {
        let Some(approval) = approval else {
            return list_managed_skill_entries(&root.path)
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                })?
                .into_iter()
                .map(|entry| {
                    skill_info_from_path(root, &entry.prompt_path, || entry.receipt.skill_id)
                })
                .collect();
        };
        let scope = approval.scope()?;
        let store = ManagedSkillStore::new(&root.path);
        let invalid = |error: String| std::io::Error::new(std::io::ErrorKind::InvalidData, error);
        let mut skills = Vec::new();
        for id in store
            .installed_skill_ids()
            .map_err(|e| invalid(e.to_string()))?
        {
            let Some(pointer) = approval.active_pointer(&id)? else {
                continue;
            };
            if pointer.revoked != Some(false) || pointer.revocation_generation.is_none() {
                continue;
            }
            let evidence = pointer
                .installation
                .as_ref()
                .ok_or_else(|| invalid("missing installation evidence".into()))?;
            let failure = if pointer.skill_id != id {
                Some(("pointer_skill_id_matches", "false".to_string()))
            } else if pointer.generation == 0 {
                Some(("pointer_generation_nonzero", pointer.generation.to_string()))
            } else if pointer.approval_ref.is_empty() {
                Some(("approval_ref_nonempty", "0".to_string()))
            } else if evidence.skill_id != id {
                Some(("evidence_skill_id_matches", "false".to_string()))
            } else if evidence.revision != pointer.active_revision {
                Some(("evidence_revision_matches", "false".to_string()))
            } else if evidence.scope_ref != scope {
                Some(("evidence_scope_matches", "false".to_string()))
            } else if scope.is_empty() {
                Some(("scope_nonempty", "0".to_string()))
            } else {
                None
            };
            if let Some((predicate, actual)) = failure {
                return Err(invalid(format!("managed skill approval identity mismatch; first_failed_predicate={predicate} actual={actual}")));
            }
            let installed = store
                .resolve_installed(&id, &pointer.active_revision, &evidence.receipt_id)
                .map_err(|e| invalid(e.to_string()))?;
            let receipt_digest = format!(
                "sha256:{:x}",
                Sha256::digest(serde_json::to_vec(&installed.receipt)?)
            );
            let failure = if evidence.receipt_digest != receipt_digest {
                Some("receipt_digest_matches")
            } else if evidence.inspection_fingerprint != installed.inspection_fingerprint {
                Some("inspection_fingerprint_matches")
            } else {
                None
            };
            if let Some(predicate) = failure {
                return Err(invalid(format!("managed skill installation evidence mismatch; first_failed_predicate={predicate} actual=false")));
            }
            let parsed = crate::parse_skill_content(&installed.prompt)
                .map_err(|e| invalid(e.to_string()))?;
            skills.push(skill_info_from_parsed(
                root,
                &installed.package_root.join("SKILL.md"),
                &parsed,
                id,
            ));
        }
        return Ok(skills);
    }
    let mut skills = Vec::new();
    for entry in fs::read_dir(&root.path)? {
        let entry = entry?;
        match root.kind {
            SkillRegistryRootKind::ManagedStore => continue,
            SkillRegistryRootKind::SkillsDir => {
                if !entry.path().is_dir() {
                    continue;
                }
                let path = entry.path().join("SKILL.md");
                if !path.is_file() {
                    continue;
                }
                skills.push(skill_info_from_path(root, &path, || {
                    entry.file_name().to_string_lossy().to_string()
                })?);
            }
            SkillRegistryRootKind::LegacyCommandsDir => {
                let path = entry.path();
                let markdown_path = if path.is_dir() {
                    let skill_path = path.join("SKILL.md");
                    if !skill_path.is_file() {
                        continue;
                    }
                    skill_path
                } else if path
                    .extension()
                    .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("md"))
                {
                    path
                } else {
                    continue;
                };
                skills.push(skill_info_from_path(root, &markdown_path, || {
                    markdown_path.file_stem().map_or_else(
                        || entry.file_name().to_string_lossy().to_string(),
                        |stem| stem.to_string_lossy().to_string(),
                    )
                })?);
            }
        }
    }
    Ok(skills)
}

fn skill_info_from_path(
    root: &SkillRegistryRoot,
    path: &Path,
    fallback_name: impl FnOnce() -> String,
) -> std::io::Result<SkillInfo> {
    let parsed = parse_skill_file_header(path).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to parse {}: {error}", path.display()),
        )
    })?;
    Ok(skill_info_from_parsed(root, path, &parsed, fallback_name()))
}

fn skill_info_from_parsed(
    root: &SkillRegistryRoot,
    path: &Path,
    parsed: &ParsedSkill,
    fallback_name: String,
) -> SkillInfo {
    SkillInfo {
        name: get_skill_name(parsed)
            .map(ToOwned::to_owned)
            .unwrap_or(fallback_name),
        description: get_skill_description(parsed).map(ToOwned::to_owned),
        version: parsed
            .manifest
            .as_ref()
            .and_then(|manifest| manifest.version.clone()),
        path: path.to_path_buf(),
        root: root.path.clone(),
        source: root.source,
        scope: root.source.scope(),
        kind: root.kind,
        tags: get_tags(parsed),
        related_skills: get_related_skills(parsed),
        platforms: platforms(parsed),
        shadowed_by: None,
    }
}

fn platforms(parsed: &ParsedSkill) -> Vec<String> {
    if matches_platform(parsed, "linux")
        && matches_platform(parsed, "macos")
        && matches_platform(parsed, "windows")
    {
        return Vec::new();
    }
    ["linux", "macos", "windows"]
        .into_iter()
        .filter(|platform| matches_platform(parsed, platform))
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_skill_name(skill: &str) -> std::io::Result<&str> {
    let requested = skill.trim().trim_start_matches('/').trim_start_matches('$');
    if requested.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "skill must not be empty",
        ));
    }
    Ok(requested)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let path = env::temp_dir().join(format!("cowd-skill-registry-{name}-{millis}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn registry_lists_skills_and_marks_shadowed_duplicates() {
        let root = temp_dir("shadow");
        let project = root.join("project-skills");
        let user = root.join("user-skills");
        write(
            &project.join("deploy").join("SKILL.md"),
            "---\nname: deploy\ndescription: Project deploy\ntags: [release]\n---\n# Deploy\n",
        );
        write(
            &user.join("deploy").join("SKILL.md"),
            "---\nname: deploy\ndescription: User deploy\n---\n# Deploy\n",
        );

        let registry = SkillRegistry::with_roots(vec![
            SkillRegistryRoot {
                source: SkillRegistrySource::ProjectCowd,
                path: project,
                kind: SkillRegistryRootKind::SkillsDir,
            },
            SkillRegistryRoot {
                source: SkillRegistrySource::UserCowd,
                path: user,
                kind: SkillRegistryRootKind::SkillsDir,
            },
        ]);

        let skills = registry.list().unwrap();
        assert_eq!(skills.len(), 2);
        assert_eq!(skills[0].description.as_deref(), Some("Project deploy"));
        assert_eq!(skills[0].shadowed_by, None);
        assert_eq!(skills[0].tags, vec!["release"]);
        assert_eq!(
            skills[1].shadowed_by,
            Some(SkillRegistrySource::ProjectCowd)
        );
    }

    #[test]
    fn registry_resolves_frontmatter_name_and_legacy_commands() {
        let root = temp_dir("legacy");
        let commands = root.join("commands");
        write(
            &commands.join("ship.md"),
            "---\nname: release-ship\ndescription: Ship release\n---\n# Ship\n",
        );

        let registry = SkillRegistry::with_roots(vec![SkillRegistryRoot {
            source: SkillRegistrySource::ProjectCowd,
            path: commands,
            kind: SkillRegistryRootKind::LegacyCommandsDir,
        }]);

        let skill = registry.resolve("$release-ship").unwrap();
        assert_eq!(skill.name, "release-ship");
        assert_eq!(skill.kind, SkillRegistryRootKind::LegacyCommandsDir);
        assert!(skill.path.ends_with("ship.md"));
    }

    #[test]
    fn registry_resolves_only_the_managed_active_revision() {
        let root = temp_dir("managed");
        let source = root.join("source");
        write(
            &source.join("SKILL.md"),
            "---\nname: managed-demo\ndescription: Managed demo\nlicense: MIT\nversion: 1.0.0\n---\nFirst\n",
        );
        let store = crate::ManagedSkillStore::new(root.join("store"));
        let lifecycle = crate::SkillLifecycle::new(store.clone()).unwrap();
        let source_value = source.to_string_lossy();
        let first = lifecycle.plan(&source_value, &root).unwrap();
        lifecycle
            .commit(&source_value, &root, &first.package_digest, false, "test")
            .unwrap();
        write(
            &source.join("SKILL.md"),
            "---\nname: managed-demo\ndescription: Managed demo\nlicense: MIT\nversion: 2.0.0\n---\nSecond\n",
        );
        let second = lifecycle.plan(&source_value, &root).unwrap();
        let receipt = lifecycle
            .commit(&source_value, &root, &second.package_digest, false, "test")
            .unwrap();

        let registry = SkillRegistry::with_roots(vec![SkillRegistryRoot {
            source: SkillRegistrySource::UserCowdManaged,
            path: store.root().to_path_buf(),
            kind: SkillRegistryRootKind::ManagedStore,
        }]);
        assert_eq!(registry.list().unwrap().len(), 2);
        assert!(registry.resolve("managed-demo").is_ok());
        #[derive(Debug)]
        struct Approval(Option<SkillActivePointer>);
        impl ManagedSkillApproval for Approval {
            fn scope(&self) -> std::io::Result<String> {
                Ok("test-scope".into())
            }
            fn active_pointer(&self, _: &str) -> std::io::Result<Option<SkillActivePointer>> {
                Ok(self.0.clone())
            }
        }
        assert!(registry
            .clone()
            .with_managed_approval(Arc::new(Approval(None)))
            .list()
            .unwrap()
            .is_empty());
        assert!(registry
            .clone()
            .with_managed_approval(Arc::new(Approval(None)))
            .resolve("managed-demo")
            .is_err());
        let mut pointer = SkillActivePointer {
            skill_id: receipt.skill_id.clone(),
            active_revision: receipt.revision.clone(),
            previous_revision: Some(first.package_digest),
            generation: 2,
            source_draft_id: Some("draft".into()),
            approval_ref: "approval".into(),
            activated_at_ms: 1,
            installation: Some(harness_contract::skill::SkillInstallationEvidence {
                scope_ref: "test-scope".into(),
                skill_id: receipt.skill_id.clone(),
                revision: receipt.revision.clone(),
                receipt_id: receipt.install_id.clone(),
                receipt_digest: format!(
                    "sha256:{:x}",
                    Sha256::digest(serde_json::to_vec(&receipt).unwrap())
                ),
                inspection_fingerprint: receipt.plan_digest.clone(),
            }),
            revoked: Some(false),
            revocation_generation: Some(0),
        };
        let approved = registry
            .clone()
            .with_managed_approval(Arc::new(Approval(Some(pointer.clone()))));
        let skills = approved.list().unwrap();
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].version.as_deref(), Some("2.0.0"));
        assert_eq!(
            approved.resolve("managed-demo").unwrap().path,
            skills[0].path
        );
        assert!(skills[0]
            .path
            .to_string_lossy()
            .contains(second.package_digest.trim_start_matches("sha256:")));
        let mut cases = Vec::new();
        let mut invalid = pointer.clone();
        invalid.skill_id = "foreign".into();
        invalid.generation = 0;
        cases.push((invalid, "pointer_skill_id_matches"));
        let mut invalid = pointer.clone();
        invalid.generation = 0;
        cases.push((invalid, "pointer_generation_nonzero"));
        let mut invalid = pointer.clone();
        invalid.approval_ref.clear();
        cases.push((invalid, "approval_ref_nonempty"));
        let mut invalid = pointer.clone();
        invalid.installation.as_mut().unwrap().skill_id = "foreign".into();
        cases.push((invalid, "evidence_skill_id_matches"));
        let mut invalid = pointer.clone();
        invalid.installation.as_mut().unwrap().revision = "foreign".into();
        cases.push((invalid, "evidence_revision_matches"));
        let mut invalid = pointer.clone();
        invalid.installation.as_mut().unwrap().scope_ref = "foreign".into();
        cases.push((invalid, "evidence_scope_matches"));
        let mut invalid = pointer.clone();
        invalid.installation.as_mut().unwrap().receipt_digest = "foreign".into();
        invalid
            .installation
            .as_mut()
            .unwrap()
            .inspection_fingerprint = "foreign".into();
        cases.push((invalid, "receipt_digest_matches"));
        let mut invalid = pointer.clone();
        invalid
            .installation
            .as_mut()
            .unwrap()
            .inspection_fingerprint = "foreign".into();
        cases.push((invalid, "inspection_fingerprint_matches"));
        for (invalid, predicate) in cases {
            let error = registry
                .clone()
                .with_managed_approval(Arc::new(Approval(Some(invalid))))
                .resolve("managed-demo")
                .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            let original = if predicate == "receipt_digest_matches"
                || predicate == "inspection_fingerprint_matches"
            {
                "managed skill installation evidence mismatch; "
            } else {
                "managed skill approval identity mismatch; "
            };
            assert!(error.to_string().starts_with(original));
            assert!(
                error
                    .to_string()
                    .contains(&format!("first_failed_predicate={predicate} actual=")),
                "{error}"
            );
        }
        #[derive(Debug)]
        struct EmptyScope(SkillActivePointer);
        impl ManagedSkillApproval for EmptyScope {
            fn scope(&self) -> std::io::Result<String> {
                Ok(String::new())
            }
            fn active_pointer(&self, _: &str) -> std::io::Result<Option<SkillActivePointer>> {
                Ok(Some(self.0.clone()))
            }
        }
        let mut empty_scope = pointer.clone();
        empty_scope.installation.as_mut().unwrap().scope_ref.clear();
        let error = registry
            .clone()
            .with_managed_approval(Arc::new(EmptyScope(empty_scope)))
            .resolve("managed-demo")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("first_failed_predicate=scope_nonempty actual=0"));
        pointer.revoked = Some(true);
        assert!(registry
            .clone()
            .with_managed_approval(Arc::new(Approval(Some(pointer.clone()))))
            .list()
            .unwrap()
            .is_empty());
        assert!(registry
            .clone()
            .with_managed_approval(Arc::new(Approval(Some(pointer.clone()))))
            .resolve("managed-demo")
            .is_err());
        pointer.revoked = Some(false);
        pointer.installation.as_mut().unwrap().receipt_digest = "sha256:foreign".into();
        assert!(registry
            .with_managed_approval(Arc::new(Approval(Some(pointer))))
            .resolve("managed-demo")
            .is_err());
    }
}
