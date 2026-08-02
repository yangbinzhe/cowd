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
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRegistrySource {
    ProjectCowd,
    ProjectAgents,
    ProjectCodex,
    ProjectClaude,
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
            Self::UserCowdConfigHome | Self::UserCodexHome => SkillRegistryScope::UserConfigHome,
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
}

impl SkillRegistry {
    #[must_use]
    pub fn discover(cwd: &Path) -> Self {
        Self {
            roots: discover_skill_registry_roots(cwd),
        }
    }

    #[must_use]
    pub fn with_roots(roots: Vec<SkillRegistryRoot>) -> Self {
        Self { roots }
    }

    #[must_use]
    pub fn roots(&self) -> &[SkillRegistryRoot] {
        &self.roots
    }

    pub fn list(&self) -> std::io::Result<Vec<SkillInfo>> {
        let mut skills = Vec::new();
        let mut active_sources = BTreeMap::<String, SkillRegistrySource>::new();

        for root in &self.roots {
            let mut root_skills = match list_root_skills(root) {
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

fn list_root_skills(root: &SkillRegistryRoot) -> std::io::Result<Vec<SkillInfo>> {
    let mut skills = Vec::new();
    for entry in fs::read_dir(&root.path)? {
        let entry = entry?;
        match root.kind {
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
}
