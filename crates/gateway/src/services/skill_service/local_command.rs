use std::{
    fs,
    path::{Path, PathBuf},
};

use command_service::SkillSlashDispatch;
use skill_service::{SkillRegistry, SkillRegistryRootKind, SkillViewOutput};

#[derive(Debug, Clone)]
pub(super) struct LocalSkillSummary {
    name: String,
    description: Option<String>,
    source: String,
    shadowed_by: Option<String>,
    origin: &'static str,
    path: String,
}

#[derive(Debug, Clone)]
pub(super) struct InstalledSkill {
    pub(super) invocation_name: String,
    pub(super) display_name: Option<String>,
    pub(super) source: PathBuf,
    pub(super) registry_root: PathBuf,
    pub(super) installed_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(super) enum SkillInstallSource {
    Directory { root: PathBuf, prompt_path: PathBuf },
    MarkdownFile { path: PathBuf },
}

impl SkillInstallSource {
    fn prompt_path(&self) -> &Path {
        match self {
            Self::Directory { prompt_path, .. } => prompt_path,
            Self::MarkdownFile { path } => path,
        }
    }

    fn report_path(&self) -> &Path {
        match self {
            Self::Directory { root, .. } => root,
            Self::MarkdownFile { path } => path,
        }
    }

    fn fallback_name(&self) -> Option<String> {
        let path = self.report_path();
        if path.is_file() {
            path.file_stem()
        } else {
            path.file_name()
        }
        .map(|name| name.to_string_lossy().into_owned())
    }
}
#[must_use]
pub(super) fn classify_static_skill_command(args: Option<&str>) -> SkillSlashDispatch {
    match normalize_optional_args(args) {
        None | Some("list" | "help" | "-h" | "--help") => SkillSlashDispatch::Local,
        Some(args) if args == "install" || args.starts_with("install ") => {
            SkillSlashDispatch::Local
        }
        Some("view") => SkillSlashDispatch::Local,
        Some(args) if args.starts_with("view ") => SkillSlashDispatch::Local,
        Some("create" | "edit" | "delete" | "generate") => SkillSlashDispatch::Local,
        Some(args)
            if args.starts_with("create ")
                || args.starts_with("edit ")
                || args.starts_with("delete ")
                || args.starts_with("generate ") =>
        {
            SkillSlashDispatch::Local
        }
        Some(args) => SkillSlashDispatch::Invoke(format!("${}", args.trim_start_matches('/'))),
    }
}

pub(super) fn normalize_optional_args(args: Option<&str>) -> Option<&str> {
    args.map(str::trim).filter(|value| !value.is_empty())
}

pub(super) fn is_help_arg(arg: &str) -> bool {
    matches!(arg.trim(), "help" | "-h" | "--help")
}

pub(super) fn help_path_from_args(args: &str) -> Option<Vec<&str>> {
    let parts = args.split_whitespace().collect::<Vec<_>>();
    let help_index = parts.iter().position(|part| is_help_arg(part))?;
    Some(parts[..help_index].to_vec())
}

pub(super) fn discover_skill_root_paths(cwd: &Path) -> Vec<PathBuf> {
    SkillRegistry::discover(cwd)
        .roots()
        .iter()
        .filter(|root| root.kind == SkillRegistryRootKind::SkillsDir)
        .map(|root| root.path.clone())
        .collect()
}

pub(super) fn local_skill_summaries(cwd: &Path) -> std::io::Result<Vec<LocalSkillSummary>> {
    SkillRegistry::discover(cwd).list().map(|skills| {
        skills
            .into_iter()
            .map(|skill| LocalSkillSummary {
                name: skill.name,
                description: skill.description,
                source: format!("{:?}", skill.source),
                shadowed_by: skill.shadowed_by.map(|source| format!("{source:?}")),
                origin: match skill.kind {
                    SkillRegistryRootKind::SkillsDir => "skills",
                    SkillRegistryRootKind::LegacyCommandsDir => "legacy /commands",
                },
                path: skill.path.display().to_string(),
            })
            .collect()
    })
}

pub(super) fn render_skills_report(skills: &[LocalSkillSummary]) -> String {
    if skills.is_empty() {
        return "No skills found.".to_string();
    }
    let active = skills
        .iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .count();
    let mut lines = vec![
        "Skills".to_string(),
        format!("  {active} available skills"),
        String::new(),
    ];
    for skill in skills {
        let mut detail = vec![skill.name.clone()];
        if let Some(description) = &skill.description {
            detail.push(description.clone());
        }
        if skill.origin != "skills" {
            detail.push(skill.origin.to_string());
        }
        match &skill.shadowed_by {
            Some(winner) => lines.push(format!("  (shadowed by {winner}) {}", detail.join(" · "))),
            None => lines.push(format!("  {}", detail.join(" · "))),
        }
    }
    lines.join("\n")
}

pub(super) fn render_skills_report_json(skills: &[LocalSkillSummary]) -> serde_json::Value {
    let active = skills
        .iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .count();
    serde_json::json!({
        "kind": "skills",
        "action": "list",
        "summary": {
            "total": skills.len(),
            "active": active,
            "shadowed": skills.len().saturating_sub(active),
        },
        "skills": skills.iter().map(|skill| serde_json::json!({
            "name": skill.name,
            "description": skill.description,
            "source": skill.source,
            "shadowed_by": skill.shadowed_by,
            "origin": skill.origin,
            "path": skill.path,
        })).collect::<Vec<_>>(),
    })
}

pub(super) fn render_skill_view_report(result: &SkillViewOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Name             {}", result.name));
        lines.push(format!("  Description      {}", result.description));
        if !result.tags.is_empty() {
            lines.push(format!("  Tags             {}", result.tags.join(", ")));
        }
        lines.push(format!(
            "  Status           {}",
            if result.setup_needed {
                "setup_needed"
            } else {
                "ready"
            }
        ));
        lines.push(String::new());
        lines.push("---".to_string());
        lines.push(String::new());
        let preview = if result.content.len() > 500 {
            format!(
                "{}...\n\n[Truncated - use /skill view {} --file <path> for full content]",
                &result.content[..500],
                result.name
            )
        } else {
            result.content.clone()
        };
        lines.push(preview);
    } else {
        lines.push("  Result           not found".to_string());
    }
    lines.join("\n")
}

pub(super) fn render_skills_usage(topic: Option<&str>) -> String {
    match topic {
        Some("create" | "edit" | "delete" | "generate" | "managed") => {
            "Skills - Managed In WebUI/TUI\n\nThe CLI intentionally exposes only list, view, install, and invocation.\nUse WebUI or TUI for create, edit, delete, generate, validation, run queues,\ngovernance review, and stateful skill management.".to_string()
        }
        Some("view") => "Skills - View\n\nUsage: /skill view <name>".to_string(),
        Some("install") => "Skills - Install\n\nUsage: /skill install <source>".to_string(),
        _ => [
            "Skills",
            "  Usage            /skills [list|view <name>|install <path>|help|<skill> [args]]",
            "  Alias            /skill",
            "  Direct CLI       cowd skill [list|view <name>|install <path>|help]",
            "  Local controls   list, view <name>, install <path>",
            "  Managed in UI    create, edit, delete, generate, validate, run, governance",
        ]
        .join("\n"),
    }
}

pub(super) fn render_skills_usage_json(topic: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "kind": "skills",
        "action": "help",
        "topic": topic.unwrap_or("overview"),
        "usage": render_skills_usage(topic),
    })
}

pub(super) fn install_skill(source: &str, cwd: &Path) -> std::io::Result<InstalledSkill> {
    let registry_root = crate::skill_static::default_skill_install_root()?;
    install_skill_into(source, cwd, &registry_root)
}

pub(super) fn install_skill_into(
    source: &str,
    cwd: &Path,
    registry_root: &Path,
) -> std::io::Result<InstalledSkill> {
    let source = resolve_skill_install_source(source, cwd)?;
    let contents = fs::read_to_string(source.prompt_path())?;
    let display_name = parse_skill_frontmatter(&contents).0;
    let invocation_name = derive_skill_install_name(&source, display_name.as_deref())?;
    let installed_path = registry_root.join(&invocation_name);
    fs::create_dir_all(&registry_root)?;
    if installed_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "skill '{invocation_name}' is already installed at {}",
                installed_path.display()
            ),
        ));
    }

    fs::create_dir_all(&installed_path)?;
    let install_result = match &source {
        SkillInstallSource::Directory { root, .. } => {
            copy_directory_contents(root, &installed_path)
        }
        SkillInstallSource::MarkdownFile { path } => {
            fs::copy(path, installed_path.join("SKILL.md")).map(|_| ())
        }
    };
    if let Err(error) = install_result {
        let _ = fs::remove_dir_all(&installed_path);
        return Err(error);
    }

    Ok(InstalledSkill {
        display_name,
        invocation_name,
        source: source.report_path().to_path_buf(),
        registry_root: registry_root.to_path_buf(),
        installed_path,
    })
}

pub(super) fn resolve_skill_install_source(
    source: &str,
    cwd: &Path,
) -> std::io::Result<SkillInstallSource> {
    let candidate = PathBuf::from(source);
    let path = if candidate.is_absolute() {
        candidate
    } else {
        cwd.join(candidate)
    };
    let canonical = path.canonicalize()?;

    if canonical.is_dir() {
        let prompt_path = canonical.join("SKILL.md");
        if !prompt_path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "skill directory '{}' must contain SKILL.md",
                    canonical.display()
                ),
            ));
        }
        return Ok(SkillInstallSource::Directory {
            root: canonical,
            prompt_path,
        });
    }

    if canonical
        .extension()
        .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("md"))
    {
        return Ok(SkillInstallSource::MarkdownFile { path: canonical });
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "skill source '{}' must be a directory with SKILL.md or a markdown file",
            canonical.display()
        ),
    ))
}

fn derive_skill_install_name(
    source: &SkillInstallSource,
    declared_name: Option<&str>,
) -> std::io::Result<String> {
    for candidate in [declared_name, source.fallback_name().as_deref()] {
        if let Some(candidate) = candidate.and_then(sanitize_skill_invocation_name) {
            return Ok(candidate);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "unable to derive an installable invocation name from '{}'",
            source.report_path().display()
        ),
    ))
}

fn parse_skill_frontmatter(contents: &str) -> (Option<String>, Option<String>) {
    let mut lines = contents.lines();
    if lines.next().map(str::trim) != Some("---") {
        return (None, None);
    }

    let mut name = None;
    let mut description = None;
    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            break;
        }
        if let Some(value) = trimmed.strip_prefix("name:") {
            let value = unquote_frontmatter_value(value.trim());
            if !value.is_empty() {
                name = Some(value);
            }
            continue;
        }
        if let Some(value) = trimmed.strip_prefix("description:") {
            let value = unquote_frontmatter_value(value.trim());
            if !value.is_empty() {
                description = Some(value);
            }
        }
    }

    (name, description)
}

fn unquote_frontmatter_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|trimmed| trimmed.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|trimmed| trimmed.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn sanitize_skill_invocation_name(candidate: &str) -> Option<String> {
    let trimmed = candidate
        .trim()
        .trim_start_matches('/')
        .trim_start_matches('$');
    if trimmed.is_empty() {
        return None;
    }

    let mut name = String::new();
    let mut previous_dash = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            previous_dash = false;
            name.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '-' | '_' | ' ' | '.') {
            if !previous_dash {
                name.push('-');
                previous_dash = true;
            }
        } else {
            return None;
        }
    }

    let name = name.trim_matches('-').to_string();
    (!name.is_empty()).then_some(name)
}

fn copy_directory_contents(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_directory_contents(&source_path, &destination_path)?;
        } else if source_path.is_file() {
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

pub(super) fn render_skill_install_report(skill: &InstalledSkill) -> String {
    let mut lines = vec![
        "Skills".to_string(),
        format!("  Result           installed {}", skill.invocation_name),
    ];
    if let Some(display_name) = &skill.display_name {
        lines.push(format!("  Display name     {display_name}"));
    }
    lines.push(format!("  Source           {}", skill.source.display()));
    lines.push(format!(
        "  Registry         {}",
        skill.registry_root.display()
    ));
    lines.push(format!(
        "  Installed path   {}",
        skill.installed_path.display()
    ));
    lines.join("\n")
}

pub(super) fn render_skill_install_report_json(skill: &InstalledSkill) -> serde_json::Value {
    serde_json::json!({
        "kind": "skills",
        "action": "install",
        "status": "installed",
        "invocation_name": skill.invocation_name,
        "display_name": skill.display_name,
        "source": skill.source.display().to_string(),
        "registry_root": skill.registry_root.display().to_string(),
        "installed_path": skill.installed_path.display().to_string(),
    })
}
