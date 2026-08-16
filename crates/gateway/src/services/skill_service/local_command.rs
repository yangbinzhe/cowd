use std::path::{Path, PathBuf};

use crate::command::slash::SkillSlashDispatch;
use skill::{SkillRegistry, SkillRegistryRootKind, SkillViewOutput};

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
    pub(super) source: String,
    pub(super) registry_root: PathBuf,
    pub(super) installed_path: PathBuf,
}

#[must_use]
pub(super) fn classify_static_skill_command(args: Option<&str>) -> SkillSlashDispatch {
    match normalize_optional_args(args) {
        None | Some("list" | "help" | "-h" | "--help") => SkillSlashDispatch::Local,
        Some(args) if args == "install" || args.starts_with("install ") => {
            SkillSlashDispatch::Local
        }
        Some(args)
            if args == "plan"
                || args.starts_with("plan ")
                || args == "status"
                || args.starts_with("status ")
                || args == "rollback"
                || args.starts_with("rollback ")
                || args == "remove"
                || args.starts_with("remove ") =>
        {
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
        .flat_map(|root| match root.kind {
            SkillRegistryRootKind::ManagedStore => skill::list_managed_skill_entries(&root.path)
                .unwrap_or_default()
                .into_iter()
                .map(|entry| entry.package_root)
                .collect::<Vec<_>>(),
            SkillRegistryRootKind::SkillsDir => vec![root.path.clone()],
            SkillRegistryRootKind::LegacyCommandsDir => Vec::new(),
        })
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
                    SkillRegistryRootKind::ManagedStore => "managed store",
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
            "Skills - Managed In WebUI/TUI\n\nCreation and editing remain human definition-management operations.\nAcquisition, review, atomic activation, status, rollback, and deactivation use the same governed Skill lifecycle in CLI, TUI, Gateway, and model tools.".to_string()
        }
        Some("view") => "Skills - View\n\nUsage: /skill view <name>".to_string(),
        Some("install") => "Skills - Install\n\nUsage:\n  /skill plan <source>\n  /skill install <source> --expected-digest <sha256:digest> [--allow-warnings]\n\nInstall re-acquires the reviewed source, verifies the exact SHA-256 tree digest, and atomically publishes an immutable revision. It never executes package content or grants permissions.".to_string(),
        _ => [
            "Skills",
            "  Usage            /skills [list|view <name>|plan <source>|install <source> --expected-digest <digest>|status <name>|rollback <name> <digest>|remove <name>|help|<skill> [args]]",
            "  Alias            /skill",
            "  Direct CLI       cowd skill [list|show|validate|plan|install|status|rollback|remove]",
            "  Local controls   immutable revisions, atomic active pointer, durable receipts",
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

pub(super) fn parse_reviewed_install_args(raw: &str) -> Option<(&str, &str, bool)> {
    let raw = raw.trim();
    let (raw, allow_warnings) = raw
        .strip_suffix(" --allow-warnings")
        .map_or((raw, false), |value| (value.trim_end(), true));
    let (source, expected_digest) = raw.rsplit_once(" --expected-digest ")?;
    let source = source.trim();
    let expected_digest = expected_digest.trim();
    (!source.is_empty() && !expected_digest.is_empty()).then_some((
        source,
        expected_digest,
        allow_warnings,
    ))
}

pub(super) fn install_skill_with_policy(
    source: &str,
    cwd: &Path,
    expected_digest: &str,
    allow_warnings: bool,
) -> std::io::Result<InstalledSkill> {
    let lifecycle = skill::SkillLifecycle::default_for_user().map_err(std::io::Error::other)?;
    let receipt = lifecycle
        .commit(source, cwd, expected_digest, allow_warnings, "cli:slash")
        .map_err(std::io::Error::other)?;
    let registry_root = lifecycle.store().root().to_path_buf();
    let installed_path = registry_root
        .join(&receipt.skill_id)
        .join("revisions")
        .join(receipt.revision.trim_start_matches("sha256:"));
    Ok(InstalledSkill {
        display_name: Some(receipt.name),
        invocation_name: receipt.skill_id,
        source: receipt.source.locator,
        registry_root,
        installed_path,
    })
}

pub(super) fn render_skill_install_report(skill: &InstalledSkill) -> String {
    let mut lines = vec![
        "Skills".to_string(),
        format!("  Result           installed {}", skill.invocation_name),
    ];
    if let Some(display_name) = &skill.display_name {
        lines.push(format!("  Display name     {display_name}"));
    }
    lines.push(format!("  Source           {}", skill.source));
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
        "source": skill.source,
        "registry_root": skill.registry_root.display().to_string(),
        "installed_path": skill.installed_path.display().to_string(),
    })
}
