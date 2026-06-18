use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use command_contract::SkillSlashDispatch;
use command_service::{
    SkillInfo, SkillManager, SkillRegistry, SkillRegistryRootKind, SkillRegistrySource,
    SkillViewInput, SkillViewOutput,
};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DefinitionSource {
    ProjectClaw,
    ProjectCodex,
    ProjectClaude,
    UserClawConfigHome,
    UserCodexHome,
    UserClaw,
    UserCodex,
    UserClaude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DefinitionScope {
    Project,
    UserConfigHome,
    UserHome,
}

impl DefinitionScope {
    fn label(self) -> &'static str {
        match self {
            Self::Project => "Project roots",
            Self::UserConfigHome => "User config roots",
            Self::UserHome => "User home roots",
        }
    }
}

impl DefinitionSource {
    fn report_scope(self) -> DefinitionScope {
        match self {
            Self::ProjectClaw | Self::ProjectCodex | Self::ProjectClaude => {
                DefinitionScope::Project
            }
            Self::UserClawConfigHome | Self::UserCodexHome => DefinitionScope::UserConfigHome,
            Self::UserClaw | Self::UserCodex | Self::UserClaude => DefinitionScope::UserHome,
        }
    }

    fn label(self) -> &'static str {
        self.report_scope().label()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentSummary {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) source: DefinitionSource,
    pub(crate) shadowed_by: Option<DefinitionSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticAgentMatch {
    name: String,
    description: Option<String>,
    source: DefinitionSource,
    shadowed_by: Option<DefinitionSource>,
    match_terms: Vec<String>,
    score: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticAgentTeam {
    leader: StaticAgentMatch,
    workers: Vec<StaticAgentMatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkillSummary {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) source: DefinitionSource,
    pub(crate) shadowed_by: Option<DefinitionSource>,
    pub(crate) origin: SkillOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkillOrigin {
    SkillsDir,
    LegacyCommandsDir,
}

impl SkillOrigin {
    fn detail_label(self) -> Option<&'static str> {
        match self {
            Self::SkillsDir => None,
            Self::LegacyCommandsDir => Some("legacy /commands"),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkillRoot {
    pub(crate) source: DefinitionSource,
    pub(crate) path: PathBuf,
    pub(crate) origin: SkillOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledSkill {
    pub(crate) invocation_name: String,
    pub(crate) display_name: Option<String>,
    pub(crate) source: PathBuf,
    pub(crate) registry_root: PathBuf,
    pub(crate) installed_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkillInstallSource {
    Directory { root: PathBuf, prompt_path: PathBuf },
    MarkdownFile { path: PathBuf },
}

pub fn handle_agents_slash_command(args: Option<&str>, cwd: &Path) -> std::io::Result<String> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_agents_usage(None),
                _ => render_agents_usage(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let roots = discover_definition_roots(cwd, "agents");
            let agents = load_agents_from_roots(&roots)?;
            Ok(render_agents_report(&agents))
        }
        Some(args) if args.starts_with("discover") => {
            let task_desc = args.strip_prefix("discover").unwrap_or("").trim();
            if task_desc.is_empty() {
                return Ok("Usage: /agents discover <task description>\n\nProvide a task description to discover a matching agent team.".to_string());
            }
            let roots = discover_definition_roots(cwd, "agents");
            let agents = load_agents_from_roots(&roots)?;
            let ranked = discover_agents_for_task(&agents, task_desc);
            if ranked.is_empty() {
                return Ok(format!(
                    "No agents matched the task: \"{task_desc}\"\n\nRegister agents with relevant capabilities first."
                ));
            }
            let mut report = format!(
                "Discovered {} agent(s) for \"{task_desc}\"\n\n",
                ranked.len()
            );
            for (i, agent) in ranked.iter().enumerate() {
                report.push_str(&format!(
                    "  {}. {} ({}) — [{}]\n",
                    i + 1,
                    agent.name,
                    agent.source.label(),
                    agent.match_terms.join(", "),
                ));
            }
            if let Some(team) = assemble_static_agent_team(&ranked) {
                report.push_str(&format!(
                    "\nAuto-assembled team:\n  Leader: {} ({})\n",
                    team.leader.name,
                    team.leader.source.label()
                ));
                if !team.workers.is_empty() {
                    report.push_str("  Workers:\n");
                    for w in &team.workers {
                        report.push_str(&format!(
                            "    - {} ({}) [{}]\n",
                            w.name,
                            w.source.label(),
                            w.match_terms.join(", ")
                        ));
                    }
                } else {
                    report.push_str("  Workers: none\n");
                }
            }
            Ok(report)
        }
        Some(args) if is_help_arg(args) => Ok(render_agents_usage(None)),
        Some(args) => Ok(render_agents_usage(Some(args))),
    }
}

pub fn handle_agents_slash_command_json(args: Option<&str>, cwd: &Path) -> std::io::Result<Value> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_agents_usage_json(None),
                _ => render_agents_usage_json(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let roots = discover_definition_roots(cwd, "agents");
            let agents = load_agents_from_roots(&roots)?;
            Ok(render_agents_report_json(cwd, &agents))
        }
        Some(args) if args.starts_with("discover") => {
            let task_desc = args.strip_prefix("discover").unwrap_or("").trim();
            let roots = discover_definition_roots(cwd, "agents");
            let agents = load_agents_from_roots(&roots)?;
            let ranked = discover_agents_for_task(&agents, task_desc);
            let agents_json: Vec<Value> = ranked
                .iter()
                .map(|a| {
                    json!({
                        "agent_id": a.name,
                        "role": a.description,
                        "capabilities": a.match_terms,
                        "reputation": null,
                        "status": if a.shadowed_by.is_some() { "shadowed" } else { "active" },
                        "source": definition_source_json(a.source),
                    })
                })
                .collect();
            let team = assemble_static_agent_team(&ranked);
            let team_json = team.map(|t| {
                json!({
                    "leader": { "agent_id": t.leader.name, "role": t.leader.description },
                    "workers": t.workers.iter().map(|w| json!({ "agent_id": w.name, "role": w.description })).collect::<Vec<_>>(),
                })
            });
            Ok(json!({
                "kind": "agents",
                "action": "discover",
                "task": task_desc,
                "count": ranked.len(),
                "agents": agents_json,
                "team": team_json,
            }))
        }
        Some(args) if is_help_arg(args) => Ok(render_agents_usage_json(None)),
        Some(args) => Ok(render_agents_usage_json(Some(args))),
    }
}

pub fn handle_skills_slash_command(args: Option<&str>, cwd: &Path) -> std::io::Result<String> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_skills_usage(None),
                ["install", ..] => render_skills_usage(Some("install")),
                ["view", ..] => render_skills_usage(Some("view")),
                ["create" | "edit" | "delete" | "generate", ..] => {
                    render_skills_usage(Some("managed"))
                }
                _ => render_skills_usage(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let skills = load_skills_from_registry(cwd)?;
            Ok(render_skills_report(&skills))
        }
        Some("install") => Ok(render_skills_usage(Some("install"))),
        Some(args) if args.starts_with("install ") => {
            let target = args["install ".len()..].trim();
            if target.is_empty() {
                return Ok(render_skills_usage(Some("install")));
            }
            let install = install_skill(target, cwd)?;
            Ok(render_skill_install_report(&install))
        }
        Some("view") => Ok(render_skills_usage(Some("view"))),
        Some(args) if args.starts_with("view ") => {
            let name = args["view ".len()..].trim();
            if name.is_empty() {
                return Ok(render_skills_usage(Some("view")));
            }
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let input = SkillViewInput {
                name: name.to_string(),
                file_path: None,
                include_files: true,
            };
            let result = manager.view_skill(input);
            Ok(render_skill_view_report(&result))
        }
        Some("create" | "edit" | "delete" | "generate") => Ok(render_skills_usage(Some("managed"))),
        Some(args)
            if args.starts_with("create ")
                || args.starts_with("edit ")
                || args.starts_with("delete ")
                || args.starts_with("generate ") =>
        {
            Ok(render_skills_usage(Some("managed")))
        }
        Some(args) if is_help_arg(args) => Ok(render_skills_usage(None)),
        Some(args) => Ok(render_skills_usage(Some(args))),
    }
}

// Discover skill root paths for SkillManager
fn discover_skill_root_paths(cwd: &Path) -> Vec<PathBuf> {
    SkillRegistry::discover(cwd)
        .roots()
        .iter()
        .filter(|root| root.kind == SkillRegistryRootKind::SkillsDir)
        .map(|root| root.path.clone())
        .collect()
}

pub fn handle_skills_slash_command_json(args: Option<&str>, cwd: &Path) -> std::io::Result<Value> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_skills_usage_json(None),
                ["install", ..] => render_skills_usage_json(Some("install")),
                ["view", ..] => render_skills_usage_json(Some("view")),
                ["create" | "edit" | "delete" | "generate", ..] => {
                    render_skills_usage_json(Some("managed"))
                }
                _ => render_skills_usage_json(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let skills = load_skills_from_registry(cwd)?;
            Ok(render_skills_report_json(&skills))
        }
        Some("install") => Ok(render_skills_usage_json(Some("install"))),
        Some(args) if args.starts_with("install ") => {
            let target = args["install ".len()..].trim();
            if target.is_empty() {
                return Ok(render_skills_usage_json(Some("install")));
            }
            let install = install_skill(target, cwd)?;
            Ok(render_skill_install_report_json(&install))
        }
        Some("view") => Ok(render_skills_usage_json(Some("view"))),
        Some(args) if args.starts_with("view ") => {
            let name = args["view ".len()..].trim();
            if name.is_empty() {
                return Ok(render_skills_usage_json(Some("view")));
            }
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let input = SkillViewInput {
                name: name.to_string(),
                file_path: None,
                include_files: true,
            };
            let result = manager.view_skill(input);
            Ok(json!({
                "kind": "skills",
                "action": "view",
                "success": result.success,
                "name": result.name,
                "description": result.description,
                "tags": result.tags,
                "content": result.content,
                "setup_needed": result.setup_needed,
                "readiness_status": result.readiness_status,
                "linked_files": {
                    "references": result.linked_files.references,
                    "templates": result.linked_files.templates,
                    "scripts": result.linked_files.scripts,
                },
                "config_vars": result.config_vars,
                "path": result.path,
            }))
        }
        Some("create" | "edit" | "delete" | "generate") => {
            Ok(render_skills_usage_json(Some("managed")))
        }
        Some(args)
            if args.starts_with("create ")
                || args.starts_with("edit ")
                || args.starts_with("delete ")
                || args.starts_with("generate ") =>
        {
            Ok(render_skills_usage_json(Some("managed")))
        }
        Some(args) if is_help_arg(args) => Ok(render_skills_usage_json(None)),
        Some(args) => Ok(render_skills_usage_json(Some(args))),
    }
}

#[must_use]
pub fn classify_skills_slash_command(args: Option<&str>) -> SkillSlashDispatch {
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

/// Resolve a skill invocation by validating the skill exists on disk before
/// returning the dispatch.  When the skill is not found, returns `Err` with a
/// human-readable message that lists nearby skill names.
pub fn resolve_skill_invocation(
    cwd: &Path,
    args: Option<&str>,
) -> Result<SkillSlashDispatch, String> {
    let dispatch = classify_skills_slash_command(args);
    if let SkillSlashDispatch::Invoke(ref prompt) = dispatch {
        // Extract the skill name from the "$skill [args]" prompt.
        let skill_token = prompt
            .trim_start_matches('$')
            .split_whitespace()
            .next()
            .unwrap_or_default();
        if !skill_token.is_empty() {
            if let Err(error) = resolve_skill_path(cwd, skill_token) {
                let mut message = format!("Unknown skill: {skill_token} ({error})");
                if let Ok(available) = load_skills_from_registry(cwd) {
                    let names: Vec<String> = available
                        .iter()
                        .filter(|s| s.shadowed_by.is_none())
                        .map(|s| s.name.clone())
                        .collect();
                    if !names.is_empty() {
                        message.push_str("\n  Available skills: ");
                        message.push_str(&names.join(", "));
                    }
                }
                message.push_str(
                    "\n  Usage: /skills [list|view <name>|install <path>|help|<skill> [args]]",
                );
                return Err(message);
            }
        }
    }
    Ok(dispatch)
}

pub fn resolve_skill_path(cwd: &Path, skill: &str) -> std::io::Result<PathBuf> {
    SkillRegistry::discover(cwd)
        .resolve(skill)
        .map(|skill| skill.path)
}

fn discover_definition_roots(cwd: &Path, leaf: &str) -> Vec<(DefinitionSource, PathBuf)> {
    let mut roots = Vec::new();

    for ancestor in cwd.ancestors() {
        push_unique_root(
            &mut roots,
            DefinitionSource::ProjectClaw,
            ancestor.join(".cowd").join(leaf),
        );
        push_unique_root(
            &mut roots,
            DefinitionSource::ProjectCodex,
            ancestor.join(".codex").join(leaf),
        );
        // Migration: discover from .claude if directory exists
        push_unique_root(
            &mut roots,
            DefinitionSource::ProjectClaude,
            ancestor.join(".claude").join(leaf),
        );
    }

    if let Ok(cc_config_home) = env::var("COWD_CONFIG_HOME") {
        push_unique_root(
            &mut roots,
            DefinitionSource::UserClawConfigHome,
            PathBuf::from(cc_config_home).join(leaf),
        );
    }

    if let Ok(codex_home) = env::var("CODEX_HOME") {
        push_unique_root(
            &mut roots,
            DefinitionSource::UserCodexHome,
            PathBuf::from(codex_home).join(leaf),
        );
    }

    if let Some(home) = env::var_os("HOME") {
        let home = PathBuf::from(home);
        push_unique_root(
            &mut roots,
            DefinitionSource::UserClaw,
            home.join(".cowd").join(leaf),
        );
        push_unique_root(
            &mut roots,
            DefinitionSource::UserCodex,
            home.join(".codex").join(leaf),
        );
        // Migration: discover from .claude if directory exists
        push_unique_root(
            &mut roots,
            DefinitionSource::UserClaude,
            home.join(".claude").join(leaf),
        );
    }

    roots
}

fn install_skill(source: &str, cwd: &Path) -> std::io::Result<InstalledSkill> {
    let registry_root = default_skill_install_root()?;
    install_skill_into(source, cwd, &registry_root)
}

pub(crate) fn install_skill_into(
    source: &str,
    cwd: &Path,
    registry_root: &Path,
) -> std::io::Result<InstalledSkill> {
    let source = resolve_skill_install_source(source, cwd)?;
    let prompt_path = source.prompt_path();
    let contents = fs::read_to_string(prompt_path)?;
    let display_name = parse_skill_frontmatter(&contents).0;
    let invocation_name = derive_skill_install_name(&source, display_name.as_deref())?;
    let installed_path = registry_root.join(&invocation_name);

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
        invocation_name,
        display_name,
        source: source.report_path().to_path_buf(),
        registry_root: registry_root.to_path_buf(),
        installed_path,
    })
}

fn default_skill_install_root() -> std::io::Result<PathBuf> {
    if let Ok(cc_config_home) = env::var("COWD_CONFIG_HOME") {
        return Ok(PathBuf::from(cc_config_home).join("skills"));
    }
    if let Ok(codex_home) = env::var("CODEX_HOME") {
        return Ok(PathBuf::from(codex_home).join("skills"));
    }
    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".cowd").join("skills"));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "unable to resolve a skills install root; set CC_CONFIG_HOME or HOME",
    ))
}

fn resolve_skill_install_source(source: &str, cwd: &Path) -> std::io::Result<SkillInstallSource> {
    let candidate = PathBuf::from(source);
    let source = if candidate.is_absolute() {
        candidate
    } else {
        cwd.join(candidate)
    };
    let source = fs::canonicalize(&source)?;

    if source.is_dir() {
        let prompt_path = source.join("SKILL.md");
        if !prompt_path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "skill directory '{}' must contain SKILL.md",
                    source.display()
                ),
            ));
        }
        return Ok(SkillInstallSource::Directory {
            root: source,
            prompt_path,
        });
    }

    if source
        .extension()
        .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("md"))
    {
        return Ok(SkillInstallSource::MarkdownFile { path: source });
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "skill source '{}' must be a directory with SKILL.md or a markdown file",
            source.display()
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

fn sanitize_skill_invocation_name(candidate: &str) -> Option<String> {
    let trimmed = candidate
        .trim()
        .trim_start_matches('/')
        .trim_start_matches('$');
    if trimmed.is_empty() {
        return None;
    }

    let mut sanitized = String::new();
    let mut last_was_separator = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch.to_ascii_lowercase());
            last_was_separator = false;
        } else if (ch.is_whitespace() || matches!(ch, '/' | '\\'))
            && !last_was_separator
            && !sanitized.is_empty()
        {
            sanitized.push('-');
            last_was_separator = true;
        }
    }

    let sanitized = sanitized
        .trim_matches(|ch| matches!(ch, '-' | '_' | '.'))
        .to_string();
    (!sanitized.is_empty()).then_some(sanitized)
}

fn copy_directory_contents(source: &Path, destination: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let entry_type = entry.file_type()?;
        let destination_path = destination.join(entry.file_name());
        if entry_type.is_dir() {
            fs::create_dir_all(&destination_path)?;
            copy_directory_contents(&entry.path(), &destination_path)?;
        } else {
            fs::copy(entry.path(), destination_path)?;
        }
    }
    Ok(())
}

impl SkillInstallSource {
    fn prompt_path(&self) -> &Path {
        match self {
            Self::Directory { prompt_path, .. } => prompt_path,
            Self::MarkdownFile { path } => path,
        }
    }

    fn fallback_name(&self) -> Option<String> {
        match self {
            Self::Directory { root, .. } => root
                .file_name()
                .map(|name| name.to_string_lossy().to_string()),
            Self::MarkdownFile { path } => path
                .file_stem()
                .map(|name| name.to_string_lossy().to_string()),
        }
    }

    fn report_path(&self) -> &Path {
        match self {
            Self::Directory { root, .. } => root,
            Self::MarkdownFile { path } => path,
        }
    }
}

fn push_unique_root(
    roots: &mut Vec<(DefinitionSource, PathBuf)>,
    source: DefinitionSource,
    path: PathBuf,
) {
    if path.is_dir() && !roots.iter().any(|(_, existing)| existing == &path) {
        roots.push((source, path));
    }
}

pub(crate) fn load_agents_from_roots(
    roots: &[(DefinitionSource, PathBuf)],
) -> std::io::Result<Vec<AgentSummary>> {
    let mut agents = Vec::new();
    let mut active_sources = BTreeMap::<String, DefinitionSource>::new();

    for (source, root) in roots {
        let mut root_agents = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if entry.path().extension().is_none_or(|ext| ext != "toml") {
                continue;
            }
            let contents = fs::read_to_string(entry.path())?;
            let fallback_name = entry.path().file_stem().map_or_else(
                || entry.file_name().to_string_lossy().to_string(),
                |stem| stem.to_string_lossy().to_string(),
            );
            root_agents.push(AgentSummary {
                name: parse_toml_string(&contents, "name").unwrap_or(fallback_name),
                description: parse_toml_string(&contents, "description"),
                model: parse_toml_string(&contents, "model"),
                reasoning_effort: parse_toml_string(&contents, "model_reasoning_effort"),
                source: *source,
                shadowed_by: None,
            });
        }
        root_agents.sort_by(|left, right| left.name.cmp(&right.name));

        for mut agent in root_agents {
            let key = agent.name.to_ascii_lowercase();
            if let Some(existing) = active_sources.get(&key) {
                agent.shadowed_by = Some(*existing);
            } else {
                active_sources.insert(key, agent.source);
            }
            agents.push(agent);
        }
    }

    Ok(agents)
}

#[cfg(test)]
pub(crate) fn load_skills_from_roots(roots: &[SkillRoot]) -> std::io::Result<Vec<SkillSummary>> {
    let mut skills = Vec::new();
    let mut active_sources = BTreeMap::<String, DefinitionSource>::new();

    for root in roots {
        let mut root_skills = Vec::new();
        for entry in fs::read_dir(&root.path)? {
            let entry = entry?;
            match root.origin {
                SkillOrigin::SkillsDir => {
                    if !entry.path().is_dir() {
                        continue;
                    }
                    let skill_path = entry.path().join("SKILL.md");
                    if !skill_path.is_file() {
                        continue;
                    }
                    let contents = fs::read_to_string(skill_path)?;
                    let (name, description) = parse_skill_frontmatter(&contents);
                    root_skills.push(SkillSummary {
                        name: name
                            .unwrap_or_else(|| entry.file_name().to_string_lossy().to_string()),
                        description,
                        source: root.source,
                        shadowed_by: None,
                        origin: root.origin,
                    });
                }
                SkillOrigin::LegacyCommandsDir => {
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

                    let contents = fs::read_to_string(&markdown_path)?;
                    let fallback_name = markdown_path.file_stem().map_or_else(
                        || entry.file_name().to_string_lossy().to_string(),
                        |stem| stem.to_string_lossy().to_string(),
                    );
                    let (name, description) = parse_skill_frontmatter(&contents);
                    root_skills.push(SkillSummary {
                        name: name.unwrap_or(fallback_name),
                        description,
                        source: root.source,
                        shadowed_by: None,
                        origin: root.origin,
                    });
                }
            }
        }
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

pub(crate) fn load_skills_from_registry(cwd: &Path) -> std::io::Result<Vec<SkillSummary>> {
    SkillRegistry::discover(cwd)
        .list()
        .map(|skills| skills.into_iter().map(skill_summary_from_info).collect())
}

fn skill_summary_from_info(skill: SkillInfo) -> SkillSummary {
    SkillSummary {
        name: skill.name,
        description: skill.description,
        source: definition_source_from_skill_source(skill.source),
        shadowed_by: skill.shadowed_by.map(definition_source_from_skill_source),
        origin: skill_origin_from_registry_kind(skill.kind),
    }
}

fn definition_source_from_skill_source(source: SkillRegistrySource) -> DefinitionSource {
    match source {
        SkillRegistrySource::ProjectCowd | SkillRegistrySource::ProjectAgents => {
            DefinitionSource::ProjectClaw
        }
        SkillRegistrySource::ProjectCodex => DefinitionSource::ProjectCodex,
        SkillRegistrySource::ProjectClaude => DefinitionSource::ProjectClaude,
        SkillRegistrySource::UserCowdConfigHome => DefinitionSource::UserClawConfigHome,
        SkillRegistrySource::UserCodexHome => DefinitionSource::UserCodexHome,
        SkillRegistrySource::UserCowd | SkillRegistrySource::UserAgents => {
            DefinitionSource::UserClaw
        }
        SkillRegistrySource::UserCodex | SkillRegistrySource::UserOpenCode => {
            DefinitionSource::UserCodex
        }
        SkillRegistrySource::UserClaude => DefinitionSource::UserClaude,
    }
}

fn skill_origin_from_registry_kind(kind: SkillRegistryRootKind) -> SkillOrigin {
    match kind {
        SkillRegistryRootKind::SkillsDir => SkillOrigin::SkillsDir,
        SkillRegistryRootKind::LegacyCommandsDir => SkillOrigin::LegacyCommandsDir,
    }
}

fn parse_toml_string(contents: &str, key: &str) -> Option<String> {
    let prefix = format!("{key} =");
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            continue;
        }
        let Some(value) = trimmed.strip_prefix(&prefix) else {
            continue;
        };
        let value = value.trim();
        let Some(value) = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        else {
            continue;
        };
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    None
}

pub(crate) fn parse_skill_frontmatter(contents: &str) -> (Option<String>, Option<String>) {
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
        .trim()
        .to_string()
}

pub(crate) fn render_agents_report(agents: &[AgentSummary]) -> String {
    if agents.is_empty() {
        return "No agents found.".to_string();
    }

    let total_active = agents
        .iter()
        .filter(|agent| agent.shadowed_by.is_none())
        .count();
    let mut lines = vec![
        "Agents".to_string(),
        format!("  {total_active} active agents"),
        String::new(),
    ];

    for scope in [
        DefinitionScope::Project,
        DefinitionScope::UserConfigHome,
        DefinitionScope::UserHome,
    ] {
        let group = agents
            .iter()
            .filter(|agent| agent.source.report_scope() == scope)
            .collect::<Vec<_>>();
        if group.is_empty() {
            continue;
        }

        lines.push(format!("{}:", scope.label()));
        for agent in group {
            let detail = agent_detail(agent);
            match agent.shadowed_by {
                Some(winner) => lines.push(format!("  (shadowed by {}) {detail}", winner.label())),
                None => lines.push(format!("  {detail}")),
            }
        }
        lines.push(String::new());
    }

    lines.join("\n").trim_end().to_string()
}

pub(crate) fn render_agents_report_json(cwd: &Path, agents: &[AgentSummary]) -> Value {
    let active = agents
        .iter()
        .filter(|agent| agent.shadowed_by.is_none())
        .count();
    json!({
        "kind": "agents",
        "action": "list",
        "working_directory": cwd.display().to_string(),
        "count": agents.len(),
        "summary": {
            "total": agents.len(),
            "active": active,
            "shadowed": agents.len().saturating_sub(active),
        },
        "agents": agents.iter().map(agent_summary_json).collect::<Vec<_>>(),
    })
}

fn discover_agents_for_task(agents: &[AgentSummary], task: &str) -> Vec<StaticAgentMatch> {
    let task_terms = normalized_terms(task);
    let mut matches = agents
        .iter()
        .filter(|agent| agent.shadowed_by.is_none())
        .filter_map(|agent| {
            let haystack = [
                agent.name.as_str(),
                agent.description.as_deref().unwrap_or_default(),
                agent.model.as_deref().unwrap_or_default(),
                agent.reasoning_effort.as_deref().unwrap_or_default(),
            ]
            .join(" ")
            .to_ascii_lowercase();
            let mut match_terms = task_terms
                .iter()
                .filter(|term| haystack.contains(term.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if match_terms.is_empty() {
                let name = agent.name.to_ascii_lowercase();
                match_terms = normalized_terms(&name)
                    .into_iter()
                    .filter(|term| task_terms.iter().any(|task_term| term.contains(task_term)))
                    .collect();
            }
            if match_terms.is_empty() {
                return None;
            }
            let score = match_terms.len();
            Some(StaticAgentMatch {
                name: agent.name.clone(),
                description: agent.description.clone(),
                source: agent.source,
                shadowed_by: agent.shadowed_by,
                match_terms,
                score,
            })
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.name.cmp(&right.name))
    });
    matches
}

fn assemble_static_agent_team(matches: &[StaticAgentMatch]) -> Option<StaticAgentTeam> {
    let leader = matches.first()?.clone();
    let workers = matches.iter().skip(1).take(4).cloned().collect();
    Some(StaticAgentTeam { leader, workers })
}

fn normalized_terms(value: &str) -> Vec<String> {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|term| term.len() >= 3)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn agent_detail(agent: &AgentSummary) -> String {
    let mut parts = vec![agent.name.clone()];
    if let Some(description) = &agent.description {
        parts.push(description.clone());
    }
    if let Some(model) = &agent.model {
        parts.push(model.clone());
    }
    if let Some(reasoning) = &agent.reasoning_effort {
        parts.push(reasoning.clone());
    }
    parts.join(" · ")
}

pub(crate) fn render_skills_report(skills: &[SkillSummary]) -> String {
    if skills.is_empty() {
        return "No skills found.".to_string();
    }

    let total_active = skills
        .iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .count();
    let mut lines = vec![
        "Skills".to_string(),
        format!("  {total_active} available skills"),
        String::new(),
    ];

    for scope in [
        DefinitionScope::Project,
        DefinitionScope::UserConfigHome,
        DefinitionScope::UserHome,
    ] {
        let group = skills
            .iter()
            .filter(|skill| skill.source.report_scope() == scope)
            .collect::<Vec<_>>();
        if group.is_empty() {
            continue;
        }

        lines.push(format!("{}:", scope.label()));
        for skill in group {
            let mut parts = vec![skill.name.clone()];
            if let Some(description) = &skill.description {
                parts.push(description.clone());
            }
            if let Some(detail) = skill.origin.detail_label() {
                parts.push(detail.to_string());
            }
            let detail = parts.join(" · ");
            match skill.shadowed_by {
                Some(winner) => lines.push(format!("  (shadowed by {}) {detail}", winner.label())),
                None => lines.push(format!("  {detail}")),
            }
        }
        lines.push(String::new());
    }

    lines.join("\n").trim_end().to_string()
}

pub(crate) fn render_skills_report_json(skills: &[SkillSummary]) -> Value {
    let active = skills
        .iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .count();
    json!({
        "kind": "skills",
        "action": "list",
        "summary": {
            "total": skills.len(),
            "active": active,
            "shadowed": skills.len().saturating_sub(active),
        },
        "skills": skills.iter().map(skill_summary_json).collect::<Vec<_>>(),
    })
}

pub(crate) fn render_skill_install_report(skill: &InstalledSkill) -> String {
    let mut lines = vec![
        "Skills".to_string(),
        format!("  Result           installed {}", skill.invocation_name),
        format!("  Invoke as        ${}", skill.invocation_name),
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

pub(crate) fn render_skill_install_report_json(skill: &InstalledSkill) -> Value {
    json!({
        "kind": "skills",
        "action": "install",
        "result": "installed",
        "invocation_name": &skill.invocation_name,
        "invoke_as": format!("${}", skill.invocation_name),
        "display_name": &skill.display_name,
        "source": skill.source.display().to_string(),
        "registry_root": skill.registry_root.display().to_string(),
        "installed_path": skill.installed_path.display().to_string(),
    })
}

// Render skill view report
fn render_skill_view_report(result: &SkillViewOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Name             {}", result.name));
        lines.push(format!("  Description      {}", result.description));
        if !result.tags.is_empty() {
            lines.push(format!("  Tags             {}", result.tags.join(", ")));
        }
        if result.setup_needed {
            lines.push(format!("  Status           setup_needed"));
        } else {
            lines.push(format!("  Status           ready"));
        }
        lines.push(String::new());
        lines.push("---".to_string());
        lines.push(String::new());
        // Show content preview
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

        let has_linked = !result.linked_files.references.is_empty()
            || !result.linked_files.templates.is_empty()
            || !result.linked_files.scripts.is_empty();
        if has_linked {
            lines.push(String::new());
            lines.push("Linked files:".to_string());
            for file in &result.linked_files.references {
                lines.push(format!("  - [ref] {}", file));
            }
            for file in &result.linked_files.templates {
                lines.push(format!("  - [tmpl] {}", file));
            }
            for file in &result.linked_files.scripts {
                lines.push(format!("  - [script] {}", file));
            }
        }
    } else {
        lines.push(format!("  Result           not found"));
    }
    lines.join("\n")
}

// Render skills usage help
fn render_skills_usage(topic: Option<&str>) -> String {
    match topic {
        Some("create" | "edit" | "delete" | "generate" | "managed") => {
            r#"Skills - Managed In WebUI/TUI

The CLI intentionally exposes only list, view, install, and invocation.
Use WebUI or TUI for create, edit, delete, generate, validation, run queues,
governance review, and stateful skill management."#
                .to_string()
        }
        Some("view") => r#"Skills - View

Usage: /skill view <name>

View skill metadata and content.

Example:
  /skill view git-essentials
  /skill view my-skill --file references/api.md"#
            .to_string(),
        Some("install") => r#"Skills - Install

Usage: /skill install <source>

Install a skill from a remote source.

Example:
  /skill install github:user/repo
  /skill install /path/to/skill"#
            .to_string(),
        _ => {
            let mut lines = vec![
                "Skills".to_string(),
                "  Usage            /skills [list|view <name>|install <path>|help|<skill> [args]]".to_string(),
                "  Alias            /skill".to_string(),
                "  Direct CLI       cowd skill [list|view <name>|install <path>|help]".to_string(),
                "  Local controls   list, view <name>, install <path>".to_string(),
                "  Managed in UI    create, edit, delete, generate, validate, run, governance".to_string(),
                "  Invoke           /skills help overview -> $help overview".to_string(),
                "  Install root     $COWD_CONFIG_HOME/skills or ~/.cowd/skills".to_string(),
                "  Sources          .cowd/skills, .agents/skills, .codex/skills, ~/.cowd/skills, ~/.cowd/skills/omc-learned, ~/.codex/skills, legacy /commands".to_string(),
            ];
            if let Some(args) = topic {
                // Should not happen for None branch, but keeps the pattern
                lines.push(format!("  Unexpected       {args}"));
            }
            lines.join("\n")
        }
    }
}

fn render_skills_usage_json(topic: Option<&str>) -> Value {
    match topic {
        Some("create" | "edit" | "delete" | "generate" | "managed") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "managed",
            "usage": "CLI supports only list, view, install, and invocation. Use WebUI or TUI for create, edit, delete, generate, validation, run queues, governance review, and stateful skill management.",
        }),
        Some("view") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "view",
            "usage": r#"Usage: /skill view <name>

View skill metadata and content.

Example:
  /skill view git-essentials
  /skill view my-skill --file references/api.md"#,
        }),
        Some("install") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "install",
            "usage": r#"Usage: /skill install <source>

Install a skill from a remote source.

Example:
  /skill install github:user/repo
  /skill install /path/to/skill"#,
        }),
        _ => json!({
            "kind": "skills",
            "action": "help",
            "usage": {
                "slash_command": "/skills [list|view <name>|install <path>|help|<skill> [args]]",
                "aliases": ["/skill"],
                "direct_cli": "cowd skill [list|view <name>|install <path>|help]",
                "local_controls": ["list", "view <name>", "install <path>"],
                "managed_in_ui": ["create", "edit", "delete", "generate", "validate", "run", "governance"],
                "invoke": "/skills help overview -> $help overview",
                "install_root": "$CC_CONFIG_HOME/skills or ~/.cowd/skills",
                "sources": [
                    ".cowd/skills",
                    ".agents/skills",
                    ".codex/skills",
                    "~/.cowd/skills",
                    "~/.cowd/skills/omc-learned",
                    "~/.codex/skills",
                    "legacy /commands",
                    "legacy fallback dirs still load automatically",
                ],
            },
        }),
    }
}

fn normalize_optional_args(args: Option<&str>) -> Option<&str> {
    args.map(str::trim).filter(|value| !value.is_empty())
}

fn is_help_arg(arg: &str) -> bool {
    matches!(arg, "help" | "-h" | "--help")
}

fn help_path_from_args(args: &str) -> Option<Vec<&str>> {
    let parts = args.split_whitespace().collect::<Vec<_>>();
    let help_index = parts.iter().position(|part| is_help_arg(part))?;
    Some(parts[..help_index].to_vec())
}

fn render_agents_usage(unexpected: Option<&str>) -> String {
    let mut lines = vec![
        "Agents".to_string(),
        "  Usage            /agents [list|discover <task>|help]".to_string(),
        "  Direct CLI       cowd agents".to_string(),
        "  Sources          .cowd/agents, ~/.cowd/agents, $CC_CONFIG_HOME/agents".to_string(),
    ];
    if let Some(args) = unexpected {
        lines.push(format!("  Unexpected       {args}"));
    }
    lines.join("\n")
}

fn render_agents_usage_json(unexpected: Option<&str>) -> Value {
    json!({
        "kind": "agents",
        "action": "help",
        "usage": {
            "slash_command": "/agents [list|discover <task>|help]",
            "direct_cli": "cowd agents [list|discover <task>|help]",
            "sources": [".cowd/agents", "~/.cowd/agents", "$CC_CONFIG_HOME/agents"],
        },
        "unexpected": unexpected,
    })
}

fn definition_source_id(source: DefinitionSource) -> &'static str {
    match source {
        DefinitionSource::ProjectClaw
        | DefinitionSource::ProjectCodex
        | DefinitionSource::ProjectClaude => "project_cowd",
        DefinitionSource::UserClawConfigHome | DefinitionSource::UserCodexHome => {
            "user_cowd_config_home"
        }
        DefinitionSource::UserClaw | DefinitionSource::UserCodex | DefinitionSource::UserClaude => {
            "user_cowd"
        }
    }
}

fn definition_source_json(source: DefinitionSource) -> Value {
    json!({
        "id": definition_source_id(source),
        "label": source.label(),
    })
}

fn agent_summary_json(agent: &AgentSummary) -> Value {
    json!({
        "name": &agent.name,
        "description": &agent.description,
        "model": &agent.model,
        "reasoning_effort": &agent.reasoning_effort,
        "source": definition_source_json(agent.source),
        "active": agent.shadowed_by.is_none(),
        "shadowed_by": agent.shadowed_by.map(definition_source_json),
    })
}

fn skill_origin_id(origin: SkillOrigin) -> &'static str {
    match origin {
        SkillOrigin::SkillsDir => "skills_dir",
        SkillOrigin::LegacyCommandsDir => "legacy_commands_dir",
    }
}

fn skill_origin_json(origin: SkillOrigin) -> Value {
    json!({
        "id": skill_origin_id(origin),
        "detail_label": origin.detail_label(),
    })
}

fn skill_summary_json(skill: &SkillSummary) -> Value {
    json!({
        "name": &skill.name,
        "description": &skill.description,
        "source": definition_source_json(skill.source),
        "origin": skill_origin_json(skill.origin),
        "active": skill.shadowed_by.is_none(),
        "shadowed_by": skill.shadowed_by.map(definition_source_json),
    })
}
