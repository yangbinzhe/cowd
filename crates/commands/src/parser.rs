use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use plugins::{PluginError, PluginLoadFailure, PluginManager, PluginSummary};
use runtime::{
    ConfigLoader, ConfigSource, McpOAuthConfig, McpServerConfig, ScopedMcpServerConfig, Session,
};
use serde_json::{json, Value};

use crate::skill_tools::{
    SkillCreateInput, SkillCreateOutput, SkillDeleteInput, SkillDeleteOutput, SkillEditInput,
    SkillEditOutput, SkillGenerateInput, SkillGenerateOutput, SkillManager, SkillViewInput,
    SkillViewOutput,
};
use crate::specs::{
    SkillSlashDispatch, SlashCommand, SlashCommandParseError, SlashCommandSpec, SLASH_COMMAND_SPECS,
};
impl SlashCommand {
    pub fn parse(input: &str) -> Result<Option<Self>, SlashCommandParseError> {
        validate_slash_command_input(input)
    }

    /// Returns the canonical slash-command name (e.g. `"/branch"`) for use in
    /// error messages and logging. Derived from the spec table so it always
    /// matches what the user would have typed.
    #[must_use]
    pub fn slash_name(&self) -> &'static str {
        match self {
            Self::Help => "/help",
            Self::Clear { .. } => "/clear",
            Self::Compact { .. } => "/compact",
            Self::Cost => "/cost",
            Self::Doctor => "/doctor",
            Self::Config { .. } => "/config",
            Self::Memory { .. } => "/memory",
            Self::History { .. } => "/history",
            Self::Diff => "/diff",
            Self::Status => "/status",
            Self::Stats => "/stats",
            Self::Version => "/version",
            Self::Commit { .. } => "/commit",
            Self::Pr { .. } => "/pr",
            Self::Issue { .. } => "/issue",
            Self::Init => "/init",
            Self::Bughunter { .. } => "/bughunter",
            Self::Ultraplan { .. } => "/ultraplan",
            Self::Teleport { .. } => "/teleport",
            Self::DebugToolCall { .. } => "/debug-tool-call",
            Self::Resume { .. } => "/resume",
            Self::Model { .. } => "/model",
            Self::Permissions { .. } => "/permissions",
            Self::Session { .. } => "/session",
            Self::Plugins { .. } => "/plugins",
            Self::Login => "/login",
            Self::Logout => "/logout",
            Self::Vim => "/vim",
            Self::Upgrade => "/upgrade",
            Self::Share => "/share",
            Self::Feedback => "/feedback",
            Self::Files => "/files",
            Self::Fast => "/fast",
            Self::Exit => "/exit",
            Self::Summary => "/summary",
            Self::Desktop => "/desktop",
            Self::Brief => "/brief",
            Self::Advisor => "/advisor",
            Self::Stickers => "/stickers",
            Self::Insights => "/insights",
            Self::Thinkback => "/thinkback",
            Self::ReleaseNotes => "/release-notes",
            Self::SecurityReview => "/security-review",
            Self::Keybindings => "/keybindings",
            Self::PrivacySettings => "/privacy-settings",
            Self::Plan { .. } => "/plan",
            Self::Review { .. } => "/review",
            Self::Tasks { .. } => "/tasks",
            Self::Theme { .. } => "/theme",
            Self::Voice { .. } => "/voice",
            Self::Usage { .. } => "/usage",
            Self::Rename { .. } => "/rename",
            Self::Copy { .. } => "/copy",
            Self::Hooks { .. } => "/hooks",
            Self::Context { .. } => "/context",
            Self::Color { .. } => "/color",
            Self::Effort { .. } => "/effort",
            Self::Branch { .. } => "/branch",
            Self::Rewind { .. } => "/rewind",
            Self::Ide { .. } => "/ide",
            Self::Tag { .. } => "/tag",
            Self::OutputStyle { .. } => "/output-style",
            Self::AddDir { .. } => "/add-dir",
            Self::Sandbox => "/sandbox",
            Self::Mcp { .. } => "/mcp",
            Self::Export { .. } => "/export",
            Self::Handoff { .. } => "/handoff",
            Self::Closet { .. } => "/closet",
            Self::SandboxSearch { .. } => "/sandbox-search",
            Self::Retry => "/retry",
            Self::Undo => "/undo",
            Self::NewSession => "/new",
            Self::Title { .. } => "/title",
            Self::Compress => "/compress",
            Self::State => "/state",
            Self::SubAgent { .. } => "/subagent",
            Self::Pipeline { .. } => "/pipeline",
            Self::Solve { .. } => "/solve",
            Self::Agents { .. } => "/agent",
            Self::AgentProfile { .. } => "/agent",
            #[allow(unreachable_patterns)]
            _ => "/unknown",
        }
    }
}

#[allow(clippy::too_many_lines)]
pub fn validate_slash_command_input(
    input: &str,
) -> Result<Option<SlashCommand>, SlashCommandParseError> {
    let trimmed = input.trim();
    if !trimmed.starts_with('/') {
        return Ok(None);
    }

    let mut parts = trimmed.trim_start_matches('/').split_whitespace();
    let command = parts.next().unwrap_or_default();
    if command.is_empty() {
        return Err(SlashCommandParseError::new(
            "Slash command name is missing. Use /help to list available slash commands.",
        ));
    }

    let args = parts.collect::<Vec<_>>();
    let remainder = remainder_after_command(trimmed, command);

    Ok(Some(match command {
        "help" => {
            validate_no_args(command, &args)?;
            SlashCommand::Help
        }
        "status" => {
            validate_no_args(command, &args)?;
            SlashCommand::Status
        }
        "sandbox" => {
            validate_no_args(command, &args)?;
            SlashCommand::Sandbox
        }
        "compact" => {
            validate_no_args(command, &args)?;
            SlashCommand::Compact
        }
        "bughunter" => SlashCommand::Bughunter { scope: remainder },
        "commit" => {
            validate_no_args(command, &args)?;
            SlashCommand::Commit
        }
        "pr" => SlashCommand::Pr { context: remainder },
        "issue" => SlashCommand::Issue { context: remainder },
        "ultraplan" => SlashCommand::Ultraplan { task: remainder },
        "teleport" => SlashCommand::Teleport {
            target: Some(require_remainder(command, remainder, "<symbol-or-path>")?),
        },
        "debug-tool-call" => {
            validate_no_args(command, &args)?;
            SlashCommand::DebugToolCall
        }
        "model" => SlashCommand::Model {
            model: optional_single_arg(command, &args, "[model]")?,
        },
        "permissions" => SlashCommand::Permissions {
            mode: parse_permissions_mode(&args)?,
        },
        "clear" => SlashCommand::Clear {
            confirm: parse_clear_args(&args)?,
        },
        "cost" => {
            validate_no_args(command, &args)?;
            SlashCommand::Cost
        }
        "resume" => SlashCommand::Resume {
            session_path: Some(require_remainder(
                command,
                remainder,
                "<session-id|latest>",
            )?),
        },
        "config" => SlashCommand::Config {
            section: parse_config_section(&args)?,
        },
        "mcp" => parse_mcp_command(&args)?,
        "memory" => {
            validate_no_args(command, &args)?;
            SlashCommand::Memory
        }
        "init" => {
            validate_no_args(command, &args)?;
            SlashCommand::Init
        }
        "diff" => {
            validate_no_args(command, &args)?;
            SlashCommand::Diff
        }
        "version" => {
            validate_no_args(command, &args)?;
            SlashCommand::Version
        }
        "export" => SlashCommand::Export { path: remainder },
        "session" => parse_session_command(&args)?,
        "plugin" | "plugins" | "marketplace" => parse_plugin_command(&args)?,
        "agents" => SlashCommand::Agents {
            args: parse_list_or_help_args(command, remainder)?,
        },
        "agent" => parse_agent_command(remainder.as_deref())?,
        "skills" | "skill" => SlashCommand::Skills {
            args: parse_skills_args(remainder.as_deref())?,
        },
        "doctor" | "providers" => {
            validate_no_args(command, &args)?;
            SlashCommand::Doctor
        }
        "login" | "logout" => {
            return Err(command_error(
                "This auth flow was removed. Set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN instead.",
                command,
                "",
            ));
        }
        "vim" => {
            validate_no_args(command, &args)?;
            SlashCommand::Vim
        }
        "upgrade" => {
            validate_no_args(command, &args)?;
            SlashCommand::Upgrade
        }
        "stats" | "tokens" | "cache" => {
            validate_no_args(command, &args)?;
            SlashCommand::Stats
        }
        "share" => {
            validate_no_args(command, &args)?;
            SlashCommand::Share
        }
        "feedback" => {
            validate_no_args(command, &args)?;
            SlashCommand::Feedback
        }
        "files" => {
            validate_no_args(command, &args)?;
            SlashCommand::Files
        }
        "fast" => {
            validate_no_args(command, &args)?;
            SlashCommand::Fast
        }
        "exit" => {
            validate_no_args(command, &args)?;
            SlashCommand::Exit
        }
        "summary" => {
            validate_no_args(command, &args)?;
            SlashCommand::Summary
        }
        "desktop" => {
            validate_no_args(command, &args)?;
            SlashCommand::Desktop
        }
        "brief" => {
            validate_no_args(command, &args)?;
            SlashCommand::Brief
        }
        "advisor" => {
            validate_no_args(command, &args)?;
            SlashCommand::Advisor
        }
        "stickers" => {
            validate_no_args(command, &args)?;
            SlashCommand::Stickers
        }
        "insights" => {
            validate_no_args(command, &args)?;
            SlashCommand::Insights
        }
        "thinkback" => {
            validate_no_args(command, &args)?;
            SlashCommand::Thinkback
        }
        "release-notes" => {
            validate_no_args(command, &args)?;
            SlashCommand::ReleaseNotes
        }
        "security-review" => {
            validate_no_args(command, &args)?;
            SlashCommand::SecurityReview
        }
        "keybindings" => {
            validate_no_args(command, &args)?;
            SlashCommand::Keybindings
        }
        "privacy-settings" => {
            validate_no_args(command, &args)?;
            SlashCommand::PrivacySettings
        }
        "plan" => SlashCommand::Plan { mode: remainder },
        "review" => SlashCommand::Review { scope: remainder },
        "tasks" => SlashCommand::Tasks { args: remainder },
        "theme" => SlashCommand::Theme { name: remainder },
        "voice" => SlashCommand::Voice { mode: remainder },
        "usage" => SlashCommand::Usage { scope: remainder },
        "rename" => SlashCommand::Rename { name: remainder },
        "copy" => SlashCommand::Copy { target: remainder },
        "hooks" => SlashCommand::Hooks { args: remainder },
        "context" => SlashCommand::Context { action: remainder },
        "color" => SlashCommand::Color { scheme: remainder },
        "effort" => SlashCommand::Effort { level: remainder },
        "branch" => SlashCommand::Branch { name: remainder },
        "rewind" => SlashCommand::Rewind { steps: remainder },
        "ide" => SlashCommand::Ide { target: remainder },
        "tag" => SlashCommand::Tag { label: remainder },
        "output-style" => SlashCommand::OutputStyle { style: remainder },
        "add-dir" => SlashCommand::AddDir { path: remainder },
        "history" => SlashCommand::History {
            count: optional_single_arg(command, &args, "[count]")?,
        },
        "handoff" | "transfer" | "handover" => {
            let action = args.first().map(|s| s.to_string());
            let session_id = args.get(1).map(|s| s.to_string());
            SlashCommand::Handoff { action, session_id }
        }
        "subagent" => {
            let role = args.first().map(|s| s.to_string());
            let task = args.get(1..).map(|s| s.join(" ")).filter(|t| !t.is_empty());
            SlashCommand::SubAgent { role, task }
        }
        "closet" | "rooms" | "memory-rooms" => SlashCommand::Closet {
            topic: args.first().map(|s| s.to_string()),
        },
        "sandbox-search" => SlashCommand::SandboxSearch {
            query: args.first().map(|s| s.to_string()),
        },
        "retry" => SlashCommand::Retry,
        "undo" => SlashCommand::Undo,
        "new" | "reset" => SlashCommand::NewSession,
        "title" => SlashCommand::Title {
            name: args.first().map(|s| s.to_string()),
        },
        "compress" => SlashCommand::Compress,
        "state" => SlashCommand::State,
        "pipeline" => SlashCommand::Pipeline {
            task: args.first().map(|s| s.to_string()),
        },
        "solve" => SlashCommand::Solve {
            problem: args.first().map(|s| s.to_string()),
        },
        other => SlashCommand::Unknown(other.to_string()),
    }))
}
fn validate_no_args(command: &str, args: &[&str]) -> Result<(), SlashCommandParseError> {
    if args.is_empty() {
        return Ok(());
    }

    Err(command_error(
        &format!("Unexpected arguments for /{command}."),
        command,
        &format!("/{command}"),
    ))
}

fn optional_single_arg(
    command: &str,
    args: &[&str],
    argument_hint: &str,
) -> Result<Option<String>, SlashCommandParseError> {
    match args {
        [] => Ok(None),
        [value] => Ok(Some((*value).to_string())),
        _ => Err(usage_error(command, argument_hint)),
    }
}

fn require_remainder(
    command: &str,
    remainder: Option<String>,
    argument_hint: &str,
) -> Result<String, SlashCommandParseError> {
    remainder.ok_or_else(|| usage_error(command, argument_hint))
}

fn parse_permissions_mode(args: &[&str]) -> Result<Option<String>, SlashCommandParseError> {
    let mode = optional_single_arg(
        "permissions",
        args,
        "[read-only|workspace-write|danger-full-access]",
    )?;
    if let Some(mode) = mode {
        if matches!(
            mode.as_str(),
            "read-only" | "workspace-write" | "danger-full-access"
        ) {
            return Ok(Some(mode));
        }
        return Err(command_error(
            &format!(
                "Unsupported /permissions mode '{mode}'. Use read-only, workspace-write, or danger-full-access."
            ),
            "permissions",
            "/permissions [read-only|workspace-write|danger-full-access]",
        ));
    }

    Ok(None)
}

fn parse_clear_args(args: &[&str]) -> Result<bool, SlashCommandParseError> {
    match args {
        [] => Ok(false),
        ["--confirm"] => Ok(true),
        [unexpected] => Err(command_error(
            &format!("Unsupported /clear argument '{unexpected}'. Use /clear or /clear --confirm."),
            "clear",
            "/clear [--confirm]",
        )),
        _ => Err(usage_error("clear", "[--confirm]")),
    }
}

fn parse_config_section(args: &[&str]) -> Result<Option<String>, SlashCommandParseError> {
    let section = optional_single_arg("config", args, "[env|hooks|model|plugins]")?;
    if let Some(section) = section {
        if matches!(section.as_str(), "env" | "hooks" | "model" | "plugins") {
            return Ok(Some(section));
        }
        return Err(command_error(
            &format!("Unsupported /config section '{section}'. Use env, hooks, model, or plugins."),
            "config",
            "/config [env|hooks|model|plugins]",
        ));
    }

    Ok(None)
}

fn parse_session_command(args: &[&str]) -> Result<SlashCommand, SlashCommandParseError> {
    match args {
        [] => Ok(SlashCommand::Session {
            action: None,
            target: None,
        }),
        ["list"] => Ok(SlashCommand::Session {
            action: Some("list".to_string()),
            target: None,
        }),
        ["list", ..] => Err(usage_error("session", "[list|switch <session-id>|fork [branch-name]|delete <session-id> [--force]]")),
        ["switch"] => Err(usage_error("session switch", "<session-id>")),
        ["switch", target] => Ok(SlashCommand::Session {
            action: Some("switch".to_string()),
            target: Some((*target).to_string()),
        }),
        ["switch", ..] => Err(command_error(
            "Unexpected arguments for /session switch.",
            "session",
            "/session switch <session-id>",
        )),
        ["fork"] => Ok(SlashCommand::Session {
            action: Some("fork".to_string()),
            target: None,
        }),
        ["fork", target] => Ok(SlashCommand::Session {
            action: Some("fork".to_string()),
            target: Some((*target).to_string()),
        }),
        ["fork", ..] => Err(command_error(
            "Unexpected arguments for /session fork.",
            "session",
            "/session fork [branch-name]",
        )),
        ["delete"] => Err(usage_error("session delete", "<session-id> [--force]")),
        ["delete", target] => Ok(SlashCommand::Session {
            action: Some("delete".to_string()),
            target: Some((*target).to_string()),
        }),
        ["delete", target, "--force"] => Ok(SlashCommand::Session {
            action: Some("delete-force".to_string()),
            target: Some((*target).to_string()),
        }),
        ["delete", _target, unexpected] => Err(command_error(
            &format!(
                "Unsupported /session delete flag '{unexpected}'. Use --force to skip confirmation."
            ),
            "session",
            "/session delete <session-id> [--force]",
        )),
        ["delete", ..] => Err(command_error(
            "Unexpected arguments for /session delete.",
            "session",
            "/session delete <session-id> [--force]",
        )),
        [action, ..] => Err(command_error(
            &format!(
                "Unknown /session action '{action}'. Use list, switch <session-id>, fork [branch-name], or delete <session-id> [--force]."
            ),
            "session",
            "/session [list|switch <session-id>|fork [branch-name]|delete <session-id> [--force]]",
        )),
    }
}

fn parse_mcp_command(args: &[&str]) -> Result<SlashCommand, SlashCommandParseError> {
    match args {
        [] => Ok(SlashCommand::Mcp {
            action: None,
            target: None,
        }),
        ["list"] => Ok(SlashCommand::Mcp {
            action: Some("list".to_string()),
            target: None,
        }),
        ["list", ..] => Err(usage_error("mcp list", "")),
        ["show"] => Err(usage_error("mcp show", "<server>")),
        ["show", target] => Ok(SlashCommand::Mcp {
            action: Some("show".to_string()),
            target: Some((*target).to_string()),
        }),
        ["show", ..] => Err(command_error(
            "Unexpected arguments for /mcp show.",
            "mcp",
            "/mcp show <server>",
        )),
        ["help" | "-h" | "--help"] => Ok(SlashCommand::Mcp {
            action: Some("help".to_string()),
            target: None,
        }),
        [action, ..] => Err(command_error(
            &format!("Unknown /mcp action '{action}'. Use list, show <server>, or help."),
            "mcp",
            "/mcp [list|show <server>|help]",
        )),
    }
}

fn parse_plugin_command(args: &[&str]) -> Result<SlashCommand, SlashCommandParseError> {
    match args {
        [] => Ok(SlashCommand::Plugins {
            action: None,
            target: None,
        }),
        ["list"] => Ok(SlashCommand::Plugins {
            action: Some("list".to_string()),
            target: None,
        }),
        ["list", ..] => Err(usage_error("plugin list", "")),
        ["install"] => Err(usage_error("plugin install", "<path>")),
        ["install", target @ ..] => Ok(SlashCommand::Plugins {
            action: Some("install".to_string()),
            target: Some(target.join(" ")),
        }),
        ["enable"] => Err(usage_error("plugin enable", "<name>")),
        ["enable", target] => Ok(SlashCommand::Plugins {
            action: Some("enable".to_string()),
            target: Some((*target).to_string()),
        }),
        ["enable", ..] => Err(command_error(
            "Unexpected arguments for /plugin enable.",
            "plugin",
            "/plugin enable <name>",
        )),
        ["disable"] => Err(usage_error("plugin disable", "<name>")),
        ["disable", target] => Ok(SlashCommand::Plugins {
            action: Some("disable".to_string()),
            target: Some((*target).to_string()),
        }),
        ["disable", ..] => Err(command_error(
            "Unexpected arguments for /plugin disable.",
            "plugin",
            "/plugin disable <name>",
        )),
        ["uninstall"] => Err(usage_error("plugin uninstall", "<id>")),
        ["uninstall", target] => Ok(SlashCommand::Plugins {
            action: Some("uninstall".to_string()),
            target: Some((*target).to_string()),
        }),
        ["uninstall", ..] => Err(command_error(
            "Unexpected arguments for /plugin uninstall.",
            "plugin",
            "/plugin uninstall <id>",
        )),
        ["update"] => Err(usage_error("plugin update", "<id>")),
        ["update", target] => Ok(SlashCommand::Plugins {
            action: Some("update".to_string()),
            target: Some((*target).to_string()),
        }),
        ["update", ..] => Err(command_error(
            "Unexpected arguments for /plugin update.",
            "plugin",
            "/plugin update <id>",
        )),
        [action, ..] => Err(command_error(
            &format!(
                "Unknown /plugin action '{action}'. Use list, install <path>, enable <name>, disable <name>, uninstall <id>, or update <id>."
            ),
            "plugin",
            "/plugin [list|install <path>|enable <name>|disable <name>|uninstall <id>|update <id>]",
        )),
    }
}

fn parse_list_or_help_args(
    command: &str,
    args: Option<String>,
) -> Result<Option<String>, SlashCommandParseError> {
    let normalized = normalize_optional_args(args.as_deref());
    match normalized {
        None | Some("list" | "help" | "-h" | "--help") => Ok(args),
        Some(rest) if rest.starts_with("discover") => Ok(args),
        Some(unexpected) => Err(command_error(
            &format!(
                "Unexpected arguments for /{command}: {unexpected}. Use /{command}, /{command} list, /{command} discover <task>, or /{command} help."
            ),
            command,
            &format!("/{command} [list|discover <task>|help]"),
        )),
    }
}

fn parse_agent_command(args: Option<&str>) -> Result<SlashCommand, SlashCommandParseError> {
    match normalize_optional_args(args) {
        None | Some("list" | "help" | "-h" | "--help") => Ok(SlashCommand::Agents {
            args: args.map(String::from),
        }),
        Some(profile_args) if profile_args.starts_with("profile") => {
            let agent_id = profile_args
                .strip_prefix("profile")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(String::from);
            Ok(SlashCommand::AgentProfile { agent_id })
        }
        Some(other) => {
            // Treat unknown subcommand as agents list/help.
            Ok(SlashCommand::Agents {
                args: Some(other.to_string()),
            })
        }
    }
}

fn parse_skills_args(args: Option<&str>) -> Result<Option<String>, SlashCommandParseError> {
    let Some(args) = normalize_optional_args(args) else {
        return Ok(None);
    };

    if matches!(args, "list" | "help" | "-h" | "--help") {
        return Ok(Some(args.to_string()));
    }

    if args == "install" {
        return Err(command_error(
            "Usage: /skills install <path>",
            "skills",
            "/skills install <path>",
        ));
    }

    if let Some(target) = args.strip_prefix("install").map(str::trim) {
        if !target.is_empty() {
            return Ok(Some(format!("install {target}")));
        }
    }

    Ok(Some(args.to_string()))
}

fn usage_error(command: &str, argument_hint: &str) -> SlashCommandParseError {
    let usage = format!("/{command} {argument_hint}");
    let usage = usage.trim_end().to_string();
    command_error(
        &format!("Usage: {usage}"),
        command_root_name(command),
        &usage,
    )
}

fn command_error(message: &str, command: &str, usage: &str) -> SlashCommandParseError {
    let detail = render_slash_command_help_detail(command)
        .map(|detail| format!("\n\n{detail}"))
        .unwrap_or_default();
    SlashCommandParseError::new(format!("{message}\n  Usage            {usage}{detail}"))
}

fn remainder_after_command(input: &str, command: &str) -> Option<String> {
    input
        .trim()
        .strip_prefix(&format!("/{command}"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn find_slash_command_spec(name: &str) -> Option<&'static SlashCommandSpec> {
    slash_command_specs().iter().find(|spec| {
        spec.name.eq_ignore_ascii_case(name)
            || spec
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(name))
    })
}

fn command_root_name(command: &str) -> &str {
    command.split_whitespace().next().unwrap_or(command)
}

fn slash_command_usage(spec: &SlashCommandSpec) -> String {
    match spec.argument_hint {
        Some(argument_hint) => format!("/{} {argument_hint}", spec.name),
        None => format!("/{}", spec.name),
    }
}

fn slash_command_detail_lines(spec: &SlashCommandSpec) -> Vec<String> {
    let mut lines = vec![format!("/{}", spec.name)];
    lines.push(format!("  Summary          {}", spec.summary));
    lines.push(format!("  Usage            {}", slash_command_usage(spec)));
    lines.push(format!(
        "  Category         {}",
        slash_command_category(spec.name)
    ));
    if !spec.aliases.is_empty() {
        lines.push(format!(
            "  Aliases          {}",
            spec.aliases
                .iter()
                .map(|alias| format!("/{alias}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if spec.resume_supported {
        lines.push("  Resume           Supported with --resume <session-id|latest>".to_string());
    }
    lines
}

#[must_use]
pub fn render_slash_command_help_detail(name: &str) -> Option<String> {
    find_slash_command_spec(name).map(|spec| slash_command_detail_lines(spec).join("\n"))
}

#[must_use]
pub fn slash_command_specs() -> &'static [SlashCommandSpec] {
    SLASH_COMMAND_SPECS
}

#[must_use]
pub fn resume_supported_slash_commands() -> Vec<&'static SlashCommandSpec> {
    slash_command_specs()
        .iter()
        .filter(|spec| spec.resume_supported)
        .collect()
}

fn slash_command_category(name: &str) -> &'static str {
    match name {
        "help" | "status" | "cost" | "resume" | "session" | "version" | "usage" | "stats"
        | "rename" | "clear" | "compact" | "history" | "tokens" | "cache" | "exit" | "summary"
        | "tag" | "thinkback" | "copy" | "share" | "feedback" | "rewind" | "pin" | "unpin"
        | "bookmarks" | "context" | "files" | "focus" | "unfocus" | "retry" | "stop" | "undo" => {
            "Session"
        }
        "model" | "permissions" | "config" | "memory" | "theme" | "vim" | "voice" | "color"
        | "effort" | "fast" | "brief" | "output-style" | "keybindings" | "privacy-settings"
        | "stickers" | "language" | "profile" | "max-tokens" | "temperature" | "system-prompt"
        | "api-key" | "terminal-setup" | "notifications" | "telemetry" | "providers" | "env"
        | "project" | "reasoning" | "budget" | "rate-limit" | "workspace" | "reset" | "ide"
        | "desktop" | "upgrade" => "Config",
        "debug-tool-call" | "doctor" | "sandbox" | "diagnostics" | "tool-details" | "changelog"
        | "metrics" => "Debug",
        _ => "Tools",
    }
}

fn format_slash_command_help_line(spec: &SlashCommandSpec) -> String {
    let name = slash_command_usage(spec);
    let alias_suffix = if spec.aliases.is_empty() {
        String::new()
    } else {
        format!(
            " (aliases: {})",
            spec.aliases
                .iter()
                .map(|alias| format!("/{alias}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let resume = if spec.resume_supported {
        " [resume]"
    } else {
        ""
    };
    format!("  {name:<66} {}{alias_suffix}{resume}", spec.summary)
}

fn levenshtein_distance(left: &str, right: &str) -> usize {
    if left == right {
        return 0;
    }
    if left.is_empty() {
        return right.chars().count();
    }
    if right.is_empty() {
        return left.chars().count();
    }

    let right_chars = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right_chars.len()).collect::<Vec<_>>();
    let mut current = vec![0; right_chars.len() + 1];

    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right_chars.iter().enumerate() {
            let substitution_cost = usize::from(left_char != *right_char);
            current[right_index + 1] = (current[right_index] + 1)
                .min(previous[right_index + 1] + 1)
                .min(previous[right_index] + substitution_cost);
        }
        previous.clone_from(&current);
    }

    previous[right_chars.len()]
}

#[must_use]
pub fn suggest_slash_commands(input: &str, limit: usize) -> Vec<String> {
    let query = input.trim().trim_start_matches('/').to_ascii_lowercase();
    if query.is_empty() || limit == 0 {
        return Vec::new();
    }

    let mut suggestions = slash_command_specs()
        .iter()
        .filter_map(|spec| {
            let best = std::iter::once(spec.name)
                .chain(spec.aliases.iter().copied())
                .map(str::to_ascii_lowercase)
                .map(|candidate| {
                    let prefix_rank =
                        if candidate.starts_with(&query) || query.starts_with(&candidate) {
                            0
                        } else if candidate.contains(&query) || query.contains(&candidate) {
                            1
                        } else {
                            2
                        };
                    let distance = levenshtein_distance(&candidate, &query);
                    (prefix_rank, distance)
                })
                .min();

            best.and_then(|(prefix_rank, distance)| {
                if prefix_rank <= 1 || distance <= 2 {
                    Some((prefix_rank, distance, spec.name.len(), spec.name))
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>();

    suggestions.sort_unstable();
    suggestions
        .into_iter()
        .map(|(_, _, _, name)| format!("/{name}"))
        .take(limit)
        .collect()
}

#[must_use]
/// Render the slash-command help section, optionally excluding stub commands
/// (commands that are registered in the spec list but not yet implemented).
/// Pass an empty slice to include all commands.
pub fn render_slash_command_help_filtered(exclude: &[&str]) -> String {
    let mut lines = vec![
        "Slash commands".to_string(),
        "  Start here        /status, /diff, /agents, /skills, /commit".to_string(),
        "  [resume]          also works with --resume <session-id|latest>".to_string(),
        String::new(),
    ];

    let categories = ["Session", "Tools", "Config", "Debug"];

    for category in categories {
        lines.push(category.to_string());
        for spec in slash_command_specs()
            .iter()
            .filter(|spec| slash_command_category(spec.name) == category)
            .filter(|spec| !exclude.contains(&spec.name))
        {
            lines.push(format_slash_command_help_line(spec));
        }
        lines.push(String::new());
    }

    lines
        .into_iter()
        .rev()
        .skip_while(String::is_empty)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn render_slash_command_help() -> String {
    let mut lines = vec![
        "Slash commands".to_string(),
        "  Start here        /status, /diff, /agents, /skills, /commit".to_string(),
        "  [resume]          also works with --resume <session-id|latest>".to_string(),
        String::new(),
    ];

    let categories = ["Session", "Tools", "Config", "Debug"];

    for category in categories {
        lines.push(category.to_string());
        for spec in slash_command_specs()
            .iter()
            .filter(|spec| slash_command_category(spec.name) == category)
        {
            lines.push(format_slash_command_help_line(spec));
        }
        lines.push(String::new());
    }

    lines.push("Keyboard shortcuts".to_string());
    lines.push("  Up/Down              Navigate prompt history".to_string());
    lines.push("  Tab                  Complete commands, modes, and recent sessions".to_string());
    lines.push("  Ctrl-C               Clear input (or exit on empty prompt)".to_string());
    lines.push("  Shift+Enter/Ctrl+J   Insert a newline".to_string());

    lines
        .into_iter()
        .rev()
        .skip_while(String::is_empty)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlashCommandResult {
    pub message: String,
    pub error: Option<String>,
    pub session: Session,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginsCommandResult {
    pub message: String,
    pub reload_runtime: bool,
}

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

#[allow(clippy::too_many_lines)]
pub fn handle_plugins_slash_command(
    action: Option<&str>,
    target: Option<&str>,
    manager: &mut PluginManager,
) -> Result<PluginsCommandResult, PluginError> {
    match action {
        None | Some("list") => {
            let report = manager.installed_plugin_registry_report()?;
            let plugins = report.summaries();
            let failures = report.failures();
            Ok(PluginsCommandResult {
                message: render_plugins_report_with_failures(&plugins, failures),
                reload_runtime: false,
            })
        }
        Some("install") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins install <path>".to_string(),
                    reload_runtime: false,
                });
            };
            let install = manager.install(target)?;
            let plugin = manager
                .list_installed_plugins()?
                .into_iter()
                .find(|plugin| plugin.metadata.id == install.plugin_id);
            Ok(PluginsCommandResult {
                message: render_plugin_install_report(&install.plugin_id, plugin.as_ref()),
                reload_runtime: true,
            })
        }
        Some("enable") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins enable <name>".to_string(),
                    reload_runtime: false,
                });
            };
            let plugin = resolve_plugin_target(manager, target)?;
            manager.enable(&plugin.metadata.id)?;
            Ok(PluginsCommandResult {
                message: format!(
                    "Plugins\n  Result           enabled {}\n  Name             {}\n  Version          {}\n  Status           enabled",
                    plugin.metadata.id, plugin.metadata.name, plugin.metadata.version
                ),
                reload_runtime: true,
            })
        }
        Some("disable") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins disable <name>".to_string(),
                    reload_runtime: false,
                });
            };
            let plugin = resolve_plugin_target(manager, target)?;
            manager.disable(&plugin.metadata.id)?;
            Ok(PluginsCommandResult {
                message: format!(
                    "Plugins\n  Result           disabled {}\n  Name             {}\n  Version          {}\n  Status           disabled",
                    plugin.metadata.id, plugin.metadata.name, plugin.metadata.version
                ),
                reload_runtime: true,
            })
        }
        Some("uninstall") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins uninstall <plugin-id>".to_string(),
                    reload_runtime: false,
                });
            };
            manager.uninstall(target)?;
            Ok(PluginsCommandResult {
                message: format!("Plugins\n  Result           uninstalled {target}"),
                reload_runtime: true,
            })
        }
        Some("update") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins update <plugin-id>".to_string(),
                    reload_runtime: false,
                });
            };
            let update = manager.update(target)?;
            let plugin = manager
                .list_installed_plugins()?
                .into_iter()
                .find(|plugin| plugin.metadata.id == update.plugin_id);
            Ok(PluginsCommandResult {
                message: format!(
                    "Plugins\n  Result           updated {}\n  Name             {}\n  Old version      {}\n  New version      {}\n  Status           {}",
                    update.plugin_id,
                    plugin
                        .as_ref()
                        .map_or_else(|| update.plugin_id.clone(), |plugin| plugin.metadata.name.clone()),
                    update.old_version,
                    update.new_version,
                    plugin
                        .as_ref()
                        .map_or("unknown", |plugin| if plugin.enabled { "enabled" } else { "disabled" }),
                ),
                reload_runtime: true,
            })
        }
        Some(other) => Ok(PluginsCommandResult {
            message: format!(
                "Unknown /plugins action '{other}'. Use list, install, enable, disable, uninstall, or update."
            ),
            reload_runtime: false,
        }),
    }
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
            let discovery = runtime::TeamDiscoveryProtocol::new();
            let ranked = discovery.discover_team(task_desc, &[]);
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
                let rep_line = agent
                    .reputation
                    .as_ref()
                    .map(|r| format!(" | rep: {:.1}/10", r.composite()))
                    .unwrap_or_default();
                report.push_str(&format!(
                    "  {}. {} ({}) — [{}] {}\n",
                    i + 1,
                    agent.role,
                    agent.agent_id,
                    agent.capabilities.join(", "),
                    rep_line,
                ));
            }
            // Show auto-assembly result
            if let Some(team) = discovery.auto_assemble(task_desc, &[]) {
                report.push_str(&format!(
                    "\nAuto-assembled team:\n  Leader: {} ({})\n",
                    team.leader.agent_id, team.leader.role
                ));
                if !team.workers.is_empty() {
                    report.push_str("  Workers:\n");
                    for w in &team.workers {
                        report.push_str(&format!(
                            "    - {} ({}) [{}]\n",
                            w.agent_id,
                            w.role,
                            w.capabilities.join(", ")
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
            let discovery = runtime::TeamDiscoveryProtocol::new();
            let ranked = discovery.discover_team(task_desc, &[]);
            let agents_json: Vec<Value> = ranked
                .iter()
                .map(|a| {
                    json!({
                        "agent_id": a.agent_id,
                        "role": a.role,
                        "capabilities": a.capabilities,
                        "reputation": a.reputation.as_ref().map(|r| r.composite()),
                        "status": format!("{:?}", a.status),
                    })
                })
                .collect();
            let team = discovery.auto_assemble(task_desc, &[]);
            let team_json = team.map(|t| {
                json!({
                    "leader": { "agent_id": t.leader.agent_id, "role": t.leader.role },
                    "workers": t.workers.iter().map(|w| json!({ "agent_id": w.agent_id, "role": w.role })).collect::<Vec<_>>(),
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

pub fn handle_mcp_slash_command(
    args: Option<&str>,
    cwd: &Path,
) -> Result<String, runtime::ConfigError> {
    let loader = ConfigLoader::default_for(cwd);
    render_mcp_report_for(&loader, cwd, args)
}

pub fn handle_mcp_slash_command_json(
    args: Option<&str>,
    cwd: &Path,
) -> Result<Value, runtime::ConfigError> {
    let loader = ConfigLoader::default_for(cwd);
    render_mcp_report_json_for(&loader, cwd, args)
}

pub fn handle_skills_slash_command(args: Option<&str>, cwd: &Path) -> std::io::Result<String> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_skills_usage(None),
                ["install", ..] => render_skills_usage(Some("install")),
                ["create", ..] => render_skills_usage(Some("create")),
                ["view", ..] => render_skills_usage(Some("view")),
                ["edit", ..] => render_skills_usage(Some("edit")),
                ["delete", ..] => render_skills_usage(Some("delete")),
                ["generate", ..] => render_skills_usage(Some("generate")),
                _ => render_skills_usage(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let roots = discover_skill_roots_internal(cwd);
            let skills = load_skills_from_roots(&roots)?;
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
        // New CRUD operations
        Some("create") => Ok(render_skills_usage(Some("create"))),
        Some(args) if args.starts_with("create ") => {
            let input = parse_skill_create_args(args["create ".len()..].trim());
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let result = manager.create_skill(input);
            Ok(render_skill_create_report(&result))
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
        Some("edit") => Ok(render_skills_usage(Some("edit"))),
        Some(args) if args.starts_with("edit ") => {
            let input = parse_skill_edit_args(args["edit ".len()..].trim());
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let result = manager.edit_skill(input);
            Ok(render_skill_edit_report(&result))
        }
        Some("delete") => Ok(render_skills_usage(Some("delete"))),
        Some(args) if args.starts_with("delete ") => {
            let name = args["delete ".len()..].trim();
            let force = name.contains("--force") || name.contains("-f");
            let name = name
                .trim_end_matches("--force")
                .trim_end_matches("-f")
                .trim();
            if name.is_empty() {
                return Ok(render_skills_usage(Some("delete")));
            }
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let input = SkillDeleteInput {
                name: name.to_string(),
                force,
            };
            let result = manager.delete_skill(input);
            Ok(render_skill_delete_report(&result))
        }
        Some("generate") => Ok(render_skills_usage(Some("generate"))),
        Some(args) if args.starts_with("generate ") => {
            let task = args["generate ".len()..].trim();
            if task.is_empty() {
                return Ok(render_skills_usage(Some("generate")));
            }
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let input = SkillGenerateInput {
                task_description: Some(task.to_string()),
                tool_call_count: None,
                error_count: None,
                user_corrections: None,
                name: None,
            };
            let result = manager.generate_skill(input);
            Ok(render_skill_generate_report(&result))
        }
        Some(args) if is_help_arg(args) => Ok(render_skills_usage(None)),
        Some(args) => Ok(render_skills_usage(Some(args))),
    }
}

// Discover skill root paths for SkillManager
fn discover_skill_root_paths(cwd: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for ancestor in cwd.ancestors() {
        for subdir in [".cowd/skills", ".cowd/skills", ".agents/skills"] {
            let path = ancestor.join(subdir);
            if path.exists() && !roots.contains(&path) {
                roots.push(path);
            }
        }
    }
    // Also check ~/.cowd/skills
    if let Ok(home) = std::env::var("HOME") {
        let home_path = PathBuf::from(home).join(".cowd/skills");
        if home_path.exists() && !roots.contains(&home_path) {
            roots.push(home_path);
        }
    }
    roots
}

pub fn handle_skills_slash_command_json(args: Option<&str>, cwd: &Path) -> std::io::Result<Value> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_skills_usage_json(None),
                ["install", ..] => render_skills_usage_json(Some("install")),
                ["create", ..] => render_skills_usage_json(Some("create")),
                ["view", ..] => render_skills_usage_json(Some("view")),
                ["edit", ..] => render_skills_usage_json(Some("edit")),
                ["delete", ..] => render_skills_usage_json(Some("delete")),
                ["generate", ..] => render_skills_usage_json(Some("generate")),
                _ => render_skills_usage_json(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let roots = discover_skill_roots_internal(cwd);
            let skills = load_skills_from_roots(&roots)?;
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
        // New CRUD operations
        Some("create") => Ok(render_skills_usage_json(Some("create"))),
        Some(args) if args.starts_with("create ") => {
            let input = parse_skill_create_args(args["create ".len()..].trim());
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let result = manager.create_skill(input);
            Ok(json!({
                "kind": "skills",
                "action": "create",
                "success": result.success,
                "name": result.name,
                "path": result.path,
                "message": result.message,
            }))
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
        Some("edit") => Ok(render_skills_usage_json(Some("edit"))),
        Some(args) if args.starts_with("edit ") => {
            let input = parse_skill_edit_args(args["edit ".len()..].trim());
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let result = manager.edit_skill(input);
            Ok(json!({
                "kind": "skills",
                "action": "edit",
                "success": result.success,
                "name": result.name,
                "path": result.path,
                "message": result.message,
            }))
        }
        Some("delete") => Ok(render_skills_usage_json(Some("delete"))),
        Some(args) if args.starts_with("delete ") => {
            let name = args["delete ".len()..].trim();
            let force = name.contains("--force") || name.contains("-f");
            let name = name
                .trim_end_matches("--force")
                .trim_end_matches("-f")
                .trim();
            if name.is_empty() {
                return Ok(render_skills_usage_json(Some("delete")));
            }
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let input = SkillDeleteInput {
                name: name.to_string(),
                force,
            };
            let result = manager.delete_skill(input);
            Ok(json!({
                "kind": "skills",
                "action": "delete",
                "success": result.success,
                "name": result.name,
                "message": result.message,
            }))
        }
        Some("generate") => Ok(render_skills_usage_json(Some("generate"))),
        Some(args) if args.starts_with("generate ") => {
            let task = args["generate ".len()..].trim();
            if task.is_empty() {
                return Ok(render_skills_usage_json(Some("generate")));
            }
            let paths = discover_skill_root_paths(cwd);
            let manager = SkillManager::new(paths);
            let input = SkillGenerateInput {
                task_description: Some(task.to_string()),
                tool_call_count: None,
                error_count: None,
                user_corrections: None,
                name: None,
            };
            let result = manager.generate_skill(input);
            Ok(json!({
                "kind": "skills",
                "action": "generate",
                "success": result.success,
                "name": result.name,
                "content": result.content,
                "path": result.path,
                "message": result.message,
            }))
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
        // New CRUD commands - all handled locally
        Some("create" | "view" | "edit" | "delete" | "generate") => SkillSlashDispatch::Local,
        Some(args)
            if args.starts_with("create ")
                || args.starts_with("view ")
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
                let roots = discover_skill_roots_internal(cwd);
                if let Ok(available) = load_skills_from_roots(&roots) {
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
                message.push_str("\n  Usage: /skills [list|install <path>|help|<skill> [args]]");
                return Err(message);
            }
        }
    }
    Ok(dispatch)
}

pub fn resolve_skill_path(cwd: &Path, skill: &str) -> std::io::Result<PathBuf> {
    let requested = skill.trim().trim_start_matches('/').trim_start_matches('$');
    if requested.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "skill must not be empty",
        ));
    }

    let roots = discover_skill_roots_internal(cwd);
    for root in &roots {
        let mut entries = Vec::new();
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
                    let contents = fs::read_to_string(&skill_path)?;
                    let (name, _) = parse_skill_frontmatter(&contents);
                    entries.push((
                        name.unwrap_or_else(|| entry.file_name().to_string_lossy().to_string()),
                        skill_path,
                    ));
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
                    let (name, _) = parse_skill_frontmatter(&contents);
                    entries.push((name.unwrap_or(fallback_name), markdown_path));
                }
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        if let Some((_, path)) = entries
            .into_iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(requested))
        {
            return Ok(path);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("unknown skill: {requested}"),
    ))
}

pub(crate) fn render_mcp_report_for(
    loader: &ConfigLoader,
    cwd: &Path,
    args: Option<&str>,
) -> Result<String, runtime::ConfigError> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_mcp_usage(None),
                ["show", ..] => render_mcp_usage(Some("show")),
                _ => render_mcp_usage(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let runtime_config = loader.load()?;
            Ok(render_mcp_summary_report(
                cwd,
                runtime_config.mcp().servers(),
            ))
        }
        Some(args) if is_help_arg(args) => Ok(render_mcp_usage(None)),
        Some("show") => Ok(render_mcp_usage(Some("show"))),
        Some(args) if args.split_whitespace().next() == Some("show") => {
            let mut parts = args.split_whitespace();
            let _ = parts.next();
            let Some(server_name) = parts.next() else {
                return Ok(render_mcp_usage(Some("show")));
            };
            if parts.next().is_some() {
                return Ok(render_mcp_usage(Some(args)));
            }
            let runtime_config = loader.load()?;
            Ok(render_mcp_server_report(
                cwd,
                server_name,
                runtime_config.mcp().get(server_name),
            ))
        }
        Some(args) => Ok(render_mcp_usage(Some(args))),
    }
}

pub(crate) fn render_mcp_report_json_for(
    loader: &ConfigLoader,
    cwd: &Path,
    args: Option<&str>,
) -> Result<Value, runtime::ConfigError> {
    if let Some(args) = normalize_optional_args(args) {
        if let Some(help_path) = help_path_from_args(args) {
            return Ok(match help_path.as_slice() {
                [] => render_mcp_usage_json(None),
                ["show", ..] => render_mcp_usage_json(Some("show")),
                _ => render_mcp_usage_json(Some(&help_path.join(" "))),
            });
        }
    }

    match normalize_optional_args(args) {
        None | Some("list") => {
            let runtime_config = loader.load()?;
            Ok(render_mcp_summary_report_json(
                cwd,
                runtime_config.mcp().servers(),
            ))
        }
        Some(args) if is_help_arg(args) => Ok(render_mcp_usage_json(None)),
        Some("show") => Ok(render_mcp_usage_json(Some("show"))),
        Some(args) if args.split_whitespace().next() == Some("show") => {
            let mut parts = args.split_whitespace();
            let _ = parts.next();
            let Some(server_name) = parts.next() else {
                return Ok(render_mcp_usage_json(Some("show")));
            };
            if parts.next().is_some() {
                return Ok(render_mcp_usage_json(Some(args)));
            }
            let runtime_config = loader.load()?;
            Ok(render_mcp_server_report_json(
                cwd,
                server_name,
                runtime_config.mcp().get(server_name),
            ))
        }
        Some(args) => Ok(render_mcp_usage_json(Some(args))),
    }
}

#[must_use]
pub fn render_plugins_report(plugins: &[PluginSummary]) -> String {
    let mut lines = vec!["Plugins".to_string()];
    if plugins.is_empty() {
        lines.push("  No plugins installed.".to_string());
        return lines.join("\n");
    }
    for plugin in plugins {
        let enabled = if plugin.enabled {
            "enabled"
        } else {
            "disabled"
        };
        lines.push(format!(
            "  {name:<20} v{version:<10} {enabled}",
            name = plugin.metadata.name,
            version = plugin.metadata.version,
        ));
    }
    lines.join("\n")
}

#[must_use]
pub fn render_plugins_report_with_failures(
    plugins: &[PluginSummary],
    failures: &[PluginLoadFailure],
) -> String {
    let mut lines = vec!["Plugins".to_string()];

    // Show successfully loaded plugins
    if plugins.is_empty() {
        lines.push("  No plugins installed.".to_string());
    } else {
        for plugin in plugins {
            let enabled = if plugin.enabled {
                "enabled"
            } else {
                "disabled"
            };
            lines.push(format!(
                "  {name:<20} v{version:<10} {enabled}",
                name = plugin.metadata.name,
                version = plugin.metadata.version,
            ));
        }
    }

    // Show warnings for broken plugins
    if !failures.is_empty() {
        lines.push(String::new());
        lines.push("Warnings:".to_string());
        for failure in failures {
            lines.push(format!(
                "  ⚠️  Failed to load {} plugin from `{}`",
                failure.kind,
                failure.plugin_root.display()
            ));
            lines.push(format!("      Error: {}", failure.error()));
        }
    }

    lines.join("\n")
}

fn render_plugin_install_report(plugin_id: &str, plugin: Option<&PluginSummary>) -> String {
    let name = plugin.map_or(plugin_id, |plugin| plugin.metadata.name.as_str());
    let version = plugin.map_or("unknown", |plugin| plugin.metadata.version.as_str());
    let enabled = plugin.is_some_and(|plugin| plugin.enabled);
    format!(
        "Plugins\n  Result           installed {plugin_id}\n  Name             {name}\n  Version          {version}\n  Status           {}",
        if enabled { "enabled" } else { "disabled" }
    )
}

fn resolve_plugin_target(
    manager: &PluginManager,
    target: &str,
) -> Result<PluginSummary, PluginError> {
    let mut matches = manager
        .list_installed_plugins()?
        .into_iter()
        .filter(|plugin| plugin.metadata.id == target || plugin.metadata.name == target)
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(PluginError::NotFound(format!(
            "plugin `{target}` is not installed or discoverable"
        ))),
        _ => Err(PluginError::InvalidManifest(format!(
            "plugin name `{target}` is ambiguous; use the full plugin id"
        ))),
    }
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

#[allow(clippy::too_many_lines)]
fn discover_skill_roots_internal(cwd: &Path) -> Vec<SkillRoot> {
    let mut roots = Vec::new();

    for ancestor in cwd.ancestors() {
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectClaw,
            ancestor.join(".cowd").join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectClaw,
            ancestor.join(".agents").join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectCodex,
            ancestor.join(".codex").join("skills"),
            SkillOrigin::SkillsDir,
        );
        // Migration: discover from .claude if directory exists
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectClaude,
            ancestor.join(".claude").join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectClaw,
            ancestor.join(".cowd").join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectCodex,
            ancestor.join(".codex").join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
        // Migration: discover from .claude if directory exists
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::ProjectClaude,
            ancestor.join(".claude").join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
    }

    if let Ok(cc_config_home) = env::var("COWD_CONFIG_HOME") {
        let cc_config_home = PathBuf::from(cc_config_home);
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClawConfigHome,
            cc_config_home.join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClawConfigHome,
            cc_config_home.join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
    }

    if let Ok(codex_home) = env::var("CODEX_HOME") {
        let codex_home = PathBuf::from(codex_home);
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserCodexHome,
            codex_home.join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserCodexHome,
            codex_home.join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
    }

    if let Some(home) = env::var_os("HOME") {
        let home = PathBuf::from(home);
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClaw,
            home.join(".cowd").join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClaw,
            home.join(".cowd").join("skills").join("omc-learned"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClaw,
            home.join(".cowd").join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserCodex,
            home.join(".codex").join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserCodex,
            home.join(".codex").join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
        // Migration: discover from .claude if directory exists
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClaude,
            home.join(".claude").join("skills"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClaude,
            home.join(".claude").join("skills").join("omc-learned"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClaude,
            home.join(".claude").join("commands"),
            SkillOrigin::LegacyCommandsDir,
        );
    }

    if let Ok(cowd_config_home) = env::var("COWD_CONFIG_HOME") {
        let cowd_config_home = PathBuf::from(cowd_config_home);
        let skills_dir = cowd_config_home.join("skills");
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClawConfigHome,
            skills_dir.clone(),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClawConfigHome,
            skills_dir.join("omc-learned"),
            SkillOrigin::SkillsDir,
        );
        push_unique_skill_root(
            &mut roots,
            DefinitionSource::UserClawConfigHome,
            cowd_config_home.join("commands"),
            SkillOrigin::LegacyCommandsDir,
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

fn push_unique_skill_root(
    roots: &mut Vec<SkillRoot>,
    source: DefinitionSource,
    path: PathBuf,
    origin: SkillOrigin,
) {
    if path.is_dir() && !roots.iter().any(|existing| existing.path == path) {
        roots.push(SkillRoot {
            source,
            path,
            origin,
        });
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

// Helper function to parse skill create arguments
fn parse_skill_create_args(input: &str) -> SkillCreateInput {
    let mut name = String::new();
    let mut description = String::new();
    let mut category = None;
    let mut tags = None;
    let content = None;

    let parts: Vec<&str> = input.split_whitespace().collect();
    let mut i = 0;
    while i < parts.len() {
        match parts[i] {
            "-n" | "--name" => {
                if i + 1 < parts.len() {
                    name = parts[i + 1].to_string();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-d" | "--description" => {
                if i + 1 < parts.len() {
                    description = parts[i + 1].to_string();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-c" | "--category" => {
                if i + 1 < parts.len() {
                    category = Some(parts[i + 1].to_string());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-t" | "--tags" => {
                if i + 1 < parts.len() {
                    tags = Some(parts[i + 1].split(',').map(String::from).collect());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => {
                if name.is_empty() {
                    name = parts[i].to_string();
                } else if description.is_empty() {
                    description = parts[i].to_string();
                }
                i += 1;
            }
        }
    }

    SkillCreateInput {
        name,
        description,
        category,
        tags,
        content,
    }
}

// Helper function to parse skill edit arguments
fn parse_skill_edit_args(input: &str) -> SkillEditInput {
    let mut name = String::new();
    let mut content = None;
    let mut description = None;
    let mut search = None;
    let mut replace = None;
    let mut file_path = None;

    let parts: Vec<&str> = input.split_whitespace().collect();
    let mut i = 0;
    while i < parts.len() {
        match parts[i] {
            "-n" | "--name" => {
                if i + 1 < parts.len() {
                    name = parts[i + 1].to_string();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-c" | "--content" => {
                if i + 1 < parts.len() {
                    content = Some(parts[i + 1].to_string());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-d" | "--description" => {
                if i + 1 < parts.len() {
                    description = Some(parts[i + 1].to_string());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-s" | "--search" => {
                if i + 1 < parts.len() {
                    search = Some(parts[i + 1].to_string());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-r" | "--replace" => {
                if i + 1 < parts.len() {
                    replace = Some(parts[i + 1].to_string());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "-f" | "--file" => {
                if i + 1 < parts.len() {
                    file_path = Some(parts[i + 1].to_string());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => {
                if name.is_empty() {
                    name = parts[i].to_string();
                }
                i += 1;
            }
        }
    }

    SkillEditInput {
        name,
        content,
        description,
        search,
        replace,
        file_path,
    }
}

// Render skill create report
fn render_skill_create_report(result: &SkillCreateOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Result           created {}", result.name));
        lines.push(format!("  Path             {}", result.path));
    } else {
        lines.push(format!("  Result           failed"));
        lines.push(format!("  Error            {}", result.message));
    }
    lines.join("\n")
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

// Render skill edit report
fn render_skill_edit_report(result: &SkillEditOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Result           updated {}", result.name));
        lines.push(format!("  Path             {}", result.path));
    } else {
        lines.push(format!("  Result           failed"));
        lines.push(format!("  Error            {}", result.message));
    }
    lines.join("\n")
}

// Render skill delete report
fn render_skill_delete_report(result: &SkillDeleteOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Result           deleted {}", result.name));
    } else {
        lines.push(format!("  Result           failed"));
        lines.push(format!("  Error            {}", result.message));
    }
    lines.join("\n")
}

// Render skill generate report
fn render_skill_generate_report(result: &SkillGenerateOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Result           generated {}", result.name));
        lines.push(format!("  Message          {}", result.message));
        if let Some(ref path) = result.path {
            lines.push(format!("  Path             {}", path));
        }
        lines.push(String::new());
        lines.push("---".to_string());
        lines.push(String::new());
        // Show generated content preview
        let preview = if result.content.len() > 800 {
            format!("{}...\n\n[Content truncated]", &result.content[..800])
        } else {
            result.content.clone()
        };
        lines.push(preview);
    } else {
        lines.push(format!("  Result           failed"));
        lines.push(format!("  Error            {}", result.message));
    }
    lines.join("\n")
}

// Render skills usage help
fn render_skills_usage(topic: Option<&str>) -> String {
    match topic {
        Some("create") => r#"Skills - Create

Usage: /skill create [options] <name> [--description <desc>]

Options:
  -n, --name <name>       Skill name
  -d, --description <desc> Description
  -c, --category <cat>   Category
  -t, --tags <tags>      Comma-separated tags

Example:
  /skill create my-skill --name my-skill --description "My custom skill""#
            .to_string(),
        Some("view") => r#"Skills - View

Usage: /skill view <name>

View skill metadata and content.

Example:
  /skill view git-essentials
  /skill view my-skill --file references/api.md"#
            .to_string(),
        Some("edit") => r#"Skills - Edit

Usage: /skill edit <name> [options]

Options:
  -n, --name <name>       Skill name
  -c, --content <text>    New content
  -d, --description <desc> New description
  -s, --search <text>     Search text for patch
  -r, --replace <text>    Replacement text
  -f, --file <path>       File to edit

Example:
  /skill edit my-skill --search old --replace new"#
            .to_string(),
        Some("delete") => r#"Skills - Delete

Usage: /skill delete <name> [--force]

Options:
  --force, -f           Skip confirmation

Example:
  /skill delete my-skill
  /skill delete my-skill --force"#
            .to_string(),
        Some("generate") => r#"Skills - Generate

Usage: /skill generate <task-description>

Auto-generate a skill based on task context.

Example:
  /skill generate "AWS Lambda deployment workflow""#
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
                "  Usage            /skills [list|install <path>|help|<skill> [args]]".to_string(),
                "  Alias            /skill".to_string(),
                "  Direct CLI       cowd skills [list|install <path>|help|<skill> [args]]".to_string(),
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
        Some("create") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "create",
            "usage": r#"Usage: /skill create [options] <name> [--description <desc>]

Options:
  -n, --name <name>       Skill name
  -d, --description <desc> Description
  -c, --category <cat>   Category
  -t, --tags <tags>      Comma-separated tags"#,
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
        Some("edit") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "edit",
            "usage": r#"Usage: /skill edit <name> [options]

Options:
  -n, --name <name>       Skill name
  -c, --content <text>    New content
  -d, --description <desc> New description
  -s, --search <text>     Search text for patch
  -r, --replace <text>    Replacement text
  -f, --file <path>       File to edit"#,
        }),
        Some("delete") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "delete",
            "usage": r#"Usage: /skill delete <name> [--force]

Options:
  --force, -f           Skip confirmation"#,
        }),
        Some("generate") => json!({
            "kind": "skills",
            "action": "help",
            "topic": "generate",
            "usage": "Usage: /skill generate <task-description>\n\nAuto-generate a skill based on task context.",
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
                "slash_command": "/skills [list|install <path>|help|<skill> [args]]",
                "aliases": ["/skill"],
                "direct_cli": "cowd skills [list|install <path>|help|<skill> [args]]",
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

fn render_mcp_summary_report(
    cwd: &Path,
    servers: &BTreeMap<String, ScopedMcpServerConfig>,
) -> String {
    let mut lines = vec![
        "MCP".to_string(),
        format!("  Working directory {}", cwd.display()),
        format!("  Configured servers {}", servers.len()),
    ];
    if servers.is_empty() {
        lines.push("  No MCP servers configured.".to_string());
        return lines.join("\n");
    }

    lines.push(String::new());
    for (name, server) in servers {
        lines.push(format!(
            "  {name:<16} {transport:<13} {scope:<7} {summary}",
            transport = mcp_transport_label(&server.config),
            scope = config_source_label(server.scope),
            summary = mcp_server_summary(&server.config)
        ));
    }

    lines.join("\n")
}

fn render_mcp_summary_report_json(
    cwd: &Path,
    servers: &BTreeMap<String, ScopedMcpServerConfig>,
) -> Value {
    json!({
        "kind": "mcp",
        "action": "list",
        "working_directory": cwd.display().to_string(),
        "configured_servers": servers.len(),
        "servers": servers
            .iter()
            .map(|(name, server)| mcp_server_json(name, server))
            .collect::<Vec<_>>(),
    })
}

fn render_mcp_server_report(
    cwd: &Path,
    server_name: &str,
    server: Option<&ScopedMcpServerConfig>,
) -> String {
    let Some(server) = server else {
        return format!(
            "MCP\n  Working directory {}\n  Result            server `{server_name}` is not configured",
            cwd.display()
        );
    };

    let mut lines = vec![
        "MCP".to_string(),
        format!("  Working directory {}", cwd.display()),
        format!("  Name              {server_name}"),
        format!("  Scope             {}", config_source_label(server.scope)),
        format!(
            "  Transport         {}",
            mcp_transport_label(&server.config)
        ),
    ];

    match &server.config {
        McpServerConfig::Stdio(config) => {
            lines.push(format!("  Command           {}", config.command));
            lines.push(format!(
                "  Args              {}",
                format_optional_list(&config.args)
            ));
            lines.push(format!(
                "  Env keys          {}",
                format_optional_keys(config.env.keys().cloned().collect())
            ));
            lines.push(format!(
                "  Tool timeout      {}",
                config
                    .tool_call_timeout_ms
                    .map_or_else(|| "<default>".to_string(), |value| format!("{value} ms"))
            ));
        }
        McpServerConfig::Sse(config) | McpServerConfig::Http(config) => {
            lines.push(format!("  URL               {}", config.url));
            lines.push(format!(
                "  Header keys       {}",
                format_optional_keys(config.headers.keys().cloned().collect())
            ));
            lines.push(format!(
                "  Header helper     {}",
                config.headers_helper.as_deref().unwrap_or("<none>")
            ));
            lines.push(format!(
                "  OAuth             {}",
                format_mcp_oauth(config.oauth.as_ref())
            ));
        }
        McpServerConfig::Ws(config) => {
            lines.push(format!("  URL               {}", config.url));
            lines.push(format!(
                "  Header keys       {}",
                format_optional_keys(config.headers.keys().cloned().collect())
            ));
            lines.push(format!(
                "  Header helper     {}",
                config.headers_helper.as_deref().unwrap_or("<none>")
            ));
        }
        McpServerConfig::Sdk(config) => {
            lines.push(format!("  SDK name          {}", config.name));
        }
        McpServerConfig::ManagedProxy(config) => {
            lines.push(format!("  URL               {}", config.url));
            lines.push(format!("  Proxy id          {}", config.id));
        }
    }

    lines.join("\n")
}

fn render_mcp_server_report_json(
    cwd: &Path,
    server_name: &str,
    server: Option<&ScopedMcpServerConfig>,
) -> Value {
    match server {
        Some(server) => json!({
            "kind": "mcp",
            "action": "show",
            "working_directory": cwd.display().to_string(),
            "found": true,
            "server": mcp_server_json(server_name, server),
        }),
        None => json!({
            "kind": "mcp",
            "action": "show",
            "working_directory": cwd.display().to_string(),
            "found": false,
            "server_name": server_name,
            "message": format!("server `{server_name}` is not configured"),
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

fn render_mcp_usage(unexpected: Option<&str>) -> String {
    let mut lines = vec![
        "MCP".to_string(),
        "  Usage            /mcp [list|show <server>|help]".to_string(),
        "  Direct CLI       cowd mcp [list|show <server>|help]".to_string(),
        "  Sources          .cowd/config.yaml, .cowd/config.local.yaml".to_string(),
    ];
    if let Some(args) = unexpected {
        lines.push(format!("  Unexpected       {args}"));
    }
    lines.join("\n")
}

fn render_mcp_usage_json(unexpected: Option<&str>) -> Value {
    json!({
        "kind": "mcp",
        "action": "help",
        "usage": {
            "slash_command": "/mcp [list|show <server>|help]",
            "direct_cli": "cowd mcp [list|show <server>|help]",
            "sources": [".cowd/config.yaml", ".cowd/config.local.yaml"],
        },
        "unexpected": unexpected,
    })
}

fn config_source_label(source: ConfigSource) -> &'static str {
    match source {
        ConfigSource::User => "user",
        ConfigSource::Project => "project",
        ConfigSource::Local => "local",
        ConfigSource::Environment => "env",
        ConfigSource::Cli => "cli",
    }
}

fn mcp_transport_label(config: &McpServerConfig) -> &'static str {
    match config {
        McpServerConfig::Stdio(_) => "stdio",
        McpServerConfig::Sse(_) => "sse",
        McpServerConfig::Http(_) => "http",
        McpServerConfig::Ws(_) => "ws",
        McpServerConfig::Sdk(_) => "sdk",
        McpServerConfig::ManagedProxy(_) => "managed-proxy",
    }
}

fn mcp_server_summary(config: &McpServerConfig) -> String {
    match config {
        McpServerConfig::Stdio(config) => {
            if config.args.is_empty() {
                config.command.clone()
            } else {
                format!("{} {}", config.command, config.args.join(" "))
            }
        }
        McpServerConfig::Sse(config) | McpServerConfig::Http(config) => config.url.clone(),
        McpServerConfig::Ws(config) => config.url.clone(),
        McpServerConfig::Sdk(config) => config.name.clone(),
        McpServerConfig::ManagedProxy(config) => format!("{} ({})", config.id, config.url),
    }
}

fn format_optional_list(values: &[String]) -> String {
    if values.is_empty() {
        "<none>".to_string()
    } else {
        values.join(" ")
    }
}

fn format_optional_keys(mut keys: Vec<String>) -> String {
    if keys.is_empty() {
        return "<none>".to_string();
    }
    keys.sort();
    keys.join(", ")
}

fn format_mcp_oauth(oauth: Option<&McpOAuthConfig>) -> String {
    let Some(oauth) = oauth else {
        return "<none>".to_string();
    };

    let mut parts = Vec::new();
    if let Some(client_id) = &oauth.client_id {
        parts.push(format!("client_id={client_id}"));
    }
    if let Some(port) = oauth.callback_port {
        parts.push(format!("callback_port={port}"));
    }
    if let Some(url) = &oauth.auth_server_metadata_url {
        parts.push(format!("metadata_url={url}"));
    }
    if let Some(xaa) = oauth.xaa {
        parts.push(format!("xaa={xaa}"));
    }
    if parts.is_empty() {
        "enabled".to_string()
    } else {
        parts.join(", ")
    }
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

fn config_source_id(source: ConfigSource) -> &'static str {
    match source {
        ConfigSource::User => "user",
        ConfigSource::Project => "project",
        ConfigSource::Local => "local",
        ConfigSource::Environment => "env",
        ConfigSource::Cli => "cli",
    }
}

fn config_source_json(source: ConfigSource) -> Value {
    json!({
        "id": config_source_id(source),
        "label": config_source_label(source),
    })
}

fn mcp_transport_json(config: &McpServerConfig) -> Value {
    let label = mcp_transport_label(config);
    json!({
        "id": label,
        "label": label,
    })
}

fn mcp_oauth_json(oauth: Option<&McpOAuthConfig>) -> Value {
    let Some(oauth) = oauth else {
        return Value::Null;
    };
    json!({
        "client_id": &oauth.client_id,
        "callback_port": oauth.callback_port,
        "auth_server_metadata_url": &oauth.auth_server_metadata_url,
        "xaa": oauth.xaa,
    })
}

fn mcp_server_details_json(config: &McpServerConfig) -> Value {
    match config {
        McpServerConfig::Stdio(config) => json!({
            "command": &config.command,
            "args": &config.args,
            "env_keys": config.env.keys().cloned().collect::<Vec<_>>(),
            "tool_call_timeout_ms": config.tool_call_timeout_ms,
        }),
        McpServerConfig::Sse(config) | McpServerConfig::Http(config) => json!({
            "url": &config.url,
            "header_keys": config.headers.keys().cloned().collect::<Vec<_>>(),
            "headers_helper": &config.headers_helper,
            "oauth": mcp_oauth_json(config.oauth.as_ref()),
        }),
        McpServerConfig::Ws(config) => json!({
            "url": &config.url,
            "header_keys": config.headers.keys().cloned().collect::<Vec<_>>(),
            "headers_helper": &config.headers_helper,
        }),
        McpServerConfig::Sdk(config) => json!({
            "name": &config.name,
        }),
        McpServerConfig::ManagedProxy(config) => json!({
            "url": &config.url,
            "id": &config.id,
        }),
    }
}

fn mcp_server_json(name: &str, server: &ScopedMcpServerConfig) -> Value {
    json!({
        "name": name,
        "scope": config_source_json(server.scope),
        "transport": mcp_transport_json(&server.config),
        "summary": mcp_server_summary(&server.config),
        "details": mcp_server_details_json(&server.config),
    })
}
