use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandManifestEntry {
    pub name: String,
    pub source: CommandSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandSource {
    Builtin,
    InternalOnly,
    FeatureGated,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandRegistry {
    entries: Vec<CommandManifestEntry>,
    definitions: Vec<CommandDefinition>,
}

impl CommandRegistry {
    #[must_use]
    pub fn new(entries: Vec<CommandManifestEntry>) -> Self {
        Self {
            entries,
            definitions: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_definitions(definitions: Vec<CommandDefinition>) -> Self {
        Self {
            entries: Vec::new(),
            definitions,
        }
    }

    #[must_use]
    pub fn entries(&self) -> &[CommandManifestEntry] {
        &self.entries
    }

    #[must_use]
    pub fn definitions(&self) -> &[CommandDefinition] {
        &self.definitions
    }

    #[must_use]
    pub fn projection(&self, surface: CommandSurface) -> CommandProjection {
        CommandProjection::from_definitions(surface, &self.definitions)
    }

    #[must_use]
    pub fn find(&self, command: &str) -> Option<&CommandDefinition> {
        let normalized = normalize_command_name(command);
        self.definitions.iter().find(|definition| {
            definition.name == normalized
                || definition.aliases.iter().any(|alias| {
                    alias == &normalized || alias.trim_start_matches('/') == normalized
                })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SlashCommandSpec {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub summary: &'static str,
    pub argument_hint: Option<&'static str>,
    pub resume_supported: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommandKind {
    Slash,
    Palette,
    Keybind,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommandCategory {
    Session,
    Runtime,
    Config,
    Skills,
    Agents,
    Memory,
    Tools,
    Gateway,
    Workspace,
    Debug,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommandSurface {
    Tui,
    Webui,
    Cli,
    Gateway,
}

impl CommandSurface {
    #[must_use]
    pub fn parse(value: Option<&str>) -> Self {
        match value
            .unwrap_or("webui")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "tui" => Self::Tui,
            "cli" => Self::Cli,
            "gateway" => Self::Gateway,
            _ => Self::Webui,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandArgumentSchema {
    pub usage: String,
    pub hint: Option<String>,
    pub accepts_freeform: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandCapabilityRequirement {
    pub capability: String,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CommandActionTarget {
    Client { action: String },
    Route { path: String },
    Runtime { operation: String },
    Config { operation: String },
    Registry { operation: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandDisplayHints {
    pub label: String,
    pub detail: String,
    pub group: String,
    pub priority: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandDefinition {
    pub id: String,
    pub name: String,
    pub aliases: Vec<String>,
    pub summary: String,
    pub kind: CommandKind,
    pub category: CommandCategory,
    pub surfaces: Vec<CommandSurface>,
    pub arguments: CommandArgumentSchema,
    pub capabilities: Vec<CommandCapabilityRequirement>,
    pub action: CommandActionTarget,
    pub display: CommandDisplayHints,
    pub resume_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandProjectionEntry {
    pub id: String,
    pub name: String,
    pub aliases: Vec<String>,
    pub description: String,
    pub usage: String,
    pub action: CommandActionTarget,
    pub category: CommandCategory,
    pub surface: CommandSurface,
    pub display: CommandDisplayHints,
    pub resume_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandProjection {
    pub surface: CommandSurface,
    pub commands: Vec<CommandProjectionEntry>,
}

impl CommandProjection {
    #[must_use]
    pub fn from_definitions(surface: CommandSurface, definitions: &[CommandDefinition]) -> Self {
        let commands = definitions
            .iter()
            .filter(|definition| definition.surfaces.contains(&surface))
            .map(|definition| CommandProjectionEntry {
                id: definition.id.clone(),
                name: definition.name.clone(),
                aliases: definition.aliases.clone(),
                description: definition.summary.clone(),
                usage: definition.arguments.usage.clone(),
                action: definition.action.clone(),
                category: definition.category,
                surface,
                display: definition.display.clone(),
                resume_supported: definition.resume_supported,
            })
            .collect();
        Self { surface, commands }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillSlashDispatch {
    Local,
    Invoke(String),
}

pub const SLASH_COMMAND_SPECS: &[SlashCommandSpec] = &[
    SlashCommandSpec {
        name: "help",
        aliases: &[],
        summary: "Show available slash commands",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "status",
        aliases: &[],
        summary: "Show current session status",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "sandbox",
        aliases: &[],
        summary: "Show sandbox isolation status",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "compact",
        aliases: &[],
        summary: "Compact the active Gateway session through a semantic checkpoint",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "model",
        aliases: &[],
        summary: "Show or switch the active model",
        argument_hint: Some("[model]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "permissions",
        aliases: &[],
        summary: "Show or switch the active permission mode",
        argument_hint: Some("[read-only|workspace-write|danger-full-access|yolo]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "clear",
        aliases: &[],
        summary: "Start a fresh local session",
        argument_hint: Some("[--confirm]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "cost",
        aliases: &[],
        summary: "Show cumulative token usage for this session",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "resume",
        aliases: &[],
        summary: "Load a saved session into the TUI",
        argument_hint: Some("<session-id|latest>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "config",
        aliases: &[],
        summary: "Inspect Cowd instruction files or merged sections",
        argument_hint: Some("[env|hooks|model|plugins]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "setup",
        aliases: &[],
        summary: "Check local setup and show the next required action",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "mcp",
        aliases: &[],
        summary: "Inspect configured MCP servers",
        argument_hint: Some("[list|show <server>|help]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "memory",
        aliases: &[],
        summary: "Inspect loaded Cowd instruction memory files",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "handoff",
        aliases: &["transfer", "handover"],
        summary: "Save session state for cross-session transfer",
        argument_hint: Some("[save|load|list|resume] [session-id]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "init",
        aliases: &[],
        summary: "Create a starter CLAUDE.md for this repo",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "diff",
        aliases: &[],
        summary: "Show git diff for current workspace changes",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "version",
        aliases: &[],
        summary: "Show CLI version and build information",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "bughunter",
        aliases: &[],
        summary: "Inspect the codebase for likely bugs",
        argument_hint: Some("[scope]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "commit",
        aliases: &[],
        summary: "Generate a commit message and create a git commit",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "pr",
        aliases: &[],
        summary: "Draft or create a pull request from the conversation",
        argument_hint: Some("[context]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "issue",
        aliases: &[],
        summary: "Draft or create a GitHub issue from the conversation",
        argument_hint: Some("[context]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "ultraplan",
        aliases: &[],
        summary: "Run a deep planning prompt with multi-step reasoning",
        argument_hint: Some("[task]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "teleport",
        aliases: &[],
        summary: "Jump to a file or symbol by searching the workspace",
        argument_hint: Some("<symbol-or-path>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "debug-tool-call",
        aliases: &[],
        summary: "Replay the last tool call with debug details",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "export",
        aliases: &[],
        summary: "Export the current conversation to a file",
        argument_hint: Some("[file]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "session",
        aliases: &[],
        summary: "List, switch, fork, or delete managed local sessions",
        argument_hint: Some(
            "[list|switch <session-id>|fork [branch-name]|delete <session-id> [--force]]",
        ),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "plugin",
        aliases: &["plugins", "marketplace"],
        summary: "Manage Claw Code plugins",
        argument_hint: Some(
            "[list|install <path>|enable <name>|disable <name>|uninstall <id>|update <id>]",
        ),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "agents",
        aliases: &[],
        summary: "List configured agents",
        argument_hint: Some("[list|help]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "skills",
        aliases: &["skill"],
        summary: "List, view, install, or invoke available skills",
        argument_hint: Some("[list|view <name>|install <path>|help|<skill> [args]]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "doctor",
        aliases: &[],
        summary: "Diagnose setup issues and environment health",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "plan",
        aliases: &[],
        summary: "Toggle or inspect planning mode",
        argument_hint: Some("[on|off]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "review",
        aliases: &[],
        summary: "Run a code review on current changes",
        argument_hint: Some("[scope]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "tasks",
        aliases: &[],
        summary: "List and manage background tasks",
        argument_hint: Some("[list|start [--yolo] <objective>|cancel <id>|complete <id>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "approvals",
        aliases: &["approval"],
        summary: "List and answer daemon approval requests",
        argument_hint: Some("[list|approve <id>|reject <id>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "cross-plane",
        aliases: &["xplane"],
        summary: "Inspect and execute cross-channel daemon actions",
        argument_hint: Some("[summary|preflight <json>|execute <json>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "theme",
        aliases: &[],
        summary: "Switch the terminal color theme",
        argument_hint: Some("[theme-name]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "vim",
        aliases: &[],
        summary: "Toggle vim keybinding mode",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "voice",
        aliases: &[],
        summary: "Toggle voice input mode",
        argument_hint: Some("[on|off]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "upgrade",
        aliases: &[],
        summary: "Check for and install CLI updates",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "usage",
        aliases: &[],
        summary: "Show detailed API usage statistics",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "stats",
        aliases: &[],
        summary: "Show workspace and session statistics",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "rename",
        aliases: &[],
        summary: "Rename the current session",
        argument_hint: Some("<name>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "copy",
        aliases: &[],
        summary: "Copy conversation or output to clipboard",
        argument_hint: Some("[last|all]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "share",
        aliases: &[],
        summary: "Share the current conversation",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "feedback",
        aliases: &[],
        summary: "Submit feedback about the current session",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "hooks",
        aliases: &[],
        summary: "List and manage lifecycle hooks",
        argument_hint: Some("[list|run <hook>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "files",
        aliases: &[],
        summary: "List files in the current context window",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "context",
        aliases: &[],
        summary: "Inspect or manage the conversation context",
        argument_hint: Some("[show|clear]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "color",
        aliases: &[],
        summary: "Configure terminal color settings",
        argument_hint: Some("[scheme]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "effort",
        aliases: &[],
        summary: "Set the effort level for responses",
        argument_hint: Some("[low|medium|high]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "fast",
        aliases: &[],
        summary: "Toggle fast/concise response mode",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "exit",
        aliases: &[],
        summary: "Exit the TUI session",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "branch",
        aliases: &[],
        summary: "Create or switch git branches",
        argument_hint: Some("[name]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "rewind",
        aliases: &[],
        summary: "Rewind the conversation to a previous state",
        argument_hint: Some("[steps]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "summary",
        aliases: &[],
        summary: "Generate a summary of the conversation",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "desktop",
        aliases: &[],
        summary: "Open or manage the desktop app integration",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "ide",
        aliases: &[],
        summary: "Open or configure IDE integration",
        argument_hint: Some("[vscode|cursor]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "tag",
        aliases: &[],
        summary: "Tag the current conversation point",
        argument_hint: Some("[label]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "brief",
        aliases: &[],
        summary: "Toggle brief output mode",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "advisor",
        aliases: &[],
        summary: "Toggle advisor mode for guidance-only responses",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "stickers",
        aliases: &[],
        summary: "Browse and manage sticker packs",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "insights",
        aliases: &[],
        summary: "Show AI-generated insights about the session",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "thinkback",
        aliases: &[],
        summary: "Replay the thinking process of the last response",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "release-notes",
        aliases: &[],
        summary: "Generate release notes from recent changes",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "security-review",
        aliases: &[],
        summary: "Run a security review on the codebase",
        argument_hint: Some("[scope]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "keybindings",
        aliases: &[],
        summary: "Show or configure keyboard shortcuts",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "privacy-settings",
        aliases: &[],
        summary: "View or modify privacy settings",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "output-style",
        aliases: &[],
        summary: "Switch output formatting style",
        argument_hint: Some("[style]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "add-dir",
        aliases: &[],
        summary: "Add an additional directory to the context",
        argument_hint: Some("<path>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "allowed-tools",
        aliases: &[],
        summary: "Show or modify the allowed tools list",
        argument_hint: Some("[add|remove|list] [tool]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "api-key",
        aliases: &[],
        summary: "Show or set the provider API key",
        argument_hint: Some("[key]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "approve",
        aliases: &["yes", "y"],
        summary: "Approve a pending tool execution",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "deny",
        aliases: &["no", "n"],
        summary: "Deny a pending tool execution",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "undo",
        aliases: &[],
        summary: "Undo the last file write or edit",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "stop",
        aliases: &[],
        summary: "Stop the current generation",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "retry",
        aliases: &[],
        summary: "Retry the last failed message",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "paste",
        aliases: &[],
        summary: "Paste clipboard content as input",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "screenshot",
        aliases: &[],
        summary: "Take a screenshot and add to conversation",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "image",
        aliases: &[],
        summary: "Add an image file to the conversation",
        argument_hint: Some("<path>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "terminal-setup",
        aliases: &[],
        summary: "Configure terminal integration settings",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "search",
        aliases: &[],
        summary: "Search files in the workspace",
        argument_hint: Some("<query>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "listen",
        aliases: &[],
        summary: "Listen for voice input",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "speak",
        aliases: &[],
        summary: "Read the last response aloud",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "language",
        aliases: &[],
        summary: "Set the interface language",
        argument_hint: Some("[language]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "profile",
        aliases: &[],
        summary: "Show or switch user profile",
        argument_hint: Some("[name]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "max-tokens",
        aliases: &[],
        summary: "Show or set the max output tokens",
        argument_hint: Some("[count]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "temperature",
        aliases: &[],
        summary: "Show or set the sampling temperature",
        argument_hint: Some("[value]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "system-prompt",
        aliases: &[],
        summary: "Show the active system prompt",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "tool-details",
        aliases: &[],
        summary: "Show detailed info about a specific tool",
        argument_hint: Some("<tool-name>"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "format",
        aliases: &[],
        summary: "Format the last response in a different style",
        argument_hint: Some("[markdown|plain|json]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "pin",
        aliases: &[],
        summary: "Pin a message to persist across compaction",
        argument_hint: Some("[message-index]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "unpin",
        aliases: &[],
        summary: "Unpin a previously pinned message",
        argument_hint: Some("[message-index]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "bookmarks",
        aliases: &[],
        summary: "List or manage conversation bookmarks",
        argument_hint: Some("[add|remove|list]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "workspace",
        aliases: &["cwd"],
        summary: "Show or change the working directory",
        argument_hint: Some("[path]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "history",
        aliases: &[],
        summary: "Show conversation history summary",
        argument_hint: Some("[count]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "tokens",
        aliases: &[],
        summary: "Show token count for the current conversation",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "cache",
        aliases: &[],
        summary: "Show prompt cache statistics",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "providers",
        aliases: &[],
        summary: "List available model providers",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "notifications",
        aliases: &[],
        summary: "Show or configure notification settings",
        argument_hint: Some("[on|off|status]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "changelog",
        aliases: &[],
        summary: "Show recent changes to the codebase",
        argument_hint: Some("[count]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "test",
        aliases: &[],
        summary: "Run tests for the current project",
        argument_hint: Some("[filter]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "lint",
        aliases: &[],
        summary: "Run linting for the current project",
        argument_hint: Some("[filter]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "build",
        aliases: &[],
        summary: "Build the current project",
        argument_hint: Some("[target]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "run",
        aliases: &[],
        summary: "Run a command in the project context",
        argument_hint: Some("<command>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "git",
        aliases: &[],
        summary: "Run a git command in the workspace",
        argument_hint: Some("<subcommand>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "stash",
        aliases: &[],
        summary: "Stash or unstash workspace changes",
        argument_hint: Some("[pop|list|apply]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "blame",
        aliases: &[],
        summary: "Show git blame for a file",
        argument_hint: Some("<file> [line]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "log",
        aliases: &[],
        summary: "Show git log for the workspace",
        argument_hint: Some("[count]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "cron",
        aliases: &[],
        summary: "Manage scheduled tasks",
        argument_hint: Some("[list|add|remove]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "team",
        aliases: &[],
        summary: "Manage agent teams",
        argument_hint: Some("[list|create|delete]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "benchmark",
        aliases: &[],
        summary: "Run performance benchmarks",
        argument_hint: Some("[suite]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "migrate",
        aliases: &[],
        summary: "Run pending data migrations",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "reset",
        aliases: &[],
        summary: "Reset configuration to defaults",
        argument_hint: Some("[section]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "telemetry",
        aliases: &[],
        summary: "Show or configure telemetry settings",
        argument_hint: Some("[on|off|status]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "env",
        aliases: &[],
        summary: "Show environment variables visible to tools",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "project",
        aliases: &[],
        summary: "Show project detection info",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "templates",
        aliases: &[],
        summary: "List or apply prompt templates",
        argument_hint: Some("[list|apply <name>]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "explain",
        aliases: &[],
        summary: "Explain a file or code snippet",
        argument_hint: Some("<path> [line-range]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "refactor",
        aliases: &[],
        summary: "Suggest refactoring for a file or function",
        argument_hint: Some("<path> [scope]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "docs",
        aliases: &[],
        summary: "Generate or show documentation",
        argument_hint: Some("[path]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "fix",
        aliases: &[],
        summary: "Fix errors in a file or project",
        argument_hint: Some("[path]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "perf",
        aliases: &[],
        summary: "Analyze performance of a function or file",
        argument_hint: Some("<path>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "chat",
        aliases: &[],
        summary: "Switch to free-form chat mode",
        argument_hint: None,
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "focus",
        aliases: &[],
        summary: "Focus context on specific files or directories",
        argument_hint: Some("<path> [path...]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "unfocus",
        aliases: &[],
        summary: "Remove focus from files or directories",
        argument_hint: Some("[path...]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "web",
        aliases: &[],
        summary: "Fetch and summarize a web page",
        argument_hint: Some("<url>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "map",
        aliases: &[],
        summary: "Show a visual map of the codebase structure",
        argument_hint: Some("[depth]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "symbols",
        aliases: &[],
        summary: "List symbols (functions, classes, etc.) in a file",
        argument_hint: Some("<path>"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "references",
        aliases: &[],
        summary: "Find all references to a symbol",
        argument_hint: Some("<symbol>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "definition",
        aliases: &[],
        summary: "Go to the definition of a symbol",
        argument_hint: Some("<symbol>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "hover",
        aliases: &[],
        summary: "Show hover information for a symbol",
        argument_hint: Some("<symbol>"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "diagnostics",
        aliases: &[],
        summary: "Show LSP diagnostics for a file",
        argument_hint: Some("[path]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "autofix",
        aliases: &[],
        summary: "Auto-fix all fixable diagnostics",
        argument_hint: Some("[path]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "multi",
        aliases: &[],
        summary: "Execute multiple slash commands in sequence",
        argument_hint: Some("<commands>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "macro",
        aliases: &[],
        summary: "Record or replay command macros",
        argument_hint: Some("[record|stop|play <name>]"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "alias",
        aliases: &[],
        summary: "Create a command alias",
        argument_hint: Some("<name> <command>"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "parallel",
        aliases: &[],
        summary: "Run commands in parallel subagents",
        argument_hint: Some("<count> <prompt>"),
        resume_supported: false,
    },
    SlashCommandSpec {
        name: "agent",
        aliases: &[],
        summary: "Manage sub-agents and spawned sessions",
        argument_hint: Some("[list|spawn|kill|profile [id]]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "subagent",
        aliases: &[],
        summary: "Control active subagent execution",
        argument_hint: Some("[list|steer <target> <msg>|kill <id>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "reasoning",
        aliases: &[],
        summary: "Toggle extended reasoning mode",
        argument_hint: Some("[on|off|stream]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "budget",
        aliases: &[],
        summary: "Show or set token budget limits",
        argument_hint: Some("[show|set <limit>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "rate-limit",
        aliases: &[],
        summary: "Configure API rate limiting",
        argument_hint: Some("[status|set <rpm>]"),
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "metrics",
        aliases: &[],
        summary: "Show performance and usage metrics",
        argument_hint: None,
        resume_supported: true,
    },
    SlashCommandSpec {
        name: "solve",
        aliases: &[],
        summary: "Joint problem solving — collaborate with agents to solve a problem",
        argument_hint: Some("\"problem description\""),
        resume_supported: false,
    },
];

/// Commands retained as parser vocabulary but not backed by an executable
/// Gateway or Surface dispatch path in this build.
pub const NON_EXECUTABLE_SLASH_COMMANDS: &[&str] = &[
    "login",
    "logout",
    "upgrade",
    "share",
    "feedback",
    "fast",
    "exit",
    "insights",
    "thinkback",
    "release-notes",
    "security-review",
    "keybindings",
    "privacy-settings",
    "plan",
    "theme",
    "usage",
    "rename",
    "copy",
    "hooks",
    "color",
    "effort",
    "rewind",
    "ide",
    "tag",
    "output-style",
    "add-dir",
    "allowed-tools",
    "bookmarks",
    "workspace",
    "reasoning",
    "budget",
    "rate-limit",
    "changelog",
    "diagnostics",
    "metrics",
    "tool-details",
    "focus",
    "unfocus",
    "pin",
    "unpin",
    "language",
    "profile",
    "max-tokens",
    "temperature",
    "system-prompt",
    "notifications",
    "telemetry",
    "env",
    "project",
    "terminal-setup",
    "api-key",
    "reset",
    "undo",
    "stop",
    "retry",
    "paste",
    "screenshot",
    "image",
    "cron",
    "team",
    "benchmark",
    "migrate",
    "templates",
    "chat",
    "map",
    "symbols",
    "references",
    "definition",
    "hover",
    "autofix",
    "multi",
    "macro",
    "alias",
    "parallel",
    "subagent",
    "agent",
];

#[must_use]
pub fn is_executable_slash_command(name: &str) -> bool {
    let name = name.trim().trim_start_matches('/');
    !NON_EXECUTABLE_SLASH_COMMANDS.contains(&name)
}

#[must_use]
pub fn unified_command_registry() -> CommandRegistry {
    let mut definitions = SLASH_COMMAND_SPECS
        .iter()
        .filter(|spec| is_executable_slash_command(spec.name))
        .enumerate()
        .map(|(index, spec)| command_definition_from_slash(index as u16, spec))
        .collect::<Vec<_>>();
    definitions.extend(palette_command_definitions());
    CommandRegistry::from_definitions(definitions)
}

#[must_use]
pub fn command_projection(surface: CommandSurface) -> CommandProjection {
    unified_command_registry().projection(surface)
}

#[must_use]
pub fn normalize_command_name(command: &str) -> String {
    let trimmed = command.trim();
    let first = trimmed.split_whitespace().next().unwrap_or(trimmed);
    if first.starts_with('/') {
        first.to_string()
    } else {
        format!("/{first}")
    }
}

fn command_definition_from_slash(priority: u16, spec: &SlashCommandSpec) -> CommandDefinition {
    let category = slash_category(spec.name);
    let name = format!("/{}", spec.name);
    let usage = match spec.argument_hint {
        Some(hint) => format!("{name} {hint}"),
        None => name.clone(),
    };
    CommandDefinition {
        id: format!("slash.{}", spec.name),
        name: name.clone(),
        aliases: spec
            .aliases
            .iter()
            .map(|alias| format!("/{alias}"))
            .collect(),
        summary: spec.summary.to_string(),
        kind: CommandKind::Slash,
        category,
        surfaces: vec![
            CommandSurface::Tui,
            CommandSurface::Webui,
            CommandSurface::Gateway,
        ],
        arguments: CommandArgumentSchema {
            usage,
            hint: spec.argument_hint.map(ToOwned::to_owned),
            accepts_freeform: spec.argument_hint.is_some(),
        },
        capabilities: capability_requirements_for(category),
        action: action_target_for_slash(spec.name),
        display: CommandDisplayHints {
            label: name,
            detail: spec.summary.to_string(),
            group: format!("{category:?}"),
            priority,
        },
        resume_supported: spec.resume_supported,
    }
}

fn palette_command_definitions() -> Vec<CommandDefinition> {
    [
        (
            "palette.toggle-help",
            "Toggle Help",
            "Show or hide the help overlay",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "toggle-help".into(),
            },
        ),
        (
            "palette.toggle-theme",
            "Toggle Theme",
            "Switch between light and dark themes",
            CommandCategory::Config,
            CommandActionTarget::Client {
                action: "toggle-theme".into(),
            },
        ),
        (
            "palette.search",
            "Search",
            "Activate incremental find / search mode",
            CommandCategory::Workspace,
            CommandActionTarget::Client {
                action: "search".into(),
            },
        ),
        (
            "palette.copy",
            "Copy",
            "Copy the focused selection to clipboard",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "copy".into(),
            },
        ),
        (
            "palette.next-panel",
            "Next Panel",
            "Focus the next panel in the layout",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "next-panel".into(),
            },
        ),
        (
            "palette.previous-panel",
            "Previous Panel",
            "Focus the previous panel in the layout",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "previous-panel".into(),
            },
        ),
        (
            "palette.submit-input",
            "Submit Input",
            "Send the current input buffer as a message",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "submit-input".into(),
            },
        ),
        (
            "palette.refresh-config-status",
            "Refresh Config Status",
            "Refresh effective config, provider projection, and Gateway hot-reload status",
            CommandCategory::Config,
            CommandActionTarget::Client {
                action: "refresh-config-status".into(),
            },
        ),
        (
            "palette.open-runtime",
            "/runtime",
            "Open runtime status, tasks, approvals, network, and connector health",
            CommandCategory::Runtime,
            CommandActionTarget::Client {
                action: "slash:runtime".into(),
            },
        ),
        (
            "palette.open-activity",
            "/activity",
            "Open the recent execution stream beside the main conversation",
            CommandCategory::Runtime,
            CommandActionTarget::Client {
                action: "slash:activity".into(),
            },
        ),
        (
            "palette.open-tools",
            "/tools",
            "Operate tool registry, cache, checkpoints, ledger, and risk checks",
            CommandCategory::Tools,
            CommandActionTarget::Client {
                action: "slash:tools".into(),
            },
        ),
        (
            "palette.open-files",
            "/files",
            "Browse workspace files in the sidebar",
            CommandCategory::Workspace,
            CommandActionTarget::Client {
                action: "slash:files".into(),
            },
        ),
        (
            "palette.open-sessions",
            "/sessions",
            "Browse and switch recent sessions",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "slash:sessions".into(),
            },
        ),
        (
            "palette.open-gateway",
            "/gateway",
            "Inspect external connector and gateway state",
            CommandCategory::Gateway,
            CommandActionTarget::Client {
                action: "slash:gateway".into(),
            },
        ),
        (
            "palette.open-diff",
            "/diff",
            "Review current code changes in the on-demand topic panel",
            CommandCategory::Workspace,
            CommandActionTarget::Client {
                action: "slash:diff".into(),
            },
        ),
        (
            "palette.focus-input",
            "/focus input",
            "Return keyboard focus to the prompt input",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "slash:focus input".into(),
            },
        ),
        (
            "palette.focus-chat",
            "/focus chat",
            "Return keyboard focus to the main conversation",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "slash:focus chat".into(),
            },
        ),
        (
            "palette.cancel",
            "Cancel",
            "Cancel the current operation",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "cancel".into(),
            },
        ),
        (
            "palette.quit",
            "Quit",
            "Exit the application",
            CommandCategory::Session,
            CommandActionTarget::Client {
                action: "quit".into(),
            },
        ),
    ]
    .into_iter()
    .enumerate()
    .map(
        |(index, (id, name, summary, category, action))| CommandDefinition {
            id: id.to_string(),
            name: name.to_string(),
            aliases: Vec::new(),
            summary: summary.to_string(),
            kind: CommandKind::Palette,
            category,
            surfaces: vec![CommandSurface::Tui],
            arguments: CommandArgumentSchema {
                usage: name.to_string(),
                hint: None,
                accepts_freeform: false,
            },
            capabilities: capability_requirements_for(category),
            action,
            display: CommandDisplayHints {
                label: name.to_string(),
                detail: summary.to_string(),
                group: format!("{category:?}"),
                priority: 10_000 + index as u16,
            },
            resume_supported: true,
        },
    )
    .collect()
}

fn slash_category(name: &str) -> CommandCategory {
    match name {
        "help" | "status" | "cost" | "resume" | "session" | "version" | "usage" | "stats"
        | "rename" | "clear" | "compact" | "history" | "tokens" | "cache" | "exit" | "summary"
        | "tag" | "thinkback" | "copy" | "share" | "feedback" | "rewind" | "pin" | "unpin"
        | "bookmarks" | "retry" | "stop" | "undo" | "plan" => CommandCategory::Session,
        "model" | "permissions" | "config" | "theme" | "vim" | "voice" | "color" | "effort"
        | "fast" | "brief" | "output-style" | "keybindings" | "privacy-settings" | "language"
        | "profile" | "max-tokens" | "temperature" | "system-prompt" | "api-key"
        | "terminal-setup" | "notifications" | "telemetry" | "providers" | "env" | "project"
        | "reasoning" | "budget" | "rate-limit" | "reset" | "ide" | "desktop" | "upgrade" => {
            CommandCategory::Config
        }
        "memory" => CommandCategory::Memory,
        "agents" | "agent-profile" | "solve" | "discuss" | "branch" | "review" | "advisor" => {
            CommandCategory::Agents
        }
        "skills" | "skill" | "plugin" | "plugins" | "marketplace" => CommandCategory::Skills,
        "workspace" | "cwd" | "files" | "focus" | "unfocus" | "add-dir" | "search" | "diff"
        | "teleport" => CommandCategory::Workspace,
        "tasks" | "approvals" | "approval" | "approve" | "deny" | "runtime" => {
            CommandCategory::Runtime
        }
        "gateway" | "cross-plane" | "xplane" | "mcp" => CommandCategory::Gateway,
        "debug-tool-call" | "doctor" | "sandbox" | "diagnostics" | "tool-details" | "changelog"
        | "metrics" => CommandCategory::Debug,
        _ => CommandCategory::Tools,
    }
}

fn capability_requirements_for(category: CommandCategory) -> Vec<CommandCapabilityRequirement> {
    let capability = match category {
        CommandCategory::Session => "session",
        CommandCategory::Runtime => "runtime",
        CommandCategory::Config => "config",
        CommandCategory::Skills => "skills",
        CommandCategory::Agents => "agents",
        CommandCategory::Memory => "memory",
        CommandCategory::Tools => "tools",
        CommandCategory::Gateway => "gateway",
        CommandCategory::Workspace => "workspace",
        CommandCategory::Debug => "diagnostics",
    };
    vec![CommandCapabilityRequirement {
        capability: capability.to_string(),
        required: true,
    }]
}

fn action_target_for_slash(name: &str) -> CommandActionTarget {
    match name {
        "status" => CommandActionTarget::Runtime {
            operation: "runtime.status".into(),
        },
        "compact" => CommandActionTarget::Runtime {
            operation: "session.compact".into(),
        },
        "permissions" => CommandActionTarget::Runtime {
            operation: "session.permissions".into(),
        },
        "model" => CommandActionTarget::Config {
            operation: "config.model".into(),
        },
        "providers" => CommandActionTarget::Config {
            operation: "config.providers".into(),
        },
        "skills" => CommandActionTarget::Registry {
            operation: "skills.registry".into(),
        },
        "agents" => CommandActionTarget::Registry {
            operation: "agents.registry".into(),
        },
        "memory" => CommandActionTarget::Route {
            path: "/api/memory".into(),
        },
        "workspace" | "files" => CommandActionTarget::Route {
            path: "/api/workspace".into(),
        },
        "tasks" => CommandActionTarget::Runtime {
            operation: "task.manage".into(),
        },
        "approvals" | "approve" | "deny" => CommandActionTarget::Runtime {
            operation: "approval.respond".into(),
        },
        "gateway" => CommandActionTarget::Route {
            path: "/api/runtime/status".into(),
        },
        other => CommandActionTarget::Client {
            action: format!("slash:{other}"),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlashCommand {
    Help,
    Status,
    Sandbox,
    Compact,
    Bughunter {
        scope: Option<String>,
    },
    Commit,
    Pr {
        context: Option<String>,
    },
    Issue {
        context: Option<String>,
    },
    Ultraplan {
        task: Option<String>,
    },
    Teleport {
        target: Option<String>,
    },
    DebugToolCall,
    Model {
        model: Option<String>,
    },
    Permissions {
        mode: Option<String>,
    },
    Clear {
        confirm: bool,
    },
    Cost,
    Resume {
        session_path: Option<String>,
    },
    Config {
        section: Option<String>,
    },
    Setup,
    Mcp {
        action: Option<String>,
        target: Option<String>,
    },
    Memory,
    Init,
    Diff,
    Version,
    Export {
        path: Option<String>,
    },
    Session {
        action: Option<String>,
        target: Option<String>,
    },
    Plugins {
        action: Option<String>,
        target: Option<String>,
    },
    Agents {
        args: Option<String>,
    },
    Skills {
        args: Option<String>,
    },
    Doctor,
    Login,
    Logout,
    Vim,
    Upgrade,
    Stats,
    Share,
    Feedback,
    Files,
    Fast,
    Exit,
    Summary,
    Desktop,
    Brief,
    Advisor,
    Stickers,
    Insights,
    Thinkback,
    ReleaseNotes,
    SecurityReview,
    Keybindings,
    PrivacySettings,
    Plan {
        mode: Option<String>,
    },
    Review {
        scope: Option<String>,
    },
    Tasks {
        args: Option<String>,
    },
    Approvals {
        args: Option<String>,
    },
    CrossPlane {
        args: Option<String>,
    },
    Theme {
        name: Option<String>,
    },
    Voice {
        mode: Option<String>,
    },
    Usage {
        scope: Option<String>,
    },
    Rename {
        name: Option<String>,
    },
    Copy {
        target: Option<String>,
    },
    Hooks {
        args: Option<String>,
    },
    Context {
        action: Option<String>,
    },
    Color {
        scheme: Option<String>,
    },
    Effort {
        level: Option<String>,
    },
    Branch {
        name: Option<String>,
    },
    Rewind {
        steps: Option<String>,
    },
    Ide {
        target: Option<String>,
    },
    Tag {
        label: Option<String>,
    },
    OutputStyle {
        style: Option<String>,
    },
    AddDir {
        path: Option<String>,
    },
    History {
        count: Option<String>,
    },
    Handoff {
        action: Option<String>,
        session_id: Option<String>,
    },
    Closet {
        topic: Option<String>,
    },
    Retry,
    Undo,
    NewSession,
    Title {
        name: Option<String>,
    },
    Compress,
    State,
    SubAgent {
        role: Option<String>,
        task: Option<String>,
    },
    Pipeline {
        task: Option<String>,
    },
    Solve {
        problem: Option<String>,
    },
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlashCommandParseError {
    message: String,
}

impl SlashCommandParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SlashCommandParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SlashCommandParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_registry_excludes_unwired_roadmap_commands() {
        let registry = unified_command_registry();
        assert!(registry.find("/upgrade").is_none());
        assert!(registry.find("/parallel").is_none());
        assert!(registry.find("/agent").is_none());
    }

    #[test]
    fn executable_registry_preserves_real_task_and_workspace_entries() {
        let registry = unified_command_registry();
        assert!(registry.find("/tasks").is_some());
        assert!(registry.find("/files").is_some());
        assert!(registry.find("/status").is_some());
    }

    #[test]
    fn every_projected_slash_command_is_executable() {
        for surface in [
            CommandSurface::Tui,
            CommandSurface::Webui,
            CommandSurface::Gateway,
        ] {
            let projection = command_projection(surface);
            assert!(projection.commands.iter().all(|command| {
                is_executable_slash_command(command.name.trim_start_matches('/'))
            }));
        }
    }
}
