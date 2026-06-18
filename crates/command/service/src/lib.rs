pub mod parser;

pub use command_contract::{
    command_projection, normalize_command_name, unified_command_registry, CommandActionTarget,
    CommandArgumentSchema, CommandCapabilityRequirement, CommandCategory, CommandDefinition,
    CommandDisplayHints, CommandKind, CommandManifestEntry, CommandProjection,
    CommandProjectionEntry, CommandRegistry, CommandSource, CommandSurface, SkillSlashDispatch,
    SlashCommand, SlashCommandParseError, SlashCommandSpec, SLASH_COMMAND_SPECS,
};

pub use parser::{
    classify_skills_slash_command, render_slash_command_help, render_slash_command_help_detail,
    render_slash_command_help_filtered, resume_supported_slash_commands, slash_command_specs,
    suggest_slash_commands, validate_slash_command_input,
};
