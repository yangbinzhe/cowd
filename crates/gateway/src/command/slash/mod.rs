pub mod parser;
pub mod specs;

#[allow(unused_imports)]
pub use parser::{
    classify_skills_slash_command, render_slash_command_help, render_slash_command_help_detail,
    render_slash_command_help_filtered, resume_supported_slash_commands, slash_command_specs,
    suggest_slash_commands, validate_slash_command_input,
};

#[allow(unused_imports)]
pub use specs::{
    command_projection, is_executable_slash_command, is_gateway_dispatchable_slash_command,
    normalize_command_name, unified_command_registry, CommandActionTarget, CommandArgumentSchema,
    CommandCapabilityRequirement, CommandCategory, CommandDefinition, CommandDisplayHints,
    CommandKind, CommandManifestEntry, CommandProjection, CommandProjectionEntry, CommandRegistry,
    CommandSource, CommandSurface, SkillSlashDispatch, SlashCommand, SlashCommandParseError,
    SlashCommandSpec, NON_EXECUTABLE_SLASH_COMMANDS, SLASH_COMMAND_SPECS,
};
