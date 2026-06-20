pub use slash_contract::{
    classify_skills_slash_command, command_projection, normalize_command_name,
    render_slash_command_help, render_slash_command_help_detail,
    render_slash_command_help_filtered, resume_supported_slash_commands, slash_command_specs,
    suggest_slash_commands, unified_command_registry, validate_slash_command_input,
    CommandActionTarget, CommandArgumentSchema, CommandCapabilityRequirement, CommandCategory,
    CommandDefinition, CommandDisplayHints, CommandKind, CommandManifestEntry, CommandProjection,
    CommandProjectionEntry, CommandRegistry, CommandSource, CommandSurface, SkillSlashDispatch,
    SlashCommand, SlashCommandParseError, SlashCommandSpec, SLASH_COMMAND_SPECS,
};
