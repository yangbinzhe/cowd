

// Re-export skill manifest module
pub mod skill_manifest;
pub use skill_manifest::{
    get_config_vars, get_related_skills, get_skill_description, get_skill_name, get_tags,
    matches_platform, parse_skill_content, parse_skill_file, check_prerequisites,
    PrerequisitesCheck, SkillConditions, SkillConfigVar, SkillHermesMetadata, SkillManifest,
    SkillPrerequisites, Platform,
};

// Re-export skill tools module
pub mod skill_tools;
pub use skill_tools::{
    SkillCreateInput, SkillCreateOutput, SkillDeleteInput, SkillDeleteOutput,
    SkillEditInput, SkillEditOutput, SkillGenerateInput, SkillGenerateOutput,
    SkillLinkedFiles, SkillListInput, SkillListOutput, SkillMeta, SkillPrerequisitesStatus,
    SkillViewInput, SkillViewOutput, SkillManager,
};

// Re-export skill security module
pub mod skill_security;
pub use skill_security::{
    SecurityScanResult, SecurityStatus, SecurityFinding, Severity, FindingCategory,
    scan_skill_content, scan_skill_file,
};

// New split modules
pub mod specs;
pub mod parser;
pub mod handlers;

// Re-export public API from split modules
pub use specs::{
    CommandManifestEntry, CommandRegistry, CommandSource, SlashCommand, SlashCommandParseError,
    SlashCommandSpec, SkillSlashDispatch, SLASH_COMMAND_SPECS,
};
pub use parser::{
    render_slash_command_help, render_slash_command_help_filtered,
    render_slash_command_help_detail, suggest_slash_commands,
    handle_agents_slash_command, handle_agents_slash_command_json,
    handle_mcp_slash_command, handle_mcp_slash_command_json,
    handle_plugins_slash_command, handle_skills_slash_command, handle_skills_slash_command_json,
    classify_skills_slash_command, resolve_skill_invocation, resolve_skill_path,
    SlashCommandResult, slash_command_specs,
    resume_supported_slash_commands, validate_slash_command_input,
};
pub use handlers::handle_slash_command;

