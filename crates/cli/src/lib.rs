use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CliSurfaceCommand {
    Tui,
    Gateway,
    Doctor,
    Config,
    Tool,
    Skill,
    Version,
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliSurfacePolicy {
    pub default_command: CliSurfaceCommand,
    pub tui_requires_full_build: bool,
    pub allowed_commands: Vec<CliSurfaceCommand>,
    pub forbidden_business_commands: Vec<String>,
}

impl CliSurfacePolicy {
    #[must_use]
    pub fn minimal() -> Self {
        Self {
            default_command: CliSurfaceCommand::Help,
            tui_requires_full_build: true,
            allowed_commands: vec![
                CliSurfaceCommand::Tui,
                CliSurfaceCommand::Gateway,
                CliSurfaceCommand::Doctor,
                CliSurfaceCommand::Config,
                CliSurfaceCommand::Tool,
                CliSurfaceCommand::Skill,
                CliSurfaceCommand::Version,
                CliSurfaceCommand::Help,
            ],
            forbidden_business_commands: [
                "run",
                "chat",
                "prompt",
                "daemon",
                "session",
                "memory",
                "matrix",
                "mfg",
                "mcp serve",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_surface_defaults_to_tui_and_blocks_business_commands() {
        let policy = CliSurfacePolicy::minimal();
        assert_eq!(policy.default_command, CliSurfaceCommand::Help);
        assert!(policy.tui_requires_full_build);
        assert!(policy
            .allowed_commands
            .contains(&CliSurfaceCommand::Gateway));
        assert!(policy
            .forbidden_business_commands
            .contains(&"daemon".to_string()));
        assert!(policy
            .forbidden_business_commands
            .contains(&"mcp serve".to_string()));
    }
}
