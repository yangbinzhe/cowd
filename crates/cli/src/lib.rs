use std::process::ExitCode;

use serde::{Deserialize, Serialize};

const INTERNAL_DISPATCH: &str = "__cowd_internal";

/// 在公开 CLI 解析前分发 Cowd 的内部子进程角色。
///
/// 这些角色不会出现在帮助面中，但仍由操作系统以独立进程运行。
#[must_use]
#[doc(hidden)]
pub fn dispatch_internal_process(args: &[String]) -> Option<ExitCode> {
    if args.first().map(String::as_str) != Some(INTERNAL_DISPATCH) {
        return None;
    }
    let Some(role) = args.get(1).map(String::as_str) else {
        eprintln!("Cowd internal process role is required");
        return Some(ExitCode::from(64));
    };
    let role_args = args.get(2..).unwrap_or_default();
    Some(match role {
        "auth-broker" => auth_broker::internal_process_entry(role_args),
        "sandbox-launcher" => sandbox_launcher::internal_process_entry(role_args),
        _ => {
            eprintln!("unsupported Cowd internal process role: {role}");
            ExitCode::from(64)
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CliSurfaceCommand {
    Tui,
    Gateway,
    Doctor,
    Config,
    Auth,
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
                CliSurfaceCommand::Auth,
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
    fn public_arguments_do_not_enter_internal_dispatch() {
        assert_eq!(dispatch_internal_process(&["gateway".to_string()]), None);
    }

    #[test]
    fn unknown_internal_role_fails_closed() {
        assert_eq!(
            dispatch_internal_process(&[INTERNAL_DISPATCH.to_string(), "unknown".to_string()]),
            Some(ExitCode::from(64))
        );
    }

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
