use std::process::ExitCode;

use sandbox_launcher::{shell_command, SandboxLaunchSpec};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(workspace) = args.next() else {
        eprintln!("usage: cowd-sandbox-launcher <absolute-workspace> <shell-command>");
        return ExitCode::from(64);
    };
    let command = args.collect::<Vec<_>>().join(" ");
    if command.trim().is_empty() {
        eprintln!("shell command is required");
        return ExitCode::from(64);
    }
    let spec = SandboxLaunchSpec::workspace(workspace);
    match shell_command(&command, &spec) {
        Ok(prepared) => match prepared.into_command().status() {
            Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
            Err(error) => {
                eprintln!("failed to launch sandbox: {error}");
                ExitCode::from(1)
            }
        },
        Err(error) => {
            eprintln!("sandbox unavailable: {error}");
            ExitCode::from(1)
        }
    }
}
