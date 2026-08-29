//! Build-time tooling for deterministic APP bundle assembly.

use std::{env, process::ExitCode};

mod apps;
mod architecture;

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask apps: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    match arguments.as_slice() {
        [command, rest @ ..] if command == "architecture" => architecture::run_cli(rest),
        [command, action, rest @ ..] if command == "apps" && action == "assemble" => {
            apps::assembler::run_cli(rest)
        }
        _ => Err(
            "usage: cargo xtask <architecture|apps> ...; run `cargo xtask architecture help` for governance commands"
                .to_owned(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_removed_static_source_lock_commands() {
        for arguments in [
            vec!["apps", "sync", "--locked"],
            vec!["apps", "verify", "--locked"],
            vec!["apps", "update", "mfg", "--rev", "0"],
        ] {
            let error = run(arguments.into_iter().map(str::to_owned).collect())
                .expect_err("static source-lock command must be absent");
            assert!(error.starts_with("usage: cargo xtask"));
        }
    }
}
