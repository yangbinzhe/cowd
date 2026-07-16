#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

fn main() -> std::process::ExitCode {
    sandbox_launcher::register_cowd_process_host();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(status) = cli::dispatch_internal_process(&args) {
        return status;
    }
    let first_arg = args.first().map(String::as_str);

    if should_open_tui(&args) || matches!(first_arg, Some("tui")) {
        return open_tui();
    }

    match first_arg {
        Some("gateway") => gateway::backend_entry(),
        _ => gateway::static_entry(),
    }
    std::process::ExitCode::SUCCESS
}

#[cfg(feature = "tui-surface")]
fn open_tui() -> std::process::ExitCode {
    match tui::terminal_entry() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("TUI failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(feature = "tui-surface"))]
fn open_tui() -> std::process::ExitCode {
    eprintln!(
        "TUI surface is not built in this binary; rebuild cowd with `--features full` or install a full build."
    );
    std::process::ExitCode::from(2)
}

#[cfg(feature = "tui-surface")]
fn should_open_tui(args: &[String]) -> bool {
    let first_arg = args.first().map(String::as_str);
    if args.iter().any(|arg| arg.trim_start().starts_with('/')) {
        return false;
    }

    match first_arg {
        None => true,
        Some(
            "--resume"
            | "--session"
            | "--session-id"
            | "-s"
            | "--model"
            | "-m"
            | "--yolo"
            | "--dangerously-skip-permissions"
            | "--danger-full-access",
        ) => true,
        Some(arg)
            if arg.starts_with("--resume=")
                || arg.starts_with("--session=")
                || arg.starts_with("--session-id=")
                || arg.starts_with("--model=") =>
        {
            true
        }
        _ => false,
    }
}

#[cfg(not(feature = "tui-surface"))]
fn should_open_tui(args: &[String]) -> bool {
    let first_arg = args.first().map(String::as_str);
    if args.iter().any(|arg| arg.trim_start().starts_with('/')) {
        return false;
    }

    match first_arg {
        Some(
            "--resume"
            | "--session"
            | "--session-id"
            | "-s"
            | "--model"
            | "-m"
            | "--yolo"
            | "--dangerously-skip-permissions"
            | "--danger-full-access",
        ) => true,
        Some(arg)
            if arg.starts_with("--resume=")
                || arg.starts_with("--session=")
                || arg.starts_with("--session-id=")
                || arg.starts_with("--model=") =>
        {
            true
        }
        _ => false,
    }
}
