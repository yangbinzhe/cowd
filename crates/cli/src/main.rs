fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let first_arg = args.first().map(String::as_str);

    if should_open_tui(&args) || matches!(first_arg, Some("tui")) {
        open_tui_or_exit();
        return;
    }

    match first_arg {
        Some("gateway") => gateway::backend_entry(),
        _ => gateway::static_entry(),
    }
}

#[cfg(feature = "tui-surface")]
fn open_tui_or_exit() {
    tui::terminal_entry();
}

#[cfg(not(feature = "tui-surface"))]
fn open_tui_or_exit() {
    eprintln!(
        "TUI surface is not built in this binary; rebuild cowd with `--features full` or install a full build."
    );
    std::process::exit(2);
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
