fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let first_arg = args.first().map(String::as_str);
    if should_open_tui(&args) {
        tui::terminal_entry();
        return;
    }

    match first_arg.as_deref() {
        Some("gateway") => gateway::backend_entry(),
        _ => gateway::static_entry(),
    }
}

fn should_open_tui(args: &[String]) -> bool {
    let first_arg = args.first().map(String::as_str);
    if args.iter().any(|arg| arg.trim_start().starts_with('/')) {
        return false;
    }

    match first_arg {
        None | Some("tui") => true,
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
