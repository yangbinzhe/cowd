fn main() {
    let first_arg = std::env::args().nth(1);
    match first_arg.as_deref() {
        None | Some("tui") => tui::terminal_entry(),
        Some("gateway") => gateway::backend_entry(),
        _ => gateway::main_entry(),
    }
}
