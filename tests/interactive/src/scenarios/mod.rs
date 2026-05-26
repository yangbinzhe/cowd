mod tui_basic;
mod tui_interact;
mod server_core;
mod server_mgmt;
mod cross_cut;

use crate::reporter::TestRunner;
use anyhow::Result;

pub fn list() {
    for (name, desc) in [
        ("tui_startup", "TUI logo + status bar"),
        ("tui_chat_stream", "Send message + verify streaming reply"),
        ("tui_scroll_expand", "PgUp/PgDn scroll + expand/collapse"),
        ("tui_search", "/ search highlight"),
        ("tui_sidebar_tabs", "Tab cycle sidebar panels"),
        ("tui_whichkey", "Space which-key overlay"),
        ("tui_cmd_palette", "Ctrl+P command palette"),
        ("tui_history", "Alt+↑ input history"),
        ("tui_toast", "Trigger toast notification"),
        ("tui_fork_export", "Session fork + export dialog"),
        ("tui_multi_input", "Shift+Enter multi-line + Ctrl+T theme"),
        ("server_health", "GET /health + session CRUD"),
        ("server_memory", "Memory search + config read"),
        ("server_workspace", "Workspace files + command execute"),
        ("server_platform", "Platform list + approval config"),
        ("cross_session_api", "TUI send → API read session"),
        ("cross_memory", "TUI trigger memory → API search"),
        ("cross_approval", "TUI approval → API pending"),
        ("cross_e2e", "Full end-to-end conversation test"),
    ] {
        println!("  {:<20} {}", name, desc);
    }
}

pub fn run_all(runner: &mut TestRunner, filter: Option<String>) -> anyhow::Result<()> {
    macro_rules! run_mod {
        ($mod:ident) => {
            if filter.as_deref().map_or(true, |f| $mod::has_scenario(f)) {
                $mod::run(runner)?;
            }
        };
    }
    run_mod!(tui_basic);
    run_mod!(tui_interact);
    run_mod!(server_core);
    run_mod!(server_mgmt);
    run_mod!(cross_cut);
    Ok(())
}
