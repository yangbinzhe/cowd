mod tui_basic;
mod tui_gateway;
mod tui_interact;
mod tui_memory;
mod tui_skills;
mod server_core;
mod server_mgmt;
mod server_gateway_api;
mod server_gateway_cmd;
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
        ("tui_gateway_panel", "Gateway panel: server status + API endpoints"),
        ("tui_memory_panel", "Memory panel: entries + keyboard hints"),
        ("tui_memory_slash", "/memory slash command response"),
        ("tui_skills_panel", "Skills panel: categories + built-in skills"),
        ("tui_skills_hints", "Skills panel: keyboard hints present"),
        ("server_health", "GET /health + session CRUD"),
        ("server_memory", "Memory search + config read"),
        ("server_workspace", "Workspace files + command execute"),
        ("server_platform", "Platform list + approval config"),
        ("server_gateway_api", "Gateway API: memory, tools, config endpoints"),
        ("server_gateway_memory", "Gateway API: /api/memory enabled + layers"),
        ("server_gateway_tools", "Gateway API: /api/tools count + definitions"),
        ("server_gateway_config", "Gateway API: /api/config response size"),
        ("server_gateway_start", "Gateway CLI: start command"),
        ("server_gateway_status", "Gateway CLI: status command"),
        ("server_gateway_stop", "Gateway CLI: stop command"),
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
    run_mod!(tui_gateway);
    run_mod!(tui_interact);
    run_mod!(tui_memory);
    run_mod!(tui_skills);
    run_mod!(server_core);
    run_mod!(server_mgmt);
    run_mod!(server_gateway_api);
    run_mod!(server_gateway_cmd);
    run_mod!(cross_cut);
    Ok(())
}
