mod cross_active;
mod cross_cut;
mod server_core;
mod server_feishu;
mod server_gateway_api;
mod server_gateway_cmd;
mod server_mgmt;
mod server_send_message;
mod tui_all_panels;
mod tui_basic;
mod tui_gateway;
mod tui_gateway_live;
mod tui_interact;
mod tui_memory;
mod tui_mfg_operations;
mod tui_session_sidebar;
mod tui_skills;
mod tui_skills_registry;

use crate::reporter::TestRunner;

pub fn list() {
    for (name, desc) in [
        ("tui_startup", "TUI startup context"),
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
        (
            "tui_gateway_panel",
            "Gateway panel: server status + API endpoints",
        ),
        ("tui_memory_panel", "Memory panel: entries + keyboard hints"),
        ("tui_memory_slash", "/memory slash command response"),
        (
            "tui_skills_panel",
            "Skills panel: categories + built-in skills",
        ),
        ("tui_skills_hints", "Skills panel: keyboard hints present"),
        ("server_health", "GET /health + session CRUD"),
        ("server_memory", "Memory search + config read"),
        ("server_workspace", "Workspace files + command execute"),
        ("server_platform", "Platform list + approval config"),
        (
            "server_gateway_api",
            "Gateway API: memory, tools, config endpoints",
        ),
        (
            "server_gateway_memory",
            "Gateway API: /api/memory enabled + layers",
        ),
        (
            "server_gateway_tools",
            "Gateway API: /api/tools count + definitions",
        ),
        (
            "server_gateway_config",
            "Gateway API: /api/config response size",
        ),
        ("server_gateway_start", "Gateway CLI: start command"),
        ("server_gateway_status", "Gateway CLI: status command"),
        ("server_gateway_stop", "Gateway CLI: stop command"),
        ("cross_session_api", "TUI send → API read session"),
        ("cross_memory", "TUI trigger memory → API search"),
        ("cross_approval", "TUI approval → API pending"),
        ("cross_e2e", "Full end-to-end conversation test"),
        (
            "cross_active_session",
            "Active session visible in API sessions list",
        ),
        ("cross_active_sync", "Active session sync verification"),
        (
            "tui_session_sidebar",
            "Session: /session list shows current session",
        ),
        ("tui_session_switch", "Session: /status shows health"),
        (
            "server_send_message",
            "API: create session + verify sessions list",
        ),
        ("server_send_chat", "API: send message returns response"),
        (
            "tui_skills_registry",
            "SkillsPanel: shows tools from GlobalToolRegistry",
        ),
        ("tui_skills_tools", "SkillsPanel: tool names visible"),
        (
            "tui_gateway_live",
            "GatewayPanel: live server status indicator",
        ),
        ("tui_gateway_status", "GatewayPanel: API endpoints visible"),
        ("server_feishu_status", "Feishu: adapter module exists"),
        ("server_feishu_config", "Feishu: config present"),
        (
            "tui_all_panels",
            "Verify all panels accessible and show content",
        ),
        (
            "tui_panel_keybinds",
            "Keyboard hints visible across all panels",
        ),
        (
            "tui_mfg_operations",
            "MFG read-only control plane responsive layout and backlink evidence",
        ),
    ] {
        println!("  {:<20} {}", name, desc);
    }
}

pub fn run_all(runner: &mut TestRunner, filter: Option<String>) -> anyhow::Result<()> {
    let filters = filter
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let mut matched_modules = 0usize;
    macro_rules! run_mod {
        ($mod:ident) => {
            if filters.is_empty() || filters.iter().any(|filter| $mod::has_scenario(filter)) {
                matched_modules += 1;
                $mod::run(runner)?;
            }
        };
    }
    run_mod!(tui_basic);
    run_mod!(tui_gateway);
    run_mod!(tui_gateway_live);
    run_mod!(tui_interact);
    run_mod!(tui_memory);
    run_mod!(tui_skills);
    run_mod!(tui_skills_registry);
    run_mod!(tui_session_sidebar);
    run_mod!(server_core);
    run_mod!(server_mgmt);
    run_mod!(server_gateway_api);
    run_mod!(server_gateway_cmd);
    run_mod!(server_send_message);
    run_mod!(server_feishu);
    run_mod!(cross_cut);
    run_mod!(cross_active);
    run_mod!(tui_all_panels);
    run_mod!(tui_mfg_operations);
    if matched_modules == 0 {
        anyhow::bail!(
            "no interactive scenario module matched filter {}",
            filters.join(",")
        );
    }
    Ok(())
}
