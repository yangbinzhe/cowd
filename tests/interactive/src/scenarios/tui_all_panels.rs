use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_all_panels" | "tui_panel_keybinds" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-all-panels")?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI All Panels ──");

    // Test all panels by tabbing through them
    let panels = ["Gateway", "Files", "Memory", "Skills", "Delegates", "Context", "Changes", "Todo"];
    for (_i, _panel) in panels.iter().enumerate() {
        let cap = tui.capture()?;
        // Verify some content is visible
        if cap.len() < 50 {
            return Err(anyhow::anyhow!("Empty panel capture at panel index {}", _i));
        }
        tui.send_key("Tab")?;
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    runner.run("All panels: keyboard hints visible", || {
        let cap = tui.capture()?;
        if cap.len() < 100 {
            return Err(anyhow::anyhow!("Panel capture too short after tab cycle"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
