use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_gateway_live" | "tui_gateway_status" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-gateway-live")?;
    tui.wait_for("COWD", 15).ok();
    println!("\n── TUI Gateway Live ──");

    // Gateway is tab 0 (default), visible immediately
    runner.run("GatewayPanel: shows live server status indicator", || {
        let cap = tui.capture()?;
        // Check for status indicator text
        if !cap.contains("Server") && !cap.contains("server") {
            return Err(anyhow::anyhow!("GatewayPanel: no server status"));
        }
        if !cap.contains("API") && !cap.contains("api") && !cap.contains("health") {
            return Err(anyhow::anyhow!("GatewayPanel: no API endpoints visible"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
