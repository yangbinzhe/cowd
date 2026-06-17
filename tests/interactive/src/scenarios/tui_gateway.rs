use crate::tui::TuiSession;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_gateway_panel" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-gateway")?;
    tui.wait_for("COWD", 15)?;
    println!("\n── TUI Gateway ──");

    runner.run("GatewayPanel: server status and API endpoints", || {
        // Gateway is tab 0 (default) — visible in sidebar on startup
        std::thread::sleep(std::time::Duration::from_millis(500));
        let cap = tui.capture()?;
        if !cap.contains("Gateway") {
            return Err(anyhow::anyhow!("Gateway panel tab not found"));
        }
        if !cap.contains("Server") {
            return Err(anyhow::anyhow!("Server status label not found"));
        }
        if !cap.contains("health") && !cap.contains("STOPPED") && !cap.contains("RUNNING") {
            return Err(anyhow::anyhow!("No health endpoint or server state visible"));
        }
        if !cap.contains("API") {
            return Err(anyhow::anyhow!("API endpoints section not visible"));
        }
        Ok(())
    });

    runner.run("GatewayPanel: keyboard hints shown", || {
        let cap = tui.capture()?;
        if !cap.contains("refresh") && !cap.contains("start/stop") {
            return Err(anyhow::anyhow!("Gateway keyboard hints not visible"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
