use crate::reporter::TestRunner;
use crate::tui::{TuiLaunchConfig, TuiSession};

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_gateway_live" | "tui_gateway_status" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new(TuiLaunchConfig::from_env("tui-gateway-live")?)?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Gateway Live ──");

    runner.run("GatewayPanel: shows live server status indicator", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    tui.close()?;
    Ok(())
}
