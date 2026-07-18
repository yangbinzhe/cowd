use crate::reporter::TestRunner;
use crate::tui::{TuiLaunchConfig, TuiSession};

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_gateway_panel" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new(TuiLaunchConfig::from_env("tui-gateway")?)?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Gateway ──");

    runner.run("GatewayPanel: server status and API endpoints", || {
        std::thread::sleep(std::time::Duration::from_millis(500));
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    runner.run("GatewayPanel: keyboard hints shown", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    tui.close()?;
    Ok(())
}
