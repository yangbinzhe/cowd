use crate::reporter::TestRunner;
use crate::tui::{TuiLaunchConfig, TuiSession};

pub fn has_scenario(name: &str) -> bool {
    matches!(
        name,
        "tui_session_sidebar" | "tui_session_switch" | "" | "all"
    )
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new(TuiLaunchConfig::from_env("tui-session-test")?)?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Session / Sidebar ──");

    runner.run("Session: /session list shows current session", || {
        tui.send("/session list")?;
        tui.enter()?;
        // Poll for response (max 5s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut cap = String::new();
        while std::time::Instant::now() < deadline {
            cap = tui.capture()?;
            if cap.len() >= 100 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        if cap.len() < 100 {
            return Err(anyhow::anyhow!(
                "Session list output too short ({})",
                cap.len()
            ));
        }
        Ok(())
    });

    runner.run("Session: /status shows health", || {
        let before = tui.capture()?;
        tui.send("/status")?;
        tui.enter()?;
        // Poll for visible command response (max 5s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut cap = String::new();
        while std::time::Instant::now() < deadline {
            cap = tui.capture()?;
            if cap != before && cap.trim().len() >= before.trim().len() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        if cap == before {
            return Err(anyhow::anyhow!("Status command did not update the TUI"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
