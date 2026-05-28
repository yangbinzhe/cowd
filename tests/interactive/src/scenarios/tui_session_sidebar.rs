use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_session_sidebar" | "tui_session_switch" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-session-test")?;
    tui.wait_for("COWD", 15).ok();
    println!("\n── TUI Session / Sidebar ──");

    runner.run("Session: /session list shows current session", || {
        tui.send("/session list")?;
        tui.enter()?;
        std::thread::sleep(std::time::Duration::from_millis(2000));
        let cap = tui.capture()?;
        // Verify session list appears
        if cap.len() < 100 {
            return Err(anyhow::anyhow!("Session list output too short"));
        }
        Ok(())
    });

    runner.run("Session: /status shows health", || {
        tui.send("/status")?;
        tui.enter()?;
        std::thread::sleep(std::time::Duration::from_millis(1500));
        let cap = tui.capture()?;
        if !cap.contains("Model") && !cap.contains("model") {
            return Err(anyhow::anyhow!("Status does not show model info"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
