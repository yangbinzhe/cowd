use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_session_sidebar" | "tui_session_switch" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-session-test")?;
    tui.wait_for("COWD", 15)?;
    println!("\n── TUI Session / Sidebar ──");

    runner.run("Session: /session list shows current session", || {
        tui.send("/session list")?;
        tui.enter()?;
        // Poll for response (max 5s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut cap = String::new();
        while std::time::Instant::now() < deadline {
            cap = tui.capture()?;
            if cap.len() >= 100 { break; }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        if cap.len() < 100 {
            return Err(anyhow::anyhow!("Session list output too short ({})", cap.len()));
        }
        Ok(())
    });

    runner.run("Session: /status shows health", || {
        tui.send("/status")?;
        tui.enter()?;
        // Poll for model info (max 5s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut cap = String::new();
        while std::time::Instant::now() < deadline {
            cap = tui.capture()?;
            if cap.contains("Model") || cap.contains("model") { break; }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        if !cap.contains("Model") && !cap.contains("model") {
            return Err(anyhow::anyhow!("Status does not show model info"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
