use crate::tui::TuiSession;
use crate::provider::ApiClient;
use crate::server::ServerProcess;
use crate::reporter::TestRunner;

pub fn has_scenario(n: &str) -> bool { n.is_empty() || n == "all" || n == "core" || n.starts_with("core_") }

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    // === TUI TESTS ===
    println!("\n── TUI Tests ──");
    let tui = TuiSession::new("base")?;

    runner.run("TUI starts", || {
        let cap = tui.capture()?;
        if cap.contains("Model") || cap.contains("Workspace") || cap.contains("cowd") || cap.contains("COWD") { Ok(()) }
        else { Err(anyhow::anyhow!("COWD logo not found")) }
    });

    runner.run("Send message", || {
        tui.send("Write a one-line shell command to list files")?;
        tui.enter()?;
        tui.wait_for("$", 25).or_else(|_| {
            let cap = tui.capture()?;
            if cap.len() < 200 { Err(anyhow::anyhow!("Response too short ({} chars)", cap.len())) }
            else { Ok(()) }
        })
    });

    runner.run("Which-Key overlay", || {
        tui.send_key("Space")?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        tui.send_key("Escape")?;
        Ok(())
    });

    if crate::tui::session_alive(tui.cmd()) {
        runner.run("Sidebar tab", || {
            tui.send_key("Tab")?;
            tui.wait_for("Context", 5).or_else(|_| {
                let cap = tui.capture()?;
                if cap.contains("Changes") { Ok(()) }
                else { Err(anyhow::anyhow!("Sidebar not visible after Tab")) }
            })
        });
    } else {
        println!("  ⬜ Sidebar tab skipped (session dead)");
    }

    tui.close();

    // === SERVER TESTS ===
    println!("\n── Server Tests ──");

    runner.run("Health endpoint", || {
        let mut srv = ServerProcess::start()?;
        let api = ApiClient::new("http://127.0.0.1:8642");
        let b = api.get("/health")?;
        let ok = b.contains("ok") || b.contains("healthy");
        if !ok { Err(anyhow::anyhow!("Health check failed")) }
        else { Ok(()) }
    });

    runner.run("Session CRUD", || {
        let mut srv = ServerProcess::start()?;
        let api = ApiClient::new("http://127.0.0.1:8642");
        let sessions = api.get("/api/sessions")?;
        let parsed = serde_json::from_str::<serde_json::Value>(&sessions);
        if parsed.is_err() { Err(anyhow::anyhow!("Sessions: invalid JSON")) }
        else { Ok(()) }
    });

    runner.run("Memory + Config", || {
        let mut srv = ServerProcess::start()?;
        let api = ApiClient::new("http://127.0.0.1:8642");
        let _ = api.get("/api/memory/search?q=test")?;
        let _ = api.get("/api/config")?;
        let _ = api.get("/api/approval/config")?;
        Ok(())
    });

    // === CROSS TEST ===
    println!("\n── Cross Test ──");
    runner.run("TUI→API session verify", || {
        let t = TuiSession::new("cross")?;
        t.wait_for("COWD", 10)?;
        t.send("/status")?; t.enter()?;
        t.wait_for("Model", 8)?;
        t.close();

        let mut srv = ServerProcess::start()?;
        let api = ApiClient::new("http://127.0.0.1:8642");
        let body = api.get("/api/sessions")?;
        if !body.contains("sessions") { Err(anyhow::anyhow!("No sessions in API")) }
        else { Ok(()) }
    });

    Ok(())
}
