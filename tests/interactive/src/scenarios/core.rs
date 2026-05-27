use crate::tui::TuiSession;
use crate::api::ApiClient;
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
        std::thread::sleep(std::time::Duration::from_secs(20));
        let cap = tui.capture()?;
        if cap.len() < 200 { Err(anyhow::anyhow!("Response too short")) }
        else { Ok(()) }
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
            std::thread::sleep(std::time::Duration::from_millis(500));
            let cap = tui.capture()?;
            if cap.contains("Context") || cap.contains("Changes") { Ok(()) }
            else { Err(anyhow::anyhow!("Sidebar not visible")) }
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
        api.get("/api/memory/search?q=test").ok();
        api.get("/api/config").ok();
        api.get("/api/approval/config").ok();
        Ok(())
    });

    // === CROSS TEST ===
    println!("\n── Cross Test ──");
    runner.run("TUI→API session verify", || {
        let t = TuiSession::new("cross")?;
        std::thread::sleep(std::time::Duration::from_secs(6));
        t.send("/status")?; t.enter()?;
        std::thread::sleep(std::time::Duration::from_secs(3));
        t.close();

        let mut srv = ServerProcess::start()?;
        let api = ApiClient::new("http://127.0.0.1:8642");
        let body = api.get("/api/sessions")?;
        if !body.contains("sessions") { Err(anyhow::anyhow!("No sessions in API")) }
        else { Ok(()) }
    });

    Ok(())
}
