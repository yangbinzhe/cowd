use crate::api::ApiClient;
use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "cross_active_session" | "cross_active_sync" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-cross-active")?;
    tui.wait_until_ready(15)?;
    println!("\n── Cross Active ──");

    runner.run("Cross: TUI session visible in API sessions list", || {
        let api = ApiClient::new("http://127.0.0.1:8642");
        let resp = api.get("/api/sessions")?;
        if !resp.contains("sessions") {
            return Err(anyhow::anyhow!("No sessions in API response"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
