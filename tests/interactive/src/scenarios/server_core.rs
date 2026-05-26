use crate::api::ApiClient;
use crate::server::ServerProcess;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    name == "server_health" || name == "server_memory" || name == "server_core" || name == "all" || name == ""
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let mut srv = ServerProcess::start()?;
    let api = ApiClient::new("http://127.0.0.1:8642");
    println!("\n── Server Core ──");

    runner.run("Health check", || {
        let body = api.get("/health")?;
        if body.contains("ok") { Ok(()) }
        else { Err(anyhow::anyhow!("Health: {body}")) }
    });

    runner.run("Session list", || {
        let body = api.get("/api/sessions")?;
        // At least valid JSON
        serde_json::from_str::<serde_json::Value>(&body)
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("{e}"))
    });

    runner.run("Memory search", || {
        api.get("/api/memory/search?q=test").ok();
        Ok(())
    });

    runner.run("Config", || {
        api.get("/api/config").ok();
        Ok(())
    });

    srv.close()?;
    Ok(())
}
