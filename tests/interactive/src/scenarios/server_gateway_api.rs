use crate::api::ApiClient;
use crate::server::ServerProcess;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "server_gateway_api" | "server_gateway_memory" | "server_gateway_tools" | "server_gateway_config" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let _server = ServerProcess::start()?;
    let api = ApiClient::new("http://127.0.0.1:8642");
    println!("\n── Server Gateway API ──");

    runner.run("Gateway API: /api/memory returns layers", || {
        let resp = api.get("/api/memory")?;
        if !resp.contains("enabled") { return Err(anyhow::anyhow!("No 'enabled' in memory response")); }
        if !resp.contains("layers") { return Err(anyhow::anyhow!("No 'layers' in memory response")); }
        Ok(())
    });

    runner.run("Gateway API: /api/tools returns tools list", || {
        let resp = api.get("/api/tools")?;
        if !resp.contains("tools") { return Err(anyhow::anyhow!("No 'tools' in tools response")); }
        if !resp.contains("count") { return Err(anyhow::anyhow!("No 'count' in tools response")); }
        Ok(())
    });

    runner.run("Gateway API: /api/config returns config", || {
        let resp = api.get("/api/config")?;
        if resp.len() < 10 { return Err(anyhow::anyhow!("Config response too short")); }
        Ok(())
    });

    // server drops here → Drop impl kills process
    Ok(())
}
