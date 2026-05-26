use crate::api::ApiClient;
use crate::server::ServerProcess;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    name == "server_workspace" || name == "server_platform" || name == "server_mgmt" || name == "all" || name == ""
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let mut srv = ServerProcess::start()?;
    let api = ApiClient::new("http://127.0.0.1:8642");
    println!("\n── Server Mgmt ──");

    runner.run("Workspace files", || { api.get("/api/workspace/files").ok(); Ok(()) });
    runner.run("Platform list", || { api.get("/api/platforms").ok(); Ok(()) });
    runner.run("Approval config", || { api.get("/api/approval/config").ok(); Ok(()) });
    runner.run("Commands list", || { api.get("/api/commands").ok(); Ok(()) });

    srv.close()?;
    Ok(())
}
