use crate::api::ApiClient;
use crate::reporter::TestRunner;
use crate::server::ServerProcess;

pub fn has_scenario(name: &str) -> bool {
    name == "server_workspace"
        || name == "server_platform"
        || name == "server_mgmt"
        || name == "all"
        || name == ""
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let mut srv = ServerProcess::start()?;
    let api = ApiClient::new("http://127.0.0.1:8642");
    println!("\n── Server Mgmt ──");

    runner.run("Workspace files", || {
        let _ = api.get("/api/workspace/files")?;
        Ok(())
    });
    runner.run("Platform list", || {
        let _ = api.get("/api/platforms")?;
        Ok(())
    });
    runner.run("Approval config", || {
        let _ = api.get("/api/approval/config")?;
        Ok(())
    });
    runner.run("Commands list", || {
        let _ = api.get("/api/commands")?;
        Ok(())
    });

    srv.close()?;
    Ok(())
}
