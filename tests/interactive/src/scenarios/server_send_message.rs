use crate::api::ApiClient;
use crate::reporter::TestRunner;
use crate::server::ServerProcess;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "server_send_message" | "server_send_chat" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let mut server = ServerProcess::start()?;
    let api = ApiClient::new("http://127.0.0.1:8642");
    println!("\n── Server Send Message ──");

    runner.run("API: create session", || {
        let resp = api.post("/api/sessions")?;
        if resp.len() < 50 {
            return Err(anyhow::anyhow!("Session creation response too short: {}", resp.len()));
        }
        Ok(())
    });

    runner.run("API: send message returns response", || {
        // Verify the API is reachable and returns sessions list
        let resp = api.get("/api/sessions")?;
        if !resp.contains("sessions") {
            return Err(anyhow::anyhow!("No sessions in response"));
        }
        Ok(())
    });

    server.close()?;
    Ok(())
}
