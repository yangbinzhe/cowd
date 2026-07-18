use crate::api::ApiClient;
use crate::llm;
use crate::reporter::TestRunner;
use crate::server::ServerProcess;
use crate::tui::{TuiLaunchConfig, TuiSession};

pub fn has_scenario(name: &str) -> bool {
    matches!(
        name,
        "cross_session_api"
            | "cross_memory"
            | "cross_approval"
            | "cross_e2e"
            | "cross_cut"
            | ""
            | "all"
    )
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let mut srv = ServerProcess::start()?;
    let api = ApiClient::new("http://127.0.0.1:8642");
    let tui = TuiSession::new(TuiLaunchConfig::from_env("cross-cut")?)?;
    tui.wait_until_ready(15)?;
    println!("\n── Cross-Cut ──");

    runner.run("TUI→API: send /status, verify API sees session", || {
        tui.send("/status")?;
        tui.enter()?;
        // Wait for session list via API polling (max 10s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut body = String::new();
        while std::time::Instant::now() < deadline {
            body = api.get("/api/sessions")?;
            if body.contains("sessions") && body.contains("id") { break; }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        // LLM validates session data
        llm::validate_output(&body, "The API response should be valid JSON containing a 'sessions' array with at least one session entry. Each session should have an 'id' field.")
            .or_else(|_| {
                // Basic fallback
                if body.contains("sessions") && body.contains("id") { Ok(()) }
                else { Err(anyhow::anyhow!("API did not return valid sessions: {}", &body[..body.len().min(200)])) }
            })
    });

    runner.run("E2E: LLM-generated conversation via TUI, verified via API", || {
        // LLM generates the conversation prompt
        let prompt = llm::generate_prompt("system_diagnostics");
        println!("  E2E prompt: {prompt}");

        tui.send(&prompt)?;
        tui.enter()?;

        // Wait for completion via TUI capture polling (max 35s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(35);
        let mut tui_output = String::new();
        while std::time::Instant::now() < deadline {
            tui_output = tui.capture()?;
            if tui_output.len() > 200 { break; }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        tui.screenshot("e2e_output.txt")?;

        // LLM validates TUI output
        llm::validate_output(&tui_output,
            "The terminal shows an AI conversation where a response was generated. \
             Look for substantial response content (not just 'assistant:' or empty text). \
             The AI should have attempted to answer the user's question.")
            .map_err(|e| anyhow::anyhow!("TUI output invalid: {e}"))?;

        // API verification
        let sessions = api.get("/api/sessions")?;
        llm::validate_output(&sessions,
            "API returns a sessions list with valid JSON structure containing at least one session entry.")
            .or_else(|_| {
                if sessions.contains("sessions") { Ok(()) }
                else { Err(anyhow::anyhow!("API sessions check failed")) }
            })
    });

    tui.close()?;
    srv.close()?;
    Ok(())
}
