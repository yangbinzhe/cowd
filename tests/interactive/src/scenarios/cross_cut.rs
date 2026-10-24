use crate::api::ApiClient;
use crate::llm;
use crate::reporter::TestRunner;
use crate::server::ServerProcess;
use crate::tui::{TuiLaunchConfig, TuiSession};
use serde::Deserialize;

// Required fields mirror SessionInfo in api_routes/session_routes.rs.
#[derive(Deserialize)]
struct SessionList {
    sessions: Vec<SessionMetadata>,
}

#[derive(Deserialize)]
struct SessionMetadata {
    id: String,
    status: String,
    message_count: i64,
}

fn validate_session_response(body: &str, session_id: &str) -> anyhow::Result<()> {
    let list: SessionList = serde_json::from_str(body)
        .map_err(|error| anyhow::anyhow!("Invalid API sessions response: {error}"))?;
    if session_id.trim().is_empty()
        || list.sessions.iter().any(|session| {
            session.id.trim().is_empty()
                || session.status.trim().is_empty()
                || session.message_count < 0
        })
    {
        return Err(anyhow::anyhow!("Invalid API session metadata"));
    }
    if !list.sessions.iter().any(|session| session.id == session_id) {
        return Err(anyhow::anyhow!("API did not return TUI session {session_id}"));
    }
    Ok(())
}

pub fn has_scenario(name: &str) -> bool {
    matches!(
        name,
        "cross_session_api"
            | "cross_e2e"
            | "cross_cut"
            | ""
            | "all"
    )
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let mut srv = ServerProcess::start()?;
    let api = ApiClient::new(srv.base_url());
    let mut config = TuiLaunchConfig::from_env("cross-cut")?;
    srv.configure_tui(&mut config);
    let session_id = config.session_id.clone();
    let tui = TuiSession::new(config)?;
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
            if validate_session_response(&body, &session_id).is_ok() { break; }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        validate_session_response(&body, &session_id)
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
        validate_session_response(&sessions, &session_id)
    });

    tui.close()?;
    srv.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_session_response;

    #[test]
    fn sessions_require_exact_tui_id_and_typed_metadata() {
        let body = r#"{"sessions":[{"id":"foreign","status":"active","message_count":0},{"id":"this-tui","status":"active","message_count":2,"title":"diagnostics"}],"total":2}"#;
        assert!(validate_session_response(body, "this-tui").is_ok());
        assert!(validate_session_response(body, "this").is_err());
        assert!(validate_session_response(body, "").is_err());
    }

    #[test]
    fn sessions_reject_invalid_json_foreign_ids_and_malformed_metadata() {
        for body in [
            "sessions id this-tui",
            r#"{"sessions":["#,
            r#"[{"id":"this-tui","status":"active","message_count":0}]"#,
            r#"{"sessions":{ "id":"this-tui"}}"#,
            r#"{"sessions":[]}"#,
            r#"{"sessions":[{"id":"foreign","status":"active","message_count":0}],"note":"this-tui"}"#,
            r#"{"sessions":[{"id":"this-tui"}]}"#,
            r#"{"sessions":[{"id":12,"status":"active","message_count":0}]}"#,
            r#"{"sessions":[{"id":"this-tui","status":false,"message_count":0}]}"#,
            r#"{"sessions":[{"id":"this-tui","status":" ","message_count":0}]}"#,
            r#"{"sessions":[{"id":"this-tui","status":"active","message_count":"0"}]}"#,
            r#"{"sessions":[{"id":"this-tui","status":"active","message_count":-1}]}"#,
        ] {
            assert!(validate_session_response(body, "this-tui").is_err(), "{body}");
        }
    }
}
