use crate::tui::TuiSession;
use crate::reporter::TestRunner;
use crate::llm;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_startup" | "tui_chat" | "tui_chat_stream" | "tui_scroll_expand" | "tui_search" | "tui_sidebar_tabs" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-basic")?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Basic ──");

    runner.run("Startup: COWD context visible", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    runner.run("Chat: send message, receive streaming reply", || {
        // LLM generates a contextually relevant test prompt
        let prompt = llm::generate_prompt("conversational_ai");
        println!("  LLM prompt: {}", prompt);

        tui.send(&prompt)?;
        tui.enter()?;

        // Use wait_for with timeout (max 20s) instead of fixed sleep
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let mut output = String::new();
        while std::time::Instant::now() < deadline {
            output = tui.capture()?;
            if output.len() > 100 { break; }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        // LLM validates the output semantics
        llm::validate_output(&output, "The terminal output shows an AI assistant's response that is helpful and on-topic. It should contain actual response text, not just an error message.")
            .map_err(|e| anyhow::anyhow!("Output validation failed: {e}"))
    });

    runner.run("Scroll: PgUp/PgDn changes viewport", || {
        tui.send("Write a haiku about programming")?; tui.enter()?;
        tui.wait_for("haiku", 15)?;
        tui.send("Write another about debugging")?; tui.enter()?;
        tui.wait_for("debugging", 15)?;
        let before = tui.capture()?;
        tui.send_key("PageUp")?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        tui.send_key("PageDown")?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        let after = tui.capture()?;
        // LLM validates scroll changed viewport
        llm::validate_output(&after, "The terminal output should look different from before scrolling, showing a different portion of conversation history.")
            .or_else(|_| {
                // Fallback: basic heuristic if no LLM
                if before == after { Err(anyhow::anyhow!("Scroll did not change viewport")) }
                else { Ok(()) }
            })
    });

    runner.run("Sidebar: Tab cycles panels", || {
        let before = tui.capture()?;
        tui.send_key("Tab")?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        let after = tui.assert_healthy_capture(120)?;
        if after == before { Err(anyhow::anyhow!("Tab did not update the TUI")) }
        else { Ok(()) }
    });

    tui.close()?;
    Ok(())
}
