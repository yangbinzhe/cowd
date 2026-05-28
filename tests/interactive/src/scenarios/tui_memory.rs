use crate::tui::TuiSession;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_memory_panel" | "tui_memory_slash" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-memory")?;
    tui.wait_for("COWD", 15).ok();
    println!("\n── TUI Memory ──");

    runner.run("MemoryPanel: navigate and verify entries display", || {
        // Navigate: Gateway(0) → Files(1) → Memory(2)
        for _ in 0..2 {
            tui.send_key("Tab")?;
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let cap = tui.capture()?;

        if !cap.contains("Memory") {
            return Err(anyhow::anyhow!("Memory panel tab not found"));
        }
        // Memory panel shows either entries or the empty placeholder
        if !cap.contains("entries") && !cap.contains("No memory") {
            return Err(anyhow::anyhow!("No memory entries or placeholder visible"));
        }
        Ok(())
    });

    runner.run("MemoryPanel: keyboard hints visible", || {
        let cap = tui.capture()?;
        if !cap.contains("select") && !cap.contains("search") {
            return Err(anyhow::anyhow!("Memory keyboard hints not visible"));
        }
        Ok(())
    });

    runner.run("Memory: slash command /memory response", || {
        // Type /memory in the chat input area
        tui.send("/memory")?;
        std::thread::sleep(std::time::Duration::from_millis(200));
        tui.enter()?;
        // Wait for memory-related output (max 10s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut cap = String::new();
        while std::time::Instant::now() < deadline {
            cap = tui.capture()?;
            if cap.contains("memory") || cap.contains("Memory") || cap.contains("entry") { break; }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        if !cap.contains("memory") && !cap.contains("Memory") && !cap.contains("entry") {
            return Err(anyhow::anyhow!("/memory slash command did not produce expected response"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
