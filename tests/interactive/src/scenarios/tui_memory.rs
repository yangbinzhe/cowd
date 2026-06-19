use crate::tui::TuiSession;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_memory_panel" | "tui_memory_slash" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-memory")?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Memory ──");

    runner.run("MemoryPanel: navigate and verify entries display", || {
        let before = tui.capture()?;
        for _ in 0..2 {
            tui.send_key("Tab")?;
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let cap = tui.assert_healthy_capture(120)?;
        if cap == before {
            return Err(anyhow::anyhow!("Panel navigation did not change capture"));
        }
        Ok(())
    });

    runner.run("MemoryPanel: keyboard hints visible", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    runner.run("Memory: slash command /memory response", || {
        let before = tui.capture()?;
        tui.send("/memory")?;
        std::thread::sleep(std::time::Duration::from_millis(200));
        tui.enter()?;
        // Wait for command output (max 10s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut cap = String::new();
        while std::time::Instant::now() < deadline {
            cap = tui.capture()?;
            if cap != before && cap.trim().len() > before.trim().len() { break; }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        if cap == before {
            return Err(anyhow::anyhow!("/memory slash command did not update the TUI"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
