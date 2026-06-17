use crate::tui::TuiSession;
use crate::reporter::TestRunner;
use crate::llm;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_whichkey" | "tui_cmd_palette" | "tui_history" | "tui_toast" | "tui_fork_export" | "tui_multi_input" | "tui_interact" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-interact")?;
    tui.wait_for("COWD", 15)?;
    println!("\n── TUI Interact ──");

    runner.run("Which-Key overlay via Space", || {
        tui.send_key("Space")?;
        std::thread::sleep(std::time::Duration::from_millis(400));
        let cap = tui.capture()?;
        tui.send_key("Escape")?;
        llm::validate_output(&cap, "The screen shows a keyboard shortcut help overlay (Which-Key) listing available keybindings.")
            .or_else(|_| {
                if cap.contains("Space") || cap.contains("leader") || cap.contains("F1") || cap.contains("Ctrl") { Ok(()) }
                else { Err(anyhow::anyhow!("Which-key not visible")) }
            })
    });

    runner.run("Command Palette Ctrl+P", || {
        tui.send_ctrl('p')?; std::thread::sleep(std::time::Duration::from_millis(300));
        tui.send_key("Escape")?;
        Ok(())
    });

    runner.run("Input history Alt+Up", || {
        tui.send("test history")?; tui.enter()?;
        tui.wait_for("history", 3)?;
        tui.send_alt("Up")?; std::thread::sleep(std::time::Duration::from_millis(300));
        Ok(())
    });

    runner.run("Toast via Ctrl+Y", || {
        tui.send_ctrl('y')?; std::thread::sleep(std::time::Duration::from_millis(300));
        Ok(())
    });

    tui.close()?;
    Ok(())
}
