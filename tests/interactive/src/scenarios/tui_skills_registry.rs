use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_skills_registry" | "tui_skills_tools" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-skills-reg")?;
    tui.wait_for("COWD", 15).ok();
    println!("\n── TUI Skills Registry ──");

    // Navigate to Skills panel (Tab 3)
    for _ in 0..3 {
        tui.send_key("Tab")?;
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    runner.run("SkillsPanel: shows tools from GlobalToolRegistry", || {
        let cap = tui.capture()?;
        // Check for known built-in tools
        if !cap.contains("Bash") && !cap.contains("bash") && !cap.contains("FileOps") && !cap.contains("Git") {
            return Err(anyhow::anyhow!("SkillsPanel: no tool names found"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
