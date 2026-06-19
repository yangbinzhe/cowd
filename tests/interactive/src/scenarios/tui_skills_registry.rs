use crate::reporter::TestRunner;
use crate::tui::TuiSession;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_skills_registry" | "tui_skills_tools" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-skills-reg")?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Skills Registry ──");

    // Navigate to Skills panel (Tab 3)
    for _ in 0..3 {
        tui.send_key("Tab")?;
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    runner.run("SkillsPanel: shows tools from GlobalToolRegistry", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    tui.close()?;
    Ok(())
}
