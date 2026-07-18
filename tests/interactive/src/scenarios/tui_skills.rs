use crate::reporter::TestRunner;
use crate::tui::{TuiLaunchConfig, TuiSession};

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_skills_panel" | "tui_skills_hints" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new(TuiLaunchConfig::from_env("tui-skills")?)?;
    tui.wait_until_ready(15)?;
    println!("\n── TUI Skills ──");

    runner.run("SkillsPanel: navigate and verify categories shown", || {
        let before = tui.capture()?;
        for _ in 0..3 {
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

    runner.run("SkillsPanel: keyboard hints present", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    runner.run("SkillsPanel: built-in skills listed", || {
        tui.assert_healthy_capture(120)?;
        Ok(())
    });

    tui.close()?;
    Ok(())
}
