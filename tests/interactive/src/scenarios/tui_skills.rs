use crate::tui::TuiSession;
use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_skills_panel" | "tui_skills_hints" | "" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let tui = TuiSession::new("tui-skills")?;
    tui.wait_for("COWD", 15).ok();
    println!("\n── TUI Skills ──");

    runner.run("SkillsPanel: navigate and verify categories shown", || {
        // Navigate: Gateway(0) → Files(1) → Memory(2) → Skills(3)
        for _ in 0..3 {
            tui.send_key("Tab")?;
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let cap = tui.capture()?;

        if !cap.contains("Skills") {
            return Err(anyhow::anyhow!("Skills panel tab not found"));
        }
        // Built-in categories: Tools, Memory, Platform, System
        let has_categories = cap.contains("Tools")
            || cap.contains("Platform")
            || cap.contains("System");
        if !has_categories {
            return Err(anyhow::anyhow!("No skill categories visible"));
        }
        Ok(())
    });

    runner.run("SkillsPanel: keyboard hints present", || {
        let cap = tui.capture()?;
        if !cap.contains("search") && !cap.contains("toggle") && !cap.contains("enable") && !cap.contains("Tab") {
            return Err(anyhow::anyhow!("Skills keyboard hints not visible"));
        }
        Ok(())
    });

    runner.run("SkillsPanel: built-in skills listed", || {
        let cap = tui.capture()?;
        // At least one built-in skill name should appear
        let has_skills = cap.contains("Bash")
            || cap.contains("GitExpert")
            || cap.contains("MCP")
            || cap.contains("Cognitive");
        if !has_skills {
            return Err(anyhow::anyhow!("No built-in skill names visible"));
        }
        Ok(())
    });

    tui.close()?;
    Ok(())
}
