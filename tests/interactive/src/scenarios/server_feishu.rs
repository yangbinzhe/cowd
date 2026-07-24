use crate::reporter::TestRunner;

pub fn has_scenario(name: &str) -> bool {
    matches!(
        name,
        "server_feishu_status" | "server_feishu_config" | "" | "all"
    )
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    println!("\n── Server Feishu ──");

    runner.run("Feishu: adapter module exists", || {
        let ok = std::fs::metadata("crates/runtime/src/platform/feishu/adapter.rs").is_ok()
            || std::fs::metadata("crates/meta/src/platform/feishu/adapter.rs").is_ok()
            || std::fs::metadata("crates/meta/src/platform/feishu.rs").is_ok();
        if !ok {
            return Err(anyhow::anyhow!("Feishu adapter file not found"));
        }
        Ok(())
    });

    Ok(())
}
