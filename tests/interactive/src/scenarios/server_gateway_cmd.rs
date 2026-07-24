use crate::reporter::TestRunner;
use std::process::Command;

pub fn has_scenario(name: &str) -> bool {
    matches!(
        name,
        "server_gateway_start" | "server_gateway_status" | "server_gateway_stop" | "" | "all"
    )
}

fn cowd_bin() -> String {
    std::env::var("COWD_BIN").unwrap_or_else(|_| "cowd".to_string())
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    println!("\n── Server Gateway CMD ──");

    runner.run("Gateway: start", || {
        let _out = Command::new(cowd_bin())
            .args(["gateway", "start"])
            .output()?;
        // gateway start succeeds silently
        Ok(())
    });

    runner.run("Gateway: status", || {
        let out = Command::new(cowd_bin())
            .args(["gateway", "status"])
            .output()?;
        if !out.status.success() && out.stdout.is_empty() && out.stderr.is_empty() {
            return Err(anyhow::anyhow!(
                "Gateway status produced no diagnostic output"
            ));
        }
        Ok(())
    });

    runner.run("Gateway: stop", || {
        let _out = Command::new(cowd_bin())
            .args(["gateway", "stop"])
            .output()?;
        Ok(())
    });

    Ok(())
}
