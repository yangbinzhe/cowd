use crate::{compiled_runtime_build_identity, CliOutputFormat, BUILD_TARGET, DEFAULT_DATE};
use std::path::PathBuf;

pub(crate) fn print_static_config_command(
    args: Option<&str>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = args.unwrap_or("list").trim();
    if !matches!(action, "" | "list" | "doctor" | "show") {
        return Err(
            format!("unsupported config action: {action}. Expected list, show, or doctor").into(),
        );
    }
    let config_home = runtime::cowd_dirs::config_home_dir();
    let project_config = std::env::current_dir()?.join(".cowd/config.yaml");
    let user_config = config_home.join("config.yaml");
    match output_format {
        CliOutputFormat::Text => {
            println!("Config");
            println!("  Scope            static");
            println!("  User config      {}", user_config.display());
            println!("  Project config   {}", project_config.display());
            println!("  Runtime effect   none");
        }
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "config",
                "action": if action.is_empty() { "list" } else { action },
                "scope": "static",
                "user_config": user_config,
                "project_config": project_config,
                "runtime_effect": "none"
            }))?
        ),
    }
    Ok(())
}

pub(crate) fn print_static_tool_command(
    args: Option<&str>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = args.unwrap_or("list").trim();
    if !matches!(action, "" | "list" | "doctor") {
        return Err(format!("unsupported tool action: {action}. Expected list or doctor").into());
    }
    let tools = tools::mvp_tool_specs()
        .iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    match output_format {
        CliOutputFormat::Text => {
            println!("Tools");
            println!("  Scope            static");
            println!("  Count            {}", tools.len());
            println!("  Runtime effect   none");
            for tool in tools.iter().take(20) {
                println!("  - {tool}");
            }
        }
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "tool",
                "action": if action.is_empty() { "list" } else { action },
                "scope": "static",
                "runtime_effect": "none",
                "count": tools.len(),
                "tools": tools
            }))?
        ),
    }
    Ok(())
}

pub(crate) fn print_bootstrap_plan(
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let phases = runtime::BootstrapPlan::claude_code_default()
        .phases()
        .iter()
        .map(|phase| format!("{phase:?}"))
        .collect::<Vec<_>>();
    match output_format {
        CliOutputFormat::Text => {
            for phase in &phases {
                println!("- {phase}");
            }
        }
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "bootstrap-plan",
                "phases": phases,
            }))?
        ),
    }
    Ok(())
}

pub(crate) fn print_version(
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match output_format {
        CliOutputFormat::Text => println!("{}", render_version_report()),
        CliOutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&version_json_value())?);
        }
    }
    Ok(())
}

pub(crate) fn print_system_prompt(
    cwd: PathBuf,
    date: String,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let sections = runtime::load_system_prompt(cwd, date, std::env::consts::OS, "unknown")?;
    let message = sections.join(
        "

",
    );
    match output_format {
        CliOutputFormat::Text => println!("{message}"),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "system-prompt",
                "message": message,
                "sections": sections,
            }))?
        ),
    }
    Ok(())
}

pub(crate) fn render_version_report() -> String {
    let build = compiled_runtime_build_identity();
    let build_state = if build.git_dirty == Some(true) {
        "dirty"
    } else {
        "clean"
    };
    let target = BUILD_TARGET.unwrap_or("unknown");
    format!(
        "Cowd\n  Version          {}\n  Git SHA          {}\n  Build state      {build_state}\n  Target           {target}\n  Build date       {DEFAULT_DATE}",
        build.semver, build.git_sha
    )
}

pub(crate) fn version_json_value() -> serde_json::Value {
    let build = compiled_runtime_build_identity();
    serde_json::json!({
        "kind": "version",
        "message": render_version_report(),
        "version": build.semver,
        "git_sha": build.git_sha,
        "git_dirty": build.git_dirty,
        "target": BUILD_TARGET,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_surface_uses_the_runtime_build_identity() {
        let build = compiled_runtime_build_identity();
        let value = version_json_value();
        assert_eq!(value["version"], build.semver);
        assert_eq!(value["git_sha"], build.git_sha);
        assert_eq!(value["git_dirty"], serde_json::json!(build.git_dirty));
        assert!(
            render_version_report().contains(if build.git_dirty == Some(true) {
                "dirty"
            } else {
                "clean"
            })
        );
    }
}
