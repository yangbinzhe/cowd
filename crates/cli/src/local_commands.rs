use std::process::ExitCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(format!(
                "unsupported value for --output-format: {other} (expected text or json)"
            )),
        }
    }
}

pub fn entry(args: &[String]) -> ExitCode {
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}\n\nRun `cowd --help` for usage.");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let (args, output) = parse_output_format(args)?;
    if args.iter().any(|arg| arg.trim_start().starts_with('/')) {
        if args.first().is_some_and(|arg| {
            arg == "--resume"
                || arg.starts_with("--resume=")
                || arg == "--session"
                || arg.starts_with("--session=")
        }) {
            return Err(
                "`cowd --resume ... /command` was removed from the CLI surface. Start `cowd --resume <session-id|latest>` and run slash commands inside the TUI."
                    .to_string(),
            );
        }
        return Err(
            "top-level slash commands were removed. Start the TUI with `cowd` and use slash commands there."
                .to_string(),
        );
    }
    let command = args.first().map(String::as_str).unwrap_or("help");
    let command_args = args.get(1..).unwrap_or_default();
    match command {
        "--help" | "-h" | "help" => print_help(output),
        "--version" | "-V" | "version" => print_version(output),
        "config" => print_config(command_args, output),
        "tool" => print_tools(command_args, output),
        "skill" => print_skills(command_args, output),
        "doctor" => print_doctor(command_args, output),
        other if is_removed_command(other) => Err(format!(
            "`cowd {other}` was removed and is not part of the CLI surface"
        )),
        other => Err(format!(
            "`cowd {other}` is not part of the CLI surface; use `cowd`, `cowd gateway`, `cowd config`, `cowd doctor`, `cowd skill`, or `cowd tool`"
        )),
    }
}

fn is_removed_command(command: &str) -> bool {
    matches!(
        command,
        "daemon"
            | "run"
            | "chat"
            | "prompt"
            | "session"
            | "memory"
            | "matrix"
            | "mfg"
            | "agent"
            | "agents"
            | "mcp"
            | "plugins"
            | "export"
            | "import-session"
            | "system-prompt"
            | "bootstrap-plan"
            | "dump-manifests"
            | "init"
            | "sandbox"
            | "status"
            | "setup"
            | "skills"
            | "tools"
    )
}

fn parse_output_format(args: &[String]) -> Result<(Vec<String>, OutputFormat), String> {
    let mut output = OutputFormat::Text;
    let mut remaining = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--output-format" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --output-format".to_string())?;
                output = OutputFormat::parse(value)?;
                index += 2;
            }
            value if value.starts_with("--output-format=") => {
                output = OutputFormat::parse(&value["--output-format=".len()..])?;
                index += 1;
            }
            value => {
                remaining.push(value.to_string());
                index += 1;
            }
        }
    }
    Ok((remaining, output))
}

fn print_help(output: OutputFormat) -> Result<(), String> {
    let message = format!(
        "Cowd {}\n\nCore commands:\n  cowd\n  cowd gateway start|stop|restart|status|doctor|logs|repair|open\n  cowd apps list|status <id>|doctor [id]|logs <id>|restart <id>\n  cowd storage <action>\n  cowd storage ownership-cutover activate|rollback --request <json> [credential channel]\n  cowd auth profile show|set\n  cowd config list|show|doctor\n  cowd doctor\n  cowd skill list|show|validate\n  cowd tool list|doctor\n  cowd version",
        env!("CARGO_PKG_VERSION")
    );
    if output == OutputFormat::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "help",
                "message": message,
                "commands": ["tui", "gateway", "apps", "storage", "auth", "config", "doctor", "skill", "tool", "version"],
            }))
            .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    println!("{message}");
    Ok(())
}

fn print_version(output: OutputFormat) -> Result<(), String> {
    let version = env!("CARGO_PKG_VERSION");
    let git_sha = option_env!("COWD_GIT_SHA").unwrap_or("unknown");
    let git_dirty = !matches!(option_env!("COWD_GIT_DIRTY"), Some("false"));
    let target = option_env!("COWD_BUILD_TARGET").unwrap_or("unknown");
    let build_state = if git_dirty { "dirty" } else { "clean" };
    match output {
        OutputFormat::Text => {
            println!(
                "Cowd\n  Version          {version}\n  Git SHA          {git_sha}\n  Build state      {build_state}\n  Target           {target}"
            );
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "version",
                "version": version,
                "git_sha": git_sha,
                "git_dirty": git_dirty,
                "target": target,
            }))
            .map_err(|error| error.to_string())?
        ),
    }
    Ok(())
}

fn print_config(args: &[String], output: OutputFormat) -> Result<(), String> {
    let action = args.first().map(String::as_str).unwrap_or("list");
    if args.len() > 1 || !matches!(action, "list" | "show" | "doctor") {
        return Err("usage: cowd config [list|show|doctor]".to_string());
    }
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let loader = runtime::ConfigLoader::default_for(&cwd);
    let discovered = loader.discover();
    let config = loader.load().map_err(|error| error.to_string())?;
    let user_config = runtime::cowd_dirs::config_home_dir().join("config.yaml");
    let project_config = cwd.join(".cowd/config.yaml");
    let payload = serde_json::json!({
        "kind": "config",
        "action": action,
        "scope": "static",
        "runtime_effect": "none",
        "workspace": config.workspace(),
        "user_config": user_config,
        "project_config": project_config,
        "discovered_files": discovered,
        "loaded_entries": config.loaded_entries().len(),
        "valid": true,
    });
    match output {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&payload).map_err(|error| error.to_string())?
        ),
        OutputFormat::Text => {
            println!("Config");
            println!(
                "  Workspace        {}",
                config.workspace().unwrap_or(&cwd).display()
            );
            println!("  User config      {}", user_config.display());
            println!("  Project config   {}", project_config.display());
            println!("  Loaded entries   {}", config.loaded_entries().len());
            println!("  Status           valid");
        }
    }
    Ok(())
}

fn print_tools(args: &[String], output: OutputFormat) -> Result<(), String> {
    let action = args.first().map(String::as_str).unwrap_or("list");
    if args.len() > 1 || !matches!(action, "list" | "doctor") {
        return Err("usage: cowd tool [list|doctor]".to_string());
    }
    let tools = tools::mvp_tool_specs()
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    match output {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "tool",
                "action": action,
                "scope": "static",
                "runtime_effect": "none",
                "count": tools.len(),
                "tools": tools,
            }))
            .map_err(|error| error.to_string())?
        ),
        OutputFormat::Text => {
            println!("Tools\n  Count            {}", tools.len());
            for tool in tools {
                println!("  - {tool}");
            }
        }
    }
    Ok(())
}

fn print_skills(args: &[String], output: OutputFormat) -> Result<(), String> {
    let action = args.first().map(String::as_str).unwrap_or("list");
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let registry = skill::SkillRegistry::discover(&cwd);
    match action {
        "list" if args.len() == 1 || args.is_empty() => {
            let skills = registry.list().map_err(|error| error.to_string())?;
            match output {
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "skills",
                        "action": "list",
                        "scope": "static",
                        "runtime_effect": "none",
                        "count": skills.len(),
                        "skills": skills,
                    }))
                    .map_err(|error| error.to_string())?
                ),
                OutputFormat::Text => {
                    println!("Skills\n  Count            {}", skills.len());
                    for skill in skills {
                        let marker = if skill.shadowed_by.is_some() {
                            "shadowed"
                        } else {
                            "active"
                        };
                        println!("  - {} ({marker})", skill.name);
                    }
                }
            }
        }
        "show" | "view" if args.len() == 2 => {
            let info = registry
                .resolve(&args[1])
                .map_err(|error| error.to_string())?;
            let content = std::fs::read_to_string(&info.path).map_err(|error| error.to_string())?;
            match output {
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "skill",
                        "info": info,
                        "content": content,
                    }))
                    .map_err(|error| error.to_string())?
                ),
                OutputFormat::Text => println!("{content}"),
            }
        }
        "validate" if args.len() == 2 => {
            let info = registry
                .resolve(&args[1])
                .map_err(|error| error.to_string())?;
            let parsed = skill::parse_skill_file(&info.path).map_err(|error| error.to_string())?;
            let security = skill::scan_skill_file(&info.path).map_err(|error| error.to_string())?;
            match output {
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "kind": "skill_validation",
                        "name": info.name,
                        "manifest": parsed.manifest,
                        "security": security,
                    }))
                    .map_err(|error| error.to_string())?
                ),
                OutputFormat::Text => println!(
                    "Skill validation\n  Name             {}\n  Security         {:?}",
                    info.name, security.status
                ),
            }
        }
        _ => {
            return Err(
                "cowd skill is limited to static skill management: list, show, or validate"
                    .to_string(),
            )
        }
    }
    Ok(())
}

fn print_doctor(args: &[String], output: OutputFormat) -> Result<(), String> {
    if !args.is_empty() {
        return Err("usage: cowd doctor".to_string());
    }
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let config = runtime::ConfigLoader::default_for(&cwd).load();
    let workspace_ok = cwd.is_dir();
    let config_error = config.as_ref().err().map(ToString::to_string);
    let ready = workspace_ok && config.is_ok();
    match output {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "doctor",
                "ready": ready,
                "workspace": cwd,
                "workspace_available": workspace_ok,
                "config_valid": config.is_ok(),
                "config_error": config_error,
            }))
            .map_err(|error| error.to_string())?
        ),
        OutputFormat::Text => {
            println!("Doctor");
            println!("  Workspace        {}", cwd.display());
            println!(
                "  Configuration    {}",
                if config.is_ok() { "valid" } else { "invalid" }
            );
            println!(
                "  Status           {}",
                if ready { "ready" } else { "blocked" }
            );
            if let Some(error) = config_error {
                println!("  Error            {error}");
            }
        }
    }
    if ready {
        Ok(())
    } else {
        Err("local Cowd configuration is not ready".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_format_is_removed_before_command_dispatch() {
        let (args, output) = parse_output_format(&[
            "tool".to_string(),
            "--output-format=json".to_string(),
            "list".to_string(),
        ])
        .expect("parse");
        assert_eq!(args, ["tool", "list"]);
        assert_eq!(output, OutputFormat::Json);
    }

    #[test]
    fn unsupported_business_command_fails_closed() {
        let error = run(&["prompt".to_string()]).expect_err("removed prompt command");
        assert!(error.contains("not part of the CLI surface"));
    }
}
