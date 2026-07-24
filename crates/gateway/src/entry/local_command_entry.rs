use crate::{
    truncate_for_prompt, CliOutputFormat, LocalHelpTopic, DEPRECATED_INSTALL_COMMAND,
    LATEST_SESSION_REFERENCE, OFFICIAL_REPO_SLUG, OFFICIAL_REPO_URL, VERSION,
};
use std::env;
use std::io::{self, Write};
use std::process::Command;

use runtime::{ContentBlock, Session};

pub(crate) fn render_help_topic(topic: LocalHelpTopic) -> String {
    match topic {
        LocalHelpTopic::Status => "Status
  Usage            cowd status
  Purpose          show the local workspace snapshot without entering the TUI
  Output           model, permissions, git state, config files, and sandbox status
  Related          /status inside TUI · cowd --resume latest"
            .to_string(),
        LocalHelpTopic::Sandbox => "Sandbox
  Usage            cowd sandbox
  Purpose          inspect the resolved sandbox and isolation state for the current directory
  Output           namespace, network, filesystem, and fallback details
  Related          /sandbox · cowd status"
            .to_string(),
        LocalHelpTopic::Doctor => "Doctor
  Usage            cowd doctor
  Purpose          diagnose local auth, config, workspace, sandbox, and build metadata
  Output           local-only health report; no provider request or session resume required
  Related          /doctor inside TUI · cowd --resume latest"
            .to_string(),
        LocalHelpTopic::Setup => "Setup
  Usage            cowd setup
  Purpose          check local setup, channels, gateway, memory, sessions, and permissions
  Output           safe readiness report with no secrets
  Related          /setup · cowd gateway status · cowd gateway open"
            .to_string(),
    }
}

pub(crate) fn print_help_topic(topic: LocalHelpTopic) {
    println!("{}", render_help_topic(topic));
}

pub(crate) fn print_help_to(out: &mut impl Write) -> io::Result<()> {
    writeln!(out, "cowd v{VERSION}")?;
    writeln!(out)?;
    writeln!(out, "Core commands:")?;
    writeln!(out, "  cowd")?;
    writeln!(out, "      Start the interactive TUI")?;
    writeln!(out, "  cowd tui")?;
    writeln!(out, "      Explicitly start the interactive TUI")?;
    writeln!(
        out,
        "  cowd gateway start|stop|restart|status|doctor|logs|repair|open"
    )?;
    writeln!(
        out,
        "      Control and diagnose the Gateway runtime entrypoint"
    )?;
    writeln!(out, "  cowd config list|show|doctor")?;
    writeln!(
        out,
        "      Inspect static configuration without runtime execution"
    )?;
    writeln!(out, "  cowd doctor")?;
    writeln!(
        out,
        "      Diagnose local provider credentials, config, workspace, and sandbox health"
    )?;
    writeln!(out, "  cowd skill list|show|validate")?;
    writeln!(
        out,
        "      Basic skill inventory and validation; use WebUI/TUI for management"
    )?;
    writeln!(out, "  cowd tool list|doctor")?;
    writeln!(
        out,
        "      Inspect static tool inventory without runtime execution"
    )?;
    writeln!(out, "  cowd version")?;
    writeln!(out, "      Print version and build information")?;
    writeln!(out, "  cowd help | cowd version")?;
    writeln!(out, "      `help` is a local alias for --help")?;
    writeln!(out)?;
    writeln!(out, "Source of truth: {OFFICIAL_REPO_SLUG}")?;
    writeln!(
        out,
        "Warning: do not `{DEPRECATED_INSTALL_COMMAND}` (deprecated stub)"
    )?;
    writeln!(out)?;
    writeln!(out, "Flags:")?;
    writeln!(
        out,
        "  --model MODEL              Override the active model for TUI startup"
    )?;
    writeln!(
        out,
        "  --session SESSION          Start or attach the interactive TUI to a managed session id"
    )?;
    writeln!(
        out,
        "  --output-format FORMAT     Machine-readable output for local diagnostics and export: text or json"
    )?;
    writeln!(
        out,
        "  --permission-mode MODE     Set read-only, workspace-write, or danger-full-access"
    )?;
    writeln!(
        out,
        "  --dangerously-skip-permissions  Skip all permission checks"
    )?;
    writeln!(
        out,
        "  --solo                       Alias for --dangerously-skip-permissions"
    )?;
    writeln!(
        out,
        "  --yolo                       Continuous autonomous mode: danger-full-access plus persistent goal execution"
    )?;
    writeln!(
        out,
        "  --version, -V              Print version and build information locally"
    )?;
    writeln!(out)?;
    writeln!(out, "Examples:")?;
    writeln!(out, "  cowd")?;
    writeln!(out, "  cowd gateway start")?;
    writeln!(out, "  cowd gateway status --output-format json")?;
    writeln!(out, "  cowd config list --output-format json")?;
    writeln!(out, "  cowd tool list")?;
    writeln!(out, "  cowd skill list")?;
    writeln!(out, "  cowd --resume {LATEST_SESSION_REFERENCE}")?;
    writeln!(out, "  cowd doctor")?;
    writeln!(out, "  source of truth: {OFFICIAL_REPO_URL}")?;
    writeln!(
        out,
        "  do not run `{DEPRECATED_INSTALL_COMMAND}` — it installs a deprecated stub"
    )?;
    Ok(())
}

pub(crate) fn print_help(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    print_help_to(&mut buffer)?;
    let message = String::from_utf8(buffer)?;
    match output_format {
        CliOutputFormat::Text => print!("{message}"),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "help",
                "message": message,
            }))?
        ),
    }
    Ok(())
}

pub(crate) fn render_teleport_report(target: &str) -> Result<String, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;

    let file_list = Command::new("rg")
        .args(["--files"])
        .current_dir(&cwd)
        .output()?;
    let file_matches = if file_list.status.success() {
        String::from_utf8(file_list.stdout)?
            .lines()
            .filter(|line| line.contains(target))
            .take(10)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let content_output = Command::new("rg")
        .args(["-n", "-S", "--color", "never", target, "."])
        .current_dir(&cwd)
        .output()?;

    let mut lines = vec![
        "Teleport".to_string(),
        format!("  Target           {target}"),
        "  Action           search workspace files and content for the target".to_string(),
    ];
    if !file_matches.is_empty() {
        lines.push(String::new());
        lines.push("File matches".to_string());
        lines.extend(file_matches.into_iter().map(|path| format!("  {path}")));
    }

    if content_output.status.success() {
        let matches = String::from_utf8(content_output.stdout)?;
        if !matches.trim().is_empty() {
            lines.push(String::new());
            lines.push("Content matches".to_string());
            lines.push(truncate_for_prompt(&matches, 4_000));
        }
    }

    if lines.len() == 1 {
        lines.push("  Result           no matches found".to_string());
    }

    Ok(lines.join("\n"))
}

pub(crate) fn render_last_tool_debug_report(
    session: &Session,
) -> Result<String, Box<dyn std::error::Error>> {
    let last_tool_use = session
        .messages()
        .rev()
        .find_map(|message| {
            message.blocks.iter().rev().find_map(|block| match block {
                ContentBlock::ToolUse { id, name, input } => {
                    Some((id.clone(), name.clone(), input.clone()))
                }
                _ => None,
            })
        })
        .ok_or_else(|| "no prior tool call found in session".to_string())?;

    let tool_result = session.messages().rev().find_map(|message| {
        message.blocks.iter().rev().find_map(|block| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error,
            } if tool_use_id == &last_tool_use.0 => {
                Some((tool_name.clone(), output.clone(), *is_error))
            }
            _ => None,
        })
    });

    let mut lines = vec![
        "Debug tool call".to_string(),
        "  Action           inspect the last recorded tool call and its result".to_string(),
        format!("  Tool id          {}", last_tool_use.0),
        format!("  Tool name        {}", last_tool_use.1),
        "  Input".to_string(),
        indent_block(&last_tool_use.2, 4),
    ];

    match tool_result {
        Some((tool_name, output, is_error)) => {
            lines.push("  Result".to_string());
            lines.push(format!("    name           {tool_name}"));
            lines.push(format!(
                "    status         {}",
                if is_error { "error" } else { "ok" }
            ));
            lines.push(indent_block(&output, 4));
        }
        None => lines.push("  Result           missing tool result".to_string()),
    }

    Ok(lines.join("\n"))
}

fn indent_block(value: &str, spaces: usize) -> String {
    let indent = " ".repeat(spaces);
    value
        .lines()
        .map(|line| format!("{indent}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn format_bughunter_report(scope: Option<&str>) -> String {
    format!(
        "Bughunter
  Scope            {}
  Action           inspect the selected code for likely bugs and correctness issues
  Output           findings should include file paths, severity, and suggested fixes",
        scope.unwrap_or("the current repository")
    )
}

pub(crate) fn format_ultraplan_report(task: Option<&str>) -> String {
    format!(
        "Ultraplan
  Task             {}
  Action           break work into a multi-step execution plan
  Output           plan should cover goals, risks, sequencing, verification, and rollback",
        task.unwrap_or("the current repo work")
    )
}

pub(crate) fn format_pr_report(branch: &str, context: Option<&str>) -> String {
    format!(
        "PR
  Branch           {branch}
  Context          {}
  Action           draft or create a pull request for the current branch
  Output           title and markdown body suitable for GitHub",
        context.unwrap_or("none")
    )
}

pub(crate) fn format_issue_report(context: Option<&str>) -> String {
    format!(
        "Issue
  Context          {}
  Action           draft or create a GitHub issue from the current context
  Output           title and markdown body suitable for GitHub",
        context.unwrap_or("none")
    )
}
