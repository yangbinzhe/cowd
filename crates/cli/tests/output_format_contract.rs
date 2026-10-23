#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

#[test]
fn help_json_only_lists_minimal_cli_surface() {
    let root = unique_temp_dir("help-json");
    let parsed = assert_json_command(root.path(), &["--output-format", "json", "help"]);
    assert_eq!(parsed["kind"], "help");
    let message = parsed["message"].as_str().expect("help text");
    assert!(message.contains("Core commands:"));
    assert!(message.contains("cowd gateway start|stop|restart|status|doctor|logs|repair|open"));
    assert!(message.contains("cowd config list|show|doctor"));
    assert!(message.contains("cowd skill list|show|validate"));
    assert!(message.contains("cowd tool list|doctor"));

    for forbidden in [
        "cowd agents",
        "cowd mcp",
        "cowd plugins",
        "cowd export",
        "cowd import-session",
        "cowd system-prompt",
        "cowd bootstrap-plan",
        "cowd dump-manifests",
        "cowd init",
        "cowd sandbox",
        "cowd status",
        "Interactive slash commands:",
        "/session list",
    ] {
        assert!(
            !message.contains(forbidden),
            "help still exposes forbidden surface: {forbidden}\n{message}"
        );
    }
}

#[test]
fn version_emits_json_when_requested() {
    let root = unique_temp_dir("version-json");
    let parsed = assert_json_command(root.path(), &["--output-format", "json", "version"]);
    assert_eq!(parsed["kind"], "version");
    assert_eq!(parsed["version"], env!("CARGO_PKG_VERSION"));
    let git_sha = parsed["git_sha"].as_str().expect("Git SHA is text");
    assert!(
        git_sha == "unknown"
            || matches!(git_sha.len(), 40 | 64)
                && git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "build identity must use a full Git object ID"
    );
    let git_dirty = parsed["git_dirty"]
        .as_bool()
        .expect("dirty/clean state is explicit");
    if git_sha == "unknown" {
        assert!(git_dirty, "unknown source state cannot claim to be clean");
    }
}

#[test]
fn resume_slash_commands_remain_tui_only() {
    let root = unique_temp_dir("resume-json");
    for slash in ["/status", "/mcp", "/skills", "/version", "/init"] {
        assert_removed_slash(
            root.path(),
            &["--output-format", "json", "--resume", "resume-json", slash],
        );
    }
}

#[test]
fn resume_flag_rejects_non_interactive_slash_commands() {
    let root = unique_temp_dir("resume-plain");
    assert_removed_slash(root.path(), &["--resume", "latest", "/status"]);
}

fn assert_removed_slash(current_dir: &Path, args: &[&str]) {
    let output = run_cowd(current_dir, args);
    assert!(
        !output.status.success(),
        "expected resume slash command to fail\nstdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("was removed from the CLI surface"),
        "{stderr}"
    );
    assert!(
        stderr.contains("run slash commands inside the TUI"),
        "{stderr}"
    );
}

fn assert_json_command(current_dir: &Path, args: &[&str]) -> Value {
    let output = run_cowd(current_dir, args);
    assert!(
        output.status.success(),
        "stdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be valid json")
}

fn run_cowd(current_dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("cowd should launch")
}

fn unique_temp_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("cowd-output-format-{label}-"))
        .tempdir()
        .expect("exclusive temporary directory")
}
