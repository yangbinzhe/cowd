use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn help_json_only_lists_minimal_cli_surface() {
    let root = unique_temp_dir("help-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let parsed = assert_json_command(&root, &["--output-format", "json", "help"]);
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

    fs::remove_dir_all(root).expect("cleanup temp dir");
}

#[test]
fn version_emits_json_when_requested() {
    let root = unique_temp_dir("version-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let parsed = assert_json_command(&root, &["--output-format", "json", "version"]);
    assert_eq!(parsed["kind"], "version");
    assert_eq!(parsed["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn resume_slash_commands_remain_tui_only() {
    let root = unique_temp_dir("resume-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    for slash in ["/status", "/mcp", "/skills", "/version", "/init"] {
        let output = run_cowd(
            &root,
            &["--output-format", "json", "--resume", "resume-json", slash],
        );
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

    fs::remove_dir_all(root).expect("cleanup temp dir");
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

fn unique_temp_dir(label: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cowd-output-format-{label}-{}-{millis}-{counter}",
        std::process::id()
    ))
}
