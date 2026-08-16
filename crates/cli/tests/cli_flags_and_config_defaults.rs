#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn removed_top_level_commands_do_not_fall_back_to_tui() {
    let temp_dir = unique_temp_dir("removed-top-level");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    for command in [
        "daemon",
        "run",
        "chat",
        "prompt",
        "session",
        "memory",
        "matrix",
        "mfg",
        "agent",
        "agents",
        "mcp",
        "plugins",
        "export",
        "import-session",
        "system-prompt",
        "bootstrap-plan",
        "dump-manifests",
        "init",
        "sandbox",
        "status",
        "setup",
    ] {
        let output = command_in(&temp_dir)
            .arg(command)
            .output()
            .expect("cowd should launch");
        assert_failure(&output);
        let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
        assert!(
            stderr.contains("removed")
                || stderr.contains("not part of the minimal CLI surface")
                || stderr.contains("no longer a top-level CLI management surface"),
            "{command}: {stderr}"
        );
        assert!(
            !stderr.contains("No such device or address"),
            "{command} leaked an OS/runtime error: {stderr}"
        );
    }

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn static_config_and_tool_commands_do_not_start_runtime() {
    let temp_dir = unique_temp_dir("static-config-tool");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    let config = command_in(&temp_dir)
        .args(["--output-format", "json", "config", "list"])
        .output()
        .expect("cowd config should launch");
    assert_success(&config);
    let config_json: serde_json::Value =
        serde_json::from_slice(&config.stdout).expect("config stdout should be json");
    assert_eq!(config_json["kind"], "config");
    assert_eq!(config_json["runtime_effect"], "none");

    let tool = command_in(&temp_dir)
        .args(["--output-format", "json", "tool", "list"])
        .output()
        .expect("cowd tool should launch");
    assert_success(&tool);
    let tool_json: serde_json::Value =
        serde_json::from_slice(&tool.stdout).expect("tool stdout should be json");
    assert_eq!(tool_json["kind"], "tool");
    assert_eq!(tool_json["runtime_effect"], "none");
    assert!(tool_json["count"].as_u64().is_some());

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn skill_singular_is_supported_and_skills_alias_is_removed() {
    let temp_dir = unique_temp_dir("skill-singular");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    let singular = command_in(&temp_dir)
        .args(["--output-format", "json", "skill", "list"])
        .output()
        .expect("cowd skill should launch");
    assert_success(&singular);
    let parsed: serde_json::Value =
        serde_json::from_slice(&singular.stdout).expect("skill stdout should be json");
    assert_eq!(parsed["kind"], "skills");

    let plural = command_in(&temp_dir)
        .args(["--output-format", "json", "skills", "list"])
        .output()
        .expect("removed skills alias should return a CLI error");
    assert_failure(&plural);

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn tool_singular_is_supported_and_tools_alias_is_removed() {
    let temp_dir = unique_temp_dir("tool-singular");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    let singular = command_in(&temp_dir)
        .args(["--output-format", "json", "tool", "list"])
        .output()
        .expect("cowd tool should launch");
    assert_success(&singular);
    let parsed: serde_json::Value =
        serde_json::from_slice(&singular.stdout).expect("tool stdout should be json");
    assert_eq!(parsed["kind"], "tool");

    let plural = command_in(&temp_dir)
        .args(["--output-format", "json", "tools", "list"])
        .output()
        .expect("removed tools alias should return a CLI error");
    assert_failure(&plural);

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn skill_runtime_invocations_are_not_top_level_cli_actions() {
    let temp_dir = unique_temp_dir("skill-runtime-rejected");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    for args in [
        ["skill", "help", "overview"].as_slice(),
        ["skill", "unknown", "arg"].as_slice(),
    ] {
        let output = command_in(&temp_dir)
            .args(args)
            .output()
            .expect("cowd skill should launch");
        assert_failure(&output);
        let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
        assert!(
            stderr.contains("limited to static skill management"),
            "{stderr}"
        );
        assert!(
            !stderr.contains("No such device or address"),
            "skill command leaked runtime error: {stderr}"
        );
    }

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn skill_plan_accepts_an_absolute_local_source() {
    let temp_dir = unique_temp_dir("skill-absolute-source");
    let config_home = temp_dir.join("config");
    let skill_dir = temp_dir.join("absolute-demo");
    fs::create_dir_all(&config_home).expect("config home should exist");
    fs::create_dir_all(&skill_dir).expect("skill dir should exist");
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: absolute-demo\ndescription: absolute source fixture\n---\n",
    )
    .expect("skill fixture");

    let output = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .args([
            "--output-format",
            "json",
            "skill",
            "plan",
            skill_dir.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("cowd skill plan should launch");
    assert_success(&output);
    let parsed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("plan should be json");
    assert_eq!(parsed["skill_id"], "absolute-demo");
    let digest = parsed["package_digest"]
        .as_str()
        .expect("plan digest")
        .to_string();

    let unreviewed = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .args(["skill", "install", skill_dir.to_str().expect("utf8 path")])
        .output()
        .expect("unreviewed install should launch");
    assert_failure(&unreviewed);
    assert!(!config_home
        .join("skill-store/v1/absolute-demo/active.json")
        .exists());

    let installed = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .args([
            "--output-format",
            "json",
            "skill",
            "install",
            skill_dir.to_str().expect("utf8 path"),
            "--expected-digest",
            &digest,
        ])
        .output()
        .expect("reviewed install should launch");
    assert_success(&installed);
    let installed: serde_json::Value =
        serde_json::from_slice(&installed.stdout).expect("receipt should be json");
    assert_eq!(installed["receipt"]["skill_id"], "absolute-demo");
    assert_eq!(installed["receipt"]["package_digest"], digest);

    make_tree_writable(&temp_dir);
    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn direct_slash_commands_are_tui_only() {
    let temp_dir = unique_temp_dir("slash-dispatch");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    for command in ["/help", "/status", "/mcp"] {
        let output = command_in(&temp_dir)
            .arg(command)
            .output()
            .expect("cowd should launch");
        assert_failure(&output);
        let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
        assert!(stderr.contains("top-level slash commands were removed"));
        assert!(stderr.contains("Start the TUI"));
    }

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn doctor_command_remains_local_static_entrypoint() {
    let temp_dir = unique_temp_dir("doctor-entrypoint");
    let config_home = temp_dir.join("home").join(".cowd");
    fs::create_dir_all(&config_home).expect("config home should exist");

    let output = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env("ANTHROPIC_BASE_URL", "http://127.0.0.1:9")
        .arg("doctor")
        .output()
        .expect("cowd doctor should launch");

    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("Doctor"));
    assert!(!stdout.contains("Thinking"));

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

fn command_in(cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cowd"));
    command.current_dir(cwd);
    command
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected command to fail\nstdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cowd-{label}-{}-{millis}-{counter}",
        std::process::id()
    ))
}

fn make_tree_writable(root: &Path) {
    if !root.exists() {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(root).expect("tree metadata");
        let mode = if metadata.is_dir() { 0o700 } else { 0o600 };
        fs::set_permissions(root, fs::Permissions::from_mode(mode)).expect("tree permissions");
    }
    if root.is_dir() {
        for entry in fs::read_dir(root).expect("tree entries") {
            make_tree_writable(&entry.expect("tree entry").path());
        }
    }
}
