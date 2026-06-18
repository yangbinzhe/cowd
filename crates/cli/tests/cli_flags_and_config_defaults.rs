use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use runtime::Session;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn status_command_applies_model_and_permission_mode_flags() {
    // given
    let temp_dir = unique_temp_dir("status-flags");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    // when
    let output = Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(&temp_dir)
        .args([
            "--model",
            "claude-sonnet-4-6",
            "--permission-mode",
            "read-only",
            "status",
        ])
        .output()
        .expect("cowd should launch");

    // then
    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("Status"));
    assert!(stdout.contains("Model            claude-sonnet-4-6"));
    assert!(stdout.contains("Permission mode  read-only"));

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn resume_flag_rejects_status_slash_dispatch() {
    // given
    let temp_dir = unique_temp_dir("resume-status");
    let config_home = temp_dir.join("home").join(".cowd");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    write_session(&config_home, &temp_dir, "resume-status");

    // when
    let output = Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .args(["--resume", "resume-status", "/status"])
        .output()
        .expect("cowd should launch");

    // then
    assert_failure(&output);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(
        stderr.contains("was removed from the CLI surface"),
        "{stderr}"
    );
    assert!(
        stderr.contains("run slash commands inside the TUI"),
        "{stderr}"
    );

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn direct_slash_commands_are_tui_only() {
    // given
    let temp_dir = unique_temp_dir("slash-dispatch");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    // when
    let help_output = Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(&temp_dir)
        .arg("/help")
        .output()
        .expect("cowd should launch");
    let unknown_output = Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(&temp_dir)
        .arg("/zstats")
        .output()
        .expect("cowd should launch");

    // then
    assert_failure(&help_output);
    let help_stderr = String::from_utf8(help_output.stderr).expect("stderr should be utf8");
    assert!(help_stderr.contains("top-level slash commands were removed"));
    assert!(help_stderr.contains("Start the TUI"));

    assert_failure(&unknown_output);
    let stderr = String::from_utf8(unknown_output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("top-level slash commands were removed"));
    assert!(stderr.contains("Start the TUI"));

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn omc_namespaced_slash_commands_surface_a_targeted_compatibility_hint() {
    let temp_dir = unique_temp_dir("slash-dispatch-omc");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    let output = Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(&temp_dir)
        .arg("/oh-my-claudecode:hud")
        .output()
        .expect("cowd should launch");

    assert!(
        !output.status.success(),
        "stdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("top-level slash commands were removed"));
    assert!(stderr.contains("Start the TUI"));

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn resume_flag_rejects_config_slash_dispatch() {
    // given
    let temp_dir = unique_temp_dir("config-defaults");
    let config_home = temp_dir.join("home").join(".cowd");
    fs::create_dir_all(temp_dir.join(".cowd")).expect("project config dir should exist");
    fs::create_dir_all(&config_home).expect("home config dir should exist");

    fs::write(config_home.join("config.yaml"), r#"{"model":"haiku"}"#)
        .expect("write user settings");
    fs::write(
        temp_dir.join(".cowd").join("config.yaml"),
        r#"{"model":"sonnet"}"#,
    )
    .expect("write project settings");
    fs::write(
        temp_dir.join(".cowd").join("config.local.yaml"),
        r#"{"model":"opus"}"#,
    )
    .expect("write local settings");
    write_session(&config_home, &temp_dir, "config-defaults");

    // when
    let output = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .args(["--resume", "config-defaults", "/config", "model"])
        .output()
        .expect("cowd should launch");

    // then
    assert_failure(&output);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(
        stderr.contains("was removed from the CLI surface"),
        "{stderr}"
    );
    assert!(
        stderr.contains("run slash commands inside the TUI"),
        "{stderr}"
    );

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn doctor_command_runs_as_a_local_shell_entrypoint() {
    // given
    let temp_dir = unique_temp_dir("doctor-entrypoint");
    let config_home = temp_dir.join("home").join(".cowd");
    fs::create_dir_all(&config_home).expect("config home should exist");

    // when
    let output = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env("ANTHROPIC_BASE_URL", "http://127.0.0.1:9")
        .arg("doctor")
        .output()
        .expect("cowd doctor should launch");

    // then
    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("Doctor"));
    assert!(stdout.contains("Auth"));
    assert!(stdout.contains("Config"));
    assert!(stdout.contains("Workspace"));
    assert!(stdout.contains("Sandbox"));
    assert!(!stdout.contains("Thinking"));

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

#[test]
fn local_subcommand_help_does_not_fall_through_to_runtime_or_provider_calls() {
    let temp_dir = unique_temp_dir("subcommand-help");
    let config_home = temp_dir.join("home").join(".cowd");
    fs::create_dir_all(&config_home).expect("config home should exist");

    let doctor_help = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env("ANTHROPIC_BASE_URL", "http://127.0.0.1:9")
        .args(["doctor", "--help"])
        .output()
        .expect("doctor help should launch");
    let status_help = command_in(&temp_dir)
        .env("COWD_CONFIG_HOME", &config_home)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("ANTHROPIC_AUTH_TOKEN")
        .env("ANTHROPIC_BASE_URL", "http://127.0.0.1:9")
        .args(["status", "--help"])
        .output()
        .expect("status help should launch");

    assert_success(&doctor_help);
    let doctor_stdout = String::from_utf8(doctor_help.stdout).expect("stdout should be utf8");
    assert!(doctor_stdout.contains("Usage            cowd doctor"));
    assert!(doctor_stdout.contains("local-only health report"));
    assert!(!doctor_stdout.contains("Thinking"));

    assert_success(&status_help);
    let status_stdout = String::from_utf8(status_help.stdout).expect("stdout should be utf8");
    assert!(status_stdout.contains("Usage            cowd status"));
    assert!(status_stdout.contains("local workspace snapshot"));
    assert!(!status_stdout.contains("Thinking"));

    let doctor_stderr = String::from_utf8(doctor_help.stderr).expect("stderr should be utf8");
    let status_stderr = String::from_utf8(status_help.stderr).expect("stderr should be utf8");
    assert!(!doctor_stderr.contains("auth_unavailable"));
    assert!(!status_stderr.contains("auth_unavailable"));

    fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
}

fn command_in(cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cowd"));
    command.current_dir(cwd);
    command
}

fn write_session(config_home: &Path, root: &Path, label: &str) {
    fs::create_dir_all(config_home).expect("config home should exist");
    let mut session = Session::new().with_workspace_root(root.to_path_buf());
    session.session_id = label.to_string();
    session
        .push_user_text(format!("session fixture for {label}"))
        .expect("session write should succeed");

    let store = memory::UnifiedSessionStore::open(&config_home.join("sessions.db"))
        .expect("unified session store should open");
    let record = memory::SessionRecord {
        session_id: session.session_id.clone(),
        platform: "cli".to_string(),
        chat_id: session.session_id.clone(),
        user_id: None,
        model: session.model.clone(),
        created_at: "2026-06-05T00:00:00Z".to_string(),
        last_activity: "2026-06-05T00:00:00Z".to_string(),
        message_count: session.messages.len() as i64,
        reset_policy: "none".to_string(),
        metadata_json: Some(
            serde_json::json!({
                "workspace_root": session.workspace_root().map(|path| path.display().to_string()),
                "session_path": config_home.join("sessions.db").display().to_string(),
            })
            .to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    };
    let messages = session
        .messages
        .iter()
        .enumerate()
        .map(|(sequence, message)| message.to_session_message(&session.session_id, sequence))
        .collect::<Vec<_>>();

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime should create");
    runtime
        .block_on(async {
            store.upsert_session(&record).await?;
            store.delete_messages_from(&session.session_id, 0).await?;
            store.insert_messages_batch(&messages).await?;
            Ok::<(), memory::MemoryError>(())
        })
        .expect("fixture should persist to unified store");
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
