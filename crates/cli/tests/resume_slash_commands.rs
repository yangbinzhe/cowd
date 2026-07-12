#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::fs;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn resume_flag_rejects_non_interactive_slash_commands() {
    let temp_dir = unique_temp_dir("resume-slash-removed");
    fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    let output = Command::new(env!("CARGO_BIN_EXE_cowd"))
        .current_dir(&temp_dir)
        .args(["--resume", "latest", "/status"])
        .output()
        .expect("cowd should launch");

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

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be after epoch")
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{nanos}-{counter}"))
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected command to fail\nstdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
