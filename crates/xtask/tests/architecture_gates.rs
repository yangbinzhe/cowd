//! The architecture gates that `cargo test` cannot see.
//!
//! `tests/test-governance/test-inventory.yaml` registers five architecture
//! gates. Only two of them are Rust tests (`runtime_module_architecture` and
//! `architecture_dependencies`), so they already run under `cargo test`. The
//! other three run from `scripts/validate.sh`, which meant a green
//! `cargo test --workspace` said nothing about them: `xtask architecture audit
//! --check` was failing with 42 structural violations while the same commit
//! reported thousands of passed tests.
//!
//! A regression gate that the green signal cannot see is not a gate. This test
//! runs the three uncovered ones from the workspace root and asserts each
//! exits cleanly, so "cargo test is green" now also means "the framework's
//! declared boundaries still hold".
//!
//! Cost is about 3.5 seconds, dominated by the dependency-direction scan.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `crates/xtask` -> workspace root.
///
/// Every gate resolves its roots from the current directory (see
/// `Roots::parse`), and a test process runs with the package directory as its
/// working directory. Spawning without this would silently check the wrong
/// tree - or fail with "Core root is not a Cargo workspace".
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask belongs to the Cowd workspace")
        .to_path_buf()
}

/// Run one gate and report it by name with its full output on failure, so the
/// failure names the gate and the violation rather than just "assertion
/// failed".
fn run_gate(name: &str, program: &str, arguments: &[&str]) {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(workspace_root())
        .output()
        .unwrap_or_else(|error| panic!("architecture gate `{name}`: spawn {program}: {error}"));
    assert!(
        output.status.success(),
        "architecture gate `{name}` failed with {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Assert a search returns nothing. ripgrep exits 0 when it matches and 1 when
/// it does not, so the "clean" answer is either exit 1 or empty stdout; the
/// empty-output assertion is the one that cannot be misread.
fn assert_no_matches(name: &str, program: &str, arguments: &[&str]) {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(workspace_root())
        .output()
        .unwrap_or_else(|error| panic!("`{name}`: spawn {program}: {error}"));
    let matches = String::from_utf8_lossy(&output.stdout);
    assert!(
        matches.trim().is_empty(),
        "`{name}` expected no matches, found:\n{matches}",
    );
}

#[test]
fn architecture_gates_stay_green_under_cargo_test() {
    // Covers inventory, source-size, structural-limits, duplicate-authority
    // and duplicate-capability in one pass.
    run_gate(
        "xtask architecture audit",
        env!("CARGO_BIN_EXE_xtask"),
        &["architecture", "audit", "--check"],
    );

    // Dependency direction, the single-AI-loop rule, and the removed-capability
    // scan (`check-boundaries.sh`).
    run_gate(
        "check-boundaries",
        "bash",
        &["scripts/architecture/check-boundaries.sh"],
    );

    // The retired `lifecycle_owner` field must stay retired. This is the part of
    // `capability-authority-gate.sh` that `audit` does not already cover; its
    // other two steps are a `cargo test` target and `duplicate-authority`,
    // which `audit` runs.
    assert_no_matches(
        "retired lifecycle_owner field",
        "rg",
        &[
            "-n",
            r"lifecycle_owner\s*:",
            "crates/runtime/src",
            "crates/harness-eval/src",
            "--glob",
            "*.rs",
        ],
    );
}
