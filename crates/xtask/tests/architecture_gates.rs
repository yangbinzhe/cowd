//! Blocking architecture invariants that `cargo test` cannot see directly.
//!
//! This test intentionally covers only protections that prevent ambiguous
//! control-plane ownership or accidental revival of a retired authority.
//! Broad style/debt scans such as `scripts/architecture/check-boundaries.sh`,
//! the 5000-line source-size report, and the 250-line structural report remain
//! available as manual commands but are not part of the scenario gate.

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
/// it does not. Only exit 1 with empty stdout is a clean search result.
fn assert_no_matches(name: &str, program: &str, arguments: &[&str]) {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(workspace_root())
        .output()
        .unwrap_or_else(|error| panic!("`{name}`: spawn {program}: {error}"));
    let matches = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(1),
        "`{name}` search did not report no matches: {:?}\n--- stdout ---\n{matches}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        output.stdout.is_empty(),
        "`{name}` expected no matches, found:\n{matches}",
    );
}

#[test]
fn no_matches_gate_rejects_search_errors_and_actual_matches() {
    assert_no_matches("clean search", "bash", &["-c", "exit 1"]);
    for script in [
        "printf 'search failed' >&2; exit 2",
        "printf 'matched source'; exit 0",
        "exit 0",
        "printf ' '; exit 1",
    ] {
        assert!(std::panic::catch_unwind(|| {
            assert_no_matches("invalid search", "bash", &["-c", script]);
        })
        .is_err());
    }
}

#[test]
fn architecture_gates_stay_green_under_cargo_test() {
    // Inventory completeness plus duplicate-authority/capability ownership.
    run_gate(
        "xtask architecture audit",
        env!("CARGO_BIN_EXE_xtask"),
        &["architecture", "audit", "--check"],
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
