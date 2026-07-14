#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::process::Command;

/// A normal downstream crate must not obtain Runtime's raw ledger type or a
/// `RuntimeServices::event_store()` accessor.  This is intentionally a real
/// compiler boundary test rather than a textual visibility scan.
#[test]
fn normal_runtime_dependency_cannot_construct_or_append_to_the_raw_event_store() {
    let root =
        tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("temporary downstream package");
    let runtime_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    fs::create_dir_all(root.path().join("src")).expect("source directory");
    fs::write(
        root.path().join("Cargo.toml"),
        format!(
            "[workspace]\n\n[package]\nname = \"v0-raw-writer-probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[dependencies]\nruntime = {{ path = {:?} }}\n",
            runtime_path
        ),
    )
    .expect("probe manifest");
    fs::write(
        root.path().join("src/main.rs"),
        "fn main() {\n    let services = runtime::RuntimeServices::in_memory().unwrap();\n    let _raw = runtime::RuntimeEventStore::try_open_in_memory();\n    let _store = services.event_store();\n}\n",
    )
    .expect("probe source");

    let output = Command::new(env!("CARGO"))
        .arg("check")
        .arg("--offline")
        .current_dir(root.path())
        .output()
        .expect("run downstream compile probe");
    assert!(
        !output.status.success(),
        "a normal dependency must not compile raw Runtime ledger access"
    );
    let diagnostics = String::from_utf8_lossy(&output.stderr);
    assert!(
        diagnostics.contains("RuntimeEventStore") || diagnostics.contains("event_store"),
        "compile failure must come from the denied raw writer surface: {diagnostics}"
    );
}
