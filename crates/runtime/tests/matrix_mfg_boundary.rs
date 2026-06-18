use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root should resolve")
}

fn read_rs_files(root: &Path) -> Vec<(PathBuf, String)> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(&path).unwrap_or_else(|_| panic!("read dir {}", path.display())) {
            let entry = entry.expect("dir entry should load");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let content = fs::read_to_string(&path)
                    .unwrap_or_else(|_| panic!("read source {}", path.display()));
                files.push((path, content));
            }
        }
    }
    files
}

#[test]
fn matrix_kernel_has_no_mfg_or_manufacturing_coupling() {
    let matrix_root = repo_root().join("crates/matrix/core/src");
    let forbidden = [
        "server_manufacturing",
        "runtime::mfg",
        "crate::mfg",
        "Mfg",
        "mfg_",
        "webui",
    ];

    for (path, content) in read_rs_files(&matrix_root) {
        for term in forbidden {
            assert!(
                !content.contains(term),
                "Matrix kernel source {} must not contain forbidden coupling term {term}",
                path.display()
            );
        }
    }
}

#[test]
fn runtime_production_source_has_no_mfg_application_implementation() {
    let runtime_root = repo_root().join("crates/runtime/src");
    let legacy_matrix_mfg_terms = [
        "incident",
        "operational_analysis",
        "action_execution",
        "cockpit_profile",
        "cockpit_report",
        "memory_case",
        "playbook",
        "skill_execution",
    ]
    .map(|suffix| ["matrix", "_", suffix].concat());
    let legacy_adapter_terms = [
        ["Mfg", "Matrix", "Adapter"].concat(),
        ["open", "_mfg", "_matrix", "_adapter"].concat(),
    ];
    let mut forbidden = vec![
        "runtime::mfg",
        "crate::mfg",
        "mod mfg",
        "pub mod mfg",
        "seed_mfg",
        "server_manufacturing",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();
    forbidden.extend(legacy_adapter_terms);
    forbidden.extend(legacy_matrix_mfg_terms);

    for (path, content) in read_rs_files(&runtime_root) {
        let production = content.split("#[cfg(test)]").next().unwrap_or(&content);
        for term in &forbidden {
            assert!(
                !production.contains(term.as_str()),
                "runtime production source {} must not contain MFG application implementation term {term}",
                path.display()
            );
        }
    }
}

#[test]
fn mfg_application_is_allowed_to_depend_on_matrix_but_not_webui() {
    let mfg_root = repo_root().join("crates/app-mfg/src");
    let files = read_rs_files(&mfg_root);
    assert!(
        files
            .iter()
            .any(|(_, content)| content.contains("use matrix_core::")
                || content.contains("matrix_core::")),
        "MFG application crate should depend on Matrix contracts"
    );

    for (path, content) in files {
        assert!(
            !content.contains("runtime::mfg"),
            "MFG application source {} must not depend on runtime::mfg",
            path.display()
        );
        assert!(
            !content.contains("webui"),
            "MFG runtime source {} must not depend on WebUI",
            path.display()
        );
    }
}

#[test]
fn structured_data_contract_has_no_matrix_or_context_dependency() {
    let contract = repo_root().join("crates/runtime/src/structured_data/contract.rs");
    let content = fs::read_to_string(&contract)
        .unwrap_or_else(|_| panic!("read source {}", contract.display()));
    for term in [
        "crate::matrix",
        "Matrix",
        "ContextItem",
        "ContextRole",
        "ContextSourceKind",
    ] {
        assert!(
            !content.contains(term),
            "structured data contract must not contain forbidden dependency term {term}"
        );
    }
}

#[test]
fn gateway_depends_on_mfg_application_crate() {
    let manifest = repo_root().join("crates/gateway/Cargo.toml");
    let content = fs::read_to_string(&manifest)
        .unwrap_or_else(|_| panic!("read manifest {}", manifest.display()));
    assert!(
        content.contains("app-mfg"),
        "gateway must consume MFG through the application crate"
    );
}

#[test]
fn mfg_store_facade_does_not_expose_matrix_store() {
    let store = repo_root().join("crates/app-mfg/src/store.rs");
    let content =
        fs::read_to_string(&store).unwrap_or_else(|_| panic!("read source {}", store.display()));

    for term in [
        "impl Deref for MfgStore",
        "pub fn matrix(",
        "pub(crate) fn matrix(",
        "pub use runtime::MatrixStore",
    ] {
        assert!(
            !content.contains(term),
            "MFG store facade must not leak MatrixStore through {term}"
        );
    }
}
