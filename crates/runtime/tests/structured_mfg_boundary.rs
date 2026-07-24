#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

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
    let legacy_structured_mfg_terms = [
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
    forbidden.extend(legacy_structured_mfg_terms);

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
fn mfg_application_is_pinned_externally_and_host_composition_stays_generic() {
    let root = repo_root();
    let manifest = fs::read_to_string(root.join("crates/product-apps/Cargo.toml"))
        .expect("product APP manifest should load");
    let generated = fs::read_to_string(root.join("crates/product-apps/src/generated.rs"))
        .expect("generated product catalogue should load");
    let source_lock = fs::read_to_string(root.join("apps/mfg/source.lock.toml"))
        .expect("source lock should load");
    assert!(
        manifest.contains(
            "cowd-app-mfg-bundle = { git = \"https://gitee.com/eyeout/cowd-app-mfg\", rev = "
        ),
        "MFG must enter the product host through an immutable external bundle dependency"
    );
    let revision = source_lock
        .lines()
        .find_map(|line| line.trim().strip_prefix("rev = "))
        .map(|value| value.trim_matches('"'))
        .expect("MFG source lock revision");
    assert!(
        manifest.contains(&format!("rev = \"{revision}\"")),
        "generated dependency must use the reviewed MFG source-lock revision"
    );
    assert!(
        generated.contains(&format!("\"{revision}\"")),
        "runtime source-lock receipt must use the same reviewed revision"
    );
    assert!(
        generated.contains("cowd_app_mfg_bundle::product().with_source_lock("),
        "MFG must register through the generic static product boundary"
    );

    for (path, content) in read_rs_files(&root.join("crates/product-apps/src")) {
        let production = content.split("#[cfg(test)]").next().unwrap_or(&content);
        assert!(
            !production.contains("runtime::mfg"),
            "product host source {} must not depend on runtime::mfg",
            path.display()
        );
        assert!(
            !production.contains("webui"),
            "product host source {} must not depend on WebUI",
            path.display()
        );
        assert!(
            !production.contains("matrix_core::"),
            "generic product host source {} must not absorb MFG/Matrix domain logic",
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
fn gateway_production_uses_only_the_static_product_boundary_for_mfg() {
    let root = repo_root();
    let manifest = fs::read_to_string(root.join("crates/gateway/Cargo.toml"))
        .expect("Gateway manifest should load");
    let production_dependencies = manifest
        .split("[dev-dependencies]")
        .next()
        .expect("Gateway production dependency section");
    assert!(
        production_dependencies.contains("cowd-product-apps"),
        "Gateway production must depend on the generic product host"
    );
    assert!(
        !production_dependencies.contains("cowd-app-mfg-core"),
        "Gateway production must not depend directly on the MFG core crate"
    );

    for (path, content) in read_rs_files(&root.join("crates/gateway/src")) {
        let production = content.split("#[cfg(test)]").next().unwrap_or(&content);
        for term in ["app_mfg::", "cowd_app_mfg_"] {
            assert!(
                !production.contains(term),
                "Gateway production source {} bypasses the static product boundary through {term}",
                path.display()
            );
        }
        assert!(
            !production.contains("pub use matrix_repository::MatrixStore"),
            "Gateway production source {} must not re-export MatrixStore to APPs",
            path.display()
        );
    }
}
