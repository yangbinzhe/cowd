#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[test]
fn memory_source_files_are_grouped_by_architecture_domain() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let root_files = fs::read_dir(&src_dir)
        .expect("read memory src")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .map(|path| {
            path.file_name()
                .expect("source file name")
                .to_string_lossy()
                .to_string()
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(
        root_files,
        ["lib.rs"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "memory root source files must stay limited to the crate entry"
    );

    for dir in [
        "compression",
        "graph",
        "ingestion",
        "kernel",
        "knowledge",
        "layers",
        "lifecycle",
        "ops",
        "search",
        "session",
        "store",
    ] {
        assert!(
            src_dir.join(dir).is_dir(),
            "memory architecture directory `{dir}` must exist"
        );
    }
}

#[test]
fn memory_public_modules_have_explicit_physical_paths_when_moved() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let lib_rs = fs::read_to_string(manifest_dir.join("src/lib.rs")).expect("read memory lib.rs");
    let path_by_module = parse_path_attrs(&lib_rs);
    let directory_modules = ["compression", "knowledge", "layers", "search", "store"]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();

    let mut missing_paths = Vec::new();
    for line in lib_rs.lines() {
        let Some(module) = line
            .trim()
            .strip_prefix("pub mod ")
            .and_then(|rest| rest.trim_end_matches(';').split_whitespace().next())
        else {
            continue;
        };
        if directory_modules.contains(module) {
            continue;
        }
        if !path_by_module.contains_key(module) {
            missing_paths.push(module.to_string());
        }
    }

    assert!(
        missing_paths.is_empty(),
        "moved memory modules must declare explicit architecture paths: {missing_paths:#?}"
    );
}

fn parse_path_attrs(lib_rs: &str) -> BTreeMap<String, String> {
    let mut path_by_module = BTreeMap::new();
    let mut pending_path = None::<String>;
    for line in lib_rs.lines() {
        let trimmed = line.trim();
        if let Some(path) = trimmed
            .strip_prefix("#[path = \"")
            .and_then(|rest| rest.strip_suffix("\"]"))
        {
            pending_path = Some(path.to_string());
            continue;
        }
        let module = trimmed
            .strip_prefix("pub mod ")
            .or_else(|| trimmed.strip_prefix("pub(crate) mod "))
            .and_then(|rest| rest.trim_end_matches(';').split_whitespace().next());
        if let (Some(module), Some(path)) = (module, pending_path.take()) {
            path_by_module.insert(module.to_string(), path);
        }
    }
    path_by_module
}
