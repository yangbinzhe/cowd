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
    let matrix_root = repo_root().join("crates/runtime/src/matrix");
    let forbidden = [
        "server_manufacturing",
        "runtime::mfg",
        "crate::mfg",
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
fn mfg_application_is_allowed_to_depend_on_matrix_but_not_webui() {
    let mfg_root = repo_root().join("crates/runtime/src/mfg");
    let files = read_rs_files(&mfg_root);
    assert!(
        files.iter().any(|(_, content)| content.contains("Matrix")),
        "MFG application boundary should document Matrix as its structured fact dependency"
    );

    for (path, content) in files {
        assert!(
            !content.contains("webui"),
            "MFG runtime source {} must not depend on WebUI",
            path.display()
        );
    }
}
