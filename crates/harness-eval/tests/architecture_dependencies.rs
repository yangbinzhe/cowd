#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    process::Command,
};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    resolve: Resolve,
}

#[derive(Debug, Deserialize)]
struct Package {
    id: String,
    name: String,
    manifest_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Resolve {
    nodes: Vec<Node>,
}

#[derive(Debug, Deserialize)]
struct Node {
    id: String,
    #[serde(default)]
    deps: Vec<NodeDependency>,
}

#[derive(Debug, Deserialize)]
struct NodeDependency {
    pkg: String,
    #[serde(default)]
    dep_kinds: Vec<DependencyKind>,
}

#[derive(Debug, Deserialize)]
struct DependencyKind {
    kind: Option<String>,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn cargo_metadata(manifest_path: &Path) -> Metadata {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(manifest_path)
        .output()
        .unwrap_or_else(|error| panic!("cargo metadata should start: {error}"));
    assert!(
        output.status.success(),
        "cargo metadata failed for {}: {}",
        manifest_path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("cargo metadata output should parse: {error}"))
}

fn normal_dependency_graph(metadata: &Metadata) -> BTreeMap<&str, Vec<&str>> {
    metadata
        .resolve
        .nodes
        .iter()
        .map(|node| {
            let dependencies = node
                .deps
                .iter()
                .filter(|dependency| {
                    dependency.dep_kinds.is_empty()
                        || dependency.dep_kinds.iter().any(|kind| kind.kind.is_none())
                })
                .map(|dependency| dependency.pkg.as_str())
                .collect();
            (node.id.as_str(), dependencies)
        })
        .collect()
}

fn package_id<'a>(metadata: &'a Metadata, name: &str) -> &'a str {
    metadata
        .packages
        .iter()
        .find(|package| package.name == name)
        .map(|package| package.id.as_str())
        .unwrap_or_else(|| panic!("workspace package `{name}` must exist"))
}

fn reachable_package_names(metadata: &Metadata, root: &str) -> BTreeSet<String> {
    let graph = normal_dependency_graph(metadata);
    let package_names = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package.name.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from([package_id(metadata, root)]);
    while let Some(id) = queue.pop_front() {
        if !seen.insert(id) {
            continue;
        }
        queue.extend(graph.get(id).into_iter().flatten().copied());
    }
    seen.into_iter()
        .filter_map(|id| package_names.get(id).map(|name| (*name).to_string()))
        .collect()
}

#[test]
fn core_contract_tools_and_tui_do_not_depend_on_runtime_implementations() {
    let metadata = cargo_metadata(&workspace_root().join("Cargo.toml"));
    for (package, forbidden) in [
        (
            "harness-contract",
            ["runtime", "gateway", "provider"].as_slice(),
        ),
        ("tools", ["runtime", "gateway", "provider"].as_slice()),
        ("tui", ["runtime", "gateway"].as_slice()),
    ] {
        let reachable = reachable_package_names(&metadata, package);
        for dependency in forbidden {
            assert!(
                !reachable.contains(*dependency),
                "{package} must not depend on `{dependency}` through its normal dependency graph: {reachable:?}"
            );
        }
    }
}

#[test]
fn edge_workspace_does_not_reverse_depend_on_core_sources() {
    let core_root = workspace_root().canonicalize().expect("core root");
    let edge_root = core_root.parent().expect("core parent").join("cowd-edge");
    let metadata = cargo_metadata(&edge_root.join("Cargo.toml"));
    for package in metadata.packages {
        let manifest = package
            .manifest_path
            .canonicalize()
            .unwrap_or(package.manifest_path);
        assert!(
            !manifest.starts_with(&core_root),
            "edge package {} must not reverse-depend on core source tree: {}",
            package.name,
            manifest.display()
        );
    }
}
