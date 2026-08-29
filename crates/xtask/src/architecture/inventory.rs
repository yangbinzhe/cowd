use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{has_flag, option_path, Roots};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RepositorySnapshot {
    head: String,
    tree: String,
    version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CapabilityInventory {
    schema_version: u32,
    core: RepositorySnapshot,
    edge: RepositorySnapshot,
    runtime_modules: Vec<String>,
    legacy_lifecycle_owners: Vec<String>,
    gateway_routes: Vec<String>,
    tool_specs: Vec<String>,
    effect_resolver_digest: String,
    tui_api_paths: Vec<String>,
    edge_acceptance_entries: Vec<String>,
    contract_digest: String,
    migration_digest: String,
}

impl CapabilityInventory {
    fn validate(&self) -> Result<(), String> {
        unique("Runtime module", &self.runtime_modules)?;
        unique("Gateway route", &self.gateway_routes)?;
        unique("Tool spec", &self.tool_specs)?;
        unique("Edge acceptance", &self.edge_acceptance_entries)?;
        if self.runtime_modules.len() != 112 {
            return Err(format!(
                "Runtime module inventory drifted: expected 112, got {}",
                self.runtime_modules.len()
            ));
        }
        if self.gateway_routes.len() != 479 {
            return Err(format!(
                "Gateway route inventory drifted: expected 479, got {}",
                self.gateway_routes.len()
            ));
        }
        if self.tool_specs.len() != 53 {
            return Err(format!(
                "Tool inventory drifted: expected 53, got {}",
                self.tool_specs.len()
            ));
        }
        if self.edge_acceptance_entries.len() != 115 {
            return Err(format!(
                "Edge acceptance inventory drifted: expected 115, got {}",
                self.edge_acceptance_entries.len()
            ));
        }
        Ok(())
    }
}

pub(super) fn run(roots: &Roots, arguments: &[String]) -> Result<(), String> {
    let inventory = collect(roots)?;
    inventory.validate()?;
    let rendered = serde_json::to_string_pretty(&inventory)
        .map_err(|error| format!("serialize capability inventory: {error}"))?
        + "\n";
    let output = option_path(arguments, "--output")?;
    if let Some(path) = output {
        fs::write(&path, rendered).map_err(|error| format!("write {}: {error}", path.display()))?;
    } else if has_flag(arguments, "--check") {
        println!(
            "architecture inventory passed: runtime={} routes={} tools={} edge={} legacy_owners={}",
            inventory.runtime_modules.len(),
            inventory.gateway_routes.len(),
            inventory.tool_specs.len(),
            inventory.edge_acceptance_entries.len(),
            inventory.legacy_lifecycle_owners.len()
        );
    } else {
        print!("{rendered}");
    }
    Ok(())
}

fn collect(roots: &Roots) -> Result<CapabilityInventory, String> {
    let module_source = read(roots.core.join("crates/runtime/src/module_map.rs"))?;
    let runtime_modules =
        extract_call_first_strings(&module_source, "RuntimeModuleDescriptor::public(");
    let legacy_lifecycle_owners = module_source
        .lines()
        .filter(|line| {
            line.contains("RuntimeModuleDescriptor::public(") && line.contains(", true)")
        })
        .filter_map(first_quoted)
        .map(str::to_owned)
        .collect();
    let gateway_routes = collect_routes(&roots.core.join("crates/gateway/src/api_routes"))?;
    let tool_source = read(roots.core.join("crates/tools/src/registry/tool_specs.rs"))?;
    let tool_body = tool_source
        .split_once("pub fn mvp_tool_specs()")
        .map(|(_, tail)| tail)
        .and_then(|tail| tail.split_once("#[cfg(test)]").map(|(body, _)| body))
        .ok_or_else(|| "cannot locate mvp_tool_specs body".to_owned())?;
    let mut tool_specs = extract_field_strings(tool_body, "name:");
    tool_specs.sort();
    let resolver_body = tool_source
        .split_once("pub fn builtin_effect_resolver_spec")
        .map(|(_, tail)| tail)
        .and_then(|tail| {
            tail.split_once("pub(crate) fn normalize_tool_name")
                .map(|(body, _)| body)
        })
        .ok_or_else(|| "cannot locate builtin_effect_resolver_spec body".to_owned())?;
    let tui_api_paths = collect_api_paths(&roots.core.join("crates/tui/src"))?;
    let edge_manifest: serde_json::Value = serde_json::from_str(&read(
        roots
            .edge
            .join("surfaces/webui/evaluation/acceptance-manifest.json"),
    )?)
    .map_err(|error| format!("parse Edge acceptance manifest: {error}"))?;
    let mut edge_acceptance_entries = edge_manifest["entries"]
        .as_array()
        .ok_or_else(|| "Edge acceptance manifest has no entries array".to_owned())?
        .iter()
        .filter_map(|entry| entry["id"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    edge_acceptance_entries.sort();
    Ok(CapabilityInventory {
        schema_version: 1,
        core: repository_snapshot(&roots.core)?,
        edge: repository_snapshot(&roots.edge)?,
        runtime_modules,
        legacy_lifecycle_owners,
        gateway_routes,
        tool_specs,
        effect_resolver_digest: format!("{:x}", Sha256::digest(resolver_body.as_bytes())),
        tui_api_paths,
        edge_acceptance_entries,
        contract_digest: digest_tree(&roots.core, &["contracts/"])?,
        migration_digest: digest_tree(&roots.core, &["migrations/", "crates/storage/migrations/"])?,
    })
}

fn repository_snapshot(root: &Path) -> Result<RepositorySnapshot, String> {
    Ok(RepositorySnapshot {
        head: git(root, &["rev-parse", "HEAD"])?,
        tree: git(root, &["rev-parse", "HEAD^{tree}"])?,
        version: workspace_version(root)?,
    })
}

fn workspace_version(root: &Path) -> Result<String, String> {
    let manifest = read(root.join("Cargo.toml"))?;
    let parsed: toml::Value = toml::from_str(&manifest)
        .map_err(|error| format!("parse {}: {error}", root.join("Cargo.toml").display()))?;
    parsed
        .get("workspace")
        .and_then(|workspace| workspace.get("package"))
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("workspace.package.version missing in {}", root.display()))
}

fn git(root: &Path, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .map_err(|error| format!("run git in {}: {error}", root.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub(super) fn tracked_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let output = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(root)
        .output()
        .map_err(|error| format!("list tracked files in {}: {error}", root.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let mut paths = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .map(|value| PathBuf::from(String::from_utf8_lossy(value).into_owned()))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn digest_tree(root: &Path, prefixes: &[&str]) -> Result<String, String> {
    let mut digest = Sha256::new();
    for relative in tracked_files(root)? {
        let normalized = relative.to_string_lossy().replace('\\', "/");
        if !prefixes.iter().any(|prefix| normalized.starts_with(prefix)) {
            continue;
        }
        digest.update(normalized.as_bytes());
        digest.update([0]);
        digest.update(fs::read(root.join(&relative)).map_err(|error| {
            format!(
                "read {} for digest: {error}",
                root.join(&relative).display()
            )
        })?);
        digest.update([0]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn collect_api_paths(root: &Path) -> Result<Vec<String>, String> {
    let mut paths = BTreeSet::new();
    for file in recursive_files(root, "rs")? {
        let normalized = file.to_string_lossy().replace('\\', "/");
        if normalized.contains("/tests/") || normalized.ends_with("_test.rs") {
            continue;
        }
        let source = read(file)?;
        let mut tail = source.as_str();
        while let Some(index) = tail.find("\"/api/") {
            let value = &tail[index + 1..];
            if let Some(end) = value.find('"') {
                paths.insert(value[..end].to_owned());
                tail = &value[end + 1..];
            } else {
                break;
            }
        }
    }
    Ok(paths.into_iter().collect())
}

fn collect_routes(root: &Path) -> Result<Vec<String>, String> {
    let mut routes = BTreeSet::new();
    for file in recursive_files(root, "rs")? {
        let name = file
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if matches!(name, "route_manifest.rs" | "route_registry.rs") {
            continue;
        }
        let source = read(file)?;
        let mut offset = 0;
        while let Some(index) = source[offset..].find(".route(") {
            let start = offset + index + ".route(".len();
            let Some(end) = route_call_end(&source, start) else {
                break;
            };
            let call = source[start..end].trim_start();
            let Some(path_tail) = call.strip_prefix('"') else {
                offset = end;
                continue;
            };
            let Some(path_end) = path_tail.find('"') else {
                break;
            };
            let path = &path_tail[..path_end];
            let handlers = &path_tail[path_end + 1..];
            for (needle, method) in [
                ("get(", "GET"),
                ("post(", "POST"),
                ("put(", "PUT"),
                ("patch(", "PATCH"),
                ("delete(", "DELETE"),
            ] {
                if handlers.contains(needle) {
                    routes.insert(format!("{method} {path}"));
                }
            }
            offset = end;
        }
    }
    Ok(routes.into_iter().collect())
}

fn route_call_end(source: &str, start: usize) -> Option<usize> {
    let mut depth = 1usize;
    let mut in_string = false;
    let mut escaped = false;
    for (relative, character) in source[start..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            continue;
        }
        match character {
            '"' => in_string = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(start + relative);
                }
            }
            _ => {}
        }
    }
    None
}

fn recursive_files(root: &Path, extension: &str) -> Result<Vec<PathBuf>, String> {
    fn visit(path: &Path, extension: &str, output: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in
            fs::read_dir(path).map_err(|error| format!("read {}: {error}", path.display()))?
        {
            let entry = entry.map_err(|error| error.to_string())?;
            let child = entry.path();
            if child.is_dir() {
                visit(&child, extension, output)?;
            } else if child.extension().and_then(|value| value.to_str()) == Some(extension) {
                output.push(child);
            }
        }
        Ok(())
    }
    let mut output = Vec::new();
    visit(root, extension, &mut output)?;
    output.sort();
    Ok(output)
}

fn extract_call_first_strings(source: &str, marker: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut tail = source;
    while let Some(index) = tail.find(marker) {
        let call = &tail[index + marker.len()..];
        let Some(value) = first_quoted(call) else {
            break;
        };
        values.push(value.to_owned());
        tail = &call[value.len()..];
    }
    values
}

fn extract_field_strings(source: &str, field: &str) -> Vec<String> {
    source
        .lines()
        .filter(|line| line.trim_start().starts_with(field))
        .filter_map(first_quoted)
        .map(str::to_owned)
        .collect()
}

fn first_quoted(line: &str) -> Option<&str> {
    let start = line.find('"')? + 1;
    let end = line[start..].find('"')? + start;
    Some(&line[start..end])
}

fn unique(kind: &str, values: &[String]) -> Result<(), String> {
    let unique = values.iter().collect::<BTreeSet<_>>();
    if unique.len() == values.len() {
        Ok(())
    } else {
        Err(format!("{kind} inventory contains duplicate IDs"))
    }
}

fn read(path: impl AsRef<Path>) -> Result<String, String> {
    let path = path.as_ref();
    fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_parser_handles_chained_methods() {
        let source = r#"Router::new().route("/api/example", get(read).post(write))"#;
        let root = std::env::temp_dir().join(format!("cowd-route-parser-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("example_routes.rs"), source).unwrap();
        let routes = collect_routes(&root).unwrap();
        assert_eq!(routes, vec!["GET /api/example", "POST /api/example"]);
        std::fs::remove_dir_all(root).unwrap();
    }
}
