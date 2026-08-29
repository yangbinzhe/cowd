use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use super::{has_flag, inventory::tracked_files, option_path, Roots};

const SOURCE_EXTENSIONS: &[&str] = &[
    "rs", "ts", "tsx", "js", "mjs", "vue", "css", "sh", "py", "go",
];

#[derive(Debug, Deserialize)]
struct Policy {
    schema_version: u32,
    max_lines: usize,
    generated: Vec<GeneratedException>,
    transitional: Vec<TransitionalException>,
}

#[derive(Debug, Deserialize)]
struct GeneratedException {
    repository: String,
    path: String,
    generator: String,
    source: String,
}

#[derive(Debug, Deserialize)]
struct TransitionalException {
    repository: String,
    path: String,
    phase: String,
}

#[derive(Debug, Serialize)]
struct SizeEntry {
    repository: String,
    path: String,
    lines: usize,
    disposition: String,
}

pub(super) fn run(roots: &Roots, arguments: &[String]) -> Result<(), String> {
    let policy_path = option_path(arguments, "--policy")?.unwrap_or_else(|| {
        roots
            .core
            .join("tests/test-governance/source-size-policy.yaml")
    });
    let policy: Policy = serde_yaml::from_str(
        &fs::read_to_string(&policy_path)
            .map_err(|error| format!("read {}: {error}", policy_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", policy_path.display()))?;
    if policy.schema_version != 1 || policy.max_lines == 0 {
        return Err("source-size policy requires schema_version=1 and max_lines>0".to_owned());
    }
    let final_mode = has_flag(arguments, "--final");
    let mut entries = Vec::new();
    collect_repository("core", &roots.core, &policy, final_mode, &mut entries)?;
    collect_repository("edge", &roots.edge, &policy, final_mode, &mut entries)?;
    validate_exception_paths(roots, &policy, &entries)?;
    let rendered = serde_json::to_string_pretty(&entries)
        .map_err(|error| format!("serialize source-size report: {error}"))?
        + "\n";
    if let Some(output) = option_path(arguments, "--output")? {
        fs::write(&output, rendered)
            .map_err(|error| format!("write {}: {error}", output.display()))?;
    } else if has_flag(arguments, "--check") || final_mode {
        let transitional = entries
            .iter()
            .filter(|entry| entry.disposition.starts_with("transitional:"))
            .count();
        println!(
            "source-size gate passed: oversized={} transitional={} generated={}",
            entries.len(),
            transitional,
            entries.len() - transitional
        );
    } else {
        print!("{rendered}");
    }
    Ok(())
}

fn collect_repository(
    name: &str,
    root: &Path,
    policy: &Policy,
    final_mode: bool,
    output: &mut Vec<SizeEntry>,
) -> Result<(), String> {
    for relative in tracked_files(root)? {
        let extension = relative.extension().and_then(|value| value.to_str());
        if !extension.is_some_and(|value| SOURCE_EXTENSIONS.contains(&value)) {
            continue;
        }
        let absolute = root.join(&relative);
        // `git ls-files` intentionally retains index entries for worktree
        // deletions. A phase which removes an oversized legacy source must be
        // auditable before commit, so deleted paths are not read as live
        // candidates.
        if !absolute.is_file() {
            continue;
        }
        let source = fs::read_to_string(&absolute)
            .map_err(|error| format!("read {}: {error}", absolute.display()))?;
        let lines = source.lines().count();
        if lines <= policy.max_lines {
            continue;
        }
        let path = relative.to_string_lossy().replace('\\', "/");
        if let Some(exception) = policy
            .generated
            .iter()
            .find(|entry| entry.repository == name && entry.path == path)
        {
            validate_generated(root, exception, &source)?;
            output.push(SizeEntry {
                repository: name.to_owned(),
                path,
                lines,
                disposition: format!("generated:{}", exception.generator),
            });
            continue;
        }
        if let Some(exception) = policy
            .transitional
            .iter()
            .find(|entry| entry.repository == name && entry.path == path)
        {
            if final_mode {
                return Err(format!(
                    "final source-size gate rejects transitional exception: {name}/{path}"
                ));
            }
            output.push(SizeEntry {
                repository: name.to_owned(),
                path,
                lines,
                disposition: format!("transitional:{}", exception.phase),
            });
            continue;
        }
        return Err(format!(
            "unregistered oversized source: {name}/{path} has {lines} lines (max {})",
            policy.max_lines
        ));
    }
    Ok(())
}

fn validate_generated(
    root: &Path,
    exception: &GeneratedException,
    source: &str,
) -> Result<(), String> {
    if !root.join(&exception.generator).is_file() {
        return Err(format!(
            "generated exception has missing generator: {}",
            exception.generator
        ));
    }
    if !root.join(&exception.source).is_file() {
        return Err(format!(
            "generated exception has missing source: {}",
            exception.source
        ));
    }
    let header = source
        .lines()
        .take(8)
        .collect::<Vec<_>>()
        .join("\n")
        .to_lowercase();
    if !header.contains("generated") {
        return Err(format!(
            "generated exception lacks a generated header: {}",
            exception.path
        ));
    }
    Ok(())
}

fn validate_exception_paths(
    roots: &Roots,
    policy: &Policy,
    oversized: &[SizeEntry],
) -> Result<(), String> {
    for (repository, path) in policy
        .generated
        .iter()
        .map(|entry| (&entry.repository, &entry.path))
        .chain(
            policy
                .transitional
                .iter()
                .map(|entry| (&entry.repository, &entry.path)),
        )
    {
        let root = match repository.as_str() {
            "core" => &roots.core,
            "edge" => &roots.edge,
            other => return Err(format!("unknown source-size repository: {other}")),
        };
        if !root.join(path).is_file() {
            return Err(format!(
                "source-size exception path does not exist: {repository}/{path}"
            ));
        }
        if !oversized
            .iter()
            .any(|entry| &entry.repository == repository && &entry.path == path)
        {
            return Err(format!(
                "stale source-size exception is no longer oversized: {repository}/{path}"
            ));
        }
    }
    Ok(())
}
