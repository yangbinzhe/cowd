use std::{collections::BTreeSet, fs};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{has_flag, Roots};

#[derive(Debug, Deserialize)]
struct Registry {
    schema_version: u32,
    mode: String,
    legacy_source_digest: String,
    legacy_candidates: Vec<String>,
    authorities: Vec<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct DuplicatePolicy {
    schema_version: u32,
    candidates: Vec<Candidate>,
}

#[derive(Debug, Deserialize)]
struct Candidate {
    id: String,
    phase: String,
    classification: String,
    owner: String,
    sources: Vec<Source>,
}

#[derive(Debug, Deserialize)]
struct Source {
    path: String,
    symbol: String,
    digest: String,
}

pub(super) fn run(roots: &Roots, arguments: &[String]) -> Result<(), String> {
    let registry_path = roots
        .core
        .join("tests/test-governance/state-authority-registry.yaml");
    let registry: Registry = serde_yaml::from_str(
        &fs::read_to_string(&registry_path)
            .map_err(|error| format!("read {}: {error}", registry_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", registry_path.display()))?;
    if registry.schema_version != 1 {
        return Err("unsupported state authority registry schema".to_owned());
    }
    let module_path = roots.core.join("crates/runtime/src/module_map.rs");
    let module_source = fs::read_to_string(&module_path)
        .map_err(|error| format!("read {}: {error}", module_path.display()))?;
    match registry.mode.as_str() {
        "migration_baseline" => validate_migration_baseline(&registry, &module_source)?,
        "enforced" => validate_enforced(&registry, &module_source)?,
        mode => return Err(format!("unknown state authority registry mode: {mode}")),
    }
    validate_duplicate_policy(roots)?;
    if has_flag(arguments, "--check") {
        println!(
            "duplicate-authority gate passed: mode={} legacy={} authorities={}",
            registry.mode,
            registry.legacy_candidates.len(),
            registry.authorities.len()
        );
    }
    Ok(())
}

fn validate_migration_baseline(registry: &Registry, source: &str) -> Result<(), String> {
    let actual_digest = format!("{:x}", Sha256::digest(source.as_bytes()));
    if actual_digest != registry.legacy_source_digest {
        return Err(
            "legacy module map changed without refreshing authority classification".to_owned(),
        );
    }
    let actual = source
        .lines()
        .filter(|line| {
            line.contains("RuntimeModuleDescriptor::public(") && line.contains(", true)")
        })
        .filter_map(first_quoted)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if actual != registry.legacy_candidates {
        return Err(format!(
            "legacy lifecycle candidates differ: registry={} source={}",
            registry.legacy_candidates.len(),
            actual.len()
        ));
    }
    Ok(())
}

fn validate_enforced(registry: &Registry, source: &str) -> Result<(), String> {
    if source.contains("lifecycle_owner") || source.contains(", true)") {
        return Err("enforced authority mode still contains legacy lifecycle ownership".to_owned());
    }
    if !registry.legacy_candidates.is_empty() || registry.authorities.is_empty() {
        return Err("enforced authority registry must replace every legacy candidate".to_owned());
    }
    Ok(())
}

fn validate_duplicate_policy(roots: &Roots) -> Result<(), String> {
    let path = roots
        .core
        .join("tests/test-governance/duplicate-capability-allowlist.yaml");
    let policy: DuplicatePolicy = serde_yaml::from_str(
        &fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", path.display()))?;
    if policy.schema_version != 1 {
        return Err("unsupported duplicate capability policy schema".to_owned());
    }
    let mut ids = BTreeSet::new();
    for candidate in policy.candidates {
        if !ids.insert(candidate.id.clone()) {
            return Err(format!(
                "duplicate capability candidate ID: {}",
                candidate.id
            ));
        }
        if candidate.phase.is_empty()
            || candidate.classification.is_empty()
            || candidate.owner.is_empty()
        {
            return Err(format!(
                "candidate {} lacks phase/classification/owner",
                candidate.id
            ));
        }
        if candidate.sources.len() < 2 {
            return Err(format!(
                "candidate {} has fewer than two source spans",
                candidate.id
            ));
        }
        for source in candidate.sources {
            let absolute = roots.core.join(&source.path);
            let body = fs::read_to_string(&absolute)
                .map_err(|error| format!("read {}: {error}", absolute.display()))?;
            if !body.contains(&source.symbol) {
                return Err(format!(
                    "candidate {} symbol {} is absent from {}",
                    candidate.id, source.symbol, source.path
                ));
            }
            let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
            if digest != source.digest {
                return Err(format!(
                    "candidate {} source digest changed for {}",
                    candidate.id, source.path
                ));
            }
        }
    }
    Ok(())
}

fn first_quoted(line: &str) -> Option<&str> {
    let start = line.find('"')? + 1;
    let end = line[start..].find('"')? + start;
    Some(&line[start..end])
}
