use std::{collections::BTreeSet, fs};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use syn::visit::Visit;

use super::{has_flag, Roots};

#[derive(Debug, Deserialize)]
struct Registry {
    schema_version: u32,
    mode: String,
    legacy_source_digest: String,
    legacy_candidates: Vec<String>,
    authorities: Vec<Authority>,
}

#[derive(Debug, Deserialize)]
struct Authority {
    id: String,
    owner_crate: String,
    owner_module: String,
    canonical_writer: String,
    revision: String,
    fence: String,
    recovery: String,
    evidence: String,
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
    let mut registered = BTreeSet::new();
    for authority in &registry.authorities {
        if !registered.insert(authority.id.as_str()) {
            return Err(format!("duplicate state authority ID: {}", authority.id));
        }
        if authority.owner_crate.is_empty()
            || authority.owner_module.is_empty()
            || authority.canonical_writer.is_empty()
            || authority.revision.is_empty()
            || authority.fence.is_empty()
            || authority.recovery.is_empty()
            || authority.evidence.is_empty()
        {
            return Err(format!(
                "state authority {} has incomplete control metadata",
                authority.id
            ));
        }
    }
    let referenced = authority_references(source);
    if referenced != registered {
        let missing = referenced.difference(&registered).collect::<Vec<_>>();
        let stale = registered.difference(&referenced).collect::<Vec<_>>();
        return Err(format!(
            "authority registry mismatch: missing={missing:?} stale={stale:?}"
        ));
    }
    Ok(())
}

pub(super) fn validate_duplicate_policy(roots: &Roots) -> Result<usize, String> {
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
    let count = policy.candidates.len();
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
        let mut function_sets = Vec::new();
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
            function_sets.push(function_names(&body)?);
        }
        if candidate.classification == "adapter_with_duplicated_semantics" {
            let Some((first, rest)) = function_sets.split_first() else {
                unreachable!("candidate source count was validated")
            };
            let common = first
                .iter()
                .filter(|name| rest.iter().all(|functions| functions.contains(*name)))
                .count();
            if common == 0 {
                return Err(format!(
                    "candidate {} claims duplicated semantics but has no shared operation names",
                    candidate.id
                ));
            }
        }
    }
    Ok(count)
}

fn function_names(source: &str) -> Result<BTreeSet<String>, String> {
    #[derive(Default)]
    struct Functions(BTreeSet<String>);
    impl<'ast> Visit<'ast> for Functions {
        fn visit_item_fn(&mut self, function: &'ast syn::ItemFn) {
            self.0.insert(function.sig.ident.to_string());
            syn::visit::visit_item_fn(self, function);
        }

        fn visit_impl_item_fn(&mut self, function: &'ast syn::ImplItemFn) {
            self.0.insert(function.sig.ident.to_string());
            syn::visit::visit_impl_item_fn(self, function);
        }
    }
    let syntax =
        syn::parse_file(source).map_err(|error| format!("parse duplicate source: {error}"))?;
    let mut functions = Functions::default();
    functions.visit_file(&syntax);
    Ok(functions.0)
}

fn authority_references(source: &str) -> BTreeSet<&str> {
    let mut references = BTreeSet::new();
    let source = source
        .split_once("pub fn runtime_module_map()")
        .map_or(source, |(_, body)| body);
    for marker in [
        "authority(",
        "coordinator(",
        "worker(",
        "projector(",
        "adapter(",
        "external(",
    ] {
        let mut tail = source;
        while let Some(index) = tail.find(marker) {
            let call = &tail[index + marker.len()..];
            let Some(capability_start) = call.find('"') else {
                break;
            };
            let capability_tail = &call[capability_start + 1..];
            let Some(capability_end) = capability_tail.find('"') else {
                break;
            };
            let state_source = &capability_tail[capability_end + 1..];
            let Some(state_start) = state_source.find('"') else {
                break;
            };
            let state_tail = &state_source[state_start + 1..];
            let Some(state_end) = state_tail.find('"') else {
                break;
            };
            let state = &state_tail[..state_end];
            references.insert(state);
            tail = &state_tail[state_end + 1..];
        }
    }
    references
}

fn first_quoted(line: &str) -> Option<&str> {
    let start = line.find('"')? + 1;
    let end = line[start..].find('"')? + start;
    Some(&line[start..end])
}
