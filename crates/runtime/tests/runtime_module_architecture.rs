use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use runtime::{runtime_module_map, RuntimeDomain};

#[test]
fn runtime_module_map_covers_core_harness_lifecycle_domains() {
    let map = runtime_module_map();
    assert!(!map.is_empty(), "runtime module map must not be empty");

    let domains = map
        .iter()
        .map(|descriptor| descriptor.domain)
        .collect::<BTreeSet<_>>();
    for required in [
        RuntimeDomain::Conversation,
        RuntimeDomain::Provider,
        RuntimeDomain::Tooling,
        RuntimeDomain::Mission,
        RuntimeDomain::Session,
        RuntimeDomain::Agent,
        RuntimeDomain::Team,
        RuntimeDomain::Steward,
        RuntimeDomain::Approval,
        RuntimeDomain::Context,
        RuntimeDomain::Recovery,
        RuntimeDomain::Policy,
        RuntimeDomain::RealityBridge,
    ] {
        assert!(
            domains.contains(&required),
            "runtime domain {:?} must be represented",
            required
        );
    }

    for required_lifecycle_module in [
        "conversation",
        "provider_runtime_client",
        "tool_dispatch",
        "mission_runtime",
        "session_execution",
        "agent_lifecycle",
        "team_execution",
        "steward_runtime",
        "global_approval_queue",
        "runtime_event_store",
        "recovery",
    ] {
        assert!(
            map.iter()
                .any(|descriptor| descriptor.module == required_lifecycle_module
                    && descriptor.lifecycle_owner),
            "{required_lifecycle_module} must be classified as a lifecycle owner"
        );
    }
}

#[test]
fn runtime_root_public_modules_are_classified_by_domain() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let lib_rs = fs::read_to_string(manifest_dir.join("src/lib.rs")).expect("read runtime lib.rs");
    let classified = runtime_module_map()
        .into_iter()
        .map(|descriptor| descriptor.module.to_string())
        .collect::<BTreeSet<_>>();
    let allowed_public_modules = [
        "bash_validation",
        "branch_lock",
        "cached_prompt",
        "cowd_dirs",
        "effect",
        "error",
        "graph_contract",
        "green_contract",
        "json",
        "lsp_client",
        "module_map",
        "projection",
        "stale_base",
        "stale_branch",
        "summary_compression",
        "wave",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();

    let mut unclassified = BTreeMap::new();
    for line in lib_rs.lines() {
        let Some(name) = line
            .trim()
            .strip_prefix("pub mod ")
            .and_then(|rest| rest.trim_end_matches(';').split_whitespace().next())
        else {
            continue;
        };
        if !classified.contains(name) && !allowed_public_modules.contains(name) {
            unclassified.insert(name.to_string(), line.to_string());
        }
    }

    assert!(
        unclassified.is_empty(),
        "runtime root public modules must be classified in module_map.rs: {unclassified:#?}"
    );
}

#[test]
fn runtime_does_not_depend_on_surface_or_connector_contracts() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read Cargo.toml");
    for forbidden in ["surface", "connector", "tui", "gateway"] {
        assert!(
            !cargo_toml
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{forbidden} ="))),
            "runtime must not depend on {forbidden}"
        );
    }
}
