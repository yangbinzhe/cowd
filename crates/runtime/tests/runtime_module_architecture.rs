use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

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
        RuntimeDomain::Evolution,
        RuntimeDomain::Skill,
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
        "agent_runtime",
        "team_builder",
        "mission_command_router",
        "approval_queue",
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
fn runtime_source_files_are_grouped_by_architecture_domain() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let root_files = fs::read_dir(&src_dir)
        .expect("read runtime src")
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
        ["lib.rs", "module_map.rs"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "runtime root source files must stay limited to the crate entry and module map"
    );

    let expected_dirs = [
        "agent",
        "approval",
        "context",
        "conversation",
        "evolution",
        "infrastructure",
        "mission",
        "policy",
        "provider",
        "structured_data",
        "recovery",
        "session",
        "skill",
        "steward",
        "team",
        "tooling",
    ];
    for dir in expected_dirs {
        assert!(
            src_dir.join(dir).is_dir(),
            "runtime architecture directory `{dir}` must exist"
        );
    }
}

#[test]
fn runtime_module_map_modules_have_physical_domain_paths() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let lib_rs = fs::read_to_string(manifest_dir.join("src/lib.rs")).expect("read runtime lib.rs");
    let path_by_module = parse_path_attrs(&lib_rs);
    let allowed_root_modules = ["module_map", "structured_data"]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();

    let missing_paths = runtime_module_map()
        .into_iter()
        .filter(|descriptor| !allowed_root_modules.contains(descriptor.module))
        .filter(|descriptor| !path_by_module.contains_key(descriptor.module))
        .map(|descriptor| descriptor.module.to_string())
        .collect::<Vec<_>>();

    assert!(
        missing_paths.is_empty(),
        "runtime module map entries must have explicit physical paths: {missing_paths:#?}"
    );
}

#[test]
fn obsolete_telemetry_crate_is_not_present() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let crates_dir = manifest_dir
        .parent()
        .expect("runtime crate has crates parent directory");
    assert!(
        !crates_dir.join("telemetry").exists(),
        "telemetry must remain owned by model-protocol; obsolete crates/telemetry must not return"
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
            .or_else(|| trimmed.strip_prefix("mod "))
            .and_then(|rest| rest.trim_end_matches(';').split_whitespace().next());
        if let (Some(module), Some(path)) = (module, pending_path.take()) {
            path_by_module.insert(module.to_string(), path);
        }
    }
    path_by_module
}
