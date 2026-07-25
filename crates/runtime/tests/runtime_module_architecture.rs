#![allow(clippy::expect_used)]

use std::collections::BTreeSet;

use runtime::{runtime_module_map, RuntimeDomain};

#[test]
fn runtime_module_map_covers_the_harness_lifecycle_once() {
    let map = runtime_module_map();
    assert!(!map.is_empty(), "runtime module map must not be empty");

    let module_names = map
        .iter()
        .map(|descriptor| descriptor.module)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        module_names.len(),
        map.len(),
        "runtime module descriptors must have unique module identities"
    );

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
            "runtime lifecycle domain {required:?} must be represented"
        );
    }

    for lifecycle_domain in [
        RuntimeDomain::Conversation,
        RuntimeDomain::Mission,
        RuntimeDomain::Session,
        RuntimeDomain::Agent,
        RuntimeDomain::Team,
        RuntimeDomain::Approval,
        RuntimeDomain::Recovery,
    ] {
        assert!(
            map.iter().any(|descriptor| {
                descriptor.domain == lifecycle_domain && descriptor.lifecycle_owner
            }),
            "runtime lifecycle domain {lifecycle_domain:?} needs an explicit owner"
        );
    }
}
