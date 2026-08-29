#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};

use runtime::{
    audit_runtime_authorities, runtime_module_map, AuthorityScope, LifecycleRole, WriterKind,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Registry {
    mode: String,
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

#[test]
fn every_runtime_binding_resolves_to_exactly_one_global_authority() {
    let registry: Registry = serde_yaml::from_str(include_str!(
        "../../../tests/test-governance/state-authority-registry.yaml"
    ))
    .expect("state authority registry must parse");
    assert_eq!(registry.mode, "enforced");
    assert!(registry.legacy_candidates.is_empty());
    let mut global = BTreeMap::new();
    for authority in &registry.authorities {
        assert!(!authority.owner_crate.is_empty());
        assert!(!authority.owner_module.is_empty());
        assert!(!authority.canonical_writer.is_empty());
        assert!(!authority.revision.is_empty());
        assert!(!authority.fence.is_empty());
        assert!(!authority.recovery.is_empty());
        assert!(!authority.evidence.is_empty());
        assert!(global.insert(authority.id.as_str(), authority).is_none());
    }

    let modules = runtime_module_map();
    let audit = audit_runtime_authorities(&modules).expect("runtime authority audit");
    let referenced = modules
        .iter()
        .flat_map(|module| &module.role_bindings)
        .map(|binding| binding.state_authority_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(referenced, global.keys().copied().collect());
    for (state, module) in audit.local_authorities {
        let authority = global.get(state).expect("local state must be registered");
        assert_eq!(authority.owner_crate, "runtime");
        assert_eq!(authority.owner_module, module);
    }
    for state in audit.external_authorities {
        let authority = global
            .get(state)
            .expect("external state must be registered");
        assert_ne!(authority.owner_crate, "runtime");
    }
}

#[test]
fn non_authority_roles_cannot_write_canonical_state() {
    for module in runtime_module_map() {
        for binding in &module.role_bindings {
            assert!(binding.validate().is_ok(), "{}: {binding:?}", module.module);
            if binding.role != LifecycleRole::Authority {
                assert_ne!(binding.writer_kind, WriterKind::Canonical);
            }
            if binding.authority_scope == AuthorityScope::ExternalPort {
                assert_ne!(binding.role, LifecycleRole::Authority);
            }
        }
    }
}
