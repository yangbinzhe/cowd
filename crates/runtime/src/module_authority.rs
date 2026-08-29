use std::collections::{BTreeMap, BTreeSet};

pub use harness_contract::governance::{
    AuthorityScope, CapabilityRoleBinding, LifecycleRole, WriterKind,
};

use crate::module_map::RuntimeModuleDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityAudit {
    pub local_authorities: BTreeMap<&'static str, &'static str>,
    pub external_authorities: BTreeSet<&'static str>,
    pub capabilities: BTreeSet<&'static str>,
}

pub fn audit_runtime_authorities(
    modules: &[RuntimeModuleDescriptor],
) -> Result<AuthorityAudit, String> {
    let mut local_authorities = BTreeMap::new();
    let mut external_authorities = BTreeSet::new();
    let mut capabilities = BTreeSet::new();
    for module in modules {
        if module.role_bindings.is_empty() {
            return Err(format!(
                "Runtime module {} has no capability binding",
                module.module
            ));
        }
        for binding in &module.role_bindings {
            binding
                .validate()
                .map_err(|error| format!("{}: {error}", module.module))?;
            capabilities.insert(binding.capability_id);
            match binding.authority_scope {
                AuthorityScope::ExternalPort => {
                    external_authorities.insert(binding.state_authority_id);
                }
                AuthorityScope::Local if binding.role == LifecycleRole::Authority => {
                    if let Some(previous) =
                        local_authorities.insert(binding.state_authority_id, module.module)
                    {
                        return Err(format!(
                            "state authority {} is claimed by both {previous} and {}",
                            binding.state_authority_id, module.module
                        ));
                    }
                }
                AuthorityScope::Local => {}
            }
        }
    }
    for module in modules {
        for binding in &module.role_bindings {
            if binding.authority_scope == AuthorityScope::Local
                && !local_authorities.contains_key(binding.state_authority_id)
            {
                return Err(format!(
                    "module {} references local authority {} without an Authority",
                    module.module, binding.state_authority_id
                ));
            }
        }
    }
    Ok(AuthorityAudit {
        local_authorities,
        external_authorities,
        capabilities,
    })
}
