//! Manufacturing application contracts.
//!
//! MFG is the manufacturing application layer built on Matrix structured facts,
//! Memory, runtime context, skills and governed action dispatch.

pub use crate::iacc::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_domain_pack, server_manufacturing_ontology_pack,
    server_manufacturing_seed_plan, server_manufacturing_skill_pack, IaccApplicationDescriptor,
    IaccApplicationDomain, IaccApplicationSurface, IaccApplicationSurfaceKind,
};

#[must_use]
pub fn manufacturing_app_descriptor() -> IaccApplicationDescriptor {
    let mut descriptor = crate::iacc::manufacturing_app_descriptor();
    descriptor.app_id = "mfg.manufacturing".to_string();
    descriptor.name = "MFG Manufacturing Application".to_string();
    descriptor.description =
        "Manufacturing operations application over Matrix structured facts, Memory, context, skills and governance."
            .to_string();
    if !descriptor
        .cowd_capabilities
        .contains(&"cowd.matrix.runtime".to_string())
    {
        descriptor
            .cowd_capabilities
            .insert(0, "cowd.matrix.runtime".to_string());
    }
    for surface in &mut descriptor.surfaces {
        for entrypoint in &mut surface.entrypoints {
            if entrypoint == "/api/iacc/app" {
                *entrypoint = "/api/apps/mfg/app".to_string();
            }
        }
    }
    descriptor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mfg_descriptor_projects_manufacturing_as_application_over_matrix() {
        let descriptor = manufacturing_app_descriptor();

        assert_eq!(descriptor.app_id, "mfg.manufacturing");
        assert_eq!(descriptor.layer, "application");
        assert!(descriptor
            .cowd_capabilities
            .contains(&"cowd.matrix.runtime".to_string()));
        assert_eq!(descriptor.domains[0].domain_id, "server_manufacturing");
    }

    #[test]
    fn mfg_descriptor_exposes_mfg_app_entrypoints() {
        let descriptor = manufacturing_app_descriptor();

        assert!(descriptor.surfaces.iter().all(|surface| surface
            .entrypoints
            .iter()
            .any(|entrypoint| entrypoint == "/api/apps/mfg/app")));
    }
}
