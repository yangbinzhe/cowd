use serde::{Deserialize, Serialize};

use crate::capability::{
    CowdCapability, CowdCapabilityRegistry, CowdSurface, CowdSurfaceAvailability, CowdSurfaceMode,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdProjection {
    pub surface: CowdSurface,
    pub capability_count: usize,
    #[serde(default)]
    pub capabilities: Vec<CowdProjectedCapability>,
    pub contract_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdProjectedCapability {
    pub id: String,
    pub name: String,
    pub layer: String,
    pub kind: String,
    pub status: String,
    pub owner_module: String,
    pub description: String,
    pub surface_mode: CowdSurfaceMode,
    #[serde(default)]
    pub actions: Vec<String>,
    #[serde(default)]
    pub required_permissions: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub management_fields: Vec<String>,
}

impl CowdProjection {
    #[must_use]
    pub fn for_surface(registry: &CowdCapabilityRegistry, surface: CowdSurface) -> Self {
        let capabilities = registry
            .capabilities
            .iter()
            .filter_map(|capability| project_capability(capability, surface))
            .collect::<Vec<_>>();

        Self {
            surface,
            capability_count: capabilities.len(),
            capabilities,
            contract_version: "cowd.projection.v1".to_string(),
        }
    }
}

fn project_capability(
    capability: &CowdCapability,
    surface: CowdSurface,
) -> Option<CowdProjectedCapability> {
    let availability = capability
        .surfaces
        .iter()
        .find(|candidate| candidate.surface == surface)?;
    if availability.mode == CowdSurfaceMode::Hidden {
        return None;
    }

    Some(CowdProjectedCapability {
        id: capability.id.clone(),
        name: capability.name.clone(),
        layer: format!("{:?}", capability.layer).to_ascii_lowercase(),
        kind: format!("{:?}", capability.kind).to_ascii_lowercase(),
        status: format!("{:?}", capability.status).to_ascii_lowercase(),
        owner_module: capability.owner_module.clone(),
        description: capability.description.clone(),
        surface_mode: availability.mode,
        actions: actions_for_surface(availability, surface),
        required_permissions: capability.required_permissions.clone(),
        depends_on: capability.depends_on.clone(),
        management_fields: management_fields_for_surface(surface),
    })
}

fn actions_for_surface(
    availability: &CowdSurfaceAvailability,
    surface: CowdSurface,
) -> Vec<String> {
    match surface {
        CowdSurface::Webui => availability.actions.clone(),
        CowdSurface::Tui => availability.actions.clone(),
        CowdSurface::Cli => availability
            .actions
            .iter()
            .filter(|action| {
                matches!(
                    action.as_str(),
                    "list" | "show" | "import" | "export" | "diagnose"
                )
            })
            .cloned()
            .collect(),
    }
}

fn management_fields_for_surface(surface: CowdSurface) -> Vec<String> {
    match surface {
        CowdSurface::Webui => vec![
            "filters".to_string(),
            "bulk_actions".to_string(),
            "comparison".to_string(),
            "visualization".to_string(),
            "audit_trail".to_string(),
            "quality_status".to_string(),
        ],
        CowdSurface::Tui => vec![
            "console_actions".to_string(),
            "detail_panel".to_string(),
            "diagnostics".to_string(),
            "keyboard_navigation".to_string(),
        ],
        CowdSurface::Cli => vec!["json_output".to_string(), "core_controls".to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_for_webui_contains_full_management_fields() {
        let projection =
            CowdProjection::for_surface(&CowdCapabilityRegistry::core(), CowdSurface::Webui);

        assert!(projection.capability_count >= 8);
        assert!(projection
            .capabilities
            .iter()
            .any(|capability| capability.id == "cowd.structured_data.core"));
        assert!(projection.capabilities.iter().all(|capability| capability
            .management_fields
            .contains(&"bulk_actions".to_string())));
    }

    #[test]
    fn projection_for_tui_contains_same_capability_set_with_console_actions() {
        let registry = CowdCapabilityRegistry::core();
        let webui = CowdProjection::for_surface(&registry, CowdSurface::Webui);
        let tui = CowdProjection::for_surface(&registry, CowdSurface::Tui);
        let webui_ids = webui
            .capabilities
            .iter()
            .map(|capability| capability.id.as_str())
            .collect::<Vec<_>>();
        let tui_ids = tui
            .capabilities
            .iter()
            .map(|capability| capability.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(webui_ids, tui_ids);
        assert!(tui.capabilities.iter().all(|capability| capability
            .management_fields
            .contains(&"console_actions".to_string())));
    }

    #[test]
    fn projection_for_cli_contains_minimal_core_controls() {
        let projection =
            CowdProjection::for_surface(&CowdCapabilityRegistry::core(), CowdSurface::Cli);

        assert!(projection.capability_count >= 8);
        assert!(projection.capabilities.iter().all(|capability| {
            capability.surface_mode == CowdSurfaceMode::Minimal
                && capability.management_fields
                    == vec!["json_output".to_string(), "core_controls".to_string()]
                && capability.actions.iter().all(|action| {
                    matches!(
                        action.as_str(),
                        "list" | "show" | "import" | "export" | "diagnose"
                    )
                })
        }));
    }
}
