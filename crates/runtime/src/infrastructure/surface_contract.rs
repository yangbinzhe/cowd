use serde::{Deserialize, Serialize};

use crate::capability::{CowdCapabilityRegistry, CowdSurface};
use crate::projection::CowdProjection;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdSurfaceProductContract {
    pub surface: CowdSurface,
    pub role: String,
    pub capability_count: usize,
    #[serde(default)]
    pub capability_ids: Vec<String>,
    #[serde(default)]
    pub primary_actions: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdSurfaceParityContract {
    pub contract_version: String,
    pub webui: CowdSurfaceProductContract,
    pub tui: CowdSurfaceProductContract,
    pub cli: CowdSurfaceProductContract,
    pub webui_tui_full_parity: bool,
    pub cli_is_minimal_control: bool,
}

impl CowdSurfaceParityContract {
    #[must_use]
    pub fn from_registry(registry: &CowdCapabilityRegistry) -> Self {
        let webui_projection = CowdProjection::for_surface(registry, CowdSurface::Webui);
        let tui_projection = CowdProjection::for_surface(registry, CowdSurface::Tui);
        let cli_projection = CowdProjection::for_surface(registry, CowdSurface::Cli);
        let webui_ids = capability_ids(&webui_projection);
        let tui_ids = capability_ids(&tui_projection);
        let cli_actions = unique_actions(&cli_projection);

        Self {
            contract_version: "cowd.surface_parity.v1".to_string(),
            webui: CowdSurfaceProductContract {
                surface: CowdSurface::Webui,
                role: "enhanced_management".to_string(),
                capability_count: webui_projection.capability_count,
                capability_ids: webui_ids.clone(),
                primary_actions: unique_actions(&webui_projection),
                constraints: vec![
                    "browser_enhanced_filter_compare_batch_audit".to_string(),
                    "must_not_own_independent_state".to_string(),
                ],
            },
            tui: CowdSurfaceProductContract {
                surface: CowdSurface::Tui,
                role: "console_full_capability".to_string(),
                capability_count: tui_projection.capability_count,
                capability_ids: tui_ids.clone(),
                primary_actions: unique_actions(&tui_projection),
                constraints: vec![
                    "keyboard_first_console_operations".to_string(),
                    "must_not_own_independent_state".to_string(),
                ],
            },
            cli: CowdSurfaceProductContract {
                surface: CowdSurface::Cli,
                role: "minimal_core_control".to_string(),
                capability_count: cli_projection.capability_count,
                capability_ids: capability_ids(&cli_projection),
                primary_actions: cli_actions.clone(),
                constraints: vec![
                    "list_show_import_export_diagnose_only".to_string(),
                    "no_complex_state_management".to_string(),
                ],
            },
            webui_tui_full_parity: webui_ids == tui_ids,
            cli_is_minimal_control: cli_actions.iter().all(|action| {
                matches!(
                    action.as_str(),
                    "list" | "show" | "import" | "export" | "diagnose"
                )
            }),
        }
    }
}

fn capability_ids(projection: &CowdProjection) -> Vec<String> {
    projection
        .capabilities
        .iter()
        .map(|capability| capability.id.clone())
        .collect()
}

fn unique_actions(projection: &CowdProjection) -> Vec<String> {
    let mut actions = std::collections::BTreeSet::new();
    for capability in &projection.capabilities {
        for action in &capability.actions {
            actions.insert(action.clone());
        }
    }
    actions.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surface_contract_keeps_webui_and_tui_capability_parity() {
        let contract = CowdSurfaceParityContract::from_registry(&CowdCapabilityRegistry::core());

        assert!(contract.webui_tui_full_parity);
        assert_eq!(contract.webui.capability_ids, contract.tui.capability_ids);
        assert_eq!(contract.webui.role, "enhanced_management");
        assert_eq!(contract.tui.role, "console_full_capability");
    }

    #[test]
    fn surface_contract_keeps_cli_minimal() {
        let contract = CowdSurfaceParityContract::from_registry(&CowdCapabilityRegistry::core());

        assert!(contract.cli_is_minimal_control);
        assert_eq!(contract.cli.role, "minimal_core_control");
        assert!(contract
            .cli
            .constraints
            .contains(&"no_complex_state_management".to_string()));
        assert!(!contract
            .cli
            .primary_actions
            .contains(&"batch_manage".to_string()));
    }
}
