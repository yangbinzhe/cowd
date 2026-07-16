use serde::{Deserialize, Serialize};

use super::{server_manufacturing_domain_pack, server_manufacturing_skill_pack};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgApplicationSurfaceKind {
    Management,
    Tui,
    Cli,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgApplicationSurface {
    pub surface: MfgApplicationSurfaceKind,
    pub role: String,
    #[serde(default)]
    pub entrypoints: Vec<String>,
    #[serde(default)]
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgApplicationDomain {
    pub domain_id: String,
    pub name: String,
    pub industry: String,
    pub version: String,
    #[serde(default)]
    pub entity_types: Vec<String>,
    #[serde(default)]
    pub relation_types: Vec<String>,
    #[serde(default)]
    pub metric_ids: Vec<String>,
    pub scenario_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgApplicationDescriptor {
    pub app_id: String,
    pub name: String,
    pub version: String,
    pub layer: String,
    pub description: String,
    #[serde(default)]
    pub cowd_capabilities: Vec<String>,
    #[serde(default)]
    pub domains: Vec<MfgApplicationDomain>,
    #[serde(default)]
    pub skill_ids: Vec<String>,
    #[serde(default)]
    pub source_contracts: Vec<String>,
    #[serde(default)]
    pub surfaces: Vec<MfgApplicationSurface>,
}

#[must_use]
pub fn manufacturing_app_descriptor() -> MfgApplicationDescriptor {
    let domain = server_manufacturing_domain_pack();
    let skills = server_manufacturing_skill_pack();

    MfgApplicationDescriptor {
        app_id: "mfg.manufacturing".to_string(),
        name: "MFG Manufacturing Application".to_string(),
        version: domain.version.clone(),
        layer: "application".to_string(),
        description:
            "Manufacturing operations application over Matrix structured facts, Memory, context, skills and governance."
                .to_string(),
        cowd_capabilities: vec![
            "cowd.structured_data.core".to_string(),
            "cowd.context.runtime".to_string(),
            "cowd.memory.runtime".to_string(),
            "cowd.runtime.event".to_string(),
            "cowd.skill.lifecycle".to_string(),
            "cowd.connector.runtime".to_string(),
        ],
        domains: vec![MfgApplicationDomain {
            domain_id: domain.domain_id,
            name: domain.name,
            industry: domain.industry,
            version: domain.version,
            entity_types: domain.entity_types,
            relation_types: domain.relation_types,
            metric_ids: domain.metric_ids,
            scenario_count: domain.scenarios.len(),
        }],
        skill_ids: skills
            .into_iter()
            .map(|skill| format!("mfg:{}", skill.skill_id))
            .collect(),
        source_contracts: vec![
            "source_pack".to_string(),
            "entity_mapping".to_string(),
            "fact_mapping".to_string(),
            "fact".to_string(),
            "evidence_packet".to_string(),
            "ingest_plan".to_string(),
        ],
        surfaces: vec![
            MfgApplicationSurface {
                surface: MfgApplicationSurfaceKind::Management,
                role: "enhanced_management".to_string(),
                entrypoints: vec![
                    "/api/apps/mfg/app".to_string(),
                    "/api/cowd/projection?surface=management".to_string(),
                ],
                actions: vec![
                    "browse_domain".to_string(),
                    "manage_source_packs".to_string(),
                    "inspect_evidence".to_string(),
                    "run_manufacturing_skills".to_string(),
                    "audit_quality".to_string(),
                ],
            },
            MfgApplicationSurface {
                surface: MfgApplicationSurfaceKind::Tui,
                role: "console_read_only".to_string(),
                entrypoints: vec![
                    "/api/apps/mfg/contract".to_string(),
                    "/mfg".to_string(),
                ],
                actions: Vec::new(),
            },
            MfgApplicationSurface {
                surface: MfgApplicationSurfaceKind::Cli,
                role: "minimal_core_control".to_string(),
                entrypoints: vec!["/api/apps/mfg/app".to_string()],
                actions: Vec::new(),
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manufacturing_app_descriptor_marks_mfg_as_application_layer() {
        let descriptor = manufacturing_app_descriptor();

        assert_eq!(descriptor.app_id, "mfg.manufacturing");
        assert_eq!(descriptor.layer, "application");
        assert!(descriptor
            .cowd_capabilities
            .contains(&"cowd.structured_data.core".to_string()));
        assert!(!descriptor
            .cowd_capabilities
            .contains(&"cowd.matrix.runtime".to_string()));
        assert!(!descriptor.source_contracts.is_empty());
    }

    #[test]
    fn manufacturing_app_descriptor_reuses_domain_and_skill_facts() {
        let descriptor = manufacturing_app_descriptor();

        assert_eq!(descriptor.domains.len(), 1);
        assert_eq!(descriptor.domains[0].domain_id, "server_manufacturing");
        assert!(descriptor.domains[0]
            .metric_ids
            .contains(&"material_shortage_risk".to_string()));
        assert!(descriptor
            .skill_ids
            .contains(&"mfg:supply-risk-analyst".to_string()));
    }

    #[test]
    fn manufacturing_app_descriptor_keeps_cli_minimal() {
        let descriptor = manufacturing_app_descriptor();
        let cli = descriptor
            .surfaces
            .iter()
            .find(|surface| surface.surface == MfgApplicationSurfaceKind::Cli)
            .expect("cli surface should be described");

        assert_eq!(cli.role, "minimal_core_control");
        assert!(cli.actions.is_empty());
        assert_eq!(cli.entrypoints, vec!["/api/apps/mfg/app"]);
    }

    #[test]
    fn manufacturing_app_descriptor_exposes_tui_as_read_only_without_operations() {
        let descriptor = manufacturing_app_descriptor();
        let tui = descriptor
            .surfaces
            .iter()
            .find(|surface| surface.surface == MfgApplicationSurfaceKind::Tui)
            .expect("TUI surface should be described");
        assert_eq!(tui.role, "console_read_only");
        assert!(tui.entrypoints.contains(&"/mfg".to_string()));
        assert!(tui.actions.is_empty());
    }
}
