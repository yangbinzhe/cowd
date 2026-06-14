use serde::{Deserialize, Serialize};

use super::{server_manufacturing_domain_pack, server_manufacturing_skill_pack};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IaccApplicationSurfaceKind {
    Webui,
    Tui,
    Cli,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccApplicationSurface {
    pub surface: IaccApplicationSurfaceKind,
    pub role: String,
    #[serde(default)]
    pub entrypoints: Vec<String>,
    #[serde(default)]
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccApplicationDomain {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccApplicationDescriptor {
    pub app_id: String,
    pub name: String,
    pub version: String,
    pub layer: String,
    pub description: String,
    #[serde(default)]
    pub cowd_capabilities: Vec<String>,
    #[serde(default)]
    pub domains: Vec<IaccApplicationDomain>,
    #[serde(default)]
    pub skill_ids: Vec<String>,
    #[serde(default)]
    pub source_contracts: Vec<String>,
    #[serde(default)]
    pub surfaces: Vec<IaccApplicationSurface>,
}

#[must_use]
pub fn manufacturing_app_descriptor() -> IaccApplicationDescriptor {
    let domain = server_manufacturing_domain_pack();
    let skills = server_manufacturing_skill_pack();

    IaccApplicationDescriptor {
        app_id: "iacc.manufacturing".to_string(),
        name: "IACC Manufacturing Application".to_string(),
        version: domain.version.clone(),
        layer: "application".to_string(),
        description:
            "Manufacturing operations application over cowd structured data, context, memory, graph, skills and governance."
                .to_string(),
        cowd_capabilities: vec![
            "cowd.structured_data.core".to_string(),
            "cowd.context.runtime".to_string(),
            "cowd.memory.runtime".to_string(),
            "cowd.runtime.event".to_string(),
            "cowd.skill.lifecycle".to_string(),
            "cowd.connector.runtime".to_string(),
        ],
        domains: vec![IaccApplicationDomain {
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
            .map(|skill| format!("iacc:{}", skill.skill_id))
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
            IaccApplicationSurface {
                surface: IaccApplicationSurfaceKind::Webui,
                role: "enhanced_management".to_string(),
                entrypoints: vec![
                    "/api/iacc/app".to_string(),
                    "/api/cowd/projection?surface=webui".to_string(),
                ],
                actions: vec![
                    "browse_domain".to_string(),
                    "manage_source_packs".to_string(),
                    "inspect_evidence".to_string(),
                    "run_manufacturing_skills".to_string(),
                    "audit_quality".to_string(),
                ],
            },
            IaccApplicationSurface {
                surface: IaccApplicationSurfaceKind::Tui,
                role: "console_full_capability".to_string(),
                entrypoints: vec![
                    "/api/iacc/app".to_string(),
                    "/api/cowd/projection?surface=tui".to_string(),
                ],
                actions: vec![
                    "browse_domain".to_string(),
                    "inspect_source_packs".to_string(),
                    "inspect_evidence".to_string(),
                    "run_manufacturing_skills".to_string(),
                    "diagnose_quality".to_string(),
                ],
            },
            IaccApplicationSurface {
                surface: IaccApplicationSurfaceKind::Cli,
                role: "minimal_core_control".to_string(),
                entrypoints: vec!["/api/iacc/app".to_string()],
                actions: vec!["show".to_string(), "export".to_string(), "diagnose".to_string()],
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manufacturing_app_descriptor_marks_iacc_as_application_layer() {
        let descriptor = manufacturing_app_descriptor();

        assert_eq!(descriptor.app_id, "iacc.manufacturing");
        assert_eq!(descriptor.layer, "application");
        assert!(descriptor
            .cowd_capabilities
            .contains(&"cowd.structured_data.core".to_string()));
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
            .contains(&"iacc:supply-risk-analyst".to_string()));
    }

    #[test]
    fn manufacturing_app_descriptor_keeps_cli_minimal() {
        let descriptor = manufacturing_app_descriptor();
        let cli = descriptor
            .surfaces
            .iter()
            .find(|surface| surface.surface == IaccApplicationSurfaceKind::Cli)
            .expect("cli surface should be described");

        assert_eq!(cli.role, "minimal_core_control");
        assert_eq!(cli.actions, vec!["show", "export", "diagnose"]);
        assert!(!cli.actions.contains(&"manage_source_packs".to_string()));
    }
}
