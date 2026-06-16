use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdCapabilityLayer {
    Kernel,
    Application,
    Surface,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdCapabilityKind {
    Runtime,
    Context,
    Memory,
    StructuredData,
    Matrix,
    Event,
    Graph,
    Skill,
    Governance,
    Connector,
    Manufacturing,
    Surface,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdCapabilityStatus {
    Available,
    Preview,
    Planned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdSurface {
    Webui,
    Tui,
    Cli,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdSurfaceMode {
    Full,
    Enhanced,
    Minimal,
    Hidden,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdSurfaceAvailability {
    pub surface: CowdSurface,
    pub mode: CowdSurfaceMode,
    #[serde(default)]
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdCapability {
    pub id: String,
    pub name: String,
    pub layer: CowdCapabilityLayer,
    pub kind: CowdCapabilityKind,
    pub status: CowdCapabilityStatus,
    pub owner_module: String,
    pub description: String,
    #[serde(default)]
    pub required_permissions: Vec<String>,
    #[serde(default)]
    pub surfaces: Vec<CowdSurfaceAvailability>,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdCapabilityRegistry {
    #[serde(default)]
    pub capabilities: Vec<CowdCapability>,
}

impl CowdCapabilityRegistry {
    #[must_use]
    pub fn core() -> Self {
        Self {
            capabilities: vec![
                kernel_capability(
                    "cowd.runtime.session",
                    "Runtime Session",
                    CowdCapabilityKind::Runtime,
                    "runtime::session",
                    "Durable session lifecycle and conversation runtime.",
                    &["read:runtime", "write:runtime"],
                ),
                kernel_capability(
                    "cowd.context.runtime",
                    "Context Runtime",
                    CowdCapabilityKind::Context,
                    "runtime::context_runtime",
                    "Context envelope assembly, scoring, visibility and evidence references.",
                    &["read:context", "write:context"],
                ),
                kernel_capability(
                    "cowd.memory.runtime",
                    "Memory Runtime",
                    CowdCapabilityKind::Memory,
                    "cowd-memory",
                    "Long-term recall, cognitive layers, links and maintenance state.",
                    &["read:memory", "write:memory"],
                ),
                kernel_capability(
                    "cowd.structured_data.core",
                    "Structured Data Core",
                    CowdCapabilityKind::StructuredData,
                    "runtime::structured_data",
                    "Source, mapping, fact, evidence, ingest, watermark and delta contracts.",
                    &["read:structured_data", "write:structured_data"],
                ),
                kernel_capability(
                    "cowd.matrix.runtime",
                    "Matrix Runtime",
                    CowdCapabilityKind::Matrix,
                    "runtime::matrix",
                    "Structured fact engine for entities, relations, facts, metrics, evidence, lineage and compute.",
                    &["read:matrix", "write:matrix"],
                ),
                kernel_capability(
                    "cowd.runtime.event",
                    "Runtime Event",
                    CowdCapabilityKind::Event,
                    "runtime::cowd_event",
                    "Unified runtime event and timeline projection substrate.",
                    &["read:event"],
                ),
                kernel_capability(
                    "cowd.skill.lifecycle",
                    "Skill Lifecycle",
                    CowdCapabilityKind::Skill,
                    "runtime::skill_activation",
                    "Skill activation, memory, governance and execution lifecycle.",
                    &["read:skill", "write:skill"],
                ),
                kernel_capability(
                    "cowd.connector.runtime",
                    "Connector Runtime",
                    CowdCapabilityKind::Connector,
                    "runtime::connector",
                    "External account, service, resource and cross-plane connector management.",
                    &["read:connector", "write:connector"],
                ),
                application_capability(
                    "mfg.manufacturing.application",
                    "MFG Manufacturing Application",
                    CowdCapabilityKind::Manufacturing,
                    "runtime::mfg",
                    "Manufacturing upper application over Matrix structured facts, memory, context and skill capabilities.",
                    &[
                        "cowd.matrix.runtime",
                        "cowd.structured_data.core",
                        "cowd.context.runtime",
                        "cowd.memory.runtime",
                        "cowd.skill.lifecycle",
                    ],
                ),
                application_capability(
                    "iacc.manufacturing.application",
                    "IACC Manufacturing Application",
                    CowdCapabilityKind::Manufacturing,
                    "runtime::iacc",
                    "Manufacturing upper application over Matrix structured facts, memory, context and skill capabilities.",
                    &[
                        "cowd.matrix.runtime",
                        "cowd.structured_data.core",
                        "cowd.context.runtime",
                        "cowd.memory.runtime",
                        "cowd.skill.lifecycle",
                    ],
                ),
            ],
        }
    }

    #[must_use]
    pub fn capability(&self, id: &str) -> Option<&CowdCapability> {
        self.capabilities
            .iter()
            .find(|capability| capability.id == id)
    }

    #[must_use]
    pub fn by_surface(&self, surface: CowdSurface) -> Vec<CowdCapability> {
        self.capabilities
            .iter()
            .filter(|capability| {
                capability.surfaces.iter().any(|availability| {
                    availability.surface == surface && availability.mode != CowdSurfaceMode::Hidden
                })
            })
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn ids_are_unique(&self) -> bool {
        let mut ids = std::collections::BTreeSet::new();
        self.capabilities
            .iter()
            .all(|capability| ids.insert(capability.id.as_str()))
    }
}

fn kernel_capability(
    id: &str,
    name: &str,
    kind: CowdCapabilityKind,
    owner_module: &str,
    description: &str,
    required_permissions: &[&str],
) -> CowdCapability {
    CowdCapability {
        id: id.to_string(),
        name: name.to_string(),
        layer: CowdCapabilityLayer::Kernel,
        kind,
        status: CowdCapabilityStatus::Available,
        owner_module: owner_module.to_string(),
        description: description.to_string(),
        required_permissions: required_permissions
            .iter()
            .map(|permission| (*permission).to_string())
            .collect(),
        surfaces: full_surface_availability(),
        depends_on: Vec::new(),
    }
}

fn application_capability(
    id: &str,
    name: &str,
    kind: CowdCapabilityKind,
    owner_module: &str,
    description: &str,
    depends_on: &[&str],
) -> CowdCapability {
    CowdCapability {
        id: id.to_string(),
        name: name.to_string(),
        layer: CowdCapabilityLayer::Application,
        kind,
        status: CowdCapabilityStatus::Preview,
        owner_module: owner_module.to_string(),
        description: description.to_string(),
        required_permissions: vec!["read:iacc".to_string(), "write:iacc".to_string()],
        surfaces: full_surface_availability(),
        depends_on: depends_on
            .iter()
            .map(|capability_id| (*capability_id).to_string())
            .collect(),
    }
}

fn full_surface_availability() -> Vec<CowdSurfaceAvailability> {
    vec![
        CowdSurfaceAvailability {
            surface: CowdSurface::Webui,
            mode: CowdSurfaceMode::Enhanced,
            actions: vec![
                "browse".to_string(),
                "filter".to_string(),
                "compare".to_string(),
                "batch_manage".to_string(),
                "audit".to_string(),
            ],
        },
        CowdSurfaceAvailability {
            surface: CowdSurface::Tui,
            mode: CowdSurfaceMode::Full,
            actions: vec![
                "browse".to_string(),
                "inspect".to_string(),
                "trigger".to_string(),
                "diagnose".to_string(),
            ],
        },
        CowdSurfaceAvailability {
            surface: CowdSurface::Cli,
            mode: CowdSurfaceMode::Minimal,
            actions: vec![
                "list".to_string(),
                "show".to_string(),
                "import".to_string(),
                "export".to_string(),
                "diagnose".to_string(),
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_registry_contains_core_runtime_capabilities() {
        let registry = CowdCapabilityRegistry::core();

        assert!(registry.ids_are_unique());
        assert!(registry.capability("cowd.runtime.session").is_some());
        assert!(registry.capability("cowd.context.runtime").is_some());
        assert!(registry.capability("cowd.memory.runtime").is_some());
        assert!(registry.capability("cowd.matrix.runtime").is_some());
        assert!(registry.capability("cowd.structured_data.core").is_some());
    }

    #[test]
    fn capability_registry_declares_matrix_as_kernel_fact_engine() {
        let registry = CowdCapabilityRegistry::core();
        let matrix = registry
            .capability("cowd.matrix.runtime")
            .expect("matrix runtime capability should exist");

        assert_eq!(matrix.layer, CowdCapabilityLayer::Kernel);
        assert_eq!(matrix.kind, CowdCapabilityKind::Matrix);
        assert_eq!(matrix.owner_module, "runtime::matrix");
        assert!(matrix.description.contains("Structured fact engine"));
    }

    #[test]
    fn capability_registry_marks_iacc_as_application_capability() {
        let registry = CowdCapabilityRegistry::core();
        let iacc = registry
            .capability("iacc.manufacturing.application")
            .expect("iacc app capability should exist");

        assert_eq!(iacc.layer, CowdCapabilityLayer::Application);
        assert!(iacc.depends_on.contains(&"cowd.matrix.runtime".to_string()));
        assert!(iacc
            .depends_on
            .contains(&"cowd.structured_data.core".to_string()));
        assert_eq!(iacc.owner_module, "runtime::iacc");
    }

    #[test]
    fn capability_registry_declares_mfg_as_application_over_matrix() {
        let registry = CowdCapabilityRegistry::core();
        let mfg = registry
            .capability("mfg.manufacturing.application")
            .expect("mfg app capability should exist");

        assert_eq!(mfg.layer, CowdCapabilityLayer::Application);
        assert_eq!(mfg.owner_module, "runtime::mfg");
        assert!(mfg.depends_on.contains(&"cowd.matrix.runtime".to_string()));
    }

    #[test]
    fn capability_registry_exposes_all_capabilities_to_webui_and_tui_but_cli_minimal() {
        let registry = CowdCapabilityRegistry::core();
        let webui = registry.by_surface(CowdSurface::Webui);
        let tui = registry.by_surface(CowdSurface::Tui);
        let cli = registry.by_surface(CowdSurface::Cli);

        assert_eq!(webui.len(), tui.len());
        assert_eq!(cli.len(), webui.len());
        assert!(cli.iter().all(|capability| capability
            .surfaces
            .iter()
            .any(|surface| surface.surface == CowdSurface::Cli
                && surface.mode == CowdSurfaceMode::Minimal)));
    }
}
