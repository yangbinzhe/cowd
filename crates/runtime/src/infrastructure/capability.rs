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
    Reality,
    Fact,
    Event,
    Graph,
    Skill,
    Governance,
    Connector,
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
                    "cowd.reality.core",
                    "Reality Core",
                    CowdCapabilityKind::Reality,
                    "gateway::services::RealityService",
                    "Read-only Reality Core projection across fact semantics, Memory, Matrix, Growth, Context, and Audit.",
                    &["read:reality"],
                ),
                kernel_capability(
                    "cowd.reality.fact_flow",
                    "Fact Flow",
                    CowdCapabilityKind::Fact,
                    "gateway::services::RealityService",
                    "Runtime trace of events, evidence, candidates, fact-kernel decisions, and Memory/Matrix targets.",
                    &["read:reality", "read:growth"],
                ),
                kernel_capability(
                    "cowd.fact.kernel",
                    "fact-kernel",
                    CowdCapabilityKind::Fact,
                    "fact-kernel",
                    "Internal fact semantics, evidence, confidence, promotion decisions, and fact health.",
                    &["read:reality"],
                ),
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
                    "Memory Engine",
                    CowdCapabilityKind::Memory,
                    "memory",
                    "Long-term recall, cognitive layers, links and maintenance state.",
                    &["read:memory", "write:memory"],
                ),
                kernel_capability(
                    "cowd.matrix.engine",
                    "Matrix Engine",
                    CowdCapabilityKind::StructuredData,
                    "matrix",
                    "Structured facts, entities, relations, metrics, evidence, lineage, and computation.",
                    &["read:matrix", "write:matrix"],
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
                    "cowd.growth.channel",
                    "Growth Channel",
                    CowdCapabilityKind::Fact,
                    "harness_contract::growth",
                    "Candidate promotion channel from runtime events into fact-kernel, Memory, and Matrix.",
                    &["read:growth", "write:growth"],
                ),
                kernel_capability(
                    "cowd.audit.trace",
                    "Audit Trace",
                    CowdCapabilityKind::Governance,
                    "gateway::services::AuditService",
                    "Approval, risk, promotion, and execution trace governance.",
                    &["read:audit"],
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
                    "runtime::skill",
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
        assert!(registry.capability("cowd.reality.core").is_some());
        assert!(registry.capability("cowd.reality.fact_flow").is_some());
        assert!(registry.capability("cowd.fact.kernel").is_some());
        assert!(registry.capability("cowd.context.runtime").is_some());
        assert!(registry.capability("cowd.memory.runtime").is_some());
        assert!(registry.capability("cowd.matrix.engine").is_some());
        assert!(registry.capability("cowd.growth.channel").is_some());
        assert!(registry.capability("cowd.audit.trace").is_some());
        assert!(registry.capability("cowd.structured_data.core").is_some());
    }

    #[test]
    fn capability_registry_declares_structured_data_as_kernel_contract() {
        let registry = CowdCapabilityRegistry::core();
        let structured_data = registry
            .capability("cowd.structured_data.core")
            .expect("structured data core capability should exist");

        assert_eq!(structured_data.layer, CowdCapabilityLayer::Kernel);
        assert_eq!(structured_data.kind, CowdCapabilityKind::StructuredData);
        assert_eq!(structured_data.owner_module, "runtime::structured_data");
        assert!(structured_data
            .description
            .contains("Source, mapping, fact"));
    }

    #[test]
    fn capability_registry_does_not_register_legacy_application_capability() {
        let registry = CowdCapabilityRegistry::core();

        let legacy_capability = ["ia", "cc.manufacturing.application"].concat();
        assert!(registry.capability(&legacy_capability).is_none());
    }

    #[test]
    fn capability_registry_declares_matrix_as_reality_core_engine_without_mfg_ownership() {
        let registry = CowdCapabilityRegistry::core();

        let matrix = registry
            .capability("cowd.matrix.engine")
            .expect("matrix engine capability should exist");
        assert_eq!(matrix.layer, CowdCapabilityLayer::Kernel);
        assert_eq!(matrix.kind, CowdCapabilityKind::StructuredData);
        assert_eq!(matrix.owner_module, "matrix");
        assert!(matrix.description.contains("Structured facts"));
        assert!(registry
            .capability("mfg.manufacturing.application")
            .is_none());
        assert!(registry.capabilities.iter().all(|capability| {
            !capability.id.starts_with("mfg.") && capability.owner_module != "app-mfg"
        }));
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
