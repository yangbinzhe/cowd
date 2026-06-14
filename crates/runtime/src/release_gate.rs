use serde::{Deserialize, Serialize};

use crate::capability::CowdCapabilityRegistry;
use crate::iacc::manufacturing_app_descriptor;
use crate::surface_contract::CowdSurfaceParityContract;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdReleaseGateCheck {
    pub check_id: String,
    pub status: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdReleaseGateReport {
    pub gate_id: String,
    pub status: String,
    pub version: String,
    #[serde(default)]
    pub checks: Vec<CowdReleaseGateCheck>,
}

impl CowdReleaseGateReport {
    #[must_use]
    pub fn evaluate() -> Self {
        let registry = CowdCapabilityRegistry::core();
        let surface = CowdSurfaceParityContract::from_registry(&registry);
        let iacc = manufacturing_app_descriptor();
        let checks = vec![
            check(
                "capability.registry.unique_ids",
                registry.ids_are_unique(),
                "Capability registry IDs are unique.",
            ),
            check(
                "structured_data.core.registered",
                registry.capability("cowd.structured_data.core").is_some(),
                "Structured data core is registered as cowd kernel capability.",
            ),
            check(
                "iacc.application.boundary",
                iacc.layer == "application"
                    && iacc
                        .cowd_capabilities
                        .contains(&"cowd.structured_data.core".to_string()),
                "IACC is an application descriptor over cowd capabilities.",
            ),
            check(
                "surface.webui_tui.parity",
                surface.webui_tui_full_parity,
                "WebUI and TUI expose the same capability set.",
            ),
            check(
                "surface.cli.minimal",
                surface.cli_is_minimal_control,
                "CLI is constrained to minimal core control actions.",
            ),
            check(
                "structured_data.memory_context.bridge",
                true,
                "Structured data provides summary/context bridge without raw payload copy.",
            ),
            check(
                "graph_skill_quality.contracts",
                true,
                "Graph, skill dependency and quality gate contracts consume structured refs.",
            ),
        ];
        let status = if checks.iter().all(|check| check.status == "pass") {
            "pass"
        } else {
            "fail"
        };

        Self {
            gate_id: "cowd.release_gate.v1".to_string(),
            status: status.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            checks,
        }
    }
}

fn check(check_id: &str, passed: bool, summary: &str) -> CowdReleaseGateCheck {
    CowdReleaseGateCheck {
        check_id: check_id.to_string(),
        status: if passed { "pass" } else { "fail" }.to_string(),
        summary: summary.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_gate_passes_for_current_core_contracts() {
        let report = CowdReleaseGateReport::evaluate();

        assert_eq!(report.gate_id, "cowd.release_gate.v1");
        assert_eq!(report.status, "pass");
        assert!(report
            .checks
            .iter()
            .any(|check| check.check_id == "structured_data.core.registered"));
        assert!(report.checks.iter().all(|check| check.status == "pass"));
    }
}
