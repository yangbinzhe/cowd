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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdReleaseGateRuntimeEvidence {
    pub structured_indexes_ready: bool,
    pub structured_watermark_persistent: bool,
    pub execution_outcome_timeline_available: bool,
    pub memory_context_bridge_available: bool,
    pub graph_skill_quality_contracts_available: bool,
}

impl CowdReleaseGateRuntimeEvidence {
    #[must_use]
    pub fn static_contracts() -> Self {
        Self {
            structured_indexes_ready: true,
            structured_watermark_persistent: true,
            execution_outcome_timeline_available: true,
            memory_context_bridge_available: true,
            graph_skill_quality_contracts_available: true,
        }
    }
}

impl CowdReleaseGateReport {
    #[must_use]
    pub fn evaluate() -> Self {
        Self::evaluate_static()
    }

    #[must_use]
    pub fn evaluate_static() -> Self {
        Self::evaluate_with(CowdReleaseGateRuntimeEvidence::static_contracts())
    }

    #[must_use]
    pub fn evaluate_with(evidence: CowdReleaseGateRuntimeEvidence) -> Self {
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
                "structured_data.indexes.ready",
                evidence.structured_indexes_ready,
                "Structured data source/fact/evidence indexes are readable.",
            ),
            check(
                "structured_data.watermark.persistent",
                evidence.structured_watermark_persistent,
                "Structured ingest watermark persistence is readable.",
            ),
            check(
                "execution_outcome.timeline.available",
                evidence.execution_outcome_timeline_available,
                "Execution outcome timeline projection is available.",
            ),
            check(
                "structured_data.memory_context.bridge",
                evidence.memory_context_bridge_available,
                "Structured data provides summary/context bridge without raw payload copy.",
            ),
            check(
                "graph_skill_quality.contracts",
                evidence.graph_skill_quality_contracts_available,
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

    #[test]
    fn release_gate_fails_when_runtime_evidence_is_missing() {
        let report = CowdReleaseGateReport::evaluate_with(CowdReleaseGateRuntimeEvidence {
            structured_indexes_ready: false,
            structured_watermark_persistent: false,
            execution_outcome_timeline_available: false,
            memory_context_bridge_available: false,
            graph_skill_quality_contracts_available: false,
        });

        assert_eq!(report.status, "fail");
        assert!(report.checks.iter().any(|check| check.check_id
            == "structured_data.indexes.ready"
            && check.status == "fail"));
        assert!(report.checks.iter().any(|check| check.check_id
            == "execution_outcome.timeline.available"
            && check.status == "fail"));
    }
}
