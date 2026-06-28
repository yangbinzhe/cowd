use harness_contract::knowledge::{KnowledgeComplianceWarning, KnowledgeGovernanceLevel};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeComplianceAction {
    Allow,
    Warn,
    Block,
}

#[derive(Debug, Clone)]
pub struct KnowledgeComplianceDecision {
    pub action: KnowledgeComplianceAction,
    pub warnings: Vec<KnowledgeComplianceWarning>,
    pub hard_gate_reasons: Vec<String>,
}

impl KnowledgeComplianceDecision {
    #[must_use]
    pub fn allows_execution(&self) -> bool {
        !matches!(self.action, KnowledgeComplianceAction::Block)
    }
}

#[derive(Debug, Clone, Default)]
pub struct KnowledgeComplianceRuntime;

impl KnowledgeComplianceRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn decide(&self, warnings: Vec<KnowledgeComplianceWarning>) -> KnowledgeComplianceDecision {
        let hard_gate_reasons = warnings
            .iter()
            .filter(|warning| warning.level == KnowledgeGovernanceLevel::Blocking)
            .map(|warning| warning.summary.clone())
            .collect::<Vec<_>>();
        let has_required = warnings
            .iter()
            .any(|warning| warning.level == KnowledgeGovernanceLevel::Required);
        let action = if !hard_gate_reasons.is_empty() {
            KnowledgeComplianceAction::Block
        } else if has_required || !warnings.is_empty() {
            KnowledgeComplianceAction::Warn
        } else {
            KnowledgeComplianceAction::Allow
        };
        KnowledgeComplianceDecision {
            action,
            warnings,
            hard_gate_reasons,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::core::KernelRef;

    #[test]
    fn blocking_warning_becomes_hard_gate() {
        let decision = KnowledgeComplianceRuntime::new().decide(vec![KnowledgeComplianceWarning {
            warning_id: "w1".to_string(),
            pack_id: "p1".to_string(),
            rule_id: Some("r1".to_string()),
            level: KnowledgeGovernanceLevel::Blocking,
            summary: "must stop on missing safety evidence".to_string(),
            evidence_refs: vec![KernelRef::new("test", "e1")],
        }]);

        assert_eq!(decision.action, KnowledgeComplianceAction::Block);
        assert!(!decision.allows_execution());
        assert_eq!(decision.hard_gate_reasons.len(), 1);
    }
}
