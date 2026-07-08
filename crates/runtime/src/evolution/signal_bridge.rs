use std::path::Path;

use super::{
    EvolutionSignal, EvolutionSignalInput, EvolutionSignalSeverity, EvolutionSignalSource,
    EvolutionSignalStore, EvolutionSignalType,
};
use crate::self_regulation::{RuntimeAdaptiveDecision, RuntimeAdaptiveDecisionKind};

#[derive(Debug, Clone)]
pub struct EvolutionSignalCollector {
    store: EvolutionSignalStore,
}

impl EvolutionSignalCollector {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            store: EvolutionSignalStore::new(root),
        }
    }

    #[must_use]
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        Self::new(config_home.as_ref().join("evolution"))
    }

    pub fn append_self_regulation_signal(
        &self,
        session_id: impl Into<String>,
        decision: &RuntimeAdaptiveDecision,
        evidence_refs: Vec<String>,
    ) -> Result<Option<EvolutionSignal>, String> {
        let Some(signal) =
            signal_from_self_regulation_decision(session_id.into(), decision, evidence_refs)
        else {
            return Ok(None);
        };
        self.store.append(&signal)?;
        Ok(Some(signal))
    }

    #[must_use]
    pub fn store(&self) -> &EvolutionSignalStore {
        &self.store
    }
}

#[must_use]
pub fn signal_from_self_regulation_decision(
    session_id: String,
    decision: &RuntimeAdaptiveDecision,
    evidence_refs: Vec<String>,
) -> Option<EvolutionSignal> {
    if matches!(decision.kind, RuntimeAdaptiveDecisionKind::Continue) {
        return None;
    }
    let reason_code = decision.reason_code().unwrap_or(decision.kind_str());
    let (signal_type, severity, immediate_task_can_continue) = match reason_code {
        "low_novelty_tool_loop" | "repeated_evidence_target" => (
            EvolutionSignalType::LowNoveltyToolLoop,
            EvolutionSignalSeverity::Warning,
            true,
        ),
        "repeated_tool_failure" => (
            EvolutionSignalType::SlowProgress,
            EvolutionSignalSeverity::Warning,
            true,
        ),
        "replan_budget_exhausted" => (
            EvolutionSignalType::SlowProgress,
            EvolutionSignalSeverity::Critical,
            false,
        ),
        "context_pressure_soft" => (
            EvolutionSignalType::ContextPressure,
            EvolutionSignalSeverity::Warning,
            true,
        ),
        "context_pressure_critical" => (
            EvolutionSignalType::ContextPressure,
            EvolutionSignalSeverity::Critical,
            true,
        ),
        _ if matches!(
            decision.kind,
            RuntimeAdaptiveDecisionKind::EmitEvolutionSignal
        ) =>
        {
            (
                EvolutionSignalType::SlowProgress,
                EvolutionSignalSeverity::Warning,
                true,
            )
        }
        _ => return None,
    };

    Some(EvolutionSignal::new(EvolutionSignalInput {
        signal_type,
        source: EvolutionSignalSource {
            owner: "runtime.self_regulation".to_string(),
            session_id: Some(session_id),
            agent_id: None,
            team_id: None,
            run_id: None,
        },
        evidence_refs,
        severity,
        summary: decision
            .reason()
            .unwrap_or("runtime self-regulation detected an execution gap")
            .to_string(),
        suggested_action: suggested_action(reason_code, decision),
        immediate_task_can_continue,
    }))
}

fn suggested_action(reason_code: &str, decision: &RuntimeAdaptiveDecision) -> String {
    match reason_code {
        "low_novelty_tool_loop" => 
            "Generate a better tool batching, read fanout, or reflexion strategy for this task class"
                .to_string(),
        "repeated_evidence_target" => 
            "Prefer runtime_orchestrate(request_parallel_tools) or team delegation before repeating the same target"
                .to_string(),
        "repeated_tool_failure" => 
            "Create a recovery or tool contract hardening proposal for repeated failures"
                .to_string(),
        "replan_budget_exhausted" => 
            "Create a sandboxed candidate that improves loop escape and final answer synthesis"
                .to_string(),
        "context_pressure_soft" | "context_pressure_critical" => 
            "Tune runtime memory compression, evidence receipts, and scoped recall for this context profile"
                .to_string(),
        _ => decision
            .recommended_action()
            .unwrap_or("Create a governed evolution proposal from this runtime signal")
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_regulation::RuntimeAdaptiveDecisionKind;

    #[test]
    fn low_novelty_decision_becomes_evolution_signal() {
        let decision = RuntimeAdaptiveDecision::with_action(
            RuntimeAdaptiveDecisionKind::NudgeModel,
            "low novelty",
            "runtime_orchestrate(request_reflexion_retry)",
            "low_novelty_tool_loop",
            None,
            None,
        );

        let signal = signal_from_self_regulation_decision(
            "session-1".to_string(),
            &decision,
            vec!["tool:read_file:README.md".to_string()],
        )
        .expect("signal");

        assert_eq!(signal.signal_type, EvolutionSignalType::LowNoveltyToolLoop);
        assert_eq!(signal.source.owner, "runtime.self_regulation");
        assert!(signal.immediate_task_can_continue);
    }
}
