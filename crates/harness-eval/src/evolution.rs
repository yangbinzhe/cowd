use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionClosureReport {
    pub kind: String,
    pub signal_count: usize,
    pub proposal_count: usize,
    pub candidate_count: usize,
    pub sandbox_eval_count: usize,
    pub skill_draft_count: usize,
    pub human_approval_boundary: bool,
    pub adoption_gate_count: usize,
    pub mainline_modified: bool,
    pub status: String,
    pub evidence_refs: Vec<String>,
}

#[must_use]
pub fn evaluate_evolution_closure() -> EvolutionClosureReport {
    let signals = vec![
        runtime::EvolutionSignal::low_novelty_tool_loop(
            "runtime",
            "session-eval",
            vec!["tool:loop".to_string()],
        ),
        runtime::EvolutionSignal::memory_noise(
            "runtime",
            "session-eval",
            vec!["memory:noise".to_string()],
        ),
        runtime::EvolutionSignal::eval_failure(
            "harness-eval-run",
            vec!["harness:report_gate".to_string()],
        ),
    ];
    let proposal = runtime::EvolutionProposal::from_signals(&signals);
    let candidate = runtime::EvolutionCandidate::from_proposal(
        &proposal,
        "baseline:current",
        "candidate:sandbox",
    )
    .with_artifact("evolution/sandbox-artifacts/candidate.json");
    let skill_draft = proposal.to_skill_draft();
    let sandbox_eval = runtime::EvolutionSandboxEval::compare(
        &proposal,
        "baseline:current",
        "candidate:sandbox",
        "evolution/sandbox-artifacts/report.json",
        60,
        80,
    );
    let human_approval_boundary = proposal.risk.approval_required
        && candidate.human_approval_required
        && sandbox_eval.human_approval_required;
    let mainline_modified = candidate.mainline_modified || sandbox_eval.mainline_modified;
    let evidence_refs = signals
        .iter()
        .flat_map(|signal| signal.evidence_refs.clone())
        .chain([
            candidate.candidate_id.clone(),
            sandbox_eval.artifact_path.clone(),
            skill_draft.skill_id.clone(),
        ])
        .collect::<Vec<_>>();
    let status = if signals.len() >= 3
        && !proposal.acceptance_gates.is_empty()
        && !candidate.adoption_gate.is_empty()
        && !skill_draft.markdown.is_empty()
        && human_approval_boundary
        && !mainline_modified
    {
        "passed"
    } else {
        "failed"
    }
    .to_string();
    EvolutionClosureReport {
        kind: "harness_eval.evolution_closure".to_string(),
        signal_count: signals.len(),
        proposal_count: 1,
        candidate_count: 1,
        sandbox_eval_count: 1,
        skill_draft_count: 1,
        human_approval_boundary,
        adoption_gate_count: candidate.adoption_gate.len(),
        mainline_modified,
        status,
        evidence_refs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolution_closure_requires_signal_proposal_sandbox_and_skill_draft() {
        let report = evaluate_evolution_closure();
        assert_eq!(report.status, "passed");
        assert_eq!(report.signal_count, 3);
        assert_eq!(report.proposal_count, 1);
        assert_eq!(report.candidate_count, 1);
        assert_eq!(report.sandbox_eval_count, 1);
        assert_eq!(report.skill_draft_count, 1);
        assert!(report.adoption_gate_count >= 4);
        assert!(report.human_approval_boundary);
        assert!(!report.mainline_modified);
        assert!(report
            .evidence_refs
            .iter()
            .any(|item| item.contains("sandbox-artifacts")));
    }
}
