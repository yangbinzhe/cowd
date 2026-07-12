use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionClosureReport {
    pub kind: String,
    pub signal_count: usize,
    pub diagnosis_count: usize,
    pub proposal_count: usize,
    pub candidate_count: usize,
    pub sandbox_eval_count: usize,
    pub skill_draft_count: usize,
    pub human_approval_boundary: bool,
    pub adoption_gate_count: usize,
    pub artifact_count: usize,
    pub adoption_receipt_count: usize,
    pub mainline_modified: bool,
    pub status: String,
    pub evidence_refs: Vec<String>,
}

fn failed_evolution_report(
    signal_count: usize,
    diagnosis_count: usize,
    proposal_count: usize,
    mut evidence_refs: Vec<String>,
    stage: &str,
    error: impl std::fmt::Display,
) -> EvolutionClosureReport {
    evidence_refs.push(format!("failure:{stage}:{error}"));
    EvolutionClosureReport {
        kind: "harness_eval.evolution_closure".to_string(),
        signal_count,
        diagnosis_count,
        proposal_count,
        candidate_count: 0,
        sandbox_eval_count: 0,
        skill_draft_count: 0,
        human_approval_boundary: false,
        adoption_gate_count: 0,
        artifact_count: 0,
        adoption_receipt_count: 0,
        mainline_modified: false,
        status: "failed".to_string(),
        evidence_refs,
    }
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
    let diagnosis = runtime::EvolutionDiagnosisEngine::diagnose(&signals);
    let mut evidence_refs = signals
        .iter()
        .flat_map(|signal| signal.evidence_refs.clone())
        .chain([diagnosis.diagnosis_id.clone()])
        .collect::<Vec<_>>();
    let mut drafts = runtime::EvolutionLifecycleService::open_from_signals(&signals);
    let Some(draft) = drafts.pop() else {
        return failed_evolution_report(
            signals.len(),
            1,
            0,
            evidence_refs,
            "open_evolution_lifecycle",
            "no lifecycle draft was generated from deterministic signals",
        );
    };
    let proposal = draft.proposal;
    let candidate = match runtime::EvolutionCandidateGenerator::generate_with_artifacts(
        std::env::temp_dir().join(format!(
            "cowd-evolution-eval-artifacts-{}",
            uuid::Uuid::new_v4()
        )),
        &proposal,
        "baseline:current",
        "candidate:sandbox",
    ) {
        Ok(candidate) => candidate,
        Err(error) => {
            return failed_evolution_report(
                signals.len(),
                1,
                1,
                evidence_refs,
                "generate_candidate_artifacts",
                error,
            );
        }
    };
    let skill_draft = proposal.to_skill_draft();
    let sandbox_root =
        std::env::temp_dir().join(format!("cowd-evolution-eval-{}", uuid::Uuid::new_v4()));
    let runner = runtime::IsolatedRunner::new(
        sandbox_root.join("runner"),
        runtime::EvolutionRunnerPolicy::default(),
    );
    let mut runner_results = match runner.run_artifact_check(&candidate) {
        Ok(result) => vec![result],
        Err(error) => {
            return failed_evolution_report(
                signals.len(),
                1,
                1,
                evidence_refs,
                "run_candidate_artifact_check",
                error,
            );
        }
    };
    for (mode, command) in [
        ("baseline", candidate.baseline_command.as_str()),
        ("candidate", candidate.candidate_command.as_str()),
        ("verification", candidate.verification_command.as_str()),
    ] {
        match runner.run_named_command(&candidate, mode, command) {
            Ok(result) => runner_results.push(result),
            Err(error) => {
                return failed_evolution_report(
                    signals.len(),
                    1,
                    1,
                    evidence_refs,
                    "run_candidate_command",
                    error,
                );
            }
        }
    }
    let eval_request =
        runtime::EvolutionEvaluationRequest::from_candidate(&candidate, &runner_results);
    let comparison = runtime::EvolutionComparisonReport::from_request_and_runner_results(
        &eval_request,
        sandbox_root.join("comparison.json").display().to_string(),
        &runner_results,
    );
    let mut candidate = candidate;
    candidate.comparison_report_ref = Some(comparison.comparison_id.clone());
    let sandbox_eval = match runtime::EvolutionSandboxOrchestrator::new(&sandbox_root)
        .run(&proposal, &candidate)
    {
        Ok(result) => result,
        Err(error) => {
            return failed_evolution_report(
                signals.len(),
                1,
                1,
                evidence_refs,
                "run_evolution_sandbox",
                error,
            );
        }
    };
    let promotion_receipt = runtime::EvolutionPromotionManager::promote(&candidate);
    let human_approval_boundary = proposal.risk.approval_required
        && candidate.human_approval_required
        && sandbox_eval.human_approval_required;
    let mainline_modified = candidate.mainline_modified || sandbox_eval.mainline_modified;
    evidence_refs.extend([
        candidate.candidate_id.clone(),
        sandbox_eval.artifact_path.clone(),
        skill_draft.skill_id.clone(),
        promotion_receipt.promotion_id.clone(),
        comparison.comparison_id.clone(),
    ]);
    evidence_refs.extend(sandbox_eval.artifact_paths.clone());
    let status = if signals.len() >= 3
        && !diagnosis.acceptance_gates.is_empty()
        && !proposal.acceptance_gates.is_empty()
        && !candidate.adoption_gate.is_empty()
        && !skill_draft.markdown.is_empty()
        && !sandbox_eval.artifact_paths.is_empty()
        && promotion_receipt.accepted
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
        diagnosis_count: 1,
        proposal_count: 1,
        candidate_count: 1,
        sandbox_eval_count: 1,
        skill_draft_count: 1,
        human_approval_boundary,
        adoption_gate_count: candidate.adoption_gate.len(),
        artifact_count: sandbox_eval.artifact_paths.len(),
        adoption_receipt_count: usize::from(promotion_receipt.accepted),
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
        assert_eq!(report.diagnosis_count, 1);
        assert_eq!(report.proposal_count, 1);
        assert_eq!(report.candidate_count, 1);
        assert_eq!(report.sandbox_eval_count, 1);
        assert_eq!(report.skill_draft_count, 1);
        assert!(report.artifact_count >= 4);
        assert_eq!(report.adoption_receipt_count, 1);
        assert!(report.adoption_gate_count >= 4);
        assert!(report.human_approval_boundary);
        assert!(!report.mainline_modified);
        assert!(report
            .evidence_refs
            .iter()
            .any(|item| item.contains("cowd-evolution-eval")));
    }
}
