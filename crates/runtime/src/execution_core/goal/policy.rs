//! Local, deterministic intervention proposals derived from durable Goal observations.
//!
//! The policy does not execute a model request, mutate a graph, or finalize a
//! turn. It only produces a typed proposal that a Runner-owned node may attach
//! to its canonical graph transaction.

use harness_contract::goal::{
    GoalCompletion, GoalContract, RuntimeIntervention, RuntimeInterventionKind, RuntimeObservation,
    RuntimeObservationKind,
};

#[derive(Debug, Clone, Default)]
pub struct InterventionPolicy;

impl InterventionPolicy {
    #[must_use]
    pub fn propose(
        &self,
        goal: &GoalContract,
        observations: &[RuntimeObservation],
    ) -> RuntimeIntervention {
        if goal.completion != GoalCompletion::Open {
            return proposal(
                goal,
                RuntimeInterventionKind::Block,
                "goal is already terminal",
            );
        }
        if goal.criteria.iter().all(|criterion| {
            !matches!(
                criterion.status,
                harness_contract::goal::AcceptanceStatus::Open
            )
        }) {
            return proposal(
                goal,
                RuntimeInterventionKind::Synthesize,
                "all acceptance criteria are already resolved",
            );
        }

        let observation_horizon = observation_horizon(goal);
        let recent = observations
            .iter()
            .rev()
            .take(observation_horizon)
            .collect::<Vec<_>>();
        if recent.iter().any(|observation| {
            observation.kind == RuntimeObservationKind::ContextPressure
                && observation
                    .metrics
                    .get("pressure_basis_points")
                    .copied()
                    .unwrap_or_default()
                    >= 8_500
        }) {
            return proposal(
                goal,
                RuntimeInterventionKind::Retrieve,
                "context pressure is high; retain the goal/evidence receipts and retrieve the focused working set before adding more raw output",
            );
        }
        // Failure repetition is a durable fact, not a short-horizon hint.
        // Otherwise a model can alternate successful reads with the same
        // failing retrieval and evade the recovery policy indefinitely.
        let failed_tools = observations
            .iter()
            .filter(|observation| {
                observation.kind == RuntimeObservationKind::ToolProgress
                    && observation.progress_delta < 0
            })
            .collect::<Vec<_>>();
        if let Some(fingerprint) = failed_tools
            .iter()
            .filter_map(|observation| observation.fingerprint.as_deref())
            .find(|fingerprint| {
                failed_tools
                    .iter()
                    .filter(|observation| observation.fingerprint.as_deref() == Some(*fingerprint))
                    .count()
                    >= 2
            })
        {
            let has_verified_evidence = observations.iter().any(|observation| {
                observation.kind == RuntimeObservationKind::ToolProgress
                    && observation.progress_delta > 0
            });
            return proposal(
                goal,
                if has_verified_evidence {
                    RuntimeInterventionKind::Synthesize
                } else {
                    RuntimeInterventionKind::Block
                },
                if has_verified_evidence {
                    format!(
                        "the same governed tool action failed repeatedly ({fingerprint}); retain independently verified evidence and synthesize with the gap explicit instead of retrying"
                    )
                } else {
                    format!(
                        "the same governed tool action failed repeatedly ({fingerprint}) before any verified evidence; preserve the failure and stop speculative retries"
                    )
                },
            );
        }
        if let Some(observation) = failed_tools.first() {
            return proposal(
                goal,
                RuntimeInterventionKind::Replan,
                format!(
                    "a governed tool action failed; replan from its retained evidence before retrying: {}",
                    observation.summary
                ),
            );
        }
        let repeated_success = recent
            .iter()
            .filter(|observation| {
                observation.kind == RuntimeObservationKind::ToolProgress
                    && observation.progress_delta >= 0
            })
            .filter_map(|observation| observation.fingerprint.as_deref())
            .find(|fingerprint| {
                recent
                    .iter()
                    .filter(|observation| {
                        observation.kind == RuntimeObservationKind::ToolProgress
                            && observation.progress_delta >= 0
                            && observation.fingerprint.as_deref() == Some(*fingerprint)
                    })
                    .count()
                    >= 2
            });
        if let Some(fingerprint) = repeated_success {
            return proposal(
                goal,
                RuntimeInterventionKind::Replan,
                format!(
                    "the same successful governed action was requested repeatedly ({fingerprint}); reuse its retained receipt and synthesize or target a named unresolved fact instead of probing again"
                ),
            );
        }
        let low_novelty_streak = recent
            .iter()
            .take_while(|observation| {
                observation.kind == RuntimeObservationKind::ToolProgress
                    && (observation.progress_delta == 0 || observation.novelty < 30)
            })
            .count();
        if low_novelty_streak >= 2 {
            return proposal(
                goal,
                RuntimeInterventionKind::Synthesize,
                "multiple consecutive tool batches added no new evidence coverage; preserve the retained receipts and produce the bounded conclusion with unresolved gaps explicit",
            );
        }
        if low_novelty_streak == 1 {
            return proposal(
                goal,
                RuntimeInterventionKind::Replan,
                "the latest tool batch added no new evidence coverage; reuse checked evidence, identify one named remaining acceptance gap, and synthesize unless that gap requires a genuinely new scope",
            );
        }
        // Parallelism is a planning fact, not a post-execution instruction.
        // Only a fresh strategy checkpoint may request it. A later successful
        // ToolProgress must win so stale fan-out advice cannot keep steering
        // a synthesis role back into more exploration.
        if recent.first().is_some_and(|observation| {
            observation.kind == RuntimeObservationKind::StrategyHistory
                && observation
                    .metrics
                    .get("parallel_ready_work")
                    .copied()
                    .unwrap_or_default()
                    >= 2
        }) {
            return proposal(
                goal,
                RuntimeInterventionKind::Parallelize,
                "independent ready work is available; execute it through the governed parallel tool schedule",
            );
        }
        // Provider failures have a different recovery shape than tool
        // failures. A transport/model failure does not invalidate the graph
        // evidence already committed, so the first failure is retried through
        // a governed replan. A second failure switches the *next* model step
        // to an evidence-constrained recovery strategy. Repeated failures are
        // terminally blocked instead of creating an unbounded retry loop.
        // Count the full durable history for this goal rather than the short
        // relevance horizon: otherwise a horizon of two could never reach the
        // safety block on a third identical provider failure.
        let failed_steps = observations
            .iter()
            .filter(|observation| {
                observation.kind == RuntimeObservationKind::ProviderProgress
                    && observation.progress_delta < 0
            })
            .count();
        if failed_steps >= 3 {
            return proposal(
                goal,
                RuntimeInterventionKind::Block,
                "provider execution failed repeatedly after governed recovery; preserve committed evidence and wait for a new provider, constraint, or explicit replan",
            );
        }
        if failed_steps >= 2 {
            return proposal(
                goal,
                RuntimeInterventionKind::Switch,
                "provider execution failed twice; switch the subsequent model step to evidence-constrained recovery rather than repeating the prior path",
            );
        }
        if failed_steps == 1 {
            return proposal(
                goal,
                RuntimeInterventionKind::Replan,
                "provider execution failed; replan the next model step from retained goal and evidence state before retrying",
            );
        }
        proposal(
            goal,
            RuntimeInterventionKind::Continue,
            "recent evidence still advances goal",
        )
    }
}

fn observation_horizon(goal: &GoalContract) -> usize {
    let complexity = goal.objective.chars().count()
        + goal
            .constraints
            .iter()
            .map(|value| value.chars().count())
            .sum::<usize>()
        + goal.criteria.len() * 160;
    (2 + complexity / 900).clamp(2, 8)
}

fn proposal(
    goal: &GoalContract,
    kind: RuntimeInterventionKind,
    reason: impl Into<String>,
) -> RuntimeIntervention {
    RuntimeIntervention {
        goal_id: goal.id.clone(),
        kind,
        reason: reason.into(),
        evidence_refs: goal.evidence_refs.clone(),
        expected_graph_revision: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::goal::{AcceptanceCriterion, AcceptanceStatus};

    fn goal() -> GoalContract {
        GoalContract {
            id: "goal".to_string(),
            session_id: "session".to_string(),
            objective: "review the implementation and produce a verified result".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "terminal".to_string(),
                statement: "one terminal result".to_string(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        }
    }

    #[test]
    fn repeated_low_novelty_tool_observations_propose_synthesis() {
        let goal = goal();
        let observations = (0..2)
            .map(|index| RuntimeObservation {
                goal_id: goal.id.clone(),
                kind: RuntimeObservationKind::ToolProgress,
                source: "tool".to_string(),
                summary: format!("repeat-{index}"),
                fingerprint: None,
                evidence_refs: Vec::new(),
                metrics: Default::default(),
                progress_delta: 0,
                novelty: 0,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            InterventionPolicy.propose(&goal, &observations).kind,
            RuntimeInterventionKind::Synthesize
        );
    }

    #[test]
    fn repeated_successful_action_replans_instead_of_repeating_it() {
        let goal = goal();
        let action = |summary: &str| RuntimeObservation {
            goal_id: goal.id.clone(),
            kind: RuntimeObservationKind::ToolProgress,
            source: "tool".to_string(),
            summary: summary.to_string(),
            fingerprint: Some("tool_action:read_cargo".to_string()),
            evidence_refs: vec!["tool_call:read".to_string()],
            metrics: Default::default(),
            progress_delta: 1,
            novelty: 0,
        };

        assert_eq!(
            InterventionPolicy
                .propose(&goal, &[action("first"), action("repeat")])
                .kind,
            RuntimeInterventionKind::Replan
        );
    }

    #[test]
    fn failed_tool_replans_once_then_blocks_repeated_identical_action() {
        let goal = goal();
        let failure = |summary: &str| RuntimeObservation {
            goal_id: goal.id.clone(),
            kind: RuntimeObservationKind::ToolProgress,
            source: "tool".to_string(),
            summary: summary.to_string(),
            fingerprint: Some("tool_failure:runtime_orchestrate".to_string()),
            evidence_refs: vec!["tool_call:orchestrate".to_string()],
            metrics: Default::default(),
            progress_delta: -1,
            novelty: 10,
        };

        assert_eq!(
            InterventionPolicy
                .propose(&goal, &[failure("first failure")])
                .kind,
            RuntimeInterventionKind::Replan
        );
        assert_eq!(
            InterventionPolicy
                .propose(
                    &goal,
                    &[failure("first failure"), failure("second failure")]
                )
                .kind,
            RuntimeInterventionKind::Block
        );
    }

    #[test]
    fn repeated_failure_with_checked_evidence_synthesizes_instead_of_looping() {
        let goal = goal();
        let failure = |index| RuntimeObservation {
            goal_id: goal.id.clone(),
            kind: RuntimeObservationKind::ToolProgress,
            source: "tool".to_string(),
            summary: format!("retrieve failure {index}"),
            fingerprint: Some("tool_failure:evidence_retrieve".to_string()),
            evidence_refs: Vec::new(),
            metrics: Default::default(),
            progress_delta: -1,
            novelty: 10,
        };
        let success = RuntimeObservation {
            goal_id: goal.id.clone(),
            kind: RuntimeObservationKind::ToolProgress,
            source: "tool".to_string(),
            summary: "read checked source".to_string(),
            fingerprint: Some("tool_action:read_file".to_string()),
            evidence_refs: vec!["tool_coverage:crates/runtime".to_string()],
            metrics: Default::default(),
            progress_delta: 1,
            novelty: 90,
        };
        let spacer = |index| RuntimeObservation {
            summary: format!("independent evidence {index}"),
            fingerprint: Some(format!("tool_action:read_{index}")),
            ..success.clone()
        };

        assert_eq!(
            InterventionPolicy
                .propose(
                    &goal,
                    &[
                        failure(1),
                        success.clone(),
                        spacer(1),
                        spacer(2),
                        failure(2)
                    ]
                )
                .kind,
            RuntimeInterventionKind::Synthesize
        );
    }

    #[test]
    fn independent_ready_work_proposes_parallel_execution() {
        let goal = goal();
        let mut metrics = std::collections::BTreeMap::new();
        metrics.insert("parallel_ready_work".to_string(), 3);
        let observation = RuntimeObservation {
            goal_id: goal.id.clone(),
            kind: RuntimeObservationKind::StrategyHistory,
            source: "runtime.model_step".to_string(),
            summary: "three independent read-only tool calls are ready".to_string(),
            fingerprint: Some("tool-schedule:parallel".to_string()),
            evidence_refs: Vec::new(),
            metrics,
            progress_delta: 0,
            novelty: 50,
        };

        assert_eq!(
            InterventionPolicy.propose(&goal, &[observation]).kind,
            RuntimeInterventionKind::Parallelize
        );
    }

    #[test]
    fn provider_failures_progress_from_replan_to_switch_to_block() {
        let goal = goal();
        let failure = |index| RuntimeObservation {
            goal_id: goal.id.clone(),
            kind: RuntimeObservationKind::ProviderProgress,
            source: "runtime.provider_stream".to_string(),
            summary: format!("provider attempt {index} failed"),
            fingerprint: Some("provider_failure".to_string()),
            evidence_refs: vec![format!("execution_node:{index}")],
            metrics: Default::default(),
            progress_delta: -1,
            novelty: 0,
        };

        assert_eq!(
            InterventionPolicy.propose(&goal, &[failure(1)]).kind,
            RuntimeInterventionKind::Replan
        );
        assert_eq!(
            InterventionPolicy
                .propose(&goal, &[failure(1), failure(2)])
                .kind,
            RuntimeInterventionKind::Switch
        );
        assert_eq!(
            InterventionPolicy
                .propose(&goal, &[failure(1), failure(2), failure(3)])
                .kind,
            RuntimeInterventionKind::Block
        );
    }
}
