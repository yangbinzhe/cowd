//! Typed, deterministic intervention proposals derived from durable Goal observations.
//!
//! The policy never parses human summaries, executes a model request, mutates
//! a graph, or finalizes a turn. Runner applies a proposal in the canonical
//! graph transaction.

use harness_contract::goal::{
    AcceptanceStatus, GoalCompletion, GoalContract, GoalProgressSnapshot, ObservationFailureClass,
    RuntimeIntervention, RuntimeInterventionKind, RuntimeObservation, RuntimeObservationKind,
};

const MAX_POLICY_OBSERVATIONS: usize = 64;

#[derive(Debug, Clone, Default)]
pub struct InterventionPolicy;

impl InterventionPolicy {
    #[must_use]
    pub fn propose(
        &self,
        goal: &GoalContract,
        progress: &GoalProgressSnapshot,
        observations: &[RuntimeObservation],
    ) -> Result<RuntimeIntervention, String> {
        if goal.completion != GoalCompletion::Open {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Block,
                "goal is already terminal",
            );
        }
        if completion_evidence_closed(goal, progress) {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Synthesize,
                "all required criteria and evidence are closed",
            );
        }
        if progress
            .effects
            .values()
            .any(|effect| *effect == harness_contract::goal::EffectTerminalClass::Uncertain)
        {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Block,
                "an irreversible effect has no terminal receipt; preserve uncertainty and require explicit recovery authorization",
            );
        }
        if progress
            .criteria
            .values()
            .any(|status| *status == AcceptanceStatus::Blocked)
        {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Block,
                "a required acceptance criterion is unreachable under the current constraints",
            );
        }
        if !progress.open_conflicts.is_empty() {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Replan,
                "typed evidence exposed an unresolved conflict; resolve it before completion",
            );
        }

        let recent = current_observation_horizon(observations);
        if recent.iter().any(|observation| {
            observation.kind == RuntimeObservationKind::ContextPressure
                && observation.context_delta.pressure_basis_points >= 8_500
        }) {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Retrieve,
                "context pressure is high; retain durable receipts and retrieve a focused working set",
            );
        }

        let failed_tools = recent
            .iter()
            .copied()
            .filter(|observation| {
                observation.failed()
                    && observation.failure_class == Some(ObservationFailureClass::Tool)
            })
            .collect::<Vec<_>>();
        if let Some(fingerprint) = repeated_fingerprint(&failed_tools) {
            let has_verified_evidence = !progress.evidence_refs.is_empty();
            return proposal(
                goal,
                progress,
                observations,
                if has_verified_evidence {
                    RuntimeInterventionKind::Synthesize
                } else {
                    RuntimeInterventionKind::Block
                },
                if has_verified_evidence {
                    format!(
                        "the same governed tool action failed repeatedly ({fingerprint}); synthesize from retained verified evidence and expose the unresolved gap"
                    )
                } else {
                    format!(
                        "the same governed tool action failed repeatedly ({fingerprint}) before verified evidence; stop speculative retries"
                    )
                },
            );
        }
        if failed_tools.first().is_some() {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Replan,
                "a governed tool effect failed; replan from its typed failure and retained receipts",
            );
        }

        let repeated_success = recent
            .iter()
            .copied()
            .filter(|observation| {
                observation.kind == RuntimeObservationKind::ToolProgress
                    && !observation.failed()
                    && !observation.has_verified_gain()
            })
            .collect::<Vec<_>>();
        if let Some(fingerprint) = repeated_fingerprint(&repeated_success) {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Replan,
                format!(
                    "the same governed action produced no new distinguishing evidence ({fingerprint}); reuse its receipt and target a named open unknown"
                ),
            );
        }

        let no_gain_streak = recent
            .iter()
            .take_while(|observation| {
                observation.kind == RuntimeObservationKind::ToolProgress
                    && !observation.failed()
                    && !observation.has_verified_gain()
            })
            .count();
        if no_gain_streak >= 2 {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Synthesize,
                "consecutive tool batches produced no new distinguishing evidence; converge with open unknowns explicit",
            );
        }
        if no_gain_streak == 1 {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Replan,
                "the latest tool batch produced no new distinguishing evidence; target one named open unknown",
            );
        }

        if recent.first().is_some_and(|observation| {
            observation.kind == RuntimeObservationKind::StrategyHistory
                && observation.parallelism_delta.ready_work >= 2
        }) {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Parallelize,
                "independent ready work is available for the governed parallel schedule",
            );
        }

        let failed_provider_steps = recent
            .iter()
            .copied()
            .filter(|observation| {
                observation.failed()
                    && observation.failure_class == Some(ObservationFailureClass::Provider)
            })
            .count();
        if failed_provider_steps >= 3 {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Block,
                "provider execution failed repeatedly after governed recovery; preserve committed evidence",
            );
        }
        if failed_provider_steps == 2 {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Switch,
                "provider execution failed twice; switch the next model attempt",
            );
        }
        if failed_provider_steps == 1 {
            return proposal(
                goal,
                progress,
                observations,
                RuntimeInterventionKind::Replan,
                "provider execution failed; replan the next attempt from retained Goal state",
            );
        }
        proposal(
            goal,
            progress,
            observations,
            RuntimeInterventionKind::Continue,
            "typed evidence still permits progress",
        )
    }
}

fn completion_evidence_closed(goal: &GoalContract, progress: &GoalProgressSnapshot) -> bool {
    progress.open_conflicts.is_empty()
        && progress.open_unknowns.is_empty()
        && progress
            .effects
            .values()
            .all(|effect| *effect != harness_contract::goal::EffectTerminalClass::Uncertain)
        && goal.criteria.iter().all(|criterion| {
            matches!(
                progress.criteria.get(&criterion.id),
                Some(AcceptanceStatus::Satisfied | AcceptanceStatus::Waived)
            ) && criterion
                .required_evidence
                .iter()
                .all(|required| progress.evidence_refs.contains(required))
        })
}

fn current_observation_horizon(observations: &[RuntimeObservation]) -> Vec<&RuntimeObservation> {
    let Some(latest) = observations
        .iter()
        .max_by_key(|observation| observation.freshness.observed_at_ms)
    else {
        return Vec::new();
    };
    let now_ms = latest.freshness.observed_at_ms;
    let policy_revision = latest.freshness.policy_revision.as_str();
    observations
        .iter()
        .rev()
        .filter(|observation| observation.freshness.is_current_at(now_ms, policy_revision))
        .take(MAX_POLICY_OBSERVATIONS)
        .collect()
}

fn repeated_fingerprint(observations: &[&RuntimeObservation]) -> Option<String> {
    observations.iter().find_map(|observation| {
        (observations
            .iter()
            .filter(|candidate| candidate.fingerprint == observation.fingerprint)
            .count()
            >= 2)
            .then(|| observation.fingerprint.clone())
    })
}

fn proposal(
    goal: &GoalContract,
    progress: &GoalProgressSnapshot,
    observations: &[RuntimeObservation],
    kind: RuntimeInterventionKind,
    reason: impl Into<String>,
) -> Result<RuntimeIntervention, String> {
    let current = current_observation_horizon(observations);
    let trigger = current
        .first()
        .ok_or_else(|| "intervention policy requires a current typed observation".to_string())?;
    if trigger.goal_id() != goal.id {
        return Err(format!(
            "intervention observation goal {} does not match {}",
            trigger.goal_id(),
            goal.id
        ));
    }
    Ok(RuntimeIntervention {
        goal_id: goal.id.clone(),
        kind,
        reason: reason.into(),
        evidence_refs: progress.evidence_refs.clone(),
        expected_graph_revision: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::core::MeasureProvenance;
    use harness_contract::goal::{
        AcceptanceCriterion, ConflictDelta, ContextDelta, CostDelta, EffectDelta,
        EffectTerminalClass, EvidenceDelta, InformationGain, ObservationFreshness,
        ObservationResultClass, ParallelismDelta, ResolutionDeltaKind, RuntimeObservationIdentity,
    };

    fn goal() -> GoalContract {
        GoalContract {
            id: "goal".to_string(),
            session_id: "session".to_string(),
            objective: "produce a verified result".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "terminal".to_string(),
                statement: "one terminal result".to_string(),
                required_evidence: vec!["evidence:terminal".to_string()],
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

    fn observation(
        revision: u64,
        kind: RuntimeObservationKind,
        fingerprint: &str,
    ) -> RuntimeObservation {
        RuntimeObservation {
            identity: RuntimeObservationIdentity {
                workspace_id: "workspace".to_string(),
                session_id: "session".to_string(),
                turn_id: Some("turn".to_string()),
                task_id: None,
                graph_id: "graph".to_string(),
                goal_id: "goal".to_string(),
                node_id: Some(format!("node-{revision}")),
            },
            kind,
            source: "test".to_string(),
            source_revision: revision,
            freshness: ObservationFreshness {
                observed_at_ms: revision,
                valid_until_ms: None,
                policy_revision: "goal-observation-v2".to_string(),
            },
            summary: "human text is not a control signal".to_string(),
            fingerprint: fingerprint.to_string(),
            evidence_refs: Vec::new(),
            criterion_deltas: Vec::new(),
            evidence_delta: EvidenceDelta::default(),
            effect_deltas: Vec::new(),
            conflict_deltas: Vec::new(),
            unknown_deltas: Vec::new(),
            cost_delta: CostDelta::default(),
            information_gain: InformationGain::default(),
            context_delta: ContextDelta::default(),
            parallelism_delta: ParallelismDelta::default(),
            result_class: ObservationResultClass::Informational,
            failure_class: None,
        }
    }

    fn progress() -> GoalProgressSnapshot {
        crate::execution_core::GoalProgressReducer::from_goal(&goal())
    }

    #[test]
    fn repeated_typed_tool_failure_blocks_without_evidence() {
        let observations = (1..=2)
            .map(|revision| {
                let mut observation =
                    observation(revision, RuntimeObservationKind::ToolProgress, "tool:x");
                observation.result_class = ObservationResultClass::Failed;
                observation.failure_class = Some(ObservationFailureClass::Tool);
                observation
            })
            .collect::<Vec<_>>();
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress(), &observations)
                .unwrap()
                .kind,
            RuntimeInterventionKind::Block
        );
    }

    #[test]
    fn repeated_failure_synthesizes_when_independent_verified_gain_exists() {
        let mut gain = observation(1, RuntimeObservationKind::ToolProgress, "tool:good");
        gain.result_class = ObservationResultClass::Succeeded;
        gain.information_gain = InformationGain {
            distinguishing_evidence_refs: vec!["evidence:good".to_string()],
            resolved_unknown_refs: Vec::new(),
            provenance: MeasureProvenance::Observed,
        };
        gain.evidence_delta.added = vec!["evidence:good".to_string()];
        let mut first = observation(2, RuntimeObservationKind::ToolProgress, "tool:bad");
        first.result_class = ObservationResultClass::Failed;
        first.failure_class = Some(ObservationFailureClass::Tool);
        let mut second = first.clone();
        second.source_revision = 3;
        second.freshness.observed_at_ms = 3;
        let mut progress = progress();
        crate::execution_core::GoalProgressReducer::apply(&mut progress, &gain).unwrap();
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress, &[gain, first, second])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Synthesize
        );
    }

    #[test]
    fn context_parallelism_and_provider_failures_use_typed_fields_only() {
        let mut context = observation(1, RuntimeObservationKind::ContextPressure, "context");
        context.context_delta.pressure_basis_points = 8_500;
        context.summary = "nothing important".to_string();
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress(), &[context])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Retrieve
        );

        let mut parallel = observation(2, RuntimeObservationKind::StrategyHistory, "strategy");
        parallel.parallelism_delta.ready_work = 2;
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress(), &[parallel])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Parallelize
        );

        let failed = (3..=5)
            .map(|revision| {
                let mut observation = observation(
                    revision,
                    RuntimeObservationKind::ProviderProgress,
                    "provider",
                );
                observation.result_class = ObservationResultClass::Failed;
                observation.failure_class = Some(ObservationFailureClass::Provider);
                observation
            })
            .collect::<Vec<_>>();
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress(), &failed)
                .unwrap()
                .kind,
            RuntimeInterventionKind::Block
        );
    }

    #[test]
    fn horizon_uses_policy_revision_and_freshness_not_objective_length() {
        let mut old = observation(1, RuntimeObservationKind::ToolProgress, "old");
        old.freshness.policy_revision = "old-policy".to_string();
        old.result_class = ObservationResultClass::Failed;
        old.failure_class = Some(ObservationFailureClass::Tool);
        let current = observation(2, RuntimeObservationKind::GraphProgress, "current");
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress(), &[old, current])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Continue
        );
    }

    #[test]
    fn closed_typed_progress_synthesizes() {
        let mut progress = progress();
        progress
            .criteria
            .insert("terminal".to_string(), AcceptanceStatus::Satisfied);
        progress.evidence_refs.push("evidence:terminal".to_string());
        let current = observation(1, RuntimeObservationKind::GraphProgress, "complete");
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress, &[current])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Synthesize
        );
    }

    #[test]
    fn open_criterion_with_new_evidence_continues_until_criterion_is_closed() {
        let mut progress = progress();
        progress.evidence_refs.push("evidence:terminal".to_string());
        assert_eq!(
            InterventionPolicy
                .propose(
                    &goal(),
                    &progress,
                    &[observation(
                        1,
                        RuntimeObservationKind::GraphProgress,
                        "new-evidence",
                    )],
                )
                .unwrap()
                .kind,
            RuntimeInterventionKind::Continue
        );
    }

    #[test]
    fn blocked_criterion_conflict_and_uncertain_effect_never_complete() {
        let current = observation(1, RuntimeObservationKind::GraphProgress, "control");

        let mut blocked = progress();
        blocked
            .criteria
            .insert("terminal".to_string(), AcceptanceStatus::Blocked);
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &blocked, std::slice::from_ref(&current))
                .unwrap()
                .kind,
            RuntimeInterventionKind::Block
        );

        let mut conflicted = progress();
        conflicted.open_conflicts.push("conflict:one".to_string());
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &conflicted, std::slice::from_ref(&current))
                .unwrap()
                .kind,
            RuntimeInterventionKind::Replan
        );

        let mut uncertain = progress();
        uncertain
            .effects
            .insert("write:one".to_string(), EffectTerminalClass::Uncertain);
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &uncertain, &[current])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Block
        );
    }

    #[test]
    fn reducer_conflict_delta_prevents_false_completion() {
        let mut progress = progress();
        progress
            .criteria
            .insert("terminal".to_string(), AcceptanceStatus::Satisfied);
        progress.evidence_refs.push("evidence:terminal".to_string());
        let mut conflict = observation(1, RuntimeObservationKind::GraphProgress, "conflict");
        conflict.conflict_deltas.push(ConflictDelta {
            conflict_id: "fact:conflict".to_string(),
            change: ResolutionDeltaKind::Opened,
            evidence_refs: vec!["evidence:terminal".to_string()],
        });
        conflict.effect_deltas.push(EffectDelta {
            effect_id: "effect:checked".to_string(),
            terminal_class: EffectTerminalClass::Completed,
            idempotency_ref: "effect:receipt".to_string(),
        });
        crate::execution_core::GoalProgressReducer::apply(&mut progress, &conflict).unwrap();
        assert_eq!(
            InterventionPolicy
                .propose(&goal(), &progress, &[conflict])
                .unwrap()
                .kind,
            RuntimeInterventionKind::Replan
        );
    }
}
