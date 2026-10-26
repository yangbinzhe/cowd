//! Deterministic Objective liveness and terminal supervisor.
//!
//! This module deliberately contains no scheduler and never awaits a provider
//! or tool.  It consumes durable obligation/evidence facts, records a typed
//! revision when the plan has a recoverable gap, and commits exactly one
//! Objective terminal through `GoalStore`.

use std::{collections::BTreeMap, sync::Arc};

use sha2::{Digest, Sha256};

use harness_contract::goal::{
    GoalCompletion, ObjectiveDiagnostic, ObjectiveObligation, ObjectiveObligationState,
    ObjectiveTerminal, ObjectiveTerminalKind,
};

use super::GoalStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectiveReconcileDecision {
    Waiting {
        unresolved_obligations: Vec<String>,
    },
    ReplanRequired {
        diagnostics: Vec<ObjectiveDiagnostic>,
    },
    Terminal(harness_contract::goal::GoalContract),
}

pub(crate) struct PreparedTurnConclusion {
    pub completion: GoalCompletion,
    pub event: crate::RuntimeTransactionEventInput,
    pub source_revisions: BTreeMap<String, u64>,
}

#[derive(Clone)]
pub struct ObjectiveSupervisor {
    goals: Arc<GoalStore>,
}

impl ObjectiveSupervisor {
    #[must_use]
    pub fn new(goals: Arc<GoalStore>) -> Self {
        Self { goals }
    }

    #[must_use]
    pub fn goal_store(&self) -> &Arc<GoalStore> {
        &self.goals
    }

    /// Reconcile one durable Objective.  The caller supplies facts already
    /// committed by the graph/evidence owners; this function never infers
    /// completion from activity counts or model text.
    pub fn reconcile(
        &self,
        goal_id: &str,
        authority_revision: u64,
        terminal_fence: &str,
        obligations: Vec<ObjectiveObligation>,
        evidence_refs: Vec<String>,
        diagnostics: Vec<ObjectiveDiagnostic>,
        allow_partial: bool,
        reason: impl Into<String>,
    ) -> Result<ObjectiveReconcileDecision, String> {
        let projection = self
            .goals
            .projection(goal_id)?
            .ok_or_else(|| format!("objective {goal_id} not found"))?;
        let obligations = if obligations.is_empty() {
            projection.goal.obligations.clone()
        } else {
            obligations
        };
        let unresolved = obligations
            .iter()
            .filter(|obligation| {
                obligation.required && obligation.state != ObjectiveObligationState::Satisfied
            })
            .map(|obligation| obligation.obligation_id.clone())
            .collect::<Vec<_>>();
        let has_blocked = obligations.iter().any(|obligation| {
            obligation.required && obligation.state == ObjectiveObligationState::Blocked
        });
        let has_failed = obligations.iter().any(|obligation| {
            obligation.required && obligation.state == ObjectiveObligationState::Failed
        });

        if !unresolved.is_empty() && !has_blocked && !has_failed {
            if !diagnostics.is_empty() || !allow_partial {
                return Ok(ObjectiveReconcileDecision::ReplanRequired { diagnostics });
            }
            return Ok(ObjectiveReconcileDecision::Waiting {
                unresolved_obligations: unresolved,
            });
        }

        let kind = if has_blocked {
            ObjectiveTerminalKind::Blocked
        } else if has_failed {
            ObjectiveTerminalKind::Failed
        } else if unresolved.is_empty() {
            ObjectiveTerminalKind::Satisfied
        } else {
            ObjectiveTerminalKind::PartiallySatisfied
        };
        let mut goal = projection.goal;
        let mut durable_evidence = projection.progress.evidence_refs.clone();
        durable_evidence.extend(evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        let criteria_need_update = goal.criteria.iter().any(|criterion| {
            criterion.status == harness_contract::goal::AcceptanceStatus::Open
                && !criterion.required_evidence.is_empty()
                && criterion
                    .required_evidence
                    .iter()
                    .all(|required| durable_evidence.contains(required))
        });
        if (!obligations.is_empty() && goal.obligations != obligations)
            || !durable_evidence.is_empty()
            || criteria_need_update
        {
            let expected = goal.revision;
            let next_sequence = goal.user_sequence.saturating_add(1);
            let (updated, _) = self.goals.revise(
                goal_id,
                expected,
                next_sequence,
                "objective_supervisor_obligation_projection",
                |current| {
                    current.obligations = obligations.clone();
                    current.evidence_refs = durable_evidence.clone();
                    for criterion in &mut current.criteria {
                        if criterion.status == harness_contract::goal::AcceptanceStatus::Open
                            && !criterion.required_evidence.is_empty()
                            && criterion
                                .required_evidence
                                .iter()
                                .all(|required| durable_evidence.contains(required))
                        {
                            criterion.status = harness_contract::goal::AcceptanceStatus::Satisfied;
                        }
                    }
                    vec![
                        "obligations".to_string(),
                        "evidence_refs".to_string(),
                        "criteria".to_string(),
                    ]
                },
            )?;
            goal = updated;
        }
        let terminal = ObjectiveTerminal {
            kind,
            terminal_fence: terminal_fence.to_string(),
            authority_revision,
            reason: reason.into(),
            evidence_refs,
            diagnostics,
            committed_at_ms: crate::tool_invocation::now_ms(),
        };
        let completed = self
            .goals
            .complete_objective(&goal.id, goal.revision, terminal)?;
        Ok(ObjectiveReconcileDecision::Terminal(completed))
    }

    /// Prepare an ordinary-turn conclusion from the original committed model
    /// attempt and Goal receipt contract. This function never appends. The
    /// caller attaches its event and read set to the original Graph commit.
    pub(crate) fn prepare_turn_conclusion(
        &self,
        goal_id: &str,
        ticket: &crate::execution_core::NodeExecutionTicket,
        resolver: &crate::path_identity::WorkspacePathIdentityResolver,
        reason: String,
        terminal_fence: String,
    ) -> Result<PreparedTurnConclusion, String> {
        use harness_contract::execution_graph::{ExecutionNodeKind, ExecutionNodeStatus};
        let source = self.goals.projection(goal_id)?.ok_or("turn Goal missing")?;
        let goal = &source.goal;
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or("turn Goal binding missing")?;
        if binding.root_execution_id != ticket.graph_id || binding.session_id != goal.session_id {
            return Err("turn Goal/Graph binding mismatch".into());
        }
        let graph = crate::execution_core::graph::ExecutionGraphStateStore::new(Arc::clone(
            &self.goals.event_store,
        ))
        .load(&ticket.graph_id)
        .map_err(|error| error.to_string())?;
        if graph.lineage.as_ref().is_none_or(|lineage| {
            lineage.session_id != binding.session_id || lineage.turn_id != binding.turn_id
        }) {
            return Err("turn Goal/Session lineage mismatch".into());
        }
        let latest = graph.nodes.iter().rev().find(|node| {
            node.kind == ExecutionNodeKind::InlineModel && graph.node_results.contains_key(&node.id)
        });
        let mut completion = GoalCompletion::Partial;
        if let Some(node) = latest {
            let result = &graph.node_results[&node.id];
            completion = match result.status {
                ExecutionNodeStatus::Cancelled => GoalCompletion::Cancelled,
                ExecutionNodeStatus::Failed => GoalCompletion::Failed,
                ExecutionNodeStatus::Blocked => GoalCompletion::Blocked,
                _ => GoalCompletion::Partial,
            };
            if result.status == ExecutionNodeStatus::Completed && result.failure.is_none() {
                if let Some(encoded) = result
                    .result_ref
                    .as_deref()
                    .and_then(|value| value.strip_prefix("assistant_json:"))
                {
                    let answer: String =
                        serde_json::from_str(encoded).map_err(|error| error.to_string())?;
                    let attempt = *graph
                        .recovery_cursor
                        .node_attempts
                        .get(&node.id)
                        .ok_or("turn candidate attempt missing")?;
                    let key = super::turn_candidate_key(&graph.id, &node.id, attempt);
                    if let Some(record) = self
                        .goals
                        .event_store
                        .event_by_idempotency_key(&super::stream_id(goal_id), &key)
                        .map_err(|error| error.to_string())?
                    {
                        let candidate: super::GoalTurnCandidate =
                            serde_json::from_value(record.payload["answer_candidate"].clone())
                                .map_err(|error| error.to_string())?;
                        let expected = super::GoalTurnCandidate {
                            goal_id: goal.id.clone(),
                            spec_revision: goal.spec_revision,
                            spec_digest: goal.spec_digest.clone(),
                            source_ref: goal
                                .source_intent_ref
                                .clone()
                                .ok_or("turn candidate source missing")?,
                            graph_id: graph.id.clone(),
                            node_id: node.id.clone(),
                            attempt,
                            answer_sha256: format!("{:x}", Sha256::digest(answer.as_bytes())),
                        };
                        if record.kind != "goal.observation"
                            || candidate != expected
                            || attempt == 0
                        {
                            return Err("turn candidate source/attempt mismatch".into());
                        }
                        // The original model-step owner may have rejected this
                        // candidate and co-committed a typed business Block with
                        // its Graph transition. Preserve that fact through the
                        // Goal reader instead of trusting Host's in-memory enum
                        // or classifying its human-readable reason.
                        let intervention_key =
                            format!("{}:goal-intervention", node.idempotency_key);
                        let blocked = if let Some(intervention_record) = self
                            .goals
                            .event_store
                            .event_by_idempotency_key(&super::stream_id(goal_id), &intervention_key)
                            .map_err(|error| error.to_string())?
                        {
                            let intervention: harness_contract::goal::RuntimeIntervention =
                                serde_json::from_value(
                                    intervention_record.payload["intervention"].clone(),
                                )
                                .map_err(|error| error.to_string())?;
                            let trace: harness_contract::goal::RuntimeInterventionTrace =
                                serde_json::from_value(
                                    intervention_record.payload["intervention_trace"].clone(),
                                )
                                .map_err(|error| error.to_string())?;
                            let observed: harness_contract::goal::RuntimeObservation =
                                serde_json::from_value(
                                    self.goals.event_store.resolve_observation_record(&record)?,
                                )
                                .map_err(|error| error.to_string())?;
                            let original = &intervention_record.payload["goal"];
                            if intervention_record.kind != "goal.intervention"
                                || intervention_record.actor.as_deref()
                                    != Some("runtime.intervention_policy")
                                || intervention_record.sequence > source.stream_revision
                                || record.sequence > source.stream_revision
                                || original["id"] != goal.id
                                || original["spec_revision"] != goal.spec_revision
                                || original["spec_digest"] != goal.spec_digest
                                || original["source_intent_ref"]
                                    != serde_json::to_value(&goal.source_intent_ref)
                                        .map_err(|error| error.to_string())?
                                || original["execution_binding"]
                                    != serde_json::to_value(binding)
                                        .map_err(|error| error.to_string())?
                                || intervention.goal_id != goal.id
                                || trace.identity != observed.identity
                                || observed.source != "runtime.model_step"
                                || trace.identity.goal_id != goal.id
                                || trace.identity.session_id != binding.session_id
                                || trace.identity.turn_id.as_deref()
                                    != Some(binding.turn_id.as_str())
                                || trace.identity.graph_id != graph.id
                                || trace.identity.node_id.as_deref() != Some(node.id.as_str())
                                || trace.trigger_observation_keys
                                    != vec![observed.idempotency_fingerprint()]
                            {
                                return Err("turn_block_intervention_binding_mismatch".into());
                            }
                            intervention.kind
                                == harness_contract::goal::RuntimeInterventionKind::Block
                        } else {
                            false
                        };
                        if !blocked
                            && !answer.trim().is_empty()
                            && self.goals.turn_writes_satisfied(&source, resolver)?
                        {
                            completion = GoalCompletion::Satisfied;
                        }
                    }
                }
            }
        }
        let mut event = self.goals.terminal_event(
            goal_id,
            completion,
            vec![format!("execution_graph:{}", graph.id)],
            reason,
            terminal_fence,
        )?;
        // The existing terminal writer may reload. The prepared source and
        // final payload must still denote the same specification; the Graph
        // transaction also fences the exact original journal head below.
        if event.event.payload["goal"]["spec_digest"] != goal.spec_digest
            || event.event.payload["goal"]["revision"] != goal.revision.saturating_add(1)
        {
            return Err("turn Goal changed while preparing terminal".into());
        }
        event.event.payload["goal_source"] = serde_json::json!({
            "stream_id": super::stream_id(goal_id), "sequence": source.stream_revision,
            "goal_revision": goal.revision, "spec_digest": goal.spec_digest,
            "source_intent_ref": goal.source_intent_ref,
        });
        Ok(PreparedTurnConclusion {
            completion,
            event,
            source_revisions: BTreeMap::from([
                (super::stream_id(goal_id), source.stream_revision),
                (format!("agentic-program:{}", binding.agentic_program_id), 0),
                (graph.id, graph.revision),
            ]),
        })
    }

    /// Legacy direct-event test seam. Production ordinary turns use the
    /// prepared source/attempt path; Programs own their checked transaction.
    #[cfg(test)]
    fn terminal_event(
        &self,
        goal_id: &str,
        completion: GoalCompletion,
        evidence_refs: Vec<String>,
        reason: String,
        terminal_fence: String,
    ) -> Result<crate::RuntimeTransactionEventInput, String> {
        let mut event = self.goals.terminal_event(
            goal_id,
            completion,
            evidence_refs,
            reason,
            terminal_fence.clone(),
        )?;
        if let Some(goal) = event.event.payload.get_mut("goal") {
            if let Some(objective_terminal) = goal.get_mut("terminal") {
                objective_terminal["terminal_fence"] = serde_json::Value::String(terminal_fence);
            }
        }
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::goal::{
        AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract,
    };

    fn goal() -> GoalContract {
        GoalContract {
            input_contract: None,
            id: "objective-supervisor-test".to_string(),
            session_id: "session".to_string(),
            objective: "test objective".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "result".to_string(),
                statement: "result exists".to_string(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Satisfied,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: harness_contract::goal::GoalScope::Internal,
            user_intent_criterion_id: Some("result".to_string()),
            source_intent_ref: Some("session_message:test".to_string()),
            execution_binding: None,
            spec_revision: 1,
            spec_digest: "test".to_string(),
            review_refs: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        }
    }

    #[test]
    fn terminal_is_committed_only_after_required_obligation_is_satisfied() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::for_test(),
        )));
        store.create(goal()).expect("create goal");
        let supervisor = ObjectiveSupervisor::new(store.clone());
        let obligations = vec![ObjectiveObligation {
            obligation_id: "deliver".to_string(),
            required: true,
            success_predicate: "artifact exists".to_string(),
            producer: Default::default(),
            evidence_requirement: Default::default(),
            state: ObjectiveObligationState::Satisfied,
            artifact_refs: vec!["artifact:1".to_string()],
            evidence_refs: vec!["evidence:1".to_string()],
            reread_receipts: vec!["reread:1".to_string()],
            verifier_decision: Some("verified".to_string()),
            diagnostic_code: None,
        }];
        let decision = supervisor
            .reconcile(
                "objective-supervisor-test",
                2,
                "fence:1",
                obligations,
                vec!["evidence:1".to_string()],
                Vec::new(),
                false,
                "verified",
            )
            .expect("reconcile");
        assert!(matches!(decision, ObjectiveReconcileDecision::Terminal(_)));
        let projection = store
            .projection("objective-supervisor-test")
            .expect("projection")
            .expect("goal");
        assert_eq!(projection.goal.completion, GoalCompletion::Satisfied);
        assert!(projection.goal.terminal.is_some());
    }

    #[test]
    fn direct_terminal_event_cannot_bypass_objective_obligation_verification() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::for_test(),
        )));
        let mut contract = goal();
        contract.obligations = vec![ObjectiveObligation {
            obligation_id: "required-team".to_string(),
            required: true,
            success_predicate: "verified Team evidence".to_string(),
            producer: Default::default(),
            evidence_requirement: Default::default(),
            state: ObjectiveObligationState::Open,
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            reread_receipts: Vec::new(),
            verifier_decision: None,
            diagnostic_code: None,
        }];
        store.create(contract).expect("create goal");
        let supervisor = ObjectiveSupervisor::new(store);
        let error = supervisor
            .terminal_event(
                "objective-supervisor-test",
                GoalCompletion::Satisfied,
                vec!["evidence:unverified".to_string()],
                "direct terminal attempt".to_string(),
                "fence:direct".to_string(),
            )
            .expect_err("unresolved obligations must reject direct terminal writes");
        assert!(error.contains("required obligations are unresolved"));
    }

    #[test]
    fn recovery_reservation_is_idempotent_and_budgeted() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::for_test(),
        )));
        store.create(goal()).expect("create goal");
        let first = store
            .reserve_recovery_for_obligation(
                "objective-supervisor-test",
                1,
                7,
                2,
                "recovery:1",
                "blocked",
                Some("team-a".to_string()),
            )
            .expect("reserve")
            .expect("first attempt");
        assert_eq!(first.recovery.as_ref().expect("state").attempts, 1);
        assert_eq!(
            first
                .recovery
                .as_ref()
                .expect("state")
                .source_obligation_id
                .as_deref(),
            Some("team-a")
        );
        let replay = store
            .reserve_recovery(
                "objective-supervisor-test",
                2,
                7,
                2,
                "recovery:1",
                "blocked",
            )
            .expect("replay")
            .expect("idempotent replay");
        assert_eq!(replay.revision, first.revision);
        assert!(
            store
                .reserve_recovery(
                    "objective-supervisor-test",
                    2,
                    7,
                    2,
                    "recovery:2",
                    "blocked"
                )
                .expect("second")
                .is_some()
        );
        let latest = store
            .get("objective-supervisor-test")
            .expect("get")
            .expect("goal");
        assert!(
            store
                .reserve_recovery(
                    "objective-supervisor-test",
                    latest.revision,
                    7,
                    2,
                    "recovery:3",
                    "blocked"
                )
                .expect("exhausted")
                .is_none()
        );
    }

    #[test]
    fn recovery_graph_state_is_durable_and_failure_is_terminal() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::for_test(),
        )));
        store.create(goal()).expect("create goal");
        let reserved = store
            .reserve_recovery(
                "objective-supervisor-test",
                1,
                7,
                2,
                "recovery:lifecycle",
                "blocked",
            )
            .expect("reserve")
            .expect("reservation");
        let applying = store
            .begin_recovery_graph(
                "objective-supervisor-test",
                reserved.revision,
                "recovery:lifecycle",
                "program-patch:1",
            )
            .expect("begin graph recovery");
        assert_eq!(
            applying.recovery.as_ref().expect("state").status,
            harness_contract::goal::ObjectiveRecoveryStatus::ApplyingGraph
        );
        let applied = store
            .mark_recovery_graph_applied(
                "objective-supervisor-test",
                applying.revision,
                "recovery:lifecycle",
                "program-patch:1",
            )
            .expect("mark applied");
        assert_eq!(
            applied.recovery.as_ref().expect("state").status,
            harness_contract::goal::ObjectiveRecoveryStatus::GraphApplied
        );
        let committed = store
            .commit_recovery(
                "objective-supervisor-test",
                applied.revision,
                "recovery:lifecycle",
            )
            .expect("commit");
        assert_eq!(
            committed.recovery.as_ref().expect("state").status,
            harness_contract::goal::ObjectiveRecoveryStatus::Committed
        );

        let reserved = store
            .reserve_recovery(
                "objective-supervisor-test",
                committed.revision,
                8,
                2,
                "recovery:failure",
                "second blocked",
            )
            .expect("reserve second")
            .expect("second reservation");
        let failed = store
            .fail_recovery(
                "objective-supervisor-test",
                reserved.revision,
                "recovery:failure",
                "capability_gap:missing verifier",
            )
            .expect("fail closed");
        assert_eq!(
            failed.recovery.as_ref().expect("state").status,
            harness_contract::goal::ObjectiveRecoveryStatus::Exhausted
        );
        assert!(
            store
                .reserve_recovery(
                    "objective-supervisor-test",
                    failed.revision,
                    8,
                    2,
                    "recovery:third",
                    "must not re-enter"
                )
                .expect("terminal reservation")
                .is_none()
        );
    }

    #[test]
    fn program_projection_closes_the_root_execution_graph_criterion() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::for_test(),
        )));
        let mut contract = goal();
        contract.criteria = vec![AcceptanceCriterion {
            id: "terminal_synthesis".to_string(),
            statement: "produce one durable terminal synthesis".to_string(),
            statement_ref: None,
            source_refs: Vec::new(),
            required_evidence: vec!["execution_graph:graph-1".to_string()],
            status: AcceptanceStatus::Open,
            waiver: None,
        }];
        store.create(contract).expect("create goal");
        let supervisor = ObjectiveSupervisor::new(store.clone());
        let obligations = vec![ObjectiveObligation {
            obligation_id: "team-1".to_string(),
            required: true,
            success_predicate: "verified Team evidence".to_string(),
            producer: Default::default(),
            evidence_requirement: Default::default(),
            state: ObjectiveObligationState::Satisfied,
            artifact_refs: Vec::new(),
            evidence_refs: vec!["team:evidence-1".to_string()],
            reread_receipts: vec!["reread:team:evidence-1".to_string()],
            verifier_decision: Some("team_terminal_verified".to_string()),
            diagnostic_code: None,
        }];
        let decision = supervisor
            .reconcile(
                "objective-supervisor-test",
                2,
                "objective:graph-1:program:3",
                obligations,
                vec!["execution_graph:graph-1".to_string()],
                Vec::new(),
                false,
                "program terminal projection",
            )
            .expect("reconcile");
        assert!(matches!(decision, ObjectiveReconcileDecision::Terminal(_)));
        let projection = store
            .projection("objective-supervisor-test")
            .expect("projection")
            .expect("goal");
        assert_eq!(
            projection.goal.criteria[0].status,
            AcceptanceStatus::Satisfied
        );
        assert!(
            projection
                .goal
                .evidence_refs
                .contains(&"execution_graph:graph-1".to_string())
        );
    }
}
