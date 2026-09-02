//! Deterministic Objective liveness and terminal supervisor.
//!
//! This module deliberately contains no scheduler and never awaits a provider
//! or tool.  It consumes durable obligation/evidence facts, records a typed
//! revision when the plan has a recoverable gap, and commits exactly one
//! Objective terminal through `GoalStore`.

use std::sync::Arc;

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
                            && criterion
                                .required_evidence
                                .iter()
                                .all(|required| durable_evidence.contains(required))
                        {
                            criterion.status = harness_contract::goal::AcceptanceStatus::Satisfied;
                        }
                    }
                    current.program_ref = current
                        .program_ref
                        .clone()
                        .or_else(|| Some(goal_id.to_string()));
                    vec![
                        "obligations".to_string(),
                        "evidence_refs".to_string(),
                        "criteria".to_string(),
                        "program_ref".to_string(),
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

    /// Build the only production terminal event used by a user-facing turn.
    /// The event remains attached to the caller's graph transaction; the
    /// supervisor is the authority that permits the terminal transition.
    pub fn terminal_event(
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
            id: "objective-supervisor-test".to_string(),
            session_id: "session".to_string(),
            objective: "test objective".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "result".to_string(),
                statement: "result exists".to_string(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Satisfied,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            obligations: Vec::new(),
            program_ref: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        }
    }

    #[test]
    fn terminal_is_committed_only_after_required_obligation_is_satisfied() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::try_open_in_memory().expect("event store"),
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
                1,
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
            crate::RuntimeEventStore::try_open_in_memory().expect("event store"),
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
    fn program_projection_closes_the_root_execution_graph_criterion() {
        let store = Arc::new(GoalStore::new(Arc::new(
            crate::RuntimeEventStore::try_open_in_memory().expect("event store"),
        )));
        let mut contract = goal();
        contract.criteria = vec![AcceptanceCriterion {
            id: "terminal_synthesis".to_string(),
            statement: "produce one durable terminal synthesis".to_string(),
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
        assert!(projection
            .goal
            .evidence_refs
            .contains(&"execution_graph:graph-1".to_string()));
    }
}
