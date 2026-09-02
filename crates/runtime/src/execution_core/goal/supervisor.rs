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
        if !obligations.is_empty() && goal.obligations != obligations {
            let expected = goal.revision;
            let next_sequence = goal.user_sequence.saturating_add(1);
            let (updated, _) = self.goals.revise(
                goal_id,
                expected,
                next_sequence,
                "objective_supervisor_obligation_projection",
                |current| {
                    current.obligations = obligations.clone();
                    current.program_ref = current
                        .program_ref
                        .clone()
                        .or_else(|| Some(goal_id.to_string()));
                    vec!["obligations".to_string(), "program_ref".to_string()]
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
}
