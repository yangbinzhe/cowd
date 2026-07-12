//! Durable goal state and projections owned by Runtime.
//!
//! The store uses RuntimeEventStore goal streams directly. It deliberately has
//! no scheduler, model loop, or Gateway dependency; interventions remain
//! proposals until ExecutionGraphRunner applies them at a checkpoint.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub mod policy;

use harness_contract::goal::{
    AcceptanceStatus, GoalCompletion, GoalContract, GoalRevision, RuntimeIntervention,
    RuntimeObservation,
};

use crate::{
    runtime_event_store::RuntimeTransactionEventInput, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore,
};

#[derive(Clone)]
pub struct GoalStore {
    event_store: Arc<RuntimeEventStore>,
}

/// Read model reconstructed exclusively from the durable Goal stream. Runtime
/// keeps no second mutable goal map: callers can drop this projection at any
/// time and rebuild it from `RuntimeEventStore`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjection {
    pub goal: GoalContract,
    pub stream_revision: u64,
    pub observations: Vec<RuntimeObservation>,
    pub interventions: Vec<RuntimeIntervention>,
}

impl GoalStore {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self { event_store }
    }

    pub fn create(&self, goal: GoalContract) -> Result<GoalContract, String> {
        validate_goal(&goal)?;
        let stream_id = stream_id(&goal.id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        if revision > 0 {
            return self
                .get(&goal.id)?
                .ok_or_else(|| format!("goal stream {stream_id} has no replayable goal snapshot"));
        }
        self.append_goal_event(
            &stream_id,
            revision,
            format!("goal-create:{}", goal.id),
            "goal.created",
            "open",
            &goal,
            "runtime.goal_store",
            Vec::new(),
        )?;
        Ok(goal)
    }

    pub fn get(&self, goal_id: &str) -> Result<Option<GoalContract>, String> {
        let stream_id = stream_id(goal_id);
        let mut latest = None;
        for event in self
            .event_store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?
        {
            if event.scope != RuntimeEventScope::Goal {
                return Err(format!("goal stream {stream_id} contains a non-goal event"));
            }
            if let Some(value) = event.payload.get("goal") {
                latest = Some(
                    serde_json::from_value::<GoalContract>(value.clone())
                        .map_err(|error| error.to_string())?,
                );
            }
        }
        Ok(latest)
    }

    pub fn projection(&self, goal_id: &str) -> Result<Option<GoalProjection>, String> {
        let stream_id = stream_id(goal_id);
        let mut goal = None;
        let mut observations = Vec::new();
        let mut interventions = Vec::new();
        let mut stream_revision = 0_u64;
        for event in self
            .event_store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?
        {
            stream_revision = event.sequence;
            if event.scope != RuntimeEventScope::Goal {
                return Err(format!("goal stream {stream_id} contains a non-goal event"));
            }
            if let Some(value) = event.payload.get("goal") {
                goal = Some(
                    serde_json::from_value::<GoalContract>(value.clone())
                        .map_err(|error| error.to_string())?,
                );
            }
            if let Some(value) = event.payload.get("observation") {
                observations.push(
                    serde_json::from_value::<RuntimeObservation>(value.clone())
                        .map_err(|error| error.to_string())?,
                );
            }
            if let Some(value) = event.payload.get("intervention") {
                interventions.push(
                    serde_json::from_value::<RuntimeIntervention>(value.clone())
                        .map_err(|error| error.to_string())?,
                );
            }
        }
        Ok(goal.map(|goal| GoalProjection {
            goal,
            stream_revision,
            observations,
            interventions,
        }))
    }

    pub fn observations(&self, goal_id: &str) -> Result<Vec<RuntimeObservation>, String> {
        self.event_store
            .list_stream(&stream_id(goal_id))
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|event| event.kind == "goal.observation")
            .filter_map(|event| event.payload.get("observation").cloned())
            .map(serde_json::from_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())
    }

    pub fn revise(
        &self,
        goal_id: &str,
        expected_revision: u64,
        user_sequence: u64,
        reason: impl Into<String>,
        mutate: impl FnOnce(&mut GoalContract) -> Vec<String>,
    ) -> Result<(GoalContract, GoalRevision), String> {
        let (goal, revision, event) =
            self.revision_event(goal_id, expected_revision, user_sequence, reason, mutate)?;
        let stream_id = event.event.stream_id.clone();
        let stream_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                stream_revision,
                format!("goal-revise:{goal_id}:{}", revision.revision),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok((goal, revision))
    }

    /// Construct a Goal revision for the caller's canonical graph
    /// transaction. This lets a running turn apply a user correction together
    /// with the node transition that observed it instead of introducing a
    /// second mutable Goal write path.
    pub fn revision_event(
        &self,
        goal_id: &str,
        expected_revision: u64,
        user_sequence: u64,
        reason: impl Into<String>,
        mutate: impl FnOnce(&mut GoalContract) -> Vec<String>,
    ) -> Result<(GoalContract, GoalRevision, RuntimeTransactionEventInput), String> {
        let reason = reason.into();
        let mut goal = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if goal.revision != expected_revision {
            return Err(format!(
                "goal revision stale: expected {expected_revision}, actual {}",
                goal.revision
            ));
        }
        if user_sequence <= goal.user_sequence {
            return Err(format!(
                "goal user sequence is stale: incoming {user_sequence}, actual {}",
                goal.user_sequence
            ));
        }
        let previous = goal.revision;
        let changed_fields = mutate(&mut goal);
        goal.revision = goal.revision.saturating_add(1);
        goal.user_sequence = user_sequence;
        validate_goal(&goal)?;
        let revision = GoalRevision {
            goal_id: goal.id.clone(),
            previous_revision: previous,
            revision: goal.revision,
            reason,
            user_sequence,
            changed_fields,
        };
        let event = goal_event(
            &goal,
            "goal.revised",
            "open",
            "runtime.goal_store".to_string(),
            vec![RuntimeEventRef {
                kind: "goal_revision".to_string(),
                id: revision.revision.to_string(),
            }],
            serde_json::json!({ "goal": goal, "revision": revision }),
            format!("goal-revise:{goal_id}:{}", goal.revision),
        );
        Ok((goal, revision, event))
    }

    pub fn record_observation(&self, observation: RuntimeObservation) -> Result<(), String> {
        let event =
            self.observation_event(&observation, format!("direct:{}", uuid::Uuid::new_v4()))?;
        let stream_id = event.event.stream_id.clone();
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                revision,
                format!(
                    "goal-observation:{}:{}",
                    observation.goal_id,
                    uuid::Uuid::new_v4()
                ),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn record_intervention(&self, intervention: RuntimeIntervention) -> Result<(), String> {
        let event =
            self.intervention_event(&intervention, format!("direct:{}", uuid::Uuid::new_v4()))?;
        let stream_id = event.event.stream_id.clone();
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                revision,
                format!(
                    "goal-intervention:{}:{}",
                    intervention.goal_id,
                    uuid::Uuid::new_v4()
                ),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Build, but do not append, an observation event. Node executors attach
    /// this to their graph commit so evidence and graph progress are durable in
    /// one transaction.
    pub fn observation_event(
        &self,
        observation: &RuntimeObservation,
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        let goal = self
            .get(&observation.goal_id)?
            .ok_or_else(|| format!("goal {} not found", observation.goal_id))?;
        Ok(goal_event(
            &goal,
            "goal.observation",
            "observed",
            observation.source.clone(),
            observation
                .evidence_refs
                .iter()
                .map(|id| RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id: id.clone(),
                })
                .collect(),
            serde_json::json!({ "goal": goal, "observation": observation }),
            idempotency_key,
        ))
    }

    /// Build, but do not append, an intervention event. Policy remains a
    /// producer; the Runner commits and therefore owns application order.
    pub fn intervention_event(
        &self,
        intervention: &RuntimeIntervention,
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        let goal = self
            .get(&intervention.goal_id)?
            .ok_or_else(|| format!("goal {} not found", intervention.goal_id))?;
        Ok(goal_event(
            &goal,
            "goal.intervention",
            "proposed",
            "runtime.intervention_policy".to_string(),
            intervention
                .evidence_refs
                .iter()
                .map(|id| RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id: id.clone(),
                })
                .collect(),
            serde_json::json!({ "goal": goal, "intervention": intervention }),
            idempotency_key,
        ))
    }

    /// Build the terminal Goal event for a graph commit. The Runner commits it
    /// together with the graph transition and (when present) session terminal
    /// outbox record, so a graph can never be terminal while its goal remains
    /// open or vice versa.
    pub fn terminal_event(
        &self,
        goal_id: &str,
        completion: GoalCompletion,
        evidence_refs: Vec<String>,
        reason: String,
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        let mut goal = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if goal.completion != GoalCompletion::Open {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        let mut durable_evidence = goal.evidence_refs.clone();
        durable_evidence.extend(evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        if completion == GoalCompletion::Satisfied {
            for criterion in &mut goal.criteria {
                if criterion.status == AcceptanceStatus::Open
                    && criterion
                        .required_evidence
                        .iter()
                        .all(|evidence| durable_evidence.contains(evidence))
                {
                    criterion.status = AcceptanceStatus::Satisfied;
                }
            }
            if goal.criteria.iter().any(|criterion| {
                !matches!(
                    criterion.status,
                    AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                )
            }) {
                return Err(
                    "cannot satisfy a goal until every criterion has required evidence or a waiver"
                        .to_string(),
                );
            }
        }
        for criterion in &goal.criteria {
            if criterion.status == AcceptanceStatus::Waived {
                let Some(waiver) = &criterion.waiver else {
                    return Err(format!(
                        "criterion {} is waived without a durable waiver receipt",
                        criterion.id
                    ));
                };
                if waiver.actor.trim().is_empty()
                    || waiver.reason.trim().is_empty()
                    || waiver.permission_receipt.trim().is_empty()
                {
                    return Err(format!(
                        "criterion {} has an invalid waiver receipt",
                        criterion.id
                    ));
                }
            }
        }
        goal.evidence_refs = durable_evidence;
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        goal.revision = goal.revision.saturating_add(1);
        let status = match completion {
            GoalCompletion::Satisfied => "satisfied",
            GoalCompletion::Blocked => "blocked",
            GoalCompletion::Cancelled => "cancelled",
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        Ok(goal_event(
            &goal,
            "goal.completed",
            status,
            "runtime.synthesize".to_string(),
            evidence_refs
                .into_iter()
                .map(|id| RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id,
                })
                .collect(),
            serde_json::json!({ "goal": goal, "completion_reason": reason }),
            idempotency_key,
        ))
    }

    pub fn complete(
        &self,
        goal_id: &str,
        expected_revision: u64,
        completion: GoalCompletion,
        reason: impl Into<String>,
    ) -> Result<GoalContract, String> {
        let stream_id = stream_id(goal_id);
        let stream_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let mut goal = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if goal.revision != expected_revision {
            return Err(format!(
                "goal completion has stale revision {expected_revision}"
            ));
        }
        match completion {
            GoalCompletion::Satisfied => {
                if goal.criteria.iter().any(|criterion| {
                    !matches!(
                        criterion.status,
                        AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                    )
                }) {
                    return Err(
                        "cannot satisfy a goal until every acceptance criterion is satisfied or waived"
                            .to_string(),
                    );
                }
            }
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string())
            }
            GoalCompletion::Blocked | GoalCompletion::Cancelled => {}
        }
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string())
            }
        };
        goal.revision = goal.revision.saturating_add(1);
        self.append_goal_event(
            &stream_id,
            stream_revision,
            format!("goal-complete:{goal_id}:{}", goal.revision),
            "goal.completed",
            match completion {
                GoalCompletion::Satisfied => "satisfied",
                GoalCompletion::Blocked => "blocked",
                GoalCompletion::Cancelled => "cancelled",
                GoalCompletion::Open => {
                    return Err("completion cannot transition back to open".to_string())
                }
            },
            &goal,
            "runtime.goal_store",
            vec![RuntimeEventRef {
                kind: "completion_reason".to_string(),
                id: reason.into(),
            }],
        )?;
        Ok(goal)
    }

    fn append_goal_event(
        &self,
        stream_id: &str,
        expected_revision: u64,
        transaction_id: String,
        kind: &str,
        status: &str,
        goal: &GoalContract,
        actor: &str,
        refs: Vec<RuntimeEventRef>,
    ) -> Result<(), String> {
        self.append_goal_event_with_payload(
            stream_id,
            expected_revision,
            transaction_id,
            kind,
            status,
            actor,
            refs,
            serde_json::json!({ "goal": goal }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_goal_event_with_payload(
        &self,
        stream_id: &str,
        expected_revision: u64,
        transaction_id: String,
        kind: &str,
        status: &str,
        actor: &str,
        refs: Vec<RuntimeEventRef>,
        payload: serde_json::Value,
    ) -> Result<(), String> {
        self.event_store
            .append_batch_if_revision(
                stream_id.to_string(),
                expected_revision,
                transaction_id,
                vec![RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Goal,
                    kind: kind.to_string(),
                    status: Some(status.to_string()),
                    actor: Some(actor.to_string()),
                    refs,
                    payload,
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn goal_event(
    goal: &GoalContract,
    kind: &str,
    status: &str,
    actor: String,
    refs: Vec<RuntimeEventRef>,
    payload: serde_json::Value,
    idempotency_key: String,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: stream_id(&goal.id),
            scope: RuntimeEventScope::Goal,
            kind: kind.to_string(),
            status: Some(status.to_string()),
            actor: Some(actor),
            refs,
            payload,
        },
        idempotency_key: Some(idempotency_key),
        schema_version: 1,
    }
}

fn stream_id(goal_id: &str) -> String {
    format!("goal:{goal_id}")
}

fn validate_goal(goal: &GoalContract) -> Result<(), String> {
    if goal.id.trim().is_empty()
        || goal.session_id.trim().is_empty()
        || goal.objective.trim().is_empty()
    {
        return Err("goal id, session_id, and objective must be non-empty".to_string());
    }
    if goal.criteria.is_empty() {
        return Err("goal requires at least one acceptance criterion".to_string());
    }
    if goal
        .criteria
        .iter()
        .any(|criterion| criterion.id.trim().is_empty() || criterion.statement.trim().is_empty())
    {
        return Err("goal criteria require non-empty id and statement".to_string());
    }
    for criterion in &goal.criteria {
        if criterion.status == AcceptanceStatus::Waived {
            let Some(waiver) = &criterion.waiver else {
                return Err(format!(
                    "criterion {} is waived without a durable waiver receipt",
                    criterion.id
                ));
            };
            if waiver.actor.trim().is_empty()
                || waiver.reason.trim().is_empty()
                || waiver.permission_receipt.trim().is_empty()
            {
                return Err(format!(
                    "criterion {} has an invalid waiver receipt",
                    criterion.id
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::goal::{AcceptanceCriterion, AcceptanceStatus};

    fn goal() -> GoalContract {
        GoalContract {
            id: "goal-test".to_string(),
            session_id: "session-test".to_string(),
            objective: "complete governed work".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "checked".to_string(),
                statement: "result is checked".to_string(),
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
    fn goal_revision_is_monotonic_and_completion_requires_acceptance() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()));
        store.create(goal()).unwrap();
        assert!(store
            .complete("goal-test", 1, GoalCompletion::Satisfied, "done")
            .is_err());
        let (revised, revision) = store
            .revise("goal-test", 1, 2, "evidence checked", |goal| {
                goal.criteria[0].status = AcceptanceStatus::Satisfied;
                vec!["criteria.checked".to_string()]
            })
            .unwrap();
        assert_eq!(revision.revision, 2);
        let completed = store
            .complete(
                &revised.id,
                2,
                GoalCompletion::Satisfied,
                "all criteria satisfied",
            )
            .unwrap();
        assert_eq!(completed.completion, GoalCompletion::Satisfied);
        assert_eq!(store.get("goal-test").unwrap(), Some(completed));
    }

    #[test]
    fn revision_event_carries_snapshot_and_revision_for_graph_atomic_commit() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()));
        store.create(goal()).unwrap();

        let (revised, revision, event) = store
            .revision_event("goal-test", 1, 2, "user correction", |goal| {
                goal.constraints.push("latest user correction".to_string());
                vec!["constraints".to_string()]
            })
            .unwrap();

        assert_eq!(revised.revision, 2);
        assert_eq!(event.event.kind, "goal.revised");
        assert_eq!(
            serde_json::from_value::<GoalRevision>(event.event.payload["revision"].clone())
                .unwrap(),
            revision
        );
        assert_eq!(
            serde_json::from_value::<GoalContract>(event.event.payload["goal"].clone()).unwrap(),
            revised
        );
    }
}
