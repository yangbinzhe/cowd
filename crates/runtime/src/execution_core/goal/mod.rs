//! Durable goal state and projections owned by Runtime.
//!
//! The store uses RuntimeEventStore goal streams directly. It deliberately has
//! no scheduler, model loop, or Gateway dependency; interventions remain
//! proposals until RuntimeExecutionSupervisor applies them at a checkpoint.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub mod policy;

use harness_contract::goal::{
    AcceptanceStatus, GoalCompletion, GoalContract, GoalProgressSnapshot, GoalRevision,
    ResolutionDeltaKind, RuntimeIntervention, RuntimeInterventionTrace, RuntimeObservation,
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
    pub intervention_traces: Vec<RuntimeInterventionTrace>,
    pub progress: GoalProgressSnapshot,
}

pub struct GoalProgressReducer;

impl GoalProgressReducer {
    #[must_use]
    pub fn from_goal(goal: &GoalContract) -> GoalProgressSnapshot {
        GoalProgressSnapshot {
            goal_id: goal.id.clone(),
            goal_revision: goal.revision,
            observation_count: 0,
            criteria: goal
                .criteria
                .iter()
                .map(|criterion| (criterion.id.clone(), criterion.status))
                .collect(),
            evidence_refs: sorted_unique(goal.evidence_refs.clone()),
            invalidated_evidence_refs: Vec::new(),
            effects: std::collections::BTreeMap::new(),
            open_conflicts: sorted_unique(goal.blockers.clone()),
            open_unknowns: sorted_unique(goal.unresolved.clone()),
            cumulative_cost: Default::default(),
            last_observed_at_ms: 0,
            applied_observation_keys: Vec::new(),
        }
    }

    pub fn reconcile_goal(
        snapshot: &mut GoalProgressSnapshot,
        previous_goal: &GoalContract,
        goal: &GoalContract,
    ) {
        snapshot.goal_revision = goal.revision;
        for criterion in &goal.criteria {
            let previous_status = previous_goal
                .criteria
                .iter()
                .find(|previous| previous.id == criterion.id)
                .map(|previous| previous.status);
            if previous_status.is_none() || previous_status != Some(criterion.status) {
                snapshot
                    .criteria
                    .insert(criterion.id.clone(), criterion.status);
            }
        }
        snapshot.criteria.retain(|criterion_id, _| {
            goal.criteria
                .iter()
                .any(|criterion| &criterion.id == criterion_id)
        });
        snapshot
            .evidence_refs
            .extend(goal.evidence_refs.iter().cloned());
        normalize(&mut snapshot.evidence_refs);
        for removed in previous_goal
            .blockers
            .iter()
            .filter(|blocker| !goal.blockers.contains(blocker))
        {
            snapshot.open_conflicts.retain(|current| current != removed);
        }
        snapshot.open_conflicts.extend(
            goal.blockers
                .iter()
                .filter(|blocker| !previous_goal.blockers.contains(blocker))
                .cloned(),
        );
        normalize(&mut snapshot.open_conflicts);
        for removed in previous_goal
            .unresolved
            .iter()
            .filter(|unknown| !goal.unresolved.contains(unknown))
        {
            snapshot.open_unknowns.retain(|current| current != removed);
        }
        snapshot.open_unknowns.extend(
            goal.unresolved
                .iter()
                .filter(|unknown| !previous_goal.unresolved.contains(unknown))
                .cloned(),
        );
        normalize(&mut snapshot.open_unknowns);
    }

    /// Apply one typed observation exactly once. Human summaries and generic
    /// numeric scores are deliberately absent from the reducer.
    pub fn apply(
        snapshot: &mut GoalProgressSnapshot,
        observation: &RuntimeObservation,
    ) -> Result<bool, String> {
        validate_observation(observation)?;
        if snapshot.goal_id != observation.goal_id() {
            return Err(format!(
                "observation goal {} does not match snapshot {}",
                observation.goal_id(),
                snapshot.goal_id
            ));
        }
        let key = observation.idempotency_fingerprint();
        if snapshot
            .applied_observation_keys
            .iter()
            .any(|existing| existing == &key)
        {
            return Ok(false);
        }
        for delta in &observation.criterion_deltas {
            let current = snapshot
                .criteria
                .get(&delta.criterion_id)
                .copied()
                .ok_or_else(|| format!("unknown goal criterion {}", delta.criterion_id))?;
            if current != delta.previous {
                return Err(format!(
                    "criterion {} delta is stale: expected {:?}, actual {:?}",
                    delta.criterion_id, delta.previous, current
                ));
            }
            snapshot
                .criteria
                .insert(delta.criterion_id.clone(), delta.current);
            snapshot
                .evidence_refs
                .extend(delta.evidence_refs.iter().cloned());
        }
        for reference in &observation.evidence_delta.invalidated {
            snapshot
                .evidence_refs
                .retain(|current| current != reference);
            snapshot.invalidated_evidence_refs.push(reference.clone());
        }
        snapshot
            .evidence_refs
            .extend(observation.evidence_delta.added.iter().cloned());
        for effect in &observation.effect_deltas {
            snapshot
                .effects
                .insert(effect.effect_id.clone(), effect.terminal_class);
        }
        for conflict in &observation.conflict_deltas {
            apply_resolution_delta(
                &mut snapshot.open_conflicts,
                &conflict.conflict_id,
                conflict.change,
            );
        }
        for unknown in &observation.unknown_deltas {
            apply_resolution_delta(
                &mut snapshot.open_unknowns,
                &unknown.unknown_id,
                unknown.change,
            );
        }
        snapshot.cumulative_cost.model_steps = snapshot
            .cumulative_cost
            .model_steps
            .saturating_add(observation.cost_delta.model_steps);
        snapshot.cumulative_cost.tool_calls = snapshot
            .cumulative_cost
            .tool_calls
            .saturating_add(observation.cost_delta.tool_calls);
        snapshot.cumulative_cost.duration_ms = snapshot
            .cumulative_cost
            .duration_ms
            .saturating_add(observation.cost_delta.duration_ms);
        snapshot.cumulative_cost.input_tokens = snapshot
            .cumulative_cost
            .input_tokens
            .saturating_add(observation.cost_delta.input_tokens);
        snapshot.cumulative_cost.output_tokens = snapshot
            .cumulative_cost
            .output_tokens
            .saturating_add(observation.cost_delta.output_tokens);
        snapshot.cumulative_cost.cached_tokens = snapshot
            .cumulative_cost
            .cached_tokens
            .saturating_add(observation.cost_delta.cached_tokens);
        snapshot.observation_count = snapshot.observation_count.saturating_add(1);
        snapshot.last_observed_at_ms = snapshot
            .last_observed_at_ms
            .max(observation.freshness.observed_at_ms);
        snapshot.applied_observation_keys.push(key);
        if snapshot.applied_observation_keys.len() > 256 {
            snapshot
                .applied_observation_keys
                .drain(..snapshot.applied_observation_keys.len() - 256);
        }
        normalize(&mut snapshot.evidence_refs);
        normalize(&mut snapshot.invalidated_evidence_refs);
        normalize(&mut snapshot.open_conflicts);
        normalize(&mut snapshot.open_unknowns);
        Ok(true)
    }
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
        let mut intervention_traces = Vec::new();
        let mut progress = None;
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
                let event_goal = serde_json::from_value::<GoalContract>(value.clone())
                    .map_err(|error| error.to_string())?;
                if progress.is_none() {
                    progress = Some(GoalProgressReducer::from_goal(&event_goal));
                } else if matches!(
                    event.kind.as_str(),
                    "goal.created" | "goal.revised" | "goal.completed"
                ) {
                    let previous_goal = goal
                        .as_ref()
                        .ok_or_else(|| "goal revision precedes goal creation".to_string())?;
                    GoalProgressReducer::reconcile_goal(
                        progress.as_mut().expect("goal progress initialized"),
                        previous_goal,
                        &event_goal,
                    );
                }
                goal = Some(event_goal);
            }
            if let Some(value) = event.payload.get("observation") {
                let observation = serde_json::from_value::<RuntimeObservation>(value.clone())
                    .map_err(|error| error.to_string())?;
                GoalProgressReducer::apply(
                    progress
                        .as_mut()
                        .ok_or_else(|| "goal observation precedes goal creation".to_string())?,
                    &observation,
                )?;
                observations.push(observation);
                if observations.len() > 256 {
                    observations.remove(0);
                }
            }
            if let Some(value) = event.payload.get("intervention") {
                interventions.push(
                    serde_json::from_value::<RuntimeIntervention>(value.clone())
                        .map_err(|error| error.to_string())?,
                );
                intervention_traces.push(
                    serde_json::from_value::<RuntimeInterventionTrace>(
                        event
                            .payload
                            .get("intervention_trace")
                            .cloned()
                            .ok_or_else(|| {
                                "goal intervention event has no typed trigger trace".to_string()
                            })?,
                    )
                    .map_err(|error| error.to_string())?,
                );
            }
        }
        Ok(goal.map(|goal| GoalProjection {
            progress: progress.unwrap_or_else(|| GoalProgressReducer::from_goal(&goal)),
            goal,
            stream_revision,
            observations,
            interventions,
            intervention_traces,
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
            .map(|mut observations| {
                if observations.len() > 256 {
                    observations.drain(..observations.len() - 256);
                }
                observations
            })
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
        let key = observation.idempotency_fingerprint();
        let projection = self
            .projection(observation.goal_id())?
            .ok_or_else(|| format!("goal {} not found", observation.goal_id()))?;
        if projection
            .progress
            .applied_observation_keys
            .iter()
            .any(|existing| existing == &key)
        {
            return Ok(());
        }
        let event = self.observation_event(&observation, format!("direct:{key}"))?;
        let stream_id = event.event.stream_id.clone();
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                revision,
                format!("goal-observation:{}:{key}", observation.goal_id()),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn record_intervention(
        &self,
        intervention: RuntimeIntervention,
        trigger_observations: &[RuntimeObservation],
    ) -> Result<(), String> {
        let event = self.intervention_event(
            &intervention,
            trigger_observations,
            format!("direct:{}", uuid::Uuid::new_v4()),
        )?;
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
        validate_observation(observation)?;
        let goal = self
            .get(observation.goal_id())?
            .ok_or_else(|| format!("goal {} not found", observation.goal_id()))?;
        Ok(goal_event(
            &goal,
            "goal.observation",
            "observed",
            observation.source.clone(),
            observation
                .evidence_delta
                .added
                .iter()
                .chain(observation.evidence_refs.iter())
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
        trigger_observations: &[RuntimeObservation],
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        let trigger = trigger_observations
            .iter()
            .max_by_key(|observation| observation.freshness.observed_at_ms)
            .ok_or_else(|| {
                "runtime intervention requires at least one typed trigger observation".to_string()
            })?;
        if trigger_observations
            .iter()
            .any(|observation| observation.goal_id() != intervention.goal_id)
        {
            return Err("runtime intervention trigger crosses Goal identity".to_string());
        }
        let trace = RuntimeInterventionTrace {
            identity: trigger.identity.clone(),
            trigger_observation_keys: trigger_observations
                .iter()
                .map(RuntimeObservation::idempotency_fingerprint)
                .collect(),
        };
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
            serde_json::json!({
                "goal": goal,
                "intervention": intervention,
                "intervention_trace": trace,
            }),
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
        let projection = self
            .projection(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let mut goal = projection.goal;
        if goal.completion != GoalCompletion::Open {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        let mut durable_evidence = projection.progress.evidence_refs.clone();
        durable_evidence.extend(evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        if completion == GoalCompletion::Satisfied {
            for criterion in &mut goal.criteria {
                if let Some(status) = projection.progress.criteria.get(&criterion.id) {
                    criterion.status = *status;
                }
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
            if !projection.progress.open_conflicts.is_empty() {
                return Err(format!(
                    "cannot satisfy a goal with unresolved conflicts: {}",
                    projection.progress.open_conflicts.join(", ")
                ));
            }
            if !projection.progress.open_unknowns.is_empty() {
                return Err(format!(
                    "cannot satisfy a goal with unresolved unknowns: {}",
                    projection.progress.open_unknowns.join(", ")
                ));
            }
            if projection
                .progress
                .effects
                .values()
                .any(|effect| *effect == harness_contract::goal::EffectTerminalClass::Uncertain)
            {
                return Err(
                    "cannot satisfy a goal while an effect has no terminal receipt".to_string(),
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
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        goal.revision = goal.revision.saturating_add(1);
        let status = match completion {
            GoalCompletion::Satisfied => "satisfied",
            GoalCompletion::Partial => "partial",
            GoalCompletion::WaitingExternalDecision => "waiting_external",
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
        let projection = self
            .projection(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let mut goal = projection.goal;
        if goal.revision != expected_revision {
            return Err(format!(
                "goal completion has stale revision {expected_revision}"
            ));
        }
        match completion {
            GoalCompletion::Satisfied => {
                for criterion in &mut goal.criteria {
                    if let Some(status) = projection.progress.criteria.get(&criterion.id) {
                        criterion.status = *status;
                    }
                }
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
                if !projection.progress.open_conflicts.is_empty()
                    || !projection.progress.open_unknowns.is_empty()
                    || projection.progress.effects.values().any(|effect| {
                        *effect == harness_contract::goal::EffectTerminalClass::Uncertain
                    })
                {
                    return Err(
                        "cannot satisfy a goal while conflicts, unknowns, or uncertain effects remain unresolved"
                            .to_string(),
                    );
                }
            }
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string())
            }
            GoalCompletion::Partial
            | GoalCompletion::WaitingExternalDecision
            | GoalCompletion::Cancelled => {}
        }
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string())
            }
        };
        goal.evidence_refs = projection.progress.evidence_refs;
        goal.revision = goal.revision.saturating_add(1);
        self.append_goal_event(
            &stream_id,
            stream_revision,
            format!("goal-complete:{goal_id}:{}", goal.revision),
            "goal.completed",
            match completion {
                GoalCompletion::Satisfied => "satisfied",
                GoalCompletion::Partial => "partial",
                GoalCompletion::WaitingExternalDecision => "waiting_external",
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

fn validate_observation(observation: &RuntimeObservation) -> Result<(), String> {
    if observation.identity.workspace_id.trim().is_empty()
        || observation.identity.session_id.trim().is_empty()
        || observation.identity.graph_id.trim().is_empty()
        || observation.identity.goal_id.trim().is_empty()
        || observation.source.trim().is_empty()
        || observation.source_revision == 0
        || observation.freshness.observed_at_ms == 0
        || observation.freshness.policy_revision.trim().is_empty()
        || observation.fingerprint.trim().is_empty()
        || observation
            .identity
            .turn_id
            .as_deref()
            .is_some_and(|turn_id| turn_id.trim().is_empty())
    {
        return Err("runtime observation has incomplete identity or provenance".to_string());
    }
    if observation.failed() && observation.failure_class.is_none() {
        return Err("failed runtime observation requires a typed failure class".to_string());
    }
    if observation.result_class == harness_contract::goal::ObservationResultClass::Succeeded
        && observation.failure_class.is_some()
    {
        return Err("successful runtime observation cannot carry a failure class".to_string());
    }
    Ok(())
}

fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    normalize(&mut values);
    values
}

fn normalize(values: &mut Vec<String>) {
    values.retain(|value| !value.trim().is_empty());
    values.sort();
    values.dedup();
}

fn apply_resolution_delta(values: &mut Vec<String>, id: &str, change: ResolutionDeltaKind) {
    match change {
        ResolutionDeltaKind::Opened => values.push(id.to_string()),
        ResolutionDeltaKind::Resolved => values.retain(|value| value != id),
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
    use harness_contract::{
        core::MeasureProvenance,
        goal::{
            AcceptanceCriterion, AcceptanceStatus, ContextDelta, CostDelta, CriterionDelta,
            EvidenceDelta, InformationGain, ObservationFreshness, ObservationResultClass,
            ParallelismDelta, RuntimeObservationIdentity, RuntimeObservationKind, UnknownDelta,
        },
    };

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

    fn observation(revision: u64) -> RuntimeObservation {
        RuntimeObservation {
            identity: RuntimeObservationIdentity {
                workspace_id: "workspace".to_string(),
                session_id: "session-test".to_string(),
                turn_id: Some("turn".to_string()),
                task_id: None,
                graph_id: "graph".to_string(),
                goal_id: "goal-test".to_string(),
                node_id: Some(format!("node-{revision}")),
            },
            kind: RuntimeObservationKind::ToolProgress,
            source: "test.tool".to_string(),
            source_revision: revision,
            freshness: ObservationFreshness {
                observed_at_ms: revision,
                valid_until_ms: None,
                policy_revision: "goal-observation-v2".to_string(),
            },
            summary: "summary is retained for people, not reduced".to_string(),
            fingerprint: format!("tool-{revision}"),
            evidence_refs: vec![format!("receipt:{revision}")],
            criterion_deltas: Vec::new(),
            evidence_delta: EvidenceDelta::default(),
            effect_deltas: Vec::new(),
            conflict_deltas: Vec::new(),
            unknown_deltas: Vec::new(),
            cost_delta: CostDelta::default(),
            information_gain: InformationGain::default(),
            context_delta: ContextDelta::default(),
            parallelism_delta: ParallelismDelta::default(),
            result_class: ObservationResultClass::Succeeded,
            failure_class: None,
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

    #[test]
    fn typed_observation_reducer_is_idempotent_and_completion_uses_its_evidence() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()));
        store.create(goal()).unwrap();
        let mut observation = observation(1);
        observation.criterion_deltas.push(CriterionDelta {
            criterion_id: "checked".to_string(),
            previous: AcceptanceStatus::Open,
            current: AcceptanceStatus::Satisfied,
            evidence_refs: vec!["evidence:checked".to_string()],
        });
        observation.evidence_delta.added = vec!["evidence:checked".to_string()];
        observation.information_gain = InformationGain {
            distinguishing_evidence_refs: vec!["evidence:checked".to_string()],
            resolved_unknown_refs: Vec::new(),
            provenance: MeasureProvenance::Observed,
        };

        store.record_observation(observation.clone()).unwrap();
        store.record_observation(observation).unwrap();
        let projection = store.projection("goal-test").unwrap().unwrap();
        assert_eq!(projection.progress.observation_count, 1);
        assert_eq!(
            projection.progress.criteria["checked"],
            AcceptanceStatus::Satisfied
        );
        assert_eq!(
            projection.progress.evidence_refs,
            vec!["evidence:checked".to_string()]
        );

        let completed = store
            .complete("goal-test", 1, GoalCompletion::Satisfied, "typed evidence")
            .unwrap();
        assert_eq!(completed.completion, GoalCompletion::Satisfied);
        assert_eq!(
            completed.evidence_refs,
            vec!["evidence:checked".to_string()]
        );
    }

    #[test]
    fn unrelated_goal_revision_preserves_reduced_progress_and_unknown_resolution() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()));
        store.create(goal()).unwrap();
        let mut opened = observation(1);
        opened.unknown_deltas.push(UnknownDelta {
            unknown_id: "pending-check".to_string(),
            change: ResolutionDeltaKind::Opened,
            evidence_refs: Vec::new(),
        });
        store.record_observation(opened).unwrap();
        let mut resolved = observation(2);
        resolved.unknown_deltas.push(UnknownDelta {
            unknown_id: "pending-check".to_string(),
            change: ResolutionDeltaKind::Resolved,
            evidence_refs: vec!["evidence:checked".to_string()],
        });
        resolved.evidence_delta.added = vec!["evidence:checked".to_string()];
        store.record_observation(resolved).unwrap();
        store
            .revise("goal-test", 1, 2, "new user constraint", |goal| {
                goal.constraints.push("keep evidence".to_string());
                vec!["constraints".to_string()]
            })
            .unwrap();

        let projection = store.projection("goal-test").unwrap().unwrap();
        assert!(projection.progress.open_unknowns.is_empty());
        assert_eq!(
            projection.progress.evidence_refs,
            vec!["evidence:checked".to_string()]
        );
    }

    #[test]
    fn intervention_projection_retains_full_trigger_identity() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()));
        store.create(goal()).unwrap();
        let trigger = observation(1);
        store.record_observation(trigger.clone()).unwrap();
        store
            .record_intervention(
                RuntimeIntervention {
                    goal_id: "goal-test".to_string(),
                    kind: harness_contract::goal::RuntimeInterventionKind::Replan,
                    reason: "typed trigger".to_string(),
                    evidence_refs: Vec::new(),
                    expected_graph_revision: None,
                },
                std::slice::from_ref(&trigger),
            )
            .unwrap();

        let projection = store.projection("goal-test").unwrap().unwrap();
        assert_eq!(projection.interventions.len(), 1);
        assert_eq!(projection.intervention_traces.len(), 1);
        assert_eq!(projection.intervention_traces[0].identity, trigger.identity);
        assert_eq!(
            projection.intervention_traces[0].trigger_observation_keys,
            vec![trigger.idempotency_fingerprint()]
        );
    }
}
