//! Durable goal state and projections owned by Runtime.
//!
//! The store uses RuntimeEventStore goal streams directly. It deliberately has
//! no scheduler, model loop, or Gateway dependency; interventions remain
//! proposals until RuntimeExecutionSupervisor applies them at a checkpoint.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::Digest;

pub mod policy;
mod program_completion;
pub mod supervisor;

pub(crate) use program_completion::PreparedProgramConclusion;

pub use supervisor::{ObjectiveReconcileDecision, ObjectiveSupervisor};

use harness_contract::goal::{
    AcceptanceStatus, GoalCompletion, GoalContract, GoalProgressSnapshot, GoalRevision,
    ObjectiveObligationState, ObjectiveRecoveryState, ObjectiveRecoveryStatus, ObjectiveTerminal,
    ObjectiveTerminalKind, ResolutionDeltaKind, RuntimeIntervention, RuntimeInterventionTrace,
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

#[derive(Debug, Clone)]
pub(crate) struct PreparedGoalMutation {
    pub stream_id: String,
    pub expected_stream_revision: u64,
    pub event: RuntimeTransactionEventInput,
}

impl GoalStore {
    /// Prepare the Goal-side half of an Agentic Objective action. The caller
    /// must commit the returned event in the same transaction as the Program
    /// action through AgentActionService::apply_with_goal_event.
    pub(crate) fn prepare_agentic_objective_action(
        &self,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
        producer_refs: &[String],
        verification: Option<&harness_contract::goal::ObjectiveReviewVerification>,
    ) -> Result<PreparedGoalMutation, String> {
        use harness_contract::agent_action::{
            AgentAction, ObjectiveReviewDecision, ObjectiveUpdateOperation,
        };

        let root_execution_id = envelope
            .actor
            .root_execution_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "Objective action requires a root execution binding".to_string())?;
        let goal_id = format!("goal:{root_execution_id}");
        let current = self
            .get(&goal_id)?
            .ok_or_else(|| format!("Objective action target {goal_id} not found"))?;
        let binding = current.execution_binding.as_ref().ok_or_else(|| {
            "Objective action target has no immutable GoalExecutionBinding".to_string()
        })?;
        if binding.objective_id != envelope.actor.objective_id
            || binding.session_id != envelope.actor.session_id
            || binding.turn_id != envelope.actor.turn_id
            || binding.root_execution_id != root_execution_id
            || binding.agentic_program_id != envelope.actor.program_id
        {
            return Err("Objective action binding does not match the durable Goal".to_string());
        }
        if let harness_contract::agent_action::AgentAction::ObjectiveUpdate(input) =
            &envelope.action
        {
            if matches!(
                input.operation,
                harness_contract::agent_action::ObjectiveUpdateOperation::Replace
                    | harness_contract::agent_action::ObjectiveUpdateOperation::Retire
            ) {
                let criterion_ref = input
                    .criterion_ref
                    .as_deref()
                    .ok_or_else(|| "Objective update requires criterion_ref".to_string())?;
                if current.user_intent_criterion_id.as_deref() == Some(criterion_ref) {
                    return Err(
                        "the original user_intent criterion cannot be replaced or retired"
                            .to_string(),
                    );
                }
                if !current
                    .criteria
                    .iter()
                    .any(|criterion| criterion.id == criterion_ref)
                {
                    return Err("Objective update criterion does not exist".to_string());
                }
            }
        }
        if let AgentAction::ObjectiveReview(input) = &envelope.action {
            if !current
                .criteria
                .iter()
                .any(|criterion| criterion.id == input.criterion_ref)
                && !current
                    .obligations
                    .iter()
                    .any(|obligation| obligation.obligation_id == input.criterion_ref)
            {
                return Err(
                    "Objective review criterion or obligation does not exist in the current specification".into(),
                );
            }
            if input.decision == ObjectiveReviewDecision::Satisfied {
                let proof = verification
                    .ok_or("satisfied review requires Runtime physical-read verification")?;
                if proof.goal_spec_revision != Some(current.spec_revision)
                    || proof.goal_spec_digest.as_deref() != Some(current.spec_digest.as_str())
                {
                    return Err("review source Goal specification changed".into());
                }
            }
        }
        let expected_goal_stream_revision = self
            .event_store
            .stream_revision(&stream_id(&goal_id))
            .map_err(|error| error.to_string())?;
        let expected_goal_revision = current.revision;
        let next_sequence = current.user_sequence.saturating_add(1);
        let producer_refs = sorted_unique(producer_refs.to_vec());
        let (updated, _, event) = match &envelope.action {
            AgentAction::ObjectiveUpdate(input) => self.revision_event(
                &goal_id,
                expected_goal_revision,
                next_sequence,
                format!("agentic_objective_update:{}", envelope.action_id),
                |goal| {
                    let changed = match input.operation {
                        ObjectiveUpdateOperation::Add => {
                            let statement_ref = input.statement_ref.as_ref().expect(
                                "AgentAction validation requires statement_ref for objective add",
                            );
                            let criterion_id = format!(
                                "criterion:{:x}",
                                sha2::Sha256::digest(
                                    format!("{}|{}|{}", goal.id, envelope.action_id, statement_ref)
                                        .as_bytes()
                                )
                            );
                            goal.criteria.push(harness_contract::goal::AcceptanceCriterion {
                                id: criterion_id,
                                statement: "Model-proposed criterion (see statement_ref)".to_string(),
                                statement_ref: Some(statement_ref.clone()),
                                source_refs: input.source_refs.clone(),
                                required_evidence: input.evidence_requirements.clone(),
                                status: harness_contract::goal::AcceptanceStatus::Open,
                                waiver: None,
                            });
                            vec!["criteria".to_string()]
                        }
                        ObjectiveUpdateOperation::Replace => {
                            let criterion_ref = input.criterion_ref.as_ref().expect(
                                "AgentAction validation requires criterion_ref for objective replace",
                            );
                            let Some(criterion) =
                                goal.criteria.iter_mut().find(|item| item.id == *criterion_ref)
                            else {
                                return Vec::new();
                            };
                            if let Some(statement_ref) = input.statement_ref.as_ref() {
                                criterion.statement =
                                    "Model-proposed criterion (see statement_ref)".to_string();
                                criterion.statement_ref = Some(statement_ref.clone());
                            }
                            if !input.source_refs.is_empty() {
                                criterion.source_refs.clone_from(&input.source_refs);
                            }
                            if !input.evidence_requirements.is_empty() {
                                criterion
                                    .required_evidence
                                    .clone_from(&input.evidence_requirements);
                            }
                            criterion.status = harness_contract::goal::AcceptanceStatus::Open;
                            vec!["criteria".to_string()]
                        }
                        ObjectiveUpdateOperation::Retire => {
                            let criterion_ref = input.criterion_ref.as_ref().expect(
                                "AgentAction validation requires criterion_ref for objective retire",
                            );
                            let before = goal.criteria.len();
                            goal.criteria.retain(|criterion| criterion.id != *criterion_ref);
                            debug_assert!(before != goal.criteria.len());
                            vec!["criteria".to_string()]
                        }
                    };
                    goal.spec_revision = goal.spec_revision.saturating_add(1);
                    goal.spec_digest = goal_spec_digest(goal);
                    goal.waiting = None;
                    let mut fields = changed;
                    fields.extend([
                        "spec_revision".to_string(),
                        "spec_digest".to_string(),
                        "waiting".to_string(),
                    ]);
                    fields
                },
            )?,
            AgentAction::ObjectiveReview(input) => self.revision_event(
                &goal_id,
                expected_goal_revision,
                next_sequence,
                format!("agentic_objective_review:{}", envelope.action_id),
                |goal| {
                    let decision = match input.decision {
                        ObjectiveReviewDecision::Satisfied => "satisfied",
                        ObjectiveReviewDecision::Gap => "gap",
                        ObjectiveReviewDecision::Blocked => "blocked",
                    };
                    let review_id = format!(
                        "objective_review:{:x}",
                        sha2::Sha256::digest(
                            format!("{}|{}|{}", goal.id, envelope.action_id, input.criterion_ref)
                                .as_bytes()
                        )
                    );
                    if let Some(criterion) = goal.criteria.iter_mut().find(|item| item.id == input.criterion_ref) {
                        criterion.status = match input.decision {
                            ObjectiveReviewDecision::Satisfied => AcceptanceStatus::Satisfied,
                            ObjectiveReviewDecision::Gap | ObjectiveReviewDecision::Blocked => AcceptanceStatus::Open,
                        };
                    } else if let Some(obligation) = goal.obligations.iter_mut().find(|item| item.obligation_id == input.criterion_ref) {
                        obligation.state = match input.decision {
                            ObjectiveReviewDecision::Satisfied => ObjectiveObligationState::Satisfied,
                            ObjectiveReviewDecision::Gap => ObjectiveObligationState::Open,
                            ObjectiveReviewDecision::Blocked => ObjectiveObligationState::Blocked,
                        };
                        obligation.artifact_refs.clone_from(&input.result_refs);
                        obligation.evidence_refs.clone_from(&input.evidence_refs);
                        obligation.reread_receipts = verification.map(|proof| proof.reads.iter()
                            .flat_map(|read| read.receipt_refs.iter().cloned()).collect()).unwrap_or_default();
                        obligation.verifier_decision = (input.decision == ObjectiveReviewDecision::Satisfied).then(|| review_id.clone());
                        obligation.diagnostic_code = (input.decision != ObjectiveReviewDecision::Satisfied).then(|| format!("objective_review_{decision}"));
                    }
                    goal.review_refs.push(review_id.clone());
                    goal.review_refs.sort();
                    goal.review_refs.dedup();
                    goal.reviews.push(harness_contract::goal::ObjectiveReviewRecord {
                        review_id,
                        criterion_ref: input.criterion_ref.clone(),
                        spec_revision: goal.spec_revision,
                        input_manifest_digest: goal.spec_digest.clone(),
                        result_refs: input.result_refs.clone(),
                        evidence_refs: input.evidence_refs.clone(),
                        reviewer_actor: envelope.actor.actor_id.clone(),
                        reviewer_execution_id: envelope.actor.execution_id.clone(),
                        producer_refs: producer_refs.clone(),
                        decision: decision.to_string(),
                        reason_ref: input.reason_ref.clone(),
                        verification: verification.cloned(),
                    });
                    vec!["criteria".to_string(), "obligations".to_string(), "review_refs".to_string(), "reviews".to_string()]
                },
            )?,
            _ => return Err("action is not an Objective mutation".to_string()),
        };
        if updated
            .reviews
            .iter()
            .any(|review| review.reviewer_actor.trim().is_empty())
        {
            return Err("Objective review is missing reviewer identity".to_string());
        }
        if let harness_contract::agent_action::AgentAction::ObjectiveReview(input) =
            &envelope.action
        {
            let review = updated
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == input.criterion_ref)
                .ok_or_else(|| "Objective review was not durably recorded".to_string())?;
            if input.decision == ObjectiveReviewDecision::Satisfied
                && review.producer_refs.is_empty()
            {
                return Err(
                    "Objective review cannot establish independent review without a Runtime-resolved result producer"
                        .to_string(),
                );
            }
            if input.decision == ObjectiveReviewDecision::Satisfied
                && review
                    .verification
                    .as_ref()
                    .is_none_or(|proof| proof.independence_required)
                && review
                    .producer_refs
                    .iter()
                    .any(|producer| producer == &review.reviewer_actor)
            {
                return Err(
                    "Objective reviewer is also a producer of the reviewed result".to_string(),
                );
            }
        }
        let action_entity_ref = match &envelope.action {
            AgentAction::ObjectiveUpdate(input) => match input.operation {
                ObjectiveUpdateOperation::Add => updated
                    .criteria
                    .last()
                    .map(|criterion| criterion.id.clone()),
                _ => input.criterion_ref.clone(),
            },
            AgentAction::ObjectiveReview(_) => updated
                .reviews
                .last()
                .map(|review| review.review_id.clone()),
            _ => None,
        };
        let mut event = event;
        event.event.payload["action_entity_ref"] = serde_json::json!(action_entity_ref);
        Ok(PreparedGoalMutation {
            stream_id: stream_id(&goal_id),
            expected_stream_revision: expected_goal_stream_revision,
            event,
        })
    }
}

pub(crate) fn goal_spec_digest(goal: &GoalContract) -> String {
    let canonical = serde_json::json!({
        "source_intent_ref": goal.source_intent_ref,
        "criteria": goal.criteria.iter().map(|criterion| serde_json::json!({
            "id":criterion.id,"statement":criterion.statement,"statement_ref":criterion.statement_ref,
            "source_refs":criterion.source_refs,"required_evidence":criterion.required_evidence,
            "waiver":criterion.waiver,
        })).collect::<Vec<_>>(),
        "constraints": goal.constraints,
        "participation_requirement": goal.participation_requirement,
        "obligations": goal.obligations.iter().map(|obligation| serde_json::json!({
            "id":obligation.obligation_id,"required":obligation.required,
            "success_predicate":obligation.success_predicate,"producer":obligation.producer,
            "evidence_requirement":obligation.evidence_requirement,
        })).collect::<Vec<_>>(),
    });
    format!(
        "{:x}",
        sha2::Sha256::digest(canonical.to_string().as_bytes())
    )
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
                    "goal.created" | "goal.revised" | "goal.completed" | "goal.completion_waiting"
                ) {
                    let previous_goal = goal
                        .as_ref()
                        .ok_or_else(|| "goal revision precedes goal creation".to_string())?;
                    let goal_progress = progress
                        .as_mut()
                        .ok_or_else(|| "goal progress was not initialized".to_string())?;
                    GoalProgressReducer::reconcile_goal(goal_progress, previous_goal, &event_goal);
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
        if trigger_observations.is_empty() {
            return Err(
                "runtime intervention requires at least one typed trigger observation".to_string(),
            );
        }
        // Settled observers may be delivered more than once after a process
        // restart.  The intervention is a durable proposal, so its
        // idempotency identity must be derived from the typed trigger and
        // intervention—not from a fresh UUID on every projection pass.
        let mut identity = serde_json::to_vec(&intervention).map_err(|error| error.to_string())?;
        for observation in trigger_observations {
            identity.extend_from_slice(observation.idempotency_fingerprint().as_bytes());
        }
        let idempotency_key = format!(
            "goal-intervention:{}:{:x}",
            intervention.goal_id,
            sha2::Sha256::digest(identity),
        );
        let event =
            self.intervention_event(&intervention, trigger_observations, idempotency_key.clone())?;
        let stream_id = event.event.stream_id.clone();
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &idempotency_key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                revision,
                format!("{}:append", idempotency_key,),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Reserve one bounded semantic recovery attempt in the durable Goal
    /// stream.  This is only the authority/fence reservation; orchestration
    /// must still compile and commit the physical recovery patch through the
    /// canonical graph pipeline.
    pub fn reserve_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        source_revision: u64,
        budget: u32,
        idempotency_key: impl Into<String>,
        diagnostic: impl Into<String>,
    ) -> Result<Option<GoalContract>, String> {
        self.reserve_recovery_for_obligation(
            goal_id,
            expected_revision,
            source_revision,
            budget,
            idempotency_key,
            diagnostic,
            None,
        )
    }

    /// Reserve recovery while binding it to the exact source obligation.  The
    /// legacy method above remains available for older callers that do not
    /// have an obligation identity; new orchestration paths should use this
    /// stronger fence whenever one is available.
    pub fn reserve_recovery_for_obligation(
        &self,
        goal_id: &str,
        expected_revision: u64,
        source_revision: u64,
        budget: u32,
        idempotency_key: impl Into<String>,
        diagnostic: impl Into<String>,
        source_obligation_id: Option<String>,
    ) -> Result<Option<GoalContract>, String> {
        let key = idempotency_key.into();
        let diagnostic = diagnostic.into();
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if current.revision != expected_revision {
            return Err(format!(
                "goal recovery revision stale: expected {expected_revision}, actual {}",
                current.revision
            ));
        }
        if let Some(recovery) = &current.recovery {
            if recovery.idempotency_key == key {
                return Ok(Some(current));
            }
            if recovery.attempts >= recovery.budget
                || matches!(
                    recovery.status,
                    ObjectiveRecoveryStatus::Exhausted | ObjectiveRecoveryStatus::Failed
                )
            {
                return Ok(None);
            }
        }
        let next_attempt = current
            .recovery
            .as_ref()
            .map_or(1, |state| state.attempts.saturating_add(1));
        if next_attempt > budget {
            return Ok(None);
        }
        let next_sequence = current.user_sequence.saturating_add(1);
        let (updated, _) = self.revise(
            goal_id,
            expected_revision,
            next_sequence,
            "objective_recovery_reservation",
            |goal| {
                goal.recovery = Some(ObjectiveRecoveryState {
                    source_revision,
                    attempts: next_attempt,
                    budget,
                    status: ObjectiveRecoveryStatus::Pending,
                    idempotency_key: key.clone(),
                    mutation_id: None,
                    source_obligation_id: source_obligation_id.clone(),
                    last_diagnostic: Some(diagnostic.clone()),
                });
                vec!["recovery".to_string()]
            },
        )?;
        Ok(Some(updated))
    }

    /// Claim the physical graph mutation phase. The mutation id is durable so
    /// a crash can safely resume the same patch instead of creating another
    /// Team instance.
    pub fn begin_recovery_graph(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        mutation_id: &str,
    ) -> Result<GoalContract, String> {
        self.transition_recovery(
            goal_id,
            expected_revision,
            idempotency_key,
            ObjectiveRecoveryStatus::ApplyingGraph,
            Some(mutation_id.to_string()),
            None,
            "objective_recovery_applying_graph",
        )
    }

    /// Record that the graph mutation receipt is durable. Goal terminal
    /// verification remains a separate canonical step, but this marker makes
    /// the two-step boundary crash-recoverable and idempotent.
    pub fn mark_recovery_graph_applied(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        mutation_id: &str,
    ) -> Result<GoalContract, String> {
        self.transition_recovery(
            goal_id,
            expected_revision,
            idempotency_key,
            ObjectiveRecoveryStatus::GraphApplied,
            Some(mutation_id.to_string()),
            None,
            "objective_recovery_graph_applied",
        )
    }

    /// Fail closed with an actionable diagnostic. This is intentionally not a
    /// model-visible retry loop; only a fresh reservation can ever create a
    /// new semantic attempt, and budget exhaustion is terminal.
    pub fn fail_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        diagnostic: impl Into<String>,
    ) -> Result<GoalContract, String> {
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let recovery = current
            .recovery
            .as_ref()
            .ok_or_else(|| "goal recovery failure has no reservation".to_string())?;
        if recovery.idempotency_key != idempotency_key {
            return Err("goal recovery failure key mismatch".to_string());
        }
        let status = if recovery.attempts >= recovery.budget {
            ObjectiveRecoveryStatus::Exhausted
        } else {
            ObjectiveRecoveryStatus::Failed
        };
        self.transition_recovery(
            goal_id,
            expected_revision,
            idempotency_key,
            status,
            recovery.mutation_id.clone(),
            Some(diagnostic.into()),
            "objective_recovery_failed",
        )
    }

    fn transition_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        status: ObjectiveRecoveryStatus,
        mutation_id: Option<String>,
        diagnostic: Option<String>,
        reason: &str,
    ) -> Result<GoalContract, String> {
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if current.revision != expected_revision {
            return Err(format!(
                "goal recovery transition revision stale: expected {expected_revision}, actual {}",
                current.revision
            ));
        }
        let recovery = current
            .recovery
            .as_ref()
            .ok_or_else(|| "goal recovery transition has no reservation".to_string())?;
        if recovery.idempotency_key != idempotency_key {
            return Err("goal recovery transition key mismatch".to_string());
        }
        if recovery.status == status
            && (mutation_id.is_none() || recovery.mutation_id == mutation_id)
        {
            return Ok(current);
        }
        let next_sequence = current.user_sequence.saturating_add(1);
        self.revise(goal_id, expected_revision, next_sequence, reason, |goal| {
            if let Some(recovery) = goal.recovery.as_mut() {
                recovery.status = status;
                if mutation_id.is_some() {
                    recovery.mutation_id = mutation_id.clone();
                }
                if let Some(diagnostic) = diagnostic.clone() {
                    recovery.last_diagnostic = Some(diagnostic);
                }
            }
            vec!["recovery.status".to_string()]
        })
        .map(|(goal, _)| goal)
    }

    /// Mark the already-reserved recovery mutation as committed only after the
    /// graph transaction has admitted its replacement work.  A stale caller
    /// cannot overwrite a newer reservation.
    pub fn commit_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
    ) -> Result<GoalContract, String> {
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if current.revision != expected_revision {
            return Err(format!(
                "goal recovery commit revision stale: expected {expected_revision}, actual {}",
                current.revision
            ));
        }
        let recovery = current
            .recovery
            .as_ref()
            .ok_or_else(|| "goal recovery commit has no reservation".to_string())?;
        if recovery.idempotency_key != idempotency_key {
            return Err("goal recovery commit key mismatch".to_string());
        }
        if recovery.status == ObjectiveRecoveryStatus::Committed {
            return Ok(current);
        }
        let next_sequence = current.user_sequence.saturating_add(1);
        self.revise(
            goal_id,
            expected_revision,
            next_sequence,
            "objective_recovery_committed",
            |goal| {
                if let Some(recovery) = goal.recovery.as_mut() {
                    recovery.status = ObjectiveRecoveryStatus::Committed;
                }
                vec!["recovery.status".to_string()]
            },
        )
        .map(|(goal, _)| goal)
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
    /// A live Program owns its successful Goal conclusion through the atomic
    /// Goal/Program transaction. Standalone goals retain the ordinary writer.
    fn validate_terminal_owner(
        &self,
        goal: &GoalContract,
        completion: GoalCompletion,
    ) -> Result<(), String> {
        if completion != GoalCompletion::Satisfied {
            return Ok(());
        }
        if let Some(binding) = &goal.execution_binding {
            let program = crate::AgentActionService::new(Arc::clone(&self.event_store))
                .project_if_exists(&binding.agentic_program_id)
                .map_err(|error| error.to_string())?;
            if program.is_some() {
                return Err(
                    "program_terminal_authority: use the atomic Program conclusion for this Goal"
                        .into(),
                );
            }
        }
        if goal.participation_requirement.is_some() {
            return Err("original_participation_requires_program_conclusion: no bound Program contribution evidence".into());
        }
        Ok(())
    }

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
        self.validate_terminal_owner(&goal, completion)?;
        let mut durable_evidence = projection.progress.evidence_refs.clone();
        durable_evidence.extend(evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        if completion == GoalCompletion::Satisfied {
            let unresolved_obligations = goal
                .obligations
                .iter()
                .filter(|obligation| {
                    obligation.required
                        && (obligation.state != ObjectiveObligationState::Satisfied
                            || (obligation
                                .evidence_requirement
                                .independent_verifier_required
                                && obligation.verifier_decision.is_none())
                            || (obligation.evidence_requirement.reread_required
                                && obligation.reread_receipts.is_empty()))
                })
                .map(|obligation| obligation.obligation_id.clone())
                .collect::<Vec<_>>();
            if !unresolved_obligations.is_empty() {
                return Err(format!(
                    "cannot satisfy a goal while required obligations are unresolved: {}",
                    unresolved_obligations.join(", ")
                ));
            }
            for criterion in &mut goal.criteria {
                if let Some(status) = projection.progress.criteria.get(&criterion.id) {
                    criterion.status = *status;
                }
                if criterion.status == AcceptanceStatus::Open
                    && !criterion.required_evidence.is_empty()
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
        validate_completion(&goal, &projection.progress, completion, &durable_evidence)?;
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
        let terminal_kind = match completion {
            GoalCompletion::Satisfied => harness_contract::goal::ObjectiveTerminalKind::Satisfied,
            GoalCompletion::Partial => {
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied
            }
            GoalCompletion::Blocked => harness_contract::goal::ObjectiveTerminalKind::Blocked,
            GoalCompletion::Failed => harness_contract::goal::ObjectiveTerminalKind::Failed,
            GoalCompletion::WaitingExternalDecision => {
                harness_contract::goal::ObjectiveTerminalKind::Blocked
            }
            GoalCompletion::Cancelled => harness_contract::goal::ObjectiveTerminalKind::Cancelled,
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        goal.terminal = Some(ObjectiveTerminal {
            kind: terminal_kind,
            terminal_fence: idempotency_key.clone(),
            authority_revision: goal.revision.saturating_add(1),
            reason: reason.clone(),
            evidence_refs: goal.evidence_refs.clone(),
            diagnostics: Vec::new(),
            committed_at_ms: crate::tool_invocation::now_ms(),
        });
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Failed => "failed".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        goal.revision = goal.revision.saturating_add(1);
        let status = match completion {
            GoalCompletion::Satisfied => "satisfied",
            GoalCompletion::Partial => "partial",
            GoalCompletion::Blocked => "blocked",
            GoalCompletion::Failed => "failed",
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
        if goal.completion != GoalCompletion::Open || goal.terminal.is_some() {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        self.validate_terminal_owner(&goal, completion)?;
        let durable_evidence = projection.progress.evidence_refs.clone();
        apply_completion_criterion_state(&mut goal, &projection.progress, &durable_evidence);
        validate_completion(&goal, &projection.progress, completion, &durable_evidence)?;
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
            | GoalCompletion::Blocked
            | GoalCompletion::Failed
            | GoalCompletion::WaitingExternalDecision
            | GoalCompletion::Cancelled => {}
        }
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Failed => "failed".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string())
            }
        };
        goal.evidence_refs = durable_evidence;
        goal.revision = goal.revision.saturating_add(1);
        self.append_goal_event(
            &stream_id,
            stream_revision,
            format!("goal-complete:{goal_id}:{}", goal.revision),
            "goal.completed",
            match completion {
                GoalCompletion::Satisfied => "satisfied",
                GoalCompletion::Partial => "partial",
                GoalCompletion::Blocked => "blocked",
                GoalCompletion::Failed => "failed",
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

    /// Commit the business Objective terminal exactly once.  Local Team or
    /// graph terminal facts are inputs to this method, never a substitute for
    /// the obligation/evidence checks below.
    pub fn complete_objective(
        &self,
        goal_id: &str,
        expected_revision: u64,
        terminal: ObjectiveTerminal,
    ) -> Result<GoalContract, String> {
        if terminal.terminal_fence.trim().is_empty() || terminal.reason.trim().is_empty() {
            return Err("objective terminal requires fence and reason".to_string());
        }
        let stream_id = stream_id(goal_id);
        let stream_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let projection = self
            .projection(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let mut goal = projection.goal;
        if let Some(existing) = goal.terminal.as_ref() {
            if existing.terminal_fence == terminal.terminal_fence {
                let mut retry = terminal.clone();
                // Retry wall time is not part of the terminal's semantic identity.
                retry.committed_at_ms = existing.committed_at_ms;
                if existing != &retry {
                    return Err("objective_terminal_fence_conflict: terminal fence names different outcome or evidence".into());
                }
                return Ok(goal);
            }
            return Err(format!(
                "goal {goal_id} already has a different terminal fence"
            ));
        }
        if goal.completion != GoalCompletion::Open {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        if goal.revision != expected_revision {
            return Err(format!(
                "objective terminal has stale revision {expected_revision}, actual {}",
                goal.revision
            ));
        }
        let completion = match terminal.kind {
            ObjectiveTerminalKind::Satisfied => GoalCompletion::Satisfied,
            ObjectiveTerminalKind::PartiallySatisfied => GoalCompletion::Partial,
            ObjectiveTerminalKind::Blocked => GoalCompletion::Blocked,
            ObjectiveTerminalKind::Failed => GoalCompletion::Failed,
            ObjectiveTerminalKind::Cancelled => GoalCompletion::Cancelled,
        };
        self.validate_terminal_owner(&goal, completion)?;
        let mut durable_evidence = projection.progress.evidence_refs.clone();
        durable_evidence.extend(goal.evidence_refs.iter().cloned());
        durable_evidence.extend(terminal.evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        apply_completion_criterion_state(&mut goal, &projection.progress, &durable_evidence);
        if terminal.kind == ObjectiveTerminalKind::Satisfied {
            let required_open = goal.obligations.iter().any(|obligation| {
                obligation.required
                    && (obligation.state != ObjectiveObligationState::Satisfied
                        || (obligation
                            .evidence_requirement
                            .independent_verifier_required
                            && obligation.verifier_decision.is_none())
                        || (obligation.evidence_requirement.reread_required
                            && obligation.reread_receipts.is_empty()))
            });
            if required_open {
                return Err(
                    "cannot satisfy Objective while a required obligation is unresolved"
                        .to_string(),
                );
            }
            if goal.criteria.iter().any(|criterion| {
                !matches!(
                    criterion.status,
                    AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                )
            }) {
                return Err(
                    "cannot satisfy Objective while acceptance criteria are open".to_string(),
                );
            }
        }
        goal.terminal = Some(terminal.clone());
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Failed => "failed".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => return Err("objective terminal cannot be open".to_string()),
        };
        validate_completion(&goal, &projection.progress, completion, &durable_evidence)?;
        goal.evidence_refs = durable_evidence;
        goal.revision = goal.revision.saturating_add(1);
        self.append_goal_event(
            &stream_id,
            stream_revision,
            format!("objective-terminal:{goal_id}:{}", terminal.terminal_fence),
            "goal.completed",
            match completion {
                GoalCompletion::Satisfied => "satisfied",
                GoalCompletion::Partial => "partial",
                GoalCompletion::Blocked => "blocked",
                GoalCompletion::Failed => "failed",
                GoalCompletion::WaitingExternalDecision => "waiting_external",
                GoalCompletion::Cancelled => "cancelled",
                GoalCompletion::Open => return Err("objective terminal cannot be open".to_string()),
            },
            &goal,
            "runtime.objective_supervisor",
            vec![RuntimeEventRef {
                kind: "terminal_fence".to_string(),
                id: terminal.terminal_fence,
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

fn apply_completion_criterion_state(
    goal: &mut GoalContract,
    progress: &GoalProgressSnapshot,
    durable_evidence: &[String],
) {
    for criterion in &mut goal.criteria {
        if let Some(status) = progress.criteria.get(&criterion.id) {
            criterion.status = *status;
        }
        // An empty requirement set means semantic review is still required;
        // all() over an empty iterator must never manufacture success.
        if criterion.status == AcceptanceStatus::Open
            && !criterion.required_evidence.is_empty()
            && criterion
                .required_evidence
                .iter()
                .all(|evidence| durable_evidence.contains(evidence))
        {
            criterion.status = AcceptanceStatus::Satisfied;
        }
    }
}

fn validate_completion(
    goal: &GoalContract,
    progress: &GoalProgressSnapshot,
    completion: GoalCompletion,
    durable_evidence: &[String],
) -> Result<(), String> {
    if completion == GoalCompletion::Open {
        return Err("terminal completion must not be open".to_string());
    }
    if completion == GoalCompletion::WaitingExternalDecision {
        return Err(
            "waiting_external_decision is a resumable wait state, not an immutable Goal terminal"
                .to_string(),
        );
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
    if completion != GoalCompletion::Satisfied {
        return Ok(());
    }
    let unresolved_obligations = goal
        .obligations
        .iter()
        .filter(|obligation| {
            obligation.required
                && (obligation.state != ObjectiveObligationState::Satisfied
                    || (obligation
                        .evidence_requirement
                        .independent_verifier_required
                        && obligation.verifier_decision.is_none())
                    || (obligation.evidence_requirement.reread_required
                        && obligation.reread_receipts.is_empty()))
        })
        .map(|obligation| obligation.obligation_id.clone())
        .collect::<Vec<_>>();
    if !unresolved_obligations.is_empty() {
        return Err(format!(
            "cannot satisfy a goal while required obligations are unresolved: {}",
            unresolved_obligations.join(", ")
        ));
    }
    if goal.criteria.iter().any(|criterion| {
        !matches!(
            criterion.status,
            AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
        )
    }) {
        return Err(
            "cannot satisfy a goal until every current criterion has semantic review or required durable evidence"
                .to_string(),
        );
    }
    let user_intent = goal
        .user_intent_criterion_id
        .as_deref()
        .and_then(|id| goal.criteria.iter().find(|criterion| criterion.id == id));
    if goal.scope == harness_contract::goal::GoalScope::UserObjective && user_intent.is_none() {
        return Err(
            "a user Objective cannot complete without its original user_intent criterion"
                .to_string(),
        );
    }
    if goal.scope == harness_contract::goal::GoalScope::UserObjective && durable_evidence.is_empty()
    {
        return Err(
            "a user Objective cannot complete with an empty durable evidence set".to_string(),
        );
    }
    if !progress.open_conflicts.is_empty() {
        return Err(format!(
            "cannot satisfy a goal with unresolved conflicts: {}",
            progress.open_conflicts.join(", ")
        ));
    }
    if !progress.open_unknowns.is_empty() {
        return Err(format!(
            "cannot satisfy a goal with unresolved unknowns: {}",
            progress.open_unknowns.join(", ")
        ));
    }
    if progress
        .effects
        .values()
        .any(|effect| *effect == harness_contract::goal::EffectTerminalClass::Uncertain)
    {
        return Err("cannot satisfy a goal while an effect has no terminal receipt".to_string());
    }
    Ok(())
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
    if goal.spec_revision == 0 || goal.spec_digest.trim().is_empty() {
        return Err(
            "goal requires a non-zero semantic specification revision and digest".to_string(),
        );
    }
    if goal.scope == harness_contract::goal::GoalScope::UserObjective {
        let source_intent = goal
            .source_intent_ref
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "user Objective requires a durable source_intent_ref".to_string())?;
        let intent_id = goal
            .user_intent_criterion_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "user Objective requires a user_intent criterion".to_string())?;
        if !goal
            .criteria
            .iter()
            .any(|criterion| criterion.id == intent_id)
        {
            return Err("user_intent criterion is not present in the Goal criteria".to_string());
        }
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or_else(|| "user Objective requires immutable GoalExecutionBinding".to_string())?;
        if binding.objective_id.trim().is_empty()
            || binding.session_id != goal.session_id
            || binding.turn_id.trim().is_empty()
            || binding.root_execution_id.trim().is_empty()
            || binding.agentic_program_id.trim().is_empty()
            || source_intent.trim().is_empty()
        {
            return Err(
                "GoalExecutionBinding is incomplete or crosses the Goal Session".to_string(),
            );
        }
    }
    if let Some(requirement) = goal.participation_requirement.as_ref() {
        if requirement.minimum_team_count == 0 || requirement.source_ref.trim().is_empty() {
            return Err(
                "participation requirement needs a user source and positive team count".to_string(),
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
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: harness_contract::goal::GoalScope::Internal,
            user_intent_criterion_id: Some("checked".to_string()),
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
    fn goal_spec_digest_tracks_requirements_without_progress_state() {
        let mut original = goal();
        original
            .obligations
            .push(harness_contract::goal::ObjectiveObligation {
                obligation_id: "delivery".into(),
                required: true,
                success_predicate: "verified source".into(),
                producer: Default::default(),
                evidence_requirement: Default::default(),
                state: ObjectiveObligationState::Open,
                artifact_refs: vec![],
                evidence_refs: vec![],
                reread_receipts: vec![],
                verifier_decision: None,
                diagnostic_code: None,
            });
        let digest = goal_spec_digest(&original);
        let mut progress = original.clone();
        progress.criteria[0].status = AcceptanceStatus::Satisfied;
        progress.obligations[0].state = ObjectiveObligationState::Satisfied;
        progress.obligations[0]
            .evidence_refs
            .push("actual-evidence".into());
        progress.obligations[0].verifier_decision = Some("review".into());
        progress.revision += 1;
        assert_eq!(goal_spec_digest(&progress), digest);
        progress.obligations[0].success_predicate = "different user requirement".into();
        assert_ne!(goal_spec_digest(&progress), digest);
        let mut changed = original.clone();
        changed.obligations[0].required = false;
        assert_ne!(goal_spec_digest(&changed), digest);
        changed = original.clone();
        changed.obligations[0].evidence_requirement.reread_required = true;
        assert_ne!(goal_spec_digest(&changed), digest);
        changed = original;
        changed.obligations[0].producer.capability_id = Some("write".into());
        assert_ne!(goal_spec_digest(&changed), digest);
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
            observed_evidence: Vec::new(),
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

    #[tokio::test]
    async fn model_refinements_preserve_original_goal_and_failed_or_replayed_updates_do_not_write()
    {
        use harness_contract::agent_action::*;
        let services = crate::RuntimeServices::in_memory().unwrap();
        let material = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:session-test".into(),
                    expected_bytes: None,
                    original_name: None,
                },
                b"Check the source-backed result against the user's original requirements",
            )
            .await
            .unwrap();
        let mut original = goal();
        original.id = "goal:refinement-root".into();
        original.scope = harness_contract::goal::GoalScope::UserObjective;
        original.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
            objective_id: "refinement-objective".into(),
            session_id: original.session_id.clone(),
            turn_id: "refinement-turn".into(),
            root_execution_id: "refinement-root".into(),
            agentic_program_id: "refinement-program".into(),
        });
        original
            .obligations
            .push(harness_contract::goal::ObjectiveObligation {
                obligation_id: "original-user-obligation".into(),
                required: true,
                success_predicate: "retain and verify the original user deliverable".into(),
                producer: Default::default(),
                evidence_requirement: Default::default(),
                state: ObjectiveObligationState::Open,
                artifact_refs: vec![],
                evidence_refs: vec![],
                reread_receipts: vec![],
                verifier_decision: None,
                diagnostic_code: None,
            });
        services.goal_store().create(original.clone()).unwrap();
        let actor = AgentActorBinding {
            objective_id: "refinement-objective".into(),
            program_id: "refinement-program".into(),
            session_id: "session-test".into(),
            turn_id: "refinement-turn".into(),
            root_execution_id: Some("refinement-root".into()),
            required_team_count: 0,
            objective_summary: original.objective.clone(),
            model_lease: "test".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec![],
            actor_id: "refinement-root-actor".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let action = |id: &str, operation, criterion_ref: Option<String>| AgentActionEnvelope {
            action_id: id.into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::ObjectiveUpdate(ObjectiveUpdateInput {
                criterion_ref,
                operation,
                statement_ref: Some(material.selector.clone()),
                source_refs: vec![material.selector.clone()],
                reason_ref: Some(material.selector.clone()),
                evidence_requirements: vec![],
            }),
        };
        let add = action("add-derived", ObjectiveUpdateOperation::Add, None);
        let added = services.submit_agent_action(&add).await.unwrap();
        assert_eq!(added.status, AgentActionStatus::Applied);
        let first = services.goal_store().get(&original.id).unwrap().unwrap();
        assert_eq!(first.spec_revision, 2);
        assert_eq!(first.criteria[0], original.criteria[0]);
        let derived = added
            .changed_refs
            .first()
            .expect("model receives its generated criterion reference")
            .clone();
        assert_eq!(derived, first.criteria[1].id);
        let inspect = |id: &str, entry_ref: Option<String>, page_cursor: Option<String>| {
            AgentActionEnvelope {
                action_id: id.into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::StateInspect(StateInspectInput {
                    query: entry_ref.is_none().then(|| original.id.clone()),
                    entry_ref,
                    page_cursor,
                    ..Default::default()
                }),
            }
        };
        let visible = services
            .submit_agent_action(&inspect("inspect-derived", Some(derived.clone()), None))
            .await
            .unwrap();
        assert_eq!(visible.status, AgentActionStatus::Observed);
        assert_eq!(
            visible.projection.unwrap()["goal_entry"]["criterion"]["id"],
            derived
        );
        let review = |id: &str| AgentActionEnvelope {
            action_id: id.into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::ObjectiveReview(ObjectiveReviewInput {
                criterion_ref: derived.clone(),
                decision: ObjectiveReviewDecision::Gap,
                result_refs: vec![],
                evidence_refs: vec![material.selector.clone()],
                reason_ref: material.selector.clone(),
            }),
        };
        let team = services
            .submit_agent_action(&AgentActionEnvelope {
                action_id: "refinement-support-team".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::TeamCreate(TeamCreateInput {
                    name: "Support".into(),
                    mission: "Support existing conditions without replacing them".into(),
                    objective: None,
                }),
            })
            .await
            .unwrap()
            .changed_refs[0]
            .clone();
        let publish = |id: &str, references: Vec<String>, purpose| AgentActionEnvelope {
            action_id: id.into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: id.into(),
                objective: "Check source".into(),
                acceptance: "source-backed evidence".into(),
                required_capabilities: vec![],
                depends_on: vec![],
                obligation_refs: references,
                purpose,
                execution_requirements: vec![],
                expertise_hints: vec![],
            }),
        };
        let refs = vec![
            derived.clone(),
            "original-user-obligation".into(),
            "checked".into(),
        ];
        let supporting = publish("support-many", refs.clone(), TaskPurpose::Delivery);
        for request in [
            supporting.clone(),
            publish("support-same", refs.clone(), TaskPurpose::Delivery),
            publish("optional-exploration", vec![], TaskPurpose::Exploration),
        ] {
            let result = services.submit_agent_action(&request).await.unwrap();
            assert_eq!(result.status, AgentActionStatus::Applied);
            let projection = services
                .agent_action_service()
                .project(&actor.program_id)
                .unwrap();
            let task = &projection.tasks[&result.changed_refs[0]];
            let AgentAction::TaskPublish(input) = request.action else {
                unreachable!()
            };
            assert_eq!(task.obligation_refs, input.obligation_refs);
            assert_eq!(task.purpose, input.purpose);
        }
        assert_eq!(
            services.goal_store().get(&original.id).unwrap().unwrap(),
            first,
            "publishing supporting or exploration work cannot change Goal obligations or success"
        );
        let before_invalid = services
            .agent_action_service()
            .project(&actor.program_id)
            .unwrap()
            .revision;
        let invalid = services
            .submit_agent_action(&publish(
                "unknown-obligation",
                vec!["foreign-condition".into()],
                TaskPurpose::Delivery,
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status, AgentActionStatus::Rejected);
        assert_eq!(invalid.error.unwrap().code, "obligation_not_found");
        assert_eq!(
            services
                .agent_action_service()
                .project(&actor.program_id)
                .unwrap()
                .revision,
            before_invalid
        );
        for operation in [
            ObjectiveUpdateOperation::Replace,
            ObjectiveUpdateOperation::Retire,
        ] {
            if operation == ObjectiveUpdateOperation::Retire {
                let reviewed = services
                    .submit_agent_action(&review("derived-gap"))
                    .await
                    .unwrap();
                assert_eq!(reviewed.status, AgentActionStatus::Applied);
                assert!(reviewed.changed_refs[0].starts_with("objective_review:"));
            }
            let before = services
                .event_store()
                .stream_revision("goal:goal:refinement-root")
                .unwrap();
            let before_program = services
                .agent_action_service()
                .project(&actor.program_id)
                .unwrap()
                .revision;
            let rejected = action(
                &format!("forbidden-{operation:?}"),
                operation,
                Some("checked".into()),
            );
            assert!(services
                .submit_agent_action(&rejected)
                .await
                .unwrap_err()
                .contains("original user_intent"));
            assert_eq!(
                services
                    .event_store()
                    .stream_revision("goal:goal:refinement-root")
                    .unwrap(),
                before
            );
            assert_eq!(
                services
                    .agent_action_service()
                    .project(&actor.program_id)
                    .unwrap()
                    .revision,
                before_program
            );
            let allowed = action(
                &format!("derived-{operation:?}"),
                operation,
                Some(derived.clone()),
            );
            assert_eq!(
                services.submit_agent_action(&allowed).await.unwrap().status,
                AgentActionStatus::Applied
            );
        }
        let before_retired = services
            .agent_action_service()
            .project(&actor.program_id)
            .unwrap()
            .revision;
        let retired = services
            .submit_agent_action(&publish(
                "retired-support",
                vec![derived.clone()],
                TaskPurpose::Delivery,
            ))
            .await
            .unwrap();
        assert_eq!(retired.status, AgentActionStatus::Rejected);
        assert_eq!(retired.error.unwrap().code, "obligation_not_found");
        let replayed = services.submit_agent_action(&supporting).await.unwrap();
        assert!(replayed.duplicate);
        assert_eq!(replayed.status, AgentActionStatus::Applied);
        assert_eq!(
            services
                .agent_action_service()
                .project(&actor.program_id)
                .unwrap()
                .revision,
            before_retired
        );
        let current = services.goal_store().get(&original.id).unwrap().unwrap();
        assert_eq!(current.criteria, original.criteria);
        assert_eq!(current.obligations, original.obligations);
        assert_eq!(current.source_intent_ref, original.source_intent_ref);
        assert_eq!(current.spec_revision, 4);
        assert!(services
            .submit_agent_action(&review("cannot-review-retired-criterion"))
            .await
            .unwrap_err()
            .contains("does not exist"));

        let before = services
            .event_store()
            .stream_revision("goal:goal:refinement-root")
            .unwrap();
        assert_eq!(
            services.submit_agent_action(&add).await.unwrap().status,
            AgentActionStatus::Applied
        );
        assert_eq!(
            services
                .event_store()
                .stream_revision("goal:goal:refinement-root")
                .unwrap(),
            before
        );
        let mut forged = serde_json::to_value(&add.action).unwrap();
        forged["input"]["waiver"] = serde_json::json!({"permission_receipt":"model-says-approved"});
        assert!(serde_json::from_value::<AgentAction>(forged).is_err());
        let mut forged_receipt = action("forged-statement", ObjectiveUpdateOperation::Add, None);
        if let AgentAction::ObjectiveUpdate(input) = &mut forged_receipt.action {
            input.statement_ref = Some("tool://invented".into());
        }
        assert!(services.submit_agent_action(&forged_receipt).await.is_err());
        assert_eq!(
            services
                .event_store()
                .stream_revision("goal:goal:refinement-root")
                .unwrap(),
            before
        );
        // Independent Goal owner updates must also invalidate a directory
        // cursor even when no Program action changes its revision.
        let current = services.goal_store().get(&original.id).unwrap().unwrap();
        services
            .goal_store()
            .revise(
                &original.id,
                current.revision,
                current.user_sequence + 1,
                "test owner adds a paged criterion set",
                |goal| {
                    for index in 0..40 {
                        let mut criterion = original.criteria[0].clone();
                        criterion.id = format!("criterion:paged:{index:03}");
                        goal.criteria.push(criterion);
                    }
                    goal.spec_revision += 1;
                    goal.spec_digest = goal_spec_digest(goal);
                    vec![
                        "criteria".into(),
                        "spec_revision".into(),
                        "spec_digest".into(),
                    ]
                },
            )
            .unwrap();
        let page = services
            .submit_agent_action(&inspect("goal-directory", None, None))
            .await
            .unwrap()
            .projection
            .unwrap();
        assert_eq!(page["entries"].as_array().unwrap().len(), 32);
        let cursor = page["next_page_cursor"].as_str().unwrap().to_string();
        let program_revision = services
            .agent_action_service()
            .project(&actor.program_id)
            .unwrap()
            .revision;
        let current = services.goal_store().get(&original.id).unwrap().unwrap();
        services
            .goal_store()
            .revise(
                &original.id,
                current.revision,
                current.user_sequence + 1,
                "test independent Goal revision",
                |goal| {
                    goal.constraints.push("new current constraint".into());
                    goal.spec_revision += 1;
                    goal.spec_digest = goal_spec_digest(goal);
                    vec![
                        "constraints".into(),
                        "spec_revision".into(),
                        "spec_digest".into(),
                    ]
                },
            )
            .unwrap();
        assert_eq!(
            services
                .agent_action_service()
                .project(&actor.program_id)
                .unwrap()
                .revision,
            program_revision
        );
        let stale = services
            .submit_agent_action(&inspect("stale-goal-directory", None, Some(cursor)))
            .await
            .unwrap();
        assert_eq!(stale.status, AgentActionStatus::Rejected);
        assert!(stale.error.unwrap().message.contains("revision"));
    }

    fn terminal_for_test() -> ObjectiveTerminal {
        ObjectiveTerminal {
            kind: ObjectiveTerminalKind::Satisfied,
            terminal_fence: "terminal-test".into(),
            authority_revision: 1,
            reason: "verified result".into(),
            evidence_refs: vec![],
            diagnostics: vec![],
            committed_at_ms: 1,
        }
    }

    #[test]
    fn all_public_terminal_writers_reject_open_obligations_uncertain_effects_and_empty_evidence() {
        for writer in ["terminal_event", "complete", "complete_objective"] {
            for defect in ["obligation", "effect", "empty", "participation"] {
                let events = Arc::new(RuntimeEventStore::for_test());
                let store = GoalStore::new(Arc::clone(&events));
                let mut goal = goal();
                goal.scope = harness_contract::goal::GoalScope::UserObjective;
                goal.criteria[0].status = AcceptanceStatus::Satisfied;
                goal.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
                    objective_id: "user-objective".into(),
                    session_id: goal.session_id.clone(),
                    turn_id: "turn".into(),
                    root_execution_id: "standalone-root".into(),
                    agentic_program_id: "not-created-program".into(),
                });
                if defect == "participation" {
                    goal.participation_requirement =
                        Some(harness_contract::goal::ParticipationRequirement {
                            minimum_team_count: 2,
                            source_ref: "session_message:original".into(),
                        });
                }
                if defect == "obligation" {
                    goal.obligations
                        .push(harness_contract::goal::ObjectiveObligation {
                            obligation_id: "required-user-work".into(),
                            required: true,
                            success_predicate: "verify the result".into(),
                            producer: Default::default(),
                            evidence_requirement: Default::default(),
                            state: ObjectiveObligationState::Open,
                            artifact_refs: vec![],
                            evidence_refs: vec![],
                            reread_receipts: vec![],
                            verifier_decision: None,
                            diagnostic_code: None,
                        });
                }
                store.create(goal).unwrap();
                if defect != "empty" {
                    let mut observed = observation(1);
                    observed.evidence_delta.added = vec!["receipt:actual".into()];
                    if defect == "effect" {
                        observed
                            .effect_deltas
                            .push(harness_contract::goal::EffectDelta {
                                effect_id: "external-effect".into(),
                                terminal_class:
                                    harness_contract::goal::EffectTerminalClass::Uncertain,
                                idempotency_ref: "effect-request".into(),
                            });
                    }
                    store.record_observation(observed).unwrap();
                }
                let before = events.stream_revision("goal:goal-test").unwrap();
                let result = match writer {
                    "terminal_event" => store
                        .terminal_event(
                            "goal-test",
                            GoalCompletion::Satisfied,
                            vec![],
                            "done".into(),
                            "terminal-test".into(),
                        )
                        .map(|_| ()),
                    "complete" => store
                        .complete("goal-test", 1, GoalCompletion::Satisfied, "done")
                        .map(|_| ()),
                    _ => store
                        .complete_objective("goal-test", 1, terminal_for_test())
                        .map(|_| ()),
                };
                let error = result.expect_err("writer must not bypass unmet acceptance");
                assert!(error.contains(defect), "{writer}/{defect}: {error}");
                assert_eq!(events.stream_revision("goal:goal-test").unwrap(), before);
                assert_eq!(
                    store.get("goal-test").unwrap().unwrap().completion,
                    GoalCompletion::Open
                );
            }
        }
    }

    #[test]
    fn legacy_complete_cannot_emit_a_second_terminal_or_be_replaced_by_an_objective_fence() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let store = GoalStore::new(Arc::clone(&events));
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        store.create(goal).unwrap();
        let completed = store
            .complete("goal-test", 1, GoalCompletion::Satisfied, "done")
            .unwrap();
        let before = events.stream_revision("goal:goal-test").unwrap();
        assert!(store
            .complete(
                "goal-test",
                completed.revision,
                GoalCompletion::Satisfied,
                "again"
            )
            .is_err());
        assert!(store
            .complete_objective("goal-test", completed.revision, terminal_for_test())
            .is_err());
        assert_eq!(events.stream_revision("goal:goal-test").unwrap(), before);
    }

    #[test]
    fn objective_terminal_fence_replays_same_result_before_revision_check_and_rejects_changed_evidence(
    ) {
        let events = Arc::new(RuntimeEventStore::for_test());
        let store = GoalStore::new(Arc::clone(&events));
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        store.create(goal).unwrap();
        let completed = store
            .complete_objective("goal-test", 1, terminal_for_test())
            .unwrap();
        let before = events.stream_revision("goal:goal-test").unwrap();
        let mut retry = terminal_for_test();
        retry.committed_at_ms = 2;
        assert_eq!(
            store
                .complete_objective("goal-test", 1, retry.clone())
                .unwrap(),
            completed
        );
        retry.evidence_refs.push("different-evidence".into());
        assert!(store
            .complete_objective("goal-test", 1, retry)
            .unwrap_err()
            .contains("fence_conflict"));
        assert_eq!(events.stream_revision("goal:goal-test").unwrap(), before);
    }

    #[test]
    fn goal_revision_is_monotonic_and_completion_requires_acceptance() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
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
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
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
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
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
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
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
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
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
