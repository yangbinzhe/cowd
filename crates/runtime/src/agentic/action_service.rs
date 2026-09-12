use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActionErrorObservation, AgentActionObservation,
    AgentActionStatus, AgentActorKind,
};
use harness_contract::goal::ObjectiveReviewVerification;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError, RuntimeTransactionEventInput,
};

use super::program::{AgenticProgramProjection, AgenticTaskStatus, AgenticTopicEntryProjection};

mod authorization;
mod delegation;

use authorization::validate_transition;

const ACTION_EVENT_KIND: &str = "agentic.action_applied";
const PROGRAM_OPENED_EVENT_KIND: &str = "agentic.program_opened";
const OBJECTIVE_VERDICT_EVENT_KIND: &str = "agentic.objective_verdict_bound";
const COMPLETION_REOPENED_EVENT_KIND: &str = "agentic.completion_reopened";
const TOPIC_CURSOR_EVENT_KIND: &str = "agentic.topic_observation_acknowledged";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationPage {
    pub entries: Vec<AgenticTopicObservation>,
    pub from_revision: u64,
    pub to_revision: u64,
    pub cursor_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct AgenticTopicObservation {
    pub topic_ref: String,
    #[serde(flatten)]
    pub entry: AgenticTopicEntryProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopicObservationKind {
    ProviderModel,
    WorkerTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationAck {
    pub program_id: String,
    pub execution_id: String,
    pub through_revision: u64,
    pub expected_cursor_revision: u64,
    pub observation_kind: TopicObservationKind,
}

#[derive(Debug, Error)]
pub enum AgentActionServiceError {
    #[error(transparent)]
    Store(#[from] RuntimeEventStoreError),
    #[error("agent action event store failed: {0}")]
    EventStore(String),
    #[error("agent action serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("agent action journal is corrupt: {0}")]
    Corrupt(String),
}

#[derive(Clone)]
pub struct AgentActionService {
    store: Arc<RuntimeEventStore>,
    artifacts: Option<Arc<crate::ArtifactStore>>,
    graphs: Option<crate::ExecutionGraphStateStore>,
    read_model: Arc<super::AgenticReadModel>,
}

impl AgentActionService {
    #[must_use]
    pub fn new(store: Arc<RuntimeEventStore>) -> Self {
        Self {
            store,
            artifacts: None,
            graphs: None,
            read_model: Arc::new(super::AgenticReadModel::default()),
        }
    }

    #[must_use]
    pub(crate) fn with_graph_store(mut self, graphs: crate::ExecutionGraphStateStore) -> Self {
        self.graphs = Some(graphs);
        self
    }

    #[must_use]
    pub(crate) fn with_read_model(mut self, read_model: Arc<super::AgenticReadModel>) -> Self {
        self.read_model = read_model;
        self
    }

    /// Bind the physical artifact authority used by production Runtime
    /// services. Unit projections may omit it; an active Runtime never does.
    #[must_use]
    pub fn with_artifact_store(mut self, artifacts: Arc<crate::ArtifactStore>) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    pub(crate) fn apply(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream_id.clone()];
        if matches!(&envelope.action, AgentAction::TaskPublish(input) if !input.obligation_refs.is_empty())
        {
            if let Some(root) = &envelope.actor.root_execution_id {
                streams.push(format!("goal:goal:{root}"));
            }
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(envelope, &stream_id, None, None)
        })
    }

    /// Commit one Program action and its prepared Goal revision atomically.
    /// The Goal mutation is prepared from durable state before acquiring the
    /// ordered two-stream lock; expected revisions make a stale preparation a
    /// whole-transaction conflict rather than a partial Objective update.
    pub(crate) fn apply_with_goal_event(
        &self,
        envelope: &AgentActionEnvelope,
        goal_stream_id: String,
        expected_goal_stream_revision: u64,
        goal_event: RuntimeTransactionEventInput,
        verification: Option<&ObjectiveReviewVerification>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let program_stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![program_stream_id.clone(), goal_stream_id.clone()];
        streams.extend(
            verification
                .into_iter()
                .flat_map(|proof| proof.result_source_revisions.keys().cloned()),
        );
        if verification.is_some_and(|proof| {
            !proof.independence_required || proof.effect_manifest_digest.is_some()
        }) {
            streams.push(format!("session:{}", envelope.actor.session_id));
            streams.extend(
                verification
                    .into_iter()
                    .flat_map(|proof| proof.effect_source_refs.iter().cloned()),
            );
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(
                envelope,
                &program_stream_id,
                Some((goal_stream_id, expected_goal_stream_revision, goal_event)),
                verification,
            )
        })
    }

    pub(crate) fn replay_if_applied(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<Option<AgentActionObservation>, AgentActionServiceError> {
        let stream_id = program_stream(&envelope.actor.program_id);
        if let Some(existing) = self
            .store
            .event_by_idempotency_key(&stream_id, &action_key(&envelope.action_id))?
        {
            let projection = self.project_snapshot(&envelope.actor.program_id)?;
            let original: AgentActionEnvelope = serde_json::from_value(
                existing.payload.get("envelope").cloned().ok_or_else(|| {
                    AgentActionServiceError::Corrupt(
                        "applied action has no durable envelope".into(),
                    )
                })?,
            )?;
            if original.actor != envelope.actor || original.action != envelope.action {
                return Ok(Some(rejected(envelope, projection.revision, "action_id_conflict",
                    "action_id already identifies a different actor or payload; use the original request to retry")));
            }
            let entity_ref = existing
                .payload
                .get("entity_ref")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            return applied_observation(envelope, &projection, entity_ref, true).map(Some);
        }

        Ok(None)
    }

    pub(crate) fn apply_verified_review(
        &self,
        envelope: &AgentActionEnvelope,
        verification: &ObjectiveReviewVerification,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        let stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream_id.clone()];
        streams.extend(verification.result_source_revisions.keys().cloned());
        if verification.effect_manifest_digest.is_some() {
            streams.push(format!("session:{}", envelope.actor.session_id));
            streams.extend(verification.effect_source_refs.iter().cloned());
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(envelope, &stream_id, None, Some(verification))
        })
    }

    fn apply_locked(
        &self,
        envelope: &AgentActionEnvelope,
        stream_id: &str,
        goal_event: Option<(String, u64, RuntimeTransactionEventInput)>,
        verification: Option<&ObjectiveReviewVerification>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Some(replay) = self.replay_if_applied(envelope)? {
            return Ok(replay);
        }

        let mut projection = match self.project_snapshot(&envelope.actor.program_id) {
            Ok(projection) => projection,
            Err(AgentActionServiceError::Corrupt(message)) if message == "program_not_found" => {
                let mut projection = AgenticProgramProjection::empty(
                    &envelope.actor.program_id,
                    &envelope.actor.objective_id,
                );
                projection.required_team_count = envelope.actor.required_team_count;
                projection
                    .objective_summary
                    .clone_from(&envelope.actor.objective_summary);
                if !envelope.actor.model_lease.trim().is_empty() {
                    projection
                        .model_lease
                        .clone_from(&envelope.actor.model_lease);
                }
                projection.permission_ceiling = envelope
                    .actor
                    .permission_ceiling
                    .unwrap_or(harness_contract::policy::PermissionMode::ReadOnly);
                projection
                    .resource_scopes
                    .clone_from(&envelope.actor.resource_scopes);
                projection.session_id.clone_from(&envelope.actor.session_id);
                projection.turn_id.clone_from(&envelope.actor.turn_id);
                projection
                    .root_execution_id
                    .clone_from(&envelope.actor.root_execution_id);
                Arc::new(projection)
            }
            Err(error) => return Err(error),
        };
        if projection.objective_id != envelope.actor.objective_id {
            return Ok(rejected(
                envelope,
                projection.revision,
                "objective_binding_mismatch",
                "the Program is bound to a different Objective",
            ));
        }
        if projection.session_id != envelope.actor.session_id
            || projection.turn_id != envelope.actor.turn_id
            || projection.root_execution_id != envelope.actor.root_execution_id
            || projection.required_team_count != envelope.actor.required_team_count
            || projection.objective_summary != envelope.actor.objective_summary
            || projection.model_lease != envelope.actor.model_lease
            || Some(projection.permission_ceiling) != envelope.actor.permission_ceiling
            || projection.resource_scopes != envelope.actor.resource_scopes
        {
            return Ok(rejected(
                envelope,
                projection.revision,
                "program_binding_mismatch",
                "session, turn, Objective, execution policy, or completion obligation differs from the frozen Program binding",
            ));
        }

        if matches!(envelope.action, AgentAction::StateInspect(_)) {
            let goal = projection
                .root_execution_id
                .as_ref()
                .map(|root| {
                    crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                        .get(&format!("goal:{root}"))
                })
                .transpose()
                .map_err(AgentActionServiceError::Corrupt)?
                .flatten();
            if matches!(&envelope.action, AgentAction::StateInspect(input)
                if input.scope_ref.as_deref() == Some("collaboration_patterns"))
            {
                let patterns =
                    match crate::evolution::collaboration_experience::read_patterns(&self.store, 8)
                    {
                        Ok(patterns) => patterns,
                        Err(error) => {
                            return Ok(rejected(
                                envelope,
                                projection.revision,
                                "advisory_evidence_unavailable",
                                &error,
                            ))
                        }
                    };
                let mut observation = inspected_observation(envelope, &projection, goal.as_ref());
                observation.projection = Some(json!({
                    "advisory_only": true,
                    "instruction": "These are structural observations from independent completed Turns, not prescribed plans, quality guarantees, executable definitions, or capability grants. Adopt, adapt, or ignore them. Inspect Runtime capabilities before using any suggested tool or skill.",
                    "patterns": patterns.iter().filter(|pattern| pattern.is_actionable()).map(|pattern| json!({
                        "pattern_ref": pattern.pattern_id,
                        "revision": pattern.pattern_revision,
                        "suggestion": pattern.semantic_suggestion,
                        "evidence_summary": pattern.evidence_summary,
                    })).collect::<Vec<_>>(),
                }));
                return Ok(observation);
            }
            return Ok(inspected_observation(envelope, &projection, goal.as_ref()));
        }
        if verification.is_some_and(|proof| {
            proof.work_manifest_digest != super::review_evidence::work_manifest_digest(&projection)
        }) {
            return Ok(rejected(
                envelope,
                projection.revision,
                "stale_review_evidence",
                "source work changed while review evidence was being verified",
            ));
        }
        let mut policy_sources = Vec::new();
        if let Some(proof) = verification.filter(|proof| !proof.independence_required) {
            if envelope.actor.kind != AgentActorKind::Root
                || !matches!(envelope.action, AgentAction::ObjectiveReview(_))
                || goal_event.is_none()
            {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "invalid_review_policy",
                    "self review is confined to the bound Root Objective",
                ));
            }
            let root = envelope
                .actor
                .root_execution_id
                .as_deref()
                .ok_or_else(|| AgentActionServiceError::Corrupt("review has no root".into()))?;
            let goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                .get(&format!("goal:{root}"))
                .map_err(AgentActionServiceError::EventStore)?
                .ok_or_else(|| AgentActionServiceError::Corrupt("review has no Goal".into()))?;
            let policy =
                super::review_evidence::root_self_review_policy(&self.store, &projection, &goal)
                    .map_err(AgentActionServiceError::EventStore)?;
            let Some(policy) = policy.filter(|policy| {
                proof.review_policy_digest.as_deref() == Some(policy.digest.as_str())
            }) else {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_review_policy",
                    "Runtime risk or tool effects no longer permit this review",
                ));
            };
            policy_sources.push(policy.source);
        }
        if let Some(digest) = verification.and_then(|proof| proof.effect_manifest_digest.as_deref())
        {
            let root = envelope.actor.root_execution_id.as_deref().ok_or_else(|| {
                AgentActionServiceError::Corrupt("effect review has no root".into())
            })?;
            let goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                .get(&format!("goal:{root}"))
                .map_err(AgentActionServiceError::EventStore)?;
            let snapshot = super::review_evidence::effect_review_snapshot(
                &self.store,
                &projection,
                goal.as_ref(),
                &verification
                    .expect("effect digest requires proof")
                    .effect_source_refs,
            )
            .map_err(AgentActionServiceError::EventStore)?;
            if snapshot.digest != digest {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_effect_review",
                    "admitted effects changed after verification; inspect the current target again",
                ));
            }
            policy_sources.retain(|source| source.stream_id != snapshot.source.stream_id);
            policy_sources.push(snapshot.source);
            policy_sources.extend(snapshot.additional_sources);
        }
        if let Some(expected) = envelope.expected_revision {
            if expected != projection.revision {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_revision",
                    &format!(
                        "expected Program revision {expected}, actual {}",
                        projection.revision
                    ),
                ));
            }
        }
        if let Some((code, message)) =
            validate_transition(&projection, envelope, now_ms(), self.graphs.as_ref())
        {
            return Ok(rejected(envelope, projection.revision, code, &message));
        }
        // A Task may support several original Goal conditions, but it cannot
        // manufacture them. Keep the read revision in this same transaction so
        // a concurrent Goal change cannot admit a dangling reference.
        let mut obligation_source = None;
        if let AgentAction::TaskPublish(input) = &envelope.action {
            if !input.obligation_refs.is_empty() {
                let goal = projection
                    .root_execution_id
                    .as_ref()
                    .map(|root| {
                        crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                            .projection(&format!("goal:{root}"))
                    })
                    .transpose()
                    .map_err(AgentActionServiceError::Corrupt)?
                    .flatten();
                let Some(goal) = goal else {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_goal_not_found",
                        "Task obligation references require the bound durable Goal",
                    ));
                };
                if !goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                    binding.agentic_program_id == projection.program_id
                        && binding.objective_id == projection.objective_id
                        && binding.session_id == projection.session_id
                        && binding.turn_id == projection.turn_id
                        && Some(&binding.root_execution_id) == projection.root_execution_id.as_ref()
                }) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_goal_binding_mismatch",
                        "Task references must belong to this Program's immutable Goal binding",
                    ));
                }
                if let Some(missing) = input.obligation_refs.iter().find(|reference| {
                    !goal
                        .goal
                        .criteria
                        .iter()
                        .any(|criterion| &criterion.id == *reference)
                        && !goal
                            .goal
                            .obligations
                            .iter()
                            .any(|obligation| &obligation.obligation_id == *reference)
                }) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_not_found",
                        &format!(
                            "Goal condition {missing} does not exist; inspect the current Goal"
                        ),
                    ));
                }
                obligation_source = Some(ExpectedStreamRevision {
                    stream_id: format!("goal:{}", goal.goal.id),
                    expected_revision: goal.stream_revision,
                });
            }
        }
        if let AgentAction::ArtifactCommit(input) = &envelope.action {
            if let Some(artifacts) = &self.artifacts {
                if let Err(error) = artifacts.resolve(&input.content_ref) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "artifact_content_not_durable",
                        &format!("content_ref must resolve in Runtime ArtifactStore: {error}"),
                    ));
                }
            }
        }
        if let Some(artifacts) = &self.artifacts {
            let evidence_refs: &[String] = match &envelope.action {
                AgentAction::TaskSubmit(input) => &input.evidence_refs,
                AgentAction::TaskReview(input) => &input.evidence_refs,
                AgentAction::TaskSupersede(input) => &input.evidence_refs,
                AgentAction::TaskWithdraw(input) => &input.evidence_refs,
                AgentAction::ObjectiveReview(input) => &input.evidence_refs,
                AgentAction::ObjectiveCompleteRequest(input) => &input.evidence_refs,
                _ => &[],
            };
            // Opaque artifact selectors can be verified locally. Stable
            // logical tool:// evidence is resolved and authenticated against
            // the Session journal by the async Gateway ingress before this
            // synchronous Program transition is applied.
            if let Some(reference) = evidence_refs.iter().find(|reference| {
                reference.starts_with("artifact://") && artifacts.resolve(reference).is_err()
            }) {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "evidence_not_durable",
                    &format!(
                        "evidence reference does not resolve in Runtime ArtifactStore: {reference}"
                    ),
                ));
            }
        }

        let entity_ref = entity_ref(envelope).or_else(|| {
            goal_event.as_ref().and_then(|(_, _, event)| {
                event
                    .event
                    .payload
                    .get("action_entity_ref")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
        });
        let mut events = Vec::with_capacity(if projection.revision == 0 { 2 } else { 1 });
        if projection.revision == 0 {
            events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Program,
                    kind: PROGRAM_OPENED_EVENT_KIND.to_string(),
                    status: Some("open".to_string()),
                    actor: Some(envelope.actor.actor_id.clone()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "objective".to_string(),
                            id: envelope.actor.objective_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "session".to_string(),
                            id: envelope.actor.session_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: envelope.actor.turn_id.clone(),
                        },
                    ]
                    .into_iter()
                    .chain(
                        envelope
                            .actor
                            .root_execution_id
                            .iter()
                            .map(|id| RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: id.clone(),
                            }),
                    )
                    .collect(),
                    payload: json!({
                        "program_id": envelope.actor.program_id,
                        "objective_id": envelope.actor.objective_id,
                        "session_id": envelope.actor.session_id,
                        "turn_id": envelope.actor.turn_id,
                        "root_execution_id": envelope.actor.root_execution_id,
                        "required_team_count": envelope.actor.required_team_count,
                        "objective_summary": envelope.actor.objective_summary,
                        "model_lease": envelope.actor.model_lease,
                        "permission_ceiling": envelope.actor.permission_ceiling,
                        "resource_scopes": envelope.actor.resource_scopes,
                    }),
                },
                idempotency_key: Some("agentic-program-opened".to_string()),
                schema_version: 1,
            });
        }
        events.push(RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.to_string(),
                scope: RuntimeEventScope::Program,
                kind: ACTION_EVENT_KIND.to_string(),
                status: Some("applied".to_string()),
                actor: Some(envelope.actor.actor_id.clone()),
                refs: action_refs(envelope, entity_ref.as_deref()),
                payload: json!({
                    "envelope": envelope,
                    "entity_ref": entity_ref,
                    "review_verification": verification,
                }),
            },
            idempotency_key: Some(action_key(&envelope.action_id)),
            schema_version: 1,
        });
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: stream_id.to_string(),
            expected_revision: projection.revision,
        }];
        expected_streams.extend(policy_sources);
        expected_streams.extend(
            verification
                .into_iter()
                .flat_map(|proof| &proof.result_source_revisions)
                .map(|(stream_id, revision)| ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: *revision,
                }),
        );
        if let Some(source) = obligation_source {
            expected_streams.push(source);
        }
        if let Some((goal_stream_id, expected_goal_stream_revision, goal_event)) = goal_event {
            expected_streams.push(ExpectedStreamRevision {
                stream_id: goal_stream_id,
                expected_revision: expected_goal_stream_revision,
            });
            events.push(goal_event);
        }
        self.store
            .append_transaction_locked(AppendTransactionRequest {
                transaction_id: format!(
                    "agent-action:{}:{}",
                    envelope.actor.program_id, envelope.action_id
                ),
                expected_streams,
                events,
            })?;
        projection = self.project_snapshot(&envelope.actor.program_id)?;
        applied_observation(envelope, &projection, entity_ref, false)
    }

    pub fn project(
        &self,
        program_id: &str,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        // Callers that need an independently mutable projection explicitly own a copy.
        // Bounded read consumers use the shared immutable snapshot below.
        self.project_snapshot(program_id)
            .map(|snapshot| (*snapshot).clone())
    }

    /// Immutable causal view for read consumers. Unlike `project`, this does
    /// not copy the complete Program on an unchanged durable-head cache hit.
    pub fn project_snapshot(
        &self,
        program_id: &str,
    ) -> Result<Arc<AgenticProgramProjection>, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
        let durable_revision = self.store.stream_revision(&stream_id)?;
        if let Some(mut cached) = self.read_model.get(program_id) {
            if cached.revision == durable_revision {
                return Ok(cached);
            }
            if cached.revision < durable_revision {
                let delta = self
                    .store
                    .list_stream_after(
                        &stream_id,
                        cached.revision,
                        durable_revision,
                        10_000,
                        32 * 1024 * 1024,
                    )
                    .map_err(AgentActionServiceError::EventStore)?;
                if delta.last().map(|event| event.sequence) == Some(durable_revision) {
                    apply_projection_events(Arc::make_mut(&mut cached), delta)?;
                    self.remember_projection(&cached);
                    return Ok(cached);
                }
            }
        }
        if let Some(mut persisted) = self.persisted_projection(program_id, durable_revision)? {
            if persisted.revision < durable_revision {
                let delta = self
                    .store
                    .list_stream_after(
                        &stream_id,
                        persisted.revision,
                        durable_revision,
                        10_000,
                        32 * 1024 * 1024,
                    )
                    .map_err(AgentActionServiceError::EventStore)?;
                if delta.last().map(|event| event.sequence) == Some(durable_revision) {
                    apply_projection_events(&mut persisted, delta)?;
                    let persisted = Arc::new(persisted);
                    self.remember_projection(&persisted);
                    return Ok(persisted);
                }
            } else {
                let persisted = Arc::new(persisted);
                self.read_model.put(Arc::clone(&persisted));
                return Ok(persisted);
            }
        }
        let events = self
            .store
            .list_stream(&stream_id)
            .map_err(AgentActionServiceError::EventStore)?;
        let opened = events
            .iter()
            .find(|event| event.kind == PROGRAM_OPENED_EVENT_KIND)
            .ok_or_else(|| AgentActionServiceError::Corrupt("program_not_found".to_string()))?;
        let objective_id = opened
            .payload
            .get("objective_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AgentActionServiceError::Corrupt("program_opened_missing_objective".to_string())
            })?;
        let mut projection = AgenticProgramProjection::empty(program_id, objective_id);
        projection.session_id = opened
            .payload
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        projection.turn_id = opened
            .payload
            .get("turn_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        projection.root_execution_id = opened
            .payload
            .get("root_execution_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        projection.required_team_count = opened
            .payload
            .get("required_team_count")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .unwrap_or_default();
        projection.objective_summary = opened
            .payload
            .get("objective_summary")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        projection.model_lease = opened
            .payload
            .get("model_lease")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default")
            .to_string();
        projection.permission_ceiling = opened
            .payload
            .get("permission_ceiling")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or(harness_contract::policy::PermissionMode::ReadOnly);
        projection.resource_scopes = opened
            .payload
            .get("resource_scopes")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default();
        apply_projection_events(&mut projection, events)?;
        let projection = Arc::new(projection);
        self.remember_projection(&projection);
        Ok(projection)
    }

    fn persisted_projection(
        &self,
        program_id: &str,
        durable_revision: u64,
    ) -> Result<Option<AgenticProgramProjection>, AgentActionServiceError> {
        let Some(checkpoint) = self
            .store
            .projection_checkpoint(&read_model_projection_id(program_id))?
        else {
            return Ok(None);
        };
        if checkpoint.source_cursor > durable_revision {
            tracing::warn!(
                program_id,
                checkpoint_revision = checkpoint.source_cursor,
                durable_revision,
                "discarding Agentic read snapshot ahead of its durable Program"
            );
            return Ok(None);
        }
        Ok(decode_read_snapshot(&checkpoint, program_id))
    }

    fn remember_projection(&self, projection: &Arc<AgenticProgramProjection>) {
        self.read_model.put(Arc::clone(projection));
        let projection_id = read_model_projection_id(&projection.program_id);
        let checkpoint = self
            .store
            .projection_checkpoint(&projection_id)
            .ok()
            .flatten();
        if checkpoint.as_ref().is_some_and(|checkpoint| {
            checkpoint.source_cursor >= projection.revision
                && self
                    .store
                    .stream_revision(&program_stream(&projection.program_id))
                    .is_ok_and(|head| checkpoint.source_cursor <= head)
                && decode_read_snapshot(checkpoint, &projection.program_id).is_some()
        }) {
            return;
        }
        let payload = match serde_json::to_value(projection.as_ref()) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(program_id = %projection.program_id, %error, "Agentic read snapshot serialization failed");
                return;
            }
        };
        let digest = format!(
            "sha256:{:x}",
            Sha256::digest(payload.to_string().as_bytes())
        );
        let payload = json!({"schema_version":1,"projection":payload,"sha256":digest});
        let repair_ahead = checkpoint.as_ref().is_some_and(|checkpoint| {
            checkpoint.source_cursor > projection.revision
                && self
                    .store
                    .stream_revision(&program_stream(&projection.program_id))
                    .is_ok_and(|head| checkpoint.source_cursor > head)
        });
        let write = if repair_ahead {
            RuntimeEventStore::compare_and_repair_projection_checkpoint
        } else {
            RuntimeEventStore::compare_and_put_projection_checkpoint
        };
        if let Err(error) = write(
            &self.store,
            &projection_id,
            projection.revision,
            checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.revision),
            &payload,
            now_ms(),
        ) {
            tracing::warn!(program_id = %projection.program_id, %error, "Agentic read snapshot persistence failed");
        }
    }

    /// Return the bounded, durable Program/own-Team topic delta for one
    /// physical Agent execution. Program revisions order both topics, while a
    /// separate execution cursor makes delivery restart-safe without mutating
    /// Program truth or repeatedly injecting the full projection.
    pub(crate) fn topic_observations(
        &self,
        program_id: &str,
        agent_id: &str,
        team_id: &str,
        execution_id: &str,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Option<AgenticTopicObservationPage>, AgentActionServiceError> {
        if program_id.trim().is_empty()
            || agent_id.trim().is_empty()
            || execution_id.trim().is_empty()
            || max_entries == 0
            || max_bytes == 0
        {
            return Ok(None);
        }
        let projection = self.project_snapshot(program_id)?;
        let _member = projection.agents.get(agent_id).ok_or_else(|| {
            AgentActionServiceError::Corrupt(format!(
                "topic_observer_not_in_program_roster:{agent_id}"
            ))
        })?;
        let team_topics = readable_topic_refs(&projection, agent_id, team_id);
        let cursor_stream = topic_cursor_stream(program_id, execution_id);
        let (from_revision, cursor_revision) = self.topic_cursor(&cursor_stream)?;
        let mut candidates = projection
            .topics
            .iter()
            .filter(|(topic_ref, _)| team_topics.contains(topic_ref.as_str()))
            .flat_map(|(topic_ref, entries)| {
                let start = entries.partition_point(|entry| entry.revision <= from_revision);
                entries[start..]
                    .iter()
                    .filter(|entry| {
                        entry.actor_id != agent_id && topic_entry_visible(entry, agent_id)
                    })
                    .take(max_entries)
                    .map(move |entry| (topic_ref, entry))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.1
                .revision
                .cmp(&right.1.revision)
                .then_with(|| left.1.entry_id.cmp(&right.1.entry_id))
        });

        let mut entries = Vec::new();
        let mut observed_bytes = 0usize;
        for (topic_ref, source) in candidates {
            if entries.len() >= max_entries {
                break;
            }
            let mut entry = AgenticTopicObservation {
                topic_ref: topic_ref.clone(),
                entry: source.clone(),
            };
            let mut entry_bytes = serde_json::to_vec(&entry)?.len();
            if !entries.is_empty()
                && (entries.len() >= max_entries
                    || observed_bytes.saturating_add(entry_bytes) > max_bytes)
            {
                break;
            }
            if entry_bytes > max_bytes {
                // Never inject an unbounded model payload. Preserve routing,
                // identity and revision, but require an explicit scoped read
                // for a producer that violated the compact-message contract.
                entry.entry.summary = Some(
                    "Message metadata exceeded the observation page; inspect this topic/entry explicitly"
                        .to_string(),
                );
                entry.entry.content_ref = None;
                entry.entry.refs.clear();
                entry_bytes = serde_json::to_vec(&entry)?.len();
                if entry_bytes > max_bytes {
                    return Err(AgentActionServiceError::Corrupt(
                        "topic_observation_identity_exceeds_page_budget".to_string(),
                    ));
                }
            }
            observed_bytes = observed_bytes.saturating_add(entry_bytes);
            entries.push(entry);
        }
        let Some(to_revision) = entries.last().map(|entry| entry.entry.revision) else {
            return Ok(None);
        };
        Ok(Some(AgenticTopicObservationPage {
            entries,
            from_revision,
            to_revision,
            cursor_revision,
        }))
    }

    /// Monotonic acknowledgement for a topic page. The cursor is a projection
    /// offset owned by Runtime, never a second writer of Program/Topic state.
    pub(crate) fn acknowledge_topic_observations(
        &self,
        request: AgenticTopicObservationAck,
    ) -> Result<(), AgentActionServiceError> {
        let stream_id = topic_cursor_stream(&request.program_id, &request.execution_id);
        self.store.with_stream_lock(&stream_id, || {
            let (through_revision, cursor_revision) = self.topic_cursor(&stream_id)?;
            if request.through_revision <= through_revision {
                return Ok(());
            }
            if cursor_revision != request.expected_cursor_revision {
                return Err(AgentActionServiceError::Store(
                    RuntimeEventStoreError::StaleRevision {
                        stream_id: stream_id.clone(),
                        expected: request.expected_cursor_revision,
                        actual: cursor_revision,
                    },
                ));
            }
            self.store
                .append_transaction_locked(AppendTransactionRequest {
                    transaction_id: format!(
                        "agentic-topic-ack:{}:{}:{}",
                        request.program_id, request.execution_id, request.through_revision
                    ),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: stream_id.clone(),
                        expected_revision: cursor_revision,
                    }],
                    events: vec![RuntimeTransactionEventInput {
                        event: RuntimeEventInput {
                            stream_id: stream_id.clone(),
                            scope: RuntimeEventScope::Agent,
                            kind: TOPIC_CURSOR_EVENT_KIND.to_string(),
                            status: Some("acknowledged".to_string()),
                            actor: Some("runtime.agent_topic_observer".to_string()),
                            refs: vec![RuntimeEventRef {
                                kind: "program".to_string(),
                                id: request.program_id.clone(),
                            }],
                            payload: json!({
                                "program_id": request.program_id,
                                "execution_id": request.execution_id,
                                "through_revision": request.through_revision,
                                "observation_kind": request.observation_kind,
                            }),
                        },
                        idempotency_key: Some(format!(
                            "agentic-topic-ack:{}",
                            request.through_revision
                        )),
                        schema_version: 1,
                    }],
                })?;
            Ok(())
        })
    }

    fn topic_cursor(&self, stream_id: &str) -> Result<(u64, u64), AgentActionServiceError> {
        let events = self
            .store
            .list_stream(stream_id)
            .map_err(AgentActionServiceError::EventStore)?;
        let cursor_revision = events.last().map_or(0, |event| event.sequence);
        let through_revision = events
            .iter()
            .rev()
            .find(|event| event.kind == TOPIC_CURSOR_EVENT_KIND)
            .and_then(|event| event.payload.get("through_revision"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        Ok((through_revision, cursor_revision))
    }

    /// Commit the Goal decision and the exact Program request disposition in
    /// one transaction. Neither domain can become terminal on its own here.
    pub(crate) fn commit_program_conclusion(
        &self,
        projection: &AgenticProgramProjection,
        prepared: crate::execution_core::goal::PreparedProgramConclusion,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        let request = projection
            .completion_request
            .as_ref()
            .ok_or_else(|| AgentActionServiceError::Corrupt("missing completion request".into()))?;
        let program_stream_id = program_stream(&projection.program_id);
        let goal_stream_id = prepared.event.event.stream_id.clone();
        let (kind, status, payload) = if let Some(terminal) = prepared.goal.terminal.as_ref() {
            let verdict = super::program::AgenticObjectiveVerdictProjection {
                goal_id: prepared.goal.id.clone(),
                goal_revision: prepared.goal.revision,
                terminal_fence: terminal.terminal_fence.clone(),
                authority_revision: terminal.authority_revision,
                kind: terminal.kind,
            };
            (
                OBJECTIVE_VERDICT_EVENT_KIND,
                "verified",
                json!({"program_id": projection.program_id, "verdict": verdict}),
            )
        } else {
            (
                COMPLETION_REOPENED_EVENT_KIND,
                "open",
                json!({
                    "program_id": projection.program_id,
                    "request_revision": request.program_revision,
                    "gaps": prepared.gaps,
                }),
            )
        };
        let mut expected_streams = prepared.policy_sources;
        expected_streams.extend([
            ExpectedStreamRevision {
                stream_id: program_stream_id.clone(),
                expected_revision: projection.revision,
            },
            ExpectedStreamRevision {
                stream_id: goal_stream_id,
                expected_revision: prepared.expected_stream_revision,
            },
        ]);
        self.store.append_transaction(AppendTransactionRequest {
            transaction_id: format!(
                "program-conclusion:{}:{}",
                projection.program_id, request.program_revision
            ),
            expected_streams,
            events: vec![
                prepared.event,
                RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: program_stream_id,
                        scope: RuntimeEventScope::Program,
                        kind: kind.into(),
                        status: Some(status.into()),
                        actor: Some("runtime.objective_supervisor".into()),
                        refs: vec![RuntimeEventRef {
                            kind: "goal".into(),
                            id: prepared.goal.id,
                        }],
                        payload,
                    },
                    idempotency_key: Some(format!(
                        "program-conclusion:{}",
                        request.program_revision
                    )),
                    schema_version: 1,
                },
            ],
        })?;
        self.project(&projection.program_id)
    }

    /// Bind the only authoritative Objective verdict back into Program truth.
    /// The caller cannot provide a boolean: this method validates the complete
    /// GoalStore terminal, its root identity and the exact request revision.
    pub(crate) fn bind_objective_verdict(
        &self,
        program_id: &str,
        goal: &harness_contract::goal::GoalContract,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        let projection = self.project(program_id)?;
        let Some(request) = projection.completion_request.as_ref() else {
            return Err(AgentActionServiceError::Corrupt(
                "objective_verdict_without_completion_request".to_string(),
            ));
        };
        let root_execution_id = projection.root_execution_id.as_deref().ok_or_else(|| {
            AgentActionServiceError::Corrupt("objective_verdict_without_root_execution".to_string())
        })?;
        let expected_goal_id = format!("goal:{root_execution_id}");
        let terminal = goal.terminal.as_ref().ok_or_else(|| {
            AgentActionServiceError::Corrupt("objective_verdict_is_not_terminal".to_string())
        })?;
        let terminal_matches_completion = matches!(
            (terminal.kind, goal.completion),
            (
                harness_contract::goal::ObjectiveTerminalKind::Satisfied,
                harness_contract::goal::GoalCompletion::Satisfied
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied,
                harness_contract::goal::GoalCompletion::Partial
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::Blocked,
                harness_contract::goal::GoalCompletion::Blocked
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::Failed,
                harness_contract::goal::GoalCompletion::Failed
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::Cancelled,
                harness_contract::goal::GoalCompletion::Cancelled
            )
        );
        let expected_fence = format!(
            "agentic-objective:{program_id}:request:{}",
            request.program_revision
        );
        if goal.id != expected_goal_id
            || goal.session_id != projection.session_id
            || goal.execution_binding.as_ref().is_none_or(|binding| {
                binding.objective_id != projection.objective_id
                    || binding.session_id != projection.session_id
                    || binding.turn_id != projection.turn_id
                    || binding.root_execution_id != root_execution_id
                    || binding.agentic_program_id != program_id
            })
            || terminal.terminal_fence != expected_fence
            || terminal.authority_revision != request.program_revision
            || !terminal_matches_completion
            || request
                .evidence_refs
                .iter()
                .any(|reference| !goal.evidence_refs.contains(reference))
            || !goal
                .evidence_refs
                .contains(&format!("execution_graph:{root_execution_id}"))
        {
            return Err(AgentActionServiceError::Corrupt(
                "objective_verdict_binding_mismatch".to_string(),
            ));
        }
        if let Some(existing) = projection.objective_verdict.as_ref() {
            if existing.terminal_fence == terminal.terminal_fence
                && existing.goal_revision == goal.revision
                && existing.kind == terminal.kind
            {
                return Ok(projection);
            }
            return Err(AgentActionServiceError::Corrupt(
                "objective_verdict_conflicts_with_bound_terminal".to_string(),
            ));
        }
        let verdict = super::program::AgenticObjectiveVerdictProjection {
            goal_id: goal.id.clone(),
            goal_revision: goal.revision,
            terminal_fence: terminal.terminal_fence.clone(),
            authority_revision: terminal.authority_revision,
            kind: terminal.kind,
        };
        let stream_id = program_stream(program_id);
        self.store.append_transaction(AppendTransactionRequest {
            transaction_id: format!("agentic-objective-verdict:{program_id}:{}", goal.revision),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: projection.revision,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Program,
                    kind: OBJECTIVE_VERDICT_EVENT_KIND.to_string(),
                    status: Some(
                        match terminal.kind {
                            harness_contract::goal::ObjectiveTerminalKind::Satisfied => "verified",
                            _ => "blocked",
                        }
                        .to_string(),
                    ),
                    actor: Some("runtime.objective_supervisor".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "goal".to_string(),
                            id: goal.id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "terminal_fence".to_string(),
                            id: terminal.terminal_fence.clone(),
                        },
                    ],
                    payload: json!({
                        "program_id": program_id,
                        "verdict": verdict,
                    }),
                },
                idempotency_key: Some(format!(
                    "agentic-objective-verdict:{}",
                    terminal.terminal_fence
                )),
                schema_version: 1,
            }],
        })?;
        self.project(program_id)
    }

    /// Resolve the already-frozen Program authority without treating a
    /// not-yet-opened Program as corruption. Root model continuations use
    /// this to inherit the first admitted binding instead of rebuilding
    /// policy and evidence scope from a later replan node.
    pub fn project_if_exists(
        &self,
        program_id: &str,
    ) -> Result<Option<AgenticProgramProjection>, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
        if self.store.stream_revision(&stream_id)? == 0 {
            return Ok(None);
        }
        self.project(program_id).map(Some)
    }

    /// Enumerate every durable Agentic Program from the authoritative event
    /// store. This is the only supported cross-program carrier/projection
    /// source; callers must not reconstruct Team state from graph payloads.
    pub fn list_programs(&self) -> Result<Vec<AgenticProgramProjection>, AgentActionServiceError> {
        let mut programs = self
            .store
            .stream_ids_for_scope(RuntimeEventScope::Program)?
            .into_iter()
            .filter_map(|stream| stream.strip_prefix("agentic-program:").map(str::to_string))
            .filter_map(|program_id| self.project_if_exists(&program_id).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        programs.sort_by(|left, right| left.program_id.cmp(&right.program_id));
        Ok(programs)
    }

    /// Compact model-facing working set. Mutations return a summary rather
    /// than re-injecting the entire growing Program on every action;
    /// `state_inspect` can request one concrete Team/Agent/Task/Topic/Artifact
    /// or the whole projection when the model genuinely needs it.
    #[must_use]
    pub fn compact_projection(
        projection: &AgenticProgramProjection,
        scope_ref: Option<&str>,
    ) -> serde_json::Value {
        if let Some(scope_ref) = scope_ref {
            if let Some(team) = projection.teams.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "team": team,
                    "agents": team.member_ids.iter().take(STATE_INSPECT_PAGE_SIZE).filter_map(|id| projection.agents.get(id)).collect::<Vec<_>>(),
                    "tasks": team.task_ids.iter().take(STATE_INSPECT_PAGE_SIZE).filter_map(|id| projection.tasks.get(id)).collect::<Vec<_>>(),
                    "topics": projection.topics.get(&team.topic_ref).map(|entries| entries.iter().rev().take(STATE_INSPECT_PAGE_SIZE).collect::<Vec<_>>()),
                    "directory_request":{"query":team.team_id},
                    "coverage":{"nested_lists":"bounded_previews","complete":false,"instruction":"use directory_request for all related entries"},
                });
            }
            if let Some(agent) = projection.agents.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "agent": agent,
                });
            }
            if let Some(task) = projection.tasks.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "task": task,
                    "issues": super::issues::issues(projection).into_iter().filter(|issue| issue.source_ref == task.task_id).collect::<Vec<_>>(),
                    "artifacts": task.artifact_refs.iter().filter_map(|id| projection.artifacts.get(id)).collect::<Vec<_>>(),
                });
            }
            if let Some(artifact) = projection.artifacts.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "artifact": artifact,
                    "read_request": { "evidence_ref": artifact.content_ref },
                });
            }
            if let Some(entries) = projection.topics.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "topic_ref": scope_ref,
                    "entries": entries.iter().rev().take(STATE_INSPECT_PAGE_SIZE).collect::<Vec<_>>(),
                    "directory_request":{"query":scope_ref},
                    "coverage":{"loaded":entries.len().min(STATE_INSPECT_PAGE_SIZE),"total":entries.len(),"complete":entries.len()<=STATE_INSPECT_PAGE_SIZE},
                });
            }
        }
        serde_json::to_value(projection).unwrap_or_else(
            |_| json!({"program_id": projection.program_id, "revision": projection.revision}),
        )
    }
}

fn apply_projection_events(
    projection: &mut AgenticProgramProjection,
    events: Vec<crate::DurableRuntimeEvent>,
) -> Result<(), AgentActionServiceError> {
    for event in events {
        if event.sequence <= projection.revision {
            continue;
        }
        if event.kind == PROGRAM_OPENED_EVENT_KIND {
            if let Some(seed) = event.payload.get("continuation_seed") {
                let inherited: AgenticProgramProjection = serde_json::from_value(seed.clone())?;
                if inherited.program_id != projection.program_id
                    || inherited.objective_id != projection.objective_id
                    || inherited.continuation.is_none()
                    || event.sequence != 1
                {
                    return Err(AgentActionServiceError::Corrupt(
                        "invalid continuation seed".into(),
                    ));
                }
                *projection = inherited;
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == OBJECTIVE_VERDICT_EVENT_KIND {
            let verdict =
                serde_json::from_value::<super::program::AgenticObjectiveVerdictProjection>(
                    event.payload.get("verdict").cloned().ok_or_else(|| {
                        AgentActionServiceError::Corrupt(
                            "objective_verdict_event_missing_verdict".to_string(),
                        )
                    })?,
                )?;
            projection.apply_objective_verdict(verdict, event.sequence);
            continue;
        }
        if event.kind == COMPLETION_REOPENED_EVENT_KIND {
            let request_revision = event
                .payload
                .get("request_revision")
                .and_then(serde_json::Value::as_u64);
            if projection
                .completion_request
                .as_ref()
                .map(|request| request.program_revision)
                != request_revision
            {
                return Err(AgentActionServiceError::Corrupt(
                    "completion reopen request fence mismatch".into(),
                ));
            }
            projection.status = super::program::AgenticProgramStatus::Open;
            projection.completion_request = None;
            projection.final_artifact_ref = None;
            projection.unresolved = serde_json::from_value(
                event
                    .payload
                    .get("gaps")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )?;
            projection.revision = event.sequence;
            continue;
        }
        if event.kind != ACTION_EVENT_KIND {
            projection.revision = event.sequence;
            continue;
        }
        let envelope: AgentActionEnvelope =
            serde_json::from_value(event.payload.get("envelope").cloned().ok_or_else(|| {
                AgentActionServiceError::Corrupt("action_event_missing_envelope".to_string())
            })?)?;
        let entity_ref = event
            .payload
            .get("entity_ref")
            .and_then(serde_json::Value::as_str);
        projection.apply(&envelope, entity_ref, event.sequence, event.created_at_ms);
    }
    Ok(())
}

fn is_durable_evidence_ref(reference: &str) -> bool {
    let reference = reference.trim();
    ["tool://", "artifact://"]
        .iter()
        .any(|prefix| reference.starts_with(prefix) && reference.len() > prefix.len())
        && !reference.chars().any(char::is_whitespace)
}

fn actor_agent_id(envelope: &AgentActionEnvelope) -> Option<&str> {
    envelope
        .actor
        .agent_id
        .as_deref()
        .or(Some(envelope.actor.actor_id.as_str()))
}

fn action_refs(envelope: &AgentActionEnvelope, entity_ref: Option<&str>) -> Vec<RuntimeEventRef> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "objective".to_string(),
            id: envelope.actor.objective_id.clone(),
        },
        RuntimeEventRef {
            kind: "program".to_string(),
            id: envelope.actor.program_id.clone(),
        },
        RuntimeEventRef {
            kind: "actor".to_string(),
            id: envelope.actor.actor_id.clone(),
        },
        RuntimeEventRef {
            kind: "session".to_string(),
            id: envelope.actor.session_id.clone(),
        },
        RuntimeEventRef {
            kind: "turn".to_string(),
            id: envelope.actor.turn_id.clone(),
        },
    ];
    if let Some(root_execution_id) = &envelope.actor.root_execution_id {
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: root_execution_id.clone(),
        });
    }
    if let Some(entity_ref) = entity_ref {
        refs.push(RuntimeEventRef {
            kind: entity_ref.split(':').next().unwrap_or("entity").to_string(),
            id: entity_ref.to_string(),
        });
    }
    if let AgentAction::TaskSupersede(input) = &envelope.action {
        refs.extend(
            input
                .replacement_task_refs
                .iter()
                .map(|id| RuntimeEventRef {
                    kind: "replacement_task".to_string(),
                    id: id.clone(),
                }),
        );
        refs.extend(input.evidence_refs.iter().map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        }));
    }
    if let AgentAction::TaskWithdraw(input) = &envelope.action {
        refs.push(RuntimeEventRef {
            kind: "task".to_string(),
            id: input.task_ref.clone(),
        });
        refs.extend(input.evidence_refs.iter().map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        }));
    }
    if let AgentAction::TaskAttemptDispatch(input) = &envelope.action {
        refs.push(RuntimeEventRef {
            kind: "task".to_string(),
            id: input.task_ref.clone(),
        });
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: input.execution_id.clone(),
        });
    }
    refs
}

fn entity_ref(envelope: &AgentActionEnvelope) -> Option<String> {
    if let AgentAction::AgentInvite(input) = &envelope.action {
        if let Some(existing_agent_ref) = input.existing_agent_ref.as_ref() {
            return Some(existing_agent_ref.clone());
        }
    }
    let prefix = match envelope.action {
        AgentAction::TeamCreate(_) => "team",
        AgentAction::AgentInvite(_) => "agent",
        AgentAction::TaskPublish(_) => "task",
        AgentAction::TaskSupersede(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskWithdraw(ref input) => return Some(input.task_ref.clone()),
        AgentAction::MessagePublish(_) => "message",
        AgentAction::ArtifactCommit(_) => "artifact",
        _ => return None,
    };
    let digest = Sha256::digest(
        format!(
            "{}|{}|{}|{}",
            envelope.actor.program_id,
            envelope.actor.actor_id,
            envelope.action_id,
            envelope.action.kind()
        )
        .as_bytes(),
    );
    let encoded = format!("{digest:x}");
    Some(format!("{prefix}:{}", &encoded[..24]))
}

fn applied_observation(
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
    entity_ref: Option<String>,
    duplicate: bool,
) -> Result<AgentActionObservation, AgentActionServiceError> {
    let artifact_access = entity_ref
        .as_ref()
        .and_then(|reference| projection.artifacts.get(reference))
        .map(|artifact| {
            json!({
                "artifact_ref": artifact.artifact_ref,
                "content_ref": artifact.content_ref,
                "read_tool": "evidence_retrieve",
                "read_request": { "evidence_ref": artifact.content_ref },
                "submit_ref": artifact.artifact_ref,
            })
        });
    let changed_refs = entity_ref.into_iter().collect::<Vec<_>>();
    Ok(AgentActionObservation {
        receipt_id: format!(
            "receipt:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision: projection.revision,
        status: AgentActionStatus::Applied,
        duplicate,
        changed_refs,
        actionable: actionable(projection, duplicate),
        projection: Some(json!({
            "program_id": projection.program_id,
            "objective_id": projection.objective_id,
            "revision": projection.revision,
            "status": projection.status,
            "counts": {
                "teams": projection.teams.len(),
                "agents": projection.agents.len(),
                "tasks": projection.tasks.len(),
                "artifacts": projection.artifacts.len(),
            },
            "final_artifact_ref": projection.final_artifact_ref,
            "unresolved": projection.unresolved,
            "artifact_access": artifact_access,
        })),
        error: None,
    })
}

pub(super) fn readable_topic_refs(
    projection: &AgenticProgramProjection,
    agent_id: &str,
    team_id: &str,
) -> std::collections::BTreeSet<String> {
    std::iter::once(format!("topic:{}", projection.program_id))
        .chain(
            projection
                .teams
                .get(team_id)
                .filter(|_| projection.agent_is_active_in(agent_id, team_id))
                .map(|team| team.topic_ref.clone()),
        )
        .collect()
}
pub(super) fn topic_entry_visible(entry: &AgenticTopicEntryProjection, agent_id: &str) -> bool {
    entry.actor_id == agent_id
        || entry.recipients.is_empty()
        || entry
            .recipients
            .iter()
            .any(|recipient| recipient == agent_id)
}

fn inspected_observation(
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
    goal: Option<&harness_contract::goal::GoalContract>,
) -> AgentActionObservation {
    let input = match &envelope.action {
        AgentAction::StateInspect(input) => input,
        _ => unreachable!("only state_inspect reaches inspected_observation"),
    };
    let reader = format!(
        "{:?}:{}:{:?}:{:?}",
        envelope.actor.kind,
        envelope.actor.actor_id,
        envelope.actor.execution_id,
        envelope.actor.team_id
    );
    let topic_scope = if matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        let Some(agent_id) = envelope
            .actor
            .agent_id
            .as_deref()
            .filter(|id| projection.agents.contains_key(*id))
        else {
            return rejected(
                envelope,
                projection.revision,
                "reader_not_authorized",
                "reader is not in the current Program roster",
            );
        };
        let Some(team_id) = envelope.actor.team_id.as_deref() else {
            return rejected(
                envelope,
                projection.revision,
                "reader_not_authorized",
                "reader has no bound Team",
            );
        };
        Some((agent_id, readable_topic_refs(projection, agent_id, team_id)))
    } else {
        None
    };
    let value = if goal.is_none()
        && input.after_revision == Some(projection.revision)
        && input.entry_ref.is_none()
        && input.scope_ref.is_none()
        && input.page_cursor.is_none()
        && input.query.is_none()
    {
        json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "unchanged": true,
        })
    } else {
        match inspect_projection_page(projection, input, &reader, goal, topic_scope.as_ref()) {
            Ok(value) => value,
            Err(error) => {
                return rejected(
                    envelope,
                    projection.revision,
                    "invalid_state_cursor",
                    &error,
                )
            }
        }
    };
    AgentActionObservation {
        receipt_id: format!(
            "observation:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision: projection.revision,
        status: AgentActionStatus::Observed,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: actionable(projection, false),
        projection: Some(value),
        error: None,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StateInspectCursor {
    reader: String,
    version: u8,
    program_id: String,
    revision: u64,
    #[serde(default)]
    goal_revision: Option<u64>,
    query: Option<String>,
    after_ref: String,
}

const STATE_INSPECT_PAGE_SIZE: usize = 32;

fn goal_summary(goal: &harness_contract::goal::GoalContract) -> serde_json::Value {
    json!({"goal_id":goal.id,"revision":goal.revision,"spec_revision":goal.spec_revision,
        "spec_digest":goal.spec_digest,"source_intent_ref":goal.source_intent_ref,"completion":goal.completion,
        "participation_requirement":goal.participation_requirement,
        "criteria_count":goal.criteria.len(),"obligation_count":goal.obligations.len(),"review_count":goal.reviews.len(),
        "inspect_request":{"query":goal.id}})
}

fn goal_directory_entries(goal: &harness_contract::goal::GoalContract) -> Vec<serde_json::Value> {
    std::iter::once(json!({"entry_ref":goal.id,"goal_id":goal.id,"kind":"goal",
        "label":goal.objective.chars().take(480).collect::<String>()}))
        .chain(goal.criteria.iter().map(|criterion| json!({"entry_ref":criterion.id,"goal_id":goal.id,
            "kind":"criterion","label":criterion.statement.chars().take(480).collect::<String>(),
            "status":criterion.status,"statement_ref":criterion.statement_ref})))
        .chain(goal.obligations.iter().map(|obligation| json!({"entry_ref":obligation.obligation_id,"goal_id":goal.id,
            "kind":"obligation","label":obligation.success_predicate.chars().take(480).collect::<String>(),
            "state":obligation.state,"required":obligation.required,
            "review_request":{"criterion_ref":obligation.obligation_id}})))
        .chain(goal.reviews.iter().map(|review| json!({"entry_ref":review.review_id,"goal_id":goal.id,
            "kind":"objective_review","criterion_ref":review.criterion_ref,"decision":review.decision,
            "spec_revision":review.spec_revision}))).collect()
}

/// Return a bounded navigation page or one exact entity.  The output never
/// contains an unbounded Program dump: models use the returned stable refs to
/// retrieve only the Task, Team, topic, or artifact relevant to their next
/// decision.  The cursor is a deterministic index over a revision snapshot,
/// so it is safe to discard after any revision change.
fn inspect_projection_page(
    projection: &AgenticProgramProjection,
    input: &harness_contract::agent_action::StateInspectInput,
    reader: &str,
    goal: Option<&harness_contract::goal::GoalContract>,
    topic_scope: Option<&(&str, std::collections::BTreeSet<String>)>,
) -> Result<serde_json::Value, String> {
    let readable_topic = |topic: &str| topic_scope.is_none_or(|(_, topics)| topics.contains(topic));
    let readable_entry = |entry: &AgenticTopicEntryProjection| {
        topic_scope.is_none_or(|(agent, _)| topic_entry_visible(entry, agent))
    };
    let exact_ref = input.entry_ref.as_deref().or(input.scope_ref.as_deref());
    if let Some(reference) = exact_ref {
        if let Some(goal) = goal {
            let value = if reference == goal.id {
                // Exact entity reads return the full objective. Directory pages keep
                // their bounded labels, but a goal read must not hide the requirement
                // behind the old 12000/4000 char thresholds.
                Some({
                    let mut summary = goal_summary(goal);
                    if let Some(object) = summary.as_object_mut() {
                        object.insert(
                            "objective".to_string(),
                            serde_json::Value::String(goal.objective.clone()),
                        );
                    }
                    summary
                })
            } else if let Some(criterion) = goal.criteria.iter().find(|item| item.id == reference) {
                Some(json!({"criterion":criterion}))
            } else if let Some(obligation) = goal
                .obligations
                .iter()
                .find(|item| item.obligation_id == reference)
            {
                Some(json!({"obligation":obligation}))
            } else {
                goal.reviews
                    .iter()
                    .find(|item| item.review_id == reference)
                    .map(|review| json!({"review":review}))
            };
            if let Some(value) = value {
                return Ok(
                    json!({"program_id":projection.program_id,"revision":projection.revision,
                    "goal_id":goal.id,"goal_revision":goal.revision,"spec_revision":goal.spec_revision,
                    "spec_digest":goal.spec_digest,"goal_entry":value}),
                );
            }
        }
        if let Some(membership) = projection.memberships.get(reference) {
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,"membership":membership}),
            );
        }
        if let Some((topic_ref, entry)) = projection
            .topics
            .iter()
            .filter(|(topic, _)| readable_topic(topic))
            .find_map(|(topic_ref, entries)| {
                entries
                    .iter()
                    .find(|entry| entry.entry_id == reference && readable_entry(entry))
                    .map(|entry| (topic_ref, entry))
            })
        {
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,"topic_ref":topic_ref,"entry":entry,
                "read_request":entry.content_ref.as_ref().map(|content| json!({"evidence_ref":content}))}),
            );
        }
        if let Some(issue) = super::issues::issues(projection)
            .into_iter()
            .find(|issue| issue.issue_ref == reference)
        {
            return Ok(
                json!({"program_id": projection.program_id, "revision": projection.revision, "issue": issue}),
            );
        }
        if let Some(entries) = projection
            .topics
            .get(reference)
            .filter(|_| readable_topic(reference))
        {
            let total = entries.iter().filter(|entry| readable_entry(entry)).count();
            let page = entries
                .iter()
                .rev()
                .filter(|entry| readable_entry(entry))
                .take(STATE_INSPECT_PAGE_SIZE)
                .collect::<Vec<_>>();
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,
                "topic_ref":reference,"entries":page,"directory_request":{"query":reference},
                "coverage":{"loaded":page.len(),"total":total,"complete":total<=STATE_INSPECT_PAGE_SIZE}}),
            );
        }
        if projection.teams.contains_key(reference)
            || projection.agents.contains_key(reference)
            || projection.tasks.contains_key(reference)
            || projection.artifacts.contains_key(reference)
        {
            return Ok(AgentActionService::compact_projection(
                projection,
                Some(reference),
            ));
        }
        return Ok(json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "entry_ref": reference,
            "not_found": true,
        }));
    }
    let query = input
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::to_lowercase);
    let cursor = input
        .page_cursor
        .as_deref()
        .map(|cursor| {
            serde_json::from_str::<StateInspectCursor>(cursor).map_err(|_| {
                "invalid Program cursor; copy next_request from a fresh directory".to_string()
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.version != 1
            || cursor.reader != reader
            || cursor.program_id != projection.program_id
            || cursor.revision != projection.revision
            || cursor.goal_revision != goal.map(|goal| goal.revision)
            || cursor.query != query
    }) {
        return Err(
            "Program source, revision or query changed; restart directory discovery".into(),
        );
    }
    let directory = projection
        .teams
        .values()
        .map(|team| {
            json!({
                "entry_ref": team.team_id,
                "kind": "team",
                "label": team.name,
                "lifecycle": team.lifecycle,
            })
        })
        .chain(projection.agents.values().map(|agent| {
            json!({
                "entry_ref": agent.agent_id,
                "kind": "agent",
                "label": agent.role,
                "memberships": agent.membership_ids,
            })
        }))
        .chain(projection.tasks.values().map(|task| {
            json!({
                "entry_ref": task.task_id,
                "kind": "task",
                "label": task.title,
                "status": task.status,
                "team_ref": task.team_id,
                "active_attempt_count": task.active_attempts.len(),
            })
        }))
        .chain(projection.topics.keys().filter(|topic| readable_topic(topic)).map(|topic_ref| {
            json!({
                "entry_ref": topic_ref,
                "kind": "topic",
            })
        }))
        .chain(projection.memberships.values().map(|membership| json!({
            "entry_ref":membership.membership_id,"kind":"membership","agent_ref":membership.agent_id,"team_ref":membership.team_id,"lifecycle":membership.lifecycle
        })))
        .chain(projection.topics.iter().filter(|(topic, _)| readable_topic(topic)).flat_map(|(topic_ref,entries)| entries.iter().filter(|entry| readable_entry(entry)).map(move |entry| json!({
            "entry_ref":entry.entry_id,"kind":"topic_entry","topic_ref":topic_ref,"author":entry.actor_id,
            "label":entry.summary.as_deref().unwrap_or("").chars().take(480).collect::<String>(),"entry_revision":entry.revision
        }))))
        .chain(projection.artifacts.values().map(|artifact| {
            json!({
                "entry_ref": artifact.artifact_ref,
                "kind": "artifact",
                "label": artifact.title,
                "read_request": { "evidence_ref": artifact.content_ref },
            })
        }))
        .chain(super::issues::issues(projection).into_iter().map(|issue| json!({
            "entry_ref": issue.issue_ref, "kind": "issue", "label": issue.description,
            "source_ref": issue.source_ref, "disposition": issue.disposition,
        })))
        .chain(goal.into_iter().flat_map(goal_directory_entries));
    // Keep only the next page plus one look-ahead entry. Discovery can scan
    // metadata, but must not materialize the complete Program directory.
    let mut entries: Vec<serde_json::Value> = Vec::with_capacity(STATE_INSPECT_PAGE_SIZE + 1);
    let mut cursor_found = cursor.is_none();
    for entry in directory {
        if query
            .as_ref()
            .is_some_and(|query| !entry.to_string().to_lowercase().contains(query))
        {
            continue;
        }
        let reference = entry["entry_ref"].as_str().unwrap_or("");
        if let Some(cursor) = &cursor {
            cursor_found |= reference == cursor.after_ref;
            if reference <= cursor.after_ref.as_str() {
                continue;
            }
        }
        let position = entries.partition_point(|candidate| {
            candidate["entry_ref"].as_str().unwrap_or("") <= reference
        });
        if position <= STATE_INSPECT_PAGE_SIZE {
            entries.insert(position, entry);
            if entries.len() > STATE_INSPECT_PAGE_SIZE + 1 {
                entries.pop();
            }
        }
    }
    if !cursor_found {
        return Err("Program cursor position is not present in this directory".into());
    }
    let start: usize = 0;
    let end = start
        .saturating_add(STATE_INSPECT_PAGE_SIZE)
        .min(entries.len());
    let next_cursor = (end < entries.len())
        .then(|| {
            serde_json::to_string(&StateInspectCursor {
                version: 1,
                reader: reader.into(),
                program_id: projection.program_id.clone(),
                revision: projection.revision,
                goal_revision: goal.map(|goal| goal.revision),
                query: query.clone(),
                after_ref: entries[end - 1]["entry_ref"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    for entry in &mut entries[start..end] {
        entry["source_kind"] = json!("program");
        entry["revision"] = json!(projection.revision);
        entry["scope"] = json!(projection.program_id);
        entry["inspect_request"] = json!({"entry_ref":entry["entry_ref"]});
    }
    Ok(json!({
        "program_id": projection.program_id,
        "objective_id": projection.objective_id,
        "continuation": projection.continuation,
        "revision": projection.revision,
        "status": projection.status,
        "counts": {
            "teams": projection.teams.len(),
            "agents": projection.agents.len(),
            "memberships": projection.memberships.len(),
            "tasks": projection.tasks.len(),
            "artifacts": projection.artifacts.len(),
        },
        "goal": goal.map(goal_summary),
        "entries": entries[start..end].to_vec(),
        "next_page_cursor": next_cursor,
        "next_request": next_cursor.as_ref().map(|cursor| {
            let mut request = json!({"page_cursor":cursor});
            if let Some(query) = &input.query { request["query"] = json!(query); }
            request
        }),
        "coverage": {"kind":"program_metadata_directory", "complete":next_cursor.is_none(), "revision":projection.revision},
    }))
}

fn rejected(
    envelope: &AgentActionEnvelope,
    revision: u64,
    code: &str,
    message: &str,
) -> AgentActionObservation {
    AgentActionObservation {
        receipt_id: format!(
            "rejection:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision,
        status: AgentActionStatus::Rejected,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: vec!["inspect current state and revise only this action".to_string()],
        projection: None,
        error: Some(AgentActionErrorObservation {
            code: code.to_string(),
            message: message.to_string(),
            recoverable: true,
        }),
    }
}

fn actionable(projection: &AgenticProgramProjection, duplicate: bool) -> Vec<String> {
    let mut actions = Vec::new();
    if duplicate {
        actions.push("idempotent replay: no state was duplicated".to_string());
    }
    match projection.status {
        super::program::AgenticProgramStatus::CompletionRequested => {
            actions.push(
                "completion request is durable; await the ObjectiveSupervisor verdict".to_string(),
            );
            return actions;
        }
        super::program::AgenticProgramStatus::Verified
        | super::program::AgenticProgramStatus::Partial
        | super::program::AgenticProgramStatus::Blocked
        | super::program::AgenticProgramStatus::Failed
        | super::program::AgenticProgramStatus::Cancelled => return actions,
        super::program::AgenticProgramStatus::Waiting => {
            actions.push("await the recorded external dependency or permission change".to_string());
            return actions;
        }
        super::program::AgenticProgramStatus::Draining => {
            actions
                .push("await cancellation and effect verification before finalizing".to_string());
            return actions;
        }
        super::program::AgenticProgramStatus::Open => {}
    }
    if !projection.unresolved.is_empty() {
        actions.push(format!("The previous completion request was reopened. Resolve the recorded gaps before requesting completion again: {}", projection.unresolved.join("; ")));
        if projection
            .unresolved
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:"))
        {
            actions.push("Use objective_review for the original criterion with the current delivered result and durable evidence; the reviewer must be distinct from its Runtime-resolved producer. Preserve accepted work.".into());
        }
    }
    if projection.teams.is_empty() {
        actions.push("create a Team or complete directly with a committed artifact".to_string());
    }
    if projection
        .teams
        .values()
        .any(|team| team.member_ids.is_empty())
    {
        actions.push("invite an Agent into an unstaffed Team".to_string());
    }
    if projection.tasks.values().any(|task| {
        matches!(
            task.status,
            AgenticTaskStatus::Published | AgenticTaskStatus::Rework
        )
    }) {
        actions.push("claim an available Task".to_string());
    }
    if projection.tasks.values().any(|task| {
        task.status == AgenticTaskStatus::Published
            && (task.failed_attempts > 0 || task.failed_review_attempts > 0)
    }) {
        actions.push(
            "reassign the failed work, or publish concrete successor Task(s) and then call task_supersede with durable failure/replan evidence"
                .to_string(),
        );
    }
    if projection
        .tasks
        .values()
        .any(|task| task.status == AgenticTaskStatus::Submitted)
    {
        actions.push("review a submitted Task".to_string());
    }
    if !projection.artifacts.is_empty()
        && projection
            .tasks
            .values()
            .any(|task| task.status != AgenticTaskStatus::Superseded)
        && projection
            .tasks
            .values()
            .filter(|task| task.status != AgenticTaskStatus::Superseded)
            .all(|task| task.status == AgenticTaskStatus::Accepted)
    {
        actions.push("request Objective completion with the final artifact".to_string());
    }
    actions
}

fn program_stream(program_id: &str) -> String {
    format!("agentic-program:{program_id}")
}

fn topic_cursor_stream(program_id: &str, execution_id: &str) -> String {
    format!("agentic-topic-cursor:{program_id}:{execution_id}")
}

fn action_key(action_id: &str) -> String {
    format!("agent-action:{action_id}")
}

fn read_model_projection_id(program_id: &str) -> String {
    format!(
        "runtime:agentic-read-model:v1:{:x}",
        Sha256::digest(program_id.as_bytes())
    )
}

fn decode_read_snapshot(
    checkpoint: &crate::RuntimeProjectionCheckpoint,
    program_id: &str,
) -> Option<AgenticProgramProjection> {
    let payload = &checkpoint.payload;
    let body = &payload["projection"];
    let expected_digest = format!("sha256:{:x}", Sha256::digest(body.to_string().as_bytes()));
    if payload["schema_version"].as_u64() != Some(1)
        || payload["sha256"].as_str() != Some(expected_digest.as_str())
    {
        tracing::warn!(
            program_id,
            "discarding unversioned or corrupt Agentic read snapshot"
        );
        return None;
    }
    let Ok(projection) = serde_json::from_value::<AgenticProgramProjection>(body.clone()) else {
        tracing::warn!(program_id, "discarding malformed Agentic read snapshot");
        return None;
    };
    if projection.program_id != program_id || projection.revision != checkpoint.source_cursor {
        tracing::warn!(program_id, "discarding misbound Agentic read snapshot");
        return None;
    }
    Some(projection)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
