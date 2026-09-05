use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActionErrorObservation, AgentActionObservation,
    AgentActionStatus,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError, RuntimeTransactionEventInput,
};

use super::program::{AgenticProgramProjection, AgenticTaskStatus, AgenticTopicEntryProjection};

mod authorization;

use authorization::validate_transition;

const ACTION_EVENT_KIND: &str = "agentic.action_applied";
const PROGRAM_OPENED_EVENT_KIND: &str = "agentic.program_opened";
const OBJECTIVE_VERDICT_EVENT_KIND: &str = "agentic.objective_verdict_bound";
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationAck {
    pub program_id: String,
    pub execution_id: String,
    pub through_revision: u64,
    pub expected_cursor_revision: u64,
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
}

impl AgentActionService {
    #[must_use]
    pub fn new(store: Arc<RuntimeEventStore>) -> Self {
        Self {
            store,
            artifacts: None,
        }
    }

    /// Bind the physical artifact authority used by production Runtime
    /// services. Unit projections may omit it; an active Runtime never does.
    #[must_use]
    pub fn with_artifact_store(mut self, artifacts: Arc<crate::ArtifactStore>) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    pub fn apply(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let stream_id = program_stream(&envelope.actor.program_id);
        self.store
            .with_stream_lock(&stream_id, || self.apply_locked(envelope, &stream_id))
    }

    fn apply_locked(
        &self,
        envelope: &AgentActionEnvelope,
        stream_id: &str,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Some(existing) = self
            .store
            .event_by_idempotency_key(stream_id, &action_key(&envelope.action_id))?
        {
            let projection = self.project(&envelope.actor.program_id)?;
            let entity_ref = existing
                .payload
                .get("entity_ref")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            return applied_observation(envelope, &projection, entity_ref, true);
        }

        let mut projection = match self.project(&envelope.actor.program_id) {
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
                projection
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
            return Ok(inspected_observation(envelope, &projection));
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
        if let Some((code, message)) = validate_transition(&projection, envelope, now_ms()) {
            return Ok(rejected(envelope, projection.revision, code, &message));
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
                AgentAction::ObjectiveCompleteRequest(input) => &input.evidence_refs,
                _ => &[],
            };
            if let Some(reference) = evidence_refs.iter().find(|reference| {
                evidence_artifact_selector(reference)
                    .is_some_and(|selector| artifacts.resolve(&selector).is_err())
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

        let entity_ref = entity_ref(envelope);
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
                }),
            },
            idempotency_key: Some(action_key(&envelope.action_id)),
            schema_version: 1,
        });
        self.store
            .append_transaction_locked(AppendTransactionRequest {
                transaction_id: format!(
                    "agent-action:{}:{}",
                    envelope.actor.program_id, envelope.action_id
                ),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.to_string(),
                    expected_revision: projection.revision,
                }],
                events,
            })?;
        projection = self.project(&envelope.actor.program_id)?;
        applied_observation(envelope, &projection, entity_ref, false)
    }

    pub fn project(
        &self,
        program_id: &str,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
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
        for event in events {
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
            if event.kind != ACTION_EVENT_KIND {
                projection.revision = event.sequence;
                continue;
            }
            let envelope: AgentActionEnvelope = serde_json::from_value(
                event.payload.get("envelope").cloned().ok_or_else(|| {
                    AgentActionServiceError::Corrupt("action_event_missing_envelope".to_string())
                })?,
            )?;
            let entity_ref = event
                .payload
                .get("entity_ref")
                .and_then(serde_json::Value::as_str);
            projection.apply(&envelope, entity_ref, event.sequence, event.created_at_ms);
        }
        Ok(projection)
    }

    /// Return the bounded, durable Program/own-Team topic delta for one
    /// physical Agent execution. Program revisions order both topics, while a
    /// separate execution cursor makes delivery restart-safe without mutating
    /// Program truth or repeatedly injecting the full projection.
    pub(crate) fn topic_observations(
        &self,
        program_id: &str,
        agent_id: &str,
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
        let projection = self.project(program_id)?;
        let member = projection.agents.get(agent_id).ok_or_else(|| {
            AgentActionServiceError::Corrupt(format!(
                "topic_observer_not_in_program_roster:{agent_id}"
            ))
        })?;
        let team_topic = projection
            .teams
            .get(&member.team_id)
            .map(|team| team.topic_ref.as_str())
            .ok_or_else(|| {
                AgentActionServiceError::Corrupt(format!(
                    "topic_observer_team_not_found:{}",
                    member.team_id
                ))
            })?;
        let cursor_stream = topic_cursor_stream(program_id, execution_id);
        let (from_revision, cursor_revision) = self.topic_cursor(&cursor_stream)?;
        let program_topic = format!("topic:{program_id}");
        let mut candidates = projection
            .topics
            .iter()
            .filter(|(topic_ref, _)| {
                topic_ref.as_str() == program_topic.as_str() || topic_ref.as_str() == team_topic
            })
            .flat_map(|(topic_ref, entries)| {
                entries.iter().map(|entry| AgenticTopicObservation {
                    topic_ref: topic_ref.clone(),
                    entry: entry.clone(),
                })
            })
            .filter(|observation| {
                observation.entry.revision > from_revision && observation.entry.actor_id != agent_id
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.entry
                .revision
                .cmp(&right.entry.revision)
                .then_with(|| left.entry.entry_id.cmp(&right.entry.entry_id))
        });

        let mut entries = Vec::new();
        let mut observed_bytes = 0usize;
        for mut entry in candidates {
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
            || goal.program_ref.as_deref() != Some(root_execution_id)
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
        let events = self
            .store
            .list_stream(&stream_id)
            .map_err(AgentActionServiceError::EventStore)?;
        if events.is_empty() {
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
                    "agents": team.member_ids.iter().filter_map(|id| projection.agents.get(id)).collect::<Vec<_>>(),
                    "tasks": team.task_ids.iter().filter_map(|id| projection.tasks.get(id)).collect::<Vec<_>>(),
                    "topics": projection.topics.get(&team.topic_ref),
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
                    "artifacts": task.artifact_refs.iter().filter_map(|id| projection.artifacts.get(id)).collect::<Vec<_>>(),
                });
            }
            if let Some(artifact) = projection.artifacts.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "artifact": artifact,
                });
            }
            if let Some(entries) = projection.topics.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "topic_ref": scope_ref,
                    "entries": entries,
                });
            }
        }
        serde_json::to_value(projection).unwrap_or_else(
            |_| json!({"program_id": projection.program_id, "revision": projection.revision}),
        )
    }
}

fn is_durable_evidence_ref(reference: &str) -> bool {
    let reference = reference.trim();
    ["tool://", "artifact://"]
        .iter()
        .any(|prefix| reference.starts_with(prefix) && reference.len() > prefix.len())
        && !reference.chars().any(char::is_whitespace)
}

fn evidence_artifact_selector(reference: &str) -> Option<String> {
    reference
        .strip_prefix("tool://")
        .map(|id| format!("artifact://{id}"))
        .or_else(|| {
            reference
                .starts_with("artifact://")
                .then(|| reference.to_string())
        })
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
    refs
}

fn entity_ref(envelope: &AgentActionEnvelope) -> Option<String> {
    let prefix = match envelope.action {
        AgentAction::TeamCreate(_) => "team",
        AgentAction::AgentInvite(_) => "agent",
        AgentAction::TaskPublish(_) => "task",
        AgentAction::TaskSupersede(ref input) => return Some(input.task_ref.clone()),
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
        })),
        error: None,
    })
}

fn inspected_observation(
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
) -> AgentActionObservation {
    let (scope_ref, after_revision) = match &envelope.action {
        AgentAction::StateInspect(input) => (input.scope_ref.as_deref(), input.after_revision),
        _ => (None, None),
    };
    let value = if after_revision.is_some_and(|revision| revision == projection.revision) {
        json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "unchanged": true,
        })
    } else {
        AgentActionService::compact_projection(projection, scope_ref)
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
        | super::program::AgenticProgramStatus::Blocked => return actions,
        super::program::AgenticProgramStatus::Open => {}
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
