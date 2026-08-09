//! Canonical read and command model for live execution state.
//!
//! This module owns no durable state. It translates the existing graph, goal,
//! agent, team, relation, approval, context and V3 event stores into the one
//! public contract exposed by `harness-contract::projection`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use harness_contract::core::MeasureProvenance;
use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::projection::{
    AdmissionProjection, AdmissionProjectionStatus, ChildExecutionProjection, EvidenceProjection,
    ExecutionCommandKind, ExecutionCommandReceipt, ExecutionCommandRequest, ExecutionProjection,
    OutcomeProjection, OutcomeQualityProjection, ProjectionCommandAvailability, ProjectionDelta,
    ProjectionDetailScope, ProjectionEntity, ProjectionEntityCollection, ProjectionEntityPayload,
    ProjectionOperation, ProjectionQueryContext, ProjectionResyncReason, ProjectionSourceHealth,
    StrategyActualProjection, StrategyActualStatus, StrategyDecisionProjection,
    StrategyEvidenceScopeProjection, StrategyProofStatus, StrategyTransitionProjection,
    EXECUTION_PROJECTION_REDUCER_VERSION, EXECUTION_PROJECTION_SCHEMA_VERSION,
    STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION,
};
use harness_contract::reality::{EvidenceCompleteness, EvidenceRef, RealityBoundary};
use harness_contract::strategy::{
    ExecutionCandidateEstimate, ExecutionCandidateKind, StrategyDecisionSource,
    StrategyResourceSnapshot,
};
use harness_contract::team::FocusPartitionPlan;
use sha2::{Digest, Sha256};

use crate::execution_core::graph::{
    ResourceAdmissionObservation, ResourceAdmissionObservationStatus, ResourceWaitReason,
};
use crate::{ExecutionGraphHost, RuntimeEventScope, RuntimeServices, RuntimeServicesError};

mod activity;
mod delta;
mod reducer_support;
mod snapshot;

pub use delta::delta;
pub use snapshot::snapshot;

use reducer_support::*;
use snapshot::safe_public_ref;

const MAX_DELTA_BATCHES: usize = 256;
const DEFAULT_PROJECTION_CACHE_ENTRIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionProjectionCacheKey {
    execution_id: String,
    graph_revision: u64,
    source_cursor: u64,
    detail_scope: ProjectionDetailScope,
    authorization_revision: u64,
    redaction_revision: String,
}

impl ExecutionProjectionCacheKey {
    pub(crate) fn new(
        execution_id: &str,
        graph_revision: u64,
        source_cursor: u64,
        context: &ProjectionQueryContext,
    ) -> Self {
        Self {
            execution_id: execution_id.to_string(),
            graph_revision,
            source_cursor,
            detail_scope: context.detail_scope,
            authorization_revision: context.authorization_revision,
            redaction_revision: redaction_revision(context),
        }
    }

    fn same_projection_family(&self, other: &Self) -> bool {
        self.execution_id == other.execution_id
            && self.detail_scope == other.detail_scope
            && self.authorization_revision == other.authorization_revision
            && self.redaction_revision == other.redaction_revision
    }
}

#[derive(Debug)]
pub(crate) struct ExecutionProjectionCache {
    capacity: usize,
    entries: VecDeque<(ExecutionProjectionCacheKey, ExecutionProjection)>,
    hits: u64,
    misses: u64,
}

impl Default for ExecutionProjectionCache {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_PROJECTION_CACHE_ENTRIES,
            entries: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }
}

impl ExecutionProjectionCache {
    pub(crate) fn get(&mut self, key: &ExecutionProjectionCacheKey) -> Option<ExecutionProjection> {
        let Some(index) = self
            .entries
            .iter()
            .position(|(candidate, _)| candidate == key)
        else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        self.hits = self.hits.saturating_add(1);
        let entry = self.entries.remove(index)?;
        let projection = entry.1.clone();
        self.entries.push_back(entry);
        Some(projection)
    }

    pub(crate) fn put(
        &mut self,
        key: ExecutionProjectionCacheKey,
        projection: ExecutionProjection,
    ) {
        self.entries
            .retain(|(candidate, _)| !candidate.same_projection_family(&key));
        self.entries.push_back((key, projection));
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> (u64, u64, usize) {
        (self.hits, self.misses, self.entries.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionAuthorizationScope {
    pub session_id: Option<String>,
    pub mission_id: Option<String>,
    pub resource_grants: Vec<String>,
}

pub fn authorization_scope(
    services: &RuntimeServices,
    execution_id: &str,
) -> Result<ProjectionAuthorizationScope, RuntimeServicesError> {
    let graph = services.graph_state_store().projection(execution_id)?;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, false)?;
    let mut resource_grants = scope
        .execution_ids
        .iter()
        .filter_map(|execution_id| services.graph_state_store().load(execution_id).ok())
        .flat_map(|graph| graph.nodes.into_iter())
        .flat_map(|node| node.resource_scopes.into_iter())
        .filter_map(|scope| safe_public_ref(&scope))
        .collect::<Vec<_>>();
    resource_grants.sort();
    resource_grants.dedup();
    Ok(ProjectionAuthorizationScope {
        session_id: scope.session_id,
        mission_id: scope.mission_id,
        resource_grants,
    })
}

pub async fn activity_detail(
    services: &RuntimeServices,
    execution_id: &str,
    activity_id: &str,
    context: &ProjectionQueryContext,
) -> Result<harness_contract::projection::ExecutionActivityDetailProjection, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services.graph_state_store().projection(execution_id)?;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, true)?;
    validate_projection_scope(&scope, context)?;

    const PAGE_SIZE: usize = 256;
    let mut events = Vec::new();
    let mut after = None;
    loop {
        let page = services
            .event_store()
            .events_for_activity(activity_id, after, PAGE_SIZE)
            .map_err(RuntimeServicesError::Invariant)?;
        if page.is_empty() {
            break;
        }
        let next = page
            .last()
            .map(|event| (event.commit_cursor, event.transaction_index));
        let page_len = page.len();
        events.extend(page);
        if page_len < PAGE_SIZE || next == after {
            break;
        }
        after = next;
    }
    let (activities, activity_relations) = activity::project_execution_activities_from_events(
        services,
        &scope,
        &graph,
        events.clone(),
        true,
    );
    let activity = activities
        .iter()
        .find(|activity| activity.activity_id == activity_id)
        .cloned()
        .ok_or_else(|| {
            RuntimeServicesError::Invariant(format!(
                "execution activity `{activity_id}` was not found"
            ))
        })?;
    let relations = activity_relations
        .iter()
        .filter(|relation| {
            relation.from_activity_id == activity_id || relation.to_activity_id == activity_id
        })
        .cloned()
        .collect::<Vec<_>>();
    let input = activity_content_projection(
        &events,
        &[
            "input",
            "input_preview",
            "request",
            "objective",
            "payload_ref",
            "constraints",
            "depends_on",
            "input_refs",
        ],
        false,
        activity.public_summary.as_deref(),
    );
    let output = activity_content_projection(
        &events,
        &[
            "output",
            "output_preview",
            "result",
            "result_summary",
            "summary",
            "outcome",
            "returned",
            "error",
            "failure",
            "full_output_ref",
            "output_ref",
        ],
        true,
        activity.result_summary.as_deref(),
    );
    let refs = activity
        .evidence_refs
        .iter()
        .chain(activity.artifact_refs.iter())
        .chain(activity.definition_refs.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut related_entities = events
        .into_iter()
        .map(|event| snapshot::entity_from_runtime_event("evidence", event, true))
        .filter(|entity| {
            refs.is_empty()
                || refs.contains(&entity.id)
                || entity
                    .evidence_refs
                    .iter()
                    .any(|reference| refs.contains(reference))
        })
        .map(|entity| (entity.id.clone(), entity))
        .collect::<BTreeMap<_, _>>();
    for entity in scope
        .teams
        .iter()
        .chain(scope.agents.iter())
        .filter(|entity| activity_identity_matches_entity(&activity, entity))
        .cloned()
    {
        related_entities.insert(entity.id.clone(), entity);
    }
    if let Some(skill_id) = activity.skill_id.as_deref() {
        for profile in services
            .skill_catalog()
            .profiles()
            .into_iter()
            .filter(|profile| profile.skill_id == skill_id)
        {
            let entity = ProjectionEntity {
                id: format!("skill-profile:{}", profile.skill_id),
                kind: "skill_profile".to_string(),
                revision: 0,
                status: Some(format!("{:?}", profile.lifecycle_status).to_ascii_lowercase()),
                summary: Some(profile.name.clone()),
                evidence_refs: Vec::new(),
                payload: None,
                detail: Some(serde_json::to_value(profile).unwrap_or_default()),
            };
            related_entities.insert(entity.id.clone(), entity);
        }
    }
    for entity in activity_identity_entities(&activity) {
        related_entities.insert(entity.id.clone(), entity);
    }
    Ok(
        harness_contract::projection::ExecutionActivityDetailProjection {
            schema_version: harness_contract::projection::EXECUTION_ACTIVITY_SCHEMA_VERSION,
            execution_id: activity.scope.execution_id.clone(),
            activity,
            input,
            output,
            relations,
            related_entities: related_entities.into_values().collect(),
        },
    )
}

fn activity_content_projection(
    events: &[crate::DurableRuntimeEvent],
    keys: &[&str],
    reverse: bool,
    fallback_summary: Option<&str>,
) -> Option<harness_contract::projection::ExecutionActivityContentProjection> {
    let ordered = if reverse {
        Box::new(events.iter().rev()) as Box<dyn Iterator<Item = &crate::DurableRuntimeEvent>>
    } else {
        Box::new(events.iter()) as Box<dyn Iterator<Item = &crate::DurableRuntimeEvent>>
    };
    for event in ordered {
        let serde_json::Value::Object(payload) = &event.payload else {
            continue;
        };
        let selected = keys
            .iter()
            .filter_map(|key| {
                payload
                    .get(*key)
                    .map(|value| ((*key).to_string(), value.clone()))
            })
            .collect::<serde_json::Map<_, _>>();
        if selected.is_empty() {
            continue;
        }
        let source = if selected.len() == 1 {
            selected
                .into_iter()
                .next()
                .map(|(_, value)| value)
                .unwrap_or_default()
        } else {
            serde_json::Value::Object(selected)
        };
        let mut truncated = false;
        let structured = bounded_activity_value(&source, None, 0, &mut truncated);
        let summary = activity_value_summary(&structured)
            .or_else(|| fallback_summary.map(|summary| snapshot::safe_public_text(summary, 320)));
        let content_ref = activity_content_ref(&structured);
        return Some(
            harness_contract::projection::ExecutionActivityContentProjection {
                kind: if structured.is_string() {
                    "text".to_string()
                } else {
                    "structured".to_string()
                },
                summary,
                structured: Some(structured),
                content_ref,
                truncated,
            },
        );
    }
    fallback_summary.map(|summary| {
        harness_contract::projection::ExecutionActivityContentProjection {
            kind: "summary".to_string(),
            summary: Some(snapshot::safe_public_text(summary, 320)),
            structured: None,
            content_ref: None,
            truncated: false,
        }
    })
}

fn bounded_activity_value(
    value: &serde_json::Value,
    field: Option<&str>,
    depth: usize,
    truncated: &mut bool,
) -> serde_json::Value {
    if field.is_some_and(sensitive_activity_field) {
        return serde_json::Value::String("[redacted]".to_string());
    }
    if depth >= 5 {
        *truncated = true;
        return serde_json::Value::String("…".to_string());
    }
    match value {
        serde_json::Value::String(value) => {
            if sensitive_activity_text(value) {
                return serde_json::Value::String("[redacted]".to_string());
            }
            let safe = snapshot::safe_public_text(value, 1_200);
            if safe.chars().count() < value.chars().count() {
                *truncated = true;
            }
            serde_json::Value::String(safe)
        }
        serde_json::Value::Array(values) => {
            if values.len() > 24 {
                *truncated = true;
            }
            serde_json::Value::Array(
                values
                    .iter()
                    .take(24)
                    .map(|value| bounded_activity_value(value, None, depth + 1, truncated))
                    .collect(),
            )
        }
        serde_json::Value::Object(values) => {
            if values.len() > 32 {
                *truncated = true;
            }
            serde_json::Value::Object(
                values
                    .iter()
                    .filter(|(key, _)| key.as_str() != "_runtime_activity_binding")
                    .take(32)
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            bounded_activity_value(value, Some(key), depth + 1, truncated),
                        )
                    })
                    .collect(),
            )
        }
        value => value.clone(),
    }
}

fn sensitive_activity_field(field: &str) -> bool {
    let normalized = field
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    matches!(
        normalized.as_str(),
        "token"
            | "accesstoken"
            | "refreshtoken"
            | "idtoken"
            | "apikey"
            | "secret"
            | "clientsecret"
            | "password"
            | "passwd"
            | "authorization"
            | "cookie"
            | "setcookie"
            | "credential"
            | "privatekey"
    )
}

fn sensitive_activity_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "authorization: bearer ",
        "authorization=basic ",
        "api_key=",
        "api-key=",
        "access_token=",
        "refresh_token=",
        "password=",
        "passwd=",
        "client_secret=",
        "private_key=",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn activity_value_summary(value: &serde_json::Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return (!value.trim().is_empty()).then(|| snapshot::safe_public_text(value, 320));
    }
    for key in [
        "summary",
        "message",
        "result_summary",
        "output_preview",
        "outcome",
        "error",
        "status",
    ] {
        if let Some(value) = value.get(key).and_then(serde_json::Value::as_str) {
            if !value.trim().is_empty() {
                return Some(snapshot::safe_public_text(value, 320));
            }
        }
    }
    match value {
        serde_json::Value::Object(values) => Some(format!("{} fields", values.len())),
        serde_json::Value::Array(values) => Some(format!("{} items", values.len())),
        _ => None,
    }
}

fn activity_content_ref(value: &serde_json::Value) -> Option<String> {
    [
        value.get("full_output_ref"),
        value.get("content_ref"),
        value.pointer("/output_ref/ref_id"),
        value.get("artifact_ref"),
        value.get("evidence_ref"),
    ]
    .into_iter()
    .flatten()
    .find_map(serde_json::Value::as_str)
    .and_then(snapshot::safe_public_ref)
}

fn activity_identity_matches_entity(
    activity: &harness_contract::projection::ExecutionActivityProjection,
    entity: &ProjectionEntity,
) -> bool {
    let exact_ids = [
        activity.team_run_id.as_deref(),
        activity.agent_instance_id.as_deref(),
        activity.agent_run_id.as_deref(),
    ];
    exact_ids.into_iter().flatten().any(|identity| {
        entity.id == identity
            || entity.detail.as_ref().is_some_and(|detail| {
                ["team_id", "run_id", "agent_id", "instance_id"]
                    .iter()
                    .any(|key| {
                        detail.get(*key).and_then(serde_json::Value::as_str) == Some(identity)
                    })
            })
    })
}

fn activity_identity_entities(
    activity: &harness_contract::projection::ExecutionActivityProjection,
) -> Vec<ProjectionEntity> {
    let mut entities = Vec::new();
    let mut push_identity = |kind: &str, id: Option<&str>, detail: serde_json::Value| {
        if let Some(id) = id.filter(|id| !id.trim().is_empty()) {
            entities.push(ProjectionEntity {
                id: format!("{kind}:{id}"),
                kind: kind.to_string(),
                revision: activity.sequence,
                status: Some(activity.status.clone()),
                summary: Some(id.to_string()),
                evidence_refs: activity.evidence_refs.clone(),
                payload: None,
                detail: Some(detail),
            });
        }
    };
    push_identity(
        "team_run",
        activity.team_run_id.as_deref(),
        serde_json::json!({
            "team_run_id": activity.team_run_id,
            "definition_refs": activity.definition_refs,
        }),
    );
    push_identity(
        "agent_run",
        activity.agent_run_id.as_deref(),
        serde_json::json!({
            "agent_instance_id": activity.agent_instance_id,
            "agent_run_id": activity.agent_run_id,
            "definition_refs": activity.definition_refs,
        }),
    );
    push_identity(
        "skill_activation",
        activity.skill_activation_id.as_deref(),
        serde_json::json!({
            "skill_id": activity.skill_id,
            "skill_revision": activity.skill_revision,
            "skill_activation_id": activity.skill_activation_id,
        }),
    );
    push_identity(
        "tool_invocation",
        activity.tool_call_id.as_deref(),
        serde_json::json!({
            "tool_contract_id": activity.tool_contract_id,
            "tool_call_id": activity.tool_call_id,
        }),
    );
    for definition_ref in &activity.definition_refs {
        push_identity(
            "definition_ref",
            Some(definition_ref),
            serde_json::json!({"definition_ref": definition_ref}),
        );
    }
    entities
}

pub async fn command(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
    request: ExecutionCommandRequest,
) -> Result<ExecutionCommandReceipt, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services
        .execution_supervisor()
        .graph_projection(execution_id)
        .await?;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, false)?;
    validate_projection_scope(&scope, context)?;
    let command = match request.command {
        ExecutionCommandKind::Pause => ExecutionGraphCommand::Pause {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "paused by projection command".to_string()),
        },
        ExecutionCommandKind::Resume => ExecutionGraphCommand::Resume {
            expected_revision: request.expected_revision,
        },
        ExecutionCommandKind::Cancel => ExecutionGraphCommand::Cancel {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "cancelled by projection command".to_string()),
        },
        ExecutionCommandKind::Replan => ExecutionGraphCommand::Replan {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "replan requested by projection command".to_string()),
            replacement_payload_ref: string_payload(&request.payload, "replacement_payload_ref")
                .unwrap_or_else(|| "projection-command:replan".to_string()),
        },
    };
    if graph.revision != request.expected_revision {
        return Ok(ExecutionCommandReceipt {
            command_id: request.command_id,
            accepted_revision: graph.revision,
            status: "rejected_stale_revision".to_string(),
            reason: Some(
                "projection revision changed; refresh snapshot before retrying".to_string(),
            ),
        });
    }
    let receipt = services
        .execution_supervisor()
        .command_graph(execution_id, command)
        .await?;
    Ok(ExecutionCommandReceipt {
        command_id: request.command_id,
        accepted_revision: receipt.accepted_revision,
        status: "accepted".to_string(),
        reason: None,
    })
}

async fn available_commands(
    services: &RuntimeServices,
    execution_id: &str,
    _context: &ProjectionQueryContext,
) -> Result<Vec<ProjectionCommandAvailability>, RuntimeServicesError> {
    let graph = services
        .execution_supervisor()
        .graph_projection(execution_id)
        .await?;
    Ok(available_commands_for_graph(&graph))
}

fn available_commands_for_graph(
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
) -> Vec<ProjectionCommandAvailability> {
    let terminal = graph.nodes.iter().all(|node| node.status.is_terminal());
    let paused = graph
        .nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Paused);
    [
        ExecutionCommandKind::Pause,
        ExecutionCommandKind::Resume,
        ExecutionCommandKind::Cancel,
        ExecutionCommandKind::Replan,
    ]
    .into_iter()
    .map(|command| {
        let available = match command {
            ExecutionCommandKind::Pause => !terminal && !paused,
            ExecutionCommandKind::Resume => !terminal && paused,
            ExecutionCommandKind::Cancel | ExecutionCommandKind::Replan => !terminal,
        };
        ProjectionCommandAvailability {
            command,
            available,
            reason: (!available)
                .then(|| "execution state does not permit this command".to_string()),
        }
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::snapshot::*;
    use super::*;
    use harness_contract::{
        execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus, ExecutionParentBinding,
        },
        goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract},
        task::{TaskCreateCommand, TaskExecutionPolicy, TaskSpec},
    };

    fn context(services: &RuntimeServices) -> ProjectionQueryContext {
        ProjectionQueryContext {
            principal: "test".to_string(),
            workspace_id: services.workspace_key().to_string(),
            session_scopes: Vec::new(),
            mission_scopes: Vec::new(),
            visibility_grants: vec![
                format!("workspace:{}", services.workspace_key()),
                "resource:*".to_string(),
            ],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        }
    }

    fn graph_with_lineage(
        objective: &str,
        session_id: &str,
        turn_id: &str,
        task_id: &str,
    ) -> ExecutionGraph {
        ExecutionGraph::new(objective).with_lineage(ExecutionGraphLineage {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            root_task_id: task_id.to_string(),
            task_id: task_id.to_string(),
            generation: 1,
        })
    }

    #[test]
    fn activity_content_is_bounded_and_redacts_sensitive_fields() {
        let source = serde_json::json!({
            "query": "summarize the report",
            "api_key": "sk-private",
            "nested": {
                "authorization": "Bearer private",
                "max_tokens": 4096,
                "note": "authorization: bearer private"
            }
        });
        let mut truncated = false;
        let projected = bounded_activity_value(&source, None, 0, &mut truncated);

        assert_eq!(projected["query"], "summarize the report");
        assert_eq!(projected["api_key"], "[redacted]");
        assert_eq!(projected["nested"]["authorization"], "[redacted]");
        assert_eq!(projected["nested"]["note"], "[redacted]");
        assert_eq!(projected["nested"]["max_tokens"], 4096);
        assert!(!truncated);
    }

    #[test]
    fn explicit_negative_strategy_warning_projects_as_cost_not_benefit() {
        let warning =
            "explicit Team request has negative estimated lift; surface must show the cost warning";
        let (benefit, cost) = strategy_public_reasons(
            &[
                warning.to_string(),
                "explicit Team topology was requested".to_string(),
            ],
            None,
            None,
            &[],
        );

        assert!(benefit.contains(&"explicit Team topology was requested".to_string()));
        assert!(!benefit.contains(&warning.to_string()));
        assert_eq!(cost, vec![warning.to_string()]);
    }

    #[test]
    fn admission_observation_projects_typed_wait_state_without_prose_inference() {
        let request_id = uuid::Uuid::new_v4();
        let event = crate::DurableRuntimeEvent {
            event_id: "admission-waiting".to_string(),
            stream_id: format!("resource-admission:{request_id}"),
            sequence: 3,
            scope: RuntimeEventScope::Schedule,
            kind: "resource.admission.waiting".to_string(),
            status: Some("waiting".to_string()),
            actor: Some("execution_resource_manager".to_string()),
            refs: vec![crate::RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: "graph-admission".to_string(),
            }],
            payload: serde_json::to_value(ResourceAdmissionObservation {
                request_id,
                status: ResourceAdmissionObservationStatus::Waiting,
                requested_priority: Some(90),
                deadline_at_ms: Some(42_000),
                requested_service_class:
                    harness_contract::execution_graph::ExecutionServiceClass::Interactive,
                resolved_service_class:
                    harness_contract::execution_graph::ExecutionServiceClass::Foreground,
                parent_class_ceiling: Some(
                    harness_contract::execution_graph::ExecutionServiceClass::Foreground,
                ),
                demands: vec![(crate::execution_core::graph::ExecutionResourceKind::Tool, 2)],
                normalized_scope: Some("workspace:/project".to_string()),
                fairness_key: "graph:graph-admission".to_string(),
                enqueue_sequence: Some(4),
                enqueued_at_ms: Some(1_000),
                observed_at_ms: 1_125,
                queue_age_ms: 125,
                wait_reason: Some(ResourceWaitReason::ScopeInfeasible),
                blocker: None,
                policy_revision: 7,
                pending: 2,
            })
            .expect("serialize observation"),
            created_at_ms: 1_125,
            commit_cursor: 8,
            transaction_id: "tx-admission".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };

        let payload = projection_entity_payload(&event).expect("typed admission payload");
        let ProjectionEntityPayload::Admission(admission) = payload else {
            panic!("expected admission payload");
        };
        assert_eq!(admission.request_id, request_id.to_string());
        assert_eq!(admission.status, AdmissionProjectionStatus::WaitingScope);
        assert_eq!(admission.queue_age_ms, 125);
        assert_eq!(admission.wait_reason.as_deref(), Some("scope_infeasible"));
        assert_eq!(admission.resource_demands, vec!["tool:2"]);
    }

    #[tokio::test]
    async fn projection_snapshot_delta_and_command_share_one_graph_revision() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("projection contract graph").with_lineage(
            harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session-a".to_string(),
                turn_id: "turn-session-scope".to_string(),
                root_task_id: "task-session-scope".to_string(),
                task_id: "task-session-scope".to_string(),
                generation: 1,
            },
        );
        let graph_id = graph.id.clone();
        let (graph_receipt, _) = services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let mission = services
            .mission_runtime()
            .ensure_default_mission()
            .expect("default Mission exists");
        let task = services
            .task_aggregate_service()
            .create(TaskCreateCommand {
                task_id: "task-session-scope".to_string(),
                mission_id: mission.mission_id.clone(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: "session-origin".to_string(),
                origin_turn_id: "turn-origin".to_string(),
                root_task_id: "task-session-scope".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::ExplicitLocked,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec {
                    objective: "session-scoped projection".to_string(),
                    phases: Vec::new(),
                    execution_policy: TaskExecutionPolicy {
                        yolo_mode: false,
                        max_failures_before_block: 3,
                    },
                    application_provenance: None,
                },
                evidence_refs: Vec::new(),
            })
            .expect("task creates");
        services
            .task_aggregate_service()
            .link_graph(
                &task.aggregate.task_id,
                task.aggregate.revision,
                &graph_id,
                graph_receipt.accepted_revision,
                Vec::new(),
            )
            .expect("task binds graph");
        let second_task = services
            .task_aggregate_service()
            .create(TaskCreateCommand {
                task_id: "task-session-scope-peer".to_string(),
                mission_id: mission.mission_id.clone(),
                kind: harness_contract::task::TaskKind::Delegated,
                origin: harness_contract::task::TaskOrigin::Delegated,
                origin_session_id: "session-a".to_string(),
                origin_turn_id: "turn-session-scope".to_string(),
                root_task_id: "task-session-scope".to_string(),
                parent_task_id: Some("task-session-scope".to_string()),
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::ExplicitLocked,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec {
                    objective: "shared Team role".to_string(),
                    phases: Vec::new(),
                    execution_policy: TaskExecutionPolicy {
                        yolo_mode: false,
                        max_failures_before_block: 3,
                    },
                    application_provenance: None,
                },
                evidence_refs: Vec::new(),
            })
            .expect("peer task creates");
        services
            .task_aggregate_service()
            .link_graph(
                &second_task.aggregate.task_id,
                second_task.aggregate.revision,
                &graph_id,
                graph_receipt.accepted_revision,
                Vec::new(),
            )
            .expect("peer task binds shared Team graph");
        let query = context(&services);
        let initial_snapshot = snapshot(&services, &graph_id, &query)
            .await
            .expect("snapshot");
        assert_eq!(initial_snapshot.execution_id, graph_id);
        assert_eq!(initial_snapshot.session_id.as_deref(), Some("session-a"));
        assert_eq!(
            initial_snapshot.mission_id.as_deref(),
            Some(mission.mission_id.as_str())
        );
        assert_eq!(
            initial_snapshot.task_id.as_deref(),
            Some("task-session-scope"),
            "the graph lineage keeps the root Task authoritative across delegated roles"
        );
        assert_eq!(
            initial_snapshot.schema_version,
            EXECUTION_PROJECTION_SCHEMA_VERSION
        );
        let root_activity_id = initial_snapshot
            .activities
            .first()
            .expect("root activity")
            .activity_id
            .clone();
        let detail = activity_detail(
            &services,
            &initial_snapshot.execution_id,
            &root_activity_id,
            &query,
        )
        .await
        .expect("activity detail");
        assert_eq!(detail.activity.activity_id, root_activity_id);
        assert_eq!(detail.execution_id, initial_snapshot.execution_id);
        let delta = delta(&services, &initial_snapshot.execution_id, 0, 0, &query).expect("delta");
        assert!(delta.target_cursor >= initial_snapshot.cursor);
        assert!(delta.operations.iter().any(|operation| {
            matches!(
                operation,
                ProjectionOperation::AdvanceCursor { cursor }
                    if *cursor == delta.target_cursor
            )
        }));
        let receipt = command(
            &services,
            &initial_snapshot.execution_id,
            &query,
            ExecutionCommandRequest {
                command_id: "projection-pause".to_string(),
                expected_revision: initial_snapshot.revision,
                command: ExecutionCommandKind::Pause,
                payload: serde_json::json!({ "reason": "test" }),
            },
        )
        .await
        .expect("command receipt");
        assert_eq!(receipt.status, "accepted");
        assert!(receipt.accepted_revision > initial_snapshot.revision);
    }

    #[tokio::test]
    async fn projection_delta_materializes_the_same_state_as_a_fresh_snapshot() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = graph_with_lineage(
            "projection equivalence",
            "projection-session",
            "projection-turn",
            "projection-task",
        );
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query)
            .await
            .expect("initial snapshot");
        let receipt = command(
            &services,
            &graph_id,
            &query,
            ExecutionCommandRequest {
                command_id: "projection-equivalence-pause".to_string(),
                expected_revision: initial.revision,
                command: ExecutionCommandKind::Pause,
                payload: serde_json::json!({"reason": "projection equivalence"}),
            },
        )
        .await
        .expect("pause command");
        assert_eq!(receipt.status, "accepted");

        let delta = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor,
            &query,
        )
        .expect("materialized delta");
        let reduced = harness_contract::projection::reduce_projection_delta(&initial, &delta)
            .expect("delta applies");
        let canonical = snapshot(&services, &graph_id, &query)
            .await
            .expect("canonical snapshot");
        assert_eq!(reduced, canonical);
    }

    #[tokio::test]
    async fn unrelated_commits_advance_only_the_projection_consumption_cursor() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = graph_with_lineage(
            "projection cursor isolation",
            "projection-session",
            "projection-turn",
            "projection-task",
        );
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query)
            .await
            .expect("initial snapshot");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "evolution:unrelated".to_string(),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.unrelated.recorded".to_string(),
                status: Some("recorded".to_string()),
                actor: Some("projection-test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"unrelated": true}),
            })
            .expect("unrelated event commits");

        let delta = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor,
            &query,
        )
        .expect("cursor-only delta");
        let reduced = harness_contract::projection::reduce_projection_delta(&initial, &delta)
            .expect("cursor-only delta applies");
        assert!(reduced.cursor > initial.cursor);
        assert_eq!(reduced.revision, initial.revision);
        assert_eq!(
            reduced.graph.commit_cursor, initial.graph.commit_cursor,
            "global projection consumption must not rewrite the graph commit cursor"
        );
    }

    #[tokio::test]
    async fn projection_exposes_only_durable_child_execution_lineage() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let parent = graph_with_lineage(
            "root execution",
            "lineage-session",
            "lineage-turn",
            "lineage-task",
        );
        let parent_id = parent.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                parent,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("parent graph starts");

        let mut child = graph_with_lineage(
            "nested team protocol",
            "lineage-session",
            "lineage-turn",
            "lineage-task",
        );
        child.parent_execution = Some(ExecutionParentBinding {
            execution_id: parent_id.clone(),
            node_id: "root-tool-batch".to_string(),
        });
        let child_id = child.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                child,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("child graph starts");

        let sibling = graph_with_lineage(
            "unrelated same-runtime execution",
            "sibling-session",
            "sibling-turn",
            "sibling-task",
        );
        let sibling_id = sibling.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                sibling,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("sibling graph starts");

        let links = services
            .graph_state_store()
            .child_links(&parent_id)
            .expect("durable child index");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].child_execution_id, child_id);
        assert_eq!(links[0].parent_node_id, "root-tool-batch");

        let projection = snapshot(&services, &parent_id, &context(&services))
            .await
            .expect("parent projection");
        assert_eq!(projection.child_executions.len(), 1);
        assert_eq!(projection.child_executions[0].execution_id, child_id);
        assert_eq!(
            projection.child_executions[0].parent_node_id,
            "root-tool-batch"
        );
        assert!(projection
            .child_executions
            .iter()
            .all(|child| child.execution_id != sibling_id));
        assert!(projection.activities.iter().any(|activity| {
            activity.kind == harness_contract::projection::ExecutionActivityKind::Execution
                && activity.scope.execution_id == child_id
        }));
        assert!(!projection
            .activities
            .iter()
            .any(|activity| { activity.scope.execution_id == sibling_id }));

        let delta = delta(&services, &parent_id, 0, 0, &context(&services)).expect("lineage delta");
        assert!(delta.operations.iter().any(|operation| {
            matches!(
                operation,
                ProjectionOperation::UpsertChildExecution { child }
                    if child.execution_id == child_id
            )
        }));
    }

    #[test]
    fn linked_team_topology_supplies_strategy_identity_without_terminal_receipt() {
        let team_id = "runtime-team:live".to_string();
        let team_graph_id = "team-graph:runtime-team:live".to_string();
        let scope = ExecutionProjectionScope {
            session_id: Some("session-live-team".to_string()),
            mission_id: None,
            task_id: None,
            turn_id: None,
            execution_ids: BTreeSet::from(["parent-execution".to_string(), team_graph_id.clone()]),
            node_ids: BTreeSet::new(),
            entity_ids: BTreeSet::from([team_id.clone()]),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: vec![ProjectionEntity {
                id: team_id.clone(),
                kind: "team".to_string(),
                revision: 1,
                status: Some("running".to_string()),
                summary: Some("live team".to_string()),
                evidence_refs: Vec::new(),
                payload: None,
                detail: Some(serde_json::json!({"graph_id": team_graph_id})),
            }],
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            child_executions: vec![ChildExecutionProjection {
                execution_id: team_graph_id.clone(),
                parent_execution_id: "parent-execution".to_string(),
                parent_node_id: "parent-node".to_string(),
                revision: 1,
                cursor: 3,
                status: "running".to_string(),
                objective: "live delegated work".to_string(),
            }],
        };

        assert_eq!(live_team_topology(&scope), Some((team_id, team_graph_id)));
    }

    #[tokio::test]
    async fn projection_uses_latest_exact_strategy_revision_not_generic_orchestration() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let session_graph = |objective: &str| {
            let mut graph = graph_with_lineage(
                objective,
                "strategy-projection",
                "turn-strategy-projection",
                "task-strategy-projection",
            );
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                serde_json::json!({
                    "session_id": "strategy-projection",
                    "kind": "projection_test",
                })
                .to_string(),
            );
            node.id = format!("{}:node", graph.id);
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
            graph.nodes.push(node);
            graph
        };
        let graph = session_graph("strategy projection");
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("graph registers");
        let sibling = session_graph("same-session sibling strategy");
        let sibling_id = sibling.id.clone();
        services
            .execution_supervisor()
            .register_graph(sibling)
            .await
            .expect("sibling graph registers");
        let child = session_graph("same-session child strategy");
        let child_id = child.id.clone();
        services
            .execution_supervisor()
            .register_graph(child)
            .await
            .expect("child graph registers");
        let strategy_event = |execution_id: &str, decision_id: &str, kind: &str, revision: u64| {
            crate::RuntimeEventInput {
                stream_id: "session:strategy-projection".to_string(),
                scope: crate::RuntimeEventScope::Session,
                kind: kind.to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: vec![crate::RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: execution_id.to_string(),
                }],
                payload: serde_json::json!({
                    "decision_id": decision_id,
                    "decision_revision": revision,
                    "execution_graph_ref": execution_id,
                    "session_ref": "strategy-projection",
                    "turn_ref": format!("turn-{decision_id}"),
                    "policy_version": "strategy-decision-v4",
                    "decision_source": "deterministic",
                    "confidence": 90,
                    "selected_candidate": if revision == 1 { "team" } else { "direct" },
                    "selected_pattern": if revision == 1 { "collaborate" } else { "direct" },
                    "candidate_estimates": [],
                    "selection_reasons": ["integer cost model selected the candidate"],
                    "resource_snapshot": harness_contract::strategy::StrategyResourceSnapshot::default(),
                    "evidence_scopes": [],
                    "outcome": if kind == "runtime.strategy.outcome" {
                        serde_json::json!({
                            "duration_ms": 42,
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "cached_tokens": 0,
                            "tool_calls": 1,
                            "duplicate_tool_calls": 0,
                            "max_tool_concurrency_observed": 1,
                            "parallel_tool_batches": 0,
                            "write_attempt_paths": ["/home/private/secret.txt"],
                            "evidence_overlap_bp": 0,
                            "evidence_overlap_observed": true,
                            "working_state_verified": true,
                            "merge_cost_ms": 0,
                            "parent_merge_count": 1,
                            "quality_score_bp": 9000,
                            "actual_speedup_ratio_bp": null,
                            "terminal_reason": "completed"
                        })
                    } else {
                        serde_json::Value::Null
                    },
                }),
            }
            .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id: execution_id.to_string(),
                session_id: "session-activity".to_string(),
                turn_id: "turn-activity".to_string(),
                root_task_id: "task-activity".to_string(),
                task_id: "task-activity".to_string(),
                activity_id: format!("activity:execution:{execution_id}"),
                node_id: None,
                parent_activity_id: None,
                initiator_activity_id: None,
                team_run_id: None,
                agent_instance_id: None,
                agent_run_id: None,
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: revision.max(1),
                fence: 1,
                generation: 1,
            })
            .expect("strategy event binding")
        };
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.selected",
                1,
            ))
            .expect("selected event");
        let selected_projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("selected-only projection")
            .strategy
            .expect("selected-only strategy");
        assert_eq!(selected_projection.status.as_deref(), Some("running"));
        assert_eq!(
            selected_projection.actual_status,
            Some(StrategyActualStatus::Unknown)
        );
        assert!(selected_projection.actual.is_none());
        for index in 0..600 {
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: "session:strategy-projection".to_string(),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "runtime.noise".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("test".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"index": index}),
                })
                .expect("noise event");
        }
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                kind: "runtime.orchestration.completed".to_string(),
                ..strategy_event(
                    &graph_id,
                    "decision-1",
                    "runtime.orchestration.completed",
                    99,
                )
            })
            .expect("generic orchestration event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.downgraded",
                2,
            ))
            .expect("downgrade event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.early_stopped",
                3,
            ))
            .expect("early stop event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                4,
            ))
            .expect("outcome event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                4,
            ))
            .expect("exact equal-revision replay");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                payload: serde_json::json!({
                    "decision_id": "decision-1",
                    "decision_revision": 4,
                    "execution_graph_ref": graph_id.clone(),
                    "session_ref": "strategy-projection",
                    "turn_ref": "turn-decision-1",
                    "policy_version": "strategy-decision-v4",
                    "decision_source": "deterministic",
                    "confidence": 1,
                    "selected_candidate": "team",
                    "selected_pattern": "collaborate",
                    "candidate_estimates": [],
                    "selection_reasons": ["conflicting replay must not replace truth"],
                    "resource_snapshot": harness_contract::strategy::StrategyResourceSnapshot::default(),
                    "evidence_scopes": [],
                    "outcome": serde_json::Value::Null,
                }),
                ..strategy_event(
                    &graph_id,
                    "decision-1",
                    "runtime.strategy.outcome",
                    4,
                )
            })
            .expect("conflicting equal-revision event");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                payload: serde_json::json!({
                    "decision_id": "decision-1",
                    "decision_revision": 99,
                    "execution_graph_ref": graph_id.clone(),
                    "session_ref": "strategy-projection",
                    "turn_ref": "turn-other",
                    "selected_candidate": "team",
                    "selected_pattern": "collaborate",
                }),
                ..strategy_event(&graph_id, "decision-1", "runtime.strategy.downgraded", 99)
            })
            .expect("wrong-turn event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.downgraded",
                1,
            ))
            .expect("stale revision event");
        for (execution_id, decision_id, revision) in [
            (&sibling_id, "decision-sibling", 30),
            (&child_id, "decision-child", 40),
        ] {
            services
                .event_store()
                .append(strategy_event(
                    execution_id,
                    decision_id,
                    "runtime.strategy.selected",
                    1,
                ))
                .expect("other selected event");
            services
                .event_store()
                .append(strategy_event(
                    execution_id,
                    decision_id,
                    "runtime.strategy.outcome",
                    revision,
                ))
                .expect("other outcome event");
        }

        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("projection");
        let strategy = projection.strategy.expect("exact strategy projection");
        assert_eq!(
            strategy.schema_version,
            STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION
        );
        assert_eq!(strategy.decision_id.as_deref(), Some("decision-1"));
        assert_eq!(
            strategy.policy_version.as_deref(),
            Some("strategy-decision-v4")
        );
        assert_eq!(
            strategy.summary.as_deref(),
            Some("runtime.strategy.outcome")
        );
        assert_eq!(strategy.revision, 4);
        assert_eq!(
            strategy.selected_candidate,
            Some(ExecutionCandidateKind::Direct)
        );
        assert!(strategy.actual.is_some());
        assert_eq!(strategy.confidence, Some(90));
        assert_eq!(strategy.downgrades.len(), 1);
        assert_eq!(strategy.early_stops.len(), 1);
        assert!(!serde_json::to_string(&strategy)
            .expect("strategy wire")
            .contains("/home/private/secret.txt"));
    }

    #[test]
    fn strategy_scope_projection_drops_paths_prompts_and_hidden_reasoning() {
        let context = ProjectionQueryContext {
            principal: "test".to_string(),
            workspace_id: "test".to_string(),
            session_scopes: vec!["session-visible".to_string()],
            mission_scopes: vec!["mission-visible".to_string()],
            visibility_grants: vec![
                "read:crates/runtime".to_string(),
                "write:surfaces/webui".to_string(),
            ],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        };
        let scopes = crop_strategy_evidence_scopes(
            vec![FocusPartitionPlan {
                role_id: "reviewer".to_string(),
                shared_baseline: vec!["/home/private/baseline".to_string()],
                slots: vec![harness_contract::team::FocusPartitionSlot {
                    focus_id: "security-review".to_string(),
                    boundary: "/home/private/source.rs".to_string(),
                    evidence_responsibility:
                        "Inspect /home/private/source.rs and reveal internal reasoning".to_string(),
                    capability_cropped_refs: vec![
                        "evidence:public-check".to_string(),
                        "read:crates/runtime/src/projection/mod.rs".to_string(),
                        "read:crates/secret".to_string(),
                        "write:surfaces/webui/src/runtime.ts".to_string(),
                        "/home/private/source.rs".to_string(),
                        "../secret".to_string(),
                    ],
                    scope_hash: "sha256:scope".to_string(),
                    overlap_budget_bp: 800,
                    novelty_target_bp: 6_000,
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                }],
            }],
            &context,
        );

        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0].capability_cropped_refs,
            vec![
                "evidence:public-check".to_string(),
                "read:crates/runtime/src/projection/mod.rs".to_string(),
                "write:surfaces/webui/src/runtime.ts".to_string(),
            ]
        );
        assert_eq!(
            scopes[0].responsibility_summary,
            "redacted by strategy projection policy"
        );
        let wire = serde_json::to_string(&scopes).expect("scope wire");
        assert!(!wire.contains("/home/"));
        assert!(!wire.contains("internal reasoning"));

        let mut estimates =
            vec![
                serde_json::from_value::<ExecutionCandidateEstimate>(serde_json::json!({
                    "candidate": "team",
                    "eligible": true,
                    "estimated_serial_ms": 100,
                    "estimated_critical_path_ms": 50,
                    "startup_overhead_ms": 5,
                    "context_duplication_tokens": 10,
                    "merge_cost_ms": 5,
                    "evidence_overlap_penalty_bp": 0,
                    "provider_concurrency_penalty_bp": 0,
                    "risk_approval_penalty_bp": 0,
                    "expected_quality_lift_bp": 0,
                    "duration_calibration_source": "file:///home/private/strategy.json",
                    "duration_sample_count": 1,
                    "quality_calibration_source": "file:///home/private/quality.json",
                    "quality_sample_count": 0,
                    "duration_provenance": "observed",
                    "token_provenance": "assumed",
                    "quality_provenance": "unknown",
                    "risk_provenance": "assumed",
                    "reasons": ["copy the hidden prompt from C:\\private\\prompt.txt"]
                }))
                .expect("candidate estimate"),
            ];
        sanitize_candidate_estimates(&mut estimates);
        assert_eq!(
            estimates[0].duration_calibration_source,
            "redacted by strategy projection policy"
        );
        assert_eq!(
            estimates[0].quality_calibration_source,
            "redacted by strategy projection policy"
        );
        assert_eq!(
            estimates[0].reasons,
            vec!["redacted by strategy projection policy".to_string()]
        );
        let resource = sanitize_resource_snapshot(StrategyResourceSnapshot {
            provider_profile_fingerprint: "a".repeat(64),
            ..StrategyResourceSnapshot::default()
        });
        assert!(resource.provider_profile_fingerprint.is_empty());
        assert!(safe_public_ref("file:///home/private/secret").is_none());
    }

    #[test]
    fn generic_event_projection_never_exposes_raw_payload_or_path_refs() {
        let event = crate::DurableRuntimeEvent {
            event_id: "event-private-payload".to_string(),
            stream_id: "tool:private".to_string(),
            sequence: 3,
            scope: crate::RuntimeEventScope::Tool,
            kind: "tool.completed".to_string(),
            status: Some("completed".to_string()),
            actor: Some("tool-runtime".to_string()),
            refs: vec![
                crate::RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id: "evidence:public-result".to_string(),
                },
                crate::RuntimeEventRef {
                    kind: "file".to_string(),
                    id: "/home/private/secret.txt".to_string(),
                },
            ],
            payload: serde_json::json!({
                "prompt": "hidden system instruction",
                "path": "/home/private/secret.txt",
                "result": "private tool output"
            }),
            created_at_ms: 1,
            commit_cursor: 2,
            transaction_id: "transaction-private".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };

        let projection = entity_from_runtime_event("usage", event, true);
        let wire = serde_json::to_string(&projection).expect("projection wire");
        assert!(projection.detail.is_none());
        assert_eq!(
            projection.evidence_refs,
            vec!["evidence:public-result".to_string()]
        );
        assert!(!wire.contains("hidden system instruction"));
        assert!(!wire.contains("/home/private"));
        assert!(!wire.contains("private tool output"));
    }

    #[tokio::test]
    async fn projection_scope_never_leaks_other_session_goals() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mission = services
            .mission_runtime()
            .ensure_default_mission()
            .expect("default Mission");
        let mut graph = graph_with_lineage(
            "session-scoped projection",
            "session-a",
            "turn-session-scope",
            "task-session-scope",
        );
        let dispatch = harness_contract::turn::SessionDispatchCommand {
            command_id: "scope-dispatch".to_string(),
            action: harness_contract::turn::SessionDispatchAction::Enqueue,
            handoff: harness_contract::turn::SessionHandoff {
                handoff_id: "scope-handoff".to_string(),
                source_session_id: "session-a".to_string(),
                target_session_id: "session-target".to_string(),
                objective: "scope test".to_string(),
                acceptance: Vec::new(),
                scope: Vec::new(),
                context_lens: Vec::new(),
                evidence_refs: Vec::new(),
                context_budget_lease: None,
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                deadline_at_ms: None,
                priority: 1,
                correlation_id: "scope-correlation".to_string(),
                result_contract: "return result".to_string(),
                task_route_hint: None,
            },
            expected_target_revision: 0,
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            crate::SESSION_DISPATCH_EXECUTOR,
            format!(
                "session_handoff:{}",
                serde_json::to_string(&dispatch).expect("handoff serializes")
            ),
        );
        node.id = "dispatch-a".to_string();
        node.idempotency_key = "dispatch-a-key".to_string();
        graph.nodes.push(node);
        graph
            .node_statuses
            .insert("dispatch-a".to_string(), ExecutionNodeStatus::Planned);
        let graph_id = graph.id.clone();
        let (graph_receipt, _) = services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let task = services
            .task_aggregate_service()
            .create(TaskCreateCommand {
                task_id: "task-session-scope".to_string(),
                mission_id: mission.mission_id.clone(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: "session-a".to_string(),
                origin_turn_id: "turn-session-scope".to_string(),
                root_task_id: "task-session-scope".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::ExplicitLocked,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec {
                    objective: "session-scoped projection".to_string(),
                    phases: Vec::new(),
                    execution_policy: TaskExecutionPolicy {
                        yolo_mode: false,
                        max_failures_before_block: 3,
                    },
                    application_provenance: None,
                },
                evidence_refs: Vec::new(),
            })
            .expect("task creates");
        services
            .task_aggregate_service()
            .link_graph(
                &task.aggregate.task_id,
                task.aggregate.revision,
                &graph_id,
                graph_receipt.accepted_revision,
                Vec::new(),
            )
            .expect("task binds graph");

        for (id, session_id) in [
            (format!("goal:{graph_id}"), "session-a"),
            ("goal-b".to_string(), "session-b"),
        ] {
            services
                .goal_store()
                .create(GoalContract {
                    id: id.clone(),
                    session_id: session_id.to_string(),
                    objective: format!("objective for {session_id}"),
                    criteria: vec![AcceptanceCriterion {
                        id: format!("criterion-{id}"),
                        statement: "produce evidence".to_string(),
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
                })
                .expect("goal creates");
        }

        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("snapshot");
        assert_eq!(projection.session_id.as_deref(), Some("session-a"));
        assert_eq!(
            projection.mission_id.as_deref(),
            Some(services.mission_runtime().default_mission_id())
        );
        assert_eq!(projection.task_id.as_deref(), Some("task-session-scope"));
        assert_eq!(projection.turn_id.as_deref(), Some("turn-session-scope"));
        assert_eq!(projection.goals.len(), 1);
        assert_eq!(projection.goals[0].id, format!("goal:{graph_id}"));
        assert!(projection.goals.iter().all(|goal| goal.id != "goal-b"));

        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let denied = ProjectionQueryContext {
            principal: "scoped-reader".to_string(),
            workspace_id: services.workspace_key().to_string(),
            session_scopes: vec!["session-b".to_string()],
            mission_scopes: vec![mission_id],
            visibility_grants: vec!["read:crates/runtime".to_string()],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 7,
        };
        assert!(matches!(
            snapshot(&services, &graph_id, &denied).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            delta(&services, &graph_id, 0, 0, &denied),
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            command(
                &services,
                &graph_id,
                &denied,
                ExecutionCommandRequest {
                    command_id: "denied-session".to_string(),
                    expected_revision: projection.revision,
                    command: ExecutionCommandKind::Pause,
                    payload: serde_json::json!({"reason": "must not execute"}),
                },
            )
            .await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));

        let denied_mission = ProjectionQueryContext {
            session_scopes: vec!["session-a".to_string()],
            mission_scopes: vec!["mission-other".to_string()],
            ..denied
        };
        assert!(matches!(
            snapshot(&services, &graph_id, &denied_mission).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            delta(&services, &graph_id, 0, 0, &denied_mission),
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            command(
                &services,
                &graph_id,
                &denied_mission,
                ExecutionCommandRequest {
                    command_id: "denied-mission".to_string(),
                    expected_revision: projection.revision,
                    command: ExecutionCommandKind::Pause,
                    payload: serde_json::json!({"reason": "must not execute"}),
                },
            )
            .await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[tokio::test]
    async fn projection_command_rejects_stale_revision_without_mutating_graph() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = graph_with_lineage(
            "stale projection command",
            "projection-session",
            "projection-turn",
            "projection-task",
        );
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial_snapshot = snapshot(&services, &graph_id, &query)
            .await
            .expect("snapshot");
        let receipt = command(
            &services,
            &graph_id,
            &query,
            ExecutionCommandRequest {
                command_id: "stale-command".to_string(),
                expected_revision: initial_snapshot.revision.saturating_add(1),
                command: ExecutionCommandKind::Pause,
                payload: serde_json::Value::Null,
            },
        )
        .await
        .expect("stale receipt");
        assert_eq!(receipt.status, "rejected_stale_revision");
        let after = snapshot(&services, &graph_id, &query).await.expect("after");
        assert_eq!(after.revision, initial_snapshot.revision);
    }

    #[tokio::test]
    async fn projection_rejects_a_context_from_another_workspace() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = graph_with_lineage(
            "workspace scope",
            "projection-session",
            "projection-turn",
            "projection-task",
        );
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let mut query = context(&services);
        query.workspace_id = "other-workspace".to_string();
        assert!(matches!(
            snapshot(&services, &graph_id, &query).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[tokio::test]
    async fn projection_cache_is_keyed_by_revision_detail_and_authorization_scope() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = graph_with_lineage(
            "projection cache",
            "projection-session",
            "projection-turn",
            "projection-task",
        );
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let mut summary = context(&services);
        summary.detail_scope = ProjectionDetailScope::Summary;

        snapshot(&services, &graph_id, &summary)
            .await
            .expect("first summary");
        assert_eq!(services.execution_projection_cache_stats(), (0, 1, 1));
        snapshot(&services, &graph_id, &summary)
            .await
            .expect("cached summary");
        assert_eq!(services.execution_projection_cache_stats(), (1, 1, 1));

        let mut full = summary.clone();
        full.detail_scope = ProjectionDetailScope::Full;
        snapshot(&services, &graph_id, &full)
            .await
            .expect("full projection");
        assert_eq!(services.execution_projection_cache_stats(), (1, 2, 2));

        let mut different_auth = summary;
        different_auth.authorization_revision += 1;
        snapshot(&services, &graph_id, &different_auth)
            .await
            .expect("new authorization projection");
        assert_eq!(services.execution_projection_cache_stats(), (1, 3, 3));
    }

    #[test]
    fn public_projection_text_rejects_every_shared_path_syntax() {
        let corpus: Vec<String> = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-public-redaction-corpus.json"
        ))
        .expect("shared redaction corpus");
        for secret in corpus {
            let rendered = safe_public_text(&format!("strategy detail {secret}"), 512);
            assert_eq!(
                rendered, "redacted by strategy projection policy",
                "{secret}"
            );
            assert!(
                safe_public_ref(&secret).is_none(),
                "unsafe reference {secret}"
            );
        }
    }
}
