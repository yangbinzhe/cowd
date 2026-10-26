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
    ExecutionCommandKind, ExecutionCommandReceipt, ExecutionCommandRequest,
    ExecutionConcurrencyCountsProjection, ExecutionConcurrencyProjection, ExecutionProjection,
    ExecutionResourceCapacityProjection, OutcomeProjection, OutcomeQualityProjection,
    ProjectionCommandAvailability, ProjectionDelta, ProjectionDetailScope, ProjectionEntity,
    ProjectionEntityCollection, ProjectionEntityPayload, ProjectionOperation,
    ProjectionQueryContext, ProjectionResyncReason, ProjectionSourceHealth,
    StrategyActualProjection, StrategyActualStatus, StrategyDecisionProjection,
    StrategyProofStatus, StrategyTransitionProjection, EXECUTION_PROJECTION_REDUCER_VERSION,
    EXECUTION_PROJECTION_SCHEMA_VERSION, STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION,
};
use harness_contract::reality::{EvidenceCompleteness, EvidenceRef, RealityBoundary};
use harness_contract::strategy::{
    ExecutionCandidateEstimate, ExecutionCandidateKind, StrategyDecisionSource,
    StrategyResourceSnapshot,
};
use sha2::{Digest, Sha256};

use crate::execution_core::graph::{
    ResourceAdmissionObservation, ResourceAdmissionObservationStatus, ResourceWaitReason,
};
use crate::{ExecutionGraphHost, RuntimeEventScope, RuntimeServices, RuntimeServicesError};

mod activity;
mod delta;
mod history;
#[cfg(test)]
pub(crate) use history::own843_checkpoint_state;
mod read_evidence;
mod reducer_support;
pub(crate) mod settlement;
mod snapshot;

pub use delta::delta;
pub use read_evidence::read_evidence;
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
    let through = services.event_store().durable_commit_cursor()?;
    let mut budget = history::RequestBudget::default();
    let scope = ExecutionProjectionScope::load_budgeted(
        services,
        execution_id,
        &graph,
        true,
        through,
        &mut budget,
    )?;
    validate_projection_scope(&scope, context)?;
    let facts = history::load_scope_budgeted(services, &scope, through, false, &mut budget)?;
    let (activities, activity_relations) =
        activity::project_execution_activities_from_history(services, &scope, &graph, &facts, true);
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
    let input = facts
        .values()
        .find_map(|state| state.detail_inputs.get(activity_id).cloned())
        .or_else(|| {
            activity_content_projection(&[], &[], false, activity.public_summary.as_deref())
        });
    let output = facts
        .values()
        .find_map(|state| state.detail_outputs.get(activity_id).cloned())
        .or_else(|| {
            activity_content_projection(&[], &[], true, activity.result_summary.as_deref())
        });
    let refs = activity
        .evidence_refs
        .iter()
        .chain(activity.artifact_refs.iter())
        .chain(activity.definition_refs.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut related_entities = facts
        .values()
        .filter_map(|state| state.detail_entities.get(activity_id))
        .flat_map(|entities| entities.values())
        .filter(|entity| {
            refs.is_empty()
                || refs.contains(&entity.id)
                || entity
                    .evidence_refs
                    .iter()
                    .any(|reference| refs.contains(reference))
        })
        .cloned()
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
            .map_err(RuntimeServicesError::Invariant)?
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
    for content in input.iter().chain(output.iter()) {
        if content.truncated && content.content_ref.is_none() {
            return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
                "activity content has no authorized durable reference".into(),
            )
            .into());
        }
    }
    let detail = harness_contract::projection::ExecutionActivityDetailProjection {
        schema_version: harness_contract::projection::EXECUTION_ACTIVITY_SCHEMA_VERSION,
        execution_id: activity.scope.execution_id.clone(),
        activity,
        input,
        output,
        relations,
        related_entities: related_entities.into_values().collect(),
    };
    validate_context(services, context)?;
    validate_projection_scope(&scope, context)?;
    history::check_bytes(&detail)?;
    Ok(detail)
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
            project_execution_graph, ExecutionFailure, ExecutionGraph, ExecutionGraphLineage,
            ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
            ExecutionParentBinding, ExecutionUsage,
        },
        goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract},
        task::{TaskCreateCommand, TaskSpec},
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

    fn r1_history_fixture() -> (
        std::sync::Arc<RuntimeServices>,
        std::sync::Arc<crate::content_publication::tests::JournalFault>,
        String,
    ) {
        let root = std::path::PathBuf::from(
            std::env::var_os("COWD_R1_06_FIXTURE_ROOT")
                .expect("explicit private plan fixture root"),
        )
        .join(format!("projection-history-{}", uuid::Uuid::new_v4()));
        assert!(
            root.is_absolute()
                && root
                    .starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
        );
        std::fs::create_dir_all(&root).unwrap();
        let (events, backend) = crate::content_publication::tests::JournalFault::new_journal();
        let services = RuntimeServices::builder(root.join("home"), &root)
            .runtime_event_store(events.clone())
            .artifact_store(std::sync::Arc::new(crate::ArtifactStore::for_test_default(
                root.join("artifacts"),
            )))
            .task_aggregate_service(std::sync::Arc::new(crate::TaskAggregateService::for_test()))
            .build()
            .unwrap();
        let graph = graph_with_lineage(
            "bounded history request",
            "history-session",
            "history-turn",
            "history-task",
        );
        let graph_id = graph.id.clone();
        crate::ExecutionCommitService::new(events)
            .register_graph(graph)
            .unwrap();
        (services, backend, graph_id)
    }

    #[tokio::test]
    async fn r1_projection_395_race_returns_complete_cut_without_replaying_full_history() {
        for detail_scope in [ProjectionDetailScope::Full, ProjectionDetailScope::Summary] {
            let (services, backend, graph_id) = r1_history_fixture();
            let body = "public projection body ".repeat(30_000);
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: format!("history:{graph_id}"),
                    scope: RuntimeEventScope::Session,
                    kind: "model.item_completed".into(),
                    status: None,
                    actor: None,
                    refs: vec![crate::RuntimeEventRef {
                        kind: "execution_graph".into(),
                        id: graph_id.clone(),
                    }],
                    payload: serde_json::json!({"text": body.clone()}),
                })
                .unwrap();
            let source_graph = services.graph_state_store().load(&graph_id).unwrap();
            let source_cut = services.event_store().durable_commit_cursor().unwrap();
            let mut query = context(&services);
            query.detail_scope = detail_scope;
            backend.reset_history_reads();
            backend.advance_cursor_after_projection_page();
            let projection = snapshot(&services, &graph_id, &query).await.unwrap();
            assert_eq!(projection.cursor, source_cut);
            assert_eq!(projection.graph.graph_id, graph_id);
            assert_eq!(projection.graph.revision, source_graph.revision);
            assert_eq!(projection.detail_scope, detail_scope);
            assert!(projection.terminal_axes.source_race);
            assert!(projection.terminal_axes.source_cursor > source_cut);
            assert_eq!(projection.terminal_axes.outcome.outcome, None);
            assert_eq!(
                projection.terminal_axes.execution.phase,
                harness_contract::projection::TerminalExecutionPhase::Unknown
            );
            assert_eq!(
                projection.terminal_axes.execution.settlement,
                harness_contract::projection::TerminalEffectSettlement::Unknown
            );
            let evidence = projection
                .evidence
                .iter()
                .find(|entity| entity.summary.as_deref() == Some("model.item_completed"))
                .unwrap();
            if detail_scope == ProjectionDetailScope::Full {
                assert_eq!(
                    evidence.detail.as_ref().unwrap()["text"].as_str(),
                    Some(body.as_str())
                );
            } else {
                assert!(evidence.detail.is_none());
            }
            assert!(serde_json::to_vec(&projection).unwrap().len() < 1024 * 1024);
            let (hydrated, full_scans) = backend.history_reads();
            assert_eq!(hydrated, 2);
            assert_eq!(full_scans, 1);
            let checkpoint_id = format!("projection-history:{graph_id}");
            assert!(services
                .event_store()
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .is_none());
            assert_eq!(services.execution_projection_cache_stats().2, 0);
            assert_eq!(
                services.graph_state_store().load(&graph_id).unwrap(),
                source_graph
            );
            let stable = snapshot(&services, &graph_id, &query).await.unwrap();
            assert!(!stable.terminal_axes.source_race);
            assert!(stable.cursor > source_cut);
            assert!(services
                .event_store()
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .is_some());
            let mut denied = query.clone();
            denied.workspace_id = "foreign-workspace-395".into();
            assert!(matches!(
                snapshot(&services, &graph_id, &denied).await,
                Err(RuntimeServicesError::ProjectionAccessDenied)
            ));
            services.shutdown_execution().await;
        }
    }

    #[tokio::test]
    async fn r1_projection_395_real_oversize_still_rejects_without_checkpoint() {
        let (services, backend, graph_id) = r1_history_fixture();
        for sequence in 0..2 {
            services.event_store().append(crate::RuntimeEventInput {
                stream_id: format!("history:{graph_id}"), scope: RuntimeEventScope::Session,
                kind: "model.item_completed".into(), status: None, actor: None,
                refs: vec![crate::RuntimeEventRef { kind: "execution_graph".into(), id: graph_id.clone() }],
                payload: serde_json::json!({"sequence": sequence, "text": "public projection body ".repeat(30_000)}),
            }).unwrap();
        }
        backend.reset_history_reads();
        assert!(matches!(
            snapshot(&services, &graph_id, &context(&services)).await,
            Err(RuntimeServicesError::EventStore(
                crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)
            ))
        ));
        assert!(backend.history_reads().0 <= 2);
        assert!(services
            .event_store()
            .projection_checkpoint(&format!("projection-history:{graph_id}"))
            .unwrap()
            .is_none());
        assert_eq!(services.execution_projection_cache_stats().2, 0);
        services.shutdown_execution().await;
    }

    pub(super) struct Projection412Executor;

    #[async_trait::async_trait]
    impl crate::execution_core::graph::ScopedNodeBackend for Projection412Executor {
        async fn execute(
            &self,
            _ticket: &crate::execution_core::graph::NodeExecutionTicket,
        ) -> Result<
            crate::execution_core::graph::NodeExecutionOutcome,
            crate::execution_core::graph::NodeExecutorError,
        > {
            Ok(crate::execution_core::graph::NodeExecutionOutcome::new(
                ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: None,
                    wait_class: None,
                    summary: Some("deterministic no-effect model transport fixture".into()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    pub(super) struct Projection412Resolver {
        pub(super) ticket: crate::execution_core::graph::NodeExecutionTicket,
        pub(super) backend: std::sync::Arc<Projection412Executor>,
    }

    impl crate::execution_core::graph::executors::ScopedNodeBackendResolver for Projection412Resolver {
        fn resolve(
            &self,
            ticket: &crate::execution_core::graph::NodeExecutionTicket,
        ) -> Option<std::sync::Arc<dyn crate::execution_core::graph::ScopedNodeBackend>> {
            if ticket != &self.ticket {
                return None;
            }
            Some(self.backend.clone())
        }
    }

    fn projection_412_goal(
        services: &RuntimeServices,
        graph_id: &str,
        session_id: &str,
        turn_id: &str,
        kind: &str,
    ) {
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":format!("goal:{graph_id}"),"session_id":session_id,"objective":"412 read-axis authority fixture",
            "criteria":[{"id":"criterion","statement":"source owner test","status":"satisfied"}],
            "phase":"terminal","scope":"internal","completion":kind,"revision":1,"user_sequence":1,
            "spec_revision":1,"spec_digest":"projection-412-goal",
            "execution_binding":{"objective_id":format!("goal:{graph_id}"),"session_id":session_id,"turn_id":turn_id,
                "root_execution_id":graph_id,"agentic_program_id":format!("program:{graph_id}")},
            "terminal":{"kind":kind,"terminal_fence":format!("412-goal:{graph_id}"),"authority_revision":1,
                "reason":"read derivation fixture; not a model acceptance","committed_at_ms":1}
        })).unwrap();
        services.goal_store().create(goal).unwrap();
    }

    #[tokio::test]
    async fn r1_execution_axes_412_no_effect_terminal_is_settled_full_summary_and_delta() {
        let services = RuntimeServices::in_memory().unwrap();
        let mut graph =
            graph_with_lineage("412 pure no-effect", "412-session", "412-turn", "412-task");
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        let expected = crate::execution_core::graph::NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: node.executor_kind.clone(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        let resolver = std::sync::Arc::new(Projection412Resolver {
            ticket: expected.clone(),
            backend: std::sync::Arc::new(Projection412Executor),
        });
        let mut foreign = expected.clone();
        foreign.graph_id.push_str(":foreign");
        assert!(crate::execution_core::graph::executors::ScopedNodeBackendResolver::resolve(
            resolver.as_ref(),
            &foreign,
        )
        .is_none());
        services.model_step_executor().install_resolver(resolver);
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
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
            .unwrap();
        projection_412_goal(&services, &graph_id, "412-session", "412-turn", "satisfied");
        let mut query = context(&services);
        let full = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(
            full.terminal_axes.execution.phase,
            harness_contract::projection::TerminalExecutionPhase::Terminal
        );
        assert_eq!(
            full.terminal_axes.execution.settlement,
            harness_contract::projection::TerminalEffectSettlement::ProvenSettled
        );
        assert!(full.terminal_axes.execution.coverage_complete);
        assert_eq!(
            full.terminal_axes.outcome.outcome,
            Some(harness_contract::goal::ObjectiveTerminalKind::Satisfied)
        );
        assert_eq!(
            full.terminal_axes.delivery.session_client_evidence,
            harness_contract::projection::SessionClientEvidence::RequiresFreshContext
        );
        query.detail_scope = ProjectionDetailScope::Summary;
        let summary = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(full.terminal_axes, summary.terminal_axes);
        let unchanged = delta(
            &services,
            &graph_id,
            summary.revision,
            summary.cursor,
            &query,
        )
        .unwrap();
        assert!(unchanged.operations.is_empty());
        assert!(unchanged.resync_reason.is_none());
        let projection = services.graph_state_store().projection(&graph_id).unwrap();
        let scope =
            ExecutionProjectionScope::load(&services, &graph_id, &projection, true).unwrap();
        let mut facts = history::load_scope(&services, &scope, summary.cursor).unwrap();
        facts.get_mut(&graph_id).unwrap().effect_missing_receipt = true;
        let blocked = execution_terminal_axes(
            &services,
            &projection,
            &scope,
            &facts,
            summary.cursor,
            &mut history::RequestBudget::default(),
        )
        .unwrap();
        assert_eq!(blocked.outcome.outcome, full.terminal_axes.outcome.outcome);
        assert_ne!(
            blocked.execution.phase,
            harness_contract::projection::TerminalExecutionPhase::Terminal
        );
        assert_eq!(
            blocked.execution.settlement,
            harness_contract::projection::TerminalEffectSettlement::Unsettled
        );
        assert!(!blocked.execution.coverage_complete);
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_execution_axes_412_original_lease_generation_and_drop_are_not_absence_guesses() {
        use crate::execution_core::graph::{
            ExecutionResourceKind, ExecutionResourceManager, ResourceQuota, ScopeLockManager,
            ScopeLockMode, ScopeLockRequest, ScopedResource,
        };
        let kind = ExecutionResourceKind::Custom("412-owned".into());
        let manager =
            ExecutionResourceManager::new([(kind.clone(), ResourceQuota::new(1, 1, 1).unwrap())]);
        let lease = manager.acquire(kind.clone(), None).await.unwrap();
        let reference = lease.projection_reference();
        assert_eq!(manager.lease_is_active(&reference).unwrap(), Some(true));
        let replacement =
            ExecutionResourceManager::new([(kind, ResourceQuota::new(1, 1, 1).unwrap())]);
        assert_eq!(replacement.lease_is_active(&reference).unwrap(), None);
        assert_eq!(
            manager.lease_is_active(&lease.id().to_string()).unwrap(),
            None
        );
        drop(lease);
        assert_eq!(manager.lease_is_active(&reference).unwrap(), Some(false));
        let locks = ScopeLockManager::new();
        let lease = locks
            .acquire(
                vec![ScopeLockRequest {
                    scope: ScopedResource::workspace("412-workspace").unwrap(),
                    mode: ScopeLockMode::Write,
                }],
                None,
            )
            .await
            .unwrap();
        let reference = lease.projection_reference();
        assert_eq!(locks.lease_is_active(&reference).unwrap(), Some(true));
        assert_eq!(
            ScopeLockManager::new().lease_is_active(&reference).unwrap(),
            None
        );
        drop(lease);
        assert_eq!(locks.lease_is_active(&reference).unwrap(), Some(false));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn r1_execution_axes_412_real_process_wait_unknown_exit_and_withdrawal() {
        use crate::agentic::execution::reviewed_goal_fixture::PhysicalProcessFixture;
        for unknown in [false, true] {
            let fixture = PhysicalProcessFixture::new(
                crate::config::AgentExecutorSandboxProfile::WorkspaceReadOnly,
                if unknown {
                    "projection-412-unknown"
                } else {
                    "projection-412-reaped"
                },
            )
            .await;
            let gate = fixture
                .services
                .agent_runtime()
                .process_fixture_exit_gate(unknown);
            let worker = fixture
                .dispatch(
                    "projection-412-worker",
                    vec!["read:.".into()],
                    false,
                    false,
                    true,
                )
                .await;
            let graph_id = worker.packet.graph_id().to_string();
            let before = snapshot(&fixture.services, &graph_id, &context(&fixture.services))
                .await
                .unwrap();
            assert_ne!(
                before.terminal_axes.execution.phase,
                harness_contract::projection::TerminalExecutionPhase::Terminal
            );
            worker.withdraw().await;
            let withdrawal = fixture
                .services
                .event_store()
                .event_by_idempotency_key(
                    &format!(
                        "agentic-program:{}",
                        worker.packet.agentic_binding.as_ref().unwrap().program_id
                    ),
                    &format!("agent-action:physical-withdraw:{}", worker.packet.run_id()),
                )
                .unwrap()
                .expect("the original withdrawal must be durably applied");
            let envelope: harness_contract::agent_action::AgentActionEnvelope =
                serde_json::from_value(withdrawal.payload["envelope"].clone())
                    .expect("the durable withdrawal must carry its original envelope");
            assert!(
                matches!(
                    &envelope.action,
                    harness_contract::agent_action::AgentAction::TaskWithdraw(input)
                        if input.task_ref == worker.task_ref
                ),
                "the durable withdrawal must be the original Task retirement"
            );
            let dispatch_context = crate::agentic::execution::AgenticDispatchContext {
                session_id: worker.packet.session_id().to_string(),
                turn_id: envelope.actor.turn_id.clone(),
                model_lease: worker.packet.model_lease.clone(),
                permission_ceiling: worker.packet.permission_ceiling,
                resource_scopes: worker.packet.resource_scopes.clone(),
            };
            let mut dispatch = fixture
                .services
                .dispatch_agentic_followups(&envelope, dispatch_context);
            struct GateRelease(std::sync::Arc<crate::agent_process_jsonl_adapter::ProcessExitGate>);
            impl Drop for GateRelease {
                fn drop(&mut self) {
                    self.0.release();
                }
            }
            let _gate_release = GateRelease(gate.clone());
            let mut dispatched: Option<
                Result<Vec<crate::agentic::execution::AgenticDispatchReceipt>, String>,
            > = None;
            let entered = tokio::time::timeout(std::time::Duration::from_secs(30), async {
                loop {
                    tokio::select! {
                        result = &mut dispatch => {
                            dispatched = Some(result);
                            return gate.entered.load(std::sync::atomic::Ordering::Acquire);
                        }
                        _ = tokio::task::yield_now() => {
                            if gate.entered.load(std::sync::atomic::Ordering::Acquire) {
                                return true;
                            }
                        }
                    }
                }
            })
            .await
            .unwrap_or(false);
            assert!(
                entered,
                "the original withdrawal consumer must cancel the live process before the driver is released"
            );
            assert!(crate::agent_runtime::confirmed_run_exit(
                fixture.services.event_store(),
                &worker.packet
            )
            .unwrap()
            .is_none());
            let pending_program = fixture
                .services
                .agent_action_service()
                .project(&worker.packet.agentic_binding.as_ref().unwrap().program_id)
                .unwrap();
            assert_eq!(
                pending_program.tasks[&worker.task_ref].status,
                crate::AgenticTaskStatus::CancelRequested
            );
            assert!(pending_program.tasks[&worker.task_ref]
                .pending_retirement
                .is_some());
            assert!(!pending_program.tasks[&worker.task_ref]
                .active_attempts
                .is_empty());
            let pending = snapshot(&fixture.services, &graph_id, &context(&fixture.services))
                .await
                .unwrap();
            assert_ne!(
                pending.terminal_axes.execution.phase,
                harness_contract::projection::TerminalExecutionPhase::Terminal
            );
            gate.release();
            let consumer = async {
                match dispatched {
                    Some(result) => result,
                    None => tokio::time::timeout(std::time::Duration::from_secs(30), dispatch)
                        .await
                        .expect("the original withdrawal consumer must finish"),
                }
            };
            let (consumer_result, terminal_result) = tokio::join!(
                consumer,
                tokio::time::timeout(std::time::Duration::from_secs(30), worker.wait_terminal()),
            );
            terminal_result.expect("the original driver must reach its terminal state");
            assert!(
                consumer_result.is_ok(),
                "the original withdrawal consumer must apply the durable cancellation: {consumer_result:?}"
            );
            let turn_id = fixture
                .services
                .graph_state_store()
                .load(&graph_id)
                .unwrap()
                .lineage
                .unwrap()
                .turn_id;
            projection_412_goal(
                &fixture.services,
                &graph_id,
                worker.packet.session_id(),
                &turn_id,
                "cancelled",
            );
            let mut last_axes = None;
            let settled_result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
                loop {
                    let projection =
                        snapshot(&fixture.services, &graph_id, &context(&fixture.services))
                            .await
                            .unwrap();
                    if unknown
                        || projection.terminal_axes.execution.phase
                            == harness_contract::projection::TerminalExecutionPhase::Terminal
                    {
                        break projection;
                    }
                    last_axes = Some(serde_json::json!(&projection.terminal_axes));
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let settled = settled_result.unwrap_or_else(|error| {
                let task = fixture.services.agent_action_service()
                    .project(&worker.packet.agentic_binding.as_ref().unwrap().program_id)
                    .ok().and_then(|program| program.tasks.get(&worker.task_ref).cloned());
                let graph = fixture.services.graph_state_store().load(&graph_id).ok();
                panic!("physical retirement did not converge: {error}; {}", serde_json::json!({
                    "run_id": worker.packet.run_id(), "attempt": worker.packet.attempt,
                    "unknown": unknown, "last_axes": last_axes,
                    "node_statuses": graph.map(|graph| graph.node_statuses),
                    "task": task.map(|task| serde_json::json!({
                        "status": task.status, "pending_retirement": task.pending_retirement,
                        "active_attempts": task.active_attempts, "claim_generation": task.claim_generation,
                    })),
                    "exact_exit": format!("{:?}", crate::agent_runtime::confirmed_run_exit(
                        fixture.services.event_store(), &worker.packet).map(|exit| exit.is_some())),
                }));
            });
            assert_eq!(
                settled.terminal_axes.outcome.outcome,
                Some(harness_contract::goal::ObjectiveTerminalKind::Cancelled)
            );
            if unknown {
                assert_ne!(
                    settled.terminal_axes.execution.phase,
                    harness_contract::projection::TerminalExecutionPhase::Terminal
                );
                assert_ne!(
                    settled.terminal_axes.execution.settlement,
                    harness_contract::projection::TerminalEffectSettlement::ProvenSettled
                );
            } else {
                assert_eq!(
                    settled.terminal_axes.execution.settlement,
                    harness_contract::projection::TerminalEffectSettlement::ProvenSettled
                );
                assert!(settled.terminal_axes.execution.coverage_complete);
                let binding = worker.packet.agentic_binding.as_ref().unwrap();
                let program = fixture
                    .services
                    .agent_action_service()
                    .project(&binding.program_id)
                    .unwrap();
                assert_eq!(
                    program.tasks[&worker.task_ref].status,
                    crate::AgenticTaskStatus::Withdrawn
                );
                assert_ne!(
                    program.tasks[&worker.task_ref].status,
                    crate::AgenticTaskStatus::Accepted
                );
                assert!(program.tasks[&worker.task_ref].pending_retirement.is_none());
                assert!(program.tasks[&worker.task_ref].active_attempts.is_empty());
                let diff = delta(
                    &fixture.services,
                    &graph_id,
                    settled.revision,
                    settled.cursor,
                    &context(&fixture.services),
                )
                .unwrap();
                assert!(diff.resync_reason.is_some());
            }
            fixture.services.shutdown_execution().await;
        }
    }

    struct BindingHistoryExecutor {
        polls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::execution_core::graph::NodeExecutor for BindingHistoryExecutor {
        fn kind(&self) -> &str {
            "binding_history_oracle"
        }

        fn validate(
            &self,
            _node: &ExecutionNodeSpec,
        ) -> Result<(), crate::execution_core::graph::NodeExecutorError> {
            Ok(())
        }

        async fn start(
            &self,
            context: crate::execution_core::graph::NodeExecutionContext,
        ) -> Result<
            crate::execution_core::graph::NodeExecutionTicket,
            crate::execution_core::graph::NodeExecutorError,
        > {
            Ok(crate::execution_core::graph::NodeExecutionTicket {
                graph_id: context.graph.id.clone(),
                node_id: context.node.id,
                executor_kind: "binding_history_oracle".into(),
                service_class: context.graph.service_class,
                attempt: context.attempt,
                idempotency_key: context.node.idempotency_key,
                payload_ref: context.node.payload_ref,
            })
        }

        async fn poll_or_await(
            &self,
            _ticket: &crate::execution_core::graph::NodeExecutionTicket,
        ) -> Result<
            crate::execution_core::graph::NodeExecutionOutcome,
            crate::execution_core::graph::NodeExecutorError,
        > {
            self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::execution_core::graph::NodeExecutionOutcome::new(
                ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: None,
                    wait_class: None,
                    summary: Some("real deterministic poll".into()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    #[tokio::test]
    async fn r1_bounded_history_unique_binding_cannot_hide_effect_debt() {
        use crate::execution_core::graph::{ExecutionGraphEvent, ExecutionNodeBinding};
        let services = RuntimeServices::in_memory().unwrap();
        let executor = std::sync::Arc::new(BindingHistoryExecutor {
            polls: std::sync::atomic::AtomicUsize::new(0),
        });
        services
            .executor_registry()
            .register(executor.clone())
            .unwrap();
        let mut original = graph_with_lineage(
            "unique historical start",
            "binding-session",
            "binding-turn",
            "binding-task",
        );
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "binding_history_oracle",
            "history-payload",
        );
        original
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        original.nodes.push(node.clone());
        let graph_id = original.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                original,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(executor.polls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let graph = services.graph_state_store().projection(&graph_id).unwrap();
        let scope = ExecutionProjectionScope::load(&services, &graph_id, &graph, true).unwrap();
        let root_scope = activity::execution_scope(&services, &scope, &graph);
        let records = services.event_store().list_stream(&graph_id).unwrap();
        let mut prefix = history::HistoryState::default();
        let mut unique = None;
        for record in &records {
            let event = services
                .event_store()
                .resolve_read_record(record.clone())
                .unwrap();
            let value: ExecutionGraphEvent = serde_json::from_value(event.payload.clone()).unwrap();
            let bindings = match &value {
                ExecutionGraphEvent::Checkpoint { bindings, .. }
                | ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.clone(),
                ExecutionGraphEvent::NodeTransitioned {
                    node_id,
                    binding: Some(binding),
                    ..
                } => BTreeMap::from([(node_id.clone(), binding.clone())]),
                _ => BTreeMap::new(),
            };
            if let Some(binding) = bindings.get(&node.id) {
                assert!(
                    unique.is_none(),
                    "only one canonical start, not a duplicate-conflict oracle"
                );
                unique = Some((
                    prefix.clone(),
                    record.clone(),
                    value.clone(),
                    binding.clone(),
                ));
            }
            prefix.fold(&event, &graph, &root_scope).unwrap();
        }
        let (before, record, value, binding) = unique.expect("Runner produced a start");
        let slot = format!(
            "{}:{}:{}",
            node.id, binding.attempt, binding.ticket_idempotency_key
        );
        let ticket = prefix.start_tickets.get(&slot).unwrap().clone();
        let effect_stream = ExecutionNodeBinding::effect_stream_id(&ticket);
        let intent = services
            .event_store()
            .event_by_idempotency_key(
                &effect_stream,
                &format!("{}:intent", binding.ticket_idempotency_key),
            )
            .unwrap()
            .unwrap();
        let receipt = services
            .event_store()
            .event_by_idempotency_key(
                &effect_stream,
                &format!("{}:receipt", binding.ticket_idempotency_key),
            )
            .unwrap()
            .unwrap();
        let stored_ticket: crate::execution_core::graph::NodeExecutionTicket =
            serde_json::from_value(intent.payload.clone()).unwrap();
        assert_eq!(stored_ticket, ticket);
        let through = services.event_store().durable_commit_cursor().unwrap();
        assert!(ExecutionNodeBinding::validate_effect_records(
            &ticket,
            Some(&intent),
            Some(&receipt),
            through
        )
        .unwrap()
        .is_some());
        let mut retry = ticket.clone();
        retry.attempt = retry.attempt.checked_add(1).unwrap();
        assert_ne!(
            ExecutionNodeBinding::effect_stream_id(&retry),
            effect_stream
        );
        assert!(ExecutionNodeBinding::validate_effect_records(
            &retry,
            Some(&intent),
            Some(&receipt),
            through
        )
        .is_err());
        assert!(ExecutionNodeBinding::validate_effect_records(
            &ticket,
            Some(&intent),
            None,
            through
        )
        .unwrap()
        .is_none());
        assert!(ExecutionNodeBinding::validate_effect_records(
            &ticket,
            None,
            Some(&receipt),
            through
        )
        .is_err());
        let mut effect_fault_hits = 0;
        for field in [
            "graph_id",
            "node_id",
            "payload_ref",
            "service_class",
            "idempotency_key",
            "attempt",
        ] {
            let mut bad = intent.clone();
            bad.payload[field] = if field == "attempt" {
                serde_json::json!(0)
            } else if field == "service_class" {
                serde_json::json!("background")
            } else {
                serde_json::json!("foreign")
            };
            assert!(
                ExecutionNodeBinding::validate_effect_records(
                    &ticket,
                    Some(&bad),
                    Some(&receipt),
                    through
                )
                .is_err(),
                "intent identity fault-hit {field}"
            );
            effect_fault_hits += 1;
        }
        for field in [
            "schema", "kind", "scope", "actor", "refs", "key", "payload", "order",
        ] {
            let mut bad = receipt.clone();
            match field {
                "schema" => bad.schema_version += 1,
                "kind" => bad.kind = "execution.effect.intent".into(),
                "scope" => bad.scope = RuntimeEventScope::Session,
                "actor" => bad.actor = Some("foreign".into()),
                "refs" => bad.refs[0].id = "foreign-node".into(),
                "key" => bad.idempotency_key = Some("foreign:receipt".into()),
                "payload" => bad.payload = serde_json::json!({"result": "bad-result"}),
                _ => {
                    bad.commit_cursor = intent.commit_cursor;
                    bad.transaction_index = intent.transaction_index;
                }
            }
            assert!(
                ExecutionNodeBinding::validate_effect_records(
                    &ticket,
                    Some(&intent),
                    Some(&bad),
                    through
                )
                .is_err(),
                "receipt source fault-hit {field}"
            );
            effect_fault_hits += 1;
        }
        assert_eq!(effect_fault_hits, 14);
        let mut missing_class = intent.clone();
        missing_class
            .payload
            .as_object_mut()
            .unwrap()
            .remove("service_class");
        assert!(
            ExecutionNodeBinding::validate_effect_records(
                &ticket,
                Some(&missing_class),
                Some(&receipt),
                through
            )
            .unwrap_err()
            .contains("field missing"),
            "required service_class field fault-hit"
        );
        assert!(binding.resource_lease_refs.is_empty());
        assert!(binding.scope_lease_ref.is_none() && binding.worktree_lease_ref.is_none());
        let mut fault_hits = 0;
        for field in ["attempt", "ticket_idempotency_key", "executor_kind"] {
            let mut changed = serde_json::to_value(&value).unwrap();
            let replacement = match field {
                "attempt" => serde_json::json!(0),
                "ticket_idempotency_key" => serde_json::json!(""),
                _ => serde_json::json!("foreign-executor"),
            };
            if changed["binding"].is_object() {
                changed["binding"][field] = replacement;
            } else {
                changed["bindings"][&node.id][field] = replacement;
            }
            let mut corrupted = record.clone();
            corrupted.payload = services
                .event_store()
                .prepare_content(
                    "session:binding-session",
                    "unique-binding-negative",
                    &changed,
                    &crate::PreparedArtifactOrigin::EventBody {
                        stream_id: record.stream_id.clone(),
                        idempotency_key: record.idempotency_key.clone().unwrap(),
                        kind: record.kind.clone(),
                    },
                )
                .unwrap();
            corrupted.payload["_runtime_activity_binding"] =
                record.payload["_runtime_activity_binding"].clone();
            let resolved = services
                .event_store()
                .resolve_read_record(corrupted)
                .unwrap();
            let mut candidate = before.clone();
            assert!(
                candidate.fold(&resolved, &graph, &root_scope).is_err(),
                "first-binding fault-hit {field}"
            );
            fault_hits += 1;
        }
        assert_eq!(fault_hits, 3);
        let query = context(&services);
        let cold = snapshot(&services, &graph_id, &query).await.unwrap();
        let hot = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(cold, hot);
        let checkpoint_id = format!("projection-history:{graph_id}");
        let saved = services
            .event_store()
            .projection_checkpoint(&checkpoint_id)
            .unwrap()
            .unwrap();
        let mut old_cache = saved.payload.clone();
        old_cache["schema"] = serde_json::json!(2);
        assert!(services
            .event_store()
            .compare_and_repair_projection_checkpoint(
                &checkpoint_id,
                saved.source_cursor,
                saved.revision,
                &old_cache,
                0
            )
            .is_err());
        services
            .event_store()
            .inject_content_checkpoint_fault(
                &checkpoint_id,
                saved.source_cursor,
                saved.revision,
                &old_cache,
            )
            .unwrap();
        let after_old_cache =
            history::load(&services, &scope, &graph, saved.source_cursor).unwrap();
        assert_eq!(after_old_cache.bindings.len(), prefix.bindings.len());
        assert_eq!(
            services
                .event_store()
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap()
                .payload["schema"],
            6
        );
        services
            .event_store()
            .delete_projection_checkpoint(&format!("projection-history:{graph_id}"))
            .unwrap();
        let rebuilt = history::load(
            &services,
            &scope,
            &graph,
            services.event_store().durable_commit_cursor().unwrap(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(prefix.bindings).unwrap(),
            serde_json::to_value(rebuilt.bindings).unwrap()
        );
        for bad_source in ["missing", "foreign"] {
            let mut pending = graph_with_lineage(
                "required intent debt",
                "binding-session",
                "binding-turn",
                "binding-task",
            );
            let pending_node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "binding_history_oracle",
                "pending-payload",
            );
            pending
                .node_statuses
                .insert(pending_node.id.clone(), ExecutionNodeStatus::Planned);
            pending.nodes.push(pending_node.clone());
            let registered = services
                .commit_service()
                .register_graph(pending)
                .unwrap()
                .graph;
            let ready = services
                .commit_service()
                .transition_node(
                    &registered,
                    &pending_node.id,
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            let start = ExecutionNodeBinding {
                executor_kind: pending_node.executor_kind.clone(),
                attempt: 1,
                ticket_idempotency_key: if bad_source == "foreign" {
                    binding.ticket_idempotency_key.clone()
                } else {
                    format!("missing:{}", pending_node.id)
                },
                resource_lease_refs: Vec::new(),
                scope_lease_ref: None,
                worktree_lease_ref: None,
            };
            let running = services
                .commit_service()
                .bind_and_start_node(&ready, &pending_node.id, start)
                .unwrap()
                .graph;
            let error = snapshot(&services, &running.id, &query).await.unwrap_err();
            assert!(
                error.to_string().contains("effect"),
                "required-intent fault-hit {bad_source}: {error}"
            );
            assert!(services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", running.id))
                .unwrap()
                .is_none());
        }
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_bounded_history_checkpoint_start_is_validated_before_empty_successor() {
        use crate::execution_core::graph::{
            ExecutionGraphEvent, ExecutionNodeBinding, NodeExecutor,
        };
        let services = RuntimeServices::in_memory().unwrap();
        let executor = BindingHistoryExecutor {
            polls: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut graph = graph_with_lineage(
            "checkpoint-only start",
            "checkpoint-session",
            "checkpoint-turn",
            "checkpoint-task",
        );
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "binding_history_oracle",
            "checkpoint-payload",
        );
        graph.nodes.push(node.clone());
        graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        while graph.revision < 63 {
            graph = services
                .commit_service()
                .apply_command(
                    &graph,
                    &ExecutionGraphCommand::Start {
                        expected_revision: graph.revision,
                    },
                )
                .unwrap()
                .graph;
        }
        let ticket = executor
            .start(crate::execution_core::graph::NodeExecutionContext {
                graph: std::sync::Arc::new(graph.clone()),
                node: node.clone(),
                attempt: 1,
            })
            .await
            .unwrap();
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: ticket.attempt,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        graph = services
            .commit_service()
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([(node.id.clone(), binding.clone())]),
                vec![ticket],
            )
            .unwrap()
            .graph;
        let start_record = services
            .event_store()
            .latest_for_stream(&graph.id)
            .unwrap()
            .unwrap();
        let start_event = services
            .event_store()
            .resolve_read_record(start_record.clone())
            .unwrap();
        let start_value: ExecutionGraphEvent =
            serde_json::from_value(start_event.payload.clone()).unwrap();
        assert!(
            matches!(&start_value, ExecutionGraphEvent::Checkpoint { bindings, .. } if bindings.get(&node.id) == Some(&binding))
        );
        let projection = services.graph_state_store().projection(&graph.id).unwrap();
        let scope =
            ExecutionProjectionScope::load(&services, &graph.id, &projection, true).unwrap();
        let root_scope = activity::execution_scope(&services, &scope, &projection);
        let mut state = history::HistoryState::default();
        for record in services.event_store().list_stream(&graph.id).unwrap() {
            if record.sequence == start_record.sequence {
                break;
            }
            let resolved = services.event_store().resolve_read_record(record).unwrap();
            state.fold(&resolved, &projection, &root_scope).unwrap();
        }
        let mut hits = 0;
        for field in ["attempt", "ticket_idempotency_key", "executor_kind"] {
            let mut altered = serde_json::to_value(&start_value).unwrap();
            altered["bindings"][&node.id][field] = match field {
                "attempt" => serde_json::json!(0),
                "ticket_idempotency_key" => serde_json::json!(""),
                _ => serde_json::json!("foreign"),
            };
            let mut record = start_record.clone();
            record.payload = services
                .event_store()
                .prepare_content(
                    "session:checkpoint-session",
                    "checkpoint-binding-negative",
                    &altered,
                    &crate::PreparedArtifactOrigin::EventBody {
                        stream_id: record.stream_id.clone(),
                        idempotency_key: record.idempotency_key.clone().unwrap(),
                        kind: record.kind.clone(),
                    },
                )
                .unwrap();
            record.payload["_runtime_activity_binding"] =
                start_record.payload["_runtime_activity_binding"].clone();
            let resolved = services.event_store().resolve_read_record(record).unwrap();
            assert!(
                state
                    .clone()
                    .fold(&resolved, &projection, &root_scope)
                    .is_err(),
                "checkpoint first-start fault-hit {field}"
            );
            hits += 1;
        }
        assert_eq!(hits, 3);
        state.fold(&start_event, &projection, &root_scope).unwrap();
        for _ in 0..64 {
            graph = services
                .commit_service()
                .apply_command(
                    &graph,
                    &ExecutionGraphCommand::Start {
                        expected_revision: graph.revision,
                    },
                )
                .unwrap()
                .graph;
            let record = services
                .event_store()
                .latest_for_stream(&graph.id)
                .unwrap()
                .unwrap();
            let resolved = services.event_store().resolve_read_record(record).unwrap();
            if let ExecutionGraphEvent::Checkpoint { bindings, .. } =
                serde_json::from_value::<ExecutionGraphEvent>(resolved.payload.clone()).unwrap()
            {
                assert!(bindings.is_empty());
            }
            state.fold(&resolved, &projection, &root_scope).unwrap();
        }
        assert_eq!(state.bindings.len(), 1);
        assert_eq!(executor.polls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let snapshot = snapshot(&services, &graph.id, &context(&services))
            .await
            .unwrap();
        assert_eq!(
            snapshot.terminal_axes.execution.settlement,
            harness_contract::projection::TerminalEffectSettlement::Unknown
        );
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_bounded_history_projection_request_does_not_accumulate_all_pages() {
        let (services, backend, graph_id) = r1_history_fixture();
        let original_plan = services.graph_state_store().load(&graph_id).unwrap();
        for sequence in 0..1000 {
            services.event_store().append(crate::RuntimeEventInput {stream_id:format!("history:{graph_id}"),scope:RuntimeEventScope::Session,kind:"model.item_completed".into(),status:None,actor:None,
                refs:vec![crate::RuntimeEventRef {kind:"execution_graph".into(),id:graph_id.clone()}],payload:serde_json::json!({"sequence":sequence,"text":"public observable text ".repeat(2048)})}).unwrap();
        }
        backend.reset_history_reads();
        match snapshot(&services, &graph_id, &context(&services)).await {
            Err(error) => assert!(matches!(
                error,
                RuntimeServicesError::EventStore(
                    crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)
                )
            )),
            Ok(projection) => {
                assert_eq!(projection.graph.graph_id, original_plan.id);
                assert_eq!(projection.graph.revision, original_plan.revision);
                assert_eq!(
                    projection.terminal_axes.execution.settlement,
                    harness_contract::projection::TerminalEffectSettlement::Unknown
                );
                assert_ne!(
                    projection.terminal_axes.outcome.outcome,
                    Some(harness_contract::goal::ObjectiveTerminalKind::Satisfied)
                );
            }
        }
        let (hydrated, full_scans) = backend.history_reads();
        assert!(
            hydrated > 0 && hydrated < 200,
            "bounded failure before all 1000 pages/events: {hydrated}"
        );
        assert_eq!(
            full_scans, 1,
            "only the current scope lookup, not a full history scan"
        );
        assert_eq!(
            services.graph_state_store().load(&graph_id).unwrap(),
            original_plan
        );
        assert!(services
            .event_store()
            .projection_checkpoint(&format!("projection-history:{graph_id}"))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn r1_703_long_history_full_hot_and_durable_reload_preserve_axes_and_bounds() {
        let (services, backend, graph_id) = r1_history_fixture();
        let query = context(&services);
        let mut latest = snapshot(&services, &graph_id, &query).await.unwrap();
        for batch in 0..3 {
            for sequence in 0..100 {
                services.event_store().append(crate::RuntimeEventInput {
                    stream_id: format!("history:{graph_id}"),
                    scope: RuntimeEventScope::Session,
                    kind: "model.item_completed".into(),
                    status: None,
                    actor: None,
                    refs: vec![crate::RuntimeEventRef { kind: "execution_graph".into(), id: graph_id.clone() }],
                    payload: serde_json::json!({"sequence":batch * 100 + sequence,"text":"public current state ".repeat(6)}),
                }).unwrap();
            }
            latest = snapshot(&services, &graph_id, &query).await.unwrap();
        }
        assert_eq!(snapshot(&services, &graph_id, &query).await.unwrap(), latest);
        let graph = services.graph_state_store().projection(&graph_id).unwrap();
        let scope = ExecutionProjectionScope::load(&services, &graph_id, &graph, true).unwrap();
        backend.reset_history_reads();
        let facts = history::load_scope(&services, &scope, latest.cursor).unwrap();
        assert_eq!(backend.history_reads().0, 0);
        let mut budget = history::RequestBudget::default();
        let axes = execution_terminal_axes(&services, &graph, &scope, &facts, latest.cursor, &mut budget).unwrap();
        assert_eq!(axes, latest.terminal_axes);
        let saved = services.event_store().projection_checkpoint(&format!("projection-history:{graph_id}")).unwrap().unwrap();
        assert_eq!(saved.payload["schema"], 6);
        assert!(serde_json::to_vec(&latest).unwrap().len() <= 1024 * 1024);
        services.event_store().delete_projection_checkpoint(&format!("projection-history:{graph_id}")).unwrap();
        assert!(matches!(history::load(&services, &scope, &graph, latest.cursor), Err(RuntimeServicesError::EventStore(crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)))));
        assert!(services.event_store().projection_checkpoint(&format!("projection-history:{graph_id}")).unwrap().is_none());
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_bounded_history_projection_cursor_scope_and_oversize_are_honest() {
        let (services, backend, graph_id) = r1_history_fixture();
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query).await.unwrap();
        backend.reset_history_reads();
        let repeated = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(initial.cursor, repeated.cursor);
        assert_eq!(backend.history_reads(), (0, 1));
        let future = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor + 100,
            &query,
        )
        .unwrap();
        assert!(future.resync_reason.is_some());
        assert!(future.operations.is_empty());
        let mut foreign = query.clone();
        foreign.workspace_id = "foreign-workspace".into();
        assert!(snapshot(&services, &graph_id, &foreign).await.is_err());
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: format!("history:{graph_id}"),
                scope: RuntimeEventScope::Session,
                kind: "model.item_completed".into(),
                status: None,
                actor: None,
                refs: vec![crate::RuntimeEventRef {
                    kind: "execution_graph".into(),
                    id: graph_id.clone(),
                }],
                payload: serde_json::json!({"text":"x".repeat(2*1024*1024)}),
            })
            .unwrap();
        backend.reset_history_reads();
        assert!(matches!(
            snapshot(&services, &graph_id, &query).await,
            Err(RuntimeServicesError::EventStore(
                crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)
            ))
        ));
        let (hydrated, full_scans) = backend.history_reads();
        assert!(
            hydrated <= 1,
            "oversized external content must fail on its first bounded record"
        );
        assert_eq!(
            full_scans, 1,
            "only the current scope lookup may use list_stream"
        );
    }

    mod budget_adversarial_173 {
        use super::*;

        fn assert_budget_error<ResultValue>(result: Result<ResultValue, RuntimeServicesError>) {
            assert!(matches!(
                result,
                Err(RuntimeServicesError::EventStore(
                    crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)
                ))
            ));
        }

        fn register_history(
            services: &RuntimeServices,
            graph_id: &str,
            parent: Option<ExecutionParentBinding>,
            revision: u64,
        ) -> ExecutionGraph {
            let mut graph = graph_with_lineage(
                "budget adversarial 173",
                "budget173-session",
                "budget173-turn",
                "budget173-task",
            );
            graph.id = graph_id.into();
            graph.parent_execution = parent;
            graph = services
                .commit_service()
                .register_graph(graph)
                .unwrap()
                .graph;
            while graph.revision < revision {
                graph = services
                    .commit_service()
                    .apply_command(
                        &graph,
                        &ExecutionGraphCommand::Start {
                            expected_revision: graph.revision,
                        },
                    )
                    .unwrap()
                    .graph;
            }
            graph
        }

        fn reserved_budget(records: usize) -> history::RequestBudget {
            let mut budget = history::RequestBudget::default();
            budget.charge(MAX_DELTA_BATCHES - records, 0).unwrap();
            budget
        }

        #[tokio::test]
        async fn root_fits_child_fits_shared_request_fails_without_checkpoints() {
            let services = RuntimeServices::in_memory().unwrap();
            let root = register_history(&services, "budget173-root", None, 140);
            let child = register_history(
                &services,
                "budget173-zchild",
                Some(ExecutionParentBinding {
                    execution_id: root.id.clone(),
                    node_id: "budget173-parent".into(),
                }),
                140,
            );
            let root_graph = services.graph_state_store().projection(&root.id).unwrap();
            let child_graph = services.graph_state_store().projection(&child.id).unwrap();
            let through = services.event_store().durable_commit_cursor().unwrap();
            let mut root_only =
                ExecutionProjectionScope::load(&services, &root.id, &root_graph, true).unwrap();
            assert_eq!(
                root_only.execution_ids,
                BTreeSet::from([root.id.clone(), child.id.clone()])
            );
            assert_eq!(root_only.execution_ids.first(), Some(&root.id));
            root_only
                .execution_ids
                .retain(|execution| execution == &root.id);
            let child_only =
                ExecutionProjectionScope::load(&services, &child.id, &child_graph, true).unwrap();
            assert_eq!(child_only.execution_ids, BTreeSet::from([child.id.clone()]));
            for scope in [&root_only, &child_only] {
                let mut independent = history::RequestBudget::default();
                let facts = history::load_scope_budgeted(
                    &services,
                    scope,
                    through,
                    false,
                    &mut independent,
                )
                .unwrap();
                assert_eq!(facts.len(), 1);
                assert_eq!(
                    facts.values().next().unwrap().effects_checked_through,
                    Some(through)
                );
            }
            let combined =
                ExecutionProjectionScope::load(&services, &root.id, &root_graph, true).unwrap();
            let mut shared = history::RequestBudget::default();
            assert_budget_error(history::load_scope_budgeted(
                &services,
                &combined,
                through,
                false,
                &mut shared,
            ));
            assert_budget_error(shared.publish(&services));
            for graph in [&root, &child] {
                assert!(services
                    .event_store()
                    .projection_checkpoint(&format!("projection-history:{}", graph.id),)
                    .unwrap()
                    .is_none());
                assert_eq!(
                    services.graph_state_store().load(&graph.id).unwrap(),
                    *graph
                );
            }
            assert_budget_error(history::load_scope(&services, &combined, through));
            assert!(services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", root.id),)
                .unwrap()
                .is_none());
            services.shutdown_execution().await;
        }

        #[tokio::test]
        async fn repeated_scope_attempt_spends_same_budget_and_exhaustion_is_sticky() {
            let services = RuntimeServices::in_memory().unwrap();
            let graph = register_history(&services, "budget173-retry", None, 140);
            let projection = services.graph_state_store().projection(&graph.id).unwrap();
            let scope =
                ExecutionProjectionScope::load(&services, &graph.id, &projection, true).unwrap();
            let through = services.event_store().durable_commit_cursor().unwrap();
            let mut shared = history::RequestBudget::default();
            let first =
                history::load_scope_budgeted(&services, &scope, through, false, &mut shared)
                    .unwrap();
            assert_eq!(first[&graph.id].graph_state.as_ref().unwrap().revision, 140);
            assert!(services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", graph.id),)
                .unwrap()
                .is_none());
            assert_budget_error(history::load_scope_budgeted(
                &services,
                &scope,
                through,
                false,
                &mut shared,
            ));
            assert_budget_error(shared.charge(0, 0));
            assert_budget_error(history::load_scope_budgeted(
                &services,
                &scope,
                through,
                false,
                &mut shared,
            ));
            assert_budget_error(shared.publish(&services));
            assert!(services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", graph.id),)
                .unwrap()
                .is_none());
            let mut independent = history::RequestBudget::default();
            let retried =
                history::load_scope_budgeted(&services, &scope, through, false, &mut independent)
                    .unwrap();
            assert_eq!(
                serde_json::to_value(&first[&graph.id]).unwrap(),
                serde_json::to_value(&retried[&graph.id]).unwrap(),
            );
            independent.publish(&services).unwrap();
            assert!(services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", graph.id),)
                .unwrap()
                .is_some());
            assert_budget_error(shared.publish(&services));
            services.shutdown_execution().await;
        }

        #[tokio::test]
        async fn checkpoint_restore_and_tail_share_allowance_and_preserve_old_checkpoint() {
            let services = RuntimeServices::in_memory().unwrap();
            let mut graph = register_history(&services, "budget173-tail", None, 1);
            let projection = services.graph_state_store().projection(&graph.id).unwrap();
            let scope =
                ExecutionProjectionScope::load(&services, &graph.id, &projection, true).unwrap();
            let old_cut = services.event_store().durable_commit_cursor().unwrap();
            history::load(&services, &scope, &projection, old_cut).unwrap();
            let checkpoint_id = format!("projection-history:{}", graph.id);
            let saved = services
                .event_store()
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap();
            let mut restore_only = reserved_budget(8);
            let restored =
                history::load_scope_budgeted(&services, &scope, old_cut, false, &mut restore_only)
                    .unwrap();
            assert_eq!(restored[&graph.id].through, saved.source_cursor);
            for _ in 0..6 {
                graph = services
                    .commit_service()
                    .apply_command(
                        &graph,
                        &ExecutionGraphCommand::Start {
                            expected_revision: graph.revision,
                        },
                    )
                    .unwrap()
                    .graph;
            }
            let through = services.event_store().durable_commit_cursor().unwrap();
            let projection = services.graph_state_store().projection(&graph.id).unwrap();
            let scope =
                ExecutionProjectionScope::load(&services, &graph.id, &projection, true).unwrap();
            let tail = services
                .event_store()
                .list_stream(&graph.id)
                .unwrap()
                .into_iter()
                .filter(|record| record.commit_cursor > old_cut)
                .collect::<Vec<_>>();
            assert_eq!(tail.len(), 6);
            let mut tail_only = reserved_budget(8);
            for record in tail {
                tail_only.resolve_record(&services, record).unwrap();
            }
            let mut shared = reserved_budget(8);
            assert_budget_error(history::load_scope_budgeted(
                &services,
                &scope,
                through,
                false,
                &mut shared,
            ));
            assert_budget_error(shared.publish(&services));
            let unchanged = services
                .event_store()
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(&unchanged).unwrap(),
                serde_json::to_value(&saved).unwrap()
            );
            let mut sufficient = history::RequestBudget::default();
            let complete =
                history::load_scope_budgeted(&services, &scope, through, false, &mut sufficient)
                    .unwrap();
            assert_eq!(
                complete[&graph.id].graph_state.as_ref().unwrap().revision,
                graph.revision
            );
            sufficient.publish(&services).unwrap();
            let advanced = services
                .event_store()
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap();
            assert_eq!(advanced.source_cursor, through);
            assert!(advanced.revision > saved.revision);
            services.shutdown_execution().await;
        }

        #[tokio::test]
        async fn declared_graph_body_oversize_is_typed_before_resolver_and_stays_exhausted() {
            let services = RuntimeServices::in_memory().unwrap();
            let graph = register_history(&services, "budget173-declared", None, 1);
            let mut record = services
                .event_store()
                .latest_for_stream(&graph.id)
                .unwrap()
                .unwrap();
            let mut body = services
                .event_store()
                .resolve_read_record(record.clone())
                .unwrap()
                .payload;
            body.as_object_mut()
                .unwrap()
                .remove("_runtime_activity_binding");
            body["budget173_padding"] = serde_json::json!("public content ".repeat(4096));
            let binding = record.payload["_runtime_activity_binding"].clone();
            record.payload = services
                .event_store()
                .prepare_content(
                    "session:budget173-session",
                    "budget173-declared-content",
                    &body,
                    &crate::PreparedArtifactOrigin::EventBody {
                        stream_id: record.stream_id.clone(),
                        idempotency_key: record.idempotency_key.clone().unwrap(),
                        kind: record.kind.clone(),
                    },
                )
                .unwrap();
            record.payload["_runtime_activity_binding"] = binding;
            assert_eq!(record.payload["body"]["kind"], "artifact_ref");
            let mut control = history::RequestBudget::default();
            control.resolve_record(&services, record.clone()).unwrap();
            let mut oversized = record.clone();
            oversized.payload["bytes"] = serde_json::json!(2 * 1024 * 1024);
            let mut budget = history::RequestBudget::default();
            assert_budget_error(budget.resolve_record(&services, oversized));
            assert_budget_error(budget.resolve_record(&services, record.clone()));
            assert_budget_error(budget.publish(&services));
            let mut independent = history::RequestBudget::default();
            independent.resolve_record(&services, record).unwrap();
            assert!(services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", graph.id),)
                .unwrap()
                .is_none());
            services.shutdown_execution().await;
        }

        #[tokio::test]
        async fn legitimate_128_revision_start_and_completed_effect_survive_cold_and_restore() {
            let services = RuntimeServices::in_memory().unwrap();
            let executor = std::sync::Arc::new(BindingHistoryExecutor {
                polls: std::sync::atomic::AtomicUsize::new(0),
            });
            services
                .executor_registry()
                .register(executor.clone())
                .unwrap();
            let mut graph = graph_with_lineage(
                "budget173 legitimate effect",
                "binding-session",
                "binding-turn",
                "binding-task",
            );
            let node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "binding_history_oracle",
                "budget173-payload",
            );
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
            graph.nodes.push(node);
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
                .unwrap();
            graph = services.graph_state_store().load(&graph_id).unwrap();
            while graph.revision < 128 {
                graph = services
                    .commit_service()
                    .apply_command(
                        &graph,
                        &ExecutionGraphCommand::Start {
                            expected_revision: graph.revision,
                        },
                    )
                    .unwrap()
                    .graph;
            }
            assert_eq!(graph.revision, 128);
            let projection = services.graph_state_store().projection(&graph_id).unwrap();
            let scope =
                ExecutionProjectionScope::load(&services, &graph_id, &projection, true).unwrap();
            let through = services.event_store().durable_commit_cursor().unwrap();
            let cold = history::load(&services, &scope, &projection, through).unwrap();
            assert_eq!(cold.bindings.len(), 1);
            assert_eq!(cold.start_tickets.len(), 1);
            assert_eq!(cold.start_specs.len(), 1);
            assert_eq!(cold.start_positions.len(), 1);
            assert!(!cold.effect_missing_receipt);
            assert!(!cold.effect_evidence_refs.is_empty());
            assert_eq!(cold.effects_checked_through, Some(through));
            let restored = history::load(&services, &scope, &projection, through).unwrap();
            assert_eq!(
                serde_json::to_value(&cold).unwrap(),
                serde_json::to_value(&restored).unwrap()
            );
            assert_eq!(restored.effect_evidence_refs, cold.effect_evidence_refs);
            assert!(!restored.effect_missing_receipt);
            assert_eq!(restored.effects_checked_through, Some(through));
            assert_eq!(executor.polls.load(std::sync::atomic::Ordering::SeqCst), 1);
            services.shutdown_execution().await;
        }
    }

    async fn terminal_axes_goal_fixture() -> (std::sync::Arc<RuntimeServices>, String) {
        let services = RuntimeServices::in_memory().unwrap();
        let graph = graph_with_lineage(
            "terminal axes fixture",
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
            .unwrap();
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":format!("goal:{graph_id}"),"session_id":"projection-session","objective":"axes current Goal fixture",
            "criteria":[{"id":"criterion","statement":"fixture satisfied","status":"satisfied"}],
            "phase":"terminal","scope":"internal","completion":"satisfied","revision":1,"user_sequence":1,
            "spec_revision":1,"spec_digest":"axes-fixture-v1",
            "execution_binding":{"objective_id":format!("goal:{graph_id}"),"session_id":"projection-session","turn_id":"projection-turn","root_execution_id":graph_id,"agentic_program_id":format!("program:{graph_id}")},
            "terminal":{"kind":"satisfied","terminal_fence":"axes-fixture-terminal","authority_revision":1,"reason":"fixture","committed_at_ms":1}
        })).unwrap();
        services.goal_store().create(goal).unwrap();
        (services, graph_id)
    }

    #[tokio::test]
    async fn r1_outcome_axes_non_program_summary_and_full_use_current_goal() {
        let (services, graph_id) = terminal_axes_goal_fixture().await;
        let mut query = context(&services);
        let full = snapshot(&services, &graph_id, &query).await.unwrap();
        query.detail_scope = ProjectionDetailScope::Summary;
        let summary = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(full.terminal_axes, summary.terminal_axes);
        assert!(full.agentic_collaboration.programs.is_empty());
        assert_eq!(
            full.terminal_axes.outcome.outcome,
            Some(harness_contract::goal::ObjectiveTerminalKind::Satisfied)
        );
        assert_eq!(
            full.terminal_axes.outcome.goal_id,
            Some(format!("goal:{graph_id}"))
        );
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_outcome_axes_satisfied_goal_does_not_finish_draining_execution() {
        let (services, graph_id) = terminal_axes_goal_fixture().await;
        let mut program = crate::AgenticProgramProjection::empty(
            format!("program:{graph_id}"),
            format!("goal:{graph_id}"),
        );
        program.session_id = "projection-session".into();
        program.turn_id = "projection-turn".into();
        program.root_execution_id = Some(graph_id.clone());
        program.status = crate::AgenticProgramStatus::Draining;
        program.continuation = Some(serde_json::from_value(serde_json::json!({"source_program_id":"source","source_objective_id":"source-goal","source_root_id":"source-root","source_revision":1,"source_status":"verified","authorization_generation":1,"authorization_revision":1,"binding_digest":"fixture-digest"})).unwrap());
        services.event_store().append(crate::RuntimeEventInput {
            stream_id:format!("agentic-program:{}",program.program_id),scope:RuntimeEventScope::Program,kind:"agentic.program_opened".into(),
            status:Some("draining".into()),actor:Some("projection-fixture".into()),refs:vec![crate::RuntimeEventRef { kind:"execution_graph".into(), id:graph_id.clone() }],
            payload:serde_json::json!({"objective_id":program.objective_id,"session_id":program.session_id,"turn_id":program.turn_id,"root_execution_id":graph_id,"continuation_seed":program}),
        }).unwrap();
        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .unwrap();
        assert_eq!(
            projection.terminal_axes.outcome.outcome,
            Some(harness_contract::goal::ObjectiveTerminalKind::Satisfied)
        );
        assert_eq!(
            projection.terminal_axes.execution.phase,
            harness_contract::projection::TerminalExecutionPhase::Draining
        );
        assert_ne!(
            projection.terminal_axes.execution.settlement,
            harness_contract::projection::TerminalEffectSettlement::ProvenSettled
        );
        assert_eq!(
            projection.terminal_axes.delivery.session_client_evidence,
            harness_contract::projection::SessionClientEvidence::RequiresFreshContext
        );
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_outcome_axes_goal_program_effect_and_descendant_deltas_match_snapshot() {
        let (services, graph_id) = terminal_axes_goal_fixture().await;
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query).await.unwrap();
        let executor = BindingHistoryExecutor {
            polls: std::sync::atomic::AtomicUsize::new(0),
        };
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "binding_history_oracle",
            "fixture://effect",
        );
        let current = services.graph_state_store().load(&graph_id).unwrap();
        let planned = services
            .commit_service()
            .replan(
                &current,
                vec![node.clone()],
                Vec::new(),
                "real effect start provenance".into(),
            )
            .unwrap()
            .graph;
        let ready = services
            .commit_service()
            .transition_node(
                &planned,
                &node.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let ticket = crate::execution_core::graph::NodeExecutor::start(
            &executor,
            crate::execution_core::graph::NodeExecutionContext {
                graph: std::sync::Arc::new(ready.clone()),
                node: node.clone(),
                attempt: 1,
            },
        )
        .await
        .unwrap();
        services
            .commit_service()
            .bind_and_start_nodes(
                &ready,
                BTreeMap::from([(
                    node.id.clone(),
                    crate::execution_core::graph::ExecutionNodeBinding {
                        executor_kind: ticket.executor_kind.clone(),
                        ticket_idempotency_key: ticket.idempotency_key.clone(),
                        attempt: ticket.attempt,
                        resource_lease_refs: Vec::new(),
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )]),
                vec![ticket],
            )
            .unwrap();
        let update = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor,
            &query,
        )
        .unwrap();
        assert!(update
            .operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::ReplaceTerminalAxes { .. })));
        let reduced =
            harness_contract::projection::reduce_projection_delta(&initial, &update).unwrap();
        let fresh = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(reduced, fresh);
        assert_eq!(
            fresh.terminal_axes.execution.settlement,
            harness_contract::projection::TerminalEffectSettlement::Unsettled
        );
        let mut previous = fresh;
        let goal_id = format!("goal:{graph_id}");
        services
            .goal_store()
            .revise(&goal_id, 1, 2, "changed current goal", |goal| {
                goal.terminal = None;
                goal.completion = GoalCompletion::Open;
                goal.phase = "execution".into();
                vec!["completion".into()]
            })
            .unwrap();
        let update = delta(
            &services,
            &graph_id,
            previous.revision,
            previous.cursor,
            &query,
        )
        .unwrap();
        let fresh = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(
            harness_contract::projection::reduce_projection_delta(&previous, &update).unwrap(),
            fresh
        );
        assert!(fresh.terminal_axes.outcome.outcome.is_none());
        previous = fresh;
        services.event_store().append(crate::RuntimeEventInput {
            stream_id:format!("agentic-program:program:{graph_id}"),scope:RuntimeEventScope::Program,kind:"agentic.program_opened".into(),
            status:Some("open".into()),actor:Some("projection-fixture".into()),refs:vec![crate::RuntimeEventRef { kind:"execution_graph".into(), id:graph_id.clone() }],
            payload:serde_json::json!({"objective_id":goal_id,"session_id":"projection-session","turn_id":"projection-turn","root_execution_id":graph_id}),
        }).unwrap();
        let update = delta(
            &services,
            &graph_id,
            previous.revision,
            previous.cursor,
            &query,
        )
        .unwrap();
        let fresh = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(
            harness_contract::projection::reduce_projection_delta(&previous, &update).unwrap(),
            fresh
        );
        previous = fresh;
        let mut child = graph_with_lineage(
            "axes descendant",
            "projection-session",
            "projection-turn",
            "projection-task",
        );
        child.parent_execution = Some(ExecutionParentBinding {
            execution_id: graph_id.clone(),
            node_id: "axes-parent".into(),
        });
        services
            .execution_supervisor()
            .submit_and_wait(
                child,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        let update = delta(
            &services,
            &graph_id,
            previous.revision,
            previous.cursor,
            &query,
        )
        .unwrap();
        let fresh = snapshot(&services, &graph_id, &query).await.unwrap();
        assert!(update
            .operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::ReplaceTerminalAxes { .. })));
        assert_eq!(
            harness_contract::projection::reduce_projection_delta(&previous, &update).unwrap(),
            fresh
        );
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn r1_outcome_axes_runtime_cache_contains_no_mutable_delivery() {
        let (services, graph_id) = terminal_axes_goal_fixture().await;
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query).await.unwrap();
        let terminal = services
            .session_terminal_delivery()
            .enqueue(
                "axes-unfenced-test-terminal",
                "axes-message",
                "projection-session",
                initial.cursor,
                "fixture://axes",
            )
            .unwrap();
        let before = services.event_store().durable_commit_cursor().unwrap();
        let claimed = services
            .session_terminal_delivery()
            .claim("axes-worker", 1, 1000, 10)
            .unwrap()
            .into_iter()
            .find(|record| record.terminal_id == terminal.terminal_id)
            .unwrap();
        services
            .session_terminal_delivery()
            .acknowledge(&claimed.terminal_id, "axes-worker", claimed.revision, 2)
            .unwrap();
        let cached = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(initial.terminal_axes, cached.terminal_axes);
        assert_eq!(
            services.event_store().durable_commit_cursor().unwrap(),
            before
        );
        let wire = serde_json::to_string(&cached.terminal_axes).unwrap();
        for forbidden in [
            "materialized_at_ms",
            "outbox_revision",
            "received_at_ms",
            "client_id",
        ] {
            assert!(!wire.contains(forbidden));
        }
        assert!(
            delta(&services, &graph_id, cached.revision, cached.cursor, &query)
                .unwrap()
                .operations
                .is_empty()
        );
        services.shutdown_execution().await;
    }

    fn bound_task_spec(services: &RuntimeServices, session_id: &str, objective: &str) -> TaskSpec {
        services.publish_session_execution_policy(
            session_id,
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Supervised,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        services
            .task_runtime_port()
            .bind_task_spec(session_id, None, TaskSpec::new(objective))
            .expect("bind projection Task policy")
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
    fn summary_graph_is_bounded_without_losing_public_node_identity_or_usage() {
        let mut graph = ExecutionGraph::new("O".repeat(8_000));
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "managed-agent", "{}");
        node.resource_scopes = (0..100).map(|index| format!("read:item-{index}")).collect();
        let node_id = node.id.clone();
        graph.nodes.push(node);
        graph
            .node_statuses
            .insert(node_id.clone(), ExecutionNodeStatus::Completed);
        let mut usage = ExecutionUsage {
            input_tokens: 12_345,
            output_tokens: 6_789,
            tool_calls: 7,
            ..ExecutionUsage::default()
        };
        usage.required_acceptance.criteria = vec!["C".repeat(8_000)];
        usage.observed_acceptance.satisfied_criteria = vec!["C".repeat(8_000)];
        usage.runtime_write_attempt_paths = vec!["private/output".to_string()];
        graph.node_results.insert(
            node_id,
            ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("execution-result:summary".to_string()),
                wait_class: None,
                summary: Some("S".repeat(8_000)),
                evidence_refs: Vec::new(),
                failure: Some(ExecutionFailure {
                    kind: "presentation".to_string(),
                    message: "F".repeat(8_000),
                    retryable: false,
                    evidence_refs: Vec::new(),
                }),
                usage,
                finished_at_ms: 1,
            },
        );

        let mut full = project_execution_graph(&graph);
        full.nodes[0].team_run_id = Some("team-summary".to_string());
        full.nodes[0].agent_run_id = Some("agent-run-summary".to_string());
        full.nodes[0].role_id = Some("reviewer".to_string());
        let summary = summary_graph_projection(full.clone());
        let node = &summary.nodes[0];

        assert_eq!(node.status, ExecutionNodeStatus::Completed);
        assert_eq!(node.team_run_id.as_deref(), Some("team-summary"));
        assert_eq!(node.agent_run_id.as_deref(), Some("agent-run-summary"));
        assert_eq!(node.role_id.as_deref(), Some("reviewer"));
        assert_eq!(node.usage.input_tokens, 12_345);
        assert_eq!(node.usage.output_tokens, 6_789);
        assert_eq!(node.usage.tool_calls, 7);
        assert!(node.usage.required_acceptance.is_empty());
        assert!(node.usage.observed_acceptance.is_empty());
        assert!(node.usage.runtime_write_attempt_paths.is_empty());
        assert!(node
            .summary
            .as_ref()
            .is_some_and(|value| value.chars().count() <= 1_025));
        assert!(node
            .failure
            .as_ref()
            .is_some_and(|failure| failure.message.chars().count() <= 513));
        assert_eq!(node.resource_scopes.len(), SUMMARY_EVIDENCE_REFS);
        assert!(
            serde_json::to_vec(&summary)
                .expect("summary serializes")
                .len()
                < serde_json::to_vec(&full).expect("full serializes").len() / 3
        );
    }

    #[test]
    fn summary_activity_reduction_crops_graph_payload_before_materialization() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut graph = ExecutionGraph::new("O".repeat(1_000_000));
        graph.id = "projection-crop-before-reduce".to_string();
        for index in 0..16 {
            let mut node =
                ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "managed-agent", "{}");
            node.id = format!("agent-{index}");
            let mut work = harness_contract::execution_graph::ExecutionWorkContract::new(
                harness_contract::execution_graph::ExecutionWorkRole::CrossCheck,
            );
            work.input_artifact_refs = (0..1_000)
                .map(|item| format!("artifact://agent-{index}/{item}/{}", "I".repeat(512)))
                .collect();
            work.output_artifact_kinds = (0..1_000)
                .map(|item| format!("output-{item}-{}", "K".repeat(512)))
                .collect();
            node.work = Some(work);
            graph.nodes.push(node.clone());
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Completed);
            graph.node_results.insert(
                node.id.clone(),
                ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: Some(format!("artifact://agent-{index}")),
                    wait_class: None,
                    summary: Some("S".repeat(1_000_000)),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: ExecutionUsage::default(),
                    finished_at_ms: index + 1,
                },
            );
        }
        let projection = project_execution_graph(&graph);
        let scope = ExecutionProjectionScope {
            programs: Vec::new(),
            session_id: None,
            mission_id: None,
            task_id: None,
            turn_id: None,
            execution_ids: BTreeSet::from([graph.id.clone()]),
            node_ids: graph.nodes.iter().map(|node| node.id.clone()).collect(),
            entity_ids: BTreeSet::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            child_executions: Vec::new(),
            agentic_collaboration: Default::default(),
            descendant_graphs: Vec::new(),
        };

        let (full, _) = activity::project_execution_activities_from_events(
            &services,
            &scope,
            &projection,
            Vec::new(),
            true,
        );
        let (summary, _) = activity::project_execution_activities_from_events(
            &services,
            &scope,
            &projection,
            Vec::new(),
            false,
        );
        assert!(
            (0..16).all(|index| summary
                .iter()
                .any(|activity| activity.node_id.as_deref() == Some(&format!("agent-{index}")))),
            "every Agent node identity must stay visible"
        );
        assert!(summary.iter().all(|activity| {
            activity
                .public_summary
                .as_deref()
                .is_none_or(|value| value.chars().count() <= 513)
                && activity
                    .result_summary
                    .as_deref()
                    .is_none_or(|value| value.chars().count() <= 1_025)
        }));
        let full_bytes = serde_json::to_vec(&full)
            .expect("full activities serialize")
            .len();
        let summary_bytes = serde_json::to_vec(&summary)
            .expect("summary activities serialize")
            .len();
        assert!(
            full_bytes > 8_000_000,
            "fixture must retain a materially oversized full projection; got {full_bytes} bytes"
        );
        assert!(
            summary_bytes < 1_024_000,
            "Summary must be bounded before activity materialization; got {summary_bytes} bytes"
        );
        assert!(summary_bytes < full_bytes / 10);
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
    async fn snapshot_cache_and_delta_recover_an_external_goal_without_a_local_commit_signal() {
        let temporary = tempfile::tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let backend =
            std::sync::Arc::new(crate::runtime_event_store::EphemeralRuntimeEventStore::new());
        let reader = std::sync::Arc::new(crate::RuntimeEventStore::from_backend(backend.clone()));
        let writer = std::sync::Arc::new(crate::RuntimeEventStore::from_backend(backend));
        let services = RuntimeServices::test_builder(temporary.path().join("home"), &workspace)
            .runtime_event_store(reader.clone())
            .build()
            .unwrap();
        let graph = graph_with_lineage(
            "external Goal recovery",
            "external-session",
            "external-turn",
            "external-task",
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
            .unwrap();
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query).await.unwrap();
        assert!(initial.goals.is_empty());
        // The independent recovery client runs under its own principal, so it
        // owns a different redaction revision and therefore a different
        // projection cache family. Its recovery snapshot can never be served
        // by the delta that the original client family asks for, and it must
        // never evict the base that family still holds.
        let mut recovery_query = query.clone();
        recovery_query.principal = "test-external-goal-recovery".to_string();
        let recovery_initial = snapshot(&services, &graph_id, &recovery_query)
            .await
            .unwrap();
        assert!(recovery_initial.goals.is_empty());
        assert_ne!(
            initial.redaction_revision, recovery_initial.redaction_revision,
            "the independent recovery client must not share the delta client cache family"
        );
        services.shutdown_maintenance().await;
        let hint = reader.current_commit_cursor();
        let goal_id = format!("goal:{graph_id}");
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id": goal_id, "session_id": "external-session", "objective": "Externally committed Goal must be visible",
            "criteria": [{"id":"external-criterion", "statement":"visible in snapshot and delta", "status":"open"}],
            "phase":"execution", "scope":"internal", "completion":"open", "revision":1, "user_sequence":1,
            "spec_revision":1,"spec_digest":"kernel-external-goal-fixture-v1",
        })).unwrap();
        crate::execution_core::GoalStore::new(writer)
            .create(goal)
            .unwrap();
        assert_eq!(reader.current_commit_cursor(), hint);
        let durable = reader.durable_commit_cursor().unwrap();
        assert!(durable > initial.cursor);
        assert!(durable > recovery_initial.cursor);
        // Independent recovery first, in the other cache family. It proves the
        // external Goal is durable and visible without refreshing the family
        // that still holds the delta client base.
        let recovered = snapshot(&services, &graph_id, &recovery_query)
            .await
            .unwrap();
        assert_eq!(recovered.cursor, durable);
        assert!(recovered.goals.iter().any(|goal| goal.id == goal_id));
        // Then the real delta from the base the original client still holds.
        let update = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor,
            &query,
        )
        .unwrap();
        assert_eq!(update.base_cursor, initial.cursor);
        assert_eq!(update.from_revision, initial.revision);
        assert_eq!(update.target_cursor, durable);
        assert!(
            update.resync_reason.is_none(),
            "a resync must never stand in for the external Goal upsert"
        );
        assert!(update.operations.iter().any(|operation| matches!(operation,
            ProjectionOperation::UpsertEntity { collection: ProjectionEntityCollection::Goals, entity }
                if entity.id == goal_id)));
        let reduced = harness_contract::projection::reduce_projection_delta(&initial, &update)
            .expect("external goal delta reduces onto the held base");
        assert!(reduced.goals.iter().any(|goal| goal.id == goal_id));
        let current = snapshot(&services, &graph_id, &query).await.unwrap();
        assert_eq!(current.cursor, durable);
        assert!(current.goals.iter().any(|goal| goal.id == goal_id));
        assert_eq!(reduced.cursor, current.cursor);
        assert_eq!(reduced.goals, current.goals);
        assert_eq!(recovered.goals, current.goals);
        services.shutdown_execution().await;
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
                spec: bound_task_spec(&services, "session-origin", "session-scoped projection"),
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
                spec: bound_task_spec(&services, "session-a", "shared Team role"),
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
        // A commit that does not move the graph revision must still advance
        // the projection consumption cursor, so the delta carries a real
        // cursor advance instead of replaying the whole projection.
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "evolution:projection-graph-revision".to_string(),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.unrelated.recorded".to_string(),
                status: Some("recorded".to_string()),
                actor: Some("projection-test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"unrelated": true}),
            })
            .expect("unrelated event commits");
        // The delta contract diffs the base a client actually holds against a
        // freshly materialized target. A base that is not cached is an honest
        // resync, never a guessed full delta, so the delta starts from the
        // snapshot this client holds.
        let initial_delta = delta(
            &services,
            &initial_snapshot.execution_id,
            initial_snapshot.revision,
            initial_snapshot.cursor,
            &query,
        )
        .expect("delta");
        assert!(
            initial_delta.resync_reason.is_none(),
            "a held snapshot base must produce a delta, not a resync"
        );
        assert_eq!(
            initial_delta.target_revision, initial_snapshot.revision,
            "the snapshot and the delta must report one graph revision"
        );
        assert!(initial_delta.target_cursor >= initial_snapshot.cursor);
        assert!(initial_delta.operations.iter().any(|operation| {
            matches!(
                operation,
                ProjectionOperation::AdvanceCursor { cursor }
                    if *cursor == initial_delta.target_cursor
            )
        }));
        let idle_delta = delta(
            &services,
            &initial_snapshot.execution_id,
            initial_delta.target_revision,
            initial_delta.target_cursor,
            &query,
        )
        .expect("unchanged delta");
        assert!(
            idle_delta.operations.is_empty(),
            "an unchanged terminal/active projection must not replay terminal or cursor operations"
        );
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
        // The delta base is the parent projection this client holds before the
        // child execution is registered. A base that is not cached is an
        // honest resync, never a guessed full delta.
        let base = snapshot(&services, &parent_id, &context(&services))
            .await
            .expect("parent base projection");
        assert!(base.child_executions.is_empty());

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

        // The delta is requested while the base is still the cached family
        // member; a later same-family snapshot would evict it.
        let lineage_delta = delta(
            &services,
            &parent_id,
            base.revision,
            base.cursor,
            &context(&services),
        )
        .expect("lineage delta");
        assert!(
            lineage_delta.resync_reason.is_none(),
            "a held base must produce a delta, not a resync"
        );
        assert!(lineage_delta.operations.iter().any(|operation| {
            matches!(
                operation,
                ProjectionOperation::UpsertChildExecution { child }
                    if child.execution_id == child_id
            )
        }));

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

    }
    #[tokio::test]
    async fn agentic_program_reducer_exports_canonical_typed_collaboration() {
        use std::sync::Arc;

        use harness_contract::agent_action::{
            AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
            ArtifactCommitInput, MessagePublishInput, ObjectiveCompleteRequestInput,
            TaskClaimInput, TaskPublishInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput,
            TeamCreateInput,
        };
        use harness_contract::goal::{
            AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract,
        };

        let services = RuntimeServices::in_memory().expect("runtime services");
        let execution_id = "execution-1";
        let mut graph = graph_with_lineage(
            "Agentic projection authority",
            "session-agentic-projection",
            "turn-agentic-projection",
            "task-agentic-projection",
        );
        graph.id = execution_id.to_string();
        services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("register Agentic projection graph");

        let root = |action_id: &str, action: AgentAction| AgentActionEnvelope {
            action_id: action_id.to_string(),
            actor: AgentActorBinding {
                objective_id: "objective-agentic-projection".to_string(),
                program_id: "program-agentic-projection".to_string(),
                session_id: "session-agentic-projection".to_string(),
                turn_id: "turn-agentic-projection".to_string(),
                root_execution_id: Some(execution_id.to_string()),
                required_team_count: 2,
                exact_team_count: None,
                objective_summary: "Produce an evidence-backed manufacturing report".to_string(),
                model_lease: "projection-test".to_string(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "root-agentic-projection".to_string(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            expected_revision: None,
            action,
        };
        let managed = |action_id: &str,
                       team_id: &str,
                       agent_id: &str,
                       execution: &str,
                       action: AgentAction| {
            let mut envelope = root(action_id, action);
            envelope.actor.actor_id = agent_id.to_string();
            envelope.actor.kind = AgentActorKind::Agent;
            envelope.actor.execution_id = Some(execution.to_string());
            envelope.actor.team_id = Some(team_id.to_string());
            envelope.actor.agent_id = Some(agent_id.to_string());
            envelope
        };
        let actions = crate::AgentActionService::new(Arc::clone(services.event_store()));
        let research_team_id = actions
            .apply(&root(
                "create-research-team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Evidence Research Team".to_string(),
                    mission: "Research manufacturing evidence".to_string(),
                    objective: Some("Ground every conclusion".to_string()),
                }),
            ))
            .expect("create research Team")
            .changed_refs[0]
            .clone();
        let synthesis_team_id = actions
            .apply(&root(
                "create-synthesis-team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Decision Synthesis Team".to_string(),
                    mission: "Challenge and synthesize the evidence".to_string(),
                    objective: Some("Deliver a reviewed decision".to_string()),
                }),
            ))
            .expect("create synthesis Team")
            .changed_refs[0]
            .clone();
        let analyst_id = actions
            .apply(&root(
                "invite-evidence-analyst",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: research_team_id.clone(),
                    role: "Evidence Analyst".to_string(),
                    mission: "Inspect the source evidence".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: Vec::new(),
                    execution_requirements: Vec::new(),
                }),
            ))
            .expect("invite analyst")
            .changed_refs[0]
            .clone();
        let reviewer_id = actions
            .apply(&root(
                "invite-decision-reviewer",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: synthesis_team_id.clone(),
                    role: "Decision Reviewer".to_string(),
                    mission: "Independently verify and synthesize findings".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: Vec::new(),
                    execution_requirements: Vec::new(),
                }),
            ))
            .expect("invite reviewer")
            .changed_refs[0]
            .clone();
        let research_task_id = actions
            .apply(&root(
                "publish-research-task",
                AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: research_team_id.clone(),
                    title: "Collect source evidence".to_string(),
                    objective: "Establish the production baseline source".to_string(),
                    acceptance: "Record the source revision".to_string(),
                    criterion_source: Default::default(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on: Vec::new(),

                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            ))
            .expect("publish research Task")
            .changed_refs[0]
            .clone();
        let synthesis_task_id = actions
            .apply(&root(
                "publish-synthesis-task",
                AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: synthesis_team_id.clone(),
                    title: "Validate production baseline".to_string(),
                    objective: "Compare the live metric with its source evidence".to_string(),
                    acceptance: "Cite the source and state uncertainty".to_string(),
                    criterion_source: Default::default(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on: vec![research_task_id.clone()],

                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            ))
            .expect("publish synthesis Task")
            .changed_refs[0]
            .clone();

        actions
            .apply(&managed(
                "claim-research-task",
                &research_team_id,
                &analyst_id,
                "execution-agentic-analyst",
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: research_task_id.clone(),
                    reason: Some("matched evidence capability".to_string()),
                }),
            ))
            .expect("claim research Task");
        let research_artifact = actions
            .apply(&managed(
                "commit-research-artifact",
                &research_team_id,
                &analyst_id,
                "execution-agentic-analyst",
                AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: "artifact://manufacturing-source".to_string(),
                    kind: "research".to_string(),
                    title: "Manufacturing source evidence".to_string(),
                    relates_to: Vec::new(),
                }),
            ))
            .expect("commit research artifact")
            .changed_refs[0]
            .clone();
        actions
            .apply(&managed(
                "publish-research-topic",
                &research_team_id,
                &analyst_id,
                "execution-agentic-analyst",
                AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: format!("topic:{research_team_id}"),
                    summary: Some("Source baseline is ready for independent review".to_string()),
                    content_ref: Some(research_artifact.clone()),
                    refs: vec![research_artifact.clone()],
                    recipients: Vec::new(),
                    intent: None,
                    issue_dispositions: Vec::new(),
                }),
            ))
            .expect("publish topic");
        actions
            .apply(&managed(
                "submit-research-task",
                &research_team_id,
                &analyst_id,
                "execution-agentic-analyst",
                AgentAction::TaskSubmit(TaskSubmitInput {
                    deliverable: None,
                    task_ref: research_task_id.clone(),
                    artifact_refs: vec![research_artifact.clone()],
                    evidence_refs: vec!["tool://source-observation".to_string()],
                    unresolved: Vec::new(),
                }),
            ))
            .expect("submit research Task");
        actions
            .apply(&managed(
                "review-research-task",
                &synthesis_team_id,
                &reviewer_id,
                "execution-agentic-reviewer",
                AgentAction::TaskReview(TaskReviewInput {
                    operator_requirement: None,
                    task_ref: research_task_id.clone(),
                    decision: TaskReviewDecision::Accept,
                    reason: "source independently verified".to_string(),
                    evidence_refs: vec!["tool://research-review".to_string()],
                }),
            ))
            .expect("review research Task");

        actions
            .apply(&managed(
                "claim-synthesis-task",
                &synthesis_team_id,
                &reviewer_id,
                "execution-agentic-reviewer",
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: synthesis_task_id.clone(),
                    reason: Some("dependency is accepted".to_string()),
                }),
            ))
            .expect("claim dependent Task");
        let final_artifact = actions
            .apply(&managed(
                "commit-final-artifact",
                &synthesis_team_id,
                &reviewer_id,
                "execution-agentic-reviewer",
                AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: "artifact://manufacturing-decision".to_string(),
                    kind: "report".to_string(),
                    title: "Reviewed manufacturing decision".to_string(),
                    relates_to: vec![research_artifact.clone()],
                }),
            ))
            .expect("commit final artifact")
            .changed_refs[0]
            .clone();
        actions
            .apply(&managed(
                "submit-synthesis-task",
                &synthesis_team_id,
                &reviewer_id,
                "execution-agentic-reviewer",
                AgentAction::TaskSubmit(TaskSubmitInput {
                    deliverable: None,
                    task_ref: synthesis_task_id.clone(),
                    artifact_refs: vec![final_artifact.clone()],
                    evidence_refs: vec!["tool://decision-validation".to_string()],
                    unresolved: Vec::new(),
                }),
            ))
            .expect("submit synthesis Task");
        actions
            .apply(&managed(
                "review-synthesis-task",
                &research_team_id,
                &analyst_id,
                "execution-agentic-analyst",
                AgentAction::TaskReview(TaskReviewInput {
                    operator_requirement: None,
                    task_ref: synthesis_task_id.clone(),
                    decision: TaskReviewDecision::Accept,
                    reason: "decision is traceable to source".to_string(),
                    evidence_refs: vec!["tool://decision-review".to_string()],
                }),
            ))
            .expect("review synthesis Task");

        services
            .goal_store()
            .create(GoalContract {
                input_contract: None,
                id: format!("goal:{execution_id}"),
                session_id: "session-agentic-projection".to_string(),
                objective: "Produce an evidence-backed manufacturing report".to_string(),
                criteria: vec![AcceptanceCriterion {
                    id: "terminal_synthesis".to_string(),
                    statement: "produce reviewed evidence".to_string(),
                    statement_ref: None,
                    source_refs: Vec::new(),
                    required_evidence: vec![format!("execution_graph:{execution_id}")],
                    status: AcceptanceStatus::Open,
                    waiver: None,
                }],
                constraints: Vec::new(),
                phase: "execution".to_string(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blockers: Vec::new(),
                scope: harness_contract::goal::GoalScope::UserObjective,
                user_intent_criterion_id: Some("terminal_synthesis".to_string()),
                source_intent_ref: Some("session_message:projection-test".to_string()),
                execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                    objective_id: "objective-agentic-projection".to_string(),
                    session_id: "session-agentic-projection".to_string(),
                    turn_id: "turn-agentic-projection".to_string(),
                    root_execution_id: execution_id.to_string(),
                    agentic_program_id: "program-agentic-projection".to_string(),
                }),
                spec_revision: 1,
                spec_digest: "projection-test".to_string(),
                review_refs: Vec::new(),
                waiting: None,
                participation_requirement: None,
                obligations: Vec::new(),
                recovery: None,
                terminal: None,
                completion: GoalCompletion::Open,
                revision: 1,
                user_sequence: 1,
                reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                    review_id: "projection-semantic-review".into(),
                    criterion_ref: "terminal_synthesis".into(),
                    spec_revision: 1,
                    input_manifest_digest: "projection-test".into(),
                    result_refs: vec![final_artifact.clone()],
                    evidence_refs: vec!["tool://decision-review".into()],
                    reviewer_actor: "root-projection-reviewer".into(),
                    reviewer_execution_id: Some(execution_id.into()),
                    producer_refs: vec![reviewer_id.clone()],
                    decision: "satisfied".into(),
                    reason_ref: "tool://decision-review".into(),
                    // Pure projection fixture: physical receipt ingress has separate tests.
                    verification: Some(harness_contract::goal::ObjectiveReviewVerification {
                        input_set: None,
                        result_source_revisions: Default::default(),
                        independence_required: true,
                        review_policy_digest: None,
                        effect_manifest_digest: None,
                        effect_source_refs: Vec::new(),
                        goal_spec_revision: Some(1),
                        goal_spec_digest: Some("projection-test".into()),
                        work_manifest_digest: crate::agentic::review_evidence::work_manifest_digest(
                            &actions.project("program-agentic-projection").unwrap(),
                        ),
                        reviewer_execution_id: execution_id.into(),
                        producer_execution_ids: vec!["projection-producer-run".into()],
                        current_review_effect_key: None,
                        reads: vec![harness_contract::goal::ObjectiveResultReadProof {
                            result_kind: harness_contract::goal::GoalResultKind::Artifact,
                            effect_observation_refs: vec![],
                            result_ref: final_artifact.clone(),
                            content_ref: "artifact://projection-fixture".into(),
                            sha256: "projection-fixture-hash".into(),
                            bytes: 1,
                            receipt_refs: vec!["fixture:read".into()],
                        }],
                    }),
                }],
            })
            .expect("create objective goal");
        let before_completion = snapshot(&services, execution_id, &context(&services))
            .await
            .expect("project open Program");
        assert!(before_completion.agentic_collaboration.programs[0]
            .completion
            .wait
            .is_none());
        actions
            .apply(&root(
                "request-program-completion",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: Vec::new(),
                    result_refs: vec![final_artifact.clone()],
                    evidence_refs: vec![research_artifact.clone(), final_artifact.clone()],
                    unresolved: Vec::new(),
                }),
            ))
            .expect("request completion");

        services
            .reconcile_agentic_completion_request("program-agentic-projection")
            .expect("reconcile completion")
            .expect("verified Program");
        let projection = snapshot(&services, execution_id, &context(&services))
            .await
            .expect("project Agentic Program");
        let collaboration = &projection.agentic_collaboration;
        assert_eq!(collaboration.schema_version, 5);
        assert_eq!(collaboration.programs.len(), 1);
        let program = &collaboration.programs[0];
        assert_eq!(program.teams.len(), 2);
        assert_eq!(program.agents.len(), 2);
        assert_eq!(program.tasks.len(), 2);
        assert!(program
            .teams
            .iter()
            .any(|team| team.name == "Evidence Research Team"));
        assert!(program
            .agents
            .iter()
            .any(|agent| agent.role == "Decision Reviewer"));
        assert!(program.tasks.iter().any(|task| {
            task.task_id == synthesis_task_id && task.depends_on == vec![research_task_id.clone()]
        }));
        let synthesis = program
            .tasks
            .iter()
            .find(|task| task.task_id == synthesis_task_id)
            .expect("synthesis task");
        assert_eq!(synthesis.dependency_resolution.len(), 1);
        assert_eq!(
            synthesis.dependency_resolution[0].dependency_ref,
            research_task_id
        );
        assert_eq!(
            synthesis.dependency_resolution[0].status,
            harness_contract::projection::AgenticCollaborationDependencyStatus::Resolved
        );
        assert!(!program.topics.is_empty());
        assert_eq!(program.artifacts.len(), 2);
        assert!(program.completion.wait.is_some());
        assert!(program.completion.verdict.is_some());
        assert!(
            projection.teams.is_empty(),
            "generic Team is not an Agentic owner"
        );
        assert!(
            projection.agents.is_empty(),
            "generic Agent is not an Agentic owner"
        );

        let update = delta(
            &services,
            execution_id,
            before_completion.revision,
            before_completion.cursor,
            &context(&services),
        )
        .expect("completion verdict delta");
        assert!(update.operations.iter().any(|operation| matches!(
            operation,
            ProjectionOperation::ReplaceAgenticCollaboration { collaboration }
                if collaboration.programs[0].completion.verdict.is_some()
        )));
        let reduced =
            harness_contract::projection::reduce_projection_delta(&before_completion, &update)
                .expect("apply contiguous Agentic delta");
        assert_eq!(
            reduced.agentic_collaboration,
            projection.agentic_collaboration
        );
        assert!(harness_contract::projection::reduce_projection_delta(&reduced, &update).is_err());
        let mut gap = update.clone();
        gap.base_cursor = gap.base_cursor.saturating_add(1);
        assert!(
            harness_contract::projection::reduce_projection_delta(&before_completion, &gap)
                .is_err()
        );

        if let Some(path) = std::env::var_os("COWD_EXPORT_AGENTIC_PROGRAM_PROJECTION_FIXTURE") {
            let fixture = serde_json::json!({
                "schema_version": projection.schema_version,
                "execution_id": projection.execution_id,
                "revision": projection.revision,
                "cursor": projection.cursor,
                "detail_scope": projection.detail_scope,
                "authorization_revision": 1_000_001,
                "redaction_revision": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                "source_health": "fresh",
                "session_id": projection.session_id,
                "task_id": projection.task_id,
                "turn_id": projection.turn_id,
                "activities": [],
                "activity_relations": [],
                "graph": projection.graph,
                "agentic_collaboration": projection.agentic_collaboration,
                "agents": [],
                "teams": [],
            });
            std::fs::write(
                path,
                serde_json::to_vec_pretty(&fixture).expect("encode projection fixture"),
            )
            .expect("write projection fixture");
        }
    }

    #[tokio::test]
    async fn corrupt_agentic_program_stream_fails_projection_closed() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut graph = graph_with_lineage(
            "corrupt Program must not become empty collaboration",
            "session-corrupt-agentic",
            "turn-corrupt-agentic",
            "task-corrupt-agentic",
        );
        graph.id = "execution-corrupt-agentic".to_string();
        services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("register graph");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:corrupt".to_string(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.invalid_fixture".to_string(),
                status: Some("corrupt".to_string()),
                actor: Some("fault-injection".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"missing": "program_opened"}),
            })
            .expect("append corrupt Program stream");

        let error = snapshot(&services, "execution-corrupt-agentic", &context(&services))
            .await
            .expect_err("corrupt Program stream must fail closed");
        assert!(error.to_string().contains("program_not_found"));
    }

    #[test]
    fn linked_team_topology_supplies_strategy_identity_without_terminal_receipt() {
        let team_id = "runtime-team:live".to_string();
        let team_graph_id = "team-graph:runtime-team:live".to_string();
        let scope = ExecutionProjectionScope {
            programs: Vec::new(),
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
            agentic_collaboration: Default::default(),
            descendant_graphs: Vec::new(),
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
                kind: "runtime.control.completed".to_string(),
                ..strategy_event(&graph_id, "decision-1", "runtime.control.completed", 99)
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
    fn strategy_projection_redacts_private_calibration() {
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
                spec: bound_task_spec(&services, "session-a", "session-scoped projection"),
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
                    input_contract: None,
                    id: id.clone(),
                    session_id: session_id.to_string(),
                    objective: format!("objective for {session_id}"),
                    criteria: vec![AcceptanceCriterion {
                        id: format!("criterion-{id}"),
                        statement: "produce evidence".to_string(),
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
                    user_intent_criterion_id: Some(format!("criterion-{id}")),
                    source_intent_ref: Some(format!("session_message:{session_id}")),
                    execution_binding: None,
                    spec_revision: 1,
                    spec_digest: "projection-test".to_string(),
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

#[cfg(any(test, feature="test-fixtures"))]
pub use read_evidence::{read_evidence_fixture, read_evidence_fixture_in};
