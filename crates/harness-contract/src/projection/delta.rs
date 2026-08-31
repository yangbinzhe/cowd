use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    ChildExecutionProjection, ExecutionActivityProjection, ExecutionActivityRelation,
    ExecutionConcurrencyProjection, ExecutionLiveState, ExecutionProjection,
    ProjectionCommandAvailability, ProjectionDetailScope, ProjectionEntity,
    StrategyDecisionProjection, EXECUTION_PROJECTION_REDUCER_VERSION,
    EXECUTION_PROJECTION_SCHEMA_VERSION,
};
use crate::execution_graph::{
    ExecutionEdgeProjection, ExecutionNodeProjection, ExecutionOrchestrationMetadata,
    ExecutionParentBinding, ExecutionServiceClass,
};
use crate::outcome::{DeliveryEnvelope, TerminalPresentation};
use crate::turn::CancellationReceipt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEntityCollection {
    Goals,
    Agents,
    Teams,
    Relations,
    Approvals,
    Admissions,
    Outcomes,
    Interventions,
    Usage,
    Context,
    Evidence,
    Health,
    Recovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionSourceHealth {
    Fresh,
    Lagged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionResyncReason {
    CursorGap,
    SchemaMismatch,
    AuthorizationChanged,
    RedactionChanged,
    DetailScopeChanged,
    RetentionGap,
    UnsafeMaterialization,
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ProjectionOperation {
    SetProjectionHeader {
        revision: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mission_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
    },
    SetGraphMetadata {
        revision: u64,
        commit_cursor: u64,
        objective: String,
        service_class: ExecutionServiceClass,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_execution: Option<ExecutionParentBinding>,
    },
    /// Replaces the complete immutable Program control/provenance record from
    /// the same graph revision as the surrounding delta.
    ReplaceGraphOrchestration {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        orchestration: Option<ExecutionOrchestrationMetadata>,
    },
    ReplaceGraphTopology {
        node_ids: Vec<String>,
        edges: Vec<ExecutionEdgeProjection>,
    },
    UpsertGraphNode {
        node: ExecutionNodeProjection,
    },
    RemoveGraphNode {
        node_id: String,
    },
    UpsertChildExecution {
        child: ChildExecutionProjection,
    },
    RemoveChildExecution {
        execution_id: String,
    },
    ReplaceActivities {
        activities: Vec<ExecutionActivityProjection>,
        relations: Vec<ExecutionActivityRelation>,
    },
    /// Replaces the exact root/inclusive concurrency and process-capacity
    /// snapshot. It is one operation so a Surface never combines counts from
    /// different durable cursors.
    ReplaceConcurrency {
        concurrency: ExecutionConcurrencyProjection,
    },
    UpsertActivity {
        activity: ExecutionActivityProjection,
    },
    UpsertActivityRelation {
        relation: ExecutionActivityRelation,
    },
    ReplaceStrategy {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        strategy: Option<StrategyDecisionProjection>,
    },
    UpsertEntity {
        collection: ProjectionEntityCollection,
        entity: ProjectionEntity,
    },
    RemoveEntity {
        collection: ProjectionEntityCollection,
        entity_key: String,
    },
    ReplaceAvailableCommands {
        commands: Vec<ProjectionCommandAvailability>,
    },
    SetTerminal {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        terminal_result_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        live: Option<ExecutionLiveState>,
    },
    /// Atomically replaces every terminal-delivery fact derived from the
    /// same durable graph/cursor materialization. A client never combines a
    /// new envelope with an older presentation or cancellation receipt.
    SetDeliveryTruth {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_envelope: Option<DeliveryEnvelope>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        terminal_presentation: Option<TerminalPresentation>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cancellation_receipt: Option<CancellationReceipt>,
    },
    AdvanceCursor {
        cursor: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProjectionDelta {
    pub schema_version: u32,
    pub reducer_version: u32,
    pub execution_id: String,
    pub from_revision: u64,
    pub target_revision: u64,
    pub base_cursor: u64,
    pub target_cursor: u64,
    pub detail_scope: ProjectionDetailScope,
    pub authorization_revision: u64,
    pub redaction_revision: String,
    pub source_health: ProjectionSourceHealth,
    #[serde(default)]
    pub operations: Vec<ProjectionOperation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resync_reason: Option<ProjectionResyncReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProjectionReduceError {
    #[error("projection schema/reducer version mismatch")]
    VersionMismatch,
    #[error("projection execution identity mismatch")]
    ExecutionMismatch,
    #[error("projection revision or cursor is not contiguous")]
    CursorGap,
    #[error("projection detail scope or authority changed")]
    AuthorityChanged,
    #[error("projection delta requires resynchronization: {0:?}")]
    ResyncRequired(ProjectionResyncReason),
    #[error("projection operation violated a materialization invariant: {0}")]
    Invariant(String),
}

pub fn reduce_projection_delta(
    current: &ExecutionProjection,
    delta: &ProjectionDelta,
) -> Result<ExecutionProjection, ProjectionReduceError> {
    if current.schema_version != EXECUTION_PROJECTION_SCHEMA_VERSION
        || delta.schema_version != EXECUTION_PROJECTION_SCHEMA_VERSION
        || delta.reducer_version != EXECUTION_PROJECTION_REDUCER_VERSION
    {
        return Err(ProjectionReduceError::VersionMismatch);
    }
    if current.execution_id != delta.execution_id {
        return Err(ProjectionReduceError::ExecutionMismatch);
    }
    if current.revision != delta.from_revision
        || current.cursor != delta.base_cursor
        || delta.target_revision < delta.from_revision
        || delta.target_cursor < delta.base_cursor
    {
        return Err(ProjectionReduceError::CursorGap);
    }
    if current.detail_scope != delta.detail_scope
        || current.authorization_revision != delta.authorization_revision
        || current.redaction_revision != delta.redaction_revision
    {
        return Err(ProjectionReduceError::AuthorityChanged);
    }
    if let Some(reason) = delta.resync_reason {
        return Err(ProjectionReduceError::ResyncRequired(reason));
    }

    let mut next = current.clone();
    for operation in &delta.operations {
        apply_operation(&mut next, operation)?;
    }
    if next.revision != delta.target_revision {
        return Err(ProjectionReduceError::Invariant(format!(
            "target revision {} was not materialized (got {})",
            delta.target_revision, next.revision
        )));
    }
    if next.cursor != delta.target_cursor {
        return Err(ProjectionReduceError::Invariant(format!(
            "target cursor {} was not materialized",
            delta.target_cursor
        )));
    }
    // Activity snapshots are ordered by durable observation, while generic
    // keyed upserts use identity order. Re-canonicalize after applying the
    // delta so incremental materialization is byte-for-byte equivalent to a
    // fresh snapshot and consumers never see recovery-path ordering jitter.
    next.activities.sort_by(|left, right| {
        (left.commit_cursor, left.sequence, left.activity_id.as_str()).cmp(&(
            right.commit_cursor,
            right.sequence,
            right.activity_id.as_str(),
        ))
    });
    validate_unique_keys(&next)?;
    Ok(next)
}

fn apply_operation(
    projection: &mut ExecutionProjection,
    operation: &ProjectionOperation,
) -> Result<(), ProjectionReduceError> {
    match operation {
        ProjectionOperation::SetProjectionHeader {
            revision,
            session_id,
            mission_id,
            task_id,
            turn_id,
        } => {
            projection.revision = *revision;
            projection.session_id.clone_from(session_id);
            projection.mission_id.clone_from(mission_id);
            projection.task_id.clone_from(task_id);
            projection.turn_id.clone_from(turn_id);
        }
        ProjectionOperation::SetGraphMetadata {
            revision,
            commit_cursor,
            objective,
            service_class,
            parent_execution,
        } => {
            projection.graph.revision = *revision;
            projection.graph.commit_cursor = *commit_cursor;
            projection.graph.objective.clone_from(objective);
            projection.graph.service_class = *service_class;
            projection
                .graph
                .parent_execution
                .clone_from(parent_execution);
        }
        ProjectionOperation::ReplaceGraphOrchestration { orchestration } => {
            projection.graph.orchestration.clone_from(orchestration);
        }
        ProjectionOperation::ReplaceGraphTopology { node_ids, edges } => {
            let retained = node_ids.iter().collect::<BTreeSet<_>>();
            projection
                .graph
                .nodes
                .retain(|node| retained.contains(&node.node_id));
            projection.graph.edges.clone_from(edges);
        }
        ProjectionOperation::UpsertGraphNode { node } => {
            upsert_by_key(&mut projection.graph.nodes, node.clone(), |value| {
                value.node_id.as_str()
            });
        }
        ProjectionOperation::RemoveGraphNode { node_id } => {
            projection
                .graph
                .nodes
                .retain(|node| node.node_id != *node_id);
        }
        ProjectionOperation::UpsertChildExecution { child } => {
            upsert_by_key(&mut projection.child_executions, child.clone(), |value| {
                value.execution_id.as_str()
            });
        }
        ProjectionOperation::RemoveChildExecution { execution_id } => {
            projection
                .child_executions
                .retain(|child| child.execution_id != *execution_id);
        }
        ProjectionOperation::ReplaceActivities {
            activities,
            relations,
        } => {
            projection.activities.clone_from(activities);
            projection.activity_relations.clone_from(relations);
        }
        ProjectionOperation::ReplaceConcurrency { concurrency } => {
            projection.concurrency.clone_from(concurrency);
        }
        ProjectionOperation::UpsertActivity { activity } => {
            upsert_by_key(&mut projection.activities, activity.clone(), |value| {
                value.activity_id.as_str()
            });
        }
        ProjectionOperation::UpsertActivityRelation { relation } => {
            upsert_by_key(
                &mut projection.activity_relations,
                relation.clone(),
                |value| value.relation_id.as_str(),
            );
        }
        ProjectionOperation::ReplaceStrategy { strategy } => {
            projection.strategy.clone_from(strategy);
        }
        ProjectionOperation::UpsertEntity { collection, entity } => {
            upsert_by_key(
                entity_collection_mut(projection, *collection),
                entity.clone(),
                |value| value.id.as_str(),
            );
        }
        ProjectionOperation::RemoveEntity {
            collection,
            entity_key,
        } => {
            entity_collection_mut(projection, *collection)
                .retain(|entity| entity.id != *entity_key);
        }
        ProjectionOperation::ReplaceAvailableCommands { commands } => {
            projection.available_commands.clone_from(commands);
        }
        ProjectionOperation::SetTerminal {
            terminal_result_ref,
            live,
        } => {
            projection
                .graph
                .terminal_result_ref
                .clone_from(terminal_result_ref);
            if live.is_some() {
                projection.live.clone_from(live);
            }
        }
        ProjectionOperation::SetDeliveryTruth {
            delivery_envelope,
            terminal_presentation,
            cancellation_receipt,
        } => {
            projection.delivery_envelope.clone_from(delivery_envelope);
            projection
                .terminal_presentation
                .clone_from(terminal_presentation);
            projection
                .cancellation_receipt
                .clone_from(cancellation_receipt);
        }
        ProjectionOperation::AdvanceCursor { cursor } => {
            if *cursor < projection.cursor {
                return Err(ProjectionReduceError::CursorGap);
            }
            projection.cursor = *cursor;
        }
    }
    Ok(())
}

fn upsert_by_key<T: Clone>(values: &mut Vec<T>, value: T, key: impl Fn(&T) -> &str) {
    let value_key = key(&value).to_string();
    if let Some(index) = values
        .iter()
        .position(|candidate| key(candidate) == value_key)
    {
        values[index] = value;
    } else {
        values.push(value);
    }
    values.sort_by(|left, right| key(left).cmp(key(right)));
}

fn entity_collection_mut(
    projection: &mut ExecutionProjection,
    collection: ProjectionEntityCollection,
) -> &mut Vec<ProjectionEntity> {
    match collection {
        ProjectionEntityCollection::Goals => &mut projection.goals,
        ProjectionEntityCollection::Agents => &mut projection.agents,
        ProjectionEntityCollection::Teams => &mut projection.teams,
        ProjectionEntityCollection::Relations => &mut projection.relations,
        ProjectionEntityCollection::Approvals => &mut projection.approvals,
        ProjectionEntityCollection::Admissions => &mut projection.admissions,
        ProjectionEntityCollection::Outcomes => &mut projection.outcomes,
        ProjectionEntityCollection::Interventions => &mut projection.interventions,
        ProjectionEntityCollection::Usage => &mut projection.usage,
        ProjectionEntityCollection::Context => &mut projection.context,
        ProjectionEntityCollection::Evidence => &mut projection.evidence,
        ProjectionEntityCollection::Health => &mut projection.health,
        ProjectionEntityCollection::Recovery => &mut projection.recovery,
    }
}

fn validate_unique_keys(projection: &ExecutionProjection) -> Result<(), ProjectionReduceError> {
    unique(
        projection
            .graph
            .nodes
            .iter()
            .map(|node| node.node_id.as_str()),
        "graph node",
    )?;
    unique(
        projection
            .child_executions
            .iter()
            .map(|child| child.execution_id.as_str()),
        "child execution",
    )?;
    unique(
        projection
            .activities
            .iter()
            .map(|activity| activity.activity_id.as_str()),
        "activity",
    )?;
    unique(
        projection
            .activity_relations
            .iter()
            .map(|relation| relation.relation_id.as_str()),
        "activity relation",
    )?;
    for (label, entities) in [
        ("goal", &projection.goals),
        ("agent", &projection.agents),
        ("team", &projection.teams),
        ("relation", &projection.relations),
        ("approval", &projection.approvals),
        ("admission", &projection.admissions),
        ("outcome", &projection.outcomes),
        ("intervention", &projection.interventions),
        ("usage", &projection.usage),
        ("context", &projection.context),
        ("evidence", &projection.evidence),
        ("health", &projection.health),
        ("recovery", &projection.recovery),
    ] {
        unique(entities.iter().map(|entity| entity.id.as_str()), label)?;
    }
    Ok(())
}

fn unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    label: &str,
) -> Result<(), ProjectionReduceError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ProjectionReduceError::Invariant(format!(
                "duplicate {label} key `{value}`"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct GoldenCorpus {
        initial: ExecutionProjection,
        delta: ProjectionDelta,
        expected: ExecutionProjection,
    }

    fn corpus() -> GoldenCorpus {
        serde_json::from_str(include_str!(
            "../../tests/fixtures/projection-v3/materialization.json"
        ))
        .expect("projection v3 golden corpus")
    }

    #[test]
    fn canonical_reducer_matches_golden_materialization() {
        let corpus = corpus();
        let operations = &corpus.delta.operations;
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::SetProjectionHeader { .. })));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::SetGraphMetadata { .. })));
        assert!(operations.iter().any(|operation| matches!(
            operation,
            ProjectionOperation::ReplaceGraphTopology { .. }
        )));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::UpsertGraphNode { .. })));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::RemoveGraphNode { .. })));
        assert!(operations.iter().any(|operation| matches!(
            operation,
            ProjectionOperation::UpsertChildExecution { .. }
        )));
        assert!(operations.iter().any(|operation| matches!(
            operation,
            ProjectionOperation::RemoveChildExecution { .. }
        )));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::ReplaceActivities { .. })));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::UpsertActivity { .. })));
        assert!(operations.iter().any(|operation| matches!(
            operation,
            ProjectionOperation::UpsertActivityRelation { .. }
        )));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::ReplaceStrategy { .. })));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::UpsertEntity { .. })));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::RemoveEntity { .. })));
        assert!(operations.iter().any(|operation| matches!(
            operation,
            ProjectionOperation::ReplaceAvailableCommands { .. }
        )));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::SetTerminal { .. })));
        assert!(operations
            .iter()
            .any(|operation| matches!(operation, ProjectionOperation::AdvanceCursor { .. })));
        let actual = reduce_projection_delta(&corpus.initial, &corpus.delta).expect("valid delta");
        assert_eq!(actual, corpus.expected);
    }

    #[test]
    fn delivery_truth_replaces_all_terminal_facts_as_one_reducer_operation() {
        let corpus = corpus();
        let delivery_envelope: crate::outcome::DeliveryEnvelope =
            serde_json::from_value(serde_json::json!({
                "envelope_id": "envelope-v3",
                "revision": 3,
                "objective_id": "objective-v3",
                "created_at_ms": 30
            }))
            .expect("minimal delivery envelope");
        let terminal_presentation: crate::outcome::TerminalPresentation =
            serde_json::from_value(serde_json::json!({
                "presentation_id": "presentation-v3",
                "attempt_id": "attempt-v3",
                "envelope_id": "envelope-v3",
                "envelope_revision": 3,
                "state": "committed",
                "answer_origin": "terminal_narrator",
                "generated_at_ms": 31,
                "committed_at_ms": 32
            }))
            .expect("minimal terminal presentation");
        let cancellation_receipt: crate::turn::CancellationReceipt =
            serde_json::from_value(serde_json::json!({
                "cancellation_id": "cancel-v3",
                "session_id": "session-golden",
                "turn_id": "turn-golden",
                "execution_id": "execution-golden",
                "actor_id": "user-v3",
                "requested_at_ms": 33
            }))
            .expect("minimal cancellation receipt");

        let mut delta = corpus.delta.clone();
        delta.operations.insert(
            delta.operations.len() - 1,
            ProjectionOperation::SetDeliveryTruth {
                delivery_envelope: Some(delivery_envelope.clone()),
                terminal_presentation: Some(terminal_presentation.clone()),
                cancellation_receipt: Some(cancellation_receipt.clone()),
            },
        );
        let actual = reduce_projection_delta(&corpus.initial, &delta).expect("valid delta");

        assert_eq!(actual.delivery_envelope, Some(delivery_envelope));
        assert_eq!(actual.terminal_presentation, Some(terminal_presentation));
        assert_eq!(actual.cancellation_receipt, Some(cancellation_receipt));
    }

    #[test]
    fn reducer_is_atomic_and_rejects_gap_version_authority_and_resync() {
        let corpus = corpus();
        let mut gap = corpus.delta.clone();
        gap.base_cursor += 1;
        assert_eq!(
            reduce_projection_delta(&corpus.initial, &gap),
            Err(ProjectionReduceError::CursorGap)
        );

        let mut recropped = corpus.delta.clone();
        recropped.authorization_revision += 1;
        assert_eq!(
            reduce_projection_delta(&corpus.initial, &recropped),
            Err(ProjectionReduceError::AuthorityChanged)
        );

        let mut redacted = corpus.delta.clone();
        redacted.redaction_revision = "sha256:changed".to_string();
        assert_eq!(
            reduce_projection_delta(&corpus.initial, &redacted),
            Err(ProjectionReduceError::AuthorityChanged)
        );

        let mut wrong_schema = corpus.delta.clone();
        wrong_schema.schema_version += 1;
        assert_eq!(
            reduce_projection_delta(&corpus.initial, &wrong_schema),
            Err(ProjectionReduceError::VersionMismatch)
        );

        let mut explicit_resync = corpus.delta.clone();
        explicit_resync.resync_reason = Some(ProjectionResyncReason::Explicit);
        assert_eq!(
            reduce_projection_delta(&corpus.initial, &explicit_resync),
            Err(ProjectionReduceError::ResyncRequired(
                ProjectionResyncReason::Explicit
            ))
        );

        let mut duplicate = corpus.delta;
        let duplicate_entity = ProjectionEntity {
            id: "duplicate".to_string(),
            kind: "evidence".to_string(),
            revision: 1,
            status: None,
            summary: None,
            evidence_refs: Vec::new(),
            payload: None,
            detail: None,
        };
        duplicate.operations.insert(
            duplicate.operations.len() - 1,
            ProjectionOperation::UpsertEntity {
                collection: ProjectionEntityCollection::Evidence,
                entity: duplicate_entity.clone(),
            },
        );
        duplicate.operations.insert(
            duplicate.operations.len() - 1,
            ProjectionOperation::UpsertEntity {
                collection: ProjectionEntityCollection::Evidence,
                entity: duplicate_entity,
            },
        );
        // Upsert is deterministic and idempotent rather than producing
        // duplicate materialized keys.
        let reduced = reduce_projection_delta(&corpus.initial, &duplicate).expect("idempotent");
        assert_eq!(
            reduced
                .evidence
                .iter()
                .filter(|entity| entity.id == "duplicate")
                .count(),
            1
        );
    }
}
