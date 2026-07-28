use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    ChildExecutionProjection, ExecutionLiveState, ExecutionProjection,
    ProjectionCommandAvailability, ProjectionDetailScope, ProjectionEntity,
    StrategyDecisionProjection, EXECUTION_PROJECTION_REDUCER_VERSION,
    EXECUTION_PROJECTION_SCHEMA_VERSION,
};
use crate::execution_graph::{
    ExecutionEdgeProjection, ExecutionNodeProjection, ExecutionParentBinding, ExecutionServiceClass,
};

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
    },
    SetGraphMetadata {
        revision: u64,
        commit_cursor: u64,
        objective: String,
        service_class: ExecutionServiceClass,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_execution: Option<ExecutionParentBinding>,
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
        } => {
            projection.revision = *revision;
            projection.session_id.clone_from(session_id);
            projection.mission_id.clone_from(mission_id);
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
            "../../tests/fixtures/projection-v2/materialization.json"
        ))
        .expect("projection v2 golden corpus")
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
